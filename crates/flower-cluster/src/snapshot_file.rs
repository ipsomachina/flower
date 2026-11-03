//! Snapshot images as files: the Raft snapshot data type, and streaming
//! encoding and decoding of the [`SyncSnapshotFile`] an image holds.
//!
//! [`SyncSnapshotFile`]: crate::sync::SyncSnapshotFile
//!
//! A member builds an image straight into a file, one database view at
//! a time, and a follower's InstallSnapshot segments spool into a file
//! as they arrive; installing reads the file twice, once for the header
//! and the database list (payloads skipped unparsed into memory), then
//! once more handing each database view to the install as it decodes.
//! So no image is ever whole in memory: one database's payload at a
//! time is. The bytes are exactly a [`SyncSnapshotFile`]'s JSON, the
//! format devices decode, so sync serving ships the file as it is.

use std::fmt;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::task::{Context, Poll};

use openraft::{BasicNode, LogId, StoredMembership};
use serde::Deserializer as _;
use serde::de::{self, DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor};
use tokio::io::{AsyncRead, AsyncSeek, AsyncWrite, AsyncWriteExt, ReadBuf};

use crate::sync::{SYNC_SNAPSHOT_VERSION, SyncDbMeta, SyncDbSnapshot};
use crate::types::NodeId;

/// An image file, the Raft snapshot data: OpenRaft reads one segment by
/// segment to send it, and writes a follower's arriving segments into
/// one, at their offsets.
#[derive(Debug)]
pub struct SnapshotFile {
    file: tokio::fs::File,
    path: PathBuf,
}

impl SnapshotFile {
    /// Wrap an open file at `path`.
    pub(crate) fn new(file: std::fs::File, path: PathBuf) -> Self {
        SnapshotFile {
            file: tokio::fs::File::from_std(file),
            path,
        }
    }

    /// Open an image for reading from its start.
    pub fn open(path: &Path) -> io::Result<Self> {
        Ok(Self::new(std::fs::File::open(path)?, path.to_path_buf()))
    }

    /// Where the file is.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Flush what was written and close the file; sealing the image
    /// syncs it.
    pub(crate) async fn close(mut self) -> io::Result<PathBuf> {
        self.file.flush().await?;
        Ok(self.path)
    }
}

impl AsyncRead for SnapshotFile {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.file).poll_read(cx, buf)
    }
}

impl AsyncWrite for SnapshotFile {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.file).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.file).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.file).poll_shutdown(cx)
    }
}

impl AsyncSeek for SnapshotFile {
    fn start_seek(mut self: Pin<&mut Self>, position: io::SeekFrom) -> io::Result<()> {
        Pin::new(&mut self.file).start_seek(position)
    }

    fn poll_complete(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        Pin::new(&mut self.file).poll_complete(cx)
    }
}

/// Image file names end in this once complete.
const IMAGE_SUFFIX: &str = ".snapshot";
/// Image file names end in this while being built or received.
const PART_SUFFIX: &str = ".part";

/// The directory a log store keeps its images in: `<n>.snapshot` once
/// complete and sealed, `<n>.part` while a build or a transfer writes
/// one. Numbers are never reused within a directory, so a deleted image
/// a reader still holds open never shares a name with a new one.
#[derive(Debug)]
pub(crate) struct Images {
    dir: PathBuf,
    next: AtomicU64,
    sync: AtomicBool,
}

impl Images {
    /// Open (creating) the directory and number past what it holds.
    pub(crate) fn open(dir: PathBuf) -> io::Result<Self> {
        std::fs::create_dir_all(&dir)?;
        let mut next = 0;
        for entry in std::fs::read_dir(&dir)? {
            let name = entry?.file_name();
            if let Some(number) = name.to_str().and_then(number) {
                next = next.max(number + 1);
            }
        }
        Ok(Images {
            dir,
            next: AtomicU64::new(next),
            sync: AtomicBool::new(true),
        })
    }

    /// Whether image files and their directory are synced.
    pub(crate) fn set_sync(&self, sync: bool) {
        self.sync.store(sync, Ordering::Release);
    }

    pub(crate) fn sync(&self) -> bool {
        self.sync.load(Ordering::Acquire)
    }

    /// A new part file, open for reading and writing.
    pub(crate) fn part(&self) -> io::Result<(PathBuf, std::fs::File)> {
        let number = self.next.fetch_add(1, Ordering::AcqRel);
        let path = self.dir.join(format!("{number}{PART_SUFFIX}"));
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)?;
        Ok((path, file))
    }

    /// A part file of this directory for `file`: `file` itself when it
    /// is one, else a copy (an image handed over from elsewhere).
    pub(crate) fn adopt(&self, file: &Path) -> io::Result<PathBuf> {
        let ours = file.parent() == Some(self.dir.as_path())
            && file
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with(PART_SUFFIX) && number(name).is_some());
        if ours {
            return Ok(file.to_path_buf());
        }
        let (path, mut part) = self.part()?;
        io::copy(&mut std::fs::File::open(file)?, &mut part)?;
        Ok(path)
    }

    /// Seal a complete part: sync it, rename it to its image name and
    /// sync the directory. Returns the image's file name.
    pub(crate) fn seal(&self, part: &Path) -> io::Result<String> {
        let name = part
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(|name| name.strip_suffix(PART_SUFFIX))
            .filter(|stem| stem.parse::<u64>().is_ok())
            .ok_or_else(|| io::Error::other(format!("{} is not a part file", part.display())))?;
        let image = format!("{name}{IMAGE_SUFFIX}");
        if self.sync() {
            // Windows flushes only handles opened for writing.
            std::fs::OpenOptions::new()
                .write(true)
                .open(part)?
                .sync_all()?;
        }
        std::fs::rename(part, self.dir.join(&image))?;
        self.sync_dir()?;
        Ok(image)
    }

    /// The path of a recorded image, refusing names this directory
    /// never gives.
    pub(crate) fn path(&self, name: &str) -> io::Result<PathBuf> {
        if name.ends_with(IMAGE_SUFFIX) && number(name).is_some() {
            Ok(self.dir.join(name))
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("recorded snapshot image {name:?} is not an image name"),
            ))
        }
    }

    /// Delete every sealed image but `keep`, best-effort: a reader that
    /// holds one open keeps reading it.
    pub(crate) fn retain(&self, keep: Option<&str>) {
        self.remove(|name| name.ends_with(IMAGE_SUFFIX) && Some(name) != keep);
    }

    /// Sync every sealed image and the directory.
    pub(crate) fn sync_all(&self) -> io::Result<()> {
        for entry in std::fs::read_dir(&self.dir)? {
            let entry = entry?;
            let name = entry.file_name();
            if name
                .to_str()
                .is_some_and(|name| name.ends_with(IMAGE_SUFFIX) && number(name).is_some())
            {
                std::fs::OpenOptions::new()
                    .write(true)
                    .open(entry.path())?
                    .sync_all()?;
            }
        }
        #[cfg(unix)]
        std::fs::File::open(&self.dir)?.sync_all()?;
        Ok(())
    }

    /// Delete every part file: on open, when no build or transfer runs.
    pub(crate) fn clear_parts(&self) {
        self.remove(|name| name.ends_with(PART_SUFFIX));
    }

    fn remove(&self, doomed: impl Fn(&str) -> bool) {
        let Ok(entries) = std::fs::read_dir(&self.dir) else {
            return;
        };
        for entry in entries.flatten() {
            let name = entry.file_name();
            if name
                .to_str()
                .is_some_and(|name| number(name).is_some() && doomed(name))
            {
                let _ = std::fs::remove_file(entry.path());
            }
        }
    }

    /// Make a rename in the directory durable. Windows has no directory
    /// handle to sync; its renames are metadata journaled by NTFS.
    fn sync_dir(&self) -> io::Result<()> {
        #[cfg(unix)]
        if self.sync() {
            std::fs::File::open(&self.dir)?.sync_all()?;
        }
        Ok(())
    }
}

/// The number of an image or part file name.
fn number(name: &str) -> Option<u64> {
    name.strip_suffix(IMAGE_SUFFIX)
        .or_else(|| name.strip_suffix(PART_SUFFIX))?
        .parse()
        .ok()
}

/// FNV-1a over an image's bytes: corruption detection for images in
/// transit and at rest (the engine's snapshot chunks use the same), not
/// authentication, which the peer transport does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Checksum(u64);

impl Default for Checksum {
    fn default() -> Self {
        Checksum(0xcbf2_9ce4_8422_2325)
    }
}

impl Checksum {
    fn update(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 ^= u64::from(*byte);
            self.0 = self.0.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }

    /// A snapshot ID naming `prefix` and this checksum, which a receiver
    /// checks the image against ([`Checksum::named_in`]).
    pub fn id(&self, prefix: &str) -> String {
        format!("{prefix}-{:016x}", self.0)
    }

    /// The checksum a snapshot ID names, if it names one.
    pub fn named_in(id: &str) -> Option<Checksum> {
        let (_, hex) = id.rsplit_once('-')?;
        if hex.len() != 16 {
            return None;
        }
        u64::from_str_radix(hex, 16).ok().map(Checksum)
    }
}

/// A reader or writer that checksums what passes through it.
pub struct Checksummed<T> {
    inner: T,
    sum: Checksum,
}

impl<T> Checksummed<T> {
    /// Checksum what passes through `inner`.
    pub fn new(inner: T) -> Self {
        Checksummed {
            inner,
            sum: Checksum::default(),
        }
    }

    /// The checksum so far.
    pub fn checksum(&self) -> Checksum {
        self.sum
    }

    /// The wrapped reader or writer.
    pub fn into_inner(self) -> T {
        self.inner
    }
}

impl<W: Write> Write for Checksummed<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let written = self.inner.write(buf)?;
        self.sum.update(&buf[..written]);
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

impl<R: Read> Read for Checksummed<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let read = self.inner.read(buf)?;
        self.sum.update(&buf[..read]);
        Ok(read)
    }
}

/// Writes a [`SyncSnapshotFile`](crate::sync::SyncSnapshotFile)'s JSON one database view at a time.
pub struct FileWriter<W: Write> {
    out: W,
    views: usize,
}

impl<W: Write> FileWriter<W> {
    /// Write the header: version, group position and membership.
    pub fn begin(
        mut out: W,
        applied: &Option<LogId<NodeId>>,
        membership: &StoredMembership<NodeId, BasicNode>,
    ) -> io::Result<Self> {
        out.write_all(b"{\"version\":")?;
        serde_json::to_writer(&mut out, &SYNC_SNAPSHOT_VERSION)?;
        out.write_all(b",\"applied\":")?;
        serde_json::to_writer(&mut out, applied)?;
        out.write_all(b",\"membership\":")?;
        serde_json::to_writer(&mut out, membership)?;
        out.write_all(b",\"dbs\":[")?;
        Ok(FileWriter { out, views: 0 })
    }

    /// Append one database view.
    pub fn view(&mut self, view: &SyncDbSnapshot) -> io::Result<()> {
        if self.views > 0 {
            self.out.write_all(b",")?;
        }
        serde_json::to_writer(&mut self.out, view)?;
        self.views += 1;
        Ok(())
    }

    /// Close the file's JSON and hand back the writer.
    pub fn finish(mut self) -> io::Result<W> {
        self.out.write_all(b"]}")?;
        Ok(self.out)
    }
}

/// An image's header and database list, read without its payloads.
#[derive(Clone, Debug, serde::Deserialize)]
pub struct FileHeader {
    /// Wire version.
    pub version: u32,
    /// Group applied log ID the views cover.
    pub applied: Option<LogId<NodeId>>,
    /// Last applied membership.
    pub membership: StoredMembership<NodeId, BasicNode>,
    /// Each view's fencing metadata, in file order.
    #[serde(deserialize_with = "view_metas")]
    pub dbs: Vec<SyncDbMeta>,
}

/// A view's metadata; its payload is skipped as it parses.
#[derive(serde::Deserialize)]
struct ViewMeta {
    meta: SyncDbMeta,
}

fn view_metas<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<SyncDbMeta>, D::Error> {
    let views: Vec<ViewMeta> = serde::Deserialize::deserialize(deserializer)?;
    Ok(views.into_iter().map(|view| view.meta).collect())
}

/// Read and check an image's header: the whole file parses (a truncated
/// image or trailing bytes fail here) and its version is this build's.
pub fn read_header<R: Read>(reader: R) -> io::Result<FileHeader> {
    let header: FileHeader = serde_json::from_reader(reader)?;
    if header.version != SYNC_SNAPSHOT_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "snapshot wire version {} is not {SYNC_SNAPSHOT_VERSION}",
                header.version
            ),
        ));
    }
    Ok(header)
}

/// Why streaming an image's views stopped.
#[derive(Debug)]
pub enum ViewsError<E> {
    /// The image failed to decode.
    Decode(serde_json::Error),
    /// The callback refused a view.
    Install(E),
}

/// Hand each database view of an image to `install` as it decodes, in
/// file order: one view's payload in memory at a time.
pub fn for_each_view<R, E, F>(reader: R, mut install: F) -> Result<(), ViewsError<E>>
where
    R: Read,
    F: FnMut(SyncDbSnapshot) -> Result<(), E>,
{
    let mut failed = None;
    let mut deserializer = serde_json::Deserializer::from_reader(reader);
    let body = Body {
        install: &mut install,
        failed: &mut failed,
    };
    let decoded = deserializer
        .deserialize_map(body)
        .and_then(|()| deserializer.end());
    match (failed, decoded) {
        (Some(error), _) => Err(ViewsError::Install(error)),
        (None, Err(error)) => Err(ViewsError::Decode(error)),
        (None, Ok(())) => Ok(()),
    }
}

/// The file's top-level object: everything but `dbs` is skipped.
struct Body<'a, F, E> {
    install: &'a mut F,
    failed: &'a mut Option<E>,
}

impl<'de, F, E> Visitor<'de> for Body<'_, F, E>
where
    F: FnMut(SyncDbSnapshot) -> Result<(), E>,
{
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a snapshot file")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<(), A::Error> {
        while let Some(key) = map.next_key::<String>()? {
            if key == "dbs" {
                map.next_value_seed(Views {
                    install: &mut *self.install,
                    failed: &mut *self.failed,
                })?;
            } else {
                map.next_value::<IgnoredAny>()?;
            }
        }
        Ok(())
    }
}

/// The `dbs` array, installed element by element.
struct Views<'a, F, E> {
    install: &'a mut F,
    failed: &'a mut Option<E>,
}

impl<'de, F, E> DeserializeSeed<'de> for Views<'_, F, E>
where
    F: FnMut(SyncDbSnapshot) -> Result<(), E>,
{
    type Value = ();

    fn deserialize<D: serde::Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        deserializer.deserialize_seq(self)
    }
}

impl<'de, F, E> Visitor<'de> for Views<'_, F, E>
where
    F: FnMut(SyncDbSnapshot) -> Result<(), E>,
{
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("database views")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<(), A::Error> {
        while let Some(view) = seq.next_element::<SyncDbSnapshot>()? {
            if let Err(error) = (self.install)(view) {
                *self.failed = Some(error);
                return Err(de::Error::custom("snapshot view refused"));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::SyncSnapshotFile;

    fn view(db: &[u8], payload: Vec<u8>) -> SyncDbSnapshot {
        SyncDbSnapshot {
            meta: SyncDbMeta {
                db: db.to_vec(),
                revision: 3,
                applied_log: 9,
                incarnation: 7,
            },
            payload,
        }
    }

    fn written(views: &[SyncDbSnapshot]) -> Vec<u8> {
        let applied = Some(LogId::new(openraft::CommittedLeaderId::new(2, 1), 9));
        let mut writer =
            FileWriter::begin(Vec::new(), &applied, &StoredMembership::default()).unwrap();
        for view in views {
            writer.view(view).unwrap();
        }
        writer.finish().unwrap()
    }

    #[test]
    fn streamed_files_are_sync_snapshot_files() {
        let views = [view(b"a", vec![1, 2, 3]), view(b"$cluster", vec![])];
        let bytes = written(&views);
        let file: SyncSnapshotFile = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(file.version, SYNC_SNAPSHOT_VERSION);
        assert_eq!(file.applied.map(|id| id.index), Some(9));
        assert_eq!(file.dbs.len(), 2);
        assert_eq!(file.dbs[0].payload, vec![1, 2, 3]);
        // The derived encoding of the same file reads back the same way.
        assert_eq!(serde_json::to_vec(&file).unwrap(), bytes);
        let empty = written(&[]);
        let file: SyncSnapshotFile = serde_json::from_slice(&empty).unwrap();
        assert!(file.dbs.is_empty());
    }

    #[test]
    fn headers_skip_payloads_and_views_stream_in_order() {
        let views = [view(b"a", vec![5; 1000]), view(b"b", vec![6; 10])];
        let bytes = written(&views);
        let header = read_header(bytes.as_slice()).unwrap();
        let names: Vec<_> = header.dbs.iter().map(|meta| meta.db.clone()).collect();
        assert_eq!(names, vec![b"a".to_vec(), b"b".to_vec()]);
        let mut seen = Vec::new();
        for_each_view(bytes.as_slice(), |view| {
            seen.push((view.meta.db, view.payload.len()));
            Ok::<_, ()>(())
        })
        .unwrap();
        assert_eq!(seen, vec![(b"a".to_vec(), 1000), (b"b".to_vec(), 10)]);
        // A refusal stops the stream and comes back as itself.
        let mut calls = 0;
        let refused = for_each_view(bytes.as_slice(), |_| {
            calls += 1;
            Err("no")
        });
        assert!(matches!(refused, Err(ViewsError::Install("no"))));
        assert_eq!(calls, 1);
    }

    #[test]
    fn truncated_trailing_and_foreign_images_fail_the_header() {
        let bytes = written(&[view(b"a", vec![1; 100])]);
        assert!(read_header(&bytes[..bytes.len() - 1]).is_err());
        assert!(read_header([bytes.as_slice(), b"junk"].concat().as_slice()).is_err());
        let foreign =
            String::from_utf8(bytes)
                .unwrap()
                .replacen("\"version\":1", "\"version\":99", 1);
        let error = read_header(foreign.as_bytes()).unwrap_err();
        assert!(error.to_string().contains("version 99"), "{error}");
    }
}
