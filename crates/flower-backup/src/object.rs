//! Opaque object naming, the backup key, compression and part sizes.
//!
//! Names carry no business data, collection names or key material:
//! a fixed prefix, a kind directory and random hex for views and
//! manifests. Segments are named by their history's incarnation and
//! index range alone, so retention, collection and restore plan
//! without downloads, a re-archived range overwrites its earlier
//! upload instead of duplicating it, and a restored history whose
//! positions restart never reuses an older history's names. Each
//! history keeps its own archive position. No object names the latest
//! backup: the driver derives it from the manifest listing.
//!
//! Every object is a sealed stream ([`flower_crypto::aead::StreamSealer`]):
//! frames of at most [`FRAME_LEN`] bytes, each authenticated on its own,
//! so objects of any size upload in parts and download frame by frame.

use std::path::Path;
use std::sync::{Arc, Mutex};

use flower_crypto::aead::{self, CryptoError, StreamOpener, StreamSealer};
use flower_crypto::keys::Keystore;

/// Object prefix: every backup object lives below it.
pub const PREFIX: &str = "flower-backup/v1";
/// Archive-position pointers, one per history incarnation, each a
/// sealed `u64` big-endian: see [`archive_position_name`].
pub const ARCHIVE_POS_DIR: &str = "flower-backup/v1/archive-pos";
/// Segment objects, one directory per history incarnation: see
/// [`segment_name`].
pub const SEGMENT_DIR: &str = "flower-backup/v1/seg";

/// zstd compression level for views and segments.
pub const ZSTD_LEVEL: i32 = 3;

/// Plaintext bytes per sealed frame, at most: what a reader holds
/// before it can authenticate, and what sealing holds before it emits.
pub const FRAME_LEN: usize = 1 << 20;
/// Bytes per uploaded part by default: an object of more goes up as a
/// multipart upload. S3 takes at most 10,000 parts, so the default stores
/// objects up to 160 GiB.
pub const DEFAULT_PART_BYTES: usize = 16 << 20;
/// The smallest part S3 accepts but for an upload's last.
pub const MIN_PART_BYTES: usize = 5 << 20;

/// Hex-encode bytes.
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// A fresh random 128-bit ID in hex.
pub fn new_id() -> String {
    let id: [u8; 16] = rand::random();
    hex(&id)
}

/// A fresh opaque object name for a kind directory (`obj`,
/// `manifest`). Kinds are directories so prefix listing works on
/// every backend, including ones that navigate by segment.
pub fn object_name(kind: &str) -> String {
    format!("{PREFIX}/{kind}/{}", new_id())
}

/// The archive-position pointer of one history incarnation:
/// `archive-pos/{incarnation:016x}`.
pub fn archive_position_name(incarnation: u64) -> String {
    format!("{ARCHIVE_POS_DIR}/{incarnation:016x}")
}

/// The directory holding one history incarnation's segments.
pub fn segment_dir(incarnation: u64) -> String {
    format!("{SEGMENT_DIR}/{incarnation:016x}")
}

/// The deterministic segment name for an index range of one history,
/// sorting by range within it:
/// `seg/{incarnation:016x}/{first:020}-{last:020}`. Committed entries
/// never change, so re-archiving a range reproduces the same segment
/// and overwrites the earlier upload.
pub fn segment_name(incarnation: u64, first: u64, last: u64) -> String {
    format!("{}/{first:020}-{last:020}", segment_dir(incarnation))
}

/// A segment's history and index range, as its name records them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct SegmentRange {
    /// Group incarnation of the history the entries belong to.
    pub incarnation: u64,
    /// First log index covered, inclusive.
    pub first: u64,
    /// Last log index covered, inclusive.
    pub last: u64,
}

/// Parse a segment name's history and index range, if well-formed.
/// Unparseable names stay untouched: retention and collection never
/// delete what they cannot read.
pub fn segment_range_from_name(name: &str) -> Option<SegmentRange> {
    let rest = name.strip_prefix(SEGMENT_DIR)?.strip_prefix('/')?;
    let (incarnation, range) = rest.split_once('/')?;
    if incarnation.len() != 16 {
        return None;
    }
    let incarnation = u64::from_str_radix(incarnation, 16).ok()?;
    let (first, last) = range.split_once('-')?;
    let (first, last) = (first.parse().ok()?, last.parse().ok()?);
    (first <= last).then_some(SegmentRange {
        incarnation,
        first,
        last,
    })
}

/// The deployment backup key: seals under the current version and
/// opens under any retained version. Clones share one keystore, so
/// online and offline drivers stay coherent.
#[derive(Clone)]
pub struct BackupKey {
    inner: Arc<Mutex<Keystore>>,
}

impl BackupKey {
    /// Open the keystore at `path` under the operator KEK.
    pub fn open(path: &Path, kek: [u8; 32]) -> Result<Self, CryptoError> {
        Ok(BackupKey {
            inner: Arc::new(Mutex::new(Keystore::open(path, kek)?)),
        })
    }

    /// Lock the keystore.
    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Keystore>, CryptoError> {
        self.inner
            .lock()
            .map_err(|_| CryptoError::Store("keystore lock poisoned".to_string()))
    }

    /// Start a sealed stream under the current version, creating
    /// version 1 on an empty store, with frames of `frame_len` bytes.
    /// Returns the sealing version plus the sealer.
    pub fn sealer(&self, frame_len: usize) -> Result<(u32, StreamSealer), CryptoError> {
        let store = self.lock()?;
        let version = store.ensure_current()?;
        let key = store.get(version)?;
        drop(store);
        Ok((version, StreamSealer::new(&key, version, frame_len)?))
    }

    /// Start opening a sealed stream from its header, under the
    /// retained version it records.
    pub fn opener(&self, header: &[u8]) -> Result<StreamOpener, CryptoError> {
        let version = aead::peek_stream_version(header)?;
        let key = self.lock()?.get(version)?;
        StreamOpener::new(&key, header)
    }

    /// Seal a whole plaintext under the current version. Returns the
    /// sealing version plus the sealed stream.
    pub fn seal_current(&self, plaintext: &[u8]) -> Result<(u32, Vec<u8>), CryptoError> {
        let (version, mut sealer) = self.sealer(FRAME_LEN)?;
        sealer.write(plaintext)?;
        Ok((version, sealer.finish()?))
    }

    /// Open a whole sealed stream under the retained version it
    /// records.
    pub fn open_sealed(&self, sealed: &[u8]) -> Result<Vec<u8>, CryptoError> {
        let version = aead::peek_stream_version(sealed)?;
        let key = self.lock()?.get(version)?;
        Ok(aead::open_stream(&key, sealed)?.1)
    }

    /// Rotate to a fresh current version.
    pub fn rotate(&self) -> Result<u32, CryptoError> {
        self.lock()?.rotate()
    }

    /// Retire a version's material. Refuses the current version.
    pub fn retire(&self, version: u32) -> Result<bool, CryptoError> {
        self.lock()?.retire(version)
    }

    /// Retained versions in order.
    pub fn versions(&self) -> Result<Vec<u32>, CryptoError> {
        self.lock()?.versions()
    }

    /// The current version for new writes.
    pub fn current(&self) -> Result<u32, CryptoError> {
        self.lock()?.current()
    }
}
