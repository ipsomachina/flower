//! `FLOWER_FSYNC=off`: a member whose files never call fsync.
//!
//! redb flushes its file at each durable commit, which is most of a
//! commit's time on a slow disk, and a follower commits each entry's
//! append durably (the leader's own appends and every apply defer their
//! flush, see [`crate::flush`]). With fsync off, both files go
//! through a backend whose `sync_data` returns at once, and snapshot
//! images and their directory aren't synced either. Every write still
//! reaches the kernel before its commit returns, so the files keep
//! everything acknowledged across a crash or kill of the process; only a
//! crash of the operating system or a power loss can lose recent
//! commits, or leave a file redb cannot open.
//!
//! A member back from such a crash may hold a log missing entries it
//! acknowledged, which its leader counted toward a quorum: voting as usual,
//! it could elect a member lacking a committed entry. So each file that may
//! hold unsynced writes has a marker beside it, `<file>.unsynced`, holding
//! the kernel's boot ID (Linux's `/proc/sys/kernel/random/boot_id`, macOS's
//! `kern.bootsessionuuid`, Windows's boot time as its kernel recorded it,
//! less the clock changes made since). It is written and synced before the
//! file opens unsynced, and removed once the file is synced: at a clean
//! [`crate::node::Node::shutdown`], or when the file opens with fsync on. A
//! marker holding another boot ID means the machine went down with unsynced
//! writes in the file: the member then opens *rejoining* ([`crate::rejoin`]),
//! voting, campaigning and serving only once it caught up with its group's
//! leader. Its rejoining mark is synced into the Raft file before the markers
//! that caught the loss are rewritten for this boot (or, with fsync on,
//! removed), so no crash in between makes it an ordinary voter. A group of
//! one has no leader to catch up from: to trust its files as they are, remove
//! the markers before it starts again. A file the loss left unreadable to
//! redb still refuses to open; empty the data directory so the member comes
//! back as a new one.
//!
//! Without a boot ID (on other systems, or when the kernel won't say),
//! a marker can't be judged and is taken as this boot's: fsync off there
//! gives no protection against power loss.
//!
//! For development stacks, tests, CI, and members of groups whose voters
//! don't go down together. Durability is the default; any value but
//! those [`parse`] takes is an error at startup, never a silent default.

use std::io::{self, Write};
use std::ops::Bound;
use std::path::{Path, PathBuf};

use redb::backends::FileBackend;
use redb::{BackendError, StorageBackend};

/// The variable [`Durability::from_env`] reads.
pub const ENV: &str = "FLOWER_FSYNC";

/// Marker content when the boot is unknown.
const UNKNOWN_BOOT: &str = "unknown";

/// Whether a member syncs its files, and the boot it runs in.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Durability {
    /// Whether commits fsync (the default).
    pub fsync: bool,
    /// This boot of the machine, when the platform names it.
    pub boot: Option<String>,
}

impl Durability {
    /// Durable commits.
    pub fn on() -> Self {
        Durability {
            fsync: true,
            boot: boot_id(),
        }
    }

    /// Unsynced commits, judged against this boot.
    pub fn off() -> Self {
        Durability {
            fsync: false,
            boot: boot_id(),
        }
    }

    /// From a `FLOWER_FSYNC` value (see [`parse`]).
    pub fn parse(value: Option<&str>) -> Result<Self, String> {
        Ok(if parse(value)? {
            Self::on()
        } else {
            Self::off()
        })
    }

    /// From the `FLOWER_FSYNC` environment variable.
    pub fn from_env() -> Result<Self, String> {
        Self::parse(std::env::var(ENV).ok().as_deref())
    }
}

/// Parse a `FLOWER_FSYNC` value: `on` (the default, also `true` and `1`)
/// or `off` (`false`, `0`).
pub fn parse(value: Option<&str>) -> Result<bool, String> {
    match value.map(str::trim) {
        None | Some("") => Ok(true),
        Some(value) => match value.to_ascii_lowercase().as_str() {
            "on" | "true" | "1" => Ok(true),
            "off" | "false" | "0" => Ok(false),
            _ => Err(format!("{ENV} must be on or off, not {value:?}")),
        },
    }
}

/// The marker beside a file that may hold unsynced writes.
pub fn marker(file: &Path) -> PathBuf {
    let mut name = file.file_name().unwrap_or_default().to_owned();
    name.push(".unsynced");
    file.with_file_name(name)
}

/// This boot of the machine, when the platform names it.
#[cfg(target_os = "linux")]
pub fn boot_id() -> Option<String> {
    std::fs::read_to_string("/proc/sys/kernel/random/boot_id")
        .ok()
        .map(|id| id.trim().to_owned())
        .filter(|id| !id.is_empty())
}

/// This boot of the machine, when the platform names it: the boot session's
/// UUID.
#[cfg(target_os = "macos")]
pub fn boot_id() -> Option<String> {
    let mut buffer = [0u8; 128];
    let mut length = buffer.len();
    // SAFETY: the name is NUL-terminated, and the buffer and its length
    // match.
    let status = unsafe {
        libc::sysctlbyname(
            c"kern.bootsessionuuid".as_ptr(),
            buffer.as_mut_ptr().cast(),
            &mut length,
            std::ptr::null_mut(),
            0,
        )
    };
    if status != 0 {
        return None;
    }
    session_id(&buffer[..length.min(buffer.len())])
}

/// A boot session ID as `sysctl` answers it: text, NUL-terminated.
#[cfg(any(target_os = "macos", test))]
fn session_id(bytes: &[u8]) -> Option<String> {
    let id = String::from_utf8_lossy(bytes);
    let id = id.trim_matches(char::from(0)).trim();
    (!id.is_empty()).then(|| id.to_owned())
}

/// This boot of the machine, when the platform names it: the time it
/// booted, as the kernel keeps it (`SYSTEM_TIMEOFDAY_INFORMATION`).
#[cfg(windows)]
pub fn boot_id() -> Option<String> {
    use windows_sys::Wdk::System::SystemInformation::{
        NtQuerySystemInformation, SystemTimeOfDayInformation,
    };
    // Words, for the structure's alignment.
    let mut info = [0u64; 6];
    let size = std::mem::size_of_val(&info) as u32;
    let mut length = 0u32;
    // SAFETY: the buffer is `size` bytes long, aligned for the
    // structure, and the length out-pointer is a valid u32.
    let status = unsafe {
        NtQuerySystemInformation(
            SystemTimeOfDayInformation,
            info.as_mut_ptr().cast(),
            size,
            &mut length,
        )
    };
    if status < 0 || length != size {
        return None;
    }
    boot_time_id(&info)
}

/// A boot ID from `SYSTEM_TIMEOFDAY_INFORMATION` read as words: the
/// boot time (100 ns ticks since 1601) less the boot time bias. Setting
/// the clock moves the kernel's boot time and adds the same step to its
/// bias, so the difference stays the time it booted at, and only
/// another boot changes it. `None` for a kernel that reports no boot
/// time.
#[cfg(any(windows, test))]
fn boot_time_id(info: &[u64; 6]) -> Option<String> {
    let (boot, bias) = (info[0], info[4]);
    (boot != 0).then(|| format!("boot-time-{}", boot.wrapping_sub(bias)))
}

/// This boot of the machine, when the platform names it.
#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
pub fn boot_id() -> Option<String> {
    None
}

/// What a marker an earlier process left says of its file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Left {
    /// No marker: the file was synced.
    Nothing,
    /// Unsynced writes from this boot: the kernel still holds them.
    ThisBoot,
    /// Unsynced writes from an earlier boot: they may be lost.
    EarlierBoot,
}

/// The marker beside `file`: `None` without one, else the boot its
/// unsynced writes are from, when that was known.
pub fn read_marker(file: &Path) -> io::Result<Option<Option<String>>> {
    let content = match std::fs::read_to_string(marker(file)) {
        Ok(content) => content,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let boot = content
        .lines()
        .next()
        .map(str::trim)
        .filter(|boot| !boot.is_empty() && *boot != UNKNOWN_BOOT)
        .map(str::to_owned);
    Ok(Some(boot))
}

/// What a marker says of its file as of `boot`. Without both boot IDs a
/// marker can't be judged: its writes are taken as this boot's.
pub fn judge(marker: Option<&Option<String>>, boot: Option<&str>) -> Left {
    match (marker, boot) {
        (None, _) => Left::Nothing,
        (Some(Some(left)), Some(boot)) if left != boot => Left::EarlierBoot,
        (Some(_), _) => Left::ThisBoot,
    }
}

/// Record, durably, that `file` may hold unsynced writes from `boot`.
pub fn mark(file: &Path, boot: Option<&str>) -> io::Result<()> {
    let marker = marker(file);
    let mut temporary = marker.clone().into_os_string();
    temporary.push(".tmp");
    let temporary = PathBuf::from(temporary);
    {
        let mut out = std::fs::File::create(&temporary)?;
        writeln!(out, "{}", boot.unwrap_or(UNKNOWN_BOOT))?;
        out.sync_all()?;
    }
    std::fs::rename(&temporary, &marker)?;
    sync_directory(&marker)
}

/// Sync `file`, through a handle of its own: what any handle wrote
/// reaches the disk.
pub fn sync(file: &Path) -> io::Result<()> {
    if file.exists() {
        // Windows flushes only handles opened for writing.
        std::fs::OpenOptions::new()
            .write(true)
            .open(file)?
            .sync_all()?;
    }
    Ok(())
}

/// Sync `file`, then remove its marker: nothing in it is unsynced.
pub fn settle(file: &Path) -> io::Result<()> {
    sync(file)?;
    let marker = marker(file);
    match std::fs::remove_file(&marker) {
        Ok(()) => sync_directory(&marker),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

/// Make a rename or removal in a file's directory durable. Windows has
/// no directory handle to sync; NTFS journals the metadata.
fn sync_directory(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::File::open(parent)?.sync_all()?;
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

/// Whether `file` may have lost writes: an earlier boot left it unsynced.
/// Its member must rejoin its group (see the module documentation).
pub fn lost_writes(file: &Path, durability: &Durability) -> io::Result<bool> {
    let left = read_marker(file)?;
    Ok(judge(left.as_ref(), durability.boot.as_deref()) == Left::EarlierBoot)
}

/// Once a member that found [`lost_writes`] holds a durable rejoining
/// mark: record `file` as this boot's, settling it with fsync on and
/// marking it with this boot's ID with fsync off.
pub fn resettle(file: &Path, durability: &Durability) -> io::Result<()> {
    if durability.fsync {
        settle(file)
    } else {
        mark(file, durability.boot.as_deref())
    }
}

/// Open or create the redb file at `path` as `durability` says (see the
/// module documentation): settle one this boot left unsynced when
/// opening with fsync on, and mark one before opening it unsynced. A
/// file an earlier boot left unsynced opens with its marker untouched:
/// the member rejoins, then [`resettle`]s it.
pub fn open(path: &Path, durability: &Durability) -> Result<redb::Database, redb::DatabaseError> {
    let io = |error: io::Error| redb::DatabaseError::Storage(redb::StorageError::Io(error));
    let left = read_marker(path).map_err(io)?;
    let earlier = match judge(left.as_ref(), durability.boot.as_deref()) {
        Left::EarlierBoot => true,
        Left::ThisBoot if durability.fsync => {
            settle(path).map_err(io)?;
            false
        }
        Left::ThisBoot | Left::Nothing => false,
    };
    if durability.fsync {
        return redb::Database::create(path);
    }
    if !earlier {
        mark(path, durability.boot.as_deref()).map_err(io)?;
    }
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
        .map_err(io)?;
    redb::Builder::new().create_with_backend(Unsynced(FileBackend::new(file)?))
}

/// A file whose flushes are left to the kernel.
#[derive(Debug)]
pub struct Unsynced(pub FileBackend);

impl StorageBackend for Unsynced {
    fn len(&self) -> Result<u64, io::Error> {
        self.0.len()
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> Result<(), io::Error> {
        self.0.read(offset, out)
    }

    fn set_len(&self, len: u64) -> Result<(), io::Error> {
        self.0.set_len(len)
    }

    fn sync_data(&self) -> Result<(), io::Error> {
        Ok(())
    }

    fn write(&self, offset: u64, data: &[u8]) -> Result<(), io::Error> {
        self.0.write(offset, data)
    }

    fn close(&self) -> Result<(), io::Error> {
        self.0.close()
    }

    fn try_lock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<bool, BackendError> {
        self.0.try_lock_range(start, end)
    }

    fn try_lock_shared_range(
        &self,
        start: Bound<u64>,
        end: Bound<u64>,
    ) -> Result<bool, BackendError> {
        self.0.try_lock_shared_range(start, end)
    }

    fn lock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<(), BackendError> {
        self.0.lock_range(start, end)
    }

    fn lock_shared_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<(), BackendError> {
        self.0.lock_shared_range(start, end)
    }

    fn unlock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<(), BackendError> {
        self.0.unlock_range(start, end)
    }

    fn query_lock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<bool, BackendError> {
        self.0.query_lock_range(start, end)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use redb::{ReadableDatabase, TableDefinition};

    const TABLE: TableDefinition<u64, u64> = TableDefinition::new("t");

    #[test]
    fn parses_on_and_off_and_refuses_anything_else() {
        assert!(parse(None).unwrap());
        assert!(parse(Some("")).unwrap());
        for on in ["on", "ON", "true", "1", " on "] {
            assert!(parse(Some(on)).unwrap(), "{on}");
        }
        for off in ["off", "Off", "false", "0"] {
            assert!(!parse(Some(off)).unwrap(), "{off}");
        }
        for bad in ["no", "yes", "2", "disabled"] {
            let error = parse(Some(bad)).unwrap_err();
            assert!(error.contains(ENV), "{error}");
        }
        assert!(!Durability::parse(Some("off")).unwrap().fsync);
    }

    #[test]
    fn boot_ids_parse_from_what_each_kernel_answers() {
        // macOS: the session UUID's text, NUL-terminated.
        assert_eq!(
            session_id(b"C6B3E7D1-0F9A-4E1B-9A65-2D9F7C1A0B42\0").as_deref(),
            Some("C6B3E7D1-0F9A-4E1B-9A65-2D9F7C1A0B42")
        );
        assert_eq!(session_id(b" id \n\0\0").as_deref(), Some("id"));
        assert_eq!(session_id(b"\0\0"), None);
        assert_eq!(session_id(b""), None);
        // Windows: boot time less its bias. A clock step moves both
        // alike and leaves the ID; another boot changes it.
        let booted = 134_379_430_383_006_275u64;
        let id = boot_time_id(&[booted, 0, 0, 0, 0, 0]).unwrap();
        assert_eq!(id, format!("boot-time-{booted}"));
        let step = 36_000_000_000u64;
        assert_eq!(
            boot_time_id(&[booted + step, 1, 2, 3, step, 9]).unwrap(),
            id
        );
        let back = 0u64.wrapping_sub(step);
        assert_eq!(
            boot_time_id(&[booted - step, 0, 0, 0, back, 0]).unwrap(),
            id
        );
        assert_ne!(boot_time_id(&[booted + 1, 0, 0, 0, 0, 0]).unwrap(), id);
        assert_eq!(boot_time_id(&[0; 6]), None);
    }

    /// Where the platform names its boot, every read names the same one,
    /// so a marker of this boot is never taken for an earlier one's.
    #[cfg(any(target_os = "linux", target_os = "macos", windows))]
    #[test]
    fn this_boot_has_one_id() {
        let id = boot_id().expect("the platform names its boot");
        assert!(!id.is_empty());
        for _ in 0..3 {
            assert_eq!(boot_id().as_ref(), Some(&id));
        }
        assert_eq!(Durability::off().boot, Some(id));
    }

    #[test]
    fn a_marker_tells_this_boot_from_an_earlier_one() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("group.redb");
        assert_eq!(marker(&file), dir.path().join("group.redb.unsynced"));
        let left = |boot| judge(read_marker(&file).unwrap().as_ref(), boot);
        assert_eq!(left(Some("a")), Left::Nothing);
        mark(&file, Some("a")).unwrap();
        assert_eq!(left(Some("a")), Left::ThisBoot);
        assert_eq!(left(Some("b")), Left::EarlierBoot);
        // Without this boot's ID, a marker is never taken for a crash,
        // nor is one whose own boot was unknown.
        assert_eq!(left(None), Left::ThisBoot);
        mark(&file, None).unwrap();
        assert_eq!(left(Some("b")), Left::ThisBoot);
        std::fs::write(&file, b"data").unwrap();
        settle(&file).unwrap();
        assert_eq!(left(Some("b")), Left::Nothing);
        settle(&file).unwrap();
    }

    /// Commits through the unsynced backend survive closing and reopening
    /// the file (what a process crash leaves).
    #[test]
    fn unsynced_files_keep_commits_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("group.redb");
        let off = Durability {
            fsync: false,
            boot: Some("a".to_string()),
        };
        {
            let db = open(&path, &off).unwrap();
            let write = db.begin_write().unwrap();
            write.open_table(TABLE).unwrap().insert(1, 42).unwrap();
            write.commit().unwrap();
        }
        assert_eq!(read_marker(&path).unwrap(), Some(Some("a".to_string())));
        let db = open(&path, &off).unwrap();
        let read = db.begin_read().unwrap();
        let value = read.open_table(TABLE).unwrap().get(1).unwrap().unwrap();
        assert_eq!(value.value(), 42);
    }

    #[test]
    fn an_earlier_boots_marker_stays_until_its_member_resettles_it() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("raft.redb");
        let at = |fsync, boot: &str| Durability {
            fsync,
            boot: Some(boot.to_string()),
        };
        drop(open(&path, &at(false, "a")).unwrap());
        // After the machine restarted, either mode opens the file but
        // keeps the marker that caught the loss, until the member made
        // its rejoining durable.
        for durability in [at(false, "b"), at(true, "b")] {
            assert!(lost_writes(&path, &durability).unwrap());
            drop(open(&path, &durability).unwrap());
            assert_eq!(read_marker(&path).unwrap(), Some(Some("a".to_string())));
        }
        resettle(&path, &at(false, "b")).unwrap();
        assert_eq!(read_marker(&path).unwrap(), Some(Some("b".to_string())));
        assert!(!lost_writes(&path, &at(false, "b")).unwrap());
        resettle(&path, &at(true, "c")).unwrap();
        assert_eq!(read_marker(&path).unwrap(), None);
        // The same boot's kernel still holds the writes: opening with
        // fsync on syncs the file and drops the marker.
        drop(open(&path, &at(false, "c")).unwrap());
        assert!(!lost_writes(&path, &at(true, "c")).unwrap());
        drop(open(&path, &at(true, "c")).unwrap());
        assert_eq!(read_marker(&path).unwrap(), None);
    }
}
