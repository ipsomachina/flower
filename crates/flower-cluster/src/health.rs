//! Storage failure: the first disk failure under a member's files.
//!
//! redb refuses every later transaction of a handle after an I/O error
//! (`PreviousIo`) until the file is reopened, and OpenRaft stops a member
//! for good on a storage error, so a failure is permanent for the
//! process. [`StorageHealth`] keeps the first one. The member then
//! reports it through readiness, refuses writes and linearizable reads
//! with [`STORAGE_FAILED`] before they reach the store, and stops its
//! maintenance and compaction. Nothing acknowledged is lost: an
//! acknowledgement follows durable commits on a quorum of logs, and
//! reopening the files recovers them, replaying from the log what the
//! group file had not flushed (see [`crate::flush`]): a full disk under
//! the group file surfaces at its next durable commit, the periodic
//! flush of applies, rather than at the apply. [`writable`] probes the
//! data directory, so `serve` can exit for its supervisor to restart it
//! once the disk accepts writes again and has a margin free
//! ([`DEFAULT_RECOVERY_FREE_BYTES`] unless set, as v1's
//! `FLOWER_STORAGE_RECOVERY_FREE_BYTES`): a restart into a disk with
//! room for the probe alone would fail again at its first commits and
//! loop.

use std::fmt::Display;
use std::path::Path;
use std::sync::OnceLock;

use crate::exec::now_ms;

/// The failure code of requests refused after a storage failure.
pub const STORAGE_FAILED: &str = "storage_failed";

/// Component of a failure in the group file (`group.redb`).
pub const GROUP: &str = "group";
/// Component of a failure in the Raft file (`raft.redb`).
pub const RAFT_LOG: &str = "raft_log";
/// Component of a Raft core that stopped on its own: on a storage error
/// of either file, or a panic.
pub const RAFT: &str = "raft";

/// The probe file [`writable`] writes, syncs and removes.
pub const PROBE_FILE: &str = ".flower-write-probe";
/// Bytes [`writable`] writes: enough that a disk with room for a probe
/// but not a page of the file doesn't pass.
pub const PROBE_BYTES: usize = 1 << 20;
/// Free space the data directory's file system needs, by default,
/// before a member whose storage failed exits to be restarted (v1's).
pub const DEFAULT_RECOVERY_FREE_BYTES: u64 = 64 << 20;

/// The first failure that stopped a member's storage.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageFailure {
    /// Which file or core failed: [`GROUP`], [`RAFT_LOG`] or [`RAFT`].
    pub component: &'static str,
    /// The error, as text.
    pub error: String,
    /// When, in milliseconds since the Unix epoch.
    pub since_ms: u64,
}

impl Display for StorageFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "storage failed ({}): {}; writes stop until the process restarts",
            self.component, self.error
        )
    }
}

/// A member's storage state: healthy until the first failure, which is
/// kept for the life of the process.
#[derive(Debug, Default)]
pub struct StorageHealth {
    failure: OnceLock<StorageFailure>,
}

impl StorageHealth {
    /// Record a failure and return the one kept: only the first counts.
    pub fn fail(&self, component: &'static str, error: impl Display) -> &StorageFailure {
        let mut first = false;
        let failure = self.failure.get_or_init(|| {
            first = true;
            StorageFailure {
                component,
                error: error.to_string(),
                since_ms: now_ms(),
            }
        });
        if first {
            tracing::error!(
                target: "flower::storage",
                component,
                error = %failure.error,
                "storage failed: writes stop until the process restarts"
            );
        }
        failure
    }

    /// The first failure, if any.
    pub fn failure(&self) -> Option<&StorageFailure> {
        self.failure.get()
    }
}

/// Whether `directory` accepts writes with `free` bytes to spare: its
/// file system has that much available to this process, and
/// [`PROBE_BYTES`] written to [`PROBE_FILE`] in it sync (the file is
/// removed). Blocking.
pub fn writable(directory: &Path, free: u64) -> std::io::Result<()> {
    use std::io::Write;
    if let Some(available) = available_bytes(directory)?
        && available < free
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::StorageFull,
            format!("{available} bytes available, {free} needed"),
        ));
    }
    let path = directory.join(PROBE_FILE);
    let result = (|| {
        let mut file = std::fs::File::create(&path)?;
        file.write_all(&vec![0; PROBE_BYTES])?;
        file.sync_all()
    })();
    let _ = std::fs::remove_file(&path);
    result
}

/// Bytes available to this process on `directory`'s file system.
#[cfg(all(unix, not(target_vendor = "apple")))]
pub fn available_bytes(directory: &Path) -> std::io::Result<Option<u64>> {
    use std::os::unix::ffi::OsStrExt;
    let path =
        std::ffi::CString::new(directory.as_os_str().as_bytes()).map_err(std::io::Error::other)?;
    // SAFETY: libc::statvfs contains only integer fields, which may all be zero.
    let mut stats: libc::statvfs = unsafe { std::mem::zeroed() };
    // SAFETY: path is NUL-terminated and stats is valid writable storage;
    // both outlive the call, which retains neither pointer.
    if unsafe { libc::statvfs(path.as_ptr(), &mut stats) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(Some(
        (stats.f_bavail as u64).saturating_mul(stats.f_frsize as u64),
    ))
}

/// Bytes available to this process on `directory`'s file system. Apple's
/// `statvfs` counts blocks in 32 bits, which fail or wrap past 16 TiB of
/// 4 KiB blocks; its `statfs` counts them in 64.
#[cfg(target_vendor = "apple")]
pub fn available_bytes(directory: &Path) -> std::io::Result<Option<u64>> {
    use std::os::unix::ffi::OsStrExt;
    let path =
        std::ffi::CString::new(directory.as_os_str().as_bytes()).map_err(std::io::Error::other)?;
    // SAFETY: libc::statfs contains only integers and arrays of integers,
    // which may all be zero.
    let mut stats: libc::statfs = unsafe { std::mem::zeroed() };
    // SAFETY: path is NUL-terminated and stats is valid writable storage;
    // both outlive the call, which retains neither pointer.
    if unsafe { libc::statfs(path.as_ptr(), &mut stats) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(Some(
        stats.f_bavail.saturating_mul(u64::from(stats.f_bsize)),
    ))
}

/// Bytes available to this process on `directory`'s volume.
#[cfg(windows)]
pub fn available_bytes(directory: &Path) -> std::io::Result<Option<u64>> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::GetDiskFreeSpaceExW;
    let path: Vec<u16> = directory
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect();
    let mut available = 0u64;
    // SAFETY: the path is NUL-terminated and outlives the call; the one
    // output it is given is a valid u64, the others are null.
    let ok = unsafe {
        GetDiskFreeSpaceExW(
            path.as_ptr(),
            &mut available,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        )
    };
    if ok == 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(Some(available))
}

/// Bytes available on `directory`'s file system: unknown here, so the
/// probe alone decides.
#[cfg(not(any(unix, windows)))]
pub fn available_bytes(_directory: &Path) -> std::io::Result<Option<u64>> {
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_first_failure_is_kept() {
        let health = StorageHealth::default();
        assert!(health.failure().is_none());
        let first = health.fail(GROUP, "No space left on device").clone();
        assert_eq!(first.component, GROUP);
        assert!(first.since_ms > 0);
        assert_eq!(health.fail(RAFT, "raft stopped"), &first);
        assert_eq!(health.failure(), Some(&first));
        let shown = first.to_string();
        assert!(shown.contains("(group)"), "{shown}");
        assert!(shown.contains("No space left"), "{shown}");
    }

    #[test]
    fn a_writable_directory_passes_the_probe_and_keeps_no_file() {
        let dir = tempfile::tempdir().unwrap();
        writable(dir.path(), 0).unwrap();
        assert!(!dir.path().join(PROBE_FILE).exists());
        assert!(writable(&dir.path().join("missing"), 0).is_err());
    }

    #[test]
    fn a_directory_short_of_the_free_space_threshold_fails_the_probe() {
        let dir = tempfile::tempdir().unwrap();
        let available = available_bytes(dir.path()).unwrap().unwrap();
        assert!(available > 0);
        writable(dir.path(), available / 2).unwrap();
        // More than the disk has: refused before anything is written.
        let error = writable(dir.path(), u64::MAX).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::StorageFull, "{error}");
        assert!(error.to_string().contains("needed"), "{error}");
        assert!(!dir.path().join(PROBE_FILE).exists());
    }
}
