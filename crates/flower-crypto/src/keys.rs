//! The deployment keystore: versioned 256-bit keys wrapped under
//! an operator key-encryption key.
//!
//! One backup key covers a deployment; rotation introduces a new
//! current version without rewriting existing objects. Old versions
//! stay readable while referenced. Retirement removes material and
//! is gated by the caller: only versions no live object references
//! may retire, and the current version never retires. Rewrapping
//! under a new KEK is a distinct lifecycle from rotation.

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::aead::{CryptoError, NONCE_LEN, open_raw, seal_raw};

/// A KEK-wrapped key version.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
struct WrappedKey {
    /// Wrapping nonce bytes.
    nonce: Vec<u8>,
    /// Wrapped 32-byte key plus tag.
    sealed: Vec<u8>,
}

/// Persisted keystore state.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
struct KeystoreState {
    /// Current version for new writes; zero when empty.
    current: u32,
    /// Wrapped material by version.
    versions: BTreeMap<u32, WrappedKey>,
}

impl KeystoreState {
    /// Whether a usable current version exists.
    fn has_current(&self) -> bool {
        self.current != 0 && self.versions.contains_key(&self.current)
    }
}

/// Check 32-byte key material from a slice.
pub fn kek_from_slice(bytes: &[u8]) -> Result<[u8; 32], CryptoError> {
    bytes
        .try_into()
        .map_err(|_| CryptoError::BadKeyLength(bytes.len()))
}

/// A sibling path: `<file name>.<suffix>` in the same directory, so
/// renames stay on one filesystem and distinct stores never share
/// helper files.
fn sibling(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.file_name().map(OsString::from).unwrap_or_default();
    name.push(".");
    name.push(suffix);
    path.with_file_name(name)
}

/// An exclusive inter-process lock on a store's sibling `.lock` file,
/// released on drop. The data file itself is replaced by rename, so
/// it cannot carry the lock; the lock file is never replaced.
pub(crate) struct StoreLock {
    _file: File,
}

/// Block until this handle holds the store's exclusive lock. OS file
/// locks belong to the open handle, so they serialize handles within
/// one process as well as across processes, and a crashed holder
/// releases its lock with its handles.
pub(crate) fn lock_store(path: &Path) -> Result<StoreLock, CryptoError> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(sibling(path, "lock"))
        .map_err(io_error)?;
    file.lock().map_err(io_error)?;
    Ok(StoreLock { _file: file })
}

/// Map an IO failure.
fn io_error(error: std::io::Error) -> CryptoError {
    CryptoError::Io(error.to_string())
}

/// Write bytes atomically and durably: a sibling temp file written
/// and fsynced through one handle, renamed over the live file, then
/// the directory synced so the rename itself survives a crash.
/// Readers see the old or the new bytes, never a mix. Writers hold
/// the store lock, so the fixed temp name never races. Shared by the
/// keystore and the managed-key store.
pub(crate) fn save_bytes_atomic(path: &Path, bytes: &[u8]) -> Result<(), CryptoError> {
    let tmp = sibling(path, "tmp");
    let written = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(&tmp, path)
    })();
    if let Err(error) = written {
        // Never leave a half-written temp behind; the live file is
        // untouched until the rename succeeds.
        let _ = std::fs::remove_file(&tmp);
        return Err(io_error(error));
    }
    sync_parent(path).map_err(io_error)
}

/// Sync the parent directory so the rename is durable. Filesystems
/// that cannot fsync directories report it as unsupported or
/// invalid, which leaves the file sync as the whole guarantee.
#[cfg(unix)]
fn sync_parent(path: &Path) -> std::io::Result<()> {
    let parent = match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    };
    match File::open(parent)?.sync_all() {
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::InvalidInput | std::io::ErrorKind::Unsupported
            ) =>
        {
            Ok(())
        }
        other => other,
    }
}

/// std cannot open directories for syncing outside Unix. On Windows
/// NTFS journals the rename's metadata, and the file sync above is
/// the whole guarantee.
#[cfg(not(unix))]
fn sync_parent(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

/// A deployment keystore backed by one JSON file, written
/// atomically on every mutation. Every operation loads fresh state
/// from disk, so handles never go stale: rotations and retirements
/// through one handle are visible to all others immediately.
/// Mutations hold an exclusive file lock across load-modify-save and
/// reload under it, so concurrent handles — in this process or
/// another, such as the online and offline backup drivers — never
/// mint the same version twice or lose each other's writes.
pub struct Keystore {
    path: PathBuf,
    kek: [u8; 32],
}

impl Keystore {
    /// Open the keystore at `path`, empty when the file is missing.
    /// Corrupt files fail loudly instead of starting empty.
    pub fn open(path: &Path, kek: [u8; 32]) -> Result<Self, CryptoError> {
        let store = Keystore {
            path: path.to_path_buf(),
            kek,
        };
        store.load()?;
        Ok(store)
    }

    /// Load fresh state, empty when the file is missing. Atomic
    /// replacement means readers never need the lock.
    fn load(&self) -> Result<KeystoreState, CryptoError> {
        match std::fs::read(&self.path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(KeystoreState {
                current: 0,
                versions: BTreeMap::new(),
            }),
            Err(error) => Err(io_error(error)),
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map_err(|error| CryptoError::Store(error.to_string())),
        }
    }

    /// Persist state atomically and durably through a sibling temp
    /// file plus rename. Callers hold the store lock.
    fn save(&self, state: &KeystoreState) -> Result<(), CryptoError> {
        let bytes =
            serde_json::to_vec(state).map_err(|error| CryptoError::Store(error.to_string()))?;
        save_bytes_atomic(&self.path, &bytes)
    }

    /// The current version, creating version 1 on an empty store.
    /// Concurrent callers on an empty store agree on one version 1.
    pub fn ensure_current(&self) -> Result<u32, CryptoError> {
        let state = self.load()?;
        if state.has_current() {
            return Ok(state.current);
        }
        let _lock = lock_store(&self.path)?;
        // Another handle may have created it while this one waited.
        let state = self.load()?;
        if state.has_current() {
            return Ok(state.current);
        }
        self.rotate_locked(state)
    }

    /// The current version for new writes; zero when empty.
    pub fn current(&self) -> Result<u32, CryptoError> {
        Ok(self.load()?.current)
    }

    /// Retained versions in order.
    pub fn versions(&self) -> Result<Vec<u32>, CryptoError> {
        Ok(self.load()?.versions.keys().copied().collect())
    }

    /// Unwrap a version's key material.
    pub fn get(&self, version: u32) -> Result<[u8; 32], CryptoError> {
        let state = self.load()?;
        let Some(wrapped) = state.versions.get(&version) else {
            return Err(CryptoError::UnknownVersion(version));
        };
        let bytes = open_raw(&self.kek, &wrapped.nonce, &wrapped.sealed)?;
        kek_from_slice(&bytes)
    }

    /// Rotate: generate a fresh version and make it current.
    /// Existing objects keep their versions; no rewrite happens.
    pub fn rotate(&self) -> Result<u32, CryptoError> {
        let _lock = lock_store(&self.path)?;
        let state = self.load()?;
        self.rotate_locked(state)
    }

    /// Append a fresh version to state loaded under the lock.
    fn rotate_locked(&self, mut state: KeystoreState) -> Result<u32, CryptoError> {
        let version = state
            .versions
            .keys()
            .next_back()
            .copied()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| CryptoError::Store("key versions exhausted".to_string()))?;
        let key: [u8; 32] = rand::random();
        let nonce: [u8; NONCE_LEN] = rand::random();
        let sealed = seal_raw(&self.kek, &nonce, &key)?;
        state.versions.insert(
            version,
            WrappedKey {
                nonce: nonce.to_vec(),
                sealed,
            },
        );
        state.current = version;
        self.save(&state)?;
        Ok(version)
    }

    /// Retire a version's material. The current version and unknown
    /// versions refuse with `false`; retirement only deletes, so the
    /// caller gates on live references first.
    pub fn retire(&self, version: u32) -> Result<bool, CryptoError> {
        let _lock = lock_store(&self.path)?;
        let mut state = self.load()?;
        if version == state.current || !state.versions.contains_key(&version) {
            return Ok(false);
        }
        state.versions.remove(&version);
        self.save(&state)?;
        Ok(true)
    }

    /// Rewrap every version under a new KEK, replacing the old one.
    pub fn rewrap(&mut self, kek: [u8; 32]) -> Result<(), CryptoError> {
        let _lock = lock_store(&self.path)?;
        let state = self.load()?;
        let mut versions = BTreeMap::new();
        for (version, wrapped) in &state.versions {
            let bytes = open_raw(&self.kek, &wrapped.nonce, &wrapped.sealed)?;
            let nonce: [u8; NONCE_LEN] = rand::random();
            let sealed = seal_raw(&kek, &nonce, &bytes)?;
            versions.insert(
                *version,
                WrappedKey {
                    nonce: nonce.to_vec(),
                    sealed,
                },
            );
        }
        self.save(&KeystoreState {
            current: state.current,
            versions,
        })?;
        self.kek = kek;
        Ok(())
    }
}
