//! Atomic compare-and-swap storage. Backends must make a successful mutation durable before returning.
use super::CommandRecord;
use async_trait::async_trait;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, PoisonError},
};

#[derive(Debug)]
pub struct StoreError(Box<dyn std::error::Error + Send + Sync>);
impl StoreError {
    pub fn new(error: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self(Box::new(error))
    }
    pub fn message(message: impl Into<String>) -> Self {
        Self::new(std::io::Error::other(message.into()))
    }
}
impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for StoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}
impl From<std::io::Error> for StoreError {
    fn from(error: std::io::Error) -> Self {
        Self::new(error)
    }
}
impl From<serde_json::Error> for StoreError {
    fn from(error: serde_json::Error) -> Self {
        Self::new(error)
    }
}

/// Inserts are idempotent only for the identical command. CAS must preserve the
/// command, check the exact generation, and increment it by one atomically.
#[async_trait]
pub trait CommandStore: Send + Sync {
    async fn get(&self, id: &str) -> Result<Option<CommandRecord>, StoreError>;
    async fn list(&self) -> Result<Vec<CommandRecord>, StoreError>;
    async fn insert(&self, record: CommandRecord) -> Result<CommandRecord, StoreError>;
    async fn compare_exchange(
        &self,
        expected: u64,
        record: CommandRecord,
    ) -> Result<bool, StoreError>;
}

/// An ephemeral backend for tests. Use a durable backend for crash recovery.
#[derive(Clone, Default)]
pub struct MemoryCommandStore(Arc<Mutex<BTreeMap<String, CommandRecord>>>);
fn insert(
    records: &mut BTreeMap<String, CommandRecord>,
    record: CommandRecord,
) -> Result<CommandRecord, StoreError> {
    record.validate()?;
    let id = record.request_id()?.to_owned();
    if let Some(previous) = records.get(&id) {
        if previous.command != record.command {
            return Err(StoreError::message(
                "command identity reused for different work",
            ));
        }
        return Ok(previous.clone());
    }
    if record.generation != 0 {
        return Err(StoreError::message("new command generation must be zero"));
    }
    records.insert(id, record.clone());
    Ok(record)
}
fn compare_exchange(
    records: &mut BTreeMap<String, CommandRecord>,
    expected: u64,
    record: CommandRecord,
) -> Result<bool, StoreError> {
    record.validate()?;
    let id = record.request_id()?.to_owned();
    let Some(previous) = records.get(&id) else {
        return Ok(false);
    };
    if previous.generation != expected {
        return Ok(false);
    }
    if expected.checked_add(1) != Some(record.generation) || previous.command != record.command {
        return Err(StoreError::message(
            "CAS must increment generation and preserve the original command",
        ));
    }
    records.insert(id, record);
    Ok(true)
}
#[async_trait]
impl CommandStore for MemoryCommandStore {
    async fn get(&self, id: &str) -> Result<Option<CommandRecord>, StoreError> {
        Ok(self
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(id)
            .cloned())
    }
    async fn list(&self) -> Result<Vec<CommandRecord>, StoreError> {
        Ok(self
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .cloned()
            .collect())
    }
    async fn insert(&self, record: CommandRecord) -> Result<CommandRecord, StoreError> {
        insert(
            &mut self.0.lock().unwrap_or_else(PoisonError::into_inner),
            record,
        )
    }
    async fn compare_exchange(
        &self,
        expected: u64,
        record: CommandRecord,
    ) -> Result<bool, StoreError> {
        compare_exchange(
            &mut self.0.lock().unwrap_or_else(PoisonError::into_inner),
            expected,
            record,
        )
    }
}

/// A checksummed append-only journal, synced before acknowledging each transition.
/// OS file locking serializes independent store instances and processes. A torn
/// final line is repaired on open; corruption in a complete entry is refused.
/// The journal is replayed per operation. Large installations can implement
/// `CommandStore` on their application's transactional database instead.
#[derive(Clone)]
pub struct FileCommandStore {
    path: Arc<PathBuf>,
}
const MAX_ENTRY_BYTES: usize = 16 * 1024 * 1024;
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    checksum: String,
    record: CommandRecord,
}
fn checksum(record: &CommandRecord) -> Result<String, StoreError> {
    let mut value = serde_json::to_value(record)?;
    value.sort_all_objects();
    Ok(flower_protocol::hex(&Sha256::digest(serde_json::to_vec(
        &value,
    )?)))
}
impl FileCommandStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let path = std::path::absolute(path)?;
        if let Some(parent) = path.parent().filter(|path| !path.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent)?;
        }
        let store = Self {
            path: Arc::new(path),
        };
        store.transact(|_, _| Ok(()))?;
        Ok(store)
    }
    pub fn path(&self) -> &Path {
        self.path.as_ref()
    }
    fn transact<T>(
        &self,
        operation: impl FnOnce(&mut File, &mut BTreeMap<String, CommandRecord>) -> Result<T, StoreError>,
    ) -> Result<T, StoreError> {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(self.path())?;
        file.lock()?;
        let mut records: BTreeMap<String, CommandRecord> = BTreeMap::new();
        let mut offset = 0u64;
        let mut partial = false;
        {
            let mut reader = BufReader::new(&mut file);
            loop {
                let mut bytes = Vec::new();
                let count = reader.read_until(b'\n', &mut bytes)?;
                if count == 0 {
                    break;
                }
                if bytes.len() > MAX_ENTRY_BYTES {
                    return Err(StoreError::message("journal entry exceeds 16 MiB"));
                }
                if bytes.last() != Some(&b'\n') {
                    partial = true;
                    break;
                }
                let entry: Entry = serde_json::from_slice(&bytes)?;
                if checksum(&entry.record)? != entry.checksum {
                    return Err(StoreError::message(
                        "journal checksum mismatch; refusing to discard a complete entry",
                    ));
                }
                let id = entry.record.request_id()?.to_owned();
                if let Some(generation) = records.get(&id).map(|record| record.generation) {
                    if !compare_exchange(&mut records, generation, entry.record)? {
                        return Err(StoreError::message("invalid journal generation"));
                    }
                } else {
                    insert(&mut records, entry.record)?;
                }
                offset += count as u64;
            }
        }
        if partial {
            file.set_len(offset)?;
        }
        file.seek(SeekFrom::End(0))?;
        file.sync_all()?;
        #[cfg(unix)]
        if let Some(parent) = self.path().parent().filter(|p| !p.as_os_str().is_empty()) {
            File::open(parent)?.sync_all()?;
        }
        operation(&mut file, &mut records)
    }
    async fn blocking<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&mut File, &mut BTreeMap<String, CommandRecord>) -> Result<T, StoreError>
        + Send
        + 'static,
    ) -> Result<T, StoreError> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || store.transact(operation))
            .await
            .map_err(StoreError::new)?
    }
}
fn append(file: &mut File, record: CommandRecord) -> Result<(), StoreError> {
    let entry = Entry {
        checksum: checksum(&record)?,
        record,
    };
    let mut bytes = serde_json::to_vec(&entry)?;
    bytes.push(b'\n');
    if bytes.len() > MAX_ENTRY_BYTES {
        return Err(StoreError::message("journal entry exceeds 16 MiB"));
    }
    file.write_all(&bytes)?;
    file.sync_all()?;
    Ok(())
}
#[async_trait]
impl CommandStore for FileCommandStore {
    async fn get(&self, id: &str) -> Result<Option<CommandRecord>, StoreError> {
        let id = id.to_owned();
        self.blocking(move |_, records| Ok(records.get(&id).cloned()))
            .await
    }
    async fn list(&self) -> Result<Vec<CommandRecord>, StoreError> {
        self.blocking(|_, records| Ok(records.values().cloned().collect()))
            .await
    }
    async fn insert(&self, record: CommandRecord) -> Result<CommandRecord, StoreError> {
        self.blocking(move |file, records| {
            let existed = records.contains_key(record.request_id()?);
            let inserted = insert(records, record)?;
            if !existed {
                append(file, inserted.clone())?;
            }
            Ok(inserted)
        })
        .await
    }
    async fn compare_exchange(
        &self,
        expected: u64,
        record: CommandRecord,
    ) -> Result<bool, StoreError> {
        self.blocking(move |file, records| {
            let changed = compare_exchange(records, expected, record.clone())?;
            if changed {
                append(file, record)?;
            }
            Ok(changed)
        })
        .await
    }
}
