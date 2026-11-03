//! The Raft log store: vote, entries, purge position and the latest
//! snapshot's record in a dedicated `raft.redb` file, and snapshot
//! images as files in `snapshots/` beside it.
//!
//! Every method commits through redb before reporting completion, so an
//! acknowledged write survives a crash, with one exception: the current
//! leader's own appends ([`LogFlushed::is_leader_append`]) commit without
//! an fsync, readable at once and ordered before every later write of the
//! file, and their callbacks complete once a durable commit covers them
//! (see [`crate::flush`]). Writes fsync on Tokio's blocking
//! pool, never on the async workers that drive the Raft core. The store
//! is shared by reference: the Raft core drives it through the async
//! traits while sync-down serving reads retained entries directly, from
//! one read transaction per batch. Retained entries double as the
//! sync-down window: positions purge only under durable snapshot
//! coverage, and purged positions fall back to snapshots.
//!
//! A snapshot image is a file (see [`crate::snapshot_file`]): a build
//! or an arriving transfer writes `<n>.part`, which is synced and
//! renamed to `<n>.snapshot` before the Raft file records it as current
//! (metadata and file name only); older images go after the record
//! commits. A build never replaces a newer recorded snapshot, which an
//! install that finished first left. Opening deletes parts and
//! unrecorded images a crash left.
//!
//! A snapshot install records its image and an install marker in one
//! transaction before touching the engine; the marker clears only after
//! the engine holds the whole snapshot and its position, so a crash
//! mid-install re-installs on restart instead of serving mixed state.
//!
//! A purge returns after one bounded transaction. OpenRaft awaits
//! [`RaftLogStorage::purge`] inside its core, which meanwhile answers no
//! AppendEntries, sends no heartbeat and confirms no read, so deleting
//! hours of log there stalled the member until an election. `purge`
//! records the purge point durably and deletes at most [`PURGE_CHUNK`]
//! entries in that transaction; a background task deletes the rest, a
//! chunk per transaction with a pause between them, so appends, votes
//! and reads interleave. Those transactions don't fsync: the next
//! durable commit persists them, and entries a crash leaves below the
//! purge point are harmless, since every read here starts above it.
//! The next purge sweeps them from the first entry on disk.
//!
//! With continuous backup on, a purge hold ([`LogStore::hold_purges`])
//! keeps purged entries on disk until the backup has archived them
//! ([`LogStore::release_purges`]): it lowers the bound both purges
//! delete through (`LogStore::sweep_bound`). The purge point Raft is
//! told of moves as usual and Raft reads nothing below it; only the
//! deletion waits, so log compaction never outruns archiving, and a
//! member that leads next still holds what the archive lacks. The
//! backup reads held entries with [`LogStore::read_retained`]. A hold
//! may cap the entries it keeps and their bytes
//! ([`LogStore::hold_purges_capped`]): past either cap the oldest held
//! entries go unarchived, and the leader takes a base to resume.
//!
//! Purges free the log's pages, but redb gives space back only from the
//! end of the file; [`LogStore::compactor`] moves live pages down online
//! (see [`flower_engine::storage::compact`]), so the file tracks the
//! retained window instead of its high-water mark.

use std::collections::VecDeque;
use std::fmt::Debug;
use std::ops::{Bound, RangeBounds};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant};

use flower_engine::storage::compact::Compactor;
use openraft::storage::{LogFlushed, RaftLogReader, RaftLogStorage};
use openraft::{
    BasicNode, Entry, EntryPayload, LogId, LogState, Membership, OptionalSend, StorageError,
    StorageIOError, Vote,
};
use redb::{Durability, ReadableDatabase, ReadableTable, TableDefinition};
use serde::de::DeserializeOwned;

use crate::flush::{Commits, LeaderFlush, LeaderFlushStats, LeaderFlusher};
use crate::snapshot_file::Images;
use crate::telemetry::{
    CommitMarks, Observer, SnapshotOutcome, SnapshotPhase, SnapshotWork, StorageBatch,
    StorageOperation, StorageStage,
};
use crate::types::{ClusterWrite, NodeId, TypeConfig};

/// Most entries one purge transaction deletes, in [`RaftLogStorage::purge`]
/// itself and in each background transaction after it.
pub const PURGE_CHUNK: u64 = 1024;
/// The pause between two background purge transactions: lower priority
/// than the Raft core's own writes.
const PURGE_PAUSE: Duration = Duration::from_millis(2);
/// How often an idle purge task checks that its store is still open.
const PURGE_IDLE: Duration = Duration::from_secs(1);

const VOTE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("raft_vote");
const LOG: TableDefinition<&[u8], &[u8]> = TableDefinition::new("raft_log");
const META: TableDefinition<&[u8], &[u8]> = TableDefinition::new("raft_meta");
const SNAPSHOT: TableDefinition<&[u8], &[u8]> = TableDefinition::new("raft_snapshot");

const VOTE_KEY: &[u8] = b"vote";
const PURGED_KEY: &[u8] = b"purged";
/// The committed log ID as of the last log write.
const COMMITTED_KEY: &[u8] = b"committed";
/// The current snapshot's [`ImageRecord`].
const IMAGE_KEY: &[u8] = b"image";
/// Where files written before images moved out held the whole snapshot
/// (a [`StoredSnapshot`] as JSON); opening one moves it to an image.
const LEGACY_SNAPSHOT_KEY: &[u8] = b"current";
const INSTALLING_KEY: &[u8] = b"installing";
/// Present while purges hold entries for the backup: restarts hold from
/// open, before Raft can purge.
const HOLD_KEY: &[u8] = b"backup_hold";
/// Reads of the current image that may race its replacement.
const IMAGE_READ_ATTEMPTS: usize = 8;
/// Present while this member is rejoining (see [`crate::rejoin`]).
const REJOINING_KEY: &[u8] = b"rejoining";
/// Written once a shutdown made every leader append durable; taken at
/// the next open (see [`LogStore::take_clean_shutdown`]).
const CLEAN_KEY: &[u8] = b"clean_shutdown";
/// The storage contract the file was written under (see
/// [`LogStore::claim_contract`]).
const CONTRACT_KEY: &[u8] = b"contract";

/// OpenRaft's snapshot metadata for this cluster.
pub type RaftSnapshotMeta = openraft::SnapshotMeta<NodeId, openraft::BasicNode>;

/// A snapshot held in memory: its metadata plus the image bytes, a
/// [`SyncSnapshotFile`](crate::sync::SyncSnapshotFile)'s JSON.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct StoredSnapshot {
    /// Snapshot metadata JSON bytes.
    pub meta: Vec<u8>,
    /// Snapshot file bytes.
    pub bytes: Vec<u8>,
}

/// The current snapshot as recorded in the Raft file: its metadata and
/// the name of its image in the image directory.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct ImageRecord {
    meta: RaftSnapshotMeta,
    file: String,
}

/// A recorded snapshot: its metadata and where its image is.
#[derive(Clone, Debug)]
pub struct SnapshotImage {
    /// OpenRaft's metadata for it.
    pub meta: RaftSnapshotMeta,
    /// The sealed image file.
    pub path: PathBuf,
}

/// One consistent sync-down view of the log.
#[derive(Debug)]
pub struct SyncView {
    /// The last purged log ID, if any.
    pub purged: Option<LogId<NodeId>>,
    /// The stored snapshot, loaded only when the requested position
    /// fell below the purge point.
    pub snapshot: Option<StoredSnapshot>,
    /// Retained entries in the requested range, otherwise.
    pub entries: Vec<Entry<TypeConfig>>,
}

/// Shared log storage, cloneable for the Raft core and sync serving.
#[derive(Clone)]
pub struct LogStore {
    inner: Arc<LogStoreInner>,
}

/// A [`LogStore`] handle that doesn't keep the file open.
#[derive(Clone)]
pub struct WeakLogStore(Weak<LogStoreInner>);

impl WeakLogStore {
    /// The store, while some [`LogStore`] keeps it open.
    pub fn upgrade(&self) -> Option<LogStore> {
        self.0.upgrade().map(|inner| LogStore { inner })
    }
}

struct LogStoreInner {
    db: Arc<redb::Database>,
    /// Compacts the file online; absent without a file path to measure.
    compactor: Option<Compactor>,
    /// Deletes purged entries in the background.
    sweep: Arc<Sweep>,
    /// Snapshot image files.
    images: Images,
    /// The committed index OpenRaft reported, and the last one recorded.
    committed: Mutex<Committed>,
    /// Purged entries kept for the backup.
    hold: Mutex<PurgeHold>,
    /// The bytes of held entries, for a byte cap on the hold.
    held: Mutex<HeldBytes>,
    /// When entries became durable here, and were applied: commits
    /// split by them (see [`CommitMarks`]).
    marks: Arc<CommitMarks>,
    /// The group's instruments, once its member names it.
    observer: OnceLock<Arc<Observer>>,
    /// The order of the file's commits and how much of it is durable.
    commits: Commits,
    /// The leader's appends committed without an fsync (see
    /// [`crate::flush`]).
    flusher: Arc<LeaderFlusher>,
    /// The task flushing them, while it runs.
    flush_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

/// Which purged entries stay on disk for the backup.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PurgeHold {
    /// Whether purges keep entries the backup has not released.
    pub enabled: bool,
    /// Entries at or below this index may go: the backup archived
    /// them. `None` keeps every purged entry.
    pub released: Option<u64>,
    /// Most purged entries kept below the purge point; older ones go
    /// unarchived. `None` keeps every one.
    pub max_held: Option<u64>,
    /// Most bytes of purged entries kept below the purge point; the
    /// oldest go unarchived past it. `None` keeps every one.
    pub max_held_bytes: Option<u64>,
}

/// The stored bytes of held entries below the purge point, measured as
/// purges pass them, in spans of at most [`PURGE_CHUNK`] entries.
#[derive(Debug, Default)]
struct HeldBytes {
    /// The highest index measured.
    through: Option<u64>,
    /// `(last index, bytes)` per span, oldest first.
    spans: VecDeque<(u64, u64)>,
    /// Bytes across the spans.
    total: u64,
}

impl HeldBytes {
    /// Forget the spans at or below `deleted`: their entries go.
    fn forget_through(&mut self, deleted: u64) {
        while let Some(&(last, bytes)) = self.spans.front() {
            if last > deleted {
                break;
            }
            self.total -= bytes;
            self.spans.pop_front();
        }
    }

    /// The highest index whose deletion brings the held bytes within
    /// `cap`, if they exceed it: whole spans, oldest first.
    fn overflow(&self, cap: u64) -> Option<u64> {
        let mut remaining = self.total;
        let mut bound = None;
        for &(last, bytes) in &self.spans {
            if remaining <= cap {
                break;
            }
            remaining -= bytes;
            bound = Some(last);
        }
        bound
    }
}

impl PurgeHold {
    /// The highest index a purge through `purged` may delete, if any.
    pub fn deletable(&self, purged: u64) -> Option<u64> {
        if !self.enabled {
            return Some(purged);
        }
        let overflow = self.max_held.and_then(|max| purged.checked_sub(max));
        self.released.max(overflow).map(|index| index.min(purged))
    }
}

/// The committed index as remembered and as recorded.
#[derive(Default)]
struct Committed {
    known: Option<LogId<NodeId>>,
    recorded: Option<LogId<NodeId>>,
}

/// Background purge progress, shared by the store and its task. The
/// task holds the store only weakly, and only for one transaction at a
/// time, so a store whose last user is gone closes its file.
#[derive(Default)]
struct Sweep {
    state: Mutex<SweepState>,
    wake: tokio::sync::Notify,
    /// Holds the task before its next transaction, for tests.
    #[cfg(test)]
    held: std::sync::atomic::AtomicBool,
}

#[derive(Default)]
struct SweepState {
    /// The highest purge point Raft asked for in this process.
    target: Option<u64>,
    /// Whether entries the sweep may delete are still on disk.
    pending: bool,
    /// Whether the task runs.
    running: bool,
    /// Set by [`LogStore::stop_purging`]: no task starts again.
    stopped: bool,
    /// The task, while it runs.
    task: Option<tokio::task::JoinHandle<()>>,
    stats: PurgeStats,
    /// The purge the task finishes, as snapshot work.
    work: Option<SnapshotWork>,
}

/// Purge counters, for metrics and tests.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PurgeStats {
    /// The highest purge point Raft asked for in this process.
    pub target: Option<u64>,
    /// Whether purged entries are still being deleted.
    pub behind: bool,
    /// Purge transactions committed, `purge`'s own included.
    pub transactions: u64,
    /// Entries those transactions deleted.
    pub deleted: u64,
    /// The most entries one transaction deleted: at most [`PURGE_CHUNK`].
    pub most: u64,
}

impl Sweep {
    fn lock(&self) -> std::sync::MutexGuard<'_, SweepState> {
        self.state.lock().expect("purge state poisoned")
    }

    /// Record one committed purge transaction.
    fn committed(&self, target: u64, deleted: u64, pending: bool) {
        let mut state = self.lock();
        state.target = state.target.max(Some(target));
        state.pending = pending;
        state.stats.transactions += 1;
        state.stats.deleted += deleted;
        state.stats.most = state.stats.most.max(deleted);
    }

    fn behind(&self) -> bool {
        self.lock().pending
    }
}

/// The client writes an entry carries: a batch's items, one for any
/// other write, none for blanks and membership.
pub(crate) fn commands_in(entry: &Entry<TypeConfig>) -> u64 {
    match &entry.payload {
        EntryPayload::Normal(ClusterWrite::Batch { items, .. }) => items.len() as u64,
        EntryPayload::Normal(_) => 1,
        EntryPayload::Blank | EntryPayload::Membership(_) => 0,
    }
}

/// Wrap a log-read failure. Storage errors take references, so each
/// fallible site maps through these one-line helpers.
fn read_logs<E: std::error::Error + 'static>(error: E) -> StorageIOError<NodeId> {
    StorageIOError::read_logs(&error)
}

/// Wrap a log-write failure.
fn write_logs<E: std::error::Error + 'static>(error: E) -> StorageIOError<NodeId> {
    StorageIOError::write_logs(&error)
}

/// Wrap a vote-read failure.
fn read_vote<E: std::error::Error + 'static>(error: E) -> StorageIOError<NodeId> {
    StorageIOError::read_vote(&error)
}

/// Wrap a vote-write failure.
fn write_vote<E: std::error::Error + 'static>(error: E) -> StorageIOError<NodeId> {
    StorageIOError::write_vote(&error)
}

/// Decode an optional JSON row.
fn decode_row<T: DeserializeOwned>(
    row: Option<redb::AccessGuard<'_, &'static [u8]>>,
) -> Result<Option<T>, StorageIOError<NodeId>> {
    match row {
        Some(row) => Ok(Some(
            serde_json::from_slice(row.value()).map_err(read_logs)?,
        )),
        None => Ok(None),
    }
}

/// Entries in a `u64` range of an open log table, decoded.
fn entries_in<T, R>(table: &T, range: R) -> Result<Vec<Entry<TypeConfig>>, StorageIOError<NodeId>>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
    R: RangeBounds<u64>,
{
    let bound = |bound: Bound<&u64>| match bound {
        Bound::Included(index) => Bound::Included(index.to_be_bytes()),
        Bound::Excluded(index) => Bound::Excluded(index.to_be_bytes()),
        Bound::Unbounded => Bound::Unbounded,
    };
    let (lower, upper) = (bound(range.start_bound()), bound(range.end_bound()));
    let rows = table
        .range::<&[u8]>((
            lower.as_ref().map(|bytes| bytes.as_slice()),
            upper.as_ref().map(|bytes| bytes.as_slice()),
        ))
        .map_err(read_logs)?;
    let mut entries = Vec::new();
    for row in rows {
        let (_, value) = row.map_err(read_logs)?;
        entries.push(serde_json::from_slice(value.value()).map_err(read_logs)?);
    }
    Ok(entries)
}

/// The current snapshot's record in an open `SNAPSHOT` table.
fn image_in<T>(table: &T) -> Result<Option<ImageRecord>, StorageIOError<NodeId>>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
{
    decode_row(table.get(IMAGE_KEY).map_err(read_logs)?)
}

/// Record an image as the current snapshot in an open `SNAPSHOT` table.
fn insert_image(
    table: &mut redb::Table<&'static [u8], &'static [u8]>,
    meta: &RaftSnapshotMeta,
    file: &str,
) -> Result<(), StorageIOError<NodeId>> {
    let record = ImageRecord {
        meta: meta.clone(),
        file: file.to_string(),
    };
    let bytes = serde_json::to_vec(&record).map_err(write_logs)?;
    table
        .insert(IMAGE_KEY, bytes.as_slice())
        .map_err(write_logs)?;
    Ok(())
}

/// Where a store keeps its images: `snapshots/` beside its file, or for
/// a store without one, a directory of its own under the system's
/// temporary directory.
fn image_dir(path: Option<&Path>) -> PathBuf {
    static UNNAMED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    match path.and_then(Path::parent) {
        Some(parent) => parent.join("snapshots"),
        None => std::env::temp_dir().join(format!(
            "flower-snapshots-{}-{}",
            std::process::id(),
            UNNAMED.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        )),
    }
}

/// The purge point recorded in an open `META` table.
fn purged_in<T>(meta: &T) -> Result<Option<LogId<NodeId>>, StorageIOError<NodeId>>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
{
    decode_row(meta.get(PURGED_KEY).map_err(read_logs)?)
}

/// A range's start raised above the purge point: entries a background
/// purge hasn't deleted yet are never read.
fn above_purged(start: Bound<&u64>, purged: Option<LogId<NodeId>>) -> Bound<u64> {
    let start = start.cloned();
    let Some(purged) = purged else {
        return start;
    };
    let floor = Bound::Excluded(purged.index);
    match start {
        Bound::Unbounded => floor,
        Bound::Included(index) if index > purged.index => start,
        Bound::Excluded(index) if index >= purged.index => start,
        _ => floor,
    }
}

/// The first index in an open log table, if any.
fn first_index<T>(table: &T) -> Result<Option<u64>, StorageIOError<NodeId>>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
{
    let Some((key, _)) = table.first().map_err(write_logs)? else {
        return Ok(None);
    };
    let bytes: [u8; 8] = key
        .value()
        .try_into()
        .map_err(|_| write_logs(std::io::Error::other("log key is not a u64 index")))?;
    Ok(Some(u64::from_be_bytes(bytes)))
}

/// Delete at most [`PURGE_CHUNK`] entries from the head of the log, none
/// past `through`. Returns how many went and whether entries at or below
/// `through` remain.
fn delete_head(
    table: &mut redb::Table<&'static [u8], &'static [u8]>,
    through: u64,
) -> Result<(u64, bool), StorageIOError<NodeId>> {
    let Some(first) = first_index(table)? else {
        return Ok((0, false));
    };
    if first > through {
        return Ok((0, false));
    }
    let upto = through.min(first.saturating_add(PURGE_CHUNK - 1));
    let (lower, upper) = (first.to_be_bytes(), upto.to_be_bytes());
    let mut deleted = 0;
    table
        .retain_in::<&[u8], _>(lower.as_slice()..=upper.as_slice(), |_, _| {
            deleted += 1;
            false
        })
        .map_err(write_logs)?;
    let pending = first_index(table)?.is_some_and(|next| next <= through);
    Ok((deleted, pending))
}

impl LogStore {
    /// Open or create the Raft file.
    pub fn open(path: &Path) -> Result<Self, StorageIOError<NodeId>> {
        let db = redb::Database::create(path).map_err(read_logs)?;
        Self::from_database(db, Some(path))
    }

    /// Wrap an open redb database, such as one over a custom storage
    /// backend, creating any missing tables. `path` names its file, for
    /// online compaction and for its image directory beside it; without
    /// one the store never compacts and keeps images in a temporary
    /// directory. Opening deletes images a crash left half written or
    /// unrecorded.
    pub fn from_database(
        db: redb::Database,
        path: Option<&Path>,
    ) -> Result<Self, StorageIOError<NodeId>> {
        let write = db.begin_write().map_err(write_logs)?;
        write.open_table(VOTE).map_err(write_logs)?;
        write.open_table(LOG).map_err(write_logs)?;
        write.open_table(META).map_err(write_logs)?;
        write.open_table(SNAPSHOT).map_err(write_logs)?;
        let held = write
            .open_table(META)
            .map_err(write_logs)?
            .get(HOLD_KEY)
            .map_err(write_logs)?
            .map(|row| decode_max_held(row.value()));
        write.commit().map_err(write_logs)?;
        let images = Images::open(image_dir(path)).map_err(write_logs)?;
        let db = Arc::new(db);
        // A reopened hold releases nothing until the backup reports
        // what it archived; its byte cap counts from the next measure.
        let hold = held.map_or_else(PurgeHold::default, |(max_held, max_held_bytes)| PurgeHold {
            enabled: true,
            released: None,
            max_held,
            max_held_bytes,
        });
        let store = LogStore {
            inner: Arc::new(LogStoreInner {
                compactor: path.map(|path| Compactor::new(db.clone(), path)),
                db,
                sweep: Arc::default(),
                images,
                committed: Mutex::default(),
                hold: Mutex::new(hold),
                held: Mutex::default(),
                marks: Arc::default(),
                observer: OnceLock::new(),
                commits: Commits::default(),
                flusher: Arc::default(),
                flush_task: Mutex::default(),
            }),
        };
        store.migrate_legacy_snapshot()?;
        store.inner.images.clear_parts();
        let current = store.current_image()?;
        let keep = current
            .as_ref()
            .and_then(|image| image.path.file_name())
            .and_then(|name| name.to_str());
        store.inner.images.retain(keep);
        Ok(store)
    }

    /// Report this store's work to `observer`, its group's instruments.
    /// A store never told reports as the root group's.
    pub fn set_observer(&self, observer: Arc<Observer>) {
        let _ = self.inner.observer.set(observer);
    }

    /// The group's instruments this store reports to.
    pub fn observer(&self) -> Arc<Observer> {
        self.inner
            .observer
            .get_or_init(|| crate::telemetry::observer(&crate::group::GroupId::root().name))
            .clone()
    }

    /// When this member's entries became durable in its log, and were
    /// applied: the state machine and the writer share them.
    pub fn commit_marks(&self) -> Arc<CommitMarks> {
        self.inner.marks.clone()
    }

    /// Move a snapshot an older build kept whole in the Raft file out to
    /// an image, keeping its install marker.
    fn migrate_legacy_snapshot(&self) -> Result<(), StorageIOError<NodeId>> {
        let read = self.inner.db.begin_read().map_err(read_logs)?;
        let table = read.open_table(SNAPSHOT).map_err(read_logs)?;
        let legacy: Option<StoredSnapshot> =
            decode_row(table.get(LEGACY_SNAPSHOT_KEY).map_err(read_logs)?)?;
        let Some(legacy) = legacy else {
            return Ok(());
        };
        let recorded = image_in(&table)?.is_some();
        drop(table);
        drop(read);
        if !recorded {
            let (meta, part) = self.spool(&legacy)?;
            let file = self.inner.images.seal(&part).map_err(write_logs)?;
            let write = self.inner.db.begin_write().map_err(write_logs)?;
            let mut table = write.open_table(SNAPSHOT).map_err(write_logs)?;
            insert_image(&mut table, &meta, &file)?;
            table.remove(LEGACY_SNAPSHOT_KEY).map_err(write_logs)?;
            drop(table);
            self.commit_durable(write).map_err(write_logs)?;
        } else {
            let write = self.inner.db.begin_write().map_err(write_logs)?;
            write
                .open_table(SNAPSHOT)
                .map_err(write_logs)?
                .remove(LEGACY_SNAPSHOT_KEY)
                .map_err(write_logs)?;
            self.commit_durable(write).map_err(write_logs)?;
        }
        Ok(())
    }

    /// Commit durably: this commit and every one before it reach the disk.
    fn commit_durable(&self, write: redb::WriteTransaction) -> Result<(), redb::CommitError> {
        let seq = self.inner.commits.next();
        write.commit()?;
        self.inner.commits.made_durable(seq);
        if self.inner.flusher.holding() {
            // Held leader appends it covers complete now.
            self.inner.flusher.wake.notify_one();
        }
        Ok(())
    }

    /// Commit without an fsync: readable at once, durable with the next
    /// durable commit. Returns the commit's number (see [`Commits`]).
    fn commit_deferred(
        &self,
        mut write: redb::WriteTransaction,
    ) -> Result<u64, StorageIOError<NodeId>> {
        write.set_durability(Durability::None).map_err(write_logs)?;
        let seq = self.inner.commits.next();
        write.commit().map_err(write_logs)?;
        Ok(seq)
    }

    /// Make every commit of the file durable, if one is not: one durable
    /// commit, which also records the committed index. Blocking. Held
    /// leader appends complete. Returns the last durable commit's number.
    fn flush_commits(&self) -> Result<u64, StorageIOError<NodeId>> {
        let target = self.inner.commits.last();
        if self.inner.commits.durable() >= target {
            return Ok(self.inner.commits.durable());
        }
        let write = self.inner.db.begin_write().map_err(write_logs)?;
        // A durable commit that held the lock meanwhile may have covered it.
        if self.inner.commits.durable() >= target {
            write.abort().map_err(write_logs)?;
            return Ok(self.inner.commits.durable());
        }
        let committed = self.record_committed(&write)?;
        self.commit_durable(write).map_err(write_logs)?;
        self.committed_recorded(committed);
        Ok(self.inner.commits.durable())
    }

    /// Make every write of the file durable now, held leader appends
    /// included, which complete. Blocking: it fsyncs when anything is
    /// unsynced.
    pub fn flush(&self) -> Result<(), StorageIOError<NodeId>> {
        let durable = self.flush_commits()?;
        self.inner.flusher.release(durable, false);
        Ok(())
    }

    /// Make the log durable through `index`: flush when a held leader
    /// append at or below it is not yet. Blocking. A group file runs
    /// this before each durable commit with the highest index it may hold
    /// applied (see [`crate::flush`]).
    pub fn flush_through(&self, index: u64) -> Result<(), StorageIOError<NodeId>> {
        if self
            .inner
            .flusher
            .unflushed_through(index, self.inner.commits.durable())
        {
            self.flush()?;
        }
        Ok(())
    }

    /// A handle that doesn't keep the file open.
    pub fn downgrade(&self) -> WeakLogStore {
        WeakLogStore(Arc::downgrade(&self.inner))
    }

    /// How the leader flushes its own appends from now on.
    pub fn set_leader_flush(&self, settings: LeaderFlush) {
        self.inner.flusher.set_settings(settings);
    }

    /// How the leader flushes its own appends.
    pub fn leader_flush(&self) -> LeaderFlush {
        self.inner.flusher.settings()
    }

    /// The leader flush's counters for this process.
    pub fn leader_flush_stats(&self) -> LeaderFlushStats {
        self.inner.flusher.stats()
    }

    /// The group applied `membership`: whether the followers alone form
    /// a majority of each of its voter configurations decides whether
    /// the leader defers its flush.
    pub fn applied_membership(&self, membership: &Membership<NodeId, BasicNode>) {
        self.inner.flusher.applied_membership(membership);
    }

    /// Start the leader flusher if it isn't running.
    fn start_flusher(&self) {
        let mut task = self.inner.flush_task.lock().expect("flush task poisoned");
        if task.as_ref().is_some_and(|task| !task.is_finished()) {
            return;
        }
        *task = Some(tokio::spawn(run_flusher(
            self.inner.flusher.clone(),
            Arc::downgrade(&self.inner),
        )));
    }

    /// Make every held leader append durable and complete it, and commit
    /// later ones durably: call once Raft stopped, before closing the
    /// file. A failed flush fails the held appends.
    pub async fn stop_flushing(&self) {
        self.inner.flusher.stop();
        let store = self.clone();
        let flushed = tokio::task::spawn_blocking(move || {
            store.flush()?;
            store.set_mark(CLEAN_KEY, true)
        })
        .await;
        match flushed {
            Ok(Ok(())) => {}
            Ok(Err(error)) => self.inner.flusher.fail(error.to_string()),
            Err(error) => self.inner.flusher.fail(error.to_string()),
        }
        let task = self
            .inner
            .flush_task
            .lock()
            .expect("flush task poisoned")
            .take();
        if let Some(task) = task {
            let _ = task.await;
        }
    }

    /// Purge counters for this process.
    pub fn purge_stats(&self) -> PurgeStats {
        let state = self.inner.sweep.lock();
        PurgeStats {
            target: state.target,
            behind: state.pending,
            ..state.stats
        }
    }

    /// Stop the background purge and wait until no purge transaction is
    /// in flight: call before closing the file. Purged entries left on
    /// disk go at the next process's first purge.
    pub async fn stop_purging(&self) {
        let task = {
            let mut state = self.inner.sweep.lock();
            state.stopped = true;
            state.task.take()
        };
        self.inner.sweep.wake.notify_one();
        if let Some(task) = task {
            let _ = task.await;
        }
    }

    /// The highest index a purge may delete for a purge point, if any:
    /// the point itself, lowered by the backup hold to what the backup
    /// released. Held entries stay on disk below the purge point, where
    /// no Raft read sees them, until a later bound lets them go.
    fn sweep_bound(&self, target: u64) -> Option<u64> {
        let hold = *self.hold();
        let bound = hold.deletable(target);
        let Some(cap) = hold.max_held_bytes.filter(|_| hold.enabled) else {
            return bound;
        };
        let mut held = self.held();
        if let Some(bound) = bound {
            held.forget_through(bound);
        }
        let bound = bound.max(held.overflow(cap).map(|overflow| overflow.min(target)));
        if let Some(bound) = bound {
            // Deleted by the purge this bound is for.
            held.forget_through(bound);
        }
        bound
    }

    /// Measure the stored bytes of held entries through `through`, the
    /// purge point, once each, when the hold caps its bytes: blocking.
    fn measure_held(&self, through: u64) -> Result<(), StorageIOError<NodeId>> {
        let hold = *self.hold();
        if !hold.enabled || hold.max_held_bytes.is_none() {
            return Ok(());
        }
        let from = match self.held().through {
            Some(measured) if measured >= through => return Ok(()),
            Some(measured) => measured + 1,
            None => 0,
        };
        let read = self.inner.db.begin_read().map_err(read_logs)?;
        let table = read.open_table(LOG).map_err(read_logs)?;
        let (lower, upper) = (from.to_be_bytes(), through.to_be_bytes());
        let mut spans: Vec<(u64, u64)> = Vec::new();
        let mut entries = 0;
        for row in table
            .range::<&[u8]>(lower.as_slice()..=upper.as_slice())
            .map_err(read_logs)?
        {
            let (key, value) = row.map_err(read_logs)?;
            let index: [u8; 8] = key
                .value()
                .try_into()
                .map_err(|_| read_logs(std::io::Error::other("log key is not a u64 index")))?;
            let (index, bytes) = (u64::from_be_bytes(index), value.value().len() as u64);
            match spans.last_mut() {
                Some(span) if entries < PURGE_CHUNK => {
                    *span = (index, span.1 + bytes);
                    entries += 1;
                }
                _ => {
                    spans.push((index, bytes));
                    entries = 1;
                }
            }
        }
        let mut held = self.held();
        held.through = Some(through);
        for span in spans {
            held.total += span.1;
            held.spans.push_back(span);
        }
        Ok(())
    }

    /// Bytes of held entries the hold measured below the purge point:
    /// zero unless it caps their bytes.
    pub fn held_bytes(&self) -> u64 {
        self.held().total
    }

    /// Lock the held-bytes measure.
    fn held(&self) -> std::sync::MutexGuard<'_, HeldBytes> {
        self.inner.held.lock().expect("held bytes poisoned")
    }

    /// Record the purge point and delete the first chunk of entries up
    /// to it, in one durable transaction. The point never moves back.
    /// Returns whether purged entries remain on disk.
    fn purge_head(&self, log_id: LogId<NodeId>) -> Result<bool, StorageIOError<NodeId>> {
        let write = self.inner.db.begin_write().map_err(write_logs)?;
        {
            let mut meta = write.open_table(META).map_err(write_logs)?;
            if purged_in(&meta)? < Some(log_id) {
                let bytes = serde_json::to_vec(&log_id).map_err(write_logs)?;
                meta.insert(PURGED_KEY, bytes.as_slice())
                    .map_err(write_logs)?;
            }
        }
        self.measure_held(log_id.index)?;
        let (deleted, pending) = match self.sweep_bound(log_id.index) {
            Some(bound) => delete_head(&mut write.open_table(LOG).map_err(write_logs)?, bound)?,
            None => (0, false),
        };
        let committed = self.record_committed(&write)?;
        self.commit_durable(write).map_err(write_logs)?;
        self.committed_recorded(committed);
        self.inner.sweep.committed(log_id.index, deleted, pending);
        Ok(pending)
    }

    /// Record the remembered committed index in a log write that is about
    /// to commit durably, if it moved since the last one recorded.
    /// Returns what was recorded, for [`LogStore::committed_recorded`].
    fn record_committed(
        &self,
        write: &redb::WriteTransaction,
    ) -> Result<Option<LogId<NodeId>>, StorageIOError<NodeId>> {
        let known = {
            let committed = self.inner.committed.lock().expect("committed poisoned");
            if committed.known <= committed.recorded {
                return Ok(None);
            }
            committed.known
        };
        let bytes = serde_json::to_vec(&known).map_err(write_logs)?;
        write
            .open_table(META)
            .map_err(write_logs)?
            .insert(COMMITTED_KEY, bytes.as_slice())
            .map_err(write_logs)?;
        Ok(known)
    }

    /// The write carrying `recorded` committed.
    fn committed_recorded(&self, recorded: Option<LogId<NodeId>>) {
        let mut committed = self.inner.committed.lock().expect("committed poisoned");
        committed.recorded = committed.recorded.max(recorded);
    }

    /// The highest committed log ID this process remembers or the file
    /// records. The record lags by at most the commits since the last log
    /// write: a lower bound OpenRaft re-applies to on open, never more.
    pub fn committed(&self) -> Result<Option<LogId<NodeId>>, StorageIOError<NodeId>> {
        let read = self.inner.db.begin_read().map_err(read_logs)?;
        let meta = read.open_table(META).map_err(read_logs)?;
        let recorded: Option<LogId<NodeId>> =
            decode_row(meta.get(COMMITTED_KEY).map_err(read_logs)?)?;
        let known = self
            .inner
            .committed
            .lock()
            .expect("committed poisoned")
            .known;
        Ok(recorded.max(known))
    }

    /// The last log index, the purge point when every entry is purged.
    pub fn last_log_index(&self) -> Result<Option<u64>, StorageIOError<NodeId>> {
        let last = self.last_log_id()?.max(self.last_purged()?);
        Ok(last.map(|id| id.index))
    }

    /// One background purge transaction: the next chunk towards the
    /// purge point, committed without an fsync unless it is the last.
    /// redb reuses the pages a commit frees only after a durable commit,
    /// so the last one makes them free for compaction even on a member
    /// that writes nothing else. Returns whether more remain.
    fn sweep_chunk(&self) -> Result<bool, StorageIOError<NodeId>> {
        let Some(target) = self.inner.sweep.lock().target else {
            return Ok(false);
        };
        let Some(bound) = self.sweep_bound(target) else {
            self.inner.sweep.lock().pending = false;
            return Ok(false);
        };
        let write = self.inner.db.begin_write().map_err(write_logs)?;
        let (deleted, pending) = {
            let mut table = write.open_table(LOG).map_err(write_logs)?;
            delete_head(&mut table, bound)?
        };
        if pending {
            self.commit_deferred(write)?;
        } else {
            self.commit_durable(write).map_err(write_logs)?;
        }
        self.inner.sweep.committed(target, deleted, pending);
        Ok(pending)
    }

    /// Start the background purge if it isn't running, and wake it.
    fn sweep_in_background(&self) {
        let sweep = self.inner.sweep.clone();
        {
            let mut state = sweep.lock();
            if state.stopped {
                return;
            }
            if !state.running {
                state.running = true;
                let task = tokio::spawn(run_sweep(sweep.clone(), Arc::downgrade(&self.inner)));
                state.task = Some(task);
            }
        }
        sweep.wake.notify_one();
    }

    /// The file's online compactor, sharing its handle so steps run
    /// beside the Raft core's own writes. `None` without a file path.
    pub fn compactor(&self) -> Option<Compactor> {
        self.inner.compactor.clone()
    }

    /// Read retained entries in `[start, end)`.
    pub fn read_entries(
        &self,
        start: u64,
        end: u64,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageIOError<NodeId>> {
        self.entries(start..end)
    }

    /// One consistent view for sync-down serving after `since`: the
    /// purge point, then either the recorded snapshot (when `since` fell
    /// below it) or the retained entries in `(since, last]`, all from
    /// one read transaction so a concurrent purge cannot open a silent
    /// gap between them. A snapshot replaced between reading its record
    /// and opening its file is read again from the new record.
    pub fn sync_view(&self, since: u64, last: u64) -> Result<SyncView, StorageIOError<NodeId>> {
        for _ in 0..IMAGE_READ_ATTEMPTS {
            let read = self.inner.db.begin_read().map_err(read_logs)?;
            let meta = read.open_table(META).map_err(read_logs)?;
            let purged = purged_in(&meta)?;
            if purged.is_some_and(|purged| since < purged.index) {
                let record = image_in(&read.open_table(SNAPSHOT).map_err(read_logs)?)?;
                drop(read);
                let snapshot = match record {
                    None => None,
                    Some(record) => match self.read_image(&record)? {
                        Some(snapshot) => Some(snapshot),
                        None => continue,
                    },
                };
                return Ok(SyncView {
                    purged,
                    snapshot,
                    entries: Vec::new(),
                });
            }
            let entries = if last > since {
                let table = read.open_table(LOG).map_err(read_logs)?;
                entries_in(&table, since + 1..=last)?
            } else {
                Vec::new()
            };
            return Ok(SyncView {
                purged,
                snapshot: None,
                entries,
            });
        }
        Err(read_logs(std::io::Error::other(
            "the snapshot kept changing under sync serving",
        )))
    }

    /// The last purged log ID, if any.
    pub fn last_purged(&self) -> Result<Option<LogId<NodeId>>, StorageIOError<NodeId>> {
        let read = self.inner.db.begin_read().map_err(read_logs)?;
        purged_in(&read.open_table(META).map_err(read_logs)?)
    }

    /// A new image file for a build or a transfer to write, in the
    /// store's image directory.
    pub(crate) fn image_part(&self) -> std::io::Result<(PathBuf, std::fs::File)> {
        self.inner.images.part()
    }

    /// Sync image files and their directory (the default) or leave
    /// their flushing to the kernel, as `FLOWER_FSYNC=off` does.
    pub fn set_sync_images(&self, sync: bool) {
        self.inner.images.set_sync(sync);
    }

    /// Sync every sealed image and the image directory now, whatever
    /// [`LogStore::set_sync_images`] said: a member with fsync off does
    /// at shutdown.
    pub fn sync_images(&self) -> std::io::Result<()> {
        self.inner.images.sync_all()
    }

    /// A part file of the image directory holding `received`: itself
    /// when it is one, else a copy.
    pub(crate) fn adopt_image(&self, received: &Path) -> std::io::Result<PathBuf> {
        self.inner.images.adopt(received)
    }

    /// Record a built image as the current snapshot unless a newer one
    /// already is (an install that finished first): seal the part, then
    /// commit its record durably, then delete older images. Returns the
    /// current snapshot afterwards, this build's or the newer one.
    pub fn publish_image(
        &self,
        meta: &RaftSnapshotMeta,
        part: &Path,
    ) -> Result<SnapshotImage, StorageIOError<NodeId>> {
        let file = self.inner.images.seal(part).map_err(write_logs)?;
        let write = self.inner.db.begin_write().map_err(write_logs)?;
        let current = {
            let mut table = write.open_table(SNAPSHOT).map_err(write_logs)?;
            let current = image_in(&table)?;
            match current {
                Some(current) if current.meta.last_log_id > meta.last_log_id => Some(current),
                _ => {
                    insert_image(&mut table, meta, &file)?;
                    None
                }
            }
        };
        self.commit_durable(write).map_err(write_logs)?;
        let record = match current {
            Some(newer) => {
                self.inner.images.retain(Some(&newer.file));
                newer
            }
            None => {
                self.inner.images.retain(Some(&file));
                ImageRecord {
                    meta: meta.clone(),
                    file,
                }
            }
        };
        self.image(record)
    }

    /// Record a received image as the current snapshot plus the install
    /// marker, in one durable transaction, before the engine changes.
    pub fn begin_install_image(
        &self,
        meta: &RaftSnapshotMeta,
        part: &Path,
    ) -> Result<SnapshotImage, StorageIOError<NodeId>> {
        let file = self.inner.images.seal(part).map_err(write_logs)?;
        let write = self.inner.db.begin_write().map_err(write_logs)?;
        insert_image(
            &mut write.open_table(SNAPSHOT).map_err(write_logs)?,
            meta,
            &file,
        )?;
        write
            .open_table(META)
            .map_err(write_logs)?
            .insert(INSTALLING_KEY, b"1".as_slice())
            .map_err(write_logs)?;
        self.commit_durable(write).map_err(write_logs)?;
        self.inner.images.retain(Some(&file));
        self.image(ImageRecord {
            meta: meta.clone(),
            file,
        })
    }

    /// Persist a snapshot held in memory as the current one.
    pub fn save_snapshot(&self, snapshot: &StoredSnapshot) -> Result<(), StorageIOError<NodeId>> {
        let (meta, part) = self.spool(snapshot)?;
        self.publish_image(&meta, &part)?;
        Ok(())
    }

    /// Record a snapshot held in memory about to install plus the
    /// install marker (see [`LogStore::begin_install_image`]).
    pub fn begin_install(&self, snapshot: &StoredSnapshot) -> Result<(), StorageIOError<NodeId>> {
        let (meta, part) = self.spool(snapshot)?;
        self.begin_install_image(&meta, &part)?;
        Ok(())
    }

    /// Write an in-memory snapshot to a part file.
    fn spool(
        &self,
        snapshot: &StoredSnapshot,
    ) -> Result<(RaftSnapshotMeta, PathBuf), StorageIOError<NodeId>> {
        use std::io::Write;
        let meta = serde_json::from_slice(&snapshot.meta).map_err(write_logs)?;
        let (path, mut file) = self.image_part().map_err(write_logs)?;
        file.write_all(&snapshot.bytes).map_err(write_logs)?;
        Ok((meta, path))
    }

    /// Clear the install marker once the engine holds the snapshot.
    pub fn finish_install(&self) -> Result<(), StorageIOError<NodeId>> {
        let write = self.inner.db.begin_write().map_err(write_logs)?;
        write
            .open_table(META)
            .map_err(write_logs)?
            .remove(INSTALLING_KEY)
            .map_err(write_logs)?;
        self.commit_durable(write).map_err(write_logs)
    }

    /// The snapshot an interrupted install left behind, if any: the
    /// engine may hold a mix of old and new databases until it
    /// installs this again.
    pub fn pending_install(&self) -> Result<Option<SnapshotImage>, StorageIOError<NodeId>> {
        let read = self.inner.db.begin_read().map_err(read_logs)?;
        let meta = read.open_table(META).map_err(read_logs)?;
        if meta.get(INSTALLING_KEY).map_err(read_logs)?.is_none() {
            return Ok(None);
        }
        match image_in(&read.open_table(SNAPSHOT).map_err(read_logs)?)? {
            Some(record) => Ok(Some(self.image(record)?)),
            None => Err(read_logs(std::io::Error::other(
                "an install is pending but no snapshot is recorded",
            ))),
        }
    }

    /// The current snapshot's image, if any.
    pub fn current_image(&self) -> Result<Option<SnapshotImage>, StorageIOError<NodeId>> {
        let read = self.inner.db.begin_read().map_err(read_logs)?;
        let record = image_in(&read.open_table(SNAPSHOT).map_err(read_logs)?)?;
        record.map(|record| self.image(record)).transpose()
    }

    /// Claim this file for `contract`, the storage contract of the build
    /// opening it, as [`flower_engine::storage::GroupStore::claim_contract`]
    /// claims a group file: recorded in a file that holds nothing yet,
    /// compared in one that does; `Err(Some(other))` names another
    /// build's, `Err(None)` says the file holds rows but records none.
    pub fn claim_contract(
        &self,
        contract: &str,
    ) -> Result<Result<(), Option<String>>, StorageIOError<NodeId>> {
        let write = self.inner.db.begin_write().map_err(write_logs)?;
        let claimed = {
            let mut meta = write.open_table(META).map_err(write_logs)?;
            let stored = meta
                .get(CONTRACT_KEY)
                .map_err(write_logs)?
                .map(|row| String::from_utf8_lossy(row.value()).into_owned());
            match stored {
                Some(stored) if stored == contract => Ok(()),
                Some(stored) => Err(Some(stored)),
                None => {
                    let vote = write.open_table(VOTE).map_err(write_logs)?;
                    let log = write.open_table(LOG).map_err(write_logs)?;
                    let snapshot = write.open_table(SNAPSHOT).map_err(write_logs)?;
                    if vote.first().map_err(write_logs)?.is_some()
                        || log.first().map_err(write_logs)?.is_some()
                        || meta.first().map_err(write_logs)?.is_some()
                        || snapshot.first().map_err(write_logs)?.is_some()
                    {
                        Err(None)
                    } else {
                        meta.insert(CONTRACT_KEY, contract.as_bytes())
                            .map_err(write_logs)?;
                        Ok(())
                    }
                }
            }
        };
        write.commit().map_err(write_logs)?;
        Ok(claimed)
    }

    /// Whether this file holds any Raft state: a vote, an entry, a purge
    /// position or a snapshot. A member opening a file without any
    /// cannot tell whether it is new or lost what it acknowledged (see
    /// [`crate::rejoin`]).
    pub fn has_raft_state(&self) -> Result<bool, StorageIOError<NodeId>> {
        let read = self.inner.db.begin_read().map_err(read_logs)?;
        let vote = read.open_table(VOTE).map_err(read_vote)?;
        let log = read.open_table(LOG).map_err(read_logs)?;
        let meta = read.open_table(META).map_err(read_logs)?;
        let snapshot = read.open_table(SNAPSHOT).map_err(read_logs)?;
        Ok(vote.get(VOTE_KEY).map_err(read_vote)?.is_some()
            || log.first().map_err(read_logs)?.is_some()
            || meta.get(PURGED_KEY).map_err(read_logs)?.is_some()
            || snapshot.get(IMAGE_KEY).map_err(read_logs)?.is_some()
            || snapshot
                .get(LEGACY_SNAPSHOT_KEY)
                .map_err(read_logs)?
                .is_some())
    }

    /// Whether the rejoining mark is set: this member opened without
    /// Raft state and has not caught up since (see [`crate::rejoin`]).
    pub fn rejoining(&self) -> Result<bool, StorageIOError<NodeId>> {
        let read = self.inner.db.begin_read().map_err(read_logs)?;
        let meta = read.open_table(META).map_err(read_logs)?;
        Ok(meta.get(REJOINING_KEY).map_err(read_logs)?.is_some())
    }

    /// Store the saved vote as uncommitted, keeping its term and leader.
    /// OpenRaft restores a member whose saved vote is its own committed
    /// one straight to leader; a rejoining member may have lost entries
    /// it wrote as leader, so it must win a later term instead.
    pub fn uncommit_vote(&self) -> Result<(), StorageIOError<NodeId>> {
        let write = self.inner.db.begin_write().map_err(write_vote)?;
        {
            let mut table = write.open_table(VOTE).map_err(write_vote)?;
            let vote = match table.get(VOTE_KEY).map_err(read_vote)? {
                Some(row) => {
                    serde_json::from_slice::<Vote<NodeId>>(row.value()).map_err(read_vote)?
                }
                None => return Ok(()),
            };
            if !vote.committed {
                return Ok(());
            }
            let uncommitted = Vote {
                committed: false,
                ..vote
            };
            let bytes = serde_json::to_vec(&uncommitted).map_err(write_vote)?;
            table
                .insert(VOTE_KEY, bytes.as_slice())
                .map_err(write_vote)?;
        }
        self.commit_durable(write).map_err(write_vote)
    }

    /// Set or clear the rejoining mark, durably.
    pub fn set_rejoining(&self, rejoining: bool) -> Result<(), StorageIOError<NodeId>> {
        self.set_mark(REJOINING_KEY, rejoining)
    }

    /// Set or clear a meta mark, durably.
    fn set_mark(&self, key: &[u8], set: bool) -> Result<(), StorageIOError<NodeId>> {
        let write = self.inner.db.begin_write().map_err(write_logs)?;
        {
            let mut meta = write.open_table(META).map_err(write_logs)?;
            if set {
                meta.insert(key, b"1".as_slice()).map_err(write_logs)?;
            } else {
                meta.remove(key).map_err(write_logs)?;
            }
        }
        self.commit_durable(write).map_err(write_logs)
    }

    /// Whether the last process shut down with every leader append
    /// durable ([`LogStore::stop_flushing`]); the mark goes, durably, so
    /// a crash of this process is told apart next time.
    pub fn take_clean_shutdown(&self) -> Result<bool, StorageIOError<NodeId>> {
        let clean = {
            let read = self.inner.db.begin_read().map_err(read_logs)?;
            let meta = read.open_table(META).map_err(read_logs)?;
            meta.get(CLEAN_KEY).map_err(read_logs)?.is_some()
        };
        if clean {
            self.set_mark(CLEAN_KEY, false)?;
        }
        Ok(clean)
    }

    /// Store this member's own committed leader vote as uncommitted (see
    /// [`LogStore::uncommit_vote`]); a vote for another leader stays.
    /// Returns whether it was this member's committed vote.
    pub fn uncommit_own_vote(&self, id: NodeId) -> Result<bool, StorageIOError<NodeId>> {
        let own = {
            let read = self.inner.db.begin_read().map_err(read_vote)?;
            let table = read.open_table(VOTE).map_err(read_vote)?;
            match table.get(VOTE_KEY).map_err(read_vote)? {
                Some(row) => {
                    let vote: Vote<NodeId> =
                        serde_json::from_slice(row.value()).map_err(read_vote)?;
                    vote.committed && vote.leader_id().voted_for() == Some(id)
                }
                None => false,
            }
        };
        if own {
            self.uncommit_vote()?;
        }
        Ok(own)
    }

    /// The current snapshot's metadata, if any.
    pub fn snapshot_meta(&self) -> Result<Option<RaftSnapshotMeta>, StorageIOError<NodeId>> {
        Ok(self.current_image()?.map(|image| image.meta))
    }

    /// Load the current snapshot into memory, if any: what sync serving
    /// ships to devices.
    pub fn load_snapshot(&self) -> Result<Option<StoredSnapshot>, StorageIOError<NodeId>> {
        for _ in 0..IMAGE_READ_ATTEMPTS {
            let read = self.inner.db.begin_read().map_err(read_logs)?;
            let Some(record) = image_in(&read.open_table(SNAPSHOT).map_err(read_logs)?)? else {
                return Ok(None);
            };
            drop(read);
            if let Some(snapshot) = self.read_image(&record)? {
                return Ok(Some(snapshot));
            }
        }
        Err(read_logs(std::io::Error::other(
            "the snapshot kept changing while loading it",
        )))
    }

    /// A record's image, located in the image directory.
    fn image(&self, record: ImageRecord) -> Result<SnapshotImage, StorageIOError<NodeId>> {
        Ok(SnapshotImage {
            path: self.inner.images.path(&record.file).map_err(read_logs)?,
            meta: record.meta,
        })
    }

    /// Read a recorded image into memory; `None` when a newer snapshot
    /// replaced (and deleted) it since its record was read.
    fn read_image(
        &self,
        record: &ImageRecord,
    ) -> Result<Option<StoredSnapshot>, StorageIOError<NodeId>> {
        let path = self.inner.images.path(&record.file).map_err(read_logs)?;
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(read_logs(error)),
        };
        Ok(Some(StoredSnapshot {
            meta: serde_json::to_vec(&record.meta).map_err(read_logs)?,
            bytes,
        }))
    }

    /// Entries in a `u64` range above the purge point, decoded.
    fn entries<R: RangeBounds<u64>>(
        &self,
        range: R,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageIOError<NodeId>> {
        let read = self.inner.db.begin_read().map_err(read_logs)?;
        let purged = purged_in(&read.open_table(META).map_err(read_logs)?)?;
        let table = read.open_table(LOG).map_err(read_logs)?;
        let start = above_purged(range.start_bound(), purged);
        entries_in(&table, (start, range.end_bound().cloned()))
    }

    /// The last stored log ID by index order, if any.
    fn last_log_id(&self) -> Result<Option<LogId<NodeId>>, StorageIOError<NodeId>> {
        let read = self.inner.db.begin_read().map_err(read_logs)?;
        let table = read.open_table(LOG).map_err(read_logs)?;
        let Some(row) = table.last().map_err(read_logs)? else {
            return Ok(None);
        };
        let entry: Entry<TypeConfig> = serde_json::from_slice(row.1.value()).map_err(read_logs)?;
        Ok(Some(entry.log_id))
    }

    /// Run a durable write on the blocking pool: redb commits fsync.
    async fn blocking<T, F>(&self, work: F) -> Result<T, StorageIOError<NodeId>>
    where
        T: Send + 'static,
        F: FnOnce(&LogStore) -> Result<T, StorageIOError<NodeId>> + Send + 'static,
    {
        let store = self.clone();
        tokio::task::spawn_blocking(move || work(&store))
            .await
            .map_err(|error| write_logs(std::io::Error::other(error.to_string())))?
    }

    /// Delete every entry from `index` on, in one transaction: a
    /// conflicting uncommitted tail, never long.
    fn truncate_from(&self, index: u64) -> Result<(), StorageIOError<NodeId>> {
        let write = self.inner.db.begin_write().map_err(write_logs)?;
        {
            let mut table = write.open_table(LOG).map_err(write_logs)?;
            let lower = index.to_be_bytes();
            table
                .retain_in::<&[u8], _>(lower.as_slice().., |_, _| false)
                .map_err(write_logs)?;
        }
        self.commit_durable(write).map_err(write_logs)?;
        Ok(())
    }

    /// Hold purged entries for the backup, from now on and on every
    /// later open until [`LogStore::drop_purge_hold`]: purges keep the
    /// entries the backup has not released, at most `max_held` of them
    /// below the purge point (`None`: every one). Entries already
    /// deleted stay deleted.
    pub async fn hold_purges(&self, max_held: Option<u64>) -> Result<(), StorageIOError<NodeId>> {
        self.hold_purges_capped(max_held, None).await
    }

    /// [`LogStore::hold_purges`] with a byte cap too: at most
    /// `max_held_bytes` of stored entries below the purge point (`None`:
    /// no byte cap), measured in spans of [`PURGE_CHUNK`] entries, the
    /// oldest spans going first past it. The entries a member already
    /// holds are measured now; later ones as purges pass them.
    pub async fn hold_purges_capped(
        &self,
        max_held: Option<u64>,
        max_held_bytes: Option<u64>,
    ) -> Result<(), StorageIOError<NodeId>> {
        self.blocking(move |store| {
            let write = store.inner.db.begin_write().map_err(write_logs)?;
            write
                .open_table(META)
                .map_err(write_logs)?
                .insert(
                    HOLD_KEY,
                    encode_max_held(max_held, max_held_bytes).as_slice(),
                )
                .map_err(write_logs)?;
            store.commit_durable(write).map_err(write_logs)?;
            {
                let mut hold = store.hold();
                hold.enabled = true;
                hold.max_held = max_held;
                hold.max_held_bytes = max_held_bytes;
            }
            if max_held_bytes.is_none() {
                *store.held() = HeldBytes::default();
            } else if let Some(purged) = store.last_purged()? {
                store.measure_held(purged.index)?;
            }
            Ok(())
        })
        .await?;
        // A smaller cap than an earlier run's lets entries go now.
        self.sweep_released()
    }

    /// Let the hold go of entries at or below `index`, which the backup
    /// archived: the background purge deletes the purged ones among
    /// them. Nothing goes without a hold.
    pub async fn release_purges(&self, index: u64) -> Result<(), StorageIOError<NodeId>> {
        {
            let mut hold = self.hold();
            if !hold.enabled || hold.released >= Some(index) {
                return Ok(());
            }
            hold.released = Some(index);
        }
        self.sweep_released()
    }

    /// Stop holding, here and on later opens: the background purge
    /// deletes every held entry.
    pub async fn drop_purge_hold(&self) -> Result<(), StorageIOError<NodeId>> {
        self.blocking(|store| {
            let write = store.inner.db.begin_write().map_err(write_logs)?;
            write
                .open_table(META)
                .map_err(write_logs)?
                .remove(HOLD_KEY)
                .map_err(write_logs)?;
            store.commit_durable(write).map_err(write_logs)?;
            *store.hold() = PurgeHold::default();
            *store.held() = HeldBytes::default();
            Ok(())
        })
        .await?;
        self.sweep_released()
    }

    /// The purge hold as it stands.
    pub fn purge_hold(&self) -> PurgeHold {
        *self.hold()
    }

    /// The first index at or above `start` the log still stores, held
    /// entries below the purge point included.
    pub fn first_index_from(&self, start: u64) -> Result<Option<u64>, StorageIOError<NodeId>> {
        let read = self.inner.db.begin_read().map_err(read_logs)?;
        let table = read.open_table(LOG).map_err(read_logs)?;
        let mut rows = table
            .range::<&[u8]>(start.to_be_bytes().as_slice()..)
            .map_err(read_logs)?;
        let Some(row) = rows.next() else {
            return Ok(None);
        };
        let (key, _) = row.map_err(read_logs)?;
        let bytes: [u8; 8] = key
            .value()
            .try_into()
            .map_err(|_| read_logs(std::io::Error::other("log key is not a u64 index")))?;
        Ok(Some(u64::from_be_bytes(bytes)))
    }

    /// Read every entry still stored in `[start, end)`, held entries
    /// below the purge point included: what the backup archives. Raft's
    /// reads ([`LogStore::read_entries`]) start above the purge point.
    pub fn read_retained(
        &self,
        start: u64,
        end: u64,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageIOError<NodeId>> {
        let read = self.inner.db.begin_read().map_err(read_logs)?;
        let table = read.open_table(LOG).map_err(read_logs)?;
        entries_in(&table, start..end)
    }

    /// Lock the purge hold.
    fn hold(&self) -> std::sync::MutexGuard<'_, PurgeHold> {
        self.inner.hold.lock().expect("purge hold poisoned")
    }

    /// Hand the purged entries the hold no longer keeps to the
    /// background purge, when any are still on disk.
    fn sweep_released(&self) -> Result<(), StorageIOError<NodeId>> {
        let Some(purged) = self.last_purged()? else {
            return Ok(());
        };
        let Some(bound) = self.sweep_bound(purged.index) else {
            return Ok(());
        };
        if self.first_index_from(0)?.is_none_or(|first| first > bound) {
            return Ok(());
        }
        {
            let mut state = self.inner.sweep.lock();
            if state.stopped {
                return Ok(());
            }
            state.target = state.target.max(Some(purged.index));
            state.pending = true;
        }
        self.sweep_in_background();
        Ok(())
    }
}

/// The stored hold caps: empty for none, eight big-endian bytes for an
/// entry cap alone, sixteen for both (no entry cap as `u64::MAX`).
fn encode_max_held(max_held: Option<u64>, max_held_bytes: Option<u64>) -> Vec<u8> {
    match (max_held, max_held_bytes) {
        (None, None) => Vec::new(),
        (Some(max), None) => max.to_be_bytes().to_vec(),
        (entries, Some(bytes)) => {
            let mut encoded = entries.unwrap_or(u64::MAX).to_be_bytes().to_vec();
            encoded.extend_from_slice(&bytes.to_be_bytes());
            encoded
        }
    }
}

/// Decode [`encode_max_held`] into the entry and byte caps; anything
/// but eight or sixteen bytes is no cap.
fn decode_max_held(bytes: &[u8]) -> (Option<u64>, Option<u64>) {
    let number = |bytes: &[u8]| bytes.try_into().ok().map(u64::from_be_bytes);
    match bytes.len() {
        8 => (number(bytes), None),
        16 => (
            number(&bytes[..8]).filter(|&max| max != u64::MAX),
            number(&bytes[8..]),
        ),
        _ => (None, None),
    }
}

/// The background purge: while purged entries remain on disk, delete
/// them a chunk per transaction with a pause between, holding the store
/// only for each transaction. Ends when the store closes, when
/// [`LogStore::stop_purging`] asks, or on a failed transaction (logged;
/// the next purge starts it again from the first entry on disk).
async fn run_sweep(sweep: Arc<Sweep>, store: Weak<LogStoreInner>) {
    loop {
        if sweep.lock().stopped {
            break;
        }
        if !sweep.behind() {
            // The purge it finished, as snapshot work.
            let work = sweep.lock().work.take();
            if let Some(mut work) = work {
                work.outcome(SnapshotOutcome::Swept);
            }
            tokio::select! {
                () = sweep.wake.notified() => {}
                () = tokio::time::sleep(PURGE_IDLE) => {}
            }
            if store.strong_count() == 0 {
                break;
            }
            continue;
        }
        tokio::time::sleep(PURGE_PAUSE).await;
        #[cfg(test)]
        while sweep.held.load(std::sync::atomic::Ordering::Acquire) && !sweep.lock().stopped {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        if sweep.lock().stopped {
            break;
        }
        let Some(inner) = store.upgrade() else {
            break;
        };
        let logs = LogStore { inner };
        match tokio::task::spawn_blocking(move || logs.sweep_chunk()).await {
            Ok(Ok(_)) => {}
            Ok(Err(error)) => {
                tracing::error!(
                    target: "flower::storage",
                    %error,
                    "deleting purged log entries failed; the next purge deletes them"
                );
                if let Some(work) = sweep.lock().work.as_mut() {
                    work.outcome(SnapshotOutcome::Failed);
                }
                break;
            }
            Err(error) => {
                tracing::error!(target: "flower::storage", %error, "log purge task failed");
                if let Some(work) = sweep.lock().work.as_mut() {
                    work.outcome(SnapshotOutcome::Failed);
                }
                break;
            }
        }
    }
    let work = {
        let mut state = sweep.lock();
        state.running = false;
        state.pending = false;
        state.work.take()
    };
    drop(work);
}

/// The leader flusher: complete held appends as durable commits cover
/// them, and flush when the policy says they are due (see
/// [`crate::flush`]). Holds the store only for each flush, so a store
/// whose last user is gone closes its file (redb makes its commits
/// durable on close). Ends then, once stopped with nothing held, or on a
/// failed flush, which fails every held append.
async fn run_flusher(flusher: Arc<LeaderFlusher>, store: Weak<LogStoreInner>) {
    /// How often an idle flusher checks that its store is still open.
    const IDLE: Duration = Duration::from_secs(1);
    loop {
        let Some(durable) = store.upgrade().map(|inner| inner.commits.durable()) else {
            flusher.fail("the Raft log closed".to_string());
            return;
        };
        flusher.release(durable, false);
        let now = Instant::now();
        let Some(due) = flusher.due(now) else {
            if flusher.is_stopped() {
                return;
            }
            tokio::select! {
                () = flusher.wake.notified() => {}
                () = tokio::time::sleep(IDLE) => {}
            }
            continue;
        };
        if due.at > now {
            tokio::select! {
                () = flusher.wake.notified() => {}
                () = tokio::time::sleep_until(tokio::time::Instant::from_std(due.at)) => {}
            }
            continue;
        }
        let Some(inner) = store.upgrade() else {
            flusher.fail("the Raft log closed".to_string());
            return;
        };
        let logs = LogStore { inner };
        match tokio::task::spawn_blocking(move || logs.flush_commits()).await {
            Ok(Ok(durable)) => {
                flusher.flushed(due.grace, Instant::now());
                flusher.release(durable, true);
            }
            Ok(Err(error)) => {
                tracing::error!(
                    target: "flower::storage",
                    %error,
                    "flushing the leader's log failed"
                );
                flusher.fail(error.to_string());
                return;
            }
            Err(error) => {
                flusher.fail(error.to_string());
                return;
            }
        }
    }
}

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>>
    where
        RB: RangeBounds<u64> + Clone + Debug + OptionalSend,
    {
        Ok(self.entries(range)?)
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = LogStore;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let purged = self.last_purged()?;
        Ok(LogState {
            last_purged_log_id: purged,
            last_log_id: self.last_log_id()?.max(purged),
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut trace = self.observer().storage(StorageOperation::Vote);
        let bytes = serde_json::to_vec(vote).map_err(write_vote)?;
        trace.bytes(bytes.len());
        trace.stage(StorageStage::Encode);
        let (trace, result) = self
            .blocking(move |store| {
                trace.stage(StorageStage::Queue);
                let result = (|| -> Result<(), StorageIOError<NodeId>> {
                    let write = store.inner.db.begin_write().map_err(write_vote)?;
                    {
                        let mut table = write.open_table(VOTE).map_err(write_vote)?;
                        table
                            .insert(VOTE_KEY, bytes.as_slice())
                            .map_err(write_vote)?;
                    }
                    trace.stage(StorageStage::Write);
                    store.commit_durable(write).map_err(write_vote)?;
                    trace.stage(StorageStage::Commit);
                    Ok(())
                })();
                Ok((trace, result))
            })
            .await?;
        trace.finish(result.is_ok());
        result?;
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        let read = self.inner.db.begin_read().map_err(read_vote)?;
        let table = read.open_table(VOTE).map_err(read_vote)?;
        let Some(row) = table.get(VOTE_KEY).map_err(read_vote)? else {
            return Ok(None);
        };
        let vote: Vote<NodeId> = serde_json::from_slice(row.value()).map_err(read_vote)?;
        Ok(Some(vote))
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut trace = self.observer().storage(StorageOperation::Append);
        let mut rows = Vec::new();
        let mut commands = 0;
        let mut blank = false;
        for entry in entries {
            if trace.on() {
                commands += commands_in(&entry);
            }
            match &entry.payload {
                EntryPayload::Membership(membership) => {
                    self.inner.flusher.appended_membership(membership);
                }
                EntryPayload::Blank => blank = true,
                EntryPayload::Normal(_) => {}
            }
            let bytes = serde_json::to_vec(&entry).map_err(write_logs)?;
            trace.bytes(bytes.len());
            rows.push((entry.log_id.index, bytes));
        }
        trace.size(StorageBatch::Entries, rows.len() as u64);
        trace.size(StorageBatch::Commands, commands);
        trace.stage(StorageStage::Encode);
        let span = match (rows.first(), rows.last()) {
            (Some((first, _)), Some((last, _))) => *first..=*last,
            _ => {
                trace.finish(true);
                callback.log_io_completed(Ok(()));
                return Ok(());
            }
        };
        // The leader's own append commits without an fsync and completes
        // once a durable commit covers it (see `crate::flush`); every
        // other one is durable before its callback.
        let leader = callback.is_leader_append();
        if leader && blank {
            // A leader's first entry of its term: see `LeaderFlusher::leading`.
            self.inner.flusher.leading(Instant::now());
        }
        let deferred = leader && self.inner.flusher.defers();
        let (trace, result) = self
            .blocking(move |store| {
                trace.stage(StorageStage::Queue);
                let result = (|| -> Result<(), StorageIOError<NodeId>> {
                    let write = store.inner.db.begin_write().map_err(write_logs)?;
                    {
                        let mut table = write.open_table(LOG).map_err(write_logs)?;
                        for (index, bytes) in &rows {
                            table
                                .insert(index.to_be_bytes().as_slice(), bytes.as_slice())
                                .map_err(write_logs)?;
                        }
                    }
                    let committed = store.record_committed(&write)?;
                    trace.stage(StorageStage::Write);
                    let last = *span.end();
                    if deferred {
                        // Readable now; durable at a later commit, ordered
                        // before every later write of the file.
                        let seq = store.commit_deferred(write)?;
                        trace.stage(StorageStage::Commit);
                        store.committed_recorded(committed);
                        store.inner.marks.appended(last);
                        store.inner.flusher.hold(seq, span, callback);
                    } else {
                        store.commit_durable(write).map_err(write_logs)?;
                        trace.stage(StorageStage::Commit);
                        store.committed_recorded(committed);
                        store.inner.marks.appended(last);
                        if leader {
                            store.inner.flusher.direct();
                        }
                        // Readable before the callback: the commit above is
                        // the flush.
                        callback.log_io_completed(Ok(()));
                    }
                    Ok(())
                })();
                Ok((trace, result))
            })
            .await?;
        trace.finish(result.is_ok());
        result?;
        if deferred {
            self.start_flusher();
        }
        Ok(())
    }

    /// Remember the committed index; the next log write records it (see
    /// [`LogStore::record_committed`]), so this never waits for a disk.
    /// The leader flusher learns which held appends the group committed.
    async fn save_committed(
        &mut self,
        committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        {
            let mut known = self.inner.committed.lock().expect("committed poisoned");
            known.known = known.known.max(committed);
        }
        self.inner.flusher.committed(committed.map(|id| id.index));
        Ok(())
    }

    /// The highest committed index remembered or recorded: OpenRaft
    /// re-applies up to it before the member starts.
    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        Ok(self.committed()?)
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.inner.flusher.truncated();
        let trace = self.observer().storage(StorageOperation::Truncate);
        let (trace, result) = self
            .blocking(move |store| {
                let mut trace = trace;
                trace.stage(StorageStage::Queue);
                let result = store.truncate_from(log_id.index);
                trace.stage(StorageStage::Commit);
                Ok((trace, result))
            })
            .await?;
        trace.finish(result.is_ok());
        result?;
        Ok(())
    }

    /// Record the purge point and delete one chunk; the background purge
    /// deletes the rest (see the module documentation). A
    /// `flower.raft.snapshot` span with `phase=purge` covers this part.
    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let observer = self.observer();
        let trace = observer.storage(StorageOperation::Purge);
        let mut work = SnapshotWork::begin(&observer, SnapshotPhase::Purge);
        work.at(log_id.index);
        let (trace, result) = self
            .blocking(move |store| {
                let mut trace = trace;
                trace.stage(StorageStage::Queue);
                let result = store.purge_head(log_id);
                trace.stage(StorageStage::Commit);
                Ok((trace, result))
            })
            .await?;
        trace.finish(result.is_ok());
        let pending = match result {
            Ok(pending) => pending,
            Err(error) => {
                work.outcome(SnapshotOutcome::Failed);
                return Err(error.into());
            }
        };
        work.outcome(SnapshotOutcome::Swept);
        if pending {
            // The background sweep finishes it, unless it already
            // finishes an earlier one.
            let mut state = self.inner.sweep.lock();
            if state.work.is_none() {
                work.outcome(SnapshotOutcome::Abandoned);
                state.work = Some(work);
            }
            drop(state);
            self.sweep_in_background();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flower_engine::storage::compact::{CompactionSettings, Step};
    use openraft::CommittedLeaderId;

    fn file_len(path: &Path) -> u64 {
        std::fs::metadata(path).unwrap().len()
    }

    /// Raw rows straight into the log table, in index order, as appends
    /// leave them: each about 1 KB.
    fn write_rows(store: &LogStore, indexes: std::ops::RangeInclusive<u64>) {
        let write = store.inner.db.begin_write().unwrap();
        {
            let mut table = write.open_table(LOG).unwrap();
            for index in indexes {
                table
                    .insert(index.to_be_bytes().as_slice(), [7u8; 1000].as_slice())
                    .unwrap();
            }
        }
        write.commit().unwrap();
    }

    fn retained(read: &redb::ReadTransaction) -> u64 {
        use redb::ReadableTableMetadata;
        read.open_table(LOG).unwrap().len().unwrap()
    }

    #[test]
    fn a_purged_log_compacts_while_sync_snapshots_stay_open() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("raft.redb");
        let store = LogStore::open(&path).unwrap();
        let compactor = store.compactor().expect("a file-backed log compacts");
        for batch in 0..40u64 {
            write_rows(&store, batch * 1000 + 1..=(batch + 1) * 1000);
        }
        // A purge under snapshot coverage frees the head of the log; the
        // retained tail sits at the end of the file.
        let purged = LogId::new(CommittedLeaderId::new(1, 1), 38_000);
        let mut pending = store.purge_head(purged).unwrap();
        while pending {
            pending = store.sweep_chunk().unwrap();
        }
        let before = file_len(&path);
        assert!(before > 40 << 20, "{before}");

        // Sync serving reads from one snapshot per batch, so some
        // snapshot is open at every moment, and appends go on between
        // steps.
        let settings = CompactionSettings::default();
        let mut snapshot = store.inner.db.begin_read().unwrap();
        let mut seen = 2_000;
        let mut appended = 0;
        while file_len(&path) * 4 >= before {
            assert_ne!(compactor.step(&settings).unwrap(), Step::Idle);
            appended += 1;
            write_rows(&store, 40_000 + appended..=40_000 + appended);
            let next = store.inner.db.begin_read().unwrap();
            assert_eq!(retained(&snapshot), seen);
            (snapshot, seen) = (next, 2_000 + appended);
            assert!(appended < 1000, "{before} → {}", file_len(&path));
        }
        assert_eq!(retained(&snapshot), seen);
        assert_eq!(store.last_purged().unwrap(), Some(purged));
        drop(snapshot);
        drop(compactor);
        drop(store);
        let reopened = LogStore::open(&path).unwrap();
        assert_eq!(retained(&reopened.inner.db.begin_read().unwrap()), seen);
    }

    fn log_id(index: u64) -> LogId<NodeId> {
        LogId::new(CommittedLeaderId::new(2, 1), index)
    }

    fn blank(index: u64) -> Entry<TypeConfig> {
        Entry {
            log_id: log_id(index),
            payload: openraft::EntryPayload::Blank,
        }
    }

    /// Entries physically in the log table in a range, purged or not.
    fn on_disk(store: &LogStore, range: std::ops::RangeInclusive<u64>) -> u64 {
        let read = store.inner.db.begin_read().unwrap();
        let table = read.open_table(LOG).unwrap();
        let (lower, upper) = (range.start().to_be_bytes(), range.end().to_be_bytes());
        table
            .range::<&[u8]>(lower.as_slice()..=upper.as_slice())
            .unwrap()
            .count() as u64
    }

    /// Append `1..=total` in chunk-sized batches.
    async fn append_blanks(store: &mut LogStore, total: u64) {
        use openraft::storage::RaftLogStorageExt;
        for first in (1..=total).step_by(PURGE_CHUNK as usize) {
            let last = (first + PURGE_CHUNK - 1).min(total);
            store
                .blocking_append((first..=last).map(blank))
                .await
                .unwrap();
        }
    }

    async fn swept(store: &LogStore) {
        tokio::time::timeout(Duration::from_secs(30), async {
            while store.purge_stats().behind {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the background purge finished");
    }

    /// The Raft core awaits `purge`: it must return after one bounded
    /// transaction however much log it lets go, while the rest goes in
    /// the background, a chunk per transaction, with appends, votes and
    /// reads served between them.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn purge_returns_after_one_chunk_and_the_rest_goes_in_the_background() {
        use openraft::storage::RaftLogStorageExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("raft.redb");
        let mut store = LogStore::open(&path).unwrap();
        let total = 8 * PURGE_CHUNK + 10;
        append_blanks(&mut store, total).await;
        let purged = total - 10;
        store
            .inner
            .sweep
            .held
            .store(true, std::sync::atomic::Ordering::Release);
        store.purge(log_id(purged)).await.unwrap();

        // Raft sees the whole purge at once; one chunk is gone so far,
        // and reads never see the rest.
        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id, Some(log_id(purged)));
        assert_eq!(state.last_log_id, Some(log_id(total)));
        assert_eq!(on_disk(&store, 1..=purged), purged - PURGE_CHUNK);
        assert!(store.purge_stats().behind);
        assert_eq!(store.try_get_log_entries(..).await.unwrap().len(), 10);
        assert!(store.read_entries(1, purged + 1).unwrap().is_empty());
        let view = store.sync_view(purged, total).unwrap();
        assert_eq!(view.entries.len(), 10);

        // The Raft core's own I/O goes on meanwhile.
        store.blocking_append([blank(total + 1)]).await.unwrap();
        store.save_vote(&Vote::new_committed(3, 1)).await.unwrap();
        let tail = store
            .try_get_log_entries(purged + 1..=total + 1)
            .await
            .unwrap();
        assert_eq!(tail.len(), 11);

        store
            .inner
            .sweep
            .held
            .store(false, std::sync::atomic::Ordering::Release);
        swept(&store).await;
        assert_eq!(on_disk(&store, 1..=purged), 0);
        assert_eq!(on_disk(&store, purged + 1..=total + 1), 11);
        let stats = store.purge_stats();
        assert!(stats.most <= PURGE_CHUNK, "{stats:?}");
        assert!(
            stats.transactions >= purged.div_ceil(PURGE_CHUNK),
            "{stats:?}"
        );
        assert_eq!(stats.deleted, purged);
        // The point never moves back.
        store.purge(log_id(5)).await.unwrap();
        assert_eq!(store.last_purged().unwrap(), Some(log_id(purged)));
        store.stop_purging().await;
    }

    /// Entries a crash (or a stop) left below the purge point stay
    /// invisible, and the next purge after a restart deletes them from
    /// the first entry on disk on.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn purged_entries_left_by_a_restart_go_at_the_next_purge() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("raft.redb");
        let total = 4 * PURGE_CHUNK + 10;
        let purged = total - 10;
        {
            let mut store = LogStore::open(&path).unwrap();
            append_blanks(&mut store, total).await;
            store
                .inner
                .sweep
                .held
                .store(true, std::sync::atomic::Ordering::Release);
            store.purge(log_id(purged)).await.unwrap();
            store.stop_purging().await;
            assert_eq!(on_disk(&store, 1..=purged), purged - PURGE_CHUNK);
        }
        let mut store = LogStore::open(&path).unwrap();
        assert_eq!(on_disk(&store, 1..=purged), purged - PURGE_CHUNK);
        assert_eq!(store.try_get_log_entries(..).await.unwrap().len(), 10);
        assert!(!store.purge_stats().behind);
        store.purge(log_id(purged + 1)).await.unwrap();
        swept(&store).await;
        assert_eq!(on_disk(&store, 1..=purged + 1), 0);
        assert_eq!(on_disk(&store, purged + 2..=total), 9);
        assert!(store.purge_stats().most <= PURGE_CHUNK);
        store.stop_purging().await;
    }

    fn snapshot_at(index: u64, bytes: &[u8]) -> StoredSnapshot {
        let meta = RaftSnapshotMeta {
            last_log_id: Some(log_id(index)),
            last_membership: openraft::StoredMembership::default(),
            snapshot_id: format!("2-{index}"),
        };
        StoredSnapshot {
            meta: serde_json::to_vec(&meta).unwrap(),
            bytes: bytes.to_vec(),
        }
    }

    fn image_files(dir: &Path) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(dir.join("snapshots"))
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect();
        names.sort();
        names
    }

    /// A build that finishes after a newer snapshot was recorded (an
    /// install overtook it) leaves the newer one current: sync serving
    /// would otherwise ship an image older than the purge point.
    #[test]
    fn an_older_build_never_replaces_a_newer_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let store = LogStore::open(&dir.path().join("raft.redb")).unwrap();
        store.save_snapshot(&snapshot_at(10, b"ten")).unwrap();
        store.save_snapshot(&snapshot_at(5, b"five")).unwrap();
        let current = store.load_snapshot().unwrap().unwrap();
        assert_eq!(current.bytes, b"ten");
        assert_eq!(image_files(dir.path()).len(), 1);
        store.save_snapshot(&snapshot_at(12, b"twelve")).unwrap();
        assert_eq!(store.load_snapshot().unwrap().unwrap().bytes, b"twelve");
        assert_eq!(image_files(dir.path()).len(), 1);
        // Installs always record what the engine now holds.
        store.begin_install(&snapshot_at(11, b"eleven")).unwrap();
        let pending = store.pending_install().unwrap().unwrap();
        assert_eq!(pending.meta.last_log_id, Some(log_id(11)));
        assert_eq!(std::fs::read(&pending.path).unwrap(), b"eleven");
        store.finish_install().unwrap();
        assert!(store.pending_install().unwrap().is_none());
    }

    /// A Raft file from before images moved out keeps its snapshot and
    /// install marker; opening deletes half-written and unrecorded images
    /// and keeps the recorded one.
    #[test]
    fn opening_migrates_whole_snapshots_and_clears_stray_images() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("raft.redb");
        {
            let db = redb::Database::create(&path).unwrap();
            let write = db.begin_write().unwrap();
            let legacy = serde_json::to_vec(&snapshot_at(7, b"seven")).unwrap();
            write
                .open_table(SNAPSHOT)
                .unwrap()
                .insert(LEGACY_SNAPSHOT_KEY, legacy.as_slice())
                .unwrap();
            write
                .open_table(META)
                .unwrap()
                .insert(INSTALLING_KEY, b"1".as_slice())
                .unwrap();
            write.commit().unwrap();
        }
        let store = LogStore::open(&path).unwrap();
        let pending = store.pending_install().unwrap().unwrap();
        assert_eq!(pending.meta.last_log_id, Some(log_id(7)));
        assert_eq!(store.load_snapshot().unwrap().unwrap().bytes, b"seven");
        let read = store.inner.db.begin_read().unwrap();
        let table = read.open_table(SNAPSHOT).unwrap();
        assert!(table.get(LEGACY_SNAPSHOT_KEY).unwrap().is_none());
        drop((table, read));
        let recorded = image_files(dir.path());
        assert_eq!(recorded.len(), 1);
        drop(store);

        let images = dir.path().join("snapshots");
        std::fs::write(images.join("90.part"), b"half").unwrap();
        std::fs::write(images.join("91.snapshot"), b"unrecorded").unwrap();
        std::fs::write(images.join("notes.txt"), b"not ours").unwrap();
        let store = LogStore::open(&path).unwrap();
        let mut left = recorded.clone();
        left.push("notes.txt".to_string());
        left.sort();
        assert_eq!(image_files(dir.path()), left);
        assert_eq!(store.load_snapshot().unwrap().unwrap().bytes, b"seven");
        // New images never reuse a number seen in the directory.
        let (part, _) = store.image_part().unwrap();
        assert_eq!(part.file_name().unwrap(), "92.part");
    }

    /// Every index on disk, held entries below the purge point included.
    fn stored(store: &LogStore) -> Vec<u64> {
        store
            .read_retained(0, u64::MAX)
            .unwrap()
            .iter()
            .map(|entry| entry.log_id.index)
            .collect()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn purges_keep_held_entries_until_the_backup_releases_them() {
        use openraft::storage::RaftLogStorageExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("raft.redb");
        let mut store = LogStore::open(&path).unwrap();
        append_blanks(&mut store, 10).await;
        store.hold_purges(None).await.unwrap();

        // Raft's purge point moves and Raft reads from there, but nothing
        // the backup has not released leaves the disk.
        store.purge(log_id(6)).await.unwrap();
        swept(&store).await;
        assert_eq!(store.last_purged().unwrap(), Some(log_id(6)));
        assert_eq!(stored(&store), (1..=10).collect::<Vec<_>>());
        assert_eq!(store.read_entries(0, u64::MAX).unwrap().len(), 4);
        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id, Some(log_id(6)));
        assert_eq!(state.last_log_id, Some(log_id(10)));

        // Released entries go in the background, never past the purge
        // point.
        store.release_purges(3).await.unwrap();
        swept(&store).await;
        assert_eq!(stored(&store), (4..=10).collect::<Vec<_>>());
        store.release_purges(8).await.unwrap();
        swept(&store).await;
        assert_eq!(stored(&store), (7..=10).collect::<Vec<_>>());
        store.purge(log_id(9)).await.unwrap();
        swept(&store).await;
        assert_eq!(stored(&store), vec![9, 10]);

        // The hold outlives a restart and releases nothing until told.
        store.stop_purging().await;
        drop(store);
        let mut store = LogStore::open(&path).unwrap();
        assert_eq!(
            store.purge_hold(),
            PurgeHold {
                enabled: true,
                released: None,
                max_held: None,
                max_held_bytes: None,
            }
        );
        store.blocking_append((11..=12).map(blank)).await.unwrap();
        store.purge(log_id(11)).await.unwrap();
        swept(&store).await;
        assert_eq!(stored(&store), vec![9, 10, 11, 12]);

        // Dropping the hold lets every purged entry go, for good.
        store.drop_purge_hold().await.unwrap();
        swept(&store).await;
        assert_eq!(stored(&store), vec![12]);
        store.stop_purging().await;
        drop(store);
        let mut store = LogStore::open(&path).unwrap();
        assert_eq!(store.purge_hold(), PurgeHold::default());
        store.blocking_append((13..=14).map(blank)).await.unwrap();
        store.purge(log_id(13)).await.unwrap();
        swept(&store).await;
        assert_eq!(stored(&store), vec![14]);
        store.release_purges(20).await.unwrap();
        assert!(!store.purge_stats().behind);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_capped_hold_lets_its_oldest_entries_go() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = LogStore::open(&dir.path().join("raft.redb")).unwrap();
        append_blanks(&mut store, 10).await;
        store.hold_purges(Some(3)).await.unwrap();
        store.purge(log_id(8)).await.unwrap();
        swept(&store).await;
        assert_eq!(stored(&store), (6..=10).collect::<Vec<_>>());
        store.release_purges(7).await.unwrap();
        swept(&store).await;
        assert_eq!(stored(&store), vec![8, 9, 10]);
        // Lowering the cap lets the excess go.
        store.hold_purges(Some(0)).await.unwrap();
        swept(&store).await;
        assert_eq!(stored(&store), vec![9, 10]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_byte_capped_hold_keeps_at_most_its_bytes_across_restarts() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("raft.redb");
        let mut store = LogStore::open(&path).unwrap();
        // 3,000 rows of 1,000 bytes: three spans of 1,024, 1,024 and 952.
        write_rows(&store, 1..=3000);
        store
            .hold_purges_capped(None, Some(2_100_000))
            .await
            .unwrap();
        store.purge(log_id(3000)).await.unwrap();
        swept(&store).await;
        // The oldest span went: what is left fits the cap.
        assert_eq!(store.first_index_from(0).unwrap(), Some(1025));
        assert_eq!(store.held_bytes(), 1_976_000);
        // Releasing and appending more keeps the measure in step.
        store.release_purges(2048).await.unwrap();
        swept(&store).await;
        assert_eq!(store.first_index_from(0).unwrap(), Some(2049));
        assert_eq!(store.held_bytes(), 952_000);
        store.stop_purging().await;
        drop(store);

        // A restart holds the same caps and measures what it holds anew
        // at the next purge.
        let mut store = LogStore::open(&path).unwrap();
        assert_eq!(store.purge_hold().max_held_bytes, Some(2_100_000));
        assert_eq!(store.purge_hold().max_held, None);
        write_rows(&store, 3001..=5000);
        store.purge(log_id(5000)).await.unwrap();
        swept(&store).await;
        // 2049..=5000 measured as spans from 2049: the first one went.
        assert_eq!(store.first_index_from(0).unwrap(), Some(3073));
        assert_eq!(store.held_bytes(), 1_928_000);
        // Without a byte cap nothing is measured.
        store.hold_purges(None).await.unwrap();
        assert_eq!(store.held_bytes(), 0);
    }

    #[test]
    fn hold_caps_round_trip_through_their_record() {
        for caps in [
            (None, None),
            (Some(7), None),
            (None, Some(1 << 30)),
            (Some(3), Some(9)),
        ] {
            assert_eq!(decode_max_held(&encode_max_held(caps.0, caps.1)), caps);
        }
        assert_eq!(decode_max_held(&[1, 2, 3]), (None, None));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_last_log_id_never_falls_below_the_purge_point() {
        use openraft::storage::RaftLogStorageExt;
        let dir = tempfile::tempdir().unwrap();
        let mut store = LogStore::open(&dir.path().join("raft.redb")).unwrap();
        append_blanks(&mut store, 5).await;
        store.hold_purges(None).await.unwrap();
        // A snapshot install purges past the log's end: the held entries
        // stay, below a purge point with nothing stored above it.
        store.purge(log_id(8)).await.unwrap();
        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id, Some(log_id(8)));
        assert_eq!(state.last_log_id, Some(log_id(8)));
        assert_eq!(store.last_log_index().unwrap(), Some(8));
        assert_eq!(store.first_index_from(0).unwrap(), Some(1));
        assert_eq!(store.first_index_from(3).unwrap(), Some(3));
        assert_eq!(store.first_index_from(6).unwrap(), None);
        store.blocking_append([blank(9)]).await.unwrap();
        assert_eq!(store.first_index_from(6).unwrap(), Some(9));
        assert_eq!(stored(&store), vec![1, 2, 3, 4, 5, 9]);
    }

    #[test]
    fn stores_over_a_custom_backend_without_a_path_never_compact() {
        let db = redb::Builder::new()
            .create_with_backend(redb::backends::InMemoryBackend::new())
            .unwrap();
        let store = LogStore::from_database(db, None).unwrap();
        assert!(store.compactor().is_none());
        assert_eq!(store.last_purged().unwrap(), None);
    }
}
