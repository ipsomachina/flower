//! Sync-down serving: retained entries plus snapshots.
//!
//! Followers that purged and devices that fell behind catch up from
//! one API: entries since a position, or a snapshot when the window
//! is gone. Only applied (hence committed) entries ship, so a device
//! never applies a write a new leader could overwrite. Blanks and
//! membership entries never ship: they carry no
//! database effects, though the covered position still advances past
//! them.
//!
//! A replica consumes a batch in order: apply each patch at its index
//! through the engine's checked apply, activate each deployment
//! idempotently (skip when the active identity matches, otherwise
//! apply with the carried bindings), commit each module removal as
//! members do (a removed module's purge pages come as batch items),
//! then set its position to the batch's covered index. A snapshot
//! batch installs first: fence and install every database view,
//! reload each module's runtime from the installed code, set the
//! position past the snapshot, then resume syncing from there.
//!
//! Every batch carries the serving member's applied group incarnation.
//! Positions restart when a cluster is restored or re-bootstrapped, so
//! a replica remembers the incarnation it follows and refuses a batch
//! from another one instead of applying a different history's entries
//! at its old position.

use openraft::{BasicNode, EntryPayload, LogId, StorageIOError, StoredMembership};

use crate::logstore::LogStore;
use crate::types::{ClusterWrite, NodeId};

/// Sync snapshot wire version, bumped on format changes.
pub const SYNC_SNAPSHOT_VERSION: u32 = 1;

/// Fencing metadata for one database snapshot.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct SyncDbMeta {
    /// Database identifier bytes.
    pub db: Vec<u8>,
    /// Committed revision the view was taken at.
    pub revision: u64,
    /// Applied log position the view was taken at.
    pub applied_log: u64,
    /// History incarnation the view belongs to.
    pub incarnation: u64,
}

/// One database inside a sync snapshot file.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct SyncDbSnapshot {
    /// Fencing metadata for the payload.
    pub meta: SyncDbMeta,
    /// Encoded engine snapshot payload.
    pub payload: Vec<u8>,
}

/// A sync snapshot file: every replicated database plus the group
/// position and membership the views were taken at. This is the
/// client-visible wire contract; the Raft snapshot builder writes it
/// and catching-up replicas decode it.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct SyncSnapshotFile {
    /// Wire version, always [`SYNC_SNAPSHOT_VERSION`].
    pub version: u32,
    /// Group applied log ID the views cover.
    pub applied: Option<LogId<NodeId>>,
    /// Last applied membership.
    pub membership: StoredMembership<NodeId, BasicNode>,
    /// One view per database, `$cluster` included.
    pub dbs: Vec<SyncDbSnapshot>,
}

/// One replicated entry for a catching-up replica.
#[derive(Clone, Debug)]
pub struct SyncedEntry {
    /// Log index the write applied at.
    pub index: u64,
    /// The replicated write.
    pub write: ClusterWrite,
}

/// A full snapshot for a replica past the retained window.
#[derive(Clone, Debug)]
pub struct SyncSnapshot {
    /// Last log index the snapshot covers: the replica continues
    /// after this position once installed.
    pub last_index: u64,
    /// Encoded [`SyncSnapshotFile`] bytes.
    pub bytes: Vec<u8>,
}

/// One sync batch: data entries plus the highest index covered.
#[derive(Clone, Debug)]
pub struct SyncBatch {
    /// The position the batch continues from, echoed so a replica can
    /// check the batch starts exactly at its own position.
    pub since: u64,
    /// Data entries after `since`, in order.
    pub entries: Vec<SyncedEntry>,
    /// Highest log index this batch covers, including skipped
    /// blanks and membership entries: `(since, current]`
    /// is gapless.
    pub current: u64,
    /// Snapshot when the requested position fell out of the retained
    /// window; entries stay empty until the replica installs it.
    pub snapshot: Option<SyncSnapshot>,
    /// The serving member's applied group incarnation, zero until the
    /// group establishes one. Read after the applied position bounding
    /// the batch, so it covers every entry the batch carries.
    pub incarnation: u64,
}

/// Most entries one batch covers, whatever the caller asked for.
pub const MAX_SYNC_LIMIT: u64 = 4096;

/// Serve up to `limit` (capped at [`MAX_SYNC_LIMIT`]) entries after
/// `since`, never past `applied`: only entries this member applied,
/// hence committed, ship, so a device never applies a write a new
/// leader could still overwrite. Purge point, snapshot and entries come
/// from one consistent read, and the covered range is checked gapless.
/// Positions below the purge point fall back to the latest snapshot; a
/// purged log without a stored snapshot is a loud invariant violation.
/// The batch carries `incarnation`, the member's applied incarnation
/// read no earlier than `applied`.
pub fn serve_since(
    logs: &LogStore,
    since: u64,
    limit: u64,
    applied: u64,
    incarnation: u64,
) -> Result<SyncBatch, StorageIOError<NodeId>> {
    let limit = limit.clamp(1, MAX_SYNC_LIMIT);
    let last = applied.min(since.saturating_add(limit));
    let view = logs.sync_view(since, last)?;
    if let Some(purged) = view.purged
        && since < purged.index
    {
        let Some(stored) = view.snapshot else {
            return Err(invariant(format!(
                "log purged through {} but no snapshot is stored",
                purged.index
            )));
        };
        // The metadata records the position the image covers, so the
        // image itself ships undecoded.
        let meta: crate::logstore::RaftSnapshotMeta = serde_json::from_slice(&stored.meta)
            .map_err(|error| StorageIOError::read_snapshot(None, &error))?;
        let last_index = meta.last_log_id.map(|id| id.index).unwrap_or(0);
        return Ok(SyncBatch {
            since,
            entries: Vec::new(),
            current: since,
            snapshot: Some(SyncSnapshot {
                last_index,
                bytes: stored.bytes,
            }),
            incarnation,
        });
    }
    let mut entries = Vec::new();
    let mut current = since;
    for entry in &view.entries {
        if entry.log_id.index != current + 1 {
            return Err(invariant(format!(
                "log gap after {current}: next retained entry is {}",
                entry.log_id.index
            )));
        }
        current = entry.log_id.index;
        // Key catalog changes stay on members: devices hold no wrapping
        // key and serve no catalog keys. They still see the entry,
        // emptied but naming its database, because it moves the group
        // head on every applier (a batch prepared before it is stale
        // there too) and drops that database's key readers.
        if let EntryPayload::Normal(write) = &entry.payload {
            let write = match write {
                ClusterWrite::Keys {
                    db, proposed_ms, ..
                } => ClusterWrite::Keys {
                    db: db.clone(),
                    change: Vec::new(),
                    proposed_ms: *proposed_ms,
                },
                write => write.clone(),
            };
            entries.push(SyncedEntry {
                index: current,
                write,
            });
        }
    }
    if last > since && current != last {
        return Err(invariant(format!(
            "log ends at {current} but {last} is applied"
        )));
    }
    Ok(SyncBatch {
        since,
        entries,
        current,
        snapshot: None,
        incarnation,
    })
}

/// A sync-serving invariant violation, reported as a snapshot read
/// failure.
fn invariant(message: String) -> StorageIOError<NodeId> {
    StorageIOError::read_snapshot(None, &std::io::Error::other(message))
}
