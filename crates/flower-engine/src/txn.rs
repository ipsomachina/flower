//! Two-phase commit across groups: the replicated records.
//!
//! A transaction whose steps live in several groups commits by
//! two-phase commit, coordinated by the group owning its first step (see
//! the cluster's `txn` module for the protocol). This module holds what
//! each group replicates about it, in its `$cluster` database, changed
//! only by [`TxnCommand`]s its writer orders with its other writes, so
//! members, devices and restore replay all hold the same records:
//!
//! - a *coordinator record* per transaction the group coordinates: its
//!   participants, the decision once taken (with every step's result on
//!   commit), and which participants finished;
//! - a *prepared record* per transaction the group takes part in: the
//!   multi-database patch its steps prepared, their results, and the
//!   coordinator to ask;
//! - a *lock* on each prepared database (its `txn_lock` meta row);
//! - a *tombstone* per transaction the group finished, with the
//!   decision, so a late prepare of a finished transaction is refused.
//!
//! A prepared database fences writes that touch its durable read or write
//! footprint, including transitive sources of cached derived values and
//! snapshot-backed timer/queue reads. Independent writes, including retry
//! receipts, may commit while it waits. Untracked, oversized and uncertified
//! derived reads keep a database fence. Reads are never refused: committed
//! state shows nothing of a prepared transaction until it finishes.
//! A database holding a lock can neither freeze for a move nor take a
//! deployment, and a database that is not serving takes no prepare.

use serde::{Deserialize, Serialize};

use flower_sdk::{Failure, Value, fail};

use crate::error::{Error, HostResult, Result};
use crate::placement::{self, Placement, to_value};
use crate::storage::{ReadTxn, WriteTxn};

/// The group database holding the records.
pub const TXN_DB: &[u8] = b"$cluster";
/// Meta row in a prepared database naming the transaction holding it.
pub const LOCK_ROW: &[u8] = b"txn_lock";
/// Failure code: the database is locked by a prepared transaction.
/// Retryable: the lock goes when the transaction finishes.
pub const TXN_PREPARED: &str = "transaction_prepared";
/// Failure code: a prepare found a database another transaction holds.
pub const TXN_CONFLICT: &str = "transaction_conflict";
/// Failure code: the transaction finished, or never prepared here.
pub const TXN_FINISHED: &str = "transaction_finished";
/// Failure code: a command naming a transaction this group has no
/// record of.
pub const TXN_UNKNOWN: &str = "transaction_unknown";
/// Failure code: the transaction was aborted.
pub const TXN_ABORTED: &str = "transaction_aborted";
/// Failure code: a participant's database does not serve at the epoch
/// the coordinator resolved.
pub const TXN_PLACEMENT: &str = "transaction_placement";

const COORD_PREFIX: &[u8] = b"txn_coord\x00";
const KEY_PREFIX: &[u8] = b"txn_key\x00";
const PREP_PREFIX: &[u8] = b"txn_prep\x00";
const DONE_PREFIX: &[u8] = b"txn_done\x00";

/// One database a participant group holds for a transaction.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct PlacedDb {
    /// The database.
    pub db: String,
    /// The ownership epoch the coordinator resolved; zero for an
    /// unplaced database.
    pub epoch: u64,
    /// The transaction's steps on it, by index.
    pub steps: Vec<usize>,
}

/// One participant group.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Participant {
    /// The group's name.
    pub group: String,
    /// Its uid.
    #[serde(with = "crate::catalog::uid")]
    pub uid: u64,
    /// Its databases in the transaction.
    pub dbs: Vec<PlacedDb>,
}

/// A transaction's outcome.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Decision {
    /// Every participant commits.
    Commit,
    /// Every participant drops what it prepared.
    Abort,
}

/// The coordinator's record of one transaction.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct CoordinatorRecord {
    /// The transaction ID: 32 hex digits.
    pub id: String,
    /// The caller's idempotency key, hex, if any.
    pub key: Option<String>,
    /// Every participant group, the coordinator's own included when it
    /// holds steps.
    pub participants: Vec<Participant>,
    /// How many steps the transaction has.
    pub steps: usize,
    /// The decision, once taken: the first applied wins.
    pub decision: Option<Decision>,
    /// Every step's result as canonical JSON text, on commit.
    pub results: Vec<String>,
    /// Why it aborted.
    pub reason: Option<String>,
    /// Canonical failure details, including the failing global step.
    #[serde(default)]
    pub details: Option<String>,
    /// The failure code it aborted with, when not `transaction_aborted`
    /// (`transaction_too_large`).
    #[serde(default)]
    pub code: Option<String>,
    /// Participant groups that finished.
    pub finished: Vec<String>,
    /// On commit, where each finished participant committed: a retry
    /// with the caller's key is answered from these.
    #[serde(default)]
    pub commits: Vec<ParticipantCommit>,
    /// Whether every participant finished.
    pub complete: bool,
    /// When it began, ms since the epoch.
    pub created_ms: u64,
    /// When it was decided.
    pub decided_ms: Option<u64>,
    /// When every participant had finished.
    pub completed_ms: Option<u64>,
}

/// A participant's record of one prepared transaction.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct PreparedRecord {
    /// The transaction ID.
    pub id: String,
    /// The coordinator group.
    pub coordinator: String,
    /// Its uid.
    #[serde(with = "crate::catalog::uid")]
    pub coordinator_uid: u64,
    /// This group's databases in it, with their epochs.
    pub dbs: Vec<PlacedDb>,
    /// The prepared multi-database patch, hex.
    pub patch: String,
    /// Each of this group's steps' results, as `(step, JSON text)`.
    pub results: Vec<(usize, String)>,
    /// When it prepared, ms since the epoch.
    pub prepared_ms: u64,
    /// Durable read footprint; absent legacy records retain a database fence.
    #[serde(default)]
    pub reads: Option<Box<crate::storage::overlay::ReadSet>>,
}

/// A finished transaction, at a participant.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Tombstone {
    /// The decision it finished with.
    pub decision: Decision,
    /// When, ms since the epoch.
    pub at_ms: u64,
    /// On commit, each of this group's databases in it with the revision
    /// the commit left.
    #[serde(default)]
    pub revisions: Vec<(String, u64)>,
    /// The log position of the entry that finished it here.
    #[serde(default)]
    pub position: u64,
}

/// Where one participant group committed its part: what the caller is
/// answered, and answered again on a retry.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ParticipantCommit {
    /// The group.
    pub group: String,
    /// The log position of its finish, in its own log.
    pub position: u64,
    /// Its databases with the revisions the commit left.
    pub revisions: Vec<(String, u64)>,
}

/// A change to a group's transaction records.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(tag = "command", rename_all = "snake_case")]
pub enum TxnCommand {
    /// The coordinator records a transaction before any participant
    /// hears of it.
    Begin {
        /// The record, undecided.
        record: Box<CoordinatorRecord>,
    },
    /// The coordinator decides; the first decision applied wins.
    Decide {
        /// The transaction.
        id: String,
        /// The decision.
        decision: Decision,
        /// Every step's result, on commit.
        results: Vec<String>,
        /// Why, on abort.
        reason: Option<String>,
        #[serde(default)]
        details: Option<String>,
        /// The failure code, on an abort that is not
        /// `transaction_aborted`.
        #[serde(default)]
        code: Option<String>,
    },
    /// Participants finished.
    Finished {
        /// The transaction.
        id: String,
        /// The groups that answered their finish.
        groups: Vec<String>,
        /// Where each of them committed, on commit.
        #[serde(default)]
        commits: Vec<ParticipantCommit>,
    },
    /// A participant prepares its steps and locks their databases.
    Prepare {
        /// The prepared record.
        record: PreparedRecord,
    },
    /// A participant applies the decision.
    Finish {
        /// The transaction.
        id: String,
        /// The decision.
        decision: Decision,
    },
    /// Drop history: completed coordinator records finished before
    /// `records_before_ms`, tombstones before `tombstones_before_ms`, at
    /// most `limit` rows.
    Collect {
        /// Completed records finished before this go.
        records_before_ms: u64,
        /// Tombstones before this go.
        tombstones_before_ms: u64,
        /// Most rows deleted.
        limit: usize,
    },
}

/// What a finish committed, for the applier's notices.
#[derive(Clone, Debug, Default)]
pub struct Finished {
    /// `(database, revision)` for each database a commit moved.
    pub revisions: Vec<(Vec<u8>, u64)>,
    /// The records each of those commits put and deleted, in the same
    /// order (see [`crate::patch::Patch::record_counts`]).
    pub records: Vec<(u64, u64)>,
}

fn key(prefix: &[u8], id: &str) -> Vec<u8> {
    let mut key = prefix.to_vec();
    key.extend_from_slice(id.as_bytes());
    key
}

fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    serde_json::from_slice(bytes)
        .map_err(|error| Error::corrupt(format!("bad transaction row: {error}")))
}

fn put<T: Serialize>(write: &WriteTxn, row: &[u8], value: &T) -> Result<()> {
    let bytes = serde_json::to_vec(value)
        .map_err(|error| Error::corrupt(format!("transaction row does not encode: {error}")))?;
    write.set_meta(TXN_DB, row, &bytes)
}

/// The transaction holding `db`'s lock in this write's state, if any.
pub fn lock_holder(write: &WriteTxn, db: &[u8]) -> Result<Option<String>> {
    Ok(write
        .meta(db, LOCK_ROW)?
        .map(|bytes| String::from_utf8_lossy(&bytes).into_owned()))
}

/// The transaction holding `db`'s lock in this read's state, if any.
pub fn read_lock_holder(read: &ReadTxn, db: &[u8]) -> Result<Option<String>> {
    Ok(read
        .meta(db, LOCK_ROW)?
        .map(|bytes| String::from_utf8_lossy(&bytes).into_owned()))
}

/// Refuse a patch only when it may change a prepared participant's state.
pub fn read_patch_refusal(read: &ReadTxn, patch: &crate::patch::Patch) -> Result<Option<Failure>> {
    let Some(holder) = read_lock_holder(read, &patch.db)? else {
        return Ok(None);
    };
    patch_refusal(
        read.meta(TXN_DB, &key(PREP_PREFIX, &holder))?,
        &holder,
        patch,
    )
}

pub fn write_patch_refusal(
    write: &WriteTxn,
    patch: &crate::patch::Patch,
) -> Result<Option<Failure>> {
    let Some(holder) = lock_holder(write, &patch.db)? else {
        return Ok(None);
    };
    patch_refusal(
        write.meta(TXN_DB, &key(PREP_PREFIX, &holder))?,
        &holder,
        patch,
    )
}

fn patch_refusal(
    bytes: Option<Vec<u8>>,
    holder: &str,
    patch: &crate::patch::Patch,
) -> Result<Option<Failure>> {
    use crate::storage::{SnapshotTable as Table, keys, meta};
    let Some(bytes) = bytes else {
        return Err(Error::corrupt("transaction lock has no prepared record"));
    };
    let record: PreparedRecord = decode(&bytes)?;
    let refuse = || Some(locked(&patch.db, holder));
    let Some(reads) = &record.reads else {
        return Ok(refuse());
    };
    // The prepare carries the leader's source proof: followers' derived
    // caches can differ, so their local graph must never decide this fence.
    if reads.overflowed() || reads.derived_touched_by(patch)? {
        return Ok(refuse());
    }
    let raw = crate::hex::decode(&record.patch)
        .ok_or_else(|| Error::corrupt("prepared patch is not hex"))?;
    let held = crate::patch::decode_multi_patch(&raw)?;
    let held = held
        .iter()
        .find(|p| p.db == patch.db)
        .ok_or_else(|| Error::corrupt("lock database is absent from prepared patch"))?;
    if !reads.tracks_derived_sources() && (!held.derived.is_empty() || !held.drops.is_empty()) {
        return Ok(refuse());
    }
    for name in [
        meta::REVISION,
        meta::APPLIED_LOG,
        crate::retention::STATE_ROW,
    ] {
        if reads.touched_by(Table::Meta, &keys::encode_meta_key(&patch.db, name)?) {
            return Ok(refuse());
        }
    }
    for row in &patch.records {
        let key = keys::encode_record_key(&patch.db, row.collection, &row.key)?;
        if reads.touched_by(Table::Records, &key)
            || held
                .records
                .iter()
                .any(|r| r.collection == row.collection && r.key == row.key)
        {
            return Ok(refuse());
        }
    }
    for entry in &patch.indexes {
        let components: Vec<_> = entry.components.iter().map(Vec::as_slice).collect();
        let key = keys::encode_index_key(&patch.db, entry.index, &components, &entry.primary)?;
        if reads.touched_by(Table::IndexEntries, &key)
            || held.indexes.iter().any(|e| {
                e.index == entry.index
                    && e.components == entry.components
                    && e.primary == entry.primary
            })
        {
            return Ok(refuse());
        }
    }
    for (definition, args) in patch
        .derived
        .iter()
        .map(|d| (d.definition, d.args.as_slice()))
        .chain(patch.drops.iter().map(|(d, args)| (*d, args.as_slice())))
    {
        let key = keys::encode_derived_key(&patch.db, definition, args)?;
        if reads.touched_by(Table::Derived, &key)
            || reads.touched_by(Table::DependentEdges, &key)
            || held
                .derived
                .iter()
                .any(|d| d.definition == definition && d.args == args)
            || held
                .drops
                .iter()
                .any(|(d, at)| *d == definition && at == args)
        {
            return Ok(refuse());
        }
    }
    for (identity, _) in &patch.receipts {
        let key = keys::encode_receipt_key(&patch.db, identity)?;
        if reads.touched_by(Table::Receipts, &key)
            || held.receipts.iter().any(|(id, _)| id == identity)
        {
            return Ok(refuse());
        }
    }
    Ok(None)
}

/// The refusal for a write to a locked database.
pub fn locked(db: &[u8], holder: &str) -> Failure {
    fail(
        TXN_PREPARED,
        format!(
            "database `{}` is locked by prepared transaction {holder}",
            String::from_utf8_lossy(db)
        ),
    )
}

/// A coordinator record, as `read` sees it.
pub fn coordinator(read: &ReadTxn, id: &str) -> Result<Option<CoordinatorRecord>> {
    read.meta(TXN_DB, &key(COORD_PREFIX, id))?
        .map(|bytes| decode(&bytes))
        .transpose()
}

/// The coordinator record a caller's idempotency key began, if kept.
pub fn by_key(read: &ReadTxn, caller_key: &str) -> Result<Option<CoordinatorRecord>> {
    let Some(id) = read.meta(TXN_DB, &key(KEY_PREFIX, caller_key))? else {
        return Ok(None);
    };
    coordinator(read, &String::from_utf8_lossy(&id))
}

/// A prepared record, as `read` sees it.
pub fn prepared(read: &ReadTxn, id: &str) -> Result<Option<PreparedRecord>> {
    read.meta(TXN_DB, &key(PREP_PREFIX, id))?
        .map(|bytes| decode(&bytes))
        .transpose()
}

/// A tombstone, as `read` sees it.
pub fn tombstone(read: &ReadTxn, id: &str) -> Result<Option<Tombstone>> {
    read.meta(TXN_DB, &key(DONE_PREFIX, id))?
        .map(|bytes| decode(&bytes))
        .transpose()
}

/// Every coordinator record the group keeps, in ID order.
pub fn coordinators(read: &ReadTxn) -> Result<Vec<CoordinatorRecord>> {
    read.scan_meta_prefix(TXN_DB, COORD_PREFIX)?
        .into_iter()
        .map(|(_, bytes)| decode(&bytes))
        .collect()
}

/// Every prepared record the group holds, in ID order.
pub fn prepared_records(read: &ReadTxn) -> Result<Vec<PreparedRecord>> {
    read.scan_meta_prefix(TXN_DB, PREP_PREFIX)?
        .into_iter()
        .map(|(_, bytes)| decode(&bytes))
        .collect()
}

/// Why a participant database cannot prepare at `epoch`, if it cannot:
/// it must serve here at exactly that epoch (zero: unplaced), and no
/// transaction may hold it.
fn unpreparable(write: &WriteTxn, placed: &PlacedDb) -> Result<Option<Failure>> {
    let db = placed.db.as_bytes();
    if let Some(holder) = lock_holder(write, db)? {
        return Ok(Some(fail(
            TXN_CONFLICT,
            format!("database `{}` is locked by transaction {holder}", placed.db),
        )));
    }
    if let Some(refusal) = placement::refusal(write, db)? {
        return Ok(Some(refusal));
    }
    let epoch = match placement::read_in(write, db)? {
        Some(Placement::Active { epoch }) => epoch,
        Some(_) => return placement::refusal(write, db),
        None => 0,
    };
    Ok((epoch != placed.epoch).then(|| {
        fail(
            TXN_PLACEMENT,
            format!(
                "database `{}` serves at epoch {epoch} here, not {}",
                placed.db, placed.epoch
            ),
        )
    }))
}

impl crate::engine::Engine {
    /// Apply a transaction command inside `staged` at `position` and
    /// expiry clock `now_ms`: its answer or deterministic refusal, and
    /// what a finish committed.
    pub fn commit_txn(
        &self,
        staged: &crate::storage::StagedWrite,
        command: &TxnCommand,
        position: u64,
        now_ms: u64,
        advance: &mut dyn FnMut(&[u8]) -> bool,
    ) -> HostResult<(HostResult<Value>, Finished)> {
        let write = staged.write();
        let none = Finished::default();
        let refuse =
            |code: &str, message: String| Ok((Err(fail(code, message)), Finished::default()));
        match command {
            TxnCommand::Begin { record } => {
                let row = key(COORD_PREFIX, &record.id);
                if let Some(existing) = write.meta(TXN_DB, &row).map_err(Failure::from)? {
                    let existing: CoordinatorRecord = decode(&existing).map_err(Failure::from)?;
                    return Ok((Ok(to_value(&existing)), none));
                }
                if let Some(caller_key) = &record.key {
                    let index = key(KEY_PREFIX, caller_key);
                    if let Some(id) = write.meta(TXN_DB, &index).map_err(Failure::from)? {
                        return refuse(
                            TXN_CONFLICT,
                            format!(
                                "idempotency key began transaction {}",
                                String::from_utf8_lossy(&id)
                            ),
                        );
                    }
                    write
                        .set_meta(TXN_DB, &index, record.id.as_bytes())
                        .map_err(Failure::from)?;
                }
                let mut record = record.clone();
                record.created_ms = now_ms;
                put(write, &row, &record).map_err(Failure::from)?;
                Ok((Ok(to_value(&record)), none))
            }
            TxnCommand::Decide {
                id,
                decision,
                results,
                reason,
                details,
                code,
            } => {
                let row = key(COORD_PREFIX, id);
                let Some(bytes) = write.meta(TXN_DB, &row).map_err(Failure::from)? else {
                    return refuse(TXN_UNKNOWN, format!("no transaction {id} here"));
                };
                let mut record: CoordinatorRecord = decode(&bytes).map_err(Failure::from)?;
                if record.decision.is_none() {
                    record.decision = Some(*decision);
                    record.decided_ms = Some(now_ms);
                    match decision {
                        Decision::Commit => record.results = results.clone(),
                        Decision::Abort => {
                            record.reason = reason.clone();
                            record.details = details.clone();
                            record.code = code.clone();
                        }
                    }
                    put(write, &row, &record).map_err(Failure::from)?;
                }
                Ok((Ok(to_value(&record)), none))
            }
            TxnCommand::Finished {
                id,
                groups,
                commits,
            } => {
                let row = key(COORD_PREFIX, id);
                let Some(bytes) = write.meta(TXN_DB, &row).map_err(Failure::from)? else {
                    return refuse(TXN_UNKNOWN, format!("no transaction {id} here"));
                };
                let mut record: CoordinatorRecord = decode(&bytes).map_err(Failure::from)?;
                if record.decision.is_none() {
                    return refuse(
                        TXN_UNKNOWN,
                        format!("transaction {id} has no decision to finish"),
                    );
                }
                for group in groups {
                    if !record.finished.contains(group)
                        && record.participants.iter().any(|p| &p.group == group)
                    {
                        record.finished.push(group.clone());
                    }
                }
                // The first report of where a group committed stands: its
                // tombstone answers every repeated finish the same.
                if record.decision == Some(Decision::Commit) {
                    for commit in commits {
                        if record.finished.contains(&commit.group)
                            && !record.commits.iter().any(|c| c.group == commit.group)
                        {
                            record.commits.push(commit.clone());
                        }
                    }
                }
                if !record.complete && record.finished.len() == record.participants.len() {
                    record.complete = true;
                    record.completed_ms = Some(now_ms);
                }
                put(write, &row, &record).map_err(Failure::from)?;
                Ok((Ok(to_value(&record)), none))
            }
            TxnCommand::Prepare { record } => {
                let row = key(PREP_PREFIX, &record.id);
                if let Some(existing) = write.meta(TXN_DB, &row).map_err(Failure::from)? {
                    let existing: PreparedRecord = decode(&existing).map_err(Failure::from)?;
                    return Ok((Ok(to_value(&existing)), none));
                }
                if let Some(done) = write
                    .meta(TXN_DB, &key(DONE_PREFIX, &record.id))
                    .map_err(Failure::from)?
                {
                    let done: Tombstone = decode(&done).map_err(Failure::from)?;
                    return refuse(
                        TXN_FINISHED,
                        format!(
                            "transaction {} finished here ({:?})",
                            record.id, done.decision
                        ),
                    );
                }
                for placed in &record.dbs {
                    if let Some(refusal) = unpreparable(write, placed).map_err(Failure::from)? {
                        return Ok((Err(refusal), none));
                    }
                }
                // The patch must still apply as prepared: the writer
                // prepared it on this very state.
                let patch = crate::hex::decode(&record.patch).ok_or_else(|| {
                    Failure::from(Error::corrupt("the prepared patch is not hex".to_string()))
                })?;
                let checked =
                    Self::check_multi_entries(&staged.read(), &patch, position, |_| false)?;
                if let Some(rejected) = checked.rejected {
                    return Ok((Err(rejected), none));
                }
                for placed in &record.dbs {
                    write
                        .set_meta(placed.db.as_bytes(), LOCK_ROW, record.id.as_bytes())
                        .map_err(Failure::from)?;
                }
                let mut record = record.clone();
                record.prepared_ms = now_ms;
                put(write, &row, &record).map_err(Failure::from)?;
                Ok((Ok(to_value(&record)), none))
            }
            TxnCommand::Finish { id, decision } => {
                let done_row = key(DONE_PREFIX, id);
                let row = key(PREP_PREFIX, id);
                let Some(bytes) = write.meta(TXN_DB, &row).map_err(Failure::from)? else {
                    // Nothing prepared here: an abort leaves a tombstone so
                    // a late prepare is refused; a commit repeats the
                    // finish that already applied, or never prepared.
                    let existing = write.meta(TXN_DB, &done_row).map_err(Failure::from)?;
                    return match (existing, decision) {
                        (Some(done), _) => {
                            let done: Tombstone = decode(&done).map_err(Failure::from)?;
                            if done.decision == *decision {
                                Ok((Ok(to_value(&done)), none))
                            } else {
                                refuse(
                                    TXN_FINISHED,
                                    format!("transaction {id} finished as {:?}", done.decision),
                                )
                            }
                        }
                        (None, Decision::Abort) => {
                            let done = Tombstone {
                                decision: Decision::Abort,
                                at_ms: now_ms,
                                revisions: Vec::new(),
                                position,
                            };
                            put(write, &done_row, &done).map_err(Failure::from)?;
                            Ok((Ok(to_value(&done)), none))
                        }
                        (None, Decision::Commit) => {
                            refuse(TXN_UNKNOWN, format!("transaction {id} never prepared here"))
                        }
                    };
                };
                let record: PreparedRecord = decode(&bytes).map_err(Failure::from)?;
                // The locks go first: the commit below is the locked
                // transaction's own.
                for placed in &record.dbs {
                    write
                        .delete_meta(placed.db.as_bytes(), LOCK_ROW)
                        .map_err(Failure::from)?;
                }
                let mut finished = Finished::default();
                if *decision == Decision::Commit {
                    let mut patch = crate::hex::decode(&record.patch).ok_or_else(|| {
                        Failure::from(Error::corrupt("the prepared patch is not hex".to_string()))
                    })?;
                    if record.reads.is_some() {
                        // Commuting writes can advance the revision. The
                        // durable footprint preserved every value this
                        // transaction read; do not mistake those writes
                        // for a failed optimistic revision check.
                        let mut entries =
                            crate::patch::decode_multi_patch(&patch).map_err(Failure::from)?;
                        for entry in &mut entries {
                            entry.expected_revision = None;
                        }
                        patch =
                            crate::patch::encode_multi_patch(&entries).map_err(Failure::from)?;
                    }
                    let checked =
                        Self::check_multi_entries(&staged.read(), &patch, position, &mut *advance)?;
                    if let Some(rejected) = &checked.rejected {
                        // Its locks kept everything it read unchanged, so
                        // this is a defect: keep it prepared and visible
                        // rather than commit part of a transaction.
                        return Err(Error::corrupt(format!(
                            "prepared transaction {id} no longer applies: {}: {}",
                            rejected.code, rejected.message
                        ))
                        .into());
                    }
                    let outcome = self.commit_multi(write, &checked, position, now_ms)?;
                    finished.records = checked.records(&outcome.revisions);
                    finished.revisions = outcome.revisions;
                }
                write.delete_meta(TXN_DB, &row).map_err(Failure::from)?;
                // Where this part committed, kept with the decision: every
                // repeated finish answers it, so the coordinator records
                // it once however many times it delivers.
                let done = Tombstone {
                    decision: *decision,
                    at_ms: now_ms,
                    revisions: finished
                        .revisions
                        .iter()
                        .map(|(db, revision)| (String::from_utf8_lossy(db).into_owned(), *revision))
                        .collect(),
                    position,
                };
                put(write, &done_row, &done).map_err(Failure::from)?;
                Ok((Ok(to_value(&done)), finished))
            }
            TxnCommand::Collect {
                records_before_ms,
                tombstones_before_ms,
                limit,
            } => {
                let mut deleted = 0usize;
                for (row, bytes) in write
                    .scan_meta_prefix(TXN_DB, COORD_PREFIX)
                    .map_err(Failure::from)?
                {
                    if deleted >= *limit {
                        break;
                    }
                    let record: CoordinatorRecord = decode(&bytes).map_err(Failure::from)?;
                    if record.complete
                        && record
                            .completed_ms
                            .is_some_and(|at| at < *records_before_ms)
                    {
                        write.delete_meta(TXN_DB, &row).map_err(Failure::from)?;
                        if let Some(caller_key) = &record.key {
                            write
                                .delete_meta(TXN_DB, &key(KEY_PREFIX, caller_key))
                                .map_err(Failure::from)?;
                        }
                        deleted += 1;
                    }
                }
                for (row, bytes) in write
                    .scan_meta_prefix(TXN_DB, DONE_PREFIX)
                    .map_err(Failure::from)?
                {
                    if deleted >= *limit {
                        break;
                    }
                    let done: Tombstone = decode(&bytes).map_err(Failure::from)?;
                    if done.at_ms < *tombstones_before_ms {
                        write.delete_meta(TXN_DB, &row).map_err(Failure::from)?;
                        deleted += 1;
                    }
                }
                Ok((Ok(Value::Number(deleted as f64)), none))
            }
        }
    }
}

/// Keep the read-side helpers' signatures in use for callers that only
/// read: whether `db` is locked in `read`.
pub fn is_locked(read: &ReadTxn, db: &[u8]) -> Result<bool> {
    Ok(read_lock_holder(read, db)?.is_some())
}

/// Why `db` takes no write in `read`'s state, if it takes none: it does
/// not serve here (see [`crate::placement`]) or a prepared transaction
/// holds it. Entries that bypass the writer's checks (deployments) ask
/// this at apply, on every applier alike.
pub fn unwritable(read: &ReadTxn, db: &[u8]) -> Result<Option<Failure>> {
    if let Some(refusal) = placement::read_refusal(read, db)? {
        return Ok(Some(refusal));
    }
    Ok(read_lock_holder(read, db)?.map(|holder| locked(db, &holder)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::patch::{IndexWrite, Patch, RecordWrite, encode_multi_patch};
    use crate::storage::{SnapshotTable, keys, overlay::ReadSet};

    fn patch() -> Patch {
        Patch {
            failed: false,
            db: b"db".to_vec(),
            versions: vec![],
            records: vec![],
            indexes: vec![],
            derived: vec![],
            drops: vec![],
            receipts: vec![],
            result: b"null".to_vec(),
            expected_revision: Some(1),
            base_revision: 1,
        }
    }

    fn serialized(reads: ReadSet, held: Patch) -> Vec<u8> {
        serde_json::to_vec(&PreparedRecord {
            id: "id".into(),
            coordinator: "root".into(),
            coordinator_uid: 0,
            dbs: vec![],
            patch: crate::hex::encode(&encode_multi_patch(&[held]).unwrap()),
            results: vec![],
            prepared_ms: 0,
            reads: Some(Box::new(reads)),
        })
        .unwrap()
    }

    #[test]
    fn receipt_identity_and_accounting_reads_still_conflict() {
        let mut reads = ReadSet::participant();
        reads.row(
            SnapshotTable::Receipts,
            &keys::encode_receipt_key(b"db", b"seen").unwrap(),
        );
        let mut held = patch();
        held.receipts.push((b"held".to_vec(), vec![]));
        let bytes = serialized(reads.clone(), held.clone());
        for (identity, conflicts) in [
            (b"seen".as_slice(), true),
            (b"held", true),
            (b"other", false),
        ] {
            let mut incoming = patch();
            incoming.receipts.push((identity.to_vec(), vec![]));
            assert_eq!(
                patch_refusal(Some(bytes.clone()), "id", &incoming)
                    .unwrap()
                    .is_some(),
                conflicts
            );
        }
        reads.row(
            SnapshotTable::Meta,
            &keys::encode_meta_key(b"db", crate::retention::STATE_ROW).unwrap(),
        );
        assert!(
            patch_refusal(Some(serialized(reads, held)), "id", &patch())
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn derived_source_ranges_and_transitive_cache_changes_are_durable_conflicts() {
        use crate::storage::{
            GroupStore,
            codec::{DerivedOutcome, OutcomeBody, OutcomeTime},
        };
        use std::sync::{Arc, Mutex};
        let dir = tempfile::tempdir().unwrap();
        let store = GroupStore::create(dir.path().join("db")).unwrap();
        let write = store.begin_write().unwrap();
        let outcome = DerivedOutcome {
            pinned: false,
            code: [0; 32],
            time: OutcomeTime::Always,
            body: OutcomeBody::Value(b"1".to_vec()),
        };
        write.put_derived(b"db", 10, b"null", &outcome).unwrap();
        write.put_derived(b"db", 11, b"null", &outcome).unwrap();
        write
            .replace_edges(
                b"db",
                10,
                b"null",
                &[keys::encode_derived_dep(11, b"null").unwrap()],
            )
            .unwrap();
        write
            .replace_edges(
                b"db",
                11,
                b"null",
                &[
                    keys::encode_record_dep(1, b"absent").unwrap(),
                    keys::encode_range_dep(
                        2,
                        &keys::RangeDep {
                            prefix: vec![b"x".to_vec()],
                            gte: Some(b"a".to_vec()),
                            lt: Some(b"z".to_vec()),
                            ..Default::default()
                        },
                    )
                    .unwrap(),
                ],
            )
            .unwrap();
        write.commit().unwrap();
        let recording = Arc::new(Mutex::new(ReadSet::participant()));
        let read = store
            .begin_read()
            .unwrap()
            .recording_into(Some(recording.clone()));
        assert!(read.get_derived(b"db", 10, b"null").unwrap().is_some());
        let bytes = serialized(recording.lock().unwrap().clone(), patch());
        let decoded: PreparedRecord = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            serde_json::to_vec(&decoded).unwrap(),
            bytes,
            "every applier stores identical certificate bytes"
        );
        // Validation needs no local cache or graph, just this serialized proof.
        assert!(
            patch_refusal(Some(bytes.clone()), "id", &patch())
                .unwrap()
                .is_none()
        );
        for (primary, conflicts) in [(b"m".as_slice(), true), (b"z", false)] {
            let mut incoming = patch();
            incoming.indexes.push(IndexWrite {
                index: 2,
                components: vec![b"x".to_vec()],
                primary: primary.to_vec(),
                present: true,
            });
            assert_eq!(
                patch_refusal(Some(bytes.clone()), "id", &incoming)
                    .unwrap()
                    .is_some(),
                conflicts
            );
        }
        let mut incoming = patch();
        incoming.records.push(RecordWrite {
            collection: 1,
            key: b"absent".to_vec(),
            value: Some(b"1".to_vec()),
        });
        assert!(
            patch_refusal(Some(bytes.clone()), "id", &incoming)
                .unwrap()
                .is_some()
        );
        for (definition, conflicts) in [(10, true), (11, true), (12, false)] {
            let mut incoming = patch();
            incoming.drops.push((definition, b"null".to_vec()));
            assert_eq!(
                patch_refusal(Some(bytes.clone()), "id", &incoming)
                    .unwrap()
                    .is_some(),
                conflicts
            );
        }
        let mut legacy: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        legacy["reads"]
            .as_object_mut()
            .unwrap()
            .remove("derived_sources");
        assert!(
            patch_refusal(Some(serde_json::to_vec(&legacy).unwrap()), "id", &patch())
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn durable_footprints_protect_absence_ranges_and_blind_writes() {
        let mut reads = ReadSet::default();
        reads.row(
            SnapshotTable::Records,
            &keys::encode_record_key(b"db", 1, b"absent").unwrap(),
        );
        let start = keys::encode_index_key(b"db", 2, &[b"x"], b"a").unwrap();
        let end = keys::encode_index_key(b"db", 2, &[b"x"], b"z").unwrap();
        reads.range(SnapshotTable::IndexEntries, start, Some(end));
        let mut held = patch();
        held.records.push(RecordWrite {
            collection: 1,
            key: b"blind".to_vec(),
            value: Some(b"1".to_vec()),
        });
        let record = PreparedRecord {
            id: "id".into(),
            coordinator: "root".into(),
            coordinator_uid: 0,
            dbs: vec![],
            patch: crate::hex::encode(&encode_multi_patch(&[held]).unwrap()),
            results: vec![],
            prepared_ms: 0,
            reads: Some(Box::new(reads)),
        };
        // Exercise the serialized certificate every follower and restart reads.
        let bytes = serde_json::to_vec(&record).unwrap();
        for (key, conflicts) in [
            (b"absent".as_slice(), true),
            (b"blind", true),
            (b"other", false),
        ] {
            let mut incoming = patch();
            incoming.records.push(RecordWrite {
                collection: 1,
                key: key.to_vec(),
                value: Some(b"2".to_vec()),
            });
            assert_eq!(
                patch_refusal(Some(bytes.clone()), "id", &incoming)
                    .unwrap()
                    .is_some(),
                conflicts
            );
        }
        for (primary, conflicts) in [(b"m".as_slice(), true), (b"z", false)] {
            let mut incoming = patch();
            incoming.indexes.push(IndexWrite {
                index: 2,
                components: vec![b"x".to_vec()],
                primary: primary.to_vec(),
                present: true,
            });
            assert_eq!(
                patch_refusal(Some(bytes.clone()), "id", &incoming)
                    .unwrap()
                    .is_some(),
                conflicts
            );
        }
        let legacy = PreparedRecord {
            reads: None,
            ..record
        };
        assert!(
            patch_refusal(Some(serde_json::to_vec(&legacy).unwrap()), "id", &patch())
                .unwrap()
                .is_some()
        );
    }
}
