//! The Raft state machine: ordered patch and deployment applier.
//!
//! Every committed entry applies through the engine's checked
//! journals, so followers converge with the leader: patches check
//! then commit beside the group applied position in one transaction,
//! batch entries apply their items in order in one transaction when
//! their head holds (see [`flower_engine::batch`]), deployments
//! activate staged code idempotently, and snapshots carry every
//! database plus the Raft position. Every data entry records itself as
//! the group head. Corrupt entries halt the node loudly instead of
//! diverging. All engine work runs on the blocking pool (see
//! [`crate::exec`]).
//!
//! Duplicate detection reads receipt expiry against a replicated
//! *expiry clock*: the largest proposal stamp applied so far, entry
//! stamps included, persisted beside the group position. It never runs
//! backwards along the log, whatever the leaders' wall clocks do, and
//! every member computes it identically. Receipt purging is local
//! hygiene that deletes only receipts expired at this member's expiry
//! clock; every later entry reads expiry at a clock at least that
//! large, so a purged receipt and a kept one give the same verdict and
//! purge timing can never make members diverge.
//!
//! Each history carries a group *incarnation*: a random non-zero tag in
//! the `$cluster` meta row [`meta::HISTORY_INCARNATION`], proposed by
//! a leader while none is applied (see
//! [`crate::node::Node::establish_incarnation`]). The first applied
//! proposal wins, so racing leaders converge. Applying it labels every
//! registered database, and databases registered later inherit the
//! label. A restored engine opening a fresh log starts a new history
//! ([`StateMachine::restart_history`]): the incarnation clears until
//! the new leader proposes a fresh one, so devices and backups can
//! tell the histories apart even though positions restart.
//!
//! A snapshot replaces the whole group state, all-or-nothing: the log
//! store records the snapshot and an install marker first, registered
//! databases the snapshot lacks are wiped, every database installs and
//! is marked replicated, the group position and membership commit,
//! and only then does the marker clear. A crash in between leaves the
//! marker, and [`StateMachine::resume_install`] installs the same
//! snapshot again before Raft starts.

use std::fs::File;
use std::io::{BufReader, BufWriter, Read};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use flower_engine::batch::{Applied, BatchItem, Head, STALE_BATCH};

use flower_engine::engine::{Activation, ApplyOutcome, Engine, MultiApplyOutcome};
use flower_engine::json::{parse_value, render};
use flower_engine::placement::PlacementEffect;
use flower_engine::runtime::Runtime;
use flower_engine::snapshot::{SnapshotMeta, apply_snapshot, encode_snapshot_in};
use flower_engine::staging::{CODE_IDENTITY_PREFIX, code_identity_key};
use flower_engine::storage::{GroupStore, ReadTxn, StagedWrite, WriteTxn, meta};
use flower_sdk::abi::encode_err;
use flower_sdk::{Failure, Value, fail};
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use openraft::{
    BasicNode, EntryPayload, LogId, Membership, OptionalSend, Snapshot,
    SnapshotMeta as RaftSnapshotMeta, StorageError, StorageIOError, StoredMembership,
};
use tokio::sync::Mutex as AsyncMutex;

use crate::exec::with_engine;
use crate::flush::ApplyDurability;
use crate::keys::{KEY_CATALOG, KeyCatalogCell, KeyCatalogs};
use crate::logstore::LogStore;
use crate::snapshot_file::{
    Checksum, Checksummed, FileHeader, FileWriter, SnapshotFile, ViewsError, for_each_view,
    read_header,
};
use crate::snapshot_policy::{Accounting, entry_bytes};
use crate::sync::{SyncDbMeta, SyncDbSnapshot};
use crate::telemetry::{
    self, SnapshotOutcome, SnapshotPhase, SnapshotWork, StorageBatch, StorageOperation,
    StorageStage,
};
use crate::types::{
    ClusterWrite, CommitNotice, NodeId, ResponseOutcome, TypeConfig, WriteResponse,
};

/// The group coordination database: applied position, membership and
/// the replicated database registry.
pub const CLUSTER_DB: &[u8] = b"$cluster";

/// Meta row holding the last group applied log ID as JSON.
const GROUP_APPLIED: &[u8] = b"group_applied";
/// Meta row holding the last applied membership as JSON.
const GROUP_MEMBERSHIP: &[u8] = b"group_membership";
/// Meta row holding the expiry clock: the largest applied proposal
/// stamp, as a `u64`.
const GROUP_EXPIRY_CLOCK: &[u8] = b"group_expiry_clock";
/// Meta-name prefix registering a replicated database:
/// `group_db\x00<db>` with a one-byte marker value.
pub(crate) const GROUP_DB_PREFIX: &[u8] = b"group_db\x00";
/// I/O buffer for reading and writing images: not a size limit.
const IMAGE_BUFFER: usize = 256 * 1024;

/// A test's hook into a snapshot build, run after each view.
#[cfg(test)]
type EncodeHook = Arc<dyn Fn() + Send + Sync>;
/// Opens of the current image that may race its replacement.
const IMAGE_OPEN_ATTEMPTS: usize = 8;

/// Build guest runtimes for deployed code: snapshot installs and
/// activation replays reconstruct execution from stored artifacts.
pub trait RuntimeFactory: Send + Sync {
    /// Build a runtime for a module of database `db` from its Wasm
    /// bytes plus its manifest value. The module name keys backends
    /// caching compiled code; the database scopes what the runtime
    /// resolves outside its rows, such as managed keys.
    fn build(
        &self,
        db: &[u8],
        module: &str,
        wasm: &[u8],
        manifest: &Value,
    ) -> Result<Box<dyn Runtime>, String>;
}

/// The state machine: an engine behind an async mutex, a log store
/// for snapshot persistence and a runtime factory for installs.
pub struct StateMachine {
    engine: Arc<AsyncMutex<Engine>>,
    logs: LogStore,
    runtimes: Arc<dyn RuntimeFactory>,
    commits: tokio::sync::broadcast::Sender<CommitNotice>,
    /// The applied group incarnation, republished whenever it may
    /// change, so sync serving reads it without the engine lock.
    incarnation: Arc<AtomicU64>,
    /// Applied bytes and snapshot builds, for the snapshot scheduler.
    accounting: Arc<Accounting>,
    /// Runs after each view a build writes, for tests.
    #[cfg(test)]
    encode_hook: Option<EncodeHook>,
    /// Receives the applied managed-key catalog.
    keys: Arc<KeyCatalogCell>,
    /// Counts applies that may change what operators see of the
    /// catalog.
    catalog: Arc<CatalogChanges>,
    /// Whether applies commit deferred, and what waits for a flush (see
    /// [`crate::flush`]).
    durability: Arc<ApplyDurability>,
    /// Closes once this machine and every snapshot builder it made are
    /// gone. Declared last, so it drops after their file handles: see
    /// [`StateMachine::files_released`].
    held: Arc<tokio::sync::watch::Sender<()>>,
}

/// A count of the changes to what operators see of a member's catalog:
/// applied deployments, activations and staging steps, key-catalog
/// changes, retention entries, the history incarnation, membership and
/// snapshot installs, and a one-call deployment's local staging on its
/// leader. A watch: subscribers see the
/// latest count, so a slow one never holds apply back, and look at the
/// applied state again when it moves.
#[derive(Debug)]
pub struct CatalogChanges(tokio::sync::watch::Sender<u64>);

impl Default for CatalogChanges {
    fn default() -> Self {
        CatalogChanges(tokio::sync::watch::channel(0).0)
    }
}

impl CatalogChanges {
    /// Count one change.
    pub fn bump(&self) {
        self.0.send_modify(|count| *count += 1);
    }

    /// Follow the count.
    pub fn subscribe(&self) -> tokio::sync::watch::Receiver<u64> {
        self.0.subscribe()
    }
}

/// Whether applying `entry` may change what operators see of the
/// catalog: data patches never do.
fn changes_catalog(entry: &openraft::Entry<TypeConfig>) -> bool {
    match &entry.payload {
        EntryPayload::Blank => false,
        EntryPayload::Membership(_) => true,
        EntryPayload::Normal(write) => match write {
            ClusterWrite::Patch { .. } | ClusterWrite::MultiPatch { .. } => false,
            // The partition catalog, a database's placement,
            // cross-group transaction records, a purge's progress and
            // replicated staging rows are operator state too.
            ClusterWrite::Batch { items, .. } => items.iter().any(|item| {
                matches!(
                    item,
                    BatchItem::Retention { .. }
                        | BatchItem::Catalog { .. }
                        | BatchItem::Placement { .. }
                        | BatchItem::Txn { .. }
                        | BatchItem::Purge { .. }
                        | BatchItem::Staging { .. }
                )
            }),
            ClusterWrite::Deploy { .. }
            | ClusterWrite::Remove { .. }
            | ClusterWrite::Keys { .. }
            | ClusterWrite::Incarnation { .. } => true,
        },
    }
}

impl StateMachine {
    /// Share an engine, a log store, a runtime factory and the
    /// commit-notice sender watch hubs subscribe to.
    pub fn new(
        engine: Arc<AsyncMutex<Engine>>,
        logs: LogStore,
        runtimes: Arc<dyn RuntimeFactory>,
        commits: tokio::sync::broadcast::Sender<CommitNotice>,
    ) -> Self {
        StateMachine {
            engine,
            logs,
            runtimes,
            commits,
            incarnation: Arc::new(AtomicU64::new(0)),
            accounting: Arc::new(Accounting::new(0, 0, None, None)),
            #[cfg(test)]
            encode_hook: None,
            keys: Arc::new(KeyCatalogCell::default()),
            catalog: Arc::new(CatalogChanges::default()),
            durability: Arc::new(ApplyDurability::new(None)),
            held: Arc::new(tokio::sync::watch::channel(()).0),
        }
    }

    /// Commit applies as `durability` says: durably each (the default),
    /// or deferred, made durable by its flushes (see [`crate::flush`]).
    pub fn with_apply_durability(mut self, durability: Arc<ApplyDurability>) -> Self {
        self.durability = durability;
        self
    }

    /// Report applies and snapshot builds into `accounting`, which a
    /// member seeds with what it applied since its snapshot.
    pub fn with_accounting(mut self, accounting: Arc<Accounting>) -> Self {
        self.accounting = accounting;
        self
    }

    /// Publish the applied managed-key catalog into `keys` on every
    /// apply, snapshot install and startup.
    pub fn with_key_catalog(mut self, keys: Arc<KeyCatalogCell>) -> Self {
        self.keys = keys;
        self
    }

    /// Count applies that may change the operator catalog into
    /// `catalog`.
    pub fn with_catalog_changes(mut self, catalog: Arc<CatalogChanges>) -> Self {
        self.catalog = catalog;
        self
    }

    /// Apply a change to database `db`'s managed-key catalog inside a
    /// caller's transaction: live applies and backup replay share it.
    /// A name that cannot be a database's ([`crate::keys::invalid_database`])
    /// takes no change, nor does a database that does not serve here
    /// (frozen, moving, moved, retired) or that a prepared transaction
    /// holds, as it takes no deployment. The outer error is a storage failure;
    /// the inner one a deterministic rejection that changed nothing.
    pub fn apply_key_change(
        write: &WriteTxn,
        db: &[u8],
        change: &[u8],
    ) -> Result<Result<serde_json::Value, String>, Failure> {
        if let Some(why) = crate::keys::invalid_database(db) {
            return Ok(Err(format!("{}: {why}", crate::node::KEY_REQUEST)));
        }
        let refusal = match flower_engine::placement::refusal(write, db)? {
            Some(refusal) => Some(refusal),
            None => flower_engine::txn::lock_holder(write, db)?
                .map(|holder| flower_engine::txn::locked(db, &holder)),
        };
        if let Some(refusal) = refusal {
            return Ok(Err(format!("{}: {}", refusal.code, refusal.message)));
        }
        let current = write.meta(db, KEY_CATALOG)?;
        match flower_crypto::catalog::apply_change(current.as_deref(), change) {
            Ok(applied) => {
                if let Some(catalog) = &applied.catalog {
                    register_in(write, db)?;
                    write.set_meta(db, KEY_CATALOG, catalog)?;
                }
                Ok(Ok(applied.response))
            }
            Err(message) => Ok(Err(message)),
        }
    }

    /// Read database `db`'s stored managed-key catalog, if any change
    /// applied.
    pub fn key_catalog(engine: &Engine, db: &[u8]) -> Result<Option<Vec<u8>>, Failure> {
        let read = engine.store().begin_read().map_err(Failure::from)?;
        read.meta(db, KEY_CATALOG).map_err(Failure::from)
    }

    /// Every stored managed-key catalog in `read`, by database: what a
    /// member serves after startup or a snapshot install.
    pub fn key_catalogs(read: &ReadTxn) -> Result<KeyCatalogs, Failure> {
        let mut catalogs = Vec::new();
        for db in registered(read).map_err(Failure::from)? {
            if let Some(catalog) = read.meta(&db, KEY_CATALOG).map_err(Failure::from)? {
                catalogs.push((db, catalog));
            }
        }
        Ok(catalogs)
    }

    /// The snapshot accounting this machine reports into.
    pub fn snapshot_accounting(&self) -> Arc<Accounting> {
        self.accounting.clone()
    }

    /// The published applied incarnation. Raft reads the applied state
    /// on startup, which publishes the stored value; applies and
    /// snapshot installs republish it, each before Raft reports the
    /// position applied.
    pub(crate) fn incarnation_handle(&self) -> Arc<AtomicU64> {
        self.incarnation.clone()
    }

    /// A receiver whose `changed` fails once this machine and every
    /// snapshot builder it made have dropped, and with them their handles
    /// on the group and Raft files. OpenRaft lets its state machine
    /// worker and builders go after its core stops, so a member that
    /// reopens its files waits for this first.
    pub fn files_released(&self) -> tokio::sync::watch::Receiver<()> {
        self.held.subscribe()
    }

    /// Apply one committed entry: journal patches, staged
    /// deployments, incarnations, membership records and leader
    /// blanks each advance the group applied position in the same
    /// transaction. Answers the entry's response and the commit notices
    /// of the databases it moved: one per touched database, so each
    /// database's watchers re-evaluate, with the records the write put
    /// and deleted there.
    fn apply_entry(
        engine: &mut Engine,
        runtimes: &Arc<dyn RuntimeFactory>,
        entry: &openraft::Entry<TypeConfig>,
    ) -> Result<(WriteResponse, Vec<CommitNotice>), StorageError<NodeId>> {
        let index = entry.log_id.index;
        let notified = |response: WriteResponse, changes: Vec<(Vec<u8>, u64, (u64, u64))>| {
            let notices = changes
                .into_iter()
                .map(|(db, revision, (puts, deletes))| {
                    CommitNotice::commit(db, revision, response.position)
                        .with_records(puts, deletes)
                })
                .collect();
            Ok((response, notices))
        };
        let response = match &entry.payload {
            EntryPayload::Blank => {
                let write = engine.store().begin_write().map_err(fail_map)?;
                record_applied(&write, &entry.log_id)?;
                write.commit().map_err(fail_map)?;
                WriteResponse::error("blank".to_string(), 0, index)
            }
            EntryPayload::Membership(membership) => {
                let write = engine.store().begin_write().map_err(fail_map)?;
                record_applied(&write, &entry.log_id)?;
                let stored = StoredMembership::new(Some(entry.log_id), membership.clone());
                record_membership(&write, &stored)?;
                write.commit().map_err(fail_map)?;
                WriteResponse::error("membership".to_string(), 0, index)
            }
            EntryPayload::Normal(write) => match write {
                ClusterWrite::Patch {
                    db,
                    patch,
                    proposed_ms,
                } => {
                    let (response, changes) =
                        Self::apply_patch(engine, db, patch, &entry.log_id, *proposed_ms)?;
                    return notified(response, changes);
                }
                ClusterWrite::MultiPatch {
                    patches,
                    proposed_ms,
                } => {
                    let (response, changes) =
                        Self::apply_multi(engine, patches, &entry.log_id, *proposed_ms)?;
                    return notified(response, changes);
                }
                ClusterWrite::Deploy {
                    db,
                    module,
                    wasm,
                    manifest,
                    identity,
                    bindings,
                    proposed_ms,
                } => {
                    let response = Self::apply_deploy(
                        engine,
                        runtimes,
                        &entry.log_id,
                        *proposed_ms,
                        Deployment {
                            db,
                            module,
                            wasm,
                            manifest,
                            identity,
                            bindings,
                        },
                    )?;
                    let revision = response.revision;
                    return notified(response, vec![(db.clone(), revision, (0, 0))]);
                }
                // A removal's watchers find its code gone.
                ClusterWrite::Remove {
                    db,
                    module,
                    purge,
                    proposed_ms,
                } => {
                    let response = Self::apply_removal(
                        engine,
                        &entry.log_id,
                        db,
                        module,
                        *purge,
                        *proposed_ms,
                    )?;
                    let revision = response.revision;
                    return notified(response, vec![(db.clone(), revision, (0, 0))]);
                }
                // No revision moves; the caller republishes the
                // incarnation.
                ClusterWrite::Incarnation {
                    incarnation,
                    proposed_ms,
                } => Self::apply_incarnation(engine, &entry.log_id, *incarnation, *proposed_ms)?,
                ClusterWrite::Batch {
                    items,
                    follows,
                    writer,
                    seq,
                    proposed_ms,
                } => {
                    let (response, changes) = Self::apply_batch(
                        engine,
                        runtimes,
                        &entry.log_id,
                        items,
                        Chain {
                            follows: *follows,
                            writer: *writer,
                            seq: *seq,
                            index: entry.log_id.index,
                        },
                        *proposed_ms,
                    )?;
                    return notified(response, changes);
                }
                // No revision moves; the caller publishes the catalog
                // and its own notice.
                ClusterWrite::Keys {
                    db,
                    change,
                    proposed_ms,
                } => Self::apply_keys(engine, &entry.log_id, db, change, *proposed_ms)?,
            },
        };
        Ok((response, Vec::new()))
    }

    /// Apply `entries` in order through `each`, which answers an entry's
    /// response and the records it changed, summed into `changed`.
    #[allow(clippy::type_complexity)]
    fn apply_entries(
        engine: &mut Engine,
        entries: &[openraft::Entry<TypeConfig>],
        changed: &mut u64,
        mut each: impl FnMut(
            &mut Engine,
            &openraft::Entry<TypeConfig>,
        ) -> Result<(WriteResponse, u64), StorageError<NodeId>>,
    ) -> Result<Vec<WriteResponse>, StorageError<NodeId>> {
        let mut responses = Vec::with_capacity(entries.len());
        for entry in entries {
            let (response, records) = each(engine, entry)?;
            *changed += records;
            responses.push(response);
        }
        Ok(responses)
    }

    /// Apply a change to database `db`'s managed-key catalog beside the
    /// group position. A rejected change (a moved revision, a reused
    /// request ID, a database that does not serve here) records only
    /// the position and answers with the rejection. Either way the entry
    /// becomes the group head, like every data entry: a batch the writer
    /// prepared before it signed, sealed or verified against the catalog
    /// it replaces, so it goes stale and is prepared again. And either
    /// way the database's derived values that resolved a key drop, as on
    /// devices, which cannot tell a rejected change from an applied one;
    /// the caller holds a key change across this and the catalog's
    /// publish ([`Engine::begin_key_change`]).
    fn apply_keys(
        engine: &Engine,
        id: &LogId<NodeId>,
        db: &[u8],
        change: &[u8],
        proposed_ms: u64,
    ) -> Result<WriteResponse, StorageError<NodeId>> {
        let write = engine.store().begin_write().map_err(fail_map)?;
        Head::Entry(id.index).record(&write).map_err(fail_map)?;
        advance_expiry_clock(&write, proposed_ms)?;
        engine.drop_key_readers(&write, db).map_err(fail_map)?;
        let outcome = Self::apply_key_change(&write, db, change).map_err(fail_write)?;
        record_applied(&write, id)?;
        write.commit().map_err(fail_map)?;
        Ok(match outcome {
            Ok(response) => WriteResponse::value(
                serde_json::to_vec(&response).map_err(ser_write)?,
                0,
                id.index,
            ),
            Err(message) => WriteResponse::error(message, 0, id.index),
        })
    }

    /// Apply a batch entry: check its head, then every item in order in
    /// one transaction with the group position (see
    /// [`StateMachine::apply_batch_in`]). A stale batch commits as
    /// applied-only, every item rejected with [`STALE_BATCH`], and moves
    /// no database. Also each database an item moved, with its revision
    /// and the records the item wrote there, in item order: each item's
    /// databases, as its own entry would notify them.
    #[allow(clippy::type_complexity)]
    fn apply_batch(
        engine: &mut Engine,
        runtimes: &Arc<dyn RuntimeFactory>,
        id: &LogId<NodeId>,
        items: &[BatchItem],
        chain: Chain,
        proposed_ms: u64,
    ) -> Result<(WriteResponse, Vec<(Vec<u8>, u64, (u64, u64))>), StorageError<NodeId>> {
        // A move's activation commits its database's rows as active and
        // loads their code together, readers fenced out between: none
        // pairs the new placement with no modules (see Engine::exclusive).
        let activates = items.iter().any(BatchItem::activates);
        let commit = |engine: &mut Engine| {
            let staged = engine.store().begin_staged().map_err(fail_map)?;
            let applied =
                Self::apply_batch_in(engine, &staged, items, chain, id.index, proposed_ms)
                    .map_err(fail_write)?;
            record_applied(staged.write(), id)?;
            staged.commit().map_err(fail_map)?;
            if let Some(applied) = &applied {
                for db in Self::finish_items(engine, applied) {
                    Self::reload_db(engine, runtimes, &db)?;
                }
            }
            Ok::<_, StorageError<NodeId>>(applied)
        };
        let applied = if activates {
            engine.exclusive(commit)?
        } else {
            commit(engine)?
        };
        let mut response = WriteResponse::value(b"null".to_vec(), 0, id.index);
        let mut changes = Vec::new();
        response.items = match applied {
            None => {
                let message = format!(
                    "{STALE_BATCH}: the batch was prepared after {:?}, but another entry applied \
                     since",
                    chain.follows
                );
                let stale = WriteResponse::error(message, 0, id.index);
                response.outcome = stale.outcome.clone();
                vec![stale; items.len()]
            }
            Some(applied) => {
                changes.extend(applied.iter().flat_map(Applied::changes));
                applied
                    .into_iter()
                    .map(|item| item_response(item, id.index))
                    .collect::<Result<_, _>>()?
            }
        };
        Ok((response, changes))
    }

    /// Apply a batch entry's items into `staged`, as every member, device
    /// and restore replay does: advance the expiry clock past its stamp,
    /// then, if the group head is the one the batch follows, commit
    /// every item in order, register their databases and record the
    /// batch as the head; otherwise apply nothing but the head, which
    /// becomes this entry. `None` means stale. The caller records the
    /// group position and commits.
    pub fn apply_batch_in(
        engine: &Engine,
        staged: &StagedWrite,
        items: &[BatchItem],
        chain: Chain,
        position: u64,
        proposed_ms: u64,
    ) -> Result<Option<Vec<Applied>>, Failure> {
        let clock = StateMachine::advance_expiry_clock(staged.write(), proposed_ms)?;
        if Head::read(&staged.read()).map_err(Failure::from)? != chain.follows {
            Head::Entry(chain.index)
                .record(staged.write())
                .map_err(Failure::from)?;
            return Ok(None);
        }
        let applied = Self::stage_items(engine, staged, items, position, clock)?;
        Head::Batch {
            writer: chain.writer,
            seq: chain.seq,
        }
        .record(staged.write())
        .map_err(Failure::from)?;
        Ok(Some(applied))
    }

    /// Commit batch items into `staged` at expiry clock `clock` and
    /// register their databases, without checking or moving the head:
    /// the writer stages writes still in flight this way to prepare the
    /// next ones on top.
    pub(crate) fn stage_items(
        engine: &Engine,
        staged: &StagedWrite,
        items: &[BatchItem],
        position: u64,
        clock: u64,
    ) -> Result<Vec<Applied>, Failure> {
        let applied = engine.commit_batch(staged, items, position, clock)?;
        for item in &applied {
            flower_engine::replication::register_applied(staged.write(), item)
                .map_err(Failure::from)?;
        }
        Ok(applied)
    }

    /// Finish what committed batch items need beyond their rows, outside
    /// the transaction: a database whose rows a placement change dropped
    /// leaves the engine's memory (modules, cached outcomes), and one a
    /// move activated is answered for the caller to build its modules
    /// from its code rows (each applier has its own runtimes). A crash
    /// before this changes nothing: a restart recovers modules from the
    /// committed rows alone.
    pub fn finish_items(engine: &mut Engine, applied: &[Applied]) -> Vec<Vec<u8>> {
        let mut activated = Vec::new();
        for item in applied {
            match item {
                Applied::Placement {
                    db,
                    effect: PlacementEffect::Dropped,
                    ..
                } => engine.forget_database(db),
                Applied::Placement {
                    db,
                    effect: PlacementEffect::Activated,
                    ..
                } => {
                    engine.set_replicated(db);
                    activated.push(db.clone());
                }
                _ => {}
            }
        }
        activated
    }

    /// Apply a patch entry: check the journal, commit it beside the
    /// group position and register its database, all atomically. The
    /// entry's database must match the journal's.
    #[allow(clippy::type_complexity)]
    fn apply_patch(
        engine: &mut Engine,
        db: &[u8],
        patch: &[u8],
        id: &LogId<NodeId>,
        proposed_ms: u64,
    ) -> Result<(WriteResponse, Vec<(Vec<u8>, u64, (u64, u64))>), StorageError<NodeId>> {
        let checked = engine.check_patch(patch, id.index).map_err(fail_write)?;
        if checked.patch.db.as_slice() != db {
            return Err(poison(format!(
                "patch entry targets {:?} but the journal targets {:?}",
                db, checked.patch.db,
            )));
        }
        let write = engine.store().begin_write().map_err(fail_map)?;
        Head::Entry(id.index).record(&write).map_err(fail_map)?;
        let clock = advance_expiry_clock(&write, proposed_ms)?;
        let outcome: ApplyOutcome = engine
            .commit_patch(&write, &checked, id.index, clock)
            .map_err(fail_write)?;
        record_applied(&write, id)?;
        register_db(&write, db)?;
        write.commit().map_err(fail_map)?;
        let changes = vec![(
            db.to_vec(),
            outcome.revision,
            checked.records(outcome.revision),
        )];
        if let Some(rejected) = &checked.rejected {
            let response = WriteResponse::error(
                format!("{}: {}", rejected.code, rejected.message),
                outcome.revision,
                id.index,
            );
            return Ok((response, changes));
        }
        Ok((response_from(outcome, id.index)?, changes))
    }

    /// Apply a multi-database entry: check every participant, then
    /// commit all journals beside the group position and register
    /// every participant database, atomically. Rejection commits the
    /// batch as applied-only with the first rejection as the error.
    #[allow(clippy::type_complexity)]
    fn apply_multi(
        engine: &mut Engine,
        patches: &[u8],
        id: &LogId<NodeId>,
        proposed_ms: u64,
    ) -> Result<(WriteResponse, Vec<(Vec<u8>, u64, (u64, u64))>), StorageError<NodeId>> {
        let checked = engine
            .check_multi_patch(patches, id.index)
            .map_err(fail_write)?;
        let write = engine.store().begin_write().map_err(fail_map)?;
        Head::Entry(id.index).record(&write).map_err(fail_map)?;
        let clock = advance_expiry_clock(&write, proposed_ms)?;
        let outcome = engine
            .commit_multi(&write, &checked, id.index, clock)
            .map_err(fail_write)?;
        record_applied(&write, id)?;
        for (db, _) in &outcome.revisions {
            register_db(&write, db)?;
        }
        write.commit().map_err(fail_map)?;
        let changes = outcome
            .revisions
            .iter()
            .cloned()
            .zip(checked.records(&outcome.revisions))
            .map(|((db, revision), records)| (db, revision, records))
            .collect();
        if let Some(rejected) = &checked.rejected {
            let revision = outcome.revisions.first().map(|(_, rev)| *rev).unwrap_or(0);
            let mut response = WriteResponse::error(
                format!("{}: {}", rejected.code, rejected.message),
                revision,
                id.index,
            );
            response.revisions = outcome.revisions;
            return Ok((response, changes));
        }
        Ok((response_multi_from(outcome, id.index)?, changes))
    }

    /// Apply a deployment entry: activate the staged bundle unless the
    /// active identity already matches, then record the group
    /// position. The identity skip makes redelivery safe: activation
    /// commits atomically, so a matching identity means its effects
    /// are fully present.
    fn apply_deploy(
        engine: &mut Engine,
        runtimes: &Arc<dyn RuntimeFactory>,
        id: &LogId<NodeId>,
        proposed_ms: u64,
        deployment: Deployment<'_>,
    ) -> Result<WriteResponse, StorageError<NodeId>> {
        let Deployment {
            db,
            module,
            wasm,
            manifest,
            identity,
            bindings,
        } = deployment;
        let read = engine.store().begin_read().map_err(fail_map_read)?;
        let active = read
            .meta(db, &code_identity_key(module))
            .map_err(fail_map_read)?;
        // A database that does not serve here, or that a prepared
        // transaction holds, takes no deployment, nor does a module whose
        // data, or a queue it declares, is being purged: the entry
        // records only its position, on every applier alike.
        let refused = match flower_engine::txn::unwritable(&read, db).map_err(fail_map_read)? {
            Some(refusal) => Some(refusal),
            None => flower_engine::purge::activation_refusal(
                &read,
                db,
                module,
                &parse_value(manifest).map_err(fail_map)?,
            )
            .map_err(fail_map_read)?,
        };
        drop(read);
        if let Some(refusal) = refused {
            let write = engine.store().begin_write().map_err(fail_map)?;
            Head::Entry(id.index).record(&write).map_err(fail_map)?;
            advance_expiry_clock(&write, proposed_ms)?;
            record_applied(&write, id)?;
            write.commit().map_err(fail_map)?;
            return Ok(WriteResponse::error(
                format!("{}: {}", refusal.code, refusal.message),
                0,
                id.index,
            ));
        }
        let skip = matches!(active, Some(bytes) if bytes.as_slice() == identity.as_slice());
        if skip {
            // The active code again activates nothing, but closes the
            // module's open staging as an activation would, on every
            // member alike: one of this code has nothing left to bring,
            // and one of other code is superseded.
            engine.cancel_open_staging(db, module).map_err(fail_write)?;
        } else {
            let (manifest, runtime) = build_runtime(runtimes, db, module, wasm, manifest)?;
            let activation = Activation {
                identity: *identity,
                bindings: bindings.to_vec(),
            };
            engine
                .apply_activation(db, module, wasm, &manifest, &activation, runtime)
                .map_err(fail_write)?;
        }
        engine.set_replicated(db);
        let write = engine.store().begin_write().map_err(fail_map)?;
        Head::Entry(id.index).record(&write).map_err(fail_map)?;
        advance_expiry_clock(&write, proposed_ms)?;
        record_applied(&write, id)?;
        register_db(&write, db)?;
        write.commit().map_err(fail_map)?;
        let read = engine.store().begin_read().map_err(fail_map_read)?;
        let revision = read.meta_u64(db, meta::REVISION).map_err(fail_map_read)?;
        let body = format!("{{\"activated\":{module:?}}}").into_bytes();
        Ok(WriteResponse::value(body, revision, id.index))
    }

    /// Apply a module removal entry (see
    /// [`Engine::commit_removal`]): the removal, or its deterministic
    /// refusal, commits with the group head, the expiry clock and the
    /// group position in one transaction; an applied one registers its
    /// database, whose maintenance purges it.
    fn apply_removal(
        engine: &mut Engine,
        id: &LogId<NodeId>,
        db: &[u8],
        module: &str,
        purge: bool,
        proposed_ms: u64,
    ) -> Result<WriteResponse, StorageError<NodeId>> {
        let staged = engine.store().begin_staged().map_err(fail_map)?;
        Head::Entry(id.index)
            .record(staged.write())
            .map_err(fail_map)?;
        advance_expiry_clock(staged.write(), proposed_ms)?;
        record_applied(staged.write(), id)?;
        // An applied removal's database is registered: its purge runs
        // from the registry. A refused one registers nothing.
        let refused = engine
            .removal_refusal(&staged.read(), db, module, purge)
            .map_err(fail_write)?
            .is_some();
        if !refused {
            register_db(staged.write(), db)?;
        }
        let outcome = engine
            .commit_removal(staged, db, module, purge)
            .map_err(fail_write)?;
        let read = engine.store().begin_read().map_err(fail_map_read)?;
        let revision = read.meta_u64(db, meta::REVISION).map_err(fail_map_read)?;
        Ok(match outcome {
            Ok(value) => {
                WriteResponse::value(render(&value).map_err(fail_map)?, revision, id.index)
            }
            Err(refusal) => WriteResponse::error(
                format!("{}: {}", refusal.code, refusal.message),
                revision,
                id.index,
            ),
        })
    }

    /// Apply an incarnation entry. While the group has none, the
    /// entry's value becomes the group incarnation and labels every
    /// registered database; once one is applied, later entries (a
    /// racing leader's proposal) change nothing, so every member keeps
    /// the first. Either way the entry advances the expiry clock and
    /// records the group position in the same transaction.
    fn apply_incarnation(
        engine: &Engine,
        id: &LogId<NodeId>,
        incarnation: u64,
        proposed_ms: u64,
    ) -> Result<WriteResponse, StorageError<NodeId>> {
        let write = engine.store().begin_write().map_err(fail_map)?;
        Head::Entry(id.index).record(&write).map_err(fail_map)?;
        advance_expiry_clock(&write, proposed_ms)?;
        let applied =
            flower_engine::replication::establish(&write, incarnation).map_err(fail_map)?;
        record_applied(&write, id)?;
        write.commit().map_err(fail_map)?;
        // Hex text: canonical JSON numbers cannot carry every u64.
        let body = format!("{{\"incarnation\":\"{applied:016x}\"}}").into_bytes();
        Ok(WriteResponse::value(body, 0, id.index))
    }

    /// Delete unscoped idempotency receipts expired at this member's
    /// expiry clock, at most `limit` rows per database, returning rows
    /// deleted. Local hygiene that needs no consensus: every entry
    /// applied later reads expiry at a clock at least this large (see
    /// the module docs), so a deleted receipt was already a miss for
    /// every future entry on every member, deleted or not.
    pub fn purge_expired_receipts(engine: &mut Engine, limit: usize) -> Result<usize, Failure> {
        let read = engine.store().begin_read().map_err(Failure::from)?;
        let clock = read
            .meta_u64(CLUSTER_DB, GROUP_EXPIRY_CLOCK)
            .map_err(Failure::from)?;
        let dbs = registered(&read).map_err(Failure::from)?;
        drop(read);
        let mut deleted = 0;
        for db in &dbs {
            deleted += engine.purge_expired_idempotency(db, clock, limit)?;
        }
        Ok(deleted)
    }

    /// Reload every deployed module's runtime from stored code after a
    /// snapshot install replaces the database rows.
    fn reload_db(
        engine: &mut Engine,
        runtimes: &Arc<dyn RuntimeFactory>,
        db: &[u8],
    ) -> Result<(), StorageError<NodeId>> {
        let read = engine.store().begin_read().map_err(fail_map_read)?;
        if !flower_engine::placement::modules_ready(&read, db).map_err(fail_map_read)? {
            return Ok(());
        }
        let rows = read
            .scan_meta_prefix(db, CODE_IDENTITY_PREFIX)
            .map_err(fail_map_read)?;
        let mut modules = Vec::with_capacity(rows.len());
        for (name, identity) in &rows {
            let module = std::str::from_utf8(&name[CODE_IDENTITY_PREFIX.len()..])
                .map_err(read_err)?
                .to_string();
            let hash: [u8; 32] = identity.as_slice().try_into().map_err(|_| {
                poison(format!(
                    "code identity for `{module}` has {} bytes, want 32",
                    identity.len()
                ))
            })?;
            let Some((wasm, manifest)) = read.get_code(db, &hash).map_err(fail_map_read)? else {
                return Err(poison(format!("code missing for deployed `{module}`")));
            };
            modules.push((module, wasm, manifest));
        }
        drop(read);
        for (module, wasm, manifest) in &modules {
            let (_, runtime) = build_runtime(runtimes, db, module, wasm, manifest)?;
            engine
                .reload_module(db, module, runtime)
                .map_err(fail_write)?;
        }
        Ok(())
    }

    /// Install a snapshot file held in memory (see
    /// [`StateMachine::install_image`]).
    #[cfg(test)]
    fn install_file(
        engine: &mut Engine,
        runtimes: &Arc<dyn RuntimeFactory>,
        bytes: &[u8],
    ) -> Result<(), StorageError<NodeId>> {
        let header = read_header(bytes).map_err(read_err)?;
        engine.exclusive(|engine| Self::install_views(engine, runtimes, &header, || Ok(bytes)))
    }

    /// Install an image into the engine: the snapshot replaces the whole
    /// group state. Registered databases the snapshot lacks are wiped
    /// first, then every database view installs as it streams from the
    /// file, marked replicated with its runtimes reloaded, then the
    /// group position, membership and registry commit in one
    /// transaction. Reinstalling the same image is safe: views at the
    /// same position replace themselves, and the wipe runs while the
    /// local registry (part of the `$cluster` view) still lists what it
    /// must remove. Readers are fenced out for the whole install
    /// ([`Engine::exclusive`]): none pairs installed rows with the code
    /// they replaced. The header was read and checked first.
    fn install_image(
        engine: &mut Engine,
        runtimes: &Arc<dyn RuntimeFactory>,
        path: &Path,
        header: &FileHeader,
    ) -> Result<(), StorageError<NodeId>> {
        let open = || Ok(BufReader::with_capacity(IMAGE_BUFFER, File::open(path)?));
        engine.exclusive(|engine| Self::install_views(engine, runtimes, header, open))
    }

    /// The install's body, inside the fence: wipe what the header's
    /// database list lacks, stream every view in, then record the group
    /// position, membership and registry.
    fn install_views<R: Read>(
        engine: &mut Engine,
        runtimes: &Arc<dyn RuntimeFactory>,
        header: &FileHeader,
        open: impl FnOnce() -> std::io::Result<R>,
    ) -> Result<(), StorageError<NodeId>> {
        let read = engine.store().begin_read().map_err(fail_map_read)?;
        let local = registered(&read).map_err(fail_map_read)?;
        drop(read);
        for db in &local {
            if header.dbs.iter().any(|meta| &meta.db == db) {
                continue;
            }
            let write = engine.store().begin_write().map_err(fail_map)?;
            write.clear_database(db).map_err(fail_map)?;
            write.commit().map_err(fail_map)?;
            engine.forget_database(db);
        }
        let mut installed = 0;
        let streamed = for_each_view(open().map_err(read_err)?, |view| {
            if header.dbs.get(installed).map(|meta| &meta.db) != Some(&view.meta.db) {
                return Err(poison("snapshot views changed under the install".into()));
            }
            installed += 1;
            Self::install_view(engine, runtimes, view)
        });
        match streamed {
            Ok(()) => {}
            Err(ViewsError::Install(error)) => return Err(error),
            Err(ViewsError::Decode(error)) => return Err(ser_read(error)),
        }
        if installed != header.dbs.len() {
            return Err(poison("snapshot views changed under the install".into()));
        }
        let write = engine.store().begin_write().map_err(fail_map)?;
        if let Some(applied) = &header.applied {
            record_applied(&write, applied)?;
        }
        record_membership(&write, &header.membership)?;
        for meta in &header.dbs {
            if meta.db.as_slice() != CLUSTER_DB {
                register_db(&write, &meta.db)?;
            }
        }
        write.commit().map_err(fail_map)?;
        Ok(())
    }

    /// Install one database view: its rows replace the database's in one
    /// transaction, then its modules reload from the installed code.
    fn install_view(
        engine: &mut Engine,
        runtimes: &Arc<dyn RuntimeFactory>,
        view: SyncDbSnapshot,
    ) -> Result<(), StorageError<NodeId>> {
        let fenced = SnapshotMeta {
            db: view.meta.db.clone(),
            revision: view.meta.revision,
            applied_log: view.meta.applied_log,
            incarnation: view.meta.incarnation,
        };
        apply_snapshot(engine.store(), &fenced, &view.payload).map_err(fail_map)?;
        drop(view.payload);
        // Modules the old rows deployed must not outlive them.
        engine.forget_database(&fenced.db);
        if fenced.db.as_slice() != CLUSTER_DB {
            engine.set_replicated(&fenced.db);
        }
        Self::reload_db(engine, runtimes, &fenced.db)
    }

    /// Finish a snapshot install a crash interrupted, before Raft
    /// starts: the engine may hold some databases from the snapshot
    /// and others from before it. Returns whether one was pending.
    pub(crate) fn resume_install(
        engine: &mut Engine,
        runtimes: &Arc<dyn RuntimeFactory>,
        logs: &LogStore,
    ) -> Result<bool, StorageError<NodeId>> {
        let Some(image) = logs.pending_install()? else {
            return Ok(false);
        };
        let header = read_image_header(&image.path, &image.meta.snapshot_id)?;
        Self::install_image(engine, runtimes, &image.path, &header)?;
        logs.finish_install()?;
        Ok(true)
    }

    /// Read the group applied position from `$cluster`, if any.
    /// Backups record it as the covered index.
    pub fn group_applied(engine: &Engine) -> Result<Option<LogId<NodeId>>, Failure> {
        Self::group_applied_in(&engine.store().begin_read().map_err(Failure::from)?)
    }

    /// [`Self::group_applied`] from a caller's pinned read: a backup
    /// reads its whole cut from one.
    pub fn group_applied_in(read: &ReadTxn) -> Result<Option<LogId<NodeId>>, Failure> {
        read_applied(read).map_err(|error| fail("corrupt", error.to_string()))
    }

    /// Read the expiry clock: the largest proposal stamp applied so
    /// far, zero before the first. Backups record it so restore and
    /// PITR replay read receipt expiry exactly as live members did.
    pub fn expiry_clock(engine: &Engine) -> Result<u64, Failure> {
        Self::expiry_clock_in(&engine.store().begin_read().map_err(Failure::from)?)
    }

    /// [`Self::expiry_clock`] from a caller's pinned read.
    pub fn expiry_clock_in(read: &ReadTxn) -> Result<u64, Failure> {
        read.meta_u64(CLUSTER_DB, GROUP_EXPIRY_CLOCK)
            .map_err(Failure::from)
    }

    /// Advance the expiry clock past a proposal stamp inside a caller's
    /// transaction, returning the clock an entry stamped `proposed_ms`
    /// reads receipt expiry at: never earlier than any stamp applied
    /// before it. Restore seeds the clock and PITR replay advances it
    /// entry by entry through here, exactly as live applies do.
    pub fn advance_expiry_clock(write: &WriteTxn, proposed_ms: u64) -> Result<u64, Failure> {
        let clock = write
            .meta_u64(CLUSTER_DB, GROUP_EXPIRY_CLOCK)
            .map_err(Failure::from)?
            .max(proposed_ms);
        write
            .set_meta_u64(CLUSTER_DB, GROUP_EXPIRY_CLOCK, clock)
            .map_err(Failure::from)?;
        Ok(clock)
    }

    /// Read the group's history incarnation, zero until a leader's
    /// proposal applies. Backups and archives record it to keep
    /// histories apart.
    pub fn incarnation(engine: &Engine) -> Result<u64, Failure> {
        Self::incarnation_in(&engine.store().begin_read().map_err(Failure::from)?)
    }

    /// [`Self::incarnation`] from a caller's pinned read.
    pub fn incarnation_in(read: &ReadTxn) -> Result<u64, Failure> {
        read.meta_u64(CLUSTER_DB, meta::HISTORY_INCARNATION)
            .map_err(Failure::from)
    }

    /// Start a new history inside an open transaction. A restored
    /// engine opening a fresh Raft log keeps its data and revisions,
    /// but the new log's positions restart at one and the old
    /// incarnation must not label them: every registered database's
    /// applied position and incarnation reset to zero, and the group
    /// incarnation clears so the new leader proposes a fresh one. The
    /// expiry clock carries over, since it never runs backwards.
    pub fn restart_history(write: &WriteTxn) -> Result<(), Failure> {
        Self::restart_positions(write)?;
        Self::end_history(write)
    }

    /// Reset every registered database's applied position to zero, for
    /// a log whose positions restart at one, keeping its history: a
    /// point-in-time restore replays its archive this way, as the
    /// history whose entries they are (see [`Self::adopt_history`]).
    pub fn restart_positions(write: &WriteTxn) -> Result<(), Failure> {
        for db in registered_in(write).map_err(Failure::from)? {
            write
                .set_meta_u64(&db, meta::APPLIED_LOG, 0)
                .map_err(Failure::from)?;
        }
        Ok(())
    }

    /// Label a restoring group with the history `incarnation` its
    /// archived entries belong to, before any database registers, so
    /// replay applies them as that history's members did: their scoped
    /// identities and sessions are its own, and a move's activation
    /// records its arrival under it.
    pub fn adopt_history(write: &WriteTxn, incarnation: u64) -> Result<(), Failure> {
        write
            .set_meta_u64(CLUSTER_DB, meta::HISTORY_INCARNATION, incarnation)
            .map_err(Failure::from)
    }

    /// End the group's history: every registered database's incarnation
    /// clears, and with it every lineage its moves carried in, and the
    /// group incarnation clears so the new leader proposes a fresh one.
    /// Nothing bound to the old history holds after it.
    pub fn end_history(write: &WriteTxn) -> Result<(), Failure> {
        for db in registered_in(write).map_err(Failure::from)? {
            write
                .set_meta_u64(&db, meta::HISTORY_INCARNATION, 0)
                .map_err(Failure::from)?;
            flower_engine::placement::forget_lineage(write, &db).map_err(Failure::from)?;
        }
        write
            .delete_meta(CLUSTER_DB, meta::HISTORY_INCARNATION)
            .map_err(Failure::from)?;
        Ok(())
    }

    /// The group head: the last data entry applied (see
    /// [`flower_engine::batch`]). Backups record it at their cut, so
    /// replay reaches the batch verdicts live members reached.
    pub fn head(engine: &Engine) -> Result<Head, Failure> {
        Self::head_in(&engine.store().begin_read().map_err(Failure::from)?)
    }

    /// [`Self::head`] from a caller's pinned read.
    pub fn head_in(read: &ReadTxn) -> Result<Head, Failure> {
        Head::read(read).map_err(Failure::from)
    }

    /// Register a database in the replicated set, labelled with the
    /// group incarnation. Restore uses this after installing views;
    /// live applies register through entries.
    pub fn register_database(store: &GroupStore, db: &[u8]) -> Result<(), Failure> {
        let write = store.begin_write().map_err(Failure::from)?;
        register_in(&write, db).map_err(Failure::from)?;
        write.commit().map_err(Failure::from)?;
        Ok(())
    }

    /// List databases registered as replicated in `$cluster`.
    /// Backup and restore enumerate the set through here.
    pub fn replicated_databases(engine: &Engine) -> Result<Vec<Vec<u8>>, Failure> {
        Self::replicated_databases_in(&engine.store().begin_read().map_err(Failure::from)?)
    }

    /// [`Self::replicated_databases`] from a caller's pinned read.
    pub fn replicated_databases_in(read: &ReadTxn) -> Result<Vec<Vec<u8>>, Failure> {
        registered(read).map_err(Failure::from)
    }

    /// Recover a reopened engine: mark registered databases
    /// replicated and reload every module's runtime from stored
    /// code. Fresh nodes recover to an empty registry.
    pub(crate) fn recover(
        engine: &mut Engine,
        runtimes: &Arc<dyn RuntimeFactory>,
    ) -> Result<(), String> {
        let dbs = Self::replicated_databases(engine)
            .map_err(|failure| format!("{}: {}", failure.code, failure.message))?;
        for db in &dbs {
            engine.set_replicated(db);
            Self::reload_db(engine, runtimes, db).map_err(|error| error.to_string())?;
        }
        Ok(())
    }
}

/// Where a batch entry belongs: the head it was prepared after and its
/// own identity, which becomes the head when it applies.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Chain {
    /// The data entry the writer prepared the batch after.
    pub follows: Head,
    /// The proposing writer's identity.
    pub writer: u64,
    /// The batch's sequence within its writer.
    pub seq: u64,
    /// The entry's log index in its history: a stale batch records it
    /// as the head. Live applies commit at it too; restore replay
    /// commits at fresh positions but keeps the history's indexes here.
    pub index: u64,
}

/// One deployment entry's fields.
struct Deployment<'a> {
    db: &'a [u8],
    module: &'a str,
    wasm: &'a [u8],
    manifest: &'a [u8],
    identity: &'a [u8; 32],
    bindings: &'a [(Vec<u8>, u32)],
}

/// Advance the expiry clock past an entry's proposal stamp inside an
/// open transaction, returning the clock the entry reads receipt
/// expiry at (see [`StateMachine::advance_expiry_clock`]).
fn advance_expiry_clock(write: &WriteTxn, proposed_ms: u64) -> Result<u64, StorageError<NodeId>> {
    StateMachine::advance_expiry_clock(write, proposed_ms).map_err(fail_write)
}

/// Parse a stored manifest and build the runtime of database `db`'s
/// module.
fn build_runtime(
    runtimes: &Arc<dyn RuntimeFactory>,
    db: &[u8],
    module: &str,
    wasm: &[u8],
    manifest: &[u8],
) -> Result<(Value, Box<dyn Runtime>), StorageError<NodeId>> {
    let manifest = parse_value(manifest).map_err(fail_map)?;
    let runtime = runtimes
        .build(db, module, wasm, &manifest)
        .map_err(poison)?;
    Ok((manifest, runtime))
}

/// What operators read of the group state in one snapshot: the applied
/// log index and the replicated expiry clock.
pub(crate) struct GroupReading {
    /// The last applied log index, if any.
    pub(crate) applied: Option<u64>,
    /// The replicated receipt expiry clock, in ms since the epoch.
    pub(crate) expiry_clock_ms: u64,
}

/// Read [`GroupReading`] from `read`.
pub(crate) fn group_reading(read: &ReadTxn) -> Result<GroupReading, Failure> {
    let applied = read
        .meta(CLUSTER_DB, GROUP_APPLIED)
        .map_err(Failure::from)?
        .map(|bytes| serde_json::from_slice::<LogId<NodeId>>(&bytes))
        .transpose()
        .map_err(|error| Failure::new("host_storage", error.to_string()))?
        .map(|id| id.index);
    Ok(GroupReading {
        applied,
        expiry_clock_ms: read
            .meta_u64(CLUSTER_DB, GROUP_EXPIRY_CLOCK)
            .map_err(Failure::from)?,
    })
}

/// Databases registered as replicated, in registry order.
pub(crate) fn registered(read: &ReadTxn) -> flower_engine::error::Result<Vec<Vec<u8>>> {
    Ok(registry_dbs(
        read.scan_meta_prefix(CLUSTER_DB, GROUP_DB_PREFIX)?,
    ))
}

/// Databases registered as replicated inside an open transaction.
fn registered_in(write: &WriteTxn) -> flower_engine::error::Result<Vec<Vec<u8>>> {
    Ok(registry_dbs(
        write.scan_meta_prefix(CLUSTER_DB, GROUP_DB_PREFIX)?,
    ))
}

/// Database names from registry rows.
fn registry_dbs(rows: Vec<(Vec<u8>, Vec<u8>)>) -> Vec<Vec<u8>> {
    rows.into_iter()
        .map(|(name, _)| name[GROUP_DB_PREFIX.len()..].to_vec())
        .collect()
}

/// Read the group applied log ID, if any.
fn read_applied(read: &ReadTxn) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
    match read
        .meta(CLUSTER_DB, GROUP_APPLIED)
        .map_err(fail_map_read)?
    {
        Some(bytes) => Ok(Some(serde_json::from_slice(&bytes).map_err(ser_read)?)),
        None => Ok(None),
    }
}

/// Read the applied group incarnation on the apply path.
fn applied_incarnation(engine: &Engine) -> Result<u64, StorageError<NodeId>> {
    StateMachine::incarnation(engine).map_err(fail_read)
}

/// Read the last applied membership, empty before the first.
fn read_membership(
    read: &ReadTxn,
) -> Result<StoredMembership<NodeId, BasicNode>, StorageError<NodeId>> {
    match read
        .meta(CLUSTER_DB, GROUP_MEMBERSHIP)
        .map_err(fail_map_read)?
    {
        Some(bytes) => serde_json::from_slice(&bytes).map_err(ser_read),
        None => Ok(StoredMembership::new(None, Membership::default())),
    }
}

/// Map an applied patch outcome to its client response.
fn response_from(outcome: ApplyOutcome, index: u64) -> Result<WriteResponse, StorageError<NodeId>> {
    let ApplyOutcome {
        result,
        revision,
        replayed_position,
        duplicate,
    } = outcome;
    // A duplicate answers where its original committed.
    let position = replayed_position.unwrap_or(index);
    let mut response = match result {
        Ok(value) => {
            let bytes = render(&value).map_err(fail_map)?;
            WriteResponse::value(bytes, revision, position)
        }
        Err(failure) => {
            let bytes = render(&encode_err(&failure)).map_err(fail_map)?;
            WriteResponse::failure(bytes, revision, position)
        }
    };
    response.duplicate = duplicate;
    Ok(response)
}

/// Whether a batch item's response says its whole batch was stale.
pub(crate) fn is_stale_batch(response: &WriteResponse) -> bool {
    matches!(
        &response.outcome,
        ResponseOutcome::Error(message) if message.starts_with(STALE_BATCH)
    )
}

/// Map one applied batch item to its response, as its own entry would
/// have answered.
fn item_response(item: Applied, index: u64) -> Result<WriteResponse, StorageError<NodeId>> {
    match item {
        Applied::Patch { checked, outcome } => match &checked.rejected {
            Some(rejected) => Ok(WriteResponse::error(
                format!("{}: {}", rejected.code, rejected.message),
                outcome.revision,
                index,
            )),
            None => response_from(outcome, index),
        },
        Applied::Multi { checked, outcome } => match &checked.rejected {
            Some(rejected) => {
                let revision = outcome.revisions.first().map(|(_, rev)| *rev).unwrap_or(0);
                let mut response = WriteResponse::error(
                    format!("{}: {}", rejected.code, rejected.message),
                    revision,
                    index,
                );
                response.revisions = outcome.revisions;
                Ok(response)
            }
            None => response_multi_from(outcome, index),
        },
        Applied::Retention {
            result, revision, ..
        }
        | Applied::Purge {
            result, revision, ..
        }
        | Applied::Staging {
            result, revision, ..
        }
        | Applied::Catalog { result, revision }
        | Applied::Placement {
            result, revision, ..
        } => match result {
            Ok(value) => {
                let bytes = render(&value).map_err(fail_map)?;
                Ok(WriteResponse::value(bytes, revision, index))
            }
            Err(failure) => Ok(WriteResponse::error(
                format!("{}: {}", failure.code, failure.message),
                revision,
                index,
            )),
        },
        Applied::Txn {
            result, revisions, ..
        } => {
            let mut response = match result {
                Ok(value) => WriteResponse::value(render(&value).map_err(fail_map)?, 0, index),
                Err(failure) => {
                    WriteResponse::error(format!("{}: {}", failure.code, failure.message), 0, index)
                }
            };
            response.revisions = revisions;
            Ok(response)
        }
    }
}

/// Map a committed multi-database outcome to its response: the
/// combined step results plus the first participant's revision and
/// the full per-participant revision list.
fn response_multi_from(
    outcome: MultiApplyOutcome,
    index: u64,
) -> Result<WriteResponse, StorageError<NodeId>> {
    let revision = outcome.revisions.first().map(|(_, rev)| *rev).unwrap_or(0);
    let index = outcome.replayed_position.unwrap_or(index);
    let mut response = match outcome.combined() {
        Ok(value) => {
            let bytes = render(&value).map_err(fail_map)?;
            WriteResponse::value(bytes, revision, index)
        }
        Err(failure) => {
            let bytes = render(&encode_err(&failure)).map_err(fail_map)?;
            WriteResponse::failure(bytes, revision, index)
        }
    };
    response.revisions = outcome.revisions;
    response.duplicate = outcome.duplicate;
    Ok(response)
}

/// Record the group applied position inside an open transaction.
fn record_applied(write: &WriteTxn, id: &LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
    let bytes = serde_json::to_vec(id).map_err(ser_write)?;
    write
        .set_meta(CLUSTER_DB, GROUP_APPLIED, &bytes)
        .map_err(fail_map)?;
    Ok(())
}

/// Record the applied membership inside an open transaction.
fn record_membership(
    write: &WriteTxn,
    membership: &StoredMembership<NodeId, BasicNode>,
) -> Result<(), StorageError<NodeId>> {
    let bytes = serde_json::to_vec(membership).map_err(ser_write)?;
    write
        .set_meta(CLUSTER_DB, GROUP_MEMBERSHIP, &bytes)
        .map_err(fail_map)?;
    Ok(())
}

/// Register a database in the replicated set inside an open apply
/// transaction (see [`register_in`]).
fn register_db(write: &WriteTxn, db: &[u8]) -> Result<(), StorageError<NodeId>> {
    register_in(write, db).map_err(fail_map)
}

/// Register a database in the replicated set inside an open
/// transaction and label it with the group incarnation, so a database
/// registered after the incarnation applied joins the same history.
/// Idempotent; the label is rewritten only when it differs.
fn register_in(write: &WriteTxn, db: &[u8]) -> flower_engine::error::Result<()> {
    flower_engine::replication::register(write, db)
}

/// Wrap an engine failure on the read path.
fn fail_read(failure: Failure) -> StorageError<NodeId> {
    let io = std::io::Error::other(format!("{}: {}", failure.code, failure.message));
    StorageIOError::read_state_machine(&io).into()
}

/// Wrap an engine failure on the write path.
fn fail_write(failure: Failure) -> StorageError<NodeId> {
    let io = std::io::Error::other(format!("{}: {}", failure.code, failure.message));
    StorageIOError::write_state_machine(&io).into()
}

/// Wrap an engine-typed error on the write path.
fn fail_map<E>(error: E) -> StorageError<NodeId>
where
    Failure: From<E>,
{
    fail_write(Failure::from(error))
}

/// Wrap an engine-typed error on the read path.
fn fail_map_read<E>(error: E) -> StorageError<NodeId>
where
    Failure: From<E>,
{
    fail_read(Failure::from(error))
}

/// Wrap a JSON decode failure.
fn ser_read(error: serde_json::Error) -> StorageError<NodeId> {
    StorageIOError::read_state_machine(&error).into()
}

/// Wrap a JSON encode failure.
fn ser_write(error: serde_json::Error) -> StorageError<NodeId> {
    StorageIOError::write_state_machine(&error).into()
}

/// Wrap a standard error on the read path.
fn read_err<E: std::error::Error + 'static>(error: E) -> StorageError<NodeId> {
    StorageIOError::read_state_machine(&error).into()
}

/// Wrap a standard error on the write path.
fn write_err<E: std::error::Error + 'static>(error: E) -> StorageError<NodeId> {
    StorageIOError::write_state_machine(&error).into()
}

/// Halt on a corrupt entry or snapshot with a message.
fn poison(message: String) -> StorageError<NodeId> {
    let io = std::io::Error::other(message);
    StorageIOError::write_state_machine(&io).into()
}

/// Run state-machine work on the blocking pool; a panic is a write
/// failure that halts the node.
async fn blocking<T, F>(
    engine: &Arc<AsyncMutex<Engine>>,
    work: F,
) -> Result<T, StorageError<NodeId>>
where
    T: Send + 'static,
    F: FnOnce(&mut Engine) -> Result<T, StorageError<NodeId>> + Send + 'static,
{
    with_engine(engine, work).await.map_err(poison)?
}

impl RaftStateMachine<TypeConfig> for StateMachine {
    type SnapshotBuilder = SnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>), StorageError<NodeId>>
    {
        let (applied, membership, incarnation, catalogs) = blocking(&self.engine, |engine| {
            let read = engine.store().begin_read().map_err(fail_map_read)?;
            let incarnation = read
                .meta_u64(CLUSTER_DB, meta::HISTORY_INCARNATION)
                .map_err(fail_map_read)?;
            let catalogs = Self::key_catalogs(&read).map_err(fail_read)?;
            Ok((
                read_applied(&read)?,
                read_membership(&read)?,
                incarnation,
                catalogs,
            ))
        })
        .await?;
        self.incarnation.store(incarnation, Ordering::Release);
        self.keys.publish_all(catalogs);
        self.logs.applied_membership(membership.membership());
        Ok((applied, membership))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<WriteResponse>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = openraft::Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries: Vec<openraft::Entry<TypeConfig>> = entries.into_iter().collect();
        let touches_catalog = entries.iter().any(changes_catalog);
        let runtimes = self.runtimes.clone();
        let commits = self.commits.clone();
        let published = self.incarnation.clone();
        let accounting = self.accounting.clone();
        let keys = self.keys.clone();
        let logs = self.logs.clone();
        let durability = self.durability.clone();
        let marks = self.logs.commit_marks();
        let last = entries.last().map(|entry| entry.log_id.index);
        // Committed by a quorum: application begins (`Server-Timing`'s
        // `replicate` ends here, `apply` starts).
        if let Some(last) = last {
            marks.applying(last);
        }
        let mut trace = self.logs.observer().storage(StorageOperation::Apply);
        if trace.on() {
            trace.size(StorageBatch::Entries, entries.len() as u64);
            trace.size(
                StorageBatch::Commands,
                entries.iter().map(crate::logstore::commands_in).sum(),
            );
        }
        let span = trace.span();
        let (trace, responses) = blocking(&self.engine, move |engine| {
            let _entered = span.enter();
            trace.stage(StorageStage::Queue);
            let mut changed = 0;
            let mut each = |engine: &mut Engine, entry: &openraft::Entry<TypeConfig>| {
                // A key change is in flight from before its transaction
                // commits until the key runtime serves its catalog.
                let mut key_change = matches!(
                    &entry.payload,
                    EntryPayload::Normal(ClusterWrite::Keys { .. })
                )
                .then(|| engine.begin_key_change());
                // Before it commits: a durable commit of the group file
                // makes the log durable through it first (see
                // `crate::flush`).
                durability.applying(entry.log_id.index);
                let (response, notices) = Self::apply_entry(engine, &runtimes, entry)?;
                match &entry.payload {
                    EntryPayload::Membership(membership) => logs.applied_membership(membership),
                    // No revision moves; republish the (possibly first)
                    // incarnation for sync serving instead.
                    EntryPayload::Normal(ClusterWrite::Incarnation { .. }) => {
                        published.store(applied_incarnation(engine)?, Ordering::Release);
                    }
                    // No revision moves; the member's key runtime takes
                    // the database's applied catalog before Raft reports
                    // the position applied, then its hubs refresh what
                    // resolved a key.
                    EntryPayload::Normal(ClusterWrite::Keys { db, .. }) => {
                        keys.publish(db, Self::key_catalog(engine, db).map_err(fail_read)?);
                        drop(key_change.take());
                        let _ = commits.send(CommitNotice::keys(db.clone(), response.position));
                    }
                    // A move's imports bring the database's catalog with
                    // its rows, and an abandoned import or a retirement
                    // takes it away: the key runtime follows the row.
                    // Imports precede the activation that serves the
                    // database, so its keys are in place when it does.
                    EntryPayload::Normal(ClusterWrite::Batch { items, .. }) => {
                        for item in items {
                            if let BatchItem::Placement { db, .. } = item {
                                keys.publish(db, Self::key_catalog(engine, db).map_err(fail_read)?);
                            }
                        }
                    }
                    _ => {}
                }
                // One notice per touched database; lagged hubs resync
                // from snapshots, and a full channel never fails the
                // apply. A traced batch's notices name its span.
                let batch = match &entry.payload {
                    EntryPayload::Normal(ClusterWrite::Batch { writer, seq, .. }) => {
                        telemetry::batch_context(*writer, *seq)
                    }
                    _ => None,
                };
                let mut changed = 0;
                for mut notice in notices {
                    changed += notice.puts + notice.deletes;
                    notice.batch = batch.clone();
                    let _ = commits.send(notice);
                }
                Ok((response, changed))
            };
            // Deferred applies commit without an fsync: the quorum's logs
            // hold every entry durably, and a crash replays from the log
            // what the group file lost (see `crate::flush`).
            let deferred = durability.deferred();
            let applied = if deferred {
                flower_engine::storage::deferring(|| {
                    Self::apply_entries(engine, &entries, &mut changed, &mut each)
                })
            } else {
                Self::apply_entries(engine, &entries, &mut changed, &mut each)
            };
            if deferred {
                durability.applied(entries.len() as u64);
            }
            match applied {
                Ok(responses) => {
                    let bytes = entries.iter().map(entry_bytes).sum();
                    accounting.applied(bytes, entries.last().map(|entry| entry.log_id.index));
                    trace.size(StorageBatch::ChangedRecords, changed);
                    trace.stage(StorageStage::Write);
                    Ok((trace, responses))
                }
                Err(error) => {
                    trace.finish(false);
                    Err(error)
                }
            }
        })
        .await?;
        trace.finish(true);
        if let Some(last) = last {
            marks.applied(last);
        }
        // Readers see the applied state by now: operators may look again.
        if touches_catalog {
            self.catalog.bump();
        }
        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        SnapshotBuilder {
            engine: self.engine.clone(),
            logs: self.logs.clone(),
            accounting: self.accounting.clone(),
            durability: self.durability.clone(),
            #[cfg(test)]
            encode_hook: self.encode_hook.clone(),
            _held: self.held.clone(),
        }
    }

    /// A new part file in the image directory, for the leader's
    /// segments to spool into as they arrive.
    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<SnapshotFile>, StorageError<NodeId>> {
        let logs = self.logs.clone();
        let (path, file) = tokio::task::spawn_blocking(move || logs.image_part())
            .await
            .map_err(|error| poison(error.to_string()))?
            .map_err(write_err)?;
        Ok(Box::new(SnapshotFile::new(file, path)))
    }

    /// Install a received image: check it whole, record it durably with
    /// the install marker, install it into the engine one database at a
    /// time, then clear the marker. A crash after the record re-installs
    /// on restart; an image that fails the check changes nothing.
    async fn install_snapshot(
        &mut self,
        meta: &RaftSnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<SnapshotFile>,
    ) -> Result<(), StorageError<NodeId>> {
        let trace = self
            .logs
            .observer()
            .storage(StorageOperation::InstallSnapshot);
        let installed = self.install_received(meta, snapshot).await;
        trace.finish(installed.is_ok());
        installed
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        for _ in 0..IMAGE_OPEN_ATTEMPTS {
            let Some(image) = self.logs.current_image()? else {
                return Ok(None);
            };
            match SnapshotFile::open(&image.path) {
                Ok(file) => {
                    return Ok(Some(Snapshot {
                        meta: image.meta,
                        snapshot: Box::new(file),
                    }));
                }
                // Replaced and deleted since its record was read.
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(read_err(error)),
            }
        }
        Err(poison("the current snapshot kept changing".to_string()))
    }
}

impl StateMachine {
    /// Install a received image, as [`RaftStateMachine::install_snapshot`]
    /// describes.
    async fn install_received(
        &mut self,
        meta: &RaftSnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<SnapshotFile>,
    ) -> Result<(), StorageError<NodeId>> {
        let received = snapshot.close().await.map_err(write_err)?;
        let (logs, recorded) = (self.logs.clone(), meta.clone());
        let id = meta.snapshot_id.clone();
        let checked = tokio::task::spawn_blocking(move || {
            let part = logs.adopt_image(&received).map_err(write_err)?;
            match read_image_header(&part, &id) {
                Ok(header) => Ok((part, header)),
                Err(error) => {
                    let _ = std::fs::remove_file(&part);
                    Err(error)
                }
            }
        })
        .await
        .map_err(|error| poison(error.to_string()))?;
        let (part, header) = checked?;
        if header.applied != meta.last_log_id || header.membership != meta.last_membership {
            let _ = std::fs::remove_file(&part);
            return Err(poison(format!(
                "snapshot {} holds a different position or membership than its metadata",
                meta.snapshot_id
            )));
        }
        // Durable first: a crash from here on re-installs on restart.
        let image = self.logs.begin_install_image(&recorded, &part)?;
        let runtimes = self.runtimes.clone();
        let (incarnation, catalogs, key_change) = blocking(&self.engine, move |engine| {
            // The image may carry other key catalogs: a key change is in
            // flight until the key runtime serves them.
            let key_change = engine.begin_key_change();
            Self::install_image(engine, &runtimes, &image.path, &header)?;
            let read = engine.store().begin_read().map_err(fail_map_read)?;
            Ok((
                applied_incarnation(engine)?,
                Self::key_catalogs(&read).map_err(fail_read)?,
                key_change,
            ))
        })
        .await?;
        self.incarnation.store(incarnation, Ordering::Release);
        self.keys.publish_all(catalogs);
        drop(key_change);
        let _ = self.commits.send(CommitNotice::all_keys(
            meta.last_log_id.map_or(0, |id| id.index),
        ));
        self.catalog.bump();
        self.logs.finish_install()?;
        self.logs
            .applied_membership(meta.last_membership.membership());
        if let Some(installed) = meta.last_log_id {
            self.durability.applying(installed.index);
        }
        self.accounting
            .installed(meta.last_log_id.map(|id| id.index));
        Ok(())
    }
}

/// Read and check a sealed image's header from its file, the whole file
/// and its checksum against the one snapshot `id` names, when it names
/// one (snapshots built before images were checksummed don't).
fn read_image_header(path: &Path, id: &str) -> Result<FileHeader, StorageError<NodeId>> {
    let file = File::open(path).map_err(read_err)?;
    let mut reader = Checksummed::new(BufReader::with_capacity(IMAGE_BUFFER, file));
    let header = read_header(&mut reader).map_err(read_err)?;
    if let Some(expected) = Checksum::named_in(id)
        && expected != reader.checksum()
    {
        return Err(poison(format!(
            "snapshot {id} doesn't match its checksum: corrupt in transit or at rest"
        )));
    }
    Ok(header)
}

/// Builds Raft snapshots from the engine's per-database views.
pub struct SnapshotBuilder {
    engine: Arc<AsyncMutex<Engine>>,
    logs: LogStore,
    accounting: Arc<Accounting>,
    /// Its machine's deferred applies, made durable before a snapshot
    /// is published.
    durability: Arc<ApplyDurability>,
    /// Runs after each view is written, for tests.
    #[cfg(test)]
    encode_hook: Option<EncodeHook>,
    /// Its machine's release token, dropped after the file handles.
    _held: Arc<tokio::sync::watch::Sender<()>>,
}

impl RaftSnapshotBuilder<TypeConfig> for SnapshotBuilder {
    /// Pin one read of the group file under the engine lock, which
    /// takes no longer than opening the read: every entry applies whole
    /// under the lock, so the read sees exactly the state at the group
    /// position it records. Then release the lock and encode every
    /// database from that read into a new image file on the blocking
    /// pool, one view at a time, while applies go on. The sealed image
    /// becomes the current snapshot unless a newer one did meanwhile.
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let activity = self.accounting.begin();
        let observer = self.logs.observer();
        let mut work = SnapshotWork::begin(&observer, SnapshotPhase::Build);
        let mut trace = observer.storage(StorageOperation::BuildSnapshot);
        let accounting = self.accounting.clone();
        let (read, capture) = blocking(&self.engine, move |engine| {
            let capture = accounting.capture();
            let read = engine.store().begin_read().map_err(fail_map_read)?;
            Ok((read, capture))
        })
        .await?;
        trace.stage(StorageStage::Queue);
        let logs = self.logs.clone();
        let accounting = self.accounting.clone();
        let (engine, durability) = (self.engine.clone(), self.durability.clone());
        #[cfg(test)]
        let hook = self.encode_hook.clone();
        let task = tokio::task::spawn_blocking(move || {
            let _activity = activity;
            let built = (|| -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
                let applied = read_applied(&read)?;
                if let Some(applied) = applied {
                    work.at(applied.index);
                }
                let membership = read_membership(&read)?;
                let mut dbs = registered(&read).map_err(fail_map_read)?;
                dbs.push(CLUSTER_DB.to_vec());
                let (part, file) = logs.image_part().map_err(write_err)?;
                let written = (|| {
                    let out = Checksummed::new(BufWriter::with_capacity(IMAGE_BUFFER, file));
                    let mut writer =
                        FileWriter::begin(out, &applied, &membership).map_err(write_err)?;
                    for db in &dbs {
                        let (meta, payload) =
                            encode_snapshot_in(&read, db).map_err(fail_map_read)?;
                        let view = SyncDbSnapshot {
                            meta: SyncDbMeta {
                                db: meta.db,
                                revision: meta.revision,
                                applied_log: meta.applied_log,
                                incarnation: meta.incarnation,
                            },
                            payload,
                        };
                        writer.view(&view).map_err(write_err)?;
                        #[cfg(test)]
                        if let Some(hook) = &hook {
                            hook();
                        }
                    }
                    let out = writer.finish().map_err(write_err)?;
                    let checksum = out.checksum();
                    out.into_inner()
                        .into_inner()
                        .map_err(|error| write_err(error.into_error()))?;
                    Ok::<_, StorageError<NodeId>>(checksum)
                })();
                drop(read);
                trace.stage(StorageStage::Write);
                let checksum = match written {
                    Ok(checksum) => checksum,
                    Err(error) => {
                        let _ = std::fs::remove_file(&part);
                        return Err(error);
                    }
                };
                // The ID carries the image's checksum to whoever receives it.
                let snapshot_id = checksum.id(&match &applied {
                    Some(id) => format!("{}-{}", id.leader_id.term, id.index),
                    None => "empty".to_string(),
                });
                let meta = RaftSnapshotMeta {
                    last_log_id: applied,
                    last_membership: membership,
                    snapshot_id,
                };
                // OpenRaft may purge the log up to a published snapshot:
                // the state it covers must survive a crash first, deferred
                // applies included, or the member would reopen behind a
                // purged log.
                {
                    let engine = engine.blocking_lock();
                    if let Err(error) = durability.flush(engine.store()) {
                        let _ = std::fs::remove_file(&part);
                        return Err(fail_map(error));
                    }
                }
                let mut current = logs.publish_image(&meta, &part)?;
                trace.stage(StorageStage::Commit);
                accounting.published(capture, applied.map(|id| id.index));
                for _ in 0..IMAGE_OPEN_ATTEMPTS {
                    match SnapshotFile::open(&current.path) {
                        Ok(file) => {
                            return Ok(Snapshot {
                                meta: current.meta,
                                snapshot: Box::new(file),
                            });
                        }
                        // A newer snapshot replaced it just now.
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                            match logs.current_image()? {
                                Some(newer) => current = newer,
                                None => return Err(read_err(error)),
                            }
                        }
                        Err(error) => return Err(read_err(error)),
                    }
                }
                Err(poison("the current snapshot kept changing".to_string()))
            })();
            trace.finish(built.is_ok());
            work.outcome(if built.is_ok() {
                SnapshotOutcome::Published
            } else {
                SnapshotOutcome::Failed
            });
            built
        });
        task.await.map_err(|error| poison(error.to_string()))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::{SYNC_SNAPSHOT_VERSION, SyncSnapshotFile};
    use flower_engine::engine::CallOptions;
    use flower_engine::runtime::{CallKind, ClosureRuntime, HostCall};
    use flower_engine::snapshot::encode_snapshot;

    /// Tests here install snapshots of databases without modules.
    struct NoRuntimes;

    impl RuntimeFactory for NoRuntimes {
        fn build(
            &self,
            _db: &[u8],
            module: &str,
            _: &[u8],
            _: &Value,
        ) -> Result<Box<dyn Runtime>, String> {
            Err(format!("no runtime for `{module}` in this test"))
        }
    }

    fn engine(dir: &tempfile::TempDir, name: &str) -> Engine {
        Engine::new(GroupStore::create(dir.path().join(name)).unwrap())
    }

    #[test]
    fn recovery_waits_for_a_partial_import_to_activate_before_loading_its_code() {
        let dir = tempfile::tempdir().unwrap();
        let mut engine = engine(&dir, "partial-import");
        let runtime: Arc<dyn RuntimeFactory> = Arc::new(NoRuntimes);
        let write = engine.store().begin_write().unwrap();
        register_in(&write, b"db").unwrap();
        write
            .set_meta(b"db", &code_identity_key("app"), &[7; 32])
            .unwrap();
        let importing = flower_engine::placement::Placement::Importing {
            epoch: 1,
            op: "move".into(),
            source: "east".into(),
            stage: flower_engine::placement::Stage::Base,
            pages: 1,
            digest: "0".repeat(32),
        };
        write
            .set_meta(
                b"db",
                meta::PLACEMENT,
                &serde_json::to_vec(&importing).unwrap(),
            )
            .unwrap();
        write.commit().unwrap();
        StateMachine::recover(&mut engine, &runtime).unwrap();
        // The same missing code on an active database remains a real corruption error.
        let write = engine.store().begin_write().unwrap();
        write
            .set_meta(
                b"db",
                meta::PLACEMENT,
                &serde_json::to_vec(&flower_engine::placement::Placement::Active { epoch: 1 })
                    .unwrap(),
            )
            .unwrap();
        write.commit().unwrap();
        assert!(
            StateMachine::recover(&mut engine, &runtime)
                .unwrap_err()
                .contains("code missing")
        );
    }

    fn put(engine: &Engine, db: &[u8], value: &[u8]) {
        let write = engine.store().begin_write().unwrap();
        write.put_record(db, 1, b"\"k\"", value).unwrap();
        write.commit().unwrap();
        StateMachine::register_database(engine.store(), db).unwrap();
    }

    fn record(engine: &Engine, db: &[u8]) -> Option<Vec<u8>> {
        let read = engine.store().begin_read().unwrap();
        read.get_record(db, 1, b"\"k\"").unwrap()
    }

    #[test]
    fn install_removes_databases_the_snapshot_lacks() {
        let dir = tempfile::tempdir().unwrap();
        // The snapshot's history only ever created `a`.
        let source = engine(&dir, "source");
        put(&source, b"a", br#"{"v":"snapshot"}"#);
        let mut dbs = Vec::new();
        for db in [b"a".as_slice(), CLUSTER_DB] {
            let (meta, payload) = encode_snapshot(source.store(), db).unwrap();
            dbs.push(SyncDbSnapshot {
                meta: SyncDbMeta {
                    db: meta.db,
                    revision: meta.revision,
                    applied_log: meta.applied_log,
                    incarnation: meta.incarnation,
                },
                payload,
            });
        }
        let file = serde_json::to_vec(&SyncSnapshotFile {
            version: SYNC_SNAPSHOT_VERSION,
            applied: None,
            membership: StoredMembership::default(),
            dbs,
        })
        .unwrap();
        // This member also holds `b`, with a deployed module serving it.
        let mut target = engine(&dir, "target");
        put(&target, b"a", br#"{"v":"local"}"#);
        put(&target, b"b", br#"{"v":"stale"}"#);
        let manifest = parse_value(
            br#"{"collections":[],"definitions":[{"name":"version","kind":"query","args":{"type":"json"}}],"triggers":[],"http":[]}"#,
        )
        .unwrap();
        let runtime = ClosureRuntime::new(
            |_: CallKind, _: &str, _: &str, _: &Value, _: &mut dyn HostCall| {
                Ok(Value::String("stale".to_string()))
            },
        );
        target
            .deploy(b"b", "app", [7; 32], b"wasm", &manifest, Box::new(runtime))
            .unwrap();
        target.set_replicated(b"b");
        let runtimes: Arc<dyn RuntimeFactory> = Arc::new(NoRuntimes);
        StateMachine::install_file(&mut target, &runtimes, &file).unwrap();
        assert_eq!(
            record(&target, b"a").as_deref(),
            Some(br#"{"v":"snapshot"}"#.as_slice())
        );
        assert_eq!(record(&target, b"b"), None, "b's rows survived the install");
        assert_eq!(
            StateMachine::replicated_databases(&target).unwrap(),
            vec![b"a".to_vec()]
        );
        let query = target.invoke_query(
            b"b",
            "app",
            "version",
            &Value::Null,
            &CallOptions::default(),
        );
        assert!(query.is_err(), "b's module still serves: {query:?}");
        // Reinstalling, as a crash-interrupted install resumes, is a no-op.
        StateMachine::install_file(&mut target, &runtimes, &file).unwrap();
        assert_eq!(record(&target, b"b"), None);
    }

    fn applied_at(engine: &Engine, index: u64) {
        let write = engine.store().begin_write().unwrap();
        record_applied(
            &write,
            &LogId::new(openraft::CommittedLeaderId::new(1, 1), index),
        )
        .unwrap();
        write.commit().unwrap();
    }

    /// A build pins one read under the engine lock, then encodes with
    /// the lock free: applies go on meanwhile, and what they commit
    /// stays out of the image, which is exactly the state at the group
    /// position it records.
    /// The release signal fires only once the machine and every snapshot
    /// builder it made are gone, after their file handles: a member's
    /// shutdown waits on it before the files may reopen.
    #[tokio::test]
    async fn files_release_after_the_machine_and_its_builders() {
        let dir = tempfile::tempdir().unwrap();
        let shared = Arc::new(AsyncMutex::new(engine(&dir, "group")));
        let path = dir.path().join("raft.redb");
        let logs = LogStore::open(&path).unwrap();
        let (commits, _) = tokio::sync::broadcast::channel(8);
        let mut machine = StateMachine::new(shared.clone(), logs, Arc::new(NoRuntimes), commits);
        drop(shared);
        let mut released = machine.files_released();
        let builder = machine.get_snapshot_builder().await;
        let closed =
            |released: &mut tokio::sync::watch::Receiver<()>| released.has_changed().is_err();
        drop(machine);
        assert!(!closed(&mut released), "a builder still holds the files");
        assert!(LogStore::open(&path).is_err());
        drop(builder);
        assert!(closed(&mut released));
        // Both files are free once it fired.
        LogStore::open(&path).unwrap();
        GroupStore::create(dir.path().join("group")).unwrap();
        assert!(released.changed().await.is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn snapshots_encode_outside_the_engine_lock_from_one_read() {
        use std::sync::atomic::AtomicBool;
        use std::sync::mpsc;
        let dir = tempfile::tempdir().unwrap();
        let source = engine(&dir, "source");
        put(&source, b"a", br#"{"v":"before"}"#);
        put(&source, b"b", br#"{"v":"before"}"#);
        applied_at(&source, 5);
        let shared = Arc::new(AsyncMutex::new(source));
        let logs = LogStore::open(&dir.path().join("raft.redb")).unwrap();
        let (commits, _) = tokio::sync::broadcast::channel(8);
        let mut machine = StateMachine::new(shared.clone(), logs, Arc::new(NoRuntimes), commits);
        let (reached, wait_reached) = mpsc::channel::<()>();
        let (release, wait_release) = mpsc::channel::<()>();
        let (reached, wait_release) = (
            std::sync::Mutex::new(reached),
            std::sync::Mutex::new(wait_release),
        );
        let paused = AtomicBool::new(false);
        // Pause after the first view (`a`), before `b` encodes.
        machine.encode_hook = Some(Arc::new(move || {
            if !paused.swap(true, Ordering::SeqCst) {
                reached.lock().unwrap().send(()).unwrap();
                wait_release.lock().unwrap().recv().unwrap();
            }
        }));
        let mut builder = machine.get_snapshot_builder().await;
        let build = tokio::spawn(async move { builder.build_snapshot().await });
        tokio::task::spawn_blocking(move || wait_reached.recv().unwrap())
            .await
            .unwrap();
        {
            let engine = shared
                .try_lock()
                .expect("the engine lock is free while the build encodes");
            put(&engine, b"b", br#"{"v":"after"}"#);
            applied_at(&engine, 6);
        }
        release.send(()).unwrap();
        let mut snapshot = build.await.unwrap().unwrap();
        assert_eq!(snapshot.meta.last_log_id.map(|id| id.index), Some(5));
        let mut bytes = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut snapshot.snapshot, &mut bytes)
            .await
            .unwrap();
        let mut target = engine(&dir, "target");
        let runtimes: Arc<dyn RuntimeFactory> = Arc::new(NoRuntimes);
        StateMachine::install_file(&mut target, &runtimes, &bytes).unwrap();
        assert_eq!(
            record(&target, b"b").as_deref(),
            Some(br#"{"v":"before"}"#.as_slice())
        );
        assert_eq!(
            StateMachine::group_applied(&target)
                .unwrap()
                .map(|id| id.index),
            Some(5)
        );
    }

    /// A committed incarnation entry at `index`.
    fn incarnation_entry(
        index: u64,
        incarnation: u64,
        proposed_ms: u64,
    ) -> openraft::Entry<TypeConfig> {
        openraft::Entry {
            log_id: LogId::new(openraft::CommittedLeaderId::new(1, 1), index),
            payload: EntryPayload::Normal(ClusterWrite::Incarnation {
                incarnation,
                proposed_ms,
            }),
        }
    }

    /// A guest-free patch writing `value` under string key `key` of
    /// collection 1 in `db`.
    fn record_patch(db: &[u8], key: &str, value: &str) -> BatchItem {
        use flower_engine::patch::{Patch, RecordWrite, encode_patch};
        let patch = encode_patch(&Patch {
            failed: false,
            db: db.to_vec(),
            versions: Vec::new(),
            records: vec![RecordWrite {
                collection: 1,
                key: format!("\"{key}\"").into_bytes(),
                value: Some(value.as_bytes().to_vec()),
            }],
            indexes: Vec::new(),
            derived: Vec::new(),
            drops: Vec::new(),
            receipts: Vec::new(),
            result: b"null".to_vec(),
            expected_revision: None,
            base_revision: 0,
        })
        .unwrap();
        BatchItem::Patch {
            db: db.to_vec(),
            patch,
        }
    }

    /// A committed batch entry at `index`.
    fn batch_entry(
        index: u64,
        items: Vec<BatchItem>,
        follows: Head,
        seq: u64,
    ) -> openraft::Entry<TypeConfig> {
        openraft::Entry {
            log_id: LogId::new(openraft::CommittedLeaderId::new(1, 1), index),
            payload: EntryPayload::Normal(ClusterWrite::Batch {
                items,
                follows,
                writer: 7,
                seq,
                proposed_ms: 100 + index,
            }),
        }
    }

    /// [`record_patch`] carrying a scoped receipt for `epoch`.
    fn scoped_patch(db: &[u8], key: &str, value: &str, epoch: u64) -> BatchItem {
        use flower_engine::patch::{decode_patch, encode_patch};
        use flower_engine::retry::{Outcome, Receipt, ScopedIdentity};
        let BatchItem::Patch { patch, .. } = record_patch(db, key, value) else {
            unreachable!("record patches are patches");
        };
        let mut patch = decode_patch(&patch).unwrap();
        let identity = ScopedIdentity {
            incarnation: 0,
            epoch,
            intent: key.as_bytes().to_vec(),
        };
        let receipt = Receipt {
            result: Some(Outcome::Value(b"null".to_vec())),
            fingerprint: [0; 32],
            floor: 0,
            expires_at: 0,
            committed: None,
        };
        patch.receipts = vec![(identity.encode().unwrap(), receipt.encode().unwrap())];
        BatchItem::Patch {
            db: db.to_vec(),
            patch: encode_patch(&patch).unwrap(),
        }
    }

    /// Retention decides when an entry applies, not only when it was
    /// prepared: a write whose identity the floor retired since commits
    /// nothing, on every member alike, while one in an admitted epoch
    /// applies.
    #[test]
    fn retired_identities_refuse_at_apply() {
        use flower_engine::retention::RetentionCommand;
        let dir = tempfile::tempdir().unwrap();
        let mut engine = engine(&dir, "group");
        let runtimes: Arc<dyn RuntimeFactory> = Arc::new(NoRuntimes);
        let retention = |command| BatchItem::Retention {
            db: b"a".to_vec(),
            command,
        };
        let setup = batch_entry(
            1,
            vec![
                retention(RetentionCommand::Rotate {
                    epoch_ms: None,
                    keep_epochs: 2,
                }),
                retention(RetentionCommand::Advance {
                    from_epoch: 0,
                    current_epoch: 1,
                    min_epoch: 1,
                }),
            ],
            Head::Entry(0),
            1,
        );
        let (applied, _) = StateMachine::apply_entry(&mut engine, &runtimes, &setup).unwrap();
        assert!(
            applied
                .items
                .iter()
                .all(|item| matches!(item.outcome, ResponseOutcome::Value(_))),
            "{applied:?}"
        );
        let writes = batch_entry(
            2,
            vec![
                scoped_patch(b"a", "old", "1", 0),
                scoped_patch(b"a", "new", "2", 1),
            ],
            Head::Batch { writer: 7, seq: 1 },
            2,
        );
        let (applied, notices) =
            StateMachine::apply_entry(&mut engine, &runtimes, &writes).unwrap();
        // One notice per item, each with the records it wrote: none for
        // the refused one, whose revision did not move.
        let records: Vec<(u64, u64, u64)> = notices
            .iter()
            .map(|notice| (notice.revision, notice.puts, notice.deletes))
            .collect();
        assert_eq!(
            records,
            [
                (applied.items[0].revision, 0, 0),
                (applied.items[1].revision, 1, 0)
            ]
        );
        let ResponseOutcome::Failure(failure) = &applied.items[0].outcome else {
            panic!("{applied:?}");
        };
        let failure = flower_sdk::abi::decode_envelope(parse_value(failure).unwrap());
        assert_eq!(failure.unwrap_err().code, "epoch_retired");
        assert!(matches!(
            applied.items[1].outcome,
            ResponseOutcome::Value(_)
        ));
        assert_eq!(value_at(&engine, b"a", "old"), None);
        assert_eq!(value_at(&engine, b"a", "new").as_deref(), Some("2"));
    }

    fn value_at(engine: &Engine, db: &[u8], key: &str) -> Option<String> {
        let read = engine.store().begin_read().unwrap();
        read.get_record(db, 1, format!("\"{key}\"").as_bytes())
            .unwrap()
            .map(|bytes| String::from_utf8(bytes).unwrap())
    }

    /// Batch items apply in order in one entry, each with a revision of
    /// its own, and only directly after the data entry their writer
    /// prepared them after: once another entry landed in between, the
    /// batch and every successor prepared on it apply nothing.
    #[test]
    fn batches_apply_in_order_only_after_their_head() {
        let dir = tempfile::tempdir().unwrap();
        let mut engine = engine(&dir, "group");
        let runtimes: Arc<dyn RuntimeFactory> = Arc::new(NoRuntimes);
        let apply = |engine: &mut Engine, entry: openraft::Entry<TypeConfig>| {
            StateMachine::apply_entry(engine, &runtimes, &entry)
                .unwrap()
                .0
        };
        let revisions = |response: &WriteResponse| {
            response
                .items
                .iter()
                .map(|item| item.revision)
                .collect::<Vec<_>>()
        };
        let first = apply(
            &mut engine,
            batch_entry(
                1,
                vec![record_patch(b"a", "k", "1"), record_patch(b"a", "k", "2")],
                Head::Entry(0),
                1,
            ),
        );
        assert_eq!(revisions(&first), [1, 2]);
        // A successor prepared while the first committed names it.
        let second = apply(
            &mut engine,
            batch_entry(
                2,
                vec![record_patch(b"a", "j", "3")],
                Head::Batch { writer: 7, seq: 1 },
                2,
            ),
        );
        assert_eq!(revisions(&second), [3]);
        // Another entry lands: a batch prepared after the second is
        // stale, and so is one prepared on top of that.
        apply(&mut engine, incarnation_entry(3, 5, 103));
        for (index, follows, seq) in [
            (4, Head::Batch { writer: 7, seq: 2 }, 3),
            (5, Head::Batch { writer: 7, seq: 3 }, 4),
        ] {
            let entry = batch_entry(index, vec![record_patch(b"a", "k", "9")], follows, seq);
            let stale = apply(&mut engine, entry);
            assert!(stale.items.iter().all(is_stale_batch), "{stale:?}");
            assert_eq!(value_at(&engine, b"a", "k").as_deref(), Some("2"));
        }
        // Prepared again after the stale entry, it applies.
        let again = apply(
            &mut engine,
            batch_entry(6, vec![record_patch(b"a", "k", "4")], Head::Entry(5), 5),
        );
        assert_eq!(revisions(&again), [4]);
        assert_eq!(value_at(&engine, b"a", "k").as_deref(), Some("4"));
        assert_eq!(value_at(&engine, b"a", "j").as_deref(), Some("3"));
        assert_eq!(
            StateMachine::group_applied(&engine)
                .unwrap()
                .map(|id| id.index),
            Some(6)
        );
    }

    /// A key catalog change is a data entry like any other: applied or
    /// rejected, it becomes the head, so a batch the writer prepared
    /// before it (which may have signed against the catalog it replaces)
    /// applies nothing, and one prepared after it applies.
    #[test]
    fn key_changes_move_the_head_like_any_data_entry() {
        let dir = tempfile::tempdir().unwrap();
        let mut engine = engine(&dir, "group");
        let runtimes: Arc<dyn RuntimeFactory> = Arc::new(NoRuntimes);
        let apply = |engine: &mut Engine, entry: openraft::Entry<TypeConfig>| {
            StateMachine::apply_entry(engine, &runtimes, &entry)
                .unwrap()
                .0
        };
        apply(
            &mut engine,
            batch_entry(1, vec![record_patch(b"a", "k", "1")], Head::Entry(0), 1),
        );
        let keys = apply(
            &mut engine,
            openraft::Entry {
                log_id: LogId::new(openraft::CommittedLeaderId::new(1, 1), 2),
                payload: EntryPayload::Normal(ClusterWrite::Keys {
                    db: b"a".to_vec(),
                    change: b"not a change".to_vec(),
                    proposed_ms: 102,
                }),
            },
        );
        assert!(
            matches!(keys.outcome, ResponseOutcome::Error(_)),
            "{keys:?}"
        );
        assert_eq!(StateMachine::head(&engine).unwrap(), Head::Entry(2));
        let stale = apply(
            &mut engine,
            batch_entry(
                3,
                vec![record_patch(b"a", "k", "9")],
                Head::Batch { writer: 7, seq: 1 },
                2,
            ),
        );
        assert!(stale.items.iter().all(is_stale_batch), "{stale:?}");
        assert_eq!(value_at(&engine, b"a", "k").as_deref(), Some("1"));
        let again = apply(
            &mut engine,
            batch_entry(4, vec![record_patch(b"a", "k", "4")], Head::Entry(3), 3),
        );
        assert!(!again.items.iter().any(is_stale_batch), "{again:?}");
        assert_eq!(value_at(&engine, b"a", "k").as_deref(), Some("4"));
    }

    fn label(engine: &Engine, db: &[u8]) -> u64 {
        let read = engine.store().begin_read().unwrap();
        read.meta_u64(db, meta::HISTORY_INCARNATION).unwrap()
    }

    #[test]
    fn first_applied_incarnation_wins() {
        let dir = tempfile::tempdir().unwrap();
        let mut engine = engine(&dir, "group");
        put(&engine, b"a", br#"{"v":"before"}"#);
        // `a` moved in from a group of incarnation 5 before this group
        // had one: zero heads its lineage.
        let lineage = |engine: &Engine, db: &[u8]| {
            let read = engine.store().begin_read().unwrap();
            flower_engine::placement::lineage(&read, db).unwrap()
        };
        let write = engine.store().begin_write().unwrap();
        let moved_in = [0u64.to_be_bytes(), 5u64.to_be_bytes()].concat();
        write
            .set_meta(b"a", meta::HISTORY_LINEAGE, &moved_in)
            .unwrap();
        write.commit().unwrap();
        assert_eq!(lineage(&engine, b"a"), vec![5]);
        let runtimes: Arc<dyn RuntimeFactory> = Arc::new(NoRuntimes);
        StateMachine::apply_entry(&mut engine, &runtimes, &incarnation_entry(1, 7, 100)).unwrap();
        // Establishing the incarnation keeps the history it carried.
        assert_eq!(lineage(&engine, b"a"), vec![5]);
        // A second leader's proposal, racing the first, commits after
        // it: it records its position and stamp but changes nothing.
        StateMachine::apply_entry(&mut engine, &runtimes, &incarnation_entry(2, 9, 200)).unwrap();
        assert_eq!(StateMachine::incarnation(&engine).unwrap(), 7);
        assert_eq!(StateMachine::expiry_clock(&engine).unwrap(), 200);
        assert_eq!(
            StateMachine::group_applied(&engine)
                .unwrap()
                .map(|id| id.index),
            Some(2)
        );
        // Databases registered before and after carry the same label.
        assert_eq!(label(&engine, b"a"), 7);
        put(&engine, b"b", br#"{"v":"after"}"#);
        assert_eq!(label(&engine, b"b"), 7);
        // A fresh log restarts the history: no incarnation, no labels,
        // while the expiry clock carries over.
        let write = engine.store().begin_write().unwrap();
        StateMachine::restart_history(&write).unwrap();
        write.commit().unwrap();
        assert_eq!(StateMachine::incarnation(&engine).unwrap(), 0);
        assert_eq!((label(&engine, b"a"), label(&engine, b"b")), (0, 0));
        assert_eq!(StateMachine::expiry_clock(&engine).unwrap(), 200);
        // ... and ends every history carried in, for good: the next
        // incarnation adopts none.
        assert!(lineage(&engine, b"a").is_empty());
        StateMachine::apply_entry(&mut engine, &runtimes, &incarnation_entry(3, 11, 300)).unwrap();
        assert_eq!(label(&engine, b"a"), 11);
        assert!(lineage(&engine, b"a").is_empty());
    }
}
