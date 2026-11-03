//! Sync-down replicas: pull batches from a server and apply
//! them to a local device engine. Positions persist in the local
//! `$sync` database; snapshots install wholesale while entries
//! apply in index order. Devices follow upstream positions
//! verbatim: nothing resets on this path.
//!
//! Crash safety. Every patch, and every batch entry's items together,
//! commits in the same transaction as the sync position it advances
//! to, along with the group head batches check exactly as the server
//! does (see [`flower_engine::batch`]); deployments activate idempotently by
//! code identity and advance the position right after; a module
//! removal commits in the transaction that advances the position, and
//! a removed module's purge pages arrive as batch items. A crash or an
//! apply error at entry `k` therefore leaves exactly the entries
//! before `k` applied, and the next pull resumes at `k`. Batches are
//! checked before anything applies: entries must sit strictly after
//! the position, in increasing order, within the covered index.
//! Snapshots are journaled in `$sync` before their views install; an
//! interrupted install finishes from the journal before anything else
//! applies (a newer snapshot replaces the journal), so a partial
//! install never stays visible to later entries.
//!
//! History. Positions restart when the server's cluster is restored or
//! re-bootstrapped, so the replica records the group incarnation it
//! follows in `$sync` beside its position. A replica following none
//! adopts the server's with its first install or apply (when it cannot
//! hold another history yet: at position zero or from a snapshot), or
//! from the replicated incarnation entry itself, first wins as on the
//! server. From then on a batch from another incarnation is refused
//! with [`FlowerError::HistoryChanged`] before anything applies, and a
//! batch from a server that has not established one yet (a restored
//! cluster, until its leader proposes) applies nothing. A refused
//! replica must be rebuilt; nothing resyncs automatically.
//!
//! Expiry. Duplicate detection reads receipt expiry against the
//! server's replicated expiry clock, never a raw entry stamp: each
//! patch, deployment and incarnation entry advances the same
//! `$cluster` clock row the server advances, in the transaction that
//! applies it, and patches commit at the advanced clock. The replica
//! therefore reaches the server's duplicate verdicts even after a
//! leader's wall clock stepped back. Snapshots carry the server's
//! `$cluster` view, clock included.
//!
//! Intent receipts. After each batch, submitted or acknowledged
//! intents whose receipts arrived are marked committed (at the
//! revision of the entry that delivered them) or failed with the
//! recorded outcome, so acknowledged intents keep previewing until
//! their effects are present locally.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use flower_engine::batch::{Applied, BatchItem, Head};
use flower_engine::engine::{Activation, Engine};
use flower_engine::intent::IntentStatus;
use flower_engine::json::parse_value;
use flower_engine::patch::Patch;
use flower_engine::placement::PlacementEffect;
use flower_engine::retry::{Outcome, Receipt};
use flower_engine::runtime::Runtime;
use flower_engine::snapshot::{SnapshotMeta, apply_snapshot};
use flower_engine::staging::{CODE_IDENTITY_PREFIX, code_identity_key};
use flower_engine::storage::{WriteTxn, meta};
use flower_sdk::{Value, fail};

use crate::client::{Method, send};
use crate::error::{FlowerError, engine_err};
use crate::queue::receipt_identity;
use crate::transport::{Credentials, Transport};

/// Builds device runtimes from synced code. The module name keys
/// backends caching compiled code.
pub type DeviceRuntimeBuilder =
    Arc<dyn Fn(&str, &[u8], &Value) -> Result<Box<dyn Runtime>, String> + Send + Sync>;

/// Local sync-position database.
const SYNC_DB: &[u8] = b"$sync";
/// Sync-position meta row.
const SYNC_POS: &[u8] = b"pos";
/// Followed group incarnation meta row, zero until adopted.
const SYNC_INCARNATION: &[u8] = b"incarnation";
/// Journaled snapshot install: the covered index and the incarnation
/// to adopt (each `u64` big-endian, zero for none) followed by the
/// snapshot file bytes. Present only while an install is unfinished.
const SYNC_INSTALL: &[u8] = b"install";
/// The server's group coordination database. Mirrors `CLUSTER_DB` in
/// flower-cluster's `statemachine.rs`, the source of truth: devices
/// never link the cluster crate.
const CLUSTER_DB: &[u8] = b"$cluster";
/// The expiry clock meta row inside [`CLUSTER_DB`]. Mirrors
/// `GROUP_EXPIRY_CLOCK` in the same file.
const GROUP_EXPIRY_CLOCK: &[u8] = b"group_expiry_clock";
/// Snapshot file wire version the client installs.
const SNAPSHOT_VERSION: u32 = 1;
/// Default timeout for one sync pull, snapshots included.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(60);

pub type WireEntry = flower_protocol::SyncEntry<WireWrite>;

/// A replicated write on the wire, mirroring the server.
#[derive(serde::Deserialize, Clone, Debug)]
pub enum WireWrite {
    /// A mutation patch.
    Patch {
        /// Database identifier bytes.
        db: Vec<u8>,
        /// Encoded patch bytes.
        patch: Vec<u8>,
        /// Leader proposal stamp in ms.
        #[serde(default)]
        proposed_ms: u64,
    },
    /// An atomic multi-database transaction patch.
    MultiPatch {
        /// Encoded multi-patch bytes.
        patches: Vec<u8>,
        /// Leader proposal stamp in ms.
        #[serde(default)]
        proposed_ms: u64,
    },
    /// A staged deployment.
    Deploy {
        /// Database identifier bytes.
        db: Vec<u8>,
        /// Module name.
        module: String,
        /// Wasm bundle bytes.
        wasm: Vec<u8>,
        /// Canonical manifest JSON bytes.
        manifest: Vec<u8>,
        /// Target code identity.
        identity: [u8; 32],
        /// Authoritative intern bindings.
        bindings: Vec<(Vec<u8>, u32)>,
        /// Leader proposal stamp in ms.
        #[serde(default)]
        proposed_ms: u64,
    },
    /// A module's removal from one database, applied as members apply
    /// it; with `purge`, its data goes through later batches' purge
    /// pages.
    Remove {
        /// Database identifier bytes.
        db: Vec<u8>,
        /// Module name.
        module: String,
        /// Whether its data purges too.
        purge: bool,
        /// Leader proposal stamp in ms.
        #[serde(default)]
        proposed_ms: u64,
    },
    /// The group's history incarnation; the first applied one wins.
    Incarnation {
        /// Proposed incarnation, never zero.
        incarnation: u64,
        /// Leader proposal stamp in ms.
        #[serde(default)]
        proposed_ms: u64,
    },
    /// A group of writes the leader's writer committed as one entry:
    /// they apply in order only directly after the data entry `follows`
    /// names (see [`flower_engine::batch`]).
    Batch {
        /// The writes, in order.
        items: Vec<BatchItem>,
        /// The data entry the batch was prepared after.
        follows: Head,
        /// The proposing writer's identity.
        writer: u64,
        /// The batch's sequence within its writer.
        seq: u64,
        /// Leader proposal stamp in ms.
        proposed_ms: u64,
    },
    /// A change to one database's managed-key catalog, emptied of its
    /// material: the device keeps no catalog, but the entry moves the
    /// group head as it does on members.
    Keys {
        /// Database identifier bytes.
        db: Vec<u8>,
        /// Leader proposal stamp in ms.
        #[serde(default)]
        proposed_ms: u64,
    },
}

pub type WireSnapshot = flower_protocol::SyncSnapshot;
pub type WireBatch = flower_protocol::SyncBatch<WireWrite>;

/// One database inside a snapshot file.
#[derive(serde::Deserialize, Clone, Debug)]
struct WireDb {
    /// Fencing metadata for the payload.
    meta: WireDbMeta,
    /// Encoded engine snapshot payload.
    payload: Vec<u8>,
}

/// Fencing metadata for one database snapshot.
#[derive(serde::Deserialize, Clone, Debug)]
struct WireDbMeta {
    /// Database identifier bytes.
    db: Vec<u8>,
    /// Committed revision the view was taken at.
    revision: u64,
    /// Applied log position the view was taken at.
    applied_log: u64,
    /// History incarnation the view belongs to.
    incarnation: u64,
}

/// A snapshot file: every database at one position. Membership and
/// Raft framing decode-and-ignore: devices track data only.
#[derive(serde::Deserialize, Clone, Debug)]
struct WireSnapshotFile {
    /// Wire version, must match [`SNAPSHOT_VERSION`].
    version: u32,
    /// One view per database.
    dbs: Vec<WireDb>,
}

/// One sync run's progress.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SyncProgress {
    /// Entries applied.
    pub applied: usize,
    /// Position synced through.
    pub current: u64,
    /// Whether a snapshot installed.
    pub snapshot: bool,
}

/// Revisions at which receipts arrived during one batch, keyed by
/// `(database, receipt identity)`.
type ArrivedReceipts = HashMap<(Vec<u8>, Vec<u8>), u64>;

/// A sync-down client over a device engine.
pub struct SyncClient {
    transport: Arc<dyn Transport>,
    credentials: Option<Credentials>,
    engine: Arc<tokio::sync::Mutex<Engine>>,
    build_runtime: DeviceRuntimeBuilder,
    timeout: Duration,
    /// The group followed, when not the root.
    group: Option<String>,
}

impl SyncClient {
    /// Pull through a transport into a device engine. Credentials
    /// ride every pull; each pull times out after 60 s unless
    /// [`SyncClient::with_timeout`] says otherwise.
    pub fn new(
        transport: Arc<dyn Transport>,
        credentials: Option<Credentials>,
        engine: Arc<tokio::sync::Mutex<Engine>>,
        build_runtime: DeviceRuntimeBuilder,
    ) -> Self {
        SyncClient {
            transport,
            credentials,
            engine,
            build_runtime,
            timeout: DEFAULT_TIMEOUT,
            group: None,
        }
    }

    /// Follow `group`'s history instead of the root's: a device follows
    /// one group, and holds the databases that group owns.
    pub fn with_group(mut self, group: impl Into<String>) -> Self {
        self.group = Some(group.into());
        self
    }

    /// Set the per-pull timeout.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// The local sync position, zero when never synced.
    pub async fn position(&self) -> Result<u64, FlowerError> {
        let engine = self.engine.lock().await;
        read_position(&engine)
    }

    /// The group incarnation this replica follows, zero until adopted.
    pub async fn incarnation(&self) -> Result<u64, FlowerError> {
        let engine = self.engine.lock().await;
        read_incarnation(&engine)
    }

    /// Pull and apply one batch of up to `limit` entries.
    pub async fn sync_once(&self, limit: u64) -> Result<SyncProgress, FlowerError> {
        let since = self.position().await?;
        let mut path = format!("/v1/sync?since={since}&limit={limit}");
        if let Some(group) = &self.group {
            path.push_str("&group=");
            path.push_str(&crate::admin::encode_query(group));
        }
        let body = send(
            self.transport.as_ref(),
            Method::Get,
            &path,
            &[],
            self.credentials.as_ref(),
            None,
            self.timeout,
        )
        .await?;
        let batch: WireBatch = serde_json::from_slice(&body)
            .map_err(|error| FlowerError::Decode(format!("sync batch: {error}")))?;
        self.apply_batch(&batch).await
    }

    /// Apply one batch to the device engine. The batch's incarnation is
    /// checked first: another history refuses the batch with
    /// [`FlowerError::HistoryChanged`], and a server without one yet
    /// applies nothing while this replica follows one (see the module
    /// docs). A snapshot batch journals and installs its views, then
    /// moves the position to the snapshot's covered index. An entry
    /// batch first finishes any interrupted install, checks the batch
    /// against the position, then applies entries in index order, each
    /// advancing the position atomically with its effects, and finally
    /// advances to the covered index. Applied entries stay applied when
    /// a later one fails.
    pub async fn apply_batch(&self, batch: &WireBatch) -> Result<SyncProgress, FlowerError> {
        let mut guard = self.engine.lock().await;
        let engine = &mut *guard;
        let mut arrived = ArrivedReceipts::new();
        let unchanged = |position| SyncProgress {
            applied: 0,
            current: position,
            snapshot: false,
        };
        if batch.version != flower_protocol::VERSION {
            return Err(FlowerError::Unsupported(format!(
                "sync version {}",
                batch.version
            )));
        }
        if let Some(snapshot) = &batch.snapshot {
            let local = read_incarnation(engine)?;
            let Lineage::Apply(adopt) = lineage(local, batch.incarnation, true)? else {
                return Ok(unchanged(read_position(engine)?));
            };
            check_batch(batch, read_position(engine)?)?;
            self.install_snapshot(engine, snapshot, adopt.unwrap_or(0))?;
            reconcile_intents(engine, &arrived)?;
            return Ok(SyncProgress {
                applied: 0,
                current: snapshot.last_index,
                snapshot: true,
            });
        }
        let before_resume = read_position(engine)?;
        self.resume_install(engine)?;
        let position = read_position(engine)?;
        let local = read_incarnation(engine)?;
        let Lineage::Apply(mut adopt) = lineage(local, batch.incarnation, position == 0)? else {
            return Ok(unchanged(position));
        };
        // A journaled install may have overtaken the response requested before it resumed.
        if position > before_resume && batch.current <= position {
            return Ok(unchanged(position));
        }
        check_batch(batch, position)?;
        let mut applied = 0usize;
        let mut outcome = Ok(());
        for entry in &batch.entries {
            if let Err(error) = self.apply_entry(engine, entry, &mut arrived, &mut adopt) {
                outcome = Err(error);
                break;
            }
            applied += 1;
        }
        // Receipts that arrived before a failing entry still settle
        // their intents; the apply error wins over a reconcile error.
        let reconciled = reconcile_intents(engine, &arrived);
        outcome?;
        reconciled?;
        // Blanks and membership entries past the last data entry
        // still advance the position; it never moves backwards.
        let reached = read_position(engine)?;
        let current = batch.current.max(reached);
        if current > reached {
            let write = engine.store().begin_write().map_err(engine_err)?;
            record_position(&write, current, &mut adopt)?;
            write.commit().map_err(engine_err)?;
        }
        Ok(SyncProgress {
            applied,
            current,
            snapshot: false,
        })
    }

    /// Sync until a pull no longer advances the position, then
    /// return the final progress.
    pub async fn sync_catch_up(&self, limit: u64) -> Result<SyncProgress, FlowerError> {
        loop {
            let before = self.position().await?;
            let progress = self.sync_once(limit).await?;
            if progress.current <= before {
                return Ok(progress);
            }
        }
    }

    /// Run sync until shutdown flips true or its sender drops: catch
    /// up, sleep, repeat. The first error aborts for the supervisor.
    pub async fn run_forever(
        &self,
        interval: Duration,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Result<(), FlowerError> {
        loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            self.sync_catch_up(512).await?;
            tokio::select! {
                _ = tokio::time::sleep(interval) => {}
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                }
            }
        }
    }

    /// Apply one entry, advancing the expiry clock and the position to
    /// its index in the same transaction as its effects (deployments:
    /// right after their idempotent activation). A pending adoption
    /// rides with the first position write.
    fn apply_entry(
        &self,
        engine: &mut Engine,
        entry: &WireEntry,
        arrived: &mut ArrivedReceipts,
        adopt: &mut Option<u64>,
    ) -> Result<(), FlowerError> {
        let index = entry.index;
        match &entry.write {
            WireWrite::Patch {
                patch, proposed_ms, ..
            } => {
                let checked = engine.check_patch(patch, index).map_err(engine_err)?;
                let write = engine.store().begin_write().map_err(engine_err)?;
                let clock = advance_expiry_clock(&write, *proposed_ms)?;
                let outcome = engine
                    .commit_patch(&write, &checked, index, clock)
                    .map_err(engine_err)?;
                flower_engine::replication::register(&write, &checked.patch.db)
                    .map_err(engine_err)?;
                Head::Entry(index).record(&write).map_err(engine_err)?;
                record_position(&write, index, adopt)?;
                write.commit().map_err(engine_err)?;
                note_receipts(arrived, &checked.patch, outcome.revision);
            }
            WireWrite::MultiPatch {
                patches,
                proposed_ms,
            } => {
                let checked = engine
                    .check_multi_patch(patches, index)
                    .map_err(engine_err)?;
                let write = engine.store().begin_write().map_err(engine_err)?;
                let clock = advance_expiry_clock(&write, *proposed_ms)?;
                let outcome = engine
                    .commit_multi(&write, &checked, index, clock)
                    .map_err(engine_err)?;
                for (db, _) in &outcome.revisions {
                    flower_engine::replication::register(&write, db).map_err(engine_err)?;
                }
                Head::Entry(index).record(&write).map_err(engine_err)?;
                record_position(&write, index, adopt)?;
                write.commit().map_err(engine_err)?;
                for (step, (_, revision)) in checked.patches.iter().zip(&outcome.revisions) {
                    note_receipts(arrived, &step.patch, *revision);
                }
            }
            WireWrite::Deploy {
                db,
                module,
                wasm,
                manifest,
                identity,
                bindings,
                proposed_ms,
            } => {
                let value = parse_value(manifest).map_err(engine_err)?;
                let read = engine.store().begin_read().map_err(engine_err)?;
                let active = read
                    .meta(db, &code_identity_key(module))
                    .map_err(engine_err)?;
                // As members apply it: a database that does not serve
                // here, or that a prepared transaction holds, takes no
                // deployment, nor does a module whose data, or a queue it
                // declares, is being purged.
                let refused = flower_engine::txn::unwritable(&read, db)
                    .map_err(engine_err)?
                    .is_some()
                    || flower_engine::purge::activation_refusal(&read, db, module, &value)
                        .map_err(engine_err)?
                        .is_some();
                drop(read);
                if !refused
                    && !matches!(active, Some(bytes) if bytes.as_slice() == identity.as_slice())
                {
                    let runtime =
                        (self.build_runtime)(module, wasm, &value).map_err(FlowerError::Engine)?;
                    let activation = Activation {
                        identity: *identity,
                        bindings: bindings.clone(),
                    };
                    engine
                        .apply_activation(db, module, wasm, &value, &activation, runtime)
                        .map_err(engine_err)?;
                }
                let write = engine.store().begin_write().map_err(engine_err)?;
                advance_expiry_clock(&write, *proposed_ms)?;
                if !refused {
                    flower_engine::replication::register(&write, db).map_err(engine_err)?;
                }
                Head::Entry(index).record(&write).map_err(engine_err)?;
                record_position(&write, index, adopt)?;
                write.commit().map_err(engine_err)?;
            }
            WireWrite::Remove {
                db,
                module,
                purge,
                proposed_ms,
            } => {
                // As members apply it: the removal, or its refusal,
                // commits with the clock, the head and the position.
                let staged = engine.store().begin_staged().map_err(engine_err)?;
                advance_expiry_clock(staged.write(), *proposed_ms)?;
                Head::Entry(index)
                    .record(staged.write())
                    .map_err(engine_err)?;
                record_position(staged.write(), index, adopt)?;
                let _ = engine
                    .commit_removal(staged, db, module, *purge)
                    .map_err(engine_err)?;
            }
            WireWrite::Keys { db, proposed_ms } => {
                // Devices hold no key material and cannot tell which
                // key changed, or whether it did: every derived value
                // of the database a member computed from a key drops,
                // as members drop it.
                let _key_change = engine.begin_key_change();
                let write = engine.store().begin_write().map_err(engine_err)?;
                advance_expiry_clock(&write, *proposed_ms)?;
                Head::Entry(index).record(&write).map_err(engine_err)?;
                record_position(&write, index, adopt)?;
                engine.drop_key_readers(&write, db).map_err(engine_err)?;
                write.commit().map_err(engine_err)?;
            }
            WireWrite::Incarnation {
                incarnation,
                proposed_ms,
            } => {
                let write = engine.store().begin_write().map_err(engine_err)?;
                flower_engine::replication::establish(&write, *incarnation).map_err(engine_err)?;
                advance_expiry_clock(&write, *proposed_ms)?;
                Head::Entry(index).record(&write).map_err(engine_err)?;
                record_position(&write, index, adopt)?;
                // First wins, as on the server: a replica following no
                // incarnation yet joins the group's when it arrives.
                let followed = write
                    .meta_u64(SYNC_DB, SYNC_INCARNATION)
                    .map_err(engine_err)?;
                if followed == 0 && *incarnation != 0 {
                    write
                        .set_meta_u64(SYNC_DB, SYNC_INCARNATION, *incarnation)
                        .map_err(engine_err)?;
                }
                write.commit().map_err(engine_err)?;
            }
            WireWrite::Batch {
                items,
                follows,
                writer,
                seq,
                proposed_ms,
            } => {
                // As the server applies it: in order and in one
                // transaction, and only right after the entry it was
                // prepared after; a stale batch applies nothing.
                // A move's activation commits its rows and loads its code
                // with readers fenced out, as on members.
                let activates = items.iter().any(BatchItem::activates);
                let mut apply = |engine: &mut Engine| -> Result<(), FlowerError> {
                    let staged = engine.store().begin_staged().map_err(engine_err)?;
                    let clock = advance_expiry_clock(staged.write(), *proposed_ms)?;
                    let head = Head::read(&staged.read()).map_err(engine_err)?;
                    let mut delivered = Vec::new();
                    let mut dropped = Vec::new();
                    let mut activated = Vec::new();
                    if head == *follows {
                        for item in engine
                            .commit_batch(&staged, items, index, clock)
                            .map_err(engine_err)?
                        {
                            flower_engine::replication::register_applied(staged.write(), &item)
                                .map_err(engine_err)?;
                            match item {
                                Applied::Patch { checked, outcome } => {
                                    delivered.push((checked.patch, outcome.revision));
                                }
                                Applied::Multi { checked, outcome } => {
                                    for (step, (_, revision)) in
                                        checked.patches.into_iter().zip(outcome.revisions)
                                    {
                                        delivered.push((step.patch, revision));
                                    }
                                }
                                // Retention, the catalog, cross-group
                                // transactions, a removed module's purge
                                // and staged deployments change no
                                // intent's fate: intents never run as
                                // transaction steps, nor in a removed
                                // module, and a staging only builds what
                                // activation switches to.
                                Applied::Retention { .. }
                                | Applied::Catalog { .. }
                                | Applied::Txn { .. }
                                | Applied::Purge { .. }
                                | Applied::Staging { .. } => {}
                                // A database that left this group leaves
                                // the device too, as it leaves members.
                                Applied::Placement { db, effect, .. } => match effect {
                                    PlacementEffect::Dropped => dropped.push(db),
                                    PlacementEffect::Activated => activated.push(db),
                                    PlacementEffect::None => {}
                                },
                            }
                        }
                        Head::Batch {
                            writer: *writer,
                            seq: *seq,
                        }
                        .record(staged.write())
                        .map_err(engine_err)?;
                    } else {
                        Head::Entry(index)
                            .record(staged.write())
                            .map_err(engine_err)?;
                    }
                    record_position(staged.write(), index, adopt)?;
                    staged.commit().map_err(engine_err)?;
                    for db in &dropped {
                        engine.forget_database(db);
                    }
                    // A database that moved into this group serves from its
                    // imported code, as on members.
                    for db in &activated {
                        engine.set_replicated(db);
                        self.reload_db(engine, db)?;
                    }
                    for (patch, revision) in &delivered {
                        note_receipts(arrived, patch, *revision);
                    }
                    Ok(())
                };
                if activates {
                    engine.exclusive(apply)?;
                } else {
                    apply(engine)?;
                }
            }
        }
        Ok(())
    }

    /// Journal a snapshot with the incarnation to adopt (zero for
    /// none), then install it. The journal is written before the first
    /// view lands, so a crash mid-install resumes from it; a newer
    /// snapshot simply replaces it.
    fn install_snapshot(
        &self,
        engine: &mut Engine,
        snapshot: &WireSnapshot,
        adopt: u64,
    ) -> Result<(), FlowerError> {
        use base64::Engine;
        let bytes = base64::prelude::BASE64_STANDARD
            .decode(snapshot.bytes_b64.as_bytes())
            .map_err(|error| FlowerError::Decode(format!("snapshot base64: {error}")))?;
        let file = decode_snapshot_file(&bytes)?;
        let mut journal = Vec::with_capacity(16 + bytes.len());
        journal.extend_from_slice(&snapshot.last_index.to_be_bytes());
        journal.extend_from_slice(&adopt.to_be_bytes());
        journal.extend_from_slice(&bytes);
        let write = engine.store().begin_write().map_err(engine_err)?;
        write
            .set_meta(SYNC_DB, SYNC_INSTALL, &journal)
            .map_err(engine_err)?;
        write.commit().map_err(engine_err)?;
        self.finish_install(engine, snapshot.last_index, adopt, &file)
    }

    /// Finish an install a crash or error interrupted, if any.
    fn resume_install(&self, engine: &mut Engine) -> Result<(), FlowerError> {
        let read = engine.store().begin_read().map_err(engine_err)?;
        let journal = read.meta(SYNC_DB, SYNC_INSTALL).map_err(engine_err)?;
        drop(read);
        let Some(journal) = journal else {
            return Ok(());
        };
        let truncated = || FlowerError::Decode("truncated snapshot journal".to_string());
        let (index, rest) = journal.split_first_chunk::<8>().ok_or_else(truncated)?;
        let (adopt, bytes) = rest.split_first_chunk::<8>().ok_or_else(truncated)?;
        let file = decode_snapshot_file(bytes)?;
        self.finish_install(
            engine,
            u64::from_be_bytes(*index),
            u64::from_be_bytes(*adopt),
            &file,
        )
    }

    /// Fence and install every view, reload runtimes from the
    /// installed code, then move the position, adopt `adopt` (unless
    /// zero or an incarnation is already followed) and drop the
    /// journal in one transaction. Reinstalling views already
    /// installed at the same position is a no-op, so this is safe to
    /// repeat.
    fn finish_install(
        &self,
        engine: &mut Engine,
        last_index: u64,
        adopt: u64,
        file: &WireSnapshotFile,
    ) -> Result<(), FlowerError> {
        for db in &file.dbs {
            let meta = SnapshotMeta {
                db: db.meta.db.clone(),
                revision: db.meta.revision,
                applied_log: db.meta.applied_log,
                incarnation: db.meta.incarnation,
            };
            apply_snapshot(engine.store(), &meta, &db.payload).map_err(engine_err)?;
            // Modules the old rows deployed (one removed since, say)
            // must not outlive them, as on members.
            engine.forget_modules(&db.meta.db);
            self.reload_db(engine, &db.meta.db)?;
        }
        let write = engine.store().begin_write().map_err(engine_err)?;
        if adopt != 0 {
            flower_engine::replication::establish(&write, adopt).map_err(engine_err)?;
        }
        for db in &file.dbs {
            if db.meta.db != CLUSTER_DB {
                flower_engine::replication::register(&write, &db.meta.db).map_err(engine_err)?;
            }
        }
        let followed = write
            .meta_u64(SYNC_DB, SYNC_INCARNATION)
            .map_err(engine_err)?;
        let mut adopt = (followed == 0 && adopt != 0).then_some(adopt);
        record_position(&write, last_index, &mut adopt)?;
        write
            .delete_meta(SYNC_DB, SYNC_INSTALL)
            .map_err(engine_err)?;
        write.commit().map_err(engine_err)?;
        Ok(())
    }

    /// Reload every deployed module's runtime from installed code.
    fn reload_db(&self, engine: &mut Engine, db: &[u8]) -> Result<(), FlowerError> {
        let read = engine.store().begin_read().map_err(engine_err)?;
        if !flower_engine::placement::modules_ready(&read, db).map_err(engine_err)? {
            return Ok(());
        }
        let rows = read
            .scan_meta_prefix(db, CODE_IDENTITY_PREFIX)
            .map_err(engine_err)?;
        let mut modules = Vec::new();
        for (name, identity) in &rows {
            let module = String::from_utf8(name[CODE_IDENTITY_PREFIX.len()..].to_vec())
                .map_err(|_| FlowerError::Decode("bad module name".to_string()))?;
            let hash: [u8; 32] = identity.as_slice().try_into().map_err(|_| {
                FlowerError::Decode(format!(
                    "code identity for `{module}` has {} bytes, want 32",
                    identity.len()
                ))
            })?;
            let Some((wasm, manifest)) = read.get_code(db, &hash).map_err(engine_err)? else {
                return Err(FlowerError::Decode(format!("code missing for `{module}`")));
            };
            modules.push((module, wasm, manifest));
        }
        drop(read);
        for (module, wasm, manifest) in &modules {
            let value = parse_value(manifest).map_err(engine_err)?;
            let runtime =
                (self.build_runtime)(module, wasm, &value).map_err(FlowerError::Engine)?;
            engine
                .reload_module(db, module, runtime)
                .map_err(engine_err)?;
        }
        Ok(())
    }
}

/// Read the persisted sync position.
fn read_position(engine: &Engine) -> Result<u64, FlowerError> {
    let read = engine.store().begin_read().map_err(engine_err)?;
    read.meta_u64(SYNC_DB, SYNC_POS).map_err(engine_err)
}

/// Read the followed group incarnation, zero when none is adopted.
fn read_incarnation(engine: &Engine) -> Result<u64, FlowerError> {
    let read = engine.store().begin_read().map_err(engine_err)?;
    read.meta_u64(SYNC_DB, SYNC_INCARNATION).map_err(engine_err)
}

/// Record the sync position inside an open transaction; a pending
/// adoption commits with it, so the incarnation is adopted exactly
/// when the first position from that history lands.
fn record_position(
    write: &WriteTxn,
    position: u64,
    adopt: &mut Option<u64>,
) -> Result<(), FlowerError> {
    write
        .set_meta_u64(SYNC_DB, SYNC_POS, position)
        .map_err(engine_err)?;
    if let Some(incarnation) = adopt.take() {
        write
            .set_meta_u64(SYNC_DB, SYNC_INCARNATION, incarnation)
            .map_err(engine_err)?;
    }
    Ok(())
}

/// Advance the replica's copy of the server's expiry clock past an
/// entry's proposal stamp inside an open transaction, returning the
/// clock the entry reads receipt expiry at. Mirrors
/// `StateMachine::advance_expiry_clock` in flower-cluster: the largest
/// stamp applied so far, so verdicts match the server's.
fn advance_expiry_clock(write: &WriteTxn, proposed_ms: u64) -> Result<u64, FlowerError> {
    let clock = write
        .meta_u64(CLUSTER_DB, GROUP_EXPIRY_CLOCK)
        .map_err(engine_err)?
        .max(proposed_ms);
    write
        .set_meta_u64(CLUSTER_DB, GROUP_EXPIRY_CLOCK, clock)
        .map_err(engine_err)?;
    Ok(clock)
}

/// A batch's place in the history this replica follows.
enum Lineage {
    /// The followed history, or one the replica cannot tell apart:
    /// apply, adopting the incarnation given (if any) with the first
    /// position write.
    Apply(Option<u64>),
    /// The server has not established an incarnation while this
    /// replica follows one: apply nothing until it has.
    Wait,
}

/// Place a batch from incarnation `remote` against the followed
/// `local` before anything applies. Another non-zero incarnation is
/// a different history and refuses. A replica following none adopts
/// the server's only when `fresh`, holding nothing a different history
/// could have written (position zero, or a snapshot replacing it all).
fn lineage(local: u64, remote: u64, fresh: bool) -> Result<Lineage, FlowerError> {
    match (local, remote) {
        (0, 0) => Ok(Lineage::Apply(None)),
        (0, remote) => Ok(Lineage::Apply(fresh.then_some(remote))),
        (_, 0) => Ok(Lineage::Wait),
        (local, remote) if local == remote => Ok(Lineage::Apply(None)),
        (local, remote) => Err(FlowerError::HistoryChanged { local, remote }),
    }
}

/// Refuse a batch that does not continue from `position`: every
/// entry must sit strictly after it, in strictly increasing index
/// order, at or below the covered index. Nothing applies from a
/// refused batch, so the position never skips an entry.
fn check_batch(batch: &WireBatch, position: u64) -> Result<(), FlowerError> {
    batch.validate(position).map_err(FlowerError::Decode)
}

/// Decode and version-check a snapshot file.
fn decode_snapshot_file(bytes: &[u8]) -> Result<WireSnapshotFile, FlowerError> {
    let file: WireSnapshotFile = serde_json::from_slice(bytes)
        .map_err(|error| FlowerError::Decode(format!("snapshot file: {error}")))?;
    if file.version != SNAPSHOT_VERSION {
        return Err(FlowerError::Decode(format!(
            "snapshot version {} is not {SNAPSHOT_VERSION}",
            file.version
        )));
    }
    Ok(file)
}

/// Record the revision at which a patch's receipts arrived.
fn note_receipts(arrived: &mut ArrivedReceipts, patch: &Patch, revision: u64) {
    for (identity, _) in &patch.receipts {
        arrived.insert((patch.db.clone(), identity.clone()), revision);
    }
}

/// Settle submitted and acknowledged intents whose receipts are now
/// present locally: a recorded failure marks the intent failed,
/// anything else committed. The revision is the one at which the
/// receipt arrived in this batch; receipts that arrived inside a
/// snapshot, or before a crash cut an earlier reconciliation short,
/// report the database's synced revision.
fn reconcile_intents(engine: &mut Engine, arrived: &ArrivedReceipts) -> Result<(), FlowerError> {
    for intent in engine.list_intents(None).map_err(engine_err)? {
        if !matches!(
            intent.status,
            IntentStatus::Submitted | IntentStatus::Acknowledged
        ) {
            continue;
        }
        let identity = receipt_identity(&intent.identity)?;
        let read = engine.store().begin_read().map_err(engine_err)?;
        let Some(bytes) = read
            .get_receipt(&intent.db, &identity)
            .map_err(engine_err)?
        else {
            continue;
        };
        let revision = match arrived.get(&(intent.db.clone(), identity)) {
            Some(revision) => *revision,
            None => read
                .meta_u64(&intent.db, meta::REVISION)
                .map_err(engine_err)?,
        };
        drop(read);
        let next = match Receipt::decode(&bytes) {
            Ok(Receipt {
                result: Some(Outcome::Failure(data)),
                ..
            }) => {
                let mut failure = fail(data.code, data.message);
                failure.details = data
                    .details
                    .as_deref()
                    .and_then(|json| parse_value(json).ok());
                IntentStatus::Failed { failure }
            }
            // A stamped receipt says exactly which revision it committed.
            Ok(Receipt {
                committed: Some(committed),
                ..
            }) => IntentStatus::Committed {
                revision: committed.revision,
            },
            _ => IntentStatus::Committed { revision },
        };
        if intent.status == IntentStatus::Submitted
            && matches!(next, IntentStatus::Committed { .. })
        {
            engine
                .mark_intent(&intent.identity, IntentStatus::Acknowledged)
                .map_err(engine_err)?;
        }
        engine
            .mark_intent(&intent.identity, next)
            .map_err(engine_err)?;
    }
    Ok(())
}
