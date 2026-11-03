//! A cluster member: Raft core, engine, log store and client paths.
//!
//! Client writes queue on the leader's writer ([`crate::writer`]),
//! which prepares each without committing on the state the writes
//! before it staged, then proposes them in groups, one batch entry per
//! group; every member applies the committed entry in log order.
//! Queries read the way they declare
//! ([`crate::read`]): linearizable ones on any member once the leader's
//! lease, a quorum round or the leader's read fence proves the member
//! fresh; replica-local ones from the member's own state, within their
//! staleness bound if any. Writes a follower receives forward to the
//! leader's API through the member channel ([`crate::peer`]). Intent
//! intake is the mutate path with a
//! client-supplied retry identity: the leader's admission decides
//! replay, rejection or execution, and the committed receipt makes
//! redelivery idempotent. Queue claims and reports and timer
//! maintenance commit through Raft too (see [`crate::work`]), so every
//! member holds the same state.
//!
//! Engine work never runs on Tokio's async workers: every engine lock
//! moves its work to the blocking pool ([`crate::exec`]), so guest
//! execution and fsyncs cannot starve the Raft core. Queries never
//! take that lock: after their quorum round they evaluate on the
//! engine's read side against their own snapshots, in parallel with
//! each other, with prepares and with applies. Client work admits
//! first ([`crate::admission`]): bounded evaluation slots, byte
//! budgets and an explicit retryable overload.
//!
//! A disk failure under either file is permanent for the process (see
//! [`crate::health`]): from the first one, readiness fails, writes and
//! linearizable reads answer [`STORAGE_FAILED`] without touching the
//! store, and maintenance and compaction stop. A restart reopens the
//! files with every acknowledged write.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use flower_crypto::replicated::{ManagedKeys, Prepared as PreparedKeys};
use flower_engine::batch::BatchItem;
use flower_engine::engine::{
    CallOptions, Engine, EngineReader, FirePrepare, MultiStep, MutationOptions, PrepareOutcome,
};
use flower_engine::error::IO_FAILURE;
use flower_engine::json::{parse_value, render};
use flower_engine::query_cache::Lookup;
use flower_engine::retry::Committed;
use flower_engine::staging::StagingPhase;
use flower_engine::storage::compact::{CompactionSettings, CompactionStats, Compactor};
use flower_engine::storage::{GroupStore, meta};
use flower_engine::task::TaskPrepare;
use flower_sdk::abi::{decode_envelope, encode_err};
use flower_sdk::{Failure, Value};
use openraft::error::{CheckIsLeaderError, ClientWriteError, Fatal, RaftError};
use openraft::raft::{ClientWriteResponse, Raft};
use openraft::{BasicNode, Config};
use tokio::sync::Mutex as AsyncMutex;

use crate::admission::{
    Admission, AdmissionConfig, Class, Overload, REQUEST_OVERHEAD, deploy_bytes, input_bytes,
};
use crate::compaction;
use crate::compat;
use crate::exec::{now_ms, with_engine, with_reader};
use crate::flight::Flights;
use crate::flush::{ApplyDurability, FlushSettings};
use crate::fsync::{self, Durability};
use crate::group::GroupId;
use crate::health::{
    GROUP, RAFT, RAFT_LOG, STORAGE_FAILED, StorageFailure, StorageHealth, writable,
};
use crate::keys::{KEY_CATALOG, KeyCatalogCell, KeyCatalogs};
use crate::logstore::{LogStore, WeakLogStore};
use crate::methods::{self, Answer, MethodRef, Work};
use crate::network::{PeerConfig, PeerFactory, PeerService, Peers, serve_peer};
use crate::peer::MemberDesk;
use crate::read::{ReadOptions, Reads};
use crate::recovery::{RECOVERING, RECOVERY_WAIT, RecoveryFence};
use crate::rejoin::{self, Rejoin};
use crate::snapshot_policy::{Accounting, SnapshotPolicy, SnapshotPolicyStats, entry_bytes};
use crate::speculation::{Speculable, SpeculationSettings};
use crate::stages::{self, Stage};
use crate::statemachine::{CatalogChanges, RuntimeFactory, StateMachine};
use crate::sync::{SyncBatch, serve_since};
use crate::telemetry::{self, ObservedFactory, Observer, QueryStage};
use crate::types::{
    ClusterWrite, CommitNotice, NodeId, ResponseOutcome, TypeConfig, WriteResponse,
};
use crate::work::{QueueOp, QueueOutcome, TimerOp, TimerOutcome, prepare_queue, prepare_timer};
use crate::writer::{Prepared, Reporting, WriterHandle, WriterStats, Written};

pub(crate) mod leadership;
mod membership;
mod modules;
mod moves;
mod operator;
mod partitions;
mod reads;
mod retention;
mod txn;

pub use leadership::{ISOLATED, TRANSFER_FAILED, TRANSFERRING, TransferOutcome};
pub use membership::{MEMBERSHIP, MembershipChange, MembershipView};
pub use modules::{ModuleListing, PurgeStats};
pub use moves::{EXPORT_LOST, ExportDigest, ExportKind, ExportPage};
pub use operator::{
    COLLECT_PAGE, CollectionStats, DatabaseCatalog, GRAPH_PAGE_TIME, MAX_ADVANCE_RECORDS,
    MAX_GRAPH_ROOTS, OperatorCatalog, ReceiptClock, StagedDeployment,
};
pub use partitions::{NOT_ROOT, control_outcome};
use reads::{Form, Reply};
pub use retention::{
    COLLECT_DUTY_PERCENT, COLLECT_REST_MAX, DEFAULT_EPOCHS, DEFAULT_MAX_RECEIPTS,
    DEFAULT_RETRY_WINDOW_MS, RetentionStats, RetryStatus, default_rotation,
};
pub use txn::{LOCK_WAIT, PREPARE_PATIENCE};

/// Group file name inside a node's directory.
const GROUP_FILE: &str = "group.redb";
/// Raft file name inside a node's directory.
const RAFT_FILE: &str = "raft.redb";
/// Backfill records drained per staging call on deploy; the engine
/// lock is released between pages.
const DEPLOY_DRAIN: usize = 4096;
/// Longest one task pass keeps running due tasks in one database before
/// moving on: a task that always has work never starves the other databases.
const TASK_BURST: Duration = Duration::from_millis(50);
/// Least time between the starts of two task passes: a stream of writes wakes
/// the leader at most this often.
const TASK_FLOOR: Duration = Duration::from_millis(250);
/// Most units of one task one step of a task pass submits together.
const TASK_UNITS: usize = 16;
/// Most due timers one sweep pass fires per database.
const SWEEP_LIMIT: usize = 64;
/// Commit notices buffered for watch hubs.
const COMMIT_BUFFER: usize = 64;
/// Minimum gap between idempotency purges: hygiene runs rarely even
/// though the sweep ticks every second.
const IDEMPOTENCY_PURGE_EVERY: Duration = Duration::from_secs(600);
/// Most receipts one purge pass deletes per database.
const IDEMPOTENCY_PURGE_PAGE: usize = 4096;
/// Queue lock entries kept before idle ones are pruned.
const WORK_LOCKS_PRUNE: usize = 1024;
/// How often a failed member probes its directory for recovery.
const RECOVERY_PROBE_EVERY: Duration = Duration::from_secs(1);
/// Entries read per page while counting the bytes applied since the
/// snapshot on open.
const SNAPSHOT_SEED_PAGE: u64 = 1024;
/// The longest shutdown waits for the state machine to let go of the
/// files after the Raft core stopped.
const FILES_RELEASE_WAIT: Duration = Duration::from_secs(10);
/// The longest a replay waits for its original's receipt to commit,
/// when admission found it in the writer's staged state: one batch's
/// commit, with room.
const RECEIPT_COMMIT_WAIT: Duration = Duration::from_secs(5);

/// Whether a refusal says the database does not serve here right now:
/// its placement (frozen, moving, moved, retired) or a prepared
/// transaction's lock. Background work leaves such a database alone.
fn not_serving(code: &str) -> bool {
    use flower_engine::placement::{PARTITION_MOVED, PARTITION_MOVING, PARTITION_RETIRED};
    [
        PARTITION_MOVING,
        PARTITION_MOVED,
        PARTITION_RETIRED,
        flower_engine::txn::TXN_PREPARED,
    ]
    .contains(&code)
}

/// What a write's preparation decided without proposing anything.
pub(crate) enum Decided {
    /// Admission's answer: a refusal, or a decision on local state.
    Done(Result<Value, Failure>),
    /// A stored receipt's outcome, and where it committed when the
    /// receipt read records it.
    Replayed(Result<Value, Failure>, Option<Committed>),
}

/// Opens one of a member's redb files at a path, creating it when
/// missing. [`Node::open`] opens plain files; [`Node::open_with`] takes
/// another opener, such as one over a storage backend that injects disk
/// failures.
pub type FileOpener =
    Arc<dyn Fn(&Path) -> Result<redb::Database, redb::DatabaseError> + Send + Sync>;

/// One timer fire through consensus.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FireReport {
    /// Missing, dead or not due: proposed nothing.
    Skipped,
    /// The fire patch committed: effects plus deletion, atomically.
    Fired,
    /// A retry patch committed past a failed fire.
    Retried,
    /// The timer of a removed module was held for it.
    Held,
    /// State moved under the prepare: retry next tick.
    Stale,
}

/// One maintenance-task run through consensus.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TaskReport {
    /// Not due, backing off or no longer declared: proposed nothing.
    Skipped,
    /// The run committed, clearing any failure state.
    Ran,
    /// The run failed and rolled back: its error callback's patch, or
    /// its failure state, committed instead.
    Failed,
    /// State moved under the prepare: the next pass decides again.
    Stale,
}

/// One maintenance-task pass across replicated databases.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TaskSweepReport {
    /// Whether this member led the pass.
    pub leader: bool,
    /// Databases swept.
    pub dbs: usize,
    /// Runs committed.
    pub ran: u64,
    /// Runs that failed, their failures handled or recorded.
    pub failed: u64,
    /// Runs that went stale under their prepare.
    pub stale: u64,
    /// Runs skipped: no longer due when prepared, or a database not
    /// serving here.
    pub skipped: u64,
    /// The earliest time a task may be due next, epoch milliseconds:
    /// now when a database still had due work at the end of its burst.
    pub next_ms: Option<u64>,
}

/// One timer sweep pass across replicated databases.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SweepReport {
    /// Whether this member led the pass.
    pub leader: bool,
    /// Databases swept.
    pub dbs: usize,
    /// Timers fired.
    pub fired: u64,
    /// Timers advanced past failed fires.
    pub retried: u64,
    /// Timers of removed modules held for them.
    pub held: u64,
    /// Fires that went stale under their prepare.
    pub stale: u64,
    /// Timers skipped as missing, dead or not due.
    pub skipped: u64,
}

/// A member-facing error: engine failure, fatal Raft failure, or a
/// forward to the leader.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NodeError {
    /// Engine or storage failure with its code and message.
    Failure {
        /// Machine-readable failure code.
        code: String,
        /// Human-readable message.
        message: String,
    },
    /// A call's own failure carrying details (a declared error
    /// variant's payload), as its code returned it: the host's failures
    /// carry none, so a query failing this way failed in its code.
    Call {
        /// Machine-readable failure code.
        code: String,
        /// Human-readable message.
        message: String,
        /// The details, canonically encoded ([`Value::encode_binary`]).
        details: Vec<u8>,
    },
    /// Fatal Raft failure with its message.
    Fatal(String),
    /// Not the leader: retry at the leader when known.
    Forward {
        /// Known leader, if any.
        leader: Option<NodeId>,
    },
    /// Admission refused the request: the node is out of room for it
    /// now. Retry after the hint (see [`crate::admission`]).
    Overloaded {
        /// Why.
        message: String,
        /// Suggested wait before retrying, in milliseconds.
        retry_after_ms: u64,
    },
}

impl std::fmt::Display for NodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeError::Failure { code, message } | NodeError::Call { code, message, .. } => {
                write!(f, "{code}: {message}")
            }
            NodeError::Fatal(message) => write!(f, "fatal: {message}"),
            NodeError::Forward { leader } => write!(f, "forward to leader: {leader:?}"),
            NodeError::Overloaded { message, .. } => write!(f, "overloaded: {message}"),
        }
    }
}

impl From<Overload> for NodeError {
    fn from(overload: Overload) -> Self {
        NodeError::Overloaded {
            message: overload.message,
            retry_after_ms: overload.retry_after.as_millis() as u64,
        }
    }
}

impl std::error::Error for NodeError {}

impl NodeError {
    /// Failure from evaluating application code, explicitly distinguished from host failures.
    pub fn application(failure: Failure) -> Self {
        let details = match failure.details.unwrap_or(Value::Null).encode_binary() {
            Ok(details) => details,
            Err(error) => return Self::coded("invalid_failure_details", error.message),
        };
        Self::Call {
            code: failure.code,
            message: failure.message,
            details,
        }
    }
    /// A failure with a code.
    pub(crate) fn coded(code: &str, message: impl ToString) -> Self {
        NodeError::Failure {
            code: code.to_string(),
            message: message.to_string(),
        }
    }

    /// Wrap an engine failure, keeping the details of a call's own.
    pub fn failure(failure: Failure) -> Self {
        match failure.details.as_ref().map(Value::encode_binary) {
            Some(Ok(details)) => NodeError::Call {
                code: failure.code,
                message: failure.message,
                details,
            },
            _ => NodeError::Failure {
                code: failure.code,
                message: failure.message,
            },
        }
    }

    /// The call's own failure, details included, when it is one.
    pub fn call_failure(&self) -> Option<Failure> {
        let NodeError::Call {
            code,
            message,
            details,
        } = self
        else {
            return None;
        };
        let details = Value::decode_binary(details).ok()?;
        Some(Failure::with_details(
            code.clone(),
            message.clone(),
            details,
        ))
    }

    /// Wrap an engine-typed error.
    pub(crate) fn engine<E>(error: E) -> Self
    where
        Failure: From<E>,
    {
        Self::failure(Failure::from(error))
    }

    /// Wrap a client-write Raft error.
    pub(crate) fn write(error: RaftError<NodeId, ClientWriteError<NodeId, BasicNode>>) -> Self {
        match error {
            RaftError::APIError(ClientWriteError::ForwardToLeader(forward)) => NodeError::Forward {
                leader: forward.leader_id,
            },
            RaftError::APIError(ClientWriteError::ChangeMembershipError(error)) => {
                Self::coded("membership", error)
            }
            RaftError::Fatal(fatal) => NodeError::Fatal(fatal.to_string()),
        }
    }

    /// Wrap a linearizable-read Raft error: forwards stay forwards,
    /// quorum loss is retryable.
    pub(crate) fn linearizable(
        error: RaftError<NodeId, CheckIsLeaderError<NodeId, BasicNode>>,
    ) -> Self {
        match error {
            RaftError::APIError(CheckIsLeaderError::ForwardToLeader(forward)) => {
                NodeError::Forward {
                    leader: forward.leader_id,
                }
            }
            RaftError::APIError(CheckIsLeaderError::QuorumNotEnough(error)) => {
                Self::coded("quorum", error)
            }
            RaftError::Fatal(fatal) => NodeError::Fatal(fatal.to_string()),
        }
    }

    /// Wrap a membership-initialization Raft error.
    fn initialize<E: std::fmt::Display>(error: RaftError<NodeId, E>) -> Self {
        match error {
            RaftError::APIError(api) => Self::coded("initialize", api),
            RaftError::Fatal(fatal) => NodeError::Fatal(fatal.to_string()),
        }
    }
}

/// Record the Raft core stopping on its own (a storage error of either
/// file, or a panic) as the member's storage failure the moment it
/// happens, so maintenance and compaction stop before touching a failed
/// handle and the failure is logged without waiting for a probe. Ends
/// when the core is dropped; holds no file.
fn watch_raft(raft: &Raft<TypeConfig>, health: Arc<StorageHealth>) {
    let mut metrics = raft.metrics();
    tokio::spawn(async move {
        loop {
            let stopped = match &metrics.borrow_and_update().running_state {
                Err(Fatal::Stopped) | Ok(()) => None,
                Err(fatal) => Some(fatal.to_string()),
            };
            if let Some(error) = stopped {
                health.fail(RAFT, error);
                return;
            }
            if metrics.changed().await.is_err() {
                return;
            }
        }
    });
}

/// How often the apply flusher looks again while applies are durable.
const APPLY_FLUSH_IDLE: Duration = Duration::from_secs(1);

/// Make deferred applies durable every [`ApplyDurability::every`] (see
/// [`crate::flush`]): one durable commit of the group file, after the
/// log's, under the engine lock. A failed flush is the member's storage
/// failure: redb refuses every later transaction of the file, so the
/// next apply stops Raft too. Ends with the node, after a storage
/// failure, or when the engine is gone; holds the engine only to flush.
async fn run_apply_flusher(
    engine: std::sync::Weak<AsyncMutex<Engine>>,
    logs: WeakLogStore,
    applies: Arc<ApplyDurability>,
    health: Arc<StorageHealth>,
    mut alive: tokio::sync::watch::Receiver<()>,
) {
    loop {
        let wait = applies.every().unwrap_or(APPLY_FLUSH_IDLE);
        tokio::select! {
            changed = alive.changed() => {
                if changed.is_err() {
                    return;
                }
            }
            // A new interval counts from now.
            () = applies.changed.notified() => continue,
            () = tokio::time::sleep(wait) => {}
        }
        if health.failure().is_some() {
            return;
        }
        if !applies.pending() {
            continue;
        }
        // The log first, outside the engine lock: what the guard then
        // flushes under it is only what was appended meanwhile.
        if let Some(logs) = logs.upgrade() {
            let flushed = tokio::task::spawn_blocking(move || logs.flush()).await;
            if let Ok(Err(error)) = flushed {
                health.fail(RAFT_LOG, format!("flushing the log: {error}"));
                return;
            }
        }
        let Some(engine) = engine.upgrade() else {
            return;
        };
        let flushing = applies.clone();
        let flushed = with_engine(&engine, move |engine| flushing.flush(engine.store())).await;
        drop(engine);
        let error = match flushed {
            Ok(Ok(_)) => continue,
            Ok(Err(error)) => error.to_string(),
            Err(panic) => panic,
        };
        health.fail(GROUP, format!("making applied entries durable: {error}"));
        return;
    }
}

/// Run engine work off the async workers; a panicked task is fatal.
async fn engine_task<T, F>(engine: &Arc<AsyncMutex<Engine>>, work: F) -> Result<T, NodeError>
where
    T: Send + 'static,
    F: FnOnce(&mut Engine) -> Result<T, NodeError> + Send + 'static,
{
    with_engine(engine, work).await.map_err(NodeError::Fatal)?
}

/// How long a readiness probe waits, at most, for this member to apply
/// what it knew committed when the probe arrived (see
/// [`Node::readiness`]).
pub const READY_APPLY_WAIT: Duration = Duration::from_millis(250);

/// A member's readiness, as `/health/ready` reports it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Readiness {
    /// Ready to serve: a leader is known, this member applied every
    /// entry it knew committed when asked (within [`READY_APPLY_WAIT`]),
    /// storage answers and hasn't failed, and it is not rejoining after
    /// opening without Raft state.
    pub ready: bool,
    /// Whether this member opened without Raft state and has not caught
    /// up with its group yet (see [`crate::rejoin`]).
    pub rejoining: bool,
    /// The storage failure that stopped this member, if any: it stays
    /// unready until the process restarts.
    pub storage: Option<StorageFailure>,
    /// Why not, when not ready.
    pub reason: Option<String>,
    /// Current leader, if known.
    pub leader: Option<NodeId>,
    /// Highest committed index this member knows.
    pub committed: Option<u64>,
    /// Highest applied index.
    pub applied: Option<u64>,
}

/// One deployment's claim on its module (see
/// [`Node::claim_deployment`]), released on drop.
struct Deploying<'a> {
    node: &'a Node,
    key: (Vec<u8>, String),
}

impl Drop for Deploying<'_> {
    fn drop(&mut self) {
        self.node
            .deploying
            .lock()
            .expect("deploy claims poisoned")
            .remove(&self.key);
    }
}

/// A cluster member.
pub struct Node {
    id: NodeId,
    /// The group this member belongs to.
    group: GroupId,
    raft: Raft<TypeConfig>,
    engine: Arc<AsyncMutex<Engine>>,
    /// The engine's read side: queries and refreshes run on it without
    /// the engine lock.
    reader: EngineReader,
    /// Admission for reads and control work: node-wide, shared by every
    /// group a process hosts.
    admission: Admission,
    /// Admission for this group's writes: its writer's places, byte
    /// budget and CoDel. The same controller as `admission` for a lone
    /// group; the group's own in a host, so one group's writers never
    /// take another's places.
    writers: Admission,
    logs: LogStore,
    commits: tokio::sync::broadcast::Sender<CommitNotice>,
    last_idempotency_purge: std::sync::Mutex<Instant>,
    /// Per-queue (and per-database timer) operation locks: each queue
    /// operation holds its lock from prepare through commit.
    work_locks: std::sync::Mutex<HashMap<Vec<u8>, Arc<AsyncMutex<()>>>>,
    /// `(database, module)` pairs with a deployment in flight on this
    /// member: one at a time per module, and a staging no deployment
    /// here drives is stale.
    deploying: std::sync::Mutex<HashSet<(Vec<u8>, String)>>,
    /// Keeps this member's intern allocations apart: a one-call
    /// deployment holds it shared from its local staging until its
    /// activation applies, a replicated staging step exclusively from
    /// its preparation until it applies. The step's bindings take IDs
    /// past the intern counter it read, which a local staging would
    /// otherwise take too.
    interning: tokio::sync::RwLock<()>,
    /// The leader's pacing of collection of retired structures.
    collection_clocks: operator::CollectionClocksLock,
    /// Runtime factory: deploys validate a module with it before
    /// anything is staged or proposed.
    runtimes: Arc<dyn RuntimeFactory>,
    /// The node directory, which recovery probes.
    dir: PathBuf,
    /// The first storage failure of either file.
    health: Arc<StorageHealth>,
    /// The group file's compactor; the Raft file's is the log store's.
    group_compactor: Option<Compactor>,
    /// The applied group incarnation as the state machine publishes
    /// it: sync serving and maintenance read it without the engine.
    incarnation: Arc<AtomicU64>,
    /// Whether this member is catching up after opening without Raft
    /// state.
    rejoin: Arc<Rejoin>,
    /// Flower's own peer requests over the Raft transport.
    peers: Peers,
    /// The leadership-transfer gate and when this member began to lead.
    leadership: Arc<leadership::Leadership>,
    /// Serializes membership changes and leadership transfers.
    membership_ops: AsyncMutex<()>,
    /// Closes when the node drops, ending its background tasks.
    _lifetime: tokio::sync::watch::Sender<()>,
    /// The rejoin catch-up task, which holds the Raft file while it runs.
    catch_up: Option<tokio::task::JoinHandle<()>>,
    /// Fails once the state machine and its snapshot builders dropped
    /// their file handles (see [`StateMachine::files_released`]).
    files_released: tokio::sync::watch::Receiver<()>,
    /// Applied bytes and snapshot builds, which the soft snapshot
    /// triggers read (see [`crate::snapshot_policy`]).
    accounting: Arc<Accounting>,
    /// The startup serving fence, when this member opened with its log
    /// past its state (see [`crate::recovery`]).
    recovery: Option<Arc<RecoveryFence>>,
    /// Files opened with fsync off, which shutdown syncs and unmarks.
    unsynced: Vec<PathBuf>,
    /// Fresh-read proofs: lease, rounds, fences, staleness.
    reads: Arc<Reads>,
    /// This member's side of the member channel.
    desk: Arc<MemberDesk>,
    /// The leader's advertised API URL, as last described.
    leader_url: reads::LeaderUrl,
    /// Identical queries in flight (see [`crate::flight`]).
    flights: Arc<Flights>,
    /// The applied managed-key catalog and the runtime it feeds.
    keys: Arc<KeyCatalogCell>,
    /// Counts changes to what operators see of the catalog (see
    /// [`CatalogChanges`]).
    catalog: Arc<CatalogChanges>,
    /// The leader's write queue: every client write prepares and
    /// commits through it (see [`crate::writer`]).
    writer: WriterHandle,
    /// The writer task, which holds the engine until it ends.
    writer_task: Option<tokio::task::JoinHandle<()>>,
    /// The leader's retention rotation and collection bookkeeping.
    retention_clocks: retention::ClocksLock,
    /// The retry window the leader gives databases nobody configured
    /// retention for, in milliseconds; zero gives none (see
    /// [`Node::set_retry_window`]).
    retry_window_ms: AtomicU64,
    /// The leader's pacing of removed modules' purges.
    purge_clocks: modules::ClocksLock,
    /// Reads this leader pinned as moves' bases, by database and move
    /// (see [`crate::moves`]).
    exports: moves::Pins,
    /// The group's instruments (see [`crate::telemetry`]).
    observer: Arc<Observer>,
    /// Deferred applies and their flushes (see [`crate::flush`]).
    applies: Arc<ApplyDurability>,
    /// The task flushing deferred applies, which shutdown awaits.
    apply_flusher: Option<tokio::task::JoinHandle<()>>,
}

impl Node {
    /// Open or create a node in `dir`: recover the engine, finish any
    /// interrupted snapshot install, open the Raft log and start the
    /// core. The caller registers the Raft handle for routing and
    /// initializes membership. A directory without Raft state opens
    /// rejoining (see [`crate::rejoin`]).
    pub async fn open<N>(
        id: NodeId,
        dir: &Path,
        config: Arc<Config>,
        network: N,
        runtimes: Arc<dyn RuntimeFactory>,
    ) -> Result<Self, NodeError>
    where
        N: PeerFactory,
    {
        let files: FileOpener = Arc::new(|path| redb::Database::create(path));
        Self::open_with(id, dir, config, network, runtimes, files).await
    }

    /// [`Node::open`] with fsync on or off (`FLOWER_FSYNC`, see
    /// [`crate::fsync`]): with it off, the group and Raft files and the
    /// snapshot images are never synced, each file is marked as possibly
    /// unsynced while open, and [`Node::shutdown`] syncs everything and
    /// removes the markers. Either way, a member whose files an earlier
    /// boot left unsynced opens rejoining (see [`crate::rejoin`]) and
    /// records them as this boot's once that is durable.
    pub async fn open_with_durability<N>(
        id: NodeId,
        dir: &Path,
        config: Arc<Config>,
        network: N,
        runtimes: Arc<dyn RuntimeFactory>,
        durability: Durability,
    ) -> Result<Self, NodeError>
    where
        N: PeerFactory,
    {
        let synced = durability.fsync;
        // A file an earlier boot left unsynced may lack writes this member
        // acknowledged: it rejoins its group (see `crate::fsync`).
        let mut lost = false;
        for file in [GROUP_FILE, RAFT_FILE] {
            let path = dir.join(file);
            if fsync::lost_writes(&path, &durability)
                .map_err(|error| NodeError::coded("io", error))?
            {
                tracing::warn!(
                    target: "flower::storage",
                    file = %path.display(),
                    "the machine restarted with unsynced writes in this file: rejoining the group"
                );
                lost = true;
            }
        }
        let opener = durability.clone();
        let files: FileOpener = Arc::new(move |path| fsync::open(path, &opener));
        let mut node = Self::open_files(id, dir, config, network, runtimes, files, lost).await?;
        if lost {
            // The rejoining mark is in the Raft file, unsynced with fsync
            // off: make it durable before the markers that caught the loss
            // go, so no crash in between makes this an ordinary voter.
            let dir = dir.to_path_buf();
            tokio::task::spawn_blocking(move || {
                fsync::sync(&dir.join(RAFT_FILE))?;
                [GROUP_FILE, RAFT_FILE]
                    .iter()
                    .try_for_each(|file| fsync::resettle(&dir.join(file), &durability))
            })
            .await
            .map_err(|error| NodeError::Fatal(error.to_string()))?
            .map_err(|error| NodeError::coded("io", error))?;
        }
        if !synced {
            node.logs.set_sync_images(false);
            node.unsynced = vec![dir.join(GROUP_FILE), dir.join(RAFT_FILE)];
        }
        Ok(node)
    }

    /// [`Node::open`], opening the group and Raft files through `files`.
    pub async fn open_with<N>(
        id: NodeId,
        dir: &Path,
        config: Arc<Config>,
        network: N,
        runtimes: Arc<dyn RuntimeFactory>,
        files: FileOpener,
    ) -> Result<Self, NodeError>
    where
        N: PeerFactory,
    {
        Self::open_files(id, dir, config, network, runtimes, files, false).await
    }

    /// [`Node::open_with`], rejoining whatever the files hold when they
    /// `lost` writes.
    async fn open_files<N>(
        id: NodeId,
        dir: &Path,
        config: Arc<Config>,
        network: N,
        runtimes: Arc<dyn RuntimeFactory>,
        files: FileOpener,
        lost: bool,
    ) -> Result<Self, NodeError>
    where
        N: PeerFactory,
    {
        let dir = dir.to_path_buf();
        // Every runtime the member builds reports its invocations to the
        // group's instruments.
        let observer = telemetry::observer(&network.group().name);
        let runtimes: Arc<dyn RuntimeFactory> =
            Arc::new(ObservedFactory::new(runtimes, observer.clone()));
        let recovery_dir = dir.clone();
        let recovery_runtimes = runtimes.clone();
        let flush = FlushSettings::default();
        let applies = Arc::new(ApplyDurability::new(flush.apply_every));
        let hooked = applies.clone();
        let (engine, logs, amnesiac, lost_tail, accounting, opened) =
            tokio::task::spawn_blocking(move || {
                let (engine, logs, amnesiac, lost_tail) =
                    Self::recover_storage(id, &recovery_dir, &recovery_runtimes, &files, lost)?;
                // No applied entry is durable in the group file before it is
                // in the log: the leader's own appends may be unflushed.
                let log = logs.downgrade();
                engine.store().set_before_durable(Arc::new(move || {
                    log.upgrade().map_or(Ok(()), |logs| {
                        logs.flush_through(hooked.applying_index())
                            .map_err(|error| flower_engine::error::Error::Io(error.to_string()))
                    })
                }));
                let accounting = Self::snapshot_accounting(id, &engine, &logs)?;
                let opened = Self::opened_positions(&engine, &logs)?;
                Ok::<_, NodeError>((engine, logs, amnesiac, lost_tail, accounting, opened))
            })
            .await
            .map_err(|error| NodeError::Fatal(error.to_string()))??;
        logs.set_leader_flush(flush.leader);
        logs.set_observer(observer.clone());
        // A rejoining member starts no election until it caught up.
        let config = if amnesiac {
            let mut config = (*config).clone();
            config.enable_elect = false;
            Arc::new(config)
        } else {
            config
        };
        let peers = network.peers();
        let group = network.group();
        let group_compactor = engine.store().compactor();
        let reader = engine.reader();
        let engine = Arc::new(AsyncMutex::new(engine));
        let (commits, _) = tokio::sync::broadcast::channel(COMMIT_BUFFER);
        let accounting = Arc::new(accounting);
        let keys = Arc::new(KeyCatalogCell::default());
        let catalog = Arc::new(CatalogChanges::default());
        let machine = StateMachine::new(
            engine.clone(),
            logs.clone(),
            runtimes.clone(),
            commits.clone(),
        )
        .with_accounting(accounting.clone())
        .with_key_catalog(keys.clone())
        .with_catalog_changes(catalog.clone())
        .with_apply_durability(applies.clone());
        let incarnation = machine.incarnation_handle();
        let files_released = machine.files_released();
        // OpenRaft re-applies the remembered committed prefix in here.
        let raft = Raft::new(id, config.clone(), network, logs.clone(), machine)
            .await
            .map_err(|error| NodeError::Fatal(error.to_string()))?;
        let health = Arc::new(StorageHealth::default());
        watch_raft(&raft, health.clone());
        let (tail, floor) = opened;
        let recovery = if lost_tail {
            Some(RecoveryFence::after_lost_tail(tail, floor))
        } else {
            RecoveryFence::new(tail, floor)
        };
        if let Some(fence) = &recovery {
            tracing::info!(target: "flower::storage", reason = %fence.reason(), "opened");
            fence.watch(raft.clone());
        }
        let (lifetime, alive) = tokio::sync::watch::channel(());
        let apply_flusher = tokio::spawn(run_apply_flusher(
            Arc::downgrade(&engine),
            logs.downgrade(),
            applies.clone(),
            health.clone(),
            lifetime.subscribe(),
        ));
        let rejoin = Rejoin::new(amnesiac);
        let catch_up = amnesiac.then(|| {
            tracing::warn!(
                id,
                "opened without Raft state: voting and serving once caught up with the group"
            );
            rejoin::drive(
                id,
                raft.clone(),
                rejoin.clone(),
                logs.clone(),
                peers.clone(),
                alive,
            )
        });
        let leadership = Arc::new(leadership::Leadership::default());
        let reads = Arc::new(Reads::new(id, raft.clone(), peers.clone(), &config));
        // A leadership restored from the files confirms no read lease:
        // before the restart it may have asked voters to release their
        // leases on it for a transfer, which they remember and nothing
        // persists (`leadership.rs`). It reads through quorum rounds
        // until it leads a later term. A core that already stopped serves
        // no reads at all.
        if let Ok(restored) = raft.with_raft_state(|state| *state.vote_ref()).await
            && restored.is_committed()
            && restored.leader_id().voted_for() == Some(id)
        {
            reads.surrender_term(restored.leader_id().get_term());
        }
        let desk = Arc::new(MemberDesk::new(reads.clone()));
        let (writer, writer_task) = WriterHandle::spawn(
            engine.clone(),
            raft.clone(),
            leadership.clone(),
            health.clone(),
            Reporting {
                marks: logs.commit_marks(),
                observer: observer.clone(),
            },
        );
        let admission = Admission::default();
        Ok(Node {
            id,
            group,
            raft,
            engine,
            reader,
            writers: admission.clone(),
            admission,
            logs,
            commits,
            last_idempotency_purge: std::sync::Mutex::new(Instant::now()),
            work_locks: std::sync::Mutex::new(HashMap::new()),
            deploying: std::sync::Mutex::new(HashSet::new()),
            interning: tokio::sync::RwLock::new(()),
            collection_clocks: Default::default(),
            runtimes,
            dir,
            health,
            group_compactor,
            incarnation,
            rejoin,
            peers,
            leadership,
            membership_ops: AsyncMutex::new(()),
            _lifetime: lifetime,
            catch_up,
            files_released,
            accounting,
            recovery,
            unsynced: Vec::new(),
            reads,
            desk,
            leader_url: reads::LeaderUrl::default(),
            flights: Arc::default(),
            keys,
            catalog,
            writer,
            writer_task: Some(writer_task),
            retention_clocks: Default::default(),
            retry_window_ms: AtomicU64::new(0),
            purge_clocks: Default::default(),
            exports: Default::default(),
            observer,
            applies,
            apply_flusher: Some(apply_flusher),
        })
    }

    /// The group's instruments on this member (see [`crate::telemetry`]).
    pub fn observer(&self) -> &Arc<Observer> {
        &self.observer
    }

    /// Flush this member's log and group file as `settings` say (see
    /// [`crate::flush`]); a member opens with the defaults.
    pub fn with_flush(self, settings: FlushSettings) -> Self {
        self.logs.set_leader_flush(settings.leader);
        self.applies.set(settings.apply_every);
        self
    }

    /// This member's deferred applies: whether applies defer, and their
    /// flushes.
    pub fn apply_durability(&self) -> &Arc<ApplyDurability> {
        &self.applies
    }

    /// Make every applied entry durable in the group file now, after
    /// the log: what a crash would otherwise replay from the log.
    pub async fn flush_applies(&self) -> Result<bool, NodeError> {
        let applies = self.applies.clone();
        self.with_engine(move |engine| applies.flush(engine.store()).map_err(NodeError::engine))
            .await
    }

    /// The last log index and the state OpenRaft will have restored at
    /// open: the applied position, raised to the committed index the log
    /// store remembers, which OpenRaft re-applies to before starting.
    fn opened_positions(
        engine: &Engine,
        logs: &LogStore,
    ) -> Result<(Option<u64>, Option<u64>), NodeError> {
        let raft_log = |error| NodeError::coded("raft_log", error);
        let applied = StateMachine::group_applied(engine)
            .map_err(NodeError::failure)?
            .map(|id| id.index);
        let committed = logs.committed().map_err(raft_log)?.map(|id| id.index);
        Ok((
            logs.last_log_index().map_err(raft_log)?,
            applied.max(committed),
        ))
    }

    /// Seed the snapshot scheduler's accounting from what this member
    /// applied since its snapshot, read back from the retained log, so a
    /// restart doesn't reset the byte count.
    fn snapshot_accounting(
        id: NodeId,
        engine: &Engine,
        logs: &LogStore,
    ) -> Result<Accounting, NodeError> {
        let applied = StateMachine::group_applied(engine)
            .map_err(NodeError::failure)?
            .map(|id| id.index);
        let snapshot = logs
            .snapshot_meta()
            .map_err(|error| NodeError::coded("raft_log", error))?
            .and_then(|meta| meta.last_log_id)
            .map(|id| id.index);
        let mut bytes = 0u64;
        let mut next = snapshot.map_or(0, |index| index + 1);
        let end = applied.map_or(0, |index| index + 1);
        while next < end {
            let page = logs
                .read_entries(next, end.min(next.saturating_add(SNAPSHOT_SEED_PAGE)))
                .map_err(|error| NodeError::coded("raft_log", error))?;
            let Some(last) = page.last() else {
                break;
            };
            next = last.log_id.index + 1;
            bytes = bytes.saturating_add(page.iter().map(entry_bytes).sum());
        }
        Ok(Accounting::new(id, bytes, applied, snapshot))
    }

    /// Open the group and Raft files and bring the engine to a
    /// consistent applied state before Raft reads it. Opening a file a
    /// failed process left behind recovers it to its last commit. Also
    /// whether the member is rejoining: a Raft file without any Raft
    /// state gets the durable mark before the core starts.
    fn recover_storage(
        id: NodeId,
        dir: &Path,
        runtimes: &Arc<dyn RuntimeFactory>,
        files: &FileOpener,
        lost: bool,
    ) -> Result<(Engine, LogStore, bool, bool), NodeError> {
        std::fs::create_dir_all(dir).map_err(|error| NodeError::coded("io", error))?;
        let group_path = dir.join(GROUP_FILE);
        let group = files(&group_path).map_err(|error| NodeError::coded("io", error))?;
        let store =
            GroupStore::from_database(group, Some(&group_path)).map_err(NodeError::engine)?;
        // Files another contract wrote never open (see `crate::compat`).
        let claimed = store
            .claim_contract(compat::storage_contract())
            .map_err(NodeError::engine)?;
        compat::check_store(&group_path, claimed)
            .map_err(|message| NodeError::coded(compat::INCOMPATIBLE_STORE, message))?;
        let mut engine = Engine::new(store);
        StateMachine::recover(&mut engine, runtimes)
            .map_err(|message| NodeError::coded("recover", message))?;
        let raft_path = dir.join(RAFT_FILE);
        if !raft_path.exists() {
            Self::reset_applied_positions(&engine)?;
        }
        let raft = files(&raft_path).map_err(|error| NodeError::coded("raft_log", error))?;
        let logs = LogStore::from_database(raft, Some(&raft_path))
            .map_err(|error| NodeError::coded("raft_log", error))?;
        let claimed = logs
            .claim_contract(compat::storage_contract())
            .map_err(|error| NodeError::coded("raft_log", error))?;
        compat::check_store(&raft_path, claimed)
            .map_err(|message| NodeError::coded(compat::INCOMPATIBLE_STORE, message))?;
        StateMachine::resume_install(&mut engine, runtimes, &logs)
            .map_err(|error| NodeError::coded("install", error))?;
        let raft_log = |error| NodeError::coded("raft_log", error);
        let amnesiac = lost
            || logs.rejoining().map_err(raft_log)?
            || !logs.has_raft_state().map_err(raft_log)?;
        if amnesiac {
            logs.set_rejoining(true).map_err(raft_log)?;
            logs.uncommit_vote().map_err(raft_log)?;
        }
        // A leader defers its own log flush (see `crate::flush`), so after
        // a crash its log may lack entries of its term that its followers
        // hold, and resuming that term would append other entries under
        // their log IDs. Unless it shut down with its log flushed, its own
        // vote comes back uncommitted: it must win a later term, which
        // only a log holding every committed entry can.
        let lost_tail = !logs.take_clean_shutdown().map_err(raft_log)?
            && logs.uncommit_own_vote(id).map_err(raft_log)?;
        Ok((engine, logs, amnesiac, lost_tail))
    }

    /// Reset per-database applied-log positions when opening over a
    /// fresh Raft log: a restored engine carries positions from its
    /// old history while the new log's indices restart at one, so
    /// the first writes would collide with replayed positions. A new
    /// log is a new history: positions restart at zero while revisions
    /// and data stay, and the incarnation clears so the new leader
    /// establishes a fresh one (see [`StateMachine::restart_history`]).
    /// No-op on a fresh engine (no databases registered) and never
    /// runs on restart, where the log file already exists.
    fn reset_applied_positions(engine: &Engine) -> Result<(), NodeError> {
        let write = engine.store().begin_write().map_err(NodeError::engine)?;
        StateMachine::restart_history(&write).map_err(NodeError::failure)?;
        write.commit().map_err(NodeError::engine)?;
        Ok(())
    }

    /// Initialize membership with default nodes. Safe to call on
    /// one member; every member initializes with the same set. The
    /// member founds a cluster: it is not rejoining one.
    pub async fn initialize(&self, members: BTreeSet<NodeId>) -> Result<(), NodeError> {
        self.raft
            .initialize(members)
            .await
            .map_err(NodeError::initialize)?;
        self.rejoin.settle(&self.raft, &self.logs).await;
        Ok(())
    }

    /// Initialize membership with explicit nodes, carrying the
    /// addresses TCP dials. Same safety as [`Node::initialize`].
    pub async fn initialize_nodes(
        &self,
        members: BTreeMap<NodeId, BasicNode>,
    ) -> Result<(), NodeError> {
        self.raft
            .initialize(members)
            .await
            .map_err(NodeError::initialize)?;
        self.rejoin.settle(&self.raft, &self.logs).await;
        Ok(())
    }

    /// Run engine work on this node's engine, off the async workers. A
    /// disk failure under the group file it reports becomes this
    /// member's storage failure.
    pub async fn with_engine<T, F>(&self, work: F) -> Result<T, NodeError>
    where
        T: Send + 'static,
        F: FnOnce(&mut Engine) -> Result<T, NodeError> + Send + 'static,
    {
        let result = engine_task(&self.engine, work).await;
        if let Err(NodeError::Failure { code, message }) = &result
            && code == IO_FAILURE
        {
            self.health.fail(GROUP, message);
        }
        result
    }

    /// Run read-only work on this node's engine reader, off the async
    /// workers and without the engine lock. Callers that linearize do
    /// so first; callers serving clients admit first. A disk failure it
    /// reports becomes this member's storage failure.
    pub async fn with_reader<T, F>(&self, work: F) -> Result<T, NodeError>
    where
        T: Send + 'static,
        F: FnOnce(&EngineReader) -> Result<T, NodeError> + Send + 'static,
    {
        let result = with_reader(&self.reader, work)
            .await
            .map_err(NodeError::Fatal)?;
        if let Err(NodeError::Failure { code, message }) = &result
            && code == IO_FAILURE
        {
            self.health.fail(GROUP, message);
        }
        result
    }

    /// Replace the admission sizing. Call before serving: requests
    /// already admitted keep the controller they were admitted by.
    pub fn with_admission(mut self, config: AdmissionConfig) -> Self {
        self.admission = Admission::new(config);
        self.writers = self.admission.clone();
        self
    }

    /// Admit reads and control work through a controller shared with the
    /// process's other groups, and this group's writes through one of
    /// their own sized by `writers` (see [`crate::host`]). Call before
    /// serving.
    pub fn with_shared_admission(mut self, shared: Admission, writers: AdmissionConfig) -> Self {
        self.admission = shared;
        self.writers = Admission::new(writers);
        self
    }

    /// The node-wide admission controller, for callers admitting their
    /// own work (watch refreshes) and for metrics.
    pub fn admission(&self) -> &Admission {
        &self.admission
    }

    /// The controller admitting this group's writes: the node-wide one
    /// for a lone group (see [`Node::with_shared_admission`]).
    pub fn writer_admission(&self) -> &Admission {
        &self.writers
    }

    /// The group this member belongs to.
    pub fn group(&self) -> &GroupId {
        &self.group
    }

    /// This member's client of its group's member channel.
    pub fn peer_client(&self) -> &Peers {
        &self.peers
    }

    /// The engine's read side.
    pub fn reader(&self) -> &EngineReader {
        &self.reader
    }

    /// The storage failure that stopped this member, if any: the first
    /// disk failure of either file, or a Raft core that stopped on its
    /// own (OpenRaft stops on any storage error, and on a panic).
    pub fn storage_failure(&self) -> Option<StorageFailure> {
        if let Some(failure) = self.health.failure() {
            return Some(failure.clone());
        }
        let stopped = match &self.raft.metrics().borrow().running_state {
            Err(Fatal::Stopped) | Ok(()) => None,
            Err(fatal) => Some(fatal.to_string()),
        };
        stopped.map(|error| self.health.fail(RAFT, error).clone())
    }

    /// Refuse work that would touch storage after a storage failure.
    pub(crate) fn check_storage(&self) -> Result<(), NodeError> {
        match self.storage_failure() {
            Some(failure) => Err(NodeError::coded(STORAGE_FAILED, failure)),
            None => Ok(()),
        }
    }

    /// A prepare's business failure, unless the disk failed under it.
    fn business<T>(&self, failure: Failure) -> Result<Result<T, Failure>, NodeError> {
        if failure.code == IO_FAILURE {
            self.health.fail(GROUP, &failure.message);
            return Err(NodeError::failure(failure));
        }
        Ok(Err(failure))
    }

    /// Propose one write and wait for its committed response. Refused
    /// with [`TRANSFERRING`] while leadership moves.
    async fn propose(&self, write: ClusterWrite) -> Result<WriteResponse, NodeError> {
        self.check_storage()?;
        let _admitted = self.leadership.admit().await?;
        let submitted = Instant::now();
        let response = self.raft.client_write(write).await;
        stages::add(Stage::Commit, submitted.elapsed());
        let response = response.map_err(NodeError::write)?;
        stages::add_commit(
            &self
                .logs
                .commit_marks()
                .phases(response.log_id.index, submitted),
        );
        Ok(response.data)
    }

    /// Run a mutation through consensus: queue it on the writer, which
    /// prepares it on what the writes queued before it staged and
    /// commits it in a batch (see [`crate::writer`]); return its
    /// committed response. Admission short-circuits answer without
    /// replicating, once everything they may have read committed.
    pub async fn mutate(
        &self,
        db: &[u8],
        module: &str,
        name: &str,
        args: &Value,
        options: &MutationOptions,
    ) -> Result<WriteResponse, NodeError> {
        // A database a prepared cross-group transaction holds takes the
        // write once it finishes: wait for that, within the deadline.
        let started = Instant::now();
        loop {
            let outcome = self.mutate_once(db, module, name, args, options).await;
            if !txn::locked_out(&outcome) || started.elapsed() >= txn::LOCK_WAIT {
                return outcome;
            }
            self.wait_for_lock(db).await;
        }
    }

    /// One attempt of [`Node::mutate`].
    async fn mutate_once(
        &self,
        db: &[u8],
        module: &str,
        name: &str,
        args: &Value,
        options: &MutationOptions,
    ) -> Result<WriteResponse, NodeError> {
        self.check_storage()?;
        self.forward_unless_leading()?;
        let method = MethodRef { db, module, name };
        // The writer slot is a place in the writer's queue, with its
        // bytes, until the write is answered.
        let queued = match self
            .writer_place(self.writers.acquire(Class::Writer, db, input_bytes(args)))
            .await
        {
            Ok(queued) => queued,
            Err(refused) => {
                if let Some(why) = methods::Refusal::of(&refused.message) {
                    methods::mutation_refused(method, why);
                }
                return Err(refused.into());
            }
        };
        let (db_owned, module_owned, name_owned, args, prepare_options) = (
            db.to_vec(),
            module.to_string(),
            name.to_string(),
            args.clone(),
            options.clone(),
        );
        let submitted = Instant::now();
        let call = Speculable {
            db: db.to_vec(),
            module: module.to_string(),
            name: name.to_string(),
            args: args.clone(),
        };
        let written = self
            .writer
            .submit_mutation(Some(queued), call, move |engine| {
                let work = Work::start();
                let prepared = engine.prepare_mutation(
                    &db_owned,
                    &module_owned,
                    &name_owned,
                    &args,
                    &prepare_options,
                );
                // Each evaluation counts, every attempt's.
                if let Ok(PrepareOutcome::Patch(_)) = &prepared {
                    let (wall, cpu) = work.elapsed();
                    let method = MethodRef {
                        db: &db_owned,
                        module: &module_owned,
                        name: &name_owned,
                    };
                    methods::mutation_evaluated(method, wall, cpu);
                }
                Ok(match prepared.map_err(NodeError::failure)? {
                    PrepareOutcome::Patch(prepared) => Prepared::Propose(
                        BatchItem::Patch {
                            db: db_owned.clone(),
                            patch: prepared.patch,
                        },
                        None,
                    ),
                    PrepareOutcome::Done(done) => Prepared::Answer(Some(Decided::Done(done))),
                    PrepareOutcome::Replayed(done, committed) => {
                        Prepared::Answer(Some(Decided::Replayed(done, committed)))
                    }
                })
            })
            .await;
        let (answered, waited) = match written {
            Ok(written) => {
                let waited = written.queued;
                (self.written(db, options, written).await, Some(waited))
            }
            Err(error) => {
                if let Some(why) = methods::Refusal::of_error(&error) {
                    methods::mutation_refused(method, why);
                }
                (Err(error), None)
            }
        };
        // Its wait and its answer, under one lock.
        if let Some(answer) = Answer::of(&answered) {
            methods::mutation_finished(method, waited, answer, submitted.elapsed());
        }
        answered
    }

    /// Run a multi-database transaction through consensus: queue it on
    /// the writer, which prepares every step on the staged state and
    /// commits the transaction as one item of a batch, all-or-nothing;
    /// return its committed response. Admission short-circuits and step
    /// failures answer without replicating.
    pub async fn transaction(
        &self,
        steps: &[MultiStep],
        options: &MutationOptions,
    ) -> Result<WriteResponse, NodeError> {
        // Linear, before the writer or any hook sees the list.
        flower_engine::engine::check_transaction_steps(steps).map_err(NodeError::failure)?;
        let started = Instant::now();
        loop {
            let outcome = self.transaction_once(steps, options).await;
            if !txn::locked_out(&outcome) || started.elapsed() >= txn::LOCK_WAIT {
                return outcome;
            }
            let dbs: Vec<_> = steps
                .iter()
                .map(|step| flower_engine::txn::PlacedDb {
                    db: String::from_utf8_lossy(&step.db).into_owned(),
                    epoch: 0,
                    steps: Vec::new(),
                })
                .collect();
            let at_most = crate::deadline::remaining()
                .map_or(txn::LOCK_WAIT, |left| left.min(txn::LOCK_WAIT));
            self.wait_for_locks(&dbs, at_most).await;
        }
    }

    /// One attempt of [`Node::transaction`].
    async fn transaction_once(
        &self,
        steps: &[MultiStep],
        options: &MutationOptions,
    ) -> Result<WriteResponse, NodeError> {
        self.check_storage()?;
        self.forward_unless_leading()?;
        // The transaction's receipt lives under its first participant's
        // database, and its queue place in that database's lane.
        let db = steps.first().map(|step| step.db.as_slice()).unwrap_or(b"");
        let bytes = steps.iter().map(|step| input_bytes(&step.args)).sum();
        let queued = self
            .writer_place(self.writers.acquire(Class::Writer, db, bytes))
            .await?;
        let (prepare_steps, prepare_options) = (steps.to_vec(), options.clone());
        let written = self
            .writer
            .submit(Some(queued), move |engine| {
                let prepared = engine
                    .prepare_transaction_multi(&prepare_steps, &prepare_options)
                    .map_err(NodeError::failure)?;
                Ok(match prepared {
                    PrepareOutcome::Patch(prepared) => Prepared::Propose(
                        BatchItem::MultiPatch {
                            patches: prepared.patch,
                        },
                        None,
                    ),
                    PrepareOutcome::Done(done) => Prepared::Answer(Some(Decided::Done(done))),
                    PrepareOutcome::Replayed(done, committed) => {
                        Prepared::Answer(Some(Decided::Replayed(done, committed)))
                    }
                })
            })
            .await?;
        self.written(db, options, written).await
    }

    /// Wait for a write's place in the writer queue: the writer's
    /// `admission` stage, when telemetry is on.
    async fn writer_place<T, E>(
        &self,
        acquire: impl std::future::Future<Output = Result<T, E>>,
    ) -> Result<T, E> {
        if !telemetry::enabled() {
            return acquire.await;
        }
        let started = Instant::now();
        let place = acquire.await;
        self.observer
            .writer_stage(telemetry::WriterStage::Admission, started.elapsed());
        place
    }

    /// What the writer did since this member opened: batches, the
    /// writes they carried and the batches that went stale.
    pub fn writer_stats(&self) -> WriterStats {
        self.writer.stats()
    }

    /// Change how this member's writer prepares mutations: in parallel
    /// speculative waves, with or without conflict prediction, or
    /// serially (see [`crate::speculation`]). Takes effect from its next
    /// chunk of writes.
    pub fn set_speculation(&self, settings: SpeculationSettings) {
        self.writer.set_speculation(settings);
    }

    /// How this member's writer prepares mutations.
    pub fn speculation(&self) -> SpeculationSettings {
        self.writer.speculation()
    }

    /// A mutation's or transaction's answer from the writer: its
    /// committed item, cached for idempotency, or the result admission
    /// decided without proposing.
    async fn written(
        &self,
        db: &[u8],
        options: &MutationOptions,
        written: Written<Option<Decided>>,
    ) -> Result<WriteResponse, NodeError> {
        match (written.response, written.note) {
            (Some(response), _) => {
                self.cache_response(db, options, &response).await;
                Ok(response)
            }
            (None, Some(decided)) => self.done_response(db, options, decided).await,
            (None, None) => Err(NodeError::Fatal(
                "a write proposed nothing and decided nothing".to_string(),
            )),
        }
    }

    /// Cache a consensus-accepted idempotency outcome from the
    /// applier's response: the committed result, which
    /// duplicate-skipping may have replayed rather than executed.
    /// Deterministic rejections cache nothing (no receipt committed),
    /// and undecodable bytes skip the cache rather than poisoning it.
    async fn cache_response(&self, db: &[u8], options: &MutationOptions, response: &WriteResponse) {
        let Some(key) = options.idempotency_key.clone() else {
            return;
        };
        let outcome: Result<Value, Failure> = match &response.outcome {
            ResponseOutcome::Value(bytes) => match parse_value(bytes) {
                Ok(value) => Ok(value),
                Err(_) => return,
            },
            ResponseOutcome::Failure(bytes) => match parse_value(bytes) {
                Ok(envelope) => decode_envelope(envelope),
                Err(_) => return,
            },
            ResponseOutcome::Error(_) => return,
        };
        let horizon = Engine::idempotency_horizon(options);
        let db = db.to_vec();
        let _ = self
            .with_engine(move |engine| {
                engine.cache_idempotency(&db, &key, &outcome, horizon);
                Ok(())
            })
            .await;
    }

    /// Deploy a module through consensus: stage and backfill on the
    /// leader to compute authoritative bindings, then propose the
    /// activation every member applies. Backfill drains in pages and
    /// releases the engine between them, so applies and queries keep
    /// flowing during a long backfill. A member that lost leadership
    /// cancels its local staging before forwarding.
    pub async fn deploy(
        &self,
        db: &[u8],
        module: &str,
        identity: [u8; 32],
        wasm: &[u8],
        manifest: &Value,
    ) -> Result<WriteResponse, NodeError> {
        // Staging writes the group file before anything is proposed.
        self.check_storage()?;
        let leader = self.status().leader;
        if leader != Some(self.id) {
            return Err(NodeError::Forward { leader });
        }
        // A database that does not serve here, or that a prepared
        // transaction holds, takes no deployment (applies refuse it too).
        let target = db.to_vec();
        let unwritable = self
            .with_reader(move |reader| {
                let read = reader.store().begin_read().map_err(NodeError::engine)?;
                flower_engine::txn::unwritable(&read, &target).map_err(NodeError::engine)
            })
            .await?;
        if let Some(refusal) = unwritable {
            return Err(NodeError::failure(refusal));
        }
        let _deploying = self.claim_deployment(db, module)?;
        // One control slot covers the whole deployment: validation,
        // staging, backfill and the proposal.
        let _control = self
            .admission
            .acquire(
                Class::Control,
                db,
                deploy_bytes(wasm.len(), input_bytes(manifest)),
            )
            .await?;
        // Every member builds this runtime when it applies the entry, and
        // a committed entry that fails there would halt each of them on
        // every replay: validate first, so a broken module never reaches
        // the log.
        let (check_db, check_module, check_wasm, check_manifest) = (
            db.to_vec(),
            module.to_string(),
            wasm.to_vec(),
            manifest.clone(),
        );
        let runtimes = self.runtimes.clone();
        tokio::task::spawn_blocking(move || {
            runtimes.build(&check_db, &check_module, &check_wasm, &check_manifest)
        })
        .await
        .map_err(|error| NodeError::Fatal(error.to_string()))?
        .map_err(|message| NodeError::Failure {
            code: "invalid_module".to_string(),
            message,
        })?;
        // A deployment replaces the module's open staging: a replicated
        // one through the log, so every member abandons it alike.
        let open = self.staged_status(db, module).await?;
        if open.is_some_and(|status| status.replicated && status.phase != StagingPhase::Gc) {
            self.cancel_open(db, module).await?;
        }
        // Until its activation applies, this staging's intern IDs are
        // this member's alone (see `Node::interning`).
        let _interning = self.interning.read().await;
        let (stage_db, stage_module, stage_wasm, stage_manifest) = (
            db.to_vec(),
            module.to_string(),
            wasm.to_vec(),
            manifest.clone(),
        );
        self.with_engine(move |engine| {
            // No deployment of the module runs here but this one, so a
            // local staging of it is stale: a crash or a lost leadership
            // left it behind, and it would block every later deployment.
            engine
                .cancel_open_staging(&stage_db, &stage_module)
                .map_err(NodeError::failure)?;
            engine
                .deploy_staged(
                    &stage_db,
                    &stage_module,
                    identity,
                    &stage_wasm,
                    &stage_manifest,
                )
                .map_err(NodeError::failure)
        })
        .await?;
        let activation = loop {
            let (page_db, page_module) = (db.to_vec(), module.to_string());
            let activation = self
                .with_engine(move |engine| {
                    let progress = engine
                        .staging_drain(&page_db, &page_module, DEPLOY_DRAIN)
                        .map_err(NodeError::failure)?;
                    if !progress.done {
                        return Ok(None);
                    }
                    engine
                        .prepare_activation(&page_db, &page_module)
                        .map(Some)
                        .map_err(NodeError::failure)
                })
                .await;
            match activation {
                Ok(Some(activation)) => break activation,
                Ok(None) => continue,
                Err(error) => {
                    self.cancel_staging(db, module).await;
                    return Err(error);
                }
            }
        };
        let manifest = render(manifest).map_err(NodeError::engine)?;
        let write = ClusterWrite::Deploy {
            db: db.to_vec(),
            module: module.to_string(),
            wasm: wasm.to_vec(),
            manifest,
            identity,
            bindings: activation.bindings,
            proposed_ms: now_ms(),
        };
        match self.propose(write).await {
            Ok(response) => Ok(response),
            Err(error) => {
                if matches!(error, NodeError::Forward { .. }) {
                    self.cancel_staging(db, module).await;
                }
                Err(error)
            }
        }
    }

    /// The module's staging row on this member, if any.
    async fn staged_status(
        &self,
        db: &[u8],
        module: &str,
    ) -> Result<Option<flower_engine::engine::StagingStatus>, NodeError> {
        let (db, module) = (db.to_vec(), module.to_string());
        self.with_reader(move |reader| {
            reader
                .staging_status(&db, &module)
                .map_err(NodeError::failure)
        })
        .await
    }

    /// Cancel local staging, best-effort.
    async fn cancel_staging(&self, db: &[u8], module: &str) {
        let (db, module) = (db.to_vec(), module.to_string());
        let _ = self
            .with_engine(move |engine| {
                let _ = engine.cancel_staging(&db, &module);
                Ok(())
            })
            .await;
        self.catalog_changed();
    }

    /// Query, reading as the query declares (see [`Node::query_read`]).
    pub async fn query(
        &self,
        db: &[u8],
        module: &str,
        name: &str,
        args: &Value,
        options: &CallOptions,
    ) -> Result<Value, NodeError> {
        self.query_at(db, module, name, args, options)
            .await
            .map(|(value, _)| value)
    }

    /// Query plus the database revision its state reflects, both from
    /// one snapshot, reading as the query declares with no staleness
    /// bound of the caller's (see [`Node::query_read`]).
    pub async fn query_at(
        &self,
        db: &[u8],
        module: &str,
        name: &str,
        args: &Value,
        options: &CallOptions,
    ) -> Result<(Value, u64), NodeError> {
        self.query_read(db, module, name, args, options, &ReadOptions::default())
            .await
            .map(|(value, revision, _)| (value, revision))
    }

    /// Evaluate a query on the reader under a query admission permit,
    /// never taking the engine lock: queries run in parallel with each
    /// other, with prepares and with applies. Callers prove freshness
    /// first when the query needs it. Answers in the form asked, with
    /// how its cache lookup went.
    async fn evaluate_query(
        &self,
        db: &[u8],
        module: &str,
        name: &str,
        args: &Value,
        options: &CallOptions,
        form: Form,
    ) -> Result<(Reply, u64, Option<Lookup>), NodeError> {
        let bytes = input_bytes(args) + module.len() + name.len();
        let permit = self
            .observer
            .query_stage(
                QueryStage::Admission,
                self.admission.acquire(Class::Query, db, bytes),
            )
            .await?;
        let (db, module, name, args, options) = (
            db.to_vec(),
            module.to_string(),
            name.to_string(),
            args.clone(),
            options.clone(),
        );
        let observer = self.observer.clone();
        let evaluation = stages::timed(Stage::Eval, async {
            // Evaluator spans are this stage's children.
            let span = tracing::Span::current();
            let queued = telemetry::enabled().then(Instant::now);
            let (answer, revision, lookup) = self
                .with_reader(move |reader| {
                    let _entered = span.enter();
                    let _permit = permit;
                    let (result, lookup) =
                        measured_query(&observer, queued, &db, &module, &name, || match form {
                            Form::Value => {
                                local_query(reader, &db, &module, &name, &args, &options)
                                    .map(|(value, revision)| (Reply::Value(value), revision))
                            }
                            Form::Encoded => reader
                                .query_encoded_at(&db, &module, &name, &args, &options)
                                .map(|(encoded, revision)| (Reply::Encoded(encoded), revision))
                                .map_err(NodeError::application),
                        });
                    result.map(|(answer, revision)| (answer, revision, lookup))
                })
                .await?;
            if let Some(lookup) = lookup {
                stages::cache(lookup.hit());
            }
            Ok((answer, revision, lookup))
        });
        self.observer
            .query_stage(QueryStage::Evaluation, evaluation)
            .await
    }

    /// Confirm that this member leads and has applied everything
    /// committed before the call, by its lease or a quorum round (see
    /// [`crate::read`]): reads after it are linearizable, and a deposed
    /// leader cannot serve stale state. Off the leader it forwards.
    pub async fn ensure_leader(&self) -> Result<(), NodeError> {
        self.check_storage()?;
        self.reads.leader_fence().await?;
        // A leader that opened with its log past its state may have held,
        // and served, state past the floor it restored: wait for it.
        if let Some(fence) = &self.recovery
            && !fence.wait(RECOVERY_WAIT).await
        {
            return Err(NodeError::coded(RECOVERING, fence.reason()));
        }
        Ok(())
    }

    /// Why this member doesn't serve its state yet, while it recovers the
    /// log it opened with past that state (see [`crate::recovery`]).
    pub fn recovering(&self) -> Option<String> {
        self.recovery
            .as_ref()
            .filter(|fence| fence.recovering())
            .map(|fence| fence.reason())
    }

    /// Claim the module for one deployment on this member, refusing
    /// while another one runs. The claim releases on drop.
    fn claim_deployment(&self, db: &[u8], module: &str) -> Result<Deploying<'_>, NodeError> {
        let key = (db.to_vec(), module.to_string());
        let mut deploying = self.deploying.lock().expect("deploy claims poisoned");
        if !deploying.insert(key.clone()) {
            return Err(NodeError::Failure {
                code: "deployment_in_progress".to_string(),
                message: format!("`{module}` is already deploying on this member"),
            });
        }
        Ok(Deploying { node: self, key })
    }

    /// The lock serializing operations on one queue (or on one
    /// database's timers) from prepare through commit.
    fn work_lock(&self, scope: &[u8]) -> Arc<AsyncMutex<()>> {
        let mut locks = self.work_locks.lock().expect("work locks poisoned");
        if locks.len() >= WORK_LOCKS_PRUNE {
            locks.retain(|_, lock| Arc::strong_count(lock) > 1);
        }
        locks.entry(scope.to_vec()).or_default().clone()
    }

    /// Run one queue claim, renewal, report or maintenance operation
    /// through consensus at the leader. Business failures (bad
    /// tokens, unknown items) are the inner `Err`.
    pub async fn queue_op(
        &self,
        db: &[u8],
        queue: &str,
        op: QueueOp,
    ) -> Result<Result<QueueOutcome, Failure>, NodeError> {
        let mut scope = b"queue\x00".to_vec();
        scope.extend_from_slice(db);
        scope.push(0);
        scope.extend_from_slice(queue.as_bytes());
        let lock = self.work_lock(&scope);
        let _serialized = lock.lock().await;
        self.ensure_leader().await?;
        // Settling a claim finishes work already done: never shed early.
        let settles = matches!(
            op,
            QueueOp::Heartbeat { .. }
                | QueueOp::Complete { .. }
                | QueueOp::CompleteWith { .. }
                | QueueOp::Fail { .. }
                | QueueOp::Kill { .. }
                | QueueOp::Release { .. }
        );
        let queued = if settles {
            self.writers
                .acquire_settling(Class::Writer, db, REQUEST_OVERHEAD)
                .await?
        } else {
            self.writers
                .acquire(Class::Writer, db, REQUEST_OVERHEAD)
                .await?
        };
        let (prepare_db, queue) = (db.to_vec(), queue.to_string());
        let written = self
            .writer
            .submit(Some(queued), move |engine| {
                // Lease expiries and due times read the clock at each
                // preparation: a write prepared again plans afresh.
                let prepared = prepare_queue(engine, &prepare_db, &queue, &op, now_ms());
                Ok(match prepared {
                    Ok(prepared) => match prepared.patch {
                        Some(patch) => Prepared::Propose(
                            BatchItem::Patch {
                                db: prepare_db.clone(),
                                patch,
                            },
                            Ok(prepared.outcome),
                        ),
                        None => Prepared::Answer(Ok(prepared.outcome)),
                    },
                    Err(failure) => Prepared::Answer(Err(failure)),
                })
            })
            .await?;
        let outcome = match written.note {
            Ok(outcome) => outcome,
            Err(failure) => return self.business(failure),
        };
        if let Some(response) = &written.response
            && let Err(failure) = committed(response)
        {
            return Ok(Err(failure));
        }
        Ok(Ok(outcome))
    }

    /// Run one operator timer operation through consensus at the
    /// leader. Timer patches are revision-guarded: a stale one
    /// prepares again, bounded times.
    pub async fn timer_op(
        &self,
        db: &[u8],
        op: TimerOp,
    ) -> Result<Result<TimerOutcome, Failure>, NodeError> {
        let mut scope = b"timer\x00".to_vec();
        scope.extend_from_slice(db);
        let lock = self.work_lock(&scope);
        let _serialized = lock.lock().await;
        self.ensure_leader().await?;
        let queued = self
            .writers
            .acquire(Class::Writer, db, REQUEST_OVERHEAD)
            .await?;
        let prepare_db = db.to_vec();
        // Timer patches are revision-guarded at the revision their plan
        // read, which on the writer's staged state is the one they apply
        // at.
        let written = self
            .writer
            .submit(Some(queued), move |engine| {
                let prepared = prepare_timer(engine, &prepare_db, &op, now_ms());
                Ok(match prepared {
                    Ok(prepared) => match prepared.patch {
                        Some(patch) => Prepared::Propose(
                            BatchItem::Patch {
                                db: prepare_db.clone(),
                                patch,
                            },
                            Ok(prepared.outcome),
                        ),
                        None => Prepared::Answer(Ok(prepared.outcome)),
                    },
                    Err(failure) => Prepared::Answer(Err(failure)),
                })
            })
            .await?;
        let outcome = match written.note {
            Ok(outcome) => outcome,
            Err(failure) => return self.business(failure),
        };
        match &written.response {
            Some(response) => Ok(committed(response).map(|()| outcome)),
            None => Ok(Ok(outcome)),
        }
    }

    /// Fire one due timer through consensus: the fire patch deletes
    /// the timer and carries the handler's effects atomically, or a
    /// retry patch advances the timer past a failed fire. Stale
    /// fires mean state moved under the prepare; the sweep retries
    /// them next tick.
    pub async fn fire_timer(&self, db: &[u8], id: &str) -> Result<FireReport, NodeError> {
        let (prepare_db, prepare_id) = (db.to_vec(), id.to_string());
        let written = self
            .writer
            .submit(None, move |engine| {
                let prepared = engine
                    .prepare_timer_fire(&prepare_db, &prepare_id)
                    .map_err(NodeError::failure)?;
                let (patch, report) = match prepared {
                    FirePrepare::Skip => return Ok(Prepared::Answer(FireReport::Skipped)),
                    FirePrepare::Patch(prepared) => (prepared, FireReport::Fired),
                    FirePrepare::Retry(prepared) => (prepared, FireReport::Retried),
                    FirePrepare::Hold(prepared) => (prepared, FireReport::Held),
                };
                Ok(Prepared::Propose(
                    BatchItem::Patch {
                        db: prepare_db.clone(),
                        patch: patch.patch,
                    },
                    report,
                ))
            })
            .await?;
        match &written.response {
            Some(response) if is_stale(response) => Ok(FireReport::Stale),
            _ => Ok(written.note),
        }
    }

    /// Sweep due timers once across replicated databases: fire each
    /// in turn and report the pass. Followers report without firing;
    /// forwards mid-pass stop the pass; fatal errors abort it.
    pub async fn sweep_timers_once(&self) -> Result<SweepReport, NodeError> {
        let mut report = SweepReport::default();
        if !self.is_leader() {
            return Ok(report);
        }
        report.leader = true;
        let dbs = self
            .with_engine(|engine| {
                StateMachine::replicated_databases(engine).map_err(NodeError::failure)
            })
            .await?;
        report.dbs = dbs.len();
        for db in dbs {
            let list_db = db.clone();
            let due = self
                .with_engine(move |engine| {
                    engine
                        .list_due_timers(&list_db, now_ms(), SWEEP_LIMIT)
                        .map_err(NodeError::failure)
                })
                .await?;
            for timer in due {
                let fired = match self.fire_timer(&db, &timer.id).await {
                    // A database that does not serve here now (frozen,
                    // moving, moved, retired, or held by a prepared
                    // transaction) keeps its timers for a later pass, or
                    // for the group it moves to; the others still fire.
                    Err(NodeError::Failure { code, .. }) if not_serving(&code) => {
                        report.skipped += 1;
                        break;
                    }
                    fired => fired?,
                };
                match fired {
                    FireReport::Fired => report.fired += 1,
                    FireReport::Retried => report.retried += 1,
                    FireReport::Held => report.held += 1,
                    FireReport::Stale => report.stale += 1,
                    FireReport::Skipped => report.skipped += 1,
                }
            }
        }
        Ok(report)
    }

    /// Run one unit of a maintenance task through consensus: the writer
    /// prepares it on its staged state ([`Engine::prepare_task_run`]:
    /// `due` again, then the run, in one transaction) and proposes its
    /// patch, or the failed run's error-callback or failure-state patch.
    /// The patch is revision-guarded: a stale one changes nothing, and
    /// the next pass decides again.
    pub async fn run_task(
        &self,
        db: &[u8],
        module: &str,
        task: &str,
    ) -> Result<TaskReport, NodeError> {
        let (prepare_db, prepare_module, prepare_task) =
            (db.to_vec(), module.to_string(), task.to_string());
        let written = self
            .writer
            .submit(None, move |engine| {
                let prepared = engine
                    .prepare_task_run(&prepare_db, &prepare_module, &prepare_task)
                    .map_err(NodeError::failure)?;
                let (patch, report) = match prepared {
                    TaskPrepare::Skip => return Ok(Prepared::Answer(TaskReport::Skipped)),
                    TaskPrepare::Ran(prepared) => (prepared, TaskReport::Ran),
                    TaskPrepare::Failed { prepared, .. } => (prepared, TaskReport::Failed),
                };
                Ok(Prepared::Propose(
                    BatchItem::Patch {
                        db: prepare_db.clone(),
                        patch: patch.patch,
                    },
                    report,
                ))
            })
            .await?;
        match &written.response {
            Some(response) if is_stale(response) => Ok(TaskReport::Stale),
            _ => Ok(written.note),
        }
    }

    /// Run due maintenance tasks once across replicated databases, as
    /// the leader: in each database, plan on the read side
    /// ([`EngineReader::due_tasks`]), run the task due earliest, and plan
    /// again, until nothing is due or the database had its burst
    /// ([`TASK_BURST`]). Followers report without running anything; a
    /// database that does not serve here keeps its tasks for a later
    /// pass or its new group; forwards mid-pass stop the pass.
    pub async fn sweep_tasks_once(&self) -> Result<TaskSweepReport, NodeError> {
        let mut report = TaskSweepReport::default();
        if !self.is_leader() {
            return Ok(report);
        }
        report.leader = true;
        // Groups whose modules declare no task skip the engine lock.
        if !self.reader.has_tasks() {
            return Ok(report);
        }
        let dbs = self
            .with_engine(|engine| {
                StateMachine::replicated_databases(engine).map_err(NodeError::failure)
            })
            .await?;
        report.dbs = dbs.len();
        let earliest = |next: Option<u64>, at: u64| Some(next.map_or(at, |next| next.min(at)));
        for db in dbs {
            let started = Instant::now();
            // Units of one task submitted at once: the writer prepares each
            // on the state the ones before it staged and commits them in one
            // entry. Doubles while every unit runs.
            let mut units = 1;
            loop {
                let plan_db = db.clone();
                let planned = self
                    .with_reader(move |reader| {
                        reader
                            .plan_tasks(&plan_db, now_ms())
                            .map_err(NodeError::failure)
                    })
                    .await;
                let plan = match planned {
                    Err(NodeError::Failure { code, .. }) if not_serving(&code) => break,
                    planned => planned?,
                };
                if let Some(at) = plan.next_ms {
                    report.next_ms = earliest(report.next_ms, at);
                }
                let Some(next) = plan.due.into_iter().next() else {
                    break;
                };
                let runs = (0..units)
                    .map(|_| self.run_task(&db, &next.module, &next.task))
                    .collect();
                let (mut moved, mut every, mut refused) = (false, true, false);
                for ran in join_all(runs).await {
                    let ran = match ran {
                        Err(NodeError::Failure { code, .. }) if not_serving(&code) => {
                            refused = true;
                            TaskReport::Skipped
                        }
                        ran => ran?,
                    };
                    match ran {
                        TaskReport::Ran => report.ran += 1,
                        TaskReport::Failed => report.failed += 1,
                        TaskReport::Stale => report.stale += 1,
                        TaskReport::Skipped => report.skipped += 1,
                    }
                    moved |= matches!(ran, TaskReport::Ran | TaskReport::Failed);
                    every &= ran == TaskReport::Ran;
                }
                if refused {
                    break;
                }
                // A plan the writer's state disagreed with waits for the
                // next pass rather than spinning on it, as does work left
                // past the burst: both soon.
                if !moved || started.elapsed() >= TASK_BURST {
                    report.next_ms = earliest(report.next_ms, now_ms());
                    break;
                }
                units = if every {
                    (units * 2).min(TASK_UNITS)
                } else {
                    1
                };
            }
        }
        Ok(report)
    }

    /// Run this member's maintenance tasks until `shutdown` turns true,
    /// waking at the next time a task may be due, after a write (which may
    /// make one due), on a change of leader, and at least every `interval`;
    /// never starting two passes within [`TASK_FLOOR`]. Only the leader runs
    /// anything (see [`Node::sweep_tasks_once`]). After a storage failure no
    /// pass runs.
    pub async fn run_tasks(
        &self,
        interval: Duration,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) {
        let mut commits = self.subscribe_commits();
        let mut metrics = self.raft.metrics();
        let mut leader = metrics.borrow().current_leader;
        while !*shutdown.borrow() {
            let started = tokio::time::Instant::now();
            let floor = started + TASK_FLOOR;
            let mut wake = started + interval;
            if self.storage_failure().is_none()
                && let Ok(report) = self.sweep_tasks_once().await
            {
                use crate::telemetry::MaintenanceOutcome as Ended;
                self.observer.maintenance_ran(Ended::Ran, report.ran);
                self.observer.maintenance_ran(Ended::Failed, report.failed);
                self.observer.maintenance_ran(Ended::Stale, report.stale);
                self.observer
                    .maintenance_ran(Ended::Skipped, report.skipped);
                if let Some(next) = report.next_ms {
                    let wait = Duration::from_millis(next.saturating_sub(now_ms()));
                    wake = wake.min(tokio::time::Instant::now() + wait);
                }
            }
            let seen = leader;
            tokio::select! {
                _ = tokio::time::sleep_until(wake.max(floor)) => {
                    self.observer.maintenance_woke(crate::telemetry::MaintenanceWake::Due);
                }
                notice = commits.recv() => {
                    if matches!(notice, Err(tokio::sync::broadcast::error::RecvError::Closed)) {
                        return;
                    }
                    self.observer.maintenance_woke(crate::telemetry::MaintenanceWake::Write);
                    tokio::time::sleep_until(floor).await;
                }
                changed = async {
                    // The metrics guard must not live across an await.
                    let changed = metrics
                        .wait_for(|current| current.current_leader != seen)
                        .await
                        .map(|current| current.current_leader);
                    match changed {
                        Ok(leader) => leader,
                        // Raft stopped: only shutdown is left to wait for.
                        Err(_) => std::future::pending().await,
                    }
                } => {
                    leader = changed;
                    self.observer.maintenance_woke(crate::telemetry::MaintenanceWake::Leadership);
                    tokio::time::sleep_until(floor).await;
                }
                changed = shutdown.changed() => {
                    if changed.is_err() {
                        return;
                    }
                }
            }
        }
    }

    /// Serve guest keys through `keys`: the member installs the applied
    /// managed-key catalog in it now and on every later change, and
    /// prepares operator changes with its wrapping keys. Without one
    /// the member applies the catalog into a locked runtime.
    pub fn with_key_catalog(self, keys: Arc<ManagedKeys>) -> Self {
        self.attach_key_catalog(keys);
        self
    }

    /// [`Node::with_key_catalog`] on a shared member.
    pub fn attach_key_catalog(&self, keys: Arc<ManagedKeys>) {
        self.keys.attach(keys);
    }

    /// The managed-key runtime this member serves guest keys through.
    pub fn key_catalog(&self) -> Arc<ManagedKeys> {
        self.keys.keys()
    }

    /// Run an operator request against database `db`'s managed-key
    /// catalog (see [`ManagedKeys::prepare`] for the operations).
    /// `cache` reports this member's prepared-key cache, which every
    /// database shares; everything else runs on the leader behind a read
    /// fence, for a database that serves in this group (one moving or
    /// moved away is refused with its placement code). A name that
    /// cannot be a database's ([`crate::keys::invalid_database`]) is a
    /// [`KEY_REQUEST`] refusal, and so is a database a group other than
    /// the root does not place: only the root holds databases no
    /// partition names, so a key change elsewhere would leave rows that
    /// keep the database from moving in later. Reads answer from
    /// the applied catalog, changes prepare with this member's wrapping
    /// key and commit through the log, preparing again when a concurrent
    /// change moved the catalog. Responses are
    /// `{revision, value, duplicate}` with the public catalog, or the
    /// cache statistics.
    pub async fn manage_keys(
        &self,
        db: &[u8],
        request: serde_json::Value,
    ) -> Result<serde_json::Value, NodeError> {
        const ATTEMPTS: usize = 8;
        let keys = self.key_catalog();
        if request.get("operation").and_then(serde_json::Value::as_str) == Some("cache") {
            return Ok(keys.cache_stats());
        }
        if let Some(why) = crate::keys::invalid_database(db) {
            return Err(NodeError::coded(KEY_REQUEST, why));
        }
        if !self.is_leader() {
            return Err(NodeError::Forward {
                leader: self.status().leader,
            });
        }
        for _ in 0..ATTEMPTS {
            self.ensure_leader().await?;
            let (read_db, group) = (db.to_vec(), self.group.clone());
            let current = self
                .with_engine(move |engine| {
                    let read = engine.store().begin_read().map_err(NodeError::engine)?;
                    if let Some(refusal) = flower_engine::placement::read_refusal(&read, &read_db)
                        .map_err(NodeError::engine)?
                    {
                        return Err(NodeError::failure(refusal));
                    }
                    if !group.is_root()
                        && flower_engine::placement::read(&read, &read_db)
                            .map_err(NodeError::engine)?
                            .is_none()
                    {
                        return Err(NodeError::coded(
                            KEY_REQUEST,
                            format!(
                                "group {group} holds no database `{}`",
                                String::from_utf8_lossy(&read_db)
                            ),
                        ));
                    }
                    read.meta(&read_db, KEY_CATALOG).map_err(NodeError::engine)
                })
                .await?;
            let prepared = {
                let keys = keys.clone();
                let request = request.clone();
                tokio::task::spawn_blocking(move || keys.prepare(current.as_deref(), &request))
                    .await
                    .map_err(|error| NodeError::Fatal(error.to_string()))?
            };
            let change = match prepared {
                Ok(PreparedKeys::Response(response)) => return Ok(response),
                Ok(PreparedKeys::Change(change)) => change,
                Err(error) => return Err(key_failure(&format!("{error:#}"))),
            };
            let response = self
                .propose(ClusterWrite::Keys {
                    db: db.to_vec(),
                    change,
                    proposed_ms: now_ms(),
                })
                .await?;
            match response.outcome {
                ResponseOutcome::Value(body) => {
                    return serde_json::from_slice(&body)
                        .map_err(|error| NodeError::Fatal(error.to_string()));
                }
                ResponseOutcome::Error(message) if message.starts_with("KEY_CATALOG_CONFLICT") => {
                    continue;
                }
                ResponseOutcome::Error(message) => return Err(key_failure(&message)),
                ResponseOutcome::Failure(body) => {
                    return Err(key_failure(&String::from_utf8_lossy(&body)));
                }
            }
        }
        Err(NodeError::coded(
            "KEY_CATALOG_CONFLICT",
            "the key catalog kept changing under the request",
        ))
    }

    /// Every database's managed-key catalog in this group, as the leader
    /// applied them, behind a read fence: `{databases: {<db>: {revision,
    /// value}}}` with each public catalog, databases without one left
    /// out.
    pub async fn list_keys(&self) -> Result<serde_json::Value, NodeError> {
        if !self.is_leader() {
            return Err(NodeError::Forward {
                leader: self.status().leader,
            });
        }
        self.ensure_leader().await?;
        self.with_reader(|reader| {
            let read = reader.store().begin_read().map_err(NodeError::engine)?;
            let catalogs = StateMachine::key_catalogs(&read).map_err(NodeError::failure)?;
            key_listing(catalogs).map_err(|error| NodeError::coded(KEY_REQUEST, error))
        })
        .await
    }

    /// Establish the group's history incarnation: while none is applied
    /// and this member leads, propose a fresh random one. The first
    /// applied proposal wins on every member (see [`StateMachine`]), so
    /// racing leaders are harmless. Returns the applied incarnation,
    /// still zero when this member does not lead. A failed proposal
    /// (lost leadership, quorum loss) is the error; maintenance simply
    /// tries again next tick.
    pub async fn establish_incarnation(&self) -> Result<u64, NodeError> {
        let applied = self.incarnation();
        if applied != 0 || !self.is_leader() {
            return Ok(applied);
        }
        // Zero means "none yet", so it is never proposed.
        let incarnation = rand::random_range(1..=u64::MAX);
        self.propose(ClusterWrite::Incarnation {
            incarnation,
            proposed_ms: now_ms(),
        })
        .await?;
        Ok(self.incarnation())
    }

    /// The applied group incarnation, zero until the group establishes
    /// one (see [`Node::establish_incarnation`]).
    pub fn incarnation(&self) -> u64 {
        self.incarnation.load(Ordering::Acquire)
    }

    /// Run this member's maintenance until `shutdown` turns true: the
    /// leader establishes the history incarnation while none is
    /// applied, sweeps due timers every `interval`, rotates and
    /// collects retry retention (see [`Node::maintain_retention`]),
    /// purges removed modules' data (see [`Node::maintain_purges`]) and
    /// collects what deployments retired (see
    /// [`Node::maintain_staging`]), and every member purges expired
    /// receipts every ten minutes and lets go of move bases it no longer
    /// needs (see [`Node::release_stale_exports`]). Forwards, quorum loss
    /// and poison wait for the next tick: timers stay pending and visible
    /// until a pass lands them. After a storage failure no pass runs but
    /// the release.
    pub async fn run_maintenance(
        &self,
        interval: Duration,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) {
        while !*shutdown.borrow() {
            self.release_stale_exports();
            if self.storage_failure().is_none() {
                let _ = self.establish_incarnation().await;
                let _ = self.sweep_timers_once().await;
                self.maintain_retention(interval).await;
                self.maintain_purges(interval).await;
                self.maintain_staging(interval).await;
                self.maybe_purge_idempotency().await;
            }
            tokio::select! {
                _ = tokio::time::sleep(interval) => {}
                changed = shutdown.changed() => {
                    if changed.is_err() {
                        return;
                    }
                }
            }
        }
    }

    /// Delete expired unscoped idempotency receipts across replicated
    /// databases, returning rows deleted. Local hygiene on every
    /// member: it deletes only receipts expired at the replicated
    /// expiry clock (the largest applied proposal stamp), which every
    /// later entry reads expiry at or past, so whether and when a
    /// member purged never changes a duplicate verdict (see
    /// [`StateMachine`]).
    pub async fn purge_expired_idempotency(&self) -> Result<usize, NodeError> {
        self.with_engine(|engine| {
            StateMachine::purge_expired_receipts(engine, IDEMPOTENCY_PURGE_PAGE)
                .map_err(NodeError::failure)
        })
        .await
    }

    /// Purge expired idempotency receipts when the hygiene interval
    /// elapsed; failures only skip the pass.
    async fn maybe_purge_idempotency(&self) {
        {
            let Ok(mut last) = self.last_idempotency_purge.lock() else {
                return;
            };
            if last.elapsed() < IDEMPOTENCY_PURGE_EVERY {
                return;
            }
            *last = Instant::now();
        }
        let _ = self.purge_expired_idempotency().await;
    }

    /// Change cluster membership through consensus, unvalidated: the
    /// primitive under [`Node::reconfigure`], which operators use.
    /// Members with dial addresses register as learners first, which is
    /// how TCP learns to reach new members; empty addresses suit
    /// in-process clusters only. Re-adding an existing member is a
    /// no-op round trip.
    pub async fn change_membership(
        &self,
        members: BTreeMap<NodeId, BasicNode>,
        retain: bool,
    ) -> Result<ClientWriteResponse<TypeConfig>, NodeError> {
        self.check_storage()?;
        let _operation = self.membership_ops.lock().await;
        for (id, node) in &members {
            if node.addr.is_empty() {
                continue;
            }
            self.raft
                .add_learner(*id, node.clone(), true)
                .await
                .map_err(NodeError::write)?;
        }
        let ids: BTreeSet<NodeId> = members.keys().copied().collect();
        self.raft
            .change_membership(ids, retain)
            .await
            .map_err(NodeError::write)
    }

    /// Serve a sync batch after `since`: only entries this member has
    /// applied, from one consistent view of the log, stamped with the
    /// applied incarnation.
    pub fn sync_since(&self, since: u64, limit: u64) -> Result<SyncBatch, NodeError> {
        // Until it caught up, its applied history may lack its group's
        // incarnation and committed entries it once held.
        if self.rejoining() {
            return Err(NodeError::coded(
                rejoin::REJOINING,
                "this member opened without Raft state and has not caught up",
            ));
        }
        if let Some(reason) = self.recovering() {
            return Err(NodeError::coded(RECOVERING, reason));
        }
        let applied = self.status().last_applied.unwrap_or(0);
        // Published before Raft reports an entry applied, so read after
        // `applied` it covers every entry the batch can carry.
        let incarnation = self.incarnation();
        serve_since(&self.logs, since, limit, applied, incarnation)
            .map_err(|error| NodeError::coded("sync", error))
    }

    /// Current Raft status.
    pub fn status(&self) -> NodeStatus {
        let metrics = self.raft.metrics().borrow().clone();
        NodeStatus {
            id: self.id,
            state: format!("{:?}", metrics.state),
            leader: metrics.current_leader,
            term: metrics.current_term,
            last_log: metrics.last_log_index,
            last_applied: metrics.last_applied.map(|id| id.index),
        }
    }

    /// Whether this member currently believes it leads.
    fn is_leader(&self) -> bool {
        self.status().leader == Some(self.id)
    }

    /// Readiness: storage hasn't failed, the Raft core runs, a leader is
    /// known, this member applied every entry it knew committed when
    /// asked, and the log store answers a read. Cheap: no quorum round,
    /// no engine lock.
    ///
    /// Applying trails committing by an entry or a batch on any busy
    /// member, so a member short of the committed index it read is given
    /// [`READY_APPLY_WAIT`] (never past the request's client deadline)
    /// to apply through it: only a member still short of it then answers
    /// "applying committed entries". One that holds committed entries it
    /// cannot apply in that time is behind, and stays not ready.
    pub async fn readiness(&self) -> Readiness {
        let metrics = self.raft.metrics().borrow().clone();
        let applied = metrics.last_applied.map(|id| id.index);
        let mut readiness = Readiness {
            ready: false,
            rejoining: self.rejoining(),
            storage: None,
            reason: None,
            leader: metrics.current_leader,
            committed: None,
            applied,
        };
        if let Some(failure) = self.storage_failure() {
            readiness.reason = Some(failure.to_string());
            readiness.storage = Some(failure);
            return readiness;
        }
        if let Err(fatal) = &metrics.running_state {
            readiness.reason = Some(format!("raft stopped: {fatal}"));
            return readiness;
        }
        let committed = match self.raft.with_raft_state(|state| state.committed).await {
            Ok(committed) => committed.map(|id| id.index),
            Err(fatal) => {
                readiness.reason = Some(format!("raft stopped: {fatal}"));
                return readiness;
            }
        };
        readiness.committed = committed;
        if let Err(error) = self.logs.last_purged() {
            readiness.reason = Some(format!("raft log unreadable: {error}"));
            return readiness;
        }
        if readiness.rejoining {
            readiness.reason = Some(
                "rejoining: this member opened without Raft state and serves once it has caught \
                 up with its group's leader"
                    .to_string(),
            );
            return readiness;
        }
        if metrics.current_leader.is_none() {
            readiness.reason = Some("no known leader".to_string());
            return readiness;
        }
        if applied < committed {
            // Applying trails committing by an entry or a batch on any
            // busy member: give it a moment to apply what it knew
            // committed when the probe arrived. Still short of that, it
            // is behind, not in flight.
            readiness.applied = self.applied_through(committed, READY_APPLY_WAIT).await;
            if readiness.applied < committed {
                readiness.reason = Some("applying committed entries".to_string());
                return readiness;
            }
        }
        if let Some(reason) = self.recovering() {
            readiness.reason = Some(reason);
            return readiness;
        }
        readiness.ready = true;
        readiness
    }

    /// Wait until this member applied through `target`, for at most
    /// `bound` and never past the request's client deadline; answers the
    /// applied index it reached.
    async fn applied_through(&self, target: Option<u64>, bound: Duration) -> Option<u64> {
        let mut metrics = self.raft.metrics();
        let until = crate::deadline::bounded(bound);
        let _ = tokio::time::timeout_at(
            until,
            metrics.wait_for(|metrics| metrics.last_applied.map(|id| id.index) >= target),
        )
        .await;
        metrics.borrow().last_applied.map(|id| id.index)
    }

    /// Resolve once storage has failed and the node directory accepts
    /// writes again with `free_bytes` available ([`writable`]): a
    /// restart can then reopen the files without filling the disk at
    /// once. There is no event for free space, so this checks every
    /// second.
    pub async fn storage_recovered(&self, free_bytes: u64) {
        let mut interval = tokio::time::interval(RECOVERY_PROBE_EVERY);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut last = String::new();
        loop {
            interval.tick().await;
            if self.storage_failure().is_none() {
                continue;
            }
            let dir = self.dir.clone();
            match tokio::task::spawn_blocking(move || writable(&dir, free_bytes)).await {
                Ok(Ok(())) => {
                    tracing::warn!(
                        target: "flower::storage",
                        "the data directory accepts writes again: restart to reopen the files"
                    );
                    return;
                }
                Ok(Err(error)) => {
                    let error = error.to_string();
                    if error != last {
                        tracing::warn!(
                            target: "flower::storage",
                            %error,
                            "waiting for the data directory to accept writes"
                        );
                        last = error;
                    }
                }
                Err(error) => {
                    tracing::warn!(target: "flower::storage", %error, "write probe failed");
                }
            }
        }
    }

    /// Compact the group and Raft files online until `shutdown` turns
    /// true or storage fails (see [`crate::compaction`]). Stop it before
    /// [`Node::shutdown`]: when it returns, no step holds either file.
    pub async fn run_compaction(
        &self,
        settings: CompactionSettings,
        shutdown: tokio::sync::watch::Receiver<bool>,
    ) {
        let run = |compactor: Option<Compactor>, component| {
            let (health, shutdown) = (self.health.clone(), shutdown.clone());
            async move {
                if let Some(compactor) = compactor {
                    compaction::run_file(compactor, settings, health, component, shutdown).await;
                }
            }
        };
        tokio::join!(
            run(self.group_compactor.clone(), GROUP),
            run(self.logs.compactor(), RAFT_LOG)
        );
    }

    /// Check the soft snapshot triggers every [`SnapshotPolicy::check`]
    /// until `shutdown` turns true, storage fails or Raft stops, asking
    /// Raft for a snapshot when one is due (see
    /// [`crate::snapshot_policy`]). OpenRaft's entry-count trigger runs
    /// whether or not this does.
    pub async fn run_snapshots(
        &self,
        policy: SnapshotPolicy,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) {
        if !policy.enabled() {
            return;
        }
        let mut tick = tokio::time::interval(policy.check);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = tick.tick() => {}
                changed = shutdown.changed() => {
                    if changed.is_err() {
                        return;
                    }
                }
            }
            if *shutdown.borrow() || self.storage_failure().is_some() {
                return;
            }
            let Some(reason) = self.accounting.due(&policy, Instant::now(), now_ms()) else {
                continue;
            };
            tracing::debug!(target: "flower::storage", reason, "snapshot due");
            if self.raft.trigger().snapshot().await.is_err() {
                return;
            }
        }
    }

    /// The soft snapshot triggers' view under `policy`, for metrics.
    pub fn snapshot_policy_stats(&self, policy: &SnapshotPolicy) -> SnapshotPolicyStats {
        self.accounting.stats(policy)
    }

    /// Online compaction counters of the group and Raft files.
    pub fn compaction_stats(&self) -> Vec<(&'static str, CompactionStats)> {
        [
            (GROUP, self.group_compactor.clone()),
            (RAFT_LOG, self.logs.compactor()),
        ]
        .into_iter()
        .filter_map(|(file, compactor)| Some((file, compactor?.stats())))
        .collect()
    }

    /// Serve Raft RPCs over TCP from this address; the handle owns
    /// the server task's lifetime.
    pub async fn serve_tcp(
        &self,
        addr: &str,
        config: PeerConfig,
    ) -> std::io::Result<tokio::task::JoinHandle<()>> {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        Ok(self.serve_listener(listener, config))
    }

    /// Serve Raft RPCs and member requests on an already-bound
    /// listener; aborting the handle closes the listener and its
    /// connections.
    pub fn serve_listener(
        &self,
        listener: tokio::net::TcpListener,
        config: PeerConfig,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(serve_peer(listener, self.peer_service(), config))
    }

    /// What answers this member's peer requests: register it with an
    /// in-process [`crate::network::Registry`] for the rejoin gate on
    /// votes and for leadership transfers to it.
    pub fn peer_service(&self) -> PeerService {
        PeerService::new(
            self.id,
            self.group.clone(),
            self.raft.clone(),
            self.rejoin.clone(),
            self.desk.clone(),
        )
    }

    /// Whether this member opened without Raft state and has not caught
    /// up with its group yet: it neither votes, campaigns nor serves
    /// sync batches until then (see [`crate::rejoin`]).
    pub fn rejoining(&self) -> bool {
        self.rejoin.amnesiac()
    }

    /// Shut the Raft core down and release the node: no more votes,
    /// appends or applies, no purge transaction in flight, and the files
    /// are closed when this returns: they may reopen at once.
    pub async fn shutdown(mut self) -> Result<(), String> {
        let stopped = self
            .raft
            .shutdown()
            .await
            .map_err(|error| error.to_string());
        self.logs.stop_purging().await;
        // OpenRaft lets its state machine worker and snapshot builders go
        // after the core stops; they hold both files until then.
        let mut released = self.files_released.clone();
        let gone = tokio::time::timeout(FILES_RELEASE_WAIT, async {
            while released.changed().await.is_ok() {}
        })
        .await;
        if gone.is_err() {
            tracing::warn!(
                target: "flower::storage",
                "the state machine still holds the files after shutdown"
            );
        }
        // Nothing appends or applies any more: make the leader's held
        // appends durable, then the deferred applies, in that order (see
        // `crate::flush`). After a storage failure these fail; the
        // restart replays from the quorum's logs.
        self.logs.stop_flushing().await;
        if self.storage_failure().is_none()
            && let Err(error) = self.flush_applies().await
        {
            tracing::warn!(
                target: "flower::storage",
                %error,
                "making applied entries durable at shutdown failed; a restart replays them"
            );
        }
        // End the background tasks: the catch-up task holds the Raft file,
        // the apply flusher and the writer the engine, which the writer
        // lets go once its queue closes with this node and its last batch
        // resolved.
        let catch_up = self.catch_up.take();
        let writer = self.writer_task.take();
        let apply_flusher = self.apply_flusher.take();
        let (unsynced, logs) = (std::mem::take(&mut self.unsynced), self.logs.clone());
        drop(self);
        if let Some(task) = catch_up {
            let _ = task.await;
        }
        if let Some(task) = writer {
            let _ = task.await;
        }
        if let Some(task) = apply_flusher {
            let _ = task.await;
        }
        if unsynced.is_empty() {
            return stopped;
        }
        // Opened with fsync off: sync everything and drop the markers, so
        // a power loss from here on loses nothing (see `crate::fsync`).
        let settled = tokio::task::spawn_blocking(move || {
            logs.sync_images()?;
            drop(logs);
            unsynced.iter().try_for_each(|file| fsync::settle(file))
        })
        .await
        .map_err(|error| error.to_string())?;
        settled.map_err(|error| format!("syncing the files opened with fsync off: {error}"))?;
        stopped
    }

    /// This node's ID.
    pub fn id(&self) -> NodeId {
        self.id
    }

    /// The Raft handle, for routing registration.
    pub fn raft(&self) -> &Raft<TypeConfig> {
        &self.raft
    }

    /// Whether this node already holds membership.
    pub async fn is_initialized(&self) -> Result<bool, NodeError> {
        self.raft
            .is_initialized()
            .await
            .map_err(|error| NodeError::Fatal(error.to_string()))
    }

    /// Subscribe to commit notices for watch hubs.
    pub fn subscribe_commits(&self) -> tokio::sync::broadcast::Receiver<CommitNotice> {
        self.commits.subscribe()
    }

    /// Follow the count of changes to what operators see of this
    /// member's catalog (see [`CatalogChanges`]): read
    /// [`Node::operator_catalog`] again when it moves.
    pub fn subscribe_catalog(&self) -> tokio::sync::watch::Receiver<u64> {
        self.catalog.subscribe()
    }

    /// Note a local change to the operator catalog: a staging step.
    pub(crate) fn catalog_changed(&self) {
        self.catalog.bump();
    }

    /// The shared engine, the writer lane, for backups and local work
    /// in tests. Lock it through [`Node::with_engine`] or
    /// [`crate::exec::with_engine`] so the work stays off the async
    /// workers; reads that need no writer go through [`Node::reader`].
    pub fn engine(&self) -> &Arc<AsyncMutex<Engine>> {
        &self.engine
    }

    /// The log store, for sync serving.
    pub fn logs(&self) -> &LogStore {
        &self.logs
    }

    /// Answer an admission short-circuit without replicating. A replay
    /// answers with the revision and log position its receipt says the
    /// original committed at, as the original call was answered. A
    /// receipt read from the writer's staged state records neither yet:
    /// its original is still committing, so this waits (bounded) for the
    /// committed receipt. Receipts written before they recorded either,
    /// and refusals, answer with the current revision and position 0.
    async fn done_response(
        &self,
        db: &[u8],
        options: &MutationOptions,
        decided: Decided,
    ) -> Result<WriteResponse, NodeError> {
        let (done, committed, duplicate) = match decided {
            Decided::Done(done) => (done, None, false),
            Decided::Replayed(done, Some(committed)) => (done, Some(committed), true),
            Decided::Replayed(done, None) => {
                let committed = self.committed_receipt(db, options).await?;
                (done, committed, true)
            }
        };
        let (revision, position) = match committed {
            Some(committed) => (committed.revision, committed.position),
            None => {
                let read_db = db.to_vec();
                let revision = self
                    .with_engine(move |engine| {
                        let read = engine.store().begin_read().map_err(NodeError::engine)?;
                        read.meta_u64(&read_db, meta::REVISION)
                            .map_err(NodeError::engine)
                    })
                    .await?;
                (revision, 0)
            }
        };
        let mut response = match done {
            Ok(value) => {
                let bytes = render(&value).map_err(NodeError::engine)?;
                WriteResponse::value(bytes, revision, position)
            }
            Err(failure) => {
                let bytes = render(&encode_err(&failure)).map_err(NodeError::engine)?;
                WriteResponse::failure(bytes, revision, position)
            }
        };
        response.duplicate = duplicate;
        Ok(response)
    }

    /// Where a replayed call's committed receipt says it committed,
    /// waiting up to [`RECEIPT_COMMIT_WAIT`] while none is committed yet
    /// (its original still in a batch being applied). `None` when the
    /// committed receipt records nothing or none appears in time.
    async fn committed_receipt(
        &self,
        db: &[u8],
        options: &MutationOptions,
    ) -> Result<Option<Committed>, NodeError> {
        let deadline = tokio::time::Instant::now() + RECEIPT_COMMIT_WAIT;
        loop {
            let (read_db, read_options) = (db.to_vec(), options.clone());
            let found = self
                .with_engine(move |engine| {
                    engine
                        .receipt_commit(&read_db, &read_options)
                        .map_err(NodeError::engine)
                })
                .await?;
            match found {
                Some(committed) => return Ok(committed),
                None if tokio::time::Instant::now() >= deadline => return Ok(None),
                None => tokio::time::sleep(Duration::from_millis(10)).await,
            }
        }
    }
}

/// A refused key request: a leading `CODE: ` names its code, anything
/// else is a `KEY_REQUEST` failure.
fn key_failure(message: &str) -> NodeError {
    if let Some((code, rest)) = message.split_once(": ")
        && !code.is_empty()
        && code
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return NodeError::coded(code, rest);
    }
    // A database that does not take the change here: moving, moved,
    // retired or held by a prepared transaction.
    if let Some((code, rest)) = message.split_once(": ")
        && not_serving(code)
    {
        return NodeError::coded(code, rest);
    }
    NodeError::coded(KEY_REQUEST, message)
}

/// Stored catalogs by database as operators list them: `{databases:
/// {<db>: {revision, value}}}` with each public catalog.
pub(crate) fn key_listing(catalogs: KeyCatalogs) -> Result<serde_json::Value, String> {
    let mut databases = serde_json::Map::new();
    for (db, bytes) in catalogs {
        let catalog = flower_crypto::catalog::decode(Some(&bytes))?;
        databases.insert(
            String::from_utf8_lossy(&db).into_owned(),
            serde_json::json!({
                "revision": catalog.revision,
                "value": flower_crypto::catalog::public(&catalog),
            }),
        );
    }
    Ok(serde_json::json!({ "databases": databases }))
}

/// Failure code of key requests the catalog refused.
pub const KEY_REQUEST: &str = "KEY_REQUEST";

/// Evaluate a query on the engine reader, with the revision of the
/// snapshot it read: value and revision come from one capture.
/// Callers linearize and admit first.
pub fn local_query(
    reader: &EngineReader,
    db: &[u8],
    module: &str,
    name: &str,
    args: &Value,
    options: &CallOptions,
) -> Result<(Value, u64), NodeError> {
    reader
        .query_at(db, module, name, args, options)
        .map_err(NodeError::application)
}

/// Run a query's `evaluation` on this thread and account for it: its
/// method's per-method costs (the thread's CPU time when it evaluated, a
/// cache hit when the cache answered), and the group's cache lookup.
/// Also how the cache lookup went, for the request's `Server-Timing`
/// (see [`stages::cache`]).
pub(crate) fn measured_query<T>(
    observer: &Observer,
    queued: Option<Instant>,
    db: &[u8],
    module: &str,
    name: &str,
    evaluation: impl FnOnce() -> Result<T, NodeError>,
) -> (Result<T, NodeError>, Option<Lookup>) {
    // Its wait for this blocking thread, when telemetry is on.
    if let Some(queued) = queued {
        observer.query_stage_took(QueryStage::BlockingQueue, queued.elapsed());
    }
    // A lookup an earlier query on this thread left is not this one's.
    let _ = flower_engine::query_cache::take_lookup();
    let work = Work::start();
    let result = evaluation();
    let (wall, cpu) = work.elapsed();
    let method = MethodRef { db, module, name };
    let lookup = flower_engine::query_cache::take_lookup();
    match lookup {
        Some(Lookup::Hit) => methods::query_cached(method),
        // Only a query that answered resolved its name.
        _ if result.is_ok() => methods::query_evaluated(method, cpu.unwrap_or(wall)),
        _ => {}
    }
    if let Some(lookup) = lookup {
        observer.query_cache(lookup.hit());
    }
    (result, lookup)
}

/// Drive `futures` to completion together on this task, their outputs
/// in order.
async fn join_all<F: std::future::Future>(futures: Vec<F>) -> Vec<F::Output> {
    let mut futures: Vec<std::pin::Pin<Box<F>>> = futures.into_iter().map(Box::pin).collect();
    let mut outputs: Vec<Option<F::Output>> = futures.iter().map(|_| None).collect();
    std::future::poll_fn(|context| {
        let mut pending = false;
        for (future, output) in futures.iter_mut().zip(outputs.iter_mut()) {
            if output.is_none() {
                match future.as_mut().poll(context) {
                    std::task::Poll::Ready(value) => *output = Some(value),
                    std::task::Poll::Pending => pending = true,
                }
            }
        }
        if pending {
            std::task::Poll::Pending
        } else {
            std::task::Poll::Ready(())
        }
    })
    .await;
    outputs.into_iter().flatten().collect()
}

/// Whether a response is a stale-revision rejection worth retrying.
fn is_stale(response: &WriteResponse) -> bool {
    matches!(
        &response.outcome,
        ResponseOutcome::Error(message) if message.starts_with("stale_revision")
    )
}

/// A committed queue or timer patch's outcome: success, or its
/// deterministic rejection as a business failure.
fn committed(response: &WriteResponse) -> Result<(), Failure> {
    match &response.outcome {
        ResponseOutcome::Value(_) => Ok(()),
        ResponseOutcome::Failure(_) => Err(Failure::new("rejected", "operation was rejected")),
        ResponseOutcome::Error(message) => {
            let (code, detail) = message.split_once(": ").unwrap_or(("rejected", message));
            Err(Failure::new(code, detail))
        }
    }
}

/// A member's Raft status.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeStatus {
    /// This node's ID.
    pub id: NodeId,
    /// Server state name.
    pub state: String,
    /// Current leader, if known.
    pub leader: Option<NodeId>,
    /// Current term.
    pub term: u64,
    /// Last appended log index.
    pub last_log: Option<u64>,
    /// Last applied log index.
    pub last_applied: Option<u64>,
}
