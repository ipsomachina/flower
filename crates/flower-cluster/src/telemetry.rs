//! OpenTelemetry metrics and spans of a member's work: queries, the writer,
//! the evaluator, storage, Raft and watches. The server installs the
//! exporters and turns this on ([`set_enabled`]); off, every recording site
//! costs one atomic load and builds nothing.
//!
//! Metric families, all under the `flower` meter, every series labelled with
//! its group (`flower.group`):
//!
//! - `flower.query.stage.duration` (seconds, by `stage`: `fresh_fence`,
//!   `admission`, `evaluation`, `blocking_queue`; and `outcome`) and
//!   `flower.query.cache.lookups` (by `outcome`: `hit`, `miss`);
//! - `flower.writer.stage.duration` (by `stage`: a write's
//!   `admission`, `queue`, `prepare` and `commit`, its batch's
//!   `batch_prepare`, `batch_fill`, `batch_commit`, `append`,
//!   `replicate` and `apply`),
//!   `flower.writer.request.duration` and `flower.writer.requests` (by
//!   `outcome`), `flower.writer.queue.rejected` (by `reason`),
//!   `flower.writer.batches` (by `outcome` and `stop_reason`),
//!   `flower.writer.batch.size` (by `component`),
//!   `flower.writer.batch.bytes`, `flower.writer.queue.depth`,
//!   `flower.writer.replication.lag` (by `kind`), and the leader's
//!   maintenance tasks: `flower.writer.maintenance.runs` (by `outcome`:
//!   `ran`, `failed`, `stale`, `skipped`) and
//!   `flower.writer.maintenance.wakes` (by `reason`: `due`, `write`,
//!   `leadership`);
//! - `flower.evaluator.invocations`,
//!   `flower.evaluator.invocation.duration`,
//!   `flower.evaluator.stage.duration` (`guest`, `host`) and
//!   `flower.evaluator.work.count` (host calls, reads, writes, nested
//!   calls, depth), by `mode` and `outcome`, and
//!   `flower.evaluator.bundle.preparation.duration`;
//! - `flower.storage.operations`, `flower.storage.operation.duration`,
//!   `flower.storage.stage.duration`, `flower.storage.batch.size` and
//!   `flower.storage.encoded.bytes`, by `operation` and `outcome`;
//! - `flower.raft.rpc.duration` (by `operation`: `append`,
//!   `heartbeat`, `vote`, `snapshot`, `read-fence`; and `outcome`),
//!   `flower.raft.append_entries.timeouts` (by `peer`),
//!   `flower.raft.snapshot.duration` (by `phase` and `outcome`) and the
//!   `flower.raft.snapshot.in_progress` gauge;
//! - `flower.watch.delivery.duration` (by `outcome`).
//!
//! Dimensions are bounded classes, never method names, keys, values,
//! principals or error text. Each group's series are bound to their
//! attributes the first time they record ([`Observer`]), so a recording
//! builds no attribute set and takes no series-map lock: when on, a
//! measurement costs a clock read and an atomic update, a span costs
//! its allocation.
//!
//! Spans (target [`OTEL_TARGET`]) use these names: `flower.query.stage`,
//! `flower.writer.submit` (each write, under its request),
//! `flower.writer.batch` (a root linking its writes' spans),
//! `flower.writer.prepare`, `flower.writer.commit` and
//! `flower.writer.replicate` (the log index and `flower.raft.append_ms`,
//! `replicate_ms` and `apply_ms`, with events `appended`, `committed` and
//! `applied`), `flower.evaluator.invocation`,
//! `flower.evaluator.bundle.prepare`, `flower.storage.operation`,
//! `flower.raft.rpc`, `flower.raft.snapshot` and `flower.watch.deliver`.
//!
//! [`CommitMarks`] are always on: `Server-Timing` splits `commit` into
//! `append`, `replicate` and `apply` with them.

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::{Duration, Instant, SystemTime};

use opentelemetry::metrics::{BoundCounter, BoundHistogram, Counter, Histogram, ObservableGauge};
use opentelemetry::trace::{SpanContext, TraceContextExt};
use opentelemetry::{KeyValue, global};
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// The target of the spans OpenTelemetry exports.
pub const OTEL_TARGET: &str = "flower::otel";

/// Whether OpenTelemetry exports in this process.
static ENABLED: AtomicBool = AtomicBool::new(false);

/// Whether OpenTelemetry exports: recording sites check it first.
pub fn enabled() -> bool {
    ENABLED.load(Ordering::Acquire)
}

/// Turn recording on once the exporters are installed (the meter
/// provider first: instruments are built on first use), or off at
/// shutdown.
pub fn set_enabled(on: bool) {
    ENABLED.store(on, Ordering::Release);
}

/// Seconds, microseconds through a minute.
fn seconds() -> Vec<f64> {
    vec![
        0.000_001, 0.000_005, 0.000_010, 0.000_025, 0.000_050, 0.000_100, 0.000_250, 0.000_500,
        0.001, 0.0025, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, 1.0, 2.5, 5.0, 10.0, 30.0,
        60.0,
    ]
}

/// Counts, zero through a million.
fn counts() -> Vec<f64> {
    vec![
        0.0,
        1.0,
        2.0,
        4.0,
        8.0,
        16.0,
        32.0,
        64.0,
        128.0,
        256.0,
        1_024.0,
        4_096.0,
        16_384.0,
        65_536.0,
        1_048_576.0,
    ]
}

/// Bytes, zero through a gibibyte.
fn bytes() -> Vec<f64> {
    vec![
        0.0,
        256.0,
        1_024.0,
        4_096.0,
        16_384.0,
        65_536.0,
        262_144.0,
        1_048_576.0,
        4_194_304.0,
        16_777_216.0,
        67_108_864.0,
        268_435_456.0,
        1_073_741_824.0,
    ]
}

/// Every instrument, built once telemetry is on.
struct Instruments {
    query_stage: Histogram<f64>,
    query_cache: Counter<u64>,
    writer_stage: Histogram<f64>,
    writer_request: Histogram<f64>,
    writer_requests: Counter<u64>,
    writer_rejected: Counter<u64>,
    writer_speculation: Counter<u64>,
    writer_batches: Counter<u64>,
    maintenance_runs: Counter<u64>,
    maintenance_wakes: Counter<u64>,
    writer_batch_size: Histogram<u64>,
    writer_batch_bytes: Histogram<u64>,
    writer_queue_depth: Histogram<u64>,
    writer_lag: Histogram<u64>,
    evaluator_invocations: Counter<u64>,
    evaluator_duration: Histogram<f64>,
    evaluator_stage: Histogram<f64>,
    evaluator_work: Histogram<u64>,
    bundle: Histogram<f64>,
    storage_operations: Counter<u64>,
    storage_duration: Histogram<f64>,
    storage_stage: Histogram<f64>,
    storage_batch: Histogram<u64>,
    storage_bytes: Histogram<u64>,
    rpc: Histogram<f64>,
    append_timeouts: Counter<u64>,
    snapshot: Histogram<f64>,
    _snapshot_in_progress: ObservableGauge<u64>,
    watch_delivery: Histogram<f64>,
}

fn instruments() -> Option<&'static Instruments> {
    if !enabled() {
        return None;
    }
    static INSTRUMENTS: OnceLock<Instruments> = OnceLock::new();
    Some(INSTRUMENTS.get_or_init(|| {
        let meter = global::meter("flower");
        let histogram = |name: &'static str, unit: &'static str, description: &'static str| {
            meter
                .f64_histogram(name)
                .with_unit(unit)
                .with_description(description)
                .with_boundaries(seconds())
                .build()
        };
        let sizes = |name: &'static str, boundaries: Vec<f64>, description: &'static str| {
            meter
                .u64_histogram(name)
                .with_description(description)
                .with_boundaries(boundaries)
                .build()
        };
        let counter = |name: &'static str, description: &'static str| {
            meter
                .u64_counter(name)
                .with_description(description)
                .build()
        };
        Instruments {
            query_stage: histogram(
                "flower.query.stage.duration",
                "s",
                "Query stages: the fresh-read proof, admission and evaluation",
            ),
            query_cache: counter(
                "flower.query.cache.lookups",
                "Query result cache lookups by outcome",
            ),
            writer_stage: histogram(
                "flower.writer.stage.duration",
                "s",
                "Writer stages: a write's queue wait, preparation and commit; its batch's \
                 preparation, commit, append, replication and apply",
            ),
            writer_request: histogram(
                "flower.writer.request.duration",
                "s",
                "Writer submission through its answer, queueing and commit included",
            ),
            writer_requests: counter(
                "flower.writer.requests",
                "Writer submissions answered, by bounded outcome",
            ),
            writer_rejected: counter(
                "flower.writer.queue.rejected",
                "Writes refused at their turn in the writer queue, by reason",
            ),
            writer_speculation: counter(
                "flower.writer.speculation.rejections",
                "Speculative candidates prepared again in their turn, by reason and class of \
                 conflicting row",
            ),
            maintenance_runs: counter(
                "flower.writer.maintenance.runs",
                "Maintenance task runs by outcome",
            ),
            maintenance_wakes: counter(
                "flower.writer.maintenance.wakes",
                "What woke a group's maintenance loop: a task due, a write, or a leadership change",
            ),
            writer_batches: counter(
                "flower.writer.batches",
                "Writer batches by outcome and why the batch stopped growing",
            ),
            writer_batch_size: sizes(
                "flower.writer.batch.size",
                counts(),
                "Writes per batch: requests, proposed items, writes decided without proposing",
            ),
            writer_batch_bytes: meter
                .u64_histogram("flower.writer.batch.bytes")
                .with_unit("By")
                .with_description("Encoded item bytes per writer batch")
                .with_boundaries(bytes())
                .build(),
            writer_queue_depth: sizes(
                "flower.writer.queue.depth",
                counts(),
                "Writes waiting in the writer queue when a batch was proposed",
            ),
            writer_lag: sizes(
                "flower.writer.replication.lag",
                counts(),
                "Log entries not applied locally, and not matched by a quorum, when a batch was \
                 proposed",
            ),
            evaluator_invocations: counter(
                "flower.evaluator.invocations",
                "Guest invocations by mode and outcome",
            ),
            evaluator_duration: histogram(
                "flower.evaluator.invocation.duration",
                "s",
                "Guest invocation wall time",
            ),
            evaluator_stage: histogram(
                "flower.evaluator.stage.duration",
                "s",
                "Guest invocation wall time in the guest and in host calls; nested invocations \
                 are inside their caller's host time",
            ),
            evaluator_work: sizes(
                "flower.evaluator.work.count",
                counts(),
                "Host calls, reads, writes, nested calls and call depth per invocation",
            ),
            bundle: histogram(
                "flower.evaluator.bundle.preparation.duration",
                "s",
                "Preparing a module's runtime from its code: compilation or the image cache",
            ),
            storage_operations: counter(
                "flower.storage.operations",
                "Storage operations by completion outcome",
            ),
            storage_duration: histogram(
                "flower.storage.operation.duration",
                "s",
                "Storage operation elapsed time, queueing included",
            ),
            storage_stage: histogram(
                "flower.storage.stage.duration",
                "s",
                "Storage stages; commit includes the fsync",
            ),
            storage_batch: sizes(
                "flower.storage.batch.size",
                counts(),
                "Entries, items and changed records per storage operation",
            ),
            storage_bytes: meter
                .u64_histogram("flower.storage.encoded.bytes")
                .with_unit("By")
                .with_description("Serialized bytes per storage operation")
                .with_boundaries(bytes())
                .build(),
            rpc: histogram(
                "flower.raft.rpc.duration",
                "s",
                "Raft and read-fence RPCs to peers, by operation and outcome",
            ),
            append_timeouts: counter(
                "flower.raft.append_entries.timeouts",
                "AppendEntries RPCs whose answer missed the Raft RPC timeout",
            ),
            snapshot: meter
                .f64_histogram("flower.raft.snapshot.duration")
                .with_unit("s")
                .with_description("Raft snapshot builds and log purges, by phase")
                .with_boundaries(vec![
                    0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0,
                    120.0,
                ])
                .build(),
            _snapshot_in_progress: meter
                .u64_observable_gauge("flower.raft.snapshot.in_progress")
                .with_description(
                    "1 while this process builds a Raft snapshot or purges the log after one",
                )
                .with_callback(|observer| {
                    observer.observe(u64::from(snapshot_activity().running > 0), &[]);
                })
                .build(),
            watch_delivery: meter
                .f64_histogram("flower.watch.delivery.duration")
                .with_unit("s")
                .with_description(
                    "From the commit that woke a watch to enqueueing its changed result, finding \
                     it unchanged, or the watch's end",
                )
                .with_boundaries(vec![
                    0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
                    10.0, 20.0, 30.0, 60.0,
                ])
                .build(),
        }
    }))
}

/// Instruments bound lazily, one per index: binding builds the
/// attribute set once, recording never does.
struct Slots<T>(Box<[OnceLock<T>]>);

impl<T> Slots<T> {
    fn new(len: usize) -> Self {
        Slots((0..len).map(|_| OnceLock::new()).collect())
    }

    fn get(&self, index: usize, bind: impl FnOnce() -> T) -> &T {
        self.0[index].get_or_init(bind)
    }
}

/// A bounded attribute value: a closed set of names.
pub trait Class: Copy + 'static {
    /// Every value, in index order.
    const ALL: &'static [Self];
    /// Its attribute value.
    fn name(self) -> &'static str;
    /// Its index in [`Class::ALL`].
    fn index(self) -> usize;
}

macro_rules! class {
    ($(#[$meta:meta])* $name:ident { $($(#[$vmeta:meta])* $variant:ident => $text:literal),+ $(,)? }) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        pub enum $name {
            $($(#[$vmeta])* $variant),+
        }

        impl Class for $name {
            const ALL: &'static [Self] = &[$($name::$variant),+];

            fn name(self) -> &'static str {
                match self {
                    $($name::$variant => $text),+
                }
            }

            fn index(self) -> usize {
                self as usize
            }
        }
    };
}

class!(
    /// How a maintenance task run ended.
    MaintenanceOutcome {
        /// It ran and committed.
        Ran => "ran",
        /// It failed; its failure was handled or recorded.
        Failed => "failed",
        /// It went stale under its preparation.
        Stale => "stale",
        /// It was no longer due, or its database did not serve here.
        Skipped => "skipped",
    }
);

class!(
    /// What woke a group's maintenance loop.
    MaintenanceWake {
        /// A task fell due, or its interval passed.
        Due => "due",
        /// A write committed.
        Write => "write",
        /// Leadership changed.
        Leadership => "leadership",
    }
);

class!(
    /// A query stage.
    QueryStage {
        /// Proving a fresh read.
        Fence => "fresh_fence",
        /// Waiting for an evaluation slot.
        Admission => "admission",
        /// Evaluating on the reader, its wait for a blocking thread
        /// included.
        Evaluation => "evaluation",
        /// Waiting for a blocking thread to evaluate on.
        BlockingQueue => "blocking_queue",
        /// Waiting for an identical query in flight to land (see
        /// [`crate::flight`]).
        CoalesceWait => "coalesce_wait",
    }
);

class!(
    /// How a stage or call ended.
    Outcome {
        /// It succeeded.
        Ok => "ok",
        /// It failed.
        Error => "error",
        /// Its caller stopped waiting first.
        Cancelled => "cancelled",
    }
);

class!(
    /// A writer stage.
    WriterStage {
        /// A write's wait for its place in the writer queue.
        Admission => "admission",
        /// A write's wait for its first turn.
        Queue => "queue",
        /// A write's preparations.
        Prepare => "prepare",
        /// A write's time after its preparation until answered.
        Commit => "commit",
        /// A batch's preparation, every chunk of it.
        BatchPrepare => "batch_prepare",
        /// From a batch's first preparation to its proposal: filling
        /// while its predecessor commits.
        BatchFill => "batch_fill",
        /// A batch's commit through Raft.
        BatchCommit => "batch_commit",
        /// Until the batch's entry was durable in the leader's log.
        Append => "append",
        /// Until a quorum committed it.
        Replicate => "replicate",
        /// Applying it.
        Apply => "apply",
    }
);

class!(
    /// How a write submitted to the writer ended.
    WriteOutcome {
        /// Answered: committed, or decided at its preparation.
        Ok => "ok",
        /// Refused at its turn, unprepared.
        Refused => "refused",
        /// Failed: its batch failed, or kept going stale.
        Error => "error",
        /// Its caller stopped waiting first.
        Cancelled => "cancelled",
    }
);

class!(
    /// Why a write was refused at its turn.
    Refusal {
        /// Its client deadline passed.
        Deadline => "deadline",
        /// CoDel shed it.
        Shed => "shed",
        /// Another reason.
        Other => "other",
    }
);

class!(
    /// Why a speculative candidate was prepared again in its turn.
    Rejection {
        /// A write before it touched a row it read.
        Certificate => "certificate",
        /// It read something no row names.
        Untracked => "untracked",
        /// Its speculative preparation failed.
        Failed => "failed",
    }
);

class!(
    /// The class of row a rejected candidate conflicted on.
    ConflictKind {
        /// A record.
        Source => "source",
        /// An index entry.
        Index => "index",
        /// A derived outcome or its edges.
        Cell => "cell",
        /// A receipt.
        Receipt => "receipt",
        /// Database metadata.
        Meta => "meta",
        /// Anything else.
        Other => "other",
        /// No row: the candidate was not refused by a conflict.
        None => "none",
    }
);

impl ConflictKind {
    /// The kind of a conflict class.
    pub fn of(class: flower_engine::speculation::ConflictClass) -> Self {
        use flower_engine::speculation::ConflictClass;
        match class {
            ConflictClass::Source => ConflictKind::Source,
            ConflictClass::Index => ConflictKind::Index,
            ConflictClass::Cell => ConflictKind::Cell,
            ConflictClass::Receipt => ConflictKind::Receipt,
            ConflictClass::Meta => ConflictKind::Meta,
            ConflictClass::Other => ConflictKind::Other,
        }
    }
}

class!(
    /// How a writer batch ended.
    BatchOutcome {
        /// Committed.
        Ok => "ok",
        /// Went stale and was prepared again.
        Stale => "stale",
        /// Failed.
        Error => "error",
    }
);

class!(
    /// Why a writer batch stopped growing.
    StopReason {
        /// Nothing was committing: it went once prepared.
        Idle => "idle",
        /// Its predecessor resolved.
        Resolved => "resolved",
        /// It held the most writes a batch carries.
        Items => "items",
        /// It held the most bytes.
        Bytes => "bytes",
        /// It was rebuilt the most times.
        Chunks => "chunks",
    }
);

class!(
    /// A batch size component.
    BatchComponent {
        /// Writes in the batch.
        Requests => "requests",
        /// Items it proposed.
        Commands => "commands",
        /// Writes decided without proposing.
        Decided => "decided",
    }
);

class!(
    /// A replication lag kind.
    Lag {
        /// Entries in the leader's log it has not applied.
        LocalUnapplied => "local_unapplied",
        /// Entries in the leader's log a quorum has not matched.
        QuorumUnmatched => "quorum_unmatched",
    }
);

class!(
    /// What a guest invocation ran.
    Mode {
        /// A query.
        Query => "query",
        /// A mutation.
        Mutation => "mutation",
        /// A derived definition.
        Derived => "derived",
        /// An aggregate's reducer, folding row changes into its value.
        Reduce => "reduce",
        /// A transaction planner.
        Transaction => "transaction",
    }
);

class!(
    /// How a guest invocation ended.
    EvalOutcome {
        /// It answered a value.
        Ok => "ok",
        /// It answered a failure.
        Error => "error",
        /// It trapped: its state is gone.
        Trap => "trap",
    }
);

class!(
    /// A guest invocation stage.
    EvalStage {
        /// In the guest.
        Guest => "guest",
        /// In host calls.
        Host => "host",
    }
);

class!(
    /// A guest invocation's work.
    Work {
        /// Host calls.
        HostCalls => "host_calls",
        /// Reads of stored state.
        Reads => "reads",
        /// Writes to it.
        Writes => "writes",
        /// Calls to other methods.
        Calls => "calls",
        /// How deep in nested calls it ran (1: the top).
        Depth => "depth",
    }
);

class!(
    /// A storage operation.
    StorageOperation {
        /// Raft log entries appended (and flushed).
        Append => "append",
        /// The vote saved.
        Vote => "vote",
        /// A conflicting log tail deleted.
        Truncate => "truncate",
        /// The log purged up to a snapshot.
        Purge => "purge",
        /// Committed entries applied to the group file.
        Apply => "apply",
        /// A snapshot image built.
        BuildSnapshot => "build_snapshot",
        /// A received snapshot image installed.
        InstallSnapshot => "install_snapshot",
    }
);

class!(
    /// How a storage operation ended.
    StorageOutcome {
        /// It completed.
        Ok => "ok",
        /// It failed.
        Error => "error",
        /// Its caller dropped it, or it panicked, before it completed.
        Incomplete => "incomplete",
    }
);

class!(
    /// A storage stage.
    StorageStage {
        /// Encoding entries.
        Encode => "encode",
        /// Waiting for the engine lock and a blocking thread.
        Queue => "queue",
        /// Writing in the transaction.
        Write => "write",
        /// Committing it (with its fsync).
        Commit => "commit",
    }
);

class!(
    /// A storage batch kind.
    StorageBatch {
        /// Log entries.
        Entries => "entries",
        /// Items they carry (a batch entry's writes).
        Commands => "commands",
        /// Records the applied writes put or deleted.
        ChangedRecords => "changed_records",
    }
);

class!(
    /// A peer RPC.
    Rpc {
        /// AppendEntries carrying entries.
        Append => "append",
        /// AppendEntries carrying none.
        Heartbeat => "heartbeat",
        /// A vote request.
        Vote => "vote",
        /// A snapshot segment.
        Snapshot => "snapshot",
        /// A read fence from the leader.
        ReadFence => "read-fence",
    }
);

class!(
    /// A snapshot work phase.
    SnapshotPhase {
        /// Building an image.
        Build => "build",
        /// Purging the log it covers.
        Purge => "purge",
    }
);

class!(
    /// How snapshot work ended.
    SnapshotOutcome {
        /// The image became the current snapshot.
        Published => "published",
        /// The purge finished.
        Swept => "swept",
        /// It failed.
        Failed => "failed",
        /// It was dropped before it finished.
        Abandoned => "abandoned",
    }
);

class!(
    /// How a write-woken watch delivery ended.
    Delivery {
        /// The changed result was enqueued.
        Delivered => "delivered",
        /// The result did not change.
        Unchanged => "unchanged",
        /// The watch ended first.
        Ended => "ended",
    }
);

/// The evaluator's series of one mode and outcome.
struct EvaluatorBound {
    invocations: BoundCounter<u64>,
    duration: BoundHistogram<f64>,
    stages: Vec<BoundHistogram<f64>>,
    work: Vec<BoundHistogram<u64>>,
}

/// The storage series of one operation and outcome.
struct StorageBound {
    operations: BoundCounter<u64>,
    duration: BoundHistogram<f64>,
    bytes: BoundHistogram<u64>,
    stages: Vec<BoundHistogram<f64>>,
    batch: Vec<BoundHistogram<u64>>,
}

/// One group's series, bound as they are first recorded.
struct Bound {
    query_stage: Slots<BoundHistogram<f64>>,
    query_cache: Slots<BoundCounter<u64>>,
    writer_stage: Slots<BoundHistogram<f64>>,
    writer_request: Slots<(BoundHistogram<f64>, BoundCounter<u64>)>,
    writer_rejected: Slots<BoundCounter<u64>>,
    writer_speculation: Slots<BoundCounter<u64>>,
    writer_batches: Slots<BoundCounter<u64>>,
    maintenance_runs: Slots<BoundCounter<u64>>,
    maintenance_wakes: Slots<BoundCounter<u64>>,
    writer_batch_size: Slots<BoundHistogram<u64>>,
    writer_batch_bytes: Slots<BoundHistogram<u64>>,
    writer_queue_depth: Slots<BoundHistogram<u64>>,
    writer_lag: Slots<BoundHistogram<u64>>,
    evaluator: Slots<EvaluatorBound>,
    bundle: Slots<BoundHistogram<f64>>,
    storage: Slots<StorageBound>,
    rpc: Slots<BoundHistogram<f64>>,
    snapshot: Slots<BoundHistogram<f64>>,
    watch: Slots<BoundHistogram<f64>>,
}

impl Bound {
    fn new() -> Self {
        Bound {
            query_stage: Slots::new(QueryStage::ALL.len() * Outcome::ALL.len()),
            query_cache: Slots::new(2),
            writer_stage: Slots::new(WriterStage::ALL.len()),
            writer_request: Slots::new(WriteOutcome::ALL.len()),
            writer_rejected: Slots::new(Refusal::ALL.len()),
            writer_speculation: Slots::new(Rejection::ALL.len() * ConflictKind::ALL.len()),
            writer_batches: Slots::new(BatchOutcome::ALL.len() * StopReason::ALL.len()),
            maintenance_runs: Slots::new(MaintenanceOutcome::ALL.len()),
            maintenance_wakes: Slots::new(MaintenanceWake::ALL.len()),
            writer_batch_size: Slots::new(BatchComponent::ALL.len()),
            writer_batch_bytes: Slots::new(1),
            writer_queue_depth: Slots::new(1),
            writer_lag: Slots::new(Lag::ALL.len()),
            evaluator: Slots::new(Mode::ALL.len() * EvalOutcome::ALL.len()),
            bundle: Slots::new(2),
            storage: Slots::new(StorageOperation::ALL.len() * StorageOutcome::ALL.len()),
            rpc: Slots::new(Rpc::ALL.len() * Outcome::ALL.len()),
            snapshot: Slots::new(SnapshotPhase::ALL.len() * SnapshotOutcome::ALL.len()),
            watch: Slots::new(Delivery::ALL.len()),
        }
    }
}

/// A group's instruments on this member, shared by everything working
/// for the group (see [`observer`]).
pub struct Observer {
    group: Arc<str>,
    bound: OnceLock<Bound>,
}

impl std::fmt::Debug for Observer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Observer")
            .field("group", &self.group)
            .finish()
    }
}

/// The observer of the group named `group`: one per name per process.
pub fn observer(group: &str) -> Arc<Observer> {
    static OBSERVERS: OnceLock<Mutex<HashMap<Arc<str>, Arc<Observer>>>> = OnceLock::new();
    let mut observers = OBSERVERS
        .get_or_init(Mutex::default)
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    if let Some(observer) = observers.get(group) {
        return observer.clone();
    }
    let name: Arc<str> = group.into();
    let observer = Arc::new(Observer {
        group: name.clone(),
        bound: OnceLock::new(),
    });
    observers.insert(name, observer.clone());
    observer
}

impl Observer {
    /// The group's name.
    pub fn group(&self) -> &str {
        &self.group
    }

    /// The instruments and this group's series, when on.
    fn bound(&self) -> Option<(&'static Instruments, &Bound)> {
        let instruments = instruments()?;
        Some((instruments, self.bound.get_or_init(Bound::new)))
    }

    /// The group's attribute plus `extra`.
    fn attributes(&self, extra: &[(&'static str, &'static str)]) -> Vec<KeyValue> {
        let mut attributes = Vec::with_capacity(extra.len() + 1);
        attributes.push(KeyValue::new("flower.group", self.group.to_string()));
        attributes.extend(extra.iter().map(|(key, value)| KeyValue::new(*key, *value)));
        attributes
    }

    /// Run a query stage: a `flower.query.stage` span and its duration.
    pub async fn query_stage<T, E>(
        &self,
        stage: QueryStage,
        future: impl Future<Output = Result<T, E>>,
    ) -> Result<T, E> {
        if !enabled() {
            return future.await;
        }
        let span = tracing::info_span!(
            target: OTEL_TARGET,
            "flower.query.stage",
            stage = stage.name(),
            flower.group = %self.group,
            outcome = tracing::field::Empty,
            otel.status_code = tracing::field::Empty,
        );
        let mut timed =
            Timed::new(span.clone(), |outcome, elapsed| {
                if let Some((instruments, bound)) = self.bound() {
                    bound
                        .query_stage
                        .get(stage.index() * Outcome::ALL.len() + outcome.index(), || {
                            instruments.query_stage.bind(&self.attributes(&[
                                ("stage", stage.name()),
                                ("outcome", outcome.name()),
                            ]))
                        })
                        .record(elapsed.as_secs_f64());
                }
            });
        let result = future.instrument(span).await;
        timed.outcome = if result.is_ok() {
            Outcome::Ok
        } else {
            Outcome::Error
        };
        result
    }

    /// A query stage that ran without a span of its own took `elapsed`.
    pub fn query_stage_took(&self, stage: QueryStage, elapsed: Duration) {
        if let Some((instruments, bound)) = self.bound() {
            bound
                .query_stage
                .get(
                    stage.index() * Outcome::ALL.len() + Outcome::Ok.index(),
                    || {
                        instruments.query_stage.bind(&self.attributes(&[
                            ("stage", stage.name()),
                            ("outcome", Outcome::Ok.name()),
                        ]))
                    },
                )
                .record(elapsed.as_secs_f64());
        }
    }

    /// A query cache lookup: `hit` or not.
    pub fn query_cache(&self, hit: bool) {
        if let Some((instruments, bound)) = self.bound() {
            let outcome = if hit { "hit" } else { "miss" };
            bound
                .query_cache
                .get(usize::from(!hit), || {
                    instruments
                        .query_cache
                        .bind(&self.attributes(&[("outcome", outcome)]))
                })
                .add(1);
        }
    }

    /// One writer stage took `elapsed`.
    pub fn writer_stage(&self, stage: WriterStage, elapsed: Duration) {
        if let Some((instruments, bound)) = self.bound() {
            bound
                .writer_stage
                .get(stage.index(), || {
                    instruments
                        .writer_stage
                        .bind(&self.attributes(&[("stage", stage.name())]))
                })
                .record(elapsed.as_secs_f64());
        }
    }

    /// A write submitted to the writer was answered after `elapsed`.
    pub fn writer_request(&self, outcome: WriteOutcome, elapsed: Duration) {
        if let Some((instruments, bound)) = self.bound() {
            let (duration, requests) = bound.writer_request.get(outcome.index(), || {
                let attributes = self.attributes(&[("outcome", outcome.name())]);
                (
                    instruments.writer_request.bind(&attributes),
                    instruments.writer_requests.bind(&attributes),
                )
            });
            duration.record(elapsed.as_secs_f64());
            requests.add(1);
        }
    }

    /// A write was refused at its turn.
    pub fn writer_rejected(&self, reason: Refusal) {
        if let Some((instruments, bound)) = self.bound() {
            bound
                .writer_rejected
                .get(reason.index(), || {
                    instruments
                        .writer_rejected
                        .bind(&self.attributes(&[("reason", reason.name())]))
                })
                .add(1);
        }
    }

    /// `count` maintenance task runs ended `outcome`.
    pub fn maintenance_ran(&self, outcome: MaintenanceOutcome, count: u64) {
        if count == 0 {
            return;
        }
        if let Some((instruments, bound)) = self.bound() {
            bound
                .maintenance_runs
                .get(outcome.index(), || {
                    instruments
                        .maintenance_runs
                        .bind(&self.attributes(&[("outcome", outcome.name())]))
                })
                .add(count);
        }
    }

    /// The group's maintenance loop woke, for `why`.
    pub fn maintenance_woke(&self, why: MaintenanceWake) {
        if let Some((instruments, bound)) = self.bound() {
            bound
                .maintenance_wakes
                .get(why.index(), || {
                    instruments
                        .maintenance_wakes
                        .bind(&self.attributes(&[("reason", why.name())]))
                })
                .add(1);
        }
    }

    /// A speculative candidate was prepared again in its turn, for
    /// `reason`, conflicting on a row of `class` (one count per class).
    pub fn writer_speculation_rejected(&self, reason: Rejection, class: ConflictKind) {
        if let Some((instruments, bound)) = self.bound() {
            let index = reason.index() * ConflictKind::ALL.len() + class.index();
            bound
                .writer_speculation
                .get(index, || {
                    instruments.writer_speculation.bind(
                        &self.attributes(&[("reason", reason.name()), ("class", class.name())]),
                    )
                })
                .add(1);
        }
    }

    /// A batch resolved: how, why it stopped growing, and its sizes.
    pub fn writer_batch(&self, batch: &BatchReport) {
        let Some((instruments, bound)) = self.bound() else {
            return;
        };
        bound
            .writer_batches
            .get(
                batch.outcome.index() * StopReason::ALL.len() + batch.stop.index(),
                || {
                    instruments.writer_batches.bind(&self.attributes(&[
                        ("outcome", batch.outcome.name()),
                        ("stop_reason", batch.stop.name()),
                    ]))
                },
            )
            .add(1);
        for (component, size) in [
            (BatchComponent::Requests, batch.requests),
            (BatchComponent::Commands, batch.commands),
            (BatchComponent::Decided, batch.decided),
        ] {
            bound
                .writer_batch_size
                .get(component.index(), || {
                    instruments
                        .writer_batch_size
                        .bind(&self.attributes(&[("component", component.name())]))
                })
                .record(size);
        }
        bound
            .writer_batch_bytes
            .get(0, || {
                instruments.writer_batch_bytes.bind(&self.attributes(&[]))
            })
            .record(batch.bytes);
        bound
            .writer_queue_depth
            .get(0, || {
                instruments.writer_queue_depth.bind(&self.attributes(&[]))
            })
            .record(batch.queued);
        for (kind, lag) in [
            (Lag::LocalUnapplied, batch.local_unapplied),
            (Lag::QuorumUnmatched, batch.quorum_unmatched),
        ] {
            bound
                .writer_lag
                .get(kind.index(), || {
                    instruments
                        .writer_lag
                        .bind(&self.attributes(&[("kind", kind.name())]))
                })
                .record(lag);
        }
    }

    /// A guest invocation ended.
    fn evaluation(&self, mode: Mode, outcome: EvalOutcome, report: &InvocationReport) {
        let Some((instruments, bound)) = self.bound() else {
            return;
        };
        let series = bound.evaluator.get(
            mode.index() * EvalOutcome::ALL.len() + outcome.index(),
            || {
                let attributes =
                    self.attributes(&[("mode", mode.name()), ("outcome", outcome.name())]);
                let with = |key: &'static str, value: &'static str| {
                    let mut attributes = attributes.clone();
                    attributes.push(KeyValue::new(key, value));
                    attributes
                };
                EvaluatorBound {
                    invocations: instruments.evaluator_invocations.bind(&attributes),
                    duration: instruments.evaluator_duration.bind(&attributes),
                    stages: EvalStage::ALL
                        .iter()
                        .map(|stage| {
                            instruments
                                .evaluator_stage
                                .bind(&with("stage", stage.name()))
                        })
                        .collect(),
                    work: Work::ALL
                        .iter()
                        .map(|kind| instruments.evaluator_work.bind(&with("kind", kind.name())))
                        .collect(),
                }
            },
        );
        series.invocations.add(1);
        series.duration.record(report.total.as_secs_f64());
        series.stages[EvalStage::Guest.index()]
            .record(report.total.saturating_sub(report.host).as_secs_f64());
        series.stages[EvalStage::Host.index()].record(report.host.as_secs_f64());
        for (kind, count) in [
            (Work::HostCalls, report.host_calls),
            (Work::Reads, report.reads),
            (Work::Writes, report.writes),
            (Work::Calls, report.calls),
            (Work::Depth, report.depth),
        ] {
            series.work[kind.index()].record(count);
        }
    }

    /// A module's runtime was prepared after `elapsed`.
    fn bundle(&self, ok: bool, elapsed: Duration) {
        if let Some((instruments, bound)) = self.bound() {
            let outcome = if ok { "ok" } else { "error" };
            bound
                .bundle
                .get(usize::from(!ok), || {
                    instruments
                        .bundle
                        .bind(&self.attributes(&[("stage", "total"), ("outcome", outcome)]))
                })
                .record(elapsed.as_secs_f64());
        }
    }

    /// One peer RPC ended.
    fn rpc(&self, rpc: Rpc, outcome: Outcome, elapsed: Duration) {
        if let Some((instruments, bound)) = self.bound() {
            bound
                .rpc
                .get(rpc.index() * Outcome::ALL.len() + outcome.index(), || {
                    instruments.rpc.bind(
                        &self.attributes(&[("operation", rpc.name()), ("outcome", outcome.name())]),
                    )
                })
                .record(elapsed.as_secs_f64());
        }
    }

    /// Run a peer RPC to `peer`: a `flower.raft.rpc` client span and its
    /// duration; an append (or heartbeat) that answered nothing within
    /// `timeout` counts in `flower.raft.append_entries.timeouts`.
    pub async fn raft_rpc<T, E>(
        &self,
        rpc: Rpc,
        peer: u64,
        timeout: Option<Duration>,
        future: impl Future<Output = Result<T, E>>,
    ) -> Result<T, E> {
        if !enabled() {
            return future.await;
        }
        let span = tracing::info_span!(
            target: OTEL_TARGET,
            "flower.raft.rpc",
            otel.kind = "client",
            operation = rpc.name(),
            flower.peer.id = int(peer),
            flower.group = %self.group,
            outcome = tracing::field::Empty,
            otel.status_code = tracing::field::Empty,
        );
        let mut timed = Timed::new(span.clone(), |outcome, elapsed| {
            self.rpc(rpc, outcome, elapsed);
            if outcome != Outcome::Ok
                && matches!(rpc, Rpc::Append | Rpc::Heartbeat)
                && timeout.is_some_and(|timeout| elapsed >= timeout)
            {
                self.append_timeout(peer, timeout.unwrap_or_default());
            }
        });
        let result = future.instrument(span).await;
        timed.outcome = if result.is_ok() {
            Outcome::Ok
        } else {
            Outcome::Error
        };
        result
    }

    /// An AppendEntries answer missed `timeout`.
    fn append_timeout(&self, peer: u64, timeout: Duration) {
        let timeout_ms = u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX);
        tracing::info!(target: OTEL_TARGET, peer = int(peer), timeout_ms = int(timeout_ms), "append_entries.timeout");
        if let Some(instruments) = instruments() {
            instruments.append_timeouts.add(
                1,
                &[
                    KeyValue::new("flower.group", self.group.to_string()),
                    KeyValue::new("peer", peer.to_string()),
                ],
            );
        }
    }

    /// A write-woken watch delivery ended after `elapsed`.
    pub fn watch_delivery(&self, outcome: Delivery, elapsed: Duration) {
        if let Some((instruments, bound)) = self.bound() {
            bound
                .watch
                .get(outcome.index(), || {
                    instruments
                        .watch_delivery
                        .bind(&self.attributes(&[("outcome", outcome.name())]))
                })
                .record(elapsed.as_secs_f64());
        }
    }

    /// Start timing a storage operation: nothing when off.
    pub fn storage(self: &Arc<Self>, operation: StorageOperation) -> StorageTrace {
        if !enabled() {
            return StorageTrace(None);
        }
        let span = tracing::info_span!(
            target: OTEL_TARGET,
            "flower.storage.operation",
            operation = operation.name(),
            flower.group = %self.group,
            outcome = tracing::field::Empty,
            otel.status_code = tracing::field::Empty,
            encode_seconds = tracing::field::Empty,
            queue_seconds = tracing::field::Empty,
            write_seconds = tracing::field::Empty,
            commit_seconds = tracing::field::Empty,
            total_seconds = tracing::field::Empty,
            encoded_bytes = tracing::field::Empty,
            entries = tracing::field::Empty,
            commands = tracing::field::Empty,
            changed_records = tracing::field::Empty,
        );
        let now = Instant::now();
        StorageTrace(Some(Box::new(StorageTiming {
            observer: self.clone(),
            operation,
            span,
            started: now,
            marked: now,
            stages: [Duration::ZERO; 4],
            bytes: 0,
            sizes: [0; 3],
        })))
    }
}

/// A timed stage: records its outcome and elapsed time on drop, so a
/// caller that stops waiting records `cancelled`.
struct Timed<F: FnMut(Outcome, Duration)> {
    span: tracing::Span,
    started: Instant,
    outcome: Outcome,
    record: F,
}

impl<F: FnMut(Outcome, Duration)> Timed<F> {
    fn new(span: tracing::Span, record: F) -> Self {
        Timed {
            span,
            started: Instant::now(),
            outcome: Outcome::Cancelled,
            record,
        }
    }
}

impl<F: FnMut(Outcome, Duration)> Drop for Timed<F> {
    fn drop(&mut self) {
        self.span.record("outcome", self.outcome.name());
        if self.outcome != Outcome::Ok {
            self.span.record("otel.status_code", "ERROR");
        }
        (self.record)(self.outcome, self.started.elapsed());
    }
}

/// What a resolved writer batch did, for [`Observer::writer_batch`].
#[derive(Clone, Copy, Debug)]
pub struct BatchReport {
    /// How it ended.
    pub outcome: BatchOutcome,
    /// Why it stopped growing.
    pub stop: StopReason,
    /// Writes it carried.
    pub requests: u64,
    /// Items it proposed.
    pub commands: u64,
    /// Writes decided without proposing.
    pub decided: u64,
    /// Encoded item bytes.
    pub bytes: u64,
    /// Writes queued behind it when it was proposed.
    pub queued: u64,
    /// Log entries the leader had not applied then.
    pub local_unapplied: u64,
    /// Log entries no quorum had matched then.
    pub quorum_unmatched: u64,
}

/// A storage operation being timed; reports on drop (`incomplete` unless
/// [`StorageTrace::finish`] said how it ended).
#[derive(Default)]
pub struct StorageTrace(Option<Box<StorageTiming>>);

/// The timings a [`StorageTrace`] keeps when on.
pub struct StorageTiming {
    observer: Arc<Observer>,
    operation: StorageOperation,
    span: tracing::Span,
    started: Instant,
    marked: Instant,
    stages: [Duration; 4],
    bytes: u64,
    sizes: [u64; 3],
}

impl StorageTrace {
    /// The operation's span, for work it runs elsewhere.
    pub fn span(&self) -> tracing::Span {
        self.0
            .as_ref()
            .map_or_else(tracing::Span::none, |timing| timing.span.clone())
    }

    /// The time since the last mark was `stage`.
    pub fn stage(&mut self, stage: StorageStage) {
        if let Some(timing) = &mut self.0 {
            let now = Instant::now();
            timing.stages[stage.index()] += now.saturating_duration_since(timing.marked);
            timing.marked = now;
        }
    }

    /// Serialized bytes the operation wrote.
    pub fn bytes(&mut self, bytes: usize) {
        if let Some(timing) = &mut self.0 {
            timing.bytes = timing.bytes.saturating_add(bytes as u64);
        }
    }

    /// One of the operation's sizes.
    pub fn size(&mut self, kind: StorageBatch, size: u64) {
        if let Some(timing) = &mut self.0 {
            timing.sizes[kind.index()] = size;
        }
    }

    /// Whether it is on.
    pub fn on(&self) -> bool {
        self.0.is_some()
    }

    /// End it: `ok` or not.
    pub fn finish(mut self, ok: bool) {
        if let Some(timing) = self.0.take() {
            timing.report(if ok {
                StorageOutcome::Ok
            } else {
                StorageOutcome::Error
            });
        }
    }
}

impl Drop for StorageTrace {
    fn drop(&mut self) {
        if let Some(timing) = self.0.take() {
            timing.report(StorageOutcome::Incomplete);
        }
    }
}

impl StorageTiming {
    fn report(self, outcome: StorageOutcome) {
        let total = self.started.elapsed();
        let span = &self.span;
        span.record("outcome", outcome.name());
        if outcome != StorageOutcome::Ok {
            span.record("otel.status_code", "ERROR");
        }
        for (stage, field) in [
            (StorageStage::Encode, "encode_seconds"),
            (StorageStage::Queue, "queue_seconds"),
            (StorageStage::Write, "write_seconds"),
            (StorageStage::Commit, "commit_seconds"),
        ] {
            span.record(field, self.stages[stage.index()].as_secs_f64());
        }
        span.record("total_seconds", total.as_secs_f64());
        span.record("encoded_bytes", int(self.bytes));
        for (kind, field) in [
            (StorageBatch::Entries, "entries"),
            (StorageBatch::Commands, "commands"),
            (StorageBatch::ChangedRecords, "changed_records"),
        ] {
            span.record(field, int(self.sizes[kind.index()]));
        }
        let observer = &self.observer;
        let Some((instruments, bound)) = observer.bound() else {
            return;
        };
        let operation = self.operation;
        let series = bound.storage.get(
            operation.index() * StorageOutcome::ALL.len() + outcome.index(),
            || {
                let attributes = observer
                    .attributes(&[("operation", operation.name()), ("outcome", outcome.name())]);
                let with = |key: &'static str, value: &'static str| {
                    let mut attributes = attributes.clone();
                    attributes.push(KeyValue::new(key, value));
                    attributes
                };
                StorageBound {
                    operations: instruments.storage_operations.bind(&attributes),
                    duration: instruments.storage_duration.bind(&attributes),
                    bytes: instruments.storage_bytes.bind(&attributes),
                    stages: StorageStage::ALL
                        .iter()
                        .map(|stage| instruments.storage_stage.bind(&with("stage", stage.name())))
                        .collect(),
                    batch: StorageBatch::ALL
                        .iter()
                        .map(|kind| instruments.storage_batch.bind(&with("kind", kind.name())))
                        .collect(),
                }
            },
        );
        series.operations.add(1);
        series.duration.record(total.as_secs_f64());
        series.bytes.record(self.bytes);
        for stage in StorageStage::ALL {
            series.stages[stage.index()].record(self.stages[stage.index()].as_secs_f64());
        }
        for kind in StorageBatch::ALL {
            series.batch[kind.index()].record(self.sizes[kind.index()]);
        }
    }
}

/// What one guest invocation did.
#[derive(Clone, Copy, Debug, Default)]
pub struct InvocationReport {
    /// Its wall time.
    pub total: Duration,
    /// Its wall time in host calls.
    pub host: Duration,
    /// Host calls it made.
    pub host_calls: u64,
    /// Reads of stored state.
    pub reads: u64,
    /// Writes to it.
    pub writes: u64,
    /// Calls to other methods.
    pub calls: u64,
    /// Its depth in nested calls (1: the top).
    pub depth: u64,
}

thread_local! {
    /// Guest invocations running on this thread, nested ones included.
    static DEPTH: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// One guest invocation being observed: a `flower.evaluator.invocation`
/// span, entered while the guest runs, and its counts.
pub struct Invocation {
    observer: Arc<Observer>,
    mode: Mode,
    span: tracing::Span,
    started: Instant,
    report: InvocationReport,
}

impl Invocation {
    /// Start observing an invocation of `mode`, when on.
    pub fn start(observer: &Arc<Observer>, mode: Mode) -> Option<Self> {
        if !enabled() {
            return None;
        }
        let depth = DEPTH.with(|depth| {
            depth.set(depth.get() + 1);
            depth.get()
        });
        let span = tracing::info_span!(
            target: OTEL_TARGET,
            "flower.evaluator.invocation",
            backend = "wasm",
            mode = mode.name(),
            flower.group = %observer.group,
            outcome = tracing::field::Empty,
            otel.status_code = tracing::field::Empty,
            total_seconds = tracing::field::Empty,
            guest_seconds = tracing::field::Empty,
            host_seconds = tracing::field::Empty,
            host_calls = tracing::field::Empty,
            reads = tracing::field::Empty,
            writes = tracing::field::Empty,
            calls = tracing::field::Empty,
            depth = int(depth),
        );
        Some(Invocation {
            observer: observer.clone(),
            mode,
            span,
            started: Instant::now(),
            report: InvocationReport {
                depth,
                ..InvocationReport::default()
            },
        })
    }

    /// Its span, entered while the guest runs.
    pub fn span(&self) -> &tracing::Span {
        &self.span
    }

    /// Count a host call of `op` (a [`flower_sdk::abi::HostOp`] number)
    /// that took `elapsed`.
    pub fn host_call(&mut self, op: u8, elapsed: Duration) {
        use flower_sdk::abi::HostOp;
        let report = &mut self.report;
        report.host_calls += 1;
        report.host += elapsed;
        match HostOp::from_u32(u32::from(op)) {
            Some(
                HostOp::Get
                | HostOp::RawGet
                | HostOp::GetDerived
                | HostOp::Scan
                | HostOp::RawScan
                | HostOp::Range
                | HostOp::History
                | HostOp::TimerGet
                | HostOp::TimerScan
                | HostOp::QueueGet
                | HostOp::QueueStats,
            ) => report.reads += 1,
            Some(
                HostOp::Set
                | HostOp::Delete
                | HostOp::DeleteIfMatch
                | HostOp::Enqueue
                | HostOp::TimerSchedule
                | HostOp::TimerCancel
                | HostOp::TimerRetry
                | HostOp::QueueClaim
                | HostOp::QueueRenew
                | HostOp::QueueReport
                | HostOp::QueueCancel
                | HostOp::QueueRetry
                | HostOp::QueueWait
                | HostOp::QueuePurge,
            ) => report.writes += 1,
            Some(HostOp::Call) => report.calls += 1,
            _ => {}
        }
    }

    /// End it with `outcome`.
    pub fn finish(mut self, outcome: EvalOutcome) {
        self.report.total = self.started.elapsed();
        let report = self.report;
        let span = &self.span;
        span.record("outcome", outcome.name());
        if outcome == EvalOutcome::Trap {
            span.record("otel.status_code", "ERROR");
        }
        span.record("total_seconds", report.total.as_secs_f64());
        span.record(
            "guest_seconds",
            report.total.saturating_sub(report.host).as_secs_f64(),
        );
        span.record("host_seconds", report.host.as_secs_f64());
        span.record("host_calls", int(report.host_calls));
        span.record("reads", int(report.reads));
        span.record("writes", int(report.writes));
        span.record("calls", int(report.calls));
        self.observer.evaluation(self.mode, outcome, &report);
    }
}

impl Drop for Invocation {
    fn drop(&mut self) {
        DEPTH.with(|depth| depth.set(depth.get().saturating_sub(1)));
    }
}

/// A runtime factory whose runtimes report their invocations to a
/// group's instruments, and whose builds count as bundle preparations.
/// Off, a call costs one atomic load more.
pub struct ObservedFactory {
    inner: Arc<dyn crate::statemachine::RuntimeFactory>,
    observer: Arc<Observer>,
}

impl ObservedFactory {
    /// Observe `inner`'s runtimes for `observer`'s group.
    pub fn new(
        inner: Arc<dyn crate::statemachine::RuntimeFactory>,
        observer: Arc<Observer>,
    ) -> Self {
        ObservedFactory { inner, observer }
    }
}

impl crate::statemachine::RuntimeFactory for ObservedFactory {
    fn build(
        &self,
        db: &[u8],
        module: &str,
        wasm: &[u8],
        manifest: &flower_sdk::Value,
    ) -> Result<Box<dyn flower_engine::runtime::Runtime>, String> {
        let runtime = bundle(&self.observer, || {
            self.inner.build(db, module, wasm, manifest)
        })?;
        Ok(Box::new(ObservedRuntime {
            inner: runtime,
            observer: self.observer.clone(),
        }))
    }
}

/// A runtime reporting each invocation (see [`Invocation`]).
struct ObservedRuntime {
    inner: Box<dyn flower_engine::runtime::Runtime>,
    observer: Arc<Observer>,
}

/// A host handler counting and timing the host calls a guest makes.
struct Counted<'a> {
    inner: &'a mut dyn flower_engine::runtime::HostCall,
    invocation: &'a mut Invocation,
}

impl flower_engine::runtime::HostCall for Counted<'_> {
    fn op(&mut self, op: u8, input: &[u8]) -> Vec<u8> {
        let started = Instant::now();
        let output = self.inner.op(op, input);
        self.invocation.host_call(op, started.elapsed());
        output
    }

    fn read_untracked(&mut self) {
        self.inner.read_untracked();
    }

    fn read_keys(&mut self) {
        self.inner.read_keys();
    }
}

/// How an invocation's result ended.
fn eval_outcome<T>(result: &Result<T, flower_sdk::CallFailure>) -> EvalOutcome {
    match result {
        Ok(_) => EvalOutcome::Ok,
        Err(failure) if failure.recovered => EvalOutcome::Error,
        Err(_) => EvalOutcome::Trap,
    }
}

impl flower_engine::runtime::Runtime for ObservedRuntime {
    fn call(
        &self,
        kind: flower_engine::runtime::CallKind,
        module: &str,
        name: &str,
        args: &flower_sdk::Value,
        host: &mut dyn flower_engine::runtime::HostCall,
    ) -> Result<flower_sdk::Value, flower_sdk::CallFailure> {
        use flower_engine::runtime::CallKind;
        let mode = match kind {
            CallKind::Query => Mode::Query,
            CallKind::Mutation => Mode::Mutation,
            CallKind::Derived => Mode::Derived,
            CallKind::Reduce => Mode::Reduce,
        };
        let Some(mut invocation) = Invocation::start(&self.observer, mode) else {
            return self.inner.call(kind, module, name, args, host);
        };
        let span = invocation.span().clone();
        let result = span.in_scope(|| {
            let mut counted = Counted {
                inner: host,
                invocation: &mut invocation,
            };
            self.inner.call(kind, module, name, args, &mut counted)
        });
        invocation.finish(eval_outcome(&result));
        result
    }

    fn plan(
        &self,
        module: &str,
        name: &str,
        args: &flower_sdk::Value,
    ) -> Result<flower_sdk::Value, flower_sdk::CallFailure> {
        let Some(invocation) = Invocation::start(&self.observer, Mode::Transaction) else {
            return self.inner.plan(module, name, args);
        };
        let result = invocation
            .span()
            .clone()
            .in_scope(|| self.inner.plan(module, name, args));
        invocation.finish(eval_outcome(&result));
        result
    }
}

/// Time a module's runtime preparation: a `flower.evaluator.bundle.prepare`
/// span and `flower.evaluator.bundle.preparation.duration`.
pub fn bundle<T, E>(observer: &Observer, prepare: impl FnOnce() -> Result<T, E>) -> Result<T, E> {
    if !enabled() {
        return prepare();
    }
    let span = tracing::info_span!(
        target: OTEL_TARGET,
        "flower.evaluator.bundle.prepare",
        flower.group = %observer.group,
        outcome = tracing::field::Empty,
        otel.status_code = tracing::field::Empty,
    );
    let started = Instant::now();
    let result = span.in_scope(prepare);
    let ok = result.is_ok();
    span.record("outcome", if ok { "ok" } else { "error" });
    if !ok {
        span.record("otel.status_code", "ERROR");
    }
    observer.bundle(ok, started.elapsed());
    result
}

static SNAPSHOTS_STARTED: AtomicU64 = AtomicU64::new(0);
static SNAPSHOTS_RUNNING: AtomicU64 = AtomicU64::new(0);

/// Snapshot work in this process: builds and the log purges after
/// them. Always counted (atomics only), telemetry or not.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SnapshotActivity {
    /// Builds and purges begun since the process started.
    pub started: u64,
    /// Builds and purges running now.
    pub running: u64,
}

impl SnapshotActivity {
    /// Whether snapshot work ran at any time between `self` and `later`.
    pub fn overlapped(self, later: Self) -> bool {
        self.started != later.started || self.running > 0 || later.running > 0
    }
}

/// The snapshot work this process began and runs now.
pub fn snapshot_activity() -> SnapshotActivity {
    SnapshotActivity {
        started: SNAPSHOTS_STARTED.load(Ordering::Acquire),
        running: SNAPSHOTS_RUNNING.load(Ordering::Acquire),
    }
}

/// One phase of snapshot work in progress, counted in
/// [`snapshot_activity`] until dropped: a root `flower.raft.snapshot`
/// span with `snapshot.start` and `snapshot.end` events, and its
/// duration by phase and outcome.
pub struct SnapshotWork {
    observer: Arc<Observer>,
    phase: SnapshotPhase,
    started: Instant,
    span: tracing::Span,
    outcome: SnapshotOutcome,
}

impl SnapshotWork {
    /// Begin `phase`.
    pub fn begin(observer: &Arc<Observer>, phase: SnapshotPhase) -> Self {
        SNAPSHOTS_STARTED.fetch_add(1, Ordering::AcqRel);
        SNAPSHOTS_RUNNING.fetch_add(1, Ordering::AcqRel);
        let span = if enabled() {
            let span = tracing::info_span!(
                target: OTEL_TARGET,
                parent: None,
                "flower.raft.snapshot",
                phase = phase.name(),
                flower.raft.log_index = tracing::field::Empty,
                flower.group = %observer.group,
                outcome = tracing::field::Empty,
            );
            tracing::info!(target: OTEL_TARGET, parent: &span, phase = phase.name(), "snapshot.start");
            span
        } else {
            tracing::Span::none()
        };
        SnapshotWork {
            observer: observer.clone(),
            phase,
            started: Instant::now(),
            span,
            outcome: SnapshotOutcome::Abandoned,
        }
    }

    /// How it ended, recorded when it drops.
    pub fn outcome(&mut self, outcome: SnapshotOutcome) {
        self.outcome = outcome;
    }

    /// The log index it covers up to: the build's applied position, or
    /// the purge point.
    pub fn at(&self, index: u64) {
        self.span.record("flower.raft.log_index", int(index));
    }
}

impl Drop for SnapshotWork {
    fn drop(&mut self) {
        SNAPSHOTS_RUNNING.fetch_sub(1, Ordering::AcqRel);
        let elapsed = self.started.elapsed();
        if !enabled() {
            return;
        }
        self.span.record("outcome", self.outcome.name());
        tracing::info!(
            target: OTEL_TARGET,
            parent: &self.span,
            phase = self.phase.name(),
            outcome = self.outcome.name(),
            seconds = elapsed.as_secs_f64(),
            "snapshot.end"
        );
        if let Some((instruments, bound)) = self.observer.bound() {
            let (phase, outcome) = (self.phase, self.outcome);
            bound
                .snapshot
                .get(
                    phase.index() * SnapshotOutcome::ALL.len() + outcome.index(),
                    || {
                        instruments.snapshot.bind(
                            &self.observer.attributes(&[
                                ("phase", phase.name()),
                                ("outcome", outcome.name()),
                            ]),
                        )
                    },
                )
                .record(elapsed.as_secs_f64());
        }
    }
}

/// Sampled batches remembered for the watch deliveries their commits wake.
const BATCHES: usize = 64;

/// Remembered batches: `(writer, seq)` and the batch span's context,
/// oldest first.
type Batches = Mutex<std::collections::VecDeque<((u64, u64), SpanContext)>>;

fn batches() -> &'static Batches {
    static RECENT: OnceLock<Batches> = OnceLock::new();
    RECENT.get_or_init(Mutex::default)
}

/// Remember the sampled `flower.writer.batch` of writer `writer`'s
/// batch `seq`, which watch deliveries its commit wakes link to; the
/// last 64 are kept.
pub fn remember_batch(writer: u64, seq: u64, span: &tracing::Span) {
    if !records(span) {
        return;
    }
    let context = span.context().span().span_context().clone();
    let mut recent = batches().lock().unwrap_or_else(PoisonError::into_inner);
    if recent.len() == BATCHES {
        recent.pop_front();
    }
    recent.push_back(((writer, seq), context));
}

/// The remembered sampled batch span of writer `writer`'s batch `seq`.
pub fn batch_context(writer: u64, seq: u64) -> Option<SpanContext> {
    if !enabled() {
        return None;
    }
    batches()
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .iter()
        .rev()
        .find(|(batch, _)| *batch == (writer, seq))
        .map(|(_, context)| context.clone())
}

/// A count as a span attribute: OpenTelemetry attributes are signed,
/// and the bridge records unsigned values as their debug text.
pub fn int(value: impl TryInto<i64>) -> i64 {
    value.try_into().unwrap_or(i64::MAX)
}

/// The trace context of the current span, when telemetry is on and it
/// has one: what a batch links to, or a watch delivery descends from.
pub fn current_context() -> Option<SpanContext> {
    if !enabled() {
        return None;
    }
    let context = tracing::Span::current().context();
    let span = context.span();
    let context = span.span_context();
    context.is_valid().then(|| context.clone())
}

/// Whether `span` records: sampled, so its attributes are worth
/// building.
pub fn records(span: &tracing::Span) -> bool {
    !span.is_disabled() && span.context().span().span_context().is_sampled()
}

/// Make `span` (created with no parent) a child of `parent`, a context
/// kept from a span that may have ended.
pub fn adopt(span: &tracing::Span, parent: &SpanContext) {
    let parent = opentelemetry::Context::new().with_remote_span_context(parent.clone());
    let _ = span.set_parent(parent);
}

/// Link `span` to `context`.
pub fn link(span: &tracing::Span, context: &SpanContext) {
    span.add_link(context.clone());
}

/// Add event `name` to `span` at `at`.
pub fn event_at(span: &tracing::Span, name: &'static str, at: Instant) {
    let at = SystemTime::now()
        .checked_sub(at.elapsed())
        .unwrap_or(SystemTime::UNIX_EPOCH);
    span.add_event_with_timestamp(name, at, Vec::new());
}

/// Marks kept per kind: enough for every commit in flight.
const MARKS: usize = 256;

/// One mark: the last log index it covers, and when.
#[derive(Clone, Copy)]
struct Mark {
    last: u64,
    at: Instant,
}

/// A ring of marks of one kind, in log order.
struct Ring {
    marks: [Option<Mark>; MARKS],
    next: usize,
    /// When the newest mark this ring overwrote was taken.
    evicted: Option<Instant>,
}

impl Ring {
    fn new() -> Self {
        Ring {
            marks: [None; MARKS],
            next: 0,
            evicted: None,
        }
    }

    fn push(&mut self, mark: Mark) {
        if let Some(old) = self.marks[self.next].replace(mark) {
            self.evicted = Some(old.at);
        }
        self.next = (self.next + 1) % MARKS;
    }

    /// When an entry at `index`, submitted at `submitted`, was first
    /// covered; `None` when not yet, or when its mark may be gone.
    fn first(&self, index: u64, submitted: Instant) -> Option<Instant> {
        if self.evicted.is_some_and(|evicted| evicted >= submitted) {
            return None;
        }
        self.marks
            .iter()
            .flatten()
            .filter(|mark| mark.last >= index && mark.at >= submitted)
            .map(|mark| mark.at)
            .min()
    }
}

/// Where a commit's time went, from its submission to Raft.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CommitPhases {
    /// Until the entry was durable in the leader's own log.
    pub append: Option<Duration>,
    /// Until a quorum committed it (when its application began). It
    /// overlaps `append`: followers replicate while the leader appends.
    pub replicate: Option<Duration>,
    /// Its application, once committed.
    pub apply: Option<Duration>,
}

/// When a member's log entries reached each step: durable in its own
/// log, committed (its application began), applied. Always on, one
/// uncontended lock per Raft append or apply call (not per write, and
/// beside an fsync), so `Server-Timing` can split a commit.
pub struct CommitMarks {
    appended: Mutex<Ring>,
    applying: Mutex<Ring>,
    applied: Mutex<Ring>,
}

impl Default for CommitMarks {
    fn default() -> Self {
        CommitMarks {
            appended: Mutex::new(Ring::new()),
            applying: Mutex::new(Ring::new()),
            applied: Mutex::new(Ring::new()),
        }
    }
}

impl std::fmt::Debug for CommitMarks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommitMarks").finish_non_exhaustive()
    }
}

impl CommitMarks {
    fn mark(ring: &Mutex<Ring>, last: u64) {
        let at = Instant::now();
        ring.lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(Mark { last, at });
    }

    /// The log is durable up to `last`.
    pub fn appended(&self, last: u64) {
        Self::mark(&self.appended, last);
    }

    /// Application of committed entries up to `last` begins.
    pub fn applying(&self, last: u64) {
        Self::mark(&self.applying, last);
    }

    /// Entries up to `last` are applied.
    pub fn applied(&self, last: u64) {
        Self::mark(&self.applied, last);
    }

    /// When the entry at `index`, submitted at `submitted`, reached each
    /// step: appended, applying, applied.
    pub fn instants(&self, index: u64, submitted: Instant) -> [Option<Instant>; 3] {
        let first = |ring: &Mutex<Ring>| {
            ring.lock()
                .unwrap_or_else(PoisonError::into_inner)
                .first(index, submitted)
        };
        [
            first(&self.appended),
            first(&self.applying),
            first(&self.applied),
        ]
    }

    /// How the commit of the entry at `index`, submitted at
    /// `submitted`, divides.
    pub fn phases(&self, index: u64, submitted: Instant) -> CommitPhases {
        let [appended, applying, applied] = self.instants(index, submitted);
        CommitPhases {
            append: appended.map(|at| at.saturating_duration_since(submitted)),
            replicate: applying.map(|at| at.saturating_duration_since(submitted)),
            apply: applying
                .zip(applied)
                .map(|(start, end)| end.saturating_duration_since(start)),
        }
    }
}

/// The calling thread's CPU time so far, where the platform tells it.
pub fn thread_cpu() -> Option<Duration> {
    #[cfg(unix)]
    {
        let mut time = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        // SAFETY: clock_gettime writes one timespec through a valid
        // pointer.
        let status = unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut time) };
        (status == 0).then(|| {
            Duration::new(
                u64::try_from(time.tv_sec).unwrap_or(0),
                u32::try_from(time.tv_nsec).unwrap_or(0),
            )
        })
    }
    #[cfg(windows)]
    {
        use windows_sys::Win32::Foundation::FILETIME;
        use windows_sys::Win32::System::Threading::{GetCurrentThread, GetThreadTimes};
        let zero = || FILETIME {
            dwLowDateTime: 0,
            dwHighDateTime: 0,
        };
        let (mut created, mut exited, mut kernel, mut user) = (zero(), zero(), zero(), zero());
        // SAFETY: the pseudo-handle of the current thread is always
        // valid, and each pointer names a FILETIME on this stack.
        let ok = unsafe {
            GetThreadTimes(
                GetCurrentThread(),
                &mut created,
                &mut exited,
                &mut kernel,
                &mut user,
            )
        };
        let ticks =
            |time: FILETIME| (u64::from(time.dwHighDateTime) << 32) | u64::from(time.dwLowDateTime);
        // FILETIME counts 100 ns ticks.
        (ok != 0).then(|| Duration::from_nanos((ticks(kernel) + ticks(user)) * 100))
    }
    #[cfg(not(any(unix, windows)))]
    {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_commit_divides_into_append_quorum_and_apply_from_its_marks() {
        let marks = CommitMarks::default();
        marks.applied(1);
        let submitted = Instant::now();
        marks.applied(4); // an earlier entry
        std::thread::sleep(Duration::from_millis(2));
        marks.appended(7);
        std::thread::sleep(Duration::from_millis(2));
        marks.applying(7);
        std::thread::sleep(Duration::from_millis(2));
        marks.applied(7);
        let phases = marks.phases(6, submitted);
        let (append, replicate, apply) = (
            phases.append.unwrap(),
            phases.replicate.unwrap(),
            phases.apply.unwrap(),
        );
        assert!(append >= Duration::from_millis(2), "{phases:?}");
        assert!(replicate >= append + Duration::from_millis(2), "{phases:?}");
        assert!(apply >= Duration::from_millis(2), "{phases:?}");
        // An entry not yet marked says nothing.
        assert_eq!(marks.phases(8, submitted), CommitPhases::default());
    }

    #[test]
    fn marks_lost_to_eviction_give_no_phases_rather_than_a_later_entrys() {
        let marks = CommitMarks::default();
        let submitted = Instant::now();
        marks.appended(1);
        marks.applying(1);
        marks.applied(1);
        // A ring's worth of later marks pushes entry 1's out.
        for index in 2..=MARKS as u64 + 1 {
            marks.appended(index);
        }
        // Entry 1's append mark was overwritten after its submission.
        assert_eq!(marks.phases(1, submitted).append, None);
        // Its other marks are still there.
        assert!(marks.phases(1, submitted).apply.is_some());
    }

    #[test]
    fn snapshot_work_counts_until_it_ends_and_overlap_sees_work_that_came_and_went() {
        let observer = observer("telemetry-unit");
        let before = snapshot_activity();
        let mut work = SnapshotWork::begin(&observer, SnapshotPhase::Build);
        let during = snapshot_activity();
        assert!(during.started > before.started);
        assert!(during.running >= 1);
        work.outcome(SnapshotOutcome::Published);
        drop(work);
        assert!(before.overlapped(snapshot_activity()));
        let idle = SnapshotActivity {
            started: 7,
            running: 0,
        };
        assert!(!idle.overlapped(idle));
    }

    #[test]
    fn classes_index_their_names() {
        for (index, stage) in WriterStage::ALL.iter().enumerate() {
            assert_eq!(stage.index(), index);
        }
        assert_eq!(Rpc::ReadFence.name(), "read-fence");
        assert_eq!(StorageOperation::ALL.len(), 7);
    }

    #[test]
    fn the_threads_cpu_time_moves_with_work() {
        let Some(before) = thread_cpu() else {
            return;
        };
        // Work until the thread's CPU time moves: a loaded machine may
        // give a spinning thread less than one clock tick of CPU (about
        // 15.6 ms on Windows) in any fixed wall time.
        let mut spin = 0u64;
        let started = Instant::now();
        let mut after = before;
        while after <= before && started.elapsed() < Duration::from_secs(10) {
            for _ in 0..1_000_000 {
                spin = spin.wrapping_add(1);
            }
            std::hint::black_box(spin);
            after = thread_cpu().unwrap();
        }
        assert!(after > before, "{before:?} {after:?}");
    }
}
