//! The leader's writer: one queue for every client write of the group,
//! group commit, and one log entry per group.
//!
//! Mutations, transactions, queue operations and timer work all enter
//! one queue. The writer prepares them in queue order, each on the state
//! the writes before it staged: committed state, plus the batch still
//! committing, plus the writes already prepared into the group being
//! built, all in one uncommitted redb transaction that is thrown away
//! afterwards ([`Engine::on_staged`]). So no write ever prepares on
//! state another write is about to change, and none is lost: a
//! read-modify-write sees every write queued before it.
//!
//! A group becomes one [`ClusterWrite::Batch`] entry: one append, one
//! replication round and one apply transaction for all of its writes,
//! while each keeps its own patch, receipt, revision and answer. One
//! batch commits at a time. While it does, the writer keeps preparing
//! arrivals into the next group on top of it, until that batch resolves,
//! the group holds [`GROUP_ITEMS`] writes or [`GROUP_BYTES`] bytes, or it
//! was rebuilt [`GROUP_CHUNKS`] times; then it proposes the next group at
//! once. Batches therefore grow with the arrival rate times the commit
//! time: one write per entry when idle, hundreds under load, with no
//! timer to tune.
//!
//! A batch applies only directly after the data entry it was prepared
//! after (its head, see [`flower_engine::batch`]). A deployment, an
//! incarnation, or a new leader's entries landing in between make it
//! stale on every member alike; the writer prepares its writes again,
//! up to [`WRITE_ATTEMPTS`] times. A batch prepared on top of one that
//! went stale, or failed, goes the same way. Every answer a group's
//! preparation decided waits for the group's batch, since it may have
//! read writes that had not committed yet.
//!
//! With more than one preparation worker ([`SpeculationSettings`]), a
//! chunk's mutations prepare in *waves*: the writer logs every row its
//! staged transaction writes, hands other threads committed state seen
//! through an overlay of those rows ([`Engine::speculation`]), and
//! prepares a wave's candidates there at once, each recording what it
//! read. Then it takes the wave in queue order: a candidate no write
//! staged since the wave's snapshot touches commits as prepared, its
//! patch moved to the revision it lands on ([`rebase`]); any other is
//! prepared again in its turn on the staged state, exactly as without
//! speculation (see [`flower_engine::speculation`]). A wave's mutations
//! run at one clock. How wide a wave is, and which calls prediction
//! places in their turn instead, is [`crate::speculation`]'s.
//!
//! A write holds its admission place in the queue until answered, and
//! takes its turn before each preparation: admission refuses it there,
//! unprepared, when its client deadline passed or the writers' CoDel
//! sheds it ([`Permit::turn`]). The writer times each write (its wait
//! for its turn, its preparations, the rest until answered, and how its
//! batch's commit divided into append, replication and apply, from the
//! member's [`CommitMarks`]) and hands the times back for its request's
//! `Server-Timing`.
//!
//! With telemetry on, each write is a `flower.writer.submit` span under its
//! request, and each batch a root `flower.writer.batch` span linking them,
//! with `flower.writer.prepare`, `flower.writer.commit` and
//! `flower.writer.replicate` inside; the writer's metric families are
//! recorded (see [`crate::telemetry`]).

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use flower_engine::batch::{BatchItem, Head};
use flower_engine::engine::Engine;
use flower_engine::error::IO_FAILURE;
use flower_engine::speculation::{
    ConflictClass, ConflictRow, MutationCertificate, PrepareMutation, Speculation, Speculator,
    Verdict, rebase,
};
use flower_engine::storage::{Overlay, StagedWrite, Write};
use openraft::BasicNode;
use openraft::error::{ClientWriteError, RaftError};
use openraft::raft::{ClientWriteResponse, Raft};
use opentelemetry::trace::SpanContext;
use tokio::sync::{Mutex as AsyncMutex, mpsc, oneshot};
use tracing::Instrument;

use crate::admission::{Permit, WRITER_QUEUE};
use crate::exec::{now_ms, with_engine};
use crate::health::{GROUP, STORAGE_FAILED, StorageHealth};
use crate::methods;
use crate::node::NodeError;
use crate::node::leadership::{AdmittedOwned, Leadership, cut_off, isolated, isolation_window};
use crate::speculation::{self, Policy, Pool, Speculable, SpeculationSettings, SpeculationStats};
use crate::stages::{self, Stage};
use crate::statemachine::{StateMachine, is_stale_batch};
use crate::telemetry::{
    self, BatchOutcome, BatchReport, Class, CommitMarks, CommitPhases, OTEL_TARGET, Observer,
    Refusal, SnapshotActivity, StopReason, WriteOutcome, WriterStage,
};
use crate::types::{ClusterWrite, NodeId, TypeConfig, WriteResponse};

/// Most writes one batch entry carries.
pub const GROUP_ITEMS: usize = 256;
/// Encoded bytes past which a group takes no more writes (one larger
/// write still goes alone).
pub const GROUP_BYTES: usize = 4 * 1024 * 1024;
/// Most times one group is rebuilt to take arrivals while its
/// predecessor commits: each rebuild stages the predecessor and the
/// group so far again.
pub const GROUP_CHUNKS: usize = 8;
/// Preparations one write gets before its batch keeps going stale is
/// answered as an overload.
pub const WRITE_ATTEMPTS: u32 = 8;
/// Positions staged writes take in the throwaway transaction: far past
/// any real one, increasing per item so each advances its database.
const STAGED_POSITION: u64 = 1 << 62;

/// What one write's preparation decided.
pub(crate) enum Prepared<T> {
    /// Propose this item; `T` is answered with its committed response.
    Propose(BatchItem, T),
    /// Decided without proposing anything.
    Answer(T),
}

/// A write's answer: what its preparation decided, plus its item's
/// committed response when it proposed one.
pub(crate) struct Written<T> {
    /// What the preparation decided.
    pub note: T,
    /// The committed item's response, if it proposed one.
    pub response: Option<WriteResponse>,
    /// Its wait in the writer queue before its first turn.
    pub queued: Duration,
}

/// Where one write spent its time in the writer, for the request's
/// `Server-Timing` (see [`crate::stages`]).
#[derive(Clone, Copy, Debug, Default)]
struct Timing {
    /// From submission to its first turn.
    queued: Duration,
    /// Its preparations, every attempt's.
    prepared: Duration,
    /// The rest, until answered: waiting for its group's batch to go,
    /// the batch's commit, and any preparation again.
    committed: Duration,
    /// How its batch's commit divided, when it committed one.
    phases: CommitPhases,
}

/// Where a write prepares: on the writer's engine, which has staged the
/// writes before it, or speculatively on a snapshot of that state.
pub(crate) enum Lane<'a> {
    /// The writer's engine, on the staged state.
    Writer(&'a mut Engine),
    /// A speculative preparer (see [`flower_engine::speculation`]).
    Speculative(&'a mut Speculator),
}

/// What a write's speculative preparation left: what it decided, the
/// item it proposes, and its certificate.
struct Speculated<T> {
    outcome: Result<(Option<BatchItem>, T), NodeError>,
    certificate: MutationCertificate,
}

/// A type-erased queued write.
trait Job: Send {
    /// The write's turn to prepare, `first` or after its batch went
    /// stale: refused unprepared when admission says so (its client
    /// deadline passed, or CoDel sheds it; see [`Permit::turn`]).
    fn turn(&mut self, first: bool) -> Result<(), NodeError>;

    /// Prepare on the engine's current state, which the writer has
    /// staged: the item to propose, if any. What it decided stays
    /// inside until [`Job::finish`].
    fn prepare(&mut self, engine: &mut Engine) -> Option<BatchItem>;

    /// The mutation it is, when it may prepare speculatively.
    fn speculable(&self) -> Option<&Speculable>;

    /// Prepare speculatively on `speculation`, keeping what it decided
    /// and its certificate until [`Job::adopt`] or [`Job::discard`].
    fn speculate(&mut self, speculation: &Speculation);

    /// The certificate of a speculative preparation that succeeded.
    fn certificate(&self) -> Option<&MutationCertificate>;

    /// Take a speculative preparation as the write's preparation: the
    /// item it proposes, if any. Only after its certificate held.
    fn adopt(&mut self) -> Option<BatchItem>;

    /// Drop a speculative preparation.
    fn discard(&mut self);

    /// Whether the last preparation failed on a disk error.
    fn disk_failure(&self) -> Option<String>;

    /// Its `flower.writer.submit` span's context, which its batch links
    /// to, when telemetry is on.
    fn link(&self) -> Option<&SpanContext>;

    /// Answer the caller once the write's group resolved: the item's
    /// committed response, none when it proposed none, or the error
    /// that failed the group; with how the batch's commit divided.
    fn finish(
        self: Box<Self>,
        outcome: Result<Option<WriteResponse>, NodeError>,
        phases: CommitPhases,
    );
}

/// The answer a submitter waits for, with the write's timings.
type Reply<T> = (Result<Written<T>, NodeError>, Timing);

/// One queued write of a concrete answer type.
struct Typed<T, F> {
    prepare: F,
    /// The mutation it is, when it may prepare speculatively.
    speculable: Option<Speculable>,
    /// Its speculative preparation, until adopted or discarded.
    speculated: Option<Speculated<T>>,
    decided: Option<Result<T, NodeError>>,
    reply: oneshot::Sender<Reply<T>>,
    /// The write's admission place, held until it is answered.
    admitted: Option<Permit>,
    submitted: Instant,
    first_turn: Option<Instant>,
    preparing: Duration,
    /// Its submit span's context, when telemetry is on.
    link: Option<SpanContext>,
}

impl<T, F> Job for Typed<T, F>
where
    T: Send,
    F: FnMut(Lane<'_>) -> Result<Prepared<T>, NodeError> + Send,
{
    fn turn(&mut self, first: bool) -> Result<(), NodeError> {
        self.first_turn.get_or_insert_with(Instant::now);
        match &self.admitted {
            Some(permit) => permit.turn(first).map_err(NodeError::from),
            None => Ok(()),
        }
    }

    fn prepare(&mut self, engine: &mut Engine) -> Option<BatchItem> {
        let started = Instant::now();
        let (decided, item) = match (self.prepare)(Lane::Writer(engine)) {
            Ok(Prepared::Propose(item, note)) => (Ok(note), Some(item)),
            Ok(Prepared::Answer(note)) => (Ok(note), None),
            Err(error) => (Err(error), None),
        };
        self.preparing += started.elapsed();
        self.decided = Some(decided);
        item
    }

    fn speculable(&self) -> Option<&Speculable> {
        self.speculable.as_ref()
    }

    fn speculate(&mut self, speculation: &Speculation) {
        let started = Instant::now();
        let mut speculator = speculation.speculator();
        let outcome =
            (self.prepare)(Lane::Speculative(&mut speculator)).map(|prepared| match prepared {
                Prepared::Propose(item, note) => (Some(item), note),
                Prepared::Answer(note) => (None, note),
            });
        self.preparing += started.elapsed();
        self.speculated = Some(Speculated {
            outcome,
            certificate: speculator.certificate(),
        });
    }

    fn certificate(&self) -> Option<&MutationCertificate> {
        self.speculated
            .as_ref()
            .filter(|speculated| speculated.outcome.is_ok())
            .map(|speculated| &speculated.certificate)
    }

    fn adopt(&mut self) -> Option<BatchItem> {
        match self.speculated.take().map(|speculated| speculated.outcome) {
            Some(Ok((item, note))) => {
                self.decided = Some(Ok(note));
                item
            }
            Some(Err(error)) => {
                self.decided = Some(Err(error));
                None
            }
            None => None,
        }
    }

    fn discard(&mut self) {
        self.speculated = None;
    }

    fn disk_failure(&self) -> Option<String> {
        match &self.decided {
            Some(Err(NodeError::Failure { code, message })) if code == IO_FAILURE => {
                Some(message.clone())
            }
            _ => None,
        }
    }

    fn link(&self) -> Option<&SpanContext> {
        self.link.as_ref()
    }

    fn finish(
        self: Box<Self>,
        outcome: Result<Option<WriteResponse>, NodeError>,
        phases: CommitPhases,
    ) {
        let Typed {
            decided,
            reply,
            admitted,
            submitted,
            first_turn,
            preparing,
            ..
        } = *self;
        // The place goes before the answer, so a caller that sees it
        // sees the place free.
        drop(admitted);
        let total = submitted.elapsed();
        let queued = first_turn.map_or(total, |turn| turn.saturating_duration_since(submitted));
        let timing = Timing {
            queued,
            prepared: preparing,
            committed: total.saturating_sub(queued).saturating_sub(preparing),
            phases,
        };
        let answer = match (outcome, decided) {
            (Err(error), _) => Err(error),
            (Ok(_), None) => Err(NodeError::Fatal("a write resolved unprepared".to_string())),
            (Ok(_), Some(Err(error))) => Err(error),
            (Ok(response), Some(Ok(note))) => Ok(Written {
                note,
                response,
                queued,
            }),
        };
        let _ = reply.send((answer, timing));
    }
}

/// A write in the writer's hands: its job, how often it was prepared,
/// and which item of its group it proposed.
struct Queued {
    job: Box<dyn Job>,
    attempts: u32,
    item: Option<usize>,
}

/// A group being prepared, or committing as one batch.
struct Group {
    seq: u64,
    /// The head the batch follows: the predecessor's identity when
    /// prepared on top of it, else the head the first chunk read.
    follows: Head,
    proposed_ms: u64,
    items: Vec<BatchItem>,
    bytes: usize,
    jobs: Vec<Queued>,
    chunks: usize,
    /// Prepared on top of the batch committing when it began.
    on_predecessor: bool,
    /// The state moved under it: requeue once its predecessor resolves.
    doomed: bool,
    /// A write did not fit the entry's bound: it waits for the next.
    full: bool,
    /// Its `flower.writer.batch` span, a root linking its writes' spans
    /// (none when telemetry is off).
    trace: tracing::Span,
    /// Its chunks' preparation time.
    preparing: Duration,
    /// When its first chunk began.
    began: Instant,
    /// Snapshot work as it began, to tell whether some overlapped it.
    snapshots: SnapshotActivity,
}

impl Group {
    fn has_room(&self) -> bool {
        !self.doomed
            && !self.full
            && self.items.len() < GROUP_ITEMS
            && self.bytes < GROUP_BYTES
            && self.chunks < GROUP_CHUNKS
    }

    /// Why it stopped growing: what left it no room, or its
    /// predecessor resolving, or nothing committing at all.
    fn stop_reason(&self) -> StopReason {
        if !self.on_predecessor {
            StopReason::Idle
        } else if self.items.len() >= GROUP_ITEMS {
            StopReason::Items
        } else if self.bytes >= GROUP_BYTES || self.full {
            StopReason::Bytes
        } else if self.chunks >= GROUP_CHUNKS {
            StopReason::Chunks
        } else {
            StopReason::Resolved
        }
    }
}

/// The batch committing: its group and the response on its way.
struct InFlight {
    group: Group,
    receiver: oneshot::Receiver<Result<ClientWriteResponse<TypeConfig>, Error>>,
    /// Holds a leadership transfer back until the batch resolved.
    _admitted: AdmittedOwned,
    /// When it went to Raft.
    submitted: Instant,
    /// Why it stopped growing, writes queued behind it and the
    /// replication lag, when it went.
    stop: StopReason,
    queued: usize,
    lag: (u64, u64),
    /// Its `flower.writer.commit` span and the `flower.writer.replicate`
    /// span inside it.
    commit: tracing::Span,
    replicate: tracing::Span,
}

type Error = ClientWriteError<NodeId, BasicNode>;

/// What a predecessor's staging needs: its writes and where it goes.
struct Predecessor {
    items: Vec<BatchItem>,
    follows: Head,
    batch: Head,
    proposed_ms: u64,
}

/// What the writer did since its member opened, for metrics.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct WriterStats {
    /// Batch entries committed.
    pub batches: u64,
    /// Writes those batches carried.
    pub writes: u64,
    /// Writes answered without proposing anything (replays, refusals,
    /// failures decided at preparation).
    pub decided: u64,
    /// Batches that went stale and were prepared again.
    pub stale: u64,
    /// The most writes one batch carried.
    pub largest: u64,
    /// What speculative preparation did (see [`crate::speculation`]).
    pub speculation: SpeculationStats,
}

/// The writer's counters, shared with its member.
#[derive(Default)]
struct Counters {
    batches: AtomicU64,
    writes: AtomicU64,
    decided: AtomicU64,
    stale: AtomicU64,
    largest: AtomicU64,
    speculation: speculation::Counters,
}

impl Counters {
    fn stats(&self) -> WriterStats {
        WriterStats {
            batches: self.batches.load(Ordering::Relaxed),
            writes: self.writes.load(Ordering::Relaxed),
            decided: self.decided.load(Ordering::Relaxed),
            stale: self.stale.load(Ordering::Relaxed),
            largest: self.largest.load(Ordering::Relaxed),
            speculation: self.speculation.stats(),
        }
    }
}

/// The queue's sending side, which every client write path submits to.
pub(crate) struct WriterHandle {
    queue: mpsc::UnboundedSender<Box<dyn Job>>,
    counters: Arc<Counters>,
    /// How the writer speculates; read before every chunk.
    settings: Arc<std::sync::Mutex<SpeculationSettings>>,
    /// Watched for a leader cut off from its quorum (see [`cut_off`]).
    raft: Raft<TypeConfig>,
    /// The group's instruments.
    observer: Arc<Observer>,
}

/// What a member's writer reports to: when its entries reached each
/// step of their commit, and its group's instruments.
pub(crate) struct Reporting {
    /// The member's commit marks (see [`CommitMarks`]).
    pub marks: Arc<CommitMarks>,
    /// Its group's instruments.
    pub observer: Arc<Observer>,
}

impl WriterHandle {
    /// Start a member's writer.
    pub(crate) fn spawn(
        engine: Arc<AsyncMutex<Engine>>,
        raft: Raft<TypeConfig>,
        leadership: Arc<Leadership>,
        health: Arc<StorageHealth>,
        reporting: Reporting,
    ) -> (Self, tokio::task::JoinHandle<()>) {
        let (queue, receiver) = mpsc::unbounded_channel();
        let counters = Arc::new(Counters::default());
        let observer = reporting.observer.clone();
        let settings = Arc::new(std::sync::Mutex::new(SpeculationSettings::default()));
        let writer = Writer {
            engine,
            raft: raft.clone(),
            leadership,
            health,
            id: rand::random_range(1..=u64::MAX),
            next_seq: 0,
            counters: counters.clone(),
            marks: reporting.marks,
            observer: reporting.observer,
            speculating: Arc::new(std::sync::Mutex::new(Speculating {
                settings: settings.clone(),
                applied: None,
                policy: Policy::new(SpeculationSettings::default(), WRITER_QUEUE),
                pool: None,
                last_clock: 0,
            })),
        };
        let task = tokio::spawn(writer.run(receiver));
        (
            WriterHandle {
                queue,
                counters,
                settings,
                raft,
                observer,
            },
            task,
        )
    }

    /// Change how the writer speculates, from its next chunk on.
    pub(crate) fn set_speculation(&self, settings: SpeculationSettings) {
        *self
            .settings
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = settings;
    }

    /// How the writer speculates.
    pub(crate) fn speculation(&self) -> SpeculationSettings {
        *self
            .settings
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Resolves once this member leads cut off from its quorum.
    async fn cut_off(&self) {
        let window = isolation_window(self.raft.config());
        let mut metrics = self.raft.metrics();
        loop {
            if cut_off(&metrics.borrow_and_update(), window) {
                return;
            }
            if metrics.changed().await.is_err() {
                return std::future::pending().await;
            }
        }
    }

    /// What the writer did so far.
    pub(crate) fn stats(&self) -> WriterStats {
        self.counters.stats()
    }

    /// Queue one write and wait for its answer. `prepare` runs on the
    /// writer's staged state, possibly more than once. `admitted` is
    /// the write's writer place, held until it is answered; its turn
    /// may refuse it ([`Permit::turn`]). The write's time in the writer
    /// counts as its request's `queue`, `eval` and `commit` stages.
    ///
    /// A leader cut off from its quorum (see [`isolation_window`]) takes no
    /// write, and stops waiting for one it took ([`crate::node::ISOLATED`], outcome
    /// unknown): the others may have elected another leader, and the
    /// caller retries there with the same idempotency key, which a
    /// receipt answers should this one commit after all.
    pub(crate) async fn submit<T, F>(
        &self,
        admitted: Option<Permit>,
        mut prepare: F,
    ) -> Result<Written<T>, NodeError>
    where
        T: Send + 'static,
        F: FnMut(&mut Engine) -> Result<Prepared<T>, NodeError> + Send + 'static,
    {
        self.queue_write(admitted, None, move |lane: Lane<'_>| match lane {
            Lane::Writer(engine) => prepare(engine),
            Lane::Speculative(_) => Err(NodeError::Fatal(
                "a write that does not speculate was speculated".to_string(),
            )),
        })
        .await
    }

    /// [`WriterHandle::submit`] for a mutation, which may prepare
    /// speculatively (see [`crate::speculation`]): `prepare` runs on the
    /// writer's engine or on a speculative preparer alike, and `call`
    /// names it for conflict prediction.
    pub(crate) async fn submit_mutation<T, F>(
        &self,
        admitted: Option<Permit>,
        call: Speculable,
        mut prepare: F,
    ) -> Result<Written<T>, NodeError>
    where
        T: Send + 'static,
        F: FnMut(&mut dyn PrepareMutation) -> Result<Prepared<T>, NodeError> + Send + 'static,
    {
        self.queue_write(admitted, Some(call), move |lane: Lane<'_>| match lane {
            Lane::Writer(engine) => prepare(engine),
            Lane::Speculative(speculator) => prepare(speculator),
        })
        .await
    }

    /// Queue one write and wait for its answer (see
    /// [`WriterHandle::submit`]).
    async fn queue_write<T, F>(
        &self,
        admitted: Option<Permit>,
        speculable: Option<Speculable>,
        prepare: F,
    ) -> Result<Written<T>, NodeError>
    where
        T: Send + 'static,
        F: FnMut(Lane<'_>) -> Result<Prepared<T>, NodeError> + Send + 'static,
    {
        if cut_off(
            &self.raft.metrics().borrow(),
            isolation_window(self.raft.config()),
        ) {
            return Err(isolated());
        }
        let mut submission = Submission::start(&self.observer);
        let (reply, answer) = oneshot::channel();
        let job: Box<dyn Job> = Box::new(Typed {
            prepare,
            speculable,
            speculated: None,
            decided: None,
            reply,
            admitted,
            submitted: Instant::now(),
            first_turn: None,
            preparing: Duration::ZERO,
            link: submission
                .as_ref()
                .and_then(|submission| submission.link.clone()),
        });
        self.queue
            .send(job)
            .map_err(|_| NodeError::Fatal("the writer stopped".to_string()))?;
        let span = submission
            .as_ref()
            .map_or_else(tracing::Span::none, |submission| submission.span.clone());
        let answered = async {
            tokio::select! {
                answered = answer => Some(answered),
                () = self.cut_off() => None,
            }
        }
        .instrument(span)
        .await;
        let Some(answered) = answered else {
            if let Some(submission) = submission.as_mut() {
                submission.outcome = WriteOutcome::Error;
            }
            return Err(isolated());
        };
        let (answer, timing) = match answered {
            Ok(answered) => answered,
            Err(_) => {
                if let Some(submission) = submission.as_mut() {
                    submission.outcome = WriteOutcome::Error;
                }
                return Err(NodeError::Fatal("the writer dropped a write".to_string()));
            }
        };
        stages::add(Stage::Queue, timing.queued);
        stages::add(Stage::Eval, timing.prepared);
        stages::add(Stage::Commit, timing.committed);
        stages::add_commit(&timing.phases);
        if let Some(submission) = submission.as_mut() {
            submission.outcome = match &answer {
                Ok(_) => WriteOutcome::Ok,
                Err(NodeError::Overloaded { .. }) if timing.prepared.is_zero() => {
                    WriteOutcome::Refused
                }
                Err(_) => WriteOutcome::Error,
            };
            let observer = &self.observer;
            observer.writer_stage(WriterStage::Queue, timing.queued);
            observer.writer_stage(WriterStage::Prepare, timing.prepared);
            observer.writer_stage(WriterStage::Commit, timing.committed);
        }
        answer
    }
}

/// One write's submission, observed: its `flower.writer.submit` span
/// and, when it ends, `flower.writer.request.duration` and
/// `flower.writer.requests` by outcome (`cancelled` when its caller
/// stopped waiting first).
struct Submission {
    observer: Arc<Observer>,
    span: tracing::Span,
    link: Option<SpanContext>,
    started: Instant,
    outcome: WriteOutcome,
}

impl Submission {
    /// Observe a submission, when telemetry is on.
    fn start(observer: &Arc<Observer>) -> Option<Self> {
        if !telemetry::enabled() {
            return None;
        }
        let span = tracing::info_span!(
            target: OTEL_TARGET,
            "flower.writer.submit",
            flower.group = observer.group(),
            outcome = tracing::field::Empty,
            otel.status_code = tracing::field::Empty,
        );
        let link = span.in_scope(telemetry::current_context);
        Some(Submission {
            observer: observer.clone(),
            span,
            link,
            started: Instant::now(),
            outcome: WriteOutcome::Cancelled,
        })
    }
}

impl Drop for Submission {
    fn drop(&mut self) {
        self.span.record("outcome", self.outcome.name());
        if self.outcome != WriteOutcome::Ok {
            self.span.record("otel.status_code", "ERROR");
        }
        self.observer
            .writer_request(self.outcome, self.started.elapsed());
    }
}

/// The writer task's state.
struct Writer {
    engine: Arc<AsyncMutex<Engine>>,
    raft: Raft<TypeConfig>,
    leadership: Arc<Leadership>,
    health: Arc<StorageHealth>,
    /// This writer's identity in batch heads: random, so no other
    /// writer's batch is ever taken for one of its own.
    id: u64,
    next_seq: u64,
    counters: Arc<Counters>,
    /// The member's commit marks, which split each batch's commit.
    marks: Arc<CommitMarks>,
    /// The group's instruments.
    observer: Arc<Observer>,
    /// Speculation's policy and threads, which each chunk's preparation
    /// takes on the blocking pool.
    speculating: Arc<std::sync::Mutex<Speculating>>,
}

/// The writer's speculation state between chunks.
struct Speculating {
    /// The settings its member last set.
    settings: Arc<std::sync::Mutex<SpeculationSettings>>,
    /// The settings the policy and pool were built for.
    applied: Option<SpeculationSettings>,
    policy: Policy,
    pool: Option<Pool>,
    /// The last wave's clock: clocks never go back.
    last_clock: u64,
}

impl Speculating {
    /// Bring the policy and pool up to the member's settings.
    fn refresh(&mut self) -> SpeculationSettings {
        let current = *self
            .settings
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.applied != Some(current) {
            self.policy = Policy::new(current, WRITER_QUEUE);
            self.pool = current.speculates().then(|| Pool::new(current.workers - 1));
            self.applied = Some(current);
        }
        current
    }

    /// A new wave's clock.
    fn clock(&mut self) -> u64 {
        self.last_clock = now_ms().max(self.last_clock);
        self.last_clock
    }
}

impl Writer {
    async fn run(mut self, mut receiver: mpsc::UnboundedReceiver<Box<dyn Job>>) {
        let mut pending: VecDeque<Queued> = VecDeque::new();
        let mut group: Option<Group> = None;
        let mut in_flight: Option<InFlight> = None;
        let mut closed = false;
        loop {
            while let Ok(job) = receiver.try_recv() {
                pending.push_back(Queued::new(job));
            }
            // A resolved batch goes first: its successor may go now. A
            // dropped one too: polling its receiver again would panic.
            let resolved = match in_flight.as_mut().map(|flight| flight.receiver.try_recv()) {
                Some(Ok(resolved)) => Some(Ok(resolved)),
                Some(Err(oneshot::error::TryRecvError::Closed)) => Some(Err(())),
                Some(Err(oneshot::error::TryRecvError::Empty)) | None => None,
            };
            if let Some(resolved) = resolved {
                let flight = in_flight.take().expect("checked above");
                self.resolve(flight, resolved, &mut group, &mut pending);
                continue;
            }
            // A group grows only while its predecessor commits; with none
            // in flight it goes as soon as it is prepared.
            let room = match &group {
                None => true,
                Some(building) => in_flight.is_some() && building.has_room(),
            };
            if !pending.is_empty() && room {
                let building = group.get_or_insert_with(|| {
                    self.next_seq += 1;
                    Group {
                        seq: self.next_seq,
                        follows: Head::Entry(0),
                        proposed_ms: now_ms(),
                        items: Vec::new(),
                        bytes: 0,
                        jobs: Vec::new(),
                        chunks: 0,
                        on_predecessor: in_flight.is_some(),
                        doomed: false,
                        full: false,
                        trace: self.batch_span(self.next_seq),
                        preparing: Duration::ZERO,
                        began: Instant::now(),
                        snapshots: telemetry::snapshot_activity(),
                    }
                });
                let predecessor = in_flight.as_ref().map(|flight| Predecessor {
                    items: flight.group.items.clone(),
                    follows: flight.group.follows,
                    batch: Head::Batch {
                        writer: self.id,
                        seq: flight.group.seq,
                    },
                    proposed_ms: flight.group.proposed_ms,
                });
                self.prepare_chunk(building, predecessor, &mut pending)
                    .await;
                continue;
            }
            if let Some(flight) = in_flight.as_mut() {
                tokio::select! {
                    resolved = &mut flight.receiver => {
                        let flight = in_flight.take().expect("selected above");
                        self.resolve(flight, resolved.map_err(|_| ()), &mut group, &mut pending);
                    }
                    job = receiver.recv(), if !closed && room => match job {
                        Some(job) => pending.push_back(Queued::new(job)),
                        None => closed = true,
                    },
                }
                continue;
            }
            if let Some(ready) = group.take() {
                in_flight = self.submit(ready, pending.len()).await;
                continue;
            }
            if closed {
                return;
            }
            match receiver.recv().await {
                Some(job) => pending.push_back(Queued::new(job)),
                None => closed = true,
            }
        }
    }

    /// Prepare every queued write that fits into `group`, in order, on
    /// the staged state: committed state, the predecessor if it has not
    /// applied yet, then the group's writes so far.
    async fn prepare_chunk(
        &self,
        group: &mut Group,
        predecessor: Option<Predecessor>,
        pending: &mut VecDeque<Queued>,
    ) {
        let mut jobs = Vec::new();
        let mut budget = GROUP_ITEMS.saturating_sub(group.items.len()).max(1);
        while budget > 0
            && let Some(mut job) = pending.pop_front()
        {
            // Past its deadline, or shed: answered unprepared, so it
            // never ran and its caller may retry.
            if let Err(refused) = job.job.turn(job.attempts == 0) {
                if telemetry::enabled() {
                    use crate::methods::Refusal as Why;
                    let reason = match Why::of_error(&refused) {
                        Some(Why::Deadline) => Refusal::Deadline,
                        Some(Why::Shed) => Refusal::Shed,
                        _ => Refusal::Other,
                    };
                    self.observer.writer_rejected(reason);
                }
                job.job.finish(Err(refused), CommitPhases::default());
                continue;
            }
            // The batch represents every write in it: it links them,
            // when it records.
            if let Some(link) = job.job.link()
                && telemetry::records(&group.trace)
            {
                telemetry::link(&group.trace, link);
            }
            jobs.push(job);
            budget -= 1;
        }
        if jobs.is_empty() {
            return;
        }
        let items = std::mem::take(&mut group.items);
        let (proposed_ms, first) = (group.proposed_ms, group.chunks == 0);
        group.chunks += 1;
        let span = if telemetry::enabled() {
            tracing::info_span!(
                target: OTEL_TARGET,
                parent: &group.trace,
                "flower.writer.prepare",
                requests = telemetry::int(jobs.len()),
                chunk = telemetry::int(group.chunks),
            )
        } else {
            tracing::Span::none()
        };
        let started = Instant::now();
        let (speculating, counters) = (self.speculating.clone(), self.counters.clone());
        let observer = self.observer.clone();
        let staged = with_engine(&self.engine, move |engine| {
            // Evaluations inside are this span's children.
            let _entered = span.enter();
            let mut speculating = speculating
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let waves = Waves {
                speculating: &mut speculating,
                counters: &counters.speculation,
                observer: &observer,
                span: &span,
            };
            let (jobs, result) =
                stage_and_prepare(engine, predecessor, &items, proposed_ms, jobs, waves);
            (items, jobs, result)
        })
        .await;
        group.preparing += started.elapsed();
        let (items, jobs, result) = match staged {
            Ok(staged) => staged,
            Err(panic) => {
                // The jobs went down with the task: their callers see the
                // writer drop them. The group's earlier writes are intact.
                tracing::error!(target: "flower::writer", %panic, "a preparation panicked");
                group.doomed = true;
                return;
            }
        };
        group.items = items;
        match result {
            Ok(Chunk::Staged { follows, proposed }) => {
                if first {
                    group.follows = follows;
                }
                // The group proposes one entry, which every follower's
                // frame must carry: past the transaction bound, a write
                // and every write prepared on top of it wait for the
                // next entry, unless it is the entry's first.
                let bound = flower_engine::patch::max_transaction_bytes();
                let mut overflow = Vec::new();
                for (mut job, item) in jobs.into_iter().zip(proposed) {
                    if !overflow.is_empty()
                        || item.as_ref().is_some_and(|item| {
                            !group.items.is_empty() && group.bytes + item.len() > bound
                        })
                    {
                        overflow.push(job);
                        continue;
                    }
                    if let Some(message) = job.job.disk_failure() {
                        self.health.fail(GROUP, message);
                    }
                    if let Some(item) = item {
                        job.item = Some(group.items.len());
                        group.bytes += item.len();
                        group.items.push(item);
                    }
                    group.jobs.push(job);
                }
                if !overflow.is_empty() {
                    group.full = true;
                    for job in overflow.into_iter().rev() {
                        pending.push_front(job);
                    }
                }
            }
            Ok(Chunk::Moved) => {
                // The head moved past the predecessor's: everything built
                // on it is void. Requeue once the predecessor resolves.
                group.doomed = true;
                for job in jobs.into_iter().rev() {
                    pending.push_front(job);
                }
            }
            Err(error) => {
                if let NodeError::Failure { code, message } = &error
                    && code == IO_FAILURE
                {
                    self.health.fail(GROUP, message);
                }
                fail_all(jobs, error);
            }
        }
    }

    /// Batch `seq`'s root span, linking the spans of the writes it
    /// carries, when telemetry is on; watch deliveries its commit wakes
    /// link to it.
    fn batch_span(&self, seq: u64) -> tracing::Span {
        if !telemetry::enabled() {
            return tracing::Span::none();
        }
        // Many unrelated requests share one batch: it starts its own
        // trace, and links them rather than inherit one's parent.
        let span = tracing::info_span!(
            target: OTEL_TARGET,
            parent: None,
            "flower.writer.batch",
            flower.group = self.observer.group(),
            requests = tracing::field::Empty,
            commands = tracing::field::Empty,
            decided = tracing::field::Empty,
            bytes = tracing::field::Empty,
            stop_reason = tracing::field::Empty,
            status = tracing::field::Empty,
            otel.status_code = tracing::field::Empty,
            flower.node.id = tracing::field::Empty,
            flower.raft.leader_id = tracing::field::Empty,
            flower.raft.term = tracing::field::Empty,
            flower.raft.snapshot_in_progress = tracing::field::Empty,
        );
        if telemetry::records(&span) {
            let metrics = self.raft.metrics();
            let metrics = metrics.borrow();
            span.record("flower.node.id", telemetry::int(metrics.id));
            span.record("flower.raft.term", telemetry::int(metrics.current_term));
            if let Some(leader) = metrics.current_leader {
                span.record("flower.raft.leader_id", telemetry::int(leader));
            }
            telemetry::remember_batch(self.id, seq, &span);
        }
        span
    }

    /// The leader's replication lag now: log entries it has not applied,
    /// and entries a quorum of voters has not matched.
    fn lag(&self) -> (u64, u64) {
        let metrics = self.raft.metrics();
        let metrics = metrics.borrow();
        let last = metrics.last_log_index.unwrap_or(0);
        let applied = metrics.last_applied.map_or(0, |id| id.index);
        let voters: Vec<NodeId> = metrics.membership_config.membership().voter_ids().collect();
        let mut matched: Vec<u64> = voters
            .iter()
            .map(|voter| {
                if *voter == metrics.id {
                    last
                } else {
                    metrics
                        .replication
                        .as_ref()
                        .and_then(|replication| replication.get(voter).copied().flatten())
                        .map_or(0, |id| id.index)
                }
            })
            .collect();
        matched.sort_unstable_by(|a, b| b.cmp(a));
        let quorum = matched.get(voters.len() / 2).copied().unwrap_or(last);
        (last.saturating_sub(applied), last.saturating_sub(quorum))
    }

    /// Propose a prepared group as one batch, or answer it at once when
    /// it proposes nothing. `None` when nothing is left committing.
    /// `queued` writes wait behind it.
    async fn submit(&mut self, group: Group, queued: usize) -> Option<InFlight> {
        if group.doomed {
            // Only a panicked preparation leaves one here: the state its
            // writes were prepared on is unknown.
            let error = NodeError::Overloaded {
                message: "the writer lost the state this write was prepared on".to_string(),
                retry_after_ms: 100,
            };
            ended(&group.trace, "error");
            fail_all(group.jobs, error);
            return None;
        }
        if group.items.is_empty() {
            let decided = group.jobs.len() as u64;
            self.counters.decided.fetch_add(decided, Ordering::Relaxed);
            if telemetry::records(&group.trace) {
                group.trace.record("requests", telemetry::int(decided));
                group.trace.record("commands", 0);
                group.trace.record("decided", telemetry::int(decided));
            }
            ended(&group.trace, "ok");
            for job in group.jobs {
                job.job.finish(Ok(None), CommitPhases::default());
            }
            return None;
        }
        if let Some(failure) = self.health.failure() {
            let error = NodeError::Failure {
                code: STORAGE_FAILED.to_string(),
                message: failure.to_string(),
            };
            ended(&group.trace, "error");
            fail_all(group.jobs, error);
            return None;
        }
        let admitted = match self.leadership.admit_owned().await {
            Ok(admitted) => admitted,
            Err(error) => {
                ended(&group.trace, "error");
                fail_all(group.jobs, error);
                return None;
            }
        };
        let write = ClusterWrite::Batch {
            items: group.items.clone(),
            follows: group.follows,
            writer: self.id,
            seq: group.seq,
            proposed_ms: group.proposed_ms,
        };
        let (commit, replicate, lag) = if telemetry::enabled() {
            let commit = tracing::info_span!(
                target: OTEL_TARGET,
                parent: &group.trace,
                "flower.writer.commit",
                commands = telemetry::int(group.items.len()),
                outcome = tracing::field::Empty,
            );
            let replicate = tracing::info_span!(
                target: OTEL_TARGET,
                parent: &commit,
                "flower.writer.replicate",
                entries = 1,
                flower.raft.log_index = tracing::field::Empty,
                flower.raft.append_ms = tracing::field::Empty,
                flower.raft.replicate_ms = tracing::field::Empty,
                flower.raft.apply_ms = tracing::field::Empty,
                flower.raft.snapshot_in_progress = tracing::field::Empty,
                outcome = tracing::field::Empty,
            );
            (commit, replicate, self.lag())
        } else {
            (tracing::Span::none(), tracing::Span::none(), (0, 0))
        };
        let submitted = Instant::now();
        match self.raft.client_write_ff(write).await {
            Ok(receiver) => Some(InFlight {
                stop: group.stop_reason(),
                group,
                receiver,
                _admitted: admitted,
                submitted,
                queued,
                lag,
                commit,
                replicate,
            }),
            Err(fatal) => {
                ended(&group.trace, "error");
                fail_all(group.jobs, NodeError::Fatal(fatal.to_string()));
                None
            }
        }
    }

    /// Report a resolved batch: its spans' attributes, and the batch
    /// metrics, as it ended with `outcome` at log index `index`.
    fn report(&self, flight: &InFlight, outcome: BatchOutcome, index: Option<u64>) -> CommitPhases {
        let phases = index.map_or_else(CommitPhases::default, |index| {
            self.marks.phases(index, flight.submitted)
        });
        if !telemetry::enabled() {
            return phases;
        }
        let group = &flight.group;
        let decided = group.jobs.iter().filter(|job| job.item.is_none()).count() as u64;
        let overlapped = group.snapshots.overlapped(telemetry::snapshot_activity());
        let status = match outcome {
            BatchOutcome::Ok => "ok",
            BatchOutcome::Stale => "stale",
            BatchOutcome::Error => "error",
        };
        if telemetry::records(&flight.replicate) {
            let span = &flight.replicate;
            span.record("outcome", status);
            if let Some(index) = index {
                span.record("flower.raft.log_index", telemetry::int(index));
                let [appended, applying, applied] = self.marks.instants(index, flight.submitted);
                for (name, at) in [
                    ("appended", appended),
                    ("committed", applying),
                    ("applied", applied),
                ] {
                    if let Some(at) = at {
                        telemetry::event_at(span, name, at);
                    }
                }
            }
            let ms = |elapsed: Duration| elapsed.as_secs_f64() * 1000.0;
            for (field, elapsed) in [
                ("flower.raft.append_ms", phases.append),
                ("flower.raft.replicate_ms", phases.replicate),
                ("flower.raft.apply_ms", phases.apply),
            ] {
                if let Some(elapsed) = elapsed {
                    span.record(field, ms(elapsed));
                }
            }
            if overlapped {
                span.record("flower.raft.snapshot_in_progress", true);
            }
            flight.commit.record("outcome", status);
        }
        if telemetry::records(&group.trace) {
            let span = &group.trace;
            span.record("requests", telemetry::int(group.jobs.len()));
            span.record("commands", telemetry::int(group.items.len()));
            span.record("decided", telemetry::int(decided));
            span.record("bytes", telemetry::int(group.bytes));
            span.record("stop_reason", flight.stop.name());
            if overlapped {
                span.record("flower.raft.snapshot_in_progress", true);
            }
        }
        ended(&group.trace, status);
        let observer = &self.observer;
        observer.writer_batch(&BatchReport {
            outcome,
            stop: flight.stop,
            requests: group.jobs.len() as u64,
            commands: group.items.len() as u64,
            decided,
            bytes: group.bytes as u64,
            queued: flight.queued as u64,
            local_unapplied: flight.lag.0,
            quorum_unmatched: flight.lag.1,
        });
        observer.writer_stage(WriterStage::BatchPrepare, group.preparing);
        observer.writer_stage(
            WriterStage::BatchFill,
            flight.submitted.saturating_duration_since(group.began),
        );
        observer.writer_stage(WriterStage::BatchCommit, flight.submitted.elapsed());
        for (stage, elapsed) in [
            (WriterStage::Append, phases.append),
            (WriterStage::Replicate, phases.replicate),
            (WriterStage::Apply, phases.apply),
        ] {
            if let Some(elapsed) = elapsed {
                observer.writer_stage(stage, elapsed);
            }
        }
        phases
    }

    /// Settle a resolved batch: answer its writes, or prepare them again
    /// when it went stale, and void the group built on it when it failed.
    fn resolve(
        &self,
        flight: InFlight,
        resolved: Result<Result<ClientWriteResponse<TypeConfig>, Error>, ()>,
        group: &mut Option<Group>,
        pending: &mut VecDeque<Queued>,
    ) {
        let built_on = group.as_ref().is_some_and(|next| next.on_predecessor);
        match resolved {
            Ok(Ok(response)) => {
                let index = response.log_id.index;
                let response = response.data;
                let stale = response.items.len() != flight.group.items.len()
                    || response.items.first().is_some_and(is_stale_batch);
                if stale {
                    self.report(&flight, BatchOutcome::Stale, Some(index));
                    self.counters.stale.fetch_add(1, Ordering::Relaxed);
                    let mut again = flight.group.jobs;
                    if built_on && let Some(next) = group.take() {
                        ended(&next.trace, "stale");
                        again.extend(next.jobs);
                    }
                    requeue(again, pending, &response);
                    return;
                }
                let phases = self.report(&flight, BatchOutcome::Ok, Some(index));
                let done = flight.group;
                let writes = done.items.len() as u64;
                self.counters.batches.fetch_add(1, Ordering::Relaxed);
                self.counters.writes.fetch_add(writes, Ordering::Relaxed);
                self.counters.largest.fetch_max(writes, Ordering::Relaxed);
                for job in done.jobs {
                    let answer = job.item.map(|item| response.items[item].clone());
                    if answer.is_none() {
                        self.counters.decided.fetch_add(1, Ordering::Relaxed);
                    }
                    job.job.finish(Ok(answer), phases);
                }
                if group.as_ref().is_some_and(|next| next.doomed)
                    && let Some(next) = group.take()
                {
                    ended(&next.trace, "stale");
                    requeue(next.jobs, pending, &response);
                }
            }
            Ok(Err(error)) => {
                self.report(&flight, BatchOutcome::Error, None);
                let error = NodeError::write(RaftError::APIError(error));
                if built_on && let Some(next) = group.take() {
                    ended(&next.trace, "error");
                    fail_all(next.jobs, error.clone());
                }
                fail_all(flight.group.jobs, error);
            }
            Err(()) => {
                self.report(&flight, BatchOutcome::Error, None);
                let error = NodeError::Fatal("Raft dropped a batch in flight".to_string());
                if built_on && let Some(next) = group.take() {
                    ended(&next.trace, "error");
                    fail_all(next.jobs, error.clone());
                }
                fail_all(flight.group.jobs, error);
            }
        }
    }
}

impl Queued {
    fn new(job: Box<dyn Job>) -> Self {
        Queued {
            job,
            attempts: 0,
            item: None,
        }
    }
}

/// Answer every write with `error`.
fn fail_all(jobs: Vec<Queued>, error: NodeError) {
    for job in jobs {
        job.job.finish(Err(error.clone()), CommitPhases::default());
    }
}

/// Record how a batch span ended (`ok`, `stale` or `error`), when it
/// records.
fn ended(span: &tracing::Span, status: &'static str) {
    if telemetry::records(span) {
        span.record("status", status);
        if status != "ok" {
            span.record("otel.status_code", "ERROR");
        }
    }
}

/// Put writes back at the front of the queue, in order, to prepare
/// again; one that already used its attempts is answered with an
/// overload instead.
fn requeue(jobs: Vec<Queued>, pending: &mut VecDeque<Queued>, response: &WriteResponse) {
    let mut again = Vec::with_capacity(jobs.len());
    for mut job in jobs {
        job.attempts += 1;
        job.item = None;
        if job.attempts >= WRITE_ATTEMPTS {
            let message = match &response.items.first() {
                Some(item) if is_stale_batch(item) => {
                    format!(
                        "{:?}: other entries kept landing before its batch",
                        item.outcome
                    )
                }
                _ => "other entries kept landing before its batch".to_string(),
            };
            job.job.finish(
                Err(NodeError::Overloaded {
                    message,
                    retry_after_ms: 100,
                }),
                CommitPhases::default(),
            );
        } else {
            again.push(job);
        }
    }
    for job in again.into_iter().rev() {
        pending.push_front(job);
    }
}

/// One chunk's preparation.
enum Chunk {
    /// Every write prepared, each with the item it proposes, if any.
    Staged {
        follows: Head,
        proposed: Vec<Option<BatchItem>>,
    },
    /// The head is neither where the predecessor was prepared nor the
    /// predecessor: something else applied, nothing was prepared.
    Moved,
}

/// What a chunk's preparation speculates with.
struct Waves<'a> {
    speculating: &'a mut Speculating,
    counters: &'a speculation::Counters,
    /// The group's instruments.
    observer: &'a Observer,
    /// The chunk's `flower.writer.prepare` span, which speculative
    /// evaluations run inside.
    span: &'a tracing::Span,
}

/// How one write of a wave prepares.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Plan {
    /// Not a mutation: in its turn, on the staged state.
    Serial,
    /// A mutation prepared in its turn while waves cool down.
    Cooled,
    /// A mutation prediction placed in its turn.
    Placed,
    /// A mutation prepared speculatively, then validated in its turn;
    /// whether an earlier slot named its record.
    Candidate(Option<bool>),
}

/// Stage the predecessor (unless it applied) and the group's writes so
/// far in a throwaway (scratch) transaction, then prepare `jobs` in
/// order on top, each staged before the next prepares; mutations in
/// waves, speculatively, when the writer speculates (see the module
/// docs). The jobs come back in their order, whatever happened.
fn stage_and_prepare(
    engine: &mut Engine,
    predecessor: Option<Predecessor>,
    items: &[BatchItem],
    proposed_ms: u64,
    jobs: Vec<Queued>,
    waves: Waves<'_>,
) -> (Vec<Queued>, Result<Chunk, NodeError>) {
    let mut slots: Vec<Option<Queued>> = jobs.into_iter().map(Some).collect();
    let result = stage_and_prepare_in(engine, predecessor, items, proposed_ms, &mut slots, waves);
    (slots.into_iter().flatten().collect(), result)
}

/// [`stage_and_prepare`] on jobs in slots, which a wave empties while
/// its candidates prepare elsewhere and fills again.
fn stage_and_prepare_in(
    engine: &mut Engine,
    predecessor: Option<Predecessor>,
    items: &[BatchItem],
    proposed_ms: u64,
    slots: &mut [Option<Queued>],
    waves: Waves<'_>,
) -> Result<Chunk, NodeError> {
    let speculable = slots
        .iter()
        .flatten()
        .filter(|job| job.job.speculable().is_some())
        .count();
    let speculates = waves.speculating.refresh().speculates() && speculable >= 2;
    let staged = engine.store().begin_scratch().map_err(NodeError::engine)?;
    if speculates {
        staged.log_writes();
    }
    let head = Head::read(&staged.read()).map_err(NodeError::engine)?;
    let mut position = STAGED_POSITION;
    let follows = match predecessor {
        None => head,
        Some(predecessor) if head == predecessor.batch => predecessor.batch,
        Some(predecessor) if head == predecessor.follows => {
            stage(
                engine,
                &staged,
                &predecessor.items,
                &mut position,
                predecessor.proposed_ms,
            )?;
            predecessor.batch
        }
        Some(_) => return Ok(Chunk::Moved),
    };
    stage(engine, &staged, items, &mut position, proposed_ms)?;
    let mut proposed = Vec::with_capacity(slots.len());
    if !speculates {
        for slot in slots.iter_mut() {
            let job = slot.as_mut().expect("every job is in its slot");
            let item = engine.on_staged(&staged, |engine| job.job.prepare(engine));
            if let Some(item) = &item {
                stage(
                    engine,
                    &staged,
                    std::slice::from_ref(item),
                    &mut position,
                    proposed_ms,
                )?;
            }
            proposed.push(item);
        }
        return Ok(Chunk::Staged { follows, proposed });
    }
    let Waves {
        speculating,
        counters,
        span,
        observer,
    } = waves;
    let mut overlay = Overlay::default();
    let mut mark = 0;
    let mut next = 0;
    while next < slots.len() {
        let width = speculating.policy.width();
        let end = (next + width.max(1)).min(slots.len());
        let clock = speculating.clock();
        // Who speculates: mutations, unless the wave cools down or
        // prediction places them in their turn.
        let mut named: Vec<Vec<u8>> = Vec::new();
        let mut plans = Vec::with_capacity(end - next);
        for slot in &slots[next..end] {
            let job = slot.as_ref().expect("every job is in its slot");
            let plan = match job.job.speculable() {
                None => Plan::Serial,
                Some(_) if width <= 1 => Plan::Cooled,
                Some(call) => {
                    let placement = speculating
                        .policy
                        .placement(call, &|key| named.iter().any(|held| held == key));
                    if let Some(key) = placement.key {
                        named.push(key);
                    }
                    if placement.serial {
                        Plan::Placed
                    } else {
                        Plan::Candidate(placement.exposed)
                    }
                }
            };
            plans.push(plan);
        }
        let candidates: Vec<usize> = plans
            .iter()
            .enumerate()
            .filter(|(_, plan)| matches!(plan, Plan::Candidate(_)))
            .map(|(offset, _)| next + offset)
            .collect();
        if !candidates.is_empty() {
            // The wave's snapshot: the staged state as it stands.
            mark = staged
                .extend_overlay(&mut overlay, mark)
                .map_err(NodeError::engine)?;
            let shared = Arc::new(std::mem::take(&mut overlay));
            let speculation = engine.speculation(shared.clone(), Some(clock));
            let jobs: Vec<Queued> = candidates
                .iter()
                .map(|index| slots[*index].take().expect("every job is in its slot"))
                .collect();
            counters.waves.fetch_add(1, Ordering::Relaxed);
            counters
                .candidates
                .fetch_add(jobs.len() as u64, Ordering::Relaxed);
            let span = span.clone();
            let work: Arc<dyn Fn(&mut Queued) + Send + Sync> = Arc::new(move |job: &mut Queued| {
                let _entered = span.enter();
                job.job.speculate(&speculation);
            });
            let back = match &speculating.pool {
                Some(pool) if pool.threads() > 0 => pool.run(jobs, work),
                _ => {
                    let mut jobs = jobs;
                    jobs.iter_mut().for_each(|job| work(job));
                    jobs
                }
            };
            for (index, job) in candidates.iter().zip(back) {
                slots[*index] = Some(job);
            }
            overlay = Arc::try_unwrap(shared).unwrap_or_else(|shared| (*shared).clone());
        }
        let snapshot = mark;
        let (mut reused, mut rejected) = (0, 0);
        for (offset, plan) in plans.into_iter().enumerate() {
            let job = slots[next + offset]
                .as_mut()
                .expect("every job is in its slot");
            let item = match plan {
                Plan::Candidate(exposed) => {
                    let call = job
                        .job
                        .speculable()
                        .cloned()
                        .expect("a candidate is a mutation");
                    let verdict = job.job.certificate().map(|certificate| {
                        staged.logged(snapshot, |writes| certificate.verdict(writes))
                    });
                    match verdict {
                        Some(Verdict::Holds) => {
                            reused += 1;
                            counters.reused.fetch_add(1, Ordering::Relaxed);
                            methods::speculation_reused(method_of(&call));
                            speculating
                                .policy
                                .observe_candidate(&call, false, exposed, &[]);
                            match job.job.adopt() {
                                Some(item) => {
                                    Some(rebase(&staged.read(), item).map_err(NodeError::engine)?)
                                }
                                None => None,
                            }
                        }
                        verdict => {
                            job.job.discard();
                            let read = staged.read();
                            let rejection = match verdict {
                                Some(Verdict::Conflicts(rows)) => {
                                    rejected += 1;
                                    counters.conflicted.fetch_add(1, Ordering::Relaxed);
                                    let conflicts = Conflicts::of(&read, &rows);
                                    for class in &conflicts.classes {
                                        counters.conflict(*class);
                                    }
                                    speculating.policy.observe_candidate(
                                        &call,
                                        true,
                                        exposed,
                                        &conflicts.rows,
                                    );
                                    methods::Rejection::Certificate(
                                        conflicts.classes,
                                        conflicts.names,
                                    )
                                }
                                Some(Verdict::Untracked) => {
                                    rejected += 1;
                                    counters.untracked.fetch_add(1, Ordering::Relaxed);
                                    methods::Rejection::Untracked
                                }
                                Some(Verdict::Holds) | None => {
                                    counters.failed.fetch_add(1, Ordering::Relaxed);
                                    methods::Rejection::Failed
                                }
                            };
                            if telemetry::enabled() {
                                rejection_metrics(observer, &rejection);
                            }
                            methods::speculation_rejected(method_of(&call), rejection);
                            prepare_serially(engine, &staged, Some(clock), job)
                        }
                    }
                }
                Plan::Placed => {
                    counters.placed.fetch_add(1, Ordering::Relaxed);
                    if let Some(call) = job.job.speculable() {
                        methods::speculation_placed(method_of(call));
                    }
                    prepare_serially(engine, &staged, Some(clock), job)
                }
                Plan::Cooled => {
                    counters.cooldown.fetch_add(1, Ordering::Relaxed);
                    if let Some(call) = job.job.speculable() {
                        methods::speculation_cooled(method_of(call));
                    }
                    prepare_serially(engine, &staged, Some(clock), job)
                }
                Plan::Serial => prepare_serially(engine, &staged, None, job),
            };
            if let Some(item) = &item {
                stage(
                    engine,
                    &staged,
                    std::slice::from_ref(item),
                    &mut position,
                    proposed_ms,
                )?;
            }
            proposed.push(item);
        }
        if !candidates.is_empty() && speculating.policy.observe_wave(reused, rejected) {
            counters.backoffs.fetch_add(1, Ordering::Relaxed);
        }
        next = end;
    }
    Ok(Chunk::Staged { follows, proposed })
}

/// Prepare one write in its turn on the staged state; a mutation at its
/// wave's `clock`.
fn prepare_serially(
    engine: &mut Engine,
    staged: &StagedWrite,
    clock: Option<u64>,
    job: &mut Queued,
) -> Option<BatchItem> {
    engine.on_staged(staged, |engine| {
        engine.at_clock(clock, |engine| job.job.prepare(engine))
    })
}

/// Count a rejected candidate in `flower.writer.speculation.rejections`.
fn rejection_metrics(observer: &Observer, rejection: &methods::Rejection) {
    use crate::telemetry::{ConflictKind, Rejection};
    match rejection {
        methods::Rejection::Certificate(classes, _) => {
            for class in classes {
                observer
                    .writer_speculation_rejected(Rejection::Certificate, ConflictKind::of(*class));
            }
        }
        methods::Rejection::Untracked => {
            observer.writer_speculation_rejected(Rejection::Untracked, ConflictKind::None);
        }
        methods::Rejection::Failed => {
            observer.writer_speculation_rejected(Rejection::Failed, ConflictKind::None);
        }
    }
}

/// The method a mutation names, for its row in `/v1/metrics` `methods`.
fn method_of(call: &Speculable) -> methods::MethodRef<'_> {
    methods::MethodRef {
        db: &call.db,
        module: &call.module,
        name: &call.name,
    }
}

/// What a rejected candidate conflicted on: each class of row once, the
/// collections and derived definitions named, and the records and
/// derived outcomes, for prediction.
struct Conflicts {
    classes: Vec<ConflictClass>,
    names: Vec<(ConflictClass, String)>,
    rows: Vec<ConflictRow>,
}

impl Conflicts {
    fn of(read: &flower_engine::storage::ReadTxn, writes: &[Write]) -> Self {
        let mut conflicts = Conflicts {
            classes: Vec::new(),
            names: Vec::new(),
            rows: Vec::new(),
        };
        for write in writes {
            let class = ConflictClass::of(write.0);
            if !conflicts.classes.contains(&class) {
                conflicts.classes.push(class);
            }
            if let Some(row) = ConflictRow::of(write) {
                if let Some(name) = row.name(read)
                    && !conflicts
                        .names
                        .iter()
                        .any(|(known, named)| *known == class && *named == name)
                {
                    conflicts.names.push((class, name));
                }
                conflicts.rows.push(row);
            }
        }
        conflicts
    }
}

/// Stage items one position apart, as their batch will apply them.
fn stage(
    engine: &Engine,
    staged: &StagedWrite,
    items: &[BatchItem],
    position: &mut u64,
    proposed_ms: u64,
) -> Result<(), NodeError> {
    if items.is_empty() {
        return Ok(());
    }
    let clock = StateMachine::advance_expiry_clock(staged.write(), proposed_ms)
        .map_err(NodeError::failure)?;
    for item in items {
        *position += 1;
        StateMachine::stage_items(engine, staged, std::slice::from_ref(item), *position, clock)
            .map_err(NodeError::failure)?;
    }
    Ok(())
}
