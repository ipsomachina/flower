//! The poll loop: claims sized to free room, concurrent claimers once
//! the queue keeps them full, concurrent handlers under a fixed or
//! adaptive limit, renewal and best-effort reports, places in the
//! queue's line, chained claims, idle jobs and drain deadlines.

use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicIsize, AtomicU8, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use flower_client::CancellationToken;
use flower_client::error::FlowerError;
use flower_client::work::{Claim, QueuePoll, QueueReadiness, QueueReport};
use futures_util::FutureExt;
use futures_util::future::BoxFuture;
use futures_util::stream::{FuturesUnordered, StreamExt};
use tokio::sync::Notify;
use tokio::task::JoinSet;
use tokio::time::Instant;

use crate::capacity::{Adaptive, Health, LimitChange, Limiter, ProcessHealth};
use crate::client::QueueClient;

/// How long claims pause once Flower refuses calls for load.
const OVERLOAD_PAUSE: Duration = Duration::from_secs(1);
/// The limit change reason for refusals for load.
const OVERLOAD_REASON: &str = "Flower is shedding load";
/// How long work may take to stop once a drain deadline cancelled it,
/// before a stopping worker gives up on it (v1's 5 s).
const ABANDON_AFTER: Duration = Duration::from_secs(5);
/// How long a stopping worker spends giving up its place in line.
const LEAVE_TIMEOUT: Duration = Duration::from_secs(2);
/// The shortest place in line: refreshing it takes a moment.
const MIN_WAIT: Duration = Duration::from_millis(2);
/// The longest place in line, as the engine bounds it.
const MAX_WAIT: Duration = Duration::from_secs(60 * 60);
/// Longest owner, in bytes, as the engine bounds it.
const MAX_OWNER: usize = 1024;

/// A handler's outcome for one item.
#[derive(Debug)]
pub enum WorkError {
    /// Fail the claim and requeue after a delay, or the default
    /// backoff when `None`.
    Retry(Option<Duration>),
    /// Kill the claim into the dead letter file now. The reason is
    /// caller context only; the dead letter keeps the attempts.
    Terminal(String),
}

/// What a worker reports as it runs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkerEvent {
    /// The adaptive limit changed.
    Limit(LimitChange),
    /// Flower, or the service behind a job, refused work for load:
    /// claims pause.
    Throttled {
        /// How long claims pause.
        pause: Duration,
    },
}

/// Receives every [`WorkerEvent`], in order.
pub type EventHandler = Arc<dyn Fn(WorkerEvent) + Send + Sync>;

/// Worker tuning.
#[derive(Clone)]
pub struct QueueWorkerOptions {
    /// Database name.
    pub db: String,
    /// Queue name.
    pub queue: String,
    /// The scope to claim from (default `""`, the default scope).
    pub scope: String,
    /// The claimer recorded on each lease and in the scope's line
    /// (default `worker-` and a random ID): unique per process.
    pub owner: String,
    /// Simultaneous handlers with a fixed limit (default 4).
    pub concurrency: usize,
    /// An adaptive limit instead: it follows demand and the process's
    /// load within these bounds (see [`crate::capacity`]).
    pub adaptive: Option<Adaptive>,
    /// The process's load, read at each adjustment of an adaptive
    /// limit. Default [`ProcessHealth`].
    pub health: Option<Arc<dyn Health>>,
    /// How often an adaptive limit follows demand and load, and the
    /// worker checks for refusals for load (default 250 ms).
    pub adjust_every: Duration,
    /// Most items per poll (default = the limit). A poll never claims
    /// more than the handler slots free at that moment.
    pub batch: Option<usize>,
    /// Polls in flight at once once polls come back full (default 4,
    /// at most the largest limit). An empty or short poll drops back
    /// to one, so an idle queue costs one poll per interval.
    pub claimers: usize,
    /// Claim lease (default 30 s).
    pub lease: Duration,
    /// Empty-poll and error sleep (default 1 s).
    pub poll_interval: Duration,
    /// Default backoff base (default 1 s).
    pub retry_base: Duration,
    /// Default backoff cap (default 60 s).
    pub retry_max: Duration,
    /// Wait in the scope's line this long after a short claim (default
    /// none): new work then wakes one worker, the first in line, which
    /// has it to itself for the queue's turn. A waiting worker reads
    /// whether it is its turn, rather than claiming, until it is, and
    /// claims again by half the wait to keep its place.
    pub wait: Option<Duration>,
    /// Report each outcome with a claim of the next items, as many as
    /// the worker has room for, in the same commit (default false):
    /// while work waits, an item costs its report alone rather than a
    /// poll too.
    pub chain: bool,
    /// Once stopping, how long held items may keep running (default:
    /// until they finish). At the deadline their handlers are dropped
    /// and their claims failed with the default backoff, or released
    /// with [`QueueWorkerOptions::release_drained`]: never completed.
    /// Handlers still running [`ABANDON_AFTER`] later (stuck outside an
    /// await point) are given up on: counted lost, their claims
    /// redeliver when their leases lapse.
    pub drain: Option<Duration>,
    /// Release items still running at the drain deadline instead of
    /// failing them: they run again at once, their attempt counted.
    /// Needs [`QueueWorkerOptions::drain`].
    pub release_drained: bool,
    /// Receives limit changes and throttling.
    pub on_event: Option<EventHandler>,
}

impl std::fmt::Debug for QueueWorkerOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueueWorkerOptions")
            .field("db", &self.db)
            .field("queue", &self.queue)
            .field("scope", &self.scope)
            .field("owner", &self.owner)
            .field("concurrency", &self.concurrency)
            .field("adaptive", &self.adaptive)
            .field("adjust_every", &self.adjust_every)
            .field("batch", &self.batch)
            .field("claimers", &self.claimers)
            .field("lease", &self.lease)
            .field("poll_interval", &self.poll_interval)
            .field("wait", &self.wait)
            .field("chain", &self.chain)
            .field("drain", &self.drain)
            .field("release_drained", &self.release_drained)
            .finish_non_exhaustive()
    }
}

impl QueueWorkerOptions {
    /// Tune a worker for a database queue.
    pub fn new(db: impl Into<String>, queue: impl Into<String>) -> Self {
        QueueWorkerOptions {
            db: db.into(),
            queue: queue.into(),
            scope: String::new(),
            owner: format!("worker-{}", uuid::Uuid::new_v4().simple()),
            concurrency: 4,
            adaptive: None,
            health: None,
            adjust_every: Duration::from_millis(250),
            batch: None,
            claimers: 4,
            lease: Duration::from_secs(30),
            poll_interval: Duration::from_secs(1),
            retry_base: Duration::from_secs(1),
            retry_max: Duration::from_secs(60),
            wait: None,
            chain: false,
            drain: None,
            release_drained: false,
            on_event: None,
        }
    }

    /// Claim from one scope of the queue only.
    pub fn scope(mut self, scope: impl Into<String>) -> Self {
        self.scope = scope.into();
        self
    }

    /// The claimer recorded on leases and in lines.
    pub fn owner(mut self, owner: impl Into<String>) -> Self {
        self.owner = owner.into();
        self
    }

    /// Simultaneous handlers, fixed.
    pub fn concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency.max(1);
        self.adaptive = None;
        self
    }

    /// Simultaneous handlers following demand and load from `min`
    /// through `max`, starting at `initial`.
    pub fn adaptive(mut self, min: usize, initial: usize, max: usize) -> Self {
        self.adaptive = Some(Adaptive::new(min, initial, max));
        self
    }

    /// The load an adaptive limit reads.
    pub fn health(mut self, health: Arc<dyn Health>) -> Self {
        self.health = Some(health);
        self
    }

    /// How often an adaptive limit adjusts.
    pub fn adjust_every(mut self, every: Duration) -> Self {
        self.adjust_every = every;
        self
    }

    /// Most items per poll.
    pub fn batch(mut self, batch: usize) -> Self {
        self.batch = Some(batch.max(1));
        self
    }

    /// Polls in flight at once while the queue keeps them full.
    pub fn claimers(mut self, claimers: usize) -> Self {
        self.claimers = claimers.max(1);
        self
    }

    /// Claim lease.
    pub fn lease(mut self, lease: Duration) -> Self {
        self.lease = lease;
        self
    }

    /// Empty-poll and error sleep.
    pub fn poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// Default backoff base and cap.
    pub fn retry(mut self, base: Duration, max: Duration) -> Self {
        self.retry_base = base;
        self.retry_max = max;
        self
    }

    /// Wait in the scope's line, keeping a place this long (v1's
    /// default is a minute).
    pub fn wait(mut self, wait: Duration) -> Self {
        self.wait = Some(wait);
        self
    }

    /// Report each outcome with a claim of the next items.
    pub fn chain(mut self) -> Self {
        self.chain = true;
        self
    }

    /// Once stopping, let held items run at most this long.
    pub fn drain(mut self, deadline: Duration) -> Self {
        self.drain = Some(deadline);
        self
    }

    /// Release items still running at the drain deadline instead of
    /// failing them.
    pub fn release_drained(mut self) -> Self {
        self.release_drained = true;
        self
    }

    /// Receive limit changes and throttling.
    pub fn on_event(mut self, handler: impl Fn(WorkerEvent) + Send + Sync + 'static) -> Self {
        self.on_event = Some(Arc::new(handler));
        self
    }

    /// Default backoff for an attempt count: exponential in the base,
    /// capped at the max.
    fn backoff(&self, attempts: u32) -> Duration {
        let shift = attempts.min(10);
        let scaled = self.retry_base.saturating_mul(1 << shift);
        scaled.min(self.retry_max)
    }

    /// Refuse options no run could honor.
    fn check(&self) -> Result<(), FlowerError> {
        let refuse = |message: &str| {
            Err(FlowerError::Server {
                code: "invalid_options".to_string(),
                message: message.to_string(),
            })
        };
        if self.owner.is_empty() || self.owner.len() > MAX_OWNER {
            return refuse("the owner takes 1 to 1024 bytes");
        }
        if self
            .wait
            .is_some_and(|wait| !(MIN_WAIT..=MAX_WAIT).contains(&wait))
        {
            return refuse("a place in line lasts 2 ms to an hour");
        }
        if self.release_drained && self.drain.is_none() {
            return refuse("releasing drained items needs a drain deadline");
        }
        Ok(())
    }

    /// The limiter these options describe.
    fn limiter(&self) -> Result<Limiter, FlowerError> {
        match self.adaptive {
            None => Ok(Limiter::fixed(self.concurrency)),
            Some(bounds) => {
                let health = self
                    .health
                    .clone()
                    .unwrap_or_else(|| Arc::new(ProcessHealth::new()));
                Limiter::new(bounds, health).map_err(|message| FlowerError::Server {
                    code: "invalid_options".to_string(),
                    message,
                })
            }
        }
    }

    /// Report an event.
    fn emit(&self, event: WorkerEvent) {
        if let Some(handler) = &self.on_event {
            handler(event);
        }
    }
}

/// What one worker run reported.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WorkerSummary {
    /// Handlers that succeeded.
    pub completed: u64,
    /// Claims failed (handler retry or panic).
    pub failed: u64,
    /// Claims killed terminally.
    pub killed: u64,
    /// Claims lost mid-run (lease lapsed under the handler, a report
    /// refused, or a handler given up on at the drain).
    pub lost: u64,
    /// Claims whose handlers the drain deadline stopped, failed or
    /// released.
    pub drained: u64,
}

/// What a job can tell the worker running it.
#[derive(Clone)]
pub struct JobControl {
    shared: Arc<Shared>,
    state: Arc<AtomicU8>,
}

/// A job still counting toward the limit.
const BUSY: u8 = 0;
/// A job that said it mostly waits.
const IDLE: u8 = 1;
/// A job whose handler returned or was dropped.
const DONE: u8 = 2;

/// What jobs share with their worker.
#[derive(Default)]
struct Shared {
    /// Held jobs that said they mostly wait: they do not count toward
    /// the limit. Signed: a job ending between turning idle and being
    /// counted takes it below zero for that moment.
    idle: AtomicIsize,
    /// Throttles jobs asked for, not yet applied.
    throttles: Mutex<Vec<(Duration, String)>>,
    /// Wakes the worker when either changes.
    wake: Notify,
}

impl Shared {
    /// Held jobs idle now. A moment below zero reads as none, which
    /// counts every held job as busy: never more room than there is.
    fn idle_now(&self) -> usize {
        self.idle.load(Ordering::SeqCst).max(0) as usize
    }
}

impl JobControl {
    fn new(shared: Arc<Shared>) -> Self {
        JobControl {
            shared,
            state: Arc::new(AtomicU8::new(BUSY)),
        }
    }

    /// From now on this job mostly waits (on a child process or a
    /// remote service, say) and loads the process little: it stops
    /// counting toward the limit, so long jobs cannot keep the worker
    /// from taking short ones. Its lease still renews. Answers whether
    /// that changed anything: false once said, or once the job ended.
    pub fn idle(&self) -> bool {
        if !self.mark_idle() {
            return false;
        }
        self.count_idle();
        self.shared.wake.notify_one();
        true
    }

    /// The job's state turns idle, if it was busy.
    fn mark_idle(&self) -> bool {
        self.state
            .compare_exchange(BUSY, IDLE, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }

    /// The worker counts the job as idle. The job can end between
    /// [`JobControl::mark_idle`] and this (a clone in another task went
    /// idle as it finished), uncounting it first.
    fn count_idle(&self) {
        self.shared.idle.fetch_add(1, Ordering::SeqCst);
    }

    /// The service behind this job refused it for being asked too much:
    /// claim nothing for `pause`, and hold fewer items with an adaptive
    /// limit, as when Flower refuses calls for load.
    pub fn throttle(&self, pause: Duration, reason: impl Into<String>) {
        self.shared
            .throttles
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((pause, reason.into()));
        self.shared.wake.notify_one();
    }

    /// The job ended: it no longer counts as idle either.
    fn finish(&self) {
        if self.state.swap(DONE, Ordering::SeqCst) == IDLE {
            self.shared.idle.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

impl std::fmt::Debug for JobControl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JobControl")
            .field("state", &self.state.load(Ordering::SeqCst))
            .finish_non_exhaustive()
    }
}

/// How one task ended.
enum Done {
    /// The handler decided.
    Handled(Result<(), WorkError>),
    /// The claim lapsed under it: redelivery owns the work.
    Lapsed,
    /// The drain deadline stopped it.
    Drained,
}

/// One task's outcome for the supervisor.
struct TaskOutcome {
    /// Item ID.
    id: String,
    /// Claim token.
    token: String,
    /// Claims so far, for backoff.
    attempts: u32,
    /// How it ended.
    done: Done,
}

/// One poll's answer, with the room it held.
struct Polled {
    /// Items asked for (and room held while asking).
    take: usize,
    /// When it was sent: its leases cannot have started earlier.
    sent: Instant,
    /// What came back.
    result: Result<Vec<Claim>, FlowerError>,
}

/// How a report settles a claim.
#[derive(Clone, Copy, Debug)]
enum Settlement {
    Complete,
    Fail(u64),
    Kill,
    Release,
}

/// Which summary counter an accepted report counts toward.
#[derive(Clone, Copy, Debug)]
enum Counter {
    Completed,
    Failed,
    Killed,
    Drained,
}

/// One report's answer, with the room its chained claim held.
struct Reported {
    /// The counter an accepted report counts toward; `None` for a
    /// surplus handed back.
    counter: Option<Counter>,
    /// Whether the server accepted it (or it stayed undecided, which
    /// lease redelivery owns).
    accepted: bool,
    /// Items the chained claim asked for (0 without one).
    take: usize,
    /// When it was sent: chained leases cannot have started earlier.
    sent: Instant,
    /// What the chained claim took.
    claimed: Vec<Claim>,
}

/// A claim's parameters, owned, for futures that outlive the loop's
/// borrows.
#[derive(Clone)]
struct Ask {
    db: String,
    queue: String,
    scope: String,
    owner: String,
    limit: usize,
    lease_ms: u64,
    wait_ms: Option<u64>,
}

impl Ask {
    fn poll(&self) -> QueuePoll<'_> {
        QueuePoll {
            scope: &self.scope,
            owner: Some(&self.owner),
            limit: Some(self.limit),
            lease_ms: Some(self.lease_ms),
            wait_ms: self.wait_ms,
        }
    }
}

/// Run handlers over a queue until `stop` fires, then drain in-flight
/// items and return the summary.
///
/// Polls claim only what can start now. Once polls come back full (the
/// queue holds at least as much as asked), up to `claimers` run at
/// once, each holding its room until it answers; an empty or short
/// poll drops back to one poll at a time. An adaptive limit grows while
/// claims keep the worker full and the process has room, and shrinks
/// when the process falls behind; whenever the client's count of
/// refusals for load grows, claims pause for a second and an adaptive
/// limit halves. A permanent poll failure (see [`permanent`]: bad
/// queue names, impossible leases, revoked credentials) stops
/// claiming, drains in-flight items the same way and returns the
/// error; forwards, overload and transport failures sleep and retry.
///
/// With [`QueueWorkerOptions::wait`], a short claim keeps the worker's
/// place in the scope's line, and the worker then reads readiness
/// instead of claiming until it is its turn; with
/// [`QueueWorkerOptions::chain`], each report claims the next items in
/// the same commit. Handlers that need to tell the worker more (that
/// they mostly wait, or that their service refused them for load) run
/// under [`run_queue_worker_with`].
pub async fn run_queue_worker<C, F, Fut>(
    client: C,
    options: QueueWorkerOptions,
    stop: Arc<Notify>,
    handler: F,
) -> Result<WorkerSummary, FlowerError>
where
    C: QueueClient,
    F: Fn(Claim) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), WorkError>> + Send + 'static,
{
    run_queue_worker_with(client, options, stop, move |claim, _job| handler(claim)).await
}

/// [`run_queue_worker`] with handlers that also take the job's
/// [`JobControl`]: a job can stop counting toward the limit while it
/// mostly waits ([`JobControl::idle`]), or pause claims when the service
/// behind it refuses it for load ([`JobControl::throttle`]).
pub async fn run_queue_worker_with<C, F, Fut>(
    client: C,
    options: QueueWorkerOptions,
    stop: Arc<Notify>,
    handler: F,
) -> Result<WorkerSummary, FlowerError>
where
    C: QueueClient,
    F: Fn(Claim, JobControl) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), WorkError>> + Send + 'static,
{
    options.check()?;
    let limiter = options.limiter()?;
    let (_, most) = limiter.bounds();
    let claimers = options.claimers.clamp(1, most);
    let mut ticks = tokio::time::interval_at(
        Instant::now() + options.adjust_every,
        options.adjust_every.max(Duration::from_millis(1)),
    );
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut run = Run {
        handler: Arc::new(handler),
        overloads: client.overloads(),
        client,
        options,
        limiter,
        tasks: JoinSet::new(),
        polls: FuturesUnordered::new(),
        probes: FuturesUnordered::new(),
        reports: FuturesUnordered::new(),
        reserved: 0,
        full: false,
        idle_until: None,
        in_line: false,
        refresh_at: None,
        admitted: false,
        shared: Arc::new(Shared::default()),
        drain: CancellationToken::new(),
        stopping: false,
        summary: WorkerSummary::default(),
    };
    let mut fatal = None;
    loop {
        // Pauses jobs asked for apply before their room is reused, and
        // finished tasks are reaped before claiming more.
        run.control();
        while let Some(outcome) = run.tasks.try_join_next() {
            run.settle(outcome.map_err(task_failed)?);
        }
        // Claim only what can start now: every poll spends an attempt
        // on each claimed item, and releasing a surplus never refunds
        // it, so over-claiming would push backlog heads toward the
        // dead letter file without ever running them.
        // A place in line due for keeping cuts an idle wait short: the
        // poll interval may outlast half the place.
        let now = Instant::now();
        // A full worker claims nothing, so has no place to keep.
        let refresh = run.refresh_at.filter(|_| run.in_line && run.room() > 0);
        let refresh_due = refresh.is_some_and(|at| now >= at);
        let idle = run.idle_until.is_some_and(|until| now < until) && !refresh_due;
        let paused = run.limiter.pause();
        if !idle && paused.is_none() {
            run.launch(claimers, now);
        }
        // Wake when an idle wait, a pause or a place's refresh ends, if
        // nothing else will.
        let wake = match (run.idle_until, paused) {
            (idle, Some(pause)) => Some(idle.map_or(pause, |idle| idle.max(pause))),
            (idle, None) => match (idle, refresh) {
                (Some(idle), Some(refresh)) => Some(idle.min(refresh)),
                (idle, refresh) => idle.or(refresh),
            },
        };
        let quiet = run.polls.is_empty() && run.probes.is_empty();
        let event = tokio::select! {
            biased;
            _ = stop.notified() => Wake::Stop,
            Some(outcome) = run.tasks.join_next(), if !run.tasks.is_empty() => Wake::Reaped(outcome),
            Some(reported) = run.reports.next(), if !run.reports.is_empty() => Wake::Reported(reported),
            Some(polled) = run.polls.next(), if !run.polls.is_empty() => Wake::Polled(polled),
            Some(probed) = run.probes.next(), if !run.probes.is_empty() => Wake::Probed(probed),
            () = run.shared.wake.notified() => Wake::Control,
            _ = ticks.tick() => Wake::Tick,
            () = sleep_until(wake), if wake.is_some() && quiet => Wake::Ready,
        };
        match event {
            Wake::Stop => break,
            Wake::Reaped(outcome) => run.settle(outcome.map_err(task_failed)?),
            Wake::Reported(reported) => run.reported(reported),
            Wake::Control => run.control(),
            Wake::Tick => run.tick(),
            Wake::Ready => run.idle_until = None,
            Wake::Probed(probed) => run.probed(probed),
            Wake::Polled(Polled { take, sent, result }) => {
                run.reserved -= take;
                match result {
                    Ok(claimed) => run.claimed(claimed, take, sent),
                    Err(error) if permanent(&error) => {
                        fatal = Some(error);
                        break;
                    }
                    Err(_) => {
                        run.full = false;
                        let retry = Instant::now() + run.options.poll_interval;
                        run.idle_until = Some(retry);
                        // A failed refresh tries again after the
                        // interval, not at once.
                        if let Some(at) = run.refresh_at.as_mut() {
                            *at = (*at).max(retry);
                        }
                    }
                }
            }
        }
    }
    run.stop(fatal.is_none()).await?;
    match fatal {
        Some(error) => Err(error),
        None => Ok(run.summary),
    }
}

/// What woke the loop.
enum Wake {
    Stop,
    Reaped(Result<TaskOutcome, tokio::task::JoinError>),
    Reported(Reported),
    Polled(Polled),
    Probed(Result<QueueReadiness, FlowerError>),
    Control,
    Tick,
    Ready,
}

/// What woke the drain.
enum Draining {
    Reaped(Result<TaskOutcome, tokio::task::JoinError>),
    Reported(Reported),
    Polled(Polled),
    Deadline,
    Abandon,
}

/// One worker run's state.
struct Run<C, F> {
    client: C,
    options: QueueWorkerOptions,
    handler: Arc<F>,
    limiter: Limiter,
    tasks: JoinSet<TaskOutcome>,
    polls: FuturesUnordered<BoxFuture<'static, Polled>>,
    probes: FuturesUnordered<BoxFuture<'static, Result<QueueReadiness, FlowerError>>>,
    reports: FuturesUnordered<BoxFuture<'static, Reported>>,
    /// Room held by polls and chained reports in flight.
    reserved: usize,
    /// Whether the last claim came back full: more claimers may run.
    full: bool,
    /// No new poll before this (an empty or failed poll waits).
    idle_until: Option<Instant>,
    /// Whether the worker holds a place in the scope's line.
    in_line: bool,
    /// When to claim again to keep that place.
    refresh_at: Option<Instant>,
    /// Readiness said it is this worker's turn: claim once.
    admitted: bool,
    /// What jobs share with the worker.
    shared: Arc<Shared>,
    /// Cancelled at the drain deadline.
    drain: CancellationToken,
    /// Claims nothing more: the loop stopped.
    stopping: bool,
    /// The client's refusals for load seen so far.
    overloads: u64,
    summary: WorkerSummary,
}

impl<C, F, Fut> Run<C, F>
where
    C: QueueClient,
    F: Fn(Claim, JobControl) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), WorkError>> + Send + 'static,
{
    /// Items counting toward the limit: held ones not idle.
    fn busy(&self) -> usize {
        self.tasks.len().saturating_sub(self.shared.idle_now())
    }

    /// Room for more items now.
    fn room(&self) -> usize {
        self.limiter
            .limit()
            .saturating_sub(self.busy() + self.reserved)
    }

    /// A claim of `limit` items with this worker's lease, owner and
    /// place in line.
    fn ask(&self, limit: usize) -> Ask {
        Ask {
            db: self.options.db.clone(),
            queue: self.options.queue.clone(),
            scope: self.options.scope.clone(),
            owner: self.options.owner.clone(),
            limit,
            lease_ms: millis(self.options.lease),
            wait_ms: self.options.wait.map(millis),
        }
    }

    /// Send polls while there is room: one at a time unless the last
    /// came back full. In line, read readiness first, and claim once it
    /// is this worker's turn or its place needs keeping.
    fn launch(&mut self, claimers: usize, now: Instant) {
        let allowed = if self.full { claimers } else { 1 };
        while self.polls.len() < allowed {
            let room = self.room();
            if room == 0 {
                break;
            }
            let refresh = self.refresh_at.is_some_and(|at| now >= at);
            if self.in_line && !self.admitted && !refresh {
                if self.probes.is_empty() && self.polls.is_empty() {
                    let (client, ask) = (self.client.clone(), self.ask(0));
                    self.probes.push(
                        async move {
                            client
                                .readiness(&ask.db, &ask.queue, &ask.scope, &ask.owner)
                                .await
                        }
                        .boxed(),
                    );
                }
                break;
            }
            self.admitted = false;
            let take = self.options.batch.unwrap_or(room).min(room);
            self.reserved += take;
            let (client, ask) = (self.client.clone(), self.ask(take));
            self.polls.push(
                async move {
                    // Lease time counts from before the request: the
                    // server's lease can only start later than this.
                    let sent = Instant::now();
                    let result = client.claim(&ask.db, &ask.queue, &ask.poll()).await;
                    Polled { take, sent, result }
                }
                .boxed(),
            );
        }
    }

    /// A readiness read answered: claim on this worker's turn, or read
    /// again after the poll interval.
    fn probed(&mut self, probed: Result<QueueReadiness, FlowerError>) {
        match probed {
            Ok(readiness) if readiness.ready => self.admitted = true,
            _ => self.idle_until = Some(Instant::now() + self.options.poll_interval),
        }
    }

    /// Items claimed, by a poll or a chained report asking for `take`:
    /// start them, and note whether the queue kept up.
    fn claimed(&mut self, claimed: Vec<Claim>, take: usize, sent: Instant) {
        self.full = claimed.len() >= take;
        if self.full {
            self.in_line = false;
            self.refresh_at = None;
        } else if let Some(wait) = self.options.wait {
            // A short claim keeps this worker's place in line.
            self.in_line = true;
            self.refresh_at = Some(sent + refresh_after(wait));
        }
        if claimed.is_empty() {
            self.idle_until = Some(Instant::now() + self.options.poll_interval);
            return;
        }
        for claim in claimed {
            if self.stopping || self.busy() >= self.limiter.limit() {
                // Only a server answering past the requested limit, a
                // limit cut while the claim was out, or a stop gets
                // here: hand the item back.
                self.hand_back(claim);
                continue;
            }
            self.start(claim, sent);
        }
        if self.full && self.busy() + self.reserved >= self.limiter.limit() {
            self.limiter.want();
        }
    }

    /// Run one claimed item's handler under lease renewal.
    fn start(&mut self, claim: Claim, sent: Instant) {
        let task = ClaimTask {
            client: self.client.clone(),
            db: self.options.db.clone(),
            queue: self.options.queue.clone(),
            lease: self.options.lease,
            claimed_at: sent,
        };
        let handler = self.handler.clone();
        let job = JobControl::new(self.shared.clone());
        let drain = self.drain.clone();
        self.tasks.spawn(async move {
            let id = claim.id.clone();
            let token = claim.token.clone();
            let attempts = claim.attempts;
            let done = task
                .run(&id, &token, handler, claim, job.clone(), drain)
                .await;
            job.finish();
            TaskOutcome {
                id,
                token,
                attempts,
                done,
            }
        });
    }

    /// Release a claim this worker will not run.
    fn hand_back(&mut self, claim: Claim) {
        let client = self.client.clone();
        let (db, queue) = (self.options.db.clone(), self.options.queue.clone());
        self.reports.push(
            async move {
                let sent = Instant::now();
                let accepted = settle(
                    &client,
                    &db,
                    &queue,
                    &claim.id,
                    &claim.token,
                    Settlement::Release,
                )
                .await;
                Reported {
                    counter: None,
                    accepted,
                    take: 0,
                    sent,
                    claimed: Vec::new(),
                }
            }
            .boxed(),
        );
    }

    /// Report a finished task, chained to a claim of the next items
    /// when the worker chains and has room.
    fn settle(&mut self, outcome: TaskOutcome) {
        // A pause the job asked for applies before its report decides
        // whether to chain.
        self.control();
        let (settlement, counter) = match outcome.done {
            Done::Lapsed => {
                self.summary.lost += 1;
                return;
            }
            Done::Handled(Ok(())) => (Settlement::Complete, Counter::Completed),
            Done::Handled(Err(WorkError::Retry(delay))) => {
                let delay = delay.unwrap_or_else(|| self.options.backoff(outcome.attempts));
                (Settlement::Fail(millis(delay)), Counter::Failed)
            }
            Done::Handled(Err(WorkError::Terminal(_))) => (Settlement::Kill, Counter::Killed),
            Done::Drained if self.options.release_drained => {
                (Settlement::Release, Counter::Drained)
            }
            Done::Drained => {
                let delay = self.options.backoff(outcome.attempts);
                (Settlement::Fail(millis(delay)), Counter::Drained)
            }
        };
        // This item's room passes to the items its report claims; polls
        // keep out of it meanwhile.
        let chains = self.options.chain
            && !self.stopping
            && self.limiter.pause().is_none()
            && !matches!(settlement, Settlement::Release);
        let take = if chains {
            let room = self.room();
            self.options.batch.unwrap_or(room).min(room)
        } else {
            0
        };
        self.reserved += take;
        let next = (take > 0).then(|| self.ask(take));
        let client = self.client.clone();
        let (db, queue) = (self.options.db.clone(), self.options.queue.clone());
        self.reports.push(
            async move {
                let sent = Instant::now();
                let (accepted, claimed) = match &next {
                    Some(next) => {
                        chained(&client, &outcome.id, &outcome.token, settlement, next).await
                    }
                    None => (
                        settle(
                            &client,
                            &db,
                            &queue,
                            &outcome.id,
                            &outcome.token,
                            settlement,
                        )
                        .await,
                        Vec::new(),
                    ),
                };
                Reported {
                    counter: Some(counter),
                    accepted,
                    take,
                    sent,
                    claimed,
                }
            }
            .boxed(),
        );
    }

    /// A report answered: count it, and start what its chained claim
    /// took.
    fn reported(&mut self, reported: Reported) {
        self.reserved -= reported.take;
        if let Some(counter) = reported.counter {
            if reported.accepted {
                *match counter {
                    Counter::Completed => &mut self.summary.completed,
                    Counter::Failed => &mut self.summary.failed,
                    Counter::Killed => &mut self.summary.killed,
                    Counter::Drained => &mut self.summary.drained,
                } += 1;
            } else {
                self.summary.lost += 1;
            }
        }
        if reported.take > 0 {
            self.claimed(reported.claimed, reported.take, reported.sent);
        } else {
            for claim in reported.claimed {
                self.hand_back(claim);
            }
        }
    }

    /// Jobs went idle or asked for a pause.
    fn control(&mut self) {
        let throttles = std::mem::take(
            &mut *self
                .shared
                .throttles
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
        );
        for (pause, reason) in throttles {
            self.options.emit(WorkerEvent::Throttled { pause });
            if let Some(change) = self.limiter.throttle(pause, &reason) {
                self.options.emit(WorkerEvent::Limit(change));
            }
        }
    }

    /// The adjustment interval passed.
    fn tick(&mut self) {
        // Flower refusing calls for load (this worker's, reports
        // included) is backpressure: claim nothing for a moment, and
        // hold fewer items.
        let refused = self.client.overloads();
        if refused > self.overloads {
            self.overloads = refused;
            self.options.emit(WorkerEvent::Throttled {
                pause: OVERLOAD_PAUSE,
            });
            if let Some(change) = self.limiter.throttle(OVERLOAD_PAUSE, OVERLOAD_REASON) {
                self.options.emit(WorkerEvent::Limit(change));
            }
        }
        // A full pool claims nothing, so its claims cannot show that
        // work waits; the last full claim does.
        if self.full && self.busy() + self.reserved >= self.limiter.limit() {
            self.limiter.want();
        }
        if let Some(change) = self.limiter.adjust() {
            self.options.emit(WorkerEvent::Limit(change));
        }
    }

    /// Stop claiming and drain: held items report normally until the
    /// drain deadline, if any, stops them, and polls and reports still
    /// out answer, whatever they claimed handed back. Then give up the
    /// place in line, which any of them could have taken again: always,
    /// since a claim whose answer was lost may hold one unseen.
    async fn stop(&mut self, leave: bool) -> Result<(), FlowerError> {
        self.stopping = true;
        self.probes = FuturesUnordered::new();
        self.drain_held().await?;
        if leave && self.options.wait.is_some() {
            // Give up the place at once, rather than when it runs out:
            // an empty claim with no wait leaves the line.
            let mut ask = self.ask(0);
            ask.wait_ms = Some(0);
            let poll = ask.poll();
            let leaving = self.client.claim(&ask.db, &ask.queue, &poll);
            let _ = tokio::time::timeout(LEAVE_TIMEOUT, leaving).await;
            self.in_line = false;
        }
        Ok(())
    }

    /// Let held items, polls and reports out finish, stopping handlers
    /// at the drain deadline and giving up on ones stuck past it.
    /// Whatever a poll or chained report still claims is handed back.
    async fn drain_held(&mut self) -> Result<(), FlowerError> {
        let deadline = self.options.drain.map(|drain| Instant::now() + drain);
        let mut abandon_at = None;
        loop {
            while let Some(outcome) = self.tasks.try_join_next() {
                self.settle(outcome.map_err(task_failed)?);
            }
            if self.tasks.is_empty() && self.reports.is_empty() && self.polls.is_empty() {
                return Ok(());
            }
            let cancelling = deadline.filter(|_| !self.drain.is_cancelled());
            let abandoning = abandon_at.filter(|_| !self.tasks.is_empty());
            let event = tokio::select! {
                biased;
                Some(outcome) = self.tasks.join_next(), if !self.tasks.is_empty() => {
                    Draining::Reaped(outcome)
                }
                Some(reported) = self.reports.next(), if !self.reports.is_empty() => {
                    Draining::Reported(reported)
                }
                Some(polled) = self.polls.next(), if !self.polls.is_empty() => {
                    Draining::Polled(polled)
                }
                () = sleep_until(cancelling), if cancelling.is_some() => Draining::Deadline,
                () = sleep_until(abandoning), if abandoning.is_some() => Draining::Abandon,
            };
            match event {
                Draining::Reaped(outcome) => self.settle(outcome.map_err(task_failed)?),
                Draining::Reported(reported) => self.reported(reported),
                Draining::Polled(Polled { take, sent, result }) => {
                    self.reserved -= take;
                    // Stopping, the claimed items go straight back.
                    if let Ok(claimed) = result {
                        self.claimed(claimed, take, sent);
                    }
                }
                Draining::Deadline => {
                    // Handlers still running are dropped at their next
                    // await point and their claims failed or released.
                    self.drain.cancel();
                    abandon_at = Some(Instant::now() + ABANDON_AFTER);
                }
                Draining::Abandon => {
                    // Stuck outside an await point: give up on them. They
                    // report nothing, and redeliver when their leases lapse.
                    self.summary.lost += self.tasks.len() as u64;
                    self.tasks.detach_all();
                }
            }
        }
    }
}

/// When a worker waiting in line claims again to keep its place: by
/// half of the wait, at a random time from 3/8 of it on, drawn anew
/// each time, so pools that started together do not claim together for
/// good (v1's `refreshAfter`).
fn refresh_after(wait: Duration) -> Duration {
    let earliest = wait * 3 / 8;
    let span = millis(wait / 2 - earliest);
    let draw = RandomState::new().hash_one(span) % (span + 1);
    earliest + Duration::from_millis(draw)
}

/// Sleep until `at`, or forever without one.
async fn sleep_until(at: Option<Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

/// Whether a failure is one the same request would meet again:
/// decided business failures (but not the refusals that end on their
/// own, such as `queue_purging`), auth refusals, statuses the server
/// marks non-retryable and undecodable answers. Retrying these spins
/// forever (a revoked worker token would poll unauthorized once a
/// second for good).
fn permanent(error: &FlowerError) -> bool {
    !error.is_transient()
        && matches!(
            error,
            FlowerError::Business { .. }
                | FlowerError::Http(_)
                | FlowerError::RetryFailed { .. }
                | FlowerError::Unsupported(_)
                | FlowerError::Auth(_)
                | FlowerError::Server { .. }
                | FlowerError::Decode(_)
        )
}

/// Saturating milliseconds of a duration.
fn millis(duration: Duration) -> u64 {
    duration.as_millis().min(u64::MAX as u128) as u64
}

/// Map a task that failed outside the handler's panic guard.
fn task_failed(error: tokio::task::JoinError) -> FlowerError {
    FlowerError::Server {
        code: "worker".to_string(),
        message: format!("worker task failed: {error}"),
    }
}

/// What one claimed item's task needs besides its handler.
struct ClaimTask<C> {
    client: C,
    db: String,
    queue: String,
    lease: Duration,
    /// When the claiming poll was sent: the lease cannot have
    /// started earlier.
    claimed_at: Instant,
}

impl<C: QueueClient> ClaimTask<C> {
    /// Run one handler while renewing its lease concurrently, so a
    /// slow heartbeat never stalls the handler and a finished handler
    /// never waits on a heartbeat. The claim lapses when a renewal is
    /// refused (the claim settled or expired elsewhere) or no renewal
    /// landed within a lease; the drain deadline stops it. Either way
    /// the handler is dropped and its result discarded. Panics, while
    /// building the handler future or inside it, fail the claim with
    /// backoff.
    async fn run<F, Fut>(
        &self,
        id: &str,
        token: &str,
        handler: Arc<F>,
        claim: Claim,
        job: JobControl,
        drain: CancellationToken,
    ) -> Done
    where
        F: Fn(Claim, JobControl) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), WorkError>> + Send + 'static,
    {
        let Ok(future) = std::panic::catch_unwind(AssertUnwindSafe(|| handler(claim, job))) else {
            return Done::Handled(Err(WorkError::Retry(None)));
        };
        let run = AssertUnwindSafe(future).catch_unwind();
        tokio::select! {
            // A finished handler wins over the drain and a due renewal.
            biased;
            outcome = run => Done::Handled(outcome.unwrap_or(Err(WorkError::Retry(None)))),
            () = drain.cancelled() => Done::Drained,
            () = self.renew(id, token) => Done::Lapsed,
        }
    }

    /// Renew the lease every third of its length; return once the
    /// claim is known lost.
    async fn renew(&self, id: &str, token: &str) {
        // Never a zero period, which `interval` rejects.
        let every = (self.lease / 3).max(Duration::from_millis(1));
        let mut renewed = self.claimed_at;
        let mut ticker = tokio::time::interval_at(renewed + every, every);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            let sent = Instant::now();
            // A renewal still in flight when the lease runs out is
            // too late either way.
            let left = (renewed + self.lease).saturating_duration_since(sent);
            let heartbeat =
                self.client
                    .heartbeat(&self.db, &self.queue, id, token, millis(self.lease));
            match tokio::time::timeout(left, heartbeat).await {
                Ok(Ok(())) => renewed = sent,
                // The claim lapsed or died elsewhere.
                Ok(Err(FlowerError::Business { .. })) | Err(_) => return,
                // Transient failures retry next tick until the lease
                // could have run out.
                Ok(Err(_)) if renewed + self.lease <= Instant::now() => return,
                Ok(Err(_)) => {}
            }
        }
    }
}

/// One report through the client's single call for it.
async fn single<C: QueueClient>(
    client: &C,
    db: &str,
    queue: &str,
    id: &str,
    token: &str,
    settlement: Settlement,
) -> Result<(), FlowerError> {
    match settlement {
        Settlement::Complete => client.complete(db, queue, id, token).await,
        Settlement::Fail(delay_ms) => client.fail(db, queue, id, token, delay_ms).await,
        Settlement::Kill => client.kill(db, queue, id, token).await,
        Settlement::Release => client.release(db, queue, id, token).await,
    }
}

/// Best-effort report: one attempt, one retry after a second on
/// transient failure, then lease redelivery owns the outcome.
/// Returns false only when the server refused the report, by
/// business verdict or by auth.
async fn settle<C: QueueClient>(
    client: &C,
    db: &str,
    queue: &str,
    id: &str,
    token: &str,
    settlement: Settlement,
) -> bool {
    match single(client, db, queue, id, token, settlement).await {
        Ok(()) => true,
        Err(error) if permanent(&error) => false,
        Err(_) => {
            tokio::time::sleep(Duration::from_secs(1)).await;
            !matches!(
                single(client, db, queue, id, token, settlement).await,
                Err(error) if permanent(&error)
            )
        }
    }
}

/// A report chained to the next claim. Its retry reports alone: the
/// first attempt may have committed, claim and all, and a claim it took
/// that never answered lapses and redelivers.
async fn chained<C: QueueClient>(
    client: &C,
    id: &str,
    token: &str,
    settlement: Settlement,
    next: &Ask,
) -> (bool, Vec<Claim>) {
    let report = match settlement {
        Settlement::Complete => QueueReport::Complete(None),
        Settlement::Fail(delay_ms) => QueueReport::Fail(delay_ms),
        Settlement::Kill => QueueReport::Kill,
        Settlement::Release => {
            let accepted = settle(client, &next.db, &next.queue, id, token, settlement).await;
            return (accepted, Vec::new());
        }
    };
    match client
        .report(&next.db, &next.queue, id, token, report, &next.poll())
        .await
    {
        Ok(claimed) => (true, claimed),
        Err(error) if permanent(&error) => (false, Vec::new()),
        Err(_) => {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let retried = single(client, &next.db, &next.queue, id, token, settlement).await;
            (
                !matches!(retried, Err(error) if permanent(&error)),
                Vec::new(),
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_caps_exponentially() {
        let options = QueueWorkerOptions::new("db", "jobs");
        assert_eq!(options.backoff(0), Duration::from_secs(1));
        assert_eq!(options.backoff(1), Duration::from_secs(2));
        assert_eq!(options.backoff(5), Duration::from_secs(32));
        assert_eq!(options.backoff(100), Duration::from_secs(60));
    }

    #[test]
    fn renew_interval_divides_lease() {
        let options = QueueWorkerOptions::new("db", "jobs").lease(Duration::from_secs(30));
        let every = options.lease.checked_div(3).unwrap();
        assert_eq!(every, Duration::from_secs(10));
    }

    #[test]
    fn invalid_adaptive_bounds_refuse_to_start() {
        let options = QueueWorkerOptions::new("db", "jobs").adaptive(4, 2, 8);
        assert!(options.limiter().is_err());
        let options = QueueWorkerOptions::new("db", "jobs").adaptive(1, 2, 8);
        assert_eq!(options.limiter().unwrap().limit(), 2);
    }

    #[test]
    fn impossible_options_refuse_to_start() {
        let options = QueueWorkerOptions::new("db", "jobs");
        assert!(options.check().is_ok());
        assert!(options.clone().owner("").check().is_err());
        assert!(options.clone().owner("x".repeat(1025)).check().is_err());
        assert!(
            options
                .clone()
                .wait(Duration::from_millis(1))
                .check()
                .is_err()
        );
        assert!(
            options
                .clone()
                .wait(Duration::from_secs(3601))
                .check()
                .is_err()
        );
        assert!(
            options
                .clone()
                .wait(Duration::from_secs(60))
                .check()
                .is_ok()
        );
        assert!(options.clone().release_drained().check().is_err());
        let draining = options.drain(Duration::from_secs(5)).release_drained();
        assert!(draining.check().is_ok());
    }

    #[test]
    fn owners_default_to_unique_workers() {
        let first = QueueWorkerOptions::new("db", "jobs").owner;
        let second = QueueWorkerOptions::new("db", "jobs").owner;
        assert!(first.starts_with("worker-"), "{first}");
        assert_ne!(first, second);
    }

    #[test]
    fn places_refresh_between_three_eighths_and_half_the_wait() {
        let wait = Duration::from_secs(60);
        let draws: Vec<Duration> = (0..200).map(|_| refresh_after(wait)).collect();
        assert!(
            draws.iter().all(
                |draw| (Duration::from_millis(22_500)..=Duration::from_secs(30)).contains(draw)
            )
        );
        // Drawn anew each time.
        assert!(draws.iter().any(|draw| *draw != draws[0]));
        // The shortest wait still refreshes before it runs out.
        assert!(refresh_after(MIN_WAIT) < MIN_WAIT);
    }

    #[test]
    fn a_job_ending_while_it_goes_idle_never_counts_below_zero() {
        let shared = Arc::new(Shared::default());
        let job = JobControl::new(shared.clone());
        let clone = job.clone();
        // A clone turns the job idle; before it counts it, the job ends.
        assert!(clone.mark_idle());
        job.finish();
        assert_eq!(shared.idle_now(), 0);
        clone.count_idle();
        assert_eq!(shared.idle_now(), 0);
        // An ordinary idle job counts once, and not after it ends.
        let other = JobControl::new(shared.clone());
        assert!(other.idle());
        assert_eq!(shared.idle_now(), 1);
        other.finish();
        assert_eq!(shared.idle_now(), 0);
        assert!(!other.idle());
    }
}
