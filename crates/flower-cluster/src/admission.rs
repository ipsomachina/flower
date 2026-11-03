//! Node-wide admission: which engine work may start, and when.
//!
//! Client entry points admit before they pin a snapshot or take the
//! writer lane, so a waiter holds only its bytes and a queue entry,
//! never a snapshot or the engine. Four classes:
//!
//! - [`Class::Query`]: linearizable queries, evaluated in parallel on
//!   the reader. They share the evaluation pool with refreshes: at most
//!   [`AdmissionConfig::query_workers`] slots, and no more than the
//!   memory reserved for evaluation holds
//!   ([`AdmissionConfig::evaluation_memory_bytes`] per slot within
//!   [`AdmissionConfig::evaluation_memory_budget`]).
//! - [`Class::Refresh`]: watch re-evaluations. A waiting query goes
//!   first, but while both wait refreshes take one turn in four
//!   ([`AdmissionConfig::refresh_share_percent`]), up to that share of
//!   the slots: neither starves. Refreshes are never refused: the hub
//!   retains their bytes anyway, and they wait as long as it takes.
//! - [`Class::Writer`]: mutations, transactions, queue and timer
//!   operations, from entering the writer's queue until answered (see
//!   [`crate::writer`]). The class has [`WRITER_QUEUE`] places of its
//!   own outside the evaluation pool: writers never queue behind reads,
//!   a group commit takes everything queued, and floods wait here,
//!   bounded, instead of in the writer. Their bytes count against the
//!   queries' budget.
//! - [`Class::Control`]: deployments, which compile and backfill for
//!   seconds. Own slots and byte budget that user traffic cannot take.
//!
//! Raft apply, snapshot builds and maintenance never pass admission:
//! they are the node's own work and are never refused. The evaluation
//! pool's bound is what keeps blocking threads and cores free for them.
//!
//! Within a class, waiters queue FIFO per database lane, and lanes are
//! served by the slot time they held (start-time fair queueing, as v1):
//! the waiting lane that has held slots least goes next, a lane's time
//! counts when its slot is released, and a lane that went idle returns
//! at the class's current virtual time, with neither credit nor debt.
//! One database that runs long evaluations cannot starve the others,
//! however many requests it queues.
//!
//! Overload is explicit and retryable ([`Overload`]: 503 with
//! `Retry-After`):
//!
//! - bytes: a request whose bytes do not fit its class budget fails at
//!   once. Request bodies lease their bytes before they are read
//!   ([`Admission::lease`]), so an oversubscribed node refuses a body
//!   instead of buffering it.
//! - share: once its class holds more than half the budget (its own
//!   queued and running requests and body leases, as v1 counts a
//!   class's input), a query or writer that would queue is refused at
//!   once when its lane already has more than an even share of the
//!   class's waiters (v1's rule): one noisy database cannot fill the
//!   bytes every other database needs to queue at all. A lane with
//!   nobody waiting, or the only lane waiting, is never refused for its
//!   share, nor a class for bytes the others hold.
//! - waiting: a query or writer waiting past
//!   [`AdmissionConfig::max_wait`], or past its client deadline (see
//!   [`crate::deadline`]), fails then.
//! - queue delay: a CoDel controller per class watches how long
//!   queries and writers waited. Once every wait in an interval
//!   ([`AdmissionConfig::codel_interval`]) exceeded the target
//!   ([`AdmissionConfig::codel_target`]), the queue stands: requests
//!   with a client deadline that waited past twice the target are shed
//!   at their turn, and new ones are refused at arrival when their
//!   lane's oldest waiter already did, until an interval passes in
//!   which some request waited no more than the target and none was
//!   shed. Requests without a deadline are never shed early: they wait
//!   at most the wait bound for their slot.
//!
//! A writer waits twice: for a place, then in the writer's queue for
//! its turn to prepare. Its wait counts from admission to that turn,
//! where CoDel measures and sheds it and its deadline is checked again
//! ([`Permit::turn`]): one controller, one decision per write. Places
//! are granted by lane as above once all are taken; the writer's queue
//! itself is FIFO, so a writer arriving at a standing writer queue
//! expects the last turn's wait whatever its lane.

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use flower_sdk::Value;
use tokio::sync::oneshot;
use tokio::time::Instant;

use crate::stages::{self, Stage};

/// Fixed bytes charged per request on top of its decoded input.
pub const REQUEST_OVERHEAD: usize = 512;
/// Writes the writer's queue holds at once, preparing or committing:
/// four of its largest groups.
pub const WRITER_QUEUE: usize = 4 * crate::writer::GROUP_ITEMS;
/// Retry hint every overload carries.
pub const RETRY_AFTER: Duration = Duration::from_secs(1);
/// Memory reserved per evaluation slot by default: a guest instance's
/// linear-memory limit plus as much again for the host, as v1.
pub const DEFAULT_EVALUATION_MEMORY: usize = 256 * 1024 * 1024;
/// Why a request was refused for bytes.
const BYTES_EXHAUSTED: &str = "node queued-input byte budget exhausted; retry later";
/// Why a request was refused for waiting.
const WAITED_TOO_LONG: &str = "request waited for admission as long as it may; retry later";
/// Why a request was refused at its client deadline.
const WAITED_UNTIL_DEADLINE: &str =
    "request waited for admission until its client deadline; retry later";
/// Why a request was refused at arrival behind a standing queue.
const SHED_AT_ARRIVAL: &str = "admission queue is overloaded; retry later";
/// Why a request was refused at its turn behind a standing queue.
const SHED_AT_TURN: &str = "request queued for admission past the overload target; retry later";
/// Why a request was refused for its lane's share of a filling budget.
const OVER_SHARE: &str = "database holds more than its share of the admission queue; retry later";

/// Why admission refused with `message`, as per-method costs count it
/// (see [`crate::methods`]); `None` for an overload admission did not
/// decide (a batch that kept going stale).
pub(crate) fn refusal(message: &str) -> Option<crate::methods::Refusal> {
    use crate::methods::Refusal;
    match message {
        SHED_AT_ARRIVAL => Some(Refusal::Overloaded),
        WAITED_UNTIL_DEADLINE => Some(Refusal::Deadline),
        SHED_AT_TURN => Some(Refusal::Shed),
        BYTES_EXHAUSTED | WAITED_TOO_LONG | OVER_SHARE => Some(Refusal::Full),
        _ => None,
    }
}

/// Who is asking: each class has its own slots or share and budget.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Class {
    /// A linearizable query.
    Query,
    /// A watch re-evaluation.
    Refresh,
    /// A write in the writer's queue.
    Writer,
    /// A deployment.
    Control,
}

impl Class {
    /// Every class, in index order.
    pub const ALL: [Class; 4] = [Class::Query, Class::Refresh, Class::Writer, Class::Control];

    fn index(self) -> usize {
        match self {
            Class::Query => 0,
            Class::Refresh => 1,
            Class::Writer => 2,
            Class::Control => 3,
        }
    }

    /// Whether the class waits at most [`AdmissionConfig::max_wait`],
    /// sheds under a standing queue and honors client deadlines.
    fn bounded(self) -> bool {
        matches!(self, Class::Query | Class::Writer)
    }
}

/// Admission sizing. The defaults follow v1: one evaluation slot per
/// core, 256 MiB of memory reserved per slot, 64 MiB of queued user
/// input, ten seconds of waiting, a 50 ms CoDel target over 500 ms
/// intervals.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdmissionConfig {
    /// Concurrent query and refresh evaluations.
    pub query_workers: usize,
    /// Concurrent deployments.
    pub control_workers: usize,
    /// Bytes queries, refreshes and writers may hold, queued or running.
    pub queued_input_bytes: usize,
    /// Bytes deployments may hold, queued or running.
    pub control_input_bytes: usize,
    /// Longest a query or writer waits for a slot.
    pub max_wait: Duration,
    /// Refreshes' minimum share of turns and slots while queries wait,
    /// in percent.
    pub refresh_share_percent: usize,
    /// The queue delay CoDel tolerates as a standing minimum.
    pub codel_target: Duration,
    /// How long the minimum must stand above the target to count as
    /// overload, and how often overload is decided again.
    pub codel_interval: Duration,
    /// Memory each running evaluation is reserved: a guest instance's
    /// linear memory plus the host's share.
    pub evaluation_memory_bytes: usize,
    /// Memory all running evaluations may be reserved together; `None`
    /// reserves for every query worker. The pool runs at most this
    /// many bytes' worth of slots.
    pub evaluation_memory_budget: Option<usize>,
}

impl Default for AdmissionConfig {
    fn default() -> Self {
        AdmissionConfig {
            query_workers: std::thread::available_parallelism().map_or(1, usize::from),
            control_workers: 1,
            queued_input_bytes: 64 * 1024 * 1024,
            // Two deployments at the server's 64 MiB body limit, each
            // charged its footprint (see `deploy_bytes`).
            control_input_bytes: 2 * deploy_bytes(64 * 1024 * 1024, 0),
            max_wait: Duration::from_secs(10),
            refresh_share_percent: 25,
            codel_target: Duration::from_millis(50),
            codel_interval: Duration::from_millis(500),
            evaluation_memory_bytes: DEFAULT_EVALUATION_MEMORY,
            evaluation_memory_budget: None,
        }
    }
}

impl AdmissionConfig {
    /// Reject sizings that would refuse or stall everything.
    pub fn validate(&self) -> Result<(), String> {
        if self.query_workers == 0 || self.control_workers == 0 {
            return Err("admission needs at least one query and one control worker".to_string());
        }
        if self.queued_input_bytes < REQUEST_OVERHEAD || self.control_input_bytes < REQUEST_OVERHEAD
        {
            return Err(format!(
                "admission byte budgets must hold one request ({REQUEST_OVERHEAD} bytes)"
            ));
        }
        if self.max_wait.is_zero() {
            return Err("the admission wait bound must be positive".to_string());
        }
        if self.refresh_share_percent == 0 || self.refresh_share_percent > 100 {
            return Err("the refresh share is a percentage from 1 to 100".to_string());
        }
        if self.codel_target.is_zero() || self.codel_interval.is_zero() {
            return Err("the admission delay target and interval must be positive".to_string());
        }
        if self.evaluation_memory_bytes == 0 {
            return Err("the memory reserved per evaluation must be positive".to_string());
        }
        if self
            .evaluation_memory_budget
            .is_some_and(|budget| budget < self.evaluation_memory_bytes)
        {
            return Err(
                "the evaluation memory budget must hold one evaluation's reservation".to_string(),
            );
        }
        Ok(())
    }

    /// Slots of a class's own; the user classes share the pool, which
    /// the memory budget may hold below the worker count.
    fn slots(&self, class: Class) -> usize {
        match class {
            Class::Query | Class::Refresh => {
                let by_memory = self.evaluation_memory_budget.map_or(usize::MAX, |budget| {
                    budget / self.evaluation_memory_bytes.max(1)
                });
                self.query_workers.min(by_memory).max(1)
            }
            // Places in the writer's queue, not evaluations: the writer
            // prepares one write at a time, outside the memory budget.
            Class::Writer => WRITER_QUEUE,
            Class::Control => self.control_workers.max(1),
        }
    }

    /// The byte budget a class draws on.
    fn budget(&self, class: Class) -> usize {
        match class {
            Class::Control => self.control_input_bytes,
            Class::Query | Class::Refresh | Class::Writer => self.queued_input_bytes,
        }
    }

    /// Slots refreshes may hold while queries wait.
    fn refresh_slots(&self) -> usize {
        let slots = self.slots(Class::Refresh);
        (slots * self.refresh_share_percent.min(100))
            .div_ceil(100)
            .clamp(1, slots)
    }
}

/// A retryable refusal: the node has no room for this request now.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Overload {
    /// The refused class.
    pub class: Class,
    /// Why.
    pub message: String,
    /// When retrying makes sense.
    pub retry_after: Duration,
}

impl Overload {
    fn new(class: Class, message: &str) -> Self {
        Overload {
            class,
            message: message.to_string(),
            retry_after: RETRY_AFTER,
        }
    }
}

/// Bytes a request holds while it waits and runs: its decoded input
/// plus fixed overhead, charged the way v1 charges queued input.
pub fn input_bytes(value: &Value) -> usize {
    value_bytes(value) + REQUEST_OVERHEAD
}

/// Approximate in-memory bytes of a decoded value.
fn value_bytes(value: &Value) -> usize {
    let own = std::mem::size_of::<Value>();
    match value {
        Value::Null | Value::Bool(_) | Value::Number(_) => own,
        Value::String(text) => own + text.len(),
        Value::Array(items) => own + items.iter().map(value_bytes).sum::<usize>(),
        Value::Object(fields) => {
            own + fields
                .iter()
                .map(|(key, value)| key.len() + 32 + value_bytes(value))
                .sum::<usize>()
        }
    }
}

/// Bytes a deployment holds: the module is copied into the request,
/// the log entry, the replicated write and the compiler (v1's
/// footprint), plus its manifest and fixed overhead.
pub fn deploy_bytes(wasm: usize, manifest: usize) -> usize {
    4 * wasm + wasm / 4 + manifest + 64 * 1024
}

/// Per-class admission counters and gauges.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct ClassMetrics {
    /// Running requests.
    pub active: usize,
    /// Waiting requests.
    pub queued: usize,
    /// Database lanes with waiters.
    pub lanes: usize,
    /// How long the oldest waiter has waited, in milliseconds.
    pub oldest_queued_ms: u64,
}

/// A snapshot of admission state, for metrics.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct AdmissionMetrics {
    /// Query, refresh, writer and control gauges, in that order.
    pub classes: Vec<(Class, ClassMetrics)>,
    /// Bytes held against the user budget, request bodies included.
    pub retained_input_bytes: usize,
    /// The user budget.
    pub input_budget_bytes: usize,
    /// Bytes held against the control budget.
    pub retained_control_bytes: usize,
    /// The control budget.
    pub control_budget_bytes: usize,
    /// Request-body bytes leased before decoding, of both budgets.
    pub ingress_bytes: usize,
    /// Evaluation slots queries and refreshes share.
    pub query_workers: usize,
    /// Memory reserved for the evaluations running now.
    pub reserved_evaluation_bytes: usize,
    /// Memory evaluations may be reserved together.
    pub evaluation_memory_budget_bytes: usize,
    /// Requests granted a slot.
    pub admitted: u64,
    /// Requests refused for bytes, request bodies included.
    pub rejected: u64,
    /// Requests refused for waiting past the wait bound.
    pub timed_out: u64,
    /// Waiters whose caller gave up.
    pub canceled: u64,
    /// Writes queued that settle work already done (a queue claim's
    /// renewal, completion, failure or release), which are never shed
    /// early (see [`Admission::acquire_settling`]).
    pub settling: u64,
    /// Requests shed: at arrival or at their turn behind a standing
    /// queue, or at their client deadline.
    pub shed: ShedMetrics,
    /// The CoDel controllers' settings and state.
    pub queueing: QueueingMetrics,
}

/// Requests shed, by when.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct ShedMetrics {
    /// Refused on arrival behind a standing queue.
    pub arrival: u64,
    /// Refused at their turn for having waited past the overload
    /// threshold.
    pub queued: u64,
    /// Refused when their client deadline passed while they waited.
    pub deadline: u64,
    /// Refused on arrival for their lane's share of a filling budget.
    pub share: u64,
}

/// The CoDel controllers' settings and state.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct QueueingMetrics {
    /// The tolerated standing queue delay, in milliseconds.
    pub target_ms: u64,
    /// The interval overload is decided over, in milliseconds.
    pub interval_ms: u64,
    /// The wait bound, in milliseconds.
    pub max_wait_ms: u64,
    /// Whether the query queue stands now.
    pub queries_overloaded: bool,
    /// Whether the writer queue stands now.
    pub writers_overloaded: bool,
}

/// Queue delay control, v1's modified CoDel: overload is decided once
/// per interval from the waits seen in the last one, and while it holds
/// every sheddable request that waited past twice the target is
/// refused. Unsheddable requests (no client deadline) are measured but
/// never refused, and a queue standing mostly with them is no overload.
#[derive(Debug)]
struct Codel {
    target: Duration,
    interval: Duration,
    /// When the current interval ends; `None` before any event.
    until: Option<Instant>,
    /// The shortest wait in the current interval.
    shortest: Option<Duration>,
    overloaded: bool,
    /// Whether the current interval refused someone.
    sloughed: bool,
    departed: u64,
    sheddable: u64,
}

impl Codel {
    fn new(target: Duration, interval: Duration) -> Self {
        Codel {
            target,
            interval,
            until: None,
            shortest: None,
            overloaded: false,
            sloughed: false,
            departed: 0,
            sheddable: 0,
        }
    }

    /// The wait past which a sheddable request is refused.
    fn slough(&self) -> Duration {
        2 * self.target
    }

    /// Decide overload again once the interval ended: it holds when
    /// the last interval refused someone, or its shortest wait stood
    /// above the target, or (no departures in it) the queue was stuck;
    /// and the queue is curable, mostly sheddable requests. An interval
    /// longer ago than one interval tells nothing.
    fn roll(&mut self, now: Instant, sheddable: bool, stuck: bool) {
        let Some(until) = self.until else {
            self.until = Some(now + self.interval);
            return;
        };
        if now < until {
            return;
        }
        let recent = now < until + self.interval;
        let curable = if recent && self.departed > 0 {
            self.sheddable * 2 >= self.departed
        } else {
            sheddable
        };
        let standing = match self.shortest {
            Some(shortest) if recent => shortest > self.target,
            _ => stuck,
        };
        self.overloaded = curable && ((recent && self.sloughed) || standing);
        self.shortest = None;
        self.sloughed = false;
        self.departed = 0;
        self.sheddable = 0;
        self.until = Some(now + self.interval);
    }

    /// A waiter leaves the queue after `waited`: whether to shed it. One
    /// `exempt` from shedding although `sheddable` (it settles work
    /// already done) counts toward the overload decision as any request
    /// with a client deadline does, and is never shed. Counted as unsheddable instead,
    /// such writes, when they made most of a queue's turns, kept it from
    /// ever counting as overloaded, and other databases' writes waited
    /// out their deadlines in it rather than being shed.
    fn depart(&mut self, now: Instant, waited: Duration, sheddable: bool, exempt: bool) -> bool {
        self.roll(now, sheddable, waited > self.interval);
        self.departed += 1;
        if sheddable {
            self.sheddable += 1;
        }
        self.shortest = Some(
            self.shortest
                .map_or(waited, |shortest| shortest.min(waited)),
        );
        let shed = sheddable && !exempt && self.overloaded && waited > self.slough();
        self.sloughed |= shed;
        shed
    }

    /// A sheddable request arrives expecting to wait `expected` behind
    /// a queue: whether to refuse it at once.
    fn refuse(&mut self, now: Instant, expected: Duration) -> bool {
        self.roll(now, true, expected > self.interval);
        let refuse = self.overloaded && expected > self.slough();
        self.sloughed |= refuse;
        refuse
    }
}

/// One queued request.
struct Waiter {
    id: u64,
    bytes: usize,
    queued_at: Instant,
    /// Whether the request has a client deadline: only those count as
    /// sheddable in CoDel's decisions, and only those are shed.
    sheddable: bool,
    /// Whether it settles work already done, and is never shed (see
    /// [`Admission::acquire_settling`]).
    settles: bool,
    /// The client deadline, which a writer's turn checks again.
    deadline: Option<Instant>,
    grant: oneshot::Sender<Result<Permit, Overload>>,
}

/// A lane's use of its class's slots.
#[derive(Clone, Copy, Debug, Default)]
struct Usage {
    /// Slot time released, plus the virtual time it joined at.
    held: Duration,
    /// Slots it holds now.
    active: usize,
}

/// One class's queue: FIFO lanes served least slot time first.
#[derive(Default)]
struct Queue {
    lanes: HashMap<Vec<u8>, VecDeque<Waiter>>,
    /// Lanes with waiters by `(held, turn)`: the first goes next.
    ready: BTreeSet<(Duration, u64, Vec<u8>)>,
    /// Each ready lane's key in `ready`.
    keys: HashMap<Vec<u8>, (Duration, u64)>,
    /// Lanes waiting or running.
    used: HashMap<Vec<u8>, Usage>,
    /// The class's virtual time: the held time of the last lane served.
    clock: Duration,
    /// The most held time a lane that went idle had.
    finished: Duration,
    turns: u64,
    queued: usize,
    active: usize,
}

impl Queue {
    fn push(&mut self, lane: &[u8], waiter: Waiter) {
        let queue = self.lanes.entry(lane.to_vec()).or_default();
        let joined = queue.is_empty();
        queue.push_back(waiter);
        self.queued += 1;
        if joined {
            let clock = self.clock;
            let usage = self.used.entry(lane.to_vec()).or_default();
            if usage.active == 0 {
                // A returning lane starts at the class's time: no credit
                // for its idle spell, no debt from before it.
                usage.held = usage.held.max(clock);
            }
            let held = usage.held;
            self.ready_push(lane, held);
        }
    }

    fn ready_push(&mut self, lane: &[u8], held: Duration) {
        let turn = self.turns;
        self.turns += 1;
        self.ready.insert((held, turn, lane.to_vec()));
        self.keys.insert(lane.to_vec(), (held, turn));
    }

    /// The next lane's oldest waiter; the lane takes a new turn while it
    /// still has waiters.
    fn pop(&mut self) -> Option<(Vec<u8>, Waiter)> {
        let (held, _, lane) = self.ready.pop_first()?;
        self.keys.remove(&lane);
        let queue = self.lanes.get_mut(&lane)?;
        let waiter = queue.pop_front()?;
        if queue.is_empty() {
            self.lanes.remove(&lane);
        } else {
            self.ready_push(&lane, held);
        }
        self.queued -= 1;
        self.clock = self.clock.max(held);
        Some((lane, waiter))
    }

    /// Take a waiter out of its lane, if it still waits.
    fn remove(&mut self, lane: &[u8], id: u64) -> Option<Waiter> {
        let queue = self.lanes.get_mut(lane)?;
        let at = queue.iter().position(|waiter| waiter.id == id)?;
        let waiter = queue.remove(at)?;
        if queue.is_empty() {
            self.lanes.remove(lane);
            if let Some((held, turn)) = self.keys.remove(lane) {
                self.ready.remove(&(held, turn, lane.to_vec()));
            }
        }
        self.queued -= 1;
        self.settle(lane);
        Some(waiter)
    }

    /// A slot granted to `lane`.
    fn start(&mut self, lane: &[u8]) {
        self.used.entry(lane.to_vec()).or_default().active += 1;
        self.active += 1;
    }

    /// `lane` released a slot it held for `held_for`: the time counts
    /// against the lane, re-ranking it if it waits.
    fn release(&mut self, lane: &[u8], held_for: Duration) {
        self.active -= 1;
        let Some(usage) = self.used.get_mut(lane) else {
            return;
        };
        usage.active = usage.active.saturating_sub(1);
        usage.held = usage.held.saturating_add(held_for);
        let held = usage.held;
        if let Some((old, turn)) = self.keys.get(lane).copied() {
            self.ready.remove(&(old, turn, lane.to_vec()));
            self.ready.insert((held, turn, lane.to_vec()));
            self.keys.insert(lane.to_vec(), (held, turn));
        }
        self.settle(lane);
    }

    /// Forget a lane that neither waits nor runs; once the whole class
    /// is idle, its time catches up with the lanes that left.
    fn settle(&mut self, lane: &[u8]) {
        if self.lanes.contains_key(lane) {
            return;
        }
        if let Some(usage) = self.used.get(lane)
            && usage.active == 0
        {
            self.finished = self.finished.max(usage.held);
            self.used.remove(lane);
        }
        if self.used.is_empty() {
            self.clock = self.clock.max(self.finished);
        }
    }

    /// When `lane`'s oldest waiter queued, if it has one.
    fn head(&self, lane: &[u8]) -> Option<Instant> {
        self.lanes
            .get(lane)
            .and_then(|queue| queue.front())
            .map(|waiter| waiter.queued_at)
    }

    /// When the oldest waiter of any lane queued.
    fn oldest(&self) -> Option<Instant> {
        self.lanes
            .values()
            .filter_map(|queue| queue.front())
            .map(|waiter| waiter.queued_at)
            .min()
    }
}

/// A grant or a refusal to hand a waiter outside the lock.
type Handout = (
    oneshot::Sender<Result<Permit, Overload>>,
    Result<Permit, Overload>,
);

struct State {
    queues: [Queue; 4],
    /// CoDel for queries and for writers.
    codel: [Codel; 2],
    /// Bytes held against the user and the control budget.
    user_bytes: usize,
    control_bytes: usize,
    /// Body bytes leased at ingress, of both budgets.
    ingress_bytes: usize,
    /// Bytes each class holds, queued or running, its body leases
    /// included: what its lanes' share is judged on.
    class_bytes: [usize; 4],
    /// Refresh turn credit while queries and refreshes both wait.
    credit: i64,
    next_id: u64,
    admitted: u64,
    rejected: u64,
    timed_out: u64,
    canceled: u64,
    shed: ShedMetrics,
    /// Settling writes queued.
    settling: u64,
    /// The last writer turn: when, and how long that writer waited.
    writer_turn: Option<(Instant, Duration)>,
}

impl State {
    fn new(config: &AdmissionConfig) -> Self {
        let codel = || Codel::new(config.codel_target, config.codel_interval);
        State {
            queues: Default::default(),
            codel: [codel(), codel()],
            user_bytes: 0,
            control_bytes: 0,
            ingress_bytes: 0,
            class_bytes: [0; 4],
            credit: 0,
            next_id: 0,
            admitted: 0,
            rejected: 0,
            timed_out: 0,
            canceled: 0,
            shed: ShedMetrics::default(),
            settling: 0,
            writer_turn: None,
        }
    }

    fn held(&mut self, class: Class) -> &mut usize {
        match class {
            Class::Control => &mut self.control_bytes,
            Class::Query | Class::Refresh | Class::Writer => &mut self.user_bytes,
        }
    }

    /// Charge `bytes` to `class`: its budget and its own count.
    fn charge(&mut self, class: Class, bytes: usize) {
        *self.held(class) += bytes;
        self.class_bytes[class.index()] += bytes;
    }

    /// Release `bytes` `class` held.
    fn discharge(&mut self, class: Class, bytes: usize) {
        *self.held(class) -= bytes;
        self.class_bytes[class.index()] -= bytes;
    }

    fn queue(&mut self, class: Class) -> &mut Queue {
        &mut self.queues[class.index()]
    }

    fn codel(&mut self, class: Class) -> Option<&mut Codel> {
        match class {
            Class::Query => Some(&mut self.codel[0]),
            Class::Writer => Some(&mut self.codel[1]),
            Class::Refresh | Class::Control => None,
        }
    }

    /// Hand out every grant a free slot admits, in priority order:
    /// control and the writer on their own slots, then queries and
    /// refreshes on the shared pool under the refresh share. A query or
    /// writer CoDel sheds at its turn is refused instead, and takes no
    /// slot.
    fn grants(&mut self, inner: &Arc<Inner>) -> Vec<Handout> {
        let config = &inner.config;
        let now = Instant::now();
        let mut handed = Vec::new();
        for class in [Class::Control, Class::Writer] {
            while self.queue(class).active < config.slots(class) {
                let Some((lane, waiter)) = self.queue(class).pop() else {
                    break;
                };
                handed.push(self.hand(inner, class, lane, waiter, now));
            }
        }
        let share = config.refresh_share_percent.min(100) as i64;
        loop {
            let active = self.queue(Class::Query).active + self.queue(Class::Refresh).active;
            if active >= config.slots(Class::Query) {
                break;
            }
            let queries = self.queue(Class::Query).queued > 0;
            let refreshes = self.queue(Class::Refresh).queued > 0;
            let class = match (queries, refreshes) {
                (false, false) => break,
                (true, false) => {
                    self.credit = 0;
                    Class::Query
                }
                (false, true) => {
                    self.credit = 0;
                    Class::Refresh
                }
                (true, true) => {
                    if self.credit >= 0
                        && self.queue(Class::Refresh).active < config.refresh_slots()
                    {
                        self.credit -= 100 - share;
                        Class::Refresh
                    } else {
                        self.credit = (self.credit + share).min(100);
                        Class::Query
                    }
                }
            };
            let Some((lane, waiter)) = self.queue(class).pop() else {
                break;
            };
            handed.push(self.hand(inner, class, lane, waiter, now));
        }
        handed
    }

    /// Grant `waiter` a slot, or refuse it when CoDel sheds it.
    fn hand(
        &mut self,
        inner: &Arc<Inner>,
        class: Class,
        lane: Vec<u8>,
        waiter: Waiter,
        now: Instant,
    ) -> Handout {
        let waited = now.saturating_duration_since(waiter.queued_at);
        // A writer's place is not its turn: it waits on in the writer's
        // queue, and is measured (and shed) there ([`Permit::turn`]).
        let shed = class != Class::Writer
            && self
                .codel(class)
                .is_some_and(|codel| codel.depart(now, waited, waiter.sheddable, waiter.settles));
        if shed {
            self.discharge(class, waiter.bytes);
            self.shed.queued += 1;
            self.queue(class).settle(&lane);
            return (waiter.grant, Err(Overload::new(class, SHED_AT_TURN)));
        }
        self.queue(class).start(&lane);
        self.admitted += 1;
        let permit = Permit {
            inner: inner.clone(),
            class,
            bytes: waiter.bytes,
            lane,
            since: now,
            arrived: waiter.queued_at,
            deadline: waiter.deadline,
            sheddable: waiter.sheddable,
            settles: waiter.settles,
        };
        (waiter.grant, Ok(permit))
    }
}

struct Inner {
    config: AdmissionConfig,
    state: Mutex<State>,
}

impl Inner {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Hand out every grant a change made possible. Sends happen
    /// outside the lock: a waiter that gave up drops its permit, whose
    /// release takes the lock again.
    fn schedule(self: &Arc<Self>) {
        let handed = self.lock().grants(self);
        for (grant, outcome) in handed {
            let _ = grant.send(outcome);
        }
    }
}

/// The node's admission controller. Cheap to clone: clones share one
/// state.
#[derive(Clone)]
pub struct Admission {
    inner: Arc<Inner>,
}

impl Default for Admission {
    fn default() -> Self {
        Admission::new(AdmissionConfig::default())
    }
}

impl Admission {
    /// An idle controller with `config`.
    pub fn new(config: AdmissionConfig) -> Self {
        Admission {
            inner: Arc::new(Inner {
                state: Mutex::new(State::new(&config)),
                config,
            }),
        }
    }

    /// The sizing in force.
    pub fn config(&self) -> &AdmissionConfig {
        &self.inner.config
    }

    /// Wait for a slot in `class` on database `lane`, holding `bytes`
    /// of the class budget from now until the permit drops. Refused at
    /// once when the bytes do not fit, and for queries and writers
    /// after [`AdmissionConfig::max_wait`] in the queue or at the
    /// client deadline of the request this task serves, or when CoDel
    /// sheds them. A request larger than the whole budget is charged
    /// the whole budget: it runs only when nothing else holds bytes.
    /// Refreshes charge nothing and are never refused. Dropping the
    /// future leaves the queue and releases the bytes. The wait counts
    /// as the request's admission stage (see [`crate::stages`]).
    pub async fn acquire(
        &self,
        class: Class,
        lane: &[u8],
        bytes: usize,
    ) -> Result<Permit, Overload> {
        self.timed(class, lane, bytes, false).await
    }

    /// [`Admission::acquire`] for a write that settles work already done
    /// (a queue claim's renewal, completion, failure or release): never
    /// shed for a standing queue or its lane's share, since refusing it
    /// wastes the work and makes more (the job is delivered again once
    /// its lease lapses, and dies at its attempt budget). It still waits
    /// at most the wait bound and its client deadline, and is refused
    /// for bytes. CoDel measures its wait like any other.
    pub async fn acquire_settling(
        &self,
        class: Class,
        lane: &[u8],
        bytes: usize,
    ) -> Result<Permit, Overload> {
        self.timed(class, lane, bytes, true).await
    }

    async fn timed(
        &self,
        class: Class,
        lane: &[u8],
        bytes: usize,
        settles: bool,
    ) -> Result<Permit, Overload> {
        let started = Instant::now();
        let outcome = self.wait(class, lane, bytes, settles).await;
        stages::add(Stage::Admission, started.elapsed());
        outcome
    }

    async fn wait(
        &self,
        class: Class,
        lane: &[u8],
        bytes: usize,
        settles: bool,
    ) -> Result<Permit, Overload> {
        let inner = &self.inner;
        let bytes = match class {
            Class::Refresh => 0,
            _ => bytes.min(inner.config.budget(class)),
        };
        let deadline = class.bounded().then(crate::deadline::current).flatten();
        // Only requests whose client gave a deadline are shed early, and
        // never one that settles work.
        let sheddable = deadline.is_some();
        let (id, receiver) = {
            let mut state = inner.lock();
            let held = state.held(class);
            if *held + bytes > inner.config.budget(class) {
                state.rejected += 1;
                return Err(Overload::new(class, BYTES_EXHAUSTED));
            }
            let now = Instant::now();
            let writer_wait = match class {
                Class::Writer => state.writer_wait(now, &inner.config),
                _ => Duration::ZERO,
            };
            if sheddable
                && !settles
                && (state.backlogged(&inner.config, class)
                    || writer_wait > inner.config.codel_target)
            {
                // Expect to wait as long as this lane's oldest waiter
                // has: a quiet lane is never refused for another's
                // backlog here. A writer also waits its turn in the
                // writer's queue, one FIFO for every lane, about as long
                // as the last writer to reach its turn did.
                let expected = state
                    .queue(class)
                    .head(lane)
                    .map_or(Duration::ZERO, |head| now.saturating_duration_since(head))
                    .max(writer_wait);
                if state
                    .codel(class)
                    .is_some_and(|codel| codel.refuse(now, expected))
                {
                    state.shed.arrival += 1;
                    return Err(Overload::new(class, SHED_AT_ARRIVAL));
                }
            }
            if class.bounded() && !settles && state.over_share(&inner.config, class, lane, bytes) {
                state.shed.share += 1;
                return Err(Overload::new(class, OVER_SHARE));
            }
            state.charge(class, bytes);
            let id = state.next_id;
            state.next_id += 1;
            let (grant, receiver) = oneshot::channel();
            state.queue(class).push(
                lane,
                Waiter {
                    id,
                    bytes,
                    queued_at: now,
                    sheddable,
                    settles,
                    deadline,
                    grant,
                },
            );
            if settles {
                state.settling += 1;
            }
            (id, receiver)
        };
        let mut waiting = Waiting {
            inner,
            class,
            lane,
            id,
            settled: false,
        };
        inner.schedule();
        let granted = if class.bounded() {
            let bound = Instant::now() + inner.config.max_wait;
            let until = deadline.map_or(bound, |deadline| deadline.min(bound));
            match tokio::time::timeout_at(until, receiver).await {
                Ok(granted) => granted,
                Err(_) => {
                    let at_deadline = deadline.is_some_and(|deadline| deadline <= bound);
                    waiting.leave(if at_deadline {
                        Left::Deadline
                    } else {
                        Left::TimedOut
                    });
                    let message = if at_deadline {
                        WAITED_UNTIL_DEADLINE
                    } else {
                        WAITED_TOO_LONG
                    };
                    return Err(Overload::new(class, message));
                }
            }
        } else {
            receiver.await
        };
        waiting.settled = true;
        // The sender lives in the queue until a grant consumes it, and
        // only this waiter removes its own entry, so this never fails.
        granted.unwrap_or_else(|_| {
            Err(Overload::new(
                class,
                "admission dropped the request; retry later",
            ))
        })
    }

    /// Lease `bytes` of `class`'s budget for a request body before it
    /// is read, held until the lease drops: refused at once when they
    /// do not fit. Grow the lease as an undeclared body arrives
    /// ([`Lease::grow`]).
    pub fn lease(&self, class: Class, bytes: usize) -> Result<Lease, Overload> {
        let mut lease = Lease {
            inner: self.inner.clone(),
            class,
            bytes: 0,
        };
        lease.grow(bytes)?;
        Ok(lease)
    }

    /// Current gauges and counters.
    pub fn metrics(&self) -> AdmissionMetrics {
        let config = &self.inner.config;
        let mut state = self.inner.lock();
        let now = Instant::now();
        let slots = config.slots(Class::Query);
        AdmissionMetrics {
            classes: Class::ALL
                .iter()
                .map(|class| {
                    let queue = &state.queues[class.index()];
                    (
                        *class,
                        ClassMetrics {
                            active: queue.active,
                            queued: queue.queued,
                            lanes: queue.lanes.len(),
                            oldest_queued_ms: queue.oldest().map_or(0, |oldest| {
                                now.saturating_duration_since(oldest).as_millis() as u64
                            }),
                        },
                    )
                })
                .collect(),
            retained_input_bytes: state.user_bytes,
            input_budget_bytes: config.queued_input_bytes,
            retained_control_bytes: state.control_bytes,
            control_budget_bytes: config.control_input_bytes,
            ingress_bytes: state.ingress_bytes,
            query_workers: slots,
            reserved_evaluation_bytes: (state.queues[Class::Query.index()].active
                + state.queues[Class::Refresh.index()].active)
                * config.evaluation_memory_bytes,
            evaluation_memory_budget_bytes: config
                .evaluation_memory_budget
                .unwrap_or(config.query_workers * config.evaluation_memory_bytes),
            admitted: state.admitted,
            rejected: state.rejected,
            timed_out: state.timed_out,
            canceled: state.canceled,
            settling: state.settling,
            shed: state.shed,
            queueing: QueueingMetrics {
                target_ms: config.codel_target.as_millis() as u64,
                interval_ms: config.codel_interval.as_millis() as u64,
                max_wait_ms: config.max_wait.as_millis() as u64,
                queries_overloaded: state
                    .codel(Class::Query)
                    .is_some_and(|codel| codel.overloaded),
                writers_overloaded: state
                    .codel(Class::Writer)
                    .is_some_and(|codel| codel.overloaded),
            },
        }
    }
}

impl State {
    /// Whether a new request of `class` would queue behind others: its
    /// slots are all taken or others already wait.
    fn backlogged(&self, config: &AdmissionConfig, class: Class) -> bool {
        let queue = &self.queues[class.index()];
        let active = match class {
            Class::Query | Class::Refresh => {
                self.queues[Class::Query.index()].active
                    + self.queues[Class::Refresh.index()].active
            }
            Class::Writer | Class::Control => queue.active,
        };
        queue.queued > 0 || active >= config.slots(class)
    }

    /// Whether a request of `class` holding `bytes` that would queue in
    /// `lane` is refused for the lane's share (v1's rule): the class holds
    /// more than half its budget, counting it (queued and running, its
    /// body leases included, as v1 counts a class's input), other lanes
    /// wait too, and this lane already has more than an even share of the
    /// waiters. The lane drains first; the others keep room to queue.
    /// Other classes' bytes on the same budget do not count: writes
    /// filling it are no reason to refuse a query its lane's share.
    fn over_share(
        &self,
        config: &AdmissionConfig,
        class: Class,
        lane: &[u8],
        bytes: usize,
    ) -> bool {
        if !self.backlogged(config, class) {
            return false;
        }
        let held = self.class_bytes[class.index()];
        let queue = &self.queues[class.index()];
        let own = queue.lanes.get(lane).map_or(0, VecDeque::len);
        queue.lanes.len() > 1
            && held.saturating_add(bytes) > config.budget(class) / 2
            && own.saturating_mul(queue.lanes.len()) > queue.queued
    }

    /// How long the last writer to reach its turn waited, while that is
    /// recent (within one interval): what a writer arriving now expects.
    fn writer_wait(&self, now: Instant, config: &AdmissionConfig) -> Duration {
        self.writer_turn
            .filter(|(at, _)| now.saturating_duration_since(*at) < config.codel_interval)
            .map_or(Duration::ZERO, |(_, waited)| waited)
    }
}

/// How a waiter left the queue without a grant.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Left {
    /// Its caller gave up.
    Canceled,
    /// It waited the wait bound.
    TimedOut,
    /// Its client deadline passed.
    Deadline,
}

/// A queued request's claim on its entry: leaving the queue without a
/// grant releases its bytes.
struct Waiting<'a> {
    inner: &'a Arc<Inner>,
    class: Class,
    lane: &'a [u8],
    id: u64,
    settled: bool,
}

impl Waiting<'_> {
    /// Leave the queue if still in it. A grant racing the departure
    /// sits in the dropped channel, and its permit releases itself.
    fn leave(&mut self, how: Left) {
        if self.settled {
            return;
        }
        self.settled = true;
        let mut state = self.inner.lock();
        if let Some(waiter) = state.queue(self.class).remove(self.lane, self.id) {
            state.discharge(self.class, waiter.bytes);
            match how {
                Left::Canceled => state.canceled += 1,
                Left::TimedOut => state.timed_out += 1,
                Left::Deadline => state.shed.deadline += 1,
            }
        }
    }
}

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        self.leave(Left::Canceled);
    }
}

/// A granted slot plus the bytes it holds; dropping it frees both,
/// charges the slot time to its lane and admits the next waiter.
pub struct Permit {
    inner: Arc<Inner>,
    class: Class,
    bytes: usize,
    lane: Vec<u8>,
    since: Instant,
    /// When the request reached admission.
    arrived: Instant,
    /// Its client deadline, if any.
    deadline: Option<Instant>,
    /// Whether it has a client deadline (see [`Waiter`]).
    sheddable: bool,
    /// Whether it settles work already done, and is never shed.
    settles: bool,
}

impl Permit {
    /// The class this permit was granted in.
    pub fn class(&self) -> Class {
        self.class
    }

    /// A writer's turn in the writer's queue, where writers wait once
    /// they hold a place (see [`crate::writer`]): refused when the
    /// request's client deadline passed, and on its `first` turn shed
    /// when the writers' CoDel finds the queue standing, for the whole
    /// time it waited since it reached admission. Nothing for other
    /// classes, whose grant is their turn.
    pub fn turn(&self, first: bool) -> Result<(), Overload> {
        if self.class != Class::Writer {
            return Ok(());
        }
        let now = Instant::now();
        let mut state = self.inner.lock();
        if self.deadline.is_some_and(|deadline| deadline <= now) {
            state.shed.deadline += 1;
            return Err(Overload::new(self.class, WAITED_UNTIL_DEADLINE));
        }
        if !first {
            return Ok(());
        }
        let waited = now.saturating_duration_since(self.arrived);
        state.writer_turn = Some((now, waited));
        if state
            .codel(self.class)
            .is_some_and(|codel| codel.depart(now, waited, self.sheddable, self.settles))
        {
            state.shed.queued += 1;
            return Err(Overload::new(self.class, SHED_AT_TURN));
        }
        Ok(())
    }
}

impl std::fmt::Debug for Permit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Permit")
            .field("class", &self.class)
            .field("bytes", &self.bytes)
            .finish()
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        {
            let mut state = self.inner.lock();
            state
                .queue(self.class)
                .release(&self.lane, self.since.elapsed());
            state.discharge(self.class, self.bytes);
        }
        self.inner.schedule();
    }
}

/// Request-body bytes held against a class budget from before the body
/// is read until the lease drops (see [`Admission::lease`]).
pub struct Lease {
    inner: Arc<Inner>,
    class: Class,
    bytes: usize,
}

impl Lease {
    /// Hold `more` bytes too, refused when they do not fit; the lease
    /// keeps what it held either way.
    pub fn grow(&mut self, more: usize) -> Result<(), Overload> {
        let budget = self.inner.config.budget(self.class);
        let mut state = self.inner.lock();
        if state.held(self.class).saturating_add(more) > budget {
            state.rejected += 1;
            return Err(Overload::new(self.class, BYTES_EXHAUSTED));
        }
        state.charge(self.class, more);
        state.ingress_bytes += more;
        self.bytes += more;
        Ok(())
    }

    /// Bytes the lease holds.
    pub fn bytes(&self) -> usize {
        self.bytes
    }
}

impl std::fmt::Debug for Lease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Lease")
            .field("class", &self.class)
            .field("bytes", &self.bytes)
            .finish()
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        let mut state = self.inner.lock();
        state.discharge(self.class, self.bytes);
        state.ingress_bytes -= self.bytes;
    }
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;

    use super::*;
    use crate::methods::Refusal;

    fn config(query_workers: usize) -> AdmissionConfig {
        AdmissionConfig {
            query_workers,
            control_workers: 1,
            queued_input_bytes: 10_000,
            control_input_bytes: 10_000,
            max_wait: Duration::from_secs(5),
            refresh_share_percent: 25,
            ..AdmissionConfig::default()
        }
    }

    /// Whether `future` is still pending after the scheduler settles.
    async fn pending<T>(future: &mut (impl Future<Output = T> + Unpin)) -> bool {
        tokio::select! {
            biased;
            _ = future => false,
            _ = tokio::time::sleep(Duration::from_millis(20)) => true,
        }
    }

    #[tokio::test]
    async fn the_pool_bounds_concurrent_evaluations() {
        let admission = Admission::new(config(2));
        let first = admission.acquire(Class::Query, b"db", 0).await.unwrap();
        let _second = admission.acquire(Class::Refresh, b"db", 0).await.unwrap();
        let mut third = Box::pin(admission.acquire(Class::Query, b"db", 0));
        assert!(pending(&mut third).await, "a third evaluation ran");
        assert_eq!(admission.metrics().classes[0].1.queued, 1);
        drop(first);
        let third = third.await.unwrap();
        assert_eq!(third.class(), Class::Query);
        assert_eq!(admission.metrics().admitted, 3);
    }

    #[tokio::test]
    async fn bytes_beyond_the_budget_refuse_at_once() {
        let admission = Admission::new(config(1));
        let held = admission.acquire(Class::Query, b"db", 6_000).await.unwrap();
        // Writers draw on the same budget as queries.
        let refused = admission
            .acquire(Class::Writer, b"db", 6_000)
            .await
            .unwrap_err();
        assert_eq!(refused.message, BYTES_EXHAUSTED);
        assert_eq!(refused.retry_after, RETRY_AFTER);
        // Control has its own budget; refreshes charge nothing.
        let _control = admission
            .acquire(Class::Control, b"db", 6_000)
            .await
            .unwrap();
        let mut refresh = Box::pin(admission.acquire(Class::Refresh, b"db", 1_000_000));
        assert!(pending(&mut refresh).await);
        drop(held);
        refresh.await.unwrap();
        let metrics = admission.metrics();
        assert_eq!(metrics.rejected, 1);
        assert_eq!(metrics.retained_input_bytes, 0);
    }

    /// Queue one request per `(lane, bytes)` of `class`, in order, each
    /// left waiting.
    async fn queue_up<'a>(
        admission: &'a Admission,
        class: Class,
        waiters: &[(&'a [u8], usize)],
    ) -> Vec<Pin<Box<impl Future<Output = Result<Permit, Overload>> + 'a>>> {
        let mut queued = Vec::new();
        for (lane, bytes) in waiters.iter().copied() {
            let mut waiter = Box::pin(admission.acquire(class, lane, bytes));
            assert!(pending(&mut waiter).await, "a queued request ran");
            queued.push(waiter);
        }
        queued
    }

    #[tokio::test]
    async fn a_lane_holding_more_than_its_share_of_a_filling_budget_is_refused() {
        let admission = Admission::new(config(1));
        let _held = admission.acquire(Class::Query, b"held", 0).await.unwrap();
        // Three noisy waiters and one quiet one hold 5,100 bytes: past
        // half the 10,000-byte budget.
        let noisy: &[u8] = b"noisy";
        let quiet: &[u8] = b"quiet";
        let _queued = queue_up(
            &admission,
            Class::Query,
            &[(noisy, 1_500), (noisy, 1_500), (noisy, 1_500), (quiet, 600)],
        )
        .await;
        let refused = admission
            .acquire(Class::Query, noisy, 100)
            .await
            .unwrap_err();
        assert_eq!(
            (refused.class, refused.message.as_str()),
            (Class::Query, OVER_SHARE)
        );
        assert_eq!(refused.retry_after, RETRY_AFTER);
        assert_eq!(Refusal::of(&refused.message), Some(Refusal::Full));
        // The quiet lane, and a lane nobody waits in, still queue.
        let mut quiet_waiter = Box::pin(admission.acquire(Class::Query, quiet, 100));
        assert!(pending(&mut quiet_waiter).await);
        let mut new_waiter = Box::pin(admission.acquire(Class::Query, b"new", 100));
        assert!(pending(&mut new_waiter).await);
        let metrics = admission.metrics();
        assert_eq!(metrics.shed.share, 1);
        // The refused request left nothing behind.
        assert_eq!(metrics.retained_input_bytes, 5_300);
        assert_eq!(metrics.classes[0].1.queued, 6);
    }

    #[tokio::test]
    async fn the_share_binds_only_past_half_the_budget_and_among_lanes() {
        let admission = Admission::new(config(1));
        let _held = admission.acquire(Class::Query, b"held", 0).await.unwrap();
        let noisy: &[u8] = b"noisy";
        // Below half the budget, a lane may hold every waiter but one.
        let mut queued = queue_up(
            &admission,
            Class::Query,
            &[
                (noisy, 1_000),
                (noisy, 1_000),
                (b"quiet", 1_000),
                (noisy, 1_000),
            ],
        )
        .await;
        let mut below = Box::pin(admission.acquire(Class::Query, noisy, 900));
        assert!(pending(&mut below).await, "refused below half the budget");
        // Past half the budget, a lane waiting alone is not refused: there
        // is no other lane to keep room for.
        drop(below);
        drop(queued.remove(2));
        let mut alone = Box::pin(admission.acquire(Class::Query, noisy, 2_500));
        assert!(
            pending(&mut alone).await,
            "the only lane waiting was refused"
        );
        assert_eq!(admission.metrics().shed.share, 0);
    }

    #[tokio::test]
    async fn a_class_is_judged_on_its_own_bytes_not_the_others() {
        let admission = Admission::new(config(1));
        // Writers hold 6,000 bytes of the 10,000-byte budget the queries
        // draw on too; queries hold little.
        let _writes = admission.acquire(Class::Writer, b"w", 6_000).await.unwrap();
        let _held = admission.acquire(Class::Query, b"held", 0).await.unwrap();
        let noisy: &[u8] = b"noisy";
        let _queued = queue_up(
            &admission,
            Class::Query,
            &[(noisy, 100), (noisy, 100), (noisy, 100), (b"quiet", 100)],
        )
        .await;
        // The noisy lane holds three of four query waiters, but queries
        // hold under half the budget: as in v1, which counts each class's
        // input apart, it still queues.
        let mut noisy_waiter = Box::pin(admission.acquire(Class::Query, noisy, 100));
        assert!(
            pending(&mut noisy_waiter).await,
            "refused for another class's bytes"
        );
        assert_eq!(admission.metrics().shed.share, 0);
    }

    #[tokio::test]
    async fn writers_keep_to_their_lanes_share_once_every_place_is_taken() {
        let admission = Admission::new(config(1));
        let mut _places = Vec::new();
        for _ in 0..WRITER_QUEUE {
            _places.push(admission.acquire(Class::Writer, b"db", 0).await.unwrap());
        }
        let noisy: &[u8] = b"noisy";
        let _queued = queue_up(
            &admission,
            Class::Writer,
            &[(noisy, 2_000), (b"quiet", 1_000), (noisy, 2_000)],
        )
        .await;
        let refused = admission
            .acquire(Class::Writer, noisy, 1_000)
            .await
            .unwrap_err();
        assert_eq!(
            (refused.class, refused.message.as_str()),
            (Class::Writer, OVER_SHARE)
        );
        let mut quiet = Box::pin(admission.acquire(Class::Writer, b"quiet", 1_000));
        assert!(pending(&mut quiet).await);
        assert_eq!(admission.metrics().shed.share, 1);
    }

    #[tokio::test]
    async fn oversized_requests_run_alone() {
        let admission = Admission::new(config(4));
        let alone = admission
            .acquire(Class::Query, b"db", 1 << 40)
            .await
            .unwrap();
        assert_eq!(admission.metrics().retained_input_bytes, 10_000);
        let refused = admission.acquire(Class::Query, b"db", 1).await.unwrap_err();
        assert_eq!(refused.message, BYTES_EXHAUSTED);
        drop(alone);
        admission.acquire(Class::Query, b"db", 1).await.unwrap();
    }

    #[tokio::test]
    async fn queries_and_writers_wait_at_most_max_wait() {
        let mut short = config(1);
        short.max_wait = Duration::from_millis(30);
        let admission = Admission::new(short);
        let _query = admission.acquire(Class::Query, b"db", 100).await.unwrap();
        // The writer's queue is full.
        let mut _writers = vec![admission.acquire(Class::Writer, b"db", 100).await.unwrap()];
        for _ in 1..WRITER_QUEUE {
            _writers.push(admission.acquire(Class::Writer, b"db", 0).await.unwrap());
        }
        for class in [Class::Query, Class::Writer] {
            let refused = admission.acquire(class, b"db", 100).await.unwrap_err();
            assert_eq!(
                (refused.class, refused.message.as_str()),
                (class, WAITED_TOO_LONG)
            );
        }
        let metrics = admission.metrics();
        assert_eq!(metrics.timed_out, 2);
        // Timed-out waiters released their bytes and left the queue.
        assert_eq!(metrics.retained_input_bytes, 200);
        assert!(metrics.classes.iter().all(|(_, class)| class.queued == 0));
    }

    #[tokio::test]
    async fn canceled_waiters_release_their_bytes() {
        let admission = Admission::new(config(1));
        let held = admission.acquire(Class::Query, b"db", 100).await.unwrap();
        let mut waiting = Box::pin(admission.acquire(Class::Query, b"db", 4_000));
        assert!(pending(&mut waiting).await);
        assert_eq!(admission.metrics().retained_input_bytes, 4_100);
        drop(waiting);
        let metrics = admission.metrics();
        assert_eq!((metrics.canceled, metrics.retained_input_bytes), (1, 100));
        drop(held);
        // The freed slot went to nobody: the next request runs at once.
        admission.acquire(Class::Query, b"db", 0).await.unwrap();
    }

    #[tokio::test]
    async fn writers_and_control_keep_capacity_under_query_floods() {
        let admission = Admission::new(config(1));
        let _query = admission.acquire(Class::Query, b"db", 0).await.unwrap();
        let mut flood: Vec<_> = (0..8)
            .map(|_| Box::pin(admission.acquire(Class::Query, b"db", 0)))
            .collect();
        for waiter in &mut flood {
            assert!(pending(waiter).await);
        }
        // Neither class queues behind the saturated pool.
        let writer = admission.acquire(Class::Writer, b"db", 0).await.unwrap();
        let control = admission.acquire(Class::Control, b"db", 0).await.unwrap();
        assert_eq!(
            (writer.class(), control.class()),
            (Class::Writer, Class::Control)
        );
    }

    /// Lanes in the order queued tasks were granted.
    type Order = Arc<Mutex<Vec<&'static str>>>;

    /// Queue `lanes` in order behind a held slot, each task recording
    /// its lane when granted and holding the slot for `hold`.
    async fn grant_order(
        admission: &Admission,
        lanes: &[&'static str],
        hold: Duration,
    ) -> (Vec<tokio::task::JoinHandle<()>>, Order) {
        let order = Order::default();
        let mut tasks = Vec::new();
        for (i, lane) in lanes.iter().copied().enumerate() {
            let (task_admission, order) = (admission.clone(), order.clone());
            tasks.push(tokio::spawn(async move {
                let permit = task_admission
                    .acquire(Class::Query, lane.as_bytes(), 0)
                    .await
                    .unwrap();
                order.lock().unwrap().push(lane);
                tokio::time::sleep(hold).await;
                drop(permit);
            }));
            // Queue in a known order.
            while admission.metrics().classes[0].1.queued <= i {
                tokio::task::yield_now().await;
            }
        }
        (tasks, order)
    }

    #[tokio::test(start_paused = true)]
    async fn lanes_take_turns() {
        let admission = Admission::new(config(1));
        let held = admission.acquire(Class::Query, b"busy", 0).await.unwrap();
        let (tasks, order) = grant_order(
            &admission,
            &["busy", "busy", "busy", "quiet"],
            Duration::ZERO,
        )
        .await;
        drop(held);
        for task in tasks {
            task.await.unwrap();
        }
        // No slot time passed: lanes alternate, so the quiet database's
        // one request went second, not last.
        assert_eq!(*order.lock().unwrap(), ["busy", "quiet", "busy", "busy"]);
    }

    #[tokio::test(start_paused = true)]
    async fn lanes_share_slots_by_the_time_they_hold_them() {
        let admission = Admission::new(config(1));
        // The noisy lane held the only slot for a second while both
        // lanes queued: the quiet lane now goes first, and again before
        // the noisy one catches up.
        let held = admission.acquire(Class::Query, b"noisy", 0).await.unwrap();
        let (tasks, order) = grant_order(
            &admission,
            &["noisy", "noisy", "quiet", "quiet"],
            Duration::from_millis(100),
        )
        .await;
        tokio::time::advance(Duration::from_secs(1)).await;
        drop(held);
        for task in tasks {
            task.await.unwrap();
        }
        assert_eq!(*order.lock().unwrap(), ["quiet", "quiet", "noisy", "noisy"]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_lane_returning_from_idle_starts_at_the_classs_time() {
        let admission = Admission::new(config(1));
        // "idle" ran once and left: a lane that neither waits nor runs
        // is forgotten.
        drop(admission.acquire(Class::Query, b"idle", 0).await.unwrap());
        assert!(
            admission.inner.lock().queues[0].used.is_empty(),
            "idle lanes linger"
        );
        // "busy" has held the slot 5 s and keeps using it.
        let first = admission.acquire(Class::Query, b"busy", 0).await.unwrap();
        let second = {
            let admission = admission.clone();
            tokio::spawn(async move { admission.acquire(Class::Query, b"busy", 0).await })
        };
        while admission.metrics().classes[0].1.queued == 0 {
            tokio::task::yield_now().await;
        }
        tokio::time::advance(Duration::from_secs(5)).await;
        drop(first);
        let second = second.await.unwrap().unwrap();
        // "idle" returns while "busy" holds the slot, behind another of
        // its requests: it joins at the class's time, not with the 5 s
        // of credit its idle spell would have banked, so the busy lane's
        // request queued first still goes first.
        let (tasks, order) = grant_order(&admission, &["busy", "idle"], Duration::ZERO).await;
        drop(second);
        for task in tasks {
            task.await.unwrap();
        }
        assert_eq!(*order.lock().unwrap(), ["busy", "idle"]);
    }

    #[tokio::test]
    async fn refreshes_get_their_share_while_queries_wait() {
        let admission = Admission::new(config(1));
        let held = admission.acquire(Class::Query, b"db", 0).await.unwrap();
        let order = Arc::new(Mutex::new(Vec::new()));
        let mut tasks = Vec::new();
        let classes = [Class::Query; 6]
            .into_iter()
            .chain([Class::Refresh; 2])
            .collect::<Vec<_>>();
        for (i, class) in classes.into_iter().enumerate() {
            let (task_admission, order) = (admission.clone(), order.clone());
            tasks.push(tokio::spawn(async move {
                let permit = task_admission.acquire(class, b"db", 0).await.unwrap();
                order.lock().unwrap().push(class);
                drop(permit);
            }));
            while admission
                .metrics()
                .classes
                .iter()
                .map(|(_, class)| class.queued)
                .sum::<usize>()
                <= i
            {
                tokio::task::yield_now().await;
            }
        }
        drop(held);
        for task in tasks {
            task.await.unwrap();
        }
        use Class::{Query as Q, Refresh as R};
        // One turn in four goes to a refresh while both wait.
        assert_eq!(*order.lock().unwrap(), [R, Q, Q, Q, R, Q, Q, Q]);
    }

    #[tokio::test]
    async fn the_memory_budget_bounds_the_pool() {
        let mut sized = config(8);
        sized.evaluation_memory_bytes = 100;
        sized.evaluation_memory_budget = Some(250);
        let admission = Admission::new(sized);
        // Two reservations fit the budget, whatever the worker count.
        let _first = admission.acquire(Class::Query, b"db", 0).await.unwrap();
        let _second = admission.acquire(Class::Refresh, b"db", 0).await.unwrap();
        let mut third = Box::pin(admission.acquire(Class::Query, b"db", 0));
        assert!(
            pending(&mut third).await,
            "evaluated past the memory budget"
        );
        let metrics = admission.metrics();
        assert_eq!(metrics.query_workers, 2);
        assert_eq!(metrics.reserved_evaluation_bytes, 200);
        assert_eq!(metrics.evaluation_memory_budget_bytes, 250);
    }

    #[tokio::test(start_paused = true)]
    async fn requests_wait_no_longer_than_their_client_deadline() {
        let admission = Admission::new(config(1));
        let _held = admission.acquire(Class::Query, b"db", 0).await.unwrap();
        let mut _places = Vec::new();
        for _ in 0..WRITER_QUEUE {
            _places.push(admission.acquire(Class::Writer, b"db", 0).await.unwrap());
        }
        for class in [Class::Query, Class::Writer] {
            let deadline = Instant::now() + Duration::from_millis(200);
            let started = Instant::now();
            let refused =
                crate::deadline::scoped(Some(deadline), admission.acquire(class, b"db", 50))
                    .await
                    .unwrap_err();
            assert_eq!(refused.message, WAITED_UNTIL_DEADLINE);
            assert_eq!(started.elapsed(), Duration::from_millis(200));
        }
        // A passed deadline refuses at once; a later one than the wait
        // bound waits the bound.
        let passed = crate::deadline::scoped(
            Some(Instant::now()),
            admission.acquire(Class::Query, b"db", 0),
        )
        .await
        .unwrap_err();
        assert_eq!(passed.message, WAITED_UNTIL_DEADLINE);
        let later = crate::deadline::scoped(
            Some(Instant::now() + Duration::from_secs(60)),
            admission.acquire(Class::Query, b"db", 0),
        )
        .await
        .unwrap_err();
        assert_eq!(later.message, WAITED_TOO_LONG);
        // Control work has no client deadline to honor.
        let control = crate::deadline::scoped(
            Some(Instant::now()),
            admission.acquire(Class::Control, b"db", 0),
        )
        .await;
        assert!(control.is_ok());
        let metrics = admission.metrics();
        assert_eq!((metrics.shed.deadline, metrics.timed_out), (3, 1));
        assert_eq!(metrics.retained_input_bytes, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn writers_are_measured_and_shed_at_their_turn_not_their_place() {
        let admission = Admission::new(config(1));
        let deadline = || Some(Instant::now() + Duration::from_secs(60));
        // Ten writers take places at once, then wait in the writer's
        // queue 200 ms apart: the queue stands.
        let mut places = Vec::new();
        for _ in 0..10 {
            places.push(
                crate::deadline::scoped(deadline(), admission.acquire(Class::Writer, b"db", 0))
                    .await
                    .unwrap(),
            );
        }
        let mut shed = 0;
        for place in &places {
            tokio::time::advance(Duration::from_millis(200)).await;
            if let Err(refused) = place.turn(true) {
                assert_eq!(refused.message, SHED_AT_TURN);
                shed += 1;
            }
        }
        // Shed once overload was decided, an interval in: not before.
        assert!((1..10).contains(&shed), "{shed} shed");
        assert!(admission.metrics().queueing.writers_overloaded);
        // A newcomer expects the last turn's wait, whatever its lane,
        // though places are free.
        let refused =
            crate::deadline::scoped(deadline(), admission.acquire(Class::Writer, b"other", 0))
                .await
                .unwrap_err();
        assert_eq!(refused.message, SHED_AT_ARRIVAL);
        // Without a deadline, never shed.
        let patient = admission.acquire(Class::Writer, b"db", 0).await.unwrap();
        tokio::time::advance(Duration::from_millis(200)).await;
        assert!(patient.turn(true).is_ok());
        let metrics = admission.metrics();
        assert_eq!((metrics.shed.queued, metrics.shed.arrival), (shed, 1));
    }

    #[tokio::test(start_paused = true)]
    async fn writes_settling_done_work_are_never_shed() {
        let admission = Admission::new(config(1));
        let deadline = || Some(Instant::now() + Duration::from_secs(60));
        let settling = crate::deadline::scoped(
            deadline(),
            admission.acquire_settling(Class::Writer, b"db", 0),
        )
        .await
        .unwrap();
        // Ten writers reach their turns 200 ms apart: the queue stands.
        let mut places = Vec::new();
        for _ in 0..10 {
            places.push(
                crate::deadline::scoped(deadline(), admission.acquire(Class::Writer, b"db", 0))
                    .await
                    .unwrap(),
            );
        }
        for place in &places {
            tokio::time::advance(Duration::from_millis(200)).await;
            let _ = place.turn(true);
        }
        assert!(admission.metrics().queueing.writers_overloaded);
        // An ordinary write is refused at arrival; one settling a claim
        // is admitted, and neither settling write is shed at its turn,
        // however long it waited.
        let refused =
            crate::deadline::scoped(deadline(), admission.acquire(Class::Writer, b"other", 0))
                .await
                .unwrap_err();
        assert_eq!(refused.message, SHED_AT_ARRIVAL);
        let late = crate::deadline::scoped(
            deadline(),
            admission.acquire_settling(Class::Writer, b"other", 0),
        )
        .await
        .unwrap();
        tokio::time::advance(Duration::from_millis(200)).await;
        assert!(settling.turn(true).is_ok());
        assert!(late.turn(true).is_ok());
        assert_eq!(admission.metrics().shed.arrival, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn settling_writes_count_toward_overload_and_others_are_shed() {
        let admission = Admission::new(config(1));
        let deadline = || Some(Instant::now() + Duration::from_secs(60));
        // Thirty writers reach their turns 200 ms apart, two of three
        // settling a worker's claims in one database, the rest ordinary
        // writes of another: the queue stands.
        let mut places = Vec::new();
        for writer in 0..30 {
            let settles = writer % 3 != 2;
            let place = if settles {
                crate::deadline::scoped(
                    deadline(),
                    admission.acquire_settling(Class::Writer, b"worker", 0),
                )
                .await
            } else {
                crate::deadline::scoped(deadline(), admission.acquire(Class::Writer, b"other", 0))
                    .await
            };
            places.push((settles, place.unwrap()));
        }
        let (mut settling_shed, mut others_shed) = (0, 0);
        for (settles, place) in &places {
            tokio::time::advance(Duration::from_millis(200)).await;
            if place.turn(true).is_err() {
                if *settles {
                    settling_shed += 1;
                } else {
                    others_shed += 1;
                }
            }
        }
        // The other database's writes get their fast refusal; the
        // settling ones, most of the turns, are never shed. Counted as
        // unsheddable, they kept the queue from ever being judged
        // overloaded, and nothing was shed.
        assert!(admission.metrics().queueing.writers_overloaded);
        assert_eq!(settling_shed, 0);
        assert!(others_shed > 0, "no ordinary write was shed");
    }

    #[tokio::test(start_paused = true)]
    async fn a_writers_turn_refuses_it_once_its_deadline_passed() {
        let admission = Admission::new(config(1));
        let deadline = Some(Instant::now() + Duration::from_millis(100));
        let place = crate::deadline::scoped(deadline, admission.acquire(Class::Writer, b"db", 0))
            .await
            .unwrap();
        assert!(place.turn(true).is_ok());
        // Prepared again after its batch went stale, past its deadline.
        tokio::time::advance(Duration::from_millis(100)).await;
        assert_eq!(
            place.turn(false).unwrap_err().message,
            WAITED_UNTIL_DEADLINE
        );
        assert_eq!(admission.metrics().shed.deadline, 1);
        // Other classes' grants are their turns.
        let query = admission.acquire(Class::Query, b"db", 0).await.unwrap();
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(query.turn(true).is_ok());
    }

    fn codel() -> Codel {
        Codel::new(Duration::from_millis(50), Duration::from_millis(500))
    }

    const MS: Duration = Duration::from_millis(1);

    #[test]
    fn a_standing_queue_sheds_only_long_waits_after_a_full_interval() {
        let mut codel = codel();
        let start = Instant::now();
        // A full interval of 150 ms waits: nothing is shed in it.
        for step in 0..50u32 {
            assert!(!codel.depart(start + step * 10 * MS, 150 * MS, true, false));
        }
        // Then the queue stands: waits past twice the target are shed,
        // shorter ones are not, and unsheddable requests never are.
        assert!(codel.depart(start + 510 * MS, 150 * MS, true, false));
        assert!(!codel.depart(start + 520 * MS, 90 * MS, true, false));
        assert!(!codel.depart(start + 530 * MS, 150 * MS, false, false));
        assert!(codel.overloaded);
    }

    #[test]
    fn one_short_wait_per_interval_clears_overload() {
        let mut codel = codel();
        let start = Instant::now();
        for step in 0..50u32 {
            codel.depart(start + step * 10 * MS, 150 * MS, true, false);
        }
        codel.depart(start + 500 * MS, 80 * MS, true, false);
        assert!(codel.overloaded);
        // One wait at or under the target in the next interval, and no
        // refusal: overload clears at its end.
        codel.depart(start + 600 * MS, 10 * MS, true, false);
        codel.depart(start + 1_010 * MS, 80 * MS, true, false);
        assert!(!codel.overloaded);
    }

    #[test]
    fn overload_lasts_while_requests_are_still_refused_for_their_wait() {
        let mut codel = codel();
        let start = Instant::now();
        for step in 0..50u32 {
            codel.depart(start + step * 10 * MS, 150 * MS, true, false);
        }
        assert!(codel.depart(start + 500 * MS, 150 * MS, true, false));
        // A short wait came too, but a refusal in the interval keeps
        // the overload for the next.
        codel.depart(start + 600 * MS, 5 * MS, true, false);
        codel.depart(start + 1_010 * MS, 5 * MS, true, false);
        assert!(codel.overloaded);
    }

    #[test]
    fn bursts_shorter_than_an_interval_are_absorbed() {
        let mut codel = codel();
        let start = Instant::now();
        codel.depart(start, Duration::ZERO, true, false);
        for step in 1..50u32 {
            codel.depart(start + step * 10 * MS, 400 * MS, true, false);
        }
        assert!(!codel.depart(start + 500 * MS, 400 * MS, true, false));
        assert!(!codel.overloaded);
    }

    #[test]
    fn idle_intervals_forget_overload() {
        let mut codel = codel();
        let start = Instant::now();
        for step in 0..50u32 {
            codel.depart(start + step * 10 * MS, 150 * MS, true, false);
        }
        codel.depart(start + 500 * MS, 150 * MS, true, false);
        assert!(codel.overloaded);
        // Seconds later, a request that barely waited finds no overload.
        assert!(!codel.depart(start + 5_000 * MS, 120 * MS, true, false));
        assert!(!codel.overloaded);
    }

    #[test]
    fn a_queue_standing_with_unsheddable_items_is_no_overload() {
        let mut codel = codel();
        let start = Instant::now();
        for step in 0..50u32 {
            codel.depart(start + step * 10 * MS, 150 * MS, step % 4 == 0, false);
        }
        assert!(!codel.depart(start + 500 * MS, 150 * MS, true, false));
        assert!(!codel.overloaded);
        // Mostly sheddable: overload.
        for step in 51..100u32 {
            codel.depart(start + step * 10 * MS, 150 * MS, step % 4 != 0, false);
        }
        codel.depart(start + 1_010 * MS, 150 * MS, true, false);
        assert!(codel.overloaded);
    }

    #[test]
    fn a_queue_stuck_for_an_interval_is_overloaded() {
        let mut codel = codel();
        let start = Instant::now();
        codel.depart(start, 10 * MS, true, false);
        // Nothing departed for over an interval while requests waited:
        // an arrival expecting a long wait finds the queue stuck.
        assert!(codel.refuse(start + 2_000 * MS, 900 * MS));
        assert!(codel.overloaded);
        // Arrivals expecting no wait are let in.
        assert!(!codel.refuse(start + 2_010 * MS, Duration::ZERO));
    }

    /// Spawn a query admission in `lane`, with a client deadline 30 s
    /// out when `sheddable`.
    fn spawn_query(
        admission: &Admission,
        lane: &'static [u8],
        sheddable: bool,
    ) -> tokio::task::JoinHandle<Result<Permit, Overload>> {
        let admission = admission.clone();
        let deadline = sheddable.then(|| Instant::now() + Duration::from_secs(30));
        tokio::spawn(crate::deadline::scoped(deadline, async move {
            admission.acquire(Class::Query, lane, 100).await
        }))
    }

    /// Wait until `queued` queries wait.
    async fn queued(admission: &Admission, queued: usize) {
        while admission.metrics().classes[0].1.queued < queued {
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_standing_queue_sheds_requests_with_deadlines_only() {
        let mut sized = config(1);
        sized.max_wait = Duration::from_secs(60);
        let admission = Admission::new(sized);
        let held = admission.acquire(Class::Query, b"db", 0).await.unwrap();
        // The query queue stands (the CoDel tests show how it gets
        // there) for the rest of the test.
        {
            let mut state = admission.inner.lock();
            state.codel[0].overloaded = true;
            state.codel[0].until = Some(Instant::now() + Duration::from_secs(3600));
        }
        let sheddable = spawn_query(&admission, b"db", true);
        queued(&admission, 1).await;
        let patient = spawn_query(&admission, b"db", false);
        queued(&admission, 2).await;
        tokio::time::advance(Duration::from_millis(300)).await;
        // Behind a lane whose oldest waiter waited past twice the
        // target, a request with a deadline is refused at arrival.
        let refused = spawn_query(&admission, b"db", true).await.unwrap();
        assert_eq!(refused.unwrap_err().message, SHED_AT_ARRIVAL);
        // A quiet database's lane is never refused for that backlog.
        let quiet = spawn_query(&admission, b"quiet", true);
        queued(&admission, 3).await;
        drop(held);
        // The quiet lane goes first: "db" just held the slot 300 ms.
        drop(quiet.await.unwrap().unwrap());
        // At its turn the sheddable waiter is refused without taking the
        // slot, and the one without a deadline runs: it is never shed,
        // however long it waited.
        assert_eq!(sheddable.await.unwrap().unwrap_err().message, SHED_AT_TURN);
        drop(patient.await.unwrap().unwrap());
        let metrics = admission.metrics();
        assert_eq!((metrics.shed.arrival, metrics.shed.queued), (1, 1));
        assert_eq!(metrics.retained_input_bytes, 0);
        assert!(metrics.queueing.queries_overloaded);
    }

    #[tokio::test]
    async fn body_leases_hold_their_budget_until_dropped() {
        let admission = Admission::new(config(1));
        let mut lease = admission.lease(Class::Query, 4_000).unwrap();
        lease.grow(2_000).unwrap();
        assert_eq!(lease.bytes(), 6_000);
        // The rest of the budget is all a request may now take.
        let refused = admission.lease(Class::Writer, 5_000).unwrap_err();
        assert_eq!(refused.message, BYTES_EXHAUSTED);
        assert!(lease.grow(5_000).is_err());
        assert_eq!(lease.bytes(), 6_000);
        assert!(admission.acquire(Class::Query, b"db", 5_000).await.is_err());
        // Control bodies draw on their own budget.
        let control = admission.lease(Class::Control, 9_000).unwrap();
        let metrics = admission.metrics();
        assert_eq!(metrics.ingress_bytes, 15_000);
        assert_eq!(metrics.retained_input_bytes, 6_000);
        assert_eq!(metrics.rejected, 3);
        drop((lease, control));
        let metrics = admission.metrics();
        assert_eq!(
            (metrics.ingress_bytes, metrics.retained_input_bytes),
            (0, 0)
        );
    }

    #[test]
    fn sizings_validate() {
        assert!(AdmissionConfig::default().validate().is_ok());
        let broken: [fn(&mut AdmissionConfig); 8] = [
            |config| config.query_workers = 0,
            |config| config.control_workers = 0,
            |config| config.queued_input_bytes = 1,
            |config| config.max_wait = Duration::ZERO,
            |config| config.refresh_share_percent = 101,
            |config| config.codel_target = Duration::ZERO,
            |config| config.evaluation_memory_bytes = 0,
            |config| config.evaluation_memory_budget = Some(1),
        ];
        for breaks in broken {
            let mut config = AdmissionConfig::default();
            breaks(&mut config);
            assert!(config.validate().is_err(), "{config:?}");
        }
    }

    #[test]
    fn input_bytes_grow_with_the_value() {
        let small = input_bytes(&Value::Null);
        let large = input_bytes(&Value::Array(vec![
            Value::String("x".repeat(1000)),
            Value::Object([("key".to_string(), Value::Bool(true))].into()),
        ]));
        assert_eq!(small, REQUEST_OVERHEAD + std::mem::size_of::<Value>());
        assert!(large > small + 1000);
        assert!(deploy_bytes(1024, 10) > 4 * 1024);
    }
}
