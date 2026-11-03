//! Accounting: every attempt and every logical call, per phase and per
//! operation, a one-second timeline, and the goodput summary.
//!
//! A *logical call* is what a customer or drone asked for: one order,
//! one tip, one read, one delivery. It may take several *attempts*
//! (uncertain retries resend the same request ID and body), and its
//! latency runs from its start (open loop: its intended arrival) to its
//! answer, retries and leader discovery included. Each call has a
//! [`Class`] saying whose work it is; only customers' primary calls
//! count toward goodput.
//!
//! **Goodput** counts successful primary customer logical calls (reads,
//! orders and tips) per second of the measured load phase. It excludes worker
//! traffic (claims, deliveries, reports), explicit receipt-replay probes,
//! setup, warmup, drain and audit calls, and every failed call; a call that
//! succeeded after transport retries counts once. The load phase runs from
//! the synchronized start until the last customer call in flight at the
//! deadline answered, so final tails are inside it.

use std::collections::BTreeMap;
use std::sync::{Mutex, PoisonError};
use std::time::Instant;

use serde::Serialize;

use crate::histogram::{Histogram, HistogramSnapshot, Percentiles};

/// Whose work a call is.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum Class {
    /// A customer's store preview.
    Read,
    /// A customer's order or tip.
    Mutation,
    /// A delivery drone: claims, deliveries and queue reports.
    Worker,
    /// An explicit receipt-replay probe.
    Replay,
    /// Setup, warmup, resolution and audit traffic.
    Control,
}

impl Class {
    /// Whether the class counts toward goodput.
    pub fn is_customer(self) -> bool {
        matches!(self, Class::Read | Class::Mutation)
    }
}

/// Run phases, in order.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum Phase {
    /// Starting servers, deploying, opening the kitchens.
    Setup,
    /// Unmeasured reads that warm connections and caches.
    Warmup,
    /// The measured window.
    Load,
    /// Customers stopped; drones finish every acknowledged order.
    Drain,
    /// The business audit.
    Audit,
}

impl Phase {
    /// Every phase, in order.
    pub const ALL: [Phase; 5] = [
        Phase::Setup,
        Phase::Warmup,
        Phase::Load,
        Phase::Drain,
        Phase::Audit,
    ];

    /// Lowercase name.
    pub fn name(self) -> &'static str {
        match self {
            Phase::Setup => "setup",
            Phase::Warmup => "warmup",
            Phase::Load => "load",
            Phase::Drain => "drain",
            Phase::Audit => "audit",
        }
    }
}

/// One operation's counters within a phase.
#[derive(Clone, Debug)]
struct OpStats {
    class: Class,
    calls: u64,
    completed: u64,
    failed: u64,
    latency: Histogram,
    attempts: u64,
    attempt_failures: u64,
    retries: u64,
    attempt_latency: Histogram,
    errors: BTreeMap<String, u64>,
    stages: BTreeMap<String, Histogram>,
}

impl OpStats {
    fn new(class: Class) -> Self {
        OpStats {
            class,
            calls: 0,
            completed: 0,
            failed: 0,
            latency: Histogram::default(),
            attempts: 0,
            attempt_failures: 0,
            retries: 0,
            attempt_latency: Histogram::default(),
            errors: BTreeMap::new(),
            stages: BTreeMap::new(),
        }
    }
}

/// Parse a `Server-Timing` header: each `name;dur=ms` entry (the
/// server's `forward`, `fence`, `admission`, `queue`, `eval`, `commit`
/// and `total`); descriptions and malformed entries are skipped.
pub fn server_timing(header: &str) -> Vec<(String, f64)> {
    header
        .split(',')
        .filter_map(|entry| {
            let mut parts = entry.trim().split(';');
            let name = parts.next()?.trim();
            let duration = parts.find_map(|part| part.trim().strip_prefix("dur="))?;
            let ms = duration
                .parse::<f64>()
                .ok()
                .filter(|ms| ms.is_finite() && *ms >= 0.0)?;
            (!name.is_empty() && name.len() <= 32).then(|| (name.to_string(), ms))
        })
        .collect()
}

/// One phase's counters.
#[derive(Clone, Debug)]
struct PhaseStats {
    started: Instant,
    ended: Option<Instant>,
    ops: BTreeMap<String, OpStats>,
}

/// The current timeline interval's counters.
#[derive(Clone, Debug)]
struct Interval {
    started: Instant,
    reads: u64,
    mutations: u64,
    worker: u64,
    failed_calls: u64,
    failed_attempts: u64,
    retries: u64,
    read_latency: Histogram,
    write_latency: Histogram,
}

impl Interval {
    fn new(started: Instant) -> Self {
        Interval {
            started,
            reads: 0,
            mutations: 0,
            worker: 0,
            failed_calls: 0,
            failed_attempts: 0,
            retries: 0,
            read_latency: Histogram::default(),
            write_latency: Histogram::default(),
        }
    }
}

/// One second of the run, as charts draw it. Rates are per second over
/// the interval; latencies are that interval's own percentiles of
/// customer calls answered in it.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TimelinePoint {
    /// Seconds from the start of the measured window (negative before).
    pub t: f64,
    /// Phase at the interval's end.
    pub phase: Phase,
    /// Successful customer reads per second.
    pub reads_per_second: f64,
    /// Successful customer orders and tips per second.
    pub writes_per_second: f64,
    /// Successful drone calls per second.
    pub worker_per_second: f64,
    /// Logical calls that failed, per second.
    pub failed_calls_per_second: f64,
    /// Attempts that failed (and may have been retried), per second.
    pub failed_attempts_per_second: f64,
    /// Retry attempts per second.
    pub retries_per_second: f64,
    /// Customer read latency, p50 and p99, in ms.
    pub read_p50: Option<f64>,
    /// Customer read p99.
    pub read_p99: Option<f64>,
    /// Customer write latency, p50, in ms.
    pub write_p50: Option<f64>,
    /// Customer write p99.
    pub write_p99: Option<f64>,
}

/// The counters a chaos event collects between a kill and shortly after
/// the first write acknowledged afterwards.
#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Burst {
    /// Attempts that failed in the window.
    pub failed_attempts: u64,
    /// Retry attempts sent in the window.
    pub retries: u64,
    /// Logical calls that failed in the window.
    pub failed_calls: u64,
    /// Failed attempts by kind.
    pub errors: BTreeMap<String, u64>,
}

struct Inner {
    phase: Phase,
    phases: BTreeMap<Phase, PhaseStats>,
    interval: Interval,
    timeline: Vec<TimelinePoint>,
    load_started: Option<Instant>,
    burst: Option<Burst>,
}

/// The run's accounting, shared by every task. A group's recorder has
/// the run's as its parent: every attempt and call it records counts
/// in both, so the run's figures are the groups' merged by bucket and
/// each group keeps its own.
pub struct Recorder {
    inner: Mutex<Inner>,
    parent: Option<std::sync::Arc<Recorder>>,
}

impl Default for Recorder {
    fn default() -> Self {
        Recorder::new()
    }
}

impl Recorder {
    /// A recorder in the setup phase.
    pub fn new() -> Self {
        Recorder::build(None)
    }

    /// A recorder whose records also count in `parent`.
    pub fn child(parent: &std::sync::Arc<Recorder>) -> Self {
        Recorder::build(Some(parent.clone()))
    }

    fn build(parent: Option<std::sync::Arc<Recorder>>) -> Self {
        let now = Instant::now();
        let mut phases = BTreeMap::new();
        phases.insert(
            Phase::Setup,
            PhaseStats {
                started: now,
                ended: None,
                ops: BTreeMap::new(),
            },
        );
        Recorder {
            inner: Mutex::new(Inner {
                phase: Phase::Setup,
                phases,
                interval: Interval::new(now),
                timeline: Vec::new(),
                load_started: None,
                burst: None,
            }),
            parent,
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Enter `phase` at `now`, ending the previous one there.
    pub fn enter(&self, phase: Phase, now: Instant) {
        let mut inner = self.lock();
        let previous = inner.phase;
        if let Some(stats) = inner.phases.get_mut(&previous) {
            stats.ended = Some(now);
        }
        inner.phase = phase;
        inner.phases.insert(
            phase,
            PhaseStats {
                started: now,
                ended: None,
                ops: BTreeMap::new(),
            },
        );
        if phase == Phase::Load {
            inner.load_started = Some(now);
        }
    }

    /// The current phase.
    pub fn phase(&self) -> Phase {
        self.lock().phase
    }

    /// When the measured window began.
    pub fn load_started(&self) -> Option<Instant> {
        self.lock().load_started
    }

    /// Record one attempt of `op`: its latency, and the error kind when
    /// it failed. `retry` marks every attempt after a call's first.
    pub fn attempt(&self, op: &str, class: Class, ms: f64, error: Option<&str>, retry: bool) {
        self.attempt_here(op, class, ms, error, retry);
        if let Some(parent) = &self.parent {
            parent.attempt(op, class, ms, error, retry);
        }
    }

    fn attempt_here(&self, op: &str, class: Class, ms: f64, error: Option<&str>, retry: bool) {
        let mut inner = self.lock();
        let inner = &mut *inner;
        let phase = inner.phase;
        let stats = op_stats(&mut inner.phases, phase, op, class);
        stats.attempts += 1;
        stats.retries += u64::from(retry);
        stats.attempt_latency.record(ms);
        if let Some(kind) = error {
            stats.attempt_failures += 1;
            *stats.errors.entry(kind.to_string()).or_default() += 1;
            inner.interval.failed_attempts += 1;
        }
        inner.interval.retries += u64::from(retry);
        if let Some(burst) = &mut inner.burst {
            burst.retries += u64::from(retry);
            if let Some(kind) = error {
                burst.failed_attempts += 1;
                *burst.errors.entry(kind.to_string()).or_default() += 1;
            }
        }
    }

    /// Record where an answered attempt of `op` spent its time on the
    /// server, as its `Server-Timing` header said.
    pub fn stages(&self, op: &str, class: Class, stages: &[(String, f64)]) {
        if stages.is_empty() {
            return;
        }
        {
            let mut inner = self.lock();
            let inner = &mut *inner;
            let phase = inner.phase;
            let stats = op_stats(&mut inner.phases, phase, op, class);
            for (stage, ms) in stages {
                stats.stages.entry(stage.clone()).or_default().record(*ms);
            }
        }
        if let Some(parent) = &self.parent {
            parent.stages(op, class, stages);
        }
    }

    /// Record one logical call of `op` answered after `ms`.
    pub fn call(&self, op: &str, class: Class, ms: f64, ok: bool) {
        self.call_here(op, class, ms, ok);
        if let Some(parent) = &self.parent {
            parent.call(op, class, ms, ok);
        }
    }

    fn call_here(&self, op: &str, class: Class, ms: f64, ok: bool) {
        let mut inner = self.lock();
        let inner = &mut *inner;
        let phase = inner.phase;
        let stats = op_stats(&mut inner.phases, phase, op, class);
        stats.calls += 1;
        if ok {
            stats.completed += 1;
            stats.latency.record(ms);
        } else {
            stats.failed += 1;
        }
        let interval = &mut inner.interval;
        if !ok {
            interval.failed_calls += 1;
            if let Some(burst) = &mut inner.burst {
                burst.failed_calls += 1;
            }
            return;
        }
        match class {
            Class::Read => {
                interval.reads += 1;
                interval.read_latency.record(ms);
            }
            Class::Mutation => {
                interval.mutations += 1;
                interval.write_latency.record(ms);
            }
            Class::Worker => interval.worker += 1,
            Class::Replay | Class::Control => {}
        }
    }

    /// Start collecting a chaos burst.
    pub fn begin_burst(&self) {
        self.lock().burst = Some(Burst::default());
    }

    /// Stop collecting the burst and return it.
    pub fn end_burst(&self) -> Burst {
        self.lock().burst.take().unwrap_or_default()
    }

    /// Close the current one-second interval at `now`, append it to the
    /// timeline and return it.
    pub fn tick(&self, now: Instant) -> TimelinePoint {
        let mut inner = self.lock();
        let inner = &mut *inner;
        let interval = std::mem::replace(&mut inner.interval, Interval::new(now));
        let seconds = now
            .saturating_duration_since(interval.started)
            .as_secs_f64()
            .max(1e-3);
        let t = match inner.load_started {
            Some(start) if now >= start => now.duration_since(start).as_secs_f64(),
            Some(start) => -start.duration_since(now).as_secs_f64(),
            None => 0.0,
        };
        let rate = |count: u64| count as f64 / seconds;
        let point = TimelinePoint {
            t,
            phase: inner.phase,
            reads_per_second: rate(interval.reads),
            writes_per_second: rate(interval.mutations),
            worker_per_second: rate(interval.worker),
            failed_calls_per_second: rate(interval.failed_calls),
            failed_attempts_per_second: rate(interval.failed_attempts),
            retries_per_second: rate(interval.retries),
            read_p50: interval.read_latency.percentile(50.0),
            read_p99: interval.read_latency.percentile(99.0),
            write_p50: interval.write_latency.percentile(50.0),
            write_p99: interval.write_latency.percentile(99.0),
        };
        inner.timeline.push(point.clone());
        point
    }

    /// The timeline so far.
    pub fn timeline(&self) -> Vec<TimelinePoint> {
        self.lock().timeline.clone()
    }

    /// The last `count` timeline points.
    pub fn recent(&self, count: usize) -> Vec<TimelinePoint> {
        let inner = self.lock();
        let from = inner.timeline.len().saturating_sub(count);
        inner.timeline[from..].to_vec()
    }

    /// Every phase's summary, ended at `now` if still open.
    pub fn phases(&self, now: Instant) -> Vec<PhaseSummary> {
        let inner = self.lock();
        Phase::ALL
            .iter()
            .filter_map(|phase| inner.phases.get(phase).map(|stats| (phase, stats)))
            .map(|(phase, stats)| summarize(*phase, stats, now))
            .collect()
    }

    /// Successful customer calls so far in the load phase, for the live
    /// status.
    pub fn load_completed(&self) -> u64 {
        let inner = self.lock();
        inner.phases.get(&Phase::Load).map_or(0, |stats| {
            stats
                .ops
                .values()
                .filter(|op| op.class.is_customer())
                .map(|op| op.completed)
                .sum()
        })
    }
}

fn op_stats<'a>(
    phases: &'a mut BTreeMap<Phase, PhaseStats>,
    phase: Phase,
    op: &str,
    class: Class,
) -> &'a mut OpStats {
    let stats = phases.entry(phase).or_insert_with(|| PhaseStats {
        started: Instant::now(),
        ended: None,
        ops: BTreeMap::new(),
    });
    if !stats.ops.contains_key(op) {
        stats.ops.insert(op.to_string(), OpStats::new(class));
    }
    stats.ops.get_mut(op).expect("inserted above")
}

/// One operation's totals in one phase.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OpSummary {
    /// Whose work it is.
    pub class: Class,
    /// Logical calls.
    pub calls: u64,
    /// Calls that succeeded.
    pub completed: u64,
    /// Calls that failed.
    pub failed: u64,
    /// Successful calls per second of the phase.
    pub per_second: f64,
    /// Latency of successful logical calls, retries included.
    pub latency: HistogramSnapshot,
    /// Attempts sent.
    pub attempts: u64,
    /// Attempts that failed.
    pub attempt_failures: u64,
    /// Attempts after a call's first.
    pub retries: u64,
    /// Latency of single attempts, failed ones included.
    pub attempt_latency: Percentiles,
    /// Failed attempts by kind.
    pub errors: BTreeMap<String, u64>,
    /// Where answered attempts spent their time on the server that
    /// answered, from its `Server-Timing` header: stage to histogram
    /// (ms), every attempt, unsampled.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub stages: BTreeMap<String, HistogramSnapshot>,
}

/// One phase's totals.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PhaseSummary {
    /// Which phase.
    pub phase: Phase,
    /// Its length, in ms.
    pub duration_ms: f64,
    /// Every operation seen in it.
    pub operations: BTreeMap<String, OpSummary>,
}

fn summarize(phase: Phase, stats: &PhaseStats, now: Instant) -> PhaseSummary {
    let ended = stats.ended.unwrap_or(now);
    let duration_ms = ended.saturating_duration_since(stats.started).as_secs_f64() * 1000.0;
    let seconds = (duration_ms / 1000.0).max(1e-9);
    PhaseSummary {
        phase,
        duration_ms,
        operations: stats
            .ops
            .iter()
            .map(|(name, op)| {
                (
                    name.clone(),
                    OpSummary {
                        class: op.class,
                        calls: op.calls,
                        completed: op.completed,
                        failed: op.failed,
                        per_second: op.completed as f64 / seconds,
                        latency: op.latency.snapshot(),
                        attempts: op.attempts,
                        attempt_failures: op.attempt_failures,
                        retries: op.retries,
                        attempt_latency: op.attempt_latency.percentiles(),
                        errors: op.errors.clone(),
                        stages: op
                            .stages
                            .iter()
                            .map(|(stage, histogram)| (stage.clone(), histogram.snapshot()))
                            .collect(),
                    },
                )
            })
            .collect(),
    }
}

/// Goodput and the customer mix over the measured window.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Goodput {
    /// What is counted.
    pub definition: &'static str,
    /// The measured window, in ms.
    pub duration_ms: f64,
    /// Primary customer calls made.
    pub calls: u64,
    /// Those that succeeded.
    pub completed: u64,
    /// Those that failed.
    pub failed: u64,
    /// Successful primary customer calls per second.
    pub per_second: f64,
    /// Reads: calls, completions, failures, share of completions.
    pub reads: Mix,
    /// Orders and tips.
    pub mutations: Mix,
    /// Latency of successful customer reads.
    pub read_latency: HistogramSnapshot,
    /// Latency of successful customer orders and tips.
    pub write_latency: HistogramSnapshot,
    /// Successful calls in the window left out: drones.
    pub excluded_worker_completions: u64,
    /// Successful calls in the window left out: replay probes.
    pub excluded_replay_completions: u64,
    /// Successful calls in the window left out: control traffic.
    pub excluded_control_completions: u64,
}

/// One side of the customer mix.
#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Mix {
    /// Calls made.
    pub calls: u64,
    /// Calls that succeeded.
    pub completed: u64,
    /// Calls that failed.
    pub failed: u64,
    /// Share of all successful customer calls.
    pub fraction: Option<f64>,
}

/// The goodput definition, as reports print it.
pub const GOODPUT_DEFINITION: &str = "Successful primary customer logical calls (store reads, orders, tips) per second of the measured load window, final in-flight tails included; excludes drone traffic, receipt-replay probes, setup, warmup, drain and audit calls, and every failed call. A call that succeeded after retries counts once.";

/// Goodput over the load phase. Latencies merge every customer
/// operation's histogram bucket by bucket.
pub fn goodput(load: &PhaseSummary) -> Goodput {
    let mut reads = Mix::default();
    let mut mutations = Mix::default();
    let mut read_latency = Histogram::default();
    let mut write_latency = Histogram::default();
    let (mut worker, mut replay, mut control) = (0, 0, 0);
    for op in load.operations.values() {
        let histogram = Histogram::from_snapshot(&op.latency).unwrap_or_default();
        match op.class {
            Class::Read => {
                reads.calls += op.calls;
                reads.completed += op.completed;
                reads.failed += op.failed;
                read_latency.merge(&histogram);
            }
            Class::Mutation => {
                mutations.calls += op.calls;
                mutations.completed += op.completed;
                mutations.failed += op.failed;
                write_latency.merge(&histogram);
            }
            Class::Worker => worker += op.completed,
            Class::Replay => replay += op.completed,
            Class::Control => control += op.completed,
        }
    }
    let completed = reads.completed + mutations.completed;
    if completed > 0 {
        reads.fraction = Some(reads.completed as f64 / completed as f64);
        mutations.fraction = Some(mutations.completed as f64 / completed as f64);
    }
    let seconds = load.duration_ms / 1000.0;
    Goodput {
        definition: GOODPUT_DEFINITION,
        duration_ms: load.duration_ms,
        calls: reads.calls + mutations.calls,
        completed,
        failed: reads.failed + mutations.failed,
        per_second: if seconds > 0.0 {
            completed as f64 / seconds
        } else {
            0.0
        },
        reads,
        mutations,
        read_latency: read_latency.snapshot(),
        write_latency: write_latency.snapshot(),
        excluded_worker_completions: worker,
        excluded_replay_completions: replay,
        excluded_control_completions: control,
    }
}

/// Failed attempts by kind across every operation of a phase.
pub fn errors_by_kind(phase: &PhaseSummary) -> BTreeMap<String, u64> {
    let mut errors = BTreeMap::new();
    for op in phase.operations.values() {
        for (kind, count) in &op.errors {
            *errors.entry(kind.clone()).or_default() += count;
        }
    }
    errors
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn load_phase(recorder: &Recorder, start: Instant, seconds: u64) -> PhaseSummary {
        recorder.enter(Phase::Load, start);
        let end = start + Duration::from_secs(seconds);
        recorder.enter(Phase::Drain, end);
        recorder
            .phases(end)
            .into_iter()
            .find(|phase| phase.phase == Phase::Load)
            .unwrap()
    }

    #[test]
    fn goodput_counts_successful_primary_customer_calls_only() {
        let recorder = Recorder::new();
        let start = Instant::now();
        recorder.enter(Phase::Load, start);
        // 6 reads, 3 orders, 1 tip succeed; one read and one order fail.
        for _ in 0..6 {
            recorder.call("pizza.shop.local", Class::Read, 1.0, true);
        }
        recorder.call("pizza.shop.local", Class::Read, 1.0, false);
        for _ in 0..3 {
            recorder.call("pizza.order", Class::Mutation, 10.0, true);
        }
        recorder.call("pizza.order", Class::Mutation, 10.0, false);
        recorder.call("pizza.tip", Class::Mutation, 10.0, true);
        // A retried order: two attempts, one logical success.
        recorder.attempt(
            "pizza.order",
            Class::Mutation,
            2000.0,
            Some("timeout"),
            false,
        );
        recorder.attempt("pizza.order", Class::Mutation, 5.0, None, true);
        // Excluded: drones, replays, control.
        for _ in 0..50 {
            recorder.call("pizza.deliver", Class::Worker, 3.0, true);
        }
        recorder.call("pizza.order.replay", Class::Replay, 3.0, true);
        recorder.call("pizza.world", Class::Control, 3.0, true);
        let end = start + Duration::from_secs(2);
        recorder.enter(Phase::Drain, end);
        // Drain traffic never counts.
        recorder.call("pizza.shop.local", Class::Read, 1.0, true);
        let load = recorder
            .phases(end)
            .into_iter()
            .find(|phase| phase.phase == Phase::Load)
            .unwrap();
        let goodput = goodput(&load);
        assert_eq!(goodput.completed, 10);
        assert_eq!(goodput.failed, 2);
        assert_eq!(goodput.calls, 12);
        assert!((goodput.per_second - 5.0).abs() < 1e-9);
        assert_eq!(goodput.reads.completed, 6);
        assert_eq!(goodput.mutations.completed, 4);
        assert!((goodput.reads.fraction.unwrap() - 0.6).abs() < 1e-9);
        assert_eq!(goodput.excluded_worker_completions, 50);
        assert_eq!(goodput.excluded_replay_completions, 1);
        assert_eq!(goodput.excluded_control_completions, 1);
        assert_eq!(goodput.write_latency.samples, 4);
        let order = &load.operations["pizza.order"];
        assert_eq!(order.attempts, 2);
        assert_eq!(order.retries, 1);
        assert_eq!(order.errors["timeout"], 1);
        assert_eq!(errors_by_kind(&load)["timeout"], 1);
    }

    #[test]
    fn group_recorders_count_in_the_run_and_keep_their_own() {
        let run = std::sync::Arc::new(Recorder::new());
        let east = Recorder::child(&run);
        let west = Recorder::child(&run);
        let start = Instant::now();
        for recorder in [&*run, &east, &west] {
            recorder.enter(Phase::Load, start);
        }
        east.call("pizza.order", Class::Mutation, 10.0, true);
        east.attempt("pizza.order", Class::Mutation, 10.0, Some("timeout"), false);
        west.call("pizza.shop.local", Class::Read, 1.0, true);
        west.call("pizza.shop.local", Class::Read, 3.0, true);
        west.begin_burst();
        west.attempt("pizza.tip", Class::Mutation, 1.0, Some("transport"), false);
        let burst = west.end_burst();
        let end = start + Duration::from_secs(1);
        let load = |recorder: &Recorder| {
            recorder
                .phases(end)
                .into_iter()
                .find(|phase| phase.phase == Phase::Load)
                .unwrap()
        };
        assert_eq!(goodput(&load(&east)).completed, 1);
        assert_eq!(goodput(&load(&west)).completed, 2);
        let all = goodput(&load(&run));
        assert_eq!(all.completed, 3);
        assert_eq!(all.read_latency.samples, 2);
        assert_eq!(errors_by_kind(&load(&run))["timeout"], 1);
        // A group's burst is its own.
        assert_eq!(burst.failed_attempts, 1);
        assert!(!errors_by_kind(&load(&east)).contains_key("transport"));
    }

    #[test]
    fn server_timing_stages_record_per_operation() {
        let parsed = server_timing(
            "forward;dur=1.5, fence;dur=0.2, eval;dur=2.0, commit;dur=10.1, total;dur=14.0, node;desc=2, leader;desc=1, bad;dur=x",
        );
        assert_eq!(
            parsed,
            vec![
                ("forward".to_string(), 1.5),
                ("fence".to_string(), 0.2),
                ("eval".to_string(), 2.0),
                ("commit".to_string(), 10.1),
                ("total".to_string(), 14.0),
            ]
        );
        let run = std::sync::Arc::new(Recorder::new());
        let group = Recorder::child(&run);
        let start = Instant::now();
        run.enter(Phase::Load, start);
        group.enter(Phase::Load, start);
        group.stages("pizza.order", Class::Mutation, &parsed);
        group.stages(
            "pizza.order",
            Class::Mutation,
            &server_timing("commit;dur=20, total;dur=21"),
        );
        let load = run
            .phases(start + Duration::from_secs(1))
            .into_iter()
            .find(|phase| phase.phase == Phase::Load)
            .unwrap();
        let stages = &load.operations["pizza.order"].stages;
        assert_eq!(stages["commit"].samples, 2);
        assert_eq!(stages["forward"].samples, 1);
        assert!(!stages.contains_key("node"));
    }

    #[test]
    fn empty_load_has_no_rate() {
        let recorder = Recorder::new();
        let load = load_phase(&recorder, Instant::now(), 0);
        let goodput = goodput(&load);
        assert_eq!(goodput.completed, 0);
        assert_eq!(goodput.per_second, 0.0);
        assert_eq!(goodput.reads.fraction, None);
    }

    #[test]
    fn ticks_close_intervals_and_bursts_collect_between_marks() {
        let recorder = Recorder::new();
        let start = Instant::now();
        recorder.enter(Phase::Load, start);
        recorder.call("pizza.order", Class::Mutation, 40.0, true);
        recorder.call("pizza.shop", Class::Read, 2.0, true);
        recorder.call("pizza.shop", Class::Read, 4.0, true);
        let point = recorder.tick(start + Duration::from_secs(2));
        assert!((point.t - 2.0).abs() < 1e-9);
        // The interval began when the recorder did, a hair before `start`.
        assert!((point.reads_per_second - 1.0).abs() < 1e-3);
        assert!((point.writes_per_second - 0.5).abs() < 1e-3);
        assert_eq!(point.write_p99, Some(40.0));
        // The next interval starts empty.
        let next = recorder.tick(start + Duration::from_secs(3));
        assert_eq!(next.reads_per_second, 0.0);
        assert_eq!(next.read_p99, None);
        recorder.attempt("pizza.order", Class::Mutation, 1.0, Some("before"), false);
        recorder.begin_burst();
        recorder.attempt(
            "pizza.order",
            Class::Mutation,
            1.0,
            Some("transport"),
            false,
        );
        recorder.attempt("pizza.order", Class::Mutation, 1.0, Some("timeout"), true);
        recorder.attempt("pizza.order", Class::Mutation, 1.0, None, true);
        let burst = recorder.end_burst();
        recorder.attempt("pizza.order", Class::Mutation, 1.0, Some("after"), false);
        assert_eq!(burst.failed_attempts, 2);
        assert_eq!(burst.retries, 2);
        assert_eq!(burst.errors.len(), 2);
        assert_eq!(recorder.timeline().len(), 2);
    }
}
