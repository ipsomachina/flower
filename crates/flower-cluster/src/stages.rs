//! Where the request being served spends its time, phase by phase, for
//! the server's `Server-Timing` header.
//!
//! The server runs each timed request inside [`scoped`] with a fresh
//! [`Recorder`]; the member's client paths add each phase that ran to
//! it: the fresh-read proof, the wait for admission, the wait in the
//! leader's writer queue, evaluation, and the commit through Raft, with
//! the commit's append, replication and apply from the member's commit
//! marks ([`crate::telemetry::CommitMarks`]).
//! Outside a scope recording does nothing. The recorder is task-local,
//! so work the request awaits records and work it spawns does not: the
//! writer runs in its own task and hands each write's timings back to
//! the request that submitted it.

use std::future::Future;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

/// One timed phase of a request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stage {
    /// Proving a fresh read: the leader's lease or quorum round, or a
    /// follower's read fence.
    Fence,
    /// Waiting for an identical query in flight to land (see
    /// [`crate::flight`]).
    Join,
    /// Waiting for an admission slot.
    Admission,
    /// Waiting in the leader's writer queue for the write's turn to
    /// prepare (see [`crate::writer`]).
    Queue,
    /// Evaluating the query, or preparing the write.
    Eval,
    /// Committing the write through Raft: append, replication to a
    /// quorum and apply. For a write through the writer, everything
    /// after its turn but its preparation: waiting for its group's
    /// batch to go, and that batch's commit.
    Commit,
    /// The part of `commit` from the entry's submission to Raft until
    /// it was durable in the leader's own log (its append and fsync).
    Append,
    /// The part of `commit` from the entry's submission until a quorum
    /// committed it (when the leader began applying it). It overlaps
    /// `append`: followers replicate while the leader appends.
    Replicate,
    /// Applying the committed entry.
    Apply,
}

impl Stage {
    /// Every stage, in `Server-Timing` order.
    pub const ALL: [Stage; 9] = [
        Stage::Fence,
        Stage::Join,
        Stage::Admission,
        Stage::Queue,
        Stage::Eval,
        Stage::Commit,
        Stage::Append,
        Stage::Replicate,
        Stage::Apply,
    ];

    /// The stage's `Server-Timing` metric name.
    pub fn name(self) -> &'static str {
        match self {
            Stage::Fence => "fence",
            Stage::Join => "join",
            Stage::Admission => "admission",
            Stage::Queue => "queue",
            Stage::Eval => "eval",
            Stage::Commit => "commit",
            Stage::Append => "append",
            Stage::Replicate => "replicate",
            Stage::Apply => "apply",
        }
    }

    fn index(self) -> usize {
        self as usize
    }
}

/// Add a commit's phases (see [`crate::telemetry::CommitMarks`]) to the
/// request this task serves, each that was measured.
pub fn add_commit(phases: &crate::telemetry::CommitPhases) {
    for (stage, elapsed) in [
        (Stage::Append, phases.append),
        (Stage::Replicate, phases.replicate),
        (Stage::Apply, phases.apply),
    ] {
        if let Some(elapsed) = elapsed {
            add(stage, elapsed);
        }
    }
}

/// One request's phase timings.
#[derive(Debug, Default)]
pub struct Recorder {
    phases: Mutex<[Option<Duration>; Stage::ALL.len()]>,
    /// The query cache's answer: 0 none, 1 a hit, 2 a miss.
    cache: AtomicU8,
}

impl Recorder {
    /// A recorder with nothing measured.
    pub fn new() -> Arc<Self> {
        Arc::default()
    }

    /// Whether the query cache answered the request's query (`true`) or
    /// it evaluated (`false`); `None` when no lookup ran.
    pub fn cache(&self) -> Option<bool> {
        match self.cache.load(Ordering::Relaxed) {
            1 => Some(true),
            2 => Some(false),
            _ => None,
        }
    }

    /// Add `elapsed` to a stage; repeats (retries) accumulate.
    pub fn add(&self, stage: Stage, elapsed: Duration) {
        let mut phases = self.phases.lock().unwrap_or_else(PoisonError::into_inner);
        let phase = &mut phases[stage.index()];
        *phase = Some(phase.unwrap_or_default().saturating_add(elapsed));
    }

    /// Each stage that ran, with its time, in `Server-Timing` order.
    pub fn phases(&self) -> Vec<(Stage, Duration)> {
        let phases = self.phases.lock().unwrap_or_else(PoisonError::into_inner);
        Stage::ALL
            .iter()
            .filter_map(|stage| phases[stage.index()].map(|elapsed| (*stage, elapsed)))
            .collect()
    }
}

tokio::task_local! {
    static RECORDER: Arc<Recorder>;
}

/// Run `future` recording its phases into `recorder`.
pub async fn scoped<F: Future>(recorder: Arc<Recorder>, future: F) -> F::Output {
    RECORDER.scope(recorder, future).await
}

/// Add `elapsed` to `stage` of the request this task serves, if timed.
pub fn add(stage: Stage, elapsed: Duration) {
    let _ = RECORDER.try_with(|recorder| recorder.add(stage, elapsed));
}

/// The query cache answered the query of the request this task serves
/// (`hit`), or it evaluated, if timed.
pub fn cache(hit: bool) {
    let _ = RECORDER.try_with(|recorder| {
        recorder
            .cache
            .store(if hit { 1 } else { 2 }, Ordering::Relaxed);
    });
}

/// Run `future`, adding its wall time to `stage`.
pub async fn timed<F: Future>(stage: Stage, future: F) -> F::Output {
    let started = Instant::now();
    let output = future.await;
    add(stage, started.elapsed());
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn phases_record_inside_a_scope_only_and_accumulate() {
        add(Stage::Eval, Duration::from_millis(5));
        let recorder = Recorder::new();
        scoped(recorder.clone(), async {
            add(Stage::Commit, Duration::from_millis(3));
            add(Stage::Admission, Duration::from_millis(1));
            add(Stage::Commit, Duration::from_millis(2));
            timed(Stage::Eval, async {}).await;
            // Spawned work is not the request's.
            tokio::spawn(async { add(Stage::Fence, Duration::from_secs(1)) })
                .await
                .unwrap();
        })
        .await;
        let phases = recorder.phases();
        let names: Vec<&str> = phases.iter().map(|(stage, _)| stage.name()).collect();
        assert_eq!(names, ["admission", "eval", "commit"]);
        assert_eq!(phases[2].1, Duration::from_millis(5));
        assert_eq!(recorder.cache(), None);
    }

    #[tokio::test]
    async fn a_commit_adds_its_parts_and_a_query_its_cache_outcome() {
        let recorder = Recorder::new();
        scoped(recorder.clone(), async {
            add_commit(&crate::telemetry::CommitPhases {
                append: Some(Duration::from_millis(2)),
                replicate: Some(Duration::from_millis(3)),
                apply: None,
            });
            cache(true);
        })
        .await;
        let names: Vec<&str> = recorder
            .phases()
            .iter()
            .map(|(stage, _)| stage.name())
            .collect();
        // Only the parts measured.
        assert_eq!(names, ["append", "replicate"]);
        assert_eq!(recorder.cache(), Some(true));
    }
}
