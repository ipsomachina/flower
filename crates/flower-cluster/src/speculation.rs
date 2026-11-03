//! How the writer speculates: its settings, the wave-width policy, the
//! conflict prediction that steers hot calls serially, the threads waves
//! prepare on, and the counters `/v1/metrics` reports (see
//! [`crate::writer`] for the waves themselves, and
//! [`flower_engine::speculation`] for what makes a candidate hold).
//!
//! A wave starts two wide, doubles after a wave with no conflict, keeps its
//! width while a partly conflicting wave still reuses more candidates than it
//! rejects, and otherwise halves; at width one it prepares serially for a
//! cooldown that doubles on every repeat, up to the writer queue, and an
//! accepted probe resets.
//!
//! Prediction is by method and by record: a decaying share of each method's
//! recent candidates whose certificates failed, and which of its arguments
//! names the record they failed on. A call of a hot method whose record an
//! earlier call in the same wave also names (or any call of a hot method
//! whose conflicts no argument names) is placed serially in its turn instead
//! of speculated: a candidate that will fail costs an evaluation, and its
//! conflict narrows the wave for every other call. One exposed call in
//! [`PROBE_EVERY`] ([`PROBE_EVERY_KEYED`] by record) still speculates, so a
//! method that stops conflicting is learned again. Placing a call serially
//! changes nothing it computes.

use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError, mpsc};

use flower_engine::speculation::{ConflictClass, ConflictRow, record_key};
use flower_engine::storage::SnapshotTable;
use flower_sdk::Value;

/// How the writer prepares queued mutations.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SpeculationSettings {
    /// Threads a wave of mutations prepares on, the writer's own
    /// included, and the widest a wave gets; 1 prepares every write
    /// serially on the staged state (`FLOWER_WRITER_PREPARATION_WORKERS`).
    pub workers: usize,
    /// Whether conflict prediction places calls of methods (and
    /// records) whose candidates keep conflicting serially
    /// (`FLOWER_WRITER_CONFLICT_PREDICTION`).
    pub prediction: bool,
}

/// The most threads a writer prepares on by default: every group's
/// writer keeps its own pool.
pub const MAX_DEFAULT_WORKERS: usize = 8;

/// Default threads: this machine's cores, at most [`MAX_DEFAULT_WORKERS`].
/// Measured on a quiet machine with fsync on (see the specification,
/// "Speculative preparation"), speculation with prediction prepared 1.9 times
/// the independent writes of a serial writer at 8 workers and never fewer
/// than it under one hot key.
pub fn default_workers() -> usize {
    std::thread::available_parallelism()
        .map_or(1, std::num::NonZeroUsize::get)
        .min(MAX_DEFAULT_WORKERS)
}

impl Default for SpeculationSettings {
    fn default() -> Self {
        SpeculationSettings {
            workers: default_workers(),
            prediction: true,
        }
    }
}

impl SpeculationSettings {
    /// Whether waves speculate at all.
    pub fn speculates(&self) -> bool {
        self.workers > 1
    }
}

/// Methods tracked per writer; calls of others always speculate.
pub const PREDICTED_METHODS: usize = 64;
/// The longest record key prediction compares; longer keys are not
/// predicted by.
const MAX_KEY_BYTES: usize = 256;
/// Share (of 256) of conflicting candidates from which calls are placed
/// serially.
const HOT: u16 = 128;
/// Candidates observed before a method can be hot.
const MIN_OBSERVED: u8 = 4;
/// One call of a hot method in this many is speculated all the same.
pub const PROBE_EVERY: u8 = 8;
/// The same for calls exposed on a learned record: on a hot record
/// nearly every probe fails, so they probe four times more rarely.
pub const PROBE_EVERY_KEYED: u8 = 32;

/// What a queued mutation names, for prediction and diagnostics.
#[derive(Clone, Debug)]
pub struct Speculable {
    /// Its database.
    pub db: Vec<u8>,
    /// Its module.
    pub module: String,
    /// Its method (an alias under its own).
    pub name: String,
    /// Its arguments.
    pub args: Value,
}

impl Speculable {
    fn method(&self) -> String {
        format!(
            "{}\u{0}{}\u{0}{}",
            String::from_utf8_lossy(&self.db),
            self.module,
            self.name
        )
    }
}

/// Which record a method's conflicts are on, by its arguments: a
/// collection's row or a derived definition's outcome, whose key is the
/// argument `field` (a top-level property) or the whole arguments.
#[derive(Clone, Debug, PartialEq, Eq)]
struct KeyPath {
    table: SnapshotTable,
    id: u32,
    field: Option<String>,
}

#[derive(Default, Clone)]
struct Heat {
    /// Share of recent exposed candidates that conflicted, in 1/256ths,
    /// decaying by an eighth per candidate.
    score: u16,
    /// Exposed candidates observed, saturating at [`MIN_OBSERVED`].
    observed: u8,
    /// Calls placed serially since the last probe.
    placed: u8,
    /// The argument naming the record its last conflict was on, if one
    /// did.
    key: Option<KeyPath>,
}

/// Where a wave's slot stands in conflict prediction.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Placement {
    /// Placed serially rather than speculated.
    pub serial: bool,
    /// The record its method's learned path names for this call.
    pub key: Option<Vec<u8>>,
    /// Whether an earlier slot of the wave names the same record; `None`
    /// when its method has no path: every call is exposed.
    pub exposed: Option<bool>,
}

#[derive(Default)]
struct Prediction {
    methods: HashMap<String, Heat>,
}

impl Prediction {
    /// The record a call names, by its method's learned path.
    fn key(&self, method: &str, args: &Value) -> Option<Vec<u8>> {
        let path = self.methods.get(method)?.key.as_ref()?;
        let value = match &path.field {
            Some(field) => match args {
                Value::Object(map) => map.get(field)?,
                _ => return None,
            },
            None => args,
        };
        let bytes = record_key(value)?;
        let mut key = Vec::with_capacity(bytes.len() + 5);
        key.push(path.table.tag());
        key.extend_from_slice(&path.id.to_be_bytes());
        key.extend_from_slice(&bytes);
        (key.len() <= MAX_KEY_BYTES).then_some(key)
    }

    fn place_serially(&mut self, method: &str, exposed: Option<bool>) -> bool {
        if exposed == Some(false) {
            return false;
        }
        let Some(heat) = self.methods.get_mut(method) else {
            return false;
        };
        if heat.observed < MIN_OBSERVED || heat.score < HOT {
            return false;
        }
        let every = if exposed.is_some() {
            PROBE_EVERY_KEYED
        } else {
            PROBE_EVERY
        };
        heat.placed += 1;
        if heat.placed >= every {
            heat.placed = 0;
            return false;
        }
        true
    }

    /// Learn from a validated candidate: `conflicted` when its
    /// certificate failed, on `conflicts`. A candidate no earlier
    /// sibling shared a record with says nothing of contention on its
    /// record unless it conflicted all the same, which teaches a new
    /// path.
    fn observe(
        &mut self,
        method: &str,
        args: &Value,
        conflicted: bool,
        exposed: Option<bool>,
        conflicts: &[ConflictRow],
    ) {
        if !self.methods.contains_key(method) && self.methods.len() >= PREDICTED_METHODS {
            return;
        }
        let heat = self.methods.entry(method.to_owned()).or_default();
        if conflicted {
            heat.key = learn_key(args, conflicts);
        } else if exposed == Some(false) {
            return;
        }
        heat.score = heat.score - heat.score / 8 + if conflicted { 32 } else { 0 };
        heat.observed = heat.observed.saturating_add(1).min(MIN_OBSERVED);
    }
}

/// The first conflicting record or derived outcome whose key is the
/// call's whole arguments or one of their top-level properties, and
/// which.
fn learn_key(args: &Value, conflicts: &[ConflictRow]) -> Option<KeyPath> {
    conflicts.iter().find_map(|row| {
        // A key every call shares (a singleton) tells calls nothing apart.
        if row.key == b"null" || row.key.len() > MAX_KEY_BYTES {
            return None;
        }
        let field = if record_key(args).as_deref() == Some(row.key.as_slice()) {
            None
        } else {
            let Value::Object(map) = args else {
                return None;
            };
            let (field, _) = map
                .iter()
                .find(|(_, value)| record_key(value).as_deref() == Some(row.key.as_slice()))?;
            Some(field.clone())
        };
        Some(KeyPath {
            table: row.table,
            id: row.id,
            field,
        })
    })
}

/// The wave-width policy, with prediction when on.
pub(crate) struct Policy {
    limit: usize,
    width: usize,
    serial: usize,
    cooldown: usize,
    max_cooldown: usize,
    prediction: Option<Prediction>,
}

impl Policy {
    /// A policy for waves of at most `limit`, cooling down for at most
    /// `queue` calls.
    pub(crate) fn new(settings: SpeculationSettings, queue: usize) -> Self {
        let limit = settings.workers.max(1);
        Policy {
            limit,
            // Learn independence with a pair before occupying every worker.
            width: limit.min(2),
            serial: 0,
            cooldown: limit,
            max_cooldown: queue.max(limit),
            prediction: (settings.prediction && limit > 1).then(Prediction::default),
        }
    }

    /// Where a call joining a wave stands: the record its method's
    /// conflicts are on, whether `held` by an earlier slot of the wave,
    /// and whether it is placed serially.
    pub(crate) fn placement(
        &mut self,
        call: &Speculable,
        held: &dyn Fn(&[u8]) -> bool,
    ) -> Placement {
        let Some(prediction) = &mut self.prediction else {
            return Placement::default();
        };
        let method = call.method();
        let key = prediction.key(&method, &call.args);
        let exposed = key.as_deref().map(held);
        Placement {
            serial: prediction.place_serially(&method, exposed),
            key,
            exposed,
        }
    }

    /// Learn from a validated candidate (see [`Prediction::observe`]).
    pub(crate) fn observe_candidate(
        &mut self,
        call: &Speculable,
        conflicted: bool,
        exposed: Option<bool>,
        conflicts: &[ConflictRow],
    ) {
        if let Some(prediction) = &mut self.prediction {
            prediction.observe(&call.method(), &call.args, conflicted, exposed, conflicts);
        }
    }

    /// Whether calls prepare serially to cool down after conflicts.
    #[cfg(test)]
    pub(crate) fn cooling(&self) -> bool {
        self.limit > 1 && self.serial > 0
    }

    /// The width of the next wave: 1 while cooling down, consuming one
    /// serial call.
    pub(crate) fn width(&mut self) -> usize {
        if self.serial > 0 {
            self.serial -= 1;
            return 1;
        }
        self.width.max(2).min(self.limit)
    }

    /// The width a wave would have without cooling down.
    #[cfg(test)]
    pub(crate) fn target(&self) -> usize {
        self.width.max(2).min(self.limit)
    }

    fn accepted(&mut self) {
        self.width = self.width.saturating_mul(2).min(self.limit);
        self.cooldown = self.limit;
    }

    fn conflicted(&mut self) {
        self.width = (self.width / 2).max(1);
        if self.width == 1 {
            // Repeated conflicts make probes progressively rarer, up to
            // the writer queue; an accepted probe resets.
            self.serial = self.cooldown;
            self.cooldown = self.cooldown.saturating_mul(2).min(self.max_cooldown);
        }
    }

    /// Learn from a finished wave; whether it backed off.
    pub(crate) fn observe_wave(&mut self, reused: usize, conflicts: usize) -> bool {
        if conflicts == 0 {
            self.accepted();
        } else if reused > conflicts {
            // A partly conflicting wave still saves most evaluations:
            // keep its width without rewarding collisions with growth.
            self.serial = 0;
            self.cooldown = self.limit;
        } else {
            self.conflicted();
            return true;
        }
        false
    }
}

type Task = Box<dyn FnOnce() + Send>;

/// Threads waves prepare on, besides the writer's own.
pub(crate) struct Pool {
    tasks: mpsc::Sender<Task>,
    queue: Arc<Mutex<mpsc::Receiver<Task>>>,
    threads: usize,
}

impl Pool {
    /// A pool of `threads` threads, ending when it drops.
    pub(crate) fn new(threads: usize) -> Self {
        let (tasks, receiver) = mpsc::channel::<Task>();
        let queue = Arc::new(Mutex::new(receiver));
        for index in 0..threads {
            let queue = queue.clone();
            let spawned = std::thread::Builder::new()
                .name(format!("flower-speculate-{index}"))
                .spawn(move || {
                    loop {
                        let task = queue.lock().unwrap_or_else(PoisonError::into_inner).recv();
                        match task {
                            Ok(task) => task(),
                            Err(_) => return,
                        }
                    }
                });
            if let Err(error) = spawned {
                tracing::warn!(target: "flower::writer", %error, "a speculation thread did not start");
            }
        }
        Pool {
            tasks,
            queue,
            threads,
        }
    }

    /// How many threads it runs.
    pub(crate) fn threads(&self) -> usize {
        self.threads
    }

    /// Run `work` on every item, on the pool's threads and this one,
    /// and hand the items back in their order. A panic in `work` leaves
    /// its item as it was then.
    pub(crate) fn run<T: Send + 'static>(
        &self,
        items: Vec<T>,
        work: Arc<dyn Fn(&mut T) + Send + Sync>,
    ) -> Vec<T> {
        let count = items.len();
        let (done, results) = mpsc::channel();
        for (index, mut item) in items.into_iter().enumerate() {
            let (done, work) = (done.clone(), work.clone());
            let task: Task = Box::new(move || {
                let _ = std::panic::catch_unwind(AssertUnwindSafe(|| work(&mut item)));
                drop(work);
                let _ = done.send((index, item));
            });
            if let Err(mpsc::SendError(task)) = self.tasks.send(task) {
                // No thread left to take it: run it here.
                task();
            }
        }
        drop(done);
        drop(work);
        // Help while tasks wait; a pool thread blocked on the queue
        // holds its lock, and then there is nothing left to take.
        loop {
            let task = match self.queue.try_lock() {
                Ok(queue) => queue.try_recv().ok(),
                Err(_) => None,
            };
            match task {
                Some(task) => task(),
                None => break,
            }
        }
        let mut back: Vec<(usize, T)> = results.iter().take(count).collect();
        back.sort_by_key(|(index, _)| *index);
        back.into_iter().map(|(_, item)| item).collect()
    }
}

/// What the writer's speculation did since its member opened.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SpeculationStats {
    /// Waves started.
    pub waves: u64,
    /// Candidates prepared speculatively.
    pub candidates: u64,
    /// Candidates whose certificates held: committed as prepared.
    pub reused: u64,
    /// Candidates prepared again because a write before them since their
    /// snapshot touched what they read.
    pub conflicted: u64,
    /// Candidates prepared again because they read something no row
    /// names (retained history), or too much to track.
    pub untracked: u64,
    /// Candidates whose speculative preparation failed: prepared again.
    pub failed: u64,
    /// Conflicted candidates by the class of row they conflicted on (one
    /// candidate counts once per class).
    pub conflicts: ConflictCounts,
    /// Calls prediction placed serially.
    pub placed_serially: u64,
    /// Calls prepared serially while waves cooled down.
    pub cooldown_serial_calls: u64,
    /// Waves whose conflicts narrowed the next.
    pub backoffs: u64,
}

/// Conflicts by class of row ([`ConflictClass`]).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct ConflictCounts {
    /// Records.
    pub source: u64,
    /// Index entries.
    pub index: u64,
    /// Derived outcomes and their edges.
    pub cell: u64,
    /// Receipts.
    pub receipt: u64,
    /// Database metadata.
    pub meta: u64,
    /// Anything else.
    pub other: u64,
}

/// The writer's speculation counters.
#[derive(Default)]
pub(crate) struct Counters {
    pub(crate) waves: AtomicU64,
    pub(crate) candidates: AtomicU64,
    pub(crate) reused: AtomicU64,
    pub(crate) conflicted: AtomicU64,
    pub(crate) untracked: AtomicU64,
    pub(crate) failed: AtomicU64,
    conflicts: [AtomicU64; 6],
    pub(crate) placed: AtomicU64,
    pub(crate) cooldown: AtomicU64,
    pub(crate) backoffs: AtomicU64,
}

impl Counters {
    /// Count a conflict of `class`.
    pub(crate) fn conflict(&self, class: ConflictClass) {
        let index = ConflictClass::ALL
            .iter()
            .position(|known| *known == class)
            .unwrap_or(ConflictClass::ALL.len() - 1);
        self.conflicts[index].fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn stats(&self) -> SpeculationStats {
        let load = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        let [source, index, cell, receipt, meta, other] = &self.conflicts;
        SpeculationStats {
            waves: load(&self.waves),
            candidates: load(&self.candidates),
            reused: load(&self.reused),
            conflicted: load(&self.conflicted),
            untracked: load(&self.untracked),
            failed: load(&self.failed),
            conflicts: ConflictCounts {
                source: load(source),
                index: load(index),
                cell: load(cell),
                receipt: load(receipt),
                meta: load(meta),
                other: load(other),
            },
            placed_serially: load(&self.placed),
            cooldown_serial_calls: load(&self.cooldown),
            backoffs: load(&self.backoffs),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn settings(workers: usize) -> SpeculationSettings {
        SpeculationSettings {
            workers,
            prediction: true,
        }
    }

    #[test]
    fn useful_partial_waves_keep_width_but_hot_conflicts_back_off() {
        let mut policy = Policy::new(settings(8), 32);
        policy.observe_wave(2, 0);
        assert_eq!(policy.width(), 4);
        policy.observe_wave(3, 1);
        assert_eq!(policy.width(), 4);
        assert!(policy.observe_wave(1, 3));
        assert_eq!(policy.width(), 2);
        policy.observe_wave(1, 1);
        for _ in 0..8 {
            assert!(policy.cooling());
            assert_eq!(policy.width(), 1);
        }
        assert_eq!(policy.width(), 2, "the next probe after the cooldown");
    }

    #[test]
    fn cooldowns_double_up_to_the_queue_and_an_accepted_probe_resets() {
        let mut policy = Policy::new(settings(8), 32);
        for expected in [8, 16, 32, 32] {
            policy.conflicted();
            for _ in 0..expected {
                assert_eq!(policy.width(), 1);
            }
            assert_eq!(policy.width(), 2);
        }
        policy.accepted();
        policy.accepted();
        policy.accepted();
        assert_eq!(policy.width(), 8);
        let serial = Policy::new(settings(1), 64);
        assert_eq!(serial.target(), 1, "one worker never starts a wave");
    }

    fn call(shop: &str) -> Speculable {
        Speculable {
            db: b"db".to_vec(),
            module: "app".to_string(),
            name: "tip".to_string(),
            args: Value::Object(
                [
                    ("shop".to_string(), Value::String(shop.to_string())),
                    ("amount".to_string(), Value::Number(1.0)),
                ]
                .into_iter()
                .collect(),
            ),
        }
    }

    fn row(shop: &str) -> ConflictRow {
        ConflictRow {
            table: SnapshotTable::Records,
            db: b"db".to_vec(),
            id: 9,
            key: format!("\"{shop}\"").into_bytes(),
        }
    }

    #[test]
    fn prediction_learns_the_record_an_argument_names_and_places_only_exposed_calls() {
        let mut policy = Policy::new(settings(4), 64);
        let nothing_held = |_: &[u8]| false;
        // Not hot yet: speculated.
        assert!(!policy.placement(&call("a"), &nothing_held).serial);
        for _ in 0..6 {
            policy.observe_candidate(&call("a"), true, None, &[row("a")]);
        }
        // Hot, keyed by `shop`: a call on a record no earlier slot names
        // speculates; one on a record an earlier slot names is placed.
        let cold = policy.placement(&call("b"), &nothing_held);
        assert_eq!(cold.exposed, Some(false));
        assert!(!cold.serial);
        let key = cold.key.clone().unwrap();
        let held = move |candidate: &[u8]| candidate == key.as_slice();
        let placed = (0..PROBE_EVERY_KEYED as usize)
            .filter(|_| policy.placement(&call("b"), &held).serial)
            .count();
        assert_eq!(
            placed,
            PROBE_EVERY_KEYED as usize - 1,
            "one exposed call in {PROBE_EVERY_KEYED} still probes"
        );
        // Without prediction nothing is placed.
        let mut off = Policy::new(
            SpeculationSettings {
                workers: 4,
                prediction: false,
            },
            64,
        );
        for _ in 0..6 {
            off.observe_candidate(&call("a"), true, None, &[row("a")]);
        }
        assert!(!off.placement(&call("a"), &held_all).serial);
    }

    fn held_all(_: &[u8]) -> bool {
        true
    }

    #[test]
    fn methods_whose_conflicts_no_argument_names_are_placed_whole() {
        let mut policy = Policy::new(settings(4), 64);
        let singleton = ConflictRow {
            table: SnapshotTable::Derived,
            db: b"db".to_vec(),
            id: 3,
            key: b"null".to_vec(),
        };
        for _ in 0..6 {
            policy.observe_candidate(&call("a"), true, None, std::slice::from_ref(&singleton));
        }
        let placement = policy.placement(&call("z"), &|_: &[u8]| false);
        assert_eq!(placement.exposed, None);
        assert!(placement.serial);
    }

    #[test]
    fn pools_run_every_item_and_keep_their_order() {
        let pool = Pool::new(3);
        let doubled = pool.run((0..50u64).collect(), Arc::new(|item: &mut u64| *item *= 2));
        assert_eq!(doubled, (0..50u64).map(|n| n * 2).collect::<Vec<_>>());
        // A panicking item comes back as it was.
        let back = pool.run(
            vec![1u64, 2, 3],
            Arc::new(|item: &mut u64| {
                if *item == 2 {
                    panic!("boom");
                }
                *item += 10;
            }),
        );
        assert_eq!(back, vec![11, 2, 13]);
    }
}
