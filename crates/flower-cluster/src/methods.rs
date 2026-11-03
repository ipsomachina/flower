//! What each application method costs this process, for `/v1/metrics`
//! `methods`, as v1's `GET /admin/resources` `methods`: per method
//! (database, module and name as the caller named it), the mutations
//! answered by outcome and refused by why, their wait in the writer
//! queue, their evaluations (wall time and the evaluating thread's CPU
//! time beside it: the difference is time spent waiting for a core or
//! a page) and their time from submission to answer; the queries
//! evaluated (CPU time) and answered by the query cache. Always on, no
//! exporter, and no lock: a recording loads the published rows (one
//! atomic step), makes three borrowed map lookups and updates atomic
//! counters in its thread's shard of the row, with no allocation once a
//! method has its row (v1 locks its table on every recording). Only
//! adding a row, once per method and at most [`MAX_METHODS`] times in a
//! process's life, takes a lock and publishes a copy of the rows.
//!
//! Counters are cumulative since the process started (diff two reads
//! for a window); times are histograms of doubling millisecond buckets,
//! reported as count, sum, max and the bucket upper bounds of the 50th
//! and 99th percentiles. `byWeight` lists the methods that account for
//! the most queue wait plus evaluation time; `all` sums every method.
//!
//! Bounded: at most [`MAX_METHODS`] rows. A method takes one only once
//! it evaluated (its name resolved); answers, refusals and waits of a
//! name with no row (one that never resolves, or before its first
//! evaluation) count as `other`, as does a name a method cannot have
//! (longer than 128 bytes, other characters), so a client naming
//! methods that do not exist cannot fill the table. No arguments,
//! results, principals or error text enter it.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use flower_engine::speculation::ConflictClass;
use serde_json::{Value, json};

use crate::node::NodeError;
use crate::types::{ResponseOutcome, WriteResponse};

/// The most method rows kept apart.
pub const MAX_METHODS: usize = 512;
/// Rows `/v1/metrics` lists by weight.
pub const SHOWN: usize = 64;
const OTHER: &str = "other";
/// Bucket `i` holds times up to `2^i` milliseconds; the last one holds
/// the rest.
const BUCKETS: usize = 16;

/// The bucket a time of `us` microseconds lands in.
fn bucket(us: u64) -> usize {
    let ms = us.div_ceil(1000).max(1);
    ((u64::BITS - (ms - 1).leading_zeros()) as usize).min(BUCKETS - 1)
}

/// A histogram any number of threads record into at once: every field
/// is an atomic of its own, so a read taken mid-recording may see its
/// count without its sum, which blurs that one read and nothing else.
#[derive(Default)]
struct Recorder {
    count: AtomicU64,
    sum_us: AtomicU64,
    max_us: AtomicU64,
    buckets: [AtomicU64; BUCKETS],
}

impl Recorder {
    fn record(&self, took: Duration) {
        // A day bounds one time, so the sum cannot wrap in any process's
        // life.
        let us = u64::try_from(took.as_micros())
            .unwrap_or(u64::MAX)
            .min(86_400_000_000);
        self.count.fetch_add(1, Relaxed);
        self.sum_us.fetch_add(us, Relaxed);
        self.max_us.fetch_max(us, Relaxed);
        self.buckets[bucket(us)].fetch_add(1, Relaxed);
    }

    fn load(&self) -> Histogram {
        Histogram {
            count: self.count.load(Relaxed),
            sum_us: self.sum_us.load(Relaxed),
            max_us: self.max_us.load(Relaxed),
            buckets: std::array::from_fn(|index| self.buckets[index].load(Relaxed)),
        }
    }
}

/// A histogram as read from a [`Recorder`].
#[derive(Default, Clone)]
struct Histogram {
    count: u64,
    sum_us: u64,
    max_us: u64,
    buckets: [u64; BUCKETS],
}

impl Histogram {
    fn merge(&mut self, other: &Histogram) {
        self.count += other.count;
        self.sum_us = self.sum_us.saturating_add(other.sum_us);
        self.max_us = self.max_us.max(other.max_us);
        for (into, from) in self.buckets.iter_mut().zip(other.buckets) {
            *into += from;
        }
    }

    /// The upper bound, in milliseconds, of the bucket holding the
    /// `q`th quantile (the last bucket's is its max).
    fn quantile_ms(&self, q: f64) -> Option<u64> {
        if self.count == 0 {
            return None;
        }
        let want = ((self.count as f64) * q).ceil().max(1.0) as u64;
        let mut seen = 0;
        for (index, count) in self.buckets.iter().enumerate() {
            seen += count;
            if seen >= want {
                return Some(if index == BUCKETS - 1 {
                    self.max_us.div_ceil(1000)
                } else {
                    1 << index
                });
            }
        }
        None
    }

    fn report(&self) -> Value {
        json!({
            "count": self.count,
            "sumSeconds": self.sum_us as f64 / 1e6,
            "maxMs": self.max_us as f64 / 1e3,
            "p50UpperMs": self.quantile_ms(0.5),
            "p99UpperMs": self.quantile_ms(0.99),
        })
    }
}

/// Why a mutation was refused before it was prepared.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Refusal {
    /// Refused on arrival, behind a standing queue.
    Overloaded,
    /// Found no room (places or queued bytes) in time.
    Full,
    /// Its client deadline passed while it waited.
    Deadline,
    /// Waited past the overload target in a standing queue.
    Shed,
}

const REFUSALS: [&str; 4] = ["overloaded", "full", "deadline", "shed"];

impl Refusal {
    /// Why admission (or the writer's turn) refused, from the refusal's
    /// message (see [`crate::admission`]); `None` for an overload
    /// admission did not decide.
    pub fn of(message: &str) -> Option<Refusal> {
        crate::admission::refusal(message)
    }

    /// Why `error` is a refusal before preparation, if it is one.
    pub fn of_error(error: &NodeError) -> Option<Refusal> {
        match error {
            NodeError::Overloaded { message, .. } => Refusal::of(message),
            _ => None,
        }
    }
}

/// How a mutation's answer ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Answer {
    /// It committed a value.
    Ok,
    /// The request failed: a business failure, a rejection, a refusal
    /// of the request itself.
    ClientError,
    /// This member failed it: storage, quorum, leadership, a fatal
    /// error.
    ServerError,
}

impl Answer {
    /// How a mutation's result ended; `None` for a refusal before
    /// preparation (counted apart) or a forward.
    pub fn of(result: &Result<WriteResponse, NodeError>) -> Option<Answer> {
        match result {
            Ok(response) => Some(match response.outcome {
                ResponseOutcome::Value(_) => Answer::Ok,
                ResponseOutcome::Failure(_) | ResponseOutcome::Error(_) => Answer::ClientError,
            }),
            Err(error) if Refusal::of_error(error).is_some() => None,
            Err(NodeError::Forward { .. }) => None,
            Err(NodeError::Fatal(_) | NodeError::Overloaded { .. }) => Some(Answer::ServerError),
            // The call's own failure, details and all.
            Err(NodeError::Call { .. }) => Some(Answer::ClientError),
            Err(NodeError::Failure { code, .. }) => Some(if server_code(code) {
                Answer::ServerError
            } else {
                Answer::ClientError
            }),
        }
    }
}

/// Whether a failure code is this member's, not the request's.
fn server_code(code: &str) -> bool {
    use crate::node::{ISOLATED, TRANSFERRING};
    [
        "quorum",
        crate::health::STORAGE_FAILED,
        flower_engine::error::IO_FAILURE,
        crate::recovery::RECOVERING,
        crate::read::STALE_REPLICA,
        crate::peer::FORWARD_FAILED,
        TRANSFERRING,
        crate::rejoin::REJOINING,
        ISOLATED,
    ]
    .contains(&code)
}

#[derive(Default, Clone)]
struct Method {
    /// Time in the writer queue before the first turn.
    wait: Histogram,
    /// Each evaluation of a mutation, and its thread's CPU time.
    eval: Histogram,
    eval_cpu: Histogram,
    /// Submission to answer, by how it ended.
    ok: Histogram,
    client_error: Histogram,
    server_error: Histogram,
    refused: [u64; REFUSALS.len()],
    /// Queries that evaluated (CPU time), and those the cache answered.
    query_eval: Histogram,
    query_hits: u64,
    /// Its mutations' speculative preparations.
    speculation: Speculation,
}

/// The most (class, name) pairs one method's speculation keeps apart.
pub const MAX_CONFLICT_KEYS: usize = 16;

/// Why a speculative candidate was prepared again instead of reused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Rejection {
    /// A write before it since its snapshot touched what it read: by
    /// class of row, and the collections and derived definitions named.
    Certificate(Vec<ConflictClass>, Vec<(ConflictClass, String)>),
    /// It read something no row names (retained history), or too much
    /// to track.
    Untracked,
    /// Its speculative preparation itself failed.
    Failed,
}

/// One method's speculation, as v1's `mutations.speculation`.
#[derive(Default, Clone)]
struct Speculation {
    reused: u64,
    certificate: u64,
    untracked: u64,
    failed: u64,
    classes: [u64; ConflictClass::ALL.len()],
    keys: std::collections::BTreeMap<(ConflictClass, String), u64>,
    /// Named pairs past [`MAX_CONFLICT_KEYS`].
    other_keys: u64,
    placed: u64,
    cooled: u64,
}

impl Speculation {
    fn merge(&mut self, other: &Speculation) {
        self.reused += other.reused;
        self.certificate += other.certificate;
        self.untracked += other.untracked;
        self.failed += other.failed;
        for (into, from) in self.classes.iter_mut().zip(other.classes) {
            *into += from;
        }
        self.placed += other.placed;
        self.cooled += other.cooled;
        // Sums keep classes and totals; names stay in each method's row.
        self.other_keys += other.other_keys + other.keys.values().sum::<u64>();
    }

    fn report(&self, keys: bool) -> Value {
        let rejected = self.certificate + self.untracked;
        let validated = self.reused + rejected;
        let conflicts: serde_json::Map<String, Value> = ConflictClass::ALL
            .iter()
            .zip(self.classes)
            .filter(|(_, count)| *count > 0)
            .map(|(class, count)| (class.as_str().to_owned(), json!(count)))
            .collect();
        let mut report = json!({
            "reused": self.reused,
            "rejected": rejected,
            "failed": self.failed,
            // Nothing validated is unknown, not 0%.
            "reuseRate": (validated > 0).then(|| self.reused as f64 / validated as f64),
            "rejections": {
                "certificate": self.certificate,
                "untracked": self.untracked,
            },
            "conflicts": conflicts,
            "placedSerially": self.placed,
            "cooldownSerialCalls": self.cooled,
        });
        if keys {
            let mut named: Vec<_> = self.keys.iter().collect();
            named.sort_by(|a, b| b.1.cmp(a.1).then_with(|| a.0.cmp(b.0)));
            report["keys"] = named
                .into_iter()
                .map(|((class, name), count)| {
                    json!({"class": class.as_str(), "name": name, "candidates": count})
                })
                .collect();
            report["otherKeys"] = json!(self.other_keys);
        } else {
            report["namedConflicts"] = json!(self.other_keys);
        }
        report
    }
}

impl Method {
    fn merge(&mut self, other: &Method) {
        self.wait.merge(&other.wait);
        self.eval.merge(&other.eval);
        self.eval_cpu.merge(&other.eval_cpu);
        self.ok.merge(&other.ok);
        self.client_error.merge(&other.client_error);
        self.server_error.merge(&other.server_error);
        for (into, from) in self.refused.iter_mut().zip(other.refused) {
            *into += from;
        }
        self.query_eval.merge(&other.query_eval);
        self.query_hits += other.query_hits;
        self.speculation.merge(&other.speculation);
    }

    /// Microseconds of work this method accounts for: the ranking key.
    fn weight(&self) -> u64 {
        self.eval
            .sum_us
            .saturating_add(self.query_eval.sum_us)
            .saturating_add(self.wait.sum_us)
    }

    fn report(&self, row: Value, named: bool) -> Value {
        let refused: serde_json::Map<String, Value> = REFUSALS
            .iter()
            .zip(self.refused)
            .filter(|(_, count)| *count > 0)
            .map(|(reason, count)| ((*reason).to_owned(), json!(count)))
            .collect();
        let finished = self.ok.count + self.client_error.count + self.server_error.count;
        let mut report = row;
        report["mutations"] = json!({
            "finished": finished,
            "ok": self.ok.count,
            "clientError": self.client_error.count,
            "serverError": self.server_error.count,
            "refused": refused,
            "queueWait": self.wait.report(),
            "evaluation": self.eval.report(),
            "evaluationCpu": self.eval_cpu.report(),
            "total": {
                "ok": self.ok.report(),
                "error": {
                    "count": self.client_error.count + self.server_error.count,
                },
            },
            "speculation": self.speculation.report(named),
        });
        report["queries"] = json!({
            "evaluated": self.query_eval.report(),
            "cacheHits": self.query_hits,
        });
        report
    }
}

/// Shards of each row: threads record into their own (by the order they
/// first recorded), so threads recording one method at once mostly
/// touch different cache lines. A shard is about 1.1 KB, so a row about
/// 9 KB and a full table at most 4.6 MB.
const SHARDS: usize = 8;

/// This thread's shard.
fn shard() -> usize {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    thread_local! {
        static SHARD: usize = NEXT.fetch_add(1, Relaxed) % SHARDS;
    }
    SHARD.with(|shard| *shard)
}

/// One shard of a method's counters, on cache lines of its own.
#[derive(Default)]
#[repr(align(128))]
struct Counters {
    wait: Recorder,
    eval: Recorder,
    eval_cpu: Recorder,
    ok: Recorder,
    client_error: Recorder,
    server_error: Recorder,
    refused: [AtomicU64; REFUSALS.len()],
    query_eval: Recorder,
    query_hits: AtomicU64,
}

impl Counters {
    fn answered(&self, answer: Answer, took: Duration) {
        match answer {
            Answer::Ok => self.ok.record(took),
            Answer::ClientError => self.client_error.record(took),
            Answer::ServerError => self.server_error.record(took),
        }
    }

    fn load(&self) -> Method {
        Method {
            wait: self.wait.load(),
            eval: self.eval.load(),
            eval_cpu: self.eval_cpu.load(),
            ok: self.ok.load(),
            client_error: self.client_error.load(),
            server_error: self.server_error.load(),
            refused: std::array::from_fn(|index| self.refused[index].load(Relaxed)),
            query_eval: self.query_eval.load(),
            query_hits: self.query_hits.load(Relaxed),
            speculation: Speculation::default(),
        }
    }
}

/// One method's speculation counters. The writer validates candidates
/// on its own thread, so a row keeps one set (no shards). The conflict
/// names, bounded and noted only when a candidate is refused, take a
/// lock of their own; nothing else does.
#[derive(Default)]
struct SpeculationCounters {
    reused: AtomicU64,
    certificate: AtomicU64,
    untracked: AtomicU64,
    failed: AtomicU64,
    classes: [AtomicU64; ConflictClass::ALL.len()],
    placed: AtomicU64,
    cooled: AtomicU64,
    /// Named conflicts, and those past [`MAX_CONFLICT_KEYS`].
    names: Mutex<(ConflictNames, u64)>,
}

/// Conflicts counted by (class, collection or derived definition).
type ConflictNames = std::collections::BTreeMap<(ConflictClass, String), u64>;

impl SpeculationCounters {
    fn rejected(&self, why: Rejection) {
        match why {
            Rejection::Untracked => {
                self.untracked.fetch_add(1, Relaxed);
            }
            Rejection::Failed => {
                self.failed.fetch_add(1, Relaxed);
            }
            Rejection::Certificate(classes, names) => {
                self.certificate.fetch_add(1, Relaxed);
                for class in classes {
                    if let Some(index) = ConflictClass::ALL.iter().position(|known| *known == class)
                    {
                        self.classes[index].fetch_add(1, Relaxed);
                    }
                }
                if names.is_empty() {
                    return;
                }
                let mut named = self.names.lock().unwrap_or_else(PoisonError::into_inner);
                let (keys, other) = &mut *named;
                for key in names {
                    if let Some(count) = keys.get_mut(&key) {
                        *count += 1;
                    } else if keys.len() < MAX_CONFLICT_KEYS {
                        keys.insert(key, 1);
                    } else {
                        *other += 1;
                    }
                }
            }
        }
    }

    fn load(&self) -> Speculation {
        let named = self.names.lock().unwrap_or_else(PoisonError::into_inner);
        Speculation {
            reused: self.reused.load(Relaxed),
            certificate: self.certificate.load(Relaxed),
            untracked: self.untracked.load(Relaxed),
            failed: self.failed.load(Relaxed),
            classes: std::array::from_fn(|index| self.classes[index].load(Relaxed)),
            keys: named.0.clone(),
            other_keys: named.1,
            placed: self.placed.load(Relaxed),
            cooled: self.cooled.load(Relaxed),
        }
    }
}

/// One method's counters, recorded into by any thread without a lock.
struct Row {
    shards: [Counters; SHARDS],
    speculation: SpeculationCounters,
}

impl Default for Row {
    fn default() -> Self {
        Row {
            shards: std::array::from_fn(|_| Counters::default()),
            speculation: SpeculationCounters::default(),
        }
    }
}

impl Row {
    /// The shard this thread records into.
    fn here(&self) -> &Counters {
        &self.shards[shard()]
    }

    /// The counters of every shard, summed, and the speculation's.
    fn load(&self) -> Method {
        let mut method = Method::default();
        for shard in &self.shards {
            method.merge(&shard.load());
        }
        method.speculation = self.speculation.load();
        method
    }
}

/// Fast hashing for the lookups every recording makes: names are
/// bounded and the table cannot grow past [`MAX_METHODS`], so flooding
/// one bucket gains a caller nothing.
type Hasher = foldhash::fast::RandomState;

/// Rows by database, module and name: lookups borrow, so recording
/// allocates nothing once a row exists.
type Rows = HashMap<String, HashMap<String, HashMap<String, Arc<Row>, Hasher>, Hasher>, Hasher>;

/// The row of `(db, module, name)` in `rows`, if it has one.
fn find<'a>(rows: &'a Rows, db: &str, module: &str, name: &str) -> Option<&'a Arc<Row>> {
    rows.get(db)?.get(module)?.get(name)
}

#[derive(Default)]
struct Table {
    /// The rows as last published: recordings read them without a lock.
    rows: ArcSwap<Rows>,
    /// Serializes adding a row, which publishes a copy of the rows; it
    /// holds their number.
    adding: Mutex<usize>,
    other: Row,
}

impl Table {
    /// Record against the row of `(db, module, name)`, adding it when
    /// `resolved` and room is left; against `other` otherwise.
    fn record(
        &self,
        db: &str,
        module: &str,
        name: &str,
        resolved: bool,
        record: impl FnOnce(&Row),
    ) {
        {
            let rows = self.rows.load();
            if let Some(row) = find(&rows, db, module, name) {
                return record(row);
            }
        }
        match resolved.then(|| self.add(db, module, name)).flatten() {
            Some(row) => record(&row),
            None => record(&self.other),
        }
    }

    /// Add the row of `(db, module, name)` unless the table is full.
    fn add(&self, db: &str, module: &str, name: &str) -> Option<Arc<Row>> {
        let mut count = self.adding.lock().unwrap_or_else(PoisonError::into_inner);
        let current = self.rows.load_full();
        if let Some(row) = find(&current, db, module, name) {
            return Some(row.clone());
        }
        if *count >= MAX_METHODS {
            return None;
        }
        let mut rows = Rows::clone(&current);
        let row = rows
            .entry(db.to_owned())
            .or_default()
            .entry(module.to_owned())
            .or_default()
            .entry(name.to_owned())
            .or_default()
            .clone();
        self.rows.store(Arc::new(rows));
        *count += 1;
        Some(row)
    }

    fn report(&self, top: usize) -> Value {
        let published = self.rows.load_full();
        let loaded: Vec<(&str, &str, &str, Method)> = published
            .iter()
            .flat_map(|(db, modules)| {
                modules.iter().flat_map(move |(module, names)| {
                    names.iter().map(move |(name, row)| {
                        (db.as_str(), module.as_str(), name.as_str(), row.load())
                    })
                })
            })
            .collect();
        let mut rows: Vec<(&str, &str, &str, &Method)> = loaded
            .iter()
            .map(|(db, module, name, method)| (*db, *module, *name, method))
            .collect();
        rows.sort_by(|a, b| {
            b.3.weight()
                .cmp(&a.3.weight())
                .then_with(|| (a.0, a.1, a.2).cmp(&(b.0, b.1, b.2)))
        });
        let mut total = Method::default();
        for (_, _, _, method) in &rows {
            total.merge(method);
        }
        let other = self.other.load();
        total.merge(&other);
        let shown: Vec<Value> = rows
            .iter()
            .take(top)
            .map(|(db, module, name, method)| {
                method.report(json!({"db": db, "module": module, "method": name}), true)
            })
            .collect();
        json!({
            "tracked": loaded.len(),
            "limit": MAX_METHODS,
            "shown": shown.len(),
            "byWeight": shown,
            "other": other.report(json!({"method": OTHER}), true),
            "all": total.report(json!({"method": "all"}), false),
        })
    }
}

fn table() -> &'static Table {
    static TABLE: OnceLock<Table> = OnceLock::new();
    TABLE.get_or_init(Table::default)
}

/// Whether `part` can name a database, module or method: a bounded
/// length of the characters names use. Anything else counts as `other`.
pub fn recordable(part: &str) -> bool {
    !part.is_empty()
        && part.len() <= 128
        && !part.starts_with("__")
        && part.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b':' | b'/')
        })
}

/// A method as a caller named it.
#[derive(Clone, Copy, Debug)]
pub struct MethodRef<'a> {
    /// Its database.
    pub db: &'a [u8],
    /// Its module.
    pub module: &'a str,
    /// Its name (an alias under its own).
    pub name: &'a str,
}

/// Record against `method`'s row, made when `resolved`; otherwise only
/// an existing row is used, and anything else is `other`.
fn with(method: MethodRef<'_>, resolved: bool, record: impl FnOnce(&Row)) {
    let table = table();
    match std::str::from_utf8(method.db).ok() {
        Some(db) if recordable(db) && recordable(method.module) && recordable(method.name) => {
            table.record(db, method.module, method.name, resolved, record);
        }
        _ => record(&table.other),
    }
}

/// A mutation reached its first turn after `waited` in the writer
/// queue.
pub fn mutation_waited(method: MethodRef<'_>, waited: Duration) {
    with(method, false, |row| row.here().wait.record(waited));
}

/// A mutation was answered `took` after its submission to the writer,
/// having waited `waited` for its first turn when it reached one: one
/// lookup for both.
pub fn mutation_finished(
    method: MethodRef<'_>,
    waited: Option<Duration>,
    answer: Answer,
    took: Duration,
) {
    with(method, false, |row| {
        if let Some(waited) = waited {
            row.here().wait.record(waited);
        }
        row.here().answered(answer, took);
    });
}

/// One evaluation of a mutation took `wall`, `cpu` of it on its
/// thread's CPU (the wall time where the platform does not tell).
pub fn mutation_evaluated(method: MethodRef<'_>, wall: Duration, cpu: Option<Duration>) {
    with(method, true, |row| {
        let here = row.here();
        here.eval.record(wall);
        here.eval_cpu.record(cpu.unwrap_or(wall));
    });
}

/// A mutation was answered `took` after its submission to the writer.
pub fn mutation_answered(method: MethodRef<'_>, answer: Answer, took: Duration) {
    with(method, false, |row| row.here().answered(answer, took));
}

/// A mutation was refused before its preparation.
pub fn mutation_refused(method: MethodRef<'_>, why: Refusal) {
    with(method, false, |row| {
        row.here().refused[why as usize].fetch_add(1, Relaxed);
    });
}

/// A query evaluated, costing `cpu` (its wall time where the platform
/// does not tell).
pub fn query_evaluated(method: MethodRef<'_>, cpu: Duration) {
    with(method, true, |row| row.here().query_eval.record(cpu));
}

/// The query cache answered a query.
pub fn query_cached(method: MethodRef<'_>) {
    with(method, false, |row| {
        row.here().query_hits.fetch_add(1, Relaxed);
    });
}

/// A mutation's speculative preparation held and was committed as
/// prepared.
pub fn speculation_reused(method: MethodRef<'_>) {
    with(method, false, |row| {
        row.speculation.reused.fetch_add(1, Relaxed);
    });
}

/// A mutation's speculative preparation was refused, and the mutation
/// prepared again in its turn.
pub fn speculation_rejected(method: MethodRef<'_>, why: Rejection) {
    with(method, false, |row| row.speculation.rejected(why));
}

/// Conflict prediction placed a mutation serially in its wave.
pub fn speculation_placed(method: MethodRef<'_>) {
    with(method, false, |row| {
        row.speculation.placed.fetch_add(1, Relaxed);
    });
}

/// A mutation prepared serially while the writer's waves cooled down.
pub fn speculation_cooled(method: MethodRef<'_>) {
    with(method, false, |row| {
        row.speculation.cooled.fetch_add(1, Relaxed);
    });
}

/// `/v1/metrics`' `methods`: the `top` methods by the work they account
/// for, the rest as `other`, and the sum.
pub fn report(top: usize) -> Value {
    table().report(top)
}

/// The evaluating thread's work: wall time, and CPU time where the
/// platform tells it.
pub struct Work {
    wall: Instant,
    cpu: Option<Duration>,
}

impl Work {
    /// Start measuring on this thread.
    pub fn start() -> Self {
        Work {
            wall: Instant::now(),
            cpu: crate::telemetry::thread_cpu(),
        }
    }

    /// Wall time so far, and this thread's CPU time since the start.
    pub fn elapsed(&self) -> (Duration, Option<Duration>) {
        let cpu = self
            .cpu
            .zip(crate::telemetry::thread_cpu())
            .map(|(start, now)| now.saturating_sub(start));
        (self.wall.elapsed(), cpu)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ms(value: u64) -> Duration {
        Duration::from_millis(value)
    }

    #[test]
    fn speculation_counts_by_reason_class_and_bounded_names() {
        let counters = SpeculationCounters::default();
        assert_eq!(
            counters.load().report(true)["reuseRate"],
            Value::Null,
            "nothing validated is unknown, not 0"
        );
        for _ in 0..3 {
            counters.reused.fetch_add(1, Relaxed);
        }
        let hot = || {
            Rejection::Certificate(
                vec![ConflictClass::Source, ConflictClass::Cell],
                vec![
                    (ConflictClass::Source, "stores".to_string()),
                    (ConflictClass::Cell, "storeStats".to_string()),
                ],
            )
        };
        counters.rejected(hot());
        counters.rejected(hot());
        counters.rejected(Rejection::Untracked);
        counters.rejected(Rejection::Failed);
        counters.placed.fetch_add(2, Relaxed);
        counters.cooled.fetch_add(5, Relaxed);
        let speculation = counters.load();
        let report = speculation.report(true);
        assert_eq!(report["reused"], 3);
        assert_eq!(report["rejected"], 3);
        assert_eq!(report["failed"], 1);
        assert_eq!(report["reuseRate"], 0.5);
        assert_eq!(
            report["rejections"],
            json!({"certificate": 2, "untracked": 1})
        );
        assert_eq!(report["conflicts"], json!({"source": 2, "cell": 2}));
        assert_eq!(report["placedSerially"], 2);
        assert_eq!(report["cooldownSerialCalls"], 5);
        assert_eq!(
            report["keys"],
            json!([
                {"class": "source", "name": "stores", "candidates": 2},
                {"class": "cell", "name": "storeStats", "candidates": 2},
            ])
        );
        let counters = SpeculationCounters::default();
        for index in 0..MAX_CONFLICT_KEYS + 3 {
            counters.rejected(Rejection::Certificate(
                vec![ConflictClass::Source],
                vec![(ConflictClass::Source, format!("c{index}"))],
            ));
        }
        let full = counters.load();
        assert_eq!(full.keys.len(), MAX_CONFLICT_KEYS);
        assert_eq!(full.report(true)["otherKeys"], 3, "the rest count together");
        let mut all = Speculation::default();
        all.merge(&speculation);
        all.merge(&full);
        let sum = all.report(false);
        assert_eq!(sum["rejected"], 3 + MAX_CONFLICT_KEYS as u64 + 3);
        assert!(sum.get("keys").is_none(), "names stay in each method's row");
        assert_eq!(sum["namedConflicts"], 4 + MAX_CONFLICT_KEYS as u64 + 3);
    }

    #[test]
    fn times_land_in_doubling_millisecond_buckets_and_quantiles_are_upper_bounds() {
        let recorder = Recorder::default();
        for took in [ms(0), ms(1), ms(2), ms(3), ms(100), ms(100_000)] {
            recorder.record(took);
        }
        let histogram = recorder.load();
        assert_eq!(histogram.count, 6);
        assert_eq!(histogram.buckets[0], 2, "up to 1 ms");
        assert_eq!(histogram.buckets[1], 1, "up to 2 ms");
        assert_eq!(histogram.buckets[2], 1, "up to 4 ms");
        assert_eq!(histogram.buckets[7], 1, "up to 128 ms");
        assert_eq!(histogram.buckets[BUCKETS - 1], 1, "the rest");
        assert_eq!(histogram.quantile_ms(0.5), Some(2));
        assert_eq!(histogram.quantile_ms(0.99), Some(100_000));
        assert_eq!(Histogram::default().quantile_ms(0.5), None);
    }

    #[test]
    fn methods_are_ranked_by_work_and_the_table_is_bounded() {
        let table = Table::default();
        let record = |name: &str, record: &dyn Fn(&Counters)| {
            table.record("db", "app", name, true, |row| record(row.here()));
        };
        record("cheap", &|row| row.eval.record(ms(1)));
        record("costly", &|row| row.eval.record(ms(900)));
        record("costly", &|row| row.eval_cpu.record(ms(600)));
        record("costly", &|row| row.wait.record(ms(50)));
        record("costly", &|row| {
            row.refused[Refusal::Deadline as usize].fetch_add(2, Relaxed);
        });
        record("read", &|row| row.query_eval.record(ms(5)));
        let report = table.report(2);
        let names: Vec<_> = report["byWeight"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["method"].as_str().unwrap())
            .collect();
        assert_eq!(names, ["costly", "read"], "the cheapest is cut by `top`");
        let costly = &report["byWeight"][0];
        assert_eq!(
            (costly["db"].as_str(), costly["module"].as_str()),
            (Some("db"), Some("app"))
        );
        assert_eq!(costly["mutations"]["refused"], json!({"deadline": 2}));
        assert_eq!(costly["mutations"]["evaluationCpu"]["sumSeconds"], 0.6);
        assert_eq!(report["all"]["mutations"]["evaluation"]["count"], 2);
        let full = Table::default();
        for n in 0..MAX_METHODS + 10 {
            full.record("db", "app", &format!("m{n}"), true, |row| {
                row.here().query_hits.fetch_add(1, Relaxed);
            });
        }
        assert_eq!(*full.adding.lock().unwrap(), MAX_METHODS);
        assert_eq!(full.report(1)["tracked"], MAX_METHODS);
        assert_eq!(
            full.other.load().query_hits,
            10,
            "the rest are counted together"
        );
        assert_eq!(
            full.report(1)["all"]["queries"]["cacheHits"],
            (MAX_METHODS + 10) as u64
        );
    }

    #[test]
    fn threads_record_at_once_and_lose_nothing() {
        // Every recording is its own atomic steps, the first evaluation
        // of a method adds its row once however many threads race to it,
        // and the counts come out exact.
        let table = Table::default();
        let threads = 8;
        let each = 10_000;
        std::thread::scope(|scope| {
            for thread in 0..threads {
                let table = &table;
                scope.spawn(move || {
                    for n in 0..each {
                        let name = if n % 2 == 0 { "even" } else { "odd" };
                        table.record("db", "app", name, true, |row| {
                            let here = row.here();
                            here.eval.record(ms(thread % 3));
                            here.query_hits.fetch_add(1, Relaxed);
                        });
                    }
                });
            }
        });
        assert_eq!(
            *table.adding.lock().unwrap(),
            2,
            "two rows, added once each"
        );
        let report = table.report(2);
        let all = &report["all"];
        assert_eq!(all["mutations"]["evaluation"]["count"], threads * each);
        assert_eq!(all["queries"]["cacheHits"], threads * each);
        for row in report["byWeight"].as_array().unwrap() {
            assert_eq!(row["queries"]["cacheHits"], threads * each / 2);
        }
    }

    #[test]
    fn only_names_a_method_can_have_get_a_row() {
        assert!(recordable("completions.complete") && recordable("worker/claim"));
        for name in ["", "__deployment", "has space", "a\nb", &"x".repeat(129)] {
            assert!(!recordable(name), "{name:?}");
        }
    }

    #[test]
    fn only_an_evaluated_method_takes_a_row_and_unresolved_names_count_as_other() {
        let method = |name: &'static str| MethodRef {
            db: b"unitdb",
            module: "unit",
            name,
        };
        let rows = |name: &str| {
            let rows = table().rows.load();
            find(&rows, "unitdb", "unit", name).map(|row| {
                let row = row.load();
                (row.ok.count + row.client_error.count, row.eval.count)
            })
        };
        mutation_answered(method("noSuch"), Answer::ClientError, ms(1));
        mutation_refused(method("noSuch"), Refusal::Full);
        mutation_waited(method("noSuch"), ms(1));
        query_cached(method("noSuch"));
        assert_eq!(rows("noSuch"), None, "it took no row");
        mutation_evaluated(method("real"), ms(1), Some(ms(1)));
        mutation_answered(method("real"), Answer::Ok, ms(2));
        assert_eq!(
            rows("real"),
            Some((1, 1)),
            "once evaluated, its answers have a row"
        );
    }

    #[test]
    fn answers_are_counted_by_how_they_ended() {
        let value = WriteResponse::value(b"1".to_vec(), 1, 1);
        assert_eq!(Answer::of(&Ok(value)), Some(Answer::Ok));
        let rejected = WriteResponse::error("stale_revision: moved".to_string(), 1, 1);
        assert_eq!(Answer::of(&Ok(rejected)), Some(Answer::ClientError));
        let storage = NodeError::Failure {
            code: crate::health::STORAGE_FAILED.to_string(),
            message: String::new(),
        };
        assert_eq!(Answer::of(&Err(storage)), Some(Answer::ServerError));
        let refused = NodeError::Overloaded {
            message: "request queued for admission past the overload target; retry later"
                .to_string(),
            retry_after_ms: 1,
        };
        assert_eq!(Refusal::of_error(&refused), Some(Refusal::Shed));
        assert_eq!(Answer::of(&Err(refused)), None, "refusals count apart");
        let stale = NodeError::Overloaded {
            message: "other entries kept landing before its batch".to_string(),
            retry_after_ms: 1,
        };
        assert_eq!(Answer::of(&Err(stale)), Some(Answer::ServerError));
    }
}
