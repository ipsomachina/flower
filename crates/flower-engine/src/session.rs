//! Invocation sessions: staged writes, savepoints, dependencies, fuel.
//!
//! One [`Invocation`] carries everything a single guest call needs: the
//! snapshot database, identity, staged overlay, savepoint stack, collected
//! dependency scopes, fuel budget and poison flag. Cross-module calls
//! reborrow the same session, so nested invocations share the snapshot,
//! the overlay and the final atomic publication. Failed savepoint branches
//! truncate staged writes but keep their reads as dependencies; only a
//! poisoned invocation fails the lane.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use flower_sdk::error::codes;
use flower_sdk::{Failure, Principal, fail};

use crate::error::HostResult;
use crate::storage::codec::OutcomeTime;

/// A staged target one module's derived values evaluate under, for a
/// replicated staging's graph phase (see [`crate::staging`]): the
/// module's derived definitions resolve to their staged twins, and its
/// code identity is the target's, so outcomes cache, edge and serve in
/// the staged graph while the active one keeps serving.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StagedGraph {
    /// The staged module.
    pub module: String,
    /// The target code identity.
    pub identity: [u8; crate::storage::keys::HASH_LEN],
    /// Staged twin IDs by derived definition name.
    pub staged: HashMap<String, u32>,
}

/// What the running callback may do.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CallerKind {
    /// Reads plus query calls.
    Query,
    /// Reads, writes and all calls.
    Mutation,
    /// Reads only, and no calls at all.
    Derived,
}

/// One staged write journal entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StagedOp {
    /// Upsert (`Some`) or delete (`None`) of canonical JSON bytes.
    Record {
        /// Interned collection ID.
        collection: u32,
        /// Record key bytes.
        key: Vec<u8>,
        /// New value, or `None` for delete.
        value: Option<Vec<u8>>,
    },
    /// Index entry insert (`true`) or delete (`false`), decoded so the
    /// lane can apply it without descriptor lookups.
    Index {
        /// Interned index ID.
        index: u32,
        /// Index component bytes.
        components: Vec<Vec<u8>>,
        /// Primary key bytes.
        primary: Vec<u8>,
        /// Present or removed.
        present: bool,
    },
}

/// A staged index entry's identity: index ID, components, primary key.
pub type IndexKey = (u32, Vec<Vec<u8>>, Vec<u8>);

/// A drained staged derived outcome: full `derived` key, encoded
/// outcome and observed edges (`None` keeps the committed edges).
pub type StagedOutcome = (Vec<u8>, Vec<u8>, Option<Vec<Vec<u8>>>);

/// One successful deletion awaiting its retention hooks: the stored row
/// it removed, and whether the delete was permanent (skipping them).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Deletion {
    /// Interned collection ID.
    pub collection: u32,
    /// Record key bytes.
    pub key: Vec<u8>,
    /// The removed row, canonical JSON.
    pub value: Vec<u8>,
    /// A permanent delete: no retention, and its cascades none either.
    pub permanent: bool,
}

/// One fired trigger change: collection, key and the after-image that fired.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct FiredChange {
    /// Interned collection ID.
    pub collection: u32,
    /// Record key bytes.
    pub key: Vec<u8>,
    /// After-image canonical bytes, or `None` for delete.
    pub after: Option<Vec<u8>>,
}

/// A staged derived outcome with the edges its evaluation observed.
#[derive(Clone, Debug, PartialEq, Eq)]
struct StagedDerived {
    /// Encoded outcome bytes.
    outcome: Vec<u8>,
    /// Observed dependency keys for commit-time edge replacement, or
    /// `None` when a pin-only restage retains the committed edges.
    observed: Option<Vec<Vec<u8>>>,
    /// The journal length whose state the outcome reflects: the
    /// staged writes it saw are exactly `journal[..basis]`. An
    /// evaluation sees the whole journal as it runs; a committed
    /// outcome restaged for its pin saw none of it. Aggregates fold the
    /// writes past it ([`crate::derived`]).
    basis: usize,
}

/// The state one derived key had before a change inside a savepoint:
/// rollback restores it exactly, whatever order later changes ran in.
#[derive(Clone, Debug)]
struct DerivedUndo {
    /// Full `derived` key bytes.
    key: Vec<u8>,
    /// The staged outcome, if any.
    staged: Option<StagedDerived>,
    /// Whether an eviction was staged.
    dropped: bool,
}

/// A savepoint: journal lengths to truncate back to plus the derived
/// undo position. Dependency scopes are deliberately not part of
/// savepoints: reads from failed branches remain dependencies.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Savepoint {
    /// Journal length.
    journal: usize,
    /// Derived undo-log length.
    derived_undo: usize,
    /// Staged receipt count.
    receipts: usize,
    /// Fired-change count.
    fired: usize,
    /// Deletion-event count.
    deletions: usize,
}

/// The staged overlay: uncommitted writes plus truncation support.
/// Journal-shaped state truncates by length; derived outcomes and
/// evictions are keyed maps, so savepoints restore them from an undo log
/// recorded only while a savepoint is open.
#[derive(Clone, Debug, Default)]
pub struct Staged {
    journal: Vec<StagedOp>,
    records: BTreeMap<(u32, Vec<u8>), Option<Vec<u8>>>,
    index: BTreeMap<IndexKey, bool>,
    derived: BTreeMap<Vec<u8>, StagedDerived>,
    drops: BTreeSet<Vec<u8>>,
    derived_undo: Vec<DerivedUndo>,
    open_savepoints: usize,
    receipts: Vec<(Vec<u8>, Vec<u8>)>,
    fired: Vec<FiredChange>,
    settled: usize,
    deletions: Vec<Deletion>,
    retained: usize,
}

type StagedRecord = (Vec<u8>, Option<Vec<u8>>);

impl Staged {
    fn new() -> Self {
        Staged::default()
    }

    /// Stage a record write; `None` deletes.
    pub fn stage_record(&mut self, collection: u32, key: Vec<u8>, value: Option<Vec<u8>>) {
        self.journal.push(StagedOp::Record {
            collection,
            key: key.clone(),
            value: value.clone(),
        });
        self.records.insert((collection, key), value);
    }

    /// Look up a staged record write.
    pub fn staged_record(&self, collection: u32, key: &[u8]) -> Option<Option<Vec<u8>>> {
        self.records.get(&(collection, key.to_vec())).cloned()
    }

    /// Inspect a staged row without copying its payload before a byte check.
    pub(crate) fn staged_record_view(
        &self,
        collection: u32,
        key: &[u8],
    ) -> Option<&Option<Vec<u8>>> {
        self.records.get(&(collection, key.to_vec()))
    }

    /// Records staged under one collection: keys plus optional JSON.
    pub fn staged_records_for(&self, collection: u32) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        self.records
            .iter()
            .filter(|((id, _), _)| *id == collection)
            .map(|((_, key), value)| (key.clone(), value.clone()))
            .collect()
    }

    /// SQL's bounded overlay lookup, without copying unrelated staged rows.
    pub(crate) fn bounded_records_for(
        &self,
        collection: u32,
        limit: usize,
        bytes: usize,
    ) -> Option<Vec<StagedRecord>> {
        let mut rows = Vec::new();
        let mut used = 0usize;
        for ((id, key), value) in self.records.range((collection, Vec::new())..) {
            if *id != collection {
                break;
            }
            used = used
                .saturating_add(key.len())
                .saturating_add(value.as_ref().map_or(0, Vec::len))
                .saturating_add(128);
            if rows.len() >= limit || used > bytes {
                return None;
            }
            rows.push((key.clone(), value.clone()));
        }
        Some(rows)
    }

    /// Stage an index entry insert or delete, decoded.
    pub fn stage_index(
        &mut self,
        index: u32,
        components: Vec<Vec<u8>>,
        primary: Vec<u8>,
        present: bool,
    ) {
        self.journal.push(StagedOp::Index {
            index,
            components: components.clone(),
            primary: primary.clone(),
            present,
        });
        self.index.insert((index, components, primary), present);
    }

    /// Iterate staged index entries of one index below a component prefix.
    /// Callers encode entries to bytes for prefix comparison.
    pub fn staged_index_prefix(
        &self,
        index: u32,
        prefix: &[Vec<u8>],
    ) -> Vec<(Vec<Vec<u8>>, Vec<u8>, bool)> {
        self.index
            .iter()
            .filter(|((id, components, _), _)| {
                *id == index
                    && components.len() >= prefix.len()
                    && components[..prefix.len()] == prefix[..]
            })
            .map(|((_, components, primary), present)| {
                (components.clone(), primary.clone(), *present)
            })
            .collect()
    }

    /// Complete index equality bucket, bounded before cloning primary keys.
    pub(crate) fn bounded_index_keys(
        &self,
        index: u32,
        components: &[Vec<u8>],
        limit: usize,
        bytes: usize,
    ) -> Option<Vec<(Vec<u8>, bool)>> {
        let mut rows = Vec::new();
        let mut used = 0usize;
        for ((id, parts, key), present) in
            self.index.range((index, components.to_vec(), Vec::new())..)
        {
            if *id != index || parts != components {
                break;
            }
            used = used.saturating_add(key.len()).saturating_add(128);
            if rows.len() >= limit || used > bytes {
                return None;
            }
            rows.push((key.clone(), *present));
        }
        Some(rows)
    }

    /// The staged journal for commit application, in order.
    pub fn journal(&self) -> &[StagedOp] {
        &self.journal
    }

    /// Every staged record as `(collection, key, value)`, for patch
    /// extraction.
    pub fn all_records(&self) -> Vec<(u32, Vec<u8>, Option<Vec<u8>>)> {
        self.records
            .iter()
            .map(|((collection, key), value)| (*collection, key.clone(), value.clone()))
            .collect()
    }

    /// Every staged index entry as `((index, components, primary),
    /// present)`, for patch extraction.
    pub fn all_index(&self) -> Vec<(IndexKey, bool)> {
        self.index
            .iter()
            .map(|(key, present)| (key.clone(), *present))
            .collect()
    }

    /// Journal length already covered by `settle`: settle processes only
    /// the tail past this watermark, so repeat settles cost nothing.
    pub fn settled(&self) -> usize {
        self.settled
    }

    /// Advance the settle watermark after processing the journal tail.
    pub fn set_settled(&mut self, settled: usize) {
        self.settled = settled;
    }

    /// Record a fired trigger change.
    pub fn mark_fired(&mut self, change: FiredChange) {
        if !self.fired.contains(&change) {
            self.fired.push(change);
        }
    }

    /// Whether a trigger change already fired.
    pub fn fired(&self, change: &FiredChange) -> bool {
        self.fired.contains(change)
    }

    /// Record one successful deletion for its retention hooks.
    pub fn record_deletion(&mut self, deletion: Deletion) {
        self.deletions.push(deletion);
    }

    /// Whether a staged deletion of this row was permanent.
    pub fn deleted_permanently(&self, collection: u32, key: &[u8]) -> bool {
        self.deletions
            .iter()
            .rev()
            .find(|deletion| deletion.collection == collection && deletion.key == key)
            .is_some_and(|deletion| deletion.permanent)
    }

    /// Deletions not yet handed to their retention hooks, marking them
    /// handed.
    pub fn take_deletions(&mut self) -> Vec<Deletion> {
        let fresh = self.deletions[self.retained..].to_vec();
        self.retained = self.deletions.len();
        fresh
    }

    /// Remember a derived key's current state before changing it, so an
    /// open savepoint can restore it exactly.
    fn record_undo(&mut self, key: &[u8]) {
        if self.open_savepoints == 0 {
            return;
        }
        self.derived_undo.push(DerivedUndo {
            key: key.to_vec(),
            staged: self.derived.get(key).cloned(),
            dropped: self.drops.contains(key),
        });
    }

    /// Stage an evaluated derived outcome with its observed edges
    /// (`None` retains the committed edges). A fresh evaluation
    /// supersedes any staged outcome or eviction for the key.
    pub fn stage_derived(
        &mut self,
        key: Vec<u8>,
        outcome: Vec<u8>,
        observed: Option<Vec<Vec<u8>>>,
    ) {
        let basis = self.journal.len();
        self.stage_derived_at(key, outcome, observed, basis);
    }

    /// [`Staged::stage_derived`] for an outcome reflecting the staged
    /// writes `journal[..basis]` only: zero for a committed outcome.
    pub fn stage_derived_at(
        &mut self,
        key: Vec<u8>,
        outcome: Vec<u8>,
        observed: Option<Vec<Vec<u8>>>,
        basis: usize,
    ) {
        self.record_undo(&key);
        self.drops.remove(&key);
        self.derived.insert(
            key,
            StagedDerived {
                outcome,
                observed,
                basis,
            },
        );
    }

    /// Look up a staged derived outcome.
    pub fn staged_derived(&self, key: &[u8]) -> Option<&[u8]> {
        self.derived.get(key).map(|entry| entry.outcome.as_slice())
    }

    /// The journal length a staged derived outcome reflects (see
    /// [`Staged::stage_derived_at`]).
    pub fn staged_basis(&self, key: &[u8]) -> Option<usize> {
        self.derived.get(key).map(|entry| entry.basis)
    }

    /// The edges a staged derived outcome's evaluation observed; `None`
    /// when nothing is staged for the key or the staging keeps the
    /// committed edges.
    pub fn staged_observed(&self, key: &[u8]) -> Option<&[Vec<u8>]> {
        self.derived.get(key)?.observed.as_deref()
    }

    /// Replace a staged derived outcome in place (pin changes).
    pub fn restage_derived(&mut self, key: &[u8], outcome: Vec<u8>) {
        if self.derived.contains_key(key) {
            self.record_undo(key);
            if let Some(entry) = self.derived.get_mut(key) {
                entry.outcome = outcome;
            }
        }
    }

    /// Stage the eviction of a cached derived outcome.
    pub fn drop_derived(&mut self, key: Vec<u8>) {
        self.record_undo(&key);
        self.derived.remove(&key);
        self.drops.insert(key);
    }

    /// Whether a derived key is staged for eviction.
    pub fn derived_dropped(&self, key: &[u8]) -> bool {
        self.drops.contains(key)
    }

    /// Staged derived keys whose observed edges include `dependency`:
    /// dependents that exist only in the overlay, invisible to the
    /// committed edge table until commit.
    pub fn staged_dependents(&self, dependency: &[u8]) -> Vec<Vec<u8>> {
        self.derived
            .iter()
            .filter(|(_, entry)| {
                entry
                    .observed
                    .as_ref()
                    .is_some_and(|edges| edges.iter().any(|edge| edge == dependency))
            })
            .map(|(key, _)| key.clone())
            .collect()
    }

    /// Every staged `(dependency, derived key)` edge, for range matching.
    pub fn staged_edges(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.derived
            .iter()
            .flat_map(|(key, entry)| {
                entry
                    .observed
                    .iter()
                    .flatten()
                    .map(move |edge| (edge.clone(), key.clone()))
            })
            .collect()
    }

    /// Stage a receipt write.
    pub fn stage_receipt(&mut self, identity: Vec<u8>, receipt: Vec<u8>) {
        self.receipts.push((identity, receipt));
    }

    /// Drain staged receipts for commit.
    pub fn take_receipts(&mut self) -> Vec<(Vec<u8>, Vec<u8>)> {
        std::mem::take(&mut self.receipts)
    }

    /// Drain staged derived outcomes for commit, in key order.
    pub fn take_derived(&mut self) -> Vec<StagedOutcome> {
        std::mem::take(&mut self.derived)
            .into_iter()
            .map(|(key, entry)| (key, entry.outcome, entry.observed))
            .collect()
    }

    /// Whether anything a query persists is staged: derived outcomes,
    /// evictions or receipts.
    pub fn has_caches(&self) -> bool {
        !self.derived.is_empty() || !self.drops.is_empty() || !self.receipts.is_empty()
    }

    /// Drain staged derived evictions for commit, in key order.
    pub fn take_drops(&mut self) -> Vec<Vec<u8>> {
        std::mem::take(&mut self.drops).into_iter().collect()
    }

    fn snapshot(&mut self) -> Savepoint {
        self.open_savepoints += 1;
        Savepoint {
            journal: self.journal.len(),
            derived_undo: self.derived_undo.len(),
            receipts: self.receipts.len(),
            fired: self.fired.len(),
            deletions: self.deletions.len(),
        }
    }

    /// Keep a savepoint's changes. The undo entries stay for enclosing
    /// savepoints and go once none is open.
    fn release(&mut self) {
        self.open_savepoints = self.open_savepoints.saturating_sub(1);
        if self.open_savepoints == 0 {
            self.derived_undo.clear();
        }
    }

    fn truncate(&mut self, point: &Savepoint) {
        self.journal.truncate(point.journal);
        self.settled = self.settled.min(point.journal);
        self.records.clear();
        self.index.clear();
        for op in &self.journal {
            match op {
                StagedOp::Record {
                    collection,
                    key,
                    value,
                } => {
                    self.records
                        .insert((*collection, key.clone()), value.clone());
                }
                StagedOp::Index {
                    index,
                    components,
                    primary,
                    present,
                } => {
                    self.index
                        .insert((*index, components.clone(), primary.clone()), *present);
                }
            }
        }
        // Undo newest first, so every derived key ends in the state it
        // had when the savepoint opened.
        while self.derived_undo.len() > point.derived_undo {
            let Some(undo) = self.derived_undo.pop() else {
                break;
            };
            match undo.staged {
                Some(staged) => {
                    self.derived.insert(undo.key.clone(), staged);
                }
                None => {
                    self.derived.remove(&undo.key);
                }
            }
            if undo.dropped {
                self.drops.insert(undo.key);
            } else {
                self.drops.remove(&undo.key);
            }
        }
        self.receipts.truncate(point.receipts);
        self.fired.truncate(point.fired);
        self.deletions.truncate(point.deletions);
        self.retained = self.retained.min(point.deletions);
        self.release();
    }
}

/// What one dependency scope saw of the clock: whether it polled it,
/// and the earliest instant it declared or a derived value it read
/// carried. A derived outcome keeps it (see
/// [`crate::storage::codec::OutcomeTime`]), and its readers inherit it.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Timing {
    /// Whether it polled the clock with `now()`.
    pub polled: bool,
    /// The earliest instant declared, in milliseconds.
    pub until: Option<f64>,
}

impl Timing {
    /// Declare that the result changes at `at`: the earliest instant stands.
    fn declare(&mut self, at: f64) {
        self.until = Some(self.until.map_or(at, |earlier| earlier.min(at)));
    }

    /// Fold in what a value read in this scope saw.
    pub fn merge(&mut self, other: Timing) {
        self.polled |= other.polled;
        if let Some(at) = other.until {
            self.declare(at);
        }
    }

    /// When an outcome evaluated at `timestamp_ms` with this timing
    /// serves: a poll pins it to that instant, and so does a declared
    /// instant not after it, as for certificates
    /// ([`crate::certificate::Validity::declared`]).
    pub fn outcome_time(self, timestamp_ms: u64) -> OutcomeTime {
        match self.until {
            _ if self.polled => OutcomeTime::At(timestamp_ms),
            None => OutcomeTime::Always,
            Some(at) if at.is_nan() || at <= timestamp_ms as f64 => OutcomeTime::At(timestamp_ms),
            Some(at) if at >= u64::MAX as f64 => OutcomeTime::Always,
            Some(at) => OutcomeTime::Between {
                from: timestamp_ms,
                until: at.ceil() as u64,
            },
        }
    }

    /// What reading an outcome with this time tells its reader.
    pub fn of(time: OutcomeTime) -> Timing {
        match time {
            OutcomeTime::Always => Timing::default(),
            OutcomeTime::Between { until, .. } => Timing {
                polled: false,
                until: Some(until as f64),
            },
            OutcomeTime::At(_) => Timing {
                polled: true,
                until: None,
            },
        }
    }
}

/// One guest invocation's session.
pub struct Invocation {
    /// Current module identity.
    pub module: String,
    /// Database identifier bytes.
    pub db: Vec<u8>,
    /// Caller capability.
    pub kind: CallerKind,
    /// Calling principal, or `None` for anonymous invocations.
    pub principal: Option<Principal>,
    /// Calling module identity, or `None` for external entry.
    pub caller: Option<String>,
    /// Fixed invocation timestamp in milliseconds.
    pub timestamp_ms: u64,
    /// Staged overlay.
    pub staged: Staged,
    savepoints: Vec<Savepoint>,
    dep_scopes: Vec<HashSet<Vec<u8>>>,
    /// One per dependency scope: what it saw of the clock.
    timings: Vec<Timing>,
    evaluating: HashSet<(u32, Vec<u8>)>,
    call_depth: usize,
    fuel: u64,
    /// What the guest calls of the invocation ask of the runtime: the
    /// called definition's declared deadline and the caller's.
    pub budget: crate::runtime::wasm::Budget,
    poisoned: Option<Failure>,
    time_polled: bool,
    /// Whether anything it read named no dependency
    /// ([`Invocation::record_volatile`]).
    volatile: bool,
    writable: bool,
    ephemeral: bool,
    /// Nesting of code running with definer rights (triggers, timer
    /// handlers, the authorization hook): access policies guard only
    /// callers.
    definer: u32,
    /// The staged target derived values evaluate under, if any.
    pub graph: Option<StagedGraph>,
    /// Access policies folded for `principal`, by module and collection:
    /// whoever replaces the principal replaces these with it.
    pub(crate) folds: crate::access::Folds,
}

impl Invocation {
    /// Begin a session. `writable` starts true and the lane clears it
    /// while evaluating derived values.
    pub fn new(
        module: String,
        db: Vec<u8>,
        kind: CallerKind,
        principal: Option<Principal>,
        caller: Option<String>,
        timestamp_ms: u64,
        fuel: u64,
    ) -> Self {
        Invocation {
            module,
            db,
            kind,
            principal,
            caller,
            timestamp_ms,
            staged: Staged::new(),
            savepoints: Vec::new(),
            dep_scopes: vec![HashSet::new()],
            timings: vec![Timing::default()],
            evaluating: HashSet::new(),
            call_depth: 0,
            fuel,
            budget: crate::runtime::wasm::Budget::default(),
            poisoned: None,
            time_polled: false,
            volatile: false,
            writable: true,
            ephemeral: false,
            definer: 0,
            graph: None,
            folds: crate::access::Folds::new(),
        }
    }

    /// Mark the session ephemeral: derived caches computed over
    /// preview overlays must never persist.
    pub fn set_ephemeral(&mut self, ephemeral: bool) {
        self.ephemeral = ephemeral;
    }

    /// Whether caches from this session persist.
    pub fn ephemeral(&self) -> bool {
        self.ephemeral
    }

    /// Whether access policies guard this point of the invocation: a
    /// caller's code, not a derived evaluation and not code running with
    /// definer rights.
    pub fn guarded(&self) -> bool {
        self.definer == 0 && self.kind != CallerKind::Derived
    }

    /// Run with definer rights until the matching [`exit_definer`].
    ///
    /// [`exit_definer`]: Invocation::exit_definer
    pub fn enter_definer(&mut self) {
        self.definer += 1;
    }

    /// Leave one level of definer rights.
    pub fn exit_definer(&mut self) {
        self.definer = self.definer.saturating_sub(1);
    }

    /// Swap the definer nesting, returning the previous one: a definer's
    /// call into another module runs there as a caller.
    pub fn replace_definer(&mut self, definer: u32) -> u32 {
        std::mem::replace(&mut self.definer, definer)
    }

    /// Logical fuel left: what a budget shared across sessions draws
    /// down by.
    pub fn fuel_left(&self) -> u64 {
        self.fuel
    }

    /// Spend fuel, failing the invocation on exhaustion, or once its
    /// transaction's deadline or its caller's passed (whatever runtime
    /// runs its code: Wasm guests also stop between host operations):
    /// the caller's when it came first, else the call's own. All are
    /// fatal and never recover through a savepoint.
    pub fn spend(&mut self, amount: u64) -> crate::error::Result<()> {
        let now = std::time::Instant::now();
        let (within, caller) = (self.budget.within, self.budget.caller);
        let passed = |at: Option<std::time::Instant>| at.is_some_and(|at| now >= at);
        if passed(within) || passed(caller) {
            self.fuel = 0;
            let callers = passed(caller) && within.is_none_or(|within| caller <= Some(within));
            return Err(if callers {
                crate::error::Error::CallerDeadline
            } else {
                crate::error::Error::Deadline
            });
        }
        match self.fuel.checked_sub(amount) {
            Some(left) => {
                self.fuel = left;
                Ok(())
            }
            None => {
                self.fuel = 0;
                Err(crate::error::Error::Budget)
            }
        }
    }

    /// Remaining fuel.
    pub fn fuel(&self) -> u64 {
        self.fuel
    }

    /// Whether writes are currently allowed.
    pub fn check_write(&self) -> HostResult<()> {
        if self.kind == CallerKind::Mutation && self.writable {
            Ok(())
        } else {
            Err(fail(
                codes::INVALID_INVOCATION,
                "writes are not allowed here",
            ))
        }
    }

    /// Whether a call to `target` is allowed from this caller kind.
    /// Derived evaluation calls nothing; transactions never nest.
    pub fn check_call(&self, target: crate::descriptors::DefKind) -> HostResult<()> {
        use crate::descriptors::DefKind;
        let allowed = matches!(
            (self.kind, target),
            (CallerKind::Query, DefKind::Query)
                | (CallerKind::Mutation, DefKind::Query | DefKind::Mutation)
        );
        if allowed {
            Ok(())
        } else {
            Err(fail(codes::INVALID_INVOCATION, "call is not allowed here"))
        }
    }

    /// Push a savepoint over the staged overlay.
    pub fn push_savepoint(&mut self) {
        let point = self.staged.snapshot();
        self.savepoints.push(point);
    }

    /// Pop a savepoint, rolling staged writes and derived changes back
    /// exactly when `rollback` is set. Dependencies always stay: reads
    /// from failed branches remain.
    pub fn pop_savepoint(&mut self, rollback: bool) {
        if let Some(point) = self.savepoints.pop() {
            if rollback {
                self.staged.truncate(&point);
            } else {
                self.staged.release();
            }
        }
    }

    /// Enter one nested call level, refusing past `max` levels: every
    /// level holds a guest instance and native stack, so depth is capped
    /// long before fuel would stop it.
    pub fn enter_call(&mut self, max: usize) -> bool {
        if self.call_depth >= max {
            return false;
        }
        self.call_depth += 1;
        true
    }

    /// Leave a nested call level entered through [`Invocation::enter_call`].
    pub fn exit_call(&mut self) {
        self.call_depth = self.call_depth.saturating_sub(1);
    }

    /// Record a dependency in the current scope.
    pub fn record_dep(&mut self, dep: Vec<u8>) {
        if let Some(scope) = self.dep_scopes.last_mut() {
            scope.insert(dep);
        }
    }

    /// Push a fresh dependency scope, returning nothing; [`pop_scope`]
    /// takes the collected edges back out.
    ///
    /// [`pop_scope`]: Invocation::pop_scope
    pub fn push_scope(&mut self) {
        self.dep_scopes.push(HashSet::new());
        self.timings.push(Timing::default());
    }

    /// Pop the current dependency scope, returning its edges sorted for
    /// deterministic storage and what it saw of the clock.
    pub fn pop_scope(&mut self) -> (Vec<Vec<u8>>, Timing) {
        let scope = self.dep_scopes.pop().unwrap_or_default();
        let timing = self.timings.pop().unwrap_or_default();
        if self.dep_scopes.is_empty() {
            self.dep_scopes.push(HashSet::new());
            self.timings.push(Timing::default());
        }
        let mut edges: Vec<Vec<u8>> = scope.into_iter().collect();
        edges.sort();
        (edges, timing)
    }

    /// The current scope read a derived value whose evaluation saw
    /// `timing`: its result changes when that value does.
    pub fn inherit(&mut self, timing: Timing) {
        if let Some(current) = self.timings.last_mut() {
            current.merge(timing);
        }
    }

    /// Mark a derived instance as evaluating; `false` means re-entry, a
    /// dependency cycle.
    pub fn begin_eval(&mut self, definition: u32, args: &[u8]) -> bool {
        self.evaluating.insert((definition, args.to_vec()))
    }

    /// Unmark a derived instance after evaluation.
    pub fn end_eval(&mut self, definition: u32, args: &[u8]) {
        self.evaluating.remove(&(definition, args.to_vec()));
    }

    /// Current derived evaluation nesting depth: one per in-flight
    /// evaluation, since each nests inside its invoker's host calls.
    pub fn eval_depth(&self) -> usize {
        self.evaluating.len()
    }

    /// Poison the invocation: the lane fails even if guests catch the
    /// failure. Only unpoisoned invocations can be poisoned; the first
    /// failure wins.
    pub fn poison(&mut self, failure: Failure) {
        if self.poisoned.is_none() {
            self.poisoned = Some(failure);
        }
    }

    /// Take the poison failure, if any.
    pub fn take_poison(&mut self) -> Option<Failure> {
        self.poisoned.take()
    }

    /// Whether the invocation is poisoned, without consuming the failure.
    pub fn is_poisoned(&self) -> bool {
        self.poisoned.is_some()
    }

    /// Declare that the current scope's result changes at `at`: the earliest
    /// instant declared stands, so no later declaration lifts one (a token's
    /// expiry, say).
    pub fn declare_change(&mut self, at: f64) -> HostResult<()> {
        if !at.is_finite() {
            return Err(fail(codes::INVALID_VALUE, "changes_at must be finite"));
        }
        if let Some(current) = self.timings.last_mut() {
            current.declare(at);
        }
        Ok(())
    }

    /// The earliest instant the invocation declared, or inherited from
    /// a derived value it read, if any.
    pub fn changes_at(&self) -> Option<f64> {
        self.timings.first().and_then(|timing| timing.until)
    }

    /// Record that the guest polled time. The current dependency scope
    /// keeps a marker, so a derived outcome that polled tells every
    /// later reader so through its edges, and its timing, so the
    /// outcome serves reads at this instant alone.
    pub fn poll_time(&mut self) {
        self.time_polled = true;
        if let Some(current) = self.timings.last_mut() {
            current.polled = true;
        }
        self.record_dep(crate::certificate::time_dep());
    }

    /// Record a read no dependency names (retained history, timer
    /// rows): a certificate of this scope counts every change.
    pub fn record_volatile(&mut self) {
        self.volatile = true;
        self.record_dep(crate::certificate::volatile_dep());
    }

    /// Whether the invocation read anything no dependency names, at any
    /// depth: retained history, which comes from outside the store.
    pub fn read_volatile(&self) -> bool {
        self.volatile
    }

    /// Record resolving a managed key: a certificate of this scope, and
    /// a derived outcome it computes, depend on the key catalog.
    pub fn record_keys(&mut self) {
        self.record_dep(crate::certificate::keys_dep());
    }

    /// Take the top-level dependency scope: what the invocation itself
    /// read, derived reads unexpanded.
    pub fn take_observed(&mut self) -> Vec<Vec<u8>> {
        let base = std::mem::take(&mut self.dep_scopes[0]);
        let mut deps: Vec<Vec<u8>> = base.into_iter().collect();
        deps.sort();
        deps
    }

    /// Whether the guest polled time.
    pub fn time_polled(&self) -> bool {
        self.time_polled
    }

    /// Swap the writable flag, returning the previous state. Derived
    /// evaluation disables writes around re-entrant guest calls without
    /// holding a borrow across the call.
    pub fn replace_writable(&mut self, writable: bool) -> bool {
        std::mem::replace(&mut self.writable, writable)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn invocation() -> Invocation {
        Invocation::new(
            "tasks".to_string(),
            Vec::new(),
            CallerKind::Mutation,
            None,
            None,
            1700000000000,
            100,
        )
    }

    #[test]
    fn savepoints_truncate_writes_but_keep_deps() {
        let mut session = invocation();
        session
            .staged
            .stage_record(1, b"k".to_vec(), Some(b"v".to_vec()));
        session.record_dep(b"dep-a".to_vec());
        session.push_savepoint();
        session
            .staged
            .stage_record(1, b"k".to_vec(), Some(b"v2".to_vec()));
        session.staged.stage_record(1, b"j".to_vec(), None);
        session.record_dep(b"dep-b".to_vec());
        session.pop_savepoint(true);
        assert_eq!(
            session.staged.staged_record(1, b"k"),
            Some(Some(b"v".to_vec()))
        );
        assert_eq!(session.staged.staged_record(1, b"j"), None);
        let (mut deps, _) = session.pop_scope();
        deps.sort();
        assert_eq!(deps, vec![b"dep-a".to_vec(), b"dep-b".to_vec()]);
    }

    #[test]
    fn committed_savepoints_keep_writes() {
        let mut session = invocation();
        session.push_savepoint();
        session
            .staged
            .stage_record(1, b"k".to_vec(), Some(b"v".to_vec()));
        session.pop_savepoint(false);
        assert_eq!(
            session.staged.staged_record(1, b"k"),
            Some(Some(b"v".to_vec()))
        );
    }

    #[test]
    fn fuel_exhaustion_is_fatal() {
        let mut session = invocation();
        assert!(session.spend(60).is_ok());
        assert!(session.spend(60).is_err());
        assert_eq!(session.fuel(), 0);
    }

    #[test]
    fn capabilities_gate_writes_and_calls() {
        use crate::descriptors::DefKind;
        let session = invocation();
        assert!(session.check_write().is_ok());
        assert!(session.check_call(DefKind::Mutation).is_ok());
        let query = Invocation::new(
            "m".to_string(),
            Vec::new(),
            CallerKind::Query,
            None,
            None,
            0,
            10,
        );
        assert!(query.check_write().is_err());
        assert!(query.check_call(DefKind::Query).is_ok());
        assert!(query.check_call(DefKind::Mutation).is_err());
        let derived = Invocation::new(
            "m".to_string(),
            Vec::new(),
            CallerKind::Derived,
            None,
            None,
            0,
            10,
        );
        assert!(derived.check_call(DefKind::Query).is_err());
        let mut session = invocation();
        assert!(session.replace_writable(false));
        assert!(session.check_write().is_err());
        assert!(!session.replace_writable(true));
        assert!(session.check_write().is_ok());
    }

    #[test]
    fn first_poison_wins() {
        let mut session = invocation();
        session.poison(fail("first", "one"));
        session.poison(fail("second", "two"));
        assert_eq!(session.take_poison().unwrap().code, "first");
        assert!(session.take_poison().is_none());
    }

    #[test]
    fn eval_guards_reentry() {
        let mut session = invocation();
        assert!(session.begin_eval(1, b"a"));
        assert!(!session.begin_eval(1, b"a"));
        session.end_eval(1, b"a");
        assert!(session.begin_eval(1, b"a"));
    }

    #[test]
    fn poison_peeks_without_consuming() {
        let mut session = invocation();
        assert!(!session.is_poisoned());
        session.poison(fail("trap", "boom"));
        assert!(session.is_poisoned());
        assert!(session.is_poisoned());
        assert_eq!(session.take_poison().unwrap().code, "trap");
        assert!(!session.is_poisoned());
    }

    #[test]
    fn eval_depth_counts_inflight() {
        let mut session = invocation();
        assert_eq!(session.eval_depth(), 0);
        session.begin_eval(1, b"a");
        session.begin_eval(2, b"b");
        assert_eq!(session.eval_depth(), 2);
        session.end_eval(1, b"a");
        assert_eq!(session.eval_depth(), 1);
    }

    #[test]
    fn savepoints_restore_derived_staging_exactly() {
        let mut session = invocation();
        session
            .staged
            .stage_derived(b"a".to_vec(), b"a0".to_vec(), Some(vec![b"r1".to_vec()]));
        session
            .staged
            .stage_derived(b"b".to_vec(), b"b0".to_vec(), None);
        session.staged.drop_derived(b"c".to_vec());
        session.push_savepoint();
        // The failed branch re-evaluates a (reordering the old Vec),
        // pins b in place, creates d, evicts b and resurrects c.
        session
            .staged
            .stage_derived(b"a".to_vec(), b"a1".to_vec(), None);
        session.staged.restage_derived(b"b", b"b-pinned".to_vec());
        session
            .staged
            .stage_derived(b"d".to_vec(), b"d1".to_vec(), None);
        session.staged.drop_derived(b"b".to_vec());
        session
            .staged
            .stage_derived(b"c".to_vec(), b"c1".to_vec(), None);
        session.pop_savepoint(true);
        assert_eq!(session.staged.staged_derived(b"a"), Some(&b"a0"[..]));
        assert_eq!(session.staged.staged_derived(b"b"), Some(&b"b0"[..]));
        assert_eq!(session.staged.staged_derived(b"c"), None);
        assert_eq!(session.staged.staged_derived(b"d"), None);
        assert!(!session.staged.derived_dropped(b"b"));
        assert!(session.staged.derived_dropped(b"c"));
        assert_eq!(session.staged.staged_dependents(b"r1"), vec![b"a".to_vec()]);
    }

    #[test]
    fn nested_savepoints_keep_inner_commits_until_outer_rollback() {
        let mut session = invocation();
        session.push_savepoint();
        session.push_savepoint();
        session
            .staged
            .stage_derived(b"a".to_vec(), b"a1".to_vec(), None);
        session.pop_savepoint(false);
        assert_eq!(session.staged.staged_derived(b"a"), Some(&b"a1"[..]));
        session.pop_savepoint(true);
        assert_eq!(session.staged.staged_derived(b"a"), None);
    }

    #[test]
    fn call_depth_is_capped() {
        let mut session = invocation();
        assert!(session.enter_call(2));
        assert!(session.enter_call(2));
        assert!(!session.enter_call(2));
        session.exit_call();
        assert!(session.enter_call(2));
    }

    #[test]
    fn settle_watermark_clamps_on_rollback() {
        let mut session = invocation();
        session
            .staged
            .stage_record(1, b"k".to_vec(), Some(b"v".to_vec()));
        session.push_savepoint();
        session
            .staged
            .stage_record(1, b"j".to_vec(), Some(b"v".to_vec()));
        session.staged.set_settled(2);
        session.pop_savepoint(true);
        assert_eq!(session.staged.settled(), 1);
        assert_eq!(session.staged.journal().len(), 1);
    }
}
