//! Engine: deploy modules and invoke definitions over the store.
//!
//! [`Engine`] owns the group store, the deployed descriptors and runtimes,
//! the history provider, the authorization policy and the unscoped
//! idempotency cache. [`Engine::deploy`] validates a manifest, interns
//! every name the execution layer resolves and stores the code artifact;
//! [`Engine::invoke_query`], [`invoke_mutation`][Engine::invoke_mutation]
//! and [`invoke_transaction`][Engine::invoke_transaction] admit, execute
//! and commit calls through the writer lane.
//!
//! [`Engine::reader`] hands out the read side, an [`EngineReader`] that
//! shares the store and the published catalog (descriptors, runtimes,
//! history and policy) across threads: its queries pin a snapshot plus
//! the catalog committed for it and run in parallel with each other and
//! with the writer. Code changes publish the catalog together with their
//! commit inside [`Engine::exclusive`], so no reader pairs a snapshot
//! with other code.
//!
//! Content hashing is the deployer's job: [`Engine::deploy`] takes the
//! code identity bytes as given and stores the artifact under them, until
//! the crypto layer lands with its hash crate. Scoped retry identities
//! assume cooperative callers: two concurrent lanes may both admit the
//! same identity, but the applier skips the duplicate journal and
//! replays the first receipt, so concurrent duplicates never
//! double-apply.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock, PoisonError, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use flower_sdk::abi::{decode_envelope, encode_err, encode_ok};
use flower_sdk::error::codes;
use flower_sdk::{Failure, Principal, Value, fail};

use crate::auth::{
    Authorized, Presented, Recall, authorize, authorize_certified, foreign_receipt, receipt_owner,
};
use crate::certificate::{Anchor, Certificate, ChangeJournal, ChangeSet, KeySequence, Validity};
use crate::decisions::{DecisionMemo, DecisionMemoStats};
use crate::descriptors::{DefKind, ModuleDef, ReadConsistency, load_module};
use crate::error::{Error, HostResult};
use crate::history::{EmptyHistory, HistoryProvider};
use crate::host::{Env, PermissivePolicy, Policy};
use crate::intent::{
    INTENT_DB, Intent, IntentStatus, check_bound, count_meta, decode_intent, encode_intent,
    ensure_intent_log, intent_counters, intent_ids, seq_meta, sequence_key,
};
use crate::json::{parse_value, render};
use crate::lane::{self, CommitSpec, PlannedCall, ReceiptSpec, Target, next_revision};
use crate::patch::{
    Patch, RecordWrite, apply_journal, decode_failure, decode_multi_patch, decode_patch,
    encode_multi_patch, encode_patch,
};
use crate::placement;
use crate::preview::{self, Preview, PreviewOptions};
use crate::query_cache::{Cached, Check, CodeVersion, Encoded, Entry, QueryCache, QueryCacheStats};
use crate::retry::{
    Admit, Committed, HASH_LEN, IDEMPOTENCY_TTL_MS, Outcome, Receipt, RejectReason, ScopedIdentity,
    UNSCOPED_PREFIX, UnscopedDedup, admit, unscoped_identity_key,
};
use crate::runtime::{CallKind, Runtime};
use crate::session::{CallerKind, Invocation, Staged};
use crate::staging::{
    Retired, RetiredKind, STAGING_PREFIX, StagingPhase, StagingState, code_identity_key,
    decode_state, deployed_versions, diff_plan, encode_state, open_stagings, staging_key,
    target_names,
};
use crate::storage::codec::decode_floors;
use crate::storage::keys::push_sized;
use crate::storage::{GroupStore, ReadTxn, ScanRange, StagedWrite, WriteTxn, meta};
use crate::txn;

mod operator;
mod removal;
mod staged;
mod tasks;

pub use operator::{ModuleSummary, StagedCode};
pub use staged::DEPLOYMENT_REFUSED;

/// Logical-operation fuel (host operations, rows, nested calls) a
/// declared millisecond buys: a host operation at its cheapest takes
/// about a microsecond, so a deadline's worth of them fits, and the
/// deadline, not the fuel, ends a loop of host calls.
pub const FUEL_PER_MS: u64 = 1_000;

/// Logical-operation fuel per invocation of the default deadline: an
/// invocation's fuel scales with the deadline its definition declares
/// ([`fuel_for`]), so nobody states fuel and a definition always gets
/// the same.
pub const DEFAULT_FUEL: u64 = FUEL_PER_MS * 5_000;

/// The logical-operation fuel of an invocation whose definition declares
/// `timeout_ms` (none: the default deadline).
pub fn fuel_for(timeout_ms: Option<u32>) -> u64 {
    let default_ms = crate::runtime::wasm::DEFAULT_TIMEOUT.as_millis() as u64;
    match timeout_ms {
        None => DEFAULT_FUEL,
        Some(ms) => DEFAULT_FUEL.saturating_mul(u64::from(ms)) / default_ms.max(1),
    }
}

/// How long a client-planned (multi-database) transaction may take, all
/// its steps and their authorization together: the longest one
/// invocation runs by default. Its steps share this deadline and the
/// logical fuel it implies.
pub const TRANSACTION_TIMEOUT: std::time::Duration = crate::runtime::wasm::DEFAULT_MAX_TIMEOUT;

/// The one deadline a transaction's planner (or steps) and every call it
/// makes share, `timeout` from now: their own, like what each declares
/// (the caller's deadline stays the caller's).
fn transaction_deadline(timeout: std::time::Duration) -> Option<std::time::Instant> {
    std::time::Instant::now().checked_add(timeout)
}

/// Validate a multi-database step list: non-empty, one step per
/// database, every target named. Linear, so callers run it before the
/// writer, before authorization, on any list a request body holds.
pub fn check_transaction_steps(steps: &[MultiStep]) -> HostResult<()> {
    if steps.is_empty() {
        return Err(fail(
            codes::INVALID_VALUE,
            "a transaction carries at least one step",
        ));
    }
    let mut dbs = HashSet::with_capacity(steps.len());
    for step in steps {
        if step.module.is_empty() || step.method.is_empty() {
            return Err(fail(
                codes::INVALID_VALUE,
                "a transaction step names a module and a method",
            ));
        }
        if !dbs.insert(step.db.as_slice()) {
            return Err(fail(
                codes::INVALID_VALUE,
                "a transaction steps each database at most once",
            ));
        }
    }
    Ok(())
}

/// Begin an invocation of `definition` with what its descriptors
/// declare: the logical fuel and the guest budget its declared deadline
/// implies, and the caller's deadline.
fn begin_invocation(
    defs: Option<&ModuleDef>,
    definition: &str,
    deadline: Option<std::time::Instant>,
    session: impl FnOnce(u64) -> Invocation,
) -> Invocation {
    let declared = defs.and_then(|defs| defs.timeouts.get(definition).copied());
    let mut session = session(fuel_for(declared));
    session.budget = crate::runtime::wasm::Budget {
        declared: declared.map(|ms| std::time::Duration::from_millis(u64::from(ms))),
        within: None,
        caller: deadline,
    };
    session
}
/// Retained unscoped idempotency results.
pub const DEFAULT_DEDUP_CAPACITY: usize = 1024;
/// Most records one backfill page builds in a single transaction.
pub const STAGING_PAGE: usize = 512;

/// One backfill drain's outcome.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StagingProgress {
    /// Whether the backfill finished.
    pub done: bool,
    /// Records scanned by this drain call.
    pub scanned: usize,
}

/// One garbage-collection pass's outcome.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GcProgress {
    /// Whether nothing retired remains.
    pub done: bool,
    /// Keys deleted by this pass.
    pub collected: usize,
}

/// A ready activation: target identity plus the intern bindings the
/// staging allocated. Appliers install bindings verbatim.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Activation {
    /// Target code identity bytes.
    pub identity: [u8; HASH_LEN],
    /// Authoritative `(name bytes, ID)` bindings.
    pub bindings: Vec<(Vec<u8>, u32)>,
}

/// A module's staging row, if any.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StagingStatus {
    /// Whether the backfill (and graph) finished and activation can run.
    pub ready: bool,
    /// Collections in the backfill plan.
    pub planned: usize,
    /// Plan position of the drain cursor.
    pub position: usize,
    /// Retired structures awaiting collection.
    pub retired: usize,
    /// The phase: building, rebuilding, ready or collecting.
    pub phase: StagingPhase,
    /// Whether the staging replicates, every member holding it alike.
    pub replicated: bool,
    /// Derived definitions whose roots the graph phase rebuilds.
    pub graph: usize,
    /// Graph position of the rebuild cursor.
    pub graph_position: usize,
    /// Roots the graph phase rebuilt so far.
    pub rebuilt: u64,
}

impl StagingStatus {
    /// A staging row as operators see it.
    pub fn of(state: &StagingState) -> Self {
        StagingStatus {
            ready: state.phase == StagingPhase::Ready,
            planned: state.plan.len(),
            position: state.plan_pos,
            retired: state.retired.len(),
            phase: state.phase,
            replicated: state.replicated,
            graph: state.graph.iter().filter(|def| def.walk).count(),
            graph_position: state.graph_pos,
            rebuilt: state.rebuilt,
        }
    }
}

/// Purge the retired entries of structures `target` re-adds: a re-added
/// collection or index reuses its interned ID, so stale rows must go
/// before the fresh backfill, and the retirement must go with them or
/// the collector would later delete the rebuilt structure. Every
/// structure the target declares counts, not only the plan's: a
/// collection re-added with no new index has nothing to backfill.
fn purge_readded(
    write: &WriteTxn,
    db: &[u8],
    module: &str,
    target: &ModuleDef,
    retired: &mut Vec<Retired>,
) -> Result<(), Error> {
    let id = |qualified: String| write.intern_id(db, qualified.as_bytes());
    for collection in &target.collections {
        let held = |retired: &[Retired], kind: RetiredKind, id: u32| {
            retired
                .iter()
                .any(|retired| retired.kind == kind && retired.id == id)
        };
        if let Some(collection_id) = id(crate::host::qualify_collection(module, &collection.name))?
            && held(retired, RetiredKind::Collection, collection_id)
        {
            purge_records(write, db, collection_id)?;
            retired.retain(|retired| {
                !(retired.kind == RetiredKind::Collection && retired.id == collection_id)
            });
        }
        for index in &collection.indexes {
            let qualified = crate::host::qualify_index(module, &collection.name, &index.name);
            if let Some(index_id) = id(qualified)?
                && held(retired, RetiredKind::Index, index_id)
            {
                purge_index(write, db, index_id)?;
                retired.retain(|retired| {
                    !(retired.kind == RetiredKind::Index && retired.id == index_id)
                });
            }
        }
    }
    Ok(())
}

/// Delete every entry below one index prefix in bounded pages.
fn purge_index(write: &WriteTxn, db: &[u8], index: u32) -> Result<(), Error> {
    let mut after = None;
    loop {
        let (_, resume) = write.delete_index_prefix(db, index, after.as_deref(), STAGING_PAGE)?;
        match resume {
            None => return Ok(()),
            Some(cursor) => after = Some(cursor),
        }
    }
}

/// Abandon a staging not yet activated inside `write`; its target never
/// activates. Added indexes under intern IDs only this member bound
/// purge now, and those IDs release, so a replicated binding may claim
/// them without inheriting stale entries. Added indexes under
/// replicated IDs, and a replicated staging's staged graph, retire to
/// lazy collection. Returns the row that remains: collection-only, or
/// `None` when nothing is left to collect.
fn abandon_staging(
    write: &WriteTxn,
    db: &[u8],
    state: StagingState,
) -> Result<Option<StagingState>, Error> {
    let mut retired = state.retired;
    let mut retire = |kind: RetiredKind, id: u32| {
        if !retired
            .iter()
            .any(|known| known.kind == kind && known.id == id)
        {
            retired.push(Retired::new(kind, id));
        }
    };
    for collection in &state.plan {
        for index in &collection.added {
            if state.local.contains(&index.id) {
                purge_index(write, db, index.id)?;
            } else {
                retire(RetiredKind::Index, index.id);
            }
        }
    }
    for def in &state.graph {
        retire(RetiredKind::Derived, def.staged);
    }
    // Local collections and definitions never served, so nothing else
    // lives under their IDs.
    for id in &state.local {
        write.release_intern(db, *id)?;
    }
    if retired.is_empty() {
        return Ok(None);
    }
    Ok(Some(StagingState::collecting(
        state.identity,
        retired,
        state.replicated,
    )))
}

/// Write a module's staging row, or delete it for `None`.
fn put_staging(
    write: &WriteTxn,
    db: &[u8],
    module: &str,
    state: Option<&StagingState>,
) -> Result<(), Error> {
    match state {
        Some(state) => write.set_meta(db, &staging_key(module), &encode_state(state)?),
        None => write.delete_meta(db, &staging_key(module)).map(drop),
    }
}

/// The fields of the index an aggregate's groups follow, as a module
/// declares them.
fn aggregate_fields<'a>(
    defs: &'a ModuleDef,
    aggregate: &crate::descriptors::AggregateDef,
) -> Option<&'a [String]> {
    defs.collection(&aggregate.collection)?
        .indexes
        .iter()
        .find(|index| index.name == aggregate.index)
        .map(|index| index.fields.as_slice())
}

/// Drop the IDs an applied activation bound from every open staging's
/// local set: replicated now, every member knows them.
fn clear_local(write: &WriteTxn, db: &[u8], bindings: &[(Vec<u8>, u32)]) -> Result<(), Error> {
    let rows = write.scan_meta_prefix(db, STAGING_PREFIX)?;
    for (module, mut state) in open_stagings(rows)? {
        let before = state.local.len();
        state
            .local
            .retain(|id| !bindings.iter().any(|(_, bound)| bound == id));
        if state.local.len() != before {
            put_staging(write, db, &module, Some(&state))?;
        }
    }
    Ok(())
}

/// Delete every record below one collection prefix in bounded pages.
fn purge_records(write: &WriteTxn, db: &[u8], collection: u32) -> Result<(), Error> {
    let mut after = None;
    loop {
        let (_, resume) =
            write.delete_record_prefix(db, collection, after.as_deref(), STAGING_PAGE)?;
        match resume {
            None => return Ok(()),
            Some(cursor) => after = Some(cursor),
        }
    }
}

/// Options shared by every invocation.
#[derive(Clone, Debug, Default)]
pub struct CallOptions {
    /// The principal the caller asserts, or `None` for anonymous. A
    /// module with an authorization hook decides for itself (the hook
    /// sees this as `principal`); without one the call runs as this.
    pub principal: Option<Principal>,
    /// When the caller stops waiting: guest code past it fails
    /// `deadline_exceeded`. It only ever shortens the deadline the
    /// definition declares, and never changes its fuel.
    pub deadline: Option<std::time::Instant>,
    /// Timestamp override; defaults to the current time. Tests pin this
    /// for deterministic execution.
    pub timestamp_ms: Option<u64>,
    /// Credentials for the module's authorization hook: opaque to the
    /// engine, and never part of arguments, fingerprints or receipts.
    pub credentials: Option<Value>,
    /// Whether the definition name is an external HTTP alias, resolved
    /// through the module's allowlist, rather than an internal name.
    pub external: bool,
}

impl CallOptions {
    /// The authentication input these options present.
    fn presented(&self, timestamp_ms: u64) -> Presented<'_> {
        Presented {
            principal: self.principal.as_ref(),
            credentials: self.credentials.as_ref(),
            external: self.external,
            timestamp_ms,
            deadline: self.deadline,
        }
    }
}

/// A scoped retry identity plus the intent fingerprint its receipt pins.
#[derive(Clone, Debug)]
pub struct RetryIdentity {
    /// History incarnation, session epoch and stable intent bytes.
    pub identity: ScopedIdentity,
    /// Fingerprint over business intent plus explicit preconditions.
    pub fingerprint: [u8; HASH_LEN],
}

/// Options for mutations and transactions: call options plus at most one
/// of a scoped retry identity or an unscoped idempotency key.
#[derive(Clone, Debug, Default)]
pub struct MutationOptions {
    /// Shared call options.
    pub call: CallOptions,
    /// Scoped retry identity with its fingerprint.
    pub retry: Option<RetryIdentity>,
    /// Unscoped caller-chosen idempotency key: first result wins
    /// until the receipt TTL past the call timestamp, backed by a
    /// durable receipt plus a bounded in-memory fast path.
    pub idempotency_key: Option<Vec<u8>>,
    /// Revision the submission was written against: the commit or apply
    /// rejects when the database moved.
    pub expected_revision: Option<u64>,
}

/// A prepared patch ready to propose, or an admission short-circuit to
/// return without proposing.
#[derive(Clone, Debug)]
pub enum PrepareOutcome {
    /// Propose this patch through consensus.
    Patch(lane::PreparedPatch),
    /// Admission decided without executing: return as-is.
    Done(HostResult<Value>),
    /// A stored receipt answered: its recorded outcome, and where that
    /// committed when the receipt records it. `None` means the receipt
    /// predates stamping, or was read from a writer's staged, not yet
    /// committed, state.
    Replayed(HostResult<Value>, Option<Committed>),
}

/// One multi-database transaction step: a mutation call against one
/// database. Every step in a transaction names a different database.
#[derive(Clone, Debug, PartialEq)]
pub struct MultiStep {
    /// Target database identifier bytes.
    pub db: Vec<u8>,
    /// Target module identity.
    pub module: String,
    /// Target mutation name.
    pub method: String,
    /// Call arguments.
    pub args: Value,
}

/// A transaction preserves its failing step separately from the callee's details.
pub fn transaction_failure(mut failure: Failure, index: usize, step: &MultiStep) -> Failure {
    let context = std::collections::BTreeMap::from([
        ("step".into(), Value::Number(index as f64)),
        (
            "database".into(),
            Value::String(String::from_utf8_lossy(&step.db).into_owned()),
        ),
        ("module".into(), Value::String(step.module.clone())),
        ("method".into(), Value::String(step.method.clone())),
        ("code".into(), Value::String(failure.code.clone())),
        ("message".into(), Value::String(failure.message.clone())),
    ]);
    let mut details = std::collections::BTreeMap::new();
    details.insert("transaction".into(), Value::Object(context));
    details.insert(
        "cause".into(),
        failure.details.take().unwrap_or(Value::Null),
    );
    failure.details = Some(Value::Object(details));
    failure
}

/// A multi-database patch checked against local state: one checked
/// patch per participant plus the transaction-wide rejection, if any.
/// Any rejection commits the whole batch as applied-only.
#[derive(Clone, Debug)]
pub struct MultiChecked {
    /// One checked patch per step, in step order.
    pub patches: Vec<CheckedPatch>,
    /// First deterministic rejection across participants, if any.
    pub rejected: Option<Failure>,
}

impl CheckedPatch {
    /// The records `(put, deleted)` its commit wrote, given the
    /// database's `revision` after it: its journal's when the revision
    /// moved, nothing when it did not (a duplicate, a rejection, a
    /// refusal, a business failure).
    pub fn records(&self, revision: u64) -> (u64, u64) {
        if revision > self.revision {
            self.patch.record_counts()
        } else {
            (0, 0)
        }
    }
}

impl MultiChecked {
    /// The records `(put, deleted)` each participant's commit wrote,
    /// given the `(database, revision)` pairs after it, in step order
    /// (see [`CheckedPatch::records`]).
    pub fn records(&self, revisions: &[(Vec<u8>, u64)]) -> Vec<(u64, u64)> {
        revisions
            .iter()
            .enumerate()
            .map(|(step, (_, revision))| {
                self.patches
                    .get(step)
                    .map_or((0, 0), |checked| checked.records(*revision))
            })
            .collect()
    }
}

/// A committed multi-database patch: per-participant results and
/// revisions. All participants commit or none do.
#[derive(Clone, Debug)]
pub struct MultiApplyOutcome {
    /// Per-step results in step order.
    pub results: Vec<Result<Value, Failure>>,
    /// Per-participant `(database, revision)` pairs in step order. For
    /// a duplicate whose receipt records it, the first participant's is
    /// the revision the original left there (the one the response
    /// leads with); the others are their current revisions.
    pub revisions: Vec<(Vec<u8>, u64)>,
    /// For a duplicate whose receipt records it, the log position the
    /// original committed at; `None` answers with this entry's.
    pub replayed_position: Option<u64>,
    /// Whether a stored receipt answered: the original committed
    /// earlier and nothing ran again (v1's `duplicate`).
    pub duplicate: bool,
}

impl MultiApplyOutcome {
    /// Combine per-step results into the transaction result: the array
    /// of step values when every step succeeded, else the first step
    /// failure. Committed multi-database patches from
    /// [`Engine::prepare_transaction_multi`] never carry step
    /// failures: a failing step aborts before any patch exists.
    pub fn combined(&self) -> Result<Value, Failure> {
        let mut values = Vec::with_capacity(self.results.len());
        for result in &self.results {
            match result {
                Ok(value) => values.push(value.clone()),
                Err(failure) => return Err(failure.clone()),
            }
        }
        Ok(Value::Array(values))
    }
}

/// A prepared timer fire: a fire patch, a retry patch, a hold, or
/// nothing.
#[derive(Clone, Debug)]
pub enum FirePrepare {
    /// Missing, dead, not due, or held for a module still removed:
    /// propose nothing.
    Skip,
    /// Propose: timer deletion plus handler effects, revision-guarded.
    Patch(lane::PreparedPatch),
    /// Propose: retry update or death, revision-guarded. The handler
    /// failed or vanished; its failure rolled back first.
    Retry(lane::PreparedPatch),
    /// Propose: the timer held for its removed module, attempts
    /// untouched, revision-guarded.
    Hold(lane::PreparedPatch),
}

/// An applied patch's outcome: the replicated result and the revision
/// the database now carries.
#[derive(Clone, Debug)]
pub struct ApplyOutcome {
    /// The replicated value or business failure.
    pub result: Result<Value, Failure>,
    /// Database revision after the apply; for a duplicate whose receipt
    /// records it, the revision the original left.
    pub revision: u64,
    /// For a duplicate whose receipt records it, the log position the
    /// original committed at; `None` answers with this entry's.
    pub replayed_position: Option<u64>,
    /// Whether a stored receipt answered: the original committed
    /// earlier and nothing ran again (v1's `duplicate`). Receipts
    /// written before they recorded where they committed answer as
    /// duplicates too, with no position.
    pub duplicate: bool,
}

/// A patch checked against local state and ready to commit: the
/// decoded journal, the revision it applies against and any
/// deterministic rejection decided at check time.
#[derive(Clone, Debug)]
pub struct CheckedPatch {
    /// The decoded journal.
    pub patch: Patch,
    /// Committed revision the patch applies against.
    pub revision: u64,
    /// Deterministic rejection, committed as applied-only.
    pub rejected: Option<Failure>,
}

/// Shared admission's answer: a final result or a lane invocation.
enum BeginOutcome {
    /// Admission decided without executing.
    Done(HostResult<Value>),
    /// A stored receipt answered, with where it committed (see
    /// [`PrepareOutcome::Replayed`]).
    Replayed(HostResult<Value>, Option<Committed>),
    /// Proceed to the lane with a pinned snapshot and session.
    Proceed {
        /// Pinned snapshot for the invocation.
        read: ReadTxn,
        /// Fresh mutation session, boxed to keep the outcome small.
        session: Box<Invocation>,
        /// Staged receipt spec for retry identities.
        receipt: Option<ReceiptSpec>,
        /// The definition the call resolved to.
        definition: String,
    },
}

/// One database's active modules: descriptors and runtimes by module.
/// Code identity, staging and interning are per database, so the
/// deployed bundles are too: activating a module in one database never
/// swaps another database's code.
#[derive(Clone, Default)]
struct DbModules {
    descriptors: HashMap<String, ModuleDef>,
    runtimes: HashMap<String, Arc<dyn Runtime>>,
}

/// The modules of a database nothing is deployed to.
static NO_MODULES: LazyLock<DbModules> = LazyLock::new(DbModules::default);

/// Everything an invocation reads besides its snapshot: every
/// database's active modules, the history provider and the policy.
/// A version never changes once built: the writer derives the next one
/// and publishes it whole, so readers holding a version keep a
/// consistent view of the code.
#[derive(Clone)]
pub(crate) struct Catalog {
    modules: HashMap<Vec<u8>, Arc<DbModules>>,
    history: Arc<dyn HistoryProvider>,
    policy: Arc<dyn Policy>,
}

impl Catalog {
    /// One database's active modules.
    fn modules(&self, db: &[u8]) -> &DbModules {
        self.modules.get(db).map_or(&NO_MODULES, Arc::as_ref)
    }

    /// The environment invocations against one database run in.
    pub(crate) fn env(&self, db: &[u8]) -> Env<'_> {
        let modules = self.modules(db);
        Env {
            runtimes: &modules.runtimes,
            descriptors: &modules.descriptors,
            history: self.history.as_ref(),
            policy: self.policy.as_ref(),
        }
    }
}

/// What the writer shares with readers: the store and the published
/// catalog.
pub(crate) struct Shared {
    pub(crate) store: GroupStore,
    /// The catalog readers capture. A code change holds the write lock
    /// across its store commit and the publish, and readers capture
    /// their snapshot under the read lock, so a reader always pairs a
    /// snapshot with the catalog committed for it.
    catalog: RwLock<Arc<Catalog>>,
    /// Recent commits' changes: applied patches record theirs before
    /// they commit.
    journal: ChangeJournal,
    /// Managed-key catalog changes on this member ([`KeyChange`]).
    keys: KeySequence,
    /// Query results reused while their certificates hold.
    queries: QueryCache,
    /// Authorization decisions reused while they hold.
    decisions: DecisionMemo,
}

impl Shared {
    /// What a decision on a committed snapshot of `db`, taken after
    /// `marks` with `catalog`, may be recalled from and kept in.
    fn recall(&self, catalog: &Catalog, db: &[u8], marks: Marks) -> Recall<'_> {
        Recall {
            memo: &self.decisions,
            journal: &self.journal,
            keys: &self.keys,
            code: CodeVersion::of(catalog.modules.get(db)),
            journal_mark: marks.journal,
            keys_mark: marks.keys,
        }
    }

    /// Whether `captured` is still the published catalog. A code change
    /// in flight holds the lock: it counts as moved, and the caller,
    /// holding the store's write lock, never waits on it.
    fn current(&self, captured: &Arc<Catalog>) -> bool {
        self.catalog
            .try_read()
            .is_ok_and(|published| Arc::ptr_eq(&published, captured))
    }

    /// The marks a reader takes before its snapshot.
    fn marks(&self) -> Marks {
        Marks {
            journal: self.journal.mark(),
            keys: self.keys.mark(),
        }
    }
}

/// What a reader takes before capturing its snapshot: the change
/// journal's mark, for anchors, and the key catalog's, for what it
/// computes from managed keys.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Marks {
    journal: u64,
    keys: u64,
}

/// A managed-key catalog change in flight on this member, from
/// [`Engine::begin_key_change`] until dropped. While one is, and for
/// readers whose snapshot predates one, derived values computed from a
/// key do not persist and watches that resolved a key count as behind.
pub struct KeyChange {
    shared: Arc<Shared>,
}

impl Drop for KeyChange {
    fn drop(&mut self) {
        self.shared.keys.end();
    }
}

/// The engine's read side: a cloneable, thread-safe handle that runs
/// queries without the writer. Each query pins a redb snapshot plus the
/// catalog committed for it, so queries run in parallel with each other
/// and with the writer's commits, and see exactly what a query on the
/// writer would have seen at the instant of capture.
#[derive(Clone)]
pub struct EngineReader {
    shared: Arc<Shared>,
}

impl EngineReader {
    /// Borrow the group store, for read transactions.
    pub fn store(&self) -> &GroupStore {
        &self.shared.store
    }

    /// Pin a snapshot and the catalog committed for it, plus the change
    /// journal's mark from just before the snapshot.
    fn capture(&self) -> HostResult<(Arc<Catalog>, ReadTxn, Marks)> {
        let published = self
            .shared
            .catalog
            .read()
            .unwrap_or_else(PoisonError::into_inner);
        let mark = self.shared.marks();
        let read = self.shared.store.begin_read().map_err(Failure::from)?;
        Ok((published.clone(), read, mark))
    }

    /// The key catalog's current mark: it moves when a managed-key
    /// change begins and when it ends (see [`ReadView::key_mark`]).
    pub fn key_mark(&self) -> u64 {
        self.shared.keys.mark()
    }

    /// Query cache counters and occupancy.
    pub fn query_cache_stats(&self) -> QueryCacheStats {
        self.shared.queries.stats()
    }

    /// Bound the query cache to `bytes`; zero disables it.
    pub fn set_query_cache_bytes(&self, bytes: usize) {
        self.shared.queries.set_budget(bytes);
    }

    /// Authorization decision memo counters and occupancy.
    pub fn decision_memo_stats(&self) -> DecisionMemoStats {
        self.shared.decisions.stats()
    }

    /// Bound the authorization decision memo to `bytes`; zero disables
    /// it, and every call runs its module's hook.
    pub fn set_decision_memo_bytes(&self, bytes: usize) {
        self.shared.decisions.set_budget(bytes);
    }

    /// The version of `db`'s code readers capture now.
    pub fn code_version(&self, db: &[u8]) -> CodeVersion {
        let published = self
            .shared
            .catalog
            .read()
            .unwrap_or_else(PoisonError::into_inner);
        CodeVersion::of(published.modules.get(db))
    }

    /// Whether nothing committed to `db` after `anchor` through
    /// `revision` touches `certificate`; `false` when the journal cannot
    /// tell.
    pub fn unchanged(
        &self,
        db: &[u8],
        anchor: Anchor,
        revision: u64,
        certificate: &Certificate,
    ) -> bool {
        self.shared
            .journal
            .unchanged(db, anchor, revision, certificate)
    }

    /// Invoke a query against a fresh snapshot, as
    /// [`Engine::invoke_query`] does on the writer.
    pub fn invoke_query(
        &self,
        db: &[u8],
        module: &str,
        name: &str,
        args: &Value,
        options: &CallOptions,
    ) -> HostResult<Value> {
        self.query_at(db, module, name, args, options)
            .map(|(value, _)| value)
    }

    /// Pin a snapshot and its catalog as a [`ReadView`].
    pub fn view(&self) -> HostResult<ReadView> {
        let (catalog, read, mark) = self.capture()?;
        Ok(ReadView {
            shared: self.shared.clone(),
            catalog,
            read,
            mark,
        })
    }

    /// The revision of `db` a snapshot captured now reads.
    pub fn revision(&self, db: &[u8]) -> HostResult<u64> {
        let read = self.shared.store.begin_read().map_err(Failure::from)?;
        read.meta_u64(db, meta::REVISION).map_err(Failure::from)
    }

    /// Invoke a query and report the database revision of the snapshot
    /// it read, both from one capture.
    pub fn query_at(
        &self,
        db: &[u8],
        module: &str,
        name: &str,
        args: &Value,
        options: &CallOptions,
    ) -> HostResult<(Value, u64)> {
        let call = QueryCall {
            db,
            module,
            name,
            args,
            options,
        };
        let (answer, revision) = self.answer_at(call, Output::Value)?;
        Ok((answer.into_value()?, revision))
    }

    /// [`EngineReader::query_at`] for a caller that only sends the result
    /// on: its JSON as the HTTP API writes it ([`Encoded`]), which a
    /// cached result answers with as it keeps it, without cloning or
    /// rendering the value.
    pub fn query_encoded_at(
        &self,
        db: &[u8],
        module: &str,
        name: &str,
        args: &Value,
        options: &CallOptions,
    ) -> HostResult<(Encoded, u64)> {
        let call = QueryCall {
            db,
            module,
            name,
            args,
            options,
        };
        let (answer, revision) = self.answer_at(call, Output::Encoded)?;
        Ok((answer.into_encoded()?, revision))
    }

    /// Answer a query on a fresh capture, in the form asked.
    fn answer_at(&self, call: QueryCall<'_>, output: Output) -> HostResult<(Answer, u64)> {
        let (catalog, read, mark) = self.capture()?;
        self.evaluate(&catalog, &read, mark, call, output)
    }

    /// A query's declared read consistency in the published catalog;
    /// `None` when no such query is deployed.
    pub fn query_consistency(
        &self,
        db: &[u8],
        module: &str,
        name: &str,
    ) -> Option<ReadConsistency> {
        let published = self
            .shared
            .catalog
            .read()
            .unwrap_or_else(PoisonError::into_inner);
        published
            .modules(db)
            .descriptors
            .get(module)?
            .read_consistency(name)
    }

    /// The declared read consistency of the query a call names, through
    /// the module's alias allowlist when `options` mark the call
    /// external (an HTTP alias), as the call itself will resolve it.
    pub fn call_consistency(
        &self,
        db: &[u8],
        module: &str,
        name: &str,
        options: &CallOptions,
    ) -> Option<ReadConsistency> {
        let published = self
            .shared
            .catalog
            .read()
            .unwrap_or_else(PoisonError::into_inner);
        published
            .modules(db)
            .descriptors
            .get(module)?
            .call_consistency(name, options.external)
    }

    /// [`EngineReader::query_at`] for a query the caller found declared
    /// `expected` (replica-local), without proving freshness: `None`,
    /// evaluating nothing, when the catalog captured with the snapshot
    /// declares it otherwise (a deployment changed it in between), so
    /// the caller decides again.
    pub fn query_replica_local(
        &self,
        db: &[u8],
        module: &str,
        name: &str,
        args: &Value,
        options: &CallOptions,
        expected: ReadConsistency,
    ) -> HostResult<Option<(Value, u64)>> {
        let call = QueryCall {
            db,
            module,
            name,
            args,
            options,
        };
        self.answer_replica_local(call, expected, Output::Value)?
            .map(|(answer, revision)| Ok((answer.into_value()?, revision)))
            .transpose()
    }

    /// [`EngineReader::query_replica_local`] answering with the result's
    /// JSON as the HTTP API writes it, as [`EngineReader::query_encoded_at`] does.
    pub fn query_replica_local_encoded(
        &self,
        db: &[u8],
        module: &str,
        name: &str,
        args: &Value,
        options: &CallOptions,
        expected: ReadConsistency,
    ) -> HostResult<Option<(Encoded, u64)>> {
        let call = QueryCall {
            db,
            module,
            name,
            args,
            options,
        };
        self.answer_replica_local(call, expected, Output::Encoded)?
            .map(|(answer, revision)| Ok((answer.into_encoded()?, revision)))
            .transpose()
    }

    /// Answer a replica-local query on a fresh capture, in the form
    /// asked; `None` when the capture's catalog declares it otherwise.
    fn answer_replica_local(
        &self,
        call: QueryCall<'_>,
        expected: ReadConsistency,
        output: Output,
    ) -> HostResult<Option<(Answer, u64)>> {
        let (catalog, read, mark) = self.capture()?;
        let declared = catalog
            .modules(call.db)
            .descriptors
            .get(call.module)
            .and_then(|defs| defs.call_consistency(call.name, call.options.external));
        if !matches!(expected, ReadConsistency::ReplicaLocal { .. }) || declared != Some(expected) {
            return Ok(None);
        }
        self.evaluate(&catalog, &read, mark, call, output).map(Some)
    }

    /// Evaluate a query on a captured snapshot and catalog, with the
    /// snapshot's revision, reusing a cached result its certificate
    /// still vouches for.
    fn evaluate(
        &self,
        catalog: &Arc<Catalog>,
        read: &ReadTxn,
        mark: Marks,
        call: QueryCall<'_>,
        output: Output,
    ) -> HostResult<(Answer, u64)> {
        let revision = read
            .meta_u64(call.db, meta::REVISION)
            .map_err(Failure::from)?;
        let captured = Captured {
            shared: &self.shared,
            catalog,
            read,
            mark,
            reuse: true,
        };
        let (answer, _) = run_query(&captured, call, output)?;
        Ok((answer, revision))
    }
}

/// The form a caller wants a query's result in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Output {
    /// The value.
    Value,
    /// Its JSON as the HTTP API writes it, only.
    Encoded,
}

/// A query's result, in the form asked.
enum Answer {
    Value(Value),
    Encoded(Encoded),
}

impl Answer {
    /// The value, decoding a rendering ([`run_authorized`] answers in
    /// the form asked, so this only converts).
    fn into_value(self) -> HostResult<Value> {
        match self {
            Answer::Value(value) => Ok(value),
            Answer::Encoded(encoded) => parse_value(encoded.bytes()).map_err(Failure::from),
        }
    }

    /// Its JSON, rendering a value.
    fn into_encoded(self) -> HostResult<Encoded> {
        match self {
            Answer::Encoded(encoded) => Ok(encoded),
            Answer::Value(value) => Encoded::render(&value),
        }
    }
}

/// One capture's snapshot, catalog and journal mark, as queries on it
/// need them.
struct Captured<'a> {
    shared: &'a Shared,
    catalog: &'a Arc<Catalog>,
    read: &'a ReadTxn,
    mark: Marks,
    /// Whether queries on it use the query cache: readers do, the
    /// writer lane evaluates every query.
    reuse: bool,
}

/// One query invocation's target and options.
struct QueryCall<'a> {
    db: &'a [u8],
    module: &'a str,
    name: &'a str,
    args: &'a Value,
    options: &'a CallOptions,
}

/// Refuse a read of a database that does not serve in this snapshot:
/// frozen, moving or retired in its group (see [`crate::placement`]).
/// A frozen source must not answer once its destination may take
/// writes, and a retired one has nothing to answer with.
fn serving(read: &ReadTxn, db: &[u8]) -> HostResult<()> {
    match placement::read_refusal(read, db).map_err(Failure::from)? {
        Some(failure) => Err(failure),
        None => Ok(()),
    }
}

/// Authorize and run one query over a capture (see [`run_authorized`]).
fn run_query(
    captured: &Captured<'_>,
    call: QueryCall<'_>,
    output: Output,
) -> HostResult<(Answer, Arc<Certificate>)> {
    let QueryCall {
        db,
        module,
        name,
        args,
        options,
    } = call;
    serving(captured.read, db)?;
    let timestamp = options.timestamp_ms.unwrap_or_else(now_ms);
    // Readers recall decisions, as they reuse results; the writer lane
    // decides every call afresh.
    let recall = captured
        .reuse
        .then(|| captured.shared.recall(captured.catalog, db, captured.mark));
    let authorized = authorize(
        recall.as_ref(),
        captured.read,
        &captured.catalog.env(db),
        db,
        module,
        name,
        args,
        &options.presented(timestamp),
    )?;
    run_authorized(captured, db, &authorized, args, options, output)
}

/// Run one query as an earlier authorization decided, without deciding
/// again, with the certificate of what it observed. On a reusing
/// capture, a cached result answers when its certificate still holds
/// there (see [`crate::query_cache`]); otherwise the query evaluates,
/// and its result enters the cache when its certificate allows.
/// Derived caches the evaluation computed install only while
/// `captured` is current and no key change began since its marks (see
/// [`lane::run_query`]). The answer comes in the form `output` asks:
/// a cached result answers with the value or the rendering it keeps,
/// and an evaluation renders once, for the cache and the caller alike.
fn run_authorized(
    captured: &Captured<'_>,
    db: &[u8],
    authorized: &Authorized,
    args: &Value,
    options: &CallOptions,
    output: Output,
) -> HostResult<(Answer, Arc<Certificate>)> {
    let Captured {
        shared,
        catalog,
        read,
        mark,
        reuse,
    } = *captured;
    serving(read, db)?;
    let timestamp = options.timestamp_ms.unwrap_or_else(now_ms);
    let revision = read.meta_u64(db, meta::REVISION).map_err(Failure::from)?;
    let code = CodeVersion::of(catalog.modules.get(db));
    let key = QueryCache::key(db, authorized, args).filter(|_| reuse);
    // Values derived again for a touched result, when one came out
    // different: the evaluation starts from them.
    let mut derived = None;
    if let Some(key) = key.as_deref()
        && let Some(cached) = shared.queries.get(key)
    {
        let answered = |cached: &Cached, certificate: Arc<Certificate>| {
            shared.queries.hit();
            let answer = match output {
                Output::Value => Answer::Value(Value::clone(&cached.value)),
                Output::Encoded => Answer::Encoded(cached.encoded.clone()),
            };
            (answer, certificate)
        };
        match cached.check(&shared.journal, db, &code, revision, timestamp) {
            Check::Valid => return Ok(answered(&cached, cached.certificate.clone())),
            Check::Touched(changes) => {
                let call = Rederive {
                    captured,
                    db,
                    authorized,
                    options,
                    key,
                    revision,
                    timestamp,
                };
                match rederive(call, &cached, &changes, &code) {
                    Standing::Stands(certificate) => {
                        shared.queries.rederived();
                        return Ok(answered(&cached, certificate));
                    }
                    Standing::Changed(staged) => derived = Some(*staged),
                    Standing::Falls => {}
                }
            }
            Check::Invalid => {}
        }
    }
    if reuse {
        shared.queries.miss();
    }
    let mut session = begin_invocation(
        catalog.modules(db).descriptors.get(authorized.module()),
        authorized.definition(),
        options.deadline,
        |fuel| {
            Invocation::new(
                authorized.module().to_string(),
                db.to_vec(),
                CallerKind::Query,
                authorized.principal().cloned(),
                None,
                timestamp,
                fuel,
            )
        },
    );
    if let Some(staged) = derived {
        session.staged = staged;
    }
    let target = Target {
        kind: CallKind::Query,
        module: authorized.module(),
        name: authorized.definition(),
        args,
    };
    let (value, certificate, graph) = lane::run_query_observed(
        &shared.store,
        read,
        &mut session,
        &catalog.env(db),
        target,
        // A key change since the snapshot may have served the old
        // catalog over state it already invalidated: such caches drop.
        &|| shared.current(catalog) && shared.keys.settled(mark.keys),
    )?;
    let certificate = Arc::new(certificate);
    if let Some(key) = key
        && QueryCache::admits(&certificate)
        && let Some(anchor) = shared.journal.anchor(db, mark.journal, revision)
        && let Ok(encoded) = Encoded::render(&value)
    {
        let value = Arc::new(value);
        let answer = match output {
            Output::Value => Answer::Value(Value::clone(&value)),
            Output::Encoded => Answer::Encoded(encoded.clone()),
        };
        let entry = Entry {
            value,
            encoded,
            certificate: certificate.clone(),
            code,
            evaluated_ms: timestamp,
            graph: graph.map(Arc::new),
        };
        if shared.queries.insert(key, entry, anchor) {
            shared.queries.kept();
        }
        return Ok((answer, certificate));
    }
    let answer = match output {
        Output::Value => Answer::Value(value),
        Output::Encoded => Answer::Encoded(Encoded::render(&value)?),
    };
    Ok((answer, certificate))
}

/// A cached result's call, for [`rederive`].
struct Rederive<'a> {
    captured: &'a Captured<'a>,
    db: &'a [u8],
    authorized: &'a Authorized,
    options: &'a CallOptions,
    key: &'a [u8],
    revision: u64,
    timestamp: u64,
}

/// Whether a touched result stands (see [`rederive`]).
enum Standing {
    /// It does, with this certificate.
    Stands(Arc<Certificate>),
    /// A derived value it read came out different: it evaluates from
    /// the values derived again.
    Changed(Box<Staged>),
    /// It evaluates.
    Falls,
}

/// Keep `cached`, which `changes` touched, when they touched it only
/// through the derived values it read and those come out as they were
/// once the ones the changes touched derive again on this capture (see
/// [`crate::certificate::Graph`]): the same result's new certificate,
/// with the entry anchored here.
fn rederive(
    call: Rederive<'_>,
    cached: &Cached,
    changes: &[Arc<ChangeSet>],
    code: &CodeVersion,
) -> Standing {
    let Rederive {
        captured,
        db,
        authorized,
        options,
        key,
        revision,
        timestamp,
    } = call;
    let Captured {
        shared,
        catalog,
        read,
        mark,
        ..
    } = *captured;
    let Some(graph) = cached.graph.as_ref() else {
        return Standing::Falls;
    };
    let Some(touched) = graph.touched(changes) else {
        return Standing::Falls;
    };
    let mut session = begin_invocation(
        catalog.modules(db).descriptors.get(authorized.module()),
        authorized.definition(),
        options.deadline,
        |fuel| {
            Invocation::new(
                authorized.module().to_string(),
                db.to_vec(),
                CallerKind::Query,
                authorized.principal().cloned(),
                None,
                timestamp,
                fuel,
            )
        },
    );
    let refreshed = lane::rederive(
        &shared.store,
        read,
        &mut session,
        &catalog.env(db),
        graph,
        &touched,
        &|| shared.current(catalog) && shared.keys.settled(mark.keys),
    );
    let refreshed = match refreshed {
        Ok(lane::Rederived::Kept(graph)) => graph,
        Ok(lane::Rederived::Changed(staged)) => return Standing::Changed(Box::new(staged)),
        Ok(lane::Rederived::Unknown) | Err(_) => return Standing::Falls,
    };
    let certificate = Arc::new(refreshed.certificate());
    if !QueryCache::admits(&certificate) || certificate.validity() != Validity::Stable {
        return Standing::Falls;
    }
    if let Some(anchor) = shared.journal.anchor(db, mark.journal, revision) {
        let entry = Entry {
            value: cached.value.clone(),
            encoded: cached.encoded.clone(),
            certificate: certificate.clone(),
            code: code.clone(),
            evaluated_ms: timestamp,
            graph: Some(Arc::new(refreshed)),
        };
        shared.queries.insert(key.to_vec(), entry, anchor);
    }
    Standing::Stands(certificate)
}

/// One pinned snapshot plus the catalog committed for it, for callers
/// that decide access and evaluate against exactly the same state: a
/// watch hub reauthorizes every subscriber and refreshes their shared
/// query on one view, so each delivery is authorized at the revision it
/// shows.
pub struct ReadView {
    shared: Arc<Shared>,
    catalog: Arc<Catalog>,
    read: ReadTxn,
    /// The change journal's mark from just before the snapshot.
    mark: Marks,
}

impl ReadView {
    /// The database revision this view reads.
    pub fn revision(&self, db: &[u8]) -> HostResult<u64> {
        self.read
            .meta_u64(db, meta::REVISION)
            .map_err(Failure::from)
    }

    /// The declared read consistency of the query a call names, in the
    /// catalog this view captured (see [`EngineReader::call_consistency`]).
    pub fn call_consistency(
        &self,
        db: &[u8],
        module: &str,
        name: &str,
        options: &CallOptions,
    ) -> Option<ReadConsistency> {
        self.catalog
            .modules(db)
            .descriptors
            .get(module)?
            .call_consistency(name, options.external)
    }

    /// Decide who a call runs as on this view: alias resolution and the
    /// module's authorization hook, as every call does first.
    pub fn authorize(
        &self,
        db: &[u8],
        module: &str,
        name: &str,
        args: &Value,
        options: &CallOptions,
    ) -> HostResult<Authorized> {
        let timestamp = options.timestamp_ms.unwrap_or_else(now_ms);
        authorize(
            Some(&self.shared.recall(&self.catalog, db, self.mark)),
            &self.read,
            &self.catalog.env(db),
            db,
            module,
            name,
            args,
            &options.presented(timestamp),
        )
    }

    /// [`ReadView::authorize`], also returning the certificate of what
    /// the decision read (see [`crate::certificate`]): the module's
    /// authorization hook's reads, nothing for a module without one.
    pub fn authorize_certified(
        &self,
        db: &[u8],
        module: &str,
        name: &str,
        args: &Value,
        options: &CallOptions,
    ) -> HostResult<(Authorized, Certificate)> {
        let timestamp = options.timestamp_ms.unwrap_or_else(now_ms);
        authorize_certified(
            Some(&self.shared.recall(&self.catalog, db, self.mark)),
            &self.read,
            &self.catalog.env(db),
            db,
            module,
            name,
            args,
            &options.presented(timestamp),
        )
    }

    /// Evaluate a query as `authorized`, a decision made on this view,
    /// without deciding again. `options` supply fuel and time only.
    pub fn query_as(
        &self,
        db: &[u8],
        authorized: &Authorized,
        args: &Value,
        options: &CallOptions,
    ) -> HostResult<Value> {
        self.query_certified(db, authorized, args, options)
            .map(|(value, _)| value)
    }

    /// [`ReadView::query_as`], also returning the certificate of what
    /// the result observed. A cached result its certificate still
    /// vouches for at this view answers without evaluating.
    pub fn query_certified(
        &self,
        db: &[u8],
        authorized: &Authorized,
        args: &Value,
        options: &CallOptions,
    ) -> HostResult<(Value, Arc<Certificate>)> {
        let captured = Captured {
            shared: &self.shared,
            catalog: &self.catalog,
            read: &self.read,
            mark: self.mark,
            reuse: true,
        };
        let (answer, certificate) =
            run_authorized(&captured, db, authorized, args, options, Output::Value)?;
        Ok((answer.into_value()?, certificate))
    }

    /// Where an evaluation of `db` on this view sits in the change
    /// journal, for [`EngineReader::unchanged`]; `None` when the
    /// journal cannot follow it (a chain broke since the snapshot).
    pub fn anchor(&self, db: &[u8]) -> HostResult<Option<Anchor>> {
        let revision = self.revision(db)?;
        Ok(self.shared.journal.anchor(db, self.mark.journal, revision))
    }

    /// The key catalog's mark from before this view's snapshot, to
    /// compare with [`EngineReader::key_mark`]: what this view computed
    /// from a managed key may predate every change since.
    pub fn key_mark(&self) -> u64 {
        self.mark.keys
    }

    /// The version of `db`'s code this view captured.
    pub fn code_version(&self, db: &[u8]) -> CodeVersion {
        CodeVersion::of(self.catalog.modules.get(db))
    }
}

/// What a patch's stored receipt decides when the patch applies.
enum Recorded {
    /// The original's outcome, and where it committed when the receipt
    /// records it: a duplicate.
    Replay(HostResult<Value>, Option<Committed>),
    /// A refusal that replays nothing: released by acknowledgement, or
    /// another principal's key.
    Refused(Failure),
}

/// A stored receipt read at one instant.
enum StoredReceipt {
    /// Expired: unavailable history, the work executes afresh.
    Expired,
    /// Released by acknowledgement: it neither replays nor executes.
    Released,
    /// The recorded result to replay, with the receipt's expiry and
    /// where the result committed, when the receipt records it.
    Replay(HostResult<Value>, u64, Option<Committed>),
}

impl StoredReceipt {
    /// Read a receipt at `now`. Undecodable results are storage
    /// corruption, never a miss: every caller surfaces them the same way.
    fn of(receipt: &Receipt, now: u64) -> crate::error::Result<Self> {
        if receipt.expired(now) {
            return Ok(StoredReceipt::Expired);
        }
        let result = match receipt.result.as_ref() {
            None => return Ok(StoredReceipt::Released),
            Some(Outcome::Value(json)) => Ok(parse_value(json)?),
            Some(Outcome::Failure(data)) => {
                let mut failure = fail(data.code.clone(), data.message.clone());
                failure.details = data.details.as_deref().map(parse_value).transpose()?;
                Err(failure)
            }
        };
        Ok(StoredReceipt::Replay(
            result,
            receipt.expires_at,
            receipt.committed,
        ))
    }

    /// Decode stored receipt bytes and read them at `now`.
    fn decode(bytes: &[u8], now: u64) -> crate::error::Result<Self> {
        Self::of(&Receipt::decode(bytes)?, now)
    }
}

/// The single-node engine and writer lane: store, modules, history,
/// policy, dedup cache. Everything that commits runs through `&mut
/// Engine`; [`Engine::reader`] hands out the read side for queries
/// that should not wait for it.
pub struct Engine {
    shared: Arc<Shared>,
    /// The writer's catalog: always the latest version, published to
    /// readers after every change.
    catalog: Arc<Catalog>,
    /// Whether an [`Engine::exclusive`] section holds the published
    /// catalog's write lock; it publishes when it ends.
    exclusive: bool,
    replicated: HashSet<Vec<u8>>,
    /// The unscoped idempotency fast path, shared with speculative
    /// preparations ([`Engine::speculation`]), which only read it.
    dedup: Arc<RwLock<UnscopedDedup>>,
    /// While set, the writer paths ([`Engine::on_staged`]) read this
    /// uncommitted state instead of the committed one.
    staged: Option<StagedWrite>,
    /// Read dependencies captured for a durable participant prepare.
    recording: Option<Arc<std::sync::Mutex<crate::storage::overlay::ReadSet>>>,
    /// The instant mutations naming none prepare at, while set
    /// ([`Engine::at_clock`]); else the wall clock.
    clock: Option<u64>,
}

impl Engine {
    /// The idempotency cache, to change.
    fn dedup_mut(&self) -> std::sync::RwLockWriteGuard<'_, UnscopedDedup> {
        self.dedup.write().unwrap_or_else(PoisonError::into_inner)
    }

    /// Open an engine over a store with empty history, a permissive
    /// policy and the default idempotency cache.
    pub fn new(store: GroupStore) -> Self {
        let catalog = Arc::new(Catalog {
            modules: HashMap::new(),
            history: Arc::new(EmptyHistory),
            policy: Arc::new(PermissivePolicy),
        });
        Engine {
            shared: Arc::new(Shared {
                store,
                catalog: RwLock::new(catalog.clone()),
                journal: ChangeJournal::default(),
                keys: KeySequence::default(),
                queries: QueryCache::default(),
                decisions: DecisionMemo::default(),
            }),
            catalog,
            exclusive: false,
            replicated: HashSet::new(),
            dedup: Arc::new(RwLock::new(UnscopedDedup::new(DEFAULT_DEDUP_CAPACITY))),
            staged: None,
            recording: None,
            clock: None,
        }
    }

    /// Run `work` with every prepare path (mutations, transactions,
    /// timer fires, queue and timer plans) reading `staged` instead of
    /// the committed state: a writer prepares each write on what the
    /// writes queued before it left, before any of them commits. Reads
    /// served to clients never see it. Nested calls keep the outer state.
    pub fn on_staged<T>(&mut self, staged: &StagedWrite, work: impl FnOnce(&mut Engine) -> T) -> T {
        if self.staged.is_some() {
            return work(self);
        }
        self.staged = Some(staged.clone());
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| work(self)));
        self.staged = None;
        result.unwrap_or_else(|panic| std::panic::resume_unwind(panic))
    }

    /// Begin a managed-key catalog change on this member: before the
    /// transaction applying it commits (where [`Engine::drop_key_readers`]
    /// runs), held until this member's crypto host serves the new
    /// catalog, then dropped. Snapshot installs, which may move the
    /// catalog too, hold one across the install and its publish.
    pub fn begin_key_change(&self) -> KeyChange {
        self.shared.keys.begin();
        KeyChange {
            shared: self.shared.clone(),
        }
    }

    /// Inside the transaction applying a change to database `db`'s key
    /// catalog, drop every committed derived outcome of that database
    /// that resolved a managed key, with everything depending on it;
    /// pins stay as invalidated outcomes that re-evaluate on their next
    /// read. Catalog-wide within the database, as v1: a change names
    /// keys while values depend on aliases bound to them, devices
    /// receive changes without them, and changes are rare operator
    /// actions. Other databases resolve keys in catalogs of their own,
    /// which the change leaves alone.
    pub fn drop_key_readers(&self, write: &WriteTxn, db: &[u8]) -> crate::error::Result<()> {
        crate::derived::invalidate_keys(write, db)
    }

    /// The state a prepare path reads: the staged state inside
    /// [`Engine::on_staged`], else a fresh snapshot of committed state.
    pub fn snapshot(&self) -> crate::error::Result<ReadTxn> {
        let read = match &self.staged {
            Some(staged) => Ok(staged.read()),
            None => self.shared.store.begin_read(),
        }?;
        Ok(read.recording_into(self.recording.clone()))
    }

    /// [`Engine::snapshot`], with the marks taken before it when it is
    /// committed state, so decisions on it may be recalled and kept;
    /// none inside [`Engine::on_staged`], whose state no journal follows.
    fn snapshot_recalling(&self) -> HostResult<(ReadTxn, Option<Marks>)> {
        if self.staged.is_some() || self.recording.is_some() {
            return Ok((self.snapshot().map_err(Failure::from)?, None));
        }
        let marks = self.shared.marks();
        let read = self.shared.store.begin_read().map_err(Failure::from)?;
        Ok((read, Some(marks)))
    }

    /// Refuse work on a database that does not serve in the state
    /// prepares read: frozen, moving or retired in this group (see
    /// [`crate::placement`]). Applies check again, so this only spares
    /// evaluating what would be refused.
    ///
    /// Inside a writer's staging the committed state must serve too:
    /// modules load as a placement change applies, not as it is staged,
    /// so a database that only an in-flight batch activates (a move's
    /// destination at cutover) or thaws has no code here yet, and is
    /// answered as still moving, which callers retry.
    pub fn serving(&self, db: &[u8]) -> HostResult<()> {
        let read = self.snapshot().map_err(Failure::from)?;
        serving(&read, db)?;
        if self.staged.is_some() {
            let committed = self.shared.store.begin_read().map_err(Failure::from)?;
            serving(&committed, db)?;
        }
        Ok(())
    }

    /// The read side: queries through it never wait for the writer.
    pub fn reader(&self) -> EngineReader {
        EngineReader {
            shared: self.shared.clone(),
        }
    }

    /// One database's active modules.
    fn modules(&self, db: &[u8]) -> &DbModules {
        self.catalog.modules(db)
    }

    /// The environment invocations against one database run in.
    fn env(&self, db: &[u8]) -> Env<'_> {
        self.catalog.env(db)
    }

    /// What the database's modules declare about one queue.
    pub(crate) fn queue_config(&self, db: &[u8], queue: &str) -> crate::work_queue::QueueConfig {
        crate::work_queue::QueueConfig::of(self.env(db).descriptors.values(), queue)
    }

    /// A module's active descriptors in one database.
    fn active_def(&self, db: &[u8], module: &str) -> Option<&ModuleDef> {
        self.modules(db).descriptors.get(module)
    }

    /// Whether `module` is deployed in `db`.
    pub(crate) fn is_deployed(&self, db: &[u8], module: &str) -> bool {
        self.active_def(db, module).is_some()
    }

    /// Register a module's active descriptors and runtime in one
    /// database, publishing the new catalog. A code change that also
    /// commits store rows runs this inside [`Engine::exclusive`] with
    /// its commit.
    fn install_module(
        &mut self,
        db: &[u8],
        module: &str,
        defs: ModuleDef,
        runtime: Box<dyn Runtime>,
    ) {
        let catalog = Arc::make_mut(&mut self.catalog);
        let modules = Arc::make_mut(catalog.modules.entry(db.to_vec()).or_default());
        modules.descriptors.insert(module.to_string(), defs);
        modules
            .runtimes
            .insert(module.to_string(), Arc::from(runtime));
        self.publish();
    }

    /// Publish the writer's catalog to readers, unless an exclusive
    /// section will when it ends.
    fn publish(&self) {
        if !self.exclusive {
            *self
                .shared
                .catalog
                .write()
                .unwrap_or_else(PoisonError::into_inner) = self.catalog.clone();
        }
    }

    /// Run `work` with readers fenced out: no reader captures a
    /// snapshot or the catalog until it returns, and catalog changes
    /// inside publish together at the end. Store rewrites that change
    /// code (activations, snapshot installs) run inside one, so no
    /// reader pairs new rows with old code or old rows with new code.
    /// Readers already running keep their own snapshot and catalog;
    /// their cache writes see the change and drop. Nested sections run
    /// inline. A panic inside still publishes what was installed before
    /// it and lifts the fence, then resumes unwinding.
    pub fn exclusive<T>(&mut self, work: impl FnOnce(&mut Engine) -> T) -> T {
        if self.exclusive {
            return work(self);
        }
        let shared = self.shared.clone();
        let mut published = shared
            .catalog
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        self.exclusive = true;
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| work(self)));
        self.exclusive = false;
        *published = self.catalog.clone();
        drop(published);
        result.unwrap_or_else(|panic| std::panic::resume_unwind(panic))
    }

    /// Mark a database as replicated: local `invoke_mutation`,
    /// `invoke_transaction` and `invoke_transaction_multi` reject
    /// writes so every commit flows through consensus. Prepare paths
    /// stay open (they feed proposals) and reads are unaffected.
    pub fn set_replicated(&mut self, db: &[u8]) {
        self.replicated.insert(db.to_vec());
    }

    /// Drop everything held in memory for one database: module
    /// runtimes and descriptors, the replicated mark and cached
    /// idempotency results. Snapshot installs call this whenever they
    /// replace or remove a database's rows, then reload whatever the
    /// new rows deploy, so no module or cached outcome outlives the
    /// rows it came from.
    pub fn forget_database(&mut self, db: &[u8]) {
        if self.catalog.modules.contains_key(db) {
            Arc::make_mut(&mut self.catalog).modules.remove(db);
            self.publish();
        }
        self.replicated.remove(db);
        // A name too long to scope can never have been cached.
        if let Ok(scope) = dedup_key(db, &[]) {
            self.dedup_mut().forget_scope(&scope);
        }
    }

    /// Drop one database's module runtimes and descriptors, keeping its
    /// replicated mark: a device installing a snapshot of the database
    /// reloads what its rows deploy after, so a module removed since
    /// the device last synced does not outlive its rows.
    pub fn forget_modules(&mut self, db: &[u8]) {
        if self.catalog.modules.contains_key(db) {
            Arc::make_mut(&mut self.catalog).modules.remove(db);
            self.publish();
        }
    }

    /// Swap the history provider.
    pub fn with_history(mut self, history: Box<dyn HistoryProvider>) -> Self {
        Arc::make_mut(&mut self.catalog).history = Arc::from(history);
        self.publish();
        self
    }

    /// Swap the authorization policy.
    pub fn with_policy(mut self, policy: Box<dyn Policy>) -> Self {
        Arc::make_mut(&mut self.catalog).policy = Arc::from(policy);
        self.publish();
        self
    }

    /// Borrow the group store.
    pub fn store(&self) -> &GroupStore {
        &self.shared.store
    }

    /// Deploy a module into one database in one call: stage (validate
    /// the manifest and triggers, intern every name execution resolves,
    /// store the code artifact under the given identity), drain the
    /// backfill to completion and activate the runtime. Added indexes
    /// cover pre-existing rows; retired structures stay for lazy
    /// [`Engine::staging_gc`]. Trigger names must resolve to mutations:
    /// firing an undefined or non-mutation trigger would abort every
    /// commit touching its source, so deploy rejects them instead.
    pub fn deploy(
        &mut self,
        db: &[u8],
        module: &str,
        identity: [u8; HASH_LEN],
        wasm: &[u8],
        manifest: &Value,
        runtime: Box<dyn Runtime>,
    ) -> HostResult<()> {
        self.deploy_staged(db, module, identity, wasm, manifest)?;
        loop {
            let progress = self.staging_drain(db, module, STAGING_PAGE)?;
            if progress.done {
                break;
            }
        }
        self.activate(db, module, runtime)?;
        Ok(())
    }

    /// Stage a target bundle without switching traffic: validate the
    /// manifest, intern its new names, store the artifact and record the
    /// backfill plan. The active bundle keeps serving. Fails when a
    /// staging for the module is already in flight.
    pub fn deploy_staged(
        &mut self,
        db: &[u8],
        module: &str,
        identity: [u8; HASH_LEN],
        wasm: &[u8],
        manifest: &Value,
    ) -> HostResult<()> {
        // `$` starts an import slot, never a module.
        if module.is_empty() || module.contains('\0') || module.starts_with('$') {
            return Err(Error::Manifest("invalid module identity".to_string()).into());
        }
        let target = load_module(manifest).map_err(Failure::from)?;
        Self::check_triggers(&target)?;
        crate::contracts::check_links(&self.modules(db).descriptors, module, &target)?;
        let read = self.shared.store.begin_read().map_err(Failure::from)?;
        // A module whose data, or a queue it declares, is being purged
        // deploys once it is gone.
        if let Some(refusal) =
            crate::purge::activation_refusal(&read, db, module, manifest).map_err(Failure::from)?
        {
            return Err(refusal);
        }
        // Once removed, what it ran last: its data and indexes stayed.
        let previous = self.previous_def(&read, db, module)?;
        let mut retired = match read.meta(db, &staging_key(module)).map_err(Failure::from)? {
            None => Vec::new(),
            Some(bytes) => {
                let state = decode_state(&bytes).map_err(Failure::from)?;
                match state.phase {
                    StagingPhase::Gc => state.retired,
                    StagingPhase::Building | StagingPhase::Rebuilding | StagingPhase::Ready => {
                        return Err(
                            Error::Manifest("staging is already in progress".to_string()).into(),
                        );
                    }
                }
            }
        };
        drop(read);
        let write = self.shared.store.begin_write().map_err(Failure::from)?;
        let counter_before = write
            .meta_u64(db, meta::INTERN_COUNTER)
            .map_err(Failure::from)?;
        // In a fixed order, so staging the same bundle from the same
        // intern state allocates the same IDs on any member.
        for name in target_names(module, &target, None) {
            write.intern(db, &name).map_err(Failure::from)?;
        }
        let (plan, _) = diff_plan(
            previous.as_ref(),
            &target,
            &|name| write.intern(db, name.as_bytes()).map(Some),
            &|collection| crate::host::qualify_collection(module, collection),
            &|collection, index| crate::host::qualify_index(module, collection, index),
        )
        .map_err(Failure::from)?;
        purge_readded(&write, db, module, &target, &mut retired).map_err(Failure::from)?;
        let manifest_json = render(manifest).map_err(Failure::from)?;
        write
            .put_code(db, &identity, wasm, &manifest_json)
            .map_err(Failure::from)?;
        let counter_before = u32::try_from(counter_before)
            .map_err(|_| Error::corrupt("intern counter overflowed"))?;
        let counter_after = u32::try_from(
            write
                .meta_u64(db, meta::INTERN_COUNTER)
                .map_err(Failure::from)?,
        )
        .map_err(|_| Error::corrupt("intern counter overflowed"))?;
        let phase = if plan.is_empty() {
            StagingPhase::Ready
        } else {
            StagingPhase::Building
        };
        let state = StagingState {
            identity,
            phase,
            plan,
            plan_pos: 0,
            key_cursor: Vec::new(),
            retired,
            counter_before,
            // Interning allocates consecutively: everything past the
            // counter before is this staging's.
            local: (counter_before + 1..=counter_after).collect(),
            replicated: false,
            graph: Vec::new(),
            graph_pos: 0,
            graph_cursor: Vec::new(),
            rebuilt: 0,
        };
        let encoded = encode_state(&state).map_err(Failure::from)?;
        write
            .set_meta(db, &staging_key(module), &encoded)
            .map_err(Failure::from)?;
        write.commit().map_err(Failure::from)?;
        Ok(())
    }

    /// Build the backfill for at most `budget` records in one bounded
    /// transaction, resuming from the persisted cursor. Rows written
    /// concurrently are dual-written under both index sets, so the drain
    /// only needs rows the backfill scan reaches.
    pub fn staging_drain(
        &mut self,
        db: &[u8],
        module: &str,
        budget: usize,
    ) -> HostResult<StagingProgress> {
        let write = self.shared.store.begin_write().map_err(Failure::from)?;
        let bytes = write
            .meta(db, &staging_key(module))
            .map_err(Failure::from)?
            .ok_or_else(|| Error::Manifest("nothing is staged".to_string()))?;
        let mut state = decode_state(&bytes).map_err(Failure::from)?;
        if state.phase != StagingPhase::Building {
            return Ok(StagingProgress {
                done: true,
                scanned: 0,
            });
        }
        if write
            .get_code(db, &state.identity)
            .map_err(Failure::from)?
            .is_none()
        {
            return Err(Error::corrupt("staged code is missing").into());
        }
        let scanned = staged::drain_in(&write, db, &mut state, budget).map_err(Failure::from)?;
        put_staging(&write, db, module, Some(&state)).map_err(Failure::from)?;
        write.commit().map_err(Failure::from)?;
        Ok(StagingProgress {
            done: state.phase != StagingPhase::Building,
            scanned,
        })
    }

    /// Read a ready staging as an activation: the target identity plus
    /// every intern binding the staging allocated. The replication layer
    /// proposes this through consensus; appliers install it verbatim.
    pub fn prepare_activation(&self, db: &[u8], module: &str) -> HostResult<Activation> {
        let read = self.shared.store.begin_read().map_err(Failure::from)?;
        let bytes = read
            .meta(db, &staging_key(module))
            .map_err(Failure::from)?
            .ok_or_else(|| Error::Manifest("nothing is staged".to_string()))?;
        let state = decode_state(&bytes).map_err(Failure::from)?;
        if state.phase != StagingPhase::Ready {
            return Err(Error::Manifest("staging is not ready".to_string()).into());
        }
        let manifest = match read.get_code(db, &state.identity).map_err(Failure::from)? {
            Some((_, manifest)) => Some(parse_value(&manifest).map_err(Failure::from)?),
            None => None,
        };
        // A purge of the module, or of a queue it declares, may have
        // started since it staged.
        if let Some(refusal) = crate::purge::activation_refusal(
            &read,
            db,
            module,
            manifest.as_ref().unwrap_or(&Value::Null),
        )
        .map_err(Failure::from)?
        {
            return Err(refusal);
        }
        // Links checked at staging may have broken since: another module
        // deployed meanwhile. Check again before anything is proposed.
        if let Some(manifest) = &manifest {
            let target = load_module(manifest).map_err(Failure::from)?;
            crate::contracts::check_links(&self.modules(db).descriptors, module, &target)?;
        }
        let counter = read
            .meta_u64(db, meta::INTERN_COUNTER)
            .map_err(Failure::from)?
            .min(u32::MAX as u64);
        let mut bindings = Vec::new();
        let mut id = state.counter_before as u64 + 1;
        while id <= counter {
            let id32 = id as u32;
            if let Some(name) = read.intern_name(db, id32).map_err(Failure::from)? {
                bindings.push((name, id32));
            }
            id += 1;
        }
        Ok(Activation {
            identity: state.identity,
            bindings,
        })
    }

    /// Apply an activation: install its intern bindings, store the
    /// bundle, build any missing backfill and swap the bundles
    /// atomically. On the staging node the backfill is already ready, so
    /// the swap is immediate; followers build locally during the call.
    /// Removed structures retire to lazy collection. A replicated
    /// activation always applies: local stagings it supersedes are
    /// abandoned first (see [`Engine::yield_stagings`]).
    pub fn apply_activation(
        &mut self,
        db: &[u8],
        module: &str,
        wasm: &[u8],
        manifest: &Value,
        activation: &Activation,
        runtime: Box<dyn Runtime>,
    ) -> HostResult<()> {
        if module.is_empty() || module.contains('\0') {
            return Err(Error::Manifest("invalid module identity".to_string()).into());
        }
        let target = load_module(manifest).map_err(Failure::from)?;
        Self::check_triggers(&target)?;
        self.yield_stagings(db, module, activation)?;
        let read = self.shared.store.begin_read().map_err(Failure::from)?;
        let existing = read
            .meta(db, &staging_key(module))
            .map_err(Failure::from)?
            .map(|bytes| decode_state(&bytes).map_err(Failure::from))
            .transpose()?;
        let mut carried = match &existing {
            None => Vec::new(),
            Some(state) => match state.phase {
                StagingPhase::Gc => state.retired.clone(),
                StagingPhase::Building | StagingPhase::Rebuilding | StagingPhase::Ready
                    if state.identity == activation.identity =>
                {
                    state.retired.clone()
                }
                // Yielding abandoned any other target.
                StagingPhase::Building | StagingPhase::Rebuilding | StagingPhase::Ready => {
                    return Err(Error::corrupt("a superseded staging survived").into());
                }
            },
        };
        let needs_row = !matches!(
            existing,
            Some(ref state) if state.identity == activation.identity
        );
        // Once removed, what it ran last: its data and indexes stayed.
        let previous = self.previous_def(&read, db, module)?;
        drop(read);
        if needs_row {
            let write = self.shared.store.begin_write().map_err(Failure::from)?;
            for (name, id) in &activation.bindings {
                write.bind_intern(db, name, *id).map_err(Failure::from)?;
            }
            let manifest_json = render(manifest).map_err(Failure::from)?;
            write
                .put_code(db, &activation.identity, wasm, &manifest_json)
                .map_err(Failure::from)?;
            let (plan, _) = diff_plan(
                previous.as_ref(),
                &target,
                &|name| write.intern(db, name.as_bytes()).map(Some),
                &|collection| crate::host::qualify_collection(module, collection),
                &|collection, index| crate::host::qualify_index(module, collection, index),
            )
            .map_err(Failure::from)?;
            purge_readded(&write, db, module, &target, &mut carried).map_err(Failure::from)?;
            let counter = write
                .meta_u64(db, meta::INTERN_COUNTER)
                .map_err(Failure::from)?;
            let counter_before =
                u32::try_from(counter).map_err(|_| Error::corrupt("intern counter overflowed"))?;
            let phase = if plan.is_empty() {
                StagingPhase::Ready
            } else {
                StagingPhase::Building
            };
            let state = StagingState {
                identity: activation.identity,
                phase,
                plan,
                plan_pos: 0,
                key_cursor: Vec::new(),
                retired: carried.clone(),
                counter_before,
                // Every name came with the bindings.
                local: Vec::new(),
                replicated: false,
                graph: Vec::new(),
                graph_pos: 0,
                graph_cursor: Vec::new(),
                rebuilt: 0,
            };
            let encoded = encode_state(&state).map_err(Failure::from)?;
            write
                .set_meta(db, &staging_key(module), &encoded)
                .map_err(Failure::from)?;
            write.commit().map_err(Failure::from)?;
        }
        loop {
            let progress = self.staging_drain(db, module, STAGING_PAGE)?;
            if progress.done {
                break;
            }
        }
        let write = self.shared.store.begin_write().map_err(Failure::from)?;
        for (name, id) in &activation.bindings {
            write.bind_intern(db, name, *id).map_err(Failure::from)?;
        }
        clear_local(&write, db, &activation.bindings).map_err(Failure::from)?;
        let active = previous.as_ref();
        let (_, removals) = diff_plan(
            active,
            &target,
            &|name| write.intern(db, name.as_bytes()).map(Some),
            &|collection| crate::host::qualify_collection(module, collection),
            &|collection, index| crate::host::qualify_index(module, collection, index),
        )
        .map_err(Failure::from)?;
        // The module's own outcomes go stale through a new code era,
        // even for code it ran before: what that code cached read
        // structures removed, collected, purged or backfilled since.
        // Every member mints alike, applying activations in log order;
        // the counter moves with the database's rows, snapshots included.
        let minted = write
            .meta_u64(db, meta::CODE_ERAS)
            .map_err(Failure::from)?
            .checked_add(1)
            .ok_or_else(|| Error::corrupt("code eras overflowed"))?;
        write
            .set_meta_u64(db, meta::CODE_ERAS, minted)
            .map_err(Failure::from)?;
        let era = crate::staging::code_era(minted);
        write
            .set_meta(db, &crate::staging::code_era_key(module), &era)
            .map_err(Failure::from)?;
        // Outcomes of other modules keep their era, so those that
        // reached this module's surface (its collections, indexes and
        // derived values) drop here, transitively.
        crate::derived::invalidate_module_readers(&write, db, module).map_err(Failure::from)?;
        // A versioned aggregate keeps its lineage, and so its cached
        // values, while its version, source, index and index fields stay
        // as the active code declared them; otherwise a new lineage
        // begins here, never one an earlier value carries (the same code
        // deployed again after a gap included). Every member decides
        // alike: the activation applies in log order. Sorted, so the
        // lineages it mints come out the same everywhere.
        let mut versioned: Vec<_> = target.aggregates.iter().collect();
        versioned.sort_by(|left, right| left.0.cmp(right.0));
        for (name, aggregate) in versioned {
            let key = crate::staging::aggregate_lineage_key(module, name);
            let Some(version) = aggregate.version.as_deref() else {
                write.delete_meta(db, &key).map_err(Failure::from)?;
                continue;
            };
            let kept = active.is_some_and(|active| {
                active.aggregates.get(name).is_some_and(|known| {
                    known.version.as_deref() == Some(version)
                        && known.index == aggregate.index
                        && known.collection == aggregate.collection
                        && aggregate_fields(active, known) == aggregate_fields(&target, aggregate)
                })
            }) && write.meta(db, &key).map_err(Failure::from)?.is_some();
            if !kept {
                let begun = write
                    .meta_u64(db, meta::AGGREGATE_LINEAGES)
                    .map_err(Failure::from)?
                    .checked_add(1)
                    .ok_or_else(|| Error::corrupt("aggregate lineages overflowed"))?;
                write
                    .set_meta_u64(db, meta::AGGREGATE_LINEAGES, begun)
                    .map_err(Failure::from)?;
                write
                    .set_meta(db, &key, &crate::staging::aggregate_lineage(begun))
                    .map_err(Failure::from)?;
            }
        }
        for name in active
            .map(|defs| defs.aggregates.keys())
            .into_iter()
            .flatten()
        {
            if !target.aggregates.contains_key(name) {
                write
                    .delete_meta(db, &crate::staging::aggregate_lineage_key(module, name))
                    .map_err(Failure::from)?;
            }
        }
        // A replicated staging rebuilt the module's graph under the
        // target: its staged outcomes take the active ones' place now,
        // after the invalidation above, so they serve from the cutover.
        let staged = write
            .meta(db, &staging_key(module))
            .map_err(Failure::from)?
            .map(|bytes| decode_state(&bytes))
            .transpose()
            .map_err(Failure::from)?
            .filter(|state| {
                state.phase != StagingPhase::Gc && state.identity == activation.identity
            });
        let replicated = staged.as_ref().is_some_and(|state| state.replicated);
        if let Some(state) = &staged {
            staged::move_staged_graph(&write, db, &target, state, era).map_err(Failure::from)?;
        }
        let mut retired = carried;
        for removal in removals {
            if !retired
                .iter()
                .any(|known: &Retired| known.kind == removal.kind && known.id == removal.id)
            {
                retired.push(removal);
            }
        }
        write
            .set_meta(db, &code_identity_key(module), &activation.identity)
            .map_err(Failure::from)?;
        // Deployed again: what its removal kept serves from here.
        write
            .delete_meta(db, &crate::purge::removed_key(module))
            .map_err(Failure::from)?;
        if retired.is_empty() {
            write
                .delete_meta(db, &staging_key(module))
                .map_err(Failure::from)?;
        } else {
            let state = StagingState::collecting(activation.identity, retired, replicated);
            put_staging(&write, db, module, Some(&state)).map_err(Failure::from)?;
        }
        // The new identity row and the new runtime reach readers
        // together.
        self.exclusive(|engine| {
            write.commit().map_err(Failure::from)?;
            engine.install_module(db, module, target, runtime);
            Ok(())
        })
    }

    /// Abandon every staging a replicated activation supersedes: one of
    /// the activation's module with another target (alike on every
    /// member for a replicated staging), and any local one whose
    /// locally allocated intern IDs collide with the activation's
    /// bindings. A local staging lives on the member that staged, so
    /// either means this member staged as leader and lost to a staging
    /// elsewhere; no other member knows its names, and the activation's
    /// bindings are authoritative. A staging of the same target
    /// survives unless its own IDs collide, and is then rebuilt from
    /// the bindings. A collision with an ID no local staging owns is
    /// corruption: binding fails as before.
    fn yield_stagings(
        &mut self,
        db: &[u8],
        module: &str,
        activation: &Activation,
    ) -> HostResult<()> {
        let read = self.shared.store.begin_read().map_err(Failure::from)?;
        let open = open_stagings(
            read.scan_meta_prefix(db, STAGING_PREFIX)
                .map_err(Failure::from)?,
        )
        .map_err(Failure::from)?;
        let mut superseded: HashSet<&str> = open
            .iter()
            .filter(|(staged, state)| staged == module && state.identity != activation.identity)
            .map(|(staged, _)| staged.as_str())
            .collect();
        for (name, id) in &activation.bindings {
            let mut colliding = Vec::new();
            if read
                .intern_name(db, *id)
                .map_err(Failure::from)?
                .is_some_and(|bound| bound != *name)
            {
                colliding.push(*id);
            }
            if let Some(known) = read.intern_id(db, name).map_err(Failure::from)?
                && known != *id
            {
                colliding.push(known);
            }
            for (staged, state) in &open {
                if colliding.iter().any(|id| state.local.contains(id)) {
                    superseded.insert(staged);
                }
            }
        }
        drop(read);
        if superseded.is_empty() {
            return Ok(());
        }
        let write = self.shared.store.begin_write().map_err(Failure::from)?;
        for (staged, state) in &open {
            if superseded.contains(staged.as_str()) {
                let rest = abandon_staging(&write, db, state.clone()).map_err(Failure::from)?;
                put_staging(&write, db, staged, rest.as_ref()).map_err(Failure::from)?;
            }
        }
        write.commit().map_err(Failure::from)?;
        Ok(())
    }

    /// Activate a locally staged bundle: prepare the activation from the
    /// ready staging row and apply it with the fresh runtime.
    pub fn activate(
        &mut self,
        db: &[u8],
        module: &str,
        runtime: Box<dyn Runtime>,
    ) -> HostResult<Activation> {
        let activation = self.prepare_activation(db, module)?;
        let read = self.shared.store.begin_read().map_err(Failure::from)?;
        let (wasm, manifest_json) = read
            .get_code(db, &activation.identity)
            .map_err(Failure::from)?
            .ok_or_else(|| Error::corrupt("staged code is missing"))?;
        drop(read);
        let manifest = parse_value(&manifest_json).map_err(Failure::from)?;
        self.apply_activation(db, module, &wasm, &manifest, &activation, runtime)?;
        Ok(activation)
    }

    /// Cancel an in-flight staging: the target bundle never activates.
    /// The partial backfill under names only this member interned is
    /// purged and those names released, so a later activation staged
    /// elsewhere can bind the IDs; the rest retires to lazy collection.
    pub fn cancel_staging(&mut self, db: &[u8], module: &str) -> HostResult<()> {
        if self.cancel_open_staging(db, module)? {
            Ok(())
        } else {
            Err(Error::Manifest("nothing is staged".to_string()).into())
        }
    }

    /// Cancel the module's staging if one is open (not yet activated), as
    /// [`Engine::cancel_staging`] does, and report whether one was. A
    /// deployment clears a staging its member left behind (a crash, or
    /// a lost leadership) through here before staging anew.
    pub fn cancel_open_staging(&mut self, db: &[u8], module: &str) -> HostResult<bool> {
        let read = self.shared.store.begin_read().map_err(Failure::from)?;
        let Some(bytes) = read.meta(db, &staging_key(module)).map_err(Failure::from)? else {
            return Ok(false);
        };
        let state = decode_state(&bytes).map_err(Failure::from)?;
        drop(read);
        if state.phase == StagingPhase::Gc {
            return Ok(false);
        }
        let write = self.shared.store.begin_write().map_err(Failure::from)?;
        let rest = abandon_staging(&write, db, state).map_err(Failure::from)?;
        put_staging(&write, db, module, rest.as_ref()).map_err(Failure::from)?;
        write.commit().map_err(Failure::from)?;
        Ok(true)
    }

    /// Collect up to `budget` retired keys in one bounded transaction:
    /// removed index entries, removed collection records and cancelled
    /// backfill. The staging row disappears once nothing remains.
    pub fn staging_gc(&mut self, db: &[u8], module: &str, budget: usize) -> HostResult<GcProgress> {
        let write = self.shared.store.begin_write().map_err(Failure::from)?;
        let progress = staged::gc_in(&write, db, module, budget).map_err(Failure::from)?;
        write.commit().map_err(Failure::from)?;
        Ok(progress)
    }

    /// Inspect a module's staging row, if any.
    pub fn staging_status(&self, db: &[u8], module: &str) -> HostResult<Option<StagingStatus>> {
        let read = self.shared.store.begin_read().map_err(Failure::from)?;
        let bytes = match read.meta(db, &staging_key(module)).map_err(Failure::from)? {
            None => return Ok(None),
            Some(bytes) => bytes,
        };
        let state = decode_state(&bytes).map_err(Failure::from)?;
        Ok(Some(StagingStatus::of(&state)))
    }

    /// Reload a deployed module after a restart: read its active code
    /// identity, parse the stored manifest and register the descriptors
    /// with the fresh runtime. Staging rows need no recovery call: the
    /// drain, activation and collector all resume from durable state.
    pub fn reload_module(
        &mut self,
        db: &[u8],
        module: &str,
        runtime: Box<dyn Runtime>,
    ) -> HostResult<()> {
        let read = self.shared.store.begin_read().map_err(Failure::from)?;
        let identity = read
            .meta(db, &code_identity_key(module))
            .map_err(Failure::from)?
            .ok_or_else(|| Error::Manifest("module is not deployed".to_string()))?;
        let identity: [u8; HASH_LEN] = identity
            .try_into()
            .map_err(|_| Error::corrupt("code identity has the wrong length"))?;
        let (_, manifest_json) = read
            .get_code(db, &identity)
            .map_err(Failure::from)?
            .ok_or_else(|| Error::corrupt("deployed code is missing"))?;
        drop(read);
        let manifest = parse_value(&manifest_json).map_err(Failure::from)?;
        let defs = load_module(&manifest).map_err(Failure::from)?;
        Self::check_triggers(&defs)?;
        self.install_module(db, module, defs, runtime);
        Ok(())
    }

    /// Reject manifests whose triggers do not name a mutation.
    fn check_triggers(defs: &ModuleDef) -> HostResult<()> {
        for trigger in &defs.triggers {
            match defs.def_kinds.get(&trigger.name) {
                Some(DefKind::Mutation) => {}
                _ => {
                    return Err(Error::Manifest(format!(
                        "trigger `{}` is not a mutation",
                        trigger.name
                    ))
                    .into());
                }
            }
        }
        // Retention hooks run the same way, as mutations of their names.
        for hook in &defs.retention_hooks {
            if defs.def_kinds.get(&hook.name) != Some(&DefKind::Mutation) {
                return Err(Error::Manifest(format!(
                    "retention hook `{}` is not a mutation",
                    hook.name
                ))
                .into());
            }
        }
        Ok(())
    }

    /// Invoke a query on the writer: no admission, no receipt, caches
    /// commit best-effort. [`EngineReader::invoke_query`] runs the same
    /// query without the writer.
    pub fn invoke_query(
        &self,
        db: &[u8],
        module: &str,
        name: &str,
        args: &Value,
        options: &CallOptions,
    ) -> HostResult<Value> {
        let read = self.shared.store.begin_read().map_err(Failure::from)?;
        let captured = Captured {
            shared: &self.shared,
            catalog: &self.catalog,
            read: &read,
            mark: self.shared.marks(),
            reuse: false,
        };
        let call = QueryCall {
            db,
            module,
            name,
            args,
            options,
        };
        let (answer, _) = run_query(&captured, call, Output::Value)?;
        answer.into_value()
    }

    /// Invoke a mutation: unscoped dedup, scoped admission, then the lane.
    /// At most one of a retry identity or an idempotency key applies.
    pub fn invoke_mutation(
        &mut self,
        db: &[u8],
        module: &str,
        name: &str,
        args: &Value,
        options: &MutationOptions,
    ) -> HostResult<Value> {
        if self.replicated.contains(db) {
            return Err(fail(
                "replicated_db",
                "mutations on a replicated database go through consensus",
            ));
        }
        // Local direct commits do not pass the replicated patch fence.
        // Keep their conservative admission fence while a participant waits.
        if let Some(holder) = txn::read_lock_holder(&self.snapshot().map_err(Failure::from)?, db)
            .map_err(Failure::from)?
        {
            return Err(txn::locked(db, &holder));
        }
        let begun = self.begin_mutation(db, module, name, args, options, false)?;
        let (read, mut session, receipt, definition) = match begun {
            BeginOutcome::Done(done) => return done,
            BeginOutcome::Replayed(done, _) => return done,
            BeginOutcome::Proceed {
                read,
                session,
                receipt,
                definition,
            } => (read, session, receipt, definition),
        };
        let commit = CommitSpec {
            receipt,
            expected_revision: options.expected_revision,
        };
        if plans(&self.env(db), module, &definition) {
            session.budget.within = transaction_deadline(
                session
                    .budget
                    .declared
                    .unwrap_or(crate::runtime::wasm::DEFAULT_TIMEOUT),
            );
            let calls = self.plan_transaction(db, module, &definition, args, session.budget)?;
            let outcome = lane::run_transaction(
                &self.shared.store,
                &read,
                &mut session,
                &self.env(db),
                module,
                &calls,
                commit,
            );
            drop(read);
            self.cache_durable(db, options);
            return outcome.map(|outcome| outcome.value);
        }
        let target = Target {
            kind: CallKind::Mutation,
            module,
            name: &definition,
            args,
        };
        let outcome = lane::run(
            &self.shared.store,
            &read,
            &mut session,
            &self.env(db),
            target,
            commit,
        );
        drop(read);
        self.cache_durable(db, options);
        outcome.map(|outcome| outcome.value)
    }

    /// Evaluate a mutation without committing it: the same admission as
    /// [`Engine::invoke_mutation`], then patch extraction. The caller
    /// proposes [`PrepareOutcome::Patch`] through consensus and returns
    /// [`PrepareOutcome::Done`] as-is.
    pub fn prepare_mutation(
        &mut self,
        db: &[u8],
        module: &str,
        name: &str,
        args: &Value,
        options: &MutationOptions,
    ) -> HostResult<PrepareOutcome> {
        let dedup = self.dedup.clone();
        let dedup = dedup.read().unwrap_or_else(PoisonError::into_inner);
        let on = self.prepare_on(&dedup)?;
        prepare_mutation_on(on, db, module, name, args, options).0
    }

    /// Prepare one timer fire without committing: load the timer,
    /// run its handler with the timer deletion staged in the same
    /// session, and revision-guard the patch so concurrent sweeps
    /// cannot double-fire. Handler failures roll back, then prepare
    /// a retry update (or death) instead. A timer of a removed module
    /// is held for it instead, attempts untouched (see
    /// [`crate::timer`]). Fatal poison returns without a patch; the
    /// timer stays for a later sweep.
    pub fn prepare_timer_fire(&mut self, db: &[u8], id: &str) -> HostResult<FirePrepare> {
        crate::timer::validate_id(id)?;
        self.serving(db)?;
        let now = now_ms();
        let Some((collection, timer, revision)) = self.fire_candidate(db, id, now)? else {
            return Ok(FirePrepare::Skip);
        };
        // A removed module's timers wait for it to be deployed again,
        // or purged.
        if !self.is_deployed(db, &timer.module) && self.removed(db, &timer.module)? {
            if timer.state == crate::timer::TimerState::Held {
                return Ok(FirePrepare::Skip);
            }
            let mut held = timer.clone();
            held.state = crate::timer::TimerState::Held;
            return Ok(FirePrepare::Hold(
                self.timer_patch(db, collection, &timer, &held, revision)?,
            ));
        }
        // Firing uses currently deployed handler code: a missing or
        // non-mutation handler fails the fire into the retry budget
        // instead of poisoning the sweep.
        let runnable = self
            .active_def(db, &timer.module)
            .and_then(|defs| defs.def_kinds.get(&timer.handler))
            .is_some_and(|kind| *kind == DefKind::Mutation)
            && self.modules(db).runtimes.contains_key(&timer.module);
        if !runnable {
            return Ok(FirePrepare::Retry(
                self.retry_patch(db, collection, &timer, revision, now)?,
            ));
        }
        let options = MutationOptions {
            call: CallOptions {
                timestamp_ms: Some(now),
                ..CallOptions::default()
            },
            retry: None,
            idempotency_key: None,
            expected_revision: None,
        };
        let begun = self.begin_mutation(
            db,
            &timer.module,
            &timer.handler,
            &timer.args,
            &options,
            true,
        )?;
        let (read, mut session, receipt) = match begun {
            BeginOutcome::Done(done) | BeginOutcome::Replayed(done, _) => {
                // No identities or keys on fires: Done is unreachable.
                let _ = done;
                return Ok(FirePrepare::Skip);
            }
            BeginOutcome::Proceed {
                read,
                session,
                receipt,
                ..
            } => (read, session, receipt),
        };
        for (key, value) in crate::timer::removal(&timer) {
            session.staged.stage_record(collection, key, value);
        }
        let target = Target {
            kind: CallKind::Mutation,
            module: &timer.module,
            name: &timer.handler,
            args: &timer.args,
        };
        let commit = CommitSpec {
            receipt,
            expected_revision: Some(revision),
        };
        let prepared = lane::prepare(&read, &mut session, &self.env(db), target, commit)?;
        match &prepared.outcome {
            Ok(_) => Ok(FirePrepare::Patch(prepared)),
            Err(_) => Ok(FirePrepare::Retry(
                self.retry_patch(db, collection, &timer, revision, now)?,
            )),
        }
    }

    /// Load a fireable timer plus its collection and the current
    /// revision: pending and due, or held (it was due when held).
    /// Missing, dead and future timers read as `None`. The ID row
    /// locates the timer directly, however many timers sort before it.
    fn fire_candidate(
        &self,
        db: &[u8],
        id: &str,
        now_ms: u64,
    ) -> HostResult<Option<(u32, crate::timer::Timer, u64)>> {
        let read = self.snapshot().map_err(Failure::from)?;
        let revision = read.meta_u64(db, meta::REVISION).map_err(Failure::from)?;
        let Some(collection) = read
            .intern_id(db, crate::host::TIMER_COLLECTION)
            .map_err(Failure::from)?
        else {
            return Ok(None);
        };
        let found = crate::timer::find(|key| read.get_record(db, collection, key), id)?;
        Ok(found
            .filter(|timer| match timer.state {
                crate::timer::TimerState::Pending => timer.due_ms <= now_ms,
                crate::timer::TimerState::Held => true,
                crate::timer::TimerState::Dead => false,
            })
            .map(|timer| (collection, timer, revision)))
    }

    /// Whether `module` was removed from `db`: its removal kept its
    /// data, or a purge of it is in flight.
    fn removed(&self, db: &[u8], module: &str) -> HostResult<bool> {
        let read = self.snapshot().map_err(Failure::from)?;
        Ok(read
            .meta(db, &crate::purge::removed_key(module))
            .map_err(Failure::from)?
            .is_some()
            || crate::purge::purges(&read, db)
                .map_err(Failure::from)?
                .contains_key(module))
    }

    /// Build the retry patch for a failed fire: attempts advance and
    /// the due time backs off, or the timer dies past its budget. No
    /// guest code runs, so no module versions pin. Revision-guarded
    /// like the fire patch.
    fn retry_patch(
        &self,
        db: &[u8],
        collection: u32,
        timer: &crate::timer::Timer,
        revision: u64,
        now_ms: u64,
    ) -> HostResult<lane::PreparedPatch> {
        let mut next = timer.clone();
        next.attempts = timer.attempts.saturating_add(1);
        if next.attempts >= timer.max_attempts {
            next.state = crate::timer::TimerState::Dead;
        } else {
            next.state = crate::timer::TimerState::Pending;
            next.due_ms = now_ms.saturating_add(crate::timer::retry_delay_ms(timer.attempts));
        }
        self.timer_patch(db, collection, timer, &next, revision)
    }

    /// Build a patch moving `timer` to `next`, revision-guarded like
    /// the fire patch. No guest code runs, so no module versions pin.
    fn timer_patch(
        &self,
        db: &[u8],
        collection: u32,
        timer: &crate::timer::Timer,
        next: &crate::timer::Timer,
        revision: u64,
    ) -> HostResult<lane::PreparedPatch> {
        let mut rows = crate::timer::removal(timer);
        rows.extend(crate::timer::timer_rows(next).map_err(Failure::from)?);
        let records = rows
            .into_iter()
            .map(|(key, value)| RecordWrite {
                collection,
                key,
                value,
            })
            .collect();
        let patch = Patch {
            failed: false,
            db: db.to_vec(),
            versions: Vec::new(),
            records,
            indexes: Vec::new(),
            derived: Vec::new(),
            drops: Vec::new(),
            receipts: Vec::new(),
            result: render(&Value::Null).map_err(Failure::from)?,
            expected_revision: Some(revision),
            base_revision: revision,
        };
        let bytes = encode_patch(&patch).map_err(Failure::from)?;
        Ok(lane::PreparedPatch {
            patch: bytes,
            outcome: Ok(Value::Null),
        })
    }

    /// Check a patch against local state without writing: decode it,
    /// require the position to advance and decide deterministic
    /// rejection — `stale_revision` when a deployment changed the
    /// database's code since prepare, or when the expected revision
    /// moved; `queue_purging` when it writes a queue a purge is
    /// deleting. Every applier reaches the same verdict at the same log
    /// position. The result commits later through
    /// [`Engine::commit_patch`], possibly in a transaction shared with
    /// other records.
    pub fn check_patch(&self, patch_bytes: &[u8], position: u64) -> HostResult<CheckedPatch> {
        let patch = decode_patch(patch_bytes).map_err(Failure::from)?;
        let read = self.shared.store.begin_read().map_err(Failure::from)?;
        Self::check_entry(&read, patch, position, true)
    }

    /// Check one patch entry against a snapshot: positions must
    /// advance (anything else is a replication bug and fails loudly),
    /// except that a batch's later items on a database share its first
    /// item's position (`advance` false); code and revision drift, and
    /// writes to a queue under purge, reject deterministically.
    pub(crate) fn check_entry(
        read: &ReadTxn,
        patch: Patch,
        position: u64,
        advance: bool,
    ) -> HostResult<CheckedPatch> {
        let revision = read
            .meta_u64(&patch.db, meta::REVISION)
            .map_err(Failure::from)?;
        let applied = read
            .meta_u64(&patch.db, meta::APPLIED_LOG)
            .map_err(Failure::from)?;
        if position < applied || (advance && position == applied) {
            return Err(Error::corrupt(format!(
                "patch position {position} does not advance {applied}"
            ))
            .into());
        }
        // Patches built without guest code pin no versions. Otherwise
        // every module deployed at prepare must still be active under
        // the same identity, and none may have deployed since.
        let redeployed = !patch.versions.is_empty()
            && deployed_versions(read, &patch.db).map_err(Failure::from)? != patch.versions;
        // A database that stopped serving here says so first: its rows
        // may be gone, which makes every other verdict moot.
        let unserved = match placement::read_refusal(read, &patch.db).map_err(Failure::from)? {
            Some(refusal) => Some(refusal),
            None => txn::read_patch_refusal(read, &patch).map_err(Failure::from)?,
        };
        let rejected = if unserved.is_some() {
            unserved
        } else if redeployed {
            Some(fail(
                "stale_revision",
                "the database's deployed code changed since the patch was prepared",
            ))
        } else {
            match patch.expected_revision {
                Some(expected) if revision != expected => Some(fail(
                    "stale_revision",
                    format!("expected revision {expected} but the database is at {revision}"),
                )),
                // A queue whose purge began after the patch was planned
                // takes none of its writes.
                _ => crate::purge::writes_refusal(read, &patch.db, &patch.records)
                    .map_err(Failure::from)?,
            }
        };
        Ok(CheckedPatch {
            patch,
            revision,
            rejected,
        })
    }

    /// If any receipt identity in the patch already has a live stored
    /// receipt, the patch is a duplicate submission: return the
    /// recorded outcome so the applier skips the journal and replays
    /// instead of double-applying. First receipt wins across members,
    /// races and restarts. Expired rows miss (the patch proceeds and
    /// replaces them); released rows reject deterministically; corrupt
    /// rows fail as storage corruption. Expiry reads against the
    /// entry's proposal stamp so every applier reaches the same verdict.
    fn duplicate_outcome(
        write: &WriteTxn,
        db: &[u8],
        receipts: &[(Vec<u8>, Vec<u8>)],
        proposed_ms: u64,
    ) -> HostResult<Option<Recorded>> {
        for (identity, proposed) in receipts {
            let Some(bytes) = write.get_receipt(db, identity).map_err(Failure::from)? else {
                continue;
            };
            match StoredReceipt::decode(&bytes, proposed_ms).map_err(Failure::from)? {
                StoredReceipt::Expired => continue,
                StoredReceipt::Released => {
                    return Ok(Some(Recorded::Refused(reject(RejectReason::Released))));
                }
                StoredReceipt::Replay(result, _, committed) => {
                    // Requests that both missed at prepare under one key
                    // but differ never answer each other: two principals
                    // racing on one idempotency key, or one scoped
                    // identity reused for other content (a conflict, as
                    // admission would have said).
                    let stored = Receipt::decode(&bytes).map_err(Failure::from)?;
                    let proposed = Receipt::decode(proposed).map_err(Failure::from)?;
                    if stored.fingerprint != proposed.fingerprint {
                        let refusal = if identity.starts_with(UNSCOPED_PREFIX) {
                            foreign_receipt()
                        } else {
                            reject(RejectReason::Conflict)
                        };
                        return Ok(Some(Recorded::Refused(refusal)));
                    }
                    return Ok(Some(Recorded::Replay(result, committed)));
                }
            }
        }
        Ok(None)
    }

    /// Commit a checked patch into an open write transaction without
    /// committing: install the journal, bump the revision on success
    /// and advance the applied log. The caller commits, so group level
    /// records can share the atomic write. Duplicate submissions skip
    /// the journal and replay the recorded outcome, with the revision and
    /// log position it committed at when its receipt records them.
    /// Receipts record both as they commit ([`Committed`]): the revision
    /// this patch leaves and the log index of its entry (the position,
    /// unless the transaction records a restored history's index), so
    /// every applier records the same receipt; a writer's scratch
    /// staging, which never commits, records none. Derived outcomes
    /// install only while the database still sits at the patch's base
    /// revision; otherwise they were computed over a snapshot other
    /// commits changed, and drop instead.
    pub fn commit_patch(
        &self,
        write: &WriteTxn,
        checked: &CheckedPatch,
        position: u64,
        proposed_ms: u64,
    ) -> HostResult<ApplyOutcome> {
        let patch = &checked.patch;
        // Placement decides before anything else, and writes nothing: a
        // database frozen, moved or retired in this group's log since the
        // patch was prepared takes no write here (see `crate::placement`).
        if let Some(failure) = placement::refusal(write, &patch.db).map_err(Failure::from)? {
            return Ok(ApplyOutcome {
                result: Err(failure),
                revision: checked.revision,
                replayed_position: None,
                duplicate: false,
            });
        }
        // A prepared participant's read and write footprint must remain
        // unchanged until its decision (see `crate::txn`).
        if let Some(refusal) = txn::write_patch_refusal(write, patch).map_err(Failure::from)? {
            return Ok(ApplyOutcome {
                result: Err(refusal),
                revision: checked.revision,
                replayed_position: None,
                duplicate: false,
            });
        }
        // Retention decides first, here as at preparation: a retired,
        // foreign or acknowledged identity neither replays nor runs,
        // whatever was ordered between its preparation and this entry.
        if let Some(failure) = Self::refused(write, &patch.db, &patch.receipts)? {
            write
                .set_meta_u64(&patch.db, meta::APPLIED_LOG, position)
                .map_err(Failure::from)?;
            return Ok(ApplyOutcome {
                result: Err(failure),
                revision: checked.revision,
                replayed_position: None,
                duplicate: false,
            });
        }
        // Duplicates replay before any other verdict: the key already
        // committed, so even a stale duplicate answers its recorded
        // outcome instead of an error demanding a pointless retry.
        if let Some(recorded) =
            Self::duplicate_outcome(write, &patch.db, &patch.receipts, proposed_ms)?
        {
            write
                .set_meta_u64(&patch.db, meta::APPLIED_LOG, position)
                .map_err(Failure::from)?;
            return Ok(match recorded {
                Recorded::Replay(result, committed) => ApplyOutcome {
                    result,
                    revision: committed.map_or(checked.revision, |committed| committed.revision),
                    replayed_position: committed.map(|committed| committed.position),
                    duplicate: true,
                },
                Recorded::Refused(failure) => ApplyOutcome {
                    result: Err(failure),
                    revision: checked.revision,
                    replayed_position: None,
                    duplicate: false,
                },
            });
        }
        if let Some(failure) = &checked.rejected {
            write
                .set_meta_u64(&patch.db, meta::APPLIED_LOG, position)
                .map_err(Failure::from)?;
            return Ok(ApplyOutcome {
                result: Err(failure.clone()),
                revision: checked.revision,
                replayed_position: None,
                duplicate: false,
            });
        }
        let revision = if patch.failed {
            checked.revision
        } else {
            next_revision(checked.revision).map_err(Failure::from)?
        };
        let stamped = Self::stamped(write, patch, revision, position)?;
        // Retention's byte budget decides before anything is written: a
        // result it could not keep never runs (see `crate::retention`).
        if let Some(failure) =
            crate::retention::over_budget_with(write, &patch.db, &stamped).map_err(Failure::from)?
        {
            write
                .set_meta_u64(&patch.db, meta::APPLIED_LOG, position)
                .map_err(Failure::from)?;
            return Ok(ApplyOutcome {
                result: Err(failure),
                revision: checked.revision,
                replayed_position: None,
                duplicate: false,
            });
        }
        let fresh = checked.revision == patch.base_revision;
        apply_journal(write, patch, fresh).map_err(Failure::from)?;
        if !patch.failed {
            write
                .set_meta_u64(&patch.db, meta::REVISION, revision)
                .map_err(Failure::from)?;
            // Recorded before the caller commits: no reader sees the
            // revision before the journal knows what it changed. A
            // writer's scratch staging never commits, so it records
            // nothing: its revision would break the chain the real
            // commit continues.
            if !write.is_scratch() {
                let changes = ChangeSet::of_patch(patch).map_err(Failure::from)?;
                self.shared.journal.record(revision, Arc::new(changes));
            }
        }
        crate::retention::charge(write, &patch.db, &stamped).map_err(Failure::from)?;
        for (identity, stored) in &stamped {
            write
                .put_receipt(&patch.db, identity, stored)
                .map_err(Failure::from)?;
        }
        write
            .set_meta_u64(&patch.db, meta::APPLIED_LOG, position)
            .map_err(Failure::from)?;
        let result = if patch.failed {
            Err(decode_failure(&patch.result).map_err(Failure::from)?)
        } else {
            Ok(parse_value(&patch.result).map_err(Failure::from)?)
        };
        Ok(ApplyOutcome {
            result,
            revision,
            replayed_position: None,
            duplicate: false,
        })
    }

    /// The receipts a patch stores when it commits at `revision` and
    /// `position`: stamped with where (see [`Committed`]), except in a
    /// writer's scratch staging, which never commits and records none.
    fn stamped(
        write: &WriteTxn,
        patch: &Patch,
        revision: u64,
        position: u64,
    ) -> HostResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let committed = Committed {
            revision,
            position: write.receipt_index(position),
        };
        patch
            .receipts
            .iter()
            .map(|(identity, receipt)| {
                let stored = if write.is_scratch() {
                    receipt.clone()
                } else {
                    Receipt::stamp(receipt, committed).map_err(Failure::from)?
                };
                Ok((identity.clone(), stored))
            })
            .collect()
    }

    /// The retention refusal of a patch's first refused scoped identity,
    /// if any (see [`crate::retention::refuse`]).
    fn refused(
        write: &WriteTxn,
        db: &[u8],
        receipts: &[(Vec<u8>, Vec<u8>)],
    ) -> HostResult<Option<Failure>> {
        for (identity, _) in receipts {
            if identity.starts_with(UNSCOPED_PREFIX) {
                continue;
            }
            let Ok(scoped) = ScopedIdentity::decode(identity) else {
                continue;
            };
            if let Some(failure) =
                crate::retention::refuse(write, db, &scoped, None).map_err(Failure::from)?
            {
                return Ok(Some(failure));
            }
        }
        Ok(None)
    }

    /// Commit one retention transition into an open write transaction
    /// at `position` (see [`crate::retention`]): its answer or its
    /// refusal, plus the database's revision, which it never moves. The
    /// position must advance the database's unless `advance` is false.
    pub fn commit_retention(
        &self,
        write: &WriteTxn,
        db: &[u8],
        command: &crate::retention::RetentionCommand,
        position: u64,
        advance: bool,
    ) -> HostResult<(HostResult<Value>, u64)> {
        let revision = write.meta_u64(db, meta::REVISION).map_err(Failure::from)?;
        if let Some(failure) = placement::refusal(write, db).map_err(Failure::from)? {
            return Ok((Err(failure), revision));
        }
        let applied = write
            .meta_u64(db, meta::APPLIED_LOG)
            .map_err(Failure::from)?;
        if position < applied || (advance && position == applied) {
            return Err(Error::corrupt(format!(
                "retention position {position} does not advance {applied}"
            ))
            .into());
        }
        let result = crate::retention::apply(write, db, command).map_err(Failure::from)?;
        write
            .set_meta_u64(db, meta::APPLIED_LOG, position)
            .map_err(Failure::from)?;
        Ok((result, revision))
    }

    /// The receipt owner a retry-session operation acts for: the
    /// principal `module`'s authorization hook decides for `operation`
    /// (`$flower.session.open` and the like, never an HTTP alias) on the
    /// state prepares read.
    pub fn session_owner(
        &self,
        db: &[u8],
        module: &str,
        operation: &str,
        options: &CallOptions,
    ) -> HostResult<[u8; HASH_LEN]> {
        let (read, marks) = self.snapshot_recalling()?;
        let options = CallOptions {
            external: false,
            ..options.clone()
        };
        let timestamp = options.timestamp_ms.unwrap_or_else(now_ms);
        let recall = marks.map(|marks| self.shared.recall(&self.catalog, db, marks));
        let authorized = authorize(
            recall.as_ref(),
            &read,
            &self.env(db),
            db,
            module,
            operation,
            &Value::Null,
            &options.presented(timestamp),
        )?;
        receipt_owner(authorized.principal())
    }

    /// Apply a committed patch atomically at one log position: check it
    /// against local state, then commit the journal, the revision bump
    /// and the applied-log advance in one transaction. Receipt expiry
    /// reads against a zero stamp; prefer [`Engine::apply_patch_at`].
    pub fn apply_patch(&mut self, patch_bytes: &[u8], position: u64) -> HostResult<ApplyOutcome> {
        self.apply_patch_at(patch_bytes, position, 0)
    }

    /// Apply a committed patch with its proposal stamp. Prefer this
    /// over [`Engine::apply_patch`] wherever the entry stamp is
    /// known: expiry reads against the stamp, not the wall clock.
    pub fn apply_patch_at(
        &mut self,
        patch_bytes: &[u8],
        position: u64,
        proposed_ms: u64,
    ) -> HostResult<ApplyOutcome> {
        let checked = self.check_patch(patch_bytes, position)?;
        let write = self.shared.store.begin_write().map_err(Failure::from)?;
        let outcome = self.commit_patch(&write, &checked, position, proposed_ms)?;
        write.commit().map_err(Failure::from)?;
        Ok(outcome)
    }

    /// Check a multi-database patch entry by entry, exactly as
    /// [`Engine::check_patch`] checks single entries. The batch rejects
    /// as one when any entry rejects, so every applier reaches the same
    /// commit-or-reject verdict.
    pub fn check_multi_patch(&self, patch_bytes: &[u8], position: u64) -> HostResult<MultiChecked> {
        let read = self.shared.store.begin_read().map_err(Failure::from)?;
        Self::check_multi_entries(&read, patch_bytes, position, |_| true)
    }

    /// Check a multi-database patch against `read`, each participant's
    /// position strictly advancing where `advance` says so for its
    /// database (see [`Engine::check_entry`]).
    pub(crate) fn check_multi_entries(
        read: &ReadTxn,
        patch_bytes: &[u8],
        position: u64,
        mut advance: impl FnMut(&[u8]) -> bool,
    ) -> HostResult<MultiChecked> {
        let patches = decode_multi_patch(patch_bytes).map_err(Failure::from)?;
        let mut checked = Vec::with_capacity(patches.len());
        let mut rejected: Option<Failure> = None;
        for patch in patches {
            let strict = advance(&patch.db);
            let entry = Self::check_entry(read, patch, position, strict)?;
            if rejected.is_none() {
                rejected = entry.rejected.clone();
            }
            checked.push(entry);
        }
        Ok(MultiChecked {
            patches: checked,
            rejected,
        })
    }

    /// Commit a checked multi-database patch into an open write
    /// transaction without committing: either every entry's journal
    /// lands with every participant's revision bump, or every
    /// participant's applied log advances with nothing else. The
    /// caller commits, so group level records share the atomic write.
    /// Duplicate batches skip every journal and replay the recorded
    /// outcome; the duplicate verdict is batch-wide so a batch never
    /// half-applies.
    pub fn commit_multi(
        &self,
        write: &WriteTxn,
        checked: &MultiChecked,
        position: u64,
        proposed_ms: u64,
    ) -> HostResult<MultiApplyOutcome> {
        // A participant that does not serve here refuses the whole
        // transaction, before anything is written (see commit_patch).
        for entry in &checked.patches {
            if let Some(failure) =
                placement::refusal(write, &entry.patch.db).map_err(Failure::from)?
            {
                return Ok(MultiApplyOutcome {
                    results: vec![Err(failure)],
                    revisions: checked
                        .patches
                        .iter()
                        .map(|entry| (entry.patch.db.clone(), entry.revision))
                        .collect(),
                    replayed_position: None,
                    duplicate: false,
                });
            }
        }
        // Duplicates replay before rejection, as in commit_patch: a
        // committed key answers its outcome, never a stale error.
        let settled = match Self::duplicate_multi_outcome(write, checked, proposed_ms)? {
            Some(((results, committed), duplicate)) => Some((results, committed, duplicate)),
            None => checked
                .rejected
                .as_ref()
                .map(|failure| (vec![Err(failure.clone())], None, false)),
        };
        if let Some((results, committed, duplicate)) = settled {
            let mut revisions = Vec::with_capacity(checked.patches.len());
            for entry in &checked.patches {
                write
                    .set_meta_u64(&entry.patch.db, meta::APPLIED_LOG, position)
                    .map_err(Failure::from)?;
                revisions.push((entry.patch.db.clone(), entry.revision));
            }
            if let (Some(committed), Some(first)) = (committed, revisions.first_mut()) {
                first.1 = committed.revision;
            }
            return Ok(MultiApplyOutcome {
                results,
                revisions,
                replayed_position: committed.map(|committed| committed.position),
                duplicate,
            });
        }
        // So does the receipt budget, for the whole batch: no entry
        // commits unless every one's receipts fit.
        for entry in &checked.patches {
            let revision = if entry.patch.failed {
                entry.revision
            } else {
                next_revision(entry.revision).map_err(Failure::from)?
            };
            let stamped = Self::stamped(write, &entry.patch, revision, position)?;
            if let Some(failure) =
                crate::retention::over_budget_with(write, &entry.patch.db, &stamped)
                    .map_err(Failure::from)?
            {
                let mut revisions = Vec::with_capacity(checked.patches.len());
                for entry in &checked.patches {
                    write
                        .set_meta_u64(&entry.patch.db, meta::APPLIED_LOG, position)
                        .map_err(Failure::from)?;
                    revisions.push((entry.patch.db.clone(), entry.revision));
                }
                return Ok(MultiApplyOutcome {
                    results: vec![Err(failure)],
                    revisions,
                    replayed_position: None,
                    duplicate: false,
                });
            }
        }
        let mut results = Vec::with_capacity(checked.patches.len());
        let mut revisions = Vec::with_capacity(checked.patches.len());
        for entry in &checked.patches {
            let outcome = self.commit_patch(write, entry, position, proposed_ms)?;
            results.push(outcome.result);
            revisions.push((entry.patch.db.clone(), outcome.revision));
        }
        Ok(MultiApplyOutcome {
            results,
            revisions,
            replayed_position: None,
            duplicate: false,
        })
    }

    /// Batch duplicate verdict: the first decisive recorded outcome
    /// across entries, in entry order, and whether it replays the
    /// original (a refusal does not). A recorded failure replays as the
    /// batch outcome; a recorded step array decomposes into per-step
    /// results. Other recorded shapes belong to another batch and miss.
    fn duplicate_multi_outcome(
        write: &WriteTxn,
        checked: &MultiChecked,
        proposed_ms: u64,
    ) -> HostResult<Option<(MultiReplay, bool)>> {
        for entry in &checked.patches {
            let found = Self::duplicate_outcome(
                write,
                &entry.patch.db,
                &entry.patch.receipts,
                proposed_ms,
            )?;
            match found {
                None => continue,
                Some(Recorded::Refused(failure)) => {
                    return Ok(Some(((vec![Err(failure)], None), false)));
                }
                Some(Recorded::Replay(Err(failure), committed)) => {
                    return Ok(Some(((vec![Err(failure)], committed), true)));
                }
                Some(Recorded::Replay(Ok(Value::Array(values)), committed))
                    if values.len() == checked.patches.len() =>
                {
                    let results = values.into_iter().map(Ok).collect();
                    return Ok(Some(((results, committed), true)));
                }
                Some(Recorded::Replay(Ok(_), _)) => continue,
            }
        }
        Ok(None)
    }

    /// Apply a committed multi-database patch atomically at one log
    /// position across every participant database.
    pub fn apply_multi_patch(
        &mut self,
        patch_bytes: &[u8],
        position: u64,
    ) -> HostResult<MultiApplyOutcome> {
        self.apply_multi_patch_at(patch_bytes, position, 0)
    }

    /// Apply a committed multi-database patch with its proposal
    /// stamp. Prefer this over [`Engine::apply_multi_patch`]
    /// wherever the entry stamp is known.
    pub fn apply_multi_patch_at(
        &mut self,
        patch_bytes: &[u8],
        position: u64,
        proposed_ms: u64,
    ) -> HostResult<MultiApplyOutcome> {
        let checked = self.check_multi_patch(patch_bytes, position)?;
        let write = self.shared.store.begin_write().map_err(Failure::from)?;
        let outcome = self.commit_multi(&write, &checked, position, proposed_ms)?;
        write.commit().map_err(Failure::from)?;
        Ok(outcome)
    }

    /// Delete expired unscoped idempotency receipts for one database,
    /// up to `limit` rows, returning rows deleted. Scoped receipts
    /// are never touched. Expiry is enforced on read, so purging only
    /// reclaims space; purged keys also leave the memory cache so
    /// redelivery re-executes instead of replaying a reclaimed
    /// outcome. Safe to run unsynchronized on every member: expiry
    /// decides correctness, deletion just follows.
    pub fn purge_expired_idempotency(
        &mut self,
        db: &[u8],
        now: u64,
        limit: usize,
    ) -> HostResult<usize> {
        let read = self.shared.store.begin_read().map_err(Failure::from)?;
        let rows = read.scan_receipts(db).map_err(Failure::from)?;
        drop(read);
        let mut victims = Vec::new();
        for (identity, bytes) in &rows {
            if victims.len() >= limit {
                break;
            }
            if !identity.starts_with(UNSCOPED_PREFIX) {
                continue;
            }
            match Receipt::decode(bytes) {
                Ok(receipt) if receipt.expired(now) => victims.push(identity.clone()),
                _ => {}
            }
        }
        if victims.is_empty() {
            return Ok(0);
        }
        let write = self.shared.store.begin_write().map_err(Failure::from)?;
        for identity in &victims {
            write.delete_receipt(db, identity).map_err(Failure::from)?;
        }
        write.commit().map_err(Failure::from)?;
        for identity in &victims {
            self.dedup_mut()
                .remove(&dedup_key(db, &identity[UNSCOPED_PREFIX.len()..]).map_err(Failure::from)?);
        }
        Ok(victims.len())
    }

    /// Enqueue an intent for later submission, returning its sequence.
    /// Duplicate identities return the existing sequence without
    /// writing: enqueue is idempotent. A full log rejects loudly.
    #[allow(clippy::too_many_arguments)]
    pub fn enqueue_intent(
        &mut self,
        db: &[u8],
        alias: &str,
        args: &Value,
        identity: &[u8],
        fingerprint: &[u8; HASH_LEN],
        expected_revision: Option<u64>,
        bound: usize,
    ) -> HostResult<u64> {
        let mut parts = alias.splitn(2, '/');
        match (parts.next(), parts.next()) {
            (Some(module), Some(name)) if !module.is_empty() && !name.is_empty() => {}
            _ => {
                return Err(fail(
                    codes::INVALID_VALUE,
                    "intent alias is not module/name",
                ));
            }
        }
        if identity.is_empty() {
            return Err(fail(codes::INVALID_VALUE, "intent identity is empty"));
        }
        let read = self.shared.store.begin_read().map_err(Failure::from)?;
        if let Some((collection, index)) = intent_ids(&read).map_err(Failure::from)?
            && let Some(intent) =
                Self::find_intent(&read, collection, index, identity).map_err(Failure::from)?
        {
            if intent.fingerprint != *fingerprint
                || intent.db != db
                || intent.alias != alias
                || intent.expected_revision != expected_revision
                || intent.args != *args
            {
                return Err(fail(
                    "identity_conflict",
                    "intent identity was reused for different work",
                ));
            }
            return Ok(intent.sequence);
        }
        let (_, count) = intent_counters(&read).map_err(Failure::from)?;
        check_bound(count, bound)?;
        drop(read);
        let write = self.shared.store.begin_write().map_err(Failure::from)?;
        let (collection, index) = ensure_intent_log(&write).map_err(Failure::from)?;
        let sequence = write
            .meta_u64(INTENT_DB, seq_meta())
            .map_err(Failure::from)?
            .checked_add(1)
            .ok_or_else(|| Error::Exhausted("intent sequences".to_string()))?;
        let count = write
            .meta_u64(INTENT_DB, count_meta())
            .map_err(Failure::from)?
            .checked_add(1)
            .ok_or_else(|| Error::Exhausted("intent count".to_string()))?;
        let intent = Intent {
            sequence,
            db: db.to_vec(),
            alias: alias.to_string(),
            args: args.clone(),
            identity: identity.to_vec(),
            fingerprint: *fingerprint,
            expected_revision,
            status: IntentStatus::Queued,
            enqueued_ms: now_ms(),
        };
        let key = sequence_key(sequence);
        write
            .put_record(
                INTENT_DB,
                collection,
                &key,
                &encode_intent(&intent).map_err(Failure::from)?,
            )
            .map_err(Failure::from)?;
        let component = crate::hex::encode(identity);
        write
            .put_index_entry(INTENT_DB, index, &[component.as_bytes()], &key)
            .map_err(Failure::from)?;
        write
            .set_meta_u64(INTENT_DB, seq_meta(), sequence)
            .map_err(Failure::from)?;
        write
            .set_meta_u64(INTENT_DB, count_meta(), count)
            .map_err(Failure::from)?;
        write.commit().map_err(Failure::from)?;
        Ok(sequence)
    }

    /// Read one intent by its stable identity.
    pub fn get_intent(&self, identity: &[u8]) -> HostResult<Option<Intent>> {
        let read = self.shared.store.begin_read().map_err(Failure::from)?;
        let Some((collection, index)) = intent_ids(&read).map_err(Failure::from)? else {
            return Ok(None);
        };
        Self::find_intent(&read, collection, index, identity).map_err(Failure::from)
    }

    /// List intents in sequence order, optionally for one database.
    pub fn list_intents(&self, db: Option<&[u8]>) -> HostResult<Vec<Intent>> {
        let read = self.shared.store.begin_read().map_err(Failure::from)?;
        let Some((collection, _)) = intent_ids(&read).map_err(Failure::from)? else {
            return Ok(Vec::new());
        };
        let rows = read
            .scan_records(INTENT_DB, collection, None, None, None, false)
            .map_err(Failure::from)?;
        let mut intents = Vec::with_capacity(rows.len());
        for (_, json) in rows {
            let intent = decode_intent(&json).map_err(Failure::from)?;
            if db.map(|db| intent.db.as_slice() == db).unwrap_or(true) {
                intents.push(intent);
            }
        }
        Ok(intents)
    }

    /// Move an intent through its lifecycle, rejecting unknown intents
    /// and illegal transitions loudly.
    pub fn mark_intent(&mut self, identity: &[u8], status: IntentStatus) -> HostResult<()> {
        let read = self.shared.store.begin_read().map_err(Failure::from)?;
        let found = match intent_ids(&read).map_err(Failure::from)? {
            None => None,
            Some((collection, index)) => {
                Self::find_intent(&read, collection, index, identity).map_err(Failure::from)?
            }
        };
        drop(read);
        let Some(mut intent) = found else {
            return Err(fail(codes::INVALID_VALUE, "unknown intent"));
        };
        if !intent.status.can_transition_to(&status) {
            return Err(fail(codes::INVALID_VALUE, "illegal intent transition"));
        }
        intent.status = status;
        let write = self.shared.store.begin_write().map_err(Failure::from)?;
        let (collection, _) = ensure_intent_log(&write).map_err(Failure::from)?;
        let key = sequence_key(intent.sequence);
        write
            .put_record(
                INTENT_DB,
                collection,
                &key,
                &encode_intent(&intent).map_err(Failure::from)?,
            )
            .map_err(Failure::from)?;
        write.commit().map_err(Failure::from)?;
        Ok(())
    }

    /// Drop an intent from the queue, reporting whether it existed.
    pub fn drop_intent(&mut self, identity: &[u8]) -> HostResult<bool> {
        let read = self.shared.store.begin_read().map_err(Failure::from)?;
        let found = match intent_ids(&read).map_err(Failure::from)? {
            None => None,
            Some((collection, index)) => {
                Self::find_intent(&read, collection, index, identity).map_err(Failure::from)?
            }
        };
        drop(read);
        let Some(intent) = found else {
            return Ok(false);
        };
        let write = self.shared.store.begin_write().map_err(Failure::from)?;
        let (collection, index) = ensure_intent_log(&write).map_err(Failure::from)?;
        let key = sequence_key(intent.sequence);
        write
            .delete_record(INTENT_DB, collection, &key)
            .map_err(Failure::from)?;
        let component = crate::hex::encode(identity);
        write
            .delete_index_entry(INTENT_DB, index, &[component.as_bytes()], &key)
            .map_err(Failure::from)?;
        let count = write
            .meta_u64(INTENT_DB, count_meta())
            .map_err(Failure::from)?;
        write
            .set_meta_u64(INTENT_DB, count_meta(), count.saturating_sub(1))
            .map_err(Failure::from)?;
        write.commit().map_err(Failure::from)?;
        Ok(true)
    }

    /// Look one intent up by identity through the identity index.
    fn find_intent(
        read: &ReadTxn,
        collection: u32,
        index: u32,
        identity: &[u8],
    ) -> crate::error::Result<Option<Intent>> {
        let component = crate::hex::encode(identity);
        let first = ScanRange {
            limit: Some(1),
            ..ScanRange::default()
        };
        let rows = read.scan_index(INTENT_DB, index, 1, &[component.as_bytes()], first)?;
        let Some((_, primary)) = rows.into_iter().next() else {
            return Ok(None);
        };
        match read.get_record(INTENT_DB, collection, &primary)? {
            None => Ok(None),
            Some(json) => Ok(Some(decode_intent(&json)?)),
        }
    }

    /// Preview a database's live intents: evaluate the longest
    /// successful prefix against the snapshot and return the applied
    /// identities plus the overlay for preview-mode reads. Only queued,
    /// submitted and acknowledged intents preview.
    pub fn preview(&self, db: &[u8], options: &PreviewOptions) -> HostResult<Preview> {
        let intents = self.list_intents(Some(db))?;
        let live: Vec<Intent> = intents
            .into_iter()
            .take_while(|intent| !matches!(intent.status, IntentStatus::Unresolved { .. }))
            .filter(|intent| {
                matches!(
                    intent.status,
                    IntentStatus::Queued | IntentStatus::Submitted | IntentStatus::Acknowledged
                )
            })
            .collect();
        let read = self.shared.store.begin_read().map_err(Failure::from)?;
        preview::preview_intents(&read, &self.env(db), db, &live, options)
    }

    /// Invoke a query in preview mode: the snapshot plus a preview
    /// overlay, without persisting anything the overlay touched.
    pub fn invoke_query_preview(
        &self,
        db: &[u8],
        module: &str,
        name: &str,
        args: &Value,
        overlay: &Staged,
        options: &CallOptions,
    ) -> HostResult<Value> {
        let marks = self.shared.marks();
        let read = self.shared.store.begin_read().map_err(Failure::from)?;
        let timestamp = options.timestamp_ms.unwrap_or_else(now_ms);
        // The hook decides on committed state: queued intents are not yet
        // authority for anything.
        let authorized = authorize(
            Some(&self.shared.recall(&self.catalog, db, marks)),
            &read,
            &self.env(db),
            db,
            module,
            name,
            args,
            &options.presented(timestamp),
        )?;
        let mut session = begin_invocation(
            self.active_def(db, module),
            authorized.definition(),
            options.deadline,
            |fuel| {
                Invocation::new(
                    module.to_string(),
                    db.to_vec(),
                    CallerKind::Query,
                    authorized.principal().cloned(),
                    None,
                    timestamp,
                    fuel,
                )
            },
        );
        session.staged = overlay.clone();
        session.set_ephemeral(true);
        let target = Target {
            kind: CallKind::Query,
            module,
            name: authorized.definition(),
            args,
        };
        let outcome = lane::run(
            &self.shared.store,
            &read,
            &mut session,
            &self.env(db),
            target,
            CommitSpec::default(),
        )?;
        Ok(outcome.value)
    }

    /// Invoke a transaction: run the planner without a context, validate
    /// its calls, then admit and commit the batch atomically. The result
    /// is the array of per-call outcomes.
    pub fn invoke_transaction(
        &mut self,
        db: &[u8],
        module: &str,
        planner: &str,
        args: &Value,
        options: &MutationOptions,
    ) -> HostResult<Value> {
        if self.replicated.contains(db) {
            return Err(fail(
                "replicated_db",
                "mutations on a replicated database go through consensus",
            ));
        }
        // Authorization (and any replay) precedes planning: the planner
        // is guest code too.
        let begun = self.begin_mutation(db, module, planner, args, options, false)?;
        let (read, mut session, receipt, definition) = match begun {
            BeginOutcome::Done(done) => return done,
            BeginOutcome::Replayed(done, _) => return done,
            BeginOutcome::Proceed {
                read,
                session,
                receipt,
                definition,
            } => (read, session, receipt, definition),
        };
        // One deadline for the planner and every call it plans.
        session.budget.within = transaction_deadline(
            session
                .budget
                .declared
                .unwrap_or(crate::runtime::wasm::DEFAULT_TIMEOUT),
        );
        let calls = self.plan_transaction(db, module, &definition, args, session.budget)?;
        let commit = CommitSpec {
            receipt,
            expected_revision: options.expected_revision,
        };
        let outcome = lane::run_transaction(
            &self.shared.store,
            &read,
            &mut session,
            &self.env(db),
            module,
            &calls,
            commit,
        );
        drop(read);
        self.cache_durable(db, options);
        outcome.map(|outcome| outcome.value)
    }

    /// Evaluate a transaction without committing it: plan, admit, then
    /// patch extraction. The caller proposes
    /// [`PrepareOutcome::Patch`] through consensus and returns
    /// [`PrepareOutcome::Done`] as-is.
    pub fn prepare_transaction(
        &mut self,
        db: &[u8],
        module: &str,
        planner: &str,
        args: &Value,
        options: &MutationOptions,
    ) -> HostResult<PrepareOutcome> {
        let begun = self.begin_mutation(db, module, planner, args, options, false)?;
        let (read, mut session, receipt, definition) = match begun {
            BeginOutcome::Done(done) => return Ok(PrepareOutcome::Done(done)),
            BeginOutcome::Replayed(done, committed) => {
                return Ok(PrepareOutcome::Replayed(done, committed));
            }
            BeginOutcome::Proceed {
                read,
                session,
                receipt,
                definition,
            } => (read, session, receipt, definition),
        };
        // One deadline for the planner and every call it plans.
        session.budget.within = transaction_deadline(
            session
                .budget
                .declared
                .unwrap_or(crate::runtime::wasm::DEFAULT_TIMEOUT),
        );
        let calls = self.plan_transaction(db, module, &definition, args, session.budget)?;
        let commit = CommitSpec {
            receipt,
            expected_revision: options.expected_revision,
        };
        let prepared =
            lane::prepare_transaction(&read, &mut session, &self.env(db), module, &calls, commit)?;
        // No memory store here: the outcome is only cacheable once
        // consensus accepts the proposal.
        Ok(PrepareOutcome::Patch(prepared))
    }

    /// Prepare a distributed participant with all read dependencies recorded.
    /// Cached derived reads include their source dependencies. Overflow and
    /// reads outside the snapshot conservatively retain a database fence.
    pub fn prepare_transaction_multi_locked(
        &mut self,
        steps: &[MultiStep],
        options: &MutationOptions,
    ) -> HostResult<(PrepareOutcome, crate::storage::overlay::ReadSet)> {
        let recording = Arc::new(std::sync::Mutex::new(
            crate::storage::overlay::ReadSet::participant(),
        ));
        let previous = self.recording.replace(recording.clone());
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.prepare_transaction_multi(steps, options)
        }));
        self.recording = previous;
        let outcome = result.unwrap_or_else(|panic| std::panic::resume_unwind(panic))?;
        let reads = recording
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        Ok((outcome, reads))
    }

    /// Evaluate a multi-database transaction without committing it:
    /// validate the step list, then prepare one mutation per step in
    /// order, each revision-guarded to its own database. Any step
    /// failure aborts the transaction with no patch; the caller
    /// proposes [`PrepareOutcome::Patch`] through consensus as one
    /// entry and returns [`PrepareOutcome::Done`] as-is.
    ///
    /// Steps run in isolated sessions: a step never observes another
    /// step's staged writes, so every step names a different database
    /// and same-database batches stay with single-database
    /// transactions. Retry identities and caller expected revisions
    /// are rejected: per-step scoped receipts are future work, and the
    /// guard is automatic per participant. An idempotency key stores
    /// one durable receipt for the whole batch under the first step's
    /// database; aborts leave no receipt and redeliver by re-executing.
    pub fn prepare_transaction_multi(
        &mut self,
        steps: &[MultiStep],
        options: &MutationOptions,
    ) -> HostResult<PrepareOutcome> {
        let (mut patches, combined, receipt) = match self.evaluate_multi(steps, options)? {
            MultiEvaluated::Done(done) => return Ok(PrepareOutcome::Done(done)),
            MultiEvaluated::Replayed(done, committed) => {
                return Ok(PrepareOutcome::Replayed(done, committed));
            }
            MultiEvaluated::Ready {
                patches,
                combined,
                receipt,
            } => (patches, combined, receipt),
        };
        // The transaction receipt lives with the first step: one
        // durable outcome for the whole batch.
        if let Some(receipt) = receipt {
            patches[0].receipts.push(receipt);
        }
        let bytes = encode_multi_patch(&patches).map_err(Failure::from)?;
        crate::patch::check_transaction_size(bytes.len())?;
        // No memory store here: the outcome is only cacheable once
        // consensus accepts the proposal.
        Ok(PrepareOutcome::Patch(lane::PreparedPatch {
            patch: bytes,
            outcome: Ok(combined),
        }))
    }

    /// Run a multi-database transaction over local-only databases,
    /// committing every step's journal in one write transaction. Any
    /// participant marked replicated rejects: replicated databases
    /// commit through consensus only.
    pub fn invoke_transaction_multi(
        &mut self,
        steps: &[MultiStep],
        options: &MutationOptions,
    ) -> HostResult<Value> {
        if steps.iter().any(|step| self.replicated.contains(&step.db)) {
            return Err(fail(
                "replicated_db",
                "mutations on a replicated database go through consensus",
            ));
        }
        let (patches, combined, receipt) = match self.evaluate_multi(steps, options)? {
            MultiEvaluated::Done(done) => return done,
            MultiEvaluated::Replayed(done, _) => return done,
            MultiEvaluated::Ready {
                patches,
                combined,
                receipt,
            } => (patches, combined, receipt),
        };
        let write = self.shared.store.begin_write().map_err(Failure::from)?;
        for patch in &patches {
            Self::commit_local_journal(&write, patch)?;
        }
        if let Some((identity, encoded)) = &receipt {
            write
                .put_receipt(&patches[0].db, identity, encoded)
                .map_err(Failure::from)?;
        }
        write.commit().map_err(Failure::from)?;
        self.cache_durable(&steps[0].db, options);
        Ok(combined)
    }

    /// Shared multi-database evaluation: validate steps and options,
    /// answer from the batch's idempotency receipt when one exists, else
    /// prepare every step in its own session, revision-guarded to its
    /// database. The first failing step aborts the batch.
    fn evaluate_multi(
        &mut self,
        steps: &[MultiStep],
        options: &MutationOptions,
    ) -> HostResult<MultiEvaluated> {
        check_transaction_steps(steps)?;
        Self::check_multi_options(options)?;
        // One deadline for every step, and one logical budget they draw
        // from in turn.
        let within = transaction_deadline(TRANSACTION_TIMEOUT);
        let mut fuel = fuel_for(u32::try_from(TRANSACTION_TIMEOUT.as_millis()).ok());
        // Every participant must serve here before any step resolves its
        // method: a database that moved away has no modules left here.
        for step in steps {
            self.serving(&step.db)?;
        }
        let timestamp = options.call.timestamp_ms.unwrap_or_else(now_ms);
        // Every step's module decides before the batch replays or runs.
        // The batch receipt lives with the first step, so the principal
        // its module decided owns it.
        let (read, marks) = self.snapshot_recalling()?;
        let mut decided = Vec::with_capacity(steps.len());
        for (index, step) in steps.iter().enumerate() {
            let recall = marks.map(|marks| self.shared.recall(&self.catalog, &step.db, marks));
            decided.push(
                authorize(
                    recall.as_ref(),
                    &read,
                    &self.env(&step.db),
                    &step.db,
                    &step.module,
                    &step.method,
                    &step.args,
                    &options.call.presented(timestamp),
                )
                .map_err(|failure| transaction_failure(failure, index, step))?,
            );
        }
        let owner = receipt_owner(decided[0].principal())?;
        if let Some(key) = options.idempotency_key.as_ref() {
            if let Some(hit) = self.dedup_hit(&read, &steps[0].db, key, &owner, timestamp)? {
                return Ok(hit.into_multi());
            }
            if let Some(hit) = Self::durable_dedup_hit(&read, &steps[0].db, key, &owner, timestamp)?
            {
                return Ok(hit.into_multi());
            }
        }
        drop(read);
        let mut patches = Vec::with_capacity(steps.len());
        let mut values = Vec::with_capacity(steps.len());
        for (index, (step, authorized)) in steps.iter().zip(&decided).enumerate() {
            let read = self.snapshot().map_err(Failure::from)?;
            let mut granted = 0;
            let mut session = begin_invocation(
                self.active_def(&step.db, &step.module),
                authorized.definition(),
                options.call.deadline,
                |own| {
                    granted = own.min(fuel);
                    Invocation::new(
                        step.module.clone(),
                        step.db.clone(),
                        CallerKind::Mutation,
                        authorized.principal().cloned(),
                        None,
                        timestamp,
                        granted,
                    )
                },
            );
            session.budget.within = within;
            let expected = read.base_revision(&step.db).map_err(Failure::from)?;
            let target = Target {
                kind: CallKind::Mutation,
                module: &step.module,
                name: authorized.definition(),
                args: &step.args,
            };
            let commit = CommitSpec {
                receipt: None,
                expected_revision: Some(expected),
            };
            let prepared = lane::prepare(&read, &mut session, &self.env(&step.db), target, commit)?;
            fuel = fuel.saturating_sub(granted.saturating_sub(session.fuel_left()));
            match prepared.outcome {
                Ok(value) => {
                    values.push(value);
                    patches.push(decode_patch(&prepared.patch).map_err(Failure::from)?);
                }
                // Aborts leave no receipt of any kind: nothing
                // committed, so redelivery re-executes and may succeed
                // against changed state.
                Err(failure) => {
                    return Ok(MultiEvaluated::Done(Err(transaction_failure(
                        failure, index, step,
                    ))));
                }
            }
        }
        let combined = Value::Array(values);
        let receipt = match options.idempotency_key.as_ref() {
            None => None,
            Some(key) => {
                let json = render(&combined).map_err(|_| {
                    fail(codes::INVALID_VALUE, "transaction result is not storable")
                })?;
                // A caller's own key records the epoch it ran in, which
                // retention collects it by (see `caller_receipt`).
                let read = self.snapshot().map_err(Failure::from)?;
                let (floor, expires_at) =
                    crate::retention::caller_receipt(&read, &steps[0].db, timestamp)
                        .map_err(Failure::from)?;
                let receipt = Receipt {
                    result: Some(Outcome::Value(json)),
                    fingerprint: owner,
                    floor,
                    expires_at,
                    committed: None,
                };
                Some((
                    unscoped_identity_key(key),
                    receipt.encode().map_err(Failure::from)?,
                ))
            }
        };
        Ok(MultiEvaluated::Ready {
            patches,
            combined,
            receipt,
        })
    }

    /// Validate multi-database options: no retry identity (receipts
    /// are per-database) and no caller expected revision (each
    /// participant guards automatically).
    fn check_multi_options(options: &MutationOptions) -> HostResult<()> {
        if options.retry.is_some() {
            return Err(fail(
                codes::INVALID_VALUE,
                "a multi-database transaction takes no retry identity",
            ));
        }
        if options.expected_revision.is_some() {
            return Err(fail(
                codes::INVALID_VALUE,
                "a multi-database transaction guards each database automatically",
            ));
        }
        Ok(())
    }

    /// Commit one prepared journal to a local-only database through the
    /// shared journal applier: journal plus revision bump, no
    /// applied-log advance and no receipts.
    fn commit_local_journal(write: &WriteTxn, patch: &Patch) -> HostResult<()> {
        if let Some(refusal) = txn::write_patch_refusal(write, patch).map_err(Failure::from)? {
            return Err(refusal);
        }
        let revision = write
            .meta_u64(&patch.db, meta::REVISION)
            .map_err(Failure::from)?;
        apply_journal(write, patch, revision == patch.base_revision).map_err(Failure::from)?;
        write
            .set_meta_u64(
                &patch.db,
                meta::REVISION,
                next_revision(revision).map_err(Failure::from)?,
            )
            .map_err(Failure::from)?;
        Ok(())
    }

    /// Run a transaction planner without a context, under the
    /// transaction's `budget`, and validate its calls.
    fn plan_transaction(
        &self,
        db: &[u8],
        module: &str,
        planner: &str,
        args: &Value,
        budget: crate::runtime::wasm::Budget,
    ) -> HostResult<Vec<PlannedCall>> {
        plan_in(&self.env(db), module, planner, args, budget)
    }
}

/// Run a transaction planner of `module` in `env` without a context,
/// under the transaction's `budget`, and validate its calls.
fn plan_in(
    env: &Env<'_>,
    module: &str,
    planner: &str,
    args: &Value,
    budget: crate::runtime::wasm::Budget,
) -> HostResult<Vec<PlannedCall>> {
    let Some(defs) = env.descriptors.get(module) else {
        return Err(fail(
            codes::UNKNOWN_METHOD,
            format!("unknown transaction `{planner}`"),
        ));
    };
    match defs.def_kinds.get(planner).copied() {
        Some(DefKind::Transaction) => {}
        Some(_) => {
            return Err(fail(
                codes::INVALID_INVOCATION,
                format!("`{planner}` is not a transaction"),
            ));
        }
        None => {
            return Err(fail(
                codes::UNKNOWN_METHOD,
                format!("unknown transaction `{planner}`"),
            ));
        }
    }
    if let Some(schema) = defs.args_schema(planner)
        && let Err(error) = schema.parse(args)
    {
        return Err(error.invalid_argument(&format!("arguments for `{planner}`")));
    }
    let runtime = env.runtimes.get(module).ok_or_else(|| {
        fail(
            crate::host::MODULE_UNAVAILABLE,
            format!("no runtime for `{module}`"),
        )
    })?;
    let planned = crate::runtime::wasm::with_budget(budget, || runtime.plan(module, planner, args))
        .map_err(|failure| failure.failure)?;
    planned_calls(&planned)
}

/// Whether `definition` of `module` in `env` is a planner transaction: a
/// mutation call naming one runs it, as v1's `http` map served
/// `transaction` methods.
fn plans(env: &Env<'_>, module: &str, definition: &str) -> bool {
    env.descriptors
        .get(module)
        .and_then(|defs| defs.def_kinds.get(definition))
        .is_some_and(|kind| *kind == DefKind::Transaction)
}

impl Engine {
    /// Shared mutation admission: option checks, unscoped dedup, scoped
    /// admission and session setup. Either short-circuits with a final
    /// result or proceeds to the lane.
    /// System calls (timer fires) skip the authorization hook and run
    /// with definer rights: they have no caller.
    fn begin_mutation(
        &mut self,
        db: &[u8],
        module: &str,
        name: &str,
        args: &Value,
        options: &MutationOptions,
        system: bool,
    ) -> HostResult<BeginOutcome> {
        let dedup = self.dedup.clone();
        let dedup = dedup.read().unwrap_or_else(PoisonError::into_inner);
        let prepare = self.prepare_on(&dedup)?;
        begin_mutation_in(prepare, db, module, name, args, options, system)
    }

    /// What this engine's prepare paths read: the staged state inside
    /// [`Engine::on_staged`] (and committed state beside it), else
    /// committed state.
    fn prepare_on<'a>(&'a self, dedup: &'a UnscopedDedup) -> HostResult<PrepareOn<'a>> {
        let committed = match self.staged {
            Some(_) => Some(self.shared.store.begin_read().map_err(Failure::from)?),
            None => None,
        };
        let (read, marks) = self.snapshot_recalling()?;
        Ok(PrepareOn {
            catalog: &self.catalog,
            dedup,
            read,
            committed,
            clock: self.clock,
            recall: marks.map(|marks| (&*self.shared, marks)),
        })
    }

    /// Run `work` with every mutation that names no timestamp of its own
    /// prepared at `clock`, as [`Engine::on_staged`] runs it on staged
    /// state: a writer preparing a wave of writes speculatively gives
    /// the wave one clock, so a write prepared again in its turn runs at
    /// the instant its speculative preparation did, and clocks never go
    /// back along a batch (see [`crate::speculation`]).
    pub fn at_clock<T>(&mut self, clock: Option<u64>, work: impl FnOnce(&mut Engine) -> T) -> T {
        let previous = std::mem::replace(&mut self.clock, clock);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| work(self)));
        self.clock = previous;
        result.unwrap_or_else(|panic| std::panic::resume_unwind(panic))
    }

    /// Speculate on the staged state `overlay` holds (see
    /// [`crate::speculation`]): mutations prepared through the handle,
    /// on any thread, read committed state seen through it, at `clock`
    /// when they name no timestamp. Only valid while the staged
    /// transaction the overlay came from is open.
    pub fn speculation(
        &self,
        overlay: Arc<crate::storage::Overlay>,
        clock: Option<u64>,
    ) -> crate::speculation::Speculation {
        crate::speculation::Speculation {
            shared: self.shared.clone(),
            catalog: self.catalog.clone(),
            dedup: self.dedup.clone(),
            overlay,
            clock,
        }
    }
}

/// What a mutation's preparation reads besides its call: the code, the
/// idempotency cache, the state it prepares on and, when that is a
/// writer's staged state, committed state, where the database must
/// serve too (see [`Engine::serving`]); and the clock it runs at when
/// the call names none.
pub(crate) struct PrepareOn<'a> {
    pub(crate) catalog: &'a Catalog,
    pub(crate) dedup: &'a UnscopedDedup,
    pub(crate) read: ReadTxn,
    pub(crate) committed: Option<ReadTxn>,
    pub(crate) clock: Option<u64>,
    /// The engine state decisions are recalled from and kept in, with the
    /// marks taken before `read`, when `read` is committed state.
    pub(crate) recall: Option<(&'a Shared, Marks)>,
}

/// Evaluate a mutation without committing it, on `on`: the lane's
/// [`Engine::prepare_mutation`], wherever it runs. Also whether its
/// evaluation read anything no dependency names.
pub(crate) fn prepare_mutation_on(
    on: PrepareOn<'_>,
    db: &[u8],
    module: &str,
    name: &str,
    args: &Value,
    options: &MutationOptions,
) -> (HostResult<PrepareOutcome>, bool) {
    let catalog = on.catalog;
    let begun = match begin_mutation_in(on, db, module, name, args, options, false) {
        Ok(begun) => begun,
        Err(failure) => return (Err(failure), false),
    };
    let (read, mut session, receipt, definition) = match begun {
        BeginOutcome::Done(done) => return (Ok(PrepareOutcome::Done(done)), false),
        BeginOutcome::Replayed(done, committed) => {
            return (Ok(PrepareOutcome::Replayed(done, committed)), false);
        }
        BeginOutcome::Proceed {
            read,
            session,
            receipt,
            definition,
        } => (read, session, receipt, definition),
    };
    let commit = CommitSpec {
        receipt,
        expected_revision: options.expected_revision,
    };
    let env = catalog.env(db);
    if plans(&env, module, &definition) {
        session.budget.within = transaction_deadline(
            session
                .budget
                .declared
                .unwrap_or(crate::runtime::wasm::DEFAULT_TIMEOUT),
        );
        let calls = match plan_in(&env, module, &definition, args, session.budget) {
            Ok(calls) => calls,
            Err(failure) => return (Err(failure), false),
        };
        let prepared = lane::prepare_transaction(&read, &mut session, &env, module, &calls, commit);
        return (prepared.map(PrepareOutcome::Patch), session.read_volatile());
    }
    let target = Target {
        kind: CallKind::Mutation,
        module,
        name: &definition,
        args,
    };
    let prepared = lane::prepare(&read, &mut session, &env, target, commit);
    // No memory store here: the outcome is only cacheable once
    // consensus accepts the proposal. Storing at prepare time would
    // replay uncommitted outcomes when proposing fails.
    (prepared.map(PrepareOutcome::Patch), session.read_volatile())
}

/// Shared mutation admission on `on`: option checks, unscoped dedup,
/// scoped admission and session setup. Either short-circuits with a
/// final result or proceeds to the lane. System calls (timer fires)
/// skip the authorization hook and run with definer rights: they have
/// no caller.
fn begin_mutation_in(
    on: PrepareOn<'_>,
    db: &[u8],
    module: &str,
    name: &str,
    args: &Value,
    options: &MutationOptions,
    system: bool,
) -> HostResult<BeginOutcome> {
    let PrepareOn {
        catalog,
        dedup,
        read,
        committed,
        clock,
        recall,
    } = on;
    serving(&read, db)?;
    if let Some(committed) = &committed {
        serving(committed, db)?;
    }
    if options.retry.is_some() && options.idempotency_key.is_some() {
        return Err(fail(
            codes::INVALID_VALUE,
            "a mutation takes either a retry identity or an idempotency key, not both",
        ));
    }
    let timestamp = options.call.timestamp_ms.or(clock).unwrap_or_else(now_ms);
    // Authorization decides before any replay: a revoked caller never
    // recovers a recorded result, and the receipt's owner is the
    // principal the hook decided, not one the caller claimed.
    let authorized = if system {
        Authorized::system(module, name)
    } else {
        authorize(
            recall
                .map(|(shared, marks)| shared.recall(catalog, db, marks))
                .as_ref(),
            &read,
            &catalog.env(db),
            db,
            module,
            name,
            args,
            &options.call.presented(timestamp),
        )?
    };
    let principal = authorized.principal();
    if let Some(key) = options.idempotency_key.as_ref() {
        let owner = receipt_owner(principal)?;
        if let Some(hit) = dedup_hit(dedup, &read, db, key, &owner, timestamp)? {
            return Ok(hit.into_begin());
        }
        if let Some(hit) = Engine::durable_dedup_hit(&read, db, key, &owner, timestamp)? {
            return Ok(hit.into_begin());
        }
    }
    let receipt = match options.retry.as_ref() {
        None => match options.idempotency_key.as_ref() {
            None => None,
            Some(key) => {
                // The epoch it runs in, which retention collects it by.
                let (floor, expires_at) = crate::retention::caller_receipt(&read, db, timestamp)
                    .map_err(Failure::from)?;
                Some(ReceiptSpec {
                    identity: unscoped_identity_key(key),
                    fingerprint: receipt_owner(principal)?,
                    floor,
                    expires_at,
                })
            }
        },
        Some(retry) => {
            // Current authorization precedes any replay.
            if let Err(failure) =
                catalog
                    .policy
                    .allow_invoke(principal, module, authorized.definition())
            {
                return Ok(BeginOutcome::Done(Err(failure)));
            }
            let owner = receipt_owner(principal)?;
            match Engine::admit(&read, db, retry, &owner, timestamp)? {
                AdmitOutcome::Done(done) => return Ok(BeginOutcome::Done(done)),
                AdmitOutcome::Replayed(done, committed) => {
                    return Ok(BeginOutcome::Replayed(done, committed));
                }
                AdmitOutcome::Execute(spec) => Some(spec),
            }
        }
    };
    let mut session = Box::new(begin_invocation(
        catalog.modules(db).descriptors.get(module),
        authorized.definition(),
        options.call.deadline,
        |fuel| {
            Invocation::new(
                module.to_string(),
                db.to_vec(),
                CallerKind::Mutation,
                principal.cloned(),
                None,
                timestamp,
                fuel,
            )
        },
    ));
    if system {
        session.enter_definer();
    }
    Ok(BeginOutcome::Proceed {
        read,
        session,
        receipt,
        definition: authorized.definition().to_string(),
    })
}

/// Look up an unscoped idempotency result in memory for one database:
/// the stored outcome envelope, decoded, when `owner` produced it;
/// another owner's entry answers with a conflict. Expired entries miss;
/// undecodable entries miss too, since the memory path only mirrors the
/// durable table, which then answers. Memory answers only while the
/// receipt it mirrors is kept (retention collects receipts in the log,
/// out of its sight), and that receipt says where the outcome committed.
fn dedup_hit(
    dedup: &UnscopedDedup,
    read: &ReadTxn,
    db: &[u8],
    key: &[u8],
    owner: &[u8; HASH_LEN],
    now: u64,
) -> HostResult<Option<Hit>> {
    let Some(bytes) = dedup_key(db, key)
        .ok()
        .and_then(|scoped| dedup.get(&scoped, now))
    else {
        return Ok(None);
    };
    let Some((stored_owner, envelope)) = bytes.split_at_checked(HASH_LEN) else {
        return Ok(None);
    };
    if stored_owner != owner {
        return Ok(Some(Hit::Refuse(foreign_receipt())));
    }
    let Ok(value) = Value::decode_binary(envelope) else {
        return Ok(None);
    };
    let Some(stored) = read
        .get_receipt(db, &unscoped_identity_key(key))
        .map_err(Failure::from)?
    else {
        return Ok(None);
    };
    let committed = Receipt::decode(&stored).map_err(Failure::from)?.committed;
    Ok(Some(Hit::Replay(decode_envelope(value), committed)))
}

impl Engine {
    /// [`dedup_hit`] on this engine's idempotency cache.
    fn dedup_hit(
        &self,
        read: &ReadTxn,
        db: &[u8],
        key: &[u8],
        owner: &[u8; HASH_LEN],
        now: u64,
    ) -> HostResult<Option<Hit>> {
        let dedup = self.dedup.read().unwrap_or_else(PoisonError::into_inner);
        dedup_hit(&dedup, read, db, key, owner, now)
    }

    /// Look up a durable unscoped idempotency receipt: the recorded
    /// result and where it committed, when `owner` produced it; another
    /// owner's receipt answers with a conflict, revealing nothing of
    /// its result. Expired, released and missing rows miss so the work
    /// executes afresh; corrupt rows fail as storage corruption.
    /// Otherwise first result wins until expiry.
    fn durable_dedup_hit(
        read: &ReadTxn,
        db: &[u8],
        key: &[u8],
        owner: &[u8; HASH_LEN],
        now: u64,
    ) -> HostResult<Option<Hit>> {
        let Some(found) = Self::durable_receipt(read, db, key, now)? else {
            return Ok(None);
        };
        if found.owner != *owner {
            return Ok(Some(Hit::Refuse(foreign_receipt())));
        }
        Ok(Some(Hit::Replay(found.result, found.committed)))
    }

    /// The live durable unscoped receipt for a key.
    fn durable_receipt(
        read: &ReadTxn,
        db: &[u8],
        key: &[u8],
        now: u64,
    ) -> HostResult<Option<LiveReceipt>> {
        let identity = unscoped_identity_key(key);
        let Some(bytes) = read.get_receipt(db, &identity).map_err(Failure::from)? else {
            return Ok(None);
        };
        let owner = Receipt::decode(&bytes).map_err(Failure::from)?.fingerprint;
        match StoredReceipt::decode(&bytes, now).map_err(Failure::from)? {
            StoredReceipt::Expired | StoredReceipt::Released => Ok(None),
            StoredReceipt::Replay(result, expires_at, committed) => Ok(Some(LiveReceipt {
                owner,
                result,
                expires_at,
                committed,
            })),
        }
    }

    /// Mirror a local call's durable idempotency receipt into memory.
    /// Only outcomes with a receipt in the table cache — committed
    /// results and persisted business failures — so the memory path
    /// answers exactly what the table answers after a restart: budget
    /// exhaustion, poison, stale revisions and failed commits leave no
    /// receipt and re-execute on redelivery. The entry expires with
    /// the receipt.
    fn cache_durable(&mut self, db: &[u8], options: &MutationOptions) {
        let Some(key) = options.idempotency_key.as_deref() else {
            return;
        };
        let now = options.call.timestamp_ms.unwrap_or_else(now_ms);
        let found = self
            .shared
            .store
            .begin_read()
            .ok()
            .and_then(|read| Self::durable_receipt(&read, db, key, now).ok().flatten());
        if let Some(found) = found {
            self.remember(db, key, &found.owner, &found.result, found.expires_at);
        }
    }

    /// Insert one outcome into the memory fast path with the owner its
    /// receipt records, skipping outcomes whose envelope cannot encode
    /// rather than failing the call.
    fn remember(
        &mut self,
        db: &[u8],
        key: &[u8],
        owner: &[u8; HASH_LEN],
        outcome: &HostResult<Value>,
        expires_at: u64,
    ) {
        let envelope = match outcome {
            Ok(value) => encode_ok(value.clone()),
            Err(failure) => encode_err(failure),
        };
        if let (Ok(bytes), Ok(key)) = (envelope.encode_binary(), dedup_key(db, key)) {
            let mut owned = Vec::with_capacity(HASH_LEN + bytes.len());
            owned.extend_from_slice(owner);
            owned.extend_from_slice(&bytes);
            self.dedup_mut().insert(&key, owned, expires_at);
        }
    }

    /// Cache a consensus-accepted idempotency outcome for one database:
    /// the cluster layer calls this after its proposal applies, with the
    /// outcome the applier committed (which duplicate-skipping may have
    /// replayed rather than executed). The outcome caches only while
    /// its durable receipt is live, and never past the receipt's
    /// expiry, so memory never answers what the table would not.
    pub fn cache_idempotency(
        &mut self,
        db: &[u8],
        key: &[u8],
        outcome: &Result<Value, Failure>,
        horizon: u64,
    ) {
        let found = self.shared.store.begin_read().ok().and_then(|read| {
            Self::durable_receipt(&read, db, key, now_ms())
                .ok()
                .flatten()
        });
        if let Some(found) = found {
            let expires_at = match found.expires_at {
                0 => horizon,
                expires_at => expires_at.min(horizon),
            };
            self.remember(db, key, &found.owner, outcome, expires_at);
        }
    }

    /// Where the committed receipt of a mutation's retry identity or
    /// idempotency key says its outcome committed: `None` when no
    /// receipt is committed under it (yet), `Some(None)` when the one
    /// committed records nothing (written before receipts recorded it).
    pub fn receipt_commit(
        &self,
        db: &[u8],
        options: &MutationOptions,
    ) -> crate::error::Result<Option<Option<Committed>>> {
        let identity = match (&options.retry, &options.idempotency_key) {
            (Some(retry), _) => retry.identity.encode()?,
            (None, Some(key)) => unscoped_identity_key(key),
            (None, None) => return Ok(None),
        };
        let read = self.shared.store.begin_read()?;
        read.get_receipt(db, &identity)?
            .map(|bytes| Receipt::decode(&bytes).map(|receipt| receipt.committed))
            .transpose()
    }

    /// The replay horizon for an idempotency key: the call timestamp
    /// plus the receipt TTL. Callers with old timestamps get already
    /// expiring receipts, which simply re-execute on redelivery.
    pub fn idempotency_horizon(options: &MutationOptions) -> u64 {
        options
            .call
            .timestamp_ms
            .unwrap_or_else(now_ms)
            .saturating_add(IDEMPOTENCY_TTL_MS)
    }

    /// Decide admission for a scoped retry identity against incarnation,
    /// floors, clock (`now`) and any stored receipt. Authorization runs
    /// first, in the caller.
    fn admit(
        read: &ReadTxn,
        db: &[u8],
        retry: &RetryIdentity,
        owner: &[u8; HASH_LEN],
        now: u64,
    ) -> HostResult<AdmitOutcome> {
        // Replicated retention first, exactly as every apply decides.
        if let Some(failure) = crate::retention::refuse(read, db, &retry.identity, Some(owner))
            .map_err(Failure::from)?
        {
            return Ok(AdmitOutcome::Done(Err(failure)));
        }
        let history = placement::History::read(read, db).map_err(Failure::from)?;
        let floor = retention_floor(read, db).map_err(Failure::from)?;
        let identity_bytes = retry.identity.encode().map_err(Failure::from)?;
        let stored = read
            .get_receipt(db, &identity_bytes)
            .map_err(Failure::from)?
            .map(|bytes| Receipt::decode(&bytes))
            .transpose()
            .map_err(Failure::from)?;
        match admit(
            &history,
            floor,
            now,
            true,
            &retry.identity,
            &retry.fingerprint,
            stored.as_ref(),
        ) {
            Admit::Replay(receipt) => {
                match StoredReceipt::of(&receipt, now).map_err(Failure::from)? {
                    // A replayed business failure fails the call itself, as
                    // the original call did; replayed values answer as results.
                    StoredReceipt::Replay(result, _, committed) => {
                        Ok(AdmitOutcome::Replayed(Ok(result?), committed))
                    }
                    StoredReceipt::Released => {
                        Ok(AdmitOutcome::Done(Err(reject(RejectReason::Released))))
                    }
                    StoredReceipt::Expired => {
                        Err(Error::corrupt("admission replayed an expired receipt").into())
                    }
                }
            }
            Admit::Execute => Ok(AdmitOutcome::Execute(ReceiptSpec {
                identity: identity_bytes,
                fingerprint: retry.fingerprint,
                floor,
                expires_at: 0,
            })),
            Admit::Reject(reason) => Ok(AdmitOutcome::Done(Err(reject(reason)))),
        }
    }
}

/// A replayed multi-database outcome: per-step results and where the
/// original committed, when its receipt records it.
type MultiReplay = (Vec<HostResult<Value>>, Option<Committed>);

/// A multi-database batch after shared evaluation: answered without
/// executing, or every step's journal ready to commit or propose.
enum MultiEvaluated {
    /// The final answer: the aborting failure.
    Done(HostResult<Value>),
    /// The batch receipt answered, with where it committed.
    Replayed(HostResult<Value>, Option<Committed>),
    /// Every step succeeded.
    Ready {
        /// One prepared journal per step, in step order.
        patches: Vec<Patch>,
        /// The array of step values.
        combined: Value,
        /// The batch's idempotency receipt, if keyed.
        receipt: Option<(Vec<u8>, Vec<u8>)>,
    },
}

/// The admission outcome: answer without executing (a replayed result
/// or a rejection), or execute with a receipt spec.
enum AdmitOutcome {
    /// Return this result without executing.
    Done(HostResult<Value>),
    /// The stored receipt answered, with where it committed.
    Replayed(HostResult<Value>, Option<Committed>),
    /// Execute and commit with this receipt spec.
    Execute(ReceiptSpec),
}

/// An idempotency key that already has an outcome.
enum Hit {
    /// The caller's own: replay the outcome, with where it committed.
    Replay(HostResult<Value>, Option<Committed>),
    /// Another principal's: refuse, revealing nothing of the receipt.
    Refuse(Failure),
}

impl Hit {
    fn into_begin(self) -> BeginOutcome {
        match self {
            Hit::Replay(result, committed) => BeginOutcome::Replayed(result, committed),
            Hit::Refuse(failure) => BeginOutcome::Done(Err(failure)),
        }
    }

    fn into_multi(self) -> MultiEvaluated {
        match self {
            Hit::Replay(result, committed) => MultiEvaluated::Replayed(result, committed),
            Hit::Refuse(failure) => MultiEvaluated::Done(Err(failure)),
        }
    }
}

/// A live durable unscoped receipt.
struct LiveReceipt {
    /// Who produced it.
    owner: [u8; HASH_LEN],
    /// Its recorded outcome.
    result: HostResult<Value>,
    /// Its expiry, or zero for none.
    expires_at: u64,
    /// Where the outcome committed, when the receipt records it.
    committed: Option<Committed>,
}

/// The memory dedup key for an idempotency key: scoped by database, so
/// tenants reusing a key in different databases never share outcomes.
fn dedup_key(db: &[u8], key: &[u8]) -> crate::error::Result<Vec<u8>> {
    let mut scoped = Vec::with_capacity(db.len() + 4 + key.len());
    push_sized(&mut scoped, db)?;
    scoped.extend_from_slice(key);
    Ok(scoped)
}

/// The retention floor admission enforces: the minimum of the stored
/// floors, or zero when none are set.
fn retention_floor(read: &ReadTxn, db: &[u8]) -> crate::error::Result<u64> {
    match read.meta(db, meta::RETENTION_FLOORS)? {
        None => Ok(0),
        Some(bytes) => Ok(decode_floors(&bytes)?.into_iter().min().unwrap_or(0)),
    }
}

/// Map an admission rejection to its failure. Authorization never reaches
/// here: the policy's own failure rejects first.
fn reject(reason: RejectReason) -> Failure {
    match reason {
        RejectReason::Unauthorized => fail("unauthorized", "invocation is not authorized"),
        RejectReason::WrongHistory => fail(
            "wrong_history",
            "retry identity belongs to a different history",
        ),
        RejectReason::Retired => fail(
            "epoch_retired",
            "retry identity epoch is below the retention floor",
        ),
        RejectReason::Conflict => fail(
            "identity_conflict",
            "retry identity reused with a different intent",
        ),
        RejectReason::Released => fail(
            "receipt_released",
            "receipt was released by acknowledgement",
        ),
    }
}

/// Validate planner output into planned calls: an array of
/// `{module, name, args, savepoint}` objects. What they commit is
/// bounded by its size, as every transaction is
/// ([`crate::patch::check_transaction_size`]).
fn planned_calls(planned: &Value) -> HostResult<Vec<PlannedCall>> {
    let items = match planned {
        Value::Array(items) => items,
        _ => {
            return Err(fail(codes::INVALID_VALUE, "planner output is not an array"));
        }
    };
    let mut calls = Vec::with_capacity(items.len());
    for item in items {
        let map = match item {
            Value::Object(map) => map,
            _ => {
                return Err(fail(codes::INVALID_VALUE, "planned call is not an object"));
            }
        };
        let member = |key: &str| {
            map.get(key).ok_or_else(|| {
                fail(
                    codes::INVALID_VALUE,
                    format!("planned call without `{key}`"),
                )
            })
        };
        let module = match member("module")? {
            Value::String(module) => module.clone(),
            _ => return Err(fail(codes::TYPE_MISMATCH, "planned module is not a string")),
        };
        let name = match member("name")? {
            Value::String(name) => name.clone(),
            _ => return Err(fail(codes::TYPE_MISMATCH, "planned name is not a string")),
        };
        let savepoint = match member("savepoint")? {
            Value::Bool(savepoint) => *savepoint,
            _ => {
                return Err(fail(
                    codes::TYPE_MISMATCH,
                    "planned savepoint is not a boolean",
                ));
            }
        };
        calls.push(PlannedCall {
            module,
            name,
            args: member("args")?.clone(),
            savepoint,
        });
    }
    Ok(calls)
}

/// Current time in milliseconds since the Unix epoch.
pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}
