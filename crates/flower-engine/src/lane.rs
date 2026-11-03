//! The writer lane: settle, commit, triggers, transactions.
//!
//! [`run`] drives one top-level query or mutation: authorize, invoke the
//! guest, then commit. Queries drop user writes (guests cannot stage any)
//! and persist derived caches best-effort without touching the revision
//! ([`run_query`] only while the snapshot is still current, since queries
//! run concurrently with the writer); mutations apply the staged journal, enforce foreign keys, invalidate
//! derived values, fire triggers and commit atomically with a receipt.
//! [`run_transaction`] drives a planner's calls in one session and commits
//! them atomically. [`prepare`] and [`prepare_transaction`] evaluate the
//! same way but extract the fixpoint journal as a replicated patch.
//!
//! [`settle`] is the mid-execution fixpoint guests invoke explicitly:
//! invalidate derived values touched by staged writes and fire triggers
//! for them, looping until the journal stabilizes (fuel bounds the loop).
//! Commit repeats the same loop over its write transaction, adds foreign
//! key enforcement and caps cascade rounds explicitly.
//!
//! Foreign keys resolve within the writing module: single-field references
//! match the field's canonical bytes against target keys, multi-field
//! references match the canonical array of field values, `Key` matches
//! the row key, and `KeyPrefix` matches structurally (a scalar target key
//! prefixes an array row key by first element, array prefixes match
//! element-wise). Missing and null fields skip the check; cascade and
//! set_null stage follow-up writes that re-enter the commit loop.

use std::collections::{BTreeMap, HashSet};

use flower_sdk::error::codes;
use flower_sdk::{CallFailure, Failure, Value, fail};

use crate::certificate::{Certificate, Graph, Validity};
use crate::derived::{IndexChange, RecordChange, failure_value};
use crate::descriptors::{CollectionDef, DefKind, OnDelete, PlaceDescriptor};
use crate::error::Error;
use crate::host::{Env, FUEL_CALL, Host, OpError, OpResult};
use crate::json::{parse_value, render};
use crate::patch::{apply_index, apply_record, install_derived};
use crate::retry::{FailureData, HASH_LEN, Outcome, Receipt};
use crate::runtime::CallKind;
use crate::session::{CallerKind, Invocation, StagedOp};
use crate::storage::codec::decode_outcome;
use crate::storage::keys::decode_derived_key;
use crate::storage::{GroupStore, IndexEntry, ReadTxn, ScanRange, WriteTxn, meta};

/// Deepest commit cascade: trigger and foreign-key rounds past this abort
/// the commit instead of looping forever.
pub const MAX_COMMIT_ROUNDS: u32 = 16;

/// A committed lane run: the top-level value. Business failures return
/// `Err` with nothing committed; poison failures return the poison.
pub struct LaneOutcome {
    /// The top-level result value.
    pub value: Value,
}

/// One planned transaction call: target, arguments and whether a
/// recovered failure continues the batch instead of aborting it.
#[derive(Clone, Debug)]
pub struct PlannedCall {
    /// Target module identity.
    pub module: String,
    /// Target definition name.
    pub name: String,
    /// Call arguments.
    pub args: Value,
    /// Continue the batch on a recovered failure.
    pub savepoint: bool,
}

/// Receipt identity for a mutation commit, minus the result: the lane
/// fills the outcome from the top-level result or business failure and
/// stages the receipt atomically with the commit.
#[derive(Clone, Debug)]
pub struct ReceiptSpec {
    /// Encoded scoped identity bytes (the receipt key suffix).
    pub identity: Vec<u8>,
    /// Intent fingerprint the receipt pins.
    pub fingerprint: [u8; HASH_LEN],
    /// Retention floor the receipt counts toward.
    pub floor: u64,
    /// Expiry instant, or zero for no expiry.
    pub expires_at: u64,
}

/// What a top-level call invokes.
#[derive(Clone, Copy, Debug)]
pub struct Target<'a> {
    /// Entry-point kind.
    pub kind: CallKind,
    /// Target module identity.
    pub module: &'a str,
    /// Target definition name.
    pub name: &'a str,
    /// Call arguments.
    pub args: &'a Value,
}

/// How a mutation commits: the receipt staged atomically with a
/// successful commit and the revision the commit must find.
#[derive(Clone, Debug, Default)]
pub struct CommitSpec {
    /// Receipt for a retry identity or idempotency key, if any.
    pub receipt: Option<ReceiptSpec>,
    /// Revision the submission was written against; the commit or apply
    /// rejects with `stale_revision` when the database moved.
    pub expected_revision: Option<u64>,
}

/// Drive one top-level query or mutation and commit it. The receipt
/// stages atomically with a successful mutation commit; the expected
/// revision rejects the commit when the database moved.
pub fn run(
    store: &GroupStore,
    read: &ReadTxn,
    session: &mut Invocation,
    env: &Env,
    target: Target,
    commit: CommitSpec,
) -> Result<LaneOutcome, Failure> {
    let outcome = execute_top_level(read, session, env, target)?;
    let mut host = Host::new(read, session, env);
    match outcome {
        Ok(value) => finish(store, &mut host, target.kind, commit, value),
        Err(failure) => {
            // Failed top levels commit no data: derived caches computed
            // over uncommitted staged writes must not persist. Business
            // failures still persist their receipt best-effort so retries
            // replay instead of re-executing.
            let failure = business_failure(&mut host, failure)?;
            if let Some(spec) = commit.receipt.as_ref() {
                commit_failure_receipt(store, &host, spec, &failure);
            }
            Err(failure)
        }
    }
}

/// Drive one top-level query and persist its derived caches
/// best-effort. Queries evaluate concurrently with the writer, so a
/// commit may land between the snapshot and the cache write: the
/// caches install only while the database still sits at the
/// snapshot's revision and `current` confirms, inside the cache write,
/// what the store cannot show (that the deployed code is unchanged).
/// Otherwise they drop and the next read re-evaluates.
pub fn run_query(
    store: &GroupStore,
    read: &ReadTxn,
    session: &mut Invocation,
    env: &Env,
    target: Target,
    current: &dyn Fn() -> bool,
) -> Result<Value, Failure> {
    run_query_with(store, read, session, env, target, current, false).map(|(value, _)| value)
}

/// [`run_query`], also returning the certificate of what the query
/// observed (see [`crate::certificate`]), taken before its derived
/// caches leave the overlay.
pub fn run_query_certified(
    store: &GroupStore,
    read: &ReadTxn,
    session: &mut Invocation,
    env: &Env,
    target: Target,
    current: &dyn Fn() -> bool,
) -> Result<(Value, Certificate), Failure> {
    let (value, observation) = run_query_with(store, read, session, env, target, current, true)?;
    let certificate = observation.map_or_else(Certificate::broad, |(certificate, _)| certificate);
    Ok((value, certificate))
}

/// [`run_query_certified`], also returning the derived graph the query
/// read through (see [`Graph`]) when its result is stable: what lets a
/// later reader keep the result across a change that leaves the derived
/// values it read as they were.
pub fn run_query_observed(
    store: &GroupStore,
    read: &ReadTxn,
    session: &mut Invocation,
    env: &Env,
    target: Target,
    current: &dyn Fn() -> bool,
) -> Result<(Value, Certificate, Option<Graph>), Failure> {
    let (value, observation) = run_query_with(store, read, session, env, target, current, true)?;
    let (certificate, graph) = observation.unwrap_or_else(|| (Certificate::broad(), None));
    Ok((value, certificate, graph))
}

/// What a finished query observed: its certificate, and the derived graph
/// a stable result read through.
type Observation = (Certificate, Option<Graph>);

/// One top-level query, certified when asked.
fn run_query_with(
    store: &GroupStore,
    read: &ReadTxn,
    session: &mut Invocation,
    env: &Env,
    target: Target,
    current: &dyn Fn() -> bool,
    certify: bool,
) -> Result<(Value, Option<Observation>), Failure> {
    let target = Target {
        kind: CallKind::Query,
        ..target
    };
    let outcome = execute_top_level(read, session, env, target)?;
    let mut host = Host::new(read, session, env);
    match outcome {
        Ok(value) => {
            if let Some(poison) = host.session.take_poison() {
                return Err(poison);
            }
            let observation = if certify {
                Some(observed_graph(&mut host).map_err(Failure::from)?)
            } else {
                None
            };
            commit_query_caches(store, &mut host, current);
            Ok((value, observation))
        }
        // Failed top levels commit nothing, caches included.
        Err(failure) => Err(business_failure(&mut host, failure)?),
    }
}

/// [`observed`], with the derived graph a stable result read through.
/// A graph that cannot be told leaves the result without one.
fn observed_graph(host: &mut Host) -> crate::error::Result<Observation> {
    let deps = host.session.take_observed();
    let certificate = Certificate::observe(
        host.read,
        &host.session.staged,
        &host.session.db,
        deps.clone(),
        host.session.changes_at(),
        host.session.timestamp_ms,
    )?;
    let stable = !certificate.is_broad()
        && !certificate.reads_keys()
        && certificate.validity() == Validity::Stable;
    let graph = if stable {
        Graph::observe(host.read, &host.session.staged, &host.session.db, &deps)
            .ok()
            .flatten()
            .filter(Graph::is_stable)
    } else {
        None
    };
    Ok((certificate, graph))
}

/// What deriving a cached result's touched values again found (see
/// [`rederive`]).
pub(crate) enum Rederived {
    /// Every instance the query read itself came out as it was: the
    /// graph of the same result.
    Kept(Graph),
    /// One came out different: the query evaluates, starting from the
    /// outcomes derived so far rather than deriving them twice.
    Changed(crate::session::Staged),
    /// The derivation went where a stable result cannot: the query
    /// evaluates afresh.
    Unknown,
}

/// Re-derive the instances `graph` read whose own reads a change touched
/// (`touched`, see [`Graph::touched`]), children first, and the instances
/// above them only when a child came out different; stop at the first one the
/// query read itself that came out different. A kept result installs what it
/// derived as a query's derived caches do.
pub(crate) fn rederive(
    store: &GroupStore,
    read: &ReadTxn,
    session: &mut Invocation,
    env: &Env,
    graph: &Graph,
    touched: &[bool],
    current: &dyn Fn() -> bool,
) -> crate::error::Result<Rederived> {
    let mut host = Host::new(read, session, env);
    let nodes: Vec<_> = graph.nodes().collect();
    let mut root = vec![false; nodes.len()];
    for at in graph.roots() {
        root[*at] = true;
    }
    let mut changed = vec![false; nodes.len()];
    let mut rederived = vec![false; nodes.len()];
    for (at, ((definition, args), children, digest)) in nodes.iter().enumerate() {
        if !touched[at] && !children.iter().any(|child| changed[*child]) {
            continue;
        }
        let qualified = host
            .read
            .intern_name(&host.session.db, *definition)?
            .ok_or_else(|| Error::corrupt("derived definition is not interned"))?;
        let (module, name) = crate::host::split_qualified(&qualified)?;
        let value = parse_value(args)?;
        match host.get_or_evaluate(&module, &name, &value) {
            Ok(_) => {}
            // A business failure is an outcome like a value.
            Err(error) if !error.fatal => {}
            Err(_) => return Ok(Rederived::Unknown),
        }
        if host.session.is_poisoned()
            || host.session.time_polled()
            || host.session.changes_at().is_some()
            || host.session.read_volatile()
        {
            return Ok(Rederived::Unknown);
        }
        let now = crate::certificate::served(
            host.read,
            &host.session.staged,
            &host.session.db,
            &(*definition, args.clone()),
        )?;
        rederived[at] = true;
        changed[at] = now.as_ref() != Some(*digest);
        if changed[at] && root[at] {
            return Ok(Rederived::Changed(std::mem::take(&mut host.session.staged)));
        }
    }
    let refreshed = graph.refreshed(read, &host.session.staged, &host.session.db, &rederived)?;
    commit_query_caches(store, &mut host, current);
    Ok(match refreshed.filter(Graph::is_stable) {
        Some(graph) => Rederived::Kept(graph),
        None => Rederived::Unknown,
    })
}

/// The certificate of what a finished invocation read, its staged
/// derived edges included.
pub(crate) fn observed(host: &mut Host) -> crate::error::Result<Certificate> {
    let deps = host.session.take_observed();
    Certificate::observe(
        host.read,
        &host.session.staged,
        &host.session.db,
        deps,
        host.session.changes_at(),
        host.session.timestamp_ms,
    )
}

/// Evaluate one top-level call without committing it, extracting the
/// fixpoint journal as a patch for replication. Admission short-circuits
/// happen before this runs; poison still fails without a patch.
pub fn prepare(
    read: &ReadTxn,
    session: &mut Invocation,
    env: &Env,
    target: Target,
    commit: CommitSpec,
) -> Result<PreparedPatch, Failure> {
    let outcome = execute_top_level(read, session, env, target)?;
    let mut host = Host::new(read, session, env);
    match outcome {
        Ok(value) => prepare_finish(&mut host, commit, value),
        Err(failure) => {
            // Business failures replicate their receipt without data, so
            // retries replay on every member instead of re-executing.
            let failure = business_failure(&mut host, failure)?;
            prepare_failure_patch(&mut host, commit.receipt, &failure)
        }
    }
}

/// How preparing one maintenance-task run ended (see [`prepare_task`]).
#[derive(Debug)]
pub enum TaskRun {
    /// `due` said the task has no work at the invocation's time.
    NotDue,
    /// The run (or the error callback) succeeded: its patch.
    Ran(PreparedPatch),
    /// `due`, the run or the error callback failed, its business
    /// failure or the budget or trap that ended it: nothing of it
    /// stays.
    Failed(Failure),
}

/// Whether a failure is the store's rather than the guest's: storage
/// failures stop a task's preparation instead of counting against it.
fn infrastructure(failure: &Failure) -> bool {
    matches!(
        failure.code.as_str(),
        "host_storage" | crate::error::IO_FAILURE | "host_corrupt" | "host_exhausted"
    )
}

/// What a failed task invocation means: a storage failure fails the
/// preparation, anything else (a business failure, a trap, an exhausted
/// budget, a poisoned invocation) fails the task.
fn task_failure(host: &mut Host, failure: CallFailure) -> Result<TaskRun, Failure> {
    let failure = match business_failure(host, failure) {
        Ok(failure) | Err(failure) => failure,
    };
    if infrastructure(&failure) {
        return Err(failure);
    }
    Ok(TaskRun::Failed(failure))
}

/// Prepare one run of a maintenance task as a patch, at the session's
/// time: `due` first, as a query of the task's name over the same
/// state (read-only), and only when it names that time or earlier the
/// run, as a mutation of the task's name. A successful run also deletes
/// the task's failure state, `clear`, when one is kept. The session
/// runs with definer rights; nothing commits here.
pub fn prepare_task(
    read: &ReadTxn,
    session: &mut Invocation,
    env: &Env,
    module: &str,
    task: &str,
    clear: Option<(u32, Vec<u8>)>,
    commit: CommitSpec,
) -> Result<TaskRun, Failure> {
    let mut host = Host::new(read, session, env);
    if host.session.kind != CallerKind::Mutation {
        return Err(fail(codes::INVALID_INVOCATION, "tasks run as mutations"));
    }
    let now = host.session.timestamp_ms as f64;
    let saved_kind = std::mem::replace(&mut host.session.kind, CallerKind::Query);
    let saved_writable = host.session.replace_writable(false);
    let due = host.invoke_runtime(CallKind::Query, module, task, &Value::Null);
    host.session.kind = saved_kind;
    host.session.replace_writable(saved_writable);
    match due.map_err(|error| fail_op(&mut host, error))? {
        Ok(Value::Null) => return Ok(TaskRun::NotDue),
        Ok(Value::Number(at)) if at > now => return Ok(TaskRun::NotDue),
        Ok(Value::Number(_)) => {}
        Ok(_) => {
            return Ok(TaskRun::Failed(fail(
                codes::INVALID_VALUE,
                "a task's due time is a number or null",
            )));
        }
        Err(failure) => return task_failure(&mut host, failure),
    }
    if let Err(error) = host.session.spend(FUEL_CALL) {
        return Ok(TaskRun::Failed(error.into()));
    }
    let value = match host
        .invoke_runtime(CallKind::Mutation, module, task, &Value::Null)
        .map_err(|error| fail_op(&mut host, error))?
    {
        Ok(value) => value,
        Err(failure) => return task_failure(&mut host, failure),
    };
    if let Some((collection, key)) = clear {
        host.session.staged.stage_record(collection, key, None);
    }
    match prepare_finish(&mut host, commit, value) {
        Ok(prepared) => Ok(TaskRun::Ran(prepared)),
        Err(failure) if infrastructure(&failure) => Err(failure),
        Err(failure) => Ok(TaskRun::Failed(failure)),
    }
}

/// Prepare a task's error callback as a patch: a mutation of the task's
/// name handed the failed run's `{error, failed_at}`, over the state the
/// run saw. It never answers [`TaskRun::NotDue`].
pub fn prepare_task_error(
    read: &ReadTxn,
    session: &mut Invocation,
    env: &Env,
    module: &str,
    task: &str,
    failure: &Failure,
    commit: CommitSpec,
) -> Result<TaskRun, Failure> {
    let mut host = Host::new(read, session, env);
    let mut args = BTreeMap::new();
    args.insert("error".to_string(), failure_value(failure));
    args.insert(
        "failed_at".to_string(),
        Value::Number(host.session.timestamp_ms as f64),
    );
    if let Err(error) = host.session.spend(FUEL_CALL) {
        return Ok(TaskRun::Failed(error.into()));
    }
    let value = match host
        .invoke_runtime(CallKind::Mutation, module, task, &Value::Object(args))
        .map_err(|error| fail_op(&mut host, error))?
    {
        Ok(value) => value,
        Err(failure) => return task_failure(&mut host, failure),
    };
    match prepare_finish(&mut host, commit, value) {
        Ok(prepared) => Ok(TaskRun::Ran(prepared)),
        Err(failure) if infrastructure(&failure) => Err(failure),
        Err(failure) => Ok(TaskRun::Failed(failure)),
    }
}

/// Classify a failed call: unrecovered failures poison the invocation,
/// and a poisoned invocation fails with its poison. Otherwise the
/// business failure stands and may persist a receipt.
fn business_failure(host: &mut Host, failure: CallFailure) -> Result<Failure, Failure> {
    if !failure.recovered {
        host.session.poison(failure.failure.clone());
    }
    match host.session.take_poison() {
        Some(poison) => Err(poison),
        None => Ok(failure.failure),
    }
}

/// Authorize, validate and invoke one top-level call, leaving the staged
/// journal for the caller to commit or extract. Shared by [`run`] and
/// [`prepare`].
fn execute_top_level(
    read: &ReadTxn,
    session: &mut Invocation,
    env: &Env,
    target: Target,
) -> Result<Result<Value, CallFailure>, Failure> {
    let Target {
        kind,
        module,
        name,
        args,
    } = target;
    let mut host = Host::new(read, session, env);
    let expected = match kind {
        CallKind::Query => CallerKind::Query,
        CallKind::Mutation => CallerKind::Mutation,
        CallKind::Derived | CallKind::Reduce => {
            return Err(fail(
                codes::INVALID_INVOCATION,
                "derived runs only through get_derived",
            ));
        }
    };
    if host.session.kind != expected {
        return Err(fail(
            codes::INVALID_INVOCATION,
            "session kind does not match the invocation",
        ));
    }
    host.policy
        .allow_invoke(host.session.principal.as_ref(), module, name)?;
    let defined = host
        .descriptors
        .get(module)
        .and_then(|defs| defs.def_kinds.get(name))
        .copied();
    let wanted = match kind {
        CallKind::Query => DefKind::Query,
        CallKind::Mutation => DefKind::Mutation,
        CallKind::Derived | CallKind::Reduce => DefKind::Derived,
    };
    match defined {
        Some(found) if found == wanted => {}
        Some(_) => {
            return Err(fail(
                codes::INVALID_INVOCATION,
                format!("`{name}` is not callable as a {}", kind_name(kind)),
            ));
        }
        None => {
            return Err(fail(
                codes::UNKNOWN_METHOD,
                format!("unknown definition `{name}`"),
            ));
        }
    }
    if let Some(schema) = host
        .descriptors
        .get(module)
        .and_then(|defs| defs.args_schema(name))
        && let Err(error) = schema.parse(args)
    {
        return Err(error.invalid_argument(&format!("arguments for `{name}`")));
    }
    host.session.spend(FUEL_CALL)?;
    match host.invoke_runtime(kind, module, name, args) {
        Ok(outcome) => Ok(outcome),
        Err(error) => {
            if error.fatal {
                host.session.poison(error.failure.clone());
            }
            Err(error.failure)
        }
    }
}

/// How a planned batch ended: every call ran (recovered savepoint
/// failures encode in place), or a recovered failure on a call without
/// a savepoint aborted the batch like a top-level business failure.
enum Planned {
    /// Per-call results in plan order.
    Completed(Vec<Value>),
    /// The aborting business failure.
    Aborted(Failure),
}

/// Drive a planner's calls in one session, each under its own
/// savepoint. Shared by [`run_transaction`] and [`prepare_transaction`].
fn drive_planned(host: &mut Host, module: &str, calls: &[PlannedCall]) -> Result<Planned, Failure> {
    if host.session.kind != CallerKind::Mutation {
        return Err(fail(
            codes::INVALID_INVOCATION,
            "transactions run as mutations",
        ));
    }
    let mut results = Vec::with_capacity(calls.len());
    for call in calls {
        if host.session.is_poisoned() {
            break;
        }
        let (kind, def) = match host
            .descriptors
            .get(&call.module)
            .and_then(|defs| defs.def_kinds.get(&call.name))
            .copied()
        {
            Some(DefKind::Query) => (CallKind::Query, DefKind::Query),
            Some(DefKind::Mutation) => (CallKind::Mutation, DefKind::Mutation),
            _ => {
                return Err(fail(
                    codes::UNKNOWN_METHOD,
                    format!("unknown callable `{}`", call.name),
                ));
            }
        };
        host.session.check_call(def)?;
        if let Some(schema) = host
            .descriptors
            .get(&call.module)
            .and_then(|defs| defs.args_schema(&call.name))
            && let Err(error) = schema.parse(&call.args)
        {
            return Err(error.invalid_argument(&format!("arguments for `{}`", call.name)));
        }
        host.policy.allow_call(
            host.session.principal.as_ref(),
            module,
            &call.module,
            &call.name,
        )?;
        if let Err(error) = host.session.spend(FUEL_CALL) {
            let failure: Failure = error.into();
            host.session.poison(failure.clone());
            return Err(failure);
        }
        host.session.push_savepoint();
        // The call runs under its own declared timeout, never past the
        // transaction's deadline.
        let transaction = host.session.budget;
        host.session.budget.declared = host
            .descriptors
            .get(&call.module)
            .and_then(|defs| defs.timeouts.get(&call.name))
            .map(|ms| std::time::Duration::from_millis(u64::from(*ms)));
        let outcome = host.invoke_nested(kind, &call.module, &call.name, &call.args);
        host.session.budget = transaction;
        match outcome {
            Ok(Ok(value)) => {
                host.session.pop_savepoint(false);
                results.push(value);
            }
            Ok(Err(failure)) => {
                host.session.pop_savepoint(true);
                if failure.recovered && call.savepoint {
                    results.push(failure_value(&failure.failure));
                    continue;
                }
                if !failure.recovered {
                    host.session.poison(failure.failure.clone());
                    return Err(failure.failure);
                }
                // A recovered failure without a savepoint aborts the
                // batch: nothing continues, so nothing poisons.
                return Ok(Planned::Aborted(failure.failure));
            }
            Err(error) => {
                host.session.pop_savepoint(true);
                if error.fatal {
                    host.session.poison(error.failure.clone());
                }
                return Err(error.failure);
            }
        }
    }
    Ok(Planned::Completed(results))
}

/// Drive a planner's calls in one session and commit them atomically. The
/// result is the array of per-call outcomes; recovered failures on
/// savepoint calls encode as `{"code","message","details"}` in place.
pub fn run_transaction(
    store: &GroupStore,
    read: &ReadTxn,
    session: &mut Invocation,
    env: &Env,
    module: &str,
    calls: &[PlannedCall],
    commit: CommitSpec,
) -> Result<LaneOutcome, Failure> {
    let mut host = Host::new(read, session, env);
    match drive_planned(&mut host, module, calls)? {
        Planned::Completed(results) => finish(
            store,
            &mut host,
            CallKind::Mutation,
            commit,
            Value::Array(results),
        ),
        Planned::Aborted(failure) => {
            // The receipt persists best-effort, like a top-level failure.
            if let Some(spec) = commit.receipt.as_ref() {
                commit_failure_receipt(store, &host, spec, &failure);
            }
            Err(failure)
        }
    }
}

/// Evaluate a planner's calls without committing them, extracting the
/// fixpoint journal as a patch for replication.
pub fn prepare_transaction(
    read: &ReadTxn,
    session: &mut Invocation,
    env: &Env,
    module: &str,
    calls: &[PlannedCall],
    commit: CommitSpec,
) -> Result<PreparedPatch, Failure> {
    let mut host = Host::new(read, session, env);
    match drive_planned(&mut host, module, calls)? {
        Planned::Completed(results) => prepare_finish(&mut host, commit, Value::Array(results)),
        // The receipt replicates instead of committing.
        Planned::Aborted(failure) => prepare_failure_patch(&mut host, commit.receipt, &failure),
    }
}

/// Finish a successful top level: poison fails the lane, then queries
/// persist caches best-effort and mutations commit.
fn finish(
    store: &GroupStore,
    host: &mut Host,
    kind: CallKind,
    commit: CommitSpec,
    value: Value,
) -> Result<LaneOutcome, Failure> {
    if let Some(poison) = host.session.take_poison() {
        return Err(poison);
    }
    match kind {
        CallKind::Query => {
            // Ephemeral (preview) sessions never persist caches:
            // values computed over an overlay would corrupt the
            // committed cache.
            if !host.session.ephemeral() {
                commit_query_caches(store, host, &|| true);
            }
            Ok(LaneOutcome { value })
        }
        CallKind::Mutation => {
            // A changes_at declaration is a freshness hint for watches
            // and delivery; the core records it and commits normally,
            // and replication honors it.
            match commit_mutation(store, host, commit, &value) {
                Ok(()) => Ok(LaneOutcome { value }),
                Err(error) => Err(fail_op(host, error)),
            }
        }
        CallKind::Derived | CallKind::Reduce => Err(fail(
            codes::INVALID_INVOCATION,
            "derived runs only through get_derived",
        )),
    }
}

/// Surface an operation error as the lane's failure, poisoning first
/// when it is fatal.
fn fail_op(host: &mut Host, error: OpError) -> Failure {
    if error.fatal {
        host.session.poison(error.failure.clone());
    }
    error.failure
}

/// A prepared patch: encoded effects plus the evaluated outcome. The
/// outcome replicates too: business failures commit their receipt on
/// every member instead of re-executing.
#[derive(Clone, Debug)]
pub struct PreparedPatch {
    /// Encoded [`Patch`][crate::patch::Patch] bytes.
    pub patch: Vec<u8>,
    /// The evaluated value or business failure.
    pub outcome: Result<Value, Failure>,
}

/// Finish a successful prepare: run the rounds without committing and
/// extract the fixpoint journal as a patch.
fn prepare_finish(
    host: &mut Host,
    commit: CommitSpec,
    value: Value,
) -> Result<PreparedPatch, Failure> {
    if let Some(poison) = host.session.take_poison() {
        return Err(poison);
    }
    let outcome = (|| -> OpResult<PreparedPatch> {
        if let Some(spec) = commit.receipt.as_ref() {
            stage_success_receipt(host, spec, &value)?;
        }
        rounds(host, None)?;
        let result = render(&value).map_err(|_| {
            OpError::from(fail(
                codes::INVALID_VALUE,
                "mutation result is not storable",
            ))
        })?;
        extract_patch(host, result, commit.expected_revision)
    })();
    outcome.map_err(|error| fail_op(host, error))
}

/// Extract a business failure as a receipt-only patch: no data, no
/// revision bump. Staged writes from the failed execution are dropped,
/// exactly as the commit path drops them.
fn prepare_failure_patch(
    host: &mut Host,
    receipt: Option<ReceiptSpec>,
    failure: &Failure,
) -> Result<PreparedPatch, Failure> {
    let outcome = (|| -> OpResult<PreparedPatch> {
        let mut receipts = Vec::new();
        if let Some(spec) = receipt {
            receipts.push(encode_failure_receipt(&spec, failure)?);
        }
        let patch = crate::patch::Patch {
            failed: true,
            db: host.session.db.clone(),
            versions: crate::staging::deployed_versions(host.read, &host.session.db)?,
            records: Vec::new(),
            indexes: Vec::new(),
            derived: Vec::new(),
            drops: Vec::new(),
            receipts,
            result: crate::patch::encode_failure(failure)?,
            expected_revision: None,
            base_revision: host.read.base_revision(&host.session.db)?,
        };
        let patch = crate::patch::encode_patch(&patch)?;
        // The failure (and its receipt) replicates like any effect.
        crate::patch::check_transaction_size(patch.len())?;
        Ok(PreparedPatch {
            patch,
            outcome: Err(failure.clone()),
        })
    })();
    outcome.map_err(|error| fail_op(host, error))
}

/// Extract the staged fixpoint as a patch: journal, derived outcomes,
/// evictions, receipts, every deployed module version and the snapshot
/// revision the derived outcomes were computed at.
fn extract_patch(
    host: &mut Host,
    result: Vec<u8>,
    expected_revision: Option<u64>,
) -> OpResult<PreparedPatch> {
    let db = host.session.db.clone();
    let records = host
        .session
        .staged
        .all_records()
        .into_iter()
        .map(|(collection, key, value)| crate::patch::RecordWrite {
            collection,
            key,
            value,
        })
        .collect();
    let indexes = host
        .session
        .staged
        .all_index()
        .into_iter()
        .map(
            |((index, components, primary), present)| crate::patch::IndexWrite {
                index,
                components,
                primary,
                present,
            },
        )
        .collect();
    let mut derived = Vec::new();
    for (key, outcome, observed) in host.session.staged.take_derived() {
        let (definition, args) = decode_derived_key(&db, &key)?;
        derived.push(crate::patch::DerivedWrite {
            definition,
            args,
            outcome,
            observed,
        });
    }
    let mut drops = Vec::new();
    for key in host.session.staged.take_drops() {
        drops.push(decode_derived_key(&db, &key)?);
    }
    let patch = crate::patch::Patch {
        failed: false,
        versions: crate::staging::deployed_versions(host.read, &db)?,
        base_revision: host.read.base_revision(&db)?,
        db,
        records,
        indexes,
        derived,
        drops,
        receipts: host.session.staged.take_receipts(),
        result,
        expected_revision,
    };
    let bytes = crate::patch::encode_patch(&patch)?;
    crate::patch::check_transaction_size(bytes.len())?;
    let value = crate::json::parse_value(&patch.result)?;
    Ok(PreparedPatch {
        patch: bytes,
        outcome: Ok(value),
    })
}

/// Persist a query's derived caches without touching the revision,
/// while the database still sits at the snapshot's revision and
/// `current` holds; otherwise the caches were computed over state a
/// later commit changed, and drop. Nothing staged opens no write.
/// Best-effort: cache loss only degrades to re-evaluation, so storage
/// failures here never fail the query itself.
fn commit_query_caches(store: &GroupStore, host: &mut Host, current: &dyn Fn() -> bool) {
    if !host.session.staged.has_caches() {
        return;
    }
    let outcome = (|| -> OpResult<()> {
        let db = host.session.db.as_slice();
        let write = store.begin_write()?;
        // Checked under the write lock: no commit can land between the
        // check and this transaction's own. Dropping it aborts.
        if write.meta_u64(db, meta::REVISION)? != host.read.meta_u64(db, meta::REVISION)?
            || !current()
        {
            return Ok(());
        }
        drain_caches(host, &write)?;
        write.commit()?;
        Ok(())
    })();
    let _ = outcome;
}

/// Persist a business failure's receipt without data or a revision
/// bump, so retries replay the failure instead of re-executing.
/// Best-effort: receipt loss only degrades to re-execution, so storage
/// failures here never mask the business failure itself.
fn commit_failure_receipt(store: &GroupStore, host: &Host, spec: &ReceiptSpec, failure: &Failure) {
    let outcome = (|| -> OpResult<()> {
        let (identity, encoded) = encode_failure_receipt(spec, failure)?;
        let write = store.begin_write()?;
        write.put_receipt(&host.session.db, &identity, &encoded)?;
        write.commit()?;
        Ok(())
    })();
    let _ = outcome;
}

/// Encode a failure receipt for the commit or the patch.
fn encode_failure_receipt(spec: &ReceiptSpec, failure: &Failure) -> OpResult<(Vec<u8>, Vec<u8>)> {
    let details = failure
        .details
        .clone()
        .map(|details| render(&details))
        .transpose()
        .map_err(|_| Error::corrupt("failure details do not render"))?;
    let receipt = Receipt {
        result: Some(Outcome::Failure(FailureData {
            code: failure.code.clone(),
            message: failure.message.clone(),
            details,
        })),
        fingerprint: spec.fingerprint,
        floor: spec.floor,
        expires_at: spec.expires_at,
        committed: None,
    };
    Ok((spec.identity.clone(), receipt.encode()?))
}

/// The revision after one more commit, failing loudly instead of
/// wrapping the stored counter.
pub(crate) fn next_revision(revision: u64) -> Result<u64, Error> {
    revision
        .checked_add(1)
        .ok_or_else(|| Error::Exhausted("database revisions".to_string()))
}

/// Commit a mutation: apply the staged journal, enforce foreign keys,
/// invalidate derived values and fire triggers to a fixpoint, then bump
/// the revision and commit. Cascade and trigger writes re-enter the loop;
/// past [`MAX_COMMIT_ROUNDS`] the commit aborts and nothing commits.
fn commit_mutation(
    store: &GroupStore,
    host: &mut Host,
    commit: CommitSpec,
    value: &Value,
) -> OpResult<()> {
    let write = store.begin_write()?;
    if let Some(spec) = commit.receipt.as_ref() {
        stage_success_receipt(host, spec, value)?;
    }
    rounds(host, Some(&write))?;
    let db = host.session.db.clone();
    let revision = write.meta_u64(&db, meta::REVISION)?;
    if let Some(expected) = commit.expected_revision
        && revision != expected
    {
        return Err(OpError::from(fail(
            "stale_revision",
            format!("expected revision {expected} but the database is at {revision}"),
        )));
    }
    write.set_meta_u64(&db, meta::REVISION, next_revision(revision)?)?;
    write.commit()?;
    Ok(())
}

/// Stage a success receipt for the commit or the patch.
fn stage_success_receipt(host: &mut Host, spec: &ReceiptSpec, value: &Value) -> OpResult<()> {
    let json = render(value).map_err(|_| {
        OpError::from(fail(
            codes::INVALID_VALUE,
            "mutation result is not storable",
        ))
    })?;
    let receipt = Receipt {
        result: Some(Outcome::Value(json)),
        fingerprint: spec.fingerprint,
        floor: spec.floor,
        expires_at: spec.expires_at,
        committed: None,
    };
    let encoded = receipt.encode()?;
    host.session
        .staged
        .stage_receipt(spec.identity.clone(), encoded);
    Ok(())
}

/// Run the commit rounds to a fixpoint: apply the journal slice,
/// enforce foreign keys, invalidate derived values and fire triggers
/// until the journal stabilizes, then drain the staged caches. Caches
/// stay staged until the fixpoint, so every round's invalidation sees
/// the edges of outcomes evaluated earlier in the session. With no
/// write transaction the rounds only stage, for patch extraction.
fn rounds(host: &mut Host, write: Option<&WriteTxn>) -> OpResult<()> {
    let mut applied = 0;
    let mut processed: HashSet<RecordChange> = HashSet::new();
    let mut processed_index: HashSet<IndexChange> = HashSet::new();
    let mut round: u32 = 0;
    loop {
        let len = host.session.staged.journal().len();
        if let Some(write) = write.filter(|_| len > applied) {
            apply_slice(host, write, applied, len)?;
        }
        let (touches, index_touches) = gather_slice(host, applied, len)?;
        applied = len;
        let fresh: Vec<RecordChange> = touches
            .into_iter()
            .filter(|change| processed.insert(change.clone()))
            .collect();
        let fresh_index: Vec<IndexChange> = index_touches
            .into_iter()
            .filter(|change| processed_index.insert(change.clone()))
            .collect();
        if fresh.is_empty() && fresh_index.is_empty() {
            break;
        }
        round += 1;
        if round > MAX_COMMIT_ROUNDS {
            return Err(OpError::from(fail(
                "cascade_too_deep",
                "commit cascade exceeded its round bound",
            )));
        }
        enforce_fk(host, &fresh)?;
        host.invalidate(&fresh, &fresh_index)?;
        host.retain_each(&fresh)?;
        fire_triggers(host, &fresh)?;
        run_deletion_hooks(host)?;
    }
    match write {
        Some(write) => drain_caches(host, write),
        None => Ok(()),
    }
}

/// Apply one journal slice to the write transaction, in order, with
/// this node's staged-index dual-write (see
/// [`crate::staging::StagedIndexes`]).
fn apply_slice(host: &Host, write: &WriteTxn, from: usize, to: usize) -> OpResult<()> {
    let db = host.session.db.as_slice();
    let staged = crate::staging::StagedIndexes::load(write, db)?;
    for op in &host.session.staged.journal()[from..to] {
        match op {
            StagedOp::Record {
                collection,
                key,
                value,
            } => {
                staged.maintain(write, db, *collection, key, value.as_deref())?;
                apply_record(write, db, *collection, key, value.as_deref())?;
            }
            StagedOp::Index {
                index,
                components,
                primary,
                present,
            } => apply_index(write, db, *index, components, primary, *present)?,
        }
    }
    Ok(())
}

/// Drain staged derived outcomes, evictions and receipts into the write
/// transaction.
fn drain_caches(host: &mut Host, write: &WriteTxn) -> OpResult<()> {
    let db = host.session.db.clone();
    for (key, outcome, observed) in host.session.staged.take_derived() {
        let (definition, args) = decode_derived_key(&db, &key)?;
        let outcome = decode_outcome(&outcome)?;
        install_derived(write, &db, definition, &args, &outcome, observed.as_deref())?;
    }
    for key in host.session.staged.take_drops() {
        let (definition, args) = decode_derived_key(&db, &key)?;
        write.delete_derived(&db, definition, &args)?;
    }
    for (identity, receipt) in host.session.staged.take_receipts() {
        write.put_receipt(&db, &identity, &receipt)?;
    }
    Ok(())
}

/// Collect one journal slice's touched records (deduplicated, with final
/// after-images from the overlay) and index deltas.
fn gather_slice(
    host: &Host,
    from: usize,
    to: usize,
) -> OpResult<(Vec<RecordChange>, Vec<IndexChange>)> {
    let mut keys: HashSet<(u32, Vec<u8>)> = HashSet::new();
    let mut index: HashSet<IndexChange> = HashSet::new();
    for op in &host.session.staged.journal()[from..to] {
        match op {
            StagedOp::Record {
                collection,
                key,
                value: _,
            } => {
                keys.insert((*collection, key.clone()));
            }
            StagedOp::Index {
                index: id,
                components,
                primary,
                present: _,
            } => {
                index.insert((*id, components.clone(), primary.clone()));
            }
        }
    }
    let mut records = Vec::with_capacity(keys.len());
    for (collection, key) in keys {
        let after = host
            .session
            .staged
            .staged_record(collection, &key)
            .ok_or_else(|| Error::corrupt("staged journal diverged from overlay"))?;
        records.push((collection, key, after));
    }
    Ok((records, index.into_iter().collect()))
}

/// Mid-execution fixpoint: invalidate derived values touched by staged
/// writes and fire triggers for them, looping over trigger-added writes
/// until the journal stabilizes. Fuel bounds the loop: every iteration
/// stages through trigger invocations that spend fuel.
pub(crate) fn settle(host: &mut Host) -> OpResult<()> {
    loop {
        let from = host.session.staged.settled();
        let len = host.session.staged.journal().len();
        if from >= len {
            break;
        }
        let (records, indexes) = gather_slice(host, from, len)?;
        host.session.staged.set_settled(len);
        host.invalidate(&records, &indexes)?;
        host.retain_each(&records)?;
        fire_triggers(host, &records)?;
        run_deletion_hooks(host)?;
    }
    Ok(())
}

/// Fire each trigger registered on a changed collection exactly once per
/// `(collection, key, after-image)`. Trigger bodies run as same- or
/// cross-module mutations under their own savepoint: a failing trigger
/// rolls its own writes back and fails the settle or commit with its
/// failure.
fn fire_triggers(host: &mut Host, records: &[RecordChange]) -> OpResult<()> {
    for (collection, key, after) in records {
        let name_bytes = host
            .read
            .intern_name(&host.session.db, *collection)?
            .ok_or_else(|| Error::corrupt("staged collection is not interned"))?;
        if name_bytes == crate::host::RAW_COLLECTION
            || name_bytes == crate::host::QUEUE_COLLECTION
            || name_bytes == crate::host::TIMER_COLLECTION
            || name_bytes == crate::host::TASK_COLLECTION
        {
            continue;
        }
        let (target_module, collection_name) = crate::host::split_qualified(&name_bytes)?;
        let triggers: Vec<String> = host
            .descriptors
            .get(&target_module)
            .map(|defs| {
                defs.triggers
                    .iter()
                    .filter(|trigger| trigger.source == collection_name)
                    .map(|trigger| trigger.name.clone())
                    .collect()
            })
            .unwrap_or_default();
        if triggers.is_empty() {
            continue;
        }
        // The change key travels as a decoded value, like scan and range
        // keys, so triggers never parse canonical text.
        let key_value = parse_value(key)?;
        let after_value = match after {
            Some(json) => parse_value(json)?,
            None => Value::Null,
        };
        // The before-image reads from the pinned snapshot, unaffected by
        // staged writes: triggers observe what changed, not just the end.
        let before_value = match host.read.get_record(&host.session.db, *collection, key)? {
            Some(json) => parse_value(&json)?,
            None => Value::Null,
        };
        // Once per change, for every trigger on its collection.
        let change = crate::session::FiredChange {
            collection: *collection,
            key: key.clone(),
            after: after.clone(),
        };
        if host.session.staged.fired(&change) {
            continue;
        }
        host.session.staged.mark_fired(change);
        for trigger in triggers {
            host.session.spend(FUEL_CALL)?;
            host.session.push_savepoint();
            let args = change_args(
                &collection_name,
                key_value.clone(),
                before_value.clone(),
                after_value.clone(),
            );
            // Triggers maintain the module's own invariants: they run with
            // definer rights, past access policies.
            host.session.enter_definer();
            let outcome = host.invoke_nested(CallKind::Mutation, &target_module, &trigger, &args);
            host.session.exit_definer();
            match outcome {
                Ok(Ok(_)) => host.session.pop_savepoint(false),
                Ok(Err(failure)) => {
                    host.session.pop_savepoint(true);
                    if failure.recovered {
                        return Err(OpError::from(failure.failure));
                    }
                    return Err(OpError::fatal(failure.failure));
                }
                Err(error) => {
                    host.session.pop_savepoint(true);
                    return Err(error);
                }
            }
        }
    }
    Ok(())
}

/// Hand every successful, non-permanent deletion staged since the last
/// call to the retention hooks of its collection: each runs as a
/// mutation of its module, with definer rights and under its own
/// savepoint, on `{collection, key, value, deleted_at}` with the whole
/// removed row, fields the deleting caller could not see included. A
/// failing hook fails the settle or commit with its failure.
fn run_deletion_hooks(host: &mut Host) -> OpResult<()> {
    for deletion in host.session.staged.take_deletions() {
        if deletion.permanent {
            continue;
        }
        let name_bytes = host
            .read
            .intern_name(&host.session.db, deletion.collection)?
            .ok_or_else(|| Error::corrupt("deleted collection is not interned"))?;
        let (module, collection) = crate::host::split_qualified(&name_bytes)?;
        let hooks: Vec<String> = host
            .descriptors
            .get(&module)
            .map(|defs| {
                defs.retention_hooks
                    .iter()
                    .filter(|hook| hook.source == collection)
                    .map(|hook| hook.name.clone())
                    .collect()
            })
            .unwrap_or_default();
        if hooks.is_empty() {
            continue;
        }
        let mut args = std::collections::BTreeMap::new();
        args.insert("collection".to_string(), Value::String(collection));
        args.insert("key".to_string(), parse_value(&deletion.key)?);
        args.insert("value".to_string(), parse_value(&deletion.value)?);
        args.insert(
            "deleted_at".to_string(),
            Value::Number(host.session.timestamp_ms as f64),
        );
        let args = Value::Object(args);
        for hook in hooks {
            host.session.spend(FUEL_CALL)?;
            host.session.push_savepoint();
            host.session.enter_definer();
            let outcome = host.invoke_nested(CallKind::Mutation, &module, &hook, &args);
            host.session.exit_definer();
            match outcome {
                Ok(Ok(_)) => host.session.pop_savepoint(false),
                Ok(Err(failure)) => {
                    host.session.pop_savepoint(true);
                    if failure.recovered {
                        return Err(OpError::from(failure.failure));
                    }
                    return Err(OpError::fatal(failure.failure));
                }
                Err(error) => {
                    host.session.pop_savepoint(true);
                    return Err(error);
                }
            }
        }
    }
    Ok(())
}

/// Trigger change arguments: `{collection, key, before, after}` with
/// absent images as null.
fn change_args(collection: &str, key: Value, before: Value, after: Value) -> Value {
    let mut map = std::collections::BTreeMap::new();
    map.insert(
        "collection".to_string(),
        Value::String(collection.to_string()),
    );
    map.insert("key".to_string(), key);
    map.insert("before".to_string(), before);
    map.insert("after".to_string(), after);
    Value::Object(map)
}

/// Enforce foreign keys for freshly touched rows: inserted and updated
/// rows must reference existing targets; deleted rows must release,
/// cascade or null their referencing rows.
fn enforce_fk(host: &mut Host, fresh: &[RecordChange]) -> OpResult<()> {
    for (collection, key, _) in fresh {
        let Some((target_module, def)) = fk_collection(host, *collection)? else {
            continue;
        };
        match host.read_record(*collection, key)? {
            Some(json) => check_outgoing(host, &target_module, &def, key, &json)?,
            None => check_incoming(host, &target_module, &def, key)?,
        }
    }
    Ok(())
}

/// Resolve a touched collection to its owning module plus descriptor,
/// skipping raw storage and unknown collections (which carry no keys).
fn fk_collection(host: &Host, collection: u32) -> OpResult<Option<(String, CollectionDef)>> {
    let name_bytes = host
        .read
        .intern_name(&host.session.db, collection)?
        .ok_or_else(|| Error::corrupt("staged collection is not interned"))?;
    if name_bytes == crate::host::RAW_COLLECTION
        || name_bytes == crate::host::QUEUE_COLLECTION
        || name_bytes == crate::host::TIMER_COLLECTION
        || name_bytes == crate::host::TASK_COLLECTION
    {
        return Ok(None);
    }
    let (module, name) = crate::host::split_qualified(&name_bytes)?;
    let def = host
        .descriptors
        .get(&module)
        .and_then(|defs| defs.collection(&name))
        .cloned();
    Ok(def.map(|def| (module, def)))
}

/// Check one inserted or updated row's outgoing references.
fn check_outgoing(
    host: &mut Host,
    module: &str,
    def: &CollectionDef,
    key: &[u8],
    json: &[u8],
) -> OpResult<()> {
    let row = parse_value(json)?;
    for reference in &def.references {
        let target = match &reference.discriminator {
            None => reference
                .targets
                .first()
                .ok_or_else(|| Error::corrupt("reference without targets"))?
                .clone(),
            Some(discriminator) => {
                let map = match &row {
                    Value::Object(map) => map,
                    _ => continue,
                };
                match map.get(discriminator) {
                    Some(Value::String(target)) if reference.targets.contains(target) => {
                        target.clone()
                    }
                    None | Some(Value::Null) => continue,
                    _ => {
                        return Err(OpError::from(fail(
                            "foreign_key_violation",
                            format!(
                                "row in `{}` has an invalid discriminator `{discriminator}`",
                                def.name
                            ),
                        )));
                    }
                }
            }
        };
        match &reference.place {
            PlaceDescriptor::Fields(fields) => {
                let map = match &row {
                    Value::Object(map) => map,
                    _ => continue,
                };
                let mut parts = Vec::with_capacity(fields.len());
                let mut skip = false;
                for field in fields {
                    match map.get(field) {
                        None | Some(Value::Null) => {
                            skip = true;
                            break;
                        }
                        Some(value) => parts.push(value.clone()),
                    }
                }
                if skip {
                    continue;
                }
                let needle = fk_needle(&parts)?;
                if !target_has(host, module, &target, &needle)? {
                    return Err(OpError::from(fail(
                        "foreign_key_violation",
                        format!("row in `{}` references missing `{target}`", def.name),
                    )));
                }
            }
            PlaceDescriptor::Key => {
                if !target_has(host, module, &target, key)? {
                    return Err(OpError::from(fail(
                        "foreign_key_violation",
                        format!("row in `{}` references missing `{target}`", def.name),
                    )));
                }
            }
            PlaceDescriptor::KeyPrefix => {
                if !target_has_prefix(host, module, &target, key)? {
                    return Err(OpError::from(fail(
                        "foreign_key_violation",
                        format!("row in `{}` references missing `{target}`", def.name),
                    )));
                }
            }
        }
    }
    Ok(())
}

/// Encode a field tuple as its match key: one field renders directly,
/// several render as their canonical array.
fn fk_needle(parts: &[Value]) -> OpResult<Vec<u8>> {
    if parts.len() == 1 {
        render(&parts[0]).map_err(|_| Error::corrupt("foreign key part does not render").into())
    } else {
        render(&Value::Array(parts.to_vec()))
            .map_err(|_| Error::corrupt("foreign key parts do not render").into())
    }
}

/// Whether a target collection holds one key, staged-or-committed.
fn target_has(host: &Host, module: &str, target: &str, key: &[u8]) -> OpResult<bool> {
    let qualified = crate::host::qualify_collection(module, target);
    let id = host
        .read
        .intern_id(&host.session.db, qualified.as_bytes())?;
    match id {
        None => Ok(false),
        Some(id) => Ok(host.read_record(id, key)?.is_some()),
    }
}

/// Whether any live target key structurally prefixes a row key. The
/// keys that can prefix a row key are few and known — the row key
/// itself, each leading slice of an array key and a non-array first
/// element — so the check is a handful of point reads, never a scan of
/// the target collection.
fn target_has_prefix(host: &Host, module: &str, target: &str, key: &[u8]) -> OpResult<bool> {
    let row_key = parse_value(key)?;
    for candidate in prefix_candidates(&row_key) {
        let candidate =
            render(&candidate).map_err(|_| Error::corrupt("key prefix does not render"))?;
        if target_has(host, module, target, &candidate)? {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Every key that [`match_prefix`] accepts as a prefix of `key`.
fn prefix_candidates(key: &Value) -> Vec<Value> {
    match key {
        Value::Array(items) => {
            let mut candidates: Vec<Value> = (0..=items.len())
                .map(|len| Value::Array(items[..len].to_vec()))
                .collect();
            if let Some(first) = items
                .first()
                .filter(|first| !matches!(first, Value::Array(_)))
            {
                candidates.push(first.clone());
            }
            candidates
        }
        other => vec![other.clone()],
    }
}

/// Live source keys a deleted target key structurally prefixes, read
/// through a bounded key-prefix scan: canonical array text starts with
/// the rendered leading elements, so only candidate rows are visited.
fn prefixed_keys(host: &Host, source: u32, deleted: &[u8]) -> OpResult<Vec<Vec<u8>>> {
    let target = parse_value(deleted)?;
    let element = |value: &Value| {
        render(value).map_err(|_| OpError::from(Error::corrupt("key prefix does not render")))
    };
    let mut text = b"[".to_vec();
    match &target {
        Value::Array(items) => {
            for item in items {
                text.extend_from_slice(&element(item)?);
                text.push(b',');
            }
            // An empty prefix matches every array; a full one matches
            // longer arrays here and the key itself below.
            if items.is_empty() {
                text.truncate(1);
            }
        }
        scalar => text.extend_from_slice(&element(scalar)?),
    }
    let db = host.session.db.as_slice();
    let mut live: BTreeMap<Vec<u8>, bool> = BTreeMap::new();
    for (key, _) in host
        .read
        .scan_records_with_key_prefix(db, source, &text, None, usize::MAX)?
    {
        live.insert(key, true);
    }
    if host.read.get_record(db, source, deleted)?.is_some() {
        live.insert(deleted.to_vec(), true);
    }
    for (key, value) in host.session.staged.staged_records_for(source) {
        live.insert(key, value.is_some());
    }
    let mut found = Vec::new();
    for (key, present) in live {
        if present && match_prefix(&target, &parse_value(&key)?) {
            found.push(key);
        }
    }
    Ok(found)
}

/// Structural prefix match: array prefixes match element-wise, a scalar
/// prefixes an array by first element, anything else matches exactly.
fn match_prefix(candidate: &Value, key: &Value) -> bool {
    match (candidate, key) {
        (Value::Array(prefix), Value::Array(full)) => {
            full.len() >= prefix.len() && full[..prefix.len()] == prefix[..]
        }
        (scalar, Value::Array(full)) => full.first() == Some(scalar),
        _ => candidate == key,
    }
}

/// Check one deleted row's incoming references: every referencing row
/// restricts, cascades or nulls.
fn check_incoming(host: &mut Host, module: &str, def: &CollectionDef, key: &[u8]) -> OpResult<()> {
    let collections: Vec<CollectionDef> = host
        .descriptors
        .get(module)
        .map(|defs| defs.collections.clone())
        .unwrap_or_default();
    for source in &collections {
        for reference in &source.references {
            if !reference.targets.contains(&def.name) {
                continue;
            }
            let referencing = find_referencing(host, module, source, reference, &def.name, key)?;
            for (source_id, source_key) in referencing {
                match reference.on_delete {
                    OnDelete::Restrict => {
                        return Err(OpError::from(fail(
                            "foreign_key_violation",
                            format!(
                                "row in `{}` is still referenced from `{}`",
                                def.name, source.name
                            ),
                        )));
                    }
                    OnDelete::Cascade => {
                        host.maintain_indexes(module, source, source_id, &source_key, None)?;
                        // A permanent delete cascades permanently.
                        let target = host.collection_id(module, &def.name)?;
                        let permanent = host.session.staged.deleted_permanently(target, key);
                        host.stage_deletion(source_id, source_key, permanent)?;
                    }
                    OnDelete::SetNull => {
                        set_null(host, module, source, reference, source_id, &source_key)?;
                    }
                }
            }
        }
    }
    Ok(())
}

/// Find the rows of one source collection referencing a deleted target
/// key through one reference.
fn find_referencing(
    host: &Host,
    module: &str,
    source: &CollectionDef,
    reference: &crate::descriptors::ReferenceDescriptor,
    target: &str,
    key: &[u8],
) -> OpResult<Vec<(u32, Vec<u8>)>> {
    let source_id = host.collection_id(module, &source.name)?;
    match &reference.place {
        PlaceDescriptor::Fields(fields) => {
            let resolved = resolve_referencing_index(
                source,
                reference.discriminator.as_deref(),
                fields,
                target,
                key,
            )?;
            let Some((index, prefix)) = resolved else {
                return Ok(Vec::new());
            };
            let qualified = crate::host::qualify_index(module, &source.name, &index.name);
            let Some(index_id) = host
                .read
                .intern_id(&host.session.db, qualified.as_bytes())?
            else {
                return Ok(Vec::new());
            };
            let mut found = Vec::new();
            for (components, primary) in
                merged_index_entries(host, index_id, index.fields.len(), &prefix)?
            {
                if components.len() < prefix.len() || components[..prefix.len()] != prefix[..] {
                    continue;
                }
                if poly_matches(host, source_id, &primary, reference, target)? {
                    found.push((source_id, primary));
                }
            }
            Ok(found)
        }
        PlaceDescriptor::Key => match host.read_record(source_id, key)? {
            Some(_) => Ok(vec![(source_id, key.to_vec())]),
            None => Ok(Vec::new()),
        },
        PlaceDescriptor::KeyPrefix => Ok(prefixed_keys(host, source_id, key)?
            .into_iter()
            .map(|candidate| (source_id, candidate))
            .collect()),
    }
}

/// First index whose leading fields cover a field reference, mirroring
/// the deploy-time validation rule.
fn covering_index<'a>(
    source: &'a CollectionDef,
    fields: &[String],
) -> Option<&'a crate::descriptors::IndexDescriptor> {
    source.indexes.iter().find(|index| {
        index.fields.len() >= fields.len() && index.fields[..fields.len()] == fields[..]
    })
}

/// First index whose leading fields are a discriminator plus a field
/// reference's key fields, mirroring deploy-time validation.
fn covering_poly_index<'a>(
    source: &'a CollectionDef,
    discriminator: &str,
    fields: &[String],
) -> Option<&'a crate::descriptors::IndexDescriptor> {
    source.indexes.iter().find(|index| {
        index.fields.len() > fields.len()
            && index.fields[0] == discriminator
            && index.fields[1..1 + fields.len()] == fields[..]
    })
}

/// A covering index plus the leading components a deleted target key
/// must match.
type Covering<'a> = (&'a crate::descriptors::IndexDescriptor, Vec<Vec<u8>>);

/// Resolve the covering index plus match prefix for a field reference:
/// poly references use their discriminator-leading index with the target
/// name prepended; anything else uses the fields-leading index, which is
/// also the fallback when a poly reference has no disc-leading index.
/// `None` matches nothing.
fn resolve_referencing_index<'a>(
    source: &'a CollectionDef,
    discriminator: Option<&str>,
    fields: &[String],
    target: &str,
    key: &[u8],
) -> OpResult<Option<Covering<'a>>> {
    let parts = fk_prefix(key, fields.len())?;
    let Some(parts) = parts else {
        return Ok(None);
    };
    if let Some(discriminator) = discriminator
        && let Some(index) = covering_poly_index(source, discriminator, fields)
    {
        let mut prefix = vec![
            render(&Value::String(target.to_string()))
                .map_err(|_| Error::corrupt("target name does not render"))?,
        ];
        prefix.extend(parts);
        return Ok(Some((index, prefix)));
    }
    Ok(covering_index(source, fields).map(|index| (index, parts)))
}

/// Decode a deleted target key into per-field match components: single
/// fields match the key bytes directly, several match the key's array
/// elements. `None` matches nothing.
fn fk_prefix(key: &[u8], fields: usize) -> OpResult<Option<Vec<Vec<u8>>>> {
    if fields == 1 {
        return Ok(Some(vec![key.to_vec()]));
    }
    let parsed = parse_value(key)?;
    match parsed {
        Value::Array(items) if items.len() == fields => {
            let mut prefix = Vec::with_capacity(fields);
            for item in &items {
                prefix.push(
                    render(item)
                        .map_err(|_| Error::corrupt("foreign key component does not render"))?,
                );
            }
            Ok(Some(prefix))
        }
        _ => Ok(None),
    }
}

/// Whether a candidate referencing row's discriminator selects the
/// deleted target. References without discriminators always match.
fn poly_matches(
    host: &Host,
    source: u32,
    key: &[u8],
    reference: &crate::descriptors::ReferenceDescriptor,
    target: &str,
) -> OpResult<bool> {
    let Some(discriminator) = reference.discriminator.as_ref() else {
        return Ok(true);
    };
    let row = host.read_record(source, key)?;
    let Some(json) = row else {
        return Ok(false);
    };
    match parse_value(&json)? {
        Value::Object(map) => {
            Ok(map.get(discriminator) == Some(&Value::String(target.to_string())))
        }
        _ => Ok(false),
    }
}

/// Merge committed index entries under a prefix with the staged overlay.
fn merged_index_entries(
    host: &Host,
    index: u32,
    arity: usize,
    prefix: &[Vec<u8>],
) -> OpResult<Vec<IndexEntry>> {
    let refs: Vec<&[u8]> = prefix.iter().map(Vec::as_slice).collect();
    let committed =
        host.read
            .scan_index(&host.session.db, index, arity, &refs, ScanRange::default())?;
    let mut merged: std::collections::BTreeMap<(Vec<Vec<u8>>, Vec<u8>), bool> =
        std::collections::BTreeMap::new();
    for (components, primary) in committed {
        merged.insert((components, primary), true);
    }
    for (components, primary, present) in host.session.staged.staged_index_prefix(index, prefix) {
        merged.insert((components, primary), present);
    }
    Ok(merged
        .into_iter()
        .filter(|(_, present)| *present)
        .map(|((components, primary), _)| (components, primary))
        .collect())
}

/// Null a referencing row's foreign-key fields plus its discriminator,
/// revalidating its schema: rows whose schema forbids null fail the
/// commit instead.
fn set_null(
    host: &mut Host,
    module: &str,
    source: &CollectionDef,
    reference: &crate::descriptors::ReferenceDescriptor,
    source_id: u32,
    source_key: &[u8],
) -> OpResult<()> {
    let fields = match &reference.place {
        PlaceDescriptor::Fields(fields) => fields,
        _ => {
            return Err(OpError::from(fail(
                "reference_misuse",
                "set_null needs a field reference",
            )));
        }
    };
    let old = host
        .read_record(source_id, source_key)?
        .ok_or_else(|| Error::corrupt("referencing row vanished"))?;
    let mut map = match parse_value(&old)? {
        Value::Object(map) => map,
        _ => return Err(Error::corrupt("referencing row is not an object").into()),
    };
    for field in fields {
        map.insert(field.clone(), Value::Null);
    }
    if let Some(discriminator) = reference.discriminator.as_ref() {
        map.insert(discriminator.clone(), Value::Null);
    }
    let nulled = Value::Object(map);
    if let Err(error) = source.value_schema.parse(&nulled) {
        return Err(OpError::from(fail(
            "foreign_key_violation",
            format!(
                "row in `{}` cannot null its reference at {}: {}",
                source.name, error.path, error.message
            ),
        )));
    }
    let json = render(&nulled).map_err(|_| Error::corrupt("nulled row does not render"))?;
    host.maintain_indexes(module, source, source_id, source_key, Some(&json))?;
    host.session
        .staged
        .stage_record(source_id, source_key.to_vec(), Some(json));
    Ok(())
}

/// Display name of a top-level call kind.
fn kind_name(kind: CallKind) -> &'static str {
    match kind {
        CallKind::Query => "query",
        CallKind::Mutation => "mutation",
        CallKind::Derived => "derived",
        CallKind::Reduce => "reducer",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefixes_match_structurally() {
        let array = |items: Vec<Value>| Value::Array(items);
        // Array prefixes match element-wise.
        assert!(match_prefix(
            &array(vec![Value::String("t".to_string())]),
            &array(vec![Value::String("t".to_string()), Value::Number(1.0)]),
        ));
        assert!(!match_prefix(
            &array(vec![Value::String("t".to_string()), Value::Number(2.0)]),
            &array(vec![Value::String("t".to_string())]),
        ));
        // Scalars prefix arrays by first element.
        assert!(match_prefix(
            &Value::String("t".to_string()),
            &array(vec![Value::String("t".to_string()), Value::Number(1.0)]),
        ));
        assert!(!match_prefix(
            &Value::String("x".to_string()),
            &array(vec![Value::String("t".to_string())]),
        ));
        // Anything else matches exactly.
        assert!(match_prefix(
            &Value::String("t".to_string()),
            &Value::String("t".to_string())
        ));
        assert!(!match_prefix(
            &Value::String("t".to_string()),
            &Value::String("x".to_string())
        ));
    }

    #[test]
    fn fk_needles_encode_field_tuples() {
        let single = fk_needle(&[Value::String("p1".to_string())]).unwrap();
        assert_eq!(single, b"\"p1\"".to_vec());
        let pair = fk_needle(&[Value::String("a".to_string()), Value::Number(1.0)]).unwrap();
        assert_eq!(pair, b"[\"a\",1]".to_vec());
    }

    #[test]
    fn fk_prefixes_decode_deleted_keys() {
        let single = fk_prefix(b"\"p1\"", 1).unwrap().unwrap();
        assert_eq!(single, vec![b"\"p1\"".to_vec()]);
        let pair = fk_prefix(b"[\"a\",1]", 2).unwrap().unwrap();
        assert_eq!(pair, vec![b"\"a\"".to_vec(), b"1".to_vec()]);
        assert!(fk_prefix(b"[\"a\"]", 2).unwrap().is_none());
        assert!(fk_prefix(b"\"p1\"", 2).unwrap().is_none());
    }

    #[test]
    fn prefix_candidates_are_exactly_the_matching_keys() {
        let t = || Value::String("t".to_string());
        let key = Value::Array(vec![t(), Value::Number(1.0), Value::Number(2.0)]);
        let candidates = prefix_candidates(&key);
        assert_eq!(candidates.len(), 5);
        for candidate in &candidates {
            assert!(match_prefix(candidate, &key), "{candidate:?}");
        }
        assert!(candidates.contains(&t()));
        assert!(candidates.contains(&Value::Array(Vec::new())));
        // Scalar keys are only prefixed by themselves.
        assert_eq!(prefix_candidates(&t()), vec![t()]);
        // Nested arrays never act as scalar first elements.
        let nested = Value::Array(vec![Value::Array(vec![t()])]);
        assert_eq!(prefix_candidates(&nested).len(), 2);
    }
}
