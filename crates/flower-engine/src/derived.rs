//! Derived evaluation: cached reads, edges and invalidation.
//!
//! [`Host::get_or_evaluate`] serves `get_derived`: staged outcomes first,
//! then the committed cache, then fresh evaluation. Evaluations run on the
//! real host with a private dependency scope, read-only, derived-kind and
//! nesting-capped, so nested derived reads cache and edge transitively.
//! Outcomes plus their observed edges stage for commit; edge replacement
//! itself happens in the write transaction.
//!
//! [`Host::invalidate`] drops everything a batch of changes touches: exact
//! record dependents, collection-range dependents whose bounds match the
//! changed keys, index-range dependents whose bounds match the changed
//! entries, then the transitive closure through derived-dependency edges.
//! Edges come from the committed table plus the outcomes staged earlier
//! in the same session, so a value evaluated before a write in the same
//! invocation drops too. Dropped pinned roots re-evaluate eagerly so
//! commits never leave pinned values stale; unpinned instances
//! re-evaluate lazily on next read. Outcomes carry what their
//! evaluation saw of the clock ([`OutcomeTime`]), the values it read
//! included: one that polled serves reads at that instant alone, one
//! that declared a change serves until it, and readers inherit both,
//! so a value that reads the clock evaluates again once time moves on,
//! as v1's cells over their advancing `clock` record. Where no guest
//! code can run (replicated applies, activations), [`invalidate_committed`]
//! and [`invalidate_module_readers`] drop committed outcomes instead and
//! keep pins as invalidated outcomes that re-evaluate on their next read. A module's own outcomes go stale
//! when it activates through its code era instead (see
//! [`crate::staging::code_era`]).
//! Range matching scans the whole edge table once per batch and tests
//! bounds in memory: invalidation cost scales with dependent count, and a
//! future per-index edge prefix can narrow the scan without changing the
//! contract.
//!
//! Aggregates (derived definitions with an `aggregate` member) are not
//! rebuilt when invalidation reaches a cached value of theirs: the value
//! is captured before the drop, and once the closure is dropped the
//! group's row changes since the value was computed (the journal past
//! its basis, see [`crate::session::Staged::stage_derived_at`]) go to
//! the reducer, which runs without a context; its answer is staged with
//! the value's edges. A reducer that cannot maintain the value leaves it
//! dropped, to build from its rows.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use flower_sdk::error::codes;
use flower_sdk::{Failure, Value, fail};

use crate::descriptors::DefKind;
use crate::error::Error;
use crate::host::{FUEL_CALL, Host, OpError, OpResult};
use crate::json::{parse_value, render};
use crate::runtime::{CallKind, HostCall, Runtime};
use crate::session::{CallerKind, StagedOp, Timing};
use crate::storage::codec::{
    DerivedOutcome, OutcomeBody, OutcomeTime, decode_outcome, encode_outcome,
};
use crate::storage::keys::{
    DepKey, collection_range_matches, decode_dep_key, decode_derived_key, encode_derived_dep,
    encode_derived_key, encode_record_dep, encode_record_key, index_range_matches,
};
use crate::storage::{DepEdge, WriteTxn};

/// Deepest derived evaluation nesting.
pub const MAX_DERIVED_DEPTH: usize = 16;
/// Fuel charged per derived-instance evaluation.
pub const FUEL_EVAL: u64 = 10;

/// One changed index entry for range invalidation: index, components,
/// primary.
pub type IndexChange = (u32, Vec<Vec<u8>>, Vec<u8>);

/// One changed record for invalidation: collection, key, after-image.
pub type RecordChange = (u32, Vec<u8>, Option<Vec<u8>>);

impl Host<'_> {
    /// Serve a derived read: staged outcome, committed cache, or fresh
    /// evaluation. Unknown names and non-derived kinds are deterministic
    /// guest failures; cycles fail the same way.
    pub(crate) fn get_or_evaluate(
        &mut self,
        module: &str,
        name: &str,
        args: &Value,
    ) -> OpResult<Value> {
        let kind = self
            .descriptors
            .get(module)
            .and_then(|defs| defs.def_kinds.get(name))
            .copied();
        if kind != Some(DefKind::Derived) {
            return Err(OpError::from(fail(
                codes::UNKNOWN_METHOD,
                format!("unknown derived `{name}`"),
            )));
        }
        self.check_derived_args(module, name, args)?;
        let definition = self.definition_id(module, name)?;
        let args_json = render(args).map_err(|_| {
            OpError::from(fail(codes::INVALID_VALUE, "derived args are not storable"))
        })?;
        let key = encode_derived_key(&self.session.db, definition, &args_json)?;
        let cached = if self.session.staged.derived_dropped(&key) {
            // A staged eviction hides the committed outcome below it.
            None
        } else if let Some(staged) = self.session.staged.staged_derived(&key) {
            Some(decode_outcome(staged)?)
        } else {
            self.read
                .get_derived(&self.session.db, definition, &args_json)?
        };
        // Outcomes computed before the module's last activation
        // re-evaluate instead of serving: every activation mints the
        // module a new era, even for code it ran before, and the stale
        // outcome's pin survives through re-evaluation. Outcomes
        // invalidated where re-evaluation could not run do the same, and
        // so do outcomes whose time lapsed at this read's clock (they
        // polled it at another instant, or declared a change since).
        // A versioned aggregate's values carry its lineage instead.
        let current = self.outcome_code(module, name)?;
        let now = self.session.timestamp_ms;
        let cached = cached.filter(|outcome| outcome.code == current && outcome.serves_at(now));
        match cached {
            Some(outcome) => self.serve_outcome(definition, &args_json, &outcome),
            None => self.evaluate_and_cache(module, name, definition, &args_json, args),
        }
    }

    /// The identity the module's outcomes compute under: its code era
    /// (see [`crate::staging::code_era`]), zeros when no activation
    /// minted one yet; the target's code identity for a module
    /// evaluating under a staged graph.
    fn active_code(&self, module: &str) -> OpResult<[u8; crate::storage::keys::HASH_LEN]> {
        if let Some(graph) = &self.session.graph
            && graph.module == module
        {
            return Ok(graph.identity);
        }
        self.code_at(&crate::staging::code_era_key(module))
    }

    /// The code identity a meta row holds, zeros when absent.
    fn code_at(&self, key: &[u8]) -> OpResult<[u8; crate::storage::keys::HASH_LEN]> {
        match self.read.meta(&self.session.db, key)? {
            None => Ok([0u8; crate::storage::keys::HASH_LEN]),
            Some(bytes) => {
                let code: [u8; crate::storage::keys::HASH_LEN] = bytes
                    .try_into()
                    .map_err(|_| Error::corrupt("code identity has the wrong length"))?;
                Ok(code)
            }
        }
    }

    /// The code identity `name`'s outcomes compute under: the module's
    /// active code, or for an aggregate declaring a version, its
    /// lineage (see [`crate::staging::aggregate_lineage_key`]), which
    /// deployments keeping the version leave alone.
    fn outcome_code(
        &self,
        module: &str,
        name: &str,
    ) -> OpResult<[u8; crate::storage::keys::HASH_LEN]> {
        let versioned = self
            .descriptors
            .get(module)
            .and_then(|defs| defs.aggregates.get(name))
            .is_some_and(|aggregate| aggregate.version.is_some());
        if versioned {
            let lineage = self.code_at(&crate::staging::aggregate_lineage_key(module, name))?;
            if lineage != [0u8; crate::storage::keys::HASH_LEN] {
                return Ok(lineage);
            }
        }
        self.active_code(module)
    }

    /// Serve a cached outcome: values decode, error markers replay as the
    /// stored business failure. Both record a derived-instance dependency,
    /// and the reader inherits what the outcome saw of the clock.
    fn serve_outcome(
        &mut self,
        definition: u32,
        args: &[u8],
        outcome: &DerivedOutcome,
    ) -> OpResult<Value> {
        let dep = encode_derived_dep(definition, args)?;
        self.session.record_dep(dep);
        self.session.inherit(Timing::of(outcome.time));
        match &outcome.body {
            OutcomeBody::Value(json) => Ok(parse_value(json)?),
            OutcomeBody::Error(json) => Err(OpError::from(stored_failure(json)?)),
            OutcomeBody::Invalid => Err(Error::corrupt("an invalidated outcome was served").into()),
        }
    }

    /// Evaluate a derived instance and stage its outcome. Nested derived
    /// reads recurse through the same path, so values cache and edge
    /// transitively. Fresh outcomes keep the current pin flag: invalidation
    /// drops pinned roots and re-evaluates them without unpinning.
    fn evaluate_and_cache(
        &mut self,
        module: &str,
        name: &str,
        definition: u32,
        args_json: &[u8],
        args: &Value,
    ) -> OpResult<Value> {
        // Nested evaluations re-enter through host dispatch, so the depth
        // cap reads the session's in-flight count instead of threading a
        // counter through the re-entrant call.
        if self.session.eval_depth() >= MAX_DERIVED_DEPTH {
            return Err(OpError::from(fail(
                codes::INVALID_INVOCATION,
                "derived evaluation is too deep",
            )));
        }
        if !self.session.begin_eval(definition, args_json) {
            return Err(OpError::from(fail(
                codes::INVALID_INVOCATION,
                format!("derived cycle at `{name}`"),
            )));
        }
        self.session.spend(FUEL_CALL)?;
        self.session.spend(FUEL_EVAL)?;
        self.session.push_scope();
        // Evaluations run derived-kind and read-only. The flags swap
        // without holding a borrow: the re-entrant guest call needs the
        // whole host.
        let saved_kind = std::mem::replace(&mut self.session.kind, CallerKind::Derived);
        let saved_writable = self.session.replace_writable(false);
        // The value computes as its own module: `""` names its module's
        // collections, whichever module reads it.
        let saved_module = std::mem::replace(&mut self.session.module, module.to_string());
        let outcome = self.invoke_runtime(CallKind::Derived, module, name, args);
        self.session.module = saved_module;
        self.session.kind = saved_kind;
        self.session.replace_writable(saved_writable);
        let (observed, timing) = self.session.pop_scope();
        self.session.end_eval(definition, args_json);
        // The outcome serves while what it saw of the clock holds, and
        // its reader changes when it does.
        let time = timing.outcome_time(self.session.timestamp_ms);
        self.session.inherit(timing);
        let retained = self.always_retained(module, name, args)?;
        let pinned = self.current_pin(definition, args_json)? || retained;
        let code = self.outcome_code(module, name)?;
        let key = encode_derived_key(&self.session.db, definition, args_json)?;
        match outcome? {
            Ok(value) => {
                let json = render(&value).map_err(|_| {
                    OpError::from(fail(codes::INVALID_VALUE, "derived value is not storable"))
                })?;
                let outcome = DerivedOutcome {
                    pinned,
                    code,
                    time,
                    body: OutcomeBody::Value(json),
                };
                self.session
                    .staged
                    .stage_derived(key, encode_outcome(&outcome), Some(observed));
                let dep = encode_derived_dep(definition, args_json)?;
                self.session.record_dep(dep);
                Ok(value)
            }
            Err(failure) => {
                if !failure.recovered {
                    return Err(OpError::fatal(failure.failure));
                }
                // Business failures cache as error markers over the old
                // edges plus the newly observed ones, so changes to either
                // can repair the instance.
                let json = render(&failure_value(&failure.failure)).map_err(|_| {
                    OpError::from(fail(
                        codes::INVALID_VALUE,
                        "derived failure is not storable",
                    ))
                })?;
                let outcome = DerivedOutcome {
                    pinned,
                    code,
                    time,
                    body: OutcomeBody::Error(json),
                };
                let mut union: HashSet<Vec<u8>> = observed.into_iter().collect();
                for edge in self
                    .read
                    .dependent_edges(&self.session.db, definition, args_json)?
                {
                    union.insert(edge);
                }
                let mut union: Vec<Vec<u8>> = union.into_iter().collect();
                union.sort();
                self.session
                    .staged
                    .stage_derived(key, encode_outcome(&outcome), Some(union));
                Err(OpError::from(failure.failure))
            }
        }
    }

    /// Whether a derived definition pins this evaluation: every one for
    /// `Always`, and for `Each` the one whose `{"key": …}` names a row
    /// its source holds, so rows that predate the deployment keep their
    /// root from its first evaluation on.
    fn always_retained(&self, module: &str, name: &str, args: &Value) -> OpResult<bool> {
        let Some(defs) = self.descriptors.get(module) else {
            return Ok(false);
        };
        match defs.retention(name) {
            crate::descriptors::MaterializePolicy::Always => Ok(true),
            crate::descriptors::MaterializePolicy::Each => {
                let (Some(source), Some(key)) = (
                    defs.each.get(name),
                    args.as_object().and_then(|map| map.get("key")),
                ) else {
                    return Ok(false);
                };
                let Ok(key) = render(key) else {
                    return Ok(false);
                };
                let collection = self.collection_id(module, source)?;
                Ok(self.read_record(collection, &key)?.is_some())
            }
            crate::descriptors::MaterializePolicy::Never => Ok(false),
        }
    }

    /// Keep `Each` roots in step with their source rows: an inserted row
    /// pins its root, a deleted one unpins it. The before-image comes
    /// from the pinned snapshot, as for triggers.
    pub(crate) fn retain_each(&mut self, records: &[RecordChange]) -> OpResult<()> {
        for (collection, key, after) in records {
            let name_bytes = self
                .read
                .intern_name(&self.session.db, *collection)?
                .ok_or_else(|| crate::error::Error::corrupt("staged collection is not interned"))?;
            let Ok((module, source)) = crate::host::split_qualified(&name_bytes) else {
                continue;
            };
            let roots: Vec<String> = self
                .descriptors
                .get(&module)
                .map(|defs| {
                    let mut roots: Vec<String> = defs
                        .each
                        .iter()
                        .filter(|(_, each)| **each == source)
                        .map(|(name, _)| name.clone())
                        .collect();
                    roots.sort();
                    roots
                })
                .unwrap_or_default();
            if roots.is_empty() {
                continue;
            }
            let existed = self
                .read
                .get_record(&self.session.db, *collection, key)?
                .is_some();
            if existed == after.is_some() {
                continue;
            }
            let mut args = std::collections::BTreeMap::new();
            args.insert("key".to_string(), parse_value(key)?);
            let args = Value::Object(args);
            for name in roots {
                if after.is_some() {
                    self.materialize(&module, &name, &args)?;
                } else {
                    self.unmaterialize(&module, &name, &args)?;
                }
            }
        }
        Ok(())
    }

    /// Validate derived arguments against the definition's schema before
    /// serving or evaluating anything.
    fn check_derived_args(&self, module: &str, name: &str, args: &Value) -> OpResult<()> {
        if let Some(schema) = self
            .descriptors
            .get(module)
            .and_then(|defs| defs.args_schema(name))
            && let Err(error) = schema.parse(args)
        {
            return Err(OpError::from(
                error.invalid_argument(&format!("arguments for `{name}`")),
            ));
        }
        Ok(())
    }

    /// Pin flag a fresh evaluation keeps: staged outcomes shadow committed
    /// ones; uncached instances start unpinned.
    fn current_pin(&self, definition: u32, args: &[u8]) -> OpResult<bool> {
        let key = encode_derived_key(&self.session.db, definition, args)?;
        if let Some(staged) = self.session.staged.staged_derived(&key) {
            return Ok(decode_outcome(staged)?.pinned);
        }
        match self.read.get_derived(&self.session.db, definition, args)? {
            Some(outcome) => Ok(outcome.pinned),
            None => Ok(false),
        }
    }

    /// Resolve a definition name to its interned ID: its staged twin's
    /// for a module evaluating under a staged graph.
    fn definition_id(&self, module: &str, name: &str) -> OpResult<u32> {
        if let Some(graph) = &self.session.graph
            && graph.module == module
        {
            return graph.staged.get(name).copied().ok_or_else(|| {
                OpError::from(fail(
                    codes::INVALID_VALUE,
                    format!("unknown definition `{name}`"),
                ))
            });
        }
        let qualified = crate::host::qualify_definition(module, name);
        self.read
            .intern_id(&self.session.db, qualified.as_bytes())?
            .ok_or_else(|| {
                OpError::from(fail(
                    codes::INVALID_VALUE,
                    format!("unknown definition `{name}`"),
                ))
            })
    }

    /// Pin a derived root: evaluate when uncached, then mark pinned so it
    /// never evicts and commits never leave it stale.
    pub(crate) fn materialize(&mut self, module: &str, name: &str, args: &Value) -> OpResult<()> {
        let definition = self.derived_definition(module, name)?;
        self.check_derived_args(module, name, args)?;
        let args_json = render(args).map_err(|_| {
            OpError::from(fail(codes::INVALID_VALUE, "derived args are not storable"))
        })?;
        let key = encode_derived_key(&self.session.db, definition, &args_json)?;
        let cached = !self.session.staged.derived_dropped(&key)
            && (self.session.staged.staged_derived(&key).is_some()
                || self
                    .read
                    .get_derived(&self.session.db, definition, &args_json)?
                    .is_some());
        if !cached {
            self.get_or_evaluate(module, name, args)?;
        }
        self.set_pinned(&key, definition, &args_json, true)
    }

    /// Unpin a derived instance. The cached value stays until invalidation
    /// or cache pressure evicts it; only the pin flag clears.
    pub(crate) fn unmaterialize(&mut self, module: &str, name: &str, args: &Value) -> OpResult<()> {
        let definition = self.derived_definition(module, name)?;
        self.check_derived_args(module, name, args)?;
        let args_json = render(args).map_err(|_| {
            OpError::from(fail(codes::INVALID_VALUE, "derived args are not storable"))
        })?;
        let key = encode_derived_key(&self.session.db, definition, &args_json)?;
        self.set_pinned(&key, definition, &args_json, false)
    }

    /// Resolve a derived definition, rejecting unknown and non-derived
    /// names as deterministic guest failures.
    fn derived_definition(&self, module: &str, name: &str) -> OpResult<u32> {
        let kind = self
            .descriptors
            .get(module)
            .and_then(|defs| defs.def_kinds.get(name))
            .copied();
        if kind != Some(DefKind::Derived) {
            return Err(OpError::from(fail(
                codes::UNKNOWN_METHOD,
                format!("unknown derived `{name}`"),
            )));
        }
        self.definition_id(module, name)
    }

    /// Rewrite one outcome's pin flag: restage staged outcomes, stage a
    /// pin-only outcome over committed ones (retaining their edges), and
    /// no-op when nothing is cached.
    fn set_pinned(
        &mut self,
        key: &[u8],
        definition: u32,
        args: &[u8],
        pinned: bool,
    ) -> OpResult<()> {
        if let Some(staged) = self.session.staged.staged_derived(key) {
            let mut outcome = decode_outcome(staged)?;
            outcome.pinned = pinned;
            self.session
                .staged
                .restage_derived(key, encode_outcome(&outcome));
            return Ok(());
        }
        if let Some(mut outcome) = self.read.get_derived(&self.session.db, definition, args)? {
            outcome.pinned = pinned;
            // The committed outcome reflects none of this session's
            // writes.
            self.session
                .staged
                .stage_derived_at(key.to_vec(), encode_outcome(&outcome), None, 0);
        }
        Ok(())
    }

    /// Drop everything a batch of changes invalidates, then re-evaluate
    /// dropped pinned roots eagerly so they never read stale. Eager
    /// business failures stay cached as error markers; traps, budget
    /// exhaustion and storage failures abort the batch.
    pub(crate) fn invalidate(
        &mut self,
        records: &[RecordChange],
        indexes: &[IndexChange],
    ) -> OpResult<()> {
        let mut dropped = HashSet::new();
        let mut queue = VecDeque::new();
        for (collection, key, _) in records {
            let dep = encode_record_dep(*collection, key)?;
            queue.extend(self.dependents_of(&dep)?);
        }
        for dependent in self.range_dependents(records, indexes)? {
            queue.push_back(dependent);
        }
        // Phase one drops the whole closure before anything re-evaluates:
        // eager re-evaluation inside the drop loop could read transitive
        // dependents that are queued but not yet dropped, caching values
        // computed from stale inputs.
        let aggregates = self
            .descriptors
            .values()
            .any(|defs| !defs.aggregates.is_empty());
        let mut captured = Vec::new();
        let mut pinned_roots = Vec::new();
        while let Some((definition, args)) = queue.pop_front() {
            if !dropped.insert((definition, args.clone())) {
                continue;
            }
            // Committed pins persist across drops until unmaterialize: a
            // staged drop never unpins the outcome below it. Pins collect
            // before the drop clears staged outcomes.
            let key = encode_derived_key(&self.session.db, definition, &args)?;
            let pinned = match self.session.staged.staged_derived(&key) {
                Some(staged) => decode_outcome(staged)?.pinned,
                None => self
                    .read
                    .get_derived(&self.session.db, definition, &args)?
                    .map(|outcome| outcome.pinned)
                    .unwrap_or(false),
            };
            // A cached aggregate keeps its accumulator to take its
            // group's row changes below, before anything re-evaluates.
            if aggregates && let Some(cached) = self.capture_aggregate(definition, &args, &key)? {
                captured.push(cached);
            }
            self.session.staged.drop_derived(key);
            let dep = encode_derived_dep(definition, &args)?;
            if pinned {
                pinned_roots.push((definition, args));
            }
            queue.extend(self.dependents_of(&dep)?);
        }
        // Aggregates go on from their accumulators: the group's row
        // changes fold in through the reducer, which reads nothing, so
        // the order against the drops above does not matter. One the
        // reducer cannot maintain stays dropped and builds from its rows
        // (below when pinned, else on its next read).
        for cached in captured {
            self.maintain_aggregate(cached)?;
        }
        // Phase two re-evaluates dropped pinned roots through the normal
        // read path, so nested reads hit staged-or-fresh values and roots
        // already evaluated as nested dependencies serve their cache.
        for (definition, args) in pinned_roots {
            let (module, name) = self.definition_name(definition)?;
            let args_value = parse_value(&args)?;
            match self.get_or_evaluate(&module, &name, &args_value) {
                Ok(_) => {}
                Err(error) if !error.fatal => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    /// Dependents of one dependency: committed edges plus the edges of
    /// outcomes staged in this session, which reach the edge table only
    /// at commit. An outcome evaluated earlier in the same session must
    /// drop when a later write touches what it read.
    fn dependents_of(&self, dep: &[u8]) -> OpResult<Vec<(u32, Vec<u8>)>> {
        let mut dependents = self.read.dependents(&self.session.db, dep)?;
        for key in self.session.staged.staged_dependents(dep) {
            dependents.push(decode_derived_key(&self.session.db, &key)?);
        }
        Ok(dependents)
    }

    /// Dependents whose range bounds match the changed keys or entries,
    /// resolved over one edge-table scan plus the staged edges.
    fn range_dependents(
        &self,
        records: &[RecordChange],
        indexes: &[IndexChange],
    ) -> OpResult<Vec<(u32, Vec<u8>)>> {
        if records.is_empty() && indexes.is_empty() {
            return Ok(Vec::new());
        }
        let mut edges = self.read.scan_dep_edges(&self.session.db)?;
        for (dep, key) in self.session.staged.staged_edges() {
            edges.push((dep, decode_derived_key(&self.session.db, &key)?));
        }
        Ok(range_matches(&self.session.db, &edges, records, indexes)?)
    }

    /// Split a derived definition ID back into module and definition names.
    fn definition_name(&self, definition: u32) -> OpResult<(String, String)> {
        let qualified = self
            .read
            .intern_name(&self.session.db, definition)?
            .ok_or_else(|| Error::corrupt("derived definition name is not interned"))?;
        crate::host::split_qualified(&qualified).map_err(OpError::from)
    }

    /// An aggregate instance's cached value, about to drop, with what
    /// its reducer needs to go on from it: `None` for anything else,
    /// for an instance with no servable value (none cached, a stored
    /// failure, an invalidated pin, older code, a time that lapsed at
    /// this clock) and for arguments that name no group of the
    /// aggregate's index.
    fn capture_aggregate(
        &self,
        definition: u32,
        args: &[u8],
        key: &[u8],
    ) -> OpResult<Option<CachedAggregate>> {
        // An earlier drop in this session hides the committed outcome.
        if self.session.staged.derived_dropped(key) {
            return Ok(None);
        }
        let (module, name) = self.definition_name(definition)?;
        let Some(aggregate) = self
            .descriptors
            .get(&module)
            .and_then(|defs| defs.aggregates.get(&name))
        else {
            return Ok(None);
        };
        let (outcome, basis, edges) = match self.session.staged.staged_derived(key) {
            Some(staged) => {
                let edges = match self.session.staged.staged_observed(key) {
                    Some(edges) => edges.to_vec(),
                    None => self
                        .read
                        .dependent_edges(&self.session.db, definition, args)?,
                };
                let basis = self.session.staged.staged_basis(key).unwrap_or(0);
                (decode_outcome(staged)?, basis, edges)
            }
            None => match self.read.get_derived(&self.session.db, definition, args)? {
                Some(outcome) => {
                    let edges = self
                        .read
                        .dependent_edges(&self.session.db, definition, args)?;
                    (outcome, 0, edges)
                }
                None => return Ok(None),
            },
        };
        let OutcomeBody::Value(json) = &outcome.body else {
            return Ok(None);
        };
        if outcome.code != self.outcome_code(&module, &name)?
            || !outcome.serves_at(self.session.timestamp_ms)
        {
            return Ok(None);
        }
        let Some(fields) = self
            .descriptors
            .get(&module)
            .and_then(|defs| defs.collection(&aggregate.collection))
            .and_then(|collection| {
                collection
                    .indexes
                    .iter()
                    .find(|index| index.name == aggregate.index)
            })
            .map(|index| index.fields.clone())
        else {
            return Ok(None);
        };
        // `{"group": [<one value per indexed field>]}`: the components
        // a row's index entry carries when it belongs to the group.
        let Value::Object(mut arguments) = parse_value(args)? else {
            return Ok(None);
        };
        let Some(group) = arguments.remove("group") else {
            return Ok(None);
        };
        let Value::Array(values) = &group else {
            return Ok(None);
        };
        if values.len() != fields.len() {
            return Ok(None);
        }
        let mut components = Vec::with_capacity(values.len());
        for value in values {
            match render(value) {
                Ok(component) => components.push(component),
                Err(_) => return Ok(None),
            }
        }
        let collection = self.collection_id(&module, &aggregate.collection)?;
        Ok(Some(CachedAggregate {
            key: key.to_vec(),
            module,
            name,
            accumulator: parse_value(json)?,
            group,
            components,
            collection,
            fields,
            basis,
            edges,
            pinned: outcome.pinned,
            code: outcome.code,
            time: outcome.time,
        }))
    }

    /// Fold a captured aggregate's group changes into its accumulator
    /// and stage the result with the value's edges: the rows of the
    /// source written past the value's basis whose image left or
    /// entered the group, each as `{key, old?, new?}` in key order. No
    /// change restages the value as it was. The reducer runs without a
    /// context; a reducer asking to rebuild, or failing, leaves the
    /// value dropped, and one reaching for the context fails the
    /// invocation, as v1's `AGGREGATE_CONTEXT_FORBIDDEN`.
    fn maintain_aggregate(&mut self, cached: CachedAggregate) -> OpResult<()> {
        let length = self.session.staged.journal().len();
        let mut written = std::collections::BTreeSet::new();
        for op in &self.session.staged.journal()[cached.basis.min(length)..] {
            if let StagedOp::Record {
                collection, key, ..
            } = op
                && *collection == cached.collection
            {
                written.insert(key.clone());
            }
        }
        let mut changes = Vec::new();
        for key in written {
            let old = self.image_at(cached.collection, &key, cached.basis)?;
            let new = match self.session.staged.staged_record(cached.collection, &key) {
                Some(image) => image,
                None => return Err(Error::corrupt("staged journal diverged from overlay").into()),
            };
            let old = self.in_group(old, &cached)?;
            let new = self.in_group(new, &cached)?;
            if old == new {
                continue;
            }
            let mut change = std::collections::BTreeMap::new();
            change.insert("key".to_string(), parse_value(&key)?);
            if let Some(old) = old {
                change.insert("old".to_string(), parse_value(&old)?);
            }
            if let Some(new) = new {
                change.insert("new".to_string(), parse_value(&new)?);
            }
            changes.push(Value::Object(change));
        }
        let accumulator = if changes.is_empty() {
            cached.accumulator
        } else {
            self.session.spend(FUEL_CALL)?;
            self.session.spend(changes.len() as u64)?;
            let mut call = std::collections::BTreeMap::new();
            call.insert("group".to_string(), cached.group);
            call.insert("previous".to_string(), cached.accumulator);
            call.insert("changes".to_string(), Value::Array(changes));
            let runtimes: &HashMap<String, Arc<dyn Runtime>> = self.runtimes;
            let Some(runtime) = runtimes.get(&cached.module) else {
                return Ok(());
            };
            let mut host = NoContext::default();
            let outcome = runtime.call(
                CallKind::Reduce,
                &cached.module,
                &cached.name,
                &Value::Object(call),
                &mut host,
            );
            if host.reached {
                return Err(OpError::fatal(fail(
                    AGGREGATE_CONTEXT_FORBIDDEN,
                    format!("the reducer of `{}` reached for a context", cached.name),
                )));
            }
            match outcome {
                Ok(Value::Object(mut answer)) => match answer.remove("value") {
                    Some(value) => value,
                    // Not invertible: the group builds from its rows.
                    None => return Ok(()),
                },
                Ok(_) => return Ok(()),
                Err(failure) if failure.recovered => return Ok(()),
                Err(failure) => return Err(OpError::fatal(failure.failure)),
            }
        };
        let Ok(json) = render(&accumulator) else {
            return Ok(());
        };
        // Reducers read no clock: the value keeps the time it had.
        let outcome = DerivedOutcome {
            pinned: cached.pinned,
            code: cached.code,
            time: cached.time,
            body: OutcomeBody::Value(json),
        };
        self.session.staged.stage_derived_at(
            cached.key,
            encode_outcome(&outcome),
            Some(cached.edges),
            length,
        );
        Ok(())
    }

    /// A row image when it belongs to the cached aggregate's group:
    /// its index entry carries the group's components.
    fn in_group(
        &self,
        image: Option<Vec<u8>>,
        cached: &CachedAggregate,
    ) -> OpResult<Option<Vec<u8>>> {
        let Some(image) = image else {
            return Ok(None);
        };
        let components = crate::host::index_components(&image, &cached.fields)?;
        Ok((components.as_ref() == Some(&cached.components)).then_some(image))
    }

    /// The image a record had after the staged writes `journal[..basis]`:
    /// the last of them to write it, else the snapshot's.
    fn image_at(&self, collection: u32, key: &[u8], basis: usize) -> OpResult<Option<Vec<u8>>> {
        let journal = self.session.staged.journal();
        for op in journal[..basis.min(journal.len())].iter().rev() {
            if let StagedOp::Record {
                collection: written,
                key: at,
                value,
            } = op
                && *written == collection
                && at == key
            {
                return Ok(value.clone());
            }
        }
        Ok(self.read.get_record(&self.session.db, collection, key)?)
    }
}

/// Failure code of a reducer that reached for a context (v1's
/// `AGGREGATE_CONTEXT_FORBIDDEN`).
pub const AGGREGATE_CONTEXT_FORBIDDEN: &str = "aggregate_context_forbidden";

/// A cached aggregate value an invalidation found, with what its
/// reducer needs to go on from it.
struct CachedAggregate {
    /// The full `derived` key.
    key: Vec<u8>,
    module: String,
    name: String,
    /// The cached value.
    accumulator: Value,
    /// The group as the arguments hold it.
    group: Value,
    /// The group's index components, rendered.
    components: Vec<Vec<u8>>,
    /// The source collection.
    collection: u32,
    /// The index's fields.
    fields: Vec<String>,
    /// The journal length the value reflects.
    basis: usize,
    /// The value's edges: a maintained value keeps them.
    edges: Vec<Vec<u8>>,
    pinned: bool,
    code: [u8; crate::storage::keys::HASH_LEN],
    time: OutcomeTime,
}

/// The host a reducer runs against: every call fails, and the reach
/// is remembered.
#[derive(Default)]
struct NoContext {
    reached: bool,
}

impl HostCall for NoContext {
    fn op(&mut self, _op: u8, _input: &[u8]) -> Vec<u8> {
        self.reached = true;
        flower_sdk::abi::encode_err(&fail(
            codes::INVALID_INVOCATION,
            "aggregate reducers run without a context",
        ))
        .encode_binary()
        .unwrap_or_default()
    }

    fn read_untracked(&mut self) {
        self.reached = true;
    }

    fn read_keys(&mut self) {
        self.reached = true;
    }
}

/// Dependents among `edges` whose range bounds match the changed record
/// keys or index entries.
fn range_matches(
    db: &[u8],
    edges: &[DepEdge],
    records: &[RecordChange],
    indexes: &[IndexChange],
) -> crate::error::Result<Vec<(u32, Vec<u8>)>> {
    let mut full_keys = Vec::with_capacity(records.len());
    for (collection, key, _) in records {
        full_keys.push((*collection, encode_record_key(db, *collection, key)?));
    }
    let mut matched = Vec::new();
    for (dep_bytes, dependent) in edges {
        let (decoded, consumed) = decode_dep_key(dep_bytes)?;
        if consumed != dep_bytes.len() {
            return Err(Error::corrupt("trailing dependency bytes"));
        }
        let hit = match decoded {
            DepKey::CollectionRange { collection, range } => full_keys
                .iter()
                .any(|(id, full)| *id == collection && collection_range_matches(&range, full)),
            DepKey::Range { index, range } => indexes.iter().any(|(id, components, primary)| {
                *id == index && index_range_matches(&range, components, primary)
            }),
            DepKey::Record { .. } | DepKey::Derived { .. } => false,
        };
        if hit {
            matched.push(dependent.clone());
        }
    }
    Ok(matched)
}

/// Invalidate committed derived outcomes inside a write transaction,
/// where no guest code can run: everything depending on the changed
/// records or index entries, transitively. Replicated applies call this
/// with each patch's journal, because outcomes committed after the
/// patch's snapshot (by queries or earlier patches) have edges its
/// prepare-time invalidation never saw.
pub(crate) fn invalidate_committed(
    write: &WriteTxn,
    db: &[u8],
    records: &[RecordChange],
    indexes: &[IndexChange],
) -> crate::error::Result<()> {
    if records.is_empty() && indexes.is_empty() {
        return Ok(());
    }
    let mut seeds = Vec::new();
    for (collection, key, _) in records {
        seeds.extend(write.dependents(db, &encode_record_dep(*collection, key)?)?);
    }
    let edges = write.scan_dep_edges(db)?;
    seeds.extend(range_matches(db, &edges, records, indexes)?);
    drop_committed_closure(write, db, seeds)
}

/// Invalidate everything that resolved a managed key, transitively: a
/// key-catalog change moves no record, so only the reserved
/// [`crate::certificate::keys_dep`] edge finds them.
pub(crate) fn invalidate_keys(write: &WriteTxn, db: &[u8]) -> crate::error::Result<()> {
    let seeds = write.dependents(db, &crate::certificate::keys_dep())?;
    drop_committed_closure(write, db, seeds)
}

/// Invalidate everything that reached `module` from another module,
/// transitively: its collections, index ranges and derived values,
/// read or refused (see [`crate::certificate::module_dep`]). Activating
/// `module` calls this: what it exports, its read rules and contracts
/// may change, and the structures it retires or adds again are
/// collected, purged and backfilled without invalidating anything,
/// while the readers' own code era is unchanged. Removal needs no
/// such pass: [`drop_module`] drops the readers of its structures, and
/// a refusal stays one while it is gone.
pub(crate) fn invalidate_module_readers(
    write: &WriteTxn,
    db: &[u8],
    module: &str,
) -> crate::error::Result<()> {
    let seeds = write.dependents(db, &crate::certificate::module_dep(module))?;
    drop_committed_closure(write, db, seeds)
}

/// Take a removed module's derived state out of `write`: every outcome
/// other modules computed from its structures (`owned`, its intern IDs:
/// records, collection and index ranges, derived instances), with
/// everything depending on them, transitively, as an activation drops
/// readers of what it replaces; then every one of its own outcomes with
/// its edges, pins included, since no code of its runs to refresh them.
/// Derived caches are each member's own, so each applier drops what it
/// holds.
pub(crate) fn drop_module(
    write: &WriteTxn,
    read: &crate::storage::ReadTxn,
    db: &[u8],
    owned: &[u32],
) -> crate::error::Result<()> {
    let mine: HashSet<u32> = owned.iter().copied().collect();
    let mut seeds = Vec::new();
    for id in owned {
        for tag in [
            crate::storage::keys::DEP_RECORD,
            crate::storage::keys::DEP_RANGE,
            crate::storage::keys::DEP_DERIVED,
            crate::storage::keys::DEP_COLLECTION_RANGE,
        ] {
            let mut prefix = vec![tag];
            prefix.extend_from_slice(&id.to_be_bytes());
            seeds.extend(
                write
                    .dependents(db, &prefix)?
                    .into_iter()
                    .filter(|(definition, _)| !mine.contains(definition)),
            );
        }
    }
    drop_committed_closure(write, db, seeds)?;
    let base = crate::storage::keys::encode_db(db)?;
    for id in owned {
        let mut prefix = base.clone();
        prefix.push(crate::storage::keys::DEP_DERIVED);
        prefix.extend_from_slice(&id.to_be_bytes());
        let mut after: Option<Vec<u8>> = None;
        loop {
            let rows = read.scan_raw_prefix(
                crate::storage::SnapshotTable::Derived,
                &prefix,
                after.as_deref(),
                1024,
            )?;
            let Some((last, _)) = rows.last() else {
                break;
            };
            after = Some(last.clone());
            let fetched = rows.len();
            for (key, _) in rows {
                let (definition, args) = decode_derived_key(db, &key)?;
                write.delete_derived(db, definition, &args)?;
            }
            if fetched < 1024 {
                break;
            }
        }
    }
    Ok(())
}

/// Drop committed outcomes and everything depending on them. Pinned
/// outcomes cannot re-evaluate here, so they stay as invalidated pins:
/// never served, re-evaluated on the next read, still pinned.
fn drop_committed_closure(
    write: &WriteTxn,
    db: &[u8],
    seeds: Vec<(u32, Vec<u8>)>,
) -> crate::error::Result<()> {
    let mut dropped = HashSet::new();
    let mut queue: VecDeque<(u32, Vec<u8>)> = seeds.into();
    while let Some((definition, args)) = queue.pop_front() {
        if !dropped.insert((definition, args.clone())) {
            continue;
        }
        drop_committed(write, db, definition, &args)?;
        queue.extend(write.dependents(db, &encode_derived_dep(definition, &args)?)?);
    }
    Ok(())
}

/// Drop one committed outcome with its edges, keeping a pin as an
/// invalidated outcome.
pub(crate) fn drop_committed(
    write: &WriteTxn,
    db: &[u8],
    definition: u32,
    args: &[u8],
) -> crate::error::Result<()> {
    let prior = write.get_derived(db, definition, args)?;
    write.delete_derived(db, definition, args)?;
    if let Some(prior) = prior.filter(|prior| prior.pinned) {
        write.put_derived(
            db,
            definition,
            args,
            &DerivedOutcome::invalidated_pin(prior.code),
        )?;
    }
    Ok(())
}

/// Encode a business failure as its stored `{"code","message","details"}`
/// marker payload.
pub(crate) fn failure_value(failure: &Failure) -> Value {
    let mut map = std::collections::BTreeMap::new();
    map.insert("code".to_string(), Value::String(failure.code.clone()));
    map.insert(
        "message".to_string(),
        Value::String(failure.message.clone()),
    );
    map.insert(
        "details".to_string(),
        failure.details.clone().unwrap_or(Value::Null),
    );
    Value::Object(map)
}

/// Decode a stored failure marker back into a business failure.
fn stored_failure(json: &[u8]) -> OpResult<Failure> {
    let value = parse_value(json)?;
    let map = match value {
        Value::Object(map) => map,
        _ => return Err(Error::corrupt("stored derived failure is not an object").into()),
    };
    let code = match map.get("code") {
        Some(Value::String(code)) => code.clone(),
        _ => return Err(Error::corrupt("stored derived failure has no code").into()),
    };
    let message = match map.get("message") {
        Some(Value::String(message)) => message.clone(),
        _ => return Err(Error::corrupt("stored derived failure has no message").into()),
    };
    let details = match map.get("details") {
        None | Some(Value::Null) => None,
        Some(details) => Some(details.clone()),
    };
    let mut failure = fail(code, message);
    failure.details = details;
    Ok(failure)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failure_markers_round_trip() {
        let mut failure = fail("BUSY", "try again");
        failure.details = Some(Value::Number(1.0));
        let json = render(&failure_value(&failure)).unwrap();
        let back = stored_failure(&json).unwrap();
        assert_eq!(back, failure);
        let plain = stored_failure(&render(&failure_value(&fail("X", "y"))).unwrap()).unwrap();
        assert_eq!(plain.details, None);
        assert!(stored_failure(b"[]").is_err());
        assert!(stored_failure(&render(&Value::Object(Default::default())).unwrap()).is_err());
    }

    #[test]
    fn error_markers_reject_garbage() {
        assert!(stored_failure(b"not json").is_err());
    }
}
