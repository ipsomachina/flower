//! Replicated staged deployments: the steps every member applies alike
//! as batch items, and the leader's preparation of them (see
//! [`crate::staging`]).
//!
//! The leader prepares a step on its writer's staged state: staging
//! computes the binding of every name the target uses, graph pages
//! evaluate the target's roots. Every applier then commits the step with
//! [`Engine::commit_staging`] inside its batch's transaction: the index
//! backfill and collection run from the replicated state, graph pages
//! install what the leader computed on exactly the state they apply to.
//! A step that no longer fits the staging it names is refused, changing
//! nothing, alike on every applier.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use flower_sdk::{Failure, Value};

use super::{
    DEFAULT_FUEL, Engine, GcProgress, STAGING_PAGE, abandon_staging, purge_readded, put_staging,
};
use crate::descriptors::{DefKind, MaterializePolicy, ModuleDef, load_module};
use crate::error::{Error, HostResult};
use crate::host::{Env, Host, index_components};
use crate::json::{parse_value, render};
use crate::runtime::Runtime;
use crate::session::{CallerKind, Invocation, StagedGraph};
use crate::staging::{
    GraphCursor, GraphDef, GraphOutcome, RetiredKind, STAGING_PREFIX, StagingCommand, StagingPhase,
    StagingState, decode_state, diff_plan, open_stagings, staged_definition, staging_key,
    target_names,
};
use crate::storage::codec::{DerivedOutcome, decode_outcome};
use crate::storage::keys::{DEP_DERIVED, HASH_LEN, decode_derived_key, encode_record_key};
use crate::storage::{ReadTxn, WriteTxn, meta};

/// The failure code of a refused staging step, and of operator steps
/// that do not fit the staging they name.
pub const DEPLOYMENT_REFUSED: &str = "deployment";

/// A refused step's failure.
fn refused(message: impl Into<String>) -> Failure {
    Failure::new(DEPLOYMENT_REFUSED, message)
}

/// A module's staging row inside `write`.
fn row_in(write: &WriteTxn, db: &[u8], module: &str) -> Result<Option<StagingState>, Error> {
    write
        .meta(db, &staging_key(module))?
        .map(|bytes| decode_state(&bytes))
        .transpose()
}

/// What a step answers: the staging's phase and progress after it,
/// plus the step's own counts.
fn answer(state: Option<&StagingState>, counts: &[(&str, f64)], done: Option<bool>) -> Value {
    let mut map = BTreeMap::new();
    map.insert(
        "phase".to_string(),
        state.map_or(Value::Null, |state| {
            Value::String(state.phase.name().to_string())
        }),
    );
    if let Some(state) = state {
        map.insert("rebuilt".to_string(), Value::Number(state.rebuilt as f64));
        map.insert(
            "retired".to_string(),
            Value::Number(state.retired.len() as f64),
        );
    }
    for (name, count) in counts {
        map.insert((*name).to_string(), Value::Number(*count));
    }
    if let Some(done) = done {
        map.insert("done".to_string(), Value::Bool(done));
    }
    Value::Object(map)
}

/// Build up to `budget` more records of `state`'s index backfill inside
/// `write`, from its cursor, and report the records scanned. Moves the
/// phase on (to rebuilding or ready) once the plan is done. Rows written
/// meanwhile are dual-written, so the scan only needs the rows it
/// reaches.
pub(crate) fn drain_in(
    write: &WriteTxn,
    db: &[u8],
    state: &mut StagingState,
    budget: usize,
) -> Result<usize, Error> {
    let mut scanned = 0usize;
    let mut remaining = budget;
    while state.plan_pos < state.plan.len() && remaining > 0 {
        let (collection_id, added) = {
            let collection = &state.plan[state.plan_pos];
            (collection.id, collection.added.clone())
        };
        let resume = !state.key_cursor.is_empty();
        let lower = if resume {
            Some(encode_record_key(db, collection_id, &state.key_cursor)?)
        } else {
            None
        };
        let take = remaining.min(STAGING_PAGE) + usize::from(resume);
        let mut page =
            write.scan_records(db, collection_id, lower.as_deref(), None, Some(take), false)?;
        // The lower bound is inclusive: skip the cursor row itself,
        // unless it vanished since the page that recorded it.
        let mut skipped = false;
        if resume
            && let Some(first) = page.first()
            && first.0 == state.key_cursor
        {
            page.remove(0);
            skipped = true;
        }
        for (key, json) in &page {
            for index in &added {
                if let Some(components) = index_components(json, &index.fields)? {
                    let refs: Vec<&[u8]> = components.iter().map(Vec::as_slice).collect();
                    write.put_index_entry(db, index.id, &refs, key)?;
                }
            }
            state.key_cursor = key.clone();
        }
        scanned += page.len();
        // The resume slot can fetch one row past the budget when the
        // cursor row vanished; saturating ends the drain on schedule.
        remaining = remaining.saturating_sub(page.len());
        if page.len() + usize::from(skipped) < take {
            state.plan_pos += 1;
            state.key_cursor.clear();
        }
    }
    if state.phase == StagingPhase::Building && state.plan_pos >= state.plan.len() {
        state.phase = state.after_backfill();
    }
    Ok(scanned)
}

/// Collect up to `budget` retired keys of `module` inside `write`:
/// removed index entries and collection records, cancelled backfill and
/// abandoned staged graphs. The staging row goes once nothing remains of
/// a collection-only one.
pub(crate) fn gc_in(
    write: &WriteTxn,
    db: &[u8],
    module: &str,
    budget: usize,
) -> Result<GcProgress, Error> {
    let Some(mut state) = row_in(write, db, module)? else {
        return Ok(GcProgress {
            done: true,
            collected: 0,
        });
    };
    let Some(next) = state.retired.first().cloned() else {
        if state.phase == StagingPhase::Gc {
            put_staging(write, db, module, None)?;
        }
        return Ok(GcProgress {
            done: true,
            collected: 0,
        });
    };
    let after = (!next.cursor.is_empty()).then(|| next.cursor.clone());
    let (deleted, resume) = match next.kind {
        RetiredKind::Collection => {
            write.delete_record_prefix(db, next.id, after.as_deref(), budget)?
        }
        RetiredKind::Index => write.delete_index_prefix(db, next.id, after.as_deref(), budget)?,
        RetiredKind::Derived => {
            let page = write.scan_derived(db, next.id, after.as_deref(), budget)?;
            for (args, _) in &page {
                write.delete_derived(db, next.id, args)?;
            }
            // As the prefix deleters: exhausted only after a page that
            // came back short, and a zero budget hands the cursor back.
            let resume = if budget > 0 && page.len() < budget {
                None
            } else {
                Some(
                    page.last()
                        .map(|(args, _)| args.clone())
                        .unwrap_or(next.cursor.clone()),
                )
            };
            (page.len(), resume)
        }
    };
    match resume {
        Some(cursor) => state.retired[0].cursor = cursor,
        None => {
            state.retired.remove(0);
        }
    }
    if state.retired.is_empty() && state.phase == StagingPhase::Gc {
        put_staging(write, db, module, None)?;
    } else {
        put_staging(write, db, module, Some(&state))?;
    }
    Ok(GcProgress {
        done: state.retired.is_empty(),
        collected: deleted,
    })
}

/// Rewrite a dependency key's staged definition to its active one.
fn remap_dependency(edge: &[u8], staged: &HashMap<u32, u32>) -> Vec<u8> {
    if edge.first() == Some(&DEP_DERIVED)
        && let Some(id) = edge.get(1..5)
        && let Ok(id) = <[u8; 4]>::try_from(id)
        && let Some(active) = staged.get(&u32::from_be_bytes(id))
    {
        let mut remapped = edge.to_vec();
        remapped[1..5].copy_from_slice(&active.to_be_bytes());
        return remapped;
    }
    edge.to_vec()
}

/// Move a replicated staging's staged graph over the active
/// definitions, inside the activation's transaction: every servable
/// staged outcome replaces its active instance with its edges (staged
/// definitions in them rewritten to active ones), keeping the active
/// instance's pin (and pinning what the target always retains), and
/// what the target's code computed takes the code era the activation
/// minted. Staged outcomes a write invalidated drop: their active
/// instances stay, stale by code era, and evaluate on their next read.
/// Nothing staged remains.
pub(crate) fn move_staged_graph(
    write: &WriteTxn,
    db: &[u8],
    target: &ModuleDef,
    state: &StagingState,
    era: [u8; HASH_LEN],
) -> Result<(), Error> {
    let graph = &state.graph;
    let staged: HashMap<u32, u32> = graph.iter().map(|def| (def.staged, def.active)).collect();
    for def in graph {
        let always = target.retention(&def.name) == MaterializePolicy::Always;
        loop {
            let page = write.scan_derived(db, def.staged, None, STAGING_PAGE)?;
            if page.is_empty() {
                break;
            }
            for (args, outcome) in page {
                let edges = write.dependent_edges(db, def.staged, &args)?;
                write.delete_derived(db, def.staged, &args)?;
                if !outcome.servable() {
                    continue;
                }
                let pinned = always
                    || write
                        .get_derived(db, def.active, &args)?
                        .is_some_and(|prior| prior.pinned);
                let edges: Vec<Vec<u8>> = edges
                    .iter()
                    .map(|edge| remap_dependency(edge, &staged))
                    .collect();
                // A versioned aggregate's lineage stays as it is.
                let code = if outcome.code == state.identity {
                    era
                } else {
                    outcome.code
                };
                crate::patch::install_derived(
                    write,
                    db,
                    def.active,
                    &args,
                    &DerivedOutcome {
                        pinned,
                        code,
                        ..outcome
                    },
                    Some(&edges),
                )?;
            }
        }
    }
    Ok(())
}

/// Whether a failure is the host's own (storage, I/O, corruption)
/// rather than the guest's: a graph page stops on these.
fn host_failure(failure: &Failure) -> bool {
    failure.code.starts_with("host_")
}

impl Engine {
    /// Prepare a replicated staging of `module` on the state prepares
    /// read (the writer's staged state): validate the target as
    /// [`Engine::deploy_staged`] does, then bind every name it uses,
    /// allocating unknown ones past this member's intern counter.
    /// `None` when the module's replicated staging already targets
    /// `identity`.
    pub fn prepare_stage(
        &self,
        db: &[u8],
        module: &str,
        identity: [u8; HASH_LEN],
        wasm: &[u8],
        manifest: &Value,
    ) -> HostResult<Option<StagingCommand>> {
        // `$` starts an import slot, never a module.
        if module.is_empty() || module.contains('\0') || module.starts_with('$') {
            return Err(Error::Manifest("invalid module identity".to_string()).into());
        }
        let target = load_module(manifest).map_err(Failure::from)?;
        Self::check_triggers(&target)?;
        crate::contracts::check_links(&self.modules(db).descriptors, module, &target)?;
        let read = self.snapshot().map_err(Failure::from)?;
        // A module whose data, or a queue it declares, is being purged
        // stages once it is gone.
        if let Some(refusal) =
            crate::purge::activation_refusal(&read, db, module, manifest).map_err(Failure::from)?
        {
            return Err(refusal);
        }
        if let Some(bytes) = read.meta(db, &staging_key(module)).map_err(Failure::from)? {
            let state = decode_state(&bytes).map_err(Failure::from)?;
            if state.replicated && state.phase != StagingPhase::Gc && state.identity == identity {
                return Ok(None);
            }
        }
        let counter = u32::try_from(
            read.meta_u64(db, meta::INTERN_COUNTER)
                .map_err(Failure::from)?,
        )
        .map_err(|_| Error::corrupt("intern counter overflowed"))?;
        let mut next = counter;
        let mut allocated: HashMap<Vec<u8>, u32> = HashMap::new();
        let mut bindings = Vec::new();
        for name in target_names(module, &target, Some(counter)) {
            let id = match read.intern_id(db, &name).map_err(Failure::from)? {
                Some(id) => id,
                None => match allocated.get(&name) {
                    Some(id) => *id,
                    None => {
                        next = next
                            .checked_add(1)
                            .ok_or_else(|| Error::Exhausted("intern IDs".to_string()))?;
                        allocated.insert(name.clone(), next);
                        next
                    }
                },
            };
            if !bindings.iter().any(|(known, _)| known == &name) {
                bindings.push((name, id));
            }
        }
        Ok(Some(StagingCommand::Stage {
            module: module.to_string(),
            identity,
            wasm: wasm.to_vec(),
            manifest: render(manifest).map_err(Failure::from)?,
            bindings,
            counter_before: counter,
        }))
    }

    /// Prepare one page of a replicated staging's graph rebuild on the state
    /// prepares read: evaluate up to `roots` pinned roots of the module's
    /// active derived definitions under the target code (built as `runtime`),
    /// in definition then argument order from the staging's cursor, into the
    /// staged graph. The page ends early once it has rebuilt a root and spent
    /// `time` (a soft target: a page holds the writer). `None` unless the
    /// module's replicated staging is rebuilding. A root whose target
    /// evaluation traps or runs out of fuel is passed over: it evaluates on
    /// its first read after activation, as it would have.
    pub fn prepare_graph_page(
        &self,
        db: &[u8],
        module: &str,
        runtime: Arc<dyn Runtime>,
        (roots, time): (usize, std::time::Duration),
        timestamp_ms: u64,
    ) -> HostResult<Option<StagingCommand>> {
        let started = std::time::Instant::now();
        let read = self.snapshot().map_err(Failure::from)?;
        let Some(bytes) = read.meta(db, &staging_key(module)).map_err(Failure::from)? else {
            return Ok(None);
        };
        let state = decode_state(&bytes).map_err(Failure::from)?;
        if !state.replicated || state.phase != StagingPhase::Rebuilding {
            return Ok(None);
        }
        let (_, manifest) = read
            .get_code(db, &state.identity)
            .map_err(Failure::from)?
            .ok_or_else(|| Error::corrupt("staged code is missing"))?;
        let target =
            load_module(&parse_value(&manifest).map_err(Failure::from)?).map_err(Failure::from)?;
        let modules = self.modules(db);
        let mut descriptors = modules.descriptors.clone();
        descriptors.insert(module.to_string(), target);
        let mut runtimes = modules.runtimes.clone();
        runtimes.insert(module.to_string(), runtime);
        let env = Env {
            runtimes: &runtimes,
            descriptors: &descriptors,
            history: self.catalog.history.as_ref(),
            policy: self.catalog.policy.as_ref(),
        };
        let graph = StagedGraph {
            module: module.to_string(),
            identity: state.identity,
            staged: state
                .graph
                .iter()
                .map(|def| (def.name.clone(), def.staged))
                .collect(),
        };
        let roots = roots.max(1);
        // Unpinned instances are skipped, but count toward the scan.
        let scan_limit = roots.saturating_mul(4);
        let mut position = state.graph_pos;
        let mut cursor = state.graph_cursor.clone();
        let mut outcomes: BTreeMap<(u32, Vec<u8>), GraphOutcome> = BTreeMap::new();
        let (mut rebuilt, mut scanned) = (0usize, 0usize);
        let spent = |rebuilt: usize| rebuilt > 0 && started.elapsed() >= time;
        while position < state.graph.len()
            && rebuilt < roots
            && scanned < scan_limit
            && !spent(rebuilt)
        {
            let def = &state.graph[position];
            if !def.walk {
                position += 1;
                cursor.clear();
                continue;
            }
            let want = (scan_limit - scanned).min(STAGING_PAGE);
            let after = (!cursor.is_empty()).then_some(cursor.as_slice());
            let page = read
                .scan_derived(db, def.active, after, want)
                .map_err(Failure::from)?;
            let fetched = page.len();
            let mut consumed = 0;
            for (args, outcome) in page {
                if rebuilt >= roots || spent(rebuilt) {
                    break;
                }
                consumed += 1;
                scanned += 1;
                if outcome.pinned {
                    self.rebuild_root(
                        &read,
                        &env,
                        &graph,
                        (db, module, &def.name),
                        &args,
                        timestamp_ms,
                        &mut outcomes,
                    )?;
                    rebuilt += 1;
                }
                cursor = args;
            }
            if consumed == fetched && fetched < want {
                position += 1;
                cursor.clear();
            }
        }
        Ok(Some(StagingCommand::Graph {
            module: module.to_string(),
            identity: state.identity,
            from: GraphCursor {
                position: state.graph_pos as u32,
                args: state.graph_cursor.clone(),
            },
            to: GraphCursor {
                position: position as u32,
                args: cursor,
            },
            done: position >= state.graph.len(),
            roots: rebuilt as u32,
            outcomes: outcomes.into_values().collect(),
        }))
    }

    /// Evaluate one root under the staged target and pin it in the
    /// staged graph, adding what the evaluation computed to `outcomes`.
    #[allow(clippy::too_many_arguments)]
    fn rebuild_root(
        &self,
        read: &ReadTxn,
        env: &Env<'_>,
        graph: &StagedGraph,
        (db, module, name): (&[u8], &str, &str),
        args: &[u8],
        timestamp_ms: u64,
        outcomes: &mut BTreeMap<(u32, Vec<u8>), GraphOutcome>,
    ) -> HostResult<()> {
        let mut session = Invocation::new(
            module.to_string(),
            db.to_vec(),
            CallerKind::Query,
            None,
            None,
            timestamp_ms,
            DEFAULT_FUEL,
        );
        session.graph = Some(graph.clone());
        session.replace_writable(false);
        let args_value = parse_value(args).map_err(Failure::from)?;
        let evaluated = {
            let mut host = Host::new(read, &mut session, env);
            // A business failure caches as an error marker, which pins
            // like a value.
            match host.get_or_evaluate(module, name, &args_value) {
                Ok(_) => Ok(()),
                Err(error) if !error.fatal => Ok(()),
                Err(error) => Err(error),
            }
            .and_then(|()| host.materialize(module, name, &args_value))
        };
        if let Err(error) = evaluated {
            if host_failure(&error.failure) {
                return Err(error.failure);
            }
            return Ok(());
        }
        for (key, outcome, observed) in session.staged.take_derived() {
            let (definition, args) = decode_derived_key(db, &key).map_err(Failure::from)?;
            outcomes.insert(
                (definition, args.clone()),
                GraphOutcome {
                    definition,
                    args,
                    outcome,
                    observed,
                },
            );
        }
        Ok(())
    }

    /// Commit one replicated staging step into `write`, as every member,
    /// device and restore replay does. The outer error is the store's;
    /// the inner one a deterministic refusal that changed nothing.
    pub(crate) fn commit_staging(
        &self,
        write: &WriteTxn,
        db: &[u8],
        command: &StagingCommand,
    ) -> HostResult<HostResult<Value>> {
        let module = command.module();
        let row = row_in(write, db, module).map_err(Failure::from)?;
        match command {
            StagingCommand::Stage {
                identity,
                wasm,
                manifest,
                bindings,
                counter_before,
                ..
            } => self.stage_in(
                write,
                db,
                module,
                row,
                (*identity, wasm, manifest),
                bindings,
                *counter_before,
            ),
            StagingCommand::Advance {
                identity, records, ..
            } => {
                let Some(mut state) = row.filter(|state| {
                    state.replicated
                        && state.phase != StagingPhase::Gc
                        && state.identity == *identity
                }) else {
                    return Ok(Err(refused("nothing is staged for that target")));
                };
                if state.phase != StagingPhase::Building {
                    return Ok(Ok(answer(Some(&state), &[("scanned", 0.0)], None)));
                }
                let scanned =
                    drain_in(write, db, &mut state, *records as usize).map_err(Failure::from)?;
                put_staging(write, db, module, Some(&state)).map_err(Failure::from)?;
                Ok(Ok(answer(
                    Some(&state),
                    &[("scanned", scanned as f64)],
                    None,
                )))
            }
            StagingCommand::Graph {
                identity,
                from,
                to,
                done,
                roots,
                outcomes,
                ..
            } => {
                let Some(mut state) = row.filter(|state| {
                    state.replicated
                        && state.phase == StagingPhase::Rebuilding
                        && state.identity == *identity
                }) else {
                    return Ok(Err(refused("no staged graph rebuilds for that target")));
                };
                if state.graph_pos != from.position as usize || state.graph_cursor != from.args {
                    return Ok(Err(refused("the graph page is stale")));
                }
                let decoded = outcomes
                    .iter()
                    .map(|outcome| decode_outcome(&outcome.outcome))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(Failure::from)?;
                for (outcome, decoded) in outcomes.iter().zip(&decoded) {
                    crate::patch::install_derived(
                        write,
                        db,
                        outcome.definition,
                        &outcome.args,
                        decoded,
                        outcome.observed.as_deref(),
                    )
                    .map_err(Failure::from)?;
                }
                state.graph_pos = to.position as usize;
                state.graph_cursor = to.args.clone();
                state.rebuilt = state.rebuilt.saturating_add(u64::from(*roots));
                if *done {
                    state.phase = StagingPhase::Ready;
                }
                put_staging(write, db, module, Some(&state)).map_err(Failure::from)?;
                Ok(Ok(answer(
                    Some(&state),
                    &[("roots", f64::from(*roots))],
                    None,
                )))
            }
            StagingCommand::Cancel { identity, .. } => {
                let Some(state) = row
                    .filter(|state| state.phase != StagingPhase::Gc && state.identity == *identity)
                else {
                    return Ok(Err(refused("nothing is staged for that target")));
                };
                let rest = abandon_staging(write, db, state).map_err(Failure::from)?;
                put_staging(write, db, module, rest.as_ref()).map_err(Failure::from)?;
                Ok(Ok(answer(rest.as_ref(), &[], None)))
            }
            StagingCommand::Collect { keys, .. } => {
                let progress = gc_in(write, db, module, *keys as usize).map_err(Failure::from)?;
                let state = row_in(write, db, module).map_err(Failure::from)?;
                Ok(Ok(answer(
                    state.as_ref(),
                    &[("collected", progress.collected as f64)],
                    Some(progress.done),
                )))
            }
        }
    }

    /// Commit a staging step: refuse a bad target or bindings that
    /// conflict with replicated ones, else abandon the module's open
    /// staging (and any local staging whose IDs the bindings claim),
    /// bind every name, store the code and plan the backfill and graph.
    #[allow(clippy::too_many_arguments)]
    fn stage_in(
        &self,
        write: &WriteTxn,
        db: &[u8],
        module: &str,
        row: Option<StagingState>,
        (identity, wasm, manifest): ([u8; HASH_LEN], &[u8], &[u8]),
        bindings: &[(Vec<u8>, u32)],
        counter_before: u32,
    ) -> HostResult<HostResult<Value>> {
        if module.is_empty() || module.contains('\0') || module.starts_with('$') {
            return Ok(Err(refused("invalid module identity")));
        }
        let target = match parse_value(manifest).and_then(|value| load_module(&value)) {
            Ok(target) => target,
            Err(error) => return Ok(Err(refused(error.to_string()))),
        };
        if let Err(failure) = Self::check_triggers(&target) {
            return Ok(Err(refused(failure.message)));
        }
        // A purge of the module, or of a queue it declares, may have
        // started since the leader prepared this: every applier refuses
        // alike.
        if let Some(refusal) = crate::purge::purge_refusal(
            &crate::purge::purges_in(write, db).map_err(Failure::from)?,
            module,
            target.queues.iter().map(|decl| decl.name.as_str()),
        ) {
            return Ok(Err(refusal));
        }
        if let Some(state) = &row
            && state.replicated
            && state.phase != StagingPhase::Gc
            && state.identity == identity
        {
            return Ok(Ok(answer(Some(state), &[], None)));
        }
        let open = open_stagings(
            write
                .scan_meta_prefix(db, STAGING_PREFIX)
                .map_err(Failure::from)?,
        )
        .map_err(Failure::from)?;
        // IDs a local staging holds are this member's alone: a binding
        // takes them from it. Any other conflict is with replicated
        // state, the same on every applier.
        let owned = |id: u32| {
            open.iter()
                .any(|(_, state)| !state.replicated && state.local.contains(&id))
        };
        let mut colliding = HashSet::new();
        for (name, id) in bindings {
            if let Some(bound) = write.intern_name(db, *id).map_err(Failure::from)?
                && bound != *name
            {
                if !owned(*id) {
                    return Ok(Err(refused(format!(
                        "intern ID {id} is bound to another name"
                    ))));
                }
                colliding.insert(*id);
            }
            if let Some(known) = write.intern_id(db, name).map_err(Failure::from)?
                && known != *id
            {
                if !owned(known) {
                    return Ok(Err(refused(format!(
                        "{} is bound to another intern ID",
                        String::from_utf8_lossy(name)
                    ))));
                }
                colliding.insert(known);
            }
        }
        for (staged, state) in &open {
            if staged == module || state.local.iter().any(|id| colliding.contains(id)) {
                let rest = abandon_staging(write, db, state.clone()).map_err(Failure::from)?;
                put_staging(write, db, staged, rest.as_ref()).map_err(Failure::from)?;
            }
        }
        for (name, id) in bindings {
            write.bind_intern(db, name, *id).map_err(Failure::from)?;
        }
        let mut retired = match row_in(write, db, module).map_err(Failure::from)? {
            Some(state) => state.retired,
            None => Vec::new(),
        };
        purge_readded(write, db, module, &target, &mut retired).map_err(Failure::from)?;
        write
            .put_code(db, &identity, wasm, manifest)
            .map_err(Failure::from)?;
        let active = self.active_def(db, module);
        let bound = |name: &str| write.intern_id(db, name.as_bytes());
        let (plan, _) = diff_plan(
            active,
            &target,
            &bound,
            &|collection| crate::host::qualify_collection(module, collection),
            &|collection, index| crate::host::qualify_index(module, collection, index),
        )
        .map_err(Failure::from)?;
        let mut names: Vec<&String> = target.def_kinds.keys().collect();
        names.sort();
        let mut graph = Vec::new();
        for name in names {
            if target.def_kinds.get(name) != Some(&DefKind::Derived) {
                continue;
            }
            let id = |qualified: String| -> HostResult<u32> {
                bound(&qualified)
                    .map_err(Failure::from)?
                    .ok_or_else(|| Error::corrupt("a staged definition is not bound").into())
            };
            graph.push(GraphDef {
                name: name.clone(),
                active: id(crate::host::qualify_definition(module, name))?,
                staged: id(staged_definition(counter_before, module, name))?,
                walk: active.and_then(|defs| defs.def_kinds.get(name)) == Some(&DefKind::Derived),
            });
        }
        let mut state = StagingState {
            identity,
            phase: StagingPhase::Building,
            plan,
            plan_pos: 0,
            key_cursor: Vec::new(),
            retired,
            counter_before,
            local: Vec::new(),
            replicated: true,
            graph,
            graph_pos: 0,
            graph_cursor: Vec::new(),
            rebuilt: 0,
        };
        if state.plan.is_empty() {
            state.phase = state.after_backfill();
        }
        put_staging(write, db, module, Some(&state)).map_err(Failure::from)?;
        Ok(Ok(answer(Some(&state), &[], None)))
    }
}
