//! Maintenance tasks on the engine: planning on the read side, runs
//! prepared on the writer, statuses for operators (see [`crate::task`]).

use flower_sdk::{Failure, Value};

use super::{DEFAULT_FUEL, Engine, EngineReader, now_ms, serving};
use crate::descriptors::ModuleDef;
use crate::error::{Error, HostResult};
use crate::host::{Host, TASK_COLLECTION};
use crate::json::render;
use crate::lane::{self, CommitSpec, PreparedPatch, TaskRun};
use crate::patch::{Patch, RecordWrite, encode_patch};
use crate::runtime::CallKind;
use crate::session::{CallerKind, Invocation};
use crate::storage::{ReadTxn, meta};
use crate::task::{DueTask, TaskPlan, TaskPrepare, TaskState, TaskStatus, state_key};

/// A task's failure state in `read`, if it keeps one.
fn task_state(
    read: &ReadTxn,
    db: &[u8],
    collection: Option<u32>,
    module: &str,
    task: &str,
) -> HostResult<Option<TaskState>> {
    let Some(collection) = collection else {
        return Ok(None);
    };
    let row = read
        .get_record(db, collection, &state_key(module, task))
        .map_err(Failure::from)?;
    row.map(|bytes| TaskState::decode(&bytes).map_err(Failure::from))
        .transpose()
}

/// Every task `defs` declares, with its failure state in `read`.
pub(super) fn task_statuses(
    read: &ReadTxn,
    db: &[u8],
    module: &str,
    defs: &ModuleDef,
) -> HostResult<Vec<TaskStatus>> {
    if defs.tasks.is_empty() {
        return Ok(Vec::new());
    }
    let collection = read.intern_id(db, TASK_COLLECTION).map_err(Failure::from)?;
    let mut statuses = Vec::with_capacity(defs.tasks.len());
    for task in &defs.tasks {
        let state = task_state(read, db, collection, module, &task.name)?;
        statuses.push(TaskStatus::of(&task.name, task.on_error, state.as_ref()));
    }
    Ok(statuses)
}

/// A session for one task invocation of `module` at `now`: no principal
/// and definer rights, as the host's own work.
fn task_session(db: &[u8], module: &str, kind: CallerKind, now: u64) -> Invocation {
    let mut session = Invocation::new(
        module.to_string(),
        db.to_vec(),
        kind,
        None,
        None,
        now,
        DEFAULT_FUEL,
    );
    session.enter_definer();
    session
}

impl EngineReader {
    /// The tasks due in `db` at `now_ms` on a fresh snapshot, earliest
    /// first (then by module and name): every declared task whose `due`
    /// answers that time or earlier and that is not backing off. A
    /// `due` that fails counts as due, so its run's preparation, which
    /// evaluates it again, records the failure. Read-only: it never
    /// takes the writer.
    pub fn due_tasks(&self, db: &[u8], now_ms: u64) -> HostResult<Vec<DueTask>> {
        self.plan_tasks(db, now_ms).map(|plan| plan.due)
    }

    /// [`EngineReader::due_tasks`], with the earliest later time a task
    /// may be due: when the leader wakes next.
    pub fn plan_tasks(&self, db: &[u8], now_ms: u64) -> HostResult<TaskPlan> {
        let (catalog, read, _) = self.capture()?;
        serving(&read, db)?;
        let modules = catalog.modules(db);
        let env = catalog.env(db);
        let collection = read.intern_id(db, TASK_COLLECTION).map_err(Failure::from)?;
        let mut names: Vec<&String> = modules.descriptors.keys().collect();
        names.sort();
        let mut plan = TaskPlan::default();
        let mut later = |at: u64| {
            plan.next_ms = Some(plan.next_ms.map_or(at, |next: u64| next.min(at)));
        };
        let mut due = Vec::new();
        for module in names {
            let defs = &modules.descriptors[module];
            if defs.tasks.is_empty() || !modules.runtimes.contains_key(module) {
                continue;
            }
            for task in &defs.tasks {
                let state = task_state(&read, db, collection, module, &task.name)?;
                let retry = state.map(|state| state.retry_at_ms);
                if let Some(retry) = retry.filter(|at| *at > now_ms) {
                    later(retry);
                    continue;
                }
                let mut session = task_session(db, module, CallerKind::Query, now_ms);
                let mut host = Host::new(&read, &mut session, &env);
                let answer = host
                    .invoke_runtime(CallKind::Query, module, &task.name, &Value::Null)
                    .map_err(|error| error.failure)?;
                let at = match answer {
                    Ok(Value::Null) => continue,
                    Ok(Value::Number(at)) => at,
                    Ok(_) | Err(_) => now_ms as f64,
                };
                if at > now_ms as f64 {
                    // Saturating: a far-future answer wakes nobody early.
                    later(at.ceil().min(u64::MAX as f64) as u64);
                    continue;
                }
                due.push(DueTask {
                    module: module.clone(),
                    task: task.name.clone(),
                    due_ms: retry.map_or(at, |retry| at.max(retry as f64)),
                });
            }
        }
        due.sort_by(|left, right| {
            left.due_ms
                .total_cmp(&right.due_ms)
                .then_with(|| left.module.cmp(&right.module))
                .then_with(|| left.task.cmp(&right.task))
        });
        plan.due = due;
        Ok(plan)
    }

    /// Whether any active module of any database declares a task: the
    /// leader's task loop does nothing else when none does.
    pub fn has_tasks(&self) -> bool {
        let published = self
            .shared
            .catalog
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        published.modules.values().any(|modules| {
            modules
                .descriptors
                .values()
                .any(|defs| !defs.tasks.is_empty())
        })
    }

    /// Every task `module` declares in `db`, with its failure state.
    pub fn tasks(&self, db: &[u8], module: &str) -> HostResult<Vec<TaskStatus>> {
        let (catalog, read, _) = self.capture()?;
        match catalog.modules(db).descriptors.get(module) {
            Some(defs) => task_statuses(&read, db, module, defs),
            None => Ok(Vec::new()),
        }
    }
}

impl Engine {
    /// Prepare one run of a declared task, at the current time, on the
    /// state prepares read (the writer's staged state inside it). The
    /// task's `due` is evaluated again in the run's own transaction: a
    /// task with no work there, or backing off, or no longer declared,
    /// is [`TaskPrepare::Skip`]. A successful run's patch also deletes
    /// the task's failure state. A failed run's writes are dropped, and
    /// the patch is the error callback's, when the task declares one and
    /// it succeeds, or else the one recording the failure state: one
    /// more failure, backing off from now. Every patch is guarded by the
    /// revision it was prepared at, so a run never lands on a state it
    /// did not see.
    pub fn prepare_task_run(
        &mut self,
        db: &[u8],
        module: &str,
        task: &str,
    ) -> HostResult<TaskPrepare> {
        self.serving(db)?;
        let now = now_ms();
        let Some(declared) = self
            .active_def(db, module)
            .and_then(|defs| defs.tasks.iter().find(|known| known.name == task))
            .cloned()
        else {
            return Ok(TaskPrepare::Skip);
        };
        if !self.modules(db).runtimes.contains_key(module) {
            return Ok(TaskPrepare::Skip);
        }
        let read = self.snapshot().map_err(Failure::from)?;
        let revision = read.meta_u64(db, meta::REVISION).map_err(Failure::from)?;
        let collection = read
            .intern_id(db, TASK_COLLECTION)
            .map_err(Failure::from)?
            .ok_or_else(|| Error::corrupt("the task state collection is not interned"))?;
        let key = state_key(module, task);
        let state = task_state(&read, db, Some(collection), module, task)?;
        if state.as_ref().is_some_and(|state| state.retry_at_ms > now) {
            return Ok(TaskPrepare::Skip);
        }
        let commit = || CommitSpec {
            receipt: None,
            expected_revision: Some(revision),
        };
        let env = self.env(db);
        let mut session = task_session(db, module, CallerKind::Mutation, now);
        let clear = state.is_some().then(|| (collection, key.clone()));
        let failure =
            match lane::prepare_task(&read, &mut session, &env, module, task, clear, commit())? {
                TaskRun::NotDue => return Ok(TaskPrepare::Skip),
                TaskRun::Ran(prepared) => return Ok(TaskPrepare::Ran(prepared)),
                TaskRun::Failed(failure) => failure,
            };
        let mut recorded = failure.clone();
        if declared.on_error {
            let mut session = task_session(db, module, CallerKind::Mutation, now);
            match lane::prepare_task_error(
                &read,
                &mut session,
                &env,
                module,
                task,
                &failure,
                commit(),
            )? {
                TaskRun::Ran(prepared) => return Ok(TaskPrepare::Failed { prepared, failure }),
                TaskRun::Failed(handled) => recorded = handled,
                TaskRun::NotDue => {}
            }
        }
        let next = TaskState::after(state.as_ref(), recorded, now);
        let prepared = state_patch(db, collection, key, &next, revision)?;
        Ok(TaskPrepare::Failed { prepared, failure })
    }
}

/// The planned patch recording a task's failure state. No guest code
/// runs, so no module versions pin; guarded by the revision the run was
/// prepared at, like a timer's retry patch.
fn state_patch(
    db: &[u8],
    collection: u32,
    key: Vec<u8>,
    state: &TaskState,
    revision: u64,
) -> HostResult<PreparedPatch> {
    let patch = Patch {
        failed: false,
        db: db.to_vec(),
        versions: Vec::new(),
        records: vec![RecordWrite {
            collection,
            key,
            value: Some(state.encode().map_err(Failure::from)?),
        }],
        indexes: Vec::new(),
        derived: Vec::new(),
        drops: Vec::new(),
        receipts: Vec::new(),
        result: render(&Value::Null).map_err(Failure::from)?,
        expected_revision: Some(revision),
        base_revision: revision,
    };
    Ok(PreparedPatch {
        patch: encode_patch(&patch).map_err(Failure::from)?,
        outcome: Ok(Value::Null),
    })
}
