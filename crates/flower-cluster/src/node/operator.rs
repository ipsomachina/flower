//! A member's operator surfaces beside deployment and membership: the
//! modules each database runs, staged deployments driven one step at a
//! time, the collection of what deployments retire, and when receipts
//! expire.
//!
//! A staged deployment is a replicated job an operator drives at the
//! leader (steps sent to a follower forward there): stage (validate the
//! module as [`Node::deploy`] does, bind every name it uses, store its
//! code, plan the index backfill and the graph rebuild), advance it a
//! bounded page at a time (the backfill, then the graph), inspect it,
//! then activate it (propose the activation every member applies) or
//! cancel it. Every step but status and activation is a batch item the
//! writer orders with the writes (see [`flower_engine::staging`]), so
//! every member holds the staging alike: a change of leader keeps it,
//! and the new leader continues where it stood. A one-call deployment
//! of the module replaces an open staging.
//!
//! What deployments retire (removed indexes and collections, cancelled
//! backfill, abandoned staged graphs) the leader's maintenance collects
//! a bounded page per entry, paced so collection takes at most
//! [`COLLECT_DUTY_PERCENT`] of the writer, as retention collection is;
//! an operator can collect a step on demand too.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use flower_engine::batch::BatchItem;
use flower_engine::engine::{ModuleSummary, StagingStatus};
use flower_engine::retention::Retention;
use flower_engine::runtime::Runtime;
use flower_engine::staging::{StagingCommand, StagingPhase, retiring_modules};
use flower_engine::storage::meta;
use flower_sdk::Value;

use super::retention::{COLLECT_DUTY_PERCENT, COLLECT_REST_MAX};
use super::{Node, NodeError, RetryStatus};
use crate::admission::{Class, REQUEST_OVERHEAD, deploy_bytes, input_bytes};
use crate::exec::now_ms;
use crate::statemachine::{StateMachine, group_reading, registered};
use crate::types::{ClusterWrite, ResponseOutcome, WriteResponse};
use crate::writer::Prepared;

/// Most backfill records one advance step may build.
pub const MAX_ADVANCE_RECORDS: usize = 65_536;
/// Most roots one advance step of the graph phase rebuilds: each
/// evaluates the target's code.
pub const MAX_GRAPH_ROOTS: usize = 1024;
/// How long one graph page evaluates before it ends early, once it has
/// rebuilt a root: a page holds the writer, as v1's (whose default
/// `FLOWER_DEPLOYMENT_PAGE_MS` this is).
pub const GRAPH_PAGE_TIME: Duration = Duration::from_millis(200);
/// Keys one collection entry of the leader's maintenance deletes, as
/// v1's purge pages.
pub const COLLECT_PAGE: usize = 1024;

/// A staged deployment's state at the leader, after an operator step.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StagedDeployment {
    /// The staging row, `None` once nothing is staged.
    pub status: Option<StagingStatus>,
    /// The staged target's code identity, while one is building,
    /// rebuilding or ready.
    pub identity: Option<[u8; 32]>,
    /// Records the step's backfill scanned.
    pub scanned: usize,
    /// Roots the step's graph page rebuilt.
    pub rebuilt: usize,
}

/// What the leader's collection of retired structures did since this
/// member opened.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct CollectionStats {
    /// Collection entries proposed.
    pub collections: u64,
    /// Keys those entries deleted.
    pub collected: u64,
    /// Modules whose retired structures were all collected.
    pub finished: u64,
}

/// The leader's collection pacing: none of it is replicated, and a
/// leader change starts it afresh.
#[derive(Default)]
pub(crate) struct CollectionClocks {
    /// The term this bookkeeping belongs to.
    term: u64,
    /// Per database and module, when its next collection entry may go.
    next: HashMap<(Vec<u8>, String), Instant>,
    stats: CollectionStats,
}

/// The collection pacing behind its lock.
pub(crate) type CollectionClocksLock = Mutex<CollectionClocks>;

/// When this member's unscoped receipts expire.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub struct ReceiptClock {
    /// The replicated expiry clock receipts expire at, in ms since the
    /// epoch: the largest applied proposal stamp.
    pub expiry_clock_ms: u64,
    /// How long an unscoped receipt answers duplicates.
    pub receipt_ttl_ms: u64,
}

/// One database as operators see it on this member.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DatabaseCatalog {
    /// Database name.
    pub db: Vec<u8>,
    /// Committed revision.
    pub revision: u64,
    /// Applied log position.
    pub applied: u64,
    /// Active modules, by name, with any staging row this member holds.
    pub modules: Vec<ModuleSummary>,
    /// The target identity of each module this member has a staging
    /// open for.
    pub staged: BTreeMap<String, [u8; 32]>,
    /// The retry state: incarnation and retention.
    pub retry: RetryStatus,
}

/// What operators see of this member's applied state, read from one
/// snapshot on the read side: no fresh-read barrier, no forward to the
/// leader, no engine lock. Replicated stagings show on every member;
/// a one-call deployment's staging only on the leader running it.
#[derive(Clone, Debug)]
pub struct OperatorCatalog {
    /// The last applied log index the snapshot holds.
    pub applied: Option<u64>,
    /// Every replicated database, and any other with active modules.
    pub databases: Vec<DatabaseCatalog>,
    /// Every database's managed-key catalog, as `GET /v1/admin/keys`
    /// without a database lists them, or why they could not be read.
    pub keys: Result<serde_json::Value, String>,
    /// When unscoped receipts expire.
    pub receipts: ReceiptClock,
}

/// A staging step's committed answer: its value, `None` when the step
/// proposed nothing, or its refusal as a coded failure.
fn staging_answer(response: Option<WriteResponse>) -> Result<Option<Value>, NodeError> {
    let Some(response) = response else {
        return Ok(None);
    };
    match response.outcome {
        ResponseOutcome::Value(bytes) => flower_engine::json::parse_value(&bytes)
            .map(Some)
            .map_err(NodeError::engine),
        ResponseOutcome::Error(message) => {
            let (code, detail) = message
                .split_once(": ")
                .unwrap_or(("deployment", message.as_str()));
            Err(NodeError::coded(code, detail))
        }
        ResponseOutcome::Failure(_) => Err(NodeError::Fatal(
            "a staging step answered a call failure".to_string(),
        )),
    }
}

/// A count a step answered, zero when absent.
fn count(answer: &Option<Value>, name: &str) -> usize {
    match answer {
        Some(Value::Object(fields)) => match fields.get(name) {
            Some(Value::Number(number)) => *number as usize,
            _ => 0,
        },
        _ => 0,
    }
}

impl Node {
    /// This member's applied operator view (see [`OperatorCatalog`]).
    /// Read it again when [`Node::subscribe_catalog`] moves.
    pub async fn operator_catalog(&self) -> Result<OperatorCatalog, NodeError> {
        self.with_reader(move |reader| {
            let read = reader.store().begin_read().map_err(NodeError::engine)?;
            let group = group_reading(&read).map_err(NodeError::failure)?;
            let mut names = registered(&read).map_err(NodeError::engine)?;
            for db in reader.databases() {
                if !names.contains(&db) {
                    names.push(db);
                }
            }
            let mut databases = Vec::with_capacity(names.len());
            for db in names {
                let modules = reader.modules(&db).map_err(NodeError::failure)?;
                let mut staged = BTreeMap::new();
                for module in modules.iter().filter(|module| module.staging.is_some()) {
                    if let Some(code) = reader
                        .staged_code(&db, &module.name)
                        .map_err(NodeError::failure)?
                    {
                        staged.insert(module.name.clone(), code.identity);
                    }
                }
                databases.push(DatabaseCatalog {
                    revision: read
                        .meta_u64(&db, meta::REVISION)
                        .map_err(NodeError::engine)?,
                    applied: read
                        .meta_u64(&db, meta::APPLIED_LOG)
                        .map_err(NodeError::engine)?,
                    retry: RetryStatus {
                        incarnation: read
                            .meta_u64(&db, meta::HISTORY_INCARNATION)
                            .map_err(NodeError::engine)?,
                        retention: Retention::read(&read, &db).map_err(NodeError::engine)?,
                    },
                    modules,
                    staged,
                    db,
                });
            }
            let keys = StateMachine::key_catalogs(&read)
                .map_err(|failure| failure.message)
                .and_then(super::key_listing);
            Ok(OperatorCatalog {
                applied: group.applied,
                databases,
                keys,
                receipts: ReceiptClock {
                    expiry_clock_ms: group.expiry_clock_ms,
                    receipt_ttl_ms: flower_engine::retry::IDEMPOTENCY_TTL_MS,
                },
            })
        })
        .await
    }

    /// When this member's unscoped receipts expire, as
    /// [`Node::receipt_clock`] answers it, on the read side: no engine
    /// lock.
    pub async fn applied_receipt_clock(&self) -> Result<ReceiptClock, NodeError> {
        self.with_reader(|reader| {
            let read = reader.store().begin_read().map_err(NodeError::engine)?;
            let group = group_reading(&read).map_err(NodeError::failure)?;
            Ok(ReceiptClock {
                expiry_clock_ms: group.expiry_clock_ms,
                receipt_ttl_ms: flower_engine::retry::IDEMPOTENCY_TTL_MS,
            })
        })
        .await
    }

    /// Each replicated database's committed revision and applied log
    /// position, on the read side: no engine lock.
    pub async fn database_positions(&self) -> Result<Vec<(Vec<u8>, u64, u64)>, NodeError> {
        self.with_reader(|reader| {
            let read = reader.store().begin_read().map_err(NodeError::engine)?;
            registered(&read)
                .map_err(NodeError::engine)?
                .into_iter()
                .map(|db| {
                    let revision = read
                        .meta_u64(&db, meta::REVISION)
                        .map_err(NodeError::engine)?;
                    let applied = read
                        .meta_u64(&db, meta::APPLIED_LOG)
                        .map_err(NodeError::engine)?;
                    Ok((db, revision, applied))
                })
                .collect()
        })
        .await
    }

    /// The staging of `module` in `db` at the leader, if any.
    pub async fn deployment_status(
        &self,
        db: &[u8],
        module: &str,
    ) -> Result<StagedDeployment, NodeError> {
        self.forward_unless_leading()?;
        self.staged(db, module, 0, 0).await
    }

    /// Stage a deployment without activating it: the module is
    /// validated as [`Node::deploy`] validates it, and a staging step
    /// every member applies binds its names, stores its code and plans
    /// its backfill and graph rebuild. The module's open staging of
    /// another target is abandoned; one of the same target stays.
    pub async fn stage_deployment(
        &self,
        db: &[u8],
        module: &str,
        identity: [u8; 32],
        wasm: &[u8],
        manifest: &Value,
    ) -> Result<StagedDeployment, NodeError> {
        self.check_storage()?;
        self.forward_unless_leading()?;
        let _deploying = self.claim_deployment(db, module)?;
        let _control = self
            .admission
            .acquire(
                Class::Control,
                db,
                deploy_bytes(wasm.len(), input_bytes(manifest)),
            )
            .await?;
        self.validate_module(db, module, wasm, manifest).await?;
        // The step's bindings take IDs past this member's intern
        // counter: no one-call deployment may intern here until it
        // applies (see `Node::interning`).
        let _interning = self.interning.write().await;
        let (stage_db, stage_module, stage_wasm, stage_manifest) = (
            db.to_vec(),
            module.to_string(),
            wasm.to_vec(),
            manifest.clone(),
        );
        self.stage_step(db, move |engine| {
            engine
                .prepare_stage(
                    &stage_db,
                    &stage_module,
                    identity,
                    &stage_wasm,
                    &stage_manifest,
                )
                .map_err(NodeError::failure)
        })
        .await?;
        self.staged(db, module, 0, 0).await
    }

    /// Advance the module's staging one bounded step: up to
    /// `max_records` records of its index backfill (at most
    /// [`MAX_ADVANCE_RECORDS`]), or once that is built, up to as many
    /// roots of its graph rebuild (at most [`MAX_GRAPH_ROOTS`]). Ready
    /// stagings stay ready.
    pub async fn advance_deployment(
        &self,
        db: &[u8],
        module: &str,
        max_records: usize,
    ) -> Result<StagedDeployment, NodeError> {
        self.check_storage()?;
        self.forward_unless_leading()?;
        let _deploying = self.claim_deployment(db, module)?;
        let _control = self
            .admission
            .acquire(Class::Control, db, REQUEST_OVERHEAD)
            .await?;
        let staged = self.staged(db, module, 0, 0).await?;
        let (Some(status), Some(identity)) = (staged.status, staged.identity) else {
            return Err(NodeError::coded("deployment", "nothing is staged"));
        };
        let budget = max_records.clamp(1, MAX_ADVANCE_RECORDS);
        if !status.replicated {
            // A one-call deployment's staging this member left behind:
            // it drains here alone, as it always did.
            let (page_db, page_module) = (db.to_vec(), module.to_string());
            let progress = self
                .with_engine(move |engine| {
                    engine
                        .staging_drain(&page_db, &page_module, budget)
                        .map_err(NodeError::failure)
                })
                .await?;
            self.catalog_changed();
            return self.staged(db, module, progress.scanned, 0).await;
        }
        let answer = match status.phase {
            StagingPhase::Ready | StagingPhase::Gc => return Ok(staged),
            StagingPhase::Building => {
                let command = StagingCommand::Advance {
                    module: module.to_string(),
                    identity,
                    records: budget as u32,
                };
                self.stage_step(db, move |_| Ok(Some(command.clone())))
                    .await?
            }
            StagingPhase::Rebuilding => {
                let runtime = self.staged_runtime(db, module).await?;
                let (page_db, page_module) = (db.to_vec(), module.to_string());
                let roots = budget.min(MAX_GRAPH_ROOTS);
                self.stage_step(db, move |engine| {
                    engine
                        .prepare_graph_page(
                            &page_db,
                            &page_module,
                            runtime.clone(),
                            (roots, GRAPH_PAGE_TIME),
                            now_ms(),
                        )
                        .map_err(NodeError::failure)
                })
                .await?
            }
        };
        self.staged(
            db,
            module,
            count(&answer, "scanned"),
            count(&answer, "roots"),
        )
        .await
    }

    /// Activate the module's ready staging: propose the activation every
    /// member applies, as [`Node::deploy`] ends.
    pub async fn activate_deployment(
        &self,
        db: &[u8],
        module: &str,
    ) -> Result<WriteResponse, NodeError> {
        self.check_storage()?;
        self.forward_unless_leading()?;
        let _deploying = self.claim_deployment(db, module)?;
        let (read_db, read_module) = (db.to_vec(), module.to_string());
        let (code, status) = self
            .with_reader(move |reader| {
                Ok((
                    reader
                        .staged_code(&read_db, &read_module)
                        .map_err(NodeError::failure)?,
                    reader
                        .staging_status(&read_db, &read_module)
                        .map_err(NodeError::failure)?,
                ))
            })
            .await?;
        let code = code.ok_or_else(|| NodeError::coded("deployment", "nothing is staged"))?;
        if !code.ready {
            return Err(NodeError::coded(
                "deployment",
                "the staging's backfill or graph has not finished: advance it first",
            ));
        }
        let replicated = status.is_some_and(|status| status.replicated);
        let _control = self
            .admission
            .acquire(
                Class::Control,
                db,
                deploy_bytes(code.wasm.len(), code.manifest.len()),
            )
            .await?;
        let (page_db, page_module) = (db.to_vec(), module.to_string());
        let activation = self
            .with_engine(move |engine| {
                engine
                    .prepare_activation(&page_db, &page_module)
                    .map_err(NodeError::failure)
            })
            .await?;
        let write = ClusterWrite::Deploy {
            db: db.to_vec(),
            module: module.to_string(),
            wasm: code.wasm,
            manifest: code.manifest,
            identity: activation.identity,
            bindings: activation.bindings,
            proposed_ms: now_ms(),
        };
        match self.propose(write).await {
            Ok(response) => Ok(response),
            Err(error) => {
                // A replicated staging outlives the leader: the next one
                // activates it. Only a staging of this member's own goes.
                if matches!(error, NodeError::Forward { .. }) && !replicated {
                    self.cancel_staging(db, module).await;
                }
                Err(error)
            }
        }
    }

    /// Cancel the module's staging: its backfill and staged graph go to
    /// collection, and the names only a local staging interned are
    /// released. Reports whether one was open.
    pub async fn cancel_deployment(&self, db: &[u8], module: &str) -> Result<bool, NodeError> {
        self.check_storage()?;
        self.forward_unless_leading()?;
        let _deploying = self.claim_deployment(db, module)?;
        self.cancel_open(db, module).await
    }

    /// Cancel the module's open staging, through the log when it
    /// replicates, and report whether one was open.
    pub(crate) async fn cancel_open(&self, db: &[u8], module: &str) -> Result<bool, NodeError> {
        let staged = self.staged(db, module, 0, 0).await?;
        let (Some(status), Some(identity)) = (staged.status, staged.identity) else {
            return Ok(false);
        };
        if status.replicated {
            let command = StagingCommand::Cancel {
                module: module.to_string(),
                identity,
            };
            return match self
                .stage_step(db, move |_| Ok(Some(command.clone())))
                .await
            {
                Ok(_) => Ok(true),
                Err(NodeError::Failure { code, .. }) if code == "deployment" => Ok(false),
                Err(error) => Err(error),
            };
        }
        let (cancel_db, cancel_module) = (db.to_vec(), module.to_string());
        let canceled = self
            .with_engine(move |engine| {
                engine
                    .cancel_open_staging(&cancel_db, &cancel_module)
                    .map_err(NodeError::failure)
            })
            .await?;
        self.catalog_changed();
        Ok(canceled)
    }

    /// Collect up to `max_keys` of the structures the module's
    /// deployments retired (removed indexes and collections, cancelled
    /// backfill, abandoned staged graphs) in one step every member
    /// applies alike. Reports the keys deleted and whether nothing
    /// retired remains. The leader's maintenance collects the same way
    /// on its own (see [`Node::maintain_staging`]).
    pub async fn collect_deployment(
        &self,
        db: &[u8],
        module: &str,
        max_keys: usize,
    ) -> Result<(usize, bool), NodeError> {
        self.check_storage()?;
        self.forward_unless_leading()?;
        let answer = self
            .collect_step(db, module, max_keys.clamp(1, MAX_ADVANCE_RECORDS))
            .await?;
        Ok(answer)
    }

    /// One collection step through the writer.
    async fn collect_step(
        &self,
        db: &[u8],
        module: &str,
        keys: usize,
    ) -> Result<(usize, bool), NodeError> {
        let command = StagingCommand::Collect {
            module: module.to_string(),
            keys: keys as u32,
        };
        let answer = self
            .stage_step(db, move |_| Ok(Some(command.clone())))
            .await?;
        let done = matches!(
            &answer,
            Some(Value::Object(fields)) if matches!(fields.get("done"), Some(Value::Bool(true)))
        );
        Ok((count(&answer, "collected"), done))
    }

    /// One pass of the leader's collection of what deployments retired:
    /// for every module whose staging row holds retired structures, a
    /// page of [`COLLECT_PAGE`] keys per entry until it is done, the
    /// pace asks for a rest (an entry that took `T` rests `T × (100 −
    /// duty) / duty`, at most [`COLLECT_REST_MAX`]) or `budget` passes.
    /// Followers forget their bookkeeping; proposals that fail wait for
    /// the next pass.
    pub async fn maintain_staging(&self, budget: Duration) {
        let term = {
            let metrics = self.raft.metrics();
            let metrics = metrics.borrow();
            (metrics.current_leader == Some(self.id)).then_some(metrics.current_term)
        };
        let Some(term) = term else {
            let mut clocks = self.collection_clocks();
            let stats = clocks.stats;
            *clocks = CollectionClocks {
                stats,
                ..CollectionClocks::default()
            };
            return;
        };
        {
            let mut clocks = self.collection_clocks();
            if clocks.term != term {
                let stats = clocks.stats;
                *clocks = CollectionClocks {
                    term,
                    stats,
                    ..CollectionClocks::default()
                };
            }
        }
        let retiring = self
            .with_reader(|reader| {
                let read = reader.store().begin_read().map_err(NodeError::engine)?;
                let mut retiring = Vec::new();
                for db in registered(&read).map_err(NodeError::engine)? {
                    // A database that does not serve here, or that a
                    // prepared transaction holds, would refuse the step.
                    if flower_engine::txn::unwritable(&read, &db)
                        .map_err(NodeError::engine)?
                        .is_some()
                    {
                        continue;
                    }
                    for module in retiring_modules(&read, &db).map_err(NodeError::engine)? {
                        retiring.push((db.clone(), module));
                    }
                }
                Ok(retiring)
            })
            .await;
        let Ok(retiring) = retiring else {
            return;
        };
        let deadline = Instant::now() + budget;
        for (db, module) in retiring {
            loop {
                let now = Instant::now();
                let key = (db.clone(), module.clone());
                let next = self
                    .collection_clocks()
                    .next
                    .get(&key)
                    .copied()
                    .unwrap_or(now);
                if now < next || now >= deadline || self.storage_failure().is_some() {
                    break;
                }
                let Ok((collected, done)) = self.collect_step(&db, &module, COLLECT_PAGE).await
                else {
                    break;
                };
                let took = now.elapsed();
                let rest = took
                    .saturating_mul(100 - COLLECT_DUTY_PERCENT)
                    .checked_div(COLLECT_DUTY_PERCENT)
                    .unwrap_or(COLLECT_REST_MAX)
                    .min(COLLECT_REST_MAX);
                let mut clocks = self.collection_clocks();
                clocks.stats.collections += 1;
                clocks.stats.collected += collected as u64;
                if done {
                    clocks.stats.finished += 1;
                    clocks.next.remove(&key);
                    break;
                }
                clocks.next.insert(key, Instant::now() + rest);
            }
        }
    }

    /// What the leader's collection of retired structures did so far.
    pub fn collection_stats(&self) -> CollectionStats {
        self.collection_clocks().stats
    }

    fn collection_clocks(&self) -> std::sync::MutexGuard<'_, CollectionClocks> {
        self.collection_clocks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Run one replicated staging step through the writer: `prepare`
    /// builds it on the writer's staged state (`None` proposes nothing).
    /// Answers the step's value, or its refusal as a coded failure.
    async fn stage_step<F>(&self, db: &[u8], mut prepare: F) -> Result<Option<Value>, NodeError>
    where
        F: FnMut(&mut flower_engine::engine::Engine) -> Result<Option<StagingCommand>, NodeError>
            + Send
            + 'static,
    {
        let queued = self
            .writers
            .acquire(Class::Writer, db, REQUEST_OVERHEAD)
            .await?;
        let item_db = db.to_vec();
        let written = self
            .writer
            .submit(Some(queued), move |engine| {
                Ok(match prepare(engine)? {
                    Some(command) => Prepared::Propose(
                        BatchItem::Staging {
                            db: item_db.clone(),
                            command,
                        },
                        (),
                    ),
                    None => Prepared::Answer(()),
                })
            })
            .await?;
        let answer = staging_answer(written.response);
        self.catalog_changed();
        answer
    }

    /// The staged target's runtime, built from its stored code.
    async fn staged_runtime(&self, db: &[u8], module: &str) -> Result<Arc<dyn Runtime>, NodeError> {
        let (read_db, read_module) = (db.to_vec(), module.to_string());
        let code = self
            .with_reader(move |reader| {
                reader
                    .staged_code(&read_db, &read_module)
                    .map_err(NodeError::failure)
            })
            .await?
            .ok_or_else(|| NodeError::coded("deployment", "nothing is staged"))?;
        let manifest =
            flower_engine::json::parse_value(&code.manifest).map_err(NodeError::engine)?;
        let (db, module, runtimes) = (db.to_vec(), module.to_string(), self.runtimes.clone());
        let runtime = tokio::task::spawn_blocking(move || {
            runtimes.build(&db, &module, &code.wasm, &manifest)
        })
        .await
        .map_err(|error| NodeError::Fatal(error.to_string()))?
        .map_err(|message| NodeError::Failure {
            code: "invalid_module".to_string(),
            message,
        })?;
        Ok(Arc::from(runtime))
    }

    /// When this member's unscoped receipts expire: the replicated
    /// expiry clock and the receipt lifetime.
    pub async fn receipt_clock(&self) -> Result<ReceiptClock, NodeError> {
        let expiry_clock_ms = self
            .with_engine(|engine| StateMachine::expiry_clock(engine).map_err(NodeError::failure))
            .await?;
        Ok(ReceiptClock {
            expiry_clock_ms,
            receipt_ttl_ms: flower_engine::retry::IDEMPOTENCY_TTL_MS,
        })
    }

    /// Validate a module as every member builds it on apply, so a broken
    /// one never reaches the log.
    async fn validate_module(
        &self,
        db: &[u8],
        module: &str,
        wasm: &[u8],
        manifest: &Value,
    ) -> Result<(), NodeError> {
        let (db, module, wasm, manifest) = (
            db.to_vec(),
            module.to_string(),
            wasm.to_vec(),
            manifest.clone(),
        );
        let runtimes = self.runtimes.clone();
        tokio::task::spawn_blocking(move || runtimes.build(&db, &module, &wasm, &manifest))
            .await
            .map_err(|error| NodeError::Fatal(error.to_string()))?
            .map(drop)
            .map_err(|message| NodeError::Failure {
                code: "invalid_module".to_string(),
                message,
            })
    }

    /// The staging row and target identity of the module, from one
    /// reader capture each.
    async fn staged(
        &self,
        db: &[u8],
        module: &str,
        scanned: usize,
        rebuilt: usize,
    ) -> Result<StagedDeployment, NodeError> {
        let (db, module) = (db.to_vec(), module.to_string());
        self.with_reader(move |reader| {
            let status = reader
                .staging_status(&db, &module)
                .map_err(NodeError::failure)?;
            let identity = reader
                .staged_code(&db, &module)
                .map_err(NodeError::failure)?
                .map(|code| code.identity);
            Ok(StagedDeployment {
                status,
                identity,
                scanned,
                rebuilt,
            })
        })
        .await
    }
}
