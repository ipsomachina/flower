//! Retry retention on a member: replicated epochs and floors, retry
//! sessions, and the leader's rotation and paced collection (see
//! [`flower_engine::retention`]).
//!
//! Every transition is a batch item the writer orders with the writes
//! it governs, so a request ordered before an advance may run and one
//! ordered after it never does. Status reads are linearizable.
//!
//! The leader advances a rotating database's epoch after `epoch_ms` of
//! its own monotonic time in it, counted from when it first saw that
//! epoch: wall clocks play no part, and a leader change restarts the
//! count, which only delays retirement. While a walk is incomplete it
//! collects one page per entry, resting `T × (100 − duty) / duty` after
//! an entry that took `T` (at most [`COLLECT_REST_MAX`]), so collection
//! takes at most [`COLLECT_DUTY_PERCENT`] of the writer. With a retry
//! window ([`Node::set_retry_window`], which `serve` sets), the leader
//! also gives every database nobody configured retention for v1's
//! default rotation ([`default_rotation`]).

use std::collections::{HashMap, HashSet};
use std::sync::atomic::Ordering;
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};

use flower_engine::batch::BatchItem;
use flower_engine::engine::CallOptions;
use flower_engine::retention::{
    Acknowledgement, COLLECT_PAGE, Retention, RetentionCommand, Session,
};
use flower_engine::storage::meta;
use flower_sdk::{Failure, Value};

use super::{Node, NodeError};
use crate::admission::{Class, REQUEST_OVERHEAD};
use crate::statemachine::registered;
use crate::types::ResponseOutcome;
use crate::writer::Prepared;

/// The writer's share collection takes at most, in percent.
pub const COLLECT_DUTY_PERCENT: u32 = 10;
/// The longest rest between two collection entries.
pub const COLLECT_REST_MAX: Duration = Duration::from_secs(10);
/// v1's default retry window (`FLOWER_RETRY_WINDOW_MS`): what `serve`
/// gives databases nobody configured retention for.
pub const DEFAULT_RETRY_WINDOW_MS: u64 = 24 * 60 * 60 * 1_000;
/// Epochs per default window: the leader advances every eighth of it.
pub const DEFAULT_EPOCHS: u64 = 8;
/// The most receipts a database may keep for the leader to initialize
/// its retention by default: initializing counts them while holding the
/// writer, so a larger one is left to an operator (v1's rule).
pub const DEFAULT_MAX_RECEIPTS: usize = 50_000;
/// How often the leader repeats that it left a database to an operator.
const DEFAULT_WARN_EVERY: Duration = Duration::from_secs(600);

/// The rotation the leader gives a database it initializes by default
/// for `window`: `(epoch_ms, keep_epochs)`, an epoch every eighth of
/// the window (at least [`flower_engine::retention::MIN_EPOCH_MS`]),
/// keeping nine, so a result stays retryable for at least the window
/// and at most an eighth longer, and each receipt is inspected about
/// eight times by collection walks over its life (v1's rule).
pub fn default_rotation(window: Duration) -> (u64, u64) {
    let window = u64::try_from(window.as_millis()).unwrap_or(u64::MAX);
    (
        window
            .div_ceil(DEFAULT_EPOCHS)
            .max(flower_engine::retention::MIN_EPOCH_MS),
        DEFAULT_EPOCHS + 1,
    )
}

/// A database's retry state as clients and operators read it.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct RetryStatus {
    /// The history incarnation identities name, zero until established.
    pub incarnation: u64,
    /// The replicated retention state, if initialized.
    pub retention: Option<Retention>,
}

/// What the leader's retention maintenance did since this member opened.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct RetentionStats {
    /// Databases given the default retention.
    pub defaults: u64,
    /// Epoch advances proposed by rotation.
    pub advances: u64,
    /// Collection entries proposed.
    pub collections: u64,
    /// Receipts and acknowledged results those entries deleted.
    pub deleted: u64,
    /// Walks finished.
    pub walks: u64,
    /// The last finished walk's duration, in milliseconds.
    pub walk_last_ms: u64,
}

/// The leader's rotation and collection bookkeeping: none of it is
/// replicated, and a leader change starts it afresh.
#[derive(Default)]
pub(crate) struct Clocks {
    /// The term this bookkeeping belongs to.
    term: u64,
    /// Per database, the epoch last seen current and since when.
    epochs: HashMap<Vec<u8>, (u64, Instant)>,
    /// Per database, when its next collection entry may go, and when
    /// its current walk began.
    walks: HashMap<Vec<u8>, (Instant, Instant)>,
    /// Per database left to an operator, when the leader last said so.
    warned: HashMap<Vec<u8>, Instant>,
    stats: RetentionStats,
}

impl Node {
    /// Run one retention transition for `db` through the writer: its
    /// answer, or its deterministic refusal as the inner `Err`.
    pub async fn retention(
        &self,
        db: &[u8],
        command: RetentionCommand,
    ) -> Result<Result<Value, Failure>, NodeError> {
        self.check_storage()?;
        self.forward_unless_leading()?;
        let queued = self
            .writers
            .acquire(Class::Writer, db, REQUEST_OVERHEAD)
            .await?;
        let db = db.to_vec();
        let written = self
            .writer
            .submit(Some(queued), move |_| {
                Ok(Prepared::Propose(
                    BatchItem::Retention {
                        db: db.clone(),
                        command: command.clone(),
                    },
                    (),
                ))
            })
            .await?;
        retention_answer(written.response)
    }

    /// A database's retry state, linearizable: the incarnation and epoch
    /// new scoped identities take, and the floor.
    pub async fn retry_status(&self, db: &[u8]) -> Result<RetryStatus, NodeError> {
        self.read_barrier().await?;
        let db = db.to_vec();
        self.with_reader(move |reader| {
            let read = reader.store().begin_read().map_err(NodeError::engine)?;
            Ok(RetryStatus {
                incarnation: read
                    .meta_u64(&db, meta::HISTORY_INCARNATION)
                    .map_err(NodeError::engine)?,
                retention: Retention::read(&read, &db).map_err(NodeError::engine)?,
            })
        })
        .await
    }

    /// Open a retry session `session` for the principal `module`'s
    /// authorization decides, or answer it again for that principal.
    pub async fn open_retry_session(
        &self,
        db: &[u8],
        module: &str,
        session: [u8; 16],
        options: &CallOptions,
    ) -> Result<Result<Value, Failure>, NodeError> {
        self.session_command(db, module, "$flower.session.open", options, move |owner| {
            RetentionCommand::OpenSession { session, owner }
        })
        .await
    }

    /// Acknowledge a session's sequences through `ack.through`: their
    /// results go and they never run again. `ack.abandon` fences
    /// sequences without a result too; never set it for a mere timeout.
    /// `ack.limit` bounds the work one acknowledgement does.
    pub async fn acknowledge_retry_session(
        &self,
        db: &[u8],
        module: &str,
        session: [u8; 16],
        ack: Acknowledgement,
        options: &CallOptions,
    ) -> Result<Result<Value, Failure>, NodeError> {
        self.session_command(db, module, "$flower.session.ack", options, move |owner| {
            RetentionCommand::Acknowledge {
                session,
                owner,
                through: ack.through,
                abandon: ack.abandon,
                limit: ack.limit,
            }
        })
        .await
    }

    /// Close a session for good: none of its requests runs again.
    pub async fn close_retry_session(
        &self,
        db: &[u8],
        module: &str,
        session: [u8; 16],
        options: &CallOptions,
    ) -> Result<Result<Value, Failure>, NodeError> {
        self.session_command(db, module, "$flower.session.close", options, move |owner| {
            RetentionCommand::CloseSession { session, owner }
        })
        .await
    }

    /// A session's state for its owner, linearizable.
    pub async fn retry_session_status(
        &self,
        db: &[u8],
        module: &str,
        session: [u8; 16],
        options: &CallOptions,
    ) -> Result<Result<Value, Failure>, NodeError> {
        self.read_barrier().await?;
        let (db, module) = (db.to_vec(), module.to_string());
        let options = CallOptions {
            external: false,
            ..options.clone()
        };
        self.with_reader(move |reader| {
            let view = reader.view().map_err(NodeError::failure)?;
            let authorized = match view.authorize(
                &db,
                &module,
                "$flower.session.status",
                &Value::Null,
                &options,
            ) {
                Ok(authorized) => authorized,
                Err(failure) => return Ok(Err(failure)),
            };
            let owner = flower_engine::auth::receipt_owner(authorized.principal())
                .map_err(NodeError::failure)?;
            let read = reader.store().begin_read().map_err(NodeError::engine)?;
            Ok(
                match Session::read(&read, &db, &session).map_err(NodeError::engine)? {
                    Some(row) if row.owner == owner => Ok(row.describe(&session)),
                    Some(_) => Err(Failure::new(
                        flower_engine::retention::SESSION_FORBIDDEN,
                        "the session belongs to another principal",
                    )),
                    None => Err(Failure::new(
                        flower_engine::retention::SESSION_UNKNOWN,
                        "the session does not exist",
                    )),
                },
            )
        })
        .await
    }

    /// One session transition for the principal `module`'s hook decides
    /// for `operation`, decided on the writer's staged state.
    async fn session_command(
        &self,
        db: &[u8],
        module: &str,
        operation: &'static str,
        options: &CallOptions,
        command: impl Fn([u8; 32]) -> RetentionCommand + Send + 'static,
    ) -> Result<Result<Value, Failure>, NodeError> {
        self.check_storage()?;
        self.forward_unless_leading()?;
        let queued = self
            .writers
            .acquire(Class::Writer, db, REQUEST_OVERHEAD)
            .await?;
        let (db, module, options) = (db.to_vec(), module.to_string(), options.clone());
        let written = self
            .writer
            .submit(Some(queued), move |engine| {
                Ok(
                    match engine.session_owner(&db, &module, operation, &options) {
                        Ok(owner) => Prepared::Propose(
                            BatchItem::Retention {
                                db: db.clone(),
                                command: command(owner),
                            },
                            None,
                        ),
                        Err(failure) => Prepared::Answer(Some(failure)),
                    },
                )
            })
            .await?;
        match written.note {
            Some(failure) => Ok(Err(failure)),
            None => retention_answer(written.response),
        }
    }

    /// One pass of the leader's retention maintenance: give databases
    /// nobody configured the default retention (see
    /// [`Node::set_retry_window`]), advance rotating databases whose
    /// epoch ran its time, then collect incomplete walks within
    /// `budget`, paced by the duty cycle. Followers forget their
    /// bookkeeping. Proposals that fail wait for the next pass.
    pub async fn maintain_retention(&self, budget: Duration) {
        let term = {
            let metrics = self.raft.metrics();
            let metrics = metrics.borrow();
            (metrics.current_leader == Some(self.id)).then_some(metrics.current_term)
        };
        let Some(term) = term else {
            *self.retention_clocks() = Clocks::default();
            return;
        };
        {
            let mut clocks = self.retention_clocks();
            if clocks.term != term {
                let stats = clocks.stats;
                *clocks = Clocks {
                    term,
                    stats,
                    ..Clocks::default()
                };
            }
        }
        let defaults = self.retry_window().is_some();
        // A database left to an operator is looked at again only when
        // the leader says so again.
        let left: HashSet<Vec<u8>> = self
            .retention_clocks()
            .warned
            .iter()
            .filter(|(_, at)| at.elapsed() < DEFAULT_WARN_EVERY)
            .map(|(db, _)| db.clone())
            .collect();
        let found = self
            .with_reader(move |reader| {
                let read = reader.store().begin_read().map_err(NodeError::engine)?;
                let (mut states, mut unset) = (Vec::new(), Vec::new());
                for db in registered(&read).map_err(NodeError::engine)? {
                    match Retention::read(&read, &db).map_err(NodeError::engine)? {
                        Some(state) => states.push((db, state)),
                        // System databases (`$`) keep no caller results,
                        // and one that does not serve here (frozen,
                        // moved away) would refuse it.
                        None if defaults
                            && !db.starts_with(b"$")
                            && !left.contains(&db)
                            && flower_engine::placement::read_refusal(&read, &db)
                                .map_err(NodeError::engine)?
                                .is_none() =>
                        {
                            let (receipts, _) = read
                                .receipts_page(&db, None, DEFAULT_MAX_RECEIPTS + 1)
                                .map_err(NodeError::engine)?;
                            unset.push((db, receipts.len() <= DEFAULT_MAX_RECEIPTS));
                        }
                        None => {}
                    }
                }
                Ok((states, unset))
            })
            .await;
        let Ok((states, unset)) = found else {
            return;
        };
        for (db, small) in unset {
            self.initialize_default(&db, small).await;
        }
        let deadline = Instant::now() + budget;
        for (db, state) in &states {
            self.rotate(db, state).await;
        }
        for (db, state) in states {
            if !state.collected {
                self.collect(&db, deadline).await;
            }
        }
    }

    /// Give `db` the default retention, unless it keeps too many receipts
    /// (`small` is false) to count them while holding the writer: those
    /// are left to an operator, with a warning every ten minutes.
    async fn initialize_default(&self, db: &[u8], small: bool) {
        let Some(window) = self.retry_window() else {
            return;
        };
        if !small {
            let due = {
                let mut clocks = self.retention_clocks();
                let now = Instant::now();
                let last = clocks.warned.get(db).copied();
                let due = last.is_none_or(|last| now.duration_since(last) >= DEFAULT_WARN_EVERY);
                if due {
                    clocks.warned.insert(db.to_vec(), now);
                }
                due
            };
            if due {
                tracing::warn!(
                    db = %String::from_utf8_lossy(db),
                    "retention left to an operator: the database keeps more than \
                     {DEFAULT_MAX_RECEIPTS} receipts; give it a rotation (`retention ... rotate`) \
                     at a quiet moment, or run with --retry-window-ms 0"
                );
            }
            return;
        }
        let (epoch_ms, keep_epochs) = default_rotation(window);
        let command = RetentionCommand::Default {
            epoch_ms,
            keep_epochs,
        };
        if let Ok(Ok(_)) = self.retention(db, command).await {
            self.retention_clocks().stats.defaults += 1;
        }
    }

    /// Have the leader give every database nobody configured retention
    /// for v1's default: epochs of an eighth of `window`, nine of them
    /// kept, so a result stays retryable for at least `window` and at
    /// most an eighth longer (see [`default_rotation`]). It initializes
    /// only a database with no retention state, through a replicated
    /// [`RetentionCommand::Default`] that changes nothing once anyone
    /// configured it, so an operator's settings always win; a different
    /// window later changes no database already initialized. `None` (the
    /// default for an embedded member) initializes nothing.
    pub fn set_retry_window(&self, window: Option<Duration>) {
        let ms = window.map_or(0, |window| {
            u64::try_from(window.as_millis()).unwrap_or(u64::MAX).max(1)
        });
        self.retry_window_ms.store(ms, Ordering::Relaxed);
    }

    /// The window [`Node::set_retry_window`] set, if any.
    pub fn retry_window(&self) -> Option<Duration> {
        match self.retry_window_ms.load(Ordering::Relaxed) {
            0 => None,
            ms => Some(Duration::from_millis(ms)),
        }
    }

    /// Advance `db` once the leader has spent its rotation's epoch in
    /// the current one.
    async fn rotate(&self, db: &[u8], state: &Retention) {
        let Some(rotation) = state.rotation else {
            return;
        };
        let now = Instant::now();
        let due = {
            let mut clocks = self.retention_clocks();
            let seen = clocks
                .epochs
                .entry(db.to_vec())
                .or_insert((state.current_epoch, now));
            if seen.0 != state.current_epoch {
                *seen = (state.current_epoch, now);
            }
            now.duration_since(seen.1) >= Duration::from_millis(rotation.epoch_ms)
        };
        if !due {
            return;
        }
        let next = state.current_epoch.saturating_add(1);
        let command = RetentionCommand::Advance {
            from_epoch: state.current_epoch,
            current_epoch: next,
            min_epoch: state.min_epoch.max(rotation.floor(next)),
        };
        if let Ok(Ok(_)) = self.retention(db, command).await {
            let mut clocks = self.retention_clocks();
            clocks.epochs.insert(db.to_vec(), (next, Instant::now()));
            clocks.stats.advances += 1;
        }
    }

    /// Collect `db`'s walk a page per entry until it completes, the pace
    /// asks for a rest, or `deadline` passes.
    async fn collect(&self, db: &[u8], deadline: Instant) {
        loop {
            let now = Instant::now();
            let (next, began) = {
                let mut clocks = self.retention_clocks();
                *clocks.walks.entry(db.to_vec()).or_insert((now, now))
            };
            if now < next || now >= deadline {
                return;
            }
            let command = RetentionCommand::Collect {
                limit: COLLECT_PAGE,
            };
            let answer = self.retention(db, command).await;
            let took = now.elapsed();
            let Ok(Ok(Value::Object(fields))) = answer else {
                return;
            };
            let number = |name: &str| match fields.get(name) {
                Some(Value::Number(number)) => *number as u64,
                _ => 0,
            };
            let complete = matches!(fields.get("complete"), Some(Value::Bool(true)));
            let rest = took
                .saturating_mul(100 - COLLECT_DUTY_PERCENT)
                .checked_div(COLLECT_DUTY_PERCENT)
                .unwrap_or(COLLECT_REST_MAX)
                .min(COLLECT_REST_MAX);
            let mut clocks = self.retention_clocks();
            clocks.stats.collections += 1;
            clocks.stats.deleted += number("deleted");
            if complete {
                clocks.stats.walks += 1;
                clocks.stats.walk_last_ms = began.elapsed().as_millis() as u64;
                clocks.walks.remove(db);
                return;
            }
            clocks
                .walks
                .insert(db.to_vec(), (Instant::now() + rest, began));
        }
    }

    /// What the leader's retention maintenance did so far.
    pub fn retention_stats(&self) -> RetentionStats {
        self.retention_clocks().stats
    }

    fn retention_clocks(&self) -> std::sync::MutexGuard<'_, Clocks> {
        self.retention_clocks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

/// A retention item's committed answer: its value, or its refusal.
fn retention_answer(
    response: Option<crate::types::WriteResponse>,
) -> Result<Result<Value, Failure>, NodeError> {
    let Some(response) = response else {
        return Err(NodeError::Fatal(
            "a retention transition proposed nothing".to_string(),
        ));
    };
    match response.outcome {
        ResponseOutcome::Value(bytes) => flower_engine::json::parse_value(&bytes)
            .map(Ok)
            .map_err(NodeError::engine),
        ResponseOutcome::Error(message) => {
            let (code, detail) = message.split_once(": ").unwrap_or(("rejected", &message));
            Ok(Err(Failure::new(code, detail)))
        }
        ResponseOutcome::Failure(_) => Ok(Err(Failure::new(
            "rejected",
            "the retention transition was refused",
        ))),
    }
}

/// The lock type the member holds its bookkeeping in.
pub(crate) type ClocksLock = Mutex<Clocks>;
