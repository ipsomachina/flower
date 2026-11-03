//! Removing modules and purging their data on a member (see
//! [`flower_engine::purge`]).
//!
//! A removal runs at the leader (followers forward): checked there first,
//! so a refused one never reaches the log, then proposed as one
//! [`ClusterWrite::Remove`] entry every member, device and restore replay
//! applies, checking again on the state it applies to. It claims its
//! module as a deployment does, so one never races the other on a member.
//!
//! The leader's maintenance purges what removals with `purge` left, a
//! page per entry ([`flower_engine::batch::BatchItem::Purge`]) through
//! the writer, so pages order with the writes the writer serves and
//! prepare on what those left. Pages are paced as retention collection
//! is: after a page that took `T`, the next waits
//! `T × (100 − duty) / duty` (at most [`COLLECT_REST_MAX`]), so purging
//! takes at most [`COLLECT_DUTY_PERCENT`] of the writer. The purge's
//! cursor is replicated with each page, so a new leader continues where
//! the last page left off; the pacing bookkeeping is not, and a leader
//! change starts it afresh.

use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};

use flower_engine::engine::ModuleSummary;
use flower_engine::purge::{self, Progress};
use flower_sdk::{Failure, Value};

use super::retention::{COLLECT_DUTY_PERCENT, COLLECT_REST_MAX};
use super::{Node, NodeError};
use crate::admission::{Class, REQUEST_OVERHEAD};
use crate::exec::now_ms;
use crate::statemachine::registered;
use crate::types::{ClusterWrite, ResponseOutcome, WriteResponse};
use crate::writer::Prepared;

/// One database as operators list its modules: the active ones, the
/// purges in flight and the modules removed whose data stays.
#[derive(Clone, Debug)]
pub struct ModuleListing {
    /// Database name.
    pub db: Vec<u8>,
    /// Active modules, by name.
    pub modules: Vec<ModuleSummary>,
    /// Modules whose data is being purged, by name, with how far each
    /// purge went.
    pub purging: Vec<(String, Progress)>,
    /// Modules removed without a purge, by name, with the code identity
    /// each last ran: their data and declarations stay.
    pub removed: Vec<(String, [u8; 32])>,
}

/// What the leader's purge maintenance did since this member opened.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct PurgeStats {
    /// Purge pages proposed and applied.
    pub pages: u64,
    /// Keys those pages deleted.
    pub deleted: u64,
    /// Purges finished.
    pub finished: u64,
}

/// The leader's purge pacing: none of it is replicated, and a leader
/// change starts it afresh.
#[derive(Default)]
pub(crate) struct Clocks {
    /// The term this bookkeeping belongs to.
    term: u64,
    /// Per database, when its next page may go.
    next: HashMap<Vec<u8>, Instant>,
    stats: PurgeStats,
}

/// The lock the member holds its purge bookkeeping in.
pub(crate) type ClocksLock = Mutex<Clocks>;

impl Node {
    /// Remove `module` from `db` through consensus, at the leader
    /// (followers forward): its code goes on every member, and its data
    /// stays with its declarations, or, with `purge`, goes a page at a
    /// time; `purge` on a module removed earlier purges what its removal
    /// kept. Refused (`MODULE_CONFLICT`) for a module not deployed (but
    /// for a purge), or one another module imports from; a database that
    /// does not serve here, or that a prepared transaction holds, refuses
    /// as it refuses deployments. The answer is `{removed, purging}`.
    pub async fn remove_module(
        &self,
        db: &[u8],
        module: &str,
        purge: bool,
    ) -> Result<WriteResponse, NodeError> {
        self.check_storage()?;
        self.forward_unless_leading()?;
        let _removing = self.claim_deployment(db, module)?;
        let _control = self
            .admission
            .acquire(Class::Control, db, REQUEST_OVERHEAD)
            .await?;
        let (check_db, check_module) = (db.to_vec(), module.to_string());
        let refusal = self
            .with_reader(move |reader| {
                reader
                    .removal_refusal(&check_db, &check_module, purge)
                    .map_err(NodeError::failure)
            })
            .await?;
        if let Some(refusal) = refusal {
            return Err(NodeError::failure(refusal));
        }
        self.propose(ClusterWrite::Remove {
            db: db.to_vec(),
            module: module.to_string(),
            purge,
            proposed_ms: now_ms(),
        })
        .await
    }

    /// Delete the next page of `db`'s first purge in flight, by module
    /// name, through the writer at the leader: the page's committed
    /// answer (`{module, deleted, keys, done}`) or refusal, or `None` when no
    /// purge is in flight (or the database does not serve here).
    pub async fn purge_page(&self, db: &[u8]) -> Result<Option<Result<Value, Failure>>, NodeError> {
        self.check_storage()?;
        self.forward_unless_leading()?;
        let queued = self
            .writers
            .acquire(Class::Writer, db, REQUEST_OVERHEAD)
            .await?;
        let page_db = db.to_vec();
        let written = self
            .writer
            .submit(Some(queued), move |engine| {
                Ok(
                    match engine
                        .prepare_purge(&page_db, purge::PAGE)
                        .map_err(NodeError::failure)?
                    {
                        Some(item) => Prepared::Propose(item, ()),
                        None => Prepared::Answer(()),
                    },
                )
            })
            .await?;
        let Some(response) = written.response else {
            return Ok(None);
        };
        Ok(Some(match response.outcome {
            ResponseOutcome::Value(bytes) => {
                Ok(flower_engine::json::parse_value(&bytes).map_err(NodeError::engine)?)
            }
            ResponseOutcome::Error(message) => {
                let (code, detail) = message.split_once(": ").unwrap_or(("rejected", &message));
                Err(Failure::new(code, detail))
            }
            ResponseOutcome::Failure(_) => {
                Err(Failure::new("rejected", "the purge page was refused"))
            }
        }))
    }

    /// One pass of the leader's purge maintenance: in every database
    /// with a purge in flight, a page per entry until its purges end,
    /// the pace asks for a rest, or `budget` passes. Followers forget
    /// their bookkeeping. Pages that fail wait for the next pass.
    pub async fn maintain_purges(&self, budget: Duration) {
        let term = {
            let metrics = self.raft.metrics();
            let metrics = metrics.borrow();
            (metrics.current_leader == Some(self.id)).then_some(metrics.current_term)
        };
        let Some(term) = term else {
            let stats = self.purge_clocks().stats;
            *self.purge_clocks() = Clocks {
                stats,
                ..Clocks::default()
            };
            return;
        };
        {
            let mut clocks = self.purge_clocks();
            if clocks.term != term {
                let stats = clocks.stats;
                *clocks = Clocks {
                    term,
                    stats,
                    ..Clocks::default()
                };
            }
        }
        let purging = self
            .with_reader(|reader| {
                let read = reader.store().begin_read().map_err(NodeError::engine)?;
                let mut purging = Vec::new();
                for db in registered(&read).map_err(NodeError::engine)? {
                    if read
                        .meta(&db, purge::PURGES_ROW)
                        .map_err(NodeError::engine)?
                        .is_some()
                    {
                        purging.push(db);
                    }
                }
                Ok(purging)
            })
            .await;
        let Ok(purging) = purging else {
            return;
        };
        let deadline = Instant::now() + budget;
        for db in purging {
            self.purge(&db, deadline).await;
        }
    }

    /// Purge `db` a page per entry until no purge is left, the pace asks
    /// for a rest, or `deadline` passes.
    async fn purge(&self, db: &[u8], deadline: Instant) {
        loop {
            let now = Instant::now();
            let next = *self.purge_clocks().next.entry(db.to_vec()).or_insert(now);
            if now < next || now >= deadline {
                return;
            }
            let answer = self.purge_page(db).await;
            let took = now.elapsed();
            let Ok(Some(Ok(Value::Object(fields)))) = answer else {
                return;
            };
            let rest = took
                .saturating_mul(100 - COLLECT_DUTY_PERCENT)
                .checked_div(COLLECT_DUTY_PERCENT)
                .unwrap_or(COLLECT_REST_MAX)
                .min(COLLECT_REST_MAX);
            let mut clocks = self.purge_clocks();
            clocks.stats.pages += 1;
            if let Some(Value::Number(keys)) = fields.get("keys") {
                clocks.stats.deleted += *keys as u64;
            }
            if matches!(fields.get("done"), Some(Value::Bool(true))) {
                clocks.stats.finished += 1;
            }
            clocks.next.insert(db.to_vec(), Instant::now() + rest);
        }
    }

    /// What the leader's purge maintenance did so far.
    pub fn purge_stats(&self) -> PurgeStats {
        self.purge_clocks().stats
    }

    fn purge_clocks(&self) -> std::sync::MutexGuard<'_, Clocks> {
        self.purge_clocks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// The modules each database runs, after a fresh-read barrier, with
    /// the purges in flight and the modules removed whose data stays:
    /// only `db` when given, else every database with modules, purges
    /// or removed modules.
    pub async fn modules(&self, db: Option<&[u8]>) -> Result<Vec<ModuleListing>, NodeError> {
        self.read_barrier().await?;
        let db = db.map(<[u8]>::to_vec);
        self.with_reader(move |reader| {
            let dbs = match db {
                Some(db) => vec![db],
                None => {
                    let mut dbs = reader.databases();
                    let read = reader.store().begin_read().map_err(NodeError::engine)?;
                    for db in registered(&read).map_err(NodeError::engine)? {
                        if dbs.contains(&db) {
                            continue;
                        }
                        let (purging, removed) =
                            reader.module_removals(&db).map_err(NodeError::failure)?;
                        if !purging.is_empty() || !removed.is_empty() {
                            dbs.push(db);
                        }
                    }
                    dbs.sort();
                    dbs
                }
            };
            dbs.into_iter()
                .map(|db| {
                    let modules = reader.modules(&db).map_err(NodeError::failure)?;
                    let (purging, removed) =
                        reader.module_removals(&db).map_err(NodeError::failure)?;
                    Ok(ModuleListing {
                        db,
                        modules,
                        purging: purging.into_iter().collect(),
                        removed,
                    })
                })
                .collect()
        })
        .await
    }
}
