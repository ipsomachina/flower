//! Replicated queue claims and reports, and timer maintenance.
//!
//! Claims, renewals, reports and operator maintenance change `$queue`
//! and `$timer` records, which are replicated state: snapshots carry
//! them and every member must agree on them. So each operation commits
//! through Raft like a mutation. The leader plans the record writes
//! against its applied state with the engine's `plan_*` functions and
//! proposes them as an ordinary engine patch; every member, and every
//! synced device, applies the same bytes at the same log position.
//! Leader-chosen values (claim tokens and the clock reading behind
//! lease expiries and due times) travel inside the records and are
//! never recomputed at apply, so apply is deterministic and claims
//! survive failover.
//!
//! Queue patches carry no revision guard: the node serializes
//! operations per queue from planning through commit, after a
//! linearizable barrier, and every write of the group, guest mutations
//! claiming or reporting through the queue host operations included,
//! prepares in the writer's one queue on the state the writes before it
//! staged, so no two plans ever take or settle the same item. A
//! revision guard would make every unrelated mutation on the database
//! fail queue work as stale. Timer patches are revision-guarded at the revision their plan
//! read, because guest mutations reschedule and cancel timers
//! concurrently; a stale timer patch plans again.
//!
//! The record layout belongs to the engine: this module never looks
//! inside `$queue` or `$timer` records.

use flower_engine::engine::Engine;
use flower_engine::json::render;
use flower_engine::patch::{Patch, Planned, RecordWrite, encode_patch};
use flower_engine::work_queue::{ClaimRequest, ClaimedItem, MAX_POLL_LIMIT, Report, new_token};
use flower_sdk::{Failure, Value};

/// One worker or operator queue operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum QueueOp {
    /// Claim up to `limit` due items under `lease_ms` leases.
    Poll {
        /// Most items to claim.
        limit: usize,
        /// Lease length.
        lease_ms: u64,
    },
    /// Claim up to `limit` claimable items of one scope, lowest
    /// priority first, under `lease_ms` leases recording `owner`.
    Claim {
        /// The scope to claim from (`""` is the default scope).
        scope: String,
        /// The claimer, recorded on each lease when given.
        owner: Option<String>,
        /// Most items to claim.
        limit: usize,
        /// Lease length.
        lease_ms: u64,
        /// Wait in the scope's line this long when the claim comes back
        /// short (needs an owner).
        wait_ms: Option<u64>,
    },
    /// Renew a claim's lease.
    Heartbeat {
        /// Item ID.
        id: String,
        /// Claim token.
        token: String,
        /// New lease length.
        lease_ms: u64,
    },
    /// Finish a claim, deleting the item.
    Complete {
        /// Item ID.
        id: String,
        /// Claim token.
        token: String,
    },
    /// Finish a claim, keeping the item as completed with a result.
    CompleteWith {
        /// Item ID.
        id: String,
        /// Claim token.
        token: String,
        /// The result, as canonical JSON.
        result: Vec<u8>,
    },
    /// Requeue after a delay, or die past the attempt budget.
    Fail {
        /// Item ID.
        id: String,
        /// Claim token.
        token: String,
        /// Redelivery delay.
        delay_ms: u64,
    },
    /// Move a claim to the dead letter file now.
    Kill {
        /// Item ID.
        id: String,
        /// Claim token.
        token: String,
    },
    /// Return a claim to pending without spending an attempt.
    Release {
        /// Item ID.
        id: String,
        /// Claim token.
        token: String,
        /// Redelivery delay.
        delay_ms: u64,
    },
    /// Delete an item in any state.
    Cancel {
        /// Item ID.
        id: String,
    },
    /// Requeue a dead item with a fresh budget.
    Retry {
        /// Item ID.
        id: String,
        /// Redelivery delay.
        delay_ms: u64,
    },
    /// Delete dead items enqueued before a cutoff.
    Purge {
        /// Cutoff in ms since epoch.
        older_than_ms: u64,
    },
    /// Settle a claim and claim the claimer's next items over the state
    /// the report leaves, in one commit (v1's chained claims). A refused
    /// report claims nothing.
    Chained {
        /// Item ID.
        id: String,
        /// Claim token.
        token: String,
        /// How the report settles the claim.
        report: Settle,
        /// The claim that follows it.
        next: NextClaim,
    },
}

/// How a chained report settles its claim.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Settle {
    /// Complete it: kept with the result (canonical JSON) when given,
    /// deleted otherwise.
    Complete {
        /// The result, as canonical JSON.
        result: Option<Vec<u8>>,
    },
    /// Requeue after a delay, or die past the attempt budget.
    Fail {
        /// Redelivery delay.
        delay_ms: u64,
    },
    /// Move it to the dead letter file now.
    Kill,
}

/// The claim a chained report makes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NextClaim {
    /// The scope to claim from (`""` is the default scope).
    pub scope: String,
    /// The claimer, recorded on each lease when given.
    pub owner: Option<String>,
    /// Most items to claim.
    pub limit: usize,
    /// Lease length.
    pub lease_ms: u64,
    /// Wait in the scope's line this long when the claim comes back
    /// short (needs an owner).
    pub wait_ms: Option<u64>,
}

/// A committed queue operation's outcome.
#[derive(Clone, Debug)]
pub enum QueueOutcome {
    /// Items claimed by a poll, due-first.
    Claimed(Vec<ClaimedItem>),
    /// A report or renewal took effect.
    Done,
    /// Whether a cancel removed an item.
    Removed(bool),
    /// Items a purge removed.
    Purged(usize),
}

/// One operator timer operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TimerOp {
    /// Remove a timer.
    Cancel {
        /// Timer ID.
        id: String,
    },
    /// Requeue a dead timer with a fresh budget.
    Retry {
        /// Timer ID.
        id: String,
        /// Delay before it is due.
        delay_ms: u64,
    },
    /// Delete dead timers scheduled before a cutoff.
    Purge {
        /// Cutoff in ms since epoch.
        older_than_ms: u64,
    },
}

/// A committed timer operation's outcome.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TimerOutcome {
    /// A retry took effect.
    Done,
    /// Whether a cancel removed a timer.
    Removed(bool),
    /// Timers a purge removed.
    Purged(usize),
}

/// A prepared operation: the patch to propose (none when nothing
/// changes) and the outcome to report once it commits.
pub(crate) struct Prepared<T> {
    /// Encoded engine patch bytes.
    pub patch: Option<Vec<u8>>,
    /// Outcome reported after commit.
    pub outcome: T,
}

impl<T> Prepared<T> {
    /// Encode a plan as a patch, mapping its outcome; `guarded` pins the
    /// patch to the revision the plan read.
    fn from_plan<P>(
        db: &[u8],
        planned: Planned<P>,
        guarded: bool,
        outcome: impl FnOnce(P) -> T,
    ) -> Result<Self, Failure> {
        let Planned {
            outcome: planned,
            writes,
            revision,
        } = planned;
        let expected = guarded.then_some(revision);
        Ok(Prepared {
            patch: into_patch(db, writes, revision, expected)?,
            outcome: outcome(planned),
        })
    }
}

/// Encode planned writes as a patch; `None` when nothing changes. The
/// patch pins no module versions: record maintenance runs no guest code.
fn into_patch(
    db: &[u8],
    records: Vec<RecordWrite>,
    base_revision: u64,
    expected_revision: Option<u64>,
) -> Result<Option<Vec<u8>>, Failure> {
    if records.is_empty() {
        return Ok(None);
    }
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
        expected_revision,
        base_revision,
    };
    Ok(Some(encode_patch(&patch).map_err(Failure::from)?))
}

/// Prepare one queue operation at the leader's clock reading `now_ms`:
/// plan it, then encode an unguarded patch (see the module docs).
/// Business failures (bad tokens, unknown items) return `Err`.
pub(crate) fn prepare_queue(
    engine: &Engine,
    db: &[u8],
    queue: &str,
    op: &QueueOp,
    now_ms: u64,
) -> Result<Prepared<QueueOutcome>, Failure> {
    // A database that does not serve here (frozen, moving, moved)
    // answers so, rather than from rows mid-move; applies check again.
    let read = engine.snapshot().map_err(Failure::from)?;
    if let Some(refusal) =
        flower_engine::placement::read_refusal(&read, db).map_err(Failure::from)?
    {
        return Err(refusal);
    }
    drop(read);
    let done = |()| QueueOutcome::Done;
    match op {
        QueueOp::Poll { limit, lease_ms } => {
            let tokens: Vec<String> = (0..(*limit).min(MAX_POLL_LIMIT))
                .map(|_| new_token())
                .collect();
            let planned = engine.plan_poll_queue(db, queue, *limit, *lease_ms, now_ms, &tokens)?;
            Prepared::from_plan(db, planned, false, QueueOutcome::Claimed)
        }
        QueueOp::Claim {
            scope,
            owner,
            limit,
            lease_ms,
            wait_ms,
        } => {
            let tokens: Vec<String> = (0..(*limit).min(MAX_POLL_LIMIT))
                .map(|_| new_token())
                .collect();
            let request = ClaimRequest {
                scope,
                owner: owner.as_deref(),
                limit: *limit,
                lease_ms: *lease_ms,
                wait_ms: *wait_ms,
            };
            let planned = engine.plan_claim_queue(db, queue, &request, now_ms, &tokens)?;
            Prepared::from_plan(db, planned, false, QueueOutcome::Claimed)
        }
        QueueOp::Heartbeat {
            id,
            token,
            lease_ms,
        } => {
            let planned = engine.plan_heartbeat_queue(db, queue, id, token, *lease_ms, now_ms)?;
            Prepared::from_plan(db, planned, false, done)
        }
        QueueOp::Complete { id, token } => {
            let planned = engine.plan_complete_queue(db, queue, id, token, now_ms)?;
            Prepared::from_plan(db, planned, false, done)
        }
        QueueOp::CompleteWith { id, token, result } => {
            let result = flower_engine::json::parse_value(result).map_err(Failure::from)?;
            let planned =
                engine.plan_complete_queue_with(db, queue, id, token, Some(&result), now_ms)?;
            Prepared::from_plan(db, planned, false, done)
        }
        QueueOp::Fail {
            id,
            token,
            delay_ms,
        } => {
            let planned = engine.plan_fail_queue(db, queue, id, token, now_ms, *delay_ms)?;
            Prepared::from_plan(db, planned, false, done)
        }
        QueueOp::Kill { id, token } => {
            let planned = engine.plan_kill_queue(db, queue, id, token, now_ms)?;
            Prepared::from_plan(db, planned, false, done)
        }
        QueueOp::Release {
            id,
            token,
            delay_ms,
        } => {
            let planned = engine.plan_release_queue(db, queue, id, token, now_ms, *delay_ms)?;
            Prepared::from_plan(db, planned, false, done)
        }
        QueueOp::Cancel { id } => {
            let planned = engine.plan_cancel_queue(db, queue, id)?;
            Prepared::from_plan(db, planned, false, QueueOutcome::Removed)
        }
        QueueOp::Retry { id, delay_ms } => {
            let planned = engine.plan_retry_queue(db, queue, id, now_ms, *delay_ms)?;
            Prepared::from_plan(db, planned, false, done)
        }
        QueueOp::Purge { older_than_ms } => {
            let planned = engine.plan_purge_queue(db, queue, *older_than_ms)?;
            Prepared::from_plan(db, planned, false, QueueOutcome::Purged)
        }
        QueueOp::Chained {
            id,
            token,
            report,
            next,
        } => {
            let result = match report {
                Settle::Complete {
                    result: Some(result),
                } => Some(flower_engine::json::parse_value(result).map_err(Failure::from)?),
                _ => None,
            };
            let report = match report {
                Settle::Complete { .. } => Report::Complete(result.as_ref()),
                Settle::Fail { delay_ms } => Report::Fail(*delay_ms),
                Settle::Kill => Report::Kill,
            };
            let tokens: Vec<String> = (0..next.limit.min(MAX_POLL_LIMIT))
                .map(|_| new_token())
                .collect();
            let request = ClaimRequest {
                scope: &next.scope,
                owner: next.owner.as_deref(),
                limit: next.limit,
                lease_ms: next.lease_ms,
                wait_ms: next.wait_ms,
            };
            let planned = engine
                .plan_report_and_claim(db, queue, id, token, report, &request, now_ms, &tokens)?;
            Prepared::from_plan(db, planned, false, QueueOutcome::Claimed)
        }
    }
}

/// Prepare one timer operation at the leader's clock reading `now_ms`,
/// guarded by the revision its plan read: any write landing in between
/// makes the patch stale, never wrong.
pub(crate) fn prepare_timer(
    engine: &Engine,
    db: &[u8],
    op: &TimerOp,
    now_ms: u64,
) -> Result<Prepared<TimerOutcome>, Failure> {
    match op {
        TimerOp::Cancel { id } => {
            let planned = engine.plan_cancel_timer(db, id)?;
            Prepared::from_plan(db, planned, true, TimerOutcome::Removed)
        }
        TimerOp::Retry { id, delay_ms } => {
            let planned = engine.plan_retry_timer(db, id, now_ms, *delay_ms)?;
            Prepared::from_plan(db, planned, true, |()| TimerOutcome::Done)
        }
        TimerOp::Purge { older_than_ms } => {
            let planned = engine.plan_purge_timers(db, *older_than_ms)?;
            Prepared::from_plan(db, planned, true, TimerOutcome::Purged)
        }
    }
}
