//! Leadership transfer: hand a group's leadership to another voter.
//!
//! Elections put the leader wherever they happen to. A transfer moves
//! it on purpose, to drain a host before a restart or to spread leaders
//! over hosts, without the failover's election timeout and without a
//! window in which no member accepts writes for want of a leader.
//!
//! OpenRaft's voters refuse any candidate while their leader's lease
//! runs (`Config::leader_lease` in Flower's fork, 300 ms by default)
//! from the leader's last AppendEntries, and the leader itself for as
//! long after it began to lead. Flower's OpenRaft fork lets a member
//! release the lease of the leadership it follows
//! (`Raft::release_leader_lease`, as v1's fork does), so the leader
//!
//! 1. gives up its read lease for the rest of the term (voters that
//!    released theirs no longer vouch that no other leader exists; see
//!    [`crate::read`]) and stops proposing: new writes answer the
//!    retryable [`TRANSFERRING`], and writes already proposed finish
//!    first;
//! 2. waits, a lease and an election timeout at most, until the target
//!    holds its whole log, heartbeating every voter meanwhile;
//! 3. releases its own lease and asks every other voter to release
//!    theirs, so they grant the target's vote at once. Should too few
//!    of them confirm for the target to win (a voter unreachable, or
//!    deposed meanwhile), it sends AppendEntries and snapshots to the
//!    target alone until the others' leases run out, as the registry
//!    crate required: one lease. Their own elections wait a further
//!    election timeout, so the target campaigns first;
//! 4. asks the target to campaign: it does only if it follows this
//!    leader in this term, is a voter, holds the log and is not
//!    rejoining after opening without Raft state;
//! 5. waits, twice a lease and an election timeout at most, until the
//!    target leads.
//!
//! Raft's vote rules alone decide the election, so a lost message or a
//! crash costs time, never safety. A transfer that gives up leaves the
//! old leader replicating to everyone and proposing again, or, if the
//! target campaigned without winning, the group electing as after any
//! failure.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use openraft::ServerState;
use serde::{Deserialize, Serialize};
use tokio::time::Instant;

use super::{Node, NodeError};
use crate::network::{Peers, TransferRequest};
use crate::types::NodeId;

/// Failure code of a write refused while leadership moves: retryable
/// (HTTP 503), at the new leader once it leads.
pub const TRANSFERRING: &str = "leadership_transfer";

/// Failure code of a transfer that did not happen (HTTP 409).
pub const TRANSFER_FAILED: &str = "transfer_failed";

/// Failure code of a write refused, or left with an unknown outcome, by
/// a leader cut off from its quorum (see [`isolation_window`]): retryable (HTTP
/// 503) with the same idempotency key, at another member, which
/// forwards it to whichever leader the others elected.
pub const ISOLATED: &str = "leader_isolated";

/// Whether these metrics show this member leading without a quorum's
/// acknowledgement for longer than `window` ([`isolation_window`]): a
/// partition or its peers' failure cut it off, the others may have
/// elected another leader by now, and its writes may never commit.
/// OpenRaft 0.9 keeps such a leader leading until it hears a higher
/// term.
pub(crate) fn cut_off(
    metrics: &openraft::RaftMetrics<NodeId, openraft::BasicNode>,
    window: Duration,
) -> bool {
    metrics.state == ServerState::Leader
        && metrics
            .millis_since_quorum_ack
            .is_some_and(|ms| Duration::from_millis(ms) > window)
}

/// How long a leader may go without a quorum's acknowledgement before
/// it counts as [`cut_off`] and gives writes up: its lease plus the
/// shortest election timeout (400 ms by default). A liveness bound, not
/// a safety one: nothing commits or is read on the strength of it. The
/// acknowledgement is dated when its request was sent; a follower whose
/// timer ran from that request asks to campaign only once its lease and
/// its election timeout passed, but one that stopped hearing this leader
/// earlier asks for a pre-vote every election timeout, and the others
/// grant it as soon as their leases ran out: another leader can exist
/// from the lease on. Giving writes up after the lease alone gave up
/// those of a leader merely slow under load: with one append in flight
/// per follower, a follower's flush of `d` makes the quorum's
/// acknowledgement up to `2d` old, so 150 ms flushes reached the 300 ms
/// lease. A leader truly cut off is still found within the window, plus
/// the metrics' reporting interval.
pub(crate) fn isolation_window(config: &openraft::Config) -> Duration {
    config.leader_lease() + Duration::from_millis(config.election_timeout_min)
}

/// The failure for a write at a member [`cut_off`] from its quorum.
pub(crate) fn isolated() -> NodeError {
    NodeError::coded(
        ISOLATED,
        "this leader heard from no quorum for a leader lease and an election timeout: the \
         write may not commit; retry with the same idempotency key, at another member",
    )
}

/// What a leadership transfer did.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransferOutcome {
    /// The member that led before.
    pub from: NodeId,
    /// The member that leads now.
    pub to: NodeId,
    /// The term `to` leads.
    pub term: u64,
    /// Whether `to` already led: nothing was done.
    pub unchanged: bool,
    /// How long the transfer took, in milliseconds.
    pub elapsed_ms: u64,
}

/// A member's leadership bookkeeping.
#[derive(Default)]
pub(crate) struct Leadership {
    /// The voter leadership is moving to, while a transfer runs.
    moving: Mutex<Option<NodeId>>,
    /// Proposals in flight hold this shared; a transfer takes it
    /// exclusively once to let them finish.
    proposals: Arc<tokio::sync::RwLock<()>>,
}

/// A proposal admitted past the transfer gate.
pub(crate) type Admitted<'a> = tokio::sync::RwLockReadGuard<'a, ()>;

/// A proposal admitted past the transfer gate, for a holder that
/// outlives the borrow: the writer keeps one per batch in flight.
pub(crate) type AdmittedOwned = tokio::sync::OwnedRwLockReadGuard<()>;

impl Leadership {
    /// Admit one proposal, or refuse it while leadership moves. Hold
    /// the guard until the proposal is in the log.
    pub(crate) async fn admit(&self) -> Result<Admitted<'_>, NodeError> {
        let admitted = self.proposals.read().await;
        self.unless_moving()?;
        Ok(admitted)
    }

    /// [`Leadership::admit`] with a guard that owns its share.
    pub(crate) async fn admit_owned(&self) -> Result<AdmittedOwned, NodeError> {
        let admitted = self.proposals.clone().read_owned().await;
        self.unless_moving()?;
        Ok(admitted)
    }

    /// Refuse while leadership moves.
    fn unless_moving(&self) -> Result<(), NodeError> {
        match *self.moving.lock().expect("leadership poisoned") {
            Some(to) => Err(NodeError::coded(
                TRANSFERRING,
                format!("leadership is moving to node {to}: retry"),
            )),
            None => Ok(()),
        }
    }
}

/// A transfer under way: proposals refused and replication held back
/// until it is dropped, whatever the outcome.
struct Moving<'a> {
    leadership: &'a Leadership,
    peers: &'a Peers,
}

impl Drop for Moving<'_> {
    fn drop(&mut self) {
        self.peers.withhold(None);
        *self.leadership.moving.lock().expect("leadership poisoned") = None;
    }
}

/// A transfer failure.
fn failed(message: impl ToString) -> NodeError {
    NodeError::coded(TRANSFER_FAILED, message)
}

impl Node {
    /// As leader, hand leadership to the voter `to` and wait until it
    /// leads (see the module documentation). Forwards when this member
    /// does not lead; [`TRANSFER_FAILED`] when `to` is not another voter,
    /// a joint membership change runs, or `to` does not catch up, take
    /// the transfer or win in time.
    pub async fn transfer_leadership(&self, to: NodeId) -> Result<TransferOutcome, NodeError> {
        let started = Instant::now();
        self.check_storage()?;
        // Not while a membership change runs: its voters are in flux.
        let _operation = self.membership_ops.lock().await;
        let metrics = self.raft.metrics().borrow().clone();
        if metrics.current_leader == Some(to) {
            return Ok(TransferOutcome {
                from: to,
                to,
                term: metrics.current_term,
                unchanged: true,
                elapsed_ms: 0,
            });
        }
        if metrics.current_leader != Some(self.id) || metrics.state != ServerState::Leader {
            return Err(NodeError::Forward {
                leader: metrics.current_leader,
            });
        }
        let term = metrics.current_term;
        let membership = metrics.membership_config.membership().clone();
        if membership.get_joint_config().len() != 1 {
            return Err(failed("a joint membership change is in progress"));
        }
        let voters: Vec<NodeId> = membership.voter_ids().collect();
        if !voters.contains(&to) {
            return Err(failed(format!("node {to} is not a voter of this group")));
        }
        let address = |id: NodeId| {
            membership
                .get_node(&id)
                .map(|node| node.addr.clone())
                .unwrap_or_default()
        };
        let config = self.raft.config();
        let lease = config.leader_lease();
        // Catching up, campaigning and winning get a lease and an election
        // timeout each.
        let patience = lease + Duration::from_millis(config.election_timeout_max);
        let rpc = config.rpc_timeout();
        let deadline = started + patience;

        // 1. Give up the read lease for this term: voters are about to
        // release theirs and the target campaigns at once, so the quorum
        // that confirmed the lease no longer vouches that no other
        // leader exists (see `crate::read`). Fresh reads here run quorum
        // rounds until the transfer settles, and confirm no new lease in
        // this term even if it fails. Then stop proposing; let proposals
        // already admitted finish.
        self.reads.surrender();
        *self.leadership.moving.lock().expect("leadership poisoned") = Some(to);
        let _moving = Moving {
            leadership: &self.leadership,
            peers: &self.peers,
        };
        let drained = tokio::time::timeout_at(deadline, self.leadership.proposals.write())
            .await
            .map_err(|_| failed("writes in flight did not finish in time"))?;
        drop(drained);

        // 2. The target needs the whole log to win.
        let last_log_index = self.raft.metrics().borrow().last_log_index;
        let mut watch = self.raft.metrics();
        let caught_up = tokio::time::timeout_at(
            deadline,
            watch.wait_for(|current| {
                current.current_leader != Some(self.id)
                    || current.replication.as_ref().is_some_and(|replication| {
                        replication
                            .get(&to)
                            .is_some_and(|matched| matched.map(|log| log.index) >= last_log_index)
                    })
            }),
        )
        .await
        .map_err(|_| failed(format!("node {to} did not catch up with the log in time")))?
        .map_err(|_| NodeError::Fatal("Raft stopped".to_string()))?
        .current_leader
            == Some(self.id);
        if !caught_up {
            return Err(failed("leadership changed during the transfer"));
        }

        // 3. Make a majority grant the target's vote at once: this
        // member and every other voter release this leadership's lease.
        let request = TransferRequest {
            from: self.id,
            term,
            last_log_index,
            to,
        };
        let own = self
            .raft
            .with_raft_state(|state| *state.vote_ref())
            .await
            .map_err(|error| NodeError::Fatal(error.to_string()))?;
        let released = own.leader_id().get_term() == term
            && self
                .raft
                .release_leader_lease(own)
                .await
                .map_err(|error| NodeError::Fatal(error.to_string()))?;
        if !released {
            return Err(failed("leadership changed during the transfer"));
        }
        let mut asked = tokio::task::JoinSet::new();
        for voter in voters.iter().copied() {
            if voter == self.id || voter == to {
                continue;
            }
            let (peers, addr, request) = (self.peers.clone(), address(voter), request.clone());
            asked.spawn(async move { (voter, peers.transfer(voter, &addr, request, rpc).await) });
        }
        // The target and this member grant it.
        let mut granting = 2;
        while let Some(answer) = asked.join_next().await {
            match answer {
                Ok((_, Ok(()))) => granting += 1,
                Ok((voter, Err(reason))) => {
                    tracing::info!(voter, %reason, "a voter kept its lease on a transferring leader");
                }
                Err(error) => tracing::warn!(%error, "a lease release did not finish"),
            }
        }
        if granting * 2 <= voters.len() {
            // Too few released: let the others' leases run out instead.
            self.peers.withhold(Some(to));
            tokio::time::sleep(lease + rpc).await;
        }

        // 4. Ask the target to campaign. One that loses a round (a vote
        // lost, a lease not quite released) campaigns again an election
        // timeout later.
        let deadline = Instant::now() + patience * 2;
        self.peers
            .transfer(to, &address(to), request, patience)
            .await
            .map_err(|reason| failed(format!("node {to} did not take the transfer: {reason}")))?;

        // 5. Wait until it leads.
        let mut watch = self.raft.metrics();
        let led = tokio::time::timeout_at(
            deadline,
            watch.wait_for(|current| {
                current
                    .current_leader
                    .is_some_and(|leader| leader != self.id)
            }),
        )
        .await
        .map_err(|_| failed(format!("node {to} did not win its election in time")))?
        .map_err(|_| NodeError::Fatal("Raft stopped".to_string()))?
        .clone();
        if led.current_leader != Some(to) {
            return Err(failed(format!(
                "node {:?} won the election instead",
                led.current_leader
            )));
        }
        Ok(TransferOutcome {
            from: self.id,
            to,
            term: led.current_term,
            unchanged: false,
            elapsed_ms: started.elapsed().as_millis() as u64,
        })
    }

    /// Whether a preferred voter is close enough for a planned transfer.
    /// The placement task requires this on two successive ticks.
    pub(crate) fn preferred_ready(&self, to: NodeId) -> bool {
        let metrics = self.raft.metrics().borrow().clone();
        metrics.current_leader == Some(self.id)
            && metrics.state == ServerState::Leader
            && to != self.id
            && metrics
                .membership_config
                .membership()
                .get_joint_config()
                .len()
                == 1
            && metrics
                .membership_config
                .membership()
                .voter_ids()
                .any(|id| id == to)
            && metrics
                .replication
                .as_ref()
                .and_then(|r| r.get(&to))
                .copied()
                .flatten()
                .is_some_and(|matched| {
                    matched.index.saturating_add(256) >= metrics.last_log_index.unwrap_or(0)
                })
    }

    /// As leader about to stop, hand leadership to another voter, so a
    /// planned restart costs a transfer (a release and a vote round
    /// trip) rather than a failover. Voters are tried in order of the
    /// log they hold, most first. `None` when this member does not lead
    /// or leads alone; the last failure when no voter took over.
    pub async fn hand_off_leadership(&self) -> Result<Option<TransferOutcome>, NodeError> {
        let metrics = self.raft.metrics().borrow().clone();
        if metrics.state != ServerState::Leader || metrics.current_leader != Some(self.id) {
            return Ok(None);
        }
        let mut voters: Vec<(Option<u64>, NodeId)> = metrics
            .membership_config
            .membership()
            .voter_ids()
            .filter(|voter| *voter != self.id)
            .map(|voter| {
                let matched = metrics
                    .replication
                    .as_ref()
                    .and_then(|replication| replication.get(&voter).copied().flatten())
                    .map(|log| log.index);
                (matched, voter)
            })
            .collect();
        voters.sort_by(|a, b| b.cmp(a));
        let mut last = None;
        for (_, voter) in voters {
            match self.transfer_leadership(voter).await {
                Ok(outcome) => return Ok(Some(outcome)),
                Err(NodeError::Forward { .. }) => return Ok(None),
                Err(error) => {
                    tracing::warn!(voter, %error, "leadership hand-off to a voter failed");
                    last = Some(error);
                }
            }
        }
        last.map_or(Ok(None), Err)
    }
}
