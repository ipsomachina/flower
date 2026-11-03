//! Rejoin of a member that opened without Raft state.
//!
//! A member whose directory comes back empty (a replaced disk, a wiped
//! volume) may be new, or may be a member that lost what it
//! acknowledged: nothing on disk tells them apart, so it is treated as
//! the latter until it proves otherwise. It forgot the votes it granted
//! and the entries it acknowledged, which other members may have
//! counted toward a quorum. Until it has caught up with its group it
//! therefore
//!
//! - grants no vote, except to a candidate bootstrapping a new cluster
//!   (whose log ends at the initial membership entry, index 0): with its
//!   acknowledgement of a committed entry gone, it could otherwise elect
//!   a candidate that lacks the entry (OpenRaft's FAQ, "Can I wipe out
//!   the data of ONE node");
//! - starts no election of its own, and takes no leadership transfer;
//! - serves no sync batches, and reports itself unready.
//!
//! Opening such a directory records a `rejoining` mark in the Raft file
//! before the Raft core starts, so a restart before it has caught up
//! (by then it holds its leader's vote, maybe part of a snapshot) does
//! not make it an ordinary voter. Its leader replicates the log or a
//! snapshot to it as to any lagging follower (`loosen-follower-log-revert`
//! lets the leader restart replication when a follower's log goes back).
//! A background task asks that leader for a read fence: the leader
//! confirms its leadership through a quorum and answers its read log
//! ID, and once this member has applied that entry it holds everything
//! its group committed before it came back. The mark then clears and it
//! votes and campaigns like any member. Bootstrap ([`Node::initialize`])
//! makes a member the founder of a new cluster, which ends it too.
//!
//! What an empty disk cannot give back is a vote granted in the term
//! the data was lost in: the gate closes the data-loss path (electing a
//! member without a committed entry), not a same-term double vote,
//! which needs an election racing the loss itself.
//!
//! [`Node::initialize`]: crate::node::Node::initialize

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use openraft::raft::{Raft, VoteRequest, VoteResponse};

use crate::logstore::LogStore;
use crate::network::Peers;
use crate::types::{NodeId, TypeConfig};

/// Failure code of what a rejoining member refuses to serve: retryable
/// (HTTP 503) at another member.
pub const REJOINING: &str = "rejoining";

/// How long one catch-up attempt waits for the leader's fence and for
/// this member to apply it.
const FENCE_TIMEOUT: Duration = Duration::from_secs(10);

/// Whether this member is still catching up after opening without Raft
/// state.
#[derive(Debug, Default)]
pub struct Rejoin {
    amnesiac: AtomicBool,
}

impl Rejoin {
    /// Rejoin state for a member that opened `amnesiac` or not.
    pub fn new(amnesiac: bool) -> Arc<Self> {
        Arc::new(Rejoin {
            amnesiac: AtomicBool::new(amnesiac),
        })
    }

    /// Whether this member opened without Raft state and has not caught
    /// up with its group yet.
    pub fn amnesiac(&self) -> bool {
        self.amnesiac.load(Ordering::Acquire)
    }

    /// The refusal an amnesiac member answers `request` with, if it must
    /// refuse it. A candidate whose log ends at the initial membership
    /// entry bootstraps a new cluster: there is nothing to lose yet.
    pub fn withheld_vote(
        &self,
        raft: &Raft<TypeConfig>,
        request: &VoteRequest<NodeId>,
    ) -> Option<VoteResponse<NodeId>> {
        if !self.amnesiac() || request.last_log_id.is_none_or(|log| log.index == 0) {
            return None;
        }
        let metrics = raft.metrics().borrow().clone();
        tracing::info!(
            candidate = %request.vote,
            "refusing a vote: this member opened without Raft state and has not caught up"
        );
        Some(VoteResponse::new(metrics.vote, metrics.last_applied, false))
    }

    /// End the amnesia: this member caught up, or founded a cluster.
    /// Lets the member campaign again, then clears the durable mark off
    /// the async workers. Losing the mark's removal (a crash first, a
    /// failing disk) only makes the next start catch up again: never
    /// the other way round. Whether this call ended it.
    pub(crate) async fn settle(&self, raft: &Raft<TypeConfig>, logs: &LogStore) -> bool {
        if !self.amnesiac.swap(false, Ordering::AcqRel) {
            return false;
        }
        raft.runtime_config().elect(true);
        let logs = logs.clone();
        let cleared = tokio::task::spawn_blocking(move || logs.set_rejoining(false)).await;
        if !matches!(cleared, Ok(Ok(()))) {
            tracing::warn!(
                ?cleared,
                "could not clear the rejoining mark; this member will catch up again when it restarts"
            );
        }
        true
    }
}

/// Catch an amnesiac member up in the background: ask its leader for a
/// read fence until it has applied one. Ends once it caught up, when
/// the Raft core stops, or when `lifetime` closes (its node dropped);
/// it holds the Raft file until then.
pub(crate) fn drive(
    id: NodeId,
    raft: Raft<TypeConfig>,
    rejoin: Arc<Rejoin>,
    logs: LogStore,
    peers: Peers,
    mut lifetime: tokio::sync::watch::Receiver<()>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        tokio::select! {
            () = catch_up(id, &raft, &rejoin, &logs, &peers) => {}
            _ = lifetime.changed() => {}
        }
    })
}

/// The catch-up loop behind [`drive`].
async fn catch_up(
    id: NodeId,
    raft: &Raft<TypeConfig>,
    rejoin: &Rejoin,
    logs: &LogStore,
    peers: &Peers,
) {
    // A failed fence waits one RPC deadline (500 ms by default), not a
    // 50 ms heartbeat, before asking again.
    let retry = raft.config().rpc_timeout();
    let mut metrics = raft.metrics();
    while rejoin.amnesiac() {
        let leader = {
            let current = metrics.borrow_and_update();
            if current.running_state.is_err() {
                return;
            }
            current.current_leader.and_then(|leader| {
                current
                    .membership_config
                    .nodes()
                    .find(|(member, _)| **member == leader)
                    .map(|(_, node)| (leader, node.addr.clone()))
            })
        };
        // Until a leader replicates the membership (a snapshot, for an
        // empty member) there is nobody to ask.
        let Some((leader, addr)) = leader.filter(|(leader, _)| *leader != id) else {
            if metrics.changed().await.is_err() {
                return;
            }
            continue;
        };
        match peers.fence(leader, &addr, FENCE_TIMEOUT).await {
            Ok(fence) => {
                let target = fence.map(|log| log.index);
                let applied = tokio::time::timeout(
                    FENCE_TIMEOUT,
                    metrics.wait_for(|current| {
                        current.running_state.is_err()
                            || current.last_applied.map(|log| log.index) >= target
                    }),
                )
                .await
                .is_ok_and(|current| current.is_ok_and(|current| current.running_state.is_ok()));
                if applied && rejoin.settle(raft, logs).await {
                    tracing::info!(
                        id,
                        "caught up after opening without Raft state: voting and serving"
                    );
                    return;
                }
            }
            Err(error) => tracing::debug!(id, leader, %error, "rejoin fence failed"),
        }
        tokio::time::sleep(retry).await;
    }
}
