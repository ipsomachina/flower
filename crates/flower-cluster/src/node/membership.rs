//! Validated membership changes.
//!
//! Voter sets, member addresses and the membership log ID live in the
//! Raft log and snapshots. An operator sends the leader the complete
//! desired voter set; the leader
//!
//! 1. confirms through a quorum that it leads;
//! 2. checks an optional `expected_log_id` against the current
//!    membership's log ID, for optimistic concurrency, before changing
//!    anything;
//! 3. refuses a different target while a joint change is in progress;
//! 4. refuses to move an existing ID to another address (replace the
//!    member under a fresh ID) or to give one address two IDs;
//! 5. probes every proposed member over the peer transport: the hello
//!    proves the address answers as that node ID, the answer carries its
//!    compatibility contract, which must match, and a member new to the
//!    group must hold no Raft state, so this cannot combine clusters or
//!    silently reincorporate an old member's directory;
//! 6. registers the missing members as learners, captures a fence (a
//!    committed log position after their registration) and waits until
//!    every proposed voter holds it;
//! 7. commits the voter set through OpenRaft's joint consensus.
//!
//! Membership calls are serialized on the member and bounded by
//! fifteen seconds. A timeout, lost quorum or changed leader can leave
//! learners added, a joint configuration, or the final configuration
//! committed: inspect ([`Node::membership`]) before retrying.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use openraft::{BasicNode, LogId};
use serde::{Deserialize, Serialize};

use super::{Node, NodeError};
use crate::compat::{Compatibility, compatibility};
use crate::network::PeerInfo;
use crate::types::NodeId;

/// Failure code of a refused or uncertain membership change (HTTP 409).
pub const MEMBERSHIP: &str = "membership";

/// The longest a membership change runs, waiting for the serialization
/// slot included.
const MEMBERSHIP_TIMEOUT: Duration = Duration::from_secs(15);

/// The longest one member's probe takes.
const PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// A requested membership change.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MembershipChange {
    /// The complete desired voter set, by node ID, with each member's
    /// Raft address (empty for in-process clusters).
    pub members: BTreeMap<NodeId, String>,
    /// The membership log ID the change was planned against, from
    /// [`Node::membership`]: the change is refused if membership moved.
    #[serde(default)]
    pub expected_log_id: Option<LogId<NodeId>>,
    /// Keep removed voters as learners.
    #[serde(default)]
    pub retain: bool,
}

/// A member's view of the group's membership: local status, not a
/// linearizable read.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MembershipView {
    /// The leader this member knows, if any.
    pub leader: Option<NodeId>,
    /// The log ID of the membership in effect.
    pub log_id: Option<LogId<NodeId>>,
    /// The last log ID this member applied.
    pub last_applied: Option<LogId<NodeId>>,
    /// The voter sets: one normally, two during a joint change.
    pub voter_configs: Vec<BTreeSet<NodeId>>,
    /// Every member, voter or learner, with its Raft address.
    pub nodes: BTreeMap<NodeId, String>,
    /// This member's build compatibility.
    pub compatibility: Compatibility,
}

/// A refused membership change.
fn refused(message: impl ToString) -> NodeError {
    NodeError::coded(MEMBERSHIP, message)
}

/// Check a desired voter set before touching anything: nonempty,
/// positive IDs, `host:port` addresses (or none, in process), no
/// address twice.
fn validate_members(members: &BTreeMap<NodeId, String>) -> Result<(), NodeError> {
    if members.is_empty() {
        return Err(refused("the voter set must not be empty"));
    }
    let mut addresses = BTreeSet::new();
    for (id, addr) in members {
        if *id == 0 {
            return Err(refused("node IDs must be positive"));
        }
        if addr.is_empty() {
            continue;
        }
        let valid = addr.rsplit_once(':').is_some_and(|(host, port)| {
            !host.is_empty() && port.parse::<u16>().is_ok_and(|port| port > 0)
        });
        if !valid {
            return Err(refused(format!(
                "node {id}'s address {addr:?} is not host:port"
            )));
        }
        if !addresses.insert(addr) {
            return Err(refused(format!(
                "address {addr} is given to two nodes: each member needs its own"
            )));
        }
    }
    Ok(())
}

impl Node {
    /// This member's view of the group's membership.
    pub fn membership(&self) -> MembershipView {
        let metrics = self.raft.metrics().borrow().clone();
        let config = &metrics.membership_config;
        MembershipView {
            leader: metrics.current_leader,
            log_id: *config.log_id(),
            last_applied: metrics.last_applied,
            voter_configs: config.membership().get_joint_config().clone(),
            nodes: config
                .nodes()
                .map(|(id, node)| (*id, node.addr.clone()))
                .collect(),
            compatibility: compatibility().clone(),
        }
    }

    /// This member's identity, initialization and contract, as a probe
    /// over the peer transport reports them.
    pub async fn peer_info(&self) -> Result<PeerInfo, NodeError> {
        self.peer_service().info().await.map_err(NodeError::Fatal)
    }

    /// Change the voter set to `change.members` after validating it
    /// (see the module documentation), returning the committed
    /// membership. Forwards when this member does not lead.
    pub async fn reconfigure(&self, change: MembershipChange) -> Result<MembershipView, NodeError> {
        validate_members(&change.members)?;
        self.check_storage()?;
        match tokio::time::timeout(MEMBERSHIP_TIMEOUT, self.reconfigure_now(change)).await {
            Ok(outcome) => outcome,
            Err(_) => Err(refused(
                "the membership change timed out; it may have partly or fully committed: \
                 inspect membership before retrying",
            )),
        }
    }

    /// The body of [`Node::reconfigure`], serialized.
    async fn reconfigure_now(&self, change: MembershipChange) -> Result<MembershipView, NodeError> {
        let _operation = self.membership_ops.lock().await;
        self.raft
            .ensure_linearizable()
            .await
            .map_err(NodeError::linearizable)?;
        let initial = self.membership();
        if let Some(expected) = change.expected_log_id
            && initial.log_id != Some(expected)
        {
            return Err(refused(format!(
                "membership changed since {expected}: it is now at {}",
                initial
                    .log_id
                    .map_or("none".to_string(), |log| log.to_string())
            )));
        }
        let desired: BTreeSet<NodeId> = change.members.keys().copied().collect();
        if initial.voter_configs.len() > 1 && initial.voter_configs.last() != Some(&desired) {
            return Err(refused(
                "a joint membership change is in progress: finish its target voter set first",
            ));
        }
        for (id, addr) in &change.members {
            if let Some(previous) = initial.nodes.get(id)
                && previous != addr
            {
                return Err(refused(format!(
                    "node {id} is at {previous:?}: an existing ID cannot move to {addr:?}; \
                     add the member under a new ID and remove this one"
                )));
            }
            if let Some((other, _)) = initial
                .nodes
                .iter()
                .find(|(other, known)| *other != id && !addr.is_empty() && *known == addr)
            {
                return Err(refused(format!(
                    "address {addr} belongs to node {other}: start the new member at an address of its own"
                )));
            }
            let peer = if *id == self.id {
                self.peer_info().await?
            } else {
                self.peers
                    .info(*id, addr, PROBE_TIMEOUT)
                    .await
                    .map_err(|error| refused(format!("probe node {id} at {addr:?}: {error}")))?
            };
            if peer.id != *id {
                return Err(refused(format!(
                    "{addr:?} answers as node {}, not node {id}",
                    peer.id
                )));
            }
            if !compatibility().compatible_with(&peer.compatibility) {
                return Err(refused(format!(
                    "node {id} runs contract {} (build {}), this member {} (build {}): \
                     members must share the wire, state machine, snapshot, value and guest runtime versions",
                    peer.compatibility.contract(),
                    peer.compatibility.build,
                    compatibility().contract(),
                    compatibility().build
                )));
            }
            if !initial.nodes.contains_key(id) && peer.initialized {
                return Err(refused(format!(
                    "new node {id} already holds Raft state: start it with an empty directory, \
                     under an ID this group never had"
                )));
            }
        }
        // Register only the missing members: their committed learner
        // entries publish their addresses and start replication without
        // changing the voters' quorum.
        for (id, addr) in &change.members {
            if !initial.nodes.contains_key(id) {
                self.raft
                    .add_learner(*id, BasicNode::new(addr), false)
                    .await
                    .map_err(NodeError::write)?;
            }
        }
        // A committed position after the registrations: every proposed
        // voter must hold it before it counts toward a quorum. The
        // learner lag threshold is no such proof.
        let fence = self
            .raft
            .ensure_linearizable()
            .await
            .map_err(NodeError::linearizable)?
            .map(|log| log.index);
        let mut metrics = self.raft.metrics();
        let term = metrics.borrow().current_term;
        loop {
            {
                let current = metrics.borrow_and_update();
                if current.current_leader != Some(self.id) || current.current_term != term {
                    return Err(refused(
                        "leadership changed while new members caught up: inspect membership and \
                         retry at the current leader",
                    ));
                }
                let caught_up = desired.iter().all(|id| {
                    *id == self.id
                        || current
                            .replication
                            .as_ref()
                            .and_then(|replication| replication.get(id))
                            .is_some_and(|matched| matched.map(|log| log.index) >= fence)
                });
                if caught_up {
                    break;
                }
            }
            metrics
                .changed()
                .await
                .map_err(|_| NodeError::Fatal("Raft stopped".to_string()))?;
        }
        let response = self
            .raft
            .change_membership(desired, change.retain)
            .await
            .map_err(NodeError::write)?;
        let committed = response
            .membership
            .ok_or_else(|| refused("the membership response carries no configuration"))?;
        Ok(MembershipView {
            leader: self.raft.metrics().borrow().current_leader,
            log_id: Some(response.log_id),
            last_applied: Some(response.log_id),
            voter_configs: committed.get_joint_config().clone(),
            nodes: committed
                .nodes()
                .map(|(id, node)| (*id, node.addr.clone()))
                .collect(),
            compatibility: compatibility().clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn members(pairs: &[(NodeId, &str)]) -> BTreeMap<NodeId, String> {
        pairs
            .iter()
            .map(|(id, addr)| (*id, addr.to_string()))
            .collect()
    }

    #[test]
    fn voter_sets_need_positive_ids_and_distinct_host_port_addresses() {
        assert!(validate_members(&members(&[(1, "a:1"), (2, "b:2")])).is_ok());
        assert!(validate_members(&members(&[(1, ""), (2, "")])).is_ok());
        for bad in [
            members(&[]),
            members(&[(0, "a:1")]),
            members(&[(1, "a:1"), (2, "a:1")]),
            members(&[(1, "nohost")]),
            members(&[(1, ":7")]),
            members(&[(1, "a:port")]),
            members(&[(1, "a:0")]),
        ] {
            assert!(validate_members(&bad).is_err(), "{bad:?}");
        }
    }
}
