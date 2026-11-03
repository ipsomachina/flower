//! Membership and leadership operator calls (admin credentials).
//!
//! `GET /v1/admin/membership` reports the addressed member's view of
//! the voter sets, member addresses and the membership log ID;
//! `POST /v1/admin/membership` changes the voter set at the leader
//! after validating it, optionally against the log ID a plan was made
//! from; `POST /v1/admin/leadership` hands leadership to another voter.
//! Refused or uncertain changes answer HTTP 409 (`membership`,
//! `transfer_failed`): inspect before retrying.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::client::{FlowerClient, Method, RequestOptions, decode_ok};
use crate::error::FlowerError;

/// A Raft log position: the leader that wrote it and its index. Pass a
/// membership's back unchanged as [`MembershipChange::expected_log_id`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct LogId {
    /// The writing leader's term and node ID.
    pub leader_id: LeaderId,
    /// The position.
    pub index: u64,
}

/// A leader: its term and node ID.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct LeaderId {
    /// The term it led.
    pub term: u64,
    /// Its node ID.
    pub node_id: u64,
}

/// What members must share to form one group; `build` is diagnostic.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Compatibility {
    /// Peer protocol version.
    pub raft_wire: u32,
    /// Entry application version.
    pub state_machine: u32,
    /// Snapshot encoding version.
    pub snapshot_format: u32,
    /// Stored value encoding version.
    pub value_format: u32,
    /// The Wasm runtime validating deployed modules.
    pub guest_runtime: String,
    /// The package version.
    pub build: String,
}

/// A member's view of the group's membership.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MembershipView {
    /// The leader this member knows, if any.
    pub leader: Option<u64>,
    /// The log ID of the membership in effect.
    pub log_id: Option<LogId>,
    /// The last log ID this member applied.
    pub last_applied: Option<LogId>,
    /// The voter sets: one normally, two during a joint change.
    pub voter_configs: Vec<BTreeSet<u64>>,
    /// Every member, voter or learner, with its Raft address.
    pub nodes: BTreeMap<u64, String>,
    /// The member's build compatibility.
    pub compatibility: Compatibility,
}

/// A requested membership change.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MembershipChange {
    /// The complete desired voter set: node ID to Raft address (empty
    /// for in-process clusters).
    pub members: BTreeMap<u64, String>,
    /// Refuse the change if membership moved since this log ID.
    pub expected_log_id: Option<LogId>,
    /// Keep removed voters as learners.
    pub retain: bool,
}

/// What a leadership transfer did.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransferOutcome {
    /// The member that led before.
    pub from: u64,
    /// The member that leads now.
    pub to: u64,
    /// The term `to` leads.
    pub term: u64,
    /// Whether `to` already led: nothing was done.
    pub unchanged: bool,
    /// How long the transfer took, in milliseconds.
    pub elapsed_ms: u64,
}

/// The `membership` field of an `ok` envelope.
#[derive(Deserialize)]
struct MembershipBody {
    membership: MembershipView,
}

/// The `transfer` field of an `ok` envelope.
#[derive(Deserialize)]
struct TransferBody {
    transfer: TransferOutcome,
}

impl FlowerClient {
    /// The addressed member's view of the membership: local status,
    /// not a linearizable read.
    pub async fn membership_view(
        &self,
        options: &RequestOptions,
    ) -> Result<MembershipView, FlowerError> {
        let body = self
            .send(Method::Get, "/v1/admin/membership", None, options)
            .await?;
        decode_ok::<MembershipBody>(&body).map(|body| body.membership)
    }

    /// Change the voter set at the leader, validated, returning the
    /// committed membership.
    pub async fn change_membership(
        &self,
        change: &MembershipChange,
        options: &RequestOptions,
    ) -> Result<MembershipView, FlowerError> {
        let members: BTreeMap<String, &String> = change
            .members
            .iter()
            .map(|(id, addr)| (id.to_string(), addr))
            .collect();
        let request = serde_json::json!({
            "members": members,
            "expected_log_id": change.expected_log_id,
            "retain": change.retain,
        });
        let body = self
            .send(
                Method::Post,
                "/v1/admin/membership",
                Some(&request),
                options,
            )
            .await?;
        decode_ok::<MembershipBody>(&body).map(|body| body.membership)
    }

    /// Hand leadership to the voter `to`, at the leader.
    pub async fn transfer_leadership(
        &self,
        to: u64,
        options: &RequestOptions,
    ) -> Result<TransferOutcome, FlowerError> {
        let body = self
            .send(
                Method::Post,
                "/v1/admin/leadership",
                Some(&serde_json::json!({"to": to})),
                options,
            )
            .await?;
        decode_ok::<TransferBody>(&body).map(|body| body.transfer)
    }
}
