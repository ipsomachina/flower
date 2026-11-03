//! Operator calls over groups, named partitions, moves and cross-group
//! transactions (admin credentials).
//!
//! Every change names an operation (`op`): repeating a call with the
//! same operation answers what the first did, so an uncertain call is
//! retried safely. A refusal the catalog's state makes (an existing
//! name, a move past its cutover) is a [`FlowerError::Status`] 409, one
//! naming something unlisted a 404, with the catalog's code.

use serde::{Deserialize, Serialize};

use crate::client::{FlowerClient, Method, RequestOptions};
use crate::error::FlowerError;

/// One group as the catalog lists it, with the answering member's view.
#[derive(Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct GroupInfo {
    /// Its name.
    pub name: String,
    /// Its uid, 16 hex digits (`0000000000000000` for the root).
    pub uid: String,
    /// Its founding members (the root's: every root member).
    pub members: Vec<u64>,
    /// `creating`, `initializing`, `active` or `removed`.
    pub status: String,
    /// The member that founded it, once one claimed it.
    #[serde(default)]
    pub founder: Option<u64>,
    /// Whether the answering member hosts a replica.
    #[serde(default)]
    pub hosted: bool,
    /// The leader the answering member knows, when it hosts a replica.
    #[serde(default)]
    pub leader: Option<u64>,
    /// Desired voter; absent when placement is disabled for the group.
    #[serde(default)]
    pub preferred_leader: Option<u64>,
}

/// Every group.
#[derive(Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Groups {
    /// The catalog revision read.
    pub revision: u64,
    /// The answering member.
    pub node: u64,
    /// The groups, the root first.
    pub groups: Vec<GroupInfo>,
}

/// One move of a partition.
#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq)]
pub struct MoveInfo {
    /// The operation that began it.
    pub op: String,
    /// The partition.
    pub partition: String,
    /// Where it comes from.
    pub source: String,
    /// Where it goes.
    pub destination: String,
    /// The epoch the source held it at.
    pub source_epoch: u64,
    /// The epoch the destination activates it at.
    pub epoch: u64,
    /// `copying`, `freezing`, `importing`, `activating`, `retiring`,
    /// `complete`, `aborting`, `abandoning` or `aborted`.
    pub phase: String,
    /// Why it was given up.
    #[serde(default)]
    pub reason: Option<String>,
    /// When it began, ms since the epoch.
    pub started_ms: u64,
    /// When it last changed phase.
    pub updated_ms: u64,
}

/// One named partition.
#[derive(Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct PartitionInfo {
    /// Its name: its database's.
    pub name: String,
    /// The group serving it.
    pub owner: String,
    /// Its ownership epoch.
    pub epoch: u64,
    /// `creating`, `active`, `moving`, `retiring` or `retired`.
    pub status: String,
    /// The operation that last changed it.
    pub op: String,
    /// Its move in flight.
    #[serde(default)]
    pub movement: Option<MoveInfo>,
}

/// Every partition and the recent moves.
#[derive(Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Partitions {
    /// The catalog revision read.
    pub revision: u64,
    /// The partitions, by name.
    pub partitions: Vec<PartitionInfo>,
    /// The last finished moves, oldest first.
    pub moves: Vec<MoveInfo>,
}

/// A catalog change's answer.
#[derive(Deserialize)]
struct Changed {
    value: serde_json::Value,
}

impl FlowerClient {
    /// Balance partition counts across active `groups`. Persist `op` before
    /// sending; retries recover the immutable plan and never plan new moves.
    pub async fn resize_partitions(
        &self,
        groups: &[String],
        op: &str,
        options: &RequestOptions,
    ) -> Result<serde_json::Value, FlowerError> {
        self.catalog_change(
            "/v1/admin/partitions",
            serde_json::json!({"operation":"resize", "groups":groups, "op":op}),
            options,
        )
        .await
    }

    /// Persist a preferred leader, or clear the preference with `None`.
    pub async fn prefer_group_leader(
        &self,
        group: &str,
        node: Option<u64>,
        op: &str,
        options: &RequestOptions,
    ) -> Result<serde_json::Value, FlowerError> {
        self.catalog_change(
            "/v1/admin/groups",
            serde_json::json!({"operation":"prefer_leader", "group":group, "node":node, "op":op}),
            options,
        )
        .await
    }

    async fn catalog_change(
        &self,
        path: &str,
        body: serde_json::Value,
        options: &RequestOptions,
    ) -> Result<serde_json::Value, FlowerError> {
        let body = self.send(Method::Post, path, Some(&body), options).await?;
        serde_json::from_slice::<Changed>(&body)
            .map(|changed| changed.value)
            .map_err(|error| FlowerError::Decode(error.to_string()))
    }

    /// Every group, as the catalog lists it.
    pub async fn groups(&self, options: &RequestOptions) -> Result<Groups, FlowerError> {
        let body = self
            .send(Method::Get, "/v1/admin/groups", None, options)
            .await?;
        serde_json::from_slice(&body).map_err(|error| FlowerError::Decode(error.to_string()))
    }

    /// List a group founded on `members` (root members, by node ID).
    pub async fn create_group(
        &self,
        group: &str,
        members: &[u64],
        op: Option<&str>,
        options: &RequestOptions,
    ) -> Result<serde_json::Value, FlowerError> {
        self.catalog_change(
            "/v1/admin/groups",
            serde_json::json!({"operation": "create", "group": group, "members": members, "op": op}),
            options,
        )
        .await
    }

    /// Remove a group that owns nothing.
    pub async fn remove_group(
        &self,
        group: &str,
        op: Option<&str>,
        options: &RequestOptions,
    ) -> Result<serde_json::Value, FlowerError> {
        self.catalog_change(
            "/v1/admin/groups",
            serde_json::json!({"operation": "remove", "group": group, "op": op}),
            options,
        )
        .await
    }

    /// Every partition and the recent moves.
    pub async fn partitions(&self, options: &RequestOptions) -> Result<Partitions, FlowerError> {
        let body = self
            .send(Method::Get, "/v1/admin/partitions", None, options)
            .await?;
        serde_json::from_slice(&body).map_err(|error| FlowerError::Decode(error.to_string()))
    }

    /// Place partition `partition` in `group` (an existing root
    /// database is adopted in place).
    pub async fn create_partition(
        &self,
        partition: &str,
        group: &str,
        op: Option<&str>,
        options: &RequestOptions,
    ) -> Result<serde_json::Value, FlowerError> {
        self.partition_change("create", partition, Some(group), op, None, options)
            .await
    }

    /// Retire a partition where it is: its rows go, its name stays taken.
    pub async fn retire_partition(
        &self,
        partition: &str,
        op: Option<&str>,
        options: &RequestOptions,
    ) -> Result<serde_json::Value, FlowerError> {
        self.partition_change("retire", partition, None, op, None, options)
            .await
    }

    /// Begin moving a partition to `group`; the move runs on the root's
    /// leader (follow it with [`FlowerClient::partitions`]).
    pub async fn move_partition(
        &self,
        partition: &str,
        group: &str,
        op: Option<&str>,
        options: &RequestOptions,
    ) -> Result<serde_json::Value, FlowerError> {
        self.partition_change("move", partition, Some(group), op, None, options)
            .await
    }

    /// Give up move `op` of a partition before its cutover.
    pub async fn abort_move(
        &self,
        partition: &str,
        op: &str,
        reason: Option<&str>,
        options: &RequestOptions,
    ) -> Result<serde_json::Value, FlowerError> {
        self.partition_change("abort", partition, None, Some(op), reason, options)
            .await
    }

    async fn partition_change(
        &self,
        operation: &str,
        partition: &str,
        group: Option<&str>,
        op: Option<&str>,
        reason: Option<&str>,
        options: &RequestOptions,
    ) -> Result<serde_json::Value, FlowerError> {
        self.catalog_change(
            "/v1/admin/partitions",
            serde_json::json!({
                "operation": operation,
                "partition": partition,
                "group": group,
                "op": op,
                "reason": reason,
            }),
            options,
        )
        .await
    }

    /// Every group's cross-group transactions: the records each
    /// coordinates (in flight first) and the ones each holds prepared.
    pub async fn transactions(
        &self,
        options: &RequestOptions,
    ) -> Result<serde_json::Value, FlowerError> {
        let body = self
            .send(Method::Get, "/v1/admin/transactions", None, options)
            .await?;
        serde_json::from_slice(&body).map_err(|error| FlowerError::Decode(error.to_string()))
    }

    /// Abort an undecided cross-group transaction at its coordinator
    /// group (the root when `None`); a decided one keeps its decision.
    pub async fn abort_transaction(
        &self,
        transaction: &str,
        group: Option<&str>,
        options: &RequestOptions,
    ) -> Result<serde_json::Value, FlowerError> {
        self.catalog_change(
            "/v1/admin/transactions",
            serde_json::json!({"operation": "abort", "transaction": transaction, "group": group}),
            options,
        )
        .await
    }
}
