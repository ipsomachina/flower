//! Operator routes over the partition catalog (admin): groups and
//! named partitions.
//!
//! - `GET /v1/admin/groups`: every group the catalog lists (the root
//!   included), with what this process knows of each: whether it hosts
//!   a replica, and who leads.
//! - `POST /v1/admin/groups`: `create` a group with founding members,
//!   or `remove` one that owns nothing.
//! - `GET /v1/admin/partitions`: every partition, the moves in flight
//!   and the last finished ones.
//! - `POST /v1/admin/partitions`: `create` a partition in a group
//!   (adopting an existing root database in place), `retire` one,
//!   `move` one to another group, or `abort` a move before its cutover.
//! - `GET /v1/admin/transactions`: every group's cross-group
//!   transaction records, coordinated and prepared.
//! - `POST /v1/admin/transactions`: `abort` an undecided transaction at
//!   its coordinator group.
//!
//! Every change names an operation (`op`, by default the operation and
//! name: `create-east`); repeating it answers its outcome again.
//! Refusals answer 409 (the catalog's state does not allow it) or 404
//! (it names nothing listed) with the catalog's code.

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use flower_cluster::host::Host;
use flower_engine::catalog::{CATALOG_UNKNOWN, Catalog, CatalogChange};
use flower_engine::txn::TXN_UNKNOWN;
use flower_sdk::{Failure, Value};
use std::sync::Arc;

use crate::api::{ApiError, AppState, value_to_json};

/// The catalog routes.
pub(crate) fn routes() -> Router<AppState> {
    Router::new()
        .route("/v1/admin/groups", get(get_groups).post(post_groups))
        .route(
            "/v1/admin/partitions",
            get(get_partitions).post(post_partitions),
        )
        .route(
            "/v1/admin/transactions",
            get(get_transactions).post(post_transactions),
        )
}

/// Every group's cross-group transaction records, as each group's leader
/// applied them.
async fn get_transactions(
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let host = host(&state)?;
    Ok(Json(
        serde_json::json!({"groups": host.transactions().await}),
    ))
}

/// `POST /v1/admin/transactions`.
#[derive(serde::Deserialize, Clone, Debug)]
struct TransactionRequest {
    /// `abort`.
    operation: String,
    /// The transaction ID.
    transaction: String,
    /// Its coordinator group: the root when absent.
    group: Option<String>,
}

async fn post_transactions(
    State(state): State<AppState>,
    Json(body): Json<TransactionRequest>,
) -> Result<Response, ApiError> {
    let host = host(&state)?;
    if body.operation != "abort" {
        return Err(ApiError::BadRequest(format!(
            "unknown operation `{}`",
            body.operation
        )));
    }
    let name = body.group.unwrap_or_else(|| "root".to_string());
    let catalog = host.cached_catalog();
    let Some(group) = host.group_id(&catalog, &name) else {
        return Err(ApiError::BadRequest(format!("no group `{name}`")));
    };
    let outcome = host
        .abort_transaction(&group, &body.transaction)
        .await
        .map_err(ApiError::node)?;
    answer(outcome)
}

fn host(state: &AppState) -> Result<&Arc<Host>, ApiError> {
    state.host.as_ref().ok_or_else(|| {
        ApiError::Unavailable("this member serves one group and keeps no catalog".to_string())
    })
}

/// A catalog change's outcome on the wire.
fn answer(outcome: Result<Value, Failure>) -> Result<Response, ApiError> {
    match outcome {
        Ok(value) => Ok(
            Json(serde_json::json!({"ok": true, "value": value_to_json(&value)?})).into_response(),
        ),
        Err(failure) => {
            let status = if failure.code == CATALOG_UNKNOWN || failure.code == TXN_UNKNOWN {
                StatusCode::NOT_FOUND
            } else {
                StatusCode::CONFLICT
            };
            Ok((
                status,
                Json(serde_json::json!({
                    "ok": false,
                    "code": failure.code,
                    "message": failure.message,
                })),
            )
                .into_response())
        }
    }
}

/// Every group `catalog` lists, the root first, with whether this
/// process hosts a replica and, when `leaders`, who it knows leads.
pub(crate) fn groups_json(host: &Host, catalog: &Catalog, leaders: bool) -> Vec<serde_json::Value> {
    let root = host.root().membership();
    let mut groups = vec![serde_json::json!({
        "name": "root",
        "uid": "0000000000000000",
        "members": root.nodes.keys().collect::<Vec<_>>(),
        "status": "active",
        "founder": null,
    })];
    groups.extend(
        catalog
            .groups
            .values()
            .map(|entry| serde_json::to_value(entry).unwrap_or(serde_json::Value::Null)),
    );
    for group in &mut groups {
        let local = group["name"].as_str().and_then(|name| host.group(name));
        if let Some(object) = group.as_object_mut() {
            object.insert(
                "preferred_leader".into(),
                catalog
                    .leaders
                    .get(
                        object
                            .get("name")
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or_default(),
                    )
                    .and_then(|p| p.node)
                    .into(),
            );
            object.insert("hosted".into(), local.is_some().into());
            if leaders {
                object.insert(
                    "leader".into(),
                    local
                        .as_ref()
                        .and_then(|node| node.status().leader)
                        .map_or(serde_json::Value::Null, Into::into),
                );
            }
        }
    }
    groups
}

/// The catalog proven fresh through the root's leader.
async fn fresh(host: &Host) -> Result<Catalog, ApiError> {
    host.catalog().await.map_err(ApiError::node)
}

async fn get_groups(State(state): State<AppState>) -> Result<Json<serde_json::Value>, ApiError> {
    let host = host(&state)?;
    let catalog = fresh(host).await?;
    Ok(Json(serde_json::json!({
        "revision": catalog.revision,
        "node": host.id(),
        "groups": groups_json(host, &catalog, true),
    })))
}

/// `POST /v1/admin/groups`.
#[derive(serde::Deserialize, Clone, Debug)]
struct GroupRequest {
    /// `create`, `remove` or `prefer_leader`.
    operation: String,
    /// The group.
    group: String,
    /// Founding members, by node ID (`create`).
    #[serde(default)]
    members: Vec<u64>,
    /// Preferred voter for `prefer_leader`; null disables placement.
    node: Option<u64>,
    /// The operation's identity; required for `prefer_leader`, otherwise
    /// `<operation>-<group>` by default.
    op: Option<String>,
}

async fn post_groups(
    State(state): State<AppState>,
    Json(body): Json<GroupRequest>,
) -> Result<Response, ApiError> {
    let host = host(&state)?;
    if body.operation == "prefer_leader" && body.op.as_ref().is_none_or(|op| op.is_empty()) {
        return Err(ApiError::BadRequest(
            "prefer_leader needs a nonempty op".into(),
        ));
    }
    let op = body
        .op
        .unwrap_or_else(|| format!("{}-{}", body.operation, body.group));
    let outcome = match body.operation.as_str() {
        "create" => host.create_group(&body.group, body.members, &op).await,
        "prefer_leader" => {
            if body.group == "root"
                && body.node.is_some_and(|node| {
                    !host
                        .root()
                        .membership()
                        .voter_configs
                        .iter()
                        .any(|voters| voters.contains(&node))
                })
            {
                return Err(ApiError::BadRequest(
                    "preferred root leader must be a voter".into(),
                ));
            }
            host.change_catalog(CatalogChange::PreferLeader {
                group: body.group,
                node: body.node,
                op,
            })
            .await
        }
        "remove" => {
            host.change_catalog(CatalogChange::RemoveGroup {
                group: body.group,
                op,
            })
            .await
        }
        other => return Err(ApiError::BadRequest(format!("unknown operation `{other}`"))),
    }
    .map_err(ApiError::node)?;
    answer(outcome)
}

async fn get_partitions(
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let host = host(&state)?;
    let catalog = fresh(host).await?;
    Ok(Json(serde_json::json!({
        "revision": catalog.revision,
        "partitions": catalog.partitions.values().collect::<Vec<_>>(),
        "moves": catalog.moves,
    })))
}

/// `POST /v1/admin/partitions`.
#[derive(serde::Deserialize, Clone, Debug)]
struct PartitionRequest {
    /// `create`, `retire`, `move`, `abort` or `resize`.
    operation: String,
    /// The partition: its database's name.
    #[serde(default)]
    partition: String,
    /// Destination groups for a count-based resize.
    #[serde(default)]
    groups: Vec<String>,
    /// Its group (`create`), or where it moves (`move`).
    group: Option<String>,
    /// The operation's identity; `<operation>-<partition>` by default.
    /// `abort` names the move it gives up. Required for `resize`.
    op: Option<String>,
    /// Why a move is given up (`abort`).
    reason: Option<String>,
}

async fn post_partitions(
    State(state): State<AppState>,
    Json(body): Json<PartitionRequest>,
) -> Result<Response, ApiError> {
    let host = host(&state)?;
    if body.operation == "resize" && body.op.as_ref().is_none_or(|op| op.is_empty()) {
        return Err(ApiError::BadRequest("resize needs a nonempty op".into()));
    }
    let op = body
        .op
        .unwrap_or_else(|| format!("{}-{}", body.operation, body.partition));
    let change = match body.operation.as_str() {
        "resize" => CatalogChange::Resize {
            groups: body.groups,
            op,
        },
        "move" => CatalogChange::BeginMove {
            partition: body.partition,
            destination: body
                .group
                .ok_or_else(|| ApiError::BadRequest("move needs a group".to_string()))?,
            op,
        },
        "abort" => CatalogChange::AbortMove {
            partition: body.partition,
            op,
            reason: body.reason.unwrap_or_else(|| "operator".to_string()),
        },
        "create" => CatalogChange::CreatePartition {
            partition: body.partition,
            group: body
                .group
                .ok_or_else(|| ApiError::BadRequest("create needs a group".to_string()))?,
            op,
        },
        "retire" => CatalogChange::RetirePartition {
            partition: body.partition,
            op,
        },
        other => return Err(ApiError::BadRequest(format!("unknown operation `{other}`"))),
    };
    let outcome = host.change_catalog(change).await.map_err(ApiError::node)?;
    answer(outcome)
}
