//! Worker-facing queue endpoints (worker or admin credential).
//!
//! Claims, renewals and reports commit through Raft at the leader
//! (see `flower_cluster::work`), so every member agrees on them and
//! they survive failover; reads confirm leadership first. Followers
//! answer 503 with the known leader. Queue failures (bad tokens,
//! unknown items) are business failures and complete with HTTP 200 and
//! `ok: false`, like calls.

use axum::Router;
use axum::extract::{Query, State};
use axum::response::Json;
use axum::routing::{get, post};
use flower_cluster::work::{NextClaim, QueueOp, QueueOutcome, Settle};
use flower_engine::work_queue::JobRef;
use flower_sdk::Failure;

use crate::api::{ApiError, AppState, engine_read, value_to_json};

/// Default claim lease: 30 s.
const DEFAULT_LEASE_MS: u64 = 30_000;

/// Build the queue routes.
pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/v1/queue/poll", post(post_poll))
        .route("/v1/queue/heartbeat", post(post_heartbeat))
        .route("/v1/queue/complete", post(post_complete))
        .route("/v1/queue/fail", post(post_fail))
        .route("/v1/queue/kill", post(post_kill))
        .route("/v1/queue/release", post(post_release))
        .route("/v1/queue/cancel", post(post_cancel))
        .route("/v1/queue/retry", post(post_retry))
        .route("/v1/queue/purge", post(post_purge))
        .route("/v1/queue/stats", get(get_stats))
        .route("/v1/queue/ready", get(get_ready))
        .route("/v1/queue/item", get(get_item))
}

/// A business outcome on the wire: `ok` plus either the operation's
/// fields or a failure code and message.
#[derive(serde::Serialize, Clone, Debug)]
pub(crate) struct Outcome {
    /// Whether the operation business-succeeded.
    ok: bool,
    /// Operation-specific fields on success.
    #[serde(flatten)]
    fields: serde_json::Map<String, serde_json::Value>,
    /// Failure code on business failure.
    #[serde(skip_serializing_if = "Option::is_none")]
    code: Option<String>,
    /// Failure message on business failure.
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<String>,
}

impl Outcome {
    /// A success carrying these fields.
    pub(crate) fn ok(fields: serde_json::Value) -> Json<Self> {
        let fields = match fields {
            serde_json::Value::Object(map) => map,
            _ => serde_json::Map::new(),
        };
        Json(Outcome {
            ok: true,
            fields,
            code: None,
            message: None,
        })
    }

    /// A business failure, with the operation's empty defaults so
    /// clients decode one shape either way.
    pub(crate) fn failed(failure: Failure, defaults: serde_json::Value) -> Json<Self> {
        let fields = match defaults {
            serde_json::Value::Object(map) => map,
            _ => serde_json::Map::new(),
        };
        Json(Outcome {
            ok: false,
            fields,
            code: Some(failure.code),
            message: Some(failure.message),
        })
    }
}

/// Run one queue operation and shape its outcome.
async fn run(
    state: &AppState,
    db: &str,
    queue: &str,
    op: QueueOp,
    defaults: serde_json::Value,
) -> Result<Json<Outcome>, ApiError> {
    let outcome = state
        .node
        .queue_op(db.as_bytes(), queue, op)
        .await
        .map_err(ApiError::node)?;
    let fields = match outcome {
        Err(failure) => return Ok(Outcome::failed(failure, defaults)),
        Ok(QueueOutcome::Claimed(claimed)) => {
            let mut items = Vec::with_capacity(claimed.len());
            for item in claimed {
                items.push(serde_json::json!({
                    "id": item.id,
                    "payload": value_to_json(&item.payload)?,
                    "token": item.token,
                    "attempts": item.attempts,
                    "fence": item.fence,
                    "scope": item.scope,
                    "priority": item.priority,
                    "owner": item.owner,
                    "name": item.name,
                    "group": item.group,
                    "expires_ms": item.expires_ms,
                    "available_ms": item.available_ms,
                    "claimed_ms": item.claimed_ms,
                }));
            }
            serde_json::json!({"items": items})
        }
        Ok(QueueOutcome::Done) => serde_json::json!({}),
        Ok(QueueOutcome::Removed(removed)) => serde_json::json!({"removed": removed}),
        Ok(QueueOutcome::Purged(removed)) => serde_json::json!({"removed": removed}),
    };
    Ok(Outcome::ok(fields))
}

/// Shared queue + database fields, and the scope stats count.
#[derive(serde::Deserialize, Clone, Debug)]
struct QueueRequest {
    /// Database name (UTF-8).
    db: String,
    /// Queue name.
    queue: String,
    /// Count one scope only (default: every scope).
    scope: Option<String>,
    /// Say whether this owner, waiting in the scope's line, may claim.
    owner: Option<String>,
}

/// Claim a batch of due items.
#[derive(serde::Deserialize, Clone, Debug)]
struct PollRequest {
    /// Database name (UTF-8).
    db: String,
    /// Queue name.
    queue: String,
    /// Most items to claim (default 10).
    limit: Option<usize>,
    /// Claim lease in milliseconds (default 30 s).
    lease_ms: Option<u64>,
    /// The scope to claim from (default `""`).
    #[serde(default)]
    scope: String,
    /// The claimer, recorded on each lease.
    owner: Option<String>,
    /// Wait in the scope's line this long when the claim comes back
    /// short (needs an owner).
    wait_ms: Option<u64>,
}

async fn post_poll(
    State(state): State<AppState>,
    Json(body): Json<PollRequest>,
) -> Result<Json<Outcome>, ApiError> {
    let op = QueueOp::Claim {
        scope: body.scope,
        owner: body.owner,
        limit: body.limit.unwrap_or(10),
        lease_ms: body.lease_ms.unwrap_or(DEFAULT_LEASE_MS),
        wait_ms: body.wait_ms,
    };
    run(
        &state,
        &body.db,
        &body.queue,
        op,
        serde_json::json!({"items": []}),
    )
    .await
}

/// Identity plus claim token shared by report operations, with the
/// optional lease or delay some of them take.
#[derive(serde::Deserialize, Clone, Debug)]
struct ReportRequest {
    /// Database name (UTF-8).
    db: String,
    /// Queue name.
    queue: String,
    /// Item ID.
    id: String,
    /// Claim token.
    token: String,
    /// New lease in milliseconds, for heartbeats (default 30 s).
    lease_ms: Option<u64>,
    /// Redelivery delay in milliseconds, for fail and release.
    delay_ms: Option<u64>,
    /// For complete: keep the item as completed with this result
    /// (checked against the queue's declared result contracts) instead
    /// of deleting it.
    #[serde(default)]
    result: Option<serde_json::Value>,
    /// For complete, fail and kill: claim the reporter's next items
    /// over the state the report leaves, in the same commit, answered
    /// as a poll's (chained claims).
    #[serde(default)]
    next: Option<NextRequest>,
}

/// The claim a chained report makes: a poll's parameters.
#[derive(serde::Deserialize, Clone, Debug)]
struct NextRequest {
    /// Most items to claim (default 10).
    limit: Option<usize>,
    /// Claim lease in milliseconds (default 30 s).
    lease_ms: Option<u64>,
    /// The scope to claim from (default `""`).
    #[serde(default)]
    scope: String,
    /// The claimer, recorded on each lease.
    owner: Option<String>,
    /// Wait in the scope's line this long when the claim comes back
    /// short (needs an owner).
    wait_ms: Option<u64>,
}

/// Run a report alone, or chained to the reporter's next claim when it
/// asks for one.
async fn report(
    state: &AppState,
    body: ReportRequest,
    alone: QueueOp,
    settle: Settle,
) -> Result<Json<Outcome>, ApiError> {
    let Some(next) = body.next else {
        return run(state, &body.db, &body.queue, alone, serde_json::json!({})).await;
    };
    let op = QueueOp::Chained {
        id: body.id,
        token: body.token,
        report: settle,
        next: NextClaim {
            scope: next.scope,
            owner: next.owner,
            limit: next.limit.unwrap_or(10),
            lease_ms: next.lease_ms.unwrap_or(DEFAULT_LEASE_MS),
            wait_ms: next.wait_ms,
        },
    };
    run(
        state,
        &body.db,
        &body.queue,
        op,
        serde_json::json!({"items": []}),
    )
    .await
}

async fn post_heartbeat(
    State(state): State<AppState>,
    Json(body): Json<ReportRequest>,
) -> Result<Json<Outcome>, ApiError> {
    let op = QueueOp::Heartbeat {
        id: body.id,
        token: body.token,
        lease_ms: body.lease_ms.unwrap_or(DEFAULT_LEASE_MS),
    };
    run(&state, &body.db, &body.queue, op, serde_json::json!({})).await
}

async fn post_complete(
    State(state): State<AppState>,
    Json(body): Json<ReportRequest>,
) -> Result<Json<Outcome>, ApiError> {
    // A result keeps the item as completed with it; none deletes it.
    let result = match &body.result {
        Some(result) => {
            Some(serde_json::to_vec(result).map_err(|error| ApiError::Fatal(error.to_string()))?)
        }
        None => None,
    };
    let (id, token) = (body.id.clone(), body.token.clone());
    let op = match result.clone() {
        Some(result) => QueueOp::CompleteWith { id, token, result },
        None => QueueOp::Complete { id, token },
    };
    report(&state, body, op, Settle::Complete { result }).await
}

async fn post_fail(
    State(state): State<AppState>,
    Json(body): Json<ReportRequest>,
) -> Result<Json<Outcome>, ApiError> {
    let delay_ms = body.delay_ms.unwrap_or(0);
    let op = QueueOp::Fail {
        id: body.id.clone(),
        token: body.token.clone(),
        delay_ms,
    };
    report(&state, body, op, Settle::Fail { delay_ms }).await
}

async fn post_kill(
    State(state): State<AppState>,
    Json(body): Json<ReportRequest>,
) -> Result<Json<Outcome>, ApiError> {
    let op = QueueOp::Kill {
        id: body.id.clone(),
        token: body.token.clone(),
    };
    report(&state, body, op, Settle::Kill).await
}

async fn post_release(
    State(state): State<AppState>,
    Json(body): Json<ReportRequest>,
) -> Result<Json<Outcome>, ApiError> {
    let op = QueueOp::Release {
        id: body.id,
        token: body.token,
        delay_ms: body.delay_ms.unwrap_or(0),
    };
    run(&state, &body.db, &body.queue, op, serde_json::json!({})).await
}

/// Cancel or retry one item by ID.
#[derive(serde::Deserialize, Clone, Debug)]
struct ItemOpRequest {
    /// Database name (UTF-8).
    db: String,
    /// Queue name.
    queue: String,
    /// Item ID.
    id: String,
    /// Redelivery delay in milliseconds, for retries (default 0).
    delay_ms: Option<u64>,
}

async fn post_cancel(
    State(state): State<AppState>,
    Json(body): Json<ItemOpRequest>,
) -> Result<Json<Outcome>, ApiError> {
    let op = QueueOp::Cancel { id: body.id };
    run(
        &state,
        &body.db,
        &body.queue,
        op,
        serde_json::json!({"removed": false}),
    )
    .await
}

async fn post_retry(
    State(state): State<AppState>,
    Json(body): Json<ItemOpRequest>,
) -> Result<Json<Outcome>, ApiError> {
    let op = QueueOp::Retry {
        id: body.id,
        delay_ms: body.delay_ms.unwrap_or(0),
    };
    run(&state, &body.db, &body.queue, op, serde_json::json!({})).await
}

/// Purge dead items enqueued before a cutoff.
#[derive(serde::Deserialize, Clone, Debug)]
struct PurgeRequest {
    /// Database name (UTF-8).
    db: String,
    /// Queue name.
    queue: String,
    /// Purge items enqueued before this time (default now).
    older_than_ms: Option<u64>,
}

async fn post_purge(
    State(state): State<AppState>,
    Json(body): Json<PurgeRequest>,
) -> Result<Json<Outcome>, ApiError> {
    let op = QueueOp::Purge {
        older_than_ms: body
            .older_than_ms
            .unwrap_or_else(flower_cluster::exec::now_ms),
    };
    run(
        &state,
        &body.db,
        &body.queue,
        op,
        serde_json::json!({"removed": 0}),
    )
    .await
}

async fn get_stats(
    State(state): State<AppState>,
    Query(query): Query<QueueRequest>,
) -> Result<Json<Outcome>, ApiError> {
    state.node.ensure_leader().await.map_err(ApiError::node)?;
    let now = flower_cluster::exec::now_ms();
    let stats = engine_read(&state, move |engine| {
        let db = query.db.as_bytes();
        let stats = engine.queue_stats_at(db, &query.queue, query.scope.as_deref(), now)?;
        let scope = query.scope.as_deref().unwrap_or("");
        let readiness =
            engine.queue_readiness(db, &query.queue, scope, query.owner.as_deref(), now)?;
        Ok::<_, Failure>((stats, readiness))
    })
    .await?;
    Ok(match stats {
        Ok((stats, readiness)) => Outcome::ok(serde_json::json!({
            "pending": stats.pending,
            "claimed": stats.claimed,
            "dead": stats.dead,
            "completed": stats.completed,
            "ready": readiness.ready,
            "next_ms": readiness.next_ms,
        })),
        Err(failure) => Outcome::failed(
            failure,
            serde_json::json!({"pending": 0, "claimed": 0, "dead": 0, "completed": 0}),
        ),
    })
}

/// Whether a claim on one scope (by `owner`, in turn in its line) would
/// take an item now, and when that next changes: what a worker waiting
/// in line reads each interval. Unlike stats it counts nothing, so its
/// cost is the scope's next items and the line's head, however many
/// dead or completed jobs the scope keeps.
async fn get_ready(
    State(state): State<AppState>,
    Query(query): Query<QueueRequest>,
) -> Result<Json<Outcome>, ApiError> {
    state.node.ensure_leader().await.map_err(ApiError::node)?;
    let now = flower_cluster::exec::now_ms();
    let readiness = engine_read(&state, move |engine| {
        let scope = query.scope.as_deref().unwrap_or("");
        engine.queue_readiness(
            query.db.as_bytes(),
            &query.queue,
            scope,
            query.owner.as_deref(),
            now,
        )
    })
    .await?;
    Ok(match readiness {
        Ok(readiness) => Outcome::ok(serde_json::json!({
            "ready": readiness.ready,
            "next_ms": readiness.next_ms,
        })),
        Err(failure) => Outcome::failed(failure, serde_json::json!({"ready": false})),
    })
}

/// Read one item by ID, or a named job by scope and name, including
/// dead and completed ones.
#[derive(serde::Deserialize, Clone, Debug)]
struct ItemRequest {
    /// Database name (UTF-8).
    db: String,
    /// Queue name.
    queue: String,
    /// Item ID.
    id: Option<String>,
    /// A named job's name.
    name: Option<String>,
    /// A named job's scope (default `""`).
    #[serde(default)]
    scope: String,
}

async fn get_item(
    State(state): State<AppState>,
    Query(query): Query<ItemRequest>,
) -> Result<Json<Outcome>, ApiError> {
    state.node.ensure_leader().await.map_err(ApiError::node)?;
    let now = flower_cluster::exec::now_ms();
    let item = engine_read(&state, move |engine| {
        let job = match (&query.name, &query.id) {
            (Some(name), _) => JobRef::Named {
                scope: &query.scope,
                name,
            },
            (None, Some(id)) => JobRef::Id(id),
            (None, None) => {
                return Err(Failure::new(
                    "INVALID_VALUE",
                    "an item needs an id or a name",
                ));
            }
        };
        engine.find_queue_item_at(query.db.as_bytes(), &query.queue, &job, now)
    })
    .await?;
    Ok(match item {
        Ok(Some(item)) => {
            let result = match &item.result {
                Some(result) => value_to_json(result)?,
                None => serde_json::Value::Null,
            };
            Outcome::ok(serde_json::json!({"item": {
                "id": item.id,
                "payload": value_to_json(&item.payload)?,
                "state": item.state.name(),
                "name": item.name,
                "group": item.group,
                "turn": item.turn,
                "result": result,
                "attempts": item.attempts,
                "max_attempts": item.max_attempts,
                "enqueued_ms": item.enqueued_ms,
                "due_ms": item.due_ms,
                "scope": item.scope,
                "priority": item.priority,
                "fence": item.fence,
                "owner": item.owner,
                "claimed_until_ms": item.claimed_until_ms,
            }}))
        }
        Ok(None) => Outcome::failed(
            Failure::new("UNKNOWN_ITEM", "unknown queue item"),
            serde_json::json!({}),
        ),
        Err(failure) => Outcome::failed(failure, serde_json::json!({})),
    })
}
