//! Operator-facing timer endpoints (admin credential).
//!
//! Cancel, retry and purge commit through Raft at the leader,
//! revision-guarded like fires, so a cancelled timer stays cancelled on
//! every member and after failover; reads confirm leadership first.
//! Followers answer 503 with the known leader. Timer failures are
//! business failures and complete with HTTP 200 and `ok: false`.
//! Firing itself is the leader's sweep, not an endpoint.

use axum::Router;
use axum::extract::{Query, State};
use axum::response::Json;
use axum::routing::{get, post};
use flower_cluster::work::{TimerOp, TimerOutcome};
use flower_engine::timer::{Timer, TimerState};
use flower_sdk::Failure;

use crate::api::{ApiError, AppState, engine_read, value_to_json};
use crate::queue::Outcome;

/// Build the timer routes.
pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/v1/timer/stats", get(get_stats))
        .route("/v1/timer/get", get(get_timer))
        .route("/v1/timer/list", get(get_list))
        .route("/v1/timer/cancel", post(post_cancel))
        .route("/v1/timer/retry", post(post_retry))
        .route("/v1/timer/purge", post(post_purge))
}

/// Shared database fields.
#[derive(serde::Deserialize, Clone, Debug)]
struct TimerDb {
    /// Database name (UTF-8).
    db: String,
}

/// A stored timer for inspection.
fn dto(timer: &Timer) -> Result<serde_json::Value, ApiError> {
    Ok(serde_json::json!({
        "id": timer.id,
        "handler": timer.handler,
        "args": value_to_json(&timer.args)?,
        "due_ms": timer.due_ms,
        "attempts": timer.attempts,
        "max_attempts": timer.max_attempts,
        "state": timer.state.name(),
    }))
}

/// Run one timer operation and shape its outcome.
async fn run(
    state: &AppState,
    db: &str,
    op: TimerOp,
    defaults: serde_json::Value,
) -> Result<Json<Outcome>, ApiError> {
    let outcome = state
        .node
        .timer_op(db.as_bytes(), op)
        .await
        .map_err(ApiError::node)?;
    Ok(match outcome {
        Err(failure) => Outcome::failed(failure, defaults),
        Ok(TimerOutcome::Done) => Outcome::ok(serde_json::json!({})),
        Ok(TimerOutcome::Removed(removed)) => Outcome::ok(serde_json::json!({"removed": removed})),
        Ok(TimerOutcome::Purged(removed)) => Outcome::ok(serde_json::json!({"removed": removed})),
    })
}

async fn get_stats(
    State(state): State<AppState>,
    Query(query): Query<TimerDb>,
) -> Result<Json<Outcome>, ApiError> {
    state.node.ensure_leader().await.map_err(ApiError::node)?;
    let stats = engine_read(&state, move |engine| {
        engine.timer_stats(query.db.as_bytes())
    })
    .await?;
    Ok(match stats {
        Ok(stats) => Outcome::ok(serde_json::json!({
            "pending": stats.pending,
            "dead": stats.dead,
            "held": stats.held,
        })),
        Err(failure) => Outcome::failed(
            failure,
            serde_json::json!({"pending": 0, "dead": 0, "held": 0}),
        ),
    })
}

/// Read one timer by ID, including dead ones.
#[derive(serde::Deserialize, Clone, Debug)]
struct ItemRequest {
    /// Database name (UTF-8).
    db: String,
    /// Timer ID.
    id: String,
}

async fn get_timer(
    State(state): State<AppState>,
    Query(query): Query<ItemRequest>,
) -> Result<Json<Outcome>, ApiError> {
    state.node.ensure_leader().await.map_err(ApiError::node)?;
    let timer = engine_read(&state, move |engine| {
        engine.get_timer(query.db.as_bytes(), &query.id)
    })
    .await?;
    Ok(match timer {
        Ok(Some(timer)) => Outcome::ok(serde_json::json!({"item": dto(&timer)?})),
        Ok(None) => Outcome::failed(
            Failure::new("UNKNOWN_TIMER", "unknown timer"),
            serde_json::json!({}),
        ),
        Err(failure) => Outcome::failed(failure, serde_json::json!({})),
    })
}

/// List timers due-first.
#[derive(serde::Deserialize, Clone, Debug)]
struct ListRequest {
    /// Database name (UTF-8).
    db: String,
    /// `pending` (default), `dead`, `held` or `all`.
    state: Option<String>,
    /// Most timers to list (default 100).
    limit: Option<usize>,
}

async fn get_list(
    State(state): State<AppState>,
    Query(query): Query<ListRequest>,
) -> Result<Json<Outcome>, ApiError> {
    state.node.ensure_leader().await.map_err(ApiError::node)?;
    let wanted = match query.state.as_deref() {
        None | Some("pending") => Some(TimerState::Pending),
        Some("dead") => Some(TimerState::Dead),
        Some("held") => Some(TimerState::Held),
        Some("all") => None,
        Some(_) => {
            return Ok(Outcome::failed(
                Failure::new(
                    "INVALID_VALUE",
                    "timer list state is pending, dead, held or all",
                ),
                serde_json::json!({"timers": []}),
            ));
        }
    };
    let limit = query.limit.unwrap_or(100);
    let timers = engine_read(&state, move |engine| {
        engine.scan_timers(query.db.as_bytes(), wanted, limit)
    })
    .await?;
    Ok(match timers {
        Ok(timers) => {
            let timers = timers.iter().map(dto).collect::<Result<Vec<_>, _>>()?;
            Outcome::ok(serde_json::json!({"timers": timers}))
        }
        Err(failure) => Outcome::failed(failure, serde_json::json!({"timers": []})),
    })
}

/// Cancel or retry a timer.
#[derive(serde::Deserialize, Clone, Debug)]
struct TimerOpRequest {
    /// Database name (UTF-8).
    db: String,
    /// Timer ID.
    id: String,
    /// Delay before a retried timer is due (default 0).
    delay_ms: Option<u64>,
}

async fn post_cancel(
    State(state): State<AppState>,
    Json(body): Json<TimerOpRequest>,
) -> Result<Json<Outcome>, ApiError> {
    let op = TimerOp::Cancel { id: body.id };
    run(&state, &body.db, op, serde_json::json!({"removed": false})).await
}

async fn post_retry(
    State(state): State<AppState>,
    Json(body): Json<TimerOpRequest>,
) -> Result<Json<Outcome>, ApiError> {
    let op = TimerOp::Retry {
        id: body.id,
        delay_ms: body.delay_ms.unwrap_or(0),
    };
    run(&state, &body.db, op, serde_json::json!({})).await
}

/// Purge dead timers scheduled before a cutoff.
#[derive(serde::Deserialize, Clone, Debug)]
struct PurgeRequest {
    /// Database name (UTF-8).
    db: String,
    /// Purge timers scheduled before this time (default now).
    older_than_ms: Option<u64>,
}

async fn post_purge(
    State(state): State<AppState>,
    Json(body): Json<PurgeRequest>,
) -> Result<Json<Outcome>, ApiError> {
    let op = TimerOp::Purge {
        older_than_ms: body
            .older_than_ms
            .unwrap_or_else(flower_cluster::exec::now_ms),
    };
    run(&state, &body.db, op, serde_json::json!({"removed": 0})).await
}
