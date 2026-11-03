//! Retry identities, retry sessions and retention control over HTTP.
//!
//! - `GET /v1/retry?db=` answers what a client needs to mint scoped
//!   request IDs: the history incarnation, the current epoch, the floor
//!   and the rotation's epoch length. A mutation sending
//!   `idempotency_key: "f1:<incarnation>:<epoch>:<sha256 intent>"` or
//!   `"f2:<incarnation>:<epoch>:<session>:<sequence>"` runs under that
//!   scoped identity, its receipt pinning the method, arguments and
//!   expected revision (see [`flower_engine::retention`]).
//! - `POST /v1/retry/session` opens, reads, acknowledges or closes a
//!   retry session for the principal the named module's authorization
//!   hook decides for `$flower.session.<operation>`.
//! - `GET/POST /v1/admin/retention` (admin credential) read the state
//!   (with the leader's maintenance counters and, under `receipts`, the
//!   replicated expiry clock and lifetime of unscoped receipts) and
//!   rotate, advance, collect or bound it with a byte budget
//!   (`max_receipt_bytes`).
//!
//! Every write goes through the leader's writer; followers forward.
//! Refusals are business outcomes: HTTP 200 with `ok: false`.

use axum::Router;
use axum::extract::{Query, State};
use axum::http::HeaderMap;
use axum::response::Json;
use axum::routing::{get, post};
use flower_cluster::node::RetryStatus;
use flower_engine::retention::{Acknowledgement, COLLECT_PAGE, RetentionCommand, describe};
use flower_sdk::{Failure, Value};

use crate::api::{ApiError, AppState, PrincipalDto, value_to_json};
use crate::queue::Outcome;

/// Client routes: identities and sessions.
pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/v1/retry", get(get_retry))
        .route("/v1/retry/session", post(post_session))
}

/// Operator routes: status and control.
pub fn admin_routes() -> Router<AppState> {
    Router::new().route(
        "/v1/admin/retention",
        get(get_retention).post(post_retention),
    )
}

/// The database a status request names.
#[derive(serde::Deserialize, Clone, Debug)]
struct RetryDb {
    /// Database name (UTF-8).
    db: String,
}

/// A database's retry state as clients mint identities from it.
pub(crate) fn status_json(status: &RetryStatus) -> Result<serde_json::Value, ApiError> {
    let mut fields = serde_json::json!({
        "incarnation": format!("{:016x}", status.incarnation),
        "initialized": status.retention.is_some(),
        "epoch": 0,
        "min_epoch": 0,
        "epoch_ms": null,
        "keep_epochs": null,
    });
    if let Some(state) = &status.retention {
        let described = value_to_json(&describe(state))?;
        if let (Some(fields), serde_json::Value::Object(described)) =
            (fields.as_object_mut(), described)
        {
            fields.extend(described);
            fields.insert("epoch".to_string(), state.current_epoch.into());
        }
    }
    Ok(fields)
}

/// The identity new scoped request IDs take.
async fn get_retry(
    State(state): State<AppState>,
    Query(query): Query<RetryDb>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let status = state
        .node
        .retry_status(query.db.as_bytes())
        .await
        .map_err(ApiError::node)?;
    Ok(Json(status_json(&status)?))
}

/// A retry session request.
#[derive(serde::Deserialize, Clone, Debug)]
struct SessionRequest {
    /// Database name (UTF-8).
    db: String,
    /// The module whose authorization decides the owner.
    module: String,
    /// The session's 128-bit ID, 32 lowercase hex digits.
    session: String,
    /// `open`, `status`, `ack` or `close`.
    operation: String,
    /// `ack`: the new watermark.
    through: Option<u64>,
    /// `ack`: fence sequences without a result too.
    #[serde(default)]
    abandon: bool,
    /// `ack`: the most work it may do (sequences a strict one moves past,
    /// results an abandon deletes at once), 256 by default.
    limit: Option<u64>,
    /// Calling principal; honored for trusted gateways only.
    principal: Option<PrincipalDto>,
    /// Credentials for the authorization hook.
    credentials: Option<serde_json::Value>,
}

/// Open, read, acknowledge or close a retry session.
async fn post_session(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<SessionRequest>,
) -> Result<Json<Outcome>, ApiError> {
    let options = state.trust(&headers).call_options(
        body.principal.as_ref(),
        None,
        body.credentials.as_ref(),
    )?;
    let session: [u8; 16] = flower_engine::hex::decode(&body.session)
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(|| ApiError::BadRequest("a session is 32 lowercase hex digits".into()))?;
    let (node, db, module) = (&state.node, body.db.as_bytes(), body.module.as_str());
    let answer = match body.operation.as_str() {
        "open" => node.open_retry_session(db, module, session, &options).await,
        "status" => {
            node.retry_session_status(db, module, session, &options)
                .await
        }
        "ack" => {
            let through = body
                .through
                .ok_or_else(|| ApiError::BadRequest("`ack` takes `through`".into()))?;
            let mut ack = Acknowledgement::through(through);
            if body.abandon {
                ack = ack.abandoning();
            }
            if let Some(limit) = body.limit {
                ack = ack.limit(limit);
            }
            node.acknowledge_retry_session(db, module, session, ack, &options)
                .await
        }
        "close" => {
            node.close_retry_session(db, module, session, &options)
                .await
        }
        other => {
            return Err(ApiError::BadRequest(format!(
                "unknown session operation `{other}`"
            )));
        }
    }
    .map_err(ApiError::node)?;
    outcome(answer, "session")
}

/// An operator's retention request.
#[derive(serde::Deserialize, Clone, Debug)]
struct RetentionRequest {
    /// Database name (UTF-8).
    db: String,
    /// `rotate`, `advance`, `collect` or `budget`.
    operation: String,
    /// `rotate`: leader time per epoch, absent or null to stop rotating.
    epoch_ms: Option<u64>,
    /// `rotate`: epochs kept, the current one included.
    keep_epochs: Option<u64>,
    /// `advance`: the current epoch it was planned against.
    from_epoch: Option<u64>,
    /// `advance`: the new current epoch.
    current_epoch: Option<u64>,
    /// `advance`: the new floor.
    min_epoch: Option<u64>,
    /// `collect`: receipts to inspect.
    limit: Option<u64>,
    /// `budget`: the most bytes of results and sessions retention keeps
    /// for new work; absent or null for no bound.
    max_receipt_bytes: Option<u64>,
}

/// A database's retention state.
async fn get_retention(
    State(state): State<AppState>,
    Query(query): Query<RetryDb>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let status = state
        .node
        .retry_status(query.db.as_bytes())
        .await
        .map_err(ApiError::node)?;
    let receipts = state.node.receipt_clock().await.map_err(ApiError::node)?;
    let mut body = status_json(&status)?;
    body["stats"] = serde_json::to_value(state.node.retention_stats())
        .map_err(|error| ApiError::Fatal(error.to_string()))?;
    body["receipts"] =
        serde_json::to_value(receipts).map_err(|error| ApiError::Fatal(error.to_string()))?;
    Ok(Json(body))
}

/// Rotate, advance, collect or set the byte budget.
async fn post_retention(
    State(state): State<AppState>,
    Json(body): Json<RetentionRequest>,
) -> Result<Json<Outcome>, ApiError> {
    let need = |field: Option<u64>, name: &str| {
        field.ok_or_else(|| ApiError::BadRequest(format!("`{}` takes `{name}`", body.operation)))
    };
    let command = match body.operation.as_str() {
        "rotate" => RetentionCommand::Rotate {
            epoch_ms: body.epoch_ms,
            keep_epochs: need(body.keep_epochs, "keep_epochs")?,
        },
        "advance" => RetentionCommand::Advance {
            from_epoch: need(body.from_epoch, "from_epoch")?,
            current_epoch: need(body.current_epoch, "current_epoch")?,
            min_epoch: need(body.min_epoch, "min_epoch")?,
        },
        "collect" => RetentionCommand::Collect {
            limit: body.limit.unwrap_or(COLLECT_PAGE),
        },
        "budget" => RetentionCommand::Budget {
            max_receipt_bytes: body.max_receipt_bytes,
        },
        other => {
            return Err(ApiError::BadRequest(format!(
                "unknown retention operation `{other}`"
            )));
        }
    };
    let answer = state
        .node
        .retention(body.db.as_bytes(), command)
        .await
        .map_err(ApiError::node)?;
    outcome(answer, "retention")
}

/// A committed answer under `field`, or its refusal.
fn outcome(answer: Result<Value, Failure>, field: &str) -> Result<Json<Outcome>, ApiError> {
    match answer {
        Ok(value) => Ok(Outcome::ok(
            serde_json::json!({ field: value_to_json(&value)? }),
        )),
        Err(failure) => Ok(Outcome::failed(failure, serde_json::json!({}))),
    }
}
