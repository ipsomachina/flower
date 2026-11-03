//! Operator endpoints beside deployment, membership and backup (admin
//! credential): the modules each database runs, staged deployments
//! driven step by step, and Raft metrics. The managed-key catalog is
//! served from [`crate::api`], retention from [`crate::retention`].
//!
//! - `GET /v1/admin/modules[?db=]`: every database's active modules
//!   (or one database's), after a fresh-read barrier: code identity,
//!   definitions by kind, collections, HTTP aliases, whether an
//!   authorization hook is declared, maintenance tasks with their
//!   failure states, and any staging row; beside them the modules whose
//!   data is being purged (`purging`: name and keys deleted so far) and
//!   those removed whose data stays (`removed`: name and the code
//!   identity each last ran).
//! - `POST /v1/admin/modules`: `{"operation": "remove", "db", "module",
//!   "purge"}` removes a module through consensus at the leader
//!   (followers forward) and answers as `/v1/deploy` does, its value
//!   `{removed, purging}`; refusals are 409 `MODULE_CONFLICT`.
//!   `{"operation": "list", "db"?}` lists as `GET` does.
//! - `POST /v1/admin/deployments`: `{"operation": "stage" | "advance" |
//!   "status" | "activate" | "cancel" | "collect", "db", "module",
//!   ...}` at the leader (followers forward). The staging replicates:
//!   every member holds it, and a new leader continues it. `stage`
//!   takes the deploy body's `wasm_b64`, `manifest` and optional
//!   `identity`; `advance` builds up to `max_records` (default 4,096)
//!   more of the backfill, then rebuilds as many roots of the derived
//!   graph under the target code; `activate` proposes the ready
//!   staging's activation and answers as `/v1/deploy` does; `collect`
//!   deletes up to `max_keys` of the structures the module's
//!   deployments retired, on every member alike (the leader's
//!   maintenance collects them on its own too). Refusals are 409
//!   `deployment`.
//! - `GET /v1/admin/raft`: OpenRaft's metrics for this member.

use axum::Router;
use axum::extract::{DefaultBodyLimit, Query, State};
use axum::response::Json;
use axum::routing::{get, post};
use flower_cluster::node::{ModuleListing, StagedDeployment};
use flower_engine::engine::{ModuleSummary, StagingStatus};
use flower_engine::hex::encode as encode_hex;

use crate::api::{ApiError, AppState, CallResponse, decode_bundle, response_from};

/// Records an `advance` builds when the request names no bound.
const DEFAULT_ADVANCE_RECORDS: usize = 4096;

/// Build the operator routes; `deploy_limit` bounds staging bodies.
pub(crate) fn routes(deploy_limit: usize) -> Router<AppState> {
    Router::new()
        .route("/v1/admin/modules", get(get_modules).post(post_modules))
        .route(
            "/v1/admin/deployments",
            post(post_deployments).route_layer(DefaultBodyLimit::max(deploy_limit)),
        )
        .route("/v1/admin/raft", get(get_raft))
}

/// `GET /v1/admin/modules` parameters.
#[derive(serde::Deserialize, Clone, Debug)]
struct ModulesParams {
    /// One database, UTF-8; every database when absent.
    db: Option<String>,
}

/// One module on the wire.
pub(crate) fn module_json(module: &ModuleSummary) -> serde_json::Value {
    serde_json::json!({
        "name": module.name,
        "identity": module.identity.map(|identity| encode_hex(&identity)),
        "definitions": module.definitions,
        "collections": module.collections,
        "http": module.http,
        "authorize": module.authorize,
        "tasks": module.tasks.iter().map(|task| serde_json::json!({
            "name": task.name,
            "on_error": task.on_error,
            "failures": task.failures,
            "retry_at_ms": task.retry_at_ms,
            "error": task.error.as_ref().map(|error| serde_json::json!({
                "code": error.code,
                "message": error.message,
            })),
        })).collect::<Vec<_>>(),
        "staging": module.staging.as_ref().map(staging_json),
    })
}

/// A staging row on the wire.
pub(crate) fn staging_json(staging: &StagingStatus) -> serde_json::Value {
    serde_json::json!({
        "ready": staging.ready,
        "planned": staging.planned,
        "position": staging.position,
        "retired": staging.retired,
        "phase": staging.phase.name(),
        "replicated": staging.replicated,
        "graph": staging.graph,
        "graph_position": staging.graph_position,
        "rebuilt": staging.rebuilt,
    })
}

/// One database's listing on the wire: its active modules, the purges
/// in flight and the modules removed whose data stays.
pub(crate) fn listing_json(listing: &ModuleListing) -> serde_json::Value {
    serde_json::json!({
        "db": String::from_utf8_lossy(&listing.db),
        "modules": listing.modules.iter().map(module_json).collect::<Vec<_>>(),
        "purging": listing
            .purging
            .iter()
            .map(|(name, progress)| serde_json::json!({
                "name": name,
                "deleted": progress.deleted,
            }))
            .collect::<Vec<_>>(),
        "removed": listing
            .removed
            .iter()
            .map(|(name, identity)| serde_json::json!({
                "name": name,
                "identity": encode_hex(identity),
            }))
            .collect::<Vec<_>>(),
    })
}

/// Every database's active modules, or one database's.
async fn get_modules(
    State(state): State<AppState>,
    Query(params): Query<ModulesParams>,
) -> Result<Json<serde_json::Value>, ApiError> {
    list_modules(&state, params.db.as_deref()).await
}

/// The listing `GET /v1/admin/modules` answers.
async fn list_modules(
    state: &AppState,
    db: Option<&str>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let listed = state
        .node
        .modules(db.map(str::as_bytes))
        .await
        .map_err(ApiError::node)?;
    let databases: Vec<serde_json::Value> = listed.iter().map(listing_json).collect();
    Ok(Json(
        serde_json::json!({"ok": true, "databases": databases}),
    ))
}

/// One operation on a database's modules.
#[derive(serde::Deserialize, Clone, Debug)]
#[serde(tag = "operation", rename_all = "snake_case")]
enum ModulesRequest {
    /// List as `GET` does.
    List {
        /// One database, UTF-8; every database when absent.
        db: Option<String>,
    },
    /// Remove a module, or purge what a removal kept.
    Remove {
        /// Database name (UTF-8).
        db: String,
        /// Module name.
        module: String,
        /// Whether its data purges too.
        #[serde(default)]
        purge: bool,
    },
}

/// Remove a module through consensus, or list.
async fn post_modules(
    State(state): State<AppState>,
    Json(body): Json<ModulesRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    match body {
        ModulesRequest::List { db } => list_modules(&state, db.as_deref()).await,
        ModulesRequest::Remove { db, module, purge } => {
            let response = state
                .node
                .remove_module(db.as_bytes(), &module, purge)
                .await
                .map_err(ApiError::node)?;
            let response: CallResponse = response_from(&response)?;
            serde_json::to_value(response)
                .map(Json)
                .map_err(|error| ApiError::BadRequest(error.to_string()))
        }
    }
}

/// One staged-deployment step.
#[derive(serde::Deserialize, Clone, Debug)]
#[serde(tag = "operation", rename_all = "snake_case")]
enum DeploymentRequest {
    /// Validate, intern and store a target without activating it.
    Stage {
        /// Database name (UTF-8).
        db: String,
        /// Module name.
        module: String,
        /// Target identity as 64 hex chars; computed when absent.
        identity: Option<String>,
        /// Wasm bundle, standard base64.
        wasm_b64: String,
        /// Module manifest as JSON.
        manifest: serde_json::Value,
    },
    /// Build more of the backfill.
    Advance {
        /// Database name (UTF-8).
        db: String,
        /// Module name.
        module: String,
        /// Most records to build in this step.
        max_records: Option<usize>,
    },
    /// Report the staging.
    Status {
        /// Database name (UTF-8).
        db: String,
        /// Module name.
        module: String,
    },
    /// Propose the ready staging's activation.
    Activate {
        /// Database name (UTF-8).
        db: String,
        /// Module name.
        module: String,
    },
    /// Abandon the staging.
    Cancel {
        /// Database name (UTF-8).
        db: String,
        /// Module name.
        module: String,
    },
    /// Collect structures the module's deployments retired, on every
    /// member alike.
    Collect {
        /// Database name (UTF-8).
        db: String,
        /// Module name.
        module: String,
        /// Most keys to delete in this step.
        max_keys: Option<usize>,
    },
}

/// A staging on the wire.
fn staged_json(staged: &StagedDeployment) -> serde_json::Value {
    serde_json::json!({
        "ok": true,
        "staging": staged.status.as_ref().map(staging_json),
        "identity": staged.identity.map(|identity| encode_hex(&identity)),
        "scanned": staged.scanned,
        "rebuilt": staged.rebuilt,
    })
}

/// Drive a staged deployment one step.
async fn post_deployments(
    State(state): State<AppState>,
    Json(body): Json<DeploymentRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let node = &state.node;
    let staged = match body {
        DeploymentRequest::Stage {
            db,
            module,
            identity,
            wasm_b64,
            manifest,
        } => {
            let (wasm, manifest, identity) =
                decode_bundle(&wasm_b64, &manifest, identity.as_deref())?;
            node.stage_deployment(db.as_bytes(), &module, identity, &wasm, &manifest)
                .await
        }
        DeploymentRequest::Advance {
            db,
            module,
            max_records,
        } => {
            node.advance_deployment(
                db.as_bytes(),
                &module,
                max_records.unwrap_or(DEFAULT_ADVANCE_RECORDS),
            )
            .await
        }
        DeploymentRequest::Status { db, module } => {
            node.deployment_status(db.as_bytes(), &module).await
        }
        DeploymentRequest::Activate { db, module } => {
            let response = node
                .activate_deployment(db.as_bytes(), &module)
                .await
                .map_err(ApiError::node)?;
            let response: CallResponse = response_from(&response)?;
            return serde_json::to_value(response)
                .map(Json)
                .map_err(|error| ApiError::BadRequest(error.to_string()));
        }
        DeploymentRequest::Cancel { db, module } => {
            let canceled = node
                .cancel_deployment(db.as_bytes(), &module)
                .await
                .map_err(ApiError::node)?;
            return Ok(Json(serde_json::json!({"ok": true, "canceled": canceled})));
        }
        DeploymentRequest::Collect {
            db,
            module,
            max_keys,
        } => {
            let (collected, done) = node
                .collect_deployment(
                    db.as_bytes(),
                    &module,
                    max_keys.unwrap_or(DEFAULT_ADVANCE_RECORDS),
                )
                .await
                .map_err(ApiError::node)?;
            return Ok(Json(
                serde_json::json!({"ok": true, "collected": collected, "done": done}),
            ));
        }
    };
    Ok(Json(staged_json(&staged.map_err(ApiError::node)?)))
}

/// OpenRaft's metrics for this member.
async fn get_raft(State(state): State<AppState>) -> Result<Json<serde_json::Value>, ApiError> {
    let metrics = state.node.raft().metrics().borrow().clone();
    let metrics =
        serde_json::to_value(&metrics).map_err(|error| ApiError::BadRequest(error.to_string()))?;
    Ok(Json(serde_json::json!({"ok": true, "raft": metrics})))
}
