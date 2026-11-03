//! Routes, handlers and the `/v1` JSON contract.
//!
//! # Credentials and caller identity
//!
//! The server holds up to three bearer credentials, each a separate
//! authority so a credential spread to many hosts cannot do more than
//! its job:
//!
//! - the **admin** token (operator, or a trusted gateway): deploy,
//!   membership, backup, metrics and timer maintenance, plus everything
//!   the other two allow;
//! - the **worker** token: the `/v1/queue/*` claim protocol only;
//! - the **sync** token: `/v1/sync` only, which ships every database's
//!   effects and snapshots to replicas and devices.
//!
//! An unconfigured credential authorizes nothing: its endpoints answer
//! 403 until the admin token or that credential is presented. Tokens
//! compare in constant time. Calls (`query`, `mutate`, `transaction`,
//! `watch`), `status` and the health probes need no credential.
//!
//! Calls name what a module lets callers reach **over HTTP**: only the
//! methods and transactions it declares `http` (its manifest's `http`
//! list) are callable, and any other name answers 404, internal
//! definition or not, as do an unknown database and module.
//!
//! A module with an **authorization hook** decides who each call runs as,
//! before any idempotency replay, query evaluation or watch delivery: the
//! hook verifies the presented credentials and answers a principal, or
//! denies (403 `unauthorized`). Credentials are the call body's
//! `credentials` member, or else `{"bearer": token}` for an
//! `Authorization: Bearer` token that is none of the server's own (the
//! server never hands its tokens to guest code). A watch is a `POST`
//! with a query's body, credentials included.
//!
//! A call's own failure (its code failed it, or ran out of its budget)
//! answers **422** with `{ok: false, code, message, details?}`, plus the
//! revision and position a write's failure committed at; the host's own
//! failures keep their statuses (403 refused, 404 unknown alias, 409
//! conflicting change, 503 retry elsewhere or later).
//!
//! Otherwise calls run as **anonymous** with the server clock. A request
//! carrying the admin token comes from a trusted gateway that
//! authenticated the end user: only then do the caller-asserted
//! `principal` and `timestamp_ms` fields apply, in call and watch bodies
//! alike. Without it those fields are ignored, so no caller can
//! impersonate another principal or rewrite the time its mutation
//! commits at. How long guest code may run is the called method's own
//! declaration; any caller can only shorten it, with its
//! `x-flower-timeout-ms` deadline. A hook sees an
//! asserted principal as its request's `principal`, and decides.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{DefaultBodyLimit, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Json, Response};
use axum::routing::{get, post};
use axum::{Router, middleware};
use flower_backup::driver::{BackupSummary, Driver as BackupDriver};
use flower_cluster::admission::AdmissionMetrics;
use flower_cluster::exec::with_engine;
use flower_cluster::health::{STORAGE_FAILED, StorageFailure};
use flower_cluster::logstore::LogStore;
use flower_cluster::network::constant_time_eq;
use flower_cluster::node::{
    ISOLATED, MEMBERSHIP, MembershipChange, Node, NodeError, Readiness, TRANSFER_FAILED,
    TRANSFERRING,
};
use flower_cluster::peer::{FORWARD_FAILED, Forwarder};
use flower_cluster::read::{ReadMetrics, ReadOptions, STALE_REPLICA, Served};
use flower_cluster::recovery::RECOVERING;
use flower_cluster::rejoin::REJOINING;
use flower_cluster::snapshot_policy::SnapshotPolicy;
use flower_engine::auth::UNAUTHORIZED;
use flower_engine::descriptors::MAX_STALENESS_MS;
use flower_engine::engine::{CallOptions, Engine, MutationOptions};
use flower_engine::error::IO_FAILURE;
use flower_engine::json::{canonicalize, parse_value, render};
use flower_engine::query_cache::Encoded;
use flower_sdk::abi::decode_envelope;
use flower_sdk::error::codes;
use flower_sdk::{Principal, Value};
use openraft::BasicNode;
use tokio_stream::StreamExt;

use crate::forward::{MAX_STALENESS, NotLeader, RouterForwarder, STALENESS};
use crate::listener::HttpOptions;
use crate::telemetry::TimedJson;
use crate::watch::HubMessage;

/// Default cap on a deploy request body: the base64 Wasm bundle plus
/// its manifest. The peer frame cap must stay several times larger:
/// the replicated entry encodes the bundle as a JSON byte array.
pub const DEFAULT_DEPLOY_LIMIT: usize = 64 * 1024 * 1024;
/// Default time in-flight requests get to finish on shutdown.
pub const DEFAULT_DRAIN: Duration = Duration::from_secs(30);
/// Code of a refused staged-deployment step: nothing staged, or not
/// ready to activate (409).
pub const DEPLOYMENT: &str = "deployment";
/// Code of a deployment refused while another of the module runs on
/// the member (409).
pub const DEPLOYMENT_IN_PROGRESS: &str = "deployment_in_progress";

/// Which credential an endpoint requires.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Role {
    /// Operator or trusted gateway.
    Admin,
    /// Queue workers.
    Worker,
    /// Sync replicas and devices.
    Sync,
}

/// Shared handler state.
#[derive(Clone)]
pub struct AppState {
    /// The cluster node serving calls.
    pub node: Arc<Node>,
    /// Watch hub sharing query evaluation across subscribers.
    pub hub: Arc<crate::watch::Hub>,
    /// Backup driver for admin-triggered backups, if configured.
    pub backup: Option<Arc<BackupDriver>>,
    /// Admin bearer token; admin endpoints 403 without one.
    pub admin_token: Option<Arc<str>>,
    /// Worker bearer token for the queue protocol.
    pub worker_token: Option<Arc<str>>,
    /// Sync bearer token for `/v1/sync`.
    pub sync_token: Option<Arc<str>>,
    /// Largest accepted deploy body, in bytes.
    pub deploy_limit: usize,
    /// Last admin-triggered backup summary.
    pub last_backup: Arc<tokio::sync::Mutex<Option<BackupSummary>>>,
    /// The soft snapshot triggers `serve` runs, which metrics report on.
    pub snapshot_policy: SnapshotPolicy,
    /// Applied writes and live watches, for `/v1/admin/activity`.
    pub activity: Arc<crate::activity::Activity>,
    /// Raft transitions and readiness for `/v1/admin/stream`.
    pub admin_events: Arc<crate::admin_stream::AdminEvents>,
    /// Whether `/admin` serves the dashboard ([`crate::dashboard`]).
    pub admin_ui: bool,
    /// The host this member's process serves every group of, when it
    /// hosts groups (see [`crate::groups`]): the catalog's operator
    /// routes and process-wide readiness read it.
    pub host: Option<Arc<flower_cluster::host::Host>>,
}

impl AppState {
    /// Share a node with no backup driver and no credentials.
    pub fn new(node: Arc<Node>) -> Self {
        AppState {
            hub: Arc::new(crate::watch::Hub::new(node.clone())),
            node,
            backup: None,
            admin_token: None,
            worker_token: None,
            sync_token: None,
            deploy_limit: DEFAULT_DEPLOY_LIMIT,
            last_backup: Arc::new(tokio::sync::Mutex::new(None)),
            snapshot_policy: SnapshotPolicy::default(),
            activity: Arc::default(),
            admin_events: Arc::default(),
            admin_ui: true,
            host: None,
        }
    }

    /// Serve as one group of `host`.
    pub fn with_host(mut self, host: Arc<flower_cluster::host::Host>) -> Self {
        self.host = Some(host);
        self
    }

    /// Serve the dashboard at `/admin`, or leave it out (404). On by
    /// default.
    pub fn with_admin_ui(mut self, enabled: bool) -> Self {
        self.admin_ui = enabled;
        self
    }

    /// Report the soft snapshot triggers in force on `/v1/metrics`.
    pub fn with_snapshot_policy(mut self, policy: SnapshotPolicy) -> Self {
        self.snapshot_policy = policy;
        self
    }

    /// Attach a backup driver.
    pub fn with_backup(mut self, backup: Arc<BackupDriver>) -> Self {
        self.backup = Some(backup);
        self
    }

    /// Configure the admin bearer token.
    pub fn with_admin_token(mut self, token: String) -> Self {
        self.admin_token = Some(token.into());
        self
    }

    /// Configure the worker bearer token for `/v1/queue/*`.
    pub fn with_worker_token(mut self, token: String) -> Self {
        self.worker_token = Some(token.into());
        self
    }

    /// Configure the sync bearer token for `/v1/sync`.
    pub fn with_sync_token(mut self, token: String) -> Self {
        self.sync_token = Some(token.into());
        self
    }

    /// Cap deploy request bodies, in bytes.
    pub fn with_deploy_limit(mut self, bytes: usize) -> Self {
        self.deploy_limit = bytes;
        self
    }

    /// Advertise the base URL clients reach this member's API at: while
    /// this member leads, replies name it so clients routing by leader
    /// send their writes there (see [`crate::forward`]).
    pub fn with_api_url(self, url: String) -> Self {
        self.node.set_api_url(Some(url));
        self
    }

    /// The node's shared engine, for backup snapshots.
    pub fn node_engine(&self) -> Arc<tokio::sync::Mutex<Engine>> {
        self.node.engine().clone()
    }

    /// The node's log store, for backup archiving.
    pub fn node_logs(&self) -> LogStore {
        self.node.logs().clone()
    }

    /// Whether the request presents a credential for `role`. The admin
    /// token holds every role.
    pub(crate) fn authorized(&self, headers: &HeaderMap, role: Role) -> bool {
        let Some(presented) = headers
            .get("authorization")
            .and_then(|value| value.as_bytes().strip_prefix(b"Bearer "))
        else {
            return false;
        };
        let matches = |token: &Option<Arc<str>>| {
            token
                .as_deref()
                .is_some_and(|token| constant_time_eq(token.as_bytes(), presented))
        };
        // Evaluate every candidate: no early exit reveals which matched.
        let admin = matches(&self.admin_token);
        let role_token = match role {
            Role::Admin => false,
            Role::Worker => matches(&self.worker_token),
            Role::Sync => matches(&self.sync_token),
        };
        admin | role_token
    }

    /// How much of a call's asserted identity and budget to honor, and
    /// the end-user bearer it presents.
    pub(crate) fn trust(&self, headers: &HeaderMap) -> Trust {
        Trust {
            gateway: self.authorized(headers, Role::Admin),
            bearer: self.end_user_bearer(headers),
        }
    }

    /// The request's bearer token when it is none of the server's own
    /// credentials: an end user's, for the authorization hook.
    fn end_user_bearer(&self, headers: &HeaderMap) -> Option<String> {
        let presented = headers
            .get("authorization")
            .and_then(|value| value.as_bytes().strip_prefix(b"Bearer "))?;
        let matches = |token: &Option<Arc<str>>| {
            token
                .as_deref()
                .is_some_and(|token| constant_time_eq(token.as_bytes(), presented))
        };
        // Evaluate every candidate: no early exit reveals which matched.
        let admin = matches(&self.admin_token);
        let worker = matches(&self.worker_token);
        let sync = matches(&self.sync_token);
        if admin | worker | sync {
            return None;
        }
        std::str::from_utf8(presented).ok().map(str::to_string)
    }
}

/// Whether a call came through a trusted gateway, and any end-user
/// bearer token.
#[derive(Clone, Debug)]
pub(crate) struct Trust {
    gateway: bool,
    bearer: Option<String>,
}

impl Trust {
    /// Engine options for a call through an HTTP alias: anonymous and
    /// the server clock unless a trusted gateway asserted them, and the
    /// request's deadline. Credentials are the body's, else the
    /// bearer's.
    pub(crate) fn call_options(
        &self,
        principal: Option<&PrincipalDto>,
        timestamp_ms: Option<u64>,
        credentials: Option<&serde_json::Value>,
    ) -> Result<CallOptions, ApiError> {
        let credentials = match credentials {
            Some(json) => args_from_json(json)?,
            None => self.bearer.as_ref().map(|token| {
                let mut map = BTreeMap::new();
                map.insert("bearer".to_string(), Value::String(token.clone()));
                Value::Object(map)
            }),
        };
        let mut options = CallOptions {
            credentials,
            external: true,
            deadline: flower_cluster::deadline::current().map(tokio::time::Instant::into_std),
            ..CallOptions::default()
        };
        if self.gateway {
            options.principal = principal.map(PrincipalDto::principal).transpose()?;
            options.timestamp_ms = timestamp_ms;
        }
        Ok(options)
    }
}

/// Build the API router. Requests only the leader can serve forward
/// there from a follower ([`crate::forward`]), after their credential
/// checks.
pub fn router(state: AppState) -> Router {
    let forward = || middleware::from_fn_with_state(state.clone(), crate::forward::layer);
    let admin = Router::new()
        .route(
            "/v1/deploy",
            post(post_deploy).route_layer(DefaultBodyLimit::max(state.deploy_limit)),
        )
        .route(
            "/v1/admin/membership",
            get(get_membership).post(post_membership),
        )
        .route("/v1/admin/leadership", post(post_leadership))
        .route("/v1/admin/backup", get(get_backup).post(post_backup))
        .route("/v1/admin/keys", get(get_keys).post(post_keys))
        .merge(crate::retention::admin_routes())
        .route("/v1/metrics", get(get_metrics))
        .merge(crate::timer::routes())
        .merge(crate::admin::routes(state.deploy_limit))
        .merge(crate::catalog::routes())
        .merge(crate::activity::routes())
        .merge(crate::admin_stream::routes())
        .layer(forward())
        .layer(middleware::from_fn_with_state(state.clone(), require_admin));
    let worker = crate::queue::routes()
        .layer(forward())
        .layer(middleware::from_fn_with_state(
            state.clone(),
            require_worker,
        ));
    let sync = Router::new()
        .route("/v1/sync", get(get_sync))
        .layer(middleware::from_fn_with_state(state.clone(), require_sync));
    // Static and credential-free: the data it shows comes from the
    // routes above, under their own checks.
    let dashboard = if state.admin_ui {
        crate::dashboard::routes()
    } else {
        Router::new()
    };
    Router::new()
        .route("/v1/query", post(post_query))
        .route("/v1/mutate", post(post_mutate))
        .route("/v1/transaction", post(post_transaction))
        .route("/v1/watch", post(post_watch))
        .merge(crate::retention::routes())
        .layer(forward())
        .layer(middleware::from_fn_with_state(
            state.clone(),
            crate::telemetry::server_timing,
        ))
        .route("/v1/status", get(get_status))
        .route(
            "/v1/protocol",
            get(|| async {
                Json(flower_protocol::Discovery {
                    version: flower_protocol::VERSION,
                    capabilities: flower_protocol::CAPABILITIES
                        .iter()
                        .map(|capability| (*capability).to_owned())
                        .collect(),
                })
            }),
        )
        .route("/health/live", get(get_live))
        .route("/health/ready", get(get_ready))
        .merge(admin)
        .merge(worker)
        .merge(sync)
        .merge(dashboard)
        // Every request runs under its client deadline, and is traced
        // when OpenTelemetry exports.
        .layer(middleware::from_fn(crate::forward::deadline))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            crate::telemetry::http,
        ))
        .layer(middleware::from_fn(crate::protocol::decorate))
        .with_state(state)
}

/// Serve the API on a bound listener until the task is dropped. The
/// watch hub pumps commit notices for as long as serving runs.
pub async fn serve(listener: tokio::net::TcpListener, state: AppState) -> std::io::Result<()> {
    serve_until(listener, state, std::future::pending(), DEFAULT_DRAIN).await
}

/// Serve the API until `shutdown` resolves, then shut down gracefully:
/// stop accepting connections, end every watch stream (subscribers
/// reconnect elsewhere), give in-flight requests up to `drain` to
/// finish, and stop the watch hub, which releases its node handle so
/// the caller can shut the node down.
pub async fn serve_until<F>(
    listener: tokio::net::TcpListener,
    state: AppState,
    shutdown: F,
    drain: Duration,
) -> std::io::Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    serve_with(listener, state, shutdown, drain, &HttpOptions::default()).await
}

/// [`serve_until`] with listener options: in-process TLS, HTTP/2 stream
/// limits (see [`crate::listener`]). HTTP/1.1 and HTTP/2 alike, h2c
/// without TLS.
pub async fn serve_with<F>(
    listener: tokio::net::TcpListener,
    state: AppState,
    shutdown: F,
    drain: Duration,
    options: &HttpOptions,
) -> std::io::Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let pump = tokio::spawn(state.hub.clone().run(stop_rx.clone()));
    let activity = tokio::spawn(
        state
            .activity
            .clone()
            .record(state.node.subscribe_commits(), stop_rx.clone()),
    );
    let admin_events = tokio::spawn(state.admin_events.clone().run(
        state.node.clone(),
        state.host.clone(),
        stop_rx,
    ));
    // Followers' forwarded requests run through this API while it
    // serves. The member holds it weakly: dropping this ends that.
    let forwarder: Arc<dyn Forwarder> = RouterForwarder::new(router(state.clone()));
    state
        .node
        .member_desk()
        .set_forwarder(Arc::downgrade(&forwarder));
    // Shutdown ends every watch stream at once, so subscribers reconnect
    // elsewhere and the drain is not held open by them.
    let stopping = {
        let stop_tx = stop_tx.clone();
        let activity = state.activity.clone();
        let admin_events = state.admin_events.clone();
        async move {
            shutdown.await;
            let _ = stop_tx.send(true);
            activity.close();
            admin_events.close();
        }
    };
    let result = crate::listener::run(listener, router(state), options, stopping, drain).await;
    let _ = stop_tx.send(true);
    let _ = pump.await;
    let _ = activity.await;
    let _ = admin_events.await;
    drop(forwarder);
    result
}

/// Reject requests without the configured credential for `role`.
async fn require(
    state: &AppState,
    role: Role,
    request: axum::extract::Request,
    next: middleware::Next,
) -> Response {
    if !state.authorized(request.headers(), role) {
        let message = match role {
            Role::Admin => "admin token missing or wrong",
            Role::Worker => "worker or admin token missing or wrong",
            Role::Sync => "sync or admin token missing or wrong",
        };
        return ApiError::Forbidden(message.to_string()).into_response();
    }
    next.run(request).await
}

/// Admin-only routes.
async fn require_admin(
    State(state): State<AppState>,
    request: axum::extract::Request,
    next: middleware::Next,
) -> Response {
    require(&state, Role::Admin, request, next).await
}

/// Worker (or admin) routes.
async fn require_worker(
    State(state): State<AppState>,
    request: axum::extract::Request,
    next: middleware::Next,
) -> Response {
    require(&state, Role::Worker, request, next).await
}

/// Sync (or admin) routes.
async fn require_sync(
    State(state): State<AppState>,
    request: axum::extract::Request,
    next: middleware::Next,
) -> Response {
    require(&state, Role::Sync, request, next).await
}

/// A handler error: HTTP status plus a JSON body.
pub(crate) enum ApiError {
    /// Malformed request.
    BadRequest(String),
    /// Authorization failed.
    Forbidden(String),
    /// Not the leader.
    Forward { leader: Option<u64> },
    /// Host failure with engine code and message.
    Failure { code: String, message: String },
    /// A call's own failure carrying details (a query's declared error
    /// variant with its payload): answered as a failure with that code,
    /// its details in the body.
    Call {
        status: StatusCode,
        code: String,
        message: String,
        details: serde_json::Value,
    },
    /// A request refused for what it asked, with its own code: 400.
    Refused { code: String, message: String },
    /// Fatal Raft failure.
    Fatal(String),
    /// Unconfigured capability.
    Unavailable(String),
    /// Admission refused the request: retry after the hint.
    Overloaded {
        message: String,
        retry_after_ms: u64,
    },
}

impl ApiError {
    /// One line saying what failed, as the JSON body's code and message
    /// would.
    pub(crate) fn describe(&self) -> String {
        match self {
            ApiError::BadRequest(message) => format!("bad_request: {message}"),
            ApiError::Forbidden(message) => format!("forbidden: {message}"),
            ApiError::Forward { leader } => format!("forward: leader {leader:?}"),
            ApiError::Failure { code, message } | ApiError::Call { code, message, .. } => {
                format!("{code}: {message}")
            }
            ApiError::Refused { code, message } => format!("{code}: {message}"),
            ApiError::Fatal(message) => format!("fatal: {message}"),
            ApiError::Unavailable(message) => format!("unavailable: {message}"),
            ApiError::Overloaded { message, .. } => format!("overloaded: {message}"),
        }
    }

    /// Map a node error: forwards stay forwards, everything else is
    /// a host failure with its code.
    pub(crate) fn node(error: NodeError) -> Self {
        if let Some(failure) = error.call_failure()
            && crate::groups::placement_status(&failure.code).is_some()
        {
            // Preserve the routing extension installed for placement refusals.
            return Self::Failure {
                code: failure.code,
                message: failure.message,
            };
        }
        if let Some(failure) = error.call_failure()
            && let Some(Ok(details)) = failure.details.as_ref().map(value_to_json)
        {
            let status = match failure_status(&failure.code) {
                StatusCode::INTERNAL_SERVER_ERROR => StatusCode::UNPROCESSABLE_ENTITY,
                status => status,
            };
            return ApiError::Call {
                status,
                code: failure.code,
                message: failure.message,
                details,
            };
        }
        match error {
            NodeError::Forward { leader } => ApiError::Forward { leader },
            NodeError::Failure { code, message } | NodeError::Call { code, message, .. } => {
                ApiError::Failure { code, message }
            }
            NodeError::Fatal(message) => ApiError::Fatal(message),
            NodeError::Overloaded {
                message,
                retry_after_ms,
            } => ApiError::Overloaded {
                message,
                retry_after_ms,
            },
        }
    }
}

/// Whether `code` is a call's own failure rather than the host's: codes
/// the call's code chose (`UPPER_SNAKE_CASE`, as every SDK and guest code
/// is) and the limits it ran out of. The host's own codes are lowercase.
pub(crate) fn own_failure(code: &str) -> bool {
    use flower_engine::runtime::wasm;
    [
        codes::TYPE_MISMATCH,
        codes::MISSING_FIELD,
        codes::INVALID_VALUE,
        codes::DEPTH_LIMIT,
        codes::REDACTED_VALUE,
        codes::SCHEMA_MISMATCH,
        codes::INVALID_ARGUMENT,
        codes::SQL_INVALID,
        codes::SQL_BUDGET,
        codes::INVALID_INVOCATION,
        codes::MODULE_PRIVATE,
        codes::MODULE_READ_ONLY,
        codes::MODULE_CONTRACT_VIOLATION,
        codes::UNKNOWN_KEY,
        codes::FORBIDDEN_USAGE,
        codes::BAD_SIGNATURE,
    ]
    .contains(&code)
        || [
            wasm::FUEL_EXHAUSTED,
            wasm::DEADLINE_EXCEEDED,
            wasm::OUT_OF_MEMORY,
            wasm::STACK_OVERFLOW,
            wasm::NESTING_TOO_DEEP,
            wasm::INIT_FAILED,
            wasm::HOST_TRAP,
        ]
        .contains(&code)
}

/// The status a failure with `code` answers with. Quorum loss is
/// retryable, and so is a member whose storage failed or that still
/// recovers its log, a replica too stale to prove a fresh read, or a
/// forward whose outcome is unknown: another attempt, or another member,
/// can serve the request. A call's own failure is 422, as v1 answered
/// it. Other host failures are 500.
fn failure_status(code: &str) -> StatusCode {
    if code == "invalid_read_token" {
        return StatusCode::BAD_REQUEST;
    }
    if code == "wrong_history" {
        return StatusCode::CONFLICT;
    }
    if code == "quorum"
        || code == STORAGE_FAILED
        || code == IO_FAILURE
        || code == RECOVERING
        || code == STALE_REPLICA
        || code == FORWARD_FAILED
    {
        StatusCode::SERVICE_UNAVAILABLE
    } else if code == UNAUTHORIZED || code == codes::ACCESS_DENIED {
        // The authorization hook or an access rule refused.
        StatusCode::FORBIDDEN
    } else if code == codes::UNKNOWN_METHOD {
        // No such database, module, or name callable over HTTP.
        StatusCode::NOT_FOUND
    } else if code == TRANSFERRING || code == REJOINING || code == ISOLATED {
        // Leadership is moving, or this member is catching up: retry,
        // there or at another member.
        StatusCode::SERVICE_UNAVAILABLE
    } else if code == flower_engine::purge::QUEUE_PURGING
        || code == flower_cluster::keys::KEYS_LOCKED
    {
        // A purge is deleting the queue (retry once it ends), or this
        // member cannot unwrap the key catalog (retry once one that can
        // leads): nothing the request did, and nothing changed.
        StatusCode::SERVICE_UNAVAILABLE
    } else if code == MEMBERSHIP
        || code == TRANSFER_FAILED
        || code == DEPLOYMENT
        || code == DEPLOYMENT_IN_PROGRESS
        || code == codes::MODULE_CONFLICT
    {
        // A refused or uncertain operator change: inspect membership,
        // the staging, or the modules and their links, before retrying.
        StatusCode::CONFLICT
    } else if code == flower_engine::patch::TRANSACTION_TOO_LARGE {
        // Its committed effects outgrow the process's bound.
        StatusCode::PAYLOAD_TOO_LARGE
    } else if code == flower_engine::runtime::wasm::INSTANCES_EXHAUSTED
        || code == flower_engine::runtime::wasm::INSTANTIATION_FAILED
        || code == flower_engine::host::MODULE_UNAVAILABLE
    {
        // Out of guest instances for now (room frees as calls end), an
        // instance that could not start, a module this member holds no
        // runtime for: the host's, which another attempt or member may
        // serve.
        StatusCode::SERVICE_UNAVAILABLE
    } else if code == flower_engine::runtime::wasm::CALLER_DEADLINE_EXCEEDED {
        // The caller stopped waiting before the call finished, maybe only
        // on a loaded member: an attempt with more time may succeed (v1
        // answered a client deadline's refusals 503). A call that ran past
        // its own deadline (`deadline_exceeded`) is its own failure, 422,
        // as v1's `EVALUATION_BUDGET` was.
        StatusCode::SERVICE_UNAVAILABLE
    } else if own_failure(code) {
        StatusCode::UNPROCESSABLE_ENTITY
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    }
}

/// The HTTP status a failure with `code` answers with: what clients and
/// workers meet for it.
pub fn status_of(code: &str) -> u16 {
    failure_status(code).as_u16()
}

/// Marks a failure response with its code, for layers that act on it
/// (see [`crate::groups`]).
#[derive(Clone, Debug)]
pub(crate) struct FailureCode(pub(crate) String);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, body) = match self {
            ApiError::BadRequest(message) => (
                StatusCode::BAD_REQUEST,
                serde_json::json!({"ok": false, "code": "bad_request", "message": message}),
            ),
            ApiError::Forbidden(message) => (
                StatusCode::FORBIDDEN,
                serde_json::json!({"ok": false, "code": "forbidden", "message": message}),
            ),
            ApiError::Forward { leader } => {
                // Marked, so the forwarding layer sends the request on
                // to the leader (see `crate::forward`).
                let body = serde_json::json!({"ok": false, "code": "forward", "leader": leader});
                let mut response = (StatusCode::SERVICE_UNAVAILABLE, Json(body)).into_response();
                response.extensions_mut().insert(NotLeader { leader });
                return response;
            }
            ApiError::Failure { code, message } => {
                if let Some(status) = crate::groups::placement_status(&code) {
                    // A partition serving elsewhere, or moving: routing
                    // retries, and so may the client, shortly.
                    let retry = crate::groups::MOVING_RETRY.as_millis() as u64;
                    let body = serde_json::json!({
                        "ok": false,
                        "code": code,
                        "message": message,
                        "retry_after_ms": retry,
                    });
                    let mut response =
                        (status, [(axum::http::header::RETRY_AFTER, "1")], Json(body))
                            .into_response();
                    response.extensions_mut().insert(FailureCode(code));
                    return response;
                }
                (
                    failure_status(&code),
                    serde_json::json!({"ok": false, "code": code, "message": message}),
                )
            }
            // Only a call's own failure carries details: whatever its
            // code, it is 422, so clients classify by status alone.
            ApiError::Call {
                status,
                code,
                message,
                details,
            } => (
                status,
                serde_json::json!({
                    "ok": false,
                    "code": code,
                    "message": message,
                    "details": details,
                }),
            ),
            ApiError::Refused { code, message } => (
                StatusCode::BAD_REQUEST,
                serde_json::json!({"ok": false, "code": code, "message": message}),
            ),
            ApiError::Fatal(message) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                serde_json::json!({"ok": false, "code": "fatal", "message": message}),
            ),
            ApiError::Unavailable(message) => (
                StatusCode::SERVICE_UNAVAILABLE,
                serde_json::json!({"ok": false, "code": "unavailable", "message": message}),
            ),
            ApiError::Overloaded {
                message,
                retry_after_ms,
            } => {
                // Retryable by definition: clients back off at least
                // the hint, in whole seconds as the header carries it.
                let seconds = retry_after_ms.div_ceil(1000).max(1);
                let body = serde_json::json!({
                    "ok": false,
                    "code": "overloaded",
                    "message": message,
                    "retry_after_ms": retry_after_ms,
                });
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    [(axum::http::header::RETRY_AFTER, seconds.to_string())],
                    Json(body),
                )
                    .into_response();
            }
        };
        (status, Json(body)).into_response()
    }
}

/// Run engine work for a handler, off the async workers.
pub(crate) async fn engine_read<T, F>(state: &AppState, work: F) -> Result<T, ApiError>
where
    T: Send + 'static,
    F: FnOnce(&mut Engine) -> T + Send + 'static,
{
    with_engine(state.node.engine(), work)
        .await
        .map_err(ApiError::Fatal)
}

/// A principal asserted by a trusted gateway.
pub(crate) type PrincipalDto = flower_protocol::request::CallPrincipal;

trait PrincipalWire {
    fn principal(&self) -> Result<Principal, ApiError>;
}
impl PrincipalWire for PrincipalDto {
    /// Validate into an engine principal.
    fn principal(&self) -> Result<Principal, ApiError> {
        let claims = self
            .claims
            .as_ref()
            .map(args_from_json)
            .transpose()?
            .flatten();
        Principal::new(self.subject.clone(), self.tenant.clone(), claims).map_err(|failure| {
            ApiError::BadRequest(format!("{}: {}", failure.code, failure.message))
        })
    }
}

/// Shared call fields.
type CallRequest = flower_protocol::request::QueryRequest;

trait CallWire {
    fn call_options(&self, trust: &Trust) -> Result<CallOptions, ApiError>;
}
impl CallWire for CallRequest {
    /// Engine call options under the request's trust.
    fn call_options(&self, trust: &Trust) -> Result<CallOptions, ApiError> {
        trust.call_options(
            self.principal.as_ref(),
            self.timestamp_ms,
            self.credentials.as_ref(),
        )
    }
}

/// A query request.
type QueryRequest = CallRequest;

/// A mutation request.
type MutateRequest = flower_protocol::request::MutationRequest;

/// HTTP wrapper over the shared wire envelope.
#[derive(serde::Serialize, Clone, Debug)]
#[serde(transparent)]
pub(crate) struct CallResponse(flower_protocol::CallEnvelope);
impl std::ops::Deref for CallResponse {
    type Target = flower_protocol::CallEnvelope;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl std::ops::DerefMut for CallResponse {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl IntoResponse for CallResponse {
    fn into_response(self) -> Response {
        let status = if self.ok {
            StatusCode::OK
        } else if self.code.as_deref() == Some(flower_engine::patch::TRANSACTION_TOO_LARGE) {
            StatusCode::PAYLOAD_TOO_LARGE
        } else {
            StatusCode::UNPROCESSABLE_ENTITY
        };
        (status, Json(self)).into_response()
    }
}

impl CallResponse {
    /// A failure response at a revision and position.
    fn failed(
        code: String,
        message: String,
        response: &flower_cluster::types::WriteResponse,
    ) -> Self {
        CallResponse(flower_protocol::CallEnvelope {
            ok: false,
            value: None,
            revision: Some(response.revision),
            position: Some(response.position),
            code: Some(code),
            message: Some(message),
            revisions: revisions_from(response),
            positions: positions_from(response),
            details: None,
            duplicate: response.duplicate,
            read_token: None,
        })
    }
}

/// Convert request JSON to canonical engine args.
fn args_from_json(json: &serde_json::Value) -> Result<Option<Value>, ApiError> {
    if json.is_null() {
        return Ok(None);
    }
    let text = serde_json::to_string(json)
        .map_err(|error| ApiError::BadRequest(format!("args encode: {error}")))?;
    let canonical = canonicalize(text.as_bytes())
        .map_err(|error| ApiError::BadRequest(format!("args canonicalize: {error}")))?;
    parse_value(&canonical)
        .map(Some)
        .map_err(|error| ApiError::BadRequest(format!("args parse: {error}")))
}

/// A render failure as a host failure.
fn render_failure(error: impl ToString) -> ApiError {
    ApiError::Failure {
        code: "render".to_string(),
        message: error.to_string(),
    }
}

/// A succeeded call's answer carrying only `value`, as [`CallResponse`]
/// renders one, around the value's JSON as it is (the bytes
/// `CallResponse` prints for it, see [`Encoded`]): a cached query result
/// goes out without being cloned, rendered or parsed.
fn value_response(value: &Encoded) -> Response {
    const HEAD: &[u8] = br#"{"ok":true,"value":"#;
    let mut body = Vec::with_capacity(HEAD.len() + value.bytes().len() + 1);
    body.extend_from_slice(HEAD);
    body.extend_from_slice(value.bytes());
    body.push(b'}');
    (
        [(
            axum::http::header::CONTENT_TYPE,
            axum::http::HeaderValue::from_static("application/json"),
        )],
        body,
    )
        .into_response()
}

/// Convert an engine value to response JSON.
pub(crate) fn value_to_json(value: &Value) -> Result<serde_json::Value, ApiError> {
    let bytes = render(value).map_err(render_failure)?;
    serde_json::from_slice(&bytes).map_err(render_failure)
}

/// Map a patch/mutation write response to the call contract.
pub(crate) fn response_from(
    response: &flower_cluster::types::WriteResponse,
) -> Result<CallResponse, ApiError> {
    use flower_cluster::types::ResponseOutcome;
    match &response.outcome {
        ResponseOutcome::Value(bytes) => {
            let value: serde_json::Value = serde_json::from_slice(bytes).map_err(render_failure)?;
            Ok(CallResponse(flower_protocol::CallEnvelope {
                ok: true,
                value: Some(value),
                revision: Some(response.revision),
                position: Some(response.position),
                code: None,
                message: None,
                revisions: revisions_from(response),
                positions: positions_from(response),
                details: None,
                duplicate: response.duplicate,
                read_token: None,
            }))
        }
        ResponseOutcome::Failure(bytes) => {
            let value = parse_value(bytes).map_err(render_failure)?;
            let (code, message, details) = match decode_envelope(value) {
                Err(failure) => (failure.code, failure.message, failure.details),
                Ok(_) => ("business".to_string(), "business failure".to_string(), None),
            };
            if crate::groups::placement_status(&code).is_some() {
                return Err(ApiError::Failure { code, message });
            }
            let mut failed = CallResponse::failed(code, message, response);
            failed.details = details.as_ref().map(value_to_json).transpose()?;
            Ok(failed)
        }
        ResponseOutcome::Error(message) => {
            let (code, detail) = match message.split_once(": ") {
                Some((code, detail)) => (code.to_string(), detail.to_string()),
                None => ("error".to_string(), message.clone()),
            };
            if crate::groups::placement_status(&code).is_some() {
                return Err(ApiError::Failure {
                    code,
                    message: detail,
                });
            }
            Ok(CallResponse::failed(code, detail, response))
        }
    }
}

/// Per-participant revisions for a write response, if any.
fn revisions_from(
    response: &flower_cluster::types::WriteResponse,
) -> Option<BTreeMap<String, u64>> {
    if response.revisions.is_empty() {
        return None;
    }
    Some(
        response
            .revisions
            .iter()
            .map(|(db, revision)| (String::from_utf8_lossy(db).into_owned(), *revision))
            .collect(),
    )
}

/// Per-group log positions for a write response across groups, if any.
fn positions_from(
    response: &flower_cluster::types::WriteResponse,
) -> Option<BTreeMap<String, u64>> {
    (!response.positions.is_empty()).then(|| response.positions.iter().cloned().collect())
}

/// The caller's staleness bound for replica-local queries, from
/// [`MAX_STALENESS`]: whole milliseconds from 0 to 2^31 - 1.
fn read_options(headers: &HeaderMap) -> Result<ReadOptions, ApiError> {
    let Some(value) = headers.get(MAX_STALENESS) else {
        return Ok(ReadOptions::default());
    };
    let invalid = || {
        ApiError::BadRequest(format!(
            "{MAX_STALENESS} must be an integer from 0 to {MAX_STALENESS_MS}"
        ))
    };
    let text = value.to_str().map_err(|_| invalid())?;
    if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(invalid());
    }
    let ms: u64 = text.parse().map_err(|_| invalid())?;
    if ms > u64::from(MAX_STALENESS_MS) {
        return Err(invalid());
    }
    Ok(ReadOptions {
        max_staleness: Some(Duration::from_millis(ms)),
        at_least: None,
    })
}

/// Run a query the way it declares: linearizable ones fresh on any
/// member, replica-local ones from this member's state within the
/// caller's [`MAX_STALENESS`] bound, saying how stale in [`STALENESS`].
#[tracing::instrument(skip(state, headers, body), fields(db = %body.db, module = %body.module, name = %body.name))]
async fn post_query(
    State(state): State<AppState>,
    headers: HeaderMap,
    TimedJson(body): TimedJson<QueryRequest>,
) -> Result<Response, ApiError> {
    let args = args_from_json(&body.args)?.unwrap_or(Value::Null);
    let options = body.call_options(&state.trust(&headers))?;
    let mut read = read_options(&headers)?;
    read.at_least = headers
        .get(flower_protocol::AT_LEAST_HEADER)
        .map(|value| {
            value
                .to_str()
                .map_err(|e| ApiError::BadRequest(e.to_string()))
                .and_then(|text| {
                    flower_protocol::ReadToken::decode(text).map_err(ApiError::BadRequest)
                })
        })
        .transpose()?;
    let (encoded, token, served) = state
        .node
        .query_read_token_encoded(
            body.db.as_bytes(),
            &body.module,
            &body.name,
            &args,
            &options,
            &read,
        )
        .await
        .map_err(ApiError::node)?;
    let mut response = value_response(&encoded);
    if let Some(token) = token {
        response.headers_mut().insert(
            flower_protocol::TOKEN_HEADER,
            token
                .encode()
                .map_err(ApiError::BadRequest)?
                .parse()
                .map_err(|e: axum::http::header::InvalidHeaderValue| {
                    ApiError::BadRequest(e.to_string())
                })?,
        );
    }
    let consistency = if matches!(served, Served::Fresh) {
        "linearizable"
    } else {
        "replica_local"
    };
    response.headers_mut().insert(
        flower_protocol::CONSISTENCY_HEADER,
        axum::http::HeaderValue::from_static(consistency),
    );
    if let Served::Local(staleness) = served {
        let staleness = staleness.map_or_else(
            || "unknown".to_string(),
            |staleness| staleness.as_millis().to_string(),
        );
        if let Ok(value) = axum::http::HeaderValue::from_str(&staleness) {
            response.headers_mut().insert(STALENESS, value);
        }
    }
    Ok(response)
}

/// Run a mutation through consensus.
#[tracing::instrument(skip(state, headers, body), fields(db = %body.call.db, module = %body.call.module, name = %body.call.name))]
async fn post_mutate(
    State(state): State<AppState>,
    headers: HeaderMap,
    TimedJson(body): TimedJson<MutateRequest>,
) -> Result<CallResponse, ApiError> {
    let args = args_from_json(&body.call.args)?.unwrap_or(Value::Null);
    let call = body.call.call_options(&state.trust(&headers))?;
    // An `f1:`/`f2:` request ID is a scoped retry identity, whose
    // receipt pins the call's business intent; any other key is the
    // caller's own.
    let (retry, idempotency_key) = match body.idempotency_key {
        None => (None, None),
        Some(key) => match flower_engine::retention::parse_request_id(&key) {
            None => (None, Some(key.into_bytes())),
            Some(identity) => {
                let identity = identity.map_err(|failure| ApiError::BadRequest(failure.message))?;
                let fingerprint = flower_engine::retention::request_fingerprint(
                    &body.call.module,
                    &body.call.name,
                    &args,
                    body.expected_revision,
                )
                .map_err(|failure| ApiError::BadRequest(failure.message))?;
                let retry = flower_engine::engine::RetryIdentity {
                    identity,
                    fingerprint,
                };
                (Some(retry), None)
            }
        },
    };
    let options = MutationOptions {
        call,
        retry,
        idempotency_key,
        expected_revision: body.expected_revision,
    };
    let response = state
        .node
        .mutate(
            body.call.db.as_bytes(),
            &body.call.module,
            &body.call.name,
            &args,
            &options,
        )
        .await
        .map_err(ApiError::node)?;
    let mut result = response_from(&response)?;
    result.read_token = state
        .node
        .read_token(
            body.call.db.as_bytes(),
            response.revision,
            if response.duplicate {
                0
            } else {
                response.position
            },
        )
        .await
        .map_err(ApiError::node)?;
    Ok(result)
}

/// A multi-database transaction request.
type TransactionRequest = flower_protocol::request::TransactionRequest;

/// Run a multi-database transaction through consensus.
#[tracing::instrument(skip(state, headers, body), fields(steps = %body.steps.len()))]
async fn post_transaction(
    State(state): State<AppState>,
    headers: HeaderMap,
    TimedJson(body): TimedJson<TransactionRequest>,
) -> Result<CallResponse, ApiError> {
    let mut steps = Vec::with_capacity(body.steps.len());
    for step in &body.steps {
        let args = args_from_json(&step.args)?.unwrap_or(Value::Null);
        steps.push(flower_engine::engine::MultiStep {
            db: step.db.as_bytes().to_vec(),
            module: step.module.clone(),
            method: step.method.clone(),
            args,
        });
    }
    let call = state.trust(&headers).call_options(
        body.principal.as_ref(),
        body.timestamp_ms,
        body.credentials.as_ref(),
    )?;
    let options = MutationOptions {
        call,
        retry: None,
        idempotency_key: body.idempotency_key.map(String::into_bytes),
        expected_revision: None,
    };
    // On a host, steps in several groups commit by two-phase commit
    // from this, the first step's group (see `flower_cluster::txn`).
    let response = match &state.host {
        Some(host) => host.distributed_transaction(&steps, &options).await,
        None => state.node.transaction(&steps, &options).await,
    }
    .map_err(ApiError::node)?;
    response_from(&response)
}

/// Deploy a module through consensus (admin).
#[derive(serde::Deserialize, Clone, Debug)]
struct DeployRequest {
    /// Database name (UTF-8).
    db: String,
    /// Module name.
    module: String,
    /// Target identity as 64 hex chars; computed from content when absent.
    identity: Option<String>,
    /// Wasm bundle, standard base64.
    wasm_b64: String,
    /// Module manifest as JSON.
    manifest: serde_json::Value,
}

/// Decode 64 hex characters into a 32-byte identity. Works on bytes,
/// so non-ASCII input is rejected instead of splitting a character.
pub fn decode_identity_hex(text: &str) -> Result<[u8; 32], String> {
    fn nibble(byte: u8) -> Option<u8> {
        match byte {
            b'0'..=b'9' => Some(byte - b'0'),
            b'a'..=b'f' => Some(byte - b'a' + 10),
            b'A'..=b'F' => Some(byte - b'A' + 10),
            _ => None,
        }
    }
    let bytes = text.as_bytes();
    if bytes.len() != 64 {
        return Err(format!(
            "identity must be 64 hex chars, got {} bytes",
            bytes.len()
        ));
    }
    let mut identity = [0u8; 32];
    for (out, pair) in identity.iter_mut().zip(bytes.as_chunks::<2>().0) {
        *out = match (nibble(pair[0]), nibble(pair[1])) {
            (Some(high), Some(low)) => (high << 4) | low,
            _ => return Err("identity must be hex".to_string()),
        };
    }
    Ok(identity)
}

/// Decode a deployment's bundle: the Wasm from base64, the manifest
/// canonicalized, and the identity given or computed from both.
pub(crate) fn decode_bundle(
    wasm_b64: &str,
    manifest: &serde_json::Value,
    identity: Option<&str>,
) -> Result<(Vec<u8>, Value, [u8; 32]), ApiError> {
    use base64::Engine;
    let wasm = base64::prelude::BASE64_STANDARD
        .decode(wasm_b64.as_bytes())
        .map_err(|error| ApiError::BadRequest(format!("wasm base64: {error}")))?;
    let text = serde_json::to_string(manifest)
        .map_err(|error| ApiError::BadRequest(format!("manifest encode: {error}")))?;
    let canonical = canonicalize(text.as_bytes())
        .map_err(|error| ApiError::BadRequest(format!("manifest canonicalize: {error}")))?;
    let manifest = parse_value(&canonical)
        .map_err(|error| ApiError::BadRequest(format!("manifest parse: {error}")))?;
    let identity = match identity {
        Some(text) => decode_identity_hex(text).map_err(ApiError::BadRequest)?,
        None => flower_crypto::hash::content_identity(&wasm, &canonical),
    };
    Ok((wasm, manifest, identity))
}

#[tracing::instrument(skip(state, body), fields(db = %body.db, module = %body.module))]
async fn post_deploy(
    State(state): State<AppState>,
    Json(body): Json<DeployRequest>,
) -> Result<CallResponse, ApiError> {
    let (wasm, manifest, identity) =
        decode_bundle(&body.wasm_b64, &body.manifest, body.identity.as_deref())?;
    let response = state
        .node
        .deploy(body.db.as_bytes(), &body.module, identity, &wasm, &manifest)
        .await
        .map_err(ApiError::node)?;
    response_from(&response)
}

/// Sync batch query parameters.
#[derive(serde::Deserialize, Clone, Debug)]
struct SyncParams {
    /// Position to continue after.
    since: u64,
    /// Maximum entries; defaults to 512, capped at 4096.
    limit: Option<u64>,
}

type SyncEntryDto = flower_protocol::SyncEntry<serde_json::Value>;
type SyncSnapshotDto = flower_protocol::SyncSnapshot;
type SyncDto = flower_protocol::SyncBatch<serde_json::Value>;

/// A watch: a query's call fields, plus whether the client applies
/// `patch` events.
type WatchRequest = flower_protocol::request::WatchRequest;

/// Stream query snapshots as server-sent events: one `snapshot`
/// immediately, then one per change to the result, plus keepalive
/// comments. The body is a query's (`{db, module, name, args,
/// credentials?, principal?}`) plus `deltas`. Every value carries an
/// `id`; with `deltas: true` a change may arrive as a `patch` event
/// instead, an RFC 6902 patch to the value whose `id` it names as
/// `base`. A failed refresh sends one `error` event (`{code, message,
/// details?}`, the details of a call's own failure as a query answers
/// them) and ends the stream; a failure before the first snapshot is an
/// ordinary error response.
async fn post_watch(
    State(state): State<AppState>,
    headers: HeaderMap,
    TimedJson(body): TimedJson<WatchRequest>,
) -> Result<Sse<impl futures_util::Stream<Item = Result<Event, std::convert::Infallible>>>, ApiError>
{
    let args = args_from_json(&body.call.args)?.unwrap_or(Value::Null);
    let options = body.call.call_options(&state.trust(&headers))?;
    let call = &body.call;
    let (initial, rx) = state
        .hub
        .subscribe_with(
            call.db.as_bytes(),
            &call.module,
            &call.name,
            &args,
            &options,
            body.deltas,
        )
        .await
        .map_err(ApiError::node)?;
    let history = state
        .node
        .read_token(call.db.as_bytes(), initial.revision, 0)
        .await
        .map_err(ApiError::node)?
        .map(|token| flower_protocol::WatchHistory {
            group: token.group,
            incarnation: token.incarnation,
        });
    let first = tokio_stream::once(Ok::<_, std::convert::Infallible>(snapshot_event(
        &initial,
        history.as_ref(),
    )));
    // Counted as a live watch from its first snapshot until the stream
    // drops, however it ends.
    let watched = state
        .activity
        .watch(&call.db, &call.module, &call.name, initial.revision);
    let rest = tokio_stream::wrappers::ReceiverStream::new(rx).map(move |message| {
        if let HubMessage::Snapshot(event) = &message {
            watched.update(event.revision);
        }
        hub_event(message, history.as_ref())
    });
    Ok(Sse::new(first.chain(rest)).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("watching"),
    ))
}

/// Render a hub event as a snapshot SSE event.
fn snapshot_event(
    event: &crate::watch::HubEvent,
    history: Option<&flower_protocol::WatchHistory>,
) -> Event {
    let data = format!(
        "{{\"revision\":{},\"id\":{},\"history\":{},\"value\":{}}}",
        event.revision,
        event.id,
        serde_json::to_string(&history).expect("history serializes"),
        String::from_utf8_lossy(&event.value)
    );
    Event::default().event("snapshot").data(data)
}

/// Map a hub message into an infallible SSE result.
fn hub_event(
    message: HubMessage,
    history: Option<&flower_protocol::WatchHistory>,
) -> Result<Event, std::convert::Infallible> {
    Ok(match message {
        HubMessage::Snapshot(event) => snapshot_event(&event, history),
        HubMessage::Delta(delta) => Event::default().event("patch").data(format!(
            "{{\"revision\":{},\"id\":{},\"base\":{},\"patch\":{}}}",
            delta.revision, delta.id, delta.base, delta.patch
        )),
        HubMessage::Error {
            status,
            code,
            message,
            details,
        } => {
            let mut body = serde_json::json!({"ok": false, "code": code, "message": message});
            if let Some(details) = details.and_then(|json| serde_json::from_str(&json).ok()) {
                body["details"] = details;
            }
            let problem = flower_protocol::Problem::from_body(
                status,
                &serde_json::to_vec(&body).expect("error serializes"),
                None,
            );
            body["problem"] = serde_json::to_value(problem).expect("problem serializes");
            Event::default().event("error").data(body.to_string())
        }
    })
}

/// Serve a sync batch to a replica (sync or admin credential).
async fn get_sync(
    State(state): State<AppState>,
    Query(params): Query<SyncParams>,
) -> Result<Json<SyncDto>, ApiError> {
    use base64::Engine;
    let batch = state
        .node
        .sync_since(params.since, params.limit.unwrap_or(512))
        .map_err(ApiError::node)?;
    let mut entries = Vec::with_capacity(batch.entries.len());
    for entry in &batch.entries {
        entries.push(SyncEntryDto {
            index: entry.index,
            write: serde_json::to_value(&entry.write).map_err(render_failure)?,
        });
    }
    let snapshot = batch.snapshot.map(|snapshot| SyncSnapshotDto {
        last_index: snapshot.last_index,
        bytes_b64: base64::prelude::BASE64_STANDARD.encode(&snapshot.bytes),
    });
    Ok(Json(SyncDto {
        version: flower_protocol::VERSION,
        since: batch.since,
        entries,
        current: batch.current,
        snapshot,
        incarnation: batch.incarnation,
    }))
}

/// Raft status on the wire.
#[derive(serde::Serialize, Clone, Debug)]
pub(crate) struct StatusDto {
    /// Node ID.
    id: u64,
    /// Server state name.
    state: String,
    /// Current leader, if known.
    leader: Option<u64>,
    /// Current term.
    term: u64,
    /// Last appended log index.
    last_log: Option<u64>,
    /// Last applied log index.
    last_applied: Option<u64>,
}

impl StatusDto {
    /// The node's current status.
    pub(crate) fn of(node: &Node) -> Self {
        let status = node.status();
        StatusDto {
            id: status.id,
            state: status.state,
            leader: status.leader,
            term: status.term,
            last_log: status.last_log,
            last_applied: status.last_applied,
        }
    }
}

/// Report Raft status.
async fn get_status(State(state): State<AppState>) -> Json<StatusDto> {
    Json(StatusDto::of(&state.node))
}

/// Liveness: the process is up and serving HTTP.
async fn get_live() -> Json<serde_json::Value> {
    Json(serde_json::json!({"ok": true}))
}

/// Readiness: storage hasn't failed, a leader is known, this member
/// applied every entry it knew committed when asked (given
/// [`flower_cluster::node::READY_APPLY_WAIT`] to catch up, within the
/// client deadline), and storage answers. 503 otherwise, with the
/// storage failure when that is why.
async fn get_ready(State(state): State<AppState>, Query(params): Query<ReadyParams>) -> Response {
    let readiness = state.node.readiness().await;
    let mut ready = readiness.ready;
    let mut body = readiness_json(&readiness);
    // A process hosting several groups is ready when every one is: asked
    // without naming a group, the root's probe answers for the process.
    if let Some(host) = &state.host
        && state.node.group().is_root()
        && params.group.is_none()
    {
        let mut groups = serde_json::Map::new();
        for node in host.groups() {
            if node.group().is_root() {
                continue;
            }
            let group = node.readiness().await;
            ready &= group.ready;
            groups.insert(node.group().name.to_string(), readiness_json(&group));
        }
        if !groups.is_empty() {
            body["groups"] = serde_json::Value::Object(groups);
            body["ok"] = ready.into();
            if !ready && body["reason"].is_null() {
                body["reason"] = "a group this process hosts is not ready".into();
            }
        }
    }
    let status = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, Json(body)).into_response()
}

/// `GET /health/ready` parameters: the group asked about, which the
/// group dispatcher routes by.
#[derive(serde::Deserialize, Clone, Debug, Default)]
struct ReadyParams {
    group: Option<String>,
}

/// A readiness answer on the wire, as `/health/ready` sends it (one
/// group's).
pub(crate) fn readiness_json(readiness: &Readiness) -> serde_json::Value {
    serde_json::json!({
        "ok": readiness.ready,
        "reason": readiness.reason,
        "rejoining": readiness.rejoining,
        "storage": readiness.storage.as_ref().map(failure_json),
        "leader": readiness.leader,
        "committed": readiness.committed,
        "applied": readiness.applied,
    })
}

/// A storage failure on the wire.
fn failure_json(failure: &StorageFailure) -> serde_json::Value {
    serde_json::json!({
        "component": failure.component,
        "error": failure.error,
        "since_ms": failure.since_ms,
    })
}

/// One database's ordering counters.
#[derive(serde::Serialize, Clone, Debug)]
struct DbMetrics {
    /// Database name, lossy UTF-8.
    db: String,
    /// Committed revision.
    revision: u64,
    /// Applied log position.
    applied: u64,
}

/// Metrics on the wire.
#[derive(serde::Serialize, Clone, Debug)]
pub(crate) struct MetricsDto {
    /// Raft status.
    node: StatusDto,
    /// Per-database counters.
    dbs: Vec<DbMetrics>,
    /// Last admin-triggered backup summary, if any.
    last_backup: Option<BackupMetrics>,
    /// The storage failure that stopped this member, if any.
    storage_failure: Option<serde_json::Value>,
    /// Online compaction of each file.
    compaction: Vec<CompactionMetrics>,
    /// Admission gauges and counters: slots, queues, held bytes and
    /// refusals.
    admission: AdmissionMetrics,
    /// Fresh-read counters: lease reads, quorum rounds, read fences.
    reads: ReadMetrics,
    /// The writer: batches, the writes they carried, stale batches.
    writer: flower_cluster::writer::WriterStats,
    /// Retention rotation and collection this member led.
    retention: flower_cluster::node::RetentionStats,
    /// Collection of what deployments retired, which this member led.
    deployments: flower_cluster::node::CollectionStats,
    /// The background log purge and the backup's hold on it.
    log_purge: serde_json::Value,
    /// The soft snapshot triggers: bytes since the last snapshot, its
    /// age, builds and the cooldown after them.
    snapshots: serde_json::Value,
    /// The query result cache (hits, misses, entries, bytes against
    /// its budget) and the watch hub's evaluations.
    queries: serde_json::Value,
    /// Authorization decisions kept for reuse: hits, hook evaluations,
    /// entries and bytes against its budget.
    authorization: serde_json::Value,
    /// The Wasm runtime: pooled instances, warm slots, image cache.
    wasm: serde_json::Value,
    /// What each application method cost this process: v1's
    /// per-method costs (see [`flower_cluster::methods`]).
    methods: serde_json::Value,
}

/// The background log purge and the backup's hold on it.
fn log_purge_json(logs: &LogStore) -> serde_json::Value {
    let stats = logs.purge_stats();
    let hold = logs.purge_hold();
    serde_json::json!({
        "target": stats.target,
        "behind": stats.behind,
        "transactions": stats.transactions,
        "deleted": stats.deleted,
        "most_per_transaction": stats.most,
        "first_retained": logs.first_index_from(0).ok().flatten(),
        "hold": {
            "enabled": hold.enabled,
            "released": hold.released,
            "max_entries": hold.max_held,
            "max_bytes": hold.max_held_bytes,
            "held_bytes": logs.held_bytes(),
        },
    })
}

/// The soft snapshot triggers' view under `policy`.
fn snapshots_json(node: &Node, policy: &SnapshotPolicy) -> serde_json::Value {
    let stats = node.snapshot_policy_stats(policy);
    serde_json::json!({
        "unsnapshotted_bytes": stats.unsnapshotted_bytes,
        "applied": stats.applied,
        "snapshot": stats.snapshot,
        "snapshot_age_ms": stats.snapshot_age.as_millis() as u64,
        "last_build_ms": stats.last_build.as_millis() as u64,
        "cooldown_ms": stats.cooldown.as_millis() as u64,
        "active": stats.active,
        "builds": stats.builds,
        "stagger_slot": stats.stagger_slot,
        "due": stats.due,
        "after_bytes": policy.after_bytes,
        "max_age_ms": policy.max_age.as_millis() as u64,
    })
}

/// One file's online compaction on the wire.
#[derive(serde::Serialize, Clone, Debug)]
struct CompactionMetrics {
    /// `group` or `raft_log`.
    file: &'static str,
    /// A run is in progress.
    active: bool,
    /// Measurements taken.
    checks: u64,
    /// Steps that moved pages.
    steps: u64,
    /// Time spent in steps, in microseconds.
    step_micros: u64,
    /// Longest step, in microseconds.
    max_step_micros: u64,
    /// File length at the last check.
    file_bytes: Option<u64>,
    /// Bytes through the last allocated page at the last check.
    occupied_bytes: Option<u64>,
    /// Bytes of allocated pages at the last check.
    allocated_bytes: Option<u64>,
    /// How long a run that gave up still waits.
    waiting_ms: Option<u64>,
}

/// Backup summary on the wire.
#[derive(serde::Serialize, Clone, Debug)]
struct BackupMetrics {
    /// Opaque backup ID.
    backup_id: String,
    /// Database views uploaded.
    objects: usize,
    /// Group applied index covered.
    covered_index: u64,
}

/// Report metrics (admin): Raft status, per-database counters, last
/// backup, storage failure and online compaction.
async fn get_metrics(State(state): State<AppState>) -> Result<Json<MetricsDto>, ApiError> {
    metrics(&state).await.map(Json)
}

/// The member's metrics, as `/v1/metrics` and the admin stream send
/// them: in-memory counters and the read side only, so sampling them
/// never takes the engine lock or a fresh-read round.
pub(crate) async fn metrics(state: &AppState) -> Result<MetricsDto, ApiError> {
    let node = StatusDto::of(&state.node);
    let storage_failure = state.node.storage_failure().as_ref().map(failure_json);
    let compaction = state
        .node
        .compaction_stats()
        .into_iter()
        .map(|(file, stats)| CompactionMetrics {
            file,
            active: stats.active,
            checks: stats.checks,
            steps: stats.steps,
            step_micros: stats.step_micros,
            max_step_micros: stats.max_step_micros,
            file_bytes: stats.file_bytes,
            occupied_bytes: stats.occupied_bytes,
            allocated_bytes: stats.allocated_bytes,
            waiting_ms: stats.waiting_ms,
        })
        .collect();
    let dbs = state
        .node
        .database_positions()
        .await
        .map_err(ApiError::node)?
        .into_iter()
        .map(|(db, revision, applied)| DbMetrics {
            db: String::from_utf8_lossy(&db).into_owned(),
            revision,
            applied,
        })
        .collect();
    let last_backup = state
        .last_backup
        .lock()
        .await
        .clone()
        .map(|summary| BackupMetrics {
            backup_id: summary.backup_id,
            objects: summary.objects,
            covered_index: summary.covered_index,
        });
    Ok(MetricsDto {
        node,
        dbs,
        last_backup,
        storage_failure,
        compaction,
        admission: state.node.admission().metrics(),
        reads: state.node.read_metrics(),
        writer: state.node.writer_stats(),
        retention: state.node.retention_stats(),
        deployments: state.node.collection_stats(),
        log_purge: log_purge_json(state.node.logs()),
        snapshots: snapshots_json(&state.node, &state.snapshot_policy),
        queries: queries_json(state),
        authorization: authorization_json(state),
        wasm: wasm_json(),
        methods: flower_cluster::methods::report(flower_cluster::methods::SHOWN),
    })
}

/// The query result cache, identical queries in flight and the watch
/// hub's evaluations.
fn queries_json(state: &AppState) -> serde_json::Value {
    let cache = state.node.reader().query_cache_stats();
    let flights = state.node.flight_stats();
    serde_json::json!({
        "cache": {
            "hits": cache.hits,
            "misses": cache.misses,
            "rederived": cache.rederived,
            "entries": cache.entries,
            "bytes": cache.bytes,
            "budget_bytes": cache.budget,
        },
        "flights": {
            "led": flights.led,
            "joined": flights.joined,
            "alone": flights.alone,
            "flights": flights.flights,
            "solo": flights.solo,
            "bytes": flights.bytes,
            "budget_bytes": flights.budget,
        },
        "watch_evaluations": state.hub.evaluations(),
    })
}

/// The authorization decision memo, as v1's `authorizations` metrics:
/// calls a kept decision answered, hook runs, entries and bytes against
/// the budget `serve --decision-memo-bytes` sets.
fn authorization_json(state: &AppState) -> serde_json::Value {
    let memo = state.node.reader().decision_memo_stats();
    serde_json::json!({
        "decisions": {
            "hits": memo.hits,
            "evaluations": memo.evaluations,
            "entries": memo.entries,
            "bytes": memo.bytes,
            "budget_bytes": memo.budget,
        },
    })
}

/// The process's Wasm runtime: pooled instances, warm slots and the
/// image cache; null until a module first loaded.
fn wasm_json() -> serde_json::Value {
    let Some(stats) = flower_engine::runtime::wasm::stats() else {
        return serde_json::Value::Null;
    };
    serde_json::json!({
        "pooling": stats.pooling,
        "pagemap_scan": stats.pagemap_scan,
        "instances": stats.instances,
        "warm_slots": stats.warm_slots,
        "images": {
            "images": stats.images.images,
            "bytes": stats.images.bytes,
            "budget_bytes": stats.images.budget,
            "prepared": stats.images.prepared,
            "evicted": stats.images.evicted,
        },
    })
}

/// Continuous backup on this member (admin), as v1's `GET
/// /admin/backup`: whether it is configured, what it last did, the last
/// backup taken through the API, and how much log the hold keeps.
async fn get_backup(State(state): State<AppState>) -> Json<serde_json::Value> {
    let mut status = backup_status(&state).await;
    status["ok"] = true.into();
    Json(status)
}

/// Continuous backup on this member, as `GET /v1/admin/backup` and the
/// admin stream send it: all in memory.
pub(crate) async fn backup_status(state: &AppState) -> serde_json::Value {
    let last_backup = state.last_backup.lock().await.clone();
    serde_json::json!({
        "enabled": state.backup.is_some(),
        "continuous": state.backup.as_ref().map(|backup| backup.continuous_status()),
        "last_backup": last_backup.map(|summary| serde_json::json!({
            "backup_id": summary.backup_id,
            "objects": summary.objects,
            "covered_index": summary.covered_index,
            "ciphertext_bytes": summary.ciphertext_bytes,
        })),
        "log": log_purge_json(state.node.logs()),
    })
}

/// Membership change request (admin).
#[derive(serde::Deserialize, Clone, Debug)]
#[serde(deny_unknown_fields)]
struct MembershipRequest {
    /// The complete desired voter set.
    members: MembersDto,
    /// The membership log ID the change was planned against, as
    /// `GET /v1/admin/membership` reports it: refused if it moved.
    #[serde(default)]
    expected_log_id: Option<openraft::LogId<u64>>,
    /// Retain removed members as learners.
    #[serde(default)]
    retain: bool,
}

/// Members as `["id", "id=addr", ...]` or `{"id": "addr", ...}`.
#[derive(serde::Deserialize, Clone, Debug)]
#[serde(untagged)]
enum MembersDto {
    /// `id` (in-process) or `id=addr` (TCP) strings.
    List(Vec<String>),
    /// ID (a JSON object key, so a string) to Raft address.
    Map(BTreeMap<String, String>),
}

/// Leadership transfer request (admin).
#[derive(serde::Deserialize, Clone, Debug)]
#[serde(deny_unknown_fields)]
struct LeadershipRequest {
    /// The voter to hand leadership to.
    to: u64,
}

/// Parse `id`/`id=addr` member strings into an ID-to-node map.
pub fn parse_members(raw: &[String]) -> Result<BTreeMap<u64, BasicNode>, String> {
    let mut out = BTreeMap::new();
    for item in raw {
        let (id, addr) = match item.split_once('=') {
            Some((id, addr)) => (id, addr.to_string()),
            None => (item.as_str(), String::new()),
        };
        let id: u64 = id.parse().map_err(|_| format!("bad member id {item:?}"))?;
        out.insert(id, BasicNode { addr });
    }
    Ok(out)
}

/// This member's view of the membership (admin): local status, not a
/// linearizable read.
async fn get_membership(State(state): State<AppState>) -> Json<serde_json::Value> {
    Json(serde_json::json!({"ok": true, "membership": state.node.membership()}))
}

/// Change cluster membership at the leader, validated (admin): 409 with
/// `membership` when refused or uncertain.
async fn post_membership(
    State(state): State<AppState>,
    Json(body): Json<MembershipRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let members = match body.members {
        MembersDto::List(raw) => parse_members(&raw)
            .map_err(ApiError::BadRequest)?
            .into_iter()
            .map(|(id, node)| (id, node.addr))
            .collect(),
        MembersDto::Map(members) => members
            .into_iter()
            .map(|(id, addr)| match id.parse::<u64>() {
                Ok(id) => Ok((id, addr)),
                Err(_) => Err(ApiError::BadRequest(format!("bad member id {id:?}"))),
            })
            .collect::<Result<_, _>>()?,
    };
    let change = MembershipChange {
        members,
        expected_log_id: body.expected_log_id,
        retain: body.retain,
    };
    let membership = state
        .node
        .reconfigure(change)
        .await
        .map_err(ApiError::node)?;
    Ok(Json(
        serde_json::json!({"ok": true, "membership": membership}),
    ))
}

/// Hand leadership to another voter, at the leader (admin): 409 with
/// `transfer_failed` when it did not happen.
async fn post_leadership(
    State(state): State<AppState>,
    Json(body): Json<LeadershipRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let transfer = state
        .node
        .transfer_leadership(body.to)
        .await
        .map_err(ApiError::node)?;
    Ok(Json(serde_json::json!({"ok": true, "transfer": transfer})))
}

/// Run an operator request against a database's managed-key catalog
/// (admin): `{db, operation, requestId, ...}`, the request as
/// `Node::manage_keys` takes it for database `db`, which routes it to
/// the group that owns the database. Changes run on the leader
/// (followers forward); `cache` reports the serving member's own cache
/// and needs no database. Refused requests are 400s, a name no database
/// can have (empty, reserved with `$`, with control characters) among
/// them.
async fn post_keys(
    State(state): State<AppState>,
    Json(mut body): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let cache = body.get("operation").and_then(serde_json::Value::as_str) == Some("cache");
    let db = match body.as_object_mut().and_then(|fields| fields.remove("db")) {
        Some(serde_json::Value::String(db)) => db,
        Some(_) => return Err(ApiError::BadRequest("db must be a string".to_string())),
        None if cache => String::new(),
        None => {
            return Err(ApiError::BadRequest(
                "key requests name their database (`db`)".to_string(),
            ));
        }
    };
    if !cache {
        database_name(&db)?;
    }
    let value = state
        .node
        .manage_keys(db.as_bytes(), body)
        .await
        .map_err(key_error)?;
    Ok(Json(value))
}

/// Refuse a name no database's key catalog can have (see
/// [`flower_cluster::keys::invalid_database`]) as a 400.
fn database_name(db: &str) -> Result<(), ApiError> {
    match flower_cluster::keys::invalid_database(db.as_bytes()) {
        Some(why) => Err(ApiError::Refused {
            code: flower_cluster::node::KEY_REQUEST.to_string(),
            message: why,
        }),
        None => Ok(()),
    }
}

/// The database a key listing names, if any.
#[derive(serde::Deserialize, Clone, Debug)]
struct KeysQuery {
    /// Database name (UTF-8).
    db: Option<String>,
}

/// Managed-key catalogs' public listings, as the leader applied them
/// (admin): with `db`, that database's (`POST /v1/admin/keys`
/// `{"db", "operation": "list"}`); without, every database of the
/// group that has one, `{databases: {<db>: {revision, value}}}`.
async fn get_keys(
    State(state): State<AppState>,
    Query(query): Query<KeysQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let value = match query.db {
        Some(db) => {
            database_name(&db)?;
            state
                .node
                .manage_keys(db.as_bytes(), serde_json::json!({"operation": "list"}))
                .await
                .map_err(key_error)?
        }
        None => state.node.list_keys().await.map_err(key_error)?,
    };
    Ok(Json(value))
}

/// A refused key request is the caller's: 400 with its own code. A
/// member that cannot unwrap the catalog answers `KEYS_LOCKED`, 503.
fn key_error(error: NodeError) -> ApiError {
    match error {
        NodeError::Failure { code, message } if code == flower_cluster::keys::KEYS_LOCKED => {
            ApiError::Failure { code, message }
        }
        NodeError::Failure { code, message } if code.starts_with("KEY") => {
            ApiError::Refused { code, message }
        }
        other => ApiError::node(other),
    }
}

/// Trigger a full backup (admin).
#[tracing::instrument(skip(state))]
async fn post_backup(State(state): State<AppState>) -> Result<Json<serde_json::Value>, ApiError> {
    let Some(backup) = state.backup.clone() else {
        return Err(ApiError::Unavailable("backup not configured".to_string()));
    };
    // The backup runs on its own task: a client that stops waiting closes
    // its connection, and the server drops the request's handler, which
    // would cut a base off mid-upload.
    let last_backup = state.last_backup.clone();
    let taken = tokio::spawn(async move {
        let summary = backup.backup_now().await?;
        *last_backup.lock().await = Some(summary.clone());
        Ok(summary)
    })
    .await
    .unwrap_or_else(|error| Err(flower_backup::BackupError::Store(error.to_string())));
    let summary = taken.map_err(|error| ApiError::Failure {
        code: "backup".to_string(),
        message: error.to_string(),
    })?;
    Ok(Json(serde_json::json!({
        "ok": true,
        "backup_id": summary.backup_id,
        "objects": summary.objects,
        "covered_index": summary.covered_index,
    })))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identities_decode_hex_and_reject_non_ascii_without_panicking() {
        assert_eq!(decode_identity_hex(&"ab".repeat(32)).unwrap(), [0xab; 32]);
        assert_eq!(decode_identity_hex(&"AB".repeat(32)).unwrap(), [0xab; 32]);
        // 64 bytes with a two-byte character straddling a pair boundary:
        // byte-offset slicing panicked here.
        let straddling = format!("a{}{}", "é", "0".repeat(61));
        assert_eq!(straddling.len(), 64);
        assert!(decode_identity_hex(&straddling).is_err());
        assert!(decode_identity_hex("zz").is_err());
        assert!(decode_identity_hex(&"g0".repeat(32)).is_err());
    }

    #[tokio::test]
    async fn overloads_answer_503_with_a_retry_hint() {
        let response = ApiError::node(NodeError::Overloaded {
            message: "busy".to_string(),
            retry_after_ms: 1500,
        })
        .into_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        // Whole seconds, rounded up: never earlier than the hint.
        assert_eq!(response.headers()["retry-after"], "2");
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["code"], "overloaded");
        assert_eq!(body["retry_after_ms"], 1500);
    }

    #[test]
    fn only_a_calls_own_failure_answers_422() {
        let status = |code: &str| {
            ApiError::Failure {
                code: code.to_string(),
                message: "m".to_string(),
            }
            .into_response()
            .status()
        };
        // The call's own: a code its code chose, a limit it ran out of
        // (its fuel, its own declared or default deadline: v1's
        // `EVALUATION_BUDGET` answered 422), its code trapping.
        assert_eq!(status("OUT_OF_STOCK"), StatusCode::INTERNAL_SERVER_ERROR);
        let application = ApiError::node(NodeError::application(flower_sdk::fail(
            "out_of_stock",
            "empty",
        )))
        .into_response();
        assert_eq!(application.status(), StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(status("budget_exhausted"), StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(status(codes::SQL_BUDGET), StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(status(codes::SQL_INVALID), StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(
            status("deadline_exceeded"),
            StatusCode::UNPROCESSABLE_ENTITY
        );
        assert_eq!(status("host_trap"), StatusCode::UNPROCESSABLE_ENTITY);
        // The caller's deadline passing may clear on another attempt with
        // more time (v1 answered a client deadline's refusals 503), and so
        // may an instance that could not start or a module this member
        // holds no runtime for: the host's.
        assert_eq!(
            status("caller_deadline_exceeded"),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            status("instantiation_failed"),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            status("module_unavailable"),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[test]
    fn work_another_attempt_clears_answers_503() {
        // A queue a purge is deleting takes operations again once the
        // purge ends; a member without the wrapping key cannot change
        // keys until one that has it leads.
        assert_eq!(status_of("queue_purging"), 503);
        assert_eq!(status_of("KEYS_LOCKED"), 503);
    }

    #[test]
    fn evaluation_placement_refusals_keep_the_routing_marker() {
        let response = ApiError::node(NodeError::application(flower_sdk::fail(
            "partition_moved",
            "owner changed",
        )))
        .into_response();
        assert_eq!(
            response.status().as_u16(),
            crate::groups::placement_status("partition_moved")
                .unwrap()
                .as_u16()
        );
        assert_eq!(
            response.extensions().get::<FailureCode>().unwrap().0,
            "partition_moved"
        );
    }

    #[tokio::test]
    async fn watch_application_failures_preserve_their_origin_and_details() {
        let event = hub_event(
            HubMessage::Error {
                status: 422,
                code: "task_locked".into(),
                message: "task is locked".into(),
                details: Some(r#"{"owner":"ada"}"#.into()),
            },
            None,
        );
        let response = Sse::new(tokio_stream::once(event)).into_response();
        let bytes = axum::body::to_bytes(response.into_body(), 16 * 1024)
            .await
            .unwrap();
        let text = std::str::from_utf8(&bytes).unwrap();
        let data = text
            .lines()
            .find_map(|line| line.strip_prefix("data:"))
            .unwrap();
        let problem = flower_protocol::Problem::from_body(422, data.as_bytes(), None);
        assert_eq!(problem.origin, flower_protocol::Origin::Application);
        assert_eq!(problem.execution, flower_protocol::Execution::Decided);
        assert_eq!(problem.details.unwrap()["owner"], "ada");
        assert!(!problem.retryable);
    }

    #[test]
    fn storage_failures_answer_503_so_clients_try_another_member() {
        let status = |code: &str| {
            ApiError::Failure {
                code: code.to_string(),
                message: "m".to_string(),
            }
            .into_response()
            .status()
        };
        assert_eq!(status(STORAGE_FAILED), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(status(IO_FAILURE), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(status("quorum"), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(status(RECOVERING), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(status("host_storage"), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn membership_and_leadership_outcomes_map_to_retry_or_conflict() {
        let status = |code: &str| {
            ApiError::Failure {
                code: code.to_string(),
                message: "m".to_string(),
            }
            .into_response()
            .status()
        };
        // Writes refused while leadership moves, and a member catching
        // up, are retried.
        assert_eq!(status(TRANSFERRING), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(status(REJOINING), StatusCode::SERVICE_UNAVAILABLE);
        // So are writes a leader cut off from its quorum refused.
        assert_eq!(status(ISOLATED), StatusCode::SERVICE_UNAVAILABLE);
        // A refused operator change is a conflict to inspect.
        assert_eq!(status(MEMBERSHIP), StatusCode::CONFLICT);
        assert_eq!(status(TRANSFER_FAILED), StatusCode::CONFLICT);
        // So is a staged-deployment step refused, or a module already
        // deploying.
        assert_eq!(status(DEPLOYMENT), StatusCode::CONFLICT);
        assert_eq!(status(DEPLOYMENT_IN_PROGRESS), StatusCode::CONFLICT);
    }

    #[test]
    fn membership_requests_take_lists_or_maps_and_an_expected_log_id() {
        let list: MembershipRequest =
            serde_json::from_value(serde_json::json!({"members": ["1=a:1", "2=b:2"]})).unwrap();
        assert!(matches!(list.members, MembersDto::List(ref raw) if raw.len() == 2));
        let map: MembershipRequest = serde_json::from_value(serde_json::json!({
            "members": {"1": "a:1", "2": "b:2"},
            "expected_log_id": {"leader_id": {"term": 3, "node_id": 1}, "index": 9},
        }))
        .unwrap();
        assert!(matches!(map.members, MembersDto::Map(ref members) if members["2"] == "b:2"));
        assert_eq!(map.expected_log_id.unwrap().index, 9);
        assert!(
            serde_json::from_value::<MembershipRequest>(
                serde_json::json!({"members": [], "expected": 1})
            )
            .is_err()
        );
    }
}
