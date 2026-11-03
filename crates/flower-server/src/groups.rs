//! Serving every group a process hosts on one listener.
//!
//! Each open group gets its own API state (its node, watch hub,
//! activity, backup driver) and router, exactly as a lone member's, so
//! every route behaves per group as it did before groups existed. A
//! dispatcher in front picks the group a request belongs to:
//!
//! 1. the group its [`GROUP_HEADER`] names, or its `group` query
//!    parameter (forwarded requests always name one; operators may);
//! 2. otherwise the owner of the database it names (`db` in its query
//!    string or its JSON body, or every step of a transaction), by the
//!    host's catalog (see [`flower_cluster::partitions`]): the owner of a
//!    named partition, the root for an unplaced database;
//! 3. otherwise the root group (health, metrics, the dashboard, the
//!    catalog's own operator routes).
//!
//! A group this process hosts serves the request through its router,
//! which forwards to the group's leader as before. A group it does not
//! host gets the request forwarded once over the member channel to the
//! group's leader ([`Host::send`]), marked with its group so it is never
//! routed or forwarded again. When the owner refuses because the
//! partition moved (this process's catalog was behind), the dispatcher
//! reads the catalog fresh through the root's leader and tries the new
//! owner once.
//!
//! Every answer names the owning group ([`GROUP_HEADER`]) and, for a
//! partition, its epoch ([`EPOCH_HEADER`]); `x-flower-leader-url` names
//! the leader of the owning group, so clients routing by leader send
//! that database's writes there.

use std::collections::BTreeMap;
use std::sync::{Arc, PoisonError, RwLock};
use std::time::Duration;

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::{Request, State};
use axum::http::{HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use flower_cluster::admission::{Class, REQUEST_OVERHEAD};
use flower_cluster::group::{GroupId, ROOT};
use flower_cluster::host::{GroupEvent, Host};
use flower_cluster::node::{Node, NodeError};
use flower_cluster::partitions::Route;
use flower_cluster::peer::{ForwardedRequest, Forwarder, MemberRequest, MemberResponse};
use flower_engine::placement::{PARTITION_MOVED, PARTITION_MOVING, PARTITION_RETIRED};
use tower::ServiceExt;

use crate::api::{ApiError, AppState, router};
use crate::forward::{CALL_LIMIT, FORWARDED_BY, RouterForwarder};

/// Request and response header naming the group a request belongs to.
pub const GROUP_HEADER: &str = "x-flower-group";
/// Response header: the ownership epoch of the partition a request
/// named.
pub const EPOCH_HEADER: &str = "x-flower-partition-epoch";

/// Builds each group's API state from its node: the credentials,
/// limits and options every group shares.
pub type StateFactory = Arc<dyn Fn(Arc<Node>) -> AppState + Send + Sync>;

/// One served group.
struct Served {
    state: AppState,
    router: Router,
    stop: tokio::sync::watch::Sender<bool>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
    /// Keeps the group's forwarder alive while it serves.
    _forwarder: Arc<dyn Forwarder>,
}

/// The API of a process hosting many groups.
pub struct HostApi {
    host: Arc<Host>,
    factory: StateFactory,
    served: RwLock<BTreeMap<Arc<str>, Arc<Served>>>,
}

impl HostApi {
    /// Serve `host`'s groups, each with the state `factory` builds.
    pub fn new(host: Arc<Host>, factory: StateFactory) -> Arc<Self> {
        Arc::new(HostApi {
            host,
            factory,
            served: RwLock::new(BTreeMap::new()),
        })
    }

    /// The host served.
    pub fn host(&self) -> &Arc<Host> {
        &self.host
    }

    /// A served group's API state, by name.
    pub fn state(&self, group: &str) -> Option<AppState> {
        self.served(group).map(|served| served.state.clone())
    }

    fn served(&self, group: &str) -> Option<Arc<Served>> {
        self.served
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(group)
            .cloned()
    }

    /// Start serving a group the host opened: its hub pumps commit
    /// notices, its activity records, and its member channel runs
    /// forwarded requests through its router.
    fn attach(&self, node: Arc<Node>) {
        let name = node.group().name.clone();
        if self
            .served(&name)
            .is_some_and(|served| served.state.node.group() == node.group())
        {
            return;
        }
        let state = (self.factory)(node.clone());
        let (stop, stopped) = tokio::sync::watch::channel(false);
        let pump = tokio::spawn(state.hub.clone().run(stopped.clone()));
        let activity = tokio::spawn(
            state
                .activity
                .clone()
                .record(node.subscribe_commits(), stopped.clone()),
        );
        // The root's admin stream carries every hosted group; another
        // group's carries its own.
        let host = node.group().is_root().then(|| self.host.clone());
        let admin_events =
            tokio::spawn(state.admin_events.clone().run(node.clone(), host, stopped));
        let router = router(state.clone());
        let forwarder: Arc<dyn Forwarder> = RouterForwarder::new(router.clone());
        node.member_desk().set_forwarder(Arc::downgrade(&forwarder));
        self.served
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(
                name,
                Arc::new(Served {
                    state,
                    router,
                    stop,
                    tasks: vec![pump, activity, admin_events],
                    _forwarder: forwarder,
                }),
            );
    }

    /// Stop serving a group the host closed.
    async fn detach(&self, group: &GroupId) {
        let served = {
            let mut served = self.served.write().unwrap_or_else(PoisonError::into_inner);
            match served.get(&group.name) {
                Some(entry) if entry.state.node.group() == group => served.remove(&group.name),
                _ => None,
            }
        };
        if let Some(served) = served {
            let _ = served.stop.send(true);
            served.state.admin_events.close();
            if let Ok(served) = Arc::try_unwrap(served) {
                served.state.activity.close();
                for task in served.tasks {
                    let _ = task.await;
                }
            }
        }
    }

    /// Serve every group open now and every one that opens later, until
    /// `stop` turns true; then stop serving them all.
    pub async fn follow(self: Arc<Self>, mut stop: tokio::sync::watch::Receiver<bool>) {
        let mut events = self.host.subscribe();
        for node in self.host.groups() {
            self.attach(node);
        }
        loop {
            tokio::select! {
                event = events.recv() => match event {
                    Ok(GroupEvent::Opened(group)) => {
                        if let Some(node) = self.host.group(&group.name) {
                            self.attach(node);
                        }
                    }
                    Ok(GroupEvent::Closed(group)) => self.detach(&group).await,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        for node in self.host.groups() {
                            self.attach(node);
                        }
                    }
                    Err(_) => break,
                },
                changed = stop.changed() => {
                    if changed.is_err() || *stop.borrow() {
                        break;
                    }
                }
            }
        }
        let groups: Vec<GroupId> = self
            .served
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .map(|served| served.state.node.group().clone())
            .collect();
        for group in groups {
            self.detach(&group).await;
        }
    }

    /// The dispatcher: every request through one handler.
    pub fn router(self: &Arc<Self>) -> Router {
        Router::new().fallback(dispatch).with_state(self.clone())
    }

    /// Close every served group's watch streams, for a drain.
    pub fn close_streams(&self) {
        for served in self
            .served
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
        {
            let _ = served.stop.send(true);
            served.state.activity.close();
            served.state.admin_events.close();
        }
    }
}

/// Where a request goes.
enum Target {
    /// A group by name, as the request said.
    Named(String),
    /// The owner of a database.
    Database(String),
    /// Several databases: a transaction's steps.
    Databases(Vec<String>),
    /// No group or database: the root.
    Root,
}

/// The `group` or `db` a query string names.
fn query_param(query: Option<&str>, key: &str) -> Option<String> {
    url_pairs(query?)
        .into_iter()
        .find(|(name, _)| name == key)
        .map(|(_, value)| value)
}

/// Decode `a=b&c=d` pairs, percent-decoding values.
fn url_pairs(query: &str) -> Vec<(String, String)> {
    query
        .split('&')
        .filter_map(|pair| {
            let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
            Some((decode(name)?, decode(value)?))
        })
        .collect()
}

fn decode(text: &str) -> Option<String> {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut at = 0;
    while at < bytes.len() {
        match bytes[at] {
            b'%' if at + 2 < bytes.len() => {
                let hex = std::str::from_utf8(&bytes[at + 1..at + 3]).ok()?;
                out.push(u8::from_str_radix(hex, 16).ok()?);
                at += 3;
            }
            b'+' => {
                out.push(b' ');
                at += 1;
            }
            byte => {
                out.push(byte);
                at += 1;
            }
        }
    }
    String::from_utf8(out).ok()
}

/// The databases a JSON body names: its `db`, or its steps' `db`s.
fn body_target(body: &[u8]) -> Option<Target> {
    #[derive(serde::Deserialize)]
    struct Step {
        db: Option<String>,
    }
    #[derive(serde::Deserialize)]
    struct Named {
        db: Option<String>,
        steps: Option<Vec<Step>>,
    }
    let named: Named = serde_json::from_slice(body).ok()?;
    if let Some(db) = named.db {
        return Some(Target::Database(db));
    }
    let dbs: Vec<String> = named
        .steps?
        .into_iter()
        .filter_map(|step| step.db)
        .collect();
    (!dbs.is_empty()).then_some(Target::Databases(dbs))
}

/// Whether a route's body may name a database. Bodies of others are
/// not read here.
fn reads_body(method: &axum::http::Method, path: &str) -> bool {
    *method == axum::http::Method::POST
        && (path.starts_with("/v1/") && !path.starts_with("/v1/admin/groups"))
        && !path.starts_with("/v1/admin/partitions")
        && !path.starts_with("/v1/admin/membership")
        && !path.starts_with("/v1/admin/leadership")
}

/// The body limit and budget class of a route, as the forwarding layer
/// sizes them.
fn body_limits(state: &AppState, path: &str) -> (usize, Class) {
    let limit = if matches!(path, "/v1/deploy" | "/v1/admin/deployments") {
        state.deploy_limit
    } else {
        CALL_LIMIT
    };
    let class =
        if path == "/v1/deploy" || path.starts_with("/v1/admin/") || path.starts_with("/v1/timer/")
        {
            Class::Control
        } else {
            Class::Query
        };
    (limit, class)
}

/// Read a body under a lease of the process's budget for its class.
async fn read_body(
    state: &AppState,
    headers: &axum::http::HeaderMap,
    body: Body,
    path: &str,
) -> Result<(Bytes, flower_cluster::admission::Lease), Box<Response>> {
    use http_body_util::BodyExt;
    let (limit, class) = body_limits(state, path);
    let too_large =
        || Box::new((StatusCode::PAYLOAD_TOO_LARGE, "request body too large").into_response());
    let declared = headers
        .get(axum::http::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok()?.parse::<usize>().ok());
    if declared.is_some_and(|declared| declared > limit) {
        return Err(too_large());
    }
    let overloaded = |overload| Box::new(ApiError::node(NodeError::from(overload)).into_response());
    let mut lease = state
        .node
        .admission()
        .lease(class, REQUEST_OVERHEAD + declared.unwrap_or(0))
        .map_err(overloaded)?;
    let mut leased = declared.unwrap_or(0);
    let mut buffered = Vec::with_capacity(declared.unwrap_or(0));
    let mut body = body;
    while let Some(frame) = body.frame().await {
        let Ok(frame) = frame else {
            return Err(Box::new(
                (StatusCode::BAD_REQUEST, "request body failed").into_response(),
            ));
        };
        let Ok(data) = frame.into_data() else {
            continue;
        };
        if buffered.len() + data.len() > limit {
            return Err(too_large());
        }
        let needed = buffered.len() + data.len();
        if needed > leased {
            lease.grow(needed - leased).map_err(overloaded)?;
            leased = needed;
        }
        buffered.extend_from_slice(&data);
    }
    Ok((Bytes::from(buffered), lease))
}

/// Marks a request whose body the dispatcher already read under a lease:
/// the forwarding layer reads it again without leasing twice.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Prebuffered;

/// The request headers a cross-group forward carries.
const FORWARD_HEADERS: [&str; 6] = [
    "authorization",
    "content-type",
    "accept",
    crate::forward::MAX_STALENESS,
    flower_cluster::deadline::TIMEOUT_HEADER,
    "traceparent",
];

/// Route one request to its group.
async fn dispatch(State(api): State<Arc<HostApi>>, request: Request) -> Response {
    let Some(root) = api.served(ROOT) else {
        return ApiError::Unavailable("the root group is not served yet".to_string())
            .into_response();
    };
    let (parts, body) = request.into_parts();
    let path = parts.uri.path().to_string();
    let query = parts.uri.query();
    let named = parts
        .headers
        .get(GROUP_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string)
        .or_else(|| query_param(query, "group"));
    let mut target = match (named, query_param(query, "db")) {
        (Some(group), _) => Target::Named(group),
        (None, Some(db)) => Target::Database(db),
        (None, None) => Target::Root,
    };
    // A body is read here, under the process's lease, whenever the
    // request may need routing by it or a second attempt; one that names
    // its group passes through untouched.
    let mut bytes = None;
    let mut lease = None;
    let mut untouched = Some(body);
    if !matches!(target, Target::Named(_)) && parts.method == axum::http::Method::POST {
        let body = untouched.take().expect("the body is still here");
        match read_body(&root.state, &parts.headers, body, &path).await {
            Ok((read, held)) => {
                if matches!(target, Target::Root)
                    && reads_body(&parts.method, &path)
                    && let Some(found) = body_target(&read)
                {
                    target = found;
                }
                bytes = Some(read);
                lease = Some(held);
            }
            Err(refused) => return *refused,
        }
    }
    let body = bytes.clone();
    let untouched = std::sync::Mutex::new(untouched);
    let rebuild = |body: Option<Bytes>| {
        let body = body.map(Body::from).or_else(|| {
            untouched
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take()
        });
        let mut builder = axum::http::Request::builder()
            .method(parts.method.clone())
            .uri(parts.uri.clone())
            .version(parts.version);
        for (name, value) in &parts.headers {
            builder = builder.header(name, value);
        }
        let mut request = builder
            .body(body.unwrap_or_else(Body::empty))
            .expect("a request rebuilt from its parts");
        if lease.is_some() {
            request.extensions_mut().insert(Prebuffered);
        }
        request
    };
    let route = match resolve(&api, &target) {
        Ok(route) => route,
        Err(refused) => return *refused,
    };
    let forwarded = parts.headers.contains_key(FORWARDED_BY);
    let first = serve(&api, &route, rebuild(body.clone()), forwarded).await;
    let response = match (&target, moved(&first)) {
        // The owner says the partition moved: this process's catalog was
        // behind. Read it fresh and try the new owner, once.
        (Target::Database(db), true) if !forwarded => match api.host.fresh_route(db).await {
            Ok(Some(fresh)) if fresh.group != route.group => {
                let retried = serve(&api, &fresh, rebuild(body), forwarded).await;
                named_response(retried, &fresh)
            }
            _ => named_response(first, &route),
        },
        _ => named_response(first, &route),
    };
    drop(lease);
    response
}

/// Resolve a target to the group serving it.
fn resolve(api: &HostApi, target: &Target) -> Result<Route, Box<Response>> {
    let unknown = |what: String| {
        Box::new(
            (
                StatusCode::NOT_FOUND,
                axum::Json(
                    serde_json::json!({"ok": false, "code": "unknown_group", "message": what}),
                ),
            )
                .into_response(),
        )
    };
    match target {
        Target::Root => Ok(root_route()),
        Target::Named(group) => {
            let catalog = api.host.cached_catalog();
            match api.host.group_id(&catalog, group) {
                Some(id) => Ok(Route {
                    group: id,
                    epoch: None,
                    partition: None,
                }),
                // A group this process hosts but its catalog does not
                // list yet (it just opened): serve it here.
                None => match api.host.group(group) {
                    Some(node) => Ok(Route {
                        group: node.group().clone(),
                        epoch: None,
                        partition: None,
                    }),
                    None => Err(unknown(format!("no group `{group}`"))),
                },
            }
        }
        Target::Database(db) => api
            .host
            .route(db)
            .ok_or_else(|| unknown(format!("partition `{db}` names no listed group"))),
        Target::Databases(dbs) => {
            let mut routes = dbs.iter().filter_map(|db| api.host.route(db));
            let first = routes.next().unwrap_or_else(root_route);
            // Steps in several groups commit by two-phase commit from
            // the first step's group (see `crate::api`'s transaction
            // route); every step's group serves its own steps.
            Ok(first)
        }
    }
}

fn root_route() -> Route {
    Route {
        group: GroupId::root(),
        epoch: None,
        partition: None,
    }
}

/// Serve a request in `route`'s group: through its router when hosted
/// here, else forwarded once to its leader.
async fn serve(api: &HostApi, route: &Route, request: Request, forwarded: bool) -> Response {
    if let Some(served) = api.served(&route.group.name)
        && served.state.node.group() == &route.group
    {
        return match served.router.clone().oneshot(request).await {
            Ok(response) => response,
            Err(never) => match never {},
        };
    }
    if forwarded {
        // Never forwarded twice: the sender routed by a stale view.
        return ApiError::Failure {
            code: PARTITION_MOVED.to_string(),
            message: format!("this member does not host group {}", route.group),
        }
        .into_response();
    }
    forward(api, route, request).await
}

/// Forward a request to a group this process does not host.
async fn forward(api: &HostApi, route: &Route, request: Request) -> Response {
    let (parts, body) = request.into_parts();
    let bytes = match axum::body::to_bytes(body, usize::MAX).await {
        Ok(bytes) => bytes,
        Err(_) => return (StatusCode::BAD_REQUEST, "request body failed").into_response(),
    };
    let Ok(body) = String::from_utf8(bytes.to_vec()) else {
        return ApiError::BadRequest("request bodies are UTF-8 JSON".to_string()).into_response();
    };
    let mut headers: Vec<(String, String)> = FORWARD_HEADERS
        .iter()
        .filter_map(|name| {
            let value = parts.headers.get(*name)?.to_str().ok()?;
            Some((name.to_string(), value.to_string()))
        })
        .collect();
    headers.push((GROUP_HEADER.to_string(), route.group.name.to_string()));
    headers.push((FORWARDED_BY.to_string(), api.host.id().to_string()));
    if let Some(left) = flower_cluster::deadline::remaining() {
        headers.retain(|(name, _)| name != flower_cluster::deadline::TIMEOUT_HEADER);
        headers.push((
            flower_cluster::deadline::TIMEOUT_HEADER.to_string(),
            left.as_millis().max(1).to_string(),
        ));
    }
    let request = ForwardedRequest {
        method: parts.method.to_string(),
        path: parts
            .uri
            .path_and_query()
            .map_or_else(|| parts.uri.path().to_string(), ToString::to_string),
        headers,
        body,
    };
    match api
        .host
        .send(&route.group, MemberRequest::Forward(request), true)
        .await
    {
        Ok(MemberResponse::Forward(response)) => {
            // The owner's refusal code, for the dispatcher's retry.
            let code = (response.status == StatusCode::SERVICE_UNAVAILABLE.as_u16())
                .then(|| serde_json::from_str::<serde_json::Value>(&response.body).ok())
                .flatten()
                .and_then(|body| body.get("code")?.as_str().map(str::to_string));
            let mut relayed = crate::forward::relay(response);
            if let Some(code) = code {
                relayed
                    .extensions_mut()
                    .insert(crate::api::FailureCode(code));
            }
            relayed
        }
        Ok(_) => ApiError::Fatal("a group's leader answered a forward with another kind".into())
            .into_response(),
        Err(error) => ApiError::node(error).into_response(),
    }
}

/// Whether a response says its partition moved away from the group
/// that answered.
fn moved(response: &Response) -> bool {
    response.status() == StatusCode::SERVICE_UNAVAILABLE
        && response
            .extensions()
            .get::<crate::api::FailureCode>()
            .is_some_and(|code| code.0 == PARTITION_MOVED)
}

/// Name the group and epoch on an answer.
fn named_response(mut response: Response, route: &Route) -> Response {
    if let Ok(value) = HeaderValue::from_str(&route.group.name) {
        response
            .headers_mut()
            .insert(HeaderName::from_static(GROUP_HEADER), value);
    }
    if let Some(epoch) = route.epoch {
        response.headers_mut().insert(
            HeaderName::from_static(EPOCH_HEADER),
            HeaderValue::from(epoch),
        );
    }
    response
}

/// Whether a failure code means a partition does not serve where the
/// request went, and how to answer it.
pub(crate) fn placement_status(code: &str) -> Option<StatusCode> {
    match code {
        PARTITION_MOVING | PARTITION_MOVED => Some(StatusCode::SERVICE_UNAVAILABLE),
        PARTITION_RETIRED => Some(StatusCode::GONE),
        _ => None,
    }
}

/// How long a client should wait before retrying a partition that is
/// moving: a freeze lasts about a round trip of each group.
pub(crate) const MOVING_RETRY: Duration = Duration::from_millis(200);

/// Serve every group of `api`'s host on a bound listener until
/// `shutdown` resolves, then drain as [`crate::api::serve_with`] does:
/// stop accepting, end every group's watch streams, give in-flight
/// requests up to `drain`, and stop serving each group, which releases
/// its node handles so the host can shut down.
pub async fn serve_host<F>(
    listener: tokio::net::TcpListener,
    api: Arc<HostApi>,
    shutdown: F,
    drain: Duration,
    options: &crate::listener::HttpOptions,
) -> std::io::Result<()>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let follow = tokio::spawn(api.clone().follow(stop_rx));
    // The root is served before the first request is taken.
    for _ in 0..200 {
        if api.served(ROOT).is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let stopping = {
        let api = api.clone();
        async move {
            shutdown.await;
            api.close_streams();
        }
    };
    let result = crate::listener::run(listener, api.router(), options, stopping, drain).await;
    let _ = stop_tx.send(true);
    let _ = follow.await;
    result
}
