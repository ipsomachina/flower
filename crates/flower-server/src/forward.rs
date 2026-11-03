//! Leader forwarding and routing headers.
//!
//! A client may call any member. A request only the leader can serve
//! (writes, queue and timer operations, deployments, membership) that
//! reaches a follower answers there with a forward; this layer then
//! sends the request as it arrived to the leader over the member
//! channel ([`flower_cluster::peer`]) and relays the leader's answer.
//! The leader runs it through its own API, as if the client had called
//! it: same credential checks, admission and idempotency. A forwarded
//! request carries [`FORWARDED_BY`] and is never forwarded again, so a
//! stale route fails instead of looping; the sending member then tries
//! the next leader within its budget (see [`Node::forward`]). A forward
//! reaches the client only when no leader could be reached in time.
//!
//! Every response through this layer names the leader's advertised API
//! URL in [`LEADER_URL`] when this member knows it, so clients routing
//! by leader send their writes there directly.
//!
//! The layer runs after the credential checks of its routes, so an
//! unauthenticated request is refused before its body is buffered.

use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderName, HeaderValue, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use flower_cluster::admission::{Class, Lease, Overload, REQUEST_OVERHEAD};
use flower_cluster::deadline::{self, TIMEOUT_HEADER};
use flower_cluster::node::{Node, NodeError};
use flower_cluster::peer::{
    BoxFuture, ForwardedRequest, ForwardedResponse, Forwarder, MemberError,
};
use tower::ServiceExt;

use crate::api::{ApiError, AppState};

/// Request header on forwarded requests: the sending member's ID.
pub const FORWARDED_BY: &str = "x-flower-forwarded-by";
/// Response header: the leader's advertised API URL, when known.
pub const LEADER_URL: &str = "x-flower-leader-url";
/// Request header: the most staleness a replica-local query may be
/// answered with, in whole milliseconds.
pub const MAX_STALENESS: &str = "x-flower-max-staleness-ms";
/// Response header on replica-local query answers: how stale the
/// serving replica's state may have been, in milliseconds, or
/// `unknown`.
pub const STALENESS: &str = "x-flower-staleness-ms";
/// Largest body a call route accepts: axum's default body limit.
pub(crate) const CALL_LIMIT: usize = 2 * 1024 * 1024;

/// The request headers a forwarded request carries.
const REQUEST_HEADERS: [&str; 4] = ["authorization", "content-type", "accept", MAX_STALENESS];
/// The response headers relayed from the leader.
const RESPONSE_HEADERS: [&str; 7] = [
    "content-type",
    "retry-after",
    STALENESS,
    crate::telemetry::SERVER_TIMING,
    LEADER_URL,
    crate::groups::GROUP_HEADER,
    crate::groups::EPOCH_HEADER,
];

/// Marks a response that says this member does not lead, so this layer
/// forwards the request (see [`ApiError`]).
#[derive(Clone, Copy, Debug)]
pub(crate) struct NotLeader {
    /// The leader this member knows, if any.
    pub(crate) leader: Option<u64>,
}

/// Run each request under the client deadline its
/// [`TIMEOUT_HEADER`] states (see [`flower_cluster::deadline`]): 400
/// for a malformed one.
pub(crate) async fn deadline(request: Request, next: Next) -> Response {
    let Some(value) = request.headers().get(TIMEOUT_HEADER) else {
        return next.run(request).await;
    };
    let Some(budget) = value.to_str().ok().and_then(deadline::parse) else {
        return ApiError::BadRequest(format!(
            "{TIMEOUT_HEADER} must be a whole number of milliseconds"
        ))
        .into_response();
    };
    let at = tokio::time::Instant::now() + budget;
    deadline::scoped(Some(at), next.run(request)).await
}

/// Forward requests this member answered with a forward, and name the
/// leader's API URL on every response.
pub(crate) async fn layer(State(state): State<AppState>, request: Request, next: Next) -> Response {
    if request.headers().contains_key(FORWARDED_BY) {
        // Never forwarded again; the answer still names this group's
        // leader for the member that relays it (another group's member
        // has no other way to learn it).
        let response = next.run(request).await;
        return name_leader(&state.node, response);
    }
    let path = request.uri().path();
    let limit = if matches!(path, "/v1/deploy" | "/v1/admin/deployments") {
        state.deploy_limit
    } else {
        CALL_LIMIT
    };
    // Operator bodies draw on the control budget, so user traffic
    // filling its own cannot keep an operator out.
    let class =
        if path == "/v1/deploy" || path.starts_with("/v1/admin/") || path.starts_with("/v1/timer/")
        {
            Class::Control
        } else {
            Class::Query
        };
    let prebuffered = request
        .extensions()
        .get::<crate::groups::Prebuffered>()
        .is_some();
    let (parts, body) = request.into_parts();
    // The body's bytes are leased before it is read and held until the
    // answer is ready: an oversubscribed member refuses a body instead
    // of buffering it. One the group dispatcher read already holds its
    // lease there.
    let (bytes, _lease) = if prebuffered {
        match axum::body::to_bytes(body, limit).await {
            Ok(bytes) => (bytes, None),
            Err(_) => {
                return (StatusCode::PAYLOAD_TOO_LARGE, "request body too large").into_response();
            }
        }
    } else {
        let received = buffer(&state, class, &parts.headers, body, limit);
        match crate::telemetry::http_stage("body_receive", received).await {
            Ok((bytes, lease)) => (bytes, Some(lease)),
            Err(refused) => return *refused,
        }
    };
    // Kept for a forward: the body is shared, not copied.
    let method = parts.method.to_string();
    let path = parts
        .uri
        .path_and_query()
        .map_or_else(|| parts.uri.path().to_string(), ToString::to_string);
    let headers: Vec<(String, String)> = REQUEST_HEADERS
        .iter()
        .filter_map(|name| {
            let value = parts.headers.get(*name)?.to_str().ok()?;
            Some((name.to_string(), value.to_string()))
        })
        .chain([(FORWARDED_BY.to_string(), state.node.id().to_string())])
        .collect();
    let response = next
        .run(Request::from_parts(parts, Body::from(bytes.clone())))
        .await;
    let forwardable = response.extensions().get::<NotLeader>().is_some();
    let response = match std::str::from_utf8(&bytes) {
        Ok(body) if forwardable => {
            let mut headers = headers;
            // What is left of the client's deadline goes with the
            // request, so the leader holds it too.
            if let Some(left) = deadline::remaining() {
                let ms = left.as_millis().max(1);
                headers.push((TIMEOUT_HEADER.to_string(), ms.to_string()));
            }
            crate::telemetry::inject(&mut headers);
            let request = ForwardedRequest {
                method,
                path,
                headers,
                body: body.to_string(),
            };
            let started = std::time::Instant::now();
            match state.node.forward(request).await {
                Ok(forwarded) => {
                    let mut response = relay(forwarded);
                    response.extensions_mut().insert(Forwarded {
                        elapsed: started.elapsed(),
                    });
                    response
                }
                Err(NodeError::Forward { .. }) => response,
                Err(error) => ApiError::node(error).into_response(),
            }
        }
        _ => response,
    };
    name_leader(&state.node, response)
}

/// Marks a response relayed from the leader, with the round trip's
/// time, for `Server-Timing` (see [`crate::telemetry`]).
#[derive(Clone, Copy, Debug)]
pub(crate) struct Forwarded {
    /// The forward's round trip.
    pub(crate) elapsed: std::time::Duration,
}

/// Read a request body of at most `limit` bytes under a lease of the
/// `class` budget: a declared length is leased whole before reading, an
/// undeclared body as it arrives. Too large answers 413; no room in the
/// budget answers a retryable 503 `overloaded`.
async fn buffer(
    state: &AppState,
    class: Class,
    headers: &axum::http::HeaderMap,
    body: Body,
    limit: usize,
) -> Result<(axum::body::Bytes, Lease), Box<Response>> {
    use http_body_util::BodyExt;
    let too_large =
        || Box::new((StatusCode::PAYLOAD_TOO_LARGE, "request body too large").into_response());
    let declared = headers
        .get(axum::http::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok()?.parse::<usize>().ok());
    if declared.is_some_and(|declared| declared > limit) {
        return Err(too_large());
    }
    let overloaded =
        |overload: Overload| Box::new(ApiError::node(NodeError::from(overload)).into_response());
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
    Ok((axum::body::Bytes::from(buffered), lease))
}

/// Add the leader's API URL, when known.
fn name_leader(node: &Node, mut response: Response) -> Response {
    if let Some(url) = node
        .leader_api_url()
        .and_then(|url| HeaderValue::from_str(&url).ok())
    {
        response
            .headers_mut()
            .insert(HeaderName::from_static(LEADER_URL), url);
    }
    response
}

/// The leader's answer, as the client gets it.
pub(crate) fn relay(forwarded: ForwardedResponse) -> Response {
    let status = StatusCode::from_u16(forwarded.status).unwrap_or(StatusCode::BAD_GATEWAY);
    let mut response = (status, forwarded.body).into_response();
    for (name, value) in forwarded.headers {
        if let (Ok(name), Ok(value)) = (
            HeaderName::try_from(name.as_str()),
            HeaderValue::from_str(&value),
        ) {
            response.headers_mut().insert(name, value);
        }
    }
    response
}

/// Runs forwarded requests through this member's API.
pub(crate) struct RouterForwarder {
    router: axum::Router,
}

impl RouterForwarder {
    /// Serve forwarded requests through `router`.
    pub(crate) fn new(router: axum::Router) -> Arc<Self> {
        Arc::new(RouterForwarder { router })
    }
}

impl Forwarder for RouterForwarder {
    fn forward(
        &self,
        request: ForwardedRequest,
    ) -> BoxFuture<Result<ForwardedResponse, MemberError>> {
        let router = self.router.clone();
        Box::pin(async move {
            let mut builder = axum::http::Request::builder()
                .method(request.method.as_str())
                .uri(request.path.as_str());
            for (name, value) in &request.headers {
                builder = builder.header(name.as_str(), value.as_str());
            }
            let request =
                builder
                    .body(Body::from(request.body))
                    .map_err(|error| MemberError::Failed {
                        code: "bad_request".to_string(),
                        message: format!("malformed forwarded request: {error}"),
                    })?;
            // Its own task, as a request arriving over the network: none
            // of the sender's task-local state (its client deadline, its
            // phase recorder) leaks in; what the request carries comes in
            // its headers.
            let response = match tokio::spawn(router.oneshot(request)).await {
                Ok(Ok(response)) => response,
                Ok(Err(never)) => match never {},
                Err(error) => {
                    return Err(MemberError::Failed {
                        code: "fatal".to_string(),
                        message: format!("forwarded request failed: {error}"),
                    });
                }
            };
            if let Some(not_leader) = response.extensions().get::<NotLeader>() {
                return Err(MemberError::NotLeader {
                    leader: not_leader.leader,
                });
            }
            let status = response.status().as_u16();
            let headers = RESPONSE_HEADERS
                .iter()
                .filter_map(|name| {
                    let value = response.headers().get(*name)?.to_str().ok()?;
                    Some((name.to_string(), value.to_string()))
                })
                .collect();
            let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .map_err(|error| MemberError::Failed {
                    code: "render".to_string(),
                    message: format!("leader response body: {error}"),
                })?;
            Ok(ForwardedResponse {
                status,
                headers,
                body: String::from_utf8_lossy(&body).into_owned(),
            })
        })
    }
}
