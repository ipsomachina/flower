//! A reusable loopback HTTP/1 + HTTP/2 probe server for custom transports.
//! It deliberately never ends a streaming response until the caller drops it,
//! detecting implementations that buffer streams or retain abandoned responses.
use crate::{Credentials, FlowerError, TIMEOUT_HEADER, Transport};
use axum::{
    Json, Router,
    body::{Body, Bytes, to_bytes},
    extract::{Request, State},
    http::{HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::any,
};
use futures_util::{StreamExt, stream};
use serde_json::{Value, json};
use std::{
    convert::Infallible,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

#[derive(Clone, Debug, serde::Serialize)]
pub struct Check {
    pub name: String,
    pub passed: bool,
    pub detail: Option<String>,
}
#[derive(Clone, Debug, serde::Serialize)]
pub struct ConformanceReport {
    pub checks: Vec<Check>,
}
impl ConformanceReport {
    pub fn passed(&self) -> bool {
        self.checks.iter().all(|check| check.passed)
    }
    pub fn assert_passed(&self) {
        assert!(self.passed(), "transport conformance failed: {self:#?}");
    }
    fn record(&mut self, name: &str, result: Result<(), String>) {
        self.checks.push(Check {
            name: name.into(),
            passed: result.is_ok(),
            detail: result.err(),
        });
    }
}
#[derive(Clone)]
struct ProbeState(Arc<AtomicUsize>);
struct Active(Arc<AtomicUsize>);
impl Drop for Active {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

pub struct ProbeServer {
    url: String,
    stop: crate::CancellationToken,
    task: Option<tokio::task::JoinHandle<std::io::Result<()>>>,
    active: Arc<AtomicUsize>,
}
impl ProbeServer {
    pub async fn start() -> std::io::Result<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        let active = Arc::new(AtomicUsize::new(0));
        let state = ProbeState(active.clone());
        let app = Router::new()
            .route("/probe/post", any(echo))
            .route("/probe/get", any(echo))
            .route("/probe/refusal", any(refusal))
            .route("/probe/stream", any(events))
            .with_state(state);
        let stop = crate::CancellationToken::new();
        let shutdown = stop.clone();
        let task = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(shutdown.cancelled_owned())
                .await
        });
        Ok(Self {
            url,
            stop,
            task: Some(task),
            active,
        })
    }
    pub fn base_url(&self) -> &str {
        &self.url
    }
    pub async fn shutdown(mut self) {
        self.stop.cancel();
        if let Some(mut task) = self.task.take()
            && tokio::time::timeout(Duration::from_secs(2), &mut task)
                .await
                .is_err()
        {
            task.abort();
        }
    }
    /// Each check has its own timeout; failures do not hide later checks.
    pub async fn check(&self, transport: &dyn Transport) -> ConformanceReport {
        let mut report = ConformanceReport { checks: Vec::new() };
        let credentials = Credentials::token("flower-conformance-secret");
        let headers = [
            (TIMEOUT_HEADER, "1234".to_owned()),
            (flower_protocol::AT_LEAST_HEADER, "probe-bound".to_owned()),
        ];
        let body = br#"{"text":"a & b","value":null}"#;
        let post = timed(async {
            let (response, returned) = transport
                .post_with_headers("/probe/post", body, Some(&credentials), &headers)
                .await?;
            check_echo(&response.body, "POST", "/probe/post", body)?;
            require(response.status == 200, "POST status was changed")?;
            require(
                returned
                    .iter()
                    .any(|(key, value)| key == "x-flower-conformance" && value == "ack"),
                "response metadata header was lost",
            )
        })
        .await;
        report.record("post_body_credentials_and_headers", post);
        let get = timed(async {
            let (response, returned) = transport
                .get_with_headers("/probe/get?value=a%26b", Some(&credentials), &headers)
                .await?;
            check_echo(&response.body, "GET", "/probe/get?value=a%26b", b"")?;
            require(
                response.status == 200
                    && returned
                        .iter()
                        .any(|(key, value)| key == "x-flower-conformance" && value == "ack"),
                "GET response status or metadata was lost",
            )
        })
        .await;
        report.record("get_query_credentials_and_headers", get);
        let refusal = timed(async {
            let response = transport.post("/probe/refusal", b"{}", None).await?;
            require(response.status == 503, "refusal status was changed")?;
            require(
                response.retry_after_ms == Some(2000),
                "Retry-After was not converted to milliseconds",
            )?;
            require(response.body == REFUSAL, "refusal body bytes were changed")
        })
        .await;
        report.record("refusal_status_body_and_retry_hint", refusal);
        let streaming = timed(async {
            let mut response = transport
                .post_stream("/probe/stream", body, Some(&credentials))
                .await?;
            require(response.status == 200, "stream status was changed")?;
            let mut bytes = Vec::new();
            while !bytes.ends_with(b"\n\n") {
                let chunk =
                    response.body.next().await.ok_or_else(|| {
                        FlowerError::Decode("stream ended before its event".into())
                    })??;
                bytes.extend_from_slice(&chunk);
                require(
                    bytes.len() <= 64 * 1024,
                    "stream exceeded probe event bound",
                )?;
            }
            let text =
                std::str::from_utf8(&bytes).map_err(|e| FlowerError::Decode(e.to_string()))?;
            let data = text
                .lines()
                .find_map(|line| line.strip_prefix("data:"))
                .ok_or_else(|| FlowerError::Decode("stream event was changed".into()))?;
            let echoed: Value =
                serde_json::from_str(data).map_err(|e| FlowerError::Decode(e.to_string()))?;
            require(
                echoed["method"] == "POST"
                    && echoed["body"] == std::str::from_utf8(body).expect("literal is UTF-8")
                    && echoed["authorization"] == "Bearer flower-conformance-secret",
                "stream body or credentials were lost",
            )?;
            drop(response);
            while self.active.load(Ordering::SeqCst) != 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            Ok(())
        })
        .await;
        report.record("incremental_streaming_and_release_on_drop", streaming);
        report
    }
}
impl Drop for ProbeServer {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}
async fn timed(
    future: impl std::future::Future<Output = Result<(), FlowerError>>,
) -> Result<(), String> {
    tokio::time::timeout(Duration::from_secs(3), future)
        .await
        .map_err(|_| {
            "probe timed out (a streaming transport must not buffer until EOF)".to_owned()
        })?
        .map_err(|error| error.to_string())
}
fn require(condition: bool, message: &str) -> Result<(), FlowerError> {
    if condition {
        Ok(())
    } else {
        Err(FlowerError::Decode(message.into()))
    }
}
fn check_echo(bytes: &[u8], method: &str, uri: &str, body: &[u8]) -> Result<(), FlowerError> {
    let value: Value =
        serde_json::from_slice(bytes).map_err(|e| FlowerError::Decode(e.to_string()))?;
    require(
        value["method"] == method
            && value["uri"] == uri
            && value["body"] == std::str::from_utf8(body).expect("probe bodies are UTF-8"),
        "method, URI or request bytes were changed",
    )?;
    require(
        value["authorization"] == "Bearer flower-conformance-secret",
        "credentials were lost",
    )?;
    require(
        value["timeout"] == "1234" && value["at_least"] == "probe-bound",
        "deadline or causal header was lost",
    )
}
async fn capture(request: Request) -> Result<Value, StatusCode> {
    let (parts, body) = request.into_parts();
    let bytes = to_bytes(body, 64 * 1024)
        .await
        .map_err(|_| StatusCode::PAYLOAD_TOO_LARGE)?;
    let header = |name: &str| {
        parts
            .headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned)
    };
    Ok(
        json!({"method":parts.method.as_str(),"uri":parts.uri.path_and_query().map(|value| value.as_str()).unwrap_or("/"),"body":String::from_utf8_lossy(&bytes),"authorization":header("authorization"),"timeout":header(TIMEOUT_HEADER),"at_least":header(flower_protocol::AT_LEAST_HEADER)}),
    )
}
async fn echo(request: Request) -> Result<Response, StatusCode> {
    let mut response = Json(capture(request).await?).into_response();
    response
        .headers_mut()
        .insert("x-flower-conformance", HeaderValue::from_static("ack"));
    Ok(response)
}
const REFUSAL: &[u8] = br#"{"ok":false,"code":"overloaded","message":"wait"}"#;
async fn refusal() -> Response {
    let mut response = (StatusCode::SERVICE_UNAVAILABLE, Body::from(REFUSAL)).into_response();
    response
        .headers_mut()
        .insert("retry-after", HeaderValue::from_static("2"));
    response
}
async fn events(State(state): State<ProbeState>, request: Request) -> Result<Response, StatusCode> {
    let value = capture(request).await?;
    state.0.fetch_add(1, Ordering::SeqCst);
    let guard = Active(state.0);
    let first = Bytes::from(format!("event: snapshot\ndata: {value}\n\n"));
    let body = stream::unfold((Some(first), guard), |(part, guard)| async move {
        let part = match part {
            Some(part) => part,
            None => std::future::pending().await,
        };
        Some((Ok::<_, Infallible>(part), (None, guard)))
    });
    Ok((
        [("content-type", "text/event-stream")],
        Body::from_stream(body),
    )
        .into_response())
}
