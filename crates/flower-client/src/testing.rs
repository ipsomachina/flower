//! Test doubles: [`MockTransport`], a scripted in-memory [`Transport`]
//! for testing code built on the client without a server. It answers
//! in script order (or through a handler), records every request, and
//! once the script runs out, leaves requests hanging like an
//! unresponsive server, so a test sees timeouts and cancellation as
//! they happen against a real one.
//!
//! ```
//! # async fn demo() {
//! use flower_client::client::RequestOptions;
//! use flower_client::testing::{MockReply, MockTransport};
//!
//! let mock = MockTransport::scripted([MockReply::ok(serde_json::json!(3))]);
//! let client = mock.client();
//! let count = client
//!     .query_value("db", "app", "count", &serde_json::json!({}), &RequestOptions::new())
//!     .await
//!     .unwrap();
//! assert_eq!(count.value, 3);
//! assert_eq!(mock.requests()[0].path, "/v1/query");
//! # }
//! ```

use std::collections::VecDeque;
#[cfg(feature = "conformance")]
pub mod conformance;
use std::sync::{Arc, Mutex, PoisonError};

use futures_util::StreamExt;
use tokio::sync::mpsc;

use crate::client::FlowerClient;
use crate::error::FlowerError;
use crate::transport::{Credentials, Headers, StreamResponse, Transport, TransportResponse};

/// A scripted reply body.
pub enum MockBody {
    /// The whole body at once.
    Full(Vec<u8>),
    /// Chunks pushed through a [`StreamHandle`] until it closes.
    Stream(mpsc::UnboundedReceiver<Result<Vec<u8>, FlowerError>>),
    /// Never answer: the request hangs until dropped.
    Hang,
}

/// A scripted reply.
pub struct MockReply {
    /// HTTP status.
    pub status: u16,
    /// `Retry-After` in milliseconds.
    pub retry_after_ms: Option<u64>,
    /// Response headers (`x-flower-*` ones reach the client).
    pub headers: Headers,
    /// The body.
    pub body: MockBody,
}

impl MockReply {
    /// A JSON reply with a status.
    pub fn json(status: u16, value: &serde_json::Value) -> Self {
        MockReply {
            status,
            retry_after_ms: None,
            headers: Headers::new(),
            body: MockBody::Full(value.to_string().into_bytes()),
        }
    }

    /// A successful call: `{ok: true, value, revision: 1, position: 1}`.
    pub fn ok(value: serde_json::Value) -> Self {
        Self::json(
            200,
            &serde_json::json!({"ok": true, "value": value, "revision": 1, "position": 1}),
        )
    }

    /// A business failure in the call envelope (`ok: false`), as writes
    /// report them.
    pub fn failure(code: &str, message: &str, details: Option<serde_json::Value>) -> Self {
        let mut body = serde_json::json!({
            "ok": false, "code": code, "message": message, "revision": 1, "position": 1,
        });
        if let Some(details) = details {
            body["details"] = details;
        }
        Self::json(200, &body)
    }

    /// An error status with the server's error envelope.
    pub fn error(status: u16, code: &str, message: &str) -> Self {
        Self::json(
            status,
            &serde_json::json!({"ok": false, "code": code, "message": message}),
        )
    }

    /// A refusal for load: 503 `overloaded` asking for a wait.
    pub fn overloaded(retry_after_ms: Option<u64>) -> Self {
        let mut reply = Self::error(503, "overloaded", "overloaded");
        reply.retry_after_ms = retry_after_ms;
        reply
    }

    /// A forward to the leader.
    pub fn forward(leader: Option<u64>) -> Self {
        Self::json(
            503,
            &serde_json::json!({"ok": false, "code": "forward", "leader": leader}),
        )
    }

    /// A complete event stream, ending after `text`.
    pub fn events(text: &str) -> Self {
        let (reply, handle) = Self::event_stream(text);
        drop(handle);
        reply
    }

    /// An event stream starting with `text`; more is pushed through the
    /// handle until it is closed or dropped.
    pub fn event_stream(text: &str) -> (Self, StreamHandle) {
        let (sender, receiver) = mpsc::unbounded_channel();
        let handle = StreamHandle { sender };
        if !text.is_empty() {
            handle.push(text);
        }
        let reply = MockReply {
            status: 200,
            retry_after_ms: None,
            headers: Headers::new(),
            body: MockBody::Stream(receiver),
        };
        (reply, handle)
    }

    /// A reply that never comes.
    pub fn hang() -> Self {
        MockReply {
            status: 0,
            retry_after_ms: None,
            headers: Headers::new(),
            body: MockBody::Hang,
        }
    }

    /// Add a response header.
    pub fn with_header(mut self, name: &str, value: &str) -> Self {
        self.headers
            .push((name.to_ascii_lowercase(), value.to_string()));
        self
    }
}

/// Feeds a streaming reply.
#[derive(Clone)]
pub struct StreamHandle {
    sender: mpsc::UnboundedSender<Result<Vec<u8>, FlowerError>>,
}

impl StreamHandle {
    /// Push a chunk; false once the client dropped the stream.
    pub fn push(&self, text: &str) -> bool {
        self.sender.send(Ok(text.as_bytes().to_vec())).is_ok()
    }

    /// Fail the stream with a transport error.
    pub fn fail(&self, message: &str) {
        let _ = self
            .sender
            .send(Err(FlowerError::Transport(message.to_string())));
    }

    /// Whether the client still holds the stream.
    pub fn open(&self) -> bool {
        !self.sender.is_closed()
    }
}

/// One request the transport received.
#[derive(Clone, Debug)]
pub struct Recorded {
    /// `POST`, `GET`, or `STREAM` for watch connections.
    pub method: &'static str,
    /// Path and query string.
    pub path: String,
    /// Request body (empty for GETs).
    pub body: Vec<u8>,
    /// Extra request headers (the deadline, staleness bounds).
    pub headers: Vec<(String, String)>,
    /// The `authorization` header the credentials produced.
    pub authorization: Option<String>,
}

impl Recorded {
    /// The body as JSON.
    pub fn json(&self) -> serde_json::Value {
        serde_json::from_slice(&self.body).unwrap_or(serde_json::Value::Null)
    }

    /// A request header by name.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(header, _)| header == name)
            .map(|(_, value)| value.as_str())
    }
}

/// Builds replies from requests.
type Handler = Box<dyn Fn(&Recorded) -> MockReply + Send + Sync>;

/// A scripted [`Transport`]: replies in order, or from a handler.
pub struct MockTransport {
    replies: Mutex<VecDeque<MockReply>>,
    handler: Option<Handler>,
    requests: Mutex<Vec<Recorded>>,
}

impl MockTransport {
    /// A transport answering `replies` in order, then hanging.
    pub fn scripted(replies: impl IntoIterator<Item = MockReply>) -> Arc<Self> {
        Arc::new(MockTransport {
            replies: Mutex::new(replies.into_iter().collect()),
            handler: None,
            requests: Mutex::new(Vec::new()),
        })
    }

    /// A transport answering every request through `handler`; queued
    /// replies (see [`MockTransport::push`]) still go first.
    pub fn with_handler(
        handler: impl Fn(&Recorded) -> MockReply + Send + Sync + 'static,
    ) -> Arc<Self> {
        Arc::new(MockTransport {
            replies: Mutex::new(VecDeque::new()),
            handler: Some(Box::new(handler)),
            requests: Mutex::new(Vec::new()),
        })
    }

    /// Queue another reply.
    pub fn push(&self, reply: MockReply) {
        self.replies
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push_back(reply);
    }

    /// Every request so far, in arrival order.
    pub fn requests(&self) -> Vec<Recorded> {
        self.requests
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// A client over this transport, with no retry.
    pub fn client(self: &Arc<Self>) -> FlowerClient {
        FlowerClient::builder("http://mock")
            .transport(self.clone())
            .build()
            .expect("a client over a transport always builds")
    }

    /// Record a request and pick its reply.
    fn take(
        &self,
        method: &'static str,
        path: &str,
        body: &[u8],
        credentials: Option<&Credentials>,
        headers: &[(&'static str, String)],
    ) -> MockReply {
        let recorded = Recorded {
            method,
            path: path.to_string(),
            body: body.to_vec(),
            headers: headers
                .iter()
                .map(|(name, value)| (name.to_string(), value.clone()))
                .collect(),
            authorization: credentials.and_then(Credentials::header),
        };
        self.requests
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(recorded.clone());
        let queued = self
            .replies
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pop_front();
        match (queued, &self.handler) {
            (Some(reply), _) => reply,
            (None, Some(handler)) => handler(&recorded),
            (None, None) => MockReply::hang(),
        }
    }

    /// Answer a unary request.
    async fn unary(
        &self,
        method: &'static str,
        path: &str,
        body: &[u8],
        credentials: Option<&Credentials>,
        headers: &[(&'static str, String)],
    ) -> Result<(TransportResponse, Headers), FlowerError> {
        let reply = self.take(method, path, body, credentials, headers);
        let body = match reply.body {
            MockBody::Full(bytes) => bytes,
            MockBody::Stream(mut receiver) => {
                let mut bytes = Vec::new();
                while let Some(chunk) = receiver.recv().await {
                    bytes.extend_from_slice(&chunk?);
                }
                bytes
            }
            MockBody::Hang => std::future::pending().await,
        };
        Ok((
            TransportResponse {
                status: reply.status,
                retry_after_ms: reply.retry_after_ms,
                body,
            },
            reply.headers,
        ))
    }
}

#[async_trait::async_trait]
impl Transport for MockTransport {
    async fn post(
        &self,
        path: &str,
        body: &[u8],
        credentials: Option<&Credentials>,
    ) -> Result<TransportResponse, FlowerError> {
        self.unary("POST", path, body, credentials, &[])
            .await
            .map(|(response, _)| response)
    }

    async fn post_with_headers(
        &self,
        path: &str,
        body: &[u8],
        credentials: Option<&Credentials>,
        headers: &[(&'static str, String)],
    ) -> Result<(TransportResponse, Headers), FlowerError> {
        self.unary("POST", path, body, credentials, headers).await
    }

    async fn get(
        &self,
        path: &str,
        credentials: Option<&Credentials>,
    ) -> Result<TransportResponse, FlowerError> {
        self.unary("GET", path, &[], credentials, &[])
            .await
            .map(|(response, _)| response)
    }

    async fn get_with_headers(
        &self,
        path: &str,
        credentials: Option<&Credentials>,
        headers: &[(&'static str, String)],
    ) -> Result<(TransportResponse, Headers), FlowerError> {
        self.unary("GET", path, &[], credentials, headers).await
    }

    async fn post_stream(
        &self,
        path: &str,
        body: &[u8],
        credentials: Option<&Credentials>,
    ) -> Result<StreamResponse, FlowerError> {
        let reply = self.take("STREAM", path, body, credentials, &[]);
        let body = match reply.body {
            MockBody::Full(bytes) => futures_util::stream::iter([Ok(bytes)]).boxed(),
            MockBody::Stream(receiver) => {
                futures_util::stream::unfold(receiver, |mut receiver| async move {
                    let chunk = receiver.recv().await?;
                    Some((chunk, receiver))
                })
                .boxed()
            }
            MockBody::Hang => std::future::pending().await,
        };
        Ok(StreamResponse {
            status: reply.status,
            retry_after_ms: reply.retry_after_ms,
            body,
        })
    }
}
