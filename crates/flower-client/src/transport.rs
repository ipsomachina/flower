//! Transports: the `Transport` trait plus the HTTP implementation.
//! Custom transports (in-process, message buses) implement the
//! trait; the client stays transport-neutral.

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::error::FlowerError;

/// Client credentials: a static token or a provider consulted on
/// every attempt. Debug redacts secret material.
#[derive(Clone)]
pub enum Credentials {
    /// A static token, sent as `Authorization: Bearer <token>`.
    Token(String),
    /// Operator credential shape (distinct from the admin token).
    Operator(String),
    /// Provider consulted on every attempt.
    Provider(Arc<dyn Fn() -> Option<String> + Send + Sync>),
}

impl std::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Credentials::Token(_) => write!(f, "Token(***)"),
            Credentials::Operator(_) => write!(f, "Operator(***)"),
            Credentials::Provider(_) => write!(f, "Provider(..)"),
        }
    }
}

impl Credentials {
    /// A static token, sent as `Authorization: Bearer <token>`.
    pub fn token(token: impl Into<String>) -> Self {
        Credentials::Token(token.into())
    }

    /// An operator credential shape.
    pub fn operator(token: impl Into<String>) -> Self {
        Credentials::Operator(token.into())
    }

    /// A provider consulted on every attempt.
    pub fn from_fn<F>(provider: F) -> Self
    where
        F: Fn() -> Option<String> + Send + Sync + 'static,
    {
        Credentials::Provider(Arc::new(provider))
    }

    /// Resolve the current header value, if any.
    pub(crate) fn header(&self) -> Option<String> {
        match self {
            Credentials::Token(token) | Credentials::Operator(token) => {
                Some(format!("Bearer {token}"))
            }
            Credentials::Provider(provider) => provider().map(|token| format!("Bearer {token}")),
        }
    }
}

/// A transport-neutral HTTP response.
#[derive(Clone, Debug)]
pub struct TransportResponse {
    /// HTTP status code.
    pub status: u16,
    /// Parsed `Retry-After` in milliseconds, if any.
    pub retry_after_ms: Option<u64>,
    /// Response body bytes.
    pub body: Vec<u8>,
}

/// A stream of body chunks.
pub type ByteStream =
    std::pin::Pin<Box<dyn futures_util::Stream<Item = Result<Vec<u8>, FlowerError>> + Send>>;

/// A streaming response: status and headers now, body chunks later.
pub struct StreamResponse {
    /// HTTP status code.
    pub status: u16,
    /// Parsed `Retry-After` in milliseconds, if any.
    pub retry_after_ms: Option<u64>,
    /// Body chunks.
    pub body: ByteStream,
}

/// Response headers a transport reports: lowercase names. The HTTP
/// transport reports Flower's own (`x-flower-*`) and `server-timing`,
/// where the answering member's time went.
pub type Headers = Vec<(String, String)>;

/// A client transport: POST and GET returning raw responses, plus
/// streaming POST for watches.
#[async_trait::async_trait]
pub trait Transport: Send + Sync {
    /// POST JSON bytes to a path.
    async fn post(
        &self,
        path: &str,
        body: &[u8],
        credentials: Option<&Credentials>,
    ) -> Result<TransportResponse, FlowerError>;

    /// POST with extra request headers, reporting the response's
    /// `x-flower-*` headers too: the staleness bound and report of
    /// replica-local queries, and the leader's URL for routing by
    /// leader. The default rejects nonempty headers: implementations must
    /// preserve them or explicitly refuse the requested guarantees.
    async fn post_with_headers(
        &self,
        path: &str,
        body: &[u8],
        credentials: Option<&Credentials>,
        headers: &[(&'static str, String)],
    ) -> Result<(TransportResponse, Headers), FlowerError> {
        require_headers(headers)?;
        Ok((self.post(path, body, credentials).await?, Headers::new()))
    }

    /// GET a path, including any query string.
    async fn get(
        &self,
        path: &str,
        credentials: Option<&Credentials>,
    ) -> Result<TransportResponse, FlowerError>;

    /// GET with extra request headers (the caller's deadline), reporting
    /// the response's `x-flower-*` headers too. The default rejects nonempty
    /// headers rather than discarding the caller's deadline.
    async fn get_with_headers(
        &self,
        path: &str,
        credentials: Option<&Credentials>,
        headers: &[(&'static str, String)],
    ) -> Result<(TransportResponse, Headers), FlowerError> {
        require_headers(headers)?;
        Ok((self.get(path, credentials).await?, Headers::new()))
    }

    /// POST JSON bytes to a path and answer the response as a chunk
    /// stream (watches). Transport failures surface here; status mapping
    /// is the caller's job, since bodies stream.
    async fn post_stream(
        &self,
        path: &str,
        body: &[u8],
        credentials: Option<&Credentials>,
    ) -> Result<StreamResponse, FlowerError>;
}

/// The request header carrying a unary call's remaining time in whole
/// milliseconds: the server need not queue or evaluate a call after its
/// caller gave up, and may refuse it early under load.
pub const TIMEOUT_HEADER: &str = "x-flower-timeout-ms";

fn require_headers(headers: &[(&'static str, String)]) -> Result<(), FlowerError> {
    if headers.is_empty() {
        return Ok(());
    }
    Err(FlowerError::Unsupported(
        "transport must implement header-preserving requests".into(),
    ))
}

/// Plain-HTTP protocol choice. HTTPS always negotiates by ALPN,
/// preferring HTTP/2.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Protocol {
    /// HTTP/1.1 over plain TCP, for proxies and servers that don't
    /// speak h2c.
    Http1,
    /// HTTP/2 over plain TCP with prior knowledge (h2c, default), as
    /// Flower serves it beside HTTP/1.1 on one port: calls multiplex
    /// over the pooled connections.
    #[default]
    H2c,
}

/// HTTP/2 flow-control windows for one lane, in bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Windows {
    /// Per stream.
    pub stream: u32,
    /// Per connection.
    pub connection: u32,
}

/// [`HttpTransport`] settings. Every timeout is explicit.
#[derive(Clone, Debug)]
pub struct TransportOptions {
    /// Plain-HTTP protocol; HTTPS negotiates by ALPN.
    pub protocol: Protocol,
    /// Unary connection pools per origin, used round-robin. Over
    /// HTTP/2 each pool multiplexes calls over one connection; over
    /// HTTP/1.1 it keeps one connection per call in flight. Default 1.
    pub connections: usize,
    /// Watch connection pools to start with, separate from unary
    /// ones, so unread watches never hold the flow-control window or
    /// the connections unary calls need. Default 1.
    pub watch_connections: usize,
    /// Open watches one watch pool carries before the lane opens
    /// another. Over HTTP/2 every watch is a stream on its pool's one
    /// connection, and a server stops a connection's streams at its
    /// limit (hyper's default is 200), where a further watch would
    /// wait for one to close. Default 100, which also keeps a full
    /// pool's unread watches inside its connection window.
    pub watch_streams: usize,
    /// TCP and TLS connect deadline. Default 10 s.
    pub connect_timeout: Duration,
    /// Close connections idle this long. Default 30 s.
    pub pool_idle_timeout: Duration,
    /// Extra PEM root files for HTTPS, merged with the platform's
    /// verifier. An unreadable file, or one holding no certificate,
    /// fails [`HttpTransport::with_options`].
    pub ca_files: Vec<PathBuf>,
    /// Extra PEM roots, as bytes.
    pub ca_pem: Vec<Vec<u8>>,
    /// Trust only the extra roots, not the platform's. Default false.
    pub roots_only: bool,
    /// Unary lane windows. Default 2 MiB per stream, 5 MiB per
    /// connection.
    pub unary_windows: Windows,
    /// Watch lane windows. Default 256 KiB per stream and 32 MiB per
    /// connection: up to 128 unread watches before one watch
    /// connection stalls.
    pub watch_windows: Windows,
    /// HTTP/2 PING interval, also while idle. Default none.
    pub keep_alive: Option<Duration>,
}

impl Default for TransportOptions {
    fn default() -> Self {
        TransportOptions {
            protocol: Protocol::H2c,
            connections: 1,
            watch_connections: 1,
            watch_streams: 100,
            connect_timeout: Duration::from_secs(10),
            pool_idle_timeout: Duration::from_secs(30),
            ca_files: Vec::new(),
            ca_pem: Vec::new(),
            roots_only: false,
            unary_windows: Windows {
                stream: 2 * 1024 * 1024,
                connection: 5 * 1024 * 1024,
            },
            watch_windows: Windows {
                stream: 256 * 1024,
                connection: 32 * 1024 * 1024,
            },
            keep_alive: None,
        }
    }
}

impl TransportOptions {
    /// Direct Flower connection: multiplexed h2c, or ALPN over TLS.
    pub fn direct() -> Self {
        Self::default()
    }

    /// Proxy-facing connection: HTTP/1.1 for plain HTTP, ALPN over TLS.
    pub fn proxy() -> Self {
        Self::default().protocol(Protocol::Http1)
    }
    /// Default options.
    pub fn new() -> Self {
        TransportOptions::default()
    }

    /// Set the plain-HTTP protocol.
    pub fn protocol(mut self, protocol: Protocol) -> Self {
        self.protocol = protocol;
        self
    }

    /// Set the unary connection pools per origin.
    pub fn connections(mut self, connections: usize) -> Self {
        self.connections = connections;
        self
    }

    /// Set the watch connection pools per origin.
    pub fn watch_connections(mut self, connections: usize) -> Self {
        self.watch_connections = connections;
        self
    }

    /// Set the open watches one watch pool carries.
    pub fn watch_streams(mut self, streams: usize) -> Self {
        self.watch_streams = streams;
        self
    }

    /// Set the connect deadline.
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Trust the PEM roots in this file too.
    pub fn ca_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.ca_files.push(path.into());
        self
    }

    /// Trust these PEM roots too.
    pub fn ca_pem(mut self, pem: impl Into<Vec<u8>>) -> Self {
        self.ca_pem.push(pem.into());
        self
    }

    /// Trust only the extra roots.
    pub fn roots_only(mut self, only: bool) -> Self {
        self.roots_only = only;
        self
    }

    /// Set the HTTP/2 keep-alive PING interval.
    pub fn keep_alive(mut self, interval: Duration) -> Self {
        self.keep_alive = Some(interval);
        self
    }
}

/// Connection pools: unary ones used round-robin, watch ones filled up
/// to `watch_streams` open watches each and added as they fill.
#[derive(Debug)]
struct Lanes {
    unary: Vec<reqwest::Client>,
    watch: Mutex<Vec<WatchPool>>,
    next_unary: AtomicUsize,
    /// Builds another watch pool.
    settings: PoolSettings,
}

/// A watch pool and the watches open on it.
#[derive(Debug)]
struct WatchPool {
    client: reqwest::Client,
    open: Arc<AtomicUsize>,
}

/// One open watch; dropping it frees its place in its pool.
struct OpenWatch(Arc<AtomicUsize>);

impl Drop for OpenWatch {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

impl Lanes {
    /// The next unary pool.
    fn unary(&self) -> &reqwest::Client {
        &self.unary[self.next_unary.fetch_add(1, Ordering::Relaxed) % self.unary.len()]
    }

    /// The least loaded watch pool with room, or a new one when every
    /// pool is full, with the watch's place in it.
    fn watch(&self) -> Result<(reqwest::Client, OpenWatch), FlowerError> {
        let mut pools = self
            .watch
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let least = pools
            .iter()
            .min_by_key(|pool| pool.open.load(Ordering::Relaxed))
            .filter(|pool| pool.open.load(Ordering::Relaxed) < self.settings.options.watch_streams);
        let pool = match least {
            Some(pool) => pool,
            None => {
                pools.push(WatchPool {
                    client: self.settings.build(self.settings.options.watch_windows)?,
                    open: Arc::new(AtomicUsize::new(0)),
                });
                pools.last().expect("just pushed")
            }
        };
        pool.open.fetch_add(1, Ordering::Relaxed);
        Ok((pool.client.clone(), OpenWatch(pool.open.clone())))
    }
}

/// Everything a pool is built from.
#[derive(Debug)]
struct PoolSettings {
    options: TransportOptions,
    tls: bool,
    roots: Vec<reqwest::Certificate>,
}

impl PoolSettings {
    /// A pool with the given flow-control windows.
    fn build(&self, windows: Windows) -> Result<reqwest::Client, FlowerError> {
        let options = &self.options;
        let mut builder = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(options.connect_timeout)
            .pool_idle_timeout(options.pool_idle_timeout)
            .http2_initial_stream_window_size(windows.stream)
            .http2_initial_connection_window_size(windows.connection);
        if let Some(interval) = options.keep_alive {
            builder = builder
                .http2_keep_alive_interval(interval)
                .http2_keep_alive_while_idle(true);
        }
        if self.tls {
            builder = if options.roots_only {
                builder.tls_certs_only(self.roots.clone())
            } else {
                builder.tls_certs_merge(self.roots.clone())
            };
        } else {
            builder = match options.protocol {
                Protocol::Http1 => builder.http1_only(),
                Protocol::H2c => builder.http2_prior_knowledge(),
            };
        }
        builder
            .build()
            .map_err(|error| FlowerError::Transport(error.to_string()))
    }
}

/// The HTTP transport over `reqwest`: HTTP/1.1 or h2c over plain TCP
/// ([`Protocol`]), h2 or HTTP/1.1 by ALPN over TLS (rustls with the
/// platform verifier plus extra roots), pooled connections per lane.
///
/// Requests carry no total timeout: callers bound each attempt
/// themselves (the client's per-attempt timeout, the watcher's stall
/// detection), so a transport-wide cap never silently overrides a
/// longer per-call timeout. Clones share connections.
#[derive(Clone, Debug)]
pub struct HttpTransport {
    base_url: String,
    options: TransportOptions,
    lanes: Arc<Lanes>,
}

impl HttpTransport {
    /// Dial `base_url` (scheme plus authority, no trailing slash) with
    /// default options. `connect_timeout` bounds connection
    /// establishment only.
    pub fn new(
        base_url: impl Into<String>,
        connect_timeout: Duration,
    ) -> Result<Self, FlowerError> {
        Self::with_options(
            base_url,
            TransportOptions::default().connect_timeout(connect_timeout),
        )
    }

    /// Dial `base_url` (an `http://` or `https://` URL without
    /// credentials) with explicit options.
    pub fn with_options(
        base_url: impl Into<String>,
        options: TransportOptions,
    ) -> Result<Self, FlowerError> {
        let base_url = base_url.into();
        let tls = validate_url(&base_url)?;
        if options.connections == 0 || options.watch_connections == 0 || options.watch_streams == 0
        {
            return Err(FlowerError::Transport(
                "connections and watch streams must be at least 1".to_string(),
            ));
        }
        let settings = PoolSettings {
            roots: if tls { roots(&options)? } else { Vec::new() },
            tls,
            options: options.clone(),
        };
        let watch = (0..options.watch_connections)
            .map(|_| {
                Ok(WatchPool {
                    client: settings.build(options.watch_windows)?,
                    open: Arc::new(AtomicUsize::new(0)),
                })
            })
            .collect::<Result<_, FlowerError>>()?;
        let lanes = Lanes {
            unary: (0..options.connections)
                .map(|_| settings.build(options.unary_windows))
                .collect::<Result<_, _>>()?,
            watch: Mutex::new(watch),
            next_unary: AtomicUsize::new(0),
            settings,
        };
        Ok(HttpTransport {
            base_url,
            options,
            lanes: Arc::new(lanes),
        })
    }

    /// The base URL this transport dials.
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// The options this transport was built with.
    pub fn options(&self) -> &TransportOptions {
        &self.options
    }

    /// Send a request and convert the response.
    async fn convert(
        &self,
        response: Result<reqwest::Response, reqwest::Error>,
    ) -> Result<TransportResponse, FlowerError> {
        self.convert_with_headers(response)
            .await
            .map(|(response, _)| response)
    }

    /// [`HttpTransport::convert`], with the response's `x-flower-*`
    /// headers.
    async fn convert_with_headers(
        &self,
        response: Result<reqwest::Response, reqwest::Error>,
    ) -> Result<(TransportResponse, Headers), FlowerError> {
        let response = response.map_err(request_error)?;
        let status = response.status().as_u16();
        let retry_after_ms = retry_after(response.headers());
        let headers = response
            .headers()
            .iter()
            .filter(|(name, _)| {
                name.as_str().starts_with("x-flower-") || name.as_str() == "server-timing"
            })
            .filter_map(|(name, value)| {
                Some((name.as_str().to_string(), value.to_str().ok()?.to_string()))
            })
            .collect();
        let body = response
            .bytes()
            .await
            .map_err(|error| FlowerError::Transport(error.to_string()))?
            .to_vec();
        Ok((
            TransportResponse {
                status,
                retry_after_ms,
                body,
            },
            headers,
        ))
    }

    /// A POST on the unary lane.
    fn post_request(
        &self,
        path: &str,
        body: &[u8],
        credentials: Option<&Credentials>,
    ) -> reqwest::RequestBuilder {
        let url = format!("{}{path}", self.base_url);
        let mut request = self
            .lanes
            .unary()
            .post(url)
            .header("content-type", "application/json")
            .body(body.to_vec());
        if let Some(header) = credentials.and_then(|creds| creds.header()) {
            request = request.header("authorization", header);
        }
        request
    }

    /// A GET on the given pool.
    fn get_request(
        &self,
        client: &reqwest::Client,
        path: &str,
        credentials: Option<&Credentials>,
    ) -> reqwest::RequestBuilder {
        let url = format!("{}{path}", self.base_url);
        let mut request = client.get(url);
        if let Some(header) = credentials.and_then(|creds| creds.header()) {
            request = request.header("authorization", header);
        }
        request
    }
}

#[async_trait::async_trait]
impl Transport for HttpTransport {
    async fn post(
        &self,
        path: &str,
        body: &[u8],
        credentials: Option<&Credentials>,
    ) -> Result<TransportResponse, FlowerError> {
        let request = self.post_request(path, body, credentials);
        self.convert(request.send().await).await
    }

    async fn post_with_headers(
        &self,
        path: &str,
        body: &[u8],
        credentials: Option<&Credentials>,
        headers: &[(&'static str, String)],
    ) -> Result<(TransportResponse, Headers), FlowerError> {
        let mut request = self.post_request(path, body, credentials);
        for (name, value) in headers {
            request = request.header(*name, value);
        }
        self.convert_with_headers(request.send().await).await
    }

    async fn get(
        &self,
        path: &str,
        credentials: Option<&Credentials>,
    ) -> Result<TransportResponse, FlowerError> {
        let request = self.get_request(self.lanes.unary(), path, credentials);
        self.convert(request.send().await).await
    }

    async fn get_with_headers(
        &self,
        path: &str,
        credentials: Option<&Credentials>,
        headers: &[(&'static str, String)],
    ) -> Result<(TransportResponse, Headers), FlowerError> {
        let mut request = self.get_request(self.lanes.unary(), path, credentials);
        for (name, value) in headers {
            request = request.header(*name, value);
        }
        self.convert_with_headers(request.send().await).await
    }

    async fn post_stream(
        &self,
        path: &str,
        body: &[u8],
        credentials: Option<&Credentials>,
    ) -> Result<StreamResponse, FlowerError> {
        use futures_util::StreamExt;
        let (pool, open) = self.lanes.watch()?;
        let mut request = pool
            .post(format!("{}{path}", self.base_url))
            .header("content-type", "application/json")
            .header("accept", "text/event-stream")
            .body(body.to_vec());
        if let Some(header) = credentials.and_then(|creds| creds.header()) {
            request = request.header("authorization", header);
        }
        let response = request.send().await.map_err(request_error)?;
        let status = response.status().as_u16();
        let retry_after_ms = retry_after(response.headers());
        let body = response
            .bytes_stream()
            .map(move |chunk| {
                // The watch holds its place until its body is dropped.
                let _open = &open;
                chunk
                    .map(|bytes| bytes.to_vec())
                    .map_err(|error| FlowerError::Transport(error.to_string()))
            })
            .boxed();
        Ok(StreamResponse {
            status,
            retry_after_ms,
            body,
        })
    }
}

/// Map a failed send: timeouts stay timeouts, everything else is a
/// transport failure with its causes (TLS verification, a refused
/// connection, a protocol error) spelled out.
fn request_error(error: reqwest::Error) -> FlowerError {
    if error.is_timeout() {
        return FlowerError::Timeout;
    }
    let mut message = error.to_string();
    let mut source = std::error::Error::source(&error);
    while let Some(cause) = source {
        message.push_str(": ");
        message.push_str(&cause.to_string());
        source = cause.source();
    }
    FlowerError::Transport(message)
}

/// Whether `url` is HTTPS; refuses other schemes and URLs carrying
/// credentials.
fn validate_url(url: &str) -> Result<bool, FlowerError> {
    let invalid = || {
        FlowerError::Transport(format!(
            "{url}: the HTTP transport needs an http:// or https:// URL without credentials"
        ))
    };
    let parsed = reqwest::Url::parse(url).map_err(|_| invalid())?;
    let tls = match parsed.scheme() {
        "http" => false,
        "https" => true,
        _ => return Err(invalid()),
    };
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(invalid());
    }
    Ok(tls)
}

/// The extra PEM roots the options name.
fn roots(options: &TransportOptions) -> Result<Vec<reqwest::Certificate>, FlowerError> {
    let parse = |pem: &[u8], origin: &str| {
        let certificates = reqwest::Certificate::from_pem_bundle(pem).map_err(|error| {
            FlowerError::Transport(format!("invalid PEM roots in {origin}: {error}"))
        })?;
        if certificates.is_empty() {
            return Err(FlowerError::Transport(format!(
                "no PEM certificates in {origin}"
            )));
        }
        Ok(certificates)
    };
    let mut roots = Vec::new();
    for pem in &options.ca_pem {
        roots.extend(parse(pem, "ca_pem")?);
    }
    for path in &options.ca_files {
        let pem = std::fs::read(path).map_err(|error| {
            FlowerError::Transport(format!("cannot read CA file {}: {error}", path.display()))
        })?;
        roots.extend(parse(&pem, &path.display().to_string())?);
    }
    Ok(roots)
}

/// Parse `Retry-After` seconds into milliseconds, if present.
fn retry_after(headers: &reqwest::header::HeaderMap) -> Option<u64> {
    headers
        .get("retry-after")
        .and_then(|value| value.to_str().ok())
        .and_then(|text| text.trim().parse::<u64>().ok())
        .map(|seconds| seconds.saturating_mul(1000))
}
