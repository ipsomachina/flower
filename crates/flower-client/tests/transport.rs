//! HTTP transport: HTTP/2 over TLS by ALPN with extra CA roots, h2c by
//! prior knowledge, pooled connections per lane, and the deadline
//! header, against a local hyper server that serves HTTP/1.1 and
//! HTTP/2 on one port and counts the connections it accepts.

use std::convert::Infallible;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use flower_client::client::{FlowerClient, RequestOptions};
use flower_client::error::FlowerError;
use flower_client::transport::{HttpTransport, Protocol, TransportOptions};
use flower_client::watch::WatchOptions;
use futures_util::StreamExt;
use http_body_util::{BodyExt, Full, StreamBody};
use hyper::body::{Bytes, Frame, Incoming};
use hyper::{Request, Response};
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::rustls::pki_types::pem::PemObject;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};

/// A request as the server saw it.
#[derive(Clone, Debug)]
struct Seen {
    version: hyper::Version,
    path: String,
    timeout_ms: Option<String>,
}

/// A local server: HTTP/1.1 and HTTP/2 on one port, over TLS or not.
struct TestServer {
    base: String,
    connections: Arc<AtomicUsize>,
    seen: Arc<Mutex<Vec<Seen>>>,
}

impl TestServer {
    fn seen(&self) -> Vec<Seen> {
        self.seen.lock().unwrap().clone()
    }

    fn connections(&self) -> usize {
        self.connections.load(Ordering::SeqCst)
    }
}

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("tls")
        .join(name)
}

/// A TLS acceptor for `localhost` offering h2 and HTTP/1.1, with the
/// given leaf certificate.
fn acceptor(certificate: &str) -> TlsAcceptor {
    let chain: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(fixture(certificate))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    let key = PrivateKeyDer::from_pem_file(fixture("node.key")).unwrap();
    let provider = Arc::new(tokio_rustls::rustls::crypto::aws_lc_rs::default_provider());
    let mut config = ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(chain, key)
        .unwrap();
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    TlsAcceptor::from(Arc::new(config))
}

type Body = http_body_util::combinators::BoxBody<Bytes, Infallible>;

/// Answer calls with an `ok` envelope naming the protocol version, and
/// `/v1/watch` with an event stream that stays open.
async fn handle(
    request: Request<Incoming>,
    seen: Arc<Mutex<Vec<Seen>>>,
) -> Result<Response<Body>, Infallible> {
    let version = request.version();
    let path = request.uri().path().to_string();
    let timeout_ms = request
        .headers()
        .get("x-flower-timeout-ms")
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    seen.lock().unwrap().push(Seen {
        version,
        path: path.clone(),
        timeout_ms,
    });
    let _ = request.into_body().collect().await;
    if path == "/v1/watch" {
        let first =
            format!("event: snapshot\ndata: {{\"revision\":1,\"value\":\"{version:?}\"}}\n\n");
        let stream = futures_util::stream::once(async move {
            Ok::<_, Infallible>(Frame::data(Bytes::from(first)))
        })
        .chain(futures_util::stream::pending());
        let body = BodyExt::boxed(StreamBody::new(stream));
        return Ok(Response::builder()
            .header("content-type", "text/event-stream")
            .body(body)
            .unwrap());
    }
    let body = serde_json::json!({
        "ok": true,
        "value": format!("{version:?}"),
        "id": 1, "state": "Leader", "leader": 1, "term": 1,
        "last_log": null, "last_applied": null,
    });
    Ok(Response::builder()
        .header("content-type", "application/json")
        .body(BodyExt::boxed(Full::new(Bytes::from(body.to_string()))))
        .unwrap())
}

/// Serve on 127.0.0.1, over TLS when given an acceptor.
async fn serve(tls: Option<TlsAcceptor>) -> TestServer {
    serve_limited(tls, None).await
}

/// [`serve`], refusing more than `streams` concurrent HTTP/2 streams per
/// connection when given.
async fn serve_limited(tls: Option<TlsAcceptor>, streams: Option<u32>) -> TestServer {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let base = match tls {
        Some(_) => format!("https://localhost:{port}"),
        None => format!("http://127.0.0.1:{port}"),
    };
    let connections = Arc::new(AtomicUsize::new(0));
    let seen = Arc::new(Mutex::new(Vec::new()));
    let (accepted, recorded) = (connections.clone(), seen.clone());
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let (tls, seen) = (tls.clone(), recorded.clone());
            let accepted = accepted.clone();
            tokio::spawn(async move {
                let service =
                    hyper::service::service_fn(move |request| handle(request, seen.clone()));
                let mut builder = auto::Builder::new(TokioExecutor::new());
                if let Some(streams) = streams {
                    builder.http2().max_concurrent_streams(streams);
                }
                match tls {
                    Some(tls) => {
                        // Handshakes the client refuses count as nothing.
                        let Ok(stream) = tls.accept(stream).await else {
                            return;
                        };
                        accepted.fetch_add(1, Ordering::SeqCst);
                        let _ = builder
                            .serve_connection(TokioIo::new(stream), service)
                            .await;
                    }
                    None => {
                        accepted.fetch_add(1, Ordering::SeqCst);
                        let _ = builder
                            .serve_connection(TokioIo::new(stream), service)
                            .await;
                    }
                }
            });
        }
    });
    TestServer {
        base,
        connections,
        seen,
    }
}

fn client(base: &str, options: TransportOptions) -> FlowerClient {
    FlowerClient::builder(base)
        .transport_options(options)
        .build()
        .unwrap()
}

async fn version(client: &FlowerClient) -> Result<String, FlowerError> {
    let result = client
        .query_value(
            "db",
            "app",
            "get",
            &serde_json::json!({}),
            &RequestOptions::default(),
        )
        .await?;
    Ok(result.value.as_str().unwrap().to_string())
}

#[tokio::test]
async fn https_with_a_ca_file_negotiates_http2() {
    let server = serve(Some(acceptor("node.crt"))).await;
    let client = FlowerClient::builder(&server.base)
        .ca_file(fixture("ca.crt"))
        .build()
        .unwrap();
    assert_eq!(version(&client).await.unwrap(), "HTTP/2.0");
    // Trusting only the extra roots works the same.
    let only = self::client(
        &server.base,
        TransportOptions::new()
            .ca_file(fixture("ca.crt"))
            .roots_only(true),
    );
    assert_eq!(version(&only).await.unwrap(), "HTTP/2.0");
    let status = only.status(&RequestOptions::default()).await.unwrap();
    assert_eq!(status.leader, Some(1));
}

#[tokio::test]
async fn https_refuses_servers_its_roots_do_not_vouch_for() {
    // No extra root: the test CA is unknown to the platform.
    let server = serve(Some(acceptor("node.crt"))).await;
    let plain = FlowerClient::builder(&server.base).build().unwrap();
    let error = version(&plain).await.unwrap_err();
    assert!(
        matches!(&error, FlowerError::Transport(message) if message.to_lowercase().contains("certificate")),
        "{error:?}"
    );
    // A leaf signed by a root the client does not trust.
    let untrusted = serve(Some(acceptor("untrusted.crt"))).await;
    let client = client(
        &untrusted.base,
        TransportOptions::new()
            .ca_file(fixture("ca.crt"))
            .roots_only(true),
    );
    let error = version(&client).await.unwrap_err();
    assert!(matches!(error, FlowerError::Transport(_)), "{error:?}");
    assert_eq!(untrusted.connections(), 0);
}

#[tokio::test]
async fn ca_files_must_hold_certificates() {
    let missing = HttpTransport::with_options(
        "https://localhost:1",
        TransportOptions::new().ca_file(fixture("absent.crt")),
    );
    assert!(
        matches!(&missing, Err(FlowerError::Transport(message)) if message.contains("absent.crt")),
        "{missing:?}"
    );
    let not_pem = HttpTransport::with_options(
        "https://localhost:1",
        TransportOptions::new().ca_file(fixture("README.md")),
    );
    assert!(matches!(not_pem, Err(FlowerError::Transport(_))));
    // Other schemes and URLs with credentials are refused up front.
    assert!(HttpTransport::with_options("ftp://localhost", TransportOptions::new()).is_err());
    assert!(
        HttpTransport::with_options("http://user:pw@localhost", TransportOptions::new()).is_err()
    );
}

#[tokio::test]
async fn plain_http_defaults_to_h2c() {
    let server = serve(None).await;
    let client = client(&server.base, TransportOptions::new());
    assert_eq!(version(&client).await.unwrap(), "HTTP/2.0");
    let client = self::client(
        &server.base,
        TransportOptions::new().protocol(Protocol::Http1),
    );
    assert_eq!(version(&client).await.unwrap(), "HTTP/1.1");
}

#[tokio::test]
async fn h2c_multiplexes_concurrent_calls_over_one_connection() {
    let server = serve(None).await;
    let client = client(
        &server.base,
        TransportOptions::new().protocol(Protocol::H2c),
    );
    let calls = (0..16).map(|_| {
        let client = client.clone();
        async move { version(&client).await }
    });
    for version in futures_util::future::join_all(calls).await {
        assert_eq!(version.unwrap(), "HTTP/2.0");
    }
    assert_eq!(server.connections(), 1);
    // HTTP/1.1 holds one connection per call in flight instead.
    let server = serve(None).await;
    let client = self::client(
        &server.base,
        TransportOptions::new().protocol(Protocol::Http1),
    );
    let calls = (0..8).map(|_| {
        let client = client.clone();
        async move { version(&client).await }
    });
    futures_util::future::join_all(calls).await;
    assert!(server.connections() > 1);
}

#[tokio::test]
async fn connection_pools_take_calls_in_turn() {
    let server = serve(None).await;
    let client = client(
        &server.base,
        TransportOptions::new()
            .protocol(Protocol::H2c)
            .connections(3),
    );
    for _ in 0..9 {
        version(&client).await.unwrap();
    }
    assert_eq!(server.connections(), 3);
    assert!(
        HttpTransport::with_options(&server.base, TransportOptions::new().connections(0)).is_err()
    );
}

#[tokio::test]
async fn watches_use_their_own_connection() {
    let server = serve(None).await;
    let client = client(
        &server.base,
        TransportOptions::new().protocol(Protocol::H2c),
    );
    let mut watch = client
        .watch::<serde_json::Value>(
            "db",
            "app",
            "get",
            &serde_json::json!({}),
            &WatchOptions::default(),
        )
        .await
        .unwrap();
    let first = watch.next().await.unwrap().unwrap();
    assert_eq!(first.value, "HTTP/2.0");
    // The watch stays open on its lane; calls open their own.
    version(&client).await.unwrap();
    version(&client).await.unwrap();
    assert_eq!(server.connections(), 2);
}

#[tokio::test]
async fn watches_past_a_servers_stream_limit_open_another_connection() {
    // The server takes two streams per connection; a third watch on a
    // full connection would wait for one of them to close.
    let server = serve_limited(None, Some(2)).await;
    let client = client(&server.base, TransportOptions::new().watch_streams(2));
    let open = |client: FlowerClient| async move {
        let mut watch = client
            .watch::<serde_json::Value>(
                "db",
                "app",
                "get",
                &serde_json::json!({}),
                &WatchOptions::default(),
            )
            .await?;
        watch.next().await.expect("a first snapshot")?;
        Ok::<_, FlowerError>(watch)
    };
    let mut watches = Vec::new();
    for _ in 0..5 {
        let watch = tokio::time::timeout(Duration::from_secs(5), open(client.clone()))
            .await
            .expect("every watch opens")
            .unwrap();
        watches.push(watch);
    }
    assert_eq!(server.connections(), 3);
    // Closed watches free their places: the next ones reuse them.
    watches.truncate(2);
    for _ in 0..3 {
        let watch = tokio::time::timeout(Duration::from_secs(5), open(client.clone()))
            .await
            .expect("every watch opens")
            .unwrap();
        watches.push(watch);
    }
    assert_eq!(server.connections(), 3);
    assert!(
        HttpTransport::with_options(&server.base, TransportOptions::new().watch_streams(0))
            .is_err()
    );
}

#[tokio::test]
async fn calls_tell_the_server_their_deadline() {
    let server = serve(None).await;
    let client = FlowerClient::builder(&server.base).build().unwrap();
    let options = RequestOptions::default().timeout(Duration::from_secs(5));
    client
        .query_value("db", "app", "get", &serde_json::json!({}), &options)
        .await
        .unwrap();
    client.status(&options).await.unwrap();
    // A whole-call deadline sooner than the attempt timeout cuts it.
    let options = RequestOptions::default().deadline(Instant::now() + Duration::from_secs(2));
    client
        .query_value("db", "app", "get", &serde_json::json!({}), &options)
        .await
        .unwrap();
    let seen = server.seen();
    let millis: Vec<u64> = seen
        .iter()
        .map(|seen| {
            seen.timeout_ms
                .as_deref()
                .unwrap_or_else(|| panic!("no deadline on {}", seen.path))
                .parse()
                .unwrap()
        })
        .collect();
    assert_eq!(seen[0].path, "/v1/query");
    assert_eq!(millis[0], 5_000);
    assert_eq!(seen[1].path, "/v1/status");
    assert_eq!(millis[1], 5_000);
    assert!((1..=2_000).contains(&millis[2]), "{millis:?}");
    // Watches are not unary calls and carry none.
    let mut watch = client
        .watch::<serde_json::Value>(
            "db",
            "app",
            "get",
            &serde_json::json!({}),
            &WatchOptions::default(),
        )
        .await
        .unwrap();
    watch.next().await.unwrap().unwrap();
    let watched = server.seen().pop().unwrap();
    assert_eq!(watched.path, "/v1/watch");
    assert_eq!(watched.timeout_ms, None);
    assert_eq!(watched.version, hyper::Version::HTTP_2);
}

#[test]
fn credential_docs_say_what_is_sent() {
    // A secret-scrubbing pass once replaced these docs with a
    // placeholder; rustdoc and IDEs showed it to callers.
    let placeholder = concat!("[RED", "ACTED]");
    let source = std::fs::read_to_string(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join("transport.rs"),
    )
    .unwrap();
    assert!(!source.contains(placeholder));
    assert!(source.contains("sent as `Authorization: Bearer <token>`"));
}
