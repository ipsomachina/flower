//! The API listener over real sockets: HTTP/1.1 and prior-knowledge
//! HTTP/2 in cleartext; over TLS, `h2` and `http/1.1` by ALPN, only to
//! clients trusting the certificate's root, with silent handshakes
//! dropped at their deadline and plaintext refused.

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use flower_cluster::network::{InProcessFactory, Registry};
use flower_cluster::node::Node;
use flower_cluster::statemachine::RuntimeFactory;
use flower_engine::runtime::ClosureRuntime;
use flower_sdk::{Value, fail};
use flower_server::api::{AppState, DEFAULT_DRAIN, serve_with};
use flower_server::listener::{ApiTls, HttpOptions};
use http_body_util::{BodyExt, Empty};
use hyper::body::Bytes;
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls::pki_types::pem::PemObject;
use tokio_rustls::rustls::pki_types::{CertificateDer, ServerName};
use tokio_rustls::rustls::{self, ClientConfig, RootCertStore};

struct NoModules;

impl RuntimeFactory for NoModules {
    fn build(
        &self,
        _db: &[u8],
        _module: &str,
        _wasm: &[u8],
        _manifest: &Value,
    ) -> Result<Box<dyn flower_engine::runtime::Runtime>, String> {
        Ok(Box::new(ClosureRuntime::new(|_, _, _, _, _| {
            Err(fail("UNKNOWN_METHOD", "no definitions"))
        })))
    }
}

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../flower-cluster/tests/fixtures/tls")
        .join(name)
}

/// Serve one initialized member with `options`; returns its address.
async fn serve_member(options: HttpOptions) -> (tempfile::TempDir, std::net::SocketAddr) {
    let dir = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let node = Node::open(
        1,
        &dir.path().join("n1"),
        Arc::new(openraft::Config::default()),
        InProcessFactory::new(registry.clone()),
        Arc::new(NoModules),
    )
    .await
    .unwrap();
    registry.register(1, node.raft().clone());
    node.initialize(BTreeSet::from([1])).await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let state = AppState::new(Arc::new(node));
    tokio::spawn(async move {
        serve_with(
            listener,
            state,
            std::future::pending(),
            DEFAULT_DRAIN,
            &options,
        )
        .await
    });
    (dir, address)
}

/// `GET /v1/status` over HTTP/2 on `io`: its status code.
async fn status_h2<I>(io: I) -> u16
where
    I: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let (mut sender, connection) =
        hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(io))
            .await
            .unwrap();
    tokio::spawn(connection);
    let request = hyper::Request::get("http://localhost/v1/status")
        .body(Empty::<Bytes>::new())
        .unwrap();
    let response = sender.send_request(request).await.unwrap();
    assert_eq!(response.version(), hyper::Version::HTTP_2);
    let status = response.status().as_u16();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert!(body.starts_with(b"{"), "{body:?}");
    status
}

/// `GET /v1/status` over HTTP/1.1 on `io`: its status code.
async fn status_h1<I>(io: I) -> u16
where
    I: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(io))
        .await
        .unwrap();
    tokio::spawn(connection);
    let request = hyper::Request::get("/v1/status")
        .header("host", "localhost")
        .body(Empty::<Bytes>::new())
        .unwrap();
    let response = sender.send_request(request).await.unwrap();
    assert_eq!(response.version(), hyper::Version::HTTP_11);
    response.status().as_u16()
}

/// A TLS client trusting `root`, offering `alpn`.
fn connector(root: &str, alpn: &[&[u8]]) -> TlsConnector {
    let mut roots = RootCertStore::empty();
    for root in CertificateDer::pem_file_iter(fixture(root)).unwrap() {
        roots.add(root.unwrap()).unwrap();
    }
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let mut config = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    config.alpn_protocols = alpn.iter().map(|protocol| protocol.to_vec()).collect();
    TlsConnector::from(Arc::new(config))
}

fn localhost() -> ServerName<'static> {
    ServerName::try_from("localhost").unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn cleartext_serves_http1_and_prior_knowledge_http2() {
    let (_dir, address) = serve_member(HttpOptions::default()).await;
    let h1 = tokio::net::TcpStream::connect(address).await.unwrap();
    assert_eq!(status_h1(h1).await, 200);
    let h2 = tokio::net::TcpStream::connect(address).await.unwrap();
    assert_eq!(status_h2(h2).await, 200);
}

#[tokio::test(flavor = "multi_thread")]
async fn tls_negotiates_h2_or_http1_for_clients_trusting_its_root() {
    let tls = ApiTls::from_files(&fixture("node.crt"), &fixture("node.key"))
        .unwrap()
        .with_handshake_timeout(Duration::from_millis(300));
    let (_dir, address) = serve_member(HttpOptions {
        tls: Some(tls),
        ..HttpOptions::default()
    })
    .await;
    let connect = || tokio::net::TcpStream::connect(address);

    let h2 = connector("ca.crt", &[b"h2", b"http/1.1"])
        .connect(localhost(), connect().await.unwrap())
        .await
        .unwrap();
    assert_eq!(h2.get_ref().1.alpn_protocol(), Some(&b"h2"[..]));
    assert_eq!(status_h2(h2).await, 200);

    let h1 = connector("ca.crt", &[b"http/1.1"])
        .connect(localhost(), connect().await.unwrap())
        .await
        .unwrap();
    assert_eq!(h1.get_ref().1.alpn_protocol(), Some(&b"http/1.1"[..]));
    assert_eq!(status_h1(h1).await, 200);

    // A name the certificate does not cover, or a root the client does
    // not trust, fails the handshake.
    let wrong = connector("ca.crt", &[b"h2"])
        .connect(
            ServerName::try_from("wrong.example").unwrap(),
            connect().await.unwrap(),
        )
        .await;
    assert!(wrong.is_err());
    let untrusted = connector("untrusted.crt", &[b"h2"])
        .connect(localhost(), connect().await.unwrap())
        .await;
    assert!(untrusted.is_err());

    // Plaintext HTTP on the TLS port gets no answer.
    let mut plain = connect().await.unwrap();
    plain
        .write_all(b"GET /v1/status HTTP/1.1\r\nhost: localhost\r\n\r\n")
        .await
        .unwrap();
    let mut answer = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(5), plain.read_to_end(&mut answer)).await;
    assert!(!answer.starts_with(b"HTTP/"), "{answer:?}");

    // A client that never starts its handshake is dropped at the
    // deadline.
    let mut silent = connect().await.unwrap();
    let mut byte = [0u8; 1];
    let closed = tokio::time::timeout(Duration::from_secs(5), silent.read(&mut byte)).await;
    assert!(
        matches!(closed, Ok(Ok(0)) | Ok(Err(_))),
        "a silent handshake was kept: {closed:?}"
    );
}
