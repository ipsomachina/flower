//! `JwtBearer` behind the HTTP server: the server hands the hook an
//! `Authorization: Bearer` header as `{"bearer": token}`, and a module
//! authenticating with the SDK's bearer authenticator admits that caller
//! on calls and watches alike, and refuses a forged token.

extern crate alloc;

mod fixtures;
use fixtures::read_dispatch;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use flower_cluster::network::{InProcessFactory, Registry};
use flower_cluster::node::Node;
use flower_cluster::statemachine::RuntimeFactory;
use flower_crypto::host::CryptoHost;
use flower_crypto::jwt::{Algorithm, KeyFormat, SignOptions};
use flower_engine::runtime::NativeRuntime;
use flower_sdk::Value;
use flower_server::api::{AppState, serve};
use flower_server::watch::Hub;

mod app {
    use flower_sdk::auth::{Access, Authentication, Authorization, BearerKey, JwtBearer};
    use flower_sdk::crypto::jwt::{Algorithm, KeyFormat};
    use flower_sdk::{App, Component, MethodCtx, QueryCtx, Value, query};

    pub const SECRET: &[u8] = b"0123456789abcdef0123456789abcdef";

    static BEARER: JwtBearer = JwtBearer {
        audience: &["api"],
        ..JwtBearer::new(BearerKey::Bytes(
            SECRET,
            KeyFormat::Raw,
            &[Algorithm::Hs256],
        ))
    };

    static AUTH: Authorization = Authorization {
        authenticate: Authentication::Typed(&BEARER),
        default: Access::Authenticated,
        sessions: Access::Authenticated,
        methods: &[],
    };

    /// Who the call runs as.
    #[query(http)]
    pub fn whoami(ctx: &mut QueryCtx<'_, '_>) -> flower_sdk::Result<Value> {
        Ok(ctx
            .principal()?
            .map_or(Value::Null, |principal| Value::String(principal.subject)))
    }

    static DEFS: [&dyn flower_sdk::Component; 1] = [&WHOAMI];
    static USES: [&dyn Component; 1] = [&AUTH];

    pub static APP: App = App::new(&[&USES, &DEFS]);
}

struct Factory;

impl RuntimeFactory for Factory {
    fn build(
        &self,
        _db: &[u8],
        _module: &str,
        _wasm: &[u8],
        _manifest: &Value,
    ) -> Result<Box<dyn flower_engine::runtime::Runtime>, String> {
        Ok(Box::new(
            NativeRuntime::new(&app::APP).with_crypto(Arc::new(CryptoHost::new())),
        ))
    }
}

fn token(subject: &str, secret: &[u8]) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let claims = serde_json::json!({"sub": subject, "aud": "api", "exp": now + 3600});
    let options = SignOptions {
        algorithm: Algorithm::HS256,
        key_format: KeyFormat::Raw,
        kid: None,
        typ: None,
    };
    flower_crypto::jwt::sign(&claims, secret, &options).unwrap()
}

/// An initialized single node with the app deployed, served over HTTP.
async fn serve_node() -> (tempfile::TempDir, String) {
    let dir = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let node = Node::open(
        1,
        &dir.path().join("n1"),
        Arc::new(openraft::Config::default()),
        InProcessFactory::new(registry.clone()),
        Arc::new(Factory),
    )
    .await
    .unwrap();
    registry.register(1, node.raft().clone());
    node.initialize(BTreeSet::from([1])).await.unwrap();
    for _ in 0..500 {
        if node.status().leader == Some(1) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let manifest = NativeRuntime::new(&app::APP).manifest().unwrap();
    node.deploy(b"db", "app", [9u8; 32], b"", &manifest)
        .await
        .unwrap();
    let node = Arc::new(node);
    let mut state = AppState::new(node.clone()).with_admin_token("secret".to_string());
    state.hub = Arc::new(Hub::new(node.clone()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(serve(listener, state));
    let http = reqwest::Client::new();
    for _ in 0..500 {
        if http.get(format!("{base}/v1/status")).send().await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    (dir, base)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bearer_header_authenticates_calls_and_watches() {
    let (_dir, base) = serve_node().await;
    let http = reqwest::Client::new();
    let query = |bearer: String| {
        http.post(format!("{base}/v1/query"))
            .header("authorization", format!("Bearer {bearer}"))
            .json(&serde_json::json!({"db": "db", "module": "app", "name": "whoami", "args": {}}))
            .send()
    };
    let ada = token("ada", app::SECRET);
    let answer = query(ada.clone()).await.unwrap();
    assert_eq!(answer.status(), 200);
    let body: serde_json::Value = answer.json().await.unwrap();
    assert_eq!(body["value"], "ada", "{body}");
    // A token the key never signed is refused.
    let forged = query(token("ada", b"another-secret-of-thirty-two-byte"))
        .await
        .unwrap();
    assert_eq!(forged.status(), 403);
    // Watches take the bearer only.
    let args: serde_json::Value = serde_json::from_slice(b"{}").unwrap();
    let mut watch = http
        .post(format!("{base}/v1/watch"))
        .json(&serde_json::json!({"db": "db", "module": "app", "name": "whoami", "args": args}))
        .header("authorization", format!("Bearer {ada}"))
        .send()
        .await
        .unwrap();
    assert_eq!(watch.status(), 200);
    let first = read_dispatch(&mut watch).await;
    assert!(first.contains("\"ada\""), "{first}");
    // A second call with the same token reuses the decision, and the
    // metrics say so.
    assert_eq!(query(ada).await.unwrap().status(), 200);
    let metrics: serde_json::Value = http
        .get(format!("{base}/v1/metrics"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let decisions = &metrics["authorization"]["decisions"];
    assert!(decisions["hits"].as_u64().unwrap() >= 1, "{decisions}");
    assert!(
        decisions["evaluations"].as_u64().unwrap() >= 2,
        "{decisions}"
    );
    assert!(decisions["entries"].as_u64().unwrap() >= 1, "{decisions}");
    assert_eq!(
        decisions["budget_bytes"],
        flower_engine::decisions::DEFAULT_DECISION_MEMO_BYTES
    );
}
