//! Native client integration: unary calls, error surfaces, retry,
//! redelivery dedup, sync-down replicas and the offline intent
//! queue against a live server.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use flower_client::CommandOptions;
use flower_client::client::{FlowerClient, MutationOptions, RequestOptions};
use flower_client::error::FlowerError;
use flower_client::queue::IntentQueue;
use flower_client::retry::{Retry, RetryPolicy};
use flower_client::sync::SyncClient;
use flower_client::transaction::{TransactionOptions, TransactionStep};
use flower_client::transport::{Credentials, HttpTransport, Transport, TransportResponse};
use flower_cluster::network::{InProcessFactory, Registry};
use flower_cluster::node::Node;
use flower_cluster::statemachine::{RuntimeFactory, StateMachine};
use flower_engine::engine::{CallOptions, Engine};
use flower_engine::runtime::{CallKind, ClosureRuntime, HostCall};
use flower_engine::storage::GroupStore;
use flower_sdk::abi::{HostOp, decode_envelope};
use flower_sdk::{Failure, Value, fail};
use flower_server::api::{AppState, serve};

fn s(text: &str) -> Value {
    Value::String(text.to_string())
}

fn obj(pairs: Vec<(&str, Value)>) -> Value {
    Value::Object(
        pairs
            .into_iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect(),
    )
}

fn k(name: &str) -> String {
    format!("\"{name}\"")
}

fn hc(host: &mut dyn HostCall, op: HostOp, args: Vec<Value>) -> Result<Value, Failure> {
    let input = Value::Array(args).encode_binary().unwrap();
    let output = host.op(op as u32 as u8, &input);
    let envelope = Value::decode_binary(&output).unwrap();
    decode_envelope(envelope)
}

fn opt(value: Value) -> Option<Value> {
    if value == Value::Null {
        None
    } else {
        Some(value)
    }
}

type Handler = Arc<
    dyn Fn(CallKind, &str, &str, &Value, &mut dyn HostCall) -> Result<Value, Failure> + Send + Sync,
>;

fn app_handler() -> Handler {
    Arc::new(
        |kind: CallKind, _module: &str, name: &str, args: &Value, host: &mut dyn HostCall| {
            let map = match args {
                Value::Object(map) => map.clone(),
                _ => return Err(fail("TYPE_MISMATCH", "args take an object")),
            };
            if let (CallKind::Mutation, "fill", Some(Value::Number(rows))) =
                (kind, name, map.get("rows"))
            {
                for row in 0..*rows as u64 {
                    let key = k(&format!("f{row}"));
                    let title = s(&format!("t{row}"));
                    hc(
                        host,
                        HostOp::Set,
                        vec![s(""), s("tasks"), s(&key), obj(vec![("title", title)])],
                    )?;
                }
                return Ok(Value::Null);
            }
            let key = match map.get("key") {
                Some(Value::String(key)) => key.clone(),
                _ => return Err(fail("TYPE_MISMATCH", "args need a key")),
            };
            match (kind, name) {
                (CallKind::Mutation, "put") => {
                    let title = map.get("title").cloned().unwrap_or(Value::Null);
                    hc(
                        host,
                        HostOp::Set,
                        vec![s(""), s("tasks"), s(&key), obj(vec![("title", title)])],
                    )?;
                    Ok(s("put-ok"))
                }
                (CallKind::Query, "get") => {
                    Ok(
                        opt(hc(host, HostOp::Get, vec![s(""), s("tasks"), s(&key)])?)
                            .unwrap_or(Value::Null),
                    )
                }
                _ => Err(fail("UNKNOWN_METHOD", "unknown definition")),
            }
        },
    )
}

#[derive(Clone)]
struct TestFactory {
    handler: Handler,
}

impl RuntimeFactory for TestFactory {
    fn build(
        &self,
        _db: &[u8],
        _module: &str,
        _wasm: &[u8],
        _manifest: &Value,
    ) -> Result<Box<dyn flower_engine::runtime::Runtime>, String> {
        let handler = self.handler.clone();
        Ok(Box::new(ClosureRuntime::new(
            move |kind, module, name, args, host| handler(kind, module, name, args, host),
        )))
    }
}

fn device_builder() -> flower_client::sync::DeviceRuntimeBuilder {
    let handler = app_handler();
    Arc::new(move |_module: &str, _wasm: &[u8], _manifest: &Value| {
        let handler = handler.clone();
        Ok(Box::new(ClosureRuntime::new(
            move |kind, module, name, args, host| handler(kind, module, name, args, host),
        )) as Box<dyn flower_engine::runtime::Runtime>)
    })
}

fn manifest() -> serde_json::Value {
    serde_json::json!({
        "collections": [{
            "name": "tasks",
            "key": {"type": "string"},
            "value": {"type": "object", "fields": [
                {"name": "title", "schema": {"type": "string"}, "optional": false}
            ]},
            "indexes": [{"name": "by_title", "fields": ["title"]}],
            "references": []
        }],
        "definitions": [
            {"name": "put", "kind": "mutation", "args": {"type": "json"}},
            {"name": "get", "kind": "query", "args": {"type": "json"}}
        ],
        "triggers": [],
        "http": [
            {"alias": "put", "definition": "put"},
            {"alias": "get", "definition": "get"}
        ]
    })
}

struct Served {
    node: Arc<Node>,
    base: String,
}

async fn initialize_queue(served: &Served, client: &FlowerClient, queue: &IntentQueue) {
    served.node.establish_incarnation().await.unwrap();
    client
        .control_retention(
            "db",
            serde_json::json!({"operation":"rotate","keep_epochs":2}),
            &RequestOptions::new(),
        )
        .await
        .unwrap();
    queue
        .initialize("db", &RequestOptions::new())
        .await
        .unwrap();
}

async fn serve_node(initialized: bool) -> (tempfile::TempDir, Served) {
    let dir = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let node = Node::open(
        1,
        &dir.path().join("n1"),
        Arc::new(openraft::Config::default()),
        InProcessFactory::new(registry.clone()),
        Arc::new(TestFactory {
            handler: app_handler(),
        }),
    )
    .await
    .unwrap();
    registry.register(1, node.raft().clone());
    if initialized {
        node.initialize(BTreeSet::from([1])).await.unwrap();
        for _ in 0..500 {
            if node.status().leader == Some(1) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(node.status().leader, Some(1));
    }
    let node = Arc::new(node);
    let state = AppState::new(node.clone()).with_admin_token("secret".to_string());
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
    (dir, Served { base, node })
}

/// A server that can restart on its port: abort the task, then
/// re-serve the same node on the same address.
struct Restartable {
    _dir: tempfile::TempDir,
    base: String,
    node: Arc<Node>,
    task: Option<tokio::task::JoinHandle<std::io::Result<()>>>,
}

/// Open an initialized single node and wait for its leadership.
async fn open_leader(dir: &std::path::Path) -> Arc<Node> {
    let registry = Registry::new();
    let node = Node::open(
        1,
        &dir.join("n1"),
        Arc::new(openraft::Config::default()),
        InProcessFactory::new(registry.clone()),
        Arc::new(TestFactory {
            handler: app_handler(),
        }),
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
    Arc::new(node)
}

impl Restartable {
    async fn start() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let node = open_leader(dir.path()).await;
        let state = AppState::new(node.clone()).with_admin_token("secret".to_string());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(serve(listener, state));
        Restartable {
            _dir: dir,
            base,
            node,
            task: Some(task),
        }
    }

    async fn restart(&mut self) {
        let task = self.task.take().unwrap();
        task.abort();
        let _ = task.await;
        let addr = self.base.strip_prefix("http://").unwrap().to_string();
        let state = AppState::new(self.node.clone()).with_admin_token("secret".to_string());
        let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
        self.task = Some(tokio::spawn(serve(listener, state)));
    }
}

fn admin_client(base: &str) -> FlowerClient {
    FlowerClient::builder(base)
        .credentials(Credentials::Token("secret".to_string()))
        .build()
        .unwrap()
}

fn device_engine(dir: &std::path::Path, name: &str) -> Arc<tokio::sync::Mutex<Engine>> {
    let store = GroupStore::create(dir.join(name)).unwrap();
    Arc::new(tokio::sync::Mutex::new(Engine::new(store)))
}

/// A sync client carrying the admin token: sync serving is gated.
fn sync_client(
    base: &str,
    engine: Arc<tokio::sync::Mutex<Engine>>,
    builder: flower_client::sync::DeviceRuntimeBuilder,
) -> SyncClient {
    let transport = Arc::new(HttpTransport::new(base, Duration::from_secs(20)).unwrap());
    SyncClient::new(
        transport,
        Some(Credentials::token("secret")),
        engine,
        builder,
    )
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or(0)
}

#[tokio::test]
async fn unary_roundtrip_via_client() {
    let (_dir, served) = serve_node(true).await;
    let client = admin_client(&served.base);
    let deployed = client
        .deploy(
            "db",
            "app",
            None,
            b"{}",
            &manifest(),
            &RequestOptions::default(),
        )
        .await
        .unwrap();
    assert!(deployed.position >= 1);

    let args = serde_json::json!({"key": k("a"), "title": "first"});
    let mutated = client
        .mutate_value("db", "app", "put", &args, &MutationOptions::default())
        .await
        .unwrap();
    assert_eq!(mutated.value, serde_json::json!("put-ok"));
    assert!(!mutated.duplicate);
    // A retry under the same request ID answers the original from its
    // receipt, and says so.
    let keyed = MutationOptions {
        request_id: Some("order-1".to_string()),
        ..MutationOptions::default()
    };
    let args = serde_json::json!({"key": k("b"), "title": "first"});
    let original = client
        .mutate_value("db", "app", "put", &args, &keyed)
        .await
        .unwrap();
    assert!(!original.duplicate);
    let again = client
        .mutate_value("db", "app", "put", &args, &keyed)
        .await
        .unwrap();
    assert!(again.duplicate);
    assert_eq!(
        (again.value, again.revision, again.position),
        (original.value, original.revision, original.position)
    );

    let args = serde_json::json!({"key": k("a")});
    let got = client
        .query_value("db", "app", "get", &args, &RequestOptions::default())
        .await
        .unwrap();
    assert_eq!(got.value, serde_json::json!({"title": "first"}));

    let status = client.status(&RequestOptions::default()).await.unwrap();
    assert_eq!(status.id, 1);
    assert_eq!(status.leader, Some(1));
}

#[tokio::test]
async fn client_surfaces_business_forward_and_auth() {
    let (_dir, served) = serve_node(true).await;
    let client = admin_client(&served.base);
    client
        .deploy(
            "db",
            "app",
            None,
            b"{}",
            &manifest(),
            &RequestOptions::default(),
        )
        .await
        .unwrap();

    let args = serde_json::json!({"title": "no key"});
    let error = client
        .mutate_value("db", "app", "put", &args, &MutationOptions::default())
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        FlowerError::Business { ref code, .. } if code == "TYPE_MISMATCH"
    ));

    let args = serde_json::json!({"key": k("a")});
    let error = client
        .query_value("db", "app", "nope", &args, &RequestOptions::default())
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        FlowerError::Http(ref problem) if problem.code == "UNKNOWN_METHOD"
    ));

    let open = FlowerClient::builder(&served.base).build().unwrap();
    let error = open
        .deploy(
            "db",
            "app",
            None,
            b"{}",
            &manifest(),
            &RequestOptions::default(),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(error, FlowerError::Http(ref problem) if problem.origin==flower_client::Origin::Authorization)
    );

    let (_dir, uninit) = serve_node(false).await;
    let lost = FlowerClient::builder(&uninit.base).build().unwrap();
    let error = lost
        .query_value("db", "app", "get", &args, &RequestOptions::default())
        .await
        .unwrap_err();
    assert!(matches!(error, FlowerError::Forward { leader: None }));
}

#[tokio::test(flavor = "multi_thread")]
async fn admin_inspects_membership_and_moves_leadership_over_http() {
    let (_dir, served) = serve_node(true).await;
    let admin = admin_client(&served.base);
    let options = RequestOptions::default();
    let view = admin.membership_view(&options).await.unwrap();
    assert_eq!(view.leader, Some(1));
    assert_eq!(view.voter_configs, vec![BTreeSet::from([1])]);
    let compatibility = flower_cluster::compat::compatibility();
    assert_eq!(
        view.compatibility.state_machine,
        compatibility.state_machine
    );
    assert_eq!(
        view.compatibility.guest_runtime,
        compatibility.guest_runtime
    );

    // Planned against the current membership: committed.
    let mut change = flower_client::membership::MembershipChange {
        members: std::collections::BTreeMap::from([(1, String::new())]),
        expected_log_id: view.log_id,
        retain: false,
    };
    let committed = admin.change_membership(&change, &options).await.unwrap();
    assert!(committed.log_id > view.log_id);
    // The same plan again is stale: a conflict to inspect.
    let error = admin
        .change_membership(&change, &options)
        .await
        .unwrap_err();
    assert!(
        matches!(error, FlowerError::Http(ref problem) if problem.code == "membership"),
        "{error:?}"
    );
    change.expected_log_id = committed.log_id;
    admin.change_membership(&change, &options).await.unwrap();

    // The leader already leads; a non-voter cannot take over.
    let outcome = admin.transfer_leadership(1, &options).await.unwrap();
    assert!(outcome.unchanged);
    let error = admin.transfer_leadership(2, &options).await.unwrap_err();
    assert!(
        matches!(error, FlowerError::Http(ref problem) if problem.code == "transfer_failed"),
        "{error:?}"
    );
    // Both are operator surfaces.
    let open = FlowerClient::builder(&served.base).build().unwrap();
    assert!(matches!(
        open.membership_view(&options).await.unwrap_err(),
        FlowerError::Http(ref problem) if problem.origin==flower_client::Origin::Authorization
    ));
}

/// A transport failing its first `failures` POSTs, then delegating.
struct Flaky {
    inner: HttpTransport,
    failures: AtomicUsize,
}

#[async_trait::async_trait]
impl Transport for Flaky {
    async fn post_with_headers(
        &self,
        path: &str,
        body: &[u8],
        credentials: Option<&Credentials>,
        headers: &[(&'static str, String)],
    ) -> Result<(TransportResponse, flower_client::transport::Headers), FlowerError> {
        if self
            .failures
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok()
        {
            return Err(FlowerError::Transport("boom".into()));
        }
        self.inner
            .post_with_headers(path, body, credentials, headers)
            .await
    }
    async fn get_with_headers(
        &self,
        path: &str,
        credentials: Option<&Credentials>,
        headers: &[(&'static str, String)],
    ) -> Result<(TransportResponse, flower_client::transport::Headers), FlowerError> {
        self.inner
            .get_with_headers(path, credentials, headers)
            .await
    }

    async fn post(
        &self,
        path: &str,
        body: &[u8],
        credentials: Option<&Credentials>,
    ) -> Result<TransportResponse, FlowerError> {
        if self.failures.fetch_sub(1, Ordering::SeqCst) > 0 {
            return Err(FlowerError::Transport("boom".to_string()));
        }
        self.inner.post(path, body, credentials).await
    }

    async fn get(
        &self,
        path: &str,
        credentials: Option<&Credentials>,
    ) -> Result<TransportResponse, FlowerError> {
        self.inner.get(path, credentials).await
    }

    async fn post_stream(
        &self,
        path: &str,
        body: &[u8],
        credentials: Option<&Credentials>,
    ) -> Result<flower_client::transport::StreamResponse, FlowerError> {
        self.inner.post_stream(path, body, credentials).await
    }
}

#[tokio::test]
async fn retry_recovers_flaky_transport() {
    let (_dir, served) = serve_node(true).await;
    let admin = admin_client(&served.base);
    admin
        .deploy(
            "db",
            "app",
            None,
            b"{}",
            &manifest(),
            &RequestOptions::default(),
        )
        .await
        .unwrap();

    let flaky = Arc::new(Flaky {
        inner: HttpTransport::new(&served.base, Duration::from_secs(20)).unwrap(),
        failures: AtomicUsize::new(2),
    });
    let client = FlowerClient::builder(&served.base)
        .transport(flaky)
        .build()
        .unwrap();
    let args = serde_json::json!({"key": k("missing")});
    let policy = RetryPolicy {
        attempts: 4,
        initial_delay: Duration::from_millis(1),
        max_delay: Duration::from_millis(5),
        attempt_timeout: Duration::from_secs(20),
        ..RetryPolicy::default()
    };
    let options = RequestOptions::default().retry(Retry::Policy(policy));
    let got = client
        .query_value("db", "app", "get", &args, &options)
        .await
        .unwrap();
    assert_eq!(got.value, serde_json::Value::Null);
}

#[tokio::test]
async fn mutation_redelivery_dedups() {
    let (_dir, served) = serve_node(true).await;
    let client = admin_client(&served.base);
    client
        .deploy(
            "db",
            "app",
            None,
            b"{}",
            &manifest(),
            &RequestOptions::default(),
        )
        .await
        .unwrap();

    let args = serde_json::json!({"key": k("a"), "title": "once"});
    let options = MutationOptions::new().request_id("fixed-1".to_string());
    let first = client
        .mutate_value("db", "app", "put", &args, &options)
        .await
        .unwrap();
    let second = client
        .mutate_value("db", "app", "put", &args, &options)
        .await
        .unwrap();
    assert_eq!(first.value, second.value);
    assert_eq!(first.revision, second.revision);
    // The redelivery short-circuits without replicating, and answers
    // where the original committed.
    assert!(first.position > 0);
    assert_eq!(second.position, first.position);
}

#[tokio::test]
async fn sync_catch_up_into_device() {
    let (dir, served) = serve_node(true).await;
    let client = admin_client(&served.base);
    client
        .deploy(
            "db",
            "app",
            None,
            b"{}",
            &manifest(),
            &RequestOptions::default(),
        )
        .await
        .unwrap();
    let args = serde_json::json!({"key": k("a"), "title": "synced"});
    client
        .mutate_value("db", "app", "put", &args, &MutationOptions::default())
        .await
        .unwrap();

    let engine = device_engine(dir.path(), "device.redb");
    let sync = sync_client(&served.base, engine.clone(), device_builder());
    assert_eq!(sync.position().await.unwrap(), 0);
    let first = sync.sync_once(64).await.unwrap();
    assert!(first.applied >= 2);
    let progress = sync.sync_catch_up(64).await.unwrap();
    assert!(progress.current >= 1);
    assert_eq!(sync.position().await.unwrap(), progress.current);

    // The device serves the synced row locally.
    let engine = engine.lock().await;
    let args = flower_engine::json::parse_value(br#"{"key": "\"a\""}"#).unwrap();
    let got = engine
        .invoke_query(b"db", "app", "get", &args, &CallOptions::default())
        .unwrap();
    let rendered = flower_engine::json::render(&got).unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&rendered).unwrap(),
        serde_json::json!({"title": "synced"})
    );
}

#[tokio::test]
async fn intent_queue_offline_then_submit() {
    let (dir, served) = serve_node(true).await;
    let client = admin_client(&served.base);
    client
        .deploy(
            "db",
            "app",
            None,
            b"{}",
            &manifest(),
            &RequestOptions::default(),
        )
        .await
        .unwrap();

    let engine = device_engine(dir.path(), "queue.redb");
    let queue = IntentQueue::new(client.clone(), engine.clone());
    initialize_queue(&served, &client, &queue).await;
    let args = flower_engine::json::parse_value(br#"{"key": "\"q\"", "title": "queued"}"#).unwrap();
    queue
        .enqueue(b"db", "app", "put", &args, None)
        .await
        .unwrap();
    let summary = queue.submit_pending(8).await.unwrap();
    assert_eq!(summary.submitted, 1);
    assert_eq!(summary.failed_business, 0);
    assert_eq!(summary.still_pending, 0);

    let args = serde_json::json!({"key": k("q")});
    let got = client
        .query_value("db", "app", "get", &args, &RequestOptions::default())
        .await
        .unwrap();
    assert_eq!(got.value, serde_json::json!({"title": "queued"}));

    // Business failures mark the intent failed instead of aborting.
    let args = flower_engine::json::parse_value(br#"{"title": "no key"}"#).unwrap();
    queue
        .enqueue(b"db", "app", "put", &args, None)
        .await
        .unwrap();
    let summary = queue.submit_pending(8).await.unwrap();
    assert_eq!(summary.submitted, 0);
    assert_eq!(summary.failed_business, 1);

    // The accepted intent finishes once sync-down delivers its receipt.
    sync_client(&served.base, engine.clone(), device_builder())
        .sync_catch_up(64)
        .await
        .unwrap();
    let pruned = queue.prune_finished(0).await.unwrap();
    assert_eq!(pruned, 2);
}

#[tokio::test]
async fn watch_deltas_stream_snapshots() {
    use flower_client::watch::{WatchEvent, WatchOptions};
    let (_dir, served) = serve_node(true).await;
    let client = admin_client(&served.base);
    client
        .deploy(
            "db",
            "app",
            None,
            b"{}",
            &manifest(),
            &RequestOptions::default(),
        )
        .await
        .unwrap();
    let args = serde_json::json!({"key": k("a"), "title": "first"});
    client
        .mutate_value("db", "app", "put", &args, &MutationOptions::default())
        .await
        .unwrap();

    let args = serde_json::json!({"key": k("a")});
    let mut watch = client
        .watch_deltas("db", "app", "get", &args, &WatchOptions::default())
        .await
        .unwrap();
    match watch.next_event().await.unwrap().unwrap() {
        WatchEvent::Snapshot { value, .. } => {
            assert_eq!(value, serde_json::json!({"title": "first"}))
        }
        WatchEvent::Reset { .. } => panic!("first event is a snapshot"),
    }
    let args = serde_json::json!({"key": k("a"), "title": "second"});
    client
        .mutate_value("db", "app", "put", &args, &MutationOptions::default())
        .await
        .unwrap();
    match tokio::time::timeout(Duration::from_secs(10), watch.next_event())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
    {
        WatchEvent::Snapshot { value, .. } => {
            assert_eq!(value, serde_json::json!({"title": "second"}))
        }
        WatchEvent::Reset { .. } => panic!("expected a snapshot"),
    }
}

#[tokio::test]
async fn subscribe_reconnects_after_restart() {
    use flower_client::watch::SubscribeOptions;
    let mut server = Restartable::start().await;
    let client = admin_client(&server.base);
    client
        .deploy(
            "db",
            "app",
            None,
            b"{}",
            &manifest(),
            &RequestOptions::default(),
        )
        .await
        .unwrap();
    let args = serde_json::json!({"key": k("a"), "title": "first"});
    client
        .mutate_value("db", "app", "put", &args, &MutationOptions::default())
        .await
        .unwrap();

    let args = serde_json::json!({"key": k("a")});
    let options = SubscribeOptions {
        retry: Retry::Policy(RetryPolicy {
            attempts: 100,
            initial_delay: Duration::from_millis(5),
            max_delay: Duration::from_millis(50),
            attempt_timeout: Duration::from_secs(5),
            ..RetryPolicy::default()
        }),
        stall_timeout: Duration::from_secs(5),
        ..SubscribeOptions::default()
    };
    let mut subscription = client.subscribe("db", "app", "get", &args, &options);
    let first: flower_client::watch::Update<serde_json::Value> =
        tokio::time::timeout(Duration::from_secs(10), subscription.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    assert!(first.reset);
    assert_eq!(first.value, serde_json::json!({"title": "first"}));

    server.restart().await;
    let second: flower_client::watch::Update<serde_json::Value> =
        tokio::time::timeout(Duration::from_secs(20), subscription.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    assert!(second.reset);
    assert_eq!(second.value, serde_json::json!({"title": "first"}));
}

#[tokio::test]
async fn wait_until_truthy_resolves() {
    use flower_client::watch::SubscribeOptions;
    let (_dir, served) = serve_node(true).await;
    let client = admin_client(&served.base);
    client
        .deploy(
            "db",
            "app",
            None,
            b"{}",
            &manifest(),
            &RequestOptions::default(),
        )
        .await
        .unwrap();

    let writer = client.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let args = serde_json::json!({"key": k("w"), "title": "late"});
        writer
            .mutate_value("db", "app", "put", &args, &MutationOptions::default())
            .await
            .unwrap();
    });
    let args = serde_json::json!({"key": k("w")});
    let update = tokio::time::timeout(
        Duration::from_secs(10),
        client.wait_until_truthy("db", "app", "get", &args, &SubscribeOptions::default()),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(update.value, serde_json::json!({"title": "late"}));
}

#[tokio::test]
async fn watch_unknown_definition_fails_clean() {
    use flower_client::watch::WatchOptions;
    let (_dir, served) = serve_node(true).await;
    let client = admin_client(&served.base);
    client
        .deploy(
            "db",
            "app",
            None,
            b"{}",
            &manifest(),
            &RequestOptions::default(),
        )
        .await
        .unwrap();
    let args = serde_json::json!({"key": k("a")});
    let error = client
        .watch_deltas("db", "app", "nope", &args, &WatchOptions::default())
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        FlowerError::Http(ref problem) if problem.code == "UNKNOWN_METHOD"
    ));
}

#[tokio::test]
async fn work_queue_roundtrip_via_client() {
    let server = Restartable::start().await;
    let client = admin_client(&server.base);
    let options = RequestOptions::default();
    // Seed one item straight into the leader's engine.
    let now = now_ms();
    {
        let engine = server.node.engine().lock().await;
        engine.enqueue(b"db", "jobs", &s("a"), 0, 5, now).unwrap();
    }
    let stats = client.queue_stats("db", "jobs", &options).await.unwrap();
    assert_eq!(
        stats,
        flower_client::work::QueueStats {
            pending: 1,
            claimed: 0,
            dead: 0,
            completed: 0,
        }
    );
    let claimed = client
        .poll_queue("db", "jobs", Some(10), Some(60_000), &options)
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].payload, serde_json::json!("a"));
    assert_eq!(claimed[0].attempts, 1);
    client
        .heartbeat_queue(
            "db",
            "jobs",
            &claimed[0].id,
            &claimed[0].token,
            Some(60_000),
            &options,
        )
        .await
        .unwrap();
    client
        .complete_queue("db", "jobs", &claimed[0].id, &claimed[0].token, &options)
        .await
        .unwrap();
    // Double completion is a business failure; the item reads as gone.
    let error = client
        .complete_queue("db", "jobs", &claimed[0].id, &claimed[0].token, &options)
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        FlowerError::Business { ref code, .. } if code == "INVALID_VALUE"
    ));
    let item = client
        .get_queue_item("db", "jobs", &claimed[0].id, &options)
        .await
        .unwrap();
    assert!(item.is_none());
    // Unknown items cancel as missing, not as errors.
    assert!(
        !client
            .cancel_queue("db", "jobs", &claimed[0].id, &options)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn work_queue_forwards_off_leader() {
    let (_dir, served) = serve_node(false).await;
    let client = admin_client(&served.base);
    let error = client
        .poll_queue("db", "jobs", None, None, &RequestOptions::default())
        .await
        .unwrap_err();
    assert!(matches!(error, FlowerError::Forward { .. }));
}

#[tokio::test]
async fn transaction_runs_via_client() {
    let server = Restartable::start().await;
    let client = admin_client(&server.base);
    client
        .deploy(
            "dba",
            "app",
            None,
            b"{}",
            &manifest(),
            &RequestOptions::default(),
        )
        .await
        .unwrap();
    client
        .deploy(
            "dbb",
            "app",
            None,
            b"{}",
            &manifest(),
            &RequestOptions::default(),
        )
        .await
        .unwrap();
    let steps = vec![
        TransactionStep {
            db: "dba".to_string(),
            module: "app".to_string(),
            method: "put".to_string(),
            args: serde_json::json!({"key": k("a"), "title": "first"}),
        },
        TransactionStep {
            db: "dbb".to_string(),
            module: "app".to_string(),
            method: "put".to_string(),
            args: serde_json::json!({"key": k("b"), "title": "second"}),
        },
    ];
    let committed = client
        .transaction(&steps, &TransactionOptions::new())
        .await
        .unwrap();
    assert_eq!(
        committed.values,
        vec![serde_json::json!("put-ok"), serde_json::json!("put-ok")]
    );
    assert_eq!(committed.revisions.get("dba"), Some(&1));
    assert_eq!(committed.revisions.get("dbb"), Some(&1));
    assert!(committed.position >= 1);
    let args = serde_json::json!({"key": k("b")});
    let read = client
        .query_value("dbb", "app", "get", &args, &RequestOptions::default())
        .await
        .unwrap();
    assert_eq!(read.value, serde_json::json!({"title": "second"}));
}

#[tokio::test]
async fn timer_inspect_and_maintain_via_client() {
    let server = Restartable::start().await;
    let client = admin_client(&server.base);
    let options = RequestOptions::default();
    let now = now_ms();
    {
        let engine = server.node.engine().lock().await;
        engine
            .schedule_timer(b"db", "t1", "app", "missing", &s("a"), 0, 1, now)
            .unwrap();
        engine
            .schedule_timer(b"db", "t2", "app", "missing", &s("b"), u64::MAX, 5, now)
            .unwrap();
    }
    let stats = client.timer_stats("db", &options).await.unwrap();
    assert_eq!(
        stats,
        flower_client::timer::TimerStats {
            pending: 2,
            dead: 0,
            held: 0
        }
    );
    let timers = client
        .list_timers("db", None, None, &options)
        .await
        .unwrap();
    assert_eq!(timers.len(), 2);
    assert_eq!(timers[0].id, "t1");
    assert_eq!(timers[0].args, serde_json::json!("a"));
    let timer = client
        .get_timer("db", "t1", &options)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(timer.handler, "missing");
    assert_eq!(timer.state, "pending");
    assert!(
        client
            .get_timer("db", "nope", &options)
            .await
            .unwrap()
            .is_none()
    );
    // Fire the one-shot through consensus: it dies.
    server.node.fire_timer(b"db", "t1").await.unwrap();
    let dead = client
        .list_timers("db", Some("dead"), None, &options)
        .await
        .unwrap();
    assert_eq!(dead.len(), 1);
    // Retry revives; cancel removes; purge finds nothing dead.
    client
        .retry_timer("db", "t1", None, &options)
        .await
        .unwrap();
    assert!(client.cancel_timer("db", "t1", &options).await.unwrap());
    assert!(!client.cancel_timer("db", "t1", &options).await.unwrap());
    assert_eq!(client.purge_timers("db", None, &options).await.unwrap(), 0);
}

// Scripted transports: canned replies and streams with call counters,
// for client behavior that needs no live server.

type PostScript = dyn Fn(usize) -> futures_util::future::BoxFuture<'static, Result<TransportResponse, FlowerError>>
    + Send
    + Sync;
type StreamScript = dyn Fn(usize) -> flower_client::transport::ByteStream + Send + Sync;

struct Scripted {
    posts: AtomicUsize,
    connects: AtomicUsize,
    on_post: Box<PostScript>,
    on_stream: Box<StreamScript>,
}

#[async_trait::async_trait]
impl Transport for Scripted {
    async fn post_with_headers(
        &self,
        path: &str,
        body: &[u8],
        credentials: Option<&Credentials>,
        _headers: &[(&'static str, String)],
    ) -> Result<(TransportResponse, flower_client::transport::Headers), FlowerError> {
        Ok((self.post(path, body, credentials).await?, vec![]))
    }

    async fn post(
        &self,
        _path: &str,
        _body: &[u8],
        _credentials: Option<&Credentials>,
    ) -> Result<TransportResponse, FlowerError> {
        let call = self.posts.fetch_add(1, Ordering::SeqCst);
        (self.on_post)(call).await
    }

    async fn get(
        &self,
        _path: &str,
        _credentials: Option<&Credentials>,
    ) -> Result<TransportResponse, FlowerError> {
        Err(FlowerError::Transport("no GET in this script".to_string()))
    }

    async fn post_stream(
        &self,
        _path: &str,
        _body: &[u8],
        _credentials: Option<&Credentials>,
    ) -> Result<flower_client::transport::StreamResponse, FlowerError> {
        let call = self.connects.fetch_add(1, Ordering::SeqCst);
        Ok(flower_client::transport::StreamResponse {
            status: 200,
            retry_after_ms: None,
            body: (self.on_stream)(call),
        })
    }
}

fn scripted<P, S>(on_post: P, on_stream: S) -> Arc<Scripted>
where
    P: Fn(
            usize,
        )
            -> futures_util::future::BoxFuture<'static, Result<TransportResponse, FlowerError>>
        + Send
        + Sync
        + 'static,
    S: Fn(usize) -> flower_client::transport::ByteStream + Send + Sync + 'static,
{
    Arc::new(Scripted {
        posts: AtomicUsize::new(0),
        connects: AtomicUsize::new(0),
        on_post: Box::new(on_post),
        on_stream: Box::new(on_stream),
    })
}

fn no_post(
    _call: usize,
) -> futures_util::future::BoxFuture<'static, Result<TransportResponse, FlowerError>> {
    Box::pin(async { Err(FlowerError::Transport("no POST in this script".to_string())) })
}

fn no_stream(_call: usize) -> flower_client::transport::ByteStream {
    Box::pin(futures_util::stream::empty())
}

fn reply(body: serde_json::Value) -> Result<TransportResponse, FlowerError> {
    Ok(TransportResponse {
        status: 200,
        retry_after_ms: None,
        body: serde_json::to_vec(&body).unwrap(),
    })
}

fn scripted_client(transport: Arc<Scripted>) -> FlowerClient {
    FlowerClient::builder("http://scripted.invalid")
        .transport(transport)
        .build()
        .unwrap()
}

fn fast_retry(attempts: u32, attempt_timeout: Duration) -> Retry {
    Retry::Policy(RetryPolicy {
        attempts,
        initial_delay: Duration::from_millis(1),
        max_delay: Duration::from_millis(5),
        attempt_timeout,
        ..RetryPolicy::default()
    })
}

fn sse(chunk: &str) -> Result<Vec<u8>, FlowerError> {
    Ok(chunk.as_bytes().to_vec())
}

#[tokio::test]
async fn retry_policy_attempt_timeout_bounds_each_attempt() {
    // The first attempt hangs forever; the policy's 100 ms attempt
    // timeout must cut it, not the client's 20 s default.
    let transport = scripted(
        |call| {
            Box::pin(async move {
                if call == 0 {
                    std::future::pending::<()>().await;
                }
                reply(serde_json::json!({"ok": true, "value": "second"}))
            })
        },
        no_stream,
    );
    let client = scripted_client(transport.clone());
    let options = RequestOptions::default().retry(fast_retry(2, Duration::from_millis(100)));
    let got = tokio::time::timeout(
        Duration::from_secs(10),
        client.query_value("db", "app", "get", &serde_json::json!({}), &options),
    )
    .await
    .expect("the hung attempt outlived the policy's attempt timeout")
    .unwrap();
    assert_eq!(got.value, serde_json::json!("second"));
    assert_eq!(transport.posts.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn stale_revision_fails_without_retry() {
    let transport = scripted(
        |_| {
            Box::pin(async {
                reply(serde_json::json!({
                    "ok": false,
                    "code": "stale_revision",
                    "message": "expected revision 1 but the database is at 2",
                    "revision": 2
                }))
            })
        },
        no_stream,
    );
    let client = scripted_client(transport.clone());
    let options = MutationOptions::new()
        .expected_revision(1)
        .retry(fast_retry(4, Duration::from_secs(5)));
    let error = client
        .mutate_value("db", "app", "put", &serde_json::json!({}), &options)
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        FlowerError::Business { ref code, revision: Some(2), .. } if code == "stale_revision"
    ));
    // The same expected revision can never succeed: one attempt only.
    assert_eq!(transport.posts.load(Ordering::SeqCst), 1);
}

/// Serve one canned JSON body after a delay, over raw HTTP/1.1.
async fn slow_http_server(delay: Duration, body: &'static str) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut request = Vec::new();
                let mut buf = [0u8; 4096];
                loop {
                    let read = socket.read(&mut buf).await.unwrap_or(0);
                    if read == 0 {
                        return;
                    }
                    request.extend_from_slice(&buf[..read]);
                    let Some(head) = request.windows(4).position(|w| w == b"\r\n\r\n") else {
                        continue;
                    };
                    let text = String::from_utf8_lossy(&request[..head]).to_ascii_lowercase();
                    let length = text
                        .lines()
                        .find_map(|line| line.strip_prefix("content-length:"))
                        .and_then(|value| value.trim().parse::<usize>().ok())
                        .unwrap_or(0);
                    if request.len() >= head + 4 + length {
                        break;
                    }
                }
                tokio::time::sleep(delay).await;
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = socket.write_all(response.as_bytes()).await;
                let _ = socket.shutdown().await;
            });
        }
    });
    base
}

#[tokio::test]
async fn per_call_timeout_outlives_client_default() {
    let base = slow_http_server(Duration::from_millis(600), r#"{"ok":true,"value":"slow"}"#).await;
    let client = FlowerClient::builder(&base)
        .transport_options(
            flower_client::TransportOptions::new()
                .protocol(flower_client::transport::Protocol::Http1),
        )
        .timeout(Duration::from_millis(200))
        .build()
        .unwrap();
    // A longer per-call timeout wins over the 200 ms client default.
    let options = RequestOptions::default().timeout(Duration::from_secs(10));
    let got = client
        .query_value("db", "app", "get", &serde_json::json!({}), &options)
        .await
        .unwrap();
    assert_eq!(got.value, serde_json::json!("slow"));
    // The default still bounds calls without an override.
    let error = client
        .query_value(
            "db",
            "app",
            "get",
            &serde_json::json!({}),
            &RequestOptions::default(),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, FlowerError::Timeout));
}

#[tokio::test]
async fn backup_via_client_reports_summary() {
    let dir = tempfile::tempdir().unwrap();
    let node = open_leader(dir.path()).await;
    let driver = flower_backup::driver::Driver::memory(
        &dir.path().join("keystore.json"),
        [7u8; 32],
        node.engine().clone(),
        node.logs().clone(),
        {
            let device = device_builder();
            Arc::new(
                move |_: &[u8], module: &str, wasm: &[u8], manifest: &Value| {
                    device(module, wasm, manifest)
                },
            )
        },
    )
    .unwrap();
    let state = AppState::new(node)
        .with_admin_token("secret".to_string())
        .with_backup(Arc::new(driver));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(serve(listener, state));
    let client = admin_client(&base);
    client
        .deploy(
            "db",
            "app",
            None,
            b"{}",
            &manifest(),
            &RequestOptions::default(),
        )
        .await
        .unwrap();
    let args = serde_json::json!({"key": k("a"), "title": "kept"});
    let put = client
        .mutate_value("db", "app", "put", &args, &MutationOptions::default())
        .await
        .unwrap();
    let outcome = client.backup(&RequestOptions::default()).await.unwrap();
    assert_eq!(outcome.backup_id.len(), 32);
    assert_eq!(outcome.objects, 1);
    assert!(outcome.covered_index >= put.position);
}

/// Pull the batch a fresh device would receive.
async fn pull_batch(base: &str, since: u64) -> flower_client::sync::WireBatch {
    let transport = HttpTransport::new(base, Duration::from_secs(20)).unwrap();
    let response = transport
        .get(
            &format!("/v1/sync?since={since}&limit=64"),
            Some(&Credentials::token("secret")),
        )
        .await
        .unwrap();
    assert_eq!(response.status, 200);
    serde_json::from_slice(&response.body).unwrap()
}

async fn seed_rows(client: &FlowerClient, rows: &[(&str, &str)]) {
    client
        .deploy(
            "db",
            "app",
            None,
            b"{}",
            &manifest(),
            &RequestOptions::default(),
        )
        .await
        .unwrap();
    for (key, title) in rows {
        let args = serde_json::json!({"key": k(key), "title": title});
        client
            .mutate_value("db", "app", "put", &args, &MutationOptions::default())
            .await
            .unwrap();
    }
}

async fn device_get(engine: &Arc<tokio::sync::Mutex<Engine>>, key: &str) -> Value {
    let engine = engine.lock().await;
    let args = obj(vec![("key", s(&k(key)))]);
    engine
        .invoke_query(b"db", "app", "get", &args, &CallOptions::default())
        .unwrap()
}

#[tokio::test]
async fn sync_resumes_after_mid_batch_failure() {
    use flower_client::sync::WireWrite;
    let (dir, served) = serve_node(true).await;
    let client = admin_client(&served.base);
    seed_rows(&client, &[("a", "first"), ("b", "second"), ("c", "third")]).await;

    // Corrupt the batch's last write: entries before it apply, then
    // the batch fails.
    let mut batch = pull_batch(&served.base, 0).await;
    let broken = batch
        .entries
        .iter()
        .rposition(|entry| matches!(entry.write, WireWrite::Batch { .. }))
        .unwrap();
    assert!(broken >= 2);
    let kept = batch.entries[broken - 1].index;
    if let WireWrite::Batch { items, .. } = &mut batch.entries[broken].write
        && let Some(flower_engine::batch::BatchItem::Patch { patch, .. }) = items.first_mut()
    {
        *patch = vec![0xff];
    }
    let engine = device_engine(dir.path(), "device.redb");
    let sync = sync_client(&served.base, engine.clone(), device_builder());
    assert!(sync.apply_batch(&batch).await.is_err());
    // The position records exactly what applied.
    assert_eq!(sync.position().await.unwrap(), kept);
    assert_eq!(
        device_get(&engine, "b").await,
        obj(vec![("title", s("second"))])
    );
    assert_eq!(device_get(&engine, "c").await, Value::Null);

    // The next pull resumes at the failed entry instead of replaying
    // entries the device already holds.
    let progress = sync.sync_catch_up(64).await.unwrap();
    assert_eq!(progress.current, batch.current);
    assert_eq!(
        device_get(&engine, "c").await,
        obj(vec![("title", s("third"))])
    );
}

#[tokio::test]
async fn sync_refuses_out_of_order_batch() {
    let (dir, served) = serve_node(true).await;
    let client = admin_client(&served.base);
    seed_rows(&client, &[("a", "first"), ("b", "second")]).await;
    let mut batch = pull_batch(&served.base, 0).await;
    let last = batch.entries.len() - 1;
    batch.entries.swap(last - 1, last);

    let engine = device_engine(dir.path(), "device.redb");
    let sync = sync_client(&served.base, engine.clone(), device_builder());
    let error = sync.apply_batch(&batch).await.unwrap_err();
    assert!(matches!(error, FlowerError::Decode(_)), "{error:?}");
    // Nothing from the refused batch applied.
    assert_eq!(sync.position().await.unwrap(), 0);
    let engine = engine.lock().await;
    let read = engine.store().begin_read().unwrap();
    assert_eq!(
        read.meta_u64(b"db", flower_engine::storage::meta::REVISION)
            .unwrap(),
        0
    );
    assert!(
        read.meta(b"db", &flower_engine::staging::code_identity_key("app"))
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn sync_resumes_interrupted_snapshot_install() {
    use base64::Engine as _;
    use std::sync::atomic::AtomicBool;
    let (dir, served) = serve_node(true).await;
    let client = admin_client(&served.base);
    seed_rows(&client, &[("a", "snap")]).await;

    // A replica in sync supplies the snapshot views.
    let source = device_engine(dir.path(), "source.redb");
    let reached = sync_client(&served.base, source.clone(), device_builder())
        .sync_catch_up(64)
        .await
        .unwrap()
        .current;
    let file = {
        let source = source.lock().await;
        let (meta, payload) =
            flower_engine::snapshot::encode_snapshot(source.store(), b"db").unwrap();
        serde_json::json!({
            "version": 1,
            "dbs": [{
                "meta": {
                    "db": meta.db,
                    "revision": meta.revision,
                    "applied_log": meta.applied_log,
                    "incarnation": meta.incarnation
                },
                "payload": payload
            }]
        })
    };
    let encoded = base64::prelude::BASE64_STANDARD.encode(serde_json::to_vec(&file).unwrap());
    let batch: flower_client::sync::WireBatch = serde_json::from_value(serde_json::json!({
        "version": 1,
        "since": 0,
        "incarnation": 0,
        "entries": [],
        "current": reached,
        "snapshot": {"last_index": reached, "bytes_b64": encoded}
    }))
    .unwrap();

    // The install fails after the view landed: runtimes cannot load.
    let unavailable = Arc::new(AtomicBool::new(true));
    let gate = unavailable.clone();
    let inner = device_builder();
    let builder: flower_client::sync::DeviceRuntimeBuilder =
        Arc::new(move |module: &str, wasm: &[u8], manifest: &Value| {
            if gate.load(Ordering::SeqCst) {
                return Err("runtime unavailable".to_string());
            }
            inner(module, wasm, manifest)
        });
    let device = device_engine(dir.path(), "device.redb");
    let replica = sync_client(&served.base, device.clone(), builder);
    assert!(replica.apply_batch(&batch).await.is_err());
    assert_eq!(replica.position().await.unwrap(), 0);

    // Whatever arrives next, the interrupted install finishes first.
    unavailable.store(false, Ordering::SeqCst);
    let empty: flower_client::sync::WireBatch = serde_json::from_value(serde_json::json!({
        "version": 1,
        "since": 0,
        "incarnation": 0,
        "entries": [],
        "current": 0,
        "snapshot": null
    }))
    .unwrap();
    let progress = replica.apply_batch(&empty).await.unwrap();
    assert_eq!(progress.current, reached);
    assert_eq!(replica.position().await.unwrap(), reached);
    assert_eq!(
        device_get(&device, "a").await,
        obj(vec![("title", s("snap"))])
    );
}

#[tokio::test]
async fn acknowledged_intent_previews_until_receipt_syncs() {
    use flower_engine::intent::IntentStatus;
    use flower_engine::preview::default_preview_options;
    let (dir, served) = serve_node(true).await;
    let client = admin_client(&served.base);
    seed_rows(&client, &[]).await;
    let engine = device_engine(dir.path(), "device.redb");
    let sync = sync_client(&served.base, engine.clone(), device_builder());
    sync.sync_catch_up(64).await.unwrap();

    let queue = IntentQueue::new(client.clone(), engine.clone());
    initialize_queue(&served, &client, &queue).await;
    let args = obj(vec![("key", s(&k("p"))), ("title", s("pending"))]);
    queue
        .enqueue(b"db", "app", "put", &args, None)
        .await
        .unwrap();
    assert_eq!(queue.submit_pending(8).await.unwrap().submitted, 1);

    let read_args = obj(vec![("key", s(&k("p")))]);
    let identity = {
        let engine = engine.lock().await;
        let intents = engine.list_intents(None).unwrap();
        assert_eq!(intents[0].status, IntentStatus::Acknowledged);
        // Not synced yet: committed reads lack the row, while the
        // preview keeps showing it instead of flickering it out.
        let committed = engine
            .invoke_query(b"db", "app", "get", &read_args, &CallOptions::default())
            .unwrap();
        assert_eq!(committed, Value::Null);
        let preview = engine.preview(b"db", &default_preview_options()).unwrap();
        assert_eq!(preview.previewed, vec![intents[0].identity.clone()]);
        let seen = engine
            .invoke_query_preview(
                b"db",
                "app",
                "get",
                &read_args,
                &preview.overlay,
                &CallOptions::default(),
            )
            .unwrap();
        assert_eq!(seen, obj(vec![("title", s("pending"))]));
        intents[0].identity.clone()
    };

    // The receipt arrives with sync-down: committed at the synced
    // revision, out of the preview, present in committed state.
    sync.sync_catch_up(64).await.unwrap();
    assert_eq!(
        device_get(&engine, "p").await,
        obj(vec![("title", s("pending"))])
    );
    let engine = engine.lock().await;
    let revision = engine
        .store()
        .begin_read()
        .unwrap()
        .meta_u64(b"db", flower_engine::storage::meta::REVISION)
        .unwrap();
    assert_eq!(
        engine.get_intent(&identity).unwrap().unwrap().status,
        IntentStatus::Committed { revision }
    );
    assert!(
        engine
            .preview(b"db", &default_preview_options())
            .unwrap()
            .previewed
            .is_empty()
    );
}

#[tokio::test]
async fn intent_queue_stops_at_first_unknown_outcome() {
    use flower_engine::intent::IntentStatus;
    let dir = tempfile::tempdir().unwrap();
    let transport = scripted(
        |call| {
            Box::pin(async move {
                if call == 0 {
                    return Err(FlowerError::Transport("connection reset".to_string()));
                }
                reply(
                    serde_json::json!({"ok": true, "value": "put-ok", "revision": 1, "position": 3}),
                )
            })
        },
        no_stream,
    );
    let engine = device_engine(dir.path(), "queue.redb");
    let queue = IntentQueue::new(scripted_client(transport.clone()), engine.clone());
    queue
        .bind_identity(
            b"db",
            &flower_client::RetryIdentity {
                incarnation: 9,
                epoch: 0,
                min_epoch: 0,
                epoch_ms: None,
                initialized: true,
            },
        )
        .await
        .unwrap();
    for key in ["first", "second"] {
        let args = obj(vec![("key", s(&k(key))), ("title", s(key))]);
        queue
            .enqueue(b"db", "app", "put", &args, None)
            .await
            .unwrap();
    }
    let summary = queue.submit_pending(8).await.unwrap();
    // The first intent's outcome is unknown, so the second must not
    // commit ahead of it.
    assert_eq!(transport.posts.load(Ordering::SeqCst), 1);
    assert_eq!(
        summary,
        flower_client::queue::SubmitSummary {
            submitted: 0,
            failed_business: 0,
            still_pending: 2
        }
    );
    let statuses: Vec<IntentStatus> = engine
        .lock()
        .await
        .list_intents(None)
        .unwrap()
        .into_iter()
        .map(|intent| intent.status)
        .collect();
    assert_eq!(
        statuses,
        vec![IntentStatus::Submitted, IntentStatus::Queued]
    );
}

#[tokio::test]
async fn subscription_heartbeats_keep_idle_watch_alive() {
    use flower_client::watch::{SubscribeOptions, Update};
    // One snapshot, 1.2 s of heartbeat comments, then a second
    // snapshot: far past the 400 ms stall timeout, but never silent.
    let transport = scripted(no_post, |connect| {
        let first = format!("event: snapshot\ndata: {{\"revision\":1,\"value\":{connect}}}\n\n");
        Box::pin(futures_util::stream::unfold(0u32, move |step| {
            let first = first.clone();
            async move {
                match step {
                    0 => Some((sse(&first), 1)),
                    1..=60 => {
                        tokio::time::sleep(Duration::from_millis(20)).await;
                        Some((sse(":keepalive\n\n"), step + 1))
                    }
                    61 => Some((
                        sse("event: snapshot\ndata: {\"revision\":2,\"value\":99}\n\n"),
                        62,
                    )),
                    _ => std::future::pending().await,
                }
            }
        }))
    });
    let client = scripted_client(transport.clone());
    let options = SubscribeOptions {
        retry: fast_retry(100, Duration::from_secs(5)),
        stall_timeout: Duration::from_millis(400),
        ..SubscribeOptions::default()
    };
    let mut subscription = client.subscribe("db", "app", "get", &serde_json::json!({}), &options);
    let first: Update<serde_json::Value> =
        tokio::time::timeout(Duration::from_secs(10), subscription.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    assert_eq!((first.revision, first.reset), (1, true));
    let second = tokio::time::timeout(Duration::from_secs(10), subscription.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!((second.revision, second.reset), (2, false));
    assert_eq!(second.value, serde_json::json!(99));
    assert_eq!(transport.connects.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn subscription_reconnect_budget_restarts_after_data() {
    use flower_client::watch::SubscribeOptions;
    // Every connection delivers one snapshot, then closes.
    let transport = scripted(no_post, |connect| {
        let event =
            format!("event: snapshot\ndata: {{\"revision\":{connect},\"value\":{connect}}}\n\n");
        Box::pin(futures_util::stream::iter(vec![sse(&event)]))
    });
    let client = scripted_client(transport);
    let options = SubscribeOptions {
        retry: fast_retry(100, Duration::from_secs(5)),
        max_reconnects: Some(1),
        ..SubscribeOptions::default()
    };
    let mut subscription =
        client.subscribe::<serde_json::Value>("db", "app", "get", &serde_json::json!({}), &options);
    // Healthy connections never exhaust a one-reconnect budget.
    for expected in 0..4u64 {
        let update = tokio::time::timeout(Duration::from_secs(10), subscription.next())
            .await
            .unwrap()
            .expect("subscription ended")
            .unwrap();
        assert_eq!((update.revision, update.reset), (expected, true));
    }
}

#[tokio::test]
async fn watch_dispatches_crlf_and_cr_framed_events() {
    use flower_client::watch::{WatchEvent, WatchOptions};
    // CRLF framing, then lone-CR framing; the stream stays open.
    let transport = scripted(no_post, |_| {
        Box::pin(futures_util::StreamExt::chain(
            futures_util::stream::iter(vec![
                sse("event: snapshot\r\ndata: {\"revision\":1,\"value\":\"one\"}\r\n\r\n"),
                sse("event: snapshot\rdata: {\"revision\":2,\"value\":\"two\"}\r\r"),
            ]),
            futures_util::stream::pending(),
        ))
    });
    let client = scripted_client(transport);
    let mut watch = client
        .watch_deltas(
            "db",
            "app",
            "get",
            &serde_json::json!({}),
            &WatchOptions::default(),
        )
        .await
        .unwrap();
    for expected in [1u64, 2] {
        let event = tokio::time::timeout(Duration::from_secs(5), watch.next_event())
            .await
            .expect("event never dispatched")
            .unwrap()
            .unwrap();
        assert!(
            matches!(event, WatchEvent::Snapshot { revision, .. } if revision == expected),
            "{event:?}"
        );
    }
}

/// Serve a fresh single-node cluster from `dir` with the app deployed
/// and `rows` put; its incarnation is established first when asked,
/// as maintenance does after bootstrap.
async fn serve_history(
    dir: &std::path::Path,
    establish: bool,
    rows: &[(&str, &str)],
) -> (Arc<Node>, String) {
    let node = open_leader(dir).await;
    if establish {
        assert_ne!(node.establish_incarnation().await.unwrap(), 0);
    }
    let state = AppState::new(node.clone()).with_admin_token("secret".to_string());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(serve(listener, state));
    seed_rows(&admin_client(&base), rows).await;
    (node, base)
}

#[tokio::test]
async fn sync_refuses_a_batch_from_another_history() {
    let dir = tempfile::tempdir().unwrap();
    // The device follows history A from before A has an incarnation,
    // and joins it when the incarnation entry arrives.
    let (a, a_base) = serve_history(&dir.path().join("a"), false, &[("a", "first")]).await;
    let engine = device_engine(dir.path(), "device.redb");
    let device = sync_client(&a_base, engine.clone(), device_builder());
    device.sync_catch_up(64).await.unwrap();
    assert_eq!(device.incarnation().await.unwrap(), 0);
    let incarnation = a.establish_incarnation().await.unwrap();
    device.sync_catch_up(64).await.unwrap();
    assert_eq!(device.incarnation().await.unwrap(), incarnation);
    let position = device.position().await.unwrap();

    // History B, a restored or re-bootstrapped cluster, restarted its
    // positions and runs past the device's.
    let rows = [("b", "x"), ("c", "x"), ("d", "x"), ("e", "x"), ("f", "x")];
    let (b, b_base) = serve_history(&dir.path().join("b"), true, &rows).await;
    assert!(b.status().last_applied.unwrap() > position);
    let error = sync_client(&b_base, engine.clone(), device_builder())
        .sync_once(64)
        .await
        .unwrap_err();
    assert!(
        matches!(
            error,
            FlowerError::HistoryChanged { local, remote }
                if local == incarnation && remote == b.incarnation()
        ),
        "{error:?}"
    );
    // Nothing from B applied.
    assert_eq!(device.position().await.unwrap(), position);
    assert_eq!(device_get(&engine, "f").await, Value::Null);
}

#[tokio::test]
async fn sync_waits_while_the_server_has_no_incarnation() {
    let dir = tempfile::tempdir().unwrap();
    // A device at position zero adopts A's incarnation from the batch.
    let (a, a_base) = serve_history(&dir.path().join("a"), true, &[("a", "first")]).await;
    let engine = device_engine(dir.path(), "device.redb");
    let device = sync_client(&a_base, engine.clone(), device_builder());
    device.sync_catch_up(64).await.unwrap();
    assert_eq!(device.incarnation().await.unwrap(), a.incarnation());
    let position = device.position().await.unwrap();

    // A restored cluster serving before its leader proposes a new
    // incarnation: its entries may belong to any history, so none
    // apply until it has one.
    let rows = [("b", "x"), ("c", "x"), ("d", "x"), ("e", "x")];
    let (c, c_base) = serve_history(&dir.path().join("c"), false, &rows).await;
    assert!(c.status().last_applied.unwrap() > position);
    let progress = sync_client(&c_base, engine.clone(), device_builder())
        .sync_once(64)
        .await
        .unwrap();
    assert_eq!(
        progress,
        flower_client::SyncProgress {
            applied: 0,
            current: position,
            snapshot: false,
        }
    );
    assert_eq!(device.position().await.unwrap(), position);
    assert_eq!(device_get(&engine, "e").await, Value::Null);
}

/// Stamp a replicated patch with a chosen proposal time.
fn stamped(
    write: flower_cluster::types::ClusterWrite,
    stamp: u64,
) -> flower_cluster::types::ClusterWrite {
    use flower_cluster::types::ClusterWrite;
    match write {
        ClusterWrite::Patch { db, patch, .. } => ClusterWrite::Patch {
            db,
            patch,
            proposed_ms: stamp,
        },
        other => other,
    }
}

/// Prepare a put under idempotency key `key-1` on a server engine,
/// called at `timestamp_ms` (now when `None`), which sets its
/// receipt's expiry.
async fn keyed_put(
    server: &Arc<tokio::sync::Mutex<Engine>>,
    timestamp_ms: Option<u64>,
) -> flower_cluster::types::ClusterWrite {
    use flower_engine::engine::PrepareOutcome;
    let options = flower_engine::engine::MutationOptions {
        call: CallOptions {
            timestamp_ms,
            ..CallOptions::default()
        },
        idempotency_key: Some(b"key-1".to_vec()),
        ..flower_engine::engine::MutationOptions::default()
    };
    let args = obj(vec![("key", s(&k("a"))), ("title", s("first"))]);
    let prepared = server
        .lock()
        .await
        .prepare_mutation(b"db", "app", "put", &args, &options)
        .unwrap();
    match prepared {
        PrepareOutcome::Patch(prepared) => flower_cluster::types::ClusterWrite::Patch {
            db: b"db".to_vec(),
            patch: prepared.patch,
            proposed_ms: 0,
        },
        other => panic!("expected a patch, got {other:?}"),
    }
}

#[tokio::test]
async fn sync_reads_receipt_expiry_at_the_server_clock() {
    use flower_cluster::types::{ClusterWrite, TypeConfig, WriteResponse};
    use flower_engine::retry::IDEMPOTENCY_TTL_MS;
    use openraft::storage::RaftStateMachine;
    use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId};
    let dir = tempfile::tempdir().unwrap();
    // A server state machine applying entries under chosen stamps.
    let store = GroupStore::create(dir.path().join("group.redb")).unwrap();
    let server = Arc::new(tokio::sync::Mutex::new(Engine::new(store)));
    let logs = flower_cluster::logstore::LogStore::open(&dir.path().join("raft.redb")).unwrap();
    let (commits, _) = tokio::sync::broadcast::channel(8);
    let factory = Arc::new(TestFactory {
        handler: app_handler(),
    });
    let mut machine = StateMachine::new(server.clone(), logs, factory, commits);
    let mut writes: Vec<ClusterWrite> = Vec::new();
    let mut apply = async |write: ClusterWrite| -> WriteResponse {
        writes.push(write.clone());
        let entry = Entry::<TypeConfig> {
            log_id: LogId::new(CommittedLeaderId::new(1, 1), writes.len() as u64),
            payload: EntryPayload::Normal(write),
        };
        machine.apply(vec![entry]).await.unwrap().remove(0)
    };
    let manifest =
        flower_engine::json::parse_value(&serde_json::to_vec(&manifest()).unwrap()).unwrap();
    let bindings = {
        let mut engine = server.lock().await;
        engine
            .deploy_staged(b"db", "app", [9u8; 32], b"wasm", &manifest)
            .unwrap();
        while !engine.staging_drain(b"db", "app", 4096).unwrap().done {}
        engine.prepare_activation(b"db", "app").unwrap().bindings
    };
    let now = now_ms();
    apply(ClusterWrite::Deploy {
        db: b"db".to_vec(),
        module: "app".to_string(),
        wasm: b"wasm".to_vec(),
        manifest: flower_engine::json::render(&manifest).unwrap(),
        identity: [9u8; 32],
        bindings,
        proposed_ms: now,
    })
    .await;
    // A keyed write whose receipt expired an hour ago by its call time
    // (stamped a second before the deployment, so the deployment holds
    // the clock), then its redelivery stamped by a leader whose clock
    // stepped back before that expiry: the server reads expiry at its
    // clock, already past it, and executes the redelivery afresh.
    let old = now - IDEMPOTENCY_TTL_MS - 3_600_000;
    let first = keyed_put(&server, Some(old)).await;
    assert_eq!(apply(stamped(first, now - 1_000)).await.revision, 1);
    let again = keyed_put(&server, None).await;
    assert_eq!(apply(stamped(again, old + 1)).await.revision, 2);
    let clock = StateMachine::expiry_clock(&*server.lock().await).unwrap();
    assert_eq!(clock, now);

    // The device applies the same entries exactly as the server ships
    // them, and reaches the same clock and verdict.
    let entries: Vec<serde_json::Value> = writes
        .iter()
        .enumerate()
        .map(|(offset, write)| serde_json::json!({"index": offset + 1, "write": write}))
        .collect();
    let batch: flower_client::sync::WireBatch = serde_json::from_value(serde_json::json!({
        "version": 1,
        "since": 0,
        "incarnation": 0,
        "entries": entries,
        "current": writes.len(),
        "snapshot": null
    }))
    .unwrap();
    let device = device_engine(dir.path(), "device.redb");
    sync_client("http://127.0.0.1:9", device.clone(), device_builder())
        .apply_batch(&batch)
        .await
        .unwrap();
    let device = device.lock().await;
    let read = device.store().begin_read().unwrap();
    assert_eq!(
        read.meta_u64(b"$cluster", b"group_expiry_clock").unwrap(),
        clock
    );
    assert_eq!(
        read.meta_u64(b"db", flower_engine::storage::meta::REVISION)
            .unwrap(),
        2
    );
}

/// A key catalog change reaches devices emptied of material, and moves
/// their group head and expiry clock as it does on members, so the next
/// batch is fresh or stale on a device exactly when it is on members.
#[tokio::test]
async fn sync_moves_the_head_past_key_changes() {
    use flower_cluster::types::ClusterWrite;
    use flower_engine::batch::Head;
    let dir = tempfile::tempdir().unwrap();
    let keys = ClusterWrite::Keys {
        db: b"db".to_vec(),
        change: Vec::new(),
        proposed_ms: 5_000,
    };
    let batch: flower_client::sync::WireBatch = serde_json::from_value(serde_json::json!({
        "version": 1,
        "since": 0,
        "incarnation": 0,
        "entries": [{"index": 1, "write": keys}],
        "current": 1,
        "snapshot": null
    }))
    .unwrap();
    let device = device_engine(dir.path(), "device.redb");
    sync_client("http://127.0.0.1:9", device.clone(), device_builder())
        .apply_batch(&batch)
        .await
        .unwrap();
    let device = device.lock().await;
    let read = device.store().begin_read().unwrap();
    assert_eq!(Head::read(&read).unwrap(), Head::Entry(1));
    assert_eq!(
        read.meta_u64(b"$cluster", b"group_expiry_clock").unwrap(),
        5_000
    );
}

/// Devices hold derived values members computed from managed keys, yet
/// receive key changes emptied of material: applying one drops every
/// derived value that resolved a key, as members drop them.
#[tokio::test]
async fn sync_drops_derived_values_that_resolved_a_key() {
    use flower_cluster::types::ClusterWrite;
    let dir = tempfile::tempdir().unwrap();
    let device = device_engine(dir.path(), "device.redb");
    // `stamp` resolves a key and answers how many times it evaluated.
    let evaluations = Arc::new(AtomicUsize::new(0));
    let counted = evaluations.clone();
    let runtime = ClosureRuntime::new(
        move |kind: CallKind, _module: &str, name: &str, _args: &Value, host: &mut dyn HostCall| {
            match (kind, name) {
                (CallKind::Derived, "stamp") => {
                    host.read_keys();
                    Ok(Value::Number(
                        (counted.fetch_add(1, Ordering::SeqCst) + 1) as f64,
                    ))
                }
                (CallKind::Query, "read") => hc(
                    host,
                    HostOp::GetDerived,
                    vec![s("app"), s("stamp"), obj(vec![])],
                ),
                _ => Err(fail("UNKNOWN_METHOD", name.to_string())),
            }
        },
    );
    let manifest = flower_engine::json::parse_value(
        serde_json::json!({
            "collections": [],
            "definitions": [
                {"name": "stamp", "kind": "derived", "args": {"type": "json"}},
                {"name": "read", "kind": "query", "args": {"type": "json"}},
            ],
            "triggers": [],
            "http": [],
        })
        .to_string()
        .as_bytes(),
    )
    .unwrap();
    device
        .lock()
        .await
        .deploy(b"db", "app", [4u8; 32], b"", &manifest, Box::new(runtime))
        .unwrap();
    let read = |engine: &Engine| {
        engine
            .reader()
            .invoke_query(b"db", "app", "read", &obj(vec![]), &CallOptions::default())
            .unwrap()
    };
    assert_eq!(read(&*device.lock().await), Value::Number(1.0));
    assert_eq!(read(&*device.lock().await), Value::Number(1.0));
    let keys = ClusterWrite::Keys {
        db: b"db".to_vec(),
        change: Vec::new(),
        proposed_ms: 5_000,
    };
    let batch: flower_client::sync::WireBatch = serde_json::from_value(serde_json::json!({
        "version": 1,
        "since": 0,
        "incarnation": 0,
        "entries": [{"index": 1, "write": keys}],
        "current": 1,
        "snapshot": null
    }))
    .unwrap();
    sync_client("http://127.0.0.1:9", device.clone(), device_builder())
        .apply_batch(&batch)
        .await
        .unwrap();
    assert_eq!(read(&*device.lock().await), Value::Number(2.0));
}

#[tokio::test]
async fn sync_adopts_the_server_incarnation_with_its_first_write() {
    use base64::Engine as _;
    let (dir, served) = serve_node(true).await;
    let client = admin_client(&served.base);
    seed_rows(&client, &[("a", "first")]).await;
    let mut batch = pull_batch(&served.base, 0).await;
    assert_eq!(batch.incarnation, 0);

    // A device at position zero adopts the incarnation a batch reports
    // with its first write, whether or not the incarnation entry ships.
    batch.incarnation = 7;
    let fresh = device_engine(dir.path(), "fresh.redb");
    let replica = sync_client(&served.base, fresh.clone(), device_builder());
    let reached = replica.apply_batch(&batch).await.unwrap().current;
    assert_eq!(replica.incarnation().await.unwrap(), 7);

    // A device past position zero that follows none may hold another
    // history, so an entry batch's incarnation is not adopted; a
    // snapshot replaces whatever it held, so that one is.
    let device = device_engine(dir.path(), "device.redb");
    let replica = sync_client(&served.base, device.clone(), device_builder());
    batch.incarnation = 0;
    replica.apply_batch(&batch).await.unwrap();
    batch.incarnation = 9;
    batch.entries.clear();
    batch.since = reached;
    replica.apply_batch(&batch).await.unwrap();
    assert_eq!(replica.incarnation().await.unwrap(), 0);
    let file = {
        let fresh = fresh.lock().await;
        let (meta, payload) =
            flower_engine::snapshot::encode_snapshot(fresh.store(), b"db").unwrap();
        serde_json::json!({
            "version": 1,
            "dbs": [{
                "meta": {
                    "db": meta.db,
                    "revision": meta.revision,
                    "applied_log": meta.applied_log,
                    "incarnation": meta.incarnation
                },
                "payload": payload
            }]
        })
    };
    let encoded = base64::prelude::BASE64_STANDARD.encode(serde_json::to_vec(&file).unwrap());
    let snapshot: flower_client::sync::WireBatch = serde_json::from_value(serde_json::json!({
        "version": 1,
        "since": 0,
        "incarnation": 0,
        "entries": [],
        "current": reached,
        "snapshot": {"last_index": reached, "bytes_b64": encoded},
        "incarnation": 9,
        "since": reached
    }))
    .unwrap();
    replica.apply_batch(&snapshot).await.unwrap();
    assert_eq!(replica.incarnation().await.unwrap(), 9);
}

/// The business failure code a call answered.
fn business_code(result: Result<impl std::fmt::Debug, FlowerError>) -> String {
    match result {
        Err(FlowerError::Business { code, .. }) => code,
        other => panic!("not a business failure: {other:?}"),
    }
}

/// Scoped request IDs over HTTP: minted from the server's identity,
/// replayed while admissible, refused once the operator advanced the
/// floor past them; and a retry session's requests, acknowledged and
/// closed.
#[tokio::test]
async fn scoped_request_ids_and_retry_sessions_over_http() {
    let (_dir, served) = serve_node(true).await;
    let client = admin_client(&served.base);
    seed_rows(&client, &[]).await;
    let options = RequestOptions::default();
    let rotate = serde_json::json!({"operation": "rotate", "keep_epochs": 2});
    client
        .control_retention("db", rotate, &options)
        .await
        .unwrap();
    let identity = client.retry_identity("db", &options).await.unwrap();
    assert!(identity.initialized);
    assert_eq!((identity.epoch, identity.min_epoch), (0, 0));
    let put = |title: &str| serde_json::json!({"key": k("a"), "title": title});
    let order = MutationOptions::new().request_id(identity.request_id(b"order-1"));
    let first = client
        .mutate_value("db", "app", "put", &put("first"), &order)
        .await
        .unwrap();
    let again = client
        .mutate_value("db", "app", "put", &put("first"), &order)
        .await
        .unwrap();
    assert_eq!(again.revision, first.revision, "the retry replayed");
    // The same ID for another intent is a conflict, not a new request.
    let reused = client
        .mutate_value("db", "app", "put", &put("other"), &order)
        .await;
    assert_eq!(business_code(reused), "identity_conflict");
    let advance = serde_json::json!({
        "operation": "advance", "from_epoch": 0, "current_epoch": 1, "min_epoch": 1,
    });
    client
        .control_retention("db", advance, &options)
        .await
        .unwrap();
    let retired = client
        .mutate_value("db", "app", "put", &put("first"), &order)
        .await;
    assert_eq!(business_code(retired), "epoch_retired");

    let identity = client.retry_identity("db", &options).await.unwrap();
    assert_eq!(identity.epoch, 1);
    let session = client
        .open_retry_session("db", "app", [7; 16], &options)
        .await
        .unwrap();
    assert_eq!((session.epoch, session.acknowledged), (1, 0));
    let first = MutationOptions::new().request_id(session.request_id(1));
    client
        .mutate_value("db", "app", "put", &put("session"), &first)
        .await
        .unwrap();
    let acked = client
        .acknowledge_retry_session(
            "db",
            "app",
            [7; 16],
            flower_client::retention::Acknowledgement::through(1),
            &options,
        )
        .await
        .unwrap();
    assert_eq!(acked.acknowledged, 1);
    let consumed = client
        .mutate_value("db", "app", "put", &put("session"), &first)
        .await;
    assert_eq!(business_code(consumed), "already_acknowledged");
    let closed = client
        .close_retry_session("db", "app", [7; 16], &options)
        .await
        .unwrap();
    assert!(closed.closed);
    let status = client
        .retry_session_status("db", "app", [7; 16], &options)
        .await
        .unwrap();
    assert_eq!(status, closed);
    let after = MutationOptions::new().request_id(session.request_id(2));
    let refused = client
        .mutate_value("db", "app", "put", &put("late"), &after)
        .await;
    assert_eq!(business_code(refused), "retry_session_closed");
}

#[tokio::test]
async fn persisted_offline_commands_replay_and_retired_history_requires_resolution() {
    use flower_client::Execution;
    use flower_engine::intent::IntentStatus;
    use flower_sdk::MutationOp;
    let (dir, served) = serve_node(true).await;
    let client = admin_client(&served.base);
    seed_rows(&client, &[]).await;
    let engine = device_engine(dir.path(), "durable-queue.redb");
    let queue = IntentQueue::new(client.clone(), engine.clone());
    initialize_queue(&served, &client, &queue).await;
    let identity = client
        .retry_identity("db", &RequestOptions::new())
        .await
        .unwrap();
    let scope = client.database("db").module("app");
    let op: MutationOp<Value, Value> = MutationOp::new("put");
    let args = obj(vec![("key", s(&k("once"))), ("title", s("first"))]);
    let command = scope.prepare_with_identity(&op, &args, &identity).unwrap();
    let handle = queue.enqueue_command(&command).await.unwrap();
    assert_eq!(queue.enqueue_command(&command).await.unwrap(), handle);
    // The first delivery committed, but its consumer lost the acknowledgement.
    let first = scope
        .execute(&command, &CommandOptions::new())
        .await
        .unwrap();
    // A separate durable consumer recovers the same server receipt, without a new effect.
    let outbox = flower_client::CommandOutbox::new(Arc::new(
        flower_client::FileCommandStore::open(dir.path().join("outbox.journal")).unwrap(),
    ));
    let durable = outbox.enqueue(&scope, &op, &command).await.unwrap();
    let receipt = durable.execute(&CommandOptions::new()).await.unwrap();
    assert!(receipt.duplicate);
    assert_eq!(receipt.revision, first.revision);
    let session = client.causal_session();
    session
        .observe(first.read_token.clone().unwrap())
        .await
        .unwrap();
    let causal = session.database("db").module("app");
    let query = flower_sdk::QueryOp::<Value, Value>::new("get");
    let read = causal
        .query(&query, &obj(vec![("key", s(&k("once")))]))
        .await
        .unwrap();
    assert!(read.read_token.unwrap().revision >= first.revision);
    assert_eq!(queue.submit_pending(8).await.unwrap().submitted, 1);
    let replay = scope
        .execute(&command, &CommandOptions::new())
        .await
        .unwrap();
    assert!(replay.duplicate);
    assert_eq!(first.revision, replay.revision);
    let mut minimum = first.read_token.clone().unwrap();
    let read = client
        .query_value(
            "db",
            "app",
            "get",
            &serde_json::json!({"key":k("once")}),
            &RequestOptions::new().at_least(minimum.clone()),
        )
        .await
        .unwrap();
    assert!(read.read_token.unwrap().revision >= first.revision);
    minimum.incarnation.0 ^= 1;
    let wrong = client
        .query_value(
            "db",
            "app",
            "get",
            &serde_json::json!({"key":k("once")}),
            &RequestOptions::new().at_least(minimum),
        )
        .await
        .unwrap_err();
    assert_eq!(wrong.code(), Some("wrong_history"));
    let old = scope.prepare_with_identity(&op, &args, &identity).unwrap();
    let later = scope.prepare_with_identity(&op, &args, &identity).unwrap();
    let old_handle = queue.enqueue_command(&old).await.unwrap();
    let later_handle = queue.enqueue_command(&later).await.unwrap();
    client.control_retention("db",serde_json::json!({"operation":"advance","from_epoch":identity.epoch,"current_epoch":identity.epoch+1,"min_epoch":identity.epoch+1}),&RequestOptions::new()).await.unwrap();
    let result = queue.submit_pending(8).await.unwrap();
    assert_eq!(result.submitted, 0);
    assert_eq!(result.failed_business, 0);
    assert!(matches!(
        queue.intent(&old_handle).await.unwrap().unwrap().status,
        IntentStatus::Unresolved { .. }
    ));
    assert_eq!(
        queue.intent(&later_handle).await.unwrap().unwrap().status,
        IntentStatus::Queued
    );
    let failure = scope
        .execute(&old, &CommandOptions::new())
        .await
        .unwrap_err();
    assert_eq!(failure.execution, Execution::Unknown);
    assert_eq!(failure.request_id, old.request_id());
    queue
        .resolve(
            old_handle.identity(),
            Err(fail("RESOLVED", "application confirmed cancellation")),
        )
        .await
        .unwrap();
    assert!(matches!(
        queue.intent(&old_handle).await.unwrap().unwrap().status,
        IntentStatus::Failed { .. }
    ));
}

#[tokio::test]
async fn typed_transaction_failures_identify_the_step_and_keep_the_cause() {
    use flower_client::TypedTransaction;
    use flower_sdk::MutationOp;
    let (_dir, served) = serve_node(true).await;
    let client = admin_client(&served.base);
    for db in ["one", "two"] {
        client
            .deploy(db, "app", None, b"{}", &manifest(), &RequestOptions::new())
            .await
            .unwrap();
    }
    let op: MutationOp<Value, Value> = MutationOp::new("put");
    let good = obj(vec![("key", s(&k("a"))), ("title", s("kept"))]);
    let bad = obj(vec![("title", s("missing key"))]);
    let tx = TypedTransaction::new()
        .step("one", "app", &op, &good)
        .unwrap()
        .step("two", "app", &op, &bad)
        .unwrap();
    let failure = tx
        .execute(&client, &CommandOptions::new())
        .await
        .unwrap_err();
    assert_eq!(failure.step, Some(1));
    assert_eq!(failure.database.as_deref(), Some("two"));
    assert_eq!(failure.source.code(), Some("TYPE_MISMATCH"));
    assert!(
        matches!(failure.declared(), Some(flower_client::StepError::Step1(error)) if error.code == "TYPE_MISMATCH")
    );
    let unchanged = client
        .query_value(
            "one",
            "app",
            "get",
            &serde_json::json!({"key":k("a")}),
            &RequestOptions::new(),
        )
        .await
        .unwrap();
    assert!(unchanged.value.is_null());
}

/// Names travel in query strings percent-encoded: a database named with
/// `&`, `=`, `#`, `+`, `/`, `?`, spaces and non-ASCII reaches the server
/// whole instead of cutting the query short. (Timer IDs and queue names
/// are limited to letters, digits, `.`, `-` and `_`.)
#[tokio::test]
async fn names_that_need_encoding_reach_the_server_whole() {
    let server = Restartable::start().await;
    let client = admin_client(&server.base);
    let options = RequestOptions::default();
    let db = "shop & co/1?x=y#z+é";
    let queue = "jobs";
    let timer = "t1";
    client
        .deploy(db, "app", None, b"{}", &manifest(), &options)
        .await
        .unwrap();
    let now = now_ms();
    {
        let engine = server.node.engine().lock().await;
        engine
            .schedule_timer(
                db.as_bytes(),
                timer,
                "app",
                "missing",
                &s("a"),
                u64::MAX,
                1,
                now,
            )
            .unwrap();
        engine
            .enqueue(db.as_bytes(), queue, &s("a"), 0, 5, now)
            .unwrap();
    }
    let stats = client.timer_stats(db, &options).await.unwrap();
    assert_eq!(stats.pending, 1);
    let listed = client
        .list_timers(db, Some("pending"), None, &options)
        .await
        .unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, timer);
    let found = client.get_timer(db, timer, &options).await.unwrap();
    assert_eq!(found.unwrap().id, timer);
    let stats = client.queue_stats(db, queue, &options).await.unwrap();
    assert_eq!(stats.pending, 1);
    let claimed = client
        .poll_queue(db, queue, Some(1), Some(60_000), &options)
        .await
        .unwrap();
    let item = client
        .get_queue_item(db, queue, &claimed[0].id, &options)
        .await
        .unwrap();
    assert_eq!(item.unwrap().payload, serde_json::json!("a"));
    // Retry identities: only the named database was rotated.
    let rotate = serde_json::json!({"operation": "rotate", "keep_epochs": 2});
    client
        .control_retention(db, rotate, &options)
        .await
        .unwrap();
    assert!(
        client
            .retry_identity(db, &options)
            .await
            .unwrap()
            .initialized
    );
    let status = client.retention_status(db, &options).await.unwrap();
    assert!(status.is_object(), "{status}");
}

/// The test module with a `fill` mutation writing `rows` rows at once.
fn bulk_manifest() -> serde_json::Value {
    let mut manifest = manifest();
    manifest["definitions"]
        .as_array_mut()
        .unwrap()
        .push(serde_json::json!({"name": "fill", "kind": "mutation", "args": {"type": "json"}}));
    manifest["http"]
        .as_array_mut()
        .unwrap()
        .push(serde_json::json!({"alias": "fill", "definition": "fill"}));
    manifest
}

/// What an engine holds of `module` in `db`: its records and index
/// entries under the names it interned, as full keys in key order.
async fn module_keys(engine: &Arc<tokio::sync::Mutex<Engine>>, module: &str) -> Vec<Vec<u8>> {
    use flower_engine::storage::SnapshotTable;
    let engine = engine.lock().await;
    let read = engine.store().begin_read().unwrap();
    let base = 4 + b"db".len();
    let owned = format!("{module}\0");
    let ids: Vec<Vec<u8>> = read
        .scan_db_raw(SnapshotTable::InternId, b"db")
        .unwrap()
        .into_iter()
        .filter(|(_, name)| name.starts_with(owned.as_bytes()))
        .map(|(key, _)| key[base..base + 4].to_vec())
        .collect();
    let mut keys = Vec::new();
    for table in [SnapshotTable::Records, SnapshotTable::IndexEntries] {
        for (key, _) in read.scan_db_raw(table, b"db").unwrap() {
            if ids.iter().any(|id| key[base..base + 4] == id[..]) {
                keys.push(key);
            }
        }
    }
    keys
}

/// A database's purges in flight and kept removals, as stored rows.
async fn removal_rows(
    engine: &Arc<tokio::sync::Mutex<Engine>>,
) -> (Option<Vec<u8>>, Vec<(String, [u8; 32])>) {
    let engine = engine.lock().await;
    let read = engine.store().begin_read().unwrap();
    (
        read.meta(b"db", flower_engine::purge::PURGES_ROW).unwrap(),
        flower_engine::purge::removed(&read, b"db").unwrap(),
    )
}

async fn device_query(
    engine: &Arc<tokio::sync::Mutex<Engine>>,
    module: &str,
    key: &str,
) -> Result<Value, Failure> {
    let engine = engine.lock().await;
    let args = obj(vec![("key", s(&k(key)))]);
    engine.invoke_query(b"db", module, "get", &args, &CallOptions::default())
}

#[tokio::test(flavor = "multi_thread")]
async fn sync_applies_module_removals_and_purge_pages_as_members_do() {
    let server = Restartable::start().await;
    let client = admin_client(&server.base);
    let options = RequestOptions::default();
    client
        .deploy("db", "app", None, b"{}", &manifest(), &options)
        .await
        .unwrap();
    client
        .deploy("db", "bulk", None, b"{}", &bulk_manifest(), &options)
        .await
        .unwrap();
    client
        .mutate_value(
            "db",
            "app",
            "put",
            &serde_json::json!({"key": k("a"), "title": "kept"}),
            &MutationOptions::default(),
        )
        .await
        .unwrap();
    // 700 rows and their index entries: more than one purge page.
    client
        .mutate_value(
            "db",
            "bulk",
            "fill",
            &serde_json::json!({"rows": 700}),
            &MutationOptions::default(),
        )
        .await
        .unwrap();

    let dir = tempfile::tempdir().unwrap();
    let device = device_engine(dir.path(), "device.redb");
    let replica = sync_client(&server.base, device.clone(), device_builder());
    // A second device that syncs now, and next installs a snapshot.
    let early = device_engine(dir.path(), "early.redb");
    let late = sync_client(&server.base, early.clone(), device_builder());
    replica.sync_catch_up(64).await.unwrap();
    late.sync_catch_up(64).await.unwrap();
    assert_eq!(
        device_query(&device, "bulk", "f1").await.unwrap(),
        obj(vec![("title", s("t1"))])
    );
    let member = server.node.engine().clone();
    assert_eq!(module_keys(&member, "bulk").await.len(), 1400);

    // The removal reaches the device: its code goes, its data stays.
    let removed = client.remove_module("db", "bulk", &options).await.unwrap();
    assert!(!removed.purging);
    replica.sync_catch_up(64).await.unwrap();
    assert!(device_query(&device, "bulk", "f1").await.is_err());
    assert_eq!(
        device_query(&device, "app", "a").await.unwrap(),
        obj(vec![("title", s("kept"))])
    );
    assert_eq!(
        module_keys(&device, "bulk").await,
        module_keys(&member, "bulk").await
    );
    let (purging, kept) = removal_rows(&device).await;
    assert!(purging.is_none());
    assert_eq!(kept.len(), 1);
    assert_eq!(removal_rows(&member).await, (purging, kept));

    // The purge reaches it page by page, each as the member applied it.
    let purged = client.purge_module("db", "bulk", &options).await.unwrap();
    assert!(purged.purging);
    let mut pages = 0;
    while let Some(page) = server.node.purge_page(b"db").await.unwrap() {
        let page = page.unwrap();
        pages += 1;
        replica.sync_catch_up(64).await.unwrap();
        assert_eq!(
            module_keys(&device, "bulk").await,
            module_keys(&member, "bulk").await,
            "after page {pages}: {page:?}"
        );
        assert_eq!(removal_rows(&device).await, removal_rows(&member).await);
    }
    assert_eq!(pages, 2);
    assert!(module_keys(&device, "bulk").await.is_empty());
    assert_eq!(removal_rows(&device).await, (None, Vec::new()));
    assert_eq!(
        device_query(&device, "app", "a").await.unwrap(),
        obj(vec![("title", s("kept"))])
    );

    // A device that last synced before the removal installs a snapshot
    // taken after it: the removed module leaves its memory too.
    assert_eq!(
        device_query(&early, "bulk", "f1").await.unwrap(),
        obj(vec![("title", s("t1"))])
    );
    use base64::Engine as _;
    let reached = replica.position().await.unwrap();
    let file = {
        let device = device.lock().await;
        let (meta, payload) =
            flower_engine::snapshot::encode_snapshot(device.store(), b"db").unwrap();
        serde_json::json!({
            "version": 1,
            "dbs": [{
                "meta": {
                    "db": meta.db,
                    "revision": meta.revision,
                    "applied_log": meta.applied_log,
                    "incarnation": meta.incarnation
                },
                "payload": payload
            }]
        })
    };
    let encoded = base64::prelude::BASE64_STANDARD.encode(serde_json::to_vec(&file).unwrap());
    let batch: flower_client::sync::WireBatch = serde_json::from_value(serde_json::json!({
        "version": 1,
        "since": 0,
                "entries": [],
        "current": reached,
        "snapshot": {"last_index": reached, "bytes_b64": encoded},
        "incarnation": late.incarnation().await.unwrap(),
        "since": late.position().await.unwrap(),
    }))
    .unwrap();
    assert!(late.apply_batch(&batch).await.unwrap().snapshot);
    assert!(device_query(&early, "bulk", "f1").await.is_err());
    assert_eq!(
        device_query(&early, "app", "a").await.unwrap(),
        obj(vec![("title", s("kept"))])
    );
    assert!(module_keys(&early, "bulk").await.is_empty());
}
