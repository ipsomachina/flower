//! Watch deltas on the client: patches from a live server apply to the
//! value a watch holds, and a patch the watch cannot apply resyncs it
//! from a fresh snapshot, without deltas after repeated gaps.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use flower_client::client::{FlowerClient, MutationOptions, RequestOptions};
use flower_client::error::FlowerError;
use flower_client::transport::{Credentials, StreamResponse, Transport, TransportResponse};
use flower_client::watch::{MAX_GAPS, SubscribeOptions, Update, WatchEvent, WatchOptions};
use flower_cluster::network::{InProcessFactory, Registry};
use flower_cluster::node::Node;
use flower_cluster::statemachine::RuntimeFactory;
use flower_engine::runtime::{CallKind, ClosureRuntime, HostCall};
use flower_sdk::abi::{HostOp, decode_envelope};
use flower_sdk::{Failure, Value, fail};
use flower_server::api::{AppState, serve};

fn s(text: &str) -> Value {
    Value::String(text.to_string())
}

fn hc(host: &mut dyn HostCall, op: HostOp, args: Vec<Value>) -> Result<Value, Failure> {
    let input = Value::Array(args).encode_binary().unwrap();
    let output = host.op(op as u32 as u8, &input);
    decode_envelope(Value::decode_binary(&output).unwrap())
}

fn field(value: &Value, name: &str) -> Value {
    match value {
        Value::Object(map) => map.get(name).cloned().unwrap_or(Value::Null),
        _ => Value::Null,
    }
}

/// `put` stores a title under a key; `list` returns every title, and
/// fails with details while one is `refused`.
struct Factory;

impl RuntimeFactory for Factory {
    fn build(
        &self,
        _db: &[u8],
        _module: &str,
        _wasm: &[u8],
        _manifest: &Value,
    ) -> Result<Box<dyn flower_engine::runtime::Runtime>, String> {
        Ok(Box::new(ClosureRuntime::new(
            |kind: CallKind, _module: &str, name: &str, args: &Value, host: &mut dyn HostCall| {
                match (kind, name) {
                    (CallKind::Mutation, "put") => {
                        let Value::String(key) = field(args, "key") else {
                            return Err(fail("TYPE_MISMATCH", "needs a key"));
                        };
                        let mut row = std::collections::BTreeMap::new();
                        row.insert("title".to_string(), field(args, "title"));
                        hc(
                            host,
                            HostOp::Set,
                            vec![
                                s(""),
                                s("tasks"),
                                s(&format!("\"{key}\"")),
                                Value::Object(row),
                            ],
                        )?;
                        Ok(s("ok"))
                    }
                    (CallKind::Query, "list") => {
                        let page = hc(
                            host,
                            HostOp::Scan,
                            vec![s(""), s("tasks"), Value::Number(100.0), Value::Null],
                        )?;
                        let Value::Array(rows) = field(&page, "rows") else {
                            return Err(fail("TYPE_MISMATCH", "page rows"));
                        };
                        let titles: Vec<Value> = rows
                            .into_iter()
                            .map(|row| match row {
                                Value::Array(pair) if pair.len() == 2 => field(&pair[1], "title"),
                                _ => Value::Null,
                            })
                            .collect();
                        if titles.contains(&s("refused")) {
                            return Err(Failure::with_details(
                                "LIST_REFUSED",
                                "a task is refused",
                                s("refused"),
                            ));
                        }
                        Ok(Value::Array(titles))
                    }
                    _ => Err(fail("UNKNOWN_METHOD", "unknown definition")),
                }
            },
        )))
    }
}

async fn serve_node(dir: &std::path::Path) -> String {
    let registry = Registry::new();
    let node = Node::open(
        1,
        &dir.join("n1"),
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
    let state = AppState::new(Arc::new(node)).with_admin_token("secret".to_string());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(serve(listener, state));
    let admin = FlowerClient::builder(&base)
        .credentials(Credentials::Token("secret".to_string()))
        .build()
        .unwrap();
    let manifest = serde_json::json!({
        "collections": [{
            "name": "tasks",
            "key": {"type": "string"},
            "value": {"type": "object", "fields": [
                {"name": "title", "schema": {"type": "string"}, "optional": false}
            ]},
            "indexes": [],
            "references": []
        }],
        "definitions": [
            {"name": "put", "kind": "mutation", "args": {"type": "json"}},
            {"name": "list", "kind": "query", "args": {"type": "json"}}
        ],
        "triggers": [],
        "http": [
            {"alias": "put", "definition": "put"},
            {"alias": "list", "definition": "list"}
        ]
    });
    admin
        .deploy(
            "db",
            "app",
            None,
            b"{}",
            &manifest,
            &RequestOptions::default(),
        )
        .await
        .unwrap();
    base
}

#[tokio::test]
async fn watches_apply_the_patches_a_live_server_sends() {
    let dir = tempfile::tempdir().unwrap();
    let base = serve_node(dir.path()).await;
    let client = FlowerClient::builder(&base).build().unwrap();
    let long = "a title long enough that a patch beats the whole list";
    let put = |key: &'static str| {
        let client = client.clone();
        async move {
            client
                .mutate::<_, serde_json::Value>(
                    "db",
                    "app",
                    "put",
                    &serde_json::json!({"key": key, "title": long}),
                    &MutationOptions::default(),
                )
                .await
                .unwrap();
        }
    };
    put("a").await;
    let mut subscription = client.subscribe::<serde_json::Value>(
        "db",
        "app",
        "list",
        &serde_json::Value::Null,
        &SubscribeOptions::default(),
    );
    let first: Update<serde_json::Value> =
        tokio::time::timeout(Duration::from_secs(10), subscription.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    assert_eq!(first.value, serde_json::json!([long]));
    let mut expected = vec![long];
    for key in ["b", "c", "d"] {
        put(key).await;
        expected.push(long);
        let update = tokio::time::timeout(Duration::from_secs(10), subscription.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(update.value, serde_json::json!(expected));
        assert!(!update.reset, "a patch resynced");
    }
}

#[tokio::test]
async fn a_watch_ending_on_a_failure_returns_its_details() {
    let dir = tempfile::tempdir().unwrap();
    let base = serve_node(dir.path()).await;
    let client = FlowerClient::builder(&base).build().unwrap();
    let put = |title: &'static str| {
        let client = client.clone();
        async move {
            client
                .mutate::<_, serde_json::Value>(
                    "db",
                    "app",
                    "put",
                    &serde_json::json!({"key": title, "title": title}),
                    &MutationOptions::default(),
                )
                .await
                .unwrap();
        }
    };
    put("fine").await;
    let mut subscription = client.subscribe::<serde_json::Value>(
        "db",
        "app",
        "list",
        &serde_json::Value::Null,
        &SubscribeOptions::default(),
    );
    let first = tokio::time::timeout(Duration::from_secs(10), subscription.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(first.value, serde_json::json!(["fine"]));
    // The query now fails: the subscriber gets the failure the query
    // answers, details included.
    put("refused").await;
    let ended = tokio::time::timeout(Duration::from_secs(10), subscription.next())
        .await
        .unwrap()
        .unwrap();
    match ended {
        Err(FlowerError::Http(problem)) => {
            assert_eq!(problem.code, "LIST_REFUSED");
            assert_eq!(problem.details, Some(serde_json::json!("refused")));
            assert_eq!(problem.status, 422);
            assert_eq!(problem.origin, flower_client::Origin::Application);
            assert_eq!(problem.execution, flower_client::Execution::Decided);
            assert!(!problem.retryable);
        }
        other => panic!("expected the query's failure, got {other:?}"),
    }
}

/// A transport serving scripted watch streams and recording the paths
/// they were opened with.
struct Scripted {
    paths: Mutex<Vec<String>>,
    stream: Box<dyn Fn(usize) -> String + Send + Sync>,
}

#[async_trait::async_trait]
impl Transport for Scripted {
    async fn post(
        &self,
        _path: &str,
        _body: &[u8],
        _credentials: Option<&Credentials>,
    ) -> Result<TransportResponse, FlowerError> {
        Err(FlowerError::Transport("no POST in this script".to_string()))
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
        path: &str,
        body: &[u8],
        _credentials: Option<&Credentials>,
    ) -> Result<StreamResponse, FlowerError> {
        let connect = {
            let mut paths = self.paths.lock().unwrap();
            paths.push(format!("{path} {}", String::from_utf8_lossy(body)));
            paths.len() - 1
        };
        let text = (self.stream)(connect);
        Ok(StreamResponse {
            status: 200,
            retry_after_ms: None,
            body: Box::pin(futures_util::StreamExt::chain(
                futures_util::stream::iter(vec![Ok(text.into_bytes())]),
                futures_util::stream::pending(),
            )),
        })
    }
}

fn scripted(
    stream: impl Fn(usize) -> String + Send + Sync + 'static,
) -> (Arc<Scripted>, FlowerClient) {
    let transport = Arc::new(Scripted {
        paths: Mutex::new(Vec::new()),
        stream: Box::new(stream),
    });
    let client = FlowerClient::builder("http://scripted.invalid")
        .transport(transport.clone())
        .build()
        .unwrap();
    (transport, client)
}

fn snapshot(revision: u64, id: u64, value: &str) -> String {
    format!("event: snapshot\ndata: {{\"revision\":{revision},\"id\":{id},\"value\":{value}}}\n\n")
}

fn patch(revision: u64, id: u64, base: u64, operations: &str) -> String {
    format!(
        "event: patch\ndata: {{\"revision\":{revision},\"id\":{id},\"base\":{base},\"patch\":{operations}}}\n\n"
    )
}

async fn next(watch: &mut flower_client::watch::DeltaStream) -> WatchEvent {
    tokio::time::timeout(Duration::from_secs(5), watch.next_event())
        .await
        .expect("no event")
        .expect("watch ended")
        .expect("watch failed")
}

#[tokio::test]
async fn a_patch_for_another_base_resyncs_from_a_fresh_snapshot() {
    let (transport, client) = scripted(|connect| match connect {
        // The second patch names a value this stream never held, though
        // it would apply to the one it holds.
        0 => [
            snapshot(1, 10, "[1]"),
            patch(2, 11, 10, r#"[{"op":"add","path":"/1","value":2}]"#),
            patch(4, 13, 12, r#"[{"op":"add","path":"/2","value":4}]"#),
        ]
        .concat(),
        _ => [
            snapshot(4, 20, "[1,2,3,4]"),
            patch(5, 21, 20, r#"[{"op":"add","path":"/-","value":5}]"#),
        ]
        .concat(),
    });
    let mut watch = client
        .watch_deltas(
            "db",
            "app",
            "list",
            &serde_json::Value::Null,
            &WatchOptions::default(),
        )
        .await
        .unwrap();
    let mut seen = Vec::new();
    for _ in 0..4 {
        seen.push(match next(&mut watch).await {
            WatchEvent::Snapshot { revision, value } => (revision, value, false),
            WatchEvent::Reset { revision, value } => (revision, value, true),
        });
    }
    assert_eq!(
        seen,
        vec![
            (1, serde_json::json!([1]), false),
            (2, serde_json::json!([1, 2]), false),
            (4, serde_json::json!([1, 2, 3, 4]), true),
            (5, serde_json::json!([1, 2, 3, 4, 5]), false),
        ]
    );
    let paths = transport.paths.lock().unwrap().clone();
    assert_eq!(paths.len(), 2);
    assert!(
        paths.iter().all(|path| path.contains(r#""deltas":true"#)),
        "{paths:?}"
    );
}

#[tokio::test]
async fn patches_that_do_not_apply_resync_subscriptions_as_resets() {
    let (transport, client) = scripted(|connect| match connect {
        // Removing what is not there cannot apply.
        0 => [
            snapshot(1, 10, r#"{"a":1}"#),
            patch(2, 11, 10, r#"[{"op":"remove","path":"/b"}]"#),
        ]
        .concat(),
        _ => snapshot(2, 30, r#"{"b":2}"#),
    });
    let mut subscription = client.subscribe::<serde_json::Value>(
        "db",
        "app",
        "list",
        &serde_json::Value::Null,
        &SubscribeOptions::default(),
    );
    let mut updates = Vec::new();
    for _ in 0..2 {
        let update = tokio::time::timeout(Duration::from_secs(5), subscription.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        updates.push((update.revision, update.value, update.reset));
    }
    assert_eq!(
        updates,
        vec![
            (1, serde_json::json!({"a": 1}), true),
            (2, serde_json::json!({"b": 2}), true),
        ]
    );
    assert_eq!(transport.paths.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn repeated_gaps_stop_asking_for_deltas() {
    // Every connection sends a patch that never applies.
    let (transport, client) = scripted(|connect| {
        [
            snapshot(1, connect as u64 * 10, "[]"),
            patch(2, 999, 998, "[]"),
        ]
        .concat()
    });
    let mut watch = client
        .watch_deltas(
            "db",
            "app",
            "list",
            &serde_json::Value::Null,
            &WatchOptions::default(),
        )
        .await
        .unwrap();
    for _ in 0..=MAX_GAPS {
        next(&mut watch).await;
    }
    let paths = transport.paths.lock().unwrap().clone();
    assert_eq!(paths.len(), MAX_GAPS as usize + 1);
    for (connect, path) in paths.iter().enumerate() {
        assert_eq!(
            path.contains(r#""deltas":true"#),
            connect < MAX_GAPS as usize,
            "connection {connect}: {path}"
        );
    }
}
