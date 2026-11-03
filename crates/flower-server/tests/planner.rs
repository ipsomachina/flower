//! Planner transactions over HTTP: a transaction a module declares `http`
//! answers `POST /v1/mutate` under its name: its planner plans, its calls run
//! in one session and commit together.

mod fixtures;

use fixtures::{hc, s};

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use flower_cluster::network::{InProcessFactory, Registry};
use flower_cluster::node::Node;
use flower_cluster::statemachine::RuntimeFactory;
use flower_engine::runtime::{CallKind, HostCall, Runtime};
use flower_sdk::abi::HostOp;
use flower_sdk::{CallFailure, Value, fail};
use flower_server::api::{AppState, serve};

/// `put {key, title}` writes a task; `get {key}` reads one; the planner
/// `pair {title}` puts it under `a` and `b`.
struct App;

impl Runtime for App {
    fn call(
        &self,
        kind: CallKind,
        _module: &str,
        name: &str,
        args: &Value,
        host: &mut dyn HostCall,
    ) -> Result<Value, CallFailure> {
        let field = |name: &str| {
            args.as_object()
                .and_then(|map| map.get(name))
                .cloned()
                .unwrap_or(Value::Null)
        };
        let key = match field("key") {
            Value::String(key) => format!("\"{key}\""),
            _ => {
                return Err(CallFailure {
                    failure: fail("TYPE_MISMATCH", "args need a key"),
                    recovered: true,
                });
            }
        };
        let outcome = match (kind, name) {
            (CallKind::Mutation, "put") => hc(
                host,
                HostOp::Set,
                vec![s(""), s("tasks"), s(&key), field("title")],
            )
            .map(|_| s("put-ok")),
            (CallKind::Query, "get") => hc(host, HostOp::Get, vec![s(""), s("tasks"), s(&key)]),
            _ => Err(fail("UNKNOWN_METHOD", "unknown definition")),
        };
        outcome.map_err(|failure| CallFailure {
            failure,
            recovered: true,
        })
    }

    fn plan(&self, _module: &str, name: &str, args: &Value) -> Result<Value, CallFailure> {
        if name != "pair" {
            return Err(CallFailure {
                failure: fail("UNKNOWN_METHOD", "no such planner"),
                recovered: false,
            });
        }
        let title = args
            .as_object()
            .and_then(|map| map.get("title"))
            .cloned()
            .unwrap_or(Value::Null);
        let call = |key: &str| {
            let mut args = std::collections::BTreeMap::new();
            args.insert("key".to_string(), s(key));
            args.insert("title".to_string(), title.clone());
            let mut call = std::collections::BTreeMap::new();
            call.insert("module".to_string(), s("app"));
            call.insert("name".to_string(), s("put"));
            call.insert("args".to_string(), Value::Object(args));
            call.insert("savepoint".to_string(), Value::Bool(false));
            Value::Object(call)
        };
        Ok(Value::Array(vec![call("a"), call("b")]))
    }
}

struct Factory;

impl RuntimeFactory for Factory {
    fn build(
        &self,
        _db: &[u8],
        _module: &str,
        _wasm: &[u8],
        _manifest: &Value,
    ) -> Result<Box<dyn Runtime>, String> {
        Ok(Box::new(App))
    }
}

fn manifest() -> serde_json::Value {
    serde_json::json!({
        "collections": [{
            "name": "tasks",
            "key": {"type": "string"},
            "value": {"type": "json"},
            "indexes": [],
            "references": []
        }],
        "definitions": [
            {"name": "put", "kind": "mutation", "args": {"type": "json"}},
            {"name": "get", "kind": "query", "args": {"type": "json"}},
            {"name": "pair", "kind": "transaction", "args": {"type": "json"}}
        ],
        "triggers": [],
        "http": [
            {"alias": "get", "definition": "get"},
            {"alias": "pair", "definition": "pair"}
        ]
    })
}

#[tokio::test]
async fn a_planner_transaction_answers_over_http() {
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
    let state = AppState::new(Arc::new(node)).with_admin_token("secret".to_string());
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
    let deployed = http
        .post(format!("{base}/v1/deploy"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({
            "db": "db", "module": "app", "identity": null,
            "wasm_b64": "e30=", "manifest": manifest(),
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(deployed.status(), 200);

    let answer = http
        .post(format!("{base}/v1/mutate"))
        .json(&serde_json::json!({
            "db": "db", "module": "app", "name": "pair", "args": {"title": "both"},
        }))
        .send()
        .await
        .unwrap();
    let status = answer.status();
    let body: serde_json::Value = answer.json().await.unwrap();
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body["value"],
        serde_json::json!(["put-ok", "put-ok"]),
        "{body}"
    );
    for key in ["a", "b"] {
        let read: serde_json::Value = http
            .post(format!("{base}/v1/query"))
            .json(&serde_json::json!({
                "db": "db", "module": "app", "name": "get", "args": {"key": key},
            }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(read["value"], "both", "{key}: {read}");
    }
}
