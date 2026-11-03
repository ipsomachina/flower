//! Clearing a hidden field over a live node: watchers who may read it
//! see it go, and watchers it is hidden from receive nothing, since their
//! view of the row never held it.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use flower_cluster::network::{InProcessFactory, Registry};
use flower_cluster::node::Node;
use flower_cluster::statemachine::RuntimeFactory;
use flower_cluster::types::ResponseOutcome;
use flower_engine::engine::{CallOptions, MutationOptions};
use flower_engine::runtime::{CallKind, ClosureRuntime, HostCall};
use flower_sdk::abi::{HostOp, decode_envelope};
use flower_sdk::{Failure, Principal, Value, fail};
use flower_server::watch::{Hub, HubMessage};
use tokio::sync::mpsc;

fn s(text: &str) -> Value {
    Value::String(text.to_string())
}

fn field(args: &Value, name: &str) -> Value {
    match args {
        Value::Object(map) => map.get(name).cloned().unwrap_or(Value::Null),
        _ => Value::Null,
    }
}

fn hc(host: &mut dyn HostCall, op: HostOp, args: Vec<Value>) -> Result<Value, Failure> {
    let input = Value::Array(args).encode_binary().unwrap();
    let output = host.op(op as u32 as u8, &input);
    decode_envelope(Value::decode_binary(&output).unwrap())
}

fn guest(
    kind: CallKind,
    _module: &str,
    name: &str,
    args: &Value,
    host: &mut dyn HostCall,
) -> Result<Value, Failure> {
    let key = s(r#""x""#);
    match (kind, name) {
        (CallKind::Query, "get") => hc(host, HostOp::Get, vec![s(""), s("vault"), key]),
        (CallKind::Mutation, "put") => {
            hc(
                host,
                HostOp::Set,
                vec![
                    s(""),
                    s("vault"),
                    key,
                    field(args, "value"),
                    field(args, "clear"),
                ],
            )?;
            Ok(Value::Null)
        }
        _ => Err(fail("UNKNOWN_METHOD", name.to_string())),
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
    ) -> Result<Box<dyn flower_engine::runtime::Runtime>, String> {
        Ok(Box::new(ClosureRuntime::new(guest)))
    }
}

/// `token`: admins read it, its owner writes it.
fn manifest() -> Value {
    let json = serde_json::json!({
        "collections": [{
            "name": "vault",
            "key": {"type": "string"},
            "value": {"type": "json"},
            "indexes": [],
            "references": [],
            "access": {
                "read": {"const": true},
                "insert": {"const": true},
                "update": {"const": true},
                "fields": {"token": {
                    "read": {"eq": [{"ref": ["principal", "claims", "role"]}, {"value": "admin"}]},
                    "write": {"eq": [{"ref": ["next", "owner"]}, {"ref": ["principal", "subject"]}]}
                }}
            }
        }],
        "definitions": [
            {"name": "get", "kind": "query", "args": {"type": "json"}},
            {"name": "put", "kind": "mutation", "args": {"type": "json"}}
        ],
        "triggers": [],
        "http": []
    });
    flower_engine::json::parse_value(json.to_string().as_bytes()).unwrap()
}

fn as_(subject: &str, role: &str) -> CallOptions {
    let claims = Value::Object([("role".to_string(), s(role))].into_iter().collect());
    CallOptions {
        principal: Some(Principal::new(subject.to_string(), None, Some(claims)).unwrap()),
        ..CallOptions::default()
    }
}

async fn put(node: &Node, who: &CallOptions, value: Value, clear: Value) {
    let args = Value::Object(
        [("value".to_string(), value), ("clear".to_string(), clear)]
            .into_iter()
            .collect(),
    );
    let options = MutationOptions {
        call: who.clone(),
        ..MutationOptions::default()
    };
    let response = node
        .mutate(b"db", "app", "put", &args, &options)
        .await
        .unwrap();
    assert!(
        matches!(response.outcome, ResponseOutcome::Value(_)),
        "{:?}",
        response.outcome
    );
}

async fn next(rx: &mut mpsc::Receiver<HubMessage>, wait: Duration) -> Option<serde_json::Value> {
    match tokio::time::timeout(wait, rx.recv()).await.ok().flatten() {
        None => None,
        Some(HubMessage::Snapshot(event)) => Some(serde_json::from_slice(&event.value).unwrap()),
        Some(other) => panic!("expected a snapshot, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn clearing_a_hidden_field_reaches_only_those_who_could_read_it() {
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
    node.deploy(b"db", "app", [7; 32], b"wasm", &manifest())
        .await
        .unwrap();
    let node = Arc::new(node);
    let hub = Arc::new(Hub::new(node.clone()));
    let (_stop, stop) = tokio::sync::watch::channel(false);
    tokio::spawn(hub.clone().run(stop));
    let ada = as_("ada", "user");
    let row = |token: bool| {
        let mut map = std::collections::BTreeMap::new();
        map.insert("owner".to_string(), s("ada"));
        map.insert("text".to_string(), s("t"));
        if token {
            map.insert("token".to_string(), s("secret-token"));
        }
        Value::Object(map)
    };
    put(&node, &ada, row(true), Value::Null).await;
    let watch = |who: CallOptions| {
        let hub = &hub;
        async move {
            hub.subscribe_with(b"db", "app", "get", &Value::Null, &who, false)
                .await
                .unwrap()
        }
    };
    let (admin_first, mut admin) = watch(as_("root", "admin")).await;
    let (bob_first, mut bob) = watch(as_("bob", "user")).await;
    let (ada_first, mut own) = watch(ada.clone()).await;
    let first = |event: &flower_server::watch::HubEvent| -> serde_json::Value {
        serde_json::from_slice(&event.value).unwrap()
    };
    assert_eq!(first(&admin_first)["token"], "secret-token");
    assert!(first(&bob_first).get("token").is_none());
    assert!(first(&ada_first).get("token").is_none());
    // Ada clears her token: only the admin's view changes.
    put(&node, &ada, row(false), Value::Array(vec![s("token")])).await;
    let seen = next(&mut admin, Duration::from_secs(5)).await.unwrap();
    assert!(seen.get("token").is_none(), "{seen}");
    assert_eq!(seen["text"], "t");
    for watcher in [&mut bob, &mut own] {
        assert_eq!(next(watcher, Duration::from_millis(500)).await, None);
    }
}
