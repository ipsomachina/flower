//! A counter app for write-path tests: `incr {key}` reads a counter and
//! writes it plus one, a read-modify-write that loses updates unless
//! every call sees the ones before it; `read {key}` answers the count.
//! For size tests, `fill {key, bytes}` writes that many bytes under the
//! key, `big {key, bytes}` answers them and `fail_big {key, bytes}` fails
//! with a message that long.

#![allow(dead_code)]

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use flower_cluster::network::{InProcessFactory, Registry};
use flower_cluster::node::Node;
use flower_cluster::statemachine::RuntimeFactory;
use flower_cluster::types::ResponseOutcome;
use flower_engine::engine::CallOptions;
use flower_engine::runtime::{CallKind, ClosureRuntime, HostCall};
use flower_sdk::abi::{HostOp, decode_envelope};
use flower_sdk::{Failure, Value, fail};

pub fn s(text: &str) -> Value {
    Value::String(text.to_string())
}

pub fn obj(pairs: Vec<(&str, Value)>) -> Value {
    Value::Object(
        pairs
            .into_iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect(),
    )
}

fn arr(items: Vec<Value>) -> Value {
    Value::Array(items)
}

fn definition(name: &str, kind: &str) -> Value {
    obj(vec![
        ("name", s(name)),
        ("kind", s(kind)),
        ("args", obj(vec![("type", s("json"))])),
    ])
}

/// One `counters` collection keyed by string, holding a number.
pub fn manifest() -> Value {
    let counters = obj(vec![
        ("name", s("counters")),
        ("key", obj(vec![("type", s("string"))])),
        ("value", obj(vec![("type", s("json"))])),
        ("indexes", arr(vec![])),
        ("references", arr(vec![])),
    ]);
    obj(vec![
        ("collections", arr(vec![counters])),
        (
            "definitions",
            arr(vec![
                definition("incr", "mutation"),
                definition("read", "query"),
                definition("fill", "mutation"),
                definition("big", "mutation"),
                definition("fail_big", "mutation"),
            ]),
        ),
        ("triggers", arr(vec![])),
        ("http", arr(vec![])),
    ])
}

/// [`manifest`] with `stamp`, a derived value that resolves a managed
/// key (as a guest signing with one does) and counts its evaluations in
/// [`STAMPS`], and `stamped`, a query reading it.
pub fn manifest_stamped() -> Value {
    let Value::Object(mut map) = manifest() else {
        unreachable!("manifests are objects")
    };
    if let Some(Value::Array(definitions)) = map.get_mut("definitions") {
        definitions.push(definition("stamp", "derived"));
        definitions.push(definition("stamped", "query"));
    }
    Value::Object(map)
}

/// Evaluations of `stamp`, on every member of the process.
pub static STAMPS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// [`manifest`] with the authorization hook [`authorize`] decides.
pub fn manifest_authorized() -> Value {
    let Value::Object(mut map) = manifest() else {
        unreachable!("manifests are objects")
    };
    map.insert("authorize".to_string(), Value::Bool(true));
    Value::Object(map)
}

/// The hook of [`manifest_authorized`]: callers without credentials are
/// anonymous; `{"token": t}` runs as subject `t` only where this
/// database's counter `grant:t` is positive, and is refused elsewhere.
fn authorize(request: &Value, host: &mut dyn HostCall) -> Result<Value, Failure> {
    let credentials = match request {
        Value::Object(map) => map.get("credentials").cloned().unwrap_or(Value::Null),
        _ => return Err(fail("TYPE_MISMATCH", "requests are objects")),
    };
    let principal = match credentials {
        Value::Null => Value::Null,
        Value::Object(map) => match map.get("token") {
            Some(Value::String(token)) => {
                if count(host, &format!("\"grant:{token}\""))? <= 0.0 {
                    return Err(fail(
                        "UNAUTHENTICATED",
                        format!("{token} has no grant here"),
                    ));
                }
                obj(vec![("subject", s(token))])
            }
            _ => return Err(fail("UNAUTHENTICATED", "credentials name a token")),
        },
        _ => return Err(fail("UNAUTHENTICATED", "credentials name a token")),
    };
    Ok(obj(vec![("principal", principal)]))
}

fn hc(host: &mut dyn HostCall, op: HostOp, args: Vec<Value>) -> Result<Value, Failure> {
    let input = Value::Array(args).encode_binary().unwrap();
    let output = host.op(op as u32 as u8, &input);
    decode_envelope(Value::decode_binary(&output).unwrap())
}

/// The counter `key` names, zero when absent.
fn count(host: &mut dyn HostCall, key: &str) -> Result<f64, Failure> {
    match hc(host, HostOp::Get, vec![s(""), s("counters"), s(key)])? {
        Value::Number(count) => Ok(count),
        Value::Null => Ok(0.0),
        other => Err(fail("TYPE_MISMATCH", format!("counter holds {other:?}"))),
    }
}

/// The `bytes` member of a size test's arguments.
fn bytes(args: &Value) -> usize {
    match args {
        Value::Object(map) => match map.get("bytes") {
            Some(Value::Number(bytes)) => *bytes as usize,
            _ => 0,
        },
        _ => 0,
    }
}

/// The counter app's runtimes.
pub struct Counters;

impl RuntimeFactory for Counters {
    fn build(
        &self,
        _db: &[u8],
        _module: &str,
        _wasm: &[u8],
        _manifest: &Value,
    ) -> Result<Box<dyn flower_engine::runtime::Runtime>, String> {
        Ok(Box::new(ClosureRuntime::new(
            |kind: CallKind, _module: &str, name: &str, args: &Value, host: &mut dyn HostCall| {
                if name == flower_sdk::auth::AUTHORIZE {
                    return authorize(args, host);
                }
                let key = match args {
                    Value::Object(map) => match map.get("key") {
                        Some(Value::String(key)) => format!("\"{key}\""),
                        _ => return Err(fail("TYPE_MISMATCH", "args need a key")),
                    },
                    _ => return Err(fail("TYPE_MISMATCH", "args take an object")),
                };
                match (kind, name) {
                    (CallKind::Mutation, "incr") => {
                        let next = count(host, &key)? + 1.0;
                        hc(
                            host,
                            HostOp::Set,
                            vec![s(""), s("counters"), s(&key), Value::Number(next)],
                        )?;
                        Ok(Value::Number(next))
                    }
                    (CallKind::Query, "read") => Ok(Value::Number(count(host, &key)?)),
                    (CallKind::Derived, "stamp") => {
                        host.read_keys();
                        let stamps = STAMPS.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                        Ok(Value::Number(stamps as f64))
                    }
                    (CallKind::Query, "stamped") => hc(
                        host,
                        HostOp::GetDerived,
                        vec![s("app"), s("stamp"), args.clone()],
                    ),
                    (CallKind::Mutation, "fill") => {
                        hc(
                            host,
                            HostOp::Set,
                            vec![s(""), s("counters"), s(&key), s(&"x".repeat(bytes(args)))],
                        )?;
                        Ok(Value::Null)
                    }
                    (CallKind::Mutation, "big") => Ok(s(&"x".repeat(bytes(args)))),
                    (CallKind::Mutation, "fail_big") => {
                        Err(fail("TOO_BIG", "x".repeat(bytes(args))))
                    }
                    _ => Err(fail("UNKNOWN_METHOD", "unknown definition")),
                }
            },
        )))
    }
}

pub fn config() -> Arc<openraft::Config> {
    Arc::new(openraft::Config::default())
}

pub async fn wait_leader(nodes: &[&Node]) -> u64 {
    for _ in 0..200 {
        for node in nodes {
            if let Some(leader) = node.status().leader {
                return leader;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("no leader elected");
}

/// An initialized single member with the counter app deployed.
pub async fn single_node(dir: &std::path::Path) -> Node {
    let registry = Registry::new();
    let node = Node::open(
        1,
        &dir.join("n1"),
        config(),
        InProcessFactory::new(registry.clone()),
        Arc::new(Counters),
    )
    .await
    .unwrap();
    registry.register(1, node.raft().clone());
    node.initialize(BTreeSet::from([1])).await.unwrap();
    assert_eq!(wait_leader(&[&node]).await, 1);
    node.deploy(b"db", "app", [9u8; 32], b"wasm", &manifest())
        .await
        .unwrap();
    node
}

/// The arguments naming counter `name`.
pub fn key(name: &str) -> Value {
    obj(vec![("key", s(name))])
}

/// Counter `name`, read fresh.
pub async fn read(node: &Node, name: &str) -> f64 {
    match node
        .query(b"db", "app", "read", &key(name), &CallOptions::default())
        .await
        .unwrap()
    {
        Value::Number(count) => count,
        other => panic!("read answered {other:?}"),
    }
}

/// A committed value's JSON.
pub fn value(outcome: &ResponseOutcome) -> Value {
    match outcome {
        ResponseOutcome::Value(bytes) => flower_engine::json::parse_value(bytes).unwrap(),
        other => panic!("not a value: {other:?}"),
    }
}
