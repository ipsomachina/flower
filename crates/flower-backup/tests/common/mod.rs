//! Shared backup-test harness: app manifest, closure runtime, a
//! seeded engine fixture and object listing.

use std::sync::Arc;

use flower_backup::driver::RuntimeBuilder;
use flower_engine::engine::{Engine, MutationOptions};
use flower_engine::runtime::{CallKind, ClosureRuntime, HostCall};
use flower_engine::storage::GroupStore;
use flower_sdk::abi::{HostOp, decode_envelope};
use flower_sdk::{Failure, Value, fail};
use futures::TryStreamExt;
use object_store::ObjectStore;
use object_store::ObjectStoreExt;
use object_store::path::Path as ObjPath;

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

pub fn arr(items: Vec<Value>) -> Value {
    Value::Array(items)
}

pub fn k(name: &str) -> String {
    format!("\"{name}\"")
}

pub fn str_schema() -> Value {
    obj(vec![("type", s("string"))])
}

pub fn json_schema() -> Value {
    obj(vec![("type", s("json"))])
}

pub fn field(name: &str, schema: Value, optional: bool) -> Value {
    obj(vec![
        ("name", s(name)),
        ("schema", schema),
        ("optional", Value::Bool(optional)),
    ])
}

pub fn object_schema(fields: Vec<Value>) -> Value {
    obj(vec![("type", s("object")), ("fields", arr(fields))])
}

pub fn manifest() -> Value {
    obj(vec![
        (
            "collections",
            arr(vec![obj(vec![
                ("name", s("tasks")),
                ("key", str_schema()),
                (
                    "value",
                    object_schema(vec![field("title", str_schema(), false)]),
                ),
                ("indexes", arr(vec![])),
                ("references", arr(vec![])),
            ])]),
        ),
        (
            "definitions",
            arr(vec![
                obj(vec![
                    ("name", s("put")),
                    ("kind", s("mutation")),
                    ("args", json_schema()),
                ]),
                obj(vec![
                    ("name", s("get")),
                    ("kind", s("query")),
                    ("args", json_schema()),
                ]),
            ]),
        ),
        ("triggers", arr(vec![])),
        ("http", arr(vec![])),
    ])
}

pub fn hc(host: &mut dyn HostCall, op: HostOp, args: Vec<Value>) -> Result<Value, Failure> {
    let input = Value::Array(args).encode_binary().unwrap();
    let output = host.op(op as u32 as u8, &input);
    let envelope = Value::decode_binary(&output).unwrap();
    decode_envelope(envelope)
}

pub fn handler(
    kind: CallKind,
    _module: &str,
    name: &str,
    args: &Value,
    host: &mut dyn HostCall,
) -> Result<Value, Failure> {
    let map = match args {
        Value::Object(map) => map.clone(),
        _ => return Err(fail("TYPE_MISMATCH", "args take an object")),
    };
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
        (CallKind::Query, "get") => hc(host, HostOp::Get, vec![s(""), s("tasks"), s(&key)]),
        _ => Err(fail("UNKNOWN_METHOD", "unknown definition")),
    }
}

pub fn runtime() -> Box<dyn flower_engine::runtime::Runtime> {
    Box::new(ClosureRuntime::new(handler))
}

/// A plain engine with the app deployed and two records committed.
pub fn fixture(dir: &std::path::Path) -> Engine {
    let store = GroupStore::create(dir.join("group.redb")).unwrap();
    let mut engine = Engine::new(store);
    engine
        .deploy(b"db", "app", [7u8; 32], b"", &manifest(), runtime())
        .unwrap();
    for (key, title) in [("a", "first"), ("b", "second")] {
        engine
            .invoke_mutation(
                b"db",
                "app",
                "put",
                &obj(vec![("key", s(&k(key))), ("title", s(title))]),
                &MutationOptions::default(),
            )
            .unwrap();
    }
    flower_cluster::statemachine::StateMachine::register_database(engine.store(), b"db").unwrap();
    engine
}

/// Hex noise: compression cannot shrink it much below half, so it
/// makes views and segments as large as a test needs.
pub fn noise(len: usize, seed: u64) -> String {
    let mut state = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            char::from_digit((state % 16) as u32, 16).unwrap()
        })
        .collect()
}

pub fn builder() -> RuntimeBuilder {
    Arc::new(|_db: &[u8], _module: &str, _wasm: &[u8], _manifest: &Value| Ok(runtime()))
}

pub fn kek() -> [u8; 32] {
    [3u8; 32]
}

/// Every object under the backup prefix with its bytes.
pub async fn stored_objects(store: &dyn ObjectStore) -> Vec<(String, Vec<u8>)> {
    let listed: Vec<_> = store
        .list(Some(&ObjPath::from("flower-backup/v1")))
        .try_collect()
        .await
        .unwrap();
    let mut out = Vec::new();
    for meta in listed {
        let bytes = store
            .get(&meta.location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .to_vec();
        out.push((meta.location.to_string(), bytes));
    }
    out
}
