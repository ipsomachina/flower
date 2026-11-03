//! Raw aggregates through the manifest's `aggregate` member, with a
//! closure runtime: the reducer runs without a context, and one that
//! reaches for it fails the write; manifests naming no index refuse.

mod fixtures;

use fixtures::{obj, s};

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

use flower_engine::derived::AGGREGATE_CONTEXT_FORBIDDEN;
use flower_engine::engine::{CallOptions, Engine, MutationOptions};
use flower_engine::runtime::{CallKind, ClosureRuntime, HostCall};
use flower_engine::storage::GroupStore;
use flower_sdk::abi::{HostOp, decode_envelope};
use flower_sdk::{Failure, Value, fail};

/// Reducer calls, and whether the reducer reaches for a host call.
static REDUCES: AtomicU32 = AtomicU32::new(0);
static BUILDS: AtomicU32 = AtomicU32::new(0);
static REACH: AtomicBool = AtomicBool::new(false);

fn json() -> Value {
    obj(vec![("type", s("json"))])
}

/// `orders` indexed by shop; `count` aggregates each shop's orders.
fn manifest(index: &str) -> Value {
    let definition =
        |name: &str, kind: &str| obj(vec![("name", s(name)), ("kind", s(kind)), ("args", json())]);
    let mut count = definition("count", "derived");
    if let Value::Object(map) = &mut count {
        map.insert(
            "aggregate".to_string(),
            obj(vec![("collection", s("orders")), ("index", s(index))]),
        );
    }
    obj(vec![
        (
            "collections",
            Value::Array(vec![obj(vec![
                ("name", s("orders")),
                ("key", obj(vec![("type", s("string"))])),
                ("value", json()),
                (
                    "indexes",
                    Value::Array(vec![obj(vec![
                        ("name", s("by_shop")),
                        ("fields", Value::Array(vec![s("shop")])),
                    ])]),
                ),
                ("references", Value::Array(vec![])),
            ])]),
        ),
        (
            "definitions",
            Value::Array(vec![
                count,
                definition("put", "mutation"),
                definition("read", "query"),
            ]),
        ),
        ("triggers", Value::Array(vec![])),
        ("http", Value::Array(vec![])),
    ])
}

fn call(host: &mut dyn HostCall, op: HostOp, args: Vec<Value>) -> Result<Value, Failure> {
    let input = Value::Array(args).encode_binary().unwrap();
    let output = host.op(op as u32 as u8, &input);
    decode_envelope(Value::decode_binary(&output).unwrap())
}

fn member<'a>(value: &'a Value, name: &str) -> &'a Value {
    value
        .as_object()
        .and_then(|map| map.get(name))
        .unwrap_or(&Value::Null)
}

fn app(
    kind: CallKind,
    name: &str,
    args: &Value,
    host: &mut dyn HostCall,
) -> Result<Value, Failure> {
    match (kind, name) {
        (CallKind::Mutation, "put") => {
            let key = format!("\"{}\"", member(args, "id").as_str().unwrap_or_default());
            let row = obj(vec![("shop", member(args, "shop").clone())]);
            call(host, HostOp::Set, vec![s(""), s("orders"), s(&key), row])
        }
        (CallKind::Query, "read") => call(
            host,
            HostOp::GetDerived,
            vec![
                s(""),
                s("count"),
                obj(vec![(
                    "group",
                    Value::Array(vec![member(args, "shop").clone()]),
                )]),
            ],
        ),
        // The build: count the group's rows.
        (CallKind::Derived, "count") => {
            BUILDS.fetch_add(1, Ordering::SeqCst);
            let shop = match member(args, "group") {
                Value::Array(group) => group[0].clone(),
                _ => return Err(fail("TYPE_MISMATCH", "count takes a group")),
            };
            let prefix = format!("\"{}\"", shop.as_str().unwrap_or_default());
            let bounds = obj(vec![
                ("prefix", Value::Array(vec![s(&prefix)])),
                ("gt", Value::Null),
                ("gte", Value::Null),
                ("lt", Value::Null),
                ("lte", Value::Null),
                ("limit", Value::Number(1000.0)),
                ("after", Value::Null),
                ("reverse", Value::Bool(false)),
            ]);
            let page = call(
                host,
                HostOp::Range,
                vec![s(""), s("orders"), s("by_shop"), bounds],
            )?;
            let rows = match member(&page, "rows") {
                Value::Array(rows) => rows.len(),
                _ => 0,
            };
            Ok(Value::Number(rows as f64))
        }
        // The reducer: plus one per row in, minus one per row out.
        (CallKind::Reduce, "count") => {
            REDUCES.fetch_add(1, Ordering::SeqCst);
            if REACH.load(Ordering::SeqCst) {
                // Caught, yet still fatal.
                let _ = call(host, HostOp::Now, vec![]);
            }
            let mut count = member(args, "previous").as_f64().unwrap_or(0.0);
            if let Value::Array(changes) = member(args, "changes") {
                for change in changes {
                    if *member(change, "old") != Value::Null {
                        count -= 1.0;
                    }
                    if *member(change, "new") != Value::Null {
                        count += 1.0;
                    }
                }
            }
            Ok(obj(vec![("value", Value::Number(count))]))
        }
        _ => Err(fail("UNKNOWN_METHOD", "unknown definition")),
    }
}

fn engine(test: &str) -> (Engine, std::path::PathBuf) {
    let mut path = std::env::temp_dir();
    path.push(format!("flower-aggregates-{test}-{}", std::process::id()));
    let _ = std::fs::remove_file(&path);
    (Engine::new(GroupStore::create(&path).unwrap()), path)
}

fn deploy(engine: &mut Engine, index: &str) -> Result<(), Failure> {
    engine.deploy(
        b"db",
        "shop",
        [3u8; 32],
        b"",
        &manifest(index),
        Box::new(ClosureRuntime::new(
            |kind: CallKind, _module: &str, name: &str, args: &Value, host: &mut dyn HostCall| {
                app(kind, name, args, host)
            },
        )),
    )
}

#[test]
fn raw_reducers_run_without_a_context() {
    let (mut engine, path) = engine("context");
    deploy(&mut engine, "by_shop").unwrap();
    let put = |engine: &mut Engine, id: &str, shop: &str| {
        engine.invoke_mutation(
            b"db",
            "shop",
            "put",
            &obj(vec![("id", s(id)), ("shop", s(shop))]),
            &MutationOptions::default(),
        )
    };
    let read = |engine: &Engine, shop: &str| {
        engine
            .invoke_query(
                b"db",
                "shop",
                "read",
                &obj(vec![("shop", s(shop))]),
                &CallOptions::default(),
            )
            .unwrap()
    };
    put(&mut engine, "o1", "north").unwrap();
    assert_eq!(read(&engine, "north"), Value::Number(1.0));
    assert_eq!(BUILDS.load(Ordering::SeqCst), 1);
    // The raw reducer takes the insert and the move as deltas.
    put(&mut engine, "o2", "north").unwrap();
    put(&mut engine, "o1", "south").unwrap();
    assert_eq!(read(&engine, "north"), Value::Number(1.0));
    assert_eq!(BUILDS.load(Ordering::SeqCst), 1);
    assert_eq!(REDUCES.load(Ordering::SeqCst), 2);
    // A reducer reaching for its context fails the write, even though
    // it caught the refusal; nothing commits.
    REACH.store(true, Ordering::SeqCst);
    let refused = put(&mut engine, "o3", "north").unwrap_err();
    assert_eq!(refused.code, AGGREGATE_CONTEXT_FORBIDDEN);
    REACH.store(false, Ordering::SeqCst);
    assert_eq!(read(&engine, "north"), Value::Number(1.0));
    drop(engine);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn aggregates_name_an_index_of_their_collection() {
    let (mut engine, path) = engine("index");
    let refused = deploy(&mut engine, "by_city").unwrap_err();
    assert!(refused.message.contains("by_city"), "{refused:?}");
    drop(engine);
    let _ = std::fs::remove_file(&path);
}
