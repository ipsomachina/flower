//! Execution-layer integration tests: host operations, derived
//! evaluation, the writer lane and engine admission, driven through
//! [`Engine`] with in-process closure runtimes.

mod fixtures;

use fixtures::{
    Guest, arr, field, hc, index, json_schema, k, n, num_schema, obj, object_schema, s, str_schema,
};

use std::sync::{Arc, Mutex};

use flower_engine::engine::{CallOptions, Engine, MutationOptions, RetryIdentity};
use flower_engine::retry::ScopedIdentity;
use flower_engine::runtime::{CallKind, ClosureRuntime, HostCall, Runtime};
use flower_engine::storage::GroupStore;
use flower_sdk::abi::{HostOp, decode_call_envelope};
use flower_sdk::{Failure, Principal, Value, fail};

fn field_reference(targets: Vec<&str>, fields: Vec<&str>, on_delete: &str) -> Value {
    obj(vec![
        ("targets", arr(targets.into_iter().map(s).collect())),
        ("discriminator", Value::Null),
        (
            "place",
            obj(vec![("fields", arr(fields.into_iter().map(s).collect()))]),
        ),
        ("on_delete", s(on_delete)),
    ])
}

fn collection(
    name: &str,
    key: Value,
    value: Value,
    indexes: Vec<Value>,
    references: Vec<Value>,
) -> Value {
    obj(vec![
        ("name", s(name)),
        ("key", key),
        ("value", value),
        ("indexes", arr(indexes)),
        ("references", arr(references)),
    ])
}

fn tasks_collection(on_delete: Option<&str>) -> Value {
    let references = on_delete
        .map(|behavior| field_reference(vec!["posts"], vec!["owner"], behavior))
        .into_iter()
        .collect();
    // set_null clears the field to null, so the field must accept it:
    // optional controls absence, nullable controls null.
    let owner = match on_delete {
        Some("set_null") => nullable_schema(str_schema()),
        _ => str_schema(),
    };
    collection(
        "tasks",
        str_schema(),
        object_schema(vec![
            field("title", str_schema(), false),
            field("owner", owner, true),
        ]),
        vec![index("by_owner", vec!["owner"])],
        references,
    )
}

fn posts_collection() -> Value {
    collection(
        "posts",
        str_schema(),
        object_schema(vec![field("title", str_schema(), false)]),
        vec![],
        vec![],
    )
}

fn photos_collection() -> Value {
    collection(
        "photos",
        str_schema(),
        object_schema(vec![field("title", str_schema(), false)]),
        vec![],
        vec![],
    )
}

fn nullable_schema(inner: Value) -> Value {
    obj(vec![("type", s("nullable")), ("of", inner)])
}

fn poly_reference(
    targets: Vec<&str>,
    discriminator: &str,
    fields: Vec<&str>,
    on_delete: &str,
) -> Value {
    obj(vec![
        ("targets", arr(targets.into_iter().map(s).collect())),
        ("discriminator", s(discriminator)),
        (
            "place",
            obj(vec![("fields", arr(fields.into_iter().map(s).collect()))]),
        ),
        ("on_delete", s(on_delete)),
    ])
}

fn comments_collection(on_delete: &str, nullable: bool) -> Value {
    let disc = if nullable {
        nullable_schema(str_schema())
    } else {
        str_schema()
    };
    let id = if nullable {
        nullable_schema(str_schema())
    } else {
        str_schema()
    };
    collection(
        "comments",
        str_schema(),
        object_schema(vec![
            field("body", str_schema(), false),
            field("target_type", disc, false),
            field("target_id", id, false),
        ]),
        vec![index("by_target", vec!["target_type", "target_id"])],
        vec![poly_reference(
            vec!["posts", "photos"],
            "target_type",
            vec!["target_id"],
            on_delete,
        )],
    )
}

fn log_collection() -> Value {
    collection("log", str_schema(), json_schema(), vec![], vec![])
}

fn cells_collection() -> Value {
    collection(
        "cells",
        str_schema(),
        object_schema(vec![field("v", num_schema(), false)]),
        vec![],
        vec![],
    )
}

fn definition(name: &str, kind: &str) -> Value {
    definition_args(name, kind, json_schema(), None)
}

fn definition_args(name: &str, kind: &str, args: Value, materialize: Option<&str>) -> Value {
    let mut pairs = vec![("name", s(name)), ("kind", s(kind)), ("args", args)];
    if let Some(policy) = materialize {
        pairs.push(("materialize", s(policy)));
    }
    obj(pairs)
}

fn trigger_def(name: &str, source: &str) -> Value {
    obj(vec![("name", s(name)), ("source", s(source))])
}

fn manifest(collections: Vec<Value>, definitions: Vec<Value>, triggers: Vec<Value>) -> Value {
    obj(vec![
        ("collections", arr(collections)),
        ("definitions", arr(definitions)),
        ("triggers", arr(triggers)),
        ("http", arr(vec![])),
    ])
}

fn setup(test: &str, manifest: Value, runtime: Box<dyn Runtime>) -> (Engine, std::path::PathBuf) {
    let mut path = std::env::temp_dir();
    path.push(format!("flower-exec-{test}-{}", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let store = GroupStore::create(&path).unwrap();
    let mut engine = Engine::new(store);
    engine
        .deploy(b"db", "app", [7u8; 32], b"", &manifest, runtime)
        .unwrap();
    (engine, path)
}

fn teardown(engine: Engine, path: std::path::PathBuf) {
    drop(engine);
    let _ = std::fs::remove_file(&path);
}

fn opt(value: Value) -> Option<Value> {
    if value == Value::Null {
        None
    } else {
        Some(value)
    }
}

fn get(host: &mut dyn HostCall, collection: &str, key: &str) -> Result<Option<Value>, Failure> {
    Ok(opt(hc(
        host,
        HostOp::Get,
        vec![s(""), s(collection), s(key)],
    )?))
}

fn set(host: &mut dyn HostCall, collection: &str, key: &str, value: Value) -> Result<(), Failure> {
    hc(host, HostOp::Set, vec![s(""), s(collection), s(key), value])?;
    Ok(())
}

fn del(host: &mut dyn HostCall, collection: &str, key: &str) -> Result<(), Failure> {
    hc(host, HostOp::Delete, vec![s(""), s(collection), s(key)])?;
    Ok(())
}

fn scan(
    host: &mut dyn HostCall,
    collection: &str,
    limit: u32,
    after: Option<String>,
) -> Result<Page, Failure> {
    let page = hc(
        host,
        HostOp::Scan,
        vec![
            s(""),
            s(collection),
            n(limit as f64),
            after.map(|item| s(&item)).unwrap_or(Value::Null),
        ],
    )?;
    Ok(decode_page(&page))
}

/// A decoded page: `(key, value)` rows plus the continuation cursor.
type Page = (Vec<(Value, Value)>, Option<String>);

fn decode_page(page: &Value) -> Page {
    let map = match page {
        Value::Object(map) => map,
        _ => panic!("page is not an object"),
    };
    let rows = match map.get("rows") {
        Some(Value::Array(rows)) => rows
            .iter()
            .map(|row| match row {
                Value::Array(pair) if pair.len() == 2 => (pair[0].clone(), pair[1].clone()),
                _ => panic!("page row is not a pair"),
            })
            .collect(),
        _ => panic!("page rows are not an array"),
    };
    let cursor = match map.get("cursor") {
        None | Some(Value::Null) => None,
        Some(Value::String(cursor)) => Some(cursor.clone()),
        _ => panic!("page cursor is not a string"),
    };
    (rows, cursor)
}

fn call(
    host: &mut dyn HostCall,
    module: &str,
    name: &str,
    args: Value,
    savepoint: bool,
) -> Result<Value, flower_sdk::CallFailure> {
    let input = Value::Array(vec![s(module), s(name), args, Value::Bool(savepoint)])
        .encode_binary()
        .unwrap();
    let output = host.op(HostOp::Call as u32 as u8, &input);
    let envelope = Value::decode_binary(&output).unwrap();
    decode_call_envelope(envelope)
}

fn settle(host: &mut dyn HostCall) -> Result<(), Failure> {
    hc(host, HostOp::Settle, vec![])?;
    Ok(())
}

fn get_derived(
    host: &mut dyn HostCall,
    module: &str,
    name: &str,
    args: Value,
) -> Result<Value, Failure> {
    hc(host, HostOp::GetDerived, vec![s(module), s(name), args])
}

fn default_mutation() -> MutationOptions {
    MutationOptions::default()
}

fn retry_options(intent: &[u8], fingerprint: u8) -> MutationOptions {
    let mut fp = [0u8; 32];
    fp[0] = fingerprint;
    MutationOptions {
        retry: Some(RetryIdentity {
            identity: ScopedIdentity {
                incarnation: 0,
                epoch: 0,
                intent: intent.to_vec(),
            },
            fingerprint: fp,
        }),
        ..Default::default()
    }
}

#[test]
fn query_echoes_args() {
    let manifest = manifest(vec![], vec![definition("echo", "query")], vec![]);
    let runtime = ClosureRuntime::new(
        |kind: CallKind, _module: &str, name: &str, args: &Value, _host: &mut dyn HostCall| {
            assert_eq!(kind, CallKind::Query);
            assert_eq!(name, "echo");
            Ok(args.clone())
        },
    );
    let (engine, path) = setup("query-echo", manifest, Box::new(runtime));
    let out = engine
        .invoke_query(b"db", "app", "echo", &s("hi"), &CallOptions::default())
        .unwrap();
    assert_eq!(out, s("hi"));
    teardown(engine, path);
}

#[test]
fn mutation_set_get_roundtrip() {
    let manifest = manifest(
        vec![tasks_collection(None)],
        vec![definition("put", "mutation"), definition("read", "query")],
        vec![],
    );
    let runtime = ClosureRuntime::new(
        |kind: CallKind, _module: &str, name: &str, args: &Value, host: &mut dyn HostCall| match (
            kind, name,
        ) {
            (CallKind::Mutation, "put") => {
                set(host, "tasks", &k("t1"), args.clone())?;
                Ok(get(host, "tasks", &k("t1"))?.unwrap())
            }
            (CallKind::Query, "read") => Ok(get(host, "tasks", &k("t1"))?.unwrap_or(Value::Null)),
            _ => Err(fail("UNKNOWN_METHOD", name.to_string())),
        },
    );
    let (mut engine, path) = setup("set-get", manifest, Box::new(runtime));
    let row = obj(vec![("title", s("milk"))]);
    let out = engine
        .invoke_mutation(b"db", "app", "put", &row, &default_mutation())
        .unwrap();
    assert_eq!(out, row);
    // The committed row survives the invocation.
    let out = engine
        .invoke_query(b"db", "app", "read", &Value::Null, &CallOptions::default())
        .unwrap();
    assert_eq!(out, row);
    teardown(engine, path);
}

#[test]
fn savepoint_call_recovers() {
    let manifest = manifest(
        vec![tasks_collection(None)],
        vec![
            definition("outer", "mutation"),
            definition("inner_fail", "mutation"),
            definition("read", "query"),
        ],
        vec![],
    );
    let runtime = ClosureRuntime::new(
        |kind: CallKind, _module: &str, name: &str, _args: &Value, host: &mut dyn HostCall| match (
            kind, name,
        ) {
            (CallKind::Mutation, "outer") => {
                set(host, "tasks", &k("kept"), obj(vec![("title", s("kept"))]))?;
                match call(host, "", "inner_fail", Value::Null, true) {
                    Err(failure) if failure.recovered => {}
                    _ => panic!("inner call should recover"),
                }
                Ok(s("outer-ok"))
            }
            (CallKind::Mutation, "inner_fail") => {
                set(
                    host,
                    "tasks",
                    &k("rolled"),
                    obj(vec![("title", s("rolled"))]),
                )?;
                Err(fail("INNER_BOOM", "inner failed"))
            }
            (CallKind::Query, "read") => {
                let (rows, _) = scan(host, "tasks", 100, None)?;
                Ok(n(rows.len() as f64))
            }
            _ => Err(fail("UNKNOWN_METHOD", name.to_string())),
        },
    );
    let (mut engine, path) = setup("savepoint", manifest, Box::new(runtime));
    let out = engine
        .invoke_mutation(b"db", "app", "outer", &Value::Null, &default_mutation())
        .unwrap();
    assert_eq!(out, s("outer-ok"));
    // The outer write committed; the savepoint branch rolled back.
    let out = engine
        .invoke_query(b"db", "app", "read", &Value::Null, &CallOptions::default())
        .unwrap();
    assert_eq!(out, n(1.0));
    teardown(engine, path);
}

/// A backend whose calls trap: unrecovered failures for poison tests.
struct TrapRuntime;

impl Runtime for TrapRuntime {
    fn call(
        &self,
        _kind: CallKind,
        _module: &str,
        name: &str,
        _args: &Value,
        _host: &mut dyn HostCall,
    ) -> Result<Value, flower_sdk::CallFailure> {
        Err(flower_sdk::CallFailure {
            failure: fail("host_trap", format!("boom in {name}")),
            recovered: false,
        })
    }

    fn plan(
        &self,
        _module: &str,
        _name: &str,
        _args: &Value,
    ) -> Result<Value, flower_sdk::CallFailure> {
        Err(flower_sdk::CallFailure {
            failure: fail("invalid_invocation", "no planners"),
            recovered: false,
        })
    }
}

#[test]
fn fatal_nested_call_poisons() {
    let app_manifest = manifest(
        vec![tasks_collection(None)],
        vec![definition("outer", "mutation"), definition("read", "query")],
        vec![],
    );
    let runtime = ClosureRuntime::new(
        |kind: CallKind, _module: &str, name: &str, _args: &Value, host: &mut dyn HostCall| {
            match (kind, name) {
                (CallKind::Mutation, "outer") => {
                    set(host, "tasks", &k("t1"), obj(vec![("title", s("t1"))]))?;
                    // The trap poisons even though the closure propagates it
                    // as an ordinary error.
                    let _ = call(host, "boom", "explode", Value::Null, false);
                    Ok(s("unreachable"))
                }
                (CallKind::Query, "read") => {
                    let (rows, _) = scan(host, "tasks", 100, None)?;
                    Ok(n(rows.len() as f64))
                }
                _ => Err(fail("UNKNOWN_METHOD", name.to_string())),
            }
        },
    );
    let mut path = std::env::temp_dir();
    path.push(format!("flower-exec-poison-{}", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let store = GroupStore::create(&path).unwrap();
    let mut engine = Engine::new(store);
    engine
        .deploy(
            b"db",
            "app",
            [7u8; 32],
            b"",
            &app_manifest,
            Box::new(runtime),
        )
        .unwrap();
    let mut boom = manifest(vec![], vec![definition("explode", "mutation")], vec![]);
    // Other modules reach exports only.
    if let Value::Object(root) = &mut boom {
        root.insert(
            "exports".to_string(),
            obj(vec![
                ("collections", Value::Array(vec![])),
                ("derived", Value::Array(vec![])),
                ("methods", Value::Array(vec![s("explode")])),
            ]),
        );
    }
    engine
        .deploy(b"db", "boom", [8u8; 32], b"", &boom, Box::new(TrapRuntime))
        .unwrap();
    let failure = engine
        .invoke_mutation(b"db", "app", "outer", &Value::Null, &default_mutation())
        .unwrap_err();
    assert_eq!(failure.code, "host_trap");
    // The poisoned invocation committed nothing.
    let out = engine
        .invoke_query(b"db", "app", "read", &Value::Null, &CallOptions::default())
        .unwrap();
    assert_eq!(out, n(0.0));
    teardown(engine, path);
}

#[test]
fn unknown_definition_is_plain_failure() {
    let manifest = manifest(vec![], vec![definition("echo", "query")], vec![]);
    let runtime = ClosureRuntime::new(
        |_kind: CallKind, _module: &str, _name: &str, args: &Value, _host: &mut dyn HostCall| {
            Ok(args.clone())
        },
    );
    let (engine, path) = setup("unknown-def", manifest, Box::new(runtime));
    let failure = engine
        .invoke_query(
            b"db",
            "app",
            "missing",
            &Value::Null,
            &CallOptions::default(),
        )
        .unwrap_err();
    assert_eq!(failure.code, "UNKNOWN_METHOD");
    teardown(engine, path);
}

#[test]
fn query_cannot_write() {
    let manifest = manifest(
        vec![tasks_collection(None)],
        vec![definition("sneaky", "query")],
        vec![],
    );
    let runtime = ClosureRuntime::new(
        |_kind: CallKind, _module: &str, _name: &str, _args: &Value, host: &mut dyn HostCall| {
            set(host, "tasks", &k("t1"), obj(vec![("title", s("t1"))]))?;
            Ok(Value::Null)
        },
    );
    let (engine, path) = setup("query-write", manifest, Box::new(runtime));
    let failure = engine
        .invoke_query(
            b"db",
            "app",
            "sneaky",
            &Value::Null,
            &CallOptions::default(),
        )
        .unwrap_err();
    assert_eq!(failure.code, "INVALID_INVOCATION");
    teardown(engine, path);
}

#[test]
fn mutation_rejects_bad_schema() {
    let manifest = manifest(
        vec![tasks_collection(None)],
        vec![definition("put", "mutation"), definition("read", "query")],
        vec![],
    );
    let runtime = ClosureRuntime::new(
        |kind: CallKind, _module: &str, name: &str, args: &Value, host: &mut dyn HostCall| match (
            kind, name,
        ) {
            (CallKind::Mutation, "put") => {
                set(host, "tasks", &k("t1"), args.clone())?;
                Ok(Value::Null)
            }
            (CallKind::Query, "read") => {
                let (rows, _) = scan(host, "tasks", 100, None)?;
                Ok(n(rows.len() as f64))
            }
            _ => Err(fail("UNKNOWN_METHOD", name.to_string())),
        },
    );
    let (mut engine, path) = setup("bad-schema", manifest, Box::new(runtime));
    let bad = obj(vec![("title", n(1.0))]);
    let failure = engine
        .invoke_mutation(b"db", "app", "put", &bad, &default_mutation())
        .unwrap_err();
    assert_eq!(failure.code, "SCHEMA_MISMATCH");
    let out = engine
        .invoke_query(b"db", "app", "read", &Value::Null, &CallOptions::default())
        .unwrap();
    assert_eq!(out, n(0.0));
    teardown(engine, path);
}

fn seed_post_and_task(engine: &mut Engine) {
    engine
        .invoke_mutation(b"db", "app", "seed", &Value::Null, &default_mutation())
        .unwrap();
}

fn fk_manifest(on_delete: &str) -> Value {
    manifest(
        vec![tasks_collection(Some(on_delete)), posts_collection()],
        vec![
            definition("seed", "mutation"),
            definition("drop_post", "mutation"),
            definition("put_task", "mutation"),
            definition("read_task", "query"),
            definition("read_post", "query"),
        ],
        vec![],
    )
}

fn fk_runtime() -> ClosureRuntime<impl Guest> {
    ClosureRuntime::new(
        |kind: CallKind, _module: &str, name: &str, args: &Value, host: &mut dyn HostCall| {
            match (kind, name) {
                (CallKind::Mutation, "seed") => {
                    set(host, "posts", &k("p1"), obj(vec![("title", s("post"))]))?;
                    // The owner field holds the plain string: its canonical
                    // bytes equal the target key bytes.
                    set(
                        host,
                        "tasks",
                        &k("t1"),
                        obj(vec![("title", s("task")), ("owner", s("p1"))]),
                    )?;
                    Ok(Value::Null)
                }
                (CallKind::Mutation, "drop_post") => {
                    del(host, "posts", &k("p1"))?;
                    Ok(Value::Null)
                }
                (CallKind::Mutation, "put_task") => {
                    set(host, "tasks", &k("t2"), args.clone())?;
                    Ok(Value::Null)
                }
                (CallKind::Query, "read_task") => {
                    Ok(get(host, "tasks", &k("t1"))?.unwrap_or(Value::Null))
                }
                (CallKind::Query, "read_post") => {
                    Ok(get(host, "posts", &k("p1"))?.unwrap_or(Value::Null))
                }
                _ => Err(fail("UNKNOWN_METHOD", name.to_string())),
            }
        },
    )
}

#[test]
fn fk_restrict_blocks_delete() {
    let (mut engine, path) = setup(
        "fk-restrict",
        fk_manifest("restrict"),
        Box::new(fk_runtime()),
    );
    seed_post_and_task(&mut engine);
    let failure = engine
        .invoke_mutation(b"db", "app", "drop_post", &Value::Null, &default_mutation())
        .unwrap_err();
    assert_eq!(failure.code, "foreign_key_violation");
    // The delete rolled back: the post survives.
    let out = engine
        .invoke_query(
            b"db",
            "app",
            "read_post",
            &Value::Null,
            &CallOptions::default(),
        )
        .unwrap();
    assert_ne!(out, Value::Null);
    teardown(engine, path);
}

#[test]
fn fk_cascade_deletes_referencing() {
    let (mut engine, path) = setup("fk-cascade", fk_manifest("cascade"), Box::new(fk_runtime()));
    seed_post_and_task(&mut engine);
    engine
        .invoke_mutation(b"db", "app", "drop_post", &Value::Null, &default_mutation())
        .unwrap();
    let out = engine
        .invoke_query(
            b"db",
            "app",
            "read_task",
            &Value::Null,
            &CallOptions::default(),
        )
        .unwrap();
    assert_eq!(out, Value::Null);
    teardown(engine, path);
}

#[test]
fn fk_set_null_clears_fields() {
    let (mut engine, path) = setup(
        "fk-setnull",
        fk_manifest("set_null"),
        Box::new(fk_runtime()),
    );
    seed_post_and_task(&mut engine);
    engine
        .invoke_mutation(b"db", "app", "drop_post", &Value::Null, &default_mutation())
        .unwrap();
    let out = engine
        .invoke_query(
            b"db",
            "app",
            "read_task",
            &Value::Null,
            &CallOptions::default(),
        )
        .unwrap();
    match out {
        Value::Object(map) => assert_eq!(map.get("owner"), Some(&Value::Null)),
        _ => panic!("task row is not an object"),
    }
    teardown(engine, path);
}

#[test]
fn fk_outgoing_missing_target_rejects() {
    let (mut engine, path) = setup(
        "fk-outgoing",
        fk_manifest("restrict"),
        Box::new(fk_runtime()),
    );
    let ghost = obj(vec![("title", s("ghost")), ("owner", s("nobody"))]);
    let failure = engine
        .invoke_mutation(b"db", "app", "put_task", &ghost, &default_mutation())
        .unwrap_err();
    assert_eq!(failure.code, "foreign_key_violation");
    teardown(engine, path);
}

#[test]
fn fk_null_skips_check() {
    let (mut engine, path) = setup("fk-null", fk_manifest("restrict"), Box::new(fk_runtime()));
    let bare = obj(vec![("title", s("bare"))]);
    engine
        .invoke_mutation(b"db", "app", "put_task", &bare, &default_mutation())
        .unwrap();
    teardown(engine, path);
}

fn poly_fixture(test: &str, on_delete: &str, nullable: bool) -> (Engine, std::path::PathBuf) {
    let manifest = manifest(
        vec![
            comments_collection(on_delete, nullable),
            posts_collection(),
            photos_collection(),
        ],
        vec![
            definition("seed", "mutation"),
            definition("put_comment", "mutation"),
            definition("drop_post", "mutation"),
            definition("read_comment", "query"),
            definition("read_post", "query"),
        ],
        vec![],
    );
    let runtime = ClosureRuntime::new(
        |kind: CallKind, _module: &str, name: &str, args: &Value, host: &mut dyn HostCall| match (
            kind, name,
        ) {
            (CallKind::Mutation, "seed") => {
                set(host, "posts", &k("p1"), obj(vec![("title", s("post"))]))?;
                set(host, "photos", &k("f1"), obj(vec![("title", s("photo"))]))?;
                set(
                    host,
                    "comments",
                    &k("c1"),
                    obj(vec![
                        ("body", s("nice")),
                        ("target_type", s("posts")),
                        ("target_id", s("p1")),
                    ]),
                )?;
                Ok(Value::Null)
            }
            (CallKind::Mutation, "put_comment") => {
                set(host, "comments", &k("c2"), args.clone())?;
                Ok(Value::Null)
            }
            (CallKind::Mutation, "drop_post") => {
                del(host, "posts", &k("p1"))?;
                Ok(Value::Null)
            }
            (CallKind::Query, "read_comment") => {
                Ok(get(host, "comments", &k("c1"))?.unwrap_or(Value::Null))
            }
            (CallKind::Query, "read_post") => {
                Ok(get(host, "posts", &k("p1"))?.unwrap_or(Value::Null))
            }
            _ => Err(fail("UNKNOWN_METHOD", name.to_string())),
        },
    );
    setup(test, manifest, Box::new(runtime))
}

#[test]
fn poly_outgoing_selects_targets() {
    let (mut engine, path) = poly_fixture("poly-out", "restrict", false);
    engine
        .invoke_mutation(b"db", "app", "seed", &Value::Null, &default_mutation())
        .unwrap();
    // A comment on the photo resolves through the same reference.
    let photo_comment = obj(vec![
        ("body", s("pretty")),
        ("target_type", s("photos")),
        ("target_id", s("f1")),
    ]);
    engine
        .invoke_mutation(
            b"db",
            "app",
            "put_comment",
            &photo_comment,
            &default_mutation(),
        )
        .unwrap();
    // An unknown discriminator fails validation.
    let bad_disc = obj(vec![
        ("body", s("lost")),
        ("target_type", s("nope")),
        ("target_id", s("p1")),
    ]);
    let failure = engine
        .invoke_mutation(b"db", "app", "put_comment", &bad_disc, &default_mutation())
        .unwrap_err();
    assert_eq!(failure.code, "foreign_key_violation");
    // A missing row in a valid target fails existence.
    let ghost = obj(vec![
        ("body", s("ghost")),
        ("target_type", s("posts")),
        ("target_id", s("missing")),
    ]);
    let failure = engine
        .invoke_mutation(b"db", "app", "put_comment", &ghost, &default_mutation())
        .unwrap_err();
    assert_eq!(failure.code, "foreign_key_violation");
    teardown(engine, path);
}

#[test]
fn poly_restrict_blocks_target_delete() {
    let (mut engine, path) = poly_fixture("poly-restrict", "restrict", false);
    engine
        .invoke_mutation(b"db", "app", "seed", &Value::Null, &default_mutation())
        .unwrap();
    let failure = engine
        .invoke_mutation(b"db", "app", "drop_post", &Value::Null, &default_mutation())
        .unwrap_err();
    assert_eq!(failure.code, "foreign_key_violation");
    let out = engine
        .invoke_query(
            b"db",
            "app",
            "read_post",
            &Value::Null,
            &CallOptions::default(),
        )
        .unwrap();
    assert_ne!(out, Value::Null);
    teardown(engine, path);
}

#[test]
fn poly_set_null_clears_both_fields() {
    let (mut engine, path) = poly_fixture("poly-setnull", "set_null", true);
    engine
        .invoke_mutation(b"db", "app", "seed", &Value::Null, &default_mutation())
        .unwrap();
    engine
        .invoke_mutation(b"db", "app", "drop_post", &Value::Null, &default_mutation())
        .unwrap();
    let out = engine
        .invoke_query(
            b"db",
            "app",
            "read_comment",
            &Value::Null,
            &CallOptions::default(),
        )
        .unwrap();
    match out {
        Value::Object(map) => {
            assert_eq!(map.get("target_type"), Some(&Value::Null));
            assert_eq!(map.get("target_id"), Some(&Value::Null));
        }
        _ => panic!("comment row is not an object"),
    }
    teardown(engine, path);
}

fn trigger_manifest() -> Value {
    manifest(
        vec![tasks_collection(None), log_collection()],
        vec![
            definition("add", "mutation"),
            definition("add_settled", "mutation"),
            definition("on_task", "mutation"),
            definition("read_log", "query"),
        ],
        vec![trigger_def("on_task", "tasks")],
    )
}

fn trigger_runtime(fail_trigger: bool) -> ClosureRuntime<impl Guest> {
    ClosureRuntime::new(
        move |kind: CallKind, _module: &str, name: &str, args: &Value, host: &mut dyn HostCall| {
            match (kind, name) {
                (CallKind::Mutation, "add") | (CallKind::Mutation, "add_settled") => {
                    set(host, "tasks", &k("t1"), obj(vec![("title", s("t1"))]))?;
                    if name == "add_settled" {
                        settle(host)?;
                    }
                    Ok(Value::Null)
                }
                (CallKind::Mutation, "on_task") => {
                    if fail_trigger {
                        return Err(fail("TRIGGER_BOOM", "trigger failed"));
                    }
                    // Change args carry collection, key and after-image.
                    let map = match args {
                        Value::Object(map) => map,
                        _ => return Err(fail("TYPE_MISMATCH", "change args are not an object")),
                    };
                    let key = match map.get("key") {
                        Some(Value::String(key)) => key.clone(),
                        _ => return Err(fail("TYPE_MISMATCH", "change key is not a string")),
                    };
                    set(host, "log", &k(&format!("saw-{key}")), args.clone())?;
                    Ok(Value::Null)
                }
                (CallKind::Query, "read_log") => {
                    let (rows, _) = scan(host, "log", 100, None)?;
                    Ok(n(rows.len() as f64))
                }
                _ => Err(fail("UNKNOWN_METHOD", name.to_string())),
            }
        },
    )
}

#[test]
fn trigger_fires_on_settle() {
    let (mut engine, path) = setup(
        "trigger-settle",
        trigger_manifest(),
        Box::new(trigger_runtime(false)),
    );
    engine
        .invoke_mutation(
            b"db",
            "app",
            "add_settled",
            &Value::Null,
            &default_mutation(),
        )
        .unwrap();
    let out = engine
        .invoke_query(
            b"db",
            "app",
            "read_log",
            &Value::Null,
            &CallOptions::default(),
        )
        .unwrap();
    assert_eq!(out, n(1.0));
    teardown(engine, path);
}

#[test]
fn trigger_fires_at_commit_without_settle() {
    let (mut engine, path) = setup(
        "trigger-commit",
        trigger_manifest(),
        Box::new(trigger_runtime(false)),
    );
    engine
        .invoke_mutation(b"db", "app", "add", &Value::Null, &default_mutation())
        .unwrap();
    let out = engine
        .invoke_query(
            b"db",
            "app",
            "read_log",
            &Value::Null,
            &CallOptions::default(),
        )
        .unwrap();
    assert_eq!(out, n(1.0));
    teardown(engine, path);
}

#[test]
fn trigger_failure_aborts_commit() {
    let manifest = manifest(
        vec![tasks_collection(None), log_collection()],
        vec![
            definition("add", "mutation"),
            definition("on_task", "mutation"),
            definition("read_tasks", "query"),
        ],
        vec![trigger_def("on_task", "tasks")],
    );
    let runtime = ClosureRuntime::new(
        |kind: CallKind, _module: &str, name: &str, _args: &Value, host: &mut dyn HostCall| match (
            kind, name,
        ) {
            (CallKind::Mutation, "add") => {
                set(host, "tasks", &k("t1"), obj(vec![("title", s("t1"))]))?;
                Ok(Value::Null)
            }
            (CallKind::Mutation, "on_task") => Err(fail("TRIGGER_BOOM", "trigger failed")),
            (CallKind::Query, "read_tasks") => {
                let (rows, _) = scan(host, "tasks", 100, None)?;
                Ok(n(rows.len() as f64))
            }
            _ => Err(fail("UNKNOWN_METHOD", name.to_string())),
        },
    );
    let (mut engine, path) = setup("trigger-fail", manifest, Box::new(runtime));
    let failure = engine
        .invoke_mutation(b"db", "app", "add", &Value::Null, &default_mutation())
        .unwrap_err();
    assert_eq!(failure.code, "TRIGGER_BOOM");
    // The abort rolled the task write back too.
    let out = engine
        .invoke_query(
            b"db",
            "app",
            "read_tasks",
            &Value::Null,
            &CallOptions::default(),
        )
        .unwrap();
    assert_eq!(out, n(0.0));
    teardown(engine, path);
}

fn derived_fixture(test: &str, evals: Arc<Mutex<u32>>) -> (Engine, std::path::PathBuf) {
    let manifest = manifest(
        vec![tasks_collection(None)],
        vec![
            definition("count_tasks", "derived"),
            definition("read", "query"),
            definition("add", "mutation"),
            definition("add_settled", "mutation"),
            definition("pin", "mutation"),
        ],
        vec![],
    );
    let runtime = ClosureRuntime::new(
        move |kind: CallKind, _module: &str, name: &str, args: &Value, host: &mut dyn HostCall| {
            match (kind, name) {
                (CallKind::Derived, "count_tasks") => {
                    *evals.lock().unwrap() += 1;
                    let (rows, _) = scan(host, "tasks", 100, None)?;
                    Ok(n(rows.len() as f64))
                }
                (CallKind::Query, "read") => {
                    let first = get_derived(host, "", "count_tasks", Value::Null)?;
                    let second = get_derived(host, "", "count_tasks", Value::Null)?;
                    assert_eq!(first, second);
                    Ok(first)
                }
                (CallKind::Mutation, "add") | (CallKind::Mutation, "add_settled") => {
                    let key = match args {
                        Value::String(key) => key.clone(),
                        _ => return Err(fail("TYPE_MISMATCH", "add takes a key string")),
                    };
                    set(host, "tasks", &key, obj(vec![("title", s(&key))]))?;
                    if name == "add_settled" {
                        settle(host)?;
                    }
                    Ok(Value::Null)
                }
                (CallKind::Mutation, "pin") => {
                    hc(
                        host,
                        HostOp::Materialize,
                        vec![s(""), s("count_tasks"), Value::Null],
                    )?;
                    Ok(Value::Null)
                }
                _ => Err(fail("UNKNOWN_METHOD", name.to_string())),
            }
        },
    );
    setup(test, manifest, Box::new(runtime))
}

fn read_count(engine: &Engine) -> f64 {
    match engine
        .invoke_query(b"db", "app", "read", &Value::Null, &CallOptions::default())
        .unwrap()
    {
        Value::Number(count) => count,
        _ => panic!("count is not a number"),
    }
}

#[test]
fn derived_caches_and_invalidates() {
    let evals = Arc::new(Mutex::new(0u32));
    let (mut engine, path) = derived_fixture("derived-inv", Arc::clone(&evals));
    assert_eq!(read_count(&engine), 0.0);
    assert_eq!(*evals.lock().unwrap(), 1);
    // The second read in the same query served the cache; a new query
    // serves the committed cache without re-evaluating.
    assert_eq!(read_count(&engine), 0.0);
    assert_eq!(*evals.lock().unwrap(), 1);
    engine
        .invoke_mutation(
            b"db",
            "app",
            "add_settled",
            &s(&k("t1")),
            &default_mutation(),
        )
        .unwrap();
    assert_eq!(read_count(&engine), 1.0);
    assert_eq!(*evals.lock().unwrap(), 2);
    teardown(engine, path);
}

#[test]
fn pinned_derived_refreshes_at_commit() {
    let evals = Arc::new(Mutex::new(0u32));
    let (mut engine, path) = derived_fixture("derived-pin", Arc::clone(&evals));
    engine
        .invoke_mutation(b"db", "app", "pin", &Value::Null, &default_mutation())
        .unwrap();
    assert_eq!(*evals.lock().unwrap(), 1);
    // No settle: the commit still re-evaluates the dropped pinned root.
    engine
        .invoke_mutation(b"db", "app", "add", &s(&k("t1")), &default_mutation())
        .unwrap();
    assert_eq!(read_count(&engine), 1.0);
    assert_eq!(*evals.lock().unwrap(), 2);
    teardown(engine, path);
}

#[test]
fn query_derived_cache_commits() {
    let evals = Arc::new(Mutex::new(0u32));
    let (engine, path) = derived_fixture("derived-qcache", Arc::clone(&evals));
    assert_eq!(read_count(&engine), 0.0);
    assert_eq!(*evals.lock().unwrap(), 1);
    // A second query invocation serves the query-committed cache.
    assert_eq!(read_count(&engine), 0.0);
    assert_eq!(*evals.lock().unwrap(), 1);
    teardown(engine, path);
}

#[test]
fn derived_cycle_fails() {
    let manifest = manifest(
        vec![],
        vec![definition("spun", "derived"), definition("read", "query")],
        vec![],
    );
    let runtime = ClosureRuntime::new(
        |kind: CallKind, _module: &str, name: &str, _args: &Value, host: &mut dyn HostCall| match (
            kind, name,
        ) {
            (CallKind::Derived, "spun") => get_derived(host, "", "spun", Value::Null),
            (CallKind::Query, "read") => get_derived(host, "", "spun", Value::Null),
            _ => Err(fail("UNKNOWN_METHOD", name.to_string())),
        },
    );
    let (engine, path) = setup("derived-cycle", manifest, Box::new(runtime));
    let failure = engine
        .invoke_query(b"db", "app", "read", &Value::Null, &CallOptions::default())
        .unwrap_err();
    assert_eq!(failure.code, "INVALID_INVOCATION");
    teardown(engine, path);
}

/// A backend with scripted planner output plus a closure handler.
struct ScriptRuntime {
    plan_out: Value,
    handler: Arc<dyn Guest + Sync>,
}

impl Runtime for ScriptRuntime {
    fn call(
        &self,
        kind: CallKind,
        _module: &str,
        name: &str,
        args: &Value,
        host: &mut dyn HostCall,
    ) -> Result<Value, flower_sdk::CallFailure> {
        (self.handler)(kind, _module, name, args, host).map_err(|failure| flower_sdk::CallFailure {
            failure,
            recovered: true,
        })
    }

    fn plan(
        &self,
        _module: &str,
        _name: &str,
        _args: &Value,
    ) -> Result<Value, flower_sdk::CallFailure> {
        Ok(self.plan_out.clone())
    }
}

fn planned(module: &str, name: &str, args: Value, savepoint: bool) -> Value {
    obj(vec![
        ("module", s(module)),
        ("name", s(name)),
        ("args", args),
        ("savepoint", Value::Bool(savepoint)),
    ])
}

fn transaction_fixture(test: &str, second: Value) -> (Engine, std::path::PathBuf) {
    let manifest = manifest(
        vec![tasks_collection(None)],
        vec![
            definition("batch", "transaction"),
            definition("put", "mutation"),
            definition("read", "query"),
        ],
        vec![],
    );
    let plan_out = arr(vec![
        planned(
            "app",
            "put",
            obj(vec![("key", s(&k("a"))), ("title", s("a"))]),
            false,
        ),
        planned("app", "put", second, false),
    ]);
    let handler: Arc<dyn Guest + Sync> = Arc::new(
        |kind: CallKind, _module: &str, name: &str, args: &Value, host: &mut dyn HostCall| match (
            kind, name,
        ) {
            (CallKind::Mutation, "put") => {
                let map = match args {
                    Value::Object(map) => map,
                    _ => return Err(fail("TYPE_MISMATCH", "put takes an object")),
                };
                let key = match map.get("key") {
                    Some(Value::String(key)) => key.clone(),
                    _ => return Err(fail("TYPE_MISMATCH", "put needs a key")),
                };
                let title = map.get("title").cloned().unwrap_or(Value::Null);
                set(host, "tasks", &key, obj(vec![("title", title)]))?;
                Ok(s("put-ok"))
            }
            (CallKind::Query, "read") => {
                let (rows, _) = scan(host, "tasks", 100, None)?;
                Ok(n(rows.len() as f64))
            }
            _ => Err(fail("UNKNOWN_METHOD", name.to_string())),
        },
    );
    let runtime = ScriptRuntime { plan_out, handler };
    setup(test, manifest, Box::new(runtime))
}

/// The budget each guest call ran under, by definition name.
type Budgets = Arc<Mutex<Vec<(String, flower_engine::runtime::wasm::Budget)>>>;

/// A planner and its planned `put`s, recording the budget each runs
/// under; every `put` sleeps `sleep` before writing.
struct Budgeted {
    calls: usize,
    sleep: std::time::Duration,
    seen: Budgets,
}

impl Runtime for Budgeted {
    fn call(
        &self,
        _kind: CallKind,
        _module: &str,
        name: &str,
        args: &Value,
        host: &mut dyn HostCall,
    ) -> Result<Value, flower_sdk::CallFailure> {
        self.seen
            .lock()
            .unwrap()
            .push((name.to_string(), flower_engine::runtime::wasm::budget()));
        std::thread::sleep(self.sleep);
        let key = match args {
            Value::Object(map) => map.get("key").cloned().unwrap_or(Value::Null),
            _ => Value::Null,
        };
        let key = key.as_str().unwrap_or("x").to_string();
        set(host, "tasks", &k(&key), obj(vec![("title", s("t"))])).map_err(|failure| {
            flower_sdk::CallFailure {
                failure,
                recovered: false,
            }
        })?;
        Ok(s("put-ok"))
    }

    fn plan(
        &self,
        _module: &str,
        name: &str,
        _args: &Value,
    ) -> Result<Value, flower_sdk::CallFailure> {
        self.seen
            .lock()
            .unwrap()
            .push((name.to_string(), flower_engine::runtime::wasm::budget()));
        Ok(arr((0..self.calls)
            .map(|index| {
                planned(
                    "app",
                    "put",
                    obj(vec![("key", s(&index.to_string()))]),
                    false,
                )
            })
            .collect()))
    }
}

/// `batch` declares `batch_ms`; `put` declares 50 ms.
fn budgeted(
    test: &str,
    batch_ms: f64,
    calls: usize,
    sleep: std::time::Duration,
) -> (Engine, std::path::PathBuf, Budgets) {
    let timed = |name: &str, kind: &str, ms: f64| {
        let Value::Object(mut def) = definition(name, kind) else {
            unreachable!()
        };
        def.insert("timeout_ms".to_string(), n(ms));
        Value::Object(def)
    };
    let manifest = manifest(
        vec![tasks_collection(None)],
        vec![
            timed("batch", "transaction", batch_ms),
            timed("put", "mutation", 50.0),
        ],
        vec![],
    );
    let seen = Arc::new(Mutex::new(Vec::new()));
    let runtime = Budgeted {
        calls,
        sleep,
        seen: seen.clone(),
    };
    let (engine, path) = setup(test, manifest, Box::new(runtime));
    (engine, path, seen)
}

#[test]
fn a_transaction_runs_under_one_deadline_and_each_call_its_own_timeout() {
    let (mut engine, path, seen) = budgeted("txn-budget", 2_000.0, 3, std::time::Duration::ZERO);
    let started = std::time::Instant::now();
    engine
        .invoke_transaction(b"db", "app", "batch", &Value::Null, &default_mutation())
        .unwrap();
    let ended = std::time::Instant::now();
    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 4, "{seen:?}");
    // The planner runs under what it declares, and sets the deadline its
    // calls share: two seconds from when the transaction began.
    let (planner, budget) = &seen[0];
    assert_eq!(planner, "batch");
    assert_eq!(
        budget.declared,
        Some(std::time::Duration::from_millis(2_000))
    );
    let deadline = budget
        .within
        .expect("the planner runs under the transaction's deadline");
    assert_eq!(budget.caller, None, "no caller deadline was given");
    let declared = std::time::Duration::from_millis(2_000);
    assert!(started + declared <= deadline && deadline <= ended + declared);
    // Each planned call keeps its own declared timeout, under that one
    // deadline.
    for (name, budget) in &seen[1..] {
        assert_eq!(name, "put");
        assert_eq!(budget.declared, Some(std::time::Duration::from_millis(50)));
        assert_eq!(budget.within, Some(deadline));
    }
    teardown(engine, path);
}

#[test]
fn planned_calls_never_outlast_their_transaction() {
    // Five calls of 80 ms each against 200 ms for the whole transaction:
    // the deadline is the transaction's, not a fresh one per call.
    let (mut engine, path, _) = budgeted(
        "txn-deadline",
        200.0,
        5,
        std::time::Duration::from_millis(80),
    );
    let failure = engine
        .invoke_transaction(b"db", "app", "batch", &Value::Null, &default_mutation())
        .unwrap_err();
    assert_eq!(failure.code, "deadline_exceeded", "{failure:?}");
    teardown(engine, path);
}

#[test]
fn transaction_commits_atomically() {
    let second = obj(vec![("key", s(&k("b"))), ("title", s("b"))]);
    let (mut engine, path) = transaction_fixture("txn-ok", second);
    let out = engine
        .invoke_transaction(b"db", "app", "batch", &Value::Null, &default_mutation())
        .unwrap();
    match out {
        Value::Array(results) if results.len() == 2 => {
            assert_eq!(results[0], s("put-ok"));
            assert_eq!(results[1], s("put-ok"));
        }
        _ => panic!("transaction result is not a pair"),
    }
    let out = engine
        .invoke_query(b"db", "app", "read", &Value::Null, &CallOptions::default())
        .unwrap();
    assert_eq!(out, n(2.0));
    teardown(engine, path);
}

#[test]
fn transaction_rolls_back_on_failure() {
    // The second write violates the schema (numeric title): the whole
    // batch aborts and neither row commits.
    let second = obj(vec![("key", s(&k("b"))), ("title", n(1.0))]);
    let (mut engine, path) = transaction_fixture("txn-abort", second);
    let failure = engine
        .invoke_transaction(b"db", "app", "batch", &Value::Null, &default_mutation())
        .unwrap_err();
    assert_eq!(failure.code, "SCHEMA_MISMATCH");
    let out = engine
        .invoke_query(b"db", "app", "read", &Value::Null, &CallOptions::default())
        .unwrap();
    assert_eq!(out, n(0.0));
    teardown(engine, path);
}

fn counting_fixture(
    test: &str,
    runs: Arc<Mutex<u32>>,
    fail_with: Option<Failure>,
) -> (Engine, std::path::PathBuf) {
    let manifest = manifest(vec![], vec![definition("counted", "mutation")], vec![]);
    let runtime = ClosureRuntime::new(
        move |_kind: CallKind,
              _module: &str,
              _name: &str,
              _args: &Value,
              _host: &mut dyn HostCall| {
            *runs.lock().unwrap() += 1;
            match &fail_with {
                Some(failure) => Err(failure.clone()),
                None => Ok(n(*runs.lock().unwrap() as f64)),
            }
        },
    );
    setup(test, manifest, Box::new(runtime))
}

#[test]
fn receipt_replays_mutation() {
    let runs = Arc::new(Mutex::new(0u32));
    let (mut engine, path) = counting_fixture("receipt-ok", Arc::clone(&runs), None);
    let options = retry_options(b"intent-1", 9);
    let first = engine
        .invoke_mutation(b"db", "app", "counted", &Value::Null, &options)
        .unwrap();
    assert_eq!(first, n(1.0));
    let second = engine
        .invoke_mutation(b"db", "app", "counted", &Value::Null, &options)
        .unwrap();
    assert_eq!(second, n(1.0));
    assert_eq!(*runs.lock().unwrap(), 1);
    teardown(engine, path);
}

#[test]
fn receipt_replays_business_failure() {
    let runs = Arc::new(Mutex::new(0u32));
    let (mut engine, path) = counting_fixture(
        "receipt-fail",
        Arc::clone(&runs),
        Some(fail("BUSY", "try again")),
    );
    let options = retry_options(b"intent-2", 9);
    let first = engine
        .invoke_mutation(b"db", "app", "counted", &Value::Null, &options)
        .unwrap_err();
    assert_eq!(first.code, "BUSY");
    let second = engine
        .invoke_mutation(b"db", "app", "counted", &Value::Null, &options)
        .unwrap_err();
    assert_eq!(second.code, "BUSY");
    assert_eq!(*runs.lock().unwrap(), 1);
    teardown(engine, path);
}

#[test]
fn identity_conflict_rejects() {
    let runs = Arc::new(Mutex::new(0u32));
    let (mut engine, path) = counting_fixture("receipt-conflict", Arc::clone(&runs), None);
    engine
        .invoke_mutation(
            b"db",
            "app",
            "counted",
            &Value::Null,
            &retry_options(b"intent-3", 9),
        )
        .unwrap();
    let failure = engine
        .invoke_mutation(
            b"db",
            "app",
            "counted",
            &Value::Null,
            &retry_options(b"intent-3", 10),
        )
        .unwrap_err();
    assert_eq!(failure.code, "identity_conflict");
    assert_eq!(*runs.lock().unwrap(), 1);
    teardown(engine, path);
}

#[test]
fn idempotency_key_dedups() {
    let runs = Arc::new(Mutex::new(0u32));
    let (mut engine, path) = counting_fixture("dedup", Arc::clone(&runs), None);
    let options = MutationOptions {
        idempotency_key: Some(b"caller-key".to_vec()),
        ..Default::default()
    };
    let first = engine
        .invoke_mutation(b"db", "app", "counted", &Value::Null, &options)
        .unwrap();
    assert_eq!(first, n(1.0));
    let second = engine
        .invoke_mutation(b"db", "app", "counted", &Value::Null, &options)
        .unwrap();
    assert_eq!(second, n(1.0));
    assert_eq!(*runs.lock().unwrap(), 1);
    teardown(engine, path);
}

fn range_fixture(test: &str) -> (Engine, std::path::PathBuf) {
    let manifest = manifest(
        vec![tasks_collection(None)],
        vec![definition("seed", "mutation"), definition("probe", "query")],
        vec![],
    );
    let runtime = ClosureRuntime::new(
        |kind: CallKind, _module: &str, name: &str, args: &Value, host: &mut dyn HostCall| {
            match (kind, name) {
                (CallKind::Mutation, "seed") => {
                    for (key, owner) in [
                        ("t1", "a"),
                        ("t2", "a"),
                        ("t3", "b"),
                        ("t4", "b"),
                        ("t5", "c"),
                    ] {
                        set(
                            host,
                            "tasks",
                            &k(key),
                            obj(vec![("title", s(key)), ("owner", s(owner))]),
                        )?;
                    }
                    Ok(Value::Null)
                }
                (CallKind::Query, "probe") => {
                    // Bounds arrive verbatim; the probe only forwards them.
                    let bounds = match args {
                        Value::Object(_) => args.clone(),
                        _ => return Err(fail("TYPE_MISMATCH", "probe takes bounds")),
                    };
                    hc(
                        host,
                        HostOp::Range,
                        vec![s(""), s("tasks"), s("by_owner"), bounds],
                    )
                }
                _ => Err(fail("UNKNOWN_METHOD", name.to_string())),
            }
        },
    );
    setup(test, manifest, Box::new(runtime))
}

/// Range bounds: equality prefix, then `[gt, gte, lt, lte]`, limit, cursor
/// and direction.
fn bounds(
    prefix: Vec<&str>,
    [gt, gte, lt, lte]: [Option<&str>; 4],
    limit: u32,
    after: Option<String>,
    reverse: bool,
) -> Value {
    let text = |item: Option<&str>| item.map(s).unwrap_or(Value::Null);
    obj(vec![
        ("prefix", arr(prefix.into_iter().map(s).collect())),
        ("gt", text(gt)),
        ("gte", text(gte)),
        ("lt", text(lt)),
        ("lte", text(lte)),
        ("limit", n(limit as f64)),
        ("after", after.map(|item| s(&item)).unwrap_or(Value::Null)),
        ("reverse", Value::Bool(reverse)),
    ])
}

fn row_keys(page: &Value) -> Vec<String> {
    let (rows, _) = decode_page(page);
    rows.into_iter()
        .map(|(key, _)| match key {
            Value::String(key) => key,
            _ => panic!("range key is not a string"),
        })
        .collect()
}

#[test]
fn range_bounds_and_pagination() {
    let (mut engine, path) = range_fixture("range");
    engine
        .invoke_mutation(b"db", "app", "seed", &Value::Null, &default_mutation())
        .unwrap();
    let probe = |engine: &Engine, bounds: Value| {
        engine
            .invoke_query(b"db", "app", "probe", &bounds, &CallOptions::default())
            .unwrap()
    };
    // Prefix equality over the owner field.
    let page = probe(
        &engine,
        bounds(vec!["\"a\""], [None, None, None, None], 10, None, false),
    );
    assert_eq!(row_keys(&page), vec!["t1".to_string(), "t2".to_string()]);
    // Prefix plus primary bounds: the prefix covers the arity, so bounds
    // apply to primary keys.
    let page = probe(
        &engine,
        bounds(
            vec!["\"b\""],
            [None, Some("\"t4\""), None, None],
            10,
            None,
            false,
        ),
    );
    assert_eq!(row_keys(&page), vec!["t4".to_string()]);
    // Reverse order flips the row sequence.
    let page = probe(
        &engine,
        bounds(vec!["\"a\""], [None, None, None, None], 10, None, true),
    );
    assert_eq!(row_keys(&page), vec!["t2".to_string(), "t1".to_string()]);
    // Pagination walks the full index two rows at a time.
    let mut cursor = None;
    let mut seen = Vec::new();
    loop {
        let page = probe(
            &engine,
            bounds(vec![], [None, None, None, None], 2, cursor, false),
        );
        let (rows, next) = decode_page(&page);
        for (key, _) in rows {
            match key {
                Value::String(key) => seen.push(key),
                _ => panic!("scan key is not a string"),
            }
        }
        cursor = next;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(seen.len(), 5);
    teardown(engine, path);
}

#[test]
fn raw_roundtrip() {
    let manifest = manifest(vec![], vec![definition("drive", "mutation")], vec![]);
    let runtime = ClosureRuntime::new(
        |_kind: CallKind, _module: &str, _name: &str, _args: &Value, host: &mut dyn HostCall| {
            set(host, "$raw/ns", "k1", s("v1"))?;
            set(host, "$raw/ns", "k2", s("v2"))?;
            // Either spelling reads the same namespace.
            let first = hc(host, HostOp::RawGet, vec![s(""), s("ns"), s("k1")])?;
            assert_eq!(first, s("v1"));
            let first = hc(host, HostOp::RawGet, vec![s(""), s("$raw/ns"), s("k1")])?;
            assert_eq!(first, s("v1"));
            let page = hc(
                host,
                HostOp::RawScan,
                vec![s(""), s("ns"), n(10.0), Value::Null],
            )?;
            let (rows, _) = decode_page(&page);
            assert_eq!(rows.len(), 2);
            hc(host, HostOp::Delete, vec![s(""), s("$raw/ns"), s("k1")])?;
            let gone = hc(host, HostOp::RawGet, vec![s(""), s("ns"), s("k1")])?;
            assert_eq!(gone, Value::Null);
            Ok(s("raw-ok"))
        },
    );
    let (mut engine, path) = setup("raw", manifest, Box::new(runtime));
    let out = engine
        .invoke_mutation(b"db", "app", "drive", &Value::Null, &default_mutation())
        .unwrap();
    assert_eq!(out, s("raw-ok"));
    teardown(engine, path);
}

#[test]
fn delete_if_match_semantics() {
    let manifest = manifest(
        vec![tasks_collection(None)],
        vec![definition("drive", "mutation")],
        vec![],
    );
    let runtime = ClosureRuntime::new(
        |_kind: CallKind, _module: &str, _name: &str, _args: &Value, host: &mut dyn HostCall| {
            let row = obj(vec![("title", s("t1"))]);
            set(host, "tasks", &k("t1"), row.clone())?;
            let missed = hc(
                host,
                HostOp::DeleteIfMatch,
                vec![
                    s(""),
                    s("tasks"),
                    s(&k("t1")),
                    obj(vec![("title", s("other"))]),
                ],
            )?;
            assert_eq!(missed, Value::Bool(false));
            assert!(get(host, "tasks", &k("t1"))?.is_some());
            let hit = hc(
                host,
                HostOp::DeleteIfMatch,
                vec![s(""), s("tasks"), s(&k("t1")), row],
            )?;
            assert_eq!(hit, Value::Bool(true));
            assert!(get(host, "tasks", &k("t1"))?.is_none());
            Ok(s("match-ok"))
        },
    );
    let (mut engine, path) = setup("dim", manifest, Box::new(runtime));
    let out = engine
        .invoke_mutation(b"db", "app", "drive", &Value::Null, &default_mutation())
        .unwrap();
    assert_eq!(out, s("match-ok"));
    teardown(engine, path);
}

#[test]
fn history_is_empty_without_replication() {
    let manifest = manifest(
        vec![tasks_collection(None)],
        vec![definition("drive", "mutation")],
        vec![],
    );
    let runtime = ClosureRuntime::new(
        |_kind: CallKind, _module: &str, _name: &str, _args: &Value, host: &mut dyn HostCall| {
            set(host, "tasks", &k("t1"), obj(vec![("title", s("t1"))]))?;
            let history = hc(
                host,
                HostOp::History,
                vec![s(""), s("tasks"), s(&k("t1")), n(10.0)],
            )?;
            assert_eq!(history, arr(vec![]));
            Ok(s("history-ok"))
        },
    );
    let (mut engine, path) = setup("history", manifest, Box::new(runtime));
    let out = engine
        .invoke_mutation(b"db", "app", "drive", &Value::Null, &default_mutation())
        .unwrap();
    assert_eq!(out, s("history-ok"));
    teardown(engine, path);
}

#[test]
fn principal_and_caller_propagate() {
    let manifest = manifest(
        vec![],
        vec![
            definition("who", "mutation"),
            definition("who_inner", "mutation"),
        ],
        vec![],
    );
    let runtime = ClosureRuntime::new(
        |kind: CallKind, _module: &str, name: &str, _args: &Value, host: &mut dyn HostCall| match (
            kind, name,
        ) {
            (CallKind::Mutation, "who") => {
                let me = hc(host, HostOp::Principal, vec![])?;
                assert_ne!(me, Value::Null);
                call(host, "", "who_inner", Value::Null, false).map_err(|failure| failure.failure)
            }
            (CallKind::Mutation, "who_inner") => {
                let caller = hc(host, HostOp::Caller, vec![])?;
                assert_eq!(caller, s("app"));
                let principal = hc(host, HostOp::Principal, vec![])?;
                let subject = match principal {
                    Value::Object(map) => map.get("subject").cloned().unwrap_or(Value::Null),
                    _ => Value::Null,
                };
                Ok(arr(vec![caller, subject]))
            }
            _ => Err(fail("UNKNOWN_METHOD", name.to_string())),
        },
    );
    let (mut engine, path) = setup("identity", manifest, Box::new(runtime));
    let principal = Principal::new("alice".to_string(), Some("t1".to_string()), None).unwrap();
    let options = MutationOptions {
        call: CallOptions {
            principal: Some(principal),
            ..Default::default()
        },
        ..Default::default()
    };
    let out = engine
        .invoke_mutation(b"db", "app", "who", &Value::Null, &options)
        .unwrap();
    assert_eq!(out, arr(vec![s("app"), s("alice")]));
    teardown(engine, path);
}

#[test]
fn fuel_follows_the_declared_deadline() {
    // A query declaring a 1 ms deadline gets the fuel of 1 ms: a loop of
    // host operations runs out of it, deterministically.
    let mut busy = definition("busy", "query");
    if let Value::Object(map) = &mut busy {
        map.insert("timeout_ms".to_string(), n(1.0));
    }
    let manifest = manifest(vec![], vec![busy, definition("echo", "query")], vec![]);
    let runtime = ClosureRuntime::new(
        |_kind: CallKind, _module: &str, name: &str, args: &Value, host: &mut dyn HostCall| {
            if name == "busy" {
                loop {
                    hc(host, HostOp::Now, vec![])?;
                }
            }
            Ok(args.clone())
        },
    );
    let (engine, path) = setup("fuel", manifest, Box::new(runtime));
    let failure = engine
        .invoke_query(b"db", "app", "busy", &Value::Null, &CallOptions::default())
        .unwrap_err();
    assert_eq!(failure.code, "budget_exhausted");
    assert_eq!(
        engine
            .invoke_query(b"db", "app", "echo", &n(1.0), &CallOptions::default())
            .unwrap(),
        n(1.0)
    );
    // A caller whose deadline passed stops at the next host operation.
    let late = CallOptions {
        deadline: Some(std::time::Instant::now()),
        ..CallOptions::default()
    };
    let failure = engine
        .invoke_query(b"db", "app", "busy", &Value::Null, &late)
        .unwrap_err();
    // The caller's, not the call's own: a retry with more time may pass.
    assert_eq!(failure.code, "caller_deadline_exceeded");
    teardown(engine, path);
}

#[test]
fn changes_at_is_recorded_and_ignored() {
    // Freshness hints have no core consumer: the lane records the
    // declaration and commits normally; replication honors it.
    let manifest = manifest(vec![], vec![definition("drive", "mutation")], vec![]);
    let runtime = ClosureRuntime::new(
        |_kind: CallKind, _module: &str, _name: &str, _args: &Value, host: &mut dyn HostCall| {
            hc(host, HostOp::ChangesAt, vec![n(1.0)])?;
            hc(host, HostOp::ChangesAt, vec![Value::Null])?;
            Ok(s("changes-ok"))
        },
    );
    let (mut engine, path) = setup("changes-at", manifest, Box::new(runtime));
    let out = engine
        .invoke_mutation(b"db", "app", "drive", &Value::Null, &default_mutation())
        .unwrap();
    assert_eq!(out, s("changes-ok"));
    teardown(engine, path);
}

#[test]
fn dispatch_validates_top_level_args() {
    let manifest = manifest(
        vec![],
        vec![definition_args("strict", "mutation", str_schema(), None)],
        vec![],
    );
    let runtime = ClosureRuntime::new(
        |_kind: CallKind, _module: &str, _name: &str, args: &Value, _host: &mut dyn HostCall| {
            Ok(args.clone())
        },
    );
    let (mut engine, path) = setup("args-top", manifest, Box::new(runtime));
    let failure = engine
        .invoke_mutation(b"db", "app", "strict", &n(1.0), &default_mutation())
        .unwrap_err();
    // The failure code includes the path to the offending value (the
    // arguments themselves here).
    assert_eq!(failure.code, "INVALID_ARGUMENT");
    assert_eq!(
        failure.details,
        Some(obj(vec![("path", Value::Array(vec![]))]))
    );
    let out = engine
        .invoke_mutation(b"db", "app", "strict", &s("ok"), &default_mutation())
        .unwrap();
    assert_eq!(out, s("ok"));
    teardown(engine, path);
}

#[test]
fn dispatch_validates_nested_call_args() {
    let manifest = manifest(
        vec![],
        vec![
            definition("outer", "mutation"),
            definition_args("inner", "mutation", str_schema(), None),
        ],
        vec![],
    );
    let runtime = ClosureRuntime::new(
        |kind: CallKind, _module: &str, name: &str, _args: &Value, host: &mut dyn HostCall| {
            match (kind, name) {
                (CallKind::Mutation, "outer") => {
                    // Invalid nested arguments recover without effects,
                    // even without a savepoint.
                    match call(host, "", "inner", n(1.0), false) {
                        Err(failure) if failure.recovered => {
                            assert_eq!(failure.failure.code, "INVALID_ARGUMENT");
                            Ok(s("outer-ok"))
                        }
                        _ => panic!("bad nested args should recover"),
                    }
                }
                (CallKind::Mutation, "inner") => panic!("inner must not run"),
                _ => Err(fail("UNKNOWN_METHOD", name.to_string())),
            }
        },
    );
    let (mut engine, path) = setup("args-nested", manifest, Box::new(runtime));
    let out = engine
        .invoke_mutation(b"db", "app", "outer", &Value::Null, &default_mutation())
        .unwrap();
    assert_eq!(out, s("outer-ok"));
    teardown(engine, path);
}

#[test]
fn dispatch_validates_derived_args() {
    let manifest = manifest(
        vec![],
        vec![
            definition_args("d", "derived", str_schema(), None),
            definition("read", "query"),
        ],
        vec![],
    );
    let runtime = ClosureRuntime::new(
        |kind: CallKind, _module: &str, name: &str, _args: &Value, host: &mut dyn HostCall| match (
            kind, name,
        ) {
            (CallKind::Query, "read") => get_derived(host, "", "d", n(1.0)),
            _ => Err(fail("UNKNOWN_METHOD", name.to_string())),
        },
    );
    let (engine, path) = setup("args-derived", manifest, Box::new(runtime));
    let failure = engine
        .invoke_query(b"db", "app", "read", &Value::Null, &CallOptions::default())
        .unwrap_err();
    assert_eq!(failure.code, "INVALID_ARGUMENT");
    teardown(engine, path);
}

#[test]
fn triggers_observe_before_and_after() {
    let manifest = manifest(
        vec![tasks_collection(None), log_collection()],
        vec![
            definition("seed", "mutation"),
            definition("bump", "mutation"),
            definition("watch", "mutation"),
            definition("read_log", "query"),
        ],
        vec![trigger_def("watch", "tasks")],
    );
    // A watching trigger that records what it saw; the log write would
    // re-fire only for log changes, which have no triggers.
    let runtime = ClosureRuntime::new(
        |kind: CallKind, _module: &str, name: &str, args: &Value, host: &mut dyn HostCall| match (
            kind, name,
        ) {
            (CallKind::Mutation, "seed") => {
                set(host, "tasks", &k("t1"), obj(vec![("title", s("v1"))]))?;
                Ok(Value::Null)
            }
            (CallKind::Mutation, "bump") => {
                set(host, "tasks", &k("t1"), obj(vec![("title", s("v2"))]))?;
                Ok(Value::Null)
            }
            (CallKind::Mutation, "watch") => {
                let map = match args {
                    Value::Object(map) => map,
                    _ => return Err(fail("TYPE_MISMATCH", "change args are not an object")),
                };
                let before = map.get("before").cloned().unwrap_or(Value::Null);
                let after = map.get("after").cloned().unwrap_or(Value::Null);
                let tag = match (&before, &after) {
                    (Value::Null, Value::Object(_)) => "insert",
                    (Value::Object(_), Value::Object(_)) => "update",
                    _ => "other",
                };
                set(host, "log", &k(&format!("saw-{tag}")), args.clone())?;
                Ok(Value::Null)
            }
            (CallKind::Query, "read_log") => {
                let (rows, _) = scan(host, "log", 100, None)?;
                Ok(n(rows.len() as f64))
            }
            _ => Err(fail("UNKNOWN_METHOD", name.to_string())),
        },
    );
    let (mut engine, path) = setup("trigger-images", manifest, Box::new(runtime));
    engine
        .invoke_mutation(b"db", "app", "seed", &Value::Null, &default_mutation())
        .unwrap();
    engine
        .invoke_mutation(b"db", "app", "bump", &Value::Null, &default_mutation())
        .unwrap();
    let out = engine
        .invoke_query(
            b"db",
            "app",
            "read_log",
            &Value::Null,
            &CallOptions::default(),
        )
        .unwrap();
    assert_eq!(out, n(2.0));
    // The bump's trigger saw the committed v1 row as its before-image.
    teardown(engine, path);
}

#[test]
fn always_derived_pins_without_materialize() {
    let manifest = manifest(
        vec![tasks_collection(None)],
        vec![
            definition_args("count_tasks", "derived", json_schema(), Some("always")),
            definition("read", "query"),
            definition("add", "mutation"),
        ],
        vec![],
    );
    let runtime = ClosureRuntime::new(
        |kind: CallKind, _module: &str, name: &str, args: &Value, host: &mut dyn HostCall| match (
            kind, name,
        ) {
            (CallKind::Derived, "count_tasks") => {
                let (rows, _) = scan(host, "tasks", 100, None)?;
                Ok(n(rows.len() as f64))
            }
            (CallKind::Query, "read") => get_derived(host, "", "count_tasks", Value::Null),
            (CallKind::Mutation, "add") => {
                let key = match args {
                    Value::String(key) => key.clone(),
                    _ => return Err(fail("TYPE_MISMATCH", "add takes a key string")),
                };
                set(host, "tasks", &key, obj(vec![("title", s(&key))]))?;
                Ok(Value::Null)
            }
            _ => Err(fail("UNKNOWN_METHOD", name.to_string())),
        },
    );
    let (mut engine, path) = setup("derived-always", manifest, Box::new(runtime));
    assert_eq!(read_count(&engine), 0.0);
    // No settle and no explicit pin: Always retention re-evaluates the
    // dropped root eagerly at commit.
    engine
        .invoke_mutation(b"db", "app", "add", &s(&k("t1")), &default_mutation())
        .unwrap();
    assert_eq!(read_count(&engine), 1.0);
    teardown(engine, path);
}

#[test]
fn commit_cascade_bound() {
    let manifest = manifest(
        vec![cells_collection()],
        vec![
            definition("seed", "mutation"),
            definition("spread", "mutation"),
            definition("read", "query"),
        ],
        vec![trigger_def("spread", "cells")],
    );
    let counter = Arc::new(Mutex::new(0u32));
    let runtime = ClosureRuntime::new(
        move |kind: CallKind, _module: &str, name: &str, _args: &Value, host: &mut dyn HostCall| {
            match (kind, name) {
                (CallKind::Mutation, "seed") => {
                    set(host, "cells", &k("c0"), obj(vec![("v", n(0.0))]))?;
                    Ok(Value::Null)
                }
                (CallKind::Mutation, "spread") => {
                    // Every firing writes a fresh key, which fires again.
                    let next = {
                        let mut counter = counter.lock().unwrap();
                        *counter += 1;
                        *counter
                    };
                    set(
                        host,
                        "cells",
                        &k(&format!("c{next}")),
                        obj(vec![("v", n(next as f64))]),
                    )?;
                    Ok(Value::Null)
                }
                (CallKind::Query, "read") => {
                    let (rows, _) = scan(host, "cells", 1000, None)?;
                    Ok(n(rows.len() as f64))
                }
                _ => Err(fail("UNKNOWN_METHOD", name.to_string())),
            }
        },
    );
    let (mut engine, path) = setup("cascade", manifest, Box::new(runtime));
    let failure = engine
        .invoke_mutation(b"db", "app", "seed", &Value::Null, &default_mutation())
        .unwrap_err();
    assert_eq!(failure.code, "cascade_too_deep");
    // The aborted cascade committed nothing, not even the seed.
    let out = engine
        .invoke_query(b"db", "app", "read", &Value::Null, &CallOptions::default())
        .unwrap();
    assert_eq!(out, n(0.0));
    teardown(engine, path);
}

#[test]
fn deploy_rejects_undefined_trigger() {
    let manifest = manifest(
        vec![tasks_collection(None)],
        vec![definition("add", "mutation")],
        vec![trigger_def("ghost", "tasks")],
    );
    let runtime = ClosureRuntime::new(
        |_kind: CallKind, _module: &str, _name: &str, _args: &Value, _host: &mut dyn HostCall| {
            Ok(Value::Null)
        },
    );
    let mut path = std::env::temp_dir();
    path.push(format!("flower-exec-deploy-{}", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let store = GroupStore::create(&path).unwrap();
    let mut engine = Engine::new(store);
    let failure = engine
        .deploy(b"db", "app", [7u8; 32], b"", &manifest, Box::new(runtime))
        .unwrap_err();
    assert_eq!(failure.code, "invalid_manifest");
    teardown(engine, path);
}

#[test]
fn deploy_rejects_bad_module() {
    let manifest = manifest(vec![], vec![definition("echo", "query")], vec![]);
    let runtime = ClosureRuntime::new(
        |_kind: CallKind, _module: &str, _name: &str, args: &Value, _host: &mut dyn HostCall| {
            Ok(args.clone())
        },
    );
    let mut path = std::env::temp_dir();
    path.push(format!("flower-exec-module-{}", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let store = GroupStore::create(&path).unwrap();
    let mut engine = Engine::new(store);
    let failure = engine
        .deploy(b"db", "a\0b", [7u8; 32], b"", &manifest, Box::new(runtime))
        .unwrap_err();
    assert_eq!(failure.code, "invalid_manifest");
    teardown(engine, path);
}

#[test]
fn a_declared_millisecond_buys_a_thousand_host_operations() {
    // A host operation at its cheapest takes about a microsecond: a
    // method's logical fuel covers its deadline's worth of them, so the
    // deadline, not the fuel, ends a loop of host calls.
    use flower_engine::engine::fuel_for;
    for ms in [1, 50, 5_000] {
        assert!(
            fuel_for(Some(ms)) >= u64::from(ms) * 1_000 * flower_engine::host::FUEL_OP,
            "{ms} ms buys {}",
            fuel_for(Some(ms))
        );
    }
    assert!(fuel_for(None) >= 5_000 * 1_000);
}
