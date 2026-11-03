//! Regression tests for audited engine defects. Each test drives the
//! scale or ordering edge that exposed its defect: same-session derived
//! staging, several databases and modules, pages past the old fixed
//! scan windows, stale patches and nested calls.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use flower_engine::engine::{
    Activation, CallOptions, Engine, MultiStep, MutationOptions, PrepareOutcome,
};
use flower_engine::lane::PreparedPatch;
use flower_engine::runtime::{CallKind, ClosureRuntime, HostCall, Runtime};
use flower_engine::storage::GroupStore;
use flower_sdk::abi::{HostOp, decode_envelope};
use flower_sdk::{Failure, Value, fail};

fn s(text: &str) -> Value {
    Value::String(text.to_string())
}

fn n(num: f64) -> Value {
    Value::Number(num)
}

fn obj(pairs: Vec<(&str, Value)>) -> Value {
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

/// Canonical key text for a string key.
fn k(name: &str) -> String {
    format!("\"{name}\"")
}

fn schema(kind: &str) -> Value {
    obj(vec![("type", s(kind))])
}

fn index(name: &str, fields: Vec<&str>) -> Value {
    obj(vec![
        ("name", s(name)),
        ("fields", arr(fields.into_iter().map(s).collect())),
    ])
}

fn collection(name: &str, key: &str, indexes: Vec<Value>, references: Vec<Value>) -> Value {
    obj(vec![
        ("name", s(name)),
        ("key", schema(key)),
        ("value", schema("json")),
        ("indexes", arr(indexes)),
        ("references", arr(references)),
    ])
}

fn definition(name: &str, kind: &str) -> Value {
    obj(vec![
        ("name", s(name)),
        ("kind", s(kind)),
        ("args", schema("json")),
    ])
}

fn manifest(collections: Vec<Value>, definitions: Vec<Value>) -> Value {
    obj(vec![
        ("collections", arr(collections)),
        ("definitions", arr(definitions)),
        ("triggers", arr(vec![])),
        ("http", arr(vec![])),
    ])
}

/// The manifest exporting `derived` values and `methods`: other modules
/// reach exports only.
fn exporting(manifest: Value, derived: &[&str], methods: &[&str]) -> Value {
    let Value::Object(mut root) = manifest else {
        unreachable!("manifests are objects");
    };
    let names = |names: &[&str]| arr(names.iter().map(|name| s(name)).collect());
    root.insert(
        "exports".to_string(),
        obj(vec![
            ("collections", arr(vec![])),
            ("derived", names(derived)),
            ("methods", names(methods)),
        ]),
    );
    Value::Object(root)
}

fn temp_store(test: &str) -> (GroupStore, std::path::PathBuf) {
    let mut path = std::env::temp_dir();
    path.push(format!("flower-regression-{test}-{}", std::process::id()));
    let _ = std::fs::remove_file(&path);
    (GroupStore::create(&path).unwrap(), path)
}

fn teardown(engine: Engine, path: std::path::PathBuf) {
    drop(engine);
    let _ = std::fs::remove_file(&path);
}

fn runtime<F>(invoke: F) -> Box<dyn Runtime>
where
    F: Fn(CallKind, &str, &str, &Value, &mut dyn HostCall) -> Result<Value, Failure>
        + Send
        + Sync
        + 'static,
{
    Box::new(ClosureRuntime::new(invoke))
}

/// One host call through a closure's handler, decoding the envelope.
fn hc(host: &mut dyn HostCall, op: HostOp, args: Vec<Value>) -> Result<Value, Failure> {
    let input = Value::Array(args).encode_binary().unwrap();
    let output = host.op(op as u32 as u8, &input);
    decode_envelope(Value::decode_binary(&output).unwrap())
}

fn set(host: &mut dyn HostCall, collection: &str, key: &str, value: Value) -> Result<(), Failure> {
    hc(host, HostOp::Set, vec![s(""), s(collection), s(key), value]).map(drop)
}

fn scan_count(host: &mut dyn HostCall, collection: &str) -> Result<Value, Failure> {
    let page = hc(
        host,
        HostOp::Scan,
        vec![s(""), s(collection), n(1000.0), Value::Null],
    )?;
    Ok(n(page_rows(&page).0.len() as f64))
}

fn get_derived(host: &mut dyn HostCall, module: &str, name: &str) -> Result<Value, Failure> {
    hc(
        host,
        HostOp::GetDerived,
        vec![s(module), s(name), Value::Null],
    )
}

/// Split a page into `(key, value)` rows and its cursor.
fn page_rows(page: &Value) -> (Vec<(Value, Value)>, Option<String>) {
    let Value::Object(map) = page else {
        panic!("page is not an object");
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
        Some(Value::String(cursor)) => Some(cursor.clone()),
        _ => None,
    };
    (rows, cursor)
}

fn text(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Number(number) => number.to_string(),
        other => panic!("not text: {other:?}"),
    }
}

fn bounds(
    prefix: Vec<&str>,
    gte: Option<&str>,
    lte: Option<&str>,
    limit: u32,
    after: Option<String>,
    reverse: bool,
) -> Value {
    let bound = |item: Option<&str>| item.map(s).unwrap_or(Value::Null);
    obj(vec![
        ("prefix", arr(prefix.into_iter().map(s).collect())),
        ("gt", Value::Null),
        ("gte", bound(gte)),
        ("lt", Value::Null),
        ("lte", bound(lte)),
        ("limit", n(limit as f64)),
        (
            "after",
            after.map(|cursor| s(&cursor)).unwrap_or(Value::Null),
        ),
        ("reverse", Value::Bool(reverse)),
    ])
}

fn timed(timestamp_ms: u64) -> MutationOptions {
    MutationOptions {
        call: CallOptions {
            timestamp_ms: Some(timestamp_ms),
            ..CallOptions::default()
        },
        ..MutationOptions::default()
    }
}

fn keyed(key: &str) -> MutationOptions {
    MutationOptions {
        idempotency_key: Some(key.as_bytes().to_vec()),
        ..MutationOptions::default()
    }
}

fn unwrap_patch(outcome: PrepareOutcome) -> PreparedPatch {
    match outcome {
        PrepareOutcome::Patch(prepared) => prepared,
        _ => panic!("prepare short-circuited"),
    }
}

/// A tasks module: `put` writes `{key, value}` rows, `count` derives
/// the row count, `read_count` serves it, `count_then_put` reads the
/// count before writing, `count_put_settle_count` reads, writes,
/// settles and reads again, `note` writes a log row after reading the
/// count.
fn tasks_module() -> (Value, Box<dyn Runtime>) {
    let manifest = manifest(
        vec![
            collection("tasks", "string", vec![], vec![]),
            collection("log", "string", vec![], vec![]),
        ],
        vec![
            definition("put", "mutation"),
            definition("count", "derived"),
            definition("read_count", "query"),
            definition("count_then_put", "mutation"),
            definition("count_put_settle_count", "mutation"),
            definition("note", "mutation"),
        ],
    );
    let runtime = runtime(|kind, _module, name, args, host| match (kind, name) {
        (CallKind::Mutation, "put") => {
            set(host, "tasks", &k(&text(args)), obj(vec![("v", n(1.0))]))?;
            Ok(Value::Null)
        }
        (CallKind::Derived, "count") => scan_count(host, "tasks"),
        (CallKind::Query, "read_count") => get_derived(host, "", "count"),
        (CallKind::Mutation, "count_then_put") => {
            let before = get_derived(host, "", "count")?;
            set(host, "tasks", &k(&text(args)), obj(vec![("v", n(1.0))]))?;
            Ok(before)
        }
        (CallKind::Mutation, "count_put_settle_count") => {
            get_derived(host, "", "count")?;
            set(host, "tasks", &k(&text(args)), obj(vec![("v", n(1.0))]))?;
            hc(host, HostOp::Settle, vec![])?;
            get_derived(host, "", "count")
        }
        (CallKind::Mutation, "note") => {
            let seen = get_derived(host, "", "count")?;
            set(host, "log", &k("seen"), seen.clone())?;
            Ok(seen)
        }
        _ => Err(fail("UNKNOWN_METHOD", name.to_string())),
    });
    (manifest, runtime)
}

fn tasks_engine(test: &str) -> (Engine, std::path::PathBuf) {
    let (store, path) = temp_store(test);
    let mut engine = Engine::new(store);
    let (manifest, runtime) = tasks_module();
    engine
        .deploy(b"db", "app", [7u8; 32], b"", &manifest, runtime)
        .unwrap();
    (engine, path)
}

fn read_count(engine: &Engine) -> Value {
    engine
        .invoke_query(
            b"db",
            "app",
            "read_count",
            &Value::Null,
            &CallOptions::default(),
        )
        .unwrap()
}

// Finding 1: a derived value evaluated earlier in the same session has
// its edges only in the overlay; a later write must still drop it.
#[test]
fn derived_read_before_a_write_does_not_commit_stale() {
    let (mut engine, path) = tasks_engine("derived-overlay");
    let before = engine
        .invoke_mutation(
            b"db",
            "app",
            "count_then_put",
            &s("t1"),
            &MutationOptions::default(),
        )
        .unwrap();
    assert_eq!(before, n(0.0));
    assert_eq!(read_count(&engine), n(1.0));
    // Settle drops it mid-execution too.
    let after = engine
        .invoke_mutation(
            b"db",
            "app",
            "count_put_settle_count",
            &s("t2"),
            &MutationOptions::default(),
        )
        .unwrap();
    assert_eq!(after, n(2.0));
    assert_eq!(read_count(&engine), n(2.0));
    teardown(engine, path);
}

// Finding 2: a patch pins only the modules of its own database; a
// module deployed in another database must not fail the apply.
#[test]
fn patches_pin_only_their_own_databases_modules() {
    let (mut engine, path) = tasks_engine("versions-multi-db");
    let (manifest, runtime) = tasks_module();
    engine
        .deploy(b"other", "elsewhere", [9u8; 32], b"", &manifest, runtime)
        .unwrap();
    let prepared = unwrap_patch(
        engine
            .prepare_mutation(b"db", "app", "put", &s("t1"), &MutationOptions::default())
            .unwrap(),
    );
    let outcome = engine.apply_patch(&prepared.patch, 1).unwrap();
    assert!(outcome.result.is_ok(), "{:?}", outcome.result);
    assert_eq!(read_count(&engine), n(1.0));
    teardown(engine, path);
}

// Finding 2: a deployment committed between prepare and apply rejects
// multi-database patches deterministically as stale, never as an error.
#[test]
fn deploys_between_prepare_and_apply_reject_multi_patches_as_stale() {
    let (mut engine, path) = tasks_engine("versions-stale-multi");
    let (manifest, runtime) = tasks_module();
    engine
        .deploy(b"db2", "app", [7u8; 32], b"", &manifest, runtime)
        .unwrap();
    let step = |db: &[u8]| MultiStep {
        db: db.to_vec(),
        module: "app".to_string(),
        method: "put".to_string(),
        args: s("t1"),
    };
    let prepared = unwrap_patch(
        engine
            .prepare_transaction_multi(&[step(b"db"), step(b"db2")], &MutationOptions::default())
            .unwrap(),
    );
    let (manifest, runtime) = tasks_module();
    engine
        .deploy(b"db2", "app", [8u8; 32], b"v2", &manifest, runtime)
        .unwrap();
    let outcome = engine.apply_multi_patch(&prepared.patch, 1).unwrap();
    assert_eq!(outcome.results.len(), 1);
    assert_eq!(
        outcome.results[0].as_ref().unwrap_err().code,
        "stale_revision"
    );
    assert_eq!(read_count(&engine), n(0.0));
    teardown(engine, path);
}

/// A tagged-tasks module at one version: `version` answers the version,
/// `put` writes `{tag}` rows and v2 adds the `by_tag` index that
/// `tagged` ranges over.
fn tagged_module(version: &'static str) -> (Value, Box<dyn Runtime>) {
    let indexes = if version == "v2" {
        vec![index("by_tag", vec!["tag"])]
    } else {
        vec![]
    };
    let manifest = manifest(
        vec![collection("tasks", "string", indexes, vec![])],
        vec![
            definition("version", "query"),
            definition("put", "mutation"),
            definition("tagged", "query"),
        ],
    );
    let runtime = runtime(move |kind, _module, name, args, host| match (kind, name) {
        (CallKind::Query, "version") => Ok(s(version)),
        (CallKind::Mutation, "put") => {
            set(host, "tasks", &k(&text(args)), obj(vec![("tag", s("x"))]))?;
            Ok(Value::Null)
        }
        (CallKind::Query, "tagged") => {
            let page = hc(
                host,
                HostOp::Range,
                vec![
                    s(""),
                    s("tasks"),
                    s("by_tag"),
                    bounds(vec!["\"x\""], None, None, 100, None, false),
                ],
            )?;
            Ok(n(page_rows(&page).0.len() as f64))
        }
        _ => Err(fail("UNKNOWN_METHOD", name.to_string())),
    });
    (manifest, runtime)
}

// Finding 3: deploying v2 to database A leaves database B on v1, so B's
// later deploy of v2 diffs v1 against v2 and backfills.
#[test]
fn deploys_stay_within_their_database() {
    let (store, path) = temp_store("per-db-modules");
    let mut engine = Engine::new(store);
    for db in [&b"a"[..], b"b"] {
        let (manifest, runtime) = tagged_module("v1");
        engine
            .deploy(db, "app", [1u8; 32], b"", &manifest, runtime)
            .unwrap();
    }
    let put = |engine: &mut Engine, key: &str| {
        engine
            .invoke_mutation(b"b", "app", "put", &s(key), &MutationOptions::default())
            .unwrap();
    };
    put(&mut engine, "t1");
    let (manifest, runtime) = tagged_module("v2");
    engine
        .deploy(b"a", "app", [2u8; 32], b"", &manifest, runtime)
        .unwrap();
    let version = |engine: &Engine, db: &[u8]| {
        engine
            .invoke_query(db, "app", "version", &Value::Null, &CallOptions::default())
            .unwrap()
    };
    assert_eq!(version(&engine, b"a"), s("v2"));
    assert_eq!(version(&engine, b"b"), s("v1"));
    put(&mut engine, "t2");
    let (manifest, runtime) = tagged_module("v2");
    engine
        .deploy(b"b", "app", [2u8; 32], b"", &manifest, runtime)
        .unwrap();
    let tagged = engine
        .invoke_query(b"b", "app", "tagged", &Value::Null, &CallOptions::default())
        .unwrap();
    assert_eq!(tagged, n(2.0));
    teardown(engine, path);
}

// Finding 4: an idempotency key reused in another database executes
// there instead of replaying the first database's result.
#[test]
fn idempotency_keys_are_scoped_by_database() {
    let (store, path) = temp_store("dedup-per-db");
    let mut engine = Engine::new(store);
    for db in [&b"a"[..], b"b"] {
        let (manifest, runtime) = tasks_module();
        engine
            .deploy(db, "app", [7u8; 32], b"", &manifest, runtime)
            .unwrap();
    }
    engine
        .invoke_mutation(b"a", "app", "put", &s("t0"), &MutationOptions::default())
        .unwrap();
    let options = keyed("req-1");
    let first = engine
        .invoke_mutation(b"a", "app", "count_then_put", &s("t1"), &options)
        .unwrap();
    assert_eq!(first, n(1.0));
    // Same key in database b: executes there.
    let answer = engine
        .invoke_mutation(b"b", "app", "count_then_put", &s("t1"), &options)
        .unwrap();
    assert_eq!(answer, n(0.0));
    let count = engine
        .invoke_query(
            b"b",
            "app",
            "read_count",
            &Value::Null,
            &CallOptions::default(),
        )
        .unwrap();
    assert_eq!(count, n(1.0));
    // Still replays inside database a.
    let replay = engine
        .invoke_mutation(b"a", "app", "count_then_put", &s("t9"), &options)
        .unwrap();
    assert_eq!(replay, n(1.0));
    teardown(engine, path);
}

/// A ranked-tasks module: `seed` writes rows with owner `a` and the
/// given ranks; `probe` forwards range bounds over `by_owner` or
/// `by_rank`.
fn ranked_engine(test: &str) -> (Engine, std::path::PathBuf) {
    let manifest = manifest(
        vec![collection(
            "tasks",
            "string",
            vec![
                index("by_owner", vec!["owner"]),
                index("by_rank", vec!["rank"]),
            ],
            vec![],
        )],
        vec![definition("seed", "mutation"), definition("probe", "query")],
    );
    let runtime = runtime(|kind, _module, name, args, host| match (kind, name) {
        (CallKind::Mutation, "seed") => {
            for item in match args {
                Value::Array(items) => items.clone(),
                _ => return Err(fail("TYPE_MISMATCH", "seed takes keys")),
            } {
                let key = text(&item);
                let rank = key.trim_start_matches('t').parse::<f64>().unwrap_or(0.0);
                set(
                    host,
                    "tasks",
                    &k(&key),
                    obj(vec![("owner", s("a")), ("rank", n(rank))]),
                )?;
            }
            Ok(Value::Null)
        }
        (CallKind::Query, "probe") => {
            let Value::Array(parts) = args else {
                return Err(fail("TYPE_MISMATCH", "probe takes [index, bounds]"));
            };
            hc(
                host,
                HostOp::Range,
                vec![s(""), s("tasks"), parts[0].clone(), parts[1].clone()],
            )
        }
        _ => Err(fail("UNKNOWN_METHOD", name.to_string())),
    });
    let (store, path) = temp_store(test);
    let mut engine = Engine::new(store);
    engine
        .deploy(b"db", "app", [7u8; 32], b"", &manifest, runtime)
        .unwrap();
    (engine, path)
}

/// Page a range to exhaustion, collecting row keys.
fn range_all(engine: &Engine, index: &str, page: impl Fn(Option<String>) -> Value) -> Vec<String> {
    let mut cursor = None;
    let mut keys = Vec::new();
    loop {
        let args = arr(vec![s(index), page(cursor.take())]);
        let result = engine
            .invoke_query(b"db", "app", "probe", &args, &CallOptions::default())
            .unwrap();
        let (rows, next) = page_rows(&result);
        keys.extend(rows.iter().map(|(key, _)| text(key)));
        match next {
            Some(next) => cursor = Some(next),
            None => return keys,
        }
    }
}

// Finding 5: range bounds narrow the storage scan itself, so a bounded
// range pages through every matching entry instead of an early empty
// page with no cursor.
#[test]
fn range_bounds_page_through_the_storage_scan() {
    let (mut engine, path) = ranked_engine("range-bounds");
    let keys: Vec<Value> = (10..30).map(|i| s(&format!("t{i}"))).collect();
    engine
        .invoke_mutation(
            b"db",
            "app",
            "seed",
            &arr(keys),
            &MutationOptions::default(),
        )
        .unwrap();
    let forward = range_all(&engine, "by_owner", |after| {
        bounds(vec!["\"a\""], Some("\"t15\""), None, 2, after, false)
    });
    let expected: Vec<String> = (15..30).map(|i| format!("t{i}")).collect();
    assert_eq!(forward, expected);
    let backward = range_all(&engine, "by_owner", |after| {
        bounds(vec!["\"a\""], None, Some("\"t14\""), 2, after, true)
    });
    let expected: Vec<String> = (10..15).rev().map(|i| format!("t{i}")).collect();
    assert_eq!(backward, expected);
    // Bounds compare in stored order — length, then bytes — so numeric
    // ranks range numerically.
    engine
        .invoke_mutation(
            b"db",
            "app",
            "seed",
            &arr(vec![s("t9"), s("t100")]),
            &MutationOptions::default(),
        )
        .unwrap();
    let ranked = range_all(&engine, "by_rank", |after| {
        bounds(vec![], Some("25"), None, 2, after, false)
    });
    assert_eq!(ranked, vec!["t25", "t26", "t27", "t28", "t29", "t100"]);
    teardown(engine, path);
}

// Finding 6: raw scans read within their namespace even when another
// namespace's rows sort first in the shared `$raw` collection.
#[test]
fn raw_scans_page_within_their_namespace() {
    let manifest = manifest(
        vec![],
        vec![definition("fill", "mutation"), definition("scan", "query")],
    );
    let runtime = runtime(|kind, _module, name, args, host| match (kind, name) {
        (CallKind::Mutation, "fill") => {
            for i in 0..30 {
                set(host, "$raw/aa", &format!("k{i}"), s("v"))?;
            }
            // Shorter framed keys: these sort before every `aa` row.
            for i in 0..5 {
                set(host, "$raw/b", &format!("x{i}"), s("w"))?;
            }
            Ok(Value::Null)
        }
        (CallKind::Query, "scan") => {
            let namespace = text(args);
            let mut keys = Vec::new();
            let mut cursor = Value::Null;
            loop {
                let page = hc(
                    host,
                    HostOp::RawScan,
                    vec![s(""), s(&namespace), n(4.0), cursor],
                )?;
                let (rows, next) = page_rows(&page);
                keys.extend(rows.into_iter().map(|(key, _)| key));
                match next {
                    Some(next) => cursor = s(&next),
                    None => return Ok(arr(keys)),
                }
            }
        }
        _ => Err(fail("UNKNOWN_METHOD", name.to_string())),
    });
    let (store, path) = temp_store("raw-namespace");
    let mut engine = Engine::new(store);
    engine
        .deploy(b"db", "app", [7u8; 32], b"", &manifest, runtime)
        .unwrap();
    engine
        .invoke_mutation(
            b"db",
            "app",
            "fill",
            &Value::Null,
            &MutationOptions::default(),
        )
        .unwrap();
    let scan = |namespace: &str| match engine
        .invoke_query(b"db", "app", "scan", &s(namespace), &CallOptions::default())
        .unwrap()
    {
        Value::Array(keys) => keys.iter().map(text).collect::<Vec<_>>(),
        other => panic!("scan returned {other:?}"),
    };
    // Length-prefixed order: the two-character keys, then the rest.
    let mut expected: Vec<String> = (0..10).map(|i| format!("k{i}")).collect();
    expected.extend((10..30).map(|i| format!("k{i}")));
    assert_eq!(scan("aa"), expected);
    assert_eq!(scan("b").len(), 5);
    teardown(engine, path);
}

/// The staged-app module for dual-write: `add` writes a titled task,
/// `titled` ranges `by_title` once v2 adds it.
fn titled_module(version: &'static str) -> (Value, Box<dyn Runtime>) {
    let indexes = if version == "v2" {
        vec![index("by_title", vec!["title"])]
    } else {
        vec![]
    };
    let manifest = exporting(
        manifest(
            vec![collection("tasks", "string", indexes, vec![])],
            vec![definition("add", "mutation"), definition("titled", "query")],
        ),
        &[],
        &["add"],
    );
    let runtime = runtime(|kind, _module, name, args, host| match (kind, name) {
        (CallKind::Mutation, "add") => {
            set(
                host,
                "tasks",
                &k(&text(args)),
                obj(vec![("title", s("same"))]),
            )?;
            Ok(Value::Null)
        }
        (CallKind::Query, "titled") => {
            let page = hc(
                host,
                HostOp::Range,
                vec![
                    s(""),
                    s("tasks"),
                    s("by_title"),
                    bounds(vec!["\"same\""], None, None, 100, None, false),
                ],
            )?;
            Ok(arr(page_rows(&page)
                .0
                .into_iter()
                .map(|(key, _)| key)
                .collect()))
        }
        _ => Err(fail("UNKNOWN_METHOD", name.to_string())),
    });
    (manifest, runtime)
}

// Finding 7: the dual-write cache resolves each module's staging, so a
// cross-module write after another module's write still dual-writes.
#[test]
fn cross_module_writes_dual_write_during_staging() {
    let (store, path) = temp_store("dual-write-cross-module");
    let mut engine = Engine::new(store);
    let (manifest, runtime) = titled_module("v1");
    engine
        .deploy(b"db", "app", [1u8; 32], b"", &manifest, runtime)
        .unwrap();
    let notes = self::manifest(
        vec![collection("notes", "string", vec![], vec![])],
        vec![definition("write_both", "mutation")],
    );
    let notes_runtime = self::runtime(|_kind, _module, name, args, host| {
        if name != "write_both" {
            return Err(fail("UNKNOWN_METHOD", name.to_string()));
        }
        set(host, "notes", &k("n1"), obj(vec![]))?;
        hc(
            host,
            HostOp::Call,
            vec![s("app"), s("add"), args.clone(), Value::Bool(false)],
        )?;
        Ok(Value::Null)
    });
    engine
        .deploy(b"db", "notes", [3u8; 32], b"", &notes, notes_runtime)
        .unwrap();
    engine
        .invoke_mutation(b"db", "app", "add", &s("t1"), &MutationOptions::default())
        .unwrap();
    let (manifest, _) = titled_module("v2");
    engine
        .deploy_staged(b"db", "app", [2u8; 32], b"", &manifest)
        .unwrap();
    assert!(engine.staging_drain(b"db", "app", 1000).unwrap().done);
    // Behind the drain cursor: only dual-write can index this row.
    engine
        .invoke_mutation(
            b"db",
            "notes",
            "write_both",
            &s("t0"),
            &MutationOptions::default(),
        )
        .unwrap();
    let (_, runtime) = titled_module("v2");
    engine.activate(b"db", "app", runtime).unwrap();
    let titled = engine
        .invoke_query(
            b"db",
            "app",
            "titled",
            &Value::Null,
            &CallOptions::default(),
        )
        .unwrap();
    assert_eq!(titled, arr(vec![s("t0"), s("t1")]));
    teardown(engine, path);
}

/// An owned-tasks module: `own` writes `{owner}` rows; `owned` ranges
/// `by_owner` when the version carries the index.
fn owned_module(indexed: bool) -> (Value, Box<dyn Runtime>) {
    let indexes = if indexed {
        vec![index("by_owner", vec!["owner"])]
    } else {
        vec![]
    };
    let manifest = manifest(
        vec![collection("tasks", "string", indexes, vec![])],
        vec![definition("own", "mutation"), definition("owned", "query")],
    );
    let runtime = runtime(|kind, _module, name, args, host| match (kind, name) {
        (CallKind::Mutation, "own") => {
            let Value::Array(pair) = args else {
                return Err(fail("TYPE_MISMATCH", "own takes [key, owner]"));
            };
            set(
                host,
                "tasks",
                &k(&text(&pair[0])),
                obj(vec![("owner", pair[1].clone())]),
            )?;
            Ok(Value::Null)
        }
        (CallKind::Query, "owned") => {
            let prefix = format!("\"{}\"", text(args));
            let page = hc(
                host,
                HostOp::Range,
                vec![
                    s(""),
                    s("tasks"),
                    s("by_owner"),
                    bounds(vec![&prefix], None, None, 100, None, false),
                ],
            )?;
            Ok(arr(page_rows(&page)
                .0
                .into_iter()
                .map(|(key, _)| key)
                .collect()))
        }
        _ => Err(fail("UNKNOWN_METHOD", name.to_string())),
    });
    (manifest, runtime)
}

// Finding 8: re-adding an index through the replicated activation path
// purges its retired entries and cancels their retirement.
#[test]
fn replicated_reactivation_purges_retired_entries() {
    let (store, path) = temp_store("readd-replicated");
    let mut engine = Engine::new(store);
    let (manifest, runtime) = owned_module(true);
    engine
        .deploy(b"db", "app", [1u8; 32], b"", &manifest, runtime)
        .unwrap();
    let own = |engine: &mut Engine, key: &str, owner: &str| {
        engine
            .invoke_mutation(
                b"db",
                "app",
                "own",
                &arr(vec![s(key), s(owner)]),
                &MutationOptions::default(),
            )
            .unwrap();
    };
    own(&mut engine, "t1", "a");
    own(&mut engine, "t2", "b");
    let activate = |engine: &mut Engine, indexed: bool, identity: u8| {
        let (manifest, runtime) = owned_module(indexed);
        let activation = Activation {
            identity: [identity; 32],
            bindings: Vec::new(),
        };
        engine
            .apply_activation(b"db", "app", b"", &manifest, &activation, runtime)
            .unwrap();
    };
    activate(&mut engine, false, 2);
    // Unindexed now: the retired entry (a, t1) goes stale.
    own(&mut engine, "t1", "c");
    activate(&mut engine, true, 3);
    let owned = |engine: &Engine, owner: &str| {
        engine
            .invoke_query(b"db", "app", "owned", &s(owner), &CallOptions::default())
            .unwrap()
    };
    assert_eq!(owned(&engine, "c"), arr(vec![s("t1")]));
    assert_eq!(owned(&engine, "a"), arr(vec![]));
    while !engine.staging_gc(b"db", "app", 1000).unwrap().done {}
    assert_eq!(owned(&engine, "c"), arr(vec![s("t1")]));
    assert_eq!(owned(&engine, "b"), arr(vec![s("t2")]));
    teardown(engine, path);
}

/// A timer module: `schedule` schedules `[id, due]` pairs in one
/// mutation; `fire` is the handler.
fn timer_engine(test: &str) -> (Engine, std::path::PathBuf) {
    let manifest = manifest(
        vec![],
        vec![
            definition("schedule", "mutation"),
            definition("fire", "mutation"),
        ],
    );
    let runtime = runtime(|kind, _module, name, args, host| match (kind, name) {
        (CallKind::Mutation, "schedule") => {
            let Value::Array(items) = args else {
                return Err(fail("TYPE_MISMATCH", "schedule takes pairs"));
            };
            for item in items {
                let Value::Array(pair) = item else {
                    return Err(fail("TYPE_MISMATCH", "schedule takes pairs"));
                };
                hc(
                    host,
                    HostOp::TimerSchedule,
                    vec![
                        pair[0].clone(),
                        pair[1].clone(),
                        s("fire"),
                        Value::Null,
                        n(3.0),
                    ],
                )?;
            }
            Ok(Value::Null)
        }
        (CallKind::Mutation, "fire") => Ok(Value::Null),
        _ => Err(fail("UNKNOWN_METHOD", name.to_string())),
    });
    let (store, path) = temp_store(test);
    let mut engine = Engine::new(store);
    engine
        .deploy(b"db", "app", [7u8; 32], b"", &manifest, runtime)
        .unwrap();
    (engine, path)
}

// Finding 9: timers scan in true due order and resolve by ID however
// many rows sort before them: 1100 far-future timers with short IDs
// used to fill the 1024-row window ahead of due ones.
#[test]
fn timers_past_the_old_scan_window_stay_reachable() {
    let (mut engine, path) = timer_engine("timer-window");
    let future: Vec<Value> = (0..1100)
        .map(|i| arr(vec![s(&format!("f{i}")), n(1_000_000_000.0 + i as f64)]))
        .collect();
    engine
        .invoke_mutation(
            b"db",
            "app",
            "schedule",
            &arr(future),
            &MutationOptions::default(),
        )
        .unwrap();
    let due: Vec<Value> = (0..3)
        .map(|i| {
            arr(vec![
                s(&format!("due-timer-with-a-long-id-{i}")),
                n(10.0 - i as f64),
            ])
        })
        .collect();
    engine
        .invoke_mutation(
            b"db",
            "app",
            "schedule",
            &arr(due),
            &MutationOptions::default(),
        )
        .unwrap();
    let listed: Vec<String> = engine
        .list_due_timers(b"db", 100, 10)
        .unwrap()
        .into_iter()
        .map(|timer| timer.id)
        .collect();
    assert_eq!(
        listed,
        vec![
            "due-timer-with-a-long-id-2",
            "due-timer-with-a-long-id-1",
            "due-timer-with-a-long-id-0"
        ]
    );
    // Lookups by ID reach every timer.
    assert!(
        engine
            .cancel_timer(b"db", "due-timer-with-a-long-id-1")
            .unwrap()
    );
    assert!(
        engine
            .get_timer(b"db", "due-timer-with-a-long-id-1")
            .unwrap()
            .is_none()
    );
    assert!(engine.get_timer(b"db", "f1099").unwrap().is_some());
    // Rescheduling an ID replaces it rather than duplicating it.
    engine
        .invoke_mutation(
            b"db",
            "app",
            "schedule",
            &arr(vec![arr(vec![s("f1099"), n(50.0)])]),
            &MutationOptions::default(),
        )
        .unwrap();
    assert_eq!(
        engine.get_timer(b"db", "f1099").unwrap().unwrap().due_ms,
        50
    );
    assert_eq!(engine.timer_stats(b"db").unwrap().pending, 1102);
    let listed = engine.list_due_timers(b"db", 100, 10).unwrap();
    assert_eq!(listed.len(), 3);
    assert_eq!(listed[2].id, "f1099");
    teardown(engine, path);
}

// Finding 9: one queue's poll never reads another queue's rows, and
// lookups by ID reach items far past the old 1024-row window.
#[test]
fn queues_poll_and_resolve_past_the_old_scan_window() {
    let mut manifest = manifest(vec![], vec![definition("enqueue", "mutation")]);
    if let Value::Object(map) = &mut manifest {
        map.insert(
            "queues".to_string(),
            arr(vec![
                obj(vec![("name", s("a"))]),
                obj(vec![("name", s("emails"))]),
            ]),
        );
    }
    let runtime = runtime(|_kind, _module, name, args, host| {
        if name != "enqueue" {
            return Err(fail("UNKNOWN_METHOD", name.to_string()));
        }
        let Value::Array(pair) = args else {
            return Err(fail("TYPE_MISMATCH", "enqueue takes [queue, count]"));
        };
        let count = match pair[1] {
            Value::Number(count) => count as usize,
            _ => return Err(fail("TYPE_MISMATCH", "count is a number")),
        };
        let mut ids = Vec::new();
        for i in 0..count {
            ids.push(hc(
                host,
                HostOp::Enqueue,
                vec![pair[0].clone(), n(i as f64), n(0.0), n(3.0)],
            )?);
        }
        Ok(arr(ids))
    });
    let (store, path) = temp_store("queue-window");
    let mut engine = Engine::new(store);
    engine
        .deploy(b"db", "app", [7u8; 32], b"", &manifest, runtime)
        .unwrap();
    let enqueue = |engine: &mut Engine, queue: &str, count: usize| -> Vec<String> {
        match engine
            .invoke_mutation(
                b"db",
                "app",
                "enqueue",
                &arr(vec![s(queue), n(count as f64)]),
                &timed(1_000),
            )
            .unwrap()
        {
            Value::Array(ids) => ids.iter().map(text).collect(),
            other => panic!("enqueue returned {other:?}"),
        }
    };
    // A shorter queue name used to sort every one of its rows first.
    let others = enqueue(&mut engine, "a", 1100);
    enqueue(&mut engine, "emails", 3);
    let claimed = engine
        .poll_queue(b"db", "emails", 2_000, 10, 60_000)
        .unwrap();
    assert_eq!(claimed.len(), 3);
    // Every item resolves by ID, wherever it sorts.
    for id in &others {
        assert!(
            engine.get_queue_item(b"db", "a", id).unwrap().is_some(),
            "{id}"
        );
    }
    let claimed = engine.poll_queue(b"db", "a", 2_000, 100, 60_000).unwrap();
    assert_eq!(claimed.len(), 100);
    for item in &claimed {
        engine
            .complete_queue(b"db", "a", &item.id, &item.token, 2_000)
            .unwrap();
    }
    assert_eq!(engine.queue_stats(b"db", "a").unwrap().pending, 1000);
    teardown(engine, path);
}

// Finding 11: failures without a durable receipt (an exhausted budget
// or deadline, stale revisions) are never replayed from memory.
#[test]
fn keyed_failures_without_receipts_re_execute() {
    let (mut engine, path) = tasks_engine("dedup-no-receipt");
    let mut starved = keyed("req-fuel");
    starved.call.deadline = Some(std::time::Instant::now());
    let failure = engine
        .invoke_mutation(b"db", "app", "put", &s("t1"), &starved)
        .unwrap_err();
    assert_eq!(failure.code, "caller_deadline_exceeded");
    let answer = engine.invoke_mutation(b"db", "app", "put", &s("t1"), &keyed("req-fuel"));
    assert_eq!(answer.unwrap(), Value::Null);
    assert_eq!(read_count(&engine), n(1.0));
    let mut stale = keyed("req-stale");
    stale.expected_revision = Some(0);
    let failure = engine
        .invoke_mutation(b"db", "app", "put", &s("t2"), &stale)
        .unwrap_err();
    assert_eq!(failure.code, "stale_revision");
    stale.expected_revision = Some(1);
    engine
        .invoke_mutation(b"db", "app", "put", &s("t2"), &stale)
        .unwrap();
    assert_eq!(read_count(&engine), n(2.0));
    teardown(engine, path);
}

// Finding 12: a patch prepared on an older snapshot never installs its
// derived outcomes once other commits moved the database.
#[test]
fn stale_patches_do_not_install_derived_outcomes() {
    let (mut engine, path) = tasks_engine("stale-derived");
    let writes = unwrap_patch(
        engine
            .prepare_mutation(b"db", "app", "put", &s("t1"), &MutationOptions::default())
            .unwrap(),
    );
    // Same snapshot: evaluates count = 0 and writes only the log.
    let reads = unwrap_patch(
        engine
            .prepare_mutation(
                b"db",
                "app",
                "note",
                &Value::Null,
                &MutationOptions::default(),
            )
            .unwrap(),
    );
    engine.apply_patch(&writes.patch, 1).unwrap();
    engine.apply_patch(&reads.patch, 2).unwrap();
    assert_eq!(read_count(&engine), n(1.0));
    teardown(engine, path);
}

// Finding 12: outcomes cached by queries between prepare and apply
// invalidate when the patch applies.
#[test]
fn queries_cached_between_prepare_and_apply_invalidate() {
    let (mut engine, path) = tasks_engine("query-race");
    let writes = unwrap_patch(
        engine
            .prepare_mutation(b"db", "app", "put", &s("t1"), &MutationOptions::default())
            .unwrap(),
    );
    assert_eq!(read_count(&engine), n(0.0));
    engine.apply_patch(&writes.patch, 1).unwrap();
    assert_eq!(read_count(&engine), n(1.0));
    teardown(engine, path);
}

// Finding 13: guest call nesting is capped and fails deterministically
// as a fatal failure instead of overflowing the native stack.
#[test]
fn call_nesting_is_capped() {
    let worker = std::thread::Builder::new()
        .stack_size(64 << 20)
        .spawn(|| {
            let depth = Arc::new(AtomicUsize::new(0));
            let deepest = Arc::new(AtomicUsize::new(0));
            let manifest = manifest(vec![], vec![definition("recurse", "mutation")]);
            let (seen, max) = (depth.clone(), deepest.clone());
            let runtime = runtime(move |_kind, _module, _name, _args, host| {
                let level = seen.fetch_add(1, Ordering::SeqCst) + 1;
                max.fetch_max(level, Ordering::SeqCst);
                // Savepoint calls would recover ordinary failures.
                let outcome = hc(
                    host,
                    HostOp::Call,
                    vec![s(""), s("recurse"), Value::Null, Value::Bool(true)],
                );
                seen.fetch_sub(1, Ordering::SeqCst);
                outcome
            });
            let (store, path) = temp_store("call-depth");
            let mut engine = Engine::new(store);
            engine
                .deploy(b"db", "app", [7u8; 32], b"", &manifest, runtime)
                .unwrap();
            let failure = engine
                .invoke_mutation(
                    b"db",
                    "app",
                    "recurse",
                    &Value::Null,
                    &MutationOptions::default(),
                )
                .unwrap_err();
            teardown(engine, path);
            (failure, deepest.load(Ordering::SeqCst))
        })
        .unwrap();
    let (failure, deepest) = worker.join().unwrap();
    assert_eq!(failure.code, "nesting_too_deep");
    assert_eq!(deepest, 33);
}

// Finding 17: integer keys beyond the safe range reject instead of
// collapsing onto their neighbours' keys.
#[test]
fn unsafe_integer_keys_reject() {
    let manifest = manifest(
        vec![collection("cells", "number", vec![], vec![])],
        vec![definition("put", "mutation")],
    );
    let runtime = runtime(|_kind, _module, _name, args, host| {
        set(host, "cells", &text(args), obj(vec![]))?;
        Ok(Value::Null)
    });
    let (store, path) = temp_store("unsafe-keys");
    let mut engine = Engine::new(store);
    engine
        .deploy(b"db", "app", [7u8; 32], b"", &manifest, runtime)
        .unwrap();
    let put = |engine: &mut Engine, key: &str| {
        engine.invoke_mutation(b"db", "app", "put", &s(key), &MutationOptions::default())
    };
    put(&mut engine, "9007199254740991").unwrap();
    for key in [
        "9007199254740992",
        "9007199254740993",
        "-9007199254740993",
        "1e300",
    ] {
        assert_eq!(
            put(&mut engine, key).unwrap_err().code,
            "INVALID_VALUE",
            "{key}"
        );
    }
    teardown(engine, path);
}

// Finding 18: key-prefix references resolve through point reads and
// bounded prefix scans, across differently sized keys.
#[test]
fn key_prefix_references_cascade_exactly() {
    let reference = obj(vec![
        ("targets", arr(vec![s("groups")])),
        ("discriminator", Value::Null),
        ("place", s("key_prefix")),
        ("on_delete", s("cascade")),
    ]);
    let manifest = manifest(
        vec![
            collection("groups", "string", vec![], vec![]),
            collection("members", "json", vec![], vec![reference]),
        ],
        vec![
            definition("write", "mutation"),
            definition("drop", "mutation"),
            definition("members", "query"),
        ],
    );
    let runtime = runtime(|kind, _module, name, args, host| match (kind, name) {
        (CallKind::Mutation, "write") => {
            let Value::Array(pair) = args else {
                return Err(fail("TYPE_MISMATCH", "write takes [collection, key]"));
            };
            set(host, &text(&pair[0]), &text(&pair[1]), obj(vec![]))?;
            Ok(Value::Null)
        }
        (CallKind::Mutation, "drop") => {
            hc(
                host,
                HostOp::Delete,
                vec![s(""), s("groups"), s(&text(args))],
            )?;
            Ok(Value::Null)
        }
        (CallKind::Query, "members") => {
            let page = hc(
                host,
                HostOp::Scan,
                vec![s(""), s("members"), n(100.0), Value::Null],
            )?;
            Ok(arr(page_rows(&page)
                .0
                .into_iter()
                .map(|(key, _)| key)
                .collect()))
        }
        _ => Err(fail("UNKNOWN_METHOD", name.to_string())),
    });
    let (store, path) = temp_store("key-prefix");
    let mut engine = Engine::new(store);
    engine
        .deploy(b"db", "app", [7u8; 32], b"", &manifest, runtime)
        .unwrap();
    let write = |engine: &mut Engine, collection: &str, key: &str| {
        engine.invoke_mutation(
            b"db",
            "app",
            "write",
            &arr(vec![s(collection), s(key)]),
            &MutationOptions::default(),
        )
    };
    for group in ["\"a\"", "\"ab\""] {
        write(&mut engine, "groups", group).unwrap();
    }
    for member in [
        "[\"a\",1]",
        "[\"a\",10]",
        "[\"ab\",1]",
        "\"a\"",
        "[[\"a\"],1]",
    ] {
        let outcome = write(&mut engine, "members", member);
        if member == "[[\"a\"],1]" {
            // A nested array never matches a scalar target key.
            assert_eq!(outcome.unwrap_err().code, "foreign_key_violation");
        } else {
            outcome.unwrap();
        }
    }
    assert_eq!(
        write(&mut engine, "members", "[\"zz\",1]")
            .unwrap_err()
            .code,
        "foreign_key_violation"
    );
    engine
        .invoke_mutation(
            b"db",
            "app",
            "drop",
            &s("\"a\""),
            &MutationOptions::default(),
        )
        .unwrap();
    let members = engine
        .invoke_query(
            b"db",
            "app",
            "members",
            &Value::Null,
            &CallOptions::default(),
        )
        .unwrap();
    assert_eq!(members, arr(vec![arr(vec![s("ab"), n(1.0)])]));
    teardown(engine, path);
}

// Finding 19: a zero-budget collection pass collects nothing and keeps
// the retirement, instead of dropping it with its entries in place.
#[test]
fn zero_budget_gc_keeps_its_retirement() {
    let (store, path) = temp_store("gc-zero");
    let mut engine = Engine::new(store);
    let (manifest, runtime) = owned_module(true);
    engine
        .deploy(b"db", "app", [1u8; 32], b"", &manifest, runtime)
        .unwrap();
    engine
        .invoke_mutation(
            b"db",
            "app",
            "own",
            &arr(vec![s("t1"), s("a")]),
            &MutationOptions::default(),
        )
        .unwrap();
    let (manifest, runtime) = owned_module(false);
    engine
        .deploy(b"db", "app", [2u8; 32], b"", &manifest, runtime)
        .unwrap();
    let progress = engine.staging_gc(b"db", "app", 0).unwrap();
    assert!(!progress.done);
    assert_eq!(progress.collected, 0);
    assert_eq!(
        engine
            .staging_status(b"db", "app")
            .unwrap()
            .unwrap()
            .retired,
        1
    );
    let progress = engine.staging_gc(b"db", "app", 1000).unwrap();
    assert_eq!(progress.collected, 1);
    assert!(engine.staging_status(b"db", "app").unwrap().is_none());
    teardown(engine, path);
}

/// A reader module whose derived `wrapped` reads module `lib`'s derived
/// `base`; `lib` answers its version.
fn lib_module(version: &'static str) -> (Value, Box<dyn Runtime>) {
    let manifest = exporting(
        manifest(vec![], vec![definition("base", "derived")]),
        &["base"],
        &[],
    );
    let runtime = runtime(move |_kind, _module, name, _args, _host| match name {
        "base" => Ok(s(version)),
        _ => Err(fail("UNKNOWN_METHOD", name.to_string())),
    });
    (manifest, runtime)
}

// Finding 20: a derived value reading another module's derived value
// refreshes when that module redeploys.
#[test]
fn cross_module_derived_values_refresh_on_redeploy() {
    let (store, path) = temp_store("cross-module-derived");
    let mut engine = Engine::new(store);
    let (manifest, runtime) = lib_module("v1");
    engine
        .deploy(b"db", "lib", [1u8; 32], b"", &manifest, runtime)
        .unwrap();
    let reader = self::manifest(
        vec![],
        vec![
            definition("wrapped", "derived"),
            definition("read", "query"),
        ],
    );
    let reader_runtime = self::runtime(|kind, _module, name, _args, host| match (kind, name) {
        (CallKind::Derived, "wrapped") => get_derived(host, "lib", "base"),
        (CallKind::Query, "read") => get_derived(host, "", "wrapped"),
        _ => Err(fail("UNKNOWN_METHOD", name.to_string())),
    });
    engine
        .deploy(b"db", "reader", [5u8; 32], b"", &reader, reader_runtime)
        .unwrap();
    let read = |engine: &Engine| {
        engine
            .invoke_query(
                b"db",
                "reader",
                "read",
                &Value::Null,
                &CallOptions::default(),
            )
            .unwrap()
    };
    assert_eq!(read(&engine), s("v1"));
    let (manifest, runtime) = lib_module("v2");
    engine
        .deploy(b"db", "lib", [2u8; 32], b"", &manifest, runtime)
        .unwrap();
    assert_eq!(read(&engine), s("v2"));
    teardown(engine, path);
}

/// Wrap planned record writes as a replicated patch.
fn planned_patch(
    db: &[u8],
    writes: Vec<flower_engine::patch::RecordWrite>,
    guard: Option<u64>,
    base: u64,
) -> Vec<u8> {
    flower_engine::patch::encode_patch(&flower_engine::patch::Patch {
        failed: false,
        db: db.to_vec(),
        versions: Vec::new(),
        records: writes,
        indexes: Vec::new(),
        derived: Vec::new(),
        drops: Vec::new(),
        receipts: Vec::new(),
        result: b"null".to_vec(),
        expected_revision: guard,
        base_revision: base,
    })
    .unwrap()
}

// Queue and timer state changes plan as record writes over committed
// state, commit nothing themselves, and apply through ordinary patches.
#[test]
fn queue_and_timer_plans_apply_as_patches() {
    let (store, path) = temp_store("plans");
    let mut engine = Engine::new(store);
    let (manifest, runtime) = tasks_module();
    engine
        .deploy(b"db", "app", [7u8; 32], b"", &manifest, runtime)
        .unwrap();
    let id = engine
        .enqueue(b"db", "jobs", &s("work"), 0, 3, 1_000)
        .unwrap();
    let tokens = vec!["t".repeat(32)];
    let planned = engine
        .plan_poll_queue(b"db", "jobs", 10, 60_000, 2_000, &tokens)
        .unwrap();
    assert_eq!(planned.outcome.len(), 1);
    assert_eq!(planned.outcome[0].token, tokens[0]);
    // Planning committed nothing.
    let item = engine.get_queue_item(b"db", "jobs", &id).unwrap().unwrap();
    assert_eq!(item.state, flower_engine::work_queue::ItemState::Pending);
    let patch = planned_patch(b"db", planned.writes, None, planned.revision);
    engine.apply_patch(&patch, 1).unwrap();
    let item = engine.get_queue_item(b"db", "jobs", &id).unwrap().unwrap();
    assert_eq!(item.state, flower_engine::work_queue::ItemState::Claimed);
    let done = engine
        .plan_complete_queue(b"db", "jobs", &id, &tokens[0], 2_000)
        .unwrap();
    engine
        .apply_patch(&planned_patch(b"db", done.writes, None, done.revision), 2)
        .unwrap();
    assert!(engine.get_queue_item(b"db", "jobs", &id).unwrap().is_none());
    // Timers plan the same way, guarded by the revision they read.
    let schedule = flower_engine::timer::TimerSchedule {
        id: "t1",
        module: "app",
        handler: "put",
        args: &s("x"),
        due_ms: 5,
        max_attempts: 3,
    };
    let planned = engine.plan_schedule_timer(b"db", &schedule, 1).unwrap();
    assert!(engine.get_timer(b"db", "t1").unwrap().is_none());
    let patch = planned_patch(
        b"db",
        planned.writes,
        Some(planned.revision),
        planned.revision,
    );
    engine.apply_patch(&patch, 3).unwrap();
    assert_eq!(engine.get_timer(b"db", "t1").unwrap().unwrap().due_ms, 5);
    let cancel = engine.plan_cancel_timer(b"db", "t1").unwrap();
    assert!(cancel.outcome);
    // A commit after the plan moves the revision: the guarded plan
    // rejects as stale instead of applying over it.
    let moved = unwrap_patch(
        engine
            .prepare_mutation(b"db", "app", "put", &s("r1"), &MutationOptions::default())
            .unwrap(),
    );
    engine.apply_patch(&moved.patch, 4).unwrap();
    let patch = planned_patch(b"db", cancel.writes, Some(cancel.revision), cancel.revision);
    let outcome = engine.apply_patch(&patch, 5).unwrap();
    assert_eq!(outcome.result.unwrap_err().code, "stale_revision");
    assert!(engine.get_timer(b"db", "t1").unwrap().is_some());
    teardown(engine, path);
}
