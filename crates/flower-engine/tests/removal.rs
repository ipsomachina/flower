//! Removing a module and purging its data on one engine: a removal takes
//! the module's code, aliases and derived values (and what other modules
//! computed from it) and keeps its data and declarations, so deploying it
//! again rebuilds nothing; a removal with `purge` deletes its records,
//! index entries, raw rows, timers and the queues it alone declared a
//! bounded page at a time, one module at a time by name, while its
//! deployment waits; its timers that come due meanwhile are held for
//! it; refusals change nothing.

use flower_engine::batch::BatchItem;
use flower_engine::engine::{CallOptions, Engine, FirePrepare, MutationOptions};
use flower_engine::host::{QUEUE_COLLECTION, RAW_COLLECTION, TIMER_COLLECTION};
use flower_engine::patch::{Patch, encode_patch};
use flower_engine::purge::{self, PURGE_REFUSED, PURGE_STALE, Part, PurgePage, QUEUE_PURGING};
use flower_engine::runtime::{CallKind, ClosureRuntime, HostCall};
use flower_engine::staging::code_era_key;
use flower_engine::storage::{GroupStore, SnapshotTable, meta};
use flower_engine::task::TaskPrepare;
use flower_engine::timer::TimerState;
use flower_engine::work_queue::{ClaimRequest, NewItem, QUEUE_UNDECLARED};
use flower_sdk::abi::{HostOp, decode_envelope};
use flower_sdk::error::codes;
use flower_sdk::{Failure, Value, fail};

const DB: &[u8] = b"db";

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

fn k(name: &str) -> String {
    format!("\"{name}\"")
}

fn schema(kind: &str) -> Value {
    obj(vec![("type", s(kind))])
}

fn collection(name: &str, indexes: Vec<(&str, &str)>) -> Value {
    obj(vec![
        ("name", s(name)),
        ("key", schema("string")),
        ("value", schema("json")),
        (
            "indexes",
            arr(indexes
                .into_iter()
                .map(|(name, field)| obj(vec![("name", s(name)), ("fields", arr(vec![s(field)]))]))
                .collect()),
        ),
        ("references", arr(vec![])),
    ])
}

fn definition(name: &str, kind: &str) -> Value {
    obj(vec![
        ("name", s(name)),
        ("kind", s(kind)),
        ("args", schema("json")),
    ])
}

/// A bulk module: `rows` indexed by shop, `plain`, raw rows, a timer per
/// fill, an exported derived count.
fn bulk_manifest(extra_index: bool) -> Value {
    let mut indexes = vec![("by_shop", "shop")];
    if extra_index {
        indexes.push(("by_n", "n"));
    }
    obj(vec![
        (
            "collections",
            arr(vec![
                collection("rows", indexes),
                collection("plain", vec![]),
            ]),
        ),
        (
            "definitions",
            arr(vec![
                definition("fill", "mutation"),
                definition("count", "query"),
                definition("total", "derived"),
            ]),
        ),
        ("triggers", arr(vec![])),
        ("http", arr(vec![])),
        (
            "exports",
            obj(vec![
                ("collections", arr(vec![s("rows")])),
                ("derived", arr(vec![s("total")])),
                ("methods", arr(vec![s("count")])),
            ]),
        ),
    ])
}

/// The host app: its own rows, raw rows and timer, and a derived value
/// over the bulk module's exported total.
fn app_manifest() -> Value {
    obj(vec![
        ("collections", arr(vec![collection("kept", vec![])])),
        (
            "definitions",
            arr(vec![
                definition("put", "mutation"),
                definition("reads", "derived"),
                definition("read", "query"),
            ]),
        ),
        ("triggers", arr(vec![])),
        ("http", arr(vec![])),
    ])
}

/// A module importing the bulk module's total, directly or through a
/// slot bound to it.
fn consumer_manifest(slot: bool) -> Value {
    let mut root = vec![
        ("collections", arr(vec![])),
        ("definitions", arr(vec![definition("read", "query")])),
        ("triggers", arr(vec![])),
        ("http", arr(vec![])),
        (
            "imports",
            arr(vec![obj(vec![
                ("module", s(if slot { "$source" } else { "bulk" })),
                ("collections", arr(vec![])),
                ("derived", arr(vec![obj(vec![("name", s("total"))])])),
                ("methods", arr(vec![])),
            ])]),
        ),
    ];
    if slot {
        root.push(("bindings", obj(vec![("source", s("bulk"))])));
    }
    obj(root)
}

fn hc(host: &mut dyn HostCall, op: HostOp, args: Vec<Value>) -> Result<Value, Failure> {
    let input = Value::Array(args).encode_binary().unwrap();
    let output = host.op(op as u32 as u8, &input);
    decode_envelope(Value::decode_binary(&output).unwrap())
}

fn number(args: &Value, name: &str) -> u64 {
    match args {
        Value::Object(map) => match map.get(name) {
            Some(Value::Number(number)) => *number as u64,
            _ => 0,
        },
        _ => 0,
    }
}

fn runtime() -> Box<dyn flower_engine::runtime::Runtime> {
    Box::new(ClosureRuntime::new(
        |kind: CallKind, module: &str, name: &str, args: &Value, host: &mut dyn HostCall| match (
            kind, name,
        ) {
            (CallKind::Mutation, "fill") => {
                let (from, to) = (number(args, "from"), number(args, "to"));
                for i in from..to {
                    let row = obj(vec![
                        ("shop", s(&format!("s{}", i % 3))),
                        ("n", n(i as f64)),
                    ]);
                    hc(
                        host,
                        HostOp::Set,
                        vec![s(""), s("rows"), s(&k(&format!("r{i}"))), row],
                    )?;
                    hc(
                        host,
                        HostOp::Set,
                        vec![s(""), s("plain"), s(&k(&format!("p{i}"))), n(i as f64)],
                    )?;
                    hc(
                        host,
                        HostOp::Set,
                        vec![s(""), s("$raw/blobs"), s(&format!("b{i}")), s("blob")],
                    )?;
                }
                hc(
                    host,
                    HostOp::TimerSchedule,
                    vec![
                        s(&format!("{module}-{from}")),
                        n(4_000_000_000_000.0),
                        s("fill"),
                        obj(vec![]),
                        n(3.0),
                    ],
                )?;
                Ok(n((to - from) as f64))
            }
            (CallKind::Query, "count") | (CallKind::Derived, "total") => {
                match hc(
                    host,
                    HostOp::Scan,
                    vec![s(""), s("rows"), n(1000.0), Value::Null],
                )? {
                    Value::Object(page) => match page.get("rows") {
                        Some(Value::Array(rows)) => Ok(n(rows.len() as f64)),
                        _ => Err(fail("TYPE_MISMATCH", "page without rows")),
                    },
                    _ => Err(fail("TYPE_MISMATCH", "page is not an object")),
                }
            }
            (CallKind::Mutation, "put") => {
                hc(
                    host,
                    HostOp::Set,
                    vec![s(""), s("kept"), s(&k("a")), n(1.0)],
                )?;
                hc(
                    host,
                    HostOp::Set,
                    vec![s(""), s("$raw/notes"), s("a"), s("note")],
                )?;
                hc(
                    host,
                    HostOp::TimerSchedule,
                    vec![
                        s("app-a"),
                        n(4_000_000_000_000.0),
                        s("put"),
                        obj(vec![]),
                        n(3.0),
                    ],
                )?;
                Ok(Value::Null)
            }
            (CallKind::Mutation, "enqueue") => {
                let queue = match args {
                    Value::Object(map) => map.get("queue").cloned().unwrap_or(Value::Null),
                    _ => Value::Null,
                };
                hc(host, HostOp::Enqueue, vec![queue, n(1.0), n(0.0), n(3.0)])
            }
            // A task always due whose every run fails.
            (CallKind::Query, "tidy") => Ok(n(0.0)),
            (CallKind::Mutation, "tidy") => Err(fail("TIDY_FAILED", "nothing tidied")),
            (CallKind::Derived, "reads") => hc(
                host,
                HostOp::GetDerived,
                vec![s("bulk"), s("total"), Value::Null],
            ),
            (CallKind::Query, "read") if module == "app" => hc(
                host,
                HostOp::GetDerived,
                vec![s(""), s("reads"), Value::Null],
            ),
            (CallKind::Query, "read") => hc(
                host,
                HostOp::GetDerived,
                vec![s("$source"), s("total"), Value::Null],
            ),
            _ => Err(fail("UNKNOWN_METHOD", name.to_string())),
        },
    ))
}

fn setup(test: &str) -> (Engine, std::path::PathBuf) {
    let mut path = std::env::temp_dir();
    path.push(format!("flower-removal-{test}-{}", std::process::id()));
    let _ = std::fs::remove_file(&path);
    (Engine::new(GroupStore::create(&path).unwrap()), path)
}

fn teardown(engine: Engine, path: std::path::PathBuf) {
    drop(engine);
    let _ = std::fs::remove_file(&path);
}

fn deploy(
    engine: &mut Engine,
    module: &str,
    identity: u8,
    manifest: &Value,
) -> Result<(), Failure> {
    engine.deploy(DB, module, [identity; 32], b"wasm", manifest, runtime())
}

fn fill(engine: &mut Engine, module: &str, from: u64, to: u64) {
    engine
        .invoke_mutation(
            DB,
            module,
            "fill",
            &obj(vec![("from", n(from as f64)), ("to", n(to as f64))]),
            &MutationOptions::default(),
        )
        .unwrap();
}

fn query(engine: &Engine, module: &str, name: &str) -> Result<Value, Failure> {
    engine.invoke_query(DB, module, name, &Value::Null, &CallOptions::default())
}

/// What a module holds in the store, by kind: its records, index
/// entries and derived outcomes under the names it interned, its raw
/// rows and its timers' rows. Each is a sorted list of full keys.
#[derive(Debug, Default, PartialEq, Eq)]
struct Held {
    records: Vec<Vec<u8>>,
    index: Vec<Vec<u8>>,
    derived: Vec<Vec<u8>>,
    raw: Vec<Vec<u8>>,
    timers: Vec<Vec<u8>>,
}

impl Held {
    fn keys(&self) -> usize {
        self.records.len() + self.index.len() + self.raw.len() + self.timers.len()
    }
}

fn sized(bytes: &[u8]) -> (&[u8], &[u8]) {
    let len = u32::from_be_bytes(bytes[..4].try_into().unwrap()) as usize;
    (&bytes[4..4 + len], &bytes[4 + len..])
}

fn held(engine: &Engine, module: &str) -> Held {
    let read = engine.store().begin_read().unwrap();
    let base = 4 + DB.len();
    let owned = format!("{module}\0");
    let ids: Vec<u32> = read
        .scan_db_raw(SnapshotTable::InternId, DB)
        .unwrap()
        .into_iter()
        .filter(|(_, name)| name.starts_with(owned.as_bytes()))
        .map(|(key, _)| u32::from_be_bytes(key[base..base + 4].try_into().unwrap()))
        .collect();
    let raw = read.intern_id(DB, RAW_COLLECTION).unwrap();
    let timers = read.intern_id(DB, TIMER_COLLECTION).unwrap();
    let id_at = |key: &[u8], at: usize| u32::from_be_bytes(key[at..at + 4].try_into().unwrap());
    let mut found = Held::default();
    for (key, value) in read.scan_db_raw(SnapshotTable::Records, DB).unwrap() {
        let id = id_at(&key, base);
        let (stored, _) = sized(&key[base + 4..]);
        if ids.contains(&id) {
            found.records.push(key.clone());
        } else if Some(id) == raw {
            let (namespace, _) = sized(stored);
            if namespace.starts_with(owned.as_bytes()) {
                found.raw.push(key.clone());
            }
        } else if Some(id) == timers && stored.len() > 100 {
            let json = flower_engine::storage::codec::unpack_value(&value).unwrap();
            let timer: serde_json::Value = serde_json::from_slice(&json).unwrap();
            if timer["module"] == module {
                found.timers.push(key.clone());
                let id = timer["id"].as_str().unwrap();
                let mut pointer = format!("i{id}").into_bytes();
                let mut full = key[..base + 4].to_vec();
                full.extend_from_slice(&(pointer.len() as u32).to_be_bytes());
                full.append(&mut pointer);
                found.timers.push(full);
            }
        }
    }
    for (key, _) in read.scan_db_raw(SnapshotTable::IndexEntries, DB).unwrap() {
        if ids.contains(&id_at(&key, base)) {
            found.index.push(key);
        }
    }
    for (key, _) in read.scan_db_raw(SnapshotTable::Derived, DB).unwrap() {
        if ids.contains(&id_at(&key, base + 1)) {
            found.derived.push(key);
        }
    }
    found.timers.sort();
    found
}

fn answered(value: &Value) -> (String, u64, u64, bool) {
    let Value::Object(map) = value else {
        panic!("a page answers an object: {value:?}");
    };
    let text = |name: &str| match map.get(name) {
        Some(Value::String(text)) => text.clone(),
        other => panic!("{name}: {other:?}"),
    };
    let count = |name: &str| match map.get(name) {
        Some(Value::Number(number)) => *number as u64,
        other => panic!("{name}: {other:?}"),
    };
    let done = matches!(map.get("done"), Some(Value::Bool(true)));
    (text("module"), count("deleted"), count("keys"), done)
}

#[test]
fn a_removal_takes_the_code_and_keeps_data_and_declarations() {
    let (mut engine, path) = setup("keep");
    deploy(&mut engine, "bulk", 1, &bulk_manifest(false)).unwrap();
    deploy(&mut engine, "app", 2, &app_manifest()).unwrap();
    fill(&mut engine, "bulk", 0, 20);
    assert_eq!(query(&engine, "bulk", "count").unwrap(), n(20.0));
    // The app's derived value reads the bulk module's: both cache.
    assert_eq!(query(&engine, "app", "read").unwrap(), n(20.0));
    let before = held(&engine, "bulk");
    assert_eq!(before.records.len(), 40);
    assert_eq!(before.index.len(), 20);
    assert!(!before.derived.is_empty());
    assert!(!held(&engine, "app").derived.is_empty());

    let removed = engine.remove_module(DB, "bulk", false).unwrap().unwrap();
    assert_eq!(
        removed,
        obj(vec![
            ("purging", Value::Bool(false)),
            ("removed", s("bulk"))
        ])
    );
    // Its code is gone: nothing calls it, and the app's value fails now.
    assert!(query(&engine, "bulk", "count").is_err());
    assert_eq!(
        query(&engine, "app", "read").unwrap_err().code,
        codes::MODULE_PRIVATE
    );
    // Its data and index entries stay; its derived outcomes, and the
    // app's that read them, went with the removal.
    let after = held(&engine, "bulk");
    assert_eq!(
        (&after.records, &after.index, &after.raw, &after.timers),
        (&before.records, &before.index, &before.raw, &before.timers)
    );
    assert!(after.derived.is_empty(), "{:?}", after.derived);
    let read = engine.store().begin_read().unwrap();
    assert_eq!(
        purge::removed(&read, DB).unwrap(),
        vec![("bulk".to_string(), [1u8; 32])]
    );
    // Its code era went with its code identity: deploying it again
    // mints another.
    assert_eq!(read.meta(DB, &code_era_key("bulk")).unwrap(), None);
    drop(read);
    // Removing it again is refused and changes nothing.
    let again = engine
        .remove_module(DB, "bulk", false)
        .unwrap()
        .unwrap_err();
    assert_eq!(again.code, codes::MODULE_CONFLICT);
    assert!(
        again.message.contains("no module `bulk` is deployed"),
        "{again:?}"
    );

    // Deployed again with the same declarations, it rebuilds nothing:
    // its staging has no backfill to plan.
    engine
        .deploy_staged(DB, "bulk", [1u8; 32], b"wasm", &bulk_manifest(false))
        .unwrap();
    let staging = engine.staging_status(DB, "bulk").unwrap().unwrap();
    assert!(staging.ready && staging.planned == 0, "{staging:?}");
    engine.activate(DB, "bulk", runtime()).unwrap();
    assert_eq!(query(&engine, "bulk", "count").unwrap(), n(20.0));
    assert_eq!(held(&engine, "bulk").index, before.index);
    let read = engine.store().begin_read().unwrap();
    assert!(purge::removed(&read, DB).unwrap().is_empty());
    drop(read);

    // Removed and deployed with another index, it builds that one only.
    engine.remove_module(DB, "bulk", false).unwrap().unwrap();
    engine
        .deploy_staged(DB, "bulk", [3u8; 32], b"wasm", &bulk_manifest(true))
        .unwrap();
    let staging = engine.staging_status(DB, "bulk").unwrap().unwrap();
    assert_eq!(staging.planned, 1, "{staging:?}");
    let drained = engine.staging_drain(DB, "bulk", 1000).unwrap();
    assert!(drained.done && drained.scanned == 20, "{drained:?}");
    engine.activate(DB, "bulk", runtime()).unwrap();
    assert_eq!(held(&engine, "bulk").index.len(), 40);
    teardown(engine, path);
}

#[test]
fn a_removal_refuses_what_others_import_and_what_is_not_deployed() {
    let (mut engine, path) = setup("refuse");
    deploy(&mut engine, "bulk", 1, &bulk_manifest(false)).unwrap();
    deploy(&mut engine, "direct", 2, &consumer_manifest(false)).unwrap();
    deploy(&mut engine, "slotted", 3, &consumer_manifest(true)).unwrap();
    fill(&mut engine, "bulk", 0, 3);
    let held_before = held(&engine, "bulk");
    for purge in [false, true] {
        let refused = engine
            .remove_module(DB, "bulk", purge)
            .unwrap()
            .unwrap_err();
        assert_eq!(refused.code, codes::MODULE_CONFLICT);
        assert!(
            refused.message.contains("`direct`, `slotted`"),
            "{refused:?}"
        );
    }
    assert_eq!(query(&engine, "bulk", "count").unwrap(), n(3.0));
    assert_eq!(held(&engine, "bulk"), held_before);
    for module in ["", "$source", "a\0b"] {
        let refused = engine
            .remove_module(DB, module, false)
            .unwrap()
            .unwrap_err();
        assert_eq!(refused.code, codes::MODULE_CONFLICT, "{module:?}");
    }
    let ghost = engine
        .remove_module(DB, "ghost", false)
        .unwrap()
        .unwrap_err();
    assert!(ghost.message.contains("no module `ghost` is deployed"));
    // Importers go first; then the provider can.
    engine.remove_module(DB, "direct", false).unwrap().unwrap();
    assert!(engine.remove_module(DB, "bulk", false).unwrap().is_err());
    engine.remove_module(DB, "slotted", true).unwrap().unwrap();
    engine.remove_module(DB, "bulk", false).unwrap().unwrap();
    // A replicated database removes through its group only.
    engine.set_replicated(b"other");
    assert_eq!(
        engine
            .remove_module(b"other", "bulk", false)
            .unwrap_err()
            .code,
        "replicated_db"
    );
    teardown(engine, path);
}

#[test]
fn a_purge_deletes_the_module_data_in_pages_while_its_deployment_waits() {
    let (mut engine, path) = setup("pages");
    deploy(&mut engine, "bulk", 1, &bulk_manifest(false)).unwrap();
    deploy(&mut engine, "app", 2, &app_manifest()).unwrap();
    fill(&mut engine, "bulk", 0, 25);
    fill(&mut engine, "bulk", 25, 30);
    engine
        .invoke_mutation(DB, "app", "put", &Value::Null, &MutationOptions::default())
        .unwrap();
    let bulk = held(&engine, "bulk");
    let app = held(&engine, "app");
    assert_eq!(
        (
            bulk.records.len(),
            bulk.index.len(),
            bulk.raw.len(),
            bulk.timers.len()
        ),
        (60, 30, 30, 4)
    );
    assert_eq!(
        (app.records.len(), app.raw.len(), app.timers.len()),
        (1, 1, 2)
    );
    let total = bulk.keys() as u64;

    let removed = engine.remove_module(DB, "bulk", true).unwrap().unwrap();
    assert_eq!(
        removed,
        obj(vec![("purging", Value::Bool(true)), ("removed", s("bulk"))])
    );
    let read = engine.store().begin_read().unwrap();
    assert_eq!(
        purge::purges(&read, DB).unwrap().keys().collect::<Vec<_>>(),
        vec!["bulk"]
    );
    assert!(purge::removed(&read, DB).unwrap().is_empty());
    drop(read);
    // Its deployment waits for the purge.
    let refused = deploy(&mut engine, "bulk", 1, &bulk_manifest(false)).unwrap_err();
    assert_eq!(refused.code, codes::MODULE_CONFLICT);
    assert!(
        refused.message.contains("still being purged"),
        "{refused:?}"
    );
    // Asked again, it goes on as it was.
    engine.remove_module(DB, "bulk", true).unwrap().unwrap();

    let mut pages = 0;
    let mut deleted = 0;
    loop {
        let answer = engine
            .purge_local(DB, 7)
            .unwrap()
            .expect("a purge in flight");
        let (module, so_far, keys, done) = answered(&answer.unwrap());
        assert_eq!(module, "bulk");
        assert!(
            keys <= 8,
            "a page deletes at most its limit (a timer's two rows)"
        );
        deleted += keys;
        assert_eq!(so_far, deleted);
        pages += 1;
        assert!(pages <= total / 7 + 2, "the purge does not end");
        if done {
            break;
        }
        // Until the last page, the module waits.
        assert!(deploy(&mut engine, "bulk", 1, &bulk_manifest(false)).is_err());
    }
    assert_eq!(deleted, total);
    assert!(pages > 2, "{pages} pages");
    assert!(engine.purge_local(DB, 7).unwrap().is_none());
    // Nothing of it is left; the app's data is untouched.
    assert_eq!(held(&engine, "bulk"), Held::default());
    assert_eq!(held(&engine, "app"), app);
    let read = engine.store().begin_read().unwrap();
    assert!(read.meta(DB, purge::PURGES_ROW).unwrap().is_none());
    drop(read);
    assert!(engine.get_timer(DB, "bulk-0").unwrap().is_none());
    assert!(engine.get_timer(DB, "app-a").unwrap().is_some());
    // Deployed again, it starts empty.
    deploy(&mut engine, "bulk", 1, &bulk_manifest(false)).unwrap();
    assert_eq!(query(&engine, "bulk", "count").unwrap(), n(0.0));
    teardown(engine, path);
}

#[test]
fn purges_go_one_module_at_a_time_by_name() {
    let (mut engine, path) = setup("order");
    for (module, identity) in [("one", 1), ("two", 2)] {
        deploy(&mut engine, module, identity, &bulk_manifest(false)).unwrap();
        fill(&mut engine, module, 0, 5);
    }
    for module in ["two", "one"] {
        engine.remove_module(DB, module, true).unwrap().unwrap();
    }
    let (module, _, _, _) = answered(&engine.purge_local(DB, 4).unwrap().unwrap().unwrap());
    assert_eq!(module, "one");
    assert!(held(&engine, "two").keys() > 0);
    loop {
        let (module, _, _, done) = answered(&engine.purge_local(DB, 4).unwrap().unwrap().unwrap());
        assert_eq!(module, "one");
        if done {
            break;
        }
    }
    assert_eq!(held(&engine, "one"), Held::default());
    assert!(held(&engine, "two").keys() > 0);
    // Each waits for its own purge alone.
    deploy(&mut engine, "one", 1, &bulk_manifest(false)).unwrap();
    assert!(deploy(&mut engine, "two", 2, &bulk_manifest(false)).is_err());
    while engine.purge_local(DB, 4).unwrap().is_some() {}
    assert_eq!(held(&engine, "two"), Held::default());
    deploy(&mut engine, "two", 2, &bulk_manifest(false)).unwrap();
    teardown(engine, path);
}

#[test]
fn purging_a_module_removed_earlier_deletes_what_its_removal_kept() {
    let (mut engine, path) = setup("later");
    deploy(&mut engine, "bulk", 1, &bulk_manifest(false)).unwrap();
    fill(&mut engine, "bulk", 0, 10);
    engine.remove_module(DB, "bulk", false).unwrap().unwrap();
    let kept = held(&engine, "bulk");
    assert!(kept.keys() > 0);
    engine.remove_module(DB, "bulk", true).unwrap().unwrap();
    let read = engine.store().begin_read().unwrap();
    // Its declarations go at once, its data a page at a time.
    assert!(purge::removed(&read, DB).unwrap().is_empty());
    assert!(purge::purges(&read, DB).unwrap().contains_key("bulk"));
    drop(read);
    while engine.purge_local(DB, 16).unwrap().is_some() {}
    assert_eq!(held(&engine, "bulk"), Held::default());
    // A module never deployed purges nothing, and ends at once.
    engine.remove_module(DB, "ghost", true).unwrap().unwrap();
    let (module, deleted, _, done) =
        answered(&engine.purge_local(DB, 16).unwrap().unwrap().unwrap());
    assert_eq!((module.as_str(), deleted, done), ("ghost", 0, true));
    assert!(engine.purge_local(DB, 16).unwrap().is_none());
    teardown(engine, path);
}

/// The database's applied position, which a purge page must advance.
fn applied(engine: &Engine) -> u64 {
    let read = engine.store().begin_read().unwrap();
    read.meta_u64(DB, meta::APPLIED_LOG).unwrap()
}

#[test]
fn a_page_applies_only_where_its_purge_stands_and_only_to_its_module() {
    let (mut engine, path) = setup("stale");
    deploy(&mut engine, "bulk", 1, &bulk_manifest(false)).unwrap();
    deploy(&mut engine, "app", 2, &app_manifest()).unwrap();
    fill(&mut engine, "bulk", 0, 10);
    engine
        .invoke_mutation(DB, "app", "put", &Value::Null, &MutationOptions::default())
        .unwrap();
    engine.remove_module(DB, "bulk", true).unwrap().unwrap();
    let Some(BatchItem::Purge { page, .. }) = engine.prepare_purge(DB, 5).unwrap() else {
        panic!("a page is due");
    };
    assert_eq!(page.deletes.len(), 5);
    // Another page went first: this one no longer continues the purge.
    engine.purge_local(DB, 5).unwrap().unwrap().unwrap();
    let left = held(&engine, "bulk");
    let staged = engine.store().begin_staged().unwrap();
    let (stale, _) = engine
        .commit_purge(&staged, DB, &page, applied(&engine) + 1, true)
        .unwrap();
    assert_eq!(stale.unwrap_err().code, PURGE_STALE);
    drop(staged);
    assert_eq!(held(&engine, "bulk"), left);

    // A page naming another module's key is refused whole.
    let read = engine.store().begin_read().unwrap();
    let progress = purge::purges(&read, DB).unwrap()["bulk"].clone();
    let kept = read.intern_id(DB, b"app\0kept").unwrap().unwrap();
    drop(read);
    let mut theirs = kept.to_be_bytes().to_vec();
    theirs.extend_from_slice(&(k("a").len() as u32).to_be_bytes());
    theirs.extend_from_slice(k("a").as_bytes());
    let forged = PurgePage {
        module: "bulk".to_string(),
        from: progress.cursor.clone(),
        deletes: vec![(Part::Records, theirs)],
        next: None,
    };
    let staged = engine.store().begin_staged().unwrap();
    let (refused, _) = engine
        .commit_purge(&staged, DB, &forged, applied(&engine) + 1, true)
        .unwrap();
    assert_eq!(refused.unwrap_err().code, PURGE_REFUSED);
    drop(staged);
    assert_eq!(held(&engine, "app").records.len(), 1);
    // No purge in flight: a page is stale.
    let ghost = PurgePage {
        module: "ghost".to_string(),
        ..forged
    };
    let staged = engine.store().begin_staged().unwrap();
    let (stale, _) = engine
        .commit_purge(&staged, DB, &ghost, applied(&engine) + 1, true)
        .unwrap();
    assert_eq!(stale.unwrap_err().code, PURGE_STALE);
    drop(staged);
    teardown(engine, path);
}

/// A module declaring `queues`, which enqueues into any queue it is
/// given.
fn queue_manifest(queues: &[&str]) -> Value {
    obj(vec![
        ("collections", arr(vec![])),
        (
            "definitions",
            arr(vec![
                definition("fill", "mutation"),
                definition("enqueue", "mutation"),
            ]),
        ),
        ("triggers", arr(vec![])),
        ("http", arr(vec![])),
        (
            "queues",
            arr(queues
                .iter()
                .map(|name| obj(vec![("name", s(name))]))
                .collect()),
        ),
    ])
}

/// The `$queue` rows of `queue`, of every kind: items, ID rows, fences,
/// turns.
fn queue_rows(engine: &Engine, queue: &str) -> Vec<Vec<u8>> {
    let read = engine.store().begin_read().unwrap();
    let Some(collection) = read.intern_id(DB, QUEUE_COLLECTION).unwrap() else {
        return Vec::new();
    };
    let base = 4 + DB.len();
    read.scan_db_raw(SnapshotTable::Records, DB)
        .unwrap()
        .into_iter()
        .map(|(key, _)| key)
        .filter(|key| u32::from_be_bytes(key[base..base + 4].try_into().unwrap()) == collection)
        .filter(|key| {
            let (stored, _) = sized(&key[base + 4..]);
            let padded = &stored[1..129];
            let end = padded.iter().position(|byte| *byte == 0).unwrap_or(128);
            &padded[..end] == queue.as_bytes()
        })
        .collect()
}

#[test]
fn a_purge_deletes_the_queues_its_module_alone_declared() {
    let (mut engine, path) = setup("queues");
    // `kept` is removed with its data kept: its `mail` stays.
    deploy(&mut engine, "kept", 1, &queue_manifest(&["mail"])).unwrap();
    engine.remove_module(DB, "kept", false).unwrap().unwrap();
    let declared = queue_manifest(&["jobs", "shared", "mail"]);
    deploy(&mut engine, "bulk", 2, &declared).unwrap();
    deploy(&mut engine, "other", 3, &queue_manifest(&["shared"])).unwrap();
    for (queue, items) in [("jobs", 30), ("shared", 4), ("mail", 4), ("loose", 4)] {
        for item in 0..items {
            engine.enqueue(DB, queue, &n(item as f64), 0, 3, 1).unwrap();
        }
    }
    // A claim leaves a fence and a turn beside the items.
    assert_eq!(
        engine.poll_queue(DB, "jobs", 2, 3, 60_000).unwrap().len(),
        3
    );
    let jobs = queue_rows(&engine, "jobs");
    assert!(jobs.len() > 60, "{} rows", jobs.len());
    let others: Vec<_> = ["shared", "mail", "loose"]
        .into_iter()
        .map(|queue| queue_rows(&engine, queue))
        .collect();

    engine.remove_module(DB, "bulk", true).unwrap().unwrap();
    let read = engine.store().begin_read().unwrap();
    assert_eq!(purge::purges(&read, DB).unwrap()["bulk"].queues, ["jobs"]);
    drop(read);
    // No module declaring a queue under purge deploys until it ends;
    // one declaring the others does.
    let refused = deploy(&mut engine, "newcomer", 4, &queue_manifest(&["jobs"])).unwrap_err();
    assert_eq!(refused.code, codes::MODULE_CONFLICT);
    assert!(refused.message.contains("queue `jobs`"), "{refused:?}");
    deploy(&mut engine, "sharer", 5, &queue_manifest(&["shared"])).unwrap();

    // A page naming a row of another queue is refused whole.
    let read = engine.store().begin_read().unwrap();
    let progress = purge::purges(&read, DB).unwrap()["bulk"].clone();
    drop(read);
    let forged = PurgePage {
        module: "bulk".to_string(),
        from: progress.cursor.clone(),
        deletes: vec![(Part::Records, others[0][0][4 + DB.len()..].to_vec())],
        next: None,
    };
    let staged = engine.store().begin_staged().unwrap();
    let (refused, _) = engine
        .commit_purge(&staged, DB, &forged, applied(&engine) + 1, true)
        .unwrap();
    assert_eq!(refused.unwrap_err().code, PURGE_REFUSED);
    drop(staged);

    let mut deleted = 0;
    loop {
        let (module, so_far, keys, done) =
            answered(&engine.purge_local(DB, 7).unwrap().unwrap().unwrap());
        assert_eq!(module, "bulk");
        assert!(keys <= 8, "a page deletes at most its limit");
        deleted += keys;
        assert_eq!(so_far, deleted);
        if done {
            break;
        }
    }
    assert_eq!(deleted as usize, jobs.len());
    assert!(queue_rows(&engine, "jobs").is_empty());
    for (queue, rows) in ["shared", "mail", "loose"].into_iter().zip(&others) {
        assert_eq!(&queue_rows(&engine, queue), rows, "{queue}");
    }
    deploy(&mut engine, "newcomer", 4, &queue_manifest(&["jobs"])).unwrap();
    teardown(engine, path);
}

/// Prepare a fire of `id` and apply what it proposes at the next
/// position: which it was.
fn apply_timer(engine: &mut Engine, id: &str) -> &'static str {
    let (kind, prepared) = match engine.prepare_timer_fire(DB, id).unwrap() {
        FirePrepare::Skip => return "skip",
        FirePrepare::Patch(prepared) => ("fire", prepared),
        FirePrepare::Retry(prepared) => ("retry", prepared),
        FirePrepare::Hold(prepared) => ("hold", prepared),
    };
    let position = applied(engine) + 1;
    engine.apply_patch(&prepared.patch, position).unwrap();
    kind
}

fn due_ids(engine: &Engine, limit: usize) -> Vec<String> {
    engine
        .list_due_timers(DB, 1_000_000, limit)
        .unwrap()
        .into_iter()
        .map(|timer| timer.id)
        .collect()
}

#[test]
fn a_removed_module_holds_its_timers_without_spending_attempts() {
    let (mut engine, path) = setup("held");
    deploy(&mut engine, "bulk", 1, &bulk_manifest(false)).unwrap();
    deploy(&mut engine, "app", 2, &app_manifest()).unwrap();
    // Due at once, with one attempt: a fire that fails kills it.
    for id in ["b1", "b2", "b3"] {
        engine
            .schedule_timer(DB, id, "bulk", "fill", &obj(vec![]), 0, 1, 1)
            .unwrap();
    }
    engine
        .schedule_timer(DB, "a1", "app", "put", &Value::Null, 5, 1, 1)
        .unwrap();
    engine.remove_module(DB, "bulk", false).unwrap().unwrap();

    assert_eq!(apply_timer(&mut engine, "b1"), "hold");
    let kept = engine.get_timer(DB, "b1").unwrap().unwrap();
    assert_eq!((kept.state, kept.attempts), (TimerState::Held, 0));
    // Held, it is not due again while its module is removed.
    assert_eq!(apply_timer(&mut engine, "b1"), "skip");
    assert_eq!(apply_timer(&mut engine, "b2"), "hold");
    // Held timers leave the due order: the app's, due later, comes
    // before them now, and they are counted apart.
    assert_eq!(due_ids(&engine, 2), ["b3", "a1"]);
    assert_eq!(apply_timer(&mut engine, "b3"), "hold");
    assert_eq!(due_ids(&engine, 2), ["a1"]);
    let stats = engine.timer_stats(DB).unwrap();
    assert_eq!((stats.pending, stats.dead, stats.held), (1, 0, 3));
    let listed = engine.scan_timers(DB, Some(TimerState::Held), 10).unwrap();
    assert_eq!(listed.len(), 3);
    // Cancelled while held, it goes.
    assert!(engine.cancel_timer(DB, "b3").unwrap());

    // Deployed again, its held timers are due at once and fire.
    deploy(&mut engine, "bulk", 1, &bulk_manifest(false)).unwrap();
    assert_eq!(due_ids(&engine, 10), ["a1", "b1", "b2"]);
    assert_eq!(apply_timer(&mut engine, "b1"), "fire");
    assert!(engine.get_timer(DB, "b1").unwrap().is_none());

    // Removed with a purge, a held timer goes with its data.
    engine.remove_module(DB, "bulk", false).unwrap().unwrap();
    engine
        .schedule_timer(DB, "b4", "bulk", "fill", &obj(vec![]), 0, 1, 1)
        .unwrap();
    assert_eq!(apply_timer(&mut engine, "b4"), "hold");
    engine.remove_module(DB, "bulk", true).unwrap().unwrap();
    while engine.purge_local(DB, 3).unwrap().is_some() {}
    assert!(engine.get_timer(DB, "b2").unwrap().is_none());
    assert!(engine.get_timer(DB, "b4").unwrap().is_none());
    assert!(held(&engine, "bulk").timers.is_empty());
    assert_eq!(engine.timer_stats(DB).unwrap().held, 0);
    // A timer naming a module never deployed spends its attempts.
    engine
        .schedule_timer(DB, "g1", "ghost", "run", &Value::Null, 0, 1, 1)
        .unwrap();
    assert_eq!(apply_timer(&mut engine, "g1"), "retry");
    let dead = engine.get_timer(DB, "g1").unwrap().unwrap();
    assert_eq!(dead.state, TimerState::Dead);
    teardown(engine, path);
}

/// Finish the purge in flight, a few keys a page.
fn purge_all(engine: &mut Engine) {
    while !answered(&engine.purge_local(DB, 7).unwrap().unwrap().unwrap()).3 {}
}

#[test]
fn a_queue_under_purge_takes_no_operation_until_its_purge_ends() {
    let (mut engine, path) = setup("busy");
    deploy(&mut engine, "bulk", 1, &queue_manifest(&["jobs"])).unwrap();
    for item in 0..20 {
        engine
            .enqueue(DB, "jobs", &n(item as f64), 0, 3, 1)
            .unwrap();
    }
    // A worker holds a claim and keeps its place in line for more.
    let waiting = ClaimRequest {
        scope: "",
        owner: Some("w"),
        limit: 1,
        lease_ms: 60_000,
        wait_ms: Some(60_000),
    };
    let held = engine.claim_queue(DB, "jobs", &waiting, 2).unwrap();
    assert_eq!(held[0].fence, 1);
    // An enqueue planned before the removal reaches the log after it.
    let late = engine
        .plan_enqueue(DB, &NewItem::new("jobs", &n(99.0), 0, 3), 3, [7; 16])
        .unwrap();
    let late = encode_patch(&Patch {
        failed: false,
        db: DB.to_vec(),
        versions: Vec::new(),
        records: late.writes,
        indexes: Vec::new(),
        derived: Vec::new(),
        drops: Vec::new(),
        receipts: Vec::new(),
        result: b"null".to_vec(),
        expected_revision: None,
        base_revision: late.revision,
    })
    .unwrap();

    engine.remove_module(DB, "bulk", true).unwrap().unwrap();
    let outcome = engine.apply_patch(&late, applied(&engine) + 1).unwrap();
    assert_eq!(outcome.result.unwrap_err().code, QUEUE_PURGING);
    // One page in, the queue's ID and fence rows are gone and its items
    // are not: no worker or operator touches it until the purge ends.
    engine.purge_local(DB, 5).unwrap().unwrap().unwrap();
    let refused = [
        engine.enqueue(DB, "jobs", &n(1.0), 0, 3, 4).err(),
        engine.claim_queue(DB, "jobs", &waiting, 4).err(),
        engine
            .complete_queue(DB, "jobs", &held[0].id, &held[0].token, 4)
            .err(),
        engine.get_queue_item(DB, "jobs", &held[0].id).err(),
        engine.queue_stats(DB, "jobs").err(),
        engine.reader().queue_stats(DB, "jobs").err(),
    ];
    for failure in refused {
        let failure = failure.expect("refused");
        assert_eq!(failure.code, QUEUE_PURGING, "{failure:?}");
        assert!(failure.message.contains("module `bulk`"), "{failure:?}");
    }
    engine.enqueue(DB, "other", &n(1.0), 0, 3, 4).unwrap();

    purge_all(&mut engine);
    assert!(queue_rows(&engine, "jobs").is_empty());
    // A module declaring the queue again starts it afresh: no stale
    // line place, no item from before, fences from 1.
    deploy(&mut engine, "newcomer", 2, &queue_manifest(&["jobs"])).unwrap();
    assert!(
        engine
            .claim_queue(DB, "jobs", &waiting, 5)
            .unwrap()
            .is_empty()
    );
    engine.enqueue(DB, "jobs", &n(1.0), 0, 3, 6).unwrap();
    let fresh = engine.claim_queue(DB, "jobs", &waiting, 7).unwrap();
    assert_eq!((fresh[0].fence, fresh[0].payload.clone()), (1, n(1.0)));
    teardown(engine, path);
}

fn enqueue_from(engine: &mut Engine, module: &str, queue: &str) -> Result<Value, Failure> {
    engine.invoke_mutation(
        DB,
        module,
        "enqueue",
        &obj(vec![("queue", s(queue))]),
        &MutationOptions::default(),
    )
}

#[test]
fn a_module_uses_only_the_queues_it_declares() {
    let (mut engine, path) = setup("declared");
    deploy(&mut engine, "mailer", 1, &queue_manifest(&["mail"])).unwrap();
    deploy(&mut engine, "app", 2, &queue_manifest(&["jobs"])).unwrap();
    enqueue_from(&mut engine, "mailer", "mail").unwrap();
    enqueue_from(&mut engine, "app", "jobs").unwrap();
    // `app` would put its work in `mailer`'s queue, which a purge of
    // `mailer` deletes: refused.
    let refused = enqueue_from(&mut engine, "app", "mail").unwrap_err();
    assert_eq!(refused.code, QUEUE_UNDECLARED);
    assert!(
        refused
            .message
            .contains("module `app` does not declare queue `mail`"),
        "{refused:?}"
    );
    let jobs = queue_rows(&engine, "jobs");
    engine.remove_module(DB, "mailer", true).unwrap().unwrap();
    purge_all(&mut engine);
    assert!(queue_rows(&engine, "mail").is_empty());
    assert_eq!(queue_rows(&engine, "jobs"), jobs);
    assert_eq!(
        enqueue_from(&mut engine, "app", "mail").unwrap_err().code,
        QUEUE_UNDECLARED
    );
    teardown(engine, path);
}

/// A module whose `tidy` task fails every run.
fn task_manifest() -> Value {
    let mut manifest = queue_manifest(&[]);
    if let Value::Object(map) = &mut manifest {
        map.insert(
            "tasks".to_string(),
            arr(vec![obj(vec![("name", s("tidy"))])]),
        );
    }
    manifest
}

/// Run `module`'s `tidy` once, recording its failure.
fn fail_tidy(engine: &mut Engine, module: &str) {
    let TaskPrepare::Failed { prepared, .. } = engine.prepare_task_run(DB, module, "tidy").unwrap()
    else {
        panic!("tidy runs and fails");
    };
    let position = applied(engine) + 1;
    engine.apply_patch(&prepared.patch, position).unwrap();
}

fn failures(engine: &Engine, module: &str) -> u64 {
    engine.reader().tasks(DB, module).unwrap()[0].failures
}

#[test]
fn a_purge_deletes_its_module_task_failures() {
    let (mut engine, path) = setup("tasks");
    deploy(&mut engine, "chores", 1, &task_manifest()).unwrap();
    deploy(&mut engine, "app", 2, &task_manifest()).unwrap();
    fail_tidy(&mut engine, "chores");
    fail_tidy(&mut engine, "app");
    assert_eq!(
        (failures(&engine, "chores"), failures(&engine, "app")),
        (1, 1)
    );
    engine.remove_module(DB, "chores", true).unwrap().unwrap();
    purge_all(&mut engine);
    // Deployed again under the same name, it inherits no backoff and no
    // last error; the other module keeps its own.
    deploy(&mut engine, "chores", 1, &task_manifest()).unwrap();
    let status = engine.reader().tasks(DB, "chores").unwrap();
    assert_eq!((status[0].failures, status[0].error.is_none()), (0, true));
    assert_eq!(failures(&engine, "app"), 1);
    teardown(engine, path);
}
