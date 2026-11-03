//! Staged-deployment integration tests: the old bundle serves while the
//! new bundle's indexes build, then activation swaps atomically.

mod fixtures;

use fixtures::{
    Guest, arr, field, hc, index, json_schema, k, n, num_schema, obj, object_schema, s, str_schema,
};

use flower_engine::engine::{CallOptions, Engine, MutationOptions};
use flower_engine::host::qualify_index;
use flower_engine::runtime::{CallKind, ClosureRuntime, HostCall};
use flower_engine::storage::GroupStore;
use flower_sdk::abi::HostOp;
use flower_sdk::error::codes::MODULE_PRIVATE;
use flower_sdk::{Failure, Value, fail};

fn collection(name: &str, key: Value, value: Value, indexes: Vec<Value>) -> Value {
    obj(vec![
        ("name", s(name)),
        ("key", key),
        ("value", value),
        ("indexes", arr(indexes)),
        ("references", arr(vec![])),
    ])
}

fn cells(indexes: Vec<Value>) -> Value {
    collection(
        "cells",
        str_schema(),
        object_schema(vec![field("v", num_schema(), false)]),
        indexes,
    )
}

fn definition(name: &str, kind: &str) -> Value {
    obj(vec![
        ("name", s(name)),
        ("kind", s(kind)),
        ("args", json_schema()),
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

fn setup(test: &str) -> (Engine, std::path::PathBuf) {
    let mut path = std::env::temp_dir();
    path.push(format!("flower-staging-{test}-{}", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let store = GroupStore::create(&path).unwrap();
    (Engine::new(store), path)
}

fn teardown(engine: Engine, path: std::path::PathBuf) {
    drop(engine);
    let _ = std::fs::remove_file(&path);
}

fn set(host: &mut dyn HostCall, collection: &str, key: &str, value: Value) -> Result<(), Failure> {
    hc(host, HostOp::Set, vec![s(""), s(collection), s(key), value])?;
    Ok(())
}

fn decode_page(page: &Value) -> (Vec<(Value, Value)>, Option<String>) {
    let Value::Object(map) = page else {
        panic!("page is not an object");
    };
    let rows = match map.get("rows") {
        Some(Value::Array(rows)) => rows
            .iter()
            .map(|row| match row {
                Value::Array(pair) if pair.len() == 2 => (pair[0].clone(), pair[1].clone()),
                _ => panic!("row is not a pair"),
            })
            .collect(),
        _ => panic!("page has no rows"),
    };
    let cursor = match map.get("cursor") {
        None | Some(Value::Null) => None,
        Some(Value::String(cursor)) => Some(cursor.clone()),
        _ => panic!("cursor is not text"),
    };
    (rows, cursor)
}

fn bounds(prefix: Vec<&str>, limit: u32) -> Value {
    obj(vec![
        ("prefix", arr(prefix.into_iter().map(s).collect())),
        ("gt", Value::Null),
        ("gte", Value::Null),
        ("lt", Value::Null),
        ("lte", Value::Null),
        ("limit", n(limit as f64)),
        ("after", Value::Null),
        ("reverse", Value::Bool(false)),
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

fn default_mutation() -> MutationOptions {
    MutationOptions::default()
}

/// Count committed entries of one index, whatever the descriptors say.
fn index_entry_count(engine: &Engine, module: &str, collection: &str, index: &str) -> usize {
    let read = engine.store().begin_read().unwrap();
    let qualified = qualify_index(module, collection, index);
    let Some(id) = read.intern_id(b"db", qualified.as_bytes()).unwrap() else {
        return 0;
    };
    read.scan_index(
        b"db",
        id,
        1,
        &[],
        flower_engine::storage::ScanRange::default(),
    )
    .unwrap()
    .len()
}

fn seed_cells(engine: &mut Engine, rows: &[(&str, f64)]) {
    for (key, v) in rows {
        engine
            .invoke_mutation(
                b"db",
                "app",
                "put",
                &obj(vec![("key", s(key)), ("v", n(*v))]),
                &default_mutation(),
            )
            .unwrap();
    }
}

fn probe(engine: &Engine, prefix: Vec<&str>) -> Value {
    engine
        .invoke_query(
            b"db",
            "app",
            "probe",
            &bounds(prefix, 100),
            &CallOptions::default(),
        )
        .unwrap()
}

/// Runtime with version-tagged reads: `put` writes cells, `probe` ranges
/// the `by_v` index, `version` reports the bundle tag.
fn versioned_runtime(tag: &'static str) -> ClosureRuntime<impl Guest> {
    ClosureRuntime::new(
        move |kind: CallKind, _module: &str, name: &str, args: &Value, host: &mut dyn HostCall| {
            match (kind, name) {
                (CallKind::Mutation, "put") => {
                    let Value::Object(map) = args else {
                        return Err(fail("TYPE_MISMATCH", "put takes an object"));
                    };
                    let key = match map.get("key") {
                        Some(Value::String(key)) => key.clone(),
                        _ => return Err(fail("TYPE_MISMATCH", "put key is not a string")),
                    };
                    let value = match map.get("v") {
                        Some(Value::Number(_)) => map.get("v").unwrap().clone(),
                        _ => return Err(fail("TYPE_MISMATCH", "put v is not a number")),
                    };
                    set(host, "cells", &k(&key), obj(vec![("v", value)]))?;
                    Ok(Value::Null)
                }
                (CallKind::Query, "probe") => hc(
                    host,
                    HostOp::Range,
                    vec![s(""), s("cells"), s("by_v"), args.clone()],
                ),
                (CallKind::Query, "version") => Ok(s(tag)),
                _ => Err(fail("UNKNOWN_METHOD", name.to_string())),
            }
        },
    )
}

fn cells_manifest(indexed: bool) -> Value {
    let indexes = if indexed {
        vec![index("by_v", vec!["v"])]
    } else {
        vec![]
    };
    manifest(
        vec![cells(indexes)],
        vec![
            definition("put", "mutation"),
            definition("probe", "query"),
            definition("version", "query"),
        ],
    )
}

/// `cells` plus, when `with_notes`, an unindexed `notes` collection;
/// `note` writes `[key, value]` to notes, `read_note` gets one.
fn readded_manifest(with_notes: bool) -> Value {
    let mut collections = vec![cells(vec![])];
    if with_notes {
        collections.push(collection("notes", str_schema(), json_schema(), vec![]));
    }
    manifest(
        collections,
        vec![
            definition("note", "mutation"),
            definition("read_note", "query"),
        ],
    )
}

fn readded_runtime() -> ClosureRuntime<impl Guest> {
    ClosureRuntime::new(
        |kind: CallKind, _module: &str, name: &str, args: &Value, host: &mut dyn HostCall| {
            let key = |args: &Value| match args {
                Value::Array(items) => match items.first() {
                    Some(Value::String(key)) => Ok(k(key)),
                    _ => Err(fail("TYPE_MISMATCH", "a note key is a string")),
                },
                _ => Err(fail("TYPE_MISMATCH", "note takes [key, value]")),
            };
            match (kind, name) {
                (CallKind::Mutation, "note") => {
                    let value = match args {
                        Value::Array(items) => items.get(1).cloned().unwrap_or(Value::Null),
                        _ => Value::Null,
                    };
                    set(host, "notes", &key(args)?, value)?;
                    Ok(Value::Null)
                }
                (CallKind::Query, "read_note") => {
                    hc(host, HostOp::Get, vec![s(""), s("notes"), s(&key(args)?)])
                }
                _ => Err(fail("UNKNOWN_METHOD", name.to_string())),
            }
        },
    )
}

#[test]
fn a_collection_re_added_without_new_indexes_survives_collection() {
    let (mut engine, path) = setup("readded");
    let deploy = |engine: &mut Engine, identity: u8, with_notes: bool| {
        engine
            .deploy(
                b"db",
                "app",
                [identity; 32],
                b"notes",
                &readded_manifest(with_notes),
                Box::new(readded_runtime()),
            )
            .unwrap();
    };
    let note = |engine: &mut Engine, key: &str| {
        engine
            .invoke_mutation(
                b"db",
                "app",
                "note",
                &arr(vec![s(key), s(key)]),
                &default_mutation(),
            )
            .unwrap();
    };
    let read = |engine: &Engine, key: &str| {
        engine
            .invoke_query(
                b"db",
                "app",
                "read_note",
                &arr(vec![s(key)]),
                &CallOptions::default(),
            )
            .unwrap()
    };
    deploy(&mut engine, 1, true);
    note(&mut engine, "old");
    // Dropping `notes` retires its rows to lazy collection.
    deploy(&mut engine, 2, false);
    assert_eq!(
        engine
            .staging_status(b"db", "app")
            .unwrap()
            .unwrap()
            .retired,
        1
    );
    // Adding it back reuses its interned ID with nothing to backfill:
    // the retired rows go now, and so does their retirement.
    deploy(&mut engine, 3, true);
    assert_eq!(read(&engine, "old"), Value::Null);
    note(&mut engine, "new");
    while !engine.staging_gc(b"db", "app", 1000).unwrap().done {}
    assert_eq!(read(&engine, "new"), s("new"));
    teardown(engine, path);
}

#[test]
fn instant_deploy_backfills_added_index() {
    let (mut engine, path) = setup("backfill");
    engine
        .deploy(
            b"db",
            "app",
            [1u8; 32],
            b"v1",
            &cells_manifest(false),
            Box::new(versioned_runtime("v1")),
        )
        .unwrap();
    seed_cells(&mut engine, &[("t1", 1.0), ("t2", 2.0), ("t3", 1.0)]);
    // The added index covers pre-existing rows: both v=1 rows range back.
    engine
        .deploy(
            b"db",
            "app",
            [2u8; 32],
            b"v2",
            &cells_manifest(true),
            Box::new(versioned_runtime("v2")),
        )
        .unwrap();
    let page = probe(&engine, vec!["1"]);
    assert_eq!(row_keys(&page), vec!["t1".to_string(), "t3".to_string()]);
    teardown(engine, path);
}

#[test]
fn old_bundle_serves_during_staging() {
    let (mut engine, path) = setup("serves");
    engine
        .deploy(
            b"db",
            "app",
            [1u8; 32],
            b"v1",
            &cells_manifest(false),
            Box::new(versioned_runtime("v1")),
        )
        .unwrap();
    seed_cells(&mut engine, &[("t1", 1.0), ("t2", 2.0)]);
    engine
        .deploy_staged(b"db", "app", [2u8; 32], b"v2", &cells_manifest(true))
        .unwrap();
    // Staged but undrained: the old bundle still answers.
    let out = engine
        .invoke_query(
            b"db",
            "app",
            "version",
            &Value::Null,
            &CallOptions::default(),
        )
        .unwrap();
    assert_eq!(out, s("v1"));
    let status = engine.staging_status(b"db", "app").unwrap().unwrap();
    assert!(!status.ready);
    assert_eq!(status.planned, 1);
    // Partially drained: still the old bundle.
    let progress = engine.staging_drain(b"db", "app", 1).unwrap();
    assert!(!progress.done);
    assert_eq!(progress.scanned, 1);
    let out = engine
        .invoke_query(
            b"db",
            "app",
            "version",
            &Value::Null,
            &CallOptions::default(),
        )
        .unwrap();
    assert_eq!(out, s("v1"));
    // Drained: ready, but the switch happens only at activation.
    let progress = engine.staging_drain(b"db", "app", 100).unwrap();
    assert!(progress.done);
    let out = engine
        .invoke_query(
            b"db",
            "app",
            "version",
            &Value::Null,
            &CallOptions::default(),
        )
        .unwrap();
    assert_eq!(out, s("v1"));
    let activation = engine
        .activate(b"db", "app", Box::new(versioned_runtime("v2")))
        .unwrap();
    assert_eq!(activation.identity, [2u8; 32]);
    assert!(!activation.bindings.is_empty());
    let out = engine
        .invoke_query(
            b"db",
            "app",
            "version",
            &Value::Null,
            &CallOptions::default(),
        )
        .unwrap();
    assert_eq!(out, s("v2"));
    let page = probe(&engine, vec!["1"]);
    assert_eq!(row_keys(&page), vec!["t1".to_string()]);
    teardown(engine, path);
}

#[test]
fn dual_write_covers_concurrent_writes() {
    let (mut engine, path) = setup("dual");
    engine
        .deploy(
            b"db",
            "app",
            [1u8; 32],
            b"v1",
            &cells_manifest(false),
            Box::new(versioned_runtime("v1")),
        )
        .unwrap();
    seed_cells(&mut engine, &[("t1", 1.0), ("t2", 2.0)]);
    engine
        .deploy_staged(b"db", "app", [2u8; 32], b"v2", &cells_manifest(true))
        .unwrap();
    // Drain one row, then write a row that sorts below the cursor: the
    // backfill scan will never reach it, so dual-write must index it.
    engine.staging_drain(b"db", "app", 1).unwrap();
    seed_cells(&mut engine, &[("t0", 1.0)]);
    engine.staging_drain(b"db", "app", 100).unwrap();
    engine
        .activate(b"db", "app", Box::new(versioned_runtime("v2")))
        .unwrap();
    let page = probe(&engine, vec!["1"]);
    assert_eq!(row_keys(&page), vec!["t0".to_string(), "t1".to_string()]);
    teardown(engine, path);
}

/// Prepare a `put` without committing it, returning the patch bytes.
fn prepare_put(engine: &mut Engine, key: &str, v: f64) -> Vec<u8> {
    match engine
        .prepare_mutation(
            b"db",
            "app",
            "put",
            &obj(vec![("key", s(key)), ("v", n(v))]),
            &default_mutation(),
        )
        .unwrap()
    {
        flower_engine::engine::PrepareOutcome::Patch(prepared) => prepared.patch,
        _ => panic!("prepare short-circuited"),
    }
}

#[test]
fn dual_write_covers_patches_prepared_before_staging() {
    // A leader prepares patches, then stages and drains past their
    // rows before they commit: the patches carry no staged index
    // writes, so the dual-write has to happen where they apply.
    let (mut engine, path) = setup("dual-prepared");
    engine
        .deploy(
            b"db",
            "app",
            [1u8; 32],
            b"v1",
            &cells_manifest(false),
            Box::new(versioned_runtime("v1")),
        )
        .unwrap();
    seed_cells(&mut engine, &[("t1", 1.0), ("t2", 2.0)]);
    let moved = prepare_put(&mut engine, "t1", 5.0);
    let added = prepare_put(&mut engine, "t0", 1.0);
    engine
        .deploy_staged(b"db", "app", [2u8; 32], b"v2", &cells_manifest(true))
        .unwrap();
    assert!(engine.staging_drain(b"db", "app", 100).unwrap().done);
    engine.apply_patch(&moved, 1).unwrap();
    engine.apply_patch(&added, 2).unwrap();
    engine
        .activate(b"db", "app", Box::new(versioned_runtime("v2")))
        .unwrap();
    assert_eq!(row_keys(&probe(&engine, vec!["1"])), vec!["t0".to_string()]);
    assert_eq!(row_keys(&probe(&engine, vec!["5"])), vec!["t1".to_string()]);
    assert_eq!(index_entry_count(&engine, "app", "cells", "by_v"), 3);
    teardown(engine, path);
}

#[test]
fn patches_never_carry_staged_index_writes() {
    // Staging is node-local, so a patch prepared on a staging leader
    // must not write the staged index on members that never staged.
    let (mut engine, path) = setup("dual-local");
    engine
        .deploy(
            b"db",
            "app",
            [1u8; 32],
            b"v1",
            &cells_manifest(false),
            Box::new(versioned_runtime("v1")),
        )
        .unwrap();
    engine
        .deploy_staged(b"db", "app", [2u8; 32], b"v2", &cells_manifest(true))
        .unwrap();
    let staged = {
        let read = engine.store().begin_read().unwrap();
        read.intern_id(b"db", qualify_index("app", "cells", "by_v").as_bytes())
            .unwrap()
            .expect("staging interned the added index")
    };
    let bytes = prepare_put(&mut engine, "t1", 1.0);
    let patch = flower_engine::patch::decode_patch(&bytes).unwrap();
    assert_eq!(patch.records.len(), 1);
    assert!(
        patch.indexes.iter().all(|entry| entry.index != staged),
        "the patch carries staged index writes: {:?}",
        patch.indexes
    );
    // The staging node still dual-writes as the patch applies.
    engine.apply_patch(&bytes, 1).unwrap();
    assert_eq!(index_entry_count(&engine, "app", "cells", "by_v"), 1);
    teardown(engine, path);
}

#[test]
fn cancel_collects_partial_backfill() {
    let (mut engine, path) = setup("cancel");
    engine
        .deploy(
            b"db",
            "app",
            [1u8; 32],
            b"v1",
            &cells_manifest(false),
            Box::new(versioned_runtime("v1")),
        )
        .unwrap();
    seed_cells(&mut engine, &[("t1", 1.0), ("t2", 2.0)]);
    engine
        .deploy_staged(b"db", "app", [2u8; 32], b"v2", &cells_manifest(true))
        .unwrap();
    engine.staging_drain(b"db", "app", 1).unwrap();
    assert_eq!(index_entry_count(&engine, "app", "cells", "by_v"), 1);
    let qualified = qualify_index("app", "cells", "by_v");
    let read = engine.store().begin_read().unwrap();
    let staged = read
        .intern_id(b"db", qualified.as_bytes())
        .unwrap()
        .unwrap();
    drop(read);
    engine.cancel_staging(b"db", "app").unwrap();
    // The old bundle still serves. The index's name was interned only
    // here, so its partial backfill purges at once and the name is
    // released for whatever binds that ID next.
    let out = engine
        .invoke_query(
            b"db",
            "app",
            "version",
            &Value::Null,
            &CallOptions::default(),
        )
        .unwrap();
    assert_eq!(out, s("v1"));
    let read = engine.store().begin_read().unwrap();
    assert_eq!(read.intern_id(b"db", qualified.as_bytes()).unwrap(), None);
    assert_eq!(read.intern_name(b"db", staged).unwrap(), None);
    let left = read
        .scan_index(
            b"db",
            staged,
            1,
            &[],
            flower_engine::storage::ScanRange::default(),
        )
        .unwrap();
    assert!(left.is_empty());
    drop(read);
    assert!(engine.staging_status(b"db", "app").unwrap().is_none());
    let collected = engine.staging_gc(b"db", "app", 100).unwrap();
    assert!(collected.done);
    assert_eq!(collected.collected, 0);
    teardown(engine, path);
}

#[test]
fn retired_index_is_collected() {
    let (mut engine, path) = setup("retire");
    engine
        .deploy(
            b"db",
            "app",
            [1u8; 32],
            b"v1",
            &cells_manifest(true),
            Box::new(versioned_runtime("v1")),
        )
        .unwrap();
    seed_cells(&mut engine, &[("t1", 1.0), ("t2", 2.0)]);
    assert_eq!(index_entry_count(&engine, "app", "cells", "by_v"), 2);
    // Dropping the index retires its entries without deleting them: the
    // activation stays fast and collection follows lazily.
    engine
        .deploy(
            b"db",
            "app",
            [2u8; 32],
            b"v2",
            &cells_manifest(false),
            Box::new(versioned_runtime("v2")),
        )
        .unwrap();
    assert_eq!(index_entry_count(&engine, "app", "cells", "by_v"), 2);
    let status = engine.staging_status(b"db", "app").unwrap().unwrap();
    assert_eq!(status.retired, 1);
    let collected = engine.staging_gc(b"db", "app", 1).unwrap();
    assert!(!collected.done);
    assert_eq!(collected.collected, 1);
    let collected = engine.staging_gc(b"db", "app", 100).unwrap();
    assert!(collected.done);
    assert_eq!(collected.collected, 1);
    assert_eq!(index_entry_count(&engine, "app", "cells", "by_v"), 0);
    assert!(engine.staging_status(b"db", "app").unwrap().is_none());
    teardown(engine, path);
}

#[test]
fn restart_resumes_drain() {
    let (mut engine, path) = setup("resume");
    engine
        .deploy(
            b"db",
            "app",
            [1u8; 32],
            b"v1",
            &cells_manifest(false),
            Box::new(versioned_runtime("v1")),
        )
        .unwrap();
    seed_cells(&mut engine, &[("t1", 1.0), ("t2", 2.0), ("t3", 1.0)]);
    engine
        .deploy_staged(b"db", "app", [2u8; 32], b"v2", &cells_manifest(true))
        .unwrap();
    engine.staging_drain(b"db", "app", 1).unwrap();
    drop(engine);
    // Reopen: descriptors reload from the active bundle, the drain
    // resumes from its persisted cursor, and activation swaps.
    let store = GroupStore::open(&path).unwrap();
    let mut engine = Engine::new(store);
    engine
        .reload_module(b"db", "app", Box::new(versioned_runtime("v1")))
        .unwrap();
    let status = engine.staging_status(b"db", "app").unwrap().unwrap();
    assert!(!status.ready);
    let out = engine
        .invoke_query(
            b"db",
            "app",
            "version",
            &Value::Null,
            &CallOptions::default(),
        )
        .unwrap();
    assert_eq!(out, s("v1"));
    let progress = engine.staging_drain(b"db", "app", 100).unwrap();
    assert!(progress.done);
    assert_eq!(progress.scanned, 2);
    engine
        .activate(b"db", "app", Box::new(versioned_runtime("v2")))
        .unwrap();
    let page = probe(&engine, vec!["1"]);
    assert_eq!(row_keys(&page), vec!["t1".to_string(), "t3".to_string()]);
    teardown(engine, path);
}

#[test]
fn follower_applies_activation() {
    let (mut leader, leader_path) = setup("follow-a");
    leader
        .deploy(
            b"db",
            "app",
            [1u8; 32],
            b"v1",
            &cells_manifest(false),
            Box::new(versioned_runtime("v1")),
        )
        .unwrap();
    seed_cells(&mut leader, &[("t1", 1.0), ("t2", 2.0)]);
    drop(leader);
    // Snapshot the v1 file for the follower, then stage, drain and
    // prepare on the leader. The follower never staged: apply installs
    // the bindings, builds the backfill and swaps.
    let mut follower_path = std::env::temp_dir();
    follower_path.push(format!("flower-staging-follow-b-{}", std::process::id()));
    let _ = std::fs::remove_file(&follower_path);
    std::fs::copy(&leader_path, &follower_path).unwrap();
    let manifest_v2 = cells_manifest(true);
    let store = GroupStore::open(&leader_path).unwrap();
    let mut leader = Engine::new(store);
    leader
        .reload_module(b"db", "app", Box::new(versioned_runtime("v1")))
        .unwrap();
    leader
        .deploy_staged(b"db", "app", [2u8; 32], b"v2", &manifest_v2)
        .unwrap();
    let progress = leader.staging_drain(b"db", "app", 100).unwrap();
    assert!(progress.done);
    let activation = leader.prepare_activation(b"db", "app").unwrap();
    assert!(!activation.bindings.is_empty());
    let store = GroupStore::open(&follower_path).unwrap();
    let mut follower = Engine::new(store);
    follower
        .reload_module(b"db", "app", Box::new(versioned_runtime("v1")))
        .unwrap();
    follower
        .apply_activation(
            b"db",
            "app",
            b"v2",
            &manifest_v2,
            &activation,
            Box::new(versioned_runtime("v2")),
        )
        .unwrap();
    let page = probe(&follower, vec!["1"]);
    assert_eq!(row_keys(&page), vec!["t1".to_string()]);
    let out = follower
        .invoke_query(
            b"db",
            "app",
            "version",
            &Value::Null,
            &CallOptions::default(),
        )
        .unwrap();
    assert_eq!(out, s("v2"));
    teardown(leader, leader_path);
    teardown(follower, follower_path);
}

#[test]
fn derived_outcomes_re_evaluate_after_activation() {
    fn runtime(tag: &'static str) -> ClosureRuntime<impl Guest> {
        ClosureRuntime::new(
            move |kind: CallKind,
                  _module: &str,
                  name: &str,
                  args: &Value,
                  host: &mut dyn HostCall| {
                match (kind, name) {
                    (CallKind::Mutation, "put") => {
                        set(host, "cells", &k("t1"), obj(vec![("v", n(1.0))]))?;
                        Ok(Value::Null)
                    }
                    (CallKind::Derived, "answer") => Ok(s(tag)),
                    (CallKind::Query, "read") => hc(
                        host,
                        HostOp::GetDerived,
                        vec![s(""), s("answer"), args.clone()],
                    ),
                    _ => Err(fail("UNKNOWN_METHOD", name.to_string())),
                }
            },
        )
    }
    fn derived_manifest() -> Value {
        manifest(
            vec![cells(vec![])],
            vec![
                definition("put", "mutation"),
                definition("answer", "derived"),
                definition("read", "query"),
            ],
        )
    }
    let (mut engine, path) = setup("derived");
    engine
        .deploy(
            b"db",
            "app",
            [1u8; 32],
            b"v1",
            &derived_manifest(),
            Box::new(runtime("v1")),
        )
        .unwrap();
    engine
        .invoke_mutation(b"db", "app", "put", &Value::Null, &default_mutation())
        .unwrap();
    let out = engine
        .invoke_query(b"db", "app", "read", &Value::Null, &CallOptions::default())
        .unwrap();
    assert_eq!(out, s("v1"));
    // The new bundle's identity retires the cached outcome: the next
    // read re-evaluates instead of serving the stale value.
    engine
        .deploy(
            b"db",
            "app",
            [2u8; 32],
            b"v2",
            &derived_manifest(),
            Box::new(runtime("v2")),
        )
        .unwrap();
    let out = engine
        .invoke_query(b"db", "app", "read", &Value::Null, &CallOptions::default())
        .unwrap();
    assert_eq!(out, s("v2"));
    teardown(engine, path);
}

#[test]
fn staging_rejects_concurrent_staging() {
    let (mut engine, path) = setup("concurrent");
    engine
        .deploy(
            b"db",
            "app",
            [1u8; 32],
            b"v1",
            &cells_manifest(false),
            Box::new(versioned_runtime("v1")),
        )
        .unwrap();
    engine
        .deploy_staged(b"db", "app", [2u8; 32], b"v2", &cells_manifest(true))
        .unwrap();
    let failure = engine
        .deploy_staged(b"db", "app", [3u8; 32], b"v3", &cells_manifest(true))
        .unwrap_err();
    assert!(
        failure.message.contains("already in progress"),
        "{failure:?}"
    );
    teardown(engine, path);
}

#[test]
fn activate_requires_ready() {
    let (mut engine, path) = setup("notready");
    engine
        .deploy(
            b"db",
            "app",
            [1u8; 32],
            b"v1",
            &cells_manifest(false),
            Box::new(versioned_runtime("v1")),
        )
        .unwrap();
    seed_cells(&mut engine, &[("t1", 1.0)]);
    engine
        .deploy_staged(b"db", "app", [2u8; 32], b"v2", &cells_manifest(true))
        .unwrap();
    let failure = engine
        .activate(b"db", "app", Box::new(versioned_runtime("v2")))
        .unwrap_err();
    assert!(failure.message.contains("not ready"), "{failure:?}");
    teardown(engine, path);
}

#[test]
fn staging_rejects_changed_index_fields() {
    let (mut engine, path) = setup("fields");
    engine
        .deploy(
            b"db",
            "app",
            [1u8; 32],
            b"v1",
            &cells_manifest(true),
            Box::new(versioned_runtime("v1")),
        )
        .unwrap();
    let mut narrower = cells_manifest(true);
    let Value::Object(map) = &mut narrower else {
        panic!("manifest is not an object");
    };
    let Some(Value::Array(collections)) = map.get_mut("collections") else {
        panic!("manifest has no collections");
    };
    let Value::Object(cells) = &mut collections[0] else {
        panic!("collection is not an object");
    };
    let Some(Value::Array(indexes)) = cells.get_mut("indexes") else {
        panic!("collection has no indexes");
    };
    // Keep the name, change the fields: the staging must refuse.
    let Value::Object(first) = &mut indexes[0] else {
        panic!("index is not an object");
    };
    first.insert("fields".to_string(), arr(vec![s("v"), s("v")]));
    let failure = engine
        .deploy_staged(b"db", "app", [2u8; 32], b"v2", &narrower)
        .unwrap_err();
    assert!(
        failure.message.contains("changed its fields"),
        "{failure:?}"
    );
    teardown(engine, path);
}

/// A second module's bundle: one indexed collection, one query.
fn notes_manifest() -> Value {
    manifest(
        vec![collection(
            "notes",
            str_schema(),
            object_schema(vec![field("tag", str_schema(), false)]),
            vec![index("by_tag", vec!["tag"])],
        )],
        vec![definition("version", "query")],
    )
}

/// Two members of one group at the same state: v1 of `app` serving
/// `t1` = 1 and `t2` = 2. The first plays the new leader, the second
/// the member deposed while it staged.
fn two_members(test: &str) -> (Engine, std::path::PathBuf, Engine, std::path::PathBuf) {
    let (mut leader, leader_path) = setup(&format!("{test}-a"));
    leader
        .deploy(
            b"db",
            "app",
            [1u8; 32],
            b"v1",
            &cells_manifest(false),
            Box::new(versioned_runtime("v1")),
        )
        .unwrap();
    seed_cells(&mut leader, &[("t1", 1.0), ("t2", 2.0)]);
    drop(leader);
    let mut deposed_path = std::env::temp_dir();
    deposed_path.push(format!("flower-staging-{test}-b-{}", std::process::id()));
    let _ = std::fs::remove_file(&deposed_path);
    std::fs::copy(&leader_path, &deposed_path).unwrap();
    let mut members = Vec::new();
    for path in [&leader_path, &deposed_path] {
        let mut engine = Engine::new(GroupStore::open(path).unwrap());
        engine
            .reload_module(b"db", "app", Box::new(versioned_runtime("v1")))
            .unwrap();
        members.push(engine);
    }
    let deposed = members.pop().unwrap();
    let leader = members.pop().unwrap();
    (leader, leader_path, deposed, deposed_path)
}

/// Stage, drain and prepare a bundle on the leader, as a deployment
/// does before proposing its activation.
fn stage_on(
    engine: &mut Engine,
    module: &str,
    identity: [u8; 32],
    manifest: &Value,
) -> flower_engine::engine::Activation {
    engine
        .deploy_staged(b"db", module, identity, b"wasm", manifest)
        .unwrap();
    while !engine.staging_drain(b"db", module, 100).unwrap().done {}
    engine.prepare_activation(b"db", module).unwrap()
}

/// Every binding of an activation resolves the same way on a member.
fn assert_bound(engine: &Engine, activation: &flower_engine::engine::Activation) {
    let read = engine.store().begin_read().unwrap();
    for (name, id) in &activation.bindings {
        assert_eq!(
            read.intern_name(b"db", *id).unwrap().as_deref(),
            Some(name.as_slice())
        );
        assert_eq!(read.intern_id(b"db", name).unwrap(), Some(*id));
    }
}

#[test]
fn deposed_leader_yields_to_another_modules_activation() {
    // The deposed member staged an index for `app` and backfilled part
    // of it under an ID only it allocated. The new leader stages a
    // different module whose names take the same IDs. Applying that
    // activation must replace the abandoned staging, not halt.
    let (mut leader, leader_path, mut deposed, deposed_path) = two_members("yield-other");
    deposed
        .deploy_staged(b"db", "app", [2u8; 32], b"v2", &cells_manifest(true))
        .unwrap();
    deposed.staging_drain(b"db", "app", 1).unwrap();
    let qualified = qualify_index("app", "cells", "by_v");
    let read = deposed.store().begin_read().unwrap();
    let abandoned = read
        .intern_id(b"db", qualified.as_bytes())
        .unwrap()
        .unwrap();
    drop(read);
    assert_eq!(index_entry_count(&deposed, "app", "cells", "by_v"), 1);
    let activation = stage_on(&mut leader, "other", [7u8; 32], &notes_manifest());
    assert!(
        activation.bindings.iter().any(|(_, id)| *id == abandoned),
        "the new leader's names must claim the abandoned ID"
    );
    deposed
        .apply_activation(
            b"db",
            "other",
            b"wasm",
            &notes_manifest(),
            &activation,
            Box::new(versioned_runtime("other")),
        )
        .unwrap();
    assert_bound(&deposed, &activation);
    // The abandoned staging left nothing behind: no row, no entries
    // under the ID another name now owns, and `app` still serves v1.
    assert!(deposed.staging_status(b"db", "app").unwrap().is_none());
    let read = deposed.store().begin_read().unwrap();
    assert_eq!(read.intern_id(b"db", qualified.as_bytes()).unwrap(), None);
    let left = read
        .scan_index(
            b"db",
            abandoned,
            1,
            &[],
            flower_engine::storage::ScanRange::default(),
        )
        .unwrap();
    assert!(left.is_empty());
    drop(read);
    let out = deposed
        .invoke_query(
            b"db",
            "app",
            "version",
            &Value::Null,
            &CallOptions::default(),
        )
        .unwrap();
    assert_eq!(out, s("v1"));
    teardown(leader, leader_path);
    teardown(deposed, deposed_path);
}

#[test]
fn deposed_leader_yields_to_another_target_of_its_module() {
    // The deposed member staged v2 of `app`; the new leader stages a
    // different v3 of the same module. The activation supersedes the
    // local staging instead of refusing it.
    let (mut leader, leader_path, mut deposed, deposed_path) = two_members("yield-target");
    deposed
        .deploy_staged(b"db", "app", [2u8; 32], b"v2", &cells_manifest(true))
        .unwrap();
    deposed.staging_drain(b"db", "app", 1).unwrap();
    let mut v3 = cells_manifest(true);
    if let Value::Object(map) = &mut v3
        && let Some(Value::Array(definitions)) = map.get_mut("definitions")
    {
        definitions.push(definition("extra", "query"));
    }
    let activation = stage_on(&mut leader, "app", [3u8; 32], &v3);
    deposed
        .apply_activation(
            b"db",
            "app",
            b"wasm",
            &v3,
            &activation,
            Box::new(versioned_runtime("v3")),
        )
        .unwrap();
    assert_bound(&deposed, &activation);
    let out = deposed
        .invoke_query(
            b"db",
            "app",
            "version",
            &Value::Null,
            &CallOptions::default(),
        )
        .unwrap();
    assert_eq!(out, s("v3"));
    assert_eq!(
        row_keys(&probe(&deposed, vec!["1"])),
        vec!["t1".to_string()]
    );
    assert_eq!(index_entry_count(&deposed, "app", "cells", "by_v"), 2);
    teardown(leader, leader_path);
    teardown(deposed, deposed_path);
}

#[test]
fn deposed_leader_rebuilds_its_target_from_the_bindings() {
    // The deposed member staged `other`, then v2 of `app`, so its IDs
    // for v2's names differ from the new leader's, which stages only
    // v2. The same target activates from the authoritative bindings:
    // the local staging rebuilds under them and `other` is abandoned.
    let (mut leader, leader_path, mut deposed, deposed_path) = two_members("yield-same");
    deposed
        .deploy_staged(b"db", "other", [7u8; 32], b"wasm", &notes_manifest())
        .unwrap();
    deposed
        .deploy_staged(b"db", "app", [2u8; 32], b"v2", &cells_manifest(true))
        .unwrap();
    while !deposed.staging_drain(b"db", "app", 100).unwrap().done {}
    let activation = stage_on(&mut leader, "app", [2u8; 32], &cells_manifest(true));
    deposed
        .apply_activation(
            b"db",
            "app",
            b"v2",
            &cells_manifest(true),
            &activation,
            Box::new(versioned_runtime("v2")),
        )
        .unwrap();
    assert_bound(&deposed, &activation);
    assert!(deposed.staging_status(b"db", "other").unwrap().is_none());
    assert_eq!(
        row_keys(&probe(&deposed, vec!["1"])),
        vec!["t1".to_string()]
    );
    assert_eq!(index_entry_count(&deposed, "app", "cells", "by_v"), 2);
    teardown(leader, leader_path);
    teardown(deposed, deposed_path);
}

/// Apply replicated staging steps on a member as one batch entry
/// applies them, answering each step's outcome.
fn apply_steps(
    engine: &Engine,
    steps: &[flower_engine::staging::StagingCommand],
) -> Vec<Result<Value, Failure>> {
    use flower_engine::batch::{Applied, BatchItem};
    let items: Vec<BatchItem> = steps
        .iter()
        .map(|command| BatchItem::Staging {
            db: b"db".to_vec(),
            command: command.clone(),
        })
        .collect();
    let staged = engine.store().begin_staged().unwrap();
    let applied = engine.commit_batch(&staged, &items, 1, 0).unwrap();
    staged.commit().unwrap();
    applied
        .into_iter()
        .map(|item| match item {
            Applied::Staging { result, .. } => result,
            other => panic!("a staging step applied as {other:?}"),
        })
        .collect()
}

/// A member's raw staging row for `module`.
fn staging_row(engine: &Engine, module: &str) -> Option<Vec<u8>> {
    engine
        .store()
        .begin_read()
        .unwrap()
        .meta(b"db", &flower_engine::staging::staging_key(module))
        .unwrap()
}

/// A member's staging phase for `app`.
fn phase(engine: &Engine) -> Option<flower_engine::staging::StagingPhase> {
    engine
        .staging_status(b"db", "app")
        .unwrap()
        .map(|status| status.phase)
}

#[test]
fn replicated_staging_builds_alike_on_every_member() {
    use flower_engine::staging::{StagingCommand, StagingPhase};
    let (mut leader, leader_path, mut follower, follower_path) = two_members("replicated");
    let stage = leader
        .prepare_stage(b"db", "app", [2u8; 32], b"v2", &cells_manifest(true))
        .unwrap()
        .expect("a new target stages");
    for member in [&leader, &follower] {
        apply_steps(member, std::slice::from_ref(&stage))[0]
            .as_ref()
            .unwrap();
    }
    assert_eq!(staging_row(&leader, "app"), staging_row(&follower, "app"));
    assert!(staging_row(&leader, "app").is_some());
    let status = leader.staging_status(b"db", "app").unwrap().unwrap();
    assert!(status.replicated && status.phase == StagingPhase::Building);
    // Staging the same target again proposes nothing.
    assert!(
        leader
            .prepare_stage(b"db", "app", [2u8; 32], b"v2", &cells_manifest(true))
            .unwrap()
            .is_none()
    );
    // One record per step, alike everywhere; a write between steps is
    // dual-written wherever it applies.
    let advance = StagingCommand::Advance {
        module: "app".to_string(),
        identity: [2u8; 32],
        records: 1,
    };
    for member in [&leader, &follower] {
        apply_steps(member, std::slice::from_ref(&advance))[0]
            .as_ref()
            .unwrap();
    }
    let patch = prepare_put(&mut leader, "t3", 3.0);
    leader.apply_patch(&patch, 1).unwrap();
    follower.apply_patch(&patch, 1).unwrap();
    let mut steps = 1;
    while phase(&leader) == Some(StagingPhase::Building) {
        for member in [&leader, &follower] {
            apply_steps(member, std::slice::from_ref(&advance))[0]
                .as_ref()
                .unwrap();
        }
        assert_eq!(staging_row(&leader, "app"), staging_row(&follower, "app"));
        steps += 1;
        assert!(steps < 10, "the backfill never finished");
    }
    assert_eq!(phase(&follower), Some(StagingPhase::Ready));
    for member in [&leader, &follower] {
        assert_eq!(index_entry_count(member, "app", "cells", "by_v"), 3);
    }
    // A step for another target is refused, alike, changing nothing.
    let other = StagingCommand::Advance {
        module: "app".to_string(),
        identity: [9u8; 32],
        records: 1,
    };
    let before = staging_row(&leader, "app");
    for member in [&leader, &follower] {
        let refused = apply_steps(member, std::slice::from_ref(&other))
            .remove(0)
            .unwrap_err();
        assert_eq!(refused.code, flower_engine::engine::DEPLOYMENT_REFUSED);
    }
    assert_eq!(staging_row(&leader, "app"), before);
    // The activation finds the backfill built on every member.
    let activation = leader.prepare_activation(b"db", "app").unwrap();
    for member in [&mut leader, &mut follower] {
        member
            .apply_activation(
                b"db",
                "app",
                b"v2",
                &cells_manifest(true),
                &activation,
                Box::new(versioned_runtime("v2")),
            )
            .unwrap();
        assert_eq!(row_keys(&probe(member, vec!["3"])), vec!["t3".to_string()]);
        assert!(member.staging_status(b"db", "app").unwrap().is_none());
    }
    teardown(leader, leader_path);
    teardown(follower, follower_path);
}

#[test]
fn a_cancelled_replicated_staging_is_collected_alike() {
    use flower_engine::staging::{StagingCommand, StagingPhase};
    let (leader, leader_path, follower, follower_path) = two_members("replicated-cancel");
    let stage = leader
        .prepare_stage(b"db", "app", [2u8; 32], b"v2", &cells_manifest(true))
        .unwrap()
        .unwrap();
    let advance = StagingCommand::Advance {
        module: "app".to_string(),
        identity: [2u8; 32],
        records: 1,
    };
    let cancel = StagingCommand::Cancel {
        module: "app".to_string(),
        identity: [2u8; 32],
    };
    for member in [&leader, &follower] {
        for outcome in apply_steps(member, &[stage.clone(), advance.clone(), cancel.clone()]) {
            outcome.unwrap();
        }
        assert_eq!(index_entry_count(member, "app", "cells", "by_v"), 1);
        assert_eq!(phase(member), Some(StagingPhase::Gc));
    }
    // Cancelling twice refuses: nothing is staged for that target.
    assert!(apply_steps(&leader, std::slice::from_ref(&cancel))[0].is_err());
    let collect = StagingCommand::Collect {
        module: "app".to_string(),
        keys: 1,
    };
    let mut steps = 0;
    while staging_row(&leader, "app").is_some() {
        for member in [&leader, &follower] {
            apply_steps(member, std::slice::from_ref(&collect))[0]
                .as_ref()
                .unwrap();
        }
        assert_eq!(staging_row(&leader, "app"), staging_row(&follower, "app"));
        steps += 1;
        assert!(steps < 10, "collection never finished");
    }
    for member in [&leader, &follower] {
        assert_eq!(index_entry_count(member, "app", "cells", "by_v"), 0);
    }
    teardown(leader, leader_path);
    teardown(follower, follower_path);
}

/// A module whose data is being purged stages once it is gone, as it
/// deploys: the leader prepares no staging of it, and a staging step
/// prepared before the purge began is refused alike on every member.
#[test]
fn a_replicated_staging_waits_for_its_modules_purge() {
    let (mut leader, leader_path, mut follower, follower_path) = two_members("replicated-purge");
    let stage = leader
        .prepare_stage(b"db", "app", [2u8; 32], b"v2", &cells_manifest(true))
        .unwrap()
        .unwrap();
    for member in [&mut leader, &mut follower] {
        let staged = member.store().begin_staged().unwrap();
        member
            .commit_removal(staged, b"db", "app", true)
            .unwrap()
            .unwrap();
    }
    let refused = leader
        .prepare_stage(b"db", "app", [2u8; 32], b"v2", &cells_manifest(true))
        .unwrap_err();
    assert_eq!(refused.code, flower_sdk::error::codes::MODULE_CONFLICT);
    assert!(
        refused.message.contains("still being purged"),
        "{refused:?}"
    );
    for member in [&leader, &follower] {
        let refused = apply_steps(member, std::slice::from_ref(&stage))
            .remove(0)
            .unwrap_err();
        assert_eq!(refused.code, flower_sdk::error::codes::MODULE_CONFLICT);
        assert!(staging_row(member, "app").is_none());
    }
    teardown(leader, leader_path);
    teardown(follower, follower_path);
}

#[test]
fn a_replicated_staging_takes_the_ids_a_deposed_members_staging_held() {
    // The deposed member staged `app` alone and backfilled part of it
    // under IDs only it allocated; the new leader's replicated staging
    // of another module binds the same IDs. Applying it must abandon
    // the local staging, not refuse or halt.
    let (leader, leader_path, mut deposed, deposed_path) = two_members("replicated-yield");
    deposed
        .deploy_staged(b"db", "app", [2u8; 32], b"v2", &cells_manifest(true))
        .unwrap();
    deposed.staging_drain(b"db", "app", 1).unwrap();
    let stage = leader
        .prepare_stage(b"db", "other", [7u8; 32], b"wasm", &notes_manifest())
        .unwrap()
        .unwrap();
    for member in [&leader, &deposed] {
        apply_steps(member, std::slice::from_ref(&stage))[0]
            .as_ref()
            .unwrap();
    }
    assert!(deposed.staging_status(b"db", "app").unwrap().is_none());
    assert_eq!(
        staging_row(&leader, "other"),
        staging_row(&deposed, "other")
    );
    let flower_engine::staging::StagingCommand::Stage { bindings, .. } = &stage else {
        unreachable!()
    };
    for member in [&leader, &deposed] {
        let read = member.store().begin_read().unwrap();
        for (name, id) in bindings {
            assert_eq!(read.intern_id(b"db", name).unwrap(), Some(*id));
        }
    }
    teardown(leader, leader_path);
    teardown(deposed, deposed_path);
}

/// Runtime whose derived `scaled` multiplies a cell's `v` by `factor`,
/// counting its evaluations; `read` gets it, `put` writes cells.
fn scaled_runtime(
    factor: f64,
    evaluations: std::sync::Arc<std::sync::atomic::AtomicUsize>,
) -> ClosureRuntime<impl Guest> {
    ClosureRuntime::new(
        move |kind: CallKind, _module: &str, name: &str, args: &Value, host: &mut dyn HostCall| {
            let field = |value: &Value, name: &str| match value {
                Value::Object(map) => map.get(name).cloned().unwrap_or(Value::Null),
                _ => Value::Null,
            };
            match (kind, name) {
                (CallKind::Mutation, "put") => {
                    let Value::String(key) = field(args, "key") else {
                        return Err(fail("TYPE_MISMATCH", "put key is not a string"));
                    };
                    set(host, "cells", &k(&key), obj(vec![("v", field(args, "v"))]))?;
                    Ok(Value::Null)
                }
                (CallKind::Derived, "scaled") => {
                    evaluations.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let Value::String(key) = field(args, "key") else {
                        return Err(fail("TYPE_MISMATCH", "scaled key is not a string"));
                    };
                    let row = hc(host, HostOp::Get, vec![s(""), s("cells"), s(&k(&key))])?;
                    let Value::Number(v) = field(&row, "v") else {
                        return Err(fail("NOT_FOUND", "no such cell"));
                    };
                    Ok(n(v * factor))
                }
                (CallKind::Query, "read") => hc(
                    host,
                    HostOp::GetDerived,
                    vec![s(""), s("scaled"), args.clone()],
                ),
                _ => Err(fail("UNKNOWN_METHOD", name.to_string())),
            }
        },
    )
}

/// `cells` with an always-materialized derived `scaled`.
fn scaled_manifest() -> Value {
    manifest(
        vec![cells(vec![])],
        vec![
            definition("put", "mutation"),
            obj(vec![
                ("name", s("scaled")),
                ("kind", s("derived")),
                ("args", json_schema()),
                ("materialize", s("always")),
            ]),
            definition("read", "query"),
        ],
    )
}

fn read_scaled(engine: &Engine, key: &str) -> Value {
    engine
        .invoke_query(
            b"db",
            "app",
            "read",
            &obj(vec![("key", s(key))]),
            &CallOptions::default(),
        )
        .unwrap()
}

#[test]
fn the_graph_phase_rebuilds_pinned_roots_before_activation() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use flower_engine::runtime::Runtime;
    use flower_engine::staging::StagingPhase;

    // Two members at v1 (`scaled` doubles) with three pinned roots.
    let v1 = Arc::new(AtomicUsize::new(0));
    let (mut first, first_path) = setup("graph-a");
    first
        .deploy(
            b"db",
            "app",
            [1u8; 32],
            b"v1",
            &scaled_manifest(),
            Box::new(scaled_runtime(2.0, v1.clone())),
        )
        .unwrap();
    for (key, v) in [("t1", 1.0), ("t2", 2.0), ("t3", 3.0)] {
        first
            .invoke_mutation(
                b"db",
                "app",
                "put",
                &obj(vec![("key", s(key)), ("v", n(v))]),
                &default_mutation(),
            )
            .unwrap();
        assert_eq!(read_scaled(&first, key), n(v * 2.0));
    }
    drop(first);
    let mut second_path = std::env::temp_dir();
    second_path.push(format!("flower-staging-graph-b-{}", std::process::id()));
    let _ = std::fs::remove_file(&second_path);
    std::fs::copy(&first_path, &second_path).unwrap();
    let mut members = Vec::new();
    for path in [&first_path, &second_path] {
        let mut engine = Engine::new(GroupStore::open(path).unwrap());
        engine
            .reload_module(b"db", "app", Box::new(scaled_runtime(2.0, v1.clone())))
            .unwrap();
        members.push(engine);
    }
    let mut follower = members.pop().unwrap();
    let mut leader = members.pop().unwrap();

    // Stage v2 (`scaled` triples): no index to build, so the graph
    // rebuilds first.
    let stage = leader
        .prepare_stage(b"db", "app", [2u8; 32], b"v2", &scaled_manifest())
        .unwrap()
        .unwrap();
    for member in [&leader, &follower] {
        apply_steps(member, std::slice::from_ref(&stage))[0]
            .as_ref()
            .unwrap();
        let status = member.staging_status(b"db", "app").unwrap().unwrap();
        assert_eq!(status.phase, StagingPhase::Rebuilding);
        assert_eq!(status.graph, 1);
    }
    let staged = Arc::new(AtomicUsize::new(0));
    let target: Arc<dyn Runtime> = Arc::new(scaled_runtime(3.0, staged.clone()));
    let page = |leader: &Engine| {
        leader
            .prepare_graph_page(
                b"db",
                "app",
                target.clone(),
                (1, std::time::Duration::from_secs(60)),
                1_000,
            )
            .unwrap()
            .expect("the graph is rebuilding")
    };
    // One root per page, installed alike on every member.
    let first_page = page(&leader);
    for member in [&leader, &follower] {
        apply_steps(member, std::slice::from_ref(&first_page))[0]
            .as_ref()
            .unwrap();
    }
    assert_eq!(staged.load(Ordering::SeqCst), 1);
    // A write to a rebuilt root's input invalidates its staged outcome,
    // as it invalidates the active one; the old code still serves.
    let patch = match leader
        .prepare_mutation(
            b"db",
            "app",
            "put",
            &obj(vec![("key", s("t1")), ("v", n(10.0))]),
            &default_mutation(),
        )
        .unwrap()
    {
        flower_engine::engine::PrepareOutcome::Patch(prepared) => prepared.patch,
        _ => panic!("prepare short-circuited"),
    };
    leader.apply_patch(&patch, 1).unwrap();
    follower.apply_patch(&patch, 1).unwrap();
    assert_eq!(read_scaled(&leader, "t1"), n(20.0));
    let mut pages = 1;
    while phase(&leader) == Some(StagingPhase::Rebuilding) {
        let next = page(&leader);
        for member in [&leader, &follower] {
            apply_steps(member, std::slice::from_ref(&next))[0]
                .as_ref()
                .unwrap();
        }
        assert_eq!(staging_row(&leader, "app"), staging_row(&follower, "app"));
        pages += 1;
        assert!(pages < 10, "the graph never finished");
    }
    assert_eq!(phase(&follower), Some(StagingPhase::Ready));
    assert_eq!(staged.load(Ordering::SeqCst), 3, "each root rebuilt once");
    let status = leader.staging_status(b"db", "app").unwrap().unwrap();
    assert_eq!(status.rebuilt, 3);

    // After the cutover, the rebuilt roots serve the target's values
    // without evaluating again; the one a write invalidated evaluates.
    let activation = leader.prepare_activation(b"db", "app").unwrap();
    for member in [&mut leader, &mut follower] {
        let after = Arc::new(AtomicUsize::new(0));
        member
            .apply_activation(
                b"db",
                "app",
                b"v2",
                &scaled_manifest(),
                &activation,
                Box::new(scaled_runtime(3.0, after.clone())),
            )
            .unwrap();
        assert!(member.staging_status(b"db", "app").unwrap().is_none());
        assert_eq!(read_scaled(member, "t2"), n(6.0));
        assert_eq!(read_scaled(member, "t3"), n(9.0));
        assert_eq!(after.load(Ordering::SeqCst), 0, "rebuilt roots serve");
        assert_eq!(read_scaled(member, "t1"), n(30.0));
        assert_eq!(
            after.load(Ordering::SeqCst),
            1,
            "the invalidated root evaluates"
        );
    }
    teardown(leader, first_path);
    teardown(follower, second_path);
}

/// `tasks` keyed by name with a `status`, indexed `by_status` when asked.
fn tasks(indexed: bool) -> Value {
    let indexes = if indexed {
        vec![index("by_status", vec!["status"])]
    } else {
        vec![]
    };
    collection(
        "tasks",
        str_schema(),
        object_schema(vec![field("status", str_schema(), false)]),
        indexes,
    )
}

/// `manifest` exporting `collections` to other modules.
fn exporting(mut manifest: Value, collections: Vec<&str>) -> Value {
    if let Value::Object(map) = &mut manifest {
        map.insert(
            "exports".to_string(),
            obj(vec![
                ("collections", arr(collections.into_iter().map(s).collect())),
                ("derived", arr(vec![])),
                ("methods", arr(vec![])),
            ]),
        );
    }
    manifest
}

/// Bundle A: `tasks` indexed by status and exported, with `open_count`
/// ranging the index and `count` scanning the collection.
fn tasks_a() -> Value {
    exporting(
        manifest(
            vec![tasks(true)],
            vec![
                definition("add", "mutation"),
                definition("open_count", "derived"),
                definition("count", "derived"),
                definition("read", "query"),
            ],
        ),
        vec!["tasks"],
    )
}

/// Bundle B: `tasks` without its index, or without `tasks` at all.
fn tasks_b(keeps_tasks: bool) -> Value {
    let collections = if keeps_tasks {
        vec![tasks(false)]
    } else {
        vec![collection("other", str_schema(), json_schema(), vec![])]
    };
    manifest(collections, vec![definition("add", "mutation")])
}

/// The guest every bundle here runs: `add` writes `[key, status]` to
/// `tasks`, `read` serves the derived value it names; `board`'s `seen`
/// scans `app`'s exported `tasks`, and `look` serves it or its failure.
fn tasks_runtime() -> ClosureRuntime<impl Guest> {
    ClosureRuntime::new(
        |kind: CallKind, _module: &str, name: &str, args: &Value, host: &mut dyn HostCall| {
            let rows = |page: Value| n(decode_page(&page).0.len() as f64);
            match (kind, name) {
                (CallKind::Mutation, "add") => {
                    let items = args.as_array().map(Vec::as_slice).unwrap_or_default();
                    let (Some(Value::String(key)), Some(status)) = (items.first(), items.get(1))
                    else {
                        return Err(fail("TYPE_MISMATCH", "add takes [key, status]"));
                    };
                    set(
                        host,
                        "tasks",
                        &k(key),
                        obj(vec![("status", status.clone())]),
                    )?;
                    Ok(Value::Null)
                }
                (CallKind::Derived, "open_count") => Ok(rows(hc(
                    host,
                    HostOp::Range,
                    vec![
                        s(""),
                        s("tasks"),
                        s("by_status"),
                        bounds(vec![&k("open")], 1000),
                    ],
                )?)),
                (CallKind::Derived, "count") => Ok(rows(hc(
                    host,
                    HostOp::Scan,
                    vec![s(""), s("tasks"), n(1000.0), Value::Null],
                )?)),
                (CallKind::Derived, "seen") => Ok(rows(hc(
                    host,
                    HostOp::Scan,
                    vec![s("app"), s("tasks"), n(1000.0), Value::Null],
                )?)),
                (CallKind::Query, "read") => hc(
                    host,
                    HostOp::GetDerived,
                    vec![s(""), args.clone(), Value::Null],
                ),
                // A refusal answers as its code, so the query commits
                // the failure `seen` cached.
                (CallKind::Query, "look") => Ok(
                    match hc(
                        host,
                        HostOp::GetDerived,
                        vec![s(""), s("seen"), Value::Null],
                    ) {
                        Ok(value) => value,
                        Err(failure) => s(&failure.code),
                    },
                ),
                _ => Err(fail("UNKNOWN_METHOD", name.to_string())),
            }
        },
    )
}

fn board_manifest() -> Value {
    manifest(
        vec![],
        vec![definition("seen", "derived"), definition("look", "query")],
    )
}

fn deploy_tasks(engine: &mut Engine, module: &str, identity: u8, manifest: &Value) {
    engine
        .deploy(
            b"db",
            module,
            [identity; 32],
            &[identity],
            manifest,
            Box::new(tasks_runtime()),
        )
        .unwrap();
}

fn add_task(engine: &mut Engine, key: &str, status: &str) {
    engine
        .invoke_mutation(
            b"db",
            "app",
            "add",
            &arr(vec![s(key), s(status)]),
            &default_mutation(),
        )
        .unwrap();
}

/// Run `module`'s query `name` with `derived` as its argument.
fn ask(engine: &Engine, module: &str, name: &str, derived: &str) -> Result<Value, Failure> {
    engine.invoke_query(b"db", module, name, &s(derived), &CallOptions::default())
}

fn collect_garbage(engine: &mut Engine, module: &str) {
    while !engine.staging_gc(b"db", module, 1000).unwrap().done {}
}

#[test]
fn code_deployed_again_never_serves_values_its_index_no_longer_backs() {
    let (mut engine, path) = setup("era-index");
    deploy_tasks(&mut engine, "app", 1, &tasks_a());
    add_task(&mut engine, "t1", "open");
    assert_eq!(ask(&engine, "app", "read", "open_count"), Ok(n(1.0)));
    // B drops the index: its writes maintain no entry of it, and the
    // retired entries wait for collection.
    deploy_tasks(&mut engine, "app", 2, &tasks_b(true));
    add_task(&mut engine, "t2", "open");
    add_task(&mut engine, "t3", "open");
    // A again: the re-added index purges and backfills all three rows,
    // so the value A cached the first time is stale.
    deploy_tasks(&mut engine, "app", 1, &tasks_a());
    assert_eq!(ask(&engine, "app", "read", "open_count"), Ok(n(3.0)));
    teardown(engine, path);
}

#[test]
fn code_deployed_again_never_serves_values_of_a_collection_it_dropped() {
    for collected in [true, false] {
        let (mut engine, path) = setup(&format!("era-collection-{collected}"));
        deploy_tasks(&mut engine, "app", 1, &tasks_a());
        add_task(&mut engine, "t1", "open");
        add_task(&mut engine, "t2", "done");
        assert_eq!(ask(&engine, "app", "read", "count"), Ok(n(2.0)));
        assert_eq!(ask(&engine, "app", "read", "open_count"), Ok(n(1.0)));
        // B drops `tasks`: collection empties it (or the re-add purges
        // it), and A comes back to an empty collection.
        deploy_tasks(&mut engine, "app", 2, &tasks_b(false));
        if collected {
            collect_garbage(&mut engine, "app");
        }
        deploy_tasks(&mut engine, "app", 1, &tasks_a());
        assert_eq!(ask(&engine, "app", "read", "count"), Ok(n(0.0)));
        assert_eq!(ask(&engine, "app", "read", "open_count"), Ok(n(0.0)));
        teardown(engine, path);
    }
}

#[test]
fn readers_in_other_modules_never_serve_what_a_redeployed_provider_retired() {
    let (mut engine, path) = setup("era-readers");
    deploy_tasks(&mut engine, "app", 1, &tasks_a());
    deploy_tasks(&mut engine, "board", 9, &board_manifest());
    add_task(&mut engine, "t1", "open");
    assert_eq!(ask(&engine, "board", "look", ""), Ok(n(1.0)));
    // B no longer exports `tasks`: `board` may not read it, cached or not.
    deploy_tasks(&mut engine, "app", 2, &tasks_b(false));
    assert_eq!(ask(&engine, "board", "look", ""), Ok(s(MODULE_PRIVATE)));
    collect_garbage(&mut engine, "app");
    // A again exports an empty `tasks`: neither the value cached before
    // B nor the refusal cached under it serves.
    deploy_tasks(&mut engine, "app", 1, &tasks_a());
    assert_eq!(ask(&engine, "board", "look", ""), Ok(n(0.0)));
    teardown(engine, path);
}

#[test]
fn readers_in_other_modules_see_a_provider_that_starts_exporting() {
    let (mut engine, path) = setup("era-refusals");
    deploy_tasks(&mut engine, "app", 2, &tasks_b(true));
    deploy_tasks(&mut engine, "board", 9, &board_manifest());
    add_task(&mut engine, "t1", "open");
    assert_eq!(ask(&engine, "board", "look", ""), Ok(s(MODULE_PRIVATE)));
    // A exports `tasks`: the refusal `board` cached no longer holds.
    deploy_tasks(&mut engine, "app", 1, &tasks_a());
    assert_eq!(ask(&engine, "board", "look", ""), Ok(n(1.0)));
    teardown(engine, path);
}

#[test]
fn members_mint_the_same_code_eras_through_a_snapshot_install() {
    let (mut leader, leader_path) = setup("era-snapshot-a");
    deploy_tasks(&mut leader, "app", 1, &tasks_a());
    add_task(&mut leader, "t1", "open");
    assert_eq!(ask(&leader, "app", "read", "open_count"), Ok(n(1.0)));
    deploy_tasks(&mut leader, "app", 2, &tasks_b(true));
    add_task(&mut leader, "t2", "open");
    // A member catches up from the leader's snapshot: A's cached value,
    // the staging row and the era counter travel together.
    let (meta, payload) = flower_engine::snapshot::encode_snapshot(leader.store(), b"db").unwrap();
    let mut member_path = std::env::temp_dir();
    member_path.push(format!(
        "flower-staging-era-snapshot-b-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&member_path);
    let store = GroupStore::create(&member_path).unwrap();
    flower_engine::snapshot::apply_snapshot(&store, &meta, &payload).unwrap();
    let mut member = Engine::new(store);
    member
        .reload_module(b"db", "app", Box::new(tasks_runtime()))
        .unwrap();
    // A again, staged on the leader and applied by both.
    let activation = stage_on(&mut leader, "app", [1u8; 32], &tasks_a());
    for engine in [&mut leader, &mut member] {
        engine
            .apply_activation(
                b"db",
                "app",
                b"wasm",
                &tasks_a(),
                &activation,
                Box::new(tasks_runtime()),
            )
            .unwrap();
        assert_eq!(ask(engine, "app", "read", "open_count"), Ok(n(2.0)));
    }
    let era = |engine: &Engine| {
        engine
            .store()
            .begin_read()
            .unwrap()
            .meta(b"db", &flower_engine::staging::code_era_key("app"))
            .unwrap()
    };
    assert!(era(&leader).is_some());
    assert_eq!(era(&leader), era(&member));
    teardown(leader, leader_path);
    teardown(member, member_path);
}
