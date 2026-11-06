//! Replication-core integration tests: patch prepare/apply, expected
//! revisions, snapshots, the intent log and intent preview, driven
//! through [`Engine`] with in-process closure runtimes.

mod fixtures;

use fixtures::{
    Guest, arr, field, hc, index, json_schema, k, n, num_schema, obj, object_schema, s, str_schema,
};

use flower_engine::engine::{
    ApplyOutcome, CallOptions, Engine, MutationOptions, PrepareOutcome, RetryIdentity,
};
use flower_engine::intent::IntentStatus;
use flower_engine::preview::default_preview_options;
use flower_engine::retry::ScopedIdentity;
use flower_engine::runtime::{CallKind, ClosureRuntime, HostCall};
use flower_engine::snapshot;
use flower_engine::storage::GroupStore;
use flower_engine::storage::meta;
use flower_sdk::abi::HostOp;
use flower_sdk::{Failure, Value, fail};

fn cells_manifest() -> Value {
    obj(vec![
        (
            "collections",
            arr(vec![obj(vec![
                ("name", s("cells")),
                ("key", str_schema()),
                (
                    "value",
                    object_schema(vec![field("v", num_schema(), false)]),
                ),
                ("indexes", arr(vec![index("by_v", vec!["v"])])),
                ("references", arr(vec![])),
            ])]),
        ),
        (
            "definitions",
            arr(vec![
                definition("put", "mutation"),
                definition("fail", "mutation"),
                definition("get", "query"),
            ]),
        ),
        ("triggers", arr(vec![])),
        ("http", arr(vec![])),
    ])
}

fn definition(name: &str, kind: &str) -> Value {
    obj(vec![
        ("name", s(name)),
        ("kind", s(kind)),
        ("args", json_schema()),
    ])
}

fn setup(test: &str) -> (Engine, std::path::PathBuf) {
    let mut path = std::env::temp_dir();
    path.push(format!("flower-replication-{test}-{}", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let store = GroupStore::create(&path).unwrap();
    let mut engine = Engine::new(store);
    engine
        .deploy(
            b"db",
            "app",
            [1u8; 32],
            b"v1",
            &cells_manifest(),
            Box::new(cells_runtime()),
        )
        .unwrap();
    (engine, path)
}

fn teardown(engine: Engine, path: std::path::PathBuf) {
    drop(engine);
    let _ = std::fs::remove_file(&path);
}

fn set(host: &mut dyn HostCall, collection: &str, key: &str, value: Value) -> Result<(), Failure> {
    hc(host, HostOp::Set, vec![s(""), s(collection), s(key), value])?;
    Ok(())
}

fn get(host: &mut dyn HostCall, collection: &str, key: &str) -> Result<Value, Failure> {
    hc(host, HostOp::Get, vec![s(""), s(collection), s(key)])
}

fn cells_runtime() -> ClosureRuntime<impl Guest> {
    ClosureRuntime::new(
        |kind: CallKind, _module: &str, name: &str, args: &Value, host: &mut dyn HostCall| match (
            kind, name,
        ) {
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
                Ok(s("put-ok"))
            }
            (CallKind::Mutation, "fail") => Err(fail("BOOM", "it broke")),
            (CallKind::Query, "get") => {
                let Value::Object(map) = args else {
                    return Err(fail("TYPE_MISMATCH", "get takes an object"));
                };
                let key = match map.get("key") {
                    Some(Value::String(key)) => key.clone(),
                    _ => return Err(fail("TYPE_MISMATCH", "get key is not a string")),
                };
                let value = get(host, "cells", &k(&key))?;
                match value {
                    Value::Null => Ok(Value::Null),
                    Value::Object(map) => Ok(map.get("v").cloned().unwrap_or(Value::Null)),
                    _ => Ok(value),
                }
            }
            _ => Err(fail("UNKNOWN_METHOD", name.to_string())),
        },
    )
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

fn put_args(key: &str, v: f64) -> Value {
    obj(vec![("key", s(key)), ("v", n(v))])
}

fn get_args(key: &str) -> Value {
    obj(vec![("key", s(key))])
}

fn revision(engine: &Engine, db: &[u8]) -> u64 {
    let read = engine.store().begin_read().unwrap();
    read.meta_u64(db, meta::REVISION).unwrap()
}

fn applied(engine: &Engine, db: &[u8]) -> u64 {
    let read = engine.store().begin_read().unwrap();
    read.meta_u64(db, meta::APPLIED_LOG).unwrap()
}

fn read_cell(engine: &Engine, key: &str) -> Value {
    engine
        .invoke_query(b"db", "app", "get", &get_args(key), &CallOptions::default())
        .unwrap()
}

fn unwrap_patch(outcome: PrepareOutcome) -> flower_engine::lane::PreparedPatch {
    match outcome {
        PrepareOutcome::Patch(prepared) => prepared,
        _ => panic!("prepare short-circuited"),
    }
}

#[test]
fn prepare_apply_round_trip() {
    let (mut engine, path) = setup("roundtrip");
    // Prepare commits nothing: the row is invisible until apply.
    let prepared = unwrap_patch(
        engine
            .prepare_mutation(
                b"db",
                "app",
                "put",
                &put_args("t1", 1.0),
                &default_mutation(),
            )
            .unwrap(),
    );
    assert_eq!(prepared.outcome.unwrap(), s("put-ok"));
    assert_eq!(read_cell(&engine, "t1"), Value::Null);
    assert_eq!(revision(&engine, b"db"), 0);
    let ApplyOutcome {
        result, revision, ..
    } = engine.apply_patch(&prepared.patch, 1).unwrap();
    assert_eq!(result.unwrap(), s("put-ok"));
    assert_eq!(revision, 1);
    assert_eq!(read_cell(&engine, "t1"), n(1.0));
    assert_eq!(applied(&engine, b"db"), 1);
    // A second patch applies at the next position with the next revision.
    let prepared = unwrap_patch(
        engine
            .prepare_mutation(
                b"db",
                "app",
                "put",
                &put_args("t2", 2.0),
                &default_mutation(),
            )
            .unwrap(),
    );
    let ApplyOutcome {
        result, revision, ..
    } = engine.apply_patch(&prepared.patch, 2).unwrap();
    assert!(result.is_ok());
    assert_eq!(revision, 2);
    assert_eq!(read_cell(&engine, "t2"), n(2.0));
    teardown(engine, path);
}

#[test]
fn prepare_failure_replicates_receipt() {
    let (mut engine, path) = setup("failpatch");
    let options = retry_options(b"intent-1", 7);
    let prepared = unwrap_patch(
        engine
            .prepare_mutation(b"db", "app", "fail", &Value::Null, &options)
            .unwrap(),
    );
    let failure = prepared.outcome.unwrap_err();
    assert_eq!(failure.code, "BOOM");
    // Failure patches persist only their receipt, without a revision bump.
    let ApplyOutcome {
        result, revision, ..
    } = engine.apply_patch(&prepared.patch, 1).unwrap();
    assert_eq!(result.unwrap_err().code, "BOOM");
    assert_eq!(revision, 0);
    assert_eq!(applied(&engine, b"db"), 1);
    // Retries replay the failure instead of re-executing: no patch.
    let failure = engine
        .prepare_mutation(b"db", "app", "fail", &Value::Null, &options)
        .unwrap_err();
    assert_eq!(failure.code, "BOOM");
    // Success receipts replay as values without proposing either.
    let options = retry_options(b"intent-2", 9);
    let prepared = unwrap_patch(
        engine
            .prepare_mutation(b"db", "app", "put", &put_args("t9", 9.0), &options)
            .unwrap(),
    );
    engine.apply_patch(&prepared.patch, 2).unwrap();
    let replay = engine
        .prepare_mutation(b"db", "app", "put", &put_args("t9", 9.0), &options)
        .unwrap();
    // The replay names where the original committed: the revision it
    // left and its log position.
    match replay {
        PrepareOutcome::Replayed(Ok(value), committed) => {
            assert_eq!(value, s("put-ok"));
            assert_eq!(
                committed,
                Some(flower_engine::retry::Committed {
                    revision: 1,
                    position: 2,
                })
            );
        }
        _ => panic!("retry did not replay"),
    }
    teardown(engine, path);
}

#[test]
fn expected_revision_rejects_locally() {
    let (mut engine, path) = setup("expected-local");
    let mut options = default_mutation();
    options.expected_revision = Some(0);
    engine
        .invoke_mutation(b"db", "app", "put", &put_args("t1", 1.0), &options)
        .unwrap();
    assert_eq!(revision(&engine, b"db"), 1);
    // The database moved: the same expectation now rejects.
    let failure = engine
        .invoke_mutation(b"db", "app", "put", &put_args("t2", 2.0), &options)
        .unwrap_err();
    assert_eq!(failure.code, "stale_revision");
    assert_eq!(read_cell(&engine, "t2"), Value::Null);
    teardown(engine, path);
}

#[test]
fn expected_revision_rejects_at_apply() {
    let (mut engine, path) = setup("expected-apply");
    let mut options = default_mutation();
    options.expected_revision = Some(0);
    let first = unwrap_patch(
        engine
            .prepare_mutation(b"db", "app", "put", &put_args("t1", 1.0), &options)
            .unwrap(),
    );
    let second = unwrap_patch(
        engine
            .prepare_mutation(b"db", "app", "put", &put_args("t2", 2.0), &options)
            .unwrap(),
    );
    engine.apply_patch(&first.patch, 1).unwrap();
    // Both prepares expected revision 0; only the first applies.
    let ApplyOutcome {
        result, revision, ..
    } = engine.apply_patch(&second.patch, 2).unwrap();
    assert_eq!(result.unwrap_err().code, "stale_revision");
    assert_eq!(revision, 1);
    assert_eq!(applied(&engine, b"db"), 2);
    assert_eq!(read_cell(&engine, "t2"), Value::Null);
    teardown(engine, path);
}

#[test]
fn version_mismatch_rejects_apply() {
    let (mut engine, path) = setup("versions");
    let prepared = unwrap_patch(
        engine
            .prepare_mutation(
                b"db",
                "app",
                "put",
                &put_args("t1", 1.0),
                &default_mutation(),
            )
            .unwrap(),
    );
    // Deploying a new bundle retires the patch's versions: every applier
    // rejects deterministically as stale (the submitter re-prepares)
    // instead of installing effects under the wrong code or halting.
    engine
        .deploy(
            b"db",
            "app",
            [2u8; 32],
            b"v2",
            &cells_manifest(),
            Box::new(cells_runtime()),
        )
        .unwrap();
    let ApplyOutcome {
        result, revision, ..
    } = engine.apply_patch(&prepared.patch, 1).unwrap();
    assert_eq!(result.unwrap_err().code, "stale_revision");
    assert_eq!(revision, 0);
    assert_eq!(applied(&engine, b"db"), 1);
    assert_eq!(read_cell(&engine, "t1"), Value::Null);
    teardown(engine, path);
}

#[test]
fn position_must_advance() {
    let (mut engine, path) = setup("position");
    let first = unwrap_patch(
        engine
            .prepare_mutation(
                b"db",
                "app",
                "put",
                &put_args("t1", 1.0),
                &default_mutation(),
            )
            .unwrap(),
    );
    engine.apply_patch(&first.patch, 1).unwrap();
    let second = unwrap_patch(
        engine
            .prepare_mutation(
                b"db",
                "app",
                "put",
                &put_args("t2", 2.0),
                &default_mutation(),
            )
            .unwrap(),
    );
    let failure = engine.apply_patch(&second.patch, 1).unwrap_err();
    assert!(failure.message.contains("does not advance"), "{failure:?}");
    engine.apply_patch(&second.patch, 2).unwrap();
    assert_eq!(read_cell(&engine, "t2"), n(2.0));
    teardown(engine, path);
}

#[test]
fn intent_lifecycle() {
    let (mut engine, path) = setup("intents");
    let args = put_args("t1", 1.0);
    let first = engine
        .enqueue_intent(b"db", "app/put", &args, b"id-1", &[7u8; 32], Some(3), 10)
        .unwrap();
    let second = engine
        .enqueue_intent(b"db", "app/put", &args, b"id-2", &[8u8; 32], None, 10)
        .unwrap();
    assert_eq!((first, second), (1, 2));
    // Duplicate identities return the existing sequence.
    let again = engine
        .enqueue_intent(b"db", "app/put", &args, b"id-1", &[7u8; 32], Some(3), 10)
        .unwrap();
    assert_eq!(again, 1);
    let intent = engine.get_intent(b"id-1").unwrap().unwrap();
    assert_eq!(intent.sequence, 1);
    assert_eq!(intent.db, b"db");
    assert_eq!(intent.expected_revision, Some(3));
    assert_eq!(intent.status, IntentStatus::Queued);
    let listed = engine.list_intents(Some(b"db")).unwrap();
    assert_eq!(listed.len(), 2);
    assert_eq!(listed[0].sequence, 1);
    // The lifecycle walks forward and rejects illegal jumps.
    engine
        .mark_intent(b"id-1", IntentStatus::Submitted)
        .unwrap();
    assert!(
        engine
            .mark_intent(b"id-1", IntentStatus::Committed { revision: 9 })
            .is_err()
    );
    engine
        .mark_intent(b"id-1", IntentStatus::Acknowledged)
        .unwrap();
    engine
        .mark_intent(b"id-1", IntentStatus::Committed { revision: 9 })
        .unwrap();
    assert!(engine.mark_intent(b"id-1", IntentStatus::Queued).is_err());
    assert!(engine.drop_intent(b"id-1").unwrap());
    assert!(!engine.drop_intent(b"id-1").unwrap());
    assert!(engine.get_intent(b"id-1").unwrap().is_none());
    assert!(
        engine
            .mark_intent(b"missing", IntentStatus::Submitted)
            .is_err()
    );
    // The bound counts live intents: one remains, so a bound of one is full.
    let failure = engine
        .enqueue_intent(b"db", "app/put", &args, b"id-3", &[9u8; 32], None, 1)
        .unwrap_err();
    assert_eq!(failure.code, "intent_queue_full");
    assert!(engine.drop_intent(b"id-2").unwrap());
    teardown(engine, path);
}

#[test]
fn preview_reads_own_intents() {
    let (mut engine, path) = setup("preview");
    engine
        .invoke_mutation(
            b"db",
            "app",
            "put",
            &put_args("t1", 1.0),
            &default_mutation(),
        )
        .unwrap();
    engine
        .enqueue_intent(
            b"db",
            "app/put",
            &put_args("t2", 2.0),
            b"id-1",
            &[7u8; 32],
            None,
            10,
        )
        .unwrap();
    // The failing intent ends the prefix: the intent after it never previews.
    engine
        .enqueue_intent(
            b"db",
            "app/fail",
            &Value::Null,
            b"id-2",
            &[8u8; 32],
            None,
            10,
        )
        .unwrap();
    engine
        .enqueue_intent(
            b"db",
            "app/put",
            &put_args("t3", 3.0),
            b"id-3",
            &[9u8; 32],
            None,
            10,
        )
        .unwrap();
    let options = default_preview_options();
    let preview = engine.preview(b"db", &options).unwrap();
    assert_eq!(preview.previewed, vec![b"id-1".to_vec()]);
    // Preview-mode reads see the snapshot plus the overlay.
    let out = engine
        .invoke_query_preview(
            b"db",
            "app",
            "get",
            &get_args("t2"),
            &preview.overlay,
            &CallOptions::default(),
        )
        .unwrap();
    assert_eq!(out, n(2.0));
    let out = engine
        .invoke_query_preview(
            b"db",
            "app",
            "get",
            &get_args("t3"),
            &preview.overlay,
            &CallOptions::default(),
        )
        .unwrap();
    assert_eq!(out, Value::Null);
    // The committed snapshot is untouched by the preview.
    assert_eq!(read_cell(&engine, "t2"), Value::Null);
    teardown(engine, path);
}

#[test]
fn preview_bound_ends_prefix() {
    let (mut engine, path) = setup("preview-bound");
    engine
        .enqueue_intent(
            b"db",
            "app/put",
            &put_args("t1", 1.0),
            b"id-1",
            &[7u8; 32],
            None,
            10,
        )
        .unwrap();
    let mut options = default_preview_options();
    options.max_journal_ops = 0;
    // A zero-operation overlay fits nothing: the prefix is empty.
    let preview = engine.preview(b"db", &options).unwrap();
    assert!(preview.previewed.is_empty());
    teardown(engine, path);
}

#[test]
fn snapshot_serves_after_install() {
    let (mut engine, path) = setup("snap-serve");
    engine
        .invoke_mutation(
            b"db",
            "app",
            "put",
            &put_args("t1", 1.0),
            &default_mutation(),
        )
        .unwrap();
    let (meta, payload) = snapshot::encode_snapshot(engine.store(), b"db").unwrap();
    let file = snapshot::encode_snapshot_file(&meta, &payload).unwrap();
    drop(engine);
    let _ = std::fs::remove_file(&path);
    let mut other = std::env::temp_dir();
    other.push(format!(
        "flower-replication-snap-copy-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&other);
    let store = GroupStore::create(&other).unwrap();
    let (meta, payload) = snapshot::decode_snapshot_file(&file).unwrap();
    snapshot::apply_snapshot(&store, &meta, &payload).unwrap();
    let mut engine = Engine::new(store);
    engine
        .reload_module(b"db", "app", Box::new(cells_runtime()))
        .unwrap();
    assert_eq!(read_cell(&engine, "t1"), n(1.0));
    // The install carries the applied position, so the next patch
    // continues the sequence instead of restarting it.
    let prepared = unwrap_patch(
        engine
            .prepare_mutation(
                b"db",
                "app",
                "put",
                &put_args("t2", 2.0),
                &default_mutation(),
            )
            .unwrap(),
    );
    let next = applied(&engine, b"db") + 1;
    engine.apply_patch(&prepared.patch, next).unwrap();
    assert_eq!(read_cell(&engine, "t2"), n(2.0));
    teardown(engine, other);
}
