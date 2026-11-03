//! Multi-database transaction tests: atomic local commits,
//! prepare-then-apply across participants, all-or-nothing stale
//! rejection, abort on step failure and step-list validation.

mod fixtures;

use fixtures::{arr, hc, k, obj, s};

use flower_engine::engine::{
    CallOptions, Engine, MultiStep, MutationOptions, PrepareOutcome, RetryIdentity,
};
use flower_engine::patch::{Patch, decode_multi_patch, encode_multi_patch};
use flower_engine::retry::ScopedIdentity;
use flower_engine::runtime::{CallKind, ClosureRuntime, HostCall, Runtime};
use flower_engine::storage::GroupStore;
use flower_engine::storage::meta;
use flower_sdk::abi::HostOp;
use flower_sdk::{Value, fail};

fn collection(name: &str) -> Value {
    obj(vec![
        ("name", s(name)),
        ("key", obj(vec![("type", s("string"))])),
        ("value", obj(vec![("type", s("json"))])),
        ("indexes", arr(vec![])),
        ("references", arr(vec![])),
    ])
}

fn definition(name: &str, kind: &str) -> Value {
    obj(vec![
        ("name", s(name)),
        ("kind", s(kind)),
        ("args", obj(vec![("type", s("json"))])),
    ])
}

fn manifest() -> Value {
    obj(vec![
        ("collections", arr(vec![collection("log")])),
        (
            "definitions",
            arr(vec![
                definition("put", "mutation"),
                definition("boom", "mutation"),
                definition("read", "query"),
            ]),
        ),
        ("triggers", arr(vec![])),
        ("http", arr(vec![])),
    ])
}

fn runtime() -> Box<dyn Runtime> {
    Box::new(ClosureRuntime::new(
        |kind: CallKind, _module: &str, name: &str, args: &Value, host: &mut dyn HostCall| {
            let arg = |field: &str| match args {
                Value::Object(map) => map.get(field).cloned().unwrap_or(Value::Null),
                _ => Value::Null,
            };
            match (kind, name) {
                (CallKind::Mutation, "put") => {
                    let Value::String(key) = arg("key") else {
                        return Err(fail("BAD_KEY", "key must be a string"));
                    };
                    hc(
                        host,
                        HostOp::Set,
                        vec![s(""), s("log"), s(&k(&key)), arg("value")],
                    )?;
                    Ok(arg("value"))
                }
                (CallKind::Mutation, "boom") => {
                    hc(
                        host,
                        HostOp::Set,
                        vec![s(""), s("log"), s(&k("staged")), s("nope")],
                    )?;
                    Err(fail("BOOM", "step failure"))
                }
                (CallKind::Query, "read") => {
                    let Value::String(key) = arg("key") else {
                        return Err(fail("BAD_KEY", "key must be a string"));
                    };
                    hc(host, HostOp::Get, vec![s(""), s("log"), s(&k(&key))])
                }
                _ => Err(fail("UNKNOWN_METHOD", name.to_string())),
            }
        },
    ))
}

fn setup(test: &str) -> (Engine, std::path::PathBuf) {
    let mut path = std::env::temp_dir();
    path.push(format!("flower-multi-{test}-{}", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let store = GroupStore::create(&path).unwrap();
    let mut engine = Engine::new(store);
    for db in [b"dba".as_slice(), b"dbb".as_slice()] {
        engine
            .deploy(db, "app", [3u8; 32], b"v1", &manifest(), runtime())
            .unwrap();
    }
    (engine, path)
}

fn teardown(engine: Engine, path: std::path::PathBuf) {
    drop(engine);
    let _ = std::fs::remove_file(&path);
}

fn options() -> MutationOptions {
    MutationOptions {
        call: CallOptions::default(),
        retry: None,
        idempotency_key: None,
        expected_revision: None,
    }
}

fn put_step(db: &[u8], key: &str, value: Value) -> MultiStep {
    MultiStep {
        db: db.to_vec(),
        module: "app".to_string(),
        method: "put".to_string(),
        args: obj(vec![("key", s(key)), ("value", value)]),
    }
}

fn revision(engine: &Engine, db: &[u8]) -> u64 {
    let read = engine.store().begin_read().unwrap();
    read.meta_u64(db, meta::REVISION).unwrap()
}

fn applied(engine: &Engine, db: &[u8]) -> u64 {
    let read = engine.store().begin_read().unwrap();
    read.meta_u64(db, meta::APPLIED_LOG).unwrap()
}

fn read_row(engine: &Engine, db: &[u8], key: &str) -> Value {
    engine
        .invoke_query(
            db,
            "app",
            "read",
            &obj(vec![("key", s(key))]),
            &CallOptions::default(),
        )
        .unwrap()
}

#[test]
fn multi_invoke_commits_both_databases() {
    let (mut engine, path) = setup("invoke");
    let out = engine
        .invoke_transaction_multi(
            &[put_step(b"dba", "a", s("1")), put_step(b"dbb", "b", s("2"))],
            &options(),
        )
        .unwrap();
    assert_eq!(out, arr(vec![s("1"), s("2")]));
    assert_eq!(read_row(&engine, b"dba", "a"), s("1"));
    assert_eq!(read_row(&engine, b"dbb", "b"), s("2"));
    assert_eq!(revision(&engine, b"dba"), 1);
    assert_eq!(revision(&engine, b"dbb"), 1);
    teardown(engine, path);
}

#[test]
fn multi_invoke_aborts_on_step_failure() {
    let (mut engine, path) = setup("abort");
    let failing = MultiStep {
        db: b"dbb".to_vec(),
        module: "app".to_string(),
        method: "boom".to_string(),
        args: Value::Null,
    };
    let failure = engine
        .invoke_transaction_multi(&[put_step(b"dba", "a", s("1")), failing], &options())
        .unwrap_err();
    assert_eq!(failure.code, "BOOM");
    // The first step's write rolled back with the transaction.
    assert_eq!(read_row(&engine, b"dba", "a"), Value::Null);
    assert_eq!(read_row(&engine, b"dbb", "staged"), Value::Null);
    assert_eq!(revision(&engine, b"dba"), 0);
    assert_eq!(revision(&engine, b"dbb"), 0);
    teardown(engine, path);
}

#[test]
fn multi_invoke_rejects_replicated_participants() {
    let (mut engine, path) = setup("replicated");
    engine.set_replicated(b"dbb");
    let failure = engine
        .invoke_transaction_multi(
            &[put_step(b"dba", "a", s("1")), put_step(b"dbb", "b", s("2"))],
            &options(),
        )
        .unwrap_err();
    assert_eq!(failure.code, "replicated_db");
    assert_eq!(revision(&engine, b"dba"), 0);
    teardown(engine, path);
}

#[test]
fn multi_rejects_bad_steps_and_options() {
    let (mut engine, path) = setup("validate");
    let failure = engine
        .invoke_transaction_multi(&[], &options())
        .unwrap_err();
    assert_eq!(failure.code, "INVALID_VALUE");
    let failure = engine
        .invoke_transaction_multi(
            &[put_step(b"dba", "a", s("1")), put_step(b"dba", "b", s("2"))],
            &options(),
        )
        .unwrap_err();
    assert_eq!(failure.code, "INVALID_VALUE");
    let retry_options = MutationOptions {
        retry: Some(RetryIdentity {
            identity: ScopedIdentity {
                incarnation: 0,
                epoch: 0,
                intent: b"id".to_vec(),
            },
            fingerprint: [0u8; 32],
        }),
        ..MutationOptions::default()
    };
    let failure = engine
        .invoke_transaction_multi(&[put_step(b"dba", "a", s("1"))], &retry_options)
        .unwrap_err();
    assert_eq!(failure.code, "INVALID_VALUE");
    let expected_options = MutationOptions {
        call: CallOptions::default(),
        retry: None,
        idempotency_key: None,
        expected_revision: Some(0),
    };
    let failure = engine
        .invoke_transaction_multi(&[put_step(b"dba", "a", s("1"))], &expected_options)
        .unwrap_err();
    assert_eq!(failure.code, "INVALID_VALUE");
    teardown(engine, path);
}

#[test]
fn step_lists_are_checked_in_linear_time() {
    // A request body holds tens of thousands of steps: checking that no
    // database repeats must not compare every pair.
    let (mut engine, path) = setup("linear");
    let mut steps: Vec<MultiStep> = (0..60_000)
        .map(|index| put_step(format!("db{index:06}").as_bytes(), "a", s("1")))
        .collect();
    steps.push(put_step(b"db000000", "a", s("1")));
    let started = std::time::Instant::now();
    let failure = engine
        .prepare_transaction_multi(&steps, &options())
        .unwrap_err();
    assert_eq!(failure.code, "INVALID_VALUE");
    assert!(
        started.elapsed() < std::time::Duration::from_secs(2),
        "{:?}",
        started.elapsed()
    );
    teardown(engine, path);
}

#[test]
fn multi_prepare_apply_roundtrip() {
    let (mut engine, path) = setup("roundtrip");
    let prepared = engine
        .prepare_transaction_multi(
            &[put_step(b"dba", "a", s("1")), put_step(b"dbb", "b", s("2"))],
            &options(),
        )
        .unwrap();
    let bytes = match prepared {
        PrepareOutcome::Patch(prepared) => {
            assert_eq!(prepared.outcome.unwrap(), arr(vec![s("1"), s("2")]));
            prepared.patch
        }
        _ => panic!("prepare answered without a patch"),
    };
    // Nothing committed before apply.
    assert_eq!(read_row(&engine, b"dba", "a"), Value::Null);
    let outcome = engine.apply_multi_patch(&bytes, 1).unwrap();
    assert_eq!(outcome.results, vec![Ok(s("1")), Ok(s("2"))]);
    assert_eq!(
        outcome.revisions,
        vec![(b"dba".to_vec(), 1), (b"dbb".to_vec(), 1)]
    );
    assert_eq!(read_row(&engine, b"dba", "a"), s("1"));
    assert_eq!(read_row(&engine, b"dbb", "b"), s("2"));
    assert_eq!(applied(&engine, b"dba"), 1);
    assert_eq!(applied(&engine, b"dbb"), 1);
    teardown(engine, path);
}

#[test]
fn multi_apply_rejects_stale_atomically() {
    let (mut engine, path) = setup("stale");
    let prepared = engine
        .prepare_transaction_multi(
            &[put_step(b"dba", "a", s("1")), put_step(b"dbb", "b", s("2"))],
            &options(),
        )
        .unwrap();
    let bytes = match prepared {
        PrepareOutcome::Patch(prepared) => prepared.patch,
        _ => panic!("prepare answered without a patch"),
    };
    // A concurrent single-database write moves dbb first.
    engine
        .invoke_mutation(
            b"dbb",
            "app",
            "put",
            &obj(vec![("key", s("z")), ("value", s("9"))]),
            &options(),
        )
        .unwrap();
    let outcome = engine.apply_multi_patch(&bytes, 1).unwrap();
    assert_eq!(outcome.results.len(), 1);
    assert_eq!(
        outcome.results[0].as_ref().unwrap_err().code,
        "stale_revision"
    );
    assert_eq!(
        outcome.revisions,
        vec![(b"dba".to_vec(), 0), (b"dbb".to_vec(), 1)]
    );
    // Neither participant's journal landed.
    assert_eq!(read_row(&engine, b"dba", "a"), Value::Null);
    assert_eq!(read_row(&engine, b"dbb", "b"), Value::Null);
    assert_eq!(read_row(&engine, b"dbb", "z"), s("9"));
    // Both applied logs advanced past the rejected entry.
    assert_eq!(applied(&engine, b"dba"), 1);
    assert_eq!(applied(&engine, b"dbb"), 1);
    teardown(engine, path);
}

#[test]
fn multi_prepare_aborts_without_patch() {
    let (mut engine, path) = setup("prepare-abort");
    let failing = MultiStep {
        db: b"dbb".to_vec(),
        module: "app".to_string(),
        method: "boom".to_string(),
        args: Value::Null,
    };
    let prepared = engine
        .prepare_transaction_multi(&[put_step(b"dba", "a", s("1")), failing], &options())
        .unwrap();
    match prepared {
        PrepareOutcome::Done(Err(failure)) => assert_eq!(failure.code, "BOOM"),
        _ => panic!("a failing step must abort without a patch"),
    }
    assert_eq!(read_row(&engine, b"dba", "a"), Value::Null);
    teardown(engine, path);
}

#[test]
fn multi_codec_roundtrip_and_rejects_duplicates() {
    let entry = |db: &[u8]| Patch {
        failed: false,
        db: db.to_vec(),
        versions: vec![],
        records: vec![],
        indexes: vec![],
        derived: vec![],
        drops: vec![],
        receipts: vec![],
        result: b"null".to_vec(),
        expected_revision: None,
        base_revision: 0,
    };
    let bytes = encode_multi_patch(&[entry(b"dba"), entry(b"dbb")]).unwrap();
    let decoded = decode_multi_patch(&bytes).unwrap();
    assert_eq!(decoded.len(), 2);
    assert_eq!(decoded[0].db, b"dba");
    let dup = encode_multi_patch(&[entry(b"dba"), entry(b"dba")]).unwrap();
    assert!(decode_multi_patch(&dup).is_err());
    let empty = encode_multi_patch(&[]).unwrap();
    assert!(decode_multi_patch(&empty).is_err());
}
