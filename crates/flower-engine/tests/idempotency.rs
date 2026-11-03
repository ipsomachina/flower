//! Idempotency durability tests: keyed outcomes persist as receipts
//! in the `receipts` table, replay after memory eviction, expire on
//! their TTL horizon and purge without touching scoped receipts.

mod fixtures;

use fixtures::{arr, hc, k, obj, s};

use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use flower_engine::engine::{
    CallOptions, Engine, MultiStep, MutationOptions, PrepareOutcome, RetryIdentity,
};
use flower_engine::retry::{
    Committed, IDEMPOTENCY_TTL_MS, Receipt, ScopedIdentity, UNSCOPED_PREFIX,
};
use flower_engine::runtime::{CallKind, ClosureRuntime, HostCall, Runtime};
use flower_engine::storage::GroupStore;
use flower_engine::storage::meta;
use flower_sdk::abi::HostOp;
use flower_sdk::{Value, fail};

fn manifest() -> Value {
    obj(vec![
        (
            "collections",
            arr(vec![obj(vec![
                ("name", s("log")),
                ("key", obj(vec![("type", s("string"))])),
                ("value", obj(vec![("type", s("json"))])),
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
                    ("args", obj(vec![("type", s("json"))])),
                ]),
                obj(vec![
                    ("name", s("boom")),
                    ("kind", s("mutation")),
                    ("args", obj(vec![("type", s("json"))])),
                ]),
                obj(vec![
                    ("name", s("read")),
                    ("kind", s("query")),
                    ("args", obj(vec![("type", s("json"))])),
                ]),
            ]),
        ),
        ("triggers", arr(vec![])),
        ("http", arr(vec![])),
    ])
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or(0)
}

fn setup(test: &str, runs: Arc<Mutex<u32>>) -> (Engine, std::path::PathBuf) {
    let mut path = std::env::temp_dir();
    path.push(format!("flower-idem-{test}-{}", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let store = GroupStore::create(&path).unwrap();
    let mut engine = Engine::new(store);
    for db in [b"db".as_slice(), b"dbb".as_slice()] {
        let runs = runs.clone();
        let runtime: Box<dyn Runtime> = Box::new(ClosureRuntime::new(
            move |kind: CallKind,
                  _module: &str,
                  name: &str,
                  args: &Value,
                  host: &mut dyn HostCall| {
                *runs.lock().unwrap() += 1;
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
                    (CallKind::Mutation, "boom") => Err(fail("BOOM", "step failure")),
                    (CallKind::Query, "read") => {
                        let Value::String(key) = arg("key") else {
                            return Err(fail("BAD_KEY", "key must be a string"));
                        };
                        hc(host, HostOp::Get, vec![s(""), s("log"), s(&k(&key))])
                    }
                    _ => Err(fail("UNKNOWN_METHOD", name.to_string())),
                }
            },
        ));
        engine
            .deploy(db, "app", [5u8; 32], b"v1", &manifest(), runtime)
            .unwrap();
    }
    (engine, path)
}

fn teardown(engine: Engine, path: std::path::PathBuf) {
    drop(engine);
    let _ = std::fs::remove_file(&path);
}

fn keyed(key: &str) -> MutationOptions {
    MutationOptions {
        call: CallOptions::default(),
        retry: None,
        idempotency_key: Some(key.as_bytes().to_vec()),
        expected_revision: None,
    }
}

fn keyed_at(key: &str, timestamp_ms: u64) -> MutationOptions {
    MutationOptions {
        call: CallOptions {
            timestamp_ms: Some(timestamp_ms),
            ..Default::default()
        },
        retry: None,
        idempotency_key: Some(key.as_bytes().to_vec()),
        expected_revision: None,
    }
}

fn put_args(key: &str, value: Value) -> Value {
    obj(vec![("key", s(key)), ("value", value)])
}

fn revision(engine: &Engine, db: &[u8]) -> u64 {
    let read = engine.store().begin_read().unwrap();
    read.meta_u64(db, meta::REVISION).unwrap()
}

/// Whether the receipts table holds an unscoped row for `key`.
fn has_unscoped(engine: &Engine, db: &[u8], key: &str) -> bool {
    let read = engine.store().begin_read().unwrap();
    let rows = read.scan_receipts(db).unwrap();
    let mut namespaced = UNSCOPED_PREFIX.to_vec();
    namespaced.extend_from_slice(key.as_bytes());
    rows.into_iter().any(|(identity, _)| identity == namespaced)
}

#[test]
fn keyed_mutation_replays_from_disk_after_eviction() {
    let runs = Arc::new(Mutex::new(0u32));
    let (mut engine, path) = setup("evict", runs.clone());
    let out = engine
        .invoke_mutation(b"db", "app", "put", &put_args("a", s("1")), &keyed("k1"))
        .unwrap();
    assert_eq!(out, s("1"));
    assert!(has_unscoped(&engine, b"db", "k1"));
    // Evict k1 from the 1024-entry memory cache: 1025 fresh keys push
    // it out, so redelivery must serve from the durable receipt.
    for index in 0..1025 {
        let key = format!("fill-{index}");
        engine
            .invoke_mutation(b"db", "app", "put", &put_args("f", s("x")), &keyed(&key))
            .unwrap();
    }
    assert_eq!(*runs.lock().unwrap(), 1026);
    let replayed = engine
        .invoke_mutation(b"db", "app", "put", &put_args("a", s("1")), &keyed("k1"))
        .unwrap();
    assert_eq!(replayed, s("1"));
    assert_eq!(*runs.lock().unwrap(), 1026);
    assert_eq!(revision(&engine, b"db"), 1026);
    teardown(engine, path);
}

#[test]
fn expired_key_reexecutes() {
    let runs = Arc::new(Mutex::new(0u32));
    let (mut engine, path) = setup("expiry", runs.clone());
    let old = now_ms().saturating_sub(IDEMPOTENCY_TTL_MS + 3_600_000);
    engine
        .invoke_mutation(
            b"db",
            "app",
            "put",
            &put_args("a", s("1")),
            &keyed_at("k1", old),
        )
        .unwrap();
    assert_eq!(*runs.lock().unwrap(), 1);
    // The receipt expired before it was read: redelivery executes
    // afresh instead of replaying.
    let out = engine
        .invoke_mutation(b"db", "app", "put", &put_args("a", s("2")), &keyed("k1"))
        .unwrap();
    assert_eq!(out, s("2"));
    assert_eq!(*runs.lock().unwrap(), 2);
    assert_eq!(revision(&engine, b"db"), 2);
    teardown(engine, path);
}

#[test]
fn purge_reclaims_expired_unscoped_only() {
    let runs = Arc::new(Mutex::new(0u32));
    let (mut engine, path) = setup("purge", runs.clone());
    let old = now_ms().saturating_sub(IDEMPOTENCY_TTL_MS + 3_600_000);
    engine
        .invoke_mutation(
            b"db",
            "app",
            "put",
            &put_args("a", s("1")),
            &keyed_at("old", old),
        )
        .unwrap();
    engine
        .invoke_mutation(b"db", "app", "put", &put_args("b", s("2")), &keyed("live"))
        .unwrap();
    let scoped = MutationOptions {
        call: CallOptions::default(),
        retry: Some(RetryIdentity {
            identity: ScopedIdentity {
                incarnation: 0,
                epoch: 0,
                intent: b"scope-1".to_vec(),
            },
            fingerprint: [1u8; 32],
        }),
        idempotency_key: None,
        expected_revision: None,
    };
    engine
        .invoke_mutation(b"db", "app", "put", &put_args("c", s("3")), &scoped)
        .unwrap();
    let deleted = engine
        .purge_expired_idempotency(b"db", now_ms(), 100)
        .unwrap();
    assert_eq!(deleted, 1);
    assert!(!has_unscoped(&engine, b"db", "old"));
    assert!(has_unscoped(&engine, b"db", "live"));
    // The purged key re-executes; live and scoped keys replay.
    engine
        .invoke_mutation(b"db", "app", "put", &put_args("a", s("9")), &keyed("old"))
        .unwrap();
    assert_eq!(revision(&engine, b"db"), 4);
    let replayed = engine
        .invoke_mutation(
            b"db",
            "app",
            "put",
            &put_args("b", s("zzz")),
            &keyed("live"),
        )
        .unwrap();
    assert_eq!(replayed, s("2"));
    let saved = engine
        .invoke_mutation(b"db", "app", "put", &put_args("c", s("zzz")), &scoped)
        .unwrap();
    assert_eq!(saved, s("3"));
    assert_eq!(revision(&engine, b"db"), 4);
    teardown(engine, path);
}

#[test]
fn failure_receipt_replays_from_disk_after_eviction() {
    let runs = Arc::new(Mutex::new(0u32));
    let (mut engine, path) = setup("fail", runs.clone());
    let failure = engine
        .invoke_mutation(b"db", "app", "boom", &Value::Null, &keyed("k9"))
        .unwrap_err();
    assert_eq!(failure.code, "BOOM");
    assert!(has_unscoped(&engine, b"db", "k9"));
    for index in 0..1025 {
        let key = format!("fill-{index}");
        engine
            .invoke_mutation(b"db", "app", "put", &put_args("f", s("x")), &keyed(&key))
            .unwrap();
    }
    assert_eq!(*runs.lock().unwrap(), 1026);
    let replayed = engine
        .invoke_mutation(b"db", "app", "boom", &Value::Null, &keyed("k9"))
        .unwrap_err();
    assert_eq!(replayed.code, "BOOM");
    assert_eq!(replayed.message, "step failure");
    assert_eq!(*runs.lock().unwrap(), 1026);
    teardown(engine, path);
}

#[test]
fn duplicate_patch_skips_at_apply() {
    let runs = Arc::new(Mutex::new(0u32));
    let (mut engine, path) = setup("dup-skip", runs.clone());
    // Concurrent duplicates both prepare: nothing is cached before
    // consensus accepts, so the second prepare executes too.
    let first = match engine
        .prepare_mutation(b"db", "app", "put", &put_args("a", s("1")), &keyed("k1"))
        .unwrap()
    {
        PrepareOutcome::Patch(prepared) => prepared.patch,
        _ => panic!("first prepare must propose"),
    };
    let second = match engine
        .prepare_mutation(b"db", "app", "put", &put_args("a", s("2")), &keyed("k1"))
        .unwrap()
    {
        PrepareOutcome::Patch(prepared) => prepared.patch,
        _ => panic!("prepare caches nothing before commit"),
    };
    assert_eq!(*runs.lock().unwrap(), 2);
    let applied = engine.apply_patch(&first, 1).unwrap();
    assert_eq!(applied.result.unwrap(), s("1"));
    assert_eq!(applied.revision, 1);
    assert!(!applied.duplicate);
    // The duplicate applies at the next position but installs
    // nothing: first receipt wins, revision and row untouched.
    let replayed = engine.apply_patch(&second, 2).unwrap();
    assert_eq!(replayed.result.unwrap(), s("1"));
    assert_eq!(replayed.revision, 1);
    // It answers as a duplicate (v1's `duplicate: true`).
    assert!(replayed.duplicate);
    let read = engine.store().begin_read().unwrap();
    assert_eq!(read.meta_u64(b"db", meta::APPLIED_LOG).unwrap(), 2);
    drop(read);
    let row = engine
        .invoke_query(
            b"db",
            "app",
            "read",
            &obj(vec![("key", s("a"))]),
            &CallOptions::default(),
        )
        .unwrap();
    assert_eq!(row, s("1"));
    teardown(engine, path);
}

/// Replays answer where the original committed: the revision it left
/// and its log position, after later writes moved both on, whether the
/// duplicate is caught at apply (both prepared before either applied)
/// or at admission (prepared after). Before receipts recorded them,
/// both answered with the database's current revision instead, and
/// the admission path with no position.
#[test]
fn replays_answer_the_original_revision_and_position_after_later_writes() {
    let runs = Arc::new(Mutex::new(0u32));
    let (mut engine, path) = setup("replay-stamp", runs);
    let prepare = |engine: &mut Engine, key: &str, value: &str| match engine
        .prepare_mutation(b"db", "app", "put", &put_args(key, s(value)), &keyed(key))
        .unwrap()
    {
        PrepareOutcome::Patch(prepared) => prepared.patch,
        _ => panic!("first prepare must propose"),
    };
    let first = prepare(&mut engine, "k1", "1");
    let duplicate = prepare(&mut engine, "k1", "2");
    let original = engine.apply_patch(&first, 5).unwrap();
    assert_eq!(original.revision, 1);
    assert_eq!(original.replayed_position, None);
    // Later writes move the revision and the log on.
    for (position, key) in [(6, "k2"), (7, "k3")] {
        let later = prepare(&mut engine, key, "x");
        engine.apply_patch(&later, position).unwrap();
    }
    assert_eq!(revision(&engine, b"db"), 3);
    // Caught at apply: the original's revision and position.
    let replayed = engine.apply_patch(&duplicate, 9).unwrap();
    assert_eq!(replayed.result.unwrap(), s("1"));
    assert_eq!(replayed.revision, 1);
    assert_eq!(replayed.replayed_position, Some(5));
    // Caught at admission: the same.
    match engine
        .prepare_mutation(b"db", "app", "put", &put_args("k1", s("3")), &keyed("k1"))
        .unwrap()
    {
        PrepareOutcome::Replayed(result, committed) => {
            assert_eq!(result.unwrap(), s("1"));
            assert_eq!(
                committed,
                Some(Committed {
                    revision: 1,
                    position: 5,
                })
            );
        }
        _ => panic!("the key must replay"),
    }
    // The stored receipt carries the stamp: what a snapshot, a synced
    // device or a backup copies.
    let read = engine.store().begin_read().unwrap();
    let mut identity = UNSCOPED_PREFIX.to_vec();
    identity.extend_from_slice(b"k1");
    let stored = read.get_receipt(b"db", &identity).unwrap().unwrap();
    assert_eq!(
        Receipt::decode(&stored).unwrap().committed,
        Some(Committed {
            revision: 1,
            position: 5,
        })
    );
    drop(read);
    teardown(engine, path);
}

#[test]
fn duplicate_scoped_patch_skips_at_apply() {
    let runs = Arc::new(Mutex::new(0u32));
    let (mut engine, path) = setup("dup-scoped", runs.clone());
    let options = MutationOptions {
        call: CallOptions::default(),
        retry: Some(RetryIdentity {
            identity: ScopedIdentity {
                incarnation: 0,
                epoch: 0,
                intent: b"scope-dup".to_vec(),
            },
            fingerprint: [7u8; 32],
        }),
        idempotency_key: None,
        expected_revision: None,
    };
    let first = match engine
        .prepare_mutation(b"db", "app", "put", &put_args("a", s("1")), &options)
        .unwrap()
    {
        PrepareOutcome::Patch(prepared) => prepared.patch,
        _ => panic!("first prepare must propose"),
    };
    let second = match engine
        .prepare_mutation(b"db", "app", "put", &put_args("a", s("2")), &options)
        .unwrap()
    {
        PrepareOutcome::Patch(prepared) => prepared.patch,
        _ => panic!("concurrent admit executes twice"),
    };
    engine.apply_patch(&first, 1).unwrap();
    let replayed = engine.apply_patch(&second, 2).unwrap();
    assert_eq!(replayed.result.unwrap(), s("1"));
    assert_eq!(replayed.revision, 1);
    assert_eq!(revision(&engine, b"db"), 1);
    teardown(engine, path);
}

/// A scoped identity reused for other content is a conflict wherever it
/// is caught: two requests under one identity with different
/// fingerprints, both prepared before either applied, leave the second
/// refused (`identity_conflict`), not answered with the first one's
/// outcome as a duplicate. (Before, apply compared fingerprints only
/// for a caller's own keys.)
#[test]
fn a_scoped_identity_reused_for_other_content_conflicts_at_apply() {
    let runs = Arc::new(Mutex::new(0u32));
    let (mut engine, path) = setup("scoped-conflict", runs.clone());
    let options = |fingerprint: u8| MutationOptions {
        call: CallOptions::default(),
        retry: Some(RetryIdentity {
            identity: ScopedIdentity {
                incarnation: 0,
                epoch: 0,
                intent: b"scope-reused".to_vec(),
            },
            fingerprint: [fingerprint; 32],
        }),
        idempotency_key: None,
        expected_revision: None,
    };
    let prepare = |engine: &mut Engine, value: &str, fingerprint: u8| match engine
        .prepare_mutation(
            b"db",
            "app",
            "put",
            &put_args("a", s(value)),
            &options(fingerprint),
        )
        .unwrap()
    {
        PrepareOutcome::Patch(prepared) => prepared.patch,
        _ => panic!("both prepare before either applies"),
    };
    let first = prepare(&mut engine, "1", 7);
    let second = prepare(&mut engine, "2", 8);
    let applied = engine.apply_patch(&first, 1).unwrap();
    assert!(!applied.duplicate);
    let refused = engine.apply_patch(&second, 2).unwrap();
    assert_eq!(refused.result.unwrap_err().code, "identity_conflict");
    assert!(!refused.duplicate);
    assert_eq!(refused.replayed_position, None);
    // Admission says the same of a third copy.
    let third = engine
        .prepare_mutation(b"db", "app", "put", &put_args("a", s("3")), &options(9))
        .unwrap();
    assert!(
        matches!(&third, PrepareOutcome::Done(Err(failure)) if failure.code == "identity_conflict"),
        "{third:?}"
    );
    assert_eq!(revision(&engine, b"db"), 1);
    teardown(engine, path);
}

#[test]
fn expired_receipt_applies_fresh_at_stamp() {
    let runs = Arc::new(Mutex::new(0u32));
    let (mut engine, path) = setup("dup-expiry", runs.clone());
    let stamp = now_ms();
    let bytes = match engine
        .prepare_mutation(
            b"db",
            "app",
            "put",
            &put_args("a", s("1")),
            &keyed_at("k1", stamp),
        )
        .unwrap()
    {
        PrepareOutcome::Patch(prepared) => prepared.patch,
        _ => panic!("prepare must propose"),
    };
    engine.apply_patch_at(&bytes, 1, stamp).unwrap();
    assert_eq!(revision(&engine, b"db"), 1);
    // Past the TTL horizon the stored row no longer deduplicates:
    // the same bytes apply as fresh work.
    let later = stamp + IDEMPOTENCY_TTL_MS + 1;
    let reapplied = engine.apply_patch_at(&bytes, 2, later).unwrap();
    assert_eq!(reapplied.result.unwrap(), s("1"));
    assert_eq!(reapplied.revision, 2);
    teardown(engine, path);
}

#[test]
fn multi_transaction_replays_from_disk_after_eviction() {
    let runs = Arc::new(Mutex::new(0u32));
    let (mut engine, path) = setup("multi", runs.clone());
    let steps = vec![
        MultiStep {
            db: b"db".to_vec(),
            module: "app".to_string(),
            method: "put".to_string(),
            args: put_args("a", s("1")),
        },
        MultiStep {
            db: b"dbb".to_vec(),
            module: "app".to_string(),
            method: "put".to_string(),
            args: put_args("b", s("2")),
        },
    ];
    let out = engine
        .invoke_transaction_multi(&steps, &keyed("txn-1"))
        .unwrap();
    assert_eq!(out, arr(vec![s("1"), s("2")]));
    // The batch receipt lives under the first step's database only.
    assert!(has_unscoped(&engine, b"db", "txn-1"));
    assert!(!has_unscoped(&engine, b"dbb", "txn-1"));
    for index in 0..1025 {
        let key = format!("fill-{index}");
        engine
            .invoke_mutation(b"db", "app", "put", &put_args("f", s("x")), &keyed(&key))
            .unwrap();
    }
    let replayed = engine
        .invoke_transaction_multi(&steps, &keyed("txn-1"))
        .unwrap();
    assert_eq!(replayed, arr(vec![s("1"), s("2")]));
    assert_eq!(revision(&engine, b"db"), 1026);
    assert_eq!(revision(&engine, b"dbb"), 1);
    teardown(engine, path);
}

#[test]
fn duplicate_multi_skips_whole_batch() {
    let runs = Arc::new(Mutex::new(0u32));
    let (mut engine, path) = setup("dup-multi", runs.clone());
    let step = |db: &[u8], key: &str, value: Value| MultiStep {
        db: db.to_vec(),
        module: "app".to_string(),
        method: "put".to_string(),
        args: put_args(key, value),
    };
    let first_steps = vec![step(b"db", "a", s("1")), step(b"dbb", "b", s("2"))];
    let second_steps = vec![step(b"db", "a", s("9")), step(b"dbb", "b", s("8"))];
    let first = match engine
        .prepare_transaction_multi(&first_steps, &keyed("txn-dup"))
        .unwrap()
    {
        PrepareOutcome::Patch(prepared) => prepared.patch,
        _ => panic!("first prepare must propose"),
    };
    let second = match engine
        .prepare_transaction_multi(&second_steps, &keyed("txn-dup"))
        .unwrap()
    {
        PrepareOutcome::Patch(prepared) => prepared.patch,
        _ => panic!("prepare caches nothing before commit"),
    };
    let applied = engine.apply_multi_patch(&first, 1).unwrap();
    assert_eq!(applied.combined().unwrap(), arr(vec![s("1"), s("2")]));
    assert!(!applied.duplicate);
    // The duplicate batch skips every journal and replays the
    // recorded step array; neither revision moves.
    let replayed = engine.apply_multi_patch(&second, 2).unwrap();
    assert_eq!(replayed.results, vec![Ok(s("1")), Ok(s("2"))]);
    assert!(replayed.duplicate);
    assert_eq!(replayed.combined().unwrap(), arr(vec![s("1"), s("2")]));
    assert_eq!(
        replayed.revisions,
        vec![(b"db".to_vec(), 1), (b"dbb".to_vec(), 1)]
    );
    let row = engine
        .invoke_query(
            b"db",
            "app",
            "read",
            &obj(vec![("key", s("a"))]),
            &CallOptions::default(),
        )
        .unwrap();
    assert_eq!(row, s("1"));
    teardown(engine, path);
}
