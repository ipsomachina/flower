//! Work-queue integration tests: guest enqueues staged into
//! mutations plus the native claim/lease protocol, driven through
//! [`Engine`] with in-process closure runtimes.

use flower_engine::engine::{CallOptions, Engine, MutationOptions};
use flower_engine::runtime::{CallKind, ClosureRuntime, HostCall, Runtime};
use flower_engine::storage::GroupStore;
use flower_engine::work_queue::ItemState;
use flower_sdk::abi::{HostOp, decode_envelope};
use flower_sdk::{Failure, Value, fail};

fn s(text: &str) -> Value {
    Value::String(text.to_string())
}

fn n(num: f64) -> Value {
    Value::Number(num)
}

fn arr(items: Vec<Value>) -> Value {
    Value::Array(items)
}

fn obj(pairs: Vec<(&str, Value)>) -> Value {
    Value::Object(
        pairs
            .into_iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect(),
    )
}

fn definition(name: &str, kind: &str) -> Value {
    obj(vec![
        ("name", s(name)),
        ("kind", s(kind)),
        ("args", obj(vec![("type", s("json"))])),
    ])
}

/// A module's manifest, declaring the queues its guests use.
fn manifest(definitions: Vec<Value>) -> Value {
    obj(vec![
        ("collections", arr(vec![])),
        ("definitions", arr(definitions)),
        ("triggers", arr(vec![])),
        ("http", arr(vec![])),
        (
            "queues",
            arr(vec![
                obj(vec![("name", s("email"))]),
                obj(vec![("name", s("jobs"))]),
            ]),
        ),
    ])
}

fn setup(test: &str, manifest: Value, runtime: Box<dyn Runtime>) -> (Engine, std::path::PathBuf) {
    let mut path = std::env::temp_dir();
    path.push(format!("flower-queue-{test}-{}", std::process::id()));
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

/// One host call through a closure's handler, decoding the envelope.
fn hc(host: &mut dyn HostCall, op: HostOp, args: Vec<Value>) -> Result<Value, Failure> {
    let input = Value::Array(args).encode_binary().unwrap();
    let output = host.op(op as u32 as u8, &input);
    let envelope = Value::decode_binary(&output).unwrap();
    decode_envelope(envelope)
}

fn enqueue(
    host: &mut dyn HostCall,
    queue: &str,
    payload: Value,
    delay_ms: f64,
    max_attempts: f64,
) -> Result<String, Failure> {
    match hc(
        host,
        HostOp::Enqueue,
        vec![s(queue), payload, n(delay_ms), n(max_attempts)],
    )? {
        Value::String(id) => Ok(id),
        _ => panic!("enqueue returns an item ID"),
    }
}

fn send_runtime() -> Box<dyn Runtime> {
    Box::new(ClosureRuntime::new(
        |kind: CallKind, _module: &str, name: &str, args: &Value, host: &mut dyn HostCall| match (
            kind, name,
        ) {
            (CallKind::Mutation, "send") => {
                let id = enqueue(host, "email", args.clone(), 0.0, 5.0)?;
                Ok(s(&id))
            }
            _ => Err(fail("UNKNOWN_METHOD", name.to_string())),
        },
    ))
}

#[test]
fn guest_enqueue_staged_pollable() {
    let manifest = manifest(vec![definition("send", "mutation")]);
    let (mut engine, path) = setup("staged", manifest, send_runtime());
    let payload = obj(vec![("to", s("ada@example.com"))]);
    let out = engine
        .invoke_mutation(b"db", "app", "send", &payload, &MutationOptions::default())
        .unwrap();
    let id = match out {
        Value::String(id) => id,
        _ => panic!("mutation returns the item ID"),
    };
    assert_eq!(id.len(), 32);
    // The staged enqueue committed with the mutation: poll it back.
    let claimed = engine
        .poll_queue(b"db", "email", u64::MAX, 10, 60_000)
        .unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].id, id);
    assert_eq!(claimed[0].payload, payload);
    assert_eq!(claimed[0].attempts, 1);
    let stats = engine.queue_stats(b"db", "email").unwrap();
    assert_eq!((stats.pending, stats.claimed, stats.dead), (0, 1, 0));
    teardown(engine, path);
}

#[test]
fn enqueue_rolls_back_with_mutation() {
    let manifest = manifest(vec![definition("send", "mutation")]);
    let runtime = ClosureRuntime::new(
        |kind: CallKind, _module: &str, name: &str, args: &Value, host: &mut dyn HostCall| match (
            kind, name,
        ) {
            (CallKind::Mutation, "send") => {
                enqueue(host, "email", args.clone(), 0.0, 5.0)?;
                Err(fail("NOPE", "rollback"))
            }
            _ => Err(fail("UNKNOWN_METHOD", name.to_string())),
        },
    );
    let (mut engine, path) = setup("rollback", manifest, Box::new(runtime));
    let failure = engine
        .invoke_mutation(b"db", "app", "send", &s("x"), &MutationOptions::default())
        .unwrap_err();
    assert_eq!(failure.code, "NOPE");
    let stats = engine.queue_stats(b"db", "email").unwrap();
    assert_eq!((stats.pending, stats.claimed, stats.dead), (0, 0, 0));
    teardown(engine, path);
}

#[test]
fn enqueue_validation_rejects() {
    let manifest = manifest(vec![definition("send", "mutation")]);
    let cases: Vec<(String, f64, f64)> = vec![
        ("".to_string(), 0.0, 5.0),
        ("has space".to_string(), 0.0, 5.0),
        ("q".repeat(129), 0.0, 5.0),
        (
            "email".to_string(),
            30.0 * 24.0 * 60.0 * 60.0 * 1000.0 + 1.0,
            5.0,
        ),
        ("email".to_string(), 0.0, 0.0),
        ("email".to_string(), 0.0, 101.0),
        ("email".to_string(), 1.5, 5.0),
    ];
    for (queue, delay, max) in cases {
        let runtime = ClosureRuntime::new(
            move |kind: CallKind,
                  _module: &str,
                  name: &str,
                  _args: &Value,
                  host: &mut dyn HostCall| {
                match (kind, name) {
                    (CallKind::Mutation, "send") => {
                        enqueue(host, &queue, Value::Null, delay, max)?;
                        Ok(Value::Null)
                    }
                    _ => Err(fail("UNKNOWN_METHOD", name.to_string())),
                }
            },
        );
        let (mut engine, path) = setup("validate", manifest.clone(), Box::new(runtime));
        let failure = engine
            .invoke_mutation(
                b"db",
                "app",
                "send",
                &Value::Null,
                &MutationOptions::default(),
            )
            .unwrap_err();
        assert!(
            failure.code == "INVALID_VALUE" || failure.code == "TYPE_MISMATCH",
            "unexpected code {}",
            failure.code
        );
        teardown(engine, path);
    }
}

#[test]
fn enqueue_rejected_in_query() {
    let manifest = manifest(vec![definition("read", "query")]);
    let runtime = ClosureRuntime::new(
        |kind: CallKind, _module: &str, name: &str, _args: &Value, host: &mut dyn HostCall| match (
            kind, name,
        ) {
            (CallKind::Query, "read") => {
                enqueue(host, "email", Value::Null, 0.0, 5.0)?;
                Ok(Value::Null)
            }
            _ => Err(fail("UNKNOWN_METHOD", name.to_string())),
        },
    );
    let (engine, path) = setup("query", manifest, Box::new(runtime));
    let failure = engine
        .invoke_query(
            b"db",
            "app",
            "read",
            &Value::Null,
            &flower_engine::engine::CallOptions::default(),
        )
        .unwrap_err();
    assert_eq!(failure.code, "INVALID_INVOCATION");
    teardown(engine, path);
}

#[test]
fn poll_claim_lifecycle() {
    let manifest = manifest(vec![definition("send", "mutation")]);
    let (engine, path) = setup("lifecycle", manifest, send_runtime());
    let now = 1_000_000u64;
    let first = engine.enqueue(b"db", "jobs", &s("a"), 0, 3, now).unwrap();
    let delayed = engine
        .enqueue(b"db", "jobs", &s("b"), 60_000, 3, now)
        .unwrap();
    assert_ne!(first, delayed);
    // Only the due item claims.
    let claimed = engine.poll_queue(b"db", "jobs", now, 10, 5_000).unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].id, first);
    assert_eq!(claimed[0].token.len(), 32);
    // A wrong token completes nothing.
    let bad = engine.complete_queue(b"db", "jobs", &first, "0".repeat(32).as_str(), now);
    assert_eq!(bad.unwrap_err().code, "INVALID_VALUE");
    // Heartbeats extend the lease; wrong tokens fail.
    assert!(
        engine
            .heartbeat_queue(
                b"db",
                "jobs",
                &first,
                "1".repeat(32).as_str(),
                5_000,
                now + 1_000
            )
            .is_err()
    );
    engine
        .heartbeat_queue(b"db", "jobs", &first, &claimed[0].token, 5_000, now + 1_000)
        .unwrap();
    // Still claimed under the extended lease: nothing due.
    assert!(
        engine
            .poll_queue(b"db", "jobs", now + 2_000, 10, 5_000)
            .unwrap()
            .is_empty()
    );
    engine
        .complete_queue(b"db", "jobs", &first, &claimed[0].token, now + 2_000)
        .unwrap();
    // Completing twice fails: the item is gone.
    assert!(
        engine
            .complete_queue(b"db", "jobs", &first, &claimed[0].token, now + 2_000)
            .is_err()
    );
    // The delayed item claims once due.
    assert!(
        engine
            .poll_queue(b"db", "jobs", now + 59_999, 10, 5_000)
            .unwrap()
            .is_empty()
    );
    let due = engine
        .poll_queue(b"db", "jobs", now + 60_000, 10, 5_000)
        .unwrap();
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].id, delayed);
    let stats = engine.queue_stats(b"db", "jobs").unwrap();
    assert_eq!((stats.pending, stats.claimed, stats.dead), (0, 1, 0));
    teardown(engine, path);
}

#[test]
fn lease_expiry_redelivers() {
    let manifest = manifest(vec![definition("send", "mutation")]);
    let (engine, path) = setup("expiry", manifest, send_runtime());
    let now = 2_000_000u64;
    let id = engine.enqueue(b"db", "jobs", &s("a"), 0, 3, now).unwrap();
    let first = engine.poll_queue(b"db", "jobs", now, 10, 5_000).unwrap();
    assert_eq!(first[0].attempts, 1);
    // A lapsed heartbeat fails; the claim expires into redelivery.
    assert!(
        engine
            .heartbeat_queue(b"db", "jobs", &id, &first[0].token, 5_000, now + 5_000)
            .is_err()
    );
    let second = engine
        .poll_queue(b"db", "jobs", now + 5_000, 10, 5_000)
        .unwrap();
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].id, id);
    assert_eq!(second[0].attempts, 2);
    assert_ne!(second[0].token, first[0].token);
    teardown(engine, path);
}

#[test]
fn fail_requeues_then_dies() {
    let manifest = manifest(vec![definition("send", "mutation")]);
    let (engine, path) = setup("fail", manifest, send_runtime());
    let now = 3_000_000u64;
    let id = engine.enqueue(b"db", "jobs", &s("a"), 0, 2, now).unwrap();
    let first = engine.poll_queue(b"db", "jobs", now, 10, 5_000).unwrap();
    // Wrong tokens fail nothing.
    assert!(
        engine
            .fail_queue(b"db", "jobs", &id, "2".repeat(32).as_str(), now, 0)
            .is_err()
    );
    // First failure requeues after a backoff.
    engine
        .fail_queue(b"db", "jobs", &id, &first[0].token, now, 10_000)
        .unwrap();
    assert!(
        engine
            .poll_queue(b"db", "jobs", now + 9_999, 10, 5_000)
            .unwrap()
            .is_empty()
    );
    let second = engine
        .poll_queue(b"db", "jobs", now + 10_000, 10, 5_000)
        .unwrap();
    assert_eq!(second[0].attempts, 2);
    // Attempts exhausted: the item dies and never requeues.
    engine
        .fail_queue(b"db", "jobs", &id, &second[0].token, now + 10_000, 0)
        .unwrap();
    assert!(
        engine
            .poll_queue(b"db", "jobs", now + 10_000, 10, 5_000)
            .unwrap()
            .is_empty()
    );
    let item = engine.get_queue_item(b"db", "jobs", &id).unwrap().unwrap();
    assert_eq!(item.state, ItemState::Dead);
    assert_eq!(item.attempts, 2);
    assert_eq!(item.payload, s("a"));
    let stats = engine.queue_stats(b"db", "jobs").unwrap();
    assert_eq!((stats.pending, stats.claimed, stats.dead), (0, 0, 1));
    teardown(engine, path);
}

#[test]
fn cancel_release_retry() {
    let manifest = manifest(vec![definition("send", "mutation")]);
    let (engine, path) = setup("cancel", manifest, send_runtime());
    let now = 5_000_000u64;
    let cancelled = engine.enqueue(b"db", "jobs", &s("a"), 0, 1, now).unwrap();
    let released = engine.enqueue(b"db", "jobs", &s("b"), 0, 1, now).unwrap();
    // Two attempts: a lease lapsing on the last one kills the item
    // instead of redelivering it (v1's rule).
    let dead = engine.enqueue(b"db", "jobs", &s("c"), 0, 2, now).unwrap();
    // Cancel removes a pending item; twice reports missing.
    assert!(engine.cancel_queue(b"db", "jobs", &cancelled).unwrap());
    assert!(!engine.cancel_queue(b"db", "jobs", &cancelled).unwrap());
    assert!(
        engine
            .get_queue_item(b"db", "jobs", &cancelled)
            .unwrap()
            .is_none()
    );
    // Release returns a claim to pending without an attempt.
    let claimed = engine.poll_queue(b"db", "jobs", now, 10, 5_000).unwrap();
    assert_eq!(claimed.len(), 2);
    let held = claimed.iter().find(|item| item.id == released).unwrap();
    engine
        .release_queue(b"db", "jobs", &released, &held.token, now, 10_000)
        .unwrap();
    // The released item stays back while its delay runs; the other
    // item's lapsed lease redelivers it instead.
    let waiting = engine
        .poll_queue(b"db", "jobs", now + 9_999, 10, 5_000)
        .unwrap();
    assert!(waiting.iter().all(|item| item.id != released));
    let reclaimed = waiting.iter().find(|item| item.id == dead).unwrap().clone();
    let again = engine
        .poll_queue(b"db", "jobs", now + 10_000, 10, 5_000)
        .unwrap();
    let again = again.iter().find(|item| item.id == released).unwrap();
    assert_eq!(again.attempts, 2);
    // Retry revives a dead item with a fresh budget; live items fail.
    engine
        .fail_queue(b"db", "jobs", &dead, &reclaimed.token, now + 9_999, 0)
        .unwrap();
    assert!(
        engine
            .retry_queue(b"db", "jobs", &released, now, 0)
            .is_err()
    );
    engine.retry_queue(b"db", "jobs", &dead, now, 0).unwrap();
    let item = engine
        .get_queue_item(b"db", "jobs", &dead)
        .unwrap()
        .unwrap();
    assert_eq!(item.state, ItemState::Pending);
    assert_eq!(item.attempts, 0);
    teardown(engine, path);
}

#[test]
fn kill_dies_immediately() {
    let manifest = manifest(vec![definition("send", "mutation")]);
    let (engine, path) = setup("kill", manifest, send_runtime());
    let now = 6_000_000u64;
    let id = engine.enqueue(b"db", "jobs", &s("a"), 0, 5, now).unwrap();
    let claimed = engine.poll_queue(b"db", "jobs", now, 10, 5_000).unwrap();
    assert!(
        engine
            .kill_queue(b"db", "jobs", &id, "3".repeat(32).as_str(), now)
            .is_err()
    );
    engine
        .kill_queue(b"db", "jobs", &id, &claimed[0].token, now)
        .unwrap();
    assert!(
        engine
            .poll_queue(b"db", "jobs", now, 10, 5_000)
            .unwrap()
            .is_empty()
    );
    let item = engine.get_queue_item(b"db", "jobs", &id).unwrap().unwrap();
    assert_eq!(item.state, ItemState::Dead);
    assert_eq!(item.attempts, 1);
    teardown(engine, path);
}

#[test]
fn purge_clears_old_dead() {
    let manifest = manifest(vec![definition("send", "mutation")]);
    let (engine, path) = setup("purge", manifest, send_runtime());
    let now = 4_000_000u64;
    let old = engine.enqueue(b"db", "jobs", &s("a"), 0, 1, now).unwrap();
    let fresh = engine
        .enqueue(b"db", "jobs", &s("b"), 0, 1, now + 100_000)
        .unwrap();
    let claimed = engine
        .poll_queue(b"db", "jobs", now + 100_000, 10, 5_000)
        .unwrap();
    assert_eq!(claimed.len(), 2);
    for item in &claimed {
        engine
            .fail_queue(b"db", "jobs", &item.id, &item.token, now + 100_000, 0)
            .unwrap();
    }
    // Only the old dead item purges.
    let removed = engine.purge_queue(b"db", "jobs", now + 100_000).unwrap();
    assert_eq!(removed, 1);
    assert!(
        engine
            .get_queue_item(b"db", "jobs", &old)
            .unwrap()
            .is_none()
    );
    assert!(
        engine
            .get_queue_item(b"db", "jobs", &fresh)
            .unwrap()
            .is_some()
    );
    assert_eq!(engine.purge_queue(b"db", "jobs", now + 100_000).unwrap(), 0);
    teardown(engine, path);
}

/// Mutation options at a pinned clock.
fn at(timestamp_ms: u64) -> MutationOptions {
    MutationOptions {
        call: CallOptions {
            timestamp_ms: Some(timestamp_ms),
            ..CallOptions::default()
        },
        ..MutationOptions::default()
    }
}

/// An app whose `flood` mutation enqueues `count` jobs into `jobs`, each
/// with a single attempt.
fn flood_runtime() -> Box<dyn Runtime> {
    Box::new(ClosureRuntime::new(
        |kind: CallKind, _module: &str, name: &str, args: &Value, host: &mut dyn HostCall| match (
            kind, name,
        ) {
            (CallKind::Mutation, "flood") => {
                let Value::Number(count) = args else {
                    return Err(fail("TYPE_MISMATCH", "flood takes a count"));
                };
                for index in 0..*count as u64 {
                    enqueue(host, "jobs", n(index as f64), 0.0, 1.0)?;
                }
                Ok(Value::Null)
            }
            _ => Err(fail("UNKNOWN_METHOD", name.to_string())),
        },
    ))
}

/// Run `body` on a thread of its own, failing rather than hanging the
/// run when it has not returned within `secs` seconds.
fn returns_within<T: Send + 'static>(secs: u64, body: impl FnOnce() -> T + Send + 'static) -> T {
    let (done, answer) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = done.send(body());
    });
    match answer.recv_timeout(std::time::Duration::from_secs(secs)) {
        Ok(value) => value,
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            panic!("still running after {secs} s: it never returns")
        }
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => panic!("it panicked"),
    }
}

#[test]
fn more_than_a_page_of_lapsed_last_attempts_on_one_expiry_never_stall() {
    let manifest = manifest(vec![definition("flood", "mutation")]);
    let (mut engine, path) = setup("lapsed-page", manifest, flood_runtime());
    let now = 7_000_000u64;
    // More than a page (1,024) of claims on their only attempt, all
    // taken at one instant under one lease, so they share one expiry
    // millisecond, and enough that one claim's kills (256) leave a page.
    let count = 1_400;
    engine
        .invoke_mutation(b"db", "app", "flood", &n(count as f64), &at(now))
        .unwrap();
    let mut claimed = 0;
    while claimed < count {
        let batch = engine.poll_queue(b"db", "jobs", now, 100, 5_000).unwrap();
        assert!(!batch.is_empty());
        claimed += batch.len();
    }
    // An owner waits in line meanwhile.
    let wait = |owner| flower_engine::work_queue::ClaimRequest {
        scope: "",
        owner: Some(owner),
        limit: 1,
        lease_ms: 5_000,
        wait_ms: Some(600_000),
    };
    assert!(
        engine
            .claim_queue(b"db", "jobs", &wait("first"), now)
            .unwrap()
            .is_empty()
    );
    let later = now + 10_000;
    let engine = returns_within(60, move || {
        // Readiness (what stats answer with) looks past every lapse on a
        // last attempt for one that redelivers: there is none.
        let readiness = engine
            .queue_readiness(b"db", "jobs", "", Some("other"), later)
            .unwrap();
        assert!(!readiness.ready);
        assert_eq!(readiness.next_ms, None);
        let stats = engine.queue_stats_at(b"db", "jobs", None, later).unwrap();
        assert_eq!((stats.claimed, stats.dead), (0, count));
        // Another owner's claim asks when work became claimable before
        // passing the first in line, then kills what lapsed.
        assert!(
            engine
                .claim_queue(b"db", "jobs", &wait("other"), later)
                .unwrap()
                .is_empty()
        );
        engine
    });
    teardown(engine, path);
}

#[test]
fn report_and_retry_delays_are_bounded_on_the_native_path() {
    use flower_engine::work_queue::MAX_DELAY_MS;
    let manifest = manifest(vec![definition("send", "mutation")]);
    let (engine, path) = setup("delays", manifest, send_runtime());
    let now = 8_000_000u64;
    let id = engine.enqueue(b"db", "jobs", &s("a"), 0, 2, now).unwrap();
    let claim = engine.poll_queue(b"db", "jobs", now, 10, 5_000).unwrap();
    let token = &claim[0].token;
    // Past 30 days, fail and release refuse and change nothing.
    for refused in [
        engine.fail_queue(b"db", "jobs", &id, token, now, MAX_DELAY_MS + 1),
        engine.release_queue(b"db", "jobs", &id, token, now, MAX_DELAY_MS + 1),
    ] {
        let failure = refused.unwrap_err();
        assert_eq!(failure.code, "INVALID_VALUE");
        assert!(failure.message.contains("delay is too long"), "{failure:?}");
    }
    // A delay that would round through a row's numbers leaves no item
    // its cancel cannot find: refused, so the cancel takes the claim.
    let huge = engine.fail_queue(b"db", "jobs", &id, token, now, 1 << 60);
    assert!(engine.cancel_queue(b"db", "jobs", &id).unwrap());
    let stats = engine.queue_stats_at(b"db", "jobs", None, now).unwrap();
    assert_eq!(
        (stats.pending, stats.claimed, stats.dead, stats.completed),
        (0, 0, 0, 0)
    );
    assert_eq!(huge.unwrap_err().code, "INVALID_VALUE");
    // The longest delay is fine, and a cancel then finds the item too.
    let id = engine.enqueue(b"db", "jobs", &s("b"), 0, 1, now).unwrap();
    let claim = engine.poll_queue(b"db", "jobs", now, 10, 5_000).unwrap();
    engine
        .release_queue(b"db", "jobs", &id, &claim[0].token, now, MAX_DELAY_MS)
        .unwrap();
    let item = engine.get_queue_item(b"db", "jobs", &id).unwrap().unwrap();
    assert_eq!(item.due_ms, now + MAX_DELAY_MS);
    // Retries are bounded alike.
    let claim = engine
        .poll_queue(b"db", "jobs", now + MAX_DELAY_MS, 10, 5_000)
        .unwrap();
    engine
        .fail_queue(b"db", "jobs", &id, &claim[0].token, now + MAX_DELAY_MS, 0)
        .unwrap();
    let refused = engine
        .retry_queue(b"db", "jobs", &id, now, MAX_DELAY_MS + 1)
        .unwrap_err();
    assert_eq!(refused.code, "INVALID_VALUE");
    engine
        .retry_queue(b"db", "jobs", &id, now, MAX_DELAY_MS)
        .unwrap();
    assert!(engine.cancel_queue(b"db", "jobs", &id).unwrap());
    let stats = engine.queue_stats_at(b"db", "jobs", None, now).unwrap();
    assert_eq!((stats.pending, stats.dead), (0, 0));
    teardown(engine, path);
}

#[test]
fn a_lapse_on_the_last_attempt_is_dead_before_any_claim_moves_it() {
    use flower_engine::work_queue::{JobRef, NewItem};
    let manifest = manifest(vec![definition("send", "mutation")]);
    let (engine, path) = setup("lapse-dead", manifest, send_runtime());
    let now = 9_000_000u64;
    let payload = s("nightly");
    let named = |replace| NewItem {
        name: Some("nightly"),
        replace,
        ..NewItem::new("jobs", &payload, 0, 1)
    };
    let id = engine.enqueue_item(b"db", &named(false), now).unwrap();
    let claim = engine.poll_queue(b"db", "jobs", now, 10, 5_000).unwrap();
    let stats = engine.queue_stats_at(b"db", "jobs", None, now + 1).unwrap();
    assert_eq!((stats.claimed, stats.dead), (1, 0));
    // The lease lapses on the only attempt and nothing claims since:
    // the job is dead all the same, as v1's `effective` reads it.
    let lapsed = now + 5_000;
    let stats = engine.queue_stats_at(b"db", "jobs", None, lapsed).unwrap();
    assert_eq!((stats.pending, stats.claimed, stats.dead), (0, 0, 1));
    let job = JobRef::Named {
        scope: "",
        name: "nightly",
    };
    let item = engine
        .find_queue_item_at(b"db", "jobs", &job, lapsed)
        .unwrap()
        .unwrap();
    assert_eq!(item.state, ItemState::Dead);
    assert_eq!(item.token, None);
    // Its holder can no longer report; a retry revives it.
    let late = engine
        .complete_queue(b"db", "jobs", &id, &claim[0].token, lapsed)
        .unwrap_err();
    assert!(late.message.contains("lapsed"), "{late:?}");
    engine.retry_queue(b"db", "jobs", &id, lapsed, 0).unwrap();
    let again = engine.poll_queue(b"db", "jobs", lapsed, 10, 5_000).unwrap();
    assert_eq!(again.len(), 1);
    assert_eq!(again[0].attempts, 1);
    // Lapsed on its last attempt again: a replace takes it, a plain
    // enqueue of the name still finds it taken.
    let later = lapsed + 5_000;
    let taken = engine
        .enqueue_item(b"db", &named(false), later)
        .unwrap_err();
    assert_eq!(taken.code, "JOB_EXISTS");
    assert_eq!(engine.enqueue_item(b"db", &named(true), later).unwrap(), id);
    let item = engine
        .find_queue_item_at(b"db", "jobs", &job, later)
        .unwrap()
        .unwrap();
    assert_eq!((item.state, item.attempts), (ItemState::Pending, 0));
    let stats = engine.queue_stats_at(b"db", "jobs", None, later).unwrap();
    assert_eq!((stats.pending, stats.claimed, stats.dead), (1, 0, 0));
    teardown(engine, path);
}

/// Apply placement commands to `db` in `store`, each answered.
fn place(store: &GroupStore, db: &[u8], commands: Vec<flower_engine::placement::PlacementCommand>) {
    for (position, command) in commands.into_iter().enumerate() {
        let write = store.begin_write().unwrap();
        let (outcome, _) =
            flower_engine::placement::apply(&write, db, &command, position as u64 + 1).unwrap();
        write.commit().unwrap();
        outcome.unwrap();
    }
}

/// Set a database's history incarnation, as establishing one, a move's
/// activation or a restore does.
fn set_incarnation(store: &GroupStore, db: &[u8], incarnation: u64) {
    let write = store.begin_write().unwrap();
    write
        .set_meta_u64(
            db,
            flower_engine::storage::meta::HISTORY_INCARNATION,
            incarnation,
        )
        .unwrap();
    write.commit().unwrap();
}

#[test]
fn claims_taken_before_a_move_stay_good_until_a_restore() {
    use flower_engine::placement::{PlacementCommand, Stage};
    let manifest = manifest(vec![definition("send", "mutation")]);
    let (source, source_path) = setup("move-source", manifest, send_runtime());
    let now = 10_000_000u64;
    let lease = 600_000;
    // A claim from before a restore of the source group stays fenced.
    set_incarnation(source.store(), b"db", 7);
    let old = source.enqueue(b"db", "jobs", &s("old"), 0, 5, now).unwrap();
    let old_claim = source.poll_queue(b"db", "jobs", now, 1, lease).unwrap();
    set_incarnation(source.store(), b"db", 10_253_275_921_945_582_101);
    let first = source.enqueue(b"db", "jobs", &s("a"), 0, 5, now).unwrap();
    let second = source.enqueue(b"db", "jobs", &s("b"), 0, 5, now).unwrap();
    let live = source.poll_queue(b"db", "jobs", now, 10, lease).unwrap();
    assert_eq!(live.len(), 2);
    let token = |id: &str| {
        live.iter()
            .find(|claim| claim.id == id)
            .unwrap()
            .token
            .clone()
    };
    // `db` moves to a group with another incarnation: freeze, export,
    // import, seal, activate.
    place(
        source.store(),
        b"db",
        vec![
            PlacementCommand::Create {
                epoch: 1,
                op: "c".into(),
            },
            PlacementCommand::Freeze {
                epoch: 1,
                op: "m".into(),
            },
        ],
    );
    let read = source.store().begin_read().unwrap();
    let page = flower_engine::movement::export_page(&read, b"db", None, usize::MAX).unwrap();
    assert!(page.next.is_none());
    let rows = flower_engine::hex::encode(&flower_engine::movement::encode_changes(&page.changes));
    let digest = flower_engine::movement::digest(&read, b"db").unwrap().hex();
    let revision = read
        .meta_u64(b"db", flower_engine::storage::meta::REVISION)
        .unwrap();
    drop(read);
    let mut path = std::env::temp_dir();
    path.push(format!(
        "flower-queue-move-destination-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&path);
    let store = GroupStore::create(&path).unwrap();
    let west = 4_242_424_242;
    set_incarnation(&store, flower_engine::batch::HEAD_DB, west);
    place(
        &store,
        b"db",
        vec![
            PlacementCommand::ImportBegin {
                epoch: 2,
                op: "m".into(),
                source: "east".into(),
                stage: Stage::Rest,
                keep: false,
            },
            PlacementCommand::ImportRows {
                epoch: 2,
                op: "m".into(),
                page: 0,
                rows,
            },
            PlacementCommand::ImportSeal {
                epoch: 2,
                op: "m".into(),
                revision,
                digest,
            },
            PlacementCommand::Activate {
                epoch: 2,
                op: "m".into(),
            },
        ],
    );
    let destination = Engine::new(store);
    // The claim taken at the source completes at the destination.
    destination
        .complete_queue(b"db", "jobs", &first, &token(&first), now + 1_000)
        .unwrap();
    // One from before the source's restore is still of another history.
    let fenced = destination
        .complete_queue(b"db", "jobs", &old, &old_claim[0].token, now + 1_000)
        .unwrap_err();
    assert!(fenced.message.contains("history"), "{fenced:?}");
    // A restore of the destination fences out what was taken before it,
    // at the source or here.
    set_incarnation(destination.store(), b"db", west + 1);
    let fenced = destination
        .complete_queue(b"db", "jobs", &second, &token(&second), now + 1_000)
        .unwrap_err();
    assert!(fenced.message.contains("history"), "{fenced:?}");
    drop(destination);
    let _ = std::fs::remove_file(&path);
    teardown(source, source_path);
}
