//! Timer integration tests: guest scheduling with staged reads,
//! consensus-style fire preparation plus direct application, retry
//! budgets and revision-guarded double-fire rejection.

mod fixtures;

use fixtures::{arr, hc, n, obj, s};

use flower_engine::engine::{CallOptions, Engine, FirePrepare, MutationOptions};
use flower_engine::runtime::{CallKind, ClosureRuntime, HostCall, Runtime};
use flower_engine::storage::GroupStore;
use flower_engine::timer::TimerState;
use flower_sdk::abi::HostOp;
use flower_sdk::{Failure, Value, fail};

fn definition(name: &str, kind: &str) -> Value {
    definition_args(name, kind, obj(vec![("type", s("json"))]))
}

fn definition_args(name: &str, kind: &str, args: Value) -> Value {
    obj(vec![("name", s(name)), ("kind", s(kind)), ("args", args)])
}

fn manifest() -> Value {
    obj(vec![
        ("collections", arr(vec![])),
        (
            "definitions",
            arr(vec![
                definition("schedule", "mutation"),
                definition("handle", "mutation"),
                definition("boom", "mutation"),
                definition_args("strict", "mutation", obj(vec![("type", s("string"))])),
                definition("read", "query"),
            ]),
        ),
        ("triggers", arr(vec![])),
        ("http", arr(vec![])),
    ])
}

fn setup(test: &str, runtime: Box<dyn Runtime>) -> (Engine, std::path::PathBuf) {
    let mut path = std::env::temp_dir();
    path.push(format!("flower-timer-{test}-{}", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let store = GroupStore::create(&path).unwrap();
    let mut engine = Engine::new(store);
    engine
        .deploy(b"db", "app", [7u8; 32], b"", &manifest(), runtime)
        .unwrap();
    (engine, path)
}

fn teardown(engine: Engine, path: std::path::PathBuf) {
    drop(engine);
    let _ = std::fs::remove_file(&path);
}

fn schedule(
    host: &mut dyn HostCall,
    id: &str,
    due_ms: f64,
    handler: &str,
    args: Value,
    max: f64,
) -> Result<(), Failure> {
    hc(
        host,
        HostOp::TimerSchedule,
        vec![s(id), n(due_ms), s(handler), args, n(max)],
    )?;
    Ok(())
}

fn timer_get(host: &mut dyn HostCall, id: &str) -> Result<Value, Failure> {
    hc(host, HostOp::TimerGet, vec![s(id)])
}

fn timer_scan(host: &mut dyn HostCall, state: Value, limit: f64) -> Result<Value, Failure> {
    hc(host, HostOp::TimerScan, vec![state, n(limit)])
}

fn timer_cancel(host: &mut dyn HostCall, id: &str) -> Result<bool, Failure> {
    match hc(host, HostOp::TimerCancel, vec![s(id)])? {
        Value::Bool(removed) => Ok(removed),
        _ => panic!("cancel returns whether removed"),
    }
}

fn timer_retry(host: &mut dyn HostCall, id: &str, due_ms: f64) -> Result<(), Failure> {
    hc(host, HostOp::TimerRetry, vec![s(id), n(due_ms)])?;
    Ok(())
}

fn raw_set(host: &mut dyn HostCall, key: &str, value: &str) -> Result<(), Failure> {
    hc(
        host,
        HostOp::Set,
        vec![s(""), s("$raw/marks"), s(key), s(value)],
    )?;
    Ok(())
}

fn raw_get(host: &mut dyn HostCall, key: &str) -> Result<Value, Failure> {
    hc(host, HostOp::RawGet, vec![s(""), s("$raw/marks"), s(key)])
}

/// App runtime: schedule/get/scan/cancel/retry timers, mark via
/// `$raw` from the handler, boom on demand.
fn app_runtime() -> Box<dyn Runtime> {
    Box::new(ClosureRuntime::new(
        |kind: CallKind, _module: &str, name: &str, args: &Value, host: &mut dyn HostCall| {
            let arg = |field: &str| match args {
                Value::Object(map) => map.get(field).cloned().unwrap_or(Value::Null),
                _ => Value::Null,
            };
            let arg_str = |field: &str| match arg(field) {
                Value::String(text) => text,
                _ => String::new(),
            };
            let arg_num = |field: &str| match arg(field) {
                Value::Number(number) => number,
                _ => 0.0,
            };
            match (kind, name) {
                (CallKind::Mutation, "schedule") => {
                    if !arg_str("cancel").is_empty() {
                        return Ok(Value::Bool(timer_cancel(host, &arg_str("cancel"))?));
                    }
                    if !arg_str("retry").is_empty() {
                        timer_retry(host, &arg_str("retry"), arg_num("due_ms"))?;
                        return timer_get(host, &arg_str("retry"));
                    }
                    schedule(
                        host,
                        &arg_str("id"),
                        arg_num("due_ms"),
                        &arg_str("handler"),
                        arg("args"),
                        arg_num("max"),
                    )?;
                    Ok(timer_get(host, &arg_str("id"))?)
                }
                (CallKind::Mutation, "handle") => {
                    raw_set(host, &arg_str("mark"), "fired")?;
                    Ok(s("handled"))
                }
                (CallKind::Mutation, "boom") => Err(fail("BOOM", "handler failed")),
                (CallKind::Mutation, "strict") => Ok(Value::Null),
                (CallKind::Query, "read") => {
                    if !arg_str("timer").is_empty() {
                        return timer_get(host, &arg_str("timer"));
                    }
                    if !arg_str("raw").is_empty() {
                        return raw_get(host, &arg_str("raw"));
                    }
                    Ok(timer_scan(host, arg("state"), arg_num("limit"))?)
                }
                _ => Err(fail("UNKNOWN_METHOD", name.to_string())),
            }
        },
    ))
}

fn schedule_args(id: &str, due_ms: f64, handler: &str, args: Value, max: f64) -> Value {
    obj(vec![
        ("id", s(id)),
        ("due_ms", n(due_ms)),
        ("handler", s(handler)),
        ("args", args),
        ("max", n(max)),
    ])
}

fn apply_fire(engine: &mut Engine, id: &str, position: u64) -> Value {
    match engine.prepare_timer_fire(b"db", id).unwrap() {
        FirePrepare::Patch(prepared) => {
            let outcome = engine.apply_patch(&prepared.patch, position).unwrap();
            outcome.result.unwrap()
        }
        FirePrepare::Retry(_) => panic!("expected a fire patch"),
        FirePrepare::Hold(_) => panic!("expected a fire patch, not a hold"),
        FirePrepare::Skip => panic!("expected a fireable timer"),
    }
}

fn apply_retry(engine: &mut Engine, id: &str, position: u64) {
    match engine.prepare_timer_fire(b"db", id).unwrap() {
        FirePrepare::Retry(prepared) => {
            let outcome = engine.apply_patch(&prepared.patch, position).unwrap();
            outcome.result.unwrap();
        }
        _ => panic!("expected a retry patch"),
    }
}

#[test]
fn guest_schedule_fires_handler_atomically() {
    let (mut engine, path) = setup("fire", app_runtime());
    // Schedule through the guest: the staged timer reads back in
    // the same mutation.
    let args = schedule_args("t1", 0.0, "handle", obj(vec![("mark", s("m1"))]), 3.0);
    let out = engine
        .invoke_mutation(b"db", "app", "schedule", &args, &MutationOptions::default())
        .unwrap();
    assert_eq!(
        out,
        obj(vec![
            ("id", s("t1")),
            ("handler", s("handle")),
            ("args", obj(vec![("mark", s("m1"))])),
            ("due_ms", n(0.0)),
            ("attempts", n(0.0)),
            ("max_attempts", n(3.0)),
            ("state", s("pending")),
        ])
    );
    // Fire through prepare plus apply: the handler's mark commits
    // and the timer vanishes in one patch.
    let value = apply_fire(&mut engine, "t1", 1);
    assert_eq!(value, s("handled"));
    assert!(engine.get_timer(b"db", "t1").unwrap().is_none());
    let out = engine
        .invoke_query(
            b"db",
            "app",
            "read",
            &obj(vec![("raw", s("m1"))]),
            &CallOptions::default(),
        )
        .unwrap();
    assert_eq!(out, s("fired"));
    teardown(engine, path);
}

#[test]
fn schedule_replaces_same_id() {
    let (mut engine, path) = setup("replace", app_runtime());
    let first = schedule_args("t1", 100.0, "handle", Value::Null, 3.0);
    engine
        .invoke_mutation(
            b"db",
            "app",
            "schedule",
            &first,
            &MutationOptions::default(),
        )
        .unwrap();
    let second = schedule_args("t1", 200.0, "boom", Value::Null, 5.0);
    engine
        .invoke_mutation(
            b"db",
            "app",
            "schedule",
            &second,
            &MutationOptions::default(),
        )
        .unwrap();
    let timer = engine.get_timer(b"db", "t1").unwrap().unwrap();
    assert_eq!(timer.handler, "boom");
    assert_eq!(timer.due_ms, 200);
    assert_eq!(timer.max_attempts, 5);
    let stats = engine.timer_stats(b"db").unwrap();
    assert_eq!((stats.pending, stats.dead), (1, 0));
    teardown(engine, path);
}

#[test]
fn schedule_rolls_back_with_mutation() {
    let runtime = ClosureRuntime::new(
        |kind: CallKind, _module: &str, name: &str, _args: &Value, host: &mut dyn HostCall| match (
            kind, name,
        ) {
            (CallKind::Mutation, "schedule") => {
                schedule(host, "t1", 0.0, "handle", Value::Null, 3.0)?;
                assert_ne!(timer_get(host, "t1")?, Value::Null);
                Err(fail("NOPE", "rollback"))
            }
            _ => Err(fail("UNKNOWN_METHOD", name.to_string())),
        },
    );
    let (mut engine, path) = setup("rollback", Box::new(runtime));
    let failure = engine
        .invoke_mutation(
            b"db",
            "app",
            "schedule",
            &Value::Null,
            &MutationOptions::default(),
        )
        .unwrap_err();
    assert_eq!(failure.code, "NOPE");
    assert!(engine.get_timer(b"db", "t1").unwrap().is_none());
    teardown(engine, path);
}

#[test]
fn schedule_validation_rejects() {
    let cases: Vec<(String, f64, String, Value, f64, &'static str)> = vec![
        (
            "".to_string(),
            0.0,
            "handle".to_string(),
            Value::Null,
            3.0,
            "INVALID_VALUE",
        ),
        // IDs are any text without control characters.
        (
            "tab\there".to_string(),
            0.0,
            "handle".to_string(),
            Value::Null,
            3.0,
            "INVALID_VALUE",
        ),
        (
            "t1".to_string(),
            0.0,
            "".to_string(),
            Value::Null,
            3.0,
            "INVALID_VALUE",
        ),
        (
            "t1".to_string(),
            0.0,
            "nope".to_string(),
            Value::Null,
            3.0,
            "INVALID_VALUE",
        ),
        (
            "t1".to_string(),
            0.0,
            "read".to_string(),
            Value::Null,
            3.0,
            "INVALID_VALUE",
        ),
        (
            "t1".to_string(),
            0.0,
            "strict".to_string(),
            n(1.0),
            3.0,
            "INVALID_ARGUMENT",
        ),
        (
            "t1".to_string(),
            0.0,
            "handle".to_string(),
            Value::Null,
            0.0,
            "INVALID_VALUE",
        ),
        (
            "t1".to_string(),
            0.0,
            "handle".to_string(),
            Value::Null,
            101.0,
            "INVALID_VALUE",
        ),
        (
            "t1".to_string(),
            1.5,
            "handle".to_string(),
            Value::Null,
            3.0,
            "TYPE_MISMATCH",
        ),
    ];
    for (id, due, handler, args, max, code) in cases {
        let (mut engine, path) = setup("validate", app_runtime());
        let failure = engine
            .invoke_mutation(
                b"db",
                "app",
                "schedule",
                &schedule_args(&id, due, &handler, args, max),
                &MutationOptions::default(),
            )
            .unwrap_err();
        assert_eq!(failure.code, code, "case {id} {handler}");
        teardown(engine, path);
    }
}

#[test]
fn timer_reads_merge_and_reject_writes_in_queries() {
    let (engine, _path) = setup("reads", app_runtime());
    engine
        .schedule_timer(b"db", "t1", "app", "handle", &Value::Null, 10, 3, 0)
        .unwrap();
    engine
        .schedule_timer(b"db", "t2", "app", "handle", &Value::Null, 5, 3, 0)
        .unwrap();
    // Due-first scan through a query.
    let out = engine
        .invoke_query(
            b"db",
            "app",
            "read",
            &obj(vec![("state", Value::Null), ("limit", n(10.0))]),
            &CallOptions::default(),
        )
        .unwrap();
    let Value::Array(items) = out else {
        panic!("scan returns an array");
    };
    assert_eq!(items.len(), 2);
    // Dead scan is empty; bad states and limits fail.
    let out = engine
        .invoke_query(
            b"db",
            "app",
            "read",
            &obj(vec![("state", s("dead")), ("limit", n(10.0))]),
            &CallOptions::default(),
        )
        .unwrap();
    assert_eq!(out, arr(vec![]));
    for (state, limit) in [(s("bogus"), 10.0), (Value::Null, 0.0), (Value::Null, 101.0)] {
        let failure = engine
            .invoke_query(
                b"db",
                "app",
                "read",
                &obj(vec![("state", state), ("limit", n(limit))]),
                &CallOptions::default(),
            )
            .unwrap_err();
        assert_eq!(failure.code, "INVALID_VALUE");
    }
    // Scheduling from a query is rejected.
    let runtime = ClosureRuntime::new(
        |kind: CallKind, _module: &str, name: &str, _args: &Value, host: &mut dyn HostCall| match (
            kind, name,
        ) {
            (CallKind::Query, "read") => {
                schedule(host, "t9", 0.0, "handle", Value::Null, 3.0)?;
                Ok(Value::Null)
            }
            _ => Err(fail("UNKNOWN_METHOD", name.to_string())),
        },
    );
    let (engine, path) = setup("readwrite", Box::new(runtime));
    let failure = engine
        .invoke_query(b"db", "app", "read", &Value::Null, &CallOptions::default())
        .unwrap_err();
    assert_eq!(failure.code, "INVALID_INVOCATION");
    teardown(engine, path);
}

#[test]
fn fire_skips_missing_dead_and_future() {
    let (mut engine, path) = setup("skip", app_runtime());
    assert!(matches!(
        engine.prepare_timer_fire(b"db", "missing").unwrap(),
        FirePrepare::Skip
    ));
    engine
        .schedule_timer(
            b"db",
            "future",
            "app",
            "handle",
            &Value::Null,
            u64::MAX,
            3,
            0,
        )
        .unwrap();
    assert!(matches!(
        engine.prepare_timer_fire(b"db", "future").unwrap(),
        FirePrepare::Skip
    ));
    engine
        .schedule_timer(b"db", "once", "app", "boom", &Value::Null, 0, 1, 0)
        .unwrap();
    apply_retry(&mut engine, "once", 1);
    let timer = engine.get_timer(b"db", "once").unwrap().unwrap();
    assert_eq!(timer.state, TimerState::Dead);
    assert!(matches!(
        engine.prepare_timer_fire(b"db", "once").unwrap(),
        FirePrepare::Skip
    ));
    teardown(engine, path);
}

#[test]
fn fire_failure_retries_with_backoff() {
    let (mut engine, path) = setup("retry", app_runtime());
    engine
        .schedule_timer(b"db", "t1", "app", "boom", &Value::Null, 0, 3, 0)
        .unwrap();
    apply_retry(&mut engine, "t1", 1);
    let timer = engine.get_timer(b"db", "t1").unwrap().unwrap();
    assert_eq!(timer.state, TimerState::Pending);
    assert_eq!(timer.attempts, 1);
    // First backoff is exactly one second past prepare time.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    assert!(timer.due_ms <= now + 1_000);
    assert!(timer.due_ms + 5_000 > now);
    // The failed handler left no mark: failure rolled back first.
    let out = engine
        .invoke_query(
            b"db",
            "app",
            "read",
            &obj(vec![("raw", s("m1"))]),
            &CallOptions::default(),
        )
        .unwrap();
    assert_eq!(out, Value::Null);
    teardown(engine, path);
}

#[test]
fn fire_unknown_handler_dies_on_budget() {
    let (mut engine, path) = setup("unrunnable", app_runtime());
    // Direct schedules skip validation: the handler vanished after
    // scheduling (a deploy removed it).
    engine
        .schedule_timer(b"db", "t1", "app", "gone", &Value::Null, 0, 1, 0)
        .unwrap();
    apply_retry(&mut engine, "t1", 1);
    let timer = engine.get_timer(b"db", "t1").unwrap().unwrap();
    assert_eq!(timer.state, TimerState::Dead);
    assert_eq!(timer.attempts, 1);
    teardown(engine, path);
}

#[test]
fn stale_fire_rejects_without_effects() {
    let (mut engine, path) = setup("stale", app_runtime());
    engine
        .schedule_timer(
            b"db",
            "t1",
            "app",
            "handle",
            &obj(vec![("mark", s("m1"))]),
            0,
            3,
            0,
        )
        .unwrap();
    let fire = match engine.prepare_timer_fire(b"db", "t1").unwrap() {
        FirePrepare::Patch(prepared) => prepared,
        _ => panic!("expected a fire patch"),
    };
    // Another commit lands first: the fire patch goes stale.
    let dummy = match engine
        .prepare_mutation(
            b"db",
            "app",
            "handle",
            &obj(vec![("mark", s("other"))]),
            &MutationOptions::default(),
        )
        .unwrap()
    {
        flower_engine::engine::PrepareOutcome::Patch(prepared) => prepared,
        _ => panic!("dummy short-circuited"),
    };
    engine.apply_patch(&dummy.patch, 1).unwrap();
    let outcome = engine.apply_patch(&fire.patch, 2).unwrap();
    assert_eq!(outcome.result.unwrap_err().code, "stale_revision");
    // Rejected means applied-only: the timer still pends, unmarked.
    let timer = engine.get_timer(b"db", "t1").unwrap().unwrap();
    assert_eq!(timer.state, TimerState::Pending);
    let out = engine
        .invoke_query(
            b"db",
            "app",
            "read",
            &obj(vec![("raw", s("m1"))]),
            &CallOptions::default(),
        )
        .unwrap();
    assert_eq!(out, Value::Null);
    teardown(engine, path);
}

#[test]
fn cancel_retry_purge() {
    let (mut engine, path) = setup("direct", app_runtime());
    engine
        .schedule_timer(b"db", "t1", "app", "handle", &Value::Null, 0, 3, 100)
        .unwrap();
    // Guest cancel reports removal, then absence.
    let out = engine
        .invoke_mutation(
            b"db",
            "app",
            "schedule",
            &obj(vec![("cancel", s("t1"))]),
            &MutationOptions::default(),
        )
        .unwrap();
    assert_eq!(out, Value::Bool(true));
    let out = engine
        .invoke_mutation(
            b"db",
            "app",
            "schedule",
            &obj(vec![("cancel", s("t1"))]),
            &MutationOptions::default(),
        )
        .unwrap();
    assert_eq!(out, Value::Bool(false));
    // One-shot timer dies on its first failed fire.
    engine
        .schedule_timer(b"db", "t2", "app", "boom", &Value::Null, 0, 1, 200)
        .unwrap();
    apply_retry(&mut engine, "t2", 1);
    assert!(engine.retry_timer(b"db", "t1", 0, 0).is_err());
    // Guest retry revives it with a fresh budget.
    let out = engine
        .invoke_mutation(
            b"db",
            "app",
            "schedule",
            &obj(vec![("retry", s("t2")), ("due_ms", n(50.0))]),
            &MutationOptions::default(),
        )
        .unwrap();
    assert_ne!(out, Value::Null);
    let timer = engine.get_timer(b"db", "t2").unwrap().unwrap();
    assert_eq!(timer.state, TimerState::Pending);
    assert_eq!(timer.attempts, 0);
    assert_eq!(timer.due_ms, 50);
    // Kill it again, then purge the dead letter away.
    apply_retry(&mut engine, "t2", 2);
    assert_eq!(engine.purge_timers(b"db", 201).unwrap(), 1);
    assert_eq!(engine.purge_timers(b"db", 201).unwrap(), 0);
    assert!(engine.get_timer(b"db", "t2").unwrap().is_none());
    let stats = engine.timer_stats(b"db").unwrap();
    assert_eq!((stats.pending, stats.dead), (0, 0));
    teardown(engine, path);
}
