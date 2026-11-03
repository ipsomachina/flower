//! Concurrent reads: queries through [`EngineReader`] run in parallel
//! with each other and with the writer, against immutable snapshots,
//! and never let a stale derived cache reach the store.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use flower_engine::descriptors::ReadConsistency;
use flower_engine::engine::{CallOptions, Engine, MutationOptions};
use flower_engine::runtime::{CallKind, ClosureRuntime, HostCall, Runtime};
use flower_engine::storage::GroupStore;
use flower_sdk::abi::{HostOp, decode_envelope};
use flower_sdk::{Failure, Value, fail};

fn s(text: &str) -> Value {
    Value::String(text.to_string())
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

/// Canonical key text for a simple string key.
fn k(name: &str) -> String {
    format!("\"{name}\"")
}

fn manifest() -> Value {
    let string = obj(vec![("type", s("string"))]);
    let tasks = obj(vec![
        ("name", s("tasks")),
        ("key", string.clone()),
        (
            "value",
            obj(vec![
                ("type", s("object")),
                (
                    "fields",
                    arr(vec![obj(vec![
                        ("name", s("title")),
                        ("schema", string),
                        ("optional", Value::Bool(false)),
                    ])]),
                ),
            ]),
        ),
        ("indexes", arr(vec![])),
        ("references", arr(vec![])),
    ]);
    let definition = |name: &str, kind: &str| {
        obj(vec![
            ("name", s(name)),
            ("kind", s(kind)),
            ("args", obj(vec![("type", s("json"))])),
        ])
    };
    obj(vec![
        ("collections", arr(vec![tasks])),
        (
            "definitions",
            arr(vec![
                definition("get", "query"),
                definition("version", "query"),
                definition("rendezvous", "query"),
                definition("parked_get", "query"),
                definition("parked_title", "query"),
                definition("title", "query"),
                definition("title_of_a", "derived"),
                definition("put", "mutation"),
            ]),
        ),
        ("triggers", arr(vec![])),
        ("http", arr(vec![])),
    ])
}

/// One host call through a closure's handler, decoding the envelope.
fn hc(host: &mut dyn HostCall, op: HostOp, args: Vec<Value>) -> Result<Value, Failure> {
    let input = Value::Array(args).encode_binary().unwrap();
    let output = host.op(op as u32 as u8, &input);
    decode_envelope(Value::decode_binary(&output).unwrap())
}

fn get(host: &mut dyn HostCall, key: &str) -> Result<Value, Failure> {
    hc(host, HostOp::Get, vec![s(""), s("tasks"), s(&k(key))])
}

fn title_of_a(host: &mut dyn HostCall) -> Result<Value, Failure> {
    hc(
        host,
        HostOp::GetDerived,
        vec![s(""), s("title_of_a"), Value::Null],
    )
}

/// A one-shot latch a guest parks on until the test opens it.
#[derive(Default)]
struct Gate {
    open: Mutex<bool>,
    changed: Condvar,
}

impl Gate {
    fn open(&self) {
        *self.open.lock().unwrap() = true;
        self.changed.notify_all();
    }

    fn wait(&self) {
        let mut open = self.open.lock().unwrap();
        while !*open {
            open = self.changed.wait(open).unwrap();
        }
    }
}

/// Shared test state the guest reads and reports through.
#[derive(Default)]
struct Probe {
    /// Queries inside the rendezvous right now.
    inside: AtomicUsize,
    /// Parked queries that reached their gate.
    parked: AtomicUsize,
    /// Releases parked queries.
    gate: Gate,
    /// Derived evaluations of `title_of_a`.
    evals: AtomicUsize,
}

/// Queries that must overlap: each waits inside until `expected`
/// queries are inside at once, or gives up after five seconds.
fn rendezvous(probe: &Probe, expected: usize) -> bool {
    probe.inside.fetch_add(1, Ordering::SeqCst);
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if probe.inside.load(Ordering::SeqCst) >= expected {
            break true;
        }
        if Instant::now() >= deadline {
            break false;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn runtime(probe: Arc<Probe>, version: &'static str) -> Box<dyn Runtime> {
    Box::new(ClosureRuntime::new(
        move |kind: CallKind, _module: &str, name: &str, args: &Value, host: &mut dyn HostCall| {
            match (kind, name) {
                (CallKind::Query, "get") => {
                    let Value::String(key) = args else {
                        return Err(fail("TYPE_MISMATCH", "get takes a key"));
                    };
                    get(host, key)
                }
                (CallKind::Query, "version") => Ok(s(version)),
                (CallKind::Query, "rendezvous") => {
                    let Value::Number(expected) = args else {
                        return Err(fail("TYPE_MISMATCH", "rendezvous takes a count"));
                    };
                    Ok(Value::Bool(rendezvous(&probe, *expected as usize)))
                }
                (CallKind::Query, "parked_get") => {
                    let value = get(host, "a")?;
                    probe.parked.fetch_add(1, Ordering::SeqCst);
                    probe.gate.wait();
                    Ok(value)
                }
                (CallKind::Query, "parked_title") => {
                    let value = title_of_a(host)?;
                    probe.parked.fetch_add(1, Ordering::SeqCst);
                    probe.gate.wait();
                    Ok(value)
                }
                (CallKind::Query, "title") => title_of_a(host),
                (CallKind::Derived, "title_of_a") => {
                    probe.evals.fetch_add(1, Ordering::SeqCst);
                    get(host, "a")
                }
                (CallKind::Mutation, "put") => {
                    let Value::String(title) = args else {
                        return Err(fail("TYPE_MISMATCH", "put takes a title"));
                    };
                    hc(
                        host,
                        HostOp::Set,
                        vec![
                            s(""),
                            s("tasks"),
                            s(&k("a")),
                            obj(vec![("title", s(title))]),
                        ],
                    )?;
                    Ok(Value::Null)
                }
                _ => Err(fail("UNKNOWN_METHOD", name.to_string())),
            }
        },
    ))
}

fn setup(test: &str, probe: &Arc<Probe>) -> (Engine, std::path::PathBuf) {
    let mut path = std::env::temp_dir();
    path.push(format!("flower-concurrency-{test}-{}", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let mut engine = Engine::new(GroupStore::create(&path).unwrap());
    engine
        .deploy(
            b"db",
            "app",
            [7u8; 32],
            b"",
            &manifest(),
            runtime(probe.clone(), "v1"),
        )
        .unwrap();
    (engine, path)
}

fn teardown(engine: Engine, path: std::path::PathBuf) {
    drop(engine);
    let _ = std::fs::remove_file(&path);
}

fn put(engine: &mut Engine, title: &str) {
    engine
        .invoke_mutation(b"db", "app", "put", &s(title), &MutationOptions::default())
        .unwrap();
}

/// Wait until `count` guests parked at the gate.
fn await_parked(probe: &Probe, count: usize) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while probe.parked.load(Ordering::SeqCst) < count {
        assert!(Instant::now() < deadline, "guests never parked");
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn title(value: &str) -> Value {
    obj(vec![("title", s(value))])
}

#[test]
fn readers_evaluate_in_parallel() {
    const READERS: usize = 4;
    let probe = Arc::new(Probe::default());
    let (engine, path) = setup("parallel", &probe);
    let handles: Vec<_> = (0..READERS)
        .map(|_| {
            let reader = engine.reader();
            std::thread::spawn(move || {
                reader.invoke_query(
                    b"db",
                    "app",
                    "rendezvous",
                    &Value::Number(READERS as f64),
                    &CallOptions::default(),
                )
            })
        })
        .collect();
    for handle in handles {
        // Every query saw all of them inside at once: none waited for
        // another to finish.
        assert_eq!(handle.join().unwrap().unwrap(), Value::Bool(true));
    }
    teardown(engine, path);
}

#[test]
fn readers_run_while_the_writer_commits() {
    let probe = Arc::new(Probe::default());
    let (mut engine, path) = setup("writer", &probe);
    put(&mut engine, "first");
    let reader = engine.reader();
    let (before, at) = reader
        .query_at(b"db", "app", "get", &s("a"), &CallOptions::default())
        .unwrap();
    assert_eq!(before, title("first"));
    let parked = {
        let reader = reader.clone();
        std::thread::spawn(move || {
            reader.query_at(
                b"db",
                "app",
                "parked_get",
                &Value::Null,
                &CallOptions::default(),
            )
        })
    };
    await_parked(&probe, 1);
    // The writer commits while the query holds its snapshot.
    put(&mut engine, "second");
    probe.gate.open();
    // The parked query answers from the snapshot it pinned, with that
    // snapshot's revision; a fresh one sees the commit.
    let (value, revision) = parked.join().unwrap().unwrap();
    assert_eq!((value, revision), (title("first"), at));
    let (after, moved) = reader
        .query_at(b"db", "app", "get", &s("a"), &CallOptions::default())
        .unwrap();
    assert_eq!(after, title("second"));
    assert_eq!(moved, at + 1);
    teardown(engine, path);
}

#[test]
fn stale_query_caches_never_install() {
    let probe = Arc::new(Probe::default());
    let (mut engine, path) = setup("stale-cache", &probe);
    let reader = engine.reader();
    let parked = {
        let reader = reader.clone();
        std::thread::spawn(move || {
            reader.invoke_query(
                b"db",
                "app",
                "parked_title",
                &Value::Null,
                &CallOptions::default(),
            )
        })
    };
    await_parked(&probe, 1);
    assert_eq!(probe.evals.load(Ordering::SeqCst), 1);
    // The commit lands after the parked query evaluated the derived
    // value but before it writes the cache.
    put(&mut engine, "fresh");
    probe.gate.open();
    assert_eq!(parked.join().unwrap().unwrap(), Value::Null);
    // The outcome computed over the old snapshot never installed: the
    // next read evaluates afresh instead of serving it, then caches.
    for _ in 0..2 {
        let value = reader
            .invoke_query(b"db", "app", "title", &Value::Null, &CallOptions::default())
            .unwrap();
        assert_eq!(value, title("fresh"));
    }
    assert_eq!(probe.evals.load(Ordering::SeqCst), 2);
    teardown(engine, path);
}

#[test]
fn exclusive_sections_fence_out_readers() {
    let probe = Arc::new(Probe::default());
    let (mut engine, path) = setup("exclusive", &probe);
    let reader = engine.reader();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let pending = engine.exclusive(|_| {
        let reader = reader.clone();
        let handle = std::thread::spawn(move || {
            let value = reader.invoke_query(
                b"db",
                "app",
                "version",
                &Value::Null,
                &CallOptions::default(),
            );
            let _ = done_tx.send(());
            value
        });
        // The reader cannot capture while the section runs.
        let blocked = done_rx.recv_timeout(Duration::from_millis(200)).is_err();
        (blocked, handle)
    });
    let (blocked, handle) = pending;
    assert!(blocked, "a reader captured inside an exclusive section");
    assert_eq!(handle.join().unwrap().unwrap(), s("v1"));
    teardown(engine, path);
}

#[test]
fn readers_see_activations_with_their_code() {
    let probe = Arc::new(Probe::default());
    let (mut engine, path) = setup("activation", &probe);
    let reader = engine.reader();
    let version = |reader: &flower_engine::engine::EngineReader| {
        reader
            .invoke_query(
                b"db",
                "app",
                "version",
                &Value::Null,
                &CallOptions::default(),
            )
            .unwrap()
    };
    assert_eq!(version(&reader), s("v1"));
    engine
        .deploy(
            b"db",
            "app",
            [8u8; 32],
            b"",
            &manifest(),
            runtime(probe.clone(), "v2"),
        )
        .unwrap();
    // A handle taken before the activation serves the new code.
    assert_eq!(version(&reader), s("v2"));
    assert_eq!(version(&engine.reader()), s("v2"));
    teardown(engine, path);
}

#[test]
fn a_panicking_exclusive_section_lifts_its_fence() {
    let probe = Arc::new(Probe::default());
    let (mut engine, path) = setup("exclusive-panic", &probe);
    let reader = engine.reader();
    let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        engine.exclusive(|_| -> u8 { panic!("section failed") })
    }));
    assert!(unwound.is_err());
    // Readers capture again, and later code changes still publish.
    let version = |reader: &flower_engine::engine::EngineReader| {
        reader
            .invoke_query(
                b"db",
                "app",
                "version",
                &Value::Null,
                &CallOptions::default(),
            )
            .unwrap()
    };
    assert_eq!(version(&reader), s("v1"));
    engine
        .deploy(
            b"db",
            "app",
            [8u8; 32],
            b"",
            &manifest(),
            runtime(probe.clone(), "v2"),
        )
        .unwrap();
    assert_eq!(version(&reader), s("v2"));
    teardown(engine, path);
}

/// `manifest()` with `query` declaring `consistency`.
fn declaring(query: &str, consistency: &str) -> Value {
    let Value::Object(mut root) = manifest() else {
        unreachable!("manifest is an object");
    };
    let Some(Value::Array(definitions)) = root.get_mut("definitions") else {
        unreachable!("manifest has definitions");
    };
    for definition in definitions {
        if let Value::Object(map) = definition
            && map.get("name") == Some(&s(query))
        {
            map.insert("consistency".to_string(), s(consistency));
        }
    }
    Value::Object(root)
}

const LOCAL: ReadConsistency = ReadConsistency::ReplicaLocal {
    max_staleness_ms: None,
};

#[test]
fn replica_local_reads_follow_the_catalog_their_snapshot_captured() {
    let probe = Arc::new(Probe::default());
    let (mut engine, path) = setup("replica-local", &probe);
    put(&mut engine, "first");
    let reader = engine.reader();
    let get = |reader: &flower_engine::engine::EngineReader| {
        reader.query_replica_local(b"db", "app", "get", &s("a"), &CallOptions::default(), LOCAL)
    };
    // Undeclared queries read linearizably: no replica-local answer.
    assert_eq!(
        reader.query_consistency(b"db", "app", "get"),
        Some(ReadConsistency::Linearizable)
    );
    assert_eq!(reader.query_consistency(b"db", "app", "missing"), None);
    assert_eq!(get(&reader).unwrap(), None);
    engine
        .deploy(
            b"db",
            "app",
            [8u8; 32],
            b"",
            &declaring("get", "replica_local"),
            runtime(probe.clone(), "v2"),
        )
        .unwrap();
    assert_eq!(reader.query_consistency(b"db", "app", "get"), Some(LOCAL));
    let (value, _) = get(&reader).unwrap().unwrap();
    assert_eq!(value, title("first"));
    // A deployment making it linearizable again ends replica-local
    // answers from the next capture on.
    engine
        .deploy(
            b"db",
            "app",
            [9u8; 32],
            b"",
            &declaring("get", "linearizable"),
            runtime(probe.clone(), "v3"),
        )
        .unwrap();
    assert_eq!(get(&reader).unwrap(), None);
    teardown(engine, path);
}
