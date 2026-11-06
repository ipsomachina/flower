//! Parallel reads and admission on a cluster member: queries evaluate
//! on the engine reader, overlapping each other and the writer lane,
//! and admission bounds them with explicit, retryable overload.

mod fixtures;

use fixtures::{arr, hc, obj, s};

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use flower_cluster::admission::AdmissionConfig;
use flower_cluster::network::{InProcessFactory, Registry};
use flower_cluster::node::{Node, NodeError};
use flower_cluster::statemachine::RuntimeFactory;
use flower_engine::engine::{CallOptions, MutationOptions};
use flower_engine::runtime::{CallKind, ClosureRuntime, HostCall, Runtime};
use flower_sdk::abi::HostOp;
use flower_sdk::{Failure, Value, fail};

/// Guest time of one `slow` query.
const SLOW: Duration = Duration::from_millis(50);

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
                definition("slow", "query"),
                definition("rendezvous", "query"),
                definition("parked", "query"),
                definition("parked_now", "query"),
                definition("parked_fail", "query"),
                definition("put", "mutation"),
            ]),
        ),
        ("triggers", arr(vec![])),
        ("http", arr(vec![])),
    ])
}

/// The one row the tests read and write.
fn get_a(host: &mut dyn HostCall) -> Result<Value, Failure> {
    hc(host, HostOp::Get, vec![s(""), s("tasks"), s("\"a\"")])
}

/// A latch guests park on until the test opens it.
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

#[derive(Default)]
struct Probe {
    inside: AtomicUsize,
    parked: AtomicUsize,
    gate: Gate,
}

impl Probe {
    /// Wait until `count` guests parked at the gate.
    async fn parked(&self, count: usize) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while self.parked.load(Ordering::SeqCst) < count {
            assert!(Instant::now() < deadline, "guests never parked");
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }
}

#[derive(Clone)]
struct Factory {
    probe: Arc<Probe>,
}

impl RuntimeFactory for Factory {
    fn build(
        &self,
        _db: &[u8],
        _module: &str,
        _wasm: &[u8],
        _manifest: &Value,
    ) -> Result<Box<dyn Runtime>, String> {
        let probe = self.probe.clone();
        Ok(Box::new(ClosureRuntime::new(
            move |kind: CallKind,
                  _module: &str,
                  name: &str,
                  args: &Value,
                  host: &mut dyn HostCall| {
                match (kind, name) {
                    (CallKind::Query, "get") => get_a(host),
                    (CallKind::Query, "slow") => {
                        std::thread::sleep(SLOW);
                        Ok(Value::Null)
                    }
                    (CallKind::Query, "rendezvous") => {
                        let Value::Number(expected) = args else {
                            return Err(fail("TYPE_MISMATCH", "rendezvous takes a count"));
                        };
                        probe.inside.fetch_add(1, Ordering::SeqCst);
                        let deadline = Instant::now() + Duration::from_secs(10);
                        while probe.inside.load(Ordering::SeqCst) < *expected as usize {
                            if Instant::now() >= deadline {
                                return Ok(Value::Bool(false));
                            }
                            std::thread::sleep(Duration::from_millis(1));
                        }
                        Ok(Value::Bool(true))
                    }
                    (CallKind::Query, "parked") => {
                        let value = get_a(host)?;
                        probe.parked.fetch_add(1, Ordering::SeqCst);
                        probe.gate.wait();
                        Ok(value)
                    }
                    // Reads the clock, so its result never enters the cache.
                    (CallKind::Query, "parked_now") => {
                        hc(host, HostOp::Now, vec![])?;
                        let value = get_a(host)?;
                        probe.parked.fetch_add(1, Ordering::SeqCst);
                        probe.gate.wait();
                        Ok(value)
                    }
                    (CallKind::Query, "parked_fail") => {
                        get_a(host)?;
                        probe.parked.fetch_add(1, Ordering::SeqCst);
                        probe.gate.wait();
                        Err(fail("PARKED", "failed after parking"))
                    }
                    (CallKind::Mutation, "put") => {
                        hc(
                            host,
                            HostOp::Set,
                            vec![
                                s(""),
                                s("tasks"),
                                s("\"a\""),
                                obj(vec![("title", args.clone())]),
                            ],
                        )?;
                        Ok(Value::Null)
                    }
                    _ => Err(fail("UNKNOWN_METHOD", name.to_string())),
                }
            },
        )))
    }
}

/// A leading single member with the app deployed.
async fn member(
    dir: &std::path::Path,
    probe: &Arc<Probe>,
    admission: AdmissionConfig,
) -> Arc<Node> {
    let registry = Registry::new();
    let node = Node::open(
        1,
        &dir.join("n1"),
        Arc::new(openraft::Config::default()),
        InProcessFactory::new(registry.clone()),
        Arc::new(Factory {
            probe: probe.clone(),
        }),
    )
    .await
    .unwrap()
    .with_admission(admission);
    registry.register(1, node.raft().clone());
    node.initialize(BTreeSet::from([1])).await.unwrap();
    for _ in 0..200 {
        if node.status().leader == Some(1) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    node.deploy(b"db", "app", [9u8; 32], b"wasm", &manifest())
        .await
        .unwrap();
    Arc::new(node)
}

fn workers(query_workers: usize) -> AdmissionConfig {
    AdmissionConfig {
        query_workers,
        ..AdmissionConfig::default()
    }
}

async fn query(node: &Node, name: &str, args: Value) -> Result<(Value, u64), NodeError> {
    node.query_at(b"db", "app", name, &args, &CallOptions::default())
        .await
}

async fn put(node: &Node, title: &str) {
    node.mutate(b"db", "app", "put", &s(title), &MutationOptions::default())
        .await
        .unwrap();
}

/// Fail instead of hanging when `future` waits on something it should not.
async fn promptly<T>(what: &str, future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), future)
        .await
        .unwrap_or_else(|_| panic!("{what} waited on a parked query"))
}

#[tokio::test(flavor = "multi_thread")]
async fn queries_overlap_on_the_reader() {
    const QUERIES: usize = 4;
    let dir = tempfile::tempdir().unwrap();
    let probe = Arc::new(Probe::default());
    let node = member(dir.path(), &probe, workers(QUERIES)).await;
    // Identical calls in flight would wait for the first one: this
    // measures evaluation itself.
    node.set_query_flight_bytes(0);
    let tasks: Vec<_> = (0..QUERIES)
        .map(|_| {
            let node = node.clone();
            tokio::spawn(
                async move { query(&node, "rendezvous", Value::Number(QUERIES as f64)).await },
            )
        })
        .collect();
    for task in tasks {
        // Each query saw all of them evaluating at once.
        assert_eq!(task.await.unwrap().unwrap().0, Value::Bool(true));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn queries_never_wait_for_the_writer_lane() {
    let dir = tempfile::tempdir().unwrap();
    let probe = Arc::new(Probe::default());
    let node = member(dir.path(), &probe, workers(4)).await;
    put(&node, "first").await;
    let (_, at) = query(&node, "get", Value::Null).await.unwrap();
    let parked = {
        let node = node.clone();
        tokio::spawn(async move { query(&node, "parked", Value::Null).await })
    };
    probe.parked(1).await;
    // A whole mutation (prepare on the writer lane, Raft commit, apply)
    // completes while the query holds its snapshot, and so do more
    // queries.
    promptly("a mutation", put(&node, "second")).await;
    let (fresh, moved) = promptly("a query", query(&node, "get", Value::Null))
        .await
        .unwrap();
    assert_eq!(fresh, obj(vec![("title", s("second"))]));
    assert_eq!(moved, at + 1);
    // The engine lock itself held by the writer stops no query.
    let (held_tx, held_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let writer = {
        let node = node.clone();
        tokio::spawn(async move {
            node.with_engine(move |_| {
                let _ = held_tx.send(());
                let _ = release_rx.recv();
                Ok(())
            })
            .await
        })
    };
    held_rx.await.unwrap();
    promptly("a query", query(&node, "get", Value::Null))
        .await
        .unwrap();
    release_tx.send(()).unwrap();
    writer.await.unwrap().unwrap();
    // The parked query answers from the snapshot it pinned.
    probe.gate.open();
    let (value, revision) = parked.await.unwrap().unwrap();
    assert_eq!((value, revision), (obj(vec![("title", s("first"))]), at));
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_queries_beat_the_serial_writer_lane() {
    const QUERIES: usize = 8;
    let dir = tempfile::tempdir().unwrap();
    let probe = Arc::new(Probe::default());
    let node = member(dir.path(), &probe, workers(QUERIES)).await;
    // Before: every query behind the one engine lock, as all reads ran
    // until the reader split.
    let started = Instant::now();
    let lane: Vec<_> = (0..QUERIES)
        .map(|_| {
            let node = node.clone();
            tokio::spawn(async move {
                node.with_engine(|engine| {
                    engine
                        .invoke_query(b"db", "app", "slow", &Value::Null, &CallOptions::default())
                        .map_err(|failure| NodeError::Failure {
                            code: failure.code,
                            message: failure.message,
                        })
                })
                .await
            })
        })
        .collect();
    for task in lane {
        task.await.unwrap().unwrap();
    }
    let serial = started.elapsed();
    // After: the same queries through the node, on the reader.
    let started = Instant::now();
    let reader: Vec<_> = (0..QUERIES)
        .map(|_| {
            let node = node.clone();
            tokio::spawn(async move { query(&node, "slow", Value::Null).await })
        })
        .collect();
    for task in reader {
        task.await.unwrap().unwrap();
    }
    let parallel = started.elapsed();
    eprintln!(
        "{QUERIES} queries of {SLOW:?}: writer lane {serial:?}, reader {parallel:?} \
         ({:.1}x)",
        serial.as_secs_f64() / parallel.as_secs_f64()
    );
    assert!(
        serial >= SLOW * QUERIES as u32,
        "the lane overlapped queries"
    );
    assert!(
        parallel * 2 < serial,
        "reader queries did not overlap: {parallel:?} vs {serial:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn saturated_evaluation_overloads_explicitly() {
    let dir = tempfile::tempdir().unwrap();
    let probe = Arc::new(Probe::default());
    let config = AdmissionConfig {
        query_workers: 1,
        queued_input_bytes: 8 * 1024,
        max_wait: Duration::from_millis(100),
        ..AdmissionConfig::default()
    };
    let node = member(dir.path(), &probe, config).await;
    let parked = {
        let node = node.clone();
        tokio::spawn(async move { query(&node, "parked", Value::Null).await })
    };
    probe.parked(1).await;
    // The only slot is busy: a query waits its bound, then overloads.
    let started = Instant::now();
    let waited = query(&node, "get", Value::Null).await.unwrap_err();
    assert!(started.elapsed() >= Duration::from_millis(100));
    // Bytes beyond the budget refuse without waiting.
    let started = Instant::now();
    let refused = query(&node, "get", s(&"x".repeat(16 * 1024)))
        .await
        .unwrap_err();
    assert!(started.elapsed() < Duration::from_millis(100));
    for error in [&waited, &refused] {
        assert!(
            matches!(
                error,
                NodeError::Overloaded {
                    retry_after_ms: 1000,
                    ..
                }
            ),
            "not an overload: {error:?}"
        );
    }
    let metrics = node.admission().metrics();
    assert_eq!((metrics.timed_out, metrics.rejected), (1, 1));
    probe.gate.open();
    parked.await.unwrap().unwrap();
    // Capacity back, queries run again.
    query(&node, "get", Value::Null).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn writers_and_deploys_keep_capacity_while_queries_saturate() {
    let dir = tempfile::tempdir().unwrap();
    let probe = Arc::new(Probe::default());
    let _release = Release(probe.clone());
    let node = member(dir.path(), &probe, workers(1)).await;
    let parked = {
        let node = node.clone();
        tokio::spawn(async move { query(&node, "parked", Value::Null).await })
    };
    probe.parked(1).await;
    let queued = {
        let node = node.clone();
        tokio::spawn(async move { query(&node, "get", Value::Null).await })
    };
    // Wait for admission rather than assuming the spawned task ran before
    // the writers. The guard also releases the guest if an assertion fails.
    promptly("query admission", async {
        while node.admission().metrics().classes[0].1.queued != 1 {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await;
    // Writers and deployments have capacity of their own.
    promptly("a mutation", put(&node, "written")).await;
    promptly(
        "a deployment",
        node.deploy(b"db", "app", [10u8; 32], b"wasm-v2", &manifest()),
    )
    .await
    .unwrap();
    let metrics = node.admission().metrics();
    assert_eq!(metrics.classes[0].1.queued, 1, "the queued query ran early");
    probe.gate.open();
    parked.await.unwrap().unwrap();
    let (value, _) = queued.await.unwrap().unwrap();
    assert_eq!(value, obj(vec![("title", s("written"))]));
}

/// Opens the gate when dropped, so a test that fails releases its
/// parked guests instead of hanging on them.
struct Release(Arc<Probe>);

impl Drop for Release {
    fn drop(&mut self) {
        self.0.gate.open();
    }
}

/// Wait until `count` calls waited for a flight on `node`.
async fn joined(node: &Node, count: u64) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while node.flight_stats().joined < count {
        assert!(
            Instant::now() < deadline,
            "calls never joined: {:?}",
            node.flight_stats()
        );
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

fn spawn_query(
    node: &Arc<Node>,
    name: &str,
) -> tokio::task::JoinHandle<Result<(Value, u64), NodeError>> {
    let (node, name) = (node.clone(), name.to_string());
    tokio::spawn(async move { query(&node, &name, Value::Null).await })
}

#[tokio::test(flavor = "multi_thread")]
async fn identical_queries_in_flight_join_one_evaluation() {
    const CALLS: usize = 8;
    let dir = tempfile::tempdir().unwrap();
    let probe = Arc::new(Probe::default());
    let _release = Release(probe.clone());
    // Two evaluation slots for eight identical calls and one other.
    let node = member(dir.path(), &probe, workers(2)).await;
    put(&node, "first").await;
    let hits = node.reader().query_cache_stats().hits;
    let calls: Vec<_> = (0..CALLS).map(|_| spawn_query(&node, "parked")).collect();
    probe.parked(1).await;
    joined(&node, CALLS as u64 - 1).await;
    // One evaluates; the others wait for it holding no evaluation slot,
    // so another query still finds one.
    assert_eq!(probe.parked.load(Ordering::SeqCst), 1);
    promptly("another query", query(&node, "get", Value::Null))
        .await
        .unwrap();
    probe.gate.open();
    let mut answers = BTreeSet::new();
    for call in calls {
        let (value, revision) = call.await.unwrap().unwrap();
        answers.insert((format!("{value:?}"), revision));
    }
    // One answer, one evaluation; every waiter answered from the cache
    // as any later call would.
    assert_eq!(answers.len(), 1, "{answers:?}");
    assert_eq!(probe.parked.load(Ordering::SeqCst), 1);
    assert_eq!(
        node.reader().query_cache_stats().hits - hits,
        CALLS as u64 - 1
    );
    let stats = node.flight_stats();
    assert_eq!(
        (stats.joined, stats.flights, stats.bytes),
        (CALLS as u64 - 1, 0, 0)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_waiter_answers_at_its_own_revision() {
    let dir = tempfile::tempdir().unwrap();
    let probe = Arc::new(Probe::default());
    let _release = Release(probe.clone());
    let node = member(dir.path(), &probe, workers(4)).await;
    put(&node, "first").await;
    let leader = spawn_query(&node, "parked");
    probe.parked(1).await;
    let waiter = spawn_query(&node, "parked");
    joined(&node, 1).await;
    // A write lands while both wait: the leader keeps the result of the
    // snapshot it pinned, which no longer holds for the waiter's.
    put(&node, "second").await;
    let (_, at) = query(&node, "get", Value::Null).await.unwrap();
    // A call proved after the write flies on its own.
    let after = spawn_query(&node, "parked");
    probe.parked(2).await;
    assert_eq!(node.flight_stats().joined, 1);
    probe.gate.open();
    let first = obj(vec![("title", s("first"))]);
    let second = obj(vec![("title", s("second"))]);
    assert_eq!(leader.await.unwrap().unwrap(), (first, at - 1));
    // The waiter evaluates on its own snapshot rather than take the
    // leader's answer: exactly what a fresh evaluation there returns.
    assert_eq!(waiter.await.unwrap().unwrap(), (second.clone(), at));
    assert_eq!(after.await.unwrap().unwrap(), (second, at));
}

#[tokio::test(flavor = "multi_thread")]
async fn flights_that_keep_nothing_release_their_waiters_and_go_solo() {
    const CALLS: usize = 4;
    let dir = tempfile::tempdir().unwrap();
    let probe = Arc::new(Probe::default());
    let _release = Release(probe.clone());
    let node = member(dir.path(), &probe, workers(CALLS)).await;
    put(&node, "first").await;
    // Reading the clock, the result never enters the cache: each waiter
    // evaluates once the leader lands.
    let calls: Vec<_> = (0..CALLS)
        .map(|_| spawn_query(&node, "parked_now"))
        .collect();
    probe.parked(1).await;
    joined(&node, CALLS as u64 - 1).await;
    probe.gate.open();
    for call in calls {
        assert_eq!(
            call.await.unwrap().unwrap().0,
            obj(vec![("title", s("first"))])
        );
    }
    assert_eq!(probe.parked.load(Ordering::SeqCst), CALLS);
    // Marked solo, later identical calls evaluate without waiting.
    let before = node.flight_stats();
    assert_eq!(before.solo, 1);
    for _ in 0..3 {
        query(&node, "parked_now", Value::Null).await.unwrap();
    }
    let after = node.flight_stats();
    assert_eq!((after.alone - before.alone, after.led - before.led), (3, 0));
    // Failures keep nothing either, and are never shared: every caller
    // evaluates and fails on its own.
    let failing = probe.parked.load(Ordering::SeqCst);
    let calls: Vec<_> = (0..CALLS)
        .map(|_| spawn_query(&node, "parked_fail"))
        .collect();
    for call in calls {
        match call.await.unwrap() {
            Err(NodeError::Call { code, .. }) => assert_eq!(code, "PARKED"),
            other => panic!("expected the guest's failure, got {other:?}"),
        }
    }
    assert_eq!(probe.parked.load(Ordering::SeqCst) - failing, CALLS);
}

#[tokio::test(flavor = "multi_thread")]
async fn calls_presented_differently_never_join() {
    let dir = tempfile::tempdir().unwrap();
    let probe = Arc::new(Probe::default());
    let _release = Release(probe.clone());
    let node = member(dir.path(), &probe, workers(4)).await;
    put(&node, "first").await;
    let as_principal = |subject: &str| {
        let node = node.clone();
        let options = CallOptions {
            principal: Some(flower_sdk::Principal::new(subject.to_string(), None, None).unwrap()),
            ..CallOptions::default()
        };
        tokio::spawn(async move {
            node.query_at(b"db", "app", "parked", &Value::Null, &options)
                .await
        })
    };
    let ada = as_principal("ada");
    let bob = as_principal("bob");
    // Both evaluate: neither waits for the other's flight.
    probe.parked(2).await;
    assert_eq!(node.flight_stats().joined, 0);
    probe.gate.open();
    ada.await.unwrap().unwrap();
    bob.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_waiter_is_refused_at_its_client_deadline() {
    let dir = tempfile::tempdir().unwrap();
    let probe = Arc::new(Probe::default());
    let _release = Release(probe.clone());
    let node = member(dir.path(), &probe, workers(4)).await;
    put(&node, "first").await;
    let leader = spawn_query(&node, "parked");
    probe.parked(1).await;
    // An identical call with a short budget joins the flight, and its
    // wait ends at its deadline as admission's would, not when the
    // leader lands.
    let started = Instant::now();
    let deadline = tokio::time::Instant::now() + Duration::from_millis(200);
    let waited = tokio::time::timeout(
        Duration::from_secs(5),
        flower_cluster::deadline::scoped(Some(deadline), query(&node, "parked", Value::Null)),
    )
    .await
    .expect("the waiter outlived its client deadline");
    assert!(
        matches!(waited, Err(NodeError::Overloaded { .. })),
        "{waited:?}"
    );
    assert!(started.elapsed() >= Duration::from_millis(200));
    assert_eq!(node.flight_stats().joined, 1);
    // The leader answers all the same.
    probe.gate.open();
    assert_eq!(
        leader.await.unwrap().unwrap().0,
        obj(vec![("title", s("first"))])
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_leader_refused_admission_sends_its_waiters_to_fly_again() {
    let dir = tempfile::tempdir().unwrap();
    let probe = Arc::new(Probe::default());
    let _release = Release(probe.clone());
    let config = AdmissionConfig {
        query_workers: 1,
        max_wait: Duration::from_millis(500),
        ..AdmissionConfig::default()
    };
    let node = member(dir.path(), &probe, config).await;
    put(&node, "first").await;
    // The only evaluation slot is taken.
    let parked = spawn_query(&node, "parked");
    probe.parked(1).await;
    let before = node.flight_stats();
    let leader = spawn_query(&node, "get");
    let deadline = Instant::now() + Duration::from_secs(10);
    while node.flight_stats().flights < before.flights + 1 {
        assert!(Instant::now() < deadline, "{:?}", node.flight_stats());
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    let waiter = spawn_query(&node, "get");
    joined(&node, before.joined + 1).await;
    // The leader never ran: refused at its wait bound, it marks nothing
    // solo and its waiter leads a flight of its own, which admission
    // refuses in turn.
    for call in [leader, waiter] {
        let refused = call.await.unwrap();
        assert!(
            matches!(refused, Err(NodeError::Overloaded { .. })),
            "{refused:?}"
        );
    }
    let after = node.flight_stats();
    assert_eq!(
        (
            after.led - before.led,
            after.joined - before.joined,
            after.alone - before.alone,
            after.solo
        ),
        (2, 1, 0, 0)
    );
    probe.gate.open();
    parked.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_leader_whose_caller_left_sends_its_waiters_to_fly_again() {
    let dir = tempfile::tempdir().unwrap();
    let probe = Arc::new(Probe::default());
    let _release = Release(probe.clone());
    let node = member(dir.path(), &probe, workers(4)).await;
    put(&node, "first").await;
    let before = node.flight_stats();
    let leader = spawn_query(&node, "parked");
    probe.parked(1).await;
    let waiter = spawn_query(&node, "parked");
    joined(&node, before.joined + 1).await;
    // The leader's caller goes away: its waiter is released and leads
    // a flight of its own, which evaluates.
    leader.abort();
    probe.parked(2).await;
    let after = node.flight_stats();
    assert_eq!(
        (
            after.led - before.led,
            after.joined - before.joined,
            after.solo
        ),
        (2, 1, 0)
    );
    probe.gate.open();
    assert_eq!(
        waiter.await.unwrap().unwrap().0,
        obj(vec![("title", s("first"))])
    );
}
