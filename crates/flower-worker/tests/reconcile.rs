//! Reconcile against the external-transactions example guest, compiled
//! to wasm32 and deployed to a live node: one key kept current, pools
//! drained, stale results rejected, failed computations retried.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use flower_client::FlowerClient;
use flower_client::client::{MutationOptions, RequestOptions};
use flower_client::error::FlowerError;
use flower_client::transport::Credentials;
use flower_cluster::network::{InProcessFactory, Registry};
use flower_cluster::node::Node;
use flower_cluster::wasm::WasmFactory;
use flower_engine::runtime::wasm::WasmBackend;
use flower_worker::{ExternalWork, ReconcileEvent, ReconcileOptions, ReconcileSummary, reconcile};
use serde_json::{Value, json};
use tokio::sync::Notify;

/// The workspace root.
fn root() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf()
}

/// Build the example for wasm32 in release, as the CLI tests build
/// theirs, and return its bytes.
fn example_wasm() -> Vec<u8> {
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
    let target = root().join("target").join("wasm-guest");
    let status = std::process::Command::new(cargo)
        .args([
            "build",
            "--offline",
            "--quiet",
            "--release",
            "--target",
            "wasm32-unknown-unknown",
            "-p",
            "external-transactions",
        ])
        .current_dir(root())
        .env("CARGO_TARGET_DIR", &target)
        .status()
        .unwrap();
    assert!(status.success(), "example build failed");
    std::fs::read(
        target
            .join("wasm32-unknown-unknown")
            .join("release")
            .join("external_transactions.wasm"),
    )
    .unwrap()
}

/// The manifest the guest declares, read through wasmtime.
fn manifest(wasm: &[u8]) -> Value {
    let mut backend = WasmBackend::new().unwrap();
    backend.register("app", wasm).unwrap();
    let manifest = backend.manifest("app").unwrap();
    serde_json::from_slice(&flower_engine::json::render(&manifest).unwrap()).unwrap()
}

/// A live node with the example deployed as `app` in `db`.
struct App {
    _dir: tempfile::TempDir,
    client: FlowerClient,
}

async fn deployed() -> App {
    let wasm = tokio::task::spawn_blocking(example_wasm).await.unwrap();
    let manifest = manifest(&wasm);
    let dir = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let node = Node::open(
        1,
        &dir.path().join("n1"),
        Arc::new(openraft::Config::default()),
        InProcessFactory::new(registry.clone()),
        Arc::new(WasmFactory::default()),
    )
    .await
    .unwrap();
    registry.register(1, node.raft().clone());
    node.initialize(BTreeSet::from([1])).await.unwrap();
    for _ in 0..500 {
        if node.status().leader == Some(1) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let state =
        flower_server::api::AppState::new(Arc::new(node)).with_admin_token("secret".to_string());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(flower_server::api::serve(listener, state));
    let client = FlowerClient::builder(&base)
        .credentials(Credentials::token("secret"))
        .build()
        .unwrap();
    for _ in 0..500 {
        if client.status(&RequestOptions::default()).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    client
        .deploy(
            "db",
            "app",
            None,
            &wasm,
            &manifest,
            &RequestOptions::default().timeout(Duration::from_secs(60)),
        )
        .await
        .unwrap();
    App { _dir: dir, client }
}

impl App {
    async fn put(&self, id: &str, text: &str) {
        self.client
            .mutate_value(
                "db",
                "app",
                "put",
                &json!({"id": id, "text": text}),
                &MutationOptions::default(),
            )
            .await
            .unwrap();
    }

    async fn query(&self, name: &str, args: Value) -> Value {
        self.client
            .query_value("db", "app", name, &args, &RequestOptions::default())
            .await
            .unwrap()
            .value
    }

    /// Wait until a query answers `expected`.
    async fn eventually(&self, name: &str, args: Value, expected: Value) {
        for _ in 0..500 {
            if self.query(name, args.clone()).await == expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!(
            "{name}({args}) never became {expected}: {}",
            self.query(name, args.clone()).await
        );
    }
}

/// Count the words of a `words` input.
async fn count_words(work: ExternalWork) -> Result<Value, String> {
    let text = work.input["text"].as_str().unwrap_or_default().to_string();
    Ok(json!({"count": text.split_whitespace().count()}))
}

type Running = tokio::task::JoinHandle<Result<ReconcileSummary, FlowerError>>;

fn spawn<F, Fut>(app: &App, options: ReconcileOptions, compute: F) -> (Arc<Notify>, Running)
where
    F: Fn(ExternalWork) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<Value, String>> + Send + 'static,
{
    let stop = Arc::new(Notify::new());
    let run = tokio::spawn(reconcile(
        app.client.clone(),
        options.poll_interval(Duration::from_millis(50)),
        stop.clone(),
        compute,
    ));
    (stop, run)
}

#[tokio::test(flavor = "multi_thread")]
async fn one_key_stays_current_as_its_input_changes() {
    let app = deployed().await;
    // Nothing to compute without text.
    assert_eq!(app.query("readWords", json!("a")).await, Value::Null);
    assert_eq!(app.query("words.pending", json!("a")).await, Value::Null);
    let options = ReconcileOptions::new("db", "app", "words").args(json!("a"));
    let (stop, run) = spawn(&app, options, count_words);
    app.put("a", "the quick brown fox").await;
    app.eventually(
        "readWords",
        json!("a"),
        json!({"status": "ready", "value": {"count": 4}}),
    )
    .await;
    assert_eq!(app.query("words.pending", json!("a")).await, Value::Null);
    app.put("a", "two words").await;
    app.eventually(
        "readWords",
        json!("a"),
        json!({"status": "ready", "value": {"count": 2}}),
    )
    .await;
    stop.notify_one();
    let summary = run.await.unwrap().unwrap();
    assert_eq!(summary.published, 2);
    assert_eq!(summary.failed, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn results_for_a_changed_input_are_rejected_and_recomputed() {
    let app = deployed().await;
    app.put("a", "one two three").await;
    let pending = app.query("words.pending", json!("a")).await;
    assert_eq!(pending["input"], json!({"text": "one two three"}));
    // A result for an outdated key is refused, never stored.
    let stale = app
        .client
        .mutate_value(
            "db",
            "app",
            "words.publish",
            &json!({"args": "a", "key": "[\"a\",{\"text\":\"older\"}]", "value": {"count": 1}}),
            &MutationOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(stale.value, json!(false));
    assert_eq!(
        app.query("readWords", json!("a")).await,
        json!({"status": "pending"})
    );
    // The worker computes, the input changes under it, its publication
    // is rejected, and it computes again.
    let gate = Arc::new(Notify::new());
    let started = Arc::new(Notify::new());
    let calls = Arc::new(AtomicUsize::new(0));
    let (held, begun, count) = (gate.clone(), started.clone(), calls.clone());
    let options = ReconcileOptions::new("db", "app", "words").args(json!("a"));
    let (stop, run) = spawn(&app, options, move |work| {
        let (held, begun, count) = (held.clone(), begun.clone(), count.clone());
        async move {
            if count.fetch_add(1, Ordering::SeqCst) == 0 {
                begun.notify_one();
                held.notified().await;
            }
            count_words(work).await
        }
    });
    started.notified().await;
    app.put("a", "now just four words").await;
    gate.notify_one();
    app.eventually(
        "readWords",
        json!("a"),
        json!({"status": "ready", "value": {"count": 4}}),
    )
    .await;
    stop.notify_one();
    let summary = run.await.unwrap().unwrap();
    assert_eq!(summary.rejected, 1);
    assert_eq!(summary.published, 1);
    // The value contract holds: a negative count is refused.
    let refused = app
        .client
        .mutate_value(
            "db",
            "app",
            "words.publish",
            &json!({"args": "a", "key": "[\"a\",{\"text\":\"now just four words\"}]", "value": {"count": -1}}),
            &MutationOptions::default(),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(refused, FlowerError::Business { ref code, .. } if code == "INVALID_VALUE"),
        "{refused:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_pool_drains_tracked_rows_and_follows_removals() {
    let app = deployed().await;
    for id in ["a", "b", "c", "d", "e"] {
        app.put(id, &format!("text of {id}")).await;
    }
    let next = app.query("tracked.next", json!({"limit": 10})).await;
    assert_eq!(next.as_array().unwrap().len(), 5);
    // The first computation of `c` fails and retries after its backoff.
    let failed_once = Arc::new(AtomicUsize::new(0));
    let failed = failed_once.clone();
    let options = ReconcileOptions::new("db", "app", "tracked")
        .concurrency(2)
        .batch(3)
        .retry(Duration::from_millis(50), Duration::from_millis(200));
    let (stop, run) = spawn(&app, options, move |work| {
        let failed = failed.clone();
        async move {
            if work.args == "c" && failed.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err("flaky".to_string());
            }
            Ok(json!(work.input.as_str().unwrap().to_uppercase()))
        }
    });
    for id in ["a", "b", "c", "d", "e"] {
        app.eventually(
            "readTracked",
            json!(id),
            json!({"status": "ready", "value": format!("TEXT OF {}", id.to_uppercase())}),
        )
        .await;
    }
    assert_eq!(app.query("tracked.next", json!({})).await, json!([]));
    // A change makes it stale again; the running pool picks it up.
    app.put("b", "changed").await;
    app.eventually(
        "readTracked",
        json!("b"),
        json!({"status": "ready", "value": "CHANGED"}),
    )
    .await;
    // A removed row takes its result with it.
    app.client
        .mutate_value(
            "db",
            "app",
            "remove",
            &json!("e"),
            &MutationOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(app.query("readTracked", json!("e")).await, Value::Null);
    stop.notify_one();
    let summary = run.await.unwrap().unwrap();
    assert_eq!(summary.failed, 1);
    assert_eq!(summary.published, 6);
    // Outside pools, `next` refuses: the call's own failure.
    let error = app
        .client
        .query_value(
            "db",
            "app",
            "words.next",
            &json!({}),
            &RequestOptions::default(),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(error, FlowerError::Business { ref code, .. } if code == "INVALID_INVOCATION")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unknown_external_value_stops_reconciling() {
    let app = deployed().await;
    let options = ReconcileOptions::new("db", "app", "nothing");
    let (_stop, run) = spawn(&app, options, count_words);
    let error = run.await.unwrap().unwrap_err();
    assert!(matches!(error, FlowerError::Http(_)), "{error:?}");
}

type Computation =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send>>;

/// Keys computing now, across every worker of a test: a second start
/// of one key while it computes is a broken lease.
#[derive(Clone, Default)]
struct InFlight {
    running: Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
    overlaps: Arc<AtomicUsize>,
    computed: Arc<std::sync::Mutex<Vec<(String, String)>>>,
}

impl InFlight {
    /// Compute uppercase text as `worker`, recording what overlapped.
    fn compute(
        &self,
        worker: &'static str,
        delay: Duration,
    ) -> impl Fn(ExternalWork) -> Computation + Send + Sync + 'static {
        let flight = self.clone();
        move |work: ExternalWork| {
            let flight = flight.clone();
            Box::pin(async move {
                let args = work.args.as_str().unwrap().to_string();
                if !flight.running.lock().unwrap().insert(args.clone()) {
                    flight.overlaps.fetch_add(1, Ordering::SeqCst);
                }
                tokio::time::sleep(delay).await;
                flight.running.lock().unwrap().remove(&args);
                flight
                    .computed
                    .lock()
                    .unwrap()
                    .push((worker.to_string(), args));
                Ok(json!(work.input.as_str().unwrap().to_uppercase()))
            })
        }
    }

    fn computed_by(&self, worker: &str) -> Vec<String> {
        self.computed
            .lock()
            .unwrap()
            .iter()
            .filter(|(by, _)| by == worker)
            .map(|(_, args)| args.clone())
            .collect()
    }
}

fn leased(owner: &str) -> ReconcileOptions {
    ReconcileOptions::new("db", "app", "tracked")
        .leased()
        .owner(owner)
        .lease(Duration::from_millis(1_500), Duration::from_millis(300))
        .batch(2)
        .retry(Duration::from_millis(50), Duration::from_millis(200))
}

#[tokio::test(flavor = "multi_thread")]
async fn leased_pools_spread_keys_across_workers_one_at_a_time() {
    let app = deployed().await;
    let ids: Vec<String> = (0..8).map(|index| format!("k{index}")).collect();
    for id in &ids {
        app.put(id, &format!("text of {id}")).await;
    }
    let flight = InFlight::default();
    let (stop_a, run_a) = spawn(
        &app,
        leased("worker-a").concurrency(2),
        flight.compute("a", Duration::from_millis(60)),
    );
    let (stop_b, run_b) = spawn(
        &app,
        leased("worker-b").adaptive(1, 1, 4),
        flight.compute("b", Duration::from_millis(60)),
    );
    for id in &ids {
        app.eventually(
            "readTracked",
            json!(id),
            json!({"status": "ready", "value": format!("TEXT OF {}", id.to_uppercase())}),
        )
        .await;
    }
    // A change while the workers wait is claimed through the readiness
    // watch and computed again.
    app.put("k3", "changed").await;
    app.eventually(
        "readTracked",
        json!("k3"),
        json!({"status": "ready", "value": "CHANGED"}),
    )
    .await;
    stop_a.notify_one();
    stop_b.notify_one();
    let (a, b) = (run_a.await.unwrap().unwrap(), run_b.await.unwrap().unwrap());
    assert_eq!(
        flight.overlaps.load(Ordering::SeqCst),
        0,
        "a key computed twice at once"
    );
    assert_eq!(a.published + b.published, 9);
    assert_eq!(a.failed + b.failed, 0);
    let mut computed: Vec<String> = flight.computed_by("a");
    computed.extend(flight.computed_by("b"));
    computed.sort();
    let mut expected: Vec<String> = ids.clone();
    expected.push("k3".to_string());
    expected.sort();
    assert_eq!(computed, expected, "every key once, k3 twice");
    assert_eq!(
        app.query("tracked.stats", Value::Null).await["ready"],
        json!(false)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_leased_pool_reclaims_a_lapsed_lease_and_backs_off_failures() {
    let app = deployed().await;
    app.put("crashed", "left behind").await;
    app.put("flaky", "fails once").await;
    // A worker claims `crashed` and dies holding it.
    let claimed = app
        .client
        .mutate_value(
            "db",
            "app",
            "tracked.claim",
            &json!({"owner": "dead-worker", "limit": 1, "leaseMs": 800}),
            &MutationOptions::default(),
        )
        .await
        .unwrap()
        .value;
    assert_eq!(claimed[0]["args"], json!("crashed"));
    assert_eq!(claimed[0]["attempt"], json!(1));
    let started = std::time::Instant::now();
    let flight = InFlight::default();
    let failed = Arc::new(AtomicUsize::new(0));
    let fails = failed.clone();
    let compute = flight.compute("w", Duration::from_millis(10));
    let events = Arc::new(std::sync::Mutex::new(Vec::new()));
    let seen = events.clone();
    let options = leased("worker-w").on_event(move |event| seen.lock().unwrap().push(event));
    let (stop, run) = spawn(&app, options, move |work: ExternalWork| {
        let fails = fails.clone();
        let flaky = work.args == "flaky";
        let computed = compute(work);
        async move {
            if flaky && fails.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err("flaky".to_string());
            }
            computed.await
        }
    });
    app.eventually(
        "readTracked",
        json!("flaky"),
        json!({"status": "ready", "value": "FAILS ONCE"}),
    )
    .await;
    // `crashed` waits out the dead worker's lease, then is reclaimed.
    app.eventually(
        "readTracked",
        json!("crashed"),
        json!({"status": "ready", "value": "LEFT BEHIND"}),
    )
    .await;
    assert!(started.elapsed() >= Duration::from_millis(700));
    stop.notify_one();
    let summary = run.await.unwrap().unwrap();
    assert_eq!(summary.failed, 1);
    assert_eq!(summary.published, 2);
    assert_eq!(flight.computed_by("w"), ["flaky", "crashed"]);
    // Attempts count claims across processes: the dead worker's was the first.
    let events = events.lock().unwrap().clone();
    let attempts: Vec<u64> = events
        .iter()
        .filter_map(|event| match event {
            ReconcileEvent::Claimed { key, attempt } if key.starts_with("[\"crashed\"") => {
                Some(*attempt)
            }
            _ => None,
        })
        .collect();
    assert_eq!(attempts, [2]);
    assert!(
        events
            .iter()
            .any(|event| matches!(event, ReconcileEvent::Failed { error, .. } if error == "flaky"))
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, ReconcileEvent::Published { accepted: true, .. }))
            .count(),
        2
    );
    // The dead worker's lease is gone: it can no longer renew.
    let lease = json!({
        "args": "crashed",
        "key": claimed[0]["key"],
        "owner": "dead-worker",
        "attempt": 1,
    });
    let renewed = app
        .client
        .mutate_value(
            "db",
            "app",
            "tracked.renew",
            &json!({"leases": [lease]}),
            &MutationOptions::default(),
        )
        .await
        .unwrap()
        .value;
    assert_eq!(renewed, json!([null]));
}

#[tokio::test(flavor = "multi_thread")]
async fn shards_split_an_unleased_pool() {
    let app = deployed().await;
    let ids: Vec<String> = (0..10).map(|index| format!("s{index}")).collect();
    for id in &ids {
        app.put(id, id).await;
    }
    let flight = InFlight::default();
    let shard = |index: u32| {
        ReconcileOptions::new("db", "app", "tracked")
            .shard(index, 2)
            .concurrency(2)
    };
    let (stop_0, run_0) = spawn(&app, shard(0), flight.compute("0", Duration::ZERO));
    let (stop_1, run_1) = spawn(&app, shard(1), flight.compute("1", Duration::ZERO));
    for id in &ids {
        app.eventually(
            "readTracked",
            json!(id),
            json!({"status": "ready", "value": id.to_uppercase()}),
        )
        .await;
    }
    stop_0.notify_one();
    stop_1.notify_one();
    run_0.await.unwrap().unwrap();
    run_1.await.unwrap().unwrap();
    for (worker, index) in [("0", 0), ("1", 1)] {
        for id in flight.computed_by(worker) {
            let row = serde_json::to_string(&id).unwrap();
            assert_eq!(
                flower_sdk::external::shard_of(&row, 2),
                index,
                "{id} computed outside its shard"
            );
        }
    }
    assert_eq!(
        flight.computed_by("0").len() + flight.computed_by("1").len(),
        ids.len()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn typed_reconcile_decodes_work_and_encodes_results() {
    let app = deployed().await;
    app.put("t", "typed text").await;
    let stop = Arc::new(Notify::new());
    let options =
        ReconcileOptions::new("db", "app", "tracked").poll_interval(Duration::from_millis(50));
    let run = tokio::spawn(flower_worker::reconcile::reconcile_typed(
        app.client.clone(),
        options,
        stop.clone(),
        |work: flower_sdk::external::Work<String, String>| async move {
            Ok::<_, String>(format!("{}:{}", work.args, work.input.len()))
        },
    ));
    app.eventually(
        "readTracked",
        json!("t"),
        json!({"status": "ready", "value": "t:10"}),
    )
    .await;
    stop.notify_one();
    assert_eq!(run.await.unwrap().unwrap().published, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn lease_options_are_checked_before_any_call() {
    let app = deployed().await;
    for options in [
        leased("w").args(json!("a")),
        leased("w").shard(0, 2),
        ReconcileOptions::new("db", "app", "tracked").adaptive(1, 1, 2),
        leased("w").lease(Duration::from_millis(100), Duration::from_millis(100)),
        ReconcileOptions::new("db", "app", "tracked").shard(2, 2),
    ] {
        let (_stop, run) = spawn(&app, options, count_words);
        let error = run.await.unwrap().unwrap_err();
        assert!(
            matches!(error, FlowerError::Server { ref code, .. } if code == "invalid_options"),
            "{error:?}"
        );
    }
}

/// Uppercase a tracked document's text.
async fn shout(work: ExternalWork) -> Result<Value, String> {
    Ok(json!(
        work.input.as_str().unwrap_or_default().to_uppercase()
    ))
}

#[tokio::test(flavor = "multi_thread")]
async fn an_adaptive_leased_pool_says_its_load() {
    let app = deployed().await;
    app.put("busy", "some text").await;
    let events = Arc::new(std::sync::Mutex::new(Vec::new()));
    let seen = events.clone();
    let health: Arc<dyn flower_worker::Health> =
        Arc::new(|| flower_worker::Load::new(0.7, "the test says so"));
    let options = leased("worker-l")
        .adaptive(1, 1, 4)
        .health(health)
        .adjust_every(Duration::from_millis(20))
        .on_event(move |event| seen.lock().unwrap().push(event));
    let (stop, run) = spawn(&app, options, shout);
    let mut said = None;
    for _ in 0..500 {
        said = events.lock().unwrap().iter().find_map(|event| match event {
            ReconcileEvent::Load(load) => Some(load.clone()),
            _ => None,
        });
        if said.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    stop.notify_one();
    run.await.unwrap().unwrap();
    let said = said.expect("a load event");
    assert_eq!(said.load, 0.7);
    assert_eq!(said.reason, "the test says so");
    assert_eq!((said.min, said.max), (1, 4));
}
