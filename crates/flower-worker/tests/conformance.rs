//! The in-memory queue against a live server: the same scenarios run
//! through `QueueClient` on both, and their transcripts must match, so
//! tests written against the fake hold against Flower.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use flower_client::FlowerClient;
use flower_client::client::RequestOptions;
use flower_client::error::FlowerError;
use flower_client::transport::Credentials;
use flower_client::work::QueueStats;
use flower_cluster::network::{InProcessFactory, Registry};
use flower_cluster::node::Node;
use flower_cluster::statemachine::RuntimeFactory;
use flower_sdk::Value;
use flower_server::api::{AppState, serve};
use flower_worker::testing::FakeQueue;
use flower_worker::{QueueClient, QueueWorkerOptions, WorkError, WorkerSummary, run_queue_worker};

const TOKEN: &str = "secret";

/// A live single node and a client for it.
struct Live {
    _dir: tempfile::TempDir,
    node: Arc<Node>,
    client: FlowerClient,
}

async fn live() -> Live {
    struct NoGuests;
    impl RuntimeFactory for NoGuests {
        fn build(
            &self,
            _db: &[u8],
            _module: &str,
            _wasm: &[u8],
            _manifest: &Value,
        ) -> Result<Box<dyn flower_engine::runtime::Runtime>, String> {
            Err("no guests".to_string())
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let node = Node::open(
        1,
        &dir.path().join("n1"),
        Arc::new(openraft::Config::default()),
        InProcessFactory::new(registry.clone()),
        Arc::new(NoGuests),
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
    let node = Arc::new(node);
    let state = AppState::new(node.clone()).with_admin_token(TOKEN.to_string());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(serve(listener, state));
    let client = FlowerClient::builder(&base)
        .credentials(Credentials::token(TOKEN))
        .build()
        .unwrap();
    for _ in 0..500 {
        if client.status(&RequestOptions::default()).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Live {
        _dir: dir,
        node,
        client,
    }
}

impl Live {
    async fn seed(&self, payload: &str, max_attempts: u32) {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let engine = self.node.engine().lock().await;
        engine
            .enqueue(
                b"db",
                "jobs",
                &Value::String(payload.to_string()),
                0,
                max_attempts,
                now_ms,
            )
            .unwrap();
    }

    /// Enqueue into a scope at a priority.
    async fn seed_in(&self, payload: &str, scope: &str, priority: i64) {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let payload = Value::String(payload.to_string());
        let engine = self.node.engine().lock().await;
        engine
            .enqueue_item(
                b"db",
                &flower_engine::work_queue::NewItem {
                    scope,
                    priority,
                    ..flower_engine::work_queue::NewItem::new("jobs", &payload, 0, 5)
                },
                now_ms,
            )
            .unwrap();
    }

    async fn stats(&self) -> QueueStats {
        self.client
            .queue_stats("db", "jobs", &RequestOptions::default())
            .await
            .unwrap()
    }
}

/// An outcome, spelled the same way for both backends.
fn spell<T: std::fmt::Debug>(result: &Result<T, FlowerError>) -> String {
    match result {
        Ok(value) => format!("ok {value:?}"),
        Err(FlowerError::Business { code, message, .. }) => format!("refused {code}: {message}"),
        Err(error) => format!("failed {error}"),
    }
}

/// Leases, tokens, attempts and death, call by call.
async fn lease_transcript<C: QueueClient>(client: &C) -> Vec<String> {
    let mut log = Vec::new();
    let lease = 300;
    let first = client.poll("db", "jobs", 10, lease).await.unwrap();
    log.push(format!(
        "claimed {:?}",
        first
            .iter()
            .map(|claim| (claim.payload.clone(), claim.attempts))
            .collect::<Vec<_>>()
    ));
    let (id, old) = (first[0].id.clone(), first[0].token.clone());
    log.push(format!(
        "again {}",
        client.poll("db", "jobs", 10, lease).await.unwrap().len()
    ));
    log.push(spell(
        &client.heartbeat("db", "jobs", &id, "nope", lease).await,
    ));
    log.push(spell(
        &client.heartbeat("db", "jobs", &id, &old, lease).await,
    ));
    // Let the lease lapse unrenewed: the item redelivers.
    tokio::time::sleep(Duration::from_millis(700)).await;
    let second = client.poll("db", "jobs", 10, lease).await.unwrap();
    log.push(format!("redelivered attempts {}", second[0].attempts));
    let new = second[0].token.clone();
    log.push(spell(
        &client.heartbeat("db", "jobs", &id, &old, lease).await,
    ));
    log.push(spell(&client.complete("db", "jobs", &id, &old).await));
    log.push(spell(&client.release("db", "jobs", &id, &new).await));
    let third = client.poll("db", "jobs", 10, lease).await.unwrap();
    log.push(format!("released attempts {}", third[0].attempts));
    let last = third[0].token.clone();
    log.push(spell(&client.fail("db", "jobs", &id, &last, 0).await));
    log.push(format!(
        "after death {}",
        client.poll("db", "jobs", 10, lease).await.unwrap().len()
    ));
    log.push(spell(&client.kill("db", "jobs", &id, &last).await));
    log.push(spell(
        &client
            .poll("db", "jobs", 10, 0)
            .await
            .map(|claims| claims.len()),
    ));
    log
}

/// A worker over three items: one completes, one retries to death,
/// one is killed.
async fn worker_outcome<C: QueueClient>(client: C) -> WorkerSummary {
    let stop = Arc::new(tokio::sync::Notify::new());
    let worker_stop = stop.clone();
    let run = tokio::spawn(run_queue_worker(
        client,
        QueueWorkerOptions::new("db", "jobs")
            .concurrency(2)
            .poll_interval(Duration::from_millis(20)),
        worker_stop,
        |claim| async move {
            match claim.payload.as_str() {
                Some("retry") => Err(WorkError::Retry(Some(Duration::from_millis(10)))),
                Some("kill") => Err(WorkError::Terminal("no".to_string())),
                _ => Ok(()),
            }
        },
    ));
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    stop.notify_one();
    run.await.unwrap().unwrap()
}

#[tokio::test]
async fn the_fake_queue_leases_like_the_server() {
    let server = live().await;
    server.seed("x", 3).await;
    let fake = FakeQueue::new();
    fake.enqueue("db", "jobs", serde_json::json!("x"), 3);
    let from_server = lease_transcript(&server.client).await;
    let from_fake = lease_transcript(&fake).await;
    assert_eq!(from_fake, from_server);
    assert_eq!(fake.stats("db", "jobs"), server.stats().await);
    assert_eq!(server.stats().await.dead, 1);
}

#[tokio::test]
async fn the_fake_queue_settles_worker_outcomes_like_the_server() {
    let server = live().await;
    let fake = FakeQueue::new();
    for (payload, attempts) in [("ok", 5), ("retry", 2), ("kill", 5)] {
        server.seed(payload, attempts).await;
        fake.enqueue("db", "jobs", serde_json::json!(payload), attempts);
    }
    let from_server = worker_outcome(server.client.clone()).await;
    let from_fake = worker_outcome(fake.clone()).await;
    assert_eq!(
        from_server,
        WorkerSummary {
            completed: 1,
            failed: 2,
            killed: 1,
            lost: 0,
            drained: 0
        }
    );
    assert_eq!(from_fake, from_server);
    assert_eq!(fake.stats("db", "jobs"), server.stats().await);
}

/// Items of two scopes at several priorities.
const SCOPED: [(&str, &str, i64); 6] = [
    ("a-low", "a", 5),
    ("a-urgent", "a", -2),
    ("b-normal", "b", 0),
    ("a-normal", "a", 0),
    ("b-urgent", "b", -1),
    ("default", "", 0),
];

/// Claim order, scopes and fencing tokens, call by call.
async fn scope_transcript<C: QueueClient>(client: &C) -> Vec<String> {
    let mut log = Vec::new();
    for (scope, limit) in [("a", 2), ("b", 10), ("a", 10), ("", 10), ("c", 10)] {
        let claims = client
            .poll_scope("db", "jobs", scope, limit, 60_000)
            .await
            .unwrap();
        log.push(format!(
            "{scope:?}: {:?}",
            claims
                .iter()
                .map(|claim| (
                    claim.payload.clone(),
                    claim.scope.clone(),
                    claim.priority,
                    claim.fence
                ))
                .collect::<Vec<_>>()
        ));
    }
    log
}

#[tokio::test]
async fn the_fake_queue_orders_scopes_and_fences_like_the_server() {
    let server = live().await;
    let fake = FakeQueue::new();
    for (payload, scope, priority) in SCOPED {
        server.seed_in(payload, scope, priority).await;
        fake.enqueue_in(
            "db",
            "jobs",
            scope,
            priority,
            serde_json::json!(payload),
            Duration::ZERO,
            5,
        );
        tokio::time::sleep(Duration::from_millis(3)).await;
    }
    let from_server = scope_transcript(&server.client).await;
    assert_eq!(
        from_server[0],
        r#""a": [(String("a-urgent"), "a", -2, 1), (String("a-normal"), "a", 0, 2)]"#
    );
    assert_eq!(
        from_server[1],
        r#""b": [(String("b-urgent"), "b", -1, 1), (String("b-normal"), "b", 0, 2)]"#
    );
    assert_eq!(from_server[2], r#""a": [(String("a-low"), "a", 5, 3)]"#);
    assert_eq!(from_server[4], r#""c": []"#);
    assert_eq!(scope_transcript(&fake).await, from_server);
    let scoped = server
        .client
        .queue_stats_in("db", "jobs", Some("a"), &RequestOptions::default())
        .await
        .unwrap();
    assert_eq!((scoped.pending, scoped.claimed), (0, 3));
}

#[tokio::test]
async fn a_scoped_worker_runs_only_its_scope() {
    let server = live().await;
    for (payload, scope, priority) in SCOPED {
        server.seed_in(payload, scope, priority).await;
    }
    let done = Arc::new(std::sync::Mutex::new(Vec::new()));
    let seen = done.clone();
    let stop = Arc::new(tokio::sync::Notify::new());
    let run = tokio::spawn(run_queue_worker(
        server.client.clone(),
        QueueWorkerOptions::new("db", "jobs")
            .scope("b")
            .concurrency(1)
            .poll_interval(Duration::from_millis(20)),
        stop.clone(),
        move |claim| {
            let seen = seen.clone();
            async move {
                seen.lock().unwrap().push((claim.payload, claim.fence));
                Ok(())
            }
        },
    ));
    for _ in 0..200 {
        if done.lock().unwrap().len() == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    stop.notify_one();
    let summary = run.await.unwrap().unwrap();
    assert_eq!(summary.completed, 2);
    assert_eq!(
        *done.lock().unwrap(),
        [
            (serde_json::json!("b-urgent"), 1),
            (serde_json::json!("b-normal"), 2)
        ]
    );
    let stats = server.stats().await;
    assert_eq!((stats.pending, stats.claimed), (4, 0));
}

#[tokio::test]
async fn workers_keep_results_find_named_jobs_and_wait_in_line_over_http() {
    let server = live().await;
    let options = RequestOptions::default();
    let client = &server.client;
    // Queues exist once a deployment or an enqueue interns them.
    server.seed_in("elsewhere", "other", 0).await;
    // Two workers wait in the default scope's line, w1 first.
    for owner in ["w1", "w2"] {
        let poll = flower_client::work::QueuePoll {
            owner: Some(owner),
            wait_ms: Some(60_000),
            limit: Some(1),
            ..Default::default()
        };
        assert!(
            client
                .claim_queue("db", "jobs", &poll, &options)
                .await
                .unwrap()
                .is_empty()
        );
        tokio::time::sleep(Duration::from_millis(3)).await;
    }
    {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let payload = Value::String("invoice".to_string());
        let item = flower_engine::work_queue::NewItem {
            name: Some("inv-9"),
            group: Some("acme"),
            ..flower_engine::work_queue::NewItem::new("jobs", &payload, 0, 5)
        };
        let engine = server.node.engine().lock().await;
        engine.enqueue_item(b"db", &item, now_ms).unwrap();
    }
    // New work is w1's for the turn; w2 waits.
    let first = client
        .queue_readiness("db", "jobs", "", Some("w1"), &options)
        .await
        .unwrap();
    assert!(first.ready);
    let second = client
        .queue_readiness("db", "jobs", "", Some("w2"), &options)
        .await
        .unwrap();
    assert!(!second.ready);
    assert!(second.next_ms.is_some());
    // w1 claims the named job and keeps a result with it.
    let poll = flower_client::work::QueuePoll {
        owner: Some("w1"),
        limit: Some(1),
        ..Default::default()
    };
    let claim = client
        .claim_queue("db", "jobs", &poll, &options)
        .await
        .unwrap();
    assert_eq!(claim[0].name.as_deref(), Some("inv-9"));
    assert_eq!(claim[0].group.as_deref(), Some("acme"));
    client
        .complete_queue_with(
            "db",
            "jobs",
            &claim[0].id,
            &claim[0].token,
            &serde_json::json!({"paid": true}),
            &options,
        )
        .await
        .unwrap();
    let job = client
        .get_queue_job("db", "jobs", "", "inv-9", &options)
        .await
        .unwrap()
        .expect("the named job is kept");
    assert_eq!(job.state, "completed");
    assert_eq!(job.result, serde_json::json!({"paid": true}));
    assert_eq!(job.id, claim[0].id);
    assert_eq!(server.stats().await.completed, 1);
}
