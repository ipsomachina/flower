//! Worker integration: claim, renew and report seeded items
//! against a live single node.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use flower_client::client::RequestOptions;
use flower_client::error::FlowerError;
use flower_client::transport::Credentials;
use flower_client::{FlowerClient, QueuePoll, QueueReport};
use flower_cluster::network::{InProcessFactory, Registry};
use flower_cluster::node::Node;
use flower_cluster::statemachine::RuntimeFactory;
use flower_sdk::Value;
use flower_server::api::{AppState, serve};
use flower_worker::{QueueWorkerOptions, WorkError, run_queue_worker};

const TOKEN: &str = "secret";

struct Served {
    _dir: tempfile::TempDir,
    base: String,
    node: Arc<Node>,
}

/// Serve an initialized single node. No module deploys: tests seed
/// the leader's engine directly.
async fn serve_node() -> Served {
    struct StubFactory;
    impl RuntimeFactory for StubFactory {
        fn build(
            &self,
            _db: &[u8],
            _module: &str,
            _wasm: &[u8],
            _manifest: &Value,
        ) -> Result<Box<dyn flower_engine::runtime::Runtime>, String> {
            Err("no guests in worker tests".to_string())
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let node = Node::open(
        1,
        &dir.path().join("n1"),
        Arc::new(openraft::Config::default()),
        InProcessFactory::new(registry.clone()),
        Arc::new(StubFactory),
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
    assert_eq!(node.status().leader, Some(1));
    let node = Arc::new(node);
    // Queue serving is admin-gated; tests carry the admin token.
    let state = AppState::new(node.clone()).with_admin_token(TOKEN.to_string());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(serve(listener, state));
    let probe = FlowerClient::builder(&base).build().unwrap();
    for _ in 0..500 {
        if probe.status(&RequestOptions::default()).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Served {
        _dir: dir,
        base,
        node,
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or(0)
}

async fn seed(server: &Served, queue: &str, payloads: &[&str], max_attempts: u32) {
    let engine = server.node.engine().lock().await;
    for payload in payloads {
        engine
            .enqueue(
                b"db",
                queue,
                &Value::String(payload.to_string()),
                0,
                max_attempts,
                now_ms(),
            )
            .unwrap();
    }
}

fn client(server: &Served) -> FlowerClient {
    FlowerClient::builder(&server.base)
        .credentials(Credentials::token(TOKEN))
        .build()
        .unwrap()
}

#[tokio::test]
async fn worker_completes_seeded_items() {
    let server = serve_node().await;
    seed(&server, "jobs", &["a", "b", "c"], 5).await;
    let seen = Arc::new(Mutex::new(Vec::new()));
    let done = Arc::new(tokio::sync::Notify::new());
    let stop = Arc::new(tokio::sync::Notify::new());
    let worker_seen = seen.clone();
    let worker_done = done.clone();
    let worker_stop = stop.clone();
    let worker_client = client(&server);
    let run = tokio::spawn(async move {
        run_queue_worker(
            worker_client,
            QueueWorkerOptions::new("db", "jobs")
                .concurrency(2)
                .lease(Duration::from_secs(30))
                .poll_interval(Duration::from_millis(20)),
            worker_stop,
            move |claim| {
                let seen = worker_seen.clone();
                let done = worker_done.clone();
                async move {
                    seen.lock().unwrap().push(claim.payload.clone());
                    if seen.lock().unwrap().len() >= 3 {
                        done.notify_one();
                    }
                    Ok(())
                }
            },
        )
        .await
        .unwrap()
    });
    tokio::time::timeout(Duration::from_secs(20), done.notified())
        .await
        .unwrap();
    stop.notify_one();
    let summary = tokio::time::timeout(Duration::from_secs(20), run)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(summary.completed, 3);
    assert_eq!(summary.failed, 0);
    let mut seen = seen.lock().unwrap().clone();
    seen.sort_by_key(|value| value.to_string());
    assert_eq!(
        seen,
        vec![
            serde_json::json!("a"),
            serde_json::json!("b"),
            serde_json::json!("c")
        ]
    );
    let stats = client(&server)
        .queue_stats("db", "jobs", &RequestOptions::default())
        .await
        .unwrap();
    assert_eq!(stats.pending, 0);
    assert_eq!(stats.claimed, 0);
}

#[tokio::test]
async fn chained_reports_claim_over_the_state_the_report_leaves() {
    let server = serve_node().await;
    seed(&server, "jobs", &["a", "b", "c"], 5).await;
    let client = client(&server);
    let options = RequestOptions::default();
    let poll = |limit| QueuePoll {
        scope: "",
        owner: Some("w"),
        limit: Some(limit),
        lease_ms: Some(30_000),
        wait_ms: None,
    };
    let stats = || async {
        let stats = client.queue_stats("db", "jobs", &options).await.unwrap();
        (stats.pending, stats.claimed, stats.dead)
    };
    let first = client
        .claim_queue("db", "jobs", &poll(1), &options)
        .await
        .unwrap()
        .remove(0);
    // Completing it claims the other two in the same commit.
    let complete = QueueReport::Complete(None);
    let next = client
        .report_queue(
            "db",
            "jobs",
            &first.id,
            &first.token,
            complete,
            Some(&poll(2)),
            &options,
        )
        .await
        .unwrap();
    assert_eq!(next.len(), 2);
    assert_eq!(stats().await, (0, 2, 0));
    // The claim sees the report: an item failed with no delay is
    // claimable at once, under a new token and fence.
    let (failing, other) = (&next[0], &next[1]);
    let again = client
        .report_queue(
            "db",
            "jobs",
            &failing.id,
            &failing.token,
            QueueReport::Fail(0),
            Some(&poll(1)),
            &options,
        )
        .await
        .unwrap();
    assert_eq!(again.len(), 1);
    assert_eq!(again[0].id, failing.id);
    assert_eq!(again[0].attempts, 2);
    assert!(again[0].fence > other.fence);
    // A refused report claims nothing.
    let error = client
        .report_queue(
            "db",
            "jobs",
            &failing.id,
            &failing.token,
            complete,
            Some(&poll(5)),
            &options,
        )
        .await
        .unwrap_err();
    assert!(matches!(error, FlowerError::Business { .. }), "{error:?}");
    assert_eq!(stats().await, (0, 2, 0));
    // A kill chained to an empty queue's claim takes none.
    let none = client
        .report_queue(
            "db",
            "jobs",
            &other.id,
            &other.token,
            QueueReport::Kill,
            Some(&poll(5)),
            &options,
        )
        .await
        .unwrap();
    assert!(none.is_empty());
    assert_eq!(stats().await, (0, 1, 1));
    // A report without a next claim answers none.
    let plain = client
        .report_queue(
            "db",
            "jobs",
            &again[0].id,
            &again[0].token,
            complete,
            None,
            &options,
        )
        .await
        .unwrap();
    assert!(plain.is_empty());
    assert_eq!(stats().await, (0, 0, 1));
}

#[tokio::test]
async fn worker_retries_then_kills() {
    let server = serve_node().await;
    seed(&server, "jobs", &["a"], 5).await;
    let stop = Arc::new(tokio::sync::Notify::new());
    let worker_stop = stop.clone();
    let worker_client = client(&server);
    let run = tokio::spawn(async move {
        run_queue_worker(
            worker_client,
            QueueWorkerOptions::new("db", "jobs")
                .concurrency(1)
                .poll_interval(Duration::from_millis(20)),
            worker_stop,
            |claim| async move {
                if claim.attempts < 2 {
                    Err(WorkError::Retry(Some(Duration::ZERO)))
                } else {
                    Err(WorkError::Terminal("poison".to_string()))
                }
            },
        )
        .await
        .unwrap()
    });
    // The kill lands shortly; then stop and check the dead letter.
    tokio::time::sleep(Duration::from_secs(2)).await;
    stop.notify_one();
    let summary = tokio::time::timeout(Duration::from_secs(20), run)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(summary.failed, 1);
    assert_eq!(summary.killed, 1);
    let stats = client(&server)
        .queue_stats("db", "jobs", &RequestOptions::default())
        .await
        .unwrap();
    assert_eq!(stats.pending, 0);
    assert_eq!(stats.dead, 1);
}

#[tokio::test]
async fn worker_recovers_panics() {
    let server = serve_node().await;
    seed(&server, "jobs", &["a"], 5).await;
    let done = Arc::new(tokio::sync::Notify::new());
    let stop = Arc::new(tokio::sync::Notify::new());
    let worker_done = done.clone();
    let worker_stop = stop.clone();
    let worker_client = client(&server);
    let run = tokio::spawn(async move {
        run_queue_worker(
            worker_client,
            QueueWorkerOptions::new("db", "jobs")
                .concurrency(1)
                .poll_interval(Duration::from_millis(20))
                .retry(Duration::ZERO, Duration::ZERO),
            worker_stop,
            move |claim| {
                let done = worker_done.clone();
                async move {
                    if claim.attempts < 2 {
                        panic!("boom");
                    }
                    done.notify_one();
                    Ok(())
                }
            },
        )
        .await
        .unwrap()
    });
    tokio::time::timeout(Duration::from_secs(20), done.notified())
        .await
        .unwrap();
    stop.notify_one();
    let summary = tokio::time::timeout(Duration::from_secs(20), run)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(summary.failed, 1);
    assert_eq!(summary.completed, 1);
}

#[tokio::test]
async fn worker_claims_only_what_it_can_start() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let server = serve_node().await;
    seed(&server, "jobs", &["a", "b", "c", "d"], 5).await;
    // The first claim holds its permit until the other three finish,
    // so every later poll has exactly one free permit.
    let attempts = Arc::new(Mutex::new(Vec::new()));
    let first = Arc::new(AtomicBool::new(true));
    let gate = Arc::new(tokio::sync::Notify::new());
    let done = Arc::new(tokio::sync::Notify::new());
    let stop = Arc::new(tokio::sync::Notify::new());
    let (worker_attempts, worker_gate, worker_done) =
        (attempts.clone(), gate.clone(), done.clone());
    let worker_stop = stop.clone();
    let worker_client = client(&server);
    let run = tokio::spawn(async move {
        run_queue_worker(
            worker_client,
            QueueWorkerOptions::new("db", "jobs")
                .concurrency(2)
                .poll_interval(Duration::from_millis(20)),
            worker_stop,
            move |claim| {
                let blocker = first.swap(false, Ordering::SeqCst);
                let (attempts, gate, done) = (
                    worker_attempts.clone(),
                    worker_gate.clone(),
                    worker_done.clone(),
                );
                async move {
                    if blocker {
                        gate.notified().await;
                    }
                    let mut attempts = attempts.lock().unwrap();
                    attempts.push(claim.attempts);
                    if attempts.len() == 3 {
                        gate.notify_one();
                    }
                    if attempts.len() == 4 {
                        done.notify_one();
                    }
                    Ok(())
                }
            },
        )
        .await
        .unwrap()
    });
    tokio::time::timeout(Duration::from_secs(20), done.notified())
        .await
        .unwrap();
    stop.notify_one();
    let summary = tokio::time::timeout(Duration::from_secs(20), run)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(summary.completed, 4);
    // No claim was taken without a free permit and handed back: each
    // item ran on its first attempt.
    assert_eq!(*attempts.lock().unwrap(), vec![1, 1, 1, 1]);
}

#[tokio::test]
async fn worker_recovers_handler_construction_panics() {
    let server = serve_node().await;
    seed(&server, "jobs", &["a"], 5).await;
    let done = Arc::new(tokio::sync::Notify::new());
    let stop = Arc::new(tokio::sync::Notify::new());
    let worker_done = done.clone();
    let worker_stop = stop.clone();
    let worker_client = client(&server);
    let run = tokio::spawn(async move {
        run_queue_worker(
            worker_client,
            QueueWorkerOptions::new("db", "jobs")
                .concurrency(1)
                .poll_interval(Duration::from_millis(20))
                .retry(Duration::ZERO, Duration::ZERO),
            worker_stop,
            move |claim| {
                // Panics before any future exists.
                assert!(claim.attempts >= 2, "cannot build the handler");
                let done = worker_done.clone();
                async move {
                    done.notify_one();
                    Ok(())
                }
            },
        )
        .await
        .unwrap()
    });
    tokio::time::timeout(Duration::from_secs(10), done.notified())
        .await
        .unwrap();
    stop.notify_one();
    let summary = tokio::time::timeout(Duration::from_secs(20), run)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(summary.failed, 1);
    assert_eq!(summary.completed, 1);
}

/// How a scripted queue answers heartbeats.
#[derive(Clone, Copy)]
enum Heartbeats {
    /// Every renewal fails in transport.
    Fail,
    /// Every renewal hangs.
    Hang,
}

/// A queue endpoint that hands out one claim, scripts heartbeats
/// and records every other report path.
struct ScriptedQueue {
    polls: std::sync::atomic::AtomicUsize,
    heartbeats: Heartbeats,
    /// Every poll after the first is refused with a 403, as when an
    /// operator revokes the worker token mid-run.
    revoked: bool,
    /// The first polls meet a queue a purge is deleting, answered as the
    /// server answers `queue_purging`.
    purging: usize,
    reports: Mutex<Vec<String>>,
}

impl ScriptedQueue {
    fn new(heartbeats: Heartbeats) -> Arc<Self> {
        Arc::new(ScriptedQueue {
            polls: std::sync::atomic::AtomicUsize::new(0),
            heartbeats,
            revoked: false,
            purging: 0,
            reports: Mutex::new(Vec::new()),
        })
    }

    fn purging(polls: usize) -> Arc<Self> {
        Arc::new(ScriptedQueue {
            polls: std::sync::atomic::AtomicUsize::new(0),
            heartbeats: Heartbeats::Hang,
            revoked: false,
            purging: polls,
            reports: Mutex::new(Vec::new()),
        })
    }

    fn revoked() -> Arc<Self> {
        Arc::new(ScriptedQueue {
            polls: std::sync::atomic::AtomicUsize::new(0),
            heartbeats: Heartbeats::Hang,
            revoked: true,
            purging: 0,
            reports: Mutex::new(Vec::new()),
        })
    }

    fn polls(&self) -> usize {
        self.polls.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn client(self: &Arc<Self>) -> FlowerClient {
        FlowerClient::builder("http://scripted.invalid")
            .transport(self.clone())
            .build()
            .unwrap()
    }

    fn reports(&self) -> Vec<String> {
        self.reports.lock().unwrap().clone()
    }
}

fn ok_body(body: serde_json::Value) -> flower_client::TransportResponse {
    flower_client::TransportResponse {
        status: 200,
        retry_after_ms: None,
        body: serde_json::to_vec(&body).unwrap(),
    }
}

#[async_trait::async_trait]
impl flower_client::Transport for ScriptedQueue {
    async fn post_with_headers(
        &self,
        path: &str,
        body: &[u8],
        credentials: Option<&Credentials>,
        _headers: &[(&'static str, String)],
    ) -> Result<
        (
            flower_client::TransportResponse,
            flower_client::transport::Headers,
        ),
        FlowerError,
    > {
        Ok((self.post(path, body, credentials).await?, vec![]))
    }
    async fn post(
        &self,
        path: &str,
        _body: &[u8],
        _credentials: Option<&Credentials>,
    ) -> Result<flower_client::TransportResponse, flower_client::FlowerError> {
        match path {
            "/v1/queue/poll" => {
                let poll = self.polls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if poll < self.purging {
                    return Ok(flower_client::TransportResponse {
                        status: flower_server::api::status_of("queue_purging"),
                        retry_after_ms: None,
                        body: br#"{"ok":false,"code":"queue_purging","message":"queue `jobs` is being purged"}"#
                            .to_vec(),
                    });
                }
                let first = poll == self.purging;
                if self.revoked && !first {
                    return Ok(flower_client::TransportResponse {
                        status: 403,
                        retry_after_ms: None,
                        body: br#"{"ok":false,"code":"forbidden","message":"revoked"}"#.to_vec(),
                    });
                }
                let items = if first {
                    serde_json::json!([{"id": "i1", "payload": "p", "token": "t1", "attempts": 1}])
                } else {
                    serde_json::json!([])
                };
                Ok(ok_body(serde_json::json!({"ok": true, "items": items})))
            }
            "/v1/queue/heartbeat" => match self.heartbeats {
                Heartbeats::Fail => Err(flower_client::FlowerError::Transport("down".to_string())),
                Heartbeats::Hang => std::future::pending().await,
            },
            other => {
                self.reports.lock().unwrap().push(other.to_string());
                Ok(ok_body(serde_json::json!({"ok": true})))
            }
        }
    }

    async fn get(
        &self,
        _path: &str,
        _credentials: Option<&Credentials>,
    ) -> Result<flower_client::TransportResponse, flower_client::FlowerError> {
        Err(flower_client::FlowerError::Transport("no GET".to_string()))
    }

    async fn post_stream(
        &self,
        _path: &str,
        _body: &[u8],
        _credentials: Option<&Credentials>,
    ) -> Result<flower_client::transport::StreamResponse, flower_client::FlowerError> {
        Err(flower_client::FlowerError::Transport(
            "no streams".to_string(),
        ))
    }
}

#[tokio::test]
async fn worker_waits_out_a_queue_being_purged() {
    // A purge deleting the queue refuses its operations until it ends:
    // a worker meeting that keeps polling, then works what comes.
    let queue = ScriptedQueue::purging(3);
    let stop = Arc::new(tokio::sync::Notify::new());
    let done = Arc::new(tokio::sync::Notify::new());
    let worked = done.clone();
    let run = tokio::spawn(run_queue_worker(
        queue.client(),
        QueueWorkerOptions::new("db", "jobs")
            .concurrency(1)
            .lease(Duration::from_secs(30))
            .poll_interval(Duration::from_millis(20)),
        stop.clone(),
        move |_claim| {
            let worked = worked.clone();
            async move {
                worked.notify_one();
                Ok(())
            }
        },
    ));
    tokio::time::timeout(Duration::from_secs(20), done.notified())
        .await
        .expect("the worker never worked the item after the purge");
    assert!(!run.is_finished(), "the worker stopped on a purge");
    stop.notify_one();
    run.await.unwrap().unwrap();
    assert!(queue.polls() > 3);
}

#[tokio::test]
async fn worker_drops_claim_when_renewals_cannot_land() {
    let queue = ScriptedQueue::new(Heartbeats::Fail);
    let started = Arc::new(tokio::sync::Notify::new());
    let stop = Arc::new(tokio::sync::Notify::new());
    let worker_started = started.clone();
    let run = tokio::spawn(run_queue_worker(
        queue.client(),
        QueueWorkerOptions::new("db", "jobs")
            .concurrency(1)
            .lease(Duration::from_millis(300))
            .poll_interval(Duration::from_millis(20)),
        stop.clone(),
        move |_claim| {
            let started = worker_started.clone();
            async move {
                started.notify_one();
                std::future::pending::<Result<(), WorkError>>().await
            }
        },
    ));
    tokio::time::timeout(Duration::from_secs(10), started.notified())
        .await
        .unwrap();
    // The handler never finishes and no renewal lands: once a whole
    // lease passes unrenewed the claim counts as lost, unreported.
    stop.notify_one();
    let summary = tokio::time::timeout(Duration::from_secs(10), run)
        .await
        .expect("a lapsed claim kept the worker draining")
        .unwrap()
        .unwrap();
    assert_eq!(summary.lost, 1);
    assert_eq!(summary.completed, 0);
    assert!(queue.reports().is_empty());
}

#[tokio::test]
async fn worker_handler_finishes_during_slow_heartbeat() {
    // Renewals run every second and hang; the handler finishes at
    // 1.5 s and must report then, not after the stuck renewal's 20 s
    // client timeout.
    let queue = ScriptedQueue::new(Heartbeats::Hang);
    let stop = Arc::new(tokio::sync::Notify::new());
    let run = tokio::spawn(run_queue_worker(
        queue.client(),
        QueueWorkerOptions::new("db", "jobs")
            .concurrency(1)
            .lease(Duration::from_secs(3))
            .poll_interval(Duration::from_millis(20)),
        stop.clone(),
        |_claim| async {
            tokio::time::sleep(Duration::from_millis(1500)).await;
            Ok(())
        },
    ));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    while queue.reports().is_empty() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the finished handler waited on a renewal"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    stop.notify_one();
    let summary = tokio::time::timeout(Duration::from_secs(10), run)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(summary.completed, 1);
    assert_eq!(queue.reports(), vec!["/v1/queue/complete".to_string()]);
}

#[tokio::test]
async fn worker_stops_on_revoked_credentials_after_draining() {
    // The first poll claims an item; every later poll answers 403.
    // The worker must stop by itself, without `stop`, once the held
    // item finishes and reports, instead of polling forever.
    let queue = ScriptedQueue::revoked();
    let stop = Arc::new(tokio::sync::Notify::new());
    let run = tokio::spawn(run_queue_worker(
        queue.client(),
        QueueWorkerOptions::new("db", "jobs")
            .concurrency(2)
            .lease(Duration::from_secs(30))
            .poll_interval(Duration::from_millis(20)),
        stop,
        |_claim| async {
            tokio::time::sleep(Duration::from_millis(300)).await;
            Ok(())
        },
    ));
    let error = tokio::time::timeout(Duration::from_secs(10), run)
        .await
        .expect("a revoked worker kept polling")
        .unwrap()
        .unwrap_err();
    assert!(error.is_auth(), "{error:?}");
    assert_eq!(queue.polls(), 2, "one refused poll ends the run");
    assert_eq!(queue.reports(), vec!["/v1/queue/complete".to_string()]);
}
