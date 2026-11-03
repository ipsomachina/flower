//! Places in the queue's line, chained claims, idle jobs, throttling
//! jobs and drain deadlines, on the in-memory queue with tokio's paused
//! clock (one stuck-handler case on the real clock).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use flower_client::error::FlowerError;
use flower_worker::testing::FakeQueue;
use flower_worker::{
    JobControl, QueueWorkerOptions, WorkError, WorkerEvent, WorkerSummary, run_queue_worker,
    run_queue_worker_with,
};
use tokio::sync::Notify;
use tokio::task::JoinHandle;

type Worker = JoinHandle<Result<WorkerSummary, FlowerError>>;

/// Run a worker whose handler records `(owner, payload)` and holds an
/// item for `hold`, returning its stop signal and join handle.
fn recording(
    queue: &FakeQueue,
    options: QueueWorkerOptions,
    seen: Arc<Mutex<Vec<(String, String)>>>,
    hold: Duration,
) -> (Arc<Notify>, Worker) {
    let stop = Arc::new(Notify::new());
    let owner = options.owner.clone();
    let handle = tokio::spawn(run_queue_worker(
        queue.clone(),
        options,
        stop.clone(),
        move |claim| {
            let (seen, owner) = (seen.clone(), owner.clone());
            async move {
                let payload = claim.payload.as_str().unwrap_or_default().to_string();
                seen.lock().unwrap().push((owner, payload));
                tokio::time::sleep(hold).await;
                Ok::<(), WorkError>(())
            }
        },
    ));
    (stop, handle)
}

fn waiting(owner: &str) -> QueueWorkerOptions {
    QueueWorkerOptions::new("db", "jobs")
        .owner(owner)
        .concurrency(1)
        .wait(Duration::from_secs(60))
        .poll_interval(Duration::from_millis(100))
}

#[tokio::test(start_paused = true)]
async fn a_worker_in_line_reads_readiness_instead_of_claiming() {
    let queue = FakeQueue::new();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let options = waiting("a").concurrency(4);
    let (stop, worker) = recording(&queue, options, seen, Duration::ZERO);
    tokio::time::sleep(Duration::from_millis(1_050)).await;
    // One claim took a place; after it, reads only, one per interval.
    assert_eq!(queue.polls(), 1);
    assert!(
        (9..=11).contains(&queue.readiness_reads()),
        "{}",
        queue.readiness_reads()
    );
    assert_eq!(queue.line("db", "jobs", ""), vec!["a".to_string()]);
    stop.notify_one();
    worker.await.unwrap().unwrap();
    // Stopping gives the place up at once.
    assert!(queue.line("db", "jobs", "").is_empty());
}

#[tokio::test(start_paused = true)]
async fn a_waiting_worker_claims_again_to_keep_its_place() {
    let queue = FakeQueue::new();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let options = waiting("a").wait(Duration::from_secs(1));
    let (stop, worker) = recording(&queue, options, seen, Duration::ZERO);
    // A one-second place would run out by now, unless claimed again
    // between 3/8 and half of it each time.
    tokio::time::sleep(Duration::from_millis(3_200)).await;
    assert_eq!(queue.line("db", "jobs", ""), vec!["a".to_string()]);
    assert!((7..=10).contains(&queue.polls()), "{}", queue.polls());
    stop.notify_one();
    worker.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn new_work_wakes_the_first_in_line() {
    let queue = FakeQueue::new();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let (stop_a, a) = recording(&queue, waiting("a"), seen.clone(), Duration::from_secs(10));
    tokio::time::sleep(Duration::from_millis(50)).await;
    let (stop_b, b) = recording(&queue, waiting("b"), seen.clone(), Duration::ZERO);
    // Both wait, `a` first. A job arrives just before `b` would look:
    // it is `a`'s for the turn, so `b` reads it is not its turn.
    tokio::time::sleep(Duration::from_millis(970)).await;
    queue.enqueue("db", "jobs", serde_json::json!("x"), 5);
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(
        *seen.lock().unwrap(),
        vec![("a".to_string(), "x".to_string())]
    );
    // `a` is busy and full, so it left the line: the next job is `b`'s.
    queue.enqueue("db", "jobs", serde_json::json!("y"), 5);
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(seen.lock().unwrap()[1], ("b".to_string(), "y".to_string()));
    stop_a.notify_one();
    stop_b.notify_one();
    a.await.unwrap().unwrap();
    b.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn chained_reports_claim_the_next_items() {
    let queue = FakeQueue::new();
    for n in 0..40 {
        queue.enqueue("db", "jobs", serde_json::json!(n), 5);
    }
    let handled = Arc::new(AtomicUsize::new(0));
    let all = Arc::new(Notify::new());
    let stop = Arc::new(Notify::new());
    let (counted, done) = (handled.clone(), all.clone());
    let worker = tokio::spawn(run_queue_worker(
        queue.clone(),
        QueueWorkerOptions::new("db", "jobs")
            .concurrency(2)
            .chain()
            .poll_interval(Duration::from_millis(100)),
        stop.clone(),
        move |_claim| {
            let (counted, done) = (counted.clone(), done.clone());
            async move {
                tokio::time::sleep(Duration::from_millis(10)).await;
                if counted.fetch_add(1, Ordering::SeqCst) + 1 == 40 {
                    done.notify_one();
                }
                Ok::<(), WorkError>(())
            }
        },
    ));
    all.notified().await;
    // One poll filled both slots; every later item came with the report
    // of the one before it.
    assert_eq!(queue.polls(), 1);
    assert_eq!(queue.chained_claims(), 38);
    stop.notify_one();
    let summary = worker.await.unwrap().unwrap();
    assert_eq!(summary.completed, 40);
    assert_eq!(queue.stats("db", "jobs").pending, 0);
}

#[tokio::test(start_paused = true)]
async fn idle_jobs_stop_counting_toward_the_limit() {
    let queue = FakeQueue::new();
    for payload in ["long", "s1", "s2", "s3"] {
        queue.enqueue("db", "jobs", serde_json::json!(payload), 5);
    }
    let short = Arc::new(AtomicUsize::new(0));
    let kept: Arc<Mutex<Option<JobControl>>> = Arc::new(Mutex::new(None));
    let stop = Arc::new(Notify::new());
    let (counted, keep) = (short.clone(), kept.clone());
    let worker = tokio::spawn(run_queue_worker_with(
        queue.clone(),
        QueueWorkerOptions::new("db", "jobs")
            .concurrency(1)
            .poll_interval(Duration::from_millis(50)),
        stop.clone(),
        move |claim, job| {
            let (counted, keep) = (counted.clone(), keep.clone());
            async move {
                if claim.payload == "long" {
                    // It mostly waits from here: the worker takes others.
                    assert!(job.idle());
                    assert!(!job.idle(), "saying it twice changes nothing");
                    *keep.lock().unwrap() = Some(job);
                    tokio::time::sleep(Duration::from_secs(10)).await;
                } else {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    counted.fetch_add(1, Ordering::SeqCst);
                }
                Ok::<(), WorkError>(())
            }
        },
    ));
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(
        short.load(Ordering::SeqCst),
        3,
        "the long job held the slot"
    );
    assert_eq!(queue.stats("db", "jobs").claimed, 1);
    tokio::time::sleep(Duration::from_secs(10)).await;
    stop.notify_one();
    let summary = worker.await.unwrap().unwrap();
    assert_eq!(summary.completed, 4);
    // A job that ended can no longer go idle.
    assert!(!kept.lock().unwrap().as_ref().unwrap().idle());
}

#[tokio::test(start_paused = true)]
async fn a_throttled_job_pauses_claims() {
    let queue = FakeQueue::new();
    for n in 0..10 {
        queue.enqueue("db", "jobs", serde_json::json!(n), 5);
    }
    let events = Arc::new(Mutex::new(Vec::new()));
    let seen = events.clone();
    let handled = Arc::new(AtomicUsize::new(0));
    let counted = handled.clone();
    let stop = Arc::new(Notify::new());
    let worker = tokio::spawn(run_queue_worker_with(
        queue.clone(),
        QueueWorkerOptions::new("db", "jobs")
            .concurrency(1)
            .poll_interval(Duration::from_millis(50))
            .on_event(move |event| seen.lock().unwrap().push(event)),
        stop.clone(),
        move |_claim, job| {
            let counted = counted.clone();
            async move {
                if counted.fetch_add(1, Ordering::SeqCst) == 0 {
                    // The service behind it says it is asked too much.
                    job.throttle(Duration::from_secs(2), "the oven is overloaded");
                }
                Ok::<(), WorkError>(())
            }
        },
    ));
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    assert_eq!(handled.load(Ordering::SeqCst), 1);
    assert_eq!(queue.polls(), 1);
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(handled.load(Ordering::SeqCst), 10);
    stop.notify_one();
    worker.await.unwrap().unwrap();
    assert!(events.lock().unwrap().contains(&WorkerEvent::Throttled {
        pause: Duration::from_secs(2)
    }));
}

/// A worker whose handler for `stuck` never finishes, stopped at once
/// with a drain deadline of one second.
async fn drain_stuck(options: QueueWorkerOptions) -> (FakeQueue, String, WorkerSummary) {
    let queue = FakeQueue::new();
    let id = queue.enqueue("db", "jobs", serde_json::json!("stuck"), 5);
    let started = Arc::new(Notify::new());
    let stop = Arc::new(Notify::new());
    let begun = started.clone();
    let worker = tokio::spawn(run_queue_worker(
        queue.clone(),
        options
            .concurrency(1)
            .drain(Duration::from_secs(1))
            .poll_interval(Duration::from_millis(50)),
        stop.clone(),
        move |_claim| {
            let begun = begun.clone();
            async move {
                begun.notify_one();
                std::future::pending::<Result<(), WorkError>>().await
            }
        },
    ));
    started.notified().await;
    stop.notify_one();
    let summary = tokio::time::timeout(Duration::from_secs(60), worker)
        .await
        .expect("the drain waited on a handler that never finishes")
        .unwrap()
        .unwrap();
    (queue, id, summary)
}

#[tokio::test(start_paused = true)]
async fn the_drain_deadline_fails_unfinished_items_and_never_completes_them() {
    let options = QueueWorkerOptions::new("db", "jobs")
        .retry(Duration::from_secs(1), Duration::from_secs(60));
    let (queue, id, summary) = drain_stuck(options).await;
    assert_eq!(summary.drained, 1);
    assert_eq!(summary.completed, 0);
    // Failed with the default backoff, its attempt spent: it runs again
    // later, never acknowledged as done.
    let item = queue.item("db", "jobs", &id).unwrap();
    assert_eq!(item.state, "pending");
    assert_eq!(item.attempts, 1);
    assert!(item.due_ms >= queue.now_ms() + 1_000, "{item:?}");
}

#[tokio::test(start_paused = true)]
async fn released_drained_items_run_again_at_once() {
    let options = QueueWorkerOptions::new("db", "jobs").release_drained();
    let (queue, id, summary) = drain_stuck(options).await;
    assert_eq!(summary.drained, 1);
    assert_eq!(summary.completed, 0);
    let item = queue.item("db", "jobs", &id).unwrap();
    assert_eq!(item.state, "pending");
    assert!(item.due_ms <= queue.now_ms(), "{item:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn handlers_stuck_past_the_drain_are_given_up_on() {
    let queue = FakeQueue::new();
    let id = queue.enqueue("db", "jobs", serde_json::json!("blocked"), 5);
    let started = Arc::new(Notify::new());
    let stop = Arc::new(Notify::new());
    let begun = started.clone();
    let worker = tokio::spawn(run_queue_worker(
        queue.clone(),
        QueueWorkerOptions::new("db", "jobs")
            .concurrency(1)
            .drain(Duration::from_millis(100))
            .poll_interval(Duration::from_millis(20)),
        stop.clone(),
        move |_claim| {
            let begun = begun.clone();
            async move {
                begun.notify_one();
                // Stuck outside any await point: the deadline cannot
                // drop it.
                std::thread::sleep(Duration::from_secs(6));
                Ok::<(), WorkError>(())
            }
        },
    ));
    started.notified().await;
    let stopped = std::time::Instant::now();
    stop.notify_one();
    let summary = worker.await.unwrap().unwrap();
    // Given up five seconds past the deadline, unreported: its claim
    // redelivers when its lease lapses.
    assert!(
        stopped.elapsed() < Duration::from_millis(5_900),
        "{:?}",
        stopped.elapsed()
    );
    assert_eq!(summary.lost, 1);
    assert_eq!(summary.completed, 0);
    assert_eq!(queue.item("db", "jobs", &id).unwrap().state, "claimed");
}

#[tokio::test(start_paused = true)]
async fn a_throttled_job_pauses_chained_claims_too() {
    let queue = FakeQueue::new();
    for n in 0..10 {
        queue.enqueue("db", "jobs", serde_json::json!(n), 5);
    }
    let handled = Arc::new(AtomicUsize::new(0));
    let counted = handled.clone();
    let stop = Arc::new(Notify::new());
    let worker = tokio::spawn(run_queue_worker_with(
        queue.clone(),
        QueueWorkerOptions::new("db", "jobs")
            .concurrency(1)
            .chain()
            .poll_interval(Duration::from_millis(50)),
        stop.clone(),
        move |_claim, job| {
            let counted = counted.clone();
            async move {
                if counted.fetch_add(1, Ordering::SeqCst) == 0 {
                    job.throttle(Duration::from_secs(2), "the oven is overloaded");
                }
                Ok::<(), WorkError>(())
            }
        },
    ));
    // The throttling job's report must not claim its successor: the
    // service said it was asked too much.
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    assert_eq!(handled.load(Ordering::SeqCst), 1);
    assert_eq!(queue.chained_claims(), 0);
    assert_eq!(queue.polls(), 1);
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(handled.load(Ordering::SeqCst), 10);
    stop.notify_one();
    worker.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_place_shorter_than_two_poll_intervals_is_kept() {
    let queue = FakeQueue::new();
    let seen = Arc::new(Mutex::new(Vec::new()));
    // The place lasts a second; the worker looks every 1.5 s.
    let options = waiting("a")
        .wait(Duration::from_secs(1))
        .poll_interval(Duration::from_millis(1_500));
    let (stop, worker) = recording(&queue, options, seen, Duration::ZERO);
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(queue.line("db", "jobs", ""), vec!["a".to_string()]);
    }
    stop.notify_one();
    worker.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_stopping_worker_leaves_the_line_a_poll_still_out_joined() {
    let queue = FakeQueue::new();
    // Claims commit at once and answer 200 ms later.
    queue.answer_latency(Duration::from_millis(200));
    let seen = Arc::new(Mutex::new(Vec::new()));
    let (stop, worker) = recording(&queue, waiting("a"), seen, Duration::ZERO);
    tokio::time::sleep(Duration::from_millis(50)).await;
    // The first claim came back short on the server and took a place,
    // but its answer is not back yet.
    assert_eq!(queue.line("db", "jobs", ""), vec!["a".to_string()]);
    stop.notify_one();
    worker.await.unwrap().unwrap();
    assert!(queue.line("db", "jobs", "").is_empty());
}

/// A worker with chained claims over `items`, each handled once,
/// whose first chained report meets a transient failure (after
/// committing when `committed`). Answers what it handled, in order,
/// and its summary.
async fn chained_through_a_failure(
    items: usize,
    committed: bool,
) -> (FakeQueue, Vec<String>, WorkerSummary) {
    let queue = FakeQueue::new();
    for n in 0..items {
        queue.enqueue("db", "jobs", serde_json::json!(format!("item-{n}")), 5);
    }
    let error = FlowerError::Transport("connection reset".to_string());
    queue.fail_chained_reports(1, error, committed);
    let seen = Arc::new(Mutex::new(Vec::new()));
    let options = QueueWorkerOptions::new("db", "jobs")
        .owner("a")
        .concurrency(1)
        .chain()
        .lease(Duration::from_secs(3))
        .poll_interval(Duration::from_millis(100));
    let (stop, worker) = recording(&queue, options, seen.clone(), Duration::from_millis(10));
    tokio::time::sleep(Duration::from_secs(10)).await;
    stop.notify_one();
    let summary = worker.await.unwrap().unwrap();
    let handled = seen
        .lock()
        .unwrap()
        .iter()
        .map(|(_, item)| item.clone())
        .collect();
    (queue, handled, summary)
}

#[tokio::test(start_paused = true)]
async fn a_chained_report_that_failed_uncommitted_is_retried_alone() {
    let (queue, handled, summary) = chained_through_a_failure(3, false).await;
    // The retry reported item-0 alone; item-1 came by poll and chained
    // item-2.
    assert_eq!(handled, ["item-0", "item-1", "item-2"]);
    assert_eq!(summary.completed, 3);
    assert_eq!(summary.lost, 0);
    assert_eq!(queue.chained_claims(), 1);
    assert_eq!(queue.stats("db", "jobs").pending, 0);
}

#[tokio::test(start_paused = true)]
async fn a_chained_report_that_committed_before_failing_loses_only_its_claim_to_the_lease() {
    let (queue, handled, summary) = chained_through_a_failure(2, true).await;
    // The first attempt completed item-0 and claimed item-1, but its
    // answer was lost: the retry, alone, is refused (counted lost), and
    // item-1 runs once, after its unseen lease lapses.
    assert_eq!(handled, ["item-0", "item-1"]);
    assert_eq!(summary.completed, 1);
    assert_eq!(summary.lost, 1);
    assert_eq!(queue.chained_claims(), 1);
    let stats = queue.stats("db", "jobs");
    assert_eq!((stats.pending, stats.claimed, stats.dead), (0, 0, 0));
}

#[tokio::test(start_paused = true)]
async fn a_handler_finishing_at_the_drain_deadline_completes() {
    let queue = FakeQueue::new();
    let id = queue.enqueue("db", "jobs", serde_json::json!("on-time"), 5);
    let started = Arc::new(Notify::new());
    let stop = Arc::new(Notify::new());
    let begun = started.clone();
    let worker = tokio::spawn(run_queue_worker(
        queue.clone(),
        QueueWorkerOptions::new("db", "jobs")
            .concurrency(1)
            .drain(Duration::from_secs(1))
            .poll_interval(Duration::from_millis(50)),
        stop.clone(),
        move |_claim| {
            let begun = begun.clone();
            async move {
                begun.notify_one();
                // Done exactly when the drain deadline falls.
                tokio::time::sleep(Duration::from_secs(1)).await;
                Ok::<(), WorkError>(())
            }
        },
    ));
    started.notified().await;
    stop.notify_one();
    let summary = worker.await.unwrap().unwrap();
    // A finished handler wins over the deadline: completed, not drained.
    assert_eq!(summary.completed, 1);
    assert_eq!(summary.drained, 0);
    assert!(queue.item("db", "jobs", &id).is_none());
}

/// A queue a removed module's purge is deleting refuses every operation
/// with `queue_purging` until the purge ends: the worker waits it out
/// like a transient failure instead of exiting.
#[tokio::test(start_paused = true)]
async fn a_purging_queue_is_waited_out() {
    let queue = FakeQueue::new();
    queue.enqueue("db", "jobs", serde_json::json!("after"), 3);
    queue.fail_polls(
        3,
        FlowerError::Business {
            code: "queue_purging".to_string(),
            message: "queue `jobs` is being purged with module `old`".to_string(),
            revision: None,
            position: None,
            details: None,
        },
    );
    let seen = Arc::new(Mutex::new(Vec::new()));
    let options = QueueWorkerOptions::new("db", "jobs").poll_interval(Duration::from_millis(50));
    let (stop, worker) = recording(&queue, options, seen.clone(), Duration::ZERO);
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(!worker.is_finished(), "the worker exited on queue_purging");
    stop.notify_one();
    let summary = worker.await.unwrap().unwrap();
    assert_eq!(summary.completed, 1);
    assert!(queue.polls() > 3);
}
