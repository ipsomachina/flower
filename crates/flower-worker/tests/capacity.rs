//! Adaptive concurrency, refusals for load and concurrent claimers,
//! on the in-memory queue with tokio's paused clock.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use flower_worker::capacity::{Health, Load};
use flower_worker::testing::FakeQueue;
use flower_worker::{QueueWorkerOptions, WorkError, WorkerEvent, WorkerSummary, run_queue_worker};
use tokio::sync::Notify;

/// Handlers in flight now and at most.
#[derive(Clone, Default)]
struct Gauge {
    now: Arc<AtomicUsize>,
    most: Arc<AtomicUsize>,
}

impl Gauge {
    fn enter(&self) {
        let now = self.now.fetch_add(1, Ordering::SeqCst) + 1;
        self.most.fetch_max(now, Ordering::SeqCst);
    }

    fn leave(&self) {
        self.now.fetch_sub(1, Ordering::SeqCst);
    }

    fn most(&self) -> usize {
        self.most.load(Ordering::SeqCst)
    }

    fn reset(&self) {
        self.most
            .store(self.now.load(Ordering::SeqCst), Ordering::SeqCst);
    }
}

/// Run a worker whose handlers take `work` each, returning its stop
/// signal and join handle.
fn spawn_worker(
    queue: FakeQueue,
    options: QueueWorkerOptions,
    gauge: Gauge,
    work: Duration,
) -> (
    Arc<Notify>,
    tokio::task::JoinHandle<Result<WorkerSummary, flower_client::error::FlowerError>>,
) {
    let stop = Arc::new(Notify::new());
    let handle = tokio::spawn(run_queue_worker(
        queue,
        options,
        stop.clone(),
        move |_claim| {
            let gauge = gauge.clone();
            async move {
                gauge.enter();
                tokio::time::sleep(work).await;
                gauge.leave();
                Ok::<(), WorkError>(())
            }
        },
    ));
    (stop, handle)
}

fn backlog(queue: &FakeQueue, count: usize) {
    for n in 0..count {
        queue.enqueue("db", "jobs", serde_json::json!(n), 5);
    }
}

#[tokio::test(start_paused = true)]
async fn an_adaptive_limit_grows_with_the_backlog_and_shrinks_under_load() {
    let queue = FakeQueue::new();
    backlog(&queue, 5_000);
    let load = Arc::new(Mutex::new(0.0));
    let reading = load.clone();
    let health: Arc<dyn Health> = Arc::new(move || Load::new(*reading.lock().unwrap(), "busy"));
    let events = Arc::new(Mutex::new(Vec::new()));
    let seen = events.clone();
    let options = QueueWorkerOptions::new("db", "jobs")
        .adaptive(1, 1, 32)
        .health(health)
        .poll_interval(Duration::from_millis(50))
        .on_event(move |event| seen.lock().unwrap().push(event));
    let gauge = Gauge::default();
    let (stop, worker) = spawn_worker(
        queue.clone(),
        options,
        gauge.clone(),
        Duration::from_millis(100),
    );
    // Doubling every 250 ms while polls come back full and the process
    // has room: 1, 2, 4, 8, 16, 32.
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(gauge.most(), 32);
    assert_eq!(queue.max_claimed(), 32);
    // The process falls behind: cut by a quarter at most once a second,
    // down to the floor.
    *load.lock().unwrap() = 1.5;
    tokio::time::sleep(Duration::from_secs(20)).await;
    gauge.reset();
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(gauge.most() <= 2, "{}", gauge.most());
    stop.notify_one();
    let summary = worker.await.unwrap().unwrap();
    assert!(summary.completed > 100);
    let events = events.lock().unwrap();
    let grew: Vec<usize> = events
        .iter()
        .filter_map(|event| match event {
            WorkerEvent::Limit(change) if change.reason == "more work is waiting" => {
                Some(change.limit)
            }
            _ => None,
        })
        .collect();
    assert_eq!(&grew[..5], &[2, 4, 8, 16, 32]);
    let cuts: Vec<usize> = events
        .iter()
        .filter_map(|event| match event {
            WorkerEvent::Limit(change) if change.reason == "busy" => Some(change.limit),
            _ => None,
        })
        .collect();
    assert_eq!(cuts.first(), Some(&24));
}

#[tokio::test(start_paused = true)]
async fn a_fixed_limit_never_adapts() {
    let queue = FakeQueue::new();
    backlog(&queue, 1_000);
    let gauge = Gauge::default();
    let options = QueueWorkerOptions::new("db", "jobs").concurrency(3);
    let (stop, worker) = spawn_worker(queue, options, gauge.clone(), Duration::from_millis(100));
    tokio::time::sleep(Duration::from_secs(5)).await;
    stop.notify_one();
    worker.await.unwrap().unwrap();
    assert_eq!(gauge.most(), 3);
}

#[tokio::test(start_paused = true)]
async fn refusals_for_load_pause_claims_and_halve_the_limit() {
    let queue = FakeQueue::new();
    backlog(&queue, 200);
    queue.refuse_polls(1);
    let events = Arc::new(Mutex::new(Vec::new()));
    let seen = events.clone();
    let polls_at_throttle = Arc::new(Mutex::new(None));
    let (polls, fake) = (polls_at_throttle.clone(), queue.clone());
    let options = QueueWorkerOptions::new("db", "jobs")
        .adaptive(1, 8, 16)
        .health(Arc::new(Load::idle))
        .poll_interval(Duration::from_millis(10))
        .on_event(move |event| {
            if matches!(event, WorkerEvent::Throttled { .. }) {
                *polls.lock().unwrap() = Some((fake.polls(), tokio::time::Instant::now()));
            }
            seen.lock().unwrap().push(event);
        });
    let gauge = Gauge::default();
    let (stop, worker) = spawn_worker(queue.clone(), options, gauge, Duration::from_millis(10));
    tokio::time::sleep(Duration::from_millis(500)).await;
    let (polls, at) = polls_at_throttle.lock().unwrap().expect("throttled");
    // No poll during the pause.
    tokio::time::sleep_until(at + Duration::from_millis(900)).await;
    assert_eq!(queue.polls(), polls);
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert!(queue.polls() > polls);
    stop.notify_one();
    let summary = worker.await.unwrap().unwrap();
    assert_eq!(summary.completed, 200);
    let events = events.lock().unwrap();
    assert_eq!(
        events[0],
        WorkerEvent::Throttled {
            pause: Duration::from_secs(1)
        }
    );
    assert!(matches!(
        &events[1],
        WorkerEvent::Limit(change) if change.limit == 4 && change.reason == "Flower is shedding load"
    ));
}

#[tokio::test(start_paused = true)]
async fn claimers_poll_concurrently_while_polls_come_back_full() {
    // Slow polls of one item each: one claimer at a time would take a
    // poll latency per item.
    let queue = FakeQueue::new();
    backlog(&queue, 100);
    queue.poll_latency(Duration::from_millis(100));
    let gauge = Gauge::default();
    let options = QueueWorkerOptions::new("db", "jobs")
        .concurrency(8)
        .batch(1)
        .claimers(4);
    let (stop, worker) = spawn_worker(
        queue.clone(),
        options,
        gauge.clone(),
        Duration::from_secs(1),
    );
    tokio::time::sleep(Duration::from_millis(450)).await;
    assert_eq!(queue.max_polls_in_flight(), 4);
    assert_eq!(gauge.most(), 8);
    stop.notify_one();
    worker.await.unwrap().unwrap();
    // One claimer: one poll at a time, and the pool fills slowly.
    let queue = FakeQueue::new();
    backlog(&queue, 100);
    queue.poll_latency(Duration::from_millis(100));
    let gauge = Gauge::default();
    let options = QueueWorkerOptions::new("db", "jobs")
        .concurrency(8)
        .batch(1)
        .claimers(1);
    let (stop, worker) = spawn_worker(
        queue.clone(),
        options,
        gauge.clone(),
        Duration::from_secs(1),
    );
    tokio::time::sleep(Duration::from_millis(450)).await;
    assert_eq!(queue.max_polls_in_flight(), 1);
    assert_eq!(gauge.most(), 4);
    stop.notify_one();
    worker.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn an_idle_queue_costs_one_poll_per_interval() {
    let queue = FakeQueue::new();
    let options = QueueWorkerOptions::new("db", "jobs")
        .concurrency(8)
        .batch(1)
        .claimers(4)
        .poll_interval(Duration::from_millis(100));
    let (stop, worker) = spawn_worker(queue.clone(), options, Gauge::default(), Duration::ZERO);
    tokio::time::sleep(Duration::from_millis(1_050)).await;
    assert_eq!(queue.max_polls_in_flight(), 1);
    assert!((10..=12).contains(&queue.polls()), "{}", queue.polls());
    stop.notify_one();
    worker.await.unwrap().unwrap();
}

/// A pool's load at `load`, with `in_use` running.
fn pool_load(load: f64, in_use: usize) -> flower_worker::PoolLoad {
    flower_worker::PoolLoad {
        load,
        reason: "test".to_string(),
        limit: 4,
        min: 1,
        max: 8,
        in_use,
        waiting: false,
        throttle: flower_worker::Throttle::Steady,
    }
}

#[tokio::test(start_paused = true)]
async fn pools_say_their_load_on_band_changes_and_once_a_minute_while_working() {
    let mut gate = flower_worker::LoadGate::default();
    let now = tokio::time::Instant::now();
    // Under half: the band it starts in, so nothing to say yet.
    assert!(!gate.due(now, &pool_load(0.2, 1)));
    // Pressed: a new band, said at once.
    assert!(gate.due(now, &pool_load(0.7, 1)));
    assert!(!gate.due(now, &pool_load(0.7, 1)));
    // Behind within five seconds of the last: held back, then said.
    assert!(!gate.due(now + Duration::from_secs(4), &pool_load(1.2, 1)));
    assert!(gate.due(now + Duration::from_secs(5), &pool_load(1.2, 1)));
    // Unchanged, it is said again a minute later while it works...
    assert!(!gate.due(now + Duration::from_secs(64), &pool_load(1.2, 1)));
    assert!(gate.due(now + Duration::from_secs(65), &pool_load(1.2, 1)));
    // ...and never while idle.
    assert!(!gate.due(now + Duration::from_secs(300), &pool_load(1.2, 0)));
}
