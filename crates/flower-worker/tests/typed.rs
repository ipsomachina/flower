use flower_sdk::queue::TypedQueue;
use flower_worker::{QueueWorkerOptions, run_typed_queue_worker, testing::FakeQueue};
use std::{sync::Arc, time::Duration};

static MAIL: TypedQueue<String> = TypedQueue::new("mail");

#[tokio::test]
async fn descriptor_workers_decode_payloads_and_settle_their_lease() {
    let queue = FakeQueue::new();
    queue.enqueue("db", "mail", serde_json::json!("invoice"), 3);
    let stop = Arc::new(tokio::sync::Notify::new());
    let stopping = stop.clone();
    let summary = run_typed_queue_worker(
        queue.clone(),
        &MAIL,
        QueueWorkerOptions::new("db", "ignored").concurrency(1),
        stop,
        move |job| {
            let stop = stopping.clone();
            async move {
                assert_eq!(job.payload, "invoice");
                assert!(job.claim.fence > 0);
                stop.notify_one();
                Ok(())
            }
        },
    )
    .await
    .unwrap();
    assert_eq!(summary.completed, 1);
    assert_eq!(queue.stats("db", "mail").claimed, 0);
}

#[tokio::test]
async fn incompatible_payloads_are_dead_lettered_without_running_a_handler() {
    let queue = FakeQueue::new();
    queue.enqueue("db", "mail", serde_json::json!(42), 3);
    let stop = Arc::new(tokio::sync::Notify::new());
    let stopping = stop.clone();
    let worker = tokio::spawn(run_typed_queue_worker(
        queue.clone(),
        &MAIL,
        QueueWorkerOptions::new("db", "mail").poll_interval(Duration::from_millis(1)),
        stop,
        |_| async { panic!("handler received incompatible payload") },
    ));
    tokio::time::timeout(Duration::from_secs(3), async {
        while queue.stats("db", "mail").dead == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    stopping.notify_one();
    assert_eq!(worker.await.unwrap().unwrap().killed, 1);
}
