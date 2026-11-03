//! Replicated retry retention: epochs and floors decided in the log,
//! paced collection, leader rotation and retry sessions.

mod counters;

use std::time::Duration;

use counters::{key, read, single_node};
use flower_cluster::node::{DEFAULT_MAX_RECEIPTS, DEFAULT_RETRY_WINDOW_MS, Node, default_rotation};
use flower_cluster::types::ResponseOutcome;
use flower_engine::engine::{CallOptions, MutationOptions, RetryIdentity};
use flower_engine::retention::{
    Acknowledgement, MAX_ACK_LIMIT, RetentionCommand, Rotation, request_fingerprint, session_intent,
};
use flower_engine::retry::{Receipt, ScopedIdentity};
use flower_sdk::error::codes;
use flower_sdk::{Failure, Principal, Value};

/// Mutation options under a scoped identity of `intent` in `epoch`.
fn scoped(node: &Node, epoch: u64, intent: Vec<u8>, name: &str) -> MutationOptions {
    MutationOptions {
        retry: Some(RetryIdentity {
            identity: ScopedIdentity {
                incarnation: node.incarnation(),
                epoch,
                intent,
            },
            fingerprint: request_fingerprint("app", "incr", &key(name), None).unwrap(),
        }),
        ..MutationOptions::default()
    }
}

/// An initialized member with an incarnation and retention at epoch 0.
async fn retained(dir: &std::path::Path) -> Node {
    let node = single_node(dir).await;
    assert_ne!(node.establish_incarnation().await.unwrap(), 0);
    let rotate = RetentionCommand::Rotate {
        epoch_ms: None,
        keep_epochs: 2,
    };
    node.retention(b"db", rotate).await.unwrap().unwrap();
    node
}

/// What an increment answered: its value, or its failure's code.
async fn incr(node: &Node, name: &str, options: &MutationOptions) -> Result<Value, String> {
    let response = node
        .mutate(b"db", "app", "incr", &key(name), options)
        .await
        .unwrap();
    match &response.outcome {
        ResponseOutcome::Value(bytes) => Ok(flower_engine::json::parse_value(bytes).unwrap()),
        ResponseOutcome::Failure(bytes) => {
            let envelope = flower_engine::json::parse_value(bytes).unwrap();
            Err(flower_sdk::abi::decode_envelope(envelope).unwrap_err().code)
        }
        ResponseOutcome::Error(message) => Err(message.split(": ").next().unwrap().to_string()),
    }
}

async fn advance(node: &Node, from: u64, current: u64, min: u64) -> Result<Value, Failure> {
    let command = RetentionCommand::Advance {
        from_epoch: from,
        current_epoch: current,
        min_epoch: min,
    };
    node.retention(b"db", command).await.unwrap()
}

/// Receipts stored for the database.
async fn receipts(node: &Node) -> usize {
    let engine = node.engine().lock().await;
    let read = engine.store().begin_read().unwrap();
    read.scan_receipts(b"db").unwrap().len()
}

/// An identity runs while its epoch is admissible and replays after;
/// once the floor passes it, it neither replays nor runs, whether or
/// not its receipt is still stored, and an epoch past the current one
/// is not admitted yet.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_replicated_floor_retires_identities() {
    let dir = tempfile::tempdir().unwrap();
    let node = retained(dir.path()).await;
    let first = scoped(&node, 0, vec![1; 32], "a");
    assert_eq!(incr(&node, "a", &first).await, Ok(Value::Number(1.0)));
    assert_eq!(incr(&node, "a", &first).await, Ok(Value::Number(1.0)));
    let early = scoped(&node, 1, vec![2; 32], "a");
    assert_eq!(
        incr(&node, "a", &early).await,
        Err("epoch_not_admitted".to_string())
    );
    advance(&node, 0, 1, 1).await.unwrap();
    // Another advance planned against epoch 0 lost the race.
    assert_eq!(
        advance(&node, 0, 2, 2).await.unwrap_err().code,
        "retention_epoch_moved"
    );
    assert_eq!(
        incr(&node, "a", &first).await,
        Err("epoch_retired".to_string())
    );
    assert_eq!(incr(&node, "a", &early).await, Ok(Value::Number(2.0)));
    assert_eq!(read(&node, "a").await, 2.0);
    node.shutdown().await.unwrap();
}

/// Collection walks the receipts a page per entry from a replicated
/// cursor and deletes exactly those the floor retired: scoped ones
/// below it and a caller's own ones that ran in an epoch below it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn collection_deletes_retired_receipts_page_by_page() {
    let dir = tempfile::tempdir().unwrap();
    let node = retained(dir.path()).await;
    for index in 0..6u8 {
        let options = scoped(&node, 0, vec![index; 32], "a");
        incr(&node, "a", &options).await.unwrap();
        let keyed = MutationOptions {
            idempotency_key: Some(vec![b'k', index]),
            ..MutationOptions::default()
        };
        incr(&node, "b", &keyed).await.unwrap();
    }
    advance(&node, 0, 1, 0).await.unwrap();
    // Both kept: they run in the new epoch.
    let kept = scoped(&node, 1, vec![9; 32], "a");
    incr(&node, "a", &kept).await.unwrap();
    let late = MutationOptions {
        idempotency_key: Some(b"late".to_vec()),
        ..MutationOptions::default()
    };
    incr(&node, "b", &late).await.unwrap();
    assert_eq!(receipts(&node).await, 14);
    advance(&node, 1, 1, 1).await.unwrap();
    let mut pages = 0;
    loop {
        let answer = node
            .retention(b"db", RetentionCommand::Collect { limit: 5 })
            .await
            .unwrap()
            .unwrap();
        pages += 1;
        let Value::Object(fields) = answer else {
            panic!("collect answered {answer:?}");
        };
        if fields.get("complete") == Some(&Value::Bool(true)) {
            break;
        }
        assert!(pages < 10);
    }
    assert_eq!(pages, 3, "14 receipts take three pages of five");
    assert_eq!(receipts(&node).await, 2);
    // A caller's own key, collected, runs as a new request.
    let collected = MutationOptions {
        idempotency_key: Some(vec![b'k', 0]),
        ..MutationOptions::default()
    };
    assert_eq!(incr(&node, "b", &collected).await, Ok(Value::Number(8.0)));
    node.shutdown().await.unwrap();
}

/// A rotating leader advances the epoch after its own time in it, keeps
/// the configured epochs, and its maintenance collects what retired.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_leader_rotates_epochs_and_collects() {
    let dir = tempfile::tempdir().unwrap();
    let node = retained(dir.path()).await;
    let first = scoped(&node, 0, vec![1; 32], "a");
    incr(&node, "a", &first).await.unwrap();
    let rotate = RetentionCommand::Rotate {
        epoch_ms: Some(1_000),
        keep_epochs: 2,
    };
    node.retention(b"db", rotate).await.unwrap().unwrap();
    let started = std::time::Instant::now();
    loop {
        node.maintain_retention(Duration::from_millis(200)).await;
        let state = node.retry_status(b"db").await.unwrap().retention.unwrap();
        if state.current_epoch >= 2 && state.collected {
            assert_eq!(state.min_epoch, state.current_epoch - 1);
            break;
        }
        assert!(started.elapsed() < Duration::from_secs(20), "{state:?}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // An epoch lasts at least its time: two advances took two seconds.
    assert!(started.elapsed() >= Duration::from_millis(2_000));
    let stats = node.retention_stats();
    assert!(stats.advances >= 2 && stats.walks >= 1, "{stats:?}");
    assert_eq!(receipts(&node).await, 0);
    assert_eq!(
        incr(&node, "a", &first).await,
        Err("epoch_retired".to_string())
    );
    node.shutdown().await.unwrap();
}

/// Session requests run once and replay until acknowledged; an
/// acknowledged prefix loses its results and never runs again, a gap
/// needs an explicit abandon, only the owner may act on the session,
/// and a closed session runs nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sessions_acknowledge_contiguous_prefixes_and_close() {
    let dir = tempfile::tempdir().unwrap();
    let node = retained(dir.path()).await;
    let ada = CallOptions {
        principal: Some(Principal::new("ada".to_string(), None, None).unwrap()),
        ..CallOptions::default()
    };
    let eve = CallOptions {
        principal: Some(Principal::new("eve".to_string(), None, None).unwrap()),
        ..CallOptions::default()
    };
    let id = [5u8; 16];
    let request = |caller: &CallOptions, sequence| MutationOptions {
        call: caller.clone(),
        ..scoped(&node, 0, session_intent(&id, sequence), "s")
    };
    // Requests never open a session.
    assert_eq!(
        incr(&node, "s", &request(&ada, 1)).await,
        Err("retry_session_unknown".to_string())
    );
    let opened = node
        .open_retry_session(b"db", "app", id, &ada)
        .await
        .unwrap()
        .unwrap();
    // Opening again answers the same session, to its owner only.
    let again = node.open_retry_session(b"db", "app", id, &ada).await;
    assert_eq!(again.unwrap().unwrap(), opened);
    let stolen = node.open_retry_session(b"db", "app", id, &eve).await;
    assert_eq!(stolen.unwrap().unwrap_err().code, "retry_session_forbidden");
    for sequence in 1..=3 {
        assert_eq!(
            incr(&node, "s", &request(&ada, sequence)).await,
            Ok(Value::Number(sequence as f64))
        );
    }
    // Another principal may not use the session.
    assert_eq!(
        incr(&node, "s", &request(&eve, 4)).await,
        Err("retry_session_forbidden".to_string())
    );
    assert_eq!(
        incr(&node, "s", &request(&ada, 1)).await,
        Ok(Value::Number(1.0))
    );
    let before = receipts(&node).await;
    let acked = node
        .acknowledge_retry_session(b"db", "app", id, Acknowledgement::through(2), &ada)
        .await
        .unwrap()
        .unwrap();
    let status = node.retry_session_status(b"db", "app", id, &ada).await;
    assert_eq!(status.unwrap().unwrap(), acked);
    assert_eq!(receipts(&node).await, before - 2);
    assert_eq!(
        incr(&node, "s", &request(&ada, 1)).await,
        Err("already_acknowledged".to_string())
    );
    assert_eq!(
        incr(&node, "s", &request(&ada, 3)).await,
        Ok(Value::Number(3.0))
    );
    let foreign = node
        .acknowledge_retry_session(b"db", "app", id, Acknowledgement::through(3), &eve)
        .await;
    assert_eq!(
        foreign.unwrap().unwrap_err().code,
        "retry_session_forbidden"
    );
    // Sequence 4 never ran: acknowledging past it takes an abandon.
    let gap = node
        .acknowledge_retry_session(b"db", "app", id, Acknowledgement::through(4), &ada)
        .await;
    assert_eq!(gap.unwrap().unwrap_err().code, "retry_ack_gap");
    node.acknowledge_retry_session(
        b"db",
        "app",
        id,
        Acknowledgement::through(4).abandoning(),
        &ada,
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        incr(&node, "s", &request(&ada, 4)).await,
        Err("already_acknowledged".to_string())
    );
    node.close_retry_session(b"db", "app", id, &ada)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        incr(&node, "s", &request(&ada, 5)).await,
        Err("retry_session_closed".to_string())
    );
    assert_eq!(read(&node, "s").await, 3.0);
    node.shutdown().await.unwrap();
}

fn principal(subject: &str) -> CallOptions {
    CallOptions {
        principal: Some(Principal::new(subject.to_string(), None, None).unwrap()),
        ..CallOptions::default()
    }
}

/// Collect `db` until its walk completes.
async fn collect_all(node: &Node) {
    for _ in 0..100 {
        let answer = node
            .retention(b"db", RetentionCommand::Collect { limit: 64 })
            .await
            .unwrap()
            .unwrap();
        let Value::Object(fields) = answer else {
            panic!("collect answered {answer:?}");
        };
        if fields.get("complete") == Some(&Value::Bool(true)) {
            return;
        }
    }
    panic!("the walk never completed");
}

/// What retention counts `(receipt_bytes, receipt_count, session_bytes,
/// session_count)`, as replicated and recounted from scratch.
async fn counted(node: &Node) -> ((u64, u64, u64, u64), (u64, u64, u64, u64)) {
    let state = node.retry_status(b"db").await.unwrap().retention.unwrap();
    let engine = node.engine().lock().await;
    let read = engine.store().begin_read().unwrap();
    let recounted = flower_engine::retention::recount(&read, b"db").unwrap();
    (
        (
            state.receipt_bytes,
            state.receipt_count,
            state.session_bytes,
            state.session_count,
        ),
        recounted,
    )
}

/// An acknowledgement's caller sets how much it may do: a strict one past
/// more sequences is refused, and an abandon fences sequences however far,
/// deleting at most its limit of results at once and leaving the rest to the
/// next walk. (Before, the limit was fixed at 256 for both.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn acknowledgements_take_a_limit_and_abandons_fence_any_distance() {
    let dir = tempfile::tempdir().unwrap();
    let node = retained(dir.path()).await;
    let ada = principal("ada");
    let id = [8u8; 16];
    node.open_retry_session(b"db", "app", id, &ada)
        .await
        .unwrap()
        .unwrap();
    let request = |sequence| MutationOptions {
        call: ada.clone(),
        ..scoped(&node, 0, session_intent(&id, sequence), "s")
    };
    for sequence in 1..=5 {
        incr(&node, "s", &request(sequence)).await.unwrap();
    }
    let ack = |ack: Acknowledgement| node.acknowledge_retry_session(b"db", "app", id, ack, &ada);
    let refused = ack(Acknowledgement::through(5).limit(4)).await.unwrap();
    assert_eq!(refused.unwrap_err().code, "retry_ack_budget");
    for limit in [0, MAX_ACK_LIMIT + 1] {
        let refused = ack(Acknowledgement::through(5).limit(limit)).await.unwrap();
        assert_eq!(refused.unwrap_err().code, codes::INVALID_VALUE, "{limit}");
    }
    ack(Acknowledgement::through(5).limit(5))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipts(&node).await, 0);
    for sequence in 6..=8 {
        incr(&node, "s", &request(sequence)).await.unwrap();
    }
    // Past three results and a million sequences that never ran, two
    // results at once.
    let far = 1_000_000;
    let acked = ack(Acknowledgement::through(far).abandoning().limit(2))
        .await
        .unwrap()
        .unwrap();
    let Value::Object(fields) = acked else {
        panic!("ack answered {acked:?}");
    };
    assert_eq!(fields.get("acknowledged"), Some(&Value::Number(far as f64)));
    assert_eq!(receipts(&node).await, 1);
    let state = node.retry_status(b"db").await.unwrap().retention.unwrap();
    assert!(!state.collected, "the walk takes the rest");
    collect_all(&node).await;
    assert_eq!(receipts(&node).await, 0);
    for sequence in [8, far] {
        assert_eq!(
            incr(&node, "s", &request(sequence)).await,
            Err("already_acknowledged".to_string())
        );
    }
    assert_eq!(
        incr(&node, "s", &request(far + 1)).await,
        Ok(Value::Number(9.0))
    );
    let (replicated, recounted) = counted(&node).await;
    assert_eq!(replicated, recounted);
    node.shutdown().await.unwrap();
}

/// Retention counts the bytes of what only it removes (receipts without an
/// expiry, a caller's own keys included once it is initialized, and session
/// rows) exactly, on every transition. Past `max_receipt_bytes` new results
/// are refused before they run, while replays, writes that keep no result and
/// everything that frees room go on; lowering the budget evicts nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_byte_budget_refuses_new_results_and_evicts_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let node = retained(dir.path()).await;
    let budget = |max: Option<u64>| {
        node.retention(
            b"db",
            RetentionCommand::Budget {
                max_receipt_bytes: max,
            },
        )
    };
    let first = scoped(&node, 0, vec![1; 32], "a");
    assert_eq!(incr(&node, "a", &first).await, Ok(Value::Number(1.0)));
    let keyed = |key: &[u8]| MutationOptions {
        idempotency_key: Some(key.to_vec()),
        ..MutationOptions::default()
    };
    assert_eq!(
        incr(&node, "b", &keyed(b"k1")).await,
        Ok(Value::Number(1.0))
    );
    // A caller's own key has no expiry under retention: the floor
    // collects it, and the budget counts it.
    {
        let engine = node.engine().lock().await;
        let read = engine.store().begin_read().unwrap();
        let identity = flower_engine::retry::unscoped_identity_key(b"k1");
        let stored = read.get_receipt(b"db", &identity).unwrap().unwrap();
        assert_eq!(Receipt::decode(&stored).unwrap().expires_at, 0);
    }
    let ada = principal("ada");
    node.open_retry_session(b"db", "app", [1; 16], &ada)
        .await
        .unwrap()
        .unwrap();
    let (replicated, recounted) = counted(&node).await;
    assert_eq!(replicated, recounted);
    assert_eq!((replicated.1, replicated.3), (2, 1));
    // Exactly full: no new result fits.
    let full = replicated.0 + replicated.2;
    let state = budget(Some(full)).await.unwrap().unwrap();
    let Value::Object(fields) = state else {
        panic!("budget answered {state:?}");
    };
    assert_eq!(
        fields.get("max_receipt_bytes"),
        Some(&Value::Number(full as f64))
    );
    let second = scoped(&node, 0, vec![2; 32], "a");
    for (name, options) in [("a", &second), ("b", &keyed(b"k2"))] {
        assert_eq!(
            incr(&node, name, options).await,
            Err("receipt_budget_exceeded".to_string()),
            "{name}"
        );
    }
    assert_eq!(read(&node, "a").await, 1.0, "a refused result never ran");
    let opened = node.open_retry_session(b"db", "app", [2; 16], &ada).await;
    assert_eq!(opened.unwrap().unwrap_err().code, "receipt_budget_exceeded");
    // Replays and writes that keep no result go on.
    assert_eq!(incr(&node, "a", &first).await, Ok(Value::Number(1.0)));
    assert_eq!(
        incr(&node, "b", &keyed(b"k1")).await,
        Ok(Value::Number(1.0))
    );
    assert_eq!(
        incr(&node, "c", &MutationOptions::default()).await,
        Ok(Value::Number(1.0))
    );
    // Lowering the budget below use evicts nothing.
    budget(Some(1)).await.unwrap().unwrap();
    assert_eq!(incr(&node, "a", &first).await, Ok(Value::Number(1.0)));
    assert_eq!(counted(&node).await.0, replicated);
    // Retiring epoch 0 and walking frees the room.
    advance(&node, 0, 1, 1).await.unwrap();
    collect_all(&node).await;
    let (freed, recounted) = counted(&node).await;
    assert_eq!(freed, recounted);
    assert_eq!(freed, (0, 0, 0, 0));
    budget(Some(full)).await.unwrap().unwrap();
    let third = scoped(&node, 1, vec![3; 32], "a");
    assert_eq!(incr(&node, "a", &third).await, Ok(Value::Number(2.0)));
    // Without a bound, anything fits.
    budget(None).await.unwrap().unwrap();
    for index in 4..12u8 {
        let options = scoped(&node, 1, vec![index; 32], "a");
        incr(&node, "a", &options).await.unwrap();
    }
    let (replicated, recounted) = counted(&node).await;
    assert_eq!(replicated, recounted);
    assert_eq!(replicated.1, 9);
    node.shutdown().await.unwrap();
}

/// With a retry window set (as `serve` does by default), the leader gives
/// every database nobody configured retention for the default once: epochs of
/// an eighth of the window, nine kept. An operator's settings always win, a
/// database keeping too many receipts is left to an operator, and system
/// databases get none.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_leader_gives_unconfigured_databases_the_default_retention() {
    let dir = tempfile::tempdir().unwrap();
    let node = single_node(dir.path()).await;
    assert_ne!(node.establish_incarnation().await.unwrap(), 0);
    assert_eq!(default_rotation(Duration::from_millis(10)), (1_000, 9));
    assert_eq!(
        default_rotation(Duration::from_millis(DEFAULT_RETRY_WINDOW_MS)),
        (10_800_000, 9)
    );
    // No window: nothing is initialized, and sessions need retention.
    node.maintain_retention(Duration::from_millis(100)).await;
    assert_eq!(node.retry_status(b"db").await.unwrap().retention, None);
    let ada = principal("ada");
    let refused = node.open_retry_session(b"db", "app", [3; 16], &ada).await;
    assert_eq!(
        refused.unwrap().unwrap_err().code,
        "retention_not_initialized"
    );
    // A database keeping too many receipts to count while holding the
    // writer (written here behind the log's back, as years of traffic
    // would have).
    node.deploy(b"big", "app", [9u8; 32], b"wasm", &counters::manifest())
        .await
        .unwrap();
    {
        let engine = node.engine().lock().await;
        let write = engine.store().begin_write().unwrap();
        let receipt = Receipt {
            result: Some(flower_engine::retry::Outcome::Value(b"1".to_vec())),
            fingerprint: [0; 32],
            floor: 0,
            expires_at: 0,
            committed: None,
        }
        .encode()
        .unwrap();
        for index in 0..=DEFAULT_MAX_RECEIPTS as u64 {
            let identity = flower_engine::retry::unscoped_identity_key(&index.to_be_bytes());
            write.put_receipt(b"big", &identity, &receipt).unwrap();
        }
        write.commit().unwrap();
    }
    node.set_retry_window(Some(Duration::from_secs(16)));
    node.maintain_retention(Duration::from_millis(100)).await;
    let state = node.retry_status(b"db").await.unwrap().retention.unwrap();
    assert_eq!(
        state.rotation,
        Some(Rotation {
            epoch_ms: 2_000,
            keep_epochs: 9
        })
    );
    assert_eq!((state.current_epoch, state.min_epoch), (0, 0));
    assert_eq!(node.retention_stats().defaults, 1);
    assert_eq!(node.retry_status(b"big").await.unwrap().retention, None);
    for system in [&b"$cluster"[..], b"$catalog"] {
        assert_eq!(node.retry_status(system).await.unwrap().retention, None);
    }
    node.open_retry_session(b"db", "app", [3; 16], &ada)
        .await
        .unwrap()
        .unwrap();
    // An operator's settings win, now and later: maintenance and the
    // default command change nothing once anyone configured retention.
    let stop = RetentionCommand::Rotate {
        epoch_ms: None,
        keep_epochs: 3,
    };
    node.retention(b"db", stop).await.unwrap().unwrap();
    node.set_retry_window(Some(Duration::from_secs(80)));
    node.maintain_retention(Duration::from_millis(100)).await;
    let again = RetentionCommand::Default {
        epoch_ms: 10_000,
        keep_epochs: 9,
    };
    node.retention(b"db", again).await.unwrap().unwrap();
    let state = node.retry_status(b"db").await.unwrap().retention.unwrap();
    assert_eq!(state.rotation, None);
    assert_eq!(node.retention_stats().defaults, 1);
    node.shutdown().await.unwrap();
}
