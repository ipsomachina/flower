//! The writer: every client write queues, prepares on the state its
//! predecessors staged, and commits in groups as one log entry.

mod counters;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use counters::{Counters, config, key, manifest, read, single_node, value, wait_leader};
use flower_cluster::network::{InProcessFactory, Registry};
use flower_cluster::node::Node;
use flower_cluster::types::ClusterWrite;
use flower_engine::engine::{CallOptions, MutationOptions};
use flower_sdk::Value;
use openraft::EntryPayload;

/// Concurrent read-modify-write mutations on one key: each must see
/// every write committed or queued before it, so none is lost and the
/// answers are exactly 1..=N.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_increments_never_lose_updates() {
    let dir = tempfile::tempdir().unwrap();
    let node = Arc::new(single_node(dir.path()).await);
    const CALLS: usize = 64;
    let calls: Vec<_> = (0..CALLS)
        .map(|_| {
            let node = node.clone();
            tokio::spawn(async move {
                node.mutate(
                    b"db",
                    "app",
                    "incr",
                    &key("hot"),
                    &MutationOptions::default(),
                )
                .await
                .unwrap()
            })
        })
        .collect();
    let mut answers = Vec::new();
    let mut revisions = Vec::new();
    for call in calls {
        let response = call.await.unwrap();
        answers.push(match value(&response.outcome) {
            Value::Number(count) => count as usize,
            other => panic!("incr answered {other:?}"),
        });
        revisions.push(response.revision);
    }
    assert_eq!(read(&node, "hot").await, CALLS as f64);
    answers.sort_unstable();
    assert_eq!(answers, (1..=CALLS).collect::<Vec<_>>());
    // Each call has a revision of its own.
    revisions.sort_unstable();
    revisions.dedup();
    assert_eq!(revisions.len(), CALLS);
    Arc::into_inner(node).unwrap().shutdown().await.unwrap();
}

/// Writes queued while a group commits share the next log entry: many
/// requests, one entry, each with its own revision and answer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queued_writes_share_one_log_entry() {
    let dir = tempfile::tempdir().unwrap();
    let node = Arc::new(single_node(dir.path()).await);
    let before = node.status().last_log.unwrap_or(0);
    const CALLS: usize = 48;
    let calls: Vec<_> = (0..CALLS)
        .map(|index| {
            let node = node.clone();
            tokio::spawn(async move {
                let name = format!("k{index}");
                node.mutate(
                    b"db",
                    "app",
                    "incr",
                    &key(&name),
                    &MutationOptions::default(),
                )
                .await
                .unwrap()
            })
        })
        .collect();
    for call in calls {
        let response = call.await.unwrap();
        assert_eq!(value(&response.outcome), Value::Number(1.0));
    }
    let after = node.status().last_log.unwrap_or(0);
    let entries = node.logs().read_entries(before + 1, after + 1).unwrap();
    let mut batched = 0;
    for entry in &entries {
        if let EntryPayload::Normal(ClusterWrite::Batch { items, .. }) = &entry.payload {
            batched += items.len();
        }
    }
    assert_eq!(batched, CALLS, "every write rode in a batch entry");
    assert!(
        entries.len() < CALLS,
        "{CALLS} writes took {} entries",
        entries.len()
    );
    Arc::into_inner(node).unwrap().shutdown().await.unwrap();
}

/// Fire `CALLS` concurrent increments of `name` at `node`, returning
/// their answers sorted.
async fn increments(node: &Arc<Node>, name: &str, calls: usize) -> Vec<usize> {
    let tasks: Vec<_> = (0..calls)
        .map(|_| {
            let (node, name) = (node.clone(), name.to_string());
            tokio::spawn(async move {
                let response = node
                    .mutate(
                        b"db",
                        "app",
                        "incr",
                        &key(&name),
                        &MutationOptions::default(),
                    )
                    .await
                    .unwrap();
                match value(&response.outcome) {
                    Value::Number(count) => count as usize,
                    other => panic!("incr answered {other:?}"),
                }
            })
        })
        .collect();
    let mut answers = Vec::new();
    for task in tasks {
        answers.push(task.await.unwrap());
    }
    answers.sort_unstable();
    answers
}

/// Entries proposed around the writer land between the state a batch
/// was prepared on and the batch: it goes stale on every member, and
/// the writer prepares its writes again on the new state, so callers
/// see neither the staleness nor a lost update.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn interleaved_entries_make_batches_stale_and_writes_prepare_again() {
    let dir = tempfile::tempdir().unwrap();
    let node = Arc::new(single_node(dir.path()).await);
    let mut total = 0;
    for round in 0..20 {
        // A few entries land while the writes commit, as deployments
        // would: fewer than a write's attempts, so every write lands.
        let interloper = {
            let node = node.clone();
            tokio::spawn(async move {
                for _ in 0..3 {
                    // An incarnation entry changes no record, but it is a
                    // data entry the writer did not prepare on.
                    node.raft()
                        .client_write(ClusterWrite::Incarnation {
                            incarnation: 1,
                            proposed_ms: 0,
                        })
                        .await
                        .unwrap();
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
            })
        };
        const CALLS: usize = 32;
        let answers = increments(&node, "hot", CALLS).await;
        interloper.await.unwrap();
        assert_eq!(answers, (total + 1..=total + CALLS).collect::<Vec<_>>());
        total += CALLS;
        assert_eq!(read(&node, "hot").await, total as f64);
        if node.writer_stats().stale > 0 {
            break;
        }
        assert!(
            round < 19,
            "no batch ever went stale: {:?}",
            node.writer_stats()
        );
    }
    Arc::into_inner(node).unwrap().shutdown().await.unwrap();
}

/// Three members: the leader's batches replicate and every follower
/// applies the same writes in the same order, revisions included.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn followers_apply_the_same_batches() {
    let dir = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let mut nodes = Vec::new();
    for id in 1..=3u64 {
        let node = Node::open(
            id,
            &dir.path().join(format!("n{id}")),
            config(),
            InProcessFactory::new(registry.clone()),
            Arc::new(Counters),
        )
        .await
        .unwrap();
        registry.register_peer(node.peer_service());
        nodes.push(Arc::new(node));
    }
    nodes[0]
        .initialize(BTreeSet::from([1, 2, 3]))
        .await
        .unwrap();
    let refs: Vec<&Node> = nodes.iter().map(Arc::as_ref).collect();
    let leader_id = wait_leader(&refs).await;
    let leader = nodes[(leader_id - 1) as usize].clone();
    leader
        .deploy(b"db", "app", [9u8; 32], b"wasm", &manifest())
        .await
        .unwrap();
    const CALLS: usize = 40;
    let answers = increments(&leader, "hot", CALLS).await;
    assert_eq!(answers, (1..=CALLS).collect::<Vec<_>>());
    assert!(
        leader.writer_stats().largest > 1,
        "{:?}",
        leader.writer_stats()
    );
    for node in &nodes {
        // A fresh read on a follower applies through the leader's commit.
        let (count, revision) = node
            .query_at(b"db", "app", "read", &key("hot"), &CallOptions::default())
            .await
            .unwrap();
        assert_eq!(count, Value::Number(CALLS as f64), "node {}", node.id());
        assert_eq!(revision, CALLS as u64, "node {}", node.id());
    }
    drop(leader);
    for node in nodes {
        Arc::into_inner(node).unwrap().shutdown().await.unwrap();
    }
}
