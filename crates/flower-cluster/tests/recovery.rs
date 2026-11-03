//! Startup recovery of a log that runs past the restored state: the
//! remembered committed prefix re-applies before the member starts, and
//! the member serves nothing from its state until a commit confirmed
//! since covers the rest (`flower_cluster::recovery`).

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use flower_cluster::logstore::LogStore;
use flower_cluster::network::{InProcessFactory, Registry};
use flower_cluster::node::{Node, NodeError};
use flower_cluster::recovery::RECOVERING;
use flower_cluster::statemachine::{RuntimeFactory, StateMachine};
use flower_cluster::types::{ClusterWrite, NodeId, TypeConfig};
use flower_engine::runtime::Runtime;
use flower_sdk::Value;
use openraft::storage::{RaftLogReader, RaftLogStorage, RaftLogStorageExt};
use openraft::{BasicNode, Entry, EntryPayload, LogId, Membership};

/// These members run no modules.
struct NoRuntimes;

impl RuntimeFactory for NoRuntimes {
    fn build(
        &self,
        _db: &[u8],
        module: &str,
        _: &[u8],
        _: &Value,
    ) -> Result<Box<dyn Runtime>, String> {
        Err(format!("no runtime for `{module}` in this test"))
    }
}

async fn open(dir: &Path) -> (Registry, Node) {
    let registry = Registry::new();
    let node = Node::open(
        1,
        dir,
        Arc::new(openraft::Config::default().validate().unwrap()),
        InProcessFactory::new(registry.clone()),
        Arc::new(NoRuntimes),
    )
    .await
    .unwrap();
    registry.register(1, node.raft().clone());
    (registry, node)
}

async fn eventually(what: &str, mut check: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while !check() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// A single member that committed a few entries and stopped cleanly.
async fn stopped_member(dir: &Path) {
    let (_registry, node) = open(dir).await;
    node.initialize(BTreeSet::from([1])).await.unwrap();
    eventually("a leader", || node.status().leader == Some(1)).await;
    for stamp in 0..3 {
        node.raft()
            .client_write(ClusterWrite::Incarnation {
                incarnation: 7,
                proposed_ms: stamp,
            })
            .await
            .unwrap();
    }
    node.shutdown().await.unwrap();
}

/// The stopped member's log, and its last entry's ID.
async fn log_of(dir: &Path) -> (LogStore, LogId<NodeId>) {
    let mut logs = LogStore::open(&dir.join("raft.redb")).unwrap();
    let last = logs.get_log_state().await.unwrap().last_log_id.unwrap();
    (logs, last)
}

fn next(last: LogId<NodeId>, by: u64) -> LogId<NodeId> {
    LogId::new(last.leader_id, last.index + by)
}

fn blank(id: LogId<NodeId>) -> Entry<TypeConfig> {
    Entry {
        log_id: id,
        payload: EntryPayload::Blank,
    }
}

async fn applied(node: &Node) -> Option<u64> {
    node.with_engine(|engine| {
        StateMachine::group_applied(engine)
            .map(|id| id.map(|id| id.index))
            .map_err(|failure| NodeError::Failure {
                code: failure.code,
                message: failure.message,
            })
    })
    .await
    .unwrap()
}

/// A member stopped with committed entries it hadn't applied, behind an
/// uncommitted tail its group never confirmed: the committed ones apply
/// before `Node::open` returns, and nothing is served from the state
/// while the tail is unconfirmed.
#[tokio::test(flavor = "multi_thread")]
async fn committed_entries_reapply_on_open_and_an_unconfirmed_tail_fences_serving() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("n1");
    stopped_member(&path).await;
    let (mut logs, last) = log_of(&path).await;
    // What a leader of a three-voter group left: a membership naming two
    // members that never come back, an entry, both committed (the store
    // recorded the commit with its next write), then one more that never
    // committed.
    let voters = BTreeMap::from([1, 2, 3].map(|id| (id, BasicNode::default())));
    let membership = Entry {
        log_id: next(last, 1),
        payload: EntryPayload::Membership(Membership::new(
            vec![voters.keys().copied().collect()],
            voters,
        )),
    };
    logs.blocking_append([membership, blank(next(last, 2))])
        .await
        .unwrap();
    logs.save_committed(Some(next(last, 2))).await.unwrap();
    logs.blocking_append([blank(next(last, 3))]).await.unwrap();
    logs.stop_purging().await;
    drop(logs);

    let (_registry, node) = open(&path).await;
    assert_eq!(applied(&node).await, Some(last.index + 2));
    let reason = node.recovering().expect("the member recovers its tail");
    assert!(
        reason.contains(&format!("reaches {}", last.index + 3)),
        "{reason}"
    );
    match node.sync_since(0, 100) {
        Err(NodeError::Failure { code, .. }) => assert_eq!(code, RECOVERING),
        other => panic!("sync served a recovering member's state: {other:?}"),
    }
    let mut readiness = node.readiness().await;
    for _ in 0..200 {
        if readiness.leader.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
        readiness = node.readiness().await;
    }
    assert!(!readiness.ready);
    let why = readiness.reason.unwrap_or_default();
    assert!(why.starts_with("recovering"), "{why}");
    // The log store reads back what OpenRaft re-applied to.
    assert_eq!(
        node.logs().committed().unwrap().map(|id| id.index),
        Some(last.index + 2)
    );
    let entries = node
        .logs()
        .clone()
        .try_get_log_entries(last.index + 1..)
        .await
        .unwrap();
    assert_eq!(entries.len(), 3);
    node.shutdown().await.unwrap();
}

/// A tail the member can confirm on its own (its group is itself)
/// commits, applies and clears the fence; a clean restart has none.
#[tokio::test(flavor = "multi_thread")]
async fn a_confirmed_tail_clears_the_fence_and_a_clean_restart_has_none() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("n1");
    stopped_member(&path).await;
    {
        let (_registry, node) = open(&path).await;
        assert!(
            node.recovering().is_none(),
            "a clean restart recovers nothing"
        );
        node.sync_since(0, 100).unwrap();
        node.shutdown().await.unwrap();
    }
    let (mut logs, last) = log_of(&path).await;
    logs.blocking_append([blank(next(last, 1)), blank(next(last, 2))])
        .await
        .unwrap();
    logs.stop_purging().await;
    drop(logs);
    let (_registry, node) = open(&path).await;
    eventually("the fence clears", || node.recovering().is_none()).await;
    assert!(node.status().last_applied.unwrap_or(0) >= last.index + 2);
    let batch = node.sync_since(0, 100).unwrap();
    assert!(batch.current >= last.index + 2);
    let mut ready = false;
    for _ in 0..200 {
        if node.readiness().await.ready {
            ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(ready, "{:?}", node.readiness().await);
    node.ensure_leader().await.unwrap();
    node.shutdown().await.unwrap();
}
