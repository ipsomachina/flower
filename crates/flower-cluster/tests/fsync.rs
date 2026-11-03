//! `FLOWER_FSYNC=off` on a member: boot markers while open, settled on a
//! clean shutdown, and members an earlier boot left unsynced rejoining
//! their group (`flower_cluster::fsync`, `flower_cluster::rejoin`).

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use flower_cluster::fsync::{self, Durability};
use flower_cluster::network::{InProcessFactory, Registry};
use flower_cluster::node::{Node, NodeError};
use flower_cluster::statemachine::RuntimeFactory;
use flower_cluster::types::ClusterWrite;
use flower_engine::runtime::Runtime;
use flower_sdk::Value;

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

fn at(fsync: bool, boot: &str) -> Durability {
    Durability {
        fsync,
        boot: Some(boot.to_string()),
    }
}

async fn open(dir: &Path, durability: Durability) -> Result<(Registry, Node), NodeError> {
    let registry = Registry::new();
    let node = Node::open_with_durability(
        1,
        dir,
        Arc::new(openraft::Config::default().validate().unwrap()),
        InProcessFactory::new(registry.clone()),
        Arc::new(NoRuntimes),
        durability,
    )
    .await?;
    registry.register(1, node.raft().clone());
    Ok((registry, node))
}

async fn wait_leader(node: &Node) {
    for _ in 0..500 {
        if node.status().leader == Some(1) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("no leader");
}

fn markers(dir: &Path) -> [bool; 2] {
    ["group.redb", "raft.redb"].map(|file| fsync::marker(&dir.join(file)).exists())
}

#[tokio::test(flavor = "multi_thread")]
async fn unsynced_members_mark_their_files_settle_on_shutdown_and_rejoin_after_an_earlier_boot() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("n1");
    let (_registry, node) = open(&path, at(false, "boot-a")).await.unwrap();
    assert_eq!(markers(&path), [true, true]);
    node.initialize(BTreeSet::from([1])).await.unwrap();
    wait_leader(&node).await;
    let written = node
        .raft()
        .client_write(ClusterWrite::Incarnation {
            incarnation: 7,
            proposed_ms: 1,
        })
        .await
        .unwrap()
        .log_id
        .index;
    node.shutdown().await.unwrap();
    assert_eq!(markers(&path), [false, false], "a clean shutdown syncs");

    // Within the same boot the kernel kept every write, even after a
    // crash with fsync off left the markers.
    for file in ["group.redb", "raft.redb"] {
        fsync::mark(&path.join(file), Some("boot-a")).unwrap();
    }
    let (_registry, node) = open(&path, at(false, "boot-a")).await.unwrap();
    assert!(!node.rejoining());
    wait_leader(&node).await;
    assert!(node.status().last_applied.unwrap_or(0) >= written);
    node.shutdown().await.unwrap();
    assert_eq!(markers(&path), [false, false]);

    // An operator who trusts the files of a group of one removes the
    // markers before it starts again.
    for file in ["group.redb", "raft.redb"] {
        fsync::mark(&path.join(file), Some("boot-a")).unwrap();
        std::fs::remove_file(fsync::marker(&path.join(file))).unwrap();
    }
    let (_registry, node) = open(&path, at(true, "boot-b")).await.unwrap();
    assert!(!node.rejoining());
    wait_leader(&node).await;
    node.shutdown().await.unwrap();

    // A crash with fsync off, then the machine restarts: either mode
    // opens the member rejoining, records the files as this boot's once
    // that is durable, and keeps it rejoining across restarts.
    for (durability, kept) in [(at(false, "boot-b"), true), (at(true, "boot-c"), false)] {
        for file in ["group.redb", "raft.redb"] {
            fsync::mark(&path.join(file), Some("boot-a")).unwrap();
        }
        let (_registry, node) = open(&path, durability.clone()).await.unwrap();
        assert!(node.rejoining());
        assert_eq!(markers(&path), [kept, kept]);
        for file in ["group.redb", "raft.redb"] {
            assert!(!fsync::lost_writes(&path.join(file), &durability).unwrap());
        }
        // Alone, it cannot catch up: it never elects itself.
        tokio::time::sleep(Duration::from_millis(900)).await;
        assert_eq!(node.status().leader, None);
        node.shutdown().await.unwrap();
        let (_registry, node) = open(&path, durability).await.unwrap();
        assert!(node.rejoining());
        node.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_member_whose_machine_lost_unsynced_writes_rejoins_its_group() {
    // A follower with fsync off loses its machine: back up, it holds a
    // log that may lack entries it acknowledged. It votes for no one
    // until it caught up with its leader, then serves as before.
    let dir = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let config = Arc::new(openraft::Config::default().validate().unwrap());
    let mut nodes = Vec::new();
    for id in 1..=3u64 {
        let durability = if id == 3 {
            at(false, "boot-a")
        } else {
            at(true, "boot-a")
        };
        let node = Node::open_with_durability(
            id,
            &dir.path().join(format!("n{id}")),
            config.clone(),
            InProcessFactory::new(registry.clone()),
            Arc::new(NoRuntimes),
            durability,
        )
        .await
        .unwrap();
        registry.register_peer(node.peer_service());
        nodes.push(node);
    }
    nodes[0]
        .initialize(BTreeSet::from([1, 2, 3]))
        .await
        .unwrap();
    wait_leader(&nodes[0]).await;
    for node in &nodes {
        for _ in 0..500 {
            if !node.rejoining() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(!node.rejoining());
    }
    let written = nodes[0]
        .raft()
        .client_write(ClusterWrite::Incarnation {
            incarnation: 7,
            proposed_ms: 1,
        })
        .await
        .unwrap()
        .log_id
        .index;

    // Node 3's process dies uncleanly: its markers stay. Then its
    // machine restarts.
    let three = nodes.pop().unwrap();
    registry.unregister(3);
    let path = dir.path().join("n3");
    three.shutdown().await.unwrap();
    for file in ["group.redb", "raft.redb"] {
        fsync::mark(&path.join(file), Some("boot-a")).unwrap();
    }
    // Its own requests can't reach the leader yet: it stays rejoining.
    let outbound = Registry::new();
    let three = Node::open_with_durability(
        3,
        &path,
        config,
        InProcessFactory::new(outbound.clone()),
        Arc::new(NoRuntimes),
        at(false, "boot-b"),
    )
    .await
    .unwrap();
    assert!(three.rejoining());
    registry.register_peer(three.peer_service());
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(three.rejoining());
    assert!(!three.readiness().await.ready);
    // Once it can ask its leader for a fence, it caught up.
    for node in &nodes {
        outbound.register_peer(node.peer_service());
    }
    for _ in 0..500 {
        if !three.rejoining() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(!three.rejoining(), "the member never caught up");
    assert!(three.status().last_applied.unwrap_or(0) >= written);
    assert!(three.readiness().await.ready);
    three.shutdown().await.unwrap();
    for node in nodes {
        node.shutdown().await.unwrap();
    }
}
