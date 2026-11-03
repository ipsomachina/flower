//! Snapshot scheduling and storage on a member: soft triggers, and
//! images built, sent and received through files.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use flower_cluster::logstore::LogStore;
use flower_cluster::network::{InProcessFactory, Registry};
use flower_cluster::node::Node;
use flower_cluster::snapshot_policy::SnapshotPolicy;
use flower_cluster::statemachine::{RuntimeFactory, StateMachine};
use flower_cluster::types::{ClusterWrite, TypeConfig};
use flower_engine::engine::Engine;
use flower_engine::runtime::Runtime;
use flower_engine::storage::GroupStore;
use flower_sdk::Value;
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use redb::ReadableDatabase;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

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

/// OpenRaft's own trigger stays far away: only soft triggers build.
const ENTRY_TRIGGER: u64 = 100_000;

async fn single_member(dir: &std::path::Path) -> (Registry, Arc<Node>) {
    let registry = Registry::new();
    let config = openraft::Config {
        snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(ENTRY_TRIGGER),
        ..openraft::Config::default()
    };
    let node = Node::open(
        1,
        dir,
        Arc::new(config.validate().unwrap()),
        InProcessFactory::new(registry.clone()),
        Arc::new(NoRuntimes),
    )
    .await
    .unwrap();
    registry.register(1, node.raft().clone());
    node.initialize(BTreeSet::from([1])).await.unwrap();
    eventually("a leader", || node.status().leader == Some(1)).await;
    (registry, Arc::new(node))
}

/// Commit `count` entries that change no database.
async fn write(node: &Node, count: u64) {
    for stamp in 0..count {
        node.raft()
            .client_write(ClusterWrite::Incarnation {
                incarnation: 7,
                proposed_ms: stamp,
            })
            .await
            .unwrap();
    }
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

fn snapshot_index(node: &Node) -> Option<u64> {
    node.raft().metrics().borrow().snapshot.map(|id| id.index)
}

/// Start the scheduler; the sender stops it.
fn schedule(node: &Arc<Node>, policy: SnapshotPolicy) -> tokio::sync::watch::Sender<bool> {
    let (stop, stopped) = tokio::sync::watch::channel(false);
    let node = node.clone();
    tokio::spawn(async move { node.run_snapshots(policy, stopped).await });
    stop
}

#[tokio::test(flavor = "multi_thread")]
async fn applied_bytes_trigger_a_snapshot_long_before_the_entry_count() {
    let dir = tempfile::tempdir().unwrap();
    let (_registry, node) = single_member(dir.path()).await;
    write(&node, 5).await;
    assert_eq!(snapshot_index(&node), None);
    let policy = SnapshotPolicy {
        after_bytes: 1,
        max_age: Duration::ZERO,
        check: Duration::from_millis(10),
        duty_percent: 100,
    };
    let stop = schedule(&node, policy);
    eventually("a snapshot for bytes", || snapshot_index(&node).is_some()).await;
    let first = snapshot_index(&node).unwrap();
    assert!(first < ENTRY_TRIGGER);
    let stats = node.snapshot_policy_stats(&policy);
    assert!(stats.builds >= 1, "{stats:?}");
    assert_eq!(stats.snapshot, Some(first));
    // Nothing applied since: nothing more is due, however many checks.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(snapshot_index(&node), Some(first));
    // New entries are due again.
    write(&node, 1).await;
    eventually("a second snapshot", || snapshot_index(&node) > Some(first)).await;
    let _ = stop.send(true);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_old_snapshot_rebuilds_for_age_once_anything_applied() {
    let dir = tempfile::tempdir().unwrap();
    let (_registry, node) = single_member(dir.path()).await;
    write(&node, 2).await;
    let policy = SnapshotPolicy {
        after_bytes: 0,
        max_age: Duration::from_millis(50),
        check: Duration::from_millis(10),
        duty_percent: 100,
    };
    let stop = schedule(&node, policy);
    eventually("a snapshot for age", || snapshot_index(&node).is_some()).await;
    let first = snapshot_index(&node).unwrap();
    write(&node, 1).await;
    eventually("a rebuild for age", || snapshot_index(&node) > Some(first)).await;
    let _ = stop.send(true);
}

#[tokio::test(flavor = "multi_thread")]
async fn disabled_soft_triggers_leave_snapshots_to_the_entry_count() {
    let dir = tempfile::tempdir().unwrap();
    let (_registry, node) = single_member(dir.path()).await;
    write(&node, 5).await;
    let policy = SnapshotPolicy {
        after_bytes: 0,
        max_age: Duration::ZERO,
        check: Duration::from_millis(10),
        duty_percent: 100,
    };
    let stop = schedule(&node, policy);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(snapshot_index(&node), None);
    let _ = stop.send(true);
}

/// A state machine over its own group and Raft files in `dir`.
type Shared = Arc<tokio::sync::Mutex<Engine>>;

fn machine(dir: &std::path::Path) -> (StateMachine, LogStore, Shared) {
    std::fs::create_dir_all(dir).unwrap();
    let group = GroupStore::create(dir.join("group.redb")).unwrap();
    let logs = LogStore::open(&dir.join("raft.redb")).unwrap();
    let (commits, _) = tokio::sync::broadcast::channel(8);
    let engine = Arc::new(tokio::sync::Mutex::new(Engine::new(group)));
    let machine = StateMachine::new(engine.clone(), logs.clone(), Arc::new(NoRuntimes), commits);
    (machine, logs, engine)
}

/// A record value of about 4 KB that LZ4 cannot shrink much.
fn value(key: u32) -> Vec<u8> {
    let mut state = u64::from(key).wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    let text: String = (0..4000)
        .map(|_| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            char::from(b"0123456789abcdef"[(state >> 60) as usize])
        })
        .collect();
    format!(r#"{{"v":"{text}"}}"#).into_bytes()
}

/// `count` records of [`value`] in database `db`, registered.
fn fill(engine: &Shared, db: &[u8], count: u32) {
    let engine = engine.blocking_lock();
    let write = engine.store().begin_write().unwrap();
    for key in 0..count {
        write
            .put_record(db, 1, format!("\"{key}\"").as_bytes(), &value(key))
            .unwrap();
    }
    write.commit().unwrap();
    StateMachine::register_database(engine.store(), db).unwrap();
}

fn blank(index: u64) -> openraft::Entry<TypeConfig> {
    openraft::Entry {
        log_id: openraft::LogId::new(openraft::CommittedLeaderId::new(1, 1), index),
        payload: openraft::EntryPayload::Blank,
    }
}

/// The bytes the Raft file holds for its snapshot.
fn recorded_bytes(raft: &std::path::Path) -> (usize, bool) {
    let table = redb::TableDefinition::<&[u8], &[u8]>::new("raft_snapshot");
    let db = redb::Database::open(raft).unwrap();
    let read = db.begin_read().unwrap();
    let table = read.open_table(table).unwrap();
    let image = table
        .get(b"image".as_slice())
        .unwrap()
        .map(|row| row.value().len());
    let legacy = table.get(b"current".as_slice()).unwrap().is_some();
    (image.unwrap_or(0), legacy)
}

/// A snapshot builds into an image file and travels in segments into a
/// file on the receiver, which installs it one database at a time; the
/// Raft files record metadata and a file name only, however large the
/// image. A truncated or padded image is refused before anything is
/// recorded or installed.
#[tokio::test(flavor = "multi_thread")]
async fn images_spool_through_files_and_bad_ones_change_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let (mut leader, _leader_logs, leader_engine) = machine(&dir.path().join("leader"));
    tokio::task::block_in_place(|| {
        fill(&leader_engine, b"a", 200);
        fill(&leader_engine, b"b", 100);
    });
    leader.apply([blank(1), blank(2)]).await.unwrap();
    let mut built = leader
        .get_snapshot_builder()
        .await
        .build_snapshot()
        .await
        .unwrap();
    assert_eq!(built.meta.last_log_id.map(|id| id.index), Some(2));
    let mut wire = Vec::new();
    built.snapshot.read_to_end(&mut wire).await.unwrap();
    assert!(wire.len() > 2 << 20, "{} bytes", wire.len());
    let images: Vec<_> = std::fs::read_dir(dir.path().join("leader/snapshots"))
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect();
    assert_eq!(images.len(), 1, "{images:?}");
    assert!(images[0].ends_with(".snapshot"), "{images:?}");

    let follower_dir = dir.path().join("follower");
    {
        let (mut follower, logs, follower_engine) = machine(&follower_dir);
        let mut receiving = follower.begin_receiving_snapshot().await.unwrap();
        for segment in wire.chunks(17_113) {
            receiving.write_all(segment).await.unwrap();
        }
        follower
            .install_snapshot(&built.meta, receiving)
            .await
            .unwrap();
        assert!(logs.pending_install().unwrap().is_none());
        let installed = || {
            let engine = follower_engine.blocking_lock();
            let read = engine.store().begin_read().unwrap();
            let rows = (0..200)
                .filter(|key| {
                    read.get_record(b"a", 1, format!("\"{key}\"").as_bytes())
                        .unwrap()
                        .as_deref()
                        == Some(value(*key).as_slice())
                })
                .count();
            (rows, StateMachine::group_applied(&engine).unwrap())
        };
        let before = tokio::task::block_in_place(installed);
        assert_eq!(before.0, 200);
        assert_eq!(before.1.map(|id| id.index), Some(2));
        // A byte that changed in transit leaves well-formed JSON behind:
        // only the checksum the snapshot ID carries tells.
        let mut corrupt = wire.clone();
        let middle = wire.len() / 2;
        let at = middle
            + wire[middle..]
                .windows(3)
                .position(|window| window == b",1,")
                .expect("a payload byte of 1");
        corrupt[at + 1] = b'2';
        serde_json::from_slice::<serde_json::Value>(&corrupt).expect("still well formed");
        // Truncated, padded and corrupt images fail their check: no
        // marker, the same current snapshot, the same state.
        for bad in [
            &wire[..wire.len() - 1],
            &[wire.as_slice(), b"junk"].concat(),
            &corrupt,
        ] {
            let mut receiving = follower.begin_receiving_snapshot().await.unwrap();
            receiving.write_all(bad).await.unwrap();
            assert!(
                follower
                    .install_snapshot(&built.meta, receiving)
                    .await
                    .is_err()
            );
            assert!(logs.pending_install().unwrap().is_none());
            assert_eq!(logs.snapshot_meta().unwrap(), Some(built.meta.clone()));
            assert_eq!(tokio::task::block_in_place(installed), before);
        }
        let current = follower.get_current_snapshot().await.unwrap().unwrap();
        assert_eq!(current.meta, built.meta);
    }
    // The Raft file holds a small record, not the image; reopening finds
    // the same snapshot and leaves no stray files.
    let (record, legacy) = recorded_bytes(&follower_dir.join("raft.redb"));
    assert!(record > 0 && record < 1024, "{record} bytes");
    assert!(!legacy);
    let (mut reopened, logs, _engine) = machine(&follower_dir);
    assert_eq!(logs.snapshot_meta().unwrap(), Some(built.meta.clone()));
    let mut current = reopened.get_current_snapshot().await.unwrap().unwrap();
    let mut bytes = Vec::new();
    current.snapshot.read_to_end(&mut bytes).await.unwrap();
    assert_eq!(bytes, wire);
    let files = std::fs::read_dir(follower_dir.join("snapshots"))
        .unwrap()
        .count();
    assert_eq!(files, 1);
}

/// An image may carry another key catalog: installing one counts as a
/// key change on the member, and watch hubs hear of it once the member's
/// key runtime serves the installed catalog.
#[tokio::test(flavor = "multi_thread")]
async fn installs_count_as_key_changes() {
    let dir = tempfile::tempdir().unwrap();
    let (mut leader, _leader_logs, _leader_engine) = machine(&dir.path().join("leader"));
    leader.apply([blank(1)]).await.unwrap();
    let mut built = leader
        .get_snapshot_builder()
        .await
        .build_snapshot()
        .await
        .unwrap();
    let mut wire = Vec::new();
    built.snapshot.read_to_end(&mut wire).await.unwrap();

    let follower_dir = dir.path().join("follower");
    std::fs::create_dir_all(&follower_dir).unwrap();
    let group = GroupStore::create(follower_dir.join("group.redb")).unwrap();
    let logs = LogStore::open(&follower_dir.join("raft.redb")).unwrap();
    let (commits, mut notices) = tokio::sync::broadcast::channel(8);
    let engine = Arc::new(tokio::sync::Mutex::new(Engine::new(group)));
    let reader = engine.lock().await.reader();
    let mut follower = StateMachine::new(engine, logs, Arc::new(NoRuntimes), commits);
    let mut receiving = follower.begin_receiving_snapshot().await.unwrap();
    receiving.write_all(&wire).await.unwrap();
    follower
        .install_snapshot(&built.meta, receiving)
        .await
        .unwrap();
    assert_eq!(reader.key_mark(), 2, "one key change, begun and ended");
    let notice = notices.try_recv().unwrap();
    assert!(notice.keys, "{notice:?}");
}
