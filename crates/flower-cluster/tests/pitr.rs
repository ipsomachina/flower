//! PITR tests: archive committed entries, recover to a target on
//! a new cluster.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use flower_backup::BackupError;
use flower_backup::driver::{Driver, RuntimeBuilder};
use flower_cluster::network::{InProcessFactory, Registry};
use flower_cluster::node::Node;
use flower_cluster::statemachine::StateMachine;
use flower_cluster::types::{ClusterWrite, ResponseOutcome, WriteResponse};
use flower_engine::engine::{
    CallOptions, MultiStep, MutationOptions, PrepareOutcome, RetryIdentity,
};
use flower_engine::retention::{RetentionCommand, request_fingerprint, session_intent};
use flower_engine::retry::{IDEMPOTENCY_TTL_MS, ScopedIdentity};
use flower_engine::runtime::{CallKind, ClosureRuntime, HostCall};
use flower_engine::storage::GroupStore;
use flower_engine::storage::meta;
use flower_sdk::abi::{HostOp, decode_envelope};
use flower_sdk::{Failure, Value, fail};

fn s(text: &str) -> Value {
    Value::String(text.to_string())
}

fn obj(pairs: Vec<(&str, Value)>) -> Value {
    Value::Object(
        pairs
            .into_iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect(),
    )
}

fn arr(items: Vec<Value>) -> Value {
    Value::Array(items)
}

fn k(name: &str) -> String {
    format!("\"{name}\"")
}

fn str_schema() -> Value {
    obj(vec![("type", s("string"))])
}

fn json_schema() -> Value {
    obj(vec![("type", s("json"))])
}

fn manifest() -> Value {
    obj(vec![
        (
            "collections",
            arr(vec![obj(vec![
                ("name", s("tasks")),
                ("key", str_schema()),
                (
                    "value",
                    obj(vec![
                        ("type", s("object")),
                        (
                            "fields",
                            arr(vec![obj(vec![
                                ("name", s("title")),
                                ("schema", str_schema()),
                                ("optional", Value::Bool(false)),
                            ])]),
                        ),
                    ]),
                ),
                ("indexes", arr(vec![])),
                ("references", arr(vec![])),
            ])]),
        ),
        (
            "definitions",
            arr(vec![
                obj(vec![
                    ("name", s("put")),
                    ("kind", s("mutation")),
                    ("args", json_schema()),
                ]),
                obj(vec![
                    ("name", s("get")),
                    ("kind", s("query")),
                    ("args", json_schema()),
                ]),
            ]),
        ),
        ("triggers", arr(vec![])),
        ("http", arr(vec![])),
    ])
}

fn hc(host: &mut dyn HostCall, op: HostOp, args: Vec<Value>) -> Result<Value, Failure> {
    let input = Value::Array(args).encode_binary().unwrap();
    let output = host.op(op as u32 as u8, &input);
    let envelope = Value::decode_binary(&output).unwrap();
    decode_envelope(envelope)
}

fn handler(
    kind: CallKind,
    _module: &str,
    name: &str,
    args: &Value,
    host: &mut dyn HostCall,
) -> Result<Value, Failure> {
    let map = match args {
        Value::Object(map) => map.clone(),
        _ => return Err(fail("TYPE_MISMATCH", "args take an object")),
    };
    let key = match map.get("key") {
        Some(Value::String(key)) => key.clone(),
        _ => return Err(fail("TYPE_MISMATCH", "args need a key")),
    };
    match (kind, name) {
        (CallKind::Mutation, "put") => {
            let title = map.get("title").cloned().unwrap_or(Value::Null);
            hc(
                host,
                HostOp::Set,
                vec![s(""), s("tasks"), s(&key), obj(vec![("title", title)])],
            )?;
            Ok(s("put-ok"))
        }
        (CallKind::Query, "get") => Ok(hc(host, HostOp::Get, vec![s(""), s("tasks"), s(&key)])?),
        _ => Err(fail("UNKNOWN_METHOD", "unknown definition")),
    }
}

fn runtime() -> Box<dyn flower_engine::runtime::Runtime> {
    Box::new(ClosureRuntime::new(handler))
}

#[derive(Clone)]
struct TestFactory;

impl flower_cluster::statemachine::RuntimeFactory for TestFactory {
    fn build(
        &self,
        _db: &[u8],
        _module: &str,
        _wasm: &[u8],
        _manifest: &Value,
    ) -> Result<Box<dyn flower_engine::runtime::Runtime>, String> {
        Ok(runtime())
    }
}

fn builder() -> RuntimeBuilder {
    Arc::new(|_db: &[u8], _module: &str, _wasm: &[u8], _manifest: &Value| Ok(runtime()))
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or(0)
}

async fn wait_leader(node: &Node) -> u64 {
    for _ in 0..200 {
        if let Some(leader) = node.status().leader {
            return leader;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("no leader elected");
}

fn put_args(key: &str, title: &str) -> Value {
    obj(vec![("key", s(&k(key))), ("title", s(title))])
}

fn get_args(key: &str) -> Value {
    obj(vec![("key", s(&k(key)))])
}

#[tokio::test(flavor = "multi_thread")]
async fn pitr_recovers_new_cluster_to_target() {
    let dir = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let node = Node::open(
        1,
        &dir.path().join("n1"),
        Arc::new(openraft::Config::default()),
        InProcessFactory::new(registry.clone()),
        Arc::new(TestFactory),
    )
    .await
    .unwrap();
    registry.register(1, node.raft().clone());
    node.initialize(BTreeSet::from([1])).await.unwrap();
    assert_eq!(wait_leader(&node).await, 1);
    // Archives are kept per history: maintenance would establish it.
    let incarnation = node.establish_incarnation().await.unwrap();
    assert_ne!(incarnation, 0);
    node.deploy(b"db", "app", [9u8; 32], b"wasm-v1", &manifest())
        .await
        .unwrap();
    node.deploy(b"dbb", "app", [9u8; 32], b"wasm-v1", &manifest())
        .await
        .unwrap();

    // Distinct proposal stamps with sleeps well above timer granularity.
    node.mutate(
        b"db",
        "app",
        "put",
        &put_args("a", "first"),
        &MutationOptions::default(),
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(60)).await;

    // Base backup covers A; B and C arrive after it.
    let store = Driver::memory_store();
    let keystore = dir.path().join("keystore.json");
    let driver = Driver::new(
        store.clone(),
        &keystore,
        [3u8; 32],
        node.engine().clone(),
        node.logs().clone(),
        builder(),
    )
    .unwrap();
    let base = driver.backup_now().await.unwrap();
    assert_eq!(base.objects, 2);

    node.mutate(
        b"db",
        "app",
        "put",
        &put_args("b", "second"),
        &MutationOptions::default(),
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(60)).await;
    // A multi-database transaction between B and the target: its
    // entry replays like any other.
    node.transaction(
        &[
            MultiStep {
                db: b"db".to_vec(),
                module: "app".to_string(),
                method: "put".to_string(),
                args: put_args("m", "multi-a"),
            },
            MultiStep {
                db: b"dbb".to_vec(),
                module: "app".to_string(),
                method: "put".to_string(),
                args: put_args("n", "multi-b"),
            },
        ],
        &MutationOptions::default(),
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(60)).await;
    let target = now_ms();
    tokio::time::sleep(Duration::from_millis(60)).await;
    let third = node
        .mutate(
            b"db",
            "app",
            "put",
            &put_args("c", "third"),
            &MutationOptions::default(),
        )
        .await
        .unwrap();

    // Full archive of the log: the incarnation, two deploys, three
    // patches and one multi-database entry.
    let archive = driver.archive_now().await.unwrap();
    assert_eq!(archive.entries, 7);
    node.shutdown().await.unwrap();

    // Recover to the target on a fresh file: A and B present, C gone.
    let offline = Driver::offline(store.clone(), &keystore, [3u8; 32], builder()).unwrap();
    let restored = dir.path().join("restored").join("group.redb");
    std::fs::create_dir_all(restored.parent().unwrap()).unwrap();
    let summary = offline.pitr(&restored, target).await.unwrap();
    assert_eq!(summary.manifest, base.manifest);
    // B and the multi-database entry replay past the base; C stops
    // past the target.
    assert_eq!(summary.replayed, 2);
    assert_eq!(summary.stopped_index, Some(third.position));
    assert!(summary.reached_ms <= target);

    // The restored state stands alone: A, B and the transaction on
    // db, the transaction on dbb, C absent.
    let engine = flower_engine::engine::Engine::new(GroupStore::open(&restored).unwrap());
    let read = engine.store().begin_read().unwrap();
    assert_eq!(read.meta_u64(b"db", meta::REVISION).unwrap(), 3);
    assert_eq!(read.meta_u64(b"db", meta::APPLIED_LOG).unwrap(), 2);
    assert_eq!(read.meta_u64(b"dbb", meta::REVISION).unwrap(), 1);
    assert_eq!(read.meta_u64(b"dbb", meta::APPLIED_LOG).unwrap(), 2);
    drop(read);
    drop(engine);

    // A new cluster bootstraps on the restored file and serves.
    let registry = Registry::new();
    let node = Node::open(
        1,
        restored.parent().unwrap(),
        Arc::new(openraft::Config::default()),
        InProcessFactory::new(registry.clone()),
        Arc::new(TestFactory),
    )
    .await
    .unwrap();
    registry.register(1, node.raft().clone());
    node.initialize(BTreeSet::from([1])).await.unwrap();
    assert_eq!(wait_leader(&node).await, 1);
    // The recovered cluster is a new history: it establishes a fresh
    // incarnation instead of continuing the old one's.
    let recovered = node.establish_incarnation().await.unwrap();
    assert_ne!(recovered, 0);
    assert_ne!(recovered, incarnation);
    let got = node
        .query(b"db", "app", "get", &get_args("b"), &CallOptions::default())
        .await
        .unwrap();
    assert_eq!(got, obj(vec![("title", s("second"))]));
    let gone = node
        .query(b"db", "app", "get", &get_args("c"), &CallOptions::default())
        .await
        .unwrap();
    assert_eq!(gone, Value::Null);
    let multi_a = node
        .query(b"db", "app", "get", &get_args("m"), &CallOptions::default())
        .await
        .unwrap();
    assert_eq!(multi_a, obj(vec![("title", s("multi-a"))]));
    let multi_b = node
        .query(
            b"dbb",
            "app",
            "get",
            &get_args("n"),
            &CallOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(multi_b, obj(vec![("title", s("multi-b"))]));
    let put = node
        .mutate(
            b"db",
            "app",
            "put",
            &put_args("d", "fourth"),
            &MutationOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(put.revision, 4);
    node.shutdown().await.unwrap();
}

/// Prepare a put under idempotency key `key-1` on the leader, called
/// at `timestamp_ms` (now when `None`), which sets its receipt's expiry.
async fn keyed_put(node: &Node, timestamp_ms: Option<u64>) -> Vec<u8> {
    let options = MutationOptions {
        call: CallOptions {
            timestamp_ms,
            ..CallOptions::default()
        },
        idempotency_key: Some(b"key-1".to_vec()),
        ..MutationOptions::default()
    };
    node.with_engine(move |engine| {
        match engine
            .prepare_mutation(b"db", "app", "put", &put_args("a", "first"), &options)
            .unwrap()
        {
            PrepareOutcome::Patch(prepared) => Ok(prepared.patch),
            other => panic!("expected a patch, got {other:?}"),
        }
    })
    .await
    .unwrap()
}

/// Commit a prepared patch through Raft under a chosen proposal stamp,
/// as a leader whose wall clock reads `proposed_ms` would.
async fn propose_at(node: &Node, patch: Vec<u8>, proposed_ms: u64) -> WriteResponse {
    node.raft()
        .client_write(ClusterWrite::Patch {
            db: b"db".to_vec(),
            patch,
            proposed_ms,
        })
        .await
        .unwrap()
        .data
}

#[tokio::test(flavor = "multi_thread")]
async fn pitr_reads_receipt_expiry_at_the_replicated_clock() {
    let dir = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let node = Node::open(
        1,
        &dir.path().join("n1"),
        Arc::new(openraft::Config::default()),
        InProcessFactory::new(registry.clone()),
        Arc::new(TestFactory),
    )
    .await
    .unwrap();
    registry.register(1, node.raft().clone());
    node.initialize(BTreeSet::from([1])).await.unwrap();
    assert_eq!(wait_leader(&node).await, 1);
    node.establish_incarnation().await.unwrap();
    node.deploy(b"db", "app", [9u8; 32], b"wasm-v1", &manifest())
        .await
        .unwrap();

    // A keyed write whose receipt expired an hour ago by its call
    // time, then the base backup.
    let now = now_ms();
    let old = now - IDEMPOTENCY_TTL_MS - 3_600_000;
    let first = propose_at(&node, keyed_put(&node, Some(old)).await, now).await;
    assert_eq!(first.revision, 1);
    let store = Driver::memory_store();
    let keystore = dir.path().join("keystore.json");
    let driver = Driver::new(
        store.clone(),
        &keystore,
        [3u8; 32],
        node.engine().clone(),
        node.logs().clone(),
        builder(),
    )
    .unwrap();
    let base = driver.backup_now().await.unwrap();

    // A leader whose clock stepped back stamps the redelivery before
    // the receipt's expiry. Live members read expiry at the replicated
    // clock, already past it, so the redelivery executes afresh.
    let again = propose_at(&node, keyed_put(&node, None).await, old + 1).await;
    assert_eq!(again.revision, 2);
    let clock = StateMachine::expiry_clock(&*node.engine().lock().await).unwrap();
    assert!(clock >= now);
    driver.archive_now().await.unwrap();
    node.shutdown().await.unwrap();

    // Replay starts from the base's clock, not the entry's own stamp:
    // the recovered state matches what every member committed.
    let offline = Driver::offline(store.clone(), &keystore, [3u8; 32], builder()).unwrap();
    let restored = dir.path().join("restored").join("group.redb");
    std::fs::create_dir_all(restored.parent().unwrap()).unwrap();
    let summary = offline.pitr(&restored, now_ms()).await.unwrap();
    assert_eq!(summary.manifest, base.manifest);
    assert_eq!(summary.replayed, 1);
    let engine = flower_engine::engine::Engine::new(GroupStore::open(&restored).unwrap());
    let read = engine.store().begin_read().unwrap();
    assert_eq!(read.meta_u64(b"db", meta::REVISION).unwrap(), 2);
    drop(read);
    assert_eq!(StateMachine::expiry_clock(&engine).unwrap(), clock);
}

/// Wait up to 20 s for `done`.
async fn wait_until(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while !done() {
        assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// The member every member reports as leader, once they agree.
async fn agreed_leader(nodes: &[&Node]) -> u64 {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let leaders: Vec<_> = nodes.iter().map(|node| node.status().leader).collect();
        if let Some(Some(leader)) = leaders.first()
            && leaders.iter().all(|seen| *seen == Some(*leader))
            && nodes.iter().any(|node| node.status().id == *leader)
        {
            return *leader;
        }
        assert!(tokio::time::Instant::now() < deadline, "no agreed leader");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Snapshot and purge a member's log through `upto`, as Raft's log
/// compaction would.
async fn compact_log(node: &Node, upto: u64) {
    wait_until("applied", || node.status().last_applied >= Some(upto)).await;
    node.raft().trigger().snapshot().await.unwrap();
    wait_until("snapshot", || {
        node.raft().metrics().borrow().snapshot.map(|id| id.index) >= Some(upto)
    })
    .await;
    node.raft().trigger().purge_log(upto).await.unwrap();
    wait_until("purge", || {
        node.raft().metrics().borrow().purged.map(|id| id.index) >= Some(upto)
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_new_leader_continues_the_archive_its_held_log_still_has() {
    let dir = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let mut nodes = Vec::new();
    for id in 1..=3u64 {
        let node = Node::open(
            id,
            &dir.path().join(format!("n{id}")),
            Arc::new(openraft::Config::default()),
            InProcessFactory::new(registry.clone()),
            Arc::new(TestFactory),
        )
        .await
        .unwrap();
        registry.register(id, node.raft().clone());
        // Continuous backup holds every member's log first.
        node.logs().hold_purges(None).await.unwrap();
        nodes.push(node);
    }
    nodes[0]
        .initialize(BTreeSet::from([1, 2, 3]))
        .await
        .unwrap();
    let leader = agreed_leader(&nodes.iter().collect::<Vec<_>>()).await;
    let first = nodes
        .iter()
        .position(|node| node.status().id == leader)
        .unwrap();
    let incarnation = nodes[first].establish_incarnation().await.unwrap();
    nodes[first]
        .deploy(b"db", "app", [9u8; 32], b"wasm-v1", &manifest())
        .await
        .unwrap();
    let mutate = async |node: &Node, key: &str| {
        node.mutate(
            b"db",
            "app",
            "put",
            &put_args(key, key),
            &MutationOptions::default(),
        )
        .await
        .unwrap()
        .position
    };
    mutate(&nodes[first], "a").await;

    // The leader takes a base and archives through B. Each member opens
    // its driver over the same store and the keystore the first seal
    // created.
    let store = Driver::memory_store();
    let keystore = dir.path().join("keystore.json");
    let driver = |node: &Node| {
        Driver::new(
            store.clone(),
            &keystore,
            [3u8; 32],
            node.engine().clone(),
            node.logs().clone(),
            builder(),
        )
        .unwrap()
    };
    let base = driver(&nodes[first]).backup_now().await.unwrap();
    let drivers: Vec<Driver> = nodes.iter().map(driver).collect();
    let b = mutate(&nodes[first], "b").await;
    let archived = drivers[first].archive_now().await.unwrap();
    assert!(archived.to >= b);

    // C and D commit but are not archived when Raft compacts every
    // member's log past them. Followers let go of only what the shared
    // position records: the rest stays, below the purge point.
    mutate(&nodes[first], "c").await;
    let d = mutate(&nodes[first], "d").await;
    for (node, driver) in nodes.iter().zip(&drivers) {
        compact_log(node, d).await;
        if node.status().id != leader {
            assert_eq!(driver.release_archived().await.unwrap(), archived.to);
            wait_until("released entries go", || !node.logs().purge_stats().behind).await;
            assert_eq!(
                node.logs().first_index_from(0).unwrap(),
                Some(archived.to + 1)
            );
        }
    }

    // The leader goes away; another member leads and continues the same
    // archive from its held log, without a base of its own.
    drop(drivers);
    nodes.remove(first).shutdown().await.unwrap();
    registry.unregister(leader);
    let drivers: Vec<Driver> = nodes.iter().map(driver).collect();
    let next = agreed_leader(&nodes.iter().collect::<Vec<_>>()).await;
    assert_ne!(next, leader);
    let at = nodes
        .iter()
        .position(|node| node.status().id == next)
        .unwrap();
    let resumed = drivers[at].archive_now().await.unwrap();
    assert_eq!(resumed.from, archived.to);
    assert!(resumed.to >= d, "{resumed:?}");
    let e = mutate(&nodes[at], "e").await;
    assert!(drivers[at].archive_now().await.unwrap().to >= e);
    let offline = Driver::offline(store.clone(), &keystore, [3u8; 32], builder()).unwrap();
    let listed = offline.list_backups().await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].incarnation, incarnation);
    assert!(listed[0].restorable_through >= e);
    for node in nodes {
        node.shutdown().await.unwrap();
    }

    // Restore through E by index on a new cluster: every write is there,
    // replayed from the one base.
    let restored = dir.path().join("restored").join("group.redb");
    std::fs::create_dir_all(restored.parent().unwrap()).unwrap();
    let summary = offline.restore_to_index(&restored, e, None).await.unwrap();
    assert_eq!(summary.manifest, base.manifest);
    assert_eq!((summary.replayed, summary.index), (4, e));
    let registry = Registry::new();
    let node = Node::open(
        1,
        restored.parent().unwrap(),
        Arc::new(openraft::Config::default()),
        InProcessFactory::new(registry.clone()),
        Arc::new(TestFactory),
    )
    .await
    .unwrap();
    registry.register(1, node.raft().clone());
    node.initialize(BTreeSet::from([1])).await.unwrap();
    assert_eq!(wait_leader(&node).await, 1);
    for key in ["a", "b", "c", "d", "e"] {
        let got = node
            .query(b"db", "app", "get", &get_args(key), &CallOptions::default())
            .await
            .unwrap();
        assert_eq!(got, obj(vec![("title", s(key))]), "{key}");
    }
    node.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_member_caught_up_by_snapshot_takes_a_base_and_archives_on() {
    let dir = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let open = async |id: u64| {
        let node = Node::open(
            id,
            &dir.path().join(format!("n{id}")),
            Arc::new(openraft::Config::default()),
            InProcessFactory::new(registry.clone()),
            Arc::new(TestFactory),
        )
        .await
        .unwrap();
        registry.register(id, node.raft().clone());
        node.logs().hold_purges(None).await.unwrap();
        node
    };
    let mut nodes = Vec::new();
    for id in 1..=3u64 {
        nodes.push(Some(open(id).await));
    }
    nodes[0]
        .as_ref()
        .unwrap()
        .initialize(BTreeSet::from([1, 2, 3]))
        .await
        .unwrap();
    let all: Vec<&Node> = nodes.iter().flatten().collect();
    let leader = agreed_leader(&all).await;
    let lead = all.iter().find(|node| node.status().id == leader).unwrap();
    let incarnation = lead.establish_incarnation().await.unwrap();
    lead.deploy(b"db", "app", [9u8; 32], b"wasm-v1", &manifest())
        .await
        .unwrap();
    let mutate = async |node: &Node, key: &str| {
        node.mutate(
            b"db",
            "app",
            "put",
            &put_args(key, key),
            &MutationOptions::default(),
        )
        .await
        .unwrap()
        .position
    };
    mutate(lead, "a").await;
    let store = Driver::memory_store();
    let keystore = dir.path().join("keystore.json");
    let driver = |node: &Node| {
        Driver::new(
            store.clone(),
            &keystore,
            [3u8; 32],
            node.engine().clone(),
            node.logs().clone(),
            builder(),
        )
        .unwrap()
    };
    let lead_driver = driver(lead);
    lead_driver.backup_now().await.unwrap();
    let b = mutate(lead, "b").await;
    let archived = lead_driver.archive_now().await.unwrap();
    assert!(archived.to >= b);
    drop(lead_driver);

    // A follower goes down; the others write C and D, which never reach
    // the archive, and compact their logs past them.
    let behind = all
        .iter()
        .find(|node| node.status().id != leader)
        .unwrap()
        .status()
        .id;
    let slot = (behind - 1) as usize;
    wait_until("B on the follower", || {
        nodes[slot].as_ref().unwrap().status().last_applied >= Some(b)
    })
    .await;
    registry.unregister(behind);
    nodes[slot].take().unwrap().shutdown().await.unwrap();
    let lead = nodes
        .iter()
        .flatten()
        .find(|node| node.status().id == leader)
        .unwrap();
    mutate(lead, "c").await;
    let d = mutate(lead, "d").await;
    for node in nodes.iter().flatten() {
        compact_log(node, d).await;
    }

    // Back, it catches up by snapshot: its log holds nothing past B
    // until after the snapshot's index.
    nodes[slot] = Some(open(behind).await);
    let caught_up = nodes[slot].as_ref().unwrap();
    wait_until("the snapshot install", || {
        caught_up
            .raft()
            .metrics()
            .borrow()
            .snapshot
            .map(|id| id.index)
            >= Some(d)
    })
    .await;
    let e = mutate(
        nodes
            .iter()
            .flatten()
            .find(|node| node.status().id == leader)
            .unwrap(),
        "e",
    )
    .await;
    wait_until("E on the follower", || {
        caught_up.status().last_applied >= Some(e)
    })
    .await;
    assert_eq!(
        caught_up.logs().first_index_from(archived.to + 1).unwrap(),
        caught_up.logs().first_index_from(d + 1).unwrap()
    );

    // Leading continuous backup, it meets the hole the install left,
    // takes a base of its own and archives on from it.
    let settings = flower_backup::ContinuousSettings {
        archive_interval: Duration::from_millis(20),
        base_interval: Duration::from_secs(3600),
        base_bytes: None,
        max_held: None,
        max_held_bytes: None,
        ..flower_backup::ContinuousSettings::default()
    };
    let (stop, stopped) = tokio::sync::watch::channel(false);
    let looping = driver(caught_up);
    let running = tokio::spawn(async move { looping.run_continuous(&settings, stopped).await });
    let offline = Driver::offline(store.clone(), &keystore, [3u8; 32], builder()).unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let listed = offline.list_backups().await.unwrap();
        if listed.len() == 2 && listed[1].restorable_through >= e {
            assert_eq!(listed[1].incarnation, incarnation);
            assert!(listed[1].covered_index >= d);
            // The older base reaches B, then the gap.
            assert_eq!(listed[0].restorable_through, archived.to);
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "{listed:?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    stop.send(true).unwrap();
    running.await.unwrap().unwrap();
    for node in nodes.into_iter().flatten() {
        node.shutdown().await.unwrap();
    }

    // Restores reach E from the new base, and refuse D, which only that
    // base's cut covers.
    let restored = dir.path().join("restored").join("group.redb");
    std::fs::create_dir_all(restored.parent().unwrap()).unwrap();
    let summary = offline.restore_to_index(&restored, e, None).await.unwrap();
    assert_eq!(summary.index, e);
    let below = offline
        .restore_to_index(&dir.path().join("gap.redb"), d, None)
        .await;
    assert!(matches!(below, Err(BackupError::Refused(_))), "{below:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn restore_registers_a_database_deployed_after_the_base() {
    let dir = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let node = Node::open(
        1,
        &dir.path().join("n1"),
        Arc::new(openraft::Config::default()),
        InProcessFactory::new(registry.clone()),
        Arc::new(TestFactory),
    )
    .await
    .unwrap();
    registry.register(1, node.raft().clone());
    node.initialize(BTreeSet::from([1])).await.unwrap();
    assert_eq!(wait_leader(&node).await, 1);
    node.establish_incarnation().await.unwrap();

    // The base predates the database: only replay creates it.
    let store = Driver::memory_store();
    let keystore = dir.path().join("keystore.json");
    let driver = Driver::new(
        store.clone(),
        &keystore,
        [3u8; 32],
        node.engine().clone(),
        node.logs().clone(),
        builder(),
    )
    .unwrap();
    assert_eq!(driver.backup_now().await.unwrap().objects, 0);
    node.deploy(b"db", "app", [9u8; 32], b"wasm-v1", &manifest())
        .await
        .unwrap();
    let put = node
        .mutate(
            b"db",
            "app",
            "put",
            &put_args("a", "first"),
            &MutationOptions::default(),
        )
        .await
        .unwrap();
    driver.archive_now().await.unwrap();
    node.shutdown().await.unwrap();

    let offline = Driver::offline(store.clone(), &keystore, [3u8; 32], builder()).unwrap();
    let restored = dir.path().join("restored").join("group.redb");
    std::fs::create_dir_all(restored.parent().unwrap()).unwrap();
    let summary = offline
        .restore_to_index(&restored, put.position, None)
        .await
        .unwrap();
    assert_eq!(summary.replayed, 2);
    let engine = flower_engine::engine::Engine::new(GroupStore::open(&restored).unwrap());
    assert_eq!(
        StateMachine::replicated_databases(&engine).unwrap(),
        vec![b"db".to_vec()]
    );
    drop(engine);

    // The restored node reloads the module and serves the write.
    let registry = Registry::new();
    let node = Node::open(
        1,
        restored.parent().unwrap(),
        Arc::new(openraft::Config::default()),
        InProcessFactory::new(registry.clone()),
        Arc::new(TestFactory),
    )
    .await
    .unwrap();
    registry.register(1, node.raft().clone());
    node.initialize(BTreeSet::from([1])).await.unwrap();
    assert_eq!(wait_leader(&node).await, 1);
    let got = node
        .query(b"db", "app", "get", &get_args("a"), &CallOptions::default())
        .await
        .unwrap();
    assert_eq!(got, obj(vec![("title", s("first"))]));
    node.shutdown().await.unwrap();
}

/// What a put under scoped identity `identity` answered: its value, or
/// its refusal's code.
async fn scoped_put(
    node: &Node,
    identity: ScopedIdentity,
    key: &str,
    caller: &CallOptions,
) -> Result<Value, String> {
    let args = put_args(key, "scoped");
    let options = MutationOptions {
        call: caller.clone(),
        retry: Some(RetryIdentity {
            identity,
            fingerprint: request_fingerprint("app", "put", &args, None).unwrap(),
        }),
        ..MutationOptions::default()
    };
    let response = node
        .mutate(b"db", "app", "put", &args, &options)
        .await
        .unwrap();
    match response.outcome {
        ResponseOutcome::Value(bytes) => Ok(flower_engine::json::parse_value(&bytes).unwrap()),
        ResponseOutcome::Failure(bytes) => {
            let envelope = flower_engine::json::parse_value(&bytes).unwrap();
            Err(decode_envelope(envelope).unwrap_err().code)
        }
        ResponseOutcome::Error(message) => Err(message.split(": ").next().unwrap().to_string()),
    }
}

/// v1 needs an operator's `reincarnate` after a restore before the
/// restored service takes traffic; forgetting it lets requests of the
/// old history replay or run again. In v2 every restore starts a new
/// history by itself: the restored group establishes a fresh
/// incarnation, so scoped request IDs and retry sessions of the old one
/// are refused (`wrong_history`) before any replay, sessions cannot be
/// reopened, and only identities of the new history run.
#[tokio::test(flavor = "multi_thread")]
async fn a_restore_starts_a_new_history_that_refuses_the_old_ones_requests() {
    let dir = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let node = Node::open(
        1,
        &dir.path().join("n1"),
        Arc::new(openraft::Config::default()),
        InProcessFactory::new(registry.clone()),
        Arc::new(TestFactory),
    )
    .await
    .unwrap();
    registry.register(1, node.raft().clone());
    node.initialize(BTreeSet::from([1])).await.unwrap();
    assert_eq!(wait_leader(&node).await, 1);
    let old = node.establish_incarnation().await.unwrap();
    node.deploy(b"db", "app", [9u8; 32], b"wasm-v1", &manifest())
        .await
        .unwrap();
    let rotate = RetentionCommand::Rotate {
        epoch_ms: None,
        keep_epochs: 2,
    };
    node.retention(b"db", rotate).await.unwrap().unwrap();
    let ada = CallOptions {
        principal: Some(flower_sdk::Principal::new("ada".to_string(), None, None).unwrap()),
        ..CallOptions::default()
    };
    let session = [4u8; 16];
    node.open_retry_session(b"db", "app", session, &ada)
        .await
        .unwrap()
        .unwrap();
    let scoped = |incarnation, intent: Vec<u8>| ScopedIdentity {
        incarnation,
        epoch: 0,
        intent,
    };
    let once = vec![1u8; 32];
    let first = session_intent(&session, 1);
    assert_eq!(
        scoped_put(&node, scoped(old, once.clone()), "a", &ada).await,
        Ok(s("put-ok"))
    );
    assert_eq!(
        scoped_put(&node, scoped(old, first.clone()), "b", &ada).await,
        Ok(s("put-ok"))
    );
    // Both replay in their own history.
    assert_eq!(
        scoped_put(&node, scoped(old, once.clone()), "a", &ada).await,
        Ok(s("put-ok"))
    );
    let store = Driver::memory_store();
    let keystore = dir.path().join("keystore.json");
    let driver = Driver::new(
        store.clone(),
        &keystore,
        [3u8; 32],
        node.engine().clone(),
        node.logs().clone(),
        builder(),
    )
    .unwrap();
    driver.backup_now().await.unwrap();
    driver.archive_now().await.unwrap();
    node.shutdown().await.unwrap();

    // Restore everything onto a new cluster: no operator step follows.
    let offline = Driver::offline(store, &keystore, [3u8; 32], builder()).unwrap();
    let restored = dir.path().join("restored").join("group.redb");
    std::fs::create_dir_all(restored.parent().unwrap()).unwrap();
    offline.pitr(&restored, now_ms()).await.unwrap();
    let registry = Registry::new();
    let node = Node::open(
        1,
        restored.parent().unwrap(),
        Arc::new(openraft::Config::default()),
        InProcessFactory::new(registry.clone()),
        Arc::new(TestFactory),
    )
    .await
    .unwrap();
    registry.register(1, node.raft().clone());
    node.initialize(BTreeSet::from([1])).await.unwrap();
    assert_eq!(wait_leader(&node).await, 1);
    let new = node.establish_incarnation().await.unwrap();
    assert_ne!(new, 0);
    assert_ne!(new, old);
    assert_eq!(node.retry_status(b"db").await.unwrap().incarnation, new);
    // The old history's requests are refused, never replayed or run,
    // although their receipts and the session were restored.
    for (intent, key) in [
        (once, "a"),
        (first, "b"),
        (session_intent(&session, 2), "c"),
    ] {
        assert_eq!(
            scoped_put(&node, scoped(old, intent), key, &ada).await,
            Err("wrong_history".to_string()),
            "{key}"
        );
    }
    let reopened = node
        .open_retry_session(b"db", "app", session, &ada)
        .await
        .unwrap();
    assert_eq!(reopened.unwrap_err().code, "wrong_history");
    // The new history's run.
    assert_eq!(
        scoped_put(&node, scoped(new, vec![2u8; 32]), "d", &ada).await,
        Ok(s("put-ok"))
    );
    let fresh = [5u8; 16];
    node.open_retry_session(b"db", "app", fresh, &ada)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        scoped_put(&node, scoped(new, session_intent(&fresh, 1)), "e", &ada).await,
        Ok(s("put-ok"))
    );
    let gone = node
        .query(b"db", "app", "get", &get_args("c"), &CallOptions::default())
        .await
        .unwrap();
    assert_eq!(gone, Value::Null);
    node.shutdown().await.unwrap();
}

/// Point-in-time recovery replays the archive as the history it
/// belongs to: writes made under scoped request IDs and retry sessions
/// after the base apply as they did live, and only then does the
/// restored group start a new history, which refuses the old one's IDs.
/// (Before, the restore cleared the incarnation before replay, so every
/// such write archived after the base was refused as `wrong_history`
/// and silently missing from the restored data.)
#[tokio::test(flavor = "multi_thread")]
async fn pitr_replays_scoped_writes_made_after_the_base() {
    let dir = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let node = Node::open(
        1,
        &dir.path().join("n1"),
        Arc::new(openraft::Config::default()),
        InProcessFactory::new(registry.clone()),
        Arc::new(TestFactory),
    )
    .await
    .unwrap();
    registry.register(1, node.raft().clone());
    node.initialize(BTreeSet::from([1])).await.unwrap();
    assert_eq!(wait_leader(&node).await, 1);
    let old = node.establish_incarnation().await.unwrap();
    node.deploy(b"db", "app", [9u8; 32], b"wasm-v1", &manifest())
        .await
        .unwrap();
    let rotate = RetentionCommand::Rotate {
        epoch_ms: None,
        keep_epochs: 2,
    };
    node.retention(b"db", rotate).await.unwrap().unwrap();
    let ada = CallOptions {
        principal: Some(flower_sdk::Principal::new("ada".to_string(), None, None).unwrap()),
        ..CallOptions::default()
    };
    let store = Driver::memory_store();
    let keystore = dir.path().join("keystore.json");
    let driver = Driver::new(
        store.clone(),
        &keystore,
        [3u8; 32],
        node.engine().clone(),
        node.logs().clone(),
        builder(),
    )
    .unwrap();
    driver.backup_now().await.unwrap();
    // After the base: a session, and writes under scoped IDs.
    let session = [6u8; 16];
    node.open_retry_session(b"db", "app", session, &ada)
        .await
        .unwrap()
        .unwrap();
    let scoped = |intent: Vec<u8>| ScopedIdentity {
        incarnation: old,
        epoch: 0,
        intent,
    };
    for (intent, key) in [(vec![1u8; 32], "a"), (session_intent(&session, 1), "b")] {
        assert_eq!(
            scoped_put(&node, scoped(intent), key, &ada).await,
            Ok(s("put-ok"))
        );
    }
    driver.archive_now().await.unwrap();
    node.shutdown().await.unwrap();

    let offline = Driver::offline(store, &keystore, [3u8; 32], builder()).unwrap();
    let restored = dir.path().join("restored").join("group.redb");
    std::fs::create_dir_all(restored.parent().unwrap()).unwrap();
    offline.pitr(&restored, now_ms()).await.unwrap();
    let registry = Registry::new();
    let node = Node::open(
        1,
        restored.parent().unwrap(),
        Arc::new(openraft::Config::default()),
        InProcessFactory::new(registry.clone()),
        Arc::new(TestFactory),
    )
    .await
    .unwrap();
    registry.register(1, node.raft().clone());
    node.initialize(BTreeSet::from([1])).await.unwrap();
    assert_eq!(wait_leader(&node).await, 1);
    for key in ["a", "b"] {
        let got = node
            .query(b"db", "app", "get", &get_args(key), &CallOptions::default())
            .await
            .unwrap();
        assert_eq!(got, obj(vec![("title", s("scoped"))]), "{key}");
    }
    // The restored group's history is a new one.
    let new = node.establish_incarnation().await.unwrap();
    assert_ne!(new, old);
    assert_eq!(
        scoped_put(&node, scoped(session_intent(&session, 2)), "c", &ada).await,
        Err("wrong_history".to_string())
    );
    node.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn pitr_refuses_without_base() {
    let dir = tempfile::tempdir().unwrap();
    let store = Driver::memory_store();
    let offline = Driver::offline(
        store,
        &dir.path().join("keystore.json"),
        [3u8; 32],
        builder(),
    )
    .unwrap();
    assert!(matches!(
        offline
            .pitr(&dir.path().join("r.redb"), now_ms())
            .await
            .unwrap_err(),
        BackupError::Refused(_)
    ));
}
