//! Cluster integration: election, replication, deployment, sync
//! serving and TCP transport.

mod fixtures;

use fixtures::{arr, hc, index, json_schema, k, obj, s, str_schema};

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use flower_cluster::logstore::LogStore;
use flower_cluster::network::{InProcessFactory, PeerConfig, Registry, TcpFactory};
use flower_cluster::node::{FireReport, Node, NodeError};
use flower_cluster::statemachine::{RuntimeFactory, StateMachine};
use flower_cluster::sync::{SyncSnapshotFile, serve_since};
use flower_cluster::types::{ClusterWrite, ResponseOutcome};
use flower_engine::engine::{CallOptions, Engine, MultiStep, MutationOptions};
use flower_engine::placement::PlacementCommand;
use flower_engine::retry::{IDEMPOTENCY_TTL_MS, UNSCOPED_PREFIX};
use flower_engine::runtime::{CallKind, ClosureRuntime, HostCall};
use flower_engine::snapshot::{SnapshotMeta, apply_snapshot};
use flower_engine::storage::GroupStore;
use flower_engine::timer::TimerState;
use flower_sdk::abi::HostOp;
use flower_sdk::{Failure, Value, fail};
use openraft::BasicNode;
use openraft::storage::{RaftLogStorage, RaftSnapshotBuilder, RaftStateMachine};

fn field(name: &str, schema: Value, optional: bool) -> Value {
    obj(vec![
        ("name", s(name)),
        ("schema", schema),
        ("optional", Value::Bool(optional)),
    ])
}

fn object_schema(fields: Vec<Value>) -> Value {
    obj(vec![("type", s("object")), ("fields", arr(fields))])
}

fn collection(
    name: &str,
    key: Value,
    value: Value,
    indexes: Vec<Value>,
    references: Vec<Value>,
) -> Value {
    obj(vec![
        ("name", s(name)),
        ("key", key),
        ("value", value),
        ("indexes", arr(indexes)),
        ("references", arr(references)),
    ])
}

fn tasks_collection() -> Value {
    collection(
        "tasks",
        str_schema(),
        object_schema(vec![field("title", str_schema(), false)]),
        vec![index("by_title", vec!["title"])],
        vec![],
    )
}

fn posts_collection() -> Value {
    collection(
        "posts",
        str_schema(),
        object_schema(vec![field("title", str_schema(), false)]),
        vec![],
        vec![],
    )
}

fn definition(name: &str, kind: &str) -> Value {
    obj(vec![
        ("name", s(name)),
        ("kind", s(kind)),
        ("args", json_schema()),
    ])
}

fn manifest(collections: Vec<Value>, definitions: Vec<Value>) -> Value {
    obj(vec![
        ("collections", arr(collections)),
        ("definitions", arr(definitions)),
        ("triggers", arr(vec![])),
        ("http", arr(vec![])),
    ])
}

fn app_manifest() -> Value {
    let mut manifest = manifest(
        vec![tasks_collection()],
        vec![
            definition("put", "mutation"),
            definition("get", "query"),
            definition("schedule", "mutation"),
            definition("enqueue", "mutation"),
            definition("slow", "query"),
        ],
    );
    // `enqueue`'s queue: guests use only the queues they declare.
    if let Value::Object(map) = &mut manifest {
        map.insert(
            "queues".to_string(),
            arr(vec![obj(vec![("name", s("jobs"))])]),
        );
    }
    manifest
}

fn app_manifest_v2() -> Value {
    manifest(
        vec![tasks_collection(), posts_collection()],
        vec![definition("put", "mutation"), definition("get", "query")],
    )
}

fn opt(value: Value) -> Option<Value> {
    if value == Value::Null {
        None
    } else {
        Some(value)
    }
}

fn get(host: &mut dyn HostCall, collection: &str, key: &str) -> Result<Option<Value>, Failure> {
    Ok(opt(hc(
        host,
        HostOp::Get,
        vec![s(""), s(collection), s(key)],
    )?))
}

fn set(host: &mut dyn HostCall, collection: &str, key: &str, value: Value) -> Result<(), Failure> {
    hc(host, HostOp::Set, vec![s(""), s(collection), s(key), value])?;
    Ok(())
}

type Handler = Arc<
    dyn Fn(CallKind, &str, &str, &Value, &mut dyn HostCall) -> Result<Value, Failure> + Send + Sync,
>;

fn app_handler() -> Handler {
    Arc::new(
        |kind: CallKind, _module: &str, name: &str, args: &Value, host: &mut dyn HostCall| {
            let map = match args {
                Value::Object(map) => map.clone(),
                _ => return Err(fail("TYPE_MISMATCH", "args take an object")),
            };
            if matches!((kind, name), (CallKind::Mutation, "enqueue")) {
                let payload = map.get("payload").cloned().unwrap_or(Value::Null);
                return hc(
                    host,
                    HostOp::Enqueue,
                    vec![s("jobs"), payload, Value::Number(0.0), Value::Number(5.0)],
                );
            }
            if let (CallKind::Mutation, "fill", Some(Value::Number(rows))) =
                (kind, name, map.get("rows"))
            {
                for row in 0..*rows as u64 {
                    let title = s(&format!("t{row}"));
                    set(
                        host,
                        "tasks",
                        &k(&format!("f{row}")),
                        obj(vec![("title", title)]),
                    )?;
                }
                return Ok(Value::Null);
            }
            if matches!((kind, name), (CallKind::Query, "slow")) {
                SLOW_STARTED.store(true, Ordering::SeqCst);
                std::thread::sleep(Duration::from_millis(1500));
                SLOW_DONE.store(true, Ordering::SeqCst);
                return Ok(Value::Null);
            }
            if matches!((kind, name), (CallKind::Mutation, "schedule")) {
                let field = |name: &str| map.get(name).cloned().unwrap_or(Value::Null);
                let text = |name: &str| match field(name) {
                    Value::String(text) => text,
                    _ => String::new(),
                };
                let number = |name: &str| match field(name) {
                    Value::Number(number) => number,
                    _ => 0.0,
                };
                hc(
                    host,
                    HostOp::TimerSchedule,
                    vec![
                        s(&text("id")),
                        Value::Number(number("due_ms")),
                        s(&text("handler")),
                        field("args"),
                        Value::Number(number("max")),
                    ],
                )?;
                return Ok(Value::Null);
            }
            let key = match map.get("key") {
                Some(Value::String(key)) => key.clone(),
                _ => return Err(fail("TYPE_MISMATCH", "args need a key")),
            };
            match (kind, name) {
                (CallKind::Mutation, "put") => {
                    let title = map.get("title").cloned().unwrap_or(Value::Null);
                    set(host, "tasks", &key, obj(vec![("title", title)]))?;
                    Ok(s("put-ok"))
                }
                (CallKind::Query, "get") => Ok(get(host, "tasks", &key)?.unwrap_or(Value::Null)),
                _ => Err(fail("UNKNOWN_METHOD", "unknown definition")),
            }
        },
    )
}

#[derive(Clone)]
struct TestFactory {
    handler: Handler,
}

impl RuntimeFactory for TestFactory {
    fn build(
        &self,
        _db: &[u8],
        _module: &str,
        _wasm: &[u8],
        _manifest: &Value,
    ) -> Result<Box<dyn flower_engine::runtime::Runtime>, String> {
        let handler = self.handler.clone();
        Ok(Box::new(ClosureRuntime::new(
            move |kind, module, name, args, host| handler(kind, module, name, args, host),
        )))
    }
}

fn factory() -> TestFactory {
    TestFactory {
        handler: app_handler(),
    }
}

fn config() -> Arc<openraft::Config> {
    Arc::new(openraft::Config::default())
}

/// Open and initialize a single voting member with this suite's runtime.
async fn open_single_node(dir: &std::path::Path, registry: &Registry) -> Node {
    let node = Node::open(
        1,
        &dir.join("n1"),
        config(),
        InProcessFactory::new(registry.clone()),
        Arc::new(factory()),
    )
    .await
    .unwrap();
    registry.register(1, node.raft().clone());
    node.initialize(BTreeSet::from([1])).await.unwrap();
    node
}

/// A member's files record the storage contract they were written under
/// and never open under another: an in-place upgrade across a contract
/// bump fails at start, naming both, rather than reading old values or
/// applying old entries as this build's. Files from a build that
/// recorded none fail alike; a member's own files reopen.
#[tokio::test(flavor = "multi_thread")]
async fn files_written_under_another_contract_never_open() {
    let dir = tempfile::tempdir().unwrap();
    let current = flower_cluster::compat::storage_contract();
    let open = |name: &str| {
        let path = dir.path().join(name);
        async move {
            Node::open(
                1,
                &path,
                config(),
                InProcessFactory::new(Registry::new()),
                Arc::new(factory()),
            )
            .await
        }
    };
    let refused = |error: NodeError, written: &str| {
        let message = error.to_string();
        assert!(message.contains(written), "{message}");
        assert!(message.contains(current), "{message}");
    };

    let node = open("own").await.unwrap();
    node.initialize(BTreeSet::from([1])).await.unwrap();
    node.shutdown().await.unwrap();
    open("own").await.unwrap().shutdown().await.unwrap();

    for (name, file) in [("group", "group.redb"), ("raft", "raft.redb")] {
        let path = dir.path().join(name);
        std::fs::create_dir_all(&path).unwrap();
        let other = "state0-snapshot0-value0";
        let claimed = if file == "group.redb" {
            GroupStore::create(path.join(file))
                .unwrap()
                .claim_contract(other)
                .unwrap()
        } else {
            LogStore::open(&path.join(file))
                .unwrap()
                .claim_contract(other)
                .unwrap()
        };
        claimed.unwrap();
        refused(open(name).await.err().unwrap(), other);
    }

    let legacy = dir.path().join("legacy");
    std::fs::create_dir_all(&legacy).unwrap();
    let store = GroupStore::create(legacy.join("group.redb")).unwrap();
    let write = store.begin_write().unwrap();
    write.set_meta_u64(b"db", b"revision", 1).unwrap();
    write.commit().unwrap();
    drop(store);
    refused(
        open("legacy").await.err().unwrap(),
        "recorded no storage contract",
    );
}

/// Wait for any node to report a leader.
async fn wait_leader(nodes: &[&Node]) -> u64 {
    for _ in 0..200 {
        for node in nodes {
            if let Some(leader) = node.status().leader {
                return leader;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("no leader elected");
}

/// Wait for a node to apply through an index.
async fn wait_applied(node: &Node, index: u64) {
    for _ in 0..200 {
        if node.status().last_applied.unwrap_or(0) >= index {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("node {} did not apply {index}", node.id());
}

/// Wait for a module's active identity on a node.
async fn wait_identity(node: &Node, db: &[u8], module: &str, identity: &[u8; 32]) {
    for _ in 0..200 {
        let engine = node.engine().lock().await;
        let read = engine.store().begin_read().unwrap();
        let active = read
            .meta(db, &flower_engine::staging::code_identity_key(module))
            .unwrap();
        drop(read);
        drop(engine);
        if matches!(active, Some(bytes) if bytes.as_slice() == identity.as_slice()) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("node {} did not activate {module:?}", node.id());
}

fn put_args(key: &str, title: &str) -> Value {
    obj(vec![("key", s(&k(key))), ("title", s(title))])
}

fn get_args(key: &str) -> Value {
    obj(vec![("key", s(&k(key)))])
}

#[tokio::test(flavor = "multi_thread")]
async fn single_node_deploy_mutate_query() {
    let dir = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let node = open_single_node(dir.path(), &registry).await;
    assert_eq!(wait_leader(&[&node]).await, 1);

    node.deploy(b"db", "app", [9u8; 32], b"wasm-v1", &app_manifest())
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
    assert_eq!(put.revision, 1);
    match &put.outcome {
        ResponseOutcome::Value(bytes) => {
            assert_eq!(String::from_utf8_lossy(bytes), "\"put-ok\"");
        }
        outcome => panic!("unexpected outcome: {outcome:?}"),
    }

    let got = node
        .query(b"db", "app", "get", &get_args("a"), &CallOptions::default())
        .await
        .unwrap();
    assert_eq!(got, obj(vec![("title", s("first"))]));

    let put2 = node
        .mutate(
            b"db",
            "app",
            "put",
            &put_args("b", "second"),
            &MutationOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(put2.revision, 2);
    assert!(put2.position > put.position);

    node.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn transaction_commits_two_databases_through_consensus() {
    let dir = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let node = open_single_node(dir.path(), &registry).await;
    assert_eq!(wait_leader(&[&node]).await, 1);

    for db in [b"dba".as_slice(), b"dbb".as_slice()] {
        node.deploy(db, "app", [9u8; 32], b"wasm-v1", &app_manifest())
            .await
            .unwrap();
    }

    let steps = vec![
        MultiStep {
            db: b"dba".to_vec(),
            module: "app".to_string(),
            method: "put".to_string(),
            args: put_args("a", "first"),
        },
        MultiStep {
            db: b"dbb".to_vec(),
            module: "app".to_string(),
            method: "put".to_string(),
            args: put_args("b", "second"),
        },
    ];
    let committed = node
        .transaction(&steps, &MutationOptions::default())
        .await
        .unwrap();
    assert_eq!(committed.revision, 1);
    assert_eq!(
        committed.revisions,
        vec![(b"dba".to_vec(), 1), (b"dbb".to_vec(), 1)]
    );
    match &committed.outcome {
        ResponseOutcome::Value(bytes) => {
            assert_eq!(String::from_utf8_lossy(bytes), "[\"put-ok\",\"put-ok\"]");
        }
        outcome => panic!("unexpected outcome: {outcome:?}"),
    }

    let row = node
        .query(
            b"dba",
            "app",
            "get",
            &get_args("a"),
            &CallOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(row, obj(vec![("title", s("first"))]));
    let row = node
        .query(
            b"dbb",
            "app",
            "get",
            &get_args("b"),
            &CallOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(row, obj(vec![("title", s("second"))]));

    node.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn transaction_aborts_without_proposing() {
    let dir = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let node = open_single_node(dir.path(), &registry).await;
    assert_eq!(wait_leader(&[&node]).await, 1);

    for db in [b"dba".as_slice(), b"dbb".as_slice()] {
        node.deploy(db, "app", [9u8; 32], b"wasm-v1", &app_manifest())
            .await
            .unwrap();
    }

    let steps = vec![
        MultiStep {
            db: b"dba".to_vec(),
            module: "app".to_string(),
            method: "put".to_string(),
            args: put_args("a", "first"),
        },
        MultiStep {
            db: b"dbb".to_vec(),
            module: "app".to_string(),
            method: "put".to_string(),
            args: Value::Null,
        },
    ];
    let aborted = node
        .transaction(&steps, &MutationOptions::default())
        .await
        .unwrap();
    // Served without replicating: position 0 and a business failure.
    assert_eq!(aborted.position, 0);
    match &aborted.outcome {
        ResponseOutcome::Failure(bytes) => {
            let text = String::from_utf8_lossy(bytes);
            assert!(text.contains("TYPE_MISMATCH"), "unexpected failure: {text}");
        }
        outcome => panic!("unexpected outcome: {outcome:?}"),
    }
    let row = node
        .query(
            b"dba",
            "app",
            "get",
            &get_args("a"),
            &CallOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(row, Value::Null);

    node.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn restart_replays_idempotency_key() {
    let dir = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let node = open_single_node(dir.path(), &registry).await;
    assert_eq!(wait_leader(&[&node]).await, 1);
    node.deploy(b"db", "app", [9u8; 32], b"wasm-v1", &app_manifest())
        .await
        .unwrap();

    let keyed = MutationOptions {
        idempotency_key: Some(b"call-9".to_vec()),
        ..MutationOptions::default()
    };
    let first = node
        .mutate(b"db", "app", "put", &put_args("a", "first"), &keyed)
        .await
        .unwrap();
    assert_eq!(first.revision, 1);
    // Same-process redelivery replays from memory, answering where the
    // original committed.
    let replayed = node
        .mutate(b"db", "app", "put", &put_args("a", "first"), &keyed)
        .await
        .unwrap();
    assert!(first.position > 0);
    assert_eq!(replayed.position, first.position);
    assert_eq!(replayed.revision, first.revision);
    assert_eq!(replayed.outcome, first.outcome);
    node.shutdown().await.unwrap();

    // Reopen drops all memory: redelivery must serve from the
    // durable receipt without proposing or committing.
    let node = Node::open(
        1,
        &dir.path().join("n1"),
        config(),
        InProcessFactory::new(registry.clone()),
        Arc::new(factory()),
    )
    .await
    .unwrap();
    registry.register(1, node.raft().clone());
    assert_eq!(wait_leader(&[&node]).await, 1);
    let after = node
        .mutate(b"db", "app", "put", &put_args("a", "first"), &keyed)
        .await
        .unwrap();
    assert_eq!(after.position, first.position);
    assert_eq!(after.revision, 1);
    assert_eq!(after.outcome, first.outcome);
    let row = node
        .query(b"db", "app", "get", &get_args("a"), &CallOptions::default())
        .await
        .unwrap();
    assert_eq!(row, obj(vec![("title", s("first"))]));
    node.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn purge_expired_idempotency_runs() {
    let dir = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let node = open_single_node(dir.path(), &registry).await;
    assert_eq!(wait_leader(&[&node]).await, 1);
    node.deploy(b"db", "app", [9u8; 32], b"wasm-v1", &app_manifest())
        .await
        .unwrap();

    let old = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        - IDEMPOTENCY_TTL_MS
        - 3_600_000;
    let stale = MutationOptions {
        call: CallOptions {
            timestamp_ms: Some(old),
            ..Default::default()
        },
        idempotency_key: Some(b"stale-1".to_vec()),
        ..MutationOptions::default()
    };
    node.mutate(b"db", "app", "put", &put_args("a", "first"), &stale)
        .await
        .unwrap();
    let fresh = MutationOptions {
        idempotency_key: Some(b"fresh-1".to_vec()),
        ..MutationOptions::default()
    };
    let original = node
        .mutate(b"db", "app", "put", &put_args("b", "second"), &fresh)
        .await
        .unwrap();
    let deleted = node.purge_expired_idempotency().await.unwrap();
    assert_eq!(deleted, 1);
    {
        let engine = node.engine().lock().await;
        let read = engine.store().begin_read().unwrap();
        let rows = read.scan_receipts(b"db").unwrap();
        let stale_left = rows
            .iter()
            .filter(|(identity, _)| identity.starts_with(UNSCOPED_PREFIX))
            .count();
        // Only the fresh key's row survives.
        assert_eq!(stale_left, 1);
    }
    // The purged key re-executes; the fresh key replays.
    let redone = node
        .mutate(b"db", "app", "put", &put_args("a", "first"), &stale)
        .await
        .unwrap();
    assert_eq!(redone.revision, 3);
    let replayed = node
        .mutate(b"db", "app", "put", &put_args("b", "second"), &fresh)
        .await
        .unwrap();
    assert_eq!(replayed.position, original.position);
    assert_eq!(replayed.revision, original.revision);
    node.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_duplicates_commit_once() {
    let dir = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let node = open_single_node(dir.path(), &registry).await;
    assert_eq!(wait_leader(&[&node]).await, 1);
    node.deploy(b"db", "app", [9u8; 32], b"wasm-v1", &app_manifest())
        .await
        .unwrap();

    // Two concurrent submissions under one key: prepares serialize
    // on the engine lock but nothing caches before consensus, so
    // both may propose; the applier skips the duplicate journal.
    // Either path answers the same committed outcome with one bump.
    let keyed = MutationOptions {
        idempotency_key: Some(b"race-1".to_vec()),
        ..MutationOptions::default()
    };
    let args = put_args("a", "first");
    let first = node.mutate(b"db", "app", "put", &args, &keyed);
    let second = node.mutate(b"db", "app", "put", &args, &keyed);
    let (one, two) = tokio::join!(first, second);
    let one = one.unwrap();
    let two = two.unwrap();
    assert_eq!(one.outcome, two.outcome);
    assert_eq!(one.revision, 1);
    assert_eq!(two.revision, 1);
    let row = node
        .query(b"db", "app", "get", &get_args("a"), &CallOptions::default())
        .await
        .unwrap();
    assert_eq!(row, obj(vec![("title", s("first"))]));
    node.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn three_nodes_replicate_and_forward() {
    let dir = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let runtime = Arc::new(factory());
    let mut nodes = Vec::new();
    for id in 1..=3u64 {
        let node = Node::open(
            id,
            &dir.path().join(format!("n{id}")),
            config(),
            InProcessFactory::new(registry.clone()),
            runtime.clone(),
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
    let refs: Vec<&Node> = nodes.iter().collect();
    let leader_id = wait_leader(&refs).await;
    let leader = nodes.iter().find(|node| node.id() == leader_id).unwrap();

    leader
        .deploy(b"db", "app", [9u8; 32], b"wasm-v1", &app_manifest())
        .await
        .unwrap();
    let put = leader
        .mutate(
            b"db",
            "app",
            "put",
            &put_args("a", "first"),
            &MutationOptions::default(),
        )
        .await
        .unwrap();
    for node in &nodes {
        wait_applied(node, put.position).await;
    }

    // Every engine converges to the committed value.
    for node in &nodes {
        let engine = node.engine().lock().await;
        let got = engine
            .invoke_query(b"db", "app", "get", &get_args("a"), &CallOptions::default())
            .unwrap();
        assert_eq!(got, obj(vec![("title", s("first"))]));
    }

    // Followers answer linearizable queries through the leader's read
    // fence; writes forward to the leader (the API sends them on).
    let follower = nodes.iter().find(|node| node.id() != leader_id).unwrap();
    assert_eq!(
        follower
            .query(b"db", "app", "get", &get_args("a"), &CallOptions::default())
            .await
            .unwrap(),
        obj(vec![("title", s("first"))])
    );
    assert_eq!(
        follower
            .mutate(
                b"db",
                "app",
                "put",
                &put_args("z", "nope"),
                &MutationOptions::default(),
            )
            .await
            .unwrap_err(),
        NodeError::Forward {
            leader: Some(leader_id)
        }
    );

    // A second deployment activates on every member.
    leader
        .deploy(b"db", "app", [10u8; 32], b"wasm-v2", &app_manifest_v2())
        .await
        .unwrap();
    for node in &nodes {
        wait_identity(node, b"db", "app", &[10u8; 32]).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn restart_recovers_data_and_guard() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("n1");
    {
        let registry = Registry::new();
        let node = Node::open(
            1,
            &path,
            config(),
            InProcessFactory::new(registry.clone()),
            Arc::new(factory()),
        )
        .await
        .unwrap();
        registry.register(1, node.raft().clone());
        node.initialize(BTreeSet::from([1])).await.unwrap();
        assert_eq!(wait_leader(&[&node]).await, 1);
        node.deploy(b"db", "app", [9u8; 32], b"wasm-v1", &app_manifest())
            .await
            .unwrap();
        node.mutate(
            b"db",
            "app",
            "put",
            &put_args("a", "first"),
            &MutationOptions::default(),
        )
        .await
        .unwrap();
        node.shutdown().await.unwrap();
    }
    {
        let registry = Registry::new();
        let node = Node::open(
            1,
            &path,
            config(),
            InProcessFactory::new(registry.clone()),
            Arc::new(factory()),
        )
        .await
        .unwrap();
        registry.register(1, node.raft().clone());
        // The single voter re-elects itself; no initialize call.
        assert_eq!(wait_leader(&[&node]).await, 1);
        let got = node
            .query(b"db", "app", "get", &get_args("a"), &CallOptions::default())
            .await
            .unwrap();
        assert_eq!(got, obj(vec![("title", s("first"))]));
        let put = node
            .mutate(
                b"db",
                "app",
                "put",
                &put_args("b", "second"),
                &MutationOptions::default(),
            )
            .await
            .unwrap();
        assert_eq!(put.revision, 2);
        // The replicated guard survived recovery.
        {
            let mut engine = node.engine().lock().await;
            let error = engine
                .invoke_mutation(
                    b"db",
                    "app",
                    "put",
                    &put_args("c", "local"),
                    &MutationOptions::default(),
                )
                .unwrap_err();
            assert_eq!(error.code, "replicated_db");
        }
        node.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn sync_serves_entries_then_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("n1");
    {
        let registry = Registry::new();
        let node = Node::open(
            1,
            &path,
            config(),
            InProcessFactory::new(registry.clone()),
            Arc::new(factory()),
        )
        .await
        .unwrap();
        registry.register(1, node.raft().clone());
        node.initialize(BTreeSet::from([1])).await.unwrap();
        assert_eq!(wait_leader(&[&node]).await, 1);
        node.deploy(b"db", "app", [9u8; 32], b"wasm-v1", &app_manifest())
            .await
            .unwrap();
        for (key, title) in [("a", "first"), ("b", "second"), ("c", "third")] {
            node.mutate(
                b"db",
                "app",
                "put",
                &put_args(key, title),
                &MutationOptions::default(),
            )
            .await
            .unwrap();
        }
        node.shutdown().await.unwrap();
    }

    let logs = LogStore::open(&path.join("raft.redb")).unwrap();
    let applied = logs
        .read_entries(0, u64::MAX)
        .unwrap()
        .last()
        .unwrap()
        .log_id
        .index;
    let batch = serve_since(&logs, 0, 100, applied, 0).unwrap();
    assert!(batch.snapshot.is_none());
    // One deployment plus three writes, one at a time, so a batch
    // each; blanks and membership skip.
    assert_eq!(batch.entries.len(), 4);
    assert!(matches!(
        batch.entries[0].write,
        ClusterWrite::Deploy { .. }
    ));
    assert!(matches!(
        &batch.entries[1].write,
        ClusterWrite::Batch { items, .. } if items.len() == 1
    ));
    assert!(batch.current >= batch.entries.last().unwrap().index);
    let tail = serve_since(&logs, batch.entries[0].index, 100, applied, 0).unwrap();
    assert_eq!(tail.entries.len(), 3);
    assert!(tail.snapshot.is_none());

    // Persist a snapshot, purge the log, then fall back to it.
    let store = GroupStore::open(path.join("group.redb")).unwrap();
    let engine = Arc::new(tokio::sync::Mutex::new(Engine::new(store)));
    let runtimes = Arc::new(factory());
    let (commits, _) = tokio::sync::broadcast::channel(8);
    let mut machine = StateMachine::new(engine, logs.clone(), runtimes.clone(), commits);
    let mut builder = machine.get_snapshot_builder().await;
    let snapshot = builder.build_snapshot().await.unwrap();
    let last = snapshot.meta.last_log_id.expect("snapshot covers logs");
    let mut writable = logs.clone();
    writable.purge(last).await.unwrap();
    let batch = serve_since(&logs, 0, 100, applied, 0).unwrap();
    assert!(batch.entries.is_empty());
    let snap = batch.snapshot.expect("snapshot fallback");
    assert_eq!(snap.last_index, last.index);
    let file: SyncSnapshotFile = serde_json::from_slice(&snap.bytes).unwrap();
    assert_eq!(file.dbs.len(), 2);

    // Install the snapshot into a fresh engine: the device loop.
    let fresh = GroupStore::create(dir.path().join("fresh.redb")).unwrap();
    for db in &file.dbs {
        let meta = SnapshotMeta {
            db: db.meta.db.clone(),
            revision: db.meta.revision,
            applied_log: db.meta.applied_log,
            incarnation: db.meta.incarnation,
        };
        apply_snapshot(&fresh, &meta, &db.payload).unwrap();
    }
    let mut engine = Engine::new(fresh);
    let read = engine.store().begin_read().unwrap();
    let rows = read
        .scan_meta_prefix(b"db", flower_engine::staging::CODE_IDENTITY_PREFIX)
        .unwrap();
    let mut modules = Vec::new();
    for (name, identity) in &rows {
        let module =
            String::from_utf8(name[flower_engine::staging::CODE_IDENTITY_PREFIX.len()..].to_vec())
                .unwrap();
        let mut hash = [0u8; 32];
        hash.copy_from_slice(identity);
        let (wasm, manifest) = read.get_code(b"db", &hash).unwrap().unwrap();
        modules.push((module, wasm, manifest));
    }
    drop(read);
    for (module, wasm, manifest) in &modules {
        let manifest = flower_engine::json::parse_value(manifest).unwrap();
        let runtime = runtimes.build(b"db", module, wasm, &manifest).unwrap();
        engine.reload_module(b"db", module, runtime).unwrap();
    }
    let got = engine
        .invoke_query(b"db", "app", "get", &get_args("b"), &CallOptions::default())
        .unwrap();
    assert_eq!(got, obj(vec![("title", s("second"))]));
}

#[tokio::test(flavor = "multi_thread")]
async fn tcp_two_nodes_replicate() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = Arc::new(factory());
    let node1 = Node::open(
        1,
        &dir.path().join("n1"),
        config(),
        TcpFactory::new(1, PeerConfig::with_token("peer-secret")),
        runtime.clone(),
    )
    .await
    .unwrap();
    let node2 = Node::open(
        2,
        &dir.path().join("n2"),
        config(),
        TcpFactory::new(2, PeerConfig::with_token("peer-secret")),
        runtime.clone(),
    )
    .await
    .unwrap();
    let listener1 = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr1 = listener1.local_addr().unwrap().to_string();
    let listener2 = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr2 = listener2.local_addr().unwrap().to_string();
    let _serve1 = node1.serve_listener(listener1, PeerConfig::with_token("peer-secret"));
    let _serve2 = node2.serve_listener(listener2, PeerConfig::with_token("peer-secret"));
    node1
        .initialize_nodes(BTreeMap::from([
            (1, BasicNode { addr: addr1 }),
            (2, BasicNode { addr: addr2 }),
        ]))
        .await
        .unwrap();
    let leader_id = wait_leader(&[&node1, &node2]).await;
    let (leader, follower) = if leader_id == 1 {
        (&node1, &node2)
    } else {
        (&node2, &node1)
    };
    leader
        .deploy(b"db", "app", [9u8; 32], b"wasm-v1", &app_manifest())
        .await
        .unwrap();
    let put = leader
        .mutate(
            b"db",
            "app",
            "put",
            &put_args("a", "first"),
            &MutationOptions::default(),
        )
        .await
        .unwrap();
    wait_applied(follower, put.position).await;
    let engine = follower.engine().lock().await;
    let got = engine
        .invoke_query(b"db", "app", "get", &get_args("a"), &CallOptions::default())
        .unwrap();
    assert_eq!(got, obj(vec![("title", s("first"))]));
}

fn schedule_args(id: &str, due_ms: f64, handler: &str, args: Value, max: f64) -> Value {
    obj(vec![
        ("id", s(id)),
        ("due_ms", Value::Number(due_ms)),
        ("handler", s(handler)),
        ("args", args),
        ("max", Value::Number(max)),
    ])
}

#[tokio::test(flavor = "multi_thread")]
async fn single_node_timer_fires_through_consensus() {
    let dir = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let node = open_single_node(dir.path(), &registry).await;
    assert_eq!(wait_leader(&[&node]).await, 1);
    node.deploy(b"db", "app", [9u8; 32], b"wasm-v1", &app_manifest())
        .await
        .unwrap();

    // Schedule through consensus with the put handler.
    node.mutate(
        b"db",
        "app",
        "schedule",
        &schedule_args("t1", 0.0, "put", put_args("t", "timer"), 3.0),
        &MutationOptions::default(),
    )
    .await
    .unwrap();
    // Fire through consensus: the row lands and the timer vanishes.
    assert_eq!(
        node.fire_timer(b"db", "t1").await.unwrap(),
        FireReport::Fired
    );
    assert_eq!(
        node.fire_timer(b"db", "t1").await.unwrap(),
        FireReport::Skipped
    );
    let got = node
        .query(b"db", "app", "get", &get_args("t"), &CallOptions::default())
        .await
        .unwrap();
    assert_eq!(got, obj(vec![("title", s("timer"))]));
    let engine = node.engine().lock().await;
    assert!(engine.get_timer(b"db", "t1").unwrap().is_none());
    drop(engine);

    node.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn timer_sweep_fires_due_and_skips_future() {
    let dir = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let node = open_single_node(dir.path(), &registry).await;
    assert_eq!(wait_leader(&[&node]).await, 1);
    node.deploy(b"db", "app", [9u8; 32], b"wasm-v1", &app_manifest())
        .await
        .unwrap();

    // One due timer and one far-future timer, scheduled directly.
    {
        let engine = node.engine().lock().await;
        engine
            .schedule_timer(b"db", "due", "app", "put", &put_args("d", "due"), 0, 3, 0)
            .unwrap();
        engine
            .schedule_timer(
                b"db",
                "future",
                "app",
                "put",
                &put_args("f", "future"),
                u64::MAX,
                3,
                0,
            )
            .unwrap();
    }
    let report = node.sweep_timers_once().await.unwrap();
    assert!(report.leader);
    assert_eq!(report.dbs, 1);
    assert_eq!(report.fired, 1);
    assert_eq!(report.retried, 0);
    let engine = node.engine().lock().await;
    assert!(engine.get_timer(b"db", "due").unwrap().is_none());
    assert!(engine.get_timer(b"db", "future").unwrap().is_some());
    drop(engine);
    // The future timer never fires: the next pass is quiet.
    let report = node.sweep_timers_once().await.unwrap();
    assert_eq!(report.fired, 0);

    node.shutdown().await.unwrap();
}

/// A database that does not serve here (frozen for a move, say) keeps
/// its due timers, untouched, for a later pass or its new group; the
/// sweep still fires every other database's.
#[tokio::test(flavor = "multi_thread")]
async fn timer_sweep_leaves_a_frozen_database_and_fires_the_others() {
    let dir = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let node = open_single_node(dir.path(), &registry).await;
    assert_eq!(wait_leader(&[&node]).await, 1);
    for db in [b"a", b"b"] {
        node.deploy(db, "app", [9u8; 32], b"wasm-v1", &app_manifest())
            .await
            .unwrap();
    }
    for command in [
        PlacementCommand::Create {
            epoch: 1,
            op: "create-a".into(),
        },
        PlacementCommand::Freeze {
            epoch: 1,
            op: "move-a".into(),
        },
    ] {
        node.place(b"a", command).await.unwrap().unwrap();
    }
    {
        let engine = node.engine().lock().await;
        for db in [b"a", b"b"] {
            engine
                .schedule_timer(db, "due", "app", "put", &put_args("d", "due"), 0, 3, 0)
                .unwrap();
        }
    }
    let report = node.sweep_timers_once().await.unwrap();
    assert_eq!((report.fired, report.retried, report.skipped), (1, 0, 1));
    let engine = node.engine().lock().await;
    assert_eq!(
        engine
            .get_timer(b"a", "due")
            .unwrap()
            .map(|timer| timer.attempts),
        Some(0)
    );
    assert!(engine.get_timer(b"b", "due").unwrap().is_none());
    drop(engine);
    node.shutdown().await.unwrap();
}

/// A removed module's due timer is held for it through consensus,
/// attempts untouched, and leaves later passes quiet; deployed again,
/// the module gets it fired.
#[tokio::test(flavor = "multi_thread")]
async fn timer_sweep_holds_a_removed_module_s_timers_until_it_returns() {
    let dir = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let node = open_single_node(dir.path(), &registry).await;
    assert_eq!(wait_leader(&[&node]).await, 1);
    for module in ["app", "side"] {
        node.deploy(b"db", module, [9u8; 32], b"wasm-v1", &app_manifest())
            .await
            .unwrap();
    }
    {
        let engine = node.engine().lock().await;
        for module in ["app", "side"] {
            engine
                .schedule_timer(
                    b"db",
                    module,
                    module,
                    "put",
                    &put_args(module, "due"),
                    0,
                    1,
                    0,
                )
                .unwrap();
        }
    }
    node.remove_module(b"db", "side", false).await.unwrap();
    let report = node.sweep_timers_once().await.unwrap();
    assert_eq!(
        (report.fired, report.retried, report.held),
        (1, 0, 1),
        "{report:?}"
    );
    let held = node
        .engine()
        .lock()
        .await
        .get_timer(b"db", "side")
        .unwrap()
        .unwrap();
    assert_eq!((held.state, held.attempts), (TimerState::Held, 0));
    let report = node.sweep_timers_once().await.unwrap();
    assert_eq!(
        (report.fired, report.retried, report.held, report.skipped),
        (0, 0, 0, 0),
        "{report:?}"
    );

    node.deploy(b"db", "side", [9u8; 32], b"wasm-v1", &app_manifest())
        .await
        .unwrap();
    let report = node.sweep_timers_once().await.unwrap();
    assert_eq!((report.fired, report.held), (1, 0), "{report:?}");
    let engine = node.engine().lock().await;
    assert!(engine.get_timer(b"db", "side").unwrap().is_none());
    drop(engine);
    node.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn failed_timer_fire_retries_then_dies() {
    let dir = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let node = open_single_node(dir.path(), &registry).await;
    assert_eq!(wait_leader(&[&node]).await, 1);
    node.deploy(b"db", "app", [9u8; 32], b"wasm-v1", &app_manifest())
        .await
        .unwrap();

    // Keyless args fail the put handler every fire.
    {
        let engine = node.engine().lock().await;
        engine
            .schedule_timer(b"db", "t1", "app", "put", &obj(vec![]), 0, 1, 0)
            .unwrap();
    }
    assert_eq!(
        node.fire_timer(b"db", "t1").await.unwrap(),
        FireReport::Retried
    );
    let engine = node.engine().lock().await;
    let timer = engine.get_timer(b"db", "t1").unwrap().unwrap();
    assert_eq!(timer.attempts, 1);
    drop(engine);
    // One attempt spent of one: the timer is dead now.
    assert_eq!(
        node.fire_timer(b"db", "t1").await.unwrap(),
        FireReport::Skipped
    );

    node.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn follower_sweep_never_fires() {
    let dir = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let runtime = Arc::new(factory());
    let mut nodes = Vec::new();
    for id in 1..=3u64 {
        let node = Node::open(
            id,
            &dir.path().join(format!("n{id}")),
            config(),
            InProcessFactory::new(registry.clone()),
            runtime.clone(),
        )
        .await
        .unwrap();
        registry.register(id, node.raft().clone());
        nodes.push(node);
    }
    nodes[0]
        .initialize(BTreeSet::from([1, 2, 3]))
        .await
        .unwrap();
    let refs: Vec<&Node> = nodes.iter().collect();
    let leader_id = wait_leader(&refs).await;
    let leader = nodes.iter().find(|node| node.id() == leader_id).unwrap();
    let follower = nodes.iter().find(|node| node.id() != leader_id).unwrap();

    leader
        .deploy(b"db", "app", [9u8; 32], b"wasm-v1", &app_manifest())
        .await
        .unwrap();
    // The timer replicates through consensus.
    let scheduled = leader
        .mutate(
            b"db",
            "app",
            "schedule",
            &schedule_args("t1", 0.0, "put", put_args("r", "replicated"), 3.0),
            &MutationOptions::default(),
        )
        .await
        .unwrap();
    wait_applied(follower, scheduled.position).await;
    {
        let engine = follower.engine().lock().await;
        assert!(engine.get_timer(b"db", "t1").unwrap().is_some());
    }
    // The follower's sweep reports without firing.
    let report = follower.sweep_timers_once().await.unwrap();
    assert!(!report.leader);
    assert_eq!(report.fired, 0);
    // The leader fires; the row replicates to the follower.
    assert_eq!(
        leader.fire_timer(b"db", "t1").await.unwrap(),
        FireReport::Fired
    );
    let status = leader.status();
    wait_applied(follower, status.last_applied.unwrap_or(0)).await;
    let engine = follower.engine().lock().await;
    let got = engine
        .invoke_query(b"db", "app", "get", &get_args("r"), &CallOptions::default())
        .unwrap();
    assert_eq!(got, obj(vec![("title", s("replicated"))]));
    assert!(engine.get_timer(b"db", "t1").unwrap().is_none());
}

/// Set when the slow query starts and finishes blocking.
static SLOW_STARTED: AtomicBool = AtomicBool::new(false);
static SLOW_DONE: AtomicBool = AtomicBool::new(false);

/// Poll a condition until it holds or a generous deadline passes.
async fn eventually<F, Fut>(what: &str, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while tokio::time::Instant::now() < deadline {
        if check().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("timed out waiting for {what}");
}

/// Open an initialized single in-process node with the test app deployed.
async fn single_node(dir: &std::path::Path) -> (Registry, Node) {
    let registry = Registry::new();
    let node = open_single_node(dir, &registry).await;
    assert_eq!(wait_leader(&[&node]).await, 1);
    node.deploy(b"db", "app", [9u8; 32], b"wasm-v1", &app_manifest())
        .await
        .unwrap();
    (registry, node)
}

/// Open three in-process members and return them with the leader's ID.
async fn three_nodes(dir: &std::path::Path) -> (Registry, Vec<Node>, u64) {
    let registry = Registry::new();
    let runtime = Arc::new(factory());
    let mut nodes = Vec::new();
    for id in 1..=3u64 {
        let node = Node::open(
            id,
            &dir.join(format!("n{id}")),
            config(),
            InProcessFactory::new(registry.clone()),
            runtime.clone(),
        )
        .await
        .unwrap();
        registry.register(id, node.raft().clone());
        nodes.push(node);
    }
    nodes[0]
        .initialize(BTreeSet::from([1, 2, 3]))
        .await
        .unwrap();
    let refs: Vec<&Node> = nodes.iter().collect();
    let leader = wait_leader(&refs).await;
    (registry, nodes, leader)
}

/// A hello frame presenting `token`.
fn hello(token: &str) -> Vec<u8> {
    hello_sending(token, flower_engine::patch::max_transaction_bytes() as u64)
}

/// A hello from a dialer whose entries reach `max_entry` bytes.
fn hello_sending(token: &str, max_entry: u64) -> Vec<u8> {
    let contract = flower_cluster::compat::contract();
    let mut frame = b"FLRP\x04".to_vec();
    frame.extend_from_slice(&(token.len() as u16).to_be_bytes());
    frame.extend_from_slice(token.as_bytes());
    // From node 9 to node 1 in the root group, this build's contract.
    frame.extend_from_slice(&9u64.to_be_bytes());
    frame.extend_from_slice(&1u64.to_be_bytes());
    frame.extend_from_slice(b"\x04root");
    frame.extend_from_slice(&0u64.to_be_bytes());
    frame.extend_from_slice(&(contract.len() as u16).to_be_bytes());
    frame.extend_from_slice(contract.as_bytes());
    frame.extend_from_slice(&max_entry.to_be_bytes());
    frame
}

/// Read until the peer closes, tolerating resets: a closed connection.
async fn closed(socket: &mut tokio::net::TcpStream) -> bool {
    use tokio::io::AsyncReadExt;
    let mut rest = Vec::new();
    match tokio::time::timeout(Duration::from_secs(10), socket.read_to_end(&mut rest)).await {
        Ok(Ok(_)) | Ok(Err(_)) => rest.is_empty(),
        Err(_) => false,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn tcp_listener_authenticates_before_reading_frames() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let dir = tempfile::tempdir().unwrap();
    let node = Node::open(
        1,
        &dir.path().join("n1"),
        config(),
        TcpFactory::new(1, PeerConfig::with_token("right")),
        Arc::new(factory()),
    )
    .await
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let _serve = node.serve_listener(listener, PeerConfig::with_token("right"));

    // A wrong token is rejected and the connection closes.
    let mut socket = tokio::net::TcpStream::connect(addr).await.unwrap();
    socket.write_all(&hello("wrong")).await.unwrap();
    assert_eq!(socket.read_u8().await.unwrap(), 1);
    assert!(closed(&mut socket).await);

    // A bare frame without a hello never reaches Raft, whatever
    // length it claims.
    let mut socket = tokio::net::TcpStream::connect(addr).await.unwrap();
    socket.write_all(&(1u64 << 40).to_be_bytes()).await.unwrap();
    // The listener rejects after the 7-byte hello header; the byte it
    // left unread may turn its close into a reset.
    match socket.read_u8().await {
        Ok(ack) => assert_eq!(ack, 1),
        Err(error) => assert_eq!(error.kind(), std::io::ErrorKind::ConnectionReset),
    }
    assert!(closed(&mut socket).await);

    // The right token is accepted; a frame claiming a terabyte is
    // refused by the cap without being allocated, and the
    // connection closes without an answer.
    let mut socket = tokio::net::TcpStream::connect(addr).await.unwrap();
    socket.write_all(&hello("right")).await.unwrap();
    assert_eq!(socket.read_u8().await.unwrap(), 0);
    // The acknowledgement names the listener and its contract.
    assert_eq!(socket.read_u64().await.unwrap(), 1);
    let len = socket.read_u16().await.unwrap() as usize;
    let mut contract = vec![0u8; len];
    socket.read_exact(&mut contract).await.unwrap();
    assert_eq!(contract, flower_cluster::compat::contract().as_bytes());
    socket.write_all(&(1u64 << 40).to_be_bytes()).await.unwrap();
    assert!(closed(&mut socket).await);

    // A listener at its connection cap with no connection idle (this
    // one still owes its hello) closes extra dials at once, and a
    // dialer that never says hello is dropped.
    let capped = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let capped_addr = capped.local_addr().unwrap();
    let _capped = node.serve_listener(
        capped,
        PeerConfig {
            max_connections: 1,
            handshake_timeout: Duration::from_millis(500),
            ..PeerConfig::with_token("right")
        },
    );
    let mut idle = tokio::net::TcpStream::connect(capped_addr).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut extra = tokio::net::TcpStream::connect(capped_addr).await.unwrap();
    let started = std::time::Instant::now();
    assert!(closed(&mut extra).await);
    assert!(started.elapsed() < Duration::from_millis(450));
    assert_eq!(idle.read_u8().await.unwrap(), 1);
    assert!(closed(&mut idle).await);
}

/// A listener refuses a dialer whose largest entry its frame cap cannot
/// carry: every member's inbound cap must fit what any leader proposes,
/// which one member's own startup check cannot see.
#[tokio::test(flavor = "multi_thread")]
async fn a_listener_refuses_dialers_whose_entries_its_frames_cannot_carry() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let dir = tempfile::tempdir().unwrap();
    let node = Node::open(
        1,
        &dir.path().join("n1"),
        config(),
        TcpFactory::new(1, PeerConfig::with_token("right")),
        Arc::new(factory()),
    )
    .await
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let _serve = node.serve_listener(
        listener,
        PeerConfig {
            max_frame: 16 * 1024 * 1024,
            ..PeerConfig::with_token("right")
        },
    );
    // Entries of up to 32 MiB need 129 MiB frames: refused, naming the
    // listener so the dialer can say which cap to raise.
    let mut socket = tokio::net::TcpStream::connect(addr).await.unwrap();
    socket
        .write_all(&hello_sending("right", 32 * 1024 * 1024))
        .await
        .unwrap();
    assert_eq!(socket.read_u8().await.unwrap(), 5);
    assert_eq!(socket.read_u64().await.unwrap(), 1);
    // Entries of up to 3 MiB fit: accepted.
    let mut socket = tokio::net::TcpStream::connect(addr).await.unwrap();
    socket
        .write_all(&hello_sending("right", 3 * 1024 * 1024))
        .await
        .unwrap();
    assert_eq!(socket.read_u8().await.unwrap(), 0);
}

/// Say hello with the right token and read the acceptance through.
async fn accepted(addr: std::net::SocketAddr) -> tokio::net::TcpStream {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut socket = tokio::net::TcpStream::connect(addr).await.unwrap();
    socket.write_all(&hello("right")).await.unwrap();
    assert_eq!(socket.read_u8().await.unwrap(), 0, "the hello was refused");
    assert_eq!(socket.read_u64().await.unwrap(), 1);
    let len = socket.read_u16().await.unwrap() as usize;
    let mut contract = vec![0u8; len];
    socket.read_exact(&mut contract).await.unwrap();
    socket
}

/// Whether nothing arrives and nothing closes on `socket` for a while.
async fn open(socket: &mut tokio::net::TcpStream) -> bool {
    use tokio::io::AsyncReadExt;
    let mut byte = [0u8; 1];
    tokio::time::timeout(Duration::from_millis(200), socket.read(&mut byte))
        .await
        .is_err()
}

/// Idle connections, which dialers pool for every group and peer, never
/// keep a dial out: at its cap the listener closes the connection idle
/// longest to take the new one. Refused instead, the dial of a
/// heartbeat or a vote failed while pooled connections nobody used held
/// every place, and a busy host's groups lost their leaders.
#[tokio::test(flavor = "multi_thread")]
async fn a_listener_at_its_cap_closes_its_longest_idle_connection_for_a_dial() {
    use tokio::io::AsyncReadExt;
    let dir = tempfile::tempdir().unwrap();
    let node = Node::open(
        1,
        &dir.path().join("n1"),
        config(),
        TcpFactory::new(1, PeerConfig::with_token("right")),
        Arc::new(factory()),
    )
    .await
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let _serve = node.serve_listener(
        listener,
        PeerConfig {
            max_connections: 2,
            ..PeerConfig::with_token("right")
        },
    );
    let mut oldest = accepted(addr).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut newer = accepted(addr).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    // Both places are taken by idle connections: a third dial is
    // accepted, and the connection idle longest closes for it, saying so
    // first (an empty frame, which no response is).
    let mut third = accepted(addr).await;
    assert_eq!(oldest.read_u64().await.unwrap(), 0, "no eviction notice");
    assert!(
        closed(&mut oldest).await,
        "the longest-idle connection stayed"
    );
    assert!(open(&mut newer).await, "a newer idle connection closed");
    assert!(open(&mut third).await, "the new connection closed");
}

/// Dials that have not passed their hello (no token, a wrong one, or
/// nothing at all) take places of their own, at most the cap, and never
/// close an established connection: a dialer without the token can
/// delay other dials, not cut members apart. Evicting at accept, before
/// the hello, let such a flood close every idle peer connection,
/// replication streams between heartbeats included, and hold the
/// places their reconnects needed.
#[tokio::test(flavor = "multi_thread")]
async fn dials_without_the_token_never_close_established_connections() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let dir = tempfile::tempdir().unwrap();
    let node = Node::open(
        1,
        &dir.path().join("n1"),
        config(),
        TcpFactory::new(1, PeerConfig::with_token("right")),
        Arc::new(factory()),
    )
    .await
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let _serve = node.serve_listener(
        listener,
        PeerConfig {
            max_connections: 2,
            handshake_timeout: Duration::from_secs(2),
            ..PeerConfig::with_token("right")
        },
    );
    let mut established = accepted(addr).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    // Silent dials take the two places for hellos; more close at once.
    let mut silent = Vec::new();
    for _ in 0..2 {
        silent.push(tokio::net::TcpStream::connect(addr).await.unwrap());
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut extra = tokio::net::TcpStream::connect(addr).await.unwrap();
    let started = std::time::Instant::now();
    assert!(closed(&mut extra).await);
    assert!(started.elapsed() < Duration::from_secs(1));
    // A wrong token, once a place frees, is refused as before.
    drop(silent.pop());
    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut wrong = tokio::net::TcpStream::connect(addr).await.unwrap();
    wrong.write_all(&hello("wrong")).await.unwrap();
    assert_eq!(wrong.read_u8().await.unwrap(), 1);
    // The established connection stayed through it all.
    assert!(
        open(&mut established).await,
        "an established connection closed"
    );
    drop(silent);
}

/// A three-member group keeps its leader and commits while dialers
/// without the token flood every member's peer listener (production
/// timing: a 300 ms lease). The flood holds the places for hellos; the
/// members' established connections, replication streams idle between
/// heartbeats, are never closed for it.
#[tokio::test(flavor = "multi_thread")]
async fn a_group_keeps_its_leader_while_strangers_flood_its_listeners() {
    use std::sync::atomic::{AtomicBool, Ordering};

    use flower_cluster::timing::{RaftTiming, SnapshotSettings, TimingSettings, raft_config};
    let timing = RaftTiming::derive(&TimingSettings::default()).unwrap();
    let config = Arc::new(raft_config(&timing, &SnapshotSettings::default()).unwrap());
    let dir = tempfile::tempdir().unwrap();
    let runtime = Arc::new(factory());
    let peers = PeerConfig::with_token("peer-secret");
    let listening = PeerConfig {
        max_connections: 8,
        handshake_timeout: Duration::from_secs(3),
        ..peers.clone()
    };
    let (mut nodes, mut members, mut servers, mut addrs) =
        (Vec::new(), BTreeMap::new(), Vec::new(), Vec::new());
    for id in 1..=3u64 {
        let node = Node::open(
            id,
            &dir.path().join(format!("n{id}")),
            config.clone(),
            TcpFactory::new(id, peers.clone()),
            runtime.clone(),
        )
        .await
        .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        members.insert(
            id,
            BasicNode {
                addr: addr.to_string(),
            },
        );
        addrs.push(addr);
        servers.push(node.serve_listener(listener, listening.clone()));
        nodes.push(node);
    }
    nodes[0].initialize_nodes(members).await.unwrap();
    let refs: Vec<&Node> = nodes.iter().collect();
    let leader_id = wait_leader(&refs).await;
    let leader = nodes.iter().find(|node| node.id() == leader_id).unwrap();
    leader
        .deploy(b"db", "app", [9u8; 32], b"wasm-v1", &app_manifest())
        .await
        .unwrap();
    let terms = || {
        nodes
            .iter()
            .map(|node| {
                let status = node.status();
                (node.id(), status.term, status.leader)
            })
            .collect::<Vec<_>>()
    };
    for _ in 0..200 {
        if terms()
            .iter()
            .all(|(_, _, leader)| *leader == Some(leader_id))
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let before = terms();

    // Every listener sees a new silent dial every 10 ms for three
    // seconds, each held until the listener drops it.
    let flooding = Arc::new(AtomicBool::new(true));
    let floods: Vec<_> = addrs
        .iter()
        .map(|addr| {
            let (addr, flooding) = (*addr, flooding.clone());
            tokio::spawn(async move {
                let mut held = Vec::new();
                while flooding.load(Ordering::SeqCst) {
                    if let Ok(socket) = tokio::net::TcpStream::connect(addr).await {
                        held.push(socket);
                    }
                    if held.len() > 64 {
                        held.remove(0);
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
        })
        .collect();
    let started = std::time::Instant::now();
    let mut written = 0u32;
    while started.elapsed() < Duration::from_secs(3) {
        leader
            .mutate(
                b"db",
                "app",
                "put",
                &put_args(&format!("k{written}"), "flooded"),
                &MutationOptions::default(),
            )
            .await
            .unwrap_or_else(|error| panic!("write {written} failed: {error}"));
        written += 1;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    flooding.store(false, Ordering::SeqCst);
    for flood in floods {
        flood.await.unwrap();
    }
    assert!(written > 10, "{written} writes");
    assert_eq!(terms(), before);
    drop(servers);
}

#[tokio::test(flavor = "multi_thread")]
async fn sync_never_serves_unapplied_entries() {
    let dir = tempfile::tempdir().unwrap();
    let (registry, nodes, leader_id) = three_nodes(dir.path()).await;
    let leader = nodes.iter().find(|node| node.id() == leader_id).unwrap();
    // Anchor on the deploy's committed position: metrics update
    // asynchronously and can lag it under load.
    let applied = leader
        .deploy(b"db", "app", [9u8; 32], b"wasm-v1", &app_manifest())
        .await
        .unwrap()
        .position;
    eventually("metrics catch up with the deploy", || async {
        leader.status().last_applied.unwrap_or(0) >= applied
            && leader.status().last_log.unwrap_or(0) == applied
    })
    .await;

    // Cut the leader off: a new write lands in its log but can never
    // commit, so a new leader may overwrite it.
    for node in &nodes {
        if node.id() != leader_id {
            registry.unregister(node.id());
        }
    }
    let patch = leader
        .with_engine(|engine| {
            let prepared = engine
                .prepare_mutation(
                    b"db",
                    "app",
                    "put",
                    &put_args("z", "phantom"),
                    &MutationOptions::default(),
                )
                .unwrap();
            match prepared {
                flower_engine::engine::PrepareOutcome::Patch(prepared) => Ok(prepared.patch),
                _ => panic!("expected a patch"),
            }
        })
        .await
        .unwrap();
    let pending = {
        let raft = leader.raft().clone();
        tokio::spawn(async move {
            let _ = raft
                .client_write(ClusterWrite::Patch {
                    db: b"db".to_vec(),
                    patch,
                    proposed_ms: 0,
                })
                .await;
        })
    };
    eventually("the uncommitted append", || async {
        leader.status().last_log.unwrap_or(0) > applied
    })
    .await;
    let unapplied = applied + 1;
    let batch = leader.sync_since(0, 4096).unwrap();
    assert_eq!(batch.since, 0);
    assert!(batch.current <= leader.status().last_applied.unwrap());
    assert!(batch.current < unapplied);
    assert!(batch.entries.iter().all(|entry| entry.index < unapplied));
    pending.abort();

    // Server-side caps: the window never passes the applied index and
    // the limit is bounded.
    let logs = leader.logs();
    let capped = serve_since(logs, 0, 1, applied, 0).unwrap();
    assert_eq!(capped.current, 1);
    let huge = serve_since(logs, 0, u64::MAX, applied, 0).unwrap();
    assert_eq!(huge.current, applied);
}

#[tokio::test(flavor = "multi_thread")]
async fn queue_and_timer_state_replicates_and_survives_failover() {
    use flower_cluster::work::{QueueOp, QueueOutcome, TimerOp, TimerOutcome};
    let dir = tempfile::tempdir().unwrap();
    let (registry, nodes, leader_id) = three_nodes(dir.path()).await;
    let leader = nodes.iter().find(|node| node.id() == leader_id).unwrap();
    leader
        .deploy(b"db", "app", [9u8; 32], b"wasm-v1", &app_manifest())
        .await
        .unwrap();
    let enqueued = leader
        .mutate(
            b"db",
            "app",
            "enqueue",
            &obj(vec![("payload", s("job"))]),
            &MutationOptions::default(),
        )
        .await
        .unwrap();
    let ResponseOutcome::Value(id) = &enqueued.outcome else {
        panic!("enqueue failed: {enqueued:?}");
    };
    let id: String = serde_json::from_slice(id).unwrap();
    let far = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as f64
        + 3_600_000.0;
    leader
        .mutate(
            b"db",
            "app",
            "schedule",
            &schedule_args("later", far, "put", put_args("t", "timer"), 3.0),
            &MutationOptions::default(),
        )
        .await
        .unwrap();

    let claimed = leader
        .queue_op(
            b"db",
            "jobs",
            QueueOp::Poll {
                limit: 10,
                lease_ms: 600_000,
            },
        )
        .await
        .unwrap()
        .unwrap();
    let QueueOutcome::Claimed(claimed) = claimed else {
        panic!("poll answered {claimed:?}");
    };
    assert_eq!(claimed.len(), 1);
    let token = claimed[0].token.clone();
    let cancelled = leader
        .timer_op(
            b"db",
            TimerOp::Cancel {
                id: "later".to_string(),
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cancelled, TimerOutcome::Removed(true));

    // Every member holds the claim with the leader's token, and none
    // keeps the cancelled timer.
    let position = leader.status().last_applied.unwrap();
    for node in &nodes {
        wait_applied(node, position).await;
        let item_id = id.clone();
        let (item, timer) = node
            .with_engine(move |engine| {
                Ok((
                    engine.get_queue_item(b"db", "jobs", &item_id).unwrap(),
                    engine.get_timer(b"db", "later").unwrap(),
                ))
            })
            .await
            .unwrap();
        let item = item.expect("claimed item replicated");
        assert_eq!(item.state, flower_engine::work_queue::ItemState::Claimed);
        assert_eq!(item.token.as_deref(), Some(token.as_str()));
        assert!(timer.is_none(), "node {} kept a cancelled timer", node.id());
    }

    // Fail over: the claim survives, nothing redelivers, and the
    // worker's token still completes it.
    registry.unregister(leader_id);
    let mut survivors = Vec::new();
    for node in nodes {
        if node.id() == leader_id {
            node.shutdown().await.unwrap();
        } else {
            survivors.push(node);
        }
    }
    eventually("a new leader", || {
        let elected = survivors
            .iter()
            .any(|node| node.status().leader == Some(node.id()));
        async move { elected }
    })
    .await;
    let leader = survivors
        .iter()
        .find(|node| node.status().leader == Some(node.id()))
        .unwrap();
    // Claiming is new work, shed like any write; settling a claim is
    // admitted as settling, never shed early.
    let settling = || leader.admission().metrics().settling;
    let before = settling();
    let again = leader
        .queue_op(
            b"db",
            "jobs",
            QueueOp::Poll {
                limit: 10,
                lease_ms: 600_000,
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(again, QueueOutcome::Claimed(ref items) if items.is_empty()));
    assert_eq!(settling(), before);
    let renewed = leader
        .queue_op(
            b"db",
            "jobs",
            QueueOp::Heartbeat {
                id: id.clone(),
                token: token.clone(),
                lease_ms: 600_000,
            },
        )
        .await
        .unwrap();
    assert!(renewed.is_ok(), "{renewed:?}");
    assert_eq!(settling(), before + 1);
    let done = leader
        .queue_op(
            b"db",
            "jobs",
            QueueOp::Complete {
                id: id.clone(),
                token,
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(done, QueueOutcome::Done));
    assert_eq!(settling(), before + 2);
    let position = leader.status().last_applied.unwrap();
    for node in &survivors {
        wait_applied(node, position).await;
        let item_id = id.clone();
        let item = node
            .with_engine(move |engine| Ok(engine.get_queue_item(b"db", "jobs", &item_id).unwrap()))
            .await
            .unwrap();
        assert!(item.is_none(), "node {} kept a completed item", node.id());
    }
}

/// Apply a deploy, a keyed write stamped a day ago, then a redelivery
/// of the same key whose proposal stamp runs backwards past the
/// receipt's expiry; optionally purge expired receipts first.
/// Returns the redelivery's revision.
async fn redelivery_revision(purge: bool) -> u64 {
    use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId};
    let dir = tempfile::tempdir().unwrap();
    let store = GroupStore::create(dir.path().join("group.redb")).unwrap();
    let engine = Arc::new(tokio::sync::Mutex::new(Engine::new(store)));
    let logs = LogStore::open(&dir.path().join("raft.redb")).unwrap();
    let (commits, _) = tokio::sync::broadcast::channel(8);
    let mut machine = StateMachine::new(engine.clone(), logs, Arc::new(factory()), commits);
    let entry = |index: u64, write: ClusterWrite| Entry::<flower_cluster::types::TypeConfig> {
        log_id: LogId::new(CommittedLeaderId::new(1, 1), index),
        payload: EntryPayload::Normal(write),
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let old = now - IDEMPOTENCY_TTL_MS - 3_600_000;
    let bindings = {
        let mut engine = engine.lock().await;
        engine
            .deploy_staged(b"db", "app", [9u8; 32], b"wasm-v1", &app_manifest())
            .unwrap();
        while !engine.staging_drain(b"db", "app", 4096).unwrap().done {}
        engine.prepare_activation(b"db", "app").unwrap().bindings
    };
    let manifest = flower_engine::json::render(&app_manifest()).unwrap();
    machine
        .apply(vec![entry(
            1,
            ClusterWrite::Deploy {
                db: b"db".to_vec(),
                module: "app".to_string(),
                wasm: b"wasm-v1".to_vec(),
                manifest,
                identity: [9u8; 32],
                bindings,
                proposed_ms: now,
            },
        )])
        .await
        .unwrap();
    let prepare = |timestamp_ms: Option<u64>| {
        let engine = engine.clone();
        async move {
            let options = MutationOptions {
                call: CallOptions {
                    timestamp_ms,
                    ..CallOptions::default()
                },
                idempotency_key: Some(b"key-1".to_vec()),
                ..MutationOptions::default()
            };
            let prepared = engine
                .lock()
                .await
                .prepare_mutation(b"db", "app", "put", &put_args("a", "first"), &options)
                .unwrap();
            match prepared {
                flower_engine::engine::PrepareOutcome::Patch(prepared) => prepared.patch,
                _ => panic!("expected a patch"),
            }
        }
    };
    let first = prepare(Some(old)).await;
    let applied = machine
        .apply(vec![entry(
            2,
            ClusterWrite::Patch {
                db: b"db".to_vec(),
                patch: first,
                proposed_ms: now,
            },
        )])
        .await
        .unwrap();
    assert_eq!(applied[0].revision, 1);
    // The receipt expired an hour ago; the redelivery executes afresh.
    let again = prepare(None).await;
    if purge {
        let deleted = StateMachine::purge_expired_receipts(&mut *engine.lock().await, 100).unwrap();
        assert_eq!(deleted, 1);
    }
    let applied = machine
        .apply(vec![entry(
            3,
            ClusterWrite::Patch {
                db: b"db".to_vec(),
                patch: again,
                proposed_ms: old + 1,
            },
        )])
        .await
        .unwrap();
    applied[0].revision
}

#[tokio::test(flavor = "multi_thread")]
async fn receipt_purge_timing_never_changes_duplicate_verdicts() {
    // A member that purged the expired receipt and one that kept it
    // must reach the same verdict, even when a later leader's clock
    // stamps the redelivery before the receipt's expiry.
    let purged = redelivery_revision(true).await;
    let kept = redelivery_revision(false).await;
    assert_eq!(purged, kept);
    assert_eq!(kept, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn interrupted_snapshot_install_resumes_on_open() {
    use flower_cluster::logstore::StoredSnapshot;
    let dir = tempfile::tempdir().unwrap();
    {
        let (_registry, node) = single_node(dir.path()).await;
        node.mutate(
            b"db",
            "app",
            "put",
            &put_args("a", "first"),
            &MutationOptions::default(),
        )
        .await
        .unwrap();
        node.shutdown().await.unwrap();
    }
    let stored = {
        let path = dir.path().join("n1");
        let store = GroupStore::open(path.join("group.redb")).unwrap();
        let logs = LogStore::open(&path.join("raft.redb")).unwrap();
        let engine = Arc::new(tokio::sync::Mutex::new(Engine::new(store)));
        let (commits, _) = tokio::sync::broadcast::channel(8);
        let mut machine = StateMachine::new(engine, logs, Arc::new(factory()), commits);
        let mut snapshot = machine
            .get_snapshot_builder()
            .await
            .build_snapshot()
            .await
            .unwrap();
        let mut bytes = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut snapshot.snapshot, &mut bytes)
            .await
            .unwrap();
        StoredSnapshot {
            meta: serde_json::to_vec(&snapshot.meta).unwrap(),
            bytes,
        }
    };
    // A crash right after the install marker: the engine holds
    // nothing of the snapshot yet.
    let fresh = dir.path().join("fresh");
    std::fs::create_dir_all(&fresh).unwrap();
    let logs = LogStore::open(&fresh.join("raft.redb")).unwrap();
    logs.claim_contract(flower_cluster::compat::storage_contract())
        .unwrap()
        .unwrap();
    logs.begin_install(&stored).unwrap();
    drop(logs);
    let registry = Registry::new();
    let node = Node::open(
        1,
        &fresh,
        config(),
        InProcessFactory::new(registry.clone()),
        Arc::new(factory()),
    )
    .await
    .unwrap();
    assert!(node.logs().pending_install().unwrap().is_none());
    let (row, guard) = node
        .with_engine(|engine| {
            let row = engine
                .invoke_query(b"db", "app", "get", &get_args("a"), &CallOptions::default())
                .unwrap();
            let guard = engine
                .invoke_mutation(
                    b"db",
                    "app",
                    "put",
                    &put_args("b", "local"),
                    &MutationOptions::default(),
                )
                .unwrap_err();
            Ok((row, guard))
        })
        .await
        .unwrap();
    assert_eq!(row, obj(vec![("title", s("first"))]));
    // Installed databases are replicated: local writes are refused.
    assert_eq!(guard.code, "replicated_db");
}

#[tokio::test]
async fn engine_work_runs_off_the_async_workers() {
    // One async worker: inline guest execution would block every task
    // on it, this one included, until the query finished.
    let dir = tempfile::tempdir().unwrap();
    let (_registry, node) = single_node(dir.path()).await;
    let node = Arc::new(node);
    SLOW_STARTED.store(false, Ordering::SeqCst);
    SLOW_DONE.store(false, Ordering::SeqCst);
    let slow = {
        let node = node.clone();
        tokio::spawn(async move {
            node.query(b"db", "app", "slow", &obj(vec![]), &CallOptions::default())
                .await
        })
    };
    let mut observed_running = false;
    for _ in 0..500 {
        if SLOW_STARTED.load(Ordering::SeqCst) {
            observed_running = !SLOW_DONE.load(Ordering::SeqCst);
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(observed_running, "the runtime stalled while the guest ran");
    slow.await.unwrap().unwrap();
}

/// A member's incarnation labels: the group's, then `db`'s and `late`'s.
async fn incarnation_labels(node: &Node) -> (u64, u64, u64) {
    use flower_engine::storage::meta::HISTORY_INCARNATION;
    node.with_engine(|engine| {
        let read = engine.store().begin_read().unwrap();
        let label = |db: &[u8]| read.meta_u64(db, HISTORY_INCARNATION).unwrap();
        let group = StateMachine::incarnation(engine).unwrap();
        Ok((group, label(b"db"), label(b"late")))
    })
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn maintenance_establishes_one_incarnation_for_the_group() {
    let dir = tempfile::tempdir().unwrap();
    let (_registry, nodes, leader_id) = three_nodes(dir.path()).await;
    let leader = nodes.iter().find(|node| node.id() == leader_id).unwrap();
    // A database created before the group has an incarnation.
    leader
        .deploy(b"db", "app", [9u8; 32], b"wasm-v1", &app_manifest())
        .await
        .unwrap();
    assert_eq!(leader.incarnation(), 0);

    // Every member runs maintenance, as servers do: the leader
    // proposes and every member applies the same value.
    let (stop, stopped) = tokio::sync::watch::channel(false);
    let interval = Duration::from_millis(20);
    tokio::join!(
        nodes[0].run_maintenance(interval, stopped.clone()),
        nodes[1].run_maintenance(interval, stopped.clone()),
        nodes[2].run_maintenance(interval, stopped.clone()),
        async {
            eventually("every member to apply one incarnation", || {
                let first = nodes[0].incarnation();
                let agreed = first != 0 && nodes.iter().all(|node| node.incarnation() == first);
                async move { agreed }
            })
            .await;
            stop.send(true).unwrap();
        },
    );
    let incarnation = leader.incarnation();

    // A database created afterwards joins the same history. Every
    // member labels both and serves the incarnation with sync batches.
    let late = leader
        .deploy(b"late", "app", [9u8; 32], b"wasm-v1", &app_manifest())
        .await
        .unwrap();
    for node in &nodes {
        wait_applied(node, late.position).await;
        assert_eq!(node.sync_since(0, 4096).unwrap().incarnation, incarnation);
        assert_eq!(
            incarnation_labels(node).await,
            (incarnation, incarnation, incarnation)
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deploy_replaces_a_stale_staging() {
    // A deployment that crashed or lost leadership mid-backfill leaves
    // its staging behind with names only this member interned. The
    // next deployment of the module must replace it, not refuse.
    let dir = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let node = open_single_node(dir.path(), &registry).await;
    assert_eq!(wait_leader(&[&node]).await, 1);
    node.deploy(b"db", "app", [9u8; 32], b"wasm-v1", &app_manifest())
        .await
        .unwrap();
    node.with_engine(|engine| {
        engine
            .deploy_staged(b"db", "app", [8u8; 32], b"wasm-lost", &app_manifest_v2())
            .map_err(|failure| NodeError::Failure {
                code: failure.code,
                message: failure.message,
            })
    })
    .await
    .unwrap();
    node.deploy(b"db", "app", [10u8; 32], b"wasm-v2", &app_manifest_v2())
        .await
        .unwrap();
    wait_identity(&node, b"db", "app", &[10u8; 32]).await;
    let engine = node.engine().lock().await;
    assert!(engine.staging_status(b"db", "app").unwrap().is_none());
    drop(engine);
    node.shutdown().await.unwrap();
}

/// A module of `tasks` that writes many rows at once and exports them.
fn bulk_manifest() -> Value {
    let mut manifest = manifest(
        vec![tasks_collection()],
        vec![
            definition("put", "mutation"),
            definition("get", "query"),
            definition("fill", "mutation"),
        ],
    );
    if let Value::Object(root) = &mut manifest {
        root.insert(
            "exports".to_string(),
            obj(vec![
                ("collections", arr(vec![s("tasks")])),
                ("derived", arr(vec![])),
                ("methods", arr(vec![])),
            ]),
        );
    }
    manifest
}

/// A module importing `bulk`'s tasks.
fn reader_manifest() -> Value {
    let mut manifest = manifest(vec![], vec![definition("get", "query")]);
    if let Value::Object(root) = &mut manifest {
        root.insert(
            "imports".to_string(),
            arr(vec![obj(vec![
                ("module", s("bulk")),
                (
                    "collections",
                    arr(vec![obj(vec![
                        ("name", s("tasks")),
                        ("key", str_schema()),
                        ("row", json_schema()),
                    ])]),
                ),
                ("derived", arr(vec![])),
                ("methods", arr(vec![])),
            ])]),
        );
    }
    manifest
}

/// What a member holds of `module` in `db`: its records and index
/// entries under the names it interned, as full keys in key order.
async fn module_keys(node: &Node, module: &str) -> Vec<Vec<u8>> {
    use flower_engine::storage::SnapshotTable;
    let engine = node.engine().lock().await;
    let read = engine.store().begin_read().unwrap();
    let base = 4 + b"db".len();
    let owned = format!("{module}\0");
    let ids: Vec<Vec<u8>> = read
        .scan_db_raw(SnapshotTable::InternId, b"db")
        .unwrap()
        .into_iter()
        .filter(|(_, name)| name.starts_with(owned.as_bytes()))
        .map(|(key, _)| key[base..base + 4].to_vec())
        .collect();
    let mut keys = Vec::new();
    for table in [SnapshotTable::Records, SnapshotTable::IndexEntries] {
        for (key, _) in read.scan_db_raw(table, b"db").unwrap() {
            if ids.iter().any(|id| key[base..base + 4] == id[..]) {
                keys.push(key);
            }
        }
    }
    keys
}

/// A member's purges in flight in `db`, as the stored row.
async fn purges_row(node: &Node) -> Option<Vec<u8>> {
    let engine = node.engine().lock().await;
    let read = engine.store().begin_read().unwrap();
    read.meta(b"db", flower_engine::purge::PURGES_ROW).unwrap()
}

fn module_conflict(error: NodeError) -> String {
    match error {
        NodeError::Failure { code, message } if code == "MODULE_CONFLICT" => message,
        other => panic!("expected MODULE_CONFLICT: {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_removal_replicates_to_every_member_and_a_refused_one_proposes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let (_registry, nodes, leader_id) = three_nodes(dir.path()).await;
    let leader = nodes.iter().find(|node| node.id() == leader_id).unwrap();
    let follower = nodes.iter().find(|node| node.id() != leader_id).unwrap();
    leader
        .deploy(b"db", "app", [9u8; 32], b"wasm-app", &app_manifest())
        .await
        .unwrap();
    leader
        .deploy(b"db", "bulk", [7u8; 32], b"wasm-bulk", &bulk_manifest())
        .await
        .unwrap();
    leader
        .deploy(
            b"db",
            "reader",
            [6u8; 32],
            b"wasm-reader",
            &reader_manifest(),
        )
        .await
        .unwrap();
    leader
        .mutate(
            b"db",
            "bulk",
            "fill",
            &obj(vec![("rows", Value::Number(10.0))]),
            &MutationOptions::default(),
        )
        .await
        .unwrap();

    // A module another imports from is refused at the leader, before
    // anything reaches the log; a follower sends removals there.
    let before = leader.status().last_log;
    let refused = leader.remove_module(b"db", "bulk", true).await.unwrap_err();
    assert!(module_conflict(refused).contains("`reader`"));
    let ghost = leader
        .remove_module(b"db", "ghost", false)
        .await
        .unwrap_err();
    assert!(module_conflict(ghost).contains("no module `ghost` is deployed"));
    assert_eq!(leader.status().last_log, before);
    assert!(matches!(
        follower.remove_module(b"db", "reader", false).await,
        Err(NodeError::Forward { leader: Some(id) }) if id == leader_id
    ));

    leader.remove_module(b"db", "reader", false).await.unwrap();
    let removed = leader.remove_module(b"db", "bulk", false).await.unwrap();
    match &removed.outcome {
        ResponseOutcome::Value(bytes) => assert_eq!(
            serde_json::from_slice::<serde_json::Value>(bytes).unwrap(),
            serde_json::json!({"purging": false, "removed": "bulk"})
        ),
        outcome => panic!("unexpected outcome: {outcome:?}"),
    }
    let kept = module_keys(leader, "bulk").await;
    assert_eq!(kept.len(), 20);
    // The leader lists it removed, its data and declarations kept.
    let listed = leader.modules(Some(b"db".as_slice())).await.unwrap();
    let removed_names: Vec<&str> = listed[0]
        .removed
        .iter()
        .map(|(name, _)| name.as_str())
        .collect();
    assert_eq!(removed_names, ["bulk", "reader"]);
    assert_eq!(listed[0].removed[0].1, [7u8; 32]);
    assert!(listed[0].purging.is_empty());
    for node in &nodes {
        wait_applied(node, removed.position).await;
        // Every member runs without it and keeps the same data.
        let engine = node.engine().lock().await;
        let call = engine.invoke_query(
            b"db",
            "bulk",
            "get",
            &get_args("f1"),
            &CallOptions::default(),
        );
        assert!(call.is_err(), "node {} still runs bulk", node.id());
        drop(engine);
        assert_eq!(module_keys(node, "bulk").await, kept, "node {}", node.id());
        let names: Vec<String> = node
            .reader()
            .modules(b"db")
            .unwrap()
            .into_iter()
            .map(|module| module.name)
            .collect();
        assert_eq!(names, ["app"], "node {}", node.id());
        let (purging, removed) = node.reader().module_removals(b"db").unwrap();
        assert!(purging.is_empty());
        assert_eq!(removed, listed[0].removed, "node {}", node.id());
    }
    for node in nodes {
        node.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_purge_goes_page_by_page_on_every_member_across_a_leader_change() {
    let dir = tempfile::tempdir().unwrap();
    let (registry, nodes, leader_id) = three_nodes(dir.path()).await;
    let leader = nodes.iter().find(|node| node.id() == leader_id).unwrap();
    leader
        .deploy(b"db", "app", [9u8; 32], b"wasm-app", &app_manifest())
        .await
        .unwrap();
    leader
        .deploy(b"db", "bulk", [7u8; 32], b"wasm-bulk", &bulk_manifest())
        .await
        .unwrap();
    leader
        .mutate(
            b"db",
            "app",
            "put",
            &put_args("a", "kept"),
            &MutationOptions::default(),
        )
        .await
        .unwrap();
    // 1,500 rows and their index entries: three pages.
    leader
        .mutate(
            b"db",
            "bulk",
            "fill",
            &obj(vec![("rows", Value::Number(1500.0))]),
            &MutationOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(module_keys(leader, "bulk").await.len(), 3000);
    let started = leader.remove_module(b"db", "bulk", true).await.unwrap();
    // Its deployment waits for the purge.
    let refused = leader
        .deploy(b"db", "bulk", [7u8; 32], b"wasm-bulk", &bulk_manifest())
        .await
        .unwrap_err();
    assert!(module_conflict(refused).contains("still being purged"));

    // One page, on every member alike.
    let page = leader.purge_page(b"db").await.unwrap().unwrap().unwrap();
    let Value::Object(page) = page else {
        panic!("a page answers an object");
    };
    assert_eq!(page["deleted"], Value::Number(1024.0));
    assert_eq!(page["done"], Value::Bool(false));
    let position = leader.status().last_applied.unwrap();
    assert!(position > started.position);
    let left = module_keys(leader, "bulk").await;
    assert_eq!(left.len(), 3000 - 1024);
    let row = purges_row(leader).await;
    assert!(row.is_some());
    for node in &nodes {
        wait_applied(node, position).await;
        assert_eq!(module_keys(node, "bulk").await, left, "node {}", node.id());
        assert_eq!(purges_row(node).await, row, "node {}", node.id());
    }

    // The leader goes; the next one's maintenance goes on from the
    // replicated cursor.
    registry.unregister(leader_id);
    let mut survivors = Vec::new();
    for node in nodes {
        if node.id() == leader_id {
            node.shutdown().await.unwrap();
        } else {
            survivors.push(node);
        }
    }
    eventually("a new leader", || {
        let elected = survivors
            .iter()
            .any(|node| node.status().leader == Some(node.id()));
        async move { elected }
    })
    .await;
    let leader = survivors
        .iter()
        .find(|node| node.status().leader == Some(node.id()))
        .unwrap();
    eventually("the purge to end", || async {
        leader.maintain_purges(Duration::from_millis(200)).await;
        purges_row(leader).await.is_none()
    })
    .await;
    let stats = leader.purge_stats();
    assert_eq!((stats.pages, stats.finished), (2, 1), "{stats:?}");
    assert_eq!(stats.deleted, 3000 - 1024);
    let position = leader.status().last_applied.unwrap();
    for node in &survivors {
        wait_applied(node, position).await;
        assert!(
            module_keys(node, "bulk").await.is_empty(),
            "node {}",
            node.id()
        );
        assert!(purges_row(node).await.is_none(), "node {}", node.id());
        let engine = node.engine().lock().await;
        let kept = engine
            .invoke_query(b"db", "app", "get", &get_args("a"), &CallOptions::default())
            .unwrap();
        assert_eq!(kept, obj(vec![("title", s("kept"))]));
    }
    // Deployed again, it starts empty.
    leader
        .deploy(b"db", "bulk", [7u8; 32], b"wasm-bulk", &bulk_manifest())
        .await
        .unwrap();
    let call = leader
        .query(
            b"db",
            "bulk",
            "get",
            &get_args("f1"),
            &CallOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(call, Value::Null);
    for node in survivors {
        node.shutdown().await.unwrap();
    }
}
