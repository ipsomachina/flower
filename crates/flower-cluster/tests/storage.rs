//! Storage failure: a disk that fills stops writes for good in this
//! process (redb refuses a handle's transactions after an I/O error, and
//! OpenRaft stops the member), is reported through readiness, and a
//! restart once the disk has room recovers every acknowledged write.
//! Online compaction runs beside it and stops on the same failure.

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use flower_cluster::compaction;
use flower_cluster::health::{
    DEFAULT_RECOVERY_FREE_BYTES, GROUP, RAFT, STORAGE_FAILED, StorageHealth,
};
use flower_cluster::network::{InProcessFactory, Registry};
use flower_cluster::node::{FileOpener, Node, NodeError};
use flower_cluster::statemachine::RuntimeFactory;
use flower_cluster::types::ResponseOutcome;
use flower_engine::engine::{CallOptions, MutationOptions};
use flower_engine::runtime::{CallKind, ClosureRuntime, HostCall};
use flower_engine::storage::compact::{CompactionSettings, Compactor};
use flower_sdk::abi::{HostOp, decode_envelope};
use flower_sdk::{Failure, Value, fail};
use redb::backends::FileBackend;
use redb::{ReadableDatabase, TableDefinition};

/// A file whose writes fail with "storage full" while `full` is set,
/// like a full disk. Reads keep working.
#[derive(Debug)]
struct FillingDisk {
    file: FileBackend,
    full: Arc<AtomicBool>,
}

impl FillingDisk {
    fn check(&self) -> std::io::Result<()> {
        if self.full.load(Ordering::SeqCst) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::StorageFull,
                "No space left on device",
            ));
        }
        Ok(())
    }
}

impl redb::StorageBackend for FillingDisk {
    fn len(&self) -> std::io::Result<u64> {
        self.file.len()
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> std::io::Result<()> {
        self.file.read(offset, out)
    }

    fn set_len(&self, len: u64) -> std::io::Result<()> {
        self.check()?;
        self.file.set_len(len)
    }

    fn sync_data(&self) -> std::io::Result<()> {
        self.check()?;
        self.file.sync_data()
    }

    fn write(&self, offset: u64, data: &[u8]) -> std::io::Result<()> {
        self.check()?;
        self.file.write(offset, data)
    }

    fn close(&self) -> std::io::Result<()> {
        self.file.close()
    }
}

/// Open `path` over a [`FillingDisk`] sharing `full`.
fn filling(path: &Path, full: &Arc<AtomicBool>) -> Result<redb::Database, redb::DatabaseError> {
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
        .map_err(|error| redb::DatabaseError::Storage(redb::StorageError::Io(error)))?;
    redb::Builder::new().create_with_backend(FillingDisk {
        file: FileBackend::new(file)?,
        full: full.clone(),
    })
}

/// Open files whose name starts with `which` over a disk that fills
/// with `full`, the others as plain files.
fn filling_files(which: &'static str, full: Arc<AtomicBool>) -> FileOpener {
    Arc::new(move |path: &Path| {
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("");
        if name.starts_with(which) {
            filling(path, &full)
        } else {
            redb::Database::create(path)
        }
    })
}

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

fn manifest() -> Value {
    let string = obj(vec![("type", s("string"))]);
    let task = obj(vec![
        ("type", s("object")),
        (
            "fields",
            Value::Array(vec![obj(vec![
                ("name", s("title")),
                ("schema", string.clone()),
                ("optional", Value::Bool(false)),
            ])]),
        ),
    ]);
    let definition = |name: &str, kind: &str| {
        obj(vec![
            ("name", s(name)),
            ("kind", s(kind)),
            ("args", obj(vec![("type", s("json"))])),
        ])
    };
    obj(vec![
        (
            "collections",
            Value::Array(vec![obj(vec![
                ("name", s("tasks")),
                ("key", string),
                ("value", task),
                ("indexes", Value::Array(vec![])),
                ("references", Value::Array(vec![])),
            ])]),
        ),
        (
            "definitions",
            Value::Array(vec![
                definition("put", "mutation"),
                definition("get", "query"),
            ]),
        ),
        ("triggers", Value::Array(vec![])),
        ("http", Value::Array(vec![])),
    ])
}

/// One host call, decoding the envelope.
fn hc(host: &mut dyn HostCall, op: HostOp, args: Vec<Value>) -> Result<Value, Failure> {
    let input = Value::Array(args).encode_binary().unwrap();
    let output = host.op(op as u32 as u8, &input);
    decode_envelope(Value::decode_binary(&output).unwrap())
}

/// `put {key, title}` stores a task; `get {key}` reads it back.
struct App;

impl RuntimeFactory for App {
    fn build(
        &self,
        _db: &[u8],
        _module: &str,
        _wasm: &[u8],
        _manifest: &Value,
    ) -> Result<Box<dyn flower_engine::runtime::Runtime>, String> {
        Ok(Box::new(ClosureRuntime::new(
            |kind, _module: &str, name: &str, args: &Value, host: &mut dyn HostCall| {
                let Value::Object(map) = args else {
                    return Err(fail("TYPE_MISMATCH", "args take an object"));
                };
                let key = match map.get("key") {
                    Some(Value::String(key)) => key.clone(),
                    _ => return Err(fail("TYPE_MISMATCH", "args need a key")),
                };
                match (kind, name) {
                    (CallKind::Mutation, "put") => {
                        let title = map.get("title").cloned().unwrap_or(Value::Null);
                        let value = obj(vec![("title", title)]);
                        hc(host, HostOp::Set, vec![s(""), s("tasks"), s(&key), value])?;
                        Ok(s("put-ok"))
                    }
                    (CallKind::Query, "get") => {
                        hc(host, HostOp::Get, vec![s(""), s("tasks"), s(&key)])
                    }
                    _ => Err(fail("UNKNOWN_METHOD", "unknown definition")),
                }
            },
        )))
    }
}

async fn open(dir: &Path, files: FileOpener) -> (Registry, Node) {
    let registry = Registry::new();
    let node = Node::open_with(
        1,
        dir,
        Arc::new(openraft::Config::default()),
        InProcessFactory::new(registry.clone()),
        Arc::new(App),
        files,
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
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn ready(node: &Node) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let readiness = node.readiness().await;
        if readiness.ready {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "not ready: {readiness:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn put(node: &Node, key: &str) -> Result<(), NodeError> {
    let args = obj(vec![("key", s(&format!("\"{key}\""))), ("title", s(key))]);
    let response = tokio::time::timeout(
        Duration::from_secs(20),
        node.mutate(b"db", "app", "put", &args, &MutationOptions::default()),
    )
    .await
    .expect("a write never hangs on a failed disk")?;
    match response.outcome {
        ResponseOutcome::Value(_) => Ok(()),
        outcome => panic!("unexpected outcome {outcome:?}"),
    }
}

async fn get(node: &Node, key: &str) -> Result<Value, NodeError> {
    let args = obj(vec![("key", s(&format!("\"{key}\"")))]);
    node.query(b"db", "app", "get", &args, &CallOptions::default())
        .await
}

fn refused(result: Result<impl std::fmt::Debug, NodeError>) {
    match result {
        Err(NodeError::Failure { code, message }) => {
            assert_eq!(code, STORAGE_FAILED, "{message}");
            assert!(message.contains("storage failed"), "{message}");
        }
        other => panic!("expected a storage_failed refusal, got {other:?}"),
    }
}

/// Take the node down after a failure: the core already stopped, so
/// shutting it down again may report that.
async fn stop(node: Node, registry: Registry) {
    let _ = tokio::time::timeout(Duration::from_secs(10), node.shutdown()).await;
    drop(registry);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_full_raft_disk_stops_writes_reports_it_and_recovers_on_restart() {
    let dir = tempfile::tempdir().unwrap();
    let full = Arc::new(AtomicBool::new(false));
    let (registry, node) = open(dir.path(), filling_files("raft", full.clone())).await;
    node.initialize(BTreeSet::from([1])).await.unwrap();
    ready(&node).await;
    node.deploy(b"db", "app", [9u8; 32], b"wasm", &manifest())
        .await
        .unwrap();
    put(&node, "before").await.unwrap();
    assert!(node.storage_failure().is_none());

    // The disk fills: the write fails rather than hang or panic, and
    // the member stops.
    full.store(true, Ordering::SeqCst);
    assert!(put(&node, "during").await.is_err());
    eventually("the failure to be recorded", || {
        node.storage_failure().is_some()
    })
    .await;
    let failure = node.storage_failure().unwrap();
    assert_eq!(failure.component, RAFT, "{failure:?}");
    assert!(failure.error.contains("No space left"), "{failure:?}");
    let readiness = node.readiness().await;
    assert!(!readiness.ready);
    assert_eq!(readiness.storage.as_ref(), Some(&failure));
    assert!(
        readiness
            .reason
            .as_deref()
            .unwrap()
            .contains("storage failed"),
        "{readiness:?}"
    );

    // Every later write and linearizable read is refused up front, even
    // once the disk has room again: the handle stays failed.
    refused(put(&node, "after").await);
    refused(get(&node, "before").await);
    refused(
        node.deploy(b"db", "app", [8u8; 32], b"wasm", &manifest())
            .await,
    );
    full.store(false, Ordering::SeqCst);
    refused(put(&node, "after").await);
    assert!(!node.readiness().await.ready);
    // With room for the probe but less free than the threshold asks,
    // it waits: a restart would fill the disk again and loop.
    assert!(
        tokio::time::timeout(
            Duration::from_millis(2500),
            node.storage_recovered(u64::MAX)
        )
        .await
        .is_err(),
        "recovered short of the free-space threshold"
    );
    // With room again the directory passes the probe: the process may
    // exit for its supervisor to restart it.
    tokio::time::timeout(
        Duration::from_secs(5),
        node.storage_recovered(DEFAULT_RECOVERY_FREE_BYTES),
    )
    .await
    .expect("recovered once the disk accepts writes");

    // The restarted member has every acknowledged write, never the one
    // that failed, and takes new ones.
    stop(node, registry).await;
    let (registry, node) = open(dir.path(), filling_files("raft", full.clone())).await;
    ready(&node).await;
    assert!(node.storage_failure().is_none());
    assert_eq!(
        get(&node, "before").await.unwrap(),
        obj(vec![("title", s("before"))])
    );
    assert_eq!(get(&node, "during").await.unwrap(), Value::Null);
    put(&node, "after-restart").await.unwrap();
    assert_eq!(
        get(&node, "after-restart").await.unwrap(),
        obj(vec![("title", s("after-restart"))])
    );
    node.shutdown().await.unwrap();
    drop(registry);
}

/// With deferred applies (the default), a full group disk surfaces at
/// the next durable commit of the file rather than at the apply: the
/// deferred apply's pages wait in redb's write buffer. The write the
/// member acknowledged meanwhile is durable in the log, so it replays on
/// restart; the flush that hits the full disk is the member's storage
/// failure, and redb refuses the file's later transactions.
#[tokio::test(flavor = "multi_thread")]
async fn a_full_group_disk_surfaces_at_the_apply_flush_and_the_acknowledged_write_replays() {
    let dir = tempfile::tempdir().unwrap();
    let full = Arc::new(AtomicBool::new(false));
    let (registry, node) = open(dir.path(), filling_files("group", full.clone())).await;
    // No periodic flush during the test: the test flushes.
    node.apply_durability().set(Some(Duration::from_secs(3600)));
    node.initialize(BTreeSet::from([1])).await.unwrap();
    ready(&node).await;
    node.deploy(b"db", "app", [9u8; 32], b"wasm", &manifest())
        .await
        .unwrap();
    put(&node, "before").await.unwrap();
    node.flush_applies().await.unwrap();

    full.store(true, Ordering::SeqCst);
    put(&node, "during").await.unwrap();
    assert!(node.storage_failure().is_none());
    assert!(node.apply_durability().pending());
    // The flush that reaches the disk fails and stops the member.
    assert!(node.flush_applies().await.is_err());
    let failure = node.storage_failure().expect("the failure is recorded");
    assert_eq!(failure.component, GROUP, "{failure:?}");
    assert!(failure.error.contains("No space left"), "{failure:?}");
    let readiness = node.readiness().await;
    assert!(!readiness.ready, "{readiness:?}");
    refused(put(&node, "after").await);
    refused(get(&node, "before").await);

    // The acknowledged write was in the log: it applies on restart.
    full.store(false, Ordering::SeqCst);
    stop(node, registry).await;
    let (registry, node) = open(dir.path(), filling_files("group", full.clone())).await;
    ready(&node).await;
    assert_eq!(
        get(&node, "during").await.unwrap(),
        obj(vec![("title", s("during"))])
    );
    put(&node, "after-restart").await.unwrap();
    node.shutdown().await.unwrap();
    drop(registry);
}

/// With durable applies (`FLOWER_APPLY_FLUSH_MS=0`), the apply itself
/// meets the full disk.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_apply_stops_the_member_and_the_committed_entry_replays_on_restart() {
    let dir = tempfile::tempdir().unwrap();
    let full = Arc::new(AtomicBool::new(false));
    let (registry, node) = open(dir.path(), filling_files("group", full.clone())).await;
    node.apply_durability().set(None);
    node.initialize(BTreeSet::from([1])).await.unwrap();
    ready(&node).await;
    node.deploy(b"db", "app", [9u8; 32], b"wasm", &manifest())
        .await
        .unwrap();
    put(&node, "before").await.unwrap();

    // The log takes the entry, but the group file can't apply it: the
    // member stops instead of serving state that skipped an entry.
    full.store(true, Ordering::SeqCst);
    assert!(put(&node, "during").await.is_err());
    eventually("the failure to be recorded", || {
        node.storage_failure().is_some()
    })
    .await;
    let readiness = node.readiness().await;
    assert!(!readiness.ready, "{readiness:?}");
    assert!(readiness.storage.is_some());
    refused(put(&node, "after").await);
    refused(get(&node, "before").await);

    // On restart the committed entry applies from the log: the member
    // converges on the log rather than diverging from it.
    full.store(false, Ordering::SeqCst);
    stop(node, registry).await;
    let (registry, node) = open(dir.path(), filling_files("group", full.clone())).await;
    ready(&node).await;
    assert_eq!(
        get(&node, "before").await.unwrap(),
        obj(vec![("title", s("before"))])
    );
    assert_eq!(
        get(&node, "during").await.unwrap(),
        obj(vec![("title", s("during"))])
    );
    put(&node, "after-restart").await.unwrap();
    node.shutdown().await.unwrap();
    drop(registry);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_disk_failure_in_a_compaction_step_is_recorded_and_ends_compaction() {
    const ROWS: TableDefinition<u64, &[u8]> = TableDefinition::new("rows");
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("group.redb");
    let full = Arc::new(AtomicBool::new(false));
    let db = Arc::new(filling(&path, &full).unwrap());
    // A file mostly free inside, so the first check starts a run.
    let write = db.begin_write().unwrap();
    {
        let mut table = write.open_table(ROWS).unwrap();
        for key in 0..4_000u64 {
            table.insert(key, [1u8; 1000].as_slice()).unwrap();
        }
    }
    write.commit().unwrap();
    let write = db.begin_write().unwrap();
    {
        let mut table = write.open_table(ROWS).unwrap();
        for key in 0..3_800u64 {
            table.remove(key).unwrap();
        }
    }
    write.commit().unwrap();
    let compactor = Compactor::new(db.clone(), &path);
    let settings = CompactionSettings {
        min_file_bytes: 0,
        check: Duration::from_millis(1),
        ..CompactionSettings::default()
    };
    let health = Arc::new(StorageHealth::default());
    let (_stop, shutdown) = tokio::sync::watch::channel(false);

    full.store(true, Ordering::SeqCst);
    tokio::time::timeout(
        Duration::from_secs(10),
        compaction::run_file(compactor, settings, health.clone(), GROUP, shutdown),
    )
    .await
    .expect("compaction stops on a disk failure");
    let failure = health.failure().expect("the failure is recorded");
    assert_eq!(failure.component, GROUP);
    assert!(failure.error.contains("online compaction"), "{failure:?}");
    assert!(failure.error.contains("No space left"), "{failure:?}");

    // The rows are intact: a failed step changes nothing.
    drop(db);
    full.store(false, Ordering::SeqCst);
    let reopened = redb::Database::create(&path).unwrap();
    let read = reopened.begin_read().unwrap();
    let table = read.open_table(ROWS).unwrap();
    assert_eq!(redb::ReadableTableMetadata::len(&table).unwrap(), 200);
}

#[tokio::test(flavor = "multi_thread")]
async fn compaction_stops_on_shutdown_and_reports_both_files() {
    let dir = tempfile::tempdir().unwrap();
    let files: FileOpener = Arc::new(|path: &Path| redb::Database::create(path));
    let (registry, node) = open(dir.path(), files).await;
    node.initialize(BTreeSet::from([1])).await.unwrap();
    ready(&node).await;
    let node = Arc::new(node);
    let (stop, shutdown) = tokio::sync::watch::channel(false);
    let settings = CompactionSettings {
        check: Duration::from_millis(5),
        ..CompactionSettings::default()
    };
    let running = {
        let node = node.clone();
        tokio::spawn(async move { node.run_compaction(settings, shutdown).await })
    };
    eventually("both files measured", || {
        let stats = node.compaction_stats();
        stats.len() == 2 && stats.iter().all(|(_, stats)| stats.checks > 0)
    })
    .await;
    let files: Vec<&str> = node
        .compaction_stats()
        .iter()
        .map(|(file, _)| *file)
        .collect();
    assert_eq!(files, ["group", "raft_log"]);
    stop.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(10), running)
        .await
        .expect("compaction stops on shutdown")
        .unwrap();
    let node = Arc::try_unwrap(node).ok().expect("no step holds the node");
    node.shutdown().await.unwrap();
    drop(registry);
    // Both files closed: they reopen at once.
    let (registry, node) = open(
        dir.path(),
        Arc::new(|path: &Path| redb::Database::create(path)),
    )
    .await;
    ready(&node).await;
    node.shutdown().await.unwrap();
    drop(registry);
}
