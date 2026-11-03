//! Backup tests: encrypted roundtrips, rotation, retention,
//! re-encryption and collection over a plain engine.

#[path = "common/mod.rs"]
mod common;

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::time::Duration;

use common::*;
use flower_backup::driver::Driver;
use flower_backup::manifest::BackupManifest;
use flower_backup::object::BackupKey;
use flower_backup::{BackupError, ContinuousSettings};
use flower_cluster::logstore::LogStore;
use flower_cluster::statemachine::StateMachine;
use flower_engine::engine::Engine;
use flower_engine::storage::GroupStore;
use flower_engine::storage::meta;
use futures::TryStreamExt;
use object_store::ObjectStoreExt;
use object_store::memory::InMemory;
use object_store::path::Path as ObjPath;

/// Open and initialize a single voting member with this suite's runtime.
async fn single_node(
    dir: &std::path::Path,
    registry: &flower_cluster::network::Registry,
) -> flower_cluster::node::Node {
    use flower_cluster::network::InProcessFactory;
    use flower_cluster::node::Node;
    let node = Node::open(
        1,
        &dir.join("n1"),
        Arc::new(openraft::Config::default()),
        InProcessFactory::new(registry.clone()),
        Arc::new(TestFactory),
    )
    .await
    .unwrap();
    registry.register(1, node.raft().clone());
    node.initialize(BTreeSet::from([1])).await.unwrap();
    node
}

#[tokio::test(flavor = "multi_thread")]
async fn backup_restore_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let engine = fixture(dir.path());
    let store = Arc::new(InMemory::new());
    let logs = LogStore::open(&dir.path().join("raft.redb")).unwrap();
    let driver = Driver::new(
        store.clone(),
        &dir.path().join("keystore.json"),
        kek(),
        Arc::new(tokio::sync::Mutex::new(engine)),
        logs,
        builder(),
    )
    .unwrap();

    let summary = driver.backup_now().await.unwrap();
    assert_eq!(summary.objects, 1);
    // A plain engine has no group position.
    assert_eq!(summary.covered_index, 0);
    assert!(summary.plaintext_bytes > 0);
    assert!(summary.ciphertext_bytes > 0);

    // Every stored object is ciphertext under an opaque name: no
    // collection names, titles or keys appear anywhere. Nothing but
    // views and the manifest is written: no pointer names the latest.
    let objects = stored_objects(&store).await;
    assert_eq!(objects.len(), 2);
    for (name, bytes) in &objects {
        assert!(
            name.starts_with("flower-backup/v1/obj/")
                || name.starts_with("flower-backup/v1/manifest/"),
            "unexpected object {name}"
        );
        let text = String::from_utf8_lossy(bytes);
        for marker in ["tasks", "first", "second", "title", "\"a\"", "\"b\""] {
            assert!(
                !text.contains(marker),
                "plaintext marker {marker} in {name}"
            );
        }
        assert!(!name.contains("tasks"), "business data in {name}");
    }

    // The manifest records fencing plus key versions.
    let key = BackupKey::open(&dir.path().join("keystore.json"), kek()).unwrap();
    let manifest_bytes = objects
        .iter()
        .find(|(name, _)| *name == summary.manifest)
        .unwrap()
        .1
        .clone();
    let manifest: BackupManifest =
        serde_json::from_slice(&key.open_sealed(&manifest_bytes).unwrap()).unwrap();
    assert_eq!(manifest.objects.len(), 1);
    assert_eq!(manifest.objects[0].db, b"db");
    assert_eq!(manifest.objects[0].revision, 2);
    assert_eq!(manifest.objects[0].key_version, 1);

    // Restore to a fresh file and read the data back.
    let restored = dir.path().join("restored.redb");
    let summary = driver.restore_latest(&restored).await.unwrap();
    assert_eq!(summary.objects, 1);
    let store = GroupStore::open(&restored).unwrap();
    let read = store.begin_read().unwrap();
    assert_eq!(read.meta_u64(b"db", meta::APPLIED_LOG).unwrap(), 0);
    assert_eq!(read.meta_u64(b"db", meta::REVISION).unwrap(), 2);
    drop(read);
    let engine = Engine::new(store);
    assert_eq!(
        StateMachine::replicated_databases(&engine).unwrap(),
        vec![b"db".to_vec()]
    );
    // Reload runtimes through a second restore handle is covered by
    // the PITR suite; here the stored rows prove the install.
}

#[tokio::test(flavor = "multi_thread")]
async fn rotation_reencrypt_retire() {
    let dir = tempfile::tempdir().unwrap();
    let engine = fixture(dir.path());
    let store = Arc::new(InMemory::new());
    let logs = LogStore::open(&dir.path().join("raft.redb")).unwrap();
    let keystore = dir.path().join("keystore.json");
    let driver = Driver::new(
        store.clone(),
        &keystore,
        kek(),
        Arc::new(tokio::sync::Mutex::new(engine)),
        logs,
        builder(),
    )
    .unwrap();

    let first = driver.backup_now().await.unwrap();
    let key = BackupKey::open(&keystore, kek()).unwrap();
    assert_eq!(key.rotate().unwrap(), 2);
    let second = driver.backup_now().await.unwrap();

    // Both backups restore: reads accept any retained version.
    driver
        .restore_backup(&dir.path().join("r1.redb"), &first.manifest)
        .await
        .unwrap();
    driver
        .restore_backup(&dir.path().join("r2.redb"), &second.manifest)
        .await
        .unwrap();
    let engine = Engine::new(GroupStore::open(dir.path().join("r1.redb")).unwrap());
    // Restored engines need runtimes for queries; the revision row
    // proves the install without executing guests.
    let read = engine.store().begin_read().unwrap();
    assert_eq!(read.meta_u64(b"db", meta::REVISION).unwrap(), 2);
    drop(read);

    // Retirement is gated: version 1 is still referenced.
    let offline = Driver::offline(store.clone(), &keystore, kek(), builder()).unwrap();
    assert!(offline.retire_versions().await.unwrap().is_empty());

    // Re-encryption moves everything forward, then version 1 retires.
    let summary = offline.reencrypt(100).await.unwrap();
    assert_eq!(summary.moved_objects, 1);
    assert_eq!(summary.manifests, 1);
    assert_eq!(offline.retire_versions().await.unwrap(), vec![1]);
    assert_eq!(key.versions().unwrap(), vec![2]);

    // The superseded manifest name is gone; the live set restores.
    assert!(matches!(
        offline
            .restore_backup(&dir.path().join("rx.redb"), &first.manifest)
            .await,
        Err(BackupError::NotFound(_))
    ));
    offline
        .restore_latest(&dir.path().join("r3.redb"))
        .await
        .unwrap();
    assert_eq!(offline.gc_orphans().await.unwrap().deleted, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn destruction_renders_unreadable() {
    let dir = tempfile::tempdir().unwrap();
    let engine = fixture(dir.path());
    let logs = LogStore::open(&dir.path().join("raft.redb")).unwrap();
    let keystore = dir.path().join("keystore.json");
    let driver = Driver::memory(
        &keystore,
        kek(),
        Arc::new(tokio::sync::Mutex::new(engine)),
        logs,
        builder(),
    )
    .unwrap();
    driver.backup_now().await.unwrap();
    let key = BackupKey::open(&keystore, kek()).unwrap();
    key.rotate().unwrap();
    // Bypass the retirement gate: models key destruction.
    assert!(key.retire(1).unwrap());
    assert!(matches!(
        driver
            .restore_latest(&dir.path().join("r.redb"))
            .await
            .unwrap_err(),
        BackupError::Crypto(_)
    ));
}

/// Retention keeps what its window reaches: the newest base at or before the
/// horizon and every base after it.
#[tokio::test(flavor = "multi_thread")]
async fn retention_keeps_what_its_window_reaches() {
    let dir = tempfile::tempdir().unwrap();
    let engine = fixture(dir.path());
    let store = Arc::new(InMemory::new());
    let logs = LogStore::open(&dir.path().join("raft.redb")).unwrap();
    let driver = Driver::new(
        store.clone(),
        &dir.path().join("keystore.json"),
        kek(),
        Arc::new(tokio::sync::Mutex::new(engine)),
        logs,
        builder(),
    )
    .unwrap();
    let mut names = Vec::new();
    for _ in 0..3 {
        names.push(driver.backup_now().await.unwrap().manifest);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let listed = driver.list_backups().await.unwrap();
    let created = |name: &str| {
        listed
            .iter()
            .find(|backup| backup.manifest == name)
            .unwrap()
            .created_ms
    };
    // A window reaching just past the second base keeps it, the newest
    // at or before the horizon, and the third.
    let window = Duration::from_millis(now_ms() - created(&names[1]));
    let summary = driver
        .enforce_retention(window, Duration::from_secs(3600))
        .await
        .unwrap();
    assert_eq!(summary.kept, 2);
    assert_eq!(summary.deleted_manifests, 1);
    assert_eq!(summary.deleted_objects, 1);
    assert!(matches!(
        driver
            .restore_backup(&dir.path().join("r.redb"), &names[0])
            .await
            .unwrap_err(),
        BackupError::NotFound(_)
    ));
    driver
        .restore_backup(&dir.path().join("r.redb"), &names[1])
        .await
        .unwrap();
    // A window of nothing keeps the newest alone.
    let summary = driver
        .enforce_retention(Duration::ZERO, Duration::from_secs(3600))
        .await
        .unwrap();
    assert_eq!((summary.kept, summary.deleted_manifests), (1, 1));
    driver
        .restore_backup(&dir.path().join("r.redb"), &names[2])
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn gc_collects_orphans() {
    let dir = tempfile::tempdir().unwrap();
    let engine = fixture(dir.path());
    let store = Arc::new(InMemory::new());
    let logs = LogStore::open(&dir.path().join("raft.redb")).unwrap();
    let driver = Driver::new(
        store.clone(),
        &dir.path().join("keystore.json"),
        kek(),
        Arc::new(tokio::sync::Mutex::new(engine)),
        logs,
        builder(),
    )
    .unwrap();
    driver.backup_now().await.unwrap();
    assert_eq!(driver.gc_orphans().await.unwrap().deleted, 0);
    store
        .put(
            &ObjPath::from("flower-backup/v1/obj/deadbeef"),
            vec![1u8, 2, 3].into(),
        )
        .await
        .unwrap();
    // A fresh orphan may still be an upload in progress: spared
    // until it outlives the grace period.
    assert_eq!(driver.gc_orphans().await.unwrap().deleted, 0);
    assert_eq!(
        driver
            .gc_orphans_older_than(Duration::ZERO)
            .await
            .unwrap()
            .deleted,
        1
    );
    driver
        .restore_latest(&dir.path().join("r.redb"))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn local_backend_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let engine = fixture(dir.path());
    let logs = LogStore::open(&dir.path().join("raft.redb")).unwrap();
    let driver = Driver::local(
        &dir.path().join("objects"),
        &dir.path().join("keystore.json"),
        kek(),
        Arc::new(tokio::sync::Mutex::new(engine)),
        logs,
        builder(),
    )
    .unwrap();
    let summary = driver.backup_now().await.unwrap();
    assert_eq!(summary.objects, 1);
    driver
        .restore_latest(&dir.path().join("r.redb"))
        .await
        .unwrap();
    let engine = Engine::new(GroupStore::open(dir.path().join("r.redb")).unwrap());
    let read = engine.store().begin_read().unwrap();
    assert_eq!(read.meta_u64(b"db", meta::REVISION).unwrap(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn offline_driver_refuses_online_ops() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(InMemory::new());
    let driver =
        Driver::offline(store, &dir.path().join("keystore.json"), kek(), builder()).unwrap();
    assert!(matches!(
        driver.backup_now().await.unwrap_err(),
        BackupError::Refused(_)
    ));
    assert!(matches!(
        driver.archive_now().await.unwrap_err(),
        BackupError::Refused(_)
    ));
}

#[cfg(feature = "s3")]
#[tokio::test(flavor = "multi_thread")]
async fn s3_builders_construct() {
    assert!(Driver::s3("bucket", "us-east-1", "key", "secret").is_ok());
    assert!(
        Driver::s3_with_endpoint(
            "bucket",
            "us-east-1",
            "key",
            "secret",
            "http://localhost:9000",
            true
        )
        .is_ok()
    );
}

/// An in-memory store with injectable faults: one-shot put failures,
/// one put paused until the test resumes it, refused deletes, and
/// multipart uploads that stall after their first part.
#[derive(Debug, Default)]
struct Hooked {
    inner: InMemory,
    /// Name prefixes whose next put fails, one shot each.
    fail_puts: std::sync::Mutex<Vec<String>>,
    /// A name prefix whose next put waits for `resume`.
    pause_put: std::sync::Mutex<Option<String>>,
    /// Signalled when the paused put is reached.
    paused: tokio::sync::Notify,
    /// Lets the paused put proceed.
    resume: tokio::sync::Notify,
    /// Whether every delete fails, as without delete permission.
    fail_deletes: AtomicBool,
    /// Whether multipart uploads never finish a part after their first.
    stall_parts: AtomicBool,
    /// Multipart uploads aborted.
    aborted: Arc<AtomicUsize>,
}

/// A multipart upload of [`Hooked`]: stalls when told, counts aborts.
#[derive(Debug)]
struct HookedUpload {
    inner: Box<dyn object_store::MultipartUpload>,
    stall: bool,
    parts: usize,
    aborted: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl object_store::MultipartUpload for HookedUpload {
    fn put_part(&mut self, data: object_store::PutPayload) -> object_store::UploadPart {
        self.parts += 1;
        if self.stall && self.parts > 1 {
            return Box::pin(futures::future::pending());
        }
        self.inner.put_part(data)
    }

    async fn complete(&mut self) -> object_store::Result<object_store::PutResult> {
        self.inner.complete().await
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        self.aborted.fetch_add(1, SeqCst);
        self.inner.abort().await
    }
}

impl Hooked {
    fn fail_next_put(&self, prefix: &str) {
        self.fail_puts.lock().unwrap().push(prefix.to_string());
    }

    fn pause_next_put(&self, prefix: &str) {
        *self.pause_put.lock().unwrap() = Some(prefix.to_string());
    }
}

impl std::fmt::Display for Hooked {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Hooked({})", self.inner)
    }
}

#[async_trait::async_trait]
impl object_store::ObjectStore for Hooked {
    async fn put_opts(
        &self,
        location: &ObjPath,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        let name = location.to_string();
        let fail = {
            let mut fails = self.fail_puts.lock().unwrap();
            match fails
                .iter()
                .position(|prefix| name.starts_with(prefix.as_str()))
            {
                Some(index) => {
                    fails.remove(index);
                    true
                }
                None => false,
            }
        };
        if fail {
            return Err(object_store::Error::Generic {
                store: "hooked",
                source: format!("injected failure writing {name}").into(),
            });
        }
        let pause = {
            let mut pause = self.pause_put.lock().unwrap();
            let hit = pause
                .as_deref()
                .is_some_and(|prefix| name.starts_with(prefix));
            if hit {
                *pause = None;
            }
            hit
        };
        if pause {
            self.paused.notify_one();
            self.resume.notified().await;
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjPath,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        let inner = self.inner.put_multipart_opts(location, opts).await?;
        Ok(Box::new(HookedUpload {
            inner,
            stall: self.stall_parts.load(SeqCst),
            parts: 0,
            aborted: self.aborted.clone(),
        }))
    }

    async fn get_opts(
        &self,
        location: &ObjPath,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<'static, object_store::Result<ObjPath>>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<ObjPath>> {
        use futures::StreamExt;
        if self.fail_deletes.load(SeqCst) {
            return locations
                .map(|location| {
                    let location = location?;
                    Err(object_store::Error::Generic {
                        store: "hooked",
                        source: format!("injected failure deleting {location}").into(),
                    })
                })
                .boxed();
        }
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&ObjPath>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjPath>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &ObjPath,
        to: &ObjPath,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn reencrypt_failure_keeps_latest_restorable() {
    // Fail the superseding manifest's upload, then (separately) a
    // view's copy forward: neither may strand the latest backup.
    for failing in ["flower-backup/v1/manifest/", "flower-backup/v1/obj/"] {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Hooked::default());
        let keystore = dir.path().join("keystore.json");
        let driver = Driver::new(
            store.clone(),
            &keystore,
            kek(),
            Arc::new(tokio::sync::Mutex::new(fixture(dir.path()))),
            LogStore::open(&dir.path().join("raft.redb")).unwrap(),
            builder(),
        )
        .unwrap();
        driver.backup_now().await.unwrap();
        let key = BackupKey::open(&keystore, kek()).unwrap();
        key.rotate().unwrap();
        let offline = Driver::offline(store.clone(), &keystore, kek(), builder()).unwrap();

        store.fail_next_put(failing);
        assert!(offline.reencrypt(100).await.is_err(), "{failing}");
        offline
            .restore_latest(&restore_target(dir.path(), "r1"))
            .await
            .unwrap_or_else(|error| panic!("after failing {failing}: {error}"));

        // A rerun plus collection converges on one live manifest under
        // the new version, and the old version retires.
        offline.reencrypt(100).await.unwrap();
        offline.gc_orphans_older_than(Duration::ZERO).await.unwrap();
        offline
            .restore_latest(&restore_target(dir.path(), "r2"))
            .await
            .unwrap();
        assert_eq!(
            offline.retire_versions().await.unwrap(),
            vec![1],
            "{failing}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn gc_spares_views_of_a_backup_in_progress() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Hooked::default());
    let keystore = dir.path().join("keystore.json");
    let driver = Arc::new(
        Driver::new(
            store.clone(),
            &keystore,
            kek(),
            Arc::new(tokio::sync::Mutex::new(fixture(dir.path()))),
            LogStore::open(&dir.path().join("raft.redb")).unwrap(),
            builder(),
        )
        .unwrap(),
    );
    let collector = Driver::offline(store.clone(), &keystore, kek(), builder()).unwrap();
    driver.backup_now().await.unwrap();

    // The next backup stops after uploading its views, before its
    // manifest exists; collection runs in that window.
    store.pause_next_put("flower-backup/v1/manifest/");
    let running = tokio::spawn({
        let driver = driver.clone();
        async move { driver.backup_now().await }
    });
    tokio::time::timeout(Duration::from_secs(10), store.paused.notified())
        .await
        .unwrap();
    assert_eq!(collector.gc_orphans().await.unwrap().deleted, 0);
    store.resume.notify_one();
    let summary = running.await.unwrap().unwrap();

    let restored = collector
        .restore_latest(&restore_target(dir.path(), "r"))
        .await
        .unwrap();
    assert_eq!(restored.manifest, summary.manifest);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_continuous_stops_when_shutdown_sender_drops() {
    let dir = tempfile::tempdir().unwrap();
    let driver = Arc::new(
        Driver::new(
            Arc::new(InMemory::new()),
            &dir.path().join("keystore.json"),
            kek(),
            Arc::new(tokio::sync::Mutex::new(fixture(dir.path()))),
            LogStore::open(&dir.path().join("raft.redb")).unwrap(),
            builder(),
        )
        .unwrap(),
    );
    let (sender, receiver) = tokio::sync::watch::channel(false);
    let settings = ContinuousSettings {
        archive_interval: Duration::from_secs(3600),
        ..ContinuousSettings::default()
    };
    let running = tokio::spawn({
        let driver = driver.clone();
        async move { driver.run_continuous(&settings, receiver).await }
    });
    // Nothing can ever signal shutdown again: the loop must end
    // instead of spinning through backups without sleeping.
    drop(sender);
    tokio::time::timeout(Duration::from_secs(20), running)
        .await
        .expect("run_continuous kept looping after its shutdown sender dropped")
        .unwrap()
        .unwrap();
}

struct TestFactory;

impl flower_cluster::statemachine::RuntimeFactory for TestFactory {
    fn build(
        &self,
        _db: &[u8],
        _module: &str,
        _wasm: &[u8],
        _manifest: &flower_sdk::Value,
    ) -> Result<Box<dyn flower_engine::runtime::Runtime>, String> {
        Ok(runtime())
    }
}

/// One node's Raft log plus copies of its group file taken as the
/// log grew: each copy is a state machine that has applied only a
/// prefix of the final log, like a member whose log tail is not yet
/// known committed.
struct History {
    dir: tempfile::TempDir,
    /// Group file copy and its applied index after each phase.
    phases: Vec<(PathBuf, u64)>,
    /// The group incarnation phase zero established.
    incarnation: u64,
}

impl History {
    /// Run one node through phases of puts, restarting between
    /// phases; phase zero also establishes the group incarnation, as
    /// maintenance does after bootstrap, and deploys the app.
    async fn record(phases: &[&[(&str, &str)]]) -> History {
        use flower_cluster::network::{InProcessFactory, Registry};
        use flower_cluster::node::Node;
        let dir = tempfile::tempdir().unwrap();
        let node_dir = dir.path().join("n1");
        let mut out = Vec::new();
        let mut incarnation = 0;
        for (phase, puts) in phases.iter().enumerate() {
            let registry = Registry::new();
            let node = Node::open(
                1,
                &node_dir,
                Arc::new(openraft::Config::default()),
                InProcessFactory::new(registry.clone()),
                Arc::new(TestFactory),
            )
            .await
            .unwrap();
            registry.register(1, node.raft().clone());
            if phase == 0 {
                node.initialize(BTreeSet::from([1])).await.unwrap();
            }
            let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
            while node.status().leader != Some(1) {
                assert!(tokio::time::Instant::now() < deadline, "no leader elected");
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            if phase == 0 {
                incarnation = node.establish_incarnation().await.unwrap();
                assert_ne!(incarnation, 0);
                node.deploy(b"db", "app", [9u8; 32], b"wasm", &manifest())
                    .await
                    .unwrap();
            }
            for (key, title) in puts.iter() {
                node.mutate(
                    b"db",
                    "app",
                    "put",
                    &obj(vec![("key", s(&k(key))), ("title", s(title))]),
                    &flower_engine::engine::MutationOptions::default(),
                )
                .await
                .unwrap();
            }
            let applied = StateMachine::group_applied(&*node.engine().lock().await)
                .unwrap()
                .unwrap()
                .index;
            node.shutdown().await.unwrap();
            drop(registry);
            let copy = dir.path().join(format!("group-{phase}.redb"));
            std::fs::copy(node_dir.join("group.redb"), &copy).unwrap();
            out.push((copy, applied));
        }
        History {
            dir,
            phases: out,
            incarnation,
        }
    }

    fn logs(&self) -> LogStore {
        LogStore::open(&self.dir.path().join("n1").join("raft.redb")).unwrap()
    }

    fn applied(&self, phase: usize) -> u64 {
        self.phases[phase].1
    }

    /// A driver over the group file as of `phase` and the full log.
    fn driver(&self, phase: usize, store: &Arc<InMemory>, logs: &LogStore) -> Driver {
        self.driver_over(phase, store.clone(), logs)
    }

    /// [`History::driver`] over any store.
    fn driver_over(
        &self,
        phase: usize,
        store: Arc<dyn object_store::ObjectStore>,
        logs: &LogStore,
    ) -> Driver {
        let engine = Engine::new(GroupStore::open(&self.phases[phase].0).unwrap());
        Driver::new(
            store,
            &self.keystore(),
            kek(),
            Arc::new(tokio::sync::Mutex::new(engine)),
            logs.clone(),
            builder(),
        )
        .unwrap()
    }

    fn keystore(&self) -> PathBuf {
        self.dir.path().join("keystore.json")
    }

    /// A fresh restore target in its own directory.
    fn target(&self, name: &str) -> PathBuf {
        let dir = self.dir.path().join(name);
        std::fs::create_dir_all(&dir).unwrap();
        dir.join("group.redb")
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or(0)
}

/// Segment names in the store, sorted.
async fn segments(store: &InMemory) -> Vec<String> {
    let mut names: Vec<String> = stored_objects(store)
        .await
        .into_iter()
        .map(|(name, _)| name)
        .filter(|name| name.starts_with("flower-backup/v1/seg/"))
        .collect();
    names.sort();
    names
}

#[tokio::test(flavor = "multi_thread")]
async fn archive_stops_at_the_applied_index() {
    let history = History::record(&[&[("a", "first")], &[("b", "second"), ("c", "third")]]).await;
    let logs = history.logs();
    let logged = logs.read_entries(1, u64::MAX).unwrap();
    assert!(logged.last().unwrap().log_id.index > history.applied(0));
    let store = Arc::new(InMemory::new());

    // A state machine trailing its log archives only what it applied.
    let lagging = history.driver(0, &store, &logs);
    let base = lagging.backup_now().await.unwrap();
    assert_eq!(base.covered_index, history.applied(0));
    let first = lagging.archive_now().await.unwrap();
    assert_eq!((first.from, first.to), (0, history.applied(0)));
    for name in segments(&store).await {
        let range = flower_backup::object::segment_range_from_name(&name).unwrap();
        assert_eq!(range.incarnation, history.incarnation);
        assert!(
            range.last <= history.applied(0),
            "{name} archived past the applied index"
        );
    }

    // Once applied, the rest archives from there.
    let caught_up = history.driver(1, &store, &logs);
    let second = caught_up.archive_now().await.unwrap();
    assert_eq!(
        (second.from, second.to),
        (history.applied(0), history.applied(1))
    );
    assert_eq!(second.entries, 2);

    let offline = Driver::offline(store.clone(), &history.keystore(), kek(), builder()).unwrap();
    let summary = offline
        .pitr(&history.target("restored"), now_ms())
        .await
        .unwrap();
    assert_eq!(summary.manifest, base.manifest);
    assert_eq!(summary.replayed, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn archive_reruns_are_idempotent_and_restore_chains_overlap() {
    let history =
        History::record(&[&[("a", "first")], &[("b", "second")], &[("c", "third")]]).await;
    let logs = history.logs();
    let store = Arc::new(InMemory::new());
    let archive_pos = ObjPath::from(flower_backup::object::archive_position_name(
        history.incarnation,
    ));
    history.driver(0, &store, &logs).backup_now().await.unwrap();

    // The pointer write is lost after the segments landed (a failed
    // put or a crash): the rerun redoes the range under the same
    // names instead of uploading duplicates.
    let lagging = history.driver(1, &store, &logs);
    lagging.archive_now().await.unwrap();
    let uploaded = segments(&store).await;
    store.delete(&archive_pos).await.unwrap();
    lagging.archive_now().await.unwrap();
    assert_eq!(segments(&store).await, uploaded);

    // Lost again while the log advanced: the next run covers a wider
    // range overlapping the first.
    store.delete(&archive_pos).await.unwrap();
    let caught_up = history.driver(2, &store, &logs);
    let summary = caught_up.archive_now().await.unwrap();
    assert_eq!((summary.from, summary.to), (0, history.applied(2)));
    assert_eq!(segments(&store).await.len(), 2);

    // Restore chains through the overlap and replays each entry once.
    let offline = Driver::offline(store.clone(), &history.keystore(), kek(), builder()).unwrap();
    let recovered = offline.pitr(&history.target("r1"), now_ms()).await.unwrap();
    assert_eq!(recovered.replayed, 2);

    // Collection drops the segment the wider one contains.
    assert_eq!(offline.gc_orphans().await.unwrap().deleted, 1);
    assert_eq!(
        segments(&store).await,
        vec![flower_backup::object::segment_name(
            history.incarnation,
            1,
            history.applied(2)
        )]
    );
    let recovered = offline.pitr(&history.target("r2"), now_ms()).await.unwrap();
    assert_eq!(recovered.replayed, 2);
    let engine = Engine::new(GroupStore::open(history.target("r2")).unwrap());
    let read = engine.store().begin_read().unwrap();
    assert_eq!(read.meta_u64(b"db", meta::REVISION).unwrap(), 3);
}

/// A restore target in its own directory: restore removes a sibling
/// `raft.redb`, which must not be the live driver's log.
fn restore_target(dir: &std::path::Path, name: &str) -> PathBuf {
    let dir = dir.join(name);
    std::fs::create_dir_all(&dir).unwrap();
    dir.join("group.redb")
}

/// Every stored manifest by name, opened.
async fn stored_manifests(
    store: &dyn object_store::ObjectStore,
    key: &BackupKey,
) -> Vec<(String, BackupManifest)> {
    stored_objects(store)
        .await
        .into_iter()
        .filter(|(name, _)| name.starts_with("flower-backup/v1/manifest/"))
        .map(|(name, bytes)| {
            let manifest = serde_json::from_slice(&key.open_sealed(&bytes).unwrap()).unwrap();
            (name, manifest)
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn latest_stays_newest_when_reencryption_races_a_backup() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Hooked::default());
    let keystore = dir.path().join("keystore.json");
    let driver = Driver::new(
        store.clone(),
        &keystore,
        kek(),
        Arc::new(tokio::sync::Mutex::new(fixture(dir.path()))),
        LogStore::open(&dir.path().join("raft.redb")).unwrap(),
        builder(),
    )
    .unwrap();
    let first = driver.backup_now().await.unwrap();
    let key = BackupKey::open(&keystore, kek()).unwrap();
    let created_ms = stored_manifests(&*store, &key)
        .await
        .into_iter()
        .find(|(name, _)| *name == first.manifest)
        .unwrap()
        .1
        .created_ms;
    key.rotate().unwrap();
    let offline = Arc::new(Driver::offline(store.clone(), &keystore, kek(), builder()).unwrap());

    // Re-encryption rewrites the first backup while a second lands. A
    // latest pointer let re-encryption read it before the backup and
    // write it back after, moving "latest" to the older backup: pause
    // re-encryption at that write, wherever one still exists.
    store.pause_next_put("flower-backup/v1/latest");
    let mut reencrypting = tokio::spawn({
        let offline = offline.clone();
        async move { offline.reencrypt(100).await }
    });
    let paused = tokio::select! {
        _ = store.paused.notified() => true,
        done = &mut reencrypting => {
            done.unwrap().unwrap();
            false
        }
    };
    // Creation times order backups; keep them distinct.
    tokio::time::sleep(Duration::from_millis(20)).await;
    let second = driver.backup_now().await.unwrap();
    if paused {
        store.resume.notify_one();
        reencrypting.await.unwrap().unwrap();
    }

    let restored = offline
        .restore_latest(&restore_target(dir.path(), "r"))
        .await
        .unwrap();
    assert_eq!(restored.manifest, second.manifest);
    // The rewritten first backup kept its ID and creation time, and so
    // its place behind the second.
    let rewritten: Vec<_> = stored_manifests(&*store, &key)
        .await
        .into_iter()
        .filter(|(_, manifest)| manifest.backup_id == first.backup_id)
        .collect();
    assert_eq!(rewritten.len(), 1);
    assert_ne!(rewritten[0].0, first.manifest);
    assert_eq!(rewritten[0].1.created_ms, created_ms);
}

#[tokio::test(flavor = "multi_thread")]
async fn archive_resumes_in_a_new_history_after_restore() {
    use flower_cluster::network::{InProcessFactory, Registry};
    use flower_cluster::node::Node;
    // The old history runs well past where the new one's log starts.
    let history =
        History::record(&[&[("a", "1"), ("b", "2"), ("c", "3"), ("d", "4"), ("e", "5")]]).await;
    let logs = history.logs();
    let store = Arc::new(InMemory::new());
    {
        let old = history.driver(0, &store, &logs);
        old.backup_now().await.unwrap();
        assert_eq!(old.archive_now().await.unwrap().to, history.applied(0));
    }
    let before = stored_objects(&*store).await;

    // Restore onto a new cluster: a new history whose log restarts.
    let offline = Driver::offline(store.clone(), &history.keystore(), kek(), builder()).unwrap();
    let target = history.target("restored");
    offline.restore_latest(&target).await.unwrap();
    let registry = Registry::new();
    let node = Node::open(
        1,
        target.parent().unwrap(),
        Arc::new(openraft::Config::default()),
        InProcessFactory::new(registry.clone()),
        Arc::new(TestFactory),
    )
    .await
    .unwrap();
    registry.register(1, node.raft().clone());
    node.initialize(BTreeSet::from([1])).await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while node.status().leader != Some(1) {
        assert!(tokio::time::Instant::now() < deadline, "no leader elected");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let driver = Driver::new(
        store.clone(),
        &history.keystore(),
        kek(),
        node.engine().clone(),
        node.logs().clone(),
        builder(),
    )
    .unwrap();

    // Nothing archives until the new history has its incarnation.
    let waiting = driver.archive_now().await.unwrap();
    assert_eq!((waiting.from, waiting.to, waiting.segments), (0, 0, 0));
    let incarnation = node.establish_incarnation().await.unwrap();
    assert_ne!(incarnation, history.incarnation);
    node.mutate(
        b"db",
        "app",
        "put",
        &obj(vec![("key", s(&k("f"))), ("title", s("6"))]),
        &flower_engine::engine::MutationOptions::default(),
    )
    .await
    .unwrap();
    let applied = StateMachine::group_applied(&*node.engine().lock().await)
        .unwrap()
        .unwrap()
        .index;
    assert!(applied < history.applied(0));

    // Archiving starts over in the new history's own namespace.
    let resumed = driver.archive_now().await.unwrap();
    assert_eq!((resumed.from, resumed.to), (0, applied));
    assert_eq!(resumed.entries, 2);
    let after = stored_objects(&*store).await;
    for (name, bytes) in &before {
        let kept = after.iter().find(|(other, _)| other == name);
        assert_eq!(kept.map(|(_, kept)| kept), Some(bytes), "{name} changed");
    }
    for name in segments(&store).await {
        if !before.iter().any(|(old, _)| *old == name) {
            let range = flower_backup::object::segment_range_from_name(&name).unwrap();
            assert_eq!(range.incarnation, incarnation);
        }
    }

    // Retention keeping only a base of the new history drops the old
    // history's segments, which no kept base replays, and the new
    // history's at or below the base's cut, but keeps the rest.
    let base = driver.backup_now().await.unwrap();
    node.mutate(
        b"db",
        "app",
        "put",
        &obj(vec![("key", s(&k("g"))), ("title", s("7"))]),
        &flower_engine::engine::MutationOptions::default(),
    )
    .await
    .unwrap();
    driver.archive_now().await.unwrap();
    let summary = driver
        .enforce_retention(Duration::ZERO, Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(summary.kept, 1);
    let left = segments(&store).await;
    assert!(!left.is_empty());
    for name in left {
        let range = flower_backup::object::segment_range_from_name(&name).unwrap();
        assert_eq!(
            range.incarnation, incarnation,
            "{name} outlived its history"
        );
        assert!(range.last > base.covered_index, "{name} is below the cut");
    }
    node.shutdown().await.unwrap();
}

/// The log ID stored at `index`.
fn log_id_at(logs: &LogStore, index: u64) -> openraft::LogId<u64> {
    logs.read_entries(index, index + 1).unwrap()[0].log_id
}

/// Indexes of the patch entries in the log, in order.
fn patch_indexes(logs: &LogStore) -> Vec<u64> {
    logs.read_entries(0, u64::MAX)
        .unwrap()
        .into_iter()
        .filter(|entry| {
            matches!(
                entry.payload,
                openraft::EntryPayload::Normal(
                    flower_cluster::types::ClusterWrite::Patch { .. }
                        | flower_cluster::types::ClusterWrite::Batch { .. }
                )
            )
        })
        .map(|entry| entry.log_id.index)
        .collect()
}

/// Seal and store a history's archive position, as a member that read
/// an older pointer would write it back.
async fn write_position(store: &InMemory, keystore: &std::path::Path, incarnation: u64, at: u64) {
    let key = BackupKey::open(keystore, kek()).unwrap();
    let (_, sealed) = key.seal_current(&at.to_be_bytes()).unwrap();
    store
        .put(
            &ObjPath::from(flower_backup::object::archive_position_name(incarnation)),
            sealed.into(),
        )
        .await
        .unwrap();
}

/// A history's archive position as stored.
async fn read_position(store: &InMemory, keystore: &std::path::Path, incarnation: u64) -> u64 {
    let key = BackupKey::open(keystore, kek()).unwrap();
    let sealed = store
        .get(&ObjPath::from(
            flower_backup::object::archive_position_name(incarnation),
        ))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    u64::from_be_bytes(key.open_sealed(&sealed).unwrap().try_into().unwrap())
}

/// Manifest names in the store.
async fn manifest_names(store: &InMemory) -> Vec<String> {
    stored_objects(store)
        .await
        .into_iter()
        .map(|(name, _)| name)
        .filter(|name| name.starts_with("flower-backup/v1/manifest/"))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn archive_reads_entries_the_hold_keeps_below_the_purge_point() {
    use openraft::storage::RaftLogStorage;
    let history = History::record(&[&[("a", "first")], &[("b", "second"), ("c", "third")]]).await;
    let mut logs = history.logs();
    let store = Arc::new(InMemory::new());
    let applied = history.applied(1);

    // Raft purges the whole applied log before anything is archived:
    // the hold keeps it on disk, below the purge point.
    logs.hold_purges(None).await.unwrap();
    logs.purge(log_id_at(&logs, applied)).await.unwrap();
    assert_eq!(logs.last_purged().unwrap().unwrap().index, applied);

    // With no base to skip to, archiving still reads every entry from
    // the log instead of refusing over the purge point.
    let driver = history.driver(1, &store, &logs);
    let summary = driver.archive_now().await.unwrap();
    assert_eq!((summary.from, summary.to), (0, applied));
    // The incarnation, the deployment and three puts.
    assert_eq!(summary.entries, 5);
    // What the archive position records leaves the log, nothing past it.
    assert!(!logs.read_retained(0, applied + 1).unwrap().is_empty());
    assert_eq!(driver.release_archived().await.unwrap(), applied);
    wait_until("the held entries go", || !logs.purge_stats().behind).await;
    assert!(
        logs.read_retained(0, u64::MAX)
            .unwrap()
            .iter()
            .all(|entry| entry.log_id.index > applied)
    );

    // A base from before replays the held entries it archived.
    let base = history.driver(0, &store, &logs).backup_now().await.unwrap();
    let offline = Driver::offline(store.clone(), &history.keystore(), kek(), builder()).unwrap();
    let restored = offline
        .restore_to_index(&history.target("r"), applied, None)
        .await
        .unwrap();
    assert_eq!(restored.manifest, base.manifest);
    assert_eq!((restored.replayed, restored.index), (2, applied));
}

#[tokio::test(flavor = "multi_thread")]
async fn archive_resumes_past_entries_another_member_archived() {
    use openraft::storage::RaftLogStorage;
    let history = History::record(&[&[("a", "first")], &[("b", "second")]]).await;
    let mut logs = history.logs();
    let store = Arc::new(InMemory::new());
    let driver = history.driver(1, &store, &logs);
    assert_eq!(driver.archive_now().await.unwrap().to, history.applied(1));
    let uploaded = segments(&store).await;

    // A deposed leader that read an older position writes it back after
    // this member moved it on, and this member's log let the archived
    // entries go.
    let keystore = history.keystore();
    write_position(&store, &keystore, history.incarnation, history.applied(0)).await;
    logs.purge(log_id_at(&logs, history.applied(1)))
        .await
        .unwrap();
    assert!(logs.first_index_from(0).unwrap().is_none());

    // No base covers the gone entries, but the archive does: archiving
    // moves the position past them again instead of refusing.
    let resumed = driver.archive_now().await.unwrap();
    assert_eq!(
        (resumed.from, resumed.to, resumed.segments),
        (history.applied(0), history.applied(1), 0)
    );
    assert_eq!(segments(&store).await, uploaded);
    assert_eq!(
        read_position(&store, &keystore, history.incarnation).await,
        history.applied(1)
    );

    // Gone entries that neither the archive nor a base covers refuse.
    for name in uploaded {
        store.delete(&ObjPath::from(name)).await.unwrap();
    }
    write_position(&store, &keystore, history.incarnation, 0).await;
    assert!(matches!(
        driver.archive_now().await,
        Err(BackupError::Refused(_))
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn restore_to_index_reaches_exactly_the_index_asked_for() {
    let history = History::record(&[&[("a", "1")], &[("b", "2"), ("c", "3")], &[("d", "4")]]).await;
    let logs = history.logs();
    let puts = patch_indexes(&logs);
    assert_eq!(puts.len(), 4);
    let store = Arc::new(InMemory::new());
    let first = history.driver(0, &store, &logs).backup_now().await.unwrap();
    let second = history.driver(1, &store, &logs).backup_now().await.unwrap();
    assert_eq!(second.covered_index, history.applied(1));
    history
        .driver(2, &store, &logs)
        .archive_now()
        .await
        .unwrap();
    let offline = Driver::offline(store.clone(), &history.keystore(), kek(), builder()).unwrap();
    let revision = |target: &PathBuf| {
        let engine = Engine::new(GroupStore::open(target).unwrap());
        let read = engine.store().begin_read().unwrap();
        read.meta_u64(b"db", meta::REVISION).unwrap()
    };

    // Between the cuts: the older base, replayed through B and no
    // further.
    let target = history.target("r1");
    let restored = offline
        .restore_to_index(&target, puts[1], None)
        .await
        .unwrap();
    assert_eq!(restored.manifest, first.manifest);
    assert_eq!(restored.incarnation, history.incarnation);
    assert_eq!((restored.replayed, restored.index), (1, puts[1]));
    assert!(restored.reached_ms > 0);
    assert_eq!(revision(&target), 2);

    // At a cut: that base alone. Past it: the newer base, replayed.
    let target = history.target("r2");
    let restored = offline
        .restore_to_index(&target, history.applied(1), Some(history.incarnation))
        .await
        .unwrap();
    assert_eq!(restored.manifest, second.manifest);
    assert_eq!(restored.replayed, 0);
    assert_eq!(revision(&target), 3);
    let target = history.target("r3");
    let restored = offline
        .restore_to_index(&target, history.applied(2), None)
        .await
        .unwrap();
    assert_eq!(restored.manifest, second.manifest);
    assert_eq!(restored.replayed, 1);
    assert_eq!(revision(&target), 4);

    // Before every base, past the archive, or in a history with no base:
    // refused, and the target is left alone.
    let target = history.target("r4");
    std::fs::write(&target, b"keep").unwrap();
    for (index, incarnation) in [
        (history.applied(0) - 1, None),
        (history.applied(2) + 1, None),
        (history.applied(2), Some(history.incarnation ^ 1)),
    ] {
        assert!(
            matches!(
                offline.restore_to_index(&target, index, incarnation).await,
                Err(BackupError::Refused(_))
            ),
            "{index} {incarnation:?}"
        );
        assert_eq!(std::fs::read(&target).unwrap(), b"keep");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn list_shows_how_far_each_backup_restores() {
    let history = History::record(&[&[("a", "1")], &[("b", "2")]]).await;
    let logs = history.logs();
    let store = Arc::new(InMemory::new());
    let first = history.driver(0, &store, &logs).backup_now().await.unwrap();
    let caught_up = history.driver(1, &store, &logs);
    caught_up.archive_now().await.unwrap();
    // Creation times order backups; keep them distinct.
    tokio::time::sleep(Duration::from_millis(5)).await;
    let second = caught_up.backup_now().await.unwrap();
    let offline = Driver::offline(store.clone(), &history.keystore(), kek(), builder()).unwrap();

    let listed = offline.list_backups().await.unwrap();
    assert_eq!(listed.len(), 2);
    let (older, newer) = (&listed[0], &listed[1]);
    assert_eq!(older.manifest, first.manifest);
    assert_eq!(newer.manifest, second.manifest);
    assert!(older.created_ms <= newer.created_ms);
    for listed in &listed {
        assert_eq!(listed.incarnation, history.incarnation);
        assert_eq!(listed.objects, 1);
        assert!(listed.ciphertext_bytes > 0);
    }
    // The older base reaches through the archive after it, up to B's
    // stamp; the newer one only its own cut.
    assert_eq!(older.covered_index, history.applied(0));
    assert_eq!(older.restorable_through, history.applied(1));
    let reached = older
        .restorable_through_ms
        .expect("archived writes after the cut");
    assert!(reached > 0 && reached <= newer.created_ms);
    assert_eq!(newer.covered_index, history.applied(1));
    assert_eq!(newer.restorable_through, history.applied(1));
    assert_eq!(newer.restorable_through_ms, None);

    // Restoring what the listing promises works.
    let restored = offline
        .restore_to_index(&history.target("r"), older.restorable_through, None)
        .await
        .unwrap();
    assert_eq!(restored.index, history.applied(1));
}

/// Wait up to 20 s for `done`.
async fn wait_until(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while !done() {
        assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn continuous_backup_holds_the_log_and_only_the_leader_backs_up() {
    use flower_cluster::network::Registry;

    use std::sync::atomic::Ordering;
    let dir = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let node = single_node(dir.path(), &registry).await;
    wait_until("a leader", || node.status().leader == Some(1)).await;
    let incarnation = node.establish_incarnation().await.unwrap();
    node.deploy(b"db", "app", [9u8; 32], b"wasm", &manifest())
        .await
        .unwrap();
    let put = |key: &'static str| {
        let node = &node;
        async move {
            node.mutate(
                b"db",
                "app",
                "put",
                &obj(vec![("key", s(&k(key))), ("title", s(key))]),
                &flower_engine::engine::MutationOptions::default(),
            )
            .await
            .unwrap()
            .position
        }
    };
    put("a").await;

    let store = Arc::new(InMemory::new());
    let keystore = dir.path().join("keystore.json");
    let leads = Arc::new(AtomicBool::new(false));
    let driver = Arc::new(
        Driver::new(
            store.clone(),
            &keystore,
            kek(),
            node.engine().clone(),
            node.logs().clone(),
            builder(),
        )
        .unwrap()
        .with_leadership({
            let leads = leads.clone();
            Arc::new(move || leads.load(Ordering::SeqCst))
        }),
    );
    assert_eq!(driver.continuous_status().role, "idle");
    let settings = ContinuousSettings {
        archive_interval: Duration::from_millis(20),
        base_interval: Duration::from_secs(3600),
        base_bytes: None,
        max_held: None,
        max_held_bytes: None,
        ..ContinuousSettings::default()
    };
    let (stop, stopped) = tokio::sync::watch::channel(false);
    let looping = driver.clone();
    let running = tokio::spawn(async move { looping.run_continuous(&settings, stopped).await });

    // A member that does not lead holds its log and backs nothing up.
    wait_until("the hold", || node.logs().purge_hold().enabled).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(stored_objects(&*store).await.is_empty());
    let status = driver.continuous_status();
    assert_eq!((status.role, status.bases_taken), ("follower", 0));
    assert!(status.ticks >= 1 && !status.failing, "{status:?}");

    // Leading, it takes one base, archives what it applied and lets its
    // log go of that, tick after tick.
    leads.store(true, Ordering::SeqCst);
    let b = put("b").await;
    wait_until("B archived", || {
        node.logs().purge_hold().released >= Some(b)
    })
    .await;
    assert!(read_position(&store, &keystore, incarnation).await >= b);
    let c = put("c").await;
    wait_until("C archived", || {
        node.logs().purge_hold().released >= Some(c)
    })
    .await;
    assert_eq!(manifest_names(&store).await.len(), 1);
    // Status reports the base and the archive, as `GET /v1/admin/backup`.
    let status = driver.continuous_status();
    assert_eq!((status.role, status.bases_taken), ("leader", 1));
    assert!(status.archived_through >= Some(c), "{status:?}");
    assert!(status.entries_archived >= 2 && status.segments_archived >= 1);
    assert!(status.newest_base.is_some() && status.errors == 0);

    stop.send(true).unwrap();
    running.await.unwrap().unwrap();
    assert_eq!(driver.continuous_status().role, "stopped");
    // The hold outlives the loop: a restart holds from open.
    assert!(node.logs().purge_hold().enabled);
    node.shutdown().await.unwrap();
}

/// A leader that stops ships what it applied since its last tick: a planned
/// stop leaves the archive complete instead of waiting for the next start to
/// send the tail.
#[tokio::test(flavor = "multi_thread")]
async fn a_stopping_leader_archives_what_it_applied_since_its_last_tick() {
    use flower_cluster::network::Registry;

    let dir = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let node = single_node(dir.path(), &registry).await;
    wait_until("a leader", || node.status().leader == Some(1)).await;
    let incarnation = node.establish_incarnation().await.unwrap();
    node.deploy(b"db", "app", [9u8; 32], b"wasm", &manifest())
        .await
        .unwrap();
    let put = |key: &'static str| {
        let node = &node;
        async move {
            node.mutate(
                b"db",
                "app",
                "put",
                &obj(vec![("key", s(&k(key))), ("title", s(key))]),
                &flower_engine::engine::MutationOptions::default(),
            )
            .await
            .unwrap()
            .position
        }
    };
    let a = put("a").await;
    let store = Arc::new(InMemory::new());
    let keystore = dir.path().join("keystore.json");
    let driver = Arc::new(
        Driver::new(
            store.clone(),
            &keystore,
            kek(),
            node.engine().clone(),
            node.logs().clone(),
            builder(),
        )
        .unwrap()
        .with_leadership(Arc::new(|| true)),
    );
    // One tick, then an hour's sleep the stop cuts short.
    let settings = ContinuousSettings {
        archive_interval: Duration::from_secs(3600),
        base_interval: Duration::from_secs(3600),
        base_bytes: None,
        max_held: None,
        max_held_bytes: None,
        ..ContinuousSettings::default()
    };
    let (stop, stopped) = tokio::sync::watch::channel(false);
    let looping = driver.clone();
    let running = tokio::spawn(async move { looping.run_continuous(&settings, stopped).await });
    wait_until("the first tick", || driver.continuous_status().ticks >= 1).await;
    assert!(read_position(&store, &keystore, incarnation).await >= a);
    let b = put("b").await;
    stop.send(true).unwrap();
    running.await.unwrap().unwrap();
    assert!(read_position(&store, &keystore, incarnation).await >= b);
    assert!(node.logs().purge_hold().released >= Some(b));
    node.shutdown().await.unwrap();
}

/// One multipart upload's part lengths, as they are put.
type Parts = Arc<std::sync::Mutex<Vec<u64>>>;

/// A store recording each request body it takes: whole puts, and each
/// multipart upload's parts.
#[derive(Debug)]
struct Recording {
    inner: Arc<dyn object_store::ObjectStore>,
    puts: std::sync::Mutex<Vec<(String, u64)>>,
    multiparts: std::sync::Mutex<Vec<(String, Parts)>>,
}

impl Recording {
    fn over(inner: Arc<dyn object_store::ObjectStore>) -> Arc<Self> {
        Arc::new(Recording {
            inner,
            puts: Default::default(),
            multiparts: Default::default(),
        })
    }

    /// Every request body's length since the last clear.
    fn bodies(&self) -> Vec<u64> {
        let mut bodies: Vec<u64> = self.puts.lock().unwrap().iter().map(|put| put.1).collect();
        for (_, parts) in self.multiparts.lock().unwrap().iter() {
            bodies.extend(parts.lock().unwrap().iter());
        }
        bodies
    }

    /// The multipart uploads of objects below `prefix`, with their
    /// parts' lengths.
    fn multipart(&self, prefix: &str) -> Vec<(String, Vec<u64>)> {
        self.multiparts
            .lock()
            .unwrap()
            .iter()
            .filter(|(name, _)| name.starts_with(prefix))
            .map(|(name, parts)| (name.clone(), parts.lock().unwrap().clone()))
            .collect()
    }

    fn clear(&self) {
        self.puts.lock().unwrap().clear();
        self.multiparts.lock().unwrap().clear();
    }
}

impl std::fmt::Display for Recording {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Recording({})", self.inner)
    }
}

/// A multipart upload recording its parts' lengths.
#[derive(Debug)]
struct RecordingUpload {
    inner: Box<dyn object_store::MultipartUpload>,
    parts: Parts,
}

#[async_trait::async_trait]
impl object_store::MultipartUpload for RecordingUpload {
    fn put_part(&mut self, data: object_store::PutPayload) -> object_store::UploadPart {
        self.parts
            .lock()
            .unwrap()
            .push(data.content_length() as u64);
        self.inner.put_part(data)
    }

    async fn complete(&mut self) -> object_store::Result<object_store::PutResult> {
        self.inner.complete().await
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        self.inner.abort().await
    }
}

#[async_trait::async_trait]
impl object_store::ObjectStore for Recording {
    async fn put_opts(
        &self,
        location: &ObjPath,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.puts
            .lock()
            .unwrap()
            .push((location.to_string(), payload.content_length() as u64));
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjPath,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        let parts = Parts::default();
        self.multiparts
            .lock()
            .unwrap()
            .push((location.to_string(), parts.clone()));
        let inner = self.inner.put_multipart_opts(location, opts).await?;
        Ok(Box::new(RecordingUpload { inner, parts }))
    }

    async fn get_opts(
        &self,
        location: &ObjPath,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<'static, object_store::Result<ObjPath>>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<ObjPath>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&ObjPath>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjPath>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &ObjPath,
        to: &ObjPath,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// The part size the multipart tests upload with: far below any object
/// they write but the manifest and the archive position.
const PART: usize = 1024;

/// A restored group file's revision.
fn revision(target: &PathBuf) -> u64 {
    let engine = Engine::new(GroupStore::open(target).unwrap());
    let read = engine.store().begin_read().unwrap();
    read.meta_u64(b"db", meta::REVISION).unwrap()
}

/// Back up and archive through `inner` in small parts, then restore,
/// restore by index, recover to a time and re-encrypt across them.
async fn objects_go_up_in_parts_and_come_back_across_them(
    inner: Arc<dyn object_store::ObjectStore>,
) {
    let titles: Vec<String> = (0..4).map(|seed| noise(3000, seed)).collect();
    let history = History::record(&[
        &[("a", titles[0].as_str())],
        &[("b", titles[1].as_str()), ("c", titles[2].as_str())],
        &[("d", titles[3].as_str())],
    ])
    .await;
    let logs = history.logs();
    let puts = patch_indexes(&logs);
    assert_eq!(puts.len(), 4);
    let store = Recording::over(inner);
    let base = history
        .driver_over(1, store.clone(), &logs)
        .with_part_bytes(PART)
        .backup_now()
        .await
        .unwrap();
    history
        .driver_over(2, store.clone(), &logs)
        .with_part_bytes(PART)
        .archive_now()
        .await
        .unwrap();

    // No request carried more than a part. The view and the segment went
    // up as multipart uploads of full parts but the last; the manifest
    // and the archive position, smaller than a part, in one request each.
    let all_within_a_part = |bodies: Vec<u64>| {
        assert!(bodies.iter().all(|len| *len <= PART as u64), "{bodies:?}");
    };
    all_within_a_part(store.bodies());
    let views = store.multipart("flower-backup/v1/obj/");
    assert_eq!(views.len(), 1);
    let (view, parts) = &views[0];
    assert!(parts.len() >= 3, "{parts:?}");
    assert!(
        parts[..parts.len() - 1]
            .iter()
            .all(|len| *len == PART as u64)
    );
    let stored = store
        .inner
        .head(&ObjPath::from(view.as_str()))
        .await
        .unwrap()
        .size;
    assert_eq!(stored, parts.iter().sum::<u64>());
    assert_eq!(base.ciphertext_bytes, stored);
    assert_eq!(store.multipart("flower-backup/v1/seg/").len(), 1);
    assert!(store.multipart("flower-backup/v1/manifest/").is_empty());
    assert!(store.multipart("flower-backup/v1/archive-pos/").is_empty());

    // Every way back reads each object across its parts and frames.
    let offline = Driver::offline(store.clone(), &history.keystore(), kek(), builder())
        .unwrap()
        .with_part_bytes(PART);
    let target = history.target("latest");
    offline.restore_latest(&target).await.unwrap();
    assert_eq!(revision(&target), 3);
    let target = history.target("index");
    let restored = offline
        .restore_to_index(&target, puts[3], None)
        .await
        .unwrap();
    assert_eq!((restored.replayed, revision(&target)), (1, 4));
    let target = history.target("time");
    let recovered = offline.pitr(&target, now_ms()).await.unwrap();
    assert_eq!((recovered.replayed, revision(&target)), (1, 4));
    // The newest state: the base and every archived entry after it.
    let target = history.target("newest");
    let newest = offline.restore_newest(&target).await.unwrap();
    assert_eq!((newest.replayed, revision(&target)), (1, 4));

    // Re-encryption reseals views and segments in parts too, and what it
    // wrote restores.
    let key = BackupKey::open(&history.keystore(), kek()).unwrap();
    assert_eq!(key.rotate().unwrap(), 2);
    store.clear();
    let moved = offline.reencrypt(100).await.unwrap();
    assert_eq!((moved.manifests, moved.segments), (1, 1));
    all_within_a_part(store.bodies());
    assert_eq!(store.multipart("flower-backup/v1/obj/").len(), 1);
    assert_eq!(store.multipart("flower-backup/v1/seg/").len(), 1);
    assert_eq!(offline.retire_versions().await.unwrap(), vec![1]);
    let target = history.target("reencrypted");
    offline
        .restore_to_index(&target, puts[3], None)
        .await
        .unwrap();
    assert_eq!(revision(&target), 4);

    // A view cut after its first frame, at a frame boundary, refuses
    // before the restore touches its target.
    let view = store
        .inner
        .list(Some(&ObjPath::from("flower-backup/v1/obj")))
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .remove(0)
        .location;
    let sealed = store.inner.get(&view).await.unwrap().bytes().await.unwrap();
    let frame = flower_crypto::aead::FRAME_OVERHEAD + PART;
    let cut = flower_crypto::aead::STREAM_HEADER_LEN + frame;
    assert!(sealed.len() > cut + frame);
    store
        .inner
        .put(&view, sealed.slice(..cut).into())
        .await
        .unwrap();
    let target = history.target("cut");
    std::fs::write(&target, b"keep").unwrap();
    assert!(matches!(
        offline.restore_latest(&target).await,
        Err(BackupError::Crypto(_))
    ));
    assert_eq!(std::fs::read(&target).unwrap(), b"keep");
}

#[tokio::test(flavor = "multi_thread")]
async fn in_memory_objects_go_up_in_parts_and_come_back_across_them() {
    objects_go_up_in_parts_and_come_back_across_them(Arc::new(InMemory::new())).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn local_objects_go_up_in_parts_and_come_back_across_them() {
    let dir = tempfile::tempdir().unwrap();
    objects_go_up_in_parts_and_come_back_across_them(Driver::local_store(dir.path()).unwrap())
        .await;
}

/// A leader that finds the archive since its history's newest base already
/// past both the base bytes and that base takes a base at its first tick by
/// bytes; one under either does not.
#[tokio::test(flavor = "multi_thread")]
async fn a_new_leader_takes_a_base_once_the_archive_outgrows_the_newest() {
    let titles: Vec<String> = (0..3).map(|seed| noise(3000, seed + 10)).collect();
    let history = History::record(&[
        &[("a", "1")],
        &[
            ("b", titles[0].as_str()),
            ("c", titles[1].as_str()),
            ("d", titles[2].as_str()),
        ],
    ])
    .await;
    let logs = history.logs();
    let store = Arc::new(InMemory::new());
    let base = history.driver(0, &store, &logs).backup_now().await.unwrap();
    let archive = history
        .driver(1, &store, &logs)
        .archive_now()
        .await
        .unwrap();
    assert!(
        archive.bytes > base.ciphertext_bytes,
        "{archive:?} {base:?}"
    );
    let driver = Arc::new(history.driver(1, &store, &logs));
    for (base_bytes, manifests) in [(None, 1), (Some(archive.bytes + 1), 1), (Some(1), 2)] {
        let settings = ContinuousSettings {
            archive_interval: Duration::from_secs(3600),
            base_interval: Duration::from_secs(3600),
            base_bytes,
            max_held: None,
            max_held_bytes: None,
            ..ContinuousSettings::default()
        };
        let ticks = driver.continuous_status().ticks;
        let (stop, stopped) = tokio::sync::watch::channel(false);
        let looping = driver.clone();
        let running = tokio::spawn(async move { looping.run_continuous(&settings, stopped).await });
        wait_until("a tick", || driver.continuous_status().ticks > ticks).await;
        stop.send(true).unwrap();
        running.await.unwrap().unwrap();
        assert_eq!(
            manifest_names(&store).await.len(),
            manifests,
            "{base_bytes:?}"
        );
    }
    assert_eq!(driver.continuous_status().errors, 0);
}

/// A leader takes a base once what it archived since the newest outgrows both
/// the base bytes and that base, and not before.
#[tokio::test(flavor = "multi_thread")]
async fn the_leader_takes_a_base_once_its_archive_outgrows_the_newest() {
    use flower_cluster::network::Registry;

    let dir = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let node = single_node(dir.path(), &registry).await;
    wait_until("a leader", || node.status().leader == Some(1)).await;
    node.establish_incarnation().await.unwrap();
    node.deploy(b"db", "app", [9u8; 32], b"wasm", &manifest())
        .await
        .unwrap();
    let put = |key: &'static str, title: String| {
        let node = &node;
        async move {
            node.mutate(
                b"db",
                "app",
                "put",
                &obj(vec![("key", s(&k(key))), ("title", s(&title))]),
                &flower_engine::engine::MutationOptions::default(),
            )
            .await
            .unwrap()
            .position
        }
    };
    // A large first record makes the first base larger than one small
    // write's segment.
    put("a", noise(6000, 1)).await;

    let store = Arc::new(InMemory::new());
    let driver = Arc::new(
        Driver::new(
            store.clone(),
            &dir.path().join("keystore.json"),
            kek(),
            node.engine().clone(),
            node.logs().clone(),
            builder(),
        )
        .unwrap()
        .with_leadership(Arc::new(|| true)),
    );
    let settings = ContinuousSettings {
        archive_interval: Duration::from_millis(20),
        base_interval: Duration::from_secs(3600),
        base_bytes: Some(1),
        max_held: None,
        max_held_bytes: None,
        ..ContinuousSettings::default()
    };
    let (stop, stopped) = tokio::sync::watch::channel(false);
    let looping = driver.clone();
    let running = tokio::spawn(async move { looping.run_continuous(&settings, stopped).await });
    wait_until("the first base", || {
        driver.continuous_status().bases_taken == 1
    })
    .await;
    let first = driver.continuous_status().newest_base.unwrap();

    // A small write archives fewer bytes than the base holds: no base.
    let b = put("b", "small".to_string()).await;
    wait_until("B archived", || {
        node.logs().purge_hold().released >= Some(b)
    })
    .await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(driver.continuous_status().bases_taken, 1);

    // One write outgrows the base and makes exactly one more due.
    // Several writes can straddle backup ticks and trigger a third
    // base before the assertion, depending on scheduling.
    let last = put("c", noise(24_000, 2)).await;
    wait_until("the second base", || {
        driver.continuous_status().bases_taken >= 2
    })
    .await;
    let second = driver.continuous_status().newest_base.unwrap();
    assert!(second.covered_index > first.covered_index);
    assert_eq!(second.covered_index, last);
    stop.send(true).unwrap();
    running.await.unwrap().unwrap();
    assert_eq!(manifest_names(&store).await.len(), 2);
    assert_eq!(driver.continuous_status().errors, 0);
    node.shutdown().await.unwrap();
}

/// A base is one cut although it uploads one database at a time while
/// applies go on: every view comes from the read pinned as it began.
#[tokio::test(flavor = "multi_thread")]
async fn a_base_is_one_cut_while_writes_land_during_its_upload() {
    use flower_engine::engine::MutationOptions;
    let put = |engine: &mut Engine, db: &[u8], key: &str| {
        engine
            .invoke_mutation(
                db,
                "app",
                "put",
                &obj(vec![("key", s(&k(key))), ("title", s(key))]),
                &MutationOptions::default(),
            )
            .unwrap();
    };
    let dir = tempfile::tempdir().unwrap();
    let mut engine = fixture(dir.path());
    engine
        .deploy(b"db2", "app", [7u8; 32], b"", &manifest(), runtime())
        .unwrap();
    StateMachine::register_database(engine.store(), b"db2").unwrap();
    put(&mut engine, b"db2", "x");
    let engine = Arc::new(tokio::sync::Mutex::new(engine));
    let store = Arc::new(Hooked::default());
    let driver = Arc::new(
        Driver::new(
            store.clone(),
            &dir.path().join("keystore.json"),
            kek(),
            engine.clone(),
            LogStore::open(&dir.path().join("raft.redb")).unwrap(),
            builder(),
        )
        .unwrap(),
    );

    // While the first view goes up, the engine is free and both
    // databases take a write.
    store.pause_next_put("flower-backup/v1/obj/");
    let running = tokio::spawn({
        let driver = driver.clone();
        async move { driver.backup_now().await }
    });
    tokio::time::timeout(Duration::from_secs(10), store.paused.notified())
        .await
        .unwrap();
    {
        let mut engine = engine.lock().await;
        put(&mut engine, b"db", "late");
        put(&mut engine, b"db2", "late");
    }
    store.resume.notify_one();
    assert_eq!(running.await.unwrap().unwrap().objects, 2);

    // Neither view holds the late writes.
    let target = restore_target(dir.path(), "r");
    driver.restore_latest(&target).await.unwrap();
    let restored = GroupStore::open(&target).unwrap();
    let read = restored.begin_read().unwrap();
    assert_eq!(read.meta_u64(b"db", meta::REVISION).unwrap(), 2);
    assert_eq!(read.meta_u64(b"db2", meta::REVISION).unwrap(), 1);
}

/// However often bases come, retention keeps every one its window
/// reaches, so a point long before the newest stays restorable: keeping a
/// count of bases instead, a leader taking one every few minutes under
/// write load kept a few hours of history.
#[tokio::test(flavor = "multi_thread")]
async fn retention_keeps_the_window_however_often_bases_come() {
    use flower_cluster::network::Registry;

    let dir = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let node = single_node(dir.path(), &registry).await;
    wait_until("a leader", || node.status().leader == Some(1)).await;
    node.establish_incarnation().await.unwrap();
    node.deploy(b"db", "app", [9u8; 32], b"wasm", &manifest())
        .await
        .unwrap();
    node.mutate(
        b"db",
        "app",
        "put",
        &obj(vec![("key", s(&k("a"))), ("title", s("a"))]),
        &flower_engine::engine::MutationOptions::default(),
    )
    .await
    .unwrap();
    let store = Arc::new(InMemory::new());
    let keystore = dir.path().join("keystore.json");
    let driver = Arc::new(
        Driver::new(
            store.clone(),
            &keystore,
            kek(),
            node.engine().clone(),
            node.logs().clone(),
            builder(),
        )
        .unwrap()
        .with_leadership(Arc::new(|| true)),
    );
    // A base every tick, under the default 30-day window.
    let settings = ContinuousSettings {
        archive_interval: Duration::from_millis(10),
        base_interval: Duration::from_millis(1),
        base_bytes: None,
        ..ContinuousSettings::default()
    };
    let (stop, stopped) = tokio::sync::watch::channel(false);
    let looping = driver.clone();
    let running = tokio::spawn(async move { looping.run_continuous(&settings, stopped).await });
    wait_until("the first base", || {
        driver.continuous_status().bases_taken >= 1
    })
    .await;
    let early = now_ms();
    wait_until("forty bases", || {
        driver.continuous_status().bases_taken >= 40
    })
    .await;
    stop.send(true).unwrap();
    running.await.unwrap().unwrap();
    let status = driver.continuous_status();
    assert_eq!(status.errors, 0, "{status:?}");
    assert_eq!(
        manifest_names(&store).await.len() as u64,
        status.bases_taken
    );
    let offline = Driver::offline(store.clone(), &keystore, kek(), builder()).unwrap();
    offline
        .pitr(&restore_target(dir.path(), "early"), early)
        .await
        .unwrap();
    node.shutdown().await.unwrap();
}

/// Bases older than the spacing thin out to one per spacing, and the newest
/// stays; what a dropped base covered restores from the one before it,
/// through the segments retention keeps.
#[tokio::test(flavor = "multi_thread")]
async fn retention_thins_old_bases_and_every_point_stays_restorable() {
    let history = History::record(&[
        &[("a", "1")],
        &[("b", "2")],
        &[("c", "3")],
        &[("d", "4")],
        &[("e", "5")],
    ])
    .await;
    let logs = history.logs();
    let puts = patch_indexes(&logs);
    assert_eq!(puts.len(), 5);
    let store = Arc::new(InMemory::new());
    let mut bases = Vec::new();
    for phase in 0..5 {
        bases.push(
            history
                .driver(phase, &store, &logs)
                .backup_now()
                .await
                .unwrap(),
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    history
        .driver(4, &store, &logs)
        .archive_now()
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(600)).await;

    // Bases 300 ms apart, thinned to one per 450 ms: the first, the
    // third and the newest stay, and every segment.
    let offline = Driver::offline(store.clone(), &history.keystore(), kek(), builder()).unwrap();
    let summary = offline
        .enforce_retention(
            Duration::from_secs(24 * 60 * 60),
            Duration::from_millis(450),
        )
        .await
        .unwrap();
    assert_eq!(
        (
            summary.kept,
            summary.deleted_manifests,
            summary.deleted_segments
        ),
        (3, 2, 0),
        "{summary:?}"
    );
    let kept: Vec<String> = offline
        .list_backups()
        .await
        .unwrap()
        .into_iter()
        .map(|backup| backup.backup_id)
        .collect();
    assert_eq!(kept, [0, 2, 4].map(|base| bases[base].backup_id.clone()));
    for (index, from) in [(puts[1], 0), (puts[3], 2)] {
        let target = history.target(&format!("at-{index}"));
        let restored = offline
            .restore_to_index(&target, index, None)
            .await
            .unwrap();
        assert_eq!(
            (restored.backup_id.as_str(), restored.index),
            (bases[from].backup_id.as_str(), index)
        );
    }
}

/// A base published before retention fails, as on a store refusing
/// deletes, is not taken again: retention runs apart from bases, so its
/// failure fails the tick and leaves the plan alone, where every tick
/// used to upload another full base without bound.
#[tokio::test(flavor = "multi_thread")]
async fn a_base_whose_retention_fails_is_not_taken_again_every_tick() {
    let titles: Vec<String> = (0..3).map(|seed| noise(3000, seed + 20)).collect();
    let history = History::record(&[
        &[("a", "1")],
        &[
            ("b", titles[0].as_str()),
            ("c", titles[1].as_str()),
            ("d", titles[2].as_str()),
        ],
    ])
    .await;
    let logs = history.logs();
    let store = Arc::new(Hooked::default());
    history
        .driver_over(0, store.clone(), &logs)
        .backup_now()
        .await
        .unwrap();
    store.fail_deletes.store(true, SeqCst);
    let driver = Arc::new(history.driver_over(1, store.clone(), &logs));
    // The archive since the first base outgrows it, so one base is due;
    // retention, keeping the newest alone, must then delete the first.
    let settings = ContinuousSettings {
        archive_interval: Duration::from_millis(20),
        base_interval: Duration::from_secs(3600),
        base_bytes: Some(1),
        retention: Duration::ZERO,
        ..ContinuousSettings::default()
    };
    let (stop, stopped) = tokio::sync::watch::channel(false);
    let looping = driver.clone();
    let running = tokio::spawn(async move { looping.run_continuous(&settings, stopped).await });
    wait_until("the base and its failed retention", || {
        let status = driver.continuous_status();
        status.bases_taken == 1 && status.errors == 1
    })
    .await;
    let ticks = driver.continuous_status().ticks;
    wait_until("ten more ticks", || {
        driver.continuous_status().ticks >= ticks + 10
    })
    .await;
    stop.send(true).unwrap();
    running.await.unwrap().unwrap();
    let status = driver.continuous_status();
    assert_eq!((status.bases_taken, status.errors), (1, 1), "{status:?}");
    let (error, _) = status.last_error.unwrap();
    assert!(error.contains("injected failure deleting"), "{error}");
    assert_eq!(manifest_names(&store.inner).await.len(), 2);
}

/// A base that fails waits the base retry before the leader tries another,
/// instead of trying every tick.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_base_waits_the_retry_before_the_next() {
    let history = History::record(&[&[("a", "1")]]).await;
    let logs = history.logs();
    let store = Arc::new(Hooked::default());
    let driver = Arc::new(history.driver_over(0, store.clone(), &logs));
    store.fail_next_put("flower-backup/v1/manifest/");
    let settings = ContinuousSettings {
        archive_interval: Duration::from_millis(20),
        base_interval: Duration::from_secs(3600),
        base_bytes: None,
        base_retry: Duration::from_secs(3),
        ..ContinuousSettings::default()
    };
    let (stop, stopped) = tokio::sync::watch::channel(false);
    let looping = driver.clone();
    let running = tokio::spawn(async move { looping.run_continuous(&settings, stopped).await });
    wait_until("the failed base", || driver.continuous_status().errors == 1).await;
    let ticks = driver.continuous_status().ticks;
    wait_until("five more ticks", || {
        driver.continuous_status().ticks >= ticks + 5
    })
    .await;
    assert_eq!(driver.continuous_status().bases_taken, 0);
    assert!(manifest_names(&store.inner).await.is_empty());
    // Once the retry passes, the base goes up.
    wait_until("the retried base", || {
        driver.continuous_status().bases_taken == 1
    })
    .await;
    stop.send(true).unwrap();
    running.await.unwrap().unwrap();
    assert_eq!(manifest_names(&store.inner).await.len(), 1);
    assert_eq!(driver.continuous_status().errors, 1);
}

/// A store holding backups in another format, such as one written
/// before objects became sealed streams, is refused at start, visibly,
/// and the log let go: continuous backup used to fail on it every tick,
/// archiving nothing, while every member's log held entries for it until
/// the hold's cap dropped them.
#[tokio::test(flavor = "multi_thread")]
async fn continuous_backup_refuses_a_store_in_another_format() {
    let history = History::record(&[&[("a", "1")]]).await;
    let logs = history.logs();
    let store = Arc::new(InMemory::new());
    // Passes empty, and holding this format.
    let offline = Driver::offline(store.clone(), &history.keystore(), kek(), builder()).unwrap();
    offline.check_format().await.unwrap();
    history.driver(0, &store, &logs).backup_now().await.unwrap();
    offline.check_format().await.unwrap();
    for (name, _) in stored_objects(&*store).await {
        store.delete(&ObjPath::from(name.as_str())).await.unwrap();
    }
    let mut envelope = b"FLB1".to_vec();
    envelope.extend([0u8; 44]);
    store
        .put(
            &ObjPath::from("flower-backup/v1/manifest/old"),
            envelope.into(),
        )
        .await
        .unwrap();
    assert!(matches!(
        offline.check_format().await,
        Err(BackupError::Foreign(_))
    ));

    // An earlier run held the log.
    logs.hold_purges(None).await.unwrap();
    let driver = history.driver(0, &store, &logs);
    let settings = ContinuousSettings {
        archive_interval: Duration::from_millis(20),
        ..ContinuousSettings::default()
    };
    let (_stop, stopped) = tokio::sync::watch::channel(false);
    let ended = tokio::time::timeout(
        Duration::from_secs(20),
        driver.run_continuous(&settings, stopped),
    )
    .await
    .expect("continuous backup kept failing on a store it cannot read");
    let Err(BackupError::Foreign(why)) = &ended else {
        panic!("{ended:?}");
    };
    assert!(why.contains("FLB1"), "{why}");
    assert!(!logs.purge_hold().enabled);
    let status = driver.continuous_status();
    assert!(status.role == "stopped" && status.failing, "{status:?}");
    assert!(status.last_error.unwrap().0.contains("another format"));
    assert_eq!(stored_objects(&*store).await.len(), 1);
}

/// A base dropped mid-upload, as when a shutdown stops waiting for it,
/// aborts its multipart upload in the background: S3 would otherwise
/// keep the parts of an upload nothing completes, which no listing shows.
#[tokio::test(flavor = "multi_thread")]
async fn an_upload_dropped_midway_aborts() {
    use flower_engine::engine::MutationOptions;
    let dir = tempfile::tempdir().unwrap();
    let mut engine = fixture(dir.path());
    engine
        .invoke_mutation(
            b"db",
            "app",
            "put",
            &obj(vec![("key", s(&k("big"))), ("title", s(&noise(20_000, 7)))]),
            &MutationOptions::default(),
        )
        .unwrap();
    let store = Arc::new(Hooked::default());
    store.stall_parts.store(true, SeqCst);
    let driver = Driver::new(
        store.clone(),
        &dir.path().join("keystore.json"),
        kek(),
        Arc::new(tokio::sync::Mutex::new(engine)),
        LogStore::open(&dir.path().join("raft.redb")).unwrap(),
        builder(),
    )
    .unwrap()
    .with_part_bytes(PART);
    let dropped = tokio::time::timeout(Duration::from_millis(500), driver.backup_now()).await;
    assert!(dropped.is_err(), "{dropped:?}");
    wait_until("the abort", || store.aborted.load(SeqCst) == 1).await;
    assert!(stored_objects(&store.inner).await.is_empty());
}

/// A store that stops answering never holds a stopping member up: a stop
/// cancels the tick it stalls (a base or an archive), and the final archive
/// gives up past its bound.
#[tokio::test(flavor = "multi_thread")]
async fn a_stalled_store_never_holds_a_stopping_leader_past_its_bound() {
    use flower_cluster::network::Registry;

    let dir = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let node = single_node(dir.path(), &registry).await;
    wait_until("a leader", || node.status().leader == Some(1)).await;
    let incarnation = node.establish_incarnation().await.unwrap();
    node.deploy(b"db", "app", [9u8; 32], b"wasm", &manifest())
        .await
        .unwrap();
    let put = |key: &'static str| {
        let node = &node;
        async move {
            node.mutate(
                b"db",
                "app",
                "put",
                &obj(vec![("key", s(&k(key))), ("title", s(key))]),
                &flower_engine::engine::MutationOptions::default(),
            )
            .await
            .unwrap()
            .position
        }
    };
    let a = put("a").await;
    let store = Arc::new(Hooked::default());
    let keystore = dir.path().join("keystore.json");
    let driver = Arc::new(
        Driver::new(
            store.clone(),
            &keystore,
            kek(),
            node.engine().clone(),
            node.logs().clone(),
            builder(),
        )
        .unwrap()
        .with_leadership(Arc::new(|| true)),
    );
    let settings = ContinuousSettings {
        archive_interval: Duration::from_secs(3600),
        base_interval: Duration::from_secs(3600),
        max_held: None,
        max_held_bytes: None,
        final_archive_within: Duration::from_millis(300),
        ..ContinuousSettings::default()
    };
    let run = |stopped: tokio::sync::watch::Receiver<bool>| {
        let (looping, settings) = (driver.clone(), settings.clone());
        tokio::spawn(async move { looping.run_continuous(&settings, stopped).await })
    };
    let stopping = |stop: tokio::sync::watch::Sender<bool>,
                    running: tokio::task::JoinHandle<Result<(), BackupError>>| async move {
        stop.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(5), running)
            .await
            .expect("the stalled store held the stop up")
            .unwrap()
            .unwrap();
    };

    // The final archive stalls on its segment: the stop gives it up.
    let (stop, stopped) = tokio::sync::watch::channel(false);
    let running = run(stopped);
    wait_until("the first tick", || driver.continuous_status().ticks >= 1).await;
    assert!(read_position(&store.inner, &keystore, incarnation).await >= a);
    let b = put("b").await;
    store.pause_next_put("flower-backup/v1/seg/");
    stopping(stop, running).await;
    store.paused.notified().await;
    assert!(read_position(&store.inner, &keystore, incarnation).await < b);

    // A tick stalls on its segment: the stop cancels it, and the final
    // archive ships what it held.
    let c = put("c").await;
    store.pause_next_put("flower-backup/v1/seg/");
    let (stop, stopped) = tokio::sync::watch::channel(false);
    let running = run(stopped);
    store.paused.notified().await;
    assert_eq!(driver.continuous_status().ticks, 1);
    stopping(stop, running).await;
    assert!(read_position(&store.inner, &keystore, incarnation).await >= c);
    node.shutdown().await.unwrap();
}
