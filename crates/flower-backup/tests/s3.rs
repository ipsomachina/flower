#![cfg(feature = "s3")]
//! S3 end-to-end: the backup driver over a real S3 API served
//! in-process by s3s, proving request signing, listing and the
//! kind-directory layout against true S3 semantics.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;

use common::*;
use flower_backup::BackupError;
use flower_backup::driver::Driver;
use flower_backup::manifest::BackupManifest;
use flower_backup::object::BackupKey;
use flower_cluster::logstore::LogStore;
use flower_cluster::statemachine::StateMachine;
use flower_engine::engine::Engine;
use flower_engine::storage::GroupStore;
use flower_engine::storage::meta;
use futures::TryStreamExt;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as ConnBuilder;
use s3s::auth::SimpleAuth;
use s3s::service::S3ServiceBuilder;
use s3s_fs::FileSystem;

const ACCESS_KEY: &str = "testadmin";
const SECRET_KEY: &str = "testadmin-secret";
const BUCKET: &str = "flowertest";
const REGION: &str = "us-east-1";

/// Serve s3s over a filesystem root with the bucket pre-created and
/// return the endpoint URL. The server task lives until the test's
/// runtime drops.
async fn serve_s3(root: &std::path::Path) -> String {
    std::fs::create_dir_all(root.join(BUCKET)).unwrap();
    let fs = FileSystem::new(root).unwrap();
    let mut builder = S3ServiceBuilder::new(fs);
    builder.set_auth(SimpleAuth::from_single(ACCESS_KEY, SECRET_KEY));
    let service = builder.build();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        let http = ConnBuilder::new(TokioExecutor::new());
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                break;
            };
            let conn = http
                .serve_connection(TokioIo::new(socket), service.clone())
                .into_owned();
            tokio::spawn(async move {
                let _ = conn.await;
            });
        }
    });
    endpoint
}

async fn s3_driver(dir: &std::path::Path, endpoint: &str) -> Driver {
    let engine = fixture(dir);
    let store =
        Driver::s3_with_endpoint(BUCKET, REGION, ACCESS_KEY, SECRET_KEY, endpoint, true).unwrap();
    let logs = LogStore::open(&dir.join("raft.redb")).unwrap();
    Driver::new(
        store,
        &dir.join("keystore.json"),
        kek(),
        Arc::new(tokio::sync::Mutex::new(engine)),
        logs,
        builder(),
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn s3_backup_restore_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let endpoint = serve_s3(&dir.path().join("s3root")).await;
    let driver = s3_driver(dir.path(), &endpoint).await;

    let summary = driver.backup_now().await.unwrap();
    assert_eq!(summary.objects, 1);
    assert_eq!(summary.covered_index, 0);

    // List through the S3 API and check the ciphertext discipline.
    let store =
        Driver::s3_with_endpoint(BUCKET, REGION, ACCESS_KEY, SECRET_KEY, &endpoint, true).unwrap();
    let objects = stored_objects(&*store).await;
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
    }

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
    assert_eq!(manifest.objects[0].revision, 2);

    let restored = dir.path().join("restored.redb");
    let summary = driver.restore_latest(&restored).await.unwrap();
    assert_eq!(summary.objects, 1);
    let store = GroupStore::open(&restored).unwrap();
    let read = store.begin_read().unwrap();
    assert_eq!(read.meta_u64(b"db", meta::REVISION).unwrap(), 2);
    drop(read);
    let engine = Engine::new(store);
    assert_eq!(
        StateMachine::replicated_databases(&engine).unwrap(),
        vec![b"db".to_vec()]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn s3_retention_uses_listing() {
    let dir = tempfile::tempdir().unwrap();
    let endpoint = serve_s3(&dir.path().join("s3root")).await;
    let driver = s3_driver(dir.path(), &endpoint).await;

    let mut names = Vec::new();
    for _ in 0..3 {
        names.push(driver.backup_now().await.unwrap().manifest);
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    // A window reaching just past the second base keeps it and the third.
    let second = driver
        .list_backups()
        .await
        .unwrap()
        .into_iter()
        .find(|backup| backup.manifest == names[1])
        .unwrap()
        .created_ms;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let summary = driver
        .enforce_retention(
            std::time::Duration::from_millis(now - second),
            std::time::Duration::from_secs(3600),
        )
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
        .restore_backup(&dir.path().join("r.redb"), &names[2])
        .await
        .unwrap();
    // Nothing orphaned: retention deleted exactly the pruned set.
    assert_eq!(driver.gc_orphans().await.unwrap().deleted, 0);
}

/// A base past S3's smallest part goes up as a multipart upload the S3
/// API accepts (s3s, like S3, refuses parts under 5 MiB but the last)
/// and comes back across its parts, re-encrypted too.
#[tokio::test(flavor = "multi_thread")]
async fn s3_large_objects_go_up_in_parts() {
    use flower_backup::object::MIN_PART_BYTES;
    use flower_engine::engine::MutationOptions;
    use object_store::ObjectStoreExt;
    use object_store::path::Path as ObjPath;
    let dir = tempfile::tempdir().unwrap();
    let endpoint = serve_s3(&dir.path().join("s3root")).await;
    // Twelve MiB of hex noise: over five once compressed, so two parts.
    let mut engine = fixture(dir.path());
    for index in 0..12u64 {
        engine
            .invoke_mutation(
                b"db",
                "app",
                "put",
                &obj(vec![
                    ("key", s(&k(&format!("big{index}")))),
                    ("title", s(&noise(1 << 20, index))),
                ]),
                &MutationOptions::default(),
            )
            .unwrap();
    }
    let store =
        Driver::s3_with_endpoint(BUCKET, REGION, ACCESS_KEY, SECRET_KEY, &endpoint, true).unwrap();
    let keystore = dir.path().join("keystore.json");
    let driver = Driver::new(
        store.clone(),
        &keystore,
        kek(),
        Arc::new(tokio::sync::Mutex::new(engine)),
        LogStore::open(&dir.path().join("raft.redb")).unwrap(),
        builder(),
    )
    .unwrap()
    .with_part_bytes(MIN_PART_BYTES);
    let summary = driver.backup_now().await.unwrap();
    assert!(summary.ciphertext_bytes > MIN_PART_BYTES as u64);

    // S3 names a multipart object's ETag by its part count.
    let parts = |e_tag: Option<String>| -> u64 {
        let e_tag = e_tag.expect("an ETag");
        let (_, count) = e_tag.trim_matches('"').rsplit_once('-').expect("multipart");
        count.parse().unwrap()
    };
    let view = stored_objects(&*store)
        .await
        .into_iter()
        .find(|(name, _)| name.starts_with("flower-backup/v1/obj/"))
        .unwrap()
        .0;
    let head = store.head(&ObjPath::from(view.as_str())).await.unwrap();
    assert_eq!(head.size, summary.ciphertext_bytes);
    assert_eq!(parts(head.e_tag), 2);

    let revision = |target: &std::path::Path| {
        let store = GroupStore::open(target).unwrap();
        let read = store.begin_read().unwrap();
        read.meta_u64(b"db", meta::REVISION).unwrap()
    };
    let restored = dir.path().join("restored.redb");
    driver.restore_latest(&restored).await.unwrap();
    assert_eq!(revision(&restored), 14);

    // Re-encryption reseals the view in parts too.
    let key = BackupKey::open(&keystore, kek()).unwrap();
    assert_eq!(key.rotate().unwrap(), 2);
    let offline = Driver::offline(store.clone(), &keystore, kek(), builder())
        .unwrap()
        .with_part_bytes(MIN_PART_BYTES);
    assert_eq!(offline.reencrypt(100).await.unwrap().moved_objects, 1);
    assert_eq!(offline.retire_versions().await.unwrap(), vec![1]);
    let view = stored_objects(&*store)
        .await
        .into_iter()
        .find(|(name, _)| name.starts_with("flower-backup/v1/obj/"))
        .unwrap()
        .0;
    let head = store.head(&ObjPath::from(view.as_str())).await.unwrap();
    assert_eq!(parts(head.e_tag), 2);
    let restored = dir.path().join("reencrypted.redb");
    offline.restore_latest(&restored).await.unwrap();
    assert_eq!(revision(&restored), 14);
}

/// An `s3://BUCKET/PREFIX` URL configures its endpoint, region and
/// credentials from the environment it is given, and keeps every object below
/// its prefix; what it cannot use, it refuses.
#[tokio::test(flavor = "multi_thread")]
async fn s3_urls_configure_from_the_environment() {
    let dir = tempfile::tempdir().unwrap();
    let endpoint = serve_s3(&dir.path().join("s3root")).await;
    let env = |name: &str| match name {
        "FLOWER_BACKUP_S3_ENDPOINT" => Some(endpoint.clone()),
        "FLOWER_BACKUP_S3_ACCESS_KEY_ID" => Some(ACCESS_KEY.to_string()),
        "AWS_SECRET_ACCESS_KEY" => Some(SECRET_KEY.to_string()),
        "AWS_REGION" => Some(REGION.to_string()),
        _ => None,
    };
    let store = Driver::s3_from_url(&format!("s3://{BUCKET}/deploy/one/"), env).unwrap();
    let driver = Driver::new(
        store,
        &dir.path().join("keystore.json"),
        kek(),
        Arc::new(tokio::sync::Mutex::new(fixture(dir.path()))),
        LogStore::open(&dir.path().join("raft.redb")).unwrap(),
        builder(),
    )
    .unwrap();
    let summary = driver.backup_now().await.unwrap();
    let root =
        Driver::s3_with_endpoint(BUCKET, REGION, ACCESS_KEY, SECRET_KEY, &endpoint, true).unwrap();
    let names: Vec<String> = root
        .list(Some(&object_store::path::Path::from("deploy")))
        .map_ok(|meta| meta.location.to_string())
        .try_collect()
        .await
        .unwrap();
    assert_eq!(names.len(), 2, "{names:?}");
    assert!(stored_objects(&*root).await.is_empty());
    assert!(
        names
            .iter()
            .all(|name| name.starts_with("deploy/one/flower-backup/v1/")),
        "{names:?}"
    );
    assert!(names.contains(&format!("deploy/one/{}", summary.manifest)));
    driver
        .restore_latest(&dir.path().join("restored.redb"))
        .await
        .unwrap();

    // Not S3, no bucket, an empty or relative prefix segment, unknown
    // addressing, half the credentials.
    for (url, set) in [
        ("file:///backups", None),
        ("s3://", None),
        ("s3://bucket/a//b", None),
        ("s3://bucket/../b", None),
        (
            "s3://bucket",
            Some(("FLOWER_BACKUP_S3_ADDRESSING", "sideways")),
        ),
        ("s3://bucket", Some(("AWS_ACCESS_KEY_ID", "alone"))),
    ] {
        let get = |name: &str| {
            set.filter(|(key, _)| *key == name)
                .map(|(_, value)| value.to_string())
        };
        assert!(
            matches!(Driver::s3_from_url(url, get), Err(BackupError::Refused(_))),
            "{url} {set:?}"
        );
    }
}
