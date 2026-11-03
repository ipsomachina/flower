//! The backup driver: snapshots, archives, retention, restore
//! and point-in-time recovery.
//!
//! Continuous backup snapshots every replicated database, seals
//! views and manifests under the deployment key and uploads only
//! ciphertext with opaque names. A separate archive loop seals
//! committed log entries into segments. Restore and PITR run
//! offline against a group file: install a base, replay segments to a
//! target as the backed-up history, then start a new history (applied
//! positions reset for a fresh ordering, no incarnation until the new
//! leader proposes one).
//!
//! Every multi-object change publishes before it deletes: new
//! objects land first, then the manifest naming them, and only then
//! are superseded objects removed. An error or crash at any step
//! leaves every published name restorable, and reruns plus
//! [`Driver::gc_orphans`] converge. No pointer names the latest
//! backup: it is the live manifest with the greatest
//! `(created_ms, backup_id)`, derived from the listing. Re-encryption
//! keeps both fields, so neither it nor a concurrent backup can move
//! "latest" back to an older backup.
//!
//! Archives are kept per history. A restored or re-bootstrapped
//! cluster restarts its log positions, so segments and the archive
//! position are namespaced by the group incarnation, nothing archives
//! until the group establishes one, and PITR replays only its base's
//! history. Archiving is idempotent: segments are named by index range
//! within their history, so a rerun overwrites what an interrupted run
//! uploaded, and restore picks a contiguous chain through any overlap.
//!
//! Continuous backup runs on every member. The leader takes bases and
//! archives; every member holds purged log entries until its history's
//! shared archive position passes them, so a new leader still stores
//! everything the archive lacks and continues the same archive instead
//! of starting over from a base. Restore reaches a time or, within one
//! history, an exact log index.
//!
//! Receipt expiry follows the replicated expiry clock. Manifests record
//! it with the cut; restore seeds the restored clock from it, and PITR
//! replay advances it entry by entry as live members did, so replayed
//! duplicate verdicts match theirs whatever the leaders' clocks did.
//!
//! PITR into an existing cluster means stopping every member,
//! restoring the same target on each group file, wiping the Raft
//! logs and re-bootstrapping membership. Restore refuses nothing
//! itself beyond corrupt data: redb's exclusive file lock fails the
//! install when a member still runs, which is the fencing. A new
//! cluster restores one group file and bootstraps around it.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::io::{BufWriter, Read, Seek, Write};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use flower_cluster::logstore::LogStore;
use flower_cluster::statemachine::{Chain as BatchChain, StateMachine};
use flower_cluster::types::ClusterWrite;
use flower_crypto::aead;
use flower_crypto::hash::{Hasher, sha256};
use flower_engine::batch::Head;
use flower_engine::engine::{Activation, Engine};
use flower_engine::json::parse_value;
use flower_engine::runtime::Runtime;
use flower_engine::snapshot::{SnapshotMeta, apply_snapshot, encode_snapshot_in};
use flower_engine::staging::{CODE_IDENTITY_PREFIX, code_identity_key};
use flower_engine::storage::GroupStore;
use flower_sdk::Value;
use futures::TryStreamExt;
use object_store::path::Path as ObjPath;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt};
use openraft::EntryPayload;

use crate::error::BackupError;
use crate::manifest::{
    BackupManifest, LogSegment, MANIFEST_VERSION, ManifestObject, SEGMENT_VERSION, SegmentEntry,
};
use crate::object::{
    ARCHIVE_POS_DIR, BackupKey, DEFAULT_PART_BYTES, PREFIX, SEGMENT_DIR, SegmentRange,
    archive_position_name, new_id, object_name, segment_dir, segment_name, segment_range_from_name,
};
use crate::transfer::{Download, Upload, Uploaded, io_error, store_error};

/// Builds the runtimes of a database's modules from stored code during
/// restore: database, module, Wasm bytes and manifest, as
/// [`flower_cluster::statemachine::RuntimeFactory::build`] takes them.
/// The module name keys backends that cache compiled code per module.
pub type RuntimeBuilder =
    Arc<dyn Fn(&[u8], &str, &[u8], &Value) -> Result<Box<dyn Runtime>, String> + Send + Sync>;

/// Entries per archived segment object.
const SEGMENT_ENTRIES: usize = 512;
/// Entries archived per call: bounded resumable work.
const ARCHIVE_ENTRIES: u64 = 4096;
/// Archive calls one continuous-backup tick runs at most while
/// draining the applied log.
const ARCHIVE_CALLS_PER_TICK: usize = 64;
/// How old an unreferenced view object must be before
/// [`Driver::gc_orphans`] collects it: longer than any backup or
/// re-encryption takes between uploading views and writing the
/// manifest that names them.
pub const GC_GRACE: Duration = Duration::from_secs(24 * 60 * 60);

/// The backup driver over one object store. Online drivers back
/// up and archive from live handles; offline drivers restore,
/// replay, retain and collect without them.
pub struct Driver {
    store: Arc<dyn ObjectStore>,
    key: BackupKey,
    engine: Option<Arc<tokio::sync::Mutex<Engine>>>,
    logs: Option<LogStore>,
    build_runtime: RuntimeBuilder,
    /// Whether this member leads; always, unless told otherwise.
    leads: Leadership,
    /// What continuous backup last did here.
    status: std::sync::Mutex<ContinuousStatus>,
    /// Bytes per uploaded part: a larger object goes up in parts.
    part_bytes: usize,
}

/// One full backup's summary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackupSummary {
    /// Opaque backup ID.
    pub backup_id: String,
    /// Sealed manifest object name.
    pub manifest: String,
    /// Database views uploaded.
    pub objects: usize,
    /// Group applied index the views cover.
    pub covered_index: u64,
    /// Plaintext payload bytes across views.
    pub plaintext_bytes: u64,
    /// Ciphertext bytes across views.
    pub ciphertext_bytes: u64,
}

/// One archive run's summary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArchiveSummary {
    /// Position archived from, exclusive.
    pub from: u64,
    /// Position archived through, inclusive.
    pub to: u64,
    /// Segment objects uploaded.
    pub segments: u32,
    /// Data entries archived.
    pub entries: usize,
    /// Sealed bytes those segments stored.
    pub bytes: u64,
}

/// One restore's summary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RestoreSummary {
    /// Restored backup ID.
    pub backup_id: String,
    /// Manifest restored from.
    pub manifest: String,
    /// Database views installed.
    pub objects: usize,
    /// Group applied index the base covers.
    pub covered_index: u64,
}

/// One PITR run's summary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PitrSummary {
    /// Base backup ID.
    pub backup_id: String,
    /// Base manifest name.
    pub manifest: String,
    /// Entries replayed after the base.
    pub replayed: usize,
    /// Index where replay stopped past the target, if any.
    pub stopped_index: Option<u64>,
    /// Last applied entry stamp in ms.
    pub reached_ms: u64,
    /// Requested target in ms.
    pub target_ms: u64,
}

/// One restore-to-index run's summary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexRestoreSummary {
    /// Base backup ID.
    pub backup_id: String,
    /// Base manifest name.
    pub manifest: String,
    /// History incarnation restored.
    pub incarnation: u64,
    /// Entries replayed after the base.
    pub replayed: usize,
    /// Log index the restored state covers, exactly.
    pub index: u64,
    /// Stamp of the last replayed entry in ms, zero when none replayed.
    pub reached_ms: u64,
}

/// One live backup as [`Driver::list_backups`] reports it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListedBackup {
    /// Opaque backup ID.
    pub backup_id: String,
    /// Sealed manifest object name, for [`Driver::restore_backup`].
    pub manifest: String,
    /// Creation time in ms since epoch: time recovery uses the latest
    /// backup at or before its target.
    pub created_ms: u64,
    /// History incarnation the backup belongs to, zero when the group
    /// had none yet.
    pub incarnation: u64,
    /// Group applied index the views cover.
    pub covered_index: u64,
    /// Database views.
    pub objects: usize,
    /// Ciphertext bytes across views.
    pub ciphertext_bytes: u64,
    /// The last index of its history a restore from it reaches: its
    /// cut, extended through the contiguous archive after it.
    pub restorable_through: u64,
    /// Stamp of the last archived write it reaches, if any write was
    /// archived after its cut.
    pub restorable_through_ms: Option<u64>,
}

/// Whether this member leads, asked once per continuous-backup tick.
pub type Leadership = Arc<dyn Fn() -> bool + Send + Sync>;

/// How continuous backup paces itself.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContinuousSettings {
    /// How often the leader archives newly applied entries and every
    /// member lets its log go of what the archive holds.
    pub archive_interval: Duration,
    /// How old the newest base of the leader's history may grow before
    /// the leader takes another.
    pub base_interval: Duration,
    /// How many sealed bytes the leader may archive after the newest
    /// base of its history before it takes another, unless that base is
    /// larger: as v1, a base is due once the archive since it outgrows
    /// both this and the base itself, so replaying it never costs more
    /// than another base. 64 MiB by default, as v1; `None` leaves bases
    /// to the interval.
    pub base_bytes: Option<u64>,
    /// How far back restores reach: retention keeps every point of this
    /// window restorable, however many bases that takes (30 days, as
    /// v1), and thins the older ones to one per base interval.
    pub retention: Duration,
    /// How often retention, orphan collection and key retirement run
    /// besides after each base (an hour, as v1). Their failure never
    /// makes a base due.
    pub retention_interval: Duration,
    /// How long the leader waits after a failed base before it tries
    /// another (a minute, as v1).
    pub base_retry: Duration,
    /// Most purged entries a member's log keeps for an archive that
    /// cannot keep up (see [`LogStore::hold_purges`]); `None` keeps
    /// every one until it is archived.
    pub max_held: Option<u64>,
    /// Most bytes of purged entries a member's log keeps for an archive
    /// that cannot keep up (see [`LogStore::hold_purges_capped`]): 1 GiB
    /// by default, as v1; `None` keeps every one.
    pub max_held_bytes: Option<u64>,
    /// How long a stopping leader may spend archiving what it applied
    /// since its last tick, as v1 bounded its last shipment (a sixth of
    /// `serve`'s shutdown budget, 5 s by default). A tick running when
    /// the stop comes, a base included, is cancelled at once.
    pub final_archive_within: Duration,
}

/// The default cap on the bytes a member's log holds for the archive.
pub const DEFAULT_MAX_HELD_BYTES: u64 = 1 << 30;
/// The default archive size since the newest base that makes another
/// due, as v1.
pub const DEFAULT_BASE_BYTES: u64 = 64 << 20;
/// How far back restores reach by default, as v1.
pub const DEFAULT_RETENTION: Duration = Duration::from_secs(30 * 24 * 60 * 60);
/// How often retention runs besides after each base, as v1.
pub const DEFAULT_RETENTION_INTERVAL: Duration = Duration::from_secs(60 * 60);
/// How long a leader waits after a failed base, as v1.
pub const BASE_RETRY: Duration = Duration::from_secs(60);

impl Default for ContinuousSettings {
    fn default() -> Self {
        ContinuousSettings {
            archive_interval: Duration::from_secs(1),
            base_interval: Duration::from_secs(24 * 60 * 60),
            base_bytes: Some(DEFAULT_BASE_BYTES),
            retention: DEFAULT_RETENTION,
            retention_interval: DEFAULT_RETENTION_INTERVAL,
            base_retry: BASE_RETRY,
            max_held: None,
            max_held_bytes: Some(DEFAULT_MAX_HELD_BYTES),
            final_archive_within: Duration::from_secs(5),
        }
    }
}

/// What continuous backup is doing on this member, for operators.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct ContinuousStatus {
    /// `starting` until the first tick, then `leader` or `follower`
    /// as this member last ticked, and `stopped` once the loop ended.
    pub role: &'static str,
    /// Ticks run, failed ones included.
    pub ticks: u64,
    /// When the last tick ended, in ms since the epoch.
    pub last_tick_ms: Option<u64>,
    /// The archive position this member last archived or released
    /// through.
    pub archived_through: Option<u64>,
    /// The newest base this member took.
    pub newest_base: Option<BaseStatus>,
    /// Bases this member took.
    pub bases_taken: u64,
    /// Segment objects this member uploaded.
    pub segments_archived: u64,
    /// Data entries those segments carried.
    pub entries_archived: u64,
    /// Failed ticks, in all.
    pub errors: u64,
    /// Whether the last tick failed.
    pub failing: bool,
    /// The last failure and when, in ms since the epoch.
    pub last_error: Option<(String, u64)>,
}

/// One base backup as continuous status reports it.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct BaseStatus {
    /// Opaque backup ID.
    pub backup_id: String,
    /// Group applied index the base covers.
    pub covered_index: u64,
    /// When it finished, in ms since the epoch.
    pub at_ms: u64,
    /// Ciphertext bytes across its views.
    pub ciphertext_bytes: u64,
}

/// One retention run's summary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetentionSummary {
    /// Manifests kept.
    pub kept: usize,
    /// Manifests deleted.
    pub deleted_manifests: usize,
    /// Data objects deleted.
    pub deleted_objects: usize,
    /// Segments deleted.
    pub deleted_segments: usize,
}

/// One re-encryption run's summary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReencryptSummary {
    /// Data and segment objects moved forward.
    pub moved_objects: usize,
    /// Manifests rewritten.
    pub manifests: usize,
    /// Segments rewritten.
    pub segments: usize,
}

/// One garbage-collection run's summary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GcSummary {
    /// Orphan objects deleted.
    pub deleted: usize,
}

impl Driver {
    /// Open over an object store with a keystore, a live engine for
    /// snapshots, the Raft log for archiving and a runtime builder
    /// for restore.
    pub fn new(
        store: Arc<dyn ObjectStore>,
        keystore: &Path,
        kek: [u8; 32],
        engine: Arc<tokio::sync::Mutex<Engine>>,
        logs: LogStore,
        build_runtime: RuntimeBuilder,
    ) -> Result<Self, BackupError> {
        let key = BackupKey::open(keystore, kek).map_err(BackupError::Crypto)?;
        Ok(Driver {
            store,
            key,
            engine: Some(engine),
            logs: Some(logs),
            build_runtime,
            leads: Arc::new(|| true),
            status: std::sync::Mutex::default(),
            part_bytes: DEFAULT_PART_BYTES,
        })
    }

    /// Tell continuous backup whether this member leads: only the
    /// leader takes bases and archives, while every member holds its
    /// log for the archive. Without this the driver always leads.
    pub fn with_leadership(mut self, leads: Leadership) -> Self {
        self.leads = leads;
        self
    }

    /// Upload objects larger than `part_bytes` (at least 1) in parts of
    /// that size; [`DEFAULT_PART_BYTES`] otherwise. S3 refuses parts
    /// under 5 MiB but an upload's last.
    pub fn with_part_bytes(mut self, part_bytes: usize) -> Self {
        self.part_bytes = part_bytes.max(1);
        self
    }

    /// Open an offline driver: restore, PITR, retention, retirement,
    /// re-encryption and collection without live handles.
    pub fn offline(
        store: Arc<dyn ObjectStore>,
        keystore: &Path,
        kek: [u8; 32],
        build_runtime: RuntimeBuilder,
    ) -> Result<Self, BackupError> {
        let key = BackupKey::open(keystore, kek).map_err(BackupError::Crypto)?;
        Ok(Driver {
            store,
            key,
            engine: None,
            logs: None,
            build_runtime,
            leads: Arc::new(|| true),
            status: std::sync::Mutex::default(),
            part_bytes: DEFAULT_PART_BYTES,
        })
    }

    /// A fresh in-memory object store, for tests and development.
    pub fn memory_store() -> Arc<dyn ObjectStore> {
        Arc::new(object_store::memory::InMemory::new())
    }

    /// In-memory backend, for tests and development.
    pub fn memory(
        keystore: &Path,
        kek: [u8; 32],
        engine: Arc<tokio::sync::Mutex<Engine>>,
        logs: LogStore,
        build_runtime: RuntimeBuilder,
    ) -> Result<Self, BackupError> {
        Self::new(
            Self::memory_store(),
            keystore,
            kek,
            engine,
            logs,
            build_runtime,
        )
    }

    /// Local filesystem backend under a directory, created when
    /// missing.
    pub fn local(
        dir: &Path,
        keystore: &Path,
        kek: [u8; 32],
        engine: Arc<tokio::sync::Mutex<Engine>>,
        logs: LogStore,
        build_runtime: RuntimeBuilder,
    ) -> Result<Self, BackupError> {
        std::fs::create_dir_all(dir).map_err(|error| BackupError::Io(error.to_string()))?;
        Self::new(
            Self::local_store(dir)?,
            keystore,
            kek,
            engine,
            logs,
            build_runtime,
        )
    }

    /// The local filesystem backend under an existing directory, for
    /// offline drivers.
    pub fn local_store(dir: &Path) -> Result<Arc<dyn ObjectStore>, BackupError> {
        let store = object_store::local::LocalFileSystem::new_with_prefix(dir)
            .map_err(|error| BackupError::Store(error.to_string()))?;
        Ok(Arc::new(store))
    }

    /// S3-compatible backend from static credentials.
    #[cfg(feature = "s3")]
    pub fn s3(
        bucket: &str,
        region: &str,
        key_id: &str,
        secret: &str,
    ) -> Result<Arc<dyn ObjectStore>, BackupError> {
        let store = object_store::aws::AmazonS3Builder::new()
            .with_bucket_name(bucket)
            .with_region(region)
            .with_access_key_id(key_id)
            .with_secret_access_key(secret)
            .build()
            .map_err(|error| BackupError::Store(error.to_string()))?;
        Ok(Arc::new(store))
    }

    /// The S3 backend an `s3://BUCKET/PREFIX` URL names, its objects
    /// below the prefix, configured as v1 from the environment `get`
    /// reads. `FLOWER_BACKUP_S3_REGION` (else `AWS_REGION`, else
    /// `AWS_DEFAULT_REGION`, else `us-east-1`); `FLOWER_BACKUP_S3_ENDPOINT`
    /// for another S3 implementation, plain HTTP when it says `http://`;
    /// `FLOWER_BACKUP_S3_ADDRESSING`, `path` or `virtual`, virtual-hosted
    /// by default without an endpoint and path-style with one; and
    /// credentials in `FLOWER_BACKUP_S3_ACCESS_KEY_ID`,
    /// `FLOWER_BACKUP_S3_SECRET_ACCESS_KEY` and
    /// `FLOWER_BACKUP_S3_SESSION_TOKEN`, else their `AWS_` names. Without
    /// static keys, AWS's own chain answers: a web identity token
    /// (`AWS_WEB_IDENTITY_TOKEN_FILE`, `AWS_ROLE_ARN`), container
    /// credentials (`AWS_CONTAINER_CREDENTIALS_*`) or instance metadata.
    #[cfg(feature = "s3")]
    pub fn s3_from_url(
        url: &str,
        get: impl Fn(&str) -> Option<String>,
    ) -> Result<Arc<dyn ObjectStore>, BackupError> {
        use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};
        let get = |name: &str| get(name).filter(|value| !value.trim().is_empty());
        let refuse = |why: String| Err(BackupError::Refused(why));
        let Some(rest) = url.strip_prefix("s3://") else {
            return refuse(format!("{url} is not an s3:// URL"));
        };
        let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
        let prefix = prefix.trim_matches('/');
        if bucket.is_empty() {
            return refuse(format!("{url} names no bucket (s3://BUCKET/PREFIX)"));
        }
        if !prefix.is_empty()
            && prefix
                .split('/')
                .any(|part| part.is_empty() || part == "." || part == "..")
        {
            return refuse(format!("{url} has an invalid prefix"));
        }
        let endpoint = get("FLOWER_BACKUP_S3_ENDPOINT");
        let virtual_hosted = match get("FLOWER_BACKUP_S3_ADDRESSING").as_deref() {
            None => endpoint.is_none(),
            Some("path") => false,
            Some("virtual") => true,
            Some(other) => {
                return refuse(format!(
                    "FLOWER_BACKUP_S3_ADDRESSING must be path or virtual, not {other}"
                ));
            }
        };
        let region = get("FLOWER_BACKUP_S3_REGION")
            .or_else(|| get("AWS_REGION"))
            .or_else(|| get("AWS_DEFAULT_REGION"))
            .unwrap_or_else(|| "us-east-1".to_string());
        let mut builder = AmazonS3Builder::new();
        for name in [
            "AWS_WEB_IDENTITY_TOKEN_FILE",
            "AWS_ROLE_ARN",
            "AWS_ROLE_SESSION_NAME",
            "AWS_ENDPOINT_URL_STS",
            "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
            "AWS_CONTAINER_CREDENTIALS_FULL_URI",
            "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
        ] {
            if let (Some(value), Ok(key)) = (
                get(name),
                name.to_ascii_lowercase().parse::<AmazonS3ConfigKey>(),
            ) {
                builder = builder.with_config(key, value);
            }
        }
        let key_id = get("FLOWER_BACKUP_S3_ACCESS_KEY_ID").or_else(|| get("AWS_ACCESS_KEY_ID"));
        let secret =
            get("FLOWER_BACKUP_S3_SECRET_ACCESS_KEY").or_else(|| get("AWS_SECRET_ACCESS_KEY"));
        match (key_id, secret) {
            (Some(key_id), Some(secret)) => {
                builder = builder
                    .with_access_key_id(key_id)
                    .with_secret_access_key(secret);
            }
            (None, None) => {}
            _ => {
                return refuse(
                    "S3 backups need both an access key ID and a secret access key, or neither"
                        .to_string(),
                );
            }
        }
        if let Some(token) =
            get("FLOWER_BACKUP_S3_SESSION_TOKEN").or_else(|| get("AWS_SESSION_TOKEN"))
        {
            builder = builder.with_token(token);
        }
        if let Some(endpoint) = &endpoint {
            builder = builder
                .with_endpoint(endpoint)
                .with_allow_http(endpoint.starts_with("http://"));
        }
        let store = builder
            .with_bucket_name(bucket)
            .with_region(region)
            .with_virtual_hosted_style_request(virtual_hosted)
            .build()
            .map_err(|error| BackupError::Store(error.to_string()))?;
        if prefix.is_empty() {
            return Ok(Arc::new(store));
        }
        Ok(Arc::new(object_store::prefix::PrefixStore::new(
            store, prefix,
        )))
    }

    /// S3-compatible backend with an explicit endpoint, for MinIO,
    /// Ceph, LocalStack and other S3 implementations.
    #[cfg(feature = "s3")]
    pub fn s3_with_endpoint(
        bucket: &str,
        region: &str,
        key_id: &str,
        secret: &str,
        endpoint: &str,
        allow_http: bool,
    ) -> Result<Arc<dyn ObjectStore>, BackupError> {
        let builder = object_store::aws::AmazonS3Builder::new()
            .with_bucket_name(bucket)
            .with_region(region)
            .with_access_key_id(key_id)
            .with_secret_access_key(secret)
            .with_endpoint(endpoint);
        let builder = if allow_http {
            builder.with_allow_http(true)
        } else {
            builder
        };
        let store = builder
            .build()
            .map_err(|error| BackupError::Store(error.to_string()))?;
        Ok(Arc::new(store))
    }

    /// Take a full backup: pin one read of the group file under the
    /// engine lock, which takes no longer than opening it (every entry
    /// applies whole under the lock, so the read is exactly the state at
    /// the group position it records), then encode every replicated
    /// database from that read on the blocking pool, one at a time, and
    /// compress, seal and upload each in parts as it goes, then the
    /// manifest. Applies go on meanwhile, one database's view is in
    /// memory at a time, and the views, the covered index, the expiry
    /// clock, the incarnation and the head form one cut because they
    /// come from one read. `$cluster` never backs up: a fresh ordering
    /// rebuilds it from the restored registry and the manifest's clock.
    pub async fn backup_now(&self) -> Result<BackupSummary, BackupError> {
        let engine = self
            .engine
            .as_ref()
            .ok_or_else(|| BackupError::Refused("offline driver cannot back up".to_string()))?;
        let mut read = {
            let engine = engine.lock().await;
            engine.store().begin_read().map_err(BackupError::engine)?
        };
        let dbs = StateMachine::replicated_databases_in(&read).map_err(BackupError::engine)?;
        let covered = StateMachine::group_applied_in(&read)
            .map_err(BackupError::engine)?
            .map_or(0, |id| id.index);
        let expiry_clock = StateMachine::expiry_clock_in(&read).map_err(BackupError::engine)?;
        let incarnation = StateMachine::incarnation_in(&read).map_err(BackupError::engine)?;
        let head = StateMachine::head_in(&read).map_err(BackupError::engine)?;
        let backup_id = new_id();
        let mut objects = Vec::with_capacity(dbs.len());
        let mut plaintext_bytes = 0u64;
        let mut ciphertext_bytes = 0u64;
        for db in dbs {
            let (pinned, encoded) = tokio::task::spawn_blocking(move || {
                let encoded = encode_snapshot_in(&read, &db);
                (read, encoded)
            })
            .await
            .map_err(|error| BackupError::Engine(error.to_string()))?;
            read = pinned;
            let (meta, payload) = encoded.map_err(BackupError::engine)?;
            plaintext_bytes += payload.len() as u64;
            let name = object_name("obj");
            let uploaded = self.put_object(&name, true, &payload).await?;
            ciphertext_bytes += uploaded.stored;
            objects.push(ManifestObject {
                name,
                db: meta.db,
                revision: meta.revision,
                applied_log: meta.applied_log,
                incarnation: meta.incarnation,
                key_version: uploaded.version,
                plaintext_sha256: sha256(&payload).to_vec(),
                compressed_len: uploaded.sealed_input,
                ciphertext_len: uploaded.stored,
            });
        }
        drop(read);
        let views = objects.len();
        let manifest = BackupManifest {
            version: MANIFEST_VERSION,
            backup_id: backup_id.clone(),
            created_ms: now_ms(),
            covered_index: covered,
            expiry_clock,
            incarnation,
            head: Some(head),
            supersedes: None,
            objects,
        };
        let manifest_name = object_name("manifest");
        self.put_sealed(&manifest_name, &encode_json(&manifest)?)
            .await?;
        Ok(BackupSummary {
            backup_id,
            manifest: manifest_name,
            objects: views,
            covered_index: covered,
            plaintext_bytes,
            ciphertext_bytes,
        })
    }

    /// Archive newly committed entries of the group's current history
    /// into sealed segments and advance that history's archive
    /// pointer. Only entries at or below the group's applied index
    /// archive: they are committed, while the log tail past them may
    /// still be truncated and replaced. Nothing archives until the
    /// group establishes its incarnation, which would otherwise split
    /// one history across two namespaces; the first run after that
    /// archives from the start of the log. Segments are named by their
    /// index range, so a run that failed before moving the pointer is
    /// simply redone: the rerun overwrites the same names, or uploads
    /// a wider range that restore prefers.
    ///
    /// Entries come from what this member's log still stores, purge
    /// hold included, never from Raft's purge point. When the entries
    /// after the pointer are gone (purged before a hold kept them,
    /// replaced by a snapshot install, or archived by another member
    /// past the pointer it last wrote), archiving resumes past them
    /// only when the history's own segments or a base of the history
    /// already cover them; otherwise the run refuses loudly instead of
    /// recording a gap. A run stops at a hole a snapshot install left
    /// in the log, which the next run meets as missing entries.
    pub async fn archive_now(&self) -> Result<ArchiveSummary, BackupError> {
        let (Some(engine), Some(logs)) = (self.engine.as_ref(), self.logs.as_ref()) else {
            return Err(BackupError::Refused(
                "offline driver cannot archive".to_string(),
            ));
        };
        let (applied, incarnation) = {
            let engine = engine.lock().await;
            (
                applied_index(&engine)?,
                StateMachine::incarnation(&engine).map_err(BackupError::engine)?,
            )
        };
        if incarnation == 0 {
            return Ok(ArchiveSummary {
                from: 0,
                to: 0,
                segments: 0,
                entries: 0,
                bytes: 0,
            });
        }
        let from = self.archive_position(incarnation).await?;
        let mut since = from;
        let first = logs
            .first_index_from(since + 1)
            .map_err(|error| BackupError::Store(error.to_string()))?;
        if since < applied && first != Some(since + 1) {
            let lost_through = first.map_or(applied, |first| (first - 1).min(applied));
            since = self.skip_lost(incarnation, since, lost_through).await?;
        }
        if since >= applied {
            if since > from {
                self.put_sealed(&archive_position_name(incarnation), &since.to_be_bytes())
                    .await?;
            }
            return Ok(ArchiveSummary {
                from,
                to: since,
                segments: 0,
                entries: 0,
                bytes: 0,
            });
        }
        let end = since
            .saturating_add(1 + ARCHIVE_ENTRIES)
            .min(applied.saturating_add(1));
        // Held entries below Raft's purge point count: Raft's own reads
        // (`read_entries`) start above it.
        let mut entries = logs
            .read_retained(since + 1, end)
            .map_err(|error| BackupError::Store(error.to_string()))?;
        // Applied entries are dense up to a hole a snapshot install left
        // in this member's log: archive that prefix and leave the hole to
        // the next run. None at all means they went under this run.
        let dense = entries
            .iter()
            .zip(since + 1..end)
            .take_while(|(entry, expected)| entry.log_id.index == *expected)
            .count();
        if dense == 0 {
            return Err(BackupError::Refused(format!(
                "archive gap: log entry {} is missing",
                since + 1
            )));
        }
        entries.truncate(dense);
        let end = since + 1 + dense as u64;
        // Group consecutive indexes into segments of at most
        // SEGMENT_ENTRIES data entries; blanks and membership entries
        // advance the range without entries, so ranges tile densely.
        let mut segments = Vec::new();
        let mut segment = LogSegment {
            version: SEGMENT_VERSION,
            first_index: since + 1,
            last_index: since + 1,
            entries: Vec::new(),
        };
        for entry in &entries {
            let index = entry.log_id.index;
            if segment.entries.len() >= SEGMENT_ENTRIES {
                let next = LogSegment {
                    version: SEGMENT_VERSION,
                    first_index: index,
                    last_index: index,
                    entries: Vec::new(),
                };
                segments.push(std::mem::replace(&mut segment, next));
            }
            segment.last_index = index;
            if let EntryPayload::Normal(write) = &entry.payload {
                let proposed_ms = write.proposed_ms();
                segment.entries.push(SegmentEntry {
                    index,
                    proposed_ms,
                    write: write.clone(),
                });
            }
        }
        segments.push(segment);
        let mut archived = 0usize;
        let mut bytes = 0u64;
        for segment in &segments {
            let uploaded = self
                .put_object(
                    &segment_name(incarnation, segment.first_index, segment.last_index),
                    true,
                    &encode_json(segment)?,
                )
                .await?;
            archived += segment.entries.len();
            bytes += uploaded.stored;
        }
        let to = end - 1;
        self.put_sealed(&archive_position_name(incarnation), &to.to_be_bytes())
            .await?;
        Ok(ArchiveSummary {
            from,
            to,
            segments: segments.len() as u32,
            entries: archived,
            bytes,
        })
    }

    /// Where archiving one history resumes when this member's log lacks
    /// entries `since + 1 ..= lost_through`: past the history's segments
    /// when they already cover those entries contiguously, else at
    /// `lost_through` when a base of the history covers it. Anything
    /// else refuses: archiving past the hole would break every restore
    /// from an older base.
    async fn skip_lost(
        &self,
        incarnation: u64,
        since: u64,
        lost_through: u64,
    ) -> Result<u64, BackupError> {
        let archived = plan_chain(&self.segment_spans(incarnation).await?, since).next - 1;
        if archived >= lost_through {
            return Ok(archived);
        }
        let history = self.list_manifests().await?.into_iter();
        let base = newest(history.filter(|(_, manifest)| manifest.incarnation == incarnation));
        match base {
            Some((_, base)) if base.covered_index >= lost_through => Ok(lost_through),
            _ => Err(BackupError::Refused(format!(
                "entries {}..={lost_through} are gone from this member's log, and neither \
                 the archive nor a base covers them",
                since + 1
            ))),
        }
    }

    /// Restore one history to a log index into `group_path` offline:
    /// install that history's base with the greatest cut at or before
    /// `index`, then replay its archived entries through `index`
    /// exactly. The history is `incarnation`, or the latest backup's
    /// when `None`: indexes count within one history, since a restored
    /// cluster restarts them. Refuses before touching `group_path` when
    /// no base qualifies or the archive stops short of `index`. The
    /// result is a new history, as for [`Driver::restore_backup`].
    pub async fn restore_to_index(
        &self,
        group_path: &Path,
        index: u64,
        incarnation: Option<u64>,
    ) -> Result<IndexRestoreSummary, BackupError> {
        let manifests = self.list_manifests().await?;
        let incarnation = match incarnation {
            Some(incarnation) => incarnation,
            None => newest(manifests.iter().cloned())
                .map(|(_, manifest)| manifest.incarnation)
                .ok_or_else(|| BackupError::NotFound("no backups taken".to_string()))?,
        };
        let (base_name, base) = manifests
            .into_iter()
            .filter(|(_, manifest)| {
                manifest.incarnation == incarnation && manifest.covered_index <= index
            })
            .max_by(|(_, a), (_, b)| {
                a.covered_index
                    .cmp(&b.covered_index)
                    .then_with(|| by_recency(a, b))
            })
            .ok_or_else(|| {
                BackupError::Refused(format!(
                    "no base of history {incarnation:016x} at or before index {index}"
                ))
            })?;
        let spans = self.segment_spans(incarnation).await?;
        let chain = plan_chain(&spans, base.covered_index);
        if chain.next <= index {
            return Err(BackupError::Refused(if chain.stranded {
                format!("archive gap: no segment covers entry {}", chain.next)
            } else {
                format!(
                    "the archive of history {incarnation:016x} holds entries through {} only",
                    chain.next - 1
                )
            }));
        }
        let needed = chain.spans.into_iter().filter(|span| span.first <= index);
        let segments = self.download_segments(needed).await?;
        let views = self.download_views(&base, group_path).await?;
        let mut engine = self.install_views(
            group_path,
            views,
            base.expiry_clock,
            base.head,
            Some(base.incarnation),
        )?;
        let replay =
            self.replay_segments(&mut engine, &segments, &base, |entry| entry.index > index)?;
        Ok(IndexRestoreSummary {
            backup_id: base.backup_id,
            manifest: base_name,
            incarnation,
            replayed: replay.replayed,
            index,
            reached_ms: replay.reached_ms,
        })
    }

    /// Every live backup, oldest first, with how far a restore from it
    /// reaches: its cut, extended through the contiguous archive of its
    /// history after it. Plans from names, downloading only manifests
    /// and, for each reach's stamp, the last segments of its chain.
    pub async fn list_backups(&self) -> Result<Vec<ListedBackup>, BackupError> {
        let mut manifests = self.list_manifests().await?;
        manifests.sort_by(|(_, a), (_, b)| by_recency(a, b));
        let mut spans: HashMap<u64, Vec<Span>> = HashMap::new();
        // Each segment's last write as (index, stamp), downloaded once.
        let mut last_writes: HashMap<String, Option<(u64, u64)>> = HashMap::new();
        let mut listed = Vec::with_capacity(manifests.len());
        for (name, manifest) in manifests {
            if let std::collections::hash_map::Entry::Vacant(vacant) =
                spans.entry(manifest.incarnation)
            {
                vacant.insert(self.segment_spans(manifest.incarnation).await?);
            }
            let chain = plan_chain(&spans[&manifest.incarnation], manifest.covered_index);
            // The newest stamp is the last segment's last write, unless
            // it holds only blanks; a write at or before the cut is no
            // stamp of what the archive adds.
            let mut through_ms = None;
            for span in chain.spans.iter().rev() {
                let last_write = match last_writes.get(&span.name) {
                    Some(last_write) => *last_write,
                    None => {
                        let segment = self.download_segments([*span]).await?;
                        let last_write = segment
                            .first()
                            .and_then(|segment| segment.entries.last())
                            .map(|entry| (entry.index, entry.proposed_ms));
                        last_writes.insert(span.name.clone(), last_write);
                        last_write
                    }
                };
                if let Some((index, proposed_ms)) = last_write {
                    through_ms = (index > manifest.covered_index).then_some(proposed_ms);
                    break;
                }
            }
            listed.push(ListedBackup {
                backup_id: manifest.backup_id,
                manifest: name,
                created_ms: manifest.created_ms,
                incarnation: manifest.incarnation,
                covered_index: manifest.covered_index,
                objects: manifest.objects.len(),
                ciphertext_bytes: manifest
                    .objects
                    .iter()
                    .map(|object| object.ciphertext_len)
                    .sum(),
                restorable_through: chain.next - 1,
                restorable_through_ms: through_ms,
            });
        }
        Ok(listed)
    }

    /// Restore a named backup into `group_path` offline: install the
    /// views, register the databases, start a new history at the
    /// backup's expiry clock and reload runtimes. Any existing group
    /// file is replaced, and a sibling Raft log is removed: the
    /// restored node must re-bootstrap membership before serving.
    pub async fn restore_backup(
        &self,
        group_path: &Path,
        manifest_name: &str,
    ) -> Result<RestoreSummary, BackupError> {
        let manifest = self.open_manifest(manifest_name).await?;
        self.restore_manifest(group_path, manifest_name, &manifest)
            .await
    }

    /// Restore the latest backup into `group_path` offline: the live
    /// manifest with the greatest `(created_ms, backup_id)`.
    pub async fn restore_latest(&self, group_path: &Path) -> Result<RestoreSummary, BackupError> {
        let Some((name, manifest)) = self.latest_manifest().await? else {
            return Err(BackupError::NotFound("no backups taken".to_string()));
        };
        self.restore_manifest(group_path, &name, &manifest).await
    }

    /// Install one opened manifest's views.
    async fn restore_manifest(
        &self,
        group_path: &Path,
        name: &str,
        manifest: &BackupManifest,
    ) -> Result<RestoreSummary, BackupError> {
        let views = self.download_views(manifest, group_path).await?;
        self.install_views(group_path, views, manifest.expiry_clock, None, None)?;
        Ok(RestoreSummary {
            backup_id: manifest.backup_id.clone(),
            manifest: name.to_string(),
            objects: manifest.objects.len(),
            covered_index: manifest.covered_index,
        })
    }

    /// Point-in-time recovery into `group_path` offline: install the
    /// latest base at or before `target_ms`, then replay its history's
    /// archived entries in index order while their stamps stay at or
    /// before the target, stopping at the first exceed. Replay reads
    /// receipt expiry at the base's expiry clock advanced by every
    /// replayed stamp, as live members did. Recovered state is always
    /// a consistent prefix; under clock skew it may sit seconds before
    /// the target, and a base taken before the group established its
    /// incarnation has no archived history to replay. Archive gaps
    /// refuse loudly.
    pub async fn pitr(
        &self,
        group_path: &Path,
        target_ms: u64,
    ) -> Result<PitrSummary, BackupError> {
        let manifests = self.list_manifests().await?.into_iter();
        let (base_name, base) = newest(
            manifests.filter(|(_, manifest)| manifest.created_ms <= target_ms),
        )
        .ok_or_else(|| BackupError::Refused(format!("no base backup at or before {target_ms}")))?;
        self.replay_after(group_path, base_name, base, target_ms)
            .await
    }

    /// Restore the newest state the backups hold into `group_path`
    /// offline: the latest base and every entry its history archived
    /// after it, whatever this machine's clock says, as v1's restore did
    /// without a point. The summary's `target_ms` is `u64::MAX`. Archive
    /// gaps refuse loudly.
    pub async fn restore_newest(&self, group_path: &Path) -> Result<PitrSummary, BackupError> {
        let Some((base_name, base)) = self.latest_manifest().await? else {
            return Err(BackupError::NotFound("no backups taken".to_string()));
        };
        self.replay_after(group_path, base_name, base, u64::MAX)
            .await
    }

    /// Install `base`, then replay its history's archived entries while
    /// their stamps stay at or before `target_ms`.
    async fn replay_after(
        &self,
        group_path: &Path,
        base_name: String,
        base: BackupManifest,
        target_ms: u64,
    ) -> Result<PitrSummary, BackupError> {
        let views = self.download_views(&base, group_path).await?;
        let mut engine = self.install_views(
            group_path,
            views,
            base.expiry_clock,
            base.head,
            Some(base.incarnation),
        )?;
        let segments = self
            .load_segments_after(base.incarnation, base.covered_index)
            .await?;
        let replay = self.replay_segments(&mut engine, &segments, &base, |entry| {
            entry.proposed_ms > target_ms
        })?;
        Ok(PitrSummary {
            backup_id: base.backup_id,
            manifest: base_name,
            replayed: replay.replayed,
            stopped_index: replay.stopped_index,
            reached_ms: replay.reached_ms,
            target_ms,
        })
    }

    /// Replay chained segments after `base`'s cut into a restoring
    /// engine at fresh positions, in index order, until the first entry
    /// `past` the target. Chained segments may overlap; each index
    /// replays once. Replay starts from the base's group head, so
    /// archived batches apply or go stale as they did live; a base from
    /// before batch entries has none, and its first archived batch is
    /// taken to follow what its writer saw. Then the head resets: the
    /// restored engine starts a history of its own, whose indexes
    /// restart, and no batch of it may follow an entry of this one.
    fn replay_segments(
        &self,
        engine: &mut Engine,
        segments: &[LogSegment],
        base: &BackupManifest,
        past: impl Fn(&SegmentEntry) -> bool,
    ) -> Result<Replay, BackupError> {
        let mut replay = Replay::default();
        let mut floor = base.covered_index;
        let mut trust = base.head.is_none();
        'segments: for segment in segments {
            for entry in &segment.entries {
                if entry.index <= floor {
                    continue;
                }
                if past(entry) {
                    replay.stopped_index = Some(entry.index);
                    break 'segments;
                }
                if std::mem::take(&mut trust)
                    && let ClusterWrite::Batch { follows, .. } = &entry.write
                {
                    record_head(engine, *follows)?;
                }
                let position = replay.replayed as u64 + 1;
                Self::replay_entry(
                    &self.build_runtime,
                    engine,
                    &entry.write,
                    position,
                    entry.index,
                )?;
                replay.replayed += 1;
                replay.reached_ms = entry.proposed_ms;
                floor = entry.index;
            }
        }
        // Only now does the replayed history end: the restored group
        // starts its own, under which nothing bound to this one holds,
        // lineages carried in by replayed moves included.
        let txn = engine.store().begin_write().map_err(BackupError::engine)?;
        Head::Entry(0).record(&txn).map_err(BackupError::engine)?;
        StateMachine::end_history(&txn).map_err(BackupError::engine)?;
        txn.commit().map_err(BackupError::engine)?;
        Ok(replay)
    }

    /// Replay one archived write into a restoring engine at a fresh
    /// position. Each entry advances the restored expiry clock exactly
    /// as live members did, and patches commit at the advanced clock
    /// in the same transaction, so duplicate verdicts match theirs.
    /// Deterministic rejections replay as rejections, the same outcome
    /// live members reached. Every entry records the group head at its
    /// `index` in the archived history, so a batch that went stale live
    /// goes stale here too and one that applied applies. Incarnation
    /// entries only advance the clock and the head: the restored engine
    /// starts a history of its own.
    fn replay_entry(
        build_runtime: &RuntimeBuilder,
        engine: &mut Engine,
        write: &ClusterWrite,
        position: u64,
        index: u64,
    ) -> Result<(), BackupError> {
        match write {
            ClusterWrite::Patch {
                patch, proposed_ms, ..
            } => {
                let checked = engine
                    .check_patch(patch, position)
                    .map_err(BackupError::engine)?;
                let txn = engine.store().begin_write().map_err(BackupError::engine)?;
                // Receipts record where the outcome committed and its
                // caller was answered: the archived index.
                txn.record_history_index(index);
                let clock = StateMachine::advance_expiry_clock(&txn, *proposed_ms)
                    .map_err(BackupError::engine)?;
                engine
                    .commit_patch(&txn, &checked, position, clock)
                    .map_err(BackupError::engine)?;
                Head::Entry(index)
                    .record(&txn)
                    .map_err(BackupError::engine)?;
                txn.commit().map_err(BackupError::engine)?;
            }
            ClusterWrite::MultiPatch {
                patches,
                proposed_ms,
            } => {
                let checked = engine
                    .check_multi_patch(patches, position)
                    .map_err(BackupError::engine)?;
                let txn = engine.store().begin_write().map_err(BackupError::engine)?;
                txn.record_history_index(index);
                let clock = StateMachine::advance_expiry_clock(&txn, *proposed_ms)
                    .map_err(BackupError::engine)?;
                engine
                    .commit_multi(&txn, &checked, position, clock)
                    .map_err(BackupError::engine)?;
                Head::Entry(index)
                    .record(&txn)
                    .map_err(BackupError::engine)?;
                txn.commit().map_err(BackupError::engine)?;
            }
            ClusterWrite::Deploy {
                db,
                module,
                wasm,
                manifest,
                identity,
                bindings,
                proposed_ms,
            } => {
                let value = parse_value(manifest).map_err(BackupError::engine)?;
                let read = engine.store().begin_read().map_err(BackupError::engine)?;
                let active = read
                    .meta(db, &code_identity_key(module))
                    .map_err(BackupError::engine)?;
                // As members apply it: a database that does not serve
                // here, or that a prepared transaction holds, takes no
                // deployment, nor does a module whose data, or a queue it
                // declares, is being purged.
                let refused = flower_engine::txn::unwritable(&read, db)
                    .map_err(BackupError::engine)?
                    .is_some()
                    || flower_engine::purge::activation_refusal(&read, db, module, &value)
                        .map_err(BackupError::engine)?
                        .is_some();
                drop(read);
                if refused {
                    advance_clock(engine, *proposed_ms, index)?;
                    return Ok(());
                }
                if !matches!(active, Some(bytes) if bytes.as_slice() == identity.as_slice()) {
                    let runtime =
                        build_runtime(db, module, wasm, &value).map_err(BackupError::Engine)?;
                    let activation = Activation {
                        identity: *identity,
                        bindings: bindings.clone(),
                    };
                    engine
                        .apply_activation(db, module, wasm, &value, &activation, runtime)
                        .map_err(BackupError::engine)?;
                }
                // As live apply does: a database first deployed after
                // the base joins the replicated set, so the restored
                // node reloads it and later backups include it.
                engine.set_replicated(db);
                StateMachine::register_database(engine.store(), db).map_err(BackupError::engine)?;
                advance_clock(engine, *proposed_ms, index)?;
            }
            // As members apply it: the removal, or its refusal, commits
            // with the clock and the head; an applied one registers its
            // database for later backups and its purge.
            ClusterWrite::Remove {
                db,
                module,
                purge,
                proposed_ms,
            } => {
                let staged = engine.store().begin_staged().map_err(BackupError::engine)?;
                StateMachine::advance_expiry_clock(staged.write(), *proposed_ms)
                    .map_err(BackupError::engine)?;
                Head::Entry(index)
                    .record(staged.write())
                    .map_err(BackupError::engine)?;
                let applied = engine
                    .commit_removal(staged, db, module, *purge)
                    .map_err(BackupError::engine)?;
                if applied.is_ok() {
                    StateMachine::register_database(engine.store(), db)
                        .map_err(BackupError::engine)?;
                }
            }
            ClusterWrite::Incarnation { proposed_ms, .. } => {
                advance_clock(engine, *proposed_ms, index)?;
            }
            ClusterWrite::Batch {
                items,
                follows,
                writer,
                seq,
                proposed_ms,
            } => {
                let staged = engine.store().begin_staged().map_err(BackupError::engine)?;
                staged.write().record_history_index(index);
                let chain = BatchChain {
                    follows: *follows,
                    writer: *writer,
                    seq: *seq,
                    index,
                };
                let applied = StateMachine::apply_batch_in(
                    engine,
                    &staged,
                    items,
                    chain,
                    position,
                    *proposed_ms,
                )
                .map_err(BackupError::engine)?;
                staged.commit().map_err(BackupError::engine)?;
                // As live apply does: a database a placement change
                // dropped leaves the engine's memory too.
                if let Some(applied) = &applied {
                    for db in StateMachine::finish_items(engine, applied) {
                        reload_modules(build_runtime, engine, &db)?;
                    }
                }
            }
            // As live apply does: the catalog row changes, or a
            // rejected change records nothing; either way the entry is
            // the head.
            ClusterWrite::Keys {
                db,
                change,
                proposed_ms,
            } => {
                // As members apply it: the database's derived values that
                // resolved a key drop with the change.
                let _key_change = engine.begin_key_change();
                let txn = engine.store().begin_write().map_err(BackupError::engine)?;
                Head::Entry(index)
                    .record(&txn)
                    .map_err(BackupError::engine)?;
                StateMachine::advance_expiry_clock(&txn, *proposed_ms)
                    .map_err(BackupError::engine)?;
                engine
                    .drop_key_readers(&txn, db)
                    .map_err(BackupError::engine)?;
                let _ = StateMachine::apply_key_change(&txn, db, change)
                    .map_err(BackupError::engine)?;
                txn.commit().map_err(BackupError::engine)?;
            }
        }
        Ok(())
    }

    /// Enforce retention by time, as v1: keep every point of the last
    /// `window` restorable, however many bases that takes, and let the
    /// rest go. In each history the newest base at or before the horizon
    /// (`window` ago) stays, or the oldest when every base is newer, with
    /// all after it; older bases go, as do the segments at or below the
    /// oldest kept cut, which no kept base replays. Of the bases kept,
    /// those older than `spacing` thin out to one per `spacing`, and the
    /// newest stays: a point a dropped one covered restores from an
    /// earlier base through the segments after it, which all stay. A
    /// history that ended before the horizon (its newest base and segment
    /// are older), other than the newest base's, goes whole; a history
    /// with no base keeps its segments until they are older than the
    /// horizon. Each manifest goes before its objects, so a crash mid-run
    /// never leaves a listed manifest naming deleted views; stranded
    /// objects fall to collection.
    pub async fn enforce_retention(
        &self,
        window: Duration,
        spacing: Duration,
    ) -> Result<RetentionSummary, BackupError> {
        let now = now_ms();
        let horizon = now.saturating_sub(as_ms(window));
        let spacing = as_ms(spacing);
        // Segments list before manifests: a base published after the
        // manifest listing cuts at or past every segment archived
        // before it, so only listed bases can need a listed segment.
        let segments: Vec<(SegmentRange, String, u64)> = self
            .list_meta(SEGMENT_DIR)
            .await?
            .into_iter()
            .filter_map(|meta| {
                let name = meta.location.to_string();
                let range = segment_range_from_name(&name)?;
                Some((range, name, modified_ms(&meta)))
            })
            .collect();
        let mut manifests = self.list_manifests().await?;
        manifests.sort_by(|(_, a), (_, b)| by_recency(a, b));
        let newest = manifests.last().map(|(_, manifest)| manifest.incarnation);
        // When each history last wrote, and its bases oldest first.
        let mut ended: HashMap<u64, u64> = HashMap::new();
        let mut histories: HashMap<u64, Vec<usize>> = HashMap::new();
        for (position, (_, manifest)) in manifests.iter().enumerate() {
            let end = ended.entry(manifest.incarnation).or_default();
            *end = (*end).max(manifest.created_ms);
            histories
                .entry(manifest.incarnation)
                .or_default()
                .push(position);
        }
        for (range, _, modified) in &segments {
            let end = ended.entry(range.incarnation).or_default();
            *end = (*end).max(*modified);
        }
        let mut dropped = vec![false; manifests.len()];
        for (incarnation, bases) in &histories {
            if Some(*incarnation) != newest && ended[incarnation] < horizon {
                for position in bases {
                    dropped[*position] = true;
                }
                continue;
            }
            let at = |rank: usize| manifests[bases[rank]].1.created_ms;
            let floor = (0..bases.len())
                .rev()
                .find(|rank| at(*rank) <= horizon)
                .unwrap_or(0);
            let mut kept_at = at(floor);
            for rank in 0..bases.len() {
                if rank < floor {
                    dropped[bases[rank]] = true;
                } else if rank > floor && rank + 1 < bases.len() {
                    if at(rank) >= now.saturating_sub(spacing)
                        || at(rank) >= kept_at.saturating_add(spacing)
                    {
                        kept_at = at(rank);
                    } else {
                        dropped[bases[rank]] = true;
                    }
                }
            }
        }
        // The oldest kept cut of each history.
        let mut floors: HashMap<u64, u64> = HashMap::new();
        for ((_, manifest), dropped) in manifests.iter().zip(&dropped) {
            if !dropped {
                let floor = floors
                    .entry(manifest.incarnation)
                    .or_insert(manifest.covered_index);
                *floor = (*floor).min(manifest.covered_index);
            }
        }
        let mut deleted_manifests = 0usize;
        let mut deleted_objects = 0usize;
        for ((name, manifest), dropped) in manifests.iter().zip(&dropped) {
            if !dropped {
                continue;
            }
            self.delete(name).await?;
            deleted_manifests += 1;
            for object in &manifest.objects {
                self.delete(&object.name).await?;
                deleted_objects += 1;
            }
        }
        let mut deleted_segments = 0usize;
        for (range, name, modified) in &segments {
            let needed = match floors.get(&range.incarnation) {
                Some(floor) => range.last > *floor,
                None => *modified >= horizon,
            };
            if !needed {
                self.delete(name).await?;
                deleted_segments += 1;
            }
        }
        Ok(RetentionSummary {
            kept: manifests.len() - deleted_manifests,
            deleted_manifests,
            deleted_objects,
            deleted_segments,
        })
    }

    /// Retire key versions no stored manifest, segment or archive
    /// position references: scans every manifest (superseded leftovers
    /// included, which must stay readable until collected) plus
    /// segment, archive-position and manifest headers through 8-byte
    /// range reads. The current version never retires.
    pub async fn retire_versions(&self) -> Result<Vec<u32>, BackupError> {
        let mut referenced = HashSet::new();
        for (name, manifest) in self.list_all_manifests().await? {
            for object in &manifest.objects {
                referenced.insert(object.key_version);
            }
            referenced.insert(self.peek_object_version(&name).await?);
        }
        for name in self.list(SEGMENT_DIR).await? {
            referenced.insert(self.peek_object_version(&name).await?);
        }
        for name in self.list(ARCHIVE_POS_DIR).await? {
            referenced.insert(self.peek_object_version(&name).await?);
        }
        let current = self.key.current().map_err(BackupError::Crypto)?;
        let mut retired = Vec::new();
        for version in self.key.versions().map_err(BackupError::Crypto)? {
            if version != current
                && !referenced.contains(&version)
                && self.key.retire(version).map_err(BackupError::Crypto)?
            {
                retired.push(version);
            }
        }
        Ok(retired)
    }

    /// Copy up to `budget` objects forward under the current
    /// version: bounded resumable operator work. View objects copy
    /// to fresh names under a manifest superseding the old one, which
    /// keeps its creation time and backup ID and so its place in the
    /// recency order; segments and archive positions reseal in place
    /// under their own names. Each manifest publishes before anything
    /// it replaces goes: new views, then the superseding manifest, and
    /// only then the old views and manifest. An error at any step
    /// leaves every published name restorable; reruns and
    /// [`Driver::gc_orphans`] converge.
    pub async fn reencrypt(&self, budget: usize) -> Result<ReencryptSummary, BackupError> {
        let current = self.key.current().map_err(BackupError::Crypto)?;
        let mut moved = 0usize;
        let mut manifests_done = 0usize;
        let mut segments_done = 0usize;
        for (name, manifest) in self.list_manifests().await? {
            if moved >= budget {
                break;
            }
            let stale: Vec<usize> = manifest
                .objects
                .iter()
                .enumerate()
                .filter(|(_, object)| object.key_version != current)
                .map(|(index, _)| index)
                .take(budget - moved)
                .collect();
            if stale.is_empty() {
                continue;
            }
            let mut next = manifest.clone();
            let mut replaced = Vec::with_capacity(stale.len());
            for position in stale {
                let object = &manifest.objects[position];
                let new_name = object_name("obj");
                let uploaded = self.reseal(&object.name, &new_name).await?;
                next.objects[position].name = new_name;
                next.objects[position].key_version = uploaded.version;
                next.objects[position].ciphertext_len = uploaded.stored;
                replaced.push(object.name.clone());
                moved += 1;
            }
            next.supersedes = Some(name.clone());
            let next_name = object_name("manifest");
            self.put_sealed(&next_name, &encode_json(&next)?).await?;
            for object in &replaced {
                self.delete(object).await?;
            }
            self.delete(&name).await?;
            manifests_done += 1;
        }
        if moved < budget {
            for seg_name in self.list(SEGMENT_DIR).await? {
                if moved >= budget {
                    break;
                }
                if self.peek_object_version(&seg_name).await? == current {
                    continue;
                }
                // Resealing in place is one atomic overwrite.
                self.reseal(&seg_name, &seg_name).await?;
                moved += 1;
                segments_done += 1;
            }
        }
        // Archive positions are tiny and outside the budget.
        for name in self.list(ARCHIVE_POS_DIR).await? {
            if self.peek_object_version(&name).await? == current {
                continue;
            }
            self.reseal(&name, &name).await?;
        }
        Ok(ReencryptSummary {
            moved_objects: moved,
            manifests: manifests_done,
            segments: segments_done,
        })
    }

    /// Collect leftovers with the default [`GC_GRACE`]: see
    /// [`Driver::gc_orphans_older_than`].
    pub async fn gc_orphans(&self) -> Result<GcSummary, BackupError> {
        self.gc_orphans_older_than(GC_GRACE).await
    }

    /// Collect leftovers from interrupted re-encryption, retention,
    /// archiving and backups: superseded manifests with the views
    /// only they reference, unreferenced view objects older than
    /// `grace`, and segments whose range another segment of the same
    /// history contains. Fresh unreferenced views are spared because a
    /// backup or re-encryption uploads views before the manifest
    /// naming them; a zero grace collects regardless of age and is
    /// only safe while nothing else writes.
    pub async fn gc_orphans_older_than(&self, grace: Duration) -> Result<GcSummary, BackupError> {
        let all = self.list_all_manifests().await?;
        let superseded: HashSet<&str> = all
            .iter()
            .filter_map(|(_, manifest)| manifest.supersedes.as_deref())
            .collect();
        let live_objects: HashSet<&str> = all
            .iter()
            .filter(|(name, _)| !superseded.contains(name.as_str()))
            .flat_map(|(_, manifest)| manifest.objects.iter().map(|object| object.name.as_str()))
            .collect();
        let mut deleted = 0usize;
        for (name, manifest) in &all {
            if !superseded.contains(name.as_str()) {
                continue;
            }
            self.delete(name).await?;
            deleted += 1;
            for object in &manifest.objects {
                if !live_objects.contains(object.name.as_str()) {
                    self.delete(&object.name).await?;
                    deleted += 1;
                }
            }
        }
        let now = now_ms();
        let grace_ms = as_ms(grace);
        for meta in self.list_meta(&format!("{PREFIX}/obj")).await? {
            let name = meta.location.to_string();
            if live_objects.contains(name.as_str()) {
                continue;
            }
            let modified = modified_ms(&meta);
            if !grace.is_zero() && now.saturating_sub(modified) < grace_ms {
                continue;
            }
            self.delete(&name).await?;
            deleted += 1;
        }
        // A segment inside another's range of the same history is
        // redundant: committed entries never change, so the wider one
        // holds the same data.
        let segments: Vec<(SegmentRange, String)> = self
            .list(SEGMENT_DIR)
            .await?
            .into_iter()
            .filter_map(|name| segment_range_from_name(&name).map(|range| (range, name)))
            .collect();
        for (range, name) in &segments {
            let contained = segments.iter().any(|(other, _)| {
                other.incarnation == range.incarnation
                    && other.first <= range.first
                    && other.last >= range.last
                    && other != range
            });
            if contained {
                self.delete(name).await?;
                deleted += 1;
            }
        }
        Ok(GcSummary { deleted })
    }

    /// Run continuous backup until shutdown flips true or its sender
    /// drops. It first checks the store holds this format
    /// ([`Driver::check_format`]) and holds this member's log purges
    /// ([`LogStore::hold_purges`]); then, every archive interval, the
    /// leader archives what it applied, takes a base when its history has
    /// none newer than the base interval or the archive since the newest
    /// outgrew both the base bytes and that base, and lets its log go of
    /// what the archive holds. After each base and every retention
    /// interval it retains, collects and retires; their failure is
    /// reported and never makes a base due, and a failed base waits the
    /// base retry before the next. A member that does not lead only lets
    /// its log go of what its history's shared archive position records,
    /// so whichever member leads next still holds the rest and continues
    /// the same archive; one whose log lost entries the archive lacks
    /// takes a base first. A failed tick is logged and retried at the
    /// next: an unreachable store keeps the log held until it answers.
    /// Only the hold failing to start, or a store in another format, end
    /// the loop early; refusing a store, it lets go of the log it held,
    /// which nothing could ever archive.
    pub async fn run_continuous(
        &self,
        settings: &ContinuousSettings,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Result<(), BackupError> {
        let logs = self
            .logs
            .as_ref()
            .ok_or_else(|| BackupError::Refused("offline driver cannot back up".to_string()))?;
        logs.hold_purges_capped(settings.max_held, settings.max_held_bytes)
            .await
            .map_err(|error| BackupError::Store(error.to_string()))?;
        self.update_status(|status| status.role = "starting");
        let ended = self.continuous_loop(settings, &mut shutdown).await;
        self.update_status(|status| status.role = "stopped");
        if ended.is_err() {
            logs.drop_purge_hold()
                .await
                .map_err(|error| BackupError::Store(error.to_string()))?;
        }
        ended
    }

    /// The ticks of [`Driver::run_continuous`], until shutdown or a
    /// store in another format.
    async fn continuous_loop(
        &self,
        settings: &ContinuousSettings,
        shutdown: &mut tokio::sync::watch::Receiver<bool>,
    ) -> Result<(), BackupError> {
        let mut plan = None;
        let mut failing: Option<String> = None;
        // The store's format, checked until a check gets an answer.
        let mut checked = false;
        loop {
            if *shutdown.borrow() {
                break;
            }
            let role = if (self.leads)() { "leader" } else { "follower" };
            // A stop cancels a running tick, a base or an archive stalled
            // on its store included: the final archive below ships what
            // it applied, within its own bound.
            let outcome = tokio::select! {
                outcome = async {
                    self.check_format_once(&mut checked).await?;
                    self.continuous_tick(settings, &mut plan).await
                } => outcome,
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break;
                    }
                    continue;
                }
            };
            if let Err(error @ BackupError::Foreign(_)) = &outcome {
                tracing::error!(
                    target: "flower::backup",
                    error = %error,
                    "continuous backup refuses this store and lets go of the log it held"
                );
                self.update_status(|status| {
                    status.errors += 1;
                    status.failing = true;
                    status.last_error = Some((error.to_string(), now_ms()));
                });
                return Err(error.clone());
            }
            self.update_status(|status| {
                status.role = role;
                status.ticks += 1;
                status.last_tick_ms = Some(now_ms());
                status.failing = outcome.is_err();
                if let Err(error) = &outcome {
                    status.errors += 1;
                    status.last_error = Some((error.to_string(), now_ms()));
                }
            });
            match outcome {
                Ok(()) => {
                    if failing.take().is_some() {
                        tracing::info!(target: "flower::backup", "continuous backup recovered");
                    }
                }
                Err(error) => {
                    let message = error.to_string();
                    if failing.as_deref() != Some(message.as_str()) {
                        tracing::warn!(
                            target: "flower::backup",
                            error = %message,
                            "continuous backup failed; retrying"
                        );
                    }
                    failing = Some(message);
                }
            }
            tokio::select! {
                _ = tokio::time::sleep(settings.archive_interval) => {}
                changed = shutdown.changed() => {
                    // A dropped sender can never signal again: stop
                    // instead of looping without a sleep.
                    if changed.is_err() || *shutdown.borrow() {
                        break;
                    }
                }
            }
        }
        let within = settings.final_archive_within;
        if tokio::time::timeout(within, self.final_archive())
            .await
            .is_err()
        {
            tracing::warn!(
                target: "flower::backup",
                timeout_ms = within.as_millis() as u64,
                "the final archive before stopping did not finish in time"
            );
        }
        Ok(())
    }

    /// Check the store's format until one check gets an answer: see
    /// [`Driver::check_format`].
    async fn check_format_once(&self, checked: &mut bool) -> Result<(), BackupError> {
        if !*checked {
            self.check_format().await?;
            *checked = true;
        }
        Ok(())
    }

    /// Refuse a store holding objects in a format this version cannot
    /// read, such as a backup directory written before backup objects
    /// became sealed streams: continuous backup would fail on it every
    /// tick, never archiving, while every member's log held entries for
    /// it until the hold's cap dropped them. Every object begins with the
    /// stream magic, so the first one listed tells; an empty store passes.
    pub async fn check_format(&self) -> Result<(), BackupError> {
        let first = self
            .store
            .list(Some(&ObjPath::from(PREFIX)))
            .try_next()
            .await
            .map_err(store_error)?;
        let Some(first) = first else {
            return Ok(());
        };
        let magic = aead::STREAM_MAGIC;
        let head = if first.size < magic.len() as u64 {
            Vec::new()
        } else {
            self.store
                .get_range(&first.location, 0..magic.len() as u64)
                .await
                .map_err(store_error)?
                .to_vec()
        };
        if head == magic {
            return Ok(());
        }
        Err(BackupError::Foreign(format!(
            "{} begins {:?}, not {:?}: the store holds backups in another format, which this \
             version cannot read; back up to an empty location, or move them away",
            first.location,
            String::from_utf8_lossy(&head),
            String::from_utf8_lossy(magic),
        )))
    }

    /// What continuous backup last did on this member: `idle` before
    /// [`Driver::run_continuous`] starts.
    pub fn continuous_status(&self) -> ContinuousStatus {
        let mut status = self
            .status
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if status.role.is_empty() {
            status.role = "idle";
        }
        status
    }

    /// Change the continuous status.
    fn update_status(&self, change: impl FnOnce(&mut ContinuousStatus)) {
        change(
            &mut self
                .status
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
    }

    /// One continuous-backup tick; `plan` is when the leader takes its
    /// next base and retains, unknown until it leads. The leader archives
    /// first and takes a base after, when one is due by age or bytes, so
    /// a base covers what was just archived; the archive a tick's base
    /// covers never counts toward the next. A failed base waits the base
    /// retry before the next try, as v1, so a store refusing part of one
    /// never uploads another every tick. Retention, collection and
    /// retirement run last, after a base or once the retention interval
    /// passed, and their failure fails the tick without touching the plan.
    async fn continuous_tick(
        &self,
        settings: &ContinuousSettings,
        plan: &mut Option<BasePlan>,
    ) -> Result<(), BackupError> {
        if !(self.leads)() {
            *plan = None;
            let released = self.release_archived().await?;
            self.update_status(|status| status.archived_through = Some(released));
            return Ok(());
        }
        let incarnation = self.incarnation().await?;
        if incarnation == 0 {
            // Nothing archives, and no base can be replayed from, until
            // the group establishes its history.
            return Ok(());
        }
        let mut current = match *plan {
            Some(current) => current,
            None => self.base_plan(incarnation, settings.base_interval).await?,
        };
        *plan = Some(current);
        let mut based = false;
        // Archive everything applied so far, so a write rate past one
        // batch per tick never falls behind.
        let mut archived = None;
        for _ in 0..ARCHIVE_CALLS_PER_TICK {
            let summary = match self.archive_now().await {
                Ok(summary) => summary,
                // This member's log lost entries the archive lacks: a
                // base of its own covers them, and archiving goes on
                // past it.
                Err(BackupError::Refused(_)) if !based && current.may_base() => {
                    based = true;
                    current = self.base_or_retry(settings, plan).await?;
                    continue;
                }
                Err(error) => return Err(error),
            };
            archived = Some(summary.to);
            self.note_archived(&summary);
            if !based {
                current.archived += summary.bytes;
                *plan = Some(current);
            }
            if summary.to == summary.from {
                break;
            }
        }
        if !based && current.due(settings) {
            current = self.base_or_retry(settings, plan).await?;
        }
        if let (Some(through), Some(logs)) = (archived, self.logs.as_ref()) {
            logs.release_purges(through)
                .await
                .map_err(|error| BackupError::Store(error.to_string()))?;
        }
        if tokio::time::Instant::now() >= current.maintain {
            current.maintain = tokio::time::Instant::now() + settings.retention_interval;
            *plan = Some(current);
            self.maintain(settings).await?;
        }
        Ok(())
    }

    /// Take a base and plan the next, or, when it fails, hold the next
    /// try off for the base retry.
    async fn base_or_retry(
        &self,
        settings: &ContinuousSettings,
        plan: &mut Option<BasePlan>,
    ) -> Result<BasePlan, BackupError> {
        match self.take_base(settings).await {
            Ok(next) => {
                *plan = Some(next);
                Ok(next)
            }
            Err(error) => {
                if let Some(current) = plan.as_mut() {
                    current.retry = Some(tokio::time::Instant::now() + settings.base_retry);
                }
                Err(error)
            }
        }
    }

    /// Retain by time, collect orphans and retire key versions: after
    /// each base and every retention interval, apart from the base so
    /// that a store refusing deletes never makes another due.
    async fn maintain(&self, settings: &ContinuousSettings) -> Result<(), BackupError> {
        self.enforce_retention(settings.retention, settings.base_interval)
            .await?;
        self.gc_orphans().await?;
        self.retire_versions().await?;
        Ok(())
    }

    /// What a stopping leader still owes the archive: everything it
    /// applied since its last tick, as v1 shipped at shutdown, so a
    /// planned stop leaves nothing for the next start to send. No base:
    /// a log that needs one leaves it to the next leader's first tick.
    /// [`ContinuousSettings::final_archive_within`] bounds it.
    async fn final_archive(&self) {
        if !(self.leads)() || self.logs.is_none() {
            return;
        }
        let mut archived = None;
        for _ in 0..ARCHIVE_CALLS_PER_TICK {
            match self.archive_now().await {
                Ok(summary) => {
                    archived = Some(summary.to);
                    self.note_archived(&summary);
                    if summary.to == summary.from {
                        break;
                    }
                }
                Err(error) => {
                    tracing::warn!(
                        target: "flower::backup",
                        error = %error,
                        "the final archive before stopping failed"
                    );
                    break;
                }
            }
        }
        if let (Some(through), Some(logs)) = (archived, self.logs.as_ref())
            && let Err(error) = logs.release_purges(through).await
        {
            tracing::warn!(
                target: "flower::backup",
                error = %error,
                "releasing the log after the final archive failed"
            );
        }
    }

    /// Count one archive run in the continuous status.
    fn note_archived(&self, summary: &ArchiveSummary) {
        self.update_status(|status| {
            status.archived_through = Some(summary.to);
            status.segments_archived += u64::from(summary.segments);
            status.entries_archived += summary.entries as u64;
        });
    }

    /// Take a base and plan the next; retention, collection and
    /// retirement follow it within the tick.
    async fn take_base(&self, settings: &ContinuousSettings) -> Result<BasePlan, BackupError> {
        let base = self.backup_now().await?;
        self.update_status(|status| {
            status.bases_taken += 1;
            status.newest_base = Some(BaseStatus {
                backup_id: base.backup_id.clone(),
                covered_index: base.covered_index,
                at_ms: now_ms(),
                ciphertext_bytes: base.ciphertext_bytes,
            });
        });
        let now = tokio::time::Instant::now();
        Ok(BasePlan {
            due: now + settings.base_interval,
            archived: 0,
            base: base.ciphertext_bytes,
            retry: None,
            maintain: now,
        })
    }

    /// When a new leader of `incarnation` takes its first base, from
    /// the history's newest: once it is `interval` old, or once the
    /// sealed segments chained after its cut outgrow the base bytes and
    /// it; at once without one. It retains at its first tick.
    async fn base_plan(
        &self,
        incarnation: u64,
        interval: Duration,
    ) -> Result<BasePlan, BackupError> {
        let history = self.list_manifests().await?.into_iter();
        let newest = newest(history.filter(|(_, manifest)| manifest.incarnation == incarnation));
        let now = tokio::time::Instant::now();
        let Some((_, manifest)) = newest else {
            return Ok(BasePlan {
                due: now,
                archived: 0,
                base: 0,
                retry: None,
                maintain: now,
            });
        };
        let age = Duration::from_millis(now_ms().saturating_sub(manifest.created_ms));
        let sizes: HashMap<String, u64> = self
            .list_meta(&segment_dir(incarnation))
            .await?
            .into_iter()
            .map(|meta| (meta.location.to_string(), meta.size))
            .collect();
        let spans = spans_of(incarnation, sizes.keys().cloned());
        let archived = plan_chain(&spans, manifest.covered_index)
            .spans
            .iter()
            .map(|span| sizes[&span.name])
            .sum();
        Ok(BasePlan {
            due: now + interval.saturating_sub(age),
            archived,
            base: manifest
                .objects
                .iter()
                .map(|object| object.ciphertext_len)
                .sum(),
            retry: None,
            maintain: now,
        })
    }

    /// Let this member's log go of entries its history's shared archive
    /// position records as archived; the log's background purge deletes
    /// those already purged. Returns the position released through,
    /// zero while the group has no history yet.
    pub async fn release_archived(&self) -> Result<u64, BackupError> {
        let logs = self
            .logs
            .as_ref()
            .ok_or_else(|| BackupError::Refused("offline driver holds no log".to_string()))?;
        let incarnation = self.incarnation().await?;
        if incarnation == 0 {
            return Ok(0);
        }
        let position = self.archive_position(incarnation).await?;
        logs.release_purges(position)
            .await
            .map_err(|error| BackupError::Store(error.to_string()))?;
        Ok(position)
    }

    /// The live engine's applied group incarnation.
    async fn incarnation(&self) -> Result<u64, BackupError> {
        let engine = self
            .engine
            .as_ref()
            .ok_or_else(|| BackupError::Refused("offline driver has no engine".to_string()))?;
        let engine = engine.lock().await;
        StateMachine::incarnation(&engine).map_err(BackupError::engine)
    }

    /// Install spooled views into a group file offline and return the
    /// engine: apply each snapshot, read back one at a time, and
    /// register its database, start a new
    /// history (applied positions reset for a fresh ordering, no
    /// incarnation until the new leader proposes one) with the expiry
    /// clock seeded from the backup's, then reload runtimes and mark
    /// replicated. Any existing group file is replaced, and a sibling
    /// Raft log is removed: the restored node must re-bootstrap
    /// membership before serving.
    ///
    /// A restore that replays the archive (`replay`: the backed-up
    /// history's incarnation) keeps that history until replay ends it
    /// ([`Driver::replay_segments`]): its archived entries apply as its
    /// members applied them, scoped identities and sessions included,
    /// which a new history would refuse.
    fn install_views(
        &self,
        group_path: &Path,
        views: Vec<SpooledView>,
        expiry_clock: u64,
        head: Option<Head>,
        replay: Option<u64>,
    ) -> Result<Engine, BackupError> {
        if group_path.exists() {
            std::fs::remove_file(group_path).map_err(|error| BackupError::Io(error.to_string()))?;
        }
        if let Some(dir) = group_path.parent() {
            let raft = dir.join("raft.redb");
            if raft.exists() {
                std::fs::remove_file(&raft).map_err(|error| BackupError::Io(error.to_string()))?;
            }
        }
        let store = GroupStore::create(group_path).map_err(BackupError::engine)?;
        // The member that opens it checks what it was written under.
        store
            .claim_contract(flower_cluster::compat::storage_contract())
            .map_err(BackupError::engine)?
            .map_err(|_| BackupError::Refused("a new group file holds rows".to_string()))?;
        if let Some(incarnation) = replay {
            // Registering labels each database with it.
            let txn = store.begin_write().map_err(BackupError::engine)?;
            StateMachine::adopt_history(&txn, incarnation).map_err(BackupError::engine)?;
            txn.commit().map_err(BackupError::engine)?;
        }
        let mut dbs = Vec::with_capacity(views.len());
        for SpooledView { meta, mut file } in views {
            let mut payload = Vec::new();
            file.read_to_end(&mut payload).map_err(io_error)?;
            apply_snapshot(&store, &meta, &payload).map_err(BackupError::engine)?;
            StateMachine::register_database(&store, &meta.db).map_err(BackupError::engine)?;
            dbs.push(meta.db);
        }
        // The fresh `$cluster` holds no incarnation (it never backs
        // up) and no clock: positions and labels reset (a replayed
        // history's labels at the end of replay), and receipts expire
        // from the backup's clock rather than from zero. The head is
        // the cut's, for replay; without replay, a new history's.
        let txn = store.begin_write().map_err(BackupError::engine)?;
        match replay {
            Some(_) => StateMachine::restart_positions(&txn),
            None => StateMachine::restart_history(&txn),
        }
        .map_err(BackupError::engine)?;
        StateMachine::advance_expiry_clock(&txn, expiry_clock).map_err(BackupError::engine)?;
        head.unwrap_or(Head::Entry(0))
            .record(&txn)
            .map_err(BackupError::engine)?;
        txn.commit().map_err(BackupError::engine)?;
        let mut engine = Engine::new(store);
        for db in &dbs {
            engine.set_replicated(db);
            self.reload_db(&mut engine, db)?;
        }
        Ok(engine)
    }

    /// Reload every deployed module's runtime from installed code.
    fn reload_db(&self, engine: &mut Engine, db: &[u8]) -> Result<(), BackupError> {
        reload_modules(&self.build_runtime, engine, db)
    }
}

/// Reload every deployed module of `db` from its installed code, with
/// `build_runtime`.
fn reload_modules(
    build_runtime: &RuntimeBuilder,
    engine: &mut Engine,
    db: &[u8],
) -> Result<(), BackupError> {
    {
        let read = engine.store().begin_read().map_err(BackupError::engine)?;
        let rows = read
            .scan_meta_prefix(db, CODE_IDENTITY_PREFIX)
            .map_err(BackupError::engine)?;
        let mut modules = Vec::new();
        for (name, identity) in &rows {
            let module = String::from_utf8(name[CODE_IDENTITY_PREFIX.len()..].to_vec())
                .map_err(|_| BackupError::Format("bad module name".to_string()))?;
            let hash: [u8; 32] = identity.as_slice().try_into().map_err(|_| {
                BackupError::Format(format!(
                    "code identity for `{module}` has {} bytes, want 32",
                    identity.len()
                ))
            })?;
            let Some((wasm, manifest)) = read.get_code(db, &hash).map_err(BackupError::engine)?
            else {
                return Err(BackupError::Format(format!("code missing for `{module}`")));
            };
            modules.push((module, wasm, manifest));
        }
        drop(read);
        for (module, wasm, manifest) in &modules {
            let value = parse_value(manifest).map_err(BackupError::engine)?;
            let runtime = (build_runtime)(db, module, wasm, &value).map_err(BackupError::Engine)?;
            engine
                .reload_module(db, module, runtime)
                .map_err(BackupError::engine)?;
        }
        Ok(())
    }
}

impl Driver {
    /// Load a contiguous chain of one history's segments covering
    /// every index after `covered`, in order. Planning runs on names
    /// alone: from the next needed index, take the segment reaching
    /// furthest among those starting at or before it. Overlap left by
    /// interrupted archive runs is harmless (replay skips indexes
    /// already replayed); an index no segment covers, with archived
    /// ranges beyond it, refuses as a gap.
    async fn load_segments_after(
        &self,
        incarnation: u64,
        covered: u64,
    ) -> Result<Vec<LogSegment>, BackupError> {
        let spans = self.segment_spans(incarnation).await?;
        let chain = plan_chain(&spans, covered);
        if chain.stranded {
            return Err(BackupError::Refused(format!(
                "archive gap: no segment covers entry {}",
                chain.next
            )));
        }
        self.download_segments(chain.spans).await
    }

    /// One history's segments by name, sorted by range.
    async fn segment_spans(&self, incarnation: u64) -> Result<Vec<Span>, BackupError> {
        Ok(spans_of(
            incarnation,
            self.list(&segment_dir(incarnation)).await?,
        ))
    }

    /// Download, open and check segments in order.
    async fn download_segments<'a>(
        &self,
        spans: impl IntoIterator<Item = &'a Span>,
    ) -> Result<Vec<LogSegment>, BackupError> {
        let mut segments = Vec::new();
        for span in spans {
            let name = &span.name;
            let bytes = self.get_object(name, true).await?;
            let segment: LogSegment = serde_json::from_slice(&bytes)
                .map_err(|error| BackupError::Format(error.to_string()))?;
            if segment.version != SEGMENT_VERSION {
                return Err(BackupError::Format(format!(
                    "segment {name} version {} is not {SEGMENT_VERSION}",
                    segment.version
                )));
            }
            if (segment.first_index, segment.last_index) != (span.first, span.last) {
                return Err(BackupError::Format(format!(
                    "segment {name} holds {}..{}",
                    segment.first_index, segment.last_index
                )));
            }
            segments.push(segment);
        }
        Ok(segments)
    }

    /// List live manifests, excluding superseded garbage.
    async fn list_manifests(&self) -> Result<Vec<(String, BackupManifest)>, BackupError> {
        let mut all = self.list_all_manifests().await?;
        let superseded: HashSet<String> = all
            .iter()
            .filter_map(|(_, manifest)| manifest.supersedes.clone())
            .collect();
        all.retain(|(name, _)| !superseded.contains(name));
        Ok(all)
    }

    /// List every stored manifest, superseded ones included.
    async fn list_all_manifests(&self) -> Result<Vec<(String, BackupManifest)>, BackupError> {
        let mut out = Vec::new();
        for name in self.list(&format!("{PREFIX}/manifest")).await? {
            let manifest = self.open_manifest(&name).await?;
            out.push((name, manifest));
        }
        Ok(out)
    }

    /// Open and check one manifest by name.
    async fn open_manifest(&self, name: &str) -> Result<BackupManifest, BackupError> {
        let bytes = self.get_sealed(name).await?;
        let manifest: BackupManifest = serde_json::from_slice(&bytes)
            .map_err(|error| BackupError::Format(error.to_string()))?;
        if manifest.version != MANIFEST_VERSION {
            return Err(BackupError::Format(format!(
                "manifest {name} version {} is not {MANIFEST_VERSION}",
                manifest.version
            )));
        }
        Ok(manifest)
    }

    /// The latest backup, if any: the live manifest with the greatest
    /// `(created_ms, backup_id)`. It is derived from the listing, never
    /// stored, so no concurrent writer can move it backwards.
    async fn latest_manifest(&self) -> Result<Option<(String, BackupManifest)>, BackupError> {
        Ok(newest(self.list_manifests().await?))
    }

    /// Download, open, decompress and verify every view in a manifest,
    /// spooling each payload to an unnamed temporary file beside
    /// `group_path` as its frames arrive: all are checked before a
    /// restore touches its target, while memory holds a frame at a time.
    async fn download_views(
        &self,
        manifest: &BackupManifest,
        group_path: &Path,
    ) -> Result<Vec<SpooledView>, BackupError> {
        let dir = match group_path.parent() {
            Some(dir) if !dir.as_os_str().is_empty() => dir,
            _ => Path::new("."),
        };
        let mut views = Vec::with_capacity(manifest.objects.len());
        for object in &manifest.objects {
            let spool = Spool {
                file: BufWriter::new(tempfile::tempfile_in(dir).map_err(io_error)?),
                digest: Hasher::default(),
            };
            let mut decoder = zstd::stream::write::Decoder::new(spool).map_err(io_error)?;
            let mut download =
                Download::begin(self.store.as_ref(), &self.key, &object.name).await?;
            while let Some(bytes) = download.next().await? {
                decoder.write_all(&bytes).map_err(io_error)?;
            }
            decoder.flush().map_err(io_error)?;
            let Spool { file, digest } = decoder.into_inner();
            if digest.finish().as_slice() != object.plaintext_sha256.as_slice() {
                return Err(BackupError::Format(format!(
                    "integrity mismatch for {}",
                    object.name
                )));
            }
            let mut file = file
                .into_inner()
                .map_err(|error| io_error(error.into_error()))?;
            file.rewind().map_err(io_error)?;
            views.push(SpooledView {
                meta: SnapshotMeta {
                    db: object.db.clone(),
                    revision: object.revision,
                    applied_log: object.applied_log,
                    incarnation: object.incarnation,
                },
                file,
            });
        }
        Ok(views)
    }

    /// Read one history's archive position, zero when never archived.
    async fn archive_position(&self, incarnation: u64) -> Result<u64, BackupError> {
        match self.get_sealed(&archive_position_name(incarnation)).await {
            Err(BackupError::NotFound(_)) => Ok(0),
            Err(error) => Err(error),
            Ok(bytes) => bytes
                .as_slice()
                .try_into()
                .map(u64::from_be_bytes)
                .map_err(|_| BackupError::Format("bad archive pointer".to_string())),
        }
    }

    /// Peek an object's sealing version through an 8-byte range read.
    async fn peek_object_version(&self, name: &str) -> Result<u32, BackupError> {
        let bytes = self
            .store
            .get_range(&ObjPath::from(name), 0..8u64)
            .await
            .map_err(store_error)?;
        aead::peek_stream_version(&bytes).map_err(BackupError::Crypto)
    }

    /// Seal and upload an object, compressed first when `compress`, in
    /// parts once it outgrows one.
    async fn put_object(
        &self,
        name: &str,
        compress: bool,
        bytes: &[u8],
    ) -> Result<Uploaded, BackupError> {
        let mut upload = Upload::begin(
            self.store.as_ref(),
            &self.key,
            name,
            self.part_bytes,
            compress,
        )?;
        upload.write(bytes).await?;
        upload.finish().await
    }

    /// Seal and upload an object's plaintext.
    async fn put_sealed(&self, name: &str, plaintext: &[u8]) -> Result<Uploaded, BackupError> {
        self.put_object(name, false, plaintext).await
    }

    /// Download and open an object, decompressed when `compressed`.
    async fn get_object(&self, name: &str, compressed: bool) -> Result<Vec<u8>, BackupError> {
        Download::begin(self.store.as_ref(), &self.key, name)
            .await?
            .read_all(compressed)
            .await
    }

    /// Download and open one sealed object.
    async fn get_sealed(&self, name: &str) -> Result<Vec<u8>, BackupError> {
        self.get_object(name, false).await
    }

    /// Seal an object again under the current version as `to`, which
    /// may be its own name: it goes up as it opens, so neither copy is
    /// ever whole in memory. `to` changes only when the upload
    /// completes, after the last byte of `from` was read.
    async fn reseal(&self, from: &str, to: &str) -> Result<Uploaded, BackupError> {
        let mut download = Download::begin(self.store.as_ref(), &self.key, from).await?;
        let mut upload = Upload::begin(self.store.as_ref(), &self.key, to, self.part_bytes, false)?;
        loop {
            match download.next().await {
                Ok(Some(bytes)) => upload.write(&bytes).await?,
                Ok(None) => break,
                Err(error) => {
                    upload.abort().await;
                    return Err(error);
                }
            }
        }
        drop(download);
        upload.finish().await
    }

    /// Delete one object. Deleting a missing object succeeds, so
    /// interrupted runs that already removed it converge.
    async fn delete(&self, name: &str) -> Result<(), BackupError> {
        match self.store.delete(&ObjPath::from(name)).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(error) => Err(store_error(error)),
        }
    }

    /// List object names below a prefix.
    async fn list(&self, prefix: &str) -> Result<Vec<String>, BackupError> {
        Ok(self
            .list_meta(prefix)
            .await?
            .into_iter()
            .map(|meta| meta.location.to_string())
            .collect())
    }

    /// List objects below a prefix with their metadata.
    async fn list_meta(&self, prefix: &str) -> Result<Vec<ObjectMeta>, BackupError> {
        self.store
            .list(Some(&ObjPath::from(prefix)))
            .try_collect()
            .await
            .map_err(store_error)
    }
}

/// The group's applied index: the authoritative committed prefix.
fn applied_index(engine: &Engine) -> Result<u64, BackupError> {
    Ok(StateMachine::group_applied(engine)
        .map_err(BackupError::engine)?
        .map(|id| id.index)
        .unwrap_or(0))
}

/// Encode a manifest or segment as JSON.
fn encode_json<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, BackupError> {
    serde_json::to_vec(value).map_err(|error| BackupError::Format(error.to_string()))
}

/// Order backups by recency: creation time, then backup ID to break
/// ties. Re-encryption keeps both, so a rewritten manifest keeps its
/// place.
fn by_recency(a: &BackupManifest, b: &BackupManifest) -> Ordering {
    (a.created_ms, &a.backup_id).cmp(&(b.created_ms, &b.backup_id))
}

/// The most recent of `manifests` (see [`by_recency`]), if any.
fn newest(
    manifests: impl IntoIterator<Item = (String, BackupManifest)>,
) -> Option<(String, BackupManifest)> {
    manifests
        .into_iter()
        .max_by(|(_, a), (_, b)| by_recency(a, b))
}

/// One segment's index range and name, planned from names alone.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Span {
    first: u64,
    last: u64,
    name: String,
}

/// The segments of one history among object names, sorted by range.
fn spans_of(incarnation: u64, names: impl IntoIterator<Item = String>) -> Vec<Span> {
    let mut spans: Vec<Span> = names
        .into_iter()
        .filter_map(|name| {
            segment_range_from_name(&name)
                .filter(|range| range.incarnation == incarnation)
                .map(|range| Span {
                    first: range.first,
                    last: range.last,
                    name,
                })
        })
        .collect();
    spans.sort();
    spans
}

/// When the leader takes its next base and next retains.
#[derive(Clone, Copy, Debug)]
struct BasePlan {
    /// When the newest base grows `base_interval` old.
    due: tokio::time::Instant,
    /// Sealed segment bytes archived in its history after the newest
    /// base.
    archived: u64,
    /// The newest base's sealed bytes.
    base: u64,
    /// No base before this, after one failed.
    retry: Option<tokio::time::Instant>,
    /// When retention, collection and retirement next run.
    maintain: tokio::time::Instant,
}

impl BasePlan {
    /// Whether a base may be tried: not while a failed one's retry
    /// waits.
    fn may_base(&self) -> bool {
        self.retry
            .is_none_or(|retry| tokio::time::Instant::now() >= retry)
    }

    /// Whether a base is due: by age, or, as v1, once the archive since
    /// the newest base outgrows both the base bytes and that base.
    fn due(&self, settings: &ContinuousSettings) -> bool {
        self.may_base()
            && (tokio::time::Instant::now() >= self.due
                || settings
                    .base_bytes
                    .is_some_and(|bytes| self.archived >= bytes.max(self.base)))
    }
}

/// A contiguous chain of segments after a cut.
struct Chain<'a> {
    /// The chain in index order.
    spans: Vec<&'a Span>,
    /// The first index the chain leaves uncovered.
    next: u64,
    /// Whether segments past a hole remain: the archive has a gap.
    stranded: bool,
}

/// Plan the chain covering every index after `covered` from sorted
/// spans: from the next needed index, take the span reaching furthest
/// among those starting at or before it. Overlap left by interrupted
/// archive runs is harmless.
fn plan_chain(spans: &[Span], covered: u64) -> Chain<'_> {
    let mut next = covered.saturating_add(1);
    let mut chain = Vec::new();
    let mut candidates = spans.iter().filter(|span| span.last > covered).peekable();
    loop {
        let mut best: Option<&Span> = None;
        while let Some(span) = candidates.next_if(|span| span.first <= next) {
            if best.is_none_or(|best| span.last > best.last) {
                best = Some(span);
            }
        }
        match best {
            Some(span) if span.last >= next => {
                next = span.last.saturating_add(1);
                chain.push(span);
            }
            _ => break,
        }
    }
    Chain {
        spans: chain,
        next,
        stranded: candidates.peek().is_some(),
    }
}

/// A downloaded view: its metadata and its checked payload in an
/// unnamed temporary file, gone once closed.
struct SpooledView {
    meta: SnapshotMeta,
    file: std::fs::File,
}

/// Writes a view's payload to its spool file, hashing it on the way.
struct Spool {
    file: BufWriter<std::fs::File>,
    digest: Hasher,
}

impl Write for Spool {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.file.write_all(bytes)?;
        self.digest.update(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}

/// How far one replay went.
#[derive(Default)]
struct Replay {
    replayed: usize,
    reached_ms: u64,
    stopped_index: Option<u64>,
}

/// Record a restoring engine's group head.
fn record_head(engine: &Engine, head: Head) -> Result<(), BackupError> {
    let txn = engine.store().begin_write().map_err(BackupError::engine)?;
    head.record(&txn).map_err(BackupError::engine)?;
    txn.commit().map_err(BackupError::engine)
}

/// Advance a restoring engine's expiry clock past an entry's stamp and
/// record the entry, at `index` in its history, as the group head.
fn advance_clock(engine: &Engine, proposed_ms: u64, index: u64) -> Result<(), BackupError> {
    let txn = engine.store().begin_write().map_err(BackupError::engine)?;
    StateMachine::advance_expiry_clock(&txn, proposed_ms).map_err(BackupError::engine)?;
    Head::Entry(index)
        .record(&txn)
        .map_err(BackupError::engine)?;
    txn.commit().map_err(BackupError::engine)
}

/// Wall-clock milliseconds since the epoch.
/// A duration in whole ms, saturating.
fn as_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// When an object was last written, in ms since the epoch.
fn modified_ms(meta: &ObjectMeta) -> u64 {
    u64::try_from(meta.last_modified.timestamp_millis()).unwrap_or(0)
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or(0)
}
