//! Backup manifests and log segments.
//!
//! Manifests are sealed JSON: one per backup, listing every view
//! with the fencing metadata restore needs, plus the expiry clock and
//! history incarnation of the cut. Segments are sealed compressed runs
//! of consecutive committed log entries, named by their history's
//! incarnation and index range; PITR replays them after a base of the
//! same history. Re-encryption supersedes manifests (`supersedes`) so
//! collection stays deterministic, keeping `created_ms` and
//! `backup_id`, which order backups by recency; it reseals segments
//! in place under their names.

use flower_cluster::types::ClusterWrite;
use flower_engine::batch::Head;

/// Manifest wire version.
pub const MANIFEST_VERSION: u32 = 1;
/// Segment wire version: 2 names segments by range and drops the
/// re-encryption `prev` chain.
pub const SEGMENT_VERSION: u32 = 2;

/// One database view inside a backup.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct ManifestObject {
    /// Opaque object name.
    pub name: String,
    /// Database identifier bytes.
    pub db: Vec<u8>,
    /// Committed revision the view was taken at.
    pub revision: u64,
    /// Applied log position the view was taken at.
    pub applied_log: u64,
    /// History incarnation the view belongs to.
    pub incarnation: u64,
    /// Key version sealing the object, for retention scans.
    pub key_version: u32,
    /// SHA-256 of the uncompressed payload, for restore checks.
    pub plaintext_sha256: Vec<u8>,
    /// Compressed payload length in bytes.
    pub compressed_len: u64,
    /// Sealed object length in bytes.
    pub ciphertext_len: u64,
}

/// A backup manifest: the sealed root of one backup.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct BackupManifest {
    /// Wire version, always [`MANIFEST_VERSION`].
    pub version: u32,
    /// Opaque backup ID.
    pub backup_id: String,
    /// Creation time in ms since epoch.
    pub created_ms: u64,
    /// Group applied index the views cover; PITR replays after it.
    pub covered_index: u64,
    /// The group expiry clock at the cut: restore seeds the restored
    /// clock from it, so receipts expire exactly as they did live.
    #[serde(default)]
    pub expiry_clock: u64,
    /// Group incarnation of the history the views belong to, zero when
    /// the group had none yet; PITR replays only this history's
    /// segments.
    #[serde(default)]
    pub incarnation: u64,
    /// The group head at the cut (see [`flower_engine::batch`]): replay
    /// starts from it, so archived batches apply or go stale exactly as
    /// they did live. Absent in manifests from before batch entries.
    #[serde(default)]
    pub head: Option<Head>,
    /// Superseded manifest name, set by re-encryption.
    pub supersedes: Option<String>,
    /// One entry per database view.
    pub objects: Vec<ManifestObject>,
}

/// One entry inside a log segment.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct SegmentEntry {
    /// Log index the write applied at.
    pub index: u64,
    /// Leader proposal stamp in ms since epoch.
    pub proposed_ms: u64,
    /// The replicated write.
    pub write: ClusterWrite,
}

/// A sealed run of consecutive committed log entries for PITR
/// replay. Segments may overlap after an interrupted archive run;
/// overlapping segments agree on every shared index because
/// committed entries never change.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct LogSegment {
    /// Wire version, always [`SEGMENT_VERSION`].
    pub version: u32,
    /// First log index covered, inclusive.
    pub first_index: u64,
    /// Last log index covered, inclusive.
    pub last_index: u64,
    /// Data entries in index order; blanks and membership entries
    /// advance the range without entries.
    pub entries: Vec<SegmentEntry>,
}
