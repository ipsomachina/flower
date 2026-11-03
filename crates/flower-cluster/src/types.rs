//! Cluster types: the Raft type config, replicated writes and their
//! responses.
//!
//! The log carries accepted mutation patches (queue claims and reports
//! and timer maintenance included), mostly grouped into batch entries
//! by the leader's writer (a removed module's purge pages among
//! them), staged deployments, module removals and the history
//! incarnation. All apply atomically in log order; activation and
//! removal entries and patches share the order, so every member
//! applies every patch under the code version the leader used.

use flower_engine::batch::{BatchItem, Head};
use openraft::{BasicNode, Entry, RaftTypeConfig, TokioRuntime};
use serde::{Deserialize, Serialize};

/// Raft node identifier.
pub type NodeId = u64;

/// The cluster's Raft type configuration.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct TypeConfig;

impl RaftTypeConfig for TypeConfig {
    type D = ClusterWrite;
    type R = WriteResponse;
    type NodeId = NodeId;
    type Node = BasicNode;
    type Entry = Entry<Self>;
    type SnapshotData = crate::snapshot_file::SnapshotFile;
    type AsyncRuntime = TokioRuntime;
    type Responder = openraft::impls::OneshotResponder<Self>;
}

/// One replicated client write.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum ClusterWrite {
    /// An accepted mutation patch for one database.
    Patch {
        /// Database identifier bytes.
        db: Vec<u8>,
        /// Encoded patch bytes.
        patch: Vec<u8>,
        /// Leader proposal time in ms since epoch. Agreed by all
        /// members as part of the entry, this is the PITR cutoff
        /// stamp; it approximates commit time within milliseconds.
        #[serde(default)]
        proposed_ms: u64,
    },
    /// An atomic multi-database transaction patch: one patch per
    /// participant, committed all-or-nothing at one log position.
    MultiPatch {
        /// Encoded multi-patch bytes.
        patches: Vec<u8>,
        /// Leader proposal time in ms since epoch, as for patches.
        #[serde(default)]
        proposed_ms: u64,
    },
    /// A staged deployment awaiting activation on every member.
    Deploy {
        /// Database identifier bytes.
        db: Vec<u8>,
        /// Module name.
        module: String,
        /// Wasm bundle bytes.
        wasm: Vec<u8>,
        /// Canonical manifest JSON bytes.
        manifest: Vec<u8>,
        /// Target code identity.
        identity: [u8; 32],
        /// Authoritative intern bindings from the staging node.
        bindings: Vec<(Vec<u8>, u32)>,
        /// Leader proposal time in ms since epoch, as for patches.
        #[serde(default)]
        proposed_ms: u64,
    },
    /// The group's history incarnation: a random tag a leader proposes
    /// while none is applied. The first applied value wins and later
    /// ones change nothing, so racing leaders still leave every member
    /// with the same tag.
    Incarnation {
        /// Proposed incarnation, never zero.
        incarnation: u64,
        /// Leader proposal time in ms since epoch, as for patches.
        #[serde(default)]
        proposed_ms: u64,
    },
    /// A group of writes the leader's writer prepared in order, each on
    /// what the ones before it staged (see [`crate::writer`]). Items
    /// apply in order in one transaction, each with its own outcome and
    /// revision. The batch applies only directly after the data entry
    /// `follows` names, else every item rejects as stale and the writer
    /// prepares them again (see [`flower_engine::batch`]).
    Batch {
        /// The writes, in the order they were prepared.
        items: Vec<BatchItem>,
        /// The data entry the writer prepared the batch after.
        follows: Head,
        /// The proposing writer's random identity.
        writer: u64,
        /// This batch's sequence within its writer, which a successor
        /// prepared while it committed names.
        seq: u64,
        /// Leader proposal time in ms since epoch, as for patches.
        proposed_ms: u64,
    },
    /// A module's removal from one database, applied on every member
    /// alike (see [`flower_engine::purge`]): its code goes, and its data
    /// stays with its declarations, or, with `purge`, goes a page at a
    /// time through later batch items.
    Remove {
        /// Database identifier bytes.
        db: Vec<u8>,
        /// Module name.
        module: String,
        /// Whether its data purges too.
        purge: bool,
        /// Leader proposal time in ms since epoch, as for patches.
        proposed_ms: u64,
    },
    /// A change to one database's managed-key catalog the leader
    /// prepared: the next catalog with sealed material, applied
    /// deterministically on every member (see [`crate::keys`]).
    Keys {
        /// Database identifier bytes.
        db: Vec<u8>,
        /// Encoded [`flower_crypto::catalog::CatalogChange`].
        change: Vec<u8>,
        /// Leader proposal time in ms since epoch, as for patches.
        #[serde(default)]
        proposed_ms: u64,
    },
}

impl ClusterWrite {
    /// The leader's proposal time the entry carries.
    pub fn proposed_ms(&self) -> u64 {
        match self {
            ClusterWrite::Patch { proposed_ms, .. }
            | ClusterWrite::MultiPatch { proposed_ms, .. }
            | ClusterWrite::Deploy { proposed_ms, .. }
            | ClusterWrite::Incarnation { proposed_ms, .. }
            | ClusterWrite::Batch { proposed_ms, .. }
            | ClusterWrite::Keys { proposed_ms, .. }
            | ClusterWrite::Remove { proposed_ms, .. } => *proposed_ms,
        }
    }
}

/// A replicated write's outcome.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum ResponseOutcome {
    /// Successful value as canonical JSON bytes.
    Value(Vec<u8>),
    /// Replicated business failure as failure JSON bytes.
    Failure(Vec<u8>),
    /// Deterministic apply-time rejection with its message.
    Error(String),
}

/// A committed write's notice for watch hubs: which database
/// changed, at which revision and log position, or that a database's
/// managed-key catalog changed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitNotice {
    /// Database name. For a key-catalog notice, the database whose
    /// catalog changed; empty when every database's may have.
    pub db: Vec<u8>,
    /// Database revision after the apply; zero for a key-catalog notice.
    pub revision: u64,
    /// Log position the write applied at.
    pub position: u64,
    /// A managed-key catalog may have changed (a key change applied,
    /// or a snapshot installed), which moves no revision: hubs refresh
    /// what resolved a key in that database (`db`), or in every one.
    /// Sent once this member serves the new catalog.
    pub keys: bool,
    /// Records the write put in the database (rows set, queue and timer
    /// rows included); zero when it applied no journal (a duplicate, a
    /// rejection, a deployment).
    pub puts: u64,
    /// Records it deleted.
    pub deletes: u64,
    /// The sampled `flower.writer.batch` span that committed it, when
    /// telemetry remembers one: watch deliveries it wakes link to it.
    pub batch: Option<opentelemetry::trace::SpanContext>,
}

impl CommitNotice {
    /// A database commit's notice.
    pub fn commit(db: Vec<u8>, revision: u64, position: u64) -> Self {
        CommitNotice {
            db,
            revision,
            position,
            keys: false,
            puts: 0,
            deletes: 0,
            batch: None,
        }
    }

    /// A notice that database `db`'s key catalog changed.
    pub fn keys(db: Vec<u8>, position: u64) -> Self {
        CommitNotice {
            db,
            revision: 0,
            position,
            keys: true,
            puts: 0,
            deletes: 0,
            batch: None,
        }
    }

    /// A notice that any database's key catalog may have changed.
    pub fn all_keys(position: u64) -> Self {
        CommitNotice::keys(Vec::new(), position)
    }

    /// The same notice, with the records the write put and deleted.
    pub fn with_records(mut self, puts: u64, deletes: u64) -> Self {
        self.puts = puts;
        self.deletes = deletes;
        self
    }
}

/// A replicated write's response: outcome plus assigned positions.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct WriteResponse {
    /// The write's outcome.
    pub outcome: ResponseOutcome,
    /// Database revision after the apply. For multi-database writes
    /// this is the first participant's revision; see `revisions`.
    pub revision: u64,
    /// Log position the write applied at.
    pub position: u64,
    /// Per-participant `(database, revision)` pairs for multi-database
    /// writes, in step order. Empty for single-database writes.
    #[serde(default)]
    pub revisions: Vec<(Vec<u8>, u64)>,
    /// Per-group `(group, log position)` pairs for a transaction across
    /// groups: where each participant group committed its part, in its
    /// own log. Empty for every other write.
    #[serde(default)]
    pub positions: Vec<(String, u64)>,
    /// One response per item of a batch entry, in item order. Empty for
    /// every other entry.
    #[serde(default)]
    pub items: Vec<WriteResponse>,
    /// Whether a stored receipt answered: the write committed earlier
    /// under the same retry identity or idempotency key, and this
    /// answer replays its outcome (with its revision and position when
    /// the receipt records them) without running anything again.
    #[serde(default)]
    pub duplicate: bool,
}

impl WriteResponse {
    /// A successful value response.
    pub fn value(result: Vec<u8>, revision: u64, position: u64) -> Self {
        WriteResponse {
            outcome: ResponseOutcome::Value(result),
            revision,
            position,
            revisions: Vec::new(),
            positions: Vec::new(),
            items: Vec::new(),
            duplicate: false,
        }
    }

    /// A replicated business-failure response.
    pub fn failure(result: Vec<u8>, revision: u64, position: u64) -> Self {
        WriteResponse {
            outcome: ResponseOutcome::Failure(result),
            revision,
            position,
            revisions: Vec::new(),
            positions: Vec::new(),
            items: Vec::new(),
            duplicate: false,
        }
    }

    /// A deterministic rejection response.
    pub fn error(message: String, revision: u64, position: u64) -> Self {
        WriteResponse {
            outcome: ResponseOutcome::Error(message),
            revision,
            position,
            revisions: Vec::new(),
            positions: Vec::new(),
            items: Vec::new(),
            duplicate: false,
        }
    }
}
