//! `flower-cluster`: Raft ordering and sync serving over the engine core.
//!
//! The cluster crate links the engine behind openraft consensus: the
//! log store persists votes and entries in a dedicated file, the state
//! machine applies patches and deployments in log order, and sync
//! serving ships applied entries plus snapshots to followers and
//! devices. Devices never link this crate: they sync as clients.
//!
//! Module map:
//!
//! - [`types`]: the Raft type config, replicated writes, responses.
//! - [`logstore`]: vote, entry and snapshot persistence.
//! - [`compaction`]: paced online compaction of both files.
//! - [`health`]: the first storage failure, and the recovery probe.
//! - [`keys`]: the replicated managed-key catalog on a member.
//! - [`statemachine`]: ordered apply, snapshots, runtime reload.
//! - [`snapshot_file`]: snapshot images as files, streamed.
//! - [`snapshot_policy`]: byte and age snapshot triggers under a duty cycle.
//! - [`group`]: group identity: names and uids.
//! - [`network`]: in-process and authenticated TCP member transport,
//!   one listener for every hosted group.
//! - [`compat`]: the compatibility contract peers check.
//! - [`tls`]: peer TLS credentials and streams.
//! - [`rejoin`]: catching up after opening without Raft state.
//! - [`peer`]: the member channel: read fences, forwarded requests.
//! - [`read`]: read consistency: leader leases, fences, staleness.
//! - [`timing`]: Raft timing and snapshot policy derivation.
//! - [`node`]: a cluster member with client paths.
//! - [`host`]: one process hosting many groups.
//! - [`partitions`]: reaching any group, keeping replicas in step with
//!   the catalog, routing databases to their owners.
//! - [`moves`]: the partition driver: create, retire and move.
//! - [`txn`]: two-phase commit across groups, and its resolver.
//! - [`writer`]: the leader's write queue: staged preparation, group
//!   commit, one batch entry per group.
//! - [`recovery`]: the startup serving fence over a log past the state.
//! - [`admission`]: node-wide admission before engine work.
//! - [`deadline`]: the client deadline of the request being served.
//! - [`stages`]: per-request phase timings, for `Server-Timing`.
//! - [`work`]: replicated queue claims, reports and timer maintenance.
//! - [`sync`]: sync-down serving for replicas and devices.
//! - [`telemetry`]: OpenTelemetry metrics and spans of queries, the
//!   writer, the evaluator, storage, Raft and watches; commit marks.
//! - [`methods`]: what each application method costs this process.
//! - [`exec`]: engine and reader work off the async workers.
//! - [`fsync`]: `FLOWER_FSYNC=off` and its boot markers.
//! - [`flush`]: deferred flushes: the leader's own log flush and the
//!   applies' flush of the group file, one fsync per write.
//! - [`wasm`]: the production wasmtime runtime factory.

// OpenRaft's storage traits fix `StorageError` as the error type, and
// the internal helpers feeding them share it rather than box and
// convert at every call.
#![allow(clippy::result_large_err)]

pub mod admission;
pub mod compaction;
pub mod compat;
pub mod deadline;
pub mod exec;
pub mod flight;
pub mod flush;
pub mod fsync;
pub mod group;
pub mod health;
pub mod host;
pub mod keys;
pub mod logstore;
pub mod methods;
pub mod moves;
pub mod network;
pub mod node;
pub mod partitions;
pub mod peer;
pub mod read;
pub mod recovery;
pub mod rejoin;
pub mod snapshot_file;
pub mod snapshot_policy;
pub mod speculation;
pub mod stages;
pub mod statemachine;
pub mod sync;
pub mod telemetry;
pub mod timing;
pub mod tls;
pub mod txn;
pub mod types;
pub mod wasm;
pub mod work;
pub mod writer;
