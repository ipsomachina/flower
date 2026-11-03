//! `flower-engine`: the single-node engine core.
//!
//! The core links without consensus, networking or API serving: storage,
//! Wasm execution and local evaluation. The replication layer (Raft,
//! movement, distributed transactions, HTTP serving) builds on top, and a
//! device build links the core plus `flower-client` for sync.
//!
//! Module map:
//!
//! - [`error`]: the engine [`error::Error`] type.
//! - [`storage`]: the per-group redb store: interned tables, records,
//!   indexes, derived outcomes, dependency edges, receipts and code.
//! - [`retention`]: replicated retry epochs, sessions and collection.
//! - [`retry`]: retry identities, receipt codecs, the admit decision,
//!   durable unscoped idempotency and its memory fast path.
//! - [`intent`]: the offline intent log on the storage core.
//! - [`json`]: canonical-JSON text handling for stored bytes and keys.
//! - [`hex`]: the lowercase hex codec for binary fields and cursors.
//! - [`descriptors`]: deployed application descriptors and validation.
//! - [`contracts`]: links between modules: exports, imports and
//!   compatibility checked at deployment.
//! - [`access`]: declarative access policies and their enforcement.
//! - [`auth`]: the authorization hook, HTTP aliases and receipt owners.
//! - [`decisions`]: authorization decisions reused while they hold.
//! - [`session`]: invocation sessions: staged writes, savepoints, fuel.
//! - [`history`]: the retained-version seam for `history` calls.
//! - [`runtime`]: guest invocation backends.
//! - [`host`]: the guest host-call implementation.
//! - [`derived`]: derived evaluation, caching and invalidation.
//! - [`lane`]: the writer lane: settle, commit, triggers, transactions.
//! - [`patch`]: replicated patches and the shared journal applier.
//! - [`batch`]: batch entries: many writes in one log entry, in order.
//! - [`placement`]: what a group may do with a named partition it holds.
//! - [`movement`]: moving a database: paged export, exact differences,
//!   the import digest.
//! - [`catalog`]: the partition catalog: groups, owners, epochs, moves.
//! - [`txn`]: two-phase commit records: coordinators, prepares, locks.
//! - [`engine`]: deploy modules and invoke definitions over the store.
//! - [`staging`]: staged deployment: backfill, dual-write, retirement.
//! - [`purge`]: module removal's kept declarations and the paged purge
//!   of a removed module's data.
//! - [`snapshot`]: consistent database snapshots and their installs.
//! - [`preview`]: read-your-own-intents over a staged overlay.
//! - [`timer`]: durable timers firing module mutations.
//! - [`task`]: maintenance tasks the leader runs whenever they are due.
//! - [`work_queue`]: durable work queues: guest enqueues plus the
//!   native claim, lease and dead-letter protocol.

pub mod access;
pub mod auth;
pub mod batch;
pub mod catalog;
pub mod certificate;
pub mod contracts;
pub mod decisions;
pub mod derived;
pub mod descriptors;
pub mod engine;
pub mod error;
pub mod hex;
pub mod history;
pub mod host;
pub mod intent;
pub mod json;
pub mod lane;
pub mod movement;
pub mod patch;
pub mod placement;
pub mod preview;
pub mod purge;
pub mod query_cache;
pub mod replication;
pub mod retention;
pub mod retry;
pub mod runtime;
pub mod session;
pub mod snapshot;
pub mod speculation;
pub mod staging;
pub mod storage;
pub mod task;
pub mod timer;
pub mod txn;
pub mod work_queue;

pub use error::Error;
pub use storage::GroupStore;
