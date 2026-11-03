//! Native queue workers: claim due items, run handlers with
//! lease renewal, and report completion, failure or death.
//!
//! ```rust,no_run
//! use std::sync::Arc;
//! use std::time::Duration;
//!
//! use flower_client::FlowerClient;
//! use flower_client::client::RequestOptions;
//! use flower_worker::{QueueWorkerOptions, WorkError, run_queue_worker};
//!
//! # async fn run() {
//! let client = FlowerClient::builder("http://127.0.0.1:8080").build().unwrap();
//! // Between 1 and 32 handlers, following demand and the process load.
//! let options = QueueWorkerOptions::new("db", "email").adaptive(1, 4, 32);
//! let stop = Arc::new(tokio::sync::Notify::new());
//! let summary = run_queue_worker(client, options, stop, |claim| async move {
//!     println!("sending to {}", claim.payload);
//!     Ok(())
//! })
//! .await
//! .unwrap();
//! # }
//! ```
//!
//! Reports are best-effort: an unreported claim redelivers when its
//! lease lapses, so handlers must be idempotent. Only the leader
//! serves claims; forwards and transport failures sleep and retry
//! inside the loop.
//!
//! The worker can wait in its scope's line (`wait`: a new job wakes the first
//! in line rather than every worker), chain each report to its next claim
//! (`chain`), let jobs that mostly wait stop counting toward the limit
//! ([`JobControl::idle`], under [`run_queue_worker_with`]) and bound the
//! drain on stop (`drain`).
//!
//! The worker talks to Flower through [`QueueClient`]: `FlowerClient`
//! implements it over HTTP, and the `testing` feature's
//! [`testing::FakeQueue`] in memory.
//!
//! Module map:
//!
//! - [`worker`]: the queue worker loop.
//! - [`capacity`]: fixed and adaptive concurrency limits.
//! - [`client`]: the [`QueueClient`] trait.
//! - [`reconcile`]: keeps `flower_sdk::external` values current.
//! - `testing` (feature `testing`): an in-memory queue.

pub mod capacity;
pub mod client;
pub mod reconcile;
#[cfg(feature = "testing")]
pub mod testing;
pub mod typed;
pub mod worker;
pub use typed::{TypedJob, run_typed_queue_worker};

pub use capacity::{
    Adaptive, Health, LimitChange, Limiter, Load, LoadGate, PoolLoad, ProcessHealth, Throttle,
};
pub use client::QueueClient;
pub use reconcile::{
    ExternalWork, ReconcileClient, ReconcileEvent, ReconcileHandler, ReconcileOptions,
    ReconcileSummary, reconcile, reconcile_typed,
};
pub use worker::{
    EventHandler, JobControl, QueueWorkerOptions, WorkError, WorkerEvent, WorkerSummary,
    run_queue_worker, run_queue_worker_with,
};
