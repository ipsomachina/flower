//! `flower-client`: the native client.
//!
//! Unary calls with retry, idempotency and typed JSON helpers over
//! any [`transport::Transport`]; sync-down replicas that pull
//! batches into a local device engine; an offline intent queue
//! that submits on reconnect; work-queue calls for workers; and
//! timer inspection and maintenance for operators. Devices link
//! the engine core plus this crate: never consensus, serving or
//! backup.
//!
//! Module map:
//!
//! - [`error`]: client failures.
//! - [`transport`]: the transport trait and HTTP implementation.
//! - [`retry`]: retry configuration.
//! - [`client`]: the native client.
//! - [`typed`]: typed calls and remote targets.
//! - [`options`]: operation-specific query and command delivery settings.
//! - [`outbox`]: durable command storage, recovery and lifecycle snapshots.
//! - [`session`]: explicit per-database causal contexts.
//! - `sync` (feature `replica`): sync-down replicas.
//! - `queue` (feature `replica`): the offline intent queue.
//! - [`work`]: work-queue calls over any transport.
//! - [`timer`]: timer inspection and maintenance over any transport.
//! - [`membership`]: membership changes and leadership transfer.
//! - [`admin`]: modules, staged deployments, managed keys, Raft and
//!   node metrics, activity, and backup status.
//! - [`retention`]: scoped retry identities, retry sessions and
//!   retention status and control.
//! - [`testing`]: a scripted transport for tests.

pub mod admin;
pub mod client;
pub mod contract;
pub mod error;
pub mod lease;
pub mod membership;
pub mod options;
pub mod outbox;
pub mod partitions;
pub mod protocol;
#[cfg(feature = "replica")]
pub mod queue;
pub mod retention;
pub mod retry;
pub mod scoped;
pub mod session;
#[cfg(feature = "replica")]
pub mod sync;
pub mod testing;
pub mod timer;
pub mod transaction;
pub mod transport;
pub mod typed;
pub mod watch;
pub mod work;

pub use client::{
    Builder, FlowerClient, MutationOptions, MutationResult, QueryResult, RequestOptions, Staleness,
};
pub use contract::CommandContract;
pub use error::FlowerError;
pub use flower_protocol::{
    Execution, Incarnation, LogPosition, Origin, Problem, ReadToken, Revision,
};
pub use lease::{ClaimDecodeError, ClaimOutcome, QueueLease, SettlementError, TypedClaim};
pub use options::{CommandOptions, QueryOptions};
pub use outbox::{
    CommandHandle, CommandOutbox, CommandRecord, CommandStatus, CommandStore, FileCommandStore,
    OutboxError, Resolution, StoreError,
};
#[cfg(feature = "replica")]
pub use queue::{IntentHandle, IntentQueue, SubmitSummary};
pub use retention::{RetryIdentity, RetrySession};
pub use retry::{Retry, RetryPolicy};
pub use scoped::{
    CommandCall, CommandError, CommandResult, DatabaseClient, ModuleClient, PreparedCommand,
    QueryCall,
};
pub use session::CausalSession;
#[cfg(feature = "replica")]
pub use sync::{DeviceRuntimeBuilder, SyncClient, SyncProgress, WireBatch, WireEntry, WireWrite};
pub use timer::{TimerInfo, TimerStats};
pub use transaction::{StepError, TransactionError, TypedTransaction, TypedTransactionResult};
pub use transaction::{TransactionOptions, TransactionResult, TransactionStep};
pub use transport::{
    Credentials, HttpTransport, Protocol, TIMEOUT_HEADER, Transport, TransportOptions,
    TransportResponse,
};
pub use typed::{
    CallResult, DeclaredSubscription, DeclaredWatch, RemoteError, RemoteTarget, TypedSubscription,
    TypedWatch,
};
pub use work::{Claim, QueueItem, QueuePoll, QueueReadiness, QueueReport, QueueStats};

/// Re-exported so callers can cancel calls without depending on
/// `tokio-util` themselves.
pub use tokio_util::sync::CancellationToken;

/// Common remote application APIs; replica and operator APIs remain explicit imports.
pub mod prelude {
    pub use crate::{
        CausalSession, CommandOptions, CommandOutbox, FlowerClient, FlowerError, ModuleClient,
        PreparedCommand, QueryOptions, ReadToken, RequestOptions, Revision, TypedTransaction,
    };
    pub use flower_sdk::{FlowerType, MutationOp, Nullable, Omittable, QueryOp};
}
