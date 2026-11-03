//! Engine work off the async workers.
//!
//! The engine has one writer lane and a read side. The writer serves
//! prepares, deploy backfill and Raft apply behind one lock; queries
//! and watch refreshes run on an [`EngineReader`] against their own
//! snapshots, without that lock. Both kinds of work are synchronous
//! and may be long: guest execution burns up to the fuel budget,
//! applies fsync, activations compile Wasm. Running it on Tokio's async
//! workers would stall every task scheduled there, the Raft core's
//! heartbeats and elections included. [`with_engine`] awaits the engine
//! lock on the calling task, then moves the guard to the blocking pool
//! for the work itself; [`with_reader`] moves reader work there
//! directly.
//!
//! [`EngineReader`]: flower_engine::engine::EngineReader

use std::sync::Arc;

use flower_engine::engine::{Engine, EngineReader};
use tokio::sync::Mutex as AsyncMutex;

/// Run `work` against the locked engine on Tokio's blocking pool. The
/// lock is awaited asynchronously, so waiters never pin a blocking
/// thread. A panic inside `work` surfaces as its message.
pub async fn with_engine<T, F>(engine: &Arc<AsyncMutex<Engine>>, work: F) -> Result<T, String>
where
    T: Send + 'static,
    F: FnOnce(&mut Engine) -> T + Send + 'static,
{
    let mut guard = engine.clone().lock_owned().await;
    tokio::task::spawn_blocking(move || work(&mut guard))
        .await
        .map_err(|error| format!("engine task failed: {error}"))
}

/// Run `work` against the engine's read side on Tokio's blocking pool,
/// without the writer lock: reader work runs in parallel with other
/// reader work and with the writer. A panic inside `work` surfaces as
/// its message.
pub async fn with_reader<T, F>(reader: &EngineReader, work: F) -> Result<T, String>
where
    T: Send + 'static,
    F: FnOnce(&EngineReader) -> T + Send + 'static,
{
    let reader = reader.clone();
    tokio::task::spawn_blocking(move || work(&reader))
        .await
        .map_err(|error| format!("reader task failed: {error}"))
}

/// Wall-clock milliseconds since the epoch, for proposal stamps,
/// leases and cutoffs.
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or(0)
}
