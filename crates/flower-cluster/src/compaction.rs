//! Paced online compaction of a member's files.
//!
//! Each file (`group.redb` and `raft.redb`) gets its own loop: measure
//! every [`CompactionSettings::check`] while idle, and while a run is
//! active take one bounded step, then wait so steps take
//! [`CompactionSettings::duty_percent`] of the time. Steps run on the
//! blocking pool and take only redb's write lock, never the engine's, so
//! queries keep flowing and writers wait at most one step. The engine
//! side ([`flower_engine::storage::compact`]) decides what each step
//! does. A disk failure in a step is recorded as the member's storage
//! failure and ends the loop; any other step error is logged and retried
//! at the next check.

use std::sync::Arc;

use flower_engine::error::Error;
use flower_engine::storage::compact::{CompactionSettings, Compactor, Step};

use crate::health::StorageHealth;

/// Compact one file until `shutdown` turns true or storage fails. When
/// this returns, no step is in flight, so the file can close.
pub async fn run_file(
    compactor: Compactor,
    settings: CompactionSettings,
    health: Arc<StorageHealth>,
    component: &'static str,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    if !settings.enabled() {
        return;
    }
    let mut pause = settings.check;
    loop {
        tokio::select! {
            () = tokio::time::sleep(pause) => {}
            changed = shutdown.changed() => {
                if changed.is_err() {
                    return;
                }
            }
        }
        if *shutdown.borrow() || health.failure().is_some() {
            return;
        }
        let was_active = compactor.stats().active;
        let step = compactor.clone();
        let outcome = tokio::task::spawn_blocking(move || step.step(&settings)).await;
        pause = match outcome {
            Ok(Ok(Step::Idle)) => settings.check,
            Ok(Ok(Step::Moved(took))) => {
                if !was_active {
                    let stats = compactor.stats();
                    tracing::info!(
                        target: "flower::storage",
                        file = %compactor.path().display(),
                        file_bytes = stats.file_bytes,
                        occupied_bytes = stats.occupied_bytes,
                        allocated_bytes = stats.allocated_bytes,
                        "compacting the file online"
                    );
                }
                settings.pause_after(took)
            }
            Ok(Ok(Step::Finished {
                before,
                after,
                reached,
            })) => {
                tracing::info!(
                    target: "flower::storage",
                    file = %compactor.path().display(),
                    before,
                    after,
                    reached,
                    steps = compactor.stats().steps,
                    "compacted the file online"
                );
                settings.check
            }
            Ok(Err(Error::Io(error))) => {
                // redb refuses the handle from here on: the member must
                // restart to write again.
                health.fail(component, format!("online compaction: {error}"));
                return;
            }
            Ok(Err(error)) => {
                tracing::warn!(
                    target: "flower::storage",
                    file = %compactor.path().display(),
                    %error,
                    "online compaction step failed"
                );
                settings.check
            }
            Err(error) => {
                tracing::error!(
                    target: "flower::storage",
                    file = %compactor.path().display(),
                    %error,
                    "online compaction task failed; compaction stops"
                );
                return;
            }
        };
    }
}
