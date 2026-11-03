//! The startup serving fence.
//!
//! Every apply commits its effects and the group position in one
//! transaction, so a member's state is always exactly the state at some
//! log position. A member can open with durable log entries beyond that
//! position: committed entries it hadn't applied when it stopped, applies
//! a crash took from the group file before they were flushed (applies
//! commit deferred, see [`crate::flush`]), and perhaps an uncommitted
//! tail. OpenRaft re-applies everything the log
//! store remembers as committed before the member starts (the store
//! records the committed index with its next write, so it may lag a
//! little), but entries past that may be committed too, and other
//! members or devices may already have read their effects.
//!
//! Such a member is *recovering* until it has applied either its whole
//! open-time log or a commit index it learned after it opened (from its
//! leader, or as a leader from its quorum): only then does it hold at
//! least the state it held before, re-established by the group. Until
//! then it reports itself unready, refuses sync-down batches with the
//! retryable [`RECOVERING`] code, and its linearizable reads (queries,
//! watches, queue and timer operations) wait for the fence, up to
//! [`RECOVERY_WAIT`], instead of answering from the restored floor. A
//! member whose log ends at its applied position opens with no fence.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use openraft::Raft;

use crate::types::TypeConfig;

/// The failure code of requests a recovering member refuses.
pub const RECOVERING: &str = "recovering";

/// The longest a linearizable read waits for the fence before answering
/// [`RECOVERING`].
pub const RECOVERY_WAIT: Duration = Duration::from_secs(10);

/// A member's startup serving fence.
#[derive(Debug)]
pub struct RecoveryFence {
    /// The last log index at open.
    tail: u64,
    /// What the member had applied once OpenRaft re-applied the
    /// remembered committed prefix: the restored floor.
    floor: u64,
    /// The member led its group when it stopped without flushing its log
    /// (see [`crate::flush`]): the log may lack entries it applied and
    /// served, so its open-time tail proves nothing.
    lost_tail: bool,
    recovering: AtomicBool,
    cleared: tokio::sync::Notify,
}

impl RecoveryFence {
    /// The fence for a member that opened with its log at `tail` and its
    /// state at `floor`, if the log runs past the state.
    pub fn new(tail: Option<u64>, floor: Option<u64>) -> Option<Arc<Self>> {
        let (tail, floor) = (tail.unwrap_or(0), floor.unwrap_or(0));
        (tail > floor).then(|| Self::fence(tail, floor, false))
    }

    /// The fence for a leader that stopped without flushing its log: a
    /// crash may have taken entries of its term it applied, served and
    /// had its followers commit. It holds, whatever the log, until the
    /// member applied a commit of its current leader's own term, which
    /// covers every entry committed before that term.
    pub fn after_lost_tail(tail: Option<u64>, floor: Option<u64>) -> Arc<Self> {
        Self::fence(tail.unwrap_or(0), floor.unwrap_or(0), true)
    }

    fn fence(tail: u64, floor: u64, lost_tail: bool) -> Arc<Self> {
        Arc::new(RecoveryFence {
            tail,
            floor,
            lost_tail,
            recovering: AtomicBool::new(true),
            cleared: tokio::sync::Notify::new(),
        })
    }

    /// Whether the member still recovers.
    pub fn recovering(&self) -> bool {
        self.recovering.load(Ordering::Acquire)
    }

    /// Why the member isn't serving, while it recovers.
    pub fn reason(&self) -> String {
        if self.lost_tail {
            return format!(
                "recovering: this member led its group when it stopped with its log unflushed \
                 (the log reaches {}, its state {}); it serves once it applied a commit of its \
                 leader's current term",
                self.tail, self.floor
            );
        }
        format!(
            "recovering: the log reaches {} but this member had applied {} when it opened; \
             it serves once it applied a commit index confirmed since",
            self.tail, self.floor
        )
    }

    /// Whether `applied` with `committed` (this member's live view)
    /// re-establishes the state: the whole open-time log is applied, or
    /// a commit index past the floor, learned since opening, is.
    fn covered(&self, applied: Option<u64>, committed: Option<u64>) -> bool {
        let applied = applied.unwrap_or(0);
        if applied >= self.tail {
            return true;
        }
        committed.is_some_and(|committed| committed > self.floor && applied >= committed)
    }

    /// [`RecoveryFence::covered`] given the committed entry's term and
    /// the member's current one: after a lost tail only an applied commit
    /// of the current term covers, since an earlier leader's commits may
    /// lag what this member served.
    fn covered_in(&self, applied: Option<u64>, committed: Option<(u64, u64)>, term: u64) -> bool {
        if !self.lost_tail {
            return self.covered(applied, committed.map(|(index, _)| index));
        }
        committed.is_some_and(|(index, committed_term)| {
            committed_term == term && index > self.floor && applied.unwrap_or(0) >= index
        })
    }

    fn clear(&self) {
        if !self.recovering.swap(false, Ordering::AcqRel) {
            return;
        }
        self.cleared.notify_waiters();
        tracing::info!(
            target: "flower::storage",
            tail = self.tail,
            floor = self.floor,
            "recovered the log past the restored state: serving"
        );
    }

    /// Clear the fence once Raft's applied and committed positions cover
    /// it: check on every metrics change until then, or until Raft stops.
    pub(crate) fn watch(self: &Arc<Self>, raft: Raft<TypeConfig>) {
        let fence = self.clone();
        tokio::spawn(async move {
            let mut metrics = raft.metrics();
            while fence.recovering() {
                let (applied, running) = {
                    let metrics = metrics.borrow_and_update();
                    let applied = metrics.last_applied.map(|id| id.index);
                    (applied, metrics.running_state.is_ok())
                };
                if !running {
                    return;
                }
                let Ok((committed, term)) = raft
                    .with_raft_state(|state| {
                        (
                            state.committed.map(|id| (id.index, id.leader_id.term)),
                            state.vote_ref().leader_id().get_term(),
                        )
                    })
                    .await
                else {
                    return;
                };
                if fence.covered_in(applied, committed, term) {
                    fence.clear();
                    return;
                }
                if metrics.changed().await.is_err() {
                    return;
                }
            }
        });
    }

    /// Wait up to `bound` for the fence to clear; whether it did.
    pub async fn wait(&self, bound: Duration) -> bool {
        let cleared = self.cleared.notified();
        tokio::pin!(cleared);
        cleared.as_mut().enable();
        if !self.recovering() {
            return true;
        }
        let _ = tokio::time::timeout(bound, cleared).await;
        !self.recovering()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_log_past_the_state_fences() {
        assert!(RecoveryFence::new(None, None).is_none());
        assert!(RecoveryFence::new(Some(5), Some(5)).is_none());
        assert!(RecoveryFence::new(Some(5), Some(7)).is_none());
        let fence = RecoveryFence::new(Some(9), Some(5)).unwrap();
        assert!(fence.recovering());
        assert!(fence.reason().contains("reaches 9"), "{}", fence.reason());
    }

    #[test]
    fn a_fresh_commit_or_the_whole_tail_covers_the_fence() {
        let fence = RecoveryFence::new(Some(9), Some(5)).unwrap();
        // The restored floor proves nothing new, applied or not.
        assert!(!fence.covered(Some(5), Some(5)));
        // A commit learned since opening covers once applied.
        assert!(!fence.covered(Some(6), Some(7)));
        assert!(fence.covered(Some(7), Some(7)));
        // As does applying the whole open-time log.
        assert!(fence.covered(Some(9), Some(5)));
        assert!(fence.covered(Some(9), None));
    }

    /// A leader that stopped with its log unflushed fences even with no
    /// log past its state, and neither its open-time tail nor a commit an
    /// earlier term's leader reports covers: only a commit of the current
    /// term, applied.
    #[test]
    fn after_a_lost_tail_only_a_commit_of_the_current_term_covers() {
        let fence = RecoveryFence::after_lost_tail(Some(5), Some(5));
        assert!(fence.recovering());
        assert!(fence.reason().contains("unflushed"), "{}", fence.reason());
        assert!(!fence.covered_in(Some(9), Some((9, 2)), 3));
        assert!(!fence.covered_in(Some(5), Some((5, 3)), 3));
        assert!(!fence.covered_in(Some(6), Some((7, 3)), 3));
        assert!(fence.covered_in(Some(7), Some((7, 3)), 3));
        // Without a lost tail, the term doesn't matter.
        let fence = RecoveryFence::new(Some(9), Some(5)).unwrap();
        assert!(fence.covered_in(Some(9), Some((5, 1)), 3));
        assert!(fence.covered_in(Some(7), Some((7, 2)), 3));
    }

    #[tokio::test]
    async fn waiters_wake_when_the_fence_clears() {
        let fence = RecoveryFence::new(Some(9), Some(5)).unwrap();
        assert!(!fence.wait(Duration::from_millis(10)).await);
        let waiter = {
            let fence = fence.clone();
            tokio::spawn(async move { fence.wait(Duration::from_secs(30)).await })
        };
        tokio::task::yield_now().await;
        fence.clear();
        assert!(waiter.await.unwrap());
        assert!(fence.wait(Duration::ZERO).await);
    }
}
