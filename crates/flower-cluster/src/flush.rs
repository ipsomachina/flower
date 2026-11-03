//! Deferred flushes: one fsync per write.
//!
//! Without them every entry is synced three times on its way to an
//! answer: the leader's append, then a follower's (the leader replicated
//! only once its own flush returned), then the leader's apply into the
//! group file. Two mechanisms take that to the one fsync Raft needs, a
//! follower's:
//!
//! - **The leader's own append is deferred.** OpenRaft's core (Flower's
//!   fork, `vendor/openraft/README.md`, change 6) hands the current
//!   leader's appends to the log store marked
//!   [`LogFlushed::is_leader_append`] and replicates them at once; it
//!   counts the leader toward the commit quorum for them only once the
//!   store completes the callback. The log store commits such an append
//!   to `raft.redb` without an fsync (readable at once, and ordered before
//!   every later write of the file, since redb commits in order and a
//!   durable commit makes every earlier one durable), and a
//!   [`LeaderFlusher`] completes the callbacks once a durable commit
//!   covers them. While the followers alone form a majority of every
//!   voter configuration (three voters or more in each, joint membership
//!   included) the flush waits: commits form on the followers' flushes,
//!   and the leader flushes an append only when the group has not
//!   committed it within [`LeaderFlush::grace`] (a follower is down or
//!   slow, so the leader's vote is needed; the leader then flushes every
//!   append at once for [`EAGER_WINDOW`]), and otherwise at most
//!   [`LeaderFlush::interval`] after it. Otherwise the leader flushes at
//!   once, in parallel with replication. A group of one voter, with no
//!   replication to overlap, appends durably as before.
//! - **Applies are deferred.** The state machine commits applies to
//!   `group.redb` without an fsync ([`flower_engine::storage::deferring`]);
//!   a node task makes them durable every [`FlushSettings::apply_every`]
//!   ([`ApplyDurability`]), and so does any other durable commit of the
//!   file. After a crash the file is back at its last durable commit,
//!   and the member re-applies the rest from its log: OpenRaft re-applies
//!   up to the committed index the log records before the member starts,
//!   and the recovery fence ([`crate::recovery`]) keeps it from serving
//!   until it caught up with a commit index confirmed since.
//!
//! Two guards keep the files consistent across a crash. Every durable
//! commit of the group file first makes the Raft log durable
//! ([`flower_engine::storage::GroupStore::set_before_durable`], which the
//! compactor's steps run too), so applied state never survives a crash
//! ahead of an unflushed entry of the leader's log. And a snapshot build
//! makes the group file durable before it publishes the snapshot, after
//! which OpenRaft may purge the log up to it, so a crash never leaves
//! the state behind a purged log.
//!
//! Every acknowledged write stays durable on a quorum of logs: the
//! leader counts itself only for appends it flushed, and a follower
//! answers an append only once it is durable. A leader that crashes can
//! lose its unflushed tail, exactly like a member that never received
//! it, and comes back as a follower to catch up. A full disk now
//! surfaces at the next durable commit rather than at the write that
//! filled it: the deferred commit's pages stay in redb's write buffer.
//! The group flush records it ([`crate::health`]), and redb refuses
//! every later transaction of the file, so the next apply stops Raft.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use openraft::storage::LogFlushed;
use openraft::{BasicNode, Membership};

use crate::types::{NodeId, TypeConfig};

/// Whether the leader defers its own flush: `lazy` (the default) or
/// `immediate`, which still flushes in parallel with replication.
pub const LEADER_FLUSH_ENV: &str = "FLOWER_LEADER_FLUSH";
/// [`LeaderFlush::grace`] in milliseconds.
pub const LEADER_FLUSH_GRACE_ENV: &str = "FLOWER_LEADER_FLUSH_GRACE_MS";
/// [`LeaderFlush::interval`] in milliseconds.
pub const LEADER_FLUSH_INTERVAL_ENV: &str = "FLOWER_LEADER_FLUSH_INTERVAL_MS";
/// [`FlushSettings::apply_every`] in milliseconds; `0` makes every
/// apply durable on its own.
pub const APPLY_FLUSH_ENV: &str = "FLOWER_APPLY_FLUSH_MS";

/// How long the leader flushes every append at once after the group
/// needed its vote for one: the followers alone did not commit it within
/// the grace, so the next ones likely need it too.
pub const EAGER_WINDOW: Duration = Duration::from_secs(1);

/// How the leader flushes its own log appends.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LeaderFlush {
    /// Defer the flush while the followers alone form a majority of every
    /// voter configuration; otherwise flush each append at once.
    pub lazy: bool,
    /// How long an append the group hasn't committed waits before the
    /// leader flushes it, its vote then being needed.
    pub grace: Duration,
    /// The longest any append waits for its flush.
    pub interval: Duration,
}

impl Default for LeaderFlush {
    /// Flower v1's: lazy, 30 ms of grace, at most 500 ms.
    fn default() -> Self {
        LeaderFlush {
            lazy: true,
            grace: Duration::from_millis(30),
            interval: Duration::from_millis(500),
        }
    }
}

/// When a member's log and group file reach the disk.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FlushSettings {
    /// The leader's own appends.
    pub leader: LeaderFlush,
    /// How often deferred applies are made durable; `None` commits every
    /// apply durably on its own, a second fsync per entry.
    pub apply_every: Option<Duration>,
}

impl Default for FlushSettings {
    fn default() -> Self {
        FlushSettings {
            leader: LeaderFlush::default(),
            apply_every: Some(Duration::from_secs(1)),
        }
    }
}

impl FlushSettings {
    /// From the four variables' values, each optional: the leader flush
    /// mode (`lazy` or `immediate`), its grace and interval, and the
    /// apply flush interval, in milliseconds (`0`: durable applies).
    pub fn parse(
        mode: Option<&str>,
        grace_ms: Option<&str>,
        interval_ms: Option<&str>,
        apply_ms: Option<&str>,
    ) -> Result<Self, String> {
        let defaults = FlushSettings::default();
        let millis = |name: &str, value: Option<&str>| -> Result<Option<u64>, String> {
            match value.map(str::trim).filter(|value| !value.is_empty()) {
                None => Ok(None),
                Some(value) => value
                    .parse()
                    .map(Some)
                    .map_err(|_| format!("{name} must be a number of milliseconds, not {value:?}")),
            }
        };
        let lazy = match mode.map(str::trim).filter(|value| !value.is_empty()) {
            None => defaults.leader.lazy,
            Some(mode) => match mode.to_ascii_lowercase().as_str() {
                "lazy" => true,
                "immediate" => false,
                _ => {
                    return Err(format!(
                        "{LEADER_FLUSH_ENV} must be lazy or immediate, not {mode:?}"
                    ));
                }
            },
        };
        let grace = millis(LEADER_FLUSH_GRACE_ENV, grace_ms)?
            .map_or(defaults.leader.grace, Duration::from_millis);
        let interval = millis(LEADER_FLUSH_INTERVAL_ENV, interval_ms)?
            .map_or(defaults.leader.interval, Duration::from_millis);
        if interval.is_zero() {
            return Err(format!("{LEADER_FLUSH_INTERVAL_ENV} must be positive"));
        }
        let apply_every = match millis(APPLY_FLUSH_ENV, apply_ms)? {
            None => defaults.apply_every,
            Some(0) => None,
            Some(ms) => Some(Duration::from_millis(ms)),
        };
        Ok(FlushSettings {
            leader: LeaderFlush {
                lazy,
                grace,
                interval,
            },
            apply_every,
        })
    }

    /// From the environment (see [`FlushSettings::parse`]).
    pub fn from_env() -> Result<Self, String> {
        let var = |name| std::env::var(name).ok();
        Self::parse(
            var(LEADER_FLUSH_ENV).as_deref(),
            var(LEADER_FLUSH_GRACE_ENV).as_deref(),
            var(LEADER_FLUSH_INTERVAL_ENV).as_deref(),
            var(APPLY_FLUSH_ENV).as_deref(),
        )
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The commit order of a Raft file's write transactions, and how much of
/// it is durable. redb runs one write transaction at a time, so numbering
/// commits as they happen numbers them in commit order, and a durable
/// commit makes every earlier one durable.
#[derive(Debug, Default)]
pub(crate) struct Commits {
    last: AtomicU64,
    durable: AtomicU64,
}

impl Commits {
    /// Number the commit about to happen; call while holding redb's
    /// write lock (an open write transaction).
    pub(crate) fn next(&self) -> u64 {
        self.last.fetch_add(1, Ordering::AcqRel) + 1
    }

    /// Commit `seq` was durable: so is every commit before it.
    pub(crate) fn made_durable(&self, seq: u64) {
        self.durable.fetch_max(seq, Ordering::AcqRel);
    }

    /// The last numbered commit.
    pub(crate) fn last(&self) -> u64 {
        self.last.load(Ordering::Acquire)
    }

    /// The last durable commit.
    pub(crate) fn durable(&self) -> u64 {
        self.durable.load(Ordering::Acquire)
    }
}

/// What a group's voter configurations need for a commit.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Quorum {
    /// Every voter configuration has at least three voters: the
    /// followers alone form a majority of each.
    pub(crate) followers_suffice: bool,
    /// Every voter configuration has one voter at most: nothing to
    /// replicate in parallel with.
    pub(crate) alone: bool,
}

impl Quorum {
    /// The quorum rule of `membership`, joint configurations included.
    pub(crate) fn of(membership: &Membership<NodeId, BasicNode>) -> Self {
        let configs = membership.get_joint_config();
        Quorum {
            followers_suffice: !configs.is_empty()
                && configs.iter().all(|voters| voters.len() >= 3),
            alone: configs.iter().all(|voters| voters.len() <= 1),
        }
    }
}

/// A leader append committed without an fsync, waiting for one.
struct Held {
    /// Its commit's number (see [`Commits`]).
    seq: u64,
    /// Its first entry's index.
    first_index: u64,
    /// Its last entry's index.
    last_index: u64,
    /// When it was appended.
    at: Instant,
    callback: LogFlushed<TypeConfig>,
}

/// Counters of a log store's leader flushes, for tests and metrics.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LeaderFlushStats {
    /// Leader appends committed without an fsync.
    pub deferred: u64,
    /// Leader appends committed durably (a group of one, or a stopped
    /// flusher).
    pub direct: u64,
    /// Durable commits the flusher made for held appends.
    pub flushes: u64,
    /// Of those, the ones an uncommitted append's grace ran out for.
    pub grace_flushes: u64,
    /// Held appends an unrelated durable commit covered.
    pub covered: u64,
}

/// When the flusher should flush next.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Due {
    pub(crate) at: Instant,
    /// The grace of an uncommitted append set it.
    pub(crate) grace: bool,
}

/// The leader's held appends and the policy that flushes them (see the
/// module documentation). The log store drives it.
#[derive(Default)]
pub(crate) struct LeaderFlusher {
    settings: Mutex<LeaderFlush>,
    held: Mutex<VecDeque<Held>>,
    /// One past the committed index Raft last reported.
    committed_next: AtomicU64,
    /// The quorum rule of the applied membership.
    applied: Mutex<Quorum>,
    /// The quorum rule of the last membership appended in this process,
    /// which Raft already uses; `None` before one, or after a truncation.
    appended: Mutex<Option<Quorum>>,
    /// Flush every append at once until then.
    eager_until: Mutex<Option<Instant>>,
    /// Set by a failed flush: every later held append fails with it.
    failed: Mutex<Option<String>>,
    /// Set once the store stops flushing: appends are then durable.
    stopped: AtomicBool,
    pub(crate) wake: tokio::sync::Notify,
    stats: Mutex<LeaderFlushStats>,
}

impl LeaderFlusher {
    pub(crate) fn set_settings(&self, settings: LeaderFlush) {
        *lock(&self.settings) = settings;
        self.wake.notify_one();
    }

    pub(crate) fn settings(&self) -> LeaderFlush {
        *lock(&self.settings)
    }

    pub(crate) fn stats(&self) -> LeaderFlushStats {
        *lock(&self.stats)
    }

    fn quorum(&self) -> Quorum {
        let applied = *lock(&self.applied);
        match *lock(&self.appended) {
            // Raft counts with the last appended membership; while the
            // applied one differs, take the stricter of the two.
            Some(appended) => Quorum {
                followers_suffice: applied.followers_suffice && appended.followers_suffice,
                alone: applied.alone && appended.alone,
            },
            None => applied,
        }
    }

    pub(crate) fn applied_membership(&self, membership: &Membership<NodeId, BasicNode>) {
        *lock(&self.applied) = Quorum::of(membership);
        self.wake.notify_one();
    }

    pub(crate) fn appended_membership(&self, membership: &Membership<NodeId, BasicNode>) {
        *lock(&self.appended) = Some(Quorum::of(membership));
        self.wake.notify_one();
    }

    /// A truncation may have removed the last appended membership.
    pub(crate) fn truncated(&self) {
        *lock(&self.appended) = None;
    }

    /// Raft committed through `committed`.
    pub(crate) fn committed(&self, committed: Option<u64>) {
        let next = committed.map_or(0, |index| index.saturating_add(1));
        if self.committed_next.fetch_max(next, Ordering::AcqRel) < next {
            self.wake.notify_one();
        }
    }

    /// Whether a leader append may commit without an fsync: not in a
    /// group of one, nor once the store stopped flushing.
    pub(crate) fn defers(&self) -> bool {
        !self.stopped.load(Ordering::Acquire) && !self.quorum().alone
    }

    /// Count a leader append committed durably.
    pub(crate) fn direct(&self) {
        lock(&self.stats).direct += 1;
    }

    /// Stop deferring: later leader appends commit durably.
    pub(crate) fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
        self.wake.notify_one();
    }

    pub(crate) fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }

    /// Hold a leader append committed as `seq` without an fsync, until a
    /// durable commit covers it. A flusher that already failed fails it.
    pub(crate) fn hold(
        &self,
        seq: u64,
        entries: std::ops::RangeInclusive<u64>,
        callback: LogFlushed<TypeConfig>,
    ) {
        if let Some(error) = lock(&self.failed).clone() {
            callback.log_io_completed(Err(std::io::Error::other(error)));
            return;
        }
        lock(&self.stats).deferred += 1;
        lock(&self.held).push_back(Held {
            seq,
            first_index: *entries.start(),
            last_index: *entries.end(),
            at: Instant::now(),
            callback,
        });
        self.wake.notify_one();
    }

    /// Whether any append waits for a flush.
    pub(crate) fn holding(&self) -> bool {
        !lock(&self.held).is_empty()
    }

    /// Whether an entry at or below `index` is in an append no durable
    /// commit covers yet: commits through `durable` are durable.
    pub(crate) fn unflushed_through(&self, index: u64, durable: u64) -> bool {
        lock(&self.held)
            .iter()
            .any(|held| held.seq > durable && held.first_index <= index)
    }

    /// Complete every held append that commits through `durable` covered.
    pub(crate) fn release(&self, durable: u64, by_flush: bool) {
        let released: Vec<Held> = {
            let mut held = lock(&self.held);
            let mut released = Vec::new();
            while held.front().is_some_and(|front| front.seq <= durable) {
                released.extend(held.pop_front());
            }
            released
        };
        if released.is_empty() {
            return;
        }
        if !by_flush {
            lock(&self.stats).covered += released.len() as u64;
        }
        for held in released {
            held.callback.log_io_completed(Ok(()));
        }
    }

    /// A new leadership began (its first entry, a blank, is appended):
    /// flush every append at once for [`EAGER_WINDOW`]. It may well need
    /// its own vote (a leader just failed, so a voter may be down), and
    /// a failover must not wait out a grace on top of the election.
    pub(crate) fn leading(&self, now: Instant) {
        let mut until = lock(&self.eager_until);
        *until = (*until).max(Some(now + EAGER_WINDOW));
    }

    /// Count a flush the flusher made.
    pub(crate) fn flushed(&self, grace: bool, now: Instant) {
        let mut stats = lock(&self.stats);
        stats.flushes += 1;
        if grace {
            stats.grace_flushes += 1;
            *lock(&self.eager_until) = Some(now + EAGER_WINDOW);
        }
    }

    /// A flush failed: fail every held append and every later one.
    pub(crate) fn fail(&self, error: String) {
        *lock(&self.failed) = Some(error.clone());
        let held = std::mem::take(&mut *lock(&self.held));
        for held in held {
            held.callback
                .log_io_completed(Err(std::io::Error::other(error.clone())));
        }
    }

    /// When the held appends are due for a flush, if any are held.
    pub(crate) fn due(&self, now: Instant) -> Option<Due> {
        let settings = self.settings();
        let held = lock(&self.held);
        let oldest = held.front()?;
        let eager = lock(&self.eager_until).is_some_and(|until| now < until);
        if !settings.lazy || !self.quorum().followers_suffice || eager || self.is_stopped() {
            return Some(Due {
                at: oldest.at,
                grace: false,
            });
        }
        let latest = oldest.at + settings.interval;
        let committed = self.committed_next.load(Ordering::Acquire);
        let uncommitted = held
            .iter()
            .find(|held| held.last_index >= committed)
            .map(|held| held.at + settings.grace);
        Some(match uncommitted {
            Some(grace) if grace < latest => Due {
                at: grace,
                grace: true,
            },
            _ => Due {
                at: latest,
                grace: false,
            },
        })
    }
}

/// A member's deferred applies: how often the group file is flushed,
/// and how many applies wait for it.
#[derive(Debug, Default)]
pub struct ApplyDurability {
    /// The flush interval in milliseconds; zero: durable applies.
    every_ms: AtomicU64,
    /// Deferred apply transactions since the last flush.
    pending: AtomicU64,
    /// Group file flushes made for deferred applies.
    flushes: AtomicU64,
    /// The highest log index an apply began on: raised before the apply
    /// commits, so a durable commit of the group file never covers an
    /// apply past it.
    applying: AtomicU64,
    /// Wakes the flushing task when the interval changes.
    pub(crate) changed: tokio::sync::Notify,
}

impl ApplyDurability {
    /// Deferred applies flushed every `every`, or durable applies.
    pub fn new(every: Option<Duration>) -> Self {
        let durability = ApplyDurability::default();
        durability.set(every);
        durability
    }

    /// Change the flush interval (`None`: durable applies from now on).
    pub fn set(&self, every: Option<Duration>) {
        let ms = every.map_or(0, |every| {
            u64::try_from(every.as_millis()).unwrap_or(u64::MAX).max(1)
        });
        self.every_ms.store(ms, Ordering::Release);
        self.changed.notify_one();
    }

    /// The flush interval, if applies are deferred.
    pub fn every(&self) -> Option<Duration> {
        match self.every_ms.load(Ordering::Acquire) {
            0 => None,
            ms => Some(Duration::from_millis(ms)),
        }
    }

    /// Whether applies commit deferred.
    pub fn deferred(&self) -> bool {
        self.every().is_some()
    }

    /// `transactions` deferred apply commits happened.
    pub(crate) fn applied(&self, transactions: u64) {
        self.pending.fetch_add(transactions, Ordering::AcqRel);
    }

    /// An apply of the entry at `index` is about to commit.
    pub(crate) fn applying(&self, index: u64) {
        self.applying.fetch_max(index, Ordering::AcqRel);
    }

    /// The highest log index the group file may hold applied.
    pub(crate) fn applying_index(&self) -> u64 {
        self.applying.load(Ordering::Acquire)
    }

    /// Whether deferred applies wait for a flush.
    pub fn pending(&self) -> bool {
        self.pending.load(Ordering::Acquire) > 0
    }

    /// Group flushes made for deferred applies.
    pub fn flushes(&self) -> u64 {
        self.flushes.load(Ordering::Acquire)
    }

    /// Make every deferred apply durable, if any waits: one durable
    /// commit of the group file, after the Raft log's (the store's
    /// before-durable hook). Blocking. Returns whether it flushed.
    pub fn flush(
        &self,
        store: &flower_engine::storage::GroupStore,
    ) -> flower_engine::error::Result<bool> {
        let pending = self.pending.swap(0, Ordering::AcqRel);
        if pending == 0 {
            return Ok(false);
        }
        match store.flush() {
            Ok(()) => {
                self.flushes.fetch_add(1, Ordering::AcqRel);
                Ok(true)
            }
            Err(error) => {
                self.pending.fetch_add(pending, Ordering::AcqRel);
                Err(error)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, BTreeSet};

    fn membership(configs: Vec<Vec<NodeId>>) -> Membership<NodeId, BasicNode> {
        let nodes: BTreeMap<NodeId, BasicNode> = configs
            .iter()
            .flatten()
            .map(|id| (*id, BasicNode::default()))
            .collect();
        let configs: Vec<BTreeSet<NodeId>> = configs
            .into_iter()
            .map(|config| config.into_iter().collect())
            .collect();
        Membership::new(configs, nodes)
    }

    /// Followers alone form a majority only when every configuration of
    /// a joint membership has three voters or more.
    #[test]
    fn the_followers_suffice_only_with_three_voters_in_every_configuration() {
        let of = |configs| Quorum::of(&membership(configs));
        assert!(of(vec![vec![1, 2, 3]]).followers_suffice);
        assert!(!of(vec![vec![1, 2]]).followers_suffice);
        assert!(of(vec![vec![1, 2, 3], vec![1, 2, 3, 4]]).followers_suffice);
        assert!(!of(vec![vec![1, 2], vec![1, 2, 3]]).followers_suffice);
        assert!(!of(vec![vec![1, 2, 3], vec![3, 4]]).followers_suffice);
        assert!(of(vec![vec![1]]).alone);
        assert!(!of(vec![vec![1], vec![1, 2]]).alone);
    }

    #[test]
    fn settings_parse_and_refuse_nonsense() {
        assert_eq!(
            FlushSettings::parse(None, None, None, None).unwrap(),
            FlushSettings::default()
        );
        let settings =
            FlushSettings::parse(Some("immediate"), Some("5"), Some("100"), Some("0")).unwrap();
        assert!(!settings.leader.lazy);
        assert_eq!(settings.leader.grace, Duration::from_millis(5));
        assert_eq!(settings.leader.interval, Duration::from_millis(100));
        assert_eq!(settings.apply_every, None);
        assert!(FlushSettings::parse(Some("sometimes"), None, None, None).is_err());
        assert!(FlushSettings::parse(None, Some("soon"), None, None).is_err());
        assert!(FlushSettings::parse(None, None, Some("0"), None).is_err());
    }
}
