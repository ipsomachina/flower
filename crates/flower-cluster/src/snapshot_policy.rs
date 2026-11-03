//! Snapshot triggers beyond OpenRaft's entry count: log bytes applied
//! since the last snapshot, and the last snapshot's age, both behind a
//! measured duty-cycle cooldown.
//!
//! OpenRaft builds a snapshot every [`SnapshotSettings::logs_since_last`]
//! entries whatever this scheduler says: that hard trigger bounds the
//! log. [`Node::run_snapshots`] adds soft ones, checked every
//! [`SnapshotPolicy::check`]: a snapshot is due once the entries applied
//! since the last one carry [`SnapshotPolicy::after_bytes`] (large
//! entries fill the log long before their count does), or once the last
//! one is [`SnapshotPolicy::max_age`] old and anything applied since
//! (followers and devices then catch up from a recent image, and the
//! log behind it can go). A soft trigger waits until the last build,
//! whatever triggered it, is far enough behind: a build that took `d`
//! is followed by at least `d × (100 − duty) / duty` without a soft one,
//! so snapshot work takes at most [`SnapshotPolicy::duty_percent`] of
//! the time however large the state grows.
//!
//! Members stagger their age-triggered builds. Every member of a group
//! usually starts at the same time (a deploy), so ages alone would make
//! them all build, and then purge, at once, every max age. The max age
//! divides into [`STAGGER_SLOTS`] windows of wall-clock time, and a
//! member builds for age only in the window of its node ID (modulo the
//! slot count), once its snapshot is at least half the max age old; past
//! one and a half max ages and a window it builds whatever the window.
//! With 300 s, members 1, 2 and 3 build 37.5 s apart.
//!
//! The byte count is an estimate from each entry's payload sizes, the
//! same on apply and when a member reopens and counts the entries
//! applied since its snapshot.
//!
//! [`SnapshotSettings::logs_since_last`]: crate::timing::SnapshotSettings::logs_since_last
//! [`Node::run_snapshots`]: crate::node::Node::run_snapshots

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use openraft::{Entry, EntryPayload};

use crate::types::{ClusterWrite, NodeId, TypeConfig};

/// The windows a max age divides into: members whose IDs differ modulo
/// this never build for age at the same time.
pub const STAGGER_SLOTS: u64 = 8;

/// Soft snapshot triggers. Use the same settings on every member.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SnapshotPolicy {
    /// `FLOWER_SNAPSHOT_AFTER_BYTES`: build once the entries applied
    /// since the last snapshot carry this many bytes; 0 disables.
    pub after_bytes: u64,
    /// `FLOWER_SNAPSHOT_MAX_AGE_MS`: build once the last snapshot is
    /// this old and anything applied since; zero disables.
    pub max_age: Duration,
    /// `FLOWER_SNAPSHOT_CHECK_MS`: how often the triggers are checked.
    pub check: Duration,
    /// `FLOWER_SNAPSHOT_DUTY_PERCENT`: the most of the time snapshot
    /// builds may take, 1 to 100.
    pub duty_percent: u8,
}

impl Default for SnapshotPolicy {
    fn default() -> Self {
        SnapshotPolicy {
            after_bytes: 16 * 1024 * 1024,
            max_age: Duration::from_secs(300),
            check: Duration::from_secs(1),
            duty_percent: 10,
        }
    }
}

impl SnapshotPolicy {
    /// Reject settings the scheduler cannot honor.
    pub fn validate(&self) -> Result<(), String> {
        if !(1..=100).contains(&self.duty_percent) {
            return Err("FLOWER_SNAPSHOT_DUTY_PERCENT must be in 1..=100".to_string());
        }
        if self.check.is_zero() {
            return Err("FLOWER_SNAPSHOT_CHECK_MS must be positive".to_string());
        }
        if Instant::now()
            .checked_add(self.max_age.saturating_mul(2))
            .is_none()
        {
            return Err("FLOWER_SNAPSHOT_MAX_AGE_MS exceeds this platform's deadline range".into());
        }
        Ok(())
    }

    /// Whether any soft trigger is on.
    pub fn enabled(&self) -> bool {
        self.after_bytes > 0 || !self.max_age.is_zero()
    }
}

/// The estimated log bytes of one entry: its payload's byte fields plus
/// a fixed overhead for framing and log metadata.
pub fn entry_bytes(entry: &Entry<TypeConfig>) -> u64 {
    const OVERHEAD: usize = 64;
    let payload = match &entry.payload {
        EntryPayload::Blank => 0,
        EntryPayload::Membership(membership) => 32 * membership.nodes().count(),
        EntryPayload::Normal(write) => match write {
            ClusterWrite::Patch { db, patch, .. } => db.len() + patch.len(),
            ClusterWrite::MultiPatch { patches, .. } => patches.len(),
            ClusterWrite::Deploy {
                db,
                module,
                wasm,
                manifest,
                bindings,
                ..
            } => {
                let bindings: usize = bindings.iter().map(|(name, _)| name.len() + 4).sum();
                db.len() + module.len() + wasm.len() + manifest.len() + bindings
            }
            ClusterWrite::Incarnation { .. } => 16,
            ClusterWrite::Batch { items, .. } => {
                items.iter().map(|item| 16 + item.len()).sum::<usize>() + 48
            }
            ClusterWrite::Keys { change, .. } => change.len(),
            ClusterWrite::Remove { db, module, .. } => db.len() + module.len() + 16,
        },
    };
    (OVERHEAD + payload) as u64
}

/// What a member applied and snapshotted, and its builds' cost: the
/// state machine reports into it, the scheduler reads it.
pub struct Accounting {
    node: NodeId,
    state: Mutex<State>,
}

struct State {
    /// Estimated bytes of every entry applied in this process, plus
    /// those applied since the snapshot before it opened.
    total: u64,
    /// `total` as of the last published snapshot's capture.
    snapshotted: u64,
    applied: Option<u64>,
    snapshot: Option<u64>,
    /// When the last snapshot was published or installed (or the
    /// member opened).
    completed: Instant,
    /// When the last build ended, and how long it took.
    last_finished: Instant,
    duration: Duration,
    active: usize,
    builds: u64,
}

/// The applied byte count a build captured, so bytes applied while it
/// ran stay counted after it publishes.
#[derive(Clone, Copy, Debug)]
pub struct Capture {
    total: u64,
}

/// One build in progress; dropping the last clone ends it and records
/// its duration. The blocking work holds a clone, so a cancelled waiter
/// doesn't end the build early.
#[derive(Clone)]
pub struct Activity {
    _timer: Arc<Timer>,
}

struct Timer {
    accounting: Arc<Accounting>,
    started: Instant,
}

impl Drop for Timer {
    fn drop(&mut self) {
        let mut state = self.accounting.lock();
        state.active -= 1;
        state.last_finished = Instant::now();
        state.duration = self.started.elapsed();
        state.builds = state.builds.saturating_add(1);
    }
}

/// The scheduler's view, for metrics.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotPolicyStats {
    /// Estimated log bytes applied since the last snapshot's capture.
    pub unsnapshotted_bytes: u64,
    /// The last applied index.
    pub applied: Option<u64>,
    /// The last snapshot's index.
    pub snapshot: Option<u64>,
    /// Age of the last snapshot (or of the process, before one).
    pub snapshot_age: Duration,
    /// How long the last build took.
    pub last_build: Duration,
    /// The wait after it before a soft trigger may fire.
    pub cooldown: Duration,
    /// Builds running now.
    pub active: usize,
    /// Builds finished (or abandoned) in this process.
    pub builds: u64,
    /// This member's age window.
    pub stagger_slot: u64,
    /// The soft trigger that is due now, if any.
    pub due: Option<&'static str>,
}

impl Accounting {
    /// Accounting for member `node` that opened with `bytes` applied
    /// since its snapshot at `snapshot`, applied through `applied`.
    pub fn new(node: NodeId, bytes: u64, applied: Option<u64>, snapshot: Option<u64>) -> Self {
        let now = Instant::now();
        Accounting {
            node,
            state: Mutex::new(State {
                total: bytes,
                snapshotted: 0,
                applied,
                snapshot,
                completed: now,
                last_finished: now,
                duration: Duration::ZERO,
                active: 0,
                builds: 0,
            }),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().expect("snapshot accounting poisoned")
    }

    /// Entries carrying `bytes` applied, through `index`.
    pub fn applied(&self, bytes: u64, index: Option<u64>) {
        let mut state = self.lock();
        state.total = state.total.saturating_add(bytes);
        state.applied = state.applied.max(index);
    }

    /// A build begins.
    pub fn begin(self: &Arc<Self>) -> Activity {
        self.lock().active += 1;
        Activity {
            _timer: Arc::new(Timer {
                accounting: self.clone(),
                started: Instant::now(),
            }),
        }
    }

    /// The applied byte count at the moment a build pins its state.
    pub fn capture(&self) -> Capture {
        Capture {
            total: self.lock().total,
        }
    }

    /// A build of `capture` covering `index` was published.
    pub fn published(&self, capture: Capture, index: Option<u64>) {
        let mut state = self.lock();
        if index < state.snapshot {
            return;
        }
        state.snapshotted = state.snapshotted.max(capture.total);
        state.snapshot = index;
        state.completed = Instant::now();
    }

    /// A snapshot covering `index` was installed: everything before it
    /// came with it.
    pub fn installed(&self, index: Option<u64>) {
        let mut state = self.lock();
        state.snapshotted = state.total;
        state.applied = state.applied.max(index);
        state.snapshot = index;
        state.completed = Instant::now();
    }

    /// The soft trigger due at `now` (wall clock `wall_ms`), if any.
    pub fn due(&self, policy: &SnapshotPolicy, now: Instant, wall_ms: u64) -> Option<&'static str> {
        self.lock().due(self.node, policy, now, wall_ms)
    }

    /// The scheduler's view.
    pub fn stats(&self, policy: &SnapshotPolicy) -> SnapshotPolicyStats {
        let now = Instant::now();
        let state = self.lock();
        SnapshotPolicyStats {
            unsnapshotted_bytes: state.total.saturating_sub(state.snapshotted),
            applied: state.applied,
            snapshot: state.snapshot,
            snapshot_age: now.saturating_duration_since(state.completed),
            last_build: state.duration,
            cooldown: cooldown(state.duration, policy.duty_percent),
            active: state.active,
            builds: state.builds,
            stagger_slot: self.node % STAGGER_SLOTS,
            due: state.due(self.node, policy, now, crate::exec::now_ms()),
        }
    }
}

impl State {
    fn due(
        &self,
        node: NodeId,
        policy: &SnapshotPolicy,
        now: Instant,
        wall_ms: u64,
    ) -> Option<&'static str> {
        if self.active > 0
            || self.applied <= self.snapshot
            || now.saturating_duration_since(self.last_finished)
                < cooldown(self.duration, policy.duty_percent)
        {
            return None;
        }
        if policy.after_bytes > 0
            && self.total.saturating_sub(self.snapshotted) >= policy.after_bytes
        {
            Some("applied_bytes")
        } else if !policy.max_age.is_zero()
            && age_due(
                node,
                now.saturating_duration_since(self.completed),
                policy.max_age,
                wall_ms,
            )
        {
            Some("age")
        } else {
            None
        }
    }
}

/// The wait after a build that took `duration` for snapshot work to
/// stay within `percent` of the time.
fn cooldown(duration: Duration, percent: u8) -> Duration {
    let percent = u32::from(percent.clamp(1, 100));
    duration.saturating_mul(100 - percent) / percent
}

/// Whether a snapshot `age` old is due for age on `node` at wall-clock
/// time `wall_ms` (module documentation).
fn age_due(node: NodeId, age: Duration, max_age: Duration, wall_ms: u64) -> bool {
    let window = (max_age / STAGGER_SLOTS as u32)
        .as_millis()
        .min(u128::from(u64::MAX)) as u64;
    if window == 0 {
        return age >= max_age;
    }
    if age < max_age / 2 {
        return false;
    }
    let window_duration = Duration::from_millis(window);
    if age >= (max_age * 3 / 2).saturating_add(window_duration) {
        // Only a wall clock that stalled or jumped gets here.
        return true;
    }
    (wall_ms / window) % STAGGER_SLOTS == node % STAGGER_SLOTS
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy(after_bytes: u64, max_age: Duration, duty_percent: u8) -> SnapshotPolicy {
        SnapshotPolicy {
            after_bytes,
            max_age,
            duty_percent,
            ..SnapshotPolicy::default()
        }
    }

    #[test]
    fn bytes_applied_while_a_build_runs_stay_counted() {
        let policy = policy(100, Duration::from_secs(60), 100);
        let accounting = Arc::new(Accounting::new(1, 0, None, None));
        accounting.applied(100, Some(1));
        assert_eq!(
            accounting.due(&policy, Instant::now(), 0),
            Some("applied_bytes")
        );
        let activity = accounting.begin();
        let capture = accounting.capture();
        accounting.applied(25, Some(2));
        assert_eq!(accounting.due(&policy, Instant::now(), 0), None, "building");
        accounting.published(capture, Some(1));
        drop(activity);
        let stats = accounting.stats(&policy);
        assert_eq!(stats.unsnapshotted_bytes, 25);
        assert_eq!((stats.snapshot, stats.applied), (Some(1), Some(2)));
        assert_eq!((stats.active, stats.builds), (0, 1));
        // An older build published late moves nothing back.
        accounting.published(Capture { total: 0 }, Some(0));
        assert_eq!(accounting.stats(&policy).snapshot, Some(1));
        accounting.installed(Some(50));
        let stats = accounting.stats(&policy);
        assert_eq!((stats.unsnapshotted_bytes, stats.snapshot), (0, Some(50)));
        assert_eq!(
            accounting.due(&policy, Instant::now() + Duration::from_secs(600), 0),
            None,
            "nothing applied since: no rebuild however old"
        );
    }

    #[test]
    fn soft_triggers_wait_out_the_duty_cooldown_and_can_be_disabled() {
        let now = Instant::now();
        let accounting = Accounting::new(1, 200, Some(2), Some(1));
        {
            let mut state = accounting.lock();
            state.completed = now;
            state.last_finished = now;
            state.duration = Duration::from_secs(2);
        }
        assert_eq!(cooldown(Duration::from_secs(2), 20), Duration::from_secs(8));
        let mut policy = policy(100, Duration::from_secs(1), 20);
        assert_eq!(
            accounting.due(&policy, now + Duration::from_secs(7), 0),
            None,
            "an overdue soft trigger still waits out the cooldown"
        );
        assert_eq!(
            accounting.due(&policy, now + Duration::from_secs(8), 0),
            Some("applied_bytes")
        );
        policy.after_bytes = 0;
        assert_eq!(
            accounting.due(&policy, now + Duration::from_secs(8), 0),
            Some("age")
        );
        policy.max_age = Duration::ZERO;
        assert!(!policy.enabled());
        assert_eq!(
            accounting.due(&policy, now + Duration::from_secs(800), 0),
            None
        );
        policy.after_bytes = 100;
        policy.duty_percent = 100;
        assert_eq!(accounting.due(&policy, now, 0), Some("applied_bytes"));
    }

    #[test]
    fn members_build_for_age_in_their_own_windows_never_together() {
        let max_age = Duration::from_secs(6 * 3600);
        let window_ms = (max_age / STAGGER_SLOTS as u32).as_millis() as u64;
        for wall in (0..STAGGER_SLOTS).map(|slot| slot * window_ms) {
            let young = max_age / 2 - Duration::from_secs(1);
            assert!(!age_due(1, young, max_age, wall), "too young");
        }
        // Three members started together: from the same age on, each
        // builds only in its own window.
        let start = 1_760_000_000_000;
        let due_at = |node: u64| {
            (0..)
                .map(|minute: u64| start + minute * 60_000)
                .find(|&wall| {
                    let age = Duration::from_millis(wall - start) + max_age / 2;
                    age_due(node, age, max_age, wall)
                })
                .unwrap()
        };
        let mut times = [1, 2, 3].map(due_at);
        times.sort();
        for pair in times.windows(2) {
            assert_ne!(pair[0] / window_ms, pair[1] / window_ms, "shared window");
        }
        for node in [1, 2, 3] {
            let wall = due_at(node);
            assert_eq!((wall / window_ms) % STAGGER_SLOTS, node % STAGGER_SLOTS);
            assert!(wall - start <= max_age.as_millis() as u64);
        }
        // A clock that never reaches the window still builds past the cap.
        let other = 2 * window_ms;
        assert!(!age_due(1, max_age, max_age, other));
        let window = Duration::from_millis(window_ms);
        assert!(age_due(1, max_age * 3 / 2 + window, max_age, other));
        // Tiny max ages (tests) aren't staggered.
        let tiny = Duration::from_millis(5);
        assert!(age_due(1, tiny, tiny, 0));
    }

    #[test]
    fn a_cancelled_waiter_leaves_the_build_to_its_worker() {
        let accounting = Arc::new(Accounting::new(1, 100, Some(1), None));
        let policy = SnapshotPolicy::default();
        let caller = accounting.begin();
        let worker = caller.clone();
        drop(caller);
        assert_eq!(accounting.stats(&policy).active, 1);
        drop(worker);
        let stats = accounting.stats(&policy);
        assert_eq!((stats.active, stats.builds), (0, 1));
    }

    #[test]
    fn policies_validate() {
        assert!(SnapshotPolicy::default().validate().is_ok());
        for bad in [
            SnapshotPolicy {
                duty_percent: 0,
                ..SnapshotPolicy::default()
            },
            SnapshotPolicy {
                duty_percent: 101,
                ..SnapshotPolicy::default()
            },
            SnapshotPolicy {
                check: Duration::ZERO,
                ..SnapshotPolicy::default()
            },
            SnapshotPolicy {
                max_age: Duration::MAX,
                ..SnapshotPolicy::default()
            },
        ] {
            assert!(bad.validate().is_err(), "{bad:?}");
        }
    }
}
