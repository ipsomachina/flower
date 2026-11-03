//! How many items a worker runs at once: a fixed number, or a limit
//! that follows demand and the process's load like a TCP congestion
//! window (v1's `Limiter`). While the queue holds more work than the
//! worker takes, the limit doubles at each adjustment when the process
//! is under half its load, and grows by a sixteenth (at least one) when
//! it is busier or was cut lately. Once the process falls behind (load
//! 1 or more) the limit shrinks by a quarter, at most once a second;
//! when Flower refuses calls for load it halves and claims pause.
//!
//! Time is tokio's, so paused-clock tests drive adjustments exactly.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use tokio::time::Instant;

/// How loaded the process is: 1 where it falls behind, with what loads
/// it most.
#[derive(Clone, Debug, PartialEq)]
pub struct Load {
    /// The load: under 0.5 the limit doubles, up to 1 it grows gently,
    /// from 1 it shrinks.
    pub load: f64,
    /// What loads the process most, for limit-change reasons.
    pub reason: String,
}

impl Load {
    /// A load and its reason.
    pub fn new(load: f64, reason: impl Into<String>) -> Self {
        Load {
            load,
            reason: reason.into(),
        }
    }

    /// No load.
    pub fn idle() -> Self {
        Load::new(0.0, "idle")
    }
}

/// Read at every adjustment: the load since the previous read.
pub trait Health: Send + Sync {
    /// The load now.
    fn load(&self) -> Load;
}

impl<F: Fn() -> Load + Send + Sync> Health for F {
    fn load(&self) -> Load {
        self()
    }
}

/// Bounds of an adaptive limit: `1 <= min <= initial <= max`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Adaptive {
    /// Fewest items at once.
    pub min: usize,
    /// Where the limit starts.
    pub initial: usize,
    /// Most items at once.
    pub max: usize,
}

impl Adaptive {
    /// Bounds from `min` through `max`, starting at `initial`.
    pub fn new(min: usize, initial: usize, max: usize) -> Self {
        Adaptive { min, initial, max }
    }
}

/// What a [`Limiter`] did at its last adjustment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Throttle {
    /// The limit went up: work waits and the process has room.
    Growing,
    /// Nothing to do: no work waits beyond the limit, or the limit is
    /// at its maximum.
    Steady,
    /// The process fell behind: the limit is cut by a quarter.
    Shrinking,
    /// A cut was recent: the limit grows by a sixteenth at most.
    Settling,
    /// Flower refused calls for load: no claims until the pause ends.
    Paused,
}

/// A change of the limit, and why.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LimitChange {
    /// The new limit.
    pub limit: usize,
    /// Why it changed.
    pub reason: String,
}

/// Below this load a pressed limit doubles; up to 1 it grows by a
/// sixteenth.
const HEADROOM: f64 = 0.5;
/// After a cut for load, the limit grows by a sixteenth at most for
/// this long, so it settles under what the process can take.
const SETTLE: Duration = Duration::from_secs(5);
/// After a refusal for load, for longer: admission frees up slowly.
const SETTLE_THROTTLED: Duration = Duration::from_secs(30);
/// Cuts at least this far apart, so a burst of refusals or a busy
/// second cuts once rather than to the floor.
const CUT_EVERY: Duration = Duration::from_secs(1);

/// What an adaptive pool says of its load (v1's `load` event of
/// `ReconcileEvent`): the load its health last read, its limit and
/// bounds, how much of the limit is in use, whether work waited beyond
/// it, and what the limiter did.
#[derive(Clone, Debug, PartialEq)]
pub struct PoolLoad {
    /// The load the last adjustment read: under 0.5 the limit doubles,
    /// up to 1 it grows gently, from 1 it shrinks.
    pub load: f64,
    /// What loads the process most.
    pub reason: String,
    /// Computations the pool may run now.
    pub limit: usize,
    /// The limit's floor.
    pub min: usize,
    /// The limit's ceiling.
    pub max: usize,
    /// Computations running.
    pub in_use: usize,
    /// Work waited beyond the limit since the adjustment before.
    pub waiting: bool,
    /// What the limiter did at its last adjustment.
    pub throttle: Throttle,
}

/// When a pool says its load (v1's gate): at once when the load moves to
/// another band (under 0.5, under 1, from 1) or the limiter starts or
/// stops holding back, at most every five seconds, and once a minute
/// while it works.
#[derive(Debug, Default)]
pub struct LoadGate {
    said_at: Option<Instant>,
    looked_at: Option<Instant>,
    band: u8,
    pressed: bool,
}

impl LoadGate {
    /// Least time between two loads said for a change.
    pub const MIN_GAP: Duration = Duration::from_secs(5);
    /// Longest silence while the pool works.
    pub const HEARTBEAT: Duration = Duration::from_secs(60);

    /// Whether to say `load` now, recording it when so.
    pub fn due(&mut self, now: Instant, load: &PoolLoad) -> bool {
        let looked = *self.looked_at.get_or_insert(now);
        let band = if load.load < HEADROOM {
            0
        } else if load.load < 1.0 {
            1
        } else {
            2
        };
        let pressed = matches!(
            load.throttle,
            Throttle::Shrinking | Throttle::Settling | Throttle::Paused
        );
        let changed = band != self.band || pressed != self.pressed;
        let spaced = self
            .said_at
            .is_none_or(|at| now.duration_since(at) >= Self::MIN_GAP);
        let beat = now.duration_since(self.said_at.unwrap_or(looked)) >= Self::HEARTBEAT;
        let working = load.in_use > 0 || load.waiting;
        let due = (changed && spaced) || (working && beat);
        if due {
            self.said_at = Some(now);
            self.band = band;
            self.pressed = pressed;
        }
        due
    }
}

/// The adaptive limit. A fixed limit is one whose bounds are equal: it
/// never adjusts, though refusals still pause claims.
pub struct Limiter {
    min: usize,
    max: usize,
    current: f64,
    wanted: bool,
    paused_until: Option<Instant>,
    last_cut: Option<Instant>,
    settle_until: Option<Instant>,
    health: Arc<dyn Health>,
    state: Throttle,
    /// The load the last adjustment read.
    last: Option<Load>,
}

impl std::fmt::Debug for Limiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Limiter")
            .field("limit", &self.limit())
            .field("min", &self.min)
            .field("max", &self.max)
            .field("state", &self.state)
            .finish()
    }
}

impl Limiter {
    /// A limiter within `bounds`, reading `health` at each adjustment.
    pub fn new(bounds: Adaptive, health: Arc<dyn Health>) -> Result<Self, String> {
        let Adaptive { min, initial, max } = bounds;
        if min < 1 || max < min || initial < min || initial > max {
            return Err("concurrency needs 1 <= min <= initial <= max".to_string());
        }
        Ok(Limiter {
            min,
            max,
            current: initial as f64,
            wanted: false,
            paused_until: None,
            last_cut: None,
            settle_until: None,
            health,
            state: Throttle::Steady,
            last: None,
        })
    }

    /// A fixed limit.
    pub fn fixed(limit: usize) -> Self {
        let limit = limit.max(1);
        Limiter::new(Adaptive::new(limit, limit, limit), Arc::new(Load::idle))
            .expect("equal bounds of at least one are valid")
    }

    /// Whether the limit never adjusts.
    pub fn is_fixed(&self) -> bool {
        self.min == self.max
    }

    /// Items the worker may hold now.
    pub fn limit(&self) -> usize {
        self.current.floor() as usize
    }

    /// The bounds.
    pub fn bounds(&self) -> (usize, usize) {
        (self.min, self.max)
    }

    /// What the last adjustment did; a pause shows at once.
    pub fn state(&self) -> Throttle {
        if self.pause().is_some() {
            Throttle::Paused
        } else {
            self.state
        }
    }

    /// When claims may resume after a refusal for load, if paused now.
    pub fn pause(&self) -> Option<Instant> {
        self.paused_until.filter(|until| *until > Instant::now())
    }

    /// The queue had more work than the worker had room for.
    pub fn want(&mut self) {
        self.wanted = true;
    }

    /// Flower refused calls for load: claim nothing for `pause`, and
    /// hold fewer items (halved, at most once a second).
    pub fn throttle(&mut self, pause: Duration, reason: &str) -> Option<LimitChange> {
        let until = Instant::now() + pause;
        self.paused_until = Some(self.paused_until.map_or(until, |paused| paused.max(until)));
        self.cut(0.5, reason, SETTLE_THROTTLED)
    }

    /// Once per adjustment interval: follow the process's load and the
    /// demand seen since the last call.
    pub fn adjust(&mut self) -> Option<LimitChange> {
        let wanted = std::mem::take(&mut self.wanted);
        if self.is_fixed() {
            return None;
        }
        let Load { load, reason } = self.health.load();
        self.last = Some(Load::new(load, reason.clone()));
        let before = self.current;
        let change = self.follow(load, &reason, wanted);
        let now = Instant::now();
        self.state = if load >= 1.0 {
            Throttle::Shrinking
        } else if self.pause().is_some() {
            Throttle::Paused
        } else if self.settle_until.is_some_and(|until| now < until) {
            Throttle::Settling
        } else if self.current > before {
            Throttle::Growing
        } else {
            Throttle::Steady
        };
        change
    }

    /// The load the last adjustment read, if any.
    pub fn last_load(&self) -> Option<&Load> {
        self.last.as_ref()
    }

    fn follow(&mut self, load: f64, reason: &str, wanted: bool) -> Option<LimitChange> {
        if load >= 1.0 {
            return self.cut(0.75, reason, SETTLE);
        }
        if !wanted || self.pause().is_some() || self.current >= self.max as f64 {
            return None;
        }
        let before = self.limit();
        let settling = self
            .settle_until
            .is_some_and(|until| Instant::now() < until);
        let grown = if load >= HEADROOM || settling {
            self.current + (self.current / 16.0).max(1.0)
        } else {
            self.current * 2.0
        };
        self.current = grown.min(self.max as f64);
        (self.limit() != before).then(|| LimitChange {
            limit: self.limit(),
            reason: "more work is waiting".to_string(),
        })
    }

    fn cut(&mut self, factor: f64, reason: &str, settle: Duration) -> Option<LimitChange> {
        let now = Instant::now();
        if self.is_fixed() || self.last_cut.is_some_and(|last| now - last < CUT_EVERY) {
            return None;
        }
        self.last_cut = Some(now);
        let until = now + settle;
        self.settle_until = Some(self.settle_until.map_or(until, |settle| settle.max(until)));
        let before = self.limit();
        self.current = (self.current * factor).max(self.min as f64);
        (self.limit() != before).then(|| LimitChange {
            limit: self.limit(),
            reason: reason.to_string(),
        })
    }
}

/// The load of this process: how busy the tokio runtime's worker
/// threads were since the last read (the share of their time spent
/// running tasks), and on Linux how full memory is (resident memory
/// against what the process may still use). `busy` and `memory` are
/// the shares at which the process counts as behind; defaults 0.9 and
/// 0.85, as v1.
pub struct ProcessHealth {
    busy: f64,
    memory: f64,
    runtime: Option<tokio::runtime::Handle>,
    last: Mutex<Option<(std::time::Instant, Duration)>>,
}

impl ProcessHealth {
    /// Health with the default limits, for the runtime it is made on.
    pub fn new() -> Self {
        Self::with_limits(0.9, 0.85)
    }

    /// Health counting the process behind at these shares.
    pub fn with_limits(busy: f64, memory: f64) -> Self {
        let runtime = tokio::runtime::Handle::try_current().ok();
        let last = runtime
            .as_ref()
            .map(|handle| (std::time::Instant::now(), busy_total(handle)));
        ProcessHealth {
            busy: if busy > 0.0 { busy } else { 0.9 },
            memory: if memory > 0.0 { memory } else { 0.85 },
            runtime,
            last: Mutex::new(last),
        }
    }
}

impl Default for ProcessHealth {
    fn default() -> Self {
        Self::new()
    }
}

/// Busy time across the runtime's workers.
fn busy_total(handle: &tokio::runtime::Handle) -> Duration {
    let metrics = handle.metrics();
    (0..metrics.num_workers())
        .map(|worker| metrics.worker_total_busy_duration(worker))
        .sum()
}

impl Health for ProcessHealth {
    fn load(&self) -> Load {
        let mut most = Load::idle();
        if let Some(handle) = &self.runtime {
            let mut last = self.last.lock().unwrap_or_else(PoisonError::into_inner);
            if let Some((at, busy)) = *last {
                let now = std::time::Instant::now();
                let total = busy_total(handle);
                let wall = now.duration_since(at).as_secs_f64()
                    * handle.metrics().num_workers().max(1) as f64;
                let share = if wall > 0.0 {
                    (total.saturating_sub(busy).as_secs_f64() / wall).clamp(0.0, 1.0)
                } else {
                    0.0
                };
                *last = Some((now, total));
                if share / self.busy > most.load {
                    most = Load::new(
                        share / self.busy,
                        format!("runtime {}% busy", (share * 100.0).round()),
                    );
                }
            }
        }
        if let Some(used) = memory_share()
            && used / self.memory > most.load
        {
            most = Load::new(
                used / self.memory,
                format!("memory {}% used", (used * 100.0).round()),
            );
        }
        most
    }
}

/// Resident memory as a share of resident plus available, on Linux.
#[cfg(target_os = "linux")]
fn memory_share() -> Option<f64> {
    let field = |text: &str, name: &str| -> Option<f64> {
        let line = text.lines().find(|line| line.starts_with(name))?;
        let kib: f64 = line[name.len()..]
            .trim()
            .trim_end_matches("kB")
            .trim()
            .parse()
            .ok()?;
        Some(kib * 1024.0)
    };
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
    let resident = field(&status, "VmRSS:")?;
    let available = field(&meminfo, "MemAvailable:")?;
    (resident + available > 0.0).then(|| resident / (resident + available))
}

/// No memory reading off Linux.
#[cfg(not(target_os = "linux"))]
fn memory_share() -> Option<f64> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn health(load: Arc<Mutex<f64>>) -> Arc<dyn Health> {
        Arc::new(move || Load::new(*load.lock().unwrap(), "test load"))
    }

    #[tokio::test(start_paused = true)]
    async fn the_limit_doubles_with_headroom_and_grows_gently_when_pressed() {
        let load = Arc::new(Mutex::new(0.0));
        let mut limiter = Limiter::new(Adaptive::new(1, 1, 64), health(load.clone())).unwrap();
        // No demand, no growth.
        assert_eq!(limiter.adjust(), None);
        assert_eq!(limiter.limit(), 1);
        for expected in [2, 4, 8, 16] {
            limiter.want();
            assert_eq!(limiter.adjust().unwrap().limit, expected);
            assert_eq!(limiter.state(), Throttle::Growing);
        }
        *load.lock().unwrap() = 0.7;
        limiter.want();
        assert_eq!(limiter.adjust().unwrap().limit, 17);
    }

    #[tokio::test(start_paused = true)]
    async fn a_process_behind_cuts_by_a_quarter_once_a_second_then_settles() {
        let load = Arc::new(Mutex::new(0.0));
        let mut limiter = Limiter::new(Adaptive::new(2, 16, 32), health(load.clone())).unwrap();
        *load.lock().unwrap() = 1.2;
        let change = limiter.adjust().unwrap();
        assert_eq!(change.limit, 12);
        assert_eq!(change.reason, "test load");
        assert_eq!(limiter.state(), Throttle::Shrinking);
        // Within a second: no second cut.
        assert_eq!(limiter.adjust(), None);
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(limiter.adjust().unwrap().limit, 9);
        // Recovered but settling: gentle growth only.
        *load.lock().unwrap() = 0.0;
        limiter.want();
        assert_eq!(limiter.adjust().unwrap().limit, 10);
        assert_eq!(limiter.state(), Throttle::Settling);
        tokio::time::advance(SETTLE).await;
        limiter.want();
        assert_eq!(limiter.adjust().unwrap().limit, 20);
        // Never below the floor.
        *load.lock().unwrap() = 5.0;
        for _ in 0..20 {
            tokio::time::advance(CUT_EVERY).await;
            limiter.adjust();
        }
        assert_eq!(limiter.limit(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn refusals_for_load_halve_the_limit_and_pause_claims() {
        let mut limiter = Limiter::new(Adaptive::new(1, 8, 16), Arc::new(Load::idle)).unwrap();
        let change = limiter
            .throttle(Duration::from_secs(1), "shedding")
            .unwrap();
        assert_eq!(change.limit, 4);
        assert!(limiter.pause().is_some());
        assert_eq!(limiter.state(), Throttle::Paused);
        // Paused: demand does not grow it.
        limiter.want();
        assert_eq!(limiter.adjust(), None);
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(limiter.pause().is_none());
        // A fixed limit pauses but never changes.
        let mut fixed = Limiter::fixed(4);
        assert_eq!(fixed.throttle(Duration::from_secs(1), "shedding"), None);
        assert!(fixed.pause().is_some());
        assert_eq!(fixed.limit(), 4);
        fixed.want();
        assert_eq!(fixed.adjust(), None);
    }

    #[test]
    fn bounds_are_checked() {
        let idle: Arc<dyn Health> = Arc::new(Load::idle);
        assert!(Limiter::new(Adaptive::new(0, 1, 2), idle.clone()).is_err());
        assert!(Limiter::new(Adaptive::new(2, 1, 4), idle.clone()).is_err());
        assert!(Limiter::new(Adaptive::new(1, 5, 4), idle.clone()).is_err());
        assert!(Limiter::new(Adaptive::new(1, 1, 1), idle).is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn process_health_reads_the_runtime() {
        let health = ProcessHealth::new();
        tokio::time::sleep(Duration::from_millis(20)).await;
        let load = health.load();
        assert!(load.load >= 0.0 && load.load.is_finite(), "{load:?}");
    }
}
