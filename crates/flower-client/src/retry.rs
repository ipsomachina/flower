//! Retry configuration: off by default, bounded with jittered
//! backoff when enabled. Caller cancellation and structured
//! business failures never retry; one mutation identity stays fixed
//! across its retry loop. Refusals for load retry on their own
//! budget, waiting at least what the server asks.

use std::time::{Duration, Instant};

/// Retry selection.
#[derive(Clone, Debug, Default)]
pub enum Retry {
    /// Single attempt.
    #[default]
    Off,
    /// Default policy.
    Default,
    /// Explicit policy.
    Policy(RetryPolicy),
}

impl From<bool> for Retry {
    fn from(enabled: bool) -> Self {
        if enabled { Retry::Default } else { Retry::Off }
    }
}

impl From<RetryPolicy> for Retry {
    fn from(policy: RetryPolicy) -> Self {
        Retry::Policy(policy)
    }
}

/// How long refusals for load retry when a policy sets no `until`: a
/// minute from the first attempt.
pub const DEFAULT_OVERLOAD_BUDGET: Duration = Duration::from_secs(60);

/// Bounded retry policy.
#[derive(Clone, Debug)]
pub struct RetryPolicy {
    /// Total attempts including the first.
    pub attempts: u32,
    /// Initial backoff delay.
    pub initial_delay: Duration,
    /// Maximum backoff delay.
    pub max_delay: Duration,
    /// Per-attempt timeout. A per-call `RequestOptions::timeout`
    /// overrides it; `Retry::Default` attempts use the client's
    /// default attempt timeout instead.
    pub attempt_timeout: Duration,
    /// No retry starts at or after this instant (`now + delay >=
    /// until`); the first attempt always runs.
    pub until: Option<Instant>,
    /// Keep retrying while the server refuses for load (429, or 503
    /// `overloaded`): those refusals never ran the request and do not
    /// count against `attempts`; they end with `until` or, without
    /// one, [`RetryPolicy::overload_budget`] after the first attempt.
    /// Other failures still stop at `attempts`. Default true.
    pub through_overload: bool,
    /// How long refusals for load retry without an `until`. Default
    /// [`DEFAULT_OVERLOAD_BUDGET`].
    pub overload_budget: Duration,
}

impl Default for RetryPolicy {
    /// Eight attempts, equal-jitter exponential backoff from 250 ms
    /// capped at 30 s, a 20 s attempt timeout, and refusals for load
    /// retried for a minute on top.
    fn default() -> Self {
        RetryPolicy {
            attempts: 8,
            initial_delay: Duration::from_millis(250),
            max_delay: Duration::from_secs(30),
            attempt_timeout: Duration::from_secs(20),
            until: None,
            through_overload: true,
            overload_budget: DEFAULT_OVERLOAD_BUDGET,
        }
    }
}

impl RetryPolicy {
    /// Set the attempts, the first included.
    pub fn attempts(mut self, attempts: u32) -> Self {
        self.attempts = attempts;
        self
    }

    /// Set the backoff's first and largest delays.
    pub fn delays(mut self, initial: Duration, max: Duration) -> Self {
        self.initial_delay = initial;
        self.max_delay = max;
        self
    }

    /// Set the per-attempt timeout.
    pub fn attempt_timeout(mut self, timeout: Duration) -> Self {
        self.attempt_timeout = timeout;
        self
    }

    /// Start no retry at or after `until`.
    pub fn until(mut self, until: Instant) -> Self {
        self.until = Some(until);
        self
    }

    /// See [`RetryPolicy::through_overload`]; `false` counts a refusal
    /// for load as any other failure.
    pub fn through_overload(mut self, through: bool) -> Self {
        self.through_overload = through;
        self
    }

    /// See [`RetryPolicy::overload_budget`].
    pub fn overload_budget(mut self, budget: Duration) -> Self {
        self.overload_budget = budget;
        self
    }
}

impl Retry {
    /// Resolve to an optional policy: `None` means one attempt.
    pub(crate) fn policy(&self) -> Option<RetryPolicy> {
        match self {
            Retry::Off => None,
            Retry::Default => Some(RetryPolicy::default()),
            Retry::Policy(policy) => Some(policy.clone()),
        }
    }

    /// The attempt timeout an explicit policy sets, if any.
    pub(crate) fn attempt_timeout(&self) -> Option<Duration> {
        match self {
            Retry::Policy(policy) => Some(policy.attempt_timeout),
            Retry::Off | Retry::Default => None,
        }
    }
}

/// Equal-jitter exponential backoff for an attempt: half the
/// exponential delay plus a uniform draw over the other half.
fn jittered(policy: &RetryPolicy, attempt: u32) -> Duration {
    let exponential = policy
        .initial_delay
        .saturating_mul(2u32.saturating_pow(attempt.min(16)));
    let capped = exponential.min(policy.max_delay);
    let half = capped.as_millis() as u64 / 2;
    let jitter = if half == 0 {
        0
    } else {
        fastrand::u64(0..=half)
    };
    Duration::from_millis(half + jitter)
}

/// The wait before a retry: the backoff, or when the server asked for a
/// wait (`Retry-After`) at least that plus up to half again at random,
/// so refused callers do not come back together. An ask is honoured
/// past `max_delay`, which only caps the backoff.
pub(crate) fn retry_delay(
    policy: &RetryPolicy,
    attempt: u32,
    retry_after_ms: Option<u64>,
) -> Duration {
    let delay = jittered(policy, attempt);
    let Some(asked) = retry_after_ms.map(Duration::from_millis) else {
        return delay;
    };
    let spread = asked.mul_f64(1.0 + fastrand::f64() / 2.0);
    delay.max(spread.min(policy.max_delay.max(asked)))
}

/// Sleep equal-jitter backoff before the next attempt, honoring any
/// `Retry-After` floor.
pub(crate) async fn backoff(policy: &RetryPolicy, attempt: u32, retry_after_ms: Option<u64>) {
    let delay = jittered(policy, attempt).max(Duration::from_millis(retry_after_ms.unwrap_or(0)));
    if !delay.is_zero() {
        tokio::time::sleep(delay).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_after_sets_a_jittered_floor_past_the_cap() {
        let policy =
            RetryPolicy::default().delays(Duration::from_millis(10), Duration::from_secs(1));
        // Refused callers spread out instead of coming back together.
        let spread: Vec<Duration> = (0..100)
            .map(|_| retry_delay(&policy, 0, Some(400)))
            .collect();
        assert!(
            spread
                .iter()
                .any(|delay| *delay > Duration::from_millis(450))
        );
        assert!(
            spread
                .iter()
                .any(|delay| *delay < Duration::from_millis(550))
        );
        for _ in 0..100 {
            let delay = retry_delay(&policy, 0, Some(2_000));
            assert!(delay >= Duration::from_secs(2), "{delay:?}");
            assert!(
                delay <= Duration::from_secs(2),
                "capped at the ask past max: {delay:?}"
            );
            let delay = retry_delay(&policy, 0, Some(400));
            assert!(delay >= Duration::from_millis(400), "{delay:?}");
            assert!(delay <= Duration::from_millis(600), "{delay:?}");
        }
    }

    #[test]
    fn backoff_stays_within_equal_jitter_bounds() {
        let policy =
            RetryPolicy::default().delays(Duration::from_millis(100), Duration::from_millis(800));
        for attempt in 0..6 {
            let ceiling = (100u64 << attempt).min(800);
            for _ in 0..50 {
                let delay = retry_delay(&policy, attempt, None).as_millis() as u64;
                assert!(
                    (ceiling / 2..=ceiling).contains(&delay),
                    "{attempt}: {delay}"
                );
            }
        }
    }
}
