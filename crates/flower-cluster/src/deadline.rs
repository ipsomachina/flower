//! Client deadlines: how long the caller of the request being served
//! waits for its answer.
//!
//! A client states its budget in [`TIMEOUT_HEADER`], whole milliseconds
//! (at most [`MAX_TIMEOUT`]); the server runs the request inside
//! [`scoped`] with the deadline it implies, and every wait on the way
//! that would outlive it ends at it instead: admission refuses a query
//! or writer still queued at its deadline, and CoDel sheds only
//! requests that have one (see [`crate::admission`]). A follower
//! forwarding the request sends the leader what is left of the budget
//! ([`remaining`]), so the deadline holds across the hop. Requests
//! without the header wait as long as admission's own bound.
//!
//! The deadline is task-local: work the request awaits sees it, work it
//! spawns does not.

use std::future::Future;
use std::time::Duration;

use tokio::time::Instant;

/// Request header: the caller's budget for the whole request, in whole
/// milliseconds.
pub const TIMEOUT_HEADER: &str = "x-flower-timeout-ms";
/// The longest budget honored: a day.
pub const MAX_TIMEOUT: Duration = Duration::from_secs(86_400);

tokio::task_local! {
    static DEADLINE: Instant;
}

/// Parse a [`TIMEOUT_HEADER`] value: an integer of milliseconds,
/// surrounding whitespace allowed, capped at [`MAX_TIMEOUT`]. `None`
/// for anything else.
pub fn parse(value: &str) -> Option<Duration> {
    let value = value.trim();
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let ms: u64 = value.parse().unwrap_or(u64::MAX);
    Some(Duration::from_millis(ms).min(MAX_TIMEOUT))
}

/// Run `future` with `deadline` as its client deadline, or with none.
pub async fn scoped<F: Future>(deadline: Option<Instant>, future: F) -> F::Output {
    match deadline {
        Some(deadline) => DEADLINE.scope(deadline, future).await,
        None => future.await,
    }
}

/// The client deadline of the request this task serves, if any.
pub fn current() -> Option<Instant> {
    DEADLINE.try_with(|deadline| *deadline).ok()
}

/// What is left of the client deadline, if there is one: zero once it
/// passed.
pub fn remaining() -> Option<Duration> {
    current().map(|deadline| deadline.saturating_duration_since(Instant::now()))
}

/// The earlier of `bound` from now and the client deadline.
pub fn bounded(bound: Duration) -> Instant {
    let bounded = Instant::now() + bound;
    current().map_or(bounded, |deadline| deadline.min(bounded))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn budgets_parse_as_whole_milliseconds_capped_at_a_day() {
        assert_eq!(parse("250"), Some(Duration::from_millis(250)));
        assert_eq!(parse(" 0 "), Some(Duration::ZERO));
        assert_eq!(parse("999999999999999999999"), Some(MAX_TIMEOUT));
        assert_eq!(
            parse(&(MAX_TIMEOUT.as_millis() + 1).to_string()),
            Some(MAX_TIMEOUT)
        );
        for bad in ["", "-1", "1.5", "1e3", "ten"] {
            assert_eq!(parse(bad), None, "{bad:?}");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn the_deadline_is_the_scopes_and_bounds_waits() {
        assert_eq!(current(), None);
        assert_eq!(remaining(), None);
        let deadline = Instant::now() + Duration::from_millis(300);
        scoped(Some(deadline), async {
            assert_eq!(current(), Some(deadline));
            assert_eq!(remaining(), Some(Duration::from_millis(300)));
            assert_eq!(bounded(Duration::from_secs(10)), deadline);
            let short = bounded(Duration::from_millis(10));
            assert!(short < deadline);
            tokio::time::advance(Duration::from_millis(400)).await;
            assert_eq!(remaining(), Some(Duration::ZERO));
        })
        .await;
        assert_eq!(current(), None);
        scoped(None, async { assert_eq!(current(), None) }).await;
    }
}
