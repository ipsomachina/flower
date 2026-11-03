//! Timer inspection and maintenance: stats, get, list, cancel,
//! retry and purge over any transport. Missing timers read as
//! `None` or `false`; other failures raise.

use crate::admin::encode_query;
use crate::client::{Envelope, FlowerClient, RequestOptions};
use crate::error::FlowerError;
use crate::work::{get, post};

/// A stored timer for inspection.
#[derive(serde::Deserialize, Clone, Debug)]
pub struct TimerInfo {
    /// Stable timer ID.
    pub id: String,
    /// Handler definition name.
    pub handler: String,
    /// Scheduled arguments.
    pub args: serde_json::Value,
    /// Due time in milliseconds.
    pub due_ms: u64,
    /// Fires so far.
    pub attempts: u32,
    /// Attempts before death.
    pub max_attempts: u32,
    /// `pending`, `dead` or `held` (its module removed).
    pub state: String,
}

/// Live counts for one database's timers.
#[derive(serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct TimerStats {
    /// Timers awaiting their due time.
    #[serde(default)]
    pub pending: usize,
    /// Exhausted timers awaiting purge or retry.
    #[serde(default)]
    pub dead: usize,
    /// Timers of removed modules, waiting for them.
    #[serde(default)]
    pub held: usize,
}

/// Stored timer.
#[derive(serde::Deserialize, Clone, Debug)]
struct ItemResponse {
    /// The timer, when found.
    #[serde(default)]
    item: Option<TimerInfo>,
}

/// Timer list.
#[derive(serde::Deserialize, Clone, Debug)]
struct ListResponse {
    /// Timers, due-first.
    #[serde(default)]
    timers: Vec<TimerInfo>,
}

/// Cancel outcome.
#[derive(serde::Deserialize, Clone, Debug)]
struct CancelResponse {
    /// Whether a timer existed.
    #[serde(default)]
    removed: bool,
}

/// Purge outcome.
#[derive(serde::Deserialize, Clone, Debug)]
struct PurgeResponse {
    /// Timers removed.
    #[serde(default)]
    removed: usize,
}

impl FlowerClient {
    /// Count pending and dead timers.
    pub async fn timer_stats(
        &self,
        db: &str,
        options: &RequestOptions,
    ) -> Result<TimerStats, FlowerError> {
        let path = format!("/v1/timer/stats?db={}", encode_query(db));
        let response: TimerStats = get(self, &path, options).await?;
        Ok(response)
    }

    /// Read one timer, including dead ones. Missing timers read as
    /// `None`; other failures raise.
    pub async fn get_timer(
        &self,
        db: &str,
        id: &str,
        options: &RequestOptions,
    ) -> Result<Option<TimerInfo>, FlowerError> {
        let path = format!(
            "/v1/timer/get?db={}&id={}",
            encode_query(db),
            encode_query(id)
        );
        match get::<ItemResponse>(self, &path, options).await {
            Ok(response) => Ok(response.item),
            Err(FlowerError::Business { code, .. }) if code == "UNKNOWN_TIMER" => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// List timers due-first. `state` selects `pending` (default),
    /// `dead` or `all`.
    pub async fn list_timers(
        &self,
        db: &str,
        state: Option<&str>,
        limit: Option<usize>,
        options: &RequestOptions,
    ) -> Result<Vec<TimerInfo>, FlowerError> {
        let mut path = format!("/v1/timer/list?db={}", encode_query(db));
        if let Some(state) = state {
            path.push_str(&format!("&state={}", encode_query(state)));
        }
        if let Some(limit) = limit {
            path.push_str(&format!("&limit={limit}"));
        }
        let response: ListResponse = get(self, &path, options).await?;
        Ok(response.timers)
    }

    /// Cancel a timer, returning whether one existed.
    pub async fn cancel_timer(
        &self,
        db: &str,
        id: &str,
        options: &RequestOptions,
    ) -> Result<bool, FlowerError> {
        let body = serde_json::json!({
            "db": db,
            "id": id,
        });
        let response: CancelResponse = post(self, "/v1/timer/cancel", &body, options).await?;
        Ok(response.removed)
    }

    /// Requeue a dead timer with a fresh attempt budget.
    pub async fn retry_timer(
        &self,
        db: &str,
        id: &str,
        delay_ms: Option<u64>,
        options: &RequestOptions,
    ) -> Result<(), FlowerError> {
        let body = serde_json::json!({
            "db": db,
            "id": id,
            "delay_ms": delay_ms,
        });
        let _: Envelope = post(self, "/v1/timer/retry", &body, options).await?;
        Ok(())
    }

    /// Delete dead timers scheduled before a cutoff, returning the
    /// count removed.
    pub async fn purge_timers(
        &self,
        db: &str,
        older_than_ms: Option<u64>,
        options: &RequestOptions,
    ) -> Result<usize, FlowerError> {
        let body = serde_json::json!({
            "db": db,
            "older_than_ms": older_than_ms,
        });
        let response: PurgeResponse = post(self, "/v1/timer/purge", &body, options).await?;
        Ok(response.removed)
    }
}
