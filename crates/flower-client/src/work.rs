//! Work-queue calls: poll, heartbeat, complete, fail, release,
//! cancel, retry, purge, stats and item inspection over any
//! transport. Queue failures are business failures; forwards name
//! the leader and never retry on one URL.

use serde::de::DeserializeOwned;

use crate::admin::encode_query;
use crate::client::{Envelope, FlowerClient, Method, RequestOptions, decode_ok};
use crate::error::FlowerError;

/// A claimed item handed to a worker.
#[derive(serde::Deserialize, Clone, Debug)]
pub struct Claim {
    /// Item ID.
    pub id: String,
    /// Guest payload.
    pub payload: serde_json::Value,
    /// Claim token for report operations.
    pub token: String,
    /// Claims so far, including this one.
    pub attempts: u32,
    /// The fencing token: larger than every token issued before it in
    /// this queue and scope, so a downstream system can refuse writes
    /// from a holder whose lease was taken over (0 from servers before
    /// fences).
    #[serde(default)]
    pub fence: u64,
    /// The item's scope (`""` is the default scope).
    #[serde(default)]
    pub scope: String,
    /// The item's priority: lower values claim first.
    #[serde(default)]
    pub priority: i64,
    /// When the lease lapses unless renewed, in server milliseconds.
    #[serde(default)]
    pub expires_ms: u64,
    /// The job's name, when it was enqueued under one.
    #[serde(default)]
    pub name: Option<String>,
    /// The job's group, if any.
    #[serde(default)]
    pub group: Option<String>,
    /// When the claim was taken, in server milliseconds.
    #[serde(default)]
    pub claimed_ms: u64,
}

/// A stored item for inspection.
#[derive(serde::Deserialize, Clone, Debug)]
pub struct QueueItem {
    /// Item ID.
    pub id: String,
    /// Guest payload.
    pub payload: serde_json::Value,
    /// `pending`, `claimed`, `dead` or `completed`.
    pub state: String,
    /// Claims so far.
    pub attempts: u32,
    /// Attempts before death.
    pub max_attempts: u32,
    /// Enqueue time in milliseconds.
    pub enqueued_ms: u64,
    /// Due time in milliseconds.
    pub due_ms: u64,
    /// The item's scope.
    #[serde(default)]
    pub scope: String,
    /// The item's priority: lower values claim first.
    #[serde(default)]
    pub priority: i64,
    /// The fencing token of its latest claim, 0 before any.
    #[serde(default)]
    pub fence: u64,
    /// The current claim's owner, when the claimer named itself.
    #[serde(default)]
    pub owner: Option<String>,
    /// The job's name, when it was enqueued under one.
    #[serde(default)]
    pub name: Option<String>,
    /// The job's group, if any.
    #[serde(default)]
    pub group: Option<String>,
    /// Its place among its priority's turns.
    #[serde(default)]
    pub turn: u64,
    /// The result its completion kept, for a completed job.
    #[serde(default)]
    pub result: serde_json::Value,
}

/// Live counts for one queue.
#[derive(serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct QueueStats {
    /// Due-or-waiting items.
    pub pending: usize,
    /// Items under claim.
    pub claimed: usize,
    /// Exhausted items awaiting purge.
    pub dead: usize,
    /// Completed items kept with their results.
    pub completed: usize,
}

/// Whether a claim would take an item now, and when that next changes.
#[derive(serde::Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub struct QueueReadiness {
    /// Whether a claim (by the owner asked about, in turn in the line)
    /// would take an item now.
    #[serde(default)]
    pub ready: bool,
    /// When that next changes, in server milliseconds.
    #[serde(default)]
    pub next_ms: Option<u64>,
}

/// A claim's parameters beyond its database and queue.
#[derive(Clone, Copy, Debug, Default)]
pub struct QueuePoll<'a> {
    /// The scope to claim from (`""` is the default scope).
    pub scope: &'a str,
    /// The claimer, recorded on each lease.
    pub owner: Option<&'a str>,
    /// Most items to claim (the server's default 10 when `None`).
    pub limit: Option<usize>,
    /// Lease length (the server's default 30 s when `None`).
    pub lease_ms: Option<u64>,
    /// Wait in the scope's line this long when the claim comes back
    /// short (needs an owner).
    pub wait_ms: Option<u64>,
}

/// How a report settles a claim.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum QueueReport<'a> {
    /// Complete it: kept with the result when given (checked against
    /// the queue's declared result contracts), deleted otherwise.
    Complete(Option<&'a serde_json::Value>),
    /// Fail it: pending again after this delay in milliseconds, or dead
    /// past its attempts.
    Fail(u64),
    /// Kill it into the dead letter file now.
    Kill,
}

/// Claimed batch.
#[derive(serde::Deserialize, Clone, Debug)]
struct PollResponse {
    /// Claimed items, due-first.
    #[serde(default)]
    items: Vec<Claim>,
}

/// Cancel outcome.
#[derive(serde::Deserialize, Clone, Debug)]
struct CancelResponse {
    /// Whether an item existed.
    #[serde(default)]
    removed: bool,
}

/// Purge outcome.
#[derive(serde::Deserialize, Clone, Debug)]
struct PurgeResponse {
    /// Items removed.
    #[serde(default)]
    removed: usize,
}

/// Stored item.
#[derive(serde::Deserialize, Clone, Debug)]
struct ItemResponse {
    /// The item, when found.
    #[serde(default)]
    item: Option<QueueItem>,
}

/// POST through the client's retry loop, decoding the `ok` envelope.
pub(crate) async fn post<T: DeserializeOwned>(
    client: &FlowerClient,
    path: &str,
    body: &serde_json::Value,
    options: &RequestOptions,
) -> Result<T, FlowerError> {
    let bytes = client.send(Method::Post, path, Some(body), options).await?;
    decode_ok(&bytes)
}

/// GET through the client's retry loop, decoding the `ok` envelope.
pub(crate) async fn get<T: DeserializeOwned>(
    client: &FlowerClient,
    path: &str,
    options: &RequestOptions,
) -> Result<T, FlowerError> {
    let bytes = client.send(Method::Get, path, None, options).await?;
    decode_ok(&bytes)
}

impl FlowerClient {
    /// Claim a batch of due items. Retries are safe: a lost
    /// response only delays the claim's visibility.
    pub async fn poll_queue(
        &self,
        db: &str,
        queue: &str,
        limit: Option<usize>,
        lease_ms: Option<u64>,
        options: &RequestOptions,
    ) -> Result<Vec<Claim>, FlowerError> {
        let poll = QueuePoll {
            limit,
            lease_ms,
            ..QueuePoll::default()
        };
        self.claim_queue(db, queue, &poll, options).await
    }

    /// Claim a batch of claimable items of one scope (`""` is the
    /// default scope): lowest priority first, then group turns, then the
    /// longest waiting, recording the owner on each lease. Each claim
    /// carries the next fencing token of its scope. With `wait_ms`, a
    /// claim that comes back short keeps the owner's place in the
    /// scope's line (see [`FlowerClient::queue_readiness`]).
    pub async fn claim_queue(
        &self,
        db: &str,
        queue: &str,
        poll: &QueuePoll<'_>,
        options: &RequestOptions,
    ) -> Result<Vec<Claim>, FlowerError> {
        let body = serde_json::json!({
            "db": db,
            "queue": queue,
            "scope": poll.scope,
            "owner": poll.owner,
            "limit": poll.limit,
            "lease_ms": poll.lease_ms,
            "wait_ms": poll.wait_ms,
        });
        let response: PollResponse = post(self, "/v1/queue/poll", &body, options).await?;
        Ok(response.items)
    }

    /// Report a claim and, with `next`, claim the reporter's next items
    /// over the state the report leaves, in the same commit (chained
    /// claims): while work waits, an item costs its report alone rather
    /// than a poll too. The claimed items answer as
    /// [`FlowerClient::claim_queue`]'s, and none without `next`. A
    /// refused report claims nothing; servers before chained claims
    /// report and answer none.
    #[allow(clippy::too_many_arguments)]
    pub async fn report_queue(
        &self,
        db: &str,
        queue: &str,
        id: &str,
        token: &str,
        report: QueueReport<'_>,
        next: Option<&QueuePoll<'_>>,
        options: &RequestOptions,
    ) -> Result<Vec<Claim>, FlowerError> {
        let mut body = serde_json::json!({
            "db": db,
            "queue": queue,
            "id": id,
            "token": token,
        });
        let path = match report {
            QueueReport::Complete(result) => {
                if let Some(result) = result {
                    body["result"] = result.clone();
                }
                "/v1/queue/complete"
            }
            QueueReport::Fail(delay_ms) => {
                body["delay_ms"] = delay_ms.into();
                "/v1/queue/fail"
            }
            QueueReport::Kill => "/v1/queue/kill",
        };
        if let Some(next) = next {
            body["next"] = serde_json::json!({
                "scope": next.scope,
                "owner": next.owner,
                "limit": next.limit,
                "lease_ms": next.lease_ms,
                "wait_ms": next.wait_ms,
            });
        }
        let response: PollResponse = post(self, path, &body, options).await?;
        Ok(response.items)
    }

    /// Complete a claim and keep the item as completed with `result`,
    /// which must meet the queue's declared result contracts (a null
    /// result deletes it, as [`FlowerClient::complete_queue`]).
    pub async fn complete_queue_with(
        &self,
        db: &str,
        queue: &str,
        id: &str,
        token: &str,
        result: &serde_json::Value,
        options: &RequestOptions,
    ) -> Result<(), FlowerError> {
        let body = serde_json::json!({
            "db": db,
            "queue": queue,
            "id": id,
            "token": token,
            "result": result,
        });
        let _: Envelope = post(self, "/v1/queue/complete", &body, options).await?;
        Ok(())
    }

    /// Whether a claim on one scope would take an item now — with an
    /// owner, only once it is that owner's turn in the scope's line —
    /// and when that next changes. It reads the scope's next items and
    /// the line's head only (`GET /v1/queue/ready`), counting nothing,
    /// so waiting workers can ask every interval.
    pub async fn queue_readiness(
        &self,
        db: &str,
        queue: &str,
        scope: &str,
        owner: Option<&str>,
        options: &RequestOptions,
    ) -> Result<QueueReadiness, FlowerError> {
        let mut path = format!(
            "/v1/queue/ready?db={}&queue={}&scope={}",
            encode_query(db),
            encode_query(queue),
            encode_query(scope)
        );
        if let Some(owner) = owner {
            path.push_str(&format!("&owner={}", encode_query(owner)));
        }
        get(self, &path, options).await
    }

    /// Read a named job by scope and name, including dead and completed
    /// ones. Missing jobs read as `None`.
    pub async fn get_queue_job(
        &self,
        db: &str,
        queue: &str,
        scope: &str,
        name: &str,
        options: &RequestOptions,
    ) -> Result<Option<QueueItem>, FlowerError> {
        let path = format!(
            "/v1/queue/item?db={}&queue={}&scope={}&name={}",
            encode_query(db),
            encode_query(queue),
            encode_query(scope),
            encode_query(name)
        );
        match get::<ItemResponse>(self, &path, options).await {
            Ok(response) => Ok(response.item),
            Err(FlowerError::Business { code, .. }) if code == "UNKNOWN_ITEM" => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Renew a claim's lease.
    pub async fn heartbeat_queue(
        &self,
        db: &str,
        queue: &str,
        id: &str,
        token: &str,
        lease_ms: Option<u64>,
        options: &RequestOptions,
    ) -> Result<(), FlowerError> {
        let body = serde_json::json!({
            "db": db,
            "queue": queue,
            "id": id,
            "token": token,
            "lease_ms": lease_ms,
        });
        let _: Envelope = post(self, "/v1/queue/heartbeat", &body, options).await?;
        Ok(())
    }

    /// Complete a claim, deleting the item.
    pub async fn complete_queue(
        &self,
        db: &str,
        queue: &str,
        id: &str,
        token: &str,
        options: &RequestOptions,
    ) -> Result<(), FlowerError> {
        let body = serde_json::json!({
            "db": db,
            "queue": queue,
            "id": id,
            "token": token,
        });
        let _: Envelope = post(self, "/v1/queue/complete", &body, options).await?;
        Ok(())
    }

    /// Fail a claim: requeue after a delay, or die past the maximum.
    pub async fn fail_queue(
        &self,
        db: &str,
        queue: &str,
        id: &str,
        token: &str,
        delay_ms: Option<u64>,
        options: &RequestOptions,
    ) -> Result<(), FlowerError> {
        let body = serde_json::json!({
            "db": db,
            "queue": queue,
            "id": id,
            "token": token,
            "delay_ms": delay_ms,
        });
        let _: Envelope = post(self, "/v1/queue/fail", &body, options).await?;
        Ok(())
    }

    /// Kill a claim into the dead letter file immediately.
    pub async fn kill_queue(
        &self,
        db: &str,
        queue: &str,
        id: &str,
        token: &str,
        options: &RequestOptions,
    ) -> Result<(), FlowerError> {
        let body = serde_json::json!({
            "db": db,
            "queue": queue,
            "id": id,
            "token": token,
        });
        let _: Envelope = post(self, "/v1/queue/kill", &body, options).await?;
        Ok(())
    }

    /// Release a claim back to pending without an attempt.
    pub async fn release_queue(
        &self,
        db: &str,
        queue: &str,
        id: &str,
        token: &str,
        delay_ms: Option<u64>,
        options: &RequestOptions,
    ) -> Result<(), FlowerError> {
        let body = serde_json::json!({
            "db": db,
            "queue": queue,
            "id": id,
            "token": token,
            "delay_ms": delay_ms,
        });
        let _: Envelope = post(self, "/v1/queue/release", &body, options).await?;
        Ok(())
    }

    /// Cancel an item in any state, returning whether one existed.
    pub async fn cancel_queue(
        &self,
        db: &str,
        queue: &str,
        id: &str,
        options: &RequestOptions,
    ) -> Result<bool, FlowerError> {
        let body = serde_json::json!({
            "db": db,
            "queue": queue,
            "id": id,
        });
        let response: CancelResponse = post(self, "/v1/queue/cancel", &body, options).await?;
        Ok(response.removed)
    }

    /// Requeue a dead item with a fresh attempt budget.
    pub async fn retry_queue(
        &self,
        db: &str,
        queue: &str,
        id: &str,
        delay_ms: Option<u64>,
        options: &RequestOptions,
    ) -> Result<(), FlowerError> {
        let body = serde_json::json!({
            "db": db,
            "queue": queue,
            "id": id,
            "delay_ms": delay_ms,
        });
        let _: Envelope = post(self, "/v1/queue/retry", &body, options).await?;
        Ok(())
    }

    /// Delete dead items enqueued before a cutoff, returning the
    /// count removed.
    pub async fn purge_queue(
        &self,
        db: &str,
        queue: &str,
        older_than_ms: Option<u64>,
        options: &RequestOptions,
    ) -> Result<usize, FlowerError> {
        let body = serde_json::json!({
            "db": db,
            "queue": queue,
            "older_than_ms": older_than_ms,
        });
        let response: PurgeResponse = post(self, "/v1/queue/purge", &body, options).await?;
        Ok(response.removed)
    }

    /// Count live, claimed and dead items. Queue names need no
    /// escaping: validation rejects anything outside the path-safe
    /// alphabet.
    pub async fn queue_stats(
        &self,
        db: &str,
        queue: &str,
        options: &RequestOptions,
    ) -> Result<QueueStats, FlowerError> {
        self.queue_stats_in(db, queue, None, options).await
    }

    /// Count one scope's live, claimed and dead items, or every
    /// scope's for `None`.
    pub async fn queue_stats_in(
        &self,
        db: &str,
        queue: &str,
        scope: Option<&str>,
        options: &RequestOptions,
    ) -> Result<QueueStats, FlowerError> {
        // Stats ride the shared status struct: pending, claimed and
        // dead decode straight off the envelope.
        #[derive(serde::Deserialize)]
        struct StatsResponse {
            #[serde(default)]
            pending: usize,
            #[serde(default)]
            claimed: usize,
            #[serde(default)]
            dead: usize,
            #[serde(default)]
            completed: usize,
        }
        let mut path = format!(
            "/v1/queue/stats?db={}&queue={}",
            encode_query(db),
            encode_query(queue)
        );
        if let Some(scope) = scope {
            path.push_str(&format!("&scope={}", encode_query(scope)));
        }
        let response: StatsResponse = get(self, &path, options).await?;
        Ok(QueueStats {
            pending: response.pending,
            claimed: response.claimed,
            dead: response.dead,
            completed: response.completed,
        })
    }

    /// Read one item, including dead ones. Missing items read as
    /// `None`; other failures raise.
    pub async fn get_queue_item(
        &self,
        db: &str,
        queue: &str,
        id: &str,
        options: &RequestOptions,
    ) -> Result<Option<QueueItem>, FlowerError> {
        let path = format!(
            "/v1/queue/item?db={}&queue={}&id={}",
            encode_query(db),
            encode_query(queue),
            encode_query(id)
        );
        // A missing item is an expected outcome, not an error: map
        // the unknown-item business failure to None here.
        match get::<ItemResponse>(self, &path, options).await {
            Ok(response) => Ok(response.item),
            Err(FlowerError::Business { code, .. }) if code == "UNKNOWN_ITEM" => Ok(None),
            Err(error) => Err(error),
        }
    }
}
