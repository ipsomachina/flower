//! What the worker needs from Flower: [`QueueClient`], implemented by
//! `FlowerClient` over HTTP and by the in-memory queue of the
//! `testing` feature.

use std::future::Future;

use flower_client::client::{FlowerClient, RequestOptions};
use flower_client::error::FlowerError;
use flower_client::retry::Retry;
use flower_client::work::{Claim, QueuePoll, QueueReadiness, QueueReport};

/// Most items a claim takes when it names no limit: the server's.
const DEFAULT_LIMIT: usize = 10;
/// A claim's lease when it names none: the server's.
const DEFAULT_LEASE_MS: u64 = 30_000;

/// The queue calls a worker makes. Every call names its database and
/// queue; implementations make one attempt each (the worker owns
/// retry and lease timing) and report failures as `FlowerError`s:
/// decided business failures (a lapsed claim, a wrong token) as
/// [`FlowerError::Business`], everything that may clear as a
/// transient error.
pub trait QueueClient: Clone + Send + Sync + 'static {
    /// Claim up to `limit` due items under a lease of `lease_ms`.
    fn poll(
        &self,
        db: &str,
        queue: &str,
        limit: usize,
        lease_ms: u64,
    ) -> impl Future<Output = Result<Vec<Claim>, FlowerError>> + Send;

    /// Claim up to `limit` claimable items of one scope (`""` is the
    /// default scope), lowest priority first. Clients without scopes
    /// serve the default scope through [`QueueClient::poll`] and refuse
    /// others.
    fn poll_scope(
        &self,
        db: &str,
        queue: &str,
        scope: &str,
        limit: usize,
        lease_ms: u64,
    ) -> impl Future<Output = Result<Vec<Claim>, FlowerError>> + Send {
        let unscoped = scope.is_empty();
        async move {
            if unscoped {
                self.poll(db, queue, limit, lease_ms).await
            } else {
                Err(FlowerError::Decode(
                    "this queue client claims from the default scope only".to_string(),
                ))
            }
        }
    }

    /// Claim as `poll` asks: up to its limit of one scope's claimable
    /// items under its lease, recording its owner on each, and with its
    /// wait, keeping the owner's place in the scope's line when the
    /// claim comes back short. Clients without owners and lines claim
    /// through [`QueueClient::poll_scope`].
    fn claim(
        &self,
        db: &str,
        queue: &str,
        poll: &QueuePoll<'_>,
    ) -> impl Future<Output = Result<Vec<Claim>, FlowerError>> + Send {
        self.poll_scope(
            db,
            queue,
            poll.scope,
            poll.limit.unwrap_or(DEFAULT_LIMIT),
            poll.lease_ms.unwrap_or(DEFAULT_LEASE_MS),
        )
    }

    /// Whether a claim by `owner` on `scope` would take an item now: once
    /// work is claimable and, while others wait in the scope's line
    /// ahead of it, once it is the owner's turn. Clients without lines
    /// answer ready.
    fn readiness(
        &self,
        db: &str,
        queue: &str,
        scope: &str,
        owner: &str,
    ) -> impl Future<Output = Result<QueueReadiness, FlowerError>> + Send {
        let _ = (db, queue, scope, owner);
        async {
            Ok(QueueReadiness {
                ready: true,
                next_ms: None,
            })
        }
    }

    /// Report a claim and claim the reporter's next items as `next` asks,
    /// over the state the report leaves, in one commit (chained claims).
    /// A refused report claims nothing. Clients without chained claims
    /// report through their single calls, then claim: a claim failing
    /// after the report landed answers none, so the report's outcome
    /// never depends on it.
    fn report(
        &self,
        db: &str,
        queue: &str,
        id: &str,
        token: &str,
        report: QueueReport<'_>,
        next: &QueuePoll<'_>,
    ) -> impl Future<Output = Result<Vec<Claim>, FlowerError>> + Send {
        async move {
            match report {
                QueueReport::Complete(None) => self.complete(db, queue, id, token).await?,
                QueueReport::Complete(Some(_)) => {
                    return Err(FlowerError::Decode(
                        "this queue client keeps no results".to_string(),
                    ));
                }
                QueueReport::Fail(delay_ms) => self.fail(db, queue, id, token, delay_ms).await?,
                QueueReport::Kill => self.kill(db, queue, id, token).await?,
            }
            Ok(self.claim(db, queue, next).await.unwrap_or_default())
        }
    }

    /// Renew a claim's lease.
    fn heartbeat(
        &self,
        db: &str,
        queue: &str,
        id: &str,
        token: &str,
        lease_ms: u64,
    ) -> impl Future<Output = Result<(), FlowerError>> + Send;

    /// Complete a claim, deleting the item.
    fn complete(
        &self,
        db: &str,
        queue: &str,
        id: &str,
        token: &str,
    ) -> impl Future<Output = Result<(), FlowerError>> + Send;

    /// Fail a claim: requeue after `delay_ms`, or die past its attempts.
    fn fail(
        &self,
        db: &str,
        queue: &str,
        id: &str,
        token: &str,
        delay_ms: u64,
    ) -> impl Future<Output = Result<(), FlowerError>> + Send;

    /// Kill a claim into the dead letters now.
    fn kill(
        &self,
        db: &str,
        queue: &str,
        id: &str,
        token: &str,
    ) -> impl Future<Output = Result<(), FlowerError>> + Send;

    /// Release a claim back to pending without spending an attempt.
    fn release(
        &self,
        db: &str,
        queue: &str,
        id: &str,
        token: &str,
    ) -> impl Future<Output = Result<(), FlowerError>> + Send;

    /// How many replies so far refused this client's calls for load. A
    /// worker whose count grows holds back: it claims nothing for a
    /// moment and, with an adaptive limit, takes on fewer items.
    /// Clients that cannot tell leave it 0.
    fn overloads(&self) -> u64 {
        0
    }
}

/// One attempt per call: the worker owns retry and lease timing.
fn once() -> RequestOptions {
    RequestOptions::new().retry(Retry::Off)
}

impl QueueClient for FlowerClient {
    async fn poll(
        &self,
        db: &str,
        queue: &str,
        limit: usize,
        lease_ms: u64,
    ) -> Result<Vec<Claim>, FlowerError> {
        self.poll_queue(db, queue, Some(limit), Some(lease_ms), &once())
            .await
    }

    async fn poll_scope(
        &self,
        db: &str,
        queue: &str,
        scope: &str,
        limit: usize,
        lease_ms: u64,
    ) -> Result<Vec<Claim>, FlowerError> {
        let poll = flower_client::work::QueuePoll {
            scope,
            limit: Some(limit),
            lease_ms: Some(lease_ms),
            ..Default::default()
        };
        self.claim_queue(db, queue, &poll, &once()).await
    }

    async fn claim(
        &self,
        db: &str,
        queue: &str,
        poll: &QueuePoll<'_>,
    ) -> Result<Vec<Claim>, FlowerError> {
        self.claim_queue(db, queue, poll, &once()).await
    }

    async fn readiness(
        &self,
        db: &str,
        queue: &str,
        scope: &str,
        owner: &str,
    ) -> Result<QueueReadiness, FlowerError> {
        self.queue_readiness(db, queue, scope, Some(owner), &once())
            .await
    }

    async fn report(
        &self,
        db: &str,
        queue: &str,
        id: &str,
        token: &str,
        report: QueueReport<'_>,
        next: &QueuePoll<'_>,
    ) -> Result<Vec<Claim>, FlowerError> {
        self.report_queue(db, queue, id, token, report, Some(next), &once())
            .await
    }

    async fn heartbeat(
        &self,
        db: &str,
        queue: &str,
        id: &str,
        token: &str,
        lease_ms: u64,
    ) -> Result<(), FlowerError> {
        self.heartbeat_queue(db, queue, id, token, Some(lease_ms), &once())
            .await
    }

    async fn complete(
        &self,
        db: &str,
        queue: &str,
        id: &str,
        token: &str,
    ) -> Result<(), FlowerError> {
        self.complete_queue(db, queue, id, token, &once()).await
    }

    async fn fail(
        &self,
        db: &str,
        queue: &str,
        id: &str,
        token: &str,
        delay_ms: u64,
    ) -> Result<(), FlowerError> {
        self.fail_queue(db, queue, id, token, Some(delay_ms), &once())
            .await
    }

    async fn kill(&self, db: &str, queue: &str, id: &str, token: &str) -> Result<(), FlowerError> {
        self.kill_queue(db, queue, id, token, &once()).await
    }

    async fn release(
        &self,
        db: &str,
        queue: &str,
        id: &str,
        token: &str,
    ) -> Result<(), FlowerError> {
        self.release_queue(db, queue, id, token, None, &once())
            .await
    }

    fn overloads(&self) -> u64 {
        FlowerClient::overloads(self)
    }
}
