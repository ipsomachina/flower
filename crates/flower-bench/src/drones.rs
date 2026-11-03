//! Delivery drones: `flower-worker` queue workers over the `deliveries`
//! queue, plus a clumsy drone that drops some claims on purpose.
//!
//! Each tenant's deliveries are jobs in that tenant's scope, so the
//! fleet of a database is one `run_queue_worker` per tenant scope, the
//! `--workers` handlers split between them, through [`DroneQueue`], a
//! `QueueClient` that sends each queue call to the leader (falling back
//! to another member, which forwards) and accounts for it as drone
//! traffic. Each worker waits in its scope's line when the scope runs
//! dry, reading readiness rather than claiming until work arrives, and
//! chains: each completion claims its next deliveries in the same
//! commit. A handler flies for 5 to 30 ms, as v1's drones did, then
//! calls `pizza.deliver` with its lease, with uncertain retries under
//! one request ID: the guest renews that lease in the paying
//! transaction, so a drone whose lease lapsed cannot collect coins. The
//! worker then completes the claim, renewing the lease while the
//! handler runs; stopping, it gives handlers [`DRAIN`] to land.
//!
//! The clumsy drone claims items itself, from each tenant's scope in
//! turn, under the short `--lease-ms` lease and abandons first claims
//! (`--abandon-rate` of acknowledged orders, at least one). Each
//! abandoned lease lapses, a fleet drone reclaims the item (its second
//! attempt) and delivers it. The audit then presents every abandoned
//! claim's token again, to complete and to renew, and both must be
//! refused: an expired lease is fenced.

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use flower_client::client::RequestOptions;
use flower_client::{
    Claim, FlowerClient, FlowerError, QueuePoll, QueueReadiness, QueueReport, Retry,
};
use flower_worker::{QueueClient, QueueWorkerOptions, WorkError, WorkerSummary, run_queue_worker};
use serde::Serialize;
use tokio::sync::Notify;

use crate::client::{BenchClient, Who};
use crate::metrics::Class;
use crate::workload::{Ledger, Random, compare};

/// The queue the guest's ovens fill, one scope per tenant.
pub const QUEUE: &str = "deliveries";
/// How long a drone keeps its place in its scope's line.
const WAIT: Duration = Duration::from_secs(30);
/// Once stopping, how long deliveries in the air may take to land
/// before their claims are failed back to the queue.
const DRAIN: Duration = Duration::from_secs(10);

/// A `QueueClient` routed like the bench's writes and accounted as
/// drone traffic. One attempt per call: the worker owns retries.
#[derive(Clone)]
pub struct DroneQueue {
    client: Arc<BenchClient>,
    members: Arc<Vec<FlowerClient>>,
}

impl DroneQueue {
    /// Queue calls through `client`'s members, with worker credentials.
    pub fn new(client: Arc<BenchClient>) -> Self {
        let members = (0..client.members().len())
            .map(|index| client.worker_client(index))
            .collect();
        DroneQueue {
            client,
            members: Arc::new(members),
        }
    }

    /// Make one queue call through the member worker calls go to,
    /// accounting for it as drone traffic.
    async fn account<T, F, Fut>(&self, op: &str, call: F) -> Result<T, FlowerError>
    where
        F: FnOnce(FlowerClient) -> Fut,
        Fut: Future<Output = Result<T, FlowerError>>,
    {
        let member = self.client.worker_target();
        let started = Instant::now();
        let result = call(self.members[member].clone()).await;
        let ms = started.elapsed().as_secs_f64() * 1000.0;
        let recorder = self.client.recorder();
        let kind = match &result {
            Ok(_) => None,
            Err(FlowerError::Business { code, .. }) => Some(format!("business:{code}")),
            Err(FlowerError::Timeout) => Some("timeout".to_string()),
            Err(FlowerError::Transport(_)) => Some("transport".to_string()),
            Err(FlowerError::Forward { .. }) => Some("http-503:forward".to_string()),
            Err(other) => Some(format!("queue:{}", error_code(other))),
        };
        recorder.attempt(op, Class::Worker, ms, kind.as_deref(), false);
        recorder.call(op, Class::Worker, ms, result.is_ok());
        if let Err(error) = &result
            && error.is_transient()
        {
            self.client.report_failure(member);
        }
        result
    }
}

fn error_code(error: &FlowerError) -> String {
    if error.is_auth() {
        return "auth".into();
    }
    if let Some(code) = error.code() {
        return code.into();
    }
    match error {
        FlowerError::Server { code, .. } => code.clone(),
        // `status 503 CODE: message`: the status and code, not the message.
        FlowerError::RetryExhausted(message) => {
            message.split(':').next().unwrap_or(message).to_string()
        }
        FlowerError::Auth(_) => "auth".to_string(),
        other => format!("{other}").chars().take(40).collect(),
    }
}

fn once() -> RequestOptions {
    RequestOptions::new().retry(Retry::Off)
}

impl QueueClient for DroneQueue {
    async fn poll(
        &self,
        db: &str,
        queue: &str,
        limit: usize,
        lease_ms: u64,
    ) -> Result<Vec<Claim>, FlowerError> {
        self.account("queue.poll", |client| async move {
            client
                .poll_queue(db, queue, Some(limit), Some(lease_ms), &once())
                .await
        })
        .await
    }

    async fn claim(
        &self,
        db: &str,
        queue: &str,
        poll: &QueuePoll<'_>,
    ) -> Result<Vec<Claim>, FlowerError> {
        self.account("queue.poll", |client| async move {
            client.claim_queue(db, queue, poll, &once()).await
        })
        .await
    }

    async fn readiness(
        &self,
        db: &str,
        queue: &str,
        scope: &str,
        owner: &str,
    ) -> Result<QueueReadiness, FlowerError> {
        self.account("queue.ready", |client| async move {
            client
                .queue_readiness(db, queue, scope, Some(owner), &once())
                .await
        })
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
        let op = match report {
            QueueReport::Complete(_) => "queue.complete",
            QueueReport::Fail(_) => "queue.fail",
            QueueReport::Kill => "queue.kill",
        };
        self.account(op, |client| async move {
            client
                .report_queue(db, queue, id, token, report, Some(next), &once())
                .await
        })
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
        self.account("queue.heartbeat", |client| async move {
            client
                .heartbeat_queue(db, queue, id, token, Some(lease_ms), &once())
                .await
        })
        .await
    }

    async fn complete(
        &self,
        db: &str,
        queue: &str,
        id: &str,
        token: &str,
    ) -> Result<(), FlowerError> {
        self.account("queue.complete", |client| async move {
            client.complete_queue(db, queue, id, token, &once()).await
        })
        .await
    }

    async fn fail(
        &self,
        db: &str,
        queue: &str,
        id: &str,
        token: &str,
        delay_ms: u64,
    ) -> Result<(), FlowerError> {
        self.account("queue.fail", |client| async move {
            client
                .fail_queue(db, queue, id, token, Some(delay_ms), &once())
                .await
        })
        .await
    }

    async fn kill(&self, db: &str, queue: &str, id: &str, token: &str) -> Result<(), FlowerError> {
        self.account("queue.kill", |client| async move {
            client.kill_queue(db, queue, id, token, &once()).await
        })
        .await
    }

    async fn release(
        &self,
        db: &str,
        queue: &str,
        id: &str,
        token: &str,
    ) -> Result<(), FlowerError> {
        self.account("queue.release", |client| async move {
            client
                .release_queue(db, queue, id, token, None, &once())
                .await
        })
        .await
    }

    fn overloads(&self) -> u64 {
        self.members.iter().map(FlowerClient::overloads).sum()
    }
}

/// A delivery in the air, for the live view.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Flight {
    /// Order ID.
    pub order: String,
    /// `[tenant, store]`.
    pub shop: Vec<String>,
    /// Which claim of the item this is.
    pub attempt: u32,
    /// Milliseconds in the air so far.
    pub airborne_ms: u64,
}

/// An abandoned claim.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Dropped {
    /// The database whose queue held it.
    #[serde(skip)]
    pub db: String,
    /// Queue item ID.
    pub item: String,
    /// Order ID.
    pub order: String,
    /// `[tenant, store]`.
    pub shop: Vec<String>,
    /// The abandoned claim's token, presented again at the audit.
    #[serde(skip)]
    pub token: String,
    /// When it was dropped.
    #[serde(skip)]
    pub at: Option<Instant>,
    /// Milliseconds until its lease lapses (0 once lapsed).
    pub lease_left_ms: u64,
    /// The drone slot that reclaimed it, once one did.
    pub reclaimed_by: Option<usize>,
}

/// Drone counters.
#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DroneCounters {
    /// Claims the fleet's handlers ran.
    pub claims: u64,
    /// Claims of items claimed before (lapsed leases).
    pub reclaimed: u64,
    /// Deliveries acknowledged.
    pub delivered: u64,
    /// Deliveries that failed and were handed back to the queue.
    pub failed: u64,
    /// Claims the clumsy drone abandoned.
    pub abandoned: u64,
    /// Claims whose lease lapsed under a handler (the worker's count).
    pub lost: u64,
    /// The fleet's handler limit.
    pub fleet: usize,
}

#[derive(Default)]
struct YardInner {
    slots: Vec<Option<(Flight, Instant)>>,
    dropped: Vec<Dropped>,
    claims: u64,
    reclaimed: u64,
    delivered: u64,
    failed: u64,
}

/// The drone yard: who is flying what, and what was dropped.
pub struct Yard {
    inner: Mutex<YardInner>,
    lease: Duration,
    lost: AtomicU64,
}

/// The yard as the live view shows it.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct YardView {
    /// One entry per fleet slot: its flight, if any.
    pub slots: Vec<Option<Flight>>,
    /// Abandoned claims, most recent last (at most 12).
    pub dropped: Vec<Dropped>,
    /// Counters.
    pub counters: DroneCounters,
}

impl Yard {
    /// A yard for `fleet` drones; abandoned leases last `lease`.
    pub fn new(fleet: usize, lease: Duration) -> Self {
        Yard {
            inner: Mutex::new(YardInner {
                slots: vec![None; fleet],
                ..YardInner::default()
            }),
            lease,
            lost: AtomicU64::new(0),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, YardInner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn take_off(&self, claim: &Claim) -> usize {
        let order = order_of(&claim.payload);
        let mut inner = self.lock();
        inner.claims += 1;
        if claim.attempts > 1 {
            inner.reclaimed += 1;
        }
        let slot = inner
            .slots
            .iter()
            .position(Option::is_none)
            .unwrap_or_else(|| {
                inner.slots.push(None);
                inner.slots.len() - 1
            });
        if let Some(dropped) = inner
            .dropped
            .iter_mut()
            .find(|dropped| dropped.item == claim.id)
        {
            dropped.reclaimed_by = Some(slot);
        }
        inner.slots[slot] = Some((
            Flight {
                order: order.0,
                shop: order.1,
                attempt: claim.attempts,
                airborne_ms: 0,
            },
            Instant::now(),
        ));
        slot
    }

    fn land(&self, slot: usize, delivered: bool) {
        let mut inner = self.lock();
        inner.slots[slot] = None;
        if delivered {
            inner.delivered += 1;
        } else {
            inner.failed += 1;
        }
    }

    fn dropped(&self, db: &str, claim: &Claim) {
        let (order, shop) = order_of(&claim.payload);
        self.lock().dropped.push(Dropped {
            db: db.to_string(),
            item: claim.id.clone(),
            order,
            shop,
            token: claim.token.clone(),
            at: Some(Instant::now()),
            lease_left_ms: 0,
            reclaimed_by: None,
        });
    }

    /// Abandoned claims so far.
    pub fn abandoned(&self) -> Vec<Dropped> {
        self.lock().dropped.clone()
    }

    /// The view the live page draws.
    pub fn view(&self) -> YardView {
        let inner = self.lock();
        let now = Instant::now();
        let slots = inner
            .slots
            .iter()
            .map(|slot| {
                slot.as_ref().map(|(flight, since)| Flight {
                    airborne_ms: now.saturating_duration_since(*since).as_millis() as u64,
                    ..flight.clone()
                })
            })
            .collect();
        let from = inner.dropped.len().saturating_sub(12);
        let dropped = inner.dropped[from..]
            .iter()
            .map(|dropped| Dropped {
                lease_left_ms: dropped.at.map_or(0, |at| {
                    (at + self.lease).saturating_duration_since(now).as_millis() as u64
                }),
                ..dropped.clone()
            })
            .collect();
        YardView {
            slots,
            dropped,
            counters: self.counters_locked(&inner),
        }
    }

    fn counters_locked(&self, inner: &YardInner) -> DroneCounters {
        DroneCounters {
            claims: inner.claims,
            reclaimed: inner.reclaimed,
            delivered: inner.delivered,
            failed: inner.failed,
            abandoned: inner.dropped.len() as u64,
            lost: self.lost.load(Ordering::Relaxed),
            fleet: inner.slots.len(),
        }
    }

    /// The counters.
    pub fn counters(&self) -> DroneCounters {
        let inner = self.lock();
        self.counters_locked(&inner)
    }
}

/// An order's ID and store from a delivery payload `{order: [t, s, id]}`.
fn order_of(payload: &serde_json::Value) -> (String, Vec<String>) {
    let parts: Vec<String> = payload["order"]
        .as_array()
        .map(|parts| {
            parts
                .iter()
                .filter_map(|part| part.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();
    match parts.as_slice() {
        [tenant, store, id] => (id.clone(), vec![tenant.clone(), store.clone()]),
        _ => (String::new(), Vec::new()),
    }
}

/// What one database's drones need.
#[derive(Clone)]
pub struct Fleet {
    /// The group's client.
    pub client: Arc<BenchClient>,
    /// The database whose queue they work.
    pub db: String,
    /// The tenants it holds: one queue scope each.
    pub tenants: Vec<String>,
    /// The ledger (replay probes, abandonment targets).
    pub ledger: Arc<Ledger>,
    /// The yard.
    pub yard: Arc<Yard>,
    /// Handlers.
    pub workers: usize,
    /// Abandoned leases' length, and the fleet's renewal lease.
    pub lease: Duration,
    /// Polling pause.
    pub poll: Duration,
    /// Receipt-replay probe rate.
    pub duplicate_rate: f64,
    /// Abandoned share of acknowledged orders.
    pub abandon_rate: f64,
    /// Workload seed.
    pub seed: String,
}

impl Fleet {
    /// Run the fleet, one worker per tenant scope, until `stop` fires,
    /// then let in-flight deliveries land and return the workers'
    /// summary.
    pub async fn fly(self, stop: Arc<Notify>) -> Result<WorkerSummary, FlowerError> {
        let queue = DroneQueue::new(self.client.clone());
        let scopes = self.tenants.len().max(1);
        let fleet = Arc::new(self);
        let flights = Arc::new(AtomicU64::new(0));
        let mut stops = Vec::new();
        let mut workers = Vec::new();
        for (index, tenant) in fleet.tenants.iter().enumerate() {
            // The handlers split between the scopes, at least one each.
            let handlers =
                (fleet.workers / scopes + usize::from(index < fleet.workers % scopes)).max(1);
            let options = QueueWorkerOptions::new(&fleet.db, QUEUE)
                .scope(tenant)
                .owner(format!("drones-{}-{tenant}", fleet.db))
                .concurrency(handlers)
                .lease(fleet.lease)
                .poll_interval(fleet.poll)
                .retry(fleet.poll, Duration::from_secs(2))
                .wait(WAIT)
                .chain()
                .drain(DRAIN);
            let stop = Arc::new(Notify::new());
            stops.push(stop.clone());
            let (fleet, flights) = (fleet.clone(), flights.clone());
            workers.push(tokio::spawn(run_queue_worker(
                queue.clone(),
                options,
                stop,
                move |claim| {
                    let fleet = fleet.clone();
                    let flight = flights.fetch_add(1, Ordering::Relaxed);
                    async move { fleet.deliver(claim, flight).await }
                },
            )));
        }
        // One stop for the fleet stops every scope's worker.
        let relay = tokio::spawn(async move {
            stop.notified().await;
            for stop in stops {
                stop.notify_one();
            }
        });
        let mut summary = WorkerSummary::default();
        let mut failed = None;
        for worker in workers {
            match worker.await {
                Ok(Ok(scope)) => {
                    summary.completed += scope.completed;
                    summary.failed += scope.failed;
                    summary.killed += scope.killed;
                    summary.lost += scope.lost;
                    summary.drained += scope.drained;
                }
                Ok(Err(error)) => failed = failed.or(Some(error)),
                Err(error) => {
                    failed = failed.or(Some(FlowerError::Transport(format!(
                        "a drone worker failed: {error}"
                    ))));
                }
            }
        }
        relay.abort();
        match failed {
            Some(error) => Err(error),
            None => Ok(summary),
        }
    }

    async fn deliver(&self, claim: Claim, flight: u64) -> Result<(), WorkError> {
        let slot = self.yard.take_off(&claim);
        let mut random = Random::new(&format!("{}:drone:{flight}", self.seed));
        tokio::time::sleep(Duration::from_millis(5 + random.below(25) as u64)).await;
        let request_id = self.client.next_id();
        // The drone delivers for the order's tenant, under its lease.
        let tenant = claim.payload["order"][0].as_str().unwrap_or_default();
        let delivery = serde_json::json!({
            "order": claim.payload["order"],
            "id": claim.id,
            "token": claim.token,
        });
        let request = self.client.mutate(
            &self.db,
            "pizza.deliver",
            &delivery,
            &request_id,
            Who::Tenant(tenant),
        );
        match self
            .client
            .call("pizza.deliver", Class::Worker, &request, Instant::now())
            .await
        {
            Ok(answer) => {
                self.yard.land(slot, true);
                if random.fraction() < self.duplicate_rate
                    && let Ok(replay) = self
                        .client
                        .call(
                            "pizza.deliver.replay",
                            Class::Replay,
                            &request,
                            Instant::now(),
                        )
                        .await
                {
                    self.ledger
                        .replayed(compare("pizza.deliver", &answer, &replay));
                }
                Ok(())
            }
            Err(_) => {
                // Counted by the recorder and the yard; the queue
                // hands the item out again after a pause.
                self.yard.land(slot, false);
                Err(WorkError::Retry(Some(self.poll)))
            }
        }
    }

    /// The clumsy drone: until `stop`, claim one item at a time under
    /// the short lease and abandon first claims while fewer than the
    /// abandon rate's share of acknowledged orders (at least one) are
    /// abandoned. A reclaimed item it gets goes straight back.
    pub async fn fumble(self, stop: flower_client::CancellationToken) {
        if self.abandon_rate <= 0.0 {
            return;
        }
        let queue = DroneQueue::new(self.client.clone());
        let db = self.db.clone();
        let lease_ms = self.lease.as_millis() as u64;
        let mut turn = 0usize;
        while !stop.is_cancelled() {
            let acknowledged = self.ledger.acknowledged_orders() as f64;
            let target = (self.abandon_rate * acknowledged).ceil().max(1.0) as usize;
            let abandoned = self.yard.abandoned().len();
            if acknowledged > 0.0
                && abandoned < target
                && let Some(tenant) = self.tenants.get(turn % self.tenants.len().max(1))
            {
                // Each tenant's scope in turn.
                turn += 1;
                let poll = QueuePoll {
                    scope: tenant,
                    owner: Some("clumsy-drone"),
                    limit: Some(1),
                    lease_ms: Some(lease_ms),
                    wait_ms: None,
                };
                let claims = queue.claim(&db, QUEUE, &poll).await.unwrap_or_default();
                for claim in claims {
                    if claim.attempts == 1 {
                        self.yard.dropped(&db, &claim);
                    } else {
                        let _ = queue.release(&db, QUEUE, &claim.id, &claim.token).await;
                    }
                }
            }
            // Until the first drop, poll hard enough to beat the fleet to
            // a fresh item, so even a short run exercises fencing.
            let pause = if self.yard.abandoned().is_empty() {
                Duration::from_millis(10)
            } else {
                self.poll
            };
            tokio::select! {
                () = stop.cancelled() => break,
                () = tokio::time::sleep(pause) => {}
            }
        }
    }

    /// Count claims the worker lost.
    pub fn lost(&self, count: u64) {
        self.yard.lost.fetch_add(count, Ordering::Relaxed);
    }
}

/// The verdict on one abandoned claim's token, presented again.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FenceProbe {
    /// Queue item.
    pub item: String,
    /// Order.
    pub order: String,
    /// What completing with the stale token answered.
    pub complete: String,
    /// What renewing with it answered.
    pub heartbeat: String,
    /// Both refused as business failures.
    pub rejected: bool,
}

/// Present every abandoned claim's token to complete and to renew.
/// Each must be refused as a decided business failure; an acceptance,
/// or no decided answer after a few tries, fails the probe.
pub async fn probe_fences(client: &BenchClient, dropped: &[Dropped]) -> Vec<FenceProbe> {
    let mut probes = Vec::new();
    for dropped in dropped {
        let complete = verdict(client, |member| {
            let db = dropped.db.clone();
            let (item, token) = (dropped.item.clone(), dropped.token.clone());
            async move {
                member
                    .complete_queue(&db, QUEUE, &item, &token, &once())
                    .await
            }
        })
        .await;
        let heartbeat = verdict(client, |member| {
            let db = dropped.db.clone();
            let (item, token) = (dropped.item.clone(), dropped.token.clone());
            async move {
                member
                    .heartbeat_queue(&db, QUEUE, &item, &token, Some(1_000), &once())
                    .await
            }
        })
        .await;
        let refused = |answer: &Result<(), String>| matches!(answer, Err(text) if text.starts_with("refused:"));
        probes.push(FenceProbe {
            item: dropped.item.clone(),
            order: dropped.order.clone(),
            rejected: refused(&complete) && refused(&heartbeat),
            complete: describe(&complete),
            heartbeat: describe(&heartbeat),
        });
    }
    probes
}

fn describe(answer: &Result<(), String>) -> String {
    match answer {
        Ok(()) => "accepted".to_string(),
        Err(text) => text.clone(),
    }
}

/// Run a queue call with the worker credentials until it is decided:
/// `Ok` if accepted, `Err("refused: ...")` for a business refusal,
/// `Err("undecided: ...")` if no member decided it.
async fn verdict<F, Fut>(client: &BenchClient, call: F) -> Result<(), String>
where
    F: Fn(FlowerClient) -> Fut,
    Fut: Future<Output = Result<(), FlowerError>>,
{
    let mut last = String::new();
    for attempt in 0..20 {
        let member = client.worker_target();
        match call(client.worker_client(member)).await {
            Ok(()) => return Ok(()),
            Err(FlowerError::Business { code, message, .. }) => {
                return Err(format!("refused: {code}: {message}"));
            }
            Err(error) => {
                last = error.to_string();
                client.report_failure(member);
                tokio::time::sleep(Duration::from_millis(50 * (attempt + 1))).await;
            }
        }
    }
    Err(format!("undecided: {last}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn claim(id: &str, attempts: u32) -> Claim {
        Claim {
            id: id.to_string(),
            payload: serde_json::json!({"order": ["tenant-0", "store-1", "pizza-000007"]}),
            token: format!("token-{id}"),
            attempts,
            fence: u64::from(attempts),
            scope: String::new(),
            priority: 0,
            expires_ms: 0,
            name: None,
            group: None,
            claimed_ms: 0,
        }
    }

    #[test]
    fn the_yard_tracks_flights_drops_and_reclaims() {
        let yard = Yard::new(2, Duration::from_secs(2));
        yard.dropped("pizza", &claim("a", 1));
        let first = yard.take_off(&claim("b", 1));
        let second = yard.take_off(&claim("a", 2));
        assert_ne!(first, second);
        let view = yard.view();
        assert_eq!(view.slots.iter().flatten().count(), 2);
        assert_eq!(view.dropped.len(), 1);
        assert_eq!(view.dropped[0].reclaimed_by, Some(second));
        assert_eq!(view.dropped[0].order, "pizza-000007");
        assert!(view.dropped[0].lease_left_ms <= 2_000);
        yard.land(first, true);
        yard.land(second, false);
        let counters = yard.counters();
        assert_eq!(counters.claims, 2);
        assert_eq!(counters.reclaimed, 1);
        assert_eq!(counters.delivered, 1);
        assert_eq!(counters.failed, 1);
        assert_eq!(counters.abandoned, 1);
        // Tokens never reach the live view.
        let json = serde_json::to_string(&yard.view()).unwrap();
        assert!(!json.contains("token-a"));
    }
}
