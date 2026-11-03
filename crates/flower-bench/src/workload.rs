//! The Goblin Pizza customers: v1's workload model, closed or open loop.
//!
//! Each customer step draws, in v1's order, a choice, the hot or whole
//! store set, a tenant and a store: 30% of choices order (until the
//! order cap, after which they read), 40% read a store and 30% tip.
//! With the defaults, 80% of store choices go to one hot store per
//! tenant, so it takes about 85% of its tenant's calls. Randomness is
//! v1's seeded generator, so a seed reproduces each loop's choices.
//!
//! Closed loop (the default) keeps `concurrency` customers each waiting
//! for its answer. Open loop (`--offered-rate`) schedules arrivals on an
//! independent clock; an arrival that finds every slot busy is a
//! *driver drop*, counted and never queued or delayed. Open-loop latency
//! runs from the intended arrival, so scheduling lag is in it.
//!
//! Every acknowledged order and tip enters the [`Ledger`], the oracle the
//! audit reconciles the database against. A fraction of acknowledged
//! mutations is sent again under the same request ID, and the answer
//! must equal the original receipt. A mutation whose retries ran out is
//! *uncertain*: it may have applied. The ledger keeps its exact request,
//! and the drain sends it again under the same ID until it is decided,
//! so the audit stays exact rather than allowing slack.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use flower_client::CancellationToken;
use serde::Serialize;
use serde_json::{Value, json};

use crate::client::{Answer, BenchClient, CallError, Request, Who};
use crate::config::{ReadConsistency, Settings};
use crate::histogram::{Histogram, HistogramSnapshot};
use crate::metrics::Class;

/// Failure samples a run keeps.
const FAILURE_SAMPLES: usize = 50;
/// Tip receipts kept for the audit's replay probes.
const TIP_RECEIPTS: usize = 64;

/// v1's seeded generator (`createRandom` in `bench/metrics.mjs`):
/// FNV-1a over the seed's code points, then mulberry32.
#[derive(Clone, Debug)]
pub struct Random(u32);

impl Random {
    /// A generator for `seed`.
    pub fn new(seed: &str) -> Self {
        let mut state = 0x811c_9dc5_u32;
        for character in seed.chars() {
            state = (state ^ u32::from(character)).wrapping_mul(0x0100_0193);
        }
        Random(state)
    }

    /// The next value in [0, 1).
    pub fn fraction(&mut self) -> f64 {
        self.0 = self.0.wrapping_add(0x6d2b_79f5);
        let mut value = (self.0 ^ (self.0 >> 15)).wrapping_mul(self.0 | 1);
        value ^= value.wrapping_add((value ^ (value >> 7)).wrapping_mul(value | 61));
        f64::from(value ^ (value >> 14)) / 4_294_967_296.0
    }

    /// A whole number in [0, n).
    pub fn below(&mut self, n: usize) -> usize {
        ((self.fraction() * n as f64) as usize).min(n.saturating_sub(1))
    }
}

/// An acknowledged order, as the customer placed it.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AckOrder {
    /// The order's key: canonical JSON of `[tenant, store, id]`.
    pub key: String,
    /// Order ID.
    pub id: String,
    /// `[tenant, store]`.
    pub shop: [String; 2],
    /// Pizzas.
    pub quantity: u64,
}

/// An acknowledged mutation kept for replay probes.
#[derive(Clone, Debug)]
pub struct Receipt {
    /// Operation name.
    pub op: String,
    /// The exact request.
    pub request: Request,
    /// The original answer.
    pub answer: Answer,
}

/// What an uncertain mutation would have done.
#[derive(Clone, Debug)]
pub enum Effect {
    /// Place this order.
    Order(AckOrder),
    /// Tip this store.
    Tip {
        /// Store key.
        shop: String,
        /// Coins.
        amount: u64,
    },
}

/// A mutation whose outcome is unknown.
#[derive(Clone, Debug)]
pub struct Pending {
    /// Operation name.
    pub op: String,
    /// The exact request: same ID, same body.
    pub request: Request,
    /// Its effect if it applied.
    pub effect: Effect,
}

/// A replay whose answer differed from the original receipt.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Mismatch {
    /// Operation name.
    pub op: String,
    /// What differed.
    pub detail: String,
}

/// A failed call, kept as a sample.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FailureSample {
    /// Who failed.
    pub context: String,
    /// Error kind.
    pub kind: String,
    /// Message.
    pub message: String,
}

#[derive(Default)]
struct LedgerInner {
    orders: BTreeMap<String, AckOrder>,
    tips: BTreeMap<String, u64>,
    receipts: Vec<Receipt>,
    tip_receipts: usize,
    pending: Vec<Pending>,
    replay_checks: u64,
    mismatches: Vec<Mismatch>,
    failures: u64,
    samples: Vec<FailureSample>,
}

/// The acknowledged business effects: the audit's oracle.
#[derive(Default)]
pub struct Ledger {
    issued: AtomicU64,
    inner: Mutex<LedgerInner>,
}

/// The ledger's contents at the end of a run.
#[derive(Clone, Debug, Default)]
pub struct LedgerView {
    /// Acknowledged orders by key.
    pub orders: BTreeMap<String, AckOrder>,
    /// Acknowledged tips by store key.
    pub tips: BTreeMap<String, u64>,
    /// Replay probes made.
    pub replay_checks: u64,
    /// Replays that did not match.
    pub mismatches: Vec<Mismatch>,
    /// Failed customer calls.
    pub failures: u64,
    /// Samples of them.
    pub samples: Vec<FailureSample>,
    /// Mutations still uncertain.
    pub pending: usize,
}

impl Ledger {
    fn lock(&self) -> std::sync::MutexGuard<'_, LedgerInner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Take the next order sequence number, if the cap allows one.
    pub fn reserve_order(&self, cap: u64) -> Option<u64> {
        let mut issued = self.issued.load(Ordering::Relaxed);
        while issued < cap {
            match self.issued.compare_exchange_weak(
                issued,
                issued + 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Some(issued + 1),
                Err(now) => issued = now,
            }
        }
        None
    }

    /// Orders issued so far.
    pub fn issued(&self) -> u64 {
        self.issued.load(Ordering::Relaxed)
    }

    /// Record an acknowledged order.
    pub fn order(&self, order: AckOrder, receipt: Receipt) {
        let mut inner = self.lock();
        inner.orders.insert(order.key.clone(), order);
        inner.receipts.push(receipt);
    }

    /// Record an acknowledged tip.
    pub fn tip(&self, shop: &str, amount: u64, receipt: Receipt) {
        let mut inner = self.lock();
        *inner.tips.entry(shop.to_string()).or_default() += amount;
        if inner.tip_receipts < TIP_RECEIPTS {
            inner.tip_receipts += 1;
            inner.receipts.push(receipt);
        }
    }

    /// Record a mutation whose outcome is unknown.
    pub fn uncertain(&self, pending: Pending) {
        self.lock().pending.push(pending);
    }

    /// Take every uncertain mutation, to resolve them.
    pub fn take_pending(&self) -> Vec<Pending> {
        std::mem::take(&mut self.lock().pending)
    }

    /// Record a replay probe and its verdict.
    pub fn replayed(&self, mismatch: Option<Mismatch>) {
        let mut inner = self.lock();
        inner.replay_checks += 1;
        if let Some(mismatch) = mismatch {
            inner.mismatches.push(mismatch);
        }
    }

    /// Record a failed customer call.
    pub fn failed(&self, context: &str, error: &CallError) {
        let mut inner = self.lock();
        inner.failures += 1;
        if inner.samples.len() < FAILURE_SAMPLES {
            inner.samples.push(FailureSample {
                context: context.to_string(),
                kind: error.kind(),
                message: error.to_string().chars().take(500).collect(),
            });
        }
    }

    /// Acknowledged orders so far.
    pub fn acknowledged_orders(&self) -> usize {
        self.lock().orders.len()
    }

    /// Kept receipts, for the audit's replay probes.
    pub fn receipts(&self) -> Vec<Receipt> {
        self.lock().receipts.clone()
    }

    /// The ledger's contents.
    pub fn view(&self) -> LedgerView {
        let inner = self.lock();
        LedgerView {
            orders: inner.orders.clone(),
            tips: inner.tips.clone(),
            replay_checks: inner.replay_checks,
            mismatches: inner.mismatches.clone(),
            failures: inner.failures,
            samples: inner.samples.clone(),
            pending: inner.pending.len(),
        }
    }
}

/// Compare a replay's answer with the original receipt: the same value,
/// revision and log position, exactly what the original call answered.
pub fn compare(op: &str, original: &Answer, replay: &Answer) -> Option<Mismatch> {
    let mut differences = Vec::new();
    if replay.value != original.value {
        differences.push(format!("value {} became {}", original.value, replay.value));
    }
    if replay.revision != original.revision {
        differences.push(format!(
            "revision {:?} became {:?}",
            original.revision, replay.revision
        ));
    }
    if replay.position != original.position {
        differences.push(format!(
            "position {:?} became {:?}",
            original.position, replay.position
        ));
    }
    (!differences.is_empty()).then(|| Mismatch {
        op: op.to_string(),
        detail: differences.join("; "),
    })
}

/// Canonical JSON of a list of strings, as the guest keys rows.
pub fn key(parts: &[&str]) -> String {
    serde_json::to_string(parts).expect("strings serialize")
}

/// Open-loop arrival counters.
#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Arrivals {
    /// Arrivals the schedule offered.
    pub offered: u64,
    /// Arrivals a free slot took.
    pub dispatched: u64,
    /// Arrivals dropped because every slot was busy.
    pub driver_dropped: u64,
    /// Dispatched arrivals that succeeded.
    pub completed: u64,
    /// Dispatched arrivals that failed.
    pub failed: u64,
    /// How late dispatched arrivals started, in ms.
    pub scheduling_lag_ms: HistogramSnapshot,
}

#[derive(Default)]
struct ArrivalState {
    offered: u64,
    dispatched: u64,
    dropped: u64,
    completed: u64,
    failed: u64,
    lag: Histogram,
}

/// Open-loop accounting shared by the scheduler and its arrivals.
#[derive(Default)]
pub struct ArrivalCounters {
    state: Mutex<ArrivalState>,
}

impl ArrivalCounters {
    fn lock(&self) -> std::sync::MutexGuard<'_, ArrivalState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Count `count` arrivals dropped at once.
    pub fn drop_arrivals(&self, count: u64) {
        let mut state = self.lock();
        state.offered += count;
        state.dropped += count;
    }

    /// Count one arrival dispatched `lag_ms` after its schedule.
    pub fn dispatch(&self, lag_ms: f64) {
        let mut state = self.lock();
        state.offered += 1;
        state.dispatched += 1;
        state.lag.record(lag_ms);
    }

    /// Count a dispatched arrival's outcome.
    pub fn finish(&self, ok: bool) {
        let mut state = self.lock();
        if ok {
            state.completed += 1;
        } else {
            state.failed += 1;
        }
    }

    /// The counters.
    pub fn snapshot(&self) -> Arrivals {
        let state = self.lock();
        Arrivals {
            offered: state.offered,
            dispatched: state.dispatched,
            driver_dropped: state.dropped,
            completed: state.completed,
            failed: state.failed,
            scheduling_lag_ms: state.lag.snapshot(),
        }
    }
}

/// When arrival `sequence` is due on an independent clock.
pub fn due(start: Instant, sequence: u64, rate: f64) -> Instant {
    start + Duration::from_secs_f64(sequence as f64 / rate)
}

/// Arrivals due by `elapsed` since the start (the next undue sequence),
/// at most `total`.
pub fn due_by(elapsed: Duration, rate: f64, total: u64) -> u64 {
    ((elapsed.as_secs_f64() * rate).floor() as u64)
        .saturating_add(1)
        .min(total)
}

/// Everything one group's customers need.
pub struct Customers {
    /// The group's client.
    pub client: Arc<BenchClient>,
    /// Settings.
    pub settings: Arc<Settings>,
    /// The group's ledger.
    pub ledger: Arc<Ledger>,
    /// The group's open-loop counters.
    pub arrivals: Arc<ArrivalCounters>,
    /// The group's tenant IDs.
    pub tenants: Vec<String>,
    /// The database holding each tenant.
    pub dbs: Arc<BTreeMap<String, String>>,
    /// The group's workload seed.
    pub seed: String,
}

impl Customers {
    /// The read alias customers use.
    pub fn read_method(&self) -> &'static str {
        match self.settings.read_consistency {
            ReadConsistency::Local => "pizza.shop.local",
            ReadConsistency::Fresh => "pizza.shop",
        }
    }

    /// The database holding `tenant`.
    pub fn db(&self, tenant: &str) -> &str {
        self.dbs
            .get(tenant)
            .map_or_else(|| self.client.db(), String::as_str)
    }

    /// One customer step. `arrival` is the intended start in open
    /// loop. Returns whether the step succeeded.
    pub async fn step(&self, context: &str, random: &mut Random, arrival: Option<Instant>) -> bool {
        let settings = &self.settings;
        let started = arrival.unwrap_or_else(Instant::now);
        let choice = random.fraction();
        let stores = if random.fraction() < settings.hot_probability {
            settings.hot_stores
        } else {
            settings.stores
        };
        let tenant = &self.tenants[random.below(self.tenants.len())];
        let store = format!("store-{}", random.below(stores));
        let order = if choice < 0.30 {
            self.ledger.reserve_order(settings.max_orders)
        } else {
            None
        };
        let result = if let Some(sequence) = order {
            self.order(tenant, &store, sequence, random, started).await
        } else if choice < 0.70 {
            let args = json!([tenant, store]);
            let request = self.client.query(
                self.db(tenant),
                self.read_method(),
                &args,
                Who::Tenant(tenant),
            );
            self.client
                .call(self.read_method(), Class::Read, &request, started)
                .await
                .map(|_| ())
        } else {
            self.tip(tenant, &store, random, started).await
        };
        match result {
            Ok(()) => true,
            Err(error) => {
                self.ledger.failed(context, &error);
                tokio::time::sleep(Duration::from_millis(settings.poll_ms)).await;
                false
            }
        }
    }

    async fn order(
        &self,
        tenant: &str,
        store: &str,
        sequence: u64,
        random: &mut Random,
        started: Instant,
    ) -> Result<(), CallError> {
        let id = format!("pizza-{sequence:06}");
        let quantity = 1 + (random.fraction() * 4.0) as u64;
        let args = json!({"id": id, "shop": [tenant, store], "quantity": quantity});
        let request_id = self.client.next_id();
        let request = self.client.mutate(
            self.db(tenant),
            "pizza.order",
            &args,
            &request_id,
            Who::Tenant(tenant),
        );
        let order = AckOrder {
            key: key(&[tenant, store, &id]),
            id,
            shop: [tenant.to_string(), store.to_string()],
            quantity,
        };
        match self
            .client
            .call("pizza.order", Class::Mutation, &request, started)
            .await
        {
            Ok(answer) => {
                self.ledger.order(
                    order,
                    Receipt {
                        op: "pizza.order".to_string(),
                        request: request.clone(),
                        answer: answer.clone(),
                    },
                );
                self.replay("pizza.order", &request, &answer, random).await
            }
            Err(error) => {
                if error.is_uncertain() {
                    self.ledger.uncertain(Pending {
                        op: "pizza.order".to_string(),
                        request,
                        effect: Effect::Order(order),
                    });
                }
                Err(error)
            }
        }
    }

    async fn tip(
        &self,
        tenant: &str,
        store: &str,
        random: &mut Random,
        started: Instant,
    ) -> Result<(), CallError> {
        let amount = 1 + (random.fraction() * 3.0) as u64;
        let args = json!({"shop": [tenant, store], "amount": amount});
        let request_id = self.client.next_id();
        let request = self.client.mutate(
            self.db(tenant),
            "pizza.tip",
            &args,
            &request_id,
            Who::Tenant(tenant),
        );
        let shop = key(&[tenant, store]);
        match self
            .client
            .call("pizza.tip", Class::Mutation, &request, started)
            .await
        {
            Ok(answer) => {
                self.ledger.tip(
                    &shop,
                    amount,
                    Receipt {
                        op: "pizza.tip".to_string(),
                        request: request.clone(),
                        answer: answer.clone(),
                    },
                );
                self.replay("pizza.tip", &request, &answer, random).await
            }
            Err(error) => {
                if error.is_uncertain() {
                    self.ledger.uncertain(Pending {
                        op: "pizza.tip".to_string(),
                        request,
                        effect: Effect::Tip { shop, amount },
                    });
                }
                Err(error)
            }
        }
    }

    /// With the duplicate rate's probability, send an acknowledged
    /// mutation again and require its original receipt back.
    async fn replay(
        &self,
        op: &str,
        request: &Request,
        original: &Answer,
        random: &mut Random,
    ) -> Result<(), CallError> {
        if random.fraction() >= self.settings.duplicate_rate {
            return Ok(());
        }
        let name = format!("{op}.replay");
        let replay = self
            .client
            .call(&name, Class::Replay, request, Instant::now())
            .await?;
        let mismatch = compare(op, original, &replay);
        let failed = mismatch.is_some();
        self.ledger.replayed(mismatch);
        if failed {
            return Err(CallError::Refused {
                kind: "replay-mismatch".to_string(),
                message: format!("{op}: the replay did not return the original receipt"),
            });
        }
        Ok(())
    }

    /// Closed loop: `concurrency` customers until `deadline` or `stop`.
    pub async fn closed_loop(self: Arc<Self>, deadline: Instant, stop: CancellationToken) {
        let mut tasks = tokio::task::JoinSet::new();
        for index in 0..self.settings.concurrency {
            let customers = self.clone();
            let stop = stop.clone();
            tasks.spawn(async move {
                let mut random = Random::new(&format!("{}:customer:{index}", customers.seed));
                let context = format!("customer {index}");
                while Instant::now() < deadline && !stop.is_cancelled() {
                    customers.step(&context, &mut random, None).await;
                }
            });
        }
        while tasks.join_next().await.is_some() {}
    }

    /// Open loop: arrivals at the offered rate on an independent clock
    /// from `start` until `deadline` (or `stop`), at most `concurrency`
    /// in flight. Saturated arrivals are dropped and counted in O(1),
    /// however high the rate.
    pub async fn open_loop(
        self: Arc<Self>,
        start: Instant,
        deadline: Instant,
        stop: CancellationToken,
    ) {
        let rate = self.settings.offered_rate;
        let total = (deadline.saturating_duration_since(start).as_secs_f64() * rate).ceil() as u64;
        let mut next = 0u64;
        let mut active = tokio::task::JoinSet::new();
        while next < total && !stop.is_cancelled() {
            let scheduled = due(start, next, rate);
            tokio::select! {
                _ = active.join_next(), if !active.is_empty() => continue,
                () = stop.cancelled() => break,
                () = tokio::time::sleep_until(scheduled.into()) => {}
            }
            while active.try_join_next().is_some() {}
            let now = Instant::now();
            if active.len() >= self.settings.concurrency {
                let end = due_by(now.saturating_duration_since(start), rate, total);
                let dropped = end.saturating_sub(next).max(1);
                self.arrivals.drop_arrivals(dropped);
                next = next.saturating_add(dropped).min(total);
                continue;
            }
            self.arrivals
                .dispatch(now.saturating_duration_since(scheduled).as_secs_f64() * 1000.0);
            let customers = self.clone();
            let sequence = next;
            next += 1;
            active.spawn(async move {
                let mut random = Random::new(&format!("{}:arrival:{sequence}", customers.seed));
                let ok = customers
                    .step(&format!("arrival {sequence}"), &mut random, Some(scheduled))
                    .await;
                customers.arrivals.finish(ok);
            });
        }
        while active.join_next().await.is_some() {}
    }

    /// Unmeasured warmup: every loop reads stores until `deadline`.
    /// `shops` are `(tenant, [tenant, store])`.
    pub async fn warm_up(self: Arc<Self>, shops: Vec<(String, Value)>, deadline: Instant) {
        let mut tasks = tokio::task::JoinSet::new();
        for index in 0..self.settings.concurrency {
            let customers = self.clone();
            let (tenant, shop) = shops[index % shops.len()].clone();
            tasks.spawn(async move {
                let request = customers.client.query(
                    customers.db(&tenant),
                    customers.read_method(),
                    &shop,
                    Who::Tenant(&tenant),
                );
                while Instant::now() < deadline {
                    let result = customers
                        .client
                        .call("warmup.read", Class::Control, &request, Instant::now())
                        .await;
                    if result.is_err() {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                }
            });
        }
        while tasks.join_next().await.is_some() {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn random_matches_v1_javascript() {
        // Generated by bench/metrics.mjs createRandom, Unicode code
        // points included (v1's driver test).
        let mut random = Random::new("pizza-🌸");
        let expected = [
            0.25823454186320305,
            0.30264041805639863,
            0.8295244711916894,
            0.45414901175536215,
            0.8598633082583547,
            0.41006043110974133,
            0.9161705092992634,
            0.2261476602870971,
        ];
        for value in expected {
            assert_eq!(random.fraction(), value);
        }
    }

    #[test]
    fn the_order_cap_admits_exactly_cap_orders() {
        let ledger = Ledger::default();
        let taken: Vec<u64> = (0..10).filter_map(|_| ledger.reserve_order(3)).collect();
        assert_eq!(taken, vec![1, 2, 3]);
        assert_eq!(ledger.issued(), 3);
    }

    #[test]
    fn open_loop_schedule_never_moves_and_drops_count_in_bulk() {
        let start = Instant::now();
        assert_eq!(due(start, 10, 100.0) - start, Duration::from_millis(100));
        assert_eq!(due(start, 11, 100.0) - start, Duration::from_millis(110));
        // Three seconds late at 100/s: everything due by then (301
        // arrivals up to and including the one due at 3 s) is accounted.
        assert_eq!(due_by(Duration::from_secs(3), 100.0, 1_000), 301);
        assert_eq!(due_by(Duration::from_secs(30), 100.0, 1_000), 1_000);
        let counters = ArrivalCounters::default();
        counters.dispatch(2.0);
        counters.dispatch(4.0);
        counters.drop_arrivals(5);
        counters.finish(true);
        counters.finish(false);
        let arrivals = counters.snapshot();
        // Offered is exactly dispatched plus dropped; dispatched is
        // exactly completed plus failed.
        assert_eq!(arrivals.offered, 7);
        assert_eq!(
            arrivals.offered,
            arrivals.dispatched + arrivals.driver_dropped
        );
        assert_eq!(arrivals.dispatched, arrivals.completed + arrivals.failed);
        assert_eq!(arrivals.scheduling_lag_ms.samples, 2);
    }

    #[test]
    fn replays_compare_value_revision_and_position() {
        let original = Answer {
            value: json!({"tips": 3}),
            revision: Some(10),
            position: Some(20),
            member: 0,
        };
        let same = Answer {
            member: 2,
            ..original.clone()
        };
        assert_eq!(compare("pizza.tip", &original, &same), None);
        let other = Answer {
            value: json!({"tips": 4}),
            revision: Some(11),
            ..original.clone()
        };
        let mismatch = compare("pizza.tip", &original, &other).unwrap();
        assert!(mismatch.detail.contains("value"));
        assert!(mismatch.detail.contains("revision"));
        // The same value at another revision or position is a mismatch:
        // a replay answers exactly what the original call did.
        let moved = Answer {
            revision: Some(12),
            position: Some(0),
            ..original.clone()
        };
        let mismatch = compare("pizza.tip", &original, &moved).unwrap();
        assert!(mismatch.detail.contains("revision") && mismatch.detail.contains("position"));
    }

    #[test]
    fn keys_are_canonical_json() {
        assert_eq!(key(&["tenant-0", "store-1"]), r#"["tenant-0","store-1"]"#);
    }
}
