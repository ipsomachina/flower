//! An in-memory Flower queue for testing workers without a server
//! (feature `testing`): [`FakeQueue`] implements [`QueueClient`] with
//! the engine's queue semantics (due order, leases that lapse and
//! redeliver, attempts spent per claim, death past the budget, token
//! checks, owners, the waiter line and its turn, chained claims), on
//! tokio's clock so paused-time tests drive leases exactly. Faults
//! inject refusals for load, transport failures and poll latency;
//! counters show what the worker did.
//!
//! The conformance tests run the same scenarios against a live server
//! and this fake and compare the outcomes.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use flower_client::error::FlowerError;
use flower_client::work::{Claim, QueueItem, QueuePoll, QueueReadiness, QueueReport, QueueStats};
use tokio::time::Instant;

use crate::client::QueueClient;

/// Most items one poll returns, as the engine.
const MAX_POLL_LIMIT: usize = 100;
/// Longest claim lease, as the engine: one hour.
const MAX_LEASE_MS: u64 = 60 * 60 * 1000;
/// Longest delay before an item is due again, as the engine: 30 days.
const MAX_DELAY_MS: u64 = 30 * 24 * 60 * 60 * 1000;
/// Longest place in line, as the engine: one hour.
const MAX_WAIT_MS: u64 = 60 * 60 * 1000;
/// How long the first owner in line has new work to itself, by default.
const DEFAULT_TURN_MS: u64 = 1_000;
/// Most items a claim takes when it names no limit, as the server.
const DEFAULT_LIMIT: usize = 10;
/// A claim's lease when it names none, as the server.
const DEFAULT_LEASE_MS: u64 = 30_000;

/// An item's state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum State {
    Pending,
    Claimed,
    Dead,
}

/// One stored item.
#[derive(Clone, Debug)]
struct Item {
    payload: serde_json::Value,
    state: State,
    attempts: u32,
    max_attempts: u32,
    claimed_until_ms: u64,
    token: Option<String>,
    enqueued_ms: u64,
    due_ms: u64,
    scope: String,
    priority: i64,
    fence: u64,
    owner: Option<String>,
}

impl Item {
    /// When the item becomes claimable: its due time, or its lease end.
    fn order_ms(&self) -> u64 {
        match self.state {
            State::Claimed => self.claimed_until_ms,
            _ => self.due_ms,
        }
    }

    /// Whether its lease lapsed on its last attempt by `now`: dead from
    /// the lease's end, as the engine reads it, before any claim moves it.
    fn lapsed_dead(&self, now: u64) -> bool {
        self.state == State::Claimed
            && self.claimed_until_ms <= now
            && self.attempts >= self.max_attempts
    }

    /// Its state as of `now`.
    fn state_at(&self, now: u64) -> State {
        if self.lapsed_dead(now) {
            State::Dead
        } else {
            self.state
        }
    }

    /// When it became claimable, if it is claimable at `now`.
    fn available_at(&self, now: u64) -> Option<u64> {
        match self.state {
            State::Pending if self.due_ms <= now => Some(self.due_ms.max(self.enqueued_ms)),
            State::Claimed if self.claimed_until_ms <= now && !self.lapsed_dead(now) => {
                Some(self.claimed_until_ms)
            }
            _ => None,
        }
    }
}

/// One owner's place in a scope's line.
#[derive(Clone, Debug)]
struct Place {
    owner: String,
    since: u64,
    expires: u64,
}

/// A fault the next polls meet.
#[derive(Clone, Debug)]
enum Fault {
    /// A refusal for load (counted in `overloads`).
    Overloaded,
    /// Any other failure.
    Error(FlowerError),
}

/// Database, queue and scope.
type ScopeKey = (String, String, String);

#[derive(Default)]
struct Inner {
    /// Items per database and queue, by ID.
    items: BTreeMap<(String, String), BTreeMap<String, Item>>,
    next_id: u64,
    next_token: u64,
    /// The last fencing token per database, queue and scope.
    fences: BTreeMap<ScopeKey, u64>,
    /// Each scope's line, by when each owner started waiting.
    lines: BTreeMap<ScopeKey, Vec<Place>>,
    turn_ms: Option<u64>,
    faults: Vec<Fault>,
    /// Faults the next chained reports meet: the error, and whether the
    /// report committed (claim and all) before it.
    report_faults: Vec<(FlowerError, bool)>,
    latency: Duration,
    answer_latency: Duration,
    overloads: u64,
    polls: u64,
    polls_in_flight: usize,
    max_polls_in_flight: usize,
    max_claimed: usize,
    readiness_reads: u64,
    reports: u64,
    chained_claims: u64,
}

impl Inner {
    fn claimed_now(&self) -> usize {
        self.items
            .values()
            .flat_map(BTreeMap::values)
            .filter(|item| item.state == State::Claimed)
            .count()
    }

    fn turn_ms(&self) -> u64 {
        self.turn_ms.unwrap_or(DEFAULT_TURN_MS)
    }

    /// When the scope's first claimable item became claimable.
    fn available(&self, db: &str, queue: &str, scope: &str, now: u64) -> Option<u64> {
        self.items
            .get(&(db.to_string(), queue.to_string()))
            .into_iter()
            .flat_map(BTreeMap::values)
            .filter(|item| item.scope == scope)
            .filter_map(|item| item.available_at(now))
            .min()
    }

    /// The scope's line, without places that ran out.
    fn line(&mut self, key: &ScopeKey, now: u64) -> &mut Vec<Place> {
        let line = self.lines.entry(key.clone()).or_default();
        line.retain(|place| place.expires > now);
        line
    }
}

/// The in-memory queue. Clones share state.
#[derive(Clone)]
pub struct FakeQueue {
    inner: Arc<Mutex<Inner>>,
    origin: Instant,
    epoch_ms: u64,
}

impl Default for FakeQueue {
    fn default() -> Self {
        Self::new()
    }
}

/// A decided queue failure, as the server reports them.
fn refused(message: &str) -> FlowerError {
    FlowerError::Business {
        code: "INVALID_VALUE".to_string(),
        message: message.to_string(),
        revision: None,
        position: None,
        details: None,
    }
}

impl FakeQueue {
    /// An empty queue on tokio's clock.
    pub fn new() -> Self {
        FakeQueue {
            inner: Arc::new(Mutex::new(Inner::default())),
            origin: Instant::now(),
            epoch_ms: 1_000_000,
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Milliseconds on the queue's clock.
    pub fn now_ms(&self) -> u64 {
        self.epoch_ms + self.origin.elapsed().as_millis() as u64
    }

    /// Enqueue a payload due now; returns its ID.
    pub fn enqueue(
        &self,
        db: &str,
        queue: &str,
        payload: serde_json::Value,
        max_attempts: u32,
    ) -> String {
        self.enqueue_after(db, queue, payload, Duration::ZERO, max_attempts)
    }

    /// Enqueue a payload due after `delay`; returns its ID.
    pub fn enqueue_after(
        &self,
        db: &str,
        queue: &str,
        payload: serde_json::Value,
        delay: Duration,
        max_attempts: u32,
    ) -> String {
        self.enqueue_in(db, queue, "", 0, payload, delay, max_attempts)
    }

    /// Enqueue a payload into one scope at a priority (lower claims
    /// first), due after `delay`; returns its ID.
    #[allow(clippy::too_many_arguments)]
    pub fn enqueue_in(
        &self,
        db: &str,
        queue: &str,
        scope: &str,
        priority: i64,
        payload: serde_json::Value,
        delay: Duration,
        max_attempts: u32,
    ) -> String {
        let now = self.now_ms();
        let mut inner = self.lock();
        inner.next_id += 1;
        let id = format!("{:032x}", inner.next_id);
        let item = Item {
            payload,
            state: State::Pending,
            attempts: 0,
            max_attempts: max_attempts.max(1),
            claimed_until_ms: 0,
            token: None,
            enqueued_ms: now,
            due_ms: now + delay.as_millis() as u64,
            scope: scope.to_string(),
            priority,
            fence: 0,
            owner: None,
        };
        inner
            .items
            .entry((db.to_string(), queue.to_string()))
            .or_default()
            .insert(id.clone(), item);
        id
    }

    /// Live counts, as `GET /v1/queue/stats` reports them.
    pub fn stats(&self, db: &str, queue: &str) -> QueueStats {
        let now = self.now_ms();
        let inner = self.lock();
        let mut stats = QueueStats {
            pending: 0,
            claimed: 0,
            dead: 0,
            completed: 0,
        };
        let key = (db.to_string(), queue.to_string());
        for item in inner.items.get(&key).into_iter().flat_map(BTreeMap::values) {
            match item.state_at(now) {
                State::Pending => stats.pending += 1,
                State::Claimed => stats.claimed += 1,
                State::Dead => stats.dead += 1,
            }
        }
        stats
    }

    /// One item in any state.
    pub fn item(&self, db: &str, queue: &str, id: &str) -> Option<QueueItem> {
        let now = self.now_ms();
        let inner = self.lock();
        let item = inner
            .items
            .get(&(db.to_string(), queue.to_string()))?
            .get(id)?;
        Some(QueueItem {
            id: id.to_string(),
            payload: item.payload.clone(),
            state: match item.state_at(now) {
                State::Pending => "pending",
                State::Claimed => "claimed",
                State::Dead => "dead",
            }
            .to_string(),
            attempts: item.attempts,
            max_attempts: item.max_attempts,
            enqueued_ms: item.enqueued_ms,
            due_ms: item.due_ms,
            scope: item.scope.clone(),
            priority: item.priority,
            fence: item.fence,
            owner: item.owner.clone(),
            name: None,
            group: None,
            turn: 0,
            result: serde_json::Value::Null,
        })
    }

    /// The owners waiting in a scope's line now, first first.
    pub fn line(&self, db: &str, queue: &str, scope: &str) -> Vec<String> {
        let now = self.now_ms();
        let key = (db.to_string(), queue.to_string(), scope.to_string());
        self.lock()
            .line(&key, now)
            .iter()
            .map(|place| place.owner.clone())
            .collect()
    }

    /// How long the first owner in line has new work to itself (default
    /// one second, as the engine).
    pub fn turn(&self, turn: Duration) {
        self.lock().turn_ms = Some(turn.as_millis() as u64);
    }

    /// The next `count` polls are refused for load.
    pub fn refuse_polls(&self, count: usize) {
        let mut inner = self.lock();
        inner
            .faults
            .extend(std::iter::repeat_n(Fault::Overloaded, count));
    }

    /// The next `count` polls fail with `error`.
    pub fn fail_polls(&self, count: usize, error: FlowerError) {
        let mut inner = self.lock();
        inner
            .faults
            .extend(std::iter::repeat_n(Fault::Error(error), count));
    }

    /// Every poll takes this long to answer.
    pub fn poll_latency(&self, latency: Duration) {
        self.lock().latency = latency;
    }

    /// Every poll commits at once but answers this much later, as a
    /// server whose answer is slow: a poll its caller dropped has
    /// still claimed.
    pub fn answer_latency(&self, latency: Duration) {
        self.lock().answer_latency = latency;
    }

    /// The next `count` chained reports fail with `error`, after
    /// committing, claim and all, when `committed`.
    pub fn fail_chained_reports(&self, count: usize, error: FlowerError, committed: bool) {
        let mut inner = self.lock();
        inner
            .report_faults
            .extend(std::iter::repeat_n((error, committed), count));
    }

    /// Polls received so far.
    pub fn polls(&self) -> u64 {
        self.lock().polls
    }

    /// Readiness reads received so far.
    pub fn readiness_reads(&self) -> u64 {
        self.lock().readiness_reads
    }

    /// Reports (complete, fail, kill, release) received so far, chained
    /// or not.
    pub fn reports(&self) -> u64 {
        self.lock().reports
    }

    /// Items chained reports claimed so far.
    pub fn chained_claims(&self) -> u64 {
        self.lock().chained_claims
    }

    /// Most polls ever in flight at once.
    pub fn max_polls_in_flight(&self) -> usize {
        self.lock().max_polls_in_flight
    }

    /// Most items ever claimed at once.
    pub fn max_claimed(&self) -> usize {
        self.lock().max_claimed
    }

    /// Resolve a claim as of `now`, checking its token: one that lapsed
    /// on its last attempt is dead, as the engine refuses it.
    fn claimed<'a>(
        inner: &'a mut Inner,
        db: &str,
        queue: &str,
        id: &str,
        token: &str,
        now: u64,
    ) -> Result<&'a mut Item, FlowerError> {
        let item = inner
            .items
            .get_mut(&(db.to_string(), queue.to_string()))
            .and_then(|items| items.get_mut(id))
            .filter(|item| item.state == State::Claimed)
            .ok_or_else(|| refused("unknown queue item"))?;
        if item.token.as_deref() != Some(token) {
            return Err(refused("claim token mismatch"));
        }
        if item.lapsed_dead(now) {
            return Err(refused("claim lapsed on its last attempt"));
        }
        Ok(item)
    }

    /// Settle a claim as a report does.
    fn settle(
        inner: &mut Inner,
        db: &str,
        queue: &str,
        id: &str,
        token: &str,
        report: Settle,
        now: u64,
    ) -> Result<(), FlowerError> {
        if let Settle::Fail(delay_ms) | Settle::Release(delay_ms) = report
            && delay_ms > MAX_DELAY_MS
        {
            return Err(refused("report delay is too long"));
        }
        inner.reports += 1;
        let item = Self::claimed(inner, db, queue, id, token, now)?;
        if matches!(report, Settle::Complete) {
            if let Some(items) = inner.items.get_mut(&(db.to_string(), queue.to_string())) {
                items.remove(id);
            }
            return Ok(());
        }
        item.token = None;
        item.claimed_until_ms = 0;
        item.state = match report {
            Settle::Fail(_) if item.attempts >= item.max_attempts => State::Dead,
            Settle::Fail(delay_ms) | Settle::Release(delay_ms) => {
                item.due_ms = now + delay_ms;
                State::Pending
            }
            Settle::Kill | Settle::Complete => State::Dead,
        };
        Ok(())
    }

    /// Claim the first claimable items of one scope (lowest priority
    /// first, then the longest waiting), as the engine does: the line's
    /// first owner keeps new work to itself for the turn and loses its
    /// place once it let work wait a whole turn; a claim with a wait
    /// that comes back short keeps its owner's place.
    fn take(
        inner: &mut Inner,
        db: &str,
        queue: &str,
        poll: &QueuePoll<'_>,
        now: u64,
    ) -> Vec<Claim> {
        let scope = poll.scope;
        let limit = poll.limit.unwrap_or(DEFAULT_LIMIT).min(MAX_POLL_LIMIT);
        let lease_ms = poll.lease_ms.unwrap_or(DEFAULT_LEASE_MS);
        let line_key = (db.to_string(), queue.to_string(), scope.to_string());
        let available = inner.available(db, queue, scope, now);
        let turn_ms = inner.turn_ms();
        let line = inner.line(&line_key, now);
        if limit > 0
            && let Some(head) = line.first()
            && poll.owner != Some(head.owner.as_str())
            && available.is_some_and(|since| now >= since + turn_ms)
        {
            line.remove(0);
        }
        let key = (db.to_string(), queue.to_string());
        // A lease that lapsed on the item's last attempt kills it rather
        // than redelivering it, as the engine does.
        if let Some(items) = inner.items.get_mut(&key) {
            for item in items.values_mut() {
                if item.scope == scope && item.lapsed_dead(now) {
                    item.state = State::Dead;
                    item.token = None;
                    item.due_ms = item.claimed_until_ms;
                }
            }
        }
        let mut candidates: Vec<(i64, u64, String)> = inner
            .items
            .get(&key)
            .into_iter()
            .flat_map(BTreeMap::iter)
            .filter(|(_, item)| item.scope == scope && item.available_at(now).is_some())
            .map(|(id, item)| (item.priority, item.order_ms(), id.clone()))
            .collect();
        candidates.sort();
        candidates.truncate(limit);
        let fence_key = line_key.clone();
        let mut fence = inner.fences.get(&fence_key).copied().unwrap_or(0);
        let mut claims = Vec::new();
        for (_, _, id) in candidates {
            inner.next_token += 1;
            let token = format!("token-{}", inner.next_token);
            fence += 1;
            let Some(item) = inner
                .items
                .get_mut(&key)
                .and_then(|items| items.get_mut(&id))
            else {
                continue;
            };
            item.state = State::Claimed;
            item.attempts = item.attempts.saturating_add(1);
            item.claimed_until_ms = now + lease_ms;
            item.token = Some(token.clone());
            item.fence = fence;
            item.owner = poll.owner.map(str::to_string);
            claims.push(Claim {
                id,
                payload: item.payload.clone(),
                token,
                attempts: item.attempts,
                fence,
                scope: item.scope.clone(),
                priority: item.priority,
                expires_ms: item.claimed_until_ms,
                name: None,
                group: None,
                claimed_ms: now,
            });
        }
        inner.fences.insert(fence_key, fence);
        if let (Some(wait_ms), Some(owner)) = (poll.wait_ms, poll.owner) {
            let line = inner.line(&line_key, now);
            let held = line.iter().position(|place| place.owner == owner);
            if claims.len() >= limit || wait_ms == 0 {
                if let Some(held) = held {
                    line.remove(held);
                }
            } else {
                let expires = now + wait_ms;
                match held {
                    Some(held) => line[held].expires = expires,
                    None => line.push(Place {
                        owner: owner.to_string(),
                        since: now,
                        expires,
                    }),
                }
                line.sort_by_key(|place| place.since);
            }
        }
        let claimed = inner.claimed_now();
        inner.max_claimed = inner.max_claimed.max(claimed);
        claims
    }

    /// Count a poll, meet its fault or latency, then claim.
    async fn poll_with(
        &self,
        db: &str,
        queue: &str,
        poll: &QueuePoll<'_>,
    ) -> Result<Vec<Claim>, FlowerError> {
        let (latency, fault) = {
            let mut inner = self.lock();
            inner.polls += 1;
            inner.polls_in_flight += 1;
            inner.max_polls_in_flight = inner.max_polls_in_flight.max(inner.polls_in_flight);
            let fault = (!inner.faults.is_empty()).then(|| inner.faults.remove(0));
            (inner.latency, fault)
        };
        if !latency.is_zero() {
            tokio::time::sleep(latency).await;
        }
        let lease_ms = poll.lease_ms.unwrap_or(DEFAULT_LEASE_MS);
        let result = match fault {
            Some(Fault::Overloaded) => {
                self.lock().overloads += 1;
                Err(FlowerError::RetryExhausted("status 503".to_string()))
            }
            Some(Fault::Error(error)) => Err(error),
            None if lease_ms == 0 || lease_ms > MAX_LEASE_MS => Err(refused("lease out of range")),
            None if poll.wait_ms.is_some_and(|wait| wait > MAX_WAIT_MS) => {
                Err(refused("line wait is too long"))
            }
            None if poll.wait_ms.is_some() && poll.owner.is_none() => {
                Err(refused("waiting in line needs an owner"))
            }
            None => {
                let now = self.now_ms();
                Ok(Self::take(&mut self.lock(), db, queue, poll, now))
            }
        };
        let answer = self.lock().answer_latency;
        if !answer.is_zero() {
            tokio::time::sleep(answer).await;
        }
        self.lock().polls_in_flight -= 1;
        result
    }
}

/// How a report settles a claim, in the fake.
#[derive(Clone, Copy, Debug)]
enum Settle {
    Complete,
    Fail(u64),
    Kill,
    Release(u64),
}

impl QueueClient for FakeQueue {
    async fn poll(
        &self,
        db: &str,
        queue: &str,
        limit: usize,
        lease_ms: u64,
    ) -> Result<Vec<Claim>, FlowerError> {
        self.poll_scope(db, queue, "", limit, lease_ms).await
    }

    async fn poll_scope(
        &self,
        db: &str,
        queue: &str,
        scope: &str,
        limit: usize,
        lease_ms: u64,
    ) -> Result<Vec<Claim>, FlowerError> {
        let poll = QueuePoll {
            scope,
            limit: Some(limit),
            lease_ms: Some(lease_ms),
            ..QueuePoll::default()
        };
        self.poll_with(db, queue, &poll).await
    }

    async fn claim(
        &self,
        db: &str,
        queue: &str,
        poll: &QueuePoll<'_>,
    ) -> Result<Vec<Claim>, FlowerError> {
        self.poll_with(db, queue, poll).await
    }

    async fn readiness(
        &self,
        db: &str,
        queue: &str,
        scope: &str,
        owner: &str,
    ) -> Result<QueueReadiness, FlowerError> {
        let now = self.now_ms();
        let mut inner = self.lock();
        inner.readiness_reads += 1;
        let Some(since) = inner.available(db, queue, scope, now) else {
            return Ok(QueueReadiness {
                ready: false,
                next_ms: None,
            });
        };
        let turn_end = since + inner.turn_ms();
        let key = (db.to_string(), queue.to_string(), scope.to_string());
        let ready = match inner.line(&key, now).first() {
            None => true,
            Some(head) => head.owner == owner || now >= turn_end,
        };
        Ok(QueueReadiness {
            ready,
            next_ms: (!ready).then_some(turn_end),
        })
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
        let settle = match report {
            QueueReport::Complete(None) => Settle::Complete,
            QueueReport::Complete(Some(_)) => {
                return Err(refused("the fake queue keeps no results"));
            }
            QueueReport::Fail(delay_ms) => Settle::Fail(delay_ms),
            QueueReport::Kill => Settle::Kill,
        };
        let now = self.now_ms();
        // The report and its claim in one step, as one commit.
        let mut inner = self.lock();
        let fault = (!inner.report_faults.is_empty()).then(|| inner.report_faults.remove(0));
        if let Some((error, false)) = fault {
            return Err(error);
        }
        Self::settle(&mut inner, db, queue, id, token, settle, now)?;
        let claims = Self::take(&mut inner, db, queue, next, now);
        inner.chained_claims += claims.len() as u64;
        match fault {
            Some((error, _)) => Err(error),
            None => Ok(claims),
        }
    }

    async fn heartbeat(
        &self,
        db: &str,
        queue: &str,
        id: &str,
        token: &str,
        lease_ms: u64,
    ) -> Result<(), FlowerError> {
        let now = self.now_ms();
        let mut inner = self.lock();
        let item = Self::claimed(&mut inner, db, queue, id, token, now)?;
        if item.claimed_until_ms <= now {
            return Err(refused("claim lapsed"));
        }
        item.claimed_until_ms = now + lease_ms;
        Ok(())
    }

    async fn complete(
        &self,
        db: &str,
        queue: &str,
        id: &str,
        token: &str,
    ) -> Result<(), FlowerError> {
        let now = self.now_ms();
        Self::settle(
            &mut self.lock(),
            db,
            queue,
            id,
            token,
            Settle::Complete,
            now,
        )
    }

    async fn fail(
        &self,
        db: &str,
        queue: &str,
        id: &str,
        token: &str,
        delay_ms: u64,
    ) -> Result<(), FlowerError> {
        let now = self.now_ms();
        let settle = Settle::Fail(delay_ms);
        Self::settle(&mut self.lock(), db, queue, id, token, settle, now)
    }

    async fn kill(&self, db: &str, queue: &str, id: &str, token: &str) -> Result<(), FlowerError> {
        let now = self.now_ms();
        Self::settle(&mut self.lock(), db, queue, id, token, Settle::Kill, now)
    }

    async fn release(
        &self,
        db: &str,
        queue: &str,
        id: &str,
        token: &str,
    ) -> Result<(), FlowerError> {
        let now = self.now_ms();
        let settle = Settle::Release(0);
        Self::settle(&mut self.lock(), db, queue, id, token, settle, now)
    }

    fn overloads(&self) -> u64 {
        self.lock().overloads
    }
}
