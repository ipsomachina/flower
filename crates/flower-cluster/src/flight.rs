//! Identical queries in flight join one evaluation.
//!
//! A query that misses the result cache evaluates; every identical call
//! arriving while it does would miss as well and evaluate the same
//! thing. [`Flights`] lets the first such call lead and the others wait
//! for it, then answer as any call does: each has proved its own read
//! before it waited, and once the leader lands it authorizes as itself
//! on a snapshot of its own and looks the result cache up, where the
//! leader's result answers it if its certificate holds there. Joining
//! only decides who waits: no call ever takes another's answer, so a
//! joined answer, its errors and its `Server-Timing` are a cached
//! call's (see [`flower_engine::query_cache`]).
//!
//! Calls join by the call as presented (database, module, name,
//! canonical arguments, asserted principal, credentials, whether the
//! name is an HTTP alias, fuel and timestamp overrides) and by the
//! database's revision once the caller's fence held. Identical calls at
//! one revision share the leader's cached result, as long as their
//! module's authorization decides the same for the same input there.
//! A call at a newer revision leads a flight of its own, since what the
//! older one keeps may not hold for it.
//!
//! A waiter waits once. When the leader's result did not enter the
//! cache (it polled the clock, failed, or read something no dependency
//! names), its waiters evaluate alone, and the call is marked solo:
//! later identical calls evaluate side by side, without waiting, until
//! one of them keeps its result again. A leader whose caller went away
//! releases its waiters, who look for a flight once more.
//!
//! The table is bounded in bytes, as v1's (`FLOWER_QUERY_FLIGHT_BYTES`):
//! past its budget calls evaluate as they would without it, and solo
//! marks, being hints, leave first.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use flower_engine::engine::CallOptions;
use flower_engine::json::render;
use flower_sdk::{FlowerType, Value};
use tokio::sync::watch;

/// Default byte budget, as v1's.
pub const DEFAULT_FLIGHT_BYTES: usize = 512 * 1024;
/// Fixed cost charged per flight or solo mark on top of its key.
const OVERHEAD: usize = 256;

/// How a flight ended, as its waiters see it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Landing {
    /// Its result is in the query cache: look it up.
    Kept,
    /// It answered or failed without keeping a result: evaluate alone.
    Unkept,
    /// Its leader stopped before it answered.
    Abandoned,
}

/// Flight counters since the member opened, and the table's occupancy.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FlightStats {
    /// Calls that led a flight.
    pub led: u64,
    /// Calls that waited for one.
    pub joined: u64,
    /// Calls that evaluated alone: marked solo, or the table was full.
    pub alone: u64,
    /// Flights in the air.
    pub flights: usize,
    /// Calls marked solo.
    pub solo: usize,
    /// Bytes charged.
    pub bytes: usize,
    /// The bytes it may hold; zero turns joining off.
    pub budget: usize,
}

#[derive(Debug, Default)]
struct Table {
    /// Flights in the air, by call and revision.
    running: HashMap<Arc<[u8]>, watch::Receiver<Option<Landing>>>,
    /// Calls whose last flight kept nothing.
    solo: HashSet<Arc<[u8]>>,
    /// The same marks, oldest first, for eviction: each key is one
    /// allocation both hold, charged once.
    solo_order: VecDeque<Arc<[u8]>>,
    bytes: usize,
}

impl Table {
    /// Forget that `call` is solo.
    fn unmark(&mut self, call: &[u8]) {
        if self.solo.remove(call) {
            self.bytes -= call.len() + OVERHEAD;
            self.solo_order.retain(|marked| **marked != *call);
        }
    }

    /// Forget the oldest solo mark; `false` when there is none.
    fn evict_mark(&mut self) -> bool {
        let Some(oldest) = self.solo_order.pop_front() else {
            return false;
        };
        self.solo.remove(&oldest);
        self.bytes -= oldest.len() + OVERHEAD;
        true
    }

    /// Mark `call` solo, evicting older marks past `budget`; a mark
    /// that does not fit is dropped.
    fn mark(&mut self, call: &[u8], budget: usize) {
        if self.solo.contains(call) {
            return;
        }
        let cost = call.len() + OVERHEAD;
        while self.bytes + cost > budget {
            if !self.evict_mark() {
                return;
            }
        }
        self.bytes += cost;
        let call: Arc<[u8]> = call.into();
        self.solo_order.push_back(call.clone());
        self.solo.insert(call);
    }
}

/// One member's flights.
#[derive(Debug)]
pub struct Flights {
    table: Mutex<Table>,
    budget: AtomicUsize,
    led: AtomicU64,
    joined: AtomicU64,
    alone: AtomicU64,
}

impl Default for Flights {
    fn default() -> Self {
        Flights {
            table: Mutex::default(),
            budget: AtomicUsize::new(DEFAULT_FLIGHT_BYTES),
            led: AtomicU64::new(0),
            joined: AtomicU64::new(0),
            alone: AtomicU64::new(0),
        }
    }
}

/// A call's turn: lead a flight, join one, or evaluate alone.
#[derive(Debug)]
pub enum Turn {
    /// Evaluate, then [`Leader::land`].
    Lead(Leader),
    /// Wait for [`Waiter::landing`], then evaluate as usual.
    Join(Waiter),
    /// Evaluate, then [`Alone::land`].
    Alone(Alone),
}

/// A flight's leader. Dropped without landing (its caller went away),
/// it releases its waiters as [`Landing::Abandoned`].
#[derive(Debug)]
pub struct Leader {
    flights: Arc<Flights>,
    /// Call and revision, one allocation with the table's key.
    key: Arc<[u8]>,
    /// The call's part of `key`.
    call: usize,
    sender: Option<watch::Sender<Option<Landing>>>,
}

impl Leader {
    /// The flight answered; `kept` when its result entered the cache.
    pub fn land(mut self, kept: bool) {
        let landing = if kept { Landing::Kept } else { Landing::Unkept };
        {
            let mut table = self.flights.lock();
            self.flights.remove(&mut table, &self.key);
            let call = &self.key[..self.call];
            if kept {
                table.unmark(call);
            } else {
                table.mark(call, self.flights.budget());
            }
        }
        if let Some(sender) = self.sender.take() {
            sender.send_replace(Some(landing));
        }
    }
}

impl Drop for Leader {
    fn drop(&mut self) {
        if self.sender.is_some() {
            let mut table = self.flights.lock();
            self.flights.remove(&mut table, &self.key);
            // Dropping the sender releases the waiters.
        }
    }
}

/// A call waiting for a flight.
#[derive(Debug)]
pub struct Waiter {
    receiver: watch::Receiver<Option<Landing>>,
}

impl Waiter {
    /// Wait for the flight to land.
    pub async fn landing(mut self) -> Landing {
        loop {
            if let Some(landing) = *self.receiver.borrow_and_update() {
                return landing;
            }
            if self.receiver.changed().await.is_err() {
                return self.receiver.borrow().unwrap_or(Landing::Abandoned);
            }
        }
    }
}

/// A call evaluating without a flight.
#[derive(Debug)]
pub struct Alone {
    /// The flights and the call, when the call was marked solo.
    solo: Option<(Arc<Flights>, Vec<u8>)>,
}

impl Alone {
    /// The call answered; `kept` when its result entered the cache,
    /// which lifts a solo mark.
    pub fn land(self, kept: bool) {
        if let (true, Some((flights, call))) = (kept, self.solo) {
            flights.lock().unmark(&call);
        }
    }
}

impl Flights {
    fn lock(&self) -> std::sync::MutexGuard<'_, Table> {
        self.table.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn budget(&self) -> usize {
        self.budget.load(Ordering::Acquire)
    }

    fn remove(&self, table: &mut Table, key: &[u8]) {
        if table.running.remove(key).is_some() {
            table.bytes -= key.len() + OVERHEAD;
        }
    }

    /// A call that waited for a flight, then evaluated, kept its result:
    /// it is no longer solo, if its leader had marked it.
    pub fn kept(&self, call: &[u8]) {
        self.lock().unmark(call);
    }

    /// Replace the byte budget; zero turns joining off. Solo marks over
    /// a smaller budget leave as new ones come.
    pub fn set_budget(&self, bytes: usize) {
        self.budget.store(bytes, Ordering::Release);
    }

    /// The flight key of a call as presented: `None` when an input does
    /// not render, which leaves the call alone.
    pub fn call_key(
        db: &[u8],
        module: &str,
        name: &str,
        args: &Value,
        options: &CallOptions,
    ) -> Option<Vec<u8>> {
        let mut key = Vec::new();
        let mut push = |bytes: &[u8]| {
            key.extend_from_slice(&(bytes.len() as u64).to_be_bytes());
            key.extend_from_slice(bytes);
        };
        push(db);
        push(module.as_bytes());
        push(name.as_bytes());
        push(&render(args).ok()?);
        match &options.principal {
            None => push(b""),
            Some(principal) => push(&render(&principal.encode().ok()?).ok()?),
        }
        match &options.credentials {
            None => push(b""),
            Some(credentials) => push(&render(credentials).ok()?),
        }
        push(&[u8::from(options.external)]);
        push(&options.timestamp_ms.map_or([0; 9], prefixed));
        Some(key)
    }

    /// Take a turn for `call` on a snapshot at `revision`.
    pub fn enter(self: &Arc<Self>, call: Vec<u8>, revision: u64) -> Turn {
        let budget = self.budget();
        let mut table = self.lock();
        if budget == 0 {
            drop(table);
            self.alone.fetch_add(1, Ordering::Relaxed);
            return Turn::Alone(Alone { solo: None });
        }
        if table.solo.contains(call.as_slice()) {
            drop(table);
            self.alone.fetch_add(1, Ordering::Relaxed);
            return Turn::Alone(Alone {
                solo: Some((self.clone(), call)),
            });
        }
        let length = call.len();
        let mut key = call;
        key.extend_from_slice(&revision.to_be_bytes());
        if let Some(receiver) = table.running.get(key.as_slice()) {
            let receiver = receiver.clone();
            drop(table);
            self.joined.fetch_add(1, Ordering::Relaxed);
            return Turn::Join(Waiter { receiver });
        }
        let cost = key.len() + OVERHEAD;
        // Solo marks are hints: flights take their room first.
        while table.bytes + cost > budget && table.evict_mark() {}
        if table.bytes + cost > budget {
            drop(table);
            self.alone.fetch_add(1, Ordering::Relaxed);
            return Turn::Alone(Alone { solo: None });
        }
        let (sender, receiver) = watch::channel(None);
        let key: Arc<[u8]> = key.into();
        table.bytes += cost;
        table.running.insert(key.clone(), receiver);
        drop(table);
        self.led.fetch_add(1, Ordering::Relaxed);
        Turn::Lead(Leader {
            flights: self.clone(),
            key,
            call: length,
            sender: Some(sender),
        })
    }

    /// Counters and occupancy.
    pub fn stats(&self) -> FlightStats {
        let table = self.lock();
        FlightStats {
            led: self.led.load(Ordering::Relaxed),
            joined: self.joined.load(Ordering::Relaxed),
            alone: self.alone.load(Ordering::Relaxed),
            flights: table.running.len(),
            solo: table.solo.len(),
            bytes: table.bytes,
            budget: self.budget(),
        }
    }
}

/// An override, told apart from its absence.
fn prefixed(value: u64) -> [u8; 9] {
    let mut bytes = [1; 9];
    bytes[1..].copy_from_slice(&value.to_be_bytes());
    bytes
}

#[cfg(test)]
mod tests {
    use super::*;

    fn call(name: &str) -> Vec<u8> {
        Flights::call_key(b"db", "app", name, &Value::Null, &CallOptions::default()).unwrap()
    }

    fn lead(turn: Turn) -> Leader {
        match turn {
            Turn::Lead(leader) => leader,
            other => panic!("expected to lead, got {other:?}"),
        }
    }

    fn join(turn: Turn) -> Waiter {
        match turn {
            Turn::Join(waiter) => waiter,
            other => panic!("expected to join, got {other:?}"),
        }
    }

    fn alone(turn: Turn) -> Alone {
        match turn {
            Turn::Alone(alone) => alone,
            other => panic!("expected to be alone, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn identical_calls_at_one_revision_join_and_land_together() {
        let flights = Arc::new(Flights::default());
        let leader = lead(flights.enter(call("get"), 7));
        let first = join(flights.enter(call("get"), 7));
        let second = join(flights.enter(call("get"), 7));
        // Another call, or the same at another revision, flies alone.
        let other = lead(flights.enter(call("other"), 7));
        let newer = lead(flights.enter(call("get"), 8));
        leader.land(true);
        assert_eq!(first.landing().await, Landing::Kept);
        assert_eq!(second.landing().await, Landing::Kept);
        // Landed, the flight is gone: the next call leads again.
        let again = lead(flights.enter(call("get"), 7));
        drop((other, newer, again));
        let stats = flights.stats();
        assert_eq!((stats.led, stats.joined, stats.alone), (4, 2, 0));
        assert_eq!((stats.flights, stats.solo, stats.bytes), (0, 0, 0));
    }

    #[tokio::test]
    async fn unkept_results_mark_the_call_solo_until_one_keeps() {
        let flights = Arc::new(Flights::default());
        let leader = lead(flights.enter(call("now"), 1));
        let waiter = join(flights.enter(call("now"), 1));
        leader.land(false);
        assert_eq!(waiter.landing().await, Landing::Unkept);
        // Solo: identical calls evaluate side by side, at any revision.
        let one = alone(flights.enter(call("now"), 1));
        let two = alone(flights.enter(call("now"), 2));
        assert_eq!(flights.stats().solo, 1);
        one.land(false);
        assert_eq!(flights.stats().solo, 1);
        two.land(true);
        assert_eq!(flights.stats().solo, 0);
        lead(flights.enter(call("now"), 2)).land(true);
        assert_eq!(flights.stats().bytes, 0);
    }

    #[tokio::test]
    async fn a_leader_that_goes_away_releases_its_waiters() {
        let flights = Arc::new(Flights::default());
        let leader = lead(flights.enter(call("get"), 1));
        let waiter = join(flights.enter(call("get"), 1));
        let pending = tokio::spawn(waiter.landing());
        tokio::task::yield_now().await;
        drop(leader);
        assert_eq!(pending.await.unwrap(), Landing::Abandoned);
        // Not marked solo: the next call leads.
        lead(flights.enter(call("get"), 1)).land(true);
        assert_eq!(flights.stats().solo, 0);
    }

    #[test]
    fn the_table_is_bounded_and_marks_leave_first() {
        let flights = Arc::new(Flights::default());
        let key = call("get");
        let cost = key.len() + 8 + OVERHEAD;
        flights.set_budget(2 * cost);
        let first = lead(flights.enter(call("get"), 1));
        let second = lead(flights.enter(call("get"), 2));
        // Full: a third flight would pass the budget.
        alone(flights.enter(call("get"), 3)).land(true);
        first.land(false);
        // The solo mark takes the room the first flight left; a new
        // flight then evicts it.
        assert_eq!(flights.stats().solo, 1);
        lead(flights.enter(call("put"), 4)).land(true);
        assert_eq!(flights.stats().solo, 0);
        second.land(true);
        assert_eq!(flights.stats().bytes, 0);
        flights.set_budget(0);
        alone(flights.enter(call("get"), 1));
    }

    #[test]
    fn the_table_keeps_one_copy_of_each_key_and_charges_it_once() {
        let flights = Arc::new(Flights::default());
        let leader = lead(flights.enter(call("now"), 1));
        {
            let table = flights.lock();
            let (key, _) = table.running.iter().next().unwrap();
            // The leader holds the table's own key.
            assert!(Arc::ptr_eq(key, &leader.key));
            assert_eq!(table.bytes, key.len() + OVERHEAD);
        }
        leader.land(false);
        let table = flights.lock();
        let marked = table.solo_order.front().unwrap();
        let held = table.solo.get(marked.as_ref()).unwrap();
        // The mark and its place in the eviction order are one key.
        assert!(Arc::ptr_eq(marked, held));
        assert_eq!(Arc::strong_count(marked), 2);
        assert_eq!(table.bytes, marked.len() + OVERHEAD);
    }

    #[test]
    fn keys_tell_presented_inputs_apart() {
        let base = CallOptions::default();
        let key = |args: &Value, options: &CallOptions| {
            Flights::call_key(b"db", "app", "get", args, options).unwrap()
        };
        let plain = key(&Value::Null, &base);
        let principal = CallOptions {
            principal: Some(flower_sdk::Principal::new("alice".to_string(), None, None).unwrap()),
            ..CallOptions::default()
        };
        let credentials = CallOptions {
            credentials: Some(Value::String("token".into())),
            ..CallOptions::default()
        };
        let external = CallOptions {
            external: true,
            ..CallOptions::default()
        };
        let at = CallOptions {
            timestamp_ms: Some(0),
            ..CallOptions::default()
        };
        let keys = [
            plain.clone(),
            key(&Value::Bool(true), &base),
            key(&Value::Null, &principal),
            key(&Value::Null, &credentials),
            key(&Value::Null, &external),
            key(&Value::Null, &at),
        ];
        for (i, a) in keys.iter().enumerate() {
            for b in &keys[i + 1..] {
                assert_ne!(a, b);
            }
        }
        assert_eq!(plain, key(&Value::Null, &base));
    }
}
