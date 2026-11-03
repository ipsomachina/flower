//! Query results reused across revisions while their certificates hold.
//!
//! A query's result depends on its snapshot, the code deployed with it,
//! who it runs as, its arguments and, for some, the clock.
//! The cache keys a result by database, module, definition, canonical
//! arguments and the whole principal the call runs as, and keeps beside it the certificate of what the evaluation
//! observed (see [`crate::certificate`]), the version of the database's
//! code and where the evaluation sits in the change journal. A later
//! call reuses it only after authorizing as usual (lookups key on the
//! decided principal) and only when the code is the same version, the
//! clock has not passed the result's declared validity, and no change
//! the journal recorded since the evaluation touches the certificate.
//! Results that polled the clock, or read something no dependency names,
//! never enter; neither do failures. Each hit moves the entry's revision
//! forward, so later checks scan fewer changes.
//!
//! An entry keeps its result twice: as a value, for callers that use it,
//! and as the JSON the HTTP API writes for it ([`Encoded`]), which callers
//! that only send it on (the HTTP query route) take as it is, without
//! cloning or rendering the value again. Both count against the budget.
//!
//! The cache is bounded in bytes and evicts in insertion order. Lookups
//! clone the entry out of the table and validate outside its lock.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError, Weak};

use flower_sdk::{Failure, FlowerType, Value, fail};

use crate::auth::Authorized;
use crate::certificate::{Anchor, Certificate, ChangeJournal, ChangeSet, Graph, Validity};

thread_local! {
    /// How this thread's last cache lookup went.
    static LOOKUP: std::cell::Cell<Option<Lookup>> = const { std::cell::Cell::new(None) };
}

/// How one query's cache lookup went.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Lookup {
    /// The cache answered.
    Hit,
    /// The query evaluated; `kept` when its result entered the cache,
    /// so the same call can answer from it next.
    Miss {
        /// Whether the result entered the cache.
        kept: bool,
    },
}

impl Lookup {
    /// Whether the cache answered.
    pub fn hit(self) -> bool {
        self == Lookup::Hit
    }

    /// Whether the cache holds the answer now: it answered, or the
    /// evaluation's result entered it.
    pub fn reusable(self) -> bool {
        matches!(self, Lookup::Hit | Lookup::Miss { kept: true })
    }
}

/// How the last query cache lookup on this thread went since the last
/// call; `None` when no lookup ran (the cache was off for it). A caller
/// attributing a query's lookup takes it before and after.
pub fn take_lookup() -> Option<Lookup> {
    LOOKUP.take()
}
use crate::json::render;
use crate::storage::keys::push_sized;

/// The failure code of a result that does not render as JSON.
pub const RENDER_FAILED: &str = "render";

/// Default byte budget.
pub const DEFAULT_QUERY_CACHE_BYTES: usize = 16 * 1024 * 1024;
/// Fixed per-entry overhead charged on top of key, value and
/// certificate bytes.
const ENTRY_OVERHEAD: usize = 256;

/// One version of a database's deployed code: equal only to itself.
/// Holds no code alive.
#[derive(Clone, Debug, Default)]
pub struct CodeVersion(Option<Weak<dyn std::any::Any + Send + Sync>>);

impl CodeVersion {
    /// The version an `Arc` of deployed code stands for.
    pub(crate) fn of<T: std::any::Any + Send + Sync>(code: Option<&Arc<T>>) -> Self {
        CodeVersion(code.map(|code| {
            let code: Arc<dyn std::any::Any + Send + Sync> = code.clone();
            Arc::downgrade(&code)
        }))
    }
}

impl PartialEq for CodeVersion {
    fn eq(&self, other: &Self) -> bool {
        match (&self.0, &other.0) {
            (None, None) => true,
            (Some(a), Some(b)) => Weak::ptr_eq(a, b),
            _ => false,
        }
    }
}

/// A query result as the HTTP API writes values: the compact JSON
/// `serde_json` prints for the value its canonical rendering reads as,
/// as every answer of the API prints them (`1e+21`, `1e-7`), so a query
/// answers a value with the same bytes a mutation does. Shared with the
/// cache entry that holds it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Encoded(Arc<[u8]>);

impl Encoded {
    /// Render `value`; a value that does not render fails with
    /// [`RENDER_FAILED`], as the HTTP API answers one.
    pub fn render(value: &Value) -> Result<Self, Failure> {
        let failed = |message: String| fail(RENDER_FAILED, message);
        let canonical = render(value).map_err(|error| failed(Failure::from(error).message))?;
        let json: serde_json::Value =
            serde_json::from_slice(&canonical).map_err(|error| failed(error.to_string()))?;
        serde_json::to_vec(&json)
            .map(|bytes| Encoded(bytes.into()))
            .map_err(|error| failed(error.to_string()))
    }

    /// The JSON bytes.
    pub fn bytes(&self) -> &[u8] {
        &self.0
    }

    /// Whether both are one rendering: one came from the cache entry
    /// the other did, and nothing rendered again.
    pub fn shares(&self, other: &Encoded) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

/// A cached result and what keeps it valid.
#[derive(Debug)]
pub(crate) struct Cached {
    /// The result.
    pub value: Arc<Value>,
    /// The result as the HTTP API writes it.
    pub encoded: Encoded,
    /// What its evaluation observed.
    pub certificate: Arc<Certificate>,
    /// The code it ran.
    pub code: CodeVersion,
    /// The journal chain it is anchored to.
    chain: u64,
    /// The newest revision it is known valid at.
    revision: AtomicU64,
    /// The clock its evaluation read, for declared validity.
    pub evaluated_ms: u64,
    /// The derived graph it read through, when it read one (see
    /// [`Graph`]).
    pub graph: Option<Arc<Graph>>,
    /// Charged bytes.
    bytes: usize,
}

/// Whether a cached result answers a call.
#[derive(Debug)]
pub(crate) enum Check {
    /// It does.
    Valid,
    /// Only these changes since its evaluation stand in the way: they
    /// touch its certificate, but its code and clock still hold.
    Touched(Vec<Arc<ChangeSet>>),
    /// It does not.
    Invalid,
}

impl Cached {
    /// Its anchor in the journal.
    pub fn anchor(&self) -> Anchor {
        Anchor {
            chain: self.chain,
            revision: self.revision.load(Ordering::Acquire),
        }
    }

    /// Whether it still answers a call at `timestamp_ms` on a snapshot
    /// of `db` at `revision` running `code`. Moves its revision forward
    /// when it does.
    pub fn check(
        &self,
        journal: &ChangeJournal,
        db: &[u8],
        code: &CodeVersion,
        revision: u64,
        timestamp_ms: u64,
    ) -> Check {
        if self.code != *code
            || !self
                .certificate
                .validity()
                .holds(self.evaluated_ms, timestamp_ms)
        {
            return Check::Invalid;
        }
        let anchor = self.anchor();
        if revision < anchor.revision {
            return Check::Invalid;
        }
        let Some(changes) = journal.since(db, anchor, revision) else {
            return Check::Invalid;
        };
        if changes
            .iter()
            .any(|changes| self.certificate.touched_by(changes))
        {
            return Check::Touched(changes);
        }
        self.revision.fetch_max(revision, Ordering::AcqRel);
        Check::Valid
    }
}

/// Cache counters since the engine opened.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct QueryCacheStats {
    /// Calls answered from the cache.
    pub hits: u64,
    /// Calls evaluated.
    pub misses: u64,
    /// Hits a change had touched through derived values only, answered
    /// once the values it touched came out as they were (see [`Graph`]).
    pub rederived: u64,
    /// Entries held.
    pub entries: usize,
    /// Bytes charged.
    pub bytes: usize,
    /// The bytes it may hold; zero when disabled.
    pub budget: usize,
}

/// A result to keep and what keeps it valid.
#[derive(Debug)]
pub(crate) struct Entry {
    /// The result.
    pub value: Arc<Value>,
    /// It as the HTTP API writes it.
    pub encoded: Encoded,
    /// What its evaluation observed.
    pub certificate: Arc<Certificate>,
    /// The code it ran.
    pub code: CodeVersion,
    /// The clock its evaluation read.
    pub evaluated_ms: u64,
    /// The derived graph it read through, if any.
    pub graph: Option<Arc<Graph>>,
}

#[derive(Debug, Default)]
struct Entries {
    map: HashMap<Vec<u8>, Arc<Cached>>,
    /// Keys in first-insertion order, for eviction.
    order: VecDeque<Vec<u8>>,
    bytes: usize,
}

/// Recent query results, bounded in bytes.
#[derive(Debug)]
pub struct QueryCache {
    entries: Mutex<Entries>,
    budget: AtomicUsize,
    hits: AtomicU64,
    misses: AtomicU64,
    rederived: AtomicU64,
}

impl Default for QueryCache {
    fn default() -> Self {
        QueryCache {
            entries: Mutex::default(),
            budget: AtomicUsize::new(DEFAULT_QUERY_CACHE_BYTES),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            rederived: AtomicU64::new(0),
        }
    }
}

impl QueryCache {
    fn lock(&self) -> std::sync::MutexGuard<'_, Entries> {
        self.entries.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The key of a call: database, module, definition, canonical
    /// arguments and the decided principal. `None` when an input does
    /// not render, which leaves the call uncached.
    pub fn key(db: &[u8], authorized: &Authorized, args: &Value) -> Option<Vec<u8>> {
        let mut key = Vec::new();
        push_sized(&mut key, db).ok()?;
        push_sized(&mut key, authorized.module().as_bytes()).ok()?;
        push_sized(&mut key, authorized.definition().as_bytes()).ok()?;
        push_sized(&mut key, &render(args).ok()?).ok()?;
        match authorized.principal() {
            None => key.push(0),
            Some(principal) => {
                key.push(1);
                push_sized(&mut key, &render(&principal.encode().ok()?).ok()?).ok()?;
            }
        }
        Some(key)
    }

    /// Replace the byte budget; zero disables the cache. Entries over
    /// a smaller budget leave on the next insert.
    pub fn set_budget(&self, bytes: usize) {
        self.budget.store(bytes, Ordering::Release);
        if bytes == 0 {
            let mut entries = self.lock();
            *entries = Entries::default();
        }
    }

    /// The entry for `key`, unvalidated.
    pub(crate) fn get(&self, key: &[u8]) -> Option<Arc<Cached>> {
        self.lock().map.get(key).cloned()
    }

    /// Count a call answered from the cache.
    pub(crate) fn hit(&self) {
        self.hits.fetch_add(1, Ordering::Relaxed);
        LOOKUP.set(Some(Lookup::Hit));
    }

    /// Count a hit that re-derived what a change touched first.
    pub(crate) fn rederived(&self) {
        self.rederived.fetch_add(1, Ordering::Relaxed);
    }

    /// Count a call evaluated.
    pub(crate) fn miss(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
        LOOKUP.set(Some(Lookup::Miss { kept: false }));
    }

    /// The call this thread just counted a miss for kept its result.
    pub(crate) fn kept(&self) {
        LOOKUP.set(Some(Lookup::Miss { kept: true }));
    }

    /// Whether a certificate's result may enter: not broad, not polled, and
    /// no managed key resolved (catalog changes move no revision the cache
    /// could check).
    pub fn admits(certificate: &Certificate) -> bool {
        !certificate.is_broad()
            && !certificate.reads_keys()
            && certificate.validity() != Validity::Polled
    }

    /// Keep a result evaluated at `anchor`, with its rendering. A late
    /// evaluation never replaces a newer one on the same chain. Whether
    /// the cache now holds it.
    pub(crate) fn insert(&self, key: Vec<u8>, entry: Entry, anchor: Anchor) -> bool {
        let Entry {
            value,
            encoded,
            certificate,
            code,
            evaluated_ms,
            graph,
        } = entry;
        let budget = self.budget.load(Ordering::Acquire);
        // The value counts at its rendering's length, and the rendering
        // it keeps besides at its own.
        let bytes = 2 * key.len()
            + 2 * encoded.bytes().len()
            + certificate.allocation_cost()
            + graph.as_ref().map_or(0, |graph| graph.allocation_cost())
            + ENTRY_OVERHEAD;
        if bytes > budget {
            return false;
        }
        let mut entries = self.lock();
        if let Some(existing) = entries.map.get(&key) {
            let at = existing.anchor();
            if at.chain == anchor.chain && at.revision > anchor.revision {
                return false;
            }
        }
        // A replaced entry keeps its place in the eviction order.
        match entries.map.remove(&key) {
            Some(previous) => entries.bytes -= previous.bytes,
            None => entries.order.push_back(key.clone()),
        }
        while entries.bytes + bytes > budget {
            let Some(oldest) = entries.order.pop_front() else {
                break;
            };
            if oldest == key {
                entries.order.push_back(oldest);
                continue;
            }
            if let Some(evicted) = entries.map.remove(&oldest) {
                entries.bytes -= evicted.bytes;
            }
        }
        entries.bytes += bytes;
        entries.map.insert(
            key,
            Arc::new(Cached {
                value,
                encoded,
                certificate,
                code,
                chain: anchor.chain,
                revision: AtomicU64::new(anchor.revision),
                evaluated_ms,
                graph,
                bytes,
            }),
        );
        true
    }

    /// Counters and occupancy.
    pub fn stats(&self) -> QueryCacheStats {
        let entries = self.lock();
        QueryCacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            rederived: self.rederived.load(Ordering::Relaxed),
            entries: entries.map.len(),
            bytes: entries.bytes,
            budget: self.budget.load(Ordering::Acquire),
        }
    }
}
