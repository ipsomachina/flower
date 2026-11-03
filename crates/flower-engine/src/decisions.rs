//! Authorization decisions reused across calls while they hold.
//!
//! A module's authorization hook decides who a call runs as from the
//! call's database, module, method name (an HTTP alias or not), the
//! credentials and asserted principal it presents, its
//! arguments, the state the hook reads, the clock and the managed-key
//! catalog, and the code deployed. A decision keyed by all of the call's
//! inputs (a SHA-256 digest of them, so entries hold no credentials) is
//! kept beside the certificate of what the hook read, the version of the
//! database's code and where the evaluation sits in the change journal,
//! and answers a later call with the same inputs only while:
//!
//! - the same code version is deployed (a redeploy, a changed policy or
//!   alias table, is a new version);
//! - no change the journal recorded since touches what the hook read (a
//!   revoked session row, a removed membership);
//! - the clock has not reached the decision's expiry (a token's `exp`,
//!   which the hook reports or declares with `changes_at`); a hook that
//!   polled the clock is never kept;
//! - when the hook resolved a managed key, no key-catalog change began
//!   since its snapshot (a rotated or revoked signing key);
//! - and the call reads a committed snapshot no older than the decision's.
//!
//! A hook that read something no dependency names is never kept, and
//! neither are denials: a refused caller runs the hook every time. A hook
//! reports whether it read the call's arguments; one that did not (the
//! SDK's typed and gateway authenticators with public or authenticated
//! methods) decides once per credential and method, whatever the
//! arguments; the others once per arguments as well, while those render
//! within [`WHOLE_ARGS_MAX_BYTES`].
//!
//! Decisions answer on committed snapshots only. A writer preparing on
//! its staged state, or speculatively on an overlay, runs the hook, so a
//! write staged before it (a revocation in the same batch) counts and a
//! speculation's read set holds what the decision read.
//!
//! The memo is bounded in bytes and evicts in insertion order; lookups
//! clone the entry out and validate outside the lock.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use flower_sdk::{FlowerType, Value};
use sha2::{Digest, Sha256};

use crate::auth::Authorized;
use crate::certificate::{Anchor, Certificate, ChangeJournal, KeySequence, Validity};
use crate::json::render;
use crate::query_cache::CodeVersion;
use crate::storage::keys::push_sized;

/// Default byte budget.
pub const DEFAULT_DECISION_MEMO_BYTES: usize = 4 * 1024 * 1024;
/// Arguments rendering longer are not worth a digest on every call: a
/// decision that read them is not kept, and the hook runs as before.
pub const WHOLE_ARGS_MAX_BYTES: usize = 16 * 1024;
/// Fixed per-entry overhead charged on top of the decision and its
/// certificate.
const ENTRY_OVERHEAD: usize = 256;
/// Most call keys remembered as having argument-reading decisions before
/// the set starts over (forgetting costs one evaluation each).
const SHAPES: usize = 4096;

/// What a decision is for: a digest of the call's inputs.
pub(crate) type Key = [u8; 32];

/// The call's inputs but its arguments: database, module, method name,
/// whether it is an alias, credentials and asserted principal.
/// `None` when an input does not render, which leaves the call unkept.
pub(crate) fn call_key(
    db: &[u8],
    module: &str,
    name: &str,
    external: bool,
    credentials: Option<&Value>,
    principal: Option<&flower_sdk::Principal>,
) -> Option<Key> {
    let mut framed = Vec::new();
    push_sized(&mut framed, db).ok()?;
    push_sized(&mut framed, module.as_bytes()).ok()?;
    push_sized(&mut framed, name.as_bytes()).ok()?;
    framed.push(u8::from(external));
    match credentials {
        None => framed.push(0),
        Some(credentials) => {
            framed.push(1);
            push_sized(&mut framed, &render(credentials).ok()?).ok()?;
        }
    }
    match principal {
        None => framed.push(0),
        Some(principal) => {
            framed.push(1);
            push_sized(&mut framed, &render(&principal.encode().ok()?).ok()?).ok()?;
        }
    }
    Some(Sha256::digest(&framed).into())
}

/// The call with its whole arguments, for decisions that read them;
/// `None` past [`WHOLE_ARGS_MAX_BYTES`].
fn args_key(call: &Key, args: &Value) -> Option<Key> {
    let rendered = render(args).ok()?;
    if rendered.len() > WHOLE_ARGS_MAX_BYTES {
        return None;
    }
    let mut digest = Sha256::new();
    digest.update(b"args\0");
    digest.update(call);
    digest.update(&rendered);
    Some(digest.finalize().into())
}

/// Where a lookup or an evaluation stands: the snapshot's revision of
/// the database, the code it captured, the journal and key marks taken
/// before it, and the call's clock.
pub(crate) struct Standing<'a> {
    pub journal: &'a ChangeJournal,
    pub keys: &'a KeySequence,
    pub db: &'a [u8],
    pub code: &'a CodeVersion,
    pub revision: u64,
    pub journal_mark: u64,
    pub keys_mark: u64,
    pub timestamp_ms: u64,
}

/// One kept decision and what keeps it valid.
#[derive(Debug)]
struct Decision {
    authorized: Authorized,
    certificate: Arc<Certificate>,
    code: CodeVersion,
    chain: u64,
    /// The newest revision it is known valid at.
    revision: AtomicU64,
    evaluated_ms: u64,
    /// The key mark it was made at, when it resolved a managed key.
    keys: Option<u64>,
    bytes: usize,
}

impl Decision {
    /// Whether it still decides a call standing at `at`; moves its
    /// revision forward when it does.
    fn holds(&self, at: &Standing<'_>) -> bool {
        if self.code != *at.code
            || !self
                .certificate
                .validity()
                .holds(self.evaluated_ms, at.timestamp_ms)
        {
            return false;
        }
        if let Some(mark) = self.keys
            && (at.keys_mark != mark || !at.keys.settled(mark))
        {
            return false;
        }
        let anchor = Anchor {
            chain: self.chain,
            revision: self.revision.load(Ordering::Acquire),
        };
        if at.revision < anchor.revision
            || !at
                .journal
                .unchanged(at.db, anchor, at.revision, &self.certificate)
        {
            return false;
        }
        self.revision.fetch_max(at.revision, Ordering::AcqRel);
        true
    }
}

/// Memo counters since the engine opened.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DecisionMemoStats {
    /// Calls a kept decision answered.
    pub hits: u64,
    /// Calls whose hook ran.
    pub evaluations: u64,
    /// Decisions held.
    pub entries: usize,
    /// Bytes charged.
    pub bytes: usize,
    /// The bytes it may hold; zero when disabled.
    pub budget: usize,
}

#[derive(Debug, Default)]
struct Entries {
    map: HashMap<Key, Arc<Decision>>,
    /// Keys in first-insertion order, for eviction.
    order: VecDeque<Key>,
    bytes: usize,
    /// Call keys whose kept decisions read the arguments.
    shapes: HashSet<Key>,
}

/// Recent authorization decisions, bounded in bytes.
#[derive(Debug)]
pub struct DecisionMemo {
    entries: Mutex<Entries>,
    budget: AtomicUsize,
    hits: AtomicU64,
    evaluations: AtomicU64,
}

impl Default for DecisionMemo {
    fn default() -> Self {
        DecisionMemo {
            entries: Mutex::default(),
            budget: AtomicUsize::new(DEFAULT_DECISION_MEMO_BYTES),
            hits: AtomicU64::new(0),
            evaluations: AtomicU64::new(0),
        }
    }
}

impl DecisionMemo {
    fn lock(&self) -> std::sync::MutexGuard<'_, Entries> {
        self.entries.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Replace the byte budget; zero disables the memo. Entries over a
    /// smaller budget leave on the next insert.
    pub fn set_budget(&self, bytes: usize) {
        self.budget.store(bytes, Ordering::Release);
        if bytes == 0 {
            *self.lock() = Entries::default();
        }
    }

    /// Whether it is on.
    pub(crate) fn enabled(&self) -> bool {
        self.budget.load(Ordering::Acquire) > 0
    }

    /// The decision kept for `call` with `args` that still holds at
    /// `at`, with the certificate of what it read.
    pub(crate) fn recall(
        &self,
        call: &Key,
        args: &Value,
        at: &Standing<'_>,
    ) -> Option<(Authorized, Arc<Certificate>)> {
        let (any_args, read_args) = {
            let entries = self.lock();
            (
                entries.map.get(call).cloned(),
                entries.shapes.contains(call),
            )
        };
        let with_args = if read_args {
            args_key(call, args).and_then(|key| self.lock().map.get(&key).cloned())
        } else {
            None
        };
        let decision = [any_args, with_args]
            .into_iter()
            .flatten()
            .find(|decision| decision.holds(at))?;
        self.hits.fetch_add(1, Ordering::Relaxed);
        Some((decision.authorized.clone(), decision.certificate.clone()))
    }

    /// Count a hook evaluation.
    pub(crate) fn evaluated(&self) {
        self.evaluations.fetch_add(1, Ordering::Relaxed);
    }

    /// Keep a decision made at `at` for `call` (with `args` when it read
    /// them), when it can be rechecked: nothing untracked read, the clock
    /// not polled, the journal able to follow its snapshot and no key
    /// change since one it resolved. A decision made on an older snapshot
    /// never replaces a newer one on the same chain.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn keep(
        &self,
        call: Key,
        args: &Value,
        reads_args: bool,
        authorized: &Authorized,
        certificate: Arc<Certificate>,
        at: &Standing<'_>,
    ) {
        let budget = self.budget.load(Ordering::Acquire);
        if budget == 0 || certificate.is_broad() || certificate.validity() == Validity::Polled {
            return;
        }
        let keys = if certificate.reads_keys() {
            if !at.keys.settled(at.keys_mark) {
                return;
            }
            Some(at.keys_mark)
        } else {
            None
        };
        let Some(anchor) = at.journal.anchor(at.db, at.journal_mark, at.revision) else {
            return;
        };
        let key = if reads_args {
            match args_key(&call, args) {
                Some(key) => key,
                None => return,
            }
        } else {
            call
        };
        let principal = authorized
            .principal()
            .and_then(|principal| principal.encode().ok())
            .and_then(|encoded| render(&encoded).ok())
            .map_or(0, |rendered| rendered.len());
        let bytes = 2 * key.len()
            + principal
            + authorized.definition().len()
            + certificate.allocation_cost()
            + ENTRY_OVERHEAD;
        if bytes > budget {
            return;
        }
        let mut entries = self.lock();
        if let Some(existing) = entries.map.get(&key)
            && existing.chain == anchor.chain
            && existing.revision.load(Ordering::Acquire) > anchor.revision
        {
            return;
        }
        if reads_args {
            if entries.shapes.len() >= SHAPES.max(2 * entries.map.len()) {
                entries.shapes.clear();
            }
            entries.shapes.insert(call);
        }
        // A replaced entry keeps its place in the eviction order.
        match entries.map.remove(&key) {
            Some(previous) => entries.bytes -= previous.bytes,
            None => entries.order.push_back(key),
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
            Arc::new(Decision {
                authorized: authorized.clone(),
                certificate,
                code: at.code.clone(),
                chain: anchor.chain,
                revision: AtomicU64::new(anchor.revision),
                evaluated_ms: at.timestamp_ms,
                keys,
                bytes,
            }),
        );
    }

    /// Counters and occupancy.
    pub fn stats(&self) -> DecisionMemoStats {
        let entries = self.lock();
        DecisionMemoStats {
            hits: self.hits.load(Ordering::Relaxed),
            evaluations: self.evaluations.load(Ordering::Relaxed),
            entries: entries.map.len(),
            bytes: entries.bytes,
            budget: self.budget.load(Ordering::Acquire),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn principal(subject: &str) -> flower_sdk::Principal {
        flower_sdk::Principal::new(subject.to_string(), None, None).unwrap()
    }

    #[test]
    fn call_keys_cover_every_input_but_the_arguments() {
        let token = Value::String("t".into());
        let base = call_key(b"db", "app", "m", false, Some(&token), None).unwrap();
        let other = Value::String("u".into());
        for different in [
            call_key(b"db2", "app", "m", false, Some(&token), None),
            call_key(b"db", "app2", "m", false, Some(&token), None),
            call_key(b"db", "app", "n", false, Some(&token), None),
            call_key(b"db", "app", "m", true, Some(&token), None),
            call_key(b"db", "app", "m", false, Some(&other), None),
            call_key(b"db", "app", "m", false, None, None),
            call_key(
                b"db",
                "app",
                "m",
                false,
                Some(&token),
                Some(&principal("a")),
            ),
        ] {
            assert_ne!(different.unwrap(), base);
        }
        // Framing keeps neighbours apart.
        assert_ne!(
            call_key(b"d", "bapp", "m", false, None, None),
            call_key(b"db", "app", "m", false, None, None)
        );
        // Claims are part of an asserted principal.
        let claimed =
            flower_sdk::Principal::new("a".into(), None, Some(Value::Bool(true))).unwrap();
        assert_ne!(
            call_key(b"db", "app", "m", false, None, Some(&claimed)),
            call_key(b"db", "app", "m", false, None, Some(&principal("a")))
        );
    }

    #[test]
    fn argument_keys_are_bounded() {
        let call = [7u8; 32];
        // The JSON string's quotes count too.
        let longest = Value::String("x".repeat(WHOLE_ARGS_MAX_BYTES - 2));
        let longer = Value::String("x".repeat(WHOLE_ARGS_MAX_BYTES - 1));
        assert!(args_key(&call, &longest).is_some());
        assert!(args_key(&call, &longer).is_none());
        assert_ne!(
            args_key(&call, &Value::Null),
            args_key(&call, &Value::Object(Default::default()))
        );
    }
}
