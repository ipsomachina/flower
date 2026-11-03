//! Durable work queues: enqueued payloads claimed under fenced leases,
//! by native workers over HTTP and by guest mutations alike, with v1's
//! scopes, priorities, group turns, named jobs, kept results, waiter
//! lines and history-bound leases.
//!
//! Items live as records under the `$queue` collection. Every key starts
//! with a tag and the queue name zero-padded to [`MAX_QUEUE_NAME`]
//! bytes; most continue with the 16-byte hash of the scope, so a scope
//! is one contiguous key range whatever its text. Each kind of row has
//! one key length, and record keys are length-prefixed, so one kind of
//! one queue (or scope) is exactly one byte-ordered range:
//!
//! - ready (`p`): pending items due now, keyed by priority
//!   (sign-flipped big-endian), turn, due time and ID: a claim takes the
//!   first rows, lowest priority first, then each group's turn, then the
//!   longest waiting;
//! - waiting (`w`): pending items not yet due, by due time; claimed
//!   (`c`), by lease expiry; dead (`d`), by their last due time;
//!   completed (`k`), kept with their results, by completion time;
//! - ID rows (`i`, queue, ID) point at an item's state row, so lookups
//!   by ID are two point reads; a named job's ID is the hash of its
//!   scope and name, so names are unique per scope;
//! - counters: each scope's last fencing token (`f`), each priority's
//!   last claimed turn (`t`) and each group's next turn (`g`), as v1's
//!   `$flower.fencing` and `$flower.turns`;
//! - the waiter line (`l`, by when each owner started waiting, with an
//!   `o` row per owner pointing at its place).
//!
//! A claim first moves into the ready rows what became claimable since:
//! waiting items now due and claims whose lease lapsed (or, on their
//! last attempt, kills them, as v1 fails them), at most [`PROMOTE`] of
//! each per claim, then takes the first ready rows. A lapse on the last
//! attempt kills the item at the lease's end whether or not a claim
//! passes to move its row: reads, counts, retries, replaces and its
//! holder's reports all see it dead from then on ([`effective`], v1's).
//! Each claim takes a fresh secret claim token and its scope's next
//! fencing token, and is bound to the database's history incarnation:
//! after a restore to an earlier history its holder can no longer renew
//! or report, while a move to another group, whose incarnation the
//! database takes, keeps it good ([`crate::placement::lineage`]). A
//! claim taken before the database has an incarnation binds none, since
//! the first one only names the history it was taken in. Every delay
//! before an item is due again (enqueue, fail, release, retry) is at
//! most [`MAX_DELAY_MS`], whichever path asks.
//!
//! Rows stored before scopes existed have a shorter key (tag, padded
//! queue, time, ID) and live in their own key-length range; they read
//! as the default scope at priority 0 and turn 0 (ahead of newer work at
//! that priority, as v1 claims its older records), and a claim moves
//! them to the current layout like waiting rows.
//!
//! Every state change is planned once, as record writes, by a pure
//! [`QueueView`] call taking the clock, claim tokens and the incarnation
//! as inputs: over committed state for the replicated `plan_*` calls
//! ([`Engine::plan_poll_queue`] and its siblings), inside one write
//! transaction for the direct calls, and over an invocation's staged
//! overlay for guest operations (see `crate::host`), whose writes stage
//! into the mutation and commit with it. Every writer of a database's
//! queue rows runs one at a time on the state the writes before it left
//! (the replicated writer, or the engine's write transaction), so a
//! guest claim and a worker poll never take the same item. Queues are
//! at-least-once by design.
//!
//! Guest code uses only the queues its own module declares
//! ([`QUEUE_UNDECLARED`] otherwise), as v1's queues are components of
//! their module: a queue's work is its declarers', so purging a removed
//! module deletes only queues no other module declares and no other
//! module's work ([`crate::purge`]). Native workers name any queue, but
//! never enqueue. A queue under purge takes no operation until the
//! purge ends ([`crate::purge::QUEUE_PURGING`]), checked as plans begin
//! and again by every applier.
//!
//! [`Engine::plan_poll_queue`]: crate::engine::Engine::plan_poll_queue

use std::collections::BTreeMap;

use flower_sdk::error::codes;
use flower_sdk::schema::Schema;
use flower_sdk::{Failure, Value, fail};
use sha2::{Digest as _, Sha256};

use crate::error::Error;
use crate::patch::{Planned, RecordWrite, apply_records};
use crate::session::Staged;
use crate::storage::keys::{encode_record_key, prefix_end, record_key_class};
use crate::storage::{ReadTxn, RecordRead, RowWrite, WriteTxn, meta};

/// Longest queue name, in bytes. Mirrors the SDK.
pub const MAX_QUEUE_NAME: usize = 128;
/// Longest scope, job name, group or owner, in bytes.
pub const MAX_NAME: usize = 1024;
/// Largest priority magnitude: the safe-integer range, as JSON carries it.
pub const MAX_PRIORITY: i64 = (1 << 53) - 1;
/// Longest enqueue delay: 30 days in milliseconds. Mirrors the SDK.
pub const MAX_DELAY_MS: u64 = 30 * 24 * 60 * 60 * 1000;
/// Most attempts before an item dies. Mirrors the SDK.
pub const MAX_ATTEMPTS: u32 = 100;
/// Longest claim lease: one hour in milliseconds.
pub const MAX_LEASE_MS: u64 = 60 * 60 * 1000;
/// Longest place in the waiter line.
pub const MAX_WAIT_MS: u64 = 60 * 60 * 1000;
/// Most items one poll returns.
pub const MAX_POLL_LIMIT: usize = 100;
/// Most leases one guest renewal names.
pub const MAX_RENEWALS: usize = 1024;
/// How long the first owner in line has new work to itself, by default.
pub const DEFAULT_TURN_MS: u64 = 1_000;
/// Failure code: guest code naming a queue its module does not declare.
pub const QUEUE_UNDECLARED: &str = "queue_undeclared";
/// Waiting items and lapsed claims of each kind one claim moves into
/// the ready rows (v1 promotes 64).
pub const PROMOTE: usize = 256;
/// Rows one maintenance page (stats, purge) reads at a time.
const PAGE: usize = 1024;

/// Ready rows: pending items due now.
const TAG_READY: u8 = b'p';
/// Waiting rows: pending items not yet due.
const TAG_WAITING: u8 = b'w';
/// Claimed rows, by lease expiry.
const TAG_CLAIMED: u8 = b'c';
/// Dead rows, by their last due time.
const TAG_DEAD: u8 = b'd';
/// Completed rows, kept with their results, by completion time.
const TAG_COMPLETED: u8 = b'k';
/// ID rows: the pointer from an ID to its state row.
const TAG_ID: u8 = b'i';
/// Fence rows: the last fencing token of one queue and scope.
const TAG_FENCE: u8 = b'f';
/// Turn clocks: the last claimed turn of one scope and priority.
const TAG_TURN: u8 = b't';
/// Group turns: the next turn of one group of one scope and priority.
const TAG_GROUP: u8 = b'g';
/// Line places, by when each owner started waiting.
const TAG_LINE: u8 = b'l';
/// Line pointers: an owner's place in line.
const TAG_OWNER: u8 = b'o';
/// Where the padded queue ends in every key.
const QUEUE_END: usize = 1 + MAX_QUEUE_NAME;
/// Where the scope hash ends.
const SCOPE_END: usize = QUEUE_END + 16;
/// Ready-row key length: prefix, priority, turn, due time, ID.
const READY_KEY_LEN: usize = SCOPE_END + 8 + 8 + 8 + 16;
/// Waiting, claimed, dead and completed key length: prefix, time, ID.
const TIMED_KEY_LEN: usize = SCOPE_END + 8 + 16;
/// State-row key length before scopes: tag, padded queue, time, ID.
const LEGACY_KEY_LEN: usize = QUEUE_END + 8 + 16;
/// Line-place key length: prefix, since, owner hash.
const LINE_KEY_LEN: usize = SCOPE_END + 8 + 16;

/// An item's lifecycle state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ItemState {
    /// Awaiting its due time or a claim.
    Pending,
    /// Claimed under a lease.
    Claimed,
    /// Exhausted its attempts, or killed.
    Dead,
    /// Completed, kept with its result.
    Completed,
}

impl ItemState {
    /// The state's wire name.
    pub fn name(&self) -> &'static str {
        match self {
            ItemState::Pending => "pending",
            ItemState::Claimed => "claimed",
            ItemState::Dead => "dead",
            ItemState::Completed => "completed",
        }
    }

    fn parse(name: &str) -> Result<Self, Error> {
        match name {
            "pending" => Ok(ItemState::Pending),
            "claimed" => Ok(ItemState::Claimed),
            "dead" => Ok(ItemState::Dead),
            "completed" => Ok(ItemState::Completed),
            _ => Err(Error::corrupt("queue item has a bad state")),
        }
    }
}

/// Where an item's state row is stored.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Class {
    Ready,
    Waiting,
    Claimed,
    Dead,
    Completed,
    LegacyPending,
    LegacyClaimed,
    LegacyDead,
}

/// A stored queue item.
#[derive(Clone, Debug)]
pub struct QueueItem {
    /// Item ID, 32 lowercase hex chars.
    pub id: String,
    /// Guest payload.
    pub payload: Value,
    /// Lifecycle state.
    pub state: ItemState,
    /// Claims so far.
    pub attempts: u32,
    /// Attempts before death.
    pub max_attempts: u32,
    /// Lease expiry in milliseconds, when claimed.
    pub claimed_until_ms: u64,
    /// Claim token, 32 hex chars, when claimed.
    pub token: Option<String>,
    /// Enqueue time in milliseconds.
    pub enqueued_ms: u64,
    /// Due time in milliseconds.
    pub due_ms: u64,
    /// The scope the item belongs to within its queue (`""` by default).
    pub scope: String,
    /// Claim priority: lower values claim first (default 0).
    pub priority: i64,
    /// Its place among its priority's turns: groups take turns.
    pub turn: u64,
    /// The group it takes turns with, if any.
    pub group: Option<String>,
    /// The caller-chosen job name, unique per scope, if any.
    pub name: Option<String>,
    /// The fencing token of the item's latest claim, 0 before any.
    pub fence: u64,
    /// Who holds the current claim, when the claimer named itself.
    pub owner: Option<String>,
    /// The result a completion kept.
    pub result: Option<Value>,
    /// The history incarnation the current claim was taken in.
    pub incarnation: Option<u64>,
    /// When the item last changed state, in milliseconds.
    pub updated_ms: u64,
    /// Where it is stored now; `None` before it is stored.
    pub(crate) class: Option<Class>,
}

impl QueueItem {
    /// When the item became, or becomes, claimable: its due time, or its
    /// lease's end while claimed.
    pub fn available_ms(&self) -> u64 {
        match self.state {
            ItemState::Claimed => self.claimed_until_ms,
            _ => self.due_ms,
        }
    }

    /// The item as guests read it (no claim token).
    pub fn to_value(&self) -> Value {
        let text = |value: &Option<String>| value.clone().map_or(Value::Null, Value::String);
        let mut map = BTreeMap::new();
        map.insert("id".to_string(), Value::String(self.id.clone()));
        map.insert("name".to_string(), text(&self.name));
        map.insert("payload".to_string(), self.payload.clone());
        map.insert(
            "state".to_string(),
            Value::String(self.state.name().to_string()),
        );
        map.insert("scope".to_string(), Value::String(self.scope.clone()));
        map.insert("priority".to_string(), Value::Number(self.priority as f64));
        map.insert("group".to_string(), text(&self.group));
        map.insert("turn".to_string(), Value::Number(self.turn as f64));
        map.insert("attempts".to_string(), Value::Number(self.attempts as f64));
        map.insert(
            "max_attempts".to_string(),
            Value::Number(self.max_attempts as f64),
        );
        map.insert("fence".to_string(), Value::Number(self.fence as f64));
        map.insert("owner".to_string(), text(&self.owner));
        map.insert(
            "result".to_string(),
            self.result.clone().unwrap_or(Value::Null),
        );
        map.insert(
            "enqueued_ms".to_string(),
            Value::Number(self.enqueued_ms as f64),
        );
        map.insert("due_ms".to_string(), Value::Number(self.due_ms as f64));
        map.insert(
            "claimed_until_ms".to_string(),
            Value::Number(self.claimed_until_ms as f64),
        );
        map.insert(
            "updated_ms".to_string(),
            Value::Number(self.updated_ms as f64),
        );
        Value::Object(map)
    }
}

/// Live counts for one queue, or one scope of it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct QueueStats {
    /// Due-or-waiting items.
    pub pending: usize,
    /// Items under claim.
    pub claimed: usize,
    /// Exhausted or killed items awaiting purge.
    pub dead: usize,
    /// Completed items kept with their results.
    pub completed: usize,
}

/// What a claim may take now, and when that next changes, for one
/// queue scope and, optionally, one owner waiting in its line.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Readiness {
    /// Whether a claim (by that owner) would take an item now.
    pub ready: bool,
    /// The next time this may change: a delayed item falls due, a lease
    /// lapses, or the line's first owner's turn ends.
    pub next_ms: Option<u64>,
}

/// A claimed item handed to a worker.
#[derive(Clone, Debug)]
pub struct ClaimedItem {
    /// Item ID.
    pub id: String,
    /// The caller-chosen job name, if any.
    pub name: Option<String>,
    /// Guest payload.
    pub payload: Value,
    /// Claim token completing, renewing or failing the claim.
    pub token: String,
    /// Claims so far, including this one.
    pub attempts: u32,
    /// The claim's fencing token: larger than every token issued before
    /// it in this queue and scope.
    pub fence: u64,
    /// The item's scope.
    pub scope: String,
    /// The item's priority.
    pub priority: i64,
    /// The item's group, if any.
    pub group: Option<String>,
    /// The claimer, when it named itself.
    pub owner: Option<String>,
    /// When the lease lapses unless renewed.
    pub expires_ms: u64,
    /// When the item became claimable: its due time, or the lapsed
    /// lease's end; never before it was enqueued.
    pub available_ms: u64,
    /// When the claim was taken.
    pub claimed_ms: u64,
}

impl ClaimedItem {
    /// The claim as guests read it.
    pub fn to_value(&self) -> Value {
        let text = |value: &Option<String>| value.clone().map_or(Value::Null, Value::String);
        let mut map = BTreeMap::new();
        map.insert("id".to_string(), Value::String(self.id.clone()));
        map.insert("name".to_string(), text(&self.name));
        map.insert("payload".to_string(), self.payload.clone());
        map.insert("token".to_string(), Value::String(self.token.clone()));
        map.insert("attempts".to_string(), Value::Number(self.attempts as f64));
        map.insert("fence".to_string(), Value::Number(self.fence as f64));
        map.insert("scope".to_string(), Value::String(self.scope.clone()));
        map.insert("priority".to_string(), Value::Number(self.priority as f64));
        map.insert("group".to_string(), text(&self.group));
        map.insert("owner".to_string(), text(&self.owner));
        map.insert(
            "expires_ms".to_string(),
            Value::Number(self.expires_ms as f64),
        );
        map.insert(
            "available_ms".to_string(),
            Value::Number(self.available_ms as f64),
        );
        map.insert(
            "claimed_ms".to_string(),
            Value::Number(self.claimed_ms as f64),
        );
        Value::Object(map)
    }
}

/// What modules declare about one queue: the contracts its payloads and
/// results meet, and how long the line's first owner has new work to
/// itself. Every declaration of a queue in a database applies.
#[derive(Clone, Debug, PartialEq)]
pub struct QueueDecl {
    /// The queue's name.
    pub name: String,
    /// Contract every enqueued payload meets.
    pub payload: Schema,
    /// Contract every kept result meets.
    pub result: Schema,
    /// The line's turn, in milliseconds.
    pub turn_ms: u64,
}

/// One queue's declared configuration in a database.
#[derive(Clone, Debug, PartialEq)]
pub struct QueueConfig {
    /// Payload contracts: every one must accept.
    pub payload: Vec<Schema>,
    /// Result contracts: every one must accept.
    pub result: Vec<Schema>,
    /// The line's turn: the shortest declared, [`DEFAULT_TURN_MS`]
    /// without a declaration.
    pub turn_ms: u64,
}

impl Default for QueueConfig {
    fn default() -> Self {
        QueueConfig {
            payload: Vec::new(),
            result: Vec::new(),
            turn_ms: DEFAULT_TURN_MS,
        }
    }
}

impl QueueConfig {
    /// The configuration every declaration of `queue` among `modules`
    /// makes together.
    pub fn of<'a>(
        modules: impl Iterator<Item = &'a crate::descriptors::ModuleDef>,
        queue: &str,
    ) -> Self {
        let mut config = QueueConfig::default();
        let mut turn = None::<u64>;
        for decl in modules.flat_map(|module| module.queues.iter()) {
            if decl.name != queue {
                continue;
            }
            config.payload.push(decl.payload);
            config.result.push(decl.result);
            turn = Some(turn.map_or(decl.turn_ms, |turn| turn.min(decl.turn_ms)));
        }
        if let Some(turn) = turn {
            config.turn_ms = turn;
        }
        config
    }

    /// Check a value against every contract of one kind.
    fn check(schemas: &[Schema], value: &Value, what: &str) -> Result<(), Failure> {
        for schema in schemas {
            if let Err(error) = schema.parse(value) {
                return Err(fail(
                    codes::SCHEMA_MISMATCH,
                    format!("the {what} is invalid at {}: {}", error.path, error.message),
                ));
            }
        }
        Ok(())
    }
}

/// The characters queue names take: never the zero byte, so zero
/// padding keeps one queue's keys apart.
fn name_chars(name: &str) -> bool {
    name.bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
}

/// Validate a queue name.
pub(crate) fn validate_queue(queue: &str) -> Result<(), Failure> {
    if queue.is_empty() || queue.len() > MAX_QUEUE_NAME {
        return Err(fail(codes::INVALID_VALUE, "queue name has a bad length"));
    }
    if !name_chars(queue) {
        return Err(fail(codes::INVALID_VALUE, "queue name has bad characters"));
    }
    Ok(())
}

/// Validate a scope: any text of at most [`MAX_NAME`] bytes, empty for
/// the default scope.
pub(crate) fn validate_scope(scope: &str) -> Result<(), Failure> {
    if scope.len() > MAX_NAME {
        return Err(fail(codes::INVALID_VALUE, "queue scope is too long"));
    }
    Ok(())
}

/// Validate a nonempty name: a job name, group or owner.
pub(crate) fn validate_name(name: &str, what: &str) -> Result<(), Failure> {
    if name.is_empty() || name.len() > MAX_NAME {
        return Err(fail(
            codes::INVALID_VALUE,
            format!("{what} takes 1 to {MAX_NAME} bytes"),
        ));
    }
    Ok(())
}

/// Validate a priority: a safe integer.
pub(crate) fn validate_priority(priority: i64) -> Result<(), Failure> {
    if !(-MAX_PRIORITY..=MAX_PRIORITY).contains(&priority) {
        return Err(fail(codes::INVALID_VALUE, "queue priority out of range"));
    }
    Ok(())
}

/// Validate a delay before an item is due again: at most
/// [`MAX_DELAY_MS`], whichever path asks (enqueue, fail, release,
/// retry; native, HTTP or guest). Due times then stay far inside what a
/// row's numbers (doubles) hold exactly, so the state key an item's due
/// time derives is always the one it is stored under.
pub(crate) fn validate_delay(delay_ms: u64, what: &str) -> Result<(), Failure> {
    if delay_ms > MAX_DELAY_MS {
        return Err(fail(
            codes::INVALID_VALUE,
            format!("{what} delay is too long"),
        ));
    }
    Ok(())
}

/// Validate enqueue parameters on both sides of the ABI.
pub(crate) fn validate_enqueue(
    queue: &str,
    delay_ms: u64,
    max_attempts: u32,
) -> Result<(), Failure> {
    validate_queue(queue)?;
    validate_delay(delay_ms, "enqueue")?;
    if max_attempts == 0 || max_attempts > MAX_ATTEMPTS {
        return Err(fail(codes::INVALID_VALUE, "max attempts out of range"));
    }
    Ok(())
}

/// Validate a claim lease.
pub(crate) fn validate_lease(lease_ms: u64) -> Result<(), Failure> {
    if lease_ms == 0 || lease_ms > MAX_LEASE_MS {
        return Err(fail(codes::INVALID_VALUE, "lease out of range"));
    }
    Ok(())
}

/// Random 128-bit ID and token material.
pub(crate) fn random_128() -> [u8; 16] {
    rand::random()
}

/// A fresh random claim token: 32 lowercase hex characters.
pub fn new_token() -> String {
    crate::hex::encode(&random_128())
}

/// The first 16 bytes of SHA-256 over a domain and length-prefixed parts.
fn hash16(domain: &str, parts: &[&[u8]]) -> [u8; 16] {
    let mut hasher = Sha256::new();
    hasher.update(domain.as_bytes());
    for part in parts {
        hasher.update((part.len() as u64).to_be_bytes());
        hasher.update(part);
    }
    let digest = hasher.finalize();
    let mut out = [0u8; 16];
    out.copy_from_slice(&digest[..16]);
    out
}

/// A scope's key hash.
fn scope_hash(scope: &str) -> [u8; 16] {
    hash16("flower.queue.scope", &[scope.as_bytes()])
}

/// The ID of the job named `name` in `scope`: the same wherever it is
/// computed, so names are unique per scope and lookups by name are
/// point reads.
pub fn named_id(scope: &str, name: &str) -> String {
    crate::hex::encode(&hash16(
        "flower.queue.job",
        &[scope.as_bytes(), name.as_bytes()],
    ))
}

/// A priority as order-preserving unsigned bits.
fn priority_bits(priority: i64) -> u64 {
    (priority as u64) ^ (1 << 63)
}

/// The queue name zero-padded to its fixed width.
fn padded_queue(queue: &str) -> Vec<u8> {
    let mut padded = queue.as_bytes().to_vec();
    padded.resize(MAX_QUEUE_NAME, 0);
    padded
}

/// The queue a `$queue` row of any kind belongs to, from its key.
pub(crate) fn row_queue(key: &[u8]) -> Result<&str, Error> {
    let padded = key
        .get(1..QUEUE_END)
        .ok_or_else(|| Error::corrupt("queue key is too short"))?;
    let end = padded
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(padded.len());
    std::str::from_utf8(&padded[..end]).map_err(|_| Error::corrupt("queue name is not UTF-8"))
}

/// A key prefix: tag plus padded queue.
fn queue_prefix(tag: u8, queue: &str) -> Vec<u8> {
    let mut prefix = vec![tag];
    prefix.extend_from_slice(&padded_queue(queue));
    prefix
}

/// A key prefix: tag, padded queue and scope hash.
fn scope_prefix(tag: u8, queue: &str, scope: &[u8; 16]) -> Vec<u8> {
    let mut prefix = queue_prefix(tag, queue);
    prefix.extend_from_slice(scope);
    prefix
}

/// The class a pending item is stored in at `now_ms`.
fn class_for(item: &QueueItem, now_ms: u64) -> Class {
    match item.state {
        ItemState::Pending if item.due_ms <= now_ms => Class::Ready,
        ItemState::Pending => Class::Waiting,
        ItemState::Claimed => Class::Claimed,
        ItemState::Dead => Class::Dead,
        ItemState::Completed => Class::Completed,
    }
}

/// The state-row key of an item stored in `class`.
fn state_key(queue: &str, item: &QueueItem, class: Class, id: &[u8; 16]) -> Vec<u8> {
    let legacy = |tag: u8, time: u64| {
        let mut key = queue_prefix(tag, queue);
        key.extend_from_slice(&time.to_be_bytes());
        key.extend_from_slice(id);
        key
    };
    let timed = |tag: u8, time: u64| {
        let mut key = scope_prefix(tag, queue, &scope_hash(&item.scope));
        key.extend_from_slice(&time.to_be_bytes());
        key.extend_from_slice(id);
        key
    };
    match class {
        Class::Ready => {
            let mut key = scope_prefix(TAG_READY, queue, &scope_hash(&item.scope));
            key.extend_from_slice(&priority_bits(item.priority).to_be_bytes());
            key.extend_from_slice(&item.turn.to_be_bytes());
            key.extend_from_slice(&item.due_ms.to_be_bytes());
            key.extend_from_slice(id);
            key
        }
        Class::Waiting => timed(TAG_WAITING, item.due_ms),
        Class::Claimed => timed(TAG_CLAIMED, item.claimed_until_ms),
        Class::Dead => timed(TAG_DEAD, item.due_ms),
        Class::Completed => timed(TAG_COMPLETED, item.updated_ms),
        Class::LegacyPending => legacy(TAG_READY, item.due_ms),
        Class::LegacyClaimed => legacy(TAG_CLAIMED, item.claimed_until_ms),
        Class::LegacyDead => legacy(TAG_DEAD, item.due_ms),
    }
}

/// An ID-row key: tag, padded queue, ID.
fn id_key(queue: &str, id: &[u8; 16]) -> Vec<u8> {
    let mut key = queue_prefix(TAG_ID, queue);
    key.extend_from_slice(id);
    key
}

/// What a state-row key says: where the item is stored and its ID.
fn decode_state_key(key: &[u8]) -> Result<(Class, [u8; 16]), Error> {
    let class = match (key.len(), key.first()) {
        (READY_KEY_LEN, Some(&TAG_READY)) => Class::Ready,
        (TIMED_KEY_LEN, Some(&TAG_WAITING)) => Class::Waiting,
        (TIMED_KEY_LEN, Some(&TAG_CLAIMED)) => Class::Claimed,
        (TIMED_KEY_LEN, Some(&TAG_DEAD)) => Class::Dead,
        (TIMED_KEY_LEN, Some(&TAG_COMPLETED)) => Class::Completed,
        (LEGACY_KEY_LEN, Some(&TAG_READY)) => Class::LegacyPending,
        (LEGACY_KEY_LEN, Some(&TAG_CLAIMED)) => Class::LegacyClaimed,
        (LEGACY_KEY_LEN, Some(&TAG_DEAD)) => Class::LegacyDead,
        _ => return Err(Error::corrupt("queue key has a bad shape")),
    };
    let id: [u8; 16] = key[key.len() - 16..]
        .try_into()
        .map_err(|_| Error::corrupt("queue key ID"))?;
    Ok((class, id))
}

/// Decode an ID row back into its state-row key: the key itself as hex
/// text, or, from before scopes, the state and time of a legacy row.
fn decode_pointer(queue: &str, id: &[u8; 16], bytes: &[u8]) -> Result<Vec<u8>, Error> {
    match crate::json::parse_value(bytes)? {
        Value::String(hex) => {
            crate::hex::decode(&hex).ok_or_else(|| Error::corrupt("queue pointer is not hex"))
        }
        Value::Array(items) => match items.as_slice() {
            [Value::String(state), Value::String(time)] => {
                let time = time
                    .parse::<u64>()
                    .map_err(|_| Error::corrupt("queue pointer has a bad time"))?;
                let tag = match ItemState::parse(state)? {
                    ItemState::Pending => TAG_READY,
                    ItemState::Claimed => TAG_CLAIMED,
                    ItemState::Dead => TAG_DEAD,
                    ItemState::Completed => return Err(Error::corrupt("legacy completed item")),
                };
                let mut key = queue_prefix(tag, queue);
                key.extend_from_slice(&time.to_be_bytes());
                key.extend_from_slice(id);
                Ok(key)
            }
            _ => Err(Error::corrupt("queue pointer has a bad shape")),
        },
        _ => Err(Error::corrupt("queue pointer is not text")),
    }
}

/// Decode a stored item ID (always 32 lowercase hex chars).
fn item_id(id: &str) -> Result<[u8; 16], Error> {
    crate::hex::decode(id)
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(|| Error::corrupt("queue item ID is not 16 hex bytes"))
}

/// Decode a caller-supplied 32-char hex ID.
fn decode_id(id: &str) -> Result<[u8; 16], Failure> {
    crate::hex::decode(id)
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(|| fail(codes::INVALID_VALUE, "item ID is 32 lowercase hex chars"))
}

/// The record writes storing an item, as of `now_ms`: its state row in
/// the class its state and due time call for, plus its ID row.
pub(crate) fn item_rows(
    queue: &str,
    item: &QueueItem,
    now_ms: u64,
) -> Result<Vec<RowWrite>, Error> {
    let id = item_id(&item.id)?;
    let key = state_key(queue, item, class_for(item, now_ms), &id);
    let pointer = crate::json::render(&Value::String(crate::hex::encode(&key)))?;
    Ok(vec![
        (key, Some(encode_item(item)?)),
        (id_key(queue, &id), Some(pointer)),
    ])
}

/// The write removing an item's state row from wherever it is stored.
fn state_removal(queue: &str, item: &QueueItem) -> Result<Option<RowWrite>, Error> {
    let Some(class) = item.class else {
        return Ok(None);
    };
    let id = item_id(&item.id)?;
    Ok(Some((state_key(queue, item, class, &id), None)))
}

/// Find an item by queue and ID through its ID row.
fn find(
    read: &impl RecordRead,
    db: &[u8],
    collection: u32,
    queue: &str,
    id: &[u8; 16],
) -> Result<Option<QueueItem>, Error> {
    let Some(pointer) = read.record(db, collection, &id_key(queue, id))? else {
        return Ok(None);
    };
    let key = decode_pointer(queue, id, &pointer)?;
    let bytes = read
        .record(db, collection, &key)?
        .ok_or_else(|| Error::corrupt("queue pointer dangles"))?;
    Ok(Some(decode_item(&key, &bytes)?))
}

/// Full-key bounds of the rows of one key length starting with
/// `prefix`: inclusive lower, exclusive upper.
fn prefix_bounds(
    db: &[u8],
    collection: u32,
    key_len: usize,
    prefix: &[u8],
) -> Result<(Vec<u8>, Vec<u8>), Error> {
    let lower = record_key_class(db, collection, key_len, prefix)?;
    let upper = prefix_end(&lower).ok_or_else(|| Error::corrupt("queue class has no end"))?;
    Ok((lower, upper))
}

/// The full-key bound at `prefix` followed by `time`.
fn time_bound(
    db: &[u8],
    collection: u32,
    key_len: usize,
    prefix: &[u8],
    time: u64,
) -> Result<Vec<u8>, Error> {
    let mut bound = prefix.to_vec();
    bound.extend_from_slice(&time.to_be_bytes());
    record_key_class(db, collection, key_len, &bound)
}

/// A stored row: its key and value bytes.
type KeyedRow = (Vec<u8>, Vec<u8>);

/// Rows of one key length under `prefix`, ordered by a time right after
/// it, within `[from, to]`, at most `limit`.
#[allow(clippy::too_many_arguments)]
fn timed_rows(
    read: &impl RecordRead,
    db: &[u8],
    collection: u32,
    key_len: usize,
    prefix: &[u8],
    from: u64,
    to: u64,
    limit: usize,
) -> Result<Vec<KeyedRow>, Error> {
    if from > to {
        return Ok(Vec::new());
    }
    let lower = time_bound(db, collection, key_len, prefix, from)?;
    let upper = match to.checked_add(1) {
        Some(end) => time_bound(db, collection, key_len, prefix, end)?,
        None => prefix_bounds(db, collection, key_len, prefix)?.1,
    };
    read.records(db, collection, Some(&lower), Some(&upper), Some(limit))
}

/// Visit the rows within full-key bounds `[lower, upper)`, in pages, in
/// key order, until `visit` answers false.
fn page_rows(
    read: &impl RecordRead,
    db: &[u8],
    collection: u32,
    mut lower: Vec<u8>,
    upper: &[u8],
    visit: &mut impl FnMut(Vec<u8>, Vec<u8>) -> Result<bool, Error>,
) -> Result<(), Error> {
    loop {
        let page = read.records(db, collection, Some(&lower), Some(upper), Some(PAGE))?;
        let full = page.len() == PAGE;
        let mut last = None;
        for (key, bytes) in page {
            last = Some(key.clone());
            if !visit(key, bytes)? {
                return Ok(());
            }
        }
        match last {
            Some(key) if full => {
                // Resume just past the last key, its immediate successor,
                // never at a time: many rows share one.
                let mut next = encode_record_key(db, collection, &key)?;
                next.push(0);
                lower = next;
            }
            _ => return Ok(()),
        }
    }
}

/// Visit every row of one key length starting with `prefix`, in pages.
fn for_each_row(
    read: &impl RecordRead,
    db: &[u8],
    collection: u32,
    key_len: usize,
    prefix: &[u8],
    visit: &mut impl FnMut(Vec<u8>, Vec<u8>) -> Result<(), Error>,
) -> Result<(), Error> {
    let (lower, upper) = prefix_bounds(db, collection, key_len, prefix)?;
    page_rows(read, db, collection, lower, &upper, &mut |key, bytes| {
        visit(key, bytes).map(|()| true)
    })
}

/// Visit the rows of one key length under `prefix` whose time (right
/// after it) is within `[from, to]`, in pages, in key order, until
/// `visit` answers false.
#[allow(clippy::too_many_arguments)]
fn for_each_timed(
    read: &impl RecordRead,
    db: &[u8],
    collection: u32,
    key_len: usize,
    prefix: &[u8],
    from: u64,
    to: u64,
    visit: &mut impl FnMut(Vec<u8>, Vec<u8>) -> Result<bool, Error>,
) -> Result<(), Error> {
    if from > to {
        return Ok(());
    }
    let lower = time_bound(db, collection, key_len, prefix, from)?;
    let upper = match to.checked_add(1) {
        Some(end) => time_bound(db, collection, key_len, prefix, end)?,
        None => prefix_bounds(db, collection, key_len, prefix)?.1,
    };
    page_rows(read, db, collection, lower, &upper, visit)
}

/// Encode an item value as canonical JSON bytes.
pub(crate) fn encode_item(item: &QueueItem) -> Result<Vec<u8>, Error> {
    let json = |value: &Value| -> Result<serde_json::Value, Error> {
        serde_json::from_slice(&crate::json::render(value)?)
            .map_err(|_| Error::corrupt("queue value"))
    };
    let text = |value: &Option<String>| {
        value
            .clone()
            .map(serde_json::Value::from)
            .unwrap_or(serde_json::Value::Null)
    };
    let mut fields = serde_json::Map::new();
    fields.insert("v".to_string(), serde_json::Value::from(3));
    fields.insert("payload".to_string(), json(&item.payload)?);
    fields.insert(
        "state".to_string(),
        serde_json::Value::from(item.state.name()),
    );
    fields.insert(
        "attempts".to_string(),
        serde_json::Value::from(item.attempts),
    );
    fields.insert(
        "max_attempts".to_string(),
        serde_json::Value::from(item.max_attempts),
    );
    fields.insert(
        "claimed_until_ms".to_string(),
        serde_json::Value::from(item.claimed_until_ms),
    );
    fields.insert("token".to_string(), text(&item.token));
    fields.insert(
        "enqueued_ms".to_string(),
        serde_json::Value::from(item.enqueued_ms),
    );
    fields.insert("due_ms".to_string(), serde_json::Value::from(item.due_ms));
    fields.insert(
        "scope".to_string(),
        serde_json::Value::from(item.scope.as_str()),
    );
    fields.insert(
        "priority".to_string(),
        serde_json::Value::from(item.priority),
    );
    fields.insert("turn".to_string(), serde_json::Value::from(item.turn));
    fields.insert("group".to_string(), text(&item.group));
    fields.insert("name".to_string(), text(&item.name));
    fields.insert("fence".to_string(), serde_json::Value::from(item.fence));
    fields.insert("owner".to_string(), text(&item.owner));
    fields.insert(
        "result".to_string(),
        match &item.result {
            Some(result) => json(result)?,
            None => serde_json::Value::Null,
        },
    );
    fields.insert(
        "has_result".to_string(),
        serde_json::Value::from(item.result.is_some()),
    );
    // In decimal text: incarnations are random 64-bit values, which a
    // row's numbers (doubles) would round.
    fields.insert(
        "incarnation".to_string(),
        item.incarnation
            .map(|incarnation| serde_json::Value::from(incarnation.to_string()))
            .unwrap_or(serde_json::Value::Null),
    );
    fields.insert(
        "updated_ms".to_string(),
        serde_json::Value::from(item.updated_ms),
    );
    let bytes = serde_json::to_vec(&serde_json::Value::Object(fields))
        .map_err(|_| Error::corrupt("queue item is not JSON"))?;
    crate::json::canonicalize(&bytes)
}

/// Decode an item value stored under `key`. Items stored before scopes,
/// fences, turns or names lack those fields and read with defaults.
fn decode_item(key: &[u8], bytes: &[u8]) -> Result<QueueItem, Error> {
    let (class, id) = decode_state_key(key)?;
    let value: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|_| Error::corrupt("queue item is not JSON"))?;
    let get = |name: &str| {
        value
            .get(name)
            .ok_or_else(|| Error::corrupt("queue item misses a field"))
    };
    let from_json = |field: &serde_json::Value| {
        crate::json::parse_value(
            &serde_json::to_vec(field).map_err(|_| Error::corrupt("queue value"))?,
        )
    };
    let payload = from_json(get("payload")?)?;
    let state = ItemState::parse(
        get("state")?
            .as_str()
            .ok_or_else(|| Error::corrupt("queue state"))?,
    )?;
    let expected = match class {
        Class::Ready | Class::Waiting | Class::LegacyPending => ItemState::Pending,
        Class::Claimed | Class::LegacyClaimed => ItemState::Claimed,
        Class::Dead | Class::LegacyDead => ItemState::Dead,
        Class::Completed => ItemState::Completed,
    };
    if state != expected {
        return Err(Error::corrupt("queue item disagrees with its key"));
    }
    // Counters round-trip through canonical JSON, which renders
    // numbers as f64: large millisecond times come back as floats.
    // All live values sit far below 2^53, so the cast is exact.
    let number = |field: &serde_json::Value| {
        if let Some(int) = field.as_u64() {
            return Ok(int);
        }
        match field.as_f64() {
            Some(float) if float.is_finite() && float.fract() == 0.0 && float >= 0.0 => {
                Ok(float as u64)
            }
            _ => Err(Error::corrupt("queue counter")),
        }
    };
    let optional = |name: &str| match value.get(name) {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(field) => number(field).map(Some),
    };
    let text = |name: &str| {
        value
            .get(name)
            .and_then(serde_json::Value::as_str)
            .map(str::to_string)
    };
    let counter = |name: &str| {
        u32::try_from(number(get(name)?)?).map_err(|_| Error::corrupt("queue counter exceeds u32"))
    };
    let priority = match value.get("priority") {
        None | Some(serde_json::Value::Null) => 0,
        Some(field) => match field.as_i64() {
            Some(priority) => priority,
            None => match field.as_f64() {
                Some(float) if float.is_finite() && float.fract() == 0.0 => float as i64,
                _ => return Err(Error::corrupt("queue priority")),
            },
        },
    };
    let has_result = value
        .get("has_result")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let due_ms = number(get("due_ms")?)?;
    Ok(QueueItem {
        id: crate::hex::encode(&id),
        payload,
        state,
        attempts: counter("attempts")?,
        max_attempts: counter("max_attempts")?,
        claimed_until_ms: number(get("claimed_until_ms")?)?,
        token: text("token"),
        enqueued_ms: number(get("enqueued_ms")?)?,
        due_ms,
        scope: text("scope").unwrap_or_default(),
        priority,
        turn: optional("turn")?.unwrap_or(0),
        group: text("group"),
        name: text("name"),
        fence: optional("fence")?.unwrap_or(0),
        owner: text("owner"),
        result: if has_result {
            Some(from_json(get("result")?)?)
        } else {
            None
        },
        incarnation: match value.get("incarnation") {
            Some(serde_json::Value::String(text)) => Some(
                text.parse()
                    .map_err(|_| Error::corrupt("queue incarnation is not a number"))?,
            ),
            _ => optional("incarnation")?,
        },
        updated_ms: optional("updated_ms")?.unwrap_or(due_ms),
        class: Some(class),
    })
}

/// How a caller names a job: its ID, or its scope and name.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JobRef<'a> {
    /// The item ID, 32 hex chars.
    Id(&'a str),
    /// A named job.
    Named {
        /// Its scope.
        scope: &'a str,
        /// Its name.
        name: &'a str,
    },
}

impl JobRef<'_> {
    fn id(&self) -> Result<[u8; 16], Failure> {
        match self {
            JobRef::Id(id) => decode_id(id),
            JobRef::Named { scope, name } => {
                validate_scope(scope)?;
                validate_name(name, "job name")?;
                decode_id(&named_id(scope, name))
            }
        }
    }
}

/// An enqueue: everything but the clock and the item ID.
#[derive(Clone, Copy, Debug)]
pub struct NewItem<'a> {
    /// Target queue.
    pub queue: &'a str,
    /// Item payload.
    pub payload: &'a Value,
    /// Delay before the item is due.
    pub delay_ms: u64,
    /// Attempts before the item dies.
    pub max_attempts: u32,
    /// The scope within the queue (`""` by default).
    pub scope: &'a str,
    /// Claim priority: lower values claim first.
    pub priority: i64,
    /// The group whose turns it takes, if any.
    pub group: Option<&'a str>,
    /// A job name unique within the scope; random IDs without one.
    pub name: Option<&'a str>,
    /// Replace a completed or dead job of the same name.
    pub replace: bool,
}

impl<'a> NewItem<'a> {
    /// A plain enqueue: the default scope, priority 0, no group or name.
    pub fn new(queue: &'a str, payload: &'a Value, delay_ms: u64, max_attempts: u32) -> Self {
        NewItem {
            queue,
            payload,
            delay_ms,
            max_attempts,
            scope: "",
            priority: 0,
            group: None,
            name: None,
            replace: false,
        }
    }

    /// Check every parameter.
    pub(crate) fn validate(&self) -> Result<(), Failure> {
        validate_enqueue(self.queue, self.delay_ms, self.max_attempts)?;
        validate_scope(self.scope)?;
        validate_priority(self.priority)?;
        if let Some(group) = self.group {
            validate_name(group, "group")?;
        }
        if let Some(name) = self.name {
            validate_name(name, "job name")?;
        }
        Ok(())
    }
}

/// A claim: which scope, by whom, how many and for how long, and
/// whether to wait in line when it comes back short.
#[derive(Clone, Copy, Debug)]
pub struct ClaimRequest<'a> {
    /// The scope to claim from (`""` by default).
    pub scope: &'a str,
    /// The claimer, recorded on the lease when given.
    pub owner: Option<&'a str>,
    /// Most items to claim.
    pub limit: usize,
    /// Lease length.
    pub lease_ms: u64,
    /// Wait in the scope's line this long when the claim comes back
    /// short (needs an owner); a place leaves the line otherwise.
    pub wait_ms: Option<u64>,
}

impl ClaimRequest<'_> {
    /// Check every parameter.
    pub(crate) fn validate(&self) -> Result<(), Failure> {
        validate_scope(self.scope)?;
        if let Some(owner) = self.owner {
            validate_name(owner, "lease owner")?;
        }
        if let Some(wait_ms) = self.wait_ms {
            if self.owner.is_none() {
                return Err(fail(codes::INVALID_VALUE, "waiting in line needs an owner"));
            }
            if wait_ms > MAX_WAIT_MS {
                return Err(fail(codes::INVALID_VALUE, "line wait is too long"));
            }
        }
        validate_lease(self.lease_ms)
    }
}

/// How a report settles a live claim.
#[derive(Clone, Copy, Debug)]
pub enum Report<'a> {
    /// Complete it: kept as completed with the result (checked against
    /// the queue's result contracts), or deleted without one.
    Complete(Option<&'a Value>),
    /// Fail it: pending again after this delay, or dead past its
    /// attempt budget.
    Fail(u64),
    /// Move it to the dead letters now.
    Kill,
}

/// An operation's answer plus the record writes carrying it out.
pub(crate) type Plan<T> = Result<(T, Vec<RecordWrite>), Failure>;

/// Rows by full key: their own key and their value, `None` for a staged
/// deletion.
type Merged = BTreeMap<Vec<u8>, (Vec<u8>, Option<Vec<u8>>)>;

/// Committed rows plus an invocation's staged writes over them: what a
/// guest queue operation reads, so a mutation sees its own enqueues and
/// claims before they commit. A chained claim reads its report's
/// writes the same way.
pub(crate) struct Overlay<'a, R: RecordRead = ReadTxn> {
    /// The pinned snapshot.
    pub read: &'a R,
    /// The invocation's staged writes.
    pub staged: &'a Staged,
}

impl<R: RecordRead> RecordRead for Overlay<'_, R> {
    fn record(
        &self,
        db: &[u8],
        collection: u32,
        key: &[u8],
    ) -> crate::error::Result<Option<Vec<u8>>> {
        match self.staged.staged_record(collection, key) {
            Some(staged) => Ok(staged),
            None => self.read.record(db, collection, key),
        }
    }

    fn records(
        &self,
        db: &[u8],
        collection: u32,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
        limit: Option<usize>,
    ) -> crate::error::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut staged = Vec::new();
        for (key, value) in self.staged.staged_records_for(collection) {
            let full = encode_record_key(db, collection, &key)?;
            if lower.is_some_and(|lower| full.as_slice() < lower)
                || upper.is_some_and(|upper| full.as_slice() >= upper)
            {
                continue;
            }
            staged.push((full, key, value));
        }
        // Every staged row can displace at most one committed row, so
        // the page plus the staged count covers the merged page.
        let cap = limit.map(|limit| limit + staged.len());
        let committed = self.read.records(db, collection, lower, upper, cap)?;
        let truncated = cap.is_some_and(|cap| committed.len() >= cap);
        let mut horizon = None;
        let mut merged: Merged = BTreeMap::new();
        for (key, value) in committed {
            let full = encode_record_key(db, collection, &key)?;
            if truncated {
                horizon = Some(full.clone());
            }
            merged.insert(full, (key, Some(value)));
        }
        for (full, key, value) in staged {
            merged.insert(full, (key, value));
        }
        let mut rows = Vec::new();
        for (full, (key, value)) in merged {
            // Past the last committed row a truncated read loaded,
            // staged rows would jump unread committed ones.
            if horizon.as_ref().is_some_and(|horizon| full > *horizon) {
                break;
            }
            if let Some(value) = value {
                rows.push((key, value));
            }
            if limit.is_some_and(|limit| rows.len() >= limit) {
                break;
            }
        }
        Ok(rows)
    }
}

/// One owner's place in a scope's line.
#[derive(Clone, Debug)]
struct Waiter {
    owner: String,
    room: u64,
    since: u64,
    expires: u64,
    key: Vec<u8>,
}

/// The writes one plan makes, with the counters and items it changed
/// along the way, so later steps of the plan read its own changes.
struct Changes<'q> {
    queue: &'q str,
    collection: u32,
    now_ms: u64,
    /// Counter rows by key: the value now, `None` once deleted.
    counters: BTreeMap<Vec<u8>, Option<u64>>,
    /// Touched items by ID: as stored before the plan, and as it leaves
    /// them (`None`: removed).
    items: BTreeMap<String, (QueueItem, Option<QueueItem>)>,
    /// Other rows, in order.
    rows: Vec<RowWrite>,
}

impl<'q> Changes<'q> {
    fn new(queue: &'q str, collection: u32, now_ms: u64) -> Self {
        Changes {
            queue,
            collection,
            now_ms,
            counters: BTreeMap::new(),
            items: BTreeMap::new(),
            rows: Vec::new(),
        }
    }

    /// Record an item's new state (or its removal).
    fn put(&mut self, before: &QueueItem, after: Option<QueueItem>) {
        match self.items.get_mut(&before.id) {
            Some(entry) => entry.1 = after,
            None => {
                self.items
                    .insert(before.id.clone(), (before.clone(), after));
            }
        }
    }

    /// The record writes carrying every change out.
    fn finish(self) -> Result<Vec<RecordWrite>, Failure> {
        let mut rows = Vec::new();
        for (before, after) in self.items.into_values() {
            if let Some(removal) = state_removal(self.queue, &before)? {
                rows.push(removal);
            }
            match after {
                Some(after) => rows.extend(item_rows(self.queue, &after, self.now_ms)?),
                None => rows.push((id_key(self.queue, &item_id(&before.id)?), None)),
            }
        }
        for (key, value) in self.counters {
            let value = match value {
                Some(value) => Some(crate::json::render(&Value::String(value.to_string()))?),
                None => None,
            };
            rows.push((key, value));
        }
        rows.extend(self.rows);
        Ok(rows
            .into_iter()
            .map(|(key, value)| RecordWrite {
                collection: self.collection,
                key,
                value,
            })
            .collect())
    }
}

/// One queue as of one read: committed state for plans, a write
/// transaction's own view for direct commits, an invocation's overlay
/// for guest operations. Every queue operation is planned here once,
/// as record writes, whoever commits them.
pub(crate) struct QueueView<'a, R: RecordRead> {
    pub(crate) read: &'a R,
    pub(crate) db: &'a [u8],
    /// The `$queue` collection, or `None` before queues exist.
    pub(crate) collection: Option<u32>,
    pub(crate) queue: &'a str,
    /// The database's history incarnation: claims are bound to it.
    pub(crate) incarnation: u64,
    /// Earlier incarnations of the same history, which the database
    /// carried here through moves: claims bound to them stay good.
    pub(crate) lineage: Vec<u64>,
    /// What modules declare about the queue.
    pub(crate) config: QueueConfig,
}

impl<R: RecordRead> QueueView<'_, R> {
    /// The `$queue` collection, failing before queues exist.
    fn collection(&self) -> Result<u32, Failure> {
        self.collection
            .ok_or_else(|| fail(codes::INVALID_VALUE, "queues are not initialized"))
    }

    /// Find an item in any state by ID through its ID row.
    fn find(&self, id: &[u8; 16]) -> Result<Option<QueueItem>, Error> {
        match self.collection {
            None => Ok(None),
            Some(collection) => find(self.read, self.db, collection, self.queue, id),
        }
    }

    /// Find an item, preferring what this plan already changed.
    fn current(&self, changes: &Changes, id: &[u8; 16]) -> Result<Option<QueueItem>, Error> {
        match changes.items.get(&crate::hex::encode(id)) {
            Some((_, after)) => Ok(after.clone()),
            None => self.find(id),
        }
    }

    /// Find an item by ID or name, as of `now_ms` (see [`effective`]).
    pub(crate) fn get(&self, job: &JobRef, now_ms: u64) -> Plan<Option<QueueItem>> {
        let item = self.find(&job.id()?)?;
        Ok((item.map(|item| effective(item, now_ms)), Vec::new()))
    }

    /// A counter row, as this plan left it.
    fn counter(&self, changes: &Changes, key: &[u8]) -> Result<u64, Error> {
        if let Some(value) = changes.counters.get(key) {
            return Ok(value.unwrap_or(0));
        }
        let Some(collection) = self.collection else {
            return Ok(0);
        };
        let Some(bytes) = self.read.record(self.db, collection, key)? else {
            return Ok(0);
        };
        match crate::json::parse_value(&bytes)? {
            Value::String(text) => text
                .parse()
                .map_err(|_| Error::corrupt("queue counter is not a number")),
            _ => Err(Error::corrupt("queue counter is not text")),
        }
    }

    fn fence_key(&self, scope: &[u8; 16]) -> Vec<u8> {
        scope_prefix(TAG_FENCE, self.queue, scope)
    }

    fn clock_key(&self, scope: &[u8; 16], priority: i64) -> Vec<u8> {
        let mut key = scope_prefix(TAG_TURN, self.queue, scope);
        key.extend_from_slice(&priority_bits(priority).to_be_bytes());
        key
    }

    fn group_key(&self, scope: &[u8; 16], priority: i64, group: &str) -> Vec<u8> {
        let mut key = scope_prefix(TAG_GROUP, self.queue, scope);
        key.extend_from_slice(&priority_bits(priority).to_be_bytes());
        key.extend_from_slice(&hash16("flower.queue.group", &[group.as_bytes()]));
        key
    }

    /// The next turn for a job of this priority and group (v1's
    /// `place`): the priority's current turn, or for a group its own next
    /// turn when that is later, so each group's jobs take turns with the
    /// others'.
    fn place(
        &self,
        changes: &mut Changes,
        scope: &[u8; 16],
        priority: i64,
        group: Option<&str>,
    ) -> Result<u64, Error> {
        let at = self.counter(changes, &self.clock_key(scope, priority))?;
        let Some(group) = group else {
            return Ok(at);
        };
        let key = self.group_key(scope, priority, group);
        let turn = at.max(self.counter(changes, &key)?);
        changes.counters.insert(key, Some(turn + 1));
        Ok(turn)
    }

    /// Whether a claim taken in history incarnation `taken` belongs to
    /// this database's history: taken under its incarnation, or under
    /// one it carried here through moves since its last restore (see
    /// [`crate::placement::lineage`]).
    fn same_history(&self, taken: u64) -> bool {
        taken == self.incarnation || self.lineage.contains(&taken)
    }

    /// Resolve a live claim as of `now_ms`, checking its token and
    /// history. A claim that lapsed on its last attempt died at its
    /// lease's end (see [`effective`]): its holder can no longer report.
    fn claimed(
        &self,
        changes: &Changes,
        id: &str,
        token: &str,
        now_ms: u64,
    ) -> Result<QueueItem, Failure> {
        match self.current(changes, &decode_id(id)?)? {
            Some(item) if item.state == ItemState::Claimed => {
                if item.token.as_deref() != Some(token) {
                    return Err(fail(codes::INVALID_VALUE, "claim token mismatch"));
                }
                if item
                    .incarnation
                    .is_some_and(|taken| !self.same_history(taken))
                {
                    return Err(fail(
                        codes::INVALID_VALUE,
                        "the claim was taken in another history",
                    ));
                }
                if lapsed_dead(&item, now_ms) {
                    return Err(fail(
                        codes::INVALID_VALUE,
                        "claim lapsed on its last attempt",
                    ));
                }
                Ok(item)
            }
            _ => Err(fail(codes::INVALID_VALUE, "unknown queue item")),
        }
    }

    /// Enqueue an item, under a name when given: a name already taken in
    /// the scope fails `JOB_EXISTS`, unless `replace` and that job is
    /// completed or dead.
    pub(crate) fn enqueue(&self, item: &NewItem, now_ms: u64, random: &[u8; 16]) -> Plan<String> {
        item.validate()?;
        QueueConfig::check(&self.config.payload, item.payload, "payload")?;
        let collection = self.collection()?;
        let mut changes = Changes::new(self.queue, collection, now_ms);
        let id = match item.name {
            Some(name) => decode_id(&named_id(item.scope, name))?,
            None => *random,
        };
        let existing = self.find(&id)?;
        // As of now: a claim that lapsed on its last attempt is dead, so
        // `replace` takes it (the stored item still names its row).
        if let Some(existing) = &existing
            && (!item.replace
                || matches!(
                    effective(existing.clone(), now_ms).state,
                    ItemState::Pending | ItemState::Claimed
                ))
        {
            return Err(fail(
                "JOB_EXISTS",
                format!("job {} already exists", item.name.unwrap_or(&existing.id)),
            ));
        }
        let scope = scope_hash(item.scope);
        let turn = self.place(&mut changes, &scope, item.priority, item.group)?;
        let stored = QueueItem {
            id: crate::hex::encode(&id),
            payload: item.payload.clone(),
            state: ItemState::Pending,
            attempts: 0,
            max_attempts: item.max_attempts,
            claimed_until_ms: 0,
            token: None,
            enqueued_ms: now_ms,
            due_ms: now_ms.saturating_add(item.delay_ms),
            scope: item.scope.to_string(),
            priority: item.priority,
            turn,
            group: item.group.map(str::to_string),
            name: item.name.map(str::to_string),
            fence: 0,
            owner: None,
            result: None,
            incarnation: None,
            updated_ms: now_ms,
            class: None,
        };
        match existing {
            Some(existing) => changes.put(&existing, Some(stored.clone())),
            None => changes.put(&stored, Some(stored.clone())),
        }
        Ok((stored.id, changes.finish()?))
    }

    /// Move into the ready rows what became claimable in one scope:
    /// waiting items now due and claims whose lease lapsed, at most
    /// [`PROMOTE`] of each; a lapsed claim on its last attempt dies
    /// instead, as v1 fails it.
    fn promote(
        &self,
        changes: &mut Changes,
        scope: &str,
        now_ms: u64,
    ) -> Result<Vec<QueueItem>, Failure> {
        let collection = self.collection()?;
        let hash = scope_hash(scope);
        let mut rows = timed_rows(
            self.read,
            self.db,
            collection,
            TIMED_KEY_LEN,
            &scope_prefix(TAG_WAITING, self.queue, &hash),
            0,
            now_ms,
            PROMOTE,
        )?;
        rows.extend(timed_rows(
            self.read,
            self.db,
            collection,
            TIMED_KEY_LEN,
            &scope_prefix(TAG_CLAIMED, self.queue, &hash),
            0,
            now_ms,
            PROMOTE,
        )?);
        if scope.is_empty() {
            for tag in [TAG_READY, TAG_CLAIMED] {
                rows.extend(timed_rows(
                    self.read,
                    self.db,
                    collection,
                    LEGACY_KEY_LEN,
                    &queue_prefix(tag, self.queue),
                    0,
                    now_ms,
                    PROMOTE,
                )?);
            }
        }
        let mut ready = Vec::new();
        for (key, bytes) in rows {
            let item = decode_item(&key, &bytes)?;
            if changes.items.contains_key(&item.id) {
                continue;
            }
            let next = match item.state {
                ItemState::Claimed => lapsed(&item),
                _ => item.clone(),
            };
            if next.state == ItemState::Dead {
                changes.put(&item, Some(next));
                continue;
            }
            changes.put(&item, Some(next.clone()));
            ready.push(next);
        }
        Ok(ready)
    }

    /// When the next claimable item of one scope became claimable, and
    /// when, failing any, the next delayed item falls due or lease
    /// lapses.
    fn availability(
        &self,
        scope: &str,
        now_ms: u64,
    ) -> Result<(Option<u64>, Option<u64>), Failure> {
        let Some(collection) = self.collection else {
            return Ok((None, None));
        };
        let hash = scope_hash(scope);
        let first = |key_len: usize, prefix: &[u8], from: u64, to: u64| {
            timed_rows(self.read, self.db, collection, key_len, prefix, from, to, 1)
        };
        let mut available: Option<u64> = None;
        let mut take = |time: u64| {
            available = Some(available.map_or(time, |at: u64| at.min(time)));
        };
        let (lower, upper) = prefix_bounds(
            self.db,
            collection,
            READY_KEY_LEN,
            &scope_prefix(TAG_READY, self.queue, &hash),
        )?;
        if let Some((key, bytes)) = self
            .read
            .records(self.db, collection, Some(&lower), Some(&upper), Some(1))?
            .into_iter()
            .next()
        {
            let item = decode_item(&key, &bytes)?;
            take(item.due_ms.max(item.enqueued_ms));
        }
        let mut waiting = vec![(TIMED_KEY_LEN, scope_prefix(TAG_WAITING, self.queue, &hash))];
        let mut claimed = vec![(TIMED_KEY_LEN, scope_prefix(TAG_CLAIMED, self.queue, &hash))];
        if scope.is_empty() {
            waiting.push((LEGACY_KEY_LEN, queue_prefix(TAG_READY, self.queue)));
            claimed.push((LEGACY_KEY_LEN, queue_prefix(TAG_CLAIMED, self.queue)));
        }
        let mut next: Option<u64> = None;
        let mut later = |time: u64| next = Some(next.map_or(time, |at: u64| at.min(time)));
        for (key_len, prefix) in &waiting {
            if let Some((key, bytes)) = first(*key_len, prefix, 0, now_ms)?.into_iter().next() {
                let item = decode_item(&key, &bytes)?;
                take(item.due_ms.max(item.enqueued_ms));
            }
            if let Some((key, _)) = first(*key_len, prefix, now_ms.saturating_add(1), u64::MAX)?
                .into_iter()
                .next()
            {
                later(time_of(&key));
            }
        }
        for (key_len, prefix) in &claimed {
            // Lapsed claims on their last attempt die rather than
            // redeliver: look past them, page by page, for one that
            // redelivers.
            let mut visit = |key: Vec<u8>, bytes: Vec<u8>| -> Result<bool, Error> {
                let item = decode_item(&key, &bytes)?;
                if item.attempts < item.max_attempts {
                    take(item.claimed_until_ms);
                    return Ok(false);
                }
                Ok(true)
            };
            for_each_timed(
                self.read, self.db, collection, *key_len, prefix, 0, now_ms, &mut visit,
            )?;
            if let Some((key, _)) = first(*key_len, prefix, now_ms.saturating_add(1), u64::MAX)?
                .into_iter()
                .next()
            {
                later(time_of(&key));
            }
        }
        Ok((available, next))
    }

    /// The first owner in a scope's line whose place has not run out, and
    /// the places that ran out ahead of it.
    fn head(&self, hash: &[u8; 16], now_ms: u64) -> Result<(Option<Waiter>, Vec<Waiter>), Failure> {
        let Some(collection) = self.collection else {
            return Ok((None, Vec::new()));
        };
        let prefix = scope_prefix(TAG_LINE, self.queue, hash);
        let mut expired = Vec::new();
        let mut first = None;
        let mut visit = |key: Vec<u8>, bytes: Vec<u8>| -> Result<(), Error> {
            if first.is_some() {
                return Ok(());
            }
            let waiter = decode_waiter(key, &bytes)?;
            if waiter.expires <= now_ms {
                expired.push(waiter);
            } else {
                first = Some(waiter);
            }
            Ok(())
        };
        for_each_row(
            self.read,
            self.db,
            collection,
            LINE_KEY_LEN,
            &prefix,
            &mut visit,
        )?;
        Ok((first, expired))
    }

    /// An owner's place in a scope's line, if it has one.
    fn place_of(&self, hash: &[u8; 16], owner: &str) -> Result<Option<Waiter>, Failure> {
        let Some(collection) = self.collection else {
            return Ok(None);
        };
        let pointer = owner_key(self.queue, hash, owner);
        let Some(bytes) = self.read.record(self.db, collection, &pointer)? else {
            return Ok(None);
        };
        let Value::String(hex) = crate::json::parse_value(&bytes)? else {
            return Err(Failure::from(Error::corrupt("line pointer is not text")));
        };
        let key = crate::hex::decode(&hex)
            .ok_or_else(|| Failure::from(Error::corrupt("line pointer is not hex")))?;
        let bytes = self
            .read
            .record(self.db, collection, &key)?
            .ok_or_else(|| Failure::from(Error::corrupt("line pointer dangles")))?;
        Ok(Some(decode_waiter(key, &bytes)?))
    }

    /// Remove a place from the line.
    fn leave(&self, changes: &mut Changes, hash: &[u8; 16], waiter: &Waiter) {
        changes.rows.push((waiter.key.clone(), None));
        changes
            .rows
            .push((owner_key(self.queue, hash, &waiter.owner), None));
    }

    /// v1's line: a claim that came back short with `wait_ms` takes (or
    /// keeps) the owner's place, asking for `room` more items; otherwise
    /// the owner leaves the line.
    fn wait_in_line(
        &self,
        changes: &mut Changes,
        scope: &str,
        owner: &str,
        room: u64,
        wait_ms: u64,
        now_ms: u64,
    ) -> Result<(), Failure> {
        let hash = scope_hash(scope);
        let existing = self.place_of(&hash, owner)?;
        if room == 0 || wait_ms == 0 {
            if let Some(existing) = existing {
                self.leave(changes, &hash, &existing);
            }
            return Ok(());
        }
        let expires = now_ms.saturating_add(wait_ms);
        // An unchanged poll need not rewrite a fresh place.
        if let Some(existing) = &existing
            && existing.room == room
            && existing.expires <= expires
            && existing.expires.saturating_sub(now_ms) > wait_ms / 4 * 3
        {
            return Ok(());
        }
        let since = match &existing {
            Some(existing) if existing.expires > now_ms => existing.since,
            _ => now_ms,
        };
        if let Some(existing) = &existing {
            changes.rows.push((existing.key.clone(), None));
        }
        let mut key = scope_prefix(TAG_LINE, self.queue, &hash);
        key.extend_from_slice(&since.to_be_bytes());
        key.extend_from_slice(&hash16("flower.queue.owner", &[owner.as_bytes()]));
        let mut place = BTreeMap::new();
        place.insert("owner".to_string(), Value::String(owner.to_string()));
        place.insert("room".to_string(), Value::Number(room as f64));
        place.insert("since".to_string(), Value::Number(since as f64));
        place.insert("expires".to_string(), Value::Number(expires as f64));
        changes.rows.push((
            key.clone(),
            Some(crate::json::render(&Value::Object(place))?),
        ));
        changes.rows.push((
            owner_key(self.queue, &hash, owner),
            Some(crate::json::render(&Value::String(crate::hex::encode(
                &key,
            )))?),
        ));
        Ok(())
    }

    /// Claim the first claimable items of one scope: lowest priority
    /// first, then each group's turn, then the longest waiting; one
    /// token and the scope's next fencing token each, bound to the
    /// history incarnation. The line's first owner keeps new work to
    /// itself for the queue's turn, then loses its place to whoever
    /// claims instead.
    pub(crate) fn claim(
        &self,
        request: &ClaimRequest,
        now_ms: u64,
        tokens: &[String],
    ) -> Plan<Vec<ClaimedItem>> {
        request.validate()?;
        let limit = request.limit.min(MAX_POLL_LIMIT).min(tokens.len());
        let Some(collection) = self.collection else {
            return Ok((Vec::new(), Vec::new()));
        };
        let mut changes = Changes::new(self.queue, collection, now_ms);
        let hash = scope_hash(request.scope);
        if limit > 0 {
            let (head, expired) = self.head(&hash, now_ms)?;
            for waiter in &expired {
                self.leave(&mut changes, &hash, waiter);
            }
            // First in line, yet it let work wait a whole turn: it is gone
            // or stuck, and loses its place to whoever claims instead.
            if let Some(head) = head
                && request.owner != Some(head.owner.as_str())
                && let (Some(since), _) = self.availability(request.scope, now_ms)?
                && now_ms >= since.saturating_add(self.config.turn_ms)
            {
                self.leave(&mut changes, &hash, &head);
            }
        }
        let mut candidates = self.promote(&mut changes, request.scope, now_ms)?;
        let (lower, upper) = prefix_bounds(
            self.db,
            collection,
            READY_KEY_LEN,
            &scope_prefix(TAG_READY, self.queue, &hash),
        )?;
        if limit > 0 {
            for (key, bytes) in
                self.read
                    .records(self.db, collection, Some(&lower), Some(&upper), Some(limit))?
            {
                candidates.push(decode_item(&key, &bytes)?);
            }
        }
        candidates.sort_by(|left, right| {
            (left.priority, left.turn, left.due_ms, &left.id).cmp(&(
                right.priority,
                right.turn,
                right.due_ms,
                &right.id,
            ))
        });
        candidates.truncate(limit);
        let fence_key = self.fence_key(&hash);
        let mut fence = self.counter(&changes, &fence_key)?;
        let mut claimed = Vec::with_capacity(candidates.len());
        for (item, token) in candidates.into_iter().zip(tokens) {
            fence = fence
                .checked_add(1)
                .ok_or_else(|| fail(codes::INVALID_VALUE, "fencing tokens ran out"))?;
            let stored = match changes.items.get(&item.id) {
                Some((before, _)) => before.clone(),
                None => item.clone(),
            };
            let mut next = item.clone();
            next.state = ItemState::Claimed;
            next.attempts = item.attempts.saturating_add(1);
            next.claimed_until_ms = now_ms.saturating_add(request.lease_ms);
            next.token = Some(token.clone());
            next.fence = fence;
            next.owner = request.owner.map(str::to_string);
            // A database with no incarnation yet (a new cluster, before
            // its leader established one) binds nothing: establishing the
            // first one names this history, it does not replace it.
            next.incarnation = (self.incarnation != 0).then_some(self.incarnation);
            next.updated_ms = now_ms;
            // The priority's clock reaches the claimed turn; a group's
            // record matters only while it has later turns queued.
            let clock = self.clock_key(&hash, item.priority);
            if self.counter(&changes, &clock)? < item.turn {
                changes.counters.insert(clock, Some(item.turn));
            }
            if let Some(group) = &item.group {
                let key = self.group_key(&hash, item.priority, group);
                if self.counter(&changes, &key)? == item.turn + 1 {
                    changes.counters.insert(key, None);
                }
            }
            changes.put(&stored, Some(next.clone()));
            claimed.push(ClaimedItem {
                id: next.id,
                name: next.name,
                payload: next.payload,
                token: token.clone(),
                attempts: next.attempts,
                fence,
                scope: next.scope,
                priority: next.priority,
                group: next.group,
                owner: next.owner,
                expires_ms: next.claimed_until_ms,
                available_ms: item.due_ms.max(item.enqueued_ms),
                claimed_ms: now_ms,
            });
        }
        if !claimed.is_empty() {
            changes.counters.insert(fence_key, Some(fence));
        }
        if let (Some(wait_ms), Some(owner)) = (request.wait_ms, request.owner) {
            let room = (request.limit as u64).saturating_sub(claimed.len() as u64);
            self.wait_in_line(&mut changes, request.scope, owner, room, wait_ms, now_ms)?;
        }
        Ok((claimed, changes.finish()?))
    }

    /// Take, keep or leave a place in line without claiming: what a
    /// claim that admitted fewer than it wanted does last.
    pub(crate) fn wait(
        &self,
        scope: &str,
        owner: &str,
        room: u64,
        wait_ms: u64,
        now_ms: u64,
    ) -> Plan<()> {
        validate_scope(scope)?;
        validate_name(owner, "lease owner")?;
        if wait_ms > MAX_WAIT_MS {
            return Err(fail(codes::INVALID_VALUE, "line wait is too long"));
        }
        let mut changes = Changes::new(self.queue, self.collection()?, now_ms);
        self.wait_in_line(&mut changes, scope, owner, room, wait_ms, now_ms)?;
        Ok(((), changes.finish()?))
    }

    /// Settle a claim, then claim the claimer's next items over the
    /// state the report leaves, as one set of writes (v1's chained
    /// claims: a worker's report takes its next work in the same
    /// commit). A refused report claims nothing. The claim sees the
    /// report: a group the report finished passes its turn on, and a
    /// job failed with no delay may be claimed again at once.
    pub(crate) fn report_then_claim(
        &self,
        id: &str,
        token: &str,
        report: Report<'_>,
        request: &ClaimRequest,
        now_ms: u64,
        tokens: &[String],
    ) -> Plan<Vec<ClaimedItem>> {
        request.validate()?;
        let collection = self.collection()?;
        let ((), reported) = match report {
            Report::Complete(result) => self.complete(id, token, result, now_ms)?,
            Report::Fail(delay_ms) => self.fail(id, token, now_ms, delay_ms)?,
            Report::Kill => self.kill(id, token, now_ms)?,
        };
        // Stage the report's writes; later writes to a key replace
        // earlier ones, so each row is written once.
        let mut staged = Staged::default();
        for write in reported {
            staged.stage_record(write.collection, write.key, write.value);
        }
        let (claimed, writes) = {
            let overlay = Overlay {
                read: self.read,
                staged: &staged,
            };
            let after = QueueView {
                read: &overlay,
                db: self.db,
                collection: self.collection,
                queue: self.queue,
                incarnation: self.incarnation,
                lineage: self.lineage.clone(),
                config: self.config.clone(),
            };
            after.claim(request, now_ms, tokens)?
        };
        for write in writes {
            staged.stage_record(write.collection, write.key, write.value);
        }
        let writes = staged
            .staged_records_for(collection)
            .into_iter()
            .map(|(key, value)| RecordWrite {
                collection,
                key,
                value,
            })
            .collect();
        Ok((claimed, writes))
    }

    /// Renew a live claim, answering its new expiry.
    pub(crate) fn heartbeat(&self, id: &str, token: &str, lease_ms: u64, now_ms: u64) -> Plan<u64> {
        validate_lease(lease_ms)?;
        let mut changes = Changes::new(self.queue, self.collection()?, now_ms);
        let item = self.claimed(&changes, id, token, now_ms)?;
        if item.claimed_until_ms <= now_ms {
            return Err(fail(codes::INVALID_VALUE, "claim lapsed"));
        }
        let mut next = item.clone();
        next.claimed_until_ms = now_ms.saturating_add(lease_ms);
        let expires = next.claimed_until_ms;
        changes.put(&item, Some(next));
        Ok((expires, changes.finish()?))
    }

    /// Finish a claim: kept as completed with `result` (checked against
    /// the queue's result contracts), or deleted without one.
    pub(crate) fn complete(
        &self,
        id: &str,
        token: &str,
        result: Option<&Value>,
        now_ms: u64,
    ) -> Plan<()> {
        if let Some(result) = result {
            QueueConfig::check(&self.config.result, result, "result")?;
        }
        let mut changes = Changes::new(self.queue, self.collection()?, now_ms);
        let item = self.claimed(&changes, id, token, now_ms)?;
        let next = result.map(|result| {
            let mut next = item.clone();
            next.state = ItemState::Completed;
            next.result = Some(result.clone());
            next.token = None;
            next.claimed_until_ms = 0;
            next.updated_ms = now_ms;
            next
        });
        changes.put(&item, next);
        Ok(((), changes.finish()?))
    }

    /// Requeue after a delay, keeping its turn, or die past the attempt
    /// budget.
    pub(crate) fn fail(&self, id: &str, token: &str, now_ms: u64, delay_ms: u64) -> Plan<()> {
        validate_delay(delay_ms, "fail")?;
        let mut changes = Changes::new(self.queue, self.collection()?, now_ms);
        let item = self.claimed(&changes, id, token, now_ms)?;
        let mut next = unclaimed(&item, now_ms);
        if item.attempts >= item.max_attempts {
            next.state = ItemState::Dead;
        } else {
            next.state = ItemState::Pending;
            next.due_ms = now_ms.saturating_add(delay_ms);
        }
        changes.put(&item, Some(next));
        Ok(((), changes.finish()?))
    }

    pub(crate) fn kill(&self, id: &str, token: &str, now_ms: u64) -> Plan<()> {
        let mut changes = Changes::new(self.queue, self.collection()?, now_ms);
        let item = self.claimed(&changes, id, token, now_ms)?;
        let mut next = unclaimed(&item, now_ms);
        next.state = ItemState::Dead;
        changes.put(&item, Some(next));
        Ok(((), changes.finish()?))
    }

    /// Hand a claim back, keeping its turn and attempt count.
    pub(crate) fn release(&self, id: &str, token: &str, now_ms: u64, delay_ms: u64) -> Plan<()> {
        validate_delay(delay_ms, "release")?;
        let mut changes = Changes::new(self.queue, self.collection()?, now_ms);
        let item = self.claimed(&changes, id, token, now_ms)?;
        let mut next = unclaimed(&item, now_ms);
        next.state = ItemState::Pending;
        next.due_ms = now_ms.saturating_add(delay_ms);
        changes.put(&item, Some(next));
        Ok(((), changes.finish()?))
    }

    /// Delete an item in any state, reporting whether one existed.
    pub(crate) fn cancel(&self, job: &JobRef, now_ms: u64) -> Plan<bool> {
        let id = job.id()?;
        let Some(item) = self.find(&id)? else {
            return Ok((false, Vec::new()));
        };
        let mut changes = Changes::new(self.queue, self.collection()?, now_ms);
        changes.put(&item, None);
        Ok((true, changes.finish()?))
    }

    /// Requeue a dead item with a fresh attempt budget and a new turn,
    /// including a claim that lapsed on its last attempt (dead from its
    /// lease's end, see [`effective`]).
    pub(crate) fn retry(&self, job: &JobRef, now_ms: u64, delay_ms: u64) -> Plan<()> {
        validate_delay(delay_ms, "retry")?;
        let Some(item) = self.find(&job.id()?)? else {
            return Err(fail(codes::INVALID_VALUE, "unknown queue item"));
        };
        let dead = effective(item.clone(), now_ms);
        if dead.state != ItemState::Dead {
            return Err(fail(codes::INVALID_VALUE, "only dead items retry"));
        }
        let mut changes = Changes::new(self.queue, self.collection()?, now_ms);
        // From the dead item, while the stored one names the row to drop.
        let mut next = dead;
        next.turn = self.place(
            &mut changes,
            &scope_hash(&item.scope),
            item.priority,
            item.group.as_deref(),
        )?;
        next.state = ItemState::Pending;
        next.attempts = 0;
        next.token = None;
        next.owner = None;
        next.claimed_until_ms = 0;
        next.due_ms = now_ms.saturating_add(delay_ms);
        next.updated_ms = now_ms;
        changes.put(&item, Some(next));
        Ok(((), changes.finish()?))
    }

    /// Delete dead and completed items enqueued before `older_than_ms`,
    /// in every scope.
    pub(crate) fn purge(&self, older_than_ms: u64, now_ms: u64) -> Plan<usize> {
        let Some(collection) = self.collection else {
            return Ok((0, Vec::new()));
        };
        let mut changes = Changes::new(self.queue, collection, now_ms);
        let mut purged = 0;
        for (key_len, tag) in [
            (TIMED_KEY_LEN, TAG_DEAD),
            (TIMED_KEY_LEN, TAG_COMPLETED),
            (LEGACY_KEY_LEN, TAG_DEAD),
        ] {
            let mut visit = |key: Vec<u8>, bytes: Vec<u8>| -> Result<(), Error> {
                let item = decode_item(&key, &bytes)?;
                if item.enqueued_ms < older_than_ms {
                    changes.put(&item, None);
                    purged += 1;
                }
                Ok(())
            };
            for_each_row(
                self.read,
                self.db,
                collection,
                key_len,
                &queue_prefix(tag, self.queue),
                &mut visit,
            )?;
        }
        Ok((purged, changes.finish()?))
    }

    /// Count the items of one scope, or of all, as of `now_ms` (see
    /// [`effective`]).
    pub(crate) fn stats(&self, scope: Option<&str>, now_ms: u64) -> Result<QueueStats, Failure> {
        Ok(self.counts(scope, now_ms)?.0)
    }

    /// [`QueueView::stats`], and the next time the counts change with
    /// the clock alone: the first lease still running, whose lapse may
    /// kill its item.
    pub(crate) fn counts(
        &self,
        scope: Option<&str>,
        now_ms: u64,
    ) -> Result<(QueueStats, Option<u64>), Failure> {
        if let Some(scope) = scope {
            validate_scope(scope)?;
        }
        let mut stats = QueueStats::default();
        let mut next: Option<u64> = None;
        let Some(collection) = self.collection else {
            return Ok((stats, next));
        };
        let prefix = |tag: u8| match scope {
            Some(scope) => scope_prefix(tag, self.queue, &scope_hash(scope)),
            None => queue_prefix(tag, self.queue),
        };
        let legacy = scope.is_none_or(str::is_empty);
        let mut classes = vec![
            (READY_KEY_LEN, prefix(TAG_READY), ItemState::Pending),
            (TIMED_KEY_LEN, prefix(TAG_WAITING), ItemState::Pending),
            (TIMED_KEY_LEN, prefix(TAG_CLAIMED), ItemState::Claimed),
            (TIMED_KEY_LEN, prefix(TAG_DEAD), ItemState::Dead),
            (TIMED_KEY_LEN, prefix(TAG_COMPLETED), ItemState::Completed),
        ];
        if legacy {
            classes.push((
                LEGACY_KEY_LEN,
                queue_prefix(TAG_READY, self.queue),
                ItemState::Pending,
            ));
            classes.push((
                LEGACY_KEY_LEN,
                queue_prefix(TAG_CLAIMED, self.queue),
                ItemState::Claimed,
            ));
            classes.push((
                LEGACY_KEY_LEN,
                queue_prefix(TAG_DEAD, self.queue),
                ItemState::Dead,
            ));
        }
        for (key_len, prefix, state) in classes {
            if state == ItemState::Claimed {
                // Claimed rows key on their lease's end: those past it
                // on their last attempt are dead already.
                let mut visit = |key: Vec<u8>, bytes: Vec<u8>| -> Result<(), Error> {
                    let expires = time_of(&key);
                    if expires > now_ms {
                        next = Some(next.map_or(expires, |at: u64| at.min(expires)));
                        stats.claimed += 1;
                    } else if lapsed_dead(&decode_item(&key, &bytes)?, now_ms) {
                        stats.dead += 1;
                    } else {
                        stats.claimed += 1;
                    }
                    Ok(())
                };
                for_each_row(self.read, self.db, collection, key_len, &prefix, &mut visit)?;
                continue;
            }
            let count = match state {
                ItemState::Pending => &mut stats.pending,
                ItemState::Claimed => &mut stats.claimed,
                ItemState::Dead => &mut stats.dead,
                ItemState::Completed => &mut stats.completed,
            };
            let mut visit = |_: Vec<u8>, _: Vec<u8>| -> Result<(), Error> {
                *count += 1;
                Ok(())
            };
            for_each_row(self.read, self.db, collection, key_len, &prefix, &mut visit)?;
        }
        Ok((stats, next))
    }

    /// Whether a claim on one scope would take an item now — with an
    /// owner, only once it is that owner's turn in line — and when that
    /// next changes.
    pub(crate) fn readiness(
        &self,
        scope: &str,
        owner: Option<&str>,
        now_ms: u64,
    ) -> Result<Readiness, Failure> {
        validate_scope(scope)?;
        let (available, next) = self.availability(scope, now_ms)?;
        let Some(since) = available else {
            return Ok(Readiness {
                ready: false,
                next_ms: next,
            });
        };
        let Some(owner) = owner else {
            return Ok(Readiness {
                ready: true,
                next_ms: None,
            });
        };
        // Whoever waits first in line has new work to itself for the
        // queue's turn, so one process wakes per job.
        let (head, _) = self.head(&scope_hash(scope), now_ms)?;
        let Some(head) = head else {
            return Ok(Readiness {
                ready: true,
                next_ms: None,
            });
        };
        let turn_end = since.saturating_add(self.config.turn_ms);
        if head.owner == owner || now_ms >= turn_end {
            return Ok(Readiness {
                ready: true,
                next_ms: None,
            });
        }
        Ok(Readiness {
            ready: false,
            next_ms: Some(turn_end.min(head.expires)),
        })
    }
}

/// A claimed item once its lease lapsed, at the lease's end: pending
/// again, or dead on its last attempt, as v1 fails it.
fn lapsed(item: &QueueItem) -> QueueItem {
    let mut next = item.clone();
    next.state = if item.attempts >= item.max_attempts {
        ItemState::Dead
    } else {
        ItemState::Pending
    };
    next.due_ms = item.claimed_until_ms;
    next.claimed_until_ms = 0;
    next.token = None;
    next.owner = None;
    next.incarnation = None;
    next.updated_ms = item.claimed_until_ms;
    next
}

/// Whether a claim lapsed on its last attempt by `now_ms`.
fn lapsed_dead(item: &QueueItem, now_ms: u64) -> bool {
    item.state == ItemState::Claimed
        && item.claimed_until_ms <= now_ms
        && item.attempts >= item.max_attempts
}

/// An item as of `now_ms`, v1's `effective`: a claim that lapsed on its
/// last attempt is dead from its lease's end, whether or not a claim
/// has passed since to move it, so reads, retries, replaces and its
/// holder's reports all see it dead. A claim with attempts left reads
/// as claimed until it is claimed again: its holder may still report.
/// Only for reading and deciding: the stored item (whose `class` and
/// times name its row) is what a plan replaces.
pub(crate) fn effective(item: QueueItem, now_ms: u64) -> QueueItem {
    if lapsed_dead(&item, now_ms) {
        lapsed(&item)
    } else {
        item
    }
}

/// An item's claim settled: no token, owner, lease or history binding.
fn unclaimed(item: &QueueItem, now_ms: u64) -> QueueItem {
    let mut next = item.clone();
    next.token = None;
    next.owner = None;
    next.incarnation = None;
    next.claimed_until_ms = 0;
    next.updated_ms = now_ms;
    next
}

/// The time right after a timed key's prefix.
fn time_of(key: &[u8]) -> u64 {
    let start = key.len() - 24;
    let bytes: [u8; 8] = key[start..start + 8].try_into().unwrap_or([0; 8]);
    u64::from_be_bytes(bytes)
}

/// An owner's line pointer key.
fn owner_key(queue: &str, scope: &[u8; 16], owner: &str) -> Vec<u8> {
    let mut key = scope_prefix(TAG_OWNER, queue, scope);
    key.extend_from_slice(&hash16("flower.queue.owner", &[owner.as_bytes()]));
    key
}

/// Decode a line place.
fn decode_waiter(key: Vec<u8>, bytes: &[u8]) -> Result<Waiter, Error> {
    let Value::Object(place) = crate::json::parse_value(bytes)? else {
        return Err(Error::corrupt("line place is not an object"));
    };
    let number = |name: &str| match place.get(name) {
        Some(Value::Number(number)) if *number >= 0.0 => Ok(*number as u64),
        _ => Err(Error::corrupt("line place misses a number")),
    };
    let owner = match place.get("owner") {
        Some(Value::String(owner)) => owner.clone(),
        _ => return Err(Error::corrupt("line place misses its owner")),
    };
    Ok(Waiter {
        owner,
        room: number("room")?,
        since: number("since")?,
        expires: number("expires")?,
        key,
    })
}

/// Refuse an operation on `queue` while one of `purges` deletes it: its
/// rows are half gone, and a write would land behind or ahead of the
/// purge's walk.
fn purging(purges: &crate::purge::Purges, queue: &str) -> Result<(), Failure> {
    match crate::purge::queue_refusal(purges, queue) {
        Some(refusal) => Err(refusal),
        None => Ok(()),
    }
}

impl crate::engine::Engine {
    /// Plan a queue operation over committed state (or the writer's
    /// staged state, see [`Engine::on_staged`]). A queue a purge is
    /// deleting refuses every operation until the purge ends.
    ///
    /// [`Engine::on_staged`]: crate::engine::Engine::on_staged
    fn plan_queue<T>(
        &self,
        db: &[u8],
        queue: &str,
        plan: impl FnOnce(&QueueView<ReadTxn>) -> Plan<T>,
    ) -> Result<Planned<T>, Failure> {
        validate_queue(queue)?;
        let read = self.snapshot().map_err(Failure::from)?;
        purging(
            &crate::purge::purges(&read, db).map_err(Failure::from)?,
            queue,
        )?;
        let collection = read
            .intern_id(db, crate::host::QUEUE_COLLECTION)
            .map_err(Failure::from)?;
        let revision = read.meta_u64(db, meta::REVISION).map_err(Failure::from)?;
        let incarnation = read
            .meta_u64(db, meta::HISTORY_INCARNATION)
            .map_err(Failure::from)?;
        let view = QueueView {
            read: &read,
            db,
            collection,
            queue,
            incarnation,
            lineage: crate::placement::lineage(&read, db).map_err(Failure::from)?,
            config: self.queue_config(db, queue),
        };
        let (outcome, writes) = plan(&view)?;
        Ok(Planned {
            outcome,
            writes,
            revision,
        })
    }

    /// Plan a queue operation inside one write transaction and commit
    /// its writes there: the node-local direct path. `intern` creates
    /// the `$queue` collection on first use. A queue under purge
    /// refuses, as in [`Engine::plan_queue`].
    ///
    /// [`Engine::plan_queue`]: crate::engine::Engine::plan_queue
    fn commit_queue<T>(
        &self,
        db: &[u8],
        queue: &str,
        intern: bool,
        plan: impl FnOnce(&QueueView<WriteTxn>) -> Plan<T>,
    ) -> Result<T, Failure> {
        validate_queue(queue)?;
        let write = self.store().begin_write().map_err(Failure::from)?;
        purging(
            &crate::purge::purges_in(&write, db).map_err(Failure::from)?,
            queue,
        )?;
        let collection = if intern {
            write.intern(db, crate::host::QUEUE_COLLECTION).map(Some)
        } else {
            write.intern_id(db, crate::host::QUEUE_COLLECTION)
        }
        .map_err(Failure::from)?;
        let incarnation = write
            .meta_u64(db, meta::HISTORY_INCARNATION)
            .map_err(Failure::from)?;
        let view = QueueView {
            read: &write,
            db,
            collection,
            queue,
            incarnation,
            lineage: crate::placement::lineage(&write, db).map_err(Failure::from)?,
            config: self.queue_config(db, queue),
        };
        let (outcome, writes) = plan(&view)?;
        apply_records(&write, db, &writes).map_err(Failure::from)?;
        write.commit().map_err(Failure::from)?;
        Ok(outcome)
    }

    /// Plan a direct enqueue without committing: the new item's ID plus
    /// its writes. Queues must exist (deploy creates them). The caller
    /// chooses the random ID, so the plan is a pure function of its
    /// inputs.
    pub fn plan_enqueue(
        &self,
        db: &[u8],
        item: &NewItem,
        now_ms: u64,
        id: [u8; 16],
    ) -> Result<Planned<String>, Failure> {
        item.validate()?;
        self.plan_queue(db, item.queue, |view| view.enqueue(item, now_ms, &id))
    }

    /// Enqueue a payload in the default scope at priority 0, returning
    /// the item ID. Direct enqueues are node-local: only guest enqueues
    /// replicate.
    pub fn enqueue(
        &self,
        db: &[u8],
        queue: &str,
        payload: &Value,
        delay_ms: u64,
        max_attempts: u32,
        now_ms: u64,
    ) -> Result<String, Failure> {
        self.enqueue_item(
            db,
            &NewItem::new(queue, payload, delay_ms, max_attempts),
            now_ms,
        )
    }

    /// Enqueue an item with its scope, priority, group and name,
    /// returning its ID. Node-local, like [`Engine::enqueue`].
    ///
    /// [`Engine::enqueue`]: crate::engine::Engine::enqueue
    pub fn enqueue_item(&self, db: &[u8], item: &NewItem, now_ms: u64) -> Result<String, Failure> {
        item.validate()?;
        let id = random_128();
        self.commit_queue(db, item.queue, true, |view| view.enqueue(item, now_ms, &id))
    }

    /// Plan a poll of the default scope without committing; see
    /// [`Engine::plan_claim_queue`].
    ///
    /// [`Engine::plan_claim_queue`]: crate::engine::Engine::plan_claim_queue
    pub fn plan_poll_queue(
        &self,
        db: &[u8],
        queue: &str,
        limit: usize,
        lease_ms: u64,
        now_ms: u64,
        tokens: &[String],
    ) -> Result<Planned<Vec<ClaimedItem>>, Failure> {
        let request = ClaimRequest {
            scope: "",
            owner: None,
            limit,
            lease_ms,
            wait_ms: None,
        };
        self.plan_claim_queue(db, queue, &request, now_ms, tokens)
    }

    /// Plan a claim without committing: the first claimable items of
    /// one scope (lowest priority, then group turns, then the longest
    /// waiting), one caller-chosen token per claim, so at most
    /// `min(limit, tokens.len(), MAX_POLL_LIMIT)` items, the next fencing
    /// tokens, and the claimer's place in line.
    pub fn plan_claim_queue(
        &self,
        db: &[u8],
        queue: &str,
        request: &ClaimRequest,
        now_ms: u64,
        tokens: &[String],
    ) -> Result<Planned<Vec<ClaimedItem>>, Failure> {
        request.validate()?;
        self.plan_queue(db, queue, |view| view.claim(request, now_ms, tokens))
    }

    /// Claim due items of the default scope under leases; see
    /// [`Engine::claim_queue`].
    ///
    /// [`Engine::claim_queue`]: crate::engine::Engine::claim_queue
    pub fn poll_queue(
        &self,
        db: &[u8],
        queue: &str,
        now_ms: u64,
        limit: usize,
        lease_ms: u64,
    ) -> Result<Vec<ClaimedItem>, Failure> {
        let request = ClaimRequest {
            scope: "",
            owner: None,
            limit,
            lease_ms,
            wait_ms: None,
        };
        self.claim_queue(db, queue, &request, now_ms)
    }

    /// Claim the first claimable items of one scope under leases, in
    /// claim order. Planning and claiming share one write transaction,
    /// so concurrent claims never take the same item.
    pub fn claim_queue(
        &self,
        db: &[u8],
        queue: &str,
        request: &ClaimRequest,
        now_ms: u64,
    ) -> Result<Vec<ClaimedItem>, Failure> {
        request.validate()?;
        let tokens: Vec<String> = (0..request.limit.min(MAX_POLL_LIMIT))
            .map(|_| new_token())
            .collect();
        self.commit_queue(db, queue, false, |view| {
            view.claim(request, now_ms, &tokens)
        })
    }

    /// Plan a report plus the claimer's next claim over the state it
    /// leaves, in one patch, without committing (chained claims): one
    /// caller-chosen token per claim, as [`Engine::plan_claim_queue`].
    ///
    /// [`Engine::plan_claim_queue`]: crate::engine::Engine::plan_claim_queue
    #[allow(clippy::too_many_arguments)]
    pub fn plan_report_and_claim(
        &self,
        db: &[u8],
        queue: &str,
        id: &str,
        token: &str,
        report: Report<'_>,
        request: &ClaimRequest,
        now_ms: u64,
        tokens: &[String],
    ) -> Result<Planned<Vec<ClaimedItem>>, Failure> {
        request.validate()?;
        self.plan_queue(db, queue, |view| {
            view.report_then_claim(id, token, report, request, now_ms, tokens)
        })
    }

    /// Plan a lease renewal without committing.
    pub fn plan_heartbeat_queue(
        &self,
        db: &[u8],
        queue: &str,
        id: &str,
        token: &str,
        lease_ms: u64,
        now_ms: u64,
    ) -> Result<Planned<()>, Failure> {
        let planned = self.plan_queue(db, queue, |view| {
            view.heartbeat(id, token, lease_ms, now_ms)
        })?;
        Ok(Planned {
            outcome: (),
            writes: planned.writes,
            revision: planned.revision,
        })
    }

    /// Renew a claim's lease. Wrong tokens, lapsed claims and claims of
    /// another history fail.
    pub fn heartbeat_queue(
        &self,
        db: &[u8],
        queue: &str,
        id: &str,
        token: &str,
        lease_ms: u64,
        now_ms: u64,
    ) -> Result<(), Failure> {
        self.commit_queue(db, queue, false, |view| {
            view.heartbeat(id, token, lease_ms, now_ms)
        })
        .map(drop)
    }

    /// Plan a completion that deletes the item at `now_ms`, without
    /// committing.
    pub fn plan_complete_queue(
        &self,
        db: &[u8],
        queue: &str,
        id: &str,
        token: &str,
        now_ms: u64,
    ) -> Result<Planned<()>, Failure> {
        self.plan_queue(db, queue, |view| view.complete(id, token, None, now_ms))
    }

    /// Plan a completion without committing: kept as completed with
    /// `result` when given (checked against the queue's declared result
    /// contracts), deleted otherwise.
    pub fn plan_complete_queue_with(
        &self,
        db: &[u8],
        queue: &str,
        id: &str,
        token: &str,
        result: Option<&Value>,
        now_ms: u64,
    ) -> Result<Planned<()>, Failure> {
        self.plan_queue(db, queue, |view| view.complete(id, token, result, now_ms))
    }

    /// Complete a claim at `now_ms`, deleting the item. Wrong tokens and
    /// claims that lapsed on their last attempt fail.
    pub fn complete_queue(
        &self,
        db: &[u8],
        queue: &str,
        id: &str,
        token: &str,
        now_ms: u64,
    ) -> Result<(), Failure> {
        self.commit_queue(db, queue, false, |view| {
            view.complete(id, token, None, now_ms)
        })
    }

    /// Complete a claim and keep the item with its result, checked
    /// against the queue's declared result contracts.
    pub fn complete_queue_with(
        &self,
        db: &[u8],
        queue: &str,
        id: &str,
        token: &str,
        result: &Value,
        now_ms: u64,
    ) -> Result<(), Failure> {
        self.commit_queue(db, queue, false, |view| {
            view.complete(id, token, Some(result), now_ms)
        })
    }

    /// Plan a failed claim without committing.
    pub fn plan_fail_queue(
        &self,
        db: &[u8],
        queue: &str,
        id: &str,
        token: &str,
        now_ms: u64,
        delay_ms: u64,
    ) -> Result<Planned<()>, Failure> {
        self.plan_queue(db, queue, |view| view.fail(id, token, now_ms, delay_ms))
    }

    /// Fail a claim: requeue after a delay, or die past the maximum.
    /// Wrong tokens fail.
    pub fn fail_queue(
        &self,
        db: &[u8],
        queue: &str,
        id: &str,
        token: &str,
        now_ms: u64,
        delay_ms: u64,
    ) -> Result<(), Failure> {
        self.commit_queue(db, queue, false, |view| {
            view.fail(id, token, now_ms, delay_ms)
        })
    }

    /// Read one item, including dead and completed ones, by ID, as of
    /// this node's clock.
    pub fn get_queue_item(
        &self,
        db: &[u8],
        queue: &str,
        id: &str,
    ) -> Result<Option<QueueItem>, Failure> {
        self.find_queue_item(db, queue, &JobRef::Id(id))
    }

    /// Read one item by ID or by scope and name, as of this node's
    /// clock.
    pub fn find_queue_item(
        &self,
        db: &[u8],
        queue: &str,
        job: &JobRef,
    ) -> Result<Option<QueueItem>, Failure> {
        self.find_queue_item_at(db, queue, job, crate::engine::now_ms())
    }

    /// Read one item by ID or by scope and name as of `now_ms`: a claim
    /// that lapsed on its last attempt by then reads dead.
    pub fn find_queue_item_at(
        &self,
        db: &[u8],
        queue: &str,
        job: &JobRef,
        now_ms: u64,
    ) -> Result<Option<QueueItem>, Failure> {
        Ok(self
            .plan_queue(db, queue, |view| view.get(job, now_ms))?
            .outcome)
    }

    /// Plan a purge without committing.
    pub fn plan_purge_queue(
        &self,
        db: &[u8],
        queue: &str,
        older_than_ms: u64,
    ) -> Result<Planned<usize>, Failure> {
        self.plan_queue(db, queue, |view| view.purge(older_than_ms, older_than_ms))
    }

    /// Delete dead and completed items enqueued before `older_than_ms`,
    /// returning the count removed, so neither grows without bound.
    pub fn purge_queue(
        &self,
        db: &[u8],
        queue: &str,
        older_than_ms: u64,
    ) -> Result<usize, Failure> {
        self.commit_queue(db, queue, false, |view| {
            view.purge(older_than_ms, older_than_ms)
        })
    }

    /// Count live, claimed, dead and completed items, every scope, as of
    /// this node's clock.
    pub fn queue_stats(&self, db: &[u8], queue: &str) -> Result<QueueStats, Failure> {
        self.queue_stats_in(db, queue, None)
    }

    /// Count the items of one scope, or of all of them, as of this
    /// node's clock.
    pub fn queue_stats_in(
        &self,
        db: &[u8],
        queue: &str,
        scope: Option<&str>,
    ) -> Result<QueueStats, Failure> {
        self.queue_stats_at(db, queue, scope, crate::engine::now_ms())
    }

    /// Count the items of one scope, or of all of them, as of `now_ms`:
    /// claims that lapsed on their last attempt by then count as dead.
    pub fn queue_stats_at(
        &self,
        db: &[u8],
        queue: &str,
        scope: Option<&str>,
        now_ms: u64,
    ) -> Result<QueueStats, Failure> {
        Ok(self
            .plan_queue(db, queue, |view| {
                Ok((view.stats(scope, now_ms)?, Vec::new()))
            })?
            .outcome)
    }

    /// Whether a claim on one scope (by `owner`, in turn in the line)
    /// would take an item at `now_ms`, and when that next changes.
    pub fn queue_readiness(
        &self,
        db: &[u8],
        queue: &str,
        scope: &str,
        owner: Option<&str>,
        now_ms: u64,
    ) -> Result<Readiness, Failure> {
        Ok(self
            .plan_queue(db, queue, |view| {
                Ok((view.readiness(scope, owner, now_ms)?, Vec::new()))
            })?
            .outcome)
    }

    /// Plan a kill at `now_ms` without committing.
    pub fn plan_kill_queue(
        &self,
        db: &[u8],
        queue: &str,
        id: &str,
        token: &str,
        now_ms: u64,
    ) -> Result<Planned<()>, Failure> {
        self.plan_queue(db, queue, |view| view.kill(id, token, now_ms))
    }

    /// Kill a claim into the dead letter file at `now_ms`, whatever
    /// attempts remain. Wrong tokens and claims that lapsed on their
    /// last attempt (dead already) fail.
    pub fn kill_queue(
        &self,
        db: &[u8],
        queue: &str,
        id: &str,
        token: &str,
        now_ms: u64,
    ) -> Result<(), Failure> {
        self.commit_queue(db, queue, false, |view| view.kill(id, token, now_ms))
    }

    /// Plan a cancellation without committing.
    pub fn plan_cancel_queue(
        &self,
        db: &[u8],
        queue: &str,
        id: &str,
    ) -> Result<Planned<bool>, Failure> {
        self.plan_queue(db, queue, |view| view.cancel(&JobRef::Id(id), 0))
    }

    /// Cancel an item in any state, returning whether one existed.
    /// Cancelled work simply disappears; an external action already
    /// running cannot be retracted.
    pub fn cancel_queue(&self, db: &[u8], queue: &str, id: &str) -> Result<bool, Failure> {
        self.commit_queue(db, queue, false, |view| view.cancel(&JobRef::Id(id), 0))
    }

    /// Plan a release without committing.
    pub fn plan_release_queue(
        &self,
        db: &[u8],
        queue: &str,
        id: &str,
        token: &str,
        now_ms: u64,
        delay_ms: u64,
    ) -> Result<Planned<()>, Failure> {
        self.plan_queue(db, queue, |view| view.release(id, token, now_ms, delay_ms))
    }

    /// Release a claim back to pending with an optional delay, without
    /// consuming an attempt. Wrong tokens fail.
    pub fn release_queue(
        &self,
        db: &[u8],
        queue: &str,
        id: &str,
        token: &str,
        now_ms: u64,
        delay_ms: u64,
    ) -> Result<(), Failure> {
        self.commit_queue(db, queue, false, |view| {
            view.release(id, token, now_ms, delay_ms)
        })
    }

    /// Plan a dead-letter retry without committing.
    pub fn plan_retry_queue(
        &self,
        db: &[u8],
        queue: &str,
        id: &str,
        now_ms: u64,
        delay_ms: u64,
    ) -> Result<Planned<()>, Failure> {
        self.plan_queue(db, queue, |view| {
            view.retry(&JobRef::Id(id), now_ms, delay_ms)
        })
    }

    /// Requeue a dead item as pending with a fresh attempt budget.
    /// Live items fail.
    pub fn retry_queue(
        &self,
        db: &[u8],
        queue: &str,
        id: &str,
        now_ms: u64,
        delay_ms: u64,
    ) -> Result<(), Failure> {
        self.commit_queue(db, queue, false, |view| {
            view.retry(&JobRef::Id(id), now_ms, delay_ms)
        })
    }
}

impl crate::engine::EngineReader {
    /// Count a queue's items in committed state, on the read side (the
    /// writer goes on applying meanwhile), as of this node's clock.
    pub fn queue_stats(&self, db: &[u8], queue: &str) -> Result<QueueStats, Failure> {
        self.queue_stats_in(db, queue, None)
    }

    /// [`EngineReader::queue_stats`] for one scope, or all of them.
    ///
    /// [`EngineReader::queue_stats`]: crate::engine::EngineReader::queue_stats
    pub fn queue_stats_in(
        &self,
        db: &[u8],
        queue: &str,
        scope: Option<&str>,
    ) -> Result<QueueStats, Failure> {
        self.queue_stats_at(db, queue, scope, crate::engine::now_ms())
    }

    /// [`EngineReader::queue_stats_in`] as of `now_ms`.
    ///
    /// [`EngineReader::queue_stats_in`]: crate::engine::EngineReader::queue_stats_in
    pub fn queue_stats_at(
        &self,
        db: &[u8],
        queue: &str,
        scope: Option<&str>,
        now_ms: u64,
    ) -> Result<QueueStats, Failure> {
        validate_queue(queue)?;
        let read = self.store().begin_read().map_err(Failure::from)?;
        purging(
            &crate::purge::purges(&read, db).map_err(Failure::from)?,
            queue,
        )?;
        let collection = read
            .intern_id(db, crate::host::QUEUE_COLLECTION)
            .map_err(Failure::from)?;
        QueueView {
            read: &read,
            db,
            collection,
            queue,
            incarnation: 0,
            lineage: Vec::new(),
            config: QueueConfig::default(),
        }
        .stats(scope, now_ms)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(state: ItemState, scope: &str, priority: i64, turn: u64, time: u64) -> QueueItem {
        QueueItem {
            id: crate::hex::encode(&[7u8; 16]),
            payload: Value::Null,
            state,
            attempts: 0,
            max_attempts: 1,
            claimed_until_ms: time,
            token: None,
            enqueued_ms: 0,
            due_ms: time,
            scope: scope.to_string(),
            priority,
            turn,
            group: None,
            name: None,
            fence: 0,
            owner: None,
            result: None,
            incarnation: None,
            updated_ms: time,
            class: None,
        }
    }

    #[test]
    fn ready_keys_order_by_scope_priority_turn_then_time() {
        let id = [7u8; 16];
        let key = |scope: &str, priority: i64, turn: u64, time: u64| {
            let item = item(ItemState::Pending, scope, priority, turn, time);
            state_key("a", &item, Class::Ready, &id)
        };
        let base = key("", 0, 0, 10);
        assert_eq!(base.len(), READY_KEY_LEN);
        assert!(key("", 0, 0, 9) < base);
        assert!(key("", 0, 1, 0) > key("", 0, 0, 99));
        assert!(key("", -1, 9, 99) < key("", 0, 0, 1));
        assert!(key("", MAX_PRIORITY, 0, 0) > key("", -MAX_PRIORITY, 9, u64::MAX));
        assert_eq!(
            &key("t", 0, 0, 0)[..SCOPE_END],
            &key("t", 9, 9, 9)[..SCOPE_END]
        );
        assert_ne!(
            &key("t", 0, 0, 0)[..SCOPE_END],
            &key("u", 0, 0, 0)[..SCOPE_END]
        );
        assert_eq!(decode_state_key(&base).unwrap(), (Class::Ready, id));
        assert!(decode_state_key(&base[1..]).is_err());
        let waiting = state_key(
            "a",
            &item(ItemState::Pending, "", 0, 0, 5),
            Class::Waiting,
            &id,
        );
        assert_eq!(decode_state_key(&waiting).unwrap().0, Class::Waiting);
        assert_eq!(time_of(&waiting), 5);
    }

    #[test]
    fn legacy_keys_and_pointers_still_decode() {
        let id = [9u8; 16];
        let legacy = state_key(
            "jobs",
            &item(ItemState::Claimed, "", 0, 0, 42),
            Class::LegacyClaimed,
            &id,
        );
        assert_eq!(legacy.len(), LEGACY_KEY_LEN);
        assert_eq!(
            decode_state_key(&legacy).unwrap(),
            (Class::LegacyClaimed, id)
        );
        assert_eq!(
            decode_pointer("jobs", &id, b"[\"claimed\",\"42\"]").unwrap(),
            legacy
        );
        assert!(decode_pointer("jobs", &id, b"[\"pending\",\"x\"]").is_err());
    }

    #[test]
    fn names_hash_per_scope_and_validate() {
        assert_eq!(named_id("a", "job"), named_id("a", "job"));
        assert_ne!(named_id("a", "job"), named_id("b", "job"));
        assert_ne!(named_id("a", "job"), named_id("", "ajob"));
        assert_eq!(named_id("", "x").len(), 32);
        assert!(validate_scope("").is_ok());
        assert!(validate_scope("tenant:7 / eu").is_ok());
        assert!(validate_scope(&"s".repeat(MAX_NAME + 1)).is_err());
        assert!(validate_name("", "group").is_err());
        assert!(validate_priority(MAX_PRIORITY).is_ok());
        assert!(validate_priority(-MAX_PRIORITY - 1).is_err());
    }
}
