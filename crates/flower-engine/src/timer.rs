//! Durable timers: not-before deadlines that fire module
//! mutations.
//!
//! Timers live as records under the `$timer` collection in two kinds
//! of rows. A state row holds the timer under a fixed-width key —
//! state tag, big-endian due time, then the ID zero-padded to
//! [`MAX_TIMER_ID`] bytes — so every row of one state shares a key
//! length and the length-prefixed record order is exactly due order,
//! then ID order. Pending and dead timers use separate tags, so dead
//! letters never sit in front of due work. An ID row (`i` plus the ID)
//! points at the timer's current state row, so lookups by ID are two
//! point reads instead of scans.
//!
//! A removed module's timers wait for it: one that comes due while its module
//! is removed is held (attempts untouched) under a key naming the module
//! before its due time and ID, out of the due order, so held timers never
//! crowd out due work and one module's are one key range. Once the module is
//! deployed again its held timers are due at once; a purge of it deletes them
//! with the rest of its data ([`crate::purge`]).
//!
//! Scheduling the same ID replaces the timer. Firing runs through
//! consensus like a mutation: the fire patch deletes the timer and
//! carries the handler's effects atomically, guarded by the prepare
//! revision so concurrent sweeps cannot double-fire. Handler failures
//! roll back, then a retry patch advances attempts and due time (or
//! retains the timer dead) under the same guard.
//!
//! Direct schedules, cancellations, retries and purges plan once, as
//! record writes over committed state (`plan_*` calls taking the clock
//! as an input); the direct calls commit their own plan. Guest
//! mutations may reschedule or cancel the same timers concurrently, so
//! proposed plans must carry their [`Planned::revision`] as the patch's
//! expected revision, like fire patches.

use flower_sdk::error::codes;
use flower_sdk::{Failure, Value, fail};

use crate::error::Error;
use crate::patch::{Planned, RecordWrite, apply_records};
use crate::storage::keys::{prefix_end, record_key_class, record_prefix};
use crate::storage::{ReadTxn, RecordRead, RowWrite, WriteTxn, meta};

/// Longest timer ID, in bytes. Mirrors the SDK.
pub const MAX_TIMER_ID: usize = 128;
/// Default attempts before a timer dies. Mirrors the SDK.
pub const DEFAULT_MAX_ATTEMPTS: u32 = 3;
/// Most attempts before a timer dies. Mirrors the SDK.
pub const MAX_ATTEMPTS: u32 = 100;
/// Retry backoff base: 1 second in milliseconds.
pub const RETRY_BASE_MS: u64 = 1_000;
/// Retry backoff cap: 60 seconds in milliseconds.
pub const RETRY_MAX_MS: u64 = 60_000;
/// Most timers one scan returns.
pub const MAX_SCAN_LIMIT: usize = 100;
/// Rows one maintenance page (stats, purge) reads at a time.
const PAGE: usize = 1024;

/// State-row tag: pending timers, ordered by due time.
const TAG_PENDING: u8 = b'p';
/// State-row tag: dead timers, ordered by their last due time.
const TAG_DEAD: u8 = b'x';
/// State-row tag: a removed module's timers, by module, then due time.
const TAG_HELD: u8 = b'h';
/// ID-row tag: the pointer from an ID to its state row.
const TAG_ID: u8 = b'i';
/// Pending and dead state-row key length: tag, due time, padded ID. A
/// held row's key is longer by its module and a zero byte; an ID row's
/// is shorter.
const STATE_KEY_LEN: usize = 1 + 8 + MAX_TIMER_ID;

/// A timer's lifecycle state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimerState {
    /// Awaiting its due time.
    Pending,
    /// Exhausted its attempts.
    Dead,
    /// Came due while its module was removed: waits, attempts
    /// untouched, for the module to be deployed again or purged.
    Held,
}

impl TimerState {
    /// The state's name, as timers render it.
    pub fn name(&self) -> &'static str {
        match self {
            TimerState::Pending => "pending",
            TimerState::Dead => "dead",
            TimerState::Held => "held",
        }
    }

    fn parse(name: &str) -> Result<Self, Error> {
        match name {
            "pending" => Ok(TimerState::Pending),
            "dead" => Ok(TimerState::Dead),
            "held" => Ok(TimerState::Held),
            _ => Err(Error::corrupt("timer has a bad state")),
        }
    }

    fn tag(self) -> u8 {
        match self {
            TimerState::Pending => TAG_PENDING,
            TimerState::Dead => TAG_DEAD,
            TimerState::Held => TAG_HELD,
        }
    }
}

/// A stored timer.
#[derive(Clone, Debug)]
pub struct Timer {
    /// Stable timer ID.
    pub id: String,
    /// Owning module.
    pub module: String,
    /// Handler definition name.
    pub handler: String,
    /// Scheduled arguments.
    pub args: Value,
    /// Lifecycle state.
    pub state: TimerState,
    /// Fires so far.
    pub attempts: u32,
    /// Attempts before death.
    pub max_attempts: u32,
    /// Schedule time in milliseconds.
    pub enqueued_ms: u64,
    /// Due time in milliseconds.
    pub due_ms: u64,
}

/// Live counts for one database's timers.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TimerStats {
    /// Timers awaiting their due time.
    pub pending: usize,
    /// Exhausted timers awaiting purge or retry.
    pub dead: usize,
    /// Timers of removed modules, waiting for them.
    pub held: usize,
}

/// Validate schedule parameters on both sides of the ABI.
pub(crate) fn validate_schedule(id: &str, handler: &str, max_attempts: u32) -> Result<(), Failure> {
    validate_id(id)?;
    if handler.is_empty() || handler.len() > MAX_TIMER_ID {
        return Err(fail(codes::INVALID_VALUE, "handler name has a bad length"));
    }
    if max_attempts == 0 || max_attempts > MAX_ATTEMPTS {
        return Err(fail(codes::INVALID_VALUE, "max attempts out of range"));
    }
    Ok(())
}

/// Validate a timer ID on both sides of the ABI. IDs never contain a
/// zero byte, so zero padding keeps key order equal to ID order.
pub(crate) fn validate_id(id: &str) -> Result<(), Failure> {
    if id.is_empty() || id.len() > MAX_TIMER_ID {
        return Err(fail(codes::INVALID_VALUE, "timer ID has a bad length"));
    }
    // No control characters: NUL in particular, which pads IDs in
    // state keys.
    if id.chars().any(char::is_control) {
        return Err(fail(
            codes::INVALID_VALUE,
            "timer IDs hold no control characters",
        ));
    }
    Ok(())
}

/// Retry delay after a failed fire: exponential in attempts, capped.
pub(crate) fn retry_delay_ms(attempts: u32) -> u64 {
    let shift = attempts.min(10);
    RETRY_BASE_MS.saturating_mul(1 << shift).min(RETRY_MAX_MS)
}

/// A state-row key: tag, big-endian due, ID zero-padded to the fixed
/// width. Within one state, byte order is due order, then ID order. A
/// held row names its module (and a zero byte, which module names
/// never hold) after the tag, so one module's held timers are one key
/// range in due order; other states ignore `module`.
pub(crate) fn state_key(state: TimerState, module: &str, due_ms: u64, id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(STATE_KEY_LEN + module.len() + 1);
    key.push(state.tag());
    if state == TimerState::Held {
        key.extend_from_slice(module.as_bytes());
        key.push(0);
    }
    let end = key.len() + STATE_KEY_LEN - 1;
    key.extend_from_slice(&due_ms.to_be_bytes());
    key.extend_from_slice(id.as_bytes());
    key.resize(end, 0);
    key
}

/// The state-row key a stored timer lives under.
fn row_key(timer: &Timer) -> Vec<u8> {
    state_key(timer.state, &timer.module, timer.due_ms, &timer.id)
}

/// An ID-row key.
pub(crate) fn id_key(id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + id.len());
    key.push(TAG_ID);
    key.extend_from_slice(id.as_bytes());
    key
}

/// A state-row key's parts: state, a held row's module, due time, ID.
type StateKey<'a> = (TimerState, Option<&'a str>, u64, &'a str);

/// Split a state-row key into state, a held row's module, due time and
/// ID.
fn decode_state_key(key: &[u8]) -> Result<StateKey<'_>, Error> {
    let (state, module, rest) = match key.first() {
        Some(&TAG_PENDING) if key.len() == STATE_KEY_LEN => (TimerState::Pending, None, &key[1..]),
        Some(&TAG_DEAD) if key.len() == STATE_KEY_LEN => (TimerState::Dead, None, &key[1..]),
        Some(&TAG_HELD) if key.len() > STATE_KEY_LEN => {
            let (named, rest) = key[1..].split_at(key.len() - STATE_KEY_LEN);
            let module = match named.split_last() {
                Some((0, module)) if !module.contains(&0) => std::str::from_utf8(module)
                    .map_err(|_| Error::corrupt("held timer key's module is not UTF-8"))?,
                _ => return Err(Error::corrupt("held timer key has a bad module")),
            };
            (TimerState::Held, Some(module), rest)
        }
        Some(&(TAG_PENDING | TAG_DEAD | TAG_HELD)) => {
            return Err(Error::corrupt("timer key has the wrong length"));
        }
        _ => return Err(Error::corrupt("timer key has a bad tag")),
    };
    let mut due = [0u8; 8];
    due.copy_from_slice(&rest[..8]);
    let padded = &rest[8..];
    let end = padded
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(padded.len());
    if padded[end..].iter().any(|byte| *byte != 0) {
        return Err(Error::corrupt("timer key has bad padding"));
    }
    let id = std::str::from_utf8(&padded[..end])
        .map_err(|_| Error::corrupt("timer key is not UTF-8"))?;
    Ok((state, module, u64::from_be_bytes(due), id))
}

/// The ID row's value: the state and due time locating the state row
/// (then a held row's module), as canonical JSON text (the due time as
/// a decimal string, exact beyond `f64` range).
fn encode_pointer(timer: &Timer) -> Result<Vec<u8>, Error> {
    let mut parts = vec![
        Value::String(timer.state.name().to_string()),
        Value::String(timer.due_ms.to_string()),
    ];
    if timer.state == TimerState::Held {
        parts.push(Value::String(timer.module.clone()));
    }
    crate::json::render(&Value::Array(parts))
}

/// Decode an ID row back into its state-row key.
fn decode_pointer(id: &str, bytes: &[u8]) -> Result<Vec<u8>, Error> {
    let Value::Array(items) = crate::json::parse_value(bytes)? else {
        return Err(Error::corrupt("timer pointer is not an array"));
    };
    let (state, due, module) = match items.as_slice() {
        [Value::String(state), Value::String(due)] => (TimerState::parse(state)?, due, None),
        [
            Value::String(state),
            Value::String(due),
            Value::String(module),
        ] => (TimerState::parse(state)?, due, Some(module.as_str())),
        _ => return Err(Error::corrupt("timer pointer has a bad shape")),
    };
    if (state == TimerState::Held) != module.is_some() {
        return Err(Error::corrupt("timer pointer has a bad shape"));
    }
    let due = due
        .parse::<u64>()
        .map_err(|_| Error::corrupt("timer pointer has a bad due time"))?;
    Ok(state_key(state, module.unwrap_or_default(), due, id))
}

/// The record writes storing a timer: its state row plus the ID row.
pub(crate) fn timer_rows(timer: &Timer) -> Result<Vec<RowWrite>, Error> {
    Ok(vec![
        (row_key(timer), Some(encode_timer(timer)?)),
        (id_key(&timer.id), Some(encode_pointer(timer)?)),
    ])
}

/// The record writes removing a stored timer entirely.
pub(crate) fn removal(timer: &Timer) -> Vec<RowWrite> {
    vec![(row_key(timer), None), (id_key(&timer.id), None)]
}

/// Find a timer by ID through its ID row, reading records through
/// `get` (committed, staged or both).
pub(crate) fn find<E: From<Error>>(
    mut get: impl FnMut(&[u8]) -> Result<Option<Vec<u8>>, E>,
    id: &str,
) -> Result<Option<Timer>, E> {
    let Some(pointer) = get(&id_key(id))? else {
        return Ok(None);
    };
    let key = decode_pointer(id, &pointer)?;
    let bytes = get(&key)?.ok_or_else(|| Error::corrupt("timer pointer dangles"))?;
    Ok(Some(decode_timer(&key, &bytes)?))
}

/// Whether a record key is a state row of `state`.
pub(crate) fn in_class(key: &[u8], state: TimerState) -> bool {
    match state {
        TimerState::Held => key.len() > STATE_KEY_LEN && key[0] == TAG_HELD,
        _ => key.len() == STATE_KEY_LEN && key[0] == state.tag(),
    }
}

/// Full-key scan bounds of one state's rows, optionally only those due
/// at or before `due_until`: inclusive lower, exclusive upper. Held
/// rows go by module, then due time, and are longer than every other
/// row: their bounds are every key from the shortest held row's length
/// on, whatever `due_until`.
pub(crate) fn class_bounds(
    db: &[u8],
    collection: u32,
    state: TimerState,
    due_until: Option<u64>,
) -> Result<(Vec<u8>, Vec<u8>), Error> {
    if state == TimerState::Held {
        let lower = record_key_class(db, collection, STATE_KEY_LEN + 1, &[])?;
        // Past every key of the collection: none is 4 GiB long.
        let mut upper = record_prefix(db, collection)?;
        upper.extend_from_slice(&[0xFF; 5]);
        return Ok((lower, upper));
    }
    let lower = record_key_class(db, collection, STATE_KEY_LEN, &[state.tag()])?;
    let upper = match due_until.and_then(|due| due.checked_add(1)) {
        Some(end) => {
            let mut prefix = vec![state.tag()];
            prefix.extend_from_slice(&end.to_be_bytes());
            record_key_class(db, collection, STATE_KEY_LEN, &prefix)?
        }
        None => record_key_class(db, collection, STATE_KEY_LEN, &[state.tag() + 1])?,
    };
    Ok((lower, upper))
}

/// Full-key scan bounds of `module`'s held rows, in due order.
fn held_bounds(db: &[u8], collection: u32, module: &str) -> Result<(Vec<u8>, Vec<u8>), Error> {
    let mut prefix = vec![TAG_HELD];
    prefix.extend_from_slice(module.as_bytes());
    prefix.push(0);
    let lower = record_key_class(db, collection, STATE_KEY_LEN + prefix.len() - 1, &prefix)?;
    let upper = prefix_end(&lower).ok_or_else(|| Error::corrupt("held timers have no bound"))?;
    Ok((lower, upper))
}

/// One state's committed timers in due order (held ones by module
/// first), at most `limit`.
fn scan_class(
    read: &impl RecordRead,
    db: &[u8],
    collection: u32,
    state: TimerState,
    due_until: Option<u64>,
    limit: usize,
) -> Result<Vec<Timer>, Error> {
    let (lower, upper) = class_bounds(db, collection, state, due_until)?;
    read.records(db, collection, Some(&lower), Some(&upper), Some(limit))?
        .into_iter()
        .map(|(key, bytes)| decode_timer(&key, &bytes))
        .collect()
}

/// Visit every committed timer of one state in pages, without holding
/// the whole state in memory.
fn for_each_in_class(
    read: &impl RecordRead,
    db: &[u8],
    collection: u32,
    state: TimerState,
    mut visit: impl FnMut(Timer) -> Result<(), Error>,
) -> Result<(), Error> {
    let (mut lower, upper) = class_bounds(db, collection, state, None)?;
    loop {
        let page = read.records(db, collection, Some(&lower), Some(&upper), Some(PAGE))?;
        let full = page.len() == PAGE;
        let mut last = None;
        for (key, bytes) in page {
            visit(decode_timer(&key, &bytes)?)?;
            last = Some(key);
        }
        match last {
            Some(key) if full => {
                // Resume just past the last key: the next fixed-width key.
                let mut next = crate::storage::keys::encode_record_key(db, collection, &key)?;
                next.push(0);
                lower = next;
            }
            _ => return Ok(()),
        }
    }
}

/// Encode a timer value as canonical JSON bytes.
pub(crate) fn encode_timer(timer: &Timer) -> Result<Vec<u8>, Error> {
    let mut fields = serde_json::Map::new();
    fields.insert("v".to_string(), serde_json::Value::from(1));
    fields.insert("id".to_string(), serde_json::Value::from(timer.id.clone()));
    fields.insert(
        "module".to_string(),
        serde_json::Value::from(timer.module.clone()),
    );
    fields.insert(
        "handler".to_string(),
        serde_json::Value::from(timer.handler.clone()),
    );
    fields.insert(
        "args".to_string(),
        serde_json::from_slice(&crate::json::render(&timer.args)?)
            .map_err(|_| Error::corrupt("timer args"))?,
    );
    fields.insert(
        "state".to_string(),
        serde_json::Value::from(timer.state.name()),
    );
    fields.insert(
        "attempts".to_string(),
        serde_json::Value::from(timer.attempts),
    );
    fields.insert(
        "max_attempts".to_string(),
        serde_json::Value::from(timer.max_attempts),
    );
    fields.insert(
        "enqueued_ms".to_string(),
        serde_json::Value::from(timer.enqueued_ms),
    );
    let bytes = serde_json::to_vec(&serde_json::Value::Object(fields))
        .map_err(|_| Error::corrupt("timer is not JSON"))?;
    crate::json::canonicalize(&bytes)
}

/// Decode a timer value plus its state-row key's state and due time.
pub(crate) fn decode_timer(key: &[u8], bytes: &[u8]) -> Result<Timer, Error> {
    let (state, key_module, due_ms, key_id) = decode_state_key(key)?;
    let value: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|_| Error::corrupt("timer is not JSON"))?;
    let get = |name: &str| {
        value
            .get(name)
            .ok_or_else(|| Error::corrupt("timer misses a field"))
    };
    let text = |name: &str| {
        get(name)?
            .as_str()
            .map(str::to_string)
            .ok_or_else(|| Error::corrupt("timer field"))
    };
    // Counters round-trip through canonical JSON, which renders
    // numbers as f64: large millisecond times come back as floats.
    // All live values sit far below 2^53, so the cast is exact.
    let number = |name: &str| {
        let field = get(name)?;
        if let Some(int) = field.as_u64() {
            return Ok(int);
        }
        match field.as_f64() {
            Some(float) if float.is_finite() && float.fract() == 0.0 && float >= 0.0 => {
                Ok(float as u64)
            }
            _ => Err(Error::corrupt("timer counter")),
        }
    };
    let counter = |name: &str| {
        u32::try_from(number(name)?).map_err(|_| Error::corrupt("timer counter exceeds u32"))
    };
    let args = get("args")?;
    let args = crate::json::parse_value(
        &serde_json::to_vec(args).map_err(|_| Error::corrupt("timer args"))?,
    )?;
    let id = text("id")?;
    let module = text("module")?;
    let stored = TimerState::parse(
        get("state")?
            .as_str()
            .ok_or_else(|| Error::corrupt("timer state"))?,
    )?;
    if id != key_id || stored != state || key_module.is_some_and(|held| held != module) {
        return Err(Error::corrupt("timer disagrees with its key"));
    }
    Ok(Timer {
        id,
        module,
        handler: text("handler")?,
        args,
        state,
        attempts: counter("attempts")?,
        max_attempts: counter("max_attempts")?,
        enqueued_ms: number("enqueued_ms")?,
        due_ms,
    })
}

/// Render a timer for guest inspection.
pub(crate) fn timer_value(timer: &Timer) -> Value {
    let mut map = std::collections::BTreeMap::new();
    map.insert("id".to_string(), Value::String(timer.id.clone()));
    map.insert("handler".to_string(), Value::String(timer.handler.clone()));
    map.insert("args".to_string(), timer.args.clone());
    map.insert("due_ms".to_string(), Value::Number(timer.due_ms as f64));
    map.insert("attempts".to_string(), Value::Number(timer.attempts as f64));
    map.insert(
        "max_attempts".to_string(),
        Value::Number(timer.max_attempts as f64),
    );
    map.insert(
        "state".to_string(),
        Value::String(timer.state.name().to_string()),
    );
    Value::Object(map)
}

/// A direct timer schedule: everything but the clock.
#[derive(Clone, Copy, Debug)]
pub struct TimerSchedule<'a> {
    /// Stable timer ID; scheduling it again replaces the timer.
    pub id: &'a str,
    /// Owning module.
    pub module: &'a str,
    /// Handler mutation name.
    pub handler: &'a str,
    /// Handler arguments.
    pub args: &'a Value,
    /// Due time in milliseconds.
    pub due_ms: u64,
    /// Attempts before the timer dies.
    pub max_attempts: u32,
}

/// An operation's answer plus the record writes carrying it out.
type Plan<T> = Result<(T, Vec<RecordWrite>), Failure>;

/// One database's timers as of one read: committed state for plans, a
/// write transaction's own view for direct commits. Every direct timer
/// operation is planned here once, as record writes.
struct TimerView<'a, R: RecordRead> {
    read: &'a R,
    db: &'a [u8],
    /// The `$timer` collection, or `None` before timers exist.
    collection: Option<u32>,
}

impl<R: RecordRead> TimerView<'_, R> {
    /// Find a timer in any state by ID through its ID row.
    fn find(&self, id: &str) -> Result<Option<Timer>, Error> {
        match self.collection {
            None => Ok(None),
            Some(collection) => find(|key| self.read.record(self.db, collection, key), id),
        }
    }

    /// Record writes for keyed rows of the timer collection.
    fn writes(&self, rows: Vec<RowWrite>) -> Result<Vec<RecordWrite>, Failure> {
        let collection = self
            .collection
            .ok_or_else(|| fail(codes::INVALID_VALUE, "timers are not initialized"))?;
        Ok(rows
            .into_iter()
            .map(|(key, value)| RecordWrite {
                collection,
                key,
                value,
            })
            .collect())
    }

    /// Store a timer, replacing any same-ID timer's state row.
    fn schedule(&self, schedule: &TimerSchedule, now_ms: u64) -> Plan<()> {
        let mut rows = match self.find(schedule.id)? {
            Some(old) => removal(&old),
            None => Vec::new(),
        };
        rows.extend(timer_rows(&Timer {
            id: schedule.id.to_string(),
            module: schedule.module.to_string(),
            handler: schedule.handler.to_string(),
            args: schedule.args.clone(),
            state: TimerState::Pending,
            attempts: 0,
            max_attempts: schedule.max_attempts,
            enqueued_ms: now_ms,
            due_ms: schedule.due_ms,
        })?);
        Ok(((), self.writes(rows)?))
    }

    fn cancel(&self, id: &str) -> Plan<bool> {
        match self.find(id)? {
            None => Ok((false, Vec::new())),
            Some(timer) => Ok((true, self.writes(removal(&timer))?)),
        }
    }

    /// Requeue a dead timer as pending with a fresh attempt budget.
    fn retry(&self, id: &str, due_ms: u64) -> Plan<()> {
        let Some(mut timer) = self.find(id)? else {
            return Err(fail(codes::INVALID_VALUE, "unknown timer"));
        };
        if timer.state != TimerState::Dead {
            return Err(fail(codes::INVALID_VALUE, "only dead timers retry"));
        }
        let mut rows = removal(&timer);
        timer.state = TimerState::Pending;
        timer.attempts = 0;
        timer.due_ms = due_ms;
        rows.extend(timer_rows(&timer)?);
        Ok(((), self.writes(rows)?))
    }

    /// Delete dead timers enqueued before `older_than_ms`.
    fn purge(&self, older_than_ms: u64) -> Plan<usize> {
        let Some(collection) = self.collection else {
            return Ok((0, Vec::new()));
        };
        let mut rows = Vec::new();
        let mut purged = 0;
        for_each_in_class(self.read, self.db, collection, TimerState::Dead, |timer| {
            if timer.enqueued_ms < older_than_ms {
                rows.extend(removal(&timer));
                purged += 1;
            }
            Ok(())
        })?;
        Ok((purged, self.writes(rows)?))
    }
}

impl crate::engine::Engine {
    /// Plan a timer operation over committed state (or the writer's
    /// staged state, see [`Engine::on_staged`]).
    fn plan_timers<T>(
        &self,
        db: &[u8],
        plan: impl FnOnce(&TimerView<ReadTxn>) -> Plan<T>,
    ) -> Result<Planned<T>, Failure> {
        let read = self.snapshot().map_err(Failure::from)?;
        let collection = read
            .intern_id(db, crate::host::TIMER_COLLECTION)
            .map_err(Failure::from)?;
        let revision = read.meta_u64(db, meta::REVISION).map_err(Failure::from)?;
        let (outcome, writes) = plan(&TimerView {
            read: &read,
            db,
            collection,
        })?;
        Ok(Planned {
            outcome,
            writes,
            revision,
        })
    }

    /// Plan a timer operation inside one write transaction and commit
    /// its writes there: the node-local direct path. `intern` creates
    /// the `$timer` collection on first use.
    fn commit_timers<T>(
        &self,
        db: &[u8],
        intern: bool,
        plan: impl FnOnce(&TimerView<WriteTxn>) -> Plan<T>,
    ) -> Result<T, Failure> {
        let write = self.store().begin_write().map_err(Failure::from)?;
        let collection = if intern {
            write.intern(db, crate::host::TIMER_COLLECTION).map(Some)
        } else {
            write.intern_id(db, crate::host::TIMER_COLLECTION)
        }
        .map_err(Failure::from)?;
        let (outcome, writes) = plan(&TimerView {
            read: &write,
            db,
            collection,
        })?;
        apply_records(&write, db, &writes).map_err(Failure::from)?;
        write.commit().map_err(Failure::from)?;
        Ok(outcome)
    }

    /// Plan a direct schedule without committing, replacing any same-ID
    /// timer. Timers must exist (deploy creates them).
    pub fn plan_schedule_timer(
        &self,
        db: &[u8],
        schedule: &TimerSchedule,
        now_ms: u64,
    ) -> Result<Planned<()>, Failure> {
        validate_schedule(schedule.id, schedule.handler, schedule.max_attempts)?;
        // Payloads must render: non-finite numbers are caller errors.
        crate::json::render(schedule.args)
            .map_err(|_| fail(codes::INVALID_VALUE, "args are not storable"))?;
        self.plan_timers(db, |view| view.schedule(schedule, now_ms))
    }

    /// Schedule a timer, replacing any same-ID timer. Direct
    /// schedules are node-local: only guest schedules replicate. The
    /// planned form is [`Engine::plan_schedule_timer`].
    #[allow(clippy::too_many_arguments)]
    pub fn schedule_timer(
        &self,
        db: &[u8],
        id: &str,
        module: &str,
        handler: &str,
        args: &Value,
        due_ms: u64,
        max_attempts: u32,
        now_ms: u64,
    ) -> Result<(), Failure> {
        let schedule = TimerSchedule {
            id,
            module,
            handler,
            args,
            due_ms,
            max_attempts,
        };
        validate_schedule(id, handler, max_attempts)?;
        crate::json::render(args)
            .map_err(|_| fail(codes::INVALID_VALUE, "args are not storable"))?;
        self.commit_timers(db, true, |view| view.schedule(&schedule, now_ms))
    }

    /// Plan a cancellation without committing.
    pub fn plan_cancel_timer(&self, db: &[u8], id: &str) -> Result<Planned<bool>, Failure> {
        validate_id(id)?;
        self.plan_timers(db, |view| view.cancel(id))
    }

    /// Cancel a timer, returning whether one existed.
    pub fn cancel_timer(&self, db: &[u8], id: &str) -> Result<bool, Failure> {
        validate_id(id)?;
        self.commit_timers(db, false, |view| view.cancel(id))
    }

    /// Read one timer, including dead ones.
    pub fn get_timer(&self, db: &[u8], id: &str) -> Result<Option<Timer>, Failure> {
        validate_id(id)?;
        let planned = self.plan_timers(db, |view| Ok((view.find(id)?, Vec::new())))?;
        Ok(planned.outcome)
    }

    /// Scan timers due-first, optionally filtered by state. Without a
    /// state filter, pending, dead and held timers merge in due order.
    /// Held timers are read by module first: past `limit` of them, a
    /// listing may leave out earlier ones of another module.
    pub fn scan_timers(
        &self,
        db: &[u8],
        state: Option<TimerState>,
        limit: usize,
    ) -> Result<Vec<Timer>, Failure> {
        let read = self.store().begin_read().map_err(Failure::from)?;
        let Some(collection) = read
            .intern_id(db, crate::host::TIMER_COLLECTION)
            .map_err(Failure::from)?
        else {
            return Ok(Vec::new());
        };
        let states = match state {
            Some(state) => vec![state],
            None => vec![TimerState::Pending, TimerState::Dead, TimerState::Held],
        };
        let mut out = Vec::new();
        for state in states {
            out.extend(scan_class(&read, db, collection, state, None, limit)?);
        }
        out.sort_by(|left, right| (left.due_ms, &left.id).cmp(&(right.due_ms, &right.id)));
        out.truncate(limit);
        Ok(out)
    }

    /// List due pending timers, due-first, for the sweep, then the
    /// held timers of modules deployed again, by module. Dead and held
    /// timers live under their own tags, so they never crowd out due
    /// work: the held rows cost one read per module holding some.
    pub fn list_due_timers(
        &self,
        db: &[u8],
        now_ms: u64,
        limit: usize,
    ) -> Result<Vec<Timer>, Failure> {
        let read = self.store().begin_read().map_err(Failure::from)?;
        let Some(collection) = read
            .intern_id(db, crate::host::TIMER_COLLECTION)
            .map_err(Failure::from)?
        else {
            return Ok(Vec::new());
        };
        let mut due = scan_class(
            &read,
            db,
            collection,
            TimerState::Pending,
            Some(now_ms),
            limit,
        )?;
        let (mut lower, upper) = class_bounds(db, collection, TimerState::Held, None)?;
        while due.len() < limit {
            let Some((key, _)) = read
                .records(db, collection, Some(&lower), Some(&upper), Some(1))
                .map_err(Failure::from)?
                .into_iter()
                .next()
            else {
                break;
            };
            let (_, module, _, _) = decode_state_key(&key)?;
            let module = module.unwrap_or_default();
            let (start, end) = held_bounds(db, collection, module)?;
            if self.is_deployed(db, module) {
                let left = limit - due.len();
                for (key, bytes) in read
                    .records(db, collection, Some(&start), Some(&end), Some(left))
                    .map_err(Failure::from)?
                {
                    due.push(decode_timer(&key, &bytes)?);
                }
            }
            lower = end;
        }
        Ok(due)
    }

    /// Plan a dead-timer retry without committing.
    pub fn plan_retry_timer(
        &self,
        db: &[u8],
        id: &str,
        now_ms: u64,
        delay_ms: u64,
    ) -> Result<Planned<()>, Failure> {
        validate_id(id)?;
        self.plan_timers(db, |view| view.retry(id, now_ms.saturating_add(delay_ms)))
    }

    /// Requeue a dead timer as pending with a fresh attempt budget.
    /// Live timers fail.
    pub fn retry_timer(
        &self,
        db: &[u8],
        id: &str,
        now_ms: u64,
        delay_ms: u64,
    ) -> Result<(), Failure> {
        validate_id(id)?;
        self.commit_timers(db, false, |view| {
            view.retry(id, now_ms.saturating_add(delay_ms))
        })
    }

    /// Plan a dead-timer purge without committing.
    pub fn plan_purge_timers(
        &self,
        db: &[u8],
        older_than_ms: u64,
    ) -> Result<Planned<usize>, Failure> {
        self.plan_timers(db, |view| view.purge(older_than_ms))
    }

    /// Delete dead timers enqueued before `older_than_ms`, returning
    /// the count removed.
    pub fn purge_timers(&self, db: &[u8], older_than_ms: u64) -> Result<usize, Failure> {
        self.commit_timers(db, false, |view| view.purge(older_than_ms))
    }

    /// Count pending, dead and held timers.
    pub fn timer_stats(&self, db: &[u8]) -> Result<TimerStats, Failure> {
        let planned = self.plan_timers(db, |view| Ok((count_timers(view)?, Vec::new())))?;
        Ok(planned.outcome)
    }
}

impl crate::engine::EngineReader {
    /// Count pending, dead and held timers in committed state, on the
    /// read side: the writer goes on applying meanwhile.
    pub fn timer_stats(&self, db: &[u8]) -> Result<TimerStats, Failure> {
        let read = self.store().begin_read().map_err(Failure::from)?;
        let collection = read
            .intern_id(db, crate::host::TIMER_COLLECTION)
            .map_err(Failure::from)?;
        count_timers(&TimerView {
            read: &read,
            db,
            collection,
        })
    }
}

/// Count a database's pending, dead and held timers in `view`.
fn count_timers(view: &TimerView<ReadTxn>) -> Result<TimerStats, Failure> {
    let mut stats = TimerStats::default();
    let Some(collection) = view.collection else {
        return Ok(stats);
    };
    for (state, count) in [
        (TimerState::Pending, &mut stats.pending),
        (TimerState::Dead, &mut stats.dead),
        (TimerState::Held, &mut stats.held),
    ] {
        for_each_in_class(view.read, view.db, collection, state, |_| {
            *count += 1;
            Ok(())
        })?;
    }
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_keys_order_by_due_then_id() {
        let early = state_key(TimerState::Pending, "app", 9, "zz");
        let late = state_key(TimerState::Pending, "app", 10, "a");
        let short = state_key(TimerState::Pending, "app", 10, "a.long.id");
        assert_eq!(early.len(), late.len());
        assert!(early < late);
        assert!(late < short);
        assert_eq!(
            decode_state_key(&short).unwrap(),
            (TimerState::Pending, None, 10, "a.long.id")
        );
        assert!(decode_state_key(&short[1..]).is_err());
        assert!(in_class(&short, TimerState::Pending));
        assert!(!in_class(&short, TimerState::Dead));
        assert!(!in_class(&short, TimerState::Held));
        assert!(!in_class(&id_key("a"), TimerState::Pending));
    }

    #[test]
    fn held_keys_name_their_module_before_due_order() {
        let early = state_key(TimerState::Held, "bulk", 9, "zz");
        let late = state_key(TimerState::Held, "bulk", 10, "a");
        assert_eq!(early.len(), STATE_KEY_LEN + 5);
        assert!(early < late);
        assert_eq!(
            decode_state_key(&late).unwrap(),
            (TimerState::Held, Some("bulk"), 10, "a")
        );
        // The host's are held too, still longer than any other row.
        let host = state_key(TimerState::Held, "", 1, "t");
        assert_eq!(host.len(), STATE_KEY_LEN + 1);
        assert_eq!(
            decode_state_key(&host).unwrap(),
            (TimerState::Held, Some(""), 1, "t")
        );
        assert!(in_class(&late, TimerState::Held));
        assert!(!in_class(&late, TimerState::Pending));
        // A module running into the due time, or none at all, is not a
        // held key.
        let mut unnamed = late.clone();
        unnamed[5] = b'x';
        assert!(decode_state_key(&unnamed).is_err());
        assert!(decode_state_key(&late[..STATE_KEY_LEN]).is_err());
    }

    #[test]
    fn pointers_round_trip_exact_due_times() {
        let mut timer = Timer {
            id: "t1".to_string(),
            module: "bulk".to_string(),
            handler: "fire".to_string(),
            args: Value::Null,
            state: TimerState::Dead,
            attempts: 0,
            max_attempts: 3,
            enqueued_ms: 1,
            due_ms: u64::MAX,
        };
        for state in [TimerState::Dead, TimerState::Held] {
            timer.state = state;
            let pointer = encode_pointer(&timer).unwrap();
            assert_eq!(decode_pointer("t1", &pointer).unwrap(), row_key(&timer));
        }
        assert!(decode_pointer("t1", b"[1]").is_err());
        assert!(decode_pointer("t1", br#"["held","5"]"#).is_err());
        assert!(decode_pointer("t1", br#"["dead","5","bulk"]"#).is_err());
    }

    #[test]
    fn counters_beyond_u32_are_corrupt() {
        let timer = Timer {
            id: "t1".to_string(),
            module: "app".to_string(),
            handler: "fire".to_string(),
            args: Value::Null,
            state: TimerState::Pending,
            attempts: 0,
            max_attempts: 3,
            enqueued_ms: 1,
            due_ms: 5,
        };
        let key = state_key(TimerState::Pending, "app", 5, "t1");
        let text = String::from_utf8(encode_timer(&timer).unwrap()).unwrap();
        let bumped = text.replace("\"attempts\":0", "\"attempts\":4294967296");
        assert!(decode_timer(&key, bumped.as_bytes()).is_err());
        assert_eq!(decode_timer(&key, text.as_bytes()).unwrap().attempts, 0);
    }
}
