//! Replicated retry retention: epochs, floors, sessions, collection.
//!
//! A scoped retry identity names the history incarnation and the retry
//! epoch it was minted in (see [`crate::retry`]). Each database keeps a
//! replicated retention state: the current epoch new identities are
//! minted in and the floor below which they are retired. Retirement is a
//! replicated transition ([`RetentionCommand::Advance`]), never a local
//! clock: an identity below the floor is refused with `epoch_retired`
//! whether or not its receipt is still stored, and one above the current
//! epoch with `epoch_not_admitted`. Prepares check this, and so does
//! every apply ([`refuse`]), on members, devices and restore replay, so
//! a request ordered after an advance never runs under the retired
//! identity.
//!
//! A leader with a [`Rotation`] advances the epoch after `epoch_ms` of
//! its own monotonic time in it, keeping the last `keep_epochs` (the
//! current one included): an identity stays retryable for at least
//! `(keep_epochs - 1) * epoch_ms`, and a leader change only delays. Once
//! the floor rises, [`RetentionCommand::Collect`] walks the receipts a
//! page per entry from a replicated cursor and deletes those the floor
//! retired: scoped ones below it or of another history, a caller's own
//! (unscoped) ones whose receipt recorded an epoch below it, and
//! acknowledged or closed session results. The leader paces collection
//! by a duty cycle. A walk sees only receipts without an expiry, which
//! every member holds alike (see [`receipt_cost`]).
//!
//! An identity's history is the database's: its incarnation and those
//! it carried in through moves since its last restore
//! ([`crate::placement::History`]). A leader gives a database nobody
//! configured retention for a default rotation
//! ([`RetentionCommand::Default`]), which never overrides anyone's.
//!
//! The state counts the bytes of what only retention removes: receipts
//! without an expiry (a caller's own keys too, once retention is
//! initialized, see [`caller_receipt`]) and session rows. Past
//! [`Retention::max_receipt_bytes`], a patch's new results are refused
//! at apply ([`BUDGET_EXCEEDED`]) and nothing kept is evicted.
//!
//! Retry sessions bound what has to be kept to the unacknowledged work
//! of a consumer. A session is opened under a caller-chosen 128-bit ID
//! by the principal that owns it, in the current epoch; its requests use
//! identities whose intent is the session and a sequence number
//! ([`session_intent`]). Acknowledging a contiguous prefix of sequences
//! deletes their results and moves a watermark in one replicated
//! transition, within the work its caller allows ([`Acknowledgement`]):
//! a sequence at or below it is `already_acknowledged` and never runs
//! again. A closed session is terminal. Session rows go once the floor
//! passes their epoch, which then refuses their identities.

use flower_sdk::error::codes;
use flower_sdk::{Failure, Value, fail};
use serde::{Deserialize, Serialize};

use crate::error::{Error, HostResult, Result};
use crate::placement::History;
use crate::retry::{HASH_LEN, Receipt, ScopedIdentity, UNSCOPED_PREFIX};
use crate::storage::{ReadTxn, Row, WriteTxn, meta};

/// Meta row holding a database's retention state.
pub(crate) const STATE_ROW: &[u8] = b"retention";
/// Meta-name prefix of a retry session's row: prefix plus its 16-byte ID.
const SESSION_ROW: &[u8] = b"retry_session\x00";
/// Intent prefix of a session request: prefix, session ID, sequence.
const SESSION_INTENT: &[u8] = b"$session\x00";
/// Shortest epoch a rotation takes.
pub const MIN_EPOCH_MS: u64 = 1_000;
/// The work one acknowledgement does unless its caller sets another limit:
/// sequences a strict acknowledgement moves past, results an abandon deletes.
pub const ACK_LIMIT: u64 = flower_protocol::ACK_LIMIT;
/// The largest limit an acknowledgement may set: one entry checks or
/// deletes at most this many results.
pub const MAX_ACK_LIMIT: u64 = 65_536;
/// Receipts one collection entry inspects by default.
pub const COLLECT_PAGE: u64 = 1_024;

/// Retention is not initialized for the database.
pub const NOT_INITIALIZED: &str = "retention_not_initialized";
/// Another advance moved the epoch first.
pub const EPOCH_MOVED: &str = "retention_epoch_moved";
/// An identity's epoch is past the current one.
pub const EPOCH_NOT_ADMITTED: &str = "epoch_not_admitted";
/// An identity's epoch is below the floor.
pub const EPOCH_RETIRED: &str = "epoch_retired";
/// An identity belongs to another history.
pub const WRONG_HISTORY: &str = "wrong_history";
/// A session request names no open session.
pub const SESSION_UNKNOWN: &str = "retry_session_unknown";
/// A session request disagrees with its session's epoch or history.
pub const SESSION_MISMATCH: &str = "retry_session_mismatch";
/// Another principal owns the session.
pub const SESSION_FORBIDDEN: &str = "retry_session_forbidden";
/// The session is closed.
pub const SESSION_CLOSED: &str = "retry_session_closed";
/// The sequence was acknowledged: it never runs again.
pub const ALREADY_ACKNOWLEDGED: &str = "already_acknowledged";
/// An acknowledgement skipped a sequence without a result.
pub const ACK_GAP: &str = "retry_ack_gap";
/// A strict acknowledgement moves past more sequences than its limit.
pub const ACK_BUDGET: &str = "retry_ack_budget";
/// New results would take retention past its byte budget: nothing ran.
/// Acknowledging, closing or collecting frees room; nothing kept is
/// ever evicted to make it.
pub const BUDGET_EXCEEDED: &str = "receipt_budget_exceeded";

/// A database's retention state.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Retention {
    /// The epoch new identities are minted in.
    pub current_epoch: u64,
    /// Identities below it are retired.
    pub min_epoch: u64,
    /// Automatic advancing, if any.
    pub rotation: Option<Rotation>,
    /// The last receipt identity collection inspected, `None` to start.
    pub cursor: Option<Vec<u8>>,
    /// Whether collection walked every receipt since anything last asked
    /// for a walk (a rising floor, a closed session, an abandon that
    /// left results behind).
    pub collected: bool,
    /// Whether another walk follows the one under way: something asked
    /// for one while it ran, and receipts it already passed may be
    /// collectable now. A walk is never restarted, so a stream of such
    /// requests cannot keep one from reaching the end.
    #[serde(default)]
    pub again: bool,
    /// Whether an operator set the epochs (`rotate` or `advance`), which
    /// the leader's default never overrides. A `budget` alone sets none,
    /// so a database an operator only bounded still takes the default
    /// rotation and keeps collecting. States from before this field were
    /// all set by `rotate`.
    #[serde(default = "set_by_rotate")]
    pub epochs_configured: bool,
    /// Logical bytes of the receipts retention keeps (see
    /// [`receipt_cost`]): identity plus stored receipt.
    #[serde(default)]
    pub receipt_bytes: u64,
    /// How many receipts `receipt_bytes` counts.
    #[serde(default)]
    pub receipt_count: u64,
    /// Logical bytes of session rows: row name plus stored row.
    #[serde(default)]
    pub session_bytes: u64,
    /// How many session rows `session_bytes` counts.
    #[serde(default)]
    pub session_count: u64,
    /// The most `receipt_bytes + session_bytes` may reach through new
    /// work, `None` for no bound. Lowering it below use evicts nothing:
    /// it refuses new results until acknowledgement, close or
    /// collection bring use back under it.
    #[serde(default)]
    pub max_receipt_bytes: Option<u64>,
    /// Whether the counters count what the database keeps. A state from
    /// before retention counted bytes has none; it is counted the first
    /// time anything uses it ([`Retention::load`]).
    #[serde(default)]
    pub counted: bool,
}

/// States written before [`Retention::epochs_configured`] existed were
/// all initialized by an operator's `rotate`.
fn set_by_rotate() -> bool {
    true
}

impl Retention {
    /// Retention at epoch zero over what `db` already keeps.
    fn initial(read: &impl ReceiptScan, db: &[u8]) -> Result<Self> {
        let mut state = Retention {
            current_epoch: 0,
            min_epoch: 0,
            rotation: None,
            cursor: None,
            collected: true,
            again: false,
            epochs_configured: false,
            receipt_bytes: 0,
            receipt_count: 0,
            session_bytes: 0,
            session_count: 0,
            max_receipt_bytes: None,
            counted: false,
        };
        state.recount(read, db)?;
        Ok(state)
    }

    /// A database's retention state to act on, counted: a state from
    /// before retention counted bytes is counted here, from what every
    /// member holds alike, so every applier reaches the same counters.
    pub fn load(read: &impl ReceiptScan, db: &[u8]) -> Result<Option<Self>> {
        let Some(mut state) = Self::read(read, db)? else {
            return Ok(None);
        };
        if !state.counted {
            state.recount(read, db)?;
        }
        Ok(Some(state))
    }

    /// Count what `db` keeps from scratch: every receipt retention
    /// removes and every session row.
    fn recount(&mut self, read: &impl ReceiptScan, db: &[u8]) -> Result<()> {
        let (mut bytes, mut count) = (0u64, 0u64);
        for (identity, receipt) in read.all_receipts(db)? {
            let cost = receipt_cost(&identity, &receipt)?;
            if cost > 0 {
                bytes = bytes.saturating_add(cost);
                count += 1;
            }
        }
        self.receipt_bytes = bytes;
        self.receipt_count = count;
        let (mut bytes, mut count) = (0u64, 0u64);
        for (name, row) in read.meta_prefix(db, SESSION_ROW)? {
            bytes = bytes.saturating_add((name.len() + row.len()) as u64);
            count += 1;
        }
        self.session_bytes = bytes;
        self.session_count = count;
        self.counted = true;
        Ok(())
    }

    /// Have every receipt walked again: start a walk, or have another
    /// follow the one under way.
    fn walk_again(&mut self) {
        if self.collected {
            self.cursor = None;
            self.collected = false;
        } else {
            self.again = true;
        }
    }

    /// The bytes the budget bounds.
    pub fn retained_bytes(&self) -> u64 {
        self.receipt_bytes.saturating_add(self.session_bytes)
    }

    /// Whether `retained` bytes are past the budget.
    fn over(&self, retained: u64) -> bool {
        self.max_receipt_bytes.is_some_and(|max| retained > max)
    }

    /// Count a receipt of `cost` bytes kept, or (`kept: false`) gone.
    fn account_receipt(&mut self, cost: u64, kept: bool) {
        if cost == 0 {
            return;
        }
        if kept {
            self.receipt_bytes = self.receipt_bytes.saturating_add(cost);
            self.receipt_count += 1;
        } else {
            self.receipt_bytes = self.receipt_bytes.saturating_sub(cost);
            self.receipt_count = self.receipt_count.saturating_sub(1);
        }
    }

    /// Count a session row going from `before` bytes (`None`: new) to
    /// `after` (`None`: dropped).
    fn account_session(&mut self, before: Option<u64>, after: Option<u64>) {
        if let Some(bytes) = before {
            self.session_bytes = self.session_bytes.saturating_sub(bytes);
            self.session_count = self.session_count.saturating_sub(1);
        }
        if let Some(bytes) = after {
            self.session_bytes = self.session_bytes.saturating_add(bytes);
            self.session_count += 1;
        }
    }

    /// A database's retention state, if initialized.
    pub fn read(read: &impl MetaRead, db: &[u8]) -> Result<Option<Self>> {
        read.meta_row(db, STATE_ROW)?
            .map(|bytes| {
                serde_json::from_slice(&bytes)
                    .map_err(|error| Error::corrupt(format!("retention state: {error}")))
            })
            .transpose()
    }

    fn store(&self, write: &WriteTxn, db: &[u8]) -> Result<()> {
        let bytes = serde_json::to_vec(self)
            .map_err(|error| Error::corrupt(format!("retention state: {error}")))?;
        write.set_meta(db, STATE_ROW, &bytes)
    }
}

/// Automatic advancing: a leader moves to the next epoch after
/// `epoch_ms` of its own time in the current one, keeping `keep_epochs`.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub struct Rotation {
    /// Leader time one epoch lasts, at least [`MIN_EPOCH_MS`].
    pub epoch_ms: u64,
    /// Epochs kept admissible, the current one included: at least 2.
    pub keep_epochs: u64,
}

impl Rotation {
    /// The floor once `current` is the current epoch.
    pub fn floor(&self, current: u64) -> u64 {
        current.saturating_sub(self.keep_epochs.saturating_sub(1))
    }
}

/// One retry session.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Session {
    /// The owning principal's receipt owner (see [`crate::auth`]).
    pub owner: [u8; HASH_LEN],
    /// The history it was opened in.
    pub incarnation: u64,
    /// The epoch its requests carry.
    pub epoch: u64,
    /// Every sequence at or below it is acknowledged.
    pub acknowledged: u64,
    /// Terminal: no request of it runs again.
    pub closed: bool,
}

impl Session {
    /// A session's row, if it exists.
    pub fn read(read: &impl MetaRead, db: &[u8], session: &[u8; 16]) -> Result<Option<Self>> {
        read.meta_row(db, &session_row(session))?
            .map(|bytes| {
                serde_json::from_slice(&bytes)
                    .map_err(|error| Error::corrupt(format!("retry session: {error}")))
            })
            .transpose()
    }

    /// Store the row, answering the bytes it takes as retention counts
    /// them: row name plus stored row.
    fn store(&self, write: &WriteTxn, db: &[u8], session: &[u8; 16]) -> Result<u64> {
        let bytes = self.encode()?;
        let name = session_row(session);
        write.set_meta(db, &name, &bytes)?;
        Ok((name.len() + bytes.len()) as u64)
    }

    /// The bytes the row takes as retention counts them.
    fn cost(&self) -> Result<u64> {
        Ok((SESSION_ROW.len() + 16 + self.encode()?.len()) as u64)
    }

    fn encode(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(self).map_err(|error| Error::corrupt(format!("retry session: {error}")))
    }

    /// The session as callers see it.
    pub fn describe(&self, session: &[u8; 16]) -> Value {
        let mut map = std::collections::BTreeMap::new();
        map.insert(
            "session".to_string(),
            Value::String(crate::hex::encode(session)),
        );
        map.insert(
            "incarnation".to_string(),
            Value::String(format!("{:016x}", self.incarnation)),
        );
        map.insert("epoch".to_string(), Value::Number(self.epoch as f64));
        map.insert(
            "acknowledged".to_string(),
            Value::Number(self.acknowledged as f64),
        );
        map.insert("closed".to_string(), Value::Bool(self.closed));
        Value::Object(map)
    }

    /// The identity of sequence `sequence` of this session.
    pub fn identity(&self, session: &[u8; 16], sequence: u64) -> ScopedIdentity {
        ScopedIdentity {
            incarnation: self.incarnation,
            epoch: self.epoch,
            intent: session_intent(session, sequence),
        }
    }
}

/// A replicated retention transition for one database.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum RetentionCommand {
    /// Initialize retention (epoch zero) if needed, and set or stop
    /// automatic advancing.
    Rotate {
        /// Leader time per epoch; `None` stops rotating.
        epoch_ms: Option<u64>,
        /// Epochs kept admissible, the current one included.
        keep_epochs: u64,
    },
    /// Move to `current_epoch` with floor `min_epoch`, if the current
    /// epoch is still `from_epoch`. Both only rise.
    Advance {
        /// The current epoch the advance was planned against.
        from_epoch: u64,
        /// The new current epoch.
        current_epoch: u64,
        /// The new floor.
        min_epoch: u64,
    },
    /// Inspect up to `limit` receipts from the cursor, deleting those
    /// the floor retired.
    Collect {
        /// Receipts inspected, kept ones included.
        limit: u64,
    },
    /// Open a session in the current epoch, or answer it again for its
    /// owner when it exists (a lost answer is recoverable).
    OpenSession {
        /// Caller-chosen 128-bit ID.
        session: [u8; 16],
        /// The opening principal's receipt owner.
        owner: [u8; HASH_LEN],
    },
    /// Acknowledge every sequence through `through`: their results go
    /// and they never run again. Without `abandon`, each must have a
    /// result and they number at most `limit`; with it, the watermark
    /// moves however far, at most `limit` results go at once and the
    /// next collection walk takes the rest.
    Acknowledge {
        /// The session.
        session: [u8; 16],
        /// The acknowledging principal's receipt owner.
        owner: [u8; HASH_LEN],
        /// The new watermark.
        through: u64,
        /// Fence sequences without a result too: never automatic.
        abandon: bool,
        /// The work this acknowledgement may do, from 1 to
        /// [`MAX_ACK_LIMIT`]: [`ACK_LIMIT`] unless the caller set one.
        #[serde(default = "default_ack_limit")]
        limit: u64,
    },
    /// Close a session for good.
    CloseSession {
        /// The session.
        session: [u8; 16],
        /// The closing principal's receipt owner.
        owner: [u8; HASH_LEN],
    },
    /// Initialize retention (epoch zero) if needed, recount what it
    /// keeps and bound it by `max_receipt_bytes`, or stop bounding it
    /// with `None`. Nothing kept is evicted.
    Budget {
        /// The new budget.
        max_receipt_bytes: Option<u64>,
    },
    /// The leader's default: initialize retention rotating every
    /// `epoch_ms`, keeping `keep_epochs`, unless the database has
    /// retention already, which this never changes.
    Default {
        /// Leader time per epoch.
        epoch_ms: u64,
        /// Epochs kept admissible, the current one included.
        keep_epochs: u64,
    },
}

fn default_ack_limit() -> u64 {
    ACK_LIMIT
}

/// What a session's owner acknowledges (see
/// [`RetentionCommand::Acknowledge`]).
pub use flower_protocol::Acknowledgement;

/// Raw meta reads, from a snapshot or inside a write.
pub trait MetaRead {
    /// Read a raw `meta` value.
    fn meta_row(&self, db: &[u8], name: &[u8]) -> Result<Option<Vec<u8>>>;
}

impl MetaRead for ReadTxn {
    fn meta_row(&self, db: &[u8], name: &[u8]) -> Result<Option<Vec<u8>>> {
        self.meta(db, name)
    }
}

impl MetaRead for WriteTxn {
    fn meta_row(&self, db: &[u8], name: &[u8]) -> Result<Option<Vec<u8>>> {
        self.meta(db, name)
    }
}

/// Receipt and meta-row scans, from a snapshot or inside a write.
pub trait ReceiptScan: MetaRead {
    /// Up to `limit` receipts after `after`, and whether none follow.
    fn receipts_after(
        &self,
        db: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<(Vec<Row>, bool)>;

    /// Every `meta` row whose name starts with `prefix`.
    fn meta_prefix(&self, db: &[u8], prefix: &[u8]) -> Result<Vec<Row>>;

    /// Every receipt of `db`.
    fn all_receipts(&self, db: &[u8]) -> Result<Vec<Row>> {
        Ok(self.receipts_after(db, None, usize::MAX)?.0)
    }
}

impl ReceiptScan for ReadTxn {
    fn receipts_after(
        &self,
        db: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<(Vec<Row>, bool)> {
        self.receipts_page(db, after, limit)
    }

    fn meta_prefix(&self, db: &[u8], prefix: &[u8]) -> Result<Vec<Row>> {
        self.scan_meta_prefix(db, prefix)
    }
}

impl ReceiptScan for WriteTxn {
    fn receipts_after(
        &self,
        db: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<(Vec<Row>, bool)> {
        self.receipts_page(db, after, limit)
    }

    fn meta_prefix(&self, db: &[u8], prefix: &[u8]) -> Result<Vec<Row>> {
        self.scan_meta_prefix(db, prefix)
    }
}

/// What retention's byte accounting counts of a stored receipt: its
/// identity and bytes when it has no expiry, zero otherwise. Only
/// replicated transitions (collection, acknowledgement, close) remove a
/// receipt without an expiry, so every member counts the same; one with
/// an expiry (a caller's own key written while retention was not
/// initialized) is bounded by its expiry instead, and each member
/// purges it on its own schedule.
pub fn receipt_cost(identity: &[u8], bytes: &[u8]) -> Result<u64> {
    Ok(if Receipt::decode(bytes)?.expires_at == 0 {
        (identity.len() + bytes.len()) as u64
    } else {
        0
    })
}

/// What `db` keeps as retention counts it, recounted from scratch:
/// `(receipt_bytes, receipt_count, session_bytes, session_count)`.
/// Equal to the replicated counters on every member (see [`Retention`]).
pub fn recount(read: &impl ReceiptScan, db: &[u8]) -> Result<(u64, u64, u64, u64)> {
    let state = Retention::initial(read, db)?;
    Ok((
        state.receipt_bytes,
        state.receipt_count,
        state.session_bytes,
        state.session_count,
    ))
}

fn session_row(session: &[u8; 16]) -> Vec<u8> {
    let mut row = SESSION_ROW.to_vec();
    row.extend_from_slice(session);
    row
}

/// The intent of a session request: the session and its sequence.
pub fn session_intent(session: &[u8; 16], sequence: u64) -> Vec<u8> {
    let mut intent = Vec::with_capacity(SESSION_INTENT.len() + 24);
    intent.extend_from_slice(SESSION_INTENT);
    intent.extend_from_slice(session);
    intent.extend_from_slice(&sequence.to_be_bytes());
    intent
}

/// The session and sequence a session request's intent names.
pub fn session_of(intent: &[u8]) -> Option<([u8; 16], u64)> {
    let rest = intent.strip_prefix(SESSION_INTENT)?;
    if rest.len() != 24 {
        return None;
    }
    let session: [u8; 16] = rest[..16].try_into().ok()?;
    let sequence = u64::from_be_bytes(rest[16..].try_into().ok()?);
    Some((session, sequence))
}

/// Whether a scoped identity may run, or replay its result, in `db` now:
/// `None` admits, else the deterministic refusal. Wrong histories,
/// retired and not yet admitted epochs and closed, unknown, foreign or
/// acknowledged session requests refuse; `owner` (the caller's receipt
/// owner) is checked against a session's at preparation only, since
/// ownership never changes.
pub fn refuse(
    read: &impl MetaRead,
    db: &[u8],
    identity: &ScopedIdentity,
    owner: Option<&[u8; HASH_LEN]>,
) -> Result<Option<Failure>> {
    // A move gives the database its destination's incarnation and keeps
    // its history: identities minted before it stay this history's.
    if !History::read(read, db)?.holds(identity.incarnation) {
        return Ok(Some(wrong_history()));
    }
    let session = session_of(&identity.intent);
    let Some(state) = Retention::read(read, db)? else {
        return Ok(session.map(|_| {
            fail(
                NOT_INITIALIZED,
                "retry sessions need retention initialized for the database",
            )
        }));
    };
    if identity.epoch < state.min_epoch {
        return Ok(Some(fail(
            EPOCH_RETIRED,
            "retry identity epoch is below the retention floor; its outcome is no longer \
             available, which does not prove it failed",
        )));
    }
    if identity.epoch > state.current_epoch {
        return Ok(Some(fail(
            EPOCH_NOT_ADMITTED,
            "retry identity epoch has not been admitted yet",
        )));
    }
    let Some((id, sequence)) = session else {
        return Ok(None);
    };
    let Some(row) = Session::read(read, db, &id)? else {
        return Ok(Some(fail(
            SESSION_UNKNOWN,
            "the session does not exist, and a request never opens one",
        )));
    };
    Ok(
        if row.epoch != identity.epoch || row.incarnation != identity.incarnation {
            Some(fail(
                SESSION_MISMATCH,
                "the request does not match its session",
            ))
        } else if owner.is_some_and(|owner| *owner != row.owner) {
            Some(fail(
                SESSION_FORBIDDEN,
                "the session belongs to another principal",
            ))
        } else if row.closed {
            Some(fail(SESSION_CLOSED, "the session is closed"))
        } else if sequence <= row.acknowledged {
            Some(fail(
                ALREADY_ACKNOWLEDGED,
                "the request was acknowledged or abandoned and never runs again",
            ))
        } else {
            None
        },
    )
}

/// The epoch a caller's own (unscoped) receipt records at execution: the
/// current one, zero before initialization.
pub fn receipt_epoch(read: &impl MetaRead, db: &[u8]) -> Result<u64> {
    Ok(Retention::read(read, db)?.map_or(0, |state| state.current_epoch))
}

/// How long a caller's own (unscoped) receipt of a call at `timestamp` lasts:
/// `(floor, expires_at)`. Once retention is initialized it keeps the receipt
/// until the floor passes the epoch it ran in, with no expiry (the retention
/// window, not a fixed lifetime, bounds it, and its byte budget counts it);
/// before, the receipt expires [`crate::retry::IDEMPOTENCY_TTL_MS`] after the
/// call.
pub fn caller_receipt(read: &impl MetaRead, db: &[u8], timestamp: u64) -> Result<(u64, u64)> {
    Ok(match Retention::read(read, db)? {
        Some(state) => (state.current_epoch, 0),
        None => (
            0,
            timestamp.saturating_add(crate::retry::IDEMPOTENCY_TTL_MS),
        ),
    })
}

/// Apply one retention transition inside `write`: the value it answers,
/// or its deterministic refusal, which changes nothing.
pub(crate) fn apply(
    write: &WriteTxn,
    db: &[u8],
    command: &RetentionCommand,
) -> Result<std::result::Result<Value, Failure>> {
    let state = Retention::load(write, db)?;
    match command {
        RetentionCommand::Rotate {
            epoch_ms,
            keep_epochs,
        } => {
            if let Some(failure) = invalid_rotation(*epoch_ms, *keep_epochs) {
                return Ok(Err(failure));
            }
            let mut state = match state {
                Some(state) => state,
                None => Retention::initial(write, db)?,
            };
            state.rotation = epoch_ms.map(|epoch_ms| Rotation {
                epoch_ms,
                keep_epochs: *keep_epochs,
            });
            state.epochs_configured = true;
            state.store(write, db)?;
            Ok(Ok(describe(&state)))
        }
        RetentionCommand::Default {
            epoch_ms,
            keep_epochs,
        } => {
            // Epochs anyone set (an operator, or an earlier default)
            // stay; a database only bounded by a budget has none.
            if let Some(state) = &state
                && (state.epochs_configured || state.rotation.is_some())
            {
                return Ok(Ok(describe(state)));
            }
            if let Some(failure) = invalid_rotation(Some(*epoch_ms), *keep_epochs) {
                return Ok(Err(failure));
            }
            let mut state = match state {
                Some(state) => state,
                None => Retention::initial(write, db)?,
            };
            state.rotation = Some(Rotation {
                epoch_ms: *epoch_ms,
                keep_epochs: *keep_epochs,
            });
            state.store(write, db)?;
            Ok(Ok(describe(&state)))
        }
        RetentionCommand::Budget { max_receipt_bytes } => {
            let mut state = match state {
                Some(state) => state,
                None => Retention::initial(write, db)?,
            };
            state.recount(write, db)?;
            state.max_receipt_bytes = *max_receipt_bytes;
            state.store(write, db)?;
            Ok(Ok(describe(&state)))
        }
        RetentionCommand::Advance {
            from_epoch,
            current_epoch,
            min_epoch,
        } => {
            let Some(mut state) = state else {
                return Ok(Err(not_initialized()));
            };
            if state.current_epoch != *from_epoch {
                return Ok(Err(fail(
                    EPOCH_MOVED,
                    format!(
                        "the epoch moved from {from_epoch} to {} first",
                        state.current_epoch
                    ),
                )));
            }
            if *current_epoch < state.current_epoch
                || *min_epoch < state.min_epoch
                || min_epoch > current_epoch
            {
                return Ok(Err(fail(
                    codes::INVALID_VALUE,
                    "epochs only rise, and the floor never passes the current epoch",
                )));
            }
            if *min_epoch > state.min_epoch {
                state.walk_again();
            }
            state.epochs_configured = true;
            state.current_epoch = *current_epoch;
            state.min_epoch = *min_epoch;
            state.store(write, db)?;
            Ok(Ok(describe(&state)))
        }
        RetentionCommand::Collect { limit } => {
            let Some(state) = state else {
                return Ok(Err(not_initialized()));
            };
            collect(write, db, state, *limit).map(Ok)
        }
        RetentionCommand::OpenSession { session, owner } => {
            let Some(mut state) = state else {
                return Ok(Err(not_initialized()));
            };
            if let Some(row) = Session::read(write, db, session)? {
                return Ok(if row.owner != *owner {
                    Err(forbidden())
                } else if !History::read(write, db)?.holds(row.incarnation) {
                    // Opened before a restore: its outcomes are unknown
                    // here, and none of its requests runs again. The
                    // next walk drops it.
                    Err(wrong_history())
                } else {
                    Ok(row.describe(session))
                });
            }
            let incarnation = write.meta_u64(db, meta::HISTORY_INCARNATION)?;
            let row = Session {
                owner: *owner,
                incarnation,
                epoch: state.current_epoch,
                acknowledged: 0,
                closed: false,
            };
            if state.over(state.retained_bytes().saturating_add(row.cost()?)) {
                return Ok(Err(over_budget(&state)));
            }
            let cost = row.store(write, db, session)?;
            state.account_session(None, Some(cost));
            state.store(write, db)?;
            Ok(Ok(row.describe(session)))
        }
        RetentionCommand::Acknowledge {
            session,
            owner,
            through,
            abandon,
            limit,
        } => acknowledge(write, db, state, session, owner, *through, *abandon, *limit),
        RetentionCommand::CloseSession { session, owner } => {
            let mut row = match Session::read(write, db, session)? {
                None => return Ok(Err(unknown())),
                Some(row) if row.owner != *owner => return Ok(Err(forbidden())),
                Some(row) => row,
            };
            if !row.closed {
                let before = row.cost()?;
                row.closed = true;
                let after = row.store(write, db, session)?;
                if let Some(mut state) = state {
                    state.account_session(Some(before), Some(after));
                    // Its results go at the next walk.
                    state.walk_again();
                    state.store(write, db)?;
                }
            }
            Ok(Ok(row.describe(session)))
        }
    }
}

/// Acknowledge `session` through `through` (see
/// [`RetentionCommand::Acknowledge`]). Everything is decided before
/// anything changes, so a refusal changes nothing.
#[allow(clippy::too_many_arguments)]
fn acknowledge(
    write: &WriteTxn,
    db: &[u8],
    state: Option<Retention>,
    session: &[u8; 16],
    owner: &[u8; HASH_LEN],
    through: u64,
    abandon: bool,
    limit: u64,
) -> Result<std::result::Result<Value, Failure>> {
    if limit == 0 || limit > MAX_ACK_LIMIT {
        return Ok(Err(fail(
            codes::INVALID_VALUE,
            format!("an acknowledgement's limit is from 1 to {MAX_ACK_LIMIT}"),
        )));
    }
    let row = match owned(write, db, session, owner)? {
        Ok(row) => row,
        Err(failure) => return Ok(Err(failure)),
    };
    if through <= row.acknowledged {
        return Ok(Ok(row.describe(session)));
    }
    let (released, unfinished) = if abandon {
        abandoned(write, db, &row, session, through, limit)?
    } else {
        if through - row.acknowledged > limit {
            return Ok(Err(fail(
                ACK_BUDGET,
                format!(
                    "this acknowledgement moves past at most {limit} sequences: acknowledge a \
                     shorter prefix first, or set a larger limit"
                ),
            )));
        }
        let mut released = Vec::new();
        for sequence in row.acknowledged + 1..=through {
            let identity = row.identity(session, sequence).encode()?;
            let Some(bytes) = write.get_receipt(db, &identity)? else {
                return Ok(Err(fail(
                    ACK_GAP,
                    format!(
                        "sequence {sequence} has no result; abandon it explicitly to \
                         acknowledge past it"
                    ),
                )));
            };
            let cost = receipt_cost(&identity, &bytes)?;
            released.push((identity, cost));
        }
        (released, false)
    };
    let mut next = row.clone();
    next.acknowledged = through;
    let (before, after) = (row.cost()?, next.cost()?);
    if let Some(state) = &state {
        // A watermark that grows its row by more than it releases must
        // fit; whatever frees room always may.
        let freed: u64 = released.iter().map(|(_, cost)| cost).sum();
        let grown = after.saturating_sub(before);
        if grown > freed && state.over(state.retained_bytes().saturating_add(grown - freed)) {
            return Ok(Err(over_budget(state)));
        }
    }
    for (identity, _) in &released {
        write.delete_receipt(db, identity)?;
    }
    next.store(write, db, session)?;
    if let Some(mut state) = state {
        for (_, cost) in &released {
            state.account_receipt(*cost, false);
        }
        state.account_session(Some(before), Some(after));
        // What an abandon left behind goes at the next walk.
        if unfinished {
            state.walk_again();
        }
        state.store(write, db)?;
    }
    Ok(Ok(next.describe(session)))
}

/// Why a rotation of `epoch_ms` keeping `keep_epochs` is invalid, if it is.
fn invalid_rotation(epoch_ms: Option<u64>, keep_epochs: u64) -> Option<Failure> {
    (keep_epochs < 2 || epoch_ms.is_some_and(|ms| ms < MIN_EPOCH_MS)).then(|| {
        fail(
            codes::INVALID_VALUE,
            format!("a rotation keeps at least 2 epochs of at least {MIN_EPOCH_MS} ms"),
        )
    })
}

/// Results an acknowledgement deletes, with what retention counts of
/// each (see [`receipt_cost`]).
type Released = Vec<(Vec<u8>, u64)>;

/// The results an abandon through `through` deletes at once: at most
/// `limit` of the session's, found in key order after its watermark
/// (the sequences it fences may number in the billions, and most never
/// ran), and whether any it fences are left for the next walk.
fn abandoned(
    write: &WriteTxn,
    db: &[u8],
    row: &Session,
    session: &[u8; 16],
    through: u64,
    limit: u64,
) -> Result<(Released, bool)> {
    // A session's identities differ only in their trailing big-endian
    // sequence: its results are contiguous, in sequence order.
    let start = row.identity(session, row.acknowledged).encode()?;
    let prefix = &start[..start.len() - 8];
    let limit = usize::try_from(limit).unwrap_or(usize::MAX);
    let (page, _) = write.receipts_page(db, Some(&start), limit.saturating_add(1))?;
    let mut released = Vec::new();
    for (identity, bytes) in page {
        let sequence = match identity.strip_prefix(prefix) {
            Some(tail) => match <[u8; 8]>::try_from(tail) {
                Ok(tail) => u64::from_be_bytes(tail),
                Err(_) => break,
            },
            None => break,
        };
        if sequence > through {
            break;
        }
        if released.len() == limit {
            return Ok((released, true));
        }
        let cost = receipt_cost(&identity, &bytes)?;
        released.push((identity, cost));
    }
    Ok((released, false))
}

/// The refusal of new results past the budget.
fn over_budget(state: &Retention) -> Failure {
    fail(
        BUDGET_EXCEEDED,
        format!(
            "retention keeps {} bytes of results and sessions, and its budget is {}: \
             acknowledge, close or collect to make room",
            state.retained_bytes(),
            state.max_receipt_bytes.unwrap_or(u64::MAX)
        ),
    )
}

/// The budget's verdict on receipts a patch is about to store in `db`:
/// its refusal when they do not fit, else `None`. Only receipts
/// retention removes count (see [`receipt_cost`]); one replacing a
/// stored receipt under the same identity releases that one, and a
/// patch that frees as much as it adds always fits.
pub(crate) fn over_budget_with(
    write: &WriteTxn,
    db: &[u8],
    receipts: &[(Vec<u8>, Vec<u8>)],
) -> Result<Option<Failure>> {
    let Some(state) = Retention::load(write, db)? else {
        return Ok(None);
    };
    let (mut added, mut freed) = (0u64, 0u64);
    for (identity, bytes) in receipts {
        added = added.saturating_add(receipt_cost(identity, bytes)?);
        if let Some(old) = write.get_receipt(db, identity)? {
            freed = freed.saturating_add(receipt_cost(identity, &old)?);
        }
    }
    Ok(
        (added > freed && state.over(state.retained_bytes().saturating_add(added - freed)))
            .then(|| over_budget(&state)),
    )
}

/// Count receipts a patch is about to store in `db`, inside its write,
/// once [`over_budget_with`] admitted them: before they replace what
/// their identities held.
pub(crate) fn charge(write: &WriteTxn, db: &[u8], receipts: &[(Vec<u8>, Vec<u8>)]) -> Result<()> {
    let Some(before) = Retention::read(write, db)? else {
        return Ok(());
    };
    let mut state = before.clone();
    if !state.counted {
        state.recount(write, db)?;
    }
    for (identity, bytes) in receipts {
        if let Some(old) = write.get_receipt(db, identity)? {
            state.account_receipt(receipt_cost(identity, &old)?, false);
        }
        state.account_receipt(receipt_cost(identity, bytes)?, true);
    }
    if state != before {
        state.store(write, db)?;
    }
    Ok(())
}

/// An open session of `owner`'s, or the refusal.
fn owned(
    write: &WriteTxn,
    db: &[u8],
    session: &[u8; 16],
    owner: &[u8; HASH_LEN],
) -> Result<std::result::Result<Session, Failure>> {
    Ok(match Session::read(write, db, session)? {
        None => Err(unknown()),
        Some(row) if row.owner != *owner => Err(forbidden()),
        Some(row) if row.closed => Err(fail(SESSION_CLOSED, "the session is closed")),
        Some(row) => Ok(row),
    })
}

/// Inspect up to `limit` receipts from the cursor and delete the ones
/// the floor retired; past the last, drop the sessions it retired too.
///
/// A walk sees only the receipts retention removes (those without an
/// expiry, see [`receipt_cost`]). Every member holds exactly those at
/// every entry, so every member inspects the same receipts, keeps the
/// same cursor and finishes at the same entry. A receipt with an expiry
/// (a caller's own key from before retention was initialized) is each
/// member's to purge once expired, at its own pace: a walk that counted
/// it would page differently on each member.
fn collect(write: &WriteTxn, db: &[u8], mut state: Retention, limit: u64) -> Result<Value> {
    let (mut inspected, mut deleted) = (0u64, 0u64);
    if !state.collected {
        let history = History::read(write, db)?;
        let limit = limit.max(1);
        let page = usize::try_from(limit).unwrap_or(usize::MAX).min(4_096);
        // Where the next physical page starts: past expiring receipts
        // too, which the replicated cursor never names.
        let mut from = state.cursor.clone();
        let complete = 'walk: loop {
            let (rows, last) = write.receipts_page(db, from.as_deref(), page)?;
            for (identity, bytes) in &rows {
                let cost = receipt_cost(identity, bytes)?;
                if cost == 0 {
                    continue;
                }
                if inspected == limit {
                    // Another one is left: the next entry goes on.
                    break 'walk false;
                }
                inspected += 1;
                state.cursor = Some(identity.clone());
                if collectable(write, db, &state, &history, identity, bytes)? {
                    write.delete_receipt(db, identity)?;
                    state.account_receipt(cost, false);
                    deleted += 1;
                }
            }
            if last || rows.is_empty() {
                break true;
            }
            from = rows.last().map(|(identity, _)| identity.clone());
        };
        if complete {
            // Another walk follows when anything asked for one during
            // this one: what it passed may be collectable now.
            state.cursor = None;
            state.collected = !state.again;
            state.again = false;
            for (name, bytes) in write.scan_meta_prefix(db, SESSION_ROW)? {
                let row: Session = serde_json::from_slice(&bytes)
                    .map_err(|error| Error::corrupt(format!("retry session: {error}")))?;
                if row.epoch < state.min_epoch || !history.holds(row.incarnation) {
                    write.delete_meta(db, &name)?;
                    state.account_session(Some((name.len() + bytes.len()) as u64), None);
                }
            }
        }
        state.store(write, db)?;
    }
    let mut map = std::collections::BTreeMap::new();
    map.insert("inspected".to_string(), Value::Number(inspected as f64));
    map.insert("deleted".to_string(), Value::Number(deleted as f64));
    map.insert("complete".to_string(), Value::Bool(state.collected));
    Ok(Value::Object(map))
}

/// Whether the floor retired a receipt: a scoped one below it or of
/// another history, an acknowledged or closed session's result, or a
/// caller's own one that ran in an epoch below it.
fn collectable(
    write: &WriteTxn,
    db: &[u8],
    state: &Retention,
    history: &History,
    identity: &[u8],
    bytes: &[u8],
) -> Result<bool> {
    if identity.starts_with(UNSCOPED_PREFIX) {
        return Ok(Receipt::decode(bytes)?.floor < state.min_epoch);
    }
    let Ok(scoped) = ScopedIdentity::decode(identity) else {
        return Ok(false);
    };
    if !history.holds(scoped.incarnation) || scoped.epoch < state.min_epoch {
        return Ok(true);
    }
    let Some((session, sequence)) = session_of(&scoped.intent) else {
        return Ok(false);
    };
    Ok(match Session::read(write, db, &session)? {
        None => true,
        Some(row) => row.closed || sequence <= row.acknowledged,
    })
}

/// The state as operators see it.
pub fn describe(state: &Retention) -> Value {
    let mut map = std::collections::BTreeMap::new();
    map.insert(
        "current_epoch".to_string(),
        Value::Number(state.current_epoch as f64),
    );
    map.insert(
        "min_epoch".to_string(),
        Value::Number(state.min_epoch as f64),
    );
    map.insert(
        "epoch_ms".to_string(),
        state.rotation.map_or(Value::Null, |rotation| {
            Value::Number(rotation.epoch_ms as f64)
        }),
    );
    map.insert(
        "keep_epochs".to_string(),
        state.rotation.map_or(Value::Null, |rotation| {
            Value::Number(rotation.keep_epochs as f64)
        }),
    );
    map.insert("collected".to_string(), Value::Bool(state.collected));
    for (name, count) in [
        ("receipt_bytes", state.receipt_bytes),
        ("receipt_count", state.receipt_count),
        ("session_bytes", state.session_bytes),
        ("session_count", state.session_count),
    ] {
        // Unknown until a state from before counting is first used.
        let count = match state.counted {
            true => Value::Number(count as f64),
            false => Value::Null,
        };
        map.insert(name.to_string(), count);
    }
    map.insert(
        "max_receipt_bytes".to_string(),
        state
            .max_receipt_bytes
            .map_or(Value::Null, |max| Value::Number(max as f64)),
    );
    Value::Object(map)
}

fn wrong_history() -> Failure {
    fail(
        WRONG_HISTORY,
        "retry identity belongs to a different history: a restore ended it, and its outcome \
         is unknown here",
    )
}

fn not_initialized() -> Failure {
    fail(
        NOT_INITIALIZED,
        "retention is not initialized for the database",
    )
}

fn unknown() -> Failure {
    fail(SESSION_UNKNOWN, "the session does not exist")
}

fn forbidden() -> Failure {
    fail(
        SESSION_FORBIDDEN,
        "the session belongs to another principal",
    )
}

/// A scoped request ID: `f1:<incarnation>:<epoch>:<intent>` with a
/// 16-digit hex incarnation and a 64-digit hex intent hash, or a session
/// request `f2:<incarnation>:<epoch>:<session>:<sequence>` with a
/// 32-digit hex session. Any other key is a caller's own, `None`.
pub fn parse_request_id(id: &str) -> Option<HostResult<ScopedIdentity>> {
    let (version, rest) = id.split_once(':')?;
    if version != "f1" && version != "f2" {
        return None;
    }
    let invalid = || {
        fail(
            codes::INVALID_VALUE,
            format!("`{id}` is not a canonical {version} request ID"),
        )
    };
    let parts: Vec<&str> = rest.split(':').collect();
    let number = |text: &str| -> HostResult<u64> {
        let parsed: u64 = text.parse().map_err(|_| invalid())?;
        if parsed.to_string() != text {
            return Err(invalid());
        }
        Ok(parsed)
    };
    let hex = |text: &str, len: usize| -> HostResult<Vec<u8>> {
        crate::hex::decode(text)
            .filter(|bytes| bytes.len() == len)
            .ok_or_else(invalid)
    };
    let parsed = (|| {
        let incarnation = u64::from_be_bytes(
            hex(parts.first().ok_or_else(invalid)?, 8)?
                .try_into()
                .map_err(|_| invalid())?,
        );
        let epoch = number(parts.get(1).ok_or_else(invalid)?)?;
        let intent = match (version, parts.len()) {
            ("f1", 3) => hex(parts[2], HASH_LEN)?,
            ("f2", 4) => {
                let session: [u8; 16] = hex(parts[2], 16)?.try_into().map_err(|_| invalid())?;
                let sequence = number(parts[3])?;
                if sequence == 0 {
                    return Err(invalid());
                }
                session_intent(&session, sequence)
            }
            _ => return Err(invalid()),
        };
        Ok(ScopedIdentity {
            incarnation,
            epoch,
            intent,
        })
    })();
    Some(parsed)
}

/// The request ID of an `f1` identity for intent hash `intent`.
pub fn request_id(incarnation: u64, epoch: u64, intent: &[u8; HASH_LEN]) -> String {
    flower_protocol::request_id(incarnation, epoch, intent)
}

/// The request ID of sequence `sequence` of a session.
pub fn session_request_id(
    incarnation: u64,
    epoch: u64,
    session: &[u8; 16],
    sequence: u64,
) -> String {
    flower_protocol::session_request_id(incarnation, epoch, session, sequence)
}

/// The business fingerprint a scoped receipt pins: the method, its
/// arguments and the caller's expected revision, never credentials.
pub fn request_fingerprint(
    module: &str,
    name: &str,
    args: &Value,
    expected_revision: Option<u64>,
) -> HostResult<[u8; HASH_LEN]> {
    use sha2::Digest;
    let mut framed = Vec::new();
    for part in [module.as_bytes(), name.as_bytes()] {
        crate::storage::keys::push_sized(&mut framed, part).map_err(Failure::from)?;
    }
    let args = crate::json::render(args).map_err(Failure::from)?;
    crate::storage::keys::push_sized(&mut framed, &args).map_err(Failure::from)?;
    match expected_revision {
        None => framed.push(0),
        Some(revision) => {
            framed.push(1);
            framed.extend_from_slice(&revision.to_be_bytes());
        }
    }
    Ok(sha2::Sha256::digest(&framed).into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_ids_round_trip_and_reject_noncanonical_forms() {
        let intent = [7u8; HASH_LEN];
        let id = request_id(0xabc, 4, &intent);
        let parsed = parse_request_id(&id).unwrap().unwrap();
        assert_eq!(
            (parsed.incarnation, parsed.epoch, parsed.intent.as_slice()),
            (0xabc, 4, intent.as_slice())
        );
        let session = [3u8; 16];
        let id = session_request_id(9, 2, &session, 17);
        let parsed = parse_request_id(&id).unwrap().unwrap();
        assert_eq!(session_of(&parsed.intent), Some((session, 17)));
        assert!(parse_request_id("order-17").is_none());
        for bad in [
            "f1:abc:4:00",
            "f1:0000000000000abc:04:0707070707070707070707070707070707070707070707070707070707070707",
            "f2:0000000000000009:2:03030303030303030303030303030303:0",
            "f2:0000000000000009:2:0303:5",
            "f1:0000000000000abc:4",
        ] {
            assert!(parse_request_id(bad).unwrap().is_err(), "{bad}");
        }
    }

    /// A store in a fresh temporary file.
    fn temp_store(name: &str) -> (crate::storage::GroupStore, std::path::PathBuf) {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "flower-retention-{name}-{}-{}",
            std::process::id(),
            crate::hex::encode(&rand::random::<[u8; 8]>())
        ));
        let _ = std::fs::remove_file(&path);
        (crate::storage::GroupStore::create(&path).unwrap(), path)
    }

    /// Mark a walk due, as a rising floor or a closed session does.
    fn walk_due(write: &WriteTxn) {
        let mut state = Retention::read(write, b"db").unwrap().unwrap();
        state.collected = false;
        state.store(write, b"db").unwrap();
    }

    /// Identities and sessions minted under the incarnation a database
    /// moved in with stay its own: they run or replay, and a walk keeps
    /// their results and rows. A restore's fresh incarnation ends that
    /// history: they are refused, and the next walk drops them.
    #[test]
    fn a_move_keeps_the_history_and_a_restore_ends_it() {
        let (store, path) = temp_store("lineage");
        let owner = [1u8; HASH_LEN];
        let write = store.begin_write().unwrap();
        write
            .set_meta_u64(b"db", meta::HISTORY_INCARNATION, 7)
            .unwrap();
        let rotate = RetentionCommand::Rotate {
            epoch_ms: None,
            keep_epochs: 2,
        };
        apply(&write, b"db", &rotate).unwrap().unwrap();
        let session = [5u8; 16];
        let open = RetentionCommand::OpenSession { session, owner };
        apply(&write, b"db", &open).unwrap().unwrap();
        let request = Session::read(&write, b"db", &session)
            .unwrap()
            .unwrap()
            .identity(&session, 1);
        let once = ScopedIdentity {
            incarnation: 7,
            epoch: 0,
            intent: vec![3; HASH_LEN],
        };
        let receipt = Receipt {
            result: Some(crate::retry::Outcome::Value(b"1".to_vec())),
            fingerprint: [0; HASH_LEN],
            floor: 0,
            expires_at: 0,
            committed: None,
        }
        .encode()
        .unwrap();
        for identity in [&request, &once] {
            write
                .put_receipt(b"db", &identity.encode().unwrap(), &receipt)
                .unwrap();
        }
        // The move activates at a destination of incarnation 8, with the
        // source's behind it in the lineage (see `placement::lineage`).
        write
            .set_meta_u64(b"db", meta::HISTORY_INCARNATION, 8)
            .unwrap();
        let lineage = [8u64.to_be_bytes(), 7u64.to_be_bytes()].concat();
        write
            .set_meta(b"db", meta::HISTORY_LINEAGE, &lineage)
            .unwrap();
        for identity in [&request, &once] {
            assert_eq!(refuse(&write, b"db", identity, Some(&owner)).unwrap(), None);
        }
        walk_due(&write);
        apply(&write, b"db", &RetentionCommand::Collect { limit: 64 })
            .unwrap()
            .unwrap();
        assert_eq!(write.receipts_page(b"db", None, 64).unwrap().0.len(), 2);
        assert!(Session::read(&write, b"db", &session).unwrap().is_some());
        // A restore of the destination gives a fresh incarnation.
        write
            .set_meta_u64(b"db", meta::HISTORY_INCARNATION, 9)
            .unwrap();
        for identity in [&request, &once] {
            let refused = refuse(&write, b"db", identity, Some(&owner)).unwrap();
            assert_eq!(refused.unwrap().code, WRONG_HISTORY);
        }
        walk_due(&write);
        apply(&write, b"db", &RetentionCommand::Collect { limit: 64 })
            .unwrap()
            .unwrap();
        assert!(write.receipts_page(b"db", None, 64).unwrap().0.is_empty());
        assert!(Session::read(&write, b"db", &session).unwrap().is_none());
        drop(write);
        drop(store);
        let _ = std::fs::remove_file(&path);
    }

    /// Members purge expired caller keys each at its own pace, so their
    /// receipt tables differ in those rows; a walk must still inspect,
    /// delete and finish exactly alike on each, leaving the same
    /// replicated state. (Before, a walk paged over the purgeable rows
    /// too, and members that had purged some kept other cursors and
    /// finished at other entries.)
    #[test]
    fn walks_are_the_same_on_members_that_purged_differently() {
        let receipt = |expires_at: u64| {
            Receipt {
                result: Some(crate::retry::Outcome::Value(b"1".to_vec())),
                fingerprint: [0; HASH_LEN],
                floor: 0,
                expires_at,
                committed: None,
            }
            .encode()
            .unwrap()
        };
        // Caller keys are shorter and sort first; kept rows of another
        // history and of the database's own follow.
        let own = 0x8000_0000_0000_0007;
        let mut kept = Vec::new();
        for (incarnation, epoch) in [(1, 0), (1, 1), (own, 0), (own, 1), (own, 2)] {
            let identity = ScopedIdentity {
                incarnation,
                epoch,
                intent: vec![epoch as u8; HASH_LEN],
            };
            kept.push(identity.encode().unwrap());
        }
        let purgeable: Vec<Vec<u8>> = (0..3u8)
            .map(|key| crate::retry::unscoped_identity_key(&[key]))
            .collect();
        let member = |name: &str, purged: bool| {
            let (store, path) = temp_store(name);
            let write = store.begin_write().unwrap();
            write
                .set_meta_u64(b"db", meta::HISTORY_INCARNATION, own)
                .unwrap();
            let rotate = RetentionCommand::Rotate {
                epoch_ms: None,
                keep_epochs: 2,
            };
            apply(&write, b"db", &rotate).unwrap().unwrap();
            for identity in &kept {
                write.put_receipt(b"db", identity, &receipt(0)).unwrap();
            }
            if !purged {
                for identity in &purgeable {
                    write.put_receipt(b"db", identity, &receipt(5)).unwrap();
                }
            }
            // Written behind accounting's back: count them.
            let recount = RetentionCommand::Budget {
                max_receipt_bytes: None,
            };
            apply(&write, b"db", &recount).unwrap().unwrap();
            write.commit().unwrap();
            (store, path)
        };
        let members = [member("unpurged", false), member("purged", true)];
        let state = |store: &crate::storage::GroupStore| {
            let read = store.begin_read().unwrap();
            Retention::read(&read, b"db").unwrap().unwrap()
        };
        let mut answers: Vec<Vec<Value>> = vec![Vec::new(), Vec::new()];
        for (index, (store, _)) in members.iter().enumerate() {
            let write = store.begin_write().unwrap();
            let advance = RetentionCommand::Advance {
                from_epoch: 0,
                current_epoch: 2,
                min_epoch: 1,
            };
            apply(&write, b"db", &advance).unwrap().unwrap();
            write.commit().unwrap();
            for _ in 0..6 {
                let write = store.begin_write().unwrap();
                let collect = RetentionCommand::Collect { limit: 1 };
                answers[index].push(apply(&write, b"db", &collect).unwrap().unwrap());
                write.commit().unwrap();
            }
        }
        assert_eq!(answers[0], answers[1]);
        assert_eq!(state(&members[0].0), state(&members[1].0));
        // Every kept row was inspected once: five pages, then done.
        let complete = |answer: &Value| match answer {
            Value::Object(fields) => fields.get("complete") == Some(&Value::Bool(true)),
            _ => false,
        };
        assert_eq!(answers[0].iter().position(complete), Some(4));
        // Of the other history and below the floor: three gone.
        let read = members[0].0.begin_read().unwrap();
        let left: Vec<Vec<u8>> = read
            .scan_receipts(b"db")
            .unwrap()
            .into_iter()
            .map(|(identity, _)| identity)
            .filter(|identity| !identity.starts_with(UNSCOPED_PREFIX))
            .collect();
        assert_eq!(left, kept[3..].to_vec());
        assert_eq!(
            recount(&read, b"db").unwrap().0,
            state(&members[0].0).receipt_bytes
        );
        drop(read);
        for (store, path) in members {
            drop(store);
            let _ = std::fs::remove_file(&path);
        }
    }

    /// A stored receipt with no expiry.
    fn kept_receipt() -> Vec<u8> {
        Receipt {
            result: Some(crate::retry::Outcome::Value(b"1".to_vec())),
            fingerprint: [0; HASH_LEN],
            floor: 0,
            expires_at: 0,
            committed: None,
        }
        .encode()
        .unwrap()
    }

    /// Whether a walk's answer says it completed.
    fn complete(answer: &Value) -> bool {
        matches!(answer, Value::Object(fields) if fields.get("complete") == Some(&Value::Bool(true)))
    }

    /// Abandons that leave results behind, however often they come,
    /// never keep a walk from reaching the end: the receipts last in
    /// key order are collected while they keep coming. (Before, each
    /// such abandon restarted the database's walk from the start, so
    /// one per entry starved collection for good.)
    #[test]
    fn abandons_never_keep_a_walk_from_reaching_the_end() {
        let (store, path) = temp_store("abandons");
        let owner = [1u8; HASH_LEN];
        let write = store.begin_write().unwrap();
        write
            .set_meta_u64(b"db", meta::HISTORY_INCARNATION, 7)
            .unwrap();
        let rotate = RetentionCommand::Rotate {
            epoch_ms: None,
            keep_epochs: 2,
        };
        apply(&write, b"db", &rotate).unwrap().unwrap();
        let session = [5u8; 16];
        let open = RetentionCommand::OpenSession { session, owner };
        apply(&write, b"db", &open).unwrap().unwrap();
        let row = Session::read(&write, b"db", &session).unwrap().unwrap();
        for sequence in 1..=600 {
            let identity = row.identity(&session, sequence).encode().unwrap();
            write
                .put_receipt(b"db", &identity, &kept_receipt())
                .unwrap();
        }
        // Of another history, and longer: last in key order.
        let tail: Vec<Vec<u8>> = (0..10u8)
            .map(|index| {
                ScopedIdentity {
                    incarnation: 1,
                    epoch: 0,
                    intent: vec![index; 64],
                }
                .encode()
                .unwrap()
            })
            .collect();
        for identity in &tail {
            write.put_receipt(b"db", identity, &kept_receipt()).unwrap();
        }
        let recount = RetentionCommand::Budget {
            max_receipt_bytes: None,
        };
        apply(&write, b"db", &recount).unwrap().unwrap();
        walk_due(&write);
        let mut through = 0;
        let mut reached = None;
        for entry in 0..200 {
            apply(&write, b"db", &RetentionCommand::Collect { limit: 8 })
                .unwrap()
                .unwrap();
            if tail
                .iter()
                .all(|identity| write.get_receipt(b"db", identity).unwrap().is_none())
            {
                reached = Some(entry);
                break;
            }
            // Three more sequences fenced, one result deleted at once.
            through += 3;
            let abandon = RetentionCommand::Acknowledge {
                session,
                owner,
                through,
                abandon: true,
                limit: 1,
            };
            apply(&write, b"db", &abandon).unwrap().unwrap();
        }
        let reached = reached.expect("the walk never reached the end");
        assert!(reached < 120, "{reached}");
        // Collection then finishes what the abandons left.
        let mut answer = Value::Null;
        for _ in 0..200 {
            answer = apply(&write, b"db", &RetentionCommand::Collect { limit: 64 })
                .unwrap()
                .unwrap();
            if complete(&answer) {
                break;
            }
        }
        assert!(complete(&answer), "{answer:?}");
        let left = write.receipts_page(b"db", None, 1_000).unwrap().0.len() as u64;
        assert_eq!(left, 600 - through);
        assert_eq!(
            recount_of(&write),
            counters(&Retention::read(&write, b"db").unwrap().unwrap())
        );
        drop(write);
        drop(store);
        let _ = std::fs::remove_file(&path);
    }

    fn counters(state: &Retention) -> (u64, u64, u64, u64) {
        (
            state.receipt_bytes,
            state.receipt_count,
            state.session_bytes,
            state.session_count,
        )
    }

    fn recount_of(write: &WriteTxn) -> (u64, u64, u64, u64) {
        recount(write, b"db").unwrap()
    }

    /// The leader's default gives a rotation to a database an operator
    /// only bounded with a budget, so its results are still collected;
    /// either order of the two ends the same. An operator's own rotation
    /// or advance still wins. (Before, a budget applied first left the
    /// database without a rotation for good, keeping every caller key
    /// until the budget refused them all.)
    #[test]
    fn a_budget_set_first_leaves_the_default_rotation_to_the_leader() {
        let (store, path) = temp_store("budget-default");
        let write = store.begin_write().unwrap();
        let budget = RetentionCommand::Budget {
            max_receipt_bytes: Some(4_096),
        };
        let default = RetentionCommand::Default {
            epoch_ms: 10_000,
            keep_epochs: 9,
        };
        apply(&write, b"one", &budget).unwrap().unwrap();
        apply(&write, b"one", &default).unwrap().unwrap();
        apply(&write, b"two", &default).unwrap().unwrap();
        apply(&write, b"two", &budget).unwrap().unwrap();
        let one = Retention::read(&write, b"one").unwrap().unwrap();
        let two = Retention::read(&write, b"two").unwrap().unwrap();
        assert_eq!(one, two);
        assert_eq!(
            one.rotation,
            Some(Rotation {
                epoch_ms: 10_000,
                keep_epochs: 9
            })
        );
        assert_eq!(one.max_receipt_bytes, Some(4_096));
        for (db, operator) in [
            (
                &b"three"[..],
                RetentionCommand::Rotate {
                    epoch_ms: None,
                    keep_epochs: 3,
                },
            ),
            (
                &b"four"[..],
                RetentionCommand::Advance {
                    from_epoch: 0,
                    current_epoch: 1,
                    min_epoch: 0,
                },
            ),
        ] {
            apply(&write, db, &budget).unwrap().unwrap();
            apply(&write, db, &operator).unwrap().unwrap();
            apply(&write, db, &default).unwrap().unwrap();
            let state = Retention::read(&write, db).unwrap().unwrap();
            assert_eq!(state.rotation, None, "{}", String::from_utf8_lossy(db));
        }
        drop(write);
        drop(store);
        let _ = std::fs::remove_file(&path);
    }

    /// A retention state written before retention counted bytes is
    /// counted the first time anything uses it: the budget then bounds
    /// what is really kept. (Before, its counters started at zero, so
    /// the budget undercounted until someone set one again.)
    #[test]
    fn a_state_from_before_counting_is_counted_when_used() {
        let (store, path) = temp_store("uncounted");
        let write = store.begin_write().unwrap();
        for index in 0..3u8 {
            let identity = ScopedIdentity {
                incarnation: 0,
                epoch: 0,
                intent: vec![index; HASH_LEN],
            }
            .encode()
            .unwrap();
            write
                .put_receipt(b"db", &identity, &kept_receipt())
                .unwrap();
        }
        // As an earlier build stored it.
        let legacy =
            br#"{"current_epoch":0,"min_epoch":0,"rotation":null,"cursor":null,"collected":true}"#;
        write.set_meta(b"db", STATE_ROW, legacy).unwrap();
        let open = RetentionCommand::OpenSession {
            session: [2; 16],
            owner: [1; HASH_LEN],
        };
        apply(&write, b"db", &open).unwrap().unwrap();
        let state = Retention::read(&write, b"db").unwrap().unwrap();
        assert_eq!(counters(&state), recount_of(&write));
        assert_eq!(state.receipt_count, 3);
        // A budget the kept receipts already fill refuses a new one.
        let mut full = state.clone();
        full.max_receipt_bytes = Some(state.retained_bytes());
        full.store(&write, b"db").unwrap();
        let identity = ScopedIdentity {
            incarnation: 0,
            epoch: 0,
            intent: vec![9; HASH_LEN],
        }
        .encode()
        .unwrap();
        let refused = over_budget_with(&write, b"db", &[(identity, kept_receipt())]).unwrap();
        assert_eq!(refused.unwrap().code, BUDGET_EXCEEDED);
        drop(write);
        drop(store);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn rotations_keep_their_epochs() {
        let rotation = Rotation {
            epoch_ms: 1_000,
            keep_epochs: 3,
        };
        assert_eq!(rotation.floor(1), 0);
        assert_eq!(rotation.floor(5), 3);
    }
}
