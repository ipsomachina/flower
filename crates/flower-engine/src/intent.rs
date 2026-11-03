//! The local intent log: queued mutations awaiting submission.
//!
//! A synced replica never commits shared state locally. Instead it stores
//! intents — method, arguments and stable request identity — in a
//! local-only database and submits them to the cloud. Each intent belongs
//! to exactly one followed database; submission can fail or be rejected,
//! and the intent stays queued until it commits or terminally fails.
//!
//! The log lives inside the ten-table layout: intents are records of the
//! reserved `$intents` collection keyed by big-endian sequence, the
//! `$intents_by_identity` index maps stable identities to sequences, and
//! the `$intent_seq`/`$intent_count` meta rows track the allocator and
//! the bound. Statuses stay readable keyed by identity until the intent
//! leaves the queue.
//!
//! Lifecycle: `queued` → `submitted` → `acknowledged` → `committed`, with
//! `failed` terminal from any live state and requeue back to `queued`
//! when a submission attempt never reached the cloud. Committed and
//! failed intents leave the queue through explicit drops after the sync
//! client observes them.

use flower_sdk::{Failure, Value, fail};

use crate::error::{Error, HostResult, Result};
use crate::retry::HASH_LEN;

/// The local-only intent-log database.
pub const INTENT_DB: &[u8] = b"$intent-log";
/// The reserved intents collection.
const INTENTS_COLLECTION: &[u8] = b"$intents";
/// The identity lookup index.
const INTENTS_BY_IDENTITY: &[u8] = b"$intents_by_identity";
/// Last assigned sequence, `u64`.
const INTENT_SEQ: &[u8] = b"$intent_seq";
/// Live intent count, `u64`.
const INTENT_COUNT: &[u8] = b"$intent_count";

/// An intent's lifecycle status.
#[derive(Clone, Debug, PartialEq)]
pub enum IntentStatus {
    /// Stored locally, never submitted.
    Queued,
    /// Submitted, awaiting acknowledgement.
    Submitted,
    /// Acknowledged by the cloud, awaiting its receipt.
    Acknowledged,
    /// Retry history is unavailable. Application evidence must resolve it; never re-key it.
    Unresolved { failure: Failure },
    /// Committed at a cloud revision.
    Committed {
        /// The committing revision.
        revision: u64,
    },
    /// Terminally failed with the cloud's outcome.
    Failed {
        /// The terminal failure.
        failure: Failure,
    },
}

impl IntentStatus {
    /// Whether `next` is a legal transition from `self`.
    pub fn can_transition_to(&self, next: &IntentStatus) -> bool {
        matches!(
            (self, next),
            (IntentStatus::Queued, IntentStatus::Submitted)
                | (
                    IntentStatus::Queued | IntentStatus::Submitted | IntentStatus::Acknowledged,
                    IntentStatus::Unresolved { .. }
                )
                | (
                    IntentStatus::Unresolved { .. },
                    IntentStatus::Committed { .. } | IntentStatus::Failed { .. }
                )
                | (IntentStatus::Queued, IntentStatus::Failed { .. })
                | (IntentStatus::Submitted, IntentStatus::Acknowledged)
                | (IntentStatus::Submitted, IntentStatus::Failed { .. })
                | (IntentStatus::Submitted, IntentStatus::Queued)
                | (IntentStatus::Acknowledged, IntentStatus::Committed { .. })
                | (IntentStatus::Acknowledged, IntentStatus::Failed { .. })
                | (IntentStatus::Acknowledged, IntentStatus::Queued)
        )
    }

    /// The status state name for encoding.
    fn name(&self) -> &'static str {
        match self {
            IntentStatus::Queued => "queued",
            IntentStatus::Submitted => "submitted",
            IntentStatus::Acknowledged => "acknowledged",
            IntentStatus::Unresolved { .. } => "unresolved",
            IntentStatus::Committed { .. } => "committed",
            IntentStatus::Failed { .. } => "failed",
        }
    }
}

/// A queued intent: a locally stored mutation for later submission.
#[derive(Clone, Debug, PartialEq)]
pub struct Intent {
    /// Log sequence, assigned at enqueue.
    pub sequence: u64,
    /// Followed database identifier bytes.
    pub db: Vec<u8>,
    /// Target method alias (`module/name`).
    pub alias: String,
    /// Canonical argument value.
    pub args: Value,
    /// Stable request identity bytes.
    pub identity: Vec<u8>,
    /// Fingerprint over business intent plus explicit preconditions.
    pub fingerprint: [u8; HASH_LEN],
    /// Revision the intent was written against, if any.
    pub expected_revision: Option<u64>,
    /// Lifecycle status.
    pub status: IntentStatus,
    /// Enqueue timestamp in milliseconds.
    pub enqueued_ms: u64,
}

/// Encode an intent as canonical JSON. Sequences and revisions travel
/// as JSON numbers, exact to 2^53 and far past any per-device counter.
pub fn encode_intent(intent: &Intent) -> Result<Vec<u8>> {
    let mut map = std::collections::BTreeMap::new();
    map.insert(
        "sequence".to_string(),
        Value::Number(intent.sequence as f64),
    );
    map.insert(
        "db".to_string(),
        Value::String(crate::hex::encode(&intent.db)),
    );
    map.insert("alias".to_string(), Value::String(intent.alias.clone()));
    map.insert("args".to_string(), intent.args.clone());
    map.insert(
        "identity".to_string(),
        Value::String(crate::hex::encode(&intent.identity)),
    );
    map.insert(
        "fingerprint".to_string(),
        Value::String(crate::hex::encode(&intent.fingerprint)),
    );
    map.insert(
        "expected_revision".to_string(),
        match intent.expected_revision {
            Some(revision) => Value::Number(revision as f64),
            None => Value::Null,
        },
    );
    map.insert("status".to_string(), encode_status(&intent.status)?);
    map.insert(
        "enqueued_ms".to_string(),
        Value::Number(intent.enqueued_ms as f64),
    );
    crate::json::render(&Value::Object(map))
}

/// Decode an intent, rejecting malformed rows as corruption: the log is
/// engine-written, so garbage means storage failure, not guest input.
pub fn decode_intent(bytes: &[u8]) -> Result<Intent> {
    let value = crate::json::parse_value(bytes)?;
    let map = match value {
        Value::Object(map) => map,
        _ => return Err(Error::corrupt("intent is not an object".to_string())),
    };
    let get = |name: &str| {
        map.get(name)
            .cloned()
            .ok_or_else(|| Error::corrupt(format!("intent has no `{name}`")))
    };
    let sequence = match get("sequence")? {
        Value::Number(sequence) => sequence as u64,
        _ => {
            return Err(Error::corrupt(
                "intent sequence is not a number".to_string(),
            ));
        }
    };
    let db = match get("db")? {
        Value::String(db) => hex_decode(&db)?,
        _ => return Err(Error::corrupt("intent db is not text".to_string())),
    };
    let alias = match get("alias")? {
        Value::String(alias) => alias,
        _ => return Err(Error::corrupt("intent alias is not text".to_string())),
    };
    let args = get("args")?;
    let identity = match get("identity")? {
        Value::String(identity) => hex_decode(&identity)?,
        _ => return Err(Error::corrupt("intent identity is not text".to_string())),
    };
    let fingerprint = match get("fingerprint")? {
        Value::String(fingerprint) => {
            let bytes = hex_decode(&fingerprint)?;
            bytes.try_into().map_err(|_| {
                Error::corrupt("intent fingerprint has the wrong length".to_string())
            })?
        }
        _ => return Err(Error::corrupt("intent fingerprint is not text".to_string())),
    };
    let expected_revision = match get("expected_revision")? {
        Value::Null => None,
        Value::Number(revision) => Some(revision as u64),
        _ => {
            return Err(Error::corrupt(
                "intent expected revision is not a number".to_string(),
            ));
        }
    };
    let status = decode_status(get("status")?)?;
    let enqueued_ms = match get("enqueued_ms")? {
        Value::Number(ms) => ms as u64,
        _ => {
            return Err(Error::corrupt(
                "intent timestamp is not a number".to_string(),
            ));
        }
    };
    Ok(Intent {
        sequence,
        db,
        alias,
        args,
        identity,
        fingerprint,
        expected_revision,
        status,
        enqueued_ms,
    })
}

/// Encode a lifecycle status with its attachment.
fn encode_status(status: &IntentStatus) -> Result<Value> {
    let mut map = std::collections::BTreeMap::new();
    map.insert(
        "state".to_string(),
        Value::String(status.name().to_string()),
    );
    match status {
        IntentStatus::Committed { revision } => {
            map.insert("revision".to_string(), Value::Number(*revision as f64));
        }
        IntentStatus::Failed { failure } | IntentStatus::Unresolved { failure } => {
            map.insert(
                "outcome".to_string(),
                crate::derived::failure_value(failure),
            );
        }
        _ => {}
    }
    Ok(Value::Object(map))
}

/// Decode a lifecycle status.
fn decode_status(value: Value) -> Result<IntentStatus> {
    let map = match value {
        Value::Object(map) => map,
        _ => return Err(Error::corrupt("intent status is not an object".to_string())),
    };
    let state = match map.get("state") {
        Some(Value::String(state)) => state.as_str(),
        _ => return Err(Error::corrupt("intent status has no state".to_string())),
    };
    match state {
        "queued" => Ok(IntentStatus::Queued),
        "submitted" => Ok(IntentStatus::Submitted),
        "acknowledged" => Ok(IntentStatus::Acknowledged),
        "committed" => match map.get("revision") {
            Some(Value::Number(revision)) => Ok(IntentStatus::Committed {
                revision: *revision as u64,
            }),
            _ => Err(Error::corrupt(
                "committed intent has no revision".to_string(),
            )),
        },
        "failed" | "unresolved" => match map.get("outcome") {
            Some(outcome) => {
                let bytes = crate::json::render(outcome)?;
                let failure = crate::patch::decode_failure(&bytes)?;
                Ok(if state == "unresolved" {
                    IntentStatus::Unresolved { failure }
                } else {
                    IntentStatus::Failed { failure }
                })
            }
            None => Err(Error::corrupt("failed intent has no outcome".to_string())),
        },
        _ => Err(Error::corrupt("unknown intent state".to_string())),
    }
}

/// Decode a stored hex field, rejecting garbage as corruption.
fn hex_decode(text: &str) -> Result<Vec<u8>> {
    crate::hex::decode(text).ok_or_else(|| Error::corrupt("intent field is not hex"))
}

/// Sequence keys sort chronologically as raw bytes.
pub fn sequence_key(sequence: u64) -> [u8; 8] {
    sequence.to_be_bytes()
}

/// Resolve the intent collection and index IDs. Reads tolerate a
/// missing log; writes ensure it first.
pub fn intent_ids(read: &crate::storage::ReadTxn) -> Result<Option<(u32, u32)>> {
    let collection = read.intern_id(INTENT_DB, INTENTS_COLLECTION)?;
    let index = read.intern_id(INTENT_DB, INTENTS_BY_IDENTITY)?;
    match (collection, index) {
        (Some(collection), Some(index)) => Ok(Some((collection, index))),
        (None, None) => Ok(None),
        _ => Err(Error::corrupt("intent log is half initialized".to_string())),
    }
}

/// Intern the intent collection and index, returning their IDs.
pub fn ensure_intent_log(write: &crate::storage::WriteTxn) -> Result<(u32, u32)> {
    let collection = write.intern(INTENT_DB, INTENTS_COLLECTION)?;
    let index = write.intern(INTENT_DB, INTENTS_BY_IDENTITY)?;
    Ok((collection, index))
}

/// Read the allocator and live count.
pub fn intent_counters(read: &crate::storage::ReadTxn) -> Result<(u64, u64)> {
    Ok((
        read.meta_u64(INTENT_DB, INTENT_SEQ)?,
        read.meta_u64(INTENT_DB, INTENT_COUNT)?,
    ))
}

/// Check an enqueue against the bound, rejecting a full log loudly.
pub fn check_bound(count: u64, bound: usize) -> HostResult<()> {
    if count >= bound as u64 {
        return Err(fail(
            "intent_queue_full",
            format!("the intent log holds {count} intents against a bound of {bound}"),
        ));
    }
    Ok(())
}

/// Meta names for the intent counters, for engine writes.
pub fn seq_meta() -> &'static [u8] {
    INTENT_SEQ
}

/// Meta names for the intent counters, for engine writes.
pub fn count_meta() -> &'static [u8] {
    INTENT_COUNT
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Intent {
        Intent {
            sequence: 7,
            db: b"db".to_vec(),
            alias: "app/put".to_string(),
            args: Value::Null,
            identity: vec![1, 2, 3],
            fingerprint: [9u8; HASH_LEN],
            expected_revision: Some(12),
            status: IntentStatus::Committed { revision: 40 },
            enqueued_ms: 1_700_000_000_000,
        }
    }

    #[test]
    fn intents_round_trip() {
        let intent = sample();
        assert_eq!(
            decode_intent(&encode_intent(&intent).unwrap()).unwrap(),
            intent
        );
        let failed = Intent {
            status: IntentStatus::Failed {
                failure: fail("BOOM", "it broke"),
            },
            ..sample()
        };
        assert_eq!(
            decode_intent(&encode_intent(&failed).unwrap()).unwrap(),
            failed
        );
    }

    #[test]
    fn intents_reject_garbage() {
        assert!(decode_intent(b"1").is_err());
        assert!(decode_intent(b"{}").is_err());
    }

    #[test]
    fn transitions_follow_the_lifecycle() {
        let queued = IntentStatus::Queued;
        assert!(queued.can_transition_to(&IntentStatus::Submitted));
        assert!(!queued.can_transition_to(&IntentStatus::Committed { revision: 1 }));
        assert!(!queued.can_transition_to(&IntentStatus::Queued));
        let committed = IntentStatus::Committed { revision: 1 };
        assert!(!committed.can_transition_to(&IntentStatus::Queued));
        let submitted = IntentStatus::Submitted;
        assert!(submitted.can_transition_to(&IntentStatus::Queued));
    }

    #[test]
    fn hex_fields_reject_garbage() {
        assert_eq!(hex_decode("00abff").unwrap(), vec![0x00, 0xAB, 0xFF]);
        assert!(hex_decode("abc").is_err());
        assert!(hex_decode("zz").is_err());
    }
}
