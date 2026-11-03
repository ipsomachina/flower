//! Retry identities, receipts and replay decisions.
//!
//! Scoped identities (`incarnation`, `epoch`, intent bytes) address the
//! `receipts` table; the stored receipt carries the committed result, the
//! intent fingerprint and floor metadata. [`admit`] is the pure
//! admit/replay/reject decision: current authorization precedes replay,
//! wrong history and retired epochs reject without execution, expired
//! receipts read as unavailable history (execute afresh), and a reused
//! identity with a different fingerprint is a conflict. Unscoped caller IDs
//! address the same table under [`UNSCOPED_PREFIX`]: first result wins
//! until [`IDEMPOTENCY_TTL_MS`] past the call timestamp, with no
//! fingerprint conflict check. [`UnscopedDedup`] is the bounded in-memory
//! fast path in front of the table; work can execute again for keys that
//! expired or were purged.

use crate::error::{Error, Result};
use crate::storage::keys::{Reader, push_sized};

/// Length of a SHA-256 fingerprint or content hash.
pub const HASH_LEN: usize = 32;

/// Receipt-key namespace for unscoped caller IDs: prefix plus the raw
/// key. Scoped identities keep their bare encoding; the prefix keeps
/// the namespaces apart. A hostile caller could still craft a scoped
/// identity whose encoding collides, but scoped identities already
/// assume cooperative callers.
pub const UNSCOPED_PREFIX: &[u8] = b"unscoped\x00";

/// How long an unscoped idempotency receipt replays: 24 hours past
/// the call timestamp. Past expiry the key misses and the work
/// executes afresh; purging reclaims the rows.
pub const IDEMPOTENCY_TTL_MS: u64 = 86_400_000;

/// Namespace a caller idempotency key for the receipts table.
pub fn unscoped_identity_key(key: &[u8]) -> Vec<u8> {
    let mut namespaced = Vec::with_capacity(UNSCOPED_PREFIX.len() + key.len());
    namespaced.extend_from_slice(UNSCOPED_PREFIX);
    namespaced.extend_from_slice(key);
    namespaced
}

/// Stored-result tag: successful value, canonical JSON bytes.
pub const RESULT_VALUE: u8 = 0x00;
/// Stored-result tag: business failure.
pub const RESULT_FAILURE: u8 = 0x01;
/// Stored-result tag: released by session acknowledgement. The floor stays,
/// so the identity can neither replay a result nor execute again.
pub const RESULT_RELEASED: u8 = 0x02;

/// A scoped retry identity: history incarnation, session epoch and the
/// caller's stable intent bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScopedIdentity {
    /// History incarnation the intent was written against.
    pub incarnation: u64,
    /// Session epoch the intent was written against.
    pub epoch: u64,
    /// Stable intent bytes chosen by the caller.
    pub intent: Vec<u8>,
}

impl ScopedIdentity {
    /// Encode the identity bytes stored after the database prefix:
    /// incarnation, epoch, length-prefixed intent.
    pub fn encode(&self) -> Result<Vec<u8>> {
        let len =
            u32::try_from(self.intent.len()).map_err(|_| Error::corrupt("intent too long"))?;
        let mut encoded = Vec::with_capacity(self.intent.len() + 20);
        encoded.extend_from_slice(&self.incarnation.to_be_bytes());
        encoded.extend_from_slice(&self.epoch.to_be_bytes());
        encoded.extend_from_slice(&len.to_be_bytes());
        encoded.extend_from_slice(&self.intent);
        Ok(encoded)
    }

    /// Decode identity bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(bytes);
        let incarnation = read_u64(&mut reader)?;
        let epoch = read_u64(&mut reader)?;
        let intent = reader.sized()?;
        if reader.pos() != bytes.len() {
            return Err(Error::corrupt("bad identity length"));
        }
        Ok(ScopedIdentity {
            incarnation,
            epoch,
            intent,
        })
    }
}

/// A stored business failure: code, message and optional JSON details.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FailureData {
    /// Stable machine-readable code.
    pub code: String,
    /// Human-readable message.
    pub message: String,
    /// Optional canonical JSON details.
    pub details: Option<Vec<u8>>,
}

/// A stored result: value or business failure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// Successful value, canonical JSON bytes.
    Value(Vec<u8>),
    /// Business failure.
    Failure(FailureData),
}

/// Encode a result blob: tag plus payload. Shared by receipts and intents.
pub fn encode_outcome_blob(outcome: &Outcome) -> Result<Vec<u8>> {
    let mut encoded = Vec::new();
    match outcome {
        Outcome::Value(bytes) => {
            encoded.push(RESULT_VALUE);
            push_sized(&mut encoded, bytes)?;
        }
        Outcome::Failure(failure) => {
            encoded.push(RESULT_FAILURE);
            push_sized(&mut encoded, failure.code.as_bytes())?;
            push_sized(&mut encoded, failure.message.as_bytes())?;
            match &failure.details {
                None => encoded.push(0x00),
                Some(details) => {
                    encoded.push(0x01);
                    push_sized(&mut encoded, details)?;
                }
            }
        }
    }
    Ok(encoded)
}

/// Decode a result blob, returning the outcome and bytes consumed.
pub fn decode_outcome_blob(bytes: &[u8]) -> Result<(Outcome, usize)> {
    let mut reader = Reader::new(bytes);
    let tag = reader.byte()?;
    let outcome = match tag {
        RESULT_VALUE => Outcome::Value(reader.sized()?),
        RESULT_FAILURE => {
            let code = read_string(&mut reader)?;
            let message = read_string(&mut reader)?;
            let details = match reader.byte()? {
                0x00 => None,
                0x01 => Some(reader.sized()?),
                _ => return Err(Error::corrupt("bad details flag")),
            };
            Outcome::Failure(FailureData {
                code,
                message,
                details,
            })
        }
        _ => return Err(Error::corrupt("unknown outcome tag")),
    };
    Ok((outcome, reader.pos()))
}

fn read_string(reader: &mut Reader) -> Result<String> {
    let bytes = reader.sized()?;
    String::from_utf8(bytes).map_err(|_| Error::corrupt("outcome text is not UTF-8"))
}

fn read_u64(reader: &mut Reader) -> Result<u64> {
    let hi = reader.u32()? as u64;
    let lo = reader.u32()? as u64;
    Ok((hi << 32) | lo)
}

/// Receipt trailer tag: the revision and log position the receipt's
/// outcome committed at follow.
const TRAILER_COMMITTED: u8 = 0x01;

/// Where a receipt's outcome committed: the database revision it left
/// (unchanged by a business failure, which bumps none) and the log
/// position of its entry. Recorded at apply, the one place both are
/// known, so every member, synced device and restore replay records
/// the same values, and a replay answers with them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Committed {
    /// Database revision after the outcome committed.
    pub revision: u64,
    /// Log position of the entry that committed it.
    pub position: u64,
}

/// A stored receipt: committed result, intent fingerprint and floor
/// metadata, and where the result committed. A `None` result is a
/// released receipt: the floor stays so the identity can neither replay
/// nor execute again.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Receipt {
    /// Committed result, or `None` once session acknowledgement releases it.
    pub result: Option<Outcome>,
    /// Fingerprint over business intent plus explicit preconditions —
    /// never refreshed credentials.
    pub fingerprint: [u8; HASH_LEN],
    /// Retention floor this receipt counts toward.
    pub floor: u64,
    /// Expiry instant, or zero for no expiry. Expired receipts read as
    /// unavailable history, not as proof the work failed.
    pub expires_at: u64,
    /// Where the result committed: stamped at apply. `None` for receipts
    /// as prepared (before apply, or staged in a writer's scratch), for
    /// local-only databases, which have no log, and for receipts written
    /// before stamping existed, which replay with the database's
    /// current revision and no position.
    pub committed: Option<Committed>,
}

impl Receipt {
    /// Encode the receipt value: result blob (or the released tag),
    /// fingerprint, floor, expiry, then, once stamped, the committed
    /// trailer (tag, revision, position).
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoded = match &self.result {
            None => vec![RESULT_RELEASED],
            Some(outcome) => encode_outcome_blob(outcome)?,
        };
        encoded.extend_from_slice(&self.fingerprint);
        encoded.extend_from_slice(&self.floor.to_be_bytes());
        encoded.extend_from_slice(&self.expires_at.to_be_bytes());
        if let Some(committed) = self.committed {
            encoded.push(TRAILER_COMMITTED);
            encoded.extend_from_slice(&committed.revision.to_be_bytes());
            encoded.extend_from_slice(&committed.position.to_be_bytes());
        }
        Ok(encoded)
    }

    /// Re-encode a receipt value with where its result committed.
    pub fn stamp(bytes: &[u8], committed: Committed) -> Result<Vec<u8>> {
        let mut receipt = Receipt::decode(bytes)?;
        receipt.committed = Some(committed);
        receipt.encode()
    }

    /// Decode a receipt value.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.is_empty() {
            return Err(Error::corrupt("truncated receipt"));
        }
        let (result, pos) = match bytes[0] {
            RESULT_RELEASED => (None, 1),
            RESULT_VALUE | RESULT_FAILURE => {
                let (outcome, consumed) = decode_outcome_blob(bytes)?;
                (Some(outcome), consumed)
            }
            _ => return Err(Error::corrupt("unknown receipt tag")),
        };
        // Receipts written before stamping end at the expiry.
        let rest = bytes.get(pos..).unwrap_or(&[]);
        let stamped = HASH_LEN + 16 + 1 + 16;
        if rest.len() != HASH_LEN + 16 && rest.len() != stamped {
            return Err(Error::corrupt("bad receipt tail"));
        }
        let mut fingerprint = [0u8; HASH_LEN];
        fingerprint.copy_from_slice(&rest[..HASH_LEN]);
        let mut tail = Reader::new(&rest[HASH_LEN..]);
        let floor = read_u64(&mut tail)?;
        let expires_at = read_u64(&mut tail)?;
        let committed = if rest.len() == stamped {
            if rest[HASH_LEN + 16] != TRAILER_COMMITTED {
                return Err(Error::corrupt("unknown receipt trailer"));
            }
            let mut trailer = Reader::new(&rest[HASH_LEN + 17..]);
            Some(Committed {
                revision: read_u64(&mut trailer)?,
                position: read_u64(&mut trailer)?,
            })
        } else {
            None
        };
        Ok(Receipt {
            result,
            fingerprint,
            floor,
            expires_at,
            committed,
        })
    }

    /// Whether the receipt has expired at `now`. Zero never expires.
    pub fn expired(&self, now: u64) -> bool {
        self.expires_at != 0 && now >= self.expires_at
    }
}

/// Why an identity was rejected without execution.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RejectReason {
    /// Current authorization failed; checked before any replay.
    Unauthorized,
    /// The identity belongs to a different history incarnation.
    WrongHistory,
    /// The identity's epoch is below the retention floor.
    Retired,
    /// The identity was reused with a different fingerprint.
    Conflict,
    /// Session acknowledgement released the result; the floor forbids
    /// both replay and re-execution.
    Released,
}

/// The admit decision for one scoped identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Admit {
    /// A matching receipt exists: return the authorized original result.
    Replay(Receipt),
    /// Admissible and new (or expired): evaluate and commit with a receipt.
    Execute,
    /// Reject without execution.
    Reject(RejectReason),
}

/// Decide admission for a scoped identity against the database's history
/// (its incarnation and those it carried in through moves), retention
/// floor, clock, authorization state and any stored receipt.
pub fn admit(
    history: &crate::placement::History,
    floor: u64,
    now: u64,
    authorized: bool,
    identity: &ScopedIdentity,
    fingerprint: &[u8; HASH_LEN],
    stored: Option<&Receipt>,
) -> Admit {
    if !authorized {
        return Admit::Reject(RejectReason::Unauthorized);
    }
    if !history.holds(identity.incarnation) {
        return Admit::Reject(RejectReason::WrongHistory);
    }
    if identity.epoch < floor {
        return Admit::Reject(RejectReason::Retired);
    }
    let Some(receipt) = stored else {
        return Admit::Execute;
    };
    if receipt.expired(now) {
        return Admit::Execute;
    }
    match &receipt.result {
        None => Admit::Reject(RejectReason::Released),
        Some(_) if receipt.fingerprint != *fingerprint => Admit::Reject(RejectReason::Conflict),
        Some(_) => Admit::Replay(receipt.clone()),
    }
}

/// Bounded in-memory deduplication for unscoped caller IDs: the fast
/// path in front of the durable receipts table. Eviction is
/// oldest-inserted-first, and entries expire at their stored horizon;
/// work can execute again after collection, expiry or purge.
#[derive(Clone, Debug, Default)]
pub struct UnscopedDedup {
    capacity: usize,
    order: std::collections::VecDeque<Vec<u8>>,
    results: std::collections::HashMap<Vec<u8>, (Vec<u8>, u64)>,
}

impl UnscopedDedup {
    /// Create a deduplicator retaining at most `capacity` results.
    /// Zero capacity retains nothing.
    pub fn new(capacity: usize) -> Self {
        UnscopedDedup {
            capacity,
            order: std::collections::VecDeque::new(),
            results: std::collections::HashMap::new(),
        }
    }

    /// Look up a retained result by caller ID, missing on expiry.
    /// Expired rows stay until eviction reclaims their slots.
    pub fn get(&self, id: &[u8], now: u64) -> Option<&Vec<u8>> {
        let (result, expires_at) = self.results.get(id)?;
        if *expires_at != 0 && now >= *expires_at {
            return None;
        }
        Some(result)
    }

    /// Retain a result until `expires_at`, evicting oldest entries
    /// beyond capacity. Re-inserting an ID refreshes its result
    /// without moving its slot.
    pub fn insert(&mut self, id: &[u8], result: Vec<u8>, expires_at: u64) {
        if self.capacity == 0 {
            return;
        }
        if !self.results.contains_key(id) {
            self.order.push_back(id.to_vec());
        }
        self.results.insert(id.to_vec(), (result, expires_at));
        while self.results.len() > self.capacity {
            if let Some(oldest) = self.order.pop_front() {
                self.results.remove(&oldest);
            } else {
                break;
            }
        }
    }

    /// Drop one retained result, pruning its eviction slot so
    /// insert/remove cycles cannot grow the queue without bound.
    pub fn remove(&mut self, id: &[u8]) {
        if self.results.remove(id).is_some() {
            self.order.retain(|queued| queued.as_slice() != id);
        }
    }

    /// Drop every retained result whose ID starts with `scope`, with
    /// its eviction slot.
    pub fn forget_scope(&mut self, scope: &[u8]) {
        self.results.retain(|id, _| !id.starts_with(scope));
        self.order.retain(|id| !id.starts_with(scope));
    }

    /// Retained entry count.
    pub fn len(&self) -> usize {
        self.results.len()
    }

    /// Whether nothing is retained.
    pub fn is_empty(&self) -> bool {
        self.results.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity() -> ScopedIdentity {
        ScopedIdentity {
            incarnation: 3,
            epoch: 40,
            intent: b"intent-1".to_vec(),
        }
    }

    fn receipt() -> Receipt {
        Receipt {
            result: Some(Outcome::Value(b"{}".to_vec())),
            fingerprint: [9u8; HASH_LEN],
            floor: 40,
            expires_at: 0,
            committed: None,
        }
    }

    #[test]
    fn identities_round_trip() {
        let encoded = identity().encode().unwrap();
        assert_eq!(ScopedIdentity::decode(&encoded).unwrap(), identity());
        assert!(ScopedIdentity::decode(&encoded[..10]).is_err());
        let mut long = encoded.clone();
        long.push(0x00);
        assert!(ScopedIdentity::decode(&long).is_err());
    }

    #[test]
    fn outcomes_round_trip() {
        for outcome in [
            Outcome::Value(b"{}".to_vec()),
            Outcome::Failure(FailureData {
                code: "BUSY".to_string(),
                message: "try again".to_string(),
                details: Some(b"{}".to_vec()),
            }),
            Outcome::Failure(FailureData {
                code: "BUSY".to_string(),
                message: "try again".to_string(),
                details: None,
            }),
        ] {
            let encoded = encode_outcome_blob(&outcome).unwrap();
            let (decoded, consumed) = decode_outcome_blob(&encoded).unwrap();
            assert_eq!(consumed, encoded.len());
            assert_eq!(decoded, outcome);
        }
        assert!(decode_outcome_blob(&[0x09]).is_err());
        assert!(decode_outcome_blob(&[]).is_err());
    }

    #[test]
    fn receipts_round_trip_with_release() {
        let encoded = receipt().encode().unwrap();
        assert_eq!(Receipt::decode(&encoded).unwrap(), receipt());
        let released = Receipt {
            result: None,
            ..receipt()
        };
        let encoded = released.encode().unwrap();
        assert_eq!(Receipt::decode(&encoded).unwrap(), released);
        assert!(Receipt::decode(&[]).is_err());
        assert!(Receipt::decode(&encoded[..encoded.len() - 1]).is_err());
    }

    #[test]
    fn stamped_receipts_round_trip_and_unstamped_ones_still_decode() {
        let committed = Committed {
            revision: 17,
            position: 4_242,
        };
        // A receipt written before stamping existed: the old encoding,
        // byte for byte, decodes with nothing committed.
        let old = receipt().encode().unwrap();
        assert!(
            old.ends_with(&0u64.to_be_bytes()),
            "the old tail ends at the expiry"
        );
        assert_eq!(Receipt::decode(&old).unwrap().committed, None);
        // Stamping appends the trailer and keeps everything else.
        let stamped = Receipt::stamp(&old, committed).unwrap();
        assert_eq!(&stamped[..old.len()], &old[..]);
        assert_eq!(stamped.len(), old.len() + 17);
        let decoded = Receipt::decode(&stamped).unwrap();
        assert_eq!(decoded.committed, Some(committed));
        assert_eq!(
            Receipt {
                committed: None,
                ..decoded.clone()
            },
            receipt()
        );
        // Released receipts keep their stamp too.
        let released = Receipt {
            result: None,
            ..decoded
        };
        assert_eq!(
            Receipt::decode(&released.encode().unwrap()).unwrap(),
            released
        );
        // A truncated or retagged trailer is corruption, not a miss.
        assert!(Receipt::decode(&stamped[..stamped.len() - 1]).is_err());
        let mut retagged = stamped.clone();
        retagged[old.len()] = 0x7f;
        assert!(Receipt::decode(&retagged).is_err());
    }

    /// The history of a database at `incarnation` that never moved.
    fn at(incarnation: u64) -> crate::placement::History {
        crate::placement::History {
            incarnation,
            earlier: Vec::new(),
        }
    }

    #[test]
    fn admit_covers_the_matrix() {
        let id = identity();
        let fp = [9u8; HASH_LEN];
        let stored = receipt();
        // Replay path.
        assert_eq!(
            admit(&at(3), 40, 100, true, &id, &fp, Some(&stored)),
            Admit::Replay(stored.clone())
        );
        // Authorization precedes replay.
        assert_eq!(
            admit(&at(3), 40, 100, false, &id, &fp, Some(&stored)),
            Admit::Reject(RejectReason::Unauthorized)
        );
        // Wrong history, with or without a receipt.
        assert_eq!(
            admit(&at(4), 40, 100, true, &id, &fp, Some(&stored)),
            Admit::Reject(RejectReason::WrongHistory)
        );
        assert_eq!(
            admit(&at(4), 40, 100, true, &id, &fp, None),
            Admit::Reject(RejectReason::WrongHistory)
        );
        // An identity of the history the database moved in with stays
        // its own.
        let moved = crate::placement::History {
            incarnation: 4,
            earlier: vec![3],
        };
        assert_eq!(
            admit(&moved, 40, 100, true, &id, &fp, Some(&stored)),
            Admit::Replay(stored.clone())
        );
        // Retired epoch.
        assert_eq!(
            admit(&at(3), 41, 100, true, &id, &fp, None),
            Admit::Reject(RejectReason::Retired)
        );
        // Fresh identity executes.
        assert_eq!(admit(&at(3), 40, 100, true, &id, &fp, None), Admit::Execute);
        // Expired receipts read as unavailable history.
        let mut expired = stored.clone();
        expired.expires_at = 100;
        assert_eq!(
            admit(&at(3), 40, 100, true, &id, &fp, Some(&expired)),
            Admit::Execute
        );
        // Released identities can neither replay nor execute.
        let released = Receipt {
            result: None,
            ..stored.clone()
        };
        assert_eq!(
            admit(&at(3), 40, 100, true, &id, &fp, Some(&released)),
            Admit::Reject(RejectReason::Released)
        );
        // Reused identity with a different fingerprint conflicts.
        assert_eq!(
            admit(&at(3), 40, 100, true, &id, &[1u8; HASH_LEN], Some(&stored)),
            Admit::Reject(RejectReason::Conflict)
        );
    }

    #[test]
    fn unscoped_dedup_evicts_oldest() {
        let mut dedup = UnscopedDedup::new(2);
        assert!(dedup.is_empty());
        dedup.insert(b"a", b"1".to_vec(), 0);
        dedup.insert(b"b", b"2".to_vec(), 0);
        assert_eq!(dedup.len(), 2);
        dedup.insert(b"c", b"3".to_vec(), 0);
        assert_eq!(dedup.len(), 2);
        assert_eq!(dedup.get(b"a", 0), None);
        assert_eq!(dedup.get(b"c", 0), Some(&b"3".to_vec()));
        // Refreshing keeps the slot but updates the result.
        dedup.insert(b"b", b"9".to_vec(), 0);
        assert_eq!(dedup.get(b"b", 0), Some(&b"9".to_vec()));
        // Zero capacity retains nothing.
        let mut none = UnscopedDedup::new(0);
        none.insert(b"a", b"1".to_vec(), 0);
        assert!(none.is_empty());
        // Entries miss past their horizon, and removal prunes slots.
        dedup.insert(b"e", b"4".to_vec(), 100);
        assert_eq!(dedup.get(b"e", 99), Some(&b"4".to_vec()));
        assert_eq!(dedup.get(b"e", 100), None);
        dedup.remove(b"e");
        assert_eq!(dedup.get(b"e", 0), None);
        dedup.remove(b"missing");
    }
}
