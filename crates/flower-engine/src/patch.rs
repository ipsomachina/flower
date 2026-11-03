//! Accepted patches: the ordered effects Raft replicates.
//!
//! A leader evaluates a mutation against a snapshot, runs the commit
//! rounds to a fixpoint and encodes the staged journal as a patch instead
//! of committing it. The patch replicates through Raft; every member
//! applies it atomically in log order. Followers never re-execute guest
//! code: the patch carries the leader's record writes, index maintenance,
//! derived outcomes with their edge replacements, evictions, the receipt
//! and the result.
//!
//! Log order keeps appliers version-consistent: activation entries and
//! patches share one log. The patch carries every module version
//! deployed in its database at the prepare snapshot; when a deployment
//! committed between prepare and apply, the versions disagree and every
//! applier rejects the patch deterministically as `stale_revision`, so
//! the submitter re-prepares against the new code.
//!
//! Revision and log position are assigned at apply, not at prepare:
//! several prepares may be in flight while apply stays serial. The
//! applier asserts the entry's position advances its applied log and
//! bumps the database revision on success only. Failure patches persist
//! their receipt without a revision bump, so retries replay the failure
//! on every member. An optional expected revision rejects stale
//! submissions deterministically: every applier observes the same
//! revision at the same log position, so all reject identically.
//!
//! Derived caches are not guarded that way: the patch records the
//! revision it was prepared against, and its derived outcomes install
//! only when the database still sits at that revision. Otherwise they
//! were computed over a snapshot later commits changed, so they drop
//! (keeping pins) and re-evaluate on demand. Either way the applier
//! re-runs invalidation for the journal against the committed edges,
//! which may hold outcomes cached after the prepare snapshot.

use flower_sdk::Value;

/// Code of a transaction whose committed effects outgrow
/// [`max_transaction_bytes`]: HTTP 413, as v1's `RESULT_TOO_LARGE`.
pub const TRANSACTION_TOO_LARGE: &str = "transaction_too_large";
/// Default bound on one transaction's encoded effects: 32 MiB, as v1's
/// `FLOWER_TRANSACTION_MAX_BYTES`.
pub const DEFAULT_MAX_TRANSACTION_BYTES: usize = 32 * 1024 * 1024;

static MAX_TRANSACTION_BYTES: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(DEFAULT_MAX_TRANSACTION_BYTES);

/// Bound every transaction's encoded effects (its replicated entry) for
/// this process. Only the size of what commits bounds a transaction: no
/// count of rows, steps or planned calls does. The serving process sets
/// it once, so that an entry fits the peer frame.
pub fn set_max_transaction_bytes(bytes: usize) {
    MAX_TRANSACTION_BYTES.store(bytes, std::sync::atomic::Ordering::Relaxed);
}

/// The bound [`set_max_transaction_bytes`] set.
pub fn max_transaction_bytes() -> usize {
    MAX_TRANSACTION_BYTES.load(std::sync::atomic::Ordering::Relaxed)
}

/// The peer frame an entry of `max_entry` bytes needs: its JSON encoding
/// (at most four times it) plus 1 MiB of framing. Every member's inbound
/// frame must be this large for what any leader proposes.
pub fn frame_for_entry(max_entry: u64) -> u64 {
    max_entry.saturating_mul(4).saturating_add(1 << 20)
}

/// Refuse a transaction whose encoded effects are `bytes` long when that
/// passes the bound, saying both sizes.
pub fn check_transaction_size(bytes: usize) -> std::result::Result<(), flower_sdk::Failure> {
    let max = max_transaction_bytes();
    if bytes > max {
        return Err(flower_sdk::fail(
            TRANSACTION_TOO_LARGE,
            format!(
                "the transaction's effects encode to {bytes} bytes, past the {max}-byte bound \
                 (serve --max-transaction-bytes)"
            ),
        ));
    }
    Ok(())
}

use crate::derived::{IndexChange, RecordChange};
use crate::error::{Error, Result};
use crate::storage::WriteTxn;
use crate::storage::codec::{DerivedOutcome, decode_outcome};
use crate::storage::keys::{HASH_LEN, Reader, push_count, push_sized, push_u32, push_u64};

/// Patch tag: successful mutation with a value result.
const TAG_VALUE: u8 = 0x01;
/// Patch tag: business failure with a receipt and no revision bump.
const TAG_FAILURE: u8 = 0x02;

/// One record write: upsert (`Some`) or delete (`None`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordWrite {
    /// Interned collection ID.
    pub collection: u32,
    /// Record key bytes.
    pub key: Vec<u8>,
    /// Canonical JSON bytes, or `None` for delete.
    pub value: Option<Vec<u8>>,
}

/// One index entry write.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexWrite {
    /// Interned index ID.
    pub index: u32,
    /// Index component bytes.
    pub components: Vec<Vec<u8>>,
    /// Primary key bytes.
    pub primary: Vec<u8>,
    /// Present or removed.
    pub present: bool,
}

/// One derived outcome write with its edge replacement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DerivedWrite {
    /// Interned definition ID.
    pub definition: u32,
    /// Canonical argument bytes.
    pub args: Vec<u8>,
    /// Encoded outcome bytes.
    pub outcome: Vec<u8>,
    /// Observed dependency keys, or `None` to retain the old edges.
    pub observed: Option<Vec<Vec<u8>>>,
}

/// An accepted patch: ordered effects plus result metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Patch {
    /// True for a business-failure patch: receipt only, no revision bump.
    pub failed: bool,
    /// Database identifier bytes.
    pub db: Vec<u8>,
    /// Every `(module, code identity)` deployed in the database at the
    /// prepare snapshot, sorted by module. Appliers reject the patch as
    /// stale when the set differs; empty for patches built without
    /// guest code (timer retries, planned queue and timer writes), which
    /// pin nothing.
    pub versions: Vec<(String, [u8; HASH_LEN])>,
    /// Record writes in journal order.
    pub records: Vec<RecordWrite>,
    /// Index writes in journal order.
    pub indexes: Vec<IndexWrite>,
    /// Derived outcomes with edge replacements.
    pub derived: Vec<DerivedWrite>,
    /// Derived evictions as `(definition, args)`.
    pub drops: Vec<(u32, Vec<u8>)>,
    /// Receipt `(identity, encoded)` pairs for retry identities.
    pub receipts: Vec<(Vec<u8>, Vec<u8>)>,
    /// Canonical JSON result, or the encoded failure for failed patches.
    pub result: Vec<u8>,
    /// Revision the submission expected, rejected when stale.
    pub expected_revision: Option<u64>,
    /// Committed revision of the snapshot the patch was prepared
    /// against: its derived outcomes install only at this revision.
    pub base_revision: u64,
}

impl Patch {
    /// The stored records its journal puts and deletes: every record
    /// write, queue and timer rows included; index entries and derived
    /// outcomes are maintained from them and not counted.
    pub fn record_counts(&self) -> (u64, u64) {
        let puts = self
            .records
            .iter()
            .filter(|record| record.value.is_some())
            .count() as u64;
        (puts, self.records.len() as u64 - puts)
    }
}

/// Encode a patch: tag, database, versions, journal, receipt, result.
pub fn encode_patch(patch: &Patch) -> Result<Vec<u8>> {
    let mut encoded = Vec::new();
    encoded.push(if patch.failed { TAG_FAILURE } else { TAG_VALUE });
    push_sized(&mut encoded, &patch.db)?;
    push_count(&mut encoded, patch.versions.len())?;
    for (module, identity) in &patch.versions {
        push_sized(&mut encoded, module.as_bytes())?;
        encoded.extend_from_slice(identity);
    }
    push_count(&mut encoded, patch.records.len())?;
    for record in &patch.records {
        push_u32(&mut encoded, record.collection);
        push_sized(&mut encoded, &record.key)?;
        match &record.value {
            Some(json) => {
                encoded.push(1);
                push_sized(&mut encoded, json)?;
            }
            None => encoded.push(0),
        }
    }
    push_count(&mut encoded, patch.indexes.len())?;
    for entry in &patch.indexes {
        push_u32(&mut encoded, entry.index);
        push_count(&mut encoded, entry.components.len())?;
        for component in &entry.components {
            push_sized(&mut encoded, component)?;
        }
        push_sized(&mut encoded, &entry.primary)?;
        encoded.push(u8::from(entry.present));
    }
    push_count(&mut encoded, patch.derived.len())?;
    for derived in &patch.derived {
        push_u32(&mut encoded, derived.definition);
        push_sized(&mut encoded, &derived.args)?;
        push_sized(&mut encoded, &derived.outcome)?;
        match &derived.observed {
            Some(edges) => {
                encoded.push(1);
                push_count(&mut encoded, edges.len())?;
                for edge in edges {
                    push_sized(&mut encoded, edge)?;
                }
            }
            None => encoded.push(0),
        }
    }
    push_count(&mut encoded, patch.drops.len())?;
    for (definition, args) in &patch.drops {
        push_u32(&mut encoded, *definition);
        push_sized(&mut encoded, args)?;
    }
    push_count(&mut encoded, patch.receipts.len())?;
    for (identity, receipt) in &patch.receipts {
        push_sized(&mut encoded, identity)?;
        push_sized(&mut encoded, receipt)?;
    }
    push_sized(&mut encoded, &patch.result)?;
    match patch.expected_revision {
        Some(revision) => {
            encoded.push(1);
            push_u64(&mut encoded, revision);
        }
        None => encoded.push(0),
    }
    push_u64(&mut encoded, patch.base_revision);
    Ok(encoded)
}

/// Decode a patch, rejecting unknown tags and trailing bytes.
pub fn decode_patch(bytes: &[u8]) -> Result<Patch> {
    let mut reader = Reader::new(bytes);
    let failed = match reader.byte()? {
        TAG_VALUE => false,
        TAG_FAILURE => true,
        _ => return Err(Error::corrupt("unknown patch tag".to_string())),
    };
    let db = reader.sized()?;
    let versions = read_bounded(&mut reader, "patch versions", |reader| {
        let module = String::from_utf8(reader.sized()?)
            .map_err(|_| Error::corrupt("patch module is not UTF-8".to_string()))?;
        let identity: [u8; HASH_LEN] = reader
            .bytes(HASH_LEN)?
            .try_into()
            .map_err(|_| Error::corrupt("truncated patch identity".to_string()))?;
        Ok((module, identity))
    })?;
    let records = read_bounded(&mut reader, "patch records", |reader| {
        let collection = reader.u32()?;
        let key = reader.sized()?;
        let value = match reader.byte()? {
            0 => None,
            1 => Some(reader.sized()?),
            _ => return Err(Error::corrupt("bad patch record flag".to_string())),
        };
        Ok(RecordWrite {
            collection,
            key,
            value,
        })
    })?;
    let indexes = read_bounded(&mut reader, "patch indexes", |reader| {
        let index = reader.u32()?;
        let components = read_bounded(reader, "patch components", |reader| reader.sized())?;
        let primary = reader.sized()?;
        let present = match reader.byte()? {
            0 => false,
            1 => true,
            _ => return Err(Error::corrupt("bad patch entry flag".to_string())),
        };
        Ok(IndexWrite {
            index,
            components,
            primary,
            present,
        })
    })?;
    let derived = read_bounded(&mut reader, "patch derived", |reader| {
        let definition = reader.u32()?;
        let args = reader.sized()?;
        let outcome = reader.sized()?;
        let observed = match reader.byte()? {
            0 => None,
            1 => Some(read_bounded(reader, "patch edges", |reader| {
                reader.sized()
            })?),
            _ => return Err(Error::corrupt("bad patch edges flag".to_string())),
        };
        Ok(DerivedWrite {
            definition,
            args,
            outcome,
            observed,
        })
    })?;
    let drops = read_bounded(&mut reader, "patch drops", |reader| {
        Ok((reader.u32()?, reader.sized()?))
    })?;
    let receipts = read_bounded(&mut reader, "patch receipts", |reader| {
        Ok((reader.sized()?, reader.sized()?))
    })?;
    let result = reader.sized()?;
    let expected_revision = match reader.byte()? {
        0 => None,
        1 => Some(reader.u64()?),
        _ => return Err(Error::corrupt("bad patch revision flag".to_string())),
    };
    let base_revision = reader.u64()?;
    if reader.pos() != bytes.len() {
        return Err(Error::corrupt("trailing patch bytes".to_string()));
    }
    Ok(Patch {
        failed,
        db,
        versions,
        records,
        indexes,
        derived,
        drops,
        receipts,
        result,
        expected_revision,
        base_revision,
    })
}

/// A planned change to engine-owned records (queue items, timers): the
/// operation's answer plus the record writes carrying it out, computed
/// from committed state without committing anything. Callers commit the
/// writes directly or propose them as a patch's records, guarded by
/// `revision` when the plan must not apply over newer commits.
#[derive(Clone, Debug)]
pub struct Planned<T> {
    /// The operation's answer, as the direct-commit call returns it.
    pub outcome: T,
    /// Record writes in apply order.
    pub writes: Vec<RecordWrite>,
    /// Committed revision of the snapshot the plan read: the expected
    /// revision for a guarded patch.
    pub revision: u64,
}

/// Apply planned record writes in order.
pub(crate) fn apply_records(write: &WriteTxn, db: &[u8], records: &[RecordWrite]) -> Result<()> {
    for record in records {
        apply_record(
            write,
            db,
            record.collection,
            &record.key,
            record.value.as_deref(),
        )?;
    }
    Ok(())
}

/// Apply one record write or deletion.
pub(crate) fn apply_record(
    write: &WriteTxn,
    db: &[u8],
    collection: u32,
    key: &[u8],
    value: Option<&[u8]>,
) -> Result<()> {
    match value {
        Some(json) => write.put_record(db, collection, key, json),
        None => write.delete_record(db, collection, key).map(drop),
    }
}

/// Apply one index entry insert or deletion.
pub(crate) fn apply_index(
    write: &WriteTxn,
    db: &[u8],
    index: u32,
    components: &[Vec<u8>],
    primary: &[u8],
    present: bool,
) -> Result<()> {
    let refs: Vec<&[u8]> = components.iter().map(Vec::as_slice).collect();
    if present {
        write.put_index_entry(db, index, &refs, primary)
    } else {
        write
            .delete_index_entry(db, index, &refs, primary)
            .map(drop)
    }
}

/// Store one derived outcome, replacing its edges when it observed any.
pub(crate) fn install_derived(
    write: &WriteTxn,
    db: &[u8],
    definition: u32,
    args: &[u8],
    outcome: &DerivedOutcome,
    observed: Option<&[Vec<u8>]>,
) -> Result<()> {
    write.put_derived(db, definition, args, outcome)?;
    if let Some(observed) = observed {
        write.replace_edges(db, definition, args, observed)?;
    }
    Ok(())
}

/// Apply a patch's journal to an open write transaction without
/// committing it: records and index entries, then derived maintenance.
/// Invalidation re-runs against the committed edges first, then the
/// patch's evictions apply; its derived outcomes install only when
/// `fresh` (the database still sits at the patch's base revision) and
/// otherwise drop, keeping pins. Receipts, revisions and positions are
/// the caller's.
pub(crate) fn apply_journal(write: &WriteTxn, patch: &Patch, fresh: bool) -> Result<()> {
    let db = patch.db.as_slice();
    let staged = crate::staging::StagedIndexes::load(write, db)?;
    for record in &patch.records {
        let value = record.value.as_deref();
        staged.maintain(write, db, record.collection, &record.key, value)?;
        apply_record(write, db, record.collection, &record.key, value)?;
    }
    for entry in &patch.indexes {
        apply_index(
            write,
            db,
            entry.index,
            &entry.components,
            &entry.primary,
            entry.present,
        )?;
    }
    // Queue, timer and task rows are never read through dependency-
    // recording operations, so they invalidate nothing: planned queue,
    // timer and task patches skip the edge scan entirely.
    let mut unread = Vec::new();
    for name in [
        crate::host::QUEUE_COLLECTION,
        crate::host::TIMER_COLLECTION,
        crate::host::TASK_COLLECTION,
    ] {
        unread.extend(write.intern_id(db, name)?);
    }
    let records: Vec<RecordChange> = patch
        .records
        .iter()
        .filter(|record| !unread.contains(&record.collection))
        .map(|record| (record.collection, record.key.clone(), record.value.clone()))
        .collect();
    let indexes: Vec<IndexChange> = patch
        .indexes
        .iter()
        .map(|entry| (entry.index, entry.components.clone(), entry.primary.clone()))
        .collect();
    crate::derived::invalidate_committed(write, db, &records, &indexes)?;
    for (definition, args) in &patch.drops {
        crate::derived::drop_committed(write, db, *definition, args)?;
    }
    for derived in &patch.derived {
        let outcome = decode_outcome(&derived.outcome)?;
        if fresh {
            install_derived(
                write,
                db,
                derived.definition,
                &derived.args,
                &outcome,
                derived.observed.as_deref(),
            )?;
        } else {
            crate::derived::drop_committed(write, db, derived.definition, &derived.args)?;
            if outcome.pinned {
                write.put_derived(
                    db,
                    derived.definition,
                    &derived.args,
                    &DerivedOutcome::invalidated_pin(outcome.code),
                )?;
            }
        }
    }
    Ok(())
}

/// Read a bounded counted list, capping the pre-allocation so corrupt
/// counts cannot claim gigabytes before the bytes run out.
fn read_bounded<T>(
    reader: &mut Reader,
    what: &str,
    mut read: impl FnMut(&mut Reader) -> Result<T>,
) -> Result<Vec<T>> {
    let count = reader.u32()? as usize;
    let mut items = Vec::with_capacity(count.min(1024));
    for _ in 0..count {
        items.push(
            read(reader).map_err(|error| Error::corrupt(format!("truncated {what}: {error}")))?,
        );
    }
    Ok(items)
}

/// Multi-patch tag: an atomic batch of one patch per database.
const TAG_MULTI: u8 = 0x03;

/// Encode an atomic multi-database patch: tag, then each patch as a
/// length-prefixed [`encode_patch`] payload. Every entry carries its
/// own database, versions and expected revision; the applier commits
/// all entries in one write transaction or rejects them all.
pub fn encode_multi_patch(patches: &[Patch]) -> Result<Vec<u8>> {
    let mut encoded = Vec::new();
    encoded.push(TAG_MULTI);
    push_count(&mut encoded, patches.len())?;
    for patch in patches {
        let bytes = encode_patch(patch)?;
        push_sized(&mut encoded, &bytes)?;
    }
    Ok(encoded)
}

/// Decode a multi-database patch, rejecting unknown tags, empty
/// batches, duplicate databases and trailing bytes.
pub fn decode_multi_patch(bytes: &[u8]) -> Result<Vec<Patch>> {
    let mut reader = Reader::new(bytes);
    match reader.byte()? {
        TAG_MULTI => {}
        _ => return Err(Error::corrupt("unknown multi-patch tag".to_string())),
    }
    let patches = read_bounded(&mut reader, "multi-patch entries", |reader| {
        let bytes = reader.sized()?;
        decode_patch(&bytes)
    })?;
    if reader.pos() != bytes.len() {
        return Err(Error::corrupt("trailing multi-patch bytes".to_string()));
    }
    if patches.is_empty() {
        return Err(Error::corrupt("multi-patch has no entries".to_string()));
    }
    let mut seen: Vec<&[u8]> = Vec::with_capacity(patches.len());
    for patch in &patches {
        if seen.contains(&patch.db.as_slice()) {
            return Err(Error::corrupt("multi-patch repeats a database".to_string()));
        }
        seen.push(&patch.db);
    }
    Ok(patches)
}

/// Encode a business failure as canonical JSON for failed patches.
pub fn encode_failure(failure: &flower_sdk::Failure) -> Result<Vec<u8>> {
    crate::json::render(&crate::derived::failure_value(failure))
}

/// Decode a failed patch's result back into a failure.
pub fn decode_failure(bytes: &[u8]) -> Result<flower_sdk::Failure> {
    let value = crate::json::parse_value(bytes)?;
    let map = match value {
        Value::Object(map) => map,
        _ => {
            return Err(Error::corrupt(
                "failure result is not an object".to_string(),
            ));
        }
    };
    let code = match map.get("code") {
        Some(Value::String(code)) => code.clone(),
        _ => return Err(Error::corrupt("failure result has no code".to_string())),
    };
    let message = match map.get("message") {
        Some(Value::String(message)) => message.clone(),
        _ => return Err(Error::corrupt("failure result has no message".to_string())),
    };
    let details = match map.get("details") {
        None | Some(Value::Null) => None,
        Some(details) => Some(details.clone()),
    };
    Ok(flower_sdk::Failure {
        code,
        message,
        details,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Patch {
        Patch {
            failed: false,
            db: b"db".to_vec(),
            versions: vec![("app".to_string(), [7u8; HASH_LEN])],
            records: vec![
                RecordWrite {
                    collection: 3,
                    key: b"k".to_vec(),
                    value: Some(b"{}".to_vec()),
                },
                RecordWrite {
                    collection: 3,
                    key: b"gone".to_vec(),
                    value: None,
                },
            ],
            indexes: vec![IndexWrite {
                index: 5,
                components: vec![b"a".to_vec()],
                primary: b"k".to_vec(),
                present: true,
            }],
            derived: vec![DerivedWrite {
                definition: 9,
                args: b"null".to_vec(),
                outcome: b"outcome".to_vec(),
                observed: Some(vec![b"edge".to_vec()]),
            }],
            drops: vec![(11, b"args".to_vec())],
            receipts: vec![(b"id".to_vec(), b"receipt".to_vec())],
            result: b"1".to_vec(),
            expected_revision: Some(41),
            base_revision: 40,
        }
    }

    #[test]
    fn patch_round_trips() {
        let patch = sample();
        assert_eq!(decode_patch(&encode_patch(&patch).unwrap()).unwrap(), patch);
    }

    #[test]
    fn patch_rejects_garbage() {
        assert!(decode_patch(b"").is_err());
        assert!(decode_patch(&[0x09]).is_err());
        let mut truncated = encode_patch(&sample()).unwrap();
        truncated.pop();
        assert!(decode_patch(&truncated).is_err());
        let mut tagged = encode_patch(&sample()).unwrap();
        tagged.push(0xFF);
        assert!(decode_patch(&tagged).is_err());
    }

    #[test]
    fn failures_round_trip() {
        let failure = flower_sdk::fail("BOOM", "it broke");
        let decoded = decode_failure(&encode_failure(&failure).unwrap()).unwrap();
        assert_eq!(decoded, failure);
        assert!(decode_failure(b"1").is_err());
    }
}
