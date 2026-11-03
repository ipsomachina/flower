//! Database snapshots: consistent immutable views for Raft catch-up,
//! sync-down after log purge, and backups.
//!
//! A snapshot captures one database's ten tables under a single pinned
//! read transaction, so the view is consistent without blocking writers.
//! The payload carries full table keys and verbatim values, including
//! the `meta` rows that record the revision, applied log position and
//! history incarnation the view was taken at. Staging and garbage
//! collection rows travel with the snapshot, so a backfill or a
//! retirement resumes on the receiving node instead of stranding
//! partial structures.
//!
//! Snapshots frame as checksummed chunks for streaming transports. The
//! checksum is FNV-1a over the chunk bytes: corruption detection, not
//! authentication (transports authenticate separately), graduating to a
//! cryptographic hash with the crypto lane.
//!
//! Installs are generation-fenced and atomic. An install whose
//! incarnation differs from local history applies unconditionally and
//! adopts the new incarnation: a restored history invalidates every
//! local position. Otherwise the snapshot's applied position must not
//! be behind the local one, so newer local state never loses to an
//! older snapshot; reinstalling the same position is a harmless no-op.
//! The wipe and the write share one transaction: installs never
//! partially apply.
//!
//! One database's payload lives in memory during its encode and its
//! install; callers stream a group's databases one at a time (the
//! cluster's snapshot files do), so a group image never needs to fit.

use crate::error::{Error, Result};
use crate::storage::keys::{Reader, push_count, push_sized, push_u32, push_u64};
use crate::storage::{GroupStore, ReadTxn, Row, SnapshotTable, meta};

/// Snapshot payload tag.
const SNAPSHOT_TAG: u8 = 0x01;
/// Snapshot file framing tag.
const FILE_TAG: u8 = 0x01;
/// Default chunk payload length.
pub const SNAPSHOT_CHUNK_LEN: usize = 64 * 1024;

/// A snapshot's fencing metadata, duplicated from its payload `meta`
/// rows so receivers fence before decoding the body.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotMeta {
    /// Database identifier bytes.
    pub db: Vec<u8>,
    /// Committed revision the view was taken at.
    pub revision: u64,
    /// Applied log position the view was taken at.
    pub applied_log: u64,
    /// History incarnation the view belongs to.
    pub incarnation: u64,
}

/// Encode one database as a snapshot payload under a single pinned
/// read. Returns the fencing metadata plus the payload bytes.
pub fn encode_snapshot(store: &GroupStore, db: &[u8]) -> Result<(SnapshotMeta, Vec<u8>)> {
    encode_snapshot_in(&store.begin_read()?, db)
}

/// Encode one database from a caller's pinned read: every database
/// encoded from one read shares its moment, however long the encoding
/// takes and whatever commits meanwhile.
pub fn encode_snapshot_in(read: &ReadTxn, db: &[u8]) -> Result<(SnapshotMeta, Vec<u8>)> {
    let revision = read.meta_u64(db, meta::REVISION)?;
    let applied_log = read.meta_u64(db, meta::APPLIED_LOG)?;
    let incarnation = read.meta_u64(db, meta::HISTORY_INCARNATION)?;
    let mut payload = Vec::new();
    payload.push(SNAPSHOT_TAG);
    push_sized(&mut payload, db)?;
    push_u64(&mut payload, revision);
    push_u64(&mut payload, applied_log);
    push_u64(&mut payload, incarnation);
    push_count(&mut payload, SnapshotTable::ALL.len())?;
    for table in SnapshotTable::ALL {
        payload.push(table.tag());
        let rows = read.scan_db_raw(table, db)?;
        push_count(&mut payload, rows.len())?;
        for (key, value) in &rows {
            push_sized(&mut payload, key)?;
            push_sized(&mut payload, value)?;
        }
    }
    Ok((
        SnapshotMeta {
            db: db.to_vec(),
            revision,
            applied_log,
            incarnation,
        },
        payload,
    ))
}

/// Frame a payload as a checksummed chunk file for streaming.
pub fn encode_snapshot_file(meta: &SnapshotMeta, payload: &[u8]) -> Result<Vec<u8>> {
    encode_snapshot_file_chunked(meta, payload, SNAPSHOT_CHUNK_LEN)
}

/// Frame a payload with an explicit chunk length, for tests.
pub fn encode_snapshot_file_chunked(
    meta: &SnapshotMeta,
    payload: &[u8],
    chunk_len: usize,
) -> Result<Vec<u8>> {
    let chunk_len = chunk_len.max(1);
    let total = payload.len().div_ceil(chunk_len).max(1) as u32;
    let mut file = Vec::new();
    file.push(FILE_TAG);
    push_sized(&mut file, &meta.db)?;
    push_u64(&mut file, meta.revision);
    push_u64(&mut file, meta.applied_log);
    push_u64(&mut file, meta.incarnation);
    push_u32(&mut file, total);
    if payload.is_empty() {
        push_chunk(&mut file, 0, total, &[])?;
    } else {
        for (index, bytes) in payload.chunks(chunk_len).enumerate() {
            push_chunk(&mut file, index as u32, total, bytes)?;
        }
    }
    Ok(file)
}

/// Decode and verify a chunk file back into metadata plus payload.
pub fn decode_snapshot_file(bytes: &[u8]) -> Result<(SnapshotMeta, Vec<u8>)> {
    let mut reader = Reader::new(bytes);
    match reader.byte()? {
        FILE_TAG => {}
        _ => return Err(Error::corrupt("unknown snapshot file tag".to_string())),
    }
    let meta = SnapshotMeta {
        db: reader.sized()?,
        revision: reader.u64()?,
        applied_log: reader.u64()?,
        incarnation: reader.u64()?,
    };
    let total = reader.u32()?;
    if total == 0 {
        return Err(Error::corrupt("snapshot file has no chunks".to_string()));
    }
    let mut payload = Vec::new();
    for index in 0..total {
        let seen = reader.u32()?;
        let seen_total = reader.u32()?;
        let checksum = reader.u64()?;
        let chunk = reader.sized()?;
        if seen != index || seen_total != total {
            return Err(Error::corrupt("snapshot chunks out of order".to_string()));
        }
        if checksum != chunk_checksum(seen, seen_total, &chunk) {
            return Err(Error::corrupt(
                "snapshot chunk checksum mismatch".to_string(),
            ));
        }
        payload.extend_from_slice(&chunk);
    }
    if reader.pos() != bytes.len() {
        return Err(Error::corrupt("trailing snapshot file bytes".to_string()));
    }
    Ok((meta, payload))
}

/// Install a snapshot payload atomically: fence on incarnation and
/// applied position, wipe the database, then write every row in one
/// transaction. The header metadata must match the payload's `meta`
/// rows.
pub fn apply_snapshot(store: &GroupStore, meta: &SnapshotMeta, payload: &[u8]) -> Result<()> {
    let tables = decode_snapshot_payload(payload)?;
    if tables.db != meta.db
        || tables.revision != meta.revision
        || tables.applied_log != meta.applied_log
        || tables.incarnation != meta.incarnation
    {
        return Err(Error::corrupt(
            "snapshot header disagrees with its payload".to_string(),
        ));
    }
    let read = store.begin_read()?;
    let local_incarnation = read.meta_u64(&meta.db, meta::HISTORY_INCARNATION)?;
    let local_applied = read.meta_u64(&meta.db, meta::APPLIED_LOG)?;
    drop(read);
    // Within one incarnation a position names exactly one state, so
    // reinstalling the same position is a harmless no-op while older
    // positions never overwrite newer local state.
    if meta.incarnation == local_incarnation && meta.applied_log < local_applied {
        return Err(Error::corrupt(format!(
            "snapshot at {} is behind local {}",
            meta.applied_log, local_applied
        )));
    }
    let write = store.begin_write()?;
    write.clear_database(&meta.db)?;
    for (table, rows) in &tables.tables {
        for (key, value) in rows {
            write.put_db_raw(*table, key, value)?;
        }
    }
    write.commit()?;
    Ok(())
}

/// A decoded snapshot payload: header values plus table rows.
struct DecodedSnapshot {
    db: Vec<u8>,
    revision: u64,
    applied_log: u64,
    incarnation: u64,
    tables: Vec<(SnapshotTable, Vec<Row>)>,
}

/// Decode a payload, verifying its header values against its `meta`
/// rows so a truncated payload cannot install under a valid header.
fn decode_snapshot_payload(payload: &[u8]) -> Result<DecodedSnapshot> {
    let mut reader = Reader::new(payload);
    match reader.byte()? {
        SNAPSHOT_TAG => {}
        _ => return Err(Error::corrupt("unknown snapshot tag".to_string())),
    }
    let db = reader.sized()?;
    let revision = reader.u64()?;
    let applied_log = reader.u64()?;
    let incarnation = reader.u64()?;
    let count = reader.u32()? as usize;
    let mut tables = Vec::with_capacity(count.min(16));
    for _ in 0..count {
        let table = SnapshotTable::from_tag(reader.byte()?)?;
        let rows = reader.u32()? as usize;
        let mut pairs = Vec::with_capacity(rows.min(1024));
        for _ in 0..rows {
            pairs.push((reader.sized()?, reader.sized()?));
        }
        tables.push((table, pairs));
    }
    if reader.pos() != payload.len() {
        return Err(Error::corrupt("trailing snapshot bytes".to_string()));
    }
    let decoded = DecodedSnapshot {
        db,
        revision,
        applied_log,
        incarnation,
        tables,
    };
    decoded.verify_scope()?;
    decoded.verify_header()?;
    Ok(decoded)
}

impl DecodedSnapshot {
    /// Every row must lie inside the snapshot's database key range: the
    /// install wipes and rewrites one database, so a foreign key would
    /// overwrite another database's rows.
    fn verify_scope(&self) -> Result<()> {
        let scope = crate::storage::keys::encode_db(&self.db)?;
        for (_, rows) in &self.tables {
            if rows.iter().any(|(key, _)| !key.starts_with(&scope)) {
                return Err(Error::corrupt(
                    "snapshot row lies outside its database".to_string(),
                ));
            }
        }
        Ok(())
    }

    /// The payload's `meta` rows must carry the header's fencing
    /// values, defaulting missing rows to zero like live reads.
    fn verify_header(&self) -> Result<()> {
        let mut values = std::collections::HashMap::new();
        for (table, rows) in &self.tables {
            if *table != SnapshotTable::Meta {
                continue;
            }
            let skip = crate::storage::keys::encode_db(&self.db)?.len();
            for (key, value) in rows {
                if key.len() < skip {
                    return Err(Error::corrupt("short snapshot meta key".to_string()));
                }
                let mut reader = Reader::new(&key[skip..]);
                values.insert(reader.sized()?, value.clone());
            }
        }
        for (name, expected) in [
            (meta::REVISION, self.revision),
            (meta::APPLIED_LOG, self.applied_log),
            (meta::HISTORY_INCARNATION, self.incarnation),
        ] {
            let actual = match values.get(name) {
                None => 0,
                Some(bytes) => crate::storage::codec::decode_u64(bytes)?,
            };
            if actual != expected {
                return Err(Error::corrupt(
                    "snapshot header disagrees with its meta rows".to_string(),
                ));
            }
        }
        Ok(())
    }
}

/// Append one framed chunk with its checksum.
fn push_chunk(file: &mut Vec<u8>, index: u32, total: u32, bytes: &[u8]) -> Result<()> {
    push_u32(file, index);
    push_u32(file, total);
    push_u64(file, chunk_checksum(index, total, bytes));
    push_sized(file, bytes)
}

/// FNV-1a over the chunk header plus bytes: corruption detection, not
/// authentication.
fn chunk_checksum(index: u32, total: u32, bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in index
        .to_be_bytes()
        .into_iter()
        .chain(total.to_be_bytes())
        .chain(bytes.iter().copied())
    {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_path(test: &str) -> std::path::PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!("flower-snapshot-{test}-{}", std::process::id()));
        let _ = std::fs::remove_file(&path);
        path
    }

    #[test]
    fn files_round_trip_chunked() {
        let meta = SnapshotMeta {
            db: b"db".to_vec(),
            revision: 3,
            applied_log: 9,
            incarnation: 1,
        };
        let payload = vec![7u8; 300];
        let file = encode_snapshot_file_chunked(&meta, &payload, 64).unwrap();
        let (decoded_meta, decoded_payload) = decode_snapshot_file(&file).unwrap();
        assert_eq!(decoded_meta, meta);
        assert_eq!(decoded_payload, payload);
    }

    #[test]
    fn files_reject_corruption() {
        let meta = SnapshotMeta {
            db: b"db".to_vec(),
            revision: 3,
            applied_log: 9,
            incarnation: 1,
        };
        let mut file = encode_snapshot_file_chunked(&meta, &[1u8; 100], 32).unwrap();
        let last = file.len() - 1;
        file[last] ^= 0xFF;
        assert!(decode_snapshot_file(&file).is_err());
    }

    #[test]
    fn snapshots_round_trip_one_database() {
        let path = temp_path("roundtrip");
        let store = GroupStore::create(&path).unwrap();
        let write = store.begin_write().unwrap();
        write.intern(b"db", b"app\x00cells").unwrap();
        write
            .put_record(b"db", 1, b"\"t1\"", br#"{"v":1}"#)
            .unwrap();
        write.set_meta_u64(b"db", meta::REVISION, 4).unwrap();
        write.set_meta_u64(b"db", meta::APPLIED_LOG, 12).unwrap();
        write
            .set_meta_u64(b"db", meta::HISTORY_INCARNATION, 2)
            .unwrap();
        write.commit().unwrap();
        let (meta, payload) = encode_snapshot(&store, b"db").unwrap();
        assert_eq!(meta.revision, 4);
        assert_eq!(meta.applied_log, 12);
        assert_eq!(meta.incarnation, 2);

        let other = temp_path("roundtrip-other");
        let store = GroupStore::create(&other).unwrap();
        apply_snapshot(&store, &meta, &payload).unwrap();
        let read = store.begin_read().unwrap();
        assert_eq!(read.meta_u64(b"db", meta::REVISION).unwrap(), 4);
        assert_eq!(read.meta_u64(b"db", meta::APPLIED_LOG).unwrap(), 12);
        let record = read.get_record(b"db", 1, b"\"t1\"").unwrap().unwrap();
        assert_eq!(record, br#"{"v":1}"#.to_vec());
        drop(read);
        drop(store);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&other);
    }

    #[test]
    fn installs_reject_rows_of_other_databases() {
        let path = temp_path("foreign");
        let store = GroupStore::create(&path).unwrap();
        let write = store.begin_write().unwrap();
        write
            .put_record(b"b", 1, b"\"k\"", br#"{"owner":"b"}"#)
            .unwrap();
        write.commit().unwrap();
        // A payload for database `a` smuggling a record of database `b`.
        let foreign = crate::storage::keys::encode_record_key(b"b", 1, b"\"k\"").unwrap();
        let mut payload = vec![SNAPSHOT_TAG];
        push_sized(&mut payload, b"a").unwrap();
        for value in [0u64, 0, 0] {
            push_u64(&mut payload, value);
        }
        push_count(&mut payload, 1).unwrap();
        payload.push(SnapshotTable::Records.tag());
        push_count(&mut payload, 1).unwrap();
        push_sized(&mut payload, &foreign).unwrap();
        push_sized(
            &mut payload,
            &crate::storage::codec::pack_value(br#"{"owner":"a"}"#),
        )
        .unwrap();
        let meta = SnapshotMeta {
            db: b"a".to_vec(),
            revision: 0,
            applied_log: 0,
            incarnation: 0,
        };
        let error = apply_snapshot(&store, &meta, &payload).unwrap_err();
        assert!(
            error.to_string().contains("outside its database"),
            "{error}"
        );
        let read = store.begin_read().unwrap();
        assert_eq!(
            read.get_record(b"b", 1, b"\"k\"").unwrap().unwrap(),
            br#"{"owner":"b"}"#.to_vec()
        );
        drop(read);
        drop(store);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn installs_fence_old_snapshots() {
        let path = temp_path("fence");
        let store = GroupStore::create(&path).unwrap();
        // A snapshot taken at 12 cannot install over local 20.
        let write = store.begin_write().unwrap();
        write.set_meta_u64(b"db", meta::APPLIED_LOG, 12).unwrap();
        write.commit().unwrap();
        let (meta, payload) = encode_snapshot(&store, b"db").unwrap();
        assert_eq!(meta.applied_log, 12);
        let write = store.begin_write().unwrap();
        write.set_meta_u64(b"db", meta::APPLIED_LOG, 20).unwrap();
        write.commit().unwrap();
        let error = apply_snapshot(&store, &meta, &payload).unwrap_err();
        assert!(error.to_string().contains("is behind local"), "{error}");
        drop(store);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn installs_force_new_incarnations() {
        let fresh = temp_path("incarnation-fresh");
        let store = GroupStore::create(&fresh).unwrap();
        let write = store.begin_write().unwrap();
        write.set_meta_u64(b"db", meta::APPLIED_LOG, 5).unwrap();
        write
            .set_meta_u64(b"db", meta::HISTORY_INCARNATION, 7)
            .unwrap();
        write.commit().unwrap();
        let (meta, payload) = encode_snapshot(&store, b"db").unwrap();
        drop(store);
        // Local history is ahead but belongs to another incarnation: a
        // restored history invalidates every local position.
        let path = temp_path("incarnation-local");
        let store = GroupStore::create(&path).unwrap();
        let write = store.begin_write().unwrap();
        write.set_meta_u64(b"db", meta::APPLIED_LOG, 20).unwrap();
        write
            .set_meta_u64(b"db", meta::HISTORY_INCARNATION, 2)
            .unwrap();
        write.commit().unwrap();
        apply_snapshot(&store, &meta, &payload).unwrap();
        let read = store.begin_read().unwrap();
        assert_eq!(read.meta_u64(b"db", meta::APPLIED_LOG).unwrap(), 5);
        assert_eq!(read.meta_u64(b"db", meta::HISTORY_INCARNATION).unwrap(), 7);
        drop(read);
        drop(store);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&fresh);
    }
}
