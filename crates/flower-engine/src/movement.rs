//! Moving a database between groups: its rows exported a page at a
//! time, the exact difference between two reads of it, and a digest of
//! its rows that imports keep up to date as they apply.
//!
//! A logical database is a key range of its group's file, so a move
//! carries rows verbatim and never rewrites a key: interned IDs,
//! revisions, receipts, retention state, timers and queues all move as
//! they are. Only the rows that describe the database's place in its
//! group stay behind ([`moves_with_database`]): its applied position,
//! history incarnation, placement and transaction lock. The destination
//! writes its own on activation.
//!
//! The digest is additive: the wrapping sum of a 128-bit hash of every
//! moving row (SHA-256 of its table, key and value, truncated). Putting
//! a row adds its hash after subtracting the one it replaces, deleting
//! subtracts it, so a destination knows the digest of what it holds
//! after every page at constant cost per row, and a source computes its
//! own in one scan.

use sha2::{Digest as _, Sha256};

use crate::error::{Error, Result};
use crate::storage::keys::{encode_db, encode_meta_key};
use crate::storage::{ReadTxn, SnapshotTable, WriteTxn, meta};

/// Rows one export page holds at most.
pub const PAGE_ROWS: usize = 4096;

/// One row to put (`value`) or delete (`None`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RowChange {
    /// Its table.
    pub table: SnapshotTable,
    /// Its full table key.
    pub key: Vec<u8>,
    /// Its value, or `None` to delete it.
    pub value: Option<Vec<u8>>,
}

/// Where an export resumes: a table, and the last key it gave.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Cursor {
    /// Index into [`SnapshotTable::ALL`].
    pub table: usize,
    /// The last full key given in that table, hex.
    pub after: Option<String>,
}

/// One page of rows and where the next begins (`None`: the last).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Page {
    /// The changes, in key order per table.
    pub changes: Vec<RowChange>,
    /// The next page's cursor.
    pub next: Option<Cursor>,
}

/// The additive digest of a set of rows.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct Digest(pub u128);

impl Digest {
    /// As 32 hex digits.
    pub fn hex(self) -> String {
        format!("{:032x}", self.0)
    }

    /// From 32 hex digits.
    pub fn parse(text: &str) -> Result<Self> {
        u128::from_str_radix(text, 16)
            .map(Digest)
            .map_err(|_| Error::corrupt(format!("bad digest `{text}`")))
    }

    fn add(&mut self, hash: u128) {
        self.0 = self.0.wrapping_add(hash);
    }

    fn sub(&mut self, hash: u128) {
        self.0 = self.0.wrapping_sub(hash);
    }
}

/// The hash of one row in the digest.
pub fn row_hash(table: SnapshotTable, key: &[u8], value: &[u8]) -> u128 {
    let mut hasher = Sha256::new();
    hasher.update([table.tag()]);
    hasher.update((key.len() as u64).to_be_bytes());
    hasher.update(key);
    hasher.update(value);
    let bytes: [u8; 32] = hasher.finalize().into();
    u128::from_be_bytes(bytes[..16].try_into().expect("16 of 32 bytes"))
}

/// The meta rows that describe a database's place in its group, which
/// never move.
fn group_rows(db: &[u8]) -> Result<[Vec<u8>; 4]> {
    Ok([
        encode_meta_key(db, meta::APPLIED_LOG)?,
        encode_meta_key(db, meta::HISTORY_INCARNATION)?,
        encode_meta_key(db, meta::PLACEMENT)?,
        encode_meta_key(db, crate::txn::LOCK_ROW)?,
    ])
}

/// Whether a row moves with its database (see the module docs).
pub fn moves_with_database(stay: &[Vec<u8>; 4], table: SnapshotTable, key: &[u8]) -> bool {
    table != SnapshotTable::Meta || !stay.iter().any(|row| row.as_slice() == key)
}

/// The digest of every moving row of `db` in `read`.
pub fn digest(read: &ReadTxn, db: &[u8]) -> Result<Digest> {
    let stay = group_rows(db)?;
    let mut digest = Digest::default();
    for table in SnapshotTable::ALL {
        let mut after: Option<Vec<u8>> = None;
        loop {
            let rows = read.scan_db_raw_page(table, db, after.as_deref(), PAGE_ROWS)?;
            for (key, value) in &rows {
                if moves_with_database(&stay, table, key) {
                    digest.add(row_hash(table, key, value));
                }
            }
            match rows.last() {
                Some((key, _)) if rows.len() == PAGE_ROWS => after = Some(key.clone()),
                _ => break,
            }
        }
    }
    Ok(digest)
}

fn decode_after(cursor: &Cursor) -> Result<Option<Vec<u8>>> {
    cursor
        .after
        .as_deref()
        .map(|hex| {
            crate::hex::decode(hex).ok_or_else(|| Error::corrupt("bad export cursor".to_string()))
        })
        .transpose()
}

fn cursor_at(table: usize, key: &[u8]) -> Cursor {
    Cursor {
        table,
        after: Some(crate::hex::encode(key)),
    }
}

/// A page of `db`'s moving rows in `read`, as puts, from `cursor` (the
/// start when `None`), holding at most `max_bytes` of keys and values
/// (at least one row).
pub fn export_page(
    read: &ReadTxn,
    db: &[u8],
    cursor: Option<&Cursor>,
    max_bytes: usize,
) -> Result<Page> {
    let stay = group_rows(db)?;
    let start = cursor.cloned().unwrap_or(Cursor {
        table: 0,
        after: None,
    });
    let mut changes = Vec::new();
    let mut bytes = 0usize;
    let mut table_index = start.table;
    let mut after = decode_after(&start)?;
    while let Some(&table) = SnapshotTable::ALL.get(table_index) {
        let rows = read.scan_db_raw_page(table, db, after.as_deref(), PAGE_ROWS)?;
        let full = rows.len() == PAGE_ROWS;
        for (key, value) in rows {
            if !moves_with_database(&stay, table, &key) {
                after = Some(key);
                continue;
            }
            bytes = bytes.saturating_add(key.len() + value.len());
            after = Some(key.clone());
            changes.push(RowChange {
                table,
                key,
                value: Some(value),
            });
            if bytes >= max_bytes {
                return Ok(Page {
                    changes,
                    next: Some(cursor_at(table_index, after.as_deref().unwrap_or_default())),
                });
            }
        }
        if !full {
            table_index += 1;
            after = None;
        }
    }
    Ok(Page {
        changes,
        next: None,
    })
}

/// A page of the difference from `base` to `current` for `db`: puts of
/// every moving row `current` holds that `base` lacks or holds
/// otherwise, deletes of every one `base` holds that `current` lacks,
/// from `cursor`, holding at most `max_bytes`.
pub fn diff_page(
    base: &ReadTxn,
    current: &ReadTxn,
    db: &[u8],
    cursor: Option<&Cursor>,
    max_bytes: usize,
) -> Result<Page> {
    let stay = group_rows(db)?;
    let start = cursor.cloned().unwrap_or(Cursor {
        table: 0,
        after: None,
    });
    let mut changes = Vec::new();
    let mut bytes = 0usize;
    let mut table_index = start.table;
    let mut after = decode_after(&start)?;
    while let Some(&table) = SnapshotTable::ALL.get(table_index) {
        let now = current.scan_db_raw_page(table, db, after.as_deref(), PAGE_ROWS)?;
        let then = base.scan_db_raw_page(table, db, after.as_deref(), PAGE_ROWS)?;
        // Both windows cover keys up to the smaller of their last keys
        // when full; a short window covers the rest of the table.
        let bound = match (now.len() == PAGE_ROWS, then.len() == PAGE_ROWS) {
            (false, false) => None,
            (true, false) => now.last().map(|(key, _)| key.clone()),
            (false, true) => then.last().map(|(key, _)| key.clone()),
            (true, true) => {
                let a = now.last().map(|(key, _)| key.clone());
                let b = then.last().map(|(key, _)| key.clone());
                a.min(b)
            }
        };
        let within = |key: &[u8]| bound.as_deref().is_none_or(|bound| key <= bound);
        let (mut i, mut j) = (0, 0);
        let now: Vec<_> = now.into_iter().filter(|(key, _)| within(key)).collect();
        let then: Vec<_> = then.into_iter().filter(|(key, _)| within(key)).collect();
        while i < now.len() || j < then.len() {
            let change = match (now.get(i), then.get(j)) {
                (Some((key, value)), Some((old_key, old))) if key == old_key => {
                    i += 1;
                    j += 1;
                    (value != old).then(|| RowChange {
                        table,
                        key: key.clone(),
                        value: Some(value.clone()),
                    })
                }
                (Some((key, value)), Some((old_key, _))) if key < old_key => {
                    i += 1;
                    Some(RowChange {
                        table,
                        key: key.clone(),
                        value: Some(value.clone()),
                    })
                }
                (Some((key, value)), None) => {
                    i += 1;
                    Some(RowChange {
                        table,
                        key: key.clone(),
                        value: Some(value.clone()),
                    })
                }
                (_, Some((old_key, _))) => {
                    j += 1;
                    Some(RowChange {
                        table,
                        key: old_key.clone(),
                        value: None,
                    })
                }
                (None, None) => unreachable!("the loop guard holds one"),
            };
            if let Some(change) =
                change.filter(|change| moves_with_database(&stay, change.table, &change.key))
            {
                bytes = bytes
                    .saturating_add(change.key.len() + change.value.as_ref().map_or(0, Vec::len));
                changes.push(change);
            }
        }
        match bound {
            Some(bound) => {
                after = Some(bound);
                if bytes >= max_bytes {
                    return Ok(Page {
                        changes,
                        next: Some(cursor_at(table_index, after.as_deref().unwrap_or_default())),
                    });
                }
            }
            None => {
                table_index += 1;
                after = None;
                if bytes >= max_bytes && table_index < SnapshotTable::ALL.len() {
                    return Ok(Page {
                        changes,
                        next: Some(Cursor {
                            table: table_index,
                            after: None,
                        }),
                    });
                }
            }
        }
    }
    Ok(Page {
        changes,
        next: None,
    })
}

/// Encode changes for a log entry: a compact binary run, LZ4-packed.
pub fn encode_changes(changes: &[RowChange]) -> Vec<u8> {
    let mut raw = Vec::new();
    for change in changes {
        raw.push(change.table.tag());
        raw.extend_from_slice(&(change.key.len() as u32).to_be_bytes());
        raw.extend_from_slice(&change.key);
        match &change.value {
            Some(value) => {
                raw.push(1);
                raw.extend_from_slice(&(value.len() as u32).to_be_bytes());
                raw.extend_from_slice(value);
            }
            None => raw.push(0),
        }
    }
    lz4_flex::compress_prepend_size(&raw)
}

/// Decode what [`encode_changes`] wrote.
pub fn decode_changes(packed: &[u8]) -> Result<Vec<RowChange>> {
    let corrupt = || Error::corrupt("truncated import page".to_string());
    let raw = lz4_flex::decompress_size_prepended(packed)
        .map_err(|error| Error::corrupt(format!("import page does not unpack: {error}")))?;
    let mut changes = Vec::new();
    let mut at = 0usize;
    let take = |at: &mut usize, len: usize| -> Result<&[u8]> {
        let bytes = raw.get(*at..*at + len).ok_or_else(corrupt)?;
        *at += len;
        Ok(bytes)
    };
    while at < raw.len() {
        let table = SnapshotTable::from_tag(take(&mut at, 1)?[0])?;
        let len = u32::from_be_bytes(take(&mut at, 4)?.try_into().expect("4 bytes")) as usize;
        let key = take(&mut at, len)?.to_vec();
        let value = match take(&mut at, 1)?[0] {
            0 => None,
            1 => {
                let len =
                    u32::from_be_bytes(take(&mut at, 4)?.try_into().expect("4 bytes")) as usize;
                Some(take(&mut at, len)?.to_vec())
            }
            _ => return Err(corrupt()),
        };
        changes.push(RowChange { table, key, value });
    }
    Ok(changes)
}

/// Apply imported changes to `db` inside `write`, keeping `digest` the
/// digest of `db`'s moving rows. Every key must lie in `db`'s range and
/// be a moving row: an import never writes another database's rows or
/// the destination's own group rows.
pub fn apply_changes(
    write: &WriteTxn,
    db: &[u8],
    changes: &[RowChange],
    digest: &mut Digest,
) -> Result<()> {
    let scope = encode_db(db)?;
    let stay = group_rows(db)?;
    for change in changes {
        if !change.key.starts_with(&scope) || !moves_with_database(&stay, change.table, &change.key)
        {
            return Err(Error::corrupt(
                "an import row lies outside its database".to_string(),
            ));
        }
        if let Some(old) = write.get_db_raw(change.table, &change.key)? {
            digest.sub(row_hash(change.table, &change.key, &old));
        }
        match &change.value {
            Some(value) => {
                write.put_db_raw(change.table, &change.key, value)?;
                digest.add(row_hash(change.table, &change.key, value));
            }
            None => {
                write.delete_db_raw(change.table, &change.key)?;
            }
        }
    }
    Ok(())
}

/// Delete every moving row of `db` inside `write`, keeping its group
/// rows: an import starting over.
pub fn clear_moving_rows(write: &WriteTxn, db: &[u8]) -> Result<()> {
    let stay = group_rows(db)?;
    let kept: Vec<(Vec<u8>, Vec<u8>)> = stay
        .iter()
        .filter_map(|key| {
            write
                .get_db_raw(SnapshotTable::Meta, key)
                .transpose()
                .map(|value| value.map(|value| (key.clone(), value)))
        })
        .collect::<Result<_>>()?;
    write.clear_database(db)?;
    for (key, value) in kept {
        write.put_db_raw(SnapshotTable::Meta, &key, &value)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::placement::tests::test_store;

    fn put(
        store: &crate::storage::GroupStore,
        db: &[u8],
        collection: u32,
        key: &[u8],
        value: &[u8],
    ) {
        let write = store.begin_write().unwrap();
        write.put_record(db, collection, key, value).unwrap();
        write.commit().unwrap();
    }

    #[test]
    fn pages_diffs_and_digests_reproduce_the_source() {
        let source = test_store("move-source");
        let source = &source.store;
        for index in 0..50u32 {
            put(
                source,
                b"acme",
                1,
                format!("k{index:03}").as_bytes(),
                b"\"v\"",
            );
        }
        put(source, b"other", 1, b"k", b"\"never moves\"");
        let write = source.begin_write().unwrap();
        write.set_meta_u64(b"acme", meta::APPLIED_LOG, 99).unwrap();
        write.set_meta_u64(b"acme", meta::REVISION, 7).unwrap();
        write.commit().unwrap();
        let base = source.begin_read().unwrap();
        // Changes after the base: an update, an insert, a delete.
        put(source, b"acme", 1, b"k001", b"\"changed\"");
        put(source, b"acme", 1, b"k999", b"\"new\"");
        let write = source.begin_write().unwrap();
        write.delete_record(b"acme", 1, b"k002").unwrap();
        write.commit().unwrap();
        let frozen = source.begin_read().unwrap();

        let destination = test_store("move-destination");
        let destination = &destination.store;
        let mut digest = Digest::default();
        // The base, in small pages.
        let mut cursor = None;
        let mut pages = 0;
        loop {
            let page = export_page(&base, b"acme", cursor.as_ref(), 64).unwrap();
            let changes = decode_changes(&encode_changes(&page.changes)).unwrap();
            let write = destination.begin_write().unwrap();
            apply_changes(&write, b"acme", &changes, &mut digest).unwrap();
            write.commit().unwrap();
            pages += 1;
            match page.next {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }
        assert!(pages > 5, "{pages} pages");
        assert_eq!(digest, super::digest(&base, b"acme").unwrap());
        // The delta, in small pages, brings it to the frozen state.
        let mut cursor = None;
        loop {
            let page = diff_page(&base, &frozen, b"acme", cursor.as_ref(), 16).unwrap();
            let write = destination.begin_write().unwrap();
            apply_changes(&write, b"acme", &page.changes, &mut digest).unwrap();
            write.commit().unwrap();
            match page.next {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }
        assert_eq!(digest, super::digest(&frozen, b"acme").unwrap());
        let read = destination.begin_read().unwrap();
        assert_eq!(super::digest(&read, b"acme").unwrap(), digest);
        assert_eq!(
            read.get_record(b"acme", 1, b"k001").unwrap().as_deref(),
            Some(&b"\"changed\""[..])
        );
        assert!(read.get_record(b"acme", 1, b"k002").unwrap().is_none());
        // Group rows stayed behind; the revision moved; nothing else did.
        assert_eq!(read.meta_u64(b"acme", meta::APPLIED_LOG).unwrap(), 0);
        assert_eq!(read.meta_u64(b"acme", meta::REVISION).unwrap(), 7);
        assert!(read.get_record(b"other", 1, b"k").unwrap().is_none());
        // A row outside the database is refused.
        let write = destination.begin_write().unwrap();
        let stray = RowChange {
            table: SnapshotTable::Records,
            key: encode_db(b"other").unwrap(),
            value: Some(b"x".to_vec()),
        };
        assert!(apply_changes(&write, b"acme", &[stray], &mut digest).is_err());
    }
}
