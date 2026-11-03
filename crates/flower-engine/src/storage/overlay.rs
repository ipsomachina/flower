//! Read sets, write logs and overlays: what a speculative preparation
//! needs from storage.
//!
//! A writer prepares writes on its staged state, one uncommitted redb
//! write transaction that only its own thread can read. To prepare
//! several writes at once, it hands other threads a view of the same
//! state: committed state (a redb snapshot taken while the staged
//! transaction holds the write lock, so nothing commits under it) seen
//! through an [`Overlay`] of every row the staged transaction wrote, as
//! the transaction holds it now. A [`ReadTxn`](super::ReadTxn) over that
//! view reads exactly what one over the staged transaction would.
//!
//! Such a read can record what it observed ([`ReadSet`]): every row it
//! looked up, present or absent, and every key range it walked, cut at
//! the row where the walk stopped, so a row inserted inside it counts.
//! The staged transaction logs every row it puts or deletes
//! ([`Write`]). A preparation made on the view at one point of the log
//! holds at a later point exactly when no row written in between is one
//! it read: everything it computed is a function of what it read.
//! Checking is by row, in every table alike, so it covers records,
//! scans, index ranges, derived outcomes and their edges, receipts,
//! placement and code rows without knowing what any of them mean.

use std::collections::{BTreeMap, HashSet};
use std::ops::Bound;
use std::sync::{Mutex, PoisonError};

use redb::{ReadOnlyTable, ReadableTable, Table};

use super::keys::Reader;
use super::{SnapshotTable, storage};
use crate::error::Result;

/// Most rows and ranges one read set keeps before it gives up: past
/// it, it holds for nothing ([`ReadSet::overflowed`]).
pub const MAX_READS: usize = 1 << 16;

/// What a walk calls on each row, `(key, value)`, until it answers
/// false.
pub(crate) type Visit<'v> = &'v mut dyn FnMut(&[u8], &[u8]) -> Result<bool>;

/// What scans and lookups read rows through: a table, a table seen
/// through an overlay, or either recording what it observed.
pub(crate) trait Rows {
    /// One row's stored bytes.
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Visit the rows with keys in `[start, end)` (`end` unbounded when
    /// `None`), ascending or, with `reverse`, descending, until `visit`
    /// answers false.
    fn walk(&self, start: &[u8], end: Option<&[u8]>, reverse: bool, visit: Visit<'_>)
    -> Result<()>;
}

fn readable_get<T: ReadableTable<&'static [u8], &'static [u8]>>(
    table: &T,
    key: &[u8],
) -> Result<Option<Vec<u8>>> {
    Ok(table
        .get(key)
        .map_err(storage)?
        .map(|guard| guard.value().to_vec()))
}

fn readable_walk<T: ReadableTable<&'static [u8], &'static [u8]>>(
    table: &T,
    start: &[u8],
    end: Option<&[u8]>,
    reverse: bool,
    visit: Visit<'_>,
) -> Result<()> {
    if end.is_some_and(|end| start >= end) {
        return Ok(());
    }
    let upper = match end {
        Some(end) => Bound::Excluded(end),
        None => Bound::Unbounded,
    };
    let range = table
        .range::<&[u8]>((Bound::Included(start), upper))
        .map_err(storage)?;
    if reverse {
        for entry in range.rev() {
            let (key, value) = entry.map_err(storage)?;
            if !visit(key.value(), value.value())? {
                break;
            }
        }
    } else {
        for entry in range {
            let (key, value) = entry.map_err(storage)?;
            if !visit(key.value(), value.value())? {
                break;
            }
        }
    }
    Ok(())
}

impl Rows for ReadOnlyTable<&'static [u8], &'static [u8]> {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        readable_get(self, key)
    }

    fn walk(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        reverse: bool,
        visit: Visit<'_>,
    ) -> Result<()> {
        readable_walk(self, start, end, reverse, visit)
    }
}

impl Rows for Table<'_, &'static [u8], &'static [u8]> {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        readable_get(self, key)
    }

    fn walk(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        reverse: bool,
        visit: Visit<'_>,
    ) -> Result<()> {
        readable_walk(self, start, end, reverse, visit)
    }
}

/// Rows a staged transaction wrote, as it holds them: the stored bytes,
/// or `None` for a row it deleted. Committed state seen through it is
/// the staged state.
#[derive(Clone, Debug, Default)]
pub struct Overlay {
    tables: [BTreeMap<Vec<u8>, Option<Vec<u8>>>; 10],
}

impl Overlay {
    /// Set one row as the staged transaction holds it.
    pub(crate) fn set(&mut self, table: SnapshotTable, key: Vec<u8>, value: Option<Vec<u8>>) {
        self.tables[table.slot()].insert(key, value);
    }

    /// One table's rows.
    pub(crate) fn table(&self, table: SnapshotTable) -> &BTreeMap<Vec<u8>, Option<Vec<u8>>> {
        &self.tables[table.slot()]
    }

    /// Rows it holds, across tables.
    pub fn len(&self) -> usize {
        self.tables.iter().map(BTreeMap::len).sum()
    }

    /// Whether it holds no row.
    pub fn is_empty(&self) -> bool {
        self.tables.iter().all(BTreeMap::is_empty)
    }
}

/// A committed table seen through one table of an [`Overlay`].
pub(crate) struct Layered<'a> {
    pub(crate) base: ReadOnlyTable<&'static [u8], &'static [u8]>,
    pub(crate) overlay: &'a BTreeMap<Vec<u8>, Option<Vec<u8>>>,
}

impl Rows for Layered<'_> {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match self.overlay.get(key) {
            Some(value) => Ok(value.clone()),
            None => Rows::get(&self.base, key),
        }
    }

    fn walk(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        reverse: bool,
        visit: Visit<'_>,
    ) -> Result<()> {
        if end.is_some_and(|end| start >= end) {
            return Ok(());
        }
        let upper = match end {
            Some(end) => Bound::Excluded(end),
            None => Bound::Unbounded,
        };
        let mut staged: Vec<(&[u8], Option<&[u8]>)> = self
            .overlay
            .range::<[u8], _>((Bound::Included(start), upper))
            .map(|(key, value)| (key.as_slice(), value.as_deref()))
            .collect();
        if reverse {
            staged.reverse();
        }
        // Merge in walk order; a staged row replaces the committed one.
        let mut staged = staged.into_iter().peekable();
        let mut stopped = false;
        let before = |staged: &[u8], key: &[u8]| -> bool {
            if reverse { staged > key } else { staged < key }
        };
        self.base.walk(start, end, reverse, &mut |key, value| {
            while let Some((staged_key, staged_value)) =
                staged.next_if(|(staged_key, _)| before(staged_key, key))
            {
                if let Some(staged_value) = staged_value
                    && !visit(staged_key, staged_value)?
                {
                    stopped = true;
                    return Ok(false);
                }
            }
            if let Some((staged_key, staged_value)) =
                staged.next_if(|(staged_key, _)| *staged_key == key)
            {
                if let Some(staged_value) = staged_value
                    && !visit(staged_key, staged_value)?
                {
                    stopped = true;
                    return Ok(false);
                }
                return Ok(true);
            }
            if !visit(key, value)? {
                stopped = true;
                return Ok(false);
            }
            Ok(true)
        })?;
        if !stopped {
            for (staged_key, staged_value) in staged {
                if let Some(staged_value) = staged_value
                    && !visit(staged_key, staged_value)?
                {
                    break;
                }
            }
        }
        Ok(())
    }
}

/// One row a staged transaction put or deleted.
pub type Write = (SnapshotTable, Vec<u8>);

/// Durable certificates must re-encode identically on every applier even
/// though hash sets use a different randomized iteration order per process.
pub(crate) fn serialize_set<T: Ord + serde::Serialize, S: serde::Serializer>(
    set: &HashSet<T>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    serde::Serialize::serialize(
        &set.iter().collect::<std::collections::BTreeSet<_>>(),
        serializer,
    )
}

/// What a read observed, row by row: lookups (present or absent), key
/// ranges walked, and `meta` names matched by prefix.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ReadSet {
    #[serde(serialize_with = "serialize_set")]
    rows: HashSet<(SnapshotTable, Vec<u8>)>,
    /// `[start, end)`, `end` unbounded when `None`.
    ranges: Vec<(SnapshotTable, Vec<u8>, Option<Vec<u8>>)>,
    /// `(encoded database, name prefix)`: every `meta` row of the
    /// database whose name starts with the prefix.
    meta_names: Vec<(Vec<u8>, Vec<u8>)>,
    overflowed: bool,
    /// Participant-only proofs for cached derived rows, keyed by hex storage
    /// key. None identifies older certificates that need a coarse fence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    derived_sources: Option<std::collections::BTreeMap<String, crate::certificate::Certificate>>,
}

impl ReadSet {
    pub(crate) fn participant() -> Self {
        Self {
            derived_sources: Some(Default::default()),
            ..Self::default()
        }
    }

    pub(crate) fn tracks_derived_sources(&self) -> bool {
        self.derived_sources.is_some()
    }

    pub(crate) fn needs_derived_sources(&self, key: &[u8]) -> bool {
        !self.overflowed
            && self
                .derived_sources
                .as_ref()
                .is_some_and(|sources| !sources.contains_key(&crate::hex::encode(key)))
    }

    pub(crate) fn derived_sources(
        &mut self,
        key: &[u8],
        certificate: crate::certificate::Certificate,
    ) {
        if let Some(sources) = &mut self.derived_sources {
            sources.insert(crate::hex::encode(key), certificate);
            self.full();
        }
    }

    /// The source dependencies behind every observed cached outcome must be
    /// certified. Comparing only physical cache rows would miss invalidations.
    pub(crate) fn derived_touched_by(&self, patch: &crate::patch::Patch) -> Result<bool> {
        let Some(sources) = &self.derived_sources else {
            return Ok(self.reads_table(SnapshotTable::Derived));
        };
        if self
            .ranges
            .iter()
            .any(|(table, _, _)| *table == SnapshotTable::Derived)
            || self.rows.iter().any(|(table, key)| {
                *table == SnapshotTable::Derived && !sources.contains_key(&crate::hex::encode(key))
            })
        {
            return Ok(true);
        }
        let prefix = crate::hex::encode(&super::keys::encode_db(&patch.db)?);
        let changes = crate::certificate::ChangeSet::of_patch(patch)?;
        Ok(sources
            .iter()
            .any(|(key, certificate)| key.starts_with(&prefix) && certificate.touched_by(&changes)))
    }

    /// Fall back to a whole-database fence for reads outside the store.
    pub(crate) fn invalidate(&mut self) {
        self.overflowed = true;
    }

    pub(crate) fn reads_table(&self, table: SnapshotTable) -> bool {
        self.rows.iter().any(|(t, _)| *t == table)
            || self.ranges.iter().any(|(t, _, _)| *t == table)
    }
    fn full(&mut self) -> bool {
        if self.len() >= MAX_READS {
            self.overflowed = true;
        }
        self.overflowed
    }

    /// Record a lookup of one row.
    pub(crate) fn row(&mut self, table: SnapshotTable, key: &[u8]) {
        if !self.full() {
            self.rows.insert((table, key.to_vec()));
        }
    }

    /// Record a walk over `[start, end)`.
    pub(crate) fn range(&mut self, table: SnapshotTable, start: Vec<u8>, end: Option<Vec<u8>>) {
        if !self.full() {
            self.ranges.push((table, start, end));
        }
    }

    /// Record a match of every `meta` name of a database (`scope`, its
    /// encoded prefix) starting with `prefix`.
    pub(crate) fn meta_names(&mut self, scope: Vec<u8>, prefix: &[u8]) {
        if !self.full() {
            self.meta_names.push((scope, prefix.to_vec()));
        }
    }

    /// Merge what another read observed.
    pub fn merge(&mut self, other: ReadSet) {
        self.overflowed |= other.overflowed;
        self.rows.extend(other.rows);
        self.ranges.extend(other.ranges);
        self.meta_names.extend(other.meta_names);
        // This merge is used for speculative read sets, not participants.
        // Discarding source proofs forces conservative participant validation.
        self.derived_sources = None;
        self.full();
    }

    /// Whether it stopped recording: it holds for no write.
    pub fn overflowed(&self) -> bool {
        self.overflowed
    }

    /// Rows and ranges recorded.
    pub fn len(&self) -> usize {
        self.rows.len()
            + self.ranges.len()
            + self.meta_names.len()
            + self.derived_sources.as_ref().map_or(0, |sources| {
                sources
                    .values()
                    .map(|certificate| certificate.dependencies() + 1)
                    .sum::<usize>()
            })
    }

    /// Whether it recorded nothing.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Whether writing `key` in `table` may change what it observed.
    pub fn touched_by(&self, table: SnapshotTable, key: &[u8]) -> bool {
        if self.overflowed {
            return true;
        }
        // Lookups first: they are most of a read set and hash.
        if self.rows.contains(&(table, key.to_vec())) {
            return true;
        }
        let in_range = self.ranges.iter().any(|(at, start, end)| {
            *at == table && key >= start.as_slice() && end.as_deref().is_none_or(|end| key < end)
        });
        if in_range {
            return true;
        }
        table == SnapshotTable::Meta
            && self.meta_names.iter().any(|(scope, prefix)| {
                key.strip_prefix(scope.as_slice()).is_some_and(|rest| {
                    Reader::new(rest)
                        .sized()
                        .is_ok_and(|name| name.starts_with(prefix))
                })
            })
    }

    /// The writes among `writes` that touch it, at most `limit`, in
    /// order.
    pub fn conflicts<'a>(&self, writes: &'a [Write], limit: usize) -> Vec<&'a Write> {
        writes
            .iter()
            .filter(|(table, key)| self.touched_by(*table, key))
            .take(limit)
            .collect()
    }
}

/// A table that records what it observed into a read set, when given
/// one.
pub(crate) struct Recorded<'r, R> {
    pub(crate) rows: R,
    pub(crate) record: Option<(&'r Mutex<ReadSet>, SnapshotTable)>,
}

impl<R: Rows> Rows for Recorded<'_, R> {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some((set, table)) = self.record {
            set.lock()
                .unwrap_or_else(PoisonError::into_inner)
                .row(table, key);
        }
        self.rows.get(key)
    }

    fn walk(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        reverse: bool,
        visit: Visit<'_>,
    ) -> Result<()> {
        let Some((set, table)) = self.record else {
            return self.rows.walk(start, end, reverse, visit);
        };
        let mut stopped: Option<Vec<u8>> = None;
        let walked = self.rows.walk(start, end, reverse, &mut |key, value| {
            let more = visit(key, value)?;
            if !more {
                stopped = Some(key.to_vec());
            }
            Ok(more)
        });
        // What the walk saw: up to the row it stopped at, inclusive, or
        // the whole range when it ran out, or failed (what the failure
        // depended on is somewhere in it).
        if walked.is_err() {
            stopped = None;
        }
        let (lower, upper) = match (stopped, reverse) {
            (None, _) => (start.to_vec(), end.map(<[u8]>::to_vec)),
            (Some(mut last), false) => {
                last.push(0);
                (start.to_vec(), Some(last))
            }
            (Some(last), true) => (last, end.map(<[u8]>::to_vec)),
        };
        set.lock()
            .unwrap_or_else(PoisonError::into_inner)
            .range(table, lower, upper);
        walked
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store_with(rows: &[(&[u8], &[u8])]) -> redb::Database {
        let db = redb::Database::builder()
            .create_with_backend(redb::backends::InMemoryBackend::new())
            .unwrap();
        let write = db.begin_write().unwrap();
        {
            let mut table = write.open_table(super::super::RECORDS).unwrap();
            for (key, value) in rows {
                table.insert(*key, *value).unwrap();
            }
        }
        write.commit().unwrap();
        db
    }

    fn walked(rows: &dyn Rows, start: &[u8], end: Option<&[u8]>, reverse: bool) -> Vec<Vec<u8>> {
        let mut seen = Vec::new();
        rows.walk(start, end, reverse, &mut |key, _| {
            seen.push(key.to_vec());
            Ok(true)
        })
        .unwrap();
        seen
    }

    #[test]
    fn overlays_merge_staged_rows_into_walks_both_ways() {
        let db = store_with(&[(b"a", b"1"), (b"c", b"3"), (b"e", b"5")]);
        let read = redb::ReadableDatabase::begin_read(&db).unwrap();
        let mut overlay = Overlay::default();
        overlay.set(SnapshotTable::Records, b"b".to_vec(), Some(b"2".to_vec()));
        overlay.set(SnapshotTable::Records, b"c".to_vec(), None);
        overlay.set(SnapshotTable::Records, b"e".to_vec(), Some(b"50".to_vec()));
        overlay.set(SnapshotTable::Records, b"f".to_vec(), Some(b"6".to_vec()));
        let layered = Layered {
            base: read.open_table(super::super::RECORDS).unwrap(),
            overlay: overlay.table(SnapshotTable::Records),
        };
        let keys = |list: &[&[u8]]| list.iter().map(|key| key.to_vec()).collect::<Vec<_>>();
        assert_eq!(
            walked(&layered, b"", None, false),
            keys(&[b"a", b"b", b"e", b"f"])
        );
        assert_eq!(
            walked(&layered, b"", None, true),
            keys(&[b"f", b"e", b"b", b"a"])
        );
        assert_eq!(
            walked(&layered, b"b", Some(b"f"), false),
            keys(&[b"b", b"e"])
        );
        assert_eq!(layered.get(b"e").unwrap(), Some(b"50".to_vec()));
        assert_eq!(layered.get(b"c").unwrap(), None);
        assert_eq!(layered.get(b"a").unwrap(), Some(b"1".to_vec()));
    }

    #[test]
    fn recorded_walks_cover_what_they_saw() {
        let db = store_with(&[(b"a", b"1"), (b"c", b"3"), (b"e", b"5")]);
        let read = redb::ReadableDatabase::begin_read(&db).unwrap();
        let set = Mutex::new(ReadSet::default());
        let recorded = Recorded {
            rows: read.open_table(super::super::RECORDS).unwrap(),
            record: Some((&set, SnapshotTable::Records)),
        };
        // Stop after the first two rows: rows past `c` were not seen.
        let mut seen = 0;
        recorded
            .walk(b"a", Some(b"z"), false, &mut |_, _| {
                seen += 1;
                Ok(seen < 2)
            })
            .unwrap();
        recorded.get(b"x").unwrap();
        let set = set.into_inner().unwrap();
        assert!(set.touched_by(SnapshotTable::Records, b"b"));
        assert!(set.touched_by(SnapshotTable::Records, b"c"));
        assert!(!set.touched_by(SnapshotTable::Records, b"d"));
        assert!(
            set.touched_by(SnapshotTable::Records, b"x"),
            "an absent row read"
        );
        assert!(!set.touched_by(SnapshotTable::Meta, b"x"), "another table");
    }
}
