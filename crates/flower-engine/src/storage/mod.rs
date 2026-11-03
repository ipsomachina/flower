//! Per-group durable storage.
//!
//! Each Raft group persists its databases in one redb file with the fixed
//! ten-table layout from the storage contract: `meta`, `intern_id`,
//! `intern_name`, `records`, `index_entries`, `derived`, `dep_edges`,
//! `dependent_edges`, `receipts` and `code`. An eleventh, `store`, is
//! the file's own: the contract it was written under
//! ([`GroupStore::claim_contract`]), which no snapshot carries. [`GroupStore`] opens the file
//! and hands out [`ReadTxn`] and [`WriteTxn`]; [`keys`] owns key codecs,
//! [`codec`] owns value codecs, and [`compact`] gives the file's free
//! space back online while readers hold snapshots.
//!
//! Engine identifiers are interned per database to 32-bit IDs allocated
//! monotonically from a `meta` counter and never reused. Record values are
//! lz4-packed canonical JSON; reads return decompressed bytes.

pub mod codec;
pub mod compact;
pub mod keys;
pub mod overlay;

use std::cell::Cell;
use std::path::Path;
use std::sync::{Arc, Mutex, PoisonError, RwLock};

use redb::{ReadOnlyTable, ReadableDatabase, ReadableTable, Table, TableDefinition, TableHandle};

use crate::error::{Error, Result};
use codec::{
    DerivedOutcome, decode_code, decode_edge_list, decode_outcome, decode_u64, encode_code,
    encode_edge_list, encode_floors, encode_outcome, encode_u64, pack_value, unpack_value,
};
use compact::Compactor;
use keys::{
    HASH_LEN, Reader, decode_dep_key, decode_dependent, decode_index_entry, decode_record_suffix,
    encode_code_key, encode_db, encode_dep_edge, encode_derived_dep, encode_derived_key,
    encode_index_key, encode_intern_id_key, encode_intern_name_key, encode_meta_key,
    encode_receipt_key, encode_record_key, index_prefix, prefix_end, record_key_class,
    record_prefix,
};
use overlay::{Layered, Recorded, Rows};
pub use overlay::{Overlay, ReadSet, Write};

const META: TableDefinition<&'static [u8], &'static [u8]> = TableDefinition::new("meta");
const INTERN_ID: TableDefinition<&'static [u8], &'static [u8]> = TableDefinition::new("intern_id");
const INTERN_NAME: TableDefinition<&'static [u8], &'static [u8]> =
    TableDefinition::new("intern_name");
const RECORDS: TableDefinition<&'static [u8], &'static [u8]> = TableDefinition::new("records");
const INDEX_ENTRIES: TableDefinition<&'static [u8], &'static [u8]> =
    TableDefinition::new("index_entries");
const DERIVED: TableDefinition<&'static [u8], &'static [u8]> = TableDefinition::new("derived");
const DEP_EDGES: TableDefinition<&'static [u8], &'static [u8]> = TableDefinition::new("dep_edges");
const DEPENDENT_EDGES: TableDefinition<&'static [u8], &'static [u8]> =
    TableDefinition::new("dependent_edges");
const RECEIPTS: TableDefinition<&'static [u8], &'static [u8]> = TableDefinition::new("receipts");
const CODE: TableDefinition<&'static [u8], &'static [u8]> = TableDefinition::new("code");
/// The file's own rows, outside the ten: no snapshot, move or backup
/// carries them.
const STORE: TableDefinition<&'static [u8], &'static [u8]> = TableDefinition::new("store");
/// The [`STORE`] row naming the contract the file was written under.
const STORE_CONTRACT: &[u8] = b"contract";

/// Well-known `meta` names.
pub mod meta {
    /// Committed revision, `u64`.
    pub const REVISION: &[u8] = b"revision";
    /// Applied log position, `u64`.
    pub const APPLIED_LOG: &[u8] = b"applied_log";
    /// History incarnation, `u64`.
    pub const HISTORY_INCARNATION: &[u8] = b"history_incarnation";
    /// A moved database's history lineage: big-endian `u64`s, the
    /// incarnation they hold for first, then the earlier incarnations of
    /// the same history it carried here (see
    /// [`crate::placement::lineage`]). Moves with the database.
    pub const HISTORY_LINEAGE: &[u8] = b"history_lineage";
    /// Retention floors, count plus `u64` entries.
    pub const RETENTION_FLOORS: &[u8] = b"retention_floors";
    /// Placement of a named partition in its group, JSON (see
    /// [`crate::placement`]); absent for an unplaced database.
    pub const PLACEMENT: &[u8] = b"placement";
    /// Last allocated intern ID, `u64` holding a `u32` range.
    pub const INTERN_COUNTER: &[u8] = b"intern_counter";
    /// Aggregate lineages begun so far, `u64` (see
    /// [`crate::staging::aggregate_lineage_key`]).
    pub const AGGREGATE_LINEAGES: &[u8] = b"aggregate_lineages";
    /// Code eras minted so far, `u64` (see [`crate::staging::code_era`]).
    pub const CODE_ERAS: &[u8] = b"code_eras";
}

/// One table row: full key or decoded key bytes, plus value bytes.
pub type Row = (Vec<u8>, Vec<u8>);
/// One record write: key bytes plus the new value, or `None` to delete.
pub type RowWrite = (Vec<u8>, Option<Vec<u8>>);
/// One index entry: index components plus primary key.
pub type IndexEntry = (Vec<Vec<u8>>, Vec<u8>);
/// One derived instance: definition ID plus canonical argument bytes.
pub type DerivedInstance = (u32, Vec<u8>);
/// One dependency edge: dependency key plus its dependent instance.
pub type DepEdge = (Vec<u8>, DerivedInstance);

/// Bounds and paging of one ordered scan: an inclusive lower and an
/// exclusive upper full key, a row cap and the direction. The default
/// scans the whole scope forwards.
#[derive(Clone, Copy, Debug, Default)]
pub struct ScanRange<'a> {
    /// Inclusive lower full-key bound.
    pub lower: Option<&'a [u8]>,
    /// Exclusive upper full-key bound.
    pub upper: Option<&'a [u8]>,
    /// Most rows to return.
    pub limit: Option<usize>,
    /// Iterate backwards from the upper end.
    pub reverse: bool,
}

/// Runs before every durable commit of a group file, its compactor's
/// included (see [`GroupStore::set_before_durable`]). A durable commit
/// makes every deferred commit before it durable too, so whatever must
/// be durable before them goes here.
pub type BeforeDurable = Arc<dyn Fn() -> Result<()> + Send + Sync>;

/// The [`BeforeDurable`] hook a store and its compactor share.
#[derive(Default)]
pub(crate) struct DurableHook(RwLock<Option<BeforeDurable>>);

impl DurableHook {
    fn set(&self, hook: BeforeDurable) {
        *self.0.write().unwrap_or_else(PoisonError::into_inner) = Some(hook);
    }

    /// Run the hook, if one is set: before a durable commit.
    pub(crate) fn run(&self) -> Result<()> {
        let hook = self
            .0
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        match hook {
            Some(hook) => hook(),
            None => Ok(()),
        }
    }
}

thread_local! {
    /// Set while [`deferring`] runs on this thread.
    static DEFERRING: Cell<bool> = const { Cell::new(false) };
}

/// Run `work` with every write transaction this thread begins on a group
/// store committing *deferred*: visible to readers at once, but durable
/// only once a later durable commit of the same file is (redb's
/// `Durability::None`), so it costs no fsync. Raft applies run this way:
/// the quorum's logs make every applied entry durable, and a member that
/// crashes replays from its log what the file lost. Scratch transactions
/// never commit either way.
pub fn deferring<T>(work: impl FnOnce() -> T) -> T {
    /// Restores the previous setting, on unwinding too.
    struct Restore(bool);
    impl Drop for Restore {
        fn drop(&mut self) {
            DEFERRING.with(|deferring| deferring.set(self.0));
        }
    }
    let _restore = Restore(DEFERRING.with(|deferring| deferring.replace(true)));
    work()
}

/// Map a redb error, keeping whether the disk failed: an I/O error, or
/// redb refusing the handle after one ([`Error::Io`]).
pub(crate) fn storage(error: impl Into<redb::Error>) -> Error {
    let error = error.into();
    match error {
        redb::Error::Io(_) | redb::Error::PreviousIo => Error::Io(error.to_string()),
        _ => Error::storage(error.to_string()),
    }
}

/// One of the ten database tables, for snapshots and bulk operations.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub enum SnapshotTable {
    /// Well-known `meta` names.
    Meta,
    /// Intern ID to name.
    InternId,
    /// Intern name to ID.
    InternName,
    /// Records by collection.
    Records,
    /// Index entries.
    IndexEntries,
    /// Derived outcomes.
    Derived,
    /// Dependency to dependent edges.
    DepEdges,
    /// Dependent to dependency edges.
    DependentEdges,
    /// Retry receipts.
    Receipts,
    /// Code artifacts.
    Code,
}

impl SnapshotTable {
    /// All ten tables in a fixed order.
    pub const ALL: [SnapshotTable; 10] = [
        SnapshotTable::Meta,
        SnapshotTable::InternId,
        SnapshotTable::InternName,
        SnapshotTable::Records,
        SnapshotTable::IndexEntries,
        SnapshotTable::Derived,
        SnapshotTable::DepEdges,
        SnapshotTable::DependentEdges,
        SnapshotTable::Receipts,
        SnapshotTable::Code,
    ];

    /// Stable tag for snapshot framing.
    pub fn tag(self) -> u8 {
        match self {
            SnapshotTable::Meta => 0x00,
            SnapshotTable::InternId => 0x01,
            SnapshotTable::InternName => 0x02,
            SnapshotTable::Records => 0x03,
            SnapshotTable::IndexEntries => 0x04,
            SnapshotTable::Derived => 0x05,
            SnapshotTable::DepEdges => 0x06,
            SnapshotTable::DependentEdges => 0x07,
            SnapshotTable::Receipts => 0x08,
            SnapshotTable::Code => 0x09,
        }
    }

    /// Decode a snapshot framing tag.
    pub fn from_tag(tag: u8) -> Result<SnapshotTable> {
        match tag {
            0x00 => Ok(SnapshotTable::Meta),
            0x01 => Ok(SnapshotTable::InternId),
            0x02 => Ok(SnapshotTable::InternName),
            0x03 => Ok(SnapshotTable::Records),
            0x04 => Ok(SnapshotTable::IndexEntries),
            0x05 => Ok(SnapshotTable::Derived),
            0x06 => Ok(SnapshotTable::DepEdges),
            0x07 => Ok(SnapshotTable::DependentEdges),
            0x08 => Ok(SnapshotTable::Receipts),
            0x09 => Ok(SnapshotTable::Code),
            _ => Err(Error::corrupt("unknown snapshot table tag".to_string())),
        }
    }

    /// Its index among [`SnapshotTable::ALL`].
    pub(crate) fn slot(self) -> usize {
        self.tag() as usize
    }

    /// The table redb names `name`.
    fn named(name: &str) -> Option<SnapshotTable> {
        SnapshotTable::ALL
            .into_iter()
            .find(|table| table.def().name() == name)
    }

    fn def(self) -> TableDefinition<'static, &'static [u8], &'static [u8]> {
        match self {
            SnapshotTable::Meta => META,
            SnapshotTable::InternId => INTERN_ID,
            SnapshotTable::InternName => INTERN_NAME,
            SnapshotTable::Records => RECORDS,
            SnapshotTable::IndexEntries => INDEX_ENTRIES,
            SnapshotTable::Derived => DERIVED,
            SnapshotTable::DepEdges => DEP_EDGES,
            SnapshotTable::DependentEdges => DEPENDENT_EDGES,
            SnapshotTable::Receipts => RECEIPTS,
            SnapshotTable::Code => CODE,
        }
    }
}

/// One group's durable store: a single redb file.
pub struct GroupStore {
    db: Arc<redb::Database>,
    /// Compacts the file online; absent without a file path to measure.
    compactor: Option<Compactor>,
    /// Runs before every durable commit, the compactor's included.
    hook: Arc<DurableHook>,
}

impl GroupStore {
    /// Create a new group file, initializing all ten tables.
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let db = redb::Database::create(path.as_ref()).map_err(storage)?;
        Self::from_database(db, Some(path.as_ref()))
    }

    /// Open an existing group file, creating any missing tables.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let db = redb::Database::open(path.as_ref()).map_err(storage)?;
        Self::from_database(db, Some(path.as_ref()))
    }

    /// Wrap an open redb database, such as one over a custom storage
    /// backend, creating any missing tables. `path` names its file, for
    /// online compaction; without one the store never compacts.
    pub fn from_database(db: redb::Database, path: Option<&Path>) -> Result<Self> {
        let db = Arc::new(db);
        let hook = Arc::new(DurableHook::default());
        let store = GroupStore {
            compactor: path.map(|path| Compactor::hooked(db.clone(), path, hook.clone())),
            db,
            hook,
        };
        store.ensure_tables()?;
        Ok(store)
    }

    /// Run `hook` before every later durable commit of this file, the
    /// compactor's steps included; a failing hook fails the commit, which
    /// changes nothing. A member makes its Raft log durable there, so no
    /// applied entry is durable here before it is in the log (see
    /// [`deferring`]).
    pub fn set_before_durable(&self, hook: BeforeDurable) {
        self.hook.set(hook);
    }

    /// Commit an empty transaction durably, after the
    /// [`GroupStore::set_before_durable`] hook: every commit before it,
    /// deferred ones included, is then durable. Blocking: it fsyncs.
    pub fn flush(&self) -> Result<()> {
        self.hook.run()?;
        self.db
            .begin_write()
            .map_err(storage)?
            .commit()
            .map_err(storage)
    }

    /// The file's online compactor, sharing its handle so compaction
    /// steps run outside whatever lock guards the store's other users.
    /// `None` when the store has no file path.
    pub fn compactor(&self) -> Option<Compactor> {
        self.compactor.clone()
    }

    /// Claim this file for `contract`, the storage contract of the build
    /// opening it: recorded in a file that holds no row yet, compared in
    /// one that does. `Err(Some(other))` names the contract another build
    /// wrote the file under, `Err(None)` says the file holds rows but
    /// records no contract (a build from before files recorded theirs):
    /// its values, and the log entries that made them, would not read as
    /// this build reads them, so the caller refuses to open it.
    pub fn claim_contract(
        &self,
        contract: &str,
    ) -> Result<std::result::Result<(), Option<String>>> {
        let txn = self.db.begin_write().map_err(storage)?;
        let claimed = {
            let mut table = txn.open_table(STORE).map_err(storage)?;
            let stored = ReadableTable::get(&table, STORE_CONTRACT)
                .map_err(storage)?
                .map(|row| String::from_utf8_lossy(row.value()).into_owned());
            match stored {
                Some(stored) if stored == contract => Ok(()),
                Some(stored) => Err(Some(stored)),
                None => {
                    let meta = txn.open_table(META).map_err(storage)?;
                    if ReadableTable::first(&meta).map_err(storage)?.is_some() {
                        Err(None)
                    } else {
                        table
                            .insert(STORE_CONTRACT, contract.as_bytes())
                            .map_err(storage)?;
                        Ok(())
                    }
                }
            }
        };
        txn.commit().map_err(storage)?;
        Ok(claimed)
    }

    fn ensure_tables(&self) -> Result<()> {
        let txn = self.db.begin_write().map_err(storage)?;
        for table in [
            META,
            INTERN_ID,
            INTERN_NAME,
            RECORDS,
            INDEX_ENTRIES,
            DERIVED,
            DEP_EDGES,
            DEPENDENT_EDGES,
            RECEIPTS,
            CODE,
        ] {
            txn.open_table(table).map_err(storage)?;
        }
        txn.commit().map_err(storage)?;
        Ok(())
    }

    /// Begin a read transaction: a pinned snapshot.
    pub fn begin_read(&self) -> Result<ReadTxn> {
        Ok(ReadTxn {
            source: Source::Snapshot(self.db.begin_read().map_err(storage)?),
            reads: None,
        })
    }

    /// Begin a read of committed state seen through `overlay`, recording
    /// what it reads into `reads` when given. Taken while the staged
    /// transaction `overlay` came from is open (it holds the write lock,
    /// so nothing commits under it), it reads exactly what that
    /// transaction held when the overlay was built ([`overlay`]).
    pub fn begin_overlay(
        &self,
        overlay: Arc<Overlay>,
        reads: Option<Arc<Mutex<ReadSet>>>,
    ) -> Result<ReadTxn> {
        Ok(ReadTxn {
            source: Source::Overlay(self.db.begin_read().map_err(storage)?, overlay),
            reads,
        })
    }

    /// Begin a write transaction: commit or abort on drop.
    pub fn begin_write(&self) -> Result<WriteTxn> {
        Ok(WriteTxn {
            txn: self.db.begin_write().map_err(storage)?,
            scratch: false,
            deferred: DEFERRING.with(Cell::get),
            hook: self.hook.clone(),
            history_index: std::sync::atomic::AtomicU64::new(0),
            log: Mutex::new(None),
        })
    }

    /// Begin a write transaction that reads can view before it commits
    /// (see [`StagedWrite`]).
    pub fn begin_staged(&self) -> Result<StagedWrite> {
        Ok(StagedWrite {
            txn: Arc::new(self.begin_write()?),
        })
    }

    /// Begin a staged write that never commits: a writer stages the
    /// writes before the ones it prepares in it and drops it. What
    /// applies in it never happened, so nothing that outlives it (the
    /// engine's change journal) may record it ([`WriteTxn::is_scratch`]).
    pub fn begin_scratch(&self) -> Result<StagedWrite> {
        let mut write = self.begin_write()?;
        write.scratch = true;
        Ok(StagedWrite {
            txn: Arc::new(write),
        })
    }
}

fn get_bytes<T: Rows>(table: &T, key: &[u8]) -> Result<Option<Vec<u8>>> {
    table.get(key)
}

fn table_get<T: Rows>(table: &T, key: &[u8]) -> Result<Option<Vec<u8>>> {
    table.get(key)
}

fn table_put(
    table: &mut Table<&'static [u8], &'static [u8]>,
    key: &[u8],
    value: &[u8],
) -> Result<()> {
    table.insert(key, value).map_err(storage)?;
    Ok(())
}

/// Remove a key, reporting whether it existed. Reads first so the return
/// value never depends on the remove call's version-specific shape.
fn table_delete(table: &mut Table<&'static [u8], &'static [u8]>, key: &[u8]) -> Result<bool> {
    let existed = ReadableTable::get(table, key).map_err(storage)?.is_some();
    if existed {
        table.remove(key).map_err(storage)?;
    }
    Ok(existed)
}

/// Scan one byte scope into owned rows, decoding each key with `decode`.
/// Optional lower (inclusive) and upper (exclusive) full-key bounds
/// narrow the scan; `limit` caps the collected rows; `reverse` iterates
/// the scope backwards. Bounds outside the scope fail instead of
/// scanning the wrong range; crossed bounds scan nothing.
fn scan_prefix<T: Rows>(
    table: &T,
    scope: &[u8],
    lower: Option<&[u8]>,
    upper: Option<&[u8]>,
    limit: Option<usize>,
    reverse: bool,
    decode: impl Fn(&[u8]) -> Result<Vec<u8>>,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    if let Some(lower) = lower
        && !lower.starts_with(scope)
    {
        return Err(Error::corrupt("scan lower bound outside scope"));
    }
    if let Some(upper) = upper
        && !upper.starts_with(scope)
    {
        return Err(Error::corrupt("scan upper bound outside scope"));
    }
    if let (Some(lower), Some(upper)) = (lower, upper)
        && lower >= upper
    {
        return Ok(Vec::new());
    }
    let mut rows = Vec::new();
    let take = limit.unwrap_or(usize::MAX);
    if take == 0 {
        return Ok(rows);
    }
    let start = lower.unwrap_or(scope);
    // The scope's end bounds the walk, so a recorded walk covers the
    // scope and no more; an all-`0xFF` scope has none, and every key at
    // or past it is inside it.
    let end = match upper {
        Some(upper) => Some(upper.to_vec()),
        None => prefix_end(scope),
    };
    table.walk(start, end.as_deref(), reverse, &mut |key, value| {
        if !key.starts_with(scope) {
            return Ok(false);
        }
        rows.push((decode(key)?, value.to_vec()));
        Ok(rows.len() < take)
    })?;
    Ok(rows)
}

/// One collection's records in key order: `(key bytes, canonical JSON)`.
fn scan_records_in<T: Rows>(
    table: &T,
    db: &[u8],
    collection: u32,
    lower: Option<&[u8]>,
    upper: Option<&[u8]>,
    limit: Option<usize>,
    reverse: bool,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let prefix = record_prefix(db, collection)?;
    let rows = scan_prefix(table, &prefix, lower, upper, limit, reverse, |key| {
        decode_record_suffix(key, prefix.len())
    })?;
    rows.into_iter()
        .map(|(key, packed)| Ok((key, unpack_value(&packed)?)))
        .collect()
}

/// One derived outcome by definition and arguments.
fn get_derived_in<T: Rows>(
    table: &T,
    db: &[u8],
    definition: u32,
    args: &[u8],
) -> Result<Option<DerivedOutcome>> {
    let key = encode_derived_key(db, definition, args)?;
    match table.get(key.as_slice())? {
        None => Ok(None),
        Some(bytes) => Ok(Some(decode_outcome(&bytes)?)),
    }
}

/// One definition's derived outcomes in argument-key order, resuming
/// past the argument bytes `after`: `(args, outcome)`, at most `limit`.
fn scan_derived_in<T: Rows>(
    table: &T,
    db: &[u8],
    definition: u32,
    after: Option<&[u8]>,
    limit: usize,
) -> Result<Vec<(Vec<u8>, DerivedOutcome)>> {
    let mut prefix = encode_db(db)?;
    prefix.push(keys::DEP_DERIVED);
    prefix.extend_from_slice(&definition.to_be_bytes());
    let lower = after
        .map(|args| encode_derived_key(db, definition, args))
        .transpose()?;
    let take = limit.saturating_add(usize::from(lower.is_some()));
    let rows = scan_prefix(
        table,
        &prefix,
        lower.as_deref(),
        None,
        Some(take),
        false,
        |key| Ok(key.to_vec()),
    )?;
    let mut outcomes = Vec::with_capacity(rows.len().min(limit));
    for (key, value) in rows {
        if outcomes.len() >= limit {
            break;
        }
        if lower.as_deref() == Some(key.as_slice()) {
            continue;
        }
        let (_, args) = keys::decode_derived_key(db, &key)?;
        outcomes.push((args, decode_outcome(&value)?));
    }
    Ok(outcomes)
}

/// Dependents of every dependency key below `dependency_prefix` (a whole
/// dependency key, or a leading part of one): `(definition ID, args)`.
fn dependents_in<T: Rows>(
    table: &T,
    db: &[u8],
    dependency_prefix: &[u8],
) -> Result<Vec<DerivedInstance>> {
    let skip = encode_db(db)?.len();
    let mut prefix = encode_db(db)?;
    prefix.extend_from_slice(dependency_prefix);
    let rows = scan_prefix(table, &prefix, None, None, None, false, |key| {
        Ok(key.to_vec())
    })?;
    rows.into_iter()
        .map(|(key, _)| {
            let (_, consumed) = decode_dep_key(&key[skip..])?;
            decode_dependent(&key[skip + consumed..])
        })
        .collect()
}

/// Every dependency edge of one database: `(dependency key, (definition
/// ID, args))` pairs.
fn scan_dep_edges_in<T: Rows>(table: &T, db: &[u8]) -> Result<Vec<DepEdge>> {
    let prefix = encode_db(db)?;
    let rows = scan_prefix(table, &prefix, None, None, None, false, |key| {
        Ok(key.to_vec())
    })?;
    rows.into_iter()
        .map(|(key, _)| {
            let suffix = key
                .get(prefix.len()..)
                .ok_or_else(|| Error::corrupt("dep edge outside database"))?;
            let (_, consumed) = decode_dep_key(suffix)?;
            let dependent = decode_dependent(&suffix[consumed..])?;
            Ok((suffix[..consumed].to_vec(), dependent))
        })
        .collect()
}

/// What a [`ReadTxn`] reads: a pinned snapshot of committed state, or
/// the state an uncommitted write transaction staged.
enum Source {
    /// A pinned snapshot over the group file.
    Snapshot(redb::ReadTransaction),
    /// An uncommitted write transaction, its own writes included.
    Staged(Arc<WriteTxn>),
    /// A pinned snapshot seen through the rows a staged transaction
    /// wrote since it began (see [`overlay`]).
    Overlay(redb::ReadTransaction, Arc<Overlay>),
}

/// Open table `$which` (a [`SnapshotTable`]) on a read's source as
/// `$table` and evaluate `$body` with it: the body is written once and
/// compiled for each table type. The table records what the body
/// observes when the read records (see [`ReadTxn::recording`]), unless
/// asked `unrecorded`.
macro_rules! with_table {
    ($read:expr, $which:expr, |$table:ident| $body:expr) => {
        with_table!(@open $read, $which, $read.recording($which), |$table| $body)
    };
    (unrecorded $read:expr, $which:expr, |$table:ident| $body:expr) => {
        with_table!(@open $read, $which, None, |$table| $body)
    };
    (@open $read:expr, $which:expr, $record:expr, |$table:ident| $body:expr) => {
        match &$read.source {
            Source::Snapshot(txn) => {
                let rows: ReadOnlyTable<&'static [u8], &'static [u8]> =
                    txn.open_table($which.def()).map_err(storage)?;
                let $table = Recorded {
                    rows,
                    record: $record,
                };
                $body
            }
            Source::Staged(write) => {
                let $table = Recorded {
                    rows: write.table($which.def())?,
                    record: $record,
                };
                $body
            }
            Source::Overlay(txn, overlay) => {
                let $table = Recorded {
                    rows: Layered {
                        base: txn.open_table($which.def()).map_err(storage)?,
                        overlay: overlay.table($which),
                    },
                    record: $record,
                };
                $body
            }
        }
    };
}

/// Pinned read snapshot over the group file, a view of what an
/// uncommitted [`StagedWrite`] holds, or a snapshot seen through a
/// staged transaction's [`Overlay`]; recording what it reads into a
/// [`ReadSet`] when asked.
pub struct ReadTxn {
    source: Source,
    reads: Option<Arc<Mutex<ReadSet>>>,
}

impl ReadTxn {
    /// A read outside the snapshot cannot be protected by row/range locks.
    pub(crate) fn record_untracked(&self) {
        if let Some(reads) = &self.reads {
            reads
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .invalidate();
        }
    }
    /// Record this view's reads into the supplied transaction certificate.
    pub(crate) fn recording_into(mut self, reads: Option<Arc<Mutex<ReadSet>>>) -> Self {
        self.reads = reads;
        self
    }
    /// Where a table of this read records what it observes.
    fn recording(&self, table: SnapshotTable) -> Option<(&Mutex<ReadSet>, SnapshotTable)> {
        self.reads.as_deref().map(|set| (set, table))
    }

    /// Whether this is a plain pinned snapshot of committed state that
    /// records nothing: neither a staged transaction's view nor an
    /// overlay, whose state no change journal follows, nor a recording
    /// read, whose reads a speculation's certificate must hold.
    pub fn is_committed_snapshot(&self) -> bool {
        matches!(self.source, Source::Snapshot(_)) && self.reads.is_none()
    }

    /// What this read observed so far, when it records.
    pub fn observed(&self) -> Option<ReadSet> {
        self.reads
            .as_ref()
            .map(|set| set.lock().unwrap_or_else(PoisonError::into_inner).clone())
    }

    /// The revision of `db` a patch prepared on this read starts from,
    /// read without recording it: a speculative preparation's patch
    /// moves to the revision it lands on as it is adopted
    /// (`crate::speculation`), so having read it decides nothing.
    pub fn base_revision(&self, db: &[u8]) -> Result<u64> {
        let key = encode_meta_key(db, meta::REVISION)?;
        let bytes = with_table!(unrecorded self, SnapshotTable::Meta, |table| get_bytes(
            &table, &key
        ))?;
        match bytes {
            None => Ok(0),
            Some(bytes) => decode_u64(&bytes),
        }
    }

    /// Resolve an interned name to its 32-bit ID.
    pub fn intern_id(&self, db: &[u8], name: &[u8]) -> Result<Option<u32>> {
        let key = encode_intern_name_key(db, name)?;
        with_table!(
            self,
            SnapshotTable::InternName,
            |table| match get_bytes(&table, &key)? {
                None => Ok(None),
                Some(bytes) => Ok(Some(decode_u32(&bytes)?)),
            }
        )
    }

    /// Resolve a 32-bit intern ID back to its name bytes.
    pub fn intern_name(&self, db: &[u8], id: u32) -> Result<Option<Vec<u8>>> {
        let key = encode_intern_id_key(db, id)?;
        with_table!(self, SnapshotTable::InternId, |table| get_bytes(
            &table, &key
        ))
    }

    /// Read one record: decompressed canonical JSON bytes.
    pub fn get_record(&self, db: &[u8], collection: u32, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let full = encode_record_key(db, collection, key)?;
        with_table!(
            self,
            SnapshotTable::Records,
            |table| match get_bytes(&table, &full)? {
                None => Ok(None),
                Some(packed) => Ok(Some(unpack_value(&packed)?)),
            }
        )
    }

    /// Scan one collection in key order: `(key bytes, canonical JSON)`.
    /// Optional full-key bounds narrow the scan, `limit` caps the rows,
    /// and `reverse` iterates backwards.
    pub fn scan_records(
        &self,
        db: &[u8],
        collection: u32,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
        limit: Option<usize>,
        reverse: bool,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        with_table!(self, SnapshotTable::Records, |table| scan_records_in(
            &table, db, collection, lower, upper, limit, reverse
        ))
    }

    /// SQL scan: enforce candidate and decoded byte ceilings before copying
    /// or decompressing a row. `None` means exhaustion, with no partial page.
    pub(crate) fn scan_records_bounded(
        &self,
        db: &[u8],
        collection: u32,
        candidates: usize,
        bytes: usize,
    ) -> Result<Option<Vec<Row>>> {
        let scope = record_prefix(db, collection)?;
        let end = prefix_end(&scope);
        with_table!(self, SnapshotTable::Records, |table| {
            let mut rows = Vec::new();
            let mut used = 0usize;
            let mut exhausted = false;
            table.walk(&scope, end.as_deref(), false, &mut |key, packed| {
                if !key.starts_with(&scope) {
                    return Ok(false);
                }
                if rows.len() >= candidates {
                    exhausted = true;
                    return Ok(false);
                }
                let size = codec::unpacked_size(packed)?;
                used = used
                    .saturating_add(key.len())
                    .saturating_add(size)
                    .saturating_add(128);
                if used > bytes {
                    exhausted = true;
                    return Ok(false);
                }
                rows.push((
                    decode_record_suffix(key, scope.len())?,
                    unpack_value(packed)?,
                ));
                Ok(true)
            })?;
            Ok(if exhausted { None } else { Some(rows) })
        })
    }

    /// Bounded exact read: outer `None` means byte exhaustion, inner `None`
    /// means absent. Walking one exact key checks expansion size before copying
    /// its compressed bytes, and retains normal overlay/read-set observation.
    pub(crate) fn get_record_bounded(
        &self,
        db: &[u8],
        collection: u32,
        key: &[u8],
        bytes: usize,
    ) -> Result<Option<Option<Vec<u8>>>> {
        let full = encode_record_key(db, collection, key)?;
        let mut end = full.clone();
        end.push(0);
        with_table!(self, SnapshotTable::Records, |table| {
            let mut row = None;
            let mut exhausted = false;
            table.walk(&full, Some(&end), false, &mut |_, packed| {
                if codec::unpacked_size(packed)? > bytes {
                    exhausted = true;
                } else {
                    row = Some(unpack_value(packed)?);
                }
                Ok(false)
            })?;
            Ok(if exhausted { None } else { Some(row) })
        })
    }

    /// Read primary keys in a complete index equality bucket, bounding candidate
    /// entries and bytes before cloning. No policy or output limit is applied.
    pub(crate) fn index_keys_bounded(
        &self,
        db: &[u8],
        index: u32,
        components: &[&[u8]],
        limit: usize,
        bytes: usize,
    ) -> Result<Option<Vec<Vec<u8>>>> {
        let scope = index_prefix(db, index, components)?;
        let end = prefix_end(&scope);
        with_table!(self, SnapshotTable::IndexEntries, |table| {
            let mut keys = Vec::new();
            let mut used = 0usize;
            let mut exhausted = false;
            table.walk(&scope, end.as_deref(), false, &mut |full, _| {
                if !full.starts_with(&scope) {
                    return Ok(false);
                }
                used = used.saturating_add(full.len()).saturating_add(128);
                if keys.len() >= limit || used > bytes {
                    exhausted = true;
                    return Ok(false);
                }
                keys.push(decode_record_suffix(full, scope.len())?);
                Ok(true)
            })?;
            Ok(if exhausted { None } else { Some(keys) })
        })
    }

    /// Scan collection records whose canonical key has a given byte prefix.
    /// The scan seeks one range per key length, rather than visiting unrelated
    /// keys. `after` is a decoded record key, exclusive.
    pub fn scan_records_with_key_prefix(
        &self,
        db: &[u8],
        collection: u32,
        key_prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        with_table!(self, SnapshotTable::Records, |table| scan_with_key_prefix(
            &table, db, collection, key_prefix, after, limit
        ))
    }

    /// Scan one index below an encoded component prefix:
    /// `(index components, primary key)`, within `range`.
    pub fn scan_index(
        &self,
        db: &[u8],
        index: u32,
        arity: usize,
        component_prefix: &[&[u8]],
        range: ScanRange,
    ) -> Result<Vec<IndexEntry>> {
        let prefix = index_prefix(db, index, component_prefix)?;
        let base = encode_db(db)?.len() + 4;
        let ScanRange {
            lower,
            upper,
            limit,
            reverse,
        } = range;
        let rows = with_table!(self, SnapshotTable::IndexEntries, |table| scan_prefix(
            &table,
            &prefix,
            lower,
            upper,
            limit,
            reverse,
            |key| Ok(key.to_vec())
        ))?;
        rows.into_iter()
            .map(|(key, _)| decode_index_entry(&key, base, arity))
            .collect()
    }

    /// Read one derived outcome.
    pub fn get_derived(
        &self,
        db: &[u8],
        definition: u32,
        args: &[u8],
    ) -> Result<Option<DerivedOutcome>> {
        let outcome = with_table!(self, SnapshotTable::Derived, |table| get_derived_in(
            &table, db, definition, args
        ))?;
        if let Some(reads) = &self.reads {
            let key = encode_derived_key(db, definition, args)?;
            let needed = reads
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .needs_derived_sources(&key);
            if needed {
                // Use the same snapshot as the cached value. Its transitive
                // source proof travels with the prepare, never rebuilt from a
                // follower's independently populated derived cache.
                let certificate = crate::certificate::Certificate::observe(
                    self,
                    &crate::session::Staged::default(),
                    db,
                    vec![keys::encode_derived_dep(definition, args)?],
                    None,
                    0,
                )?;
                reads
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .derived_sources(&key, certificate);
            }
        }
        Ok(outcome)
    }

    /// One definition's derived outcomes in argument order, resuming
    /// past the argument bytes `after`: at most `limit` `(args,
    /// outcome)` pairs.
    pub fn scan_derived(
        &self,
        db: &[u8],
        definition: u32,
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, DerivedOutcome)>> {
        with_table!(self, SnapshotTable::Derived, |table| scan_derived_in(
            &table, db, definition, after, limit
        ))
    }

    /// List the dependents of one dependency: `(definition ID, args)` pairs.
    pub fn dependents(&self, db: &[u8], dependency: &[u8]) -> Result<Vec<DerivedInstance>> {
        with_table!(self, SnapshotTable::DepEdges, |table| dependents_in(
            &table, db, dependency
        ))
    }

    /// Read a dependent's prior edge list for replacement.
    pub fn dependent_edges(&self, db: &[u8], definition: u32, args: &[u8]) -> Result<Vec<Vec<u8>>> {
        let key = encode_derived_key(db, definition, args)?;
        with_table!(
            self,
            SnapshotTable::DependentEdges,
            |table| match get_bytes(&table, &key)? {
                None => Ok(Vec::new()),
                Some(bytes) => decode_edge_list(&bytes),
            }
        )
    }

    /// Scan every dependency edge of one database: `(dependency key,
    /// (definition ID, args))` pairs. Range invalidation matches bounds in
    /// memory over this scan.
    pub fn scan_dep_edges(&self, db: &[u8]) -> Result<Vec<DepEdge>> {
        with_table!(self, SnapshotTable::DepEdges, |table| scan_dep_edges_in(
            &table, db
        ))
    }

    /// Read one receipt's opaque bytes.
    pub fn get_receipt(&self, db: &[u8], identity: &[u8]) -> Result<Option<Vec<u8>>> {
        let key = encode_receipt_key(db, identity)?;
        with_table!(self, SnapshotTable::Receipts, |table| get_bytes(
            &table, &key
        ))
    }

    /// All `(identity, receipt)` pairs for one database, for retention
    /// sweeps. Identities carry their namespace prefix, if any.
    pub fn scan_receipts(&self, db: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let rows = self.scan_db_raw(SnapshotTable::Receipts, db)?;
        let skip = encode_db(db)?.len();
        let mut receipts = Vec::with_capacity(rows.len());
        for (key, value) in rows {
            let mut reader = Reader::new(&key[skip..]);
            receipts.push((reader.sized()?, value));
        }
        Ok(receipts)
    }

    /// Up to `limit` `(identity, receipt)` pairs of one database in
    /// identity order, starting past the identity `after` (from the
    /// first when `None`), plus whether the scan reached the end: paced
    /// collection walks the table a page at a time.
    pub fn receipts_page(
        &self,
        db: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<(Vec<Row>, bool)> {
        with_table!(self, SnapshotTable::Receipts, |table| receipts_page_in(
            &table, db, after, limit
        ))
    }

    /// Read one code artifact: Wasm bytes plus validated manifest.
    pub fn get_code(&self, db: &[u8], hash: &[u8; HASH_LEN]) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let key = encode_code_key(db, hash)?;
        with_table!(
            self,
            SnapshotTable::Code,
            |table| match get_bytes(&table, &key)? {
                None => Ok(None),
                Some(bytes) => Ok(Some(decode_code(&bytes)?)),
            }
        )
    }

    /// Read a raw `meta` value.
    pub fn meta(&self, db: &[u8], name: &[u8]) -> Result<Option<Vec<u8>>> {
        let key = encode_meta_key(db, name)?;
        with_table!(self, SnapshotTable::Meta, |table| get_bytes(&table, &key))
    }

    /// Read a `meta` scalar, defaulting to zero when absent.
    pub fn meta_u64(&self, db: &[u8], name: &[u8]) -> Result<u64> {
        match self.meta(db, name)? {
            None => Ok(0),
            Some(bytes) => decode_u64(&bytes),
        }
    }

    /// Raw `(key, value)` pairs below one database prefix in one table,
    /// for snapshots. Keys are full table keys.
    pub fn scan_db_raw(&self, table: SnapshotTable, db: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let prefix = encode_db(db)?;
        with_table!(self, table, |handle| scan_prefix(
            &handle,
            &prefix,
            None,
            None,
            None,
            false,
            |key| Ok(key.to_vec())
        ))
    }

    /// Raw `(name, value)` meta pairs whose name starts with `prefix`.
    /// A recording read records the names it matched, not the walk over
    /// every name of the database: other names (the revision, the
    /// applied position) move with every write.
    pub fn scan_meta_prefix(&self, db: &[u8], prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let rows = with_table!(unrecorded self, SnapshotTable::Meta, |table| {
            scan_meta_prefix_in(&table, db, prefix)
        })?;
        if let Some(set) = &self.reads {
            set.lock()
                .unwrap_or_else(PoisonError::into_inner)
                .meta_names(encode_db(db)?, prefix);
        }
        Ok(rows)
    }

    /// At most `limit` raw `(key, value)` pairs below one database prefix
    /// in one table, in key order, after full key `after` when given:
    /// [`ReadTxn::scan_db_raw`] a page at a time, for moving a database.
    pub fn scan_db_raw_page(
        &self,
        table: SnapshotTable,
        db: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let prefix = encode_db(db)?;
        // The least key greater than `after`.
        let lower = after.map(|after| {
            let mut next = after.to_vec();
            next.push(0);
            next
        });
        with_table!(self, table, |handle| scan_prefix(
            &handle,
            &prefix,
            lower.as_deref(),
            None,
            Some(limit),
            false,
            |key| Ok(key.to_vec())
        ))
    }

    /// At most `limit` raw `(key, value)` pairs of one table whose full
    /// key starts with `prefix`, in key order, after full key `after`
    /// when given: one structure's keys a page at a time, as a module
    /// purge walks them.
    pub fn scan_raw_prefix(
        &self,
        table: SnapshotTable,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        // The least key greater than `after`.
        let lower = after.map(|after| {
            let mut next = after.to_vec();
            next.push(0);
            next
        });
        with_table!(self, table, |handle| scan_prefix(
            &handle,
            prefix,
            lower.as_deref(),
            None,
            Some(limit),
            false,
            |key| Ok(key.to_vec())
        ))
    }
}

/// One page of a database's receipts (see [`ReadTxn::receipts_page`]).
fn receipts_page_in<T: Rows>(
    table: &T,
    db: &[u8],
    after: Option<&[u8]>,
    limit: usize,
) -> Result<(Vec<Row>, bool)> {
    let scope = encode_db(db)?;
    let lower = after
        .map(|after| encode_receipt_key(db, after))
        .transpose()?;
    // A page and one more row tell whether more follow; the scan starts
    // at `after` itself, which may still be there and is skipped.
    let wanted = limit.saturating_add(1 + usize::from(after.is_some()));
    let rows = scan_prefix(
        table,
        &scope,
        lower.as_deref(),
        None,
        Some(wanted),
        false,
        |key| Ok(key.to_vec()),
    )?;
    let mut page = Vec::with_capacity(rows.len().min(limit));
    for (key, value) in rows {
        let mut reader = Reader::new(&key[scope.len()..]);
        let identity = reader.sized()?;
        if Some(identity.as_slice()) == after {
            continue;
        }
        if page.len() == limit {
            return Ok((page, false));
        }
        page.push((identity, value));
    }
    Ok((page, true))
}

/// Scan the records of one collection whose stored key bytes start with
/// `key_prefix` (see [`ReadTxn::scan_records_with_key_prefix`]).
fn scan_with_key_prefix<T: Rows>(
    table: &T,
    db: &[u8],
    collection: u32,
    key_prefix: &[u8],
    after: Option<&[u8]>,
    limit: usize,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let base = record_prefix(db, collection)?;
    let class = |len: usize| record_key_class(db, collection, len, key_prefix);
    let after_full = after
        .map(|key| encode_record_key(db, collection, key))
        .transpose()?;
    let mut len = match after {
        Some(key) if key.starts_with(key_prefix) => key.len(),
        Some(_) => return Err(Error::corrupt("scan cursor outside the key prefix")),
        None => key_prefix.len(),
    };
    let mut rows = Vec::new();
    while rows.len() < limit {
        let start = class(len)?;
        let lower = match &after_full {
            Some(after) if *after > start => after.clone(),
            _ => start.clone(),
        };
        let upper = prefix_end(&start);
        let wanted = (limit - rows.len()).saturating_add(1);
        for (key, value) in scan_records_in(
            table,
            db,
            collection,
            Some(&lower),
            upper.as_deref(),
            Some(wanted),
            false,
        )? {
            if Some(key.as_slice()) == after || rows.len() >= limit {
                continue;
            }
            rows.push((key, value));
        }
        if rows.len() >= limit {
            break;
        }
        // Seek to the shortest key longer than this class: the next
        // length present anywhere in the collection.
        let Some(next) = len.checked_add(1) else {
            break;
        };
        let probe = record_key_class(db, collection, next, b"")?;
        let found = scan_prefix(table, &base, Some(&probe), None, Some(1), false, |key| {
            decode_record_suffix(key, base.len())
        })?;
        match found.first() {
            None => break,
            Some((key, _)) => len = key.len(),
        }
    }
    Ok(rows)
}

/// Raw `(name, value)` meta pairs whose name starts with `prefix`.
/// Names are length-prefixed in keys, so this walks the database's
/// whole `meta` range, which holds only well-known names, code
/// identities and staging rows.
fn scan_meta_prefix_in<T: Rows>(
    table: &T,
    db: &[u8],
    prefix: &[u8],
) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let scope = encode_db(db)?;
    let rows = scan_prefix(table, &scope, None, None, None, false, |key| {
        Ok(key.to_vec())
    })?;
    let mut matched = Vec::new();
    for (key, value) in rows {
        let mut reader = Reader::new(&key[scope.len()..]);
        let name = reader.sized()?;
        if name.starts_with(prefix) {
            matched.push((name, value));
        }
    }
    Ok(matched)
}

/// Record reads shared by read and write transactions, so record-level
/// codecs (timers, queues) serve both without duplicating lookups.
pub trait RecordRead {
    /// Read one record: decompressed canonical JSON bytes.
    fn record(&self, db: &[u8], collection: u32, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Scan one collection between optional inclusive-lower and
    /// exclusive-upper full keys, at most `limit` rows, in key order.
    fn records(
        &self,
        db: &[u8],
        collection: u32,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
        limit: Option<usize>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;
}

impl RecordRead for ReadTxn {
    fn record(&self, db: &[u8], collection: u32, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.get_record(db, collection, key)
    }

    fn records(
        &self,
        db: &[u8],
        collection: u32,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
        limit: Option<usize>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.scan_records(db, collection, lower, upper, limit, false)
    }
}

impl RecordRead for WriteTxn {
    fn record(&self, db: &[u8], collection: u32, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.get_record(db, collection, key)
    }

    fn records(
        &self,
        db: &[u8],
        collection: u32,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
        limit: Option<usize>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.scan_records(db, collection, lower, upper, limit, false)
    }
}

fn decode_u32(bytes: &[u8]) -> Result<u32> {
    if bytes.len() != 4 {
        return Err(Error::corrupt("bad u32 length"));
    }
    Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

/// Write transaction over the group file: commit or abort on drop.
pub struct WriteTxn {
    txn: redb::WriteTransaction,
    /// Begun by [`GroupStore::begin_scratch`]: never commits.
    scratch: bool,
    /// Begun inside [`deferring`]: commits without an fsync.
    deferred: bool,
    /// The store's [`BeforeDurable`] hook, run before a durable commit.
    hook: Arc<DurableHook>,
    /// The log index receipts committed in this transaction record, when
    /// it is not the position the entry applies at; 0 when it is.
    history_index: std::sync::atomic::AtomicU64,
    /// Every row put or deleted, in order, once [`StagedWrite::log_writes`]
    /// asked for it.
    log: Mutex<Option<Vec<Write>>>,
}

/// A write transaction that [`ReadTxn`] views can share before it
/// commits: a writer stages earlier writes in it and prepares later ones
/// against what they left, and a batch apply checks each item against
/// the items before it. Dropping every handle uncommitted discards it
/// all. Views open one table at a time, so they share the transaction
/// on one thread.
#[derive(Clone)]
pub struct StagedWrite {
    txn: Arc<WriteTxn>,
}

impl StagedWrite {
    /// The transaction, for writes.
    pub fn write(&self) -> &WriteTxn {
        &self.txn
    }

    /// A read of everything committed before the transaction began plus
    /// what it staged since, including writes made after this call.
    pub fn read(&self) -> ReadTxn {
        ReadTxn {
            source: Source::Staged(self.txn.clone()),
            reads: None,
        }
    }

    /// Log every row the transaction puts or deletes from now on, for
    /// overlays and validation (see [`overlay`]).
    pub fn log_writes(&self) {
        let mut log = self.txn.log.lock().unwrap_or_else(PoisonError::into_inner);
        if log.is_none() {
            *log = Some(Vec::new());
        }
    }

    /// How many rows the log holds: a mark for [`StagedWrite::logged`].
    pub fn log_mark(&self) -> usize {
        self.txn
            .log
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .map_or(0, Vec::len)
    }

    /// Run `check` on the rows logged since `mark`, in order.
    pub fn logged<T>(&self, mark: usize, check: impl FnOnce(&[Write]) -> T) -> T {
        let log = self.txn.log.lock().unwrap_or_else(PoisonError::into_inner);
        let rows = log.as_deref().unwrap_or_default();
        check(rows.get(mark..).unwrap_or_default())
    }

    /// Bring `overlay` up to the transaction's current state for the
    /// rows logged since `mark`, reading back what it holds of each;
    /// returns the new mark.
    pub fn extend_overlay(&self, overlay: &mut Overlay, mark: usize) -> Result<usize> {
        let rows: Vec<Write> = self.logged(mark, <[Write]>::to_vec);
        let next = mark + rows.len();
        let mut by_table: Vec<Vec<Vec<u8>>> = vec![Vec::new(); SnapshotTable::ALL.len()];
        for (table, key) in rows {
            by_table[table.slot()].push(key);
        }
        for (table, keys) in SnapshotTable::ALL.into_iter().zip(by_table) {
            if keys.is_empty() {
                continue;
            }
            let handle = self.txn.table(table.def())?;
            for key in keys {
                let value = table_get(&handle, &key)?;
                overlay.set(table, key, value);
            }
        }
        Ok(next)
    }

    /// Commit durably. Fails while another handle or a view is alive.
    pub fn commit(self) -> Result<()> {
        match Arc::try_unwrap(self.txn) {
            Ok(write) => write.commit(),
            Err(_) => Err(Error::corrupt(
                "a staged write is still shared at commit".to_string(),
            )),
        }
    }
}

impl WriteTxn {
    fn table(
        &self,
        def: TableDefinition<&'static [u8], &'static [u8]>,
    ) -> Result<Table<'_, &'static [u8], &'static [u8]>> {
        self.txn.open_table(def).map_err(storage)
    }

    /// Note one row put or deleted in the log, when it logs.
    fn note(&self, table: &Table<'_, &'static [u8], &'static [u8]>, key: &[u8]) {
        let mut log = self.log.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(log) = log.as_mut() {
            match SnapshotTable::named(table.name()) {
                Some(which) => log.push((which, key.to_vec())),
                // Every table is one of the ten: a write to any other
                // would be invisible to overlays, so none may be made.
                None => unreachable!("a write to an unknown table"),
            }
        }
    }

    /// Put one row, logging it.
    fn put_row(
        &self,
        table: &mut Table<'_, &'static [u8], &'static [u8]>,
        key: &[u8],
        value: &[u8],
    ) -> Result<()> {
        table_put(table, key, value)?;
        self.note(table, key);
        Ok(())
    }

    /// Delete one row, logging it when it existed.
    fn delete_row(
        &self,
        table: &mut Table<'_, &'static [u8], &'static [u8]>,
        key: &[u8],
    ) -> Result<bool> {
        let existed = table_delete(table, key)?;
        if existed {
            self.note(table, key);
        }
        Ok(existed)
    }

    /// Commit the transaction: durably, after the store's
    /// [`BeforeDurable`] hook, or deferred when it began inside
    /// [`deferring`]. A scratch transaction refuses.
    pub fn commit(mut self) -> Result<()> {
        if self.scratch {
            return Err(Error::corrupt("a scratch write never commits".to_string()));
        }
        if self.deferred {
            self.txn
                .set_durability(redb::Durability::None)
                .map_err(storage)?;
        } else {
            self.hook.run()?;
        }
        self.txn.commit().map_err(storage)?;
        Ok(())
    }

    /// Whether this transaction commits deferred (see [`deferring`]).
    pub fn is_deferred(&self) -> bool {
        self.deferred
    }

    /// Whether this transaction is scratch ([`GroupStore::begin_scratch`]):
    /// what applies in it is dropped, never committed.
    pub fn is_scratch(&self) -> bool {
        self.scratch
    }

    /// Record `index` in the receipts this transaction commits instead of
    /// the position its entry applies at. A point-in-time restore replays
    /// an archived entry at a fresh position of its own history, while
    /// the outcome committed, and its caller was answered, at the
    /// entry's archived index.
    pub fn record_history_index(&self, index: u64) {
        self.history_index
            .store(index, std::sync::atomic::Ordering::Relaxed);
    }

    /// The log index a receipt committed at `position` records.
    pub fn receipt_index(&self, position: u64) -> u64 {
        match self
            .history_index
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            0 => position,
            index => index,
        }
    }

    /// Read one record inside the write transaction: decompressed
    /// canonical JSON bytes.
    pub fn get_record(&self, db: &[u8], collection: u32, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let table = self.table(RECORDS)?;
        let full = encode_record_key(db, collection, key)?;
        match table_get(&table, &full)? {
            None => Ok(None),
            Some(packed) => Ok(Some(unpack_value(&packed)?)),
        }
    }

    /// Scan one collection inside the write transaction, seeing its own
    /// writes; arguments as [`ReadTxn::scan_records`].
    pub fn scan_records(
        &self,
        db: &[u8],
        collection: u32,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
        limit: Option<usize>,
        reverse: bool,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let table = self.table(RECORDS)?;
        scan_records_in(&table, db, collection, lower, upper, limit, reverse)
    }

    /// Read one derived outcome inside the write transaction.
    pub fn get_derived(
        &self,
        db: &[u8],
        definition: u32,
        args: &[u8],
    ) -> Result<Option<DerivedOutcome>> {
        let table = self.table(DERIVED)?;
        get_derived_in(&table, db, definition, args)
    }

    /// One definition's derived outcomes inside the write transaction,
    /// as [`ReadTxn::scan_derived`].
    pub fn scan_derived(
        &self,
        db: &[u8],
        definition: u32,
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, DerivedOutcome)>> {
        let table = self.table(DERIVED)?;
        scan_derived_in(&table, db, definition, after, limit)
    }

    /// A dependent's stored edge list inside the write transaction.
    pub fn dependent_edges(&self, db: &[u8], definition: u32, args: &[u8]) -> Result<Vec<Vec<u8>>> {
        let table = self.table(DEPENDENT_EDGES)?;
        let key = encode_derived_key(db, definition, args)?;
        match table_get(&table, &key)? {
            None => Ok(Vec::new()),
            Some(bytes) => decode_edge_list(&bytes),
        }
    }

    /// Dependents of every dependency key below `dependency_prefix` (a
    /// whole dependency key or a leading part of one) inside the write
    /// transaction: `(definition ID, args)` pairs.
    pub fn dependents(&self, db: &[u8], dependency_prefix: &[u8]) -> Result<Vec<DerivedInstance>> {
        let table = self.table(DEP_EDGES)?;
        dependents_in(&table, db, dependency_prefix)
    }

    /// Every dependency edge of one database inside the write
    /// transaction, as [`ReadTxn::scan_dep_edges`].
    pub fn scan_dep_edges(&self, db: &[u8]) -> Result<Vec<DepEdge>> {
        let table = self.table(DEP_EDGES)?;
        scan_dep_edges_in(&table, db)
    }

    /// Resolve an interned name inside the write transaction without
    /// allocating.
    pub fn intern_id(&self, db: &[u8], name: &[u8]) -> Result<Option<u32>> {
        let names = self.table(INTERN_NAME)?;
        let key = encode_intern_name_key(db, name)?;
        table_get(&names, &key)?
            .map(|bytes| decode_u32(&bytes))
            .transpose()
    }

    /// Resolve an interned ID back to its name inside the write
    /// transaction.
    pub fn intern_name(&self, db: &[u8], id: u32) -> Result<Option<Vec<u8>>> {
        let ids = self.table(INTERN_ID)?;
        table_get(&ids, &encode_intern_id_key(db, id)?)
    }

    /// Remove an ID's binding in both directions, as when a staging
    /// that allocated it locally is abandoned before any activation
    /// replicated it. Only for IDs nothing replicated refers to: the
    /// caller purges whatever this member stored under the ID first.
    /// The counter stays put, so the ID is never reallocated locally;
    /// an authoritative binding may still claim it. Unbound IDs are a
    /// no-op.
    pub fn release_intern(&self, db: &[u8], id: u32) -> Result<()> {
        let mut ids = self.table(INTERN_ID)?;
        let id_key = encode_intern_id_key(db, id)?;
        let Some(name) = table_get(&ids, &id_key)? else {
            return Ok(());
        };
        self.delete_row(&mut ids, &id_key)?;
        drop(ids);
        let mut names = self.table(INTERN_NAME)?;
        let key = encode_intern_name_key(db, &name)?;
        let bound = table_get(&names, &key)?
            .map(|bytes| decode_u32(&bytes))
            .transpose()?;
        if bound == Some(id) {
            self.delete_row(&mut names, &key)?;
        }
        Ok(())
    }

    /// Intern a name, allocating a fresh monotonic ID on first use.
    pub fn intern(&self, db: &[u8], name: &[u8]) -> Result<u32> {
        let mut names = self.table(INTERN_NAME)?;
        let key = encode_intern_name_key(db, name)?;
        if let Some(bytes) = table_get(&names, &key)? {
            return decode_u32(&bytes);
        }
        let counter = self
            .meta_u64(db, meta::INTERN_COUNTER)?
            .checked_add(1)
            .ok_or_else(|| Error::Exhausted("intern IDs".to_string()))?;
        let id = u32::try_from(counter).map_err(|_| Error::Exhausted("intern IDs".to_string()))?;
        self.put_row(&mut names, &key, &id.to_be_bytes())?;
        let mut ids = self.table(INTERN_ID)?;
        self.put_row(&mut ids, &encode_intern_id_key(db, id)?, name)?;
        self.set_meta_u64(db, meta::INTERN_COUNTER, counter)?;
        Ok(id)
    }

    /// Install an authoritative intern binding: the name maps to exactly
    /// `id` and the ID back to exactly the name. An unbound pair inserts
    /// in both directions; the identical pair is a no-op; a name bound to
    /// another ID, or an ID bound to another name, fails loudly as
    /// [`Error::Corrupt`] without writing, instead of forking the mapping
    /// or letting two names share a keyspace. The counter fast-forwards
    /// past installed IDs, so later allocations never collide.
    /// Activation entries carry these bindings.
    pub fn bind_intern(&self, db: &[u8], name: &[u8], id: u32) -> Result<()> {
        let mut names = self.table(INTERN_NAME)?;
        let key = encode_intern_name_key(db, name)?;
        let mut ids = self.table(INTERN_ID)?;
        let id_key = encode_intern_id_key(db, id)?;
        let known_id = table_get(&names, &key)?
            .map(|bytes| decode_u32(&bytes))
            .transpose()?;
        let known_name = table_get(&ids, &id_key)?;
        match (known_id, known_name) {
            (Some(known), Some(bound)) if known == id && bound == name => return Ok(()),
            (None, None) => {}
            (Some(known), _) if known != id => {
                return Err(Error::corrupt(format!(
                    "intern binding for {} disagrees: {known} versus {id}",
                    String::from_utf8_lossy(name)
                )));
            }
            (_, Some(bound)) if bound != name => {
                return Err(Error::corrupt(format!(
                    "intern ID {id} is bound to {} instead of {}",
                    String::from_utf8_lossy(&bound),
                    String::from_utf8_lossy(name)
                )));
            }
            _ => {
                return Err(Error::corrupt(format!(
                    "intern binding for {} is one-sided",
                    String::from_utf8_lossy(name)
                )));
            }
        }
        self.put_row(&mut names, &key, &id.to_be_bytes())?;
        self.put_row(&mut ids, &id_key, name)?;
        let counter = self.meta_u64(db, meta::INTERN_COUNTER)?;
        let counter = counter.max(id as u64);
        self.set_meta_u64(db, meta::INTERN_COUNTER, counter)?;
        Ok(())
    }

    /// Delete up to `budget` keys below an index prefix, resuming after
    /// `after` (an empty cursor starts at the prefix). Returns the
    /// deleted count plus a resume cursor, or `None` when the prefix is
    /// exhausted; a zero budget deletes nothing and never reports
    /// exhaustion.
    pub fn delete_index_prefix(
        &self,
        db: &[u8],
        index: u32,
        after: Option<&[u8]>,
        budget: usize,
    ) -> Result<(usize, Option<Vec<u8>>)> {
        let prefix = index_prefix(db, index, &[])?;
        self.delete_key_prefix(INDEX_ENTRIES, &prefix, after, budget)
    }

    /// Delete up to `budget` keys below a collection prefix, resuming
    /// after `after` (an empty cursor starts at the prefix). Returns the
    /// deleted count plus a resume cursor, or `None` when the prefix is
    /// exhausted; a zero budget deletes nothing and never reports
    /// exhaustion.
    pub fn delete_record_prefix(
        &self,
        db: &[u8],
        collection: u32,
        after: Option<&[u8]>,
        budget: usize,
    ) -> Result<(usize, Option<Vec<u8>>)> {
        let prefix = record_prefix(db, collection)?;
        self.delete_key_prefix(RECORDS, &prefix, after, budget)
    }

    /// Bounded raw deletion below one byte prefix. Keys collect first so
    /// deletion never mutates the table mid-iteration. An empty `after`
    /// starts at the prefix. Exhaustion is only reported after a scan
    /// that found fewer keys than its budget, so a zero budget hands the
    /// cursor back unchanged.
    fn delete_key_prefix(
        &self,
        def: TableDefinition<&'static [u8], &'static [u8]>,
        prefix: &[u8],
        after: Option<&[u8]>,
        budget: usize,
    ) -> Result<(usize, Option<Vec<u8>>)> {
        use std::ops::Bound;
        let after = after.filter(|cursor| !cursor.is_empty());
        if budget == 0 {
            return Ok((0, Some(after.map(<[u8]>::to_vec).unwrap_or_default())));
        }
        let table = self.table(def)?;
        let lower = match after {
            None => Bound::Included(prefix),
            Some(cursor) => Bound::Excluded(cursor),
        };
        let mut keys = Vec::new();
        match prefix_end(prefix) {
            Some(end) => {
                let range = table
                    .range::<&[u8]>((lower, Bound::Excluded(end.as_slice())))
                    .map_err(storage)?;
                for entry in range {
                    if keys.len() >= budget {
                        break;
                    }
                    let (key, _) = entry.map_err(storage)?;
                    keys.push(key.value().to_vec());
                }
            }
            None => {
                let range = table
                    .range::<&[u8]>((lower, Bound::Unbounded))
                    .map_err(storage)?;
                for entry in range {
                    if keys.len() >= budget {
                        break;
                    }
                    let (key, _) = entry.map_err(storage)?;
                    let key: &[u8] = key.value();
                    if !key.starts_with(prefix) {
                        break;
                    }
                    keys.push(key.to_vec());
                }
            }
        }
        drop(table);
        let mut table = self.table(def)?;
        for key in &keys {
            self.delete_row(&mut table, key)?;
        }
        let cursor = if keys.len() < budget {
            None
        } else {
            keys.last()
                .cloned()
                .or_else(|| after.map(|cursor| cursor.to_vec()))
        };
        Ok((keys.len(), cursor))
    }

    /// Upsert one record. The value must be valid UTF-8 canonical JSON;
    /// canonicalization itself is the writer lane's contract.
    pub fn put_record(&self, db: &[u8], collection: u32, key: &[u8], json: &[u8]) -> Result<()> {
        std::str::from_utf8(json).map_err(|_| Error::corrupt("record value is not UTF-8"))?;
        let mut table = self.table(RECORDS)?;
        let full = encode_record_key(db, collection, key)?;
        let packed = pack_value(json);
        self.put_row(&mut table, &full, &packed)
    }

    /// Delete one record, reporting whether it existed.
    pub fn delete_record(&self, db: &[u8], collection: u32, key: &[u8]) -> Result<bool> {
        let mut table = self.table(RECORDS)?;
        let full = encode_record_key(db, collection, key)?;
        self.delete_row(&mut table, &full)
    }

    /// Insert one index entry. The components must already be canonical
    /// bytes; compound order follows component order.
    pub fn put_index_entry(
        &self,
        db: &[u8],
        index: u32,
        components: &[&[u8]],
        primary: &[u8],
    ) -> Result<()> {
        let mut table = self.table(INDEX_ENTRIES)?;
        let full = encode_index_key(db, index, components, primary)?;
        self.put_row(&mut table, &full, b"")
    }

    /// Delete one index entry, reporting whether it existed.
    pub fn delete_index_entry(
        &self,
        db: &[u8],
        index: u32,
        components: &[&[u8]],
        primary: &[u8],
    ) -> Result<bool> {
        let mut table = self.table(INDEX_ENTRIES)?;
        let full = encode_index_key(db, index, components, primary)?;
        self.delete_row(&mut table, &full)
    }

    /// Store one derived outcome.
    pub fn put_derived(
        &self,
        db: &[u8],
        definition: u32,
        args: &[u8],
        outcome: &DerivedOutcome,
    ) -> Result<()> {
        let mut table = self.table(DERIVED)?;
        let key = encode_derived_key(db, definition, args)?;
        let encoded = encode_outcome(outcome);
        self.put_row(&mut table, &key, &encoded)
    }

    /// Replace a dependent's edges transactionally: remove the prior edges
    /// listed in `dependent_edges`, insert the observed ones, and store the
    /// new list for the next replacement.
    pub fn replace_edges(
        &self,
        db: &[u8],
        definition: u32,
        args: &[u8],
        observed: &[Vec<u8>],
    ) -> Result<()> {
        let mut edges = self.table(DEP_EDGES)?;
        let mut stored = self.table(DEPENDENT_EDGES)?;
        let dependent = encode_derived_dep(definition, args)?;
        let key = encode_derived_key(db, definition, args)?;
        if let Some(prior) = table_get(&stored, &key)? {
            for edge in decode_edge_list(&prior)? {
                let full = encode_dep_edge(db, &edge, &dependent)?;
                self.delete_row(&mut edges, &full)?;
            }
        }
        for edge in observed {
            let full = encode_dep_edge(db, edge, &dependent)?;
            self.put_row(&mut edges, &full, b"")?;
        }
        let encoded = encode_edge_list(observed)?;
        self.put_row(&mut stored, &key, &encoded)
    }

    /// Delete one derived outcome with its edges: removes the outcome,
    /// the prior edge list and every `dep_edges` row it names.
    pub fn delete_derived(&self, db: &[u8], definition: u32, args: &[u8]) -> Result<()> {
        let mut table = self.table(DERIVED)?;
        let key = encode_derived_key(db, definition, args)?;
        self.delete_row(&mut table, &key)?;
        let mut edges = self.table(DEP_EDGES)?;
        let mut stored = self.table(DEPENDENT_EDGES)?;
        let dependent = encode_derived_dep(definition, args)?;
        if let Some(prior) = table_get(&stored, &key)? {
            for edge in decode_edge_list(&prior)? {
                let full = encode_dep_edge(db, &edge, &dependent)?;
                self.delete_row(&mut edges, &full)?;
            }
            self.delete_row(&mut stored, &key)?;
        }
        Ok(())
    }

    /// Store one receipt's opaque bytes.
    pub fn put_receipt(&self, db: &[u8], identity: &[u8], receipt: &[u8]) -> Result<()> {
        let mut table = self.table(RECEIPTS)?;
        let key = encode_receipt_key(db, identity)?;
        self.put_row(&mut table, &key, receipt)
    }

    /// Read one receipt's opaque bytes inside an open write, seeing
    /// the transaction's own writes. Lets commit paths detect
    /// duplicate submissions before installing a journal.
    pub fn get_receipt(&self, db: &[u8], identity: &[u8]) -> Result<Option<Vec<u8>>> {
        let table = self.table(RECEIPTS)?;
        let key = encode_receipt_key(db, identity)?;
        table_get(&table, &key)
    }

    /// Delete one receipt, reporting whether one existed. Retention
    /// only: live receipts are never deleted.
    pub fn delete_receipt(&self, db: &[u8], identity: &[u8]) -> Result<bool> {
        let mut table = self.table(RECEIPTS)?;
        let key = encode_receipt_key(db, identity)?;
        self.delete_row(&mut table, &key)
    }

    /// Read one code artifact inside the write transaction, as
    /// [`ReadTxn::get_code`].
    pub fn get_code(&self, db: &[u8], hash: &[u8; HASH_LEN]) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let table = self.table(CODE)?;
        let key = encode_code_key(db, hash)?;
        match table_get(&table, &key)? {
            None => Ok(None),
            Some(bytes) => Ok(Some(decode_code(&bytes)?)),
        }
    }

    /// Store one code artifact.
    pub fn put_code(
        &self,
        db: &[u8],
        hash: &[u8; HASH_LEN],
        wasm: &[u8],
        manifest: &[u8],
    ) -> Result<()> {
        let mut table = self.table(CODE)?;
        let key = encode_code_key(db, hash)?;
        let encoded = encode_code(wasm, manifest)?;
        self.put_row(&mut table, &key, &encoded)
    }

    /// Write a raw `meta` value.
    pub fn set_meta(&self, db: &[u8], name: &[u8], value: &[u8]) -> Result<()> {
        let mut table = self.table(META)?;
        let key = encode_meta_key(db, name)?;
        self.put_row(&mut table, &key, value)
    }

    /// Write a `meta` scalar.
    pub fn set_meta_u64(&self, db: &[u8], name: &[u8], value: u64) -> Result<()> {
        self.set_meta(db, name, &encode_u64(value))
    }

    /// Read a `meta` scalar inside the write transaction.
    pub fn meta_u64(&self, db: &[u8], name: &[u8]) -> Result<u64> {
        let table = self.table(META)?;
        let key = encode_meta_key(db, name)?;
        match table_get(&table, &key)? {
            None => Ok(0),
            Some(bytes) => decode_u64(&bytes),
        }
    }

    /// Read a raw `meta` value inside the write transaction.
    pub fn meta(&self, db: &[u8], name: &[u8]) -> Result<Option<Vec<u8>>> {
        let table = self.table(META)?;
        table_get(&table, &encode_meta_key(db, name)?)
    }

    /// One page of a database's receipts inside the write transaction,
    /// as [`ReadTxn::receipts_page`].
    pub fn receipts_page(
        &self,
        db: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<(Vec<Row>, bool)> {
        let table = self.table(RECEIPTS)?;
        receipts_page_in(&table, db, after, limit)
    }

    /// Raw `(name, value)` meta pairs whose name starts with `prefix`
    /// inside the write transaction, as [`ReadTxn::scan_meta_prefix`].
    pub fn scan_meta_prefix(&self, db: &[u8], prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let table = self.table(META)?;
        scan_meta_prefix_in(&table, db, prefix)
    }

    /// Write retention floors.
    pub fn set_floors(&self, db: &[u8], floors: &[u64]) -> Result<()> {
        let encoded = encode_floors(floors)?;
        self.set_meta(db, meta::RETENTION_FLOORS, &encoded)
    }

    /// Delete a raw `meta` value, reporting whether it existed.
    pub fn delete_meta(&self, db: &[u8], name: &[u8]) -> Result<bool> {
        let mut table = self.table(META)?;
        let key = encode_meta_key(db, name)?;
        self.delete_row(&mut table, &key)
    }

    /// Raw put below one table, for snapshot installs. Keys are full
    /// table keys, values verbatim.
    pub fn put_db_raw(&self, table: SnapshotTable, key: &[u8], value: &[u8]) -> Result<()> {
        let mut handle = self.table(table.def())?;
        self.put_row(&mut handle, key, value)
    }

    /// The raw value under a full table key, if any.
    pub fn get_db_raw(&self, table: SnapshotTable, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let handle = self.table(table.def())?;
        table_get(&handle, key)
    }

    /// Raw delete of a full table key; whether it existed.
    pub fn delete_db_raw(&self, table: SnapshotTable, key: &[u8]) -> Result<bool> {
        let mut handle = self.table(table.def())?;
        self.delete_row(&mut handle, key)
    }

    /// Delete every key below one database prefix in all ten tables, in
    /// bounded pages. Snapshot installs wipe before writing so no stale
    /// row survives.
    pub fn clear_database(&self, db: &[u8]) -> Result<()> {
        const CLEAR_PAGE: usize = 4096;
        let prefix = encode_db(db)?;
        for table in SnapshotTable::ALL {
            let mut after = None;
            loop {
                let (_, resume) =
                    self.delete_key_prefix(table.def(), &prefix, after.as_deref(), CLEAR_PAGE)?;
                match resume {
                    None => break,
                    Some(cursor) => after = Some(cursor),
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    // Store tests need a writable file and the redb backend, so they run
    // with the workspace suite, not as host-independent checks.
    use super::keys::ROOT_DB;
    use super::*;

    fn temp_path(name: &str) -> std::path::PathBuf {
        let mut dir = std::env::temp_dir();
        dir.push(format!("flower-engine-test-{name}-{}", std::process::id()));
        dir
    }

    /// An in-memory file counting its syncs.
    #[derive(Debug)]
    struct CountingSyncs {
        file: redb::backends::InMemoryBackend,
        syncs: Arc<std::sync::atomic::AtomicU64>,
    }

    impl redb::StorageBackend for CountingSyncs {
        fn len(&self) -> std::io::Result<u64> {
            self.file.len()
        }
        fn read(&self, offset: u64, out: &mut [u8]) -> std::io::Result<()> {
            self.file.read(offset, out)
        }
        fn set_len(&self, len: u64) -> std::io::Result<()> {
            self.file.set_len(len)
        }
        fn sync_data(&self) -> std::io::Result<()> {
            self.syncs.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.file.sync_data()
        }
        fn write(&self, offset: u64, data: &[u8]) -> std::io::Result<()> {
            self.file.write(offset, data)
        }
    }

    /// A page says whether more follow, also when it starts past a row
    /// still stored. (Before, the scan counted that row toward the one
    /// it reads past the page and answered "end" with rows left, so a
    /// collection walk whose cursor row was kept stopped early.)
    #[test]
    fn receipt_pages_know_whether_more_follow() {
        let path = temp_path("receipt-pages");
        let _ = std::fs::remove_file(&path);
        let store = GroupStore::create(&path).unwrap();
        let write = store.begin_write().unwrap();
        for identity in [b"r1", b"r3", b"r5", b"r7"] {
            write.put_receipt(b"db", identity, b"x").unwrap();
        }
        write.commit().unwrap();
        let read = store.begin_read().unwrap();
        let identities = |rows: Vec<Row>| -> Vec<Vec<u8>> {
            rows.into_iter().map(|(identity, _)| identity).collect()
        };
        let (page, end) = read.receipts_page(b"db", None, 2).unwrap();
        assert_eq!(
            (identities(page), end),
            (vec![b"r1".to_vec(), b"r3".to_vec()], false)
        );
        let (page, end) = read.receipts_page(b"db", Some(b"r1"), 2).unwrap();
        assert_eq!(
            (identities(page), end),
            (vec![b"r3".to_vec(), b"r5".to_vec()], false)
        );
        let (page, end) = read.receipts_page(b"db", Some(b"r3"), 2).unwrap();
        assert_eq!(
            (identities(page), end),
            (vec![b"r5".to_vec(), b"r7".to_vec()], true)
        );
        // Past a row no longer stored.
        let (page, end) = read.receipts_page(b"db", Some(b"r2"), 1).unwrap();
        assert_eq!((identities(page), end), (vec![b"r3".to_vec()], false));
        drop(read);
        drop(store);
        let _ = std::fs::remove_file(&path);
    }

    /// Commits inside `deferring` sync nothing and skip the hook; durable
    /// ones, `flush` among them, run the hook first, and a failing hook
    /// fails the commit, which changes nothing.
    #[test]
    fn deferred_commits_skip_the_fsync_and_durable_ones_run_the_hook_first() {
        use std::sync::atomic::{AtomicU64, Ordering};
        let syncs = Arc::new(AtomicU64::new(0));
        let db = redb::Builder::new()
            .create_with_backend(CountingSyncs {
                file: redb::backends::InMemoryBackend::new(),
                syncs: syncs.clone(),
            })
            .unwrap();
        let store = GroupStore::from_database(db, None).unwrap();
        let hooked = Arc::new(AtomicU64::new(0));
        let fail = Arc::new(std::sync::atomic::AtomicBool::new(false));
        {
            let (hooked, fail, syncs) = (hooked.clone(), fail.clone(), syncs.clone());
            store.set_before_durable(Arc::new(move || {
                // The hook runs before the commit's own sync.
                hooked.store(syncs.load(Ordering::SeqCst) + 1, Ordering::SeqCst);
                if fail.load(Ordering::SeqCst) {
                    return Err(Error::Io("the log could not flush".into()));
                }
                Ok(())
            }));
        }
        let put = |key: &[u8]| {
            let txn = store.begin_write().unwrap();
            let coll = txn.intern(ROOT_DB, b"rows").unwrap();
            txn.put_record(ROOT_DB, coll, key, b"1").unwrap();
            txn
        };
        let before = syncs.load(Ordering::SeqCst);
        deferring(|| {
            let txn = put(b"a");
            assert!(txn.is_deferred());
            txn.commit().unwrap();
        });
        assert_eq!(syncs.load(Ordering::SeqCst), before, "a deferred commit");
        assert_eq!(hooked.load(Ordering::SeqCst), 0);
        let read = store.begin_read().unwrap();
        let coll = read.intern_id(ROOT_DB, b"rows").unwrap().unwrap();
        assert!(read.get_record(ROOT_DB, coll, b"a").unwrap().is_some());
        drop(read);

        let txn = put(b"b");
        assert!(!txn.is_deferred(), "only inside deferring");
        txn.commit().unwrap();
        assert!(syncs.load(Ordering::SeqCst) > before);
        assert_eq!(hooked.load(Ordering::SeqCst), before + 1);

        fail.store(true, Ordering::SeqCst);
        assert!(put(b"c").commit().is_err());
        assert!(store.flush().is_err());
        let read = store.begin_read().unwrap();
        assert!(read.get_record(ROOT_DB, coll, b"c").unwrap().is_none());
        drop(read);
        fail.store(false, Ordering::SeqCst);
        let synced = syncs.load(Ordering::SeqCst);
        store.flush().unwrap();
        assert_eq!(hooked.load(Ordering::SeqCst), synced + 1);
        assert!(syncs.load(Ordering::SeqCst) > synced);
    }

    #[test]
    fn records_round_trip_compressed() {
        let path = temp_path("records");
        let _ = std::fs::remove_file(&path);
        let store = GroupStore::create(&path).unwrap();
        let txn = store.begin_write().unwrap();
        let coll = txn.intern(ROOT_DB, b"tasks").unwrap();
        txn.put_record(ROOT_DB, coll, b"k", br#"{"a":1}"#).unwrap();
        txn.commit().unwrap();

        let read = store.begin_read().unwrap();
        assert_eq!(read.intern_id(ROOT_DB, b"tasks").unwrap(), Some(coll));
        assert_eq!(
            read.intern_name(ROOT_DB, coll).unwrap().unwrap(),
            b"tasks".to_vec()
        );
        assert_eq!(
            read.get_record(ROOT_DB, coll, b"k").unwrap().unwrap(),
            br#"{"a":1}"#.to_vec()
        );
        let rows = read
            .scan_records(ROOT_DB, coll, None, None, None, false)
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, b"k".to_vec());
        drop(read);

        let txn = store.begin_write().unwrap();
        assert!(txn.delete_record(ROOT_DB, coll, b"k").unwrap());
        assert!(!txn.delete_record(ROOT_DB, coll, b"k").unwrap());
        txn.commit().unwrap();
        let read = store.begin_read().unwrap();
        assert_eq!(read.get_record(ROOT_DB, coll, b"k").unwrap(), None);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn invalid_record_values_reject() {
        let path = temp_path("invalid");
        let _ = std::fs::remove_file(&path);
        let store = GroupStore::create(&path).unwrap();
        let txn = store.begin_write().unwrap();
        assert!(txn.put_record(ROOT_DB, 1, b"k", b"\xff\xfe").is_err());
        drop(txn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn index_entries_scan_by_component_prefix() {
        let path = temp_path("index");
        let _ = std::fs::remove_file(&path);
        let store = GroupStore::create(&path).unwrap();
        let txn = store.begin_write().unwrap();
        let index = txn.intern(ROOT_DB, b"by_owner").unwrap();
        txn.put_index_entry(ROOT_DB, index, &[&b"ada"[..]], b"pk1")
            .unwrap();
        txn.put_index_entry(ROOT_DB, index, &[&b"ada"[..]], b"pk2")
            .unwrap();
        txn.put_index_entry(ROOT_DB, index, &[&b"bob"[..]], b"pk3")
            .unwrap();
        txn.commit().unwrap();

        let read = store.begin_read().unwrap();
        let rows = read
            .scan_index(ROOT_DB, index, 1, &[&b"ada"[..]], ScanRange::default())
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].1, b"pk1".to_vec());
        assert_eq!(rows[1].1, b"pk2".to_vec());
        let all = read
            .scan_index(ROOT_DB, index, 1, &[], ScanRange::default())
            .unwrap();
        assert_eq!(all.len(), 3);
        let limited = read
            .scan_index(
                ROOT_DB,
                index,
                1,
                &[],
                ScanRange {
                    limit: Some(2),
                    ..ScanRange::default()
                },
            )
            .unwrap();
        assert_eq!(limited.len(), 2);
        let reversed = read
            .scan_index(
                ROOT_DB,
                index,
                1,
                &[],
                ScanRange {
                    reverse: true,
                    ..ScanRange::default()
                },
            )
            .unwrap();
        assert_eq!(reversed.len(), 3);
        assert_eq!(reversed[0].1, b"pk3".to_vec());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn edges_replace_transactionally() {
        use super::keys::{DepKey, decode_dep_key, encode_record_dep};

        let path = temp_path("edges");
        let _ = std::fs::remove_file(&path);
        let store = GroupStore::create(&path).unwrap();
        let txn = store.begin_write().unwrap();
        let first = encode_record_dep(1, b"a").unwrap();
        txn.replace_edges(ROOT_DB, 9, b"args", std::slice::from_ref(&first))
            .unwrap();
        txn.commit().unwrap();

        let read = store.begin_read().unwrap();
        assert_eq!(
            read.dependents(ROOT_DB, &first).unwrap(),
            vec![(9, b"args".to_vec())]
        );
        assert_eq!(
            read.dependent_edges(ROOT_DB, 9, b"args").unwrap(),
            vec![first.clone()]
        );
        drop(read);

        let txn = store.begin_write().unwrap();
        let second = encode_record_dep(1, b"b").unwrap();
        txn.replace_edges(ROOT_DB, 9, b"args", std::slice::from_ref(&second))
            .unwrap();
        txn.commit().unwrap();
        let read = store.begin_read().unwrap();
        assert!(read.dependents(ROOT_DB, &first).unwrap().is_empty());
        assert_eq!(
            read.dependents(ROOT_DB, &second).unwrap(),
            vec![(9, b"args".to_vec())]
        );
        let (key, _) = decode_dep_key(&second).unwrap();
        assert_eq!(
            key,
            DepKey::Record {
                collection: 1,
                key: b"b".to_vec()
            }
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn intern_bindings_check_both_directions() {
        let path = temp_path("bind");
        let _ = std::fs::remove_file(&path);
        let store = GroupStore::create(&path).unwrap();
        let txn = store.begin_write().unwrap();
        txn.bind_intern(ROOT_DB, b"tasks", 5).unwrap();
        txn.bind_intern(ROOT_DB, b"tasks", 5).unwrap();
        // The name is bound elsewhere.
        assert!(matches!(
            txn.bind_intern(ROOT_DB, b"tasks", 6),
            Err(Error::Corrupt(_))
        ));
        // The ID is bound to another name: two collections would share
        // one keyspace.
        assert!(matches!(
            txn.bind_intern(ROOT_DB, b"posts", 5),
            Err(Error::Corrupt(_))
        ));
        txn.commit().unwrap();
        let read = store.begin_read().unwrap();
        assert_eq!(
            read.intern_name(ROOT_DB, 5).unwrap(),
            Some(b"tasks".to_vec())
        );
        assert_eq!(read.intern_id(ROOT_DB, b"posts").unwrap(), None);
        drop(read);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn zero_budget_deletes_never_report_exhaustion() {
        let path = temp_path("budget");
        let _ = std::fs::remove_file(&path);
        let store = GroupStore::create(&path).unwrap();
        let txn = store.begin_write().unwrap();
        txn.put_index_entry(ROOT_DB, 3, &[&b"a"[..]], b"pk")
            .unwrap();
        let (deleted, resume) = txn.delete_index_prefix(ROOT_DB, 3, None, 0).unwrap();
        assert_eq!((deleted, resume.as_deref()), (0, Some(&b""[..])));
        // The empty cursor resumes at the prefix.
        let (deleted, resume) = txn.delete_index_prefix(ROOT_DB, 3, Some(b""), 10).unwrap();
        assert_eq!((deleted, resume), (1, None));
        drop(txn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn key_prefix_scans_cross_length_classes() {
        let path = temp_path("keyprefix");
        let _ = std::fs::remove_file(&path);
        let store = GroupStore::create(&path).unwrap();
        let txn = store.begin_write().unwrap();
        // Interleave matching and foreign keys across several lengths:
        // stored order is length first, so the prefix spans classes.
        for key in ["a1", "b1", "a22", "zz", "b333", "a4444", "c", "a55555"] {
            txn.put_record(ROOT_DB, 1, key.as_bytes(), b"{}").unwrap();
        }
        txn.commit().unwrap();
        let read = store.begin_read().unwrap();
        let keys = |after: Option<&[u8]>, limit| -> Vec<Vec<u8>> {
            read.scan_records_with_key_prefix(ROOT_DB, 1, b"a", after, limit)
                .unwrap()
                .into_iter()
                .map(|(key, _)| key)
                .collect()
        };
        let all: Vec<Vec<u8>> = ["a1", "a22", "a4444", "a55555"]
            .iter()
            .map(|key| key.as_bytes().to_vec())
            .collect();
        assert_eq!(keys(None, 10), all);
        assert_eq!(keys(None, 2), all[..2].to_vec());
        assert_eq!(keys(Some(b"a22"), 10), all[2..].to_vec());
        assert!(
            read.scan_records_with_key_prefix(ROOT_DB, 1, b"a", Some(b"b1"), 1)
                .is_err()
        );
        drop(read);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn derived_code_meta_round_trip() {
        use super::codec::{OutcomeBody, OutcomeTime, decode_floors};

        let path = temp_path("meta");
        let _ = std::fs::remove_file(&path);
        let store = GroupStore::create(&path).unwrap();
        let txn = store.begin_write().unwrap();
        txn.put_derived(
            ROOT_DB,
            4,
            b"args",
            &DerivedOutcome {
                pinned: true,
                code: [1u8; HASH_LEN],
                time: OutcomeTime::Always,
                body: OutcomeBody::Value(b"{}".to_vec()),
            },
        )
        .unwrap();
        txn.put_code(ROOT_DB, &[3u8; HASH_LEN], b"wasm", b"manifest")
            .unwrap();
        txn.set_meta_u64(ROOT_DB, meta::REVISION, 41).unwrap();
        txn.set_floors(ROOT_DB, &[7, 9]).unwrap();
        txn.commit().unwrap();

        let read = store.begin_read().unwrap();
        let outcome = read.get_derived(ROOT_DB, 4, b"args").unwrap().unwrap();
        assert!(outcome.pinned);
        assert_eq!(
            read.get_code(ROOT_DB, &[3u8; HASH_LEN]).unwrap().unwrap(),
            (b"wasm".to_vec(), b"manifest".to_vec())
        );
        assert_eq!(read.meta_u64(ROOT_DB, meta::REVISION).unwrap(), 41);
        assert_eq!(read.meta_u64(ROOT_DB, meta::APPLIED_LOG).unwrap(), 0);
        let floors = read.meta(ROOT_DB, meta::RETENTION_FLOORS).unwrap().unwrap();
        assert_eq!(decode_floors(&floors).unwrap(), vec![7, 9]);
        let _ = std::fs::remove_file(&path);
    }
}
