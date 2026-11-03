//! Module removal and data purge.
//!
//! Removing a module ([`crate::engine::Engine::commit_removal`]) takes its
//! code, aliases, hooks and runtime away, drops its derived outcomes with
//! their edges, and drops what other modules computed from it. Its data
//! stays where it is, unreachable, and so do its declarations: the code
//! it last ran is named by a `removed\0<module>` meta row, so deploying it
//! again diffs against them and rebuilds nothing that did not change.
//!
//! A removal with `purge` records the module in the database's
//! [`PURGES_ROW`] instead, and the leader's maintenance deletes its data
//! a page at a time. Each page is a batch item
//! ([`crate::batch::BatchItem::Purge`]) the leader's writer prepares on
//! the state it stages: it names the keys it deletes, at most [`PAGE`],
//! and where the next page starts. Every member, device and restore
//! replay applies the same page alike, and the purge's progress (keys
//! deleted, the cursor) is replicated with it, so a new leader continues
//! where the last page left off. A page applies only where the purge
//! stands ([`PurgePage::from`]); any other changes nothing. Until a
//! module's purge ends it cannot deploy, so new data never mixes with
//! the old; nor can any module declaring one of the queues it purges,
//! and no operation reads or writes those queues ([`queue_refusal`]):
//! a write behind the walk's cursor would leave rows pointing at rows a
//! page deleted, and one ahead of it would be deleted by a later page.
//! Purges go one module at a time, by name.
//!
//! A module's data, in the order a purge walks it: the records and index
//! entries under every name it interned (`<module>\0...`: its
//! collections and indexes, the ones its earlier deployments retired
//! included), by intern ID; the rows of its raw namespaces in the shared
//! `$raw` collection; its timers in the shared `$timer` collection,
//! held ones included; the items and bookkeeping of its queues in the
//! shared `$queue` collection; its maintenance tasks' failure states in
//! the shared `$task` collection. Its derived outcomes went with the
//! removal (derived caches are each member's own, so no page could name
//! them alike). Queues are named by queue, not by module: a module's
//! queues are those its manifest declared that no other module declares
//! (deployed, or removed with its data kept), fixed when its purge
//! begins ([`Progress::queues`]). Guest code uses only the queues its
//! module declares, so no other module's work is in them. Walking the
//! shared collections reads every module's rows, so a page also ends
//! after reading [`SCAN`] rows.

use std::collections::{BTreeMap, HashSet};

use flower_sdk::error::codes;
use flower_sdk::{Failure, Value, fail};
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::host::{QUEUE_COLLECTION, RAW_COLLECTION, TASK_COLLECTION, TIMER_COLLECTION};
use crate::patch::RecordWrite;
use crate::retry::HASH_LEN;
use crate::storage::codec::unpack_value;
use crate::storage::keys::{Reader, encode_db, encode_record_key};
use crate::storage::{ReadTxn, SnapshotTable, WriteTxn};
use crate::timer::{TimerState, decode_timer, in_class, removal};

/// The most keys one purge page deletes.
pub const PAGE: usize = 1024;
/// The most rows one purge page reads while it walks the shared
/// collections, where other modules' rows are skipped.
pub const SCAN: usize = 8 * PAGE;
/// Rows one scan of a walk fetches at a time.
const CHUNK: usize = 1024;
/// Meta-name prefix of a removed module's kept declarations:
/// `removed\0<module>`, whose value is the code identity it last ran.
pub const REMOVED_PREFIX: &[u8] = b"removed\x00";
/// Meta row holding a database's purges in flight: canonical JSON of
/// [`Purges`].
pub const PURGES_ROW: &[u8] = b"module_purges";
/// Failure code: a purge page that does not continue its purge where it
/// stands (it changed nothing).
pub const PURGE_STALE: &str = "purge_stale";
/// Failure code: a purge page naming a key its module does not own (it
/// changed nothing).
pub const PURGE_REFUSED: &str = "purge_refused";
/// Failure code: an operation on a queue a purge is deleting (it
/// changed nothing). Retry once the purge ends.
pub const QUEUE_PURGING: &str = "queue_purging";

/// Meta name of a removed module's kept declarations.
pub fn removed_key(module: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(REMOVED_PREFIX.len() + module.len());
    key.extend_from_slice(REMOVED_PREFIX);
    key.extend_from_slice(module.as_bytes());
    key
}

/// The prefix every name `module` interns starts with.
fn owned_prefix(module: &str) -> Vec<u8> {
    let mut prefix = module.as_bytes().to_vec();
    prefix.push(0);
    prefix
}

/// Which table of a module's structure a purge walks or deletes in.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum Part {
    /// Records (`records`).
    Records,
    /// Index entries (`index_entries`).
    Index,
}

impl Part {
    fn table(self) -> SnapshotTable {
        match self {
            Part::Records => SnapshotTable::Records,
            Part::Index => SnapshotTable::IndexEntries,
        }
    }
}

/// One stretch of a purge's walk, in walk order.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum Segment {
    /// One interned structure's keys in one table.
    Structure {
        /// The intern ID.
        id: u32,
        /// The table.
        part: Part,
    },
    /// The module's raw rows in the shared `$raw` collection.
    Raw,
    /// The module's timers in the shared `$timer` collection.
    Timers,
    /// The rows of the module's queues in the shared `$queue`
    /// collection.
    Queues,
    /// The module's task failure states in the shared `$task`
    /// collection.
    Tasks,
}

/// Where a purge's walk stands.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Cursor {
    /// The segment the next page starts in.
    pub segment: Segment,
    /// The last key visited in that segment, without its database
    /// prefix; the next page starts after it (at the segment's start
    /// when `None`).
    pub after: Option<Vec<u8>>,
}

impl Cursor {
    /// The start of a walk.
    pub fn start() -> Self {
        Cursor {
            segment: Segment::Structure {
                id: 0,
                part: Part::Records,
            },
            after: None,
        }
    }
}

/// How far one module's purge went.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Progress {
    /// Keys the pages applied so far deleted.
    pub deleted: u64,
    /// Where the next page starts.
    pub cursor: Cursor,
    /// The queues the purge deletes, sorted: those the module declared
    /// that no other module declared when the purge began.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub queues: Vec<String>,
}

impl Progress {
    /// A purge about to start, deleting `queues` with the rest.
    pub fn start(mut queues: Vec<String>) -> Self {
        queues.sort();
        queues.dedup();
        Progress {
            deleted: 0,
            cursor: Cursor::start(),
            queues,
        }
    }

    /// Whether the purge deletes `queue`.
    fn deletes_queue(&self, queue: &str) -> bool {
        self.queues
            .binary_search_by(|name| name.as_str().cmp(queue))
            .is_ok()
    }
}

/// A database's purges in flight, by module.
pub type Purges = BTreeMap<String, Progress>;

/// The modules removed from a database whose data and declarations
/// stay, by name, with the code identity each last ran.
pub type Removed = Vec<(String, [u8; HASH_LEN])>;

/// One page of a module's purge.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct PurgePage {
    /// The module whose data the page deletes.
    pub module: String,
    /// Where the purge stood when the page was prepared: the page
    /// applies only there.
    pub from: Cursor,
    /// The keys to delete, without their database prefix.
    pub deletes: Vec<(Part, Vec<u8>)>,
    /// Where the next page starts; `None` when this page ends the purge.
    pub next: Option<Cursor>,
}

impl PurgePage {
    /// Encoded bytes the page carries, roughly.
    pub fn len(&self) -> usize {
        64 + self.module.len()
            + self
                .deletes
                .iter()
                .map(|(_, key)| key.len() + 8)
                .sum::<usize>()
    }

    /// Whether the page deletes nothing.
    pub fn is_empty(&self) -> bool {
        self.deletes.is_empty()
    }
}

/// Decode a database's purges row.
pub fn decode_purges(bytes: Option<&[u8]>) -> Result<Purges> {
    match bytes {
        None => Ok(Purges::new()),
        Some(bytes) => serde_json::from_slice(bytes)
            .map_err(|error| Error::corrupt(format!("module purges row: {error}"))),
    }
}

/// The purges in flight in `db`, as `read` sees them.
pub fn purges(read: &ReadTxn, db: &[u8]) -> Result<Purges> {
    decode_purges(read.meta(db, PURGES_ROW)?.as_deref())
}

/// The purges in flight in `db`, inside a write.
pub fn purges_in(write: &WriteTxn, db: &[u8]) -> Result<Purges> {
    decode_purges(write.meta(db, PURGES_ROW)?.as_deref())
}

/// Store `db`'s purges in flight, dropping the row once none is left.
pub fn write_purges(write: &WriteTxn, db: &[u8], purges: &Purges) -> Result<()> {
    if purges.is_empty() {
        write.delete_meta(db, PURGES_ROW)?;
        return Ok(());
    }
    let bytes = serde_json::to_vec(purges)
        .map_err(|error| Error::corrupt(format!("module purges row: {error}")))?;
    write.set_meta(db, PURGES_ROW, &bytes)
}

/// The modules removed from `db` whose data and declarations stay, with
/// the code identity each last ran, by name.
pub fn removed(read: &ReadTxn, db: &[u8]) -> Result<Removed> {
    let mut kept = Vec::new();
    for (name, identity) in read.scan_meta_prefix(db, REMOVED_PREFIX)? {
        let module = String::from_utf8(name[REMOVED_PREFIX.len()..].to_vec())
            .map_err(|_| Error::corrupt("removed module name is not UTF-8"))?;
        let identity: [u8; HASH_LEN] = identity
            .try_into()
            .map_err(|_| Error::corrupt("removed module identity has the wrong length"))?;
        kept.push((module, identity));
    }
    kept.sort();
    Ok(kept)
}

/// Why deploying `module` with `manifest` into `db` is refused while a
/// purge is in flight, if it is: what every applier checks before an
/// activation, and staging before anything.
pub fn activation_refusal(
    read: &ReadTxn,
    db: &[u8],
    module: &str,
    manifest: &Value,
) -> Result<Option<Failure>> {
    Ok(purge_refusal(
        &purges(read, db)?,
        module,
        declared_queues(manifest),
    ))
}

/// Why deploying `module`, declaring `queues`, is refused while
/// `purges` are in flight: its own data is being purged, or the rows
/// of a queue it declares.
pub fn purge_refusal<'a>(
    purges: &Purges,
    module: &str,
    queues: impl IntoIterator<Item = &'a str>,
) -> Option<Failure> {
    if purges.contains_key(module) {
        return Some(still_purging(module));
    }
    for queue in queues {
        for (purging, progress) in purges {
            if progress.deletes_queue(queue) {
                return Some(fail(
                    codes::MODULE_CONFLICT,
                    format!(
                        "queue `{queue}` is still being purged with module `{purging}`: deploy \
                         `{module}` again once the modules listing no longer shows it purging"
                    ),
                ));
            }
        }
    }
    None
}

/// Why an operation on `queue` is refused while `purges` are in flight,
/// if it is: a purge is deleting the queue's rows. Every applier checks
/// the writes a patch carries ([`writes_refusal`]); planners check
/// first, as a half-deleted queue does not plan.
pub fn queue_refusal(purges: &Purges, queue: &str) -> Option<Failure> {
    let (module, _) = purges
        .iter()
        .find(|(_, progress)| progress.deletes_queue(queue))?;
    Some(fail(
        QUEUE_PURGING,
        format!(
            "queue `{queue}` is being purged with module `{module}`: retry once the modules \
             listing no longer shows it purging"
        ),
    ))
}

/// Why `records`, written to `db` in `read`'s state, are refused, if
/// they are: one is a row of a queue under purge.
pub(crate) fn writes_refusal(
    read: &ReadTxn,
    db: &[u8],
    records: &[RecordWrite],
) -> Result<Option<Failure>> {
    if records.is_empty() {
        return Ok(None);
    }
    let purges = purges(read, db)?;
    if purges.values().all(|progress| progress.queues.is_empty()) {
        return Ok(None);
    }
    let Some(collection) = read.intern_id(db, QUEUE_COLLECTION)? else {
        return Ok(None);
    };
    for record in records
        .iter()
        .filter(|record| record.collection == collection)
    {
        let queue = crate::work_queue::row_queue(&record.key)?;
        if let Some(refusal) = queue_refusal(&purges, queue) {
            return Ok(Some(refusal));
        }
    }
    Ok(None)
}

/// The queue names a module manifest declares.
pub fn declared_queues(manifest: &Value) -> impl Iterator<Item = &str> {
    let items = match manifest {
        Value::Object(root) => match root.get("queues") {
            Some(Value::Array(items)) => items.as_slice(),
            _ => &[],
        },
        _ => &[],
    };
    items.iter().filter_map(|item| match item {
        Value::Object(def) => match def.get("name") {
            Some(Value::String(name)) => Some(name.as_str()),
            _ => None,
        },
        _ => None,
    })
}

/// The refusal for deploying a module whose purge is in flight.
pub fn still_purging(module: &str) -> Failure {
    fail(
        codes::MODULE_CONFLICT,
        format!(
            "module `{module}`'s data is still being purged: deploy it again once the modules \
             listing no longer shows it purging"
        ),
    )
}

/// The intern IDs of every name `module` interned in `db`: its
/// collections, indexes, definitions and those its deployments retired,
/// sorted.
pub(crate) fn module_ids(read: &ReadTxn, db: &[u8], module: &str) -> Result<Vec<u32>> {
    let owned = owned_prefix(module);
    let skip = encode_db(db)?.len();
    let mut ids = Vec::new();
    for (key, name) in read.scan_db_raw(SnapshotTable::InternId, db)? {
        if name.starts_with(&owned) {
            let tail = key
                .get(skip..)
                .ok_or_else(|| Error::corrupt("intern key outside its database"))?;
            ids.push(Reader::new(tail).u32()?);
        }
    }
    ids.sort_unstable();
    Ok(ids)
}

/// The next page of `module`'s purge in `db` where `progress` stands: up
/// to `limit` keys, reading up to `scan` rows. `next` is `None` once the
/// walk found everything.
pub fn next_page(
    read: &ReadTxn,
    db: &[u8],
    module: &str,
    progress: &Progress,
    limit: usize,
    scan: usize,
) -> Result<PurgePage> {
    let from = &progress.cursor;
    let limit = limit.max(1);
    let scan = scan.max(limit);
    let base = encode_db(db)?;
    let owned = owned_prefix(module);
    let raw = read.intern_id(db, RAW_COLLECTION)?;
    let timers = read.intern_id(db, TIMER_COLLECTION)?;
    let queues = match progress.queues.is_empty() {
        true => None,
        false => read.intern_id(db, QUEUE_COLLECTION)?,
    };
    let tasks = read.intern_id(db, TASK_COLLECTION)?;
    let mut segments = Vec::new();
    for id in module_ids(read, db, module)? {
        for part in [Part::Records, Part::Index] {
            segments.push((Segment::Structure { id, part }, part.table(), id));
        }
    }
    if let Some(id) = raw {
        segments.push((Segment::Raw, SnapshotTable::Records, id));
    }
    if let Some(id) = timers {
        segments.push((Segment::Timers, SnapshotTable::Records, id));
    }
    if let Some(id) = queues {
        segments.push((Segment::Queues, SnapshotTable::Records, id));
    }
    if let Some(id) = tasks {
        segments.push((Segment::Tasks, SnapshotTable::Records, id));
    }
    let mut deletes = Vec::new();
    let mut scanned = 0usize;
    for (segment, table, id) in segments {
        if segment < from.segment {
            continue;
        }
        let mut prefix = base.clone();
        prefix.extend_from_slice(&id.to_be_bytes());
        let mut after = if segment == from.segment {
            from.after.clone()
        } else {
            None
        };
        loop {
            let want = (scan - scanned).min(CHUNK);
            let lower = after
                .as_ref()
                .map(|suffix| [base.as_slice(), suffix].concat());
            let rows = read.scan_raw_prefix(table, &prefix, lower.as_deref(), want)?;
            let fetched = rows.len();
            for (key, value) in rows {
                scanned += 1;
                let stored = &key[prefix.len()..];
                match segment {
                    Segment::Structure { part, .. } => {
                        deletes.push((part, key[base.len()..].to_vec()));
                    }
                    Segment::Raw => {
                        if raw_namespace(stored)?.starts_with(&owned) {
                            deletes.push((Part::Records, key[base.len()..].to_vec()));
                        }
                    }
                    Segment::Timers => {
                        for row in owned_timer(stored, Some(&value), module)? {
                            let full = encode_record_key(db, id, &row)?;
                            deletes.push((Part::Records, full[base.len()..].to_vec()));
                        }
                    }
                    Segment::Queues => {
                        if queue_owned(stored, progress)? {
                            deletes.push((Part::Records, key[base.len()..].to_vec()));
                        }
                    }
                    Segment::Tasks => {
                        if Reader::new(stored).sized()?.starts_with(&owned) {
                            deletes.push((Part::Records, key[base.len()..].to_vec()));
                        }
                    }
                }
                after = Some(key[base.len()..].to_vec());
                if deletes.len() >= limit || scanned >= scan {
                    return Ok(PurgePage {
                        module: module.to_string(),
                        from: from.clone(),
                        deletes,
                        next: Some(Cursor { segment, after }),
                    });
                }
            }
            if fetched < want {
                break;
            }
        }
    }
    Ok(PurgePage {
        module: module.to_string(),
        from: from.clone(),
        deletes,
        next: None,
    })
}

/// The namespace of a stored raw record key (`sized(sized(namespace),
/// sized(key))`).
fn raw_namespace(stored: &[u8]) -> Result<Vec<u8>> {
    let key = Reader::new(stored).sized()?;
    Reader::new(&key).sized()
}

/// Whether a stored `$queue` row (`sized(key)`) belongs to one of the
/// queues `progress` deletes.
fn queue_owned(stored: &[u8], progress: &Progress) -> Result<bool> {
    let key = Reader::new(stored).sized()?;
    Ok(progress.deletes_queue(crate::work_queue::row_queue(&key)?))
}

/// Whether a record key is a timer's state row, in any state.
fn timer_state_row(key: &[u8]) -> bool {
    [TimerState::Pending, TimerState::Dead, TimerState::Held]
        .into_iter()
        .any(|state| in_class(key, state))
}

/// The record keys removing a stored timer row of `module`, given the
/// row's stored key and packed value: its state row and ID row, or none
/// for an ID row or another module's timer.
fn owned_timer(stored: &[u8], packed: Option<&[u8]>, module: &str) -> Result<Vec<Vec<u8>>> {
    let key = Reader::new(stored).sized()?;
    if !timer_state_row(&key) {
        return Ok(Vec::new());
    }
    let Some(packed) = packed else {
        return Ok(Vec::new());
    };
    let timer = decode_timer(&key, &unpack_value(packed)?)?;
    if timer.module != module {
        return Ok(Vec::new());
    }
    Ok(removal(&timer).into_iter().map(|(key, _)| key).collect())
}

/// Apply one purge page inside `write` (`read` viewing it): delete its
/// keys and record how far the purge went, or refuse it, changing
/// nothing, when the purge does not stand where it starts or a key is
/// not the module's. The answer is `{module, deleted, keys, done}`:
/// the keys deleted so far, and by this page.
pub(crate) fn apply_page(
    write: &WriteTxn,
    read: &ReadTxn,
    db: &[u8],
    page: &PurgePage,
) -> Result<std::result::Result<Value, Failure>> {
    let module = page.module.as_str();
    let mut purges = purges_in(write, db)?;
    let Some(progress) = purges.get(module).cloned() else {
        return Ok(Err(fail(
            PURGE_STALE,
            format!("no purge of `{module}` is in flight"),
        )));
    };
    if progress.cursor != page.from {
        return Ok(Err(fail(
            PURGE_STALE,
            format!("the page does not continue the purge of `{module}` where it stands"),
        )));
    }
    let base = encode_db(db)?;
    let owned: HashSet<u32> = module_ids(read, db, module)?.into_iter().collect();
    let raw = read.intern_id(db, RAW_COLLECTION)?;
    let timers = read.intern_id(db, TIMER_COLLECTION)?;
    let queues = read.intern_id(db, QUEUE_COLLECTION)?;
    let tasks = read.intern_id(db, TASK_COLLECTION)?;
    let owned_prefix = owned_prefix(module);
    let mut keys = Vec::with_capacity(page.deletes.len());
    for (part, suffix) in &page.deletes {
        let mut reader = Reader::new(suffix);
        let id = reader.u32().ok();
        let stored = suffix.get(4..).unwrap_or_default();
        let mine = match (part, id) {
            (_, Some(id)) if owned.contains(&id) => true,
            (Part::Records, Some(id)) if Some(id) == raw => {
                raw_namespace(stored)?.starts_with(&owned_prefix)
            }
            (Part::Records, Some(id)) if Some(id) == timers => {
                timer_owned(write, db, id, stored, module)?
            }
            (Part::Records, Some(id)) if Some(id) == queues => queue_owned(stored, &progress)?,
            (Part::Records, Some(id)) if Some(id) == tasks => {
                Reader::new(stored).sized()?.starts_with(&owned_prefix)
            }
            _ => false,
        };
        if !mine {
            return Ok(Err(fail(
                PURGE_REFUSED,
                format!("the page deletes a key module `{module}` does not own"),
            )));
        }
        keys.push((part.table(), [base.as_slice(), suffix].concat()));
    }
    for (table, key) in &keys {
        write.delete_db_raw(*table, key)?;
    }
    let deleted = progress.deleted.saturating_add(page.deletes.len() as u64);
    match &page.next {
        Some(cursor) => {
            purges.insert(
                module.to_string(),
                Progress {
                    deleted,
                    cursor: cursor.clone(),
                    queues: progress.queues,
                },
            );
        }
        None => {
            purges.remove(module);
        }
    }
    write_purges(write, db, &purges)?;
    Ok(Ok(Value::Object(BTreeMap::from([
        ("module".to_string(), Value::String(module.to_string())),
        ("deleted".to_string(), Value::Number(deleted as f64)),
        ("keys".to_string(), Value::Number(page.deletes.len() as f64)),
        ("done".to_string(), Value::Bool(page.next.is_none())),
    ]))))
}

/// Whether a stored `$timer` row is `module`'s (an ID row through the
/// state row it points at); a row already gone counts, as deleting it
/// changes nothing.
fn timer_owned(
    write: &WriteTxn,
    db: &[u8],
    collection: u32,
    stored: &[u8],
    module: &str,
) -> Result<bool> {
    let key = Reader::new(stored).sized()?;
    if timer_state_row(&key) {
        let Some(value) = write.get_record(db, collection, &key)? else {
            return Ok(true);
        };
        return Ok(decode_timer(&key, &value)?.module == module);
    }
    let Some(id) = key.strip_prefix(b"i") else {
        return Ok(false);
    };
    let id = std::str::from_utf8(id).map_err(|_| Error::corrupt("timer ID is not UTF-8"))?;
    Ok(
        crate::timer::find(|key| write.get_record(db, collection, key), id)?
            .is_none_or(|timer| timer.module == module),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segments_order_as_the_walk_goes() {
        let structure = |id, part| Segment::Structure { id, part };
        assert!(structure(1, Part::Records) < structure(1, Part::Index));
        assert!(structure(1, Part::Index) < structure(2, Part::Records));
        assert!(structure(u32::MAX, Part::Index) < Segment::Raw);
        assert!(Segment::Raw < Segment::Timers);
        assert!(Segment::Timers < Segment::Queues);
        assert!(Cursor::start().segment <= structure(0, Part::Records));
    }

    #[test]
    fn a_deployment_waits_for_a_purge_of_its_name_or_its_queues() {
        let mut purges = Purges::new();
        purges.insert(
            "bulk".to_string(),
            Progress::start(vec!["mail".to_string(), "jobs".to_string()]),
        );
        assert_eq!(purges["bulk"].queues, ["jobs", "mail"]);
        let refused = purge_refusal(&purges, "bulk", []).unwrap();
        assert!(
            refused.message.contains("still being purged"),
            "{refused:?}"
        );
        let refused = purge_refusal(&purges, "other", ["audit", "mail"]).unwrap();
        assert_eq!(refused.code, codes::MODULE_CONFLICT);
        assert!(refused.message.contains("queue `mail`"), "{refused:?}");
        assert!(purge_refusal(&purges, "other", ["audit"]).is_none());
        let manifest = Value::Object(BTreeMap::from([(
            "queues".to_string(),
            Value::Array(vec![
                Value::Object(BTreeMap::from([(
                    "name".to_string(),
                    Value::String("jobs".to_string()),
                )])),
                Value::Null,
            ]),
        )]));
        assert_eq!(declared_queues(&manifest).collect::<Vec<_>>(), ["jobs"]);
        assert_eq!(declared_queues(&Value::Null).count(), 0);
    }

    #[test]
    fn purges_round_trip_and_an_empty_row_reads_as_none() {
        let mut purges = Purges::new();
        purges.insert(
            "bulk".to_string(),
            Progress {
                deleted: 7,
                cursor: Cursor {
                    segment: Segment::Raw,
                    after: Some(vec![0, 1, 2]),
                },
                queues: vec!["jobs".to_string()],
            },
        );
        let bytes = serde_json::to_vec(&purges).unwrap();
        assert_eq!(decode_purges(Some(&bytes)).unwrap(), purges);
        assert!(decode_purges(None).unwrap().is_empty());
        assert!(decode_purges(Some(b"{")).is_err());
    }
}
