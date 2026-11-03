//! Speculative preparation: mutations prepared in parallel on a
//! snapshot of the writer's staged state, each certified by what it
//! read, then adopted in queue order only if nothing written before it
//! since the snapshot is something it read.
//!
//! A writer prepares writes in queue order, each on the state the
//! writes before it staged (see [`Engine::on_staged`]). To prepare a
//! *wave* of them at once, it builds an [`Overlay`] of the rows its
//! staged transaction wrote, as they stand, and hands out a
//! [`Speculation`]: a thread-safe handle on committed state seen through
//! that overlay (taken while the staged transaction holds the store's
//! write lock, so nothing commits under it), with the writer's code and
//! idempotency cache. A [`Speculator`] made from it prepares one
//! mutation exactly as [`Engine::prepare_mutation`] does, with the same
//! code, but through a read that records every row it looks up and every
//! key range it walks ([`ReadSet`]). That is the mutation's *certificate*
//! ([`MutationCertificate`]): records and their absence, collection
//! scans, index ranges, derived outcomes and the edges their
//! evaluations and invalidations walked, receipts, placement, locks and
//! code rows, all alike, since the preparation reads nothing else from
//! the store.
//!
//! The writer then takes the wave in queue order. Each write before a
//! candidate stages into the staged transaction, which logs every row it
//! puts or deletes. A candidate holds when no row logged since the wave's
//! snapshot touches its certificate ([`MutationCertificate::verdict`]):
//! then everything it read is what it would read now, and preparing it
//! again would compute the same patch and answer. The one thing it read
//! that every write moves is the database revision its patch starts from,
//! which it reads unrecorded and [`rebase`] sets to the revision it lands
//! on, as its own preparation there would have. A candidate that does
//! not hold is prepared again in its turn, on the staged state, as if
//! there were no speculation; so is one whose evaluation read retained
//! history (it comes from outside the store), or failed.
//!
//! A wave's mutations that name no timestamp all run at the wave's one
//! clock, speculative or prepared again ([`Engine::at_clock`]), so a
//! mutation prepared again sees the instant its speculation saw, and
//! clocks never go back along a batch.

use std::sync::{Arc, Mutex, PoisonError, RwLock};

use flower_sdk::{Failure, Value};

use crate::batch::BatchItem;
use crate::engine::{
    Catalog, Engine, MutationOptions, PrepareOn, PrepareOutcome, Shared, prepare_mutation_on,
};
use crate::error::{HostResult, Result};
use crate::patch::{decode_patch, encode_patch};
use crate::retry::UnscopedDedup;
use crate::storage::keys::{DEP_DERIVED, Reader};
use crate::storage::{Overlay, ReadSet, ReadTxn, SnapshotTable, Write};

/// Conflicting rows a rejected candidate keeps, for diagnostics and
/// conflict prediction.
pub const CONFLICTS_KEPT: usize = 4;

/// Something that prepares a mutation without committing it: the
/// writer's engine on its staged state, or a [`Speculator`] on a
/// snapshot of it.
pub trait PrepareMutation {
    /// Evaluate a mutation without committing it, as
    /// [`Engine::prepare_mutation`].
    fn prepare_mutation(
        &mut self,
        db: &[u8],
        module: &str,
        name: &str,
        args: &Value,
        options: &MutationOptions,
    ) -> HostResult<PrepareOutcome>;
}

impl PrepareMutation for Engine {
    fn prepare_mutation(
        &mut self,
        db: &[u8],
        module: &str,
        name: &str,
        args: &Value,
        options: &MutationOptions,
    ) -> HostResult<PrepareOutcome> {
        Engine::prepare_mutation(self, db, module, name, args, options)
    }
}

/// A writer's staged state, handed to other threads to prepare on (see
/// the module docs). Valid only while the staged transaction its overlay
/// came from is open.
#[derive(Clone)]
pub struct Speculation {
    pub(crate) shared: Arc<Shared>,
    pub(crate) catalog: Arc<Catalog>,
    pub(crate) dedup: Arc<RwLock<UnscopedDedup>>,
    pub(crate) overlay: Arc<Overlay>,
    pub(crate) clock: Option<u64>,
}

impl Speculation {
    /// A preparer for one candidate.
    pub fn speculator(&self) -> Speculator {
        Speculator {
            speculation: self.clone(),
            reads: ReadSet::default(),
            untracked: false,
        }
    }

    /// The clock its mutations run at when they name none.
    pub fn clock(&self) -> Option<u64> {
        self.clock
    }
}

/// Prepares one candidate on a [`Speculation`], recording what it read.
pub struct Speculator {
    speculation: Speculation,
    reads: ReadSet,
    untracked: bool,
}

impl Speculator {
    /// What its preparations read, so far.
    pub fn certificate(self) -> MutationCertificate {
        MutationCertificate {
            reads: self.reads,
            untracked: self.untracked,
        }
    }
}

impl PrepareMutation for Speculator {
    fn prepare_mutation(
        &mut self,
        db: &[u8],
        module: &str,
        name: &str,
        args: &Value,
        options: &MutationOptions,
    ) -> HostResult<PrepareOutcome> {
        let speculation = &self.speculation;
        let store = &speculation.shared.store;
        let set = Arc::new(Mutex::new(ReadSet::default()));
        let read = store
            .begin_overlay(speculation.overlay.clone(), Some(set.clone()))
            .map_err(Failure::from)?;
        let committed = store.begin_read().map_err(Failure::from)?;
        let dedup = speculation
            .dedup
            .read()
            .unwrap_or_else(PoisonError::into_inner);
        let on = PrepareOn {
            catalog: &speculation.catalog,
            dedup: &dedup,
            read,
            committed: Some(committed),
            clock: speculation.clock,
            // An overlay records reads its certificate must hold: the hook
            // runs.
            recall: None,
        };
        let (outcome, volatile) = prepare_mutation_on(on, db, module, name, args, options);
        drop(dedup);
        self.untracked |= volatile;
        let reads = match Arc::try_unwrap(set) {
            Ok(set) => set.into_inner().unwrap_or_else(PoisonError::into_inner),
            Err(set) => set.lock().unwrap_or_else(PoisonError::into_inner).clone(),
        };
        self.reads.merge(reads);
        outcome
    }
}

/// What a speculative preparation read (see the module docs).
#[derive(Clone, Debug, Default)]
pub struct MutationCertificate {
    reads: ReadSet,
    /// It read something no row names (retained history): it holds for
    /// nothing.
    untracked: bool,
}

/// Whether a candidate holds after the rows written since its snapshot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Verdict {
    /// Nothing written since touches what it read.
    Holds,
    /// It read something no row names, or more than a read set keeps.
    Untracked,
    /// Rows written since that it read, records and derived outcomes
    /// first, at most [`CONFLICTS_KEPT`].
    Conflicts(Vec<Write>),
}

impl MutationCertificate {
    /// Whether it holds after `writes`, the rows staged since its
    /// snapshot.
    pub fn verdict(&self, writes: &[Write]) -> Verdict {
        if self.untracked || self.reads.overflowed() {
            return Verdict::Untracked;
        }
        let mut conflicts: Vec<Write> = self
            .reads
            .conflicts(writes, 64)
            .into_iter()
            .cloned()
            .collect();
        if conflicts.is_empty() {
            return Verdict::Holds;
        }
        conflicts.sort_by_key(|(table, _)| match table {
            SnapshotTable::Records => 0,
            SnapshotTable::Derived => 1,
            _ => 2,
        });
        conflicts.truncate(CONFLICTS_KEPT);
        Verdict::Conflicts(conflicts)
    }

    /// What it read.
    pub fn reads(&self) -> &ReadSet {
        &self.reads
    }

    /// Whether it read something no row names.
    pub fn untracked(&self) -> bool {
        self.untracked
    }
}

/// Move a speculatively prepared item to where it lands: its patch
/// starts from the revision `read` (the staged state at its turn) holds,
/// as its preparation there would have. Only patches are speculated;
/// other items pass unchanged.
pub fn rebase(read: &ReadTxn, item: BatchItem) -> Result<BatchItem> {
    let BatchItem::Patch { db, patch } = item else {
        return Ok(item);
    };
    let mut decoded = decode_patch(&patch)?;
    let revision = read.base_revision(&db)?;
    if decoded.base_revision == revision {
        return Ok(BatchItem::Patch { db, patch });
    }
    decoded.base_revision = revision;
    Ok(BatchItem::Patch {
        db,
        patch: encode_patch(&decoded)?,
    })
}

/// What kind of row a conflict was on.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ConflictClass {
    /// A record.
    Source,
    /// An index entry.
    Index,
    /// A derived outcome or its edges.
    Cell,
    /// A receipt.
    Receipt,
    /// Database metadata: placement, locks, retention, code identities.
    Meta,
    /// Anything else (interned names, code).
    Other,
}

impl ConflictClass {
    /// Every class, in report order.
    pub const ALL: [ConflictClass; 6] = [
        ConflictClass::Source,
        ConflictClass::Index,
        ConflictClass::Cell,
        ConflictClass::Receipt,
        ConflictClass::Meta,
        ConflictClass::Other,
    ];

    /// The class of a row in `table`.
    pub fn of(table: SnapshotTable) -> ConflictClass {
        match table {
            SnapshotTable::Records => ConflictClass::Source,
            SnapshotTable::IndexEntries => ConflictClass::Index,
            SnapshotTable::Derived | SnapshotTable::DepEdges | SnapshotTable::DependentEdges => {
                ConflictClass::Cell
            }
            SnapshotTable::Receipts => ConflictClass::Receipt,
            SnapshotTable::Meta => ConflictClass::Meta,
            SnapshotTable::InternId | SnapshotTable::InternName | SnapshotTable::Code => {
                ConflictClass::Other
            }
        }
    }

    /// Its name in reports.
    pub fn as_str(self) -> &'static str {
        match self {
            ConflictClass::Source => "source",
            ConflictClass::Index => "index",
            ConflictClass::Cell => "cell",
            ConflictClass::Receipt => "receipt",
            ConflictClass::Meta => "meta",
            ConflictClass::Other => "other",
        }
    }
}

/// A conflicting row that is one record or one derived outcome: its
/// table, database, collection or definition ID and key or argument
/// bytes (canonical JSON, as [`record_key`] renders a value).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConflictRow {
    /// Records or derived outcomes.
    pub table: SnapshotTable,
    /// The database.
    pub db: Vec<u8>,
    /// The collection or derived definition, interned.
    pub id: u32,
    /// The record key or derived arguments.
    pub key: Vec<u8>,
}

impl ConflictRow {
    /// The record or derived outcome a written row is, if it is one.
    pub fn of(write: &Write) -> Option<ConflictRow> {
        let (table, full) = write;
        let mut reader = Reader::new(full);
        let db = reader.sized().ok()?;
        match table {
            SnapshotTable::Records => {}
            SnapshotTable::Derived => {
                if reader.byte().ok()? != DEP_DERIVED {
                    return None;
                }
            }
            _ => return None,
        }
        let id = reader.u32().ok()?;
        let key = reader.sized().ok()?;
        Some(ConflictRow {
            table: *table,
            db,
            id,
            key,
        })
    }

    /// The collection's or definition's name, as `read` interns it.
    pub fn name(&self, read: &ReadTxn) -> Option<String> {
        let name = read.intern_name(&self.db, self.id).ok()??;
        Some(String::from_utf8_lossy(&name).into_owned())
    }
}

/// The bytes a record key or derived arguments of `value` are stored
/// under: its canonical JSON. `None` when it has none.
pub fn record_key(value: &Value) -> Option<Vec<u8>> {
    crate::json::render(value).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::keys::{encode_derived_key, encode_record_key};

    #[test]
    fn conflict_rows_decode_records_and_derived_outcomes() {
        let record = (
            SnapshotTable::Records,
            encode_record_key(b"db", 7, b"\"k\"").unwrap(),
        );
        assert_eq!(
            ConflictRow::of(&record),
            Some(ConflictRow {
                table: SnapshotTable::Records,
                db: b"db".to_vec(),
                id: 7,
                key: b"\"k\"".to_vec(),
            })
        );
        let derived = (
            SnapshotTable::Derived,
            encode_derived_key(b"db", 3, b"{\"a\":1}").unwrap(),
        );
        assert_eq!(ConflictRow::of(&derived).unwrap().id, 3);
        assert_eq!(
            ConflictRow::of(&(SnapshotTable::Meta, b"anything".to_vec())),
            None
        );
        assert_eq!(
            ConflictClass::of(SnapshotTable::DepEdges),
            ConflictClass::Cell
        );
        assert_eq!(
            record_key(&Value::String("k".into())),
            Some(b"\"k\"".to_vec())
        );
    }
}
