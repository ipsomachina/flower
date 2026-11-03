//! Batch entries: many writes in one log entry.
//!
//! A leader's writer prepares each queued write on the state the writes
//! queued before it staged, then proposes a group of them as one log
//! entry: one append, one replication round and one apply transaction
//! for all of them, while each keeps its own patch, receipt, revision
//! and answer. Every member applies the items in order with
//! [`Engine::commit_batch`], each checked against what the items before
//! it wrote, which is exactly the state its writer prepared it on.
//!
//! That holds only if nothing else applied between the state the writer
//! read and the batch. So a batch names its *head*: the data entry it
//! was prepared after, by log index, or by the writer and sequence of
//! the batch before it when the writer prepared it while that one was
//! still committing. A batch applies only when the group's recorded head
//! is the one it names; otherwise every item rejects as [`STALE_BATCH`]
//! and the writer prepares them again. Every data entry records the
//! head as it applies, on members, devices and restore replay alike, so
//! all of them reach the same verdict. Blanks and membership entries
//! carry no data and record nothing.

use std::collections::HashSet;

use flower_sdk::{Failure, Value};
use serde::{Deserialize, Serialize};

use crate::catalog::CatalogChange;
use crate::engine::{ApplyOutcome, CheckedPatch, Engine, MultiApplyOutcome, MultiChecked};
use crate::error::{Error, HostResult, Result};
use crate::patch::decode_patch;
use crate::placement::{PlacementCommand, PlacementEffect};
use crate::purge::PurgePage;
use crate::retention::RetentionCommand;
use crate::staging::StagingCommand;
use crate::storage::{ReadTxn, StagedWrite, WriteTxn};
use crate::txn::TxnCommand;

/// The group coordination database that holds the head, beside the
/// group position and expiry clock.
pub const HEAD_DB: &[u8] = b"$cluster";
/// Meta row holding the group's head.
const HEAD_ROW: &[u8] = b"group_head";
/// Why a whole batch rejected: its head moved before it applied.
pub const STALE_BATCH: &str = "stale_batch";

/// One write inside a batch entry.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum BatchItem {
    /// An accepted mutation, queue or timer patch for one database.
    Patch {
        /// Database identifier bytes.
        db: Vec<u8>,
        /// Encoded patch bytes.
        patch: Vec<u8>,
    },
    /// An atomic multi-database transaction patch.
    MultiPatch {
        /// Encoded multi-patch bytes.
        patches: Vec<u8>,
    },
    /// A retention transition for one database: epochs, collection or
    /// a retry session (see [`crate::retention`]).
    Retention {
        /// Database identifier bytes.
        db: Vec<u8>,
        /// The transition.
        command: RetentionCommand,
    },
    /// A change to the partition catalog, in the root group (see
    /// [`crate::catalog`]).
    Catalog {
        /// The change.
        change: CatalogChange,
    },
    /// A change to one database's placement in this group (see
    /// [`crate::placement`]).
    Placement {
        /// Database identifier bytes.
        db: Vec<u8>,
        /// The change.
        command: PlacementCommand,
    },
    /// A two-phase commit record change (see [`crate::txn`]).
    Txn {
        /// The change.
        command: TxnCommand,
    },
    /// One page of a removed module's data purge (see
    /// [`crate::purge`]).
    Purge {
        /// Database identifier bytes.
        db: Vec<u8>,
        /// The page.
        page: PurgePage,
    },
    /// A step of a replicated staged deployment in one database (see
    /// [`crate::staging`]).
    Staging {
        /// Database identifier bytes.
        db: Vec<u8>,
        /// The step.
        command: StagingCommand,
    },
}

impl BatchItem {
    /// Whether the item activates a moved database in this group: its
    /// code loads as the item applies, so appliers commit it and load
    /// the code with readers fenced out ([`Engine::exclusive`]).
    pub fn activates(&self) -> bool {
        matches!(
            self,
            BatchItem::Placement {
                command: PlacementCommand::Activate { .. },
                ..
            }
        )
    }

    /// Encoded bytes the item carries.
    pub fn len(&self) -> usize {
        match self {
            BatchItem::Patch { db, patch } => db.len() + patch.len(),
            BatchItem::MultiPatch { patches } => patches.len(),
            BatchItem::Retention { db, .. } => db.len() + 64,
            BatchItem::Catalog { .. } => 256,
            BatchItem::Placement { db, .. } => db.len() + 128,
            BatchItem::Txn { command } => match command {
                TxnCommand::Prepare { record } => record.patch.len() + 512,
                TxnCommand::Decide {
                    results,
                    reason,
                    code,
                    details,
                    ..
                } => {
                    results.iter().map(String::len).sum::<usize>()
                        + reason.as_ref().map_or(0, String::len)
                        + code.as_ref().map_or(0, String::len)
                        + details.as_ref().map_or(0, String::len)
                        + 256
                }
                _ => 256,
            },
            BatchItem::Purge { db, page } => db.len() + page.len(),
            BatchItem::Staging { db, command } => db.len() + command.len(),
        }
    }

    /// Whether the item carries no bytes.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// The last data entry a group applied.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Head {
    /// A data entry other than a committed batch, by log index: zero
    /// before the first.
    Entry(u64),
    /// A committed batch, by its writer and sequence.
    Batch {
        /// The proposing writer's random identity.
        writer: u64,
        /// The batch's sequence within its writer.
        seq: u64,
    },
}

impl Head {
    /// The group's head as `read` sees it.
    pub fn read(read: &ReadTxn) -> Result<Head> {
        match read.meta(HEAD_DB, HEAD_ROW)? {
            None => Ok(Head::Entry(0)),
            Some(bytes) => Self::decode(&bytes),
        }
    }

    /// Record this head inside an open transaction.
    pub fn record(self, write: &WriteTxn) -> Result<()> {
        let mut bytes = Vec::with_capacity(17);
        match self {
            Head::Entry(index) => {
                bytes.push(0);
                bytes.extend_from_slice(&index.to_be_bytes());
            }
            Head::Batch { writer, seq } => {
                bytes.push(1);
                bytes.extend_from_slice(&writer.to_be_bytes());
                bytes.extend_from_slice(&seq.to_be_bytes());
            }
        }
        write.set_meta(HEAD_DB, HEAD_ROW, &bytes)
    }

    fn decode(bytes: &[u8]) -> Result<Head> {
        let word = |at: usize| -> Result<u64> {
            bytes
                .get(at..at + 8)
                .and_then(|word| word.try_into().ok())
                .map(u64::from_be_bytes)
                .ok_or_else(|| Error::corrupt("truncated group head".to_string()))
        };
        match (bytes.first(), bytes.len()) {
            (Some(0), 9) => Ok(Head::Entry(word(1)?)),
            (Some(1), 17) => Ok(Head::Batch {
                writer: word(1)?,
                seq: word(9)?,
            }),
            _ => Err(Error::corrupt("bad group head".to_string())),
        }
    }
}

/// One applied batch item: what it checked and what it committed.
#[derive(Clone, Debug)]
pub enum Applied {
    /// A single-database patch.
    Patch {
        /// The decoded patch and its verdict.
        checked: Box<CheckedPatch>,
        /// Its committed outcome.
        outcome: ApplyOutcome,
    },
    /// A multi-database transaction patch.
    Multi {
        /// The decoded participants and the verdict.
        checked: MultiChecked,
        /// Its committed outcome.
        outcome: MultiApplyOutcome,
    },
    /// A retention transition.
    Retention {
        /// Its database.
        db: Vec<u8>,
        /// What it answered, or its deterministic refusal, which
        /// changed nothing.
        result: HostResult<Value>,
        /// The database revision, which retention never moves.
        revision: u64,
    },
    /// A catalog change.
    Catalog {
        /// What it answered, or its deterministic refusal, which
        /// changed nothing.
        result: HostResult<Value>,
        /// The catalog's revision after it.
        revision: u64,
    },
    /// A placement change.
    Placement {
        /// Its database.
        db: Vec<u8>,
        /// What it answered, or its deterministic refusal, which
        /// changed nothing.
        result: HostResult<Value>,
        /// What the applier finishes after committing.
        effect: PlacementEffect,
        /// The database's revision.
        revision: u64,
    },
    /// A two-phase commit record change.
    Txn {
        /// What it answered, or its deterministic refusal, which
        /// changed nothing.
        result: HostResult<Value>,
        /// The databases a commit moved, with their revisions.
        revisions: Vec<(Vec<u8>, u64)>,
        /// The records `(put, deleted)` each of those commits wrote, in
        /// the same order.
        records: Vec<(u64, u64)>,
    },
    /// A purge page.
    Purge {
        /// Its database.
        db: Vec<u8>,
        /// What it answered (`{module, deleted, keys, done}`), or its
        /// deterministic refusal, which changed nothing.
        result: HostResult<Value>,
        /// The database revision, which a purge never moves.
        revision: u64,
    },
    /// A replicated staging step.
    Staging {
        /// Its database.
        db: Vec<u8>,
        /// What it answered, or its deterministic refusal, which
        /// changed nothing.
        result: HostResult<Value>,
        /// The database revision, which staging never moves.
        revision: u64,
    },
}

impl Applied {
    /// Each database the item moved, with its revision after it and the
    /// records `(put, deleted)` it wrote there, as its commit notices
    /// carry them. Retention, purge pages and staging steps move no
    /// revision and notify nothing.
    pub fn changes(&self) -> Vec<(Vec<u8>, u64, (u64, u64))> {
        match self {
            Applied::Patch { checked, outcome } => vec![(
                checked.patch.db.clone(),
                outcome.revision,
                checked.records(outcome.revision),
            )],
            Applied::Multi { checked, outcome } => outcome
                .revisions
                .iter()
                .cloned()
                .zip(checked.records(&outcome.revisions))
                .map(|((db, revision), records)| (db, revision, records))
                .collect(),
            // Nor do a removed module's purge pages (no query reads its
            // data) or staging steps (staged structures serve no query
            // until their activation, an entry of its own).
            Applied::Retention { .. } | Applied::Purge { .. } | Applied::Staging { .. } => {
                Vec::new()
            }
            Applied::Placement { db, revision, .. } => vec![(db.clone(), *revision, (0, 0))],
            Applied::Catalog { revision, .. } => {
                vec![(crate::catalog::CATALOG_DB.to_vec(), *revision, (0, 0))]
            }
            Applied::Txn {
                revisions, records, ..
            } => revisions
                .iter()
                .cloned()
                .enumerate()
                .map(|(step, (db, revision))| {
                    (db, revision, records.get(step).copied().unwrap_or((0, 0)))
                })
                .collect(),
        }
    }

    /// The deterministic rejection the item committed, if any.
    pub fn rejected(&self) -> Option<&Failure> {
        match self {
            Applied::Patch { checked, .. } => checked.rejected.as_ref(),
            Applied::Multi { checked, .. } => checked.rejected.as_ref(),
            Applied::Retention { .. }
            | Applied::Catalog { .. }
            | Applied::Placement { .. }
            | Applied::Txn { .. }
            | Applied::Purge { .. }
            | Applied::Staging { .. } => None,
        }
    }
}

impl Engine {
    /// Commit a batch's items in order into `staged` without committing
    /// it: each is checked against what the items before it staged,
    /// then committed exactly as its own entry would be, at `position`
    /// and expiry clock `clock`. A database's first item must advance
    /// its applied position; later ones share it. The caller checks the
    /// head first and commits, so the whole batch lands in one
    /// transaction. An item whose database disagrees with its journal is
    /// corrupt.
    pub fn commit_batch(
        &self,
        staged: &StagedWrite,
        items: &[BatchItem],
        position: u64,
        clock: u64,
    ) -> HostResult<Vec<Applied>> {
        let mut seen: HashSet<Vec<u8>> = HashSet::new();
        let mut applied = Vec::with_capacity(items.len());
        for item in items {
            match item {
                BatchItem::Patch { db, patch } => {
                    let patch = decode_patch(patch).map_err(Failure::from)?;
                    if &patch.db != db {
                        return Err(Error::corrupt(format!(
                            "batch item targets {db:?} but its journal targets {:?}",
                            patch.db
                        ))
                        .into());
                    }
                    let strict = seen.insert(db.clone());
                    let checked = Self::check_entry(&staged.read(), patch, position, strict)?;
                    let outcome = self.commit_patch(staged.write(), &checked, position, clock)?;
                    applied.push(Applied::Patch {
                        checked: Box::new(checked),
                        outcome,
                    });
                }
                BatchItem::MultiPatch { patches } => {
                    let checked =
                        Self::check_multi_entries(&staged.read(), patches, position, |db| {
                            seen.insert(db.to_vec())
                        })?;
                    let outcome = self.commit_multi(staged.write(), &checked, position, clock)?;
                    applied.push(Applied::Multi { checked, outcome });
                }
                BatchItem::Retention { db, command } => {
                    let strict = seen.insert(db.clone());
                    let (result, revision) =
                        self.commit_retention(staged.write(), db, command, position, strict)?;
                    applied.push(Applied::Retention {
                        db: db.clone(),
                        result,
                        revision,
                    });
                }
                BatchItem::Catalog { change } => {
                    let result = crate::catalog::apply(staged.write(), change, clock)
                        .map_err(Failure::from)?;
                    let revision = staged
                        .write()
                        .meta_u64(crate::catalog::CATALOG_DB, crate::catalog::REVISION_ROW)
                        .map_err(Failure::from)?;
                    applied.push(Applied::Catalog { result, revision });
                }
                BatchItem::Txn { command } => {
                    let (result, finished) =
                        self.commit_txn(staged, command, position, clock, &mut |db: &[u8]| {
                            seen.insert(db.to_vec())
                        })?;
                    applied.push(Applied::Txn {
                        result,
                        revisions: finished.revisions,
                        records: finished.records,
                    });
                }
                BatchItem::Staging { db, command } => {
                    let revision = staged
                        .write()
                        .meta_u64(db, crate::storage::meta::REVISION)
                        .map_err(Failure::from)?;
                    // As deployments: a database that does not serve
                    // here, or that a prepared transaction holds, takes
                    // no step, on every applier alike.
                    let result =
                        match crate::txn::unwritable(&staged.read(), db).map_err(Failure::from)? {
                            Some(refusal) => Err(refusal),
                            None => self.commit_staging(staged.write(), db, command)?,
                        };
                    applied.push(Applied::Staging {
                        db: db.clone(),
                        result,
                        revision,
                    });
                }
                BatchItem::Placement { db, command } => {
                    let (result, effect) =
                        crate::placement::apply(staged.write(), db, command, position)
                            .map_err(Failure::from)?;
                    let revision = staged
                        .write()
                        .meta_u64(db, crate::storage::meta::REVISION)
                        .map_err(Failure::from)?;
                    applied.push(Applied::Placement {
                        db: db.clone(),
                        result,
                        effect,
                        revision,
                    });
                }
                BatchItem::Purge { db, page } => {
                    let strict = seen.insert(db.clone());
                    let (result, revision) =
                        self.commit_purge(staged, db, page, position, strict)?;
                    applied.push(Applied::Purge {
                        db: db.clone(),
                        result,
                        revision,
                    });
                }
            }
        }
        Ok(applied)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::GroupStore;

    #[test]
    fn heads_round_trip_and_default_to_the_start() {
        let dir = tempfile_dir();
        let store = GroupStore::create(dir.join("group.redb")).unwrap();
        assert_eq!(
            Head::read(&store.begin_read().unwrap()).unwrap(),
            Head::Entry(0)
        );
        for head in [
            Head::Entry(7),
            Head::Batch {
                writer: u64::MAX,
                seq: 3,
            },
        ] {
            let write = store.begin_write().unwrap();
            head.record(&write).unwrap();
            write.commit().unwrap();
            assert_eq!(Head::read(&store.begin_read().unwrap()).unwrap(), head);
        }
        assert!(Head::decode(&[2]).is_err());
        assert!(Head::decode(&[0, 1]).is_err());
        let _ = std::fs::remove_dir_all(dir);
    }

    /// A per-test directory under the system temp directory.
    fn tempfile_dir() -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "flower-batch-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }
}
