//! Placement: what a group may do with a named partition it holds.
//!
//! The cluster's catalog says which group should own a partition (see
//! [`crate::catalog`]); the owning group's own state says what that
//! group may do with it, and that state is the fence. Each placed
//! database keeps one [`meta::PLACEMENT`] row in its group, changed only
//! by [`PlacementCommand`]s that its group's writer orders with the
//! group's other writes, so every member, device and restore replay
//! applies the same transitions at the same log positions.
//!
//! A database without the row is *unplaced*: a database of the root
//! group as every database was before partitions existed. Every data
//! write (patches, multi-database patches, retention transitions,
//! transaction prepares) checks the row of each database it touches
//! before anything else and writes nothing when the database is not
//! serving here ([`refusal`]): a write ordered after a freeze or a
//! retirement in its group's log is refused on every applier alike,
//! however stale the router or leader that sent it.

use flower_sdk::{Failure, Value, fail};
use serde::{Deserialize, Serialize};

use crate::error::{Error, HostResult, Result};
use crate::retention::MetaRead;
use crate::storage::{ReadTxn, WriteTxn, meta};

/// Failure code: the partition is moving and serves nothing here for
/// now. Retryable: it serves again here or at its new owner shortly.
pub const PARTITION_MOVING: &str = "partition_moving";
/// Failure code: the partition moved to another group. Retryable once
/// routing knows the new owner.
pub const PARTITION_MOVED: &str = "partition_moved";
/// Failure code: the partition was retired and serves nowhere.
pub const PARTITION_RETIRED: &str = "partition_retired";
/// Failure code: a placement command that the partition's state here
/// does not allow.
pub const PLACEMENT_CONFLICT: &str = "placement_conflict";

/// Failure code: an import's digest disagrees with its source's.
pub const IMPORT_DIGEST: &str = "import_digest";

/// Which part of a move an import carries.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Stage {
    /// The base, copied while the source serves.
    Base,
    /// The rest, after the source froze: the difference from the base,
    /// or the whole frozen image.
    Rest,
}

/// A placed database's state in its group.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum Placement {
    /// Owned and served here at `epoch`.
    Active {
        /// The ownership epoch this group serves.
        epoch: u64,
    },
    /// A move's source after its freeze: nothing writes or reads it here
    /// while its destination takes it over.
    Frozen {
        /// The epoch it was active at.
        epoch: u64,
        /// The move.
        op: String,
        /// Its revision when it froze.
        revision: u64,
    },
    /// A move's destination taking rows in.
    Importing {
        /// The epoch it will activate at.
        epoch: u64,
        /// The move.
        op: String,
        /// The group it comes from.
        source: String,
        /// Which part it is taking.
        stage: Stage,
        /// Pages applied so far in this stage.
        pages: u64,
        /// The digest of the rows it holds, 32 hex digits.
        digest: String,
    },
    /// A move's destination holding a whole base.
    Copied {
        /// The epoch it will activate at.
        epoch: u64,
        /// The move.
        op: String,
        /// The group it comes from.
        source: String,
        /// The source's revision at the base.
        base_revision: u64,
        /// The base's digest.
        digest: String,
    },
    /// A move's destination holding exactly the frozen source, ready to
    /// activate.
    Staged {
        /// The epoch it will activate at.
        epoch: u64,
        /// The move.
        op: String,
        /// The group it comes from.
        source: String,
        /// The frozen source's revision.
        revision: u64,
        /// The frozen source's digest.
        digest: String,
    },
    /// No longer served here: it moved to `moved_to` at the epoch after
    /// `epoch`, or (`None`) was retired where it was. Its rows are gone;
    /// the name stays taken.
    Retired {
        /// The last epoch this group held it at.
        epoch: u64,
        /// The operation that retired it.
        op: String,
        /// The group it moved to, if it moved.
        moved_to: Option<String>,
    },
}

impl Placement {
    /// The ownership epoch of this state.
    pub fn epoch(&self) -> u64 {
        match self {
            Placement::Active { epoch }
            | Placement::Frozen { epoch, .. }
            | Placement::Importing { epoch, .. }
            | Placement::Copied { epoch, .. }
            | Placement::Staged { epoch, .. }
            | Placement::Retired { epoch, .. } => *epoch,
        }
    }

    /// The refusal for a write or read of a database in this state, if
    /// it does not serve.
    pub fn refusal(&self, db: &[u8]) -> Option<Failure> {
        let name = String::from_utf8_lossy(db);
        match self {
            Placement::Active { .. } => None,
            Placement::Frozen { .. }
            | Placement::Importing { .. }
            | Placement::Copied { .. }
            | Placement::Staged { .. } => Some(fail(
                PARTITION_MOVING,
                format!("partition `{name}` is moving: retry shortly"),
            )),
            Placement::Retired {
                moved_to: Some(group),
                ..
            } => Some(fail(
                PARTITION_MOVED,
                format!("partition `{name}` moved to group `{group}`"),
            )),
            Placement::Retired { moved_to: None, .. } => Some(fail(
                PARTITION_RETIRED,
                format!("partition `{name}` was retired"),
            )),
        }
    }
}

/// A change to one database's placement in its group.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(tag = "command", rename_all = "snake_case")]
pub enum PlacementCommand {
    /// Place the database here at `epoch`: an unplaced database becomes
    /// active, keeping any rows it has (an existing root database is
    /// adopted in place).
    Create {
        /// Its first epoch.
        epoch: u64,
        /// The catalog operation creating it.
        op: String,
    },
    /// Retire an active database where it is: its rows go, its name
    /// stays taken.
    Discard {
        /// The epoch it is active at.
        epoch: u64,
        /// The catalog operation retiring it.
        op: String,
    },
    /// A move's source stops serving: every write ordered after this
    /// is refused. Refused while a prepared transaction holds it.
    Freeze {
        /// The epoch it is active at.
        epoch: u64,
        /// The move.
        op: String,
    },
    /// An aborted move's source serves again, at `next`.
    Thaw {
        /// The epoch it was active at.
        epoch: u64,
        /// The move.
        op: String,
        /// The epoch it serves at from now on: past the move's.
        next: u64,
    },
    /// A completed move's source drops its copy.
    Retire {
        /// The epoch it was active at.
        epoch: u64,
        /// The move.
        op: String,
        /// The group it moved to.
        to: String,
    },
    /// A move's destination starts taking a stage in: the base from
    /// nothing, the rest on top of its base (`keep`) or from nothing.
    ImportBegin {
        /// The epoch it will activate at.
        epoch: u64,
        /// The move.
        op: String,
        /// The group it comes from.
        source: String,
        /// The stage.
        stage: Stage,
        /// Keep the copied base and apply a difference onto it.
        keep: bool,
    },
    /// One page of rows, in order.
    ImportRows {
        /// The epoch it will activate at.
        epoch: u64,
        /// The move.
        op: String,
        /// The page's number in its stage, from zero.
        page: u64,
        /// The rows, encoded (see [`crate::movement::encode_changes`]),
        /// hex.
        rows: String,
    },
    /// The stage is complete: its digest must match the source's.
    ImportSeal {
        /// The epoch it will activate at.
        epoch: u64,
        /// The move.
        op: String,
        /// The source's revision the stage reached.
        revision: u64,
        /// The source's digest at that revision, 32 hex digits.
        digest: String,
    },
    /// The destination serves, at the move's epoch.
    Activate {
        /// The move's epoch.
        epoch: u64,
        /// The move.
        op: String,
    },
    /// An aborted move's destination drops what it imported.
    Abandon {
        /// The move's epoch.
        epoch: u64,
        /// The move.
        op: String,
        /// The group the partition stays in.
        source: String,
    },
}

/// What applying a placement command did beyond its rows, for the
/// applier to finish after it commits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PlacementEffect {
    /// Nothing more.
    None,
    /// The database's rows are gone: forget its modules and cached
    /// outcomes.
    Dropped,
    /// The database serves here now: build its modules from its code
    /// rows and register it.
    Activated,
}

/// The database's placement, if it is placed.
pub fn read(read: &ReadTxn, db: &[u8]) -> Result<Option<Placement>> {
    decode(read.meta(db, meta::PLACEMENT)?)
}

/// A destination's copied metadata can arrive before its code. Load runtimes only
/// after activation; frozen sources retain complete code for a possible thaw.
pub fn modules_ready(read: &ReadTxn, db: &[u8]) -> Result<bool> {
    Ok(!matches!(
        self::read(read, db)?,
        Some(
            Placement::Importing { .. }
                | Placement::Copied { .. }
                | Placement::Staged { .. }
                | Placement::Retired { .. }
        )
    ))
}

/// The database's placement inside a write.
pub fn read_in(write: &WriteTxn, db: &[u8]) -> Result<Option<Placement>> {
    decode(write.meta(db, meta::PLACEMENT)?)
}

fn decode(bytes: Option<Vec<u8>>) -> Result<Option<Placement>> {
    bytes
        .map(|bytes| {
            serde_json::from_slice(&bytes)
                .map_err(|error| Error::corrupt(format!("bad placement row: {error}")))
        })
        .transpose()
}

fn write_placement(write: &WriteTxn, db: &[u8], placement: &Placement) -> Result<()> {
    let bytes = serde_json::to_vec(placement)
        .map_err(|error| Error::corrupt(format!("placement does not encode: {error}")))?;
    write.set_meta(db, meta::PLACEMENT, &bytes)
}

/// Why a database does not serve in this write's state, if it does not:
/// active databases serve, and unplaced ones unless this is the root
/// group and its catalog places them in another group.
pub fn refusal(write: &WriteTxn, db: &[u8]) -> Result<Option<Failure>> {
    match read_in(write, db)? {
        Some(placement) => Ok(placement.refusal(db)),
        None => Ok(elsewhere(write.meta(
            crate::catalog::CATALOG_DB,
            &crate::catalog::partition_key(db),
        )?)),
    }
}

/// Why a database does not serve in this read's state, if it does not.
pub fn read_refusal(read: &ReadTxn, db: &[u8]) -> Result<Option<Failure>> {
    match self::read(read, db)? {
        Some(placement) => Ok(placement.refusal(db)),
        None => Ok(elsewhere(read.meta(
            crate::catalog::CATALOG_DB,
            &crate::catalog::partition_key(db),
        )?)),
    }
}

/// The refusal for an unplaced database the root's catalog (when this
/// group holds it) places in another group: the root never serves a
/// partition it does not own, even one a stale router sends it.
fn elsewhere(entry: Option<Vec<u8>>) -> Option<Failure> {
    let entry: crate::catalog::PartitionEntry = serde_json::from_slice(&entry?).ok()?;
    (entry.owner != crate::catalog::ROOT_GROUP).then(|| {
        fail(
            PARTITION_MOVED,
            format!(
                "partition `{}` belongs to group `{}`",
                entry.name, entry.owner
            ),
        )
    })
}

/// The placement as an answer.
pub fn describe(placement: &Placement) -> Value {
    to_value(placement)
}

/// Any serializable state as an answer value.
pub(crate) fn to_value<T: Serialize>(state: &T) -> Value {
    serde_json::to_vec(state)
        .ok()
        .and_then(|bytes| crate::json::parse_value(&bytes).ok())
        .unwrap_or(Value::Null)
}

/// Most incarnations a lineage keeps: claims outlive a lease (an hour at
/// most), never this many moves.
const MAX_LINEAGE: usize = 16;

/// The entries of a [`meta::HISTORY_LINEAGE`] row.
fn lineage_entries(read: &impl MetaRead, db: &[u8]) -> Result<Vec<u64>> {
    let Some(bytes) = read.meta_row(db, meta::HISTORY_LINEAGE)? else {
        return Ok(Vec::new());
    };
    let (entries, rest) = bytes.as_chunks::<8>();
    if !rest.is_empty() {
        return Err(Error::corrupt("history lineage is not whole u64s"));
    }
    Ok(entries.iter().copied().map(u64::from_be_bytes).collect())
}

fn write_lineage(write: &WriteTxn, db: &[u8], entries: &[u64]) -> Result<()> {
    let bytes: Vec<u8> = entries
        .iter()
        .take(MAX_LINEAGE)
        .flat_map(|entry| entry.to_be_bytes())
        .collect();
    write.set_meta(db, meta::HISTORY_LINEAGE, &bytes)
}

/// The earlier history incarnations still of `db`'s own history: those
/// of the groups it moved here from, with no restore since. A claim (or
/// any binding) taken under one of them belongs to this history; one
/// taken under any other incarnation was taken in another history.
///
/// A database's incarnation is its group's, and changes both when it
/// moves (to its destination's) and when its group restores (to a fresh
/// one). Only a move keeps the history, so a move records the lineage:
/// its freeze writes the source's incarnation, then the earlier ones
/// still valid, into the row (which moves with the database), and its
/// activation puts the destination's incarnation in front. The row holds
/// only while the database's incarnation is its first entry: a restore
/// gives a fresh incarnation, which voids it along with every earlier
/// binding, wherever the restored image came from.
pub fn lineage(read: &impl MetaRead, db: &[u8]) -> Result<Vec<u64>> {
    let incarnation = match read.meta_row(db, meta::HISTORY_INCARNATION)? {
        Some(bytes) => crate::storage::codec::decode_u64(&bytes)?,
        None => 0,
    };
    let entries = lineage_entries(read, db)?;
    Ok(match entries.split_first() {
        Some((first, earlier)) if *first == incarnation => earlier.to_vec(),
        _ => Vec::new(),
    })
}

/// The history a database's bindings are checked against: its own
/// incarnation and the earlier ones it carried here through moves since
/// its last restore (see [`lineage`]). Retry identities, retry sessions
/// and their receipts, and queue claims taken under any of them belong
/// to this history.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct History {
    /// The database's incarnation, zero until its group establishes one.
    pub incarnation: u64,
    /// Earlier incarnations still of this history, newest first.
    pub earlier: Vec<u64>,
}

impl History {
    /// `db`'s history as `read` sees it.
    pub fn read(read: &impl MetaRead, db: &[u8]) -> Result<Self> {
        let incarnation = match read.meta_row(db, meta::HISTORY_INCARNATION)? {
            Some(bytes) => crate::storage::codec::decode_u64(&bytes)?,
            None => 0,
        };
        Ok(History {
            incarnation,
            earlier: lineage(read, db)?,
        })
    }

    /// Whether something bound to `incarnation` belongs to this history.
    pub fn holds(&self, incarnation: u64) -> bool {
        incarnation == self.incarnation || self.earlier.contains(&incarnation)
    }
}

/// A move's source freezes: the incarnation the database's bindings
/// were taken under here, and the earlier ones still valid, go with it.
fn record_departure(write: &WriteTxn, db: &[u8]) -> Result<()> {
    let mut entries = vec![write.meta_u64(db, meta::HISTORY_INCARNATION)?];
    entries.extend(lineage(write, db)?);
    write_lineage(write, db, &entries)
}

/// A move's destination activates under `incarnation`: what the source
/// recorded becomes this history's earlier incarnations. Zero (no
/// incarnation yet) binds nothing, so it is never an earlier one.
fn record_arrival(write: &WriteTxn, db: &[u8], incarnation: u64) -> Result<()> {
    let moved = lineage_entries(write, db)?;
    if moved.is_empty() {
        return Ok(());
    }
    let mut entries = vec![incarnation];
    for earlier in moved {
        if earlier != 0 && !entries.contains(&earlier) {
            entries.push(earlier);
        }
    }
    write_lineage(write, db, &entries)
}

/// Label `db` with its group's history incarnation `incarnation`, as
/// establishing the group's incarnation and registering a database do.
/// A database that moved in before its group had an incarnation arrived
/// with zero in front of its lineage ([`record_arrival`]): establishing
/// one takes zero's place there, so the history it carried stays its
/// own. A restore deletes lineages ([`forget_lineage`]), so a restored
/// database never adopts one this way.
pub fn label(write: &WriteTxn, db: &[u8], incarnation: u64) -> Result<()> {
    let current = write.meta_u64(db, meta::HISTORY_INCARNATION)?;
    if current == incarnation {
        return Ok(());
    }
    if current == 0 {
        let mut entries = lineage_entries(write, db)?;
        if entries.first() == Some(&0) {
            entries[0] = incarnation;
            write_lineage(write, db, &entries)?;
        }
    }
    write.set_meta_u64(db, meta::HISTORY_INCARNATION, incarnation)
}

/// A restore ends `db`'s history: whatever incarnations it carried in
/// through moves no longer bind anything here.
pub fn forget_lineage(write: &WriteTxn, db: &[u8]) -> Result<()> {
    write.delete_meta(db, meta::HISTORY_LINEAGE)?;
    Ok(())
}

/// Whether `db` holds anything here, placed or not: every database that
/// ever took a write or a deployment has meta rows (its revision, its
/// code identities).
fn has_rows(write: &WriteTxn, db: &[u8]) -> Result<bool> {
    Ok(!write.scan_meta_prefix(db, b"")?.is_empty())
}

/// Apply a placement command to `db` inside `write` at log `position`:
/// its outcome (the placement it left, or a deterministic refusal that
/// changed nothing) and what the applier finishes after committing.
/// Every command repeats as applied: a move's driver that lost an
/// answer sends it again.
pub fn apply(
    write: &WriteTxn,
    db: &[u8],
    command: &PlacementCommand,
    position: u64,
) -> Result<(HostResult<Value>, PlacementEffect)> {
    let current = read_in(write, db)?;
    let conflict = |message: String| {
        Ok((
            Err(fail(PLACEMENT_CONFLICT, message)),
            PlacementEffect::None,
        ))
    };
    let done = |placement: &Placement, effect| Ok((Ok(describe(placement)), effect));
    let set = |placement: Placement, effect| -> Result<(HostResult<Value>, PlacementEffect)> {
        write_placement(write, db, &placement)?;
        Ok((Ok(describe(&placement)), effect))
    };
    let locked = crate::txn::lock_holder(write, db)?;
    let name = String::from_utf8_lossy(db).into_owned();
    match command {
        PlacementCommand::Create { epoch, .. } => match current {
            None => set(Placement::Active { epoch: *epoch }, PlacementEffect::None),
            Some(ref placement @ Placement::Active { epoch: held }) if held == *epoch => {
                done(placement, PlacementEffect::None)
            }
            Some(other) => conflict(format!(
                "partition `{name}` cannot be created at epoch {epoch}: it is {other:?} here"
            )),
        },
        PlacementCommand::Discard { epoch, op } => match current {
            // A prepared transaction's databases stay until it finishes.
            Some(Placement::Active { .. }) if locked.is_some() => conflict(format!(
                "partition `{name}` holds a prepared transaction: retire it once that finishes"
            )),
            Some(Placement::Active { epoch: held }) if held == *epoch => {
                write.clear_database(db)?;
                set(
                    Placement::Retired {
                        epoch: *epoch,
                        op: op.clone(),
                        moved_to: None,
                    },
                    PlacementEffect::Dropped,
                )
            }
            Some(
                ref placement @ Placement::Retired {
                    epoch: held,
                    op: ref by,
                    moved_to: None,
                },
            ) if held == *epoch && by == op => done(placement, PlacementEffect::None),
            other => conflict(format!(
                "partition `{name}` cannot be retired at epoch {epoch}: it is {other:?} here"
            )),
        },
        PlacementCommand::Freeze { epoch, op } => match current {
            Some(Placement::Active { epoch: held }) if held == *epoch => {
                if let Some(holder) = &locked {
                    return Ok((Err(crate::txn::locked(db, holder)), PlacementEffect::None));
                }
                // The history its claims were taken in here travels with
                // it (see [`lineage`]).
                record_departure(write, db)?;
                let revision = write.meta_u64(db, meta::REVISION)?;
                set(
                    Placement::Frozen {
                        epoch: *epoch,
                        op: op.clone(),
                        revision,
                    },
                    PlacementEffect::None,
                )
            }
            Some(
                ref placement @ Placement::Frozen {
                    epoch: held,
                    op: ref by,
                    ..
                },
            ) if held == *epoch && by == op => done(placement, PlacementEffect::None),
            other => conflict(format!(
                "partition `{name}` cannot freeze at epoch {epoch}: it is {other:?} here"
            )),
        },
        PlacementCommand::Thaw { epoch, op, next } => match current {
            Some(Placement::Frozen {
                epoch: held,
                op: ref by,
                ..
            }) if held == *epoch && by == op => {
                set(Placement::Active { epoch: *next }, PlacementEffect::None)
            }
            // Aborted before it froze: the source never stopped serving.
            Some(Placement::Active { epoch: held }) if held == *epoch => {
                set(Placement::Active { epoch: *next }, PlacementEffect::None)
            }
            Some(ref placement @ Placement::Active { epoch: held }) if held == *next => {
                done(placement, PlacementEffect::None)
            }
            other => conflict(format!(
                "partition `{name}` cannot thaw from epoch {epoch}: it is {other:?} here"
            )),
        },
        PlacementCommand::Retire { epoch, op, to } => match current {
            Some(Placement::Frozen {
                epoch: held,
                op: ref by,
                ..
            }) if held == *epoch && by == op => {
                write.clear_database(db)?;
                set(
                    Placement::Retired {
                        epoch: *epoch,
                        op: op.clone(),
                        moved_to: Some(to.clone()),
                    },
                    PlacementEffect::Dropped,
                )
            }
            Some(
                ref placement @ Placement::Retired {
                    epoch: held,
                    op: ref by,
                    ..
                },
            ) if held == *epoch && by == op => done(placement, PlacementEffect::None),
            other => conflict(format!(
                "partition `{name}` cannot retire from epoch {epoch}: it is {other:?} here"
            )),
        },
        PlacementCommand::ImportBegin {
            epoch,
            op,
            source,
            stage,
            keep,
        } => {
            let importing = |digest: String| Placement::Importing {
                epoch: *epoch,
                op: op.clone(),
                source: source.clone(),
                stage: *stage,
                pages: 0,
                digest,
            };
            let empty = crate::movement::Digest::default().hex();
            match (stage, keep, current) {
                // The base starts from nothing: no copy of it here, or a
                // copy that left at an earlier epoch, or an interrupted
                // base of this very move, which starts over.
                (Stage::Base, false, None) if !has_rows(write, db)? => {
                    set(importing(empty), PlacementEffect::None)
                }
                (Stage::Base, false, Some(Placement::Retired { epoch: held, .. }))
                    if held < *epoch =>
                {
                    write.clear_database(db)?;
                    set(importing(empty), PlacementEffect::None)
                }
                (
                    Stage::Base,
                    false,
                    Some(Placement::Importing {
                        epoch: held,
                        op: ref by,
                        stage: Stage::Base,
                        ..
                    }),
                ) if held == *epoch && by == op => {
                    crate::movement::clear_moving_rows(write, db)?;
                    set(importing(empty), PlacementEffect::None)
                }
                // The difference goes onto the copied base.
                (
                    Stage::Rest,
                    true,
                    Some(Placement::Copied {
                        epoch: held,
                        op: ref by,
                        ref digest,
                        ..
                    }),
                ) if held == *epoch && by == op => {
                    set(importing(digest.clone()), PlacementEffect::None)
                }
                // The whole frozen image replaces whatever is here.
                (
                    Stage::Rest,
                    false,
                    Some(
                        Placement::Copied {
                            epoch: held,
                            op: ref by,
                            ..
                        }
                        | Placement::Importing {
                            epoch: held,
                            op: ref by,
                            ..
                        },
                    ),
                ) if held == *epoch && by == op => {
                    crate::movement::clear_moving_rows(write, db)?;
                    set(importing(empty), PlacementEffect::None)
                }
                (Stage::Rest, false, None) if !has_rows(write, db)? => {
                    set(importing(empty), PlacementEffect::None)
                }
                (_, _, other) => conflict(format!(
                    "partition `{name}` cannot begin importing {stage:?} of move `{op}`: it is \
                     {other:?} here"
                )),
            }
        }
        PlacementCommand::ImportRows {
            epoch,
            op,
            page,
            rows,
        } => match current {
            Some(Placement::Importing {
                epoch: held,
                op: ref by,
                ref source,
                stage,
                pages,
                ref digest,
            }) if held == *epoch && by == op => {
                if *page < pages {
                    return done(current.as_ref().expect("matched"), PlacementEffect::None);
                }
                if *page > pages {
                    return conflict(format!(
                        "partition `{name}` expects import page {pages}, not {page}"
                    ));
                }
                let packed = crate::hex::decode(rows)
                    .ok_or_else(|| Error::corrupt("an import page is not hex".to_string()))?;
                let changes = crate::movement::decode_changes(&packed)?;
                let mut digest = crate::movement::Digest::parse(digest)?;
                crate::movement::apply_changes(write, db, &changes, &mut digest)?;
                set(
                    Placement::Importing {
                        epoch: held,
                        op: op.clone(),
                        source: source.clone(),
                        stage,
                        pages: pages + 1,
                        digest: digest.hex(),
                    },
                    PlacementEffect::None,
                )
            }
            other => conflict(format!(
                "partition `{name}` is not importing move `{op}`: it is {other:?} here"
            )),
        },
        PlacementCommand::ImportSeal {
            epoch,
            op,
            revision,
            digest,
        } => match current {
            Some(Placement::Importing {
                epoch: held,
                op: ref by,
                ref source,
                stage,
                digest: ref held_digest,
                ..
            }) if held == *epoch && by == op => {
                if held_digest != digest {
                    return Ok((
                        Err(fail(
                            IMPORT_DIGEST,
                            format!(
                                "partition `{name}` imported rows with digest {held_digest}, the \
                                 source's are {digest}"
                            ),
                        )),
                        PlacementEffect::None,
                    ));
                }
                let next = match stage {
                    Stage::Base => Placement::Copied {
                        epoch: held,
                        op: op.clone(),
                        source: source.clone(),
                        base_revision: *revision,
                        digest: digest.clone(),
                    },
                    Stage::Rest => Placement::Staged {
                        epoch: held,
                        op: op.clone(),
                        source: source.clone(),
                        revision: *revision,
                        digest: digest.clone(),
                    },
                };
                set(next, PlacementEffect::None)
            }
            Some(
                ref placement @ (Placement::Copied {
                    epoch: held,
                    op: ref by,
                    ..
                }
                | Placement::Staged {
                    epoch: held,
                    op: ref by,
                    ..
                }),
            ) if held == *epoch && by == op => done(placement, PlacementEffect::None),
            other => conflict(format!(
                "partition `{name}` has no import of move `{op}` to seal: it is {other:?} here"
            )),
        },
        PlacementCommand::Activate { epoch, op } => match current {
            Some(Placement::Staged {
                epoch: held,
                op: ref by,
                ..
            }) if held == *epoch && by == op => {
                // The rows describing its place in this group are this
                // group's own: its position and history incarnation from
                // here on. The history goes on: the source's incarnation
                // stays its own through the lineage (see [`lineage`]).
                write.set_meta_u64(db, meta::APPLIED_LOG, position)?;
                let incarnation =
                    write.meta_u64(crate::batch::HEAD_DB, meta::HISTORY_INCARNATION)?;
                record_arrival(write, db, incarnation)?;
                write.set_meta_u64(db, meta::HISTORY_INCARNATION, incarnation)?;
                set(
                    Placement::Active { epoch: held },
                    PlacementEffect::Activated,
                )
            }
            Some(ref placement @ Placement::Active { epoch: held }) if held == *epoch => {
                done(placement, PlacementEffect::None)
            }
            other => conflict(format!(
                "partition `{name}` cannot activate move `{op}`: it is {other:?} here"
            )),
        },
        PlacementCommand::Abandon { epoch, op, source } => match current {
            // Never a database that serves here.
            Some(Placement::Active { .. } | Placement::Frozen { .. }) => conflict(format!(
                "partition `{name}` serves here: an abandoned import never drops it"
            )),
            Some(
                ref placement @ Placement::Retired {
                    epoch: held,
                    op: ref by,
                    ..
                },
            ) if held == *epoch && by == op => done(placement, PlacementEffect::None),
            // Whatever it imported goes; a marker at the move's epoch
            // refuses that move's late imports and routes callers home.
            _ => {
                if current.is_some() {
                    write.clear_database(db)?;
                }
                set(
                    Placement::Retired {
                        epoch: *epoch,
                        op: op.clone(),
                        moved_to: Some(source.clone()),
                    },
                    PlacementEffect::Dropped,
                )
            }
        },
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::storage::GroupStore;

    /// A store in a directory of its own, removed when dropped.
    pub(crate) struct TestStore {
        pub(crate) store: GroupStore,
        dir: std::path::PathBuf,
    }

    impl Drop for TestStore {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    pub(crate) fn test_store(name: &str) -> TestStore {
        let dir = std::env::temp_dir().join(format!(
            "flower-{name}-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let store = GroupStore::create(dir.join("group.redb")).unwrap();
        TestStore { store, dir }
    }

    fn store() -> ((), TestStore) {
        ((), test_store("placement"))
    }

    fn run(store: &GroupStore, db: &[u8], command: PlacementCommand) -> HostResult<Value> {
        let write = store.begin_write().unwrap();
        let (outcome, _) = apply(&write, db, &command, 1).unwrap();
        write.commit().unwrap();
        outcome
    }

    #[test]
    fn create_adopts_and_discard_retires_for_good() {
        let (_, fixture) = store();
        let store = &fixture.store;
        let write = store.begin_write().unwrap();
        write.set_meta_u64(b"acme", meta::REVISION, 7).unwrap();
        write.commit().unwrap();
        let create = PlacementCommand::Create {
            epoch: 1,
            op: "c".into(),
        };
        run(store, b"acme", create.clone()).unwrap();
        // Adopted in place, and a repeat answers as applied.
        assert_eq!(
            store
                .begin_read()
                .unwrap()
                .meta_u64(b"acme", meta::REVISION)
                .unwrap(),
            7
        );
        run(store, b"acme", create).unwrap();
        let other_epoch = PlacementCommand::Create {
            epoch: 2,
            op: "c2".into(),
        };
        assert_eq!(
            run(store, b"acme", other_epoch).unwrap_err().code,
            PLACEMENT_CONFLICT
        );
        let discard = PlacementCommand::Discard {
            epoch: 1,
            op: "d".into(),
        };
        run(store, b"acme", discard.clone()).unwrap();
        run(store, b"acme", discard).unwrap();
        let read = store.begin_read().unwrap();
        assert_eq!(read.meta_u64(b"acme", meta::REVISION).unwrap(), 0);
        assert_eq!(
            read_refusal(&read, b"acme").unwrap().unwrap().code,
            PARTITION_RETIRED
        );
        drop(read);
        // The name stays taken.
        let again = PlacementCommand::Create {
            epoch: 1,
            op: "c".into(),
        };
        assert_eq!(
            run(store, b"acme", again).unwrap_err().code,
            PLACEMENT_CONFLICT
        );
    }

    /// The rows of `db` as one encoded import page.
    fn page_of(store: &GroupStore, db: &[u8]) -> String {
        let read = store.begin_read().unwrap();
        let page = crate::movement::export_page(&read, db, None, usize::MAX).unwrap();
        assert!(page.next.is_none());
        crate::hex::encode(&crate::movement::encode_changes(&page.changes))
    }

    #[test]
    fn an_import_seals_only_at_its_sources_digest_and_activates_once() {
        let source = test_store("import-source");
        let source = &source.store;
        let write = source.begin_write().unwrap();
        write.put_record(b"acme", 1, b"k", b"\"v\"").unwrap();
        write.set_meta_u64(b"acme", meta::REVISION, 3).unwrap();
        write.commit().unwrap();
        let digest = crate::movement::digest(&source.begin_read().unwrap(), b"acme")
            .unwrap()
            .hex();
        let (_, fixture) = store();
        let store = &fixture.store;
        let begin = |stage, keep| PlacementCommand::ImportBegin {
            epoch: 2,
            op: "m".into(),
            source: "east".into(),
            stage,
            keep,
        };
        run(store, b"acme", begin(Stage::Rest, false)).unwrap();
        let rows = PlacementCommand::ImportRows {
            epoch: 2,
            op: "m".into(),
            page: 0,
            rows: page_of(source, b"acme"),
        };
        run(store, b"acme", rows.clone()).unwrap();
        // A page repeated by a driver that lost the answer changes nothing.
        run(store, b"acme", rows).unwrap();
        // The importing database serves nothing yet.
        let read = store.begin_read().unwrap();
        assert_eq!(
            read_refusal(&read, b"acme").unwrap().unwrap().code,
            PARTITION_MOVING
        );
        drop(read);
        // A digest other than the source's is refused and changes nothing.
        let seal = |digest: &str| PlacementCommand::ImportSeal {
            epoch: 2,
            op: "m".into(),
            revision: 3,
            digest: digest.into(),
        };
        let wrong = crate::movement::Digest(7).hex();
        assert_eq!(
            run(store, b"acme", seal(&wrong)).unwrap_err().code,
            IMPORT_DIGEST
        );
        run(store, b"acme", seal(&digest)).unwrap();
        run(store, b"acme", seal(&digest)).unwrap();
        let activate = PlacementCommand::Activate {
            epoch: 2,
            op: "m".into(),
        };
        let write = store.begin_write().unwrap();
        let (outcome, effect) = apply(&write, b"acme", &activate, 41).unwrap();
        write.commit().unwrap();
        outcome.unwrap();
        assert_eq!(effect, PlacementEffect::Activated);
        let read = store.begin_read().unwrap();
        assert_eq!(read.meta_u64(b"acme", meta::REVISION).unwrap(), 3);
        assert_eq!(read.meta_u64(b"acme", meta::APPLIED_LOG).unwrap(), 41);
        assert_eq!(
            read.get_record(b"acme", 1, b"k").unwrap().as_deref(),
            Some(&b"\"v\""[..])
        );
        drop(read);
        // Activating again answers as applied, and changes nothing.
        let write = store.begin_write().unwrap();
        let (outcome, effect) = apply(&write, b"acme", &activate, 50).unwrap();
        write.commit().unwrap();
        outcome.unwrap();
        assert_eq!(effect, PlacementEffect::None);
        // An abandon never drops a database that serves here.
        let abandon = PlacementCommand::Abandon {
            epoch: 2,
            op: "m".into(),
            source: "east".into(),
        };
        assert_eq!(
            run(store, b"acme", abandon).unwrap_err().code,
            PLACEMENT_CONFLICT
        );
    }

    #[test]
    fn an_abandoned_import_refuses_its_late_pages_until_a_later_move() {
        let (_, fixture) = store();
        let store = &fixture.store;
        let begin = |epoch, op: &str| PlacementCommand::ImportBegin {
            epoch,
            op: op.into(),
            source: "east".into(),
            stage: Stage::Base,
            keep: false,
        };
        run(store, b"acme", begin(2, "m")).unwrap();
        let abandon = PlacementCommand::Abandon {
            epoch: 2,
            op: "m".into(),
            source: "east".into(),
        };
        run(store, b"acme", abandon.clone()).unwrap();
        run(store, b"acme", abandon).unwrap();
        // The aborted move's late commands are refused.
        assert_eq!(
            run(store, b"acme", begin(2, "m")).unwrap_err().code,
            PLACEMENT_CONFLICT
        );
        let late = PlacementCommand::ImportRows {
            epoch: 2,
            op: "m".into(),
            page: 0,
            rows: crate::hex::encode(&crate::movement::encode_changes(&[])),
        };
        assert_eq!(
            run(store, b"acme", late).unwrap_err().code,
            PLACEMENT_CONFLICT
        );
        // Its marker routes callers to the source.
        let read = store.begin_read().unwrap();
        assert_eq!(
            read_refusal(&read, b"acme").unwrap().unwrap().code,
            PARTITION_MOVED
        );
        drop(read);
        // A later move, at a later epoch, imports.
        run(store, b"acme", begin(4, "m2")).unwrap();
    }

    #[test]
    fn a_frozen_source_thaws_past_the_moves_epoch_or_retires() {
        let (_, fixture) = store();
        let store = &fixture.store;
        run(
            store,
            b"acme",
            PlacementCommand::Create {
                epoch: 1,
                op: "c".into(),
            },
        )
        .unwrap();
        let freeze = PlacementCommand::Freeze {
            epoch: 1,
            op: "m".into(),
        };
        run(store, b"acme", freeze.clone()).unwrap();
        run(store, b"acme", freeze).unwrap();
        let read = store.begin_read().unwrap();
        assert_eq!(
            read_refusal(&read, b"acme").unwrap().unwrap().code,
            PARTITION_MOVING
        );
        drop(read);
        // Another move's thaw is refused; this one's serves at epoch 3.
        let thaw = |op: &str| PlacementCommand::Thaw {
            epoch: 1,
            op: op.into(),
            next: 3,
        };
        assert_eq!(
            run(store, b"acme", thaw("other")).unwrap_err().code,
            PLACEMENT_CONFLICT
        );
        run(store, b"acme", thaw("m")).unwrap();
        run(store, b"acme", thaw("m")).unwrap();
        let view = store.begin_read().unwrap();
        assert_eq!(
            super::read(&view, b"acme").unwrap(),
            Some(Placement::Active { epoch: 3 })
        );
        drop(view);
        // The aborted move cannot freeze it again at its old epoch.
        let refreeze = PlacementCommand::Freeze {
            epoch: 1,
            op: "m".into(),
        };
        assert_eq!(
            run(store, b"acme", refreeze).unwrap_err().code,
            PLACEMENT_CONFLICT
        );
    }
}
