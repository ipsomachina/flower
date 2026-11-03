//! The partition catalog: which groups exist, which group owns each
//! named partition at which epoch, and the moves in flight.
//!
//! The catalog is the reserved database [`CATALOG_DB`] of the root
//! group, changed only by [`CatalogChange`]s the root's writer orders,
//! so it replicates, snapshots and backs up with the root and applies
//! the same everywhere (members, devices, restore replay). Every change
//! names an operation; repeating a change with the operation that made
//! it answers its outcome again, so an uncertain operator request is
//! retried safely. Changes that the state does not allow are refused
//! deterministically and change nothing.
//!
//! The catalog says where a partition *should* be. What a group may do
//! with a database it holds is that group's own placement row (see
//! [`crate::placement`]), which every write checks: a router acting on a
//! catalog it applied late is refused there, never obeyed.

use std::collections::BTreeMap;

use flower_sdk::{Failure, Value, fail};
use serde::{Deserialize, Serialize};

use crate::error::{Error, HostResult, Result};
use crate::placement::to_value;
use crate::storage::{ReadTxn, WriteTxn};

mod management;
pub use management::{LeaderPreference, ResizePlan};

/// The root group's catalog database.
pub const CATALOG_DB: &[u8] = b"$catalog";
/// The bootstrap group's name (see the cluster's group identities).
pub const ROOT_GROUP: &str = "root";
/// Finished moves the catalog keeps for operators.
pub const MOVE_HISTORY: usize = 256;
/// Failure code: a catalog change the catalog's state does not allow.
pub const CATALOG_CONFLICT: &str = "catalog_conflict";
/// Failure code: a catalog change naming something that does not exist.
pub const CATALOG_UNKNOWN: &str = "catalog_unknown";

const GROUP_PREFIX: &[u8] = b"group\x00";
const PARTITION_PREFIX: &[u8] = b"partition\x00";
const MOVE_PREFIX: &[u8] = b"move\x00";
/// Meta row holding the catalog revision.
pub const REVISION_ROW: &[u8] = b"catalog_revision";

/// A group's lifecycle in the catalog.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GroupStatus {
    /// Listed; its members open empty replicas.
    Creating,
    /// Its founder is initializing its Raft membership.
    Initializing,
    /// It elected a leader and may own partitions.
    Active,
    /// Removed: its members close their replicas. Final.
    Removed,
}

/// One group, as the catalog lists it.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct GroupEntry {
    /// The group's name.
    pub name: String,
    /// Its uid, fixed at creation.
    #[serde(with = "uid")]
    pub uid: u64,
    /// Its founding members, by node ID, ascending: replicas open on
    /// these nodes, and routing tries them for its leader.
    pub members: Vec<u64>,
    /// Where it is in its lifecycle.
    pub status: GroupStatus,
    /// The member founding it, once one claimed it.
    pub founder: Option<u64>,
    /// The operation that created it (or removed it).
    pub op: String,
    /// When it was created, ms since the epoch.
    pub created_ms: u64,
}

/// A partition's lifecycle in the catalog.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PartitionStatus {
    /// Listed; its owner is placing it.
    Creating,
    /// Owned and served by its owner.
    Active,
    /// A move is in flight (see [`Movement`]).
    Moving,
    /// Being retired where it is.
    Retiring,
    /// Retired: served nowhere, its name taken for good.
    Retired,
}

/// A move's phase (see the spec's move protocol).
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum MovePhase {
    /// The destination imports a base while the source serves.
    Copying,
    /// The source freezes.
    Freezing,
    /// The destination imports the rest and seals.
    Importing,
    /// Cut over: the destination owns the partition and activates.
    Activating,
    /// The source retires its copy.
    Retiring,
    /// Done.
    Complete,
    /// Given up: the source thaws.
    Aborting,
    /// The destination abandons what it imported.
    Abandoning,
    /// Given up and cleaned up.
    Aborted,
}

impl MovePhase {
    /// Whether nothing more happens to a move in this phase.
    pub fn finished(self) -> bool {
        matches!(self, MovePhase::Complete | MovePhase::Aborted)
    }

    /// Whether a move in this phase may still be given up: before the
    /// cutover.
    pub fn abortable(self) -> bool {
        matches!(
            self,
            MovePhase::Copying | MovePhase::Freezing | MovePhase::Importing
        )
    }

    /// The phase a move advances to from this one, if any.
    pub fn next(self) -> Option<MovePhase> {
        Some(match self {
            MovePhase::Copying => MovePhase::Freezing,
            MovePhase::Freezing => MovePhase::Importing,
            MovePhase::Importing => MovePhase::Activating,
            MovePhase::Activating => MovePhase::Retiring,
            MovePhase::Retiring => MovePhase::Complete,
            MovePhase::Aborting => MovePhase::Abandoning,
            MovePhase::Abandoning => MovePhase::Aborted,
            MovePhase::Complete | MovePhase::Aborted => return None,
        })
    }
}

/// One move of a partition between groups.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Movement {
    /// The operation that began it.
    pub op: String,
    /// The partition.
    pub partition: String,
    /// The group it moves from.
    pub source: String,
    /// The group it moves to.
    pub destination: String,
    /// The epoch the source held it at.
    pub source_epoch: u64,
    /// The epoch the destination activates it at: `source_epoch + 1`.
    pub epoch: u64,
    /// Where the move is.
    pub phase: MovePhase,
    /// Why it was given up, once it was.
    pub reason: Option<String>,
    /// When it began, ms since the epoch.
    pub started_ms: u64,
    /// When it last changed phase, ms since the epoch.
    pub updated_ms: u64,
}

/// One named partition, as the catalog lists it.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct PartitionEntry {
    /// The partition's name: its database's name.
    pub name: String,
    /// The group that serves it: the destination from a move's cutover.
    pub owner: String,
    /// Its ownership epoch at the owner.
    pub epoch: u64,
    /// Where it is in its lifecycle.
    pub status: PartitionStatus,
    /// The operation that last changed it.
    pub op: String,
    /// The move in flight, if any.
    pub movement: Option<Movement>,
    /// When it last changed, ms since the epoch.
    pub updated_ms: u64,
}

/// A change to the catalog.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(tag = "change", rename_all = "snake_case")]
pub enum CatalogChange {
    /// Balance partition counts over these active groups using durable moves.
    Resize { groups: Vec<String>, op: String },
    /// Set or clear a group's preferred leader. Clearing disables placement.
    PreferLeader {
        group: String,
        node: Option<u64>,
        op: String,
    },
    /// List a new group with its founding members.
    CreateGroup {
        /// Its name.
        group: String,
        /// Its uid, which the root leader drew.
        #[serde(with = "uid")]
        uid: u64,
        /// Its founding members.
        members: Vec<u64>,
        /// The operation.
        op: String,
    },
    /// The founder claims the group before initializing it.
    GroupInitializing {
        /// The group.
        group: String,
        /// Its uid.
        #[serde(with = "uid")]
        uid: u64,
        /// The founding member.
        founder: u64,
    },
    /// The group elected a leader.
    GroupActive {
        /// The group.
        group: String,
        /// Its uid.
        #[serde(with = "uid")]
        uid: u64,
    },
    /// Remove a group that owns nothing.
    RemoveGroup {
        /// The group.
        group: String,
        /// The operation.
        op: String,
    },
    /// List a new partition owned by `group` at epoch 1.
    CreatePartition {
        /// Its name.
        partition: String,
        /// Its owner.
        group: String,
        /// The operation.
        op: String,
    },
    /// The owner placed it: it serves.
    PartitionReady {
        /// The partition.
        partition: String,
        /// The operation that created it.
        op: String,
    },
    /// Begin retiring a partition where it is.
    RetirePartition {
        /// The partition.
        partition: String,
        /// The operation.
        op: String,
    },
    /// The owner retired it.
    PartitionRetired {
        /// The partition.
        partition: String,
        /// The operation that began retiring it.
        op: String,
    },
    /// Begin moving a partition to `destination`.
    BeginMove {
        /// The partition.
        partition: String,
        /// The group it moves to.
        destination: String,
        /// The operation.
        op: String,
    },
    /// The move's current step applied: record the next phase.
    AdvanceMove {
        /// The partition.
        partition: String,
        /// The operation that began the move.
        op: String,
        /// The phase it advances to.
        phase: MovePhase,
    },
    /// Give up a move before its cutover.
    AbortMove {
        /// The partition.
        partition: String,
        /// The operation that began the move.
        op: String,
        /// Why.
        reason: String,
    },
}

/// The whole catalog at one revision.
#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq, Eq)]
pub struct Catalog {
    /// Changes applied so far.
    pub revision: u64,
    /// Every group but the root, by name.
    pub groups: BTreeMap<String, GroupEntry>,
    /// Every partition, by name.
    pub partitions: BTreeMap<String, PartitionEntry>,
    /// The last [`MOVE_HISTORY`] finished moves, oldest first.
    pub moves: Vec<Movement>,
    /// Explicit leader preferences, including automatic placement at creation.
    #[serde(default)]
    pub leaders: BTreeMap<String, LeaderPreference>,
}

impl Catalog {
    /// Read the catalog as `read` sees it.
    pub fn read(read: &ReadTxn) -> Result<Catalog> {
        let revision = read.meta_u64(CATALOG_DB, REVISION_ROW)?;
        let mut groups = BTreeMap::new();
        for (_, value) in read.scan_meta_prefix(CATALOG_DB, GROUP_PREFIX)? {
            let entry: GroupEntry = decode(&value)?;
            groups.insert(entry.name.clone(), entry);
        }
        let mut partitions = BTreeMap::new();
        for (_, value) in read.scan_meta_prefix(CATALOG_DB, PARTITION_PREFIX)? {
            let entry: PartitionEntry = decode(&value)?;
            partitions.insert(entry.name.clone(), entry);
        }
        let moves = read
            .scan_meta_prefix(CATALOG_DB, MOVE_PREFIX)?
            .into_iter()
            .map(|(_, value)| decode(&value))
            .collect::<Result<_>>()?;
        Ok(Catalog {
            revision,
            groups,
            partitions,
            moves,
            leaders: management::leaders(read)?,
        })
    }

    /// The catalog as an answer value.
    pub fn to_value(&self) -> Value {
        to_value(self)
    }
}

/// One partition's entry, as `read` sees it.
pub fn partition(read: &ReadTxn, name: &str) -> Result<Option<PartitionEntry>> {
    read.meta(CATALOG_DB, &key(PARTITION_PREFIX, name))?
        .map(|value| decode(&value))
        .transpose()
}

/// One group's entry, as `read` sees it.
pub fn group(read: &ReadTxn, name: &str) -> Result<Option<GroupEntry>> {
    read.meta(CATALOG_DB, &key(GROUP_PREFIX, name))?
        .map(|value| decode(&value))
        .transpose()
}

fn key(prefix: &[u8], name: &str) -> Vec<u8> {
    partition_key_of(prefix, name.as_bytes())
}

fn partition_key_of(prefix: &[u8], name: &[u8]) -> Vec<u8> {
    let mut key = prefix.to_vec();
    key.extend_from_slice(name);
    key
}

/// The catalog row naming database `db` as a partition.
pub fn partition_key(db: &[u8]) -> Vec<u8> {
    partition_key_of(PARTITION_PREFIX, db)
}

fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    serde_json::from_slice(bytes)
        .map_err(|error| Error::corrupt(format!("bad catalog row: {error}")))
}

fn put<T: Serialize>(write: &WriteTxn, key: &[u8], value: &T) -> Result<()> {
    let bytes = serde_json::to_vec(value)
        .map_err(|error| Error::corrupt(format!("catalog row does not encode: {error}")))?;
    write.set_meta(CATALOG_DB, key, &bytes)
}

fn read_group(write: &WriteTxn, name: &str) -> Result<Option<GroupEntry>> {
    write
        .meta(CATALOG_DB, &key(GROUP_PREFIX, name))?
        .map(|value| decode(&value))
        .transpose()
}

fn read_partition(write: &WriteTxn, name: &str) -> Result<Option<PartitionEntry>> {
    write
        .meta(CATALOG_DB, &key(PARTITION_PREFIX, name))?
        .map(|value| decode(&value))
        .transpose()
}

/// What one change did.
enum Step {
    /// It changed the catalog; its answer.
    Changed(Value),
    /// It repeated an applied change: its answer, nothing written.
    Replayed(Value),
    /// The state does not allow it: nothing written.
    Refused(Failure),
}

fn conflict(message: impl Into<String>) -> Result<Step> {
    Ok(Step::Refused(fail(CATALOG_CONFLICT, message.into())))
}

fn unknown(message: impl Into<String>) -> Result<Step> {
    Ok(Step::Refused(fail(CATALOG_UNKNOWN, message.into())))
}

/// Whether `name` names a group partitions may be placed in now.
fn usable_group(write: &WriteTxn, name: &str) -> Result<bool> {
    if name == ROOT_GROUP {
        return Ok(true);
    }
    Ok(read_group(write, name)?.is_some_and(|group| group.status == GroupStatus::Active))
}

/// Whether `name` may name a partition: a database name, not empty,
/// without control characters, not reserved (`$`).
pub fn valid_partition_name(name: &str) -> bool {
    !name.is_empty() && !name.starts_with('$') && !name.chars().any(char::is_control)
}

/// Apply a change inside `write` at `now_ms` (the replicated expiry
/// clock, so every applier stamps alike): its answer (the entry it
/// left), or a deterministic refusal that changed nothing. Every change
/// that changed the catalog moves its revision.
pub fn apply(write: &WriteTxn, change: &CatalogChange, now_ms: u64) -> Result<HostResult<Value>> {
    Ok(match step(write, change, now_ms)? {
        Step::Changed(value) => {
            let revision = write.meta_u64(CATALOG_DB, REVISION_ROW)? + 1;
            write.set_meta_u64(CATALOG_DB, REVISION_ROW, revision)?;
            Ok(value)
        }
        Step::Replayed(value) => Ok(value),
        Step::Refused(failure) => Err(failure),
    })
}

fn step(write: &WriteTxn, change: &CatalogChange, now_ms: u64) -> Result<Step> {
    match change {
        CatalogChange::Resize { groups, op } => management::resize(write, groups, op, now_ms),
        CatalogChange::PreferLeader { group, node, op } => {
            management::prefer(write, group, *node, op)
        }
        CatalogChange::CreateGroup {
            group,
            uid,
            members,
            op,
        } => {
            if group == ROOT_GROUP || *uid == 0 {
                return conflict(format!("`{ROOT_GROUP}` exists from the start"));
            }
            if let Some(existing) = read_group(write, group)? {
                if existing.op == *op && existing.status != GroupStatus::Removed {
                    return Ok(Step::Replayed(to_value(&existing)));
                }
                return conflict(format!("group `{group}` exists"));
            }
            let mut sorted = members.clone();
            sorted.sort_unstable();
            sorted.dedup();
            if sorted.is_empty() || sorted.len() != members.len() {
                return conflict("a group needs distinct founding members");
            }
            let entry = GroupEntry {
                name: group.clone(),
                uid: *uid,
                members: sorted,
                status: GroupStatus::Creating,
                founder: None,
                op: op.clone(),
                created_ms: now_ms,
            };
            put(write, &key(GROUP_PREFIX, group), &entry)?;
            management::spread(write, &entry)?;
            Ok(Step::Changed(to_value(&entry)))
        }
        CatalogChange::GroupInitializing {
            group,
            uid,
            founder,
        } => {
            let Some(mut entry) = read_group(write, group)?.filter(|entry| entry.uid == *uid)
            else {
                return unknown(format!("no group `{group}` with that uid"));
            };
            match (entry.status, entry.founder) {
                (GroupStatus::Creating, _) if entry.members.contains(founder) => {
                    entry.status = GroupStatus::Initializing;
                    entry.founder = Some(*founder);
                    put(write, &key(GROUP_PREFIX, group), &entry)?;
                    Ok(Step::Changed(to_value(&entry)))
                }
                (GroupStatus::Initializing | GroupStatus::Active, Some(by)) if by == *founder => {
                    Ok(Step::Replayed(to_value(&entry)))
                }
                _ => conflict(format!(
                    "group `{group}` cannot be founded by node {founder} now"
                )),
            }
        }
        CatalogChange::GroupActive { group, uid } => {
            let Some(mut entry) = read_group(write, group)?.filter(|entry| entry.uid == *uid)
            else {
                return unknown(format!("no group `{group}` with that uid"));
            };
            match entry.status {
                GroupStatus::Creating | GroupStatus::Initializing => {
                    entry.status = GroupStatus::Active;
                    put(write, &key(GROUP_PREFIX, group), &entry)?;
                    Ok(Step::Changed(to_value(&entry)))
                }
                GroupStatus::Active => Ok(Step::Replayed(to_value(&entry))),
                GroupStatus::Removed => conflict(format!("group `{group}` was removed")),
            }
        }
        CatalogChange::RemoveGroup { group, op } => {
            if group == ROOT_GROUP {
                return conflict(format!("`{ROOT_GROUP}` is never removed"));
            }
            let Some(mut entry) = read_group(write, group)? else {
                return unknown(format!("no group `{group}`"));
            };
            if entry.status == GroupStatus::Removed {
                return if entry.op == *op {
                    Ok(Step::Replayed(to_value(&entry)))
                } else {
                    conflict(format!("group `{group}` was removed"))
                };
            }
            for (_, value) in write.scan_meta_prefix(CATALOG_DB, PARTITION_PREFIX)? {
                let partition: PartitionEntry = decode(&value)?;
                let moving = partition.movement.as_ref().is_some_and(|movement| {
                    movement.source == *group || movement.destination == *group
                });
                if (partition.owner == *group && partition.status != PartitionStatus::Retired)
                    || moving
                {
                    return conflict(format!(
                        "group `{group}` still owns or moves partition `{}`",
                        partition.name
                    ));
                }
            }
            entry.status = GroupStatus::Removed;
            entry.op = op.clone();
            put(write, &key(GROUP_PREFIX, group), &entry)?;
            write.delete_meta(CATALOG_DB, &key(management::LEADERS, group))?;
            Ok(Step::Changed(to_value(&entry)))
        }
        CatalogChange::CreatePartition {
            partition,
            group,
            op,
        } => {
            if !valid_partition_name(partition) {
                return conflict(format!(
                    "`{partition}` cannot name a partition: it must be non-empty, without \
                     control characters, and not start with `$`"
                ));
            }
            if let Some(existing) = read_partition(write, partition)? {
                if existing.op == *op && existing.status != PartitionStatus::Retired {
                    return Ok(Step::Replayed(to_value(&existing)));
                }
                return conflict(format!("partition `{partition}` exists"));
            }
            if !usable_group(write, group)? {
                return unknown(format!("no active group `{group}`"));
            }
            let entry = PartitionEntry {
                name: partition.clone(),
                owner: group.clone(),
                epoch: 1,
                status: PartitionStatus::Creating,
                op: op.clone(),
                movement: None,
                updated_ms: now_ms,
            };
            put(write, &key(PARTITION_PREFIX, partition), &entry)?;
            Ok(Step::Changed(to_value(&entry)))
        }
        CatalogChange::PartitionReady { partition, op } => {
            let Some(mut entry) = read_partition(write, partition)? else {
                return unknown(format!("no partition `{partition}`"));
            };
            match entry.status {
                PartitionStatus::Creating if entry.op == *op => {
                    entry.status = PartitionStatus::Active;
                    entry.updated_ms = now_ms;
                    put(write, &key(PARTITION_PREFIX, partition), &entry)?;
                    Ok(Step::Changed(to_value(&entry)))
                }
                PartitionStatus::Active if entry.op == *op => Ok(Step::Replayed(to_value(&entry))),
                _ => conflict(format!(
                    "partition `{partition}` is not being created by `{op}`"
                )),
            }
        }
        CatalogChange::RetirePartition { partition, op } => {
            let Some(mut entry) = read_partition(write, partition)? else {
                return unknown(format!("no partition `{partition}`"));
            };
            match entry.status {
                PartitionStatus::Active => {
                    entry.status = PartitionStatus::Retiring;
                    entry.op = op.clone();
                    entry.updated_ms = now_ms;
                    put(write, &key(PARTITION_PREFIX, partition), &entry)?;
                    Ok(Step::Changed(to_value(&entry)))
                }
                PartitionStatus::Retiring | PartitionStatus::Retired if entry.op == *op => {
                    Ok(Step::Replayed(to_value(&entry)))
                }
                _ => conflict(format!(
                    "partition `{partition}` cannot be retired while {:?}",
                    entry.status
                )),
            }
        }
        CatalogChange::PartitionRetired { partition, op } => {
            let Some(mut entry) = read_partition(write, partition)? else {
                return unknown(format!("no partition `{partition}`"));
            };
            match entry.status {
                PartitionStatus::Retiring if entry.op == *op => {
                    entry.status = PartitionStatus::Retired;
                    entry.updated_ms = now_ms;
                    put(write, &key(PARTITION_PREFIX, partition), &entry)?;
                    Ok(Step::Changed(to_value(&entry)))
                }
                PartitionStatus::Retired if entry.op == *op => Ok(Step::Replayed(to_value(&entry))),
                _ => conflict(format!(
                    "partition `{partition}` is not being retired by `{op}`"
                )),
            }
        }
        CatalogChange::BeginMove {
            partition,
            destination,
            op,
        } => {
            let Some(mut entry) = read_partition(write, partition)? else {
                return unknown(format!("no partition `{partition}`"));
            };
            if let Some(movement) = &entry.movement {
                return if movement.op == *op {
                    Ok(Step::Replayed(to_value(&entry)))
                } else {
                    conflict(format!(
                        "partition `{partition}` is already moving (`{}`)",
                        movement.op
                    ))
                };
            }
            if entry.status != PartitionStatus::Active {
                return conflict(format!(
                    "partition `{partition}` cannot move while {:?}",
                    entry.status
                ));
            }
            if entry.owner == *destination {
                return conflict(format!(
                    "partition `{partition}` is already in `{destination}`"
                ));
            }
            if !usable_group(write, destination)? {
                return unknown(format!("no active group `{destination}`"));
            }
            // Thawing after an abort takes the epoch after the transfer's.
            let Some(epoch) = entry.epoch.checked_add(1).filter(|epoch| *epoch < u64::MAX) else {
                return conflict(format!("partition `{partition}` ran out of epochs"));
            };
            entry.movement = Some(Movement {
                op: op.clone(),
                partition: partition.clone(),
                source: entry.owner.clone(),
                destination: destination.clone(),
                source_epoch: entry.epoch,
                epoch,
                phase: MovePhase::Copying,
                reason: None,
                started_ms: now_ms,
                updated_ms: now_ms,
            });
            entry.status = PartitionStatus::Moving;
            entry.op = op.clone();
            entry.updated_ms = now_ms;
            put(write, &key(PARTITION_PREFIX, partition), &entry)?;
            Ok(Step::Changed(to_value(&entry)))
        }
        CatalogChange::AdvanceMove {
            partition,
            op,
            phase,
        } => {
            let Some(mut entry) = read_partition(write, partition)? else {
                return unknown(format!("no partition `{partition}`"));
            };
            let Some(mut movement) = entry.movement.clone().filter(|movement| movement.op == *op)
            else {
                // A finished move answers from history.
                if let Some(done) = finished_move(write, partition, op)? {
                    return if done.phase == *phase {
                        Ok(Step::Replayed(to_value(&entry)))
                    } else {
                        conflict(format!("move `{op}` finished as {:?}", done.phase))
                    };
                }
                return unknown(format!("no move `{op}` of partition `{partition}`"));
            };
            if movement.phase == *phase {
                return Ok(Step::Replayed(to_value(&entry)));
            }
            if movement.phase.next() != Some(*phase) {
                return conflict(format!(
                    "move `{op}` cannot go from {:?} to {phase:?}",
                    movement.phase
                ));
            }
            movement.phase = *phase;
            movement.updated_ms = now_ms;
            entry.updated_ms = now_ms;
            match phase {
                // The cutover: the destination owns it from here on.
                MovePhase::Activating => {
                    entry.owner = movement.destination.clone();
                    entry.epoch = movement.epoch;
                }
                // The source thawed past the transfer's epoch.
                MovePhase::Abandoning => {
                    entry.owner = movement.source.clone();
                    entry.epoch = movement.epoch + 1;
                }
                _ => {}
            }
            if phase.finished() {
                entry.status = PartitionStatus::Active;
                entry.movement = None;
                record_finished(write, &movement)?;
            } else {
                entry.movement = Some(movement);
            }
            put(write, &key(PARTITION_PREFIX, partition), &entry)?;
            Ok(Step::Changed(to_value(&entry)))
        }
        CatalogChange::AbortMove {
            partition,
            op,
            reason,
        } => {
            let Some(mut entry) = read_partition(write, partition)? else {
                return unknown(format!("no partition `{partition}`"));
            };
            let Some(mut movement) = entry.movement.clone().filter(|movement| movement.op == *op)
            else {
                if let Some(done) = finished_move(write, partition, op)? {
                    return if done.phase == MovePhase::Aborted {
                        Ok(Step::Replayed(to_value(&entry)))
                    } else {
                        conflict(format!("move `{op}` completed"))
                    };
                }
                return unknown(format!("no move `{op}` of partition `{partition}`"));
            };
            if matches!(movement.phase, MovePhase::Aborting | MovePhase::Abandoning) {
                return Ok(Step::Replayed(to_value(&entry)));
            }
            if !movement.phase.abortable() {
                return conflict(format!(
                    "move `{op}` is past its cutover ({:?}) and only rolls forward",
                    movement.phase
                ));
            }
            movement.phase = MovePhase::Aborting;
            movement.reason = Some(reason.clone());
            movement.updated_ms = now_ms;
            entry.movement = Some(movement);
            entry.updated_ms = now_ms;
            put(write, &key(PARTITION_PREFIX, partition), &entry)?;
            Ok(Step::Changed(to_value(&entry)))
        }
    }
}

/// A finished move of `partition` under `op`, from history.
fn finished_move(write: &WriteTxn, partition: &str, op: &str) -> Result<Option<Movement>> {
    for (_, value) in write.scan_meta_prefix(CATALOG_DB, MOVE_PREFIX)? {
        let movement: Movement = decode(&value)?;
        if movement.partition == partition && movement.op == op {
            return Ok(Some(movement));
        }
    }
    Ok(None)
}

/// Keep a finished move in history, dropping the oldest past
/// [`MOVE_HISTORY`].
fn record_finished(write: &WriteTxn, movement: &Movement) -> Result<()> {
    let revision = write.meta_u64(CATALOG_DB, REVISION_ROW)? + 1;
    let mut row = MOVE_PREFIX.to_vec();
    row.extend_from_slice(format!("{revision:016x}").as_bytes());
    put(write, &row, movement)?;
    let rows = write.scan_meta_prefix(CATALOG_DB, MOVE_PREFIX)?;
    if rows.len() > MOVE_HISTORY {
        for (name, _) in &rows[..rows.len() - MOVE_HISTORY] {
            write.delete_meta(CATALOG_DB, name)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::placement::tests::{TestStore, test_store};

    struct Fixture {
        store: TestStore,
    }

    impl Fixture {
        fn new() -> Self {
            Fixture {
                store: test_store("catalog"),
            }
        }

        fn apply(&self, change: CatalogChange) -> HostResult<Value> {
            let write = self.store.store.begin_write().unwrap();
            let outcome = apply(&write, &change, 1_000).unwrap();
            write.commit().unwrap();
            outcome
        }

        fn catalog(&self) -> Catalog {
            Catalog::read(&self.store.store.begin_read().unwrap()).unwrap()
        }
    }

    fn group(name: &str) -> CatalogChange {
        CatalogChange::CreateGroup {
            group: name.into(),
            uid: 7,
            members: vec![3, 1, 2],
            op: format!("create-{name}"),
        }
    }

    #[test]
    fn resizing_is_balanced_durable_and_retry_safe() {
        let fixture = Fixture::new();
        for name in ["east", "west"] {
            fixture.apply(group(name)).unwrap();
            fixture
                .apply(CatalogChange::GroupInitializing {
                    group: name.into(),
                    uid: 7,
                    founder: 1,
                })
                .unwrap();
            fixture
                .apply(CatalogChange::GroupActive {
                    group: name.into(),
                    uid: 7,
                })
                .unwrap();
        }
        assert_eq!(fixture.catalog().leaders["east"].node, Some(1));
        assert_eq!(fixture.catalog().leaders["west"].node, Some(2));
        for name in ["a", "b", "c", "d", "e"] {
            fixture
                .apply(CatalogChange::CreatePartition {
                    partition: name.into(),
                    group: "east".into(),
                    op: name.into(),
                })
                .unwrap();
            fixture
                .apply(CatalogChange::PartitionReady {
                    partition: name.into(),
                    op: name.into(),
                })
                .unwrap();
        }
        let resize = CatalogChange::Resize {
            groups: vec!["west".into(), "east".into()],
            op: "balance".into(),
        };
        let plan = fixture.apply(resize.clone()).unwrap();
        let revision = fixture.catalog().revision;
        assert_eq!(fixture.apply(resize.clone()).unwrap(), plan);
        assert_eq!(fixture.catalog().revision, revision);
        let catalog = fixture.catalog();
        assert_eq!(
            catalog
                .partitions
                .values()
                .filter(|entry| entry.status == PartitionStatus::Moving)
                .count(),
            2
        );
        assert!(
            fixture
                .apply(CatalogChange::Resize {
                    groups: vec!["east".into()],
                    op: "balance".into()
                })
                .is_err()
        );
        assert!(
            fixture
                .apply(CatalogChange::Resize {
                    groups: vec!["east".into()],
                    op: "another".into()
                })
                .is_err()
        );
        assert_eq!(fixture.catalog().revision, revision);
        for entry in catalog
            .partitions
            .values()
            .filter(|entry| entry.movement.is_some())
        {
            let movement = entry.movement.as_ref().unwrap();
            for phase in [
                MovePhase::Freezing,
                MovePhase::Importing,
                MovePhase::Activating,
                MovePhase::Retiring,
                MovePhase::Complete,
            ] {
                fixture
                    .apply(CatalogChange::AdvanceMove {
                        partition: entry.name.clone(),
                        op: movement.op.clone(),
                        phase,
                    })
                    .unwrap();
            }
        }
        assert_eq!(
            fixture
                .catalog()
                .partitions
                .values()
                .filter(|entry| entry.owner == "west")
                .count(),
            2
        );
        assert_eq!(fixture.apply(resize).unwrap(), plan);
        fixture
            .apply(CatalogChange::PreferLeader {
                group: "east".into(),
                node: None,
                op: "off".into(),
            })
            .unwrap();
        assert_eq!(fixture.catalog().leaders["east"].node, None);
        assert!(
            fixture
                .apply(CatalogChange::PreferLeader {
                    group: "east".into(),
                    node: Some(99),
                    op: "bad".into()
                })
                .is_err()
        );
    }

    #[test]
    fn groups_found_activate_and_leave_only_when_empty() {
        let catalog = Fixture::new();
        catalog.apply(group("east")).unwrap();
        // A repeat replays without moving the revision.
        catalog.apply(group("east")).unwrap();
        assert_eq!(catalog.catalog().revision, 1);
        assert_eq!(catalog.catalog().groups["east"].members, vec![1, 2, 3]);
        let found = |founder| CatalogChange::GroupInitializing {
            group: "east".into(),
            uid: 7,
            founder,
        };
        assert_eq!(catalog.apply(found(9)).unwrap_err().code, CATALOG_CONFLICT);
        catalog.apply(found(1)).unwrap();
        catalog.apply(found(1)).unwrap();
        assert_eq!(catalog.apply(found(2)).unwrap_err().code, CATALOG_CONFLICT);
        let wrong_uid = CatalogChange::GroupActive {
            group: "east".into(),
            uid: 8,
        };
        assert_eq!(catalog.apply(wrong_uid).unwrap_err().code, CATALOG_UNKNOWN);
        catalog
            .apply(CatalogChange::GroupActive {
                group: "east".into(),
                uid: 7,
            })
            .unwrap();
        catalog
            .apply(CatalogChange::CreatePartition {
                partition: "acme".into(),
                group: "east".into(),
                op: "p".into(),
            })
            .unwrap();
        let remove = CatalogChange::RemoveGroup {
            group: "east".into(),
            op: "rm".into(),
        };
        assert_eq!(
            catalog.apply(remove.clone()).unwrap_err().code,
            CATALOG_CONFLICT
        );
        catalog
            .apply(CatalogChange::PartitionReady {
                partition: "acme".into(),
                op: "p".into(),
            })
            .unwrap();
        catalog
            .apply(CatalogChange::RetirePartition {
                partition: "acme".into(),
                op: "r".into(),
            })
            .unwrap();
        catalog
            .apply(CatalogChange::PartitionRetired {
                partition: "acme".into(),
                op: "r".into(),
            })
            .unwrap();
        catalog.apply(remove).unwrap();
        assert_eq!(
            catalog.catalog().groups["east"].status,
            GroupStatus::Removed
        );
        // Root exists from the start and never leaves.
        assert!(
            catalog
                .apply(CatalogChange::RemoveGroup {
                    group: ROOT_GROUP.into(),
                    op: "x".into()
                })
                .is_err()
        );
    }

    #[test]
    fn moves_cut_over_once_abort_only_before_and_keep_history() {
        let catalog = Fixture::new();
        for name in ["east", "west"] {
            catalog.apply(group(name)).unwrap();
            catalog
                .apply(CatalogChange::GroupActive {
                    group: name.into(),
                    uid: 7,
                })
                .unwrap();
        }
        catalog
            .apply(CatalogChange::CreatePartition {
                partition: "acme".into(),
                group: "east".into(),
                op: "p".into(),
            })
            .unwrap();
        let begin = |op: &str| CatalogChange::BeginMove {
            partition: "acme".into(),
            destination: "west".into(),
            op: op.into(),
        };
        // Not before it serves.
        assert_eq!(
            catalog.apply(begin("m1")).unwrap_err().code,
            CATALOG_CONFLICT
        );
        catalog
            .apply(CatalogChange::PartitionReady {
                partition: "acme".into(),
                op: "p".into(),
            })
            .unwrap();
        catalog.apply(begin("m1")).unwrap();
        catalog.apply(begin("m1")).unwrap();
        assert_eq!(
            catalog.apply(begin("m2")).unwrap_err().code,
            CATALOG_CONFLICT
        );
        let advance = |op: &str, phase| CatalogChange::AdvanceMove {
            partition: "acme".into(),
            op: op.into(),
            phase,
        };
        // Phases advance one at a time.
        assert_eq!(
            catalog
                .apply(advance("m1", MovePhase::Importing))
                .unwrap_err()
                .code,
            CATALOG_CONFLICT
        );
        catalog.apply(advance("m1", MovePhase::Freezing)).unwrap();
        catalog.apply(advance("m1", MovePhase::Importing)).unwrap();
        let entry = &catalog.catalog().partitions["acme"];
        assert_eq!((entry.owner.as_str(), entry.epoch), ("east", 1));
        catalog.apply(advance("m1", MovePhase::Activating)).unwrap();
        let entry = &catalog.catalog().partitions["acme"];
        assert_eq!((entry.owner.as_str(), entry.epoch), ("west", 2));
        // Past the cutover it only rolls forward.
        let abort = |op: &str| CatalogChange::AbortMove {
            partition: "acme".into(),
            op: op.into(),
            reason: "operator".into(),
        };
        assert_eq!(
            catalog.apply(abort("m1")).unwrap_err().code,
            CATALOG_CONFLICT
        );
        catalog.apply(advance("m1", MovePhase::Retiring)).unwrap();
        catalog.apply(advance("m1", MovePhase::Complete)).unwrap();
        catalog.apply(advance("m1", MovePhase::Complete)).unwrap();
        let state = catalog.catalog();
        assert_eq!(state.partitions["acme"].status, PartitionStatus::Active);
        assert_eq!(state.moves.len(), 1);
        // Moving back, then giving up: the source serves at the epoch
        // after the transfer's.
        let back = CatalogChange::BeginMove {
            partition: "acme".into(),
            destination: "east".into(),
            op: "m3".into(),
        };
        catalog.apply(back).unwrap();
        catalog.apply(advance("m3", MovePhase::Freezing)).unwrap();
        catalog.apply(abort("m3")).unwrap();
        catalog.apply(abort("m3")).unwrap();
        catalog.apply(advance("m3", MovePhase::Abandoning)).unwrap();
        let entry = &catalog.catalog().partitions["acme"];
        assert_eq!((entry.owner.as_str(), entry.epoch), ("west", 4));
        catalog.apply(advance("m3", MovePhase::Aborted)).unwrap();
        catalog.apply(abort("m3")).unwrap();
        let state = catalog.catalog();
        assert_eq!(state.moves.len(), 2);
        assert_eq!(state.moves[1].phase, MovePhase::Aborted);
        assert_eq!(state.partitions["acme"].movement, None);
    }
}

/// Group uids as 16 hex digits: answers travel as JSON values whose
/// numbers are doubles, which cannot hold every 64-bit uid.
pub mod uid {
    use serde::{Deserialize, Deserializer, Serializer, de::Error};

    /// Write a uid as 16 lowercase hex digits.
    pub fn serialize<S: Serializer>(uid: &u64, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&format!("{uid:016x}"))
    }

    /// Read a uid written by [`serialize`].
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
        let text = String::deserialize(deserializer)?;
        u64::from_str_radix(&text, 16).map_err(D::Error::custom)
    }
}
