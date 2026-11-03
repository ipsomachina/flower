//! The partition driver's steps: creating, retiring and moving named
//! partitions, one idempotent step at a time.
//!
//! The driver runs on the root group's leader ([`Host::run_driver`]).
//! For each partition with unfinished work it reads the catalog entry,
//! asks the group the step belongs to for it (a control request naming
//! the partition, its epoch and the operation), and records the next
//! phase in the catalog only once the step applied. Every step answers
//! as applied when repeated, so a driver that crashed, lost leadership
//! or ran twice leaves nothing a later driver cannot finish.
//!
//! A database's managed keys travel in its catalog row with the rest of its
//! rows (see [`crate::keys`]), sealed under the wrapping key. A move holds
//! until its destination can serve them: once the base is copied, before the
//! source freezes, the destination's leader must unwrap every unrevoked
//! version it holds, so a locked destination keeps the move copying while the
//! source serves on; and once the rest is sealed, before the cutover, again,
//! for keys the source changed meanwhile, so the move waits importing. Either
//! wait ends past the freeze limit: the move gives itself up, its source
//! serving again (while copying, the source's leader keeps the base read
//! pinned, which keeps its group file from reusing pages). A destination
//! mounted with the right key passes on the driver's next try. The activation
//! checks once more where it is proposed (see [`crate::node::Node::place`]):
//! a destination leader elected since without the key refuses it, and the
//! move, past its cutover, waits activating until one with the key leads.

use std::sync::{Arc, PoisonError};
use std::time::{Duration, Instant};

use flower_engine::catalog::{CatalogChange, MovePhase, Movement, PartitionEntry, PartitionStatus};
use flower_engine::placement::{Placement, PlacementCommand, Stage};
use flower_sdk::{Failure, Value};

use crate::exec::now_ms;
use crate::group::GroupId;
use crate::host::Host;
use crate::keys::KEYS_LOCKED;
use crate::node::NodeError;
use crate::node::{EXPORT_LOST, ExportDigest, ExportKind, ExportPage};
use crate::partitions::unfinished;
use crate::peer::{ControlAnswer, ControlRequest};

/// Most partitions with work the driver steps at once.
pub const CONCURRENT_STEPS: usize = 4;

/// Take the next step of every partition with unfinished work, at most
/// [`CONCURRENT_STEPS`] at once, by the catalog the root applied here.
pub async fn drive_once(host: &Arc<Host>) -> Result<(), NodeError> {
    let catalog = host.applied_catalog()?;
    let pending: Vec<PartitionEntry> = catalog
        .partitions
        .values()
        .filter(|entry| unfinished(entry))
        .cloned()
        .collect();
    // Forget holds of moves that finished, or that an operator gave up.
    host.key_holds
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .retain(|op, _| {
            pending
                .iter()
                .any(|entry| entry.movement.as_ref().is_some_and(|m| m.op == *op))
        });
    for chunk in pending.chunks(CONCURRENT_STEPS) {
        let mut steps = tokio::task::JoinSet::new();
        for entry in chunk.iter().cloned() {
            let host = host.clone();
            steps.spawn(async move {
                if let Err(error) = step(&host, &entry).await {
                    tracing::debug!(partition = %entry.name, %error, "a partition step will be retried");
                }
            });
        }
        while steps.join_next().await.is_some() {}
    }
    Ok(())
}

/// A step's refusal as an error, so the driver retries it.
fn refused(failure: Failure) -> NodeError {
    NodeError::failure(failure)
}

/// One step of one partition.
async fn step(host: &Arc<Host>, entry: &PartitionEntry) -> Result<(), NodeError> {
    let catalog = host.applied_catalog()?;
    let Some(owner) = host.group_id(&catalog, &entry.owner) else {
        return Err(NodeError::coded(
            "catalog",
            format!("partition `{}` names no listed owner", entry.name),
        ));
    };
    let db = entry.name.as_bytes().to_vec();
    match entry.status {
        PartitionStatus::Creating => {
            host.control(
                &owner,
                ControlRequest::Place {
                    db,
                    command: PlacementCommand::Create {
                        epoch: entry.epoch,
                        op: entry.op.clone(),
                    },
                },
            )
            .await?
            .map_err(refused)?;
            host.change_catalog(CatalogChange::PartitionReady {
                partition: entry.name.clone(),
                op: entry.op.clone(),
            })
            .await?
            .map_err(refused)?;
        }
        PartitionStatus::Retiring => {
            host.control(
                &owner,
                ControlRequest::Place {
                    db,
                    command: PlacementCommand::Discard {
                        epoch: entry.epoch,
                        op: entry.op.clone(),
                    },
                },
            )
            .await?
            .map_err(refused)?;
            host.change_catalog(CatalogChange::PartitionRetired {
                partition: entry.name.clone(),
                op: entry.op.clone(),
            })
            .await?
            .map_err(refused)?;
        }
        PartitionStatus::Moving => {
            if let Some(movement) = &entry.movement {
                step_move(host, entry, movement).await?;
            }
        }
        PartitionStatus::Active | PartitionStatus::Retired => {}
    }
    Ok(())
}

/// The bytes one exported page holds at most.
pub const PAGE_BYTES: usize = 1024 * 1024;

/// One move step: the next thing its phase needs, then the catalog
/// advances (see the module docs and [`MovePhase`]).
async fn step_move(
    host: &Arc<Host>,
    entry: &PartitionEntry,
    movement: &Movement,
) -> Result<(), NodeError> {
    let catalog = host.cached_catalog();
    let group = |name: &str| {
        host.group_id(&catalog, name).ok_or_else(|| {
            NodeError::coded(
                "catalog",
                format!("move `{}` names no group `{name}`", movement.op),
            )
        })
    };
    let (source, destination) = (group(&movement.source)?, group(&movement.destination)?);
    let db = entry.name.as_bytes().to_vec();
    let op = movement.op.clone();
    // A move that keeps its partition unserved too long gives up while it
    // still may: the source serves again.
    if matches!(movement.phase, MovePhase::Freezing | MovePhase::Importing)
        && now_ms().saturating_sub(movement.updated_ms) > host.freeze_max().as_millis() as u64
    {
        let reason = format!(
            "the partition stayed unserved past {} ms",
            host.freeze_max().as_millis()
        );
        return give_up(host, entry, &op, reason).await;
    }
    let next = match movement.phase {
        MovePhase::Copying => {
            let state = inspect(host, &destination, &db).await?;
            if !matches!(&state, Some(Placement::Copied { op: by, .. }) if *by == op) {
                begin(host, &destination, &db, movement, Stage::Base, false).await?;
                pump(host, &source, &destination, &db, movement, ExportKind::Base).await?;
                seal(host, &source, &destination, &db, movement, ExportKind::Base).await?;
            }
            // Never freeze the source for a destination that could not
            // serve its keys, nor wait for one longer than the freeze
            // limit.
            if let Some((held, why)) = keys_ready(host, &destination, &db, movement).await? {
                if held > host.freeze_max() {
                    let reason = format!(
                        "the destination could not serve the partition's keys past {} ms: {why}",
                        host.freeze_max().as_millis()
                    );
                    return give_up(host, entry, &op, reason).await;
                }
                return Err(NodeError::coded(KEYS_LOCKED, why));
            }
            MovePhase::Freezing
        }
        MovePhase::Freezing => {
            place(
                host,
                &source,
                &db,
                PlacementCommand::Freeze {
                    epoch: movement.source_epoch,
                    op: op.clone(),
                },
            )
            .await?;
            MovePhase::Importing
        }
        MovePhase::Importing => {
            let state = inspect(host, &destination, &db).await?;
            match &state {
                Some(Placement::Staged { op: by, .. }) if *by == op => {}
                // On the copied base: the difference, unless the source
                // lost the base, then the whole frozen image.
                Some(Placement::Copied { op: by, .. }) if *by == op => {
                    begin(host, &destination, &db, movement, Stage::Rest, true).await?;
                    let delta = pump(
                        host,
                        &source,
                        &destination,
                        &db,
                        movement,
                        ExportKind::Delta,
                    )
                    .await;
                    let kind = match delta {
                        Ok(()) => ExportKind::Delta,
                        Err(NodeError::Failure { code, .. }) if code == EXPORT_LOST => {
                            begin(host, &destination, &db, movement, Stage::Rest, false).await?;
                            pump(host, &source, &destination, &db, movement, ExportKind::Full)
                                .await?;
                            ExportKind::Full
                        }
                        Err(error) => return Err(error),
                    };
                    seal(host, &source, &destination, &db, movement, kind).await?;
                }
                // An interrupted or refused rest starts over whole.
                _ => {
                    begin(host, &destination, &db, movement, Stage::Rest, false).await?;
                    pump(host, &source, &destination, &db, movement, ExportKind::Full).await?;
                    seal(host, &source, &destination, &db, movement, ExportKind::Full).await?;
                }
            }
            // Nor cut over to one, with the keys as they froze (the freeze
            // limit above bounds this wait).
            if let Some((_, why)) = keys_ready(host, &destination, &db, movement).await? {
                return Err(NodeError::coded(KEYS_LOCKED, why));
            }
            MovePhase::Activating
        }
        MovePhase::Activating => {
            let activated = place(
                host,
                &destination,
                &db,
                PlacementCommand::Activate {
                    epoch: movement.epoch,
                    op: op.clone(),
                },
            )
            .await;
            // Its leader checked the keys again, and could not serve them.
            if let Err(NodeError::Failure { code, message }) = &activated
                && code == KEYS_LOCKED
            {
                held_for_keys(host, movement, message);
            }
            activated?;
            MovePhase::Retiring
        }
        MovePhase::Retiring => {
            place(
                host,
                &source,
                &db,
                PlacementCommand::Retire {
                    epoch: movement.source_epoch,
                    op: op.clone(),
                    to: movement.destination.clone(),
                },
            )
            .await?;
            MovePhase::Complete
        }
        // The source first, so it serves again even while the destination
        // cannot be reached.
        MovePhase::Aborting => {
            place(
                host,
                &source,
                &db,
                PlacementCommand::Thaw {
                    epoch: movement.source_epoch,
                    op: op.clone(),
                    next: movement.epoch + 1,
                },
            )
            .await?;
            MovePhase::Abandoning
        }
        MovePhase::Abandoning => {
            place(
                host,
                &destination,
                &db,
                PlacementCommand::Abandon {
                    epoch: movement.epoch,
                    op: op.clone(),
                    source: movement.source.clone(),
                },
            )
            .await?;
            MovePhase::Aborted
        }
        MovePhase::Complete | MovePhase::Aborted => return Ok(()),
    };
    host.change_catalog(CatalogChange::AdvanceMove {
        partition: entry.name.clone(),
        op,
        phase: next,
    })
    .await?
    .map_err(refused)?;
    Ok(())
}

/// Give a move up while it still may, for `reason`: its source serves
/// again.
async fn give_up(
    host: &Arc<Host>,
    entry: &PartitionEntry,
    op: &str,
    reason: String,
) -> Result<(), NodeError> {
    host.change_catalog(CatalogChange::AbortMove {
        partition: entry.name.clone(),
        op: op.to_string(),
        reason,
    })
    .await?
    .map_err(refused)?;
    Ok(())
}

/// Apply a placement change at a group's leader, refusals as errors.
async fn place(
    host: &Arc<Host>,
    group: &GroupId,
    db: &[u8],
    command: PlacementCommand,
) -> Result<Value, NodeError> {
    host.control(
        group,
        ControlRequest::Place {
            db: db.to_vec(),
            command,
        },
    )
    .await?
    .map_err(refused)
}

/// A database's placement at a group's leader.
async fn inspect(
    host: &Arc<Host>,
    group: &GroupId,
    db: &[u8],
) -> Result<Option<Placement>, NodeError> {
    let text = applied(host, group, ControlRequest::Inspect { db: db.to_vec() }).await?;
    serde_json::from_str(&text).map_err(|error| NodeError::coded("move", error))
}

/// A control request answered with JSON text.
async fn applied(
    host: &Arc<Host>,
    group: &GroupId,
    request: ControlRequest,
) -> Result<String, NodeError> {
    match host
        .send(group, crate::peer::MemberRequest::Control(request), false)
        .await?
    {
        crate::peer::MemberResponse::Control(ControlAnswer::Applied(text)) => Ok(text),
        crate::peer::MemberResponse::Control(ControlAnswer::Refused { code, message, .. }) => {
            Err(NodeError::Failure { code, message })
        }
        _ => Err(NodeError::coded(
            "move",
            "a group's leader answered with another kind",
        )),
    }
}

/// Start a stage of the import at the destination.
async fn begin(
    host: &Arc<Host>,
    destination: &GroupId,
    db: &[u8],
    movement: &Movement,
    stage: Stage,
    keep: bool,
) -> Result<(), NodeError> {
    place(
        host,
        destination,
        db,
        PlacementCommand::ImportBegin {
            epoch: movement.epoch,
            op: movement.op.clone(),
            source: movement.source.clone(),
            stage,
            keep,
        },
    )
    .await
    .map(|_| ())
}

/// Carry every page of an export from the source's leader into the
/// destination, in order, through the destination's log.
async fn pump(
    host: &Arc<Host>,
    source: &GroupId,
    destination: &GroupId,
    db: &[u8],
    movement: &Movement,
    kind: ExportKind,
) -> Result<(), NodeError> {
    let mut cursor = None;
    let mut page = 0u64;
    loop {
        let text = applied(
            host,
            source,
            ControlRequest::Export {
                db: db.to_vec(),
                op: movement.op.clone(),
                kind,
                cursor: cursor.take(),
                max_bytes: PAGE_BYTES,
            },
        )
        .await?;
        let exported: ExportPage =
            serde_json::from_str(&text).map_err(|error| NodeError::coded("move", error))?;
        place(
            host,
            destination,
            db,
            PlacementCommand::ImportRows {
                epoch: movement.epoch,
                op: movement.op.clone(),
                page,
                rows: exported.rows,
            },
        )
        .await?;
        page += 1;
        match exported.next {
            Some(next) => cursor = Some(next),
            None => return Ok(()),
        }
    }
}

/// How often a move held for its destination's keys says so.
const KEYS_WARNING: Duration = Duration::from_secs(10);

/// A move held for its destination's managed keys, as this host's driver saw
/// it: a driver on another host counts again.
pub(crate) struct KeyHold {
    /// Since when, with no pass in between.
    since: Instant,
    /// When the driver last said why.
    warned: Instant,
}

/// Ask the destination's leader whether it can serve the managed keys
/// of what it holds (see the module docs): `None` when it can, else how
/// long this driver has held the move for them and why, so the step
/// fails and the driver tries again on its next tick. Other refusals
/// are the step's error.
async fn keys_ready(
    host: &Arc<Host>,
    destination: &GroupId,
    db: &[u8],
    movement: &Movement,
) -> Result<Option<(Duration, String)>, NodeError> {
    let answer = host
        .control(destination, ControlRequest::Keys { db: db.to_vec() })
        .await?;
    match answer {
        Ok(_) => {
            host.key_holds
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(&movement.op);
            Ok(None)
        }
        Err(failure) if failure.code == KEYS_LOCKED => Ok(Some((
            held_for_keys(host, movement, &failure.message),
            failure.message,
        ))),
        Err(failure) => Err(refused(failure)),
    }
}

/// Note that `movement` waits for its destination's keys, saying why at
/// most every [`KEYS_WARNING`] (an operator mounts the destination's
/// wrapping key, or aborts): how long it has waited.
fn held_for_keys(host: &Host, movement: &Movement, why: &str) -> Duration {
    let mut holds = host
        .key_holds
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    let now = Instant::now();
    let mut first = false;
    let hold = holds.entry(movement.op.clone()).or_insert_with(|| {
        first = true;
        KeyHold {
            since: now,
            warned: now,
        }
    });
    if first || hold.warned.elapsed() >= KEYS_WARNING {
        hold.warned = now;
        tracing::warn!(
            partition = %movement.partition,
            op = %movement.op,
            phase = ?movement.phase,
            destination = %movement.destination,
            "the move waits for its destination's wrapping key: {why}"
        );
    }
    hold.since.elapsed()
}

/// Seal a stage with the source's digest.
async fn seal(
    host: &Arc<Host>,
    source: &GroupId,
    destination: &GroupId,
    db: &[u8],
    movement: &Movement,
    kind: ExportKind,
) -> Result<(), NodeError> {
    let text = applied(
        host,
        source,
        ControlRequest::ExportDigest {
            db: db.to_vec(),
            op: movement.op.clone(),
            kind,
        },
    )
    .await?;
    let digest: ExportDigest =
        serde_json::from_str(&text).map_err(|error| NodeError::coded("move", error))?;
    place(
        host,
        destination,
        db,
        PlacementCommand::ImportSeal {
            epoch: movement.epoch,
            op: movement.op.clone(),
            revision: digest.revision,
            digest: digest.digest,
        },
    )
    .await
    .map(|_| ())
}

impl Host {
    /// Begin moving partition `partition` to group `destination` under
    /// operation `op`; the driver on the root's leader carries it out.
    pub async fn move_partition(
        &self,
        partition: &str,
        destination: &str,
        op: &str,
    ) -> Result<Result<Value, Failure>, NodeError> {
        self.change_catalog(CatalogChange::BeginMove {
            partition: partition.to_string(),
            destination: destination.to_string(),
            op: op.to_string(),
        })
        .await
    }

    /// Give up a move before its cutover: its source serves again.
    pub async fn abort_move(
        &self,
        partition: &str,
        op: &str,
        reason: &str,
    ) -> Result<Result<Value, Failure>, NodeError> {
        self.change_catalog(CatalogChange::AbortMove {
            partition: partition.to_string(),
            op: op.to_string(),
            reason: reason.to_string(),
        })
        .await
    }
}
