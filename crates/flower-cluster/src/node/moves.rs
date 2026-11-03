//! A move's source side: the leader exports a database's rows a page at
//! a time, from a base read it pins while the database still serves,
//! then the exact difference to its frozen state, or the whole frozen
//! image when the base is gone (see [`crate::moves`]).
//!
//! A pinned read keeps the group file from reusing the pages it sees,
//! so no pin outlives its use: a thaw or retirement lets go of its
//! move's, and maintenance ([`Node::release_stale_exports`]) lets go of
//! every pin on a member that no longer leads and of any unused for
//! [`PIN_TTL`], whether or not another export comes.

use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};

use flower_engine::movement::{self, Cursor, Digest};
use flower_engine::placement::{self, Placement};
use flower_engine::storage::{ReadTxn, meta};
use serde::{Deserialize, Serialize};

use super::{Node, NodeError};

/// Failure code: the base a difference needs is no longer pinned here
/// (another leader, or a restart): import the whole frozen image.
pub const EXPORT_LOST: &str = "export_lost";
/// How long an unused pin is kept.
const PIN_TTL: Duration = Duration::from_secs(15 * 60);

/// What an export reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExportKind {
    /// The base: a read pinned while the database serves.
    Base,
    /// The difference from the pinned base to the frozen state.
    Delta,
    /// The whole frozen state.
    Full,
}

/// One exported page on the wire.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExportPage {
    /// The rows, encoded, hex.
    pub rows: String,
    /// Where the next page begins; `None` after the last.
    pub next: Option<Cursor>,
    /// The revision the export reaches: the base's, or the frozen one.
    pub revision: u64,
}

/// A digest on the wire.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExportDigest {
    /// The digest, 32 hex digits.
    pub digest: String,
    /// The revision it is the digest at.
    pub revision: u64,
}

/// One pinned base.
struct Pin {
    read: ReadTxn,
    revision: u64,
    used: Instant,
}

/// Pinned bases by database and move.
#[derive(Default)]
pub(crate) struct Pins {
    pins: Mutex<HashMap<(Vec<u8>, String), Pin>>,
}

impl Pins {
    fn drop_stale(&self) {
        self.sweep(true);
    }

    /// Let go of every pin unused for [`PIN_TTL`], and of every pin when
    /// not `leading`: a new leader exports from reads of its own, and a
    /// difference whose base went sends the move to a whole image.
    fn sweep(&self, leading: bool) {
        let mut pins = self.pins.lock().unwrap_or_else(PoisonError::into_inner);
        if leading {
            pins.retain(|_, pin| pin.used.elapsed() < PIN_TTL);
        } else {
            pins.clear();
        }
    }

    /// Let go of a move's base.
    pub(crate) fn release(&self, db: &[u8], op: &str) {
        self.pins
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&(db.to_vec(), op.to_string()));
    }
}

impl Node {
    /// A database's placement here, linearizable.
    pub async fn inspect(&self, db: &[u8]) -> Result<Option<Placement>, NodeError> {
        self.read_barrier().await?;
        self.placement(db)
    }

    fn fresh_read(&self) -> Result<ReadTxn, NodeError> {
        self.reader.store().begin_read().map_err(NodeError::engine)
    }

    fn frozen_read(&self, db: &[u8], op: &str) -> Result<(ReadTxn, u64), NodeError> {
        let read = self.fresh_read()?;
        match placement::read(&read, db).map_err(NodeError::engine)? {
            Some(Placement::Frozen {
                op: ref by,
                revision,
                ..
            }) if by == op => Ok((read, revision)),
            other => Err(NodeError::coded(
                placement::PLACEMENT_CONFLICT,
                format!(
                    "database `{}` is not frozen for move `{op}`: {other:?}",
                    String::from_utf8_lossy(db)
                ),
            )),
        }
    }

    /// Export one page of `db` for move `op`, as its group's leader.
    pub async fn export(
        &self,
        db: &[u8],
        op: &str,
        kind: ExportKind,
        cursor: Option<Cursor>,
        max_bytes: usize,
    ) -> Result<ExportPage, NodeError> {
        self.forward_unless_leading()?;
        self.read_barrier().await?;
        self.exports.drop_stale();
        let key = (db.to_vec(), op.to_string());
        let (db, op) = (db.to_vec(), op.to_string());
        let page = match kind {
            ExportKind::Base => {
                let mut pins = self
                    .exports
                    .pins
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                if !pins.contains_key(&key) {
                    if cursor.is_some() {
                        return Err(NodeError::coded(
                            EXPORT_LOST,
                            "the base this export continues is gone",
                        ));
                    }
                    let read = self.fresh_read()?;
                    match placement::read(&read, &db).map_err(NodeError::engine)? {
                        Some(Placement::Active { .. }) | None => {}
                        other => {
                            return Err(NodeError::coded(
                                placement::PLACEMENT_CONFLICT,
                                format!(
                                    "a base is only taken while the database serves: {other:?}"
                                ),
                            ));
                        }
                    }
                    let revision = read
                        .meta_u64(&db, meta::REVISION)
                        .map_err(NodeError::engine)?;
                    pins.insert(
                        key.clone(),
                        Pin {
                            read,
                            revision,
                            used: Instant::now(),
                        },
                    );
                }
                let pin = pins.get_mut(&key).expect("pinned above");
                pin.used = Instant::now();
                let page = movement::export_page(&pin.read, &db, cursor.as_ref(), max_bytes)
                    .map_err(NodeError::engine)?;
                (page, pin.revision)
            }
            ExportKind::Delta => {
                let (current, revision) = self.frozen_read(&db, &op)?;
                let mut pins = self
                    .exports
                    .pins
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                let Some(pin) = pins.get_mut(&key) else {
                    return Err(NodeError::coded(
                        EXPORT_LOST,
                        "no base is pinned for this move here",
                    ));
                };
                pin.used = Instant::now();
                let page =
                    movement::diff_page(&pin.read, &current, &db, cursor.as_ref(), max_bytes)
                        .map_err(NodeError::engine)?;
                (page, revision)
            }
            ExportKind::Full => {
                let (current, revision) = self.frozen_read(&db, &op)?;
                let page = movement::export_page(&current, &db, cursor.as_ref(), max_bytes)
                    .map_err(NodeError::engine)?;
                (page, revision)
            }
        };
        let (page, revision) = page;
        Ok(ExportPage {
            rows: flower_engine::hex::encode(&movement::encode_changes(&page.changes)),
            next: page.next,
            revision,
        })
    }

    /// The digest an import of `kind` must reach: the pinned base's, or
    /// the frozen state's.
    pub async fn export_digest(
        &self,
        db: &[u8],
        op: &str,
        kind: ExportKind,
    ) -> Result<ExportDigest, NodeError> {
        self.forward_unless_leading()?;
        self.read_barrier().await?;
        let (digest, revision) = match kind {
            ExportKind::Base => {
                let pins = self
                    .exports
                    .pins
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                let Some(pin) = pins.get(&(db.to_vec(), op.to_string())) else {
                    return Err(NodeError::coded(
                        EXPORT_LOST,
                        "no base is pinned for this move here",
                    ));
                };
                (
                    movement::digest(&pin.read, db).map_err(NodeError::engine)?,
                    pin.revision,
                )
            }
            ExportKind::Delta | ExportKind::Full => {
                let (current, revision) = self.frozen_read(db, op)?;
                (
                    movement::digest(&current, db).map_err(NodeError::engine)?,
                    revision,
                )
            }
        };
        Ok(ExportDigest {
            digest: Digest::hex(digest),
            revision,
        })
    }

    /// Let go of a move's pinned base: it thawed or retired.
    pub(crate) fn release_export(&self, db: &[u8], op: &str) {
        self.exports.release(db, op);
    }

    /// Let go of the base reads this member pins but no longer needs:
    /// every one once it stops leading its group, and any unused for
    /// [`PIN_TTL`]. Maintenance runs it every interval, so neither a move
    /// waiting before its freeze, a lost leadership nor a move abandoned
    /// without a thaw here keeps a read pinned for good.
    pub fn release_stale_exports(&self) {
        self.exports.sweep(self.is_leader());
    }

    /// How many base reads this member pins for moves.
    pub fn pinned_exports(&self) -> usize {
        self.exports
            .pins
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }
}
