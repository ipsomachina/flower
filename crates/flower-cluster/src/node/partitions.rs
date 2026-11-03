//! A member's side of the partition catalog and of placement.
//!
//! Catalog changes go through the root group's writer; placement
//! changes through the writer of the group that holds the database.
//! Both are batch items ordered with the group's other writes, so a
//! freeze or a retirement lands at one log position on every member
//! (see [`flower_engine::catalog`], [`flower_engine::placement`]).

use flower_engine::batch::BatchItem;
use flower_engine::catalog::{CATALOG_DB, Catalog, CatalogChange};
use flower_engine::placement::{self, Placement, PlacementCommand};
use std::sync::Arc;

use flower_sdk::{Failure, Value};

use super::{Node, NodeError};
use crate::admission::{Class, REQUEST_OVERHEAD};
use crate::keys::{KEY_CATALOG, KEYS_LOCKED};
use crate::peer::{BoxFuture, ControlAnswer, ControlHandler, ControlRequest, MemberError};
use crate::types::{ResponseOutcome, WriteResponse};
use crate::writer::Prepared;

/// Failure code: a catalog change sent to a group other than the root.
pub const NOT_ROOT: &str = "not_root";

impl Node {
    /// Apply a catalog change through this root group's writer: its
    /// answer, or its deterministic refusal as the inner `Err`. Forwards
    /// from a follower; refused on any group but the root.
    pub async fn change_catalog(
        &self,
        change: CatalogChange,
    ) -> Result<Result<Value, Failure>, NodeError> {
        if !self.group.is_root() {
            return Err(NodeError::coded(
                NOT_ROOT,
                format!("group {} holds no catalog", self.group),
            ));
        }
        self.check_storage()?;
        self.forward_unless_leading()?;
        let queued = self
            .writers
            .acquire(Class::Writer, CATALOG_DB, REQUEST_OVERHEAD)
            .await?;
        let written = self
            .writer
            .submit(Some(queued), move |_| {
                Ok(Prepared::Propose(
                    BatchItem::Catalog {
                        change: change.clone(),
                    },
                    (),
                ))
            })
            .await?;
        item_answer(written.response, "catalog change")
    }

    /// Apply a placement change to `db` through this group's writer: the
    /// placement it left, or its deterministic refusal as the inner
    /// `Err`. Forwards from a follower.
    ///
    /// A move's activation checks the database's managed keys first, on the
    /// leader that proposes it: a leader elected since the move's last check
    /// (see [`crate::moves`]) that cannot unwrap them refuses with
    /// [`KEYS_LOCKED`], and the move waits activating (past its cutover, it
    /// can no longer give up) until a leader that can proposes it.
    pub async fn place(
        &self,
        db: &[u8],
        command: PlacementCommand,
    ) -> Result<Result<Value, Failure>, NodeError> {
        self.check_storage()?;
        self.forward_unless_leading()?;
        if matches!(command, PlacementCommand::Activate { .. })
            && let Err(locked) = self.check_keys(db).await?
        {
            return Ok(Err(locked));
        }
        let queued = self
            .writers
            .acquire(Class::Writer, db, REQUEST_OVERHEAD)
            .await?;
        let db = db.to_vec();
        let (released, db_released) = (command.clone(), db.clone());
        let written = self
            .writer
            .submit(Some(queued), move |_| {
                Ok(Prepared::Propose(
                    BatchItem::Placement {
                        db: db.clone(),
                        command: command.clone(),
                    },
                    (),
                ))
            })
            .await?;
        let answer = item_answer(written.response, "placement change")?;
        // A move that thawed or retired its source needs no base any more.
        if answer.is_ok()
            && let PlacementCommand::Thaw { op, .. } | PlacementCommand::Retire { op, .. } =
                &released
        {
            self.release_export(&db_released, op);
        }
        Ok(answer)
    }

    /// The catalog as this member applied it, without proving it fresh:
    /// what routing reads. Empty on any group but the root.
    pub fn applied_catalog(&self) -> Result<Catalog, NodeError> {
        let read = self
            .reader
            .store()
            .begin_read()
            .map_err(NodeError::engine)?;
        Catalog::read(&read).map_err(NodeError::engine)
    }

    /// The catalog, linearizable: what the root's leader committed when
    /// asked, applied here.
    pub async fn catalog(&self) -> Result<Catalog, NodeError> {
        self.read_barrier().await?;
        self.applied_catalog()
    }

    /// Whether this group's leader can serve database `db`'s managed keys as
    /// the group holds them now, a move's import included: every unrevoked
    /// version of its stored catalog unwraps and prepares under this member's
    /// wrapping keys. A refusal is [`KEYS_LOCKED`] with why. A move asks its
    /// destination before it freezes the source and again before its cutover
    /// (see [`crate::moves`]), and the destination's activation asks again
    /// where it is proposed (see [`Node::place`]), so a database never lands
    /// where its keys stay locked.
    pub async fn check_keys(&self, db: &[u8]) -> Result<Result<Value, Failure>, NodeError> {
        self.forward_unless_leading()?;
        self.read_barrier().await?;
        let read = self
            .reader
            .store()
            .begin_read()
            .map_err(NodeError::engine)?;
        let catalog = read.meta(db, KEY_CATALOG).map_err(NodeError::engine)?;
        drop(read);
        let keys = self.key_catalog();
        let checked = tokio::task::spawn_blocking(move || keys.validate(catalog.as_deref()))
            .await
            .map_err(|error| NodeError::Fatal(error.to_string()))?;
        Ok(match checked {
            Ok(()) => Ok(Value::Null),
            Err(error) => Err(Failure::new(
                KEYS_LOCKED,
                format!(
                    "group {} cannot serve the keys of `{}`: {error:#}",
                    self.group,
                    String::from_utf8_lossy(db)
                ),
            )),
        })
    }

    /// A database's placement in this group as this member applied it:
    /// `None` when unplaced.
    pub fn placement(&self, db: &[u8]) -> Result<Option<Placement>, NodeError> {
        let read = self
            .reader
            .store()
            .begin_read()
            .map_err(NodeError::engine)?;
        placement::read(&read, db).map_err(NodeError::engine)
    }
}

impl ControlHandler for Node {
    fn control(
        self: Arc<Self>,
        request: ControlRequest,
    ) -> BoxFuture<Result<ControlAnswer, MemberError>> {
        let outcome = async move {
            let answer = match request {
                ControlRequest::Catalog(change) => self.change_catalog(change).await?,
                ControlRequest::Place { db, command } => self.place(&db, command).await?,
                ControlRequest::Txn(request) => return crate::txn::answer(&self, request).await,
                ControlRequest::Export {
                    db,
                    op,
                    kind,
                    cursor,
                    max_bytes,
                } => {
                    let page = self.export(&db, &op, kind, cursor, max_bytes).await?;
                    return json_answer(&page);
                }
                ControlRequest::ExportDigest { db, op, kind } => {
                    return json_answer(&self.export_digest(&db, &op, kind).await?);
                }
                ControlRequest::Inspect { db } => {
                    self.forward_unless_leading()?;
                    return json_answer(&self.inspect(&db).await?);
                }
                ControlRequest::Keys { db } => self.check_keys(&db).await?,
            };
            Ok::<_, NodeError>(match answer {
                Ok(value) => ControlAnswer::Applied(
                    String::from_utf8(
                        flower_engine::json::render(&value).map_err(NodeError::engine)?,
                    )
                    .map_err(|error| NodeError::coded("render", error))?,
                ),
                Err(failure) => ControlAnswer::Refused {
                    code: failure.code,
                    message: failure.message,
                    details: None,
                },
            })
        };
        Box::pin(async move { outcome.await.map_err(MemberError::from) })
    }
}

/// A control answer as the caller's outcome: the applied answer, or the
/// refusal as the inner `Err`.
pub fn control_outcome(answer: ControlAnswer) -> Result<Result<Value, Failure>, NodeError> {
    match answer {
        ControlAnswer::Applied(text) => flower_engine::json::parse_value(text.as_bytes())
            .map(Ok)
            .map_err(NodeError::engine),
        ControlAnswer::Refused { code, message, .. } => Ok(Err(Failure::new(code, message))),
    }
}

/// A batch item's answer: its value, or its refusal as the inner `Err`.
pub(crate) fn item_answer(
    response: Option<WriteResponse>,
    what: &str,
) -> Result<Result<Value, Failure>, NodeError> {
    let Some(response) = response else {
        return Err(NodeError::Fatal(format!("a {what} proposed nothing")));
    };
    match response.outcome {
        ResponseOutcome::Value(bytes) => flower_engine::json::parse_value(&bytes)
            .map(Ok)
            .map_err(NodeError::engine),
        ResponseOutcome::Error(message) => {
            let (code, detail) = message.split_once(": ").unwrap_or(("rejected", &message));
            Ok(Err(Failure::new(code, detail)))
        }
        ResponseOutcome::Failure(_) => Ok(Err(Failure::new(
            "rejected",
            format!("the {what} was refused"),
        ))),
    }
}

/// A control answer carrying `value` as JSON text.
fn json_answer<T: serde::Serialize>(value: &T) -> Result<ControlAnswer, NodeError> {
    serde_json::to_string(value)
        .map(ControlAnswer::Applied)
        .map_err(|error| NodeError::coded("render", error))
}
