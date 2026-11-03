//! A member's side of two-phase commit across groups: the commands its
//! writer orders, preparing a participant's steps, and the records it
//! answers about (see [`crate::txn`] for the protocol).

use std::time::{Duration, Instant};

use flower_engine::batch::BatchItem;
use flower_engine::engine::{MultiStep, MutationOptions, PrepareOutcome};
use flower_engine::txn::{
    self, CoordinatorRecord, Decision, PlacedDb, PreparedRecord, TXN_CONFLICT, TXN_PREPARED,
    TxnCommand,
};
use flower_sdk::{Failure, Value};

use super::partitions::item_answer;
use super::{Node, NodeError};
use crate::admission::{Class, REQUEST_OVERHEAD, input_bytes};
use crate::txn::{ParticipantState, PrepareRequest};
use crate::writer::Prepared;

/// How long a prepare waits for another transaction's lock on one of
/// its databases before refusing: waits are bounded, so no cycle of
/// waiting transactions holds anyone for longer.
pub const PREPARE_PATIENCE: Duration = Duration::from_secs(1);
/// The longest a write refused for a lock waits for it to go before
/// refusing for good (its client deadline cuts it shorter).
pub const LOCK_WAIT: Duration = Duration::from_secs(2);

impl Node {
    /// Apply a transaction command through this group's writer: its
    /// answer, or its deterministic refusal as the inner `Err`.
    pub async fn txn_command(
        &self,
        command: TxnCommand,
    ) -> Result<Result<Value, Failure>, NodeError> {
        self.check_storage()?;
        self.forward_unless_leading()?;
        let queued = self
            .writers
            .acquire(Class::Writer, txn::TXN_DB, REQUEST_OVERHEAD)
            .await?;
        let written = self
            .writer
            .submit(Some(queued), move |_| {
                Ok(Prepared::Propose(
                    BatchItem::Txn {
                        command: command.clone(),
                    },
                    (),
                ))
            })
            .await?;
        item_answer(written.response, "transaction command")
    }

    /// Prepare this group's steps of a cross-group transaction: evaluate
    /// them through the writer on its staged state, exactly as a
    /// multi-database transaction's steps prepare, and replicate the
    /// prepared patch with the locks it takes. A database another
    /// transaction holds is waited for, at most [`PREPARE_PATIENCE`],
    /// then refused. A step's failure is the inner `Err`, and nothing
    /// was prepared.
    pub async fn prepare_txn(
        &self,
        request: PrepareRequest,
    ) -> Result<Result<Value, Failure>, NodeError> {
        self.check_storage()?;
        self.forward_unless_leading()?;
        let steps: Vec<MultiStep> = request
            .steps
            .iter()
            .map(|step| step.step())
            .collect::<Result<_, _>>()?;
        let options = MutationOptions {
            call: request.options.call_options()?,
            retry: None,
            idempotency_key: None,
            expected_revision: None,
        };
        let indexes: Vec<usize> = request.steps.iter().map(|step| step.index).collect();
        let started = Instant::now();
        loop {
            let mut outcome = self
                .prepare_txn_once(&request, &steps, &options, &indexes)
                .await?;
            if let Err(failure) = &mut outcome
                && let Some(Value::Object(details)) = &mut failure.details
                && let Some(Value::Object(context)) = details.get_mut("transaction")
                && let Some(Value::Number(index)) = context.get("step")
                && let Some(global) = indexes.get(*index as usize)
            {
                context.insert("step".into(), Value::Number(*global as f64));
            }
            match &outcome {
                Err(failure)
                    if (failure.code == TXN_CONFLICT || failure.code == TXN_PREPARED)
                        && started.elapsed() < PREPARE_PATIENCE =>
                {
                    self.wait_for_locks(
                        &request.dbs,
                        PREPARE_PATIENCE.saturating_sub(started.elapsed()),
                    )
                    .await;
                }
                _ => return Ok(outcome),
            }
        }
    }

    async fn prepare_txn_once(
        &self,
        request: &PrepareRequest,
        steps: &[MultiStep],
        options: &MutationOptions,
        indexes: &[usize],
    ) -> Result<Result<Value, Failure>, NodeError> {
        let bytes = steps.iter().map(|step| input_bytes(&step.args)).sum();
        let lane = steps
            .first()
            .map(|step| step.db.clone())
            .unwrap_or_default();
        let queued = self.writers.acquire(Class::Writer, &lane, bytes).await?;
        let (steps, options, indexes) = (steps.to_vec(), options.clone(), indexes.to_vec());
        let (id, coordinator, coordinator_uid, dbs) = (
            request.id.clone(),
            request.coordinator.clone(),
            request.coordinator_uid,
            request.dbs.clone(),
        );
        let written = self
            .writer
            .submit(Some(queued), move |engine| {
                for placed in &dbs {
                    if let Some(holder) = txn::read_lock_holder(
                        &engine.snapshot().map_err(NodeError::engine)?,
                        placed.db.as_bytes(),
                    )
                    .map_err(NodeError::engine)?
                    {
                        return Ok(Prepared::Answer(Err(flower_sdk::fail(
                            TXN_CONFLICT,
                            format!("database `{}` is locked by transaction {holder}", placed.db),
                        ))));
                    }
                }
                let (prepared, reads) =
                    match engine.prepare_transaction_multi_locked(&steps, &options) {
                        Ok(prepared) => prepared,
                        Err(failure) => return Ok(Prepared::Answer(Err(failure))),
                    };
                // Participants prepare without a retry identity (the
                // coordinator's record answers retries), so no receipt
                // replays here; answer one as admission decided it.
                let prepared = match prepared {
                    PrepareOutcome::Patch(prepared) => prepared,
                    PrepareOutcome::Done(done) | PrepareOutcome::Replayed(done, _) => {
                        return Ok(Prepared::Answer(done.map(|_| Value::Null)));
                    }
                };
                let values = match &prepared.outcome {
                    Ok(Value::Array(values)) => values.clone(),
                    Ok(other) => vec![other.clone()],
                    Err(failure) => return Ok(Prepared::Answer(Err(failure.clone()))),
                };
                let mut results = Vec::with_capacity(values.len());
                for (index, value) in indexes.iter().zip(&values) {
                    let text = flower_engine::json::render(value).map_err(NodeError::engine)?;
                    results.push((
                        *index,
                        String::from_utf8(text)
                            .map_err(|error| NodeError::coded("render", error))?,
                    ));
                }
                let record = PreparedRecord {
                    id: id.clone(),
                    coordinator: coordinator.clone(),
                    coordinator_uid,
                    dbs: dbs.clone(),
                    patch: flower_engine::hex::encode(&prepared.patch),
                    results,
                    prepared_ms: 0,
                    reads: Some(Box::new(reads)),
                };
                Ok(Prepared::Propose(
                    BatchItem::Txn {
                        command: TxnCommand::Prepare { record },
                    },
                    Ok(Value::Null),
                ))
            })
            .await?;
        match written.note {
            Err(failure) => Ok(Err(failure)),
            Ok(_) => item_answer(written.response, "transaction prepare"),
        }
    }

    /// Wait until none of `dbs` is locked, or `at_most` passes: each
    /// finish that applies here sends its databases' commit notices.
    pub(crate) async fn wait_for_locks(&self, dbs: &[PlacedDb], at_most: Duration) {
        let mut notices = self.subscribe_commits();
        let deadline = tokio::time::Instant::now() + at_most;
        loop {
            let locked = {
                let Ok(read) = self.reader.store().begin_read() else {
                    return;
                };
                dbs.iter()
                    .any(|placed| txn::is_locked(&read, placed.db.as_bytes()).unwrap_or(false))
            };
            if !locked {
                return;
            }
            tokio::select! {
                _ = notices.recv() => {}
                _ = tokio::time::sleep_until(deadline) => return,
            }
        }
    }

    /// Wait until `db` is not locked by a prepared transaction, or
    /// [`LOCK_WAIT`] (cut by the client deadline) passes.
    pub(crate) async fn wait_for_lock(&self, db: &[u8]) {
        let at_most = crate::deadline::remaining().map_or(LOCK_WAIT, |left| left.min(LOCK_WAIT));
        let placed = [PlacedDb {
            db: String::from_utf8_lossy(db).into_owned(),
            epoch: 0,
            steps: Vec::new(),
        }];
        self.wait_for_locks(&placed, at_most).await;
    }

    /// Finish this group's part of a transaction with `decision`.
    pub async fn finish_txn(
        &self,
        id: &str,
        decision: Decision,
    ) -> Result<Result<Value, Failure>, NodeError> {
        self.txn_command(TxnCommand::Finish {
            id: id.to_string(),
            decision,
        })
        .await
    }

    /// Where this group stands on a transaction it takes part in,
    /// linearizable.
    pub async fn participant_state(&self, id: &str) -> Result<ParticipantState, NodeError> {
        self.read_barrier().await?;
        let read = self
            .reader
            .store()
            .begin_read()
            .map_err(NodeError::engine)?;
        if let Some(record) = txn::prepared(&read, id).map_err(NodeError::engine)? {
            return Ok(ParticipantState::Prepared {
                results: record.results,
            });
        }
        if let Some(done) = txn::tombstone(&read, id).map_err(NodeError::engine)? {
            return Ok(ParticipantState::Finished {
                decision: done.decision,
            });
        }
        Ok(ParticipantState::Unknown)
    }

    /// This group's coordinator record of a transaction, linearizable.
    pub async fn coordinator_record(
        &self,
        id: &str,
    ) -> Result<Option<CoordinatorRecord>, NodeError> {
        self.read_barrier().await?;
        let read = self
            .reader
            .store()
            .begin_read()
            .map_err(NodeError::engine)?;
        txn::coordinator(&read, id).map_err(NodeError::engine)
    }

    /// The coordinator record an idempotency key began here, if kept.
    pub async fn coordinator_by_key(
        &self,
        key: &str,
    ) -> Result<Option<CoordinatorRecord>, NodeError> {
        self.read_barrier().await?;
        let read = self
            .reader
            .store()
            .begin_read()
            .map_err(NodeError::engine)?;
        txn::by_key(&read, key).map_err(NodeError::engine)
    }

    /// Every coordinator and prepared record this member applied.
    pub fn txn_records(&self) -> Result<(Vec<CoordinatorRecord>, Vec<PreparedRecord>), NodeError> {
        let read = self
            .reader
            .store()
            .begin_read()
            .map_err(NodeError::engine)?;
        Ok((
            txn::coordinators(&read).map_err(NodeError::engine)?,
            txn::prepared_records(&read).map_err(NodeError::engine)?,
        ))
    }
}

/// Whether a write was refused because a prepared transaction holds its
/// database: at preparation, or at apply.
pub(crate) fn locked_out(outcome: &Result<crate::types::WriteResponse, NodeError>) -> bool {
    use crate::types::ResponseOutcome;
    match outcome {
        Err(NodeError::Failure { code, .. }) => code == TXN_PREPARED,
        Ok(response) => match &response.outcome {
            ResponseOutcome::Error(message) => message.starts_with(TXN_PREPARED),
            ResponseOutcome::Failure(bytes) => flower_engine::json::parse_value(bytes)
                .ok()
                .and_then(|value| flower_sdk::abi::decode_envelope(value).err())
                .is_some_and(|failure| failure.code == TXN_PREPARED),
            ResponseOutcome::Value(_) => false,
        },
        Err(_) => false,
    }
}
