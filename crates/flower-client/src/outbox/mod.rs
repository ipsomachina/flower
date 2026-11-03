//! Durable command submission and explicit recovery. Storage always precedes delivery.
pub mod store;
use crate::{
    CommandError, CommandOptions, CommandResult, Execution, FlowerError, ModuleClient,
    PreparedCommand,
};
use flower_sdk::{BusinessError, FlowerType, MutationOp};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, PoisonError},
};
pub use store::{CommandStore, FileCommandStore, MemoryCommandStore, StoreError};

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandStatus {
    Pending,
    Sending,
    Succeeded,
    Rejected,
    Unknown,
    Unresolved,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct StoredFailure {
    pub execution: Execution,
    pub source: FlowerError,
}

/// A durable snapshot, not an event-history cursor. The original command is immutable.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandRecord {
    pub version: u32,
    pub generation: u64,
    pub attempts: u64,
    pub command: serde_json::Value,
    pub status: CommandStatus,
    pub outcome: Option<CommandResult<serde_json::Value>>,
    pub failure: Option<StoredFailure>,
    pub evidence: Option<String>,
}
impl CommandRecord {
    pub fn request_id(&self) -> Result<&str, StoreError> {
        self.command["request_id"]
            .as_str()
            .filter(|id| !id.is_empty())
            .ok_or_else(|| StoreError::message("stored command has no identity"))
    }
    pub fn validate(&self) -> Result<(), StoreError> {
        self.request_id()?;
        if self.version != 1
            || (self.status != CommandStatus::Pending && self.attempts == 0)
            || (self.status == CommandStatus::Succeeded) != self.outcome.is_some()
            || (matches!(
                self.status,
                CommandStatus::Rejected | CommandStatus::Unknown | CommandStatus::Unresolved
            ) && self.failure.is_none())
        {
            return Err(StoreError::message("invalid stored command lifecycle"));
        }
        if let Some(result) = &self.outcome
            && result.request_id != self.request_id()?
        {
            return Err(StoreError::message(
                "stored result identity differs from command",
            ));
        }
        Ok(())
    }
}

#[derive(Debug)]
pub enum OutboxError<E> {
    Store(StoreError),
    Command(Box<CommandError<E>>),
    /// Another delivery owns this generation. Explicit recovery can replay the identical command.
    Busy,
    /// A concurrent transition won; inspect the persisted record before acting.
    Conflict,
    /// History or retention no longer establishes the original outcome. Reconcile explicitly.
    Unresolved,
}
impl<E: std::fmt::Debug> std::fmt::Display for OutboxError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Store(error) => error.fmt(f),
            Self::Command(error) => error.fmt(f),
            Self::Busy => {
                f.write_str("command delivery is in flight; inspect or explicitly recover it")
            }
            Self::Conflict => f.write_str("command state changed concurrently; reload it"),
            Self::Unresolved => f.write_str("command outcome requires explicit reconciliation"),
        }
    }
}
impl<E: std::fmt::Debug + 'static> std::error::Error for OutboxError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Store(error) => Some(error),
            Self::Command(error) => Some(error),
            _ => None,
        }
    }
}
impl<E> From<StoreError> for OutboxError<E> {
    fn from(error: StoreError) -> Self {
        Self::Store(error)
    }
}

#[derive(Clone)]
pub struct CommandOutbox {
    inner: Arc<Inner>,
}
struct Inner {
    store: Arc<dyn CommandStore>,
    updates: Mutex<BTreeMap<String, tokio::sync::watch::Sender<CommandRecord>>>,
}
impl CommandOutbox {
    pub fn new(store: Arc<dyn CommandStore>) -> Self {
        Self {
            inner: Arc::new(Inner {
                store,
                updates: Mutex::default(),
            }),
        }
    }
    pub async fn records(&self) -> Result<Vec<CommandRecord>, StoreError> {
        self.inner.store.list().await
    }

    /// Check the entire current contract and persist before returning a handle. Does not send.
    pub async fn enqueue<A: FlowerType, T: FlowerType, E: BusinessError>(
        &self,
        scope: &ModuleClient,
        op: &MutationOp<A, T, E>,
        command: &PreparedCommand<T, E>,
    ) -> Result<CommandHandle<T, E>, OutboxError<E>> {
        let bytes = serde_json::to_vec(command).map_err(StoreError::from)?;
        let command = scope
            .restore(op, &bytes)
            .map_err(|error| local_error(command.request_id(), Execution::NotStarted, error))?;
        let record = CommandRecord {
            version: 1,
            generation: 0,
            attempts: 0,
            command: serde_json::from_slice(&bytes).map_err(StoreError::from)?,
            status: CommandStatus::Pending,
            outcome: None,
            failure: None,
            evidence: None,
        };
        let stored = self.inner.store.insert(record).await?;
        let handle = CommandHandle {
            outbox: self.clone(),
            scope: scope.clone(),
            command,
        };
        handle.check_record(&stored)?;
        self.publish(stored);
        Ok(handle)
    }
    /// Restore under today's credentials and complete contract. Never mint a new identity.
    pub async fn resume<A: FlowerType, T: FlowerType, E: BusinessError>(
        &self,
        scope: &ModuleClient,
        op: &MutationOp<A, T, E>,
        id: &str,
    ) -> Result<CommandHandle<T, E>, OutboxError<E>> {
        let record = self
            .inner
            .store
            .get(id)
            .await?
            .ok_or_else(|| StoreError::message("command not found"))?;
        let bytes = serde_json::to_vec(&record.command).map_err(StoreError::from)?;
        let command = scope
            .restore(op, &bytes)
            .map_err(|error| local_error(id, Execution::NotStarted, error))?;
        if command.request_id() != id {
            return Err(StoreError::message("store returned another command identity").into());
        }
        let handle = CommandHandle {
            outbox: self.clone(),
            scope: scope.clone(),
            command,
        };
        handle.check_record(&record)?;
        self.publish(record);
        Ok(handle)
    }
    fn publish(&self, record: CommandRecord) {
        let Ok(id) = record.request_id() else {
            return;
        };
        let mut updates = self
            .inner
            .updates
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        updates.retain(|_, sender| sender.receiver_count() > 0);
        if let Some(sender) = updates.get(id) {
            sender.send_if_modified(|previous| {
                if record.generation <= previous.generation {
                    return false;
                }
                *previous = record.clone();
                true
            });
        }
    }
}
fn local_error<E: BusinessError>(
    id: &str,
    execution: Execution,
    source: FlowerError,
) -> OutboxError<E> {
    OutboxError::Command(Box::new(CommandError {
        request_id: id.to_owned(),
        execution,
        source: crate::typed::declared(source),
    }))
}

pub struct CommandHandle<T, E = flower_sdk::Failure> {
    outbox: CommandOutbox,
    scope: ModuleClient,
    command: PreparedCommand<T, E>,
}
impl<T: FlowerType, E: BusinessError> CommandHandle<T, E> {
    pub fn request_id(&self) -> &str {
        self.command.request_id()
    }
    fn check_record(&self, record: &CommandRecord) -> Result<(), StoreError> {
        record.validate()?;
        if record.command != serde_json::to_value(&self.command)? {
            return Err(StoreError::message("store changed the immutable command"));
        }
        Ok(())
    }
    pub async fn status(&self) -> Result<CommandRecord, StoreError> {
        let record = self
            .outbox
            .inner
            .store
            .get(self.request_id())
            .await?
            .ok_or_else(|| StoreError::message("command disappeared from store"))?;
        self.check_record(&record)?;
        self.outbox.publish(record.clone());
        Ok(record)
    }
    /// Updates from this outbox instance and its clones, coalesced by generation.
    /// Call `status` to refresh changes made by another process or backend instance.
    pub async fn updates(&self) -> Result<tokio::sync::watch::Receiver<CommandRecord>, StoreError> {
        let record = self.status().await?;
        let receiver = self
            .outbox
            .inner
            .updates
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(self.request_id().to_owned())
            .or_insert_with(|| tokio::sync::watch::channel(record).0)
            .subscribe();
        self.status().await?; // Close the subscribe/load race; publish never regresses a generation.
        Ok(receiver)
    }
    async fn transition(
        &self,
        expected: u64,
        mut record: CommandRecord,
    ) -> Result<(), OutboxError<E>> {
        record.generation = expected
            .checked_add(1)
            .ok_or_else(|| StoreError::message("command generation overflow"))?;
        if !self
            .outbox
            .inner
            .store
            .compare_exchange(expected, record.clone())
            .await?
        {
            return Err(OutboxError::Conflict);
        }
        self.outbox.publish(record);
        Ok(())
    }
    pub async fn execute(
        &self,
        options: &CommandOptions,
    ) -> Result<CommandResult<T>, OutboxError<E>> {
        self.deliver(options, false).await
    }
    /// Explicitly replay Pending, Unknown or an abandoned Sending state under the identical ID.
    /// This does not decide whether a previous sender is still active; CAS protects the record.
    pub async fn retry_uncertain(
        &self,
        options: &CommandOptions,
    ) -> Result<CommandResult<T>, OutboxError<E>> {
        self.deliver(options, true).await
    }
    async fn deliver(
        &self,
        options: &CommandOptions,
        recover: bool,
    ) -> Result<CommandResult<T>, OutboxError<E>> {
        let mut record = self.status().await?;
        match record.status {
            CommandStatus::Succeeded | CommandStatus::Rejected => {
                return self.decode_record(&record);
            }
            CommandStatus::Unresolved => return Err(OutboxError::Unresolved),
            CommandStatus::Sending if !recover => return Err(OutboxError::Busy),
            CommandStatus::Unknown if !recover => return self.decode_record(&record),
            _ => {}
        }
        let generation = record.generation;
        let prior_uncertainty = matches!(
            record.status,
            CommandStatus::Sending | CommandStatus::Unknown
        ) || record
            .failure
            .as_ref()
            .is_some_and(|failure| failure.execution == Execution::Unknown);
        record.attempts = record
            .attempts
            .checked_add(1)
            .ok_or_else(|| StoreError::message("command attempt counter overflow"))?;
        record.status = CommandStatus::Sending;
        self.transition(generation, record).await?; // Durable before the first byte can be sent.
        let mut sent = self.status().await?;
        if sent.generation != generation + 1 {
            return Err(OutboxError::Conflict);
        }
        let result = self.scope.execute_raw(&self.command, options).await;
        match result {
            Ok(result) => {
                sent.status = CommandStatus::Succeeded;
                sent.outcome = Some(result);
                sent.failure = None;
            }
            Err(error) => {
                let source = error.source.source().clone();
                sent.status = if source
                    .code()
                    .is_some_and(flower_protocol::unresolved_outcome)
                {
                    CommandStatus::Unresolved
                } else {
                    match error.execution {
                        Execution::NotStarted if prior_uncertainty => CommandStatus::Unknown,
                        Execution::NotStarted => CommandStatus::Pending,
                        Execution::Unknown => CommandStatus::Unknown,
                        Execution::Decided if crate::typed::call_failure(&source).is_some() => {
                            CommandStatus::Rejected
                        }
                        Execution::Decided => CommandStatus::Unresolved,
                    }
                };
                sent.failure = Some(StoredFailure {
                    execution: if prior_uncertainty && error.execution == Execution::NotStarted {
                        Execution::Unknown
                    } else {
                        error.execution
                    },
                    source,
                });
            }
        }
        self.transition(generation + 1, sent).await?;
        self.decode_record(&self.status().await?)
    }
    fn decode_record(&self, record: &CommandRecord) -> Result<CommandResult<T>, OutboxError<E>> {
        if let Some(result) = &record.outcome {
            let value = crate::typed::json_value(&result.value)
                .and_then(|value| T::decode(value).map_err(|e| FlowerError::Decode(e.message)))
                .map_err(|error| local_error(self.request_id(), Execution::Decided, error))?;
            return Ok(CommandResult {
                value,
                revision: result.revision,
                position: result.position,
                duplicate: result.duplicate,
                request_id: result.request_id.clone(),
                read_token: result.read_token.clone(),
            });
        }
        if let Some(failure) = &record.failure {
            return Err(local_error(
                self.request_id(),
                failure.execution,
                failure.source.clone(),
            ));
        }
        Err(OutboxError::Busy)
    }
    /// Persist externally established evidence for an unknown outcome. Does not send a mutation.
    pub async fn resolve(
        &self,
        resolution: Resolution,
        evidence: impl Into<String>,
    ) -> Result<(), OutboxError<E>> {
        let mut record = self.status().await?;
        if !matches!(
            record.status,
            CommandStatus::Unknown | CommandStatus::Unresolved | CommandStatus::Sending
        ) {
            return Err(OutboxError::Conflict);
        }
        let evidence = evidence.into();
        if evidence.trim().is_empty() {
            return Err(StoreError::message("reconciliation requires nonempty evidence").into());
        }
        match resolution {
            Resolution::Succeeded(result) => {
                if result.request_id != self.request_id() {
                    return Err(StoreError::message(
                        "reconciliation result belongs to another command",
                    )
                    .into());
                }
                record.status = CommandStatus::Succeeded;
                record.outcome = Some(*result);
                record.failure = None;
            }
            Resolution::Rejected(failure) => {
                if failure.execution != Execution::Decided {
                    return Err(StoreError::message(
                        "rejection evidence must establish a decided outcome",
                    )
                    .into());
                }
                record.status = CommandStatus::Rejected;
                record.outcome = None;
                record.failure = Some(*failure);
            }
        }
        record.evidence = Some(evidence);
        self.transition(record.generation, record).await
    }
}
pub enum Resolution {
    Succeeded(Box<CommandResult<serde_json::Value>>),
    Rejected(Box<StoredFailure>),
}
