//! Scoped typed handles and durable commands. Persist before execution; never re-key a retry.
use crate::typed::{json_value, value_json};
use crate::{
    FlowerClient, FlowerError, MutationOptions, MutationResult, QueryResult, RemoteError,
    RetryIdentity,
};
use flower_sdk::{BusinessError, FlowerType, MutationOp, QueryOp};
use std::{
    future::{Future, IntoFuture},
    marker::PhantomData,
    pin::Pin,
};

#[derive(Clone)]
pub struct DatabaseClient {
    client: FlowerClient,
    database: String,
    pub(crate) session: Option<crate::session::CausalSession>,
}
#[derive(Clone)]
pub struct ModuleClient {
    client: FlowerClient,
    database: String,
    module: String,
    pub(crate) session: Option<crate::session::CausalSession>,
}

impl FlowerClient {
    pub fn database(&self, name: impl Into<String>) -> DatabaseClient {
        DatabaseClient {
            client: self.clone(),
            database: name.into(),
            session: None,
        }
    }
}
impl DatabaseClient {
    pub fn module(&self, name: impl Into<String>) -> ModuleClient {
        ModuleClient {
            client: self.client.clone(),
            database: self.database.clone(),
            module: name.into(),
            session: self.session.clone(),
        }
    }
}

/// Serialized intent, including its original retry identity and precondition.
/// Request options and credentials are supplied on execution, not persisted.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(bound = "", deny_unknown_fields)]
pub struct PreparedCommand<T, E = flower_sdk::Failure> {
    version: u32,
    pub(crate) database: String,
    pub(crate) module: String,
    pub(crate) method: String,
    pub(crate) args: serde_json::Value,
    pub(crate) request_id: String,
    pub(crate) expected_revision: Option<u64>,
    pub(crate) contract: crate::CommandContract,
    #[serde(skip)]
    types: PhantomData<fn() -> (T, E)>,
}
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyCommand {
    version: u32,
    database: String,
    module: String,
    method: String,
    args: serde_json::Value,
    request_id: String,
    expected_revision: Option<u64>,
}
impl<T, E> PreparedCommand<T, E> {
    pub fn if_unchanged(self, token: &flower_protocol::ReadToken) -> Result<Self, FlowerError> {
        token.validate().map_err(FlowerError::Decode)?;
        if self.database != token.database || self.incarnation()? != token.incarnation.0 {
            return Err(FlowerError::Decode(
                "precondition token belongs to another database or history".into(),
            ));
        }
        Ok(self.expected_revision(token.revision))
    }
    pub fn request_id(&self) -> &str {
        &self.request_id
    }
    pub fn database(&self) -> &str {
        &self.database
    }
    pub fn contract(&self) -> &crate::CommandContract {
        &self.contract
    }
    pub fn expected_revision(mut self, revision: flower_protocol::Revision) -> Self {
        self.expected_revision = Some(revision.0);
        self
    }
    pub(crate) fn validate(&self) -> Result<(), FlowerError>
    where
        T: FlowerType,
        E: BusinessError,
    {
        if self.version != crate::contract::COMMAND_VERSION
            || self.database.is_empty()
            || self.module.is_empty()
            || self.method.is_empty()
            || !self.request_id.starts_with("f1:")
        {
            return Err(FlowerError::Decode(
                "invalid or unsupported prepared command".into(),
            ));
        }
        self.incarnation()?;
        self.contract.validate_result::<T, E>()?;
        Ok(())
    }
    pub(crate) fn incarnation(&self) -> Result<u64, FlowerError> {
        let parts: Vec<_> = self.request_id.split(':').collect();
        let valid_hex = |s: &str| {
            s.bytes()
                .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
        };
        if parts.len() != 4
            || parts[0] != "f1"
            || parts[1].len() != 16
            || !valid_hex(parts[1])
            || parts[2]
                .parse::<u64>()
                .ok()
                .is_none_or(|n| n.to_string() != parts[2])
            || parts[3].len() != 64
            || !valid_hex(parts[3])
        {
            return Err(FlowerError::Decode(
                "invalid prepared command identity".into(),
            ));
        }
        u64::from_str_radix(parts[1], 16)
            .ok()
            .filter(|n| *n != 0)
            .ok_or_else(|| FlowerError::Decode("invalid command history".into()))
    }
}

/// A command's counters have distinct types; the dynamic API retains its legacy integer fields.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct CommandResult<T> {
    pub value: T,
    pub revision: flower_protocol::Revision,
    pub position: flower_protocol::LogPosition,
    pub duplicate: bool,
    pub request_id: String,
    pub read_token: Option<flower_protocol::ReadToken>,
}
impl<T> From<MutationResult<T>> for CommandResult<T> {
    fn from(result: MutationResult<T>) -> Self {
        Self {
            value: result.value,
            revision: flower_protocol::Revision(result.revision),
            position: flower_protocol::LogPosition(result.position),
            duplicate: result.duplicate,
            request_id: result.request_id,
            read_token: result.read_token,
        }
    }
}

#[derive(Debug)]
pub struct CommandError<E> {
    pub request_id: String,
    pub execution: flower_protocol::Execution,
    pub source: RemoteError<E>,
}
impl<E> CommandError<E> {
    pub fn declared(&self) -> Option<&E> {
        self.source.declared()
    }
    pub fn problem(&self) -> Option<&flower_protocol::Problem> {
        self.source.source().problem()
    }
    pub fn execution(&self) -> flower_protocol::Execution {
        self.execution
    }
}
impl<E: std::fmt::Debug> std::fmt::Display for CommandError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "command {} ({:?}): {}",
            self.request_id, self.execution, self.source
        )
    }
}
impl<E: std::fmt::Debug + 'static> std::error::Error for CommandError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

impl ModuleClient {
    pub fn database_name(&self) -> &str {
        &self.database
    }
    pub fn module_name(&self) -> &str {
        &self.module
    }

    /// Explicitly migrate a format-one snapshot using its original contract.
    /// The caller must supply that contract: an old snapshot cannot prove its
    /// result/error schema. Identity, arguments and precondition are never changed.
    pub fn migrate_legacy<A: FlowerType, T: FlowerType, E: BusinessError>(
        &self,
        original: &MutationOp<A, T, E>,
        bytes: &[u8],
    ) -> Result<PreparedCommand<T, E>, FlowerError> {
        let legacy: LegacyCommand =
            serde_json::from_slice(bytes).map_err(|e| FlowerError::Decode(e.to_string()))?;
        if legacy.version != 1 {
            return Err(FlowerError::Unsupported(
                "only format-one snapshots can be explicitly migrated".into(),
            ));
        }
        let command: PreparedCommand<T, E> = PreparedCommand {
            version: crate::contract::COMMAND_VERSION,
            database: legacy.database,
            module: legacy.module,
            method: legacy.method,
            args: legacy.args,
            request_id: legacy.request_id,
            expected_revision: legacy.expected_revision,
            contract: crate::CommandContract::of::<A, T, E>()?,
            types: PhantomData,
        };
        self.restore(
            original,
            &serde_json::to_vec(&command).map_err(|e| FlowerError::Decode(e.to_string()))?,
        )
    }

    /// Restore against the current complete contract, retaining the original identity.
    pub fn restore<A: FlowerType, T: FlowerType, E: BusinessError>(
        &self,
        op: &MutationOp<A, T, E>,
        bytes: &[u8],
    ) -> Result<PreparedCommand<T, E>, FlowerError> {
        let command: PreparedCommand<T, E> =
            serde_json::from_slice(bytes).map_err(|e| FlowerError::Decode(e.to_string()))?;
        command.validate()?;
        if command.database != self.database
            || command.module != self.module
            || command.method != op.name
            || command.contract != crate::CommandContract::of::<A, T, E>()?
        {
            return Err(FlowerError::Unsupported("persisted command scope or argument contract changed; explicit migration is required".into()));
        }
        let args = json_value(&command.args)?;
        A::SCHEMA.parse(&args).map_err(|e| {
            FlowerError::Decode(format!(
                "stored arguments violate the contract at {}: {}",
                e.path, e.message
            ))
        })?;
        A::decode(args).map_err(|e| FlowerError::Decode(e.message))?;
        Ok(command)
    }
    pub fn query<'a, A: FlowerType, T: FlowerType, E: BusinessError>(
        &'a self,
        op: &QueryOp<A, T, E>,
        args: &'a A,
    ) -> QueryCall<'a, A, T, E> {
        QueryCall {
            scope: self,
            op: *op,
            args,
            options: crate::QueryOptions::new(),
        }
    }
    /// Discover a retry identity once, then return an intent ready for durable storage.
    pub async fn prepare<A: FlowerType, T: FlowerType, E: BusinessError>(
        &self,
        op: &MutationOp<A, T, E>,
        args: &A,
    ) -> Result<PreparedCommand<T, E>, FlowerError> {
        self.prepare_with_options(op, args, &crate::CommandOptions::new())
            .await
    }
    pub async fn prepare_with_options<A: FlowerType, T: FlowerType, E: BusinessError>(
        &self,
        op: &MutationOp<A, T, E>,
        args: &A,
        options: &crate::CommandOptions,
    ) -> Result<PreparedCommand<T, E>, FlowerError> {
        let identity = self
            .client
            .retry_identity(&self.database, &options.0)
            .await?;
        self.prepare_with_identity(op, args, &identity)
    }
    /// Offline preparation uses a previously stored identity, never a new identity on retry.
    pub fn prepare_with_identity<A: FlowerType, T: FlowerType, E: BusinessError>(
        &self,
        op: &MutationOp<A, T, E>,
        args: &A,
        identity: &RetryIdentity,
    ) -> Result<PreparedCommand<T, E>, FlowerError> {
        if !identity.initialized || identity.incarnation == 0 || identity.epoch < identity.min_epoch
        {
            return Err(FlowerError::Unsupported(
                "a live, initialized retry identity is required before preparing commands".into(),
            ));
        }
        let args = value_json(&args.encode().map_err(|e| FlowerError::Decode(e.message))?)?;
        let request_id = identity.request_id(uuid::Uuid::new_v4().as_bytes());
        let command = PreparedCommand {
            version: crate::contract::COMMAND_VERSION,
            database: self.database.clone(),
            module: self.module.clone(),
            method: op.name.into(),
            args,
            request_id,
            expected_revision: None,
            contract: crate::CommandContract::of::<A, T, E>()?,
            types: PhantomData,
        };
        command.validate()?;
        Ok(command)
    }
    pub async fn execute<T: FlowerType, E: BusinessError>(
        &self,
        command: &PreparedCommand<T, E>,
        options: &crate::CommandOptions,
    ) -> Result<CommandResult<T>, CommandError<E>> {
        let result = self.execute_raw(command, options).await?;
        let value = json_value(&result.value)
            .and_then(|value| T::decode(value).map_err(|e| FlowerError::Decode(e.message)))
            .map_err(|error| CommandError {
                request_id: command.request_id.clone(),
                execution: flower_protocol::Execution::Decided,
                source: crate::typed::declared(error),
            })?;
        Ok(CommandResult {
            value,
            revision: result.revision,
            position: result.position,
            duplicate: result.duplicate,
            request_id: result.request_id,
            read_token: result.read_token,
        })
    }
    pub(crate) async fn execute_raw<T: FlowerType, E: BusinessError>(
        &self,
        command: &PreparedCommand<T, E>,
        options: &crate::CommandOptions,
    ) -> Result<CommandResult<serde_json::Value>, CommandError<E>> {
        let mut execution = flower_protocol::Execution::NotStarted;
        let result = async {
            command.validate()?;
            let state = self.session.as_ref().map(|session| session.state(&self.database));
            let mut held = match state.as_ref() { Some(state) => Some(crate::session::acquire(state, &options.0).await?), None => None };
            if let Some(token) = held.as_ref().and_then(|held| held.as_ref())
                && token.incarnation.0 != command.incarnation()? {
                return Err(FlowerError::Unsupported("command and causal session have different histories; reconcile or prepare against the observed history".into()));
            }
            if command.database != self.database || command.module != self.module {
                return Err(FlowerError::Decode(
                    "command belongs to another scope".into(),
                ));
            }
            if options
                .0.cancel
                .as_ref()
                .is_some_and(crate::CancellationToken::is_cancelled)
            {
                return Err(FlowerError::Cancelled);
            }
            if options
                .0.deadline
                .is_some_and(|deadline| deadline <= std::time::Instant::now())
            {
                return Err(FlowerError::Timeout);
            }
            let options = MutationOptions {
                request_id: Some(command.request_id.clone()),
                expected_revision: command.expected_revision,
                base: options.0.clone(),
            };
            let result = self
                .client
                .mutate_value(
                    &command.database,
                    &command.module,
                    &command.method,
                    &command.args,
                    &options,
                )
                .await
                .inspect_err(|error| {
                    execution = error.execution();
                })?;
            execution = flower_protocol::Execution::Decided;
            if let Some(held) = held.as_mut() {
                crate::session::observe(held, &self.database, result.read_token.as_ref(), true)?;
            }
            Ok(result)
        }
        .await;
        result
            .map(CommandResult::from)
            .map_err(|error| CommandError {
                request_id: command.request_id.clone(),
                execution,
                source: crate::typed::declared(error),
            })
    }
    pub fn subscribe<A: FlowerType, T: FlowerType, E: BusinessError>(
        &self,
        op: &QueryOp<A, T, E>,
        args: &A,
        options: &crate::watch::SubscribeOptions,
    ) -> Result<crate::DeclaredSubscription<T, E>, RemoteError<E>> {
        if self.session.is_some() {
            return Err(crate::typed::declared(FlowerError::Unsupported("causal sessions support unary calls; use a separate subscription and observe its tokens explicitly".into())));
        }
        self.client
            .typed_subscribe_declared(&self.database, &self.module, op.name, args, options)
    }
    pub async fn watch<A: FlowerType, T: FlowerType, E: BusinessError>(
        &self,
        op: &QueryOp<A, T, E>,
        args: &A,
        options: &crate::watch::WatchOptions,
    ) -> Result<crate::DeclaredWatch<T, E>, RemoteError<E>> {
        if self.session.is_some() {
            return Err(crate::typed::declared(FlowerError::Unsupported("causal sessions support unary calls; use a separate watch and observe its tokens explicitly".into())));
        }
        self.client
            .typed_watch_declared(&self.database, &self.module, op.name, args, options)
            .await
    }
}

impl flower_sdk::remote::RemoteModule for ModuleClient {
    type Query<'a, A: FlowerType + Sync, T: FlowerType, E: BusinessError> = QueryCall<'a, A, T, E>;
    type Prepare<'a, A: FlowerType + Sync, T: FlowerType, E: BusinessError> =
        Pin<Box<dyn Future<Output = Result<PreparedCommand<T, E>, FlowerError>> + Send + 'a>>;

    fn query<'a, A: FlowerType + Sync, T: FlowerType, E: BusinessError>(
        &'a self,
        op: QueryOp<A, T, E>,
        args: &'a A,
    ) -> Self::Query<'a, A, T, E> {
        self.query(&op, args)
    }
    fn prepare<'a, A: FlowerType + Sync, T: FlowerType, E: BusinessError>(
        &'a self,
        op: MutationOp<A, T, E>,
        args: &'a A,
    ) -> Self::Prepare<'a, A, T, E> {
        Box::pin(async move { self.prepare(&op, args).await })
    }
}

pub struct QueryCall<'a, A, T, E> {
    scope: &'a ModuleClient,
    op: QueryOp<A, T, E>,
    args: &'a A,
    options: crate::QueryOptions,
}
/// An unsent mutation. Preparing discovers identity; enqueueing also persists it.
#[must_use]
pub struct CommandCall<'a, A, T, E> {
    scope: &'a ModuleClient,
    op: MutationOp<A, T, E>,
    args: &'a A,
    options: crate::CommandOptions,
}
impl ModuleClient {
    pub fn command<'a, A: FlowerType, T: FlowerType, E: BusinessError>(
        &'a self,
        op: &MutationOp<A, T, E>,
        args: &'a A,
    ) -> CommandCall<'a, A, T, E> {
        CommandCall {
            scope: self,
            op: *op,
            args,
            options: crate::CommandOptions::new(),
        }
    }
}
impl flower_sdk::remote::RemoteCommands for ModuleClient {
    type Command<'a, A: FlowerType + Sync, T: FlowerType, E: BusinessError> =
        CommandCall<'a, A, T, E>;
    fn command<'a, A: FlowerType + Sync, T: FlowerType, E: BusinessError>(
        &'a self,
        op: MutationOp<A, T, E>,
        args: &'a A,
    ) -> Self::Command<'a, A, T, E> {
        self.command(&op, args)
    }
}
impl<A: FlowerType, T: FlowerType, E: BusinessError> CommandCall<'_, A, T, E> {
    pub fn options(mut self, options: crate::CommandOptions) -> Self {
        self.options = options;
        self
    }
    pub async fn prepare(self) -> Result<PreparedCommand<T, E>, FlowerError> {
        self.scope
            .prepare_with_options(&self.op, self.args, &self.options)
            .await
    }
    pub async fn enqueue(
        self,
        outbox: &crate::CommandOutbox,
    ) -> Result<crate::CommandHandle<T, E>, crate::OutboxError<E>> {
        let command = self
            .scope
            .prepare_with_options(&self.op, self.args, &self.options)
            .await
            .map_err(|error| {
                crate::OutboxError::Command(Box::new(CommandError {
                    request_id: String::new(),
                    execution: flower_protocol::Execution::NotStarted,
                    source: crate::typed::declared(error),
                }))
            })?;
        outbox.enqueue(self.scope, &self.op, &command).await
    }
}
impl<A: FlowerType, T: FlowerType, E: BusinessError> QueryCall<'_, A, T, E> {
    fn stream_options(&self) -> Result<crate::watch::WatchOptions, RemoteError<E>> {
        let options = &self.options.0;
        if options.at_least.is_some()
            || options.max_staleness.is_some()
            || options.deadline.is_some()
            || options.retry.is_some()
        {
            return Err(crate::typed::declared(FlowerError::Unsupported(
                "query consistency, deadline and retry options cannot be applied to a stream; use subscription options".into()
            )));
        }
        Ok(crate::watch::WatchOptions {
            credentials: options.credentials.clone(),
            timeout: options.timeout,
            cancel: options.cancel.clone(),
        })
    }
    pub async fn watch(self) -> Result<crate::DeclaredWatch<T, E>, RemoteError<E>> {
        let options = self.stream_options()?;
        self.scope.watch(&self.op, self.args, &options).await
    }
    pub fn subscribe(self) -> Result<crate::DeclaredSubscription<T, E>, RemoteError<E>> {
        let options = crate::watch::SubscribeOptions {
            watch: self.stream_options()?,
            ..Default::default()
        };
        self.scope.subscribe(&self.op, self.args, &options)
    }
}
impl<A, T, E> QueryCall<'_, A, T, E> {
    pub fn options(mut self, options: crate::QueryOptions) -> Self {
        self.options = options;
        self
    }
    pub fn at_least(mut self, token: flower_protocol::ReadToken) -> Self {
        self.options.0.at_least = Some(token);
        self
    }
}
impl<'a, A: FlowerType + Sync, T: FlowerType, E: BusinessError> IntoFuture
    for QueryCall<'a, A, T, E>
{
    type Output = Result<QueryResult<T>, RemoteError<E>>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;
    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let state = self
                .scope
                .session
                .as_ref()
                .map(|session| session.state(&self.scope.database));
            let mut held = match state.as_ref() {
                Some(state) => Some(
                    crate::session::acquire(state, &self.options.0)
                        .await
                        .map_err(crate::typed::declared)?,
                ),
                None => None,
            };
            let options = match held.as_ref() {
                Some(held) => crate::session::bound_options(held, &self.options)
                    .map_err(crate::typed::declared)?,
                None => self.options.0,
            };
            let result = self
                .scope
                .client
                .typed_query_declared(
                    &self.scope.database,
                    &self.scope.module,
                    self.op.name,
                    self.args,
                    &options,
                )
                .await?;
            if let Some(held) = held.as_mut() {
                crate::session::observe(
                    held,
                    &self.scope.database,
                    result.read_token.as_ref(),
                    options.at_least.is_some(),
                )
                .map_err(crate::typed::declared)?;
            }
            Ok(result)
        })
    }
}
