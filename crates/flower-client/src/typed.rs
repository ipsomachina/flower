//! Typed calls, watches and subscriptions over `FlowerType` codecs,
//! declared failures, and remote targets.
//!
//! These adapt the JSON client with the codecs guests use; they are not
//! a second transport. Request identities, retry, credentials and
//! consistency keep their usual semantics.

use std::marker::PhantomData;

use flower_sdk::Failure;
use flower_sdk::typed::{BusinessError, FlowerType};

use crate::client::{FlowerClient, MutationOptions, MutationResult, QueryResult, RequestOptions};
use crate::error::FlowerError;
use crate::watch::{SubscribeOptions, Subscription, Update, ValueWatch, WatchOptions};

/// A remote call target: a module plus the name of a method it lets
/// callers reach over HTTP.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteTarget {
    module: String,
    name: String,
}

/// Descriptor calls retain the metadata appropriate to their declared kind.
#[derive(Clone, Debug)]
pub enum CallResult<T> {
    Query(QueryResult<T>),
    Mutation(MutationResult<T>),
}
impl<T> CallResult<T> {
    pub fn value(&self) -> &T {
        match self {
            Self::Query(r) => &r.value,
            Self::Mutation(r) => &r.value,
        }
    }
    pub fn into_value(self) -> T {
        match self {
            Self::Query(r) => r.value,
            Self::Mutation(r) => r.value,
        }
    }
}

impl RemoteTarget {
    /// A target with an explicit module and method name.
    pub fn new(module: impl Into<String>, name: impl Into<String>) -> Result<Self, FlowerError> {
        let (module, name) = (module.into(), name.into());
        if module.is_empty() || name.is_empty() {
            return Err(FlowerError::Decode(
                "target needs a module and a method name".to_string(),
            ));
        }
        Ok(RemoteTarget { module, name })
    }

    /// The target module.
    pub fn module(&self) -> &str {
        &self.module
    }

    /// The method's name.
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Encode an engine value as JSON.
pub fn value_json(value: &flower_sdk::Value) -> Result<serde_json::Value, FlowerError> {
    flower_protocol::json::to_json(value).map_err(FlowerError::Decode)
}

/// Decode JSON as an engine value through canonicalization.
pub fn json_value(json: &serde_json::Value) -> Result<flower_sdk::Value, FlowerError> {
    flower_protocol::json::from_json(json).map_err(FlowerError::Decode)
}

/// Map a codec failure.
fn codec(failure: flower_sdk::Failure) -> FlowerError {
    FlowerError::Decode(format!("{}: {}", failure.code, failure.message))
}

impl FlowerClient {
    /// Typed query: encode args with `A`, decode the result with `T`.
    pub async fn typed_query<A: FlowerType, T: FlowerType>(
        &self,
        db: &str,
        module: &str,
        name: &str,
        args: &A,
        options: &RequestOptions,
    ) -> Result<QueryResult<T>, FlowerError> {
        let json = value_json(&args.encode().map_err(codec)?)?;
        let result = self.query_value(db, module, name, &json, options).await?;
        let value = json_value(&result.value)?;
        Ok(QueryResult {
            value: T::decode(value).map_err(codec)?,
            staleness: result.staleness,
            read_token: result.read_token,
        })
    }

    /// Typed mutation: encode args with `A`, decode the result with `T`.
    pub async fn typed_mutate<A: FlowerType, T: FlowerType>(
        &self,
        db: &str,
        module: &str,
        name: &str,
        args: &A,
        options: &MutationOptions,
    ) -> Result<MutationResult<T>, FlowerError> {
        let json = value_json(&args.encode().map_err(codec)?)?;
        let result = self.mutate_value(db, module, name, &json, options).await?;
        let value = json_value(&result.value)?;
        Ok(MutationResult {
            value: T::decode(value).map_err(codec)?,
            revision: result.revision,
            position: result.position,
            duplicate: result.duplicate,
            request_id: result.request_id,
            read_token: result.read_token,
        })
    }

    /// Typed query at a remote target.
    pub async fn typed_query_at<A: FlowerType, T: FlowerType>(
        &self,
        db: &str,
        target: &RemoteTarget,
        args: &A,
        options: &RequestOptions,
    ) -> Result<QueryResult<T>, FlowerError> {
        self.typed_query(db, target.module(), target.name(), args, options)
            .await
    }

    /// Typed mutation at a remote target.
    pub async fn typed_mutate_at<A: FlowerType, T: FlowerType>(
        &self,
        db: &str,
        target: &RemoteTarget,
        args: &A,
        options: &MutationOptions,
    ) -> Result<MutationResult<T>, FlowerError> {
        self.typed_mutate(db, target.module(), target.name(), args, options)
            .await
    }
}

/// The outer error of a declared-error call: a failure the callee's
/// declared error type accepts is separated out typed, and every
/// other outcome (transport, retry, authorization, an undeclared or
/// undecodable failure) stays [`RemoteError::Other`], never disguised
/// as a declared variant. Its parts are boxed, so it stays a pointer or
/// two wide whatever the declared type holds and the `Result`s callers
/// return stay small.
#[derive(Debug)]
pub enum RemoteError<E = Failure> {
    /// The callee failed with a variant of its declared error type.
    /// `source` keeps the failure as the client saw it.
    Declared {
        /// The decoded declared failure.
        error: Box<E>,
        /// The original failure.
        source: Box<FlowerError>,
    },
    /// Any other failure, untouched.
    Other(Box<FlowerError>),
}

impl<E> RemoteError<E> {
    pub fn problem(&self) -> Option<&flower_protocol::Problem> {
        self.source().problem()
    }
    pub fn execution(&self) -> flower_protocol::Execution {
        self.source().execution()
    }
    /// The failure as the client saw it.
    pub fn source(&self) -> &FlowerError {
        match self {
            RemoteError::Declared { source, .. } | RemoteError::Other(source) => source,
        }
    }

    /// The declared failure, if it was one.
    pub fn declared(&self) -> Option<&E> {
        match self {
            RemoteError::Declared { error, .. } => Some(error),
            RemoteError::Other(_) => None,
        }
    }
}

impl<E: std::fmt::Debug> std::fmt::Display for RemoteError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RemoteError::Declared { error, source } => write!(f, "{error:?} ({source})"),
            RemoteError::Other(source) => write!(f, "{source}"),
        }
    }
}

impl<E: std::fmt::Debug> std::error::Error for RemoteError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(RemoteError::source(self))
    }
}

/// Extract only an application failure, preserving its declared payload.
pub(crate) fn call_failure(error: &FlowerError) -> Option<Failure> {
    match error {
        FlowerError::Business {
            code,
            message,
            details,
            ..
        } => Some(Failure {
            code: code.clone(),
            message: message.clone(),
            details: details.as_ref().map(json_value).transpose().ok()?,
        }),
        _ => error
            .problem()
            .filter(|problem| problem.origin == flower_protocol::Origin::Application)
            .and_then(|problem| {
                Some(Failure {
                    code: problem.code.clone(),
                    message: problem.message.clone(),
                    details: problem.details.as_ref().map(json_value).transpose().ok()?,
                })
            }),
    }
}

/// Separate a declared failure from every other outcome.
pub(crate) fn declared<E: BusinessError>(error: FlowerError) -> RemoteError<E> {
    match call_failure(&error).and_then(|failure| E::from_failure(&failure).ok()) {
        Some(declared) => RemoteError::Declared {
            error: Box::new(declared),
            source: Box::new(error),
        },
        None => RemoteError::Other(Box::new(error)),
    }
}

/// Decode a JSON value with a codec.
fn decode<T: FlowerType>(value: serde_json::Value) -> Result<T, FlowerError> {
    T::decode(json_value(&value)?).map_err(codec)
}

/// A typed watch: one connection yielding values decoded with the
/// codec of `T`. A value that does not decode ends the watch and drops
/// its connection: the query and `T` disagree on the contract, so later
/// values would not decode either.
pub struct TypedWatch<T> {
    inner: Option<ValueWatch<serde_json::Value>>,
    marker: PhantomData<fn() -> T>,
}

impl<T: FlowerType> TypedWatch<T> {
    pub fn into_stream(
        self,
    ) -> impl futures_util::Stream<Item = Result<QueryResult<T>, FlowerError>> {
        futures_util::stream::unfold(self, |mut watch| async move {
            watch.next().await.map(|item| (item, watch))
        })
    }
    /// Poll the next decoded value: `None` is a clean end.
    pub async fn next(&mut self) -> Option<Result<QueryResult<T>, FlowerError>> {
        let item = self.inner.as_mut()?.next().await;
        let result = match item {
            None => {
                self.inner = None;
                return None;
            }
            Some(Err(error)) => return Some(Err(error)),
            Some(Ok(result)) => result,
        };
        match decode(result.value) {
            Ok(value) => Some(Ok(QueryResult {
                value,
                staleness: result.staleness,
                read_token: result.read_token,
            })),
            Err(error) => {
                self.inner = None;
                Some(Err(error))
            }
        }
    }
}

/// A reconnecting typed subscription: updates decoded with the codec
/// of `T`. A value that does not decode ends it, as with
/// [`TypedWatch`].
pub struct TypedSubscription<T> {
    inner: Option<Subscription<serde_json::Value>>,
    marker: PhantomData<fn() -> T>,
}

/// A typed watch retaining the query's declared error type, including errors opening it.
pub struct DeclaredWatch<T, E> {
    inner: TypedWatch<T>,
    error: PhantomData<fn() -> E>,
}
impl<T: FlowerType, E: BusinessError> DeclaredWatch<T, E> {
    pub async fn next(&mut self) -> Option<Result<QueryResult<T>, RemoteError<E>>> {
        self.inner.next().await.map(|item| item.map_err(declared))
    }
    pub fn into_stream(
        self,
    ) -> impl futures_util::Stream<Item = Result<QueryResult<T>, RemoteError<E>>> {
        futures_util::stream::unfold(self, |mut watch| async move {
            watch.next().await.map(|item| (item, watch))
        })
    }
}

/// A reconnecting value subscription retaining the query's declared error type.
pub struct DeclaredSubscription<T, E> {
    inner: TypedSubscription<T>,
    error: PhantomData<fn() -> E>,
}
impl<T: FlowerType, E: BusinessError> DeclaredSubscription<T, E> {
    pub async fn next(&mut self) -> Option<Result<Update<T>, RemoteError<E>>> {
        self.inner.next().await.map(|item| item.map_err(declared))
    }
    pub fn into_stream(
        self,
    ) -> impl futures_util::Stream<Item = Result<Update<T>, RemoteError<E>>> {
        futures_util::stream::unfold(self, |mut watch| async move {
            watch.next().await.map(|item| (item, watch))
        })
    }
}

impl<T: FlowerType> TypedSubscription<T> {
    pub fn into_stream(self) -> impl futures_util::Stream<Item = Result<Update<T>, FlowerError>> {
        futures_util::stream::unfold(self, |mut watch| async move {
            watch.next().await.map(|item| (item, watch))
        })
    }
    /// Poll the next decoded update: `None` ends the subscription.
    pub async fn next(&mut self) -> Option<Result<Update<T>, FlowerError>> {
        let item = self.inner.as_mut()?.next().await;
        let update = match item {
            None => {
                self.inner = None;
                return None;
            }
            Some(Err(error)) => return Some(Err(error)),
            Some(Ok(update)) => update,
        };
        match decode(update.value) {
            Ok(value) => Some(Ok(Update {
                revision: update.revision,
                value,
                reset: update.reset,
                history: update.history,
                read_token: update.read_token,
            })),
            Err(error) => {
                self.inner = None;
                Some(Err(error))
            }
        }
    }
}

impl FlowerClient {
    /// Typed query with declared failures: a failure `E` accepts comes
    /// back as [`RemoteError::Declared`], everything else as
    /// [`RemoteError::Other`]. Variants carrying a payload come back
    /// with it, as from mutations: the server answers a query's failure
    /// with its details.
    pub async fn typed_query_declared<A: FlowerType, T: FlowerType, E: BusinessError>(
        &self,
        db: &str,
        module: &str,
        name: &str,
        args: &A,
        options: &RequestOptions,
    ) -> Result<QueryResult<T>, RemoteError<E>> {
        self.typed_query(db, module, name, args, options)
            .await
            .map_err(declared)
    }

    /// Typed query with declared failures at a remote target.
    pub async fn typed_query_declared_at<A: FlowerType, T: FlowerType, E: BusinessError>(
        &self,
        db: &str,
        target: &RemoteTarget,
        args: &A,
        options: &RequestOptions,
    ) -> Result<QueryResult<T>, RemoteError<E>> {
        self.typed_query_declared(db, target.module(), target.name(), args, options)
            .await
    }

    /// Typed mutation with declared failures, payloads included. The
    /// request identity and retry loop are those of
    /// [`FlowerClient::typed_mutate`].
    pub async fn typed_mutate_declared<A: FlowerType, T: FlowerType, E: BusinessError>(
        &self,
        db: &str,
        module: &str,
        name: &str,
        args: &A,
        options: &MutationOptions,
    ) -> Result<MutationResult<T>, RemoteError<E>> {
        self.typed_mutate(db, module, name, args, options)
            .await
            .map_err(declared)
    }

    /// Call a method through its typed descriptor, the static
    /// `#[query]` or `#[mutation]` declared in the guest crate
    /// (`goblin_pizza::ORDER`): its name and kind come from the
    /// declaration, the arguments encode as its argument type and the
    /// result decodes as its result type. A query reads as it declares;
    /// a mutation runs under a fresh request ID. A failure its declared
    /// error type accepts comes back as [`RemoteError::Declared`].
    pub async fn call<A: FlowerType, T: FlowerType, E: BusinessError>(
        &self,
        db: &str,
        module: &str,
        op: &flower_sdk::Op<A, T, E>,
        args: &A,
    ) -> Result<T, RemoteError<E>> {
        self.call_with(db, module, op, args, &MutationOptions::default())
            .await
    }

    /// [`FlowerClient::call`] with options: a mutation takes them whole
    /// (a chosen request ID, an expected revision), a query their
    /// [`base`](MutationOptions::base) (deadline, retries, staleness,
    /// credentials).
    pub async fn call_with<A: FlowerType, T: FlowerType, E: BusinessError>(
        &self,
        db: &str,
        module: &str,
        op: &flower_sdk::Op<A, T, E>,
        args: &A,
        options: &MutationOptions,
    ) -> Result<T, RemoteError<E>> {
        self.call_result(db, module, op, args, options)
            .await
            .map(CallResult::into_value)
    }

    /// Metadata-preserving counterpart of the legacy value-only convenience call.
    pub async fn call_result<A: FlowerType, T: FlowerType, E: BusinessError>(
        &self,
        db: &str,
        module: &str,
        op: &flower_sdk::Op<A, T, E>,
        args: &A,
        options: &MutationOptions,
    ) -> Result<CallResult<T>, RemoteError<E>> {
        if op.is_query() {
            self.typed_query_declared(db, module, op.name, args, &options.base)
                .await
                .map(CallResult::Query)
        } else {
            self.typed_mutate_declared(db, module, op.name, args, options)
                .await
                .map(CallResult::Mutation)
        }
    }

    /// Typed mutation with declared failures at a remote target.
    pub async fn typed_mutate_declared_at<A: FlowerType, T: FlowerType, E: BusinessError>(
        &self,
        db: &str,
        target: &RemoteTarget,
        args: &A,
        options: &MutationOptions,
    ) -> Result<MutationResult<T>, RemoteError<E>> {
        self.typed_mutate_declared(db, target.module(), target.name(), args, options)
            .await
    }

    /// Open one typed watch: values decode with the codec of `T`.
    pub async fn typed_watch<A: FlowerType, T: FlowerType>(
        &self,
        db: &str,
        module: &str,
        name: &str,
        args: &A,
        options: &WatchOptions,
    ) -> Result<TypedWatch<T>, FlowerError> {
        let json = value_json(&args.encode().map_err(codec)?)?;
        Ok(TypedWatch {
            inner: Some(self.watch(db, module, name, &json, options).await?),
            marker: PhantomData,
        })
    }

    pub async fn typed_watch_declared<A: FlowerType, T: FlowerType, E: BusinessError>(
        &self,
        db: &str,
        module: &str,
        name: &str,
        args: &A,
        options: &WatchOptions,
    ) -> Result<DeclaredWatch<T, E>, RemoteError<E>> {
        self.typed_watch(db, module, name, args, options)
            .await
            .map(|inner| DeclaredWatch {
                inner,
                error: PhantomData,
            })
            .map_err(declared)
    }

    /// Open one typed watch at a remote target.
    pub async fn typed_watch_at<A: FlowerType, T: FlowerType>(
        &self,
        db: &str,
        target: &RemoteTarget,
        args: &A,
        options: &WatchOptions,
    ) -> Result<TypedWatch<T>, FlowerError> {
        self.typed_watch(db, target.module(), target.name(), args, options)
            .await
    }

    /// Open a reconnecting typed subscription: updates decode with the
    /// codec of `T`. Connections open lazily on the first `next`.
    pub fn typed_subscribe<A: FlowerType, T: FlowerType>(
        &self,
        db: &str,
        module: &str,
        name: &str,
        args: &A,
        options: &SubscribeOptions,
    ) -> Result<TypedSubscription<T>, FlowerError> {
        let json = value_json(&args.encode().map_err(codec)?)?;
        Ok(TypedSubscription {
            inner: Some(self.subscribe(db, module, name, &json, options)),
            marker: PhantomData,
        })
    }

    pub fn typed_subscribe_declared<A: FlowerType, T: FlowerType, E: BusinessError>(
        &self,
        db: &str,
        module: &str,
        name: &str,
        args: &A,
        options: &SubscribeOptions,
    ) -> Result<DeclaredSubscription<T, E>, RemoteError<E>> {
        self.typed_subscribe(db, module, name, args, options)
            .map(|inner| DeclaredSubscription {
                inner,
                error: PhantomData,
            })
            .map_err(declared)
    }

    /// Open a reconnecting typed subscription at a remote target.
    pub fn typed_subscribe_at<A: FlowerType, T: FlowerType>(
        &self,
        db: &str,
        target: &RemoteTarget,
        args: &A,
        options: &SubscribeOptions,
    ) -> Result<TypedSubscription<T>, FlowerError> {
        self.typed_subscribe(db, target.module(), target.name(), args, options)
    }
}
