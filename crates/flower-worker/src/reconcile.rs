//! `reconcile`: keep external values current (`flower_sdk::external`).
//! Wait for pending work, compute it outside Flower, and publish the
//! result guarded by the key it was computed for; a result whose input
//! changed meanwhile is rejected, never stored, and the fresh work is
//! picked up again.
//!
//! Three modes:
//!
//! - **One key** ([`ReconcileOptions::args`]): watch `<name>.pending`
//!   for those arguments, compute each work item as it appears, publish.
//! - **Pool** (no `args`): drain `<name>.next` with up to `concurrency`
//!   computations at once, one per argument set at a time, and watch it
//!   while it is empty. [`ReconcileOptions::shard`] drains one hash shard
//!   of the argument sets, so unleased workers can split a pool.
//! - **Leased pool** ([`ReconcileOptions::leased`]): claim work through
//!   `<name>.claim`, so each argument set is computed by one process at
//!   a time across every worker: claim only as many keys as there are
//!   free computations, renew every held lease in one call at a third of
//!   the lease, stop a computation whose lease is lost or about to run
//!   out, hand failed or rejected keys back through `<name>.release`
//!   (failures after a backoff that every process honours), and watch
//!   `<name>.ready` while nothing is claimable. Concurrency is fixed or
//!   adaptive ([`ReconcileOptions::adaptive`], the queue worker's limiter).
//!
//! A failed computation retries after a backoff of its own (per argument
//! set in a pool) without holding up the others. Publications retry
//! transient failures with one request ID, so a retried publication
//! applies once. [`reconcile_typed`] runs any mode over typed arguments,
//! inputs and results ([`flower_sdk::external::Work`]).

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;

use flower_client::client::{FlowerClient, MutationOptions, RequestOptions};
use flower_client::error::FlowerError;
use flower_client::typed::{json_value, value_json};
use flower_client::watch::WatchOptions;
use flower_sdk::FlowerType;
use flower_sdk::external::Work;
use futures_util::FutureExt;
use futures_util::stream::{FuturesUnordered, StreamExt};
use serde_json::Value;
use tokio::sync::Notify;
use tokio::time::Instant;

use flower_client::retry::Retry;

use crate::capacity::{Adaptive, Health, LimitChange, Limiter, LoadGate, PoolLoad, ProcessHealth};

/// Wait before reopening a watch that failed or ended.
const RECONNECT: Duration = Duration::from_millis(250);
/// Reopen a watch silent this long; servers send heartbeats.
const STALL: Duration = Duration::from_secs(45);

/// One unit of work as the server described it: its arguments and key
/// go back verbatim with the result.
#[derive(Clone, Debug, PartialEq)]
pub struct ExternalWork {
    /// The argument set.
    pub args: Value,
    /// The key the work is for: canonical `[args, input]`.
    pub key: String,
    /// The input to compute from.
    pub input: Value,
}

impl ExternalWork {
    /// Parse a work item, or `None` for null.
    fn parse(value: Value) -> Result<Option<Self>, FlowerError> {
        if value.is_null() {
            return Ok(None);
        }
        let malformed = || FlowerError::Decode("malformed external work".to_string());
        let Value::Object(mut fields) = value else {
            return Err(malformed());
        };
        let (Some(args), Some(Value::String(key)), Some(input)) = (
            fields.remove("args"),
            fields.remove("key"),
            fields.remove("input"),
        ) else {
            return Err(malformed());
        };
        Ok(Some(ExternalWork { args, key, input }))
    }
}

/// The calls reconciling makes. `FlowerClient` implements it; tests can
/// script it.
pub trait ReconcileClient: Clone + Send + Sync + 'static {
    /// Run a query, retrying transient failures.
    fn query(
        &self,
        db: &str,
        module: &str,
        name: &str,
        args: &Value,
    ) -> impl Future<Output = Result<Value, FlowerError>> + Send;

    /// Run a mutation under one request ID across its retries.
    fn mutate(
        &self,
        db: &str,
        module: &str,
        name: &str,
        args: &Value,
    ) -> impl Future<Output = Result<Value, FlowerError>> + Send;

    /// Run a mutation with these options: a retry policy and a deadline
    /// per call (one request ID across its retries). Clients without
    /// per-call options run [`ReconcileClient::mutate`].
    fn mutate_with(
        &self,
        db: &str,
        module: &str,
        name: &str,
        args: &Value,
        options: MutationOptions,
    ) -> impl Future<Output = Result<Value, FlowerError>> + Send {
        let _ = options;
        self.mutate(db, module, name, args)
    }

    /// The first value of the query's subscription satisfying
    /// `predicate`, reconnecting on transient failures.
    fn wait_until(
        &self,
        db: &str,
        module: &str,
        name: &str,
        args: &Value,
        predicate: fn(&Value) -> bool,
    ) -> impl Future<Output = Result<Value, FlowerError>> + Send;
}

impl ReconcileClient for FlowerClient {
    async fn query(
        &self,
        db: &str,
        module: &str,
        name: &str,
        args: &Value,
    ) -> Result<Value, FlowerError> {
        self.query_value(db, module, name, args, &RequestOptions::retrying())
            .await
            .map(|result| result.value)
    }

    async fn mutate(
        &self,
        db: &str,
        module: &str,
        name: &str,
        args: &Value,
    ) -> Result<Value, FlowerError> {
        self.mutate_value(db, module, name, args, &MutationOptions::retrying())
            .await
            .map(|result| result.value)
    }

    async fn mutate_with(
        &self,
        db: &str,
        module: &str,
        name: &str,
        args: &Value,
        options: MutationOptions,
    ) -> Result<Value, FlowerError> {
        self.mutate_value(db, module, name, args, &options)
            .await
            .map(|result| result.value)
    }

    async fn wait_until(
        &self,
        db: &str,
        module: &str,
        name: &str,
        args: &Value,
        predicate: fn(&Value) -> bool,
    ) -> Result<Value, FlowerError> {
        // A reconnecting loop over single watches: `Subscription`
        // futures are not `Send`, and reconcile runs on any runtime.
        loop {
            let watched = self
                .watch::<Value>(db, module, name, args, &WatchOptions::default())
                .await;
            let mut watch = match watched {
                Ok(watch) => watch,
                Err(error) if error.is_transient() => {
                    tokio::time::sleep(RECONNECT).await;
                    continue;
                }
                Err(error) => return Err(error),
            };
            loop {
                // Servers send heartbeats: silence means a dead link.
                match tokio::time::timeout(STALL, watch.next()).await {
                    Ok(Some(Ok(result))) if predicate(&result.value) => return Ok(result.value),
                    Ok(Some(Ok(_))) => {}
                    Ok(Some(Err(error))) if error.is_transient() => break,
                    Ok(Some(Err(FlowerError::Decode(_)))) => {}
                    Ok(Some(Err(error))) => return Err(error),
                    Ok(None) | Err(_) => break,
                }
            }
            tokio::time::sleep(RECONNECT).await;
        }
    }
}

/// What reconcile reports as it goes (v1's `ReconcileEvent`).
#[derive(Clone, Debug, PartialEq)]
pub enum ReconcileEvent {
    /// A leased pool claimed a key: the attempt counts claims of the
    /// key across every process.
    Claimed {
        /// The work's key.
        key: String,
        /// Claims of the key so far, this one included.
        attempt: u64,
    },
    /// A result was published: accepted, or rejected as computed for
    /// an older input.
    Published {
        /// The work's key.
        key: String,
        /// Whether it was accepted.
        accepted: bool,
    },
    /// A computation failed, or its result was refused; it retries
    /// after its backoff.
    Failed {
        /// The work's key.
        key: String,
        /// Why.
        error: String,
    },
    /// A leased computation stopped: its lease was lost or about to run
    /// out.
    Lost {
        /// The work's key.
        key: String,
    },
    /// A leased pool's adaptive limit changed.
    Limit(LimitChange),
    /// A leased pool with an adaptive limit says its load: when it moves
    /// to another band or the limiter starts or stops holding back (at
    /// most every five seconds), and once a minute while it works.
    Load(PoolLoad),
    /// A call failed transiently; reconcile waits and tries again.
    Waiting {
        /// The failure.
        error: String,
    },
}

/// Receives every [`ReconcileEvent`], in order.
pub type ReconcileHandler = Arc<dyn Fn(ReconcileEvent) + Send + Sync>;

/// Reconcile tuning.
#[derive(Clone)]
pub struct ReconcileOptions {
    /// Database name.
    pub db: String,
    /// Module that declares the external value.
    pub module: String,
    /// The external value's name.
    pub external: String,
    /// Keep the key of these arguments current; `None` drains the pool.
    pub args: Option<Value>,
    /// Pool: work only on argument sets in hash shard `index` of
    /// `count` (`<name>.next`'s `shard`).
    pub shard: Option<(u32, u32)>,
    /// Pool: computations at once (default 4); the fixed limit of a
    /// leased pool.
    pub concurrency: usize,
    /// Pool: work items fetched or claimed per round (default 16).
    pub batch: usize,
    /// Sleep after a transient failure, and the longest wait between
    /// pool rounds while computations run (default 1 s).
    pub poll_interval: Duration,
    /// Backoff base and cap for failed computations (default 1 s, 60 s).
    pub retry_base: Duration,
    /// See `retry_base`.
    pub retry_max: Duration,
    /// Lease keys through `<name>.claim` so each is computed by one
    /// process at a time (see the module docs).
    pub leased: bool,
    /// Leased pool: the lease owner, unique per process (default
    /// `reconciler-<random>`).
    pub owner: Option<String>,
    /// Leased pool: lease length per claim and renewal (default 30 s).
    pub lease: Duration,
    /// Leased pool: stop a computation this long before its lease ends
    /// (default a fifth of the lease).
    pub margin: Option<Duration>,
    /// Leased pool: an adaptive limit within these bounds instead of
    /// `concurrency`, following the backlog and the process's load.
    pub adaptive: Option<Adaptive>,
    /// Leased pool with an adaptive limit: the load it reads (default
    /// [`ProcessHealth`]).
    pub health: Option<Arc<dyn Health>>,
    /// Leased pool with an adaptive limit: how often it adjusts
    /// (default 250 ms).
    pub adjust_every: Duration,
    /// Receives claims, publications, failures, lost leases, limit
    /// changes and waits.
    pub on_event: Option<ReconcileHandler>,
    /// The retry policy of each call: publications retry with it (the
    /// client's default policy when `None`), and a leased pool's claims,
    /// renewals and releases with it and a deadline: a claim's lease, the
    /// earliest of the renewed leases, a release's lease end.
    pub call_retry: Option<Retry>,
}

impl std::fmt::Debug for ReconcileOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReconcileOptions")
            .field("db", &self.db)
            .field("module", &self.module)
            .field("external", &self.external)
            .field("args", &self.args)
            .field("shard", &self.shard)
            .field("concurrency", &self.concurrency)
            .field("batch", &self.batch)
            .field("leased", &self.leased)
            .field("owner", &self.owner)
            .field("lease", &self.lease)
            .field("adaptive", &self.adaptive)
            .finish_non_exhaustive()
    }
}

impl ReconcileOptions {
    /// Reconcile an external value of a module.
    pub fn new(
        db: impl Into<String>,
        module: impl Into<String>,
        external: impl Into<String>,
    ) -> Self {
        ReconcileOptions {
            db: db.into(),
            module: module.into(),
            external: external.into(),
            args: None,
            shard: None,
            concurrency: 4,
            batch: 16,
            poll_interval: Duration::from_secs(1),
            retry_base: Duration::from_secs(1),
            retry_max: Duration::from_secs(60),
            leased: false,
            owner: None,
            lease: Duration::from_secs(30),
            margin: None,
            adaptive: None,
            health: None,
            adjust_every: Duration::from_millis(250),
            on_event: None,
            call_retry: None,
        }
    }

    /// Keep the key of these arguments current.
    pub fn args(mut self, args: Value) -> Self {
        self.args = Some(args);
        self
    }

    /// Pool: drain only hash shard `index` of `count`.
    pub fn shard(mut self, index: u32, count: u32) -> Self {
        self.shard = Some((index, count));
        self
    }

    /// Lease keys, so each is computed by one process at a time.
    pub fn leased(mut self) -> Self {
        self.leased = true;
        self
    }

    /// Leased pool: the lease owner.
    pub fn owner(mut self, owner: impl Into<String>) -> Self {
        self.owner = Some(owner.into());
        self
    }

    /// Leased pool: lease length, and how long before its end a
    /// computation stops.
    pub fn lease(mut self, lease: Duration, margin: Duration) -> Self {
        self.lease = lease;
        self.margin = Some(margin);
        self
    }

    /// Leased pool: computations at once following the backlog and the
    /// process's load, from `min` through `max`, starting at `initial`.
    pub fn adaptive(mut self, min: usize, initial: usize, max: usize) -> Self {
        self.adaptive = Some(Adaptive::new(min, initial, max));
        self
    }

    /// Leased pool: the load an adaptive limit reads.
    pub fn health(mut self, health: Arc<dyn Health>) -> Self {
        self.health = Some(health);
        self
    }

    /// Leased pool: how often an adaptive limit adjusts.
    pub fn adjust_every(mut self, every: Duration) -> Self {
        self.adjust_every = every;
        self
    }

    /// Receive every [`ReconcileEvent`].
    pub fn on_event(mut self, handler: impl Fn(ReconcileEvent) + Send + Sync + 'static) -> Self {
        self.on_event = Some(Arc::new(handler));
        self
    }

    /// Retry every call with this policy (see `call_retry`).
    pub fn call_retry(mut self, retry: impl Into<Retry>) -> Self {
        self.call_retry = Some(retry.into());
        self
    }

    /// The options of one call, with a deadline when given.
    fn call(&self, deadline: Option<Instant>) -> MutationOptions {
        let mut options = MutationOptions::retrying();
        if let Some(retry) = &self.call_retry {
            options = options.retry(retry.clone());
        }
        if let Some(deadline) = deadline {
            options = options.deadline(deadline.into_std());
        }
        options
    }

    /// Report an event.
    fn emit(&self, event: ReconcileEvent) {
        if let Some(handler) = &self.on_event {
            handler(event);
        }
    }

    /// Pool: computations at once.
    pub fn concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency.max(1);
        self
    }

    /// Pool: work items fetched per round.
    pub fn batch(mut self, batch: usize) -> Self {
        self.batch = batch.max(1);
        self
    }

    /// Sleep after a transient failure.
    pub fn poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// Backoff base and cap for failed computations.
    pub fn retry(mut self, base: Duration, max: Duration) -> Self {
        self.retry_base = base;
        self.retry_max = max;
        self
    }

    fn method(&self, suffix: &str) -> String {
        format!("{}.{suffix}", self.external)
    }

    fn backoff(&self, failures: u32) -> Duration {
        self.retry_base
            .saturating_mul(1 << failures.min(10))
            .min(self.retry_max)
    }
}

/// What one reconcile run did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReconcileSummary {
    /// Results accepted.
    pub published: u64,
    /// Results rejected because the input changed meanwhile.
    pub rejected: u64,
    /// Computations that failed (and were retried).
    pub failed: u64,
    /// Leased pool: computations stopped because their lease was lost
    /// or about to run out; another process computes the key again.
    pub lost: u64,
}

/// Invalid reconcile options, as the queue worker reports its own.
fn invalid(message: &str) -> FlowerError {
    FlowerError::Server {
        code: "invalid_options".to_string(),
        message: message.to_string(),
    }
}

/// Whether a client failure would recur on the same request (a
/// refusal that ends on its own, such as `queue_purging`, does not).
fn permanent(error: &FlowerError) -> bool {
    !error.is_transient()
        && matches!(
            error,
            FlowerError::Business { .. }
                | FlowerError::Http(_)
                | FlowerError::RetryFailed { .. }
                | FlowerError::Unsupported(_)
                | FlowerError::Auth(_)
                | FlowerError::Server { .. }
                | FlowerError::Decode(_)
        )
}

/// Run a computation, turning a panic into a failure.
async fn run_compute<F, Fut>(compute: &F, work: ExternalWork) -> Result<Value, String>
where
    F: Fn(ExternalWork) -> Fut,
    Fut: Future<Output = Result<Value, String>>,
{
    match std::panic::catch_unwind(AssertUnwindSafe(|| compute(work))) {
        Ok(future) => AssertUnwindSafe(future)
            .catch_unwind()
            .await
            .unwrap_or_else(|_| Err("the computation panicked".to_string())),
        Err(_) => Err("the computation panicked".to_string()),
    }
}

/// Keep an external value current until `stop` fires. `compute` gets
/// each work item and returns the value to publish, or a failure to
/// retry after a backoff. Returns the summary, or the first permanent
/// client failure (an unknown module or external value, revoked
/// credentials).
pub async fn reconcile<C, F, Fut>(
    client: C,
    options: ReconcileOptions,
    stop: Arc<Notify>,
    compute: F,
) -> Result<ReconcileSummary, FlowerError>
where
    C: ReconcileClient,
    F: Fn(ExternalWork) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Value, String>> + Send + 'static,
{
    let compute = Arc::new(compute);
    let mut summary = ReconcileSummary::default();
    if options.leased {
        if options.args.is_some() || options.shard.is_some() {
            return Err(invalid(
                "a leased pool spreads the whole pool: omit args and shard",
            ));
        }
        return leased(&client, &options, &stop, compute, &mut summary)
            .await
            .map(|()| summary);
    }
    if options.adaptive.is_some() {
        return Err(invalid("adaptive concurrency needs a leased pool"));
    }
    if options.shard.is_some_and(|(index, count)| index >= count) {
        return Err(invalid("shard is (index, count) with index < count"));
    }
    let outcome = match options.args.clone() {
        Some(args) => one_key(&client, &options, &stop, compute, args, &mut summary).await,
        None => pool(&client, &options, &stop, compute, &mut summary).await,
    };
    outcome.map(|()| summary)
}

/// [`reconcile`] over typed arguments `A`, inputs `I` and results `R`,
/// through the guests' [`FlowerType`] codecs: `compute` gets
/// [`flower_sdk::external::Work`] and returns the result to publish. A
/// work item whose arguments or input do not decode, or a result that
/// does not encode, fails that computation (and retries after its
/// backoff) like any other failure.
pub async fn reconcile_typed<C, A, I, R, F, Fut>(
    client: C,
    options: ReconcileOptions,
    stop: Arc<Notify>,
    compute: F,
) -> Result<ReconcileSummary, FlowerError>
where
    C: ReconcileClient,
    A: FlowerType + Send,
    I: FlowerType + Send,
    R: FlowerType + Send,
    F: Fn(Work<A, I>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<R, String>> + Send + 'static,
{
    let compute = Arc::new(compute);
    reconcile(client, options, stop, move |work: ExternalWork| {
        let compute = compute.clone();
        async move {
            let decode = |json: &Value| {
                json_value(json).map_err(|error| format!("the work does not decode: {error}"))
            };
            let typed = Work {
                args: A::decode(decode(&work.args)?).map_err(|error| error.message)?,
                key: work.key,
                input: I::decode(decode(&work.input)?).map_err(|error| error.message)?,
            };
            let result = compute(typed).await?;
            let encoded = result.encode().map_err(|error| error.message)?;
            value_json(&encoded).map_err(|error| error.to_string())
        }
    })
    .await
}

/// Publish one result, counting the verdict: whether it was accepted.
async fn publish<C: ReconcileClient>(
    client: &C,
    options: &ReconcileOptions,
    work: &ExternalWork,
    value: Value,
    summary: &mut ReconcileSummary,
) -> Result<bool, FlowerError> {
    let body = serde_json::json!({"args": work.args, "key": work.key, "value": value});
    let accepted = client
        .mutate_with(
            &options.db,
            &options.module,
            &options.method("publish"),
            &body,
            options.call(None),
        )
        .await?;
    let accepted = accepted == Value::Bool(true);
    options.emit(ReconcileEvent::Published {
        key: work.key.clone(),
        accepted,
    });
    if accepted {
        summary.published += 1;
    } else {
        summary.rejected += 1;
    }
    Ok(accepted)
}

/// Sleep, or return early when `stop` fires; true when stopped.
async fn pause(stop: &Notify, duration: Duration) -> bool {
    tokio::select! {
        _ = stop.notified() => true,
        _ = tokio::time::sleep(duration) => false,
    }
}

/// One key: watch its pending work, compute, publish, repeat.
async fn one_key<C, F, Fut>(
    client: &C,
    options: &ReconcileOptions,
    stop: &Notify,
    compute: Arc<F>,
    args: Value,
    summary: &mut ReconcileSummary,
) -> Result<(), FlowerError>
where
    C: ReconcileClient,
    F: Fn(ExternalWork) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Value, String>> + Send + 'static,
{
    let pending = options.method("pending");
    let mut failures = 0u32;
    loop {
        let waited = tokio::select! {
            _ = stop.notified() => return Ok(()),
            waited = client.wait_until(&options.db, &options.module, &pending, &args, |value| !value.is_null()) => waited,
        };
        let work = match waited.and_then(ExternalWork::parse) {
            Ok(Some(work)) => work,
            Ok(None) => continue,
            Err(error) if permanent(&error) => return Err(error),
            Err(error) => {
                options.emit(ReconcileEvent::Waiting {
                    error: error.to_string(),
                });
                if pause(stop, options.poll_interval).await {
                    return Ok(());
                }
                continue;
            }
        };
        let computed = tokio::select! {
            _ = stop.notified() => return Ok(()),
            computed = run_compute(compute.as_ref(), work.clone()) => computed,
        };
        match computed {
            Ok(value) => {
                failures = 0;
                match publish(client, options, &work, value, summary).await {
                    Ok(_) => {}
                    Err(error) if permanent(&error) => return Err(error),
                    Err(error) => {
                        options.emit(ReconcileEvent::Waiting {
                            error: error.to_string(),
                        });
                        if pause(stop, options.poll_interval).await {
                            return Ok(());
                        }
                    }
                }
            }
            Err(error) => {
                summary.failed += 1;
                options.emit(ReconcileEvent::Failed {
                    key: work.key.clone(),
                    error,
                });
                let delay = options.backoff(failures);
                failures += 1;
                if pause(stop, delay).await {
                    return Ok(());
                }
            }
        }
    }
}

/// A finished pool computation.
struct Computed {
    work: ExternalWork,
    row: String,
    result: Result<Value, String>,
}

/// Pool: drain `next` with bounded concurrency, one computation per
/// argument set at a time.
async fn pool<C, F, Fut>(
    client: &C,
    options: &ReconcileOptions,
    stop: &Notify,
    compute: Arc<F>,
    summary: &mut ReconcileSummary,
) -> Result<(), FlowerError>
where
    C: ReconcileClient,
    F: Fn(ExternalWork) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Value, String>> + Send + 'static,
{
    let next = options.method("next");
    let mut limit = serde_json::json!({"limit": options.batch});
    if let Some((index, count)) = options.shard {
        limit["shard"] = serde_json::json!([index, count]);
    }
    let mut running = FuturesUnordered::new();
    // Argument sets computing now, by canonical text.
    let mut busy: BTreeSet<String> = BTreeSet::new();
    // Failed argument sets: failures so far and when they may retry.
    let mut backoff: BTreeMap<String, (u32, Instant)> = BTreeMap::new();
    loop {
        // Fetch work while there is room.
        if running.len() < options.concurrency {
            let fetched = client
                .query(&options.db, &options.module, &next, &limit)
                .await;
            let items = match fetched {
                Ok(Value::Array(items)) => items,
                Ok(_) => return Err(FlowerError::Decode("next returned no list".to_string())),
                Err(error) if permanent(&error) => return Err(error),
                Err(error) => {
                    options.emit(ReconcileEvent::Waiting {
                        error: error.to_string(),
                    });
                    if pause(stop, options.poll_interval).await {
                        break;
                    }
                    continue;
                }
            };
            let now = Instant::now();
            let mut started = false;
            for item in items {
                if running.len() >= options.concurrency {
                    break;
                }
                let Some(work) = ExternalWork::parse(item)? else {
                    continue;
                };
                let row = work.args.to_string();
                if busy.contains(&row) || backoff.get(&row).is_some_and(|(_, until)| *until > now) {
                    continue;
                }
                busy.insert(row.clone());
                started = true;
                let compute = compute.clone();
                running.push(async move {
                    let result = run_compute(compute.as_ref(), work.clone()).await;
                    Computed { work, row, result }
                });
            }
            if !started && running.is_empty() {
                // Nothing to start: wait for new work, or for the
                // earliest backoff to end.
                let retry_at = backoff.values().map(|(_, until)| *until).min();
                let waited = tokio::select! {
                    _ = stop.notified() => break,
                    () = sleep_until(retry_at) => continue,
                    waited = client.wait_until(&options.db, &options.module, &next, &limit, |value| {
                        value.as_array().is_some_and(|items| !items.is_empty())
                    }) => waited,
                };
                match waited {
                    Ok(_) => continue,
                    Err(error) if permanent(&error) => return Err(error),
                    Err(error) => {
                        options.emit(ReconcileEvent::Waiting {
                            error: error.to_string(),
                        });
                        if pause(stop, options.poll_interval).await {
                            break;
                        }
                        continue;
                    }
                }
            }
        }
        let done = tokio::select! {
            _ = stop.notified() => break,
            Some(done) = running.next(), if !running.is_empty() => Some(done),
            // Room left but nothing new started: look again shortly.
            _ = tokio::time::sleep(options.poll_interval), if running.len() < options.concurrency => None,
        };
        let Some(Computed { work, row, result }) = done else {
            continue;
        };
        busy.remove(&row);
        match result {
            Ok(value) => {
                backoff.remove(&row);
                match publish(client, options, &work, value, summary).await {
                    Ok(_) => {}
                    Err(error) if permanent(&error) => return Err(error),
                    Err(_) => {}
                }
            }
            Err(error) => {
                summary.failed += 1;
                options.emit(ReconcileEvent::Failed {
                    key: work.key.clone(),
                    error,
                });
                let failures = backoff.get(&row).map_or(0, |(failures, _)| *failures);
                let until = Instant::now() + options.backoff(failures);
                backoff.insert(row, (failures + 1, until));
            }
        }
    }
    // Stopping: computations in flight are dropped unpublished; their
    // work stays stale for the next run.
    Ok(())
}

/// One leased key being computed.
struct Held {
    /// `{args, key, owner, attempt}`: what renewals and releases name.
    lease: Value,
    /// Claims of this key so far, across every process.
    attempt: u64,
    /// When the computation stops unless a renewal lands first: the
    /// lease's end, as measured from before the call that set it, less
    /// the margin.
    deadline: Instant,
    /// The lease was lost or ran out: the computation was told to stop.
    lost: bool,
    /// Tells the computation to stop.
    stop: Arc<Notify>,
}

/// A leased computation that ended: its slot, its work, and its result,
/// or `None` when its lease was lost first.
struct LeasedDone {
    slot: u64,
    work: ExternalWork,
    result: Option<Result<Value, String>>,
}

/// A lease owner unique enough per process.
fn random_owner() -> String {
    use std::hash::BuildHasher;
    let seed =
        std::collections::hash_map::RandomState::new().hash_one(std::time::SystemTime::now());
    format!("reconciler-{seed:016x}")
}

/// Parse one claim: the work, its lease and its attempt.
fn parse_claim(claim: Value) -> Result<(ExternalWork, Value, u64), FlowerError> {
    let malformed = || FlowerError::Decode("malformed external claim".to_string());
    let (Some(owner), Some(attempt)) = (
        claim
            .get("owner")
            .filter(|owner| owner.is_string())
            .cloned(),
        claim.get("attempt").and_then(Value::as_u64),
    ) else {
        return Err(malformed());
    };
    let work = ExternalWork::parse(claim)?.ok_or_else(malformed)?;
    let lease = serde_json::json!({
        "args": work.args,
        "key": work.key,
        "owner": owner,
        "attempt": attempt,
    });
    Ok((work, lease, attempt))
}

/// Hand a held key back after `delay`, best effort: a release that
/// does not land leaves the lease to run out instead.
async fn release_lease<C: ReconcileClient>(
    client: &C,
    options: &ReconcileOptions,
    lease: &Value,
    delay: Duration,
    deadline: Option<Instant>,
) {
    let mut body = lease.clone();
    body["delayMs"] = serde_json::json!(delay.as_millis() as u64);
    let release = options.method("release");
    // With no deadline (on the way out), one attempt.
    let call = match deadline {
        Some(deadline) => options.call(Some(deadline)),
        None => MutationOptions::default().retry(Retry::Off),
    };
    let _ = tokio::time::timeout(
        options.lease,
        client.mutate_with(&options.db, &options.module, &release, &body, call),
    )
    .await;
}

/// The readiness watch: resolves once `<name>.ready` is true.
type ReadyWatch = std::pin::Pin<Box<dyn Future<Output = Result<Value, FlowerError>> + Send>>;

/// Await the readiness watch, or forever without one.
async fn ready_or_pending(waiting: &mut Option<ReadyWatch>) -> Result<Value, FlowerError> {
    match waiting {
        Some(watch) => watch.await,
        None => std::future::pending().await,
    }
}

/// A leased pool: claim keys for free computations, renew what is held,
/// stop what is lost, hand back what failed, publish the rest.
async fn leased<C, F, Fut>(
    client: &C,
    options: &ReconcileOptions,
    stop: &Notify,
    compute: Arc<F>,
    summary: &mut ReconcileSummary,
) -> Result<(), FlowerError>
where
    C: ReconcileClient,
    F: Fn(ExternalWork) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Value, String>> + Send + 'static,
{
    let owner = options.owner.clone().unwrap_or_else(random_owner);
    let margin = options.margin.unwrap_or(options.lease / 5);
    if options.lease < Duration::from_millis(1) || margin >= options.lease {
        return Err(invalid("the lease must exceed its margin"));
    }
    let usable = options.lease - margin;
    let lease_ms = options.lease.as_millis() as u64;
    let mut limiter = match options.adaptive {
        None => Limiter::fixed(options.concurrency),
        Some(bounds) => {
            let health = options
                .health
                .clone()
                .unwrap_or_else(|| Arc::new(ProcessHealth::new()));
            Limiter::new(bounds, health).map_err(|message| invalid(&message))?
        }
    };
    if limiter.bounds().1 > MAX_LEASED {
        return Err(invalid(
            "a leased pool runs at most 1024 computations at once",
        ));
    }
    let (claim, renew, ready) = (
        options.method("claim"),
        options.method("renew"),
        options.method("ready"),
    );
    let every = (usable / 3).max(Duration::from_millis(1));
    let mut renewals = tokio::time::interval_at(Instant::now() + every, every);
    renewals.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let adjust_every = options.adjust_every.max(Duration::from_millis(1));
    let mut adjustments = tokio::time::interval_at(Instant::now() + adjust_every, adjust_every);
    adjustments.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut running = FuturesUnordered::new();
    let mut held: BTreeMap<u64, Held> = BTreeMap::new();
    let mut slots = 0u64;
    // Set while the last claim came back short: claim again once
    // `<name>.ready` says something is claimable.
    let mut waiting: Option<ReadyWatch> = None;
    // When the pool says its load.
    let mut gate = LoadGate::default();
    let outcome = loop {
        let room = limiter.limit().saturating_sub(running.len());
        if waiting.is_none() && room > 0 {
            let limit = options.batch.min(room);
            // Leases count from before the call: the server's can only
            // start later.
            let sent = Instant::now();
            let body = serde_json::json!({"owner": owner, "limit": limit, "leaseMs": lease_ms});
            let claimed = tokio::select! {
                _ = stop.notified() => break Ok(()),
                claimed = client.mutate_with(&options.db, &options.module, &claim, &body, options.call(Some(sent + options.lease))) => claimed,
            };
            let claims = match claimed {
                Ok(Value::Array(claims)) => claims,
                Ok(_) => break Err(FlowerError::Decode("claim returned no list".to_string())),
                Err(error) if permanent(&error) => break Err(error),
                Err(error) => {
                    options.emit(ReconcileEvent::Waiting {
                        error: error.to_string(),
                    });
                    if pause(stop, options.poll_interval).await {
                        break Ok(());
                    }
                    continue;
                }
            };
            let count = claims.len();
            let parsed: Result<Vec<_>, FlowerError> = claims.into_iter().map(parse_claim).collect();
            let parsed = match parsed {
                Ok(parsed) => parsed,
                Err(error) => break Err(error),
            };
            for (work, lease, attempt) in parsed {
                options.emit(ReconcileEvent::Claimed {
                    key: work.key.clone(),
                    attempt,
                });
                slots += 1;
                let slot = slots;
                let lost = Arc::new(Notify::new());
                held.insert(
                    slot,
                    Held {
                        lease,
                        attempt,
                        deadline: sent + usable,
                        lost: false,
                        stop: lost.clone(),
                    },
                );
                let compute = compute.clone();
                running.push(async move {
                    let result = tokio::select! {
                        result = run_compute(compute.as_ref(), work.clone()) => Some(result),
                        () = lost.notified() => None,
                    };
                    LeasedDone { slot, work, result }
                });
            }
            if count < limit {
                // The backlog is drained: claim again once work is ready.
                let (client, db, module, ready) = (
                    client.clone(),
                    options.db.clone(),
                    options.module.clone(),
                    ready.clone(),
                );
                waiting = Some(Box::pin(async move {
                    client
                        .wait_until(&db, &module, &ready, &Value::Null, |value| {
                            *value == Value::Bool(true)
                        })
                        .await
                }));
            } else if running.len() >= limiter.limit() {
                limiter.want();
            }
            continue;
        }
        let deadline = held
            .values()
            .filter(|entry| !entry.lost)
            .map(|entry| entry.deadline)
            .min();
        tokio::select! {
            _ = stop.notified() => break Ok(()),
            Some(done) = running.next(), if !running.is_empty() => {
                let Some(entry) = held.remove(&done.slot) else { continue };
                let backoff = options.backoff(entry.attempt.saturating_sub(1).min(31) as u32);
                match done.result {
                    None => {
                        summary.lost += 1;
                        options.emit(ReconcileEvent::Lost { key: done.work.key.clone() });
                    }
                    Some(Err(error)) => {
                        summary.failed += 1;
                        options.emit(ReconcileEvent::Failed { key: done.work.key.clone(), error });
                        // The delay holds for every process, and the
                        // attempt count carries on across them.
                        release_lease(client, options, &entry.lease, backoff, Some(entry.deadline + margin)).await;
                    }
                    Some(Ok(value)) => match publish(client, options, &done.work, value, summary).await {
                        Ok(true) => {}
                        // Computed for an older input: hand the key back
                        // for the current one.
                        Ok(false) => release_lease(client, options, &entry.lease, Duration::ZERO, Some(entry.deadline + margin)).await,
                        // The result itself was refused (its contract):
                        // other keys still progress.
                        Err(error @ FlowerError::Business { .. }) => {
                            summary.failed += 1;
                            options.emit(ReconcileEvent::Failed { key: done.work.key.clone(), error: error.to_string() });
                            release_lease(client, options, &entry.lease, backoff, Some(entry.deadline + margin)).await;
                        }
                        Err(error) if permanent(&error) => break Err(error),
                        // Once the lease runs out, a process computes it again.
                        Err(_) => {}
                    },
                }
            }
            _ = renewals.tick() => {
                // One call per 1,024 leases, each extended from before
                // its call; a null answer means the lease was lost.
                let leases: Vec<(u64, Value)> = held
                    .iter()
                    .filter(|(_, entry)| !entry.lost)
                    .map(|(slot, entry)| (*slot, entry.lease.clone()))
                    .collect();
                let mut failed = None;
                for chunk in leases.chunks(MAX_RENEWALS) {
                    let sent = Instant::now();
                    let body = serde_json::json!({
                        "leases": chunk.iter().map(|(_, lease)| lease.clone()).collect::<Vec<_>>(),
                        "leaseMs": lease_ms,
                    });
                    let until = chunk
                        .iter()
                        .filter_map(|(slot, _)| held.get(slot).map(|entry| entry.deadline))
                        .min();
                    match client.mutate_with(&options.db, &options.module, &renew, &body, options.call(until)).await {
                        Ok(Value::Array(expiries)) => {
                            for ((slot, _), expiry) in chunk.iter().zip(expiries) {
                                let Some(entry) = held.get_mut(slot) else { continue };
                                if expiry.is_null() {
                                    entry.lost = true;
                                    entry.stop.notify_one();
                                } else {
                                    entry.deadline = sent + usable;
                                }
                            }
                        }
                        Ok(_) => {
                            failed = Some(FlowerError::Decode("renew returned no list".to_string()));
                            break;
                        }
                        Err(error) if permanent(&error) => {
                            failed = Some(error);
                            break;
                        }
                        // Deadlines stop the computations if renewals
                        // keep failing.
                        Err(_) => {}
                    }
                }
                if let Some(error) = failed {
                    break Err(error);
                }
            }
            _ = adjustments.tick(), if !limiter.is_fixed() => {
                let pressed = waiting.is_none() && running.len() >= limiter.limit();
                if pressed {
                    limiter.want();
                }
                if let Some(change) = limiter.adjust() {
                    options.emit(ReconcileEvent::Limit(change));
                }
                let (min, max) = limiter.bounds();
                let (load, reason) = limiter
                    .last_load()
                    .map_or((0.0, String::new()), |load| (load.load, load.reason.clone()));
                let said = PoolLoad {
                    load,
                    reason,
                    limit: limiter.limit(),
                    min,
                    max,
                    in_use: running.len(),
                    waiting: pressed,
                    throttle: limiter.state(),
                };
                if gate.due(Instant::now(), &said) {
                    options.emit(ReconcileEvent::Load(said));
                }
            }
            () = sleep_until(deadline) => {
                // Too close to the lease's end: stop, and let another
                // process compute the key once the lease runs out.
                let now = Instant::now();
                for entry in held.values_mut() {
                    if !entry.lost && entry.deadline <= now {
                        entry.lost = true;
                        entry.stop.notify_one();
                    }
                }
            }
            waited = ready_or_pending(&mut waiting) => {
                waiting = None;
                match waited {
                    Ok(_) => {}
                    Err(error) if permanent(&error) => break Err(error),
                    Err(error) => {
                        options.emit(ReconcileEvent::Waiting {
                            error: error.to_string(),
                        });
                        if pause(stop, options.poll_interval).await {
                            break Ok(());
                        }
                    }
                }
            }
        }
    };
    // On the way out: drop the computations unpublished and hand every
    // key still held back once, so others need not wait out the lease.
    drop(running);
    for entry in held.values().filter(|entry| !entry.lost) {
        release_lease(client, options, &entry.lease, Duration::ZERO, None).await;
    }
    outcome
}

/// Most computations a leased pool runs at once: one renewal call
/// covers them all.
const MAX_LEASED: usize = 1024;
/// Most leases one renewal names.
const MAX_RENEWALS: usize = 1024;

/// Sleep until `at`, or forever without one.
async fn sleep_until(at: Option<Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn work_parses_and_rejects_malformed_items() {
        assert_eq!(ExternalWork::parse(Value::Null).unwrap(), None);
        let work = ExternalWork::parse(serde_json::json!({
            "args": "a", "key": "[\"a\",1]", "input": 1
        }))
        .unwrap()
        .unwrap();
        assert_eq!(work.args, "a");
        assert_eq!(work.key, "[\"a\",1]");
        assert!(ExternalWork::parse(serde_json::json!({"args": "a"})).is_err());
        assert!(ExternalWork::parse(serde_json::json!(3)).is_err());
    }

    /// One call a scripted client saw: its method, its retry policy (as
    /// text) and whether it had a deadline; `None` when it came through
    /// plain `mutate`, without per-call options.
    type Seen = (String, Option<(String, bool)>);

    /// A client with one pending work item and nothing claimable,
    /// recording every mutation.
    #[derive(Clone, Default)]
    struct Scripted {
        calls: Arc<std::sync::Mutex<Vec<Seen>>>,
        waits: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl ReconcileClient for Scripted {
        async fn query(
            &self,
            _db: &str,
            _module: &str,
            _name: &str,
            _args: &Value,
        ) -> Result<Value, FlowerError> {
            Ok(Value::Array(Vec::new()))
        }

        async fn mutate(
            &self,
            _db: &str,
            _module: &str,
            name: &str,
            _args: &Value,
        ) -> Result<Value, FlowerError> {
            self.calls.lock().unwrap().push((name.to_string(), None));
            Ok(if name.ends_with(".claim") {
                Value::Array(Vec::new())
            } else {
                Value::Bool(true)
            })
        }

        async fn mutate_with(
            &self,
            _db: &str,
            _module: &str,
            name: &str,
            _args: &Value,
            options: MutationOptions,
        ) -> Result<Value, FlowerError> {
            let seen = (
                format!("{:?}", options.base.retry),
                options.base.deadline.is_some(),
            );
            self.calls
                .lock()
                .unwrap()
                .push((name.to_string(), Some(seen)));
            Ok(if name.ends_with(".claim") {
                Value::Array(Vec::new())
            } else {
                Value::Bool(true)
            })
        }

        async fn wait_until(
            &self,
            _db: &str,
            _module: &str,
            name: &str,
            _args: &Value,
            _predicate: fn(&Value) -> bool,
        ) -> Result<Value, FlowerError> {
            let first = self.waits.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0;
            if first && name.ends_with(".pending") {
                return Ok(serde_json::json!({"args": "a", "key": "[\"a\",1]", "input": 1}));
            }
            std::future::pending().await
        }
    }

    /// Run `options` against a scripted client until it made `calls`
    /// mutations, then stop.
    async fn calls_seen(options: ReconcileOptions, calls: usize) -> Vec<Seen> {
        let client = Scripted::default();
        let stop = Arc::new(Notify::new());
        let run = tokio::spawn(reconcile(
            client.clone(),
            options,
            stop.clone(),
            |_| async { Ok(Value::Bool(true)) },
        ));
        for _ in 0..500 {
            if client.calls.lock().unwrap().len() >= calls {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        stop.notify_one();
        run.await.unwrap().unwrap();
        client.calls.lock().unwrap().clone()
    }

    #[tokio::test]
    async fn publications_take_the_per_call_retry_policy() {
        let options = ReconcileOptions::new("db", "app", "words")
            .args(serde_json::json!("a"))
            .call_retry(flower_client::retry::Retry::Off);
        let seen = calls_seen(options, 1).await;
        assert_eq!(
            seen[0],
            (
                "words.publish".to_string(),
                Some(("Some(Off)".to_string(), false))
            )
        );
    }

    #[tokio::test]
    async fn leased_claims_carry_the_policy_and_their_lease_as_deadline() {
        let options = ReconcileOptions::new("db", "app", "pool")
            .leased()
            .call_retry(flower_client::retry::Retry::Default);
        let seen = calls_seen(options, 1).await;
        assert_eq!(
            seen[0],
            (
                "pool.claim".to_string(),
                Some(("Some(Default)".to_string(), true))
            )
        );
    }
}
