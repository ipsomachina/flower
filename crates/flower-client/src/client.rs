//! The native client: unary calls with retry, idempotency and
//! typed JSON helpers.
//!
//! Any cluster member serves any call: queries read as they declare,
//! and members forward writes to the leader. With
//! [`Builder::leader_routing`] the client sends writes straight to the
//! leader each reply names, saving that hop.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio_util::sync::CancellationToken;

use crate::error::FlowerError;
use crate::retry::{Retry, RetryPolicy, retry_delay};
use crate::transport::{
    Credentials, Headers, HttpTransport, TIMEOUT_HEADER, Transport, TransportOptions,
};

/// Default per-attempt timeout.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(20);
/// Reply header naming the leader's API URL.
const LEADER_URL_HEADER: &str = "x-flower-leader-url";
/// Request header bounding a replica-local query's staleness.
const MAX_STALENESS_HEADER: &str = "x-flower-max-staleness-ms";
/// Reply header reporting a replica-local answer's staleness.
const STALENESS_HEADER: &str = "x-flower-staleness-ms";

/// How stale a replica-local answer may have been, as the serving
/// replica reported.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Staleness {
    /// At most this stale; zero when the replica proved a fresh read.
    Known(Duration),
    /// The replica did not know.
    Unknown,
}

impl Staleness {
    /// Parse the reply header.
    fn parse(value: &str) -> Option<Self> {
        if value == "unknown" {
            return Some(Staleness::Unknown);
        }
        value
            .parse()
            .ok()
            .map(|ms| Staleness::Known(Duration::from_millis(ms)))
    }
}

/// A query result: the decoded value.
#[derive(Clone, Debug)]
pub struct QueryResult<T> {
    /// Decoded value.
    pub value: T,
    /// How stale a replica-local answer may have been; `None` for
    /// linearizable answers, which are fresh.
    pub staleness: Option<Staleness>,
    /// Scope and history of the evaluated state, when supported by the peer.
    pub read_token: Option<flower_protocol::ReadToken>,
}

/// A mutation result: the decoded value plus ordering counters.
#[derive(Clone, Debug)]
pub struct MutationResult<T> {
    /// Decoded value.
    pub value: T,
    /// Committed revision.
    pub revision: u64,
    /// Applied log position (0 when served without replicating).
    pub position: u64,
    /// Whether the server answered from a stored receipt: this request ID
    /// committed earlier, and the answer is that outcome (with its
    /// revision and position), not a second run.
    pub duplicate: bool,
    /// Stable request identity, also available from prepared commands.
    pub request_id: String,
    pub read_token: Option<flower_protocol::ReadToken>,
}

/// Per-call options: retry, timeout and credential overrides, plus a
/// staleness bound for replica-local queries.
#[derive(Clone, Debug, Default)]
pub struct RequestOptions {
    /// Retry selection override.
    pub retry: Option<Retry>,
    /// Attempt timeout override.
    pub timeout: Option<Duration>,
    /// Credential override.
    pub credentials: Option<Credentials>,
    /// Most staleness a replica-local query may be answered with: a
    /// replica past it proves a fresh read first, or refuses with a
    /// retryable `stale_replica`. Whole milliseconds; ignored by
    /// linearizable queries.
    pub max_staleness: Option<Duration>,
    /// Whole-call deadline: each attempt's timeout is cut to what is
    /// left, no retry starts past it, and the server learns the time
    /// left (`x-flower-timeout-ms`). A call whose deadline has passed
    /// fails with [`FlowerError::Timeout`] without sending.
    pub deadline: Option<Instant>,
    /// Cancellation: once cancelled, the attempt in flight is dropped,
    /// no retry starts and the call fails with
    /// [`FlowerError::Cancelled`].
    pub cancel: Option<CancellationToken>,
    /// Require a result that includes this committed state.
    pub at_least: Option<flower_protocol::ReadToken>,
}

impl RequestOptions {
    /// Default options.
    pub fn new() -> Self {
        RequestOptions::default()
    }

    /// Options with the default retry policy.
    pub fn retrying() -> Self {
        RequestOptions {
            retry: Some(Retry::Default),
            ..RequestOptions::default()
        }
    }

    pub fn at_least(mut self, token: flower_protocol::ReadToken) -> Self {
        self.at_least = Some(token);
        self
    }

    /// Bound a replica-local query's staleness.
    pub fn max_staleness(mut self, bound: Duration) -> Self {
        self.max_staleness = Some(bound);
        self
    }

    /// Set retry selection.
    pub fn retry(mut self, retry: impl Into<Retry>) -> Self {
        self.retry = Some(retry.into());
        self
    }

    /// Set the attempt timeout.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Set the credential override.
    pub fn credentials(mut self, credentials: Credentials) -> Self {
        self.credentials = Some(credentials);
        self
    }

    /// Set the whole-call deadline.
    pub fn deadline(mut self, deadline: Instant) -> Self {
        self.deadline = Some(deadline);
        self
    }

    /// Cancel the call when `token` is cancelled.
    pub fn cancel(mut self, token: CancellationToken) -> Self {
        self.cancel = Some(token);
        self
    }
}

/// Per-mutation options: a stable request identity plus the base
/// call options. The identity and expected revision stay fixed
/// across the retry loop and are never refreshed silently.
#[derive(Clone, Debug, Default)]
pub struct MutationOptions {
    /// Stable request identity; a UUID v4 when absent.
    pub request_id: Option<String>,
    /// Revision the submission was written against.
    pub expected_revision: Option<u64>,
    /// Base call options.
    pub base: RequestOptions,
}

impl MutationOptions {
    /// Default options.
    pub fn new() -> Self {
        MutationOptions::default()
    }

    /// Options with the default retry policy.
    pub fn retrying() -> Self {
        MutationOptions {
            base: RequestOptions::retrying(),
            ..MutationOptions::default()
        }
    }

    /// Set retry selection.
    pub fn retry(mut self, retry: impl Into<Retry>) -> Self {
        self.base.retry = Some(retry.into());
        self
    }

    /// Set the stable request identity.
    pub fn request_id(mut self, id: impl Into<String>) -> Self {
        self.request_id = Some(id.into());
        self
    }

    /// Set the expected revision.
    pub fn expected_revision(mut self, revision: u64) -> Self {
        self.expected_revision = Some(revision);
        self
    }

    /// Set the attempt timeout.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.base.timeout = Some(timeout);
        self
    }

    /// Set the credential override.
    pub fn credentials(mut self, credentials: Credentials) -> Self {
        self.base.credentials = Some(credentials);
        self
    }

    /// Set the whole-call deadline.
    pub fn deadline(mut self, deadline: Instant) -> Self {
        self.base.deadline = Some(deadline);
        self
    }

    /// Cancel the call when `token` is cancelled.
    pub fn cancel(mut self, token: CancellationToken) -> Self {
        self.base.cancel = Some(token);
        self
    }
}

pub(crate) type CallResponse = flower_protocol::CallEnvelope;

/// The `ok`/`code`/`message`/`leader` prefix every JSON envelope
/// shares: calls, queue and timer operations, admin operations and
/// error bodies. Unknown fields are ignored.
#[derive(serde::Deserialize, Clone, Debug, Default)]
pub(crate) struct Envelope {
    /// Whether the operation business-succeeded.
    pub(crate) ok: bool,
    /// Failure code.
    #[serde(default)]
    pub(crate) code: Option<String>,
    /// Failure message.
    #[serde(default)]
    pub(crate) message: Option<String>,
    /// Forward leader, on forwards.
    #[serde(default)]
    pub(crate) leader: Option<u64>,
    /// Failure details: only a call's own failure carries any.
    #[serde(default)]
    pub(crate) details: Option<serde_json::Value>,
}

/// HTTP verb for [`send`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Method {
    /// GET: the query rides the path; the body is ignored.
    Get,
    /// POST with a JSON body.
    Post,
}

/// Map a non-success status outside the retry set to an error,
/// preferring the envelope's code and message. A 422 is the call's own
/// failure, a business failure; every other status is the server's.
pub(crate) fn status_error(status: u16, body: &[u8]) -> FlowerError {
    let problem = flower_protocol::Problem::from_body(status, body, None);
    if status == 422
        && let Ok(envelope) = serde_json::from_slice::<Envelope>(body)
        && !envelope.ok
        && envelope.code.is_some()
        && envelope.message.is_some()
    {
        return FlowerError::Business {
            code: problem.code,
            message: problem.message,
            revision: None,
            position: None,
            details: problem.details,
        };
    }
    FlowerError::Http(Box::new(problem))
}

/// Whether a body is a call's failure envelope: `{ok: false, code, ...}`.
fn call_envelope(body: &[u8]) -> bool {
    serde_json::from_slice::<Envelope>(body)
        .is_ok_and(|envelope| !envelope.ok && envelope.code.is_some())
}

/// Decode a 200 body whose envelope reports `ok`: business failures
/// surface as [`FlowerError::Business`], successes decode as `T`.
pub(crate) fn decode_ok<T: DeserializeOwned>(body: &[u8]) -> Result<T, FlowerError> {
    let envelope: Envelope = serde_json::from_slice(body)
        .map_err(|error| FlowerError::Decode(format!("envelope: {error}")))?;
    if !envelope.ok {
        if envelope.code.is_none() || envelope.message.is_none() {
            return Err(FlowerError::Decode(
                "failed envelope has no code or message".into(),
            ));
        }
        return Err(FlowerError::Business {
            code: envelope.code.unwrap_or_else(|| "error".to_string()),
            message: envelope.message.unwrap_or_default(),
            revision: None,
            position: None,
            details: envelope.details,
        });
    }
    serde_json::from_slice(body).map_err(|error| FlowerError::Decode(format!("body: {error}")))
}

/// One request as the retry loop sends it.
pub(crate) struct Call<'a> {
    /// HTTP verb.
    pub(crate) method: Method,
    /// Path and query string.
    pub(crate) path: &'a str,
    /// JSON body; ignored by GET.
    pub(crate) body: &'a [u8],
    /// Extra request headers; POST only.
    pub(crate) headers: &'a [(&'static str, String)],
    /// Credentials, if any.
    pub(crate) credentials: Option<&'a Credentials>,
    /// The database the call names, which leader routing keys on: each
    /// database's owner group has a leader of its own.
    pub(crate) db: Option<&'a str>,
}

/// A 200 reply: its body plus its `x-flower-*` headers.
pub(crate) struct Reply {
    /// Response body.
    pub(crate) body: Vec<u8>,
    /// Response headers the transport reported.
    pub(crate) headers: Headers,
}

/// Where each database's leader serves its API, as the last reply for it
/// named it, for routing writes there: a database's leader is the leader
/// of the group that owns it, which differs between partitions.
pub(crate) struct LeaderRouting {
    options: TransportOptions,
    leaders: Mutex<Leaders>,
}

/// Known leaders by database (`None` for calls naming none), the
/// transports to them by URL, the last one learned, and the URL last
/// forgotten with when: replies naming it are not followed again for
/// [`FORGOTTEN_LEADER`].
#[derive(Default)]
struct Leaders {
    by_db: HashMap<Option<String>, String>,
    transports: HashMap<String, Arc<dyn Transport>>,
    last: Option<String>,
    forgotten: Option<(String, Instant)>,
}

/// How long a leader URL that failed is not routed to again, though
/// replies name it: other members go on naming a dead leader until
/// they notice its silence, and dialing a dead host can take seconds.
const FORGOTTEN_LEADER: Duration = Duration::from_secs(1);

/// The longest a routed attempt waits to connect to the leader: a dead
/// leader's host may never answer (and a refused dial takes seconds on
/// some systems), while the main URL forwards to whichever leader
/// there is. A live one connects within a round trip or a TLS handshake.
const ROUTED_CONNECT_TIMEOUT: Duration = Duration::from_millis(500);

impl LeaderRouting {
    fn new(options: TransportOptions) -> Self {
        LeaderRouting {
            options,
            leaders: Mutex::new(Leaders::default()),
        }
    }

    /// The URL of `db`'s leader and a transport to it, if known.
    fn current(&self, db: Option<&str>) -> Option<(String, Arc<dyn Transport>)> {
        let leaders = self.leaders.lock().unwrap_or_else(PoisonError::into_inner);
        let url = leaders.by_db.get(&db.map(str::to_string))?;
        let transport = leaders.transports.get(url)?;
        Some((url.clone(), transport.clone()))
    }

    /// The leader learned last, for any database.
    fn last(&self) -> Option<String> {
        self.leaders
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .last
            .clone()
    }

    /// Follow the leader a reply for `db` named, if it named an HTTP
    /// URL that did not just fail. The transport to it shares the main
    /// transport's settings (protocol, pools, extra roots) but connects
    /// within [`ROUTED_CONNECT_TIMEOUT`], and every database it leads
    /// shares the transport.
    fn learn(&self, db: Option<&str>, headers: &Headers) {
        let Some((_, url)) = headers.iter().find(|(name, _)| name == LEADER_URL_HEADER) else {
            return;
        };
        if !(url.starts_with("http://") || url.starts_with("https://")) {
            return;
        }
        let mut leaders = self.leaders.lock().unwrap_or_else(PoisonError::into_inner);
        if leaders
            .forgotten
            .as_ref()
            .is_some_and(|(failed, at)| failed == url && at.elapsed() < FORGOTTEN_LEADER)
        {
            return;
        }
        if !leaders.transports.contains_key(url) {
            let options = self.options.clone();
            let connect = options.connect_timeout.min(ROUTED_CONNECT_TIMEOUT);
            let Ok(transport) =
                HttpTransport::with_options(url.clone(), options.connect_timeout(connect))
            else {
                return;
            };
            leaders.transports.insert(url.clone(), Arc::new(transport));
        }
        leaders.by_db.insert(db.map(str::to_string), url.clone());
        leaders.last = Some(url.clone());
    }

    /// Stop routing to `url`: it stopped leading or answering. Writes of
    /// every database it led go through the main URL until a reply
    /// names another leader, or this one again after
    /// [`FORGOTTEN_LEADER`].
    fn forget(&self, url: &str) {
        let mut leaders = self.leaders.lock().unwrap_or_else(PoisonError::into_inner);
        leaders.by_db.retain(|_, known| known != url);
        leaders.transports.remove(url);
        if leaders.last.as_deref() == Some(url) {
            leaders.last = None;
        }
        leaders.forgotten = Some((url.to_string(), Instant::now()));
    }
}

/// What bounds one call's retry loop besides its policy.
pub(crate) struct Bounds<'a> {
    /// Timeout of each attempt, before the deadline cuts it.
    pub(crate) attempt_timeout: Duration,
    /// Whole-call deadline, if any.
    pub(crate) deadline: Option<Instant>,
    /// Caller cancellation, if any.
    pub(crate) cancel: Option<&'a CancellationToken>,
    /// Counts replies refusing a request for load, if given.
    pub(crate) overloads: Option<&'a AtomicU64>,
}

impl Bounds<'_> {
    /// Attempts under `attempt_timeout` with no deadline, cancellation or
    /// overload count.
    #[cfg(feature = "replica")]
    pub(crate) fn timeout(attempt_timeout: Duration) -> Bounds<'static> {
        Bounds {
            attempt_timeout,
            deadline: None,
            cancel: None,
            overloads: None,
        }
    }
}

/// [`send_call`] for one request, through one transport.
#[cfg(feature = "replica")]
pub(crate) async fn send(
    transport: &dyn Transport,
    method: Method,
    path: &str,
    body: &[u8],
    credentials: Option<&Credentials>,
    policy: Option<&RetryPolicy>,
    attempt_timeout: Duration,
) -> Result<Vec<u8>, FlowerError> {
    let call = Call {
        method,
        path,
        body,
        headers: &[],
        credentials,
        db: None,
    };
    send_call(
        transport,
        None,
        &call,
        policy,
        &Bounds::timeout(attempt_timeout),
    )
    .await
    .map(|reply| reply.body)
}

/// Resolve `future` unless `cancel` fires first.
async fn cancellable<T>(
    cancel: Option<&CancellationToken>,
    future: impl Future<Output = T>,
) -> Result<T, FlowerError> {
    match cancel {
        None => Ok(future.await),
        Some(cancel) => tokio::select! {
            biased;
            () = cancel.cancelled() => Err(FlowerError::Cancelled),
            output = future => Ok(output),
        },
    }
}

/// The one retry loop behind every unary call. Each attempt runs
/// under the attempt timeout, cut to what is left of the call's
/// deadline, and tells the server that time (`x-flower-timeout-ms`).
/// Transport failures, timeouts and overload statuses (429, 502, 504
/// and non-forward 503) retry with backoff while the policy allows,
/// waiting at least what `Retry-After` asks plus up to half again.
/// Refusals for load (429, 503 `overloaded`) never ran the request:
/// with `through_overload` they do not count against the attempts and
/// retry until the policy's `until` or its overload budget. Every 200
/// returns its body for the caller to decode, so business failures,
/// including deterministic ones such as `stale_revision`, never retry;
/// forwards, auth refusals and other statuses fail at once. The
/// request body stays fixed across attempts. Cancellation drops the
/// attempt in flight or the wait and fails with
/// [`FlowerError::Cancelled`].
///
/// With `routing`, every reply naming the leader's URL updates it, and
/// each attempt of a write (any POST but a query) goes there while it
/// is known. An attempt the leader failed to take (a transport failure,
/// a timeout, 502, 504, a 503 other than overload, or a forward: it
/// stopped leading) forgets it, and the next attempt goes through the
/// main URL, whose member forwards to whichever member leads.
pub(crate) async fn send_call(
    main: &dyn Transport,
    routing: Option<&LeaderRouting>,
    call: &Call<'_>,
    policy: Option<&RetryPolicy>,
    bounds: &Bounds<'_>,
) -> Result<Reply, FlowerError> {
    let attempts = policy.map(|policy| policy.attempts.max(1)).unwrap_or(1);
    let write = call.method == Method::Post && call.path != "/v1/query";
    // Tokio's clock, so paused-time tests see the budget pass.
    let deadline = bounds.deadline.map(tokio::time::Instant::from_std);
    let overload_until = tokio::time::Instant::now()
        + policy.map_or(Duration::ZERO, |policy| policy.overload_budget);
    // `attempt` grows the backoff with every retry; `failed` is what the
    // attempt cap counts: refusals for load do not when the policy goes
    // on through them.
    let (mut attempt, mut failed) = (0u32, 0u32);
    let mut last = None;
    let mut uncertain = false;
    loop {
        if bounds.cancel.is_some_and(CancellationToken::is_cancelled) {
            return Err(FlowerError::Cancelled);
        }
        let mut timeout = bounds.attempt_timeout;
        if let Some(deadline) = deadline {
            let left = deadline.saturating_duration_since(tokio::time::Instant::now());
            if left.is_zero() {
                return Err(last.unwrap_or(FlowerError::Timeout));
            }
            timeout = timeout.min(left);
        }
        let mut headers = call.headers.to_vec();
        let milliseconds = timeout.as_millis().clamp(1, u128::from(u64::MAX)) as u64;
        headers.push((TIMEOUT_HEADER, milliseconds.to_string()));
        let routed = routing
            .filter(|_| write)
            .and_then(|routing| routing.current(call.db));
        let transport = routed
            .as_ref()
            .map_or(main, |(_, transport)| transport.as_ref());
        let request = async {
            match call.method {
                Method::Post => {
                    transport
                        .post_with_headers(call.path, call.body, call.credentials, &headers)
                        .await
                }
                Method::Get => {
                    transport
                        .get_with_headers(call.path, call.credentials, &headers)
                        .await
                }
            }
        };
        let outcome = cancellable(bounds.cancel, tokio::time::timeout(timeout, request)).await?;
        if let (Some(routing), Ok(Ok((_, headers)))) = (routing, &outcome) {
            routing.learn(call.db, headers);
        }
        let (error, retry_after_ms, misrouted, overloaded) = match outcome {
            Err(_) => (FlowerError::Timeout, None, true, false),
            Ok(Err(error)) if error.retryable() => (error, None, true, false),
            Ok(Err(error)) => return Err(error),
            Ok(Ok((response, headers))) => match response.status {
                // A call's own failure is an answer too: its envelope
                // says `ok: false`, and decoding it makes it a business
                // failure, never retried. A 422 that is no envelope
                // (a proxy's, a framework's) is the server's failure.
                200 => {
                    return Ok(Reply {
                        body: response.body,
                        headers,
                    });
                }
                422 if call_envelope(&response.body) => {
                    return Ok(Reply {
                        body: response.body,
                        headers,
                    });
                }
                429 | 502 | 504 => (
                    FlowerError::Http(Box::new(flower_protocol::Problem::from_body(
                        response.status,
                        &response.body,
                        response.retry_after_ms,
                    ))),
                    response.retry_after_ms,
                    response.status != 429,
                    response.status == 429,
                ),
                503 => {
                    // Forwards name a leader and never retry on one
                    // URL; other 503s (quorum, overload) retry.
                    let envelope: Envelope =
                        serde_json::from_slice(&response.body).unwrap_or_default();
                    if envelope.code.as_deref() == Some("forward") {
                        let forward = FlowerError::Forward {
                            leader: envelope.leader,
                        };
                        if routed.is_none() {
                            return Err(forward);
                        }
                        // The routed leader stopped leading: the next
                        // attempt goes through the main URL.
                        (forward, None, true, false)
                    } else {
                        let overloaded = envelope.code.as_deref() == Some("overloaded");
                        // The refusal says why (`KEYS_LOCKED`, say): the
                        // error ending the retries keeps it.
                        let problem = flower_protocol::Problem::from_body(
                            503,
                            &response.body,
                            response.retry_after_ms,
                        );
                        if !problem.retryable {
                            return Err(FlowerError::Http(Box::new(problem)));
                        }
                        (
                            FlowerError::Http(Box::new(problem)),
                            response.retry_after_ms,
                            !overloaded,
                            overloaded,
                        )
                    }
                }
                401 | 403 => {
                    return Err(status_error(response.status, &response.body));
                }
                status => return Err(status_error(status, &response.body)),
            },
        };
        if overloaded && let Some(overloads) = bounds.overloads {
            overloads.fetch_add(1, Ordering::Relaxed);
        }
        uncertain |= error.execution() == flower_protocol::Execution::Unknown;
        // A routed attempt that failed tells about the route, not the
        // cluster: the next attempt goes through the main URL at once.
        let rerouted = routed.is_some() && misrouted;
        if let (Some(routing), Some((url, _)), true) = (routing, &routed, misrouted) {
            routing.forget(url);
        }
        let Some(policy) = policy else {
            return Err(error);
        };
        let through = overloaded && policy.through_overload;
        if !through {
            failed += 1;
        }
        if failed >= attempts {
            let execution = if uncertain {
                flower_protocol::Execution::Unknown
            } else {
                error.execution()
            };
            return Err(FlowerError::RetryFailed {
                last: Box::new(error),
                attempts: attempt + 1,
                reason: crate::error::RetryStop::Attempts,
                execution,
            });
        }
        let delay = if rerouted {
            Duration::ZERO
        } else {
            retry_delay(policy, attempt, retry_after_ms)
        };
        // Refusals for load wait out their own budget when the policy
        // sets no `until`.
        let until = match policy.until.map(tokio::time::Instant::from_std) {
            None if through => Some(overload_until),
            until => until,
        };
        let resume = tokio::time::Instant::now() + delay;
        if until.is_some_and(|until| resume >= until)
            || deadline.is_some_and(|deadline| resume >= deadline)
        {
            let reason = if deadline.is_some_and(|deadline| resume >= deadline) {
                crate::error::RetryStop::Deadline
            } else {
                crate::error::RetryStop::OverloadBudget
            };
            let execution = if uncertain {
                flower_protocol::Execution::Unknown
            } else {
                error.execution()
            };
            return Err(FlowerError::RetryFailed {
                last: Box::new(error),
                attempts: attempt + 1,
                reason,
                execution,
            });
        }
        cancellable(bounds.cancel, tokio::time::sleep(delay)).await?;
        last = Some(error);
        attempt += 1;
    }
}

/// The native client over any transport.
#[derive(Clone)]
pub struct FlowerClient {
    pub(crate) transport: Arc<dyn Transport>,
    pub(crate) credentials: Option<Credentials>,
    pub(crate) retry: Retry,
    pub(crate) timeout: Duration,
    /// Leader routing, when enabled.
    pub(crate) routing: Option<Arc<LeaderRouting>>,
    /// Replies refusing this client's calls for load, shared by clones.
    pub(crate) overloads: Arc<AtomicU64>,
}

/// Client builder.
pub struct Builder {
    base_url: Option<String>,
    transport: Option<Arc<dyn Transport>>,
    transport_options: TransportOptions,
    credentials: Option<Credentials>,
    retry: Retry,
    timeout: Duration,
    leader_routing: bool,
}

impl FlowerClient {
    /// Build a client for a base URL (scheme plus authority).
    pub fn builder(base_url: impl Into<String>) -> Builder {
        Builder {
            base_url: Some(base_url.into()),
            transport: None,
            transport_options: TransportOptions::default(),
            credentials: None,
            retry: Retry::Off,
            timeout: DEFAULT_TIMEOUT,
            leader_routing: false,
        }
    }

    /// How many replies so far refused this client's calls (or its
    /// clones') for load (429, or 503 `overloaded`), retried attempts
    /// included. Workers hold back while it grows.
    pub fn overloads(&self) -> u64 {
        self.overloads.load(Ordering::Relaxed)
    }

    /// With leader routing, where the last reply said the leader's API
    /// serves; writes go there while it answers.
    pub fn leader(&self) -> Option<String> {
        self.routing.as_ref().and_then(|routing| routing.last())
    }

    /// Dynamic-JSON query. A replica-local query's answer reports how
    /// stale it may have been; `options.max_staleness` bounds it.
    pub async fn query_value(
        &self,
        db: &str,
        module: &str,
        name: &str,
        args: &serde_json::Value,
        options: &RequestOptions,
    ) -> Result<QueryResult<serde_json::Value>, FlowerError> {
        let body = serde_json::to_value(flower_protocol::request::QueryRequest::new(
            db,
            module,
            name,
            args.clone(),
        ))
        .map_err(|e| FlowerError::Decode(e.to_string()))?;
        let mut headers: Vec<(&'static str, String)> = options
            .max_staleness
            .map(|bound| (MAX_STALENESS_HEADER, bound.as_millis().to_string()))
            .into_iter()
            .collect();
        if let Some(token) = &options.at_least {
            if token.database != db {
                return Err(FlowerError::Decode(
                    "read token belongs to another database".into(),
                ));
            }
            headers.push((
                flower_protocol::AT_LEAST_HEADER,
                token.encode().map_err(FlowerError::Decode)?,
            ));
        }
        let reply = self
            .send_with(Method::Post, "/v1/query", Some(&body), &headers, options)
            .await?;
        let response = decode_call(&reply.body)?;
        response.validate(false).map_err(FlowerError::Decode)?;
        let mut staleness = reply
            .headers
            .iter()
            .find(|(name, _)| name == STALENESS_HEADER)
            .and_then(|(_, value)| Staleness::parse(value));
        let fresh = reply.headers.iter().any(|(name, value)| {
            name == flower_protocol::CONSISTENCY_HEADER && value == "linearizable"
        });
        if staleness.is_none() && !fresh {
            staleness = Some(Staleness::Unknown);
        }
        let read_token = reply
            .headers
            .iter()
            .find(|(name, _)| name == flower_protocol::TOKEN_HEADER)
            .map(|(_, value)| {
                flower_protocol::ReadToken::decode(value).map_err(FlowerError::Decode)
            })
            .transpose()?;
        if let Some(minimum) = &options.at_least {
            let observed = read_token.as_ref().ok_or_else(|| {
                FlowerError::Unsupported("peer did not acknowledge a causal read".into())
            })?;
            if observed.database != minimum.database || observed.revision < minimum.revision {
                return Err(FlowerError::Decode(
                    "peer violated causal read bound".into(),
                ));
            }
        }
        Ok(QueryResult {
            value: response.value.expect("validated value"),
            staleness,
            read_token,
        })
    }

    /// Dynamic-JSON mutation.
    pub async fn mutate_value(
        &self,
        db: &str,
        module: &str,
        name: &str,
        args: &serde_json::Value,
        options: &MutationOptions,
    ) -> Result<MutationResult<serde_json::Value>, FlowerError> {
        crate::options::require_command_options(&options.base)?;
        let request_id = options
            .request_id
            .clone()
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let body = serde_json::to_value(flower_protocol::request::MutationRequest {
            call: flower_protocol::request::QueryRequest::new(db, module, name, args.clone()),
            idempotency_key: Some(request_id.clone()),
            expected_revision: options.expected_revision,
        })
        .map_err(|e| FlowerError::Decode(e.to_string()))?;
        let response = self.execute("/v1/mutate", &body, &options.base).await?;
        response.validate(true).map_err(FlowerError::Decode)?;
        Ok(MutationResult {
            value: response.value.expect("validated value"),
            revision: response.revision.unwrap_or(0),
            position: response.position.unwrap_or(0),
            duplicate: response.duplicate,
            request_id,
            read_token: response.read_token,
        })
    }

    /// Typed query over serde data.
    pub async fn query<A: Serialize + ?Sized, T: DeserializeOwned>(
        &self,
        db: &str,
        module: &str,
        name: &str,
        args: &A,
        options: &RequestOptions,
    ) -> Result<QueryResult<T>, FlowerError> {
        let args =
            serde_json::to_value(args).map_err(|error| FlowerError::Decode(error.to_string()))?;
        let result = self.query_value(db, module, name, &args, options).await?;
        let value = serde_json::from_value(result.value)
            .map_err(|error| FlowerError::Decode(error.to_string()))?;
        Ok(QueryResult {
            value,
            staleness: result.staleness,
            read_token: result.read_token,
        })
    }

    /// Typed mutation over serde data.
    pub async fn mutate<A: Serialize + ?Sized, T: DeserializeOwned>(
        &self,
        db: &str,
        module: &str,
        name: &str,
        args: &A,
        options: &MutationOptions,
    ) -> Result<MutationResult<T>, FlowerError> {
        let args =
            serde_json::to_value(args).map_err(|error| FlowerError::Decode(error.to_string()))?;
        let result = self.mutate_value(db, module, name, &args, options).await?;
        let value = serde_json::from_value(result.value)
            .map_err(|error| FlowerError::Decode(error.to_string()))?;
        Ok(MutationResult {
            value,
            revision: result.revision,
            position: result.position,
            duplicate: result.duplicate,
            request_id: result.request_id,
            read_token: result.read_token,
        })
    }

    /// Run one request through [`send`] with this client's resolved
    /// retry policy, attempt timeout and credentials. The attempt
    /// timeout is the per-call override, else an explicit policy's
    /// `attempt_timeout`, else the client default.
    pub(crate) async fn send(
        &self,
        method: Method,
        path: &str,
        body: Option<&serde_json::Value>,
        options: &RequestOptions,
    ) -> Result<Vec<u8>, FlowerError> {
        self.send_with(method, path, body, &[], options)
            .await
            .map(|reply| reply.body)
    }

    /// [`FlowerClient::send`] with extra request headers, returning the
    /// reply's headers too. Writes go to the leader when routing by
    /// leader (see [`send_call`]).
    pub(crate) async fn send_with(
        &self,
        method: Method,
        path: &str,
        body: Option<&serde_json::Value>,
        headers: &[(&'static str, String)],
        options: &RequestOptions,
    ) -> Result<Reply, FlowerError> {
        let retry = options.retry.as_ref().unwrap_or(&self.retry);
        let policy = retry.policy();
        let timeout = options
            .timeout
            .or_else(|| retry.attempt_timeout())
            .unwrap_or(self.timeout);
        let credentials = options.credentials.as_ref().or(self.credentials.as_ref());
        let bytes = match body {
            Some(body) => {
                serde_json::to_vec(body).map_err(|error| FlowerError::Decode(error.to_string()))?
            }
            None => Vec::new(),
        };
        // The database the call names, or a transaction's first step's:
        // its owner group's leader is where its writes go.
        let db = body.and_then(|body| {
            body.get("db")
                .or_else(|| body.get("steps")?.get(0)?.get("db"))?
                .as_str()
        });
        let call = Call {
            method,
            path,
            body: &bytes,
            headers,
            credentials,
            db,
        };
        let bounds = Bounds {
            attempt_timeout: timeout,
            deadline: options.deadline,
            cancel: options.cancel.as_ref(),
            overloads: Some(&self.overloads),
        };
        send_call(
            self.transport.as_ref(),
            self.routing.as_deref(),
            &call,
            policy.as_ref(),
            &bounds,
        )
        .await
    }

    /// POST a call through the retry loop and decode its envelope:
    /// business failures surface as [`FlowerError::Business`].
    async fn execute(
        &self,
        path: &str,
        body: &serde_json::Value,
        options: &RequestOptions,
    ) -> Result<CallResponse, FlowerError> {
        let bytes = self.send(Method::Post, path, Some(body), options).await?;
        decode_call(&bytes)
    }
}

/// Decode a call envelope: business failures surface as
/// [`FlowerError::Business`].
fn decode_call(bytes: &[u8]) -> Result<CallResponse, FlowerError> {
    let call: CallResponse = serde_json::from_slice(bytes)
        .map_err(|error| FlowerError::Decode(format!("call envelope: {error}")))?;
    finish(call)
}

/// A server's status.
#[derive(serde::Deserialize, Clone, Debug)]
pub struct ServerStatus {
    /// Node ID.
    pub id: u64,
    /// Server state name.
    pub state: String,
    /// Current leader, if known.
    pub leader: Option<u64>,
    /// Current term.
    pub term: u64,
    /// Last appended log index.
    pub last_log: Option<u64>,
    /// Last applied log index.
    pub last_applied: Option<u64>,
}

/// A deployment's outcome.
#[derive(Clone, Debug)]
pub struct DeployOutcome {
    /// Current database revision.
    pub revision: u64,
    /// Applied log position.
    pub position: u64,
}

/// A triggered backup's outcome.
#[derive(serde::Deserialize, Clone, Debug)]
pub struct BackupOutcome {
    /// Opaque backup ID.
    pub backup_id: String,
    /// Database views uploaded.
    pub objects: usize,
    /// Group applied index covered.
    pub covered_index: u64,
}

impl FlowerClient {
    /// Deploy a module through consensus (admin credentials).
    /// Single attempt by default; redelivery is idempotent by
    /// identity, so retries are safe to enable.
    pub async fn deploy(
        &self,
        db: &str,
        module: &str,
        identity: Option<[u8; 32]>,
        wasm: &[u8],
        manifest: &serde_json::Value,
        options: &RequestOptions,
    ) -> Result<DeployOutcome, FlowerError> {
        use base64::Engine;
        let identity = identity.map(|bytes| {
            bytes
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        });
        let body = serde_json::json!({
            "db": db,
            "module": module,
            "identity": identity,
            "wasm_b64": base64::prelude::BASE64_STANDARD.encode(wasm),
            "manifest": manifest,
        });
        let response = self.execute("/v1/deploy", &body, options).await?;
        response.validate(true).map_err(FlowerError::Decode)?;
        Ok(DeployOutcome {
            revision: response.revision.unwrap_or(0),
            position: response.position.unwrap_or(0),
        })
    }

    /// Report server status.
    pub async fn status(&self, options: &RequestOptions) -> Result<ServerStatus, FlowerError> {
        self.get_json("/v1/status", options).await
    }

    /// Change cluster membership (admin credentials): member IDs
    /// with dial addresses (empty for in-process).
    pub async fn membership(
        &self,
        members: &std::collections::BTreeMap<u64, String>,
        retain: bool,
        options: &RequestOptions,
    ) -> Result<(), FlowerError> {
        let members: Vec<String> = members
            .iter()
            .map(|(id, addr)| {
                if addr.is_empty() {
                    id.to_string()
                } else {
                    format!("{id}={addr}")
                }
            })
            .collect();
        let body = serde_json::json!({"members": members, "retain": retain});
        self.execute("/v1/admin/membership", &body, options).await?;
        Ok(())
    }

    /// Trigger a full backup (admin credentials). The summary rides
    /// the top level of the `ok` envelope.
    pub async fn backup(&self, options: &RequestOptions) -> Result<BackupOutcome, FlowerError> {
        let body = self
            .send(
                Method::Post,
                "/v1/admin/backup",
                Some(&serde_json::json!({})),
                options,
            )
            .await?;
        decode_ok(&body)
    }

    /// GET through the retry loop, decoding JSON.
    async fn get_json<T: DeserializeOwned>(
        &self,
        path: &str,
        options: &RequestOptions,
    ) -> Result<T, FlowerError> {
        let body = self.send(Method::Get, path, None, options).await?;
        serde_json::from_slice(&body).map_err(|error| FlowerError::Decode(error.to_string()))
    }
}

/// Finish a 200 call envelope: success decodes, business failure errors.
fn finish(call: CallResponse) -> Result<CallResponse, FlowerError> {
    if call.ok {
        Ok(call)
    } else {
        call.validate(false).map_err(FlowerError::Decode)?;
        Err(FlowerError::Business {
            code: call.code.unwrap_or_else(|| "business".to_string()),
            message: call.message.unwrap_or_default(),
            revision: call.revision,
            position: call.position,
            details: call.details,
        })
    }
}

impl Builder {
    /// Set client credentials.
    pub fn credentials(mut self, credentials: Credentials) -> Self {
        self.credentials = Some(credentials);
        self
    }

    /// Set retry selection.
    pub fn retry(mut self, retry: impl Into<Retry>) -> Self {
        self.retry = retry.into();
        self
    }

    /// Set the default attempt timeout.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Use a custom transport instead of HTTP.
    pub fn transport(mut self, transport: Arc<dyn Transport>) -> Self {
        self.transport = Some(transport);
        self
    }

    /// Set the HTTP transport's options: protocol (HTTP/1.1 or h2c over
    /// plain TCP; HTTPS negotiates h2 by ALPN), connection pools, extra
    /// CA roots. Ignored with a custom transport.
    pub fn transport_options(mut self, options: TransportOptions) -> Self {
        self.transport_options = options;
        self
    }

    /// Trust the PEM roots in this file for HTTPS, besides the
    /// platform's.
    pub fn ca_file(mut self, path: impl Into<std::path::PathBuf>) -> Self {
        self.transport_options.ca_files.push(path.into());
        self
    }

    /// Send writes (every POST but queries) straight to the leader:
    /// each reply names the leader's advertised API URL
    /// (`x-flower-leader-url`), and later writes go there instead of
    /// through the main URL's member, which would forward them. A write
    /// the leader fails to take (it stopped answering or leading) falls
    /// back to the main URL on its next attempt. Reads keep the main
    /// URL. The client must reach the leader's advertised URL; HTTP
    /// clients only, as a custom transport has no URL to route by. Off
    /// by default.
    pub fn leader_routing(mut self, on: bool) -> Self {
        self.leader_routing = on;
        self
    }

    /// Build the client.
    pub fn build(self) -> Result<FlowerClient, FlowerError> {
        let routable = self.transport.is_none();
        let transport = match (self.transport, self.base_url) {
            (Some(transport), _) => transport,
            (None, Some(url)) => Arc::new(HttpTransport::with_options(
                url,
                self.transport_options.clone(),
            )?),
            (None, None) => {
                return Err(FlowerError::Transport("no URL or transport".to_string()));
            }
        };
        let routing = (self.leader_routing && routable)
            .then(|| Arc::new(LeaderRouting::new(self.transport_options)));
        Ok(FlowerClient {
            transport,
            credentials: self.credentials,
            retry: self.retry,
            timeout: self.timeout,
            routing,
            overloads: Arc::new(AtomicU64::new(0)),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn named(url: &str) -> Headers {
        vec![(LEADER_URL_HEADER.to_string(), url.to_string())]
    }

    #[test]
    fn leader_routing_keeps_a_leader_per_database() {
        let routing = LeaderRouting::new(TransportOptions::default());
        routing.learn(Some("acme"), &named("http://127.0.0.1:1"));
        routing.learn(Some("plain"), &named("http://127.0.0.1:2"));
        routing.learn(Some("other"), &named("http://127.0.0.1:1"));
        // Each database's writes go to the leader of its own group.
        assert_eq!(
            routing.current(Some("acme")).map(|(url, _)| url).as_deref(),
            Some("http://127.0.0.1:1")
        );
        assert_eq!(
            routing
                .current(Some("plain"))
                .map(|(url, _)| url)
                .as_deref(),
            Some("http://127.0.0.1:2")
        );
        assert!(routing.current(Some("unknown")).is_none());
        assert_eq!(routing.last().as_deref(), Some("http://127.0.0.1:1"));
        // A leader that stopped answering is forgotten for every database
        // it led, and only those.
        routing.forget("http://127.0.0.1:1");
        assert!(routing.current(Some("acme")).is_none());
        assert!(routing.current(Some("other")).is_none());
        assert!(routing.current(Some("plain")).is_some());
    }
}
