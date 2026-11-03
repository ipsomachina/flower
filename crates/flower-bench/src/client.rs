//! The bench's client: one pooled h2c transport per member and a retry
//! loop that accounts for every attempt.
//!
//! `flower_client::FlowerClient` retries inside one call and reports
//! only the outcome; a benchmark must count each attempt, each retry
//! and each error by kind, route reads across replicas and know which
//! member answered, so this module drives the client crate's
//! [`HttpTransport`] directly (as v1's Rust driver drove reqwest).
//!
//! - **Bodies are fixed.** A call's request body is serialized once;
//!   every attempt sends the same bytes, so an uncertain mutation
//!   retries under the same request ID (`idempotency_key`) and the
//!   server answers a retry that already applied from its receipt.
//! - **Reads rotate** over every member not cooling down after a
//!   failure. Replica-local reads answer from the member's own state;
//!   fresh reads prove freshness on whichever member serves them.
//! - **Writes go to the leader** each reply names
//!   (`x-flower-leader-url`), falling back to the next member, which
//!   forwards; a failed write triggers one coalesced `/v1/status`
//!   round to find the leader, as v1's driver did with `/raft/metrics`.
//!   A member whose connection failed is passed over for a second,
//!   even when replies still name it leader: dialing a dead member
//!   again can take seconds, while the next member parks the write
//!   until a new leader takes it.
//! - **Outcomes:** a 200 envelope with `ok` is a success; a 422 one
//!   (`ok: false`) a decided business failure, never retried. Transport
//!   failures, timeouts, 429, 502, 503 and 504 retry until the retry
//!   budget ends, after which the call is *uncertain*: it may have
//!   applied. Other statuses fail at once.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use flower_client::transport::{
    Credentials, HttpTransport, Protocol, TIMEOUT_HEADER, Transport, TransportOptions,
};
use flower_client::{FlowerClient, Retry};
use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use serde::Serialize;
use serde_json::Value;

use crate::auth::Identities;
use crate::metrics::{Class, Recorder};

/// Reply header naming the leader's API URL.
const LEADER_URL_HEADER: &str = "x-flower-leader-url";
/// Reply header saying where the request's time went on the server.
const SERVER_TIMING: &str = "server-timing";
/// Longest pause between retries, and the longest cooldown.
const MAX_PAUSE: Duration = Duration::from_millis(250);
/// Discovery rounds closer together than this coalesce.
const DISCOVERY_SPACING: Duration = Duration::from_millis(25);
/// How long one member may take to answer a discovery round's status
/// request: a live member answers at once, and waiting longer on a dead
/// one (a refused dial takes seconds on some hosts) holds every write
/// back.
const DISCOVERY_TIMEOUT: Duration = Duration::from_millis(250);
/// How long a member whose connection failed is passed over, writes and
/// replies naming it as leader included: dialing a dead member again
/// can take seconds.
const DOWN: Duration = Duration::from_secs(1);

/// One cluster member's API.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Member {
    /// Raft node ID.
    pub id: u64,
    /// API base URL, no trailing slash.
    pub url: String,
}

/// A successful answer.
#[derive(Clone, Debug, PartialEq)]
pub struct Answer {
    /// The call's value.
    pub value: Value,
    /// Committed revision (mutations).
    pub revision: Option<u64>,
    /// Applied log position (mutations).
    pub position: Option<u64>,
    /// Index of the member that answered.
    pub member: usize,
}

/// Why a call failed.
#[derive(Clone, Debug, PartialEq)]
pub enum CallError {
    /// The call ran and failed with this business code (or replayed
    /// such a failure from its receipt). Decided: never retried.
    Business {
        /// Failure code.
        code: String,
        /// Failure message.
        message: String,
    },
    /// The server refused the request without running it (a status
    /// outside the retry set). Decided.
    Refused {
        /// Error kind, as the error counters name it.
        kind: String,
        /// Detail.
        message: String,
    },
    /// Every attempt failed retryably until the budget ran out: the
    /// call may or may not have applied.
    Uncertain {
        /// The last attempt's error kind.
        kind: String,
    },
}

impl CallError {
    /// The kind the error counters use.
    pub fn kind(&self) -> String {
        match self {
            CallError::Business { code, .. } => format!("business:{code}"),
            CallError::Refused { kind, .. } => kind.clone(),
            CallError::Uncertain { kind } => format!("retry-exhausted:{kind}"),
        }
    }

    /// Whether the outcome is unknown.
    pub fn is_uncertain(&self) -> bool {
        matches!(self, CallError::Uncertain { .. })
    }
}

impl std::fmt::Display for CallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CallError::Business { code, message } => write!(f, "{code}: {message}"),
            CallError::Refused { kind, message } => write!(f, "{kind}: {message}"),
            CallError::Uncertain { kind } => {
                write!(f, "retry budget exhausted, outcome unknown (last: {kind})")
            }
        }
    }
}

/// Which endpoint a call uses.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Endpoint {
    /// `POST /v1/query`, routed across replicas.
    Query,
    /// `POST /v1/mutate`, routed to the leader.
    Mutate,
}

/// A serialized call: the exact bytes every attempt sends.
#[derive(Clone, Debug)]
pub struct Request {
    /// Which endpoint.
    pub endpoint: Endpoint,
    /// Method alias, for messages.
    pub name: String,
    /// The body.
    pub body: Arc<Vec<u8>>,
    /// Sent with the admin token: a trusted gateway asserting the
    /// principal in the body (`--auth` setup).
    pub gateway: bool,
}

/// Whom a call acts for. Without `--auth` every call is anonymous and
/// this only documents intent; with it, each carries its caller's
/// credentials in the body (`credentials: {bearer}`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Who<'a> {
    /// No credentials.
    Anyone,
    /// A tenant's customer (or a drone delivering for it).
    Tenant(&'a str),
    /// The kitchens' administrator, by token.
    Admin,
    /// The deployer as a trusted gateway: the server's admin token and
    /// an asserted administrator principal. Opens the kitchens before
    /// the kitchen trusts any key.
    Gateway,
}

/// Per-member routing counters.
#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RouteCounters {
    /// Member ID.
    pub id: u64,
    /// Member URL.
    pub url: String,
    /// Attempts sent to it.
    pub attempts: u64,
    /// Attempts it answered with a decided outcome.
    pub answered: u64,
    /// Attempts that failed retryably there.
    pub failures: u64,
}

struct Routing {
    leader: usize,
    cursor: usize,
    cooldown: Vec<Option<Instant>>,
    /// Until when each member is passed over after its connection
    /// failed (see [`DOWN`]).
    down: Vec<Option<Instant>>,
    discovered: Option<Instant>,
    counters: Vec<RouteCounters>,
}

/// Watching for the first write a member other than the killed one
/// acknowledges, in an attempt sent after the kill.
#[derive(Clone, Copy, Debug)]
struct AckWatch {
    kill: Instant,
    killed: usize,
    first: Option<(Instant, usize)>,
}

/// Retry and timeout settings.
#[derive(Clone, Copy, Debug)]
pub struct Timing {
    /// One attempt's timeout.
    pub request_timeout: Duration,
    /// A logical call's whole retry budget.
    pub retry_budget: Duration,
}

/// The bench's client: one Raft group's members, its leader and its
/// accounting.
pub struct BenchClient {
    members: Vec<Member>,
    transports: Vec<Arc<HttpTransport>>,
    admin: Option<Credentials>,
    worker: Option<Credentials>,
    timing: Timing,
    group: Option<String>,
    db: String,
    module: String,
    recorder: Arc<Recorder>,
    identities: Option<Arc<Identities>>,
    route: Mutex<Routing>,
    discovery: tokio::sync::Mutex<()>,
    sequence: AtomicU64,
    run_tag: String,
    acks: Mutex<Option<AckWatch>>,
}

/// Credentials the bench holds.
#[derive(Clone, Debug, Default)]
pub struct Tokens {
    /// Admin bearer: deployment, setup, status, metrics.
    pub admin: Option<String>,
    /// Worker bearer: the drones' queue calls.
    pub worker: Option<String>,
}

/// What a client talks to.
#[derive(Clone, Debug)]
pub struct ClientSpec {
    /// The members.
    pub members: Vec<Member>,
    /// The server tokens.
    pub tokens: Tokens,
    /// Retry and timeout settings.
    pub timing: Timing,
    /// The Raft group whose leader writes go to and whose status the
    /// client reads (`?group=`); `None` is the root.
    pub group: Option<String>,
    /// The database calls go to unless they name another.
    pub db: String,
    /// The module.
    pub module: String,
    /// Prefix of request IDs: unique per run and client.
    pub run_tag: String,
    /// End-user credentials, with `--auth`.
    pub identities: Option<Arc<Identities>>,
}

impl BenchClient {
    /// A client for `spec`'s members, calling `spec.module` in
    /// `spec.db` unless told otherwise. Request IDs start with
    /// `spec.run_tag`, so they never collide across runs or clients.
    pub fn new(spec: ClientSpec, recorder: Arc<Recorder>) -> Result<Self, String> {
        let ClientSpec {
            members,
            tokens,
            timing,
            group,
            db,
            module,
            run_tag,
            identities,
        } = spec;
        let options = TransportOptions::default()
            .protocol(Protocol::H2c)
            .connect_timeout(Duration::from_secs(1));
        let transports = members
            .iter()
            .map(|member| {
                HttpTransport::with_options(member.url.clone(), options.clone())
                    .map(Arc::new)
                    .map_err(|error| format!("{}: {error}", member.url))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let counters = members
            .iter()
            .map(|member| RouteCounters {
                id: member.id,
                url: member.url.clone(),
                ..RouteCounters::default()
            })
            .collect();
        Ok(BenchClient {
            route: Mutex::new(Routing {
                leader: 0,
                cursor: 0,
                cooldown: vec![None; members.len()],
                down: vec![None; members.len()],
                discovered: None,
                counters,
            }),
            members,
            transports,
            admin: tokens.admin.clone().map(Credentials::token),
            worker: tokens.worker.clone().map(Credentials::token),
            timing,
            group,
            db,
            module,
            recorder,
            identities,
            discovery: tokio::sync::Mutex::new(()),
            sequence: AtomicU64::new(0),
            run_tag,
            acks: Mutex::new(None),
        })
    }

    /// The members.
    pub fn members(&self) -> &[Member] {
        &self.members
    }

    /// The database calls go to unless they name another.
    pub fn db(&self) -> &str {
        &self.db
    }

    /// The Raft group this client follows; `None` is the root.
    pub fn group(&self) -> Option<&str> {
        self.group.as_deref()
    }

    /// End-user credentials, with `--auth`.
    pub fn identities(&self) -> Option<&Arc<Identities>> {
        self.identities.as_ref()
    }

    /// The bearer a caller presents, with `--auth`.
    pub fn bearer_for(&self, who: Who<'_>) -> Option<String> {
        let identities = self.identities.as_ref()?;
        match who {
            Who::Anyone | Who::Gateway => None,
            Who::Tenant(tenant) => identities.tenant(tenant).map(str::to_string),
            Who::Admin => Some(identities.admin().to_string()),
        }
    }

    /// The module calls go to.
    pub fn module(&self) -> &str {
        &self.module
    }

    /// The recorder.
    pub fn recorder(&self) -> &Arc<Recorder> {
        &self.recorder
    }

    /// A fresh request ID.
    pub fn next_id(&self) -> String {
        let sequence = self.sequence.fetch_add(1, Ordering::Relaxed) + 1;
        format!("{}-{sequence}", self.run_tag)
    }

    /// Serialize a query of the default database, anonymously.
    pub fn query_request(&self, name: &str, args: &Value) -> Request {
        self.query(&self.db, name, args, Who::Anyone)
    }

    /// Serialize a mutation of the default database under a request ID,
    /// anonymously.
    pub fn mutate_request(&self, name: &str, args: &Value, request_id: &str) -> Request {
        self.mutate(&self.db, name, args, request_id, Who::Anyone)
    }

    /// Serialize a query of `db` for `who`.
    pub fn query(&self, db: &str, name: &str, args: &Value, who: Who<'_>) -> Request {
        let mut body = serde_json::json!({
            "db": db,
            "module": self.module,
            "name": name,
            "args": args,
        });
        let gateway = self.authorize(&mut body, who);
        Request {
            endpoint: Endpoint::Query,
            name: name.to_string(),
            body: Arc::new(serde_json::to_vec(&body).expect("JSON values serialize")),
            gateway,
        }
    }

    /// Serialize a mutation of `db` under a request ID, for `who`.
    pub fn mutate(
        &self,
        db: &str,
        name: &str,
        args: &Value,
        request_id: &str,
        who: Who<'_>,
    ) -> Request {
        let mut body = serde_json::json!({
            "db": db,
            "module": self.module,
            "name": name,
            "args": args,
            "idempotency_key": request_id,
        });
        let gateway = self.authorize(&mut body, who);
        Request {
            endpoint: Endpoint::Mutate,
            name: name.to_string(),
            body: Arc::new(serde_json::to_vec(&body).expect("JSON values serialize")),
            gateway,
        }
    }

    /// Put `who`'s credentials in a call body, with `--auth`; returns
    /// whether the call goes as a trusted gateway.
    fn authorize(&self, body: &mut Value, who: Who<'_>) -> bool {
        if self.identities.is_none() {
            return false;
        }
        if who == Who::Gateway {
            body["principal"] = Identities::gateway_principal();
            return true;
        }
        if let Some(token) = self.bearer_for(who) {
            body["credentials"] = crate::auth::bearer(&token);
        }
        false
    }

    /// Run one logical call through the retry loop, recording it as
    /// `op` of `class`. Its latency runs from `started`, which is the
    /// intended arrival in open-loop mode.
    pub async fn call(
        &self,
        op: &str,
        class: Class,
        request: &Request,
        started: Instant,
    ) -> Result<Answer, CallError> {
        let result = self.attempts(op, class, request, None).await;
        self.recorder.call(
            op,
            class,
            started.elapsed().as_secs_f64() * 1000.0,
            result.is_ok(),
        );
        result
    }

    /// [`BenchClient::call`] on one member only: the audit reads each
    /// replica's own state this way.
    pub async fn call_on(
        &self,
        member: usize,
        op: &str,
        class: Class,
        request: &Request,
    ) -> Result<Answer, CallError> {
        let started = Instant::now();
        let result = self.attempts(op, class, request, Some(member)).await;
        self.recorder.call(
            op,
            class,
            started.elapsed().as_secs_f64() * 1000.0,
            result.is_ok(),
        );
        result
    }

    async fn attempts(
        &self,
        op: &str,
        class: Class,
        request: &Request,
        pinned: Option<usize>,
    ) -> Result<Answer, CallError> {
        let first = Instant::now();
        let deadline = first + self.timing.retry_budget;
        let mut tries = 0u32;
        let mut excluded = vec![false; self.members.len()];
        let mut last = String::from("no member reachable");
        while Instant::now() < deadline {
            let target = match pinned {
                Some(member) => {
                    if tries > 0 {
                        pause(tries, deadline).await;
                    }
                    Some(member)
                }
                None => self.target(request.endpoint, &excluded),
            };
            let Some(member) = target else {
                pause(tries, deadline).await;
                excluded.fill(false);
                continue;
            };
            let leader = self.leader();
            let sent = Instant::now();
            let timeout = self
                .timing
                .request_timeout
                .min(deadline.saturating_duration_since(sent));
            let (outcome, stages) = self.attempt(member, request, timeout).await;
            let ms = sent.elapsed().as_secs_f64() * 1000.0;
            let retry = tries > 0;
            tries += 1;
            self.recorder.stages(op, class, &stages);
            match outcome {
                Attempt::Answered(answer) => {
                    self.recorder.attempt(op, class, ms, None, retry);
                    self.count(member, true);
                    if request.endpoint == Endpoint::Mutate {
                        self.acknowledged(sent, member);
                    }
                    return Ok(answer);
                }
                Attempt::Decided(error) => {
                    // A business failure is an answer, not a transport
                    // failure: the member served the call.
                    self.recorder
                        .attempt(op, class, ms, Some(&error.kind()), retry);
                    self.count(member, true);
                    return Err(error);
                }
                Attempt::Retry(kind) => {
                    self.recorder.attempt(op, class, ms, Some(&kind), retry);
                    self.count(member, false);
                    self.cool(member, tries);
                    if matches!(kind.as_str(), "timeout" | "connect-refused" | "transport") {
                        self.mark_down(member);
                    }
                    excluded[member] = true;
                    last = kind;
                    if request.endpoint == Endpoint::Mutate {
                        self.discover(leader, deadline).await;
                        if self.leader() == leader {
                            pause(tries, deadline).await;
                        }
                    }
                    if excluded.iter().all(|excluded| *excluded) {
                        excluded.fill(false);
                        pause(tries, deadline).await;
                    }
                }
            }
        }
        Err(CallError::Uncertain { kind: last })
    }

    /// One attempt to one member.
    /// One attempt to one member: its outcome, and the stages the
    /// answering server reported (`Server-Timing`).
    async fn attempt(
        &self,
        member: usize,
        request: &Request,
        timeout: Duration,
    ) -> (Attempt, Vec<(String, f64)>) {
        let path = match request.endpoint {
            Endpoint::Query => "/v1/query",
            Endpoint::Mutate => "/v1/mutate",
        };
        let headers = [(
            TIMEOUT_HEADER,
            timeout
                .as_millis()
                .clamp(1, u128::from(u64::MAX))
                .to_string(),
        )];
        let transport = &self.transports[member];
        let credentials = if request.gateway {
            self.admin.as_ref()
        } else {
            None
        };
        let sent = tokio::time::timeout(
            timeout,
            transport.post_with_headers(path, &request.body, credentials, &headers),
        )
        .await;
        let (response, headers) = match sent {
            Err(_) => return (Attempt::Retry("timeout".to_string()), Vec::new()),
            Ok(Err(error)) => return (Attempt::Retry(transport_kind(&error)), Vec::new()),
            Ok(Ok(reply)) => reply,
        };
        if let Some((_, url)) = headers.iter().find(|(name, _)| name == LEADER_URL_HEADER) {
            self.learn(url);
        }
        let stages = headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(SERVER_TIMING))
            .map(|(_, timing)| crate::metrics::server_timing(timing))
            .unwrap_or_default();
        (classify(response.status, &response.body, member), stages)
    }

    /// The member an attempt goes to: the leader for writes while it is
    /// not cooling down, else the next member in rotation.
    fn target(&self, endpoint: Endpoint, excluded: &[bool]) -> Option<usize> {
        let mut route = self.route.lock().unwrap_or_else(PoisonError::into_inner);
        let now = Instant::now();
        let ready = |route: &Routing, index: usize| {
            !route.cooldown[index].is_some_and(|until| until > now)
                && !route.down[index].is_some_and(|until| until > now)
        };
        if endpoint == Endpoint::Mutate && !excluded[route.leader] && ready(&route, route.leader) {
            return Some(route.leader);
        }
        let count = self.members.len();
        for offset in 0..count {
            let index = (route.cursor + offset) % count;
            if excluded[index] || !ready(&route, index) {
                continue;
            }
            route.cursor = (index + 1) % count;
            return Some(index);
        }
        None
    }

    /// The member writes currently go to.
    pub fn leader(&self) -> usize {
        self.route
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .leader
    }

    /// Follow the leader a reply named, unless its member is down: other
    /// members go on naming a dead leader until they notice its silence.
    fn learn(&self, url: &str) {
        let url = url.trim_end_matches('/');
        if let Some(index) = self.members.iter().position(|member| member.url == url) {
            let mut route = self.route.lock().unwrap_or_else(PoisonError::into_inner);
            if !route.down[index].is_some_and(|until| until > Instant::now()) {
                route.leader = index;
            }
        }
    }

    /// Pass member `member` over for [`DOWN`] after its connection
    /// failed, routing writes to the next member meanwhile, which
    /// forwards them to whichever leader it finds.
    fn mark_down(&self, member: usize) {
        let now = Instant::now();
        let mut route = self.route.lock().unwrap_or_else(PoisonError::into_inner);
        route.down[member] = Some(now + DOWN);
        if route.leader == member {
            let count = self.members.len();
            if let Some(next) = (1..count)
                .map(|offset| (member + offset) % count)
                .find(|index| !route.down[*index].is_some_and(|until| until > now))
            {
                route.leader = next;
            }
        }
    }

    fn count(&self, member: usize, answered: bool) {
        let mut route = self.route.lock().unwrap_or_else(PoisonError::into_inner);
        let counters = &mut route.counters[member];
        counters.attempts += 1;
        if answered {
            counters.answered += 1;
        } else {
            counters.failures += 1;
        }
    }

    fn cool(&self, member: usize, tries: u32) {
        let until = Instant::now() + backoff(tries);
        self.route
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .cooldown[member] = Some(until);
    }

    /// Find the leader through `/v1/status`, unless another caller just
    /// did or the leader already moved from `attempted`.
    async fn discover(&self, attempted: usize, deadline: Instant) {
        let wait = deadline.saturating_duration_since(Instant::now());
        let Ok(_guard) = tokio::time::timeout(wait, self.discovery.lock()).await else {
            return;
        };
        {
            let route = self.route.lock().unwrap_or_else(PoisonError::into_inner);
            if route.leader != attempted
                || route
                    .discovered
                    .is_some_and(|at| at.elapsed() < DISCOVERY_SPACING)
            {
                return;
            }
        }
        let timeout = self
            .timing
            .request_timeout
            .min(DISCOVERY_TIMEOUT)
            .min(deadline.saturating_duration_since(Instant::now()));
        if let Some(leader) = self.find_leader(timeout).await {
            self.route
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .leader = leader;
        }
        self.route
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .discovered = Some(Instant::now());
    }

    /// Ask every member for its status at once; the first that says it
    /// leads wins.
    pub async fn find_leader(&self, timeout: Duration) -> Option<usize> {
        let mut pending: FuturesUnordered<_> = (0..self.members.len())
            .map(|index| async move {
                let status = self.status(index, timeout).await.ok()?;
                (status.state == "Leader" && status.leader == Some(status.id)).then_some(index)
            })
            .collect();
        while let Some(found) = pending.next().await {
            if found.is_some() {
                return found;
            }
        }
        None
    }

    /// One member's `/v1/status` for this client's group.
    pub async fn status(
        &self,
        member: usize,
        timeout: Duration,
    ) -> Result<flower_client::client::ServerStatus, String> {
        self.status_in(member, self.group.as_deref(), timeout).await
    }

    /// One admin `POST` to one member, answered with its status and
    /// JSON body (operator calls the client crate does not name a group
    /// for, such as a group's leadership transfer).
    pub async fn admin_post(
        &self,
        member: usize,
        path: &str,
        body: &Value,
        timeout: Duration,
    ) -> Result<(u16, Value), String> {
        let bytes = serde_json::to_vec(body).map_err(|error| error.to_string())?;
        let reply = tokio::time::timeout(
            timeout,
            self.transports[member].post(path, &bytes, self.admin.as_ref()),
        )
        .await
        .map_err(|_| "timeout".to_string())?
        .map_err(|error| error.to_string())?;
        let value = serde_json::from_slice(&reply.body).unwrap_or(Value::Null);
        Ok((reply.status, value))
    }

    /// One member's `/v1/status` for `group` (`None`: the root).
    pub async fn status_in(
        &self,
        member: usize,
        group: Option<&str>,
        timeout: Duration,
    ) -> Result<flower_client::client::ServerStatus, String> {
        let path = match group {
            Some(group) => format!("/v1/status?group={group}"),
            None => "/v1/status".to_string(),
        };
        let reply = tokio::time::timeout(timeout, self.transports[member].get(&path, None))
            .await
            .map_err(|_| "timeout".to_string())?
            .map_err(|error| error.to_string())?;
        if reply.status != 200 {
            return Err(format!("status {}", reply.status));
        }
        serde_json::from_slice(&reply.body).map_err(|error| error.to_string())
    }

    /// A `FlowerClient` for one member: admin credentials, one attempt
    /// per call, the request timeout.
    pub fn admin_client(&self, member: usize) -> FlowerClient {
        self.flower_client(member, self.admin.clone())
    }

    /// A `FlowerClient` for one member without credentials, as a
    /// customer's (watches).
    pub fn anonymous_client(&self, member: usize) -> FlowerClient {
        self.flower_client(member, None)
    }

    /// A `FlowerClient` for one member with the worker credentials.
    pub fn worker_client(&self, member: usize) -> FlowerClient {
        self.flower_client(member, self.worker.clone().or_else(|| self.admin.clone()))
    }

    fn flower_client(&self, member: usize, credentials: Option<Credentials>) -> FlowerClient {
        let transport: Arc<dyn Transport> = self.transports[member].clone();
        let mut builder = FlowerClient::builder(self.members[member].url.clone())
            .transport(transport)
            .retry(Retry::Off)
            .timeout(self.timing.request_timeout);
        if let Some(credentials) = credentials {
            builder = builder.credentials(credentials);
        }
        builder.build().expect("a custom transport always builds")
    }

    /// Mark a member as failed for routing purposes (a queue call to it
    /// failed), so the next calls try another.
    pub fn report_failure(&self, member: usize) {
        self.cool(member, 1);
        let mut route = self.route.lock().unwrap_or_else(PoisonError::into_inner);
        if route.leader == member {
            route.leader = (member + 1) % self.members.len();
        }
    }

    /// The member worker calls go to: the leader unless cooling down.
    pub fn worker_target(&self) -> usize {
        let excluded = vec![false; self.members.len()];
        self.target(Endpoint::Mutate, &excluded)
            .unwrap_or_else(|| self.leader())
    }

    /// Routing counters per member.
    pub fn routing(&self) -> Vec<RouteCounters> {
        self.route
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .counters
            .clone()
    }

    /// Start watching for the first write acknowledged after `kill` by a
    /// member other than `killed`.
    pub fn watch_acks(&self, kill: Instant, killed: usize) {
        *self.acks.lock().unwrap_or_else(PoisonError::into_inner) = Some(AckWatch {
            kill,
            killed,
            first: None,
        });
    }

    /// The first such acknowledgement: when, and by which member.
    pub fn first_ack(&self) -> Option<(Instant, usize)> {
        self.acks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .and_then(|watch| watch.first)
    }

    fn acknowledged(&self, sent: Instant, member: usize) {
        let mut acks = self.acks.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(watch) = acks.as_mut()
            && watch.first.is_none()
            && sent > watch.kill
            && member != watch.killed
        {
            watch.first = Some((Instant::now(), member));
        }
    }
}

/// One attempt's outcome.
enum Attempt {
    Answered(Answer),
    Decided(CallError),
    Retry(String),
}

/// The call envelope every 200 carries.
#[derive(serde::Deserialize, Default)]
struct Envelope {
    #[serde(default)]
    ok: bool,
    #[serde(default)]
    value: Option<Value>,
    #[serde(default)]
    revision: Option<u64>,
    #[serde(default)]
    position: Option<u64>,
    #[serde(default)]
    code: Option<String>,
    #[serde(default)]
    message: Option<String>,
}

/// Map a reply to an attempt outcome.
fn classify(status: u16, body: &[u8], member: usize) -> Attempt {
    let envelope = serde_json::from_slice::<Envelope>(body);
    match status {
        200 | 422 => match envelope {
            Ok(envelope) if envelope.ok => Attempt::Answered(Answer {
                value: envelope.value.unwrap_or(Value::Null),
                revision: envelope.revision,
                position: envelope.position,
                member,
            }),
            Ok(envelope) => Attempt::Decided(CallError::Business {
                code: envelope.code.unwrap_or_else(|| "business".to_string()),
                message: envelope.message.unwrap_or_default(),
            }),
            Err(error) => Attempt::Decided(CallError::Refused {
                kind: "invalid-response".to_string(),
                message: error.to_string(),
            }),
        },
        429 | 502 | 503 | 504 => {
            let code = envelope.ok().and_then(|envelope| envelope.code);
            Attempt::Retry(match code {
                Some(code) => format!("http-{status}:{code}"),
                None => format!("http-{status}"),
            })
        }
        // Refused credentials, or an authorization hook's denial, whose
        // message carries the hook's own code (`FORBIDDEN`,
        // `UNAUTHENTICATED`).
        401 | 403 => {
            let envelope = envelope.unwrap_or_default();
            Attempt::Decided(CallError::Refused {
                kind: match envelope.code {
                    Some(code) => format!("http-{status}:{code}"),
                    None => format!("http-{status}"),
                },
                message: envelope
                    .message
                    .unwrap_or_else(|| "credentials refused".to_string()),
            })
        }
        _ => {
            let envelope = envelope.unwrap_or_default();
            Attempt::Decided(CallError::Refused {
                kind: match envelope.code {
                    Some(code) => format!("http-{status}:{code}"),
                    None => format!("http-{status}"),
                },
                message: envelope.message.unwrap_or_default(),
            })
        }
    }
}

/// A transport failure's kind: refused connections (a dead member)
/// apart from the rest.
fn transport_kind(error: &flower_client::FlowerError) -> String {
    let text = error.to_string().to_ascii_lowercase();
    if text.contains("refused") || text.contains("10061") {
        "connect-refused".to_string()
    } else {
        "transport".to_string()
    }
}

/// Retry backoff: 25 ms per attempt so far, at most [`MAX_PAUSE`].
fn backoff(tries: u32) -> Duration {
    Duration::from_millis(25 * u64::from(tries.max(1))).min(MAX_PAUSE)
}

async fn pause(tries: u32, deadline: Instant) {
    let wait = backoff(tries).min(deadline.saturating_duration_since(Instant::now()));
    tokio::time::sleep(wait).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replies_classify_by_status_and_envelope() {
        let ok = br#"{"ok":true,"value":{"tips":3},"revision":7,"position":9}"#;
        match classify(200, ok, 2) {
            Attempt::Answered(answer) => {
                assert_eq!(answer.value["tips"], 3);
                assert_eq!(answer.revision, Some(7));
                assert_eq!(answer.position, Some(9));
                assert_eq!(answer.member, 2);
            }
            _ => panic!("expected an answer"),
        }
        let failed = br#"{"ok":false,"code":"OUT_OF_STOCK","message":"no dough"}"#;
        assert!(matches!(
            classify(422, failed, 0),
            Attempt::Decided(CallError::Business { ref code, .. }) if code == "OUT_OF_STOCK"
        ));
        let forward = br#"{"ok":false,"code":"forward_failed","message":"?"}"#;
        assert!(matches!(
            classify(503, forward, 0),
            Attempt::Retry(ref kind) if kind == "http-503:forward_failed"
        ));
        assert!(matches!(classify(504, b"", 0), Attempt::Retry(ref kind) if kind == "http-504"));
        assert!(matches!(classify(429, b"{}", 0), Attempt::Retry(_)));
        assert!(matches!(
            classify(404, br#"{"ok":false,"code":"UNKNOWN_METHOD"}"#, 0),
            Attempt::Decided(CallError::Refused { ref kind, .. }) if kind == "http-404:UNKNOWN_METHOD"
        ));
        assert!(matches!(
            classify(403, b"", 0),
            Attempt::Decided(CallError::Refused { .. })
        ));
        assert!(matches!(
            classify(200, b"not json", 0),
            Attempt::Decided(CallError::Refused { .. })
        ));
    }

    #[test]
    fn errors_name_their_kind() {
        assert_eq!(
            CallError::Business {
                code: "X".into(),
                message: String::new()
            }
            .kind(),
            "business:X"
        );
        let uncertain = CallError::Uncertain {
            kind: "timeout".into(),
        };
        assert!(uncertain.is_uncertain());
        assert_eq!(uncertain.kind(), "retry-exhausted:timeout");
        assert_eq!(backoff(1), Duration::from_millis(25));
        assert_eq!(backoff(100), MAX_PAUSE);
    }

    fn spec(identities: Option<Arc<Identities>>) -> ClientSpec {
        ClientSpec {
            members: vec![Member {
                id: 1,
                url: "http://127.0.0.1:9".to_string(),
            }],
            tokens: Tokens::default(),
            timing: Timing {
                request_timeout: Duration::from_secs(1),
                retry_budget: Duration::from_secs(1),
            },
            group: None,
            db: "pizza".to_string(),
            module: "pizza".to_string(),
            run_tag: "run".to_string(),
            identities,
        }
    }

    #[test]
    fn calls_carry_their_callers_credentials_only_with_auth() {
        let tenants = vec!["tenant-0".to_string()];
        let identities = Arc::new(Identities::from_seed([1u8; 32], &tenants, 1_000));
        let client =
            BenchClient::new(spec(Some(identities.clone())), Arc::new(Recorder::new())).unwrap();
        let args = serde_json::json!(["tenant-0", "store-0"]);
        let read = client.query(
            "pizza-tenant-0",
            "pizza.shop",
            &args,
            Who::Tenant("tenant-0"),
        );
        let body: Value = serde_json::from_slice(&read.body).unwrap();
        assert_eq!(body["db"], "pizza-tenant-0");
        assert_eq!(
            body["credentials"]["bearer"],
            identities.tenant("tenant-0").unwrap()
        );
        assert!(!read.gateway && body.get("principal").is_none());
        let setup = client.mutate("pizza", "pizza.setup", &args, "id", Who::Gateway);
        let body: Value = serde_json::from_slice(&setup.body).unwrap();
        assert!(setup.gateway);
        assert_eq!(body["principal"]["subject"], crate::auth::ADMIN_SUBJECT);
        assert!(body.get("credentials").is_none());
        let world = client.query("pizza", "pizza.world", &args, Who::Admin);
        let body: Value = serde_json::from_slice(&world.body).unwrap();
        assert_eq!(body["credentials"]["bearer"], identities.admin());
        // Without --auth nothing is attached, whoever the caller.
        let plain = BenchClient::new(spec(None), Arc::new(Recorder::new())).unwrap();
        for who in [Who::Tenant("tenant-0"), Who::Admin, Who::Gateway] {
            let request = plain.mutate("pizza", "pizza.tip", &args, "id", who);
            let body: Value = serde_json::from_slice(&request.body).unwrap();
            assert!(!request.gateway);
            assert!(body.get("credentials").is_none() && body.get("principal").is_none());
        }
    }

    #[test]
    fn hook_denials_keep_their_code() {
        let denied = br#"{"ok":false,"code":"unauthorized","message":"FORBIDDEN: access denied"}"#;
        match classify(403, denied, 0) {
            Attempt::Decided(CallError::Refused { kind, message }) => {
                assert_eq!(kind, "http-403:unauthorized");
                assert!(message.contains("FORBIDDEN"));
            }
            _ => panic!("a denial is decided"),
        }
    }

    #[test]
    fn bodies_are_fixed_and_carry_the_request_id() {
        let client = BenchClient::new(spec(None), Arc::new(Recorder::new())).unwrap();
        let id = client.next_id();
        assert_eq!(id, "run-1");
        let args = serde_json::json!({"shop": ["t", "s"], "amount": 2});
        let first = client.mutate_request("pizza.tip", &args, &id);
        let again = client.mutate_request("pizza.tip", &args, &id);
        assert_eq!(first.body, again.body);
        let body: Value = serde_json::from_slice(&first.body).unwrap();
        assert_eq!(body["idempotency_key"], "run-1");
        assert_eq!(body["db"], "pizza");
        let query: Value =
            serde_json::from_slice(&client.query_request("pizza.shop", &args).body).unwrap();
        assert!(query.get("idempotency_key").is_none());
    }
}
