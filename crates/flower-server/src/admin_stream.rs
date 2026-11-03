//! The operator event stream (admin): `GET /v1/admin/stream`, one
//! server-sent event stream carrying what the dashboard shows of this
//! member, pushed as it changes instead of polled.
//!
//! # Events
//!
//! Every event's data is one JSON object. Clients ignore event names
//! and fields they do not know: later sections (per-group and partition
//! views, say) arrive as events and snapshot keys of their own without
//! breaking older pages. Times are this member's clock, in ms since the
//! epoch (`at_ms`).
//!
//! - `snapshot`, first, and again with `reason: "resync"` when this
//!   stream fell too far behind to deliver every transition: `v` (the
//!   schema version, [`VERSION`]), `interval_ms`, `node` (as
//!   `/v1/status`), `readiness` (as `/health/ready`), `raft` (OpenRaft's
//!   metrics), `transitions` (the last [`TRANSITIONS_KEPT`] Raft
//!   transitions this member recorded, oldest first, so a page that
//!   reconnects still sees the flips it missed), `membership`, `metrics`
//!   (one sample), `catalog`, `backup`, `work` and `activity` (null with
//!   `activity=0`). A section that failed to read is `{"error": ...}`.
//! - `raft`: one Raft transition: each distinct (term, role, leader,
//!   membership) the recorder saw on OpenRaft's metrics channel, in
//!   order, with `seq`, `at_ms` and the `previous` term, role and leader.
//!   The channel keeps only its latest value, so two changes the core
//!   publishes before the recorder runs (a candidate winning its own
//!   election at once, say) arrive as one transition whose `previous` is
//!   the one before both; changes an election timeout apart never merge.
//! - `readiness`: `/health/ready`'s answer whenever whether or why this
//!   member is ready changes.
//! - `replication`: the log, committed, applied, snapshot and purge
//!   positions and, on the leader, each member's matched index; at most
//!   one per [`REPLICATION_EVERY`]. The committed index is the last
//!   readiness check's.
//! - `metrics`: a `/v1/metrics` sample every `interval_ms` (clamped to
//!   [`MIN_INTERVAL_MS`]..=[`MAX_INTERVAL_MS`], default
//!   [`DEFAULT_INTERVAL_MS`]), stamped `at_ms`, so rates come from this
//!   member's clock. A reader that falls behind gets the latest sample,
//!   never a backlog.
//! - `catalog`: modules with their staging, every database's key
//!   catalog's public listing, each database's retention and the membership, with the
//!   `applied` index they were read at, after an applied entry
//!   (deployment, activation, key change, retention entry, incarnation,
//!   membership, snapshot install) or a staging step may have changed
//!   them, and only if they did. A replicated staging shows alike on
//!   every member; a one-call deployment's is the leader's own: `leads`
//!   says whether this member leads.
//! - `backup` and `work` (timer counts per database, the queues
//!   `queue=db/name` names, repeatable, and the receipt expiry clock):
//!   sampled with metrics, at most once per [`WORK_EVERY`], sent when
//!   they changed.
//! - `activity`: the `/v1/admin/activity` snapshot after changes, at
//!   most one per second, unless `activity=0`.
//!
//! ## Groups, partitions and cross-group transactions
//!
//! A process hosting several groups ([`flower_cluster::host::Host`])
//! streams them all on the root's stream: the sections above stay the
//! root's, and these carry the rest. Without a host they are `null` in
//! the snapshot and never sent.
//!
//! - snapshot `groups`: every group this process hosts besides the root,
//!   each with `name`, `uid`, `node` (as `/v1/status`), `readiness` (as
//!   `/health/ready?group=`) and the last [`TRANSITIONS_KEPT`]
//!   `transitions` its replica recorded.
//! - `group_raft`: one Raft transition of a hosted group, as `raft`,
//!   tagged with `group` and `uid`. Transitions of every group share one
//!   sequence, so a snapshot and the events after it never leave a gap.
//! - `groups`: the hosted groups (as the snapshot's, without their
//!   transitions) whenever a group opens or closes here, or whether or why
//!   one is ready changes.
//! - `partitions`: the root's applied partition catalog — every group it
//!   lists with whether this process `hosted` it, every partition with
//!   its owner, ownership epoch, status and move in flight (with its
//!   `phase`), and the last finished moves — after a catalog entry
//!   changed it.
//! - `transactions`: the cross-group transactions in doubt in every
//!   hosted group, as its replica applied them (coordinator records not
//!   yet complete, participant records still prepared, without patches
//!   or results), after a transaction entry changed them.
//!
//! # Cost
//!
//! Nothing here runs a fresh-read barrier, forwards to the leader or
//! takes the engine lock: the catalogs, the counts and the transaction
//! records read each group's applied state, metrics are in memory, and
//! readiness asks the Raft core for its committed index. Transitions and
//! readiness are recorded once per member, however many streams follow
//! them; readiness is checked only while one does. At most [`MAX_STREAMS`] streams run at once (429
//! `resource_exhausted` past them), idle ones carry `: flower` comments
//! every 15 s, and shutdown ends them all.
//!
//! A stream computes only while its client reads: a stalled reader
//! costs no samples, never holds apply back, and the transitions it has
//! not taken wait in a shared buffer of [`BACKLOG`]; past it, the stream
//! resyncs with a fresh snapshot.

use std::collections::{BTreeMap, VecDeque};
use std::convert::Infallible;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use axum::Router;
use axum::extract::{RawQuery, State};
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Json, Response};
use axum::routing::get;
use flower_cluster::exec::now_ms;
use flower_cluster::group::GroupId;
use flower_cluster::host::{GroupEvent, Host};
use flower_cluster::node::Node;
use flower_cluster::types::NodeId;
use flower_engine::hex::encode as encode_hex;
use openraft::{BasicNode, LogId, RaftMetrics, ServerState};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, broadcast, watch};
use tokio::time::{Instant, Interval, MissedTickBehavior};

use crate::api::{ApiError, AppState, StatusDto, backup_status, metrics, readiness_json};

/// The event schema's version, in every snapshot's `v`.
pub const VERSION: u64 = 1;
/// Concurrent admin streams.
pub const MAX_STREAMS: usize = 16;
/// Raft transitions a snapshot carries.
pub const TRANSITIONS_KEPT: usize = 64;
/// Pushed events a stream may leave unread before it resyncs.
pub const BACKLOG: usize = 256;
/// Metrics sampling when the request names no interval.
pub const DEFAULT_INTERVAL_MS: u64 = 2_000;
/// The shortest metrics interval.
pub const MIN_INTERVAL_MS: u64 = 250;
/// The longest metrics interval.
pub const MAX_INTERVAL_MS: u64 = 60_000;
/// The most often replication progress is sent.
pub const REPLICATION_EVERY: Duration = Duration::from_millis(250);
/// The most often backup status and timer and queue counts are read.
pub const WORK_EVERY: Duration = Duration::from_secs(1);
/// Queues one stream may follow.
pub const MAX_QUEUES: usize = 32;
/// Every event name the stream sends.
pub const EVENTS: [&str; 13] = [
    "snapshot",
    "raft",
    "readiness",
    "replication",
    "metrics",
    "catalog",
    "backup",
    "work",
    "activity",
    "group_raft",
    "groups",
    "partitions",
    "transactions",
];
/// How often readiness is checked while a stream follows it.
const READINESS_EVERY: Duration = Duration::from_millis(500);
/// How long catalog changes settle before the catalog is read again:
/// a deployment's entries arrive together.
const CATALOG_SETTLE: Duration = Duration::from_millis(50);
/// The most often activity is sent.
const ACTIVITY_EVERY: Duration = Duration::from_secs(1);
/// Idle stream comments.
const KEEPALIVE: Duration = Duration::from_secs(15);

type Metrics = RaftMetrics<NodeId, BasicNode>;

/// Each hosted group's transitions kept, by name.
type GroupTransitions = BTreeMap<Arc<str>, Vec<serde_json::Value>>;

/// A group's uid as the catalog writes it: 16 hex digits.
fn uid_hex(uid: u64) -> String {
    format!("{uid:016x}")
}

/// One event recorded for every stream: its name, its JSON text and,
/// for Raft transitions, its sequence number.
#[derive(Clone, Debug)]
struct Pushed {
    event: &'static str,
    data: Arc<str>,
    seq: Option<u64>,
}

/// What makes a Raft transition: term, role, leader and membership.
#[derive(Clone, Debug, PartialEq, Eq)]
struct RaftKey {
    term: u64,
    state: ServerState,
    leader: Option<NodeId>,
    membership: Option<LogId<NodeId>>,
}

impl RaftKey {
    fn of(metrics: &Metrics) -> Self {
        RaftKey {
            term: metrics.current_term,
            state: metrics.state,
            leader: metrics.current_leader,
            membership: *metrics.membership_config.log_id(),
        }
    }
}

/// The transitions kept: this member's, and each hosted group's.
#[derive(Default)]
struct Kept {
    root: VecDeque<serde_json::Value>,
    groups: BTreeMap<Arc<str>, VecDeque<serde_json::Value>>,
}

/// A member's record of Raft transitions and readiness, shared by its
/// admin streams.
pub struct AdminEvents {
    transitions: Mutex<Kept>,
    /// The last transition's number, of any group: taken under the
    /// `transitions` lock.
    seq: AtomicU64,
    pushed: broadcast::Sender<Pushed>,
    readiness: Mutex<Option<serde_json::Value>>,
    /// The hosted groups' summary last pushed.
    groups: Mutex<Option<serde_json::Value>>,
    /// Counts changes to any hosted group's operator state: its catalog,
    /// placements and transaction records, and groups opening or
    /// closing here.
    group_changes: watch::Sender<u64>,
    /// The committed index the last readiness check read, plus one;
    /// zero before one did.
    committed: AtomicU64,
    nudge: Notify,
    streams: Arc<Semaphore>,
    closed: watch::Sender<bool>,
}

impl Default for AdminEvents {
    fn default() -> Self {
        AdminEvents {
            transitions: Mutex::new(Kept::default()),
            seq: AtomicU64::new(0),
            pushed: broadcast::channel(BACKLOG).0,
            readiness: Mutex::new(None),
            groups: Mutex::new(None),
            group_changes: watch::channel(0).0,
            committed: AtomicU64::new(0),
            nudge: Notify::new(),
            streams: Arc::new(Semaphore::new(MAX_STREAMS)),
            closed: watch::channel(false).0,
        }
    }
}

impl AdminEvents {
    /// Record `node`'s Raft transitions, and its readiness while a stream
    /// follows it, until `stop` turns true; with the `host` it serves the
    /// root of, every other hosted group's too.
    pub(crate) async fn run(
        self: Arc<Self>,
        node: Arc<Node>,
        host: Option<Arc<Host>>,
        stop: watch::Receiver<bool>,
    ) {
        tokio::join!(
            self.clone()
                .record_raft(node.raft().metrics(), None, stop.clone()),
            self.clone()
                .check_readiness(node, host.clone(), stop.clone()),
            self.follow_groups(host, stop),
        );
    }

    /// End every stream: the member shuts down.
    pub(crate) fn close(&self) {
        let _ = self.closed.send(true);
    }

    /// Follow OpenRaft's metrics and record each distinct transition, of
    /// this member or of the hosted `group`. This task only reads and
    /// records, so it wakes for each value the core publishes; values
    /// published faster than it wakes merge.
    async fn record_raft(
        self: Arc<Self>,
        mut metrics: watch::Receiver<Metrics>,
        group: Option<GroupId>,
        mut stop: watch::Receiver<bool>,
    ) {
        let mut last: Option<RaftKey> = None;
        loop {
            {
                let current = metrics.borrow_and_update();
                let key = RaftKey::of(&current);
                if last.as_ref() != Some(&key) {
                    self.transition(&current, last.as_ref(), group.as_ref());
                    last = Some(key);
                }
            }
            tokio::select! {
                changed = metrics.changed() => if changed.is_err() {
                    return;
                },
                changed = stop.changed() => if changed.is_err() || *stop.borrow() {
                    return;
                },
            }
        }
    }

    /// Keep one transition and push it to every stream, under the rings'
    /// lock so a stream's snapshot and its events never leave a gap.
    fn transition(&self, metrics: &Metrics, previous: Option<&RaftKey>, group: Option<&GroupId>) {
        let mut kept = self
            .transitions
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let seq = self.seq.fetch_add(1, Ordering::Relaxed) + 1;
        let membership = metrics.membership_config.membership();
        let mut value = serde_json::json!({
            "seq": seq,
            "at_ms": now_ms(),
            "term": metrics.current_term,
            "state": format!("{:?}", metrics.state),
            "leader": metrics.current_leader,
            "membership": {
                "log_id": metrics.membership_config.log_id(),
                "voters": membership.get_joint_config(),
                "nodes": membership
                    .nodes()
                    .map(|(id, node)| (id.to_string(), node.addr.clone().into()))
                    .collect::<serde_json::Map<_, _>>(),
            },
            "last_log": metrics.last_log_index,
            "applied": metrics.last_applied.map(|id| id.index),
            "previous": previous.map(|key| serde_json::json!({
                "term": key.term,
                "state": format!("{:?}", key.state),
                "leader": key.leader,
            })),
        });
        let (ring, event) = match group {
            None => (&mut kept.root, "raft"),
            Some(group) => {
                value["group"] = group.name.to_string().into();
                value["uid"] = uid_hex(group.uid).into();
                (
                    kept.groups.entry(group.name.clone()).or_default(),
                    "group_raft",
                )
            }
        };
        if ring.len() == TRANSITIONS_KEPT {
            ring.pop_front();
        }
        ring.push_back(value.clone());
        let _ = self.pushed.send(Pushed {
            event,
            data: value.to_string().into(),
            seq: Some(seq),
        });
        drop(kept);
        // Readiness may have moved with it.
        self.nudge.notify_one();
    }

    /// This member's transitions kept, oldest first, each hosted group's,
    /// and the last one's number.
    fn kept(&self) -> (Vec<serde_json::Value>, GroupTransitions, u64) {
        let kept = self
            .transitions
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let groups = kept
            .groups
            .iter()
            .map(|(name, ring)| (name.clone(), ring.iter().cloned().collect()))
            .collect();
        (
            kept.root.iter().cloned().collect(),
            groups,
            self.seq.load(Ordering::Relaxed),
        )
    }

    /// Follow the groups `host` opens and closes besides the root: record
    /// each one's Raft transitions and count its operator-state changes
    /// into `group_changes`, until `stop` turns true.
    async fn follow_groups(
        self: Arc<Self>,
        host: Option<Arc<Host>>,
        mut stop: watch::Receiver<bool>,
    ) {
        let Some(host) = host else {
            return;
        };
        let mut events = host.subscribe();
        let mut followed: BTreeMap<Arc<str>, (u64, watch::Sender<bool>)> = BTreeMap::new();
        let mut tasks = tokio::task::JoinSet::new();
        self.sync_groups(&host, &mut followed, &mut tasks);
        loop {
            tokio::select! {
                event = events.recv() => match event {
                    // A group opened again under the same identity has a
                    // new Raft core: follow that one, not the closed one.
                    Ok(GroupEvent::Opened(group)) => {
                        if let Some((_, stop)) = followed.remove(&group.name) {
                            let _ = stop.send(true);
                        }
                        self.sync_groups(&host, &mut followed, &mut tasks);
                    }
                    Ok(GroupEvent::Closed(_)) | Err(broadcast::error::RecvError::Lagged(_)) => {
                        self.sync_groups(&host, &mut followed, &mut tasks);
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                },
                changed = stop.changed() => if changed.is_err() || *stop.borrow() {
                    break;
                },
                Some(_) = tasks.join_next(), if !tasks.is_empty() => {}
            }
        }
        for (_, (_, stop)) in followed {
            let _ = stop.send(true);
        }
        while tasks.join_next().await.is_some() {}
    }

    /// Start following groups `host` opened, stop following those it
    /// closed.
    fn sync_groups(
        self: &Arc<Self>,
        host: &Host,
        followed: &mut BTreeMap<Arc<str>, (u64, watch::Sender<bool>)>,
        tasks: &mut tokio::task::JoinSet<()>,
    ) {
        let hosted: BTreeMap<Arc<str>, Arc<Node>> = host
            .groups()
            .into_iter()
            .filter(|node| !node.group().is_root())
            .map(|node| (node.group().name.clone(), node))
            .collect();
        let mut changed = false;
        followed.retain(|name, (uid, stop)| {
            let keep = hosted
                .get(name)
                .is_some_and(|node| node.group().uid == *uid);
            if !keep {
                let _ = stop.send(true);
                self.transitions
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .groups
                    .remove(name);
                changed = true;
            }
            keep
        });
        for (name, node) in hosted {
            if followed.contains_key(&name) {
                continue;
            }
            let (stop, stopped) = watch::channel(false);
            followed.insert(name, (node.group().uid, stop));
            // The tasks hold the group's channels, not the group: closing
            // it ends them.
            let metrics = node.raft().metrics();
            let catalog = node.subscribe_catalog();
            let group = node.group().clone();
            drop(node);
            let this = self.clone();
            tasks.spawn(async move {
                tokio::join!(
                    this.clone()
                        .record_raft(metrics, Some(group), stopped.clone()),
                    this.count_changes(catalog, stopped),
                );
            });
            changed = true;
        }
        if changed {
            self.group_changes.send_modify(|count| *count += 1);
            self.nudge.notify_one();
        }
    }

    /// Count a hosted group's operator-state changes into
    /// `group_changes`, until it closes or `stop` turns true.
    async fn count_changes(
        &self,
        mut catalog: watch::Receiver<u64>,
        mut stop: watch::Receiver<bool>,
    ) {
        loop {
            tokio::select! {
                changed = catalog.changed() => {
                    if changed.is_err() {
                        return;
                    }
                    self.group_changes.send_modify(|count| *count += 1);
                }
                changed = stop.changed() => if changed.is_err() || *stop.borrow() {
                    return;
                },
            }
        }
    }

    /// The committed index the last readiness check read, if any: at
    /// most [`READINESS_EVERY`] old while a stream follows this member.
    fn committed(&self) -> Option<u64> {
        self.committed.load(Ordering::Relaxed).checked_sub(1)
    }

    /// Whether a stream follows this member.
    fn watched(&self) -> bool {
        self.streams.available_permits() < MAX_STREAMS
    }

    /// Check readiness every [`READINESS_EVERY`], and at once after a
    /// transition, while a stream follows it; push it when it changed.
    /// With a `host`, each hosted group's too, pushed as `groups`.
    async fn check_readiness(
        self: Arc<Self>,
        node: Arc<Node>,
        host: Option<Arc<Host>>,
        mut stop: watch::Receiver<bool>,
    ) {
        let mut every = tokio::time::interval(READINESS_EVERY);
        every.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = every.tick() => {}
                () = self.nudge.notified() => {}
                changed = stop.changed() => if changed.is_err() || *stop.borrow() {
                    return;
                },
            }
            if !self.watched() {
                continue;
            }
            let readiness = node.readiness().await;
            self.committed.store(
                readiness.committed.map_or(0, |index| index + 1),
                Ordering::Relaxed,
            );
            let readiness = readiness_json(&readiness);
            self.readiness_seen(readiness);
            if let Some(host) = &host {
                self.groups_seen(hosted_groups(host).await);
            }
        }
    }

    /// Note the hosted groups' summary; push it when a group opened or
    /// closed, or whether or why one is ready changed.
    fn groups_seen(&self, groups: serde_json::Value) {
        let verdicts = |value: &serde_json::Value| -> Vec<_> {
            value
                .as_array()
                .map(|groups| {
                    groups
                        .iter()
                        .map(|group| {
                            let ready = &group["readiness"];
                            (
                                group["name"].clone(),
                                group["uid"].clone(),
                                ready["ok"].clone(),
                                ready["reason"].clone(),
                                ready["rejoining"].clone(),
                                ready["storage"].is_null(),
                            )
                        })
                        .collect()
                })
                .unwrap_or_default()
        };
        let mut last = self.groups.lock().unwrap_or_else(PoisonError::into_inner);
        if last
            .as_ref()
            .is_some_and(|last| verdicts(last) == verdicts(&groups))
        {
            return;
        }
        let pushed = serde_json::json!({"at_ms": now_ms(), "groups": groups});
        *last = Some(groups);
        let _ = self.pushed.send(Pushed {
            event: "groups",
            data: pushed.to_string().into(),
            seq: None,
        });
    }

    /// Note a readiness answer; push it when whether or why changed.
    fn readiness_seen(&self, readiness: serde_json::Value) {
        let verdict = |value: &serde_json::Value| {
            (
                value["ok"].clone(),
                value["reason"].clone(),
                value["rejoining"].clone(),
                value["storage"].is_null(),
            )
        };
        let mut last = self
            .readiness
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if last
            .as_ref()
            .is_some_and(|last| verdict(last) == verdict(&readiness))
        {
            return;
        }
        let mut pushed = readiness.clone();
        pushed["at_ms"] = now_ms().into();
        *last = Some(readiness);
        let _ = self.pushed.send(Pushed {
            event: "readiness",
            data: pushed.to_string().into(),
            seq: None,
        });
    }
}

/// The admin stream's route, admin-only.
pub(crate) fn routes() -> Router<AppState> {
    Router::new().route("/v1/admin/stream", get(get_stream))
}

/// What one stream asked for.
#[derive(Clone, Debug, PartialEq, Eq)]
struct StreamOptions {
    interval_ms: u64,
    queues: Vec<(String, String)>,
    activity: bool,
}

impl StreamOptions {
    /// Parse `interval_ms`, repeated `queue=db/name` and `activity`.
    fn parse(query: Option<&str>) -> Result<Self, String> {
        let mut options = StreamOptions {
            interval_ms: DEFAULT_INTERVAL_MS,
            queues: Vec::new(),
            activity: true,
        };
        for pair in query
            .unwrap_or("")
            .split('&')
            .filter(|pair| !pair.is_empty())
        {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            let value = percent_decode(value)?;
            match key {
                "interval_ms" => {
                    let ms: u64 = value
                        .parse()
                        .map_err(|_| "interval_ms must be a whole number of milliseconds")?;
                    options.interval_ms = ms.clamp(MIN_INTERVAL_MS, MAX_INTERVAL_MS);
                }
                "queue" => {
                    let Some((db, queue)) = value.split_once('/') else {
                        return Err("queue takes db/name".to_string());
                    };
                    if options.queues.len() >= MAX_QUEUES {
                        return Err(format!("at most {MAX_QUEUES} queues"));
                    }
                    options.queues.push((db.to_string(), queue.to_string()));
                }
                "activity" => options.activity = !matches!(value.as_str(), "0" | "false"),
                _ => {}
            }
        }
        Ok(options)
    }
}

/// Decode a query value's `+` and `%XX` escapes as UTF-8.
fn percent_decode(text: &str) -> Result<String, String> {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'+' => out.push(b' '),
            b'%' => {
                let hex = bytes
                    .get(index + 1..index + 3)
                    .and_then(|pair| std::str::from_utf8(pair).ok())
                    .and_then(|pair| u8::from_str_radix(pair, 16).ok())
                    .ok_or("malformed percent escape")?;
                out.push(hex);
                index += 2;
            }
            byte => out.push(byte),
        }
        index += 1;
    }
    String::from_utf8(out).map_err(|_| "query values must be UTF-8".to_string())
}

/// One stream: the snapshot first, then events as they come.
async fn get_stream(State(state): State<AppState>, RawQuery(query): RawQuery) -> Response {
    let options = match StreamOptions::parse(query.as_deref()) {
        Ok(options) => options,
        Err(message) => return ApiError::BadRequest(message).into_response(),
    };
    let streaming = match open(state, options).await {
        Ok(streaming) => streaming,
        Err(refused) => return *refused,
    };
    let events = futures_util::stream::unfold(streaming, |mut streaming| async move {
        let event = streaming.next().await?;
        Some((Ok::<_, Infallible>(event.sse()), streaming))
    });
    let mut response = Sse::new(events)
        .keep_alive(KeepAlive::new().interval(KEEPALIVE).text("flower"))
        .into_response();
    response.headers_mut().insert(
        axum::http::header::CACHE_CONTROL,
        axum::http::HeaderValue::from_static("no-store"),
    );
    response
}

/// Take a stream slot and read the opening snapshot: 429 when every
/// slot is taken.
async fn open(state: AppState, options: StreamOptions) -> Result<Streaming, Box<Response>> {
    let Ok(permit) = state.admin_events.streams.clone().try_acquire_owned() else {
        let body = serde_json::json!({
            "ok": false,
            "code": "resource_exhausted",
            "message": "admin stream capacity reached",
        });
        return Err(Box::new(
            (StatusCode::TOO_MANY_REQUESTS, Json(body)).into_response(),
        ));
    };
    // Subscribed before the snapshot reads the transitions kept, so
    // none falls between them.
    let pushed = state.admin_events.pushed.subscribe();
    let group_changes = state.admin_events.group_changes.subscribe();
    let (snapshot, seen) = snapshot(&state, &options, "connect").await;
    let period = Duration::from_millis(options.interval_ms);
    let mut metrics_every = tokio::time::interval_at(Instant::now() + period, period);
    metrics_every.set_missed_tick_behavior(MissedTickBehavior::Skip);
    Ok(Streaming {
        raft: state.node.raft().metrics(),
        catalog: state.node.subscribe_catalog(),
        closed: state.admin_events.closed.subscribe(),
        replication: None,
        replication_due: None,
        replication_next: Instant::now(),
        catalog_due: None,
        catalog_last: snapshot["catalog"]
            .as_object()
            .map(|_| catalog_key(&snapshot["catalog"])),
        group_changes,
        groups_due: None,
        partitions_last: Some(snapshot["partitions"].to_string()),
        transactions_last: Some(snapshot["transactions"].to_string()),
        work_last: Some(snapshot["work"].to_string()),
        backup_last: Some(snapshot["backup"].to_string()),
        work_next: Instant::now() + WORK_EVERY,
        activity_seen: snapshot["activity"]["sequence"].as_u64(),
        activity_next: Instant::now() + ACTIVITY_EVERY,
        queue: VecDeque::from([event("snapshot", &snapshot)]),
        metrics_every,
        seen,
        pushed,
        options,
        state,
        _permit: permit,
    })
}

/// One event on its way out: its name and its JSON text.
#[derive(Clone, Debug)]
struct Outgoing {
    name: &'static str,
    data: Arc<str>,
}

impl Outgoing {
    /// The SSE event it is sent as.
    fn sse(&self) -> Event {
        Event::default().event(self.name).data(&*self.data)
    }
}

/// An event named `name` carrying `data`.
fn event(name: &'static str, data: &serde_json::Value) -> Outgoing {
    Outgoing {
        name,
        data: data.to_string().into(),
    }
}

/// A failed section's stand-in.
fn failed(error: impl std::fmt::Display) -> serde_json::Value {
    serde_json::json!({"error": error.to_string()})
}

/// Everything at once, with the last transition number it holds.
async fn snapshot(
    state: &AppState,
    options: &StreamOptions,
    reason: &str,
) -> (serde_json::Value, u64) {
    let (transitions, group_transitions, seen) = state.admin_events.kept();
    let readiness = readiness_json(&state.node.readiness().await);
    let raft = serde_json::to_value(&*state.node.raft().metrics().borrow()).unwrap_or_else(failed);
    let metrics = metrics_sample(state).await;
    let catalog = catalog(state).await;
    let work = work(state, &options.queues).await;
    let snapshot = serde_json::json!({
        "v": VERSION,
        "reason": reason,
        "at_ms": now_ms(),
        "interval_ms": options.interval_ms,
        "node": StatusDto::of(&state.node),
        "readiness": readiness,
        "raft": raft,
        "transitions": transitions,
        "membership": state.node.membership(),
        "metrics": metrics,
        "catalog": catalog,
        "backup": backup_status(state).await,
        "work": work,
        "activity": options.activity.then(|| state.activity.snapshot()),
        "groups": groups_snapshot(state, group_transitions).await,
        "partitions": partitions(state).await,
        "transactions": transactions(state).await,
    });
    (snapshot, seen)
}

/// The host whose other groups this stream carries: the one this member
/// is the root of.
fn hosting(state: &AppState) -> Option<&Arc<Host>> {
    state.host.as_ref().filter(|_| state.node.group().is_root())
}

/// Every group the host serves besides the root: identity, status and
/// readiness.
async fn hosted_groups(host: &Host) -> serde_json::Value {
    let mut groups = Vec::new();
    for node in host.groups() {
        if node.group().is_root() {
            continue;
        }
        let readiness = node.readiness().await;
        groups.push(serde_json::json!({
            "name": &*node.group().name,
            "uid": uid_hex(node.group().uid),
            "node": StatusDto::of(&node),
            "readiness": readiness_json(&readiness),
        }));
    }
    serde_json::Value::Array(groups)
}

/// The snapshot's hosted groups, each with its transitions kept.
async fn groups_snapshot(state: &AppState, mut transitions: GroupTransitions) -> serde_json::Value {
    let Some(host) = hosting(state) else {
        return serde_json::Value::Null;
    };
    let mut groups = hosted_groups(host).await;
    for group in groups.as_array_mut().into_iter().flatten() {
        let kept = group["name"]
            .as_str()
            .and_then(|name| transitions.remove(name))
            .unwrap_or_default();
        group["transitions"] = kept.into();
    }
    groups
}

/// The root's applied partition catalog: groups, partitions with their
/// epochs and moves in flight, and the last finished moves.
async fn partitions(state: &AppState) -> serde_json::Value {
    let Some(host) = hosting(state).cloned() else {
        return serde_json::Value::Null;
    };
    let read = tokio::task::spawn_blocking(move || {
        let catalog = host.applied_catalog()?;
        Ok::<_, flower_cluster::node::NodeError>(serde_json::json!({
            "revision": catalog.revision,
            "groups": crate::catalog::groups_json(&host, &catalog, false),
            "partitions": catalog.partitions.values().collect::<Vec<_>>(),
            "moves": catalog.moves,
        }))
    })
    .await;
    match read {
        Ok(Ok(value)) => value,
        Ok(Err(error)) => failed(error),
        Err(error) => failed(error),
    }
}

/// The cross-group transactions in doubt in every hosted group.
async fn transactions(state: &AppState) -> serde_json::Value {
    let Some(host) = hosting(state).cloned() else {
        return serde_json::Value::Null;
    };
    match tokio::task::spawn_blocking(move || host.in_doubt()).await {
        Ok(groups) => serde_json::json!({"groups": groups}),
        Err(error) => failed(error),
    }
}

/// One metrics sample, stamped with this member's clock.
async fn metrics_sample(state: &AppState) -> serde_json::Value {
    let at_ms = now_ms();
    match metrics(state).await {
        Ok(metrics) => {
            let mut value = serde_json::to_value(metrics).unwrap_or_else(failed);
            value["at_ms"] = at_ms.into();
            value
        }
        Err(error) => failed(error.describe()),
    }
}

/// The member's applied catalog: modules and staging, keys, retention
/// and membership.
async fn catalog(state: &AppState) -> serde_json::Value {
    let catalog = match state.node.operator_catalog().await {
        Ok(catalog) => catalog,
        Err(error) => return failed(error),
    };
    let status = state.node.status();
    let databases: Vec<serde_json::Value> = catalog
        .databases
        .iter()
        .map(|database| {
            let modules: Vec<serde_json::Value> = database
                .modules
                .iter()
                .map(|module| {
                    let mut value = crate::admin::module_json(module);
                    if let Some(identity) = database.staged.get(&module.name) {
                        value["staged_identity"] = encode_hex(identity).into();
                    }
                    value
                })
                .collect();
            serde_json::json!({
                "db": String::from_utf8_lossy(&database.db),
                "revision": database.revision,
                "applied": database.applied,
                "modules": modules,
                "retention": crate::retention::status_json(&database.retry)
                    .unwrap_or_else(|error| failed(error.describe())),
            })
        })
        .collect();
    serde_json::json!({
        "applied": catalog.applied,
        "leads": status.leader == Some(status.id),
        "databases": databases,
        "keys": catalog.keys.unwrap_or_else(failed),
        "membership": state.node.membership(),
    })
}

/// A catalog's content without the index it was read at: entries that
/// changed nothing an operator sees send no event.
fn catalog_key(catalog: &serde_json::Value) -> String {
    let mut content = catalog.clone();
    if let Some(fields) = content.as_object_mut() {
        fields.remove("applied");
    }
    content.to_string()
}

/// Timer counts per database and the counts of the queues asked for,
/// from the read side.
async fn work(state: &AppState, queues: &[(String, String)]) -> serde_json::Value {
    let queues = queues.to_vec();
    let dbs: Vec<Vec<u8>> = match state.node.database_positions().await {
        Ok(positions) => positions.into_iter().map(|(db, _, _)| db).collect(),
        Err(error) => return failed(error),
    };
    let counted =
        state
            .node
            .with_reader(move |reader| {
                let timers: Vec<serde_json::Value> = dbs
                    .iter()
                    .map(|db| match reader.timer_stats(db) {
                        Ok(stats) => serde_json::json!({
                            "db": String::from_utf8_lossy(db),
                            "pending": stats.pending,
                            "dead": stats.dead,
                        }),
                        Err(failure) => serde_json::json!({
                            "db": String::from_utf8_lossy(db),
                            "error": format!("{}: {}", failure.code, failure.message),
                        }),
                    })
                    .collect();
                let queues: Vec<serde_json::Value> = queues
                .iter()
                .map(|(db, queue)| match reader.queue_stats(db.as_bytes(), queue) {
                    Ok(stats) => serde_json::json!({
                        "db": db, "queue": queue,
                        "pending": stats.pending, "claimed": stats.claimed, "dead": stats.dead,
                    }),
                    Err(failure) => serde_json::json!({
                        "db": db, "queue": queue,
                        "error": format!("{}: {}", failure.code, failure.message),
                    }),
                })
                .collect();
                Ok(serde_json::json!({"timers": timers, "queues": queues}))
            })
            .await;
    let mut work = counted.unwrap_or_else(failed);
    // The receipt expiry clock moves with every write: a sample, not
    // part of the catalog.
    work["receipts"] = match state.node.applied_receipt_clock().await {
        Ok(clock) => serde_json::to_value(clock).unwrap_or_else(failed),
        Err(error) => failed(error),
    };
    work
}

/// Replication progress, without the quorum ack age that moves with
/// every heartbeat.
#[derive(Clone, Debug, PartialEq, Eq)]
struct ReplicationKey {
    last_log: Option<u64>,
    applied: Option<u64>,
    snapshot: Option<u64>,
    purged: Option<u64>,
    matched: Vec<(NodeId, Option<u64>)>,
}

impl ReplicationKey {
    fn of(metrics: &Metrics) -> Self {
        ReplicationKey {
            last_log: metrics.last_log_index,
            applied: metrics.last_applied.map(|id| id.index),
            snapshot: metrics.snapshot.map(|id| id.index),
            purged: metrics.purged.map(|id| id.index),
            matched: metrics
                .replication
                .as_ref()
                .map(|replication| {
                    replication
                        .iter()
                        .map(|(id, matched)| (*id, matched.map(|log| log.index)))
                        .collect()
                })
                .unwrap_or_default(),
        }
    }
}

/// One stream's state between events.
struct Streaming {
    state: AppState,
    options: StreamOptions,
    pushed: broadcast::Receiver<Pushed>,
    /// Transitions at or below this number were in the last snapshot.
    seen: u64,
    raft: watch::Receiver<Metrics>,
    catalog: watch::Receiver<u64>,
    closed: watch::Receiver<bool>,
    metrics_every: Interval,
    replication: Option<ReplicationKey>,
    replication_due: Option<Instant>,
    replication_next: Instant,
    catalog_due: Option<Instant>,
    catalog_last: Option<String>,
    group_changes: watch::Receiver<u64>,
    groups_due: Option<Instant>,
    partitions_last: Option<String>,
    transactions_last: Option<String>,
    work_last: Option<String>,
    backup_last: Option<String>,
    work_next: Instant,
    activity_seen: Option<u64>,
    activity_next: Instant,
    queue: VecDeque<Outgoing>,
    _permit: OwnedSemaphorePermit,
}

impl Streaming {
    /// The next event, or `None` once the stream ends.
    async fn next(&mut self) -> Option<Outgoing> {
        loop {
            if let Some(event) = self.queue.pop_front() {
                return Some(event);
            }
            if *self.closed.borrow() {
                return None;
            }
            let activity = self.state.activity.clone();
            let changed = activity.next_change();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let activity_due = (self.options.activity
                && self.activity_seen != Some(activity.sequence()))
            .then(|| self.activity_next.max(Instant::now()));
            let far = Instant::now() + Duration::from_secs(3600);
            tokio::select! {
                biased;
                changed = self.closed.changed() => if changed.is_err() || *self.closed.borrow() {
                    return None;
                },
                pushed = self.pushed.recv() => match pushed {
                    Ok(pushed) => {
                        if pushed.seq.is_some_and(|seq| seq <= self.seen) {
                            continue;
                        }
                        if pushed.event == "raft" {
                            // A one-call deployment's staging is the
                            // leader's own, and membership moves with
                            // Raft: look at the catalog again.
                            self.catalog_due.get_or_insert(Instant::now() + CATALOG_SETTLE);
                        }
                        self.queue.push_back(Outgoing {
                            name: pushed.event,
                            data: pushed.data,
                        });
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => self.resync().await,
                    Err(broadcast::error::RecvError::Closed) => return None,
                },
                changed = self.catalog.changed() => {
                    if changed.is_err() {
                        return None;
                    }
                    self.catalog_due.get_or_insert(Instant::now() + CATALOG_SETTLE);
                    // The partition catalog is the root's.
                    self.groups_due.get_or_insert(Instant::now() + CATALOG_SETTLE);
                }
                changed = self.group_changes.changed() => {
                    if changed.is_err() {
                        return None;
                    }
                    self.groups_due.get_or_insert(Instant::now() + CATALOG_SETTLE);
                }
                () = tokio::time::sleep_until(self.groups_due.unwrap_or(far)), if self.groups_due.is_some() => {
                    self.groups_due = None;
                    self.sample_groups().await;
                }
                () = tokio::time::sleep_until(self.catalog_due.unwrap_or(far)), if self.catalog_due.is_some() => {
                    self.catalog_due = None;
                    let catalog = catalog(&self.state).await;
                    let key = catalog_key(&catalog);
                    if self.catalog_last.as_ref() != Some(&key) {
                        self.catalog_last = Some(key);
                        let mut value = catalog;
                        value["at_ms"] = now_ms().into();
                        self.queue.push_back(event("catalog", &value));
                    }
                }
                changed = self.raft.changed() => {
                    if changed.is_err() {
                        return None;
                    }
                    let key = ReplicationKey::of(&self.raft.borrow_and_update());
                    if self.replication.as_ref() != Some(&key) {
                        self.replication = Some(key);
                        self.replication_due
                            .get_or_insert(self.replication_next.max(Instant::now()));
                    }
                }
                () = tokio::time::sleep_until(self.replication_due.unwrap_or(far)), if self.replication_due.is_some() => {
                    self.replication_due = None;
                    self.replication_next = Instant::now() + REPLICATION_EVERY;
                    let committed = self.state.admin_events.committed();
                    let value = replication(&self.raft.borrow(), committed);
                    self.queue.push_back(event("replication", &value));
                }
                _ = self.metrics_every.tick() => {
                    let value = metrics_sample(&self.state).await;
                    self.queue.push_back(event("metrics", &value));
                    if Instant::now() >= self.work_next {
                        self.work_next = Instant::now() + WORK_EVERY;
                        self.sample_work().await;
                    }
                }
                () = tokio::time::sleep_until(activity_due.unwrap_or(far)), if activity_due.is_some() => {
                    self.activity_next = Instant::now() + ACTIVITY_EVERY;
                    let snapshot = activity.snapshot();
                    self.activity_seen = snapshot["sequence"].as_u64();
                    self.queue.push_back(event("activity", &snapshot));
                }
                () = &mut changed, if self.options.activity && activity_due.is_none() => {}
            }
        }
    }

    /// Too far behind to deliver every transition: start over from a
    /// fresh snapshot.
    async fn resync(&mut self) {
        self.pushed = self.state.admin_events.pushed.subscribe();
        let (snapshot, seen) = snapshot(&self.state, &self.options, "resync").await;
        self.seen = seen;
        self.catalog_last = Some(catalog_key(&snapshot["catalog"]));
        self.partitions_last = Some(snapshot["partitions"].to_string());
        self.transactions_last = Some(snapshot["transactions"].to_string());
        self.work_last = Some(snapshot["work"].to_string());
        self.backup_last = Some(snapshot["backup"].to_string());
        self.activity_seen = snapshot["activity"]["sequence"].as_u64();
        self.queue.clear();
        self.queue.push_back(event("snapshot", &snapshot));
    }

    /// Read the partition catalog and the transactions in doubt; queue an
    /// event for each that changed.
    async fn sample_groups(&mut self) {
        if hosting(&self.state).is_none() {
            return;
        }
        let sections = [
            ("partitions", partitions(&self.state).await),
            ("transactions", transactions(&self.state).await),
        ];
        for (name, value) in sections {
            let text = value.to_string();
            let last = match name {
                "partitions" => &mut self.partitions_last,
                _ => &mut self.transactions_last,
            };
            if last.as_ref() == Some(&text) {
                continue;
            }
            *last = Some(text);
            let mut value = value;
            if let Some(fields) = value.as_object_mut() {
                fields.insert("at_ms".into(), now_ms().into());
            }
            self.queue.push_back(event(name, &value));
        }
    }

    /// Read backup status and the timer and queue counts; queue an event
    /// for each that changed.
    async fn sample_work(&mut self) {
        let backup = backup_status(&self.state).await;
        let text = backup.to_string();
        if self.backup_last.as_ref() != Some(&text) {
            self.backup_last = Some(text);
            self.queue.push_back(event("backup", &backup));
        }
        let work = work(&self.state, &self.options.queues).await;
        let text = work.to_string();
        if self.work_last.as_ref() != Some(&text) {
            self.work_last = Some(text);
            let mut value = work;
            value["at_ms"] = now_ms().into();
            self.queue.push_back(event("work", &value));
        }
    }
}

/// Replication progress on the wire.
fn replication(metrics: &Metrics, committed: Option<u64>) -> serde_json::Value {
    serde_json::json!({
        "at_ms": now_ms(),
        "committed": committed,
        "last_log": metrics.last_log_index,
        "applied": metrics.last_applied.map(|id| id.index),
        "snapshot": metrics.snapshot,
        "purged": metrics.purged,
        "replication": metrics.replication,
        "millis_since_quorum_ack": metrics.millis_since_quorum_ack,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn options_parse_and_clamp() {
        let parsed = StreamOptions::parse(Some(
            "interval_ms=10&queue=demo%2Femail&queue=a/b+c&activity=0&other=1",
        ))
        .unwrap();
        assert_eq!(parsed.interval_ms, MIN_INTERVAL_MS);
        assert_eq!(
            parsed.queues,
            vec![
                ("demo".to_string(), "email".to_string()),
                ("a".to_string(), "b c".to_string())
            ]
        );
        assert!(!parsed.activity);
        let defaults = StreamOptions::parse(None).unwrap();
        assert_eq!(defaults.interval_ms, DEFAULT_INTERVAL_MS);
        assert!(defaults.activity);
        assert_eq!(
            StreamOptions::parse(Some("interval_ms=99999999"))
                .unwrap()
                .interval_ms,
            MAX_INTERVAL_MS
        );
        assert!(StreamOptions::parse(Some("interval_ms=soon")).is_err());
        assert!(StreamOptions::parse(Some("queue=nodb")).is_err());
        assert!(StreamOptions::parse(Some("queue=%zz")).is_err());
    }

    /// A one-member cluster's API state.
    async fn single_member() -> (tempfile::TempDir, AppState) {
        let dir = tempfile::tempdir().unwrap();
        let registry = flower_cluster::network::Registry::new();
        let node = Node::open(
            1,
            &dir.path().join("n1"),
            Arc::new(openraft::Config::default()),
            flower_cluster::network::InProcessFactory::new(registry.clone()),
            Arc::new(flower_cluster::wasm::WasmFactory::default()),
        )
        .await
        .unwrap();
        registry.register(1, node.raft().clone());
        node.initialize(std::collections::BTreeSet::from([1]))
            .await
            .unwrap();
        for _ in 0..500 {
            if node.status().leader == Some(1) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        (dir, AppState::new(Arc::new(node)))
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_stream_too_far_behind_resyncs_from_a_bounded_backlog() {
        let (_dir, state) = single_member().await;
        let options = StreamOptions::parse(Some("activity=0")).unwrap();
        let mut streaming = open(state.clone(), options).await.ok().unwrap();
        let first = streaming.next().await.unwrap();
        assert_eq!(first.name, "snapshot");
        assert!(first.data.contains("\"reason\":\"connect\""));
        // Far more pushed events than the backlog holds, none read.
        for index in 0..BACKLOG * 4 {
            let _ = state.admin_events.pushed.send(Pushed {
                event: "readiness",
                data: format!("{{\"n\":{index}}}").into(),
                seq: None,
            });
        }
        assert!(state.admin_events.pushed.len() <= BACKLOG);
        // The stream does not pretend to have delivered them: it starts
        // over from a fresh snapshot.
        let next = streaming.next().await.unwrap();
        assert_eq!(next.name, "snapshot");
        assert!(next.data.contains("\"reason\":\"resync\""), "{}", next.data);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_stalled_reader_gets_the_latest_sample_not_a_backlog() {
        let (_dir, state) = single_member().await;
        let options = StreamOptions::parse(Some("interval_ms=1000&activity=0")).unwrap();
        // The metrics interval starts after the opening snapshot, so its
        // ticks fall at or after `opened` plus 1 s, 2 s, and so on.
        let opened = Instant::now();
        let mut streaming = open(state, options).await.ok().unwrap();
        assert_eq!(streaming.next().await.unwrap().name, "snapshot");
        // Unread until 4.5 s: the ticks at 1, 2, 3 and 4 s are missed.
        tokio::time::sleep_until(opened + Duration::from_millis(4500)).await;
        // One deadline over the whole read rather than one per call: a
        // timeout that dropped `next()` while it sampled would lose the
        // sample its missed tick already consumed. The next tick (5 s)
        // falls after the deadline.
        let mut samples = Vec::new();
        let _ = tokio::time::timeout_at(opened + Duration::from_millis(4900), async {
            while let Some(event) = streaming.next().await {
                if event.name == "metrics" {
                    samples.push(event.data);
                }
            }
        })
        .await;
        // A backlog would deliver four.
        assert_eq!(samples.len(), 1, "{samples:?}");
    }

    #[test]
    fn readiness_is_pushed_only_when_whether_or_why_changes() {
        let events = AdminEvents::default();
        let mut pushed = events.pushed.subscribe();
        let ready = serde_json::json!({"ok": true, "reason": null, "rejoining": false, "storage": null, "applied": 1});
        events.readiness_seen(ready.clone());
        let mut moved = ready.clone();
        moved["applied"] = 9.into();
        events.readiness_seen(moved);
        events.readiness_seen(serde_json::json!({"ok": false, "reason": "no known leader", "rejoining": false, "storage": null}));
        let first = pushed.try_recv().unwrap();
        assert_eq!(first.event, "readiness");
        let second = pushed.try_recv().unwrap();
        assert!(second.data.contains("no known leader"));
        assert!(pushed.try_recv().is_err());
    }
}
