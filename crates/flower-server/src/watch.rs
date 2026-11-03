//! Watch hub: shared query evaluation fanned out to SSE subscribers.
//!
//! Each distinct query (database, module, name, canonical arguments and
//! the principal it runs as) evaluates once per change
//! that can affect it; every subscriber on that key shares the
//! evaluation. Every member serves watches: the first snapshot reads the
//! way the query declares, and later refreshes take one fresh-read
//! barrier for the whole batch of affected entries (the leader's lease
//! or a quorum round, or a follower's read fence) and then run in
//! parallel on the engine reader, each under a refresh admission permit,
//! off the async workers, without the engine lock and without holding
//! the entry table.
//!
//! Wakes follow what each result observed. An evaluation carries the
//! certificate of its reads (see [`flower_engine::certificate`]): the
//! records, collection scans and index ranges it read, derived reads
//! expanded into theirs, plus whether it polled the clock or declared
//! when its result changes. A commit notice wakes an entry only when the
//! engine's change journal shows a commit since its evaluation touching
//! that certificate, when the database's code changed, or when the
//! journal cannot tell (a gap such as a snapshot install, or changes
//! that aged out); entries on unrelated rows cost one journal check and
//! no evaluation. A result that declared `changes_at` refreshes when the
//! clock reaches that instant, and one that polled the clock (`now`)
//! every [`POLL_INTERVAL`]. Subscribers' access decisions carry their
//! own certificates: a commit touching what a decision read
//! reauthorizes the entry's subscribers without evaluating, and a
//! decision that declared an instant is rechecked then.
//!
//! Access is decided per subscriber, never shared: subscribers with the
//! same principal share an evaluation, but each one's own credentials are
//! presented again before every delivery. A refresh pins one read view,
//! reauthorizes each subscriber on it (alias resolution and the module's
//! authorization hook, as for any call), then evaluates the query on the
//! same view, so every delivered snapshot is authorized at exactly the
//! revision it shows. A subscriber the hook now denies (a revoked session,
//! an expired token, a removed alias) gets an error event and its stream
//! ends; one whose principal changed (refreshed claims, another tenant)
//! ends with `watch_scope_changed` so it reconnects into the right entry.
//! For revocations nothing records, such as a token expiring without a
//! declared instant, every subscriber is also reauthorized each
//! [`REAUTHORIZE_INTERVAL`], which bounds how long such a stream outlives
//! its credentials.
//!
//! When a re-evaluation renders different bytes at a newer revision, the
//! hub sends it to each subscriber authorized at that revision; one that
//! joined during the refresh, and so was authorized earlier, is held back
//! and caught up by another round. Publications never go backwards in
//! revision. Every published value carries an identity; a subscriber
//! that asked for deltas and holds the value the refresh diffed against
//! receives an RFC 6902 patch naming the identity it applies to
//! (`base`) instead of the whole value, whenever the patch is smaller.
//! Slow subscribers hold bounded queues: a full queue drops the
//! subscriber, whose client reconnects for a fresh snapshot.
//!
//! A subscription's later rounds run on this member's clock and without
//! the subscribing request's deadline: a gateway's pinned `timestamp_ms`
//! would freeze a clock-reading query and every token's expiry, and a
//! request's `x-flower-timeout-ms` bounds that request, not the stream
//! (nor the other subscribers sharing its entry). Arguments and
//! credentials are held once, shared by every round, and the hub holds
//! at most [`DEFAULT_MAX_RETAINED_BYTES`] of them
//! ([`Hub::with_max_retained_bytes`]): past it, a subscription is refused
//! as an overload, to retry once watches end.
//!
//! When a refresh fails (this member could not prove a fresh read, or
//! the query itself now fails), the hub sends each affected subscriber
//! an error event and ends its stream: the client reconnects instead of
//! keeping a stale value alive on keepalives. Between commits, every
//! second, the hub also refreshes the entries whose query declares a
//! staleness bound this member is past: a member cut off from its
//! leader hears no commits, and those watches end there instead.
//! Stopping the hub ends every stream the same way.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use flower_cluster::admission::{Class, input_bytes};
use flower_cluster::exec::now_ms;
use flower_cluster::node::{Node, NodeError};
use flower_cluster::read::{ReadOptions, ReadPlan};
use flower_cluster::telemetry::{self, Class as _, Delivery, OTEL_TARGET, Observer};
use flower_engine::auth::Authorized;
use flower_engine::certificate::{Anchor, Certificate, Validity};
use flower_engine::descriptors::ReadConsistency;
use flower_engine::engine::{CallOptions, EngineReader, ReadView};
use flower_engine::json::render;
use flower_engine::query_cache::CodeVersion;
use flower_sdk::{Failure, Principal, Value};
use opentelemetry::trace::SpanContext;
use tokio::sync::{Mutex, Notify, mpsc, watch};
use tracing::Instrument;

/// Subscriber queue depth: past this, the hub drops the lagged
/// subscriber instead of buffering.
const SUBSCRIBER_BUFFER: usize = 32;
/// Idle sweep for entries whose subscribers all disconnected.
const SWEEP_INTERVAL: Duration = Duration::from_secs(60);
/// How often watches of queries declaring a staleness bound check it
/// between commits.
const BOUND_CHECK_INTERVAL: Duration = Duration::from_secs(1);
/// Default bound on how long a watch outlives credentials that stop
/// being accepted without any commit, such as an expiring token.
pub const REAUTHORIZE_INTERVAL: Duration = Duration::from_secs(30);
/// Default refresh period of results that polled the clock, as v1's.
pub const POLL_INTERVAL: Duration = Duration::from_millis(250);
/// Failure code ending a watch whose subscriber now runs as another
/// principal.
pub const SCOPE_CHANGED: &str = "watch_scope_changed";

/// Default bound on the arguments and credentials every subscriber
/// holds together, in bytes: 64 MiB.
pub const DEFAULT_MAX_RETAINED_BYTES: usize = 64 * 1024 * 1024;

/// What a subscriber costs the bound beyond its arguments and
/// credentials, so empty ones are bounded too.
const SUBSCRIBER_BYTES: usize = 512;

/// Map an engine failure into a node failure.
fn engine_error(error: flower_engine::error::Error) -> NodeError {
    NodeError::Failure {
        code: "engine".to_string(),
        message: error.to_string(),
    }
}

/// Map a host failure into a node failure, keeping its code and, for a
/// call's own failure, its details.
fn failed(failure: Failure) -> NodeError {
    NodeError::failure(failure)
}

/// A hub publication: the rendered value at a revision.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HubEvent {
    /// Database revision at evaluation.
    pub revision: u64,
    /// Rendered JSON value bytes.
    pub value: Vec<u8>,
    /// The value's identity on the stream that carries it: a later
    /// delta names the identity it applies to.
    pub id: u64,
    /// The clock the evaluation read: at one revision, a later clock is
    /// the newer value.
    evaluated_ms: u64,
}

impl HubEvent {
    /// Whether it reflects a later state than `other`: a newer revision,
    /// or the same one at a later clock.
    fn newer_than(&self, other: &HubEvent) -> bool {
        (self.revision, self.evaluated_ms) > (other.revision, other.evaluated_ms)
    }
}

/// A change from the value a stream holds to a newer one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HubDelta {
    /// Database revision of the newer value.
    pub revision: u64,
    /// The newer value's identity.
    pub id: u64,
    /// The identity of the value the patch applies to: the last one the
    /// stream carried.
    pub base: u64,
    /// RFC 6902 operations, as a JSON array.
    pub patch: Arc<str>,
}

/// One message on a subscriber's stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HubMessage {
    /// A changed value, whole.
    Snapshot(HubEvent),
    /// A changed value, as a patch to the previous one.
    Delta(HubDelta),
    /// The watch ended; the stream closes after this.
    Error {
        /// Origin status, preserved even for payload-free application failures.
        status: u16,
        /// Failure code (`forward`, `quorum`, `unauthorized`, an engine
        /// code, ...).
        code: String,
        /// Human-readable message.
        message: String,
        /// A call's own failure's details, rendered as JSON.
        details: Option<String>,
    },
}

impl HubMessage {
    /// The terminal message for a failed refresh.
    fn from_error(error: &NodeError) -> Self {
        let (code, message) = match error {
            NodeError::Forward { leader } => (
                "forward".to_string(),
                format!("leadership moved; leader is {leader:?}"),
            ),
            NodeError::Failure { code, message } | NodeError::Call { code, message, .. } => {
                (code.clone(), message.clone())
            }
            NodeError::Fatal(message) => ("fatal".to_string(), message.clone()),
            NodeError::Overloaded { message, .. } => ("overloaded".to_string(), message.clone()),
        };
        let details = error
            .call_failure()
            .and_then(|failure| failure.details)
            .and_then(|details| render(&details).ok())
            .map(|json| String::from_utf8_lossy(&json).into_owned());
        HubMessage::Error {
            status: match crate::api::status_of(&code) {
                500 if matches!(error, NodeError::Call { .. }) => 422,
                status => status,
            },
            code,
            message,
            details,
        }
    }

    /// The terminal message for a subscriber access now refuses.
    fn denied(failure: &Failure) -> Self {
        HubMessage::Error {
            status: 403,
            code: failure.code.clone(),
            message: failure.message.clone(),
            details: None,
        }
    }

    /// The terminal message for a subscriber now running as someone else.
    fn scope_changed() -> Self {
        HubMessage::Error {
            status: 503,
            code: SCOPE_CHANGED.to_string(),
            message: "the watch's principal changed; reconnect for a fresh snapshot".to_string(),
            details: None,
        }
    }
}

/// Canonical bytes of a principal, empty when anonymous.
fn principal_bytes(principal: Option<&Principal>) -> Result<Vec<u8>, NodeError> {
    let Some(principal) = principal else {
        return Ok(Vec::new());
    };
    let mut fields = std::collections::BTreeMap::new();
    fields.insert(
        "subject".to_string(),
        Value::String(principal.subject.clone()),
    );
    if let Some(tenant) = &principal.tenant {
        fields.insert("tenant".to_string(), Value::String(tenant.clone()));
    }
    if let Some(claims) = &principal.claims {
        fields.insert("claims".to_string(), claims.clone());
    }
    render(&Value::Object(fields)).map_err(engine_error)
}

/// A shareable query identity: subscribers share one evaluation only
/// when everything the evaluation depends on matches, the whole
/// principal the call runs as (subject, tenant and claims) included.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct QueryKey {
    /// Database name.
    db: Vec<u8>,
    /// Module name.
    module: String,
    /// Method name, as callers reach it over HTTP.
    name: String,
    /// Canonical argument bytes.
    args: Vec<u8>,
    /// Canonical bytes of the principal the call runs as, empty when
    /// anonymous.
    principal: Vec<u8>,
}

impl QueryKey {
    /// Key a subscription by everything its evaluation depends on.
    fn new(
        db: &[u8],
        module: &str,
        name: &str,
        args: &Value,
        principal: Option<&Principal>,
    ) -> Result<Self, NodeError> {
        Ok(QueryKey {
            db: db.to_vec(),
            module: module.to_string(),
            name: name.to_string(),
            args: render(args).map_err(engine_error)?,
            principal: principal_bytes(principal)?,
        })
    }
}

/// What one evaluation needs, cloned out of the entry table.
#[derive(Clone)]
struct Spec {
    db: Vec<u8>,
    module: String,
    name: String,
    /// Shared by every round, never copied.
    args: Arc<Value>,
    /// The evaluation's options: standing (no request deadline, the
    /// member's clock) and without credentials, which each subscriber
    /// presents itself.
    options: CallOptions,
}

/// What an evaluation or an access decision observed, and where.
#[derive(Clone, Debug)]
struct Observed {
    certificate: Arc<Certificate>,
    /// Its place in the engine's change journal; `None` when the
    /// journal could not follow it, which wakes it on every commit.
    anchor: Option<Anchor>,
    /// The code it ran.
    code: CodeVersion,
    /// The clock it read.
    evaluated_ms: u64,
    /// The key catalog's mark before its snapshot.
    keys: u64,
}

impl Observed {
    /// What a view's evaluation of `db` at `timestamp_ms` observed.
    fn on(
        view: &ReadView,
        db: &[u8],
        certificate: Arc<Certificate>,
        timestamp_ms: u64,
    ) -> Result<Self, NodeError> {
        Ok(Observed {
            certificate,
            anchor: view.anchor(db).map_err(failed)?,
            code: view.code_version(db),
            evaluated_ms: timestamp_ms,
            keys: view.key_mark(),
        })
    }

    /// Whether commits to `db` through `revision`, now running `code`,
    /// may have changed it. When they cannot, it moves forward to
    /// `revision`, so the next check looks at newer commits only.
    fn stale(
        &mut self,
        reader: &EngineReader,
        db: &[u8],
        code: &CodeVersion,
        revision: u64,
    ) -> bool {
        if self.code != *code {
            return true;
        }
        if self.certificate.is_empty() {
            return false;
        }
        let Some(anchor) = self.anchor.as_mut() else {
            return true;
        };
        if anchor.revision >= revision {
            return false;
        }
        if !reader.unchanged(db, *anchor, revision, &self.certificate) {
            return true;
        }
        anchor.revision = revision;
        false
    }

    /// The wall-clock instant (milliseconds) its result may change
    /// without commits: a declared instant, or the next poll when
    /// `poll` is given and it polled.
    fn due_ms(&self, poll: Option<Duration>) -> Option<u64> {
        match self.certificate.validity() {
            Validity::Stable => None,
            Validity::Until(at) => Some(at),
            Validity::Polled => {
                poll.map(|poll| self.evaluated_ms.saturating_add(poll.as_millis() as u64))
            }
        }
    }
}

/// One subscriber's stream and the access it presents.
struct Subscriber {
    /// Stable within the hub, so a refresh finds it again.
    id: u64,
    tx: mpsc::Sender<HubMessage>,
    /// Credentials and asserted principal, presented on every
    /// reauthorization: standing options, shared by every round.
    options: Arc<CallOptions>,
    /// Revision its access was last decided at.
    authorized_at: u64,
    /// What its latest access decision read.
    access: Observed,
    /// The value it holds, under the hub's identity for it.
    last: Arc<HubEvent>,
    /// The identity its stream last carried for that value.
    delivered: u64,
    /// Whether it takes deltas.
    deltas: bool,
    /// Its `/v1/watch` request's trace context, when telemetry is on:
    /// write-woken deliveries of a sampled one are its children.
    trace: Option<SpanContext>,
    /// Its share of the hub's bound, given back when it goes.
    _charge: Charge,
}

/// Bytes a subscriber holds against [`Hub::with_max_retained_bytes`],
/// given back when it is dropped, however it goes.
struct Charge {
    retained: Arc<AtomicUsize>,
    bytes: usize,
}

impl Drop for Charge {
    fn drop(&mut self) {
        self.retained.fetch_sub(self.bytes, Ordering::Relaxed);
    }
}

/// The options a subscription keeps for its later rounds: the member's
/// clock and no request deadline.
fn standing(options: &CallOptions) -> CallOptions {
    CallOptions {
        deadline: None,
        timestamp_ms: None,
        ..options.clone()
    }
}

/// A refresh's patch from the entry value it diffed against.
#[derive(Clone, Debug)]
struct Delta {
    /// Identity of the value the patch applies to.
    from: u64,
    /// RFC 6902 operations, as a JSON array.
    patch: Arc<str>,
}

struct Entry {
    /// What to evaluate.
    spec: Spec,
    /// The principal every subscriber here runs as.
    principal: Option<Principal>,
    /// Newest evaluation, if any.
    last: Option<Arc<HubEvent>>,
    /// What the newest evaluation observed.
    observed: Option<Observed>,
    /// Live subscribers.
    subscribers: Vec<Subscriber>,
}

impl Entry {
    /// Recheck a queued catch-up against what is installed now. Another
    /// refresh may have covered it while it waited in the pump's queue.
    fn catch_up_mode(
        &self,
        now: u64,
        poll: Duration,
        mut missed: impl FnMut(&mut Observed) -> bool,
    ) -> Option<Mode> {
        if self.observed.as_ref().is_none_or(|observed| {
            missed(&mut observed.clone()) || observed.due_ms(Some(poll)).is_some_and(|at| at <= now)
        }) || self.last.as_ref().is_none_or(|last| {
            self.subscribers
                .iter()
                .any(|subscriber| last.newer_than(&subscriber.last))
        }) {
            return Some(Mode::Evaluate);
        }
        self.subscribers
            .iter()
            .any(|subscriber| {
                missed(&mut subscriber.access.clone())
                    || subscriber.access.due_ms(None).is_some_and(|at| at <= now)
            })
            .then_some(Mode::Reauthorize)
    }

    /// Offer an evaluation to every subscriber: each gets it when it is
    /// newer than what it holds, renders differently, and its access was
    /// decided at or after the evaluation's revision; as `delta` when it
    /// takes deltas and holds the value the delta applies to. A full or
    /// closed queue drops that subscriber. The evaluation becomes the
    /// entry's newest, with what it `observed`, unless one is newer.
    /// Returns whether some subscriber was held back for want of a fresh
    /// decision.
    fn publish(
        &mut self,
        event: Arc<HubEvent>,
        delta: Option<&Delta>,
        observed: Option<Observed>,
    ) -> bool {
        self.publish_reporting(event, delta, observed, &mut |_, _, _| {})
    }

    /// [`Entry::publish`], reporting how each subscriber's delivery
    /// ended (`delivered` with the bytes sent, `unchanged`, or `ended`
    /// when its queue was full or closed); a subscriber held back for a
    /// fresh decision is not reported.
    fn publish_reporting(
        &mut self,
        event: Arc<HubEvent>,
        delta: Option<&Delta>,
        observed: Option<Observed>,
        report: &mut dyn FnMut(u64, Delivery, usize),
    ) -> bool {
        let mut held = false;
        self.subscribers.retain_mut(|subscriber| {
            if !event.newer_than(&subscriber.last) {
                report(subscriber.id, Delivery::Unchanged, 0);
                return true;
            }
            if subscriber.authorized_at < event.revision {
                held = true;
                return true;
            }
            if event.value == subscriber.last.value {
                subscriber.last = event.clone();
                report(subscriber.id, Delivery::Unchanged, 0);
                return true;
            }
            let message = match delta {
                Some(delta) if subscriber.deltas && delta.from == subscriber.last.id => {
                    HubMessage::Delta(HubDelta {
                        revision: event.revision,
                        id: event.id,
                        base: subscriber.delivered,
                        patch: delta.patch.clone(),
                    })
                }
                _ => HubMessage::Snapshot((*event).clone()),
            };
            let bytes = match &message {
                HubMessage::Delta(delta) => delta.patch.len(),
                _ => event.value.len(),
            };
            let sent = subscriber.tx.try_send(message).is_ok();
            if sent {
                subscriber.last = event.clone();
                subscriber.delivered = event.id;
                report(subscriber.id, Delivery::Delivered, bytes);
            } else {
                report(subscriber.id, Delivery::Ended, 0);
            }
            sent
        });
        if self
            .last
            .as_ref()
            .is_none_or(|last| !last.newer_than(&event))
        {
            self.last = Some(event);
            if observed.is_some() {
                self.observed = observed;
            }
        }
        held
    }

    /// End every subscriber's stream with an error event.
    fn fail(self, message: &HubMessage) {
        for subscriber in self.subscribers {
            let _ = subscriber.tx.try_send(message.clone());
        }
    }

    /// The earliest wall-clock instant the clock may change the
    /// entry's result or one of its subscribers' access decisions.
    fn due_ms(&self, poll: Duration) -> Option<u64> {
        let evaluation = self
            .observed
            .as_ref()
            .and_then(|observed| observed.due_ms(Some(poll)));
        self.subscribers
            .iter()
            .filter_map(|subscriber| subscriber.access.due_ms(None))
            .chain(evaluation)
            .min()
    }

    /// Whether a key-catalog change calls for a round: an evaluating one
    /// when the result resolved a managed key, else a reauthorizing one
    /// when a subscriber's decision did.
    fn keyed(&self) -> Option<Mode> {
        if self
            .observed
            .as_ref()
            .is_none_or(|observed| observed.certificate.reads_keys())
        {
            return Some(Mode::Evaluate);
        }
        self.subscribers
            .iter()
            .any(|subscriber| subscriber.access.certificate.reads_keys())
            .then_some(Mode::Reauthorize)
    }

    /// Whether commits to its database through `revision` call for a
    /// round: an evaluating one when the result may have changed, else
    /// a reauthorizing one when a subscriber's decision may have.
    fn woken(&mut self, reader: &EngineReader, code: &CodeVersion, revision: u64) -> Option<Mode> {
        let db = self.spec.db.clone();
        let evaluate = self
            .observed
            .as_mut()
            .is_none_or(|observed| observed.stale(reader, &db, code, revision));
        if evaluate {
            return Some(Mode::Evaluate);
        }
        self.subscribers
            .iter_mut()
            .any(|subscriber| subscriber.access.stale(reader, &db, code, revision))
            .then_some(Mode::Reauthorize)
    }
}

/// What a round does besides reauthorizing every subscriber.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    /// Evaluate the query too, behind a fresh-read barrier.
    Evaluate,
    /// Only reauthorize, on this member's own state.
    Reauthorize,
}

/// One entry's refresh: what to evaluate and whom to reauthorize.
struct Round {
    mode: Mode,
    key: QueryKey,
    spec: Spec,
    principal: Option<Principal>,
    presented: Vec<(u64, Arc<CallOptions>)>,
    /// The entry's newest value, for the delta; `None` when no
    /// subscriber takes deltas.
    previous: Option<Arc<HubEvent>>,
}

/// A subscriber's fresh access decision.
enum Verdict {
    /// Still authorized, as the entry's principal, by a decision that
    /// observed this.
    Keep(Observed),
    /// End the stream with this message.
    End(HubMessage),
}

/// A round's new evaluation.
struct Evaluation {
    value: Vec<u8>,
    /// The clock it read.
    evaluated_ms: u64,
    /// Patch from the round's previous value, when smaller than the
    /// value itself.
    delta: Option<Delta>,
    observed: Observed,
}

/// One entry's refreshed state, all decided on one view.
struct Refreshed {
    revision: u64,
    verdicts: Vec<(u64, Verdict)>,
    /// The new evaluation, unless the round only reauthorized or no
    /// subscriber kept access.
    evaluation: Option<Evaluation>,
}

/// The patch turning `previous` into `next`, both rendered JSON, when
/// it is smaller than `next` itself.
fn delta(previous: &HubEvent, next: &[u8]) -> Option<Delta> {
    if previous.value == next {
        return None;
    }
    let before: serde_json::Value = serde_json::from_slice(&previous.value).ok()?;
    let after: serde_json::Value = serde_json::from_slice(next).ok()?;
    let operations = crate::json_patch::diff(&before, &after)?;
    let patch = serde_json::to_string(&operations).ok()?;
    (patch.len() < next.len()).then(|| Delta {
        from: previous.id,
        patch: patch.into(),
    })
}

impl Round {
    /// Reauthorize every subscriber on one view and evaluate the query
    /// there as the entry's principal. Subscribers presenting the same
    /// credentials and asserted principal share one decision.
    fn run(self, reader: &EngineReader) -> Result<Refreshed, NodeError> {
        let Spec {
            db,
            module,
            name,
            args,
            options,
        } = &self.spec;
        // One clock for every decision and the evaluation, so each
        // validity is relative to what they read.
        let timestamp = options.timestamp_ms.unwrap_or_else(now_ms);
        let view = reader.view().map_err(failed)?;
        let revision = view.revision(db).map_err(failed)?;
        let mut decided: HashMap<Vec<u8>, Result<(Authorized, Observed), Failure>> = HashMap::new();
        let mut authorized = None;
        let mut verdicts = Vec::with_capacity(self.presented.len());
        for (id, presented) in &self.presented {
            let memo = presentation_key(presented)?;
            let decision = match decided.get(&memo) {
                Some(decision) => decision.clone(),
                None => {
                    let presented = CallOptions {
                        timestamp_ms: Some(timestamp),
                        ..(**presented).clone()
                    };
                    let decision =
                        match view.authorize_certified(db, module, name, args, &presented) {
                            Ok((decision, certificate)) => Ok((
                                decision,
                                Observed::on(&view, db, Arc::new(certificate), timestamp)?,
                            )),
                            Err(failure) => Err(failure),
                        };
                    decided.insert(memo, decision.clone());
                    decision
                }
            };
            let verdict = match decision {
                Err(failure) => Verdict::End(HubMessage::denied(&failure)),
                Ok((decision, _)) if decision.principal() != self.principal.as_ref() => {
                    Verdict::End(HubMessage::scope_changed())
                }
                Ok((decision, observed)) => {
                    authorized.get_or_insert(decision);
                    Verdict::Keep(observed)
                }
            };
            verdicts.push((*id, verdict));
        }
        let evaluation = match authorized.filter(|_| self.mode == Mode::Evaluate) {
            None => None,
            Some(authorized) => {
                let options = CallOptions {
                    timestamp_ms: Some(timestamp),
                    ..options.clone()
                };
                let (value, certificate) = view
                    .query_certified(db, &authorized, args, &options)
                    .map_err(failed)?;
                let value = render(&value).map_err(engine_error)?;
                Some(Evaluation {
                    delta: self
                        .previous
                        .as_deref()
                        .and_then(|previous| delta(previous, &value)),
                    value,
                    evaluated_ms: timestamp,
                    observed: Observed::on(&view, db, certificate, timestamp)?,
                })
            }
        };
        Ok(Refreshed {
            revision,
            verdicts,
            evaluation,
        })
    }

    /// Run under a refresh permit: in parallel with other refreshes and
    /// with queries, within the node's evaluation pool. Callers
    /// linearize first.
    async fn refresh(self, node: &Node) -> Result<Refreshed, NodeError> {
        let permit = node
            .admission()
            .acquire(Class::Refresh, &self.spec.db, 0)
            .await?;
        // Evaluator spans are the delivery's children, when one traces.
        let span = tracing::Span::current();
        node.with_reader(move |reader| {
            let _entered = span.enter();
            let _permit = permit;
            self.run(reader)
        })
        .await
    }
}

/// A commit that woke watches: when the hub heard of it, the revision
/// it made, and the sampled writer batch that committed it.
#[derive(Clone, Debug)]
struct Wake {
    at: std::time::Instant,
    revision: u64,
    batch: Option<SpanContext>,
}

/// The write-woken deliveries of one refresh, observed when telemetry
/// is on: `flower.watch.delivery.duration` for every subscriber of an
/// evaluating round, from the wake to its changed result enqueued, its
/// result found unchanged, or its watch ended; and a
/// `flower.watch.deliver` span, a child of its `/v1/watch` request's,
/// for each subscriber in a sampled trace (the round's evaluation runs
/// inside the first one's).
struct Deliveries {
    observer: Arc<Observer>,
    wake: Wake,
    /// Subscribers watched, with their spans when sampled.
    watched: HashMap<u64, Option<tracing::Span>>,
    /// Each entry's span its round runs in.
    rounds: HashMap<QueryKey, tracing::Span>,
}

impl Deliveries {
    fn new(observer: Arc<Observer>, wake: Wake) -> Self {
        Deliveries {
            observer,
            wake,
            watched: HashMap::new(),
            rounds: HashMap::new(),
        }
    }

    /// Watch the deliveries to `entry`'s subscribers.
    fn watch(&mut self, key: &QueryKey, entry: &Entry) {
        for subscriber in &entry.subscribers {
            let span = subscriber
                .trace
                .as_ref()
                .filter(|trace| trace.is_sampled())
                .map(|trace| {
                    let span = tracing::info_span!(
                        target: OTEL_TARGET,
                        parent: None,
                        "flower.watch.deliver",
                        flower.watch.woken_revision = telemetry::int(self.wake.revision),
                        flower.watch.revision = tracing::field::Empty,
                        flower.watch.bytes = tracing::field::Empty,
                        flower.watch.delay_ms = tracing::field::Empty,
                        flower.group = self.observer.group(),
                        outcome = tracing::field::Empty,
                    );
                    telemetry::adopt(&span, trace);
                    if let Some(batch) = &self.wake.batch {
                        telemetry::link(&span, batch);
                    }
                    span
                });
            if let Some(span) = &span {
                self.rounds
                    .entry(key.clone())
                    .or_insert_with(|| span.clone());
            }
            self.watched.insert(subscriber.id, span);
        }
    }

    /// The span `key`'s round runs in.
    fn round(&self, key: &QueryKey) -> tracing::Span {
        self.rounds
            .get(key)
            .cloned()
            .unwrap_or_else(tracing::Span::none)
    }

    /// Subscriber `id`'s delivery ended with `outcome`: `bytes` sent at
    /// `revision` when delivered.
    fn report(&mut self, id: u64, outcome: Delivery, revision: u64, bytes: usize) {
        let Some(span) = self.watched.remove(&id) else {
            return;
        };
        let elapsed = self.wake.at.elapsed();
        self.observer.watch_delivery(outcome, elapsed);
        if let Some(span) = span {
            span.record("outcome", outcome.name());
            span.record("flower.watch.delay_ms", elapsed.as_secs_f64() * 1000.0);
            if outcome == Delivery::Delivered {
                span.record("flower.watch.revision", telemetry::int(revision));
                span.record("flower.watch.bytes", telemetry::int(bytes));
            }
        }
    }
}

/// What a subscriber presents to the hook, as bytes: credentials and the
/// asserted principal.
fn presentation_key(options: &CallOptions) -> Result<Vec<u8>, NodeError> {
    let mut key =
        render(options.credentials.as_ref().unwrap_or(&Value::Null)).map_err(engine_error)?;
    key.push(0);
    key.extend(principal_bytes(options.principal.as_ref())?);
    Ok(key)
}

/// A first subscription's decision and evaluation, on one view.
struct First {
    authorized: Authorized,
    access: Observed,
    revision: u64,
    value: Vec<u8>,
    observed: Observed,
    evaluated_ms: u64,
}

/// Sleep until the wall clock reaches `at` (milliseconds), or forever.
async fn sleep_until_ms(at: Option<u64>) {
    match at {
        Some(at) => tokio::time::sleep(Duration::from_millis(at.saturating_sub(now_ms()))).await,
        None => std::future::pending().await,
    }
}

/// The watch hub over a node.
pub struct Hub {
    node: Arc<Node>,
    entries: Mutex<HashMap<QueryKey, Entry>>,
    next_id: AtomicU64,
    /// The last identity given a published value.
    next_event: AtomicU64,
    /// Entries with a subscriber held back or a change possibly
    /// missed, for another round.
    behind: std::sync::Mutex<HashSet<QueryKey>>,
    /// The newest revision of each database whose commit notice the
    /// pump handled; held under the entry table's lock.
    noticed: std::sync::Mutex<HashMap<Vec<u8>, u64>>,
    /// The earliest instant (wall-clock milliseconds) the clock may
    /// change some entry.
    due: std::sync::Mutex<Option<u64>>,
    /// Query evaluations refresh rounds ran.
    evaluations: AtomicU64,
    wake: Notify,
    reauthorize: Duration,
    poll: Duration,
    /// Bytes every subscriber's arguments and credentials hold.
    retained: Arc<AtomicUsize>,
    max_retained: usize,
}

impl Hub {
    /// Open a hub; [`Hub::run`] pumps it until stopped.
    pub fn new(node: Arc<Node>) -> Self {
        Hub {
            node,
            entries: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(0),
            next_event: AtomicU64::new(0),
            behind: std::sync::Mutex::new(HashSet::new()),
            noticed: std::sync::Mutex::new(HashMap::new()),
            due: std::sync::Mutex::new(None),
            evaluations: AtomicU64::new(0),
            wake: Notify::new(),
            reauthorize: REAUTHORIZE_INTERVAL,
            poll: POLL_INTERVAL,
            retained: Arc::new(AtomicUsize::new(0)),
            max_retained: DEFAULT_MAX_RETAINED_BYTES,
        }
    }

    /// Hold at most this many bytes of subscribers' arguments and
    /// credentials; a subscription past it is refused as an overload.
    pub fn with_max_retained_bytes(mut self, bytes: usize) -> Self {
        self.max_retained = bytes;
        self
    }

    /// Take `bytes` of the bound for one subscriber, or refuse.
    fn charge(&self, bytes: usize) -> Result<Charge, NodeError> {
        let bytes = bytes.saturating_add(SUBSCRIBER_BYTES);
        let mut held = self.retained.load(Ordering::Relaxed);
        loop {
            let Some(next) = held
                .checked_add(bytes)
                .filter(|next| *next <= self.max_retained)
            else {
                return Err(NodeError::Overloaded {
                    message: format!(
                        "watches hold {held} of the hub's {} bytes of arguments and credentials",
                        self.max_retained
                    ),
                    retry_after_ms: 1_000,
                });
            };
            match self.retained.compare_exchange_weak(
                held,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    return Ok(Charge {
                        retained: self.retained.clone(),
                        bytes,
                    });
                }
                Err(actual) => held = actual,
            }
        }
    }

    /// Reauthorize every subscriber at least this often, commits or not.
    pub fn with_reauthorize_interval(mut self, interval: Duration) -> Self {
        self.reauthorize = interval;
        self
    }

    /// Refresh results that polled the clock this often.
    pub fn with_poll_interval(mut self, interval: Duration) -> Self {
        self.poll = interval;
        self
    }

    /// Query evaluations refresh rounds ran since the hub opened (first
    /// snapshots not included), for tests and metrics.
    pub fn evaluations(&self) -> u64 {
        self.evaluations.load(Ordering::Relaxed)
    }

    /// A fresh identity for a published value.
    fn event_id(&self) -> u64 {
        self.next_event.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Ask the pump for another evaluating round on `key`.
    fn catch_up(&self, key: QueryKey) {
        self.behind
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(key);
        self.wake.notify_one();
    }

    /// The newest noticed revision of `db`.
    fn noticed(&self, db: &[u8]) -> Option<u64> {
        self.noticed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(db)
            .copied()
    }

    /// Note that the clock may change something at `at`.
    fn schedule(&self, at: Option<u64>) {
        let Some(at) = at else {
            return;
        };
        let mut due = self
            .due
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if due.is_none_or(|due| at < due) {
            *due = Some(at);
            // The pump sleeps until the instant it last read: wake it
            // to sleep until this earlier one.
            self.wake.notify_one();
        }
    }

    /// The earliest scheduled instant.
    fn next_due(&self) -> Option<u64> {
        *self
            .due
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Whether commits the pump already handled may have changed what
    /// an evaluation installed late observed: the pump checked them
    /// against what the entry held then.
    fn missed(&self, db: &[u8], observed: &mut Observed) -> bool {
        // A key change that began or ended since its snapshot may have
        // been handled already: what resolved a key may predate it.
        if observed.certificate.reads_keys() && observed.keys != self.node.reader().key_mark() {
            return true;
        }
        let Some(noticed) = self.noticed(db) else {
            return false;
        };
        let code = self.node.reader().code_version(db);
        observed.stale(self.node.reader(), db, &code, noticed)
    }

    /// Decide access and evaluate a first snapshot on one reader view,
    /// under a query admission permit. With `declared` (a replica-local
    /// read, unproved), `None` when the view's catalog no longer
    /// declares it so, for the caller to prove a fresh read instead.
    async fn first(
        &self,
        db: &[u8],
        module: &str,
        name: &str,
        args: &Value,
        options: &CallOptions,
        declared: Option<ReadConsistency>,
    ) -> Result<Option<First>, NodeError> {
        let bytes = input_bytes(args) + module.len() + name.len();
        let permit = self
            .node
            .admission()
            .acquire(Class::Query, db, bytes)
            .await?;
        let timestamp = options.timestamp_ms.unwrap_or_else(now_ms);
        let (db, module, name, args, options) = (
            db.to_vec(),
            module.to_string(),
            name.to_string(),
            args.clone(),
            CallOptions {
                timestamp_ms: Some(timestamp),
                ..options.clone()
            },
        );
        self.node
            .with_reader(move |reader| {
                let _permit = permit;
                let view = reader.view().map_err(failed)?;
                if declared.is_some()
                    && view.call_consistency(&db, &module, &name, &options) != declared
                {
                    return Ok(None);
                }
                let (authorized, access) = view
                    .authorize_certified(&db, &module, &name, &args, &options)
                    .map_err(failed)?;
                let (value, certificate) = view
                    .query_certified(&db, &authorized, &args, &options)
                    .map_err(NodeError::application)?;
                Ok(Some(First {
                    access: Observed::on(&view, &db, Arc::new(access), timestamp)?,
                    observed: Observed::on(&view, &db, certificate, timestamp)?,
                    authorized,
                    revision: view.revision(&db).map_err(failed)?,
                    value: render(&value).map_err(engine_error)?,
                    evaluated_ms: timestamp,
                }))
            })
            .await
    }

    /// Subscribe to an alias for whole snapshots; see
    /// [`Hub::subscribe_with`].
    pub async fn subscribe(
        &self,
        db: &[u8],
        module: &str,
        name: &str,
        args: &Value,
        options: &CallOptions,
    ) -> Result<(HubEvent, mpsc::Receiver<HubMessage>), NodeError> {
        self.subscribe_with(db, module, name, args, options, false)
            .await
    }

    /// Subscribe to an alias: read it the way the query declares (see
    /// [`Node::plan_read`]), deciding access and evaluating on one reader
    /// view, then register for later changes. With `deltas`, later
    /// values may arrive as [`HubMessage::Delta`]. Returns the initial
    /// event plus the stream. Denials, evaluation failures (unknown
    /// aliases, stale replicas, unproved fresh reads) surface here,
    /// before any streaming starts. Fresh-read proofs run without
    /// holding the entry table.
    pub async fn subscribe_with(
        &self,
        db: &[u8],
        module: &str,
        name: &str,
        args: &Value,
        options: &CallOptions,
        deltas: bool,
    ) -> Result<(HubEvent, mpsc::Receiver<HubMessage>), NodeError> {
        // Later rounds run on the member's clock without the request's
        // deadline; the first snapshot keeps the deadline, never a
        // pinned clock.
        let standing = Arc::new(standing(options));
        let options = &CallOptions {
            timestamp_ms: None,
            ..options.clone()
        };
        let charge = self.charge(
            render(args).map_err(engine_error)?.len() + presentation_key(&standing)?.len(),
        )?;
        let plan = self
            .node
            .plan_read(db, module, name, options, &ReadOptions::default())
            .await?;
        let declared = match plan {
            ReadPlan::Local { declared, .. } => Some(declared),
            ReadPlan::Proved(_) => None,
        };
        let first = match self
            .first(db, module, name, args, options, declared)
            .await?
        {
            Some(first) => first,
            // A deployment changed how it reads meanwhile: read fresh.
            None => {
                self.node.read_barrier().await?;
                self.first(db, module, name, args, options, None)
                    .await?
                    .ok_or_else(|| NodeError::Fatal("a fresh read declined".to_string()))?
            }
        };
        let First {
            authorized,
            mut access,
            revision,
            value,
            mut observed,
            evaluated_ms,
        } = first;
        let key = QueryKey::new(db, module, name, args, authorized.principal())?;
        let event = Arc::new(HubEvent {
            revision,
            value,
            id: self.event_id(),
            evaluated_ms,
        });
        let (tx, rx) = mpsc::channel(SUBSCRIBER_BUFFER);
        let behind = {
            let mut entries = self.entries.lock().await;
            // Commits the pump handled before this registration were
            // checked against what the entry held, not against this
            // evaluation or decision.
            let missed = self.missed(db, &mut observed) | self.missed(db, &mut access);
            let entry = entries.entry(key.clone()).or_insert_with(|| Entry {
                spec: Spec {
                    db: db.to_vec(),
                    module: module.to_string(),
                    name: name.to_string(),
                    args: Arc::new(args.clone()),
                    options: CallOptions {
                        credentials: None,
                        ..(*standing).clone()
                    },
                },
                principal: authorized.principal().cloned(),
                last: None,
                observed: None,
                subscribers: Vec::new(),
            });
            // A refresh may have evaluated something newer while this
            // evaluation ran, without deciding this subscriber's access:
            // another round catches it up.
            let behind = entry
                .last
                .as_ref()
                .is_some_and(|last| last.newer_than(&event));
            // A value the entry already holds keeps the entry's identity
            // for it, so the entry's next delta applies to this stream
            // too; the stream itself names the value by the `id` it got.
            let held = match &entry.last {
                Some(last) if last.value == event.value && behind => last.clone(),
                Some(last) if last.value == event.value => Arc::new(HubEvent {
                    id: last.id,
                    ..(*event).clone()
                }),
                _ => event.clone(),
            };
            if !behind {
                self.schedule(observed.due_ms(Some(self.poll)));
                entry.last = Some(held.clone());
                entry.observed = Some(observed);
            }
            self.schedule(access.due_ms(None));
            entry.subscribers.push(Subscriber {
                id: self.next_id.fetch_add(1, Ordering::Relaxed),
                tx,
                options: standing,
                authorized_at: event.revision,
                access,
                last: held,
                delivered: event.id,
                deltas,
                trace: telemetry::current_context(),
                _charge: charge,
            });
            behind || missed
        };
        if behind {
            self.catch_up(key);
        }
        Ok(((*event).clone(), rx))
    }

    /// Pump commit notices into refreshes until `stop` turns true or the
    /// node's notices close. Lag overruns refresh every entry; the
    /// reauthorization tick reauthorizes every subscriber. Stopping ends
    /// every subscriber's stream.
    pub async fn run(self: Arc<Self>, mut stop: watch::Receiver<bool>) {
        let mut notices = self.node.subscribe_commits();
        let mut sweep = tokio::time::interval(SWEEP_INTERVAL);
        let mut reauthorize = tokio::time::interval_at(
            tokio::time::Instant::now() + self.reauthorize,
            self.reauthorize,
        );
        let mut bounds = tokio::time::interval(BOUND_CHECK_INTERVAL);
        bounds.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        while !*stop.borrow() {
            let due = self.next_due();
            tokio::select! {
                notice = notices.recv() => {
                    match notice {
                        Ok(notice) if notice.keys => self.keys_changed(&notice.db).await,
                        Ok(notice) => {
                            self.commit(&notice.db, notice.revision, notice.batch).await
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                            self.refresh_where(|_, _| Some(Mode::Evaluate)).await
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    }
                }
                () = self.wake.notified() => {
                    let behind: HashSet<QueryKey> = self
                        .behind
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .drain()
                        .collect();
                    self.refresh_where(|key, entry| {
                        behind.contains(key).then(|| {
                            entry.catch_up_mode(now_ms(), self.poll, |observed| self.missed(&key.db, observed))
                        }).flatten()
                    })
                        .await;
                }
                () = sleep_until_ms(due) => self.refresh_due().await,
                _ = reauthorize.tick() => self.refresh_where(|_, _| Some(Mode::Reauthorize)).await,
                _ = bounds.tick() => self.recheck_bounds().await,
                _ = sweep.tick() => self.sweep().await,
                changed = stop.changed() => {
                    if changed.is_err() {
                        break;
                    }
                }
            }
        }
        let stopped = HubMessage::Error {
            status: 503,
            code: "shutdown".to_string(),
            message: "server shutting down".to_string(),
            details: None,
        };
        for (_, entry) in self.entries.lock().await.drain() {
            entry.fail(&stopped);
        }
    }

    /// Handle a commit notice: refresh the entries on `db` whose result
    /// or access the commits through `revision` may have changed, and
    /// only those.
    async fn commit(&self, db: &[u8], revision: u64, batch: Option<SpanContext>) {
        let woken = std::time::Instant::now();
        let reader = self.node.reader();
        let code = reader.code_version(db);
        let woken_keys: HashMap<QueryKey, Mode> = {
            let mut entries = self.entries.lock().await;
            {
                let mut noticed = self
                    .noticed
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let newest = noticed.entry(db.to_vec()).or_insert(revision);
                *newest = (*newest).max(revision);
            }
            entries
                .iter_mut()
                .filter(|(key, _)| key.db == db)
                .filter_map(|(key, entry)| {
                    entry
                        .woken(reader, &code, revision)
                        .map(|mode| (key.clone(), mode))
                })
                .collect()
        };
        if !woken_keys.is_empty() {
            let wake = Wake {
                at: woken,
                revision,
                batch,
            };
            self.refresh_woken(|key, _| woken_keys.get(key).copied(), Some(wake))
                .await;
        }
    }

    /// Handle a key-catalog notice for database `db` (empty: any
    /// database's catalog may have changed): evaluate that database's
    /// entries whose result resolved a managed key and reauthorize those
    /// where only a subscriber's decision did (a token check against a
    /// revoked key ends its stream). No revision moved, and other
    /// databases resolve keys in catalogs of their own, so nothing else
    /// changed; v1 wakes every watch instead.
    async fn keys_changed(&self, db: &[u8]) {
        self.refresh_where(|key, entry| {
            if db.is_empty() || key.db == db {
                entry.keyed()
            } else {
                None
            }
        })
        .await;
    }

    /// Refresh the entries whose clock came due: an evaluating round
    /// when the result's validity ended, a reauthorizing one when only a
    /// subscriber's decision's did.
    async fn refresh_due(&self) {
        let now = now_ms();
        *self
            .due
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        let poll = self.poll;
        self.refresh_where(|_, entry| {
            let evaluation = entry
                .observed
                .as_ref()
                .and_then(|observed| observed.due_ms(Some(poll)));
            if evaluation.is_some_and(|at| at <= now) {
                return Some(Mode::Evaluate);
            }
            entry
                .subscribers
                .iter()
                .any(|subscriber| subscriber.access.due_ms(None).is_some_and(|at| at <= now))
                .then_some(Mode::Reauthorize)
        })
        .await;
        // Rounds rescheduled what they refreshed; the rest still waits.
        let entries = self.entries.lock().await;
        for entry in entries.values() {
            self.schedule(entry.due_ms(poll).filter(|at| *at > now));
        }
    }

    /// Refresh the entries whose query declares a staleness bound this
    /// member is past now. A member cut off from its leader receives no
    /// commits, so its bounded watches would otherwise keep a snapshot
    /// staler than they allow. The refresh proves a fresh read first,
    /// or ends their streams so clients reconnect where the bound holds.
    async fn recheck_bounds(&self) {
        let staleness = self.node.staleness();
        let reader = self.node.reader();
        let past: HashSet<QueryKey> = {
            let entries = self.entries.lock().await;
            entries
                .iter()
                .filter(|(key, entry)| {
                    match reader.call_consistency(
                        &key.db,
                        &key.module,
                        &key.name,
                        &entry.spec.options,
                    ) {
                        Some(ReadConsistency::ReplicaLocal {
                            max_staleness_ms: Some(bound),
                        }) => staleness.is_none_or(|staleness| {
                            staleness > Duration::from_millis(u64::from(bound))
                        }),
                        _ => false,
                    }
                })
                .map(|(key, _)| key.clone())
                .collect()
        };
        if !past.is_empty() {
            self.refresh_where(|key, _| past.contains(key).then_some(Mode::Evaluate))
                .await;
        }
    }

    /// Refresh the entries `select` picks, each in the mode it picks.
    /// One fresh-read barrier covers the batch when any round
    /// evaluates; an evaluating round reauthorizes its subscribers and
    /// evaluates on one view. Reauthorizing rounds read this member's
    /// own state with no fresh-read proof, as v1 does: a revocation that
    /// committed is applied here once it arrives, and a member cut off
    /// from its leader keeps the watches of unbounded queries rather
    /// than ending them for want of a proof. Rounds run in parallel on
    /// the reader, outside the entry table's lock.
    async fn refresh_where(&self, select: impl Fn(&QueryKey, &Entry) -> Option<Mode>) {
        self.refresh_woken(select, None).await;
    }

    /// [`WatchHub::refresh_where`], for rounds a commit woke when
    /// `wake` says so: each evaluating round's subscribers' deliveries
    /// are observed from the wake (see [`Deliveries`]).
    async fn refresh_woken(
        &self,
        select: impl Fn(&QueryKey, &Entry) -> Option<Mode>,
        wake: Option<Wake>,
    ) {
        let mut deliveries = wake
            .filter(|_| telemetry::enabled())
            .map(|wake| Deliveries::new(self.node.observer().clone(), wake));
        let rounds: Vec<Round> = {
            let entries = self.entries.lock().await;
            entries
                .iter()
                .filter_map(|(key, entry)| {
                    let mode = select(key, entry)?;
                    if mode == Mode::Evaluate
                        && let Some(deliveries) = deliveries.as_mut()
                    {
                        deliveries.watch(key, entry);
                    }
                    Some(Round {
                        mode,
                        key: key.clone(),
                        spec: entry.spec.clone(),
                        principal: entry.principal.clone(),
                        presented: entry
                            .subscribers
                            .iter()
                            .map(|subscriber| (subscriber.id, subscriber.options.clone()))
                            .collect(),
                        // Diff only for subscribers that take deltas.
                        previous: entry
                            .subscribers
                            .iter()
                            .any(|subscriber| subscriber.deltas)
                            .then(|| entry.last.clone())
                            .flatten(),
                    })
                })
                .collect()
        };
        if rounds.is_empty() {
            return;
        }
        let barrier = if rounds.iter().any(|round| round.mode == Mode::Evaluate) {
            self.node.read_barrier().await
        } else {
            Ok(())
        };
        let results: Vec<(QueryKey, Result<Refreshed, NodeError>)> = match barrier {
            Err(error) => rounds
                .into_iter()
                .map(|round| (round.key, Err(error.clone())))
                .collect(),
            Ok(()) => {
                let mut evaluations = tokio::task::JoinSet::new();
                for round in rounds {
                    let node = self.node.clone();
                    let key = round.key.clone();
                    let span = deliveries
                        .as_ref()
                        .map_or_else(tracing::Span::none, |deliveries| deliveries.round(&key));
                    evaluations
                        .spawn(async move { (key, round.refresh(&node).await) }.instrument(span));
                }
                let mut results = Vec::with_capacity(evaluations.len());
                while let Some(evaluated) = evaluations.join_next().await {
                    match evaluated {
                        Ok(result) => results.push(result),
                        Err(error) => {
                            let error = NodeError::Fatal(format!("refresh failed: {error}"));
                            self.fail_all(&error).await;
                            return;
                        }
                    }
                }
                results
            }
        };
        let mut behind = HashSet::new();
        let mut entries = self.entries.lock().await;
        for (key, result) in results {
            match result {
                Ok(refreshed) => {
                    let Some(entry) = entries.get_mut(&key) else {
                        continue;
                    };
                    for (id, verdict) in refreshed.verdicts {
                        let Some(at) = entry
                            .subscribers
                            .iter()
                            .position(|subscriber| subscriber.id == id)
                        else {
                            continue;
                        };
                        match verdict {
                            Verdict::Keep(mut access) => {
                                if self.missed(&key.db, &mut access) {
                                    behind.insert(key.clone());
                                }
                                self.schedule(access.due_ms(None));
                                let subscriber = &mut entry.subscribers[at];
                                subscriber.authorized_at = refreshed.revision;
                                subscriber.access = access;
                            }
                            Verdict::End(message) => {
                                let _ = entry.subscribers.remove(at).tx.try_send(message);
                                if let Some(deliveries) = deliveries.as_mut() {
                                    deliveries.report(id, Delivery::Ended, 0, 0);
                                }
                            }
                        }
                    }
                    if let Some(evaluation) = refreshed.evaluation {
                        self.evaluations.fetch_add(1, Ordering::Relaxed);
                        let Evaluation {
                            value,
                            evaluated_ms,
                            delta,
                            mut observed,
                        } = evaluation;
                        if self.missed(&key.db, &mut observed) {
                            behind.insert(key.clone());
                        }
                        self.schedule(observed.due_ms(Some(self.poll)));
                        let event = Arc::new(HubEvent {
                            revision: refreshed.revision,
                            value,
                            id: self.event_id(),
                            evaluated_ms,
                        });
                        let revision = event.revision;
                        let held = match deliveries.as_mut() {
                            Some(deliveries) => entry.publish_reporting(
                                event,
                                delta.as_ref(),
                                Some(observed),
                                &mut |id, outcome, bytes| {
                                    deliveries.report(id, outcome, revision, bytes);
                                },
                            ),
                            None => entry.publish(event, delta.as_ref(), Some(observed)),
                        };
                        if held {
                            behind.insert(key.clone());
                        }
                    }
                }
                Err(error) => {
                    if let Some(entry) = entries.remove(&key) {
                        if let Some(deliveries) = deliveries.as_mut() {
                            for subscriber in &entry.subscribers {
                                deliveries.report(subscriber.id, Delivery::Ended, 0, 0);
                            }
                        }
                        entry.fail(&HubMessage::from_error(&error));
                    }
                }
            }
        }
        entries.retain(|_, entry| {
            entry
                .subscribers
                .retain(|subscriber| !subscriber.tx.is_closed());
            !entry.subscribers.is_empty()
        });
        drop(entries);
        for key in behind {
            self.catch_up(key);
        }
    }

    /// End every stream with an error.
    async fn fail_all(&self, error: &NodeError) {
        let message = HubMessage::from_error(error);
        for (_, entry) in self.entries.lock().await.drain() {
            entry.fail(&message);
        }
    }

    /// Drop entries whose subscribers all disconnected.
    async fn sweep(&self) {
        let mut entries = self.entries.lock().await;
        entries.retain(|_, entry| {
            entry
                .subscribers
                .retain(|subscriber| !subscriber.tx.is_closed());
            !entry.subscribers.is_empty()
        });
    }

    /// Live watch keys, for tests and metrics.
    pub async fn keys(&self) -> usize {
        self.entries.lock().await.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(revision: u64, value: &str) -> Arc<HubEvent> {
        event_with(revision, value, revision)
    }

    fn event_with(revision: u64, value: &str, id: u64) -> Arc<HubEvent> {
        Arc::new(HubEvent {
            revision,
            value: value.as_bytes().to_vec(),
            id,
            evaluated_ms: 0,
        })
    }

    fn observed() -> Observed {
        Observed {
            certificate: Arc::new(Certificate::default()),
            anchor: None,
            code: CodeVersion::default(),
            evaluated_ms: 0,
            keys: 0,
        }
    }

    fn entry(subscribers: Vec<Subscriber>) -> Entry {
        Entry {
            spec: Spec {
                db: b"db".to_vec(),
                module: "app".to_string(),
                name: "me".to_string(),
                args: Arc::new(Value::Null),
                options: CallOptions::default(),
            },
            principal: None,
            last: None,
            observed: None,
            subscribers,
        }
    }

    fn subscriber(id: u64, authorized_at: u64) -> (Subscriber, mpsc::Receiver<HubMessage>) {
        let (tx, rx) = mpsc::channel(SUBSCRIBER_BUFFER);
        let last = event(1, "a");
        let subscriber = Subscriber {
            id,
            tx,
            options: Arc::new(CallOptions::default()),
            authorized_at,
            access: observed(),
            delivered: last.id,
            last,
            deltas: false,
            trace: None,
            _charge: Charge {
                retained: Arc::new(AtomicUsize::new(0)),
                bytes: 0,
            },
        };
        (subscriber, rx)
    }

    #[test]
    fn queued_catch_up_skips_only_when_evaluation_access_and_delivery_are_current() {
        let (only, _rx) = subscriber(1, 5);
        let mut entry = entry(vec![only]);
        let poll = Duration::from_secs(1);
        assert_eq!(
            entry.catch_up_mode(0, poll, |_| false),
            Some(Mode::Evaluate)
        );
        entry.publish(event(5, "b"), None, Some(observed()));
        assert_eq!(entry.catch_up_mode(0, poll, |_| false), None);
        // A later commit or key change invalidates the queued result.
        assert_eq!(entry.catch_up_mode(0, poll, |_| true), Some(Mode::Evaluate));
        // Fresh evaluation with stale authorization still reauthorizes.
        entry.subscribers[0].access.keys = 7;
        assert_eq!(
            entry.catch_up_mode(0, poll, |observed| observed.keys == 7),
            Some(Mode::Reauthorize)
        );
        // A late subscriber whose delivery was held still needs a round.
        let (late, _late_rx) = subscriber(2, 3);
        entry.subscribers.push(late);
        assert_eq!(
            entry.catch_up_mode(0, poll, |_| false),
            Some(Mode::Evaluate)
        );
    }

    #[test]
    fn deliveries_wait_for_a_decision_at_their_revision() {
        let (current, mut current_rx) = subscriber(1, 5);
        let (joined, mut joined_rx) = subscriber(2, 3);
        let mut entry = entry(vec![current, joined]);
        // Revision 5 was decided for the first subscriber only: the one
        // authorized at 3 is held back, and the hub must run a round.
        assert!(entry.publish(event(5, "b"), None, None));
        assert_eq!(
            current_rx.try_recv().unwrap(),
            HubMessage::Snapshot((*event(5, "b")).clone())
        );
        assert!(joined_rx.try_recv().is_err());
        // Once decided at 5, it receives the value the entry already
        // published, though the entry's own last value is the same.
        entry.subscribers[1].authorized_at = 5;
        assert!(!entry.publish(event(5, "b"), None, None));
        assert_eq!(
            joined_rx.try_recv().unwrap(),
            HubMessage::Snapshot((*event(5, "b")).clone())
        );
        assert!(current_rx.try_recv().is_err());
    }

    #[test]
    fn deliveries_never_repeat_or_go_backwards() {
        let (only, mut rx) = subscriber(1, 9);
        let mut entry = entry(vec![only]);
        assert!(!entry.publish(event(4, "b"), None, None));
        assert!(rx.try_recv().is_ok());
        // Same value at a newer revision: nothing to send.
        assert!(!entry.publish(event(6, "b"), None, None));
        assert!(rx.try_recv().is_err());
        // Older revisions never send.
        assert!(!entry.publish(event(5, "c"), None, None));
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn publishing_reports_how_each_delivery_ended() {
        let (sends, _sends_rx) = subscriber(1, 9);
        let (holds, _holds_rx) = subscriber(2, 9);
        let (gone, gone_rx) = subscriber(3, 9);
        let (late, _late_rx) = subscriber(4, 3);
        drop(gone_rx);
        let mut entry = entry(vec![sends, holds, gone, late]);
        // The second subscriber already holds the value.
        entry.subscribers[1].last = event(2, "b");
        let mut reported = Vec::new();
        let held = entry.publish_reporting(event(4, "b"), None, None, &mut |id, outcome, bytes| {
            reported.push((id, outcome, bytes));
        });
        // The one decided before the revision is held back, unreported.
        assert!(held);
        assert_eq!(
            reported,
            [
                (1, Delivery::Delivered, 1),
                (2, Delivery::Unchanged, 0),
                (3, Delivery::Ended, 0),
            ]
        );
    }

    #[test]
    fn deltas_go_only_to_subscribers_holding_their_base() {
        let (mut wants, mut wants_rx) = subscriber(1, 9);
        wants.deltas = true;
        let (mut stale, mut stale_rx) = subscriber(2, 9);
        stale.deltas = true;
        stale.last = event_with(1, "z", 7);
        stale.delivered = 7;
        let (whole, mut whole_rx) = subscriber(3, 9);
        let mut entry = entry(vec![wants, stale, whole]);
        let delta = Delta {
            from: 1,
            patch: "[]".into(),
        };
        assert!(!entry.publish(event_with(2, "b", 20), Some(&delta), None));
        // Holds the base and asked: a delta naming the identity it holds.
        assert_eq!(
            wants_rx.try_recv().unwrap(),
            HubMessage::Delta(HubDelta {
                revision: 2,
                id: 20,
                base: 1,
                patch: "[]".into(),
            })
        );
        // Holds another value: the whole value. Never asked: the same.
        assert!(matches!(
            stale_rx.try_recv().unwrap(),
            HubMessage::Snapshot(_)
        ));
        assert!(matches!(
            whole_rx.try_recv().unwrap(),
            HubMessage::Snapshot(_)
        ));
        // A value equal to what a stream holds moves it on silently, and
        // the next delta still names the identity the stream carried.
        assert!(!entry.publish(event_with(3, "b", 30), None, None));
        let next = Delta {
            from: 30,
            patch: "[1]".into(),
        };
        assert!(!entry.publish(event_with(4, "c", 40), Some(&next), None));
        assert_eq!(
            wants_rx.try_recv().unwrap(),
            HubMessage::Delta(HubDelta {
                revision: 4,
                id: 40,
                base: 20,
                patch: "[1]".into(),
            })
        );
    }

    #[test]
    fn deltas_are_patches_smaller_than_the_value() {
        let previous = event_with(1, r#"{"items":[1,2,3],"title":"a long enough title"}"#, 5);
        let next = br#"{"items":[1,2,3,4],"title":"a long enough title"}"#;
        let delta = delta(&previous, next).unwrap();
        assert_eq!(delta.from, 5);
        assert_eq!(
            &*delta.patch,
            r#"[{"op":"add","path":"/items/3","value":4}]"#
        );
        // A patch no smaller than the value itself is not worth sending.
        assert!(super::delta(&event_with(1, "1", 1), b"2").is_none());
        assert!(super::delta(&previous, &previous.value).is_none());
    }
}
