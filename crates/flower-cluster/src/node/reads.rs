//! A member's reads and routing: queries as they declare, fresh-read
//! barriers, client requests forwarded to the leader, and where the
//! leader's API serves clients.

use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use flower_engine::engine::CallOptions;
use flower_engine::json::parse_value;
use flower_engine::query_cache::{Encoded, Lookup};
use flower_sdk::Value;
use openraft::BasicNode;

use super::{Node, NodeError, measured_query};
use crate::admission::{Class, RETRY_AFTER, input_bytes};
use crate::flight::{FlightStats, Flights, Landing, Turn};
use crate::peer::{
    FORWARD_FAILED, ForwardedRequest, ForwardedResponse, MemberDesk, MemberError, MemberRequest,
    MemberResponse,
};
use crate::read::{
    ReadConsistency, ReadMetrics, ReadOptions, ReadPlan, ReadSettings, STALE_REPLICA, Served,
};
use crate::recovery::{RECOVERING, RECOVERY_WAIT};
use crate::rejoin::REJOINING;
use crate::stages::{self, Stage};
use crate::telemetry::QueryStage;
use crate::types::NodeId;

/// How long a forwarded request may keep looking for a leader that
/// takes it: an election or two.
const FORWARD_BUDGET: Duration = Duration::from_secs(10);
/// How long the leader may take to answer one forwarded request: long
/// enough for a deployment's backfill. The caller's own deadline
/// usually ends the wait first.
const FORWARD_CALL_TIMEOUT: Duration = Duration::from_secs(300);
/// Pause before forwarding again when the leader was unreachable and
/// no other was elected meanwhile.
const FORWARD_RETRY: Duration = Duration::from_millis(100);

/// Most flights one query call waits for: a waiter whose leader went
/// away looks for a flight once more.
const MAX_JOINS: usize = 2;
/// Why a query call was refused at its client deadline while it waited
/// for an identical one in flight.
const WAITED_FOR_FLIGHT: &str =
    "request waited for an identical query in flight until its client deadline; retry later";

/// Whether an answered query call's result is in the cache now.
fn kept<T>(result: &Result<(T, Option<Lookup>), NodeError>) -> bool {
    matches!(result, Ok((_, Some(lookup))) if lookup.reusable())
}

/// A leader's advertised API URL, by leader and term.
type Described = Option<(NodeId, u64, Option<String>)>;

/// One query call's target and options.
#[derive(Clone, Copy)]
struct Call<'a> {
    db: &'a [u8],
    module: &'a str,
    name: &'a str,
    args: &'a Value,
    options: &'a CallOptions,
}

/// The form a query's answer takes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Form {
    /// The value.
    Value,
    /// Its JSON as the HTTP API writes it, only.
    Encoded,
}

/// A query's answer, in the form asked.
pub(super) enum Reply {
    Value(Value),
    Encoded(Encoded),
}

impl Reply {
    /// The value, decoding a rendering (answers come in the form asked,
    /// so this only converts).
    fn into_value(self) -> Result<Value, NodeError> {
        match self {
            Reply::Value(value) => Ok(value),
            Reply::Encoded(encoded) => {
                parse_value(encoded.bytes()).map_err(|error| NodeError::failure(error.into()))
            }
        }
    }

    /// Its JSON, rendering a value.
    fn into_encoded(self) -> Result<Encoded, NodeError> {
        match self {
            Reply::Encoded(encoded) => Ok(encoded),
            Reply::Value(value) => Encoded::render(&value).map_err(NodeError::failure),
        }
    }
}

/// The leader's advertised API URL as last described, plus whether a
/// description is under way.
#[derive(Default)]
pub(super) struct LeaderUrl {
    known: Arc<Mutex<Described>>,
    describing: Arc<AtomicBool>,
}

/// Clears the description flag when a description ends, however.
struct Describing(Arc<AtomicBool>);

impl Drop for Describing {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

impl Node {
    /// Replace how this member proves fresh reads. Call before serving.
    /// A lease at least as long as the leader lease is cut to the
    /// default (see [`crate::read`]).
    pub fn with_reads(self, settings: ReadSettings) -> Self {
        self.reads.configure(settings);
        self
    }

    /// How this member proves fresh reads.
    pub fn read_settings(&self) -> ReadSettings {
        self.reads.settings()
    }

    /// Fresh-read counters since this member opened.
    pub fn read_metrics(&self) -> ReadMetrics {
        self.reads.metrics()
    }

    /// Give up the read lease for the rest of the current term: fresh
    /// reads run quorum rounds until this member leads in a later term.
    /// A leadership transfer calls this before asking another member to
    /// campaign at once, which ignores that member's own vote refusal
    /// (see [`crate::read`]); a transfer that fails does not restore
    /// the lease.
    pub fn release_read_lease(&self) {
        self.reads.surrender();
    }

    /// How stale this member's applied state may be now; `None` when
    /// unknown (see [`crate::read`]), and while the state is behind what
    /// this member held before ([`Node::rejoining`], [`Node::recovering`]).
    pub fn staleness(&self) -> Option<Duration> {
        if self.behind().is_some() {
            return None;
        }
        self.reads.staleness(Instant::now())
    }

    /// Why this member's own state may not answer as it is: it opened
    /// without Raft state and is catching up ([`REJOINING`]), or opened
    /// with its log past its state and recovers it ([`RECOVERING`]). Its
    /// staleness clock may read recent all the same, from the appends it
    /// hears, while its state lags what it or its group served before.
    fn behind(&self) -> Option<NodeError> {
        if self.rejoining() {
            return Some(NodeError::Failure {
                code: REJOINING.to_string(),
                message: "this member opened without Raft state and has not caught up with \n                          its group: it answers no replica-local read"
                    .to_string(),
            });
        }
        self.recovering().map(|reason| NodeError::Failure {
            code: RECOVERING.to_string(),
            message: reason,
        })
    }

    /// This member's side of the member channel, which its
    /// [`Node::peer_service`] answers member requests through, and the
    /// API serves forwarded requests through.
    pub fn member_desk(&self) -> Arc<MemberDesk> {
        self.desk.clone()
    }

    /// Prove that this member holds every write completed before the
    /// call, and wait until it applied them: at the leader by its lease
    /// or a quorum round, at a follower by the leader's read fence. A
    /// member recovering the log it opened with past its state also waits
    /// for that, as [`Node::ensure_leader`] does.
    pub async fn read_barrier(&self) -> Result<(), NodeError> {
        self.check_storage()?;
        self.reads.fence().await?;
        if let Some(fence) = &self.recovery
            && !fence.wait(RECOVERY_WAIT).await
        {
            return Err(NodeError::Failure {
                code: RECOVERING.to_string(),
                message: fence.reason(),
            });
        }
        Ok(())
    }

    /// Query, reading the way the query declares, plus the revision its
    /// snapshot reflects and how the answer was served.
    ///
    /// Linearizable queries prove freshness first ([`Node::read_barrier`])
    /// on any member. Replica-local queries read this member's applied
    /// state as it is, unless the query or `read` bounds their staleness
    /// (the tighter bound holds) and this member is past it or does not
    /// know: then they prove freshness first, failing with retryable
    /// [`STALE_REPLICA`] when they cannot. A member whose state is behind
    /// what it held before (rejoining or recovering) refuses them at once
    /// with its retryable reason, so callers try another replica. Either way the query then
    /// admits and evaluates on the reader, never taking the engine lock.
    ///
    /// Once its read is proved, a call identical to one in flight at the
    /// same revision waits for that one, then answers as usual, from the
    /// result it left in the cache when that still holds for it (see
    /// [`crate::flight`]).
    pub async fn query_read(
        &self,
        db: &[u8],
        module: &str,
        name: &str,
        args: &Value,
        options: &CallOptions,
        read: &ReadOptions,
    ) -> Result<(Value, u64, Served), NodeError> {
        let call = Call {
            db,
            module,
            name,
            args,
            options,
        };
        let (reply, revision, served) = self.query_read_as(call, read, Form::Value).await?;
        Ok((reply.into_value()?, revision, served))
    }

    /// [`Node::query_read`] answering with the result's JSON as the HTTP
    /// API writes it, for a caller that sends it on as it is (the HTTP
    /// query route): a cached result answers with the rendering it
    /// keeps, which is the rendering of any evaluation of it.
    pub async fn query_read_encoded(
        &self,
        db: &[u8],
        module: &str,
        name: &str,
        args: &Value,
        options: &CallOptions,
        read: &ReadOptions,
    ) -> Result<(Encoded, u64, Served), NodeError> {
        let call = Call {
            db,
            module,
            name,
            args,
            options,
        };
        let (reply, revision, served) = self.query_read_as(call, read, Form::Encoded).await?;
        Ok((reply.into_encoded()?, revision, served))
    }

    /// Capture a query token only when its history stayed stable across evaluation.
    pub async fn query_read_token_encoded(
        &self,
        db: &[u8],
        module: &str,
        name: &str,
        args: &Value,
        options: &CallOptions,
        read: &ReadOptions,
    ) -> Result<(Encoded, Option<flower_protocol::ReadToken>, Served), NodeError> {
        let before = self.read_token(db, 0, 0).await?;
        let (encoded, revision, served) = self
            .query_read_encoded(db, module, name, args, options, read)
            .await?;
        let token = self.read_token(db, revision, 0).await?;
        if before.as_ref().map(|t| t.incarnation) != token.as_ref().map(|t| t.incarnation) {
            return Err(NodeError::coded(
                STALE_REPLICA,
                "database history changed during query; retry on current state",
            ));
        }
        Ok((encoded, token, served))
    }

    /// Create a scope-qualified token. Zero incarnation is deliberately unavailable.
    pub async fn read_token(
        &self,
        db: &[u8],
        revision: u64,
        position: u64,
    ) -> Result<Option<flower_protocol::ReadToken>, NodeError> {
        let group = self.group().to_string();
        let db = db.to_vec();
        self.with_reader(move |reader| {
            let read = reader.store().begin_read().map_err(NodeError::engine)?;
            let history =
                flower_engine::placement::History::read(&read, &db).map_err(NodeError::engine)?;
            Ok(
                (history.incarnation != 0).then(|| flower_protocol::ReadToken {
                    version: flower_protocol::VERSION,
                    database: String::from_utf8_lossy(&db).into_owned(),
                    group,
                    incarnation: flower_protocol::Incarnation(history.incarnation),
                    revision: flower_protocol::Revision(revision),
                    position: flower_protocol::LogPosition(position),
                }),
            )
        })
        .await
    }

    async fn check_read_token(
        &self,
        db: &[u8],
        token: &flower_protocol::ReadToken,
    ) -> Result<(), NodeError> {
        if token.database.as_bytes() != db {
            return Err(NodeError::coded(
                "invalid_read_token",
                "read token belongs to another database",
            ));
        }
        let db = db.to_vec();
        let token = token.clone();
        let group = self.group().to_string();
        self.with_reader(move |reader| {
            let read = reader.store().begin_read().map_err(NodeError::engine)?;
            let history =
                flower_engine::placement::History::read(&read, &db).map_err(NodeError::engine)?;
            if !history.holds(token.incarnation.0) {
                return Err(NodeError::coded(
                    "wrong_history",
                    "causal token belongs to a different history",
                ));
            }
            let revision = read
                .meta_u64(&db, flower_engine::storage::meta::REVISION)
                .map_err(NodeError::engine)?;
            let position = read
                .meta_u64(&db, flower_engine::storage::meta::APPLIED_LOG)
                .map_err(NodeError::engine)?;
            if revision < token.revision.0 || (group == token.group && position < token.position.0)
            {
                return Err(NodeError::coded(
                    STALE_REPLICA,
                    "replica has not applied causal bound",
                ));
            }
            Ok(())
        })
        .await
    }

    /// Identical query calls in flight on this member, and how they
    /// joined (see [`crate::flight`]).
    pub fn flight_stats(&self) -> FlightStats {
        self.flights.stats()
    }

    /// Bound the identical query calls this member keeps track of to
    /// `bytes`; zero turns joining off.
    pub fn set_query_flight_bytes(&self, bytes: usize) {
        self.flights.set_budget(bytes);
    }

    /// [`Node::query_read`] in the form asked.
    async fn query_read_as(
        &self,
        call: Call<'_>,
        read: &ReadOptions,
        form: Form,
    ) -> Result<(Reply, u64, Served), NodeError> {
        let Call {
            db,
            module,
            name,
            args,
            options,
        } = call;
        let flight = Flights::call_key(db, module, name, args, options);
        let plan = if let Some(token) = &read.at_least {
            token
                .validate()
                .map_err(|e| NodeError::coded("invalid_read_token", e))?;
            self.fresh_fence().await?;
            self.check_read_token(db, token).await?;
            ReadPlan::Proved(Served::Fresh)
        } else {
            self.plan_read(db, module, name, options, read).await?
        };
        let served = match plan {
            ReadPlan::Proved(served) => served,
            ReadPlan::Local {
                declared,
                staleness,
            } => {
                let local = self
                    .in_flight(db, flight.as_deref(), || async {
                        Ok(
                            match self
                                .replica_local(db, module, name, args, options, declared, form)
                                .await?
                            {
                                Some((reply, revision, lookup)) => {
                                    (Some((reply, revision)), lookup)
                                }
                                None => (None, None),
                            },
                        )
                    })
                    .await?;
                if let Some((reply, revision)) = local {
                    return Ok((reply, revision, Served::Local(staleness)));
                }
                // A deployment made it linearizable meanwhile: read fresh.
                self.fresh_fence().await?;
                Served::Fresh
            }
        };
        let (reply, revision) = self
            .in_flight(db, flight.as_deref(), || async {
                let (reply, revision, lookup) = self
                    .evaluate_query(db, module, name, args, options, form)
                    .await?;
                Ok(((reply, revision), lookup))
            })
            .await?;
        if let Some(token) = &read.at_least {
            self.check_read_token(db, token).await?;
            if revision < token.revision.0 {
                return Err(NodeError::coded(
                    STALE_REPLICA,
                    "query snapshot preceded causal bound",
                ));
            }
        }
        Ok((reply, revision, served))
    }

    /// Run `evaluate` in the flight of `call` on `db` (see
    /// [`crate::flight`]): lead it and land it, join it and evaluate
    /// once it landed, or evaluate alone. `evaluate` answers with its
    /// cache lookup, which says whether its result is in the cache now.
    async fn in_flight<T, F, E>(
        &self,
        db: &[u8],
        call: Option<&[u8]>,
        evaluate: E,
    ) -> Result<T, NodeError>
    where
        E: Fn() -> F,
        F: Future<Output = Result<(T, Option<Lookup>), NodeError>>,
    {
        let Some(call) = call else {
            return evaluate().await.map(|(answer, _)| answer);
        };
        // A waiter whose leader went away looks for a flight once more.
        for _ in 0..MAX_JOINS {
            // The revision the caller's proof reached at least: its own
            // snapshot reads it or a later one.
            let Ok(revision) = self.reader.revision(db) else {
                break;
            };
            match self.flights.enter(call.to_vec(), revision) {
                Turn::Lead(leader) => {
                    let result = evaluate().await;
                    // Refused admission, it never ran: its waiters look
                    // for a flight again rather than mark the call solo.
                    if !matches!(result, Err(NodeError::Overloaded { .. })) {
                        leader.land(kept(&result));
                    }
                    return result.map(|(answer, _)| answer);
                }
                Turn::Alone(alone) => {
                    let result = evaluate().await;
                    alone.land(kept(&result));
                    return result.map(|(answer, _)| answer);
                }
                Turn::Join(waiter) => {
                    // The wait ends at the client deadline, as admission's
                    // does: the call is refused then.
                    let deadline = crate::deadline::current();
                    let landing = self
                        .observer
                        .query_stage(
                            QueryStage::CoalesceWait,
                            stages::timed(Stage::Join, async {
                                match deadline {
                                    None => Ok(waiter.landing().await),
                                    Some(deadline) => {
                                        tokio::time::timeout_at(deadline, waiter.landing())
                                            .await
                                            .map_err(|_| NodeError::Overloaded {
                                                message: WAITED_FOR_FLIGHT.to_string(),
                                                retry_after_ms: RETRY_AFTER.as_millis() as u64,
                                            })
                                    }
                                }
                            }),
                        )
                        .await?;
                    if landing != Landing::Abandoned {
                        let result = evaluate().await;
                        if kept(&result) {
                            self.flights.kept(call);
                        }
                        return result.map(|(answer, _)| answer);
                    }
                }
            }
        }
        evaluate().await.map(|(answer, _)| answer)
    }

    /// Decide how a query reads, as [`Node::query_read`] does, and prove
    /// freshness first when it must: [`ReadPlan::Proved`] once proved,
    /// or [`ReadPlan::Local`] for a replica-local read of this member's
    /// state as it is. The plan names the query the way the call does,
    /// through the alias allowlist for external calls. Callers that
    /// evaluate on their own reader view (the watch hub authorizing and
    /// evaluating together) start here.
    pub async fn plan_read(
        &self,
        db: &[u8],
        module: &str,
        name: &str,
        options: &CallOptions,
        read: &ReadOptions,
    ) -> Result<ReadPlan, NodeError> {
        if let Some(
            declared @ ReadConsistency::ReplicaLocal {
                max_staleness_ms: declared_bound,
            },
        ) = self.reader.call_consistency(db, module, name, options)
        {
            if let Some(behind) = self.behind() {
                return Err(behind);
            }
            let declared_bound = declared_bound.map(|ms| Duration::from_millis(u64::from(ms)));
            let bound = match (declared_bound, read.max_staleness) {
                (Some(declared), Some(asked)) => Some(declared.min(asked)),
                (declared, asked) => declared.or(asked),
            };
            let staleness = self.staleness();
            let within =
                bound.is_none_or(|bound| staleness.is_some_and(|staleness| staleness <= bound));
            if !within {
                self.fresh_fence()
                    .await
                    .map_err(|error| NodeError::Failure {
                        code: STALE_REPLICA.to_string(),
                        message: format!(
                            "this replica is staler than the read allows ({staleness:?}) and \
                             could not prove a fresh read: {error}"
                        ),
                    })?;
                return Ok(ReadPlan::Proved(Served::Local(Some(Duration::ZERO))));
            }
            return Ok(ReadPlan::Local {
                declared,
                staleness,
            });
        }
        self.fresh_fence().await?;
        Ok(ReadPlan::Proved(Served::Fresh))
    }

    /// Evaluate a replica-local query on the reader under a query
    /// admission permit, in the form asked and with its cache lookup;
    /// `None` when the snapshot's catalog no longer declares it
    /// `declared`.
    #[allow(clippy::too_many_arguments)]
    async fn replica_local(
        &self,
        db: &[u8],
        module: &str,
        name: &str,
        args: &Value,
        options: &CallOptions,
        declared: ReadConsistency,
        form: Form,
    ) -> Result<Option<(Reply, u64, Option<Lookup>)>, NodeError> {
        let bytes = input_bytes(args) + module.len() + name.len();
        let permit = self
            .observer
            .query_stage(
                QueryStage::Admission,
                self.admission.acquire(Class::Query, db, bytes),
            )
            .await?;
        let (db, module, name, args, options) = (
            db.to_vec(),
            module.to_string(),
            name.to_string(),
            args.clone(),
            options.clone(),
        );
        let observer = self.observer.clone();
        let evaluation = stages::timed(Stage::Eval, async {
            // Evaluator spans are this stage's children.
            let span = tracing::Span::current();
            let queued = crate::telemetry::enabled().then(Instant::now);
            let (found, lookup) = self
                .with_reader(move |reader| {
                    let _entered = span.enter();
                    let _permit = permit;
                    let (result, lookup) =
                        measured_query(&observer, queued, &db, &module, &name, || {
                            match form {
                                Form::Value => reader
                                    .query_replica_local(
                                        &db, &module, &name, &args, &options, declared,
                                    )
                                    .map(|found| {
                                        found.map(|(value, revision)| {
                                            (Reply::Value(value), revision)
                                        })
                                    }),
                                Form::Encoded => reader
                                    .query_replica_local_encoded(
                                        &db, &module, &name, &args, &options, declared,
                                    )
                                    .map(|found| {
                                        found.map(|(encoded, revision)| {
                                            (Reply::Encoded(encoded), revision)
                                        })
                                    }),
                            }
                            .map_err(NodeError::application)
                        });
                    result.map(|found| (found, lookup))
                })
                .await?;
            if let Some(lookup) = lookup {
                stages::cache(lookup.hit());
            }
            Ok(found.map(|(reply, revision)| (reply, revision, lookup)))
        });
        self.observer
            .query_stage(QueryStage::Evaluation, evaluation)
            .await
    }

    /// Prove a fresh read for a query: its `fence` phase and
    /// `fresh_fence` stage.
    async fn fresh_fence(&self) -> Result<(), NodeError> {
        self.observer
            .query_stage(
                QueryStage::Fence,
                stages::timed(Stage::Fence, self.read_barrier()),
            )
            .await
    }

    /// Serve a client request this member answered with a forward
    /// through the leader's API, and return the leader's answer.
    ///
    /// The request goes to the leader this member knows, over the
    /// member channel. A leader that turned out not to lead, or could
    /// not be reached, ran nothing: the request goes again, to the next
    /// leader once one is known, while [`FORWARD_BUDGET`] lasts. If this
    /// member leads by then, it serves the request itself. An exchange
    /// that broke after the request may have arrived fails with
    /// retryable [`FORWARD_FAILED`]: its outcome is unknown, so only the
    /// caller, holding its idempotency key, may send it again. When the
    /// budget ends, the leader serves no API, or this member belongs to
    /// no cluster yet, the answer is [`NodeError::Forward`].
    pub async fn forward(&self, request: ForwardedRequest) -> Result<ForwardedResponse, NodeError> {
        self.reads.forwarded();
        // Within the client's own deadline, when it set one.
        let budget =
            crate::deadline::remaining().map_or(FORWARD_BUDGET, |left| left.min(FORWARD_BUDGET));
        let deadline = Instant::now() + budget;
        let peers = self.reads.peers();
        let mut changes = self.raft.metrics();
        let probe_timeout = self.raft.config().rpc_timeout();
        loop {
            let (leader, node, alone) = {
                let metrics = changes.borrow_and_update();
                let leader = metrics.current_leader;
                let membership = metrics.membership_config.membership();
                let node = leader.and_then(|leader| membership.get_node(&leader).cloned());
                (leader, node, membership.voter_ids().next().is_none())
            };
            if alone || Instant::now() >= deadline {
                return Err(NodeError::Forward { leader });
            }
            let silent = leader != Some(self.id) && !self.reads.hears_leader(Instant::now());
            let outcome = match (leader, node) {
                (Some(leader), _) if leader == self.id => {
                    self.desk
                        .handle(MemberRequest::Forward(request.clone()))
                        .await
                }
                (Some(leader), Some(node)) => {
                    // A leader silent for its contact window may be gone,
                    // and dialing a dead host can take seconds: ask who it
                    // is first, a harmless probe given up as soon as
                    // another leader is elected, before sending it the
                    // request, which must not be sent twice. One that
                    // answers is alive and only unheard (a one-way
                    // partition).
                    let reachable = if silent {
                        let probe = peers.info(leader, &node.addr, probe_timeout);
                        let elected =
                            changes.wait_for(|metrics| metrics.current_leader != Some(leader));
                        tokio::select! {
                            probed = probe => probed.is_ok(),
                            // Look at the new leader at once.
                            _ = elected => continue,
                        }
                    } else {
                        true
                    };
                    if reachable {
                        // A dial still under way when the leader changes
                        // (it died before going silent) is given up.
                        let changed = async {
                            let _ = changes
                                .wait_for(|metrics| metrics.current_leader != Some(leader))
                                .await;
                        };
                        peers
                            .member_unless(
                                leader,
                                &node.addr,
                                MemberRequest::Forward(request.clone()),
                                // The leader holds the request to its client's
                                // deadline itself, and answers when it passes.
                                FORWARD_CALL_TIMEOUT,
                                changed,
                            )
                            .await
                    } else {
                        Err(MemberError::Unreachable(format!(
                            "leader {leader} is silent and does not answer"
                        )))
                    }
                }
                _ => Err(MemberError::Unreachable("no leader is known".to_string())),
            };
            match outcome {
                Ok(MemberResponse::Forward(response)) => return Ok(response),
                Ok(_) => {
                    return Err(NodeError::Failure {
                        code: FORWARD_FAILED.to_string(),
                        message: "the leader answered a forward with another kind".to_string(),
                    });
                }
                Err(MemberError::NotLeader { .. } | MemberError::Unreachable(_)) => {}
                Err(MemberError::Broken(message)) => {
                    return Err(NodeError::Failure {
                        code: FORWARD_FAILED.to_string(),
                        message: format!(
                            "forwarding to leader {leader:?} broke ({message}); the outcome is \
                             unknown: retry with the same idempotency key"
                        ),
                    });
                }
                Err(MemberError::Failed { code, message }) => {
                    tracing::debug!(%code, %message, "the leader cannot serve forwarded requests");
                    return Err(NodeError::Forward { leader });
                }
            }
            // A changed leader wakes at once; otherwise pause briefly.
            let pause = FORWARD_RETRY.min(deadline.saturating_duration_since(Instant::now()));
            if let Ok(Err(_)) = tokio::time::timeout(pause, changes.changed()).await {
                return Err(NodeError::Fatal("Raft stopped".to_string()));
            }
        }
    }

    /// Forward unless this member believes it leads: only the leader
    /// prepares writes, against state no follower may yet hold (a
    /// follower lagging a deployment would refuse a call the leader
    /// takes). A member that believes it leads but no longer does still
    /// forwards, when its proposal is refused.
    pub(crate) fn forward_unless_leading(&self) -> Result<(), NodeError> {
        let leader = self.raft.metrics().borrow().current_leader;
        if leader == Some(self.id) {
            return Ok(());
        }
        Err(NodeError::Forward { leader })
    }

    /// Advertise where this member's API serves clients, so members and
    /// their replies can name it while this member leads.
    pub fn set_api_url(&self, url: Option<String>) {
        self.desk.set_api_url(url);
    }

    /// Where the current leader's API serves clients, if known: this
    /// member's own when it leads (and hears from a quorum), else the
    /// leader's as it last described it in this term, while this member
    /// still hears from it (a silent leader may be gone, and clients
    /// should not chase it). An unknown one is asked for in the
    /// background, so a later call knows it.
    pub fn leader_api_url(&self) -> Option<String> {
        let (leader, term, node, isolated) = {
            let metrics = self.raft.metrics();
            let metrics = metrics.borrow();
            let leader = metrics.current_leader?;
            let node = metrics
                .membership_config
                .membership()
                .get_node(&leader)
                .cloned();
            let isolated = super::leadership::cut_off(
                &metrics,
                super::leadership::isolation_window(self.raft.config()),
            );
            (leader, metrics.current_term, node, isolated)
        };
        if leader == self.id {
            return if isolated { None } else { self.desk.api_url() };
        }
        if !self.reads.hears_leader(Instant::now()) {
            return None;
        }
        if let Some((known, known_term, url)) = self
            .leader_url
            .known
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            && *known == leader
            && *known_term == term
        {
            return url.clone();
        }
        if let Some(node) = node {
            self.describe(leader, term, node);
        }
        None
    }

    /// Ask `leader` where its API serves, in the background, once at a
    /// time.
    fn describe(&self, leader: NodeId, term: u64, node: BasicNode) {
        if self.leader_url.describing.swap(true, Ordering::AcqRel) {
            return;
        }
        let describing = Describing(self.leader_url.describing.clone());
        let known = self.leader_url.known.clone();
        let peers = self.reads.peers().clone();
        let ttl = self.reads.settings().timeout;
        tokio::spawn(async move {
            let _describing = describing;
            if let Ok(MemberResponse::Describe { api_url }) = peers
                .member(leader, &node.addr, MemberRequest::Describe, ttl)
                .await
            {
                *known.lock().unwrap_or_else(PoisonError::into_inner) =
                    Some((leader, term, api_url));
            }
        });
    }
}
