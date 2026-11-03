//! Two-phase commit across groups.
//!
//! A transaction whose steps live in one group commits as one entry, as
//! it always did. One whose steps span groups is coordinated by the
//! group owning its first step, from that group's leader
//! ([`Host::distributed_transaction`]):
//!
//! 1. the coordinator records the transaction (`begin`) before any
//!    participant hears of it;
//! 2. it sends every participant group's leader its steps at once; each
//!    prepares them through its writer on its staged state, as a
//!    single-group transaction would, and replicates the prepared patch,
//!    its results and a write lock on each of its databases (`prepare`),
//!    refusing a database that does not serve at the epoch the
//!    coordinator resolved, or that another transaction holds past a
//!    bounded wait;
//! 3. with every participant prepared, the coordinator decides commit
//!    with every step's result, else abort; the first decision applied
//!    wins, so a deposed coordinator racing its successor cannot flip it;
//! 4. it sends the decision to every participant at once; each commits
//!    its stored patch (or drops it), releases its locks and leaves a
//!    tombstone (`finish`);
//! 5. once every participant finished, the record is complete, and the
//!    caller gets the results.
//!
//! Every group's leader runs a resolver ([`Host::run_resolver`]) for
//! what a crash or a leadership change left in flight: an undecided
//! coordinator record asks its participants and commits when all of
//! them prepared (with the results their records hold), else aborts; a
//! decided one finishes its participants; a participant prepared for a
//! while asks its coordinator and applies the decision, aborting only
//! what no coordinator can commit (it has no record, or no longer
//! exists). Participants never commit on their own.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, PoisonError, Weak};
use std::time::Duration;

use flower_engine::engine::{CallOptions, MultiStep, MutationOptions};
use flower_engine::txn::{
    CoordinatorRecord, Decision, Participant, ParticipantCommit, PlacedDb, PreparedRecord,
    TXN_ABORTED, Tombstone, TxnCommand,
};
use flower_sdk::{Failure, Principal, Value, fail};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use crate::exec::now_ms;
use crate::group::GroupId;
use crate::host::Host;
use crate::node::NodeError;
use crate::peer::{ControlAnswer, ControlRequest};
use crate::types::WriteResponse;

/// How often the resolver looks at records in flight.
pub const RESOLVE_EVERY: Duration = Duration::from_secs(1);
/// How long a participant stays prepared before it asks its
/// coordinator.
pub const ASK_AFTER: Duration = Duration::from_secs(2);
/// How long the coordinator keeps delivering a decision before it
/// answers the caller and leaves the rest to the resolver.
pub const FINISH_BUDGET: Duration = Duration::from_secs(10);
/// How long completed coordinator records are kept: the receipt window,
/// within which a retry with the same idempotency key replays.
pub const KEEP_RECORDS: Duration = Duration::from_secs(24 * 60 * 60);
/// How long participants keep tombstones of finished transactions.
pub const KEEP_TOMBSTONES: Duration = Duration::from_secs(60 * 60);
/// Most history rows one collection deletes.
const COLLECT_PAGE: usize = 1024;

/// One step on the wire to a participant.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StepWire {
    /// Its index in the transaction.
    pub index: usize,
    /// Its database.
    pub db: String,
    /// Its module.
    pub module: String,
    /// Its mutation (or HTTP alias).
    pub method: String,
    /// Its arguments, canonical JSON text.
    pub args: String,
}

impl StepWire {
    /// The engine's step.
    pub fn step(&self) -> Result<MultiStep, NodeError> {
        Ok(MultiStep {
            db: self.db.as_bytes().to_vec(),
            module: self.module.clone(),
            method: self.method.clone(),
            args: flower_engine::json::parse_value(self.args.as_bytes())
                .map_err(NodeError::engine)?,
        })
    }
}

/// The call options a participant runs its steps with.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CallWire {
    /// The asserted principal: subject, tenant, claims as JSON text.
    pub principal: Option<(String, Option<String>, Option<String>)>,
    /// Credentials for the authorization hook, JSON text.
    pub credentials: Option<String>,
    /// What was left of the caller's deadline when the request was
    /// built, in milliseconds.
    pub timeout_ms: Option<u64>,
    /// The caller's deadline itself, in wall-clock milliseconds since the
    /// epoch: a request delivered late (a retry, a slow dial) gets only
    /// what is left of it. The earlier of the two holds, so a skewed
    /// clock can only shorten it.
    #[serde(default)]
    pub deadline_ms: Option<u64>,
    /// The call timestamp.
    pub timestamp_ms: Option<u64>,
    /// Whether method names are HTTP aliases.
    pub external: bool,
}

fn render(value: &Value) -> Result<String, NodeError> {
    let bytes = flower_engine::json::render(value).map_err(NodeError::engine)?;
    String::from_utf8(bytes).map_err(|error| NodeError::coded("render", error))
}

fn parse(text: &str) -> Result<Value, NodeError> {
    flower_engine::json::parse_value(text.as_bytes()).map_err(NodeError::engine)
}

impl CallWire {
    /// The wire form of call options.
    pub fn of(options: &CallOptions) -> Result<Self, NodeError> {
        let timeout_ms = options.deadline.map(|deadline| {
            deadline
                .saturating_duration_since(std::time::Instant::now())
                .as_millis() as u64
        });
        Ok(CallWire {
            principal: options
                .principal
                .as_ref()
                .map(|principal| {
                    Ok::<_, NodeError>((
                        principal.subject.clone(),
                        principal.tenant.clone(),
                        principal.claims.as_ref().map(render).transpose()?,
                    ))
                })
                .transpose()?,
            credentials: options.credentials.as_ref().map(render).transpose()?,
            timeout_ms,
            deadline_ms: timeout_ms.map(|left| crate::exec::now_ms().saturating_add(left)),
            timestamp_ms: options.timestamp_ms,
            external: options.external,
        })
    }

    /// The engine's call options.
    pub fn call_options(&self) -> Result<CallOptions, NodeError> {
        Ok(CallOptions {
            principal: self
                .principal
                .as_ref()
                .map(|(subject, tenant, claims)| {
                    Ok::<_, NodeError>(Principal {
                        subject: subject.clone(),
                        tenant: tenant.clone(),
                        claims: claims.as_deref().map(parse).transpose()?,
                    })
                })
                .transpose()?,
            deadline: self.deadline(),
            timestamp_ms: self.timestamp_ms,
            credentials: self.credentials.as_deref().map(parse).transpose()?,
            external: self.external,
        })
    }
}

impl CallWire {
    /// The deadline here: the earlier of what was left when the request
    /// was built and what is left of the caller's own.
    fn deadline(&self) -> Option<std::time::Instant> {
        let now = std::time::Instant::now();
        let left = self
            .timeout_ms
            .and_then(|ms| now.checked_add(std::time::Duration::from_millis(ms)));
        let caller = self.deadline_ms.and_then(|at| {
            now.checked_add(std::time::Duration::from_millis(
                at.saturating_sub(crate::exec::now_ms()),
            ))
        });
        match (left, caller) {
            (Some(left), Some(caller)) => Some(left.min(caller)),
            (left, caller) => left.or(caller),
        }
    }
}

/// Commit with every step's result, unless the decision carrying them
/// would outgrow the transaction bound, which every replicated entry
/// keeps: then abort as too large. Aborting is always open before the
/// decision.
fn commit_or_too_large(id: &str, results: Vec<String>) -> TxnCommand {
    let bytes = results.iter().map(String::len).sum::<usize>();
    match flower_engine::patch::check_transaction_size(bytes) {
        Ok(()) => TxnCommand::Decide {
            id: id.to_string(),
            decision: Decision::Commit,
            results,
            reason: None,
            code: None,
            details: None,
        },
        Err(failure) => TxnCommand::Decide {
            id: id.to_string(),
            decision: Decision::Abort,
            results: Vec::new(),
            reason: Some(format!("the decision: {}", failure.message)),
            code: Some(failure.code),
            details: None,
        },
    }
}

/// A participant's steps to prepare.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PrepareRequest {
    /// The transaction.
    pub id: String,
    /// Its coordinator group.
    pub coordinator: String,
    /// Its coordinator's uid.
    pub coordinator_uid: u64,
    /// This participant's databases with their epochs.
    pub dbs: Vec<PlacedDb>,
    /// Its steps.
    pub steps: Vec<StepWire>,
    /// The caller's call options.
    pub options: CallWire,
}

/// What one group asks another about a transaction.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum TxnRequest {
    /// Prepare these steps (to a participant).
    Prepare(Box<PrepareRequest>),
    /// Apply the decision (to a participant).
    Finish {
        /// The transaction.
        id: String,
        /// The decision.
        decision: Decision,
    },
    /// Where the participant stands (to a participant).
    Participant {
        /// The transaction.
        id: String,
    },
    /// The coordinator's record (to the coordinator).
    Coordinator {
        /// The transaction.
        id: String,
    },
    /// Abort it if it has not decided (to the coordinator, from an
    /// operator): the coordinator may always abort an undecided
    /// transaction, and the first decision applied wins.
    Abort {
        /// The transaction.
        id: String,
    },
    /// The group's coordinator and prepared records, as its leader
    /// applied them, for operators.
    Records,
}

/// Where a participant stands on a transaction.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ParticipantState {
    /// Prepared, with its steps' results.
    Prepared {
        /// `(step, JSON text)`.
        results: Vec<(usize, String)>,
    },
    /// Finished with this decision.
    Finished {
        /// The decision.
        decision: Decision,
    },
    /// No record: never prepared, or finished long ago.
    Unknown,
}

/// Answer a transaction request as this group's leader.
pub(crate) async fn answer(
    node: &crate::node::Node,
    request: TxnRequest,
) -> Result<ControlAnswer, NodeError> {
    let applied = |value: Result<Value, Failure>| -> Result<ControlAnswer, NodeError> {
        Ok(match value {
            Ok(value) => ControlAnswer::Applied(render(&value)?),
            Err(failure) => ControlAnswer::Refused {
                details: failure.details.as_ref().map(render).transpose()?,
                code: failure.code,
                message: failure.message,
            },
        })
    };
    let json = |value: &dyn erased::Encode| -> Result<ControlAnswer, NodeError> {
        Ok(ControlAnswer::Applied(value.encode()?))
    };
    match request {
        TxnRequest::Prepare(request) => applied(node.prepare_txn(*request).await?),
        TxnRequest::Finish { id, decision } => applied(node.finish_txn(&id, decision).await?),
        TxnRequest::Participant { id } => {
            node.forward_unless_leading()?;
            json(&node.participant_state(&id).await?)
        }
        TxnRequest::Coordinator { id } => {
            node.forward_unless_leading()?;
            json(&node.coordinator_record(&id).await?)
        }
        TxnRequest::Records => {
            node.forward_unless_leading()?;
            let (mut coordinated, prepared) = node.txn_records()?;
            // In flight first, then the most recent.
            coordinated
                .sort_by_key(|record| (record.complete, std::cmp::Reverse(record.created_ms)));
            let prepared: Vec<serde_json::Value> = prepared.iter().map(prepared_summary).collect();
            json(&serde_json::json!({"coordinated": coordinated, "prepared": prepared}))
        }
        TxnRequest::Abort { id } => applied(
            node.txn_command(TxnCommand::Decide {
                id,
                decision: Decision::Abort,
                results: Vec::new(),
                reason: Some("aborted by an operator".to_string()),
                code: None,
                details: None,
            })
            .await?,
        ),
    }
}

/// What a participant's prepared record holds, without its patch.
pub fn prepared_summary(record: &PreparedRecord) -> serde_json::Value {
    serde_json::json!({
        "id": record.id,
        "coordinator": record.coordinator,
        "dbs": record.dbs,
        "steps": record.results.iter().map(|(step, _)| step).collect::<Vec<_>>(),
        "patch_bytes": record.patch.len() / 2,
        "prepared_ms": record.prepared_ms,
    })
}

/// What a coordinator's record holds, without its results.
pub fn coordinator_summary(record: &CoordinatorRecord) -> serde_json::Value {
    let mut value = serde_json::to_value(record).unwrap_or(serde_json::Value::Null);
    if let Some(fields) = value.as_object_mut() {
        fields.remove("results");
    }
    value
}

/// JSON answers of any serializable kind.
mod erased {
    use super::NodeError;

    pub trait Encode {
        fn encode(&self) -> Result<String, NodeError>;
    }

    impl<T: serde::Serialize> Encode for T {
        fn encode(&self) -> Result<String, NodeError> {
            serde_json::to_string(self).map_err(|error| NodeError::coded("render", error))
        }
    }
}

/// A transaction a live call on this host drives, until dropped (see
/// [`Host::driving`]).
struct Driving<'a> {
    host: &'a Host,
    id: String,
}

impl Drop for Driving<'_> {
    fn drop(&mut self) {
        self.host
            .driving
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&self.id);
    }
}

/// A transaction's steps grouped by owner, as the coordinator plans it.
struct Plan {
    participants: Vec<Participant>,
}

impl Host {
    /// Send a transaction request to `group`'s leader.
    async fn txn_request(
        &self,
        group: &GroupId,
        request: TxnRequest,
    ) -> Result<ControlAnswer, NodeError> {
        match self
            .send(
                group,
                crate::peer::MemberRequest::Control(ControlRequest::Txn(request)),
                false,
            )
            .await?
        {
            crate::peer::MemberResponse::Control(answer) => Ok(answer),
            _ => Err(NodeError::coded(
                "control",
                "a group's leader answered a transaction request with another kind",
            )),
        }
    }

    /// Where each step's database is served, grouped: `None` when they
    /// all live in one group.
    fn plan(&self, steps: &[MultiStep]) -> Result<Option<Plan>, NodeError> {
        let catalog = self.cached_catalog();
        let mut by_group: BTreeMap<GroupId, Vec<PlacedDb>> = BTreeMap::new();
        let mut order: Vec<GroupId> = Vec::new();
        for (index, step) in steps.iter().enumerate() {
            let db = String::from_utf8_lossy(&step.db).into_owned();
            let route = self.route_in(&catalog, &db).ok_or_else(|| {
                NodeError::coded(
                    flower_engine::placement::PARTITION_MOVED,
                    format!("partition `{db}` names no listed group"),
                )
            })?;
            if !order.contains(&route.group) {
                order.push(route.group.clone());
            }
            let dbs = by_group.entry(route.group).or_default();
            match dbs.iter_mut().find(|placed| placed.db == db) {
                Some(placed) => placed.steps.push(index),
                None => dbs.push(PlacedDb {
                    db,
                    epoch: route.epoch.unwrap_or(0),
                    steps: vec![index],
                }),
            }
        }
        if order.len() <= 1 {
            return Ok(None);
        }
        let participants = order
            .into_iter()
            .map(|group| Participant {
                dbs: by_group.remove(&group).unwrap_or_default(),
                group: group.name.to_string(),
                uid: group.uid,
            })
            .collect();
        Ok(Some(Plan { participants }))
    }

    /// Run a multi-database transaction from this host. Steps that all
    /// live in one group commit as one entry there; steps across groups
    /// commit by two-phase commit coordinated by the first step's group,
    /// which must be led here (else [`NodeError::Forward`]).
    pub async fn distributed_transaction(
        self: &Arc<Self>,
        steps: &[MultiStep],
        options: &MutationOptions,
    ) -> Result<WriteResponse, NodeError> {
        // Linear, before any writer or hook sees the list.
        flower_engine::engine::check_transaction_steps(steps).map_err(NodeError::failure)?;
        let Some(plan) = self.plan(steps)? else {
            let group = match steps.first() {
                Some(step) => self
                    .route(&String::from_utf8_lossy(&step.db))
                    .map(|route| route.group)
                    .unwrap_or_else(GroupId::root),
                None => GroupId::root(),
            };
            let node = crate::partitions::hosted(self, &group)
                .ok_or(NodeError::Forward { leader: None })?;
            return node.transaction(steps, options).await;
        };
        let coordinator_group = GroupId::new(&plan.participants[0].group, plan.participants[0].uid)
            .map_err(|message| NodeError::coded("group", message))?;
        let coordinator = crate::partitions::hosted(self, &coordinator_group)
            .ok_or(NodeError::Forward { leader: None })?;
        coordinator.check_storage()?;
        coordinator.forward_unless_leading()?;
        if options.retry.is_some() || options.expected_revision.is_some() {
            return Err(NodeError::coded(
                flower_sdk::error::codes::INVALID_VALUE,
                "transactions take an idempotency key, not a retry identity or expected revision",
            ));
        }
        let key = options
            .idempotency_key
            .as_ref()
            .map(|key| flower_engine::hex::encode(key));
        // A retry with the same key finds what the first attempt began,
        // and answers its outcome: nothing runs again.
        if let Some(caller_key) = &key
            && let Some(record) = coordinator.coordinator_by_key(caller_key).await?
        {
            let mut response = self.finish_coordination(&coordinator_group, record).await?;
            response.duplicate = true;
            return Ok(response);
        }
        let id = flower_engine::hex::encode(&rand::random::<[u8; 16]>());
        let record = CoordinatorRecord {
            id: id.clone(),
            key,
            participants: plan.participants.clone(),
            steps: steps.len(),
            decision: None,
            results: Vec::new(),
            reason: None,
            code: None,
            finished: Vec::new(),
            commits: Vec::new(),
            complete: false,
            created_ms: 0,
            decided_ms: None,
            completed_ms: None,
            details: None,
        };
        let _driving = self.driving(&id);
        self.coordinate(&coordinator_group, record, steps, options)
            .await
    }

    /// Drive one transaction from `begin` to its answer.
    async fn coordinate(
        self: &Arc<Self>,
        group: &GroupId,
        record: CoordinatorRecord,
        steps: &[MultiStep],
        options: &MutationOptions,
    ) -> Result<WriteResponse, NodeError> {
        let coordinator =
            crate::partitions::hosted(self, group).ok_or(NodeError::Forward { leader: None })?;
        let begun = coordinator
            .txn_command(TxnCommand::Begin {
                record: Box::new(record.clone()),
            })
            .await?
            .map_err(NodeError::failure)?;
        let _ = begun;
        let call = CallWire::of(&options.call)?;
        let mut prepares = tokio::task::JoinSet::new();
        for participant in &record.participants {
            let group = GroupId::new(&participant.group, participant.uid)
                .map_err(|message| NodeError::coded("group", message))?;
            let mut wire = Vec::new();
            for placed in &participant.dbs {
                for index in &placed.steps {
                    let step = &steps[*index];
                    wire.push(StepWire {
                        index: *index,
                        db: placed.db.clone(),
                        module: step.module.clone(),
                        method: step.method.clone(),
                        args: render(&step.args)?,
                    });
                }
            }
            let request = PrepareRequest {
                id: record.id.clone(),
                coordinator: record.participants[0].group.clone(),
                coordinator_uid: record.participants[0].uid,
                dbs: participant.dbs.clone(),
                steps: wire,
                options: call.clone(),
            };
            let host = self.clone();
            // A spawned task leaves the request's scope: carry its
            // deadline, so delivery gives up with the caller.
            prepares.spawn(crate::deadline::scoped(
                crate::deadline::current(),
                async move {
                    let answer = host
                        .txn_request(&group, TxnRequest::Prepare(Box::new(request)))
                        .await;
                    (group, answer)
                },
            ));
        }
        let mut failure: Option<(String, String, Option<String>)> = None;
        let mut results: BTreeMap<usize, String> = BTreeMap::new();
        while let Some(joined) = prepares.join_next().await {
            let (group, answer) =
                joined.map_err(|error| NodeError::Fatal(format!("a prepare panicked: {error}")))?;
            match answer {
                Ok(ControlAnswer::Applied(text)) => {
                    let prepared: flower_engine::txn::PreparedRecord = serde_json::from_str(&text)
                        .map_err(|error| {
                            NodeError::coded("transaction", format!("prepare answer: {error}"))
                        })?;
                    results.extend(prepared.results);
                }
                Ok(ControlAnswer::Refused {
                    code,
                    message,
                    details,
                }) => {
                    failure.get_or_insert((
                        code.clone(),
                        format!("group {group}: {code}: {message}"),
                        details,
                    ));
                }
                Err(error) => {
                    let code = match &error {
                        NodeError::Failure { code, .. } | NodeError::Call { code, .. } => {
                            code.clone()
                        }
                        _ => TXN_ABORTED.to_string(),
                    };
                    failure.get_or_insert((
                        code,
                        format!("group {group}: {error}"),
                        error
                            .call_failure()
                            .and_then(|f| f.details)
                            .and_then(|d| render(&d).ok()),
                    ));
                }
            }
        }
        let mut decide = match failure {
            None => commit_or_too_large(
                &record.id,
                (0..record.steps)
                    .map(|index| {
                        results
                            .get(&index)
                            .cloned()
                            .unwrap_or_else(|| "null".into())
                    })
                    .collect(),
            ),
            Some((code, reason, details)) => TxnCommand::Decide {
                id: record.id.clone(),
                decision: Decision::Abort,
                results: Vec::new(),
                reason: Some(reason),
                // Too large anywhere is too large for the transaction:
                // a 413, not the 422 of `transaction_aborted`.
                code: (code == flower_engine::patch::TRANSACTION_TOO_LARGE).then_some(code),
                details,
            },
        };
        let encoded =
            serde_json::to_vec(&decide).map_err(|error| NodeError::coded("transaction", error))?;
        if let Err(failure) = flower_engine::patch::check_transaction_size(encoded.len()) {
            // Failure details are durable decision data too. Keep aborts possible when
            // an oversized callee failure cannot fit in the coordinator's record.
            decide = TxnCommand::Decide {
                id: record.id.clone(),
                decision: Decision::Abort,
                results: Vec::new(),
                reason: Some(failure.message),
                code: Some(failure.code),
                details: None,
            };
        }
        let decided = coordinator
            .txn_command(decide)
            .await?
            .map_err(NodeError::failure)?;
        let decided: CoordinatorRecord = decode_value(&decided)?;
        self.finish_coordination(group, decided).await
    }

    /// Deliver a decided record's decision and answer the caller, or,
    /// for an undecided one (a retry of an attempt still in flight, or
    /// one a crash left), resolve it first.
    async fn finish_coordination(
        self: &Arc<Self>,
        group: &GroupId,
        mut record: CoordinatorRecord,
    ) -> Result<WriteResponse, NodeError> {
        if record.decision.is_none() {
            record = self.resolve_undecided(group, &record).await?;
        }
        let deadline = tokio::time::Instant::now() + FINISH_BUDGET;
        while !record.complete && tokio::time::Instant::now() < deadline {
            let (finished, commits) = self.deliver(group, &record).await;
            if !finished.is_empty() {
                let coordinator = crate::partitions::hosted(self, group)
                    .ok_or(NodeError::Forward { leader: None })?;
                let updated = coordinator
                    .txn_command(TxnCommand::Finished {
                        id: record.id.clone(),
                        groups: finished,
                        commits,
                    })
                    .await?
                    .map_err(NodeError::failure)?;
                record = decode_value(&updated)?;
            }
            if !record.complete {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
        answer_of(&record)
    }

    /// Send the decision to every participant not finished yet: the
    /// groups that applied it, and where those that committed did.
    async fn deliver(
        self: &Arc<Self>,
        _coordinator: &GroupId,
        record: &CoordinatorRecord,
    ) -> (Vec<String>, Vec<ParticipantCommit>) {
        let Some(decision) = record.decision else {
            return (Vec::new(), Vec::new());
        };
        let mut finishes = tokio::task::JoinSet::new();
        for participant in &record.participants {
            if record.finished.contains(&participant.group) {
                continue;
            }
            let Ok(group) = GroupId::new(&participant.group, participant.uid) else {
                continue;
            };
            let (host, id) = (self.clone(), record.id.clone());
            let name = participant.group.clone();
            finishes.spawn(async move {
                let answer = host
                    .txn_request(&group, TxnRequest::Finish { id, decision })
                    .await;
                (name, answer)
            });
        }
        let mut finished = Vec::new();
        let mut commits = Vec::new();
        while let Some(Ok((name, answer))) = finishes.join_next().await {
            match answer {
                Ok(ControlAnswer::Applied(text)) => {
                    // The participant's tombstone: where it committed.
                    if let Ok(done) = serde_json::from_str::<Tombstone>(&text)
                        && done.decision == Decision::Commit
                    {
                        commits.push(ParticipantCommit {
                            group: name.clone(),
                            position: done.position,
                            revisions: done.revisions,
                        });
                    }
                    finished.push(name);
                }
                // A group gone from the catalog holds nothing of it.
                Err(NodeError::Failure { code, .. })
                    if code == crate::partitions::GROUP_UNREACHABLE
                        && !self.group_listed(&name) =>
                {
                    finished.push(name);
                }
                outcome => {
                    tracing::debug!(group = %name, ?outcome, "a finish will be delivered again");
                }
            }
        }
        (finished, commits)
    }

    /// Whether the catalog still lists a group (the root always).
    fn group_listed(&self, name: &str) -> bool {
        let catalog = self.cached_catalog();
        self.group_id(&catalog, name).is_some()
    }

    /// Decide an undecided record from what its participants say: commit
    /// when every one prepared, else abort. The decision applied first
    /// wins, so this never flips one a live coordinator took.
    async fn resolve_undecided(
        self: &Arc<Self>,
        group: &GroupId,
        record: &CoordinatorRecord,
    ) -> Result<CoordinatorRecord, NodeError> {
        let mut results: BTreeMap<usize, String> = BTreeMap::new();
        let mut all_prepared = true;
        for participant in &record.participants {
            let Ok(participant_group) = GroupId::new(&participant.group, participant.uid) else {
                all_prepared = false;
                break;
            };
            let answer = self
                .txn_request(
                    &participant_group,
                    TxnRequest::Participant {
                        id: record.id.clone(),
                    },
                )
                .await;
            let unreachable = matches!(
                &answer,
                Err(NodeError::Failure { code, .. })
                    if code == crate::partitions::GROUP_UNREACHABLE
            );
            let state = match answer {
                Ok(ControlAnswer::Applied(text)) => {
                    serde_json::from_str::<ParticipantState>(&text).ok()
                }
                _ => None,
            };
            match state {
                Some(ParticipantState::Prepared { results: prepared }) => {
                    results.extend(prepared);
                }
                Some(ParticipantState::Unknown | ParticipantState::Finished { .. }) => {
                    all_prepared = false;
                    break;
                }
                // No leader of its group answered for as long as a request
                // may look for one (`SEND_BUDGET`): a participant that
                // cannot say counts as not prepared, so the reachable ones
                // stop holding their locks for it, as v1's replacement
                // coordinator aborts. Participants never commit on their
                // own, so the abort is always safe.
                None if unreachable => {
                    all_prepared = false;
                    break;
                }
                // Cannot ask for another reason: decide nothing yet.
                None => {
                    return Err(NodeError::coded(
                        crate::partitions::GROUP_UNREACHABLE,
                        format!("participant {participant_group} did not say whether it prepared"),
                    ));
                }
            }
        }
        let decide = if all_prepared {
            commit_or_too_large(
                &record.id,
                (0..record.steps)
                    .map(|index| {
                        results
                            .get(&index)
                            .cloned()
                            .unwrap_or_else(|| "null".into())
                    })
                    .collect(),
            )
        } else {
            TxnCommand::Decide {
                id: record.id.clone(),
                decision: Decision::Abort,
                results: Vec::new(),
                reason: Some("the coordinator recovered an unfinished preparation".to_string()),
                code: None,
                details: None,
            }
        };
        let coordinator =
            crate::partitions::hosted(self, group).ok_or(NodeError::Forward { leader: None })?;
        let decided = coordinator
            .txn_command(decide)
            .await?
            .map_err(NodeError::failure)?;
        decode_value(&decided)
    }

    /// The transactions in doubt in every group this process hosts, as
    /// its replicas applied them: `{group: {coordinated, prepared}}`, the
    /// coordinator records not yet complete and the participant records
    /// still prepared, oldest first. Reads the applied state only: no
    /// barrier, no forward. Blocks on storage reads.
    pub fn in_doubt(&self) -> serde_json::Value {
        let mut out = serde_json::Map::new();
        for node in self.groups() {
            let value = match node.txn_records() {
                Ok((mut coordinated, mut prepared)) => {
                    coordinated.retain(|record| !record.complete);
                    coordinated.sort_by_key(|record| (record.created_ms, record.id.clone()));
                    prepared.sort_by_key(|record| (record.prepared_ms, record.id.clone()));
                    serde_json::json!({
                        "coordinated": coordinated.iter().map(coordinator_summary).collect::<Vec<_>>(),
                        "prepared": prepared.iter().map(prepared_summary).collect::<Vec<_>>(),
                    })
                }
                Err(error) => serde_json::json!({"error": error.to_string()}),
            };
            out.insert(node.group().name.to_string(), value);
        }
        serde_json::Value::Object(out)
    }

    /// Every listed group's transaction records, as each group's leader
    /// applied them: `{group: {coordinated, prepared}}`, in flight first.
    pub async fn transactions(&self) -> serde_json::Value {
        let catalog = self.cached_catalog();
        let mut groups = vec![GroupId::root()];
        groups.extend(
            catalog
                .groups
                .values()
                .filter_map(|entry| self.group_id(&catalog, &entry.name)),
        );
        let mut out = serde_json::Map::new();
        for group in groups {
            let request = TxnRequest::Records;
            let value = match self.txn_request(&group, request).await {
                Ok(ControlAnswer::Applied(text)) => {
                    serde_json::from_str(&text).unwrap_or(serde_json::Value::Null)
                }
                Ok(ControlAnswer::Refused { code, message, .. }) => {
                    serde_json::json!({"error": format!("{code}: {message}")})
                }
                Err(error) => serde_json::json!({"error": error.to_string()}),
            };
            out.insert(group.name.to_string(), value);
        }
        serde_json::Value::Object(out)
    }

    /// Abort an undecided transaction at its coordinator group: its
    /// answer, or the refusal as the inner `Err` (an unknown
    /// transaction). A decided one keeps its decision.
    pub async fn abort_transaction(
        &self,
        coordinator: &GroupId,
        id: &str,
    ) -> Result<Result<Value, Failure>, NodeError> {
        match self
            .txn_request(coordinator, TxnRequest::Abort { id: id.to_string() })
            .await?
        {
            ControlAnswer::Applied(text) => Ok(Ok(parse(&text)?)),
            ControlAnswer::Refused { code, message, .. } => Ok(Err(Failure::new(code, message))),
        }
    }

    /// Mark a transaction as driven by a live call on this host until the
    /// guard drops: the resolver leaves driven ones alone, and a call that
    /// goes away (a client that hung up, its request dropped) leaves the
    /// transaction to it.
    fn driving(&self, id: &str) -> Driving<'_> {
        self.driving
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(id.to_string());
        Driving {
            host: self,
            id: id.to_string(),
        }
    }

    fn is_driving(&self, id: &str) -> bool {
        self.driving
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains(id)
    }

    /// Resolve what crashes and leadership changes left in flight, on
    /// every group this host leads, every [`RESOLVE_EVERY`], until
    /// `stop` turns true.
    pub async fn run_resolver(host: Weak<Host>, mut stop: watch::Receiver<bool>) {
        let mut last_collect = std::time::Instant::now();
        loop {
            if *stop.borrow() {
                return;
            }
            let Some(strong) = host.upgrade() else { return };
            let collect = last_collect.elapsed() >= Duration::from_secs(60);
            if collect {
                last_collect = std::time::Instant::now();
            }
            strong.resolve_once(collect).await;
            drop(strong);
            tokio::select! {
                _ = tokio::time::sleep(RESOLVE_EVERY) => {}
                changed = stop.changed() => {
                    if changed.is_err() {
                        return;
                    }
                }
            }
        }
    }

    /// One resolver pass over every group this host leads.
    pub async fn resolve_once(self: &Arc<Self>, collect: bool) {
        for node in self.groups() {
            if node.status().leader != Some(self.id()) {
                continue;
            }
            let group = node.group().clone();
            let Ok((coordinated, prepared)) = node.txn_records() else {
                continue;
            };
            for record in coordinated {
                if record.complete || self.is_driving(&record.id) {
                    continue;
                }
                let driving = self.driving(&record.id);
                let outcome = self.finish_coordination(&group, record.clone()).await;
                drop(driving);
                if let Err(error) = outcome {
                    tracing::debug!(id = %record.id, %error, "resolving a transaction failed for now");
                }
            }
            let now = now_ms();
            for record in prepared {
                if now.saturating_sub(record.prepared_ms) < ASK_AFTER.as_millis() as u64 {
                    continue;
                }
                if let Some(decision) = self.ask_coordinator(&record).await
                    && let Err(error) = node.finish_txn(&record.id, decision).await
                {
                    tracing::debug!(id = %record.id, %error, "finishing a transaction failed for now");
                }
            }
            if collect {
                let now = now_ms();
                let _ = node
                    .txn_command(TxnCommand::Collect {
                        records_before_ms: now.saturating_sub(KEEP_RECORDS.as_millis() as u64),
                        tombstones_before_ms: now
                            .saturating_sub(KEEP_TOMBSTONES.as_millis() as u64),
                        limit: COLLECT_PAGE,
                    })
                    .await;
            }
        }
    }

    /// The decision a prepared participant should apply, by asking its
    /// coordinator: `None` while undecided or unreachable.
    async fn ask_coordinator(
        self: &Arc<Self>,
        record: &flower_engine::txn::PreparedRecord,
    ) -> Option<Decision> {
        let catalog = self.cached_catalog();
        let listed = self
            .group_id(&catalog, &record.coordinator)
            .is_some_and(|group| group.uid == record.coordinator_uid);
        if !listed {
            // The coordinator group is gone: nothing can commit this.
            return Some(Decision::Abort);
        }
        let group = GroupId::new(&record.coordinator, record.coordinator_uid).ok()?;
        match self
            .txn_request(
                &group,
                TxnRequest::Coordinator {
                    id: record.id.clone(),
                },
            )
            .await
        {
            Ok(ControlAnswer::Applied(text)) => {
                match serde_json::from_str::<Option<CoordinatorRecord>>(&text).ok()? {
                    // No record: never began, or completed and collected,
                    // which needed this participant's finish. Either way
                    // nothing commits it.
                    None => Some(Decision::Abort),
                    Some(coordinated) => coordinated.decision,
                }
            }
            _ => None,
        }
    }
}

/// A record's answer value decoded.
fn decode_value(value: &Value) -> Result<CoordinatorRecord, NodeError> {
    let text = render(value)?;
    serde_json::from_str(&text)
        .map_err(|error| NodeError::coded("transaction", format!("record: {error}")))
}

/// The caller's answer for a decided record, the same however often it
/// is asked: the step values on commit, with each database's revision
/// after the commit in step order and each group's log position (the
/// first step's lead the response, as for a single group's transaction);
/// the abort as a failure. A participant still finishing when the
/// answer is given shows revision and position 0.
fn answer_of(record: &CoordinatorRecord) -> Result<WriteResponse, NodeError> {
    match record.decision {
        Some(Decision::Commit) => {
            let values = record
                .results
                .iter()
                .map(|text| parse(text))
                .collect::<Result<Vec<_>, _>>()?;
            let bytes =
                flower_engine::json::render(&Value::Array(values)).map_err(NodeError::engine)?;
            // Each step's database and group, in step order.
            let mut steps: Vec<(usize, &str, &str)> = record
                .participants
                .iter()
                .flat_map(|participant| {
                    participant.dbs.iter().flat_map(move |placed| {
                        placed.steps.iter().map(move |step| {
                            (*step, placed.db.as_str(), participant.group.as_str())
                        })
                    })
                })
                .collect();
            steps.sort_unstable();
            let committed = |group: &str| record.commits.iter().find(|c| c.group == group);
            let revisions: Vec<(Vec<u8>, u64)> = steps
                .iter()
                .map(|(_, db, group)| {
                    let revision = committed(group)
                        .and_then(|commit| commit.revisions.iter().find(|(name, _)| name == db))
                        .map_or(0, |(_, revision)| *revision);
                    (db.as_bytes().to_vec(), revision)
                })
                .collect();
            let positions: Vec<(String, u64)> = record
                .participants
                .iter()
                .map(|participant| {
                    let position = committed(&participant.group).map_or(0, |c| c.position);
                    (participant.group.clone(), position)
                })
                .collect();
            let first = steps.first().map(|(_, _, group)| *group);
            let mut response = WriteResponse::value(
                bytes,
                revisions.first().map_or(0, |(_, revision)| *revision),
                first
                    .and_then(committed)
                    .map_or(0, |commit| commit.position),
            );
            response.revisions = revisions;
            response.positions = positions;
            Ok(response)
        }
        Some(Decision::Abort) => {
            let mut failure = fail(
                record.code.as_deref().unwrap_or(TXN_ABORTED),
                record
                    .reason
                    .clone()
                    .unwrap_or_else(|| "the transaction was aborted".to_string()),
            );
            failure.details = record.details.as_deref().map(parse).transpose()?;
            let bytes = flower_engine::json::render(&flower_sdk::abi::encode_err(&failure))
                .map_err(NodeError::engine)?;
            Ok(WriteResponse::failure(bytes, 0, 0))
        }
        None => Err(NodeError::coded(
            "transaction",
            "the transaction has no decision yet: retry with the same idempotency key",
        )),
    }
}

/// The participants a record names, as group identities.
pub fn participant_groups(record: &CoordinatorRecord) -> BTreeSet<String> {
    record
        .participants
        .iter()
        .map(|participant| participant.group.clone())
        .collect()
}
