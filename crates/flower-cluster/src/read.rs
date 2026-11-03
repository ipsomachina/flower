//! Read consistency: leader leases, read fences and replica staleness.
//!
//! Every query declares how it reads ([`ReadConsistency`]):
//!
//! - A **linearizable** query reflects every write that completed before
//!   it began, on any member. The leader proves it still holds them by
//!   its lease, or by a quorum round once the lease lapsed. A follower
//!   asks the leader for a [`ReadFence`]: the leader's commit index,
//!   proved the same way after the request arrived. Either member then
//!   waits until it applied the proven index and reads its own snapshot.
//! - A **replica-local** query reads the serving member's applied state
//!   as it is, and reports how stale that state may be
//!   ([`Reads::staleness`]). A bound, the query's tightened by its
//!   caller's, turns a replica past it, or one that does not know, into a
//!   fresh read first; when no fence can be had the read fails with
//!   retryable [`STALE_REPLICA`].
//!
//! # The lease
//!
//! OpenRaft 0.9 voters refuse every vote request for the leader lease
//! (E: `Config::leader_lease` in Flower's fork, 300 ms by default;
//! `election_timeout_max` upstream) after their vote last changed or was
//! touched: each append from their leader, heartbeats included, touches
//! it on arrival, and a restarted voter touches it when it opens. Nor do
//! they campaign before E plus an election timeout has passed, and with
//! pre-vote only once a quorum, each past its own E, would vote. A member
//! that opens rejoining (without Raft state, or having lost unsynced
//! writes) reloads its vote uncommitted, which that refusal ignores, but
//! grants no vote at all until it caught up with a leader, whose appends
//! re-arm the refusal ([`crate::rejoin`]); no lease survives a restart.
//! When a leader's quorum round that began at `t0` on its clock
//! succeeds, each voter of that quorum touched its vote at or after
//! `t0`, so none votes for another candidate until E of its own clock
//! after `t0`. A new leader needs votes from a quorum, which intersects
//! that one, so none can exist before the first of those refusals
//! ends. With every clock running within a factor `1 ± ρ` of real
//! time, a lease of length `δ` on the leader's clock ends at most
//! `δ / (1 − ρ)` of real time after `t0` and the refusals end at least
//! `E / (1 + ρ)` after it: the lease is safe while
//! `δ ≤ E (1 − ρ) / (1 + ρ)`. The default `δ = 0.9 E` allows clock
//! rates about 5% apart; [`crate::timing`] refuses `δ ≥ E`. Clocks must
//! not stop while a member runs: a host suspended to RAM could serve a
//! stale read on waking.
//!
//! One member of that quorum is not bound by its refusal: the leader
//! itself, which grants a vote once its own vote is older than E,
//! stepping down as it does. So a lease read first asks the Raft core
//! whether this member still holds the vote, role and membership the
//! lease was confirmed under. The core handles vote requests in order
//! with that question, so a vote granted before the read began ends the
//! lease. A membership change ends it too: the new configuration's
//! quorums need not intersect the one that confirmed it. A leadership
//! transfer voids the refusals: a member told to campaign at once
//! (`Raft::trigger().elect()`) ignores its own, and voters that release
//! the leader's lease (`Raft::release_leader_lease`, Flower's OpenRaft
//! fork) give up theirs until their vote changes. So the leader
//! surrenders its lease for the rest of its term before it asks
//! ([`Reads::surrender`]), and a transfer that fails does not restore
//! it. Nothing persists a release, so a leadership restored from the
//! files when its member reopens confirms no lease either.
//!
//! A round confirms a lease only after this leader applied an entry of
//! its own term (the round waits for its read index, which is at least
//! that entry), so a lease read never misses what an earlier leader
//! committed. Lease reads read the commit index the core reports, wait
//! for it to apply, and renew the lease in the background once half of
//! it has passed, so steady traffic never waits for a round.
//!
//! Concurrent proofs share work only forward in time (see [`Cohort`]): a
//! caller joins a round or a remote fence only if it began after the
//! caller arrived.
//!
//! # Staleness
//!
//! How stale a replica's applied state may be is the time since it last
//! held everything its leader had committed. A follower learns it from
//! the leader's appends: one that succeeds and leaves its log covering
//! the leader's commit index (heartbeats included) says the follower's
//! log held everything committed when it arrived; once it has also
//! applied that index, its state was current then. A fresh read fence
//! proves the same for the instant its proof began. A leader within its
//! lease is current, once it applied an entry of its own term: nobody
//! else can commit before the lease ends, so its staleness runs from
//! there. A sole voter is current on the same condition. A replica that
//! heard nothing has unknown staleness, which bounds treat as unbounded.
//! Instants are taken when an append arrives, so the one-way delay from
//! the leader is not counted. A follower still hearing a deposed leader
//! cut off with a minority (five voters or more) understates its
//! staleness until it learns the new term.

use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use openraft::error::Fatal;
use openraft::metrics::WaitError;
use openraft::{Config, LogId, Raft, ServerState, Vote};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex as AsyncMutex;

pub use flower_engine::descriptors::ReadConsistency;

use crate::network::Peers;
use crate::node::NodeError;
use crate::peer::{MemberRequest, MemberResponse};
use crate::timing::{RaftTiming, default_read_lease_ms};
use crate::types::{NodeId, TypeConfig};

/// Failure code: a replica staler than the read allows could not prove
/// a fresh read. Retryable, at another replica too.
pub const STALE_REPLICA: &str = "stale_replica";
/// Failure code: no quorum, leader or fence proved a fresh read in
/// time. Retryable.
pub const QUORUM: &str = "quorum";
/// Default deadline for proving one fresh read, applying it included.
pub const DEFAULT_READ_TIMEOUT: Duration = Duration::from_secs(10);

/// How this member proves fresh reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReadSettings {
    /// How long a confirmed leader answers fresh reads without another
    /// quorum round; `None` runs a round for every fresh read that
    /// arrives after the last one began.
    pub lease: Option<Duration>,
    /// Deadline for proving one fresh read: the lease check or round,
    /// or the leader's fence, plus applying what it proved.
    pub timeout: Duration,
}

impl ReadSettings {
    /// The default for a Raft configuration: a lease of nine tenths of
    /// its leader lease (`Config::leader_lease`).
    pub fn for_config(config: &Config) -> Self {
        Self::with_lease_ms(default_read_lease_ms(
            config.leader_lease().as_millis() as u64
        ))
    }

    /// The settings derived Raft timing asks for.
    pub fn from_timing(timing: &RaftTiming) -> Self {
        Self::with_lease_ms(timing.read_lease_ms)
    }

    fn with_lease_ms(lease_ms: u64) -> Self {
        ReadSettings {
            lease: (lease_ms > 0).then(|| Duration::from_millis(lease_ms)),
            timeout: DEFAULT_READ_TIMEOUT,
        }
    }
}

/// Proof for a fresh read: the log index a member must have applied
/// before its snapshot holds every write completed before the read
/// began. `None` when nothing was committed yet.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadFence {
    /// The proven commit index.
    pub index: Option<u64>,
}

/// A caller's read options.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReadOptions {
    /// Most staleness a replica-local query may be answered with: past
    /// it, or unknown, the replica proves a fresh read first. It only
    /// tightens a bound the query declares. Ignored by linearizable
    /// queries, which are fresh already.
    pub max_staleness: Option<Duration>,
    pub at_least: Option<flower_protocol::ReadToken>,
}

/// How an answer relates to the writes before it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Served {
    /// Proved fresh: by the leader's lease, a quorum round or the
    /// leader's fence.
    Fresh,
    /// A replica-local answer from state this stale at most (`None`:
    /// unknown).
    Local(Option<Duration>),
}

/// How a query will read, decided before it evaluates (see
/// [`crate::node::Node::plan_read`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadPlan {
    /// Freshness is proved: evaluate on any later snapshot, serving it
    /// this way.
    Proved(Served),
    /// Replica-local as declared: evaluate this member's state as it is,
    /// on a snapshot whose catalog still declares `declared`, at most
    /// `staleness` behind.
    Local {
        /// The consistency the query declared.
        declared: ReadConsistency,
        /// How stale this member's state may be (`None`: unknown).
        staleness: Option<Duration>,
    },
}

/// Fresh-read counters since the member opened.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct ReadMetrics {
    /// Fresh reads the leader's lease proved without a round.
    pub lease_reads: u64,
    /// Quorum rounds run to prove fresh reads or confirm the lease.
    pub quorum_rounds: u64,
    /// Read fences this member obtained from its leader.
    pub remote_fences: u64,
    /// Read fences this member proved for its followers.
    pub fences_served: u64,
    /// Requests this member forwarded to its leader (see
    /// [`crate::node::Node::forward`]).
    pub forwards: u64,
}

/// A confirmed lease: no other leader exists before `until`, while
/// this member holds `vote` and `membership`.
#[derive(Clone, Debug)]
struct Lease {
    vote: Vote<NodeId>,
    membership: Option<LogId<NodeId>>,
    until: Instant,
}

/// One proof shared by the callers that arrived before it began.
///
/// A caller notes how many proofs had started when it arrived, then
/// queues for the cohort. The proof that finished last answers it only
/// if that proof started afterwards; otherwise the caller starts the
/// next one, which every caller queued meanwhile shares. A proof that
/// began before a caller arrived never answers it.
#[derive(Default)]
pub(crate) struct Cohort {
    started: AtomicU64,
    last: AsyncMutex<Option<(u64, Result<ReadFence, NodeError>)>>,
}

impl Cohort {
    /// Share or run one proof that begins after this call.
    async fn run<F, Fut>(&self, prove: F) -> Result<ReadFence, NodeError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<ReadFence, NodeError>>,
    {
        let arrived = self.started.load(Ordering::SeqCst);
        let mut last = self.last.lock().await;
        if let Some((proof, outcome)) = last.as_ref()
            && *proof > arrived
        {
            return outcome.clone();
        }
        let proof = self.started.fetch_add(1, Ordering::SeqCst) + 1;
        let outcome = prove().await;
        *last = Some((proof, outcome.clone()));
        outcome
    }
}

/// When this replica last held everything committed: appends heard,
/// fences applied.
#[derive(Default)]
struct Clock {
    /// Covering appends heard but not yet applied: the oldest and the
    /// newest, with the commit index each covered. Keeping both lets a
    /// busy follower confirm the oldest while newer ones arrive.
    first: Option<(Instant, Option<u64>)>,
    last: Option<(Instant, Option<u64>)>,
    /// The applied state held everything committed as of this instant.
    current_at: Option<Instant>,
}

impl Clock {
    fn confirm(&mut self, at: Instant) {
        self.current_at = Some(self.current_at.map_or(at, |current| current.max(at)));
    }

    /// An append that arrived `at` left this replica's log covering the
    /// leader's commit index `commit`.
    fn heard(&mut self, at: Instant, commit: Option<u64>, applied: Option<u64>) {
        if commit <= applied {
            self.confirm(at);
        } else if self.first.is_none() {
            self.first = Some((at, commit));
        } else {
            self.last = Some((at, commit));
        }
    }

    /// When the applied state was last current, confirming the heard
    /// appends it has applied since.
    fn current(&mut self, applied: Option<u64>) -> Option<Instant> {
        while let Some((at, commit)) = self.first {
            if commit > applied {
                break;
            }
            self.confirm(at);
            self.first = self.last.take();
        }
        self.current_at
    }
}

/// Resets the renewal flag when a background renewal ends, however.
struct Renewing<'a>(&'a AtomicBool);

impl Drop for Renewing<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

/// One member's fresh-read machinery.
pub(crate) struct Reads {
    id: NodeId,
    raft: Raft<TypeConfig>,
    peers: Peers,
    /// The voters' refusal (the leader lease): read leases stay shorter.
    refusal: Duration,
    /// Lease length in milliseconds, 0 when off.
    lease_ms: AtomicU64,
    timeout_ms: AtomicU64,
    lease: Mutex<Option<Lease>>,
    /// The latest term whose lease this member surrendered: no lease
    /// is confirmed in it again.
    surrendered: Mutex<Option<u64>>,
    /// Quorum rounds at this member as leader.
    rounds: Cohort,
    /// Fences from the leader at this member as follower.
    remote: Cohort,
    renewing: AtomicBool,
    clock: Mutex<Clock>,
    /// When an append from the leader last took here.
    contact: Mutex<Option<Instant>>,
    /// How long the leader may stay silent before this member stops
    /// counting on it: three heartbeats, at most the leader lease.
    contact_window: Duration,
    lease_reads: AtomicU64,
    quorum_rounds: AtomicU64,
    remote_fences: AtomicU64,
    fences_served: AtomicU64,
    forwards: AtomicU64,
}

impl Reads {
    /// Fresh reads for a member with this Raft configuration.
    pub(crate) fn new(id: NodeId, raft: Raft<TypeConfig>, peers: Peers, config: &Config) -> Self {
        let reads = Reads {
            id,
            raft,
            peers,
            refusal: config.leader_lease(),
            lease_ms: AtomicU64::new(0),
            timeout_ms: AtomicU64::new(0),
            lease: Mutex::new(None),
            surrendered: Mutex::new(None),
            rounds: Cohort::default(),
            remote: Cohort::default(),
            renewing: AtomicBool::new(false),
            clock: Mutex::new(Clock::default()),
            contact: Mutex::new(None),
            contact_window: config.leader_lease().min(Duration::from_millis(
                config.heartbeat_interval.saturating_mul(3),
            )),
            lease_reads: AtomicU64::new(0),
            quorum_rounds: AtomicU64::new(0),
            remote_fences: AtomicU64::new(0),
            fences_served: AtomicU64::new(0),
            forwards: AtomicU64::new(0),
        };
        reads.configure(ReadSettings::for_config(config));
        reads
    }

    /// Apply settings. A lease at least as long as the voters' refusal
    /// could outlive it: it is cut to the default, with a warning.
    pub(crate) fn configure(&self, settings: ReadSettings) {
        let mut lease = settings.lease.unwrap_or(Duration::ZERO);
        if lease >= self.refusal {
            let safe =
                Duration::from_millis(default_read_lease_ms(self.refusal.as_millis() as u64));
            tracing::warn!(
                ?lease,
                ?safe,
                "a read lease must be shorter than the leader lease; using the default"
            );
            lease = safe;
        }
        self.lease_ms
            .store(lease.as_millis() as u64, Ordering::Release);
        self.timeout_ms
            .store(settings.timeout.as_millis() as u64, Ordering::Release);
        self.release();
    }

    /// The settings in force.
    pub(crate) fn settings(&self) -> ReadSettings {
        let lease_ms = self.lease_ms.load(Ordering::Acquire);
        ReadSettings {
            lease: (lease_ms > 0).then(|| Duration::from_millis(lease_ms)),
            timeout: self.timeout(),
        }
    }

    fn timeout(&self) -> Duration {
        Duration::from_millis(self.timeout_ms.load(Ordering::Acquire))
    }

    /// Drop the lease: fresh reads run rounds until one confirms a new
    /// lease.
    fn release(&self) {
        *self.lease.lock().unwrap_or_else(PoisonError::into_inner) = None;
    }

    /// Give up the lease and confirm none again in the current term:
    /// fresh reads run quorum rounds until this member leads in a later
    /// term. A leadership transfer calls this before asking another
    /// member to campaign at once, which ignores that member's own vote
    /// refusal; a transfer that fails does not restore the lease.
    pub(crate) fn surrender(&self) {
        let term = self.raft.metrics().borrow().current_term;
        self.surrender_term(term);
    }

    /// [`Reads::surrender`] for `term`: a lease of this leadership or an
    /// earlier one is never confirmed again.
    pub(crate) fn surrender_term(&self, term: u64) {
        let mut lease = self.lease.lock().unwrap_or_else(PoisonError::into_inner);
        let mut surrendered = self
            .surrendered
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        *surrendered = Some(surrendered.map_or(term, |held| held.max(term)));
        *lease = None;
    }

    /// The peer requests this member sends.
    pub(crate) fn peers(&self) -> &Peers {
        &self.peers
    }

    /// Count one request forwarded to the leader.
    pub(crate) fn forwarded(&self) {
        self.forwards.fetch_add(1, Ordering::Relaxed);
    }

    /// Counters since the member opened.
    pub(crate) fn metrics(&self) -> ReadMetrics {
        ReadMetrics {
            lease_reads: self.lease_reads.load(Ordering::Relaxed),
            quorum_rounds: self.quorum_rounds.load(Ordering::Relaxed),
            remote_fences: self.remote_fences.load(Ordering::Relaxed),
            fences_served: self.fences_served.load(Ordering::Relaxed),
            forwards: self.forwards.load(Ordering::Relaxed),
        }
    }

    /// Prove a fresh read at this member, whatever its role, and wait
    /// until it applied what the proof covers. With no leader known it
    /// forwards: the API sends the read on once one is elected.
    pub(crate) async fn fence(self: &Arc<Self>) -> Result<ReadFence, NodeError> {
        let leader = self.raft.metrics().borrow().current_leader;
        match leader {
            Some(leader) if leader == self.id => match self.leader_fence().await {
                // Deposed since the metrics said otherwise.
                Err(NodeError::Forward {
                    leader: Some(leader),
                }) if leader != self.id => self.remote_fence(leader).await,
                outcome => outcome,
            },
            Some(leader) => self.remote_fence(leader).await,
            None => Err(NodeError::Forward { leader: None }),
        }
    }

    /// Prove, as leader, that this member holds every write completed
    /// before the call, and wait until it applied them. Never asks
    /// another member: off the leader it fails with a forward.
    pub(crate) async fn leader_fence(self: &Arc<Self>) -> Result<ReadFence, NodeError> {
        let timeout = self.timeout();
        let proof = async {
            if let Some(fence) = self.lease_fence().await? {
                return Ok(fence);
            }
            let reads = self.clone();
            self.rounds.run(|| async move { reads.round().await }).await
        };
        match tokio::time::timeout(timeout, proof).await {
            Ok(outcome) => outcome,
            Err(_) => Err(NodeError::Failure {
                code: QUORUM.to_string(),
                message: format!("no quorum confirmed a fresh read within {timeout:?}"),
            }),
        }
    }

    /// Prove a fresh read for a follower: [`Reads::leader_fence`].
    pub(crate) async fn serve_fence(self: &Arc<Self>) -> Result<ReadFence, NodeError> {
        let fence = self.leader_fence().await?;
        self.fences_served.fetch_add(1, Ordering::Relaxed);
        Ok(fence)
    }

    /// A fence from this member's lease, if it holds one: checked
    /// against the core, then applied.
    async fn lease_fence(self: &Arc<Self>) -> Result<Option<ReadFence>, NodeError> {
        let Some(lease) = self.valid_lease(Instant::now()) else {
            return Ok(None);
        };
        // The core answers in order with the vote requests it handles,
        // so a vote this member granted before the read began shows.
        let (vote, state, membership, committed) = self
            .raft
            .with_raft_state(|state| {
                (
                    *state.vote_ref(),
                    state.server_state,
                    *state.membership_state.effective().log_id(),
                    state.committed,
                )
            })
            .await
            .map_err(fatal)?;
        if vote != lease.vote || state != ServerState::Leader || membership != lease.membership {
            self.release();
            return Ok(None);
        }
        let fence = ReadFence {
            index: committed.map(|id| id.index),
        };
        self.wait_applied(fence).await?;
        self.lease_reads.fetch_add(1, Ordering::Relaxed);
        self.maybe_renew(&lease);
        Ok(Some(fence))
    }

    /// The lease, if one holds at `now`.
    fn valid_lease(&self, now: Instant) -> Option<Lease> {
        let lease = self.lease.lock().unwrap_or_else(PoisonError::into_inner);
        lease.clone().filter(|lease| now < lease.until)
    }

    /// One quorum round: prove leadership through heartbeats a quorum
    /// acknowledges, wait for the read index to apply, and confirm a
    /// lease from the instant before the round began.
    async fn round(&self) -> Result<ReadFence, NodeError> {
        let (vote, membership) = self
            .raft
            .with_raft_state(|state| {
                (
                    *state.vote_ref(),
                    *state.membership_state.effective().log_id(),
                )
            })
            .await
            .map_err(fatal)?;
        let began = Instant::now();
        let read = self
            .raft
            .ensure_linearizable()
            .await
            .map_err(NodeError::linearizable)?;
        self.quorum_rounds.fetch_add(1, Ordering::Relaxed);
        let lease_ms = self.lease_ms.load(Ordering::Acquire);
        if lease_ms > 0 && vote.is_committed() && vote.leader_id().voted_for() == Some(self.id) {
            let until = began + Duration::from_millis(lease_ms);
            // Checked under the lease lock `surrender` takes too, so a
            // round in flight cannot confirm a surrendered term's lease.
            let mut lease = self.lease.lock().unwrap_or_else(PoisonError::into_inner);
            let surrendered = *self
                .surrendered
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if surrendered.is_some_and(|term| vote.leader_id().get_term() <= term) {
                return Ok(ReadFence {
                    index: read.map(|id| id.index),
                });
            }
            let newer = match lease.as_ref() {
                Some(held) if held.vote == vote && held.membership == membership => {
                    held.until < until
                }
                _ => true,
            };
            if newer {
                *lease = Some(Lease {
                    vote,
                    membership,
                    until,
                });
            }
        }
        Ok(ReadFence {
            index: read.map(|id| id.index),
        })
    }

    /// Renew the lease in the background once half of it has passed,
    /// so steady reads never wait for a round.
    fn maybe_renew(self: &Arc<Self>, lease: &Lease) {
        let lease_ms = self.lease_ms.load(Ordering::Acquire);
        let remaining = lease.until.saturating_duration_since(Instant::now());
        if lease_ms == 0 || remaining > Duration::from_millis(lease_ms / 2) {
            return;
        }
        if self.renewing.swap(true, Ordering::AcqRel) {
            return;
        }
        let reads = self.clone();
        tokio::spawn(async move {
            let _renewing = Renewing(&reads.renewing);
            let round = reads.clone();
            let _ = reads
                .rounds
                .run(|| async move { round.round().await })
                .await;
        });
    }

    /// Ask the leader for a fence, then apply it.
    async fn remote_fence(self: &Arc<Self>, leader: NodeId) -> Result<ReadFence, NodeError> {
        let timeout = self.timeout();
        let reads = self.clone();
        let proof = self.remote.run(|| async move {
            let began = Instant::now();
            let node = {
                let metrics = reads.raft.metrics();
                let metrics = metrics.borrow();
                metrics
                    .membership_config
                    .membership()
                    .get_node(&leader)
                    .cloned()
            };
            let Some(node) = node else {
                return Err(NodeError::Failure {
                    code: QUORUM.to_string(),
                    message: format!("leader {leader} is absent from the membership"),
                });
            };
            let observer = crate::telemetry::observer(&reads.peers.group().name);
            let asked = reads
                .peers
                .member(leader, &node.addr, MemberRequest::ReadFence, timeout);
            let fence = match observer
                .raft_rpc(crate::telemetry::Rpc::ReadFence, leader, None, asked)
                .await
            {
                Ok(MemberResponse::ReadFence(fence)) => fence,
                Ok(_) => {
                    return Err(NodeError::Failure {
                        code: QUORUM.to_string(),
                        message: format!("leader {leader} answered a read fence with another kind"),
                    });
                }
                Err(error) => {
                    return Err(NodeError::Failure {
                        code: QUORUM.to_string(),
                        message: format!("leader {leader} proved no read fence: {error}"),
                    });
                }
            };
            reads.wait_applied(fence).await?;
            reads.remote_fences.fetch_add(1, Ordering::Relaxed);
            reads
                .clock
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .confirm(began);
            Ok(fence)
        });
        match tokio::time::timeout(timeout, proof).await {
            Ok(outcome) => outcome,
            Err(_) => Err(NodeError::Failure {
                code: QUORUM.to_string(),
                message: format!("no read fence from leader {leader} applied within {timeout:?}"),
            }),
        }
    }

    /// Wait until this member applied what `fence` proves.
    async fn wait_applied(&self, fence: ReadFence) -> Result<(), NodeError> {
        if fence.index.is_none() {
            return Ok(());
        }
        match self
            .raft
            .wait(Some(self.timeout()))
            .applied_index_at_least(fence.index, "read fence")
            .await
        {
            Ok(_) => Ok(()),
            Err(WaitError::Timeout(waited, _)) => Err(NodeError::Failure {
                code: QUORUM.to_string(),
                message: format!("the read fence did not apply within {waited:?}"),
            }),
            Err(WaitError::ShuttingDown) => Err(NodeError::Fatal("Raft stopped".to_string())),
        }
    }

    /// A leader's append that arrived `at` succeeded, leaving this
    /// replica's log ending at `end`, while the leader had committed
    /// `commit`.
    pub(crate) fn heard(&self, at: Instant, commit: Option<u64>, end: Option<u64>) {
        {
            let mut contact = self.contact.lock().unwrap_or_else(PoisonError::into_inner);
            *contact = Some(contact.map_or(at, |known| known.max(at)));
        }
        if end < commit {
            return;
        }
        let applied = self.raft.metrics().borrow().last_applied.map(|id| id.index);
        self.clock
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .heard(at, commit, applied);
    }

    /// Whether an append from the leader took here within the contact
    /// window before `now`. A leader silent longer may be gone: requests
    /// wait for the next leader (or its next append) rather than go to it,
    /// and replies stop naming it, so clients do not chase a dead leader.
    pub(crate) fn hears_leader(&self, now: Instant) -> bool {
        self.contact
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_some_and(|at| now.saturating_duration_since(at) <= self.contact_window)
    }

    /// How stale this member's applied state may be at `now`; `None`
    /// when unknown.
    pub(crate) fn staleness(&self, now: Instant) -> Option<Duration> {
        let (leading, sole, term, applied) = {
            let metrics = self.raft.metrics();
            let metrics = metrics.borrow();
            // A leader vouches for its state only once it applied an
            // entry of its own term, which every earlier commit precedes.
            let leading = metrics.state == ServerState::Leader
                && metrics.current_leader == Some(self.id)
                && metrics
                    .last_applied
                    .is_some_and(|id| id.leader_id.term == metrics.current_term);
            let mut voters = metrics.membership_config.membership().voter_ids();
            let sole = voters.next() == Some(self.id) && voters.next().is_none();
            (
                leading,
                sole,
                metrics.current_term,
                metrics.last_applied.map(|id| id.index),
            )
        };
        if leading && sole {
            return Some(Duration::ZERO);
        }
        let lease_end = if leading {
            self.lease
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .as_ref()
                .filter(|lease| lease.vote.leader_id().get_term() == term)
                .map(|lease| lease.until)
        } else {
            None
        };
        let current = self
            .clock
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .current(applied);
        let as_of = match (lease_end, current) {
            (Some(lease), Some(current)) => Some(lease.max(current)),
            (lease, current) => lease.or(current),
        };
        as_of.map(|at| now.saturating_duration_since(at))
    }
}

/// A stopped Raft core.
fn fatal(error: Fatal<NodeId>) -> NodeError {
    NodeError::Fatal(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    const MS: Duration = Duration::from_millis(1);

    #[test]
    fn a_replica_that_heard_nothing_has_unknown_staleness() {
        let mut clock = Clock::default();
        assert_eq!(clock.current(Some(4)), None);
    }

    #[test]
    fn each_covering_append_resets_a_followers_staleness() {
        let mut clock = Clock::default();
        let heard = Instant::now();
        clock.heard(heard, Some(7), Some(7));
        assert_eq!(clock.current(Some(7)), Some(heard));
        let again = heard + 250 * MS;
        clock.heard(again, Some(7), Some(7));
        assert_eq!(clock.current(Some(7)), Some(again));
        // An older arrival confirmed late never moves it back.
        clock.heard(heard, Some(7), Some(7));
        assert_eq!(clock.current(Some(7)), Some(again));
    }

    #[test]
    fn an_append_counts_once_its_commit_index_is_applied() {
        let mut clock = Clock::default();
        let first = Instant::now();
        clock.heard(first, Some(5), Some(3));
        assert_eq!(clock.current(Some(3)), None);
        let second = first + 10 * MS;
        clock.heard(second, Some(9), Some(3));
        let third = second + 10 * MS;
        clock.heard(third, Some(12), Some(3));
        // Applying index 5 confirms the oldest; the newest waits for 12.
        assert_eq!(clock.current(Some(6)), Some(first));
        assert_eq!(clock.current(Some(11)), Some(first));
        assert_eq!(clock.current(Some(12)), Some(third));
    }

    #[tokio::test]
    async fn a_proof_never_answers_a_caller_that_arrived_after_it_began() {
        let cohort = Arc::new(Cohort::default());
        let runs = Arc::new(AtomicU64::new(0));
        let (begun_tx, begun_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
        let first = {
            let (cohort, runs) = (cohort.clone(), runs.clone());
            tokio::spawn(async move {
                cohort
                    .run(|| async move {
                        runs.fetch_add(1, Ordering::SeqCst);
                        let _ = begun_tx.send(());
                        let _ = release_rx.await;
                        Ok(ReadFence { index: Some(1) })
                    })
                    .await
            })
        };
        begun_rx.await.unwrap();
        // These arrive while the first proof runs: they share the next.
        let late: Vec<_> = (0..3)
            .map(|index| {
                let (cohort, runs) = (cohort.clone(), runs.clone());
                tokio::spawn(async move {
                    cohort
                        .run(|| async move {
                            runs.fetch_add(1, Ordering::SeqCst);
                            Ok(ReadFence {
                                index: Some(10 + index),
                            })
                        })
                        .await
                })
            })
            .collect();
        tokio::task::yield_now().await;
        release_tx.send(()).unwrap();
        assert_eq!(first.await.unwrap().unwrap().index, Some(1));
        let mut answers = Vec::new();
        for late in late {
            answers.push(late.await.unwrap().unwrap().index);
        }
        // One more proof served all three, none of them the first one.
        assert_eq!(runs.load(Ordering::SeqCst), 2);
        assert!(answers.iter().all(|index| *index == answers[0]));
        assert_ne!(answers[0], Some(1));
    }
}
