//! Member transport: in-process routing plus authenticated, framed JSON
//! over pooled TCP connections.
//!
//! The in-process network routes RPCs through a shared registry for
//! tests and single-process deployments; tests can drop the traffic one
//! member sends another ([`Registry::block`]).
//!
//! The TCP network keeps one connection per Raft client (OpenRaft
//! drives each replication stream through one client, serially) and
//! returns it to a small per-peer pool when the client goes, so the
//! short-lived clients behind quorum reads and Flower's own peer
//! requests reuse authenticated connections instead of dialing each
//! time. A connection opens with a hello: magic, version, the cluster's
//! peer token, the dialer's and the intended listener's node IDs and
//! this build's compatibility contract ([`crate::compat`]). The
//! listener compares the token in constant time before it reads
//! anything else, then refuses a hello meant for another node ID or
//! carrying another contract; it answers with its own ID and contract,
//! which the dialer checks in turn. Every request on the connection is
//! therefore between the two members its hello named, built to the same
//! contract: an address alias cannot count one process as two voters,
//! and a member of an incompatible build never exchanges an entry. A
//! listener configured without a token accepts any token (loopback test
//! clusters only: `cargo-flower serve` refuses an exposed listener
//! without one). With [`PeerConfig::tls`] the connection is TLS first
//! ([`crate::tls`]) and everything above runs inside it unchanged.
//!
//! Requests and responses then alternate as length-prefixed JSON
//! frames. Bodies are read incrementally as bytes arrive, never
//! pre-allocated from the claimed length, and capped at
//! [`PeerConfig::max_frame`]; reads and writes run under per-connection
//! deadlines, and the listener serves at most
//! [`PeerConfig::max_connections`] connections at once. Frames over
//! 64 KiB also take room from a budget of bytes the listener holds at
//! once ([`PeerConfig::max_queued_bytes`]) until their response is
//! written; a frame that finds no room is read off the wire and
//! discarded, and answered `Busy`. A leader told `Busy` resends one
//! entry at a time and, after a second refusal in a row, waits before
//! its next data append, doubling the wait with each further refusal:
//! a slow follower costs the leader time, never the follower's memory.
//!
//! Each RPC honors OpenRaft's [`RPCOption::hard_ttl`], and an exchange
//! takes its connection out of the client until its whole response has
//! arrived: one OpenRaft abandons mid-way (it drops calls at the same
//! deadline) closes its connection rather than leave a response behind
//! that would answer the next request. A data append is different:
//! OpenRaft gives every AppendEntries one RPC deadline (500 ms by
//! default, [`crate::timing`]), which a
//! large entry (a deployment's Wasm bundle) on a slow link can never
//! meet, so it runs in a task of its own with time for its bytes
//! ([`PeerConfig::append_bytes_per_second`]), and OpenRaft's retry of
//! the same entries resumes waiting for it instead of sending them
//! again. Snapshots travel as OpenRaft's chunked InstallSnapshot
//! segments of `snapshot_max_chunk_size` bytes, so no snapshot needs to
//! fit one frame. An AppendEntries batch over the frame cap asks
//! OpenRaft to resend fewer entries. Connection and handshake failures
//! surface as unreachable (OpenRaft backs off); failures mid-exchange
//! as network errors (retried promptly); remote Raft errors collapse to
//! unreachable: the peer is broken or gone.
//!
//! One process hosts any number of groups ([`crate::host`]) on one
//! listener. The hello also names the group the connection belongs to,
//! by name and uid ([`GroupId`]); the listener answers it through that
//! group's service from its [`PeerDirectory`], and refuses a group it
//! does not host, or hosts under another uid, before reading a request.
//! Connections and their pools are per peer and group: the transport
//! runs one exchange at a time per connection, so one group's large
//! appends or snapshot segments never wait in front of another group's
//! heartbeats. The in-process registry keys services by group and node
//! the same way.
//!
//! Beside OpenRaft's three RPCs the transport carries Flower's own:
//! `Info` (a peer's identity, initialization and contract, probed
//! before it joins), `Fence` (a leader's quorum-confirmed read log ID,
//! for a member catching up after opening without Raft state),
//! `TransferLeader` (a leader asking every other voter to release its
//! lease, and then a caught-up voter to campaign) and
//! `Member` (the member channel of [`crate::peer`]: read fences proved
//! by the leader's lease, client requests forwarded to the leader's API,
//! and where that API serves). [`Peers`] sends them; [`PeerService`]
//! answers them, and tells its member about each append from the
//! leader that takes, for its staleness ([`crate::read`]).

use std::collections::{HashMap, HashSet};
use std::io::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError, RwLock};
use std::time::Duration;

use openraft::error::{
    InstallSnapshotError, NetworkError, PayloadTooLarge, RPCError, RaftError, RemoteError,
    Unreachable,
};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    Raft, VoteRequest, VoteResponse,
};
use openraft::{BasicNode, LogId, Vote};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::Instant;

use crate::compat::{Compatibility, compatibility, contract};
use crate::group::{GroupId, MAX_NAME};
use crate::peer::{ForwardedResponse, MemberDesk, MemberError, MemberRequest, MemberResponse};
use crate::rejoin::Rejoin;
use crate::telemetry::{Observer, Rpc};
use crate::tls::{PeerTls, Stream};
use crate::types::{NodeId, TypeConfig};

/// A member's answer to an `Info` probe.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerInfo {
    /// Its node ID.
    pub id: NodeId,
    /// Whether it holds any Raft state (a vote or a log).
    pub initialized: bool,
    /// Whether it opened without Raft state and has not caught up yet
    /// (see [`crate::rejoin`]).
    pub rejoining: bool,
    /// Its build's compatibility.
    pub compatibility: Compatibility,
}

/// A leader's request that its group let `to` lead: `to` campaigns,
/// any other voter releases the leader's lease, granting `to`'s vote at
/// once rather than after election max (OpenRaft fork,
/// `Raft::release_leader_lease`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransferRequest {
    /// The leader handing over.
    pub from: NodeId,
    /// The term it leads.
    pub term: u64,
    /// Its last log index: the target must hold the log up to it.
    pub last_log_index: Option<u64>,
    /// The voter to lead next.
    pub to: NodeId,
}

/// What answers peer requests on a member: its Raft handle, plus the
/// rejoin state that gates its votes and the desk that answers member
/// requests. A bare service (a Raft handle registered on its own)
/// withholds nothing and answers no member requests.
#[derive(Clone)]
pub struct PeerService {
    id: NodeId,
    group: GroupId,
    raft: Raft<TypeConfig>,
    rejoin: Option<Arc<Rejoin>>,
    desk: Option<Arc<MemberDesk>>,
}

impl PeerService {
    /// A service answering for `raft` alone, in the root group.
    pub fn bare(id: NodeId, raft: Raft<TypeConfig>) -> Self {
        PeerService {
            id,
            group: GroupId::root(),
            raft,
            rejoin: None,
            desk: None,
        }
    }

    /// A member's full service.
    pub(crate) fn new(
        id: NodeId,
        group: GroupId,
        raft: Raft<TypeConfig>,
        rejoin: Arc<Rejoin>,
        desk: Arc<MemberDesk>,
    ) -> Self {
        PeerService {
            id,
            group,
            raft,
            rejoin: Some(rejoin),
            desk: Some(desk),
        }
    }

    /// The node ID this service answers for.
    pub fn id(&self) -> NodeId {
        self.id
    }

    /// The group this service answers for.
    pub fn group(&self) -> &GroupId {
        &self.group
    }

    /// Whether the member is still catching up after opening without
    /// Raft state.
    fn rejoining(&self) -> bool {
        self.rejoin.as_ref().is_some_and(|rejoin| rejoin.amnesiac())
    }

    /// Take an append, and tell the member's desk about one that took:
    /// when it arrived, how far it left the log, and what the leader
    /// had committed (the follower's staleness, [`crate::read`]).
    async fn append_entries(
        &self,
        rpc: AppendEntriesRequest<TypeConfig>,
    ) -> Result<AppendEntriesResponse<NodeId>, RaftError<NodeId>> {
        let arrived = std::time::Instant::now();
        let commit = rpc.leader_commit.map(|id| id.index);
        let end = rpc
            .entries
            .last()
            .map(|entry| entry.log_id.index)
            .or(rpc.prev_log_id.map(|id| id.index));
        let response = self.raft.append_entries(rpc).await?;
        if let Some(desk) = &self.desk {
            let end = match &response {
                AppendEntriesResponse::Success => Some(end),
                AppendEntriesResponse::PartialSuccess(matching) => {
                    Some(matching.map(|id| id.index))
                }
                AppendEntriesResponse::Conflict | AppendEntriesResponse::HigherVote(_) => None,
            };
            if let Some(end) = end {
                desk.heard(arrived, commit, end);
            }
        }
        Ok(response)
    }

    async fn install_snapshot(
        &self,
        rpc: InstallSnapshotRequest<TypeConfig>,
    ) -> Result<InstallSnapshotResponse<NodeId>, RaftError<NodeId, InstallSnapshotError>> {
        self.raft.install_snapshot(rpc).await
    }

    /// Answer a vote request, unless a rejoining member must refuse it.
    async fn vote(
        &self,
        rpc: VoteRequest<NodeId>,
    ) -> Result<VoteResponse<NodeId>, RaftError<NodeId>> {
        if let Some(refusal) = self
            .rejoin
            .as_ref()
            .and_then(|rejoin| rejoin.withheld_vote(&self.raft, &rpc))
        {
            return Ok(refusal);
        }
        self.raft.vote(rpc).await
    }

    /// This member's identity, initialization and contract.
    pub async fn info(&self) -> Result<PeerInfo, String> {
        let initialized = self
            .raft
            .is_initialized()
            .await
            .map_err(|error| error.to_string())?;
        Ok(PeerInfo {
            id: self.id,
            initialized,
            rejoining: self.rejoining(),
            compatibility: compatibility().clone(),
        })
    }

    /// As leader, confirm leadership through a quorum and answer the
    /// read log ID once this member applied it.
    async fn fence(&self) -> Result<Option<LogId<NodeId>>, String> {
        self.raft
            .ensure_linearizable()
            .await
            .map_err(|error| error.to_string())
    }

    /// Answer a member request ([`crate::peer`]) through the member's
    /// desk.
    async fn member(&self, request: MemberRequest) -> Result<MemberResponse, MemberError> {
        match &self.desk {
            Some(desk) => desk.handle(request).await,
            None => Err(MemberError::Failed {
                code: "unavailable".to_string(),
                message: format!("node {} answers no member requests", self.id),
            }),
        }
    }

    /// Take this member's part in the leadership transfer `request`
    /// describes, if it may (see [`crate::node::Node::transfer_leadership`]):
    /// as its target, campaign; as another voter, release the leader's
    /// lease so that the target's vote is granted at once.
    async fn transfer(&self, request: TransferRequest) -> Result<(), String> {
        if self.rejoining() {
            return Err(format!(
                "node {} opened without Raft state and has not caught up",
                self.id
            ));
        }
        let metrics = self.raft.metrics().borrow().clone();
        if metrics.current_leader != Some(request.from) || metrics.current_term != request.term {
            return Err(format!(
                "node {} follows {:?} in term {}, not node {} in term {}",
                self.id, metrics.current_leader, metrics.current_term, request.from, request.term
            ));
        }
        let membership = metrics.membership_config.membership();
        if !membership.voter_ids().any(|voter| voter == self.id) {
            return Err(format!("node {} is not a voter", self.id));
        }
        if request.to != self.id {
            // The core checks again, in order with the votes it handles.
            let leader = Vote::new_committed(request.term, request.from);
            return match self.raft.release_leader_lease(leader).await {
                Ok(true) => Ok(()),
                Ok(false) => Err(format!(
                    "node {} no longer follows node {} in term {}",
                    self.id, request.from, request.term
                )),
                Err(error) => Err(error.to_string()),
            };
        }
        if metrics.last_log_index < request.last_log_index {
            return Err(format!(
                "node {} holds the log to {:?}, not {:?}",
                self.id, metrics.last_log_index, request.last_log_index
            ));
        }
        self.raft
            .trigger()
            .elect()
            .await
            .map_err(|error| error.to_string())
    }
}

/// In-process members by group name and node ID.
type Members = HashMap<(Arc<str>, NodeId), PeerService>;

/// Registry of live in-process members by group and node ID.
#[derive(Clone, Default)]
pub struct Registry {
    nodes: Arc<Mutex<Members>>,
    /// Blocked `(from, to)` directions, every group's traffic alike.
    blocked: Arc<Mutex<HashSet<(NodeId, NodeId)>>>,
}

impl Registry {
    /// An empty registry.
    pub fn new() -> Self {
        Registry::default()
    }

    /// Register a member's Raft handle for routing. Its votes route
    /// straight to the handle: register the member's
    /// [`crate::node::Node::peer_service`] with [`Registry::register_peer`]
    /// for its rejoin gate.
    pub fn register(&self, id: NodeId, raft: Raft<TypeConfig>) {
        self.register_peer(PeerService::bare(id, raft));
    }

    /// Register a member's peer service for routing, under its group.
    pub fn register_peer(&self, service: PeerService) {
        self.nodes
            .lock()
            .expect("registry poisoned")
            .insert((service.group.name.clone(), service.id), service);
    }

    /// Remove a node from every group, as if its process died.
    pub fn unregister(&self, id: NodeId) {
        self.nodes
            .lock()
            .expect("registry poisoned")
            .retain(|(_, node), _| *node != id);
    }

    /// Remove one group's member, as if the process closed its replica.
    pub fn unregister_group(&self, group: &str, id: NodeId) {
        self.nodes
            .lock()
            .expect("registry poisoned")
            .remove(&(Arc::from(group), id));
    }

    /// Drop every request `from` sends `to`, Raft's and Flower's own, in
    /// every group, as an unreachable peer. Only traffic from a factory
    /// that knows its sender ([`InProcessFactory::with_source`]) can be
    /// blocked.
    pub fn block(&self, from: NodeId, to: NodeId) {
        self.blocked
            .lock()
            .expect("registry poisoned")
            .insert((from, to));
    }

    /// Unblock every direction.
    pub fn heal(&self) {
        self.blocked.lock().expect("registry poisoned").clear();
    }

    /// Whether requests from `from` to `to` are dropped.
    fn is_blocked(&self, from: Option<NodeId>, to: NodeId) -> bool {
        from.is_some_and(|from| {
            self.blocked
                .lock()
                .expect("registry poisoned")
                .contains(&(from, to))
        })
    }

    /// Look up a group's member, if registered under that group's uid.
    fn get(&self, group: &GroupId, id: NodeId) -> Option<PeerService> {
        self.nodes
            .lock()
            .expect("registry poisoned")
            .get(&(group.name.clone(), id))
            .filter(|service| service.group.uid == group.uid)
            .cloned()
    }
}

/// The voter a leadership transfer hands leadership to, while one runs
/// on this member: the member's clients send AppendEntries and
/// InstallSnapshot to that voter alone, so the other voters' leases on
/// this leader run out (see [`crate::node::Node::transfer_leadership`]).
#[derive(Debug, Default)]
pub struct Withhold {
    target: Mutex<Option<NodeId>>,
}

impl Withhold {
    /// Send to `to` alone, or (`None`) to everyone again.
    fn set(&self, to: Option<NodeId>) {
        *self.target.lock().expect("withhold poisoned") = to;
    }

    /// Whether replication to `target` is held back.
    fn withholds(&self, target: NodeId) -> bool {
        matches!(*self.target.lock().expect("withhold poisoned"), Some(to) if to != target)
    }
}

/// A network factory that also carries Flower's own peer requests.
pub trait PeerFactory: RaftNetworkFactory<TypeConfig> {
    /// Flower's peer requests over this factory's transport, sharing its
    /// clients' replication hold.
    fn peers(&self) -> Peers;

    /// The group this factory's connections belong to.
    fn group(&self) -> GroupId;
}

/// Flower's own peer requests: `Info`, `Fence`, `TransferLeader` and
/// member requests ([`crate::peer`]).
#[derive(Clone)]
pub struct Peers {
    route: Route,
    withhold: Arc<Withhold>,
    /// Member requests in flight at once: each holds a connection, and a
    /// listener's connection cap is shared with Raft's.
    members: Arc<tokio::sync::Semaphore>,
}

/// How [`Peers`] reaches a member.
#[derive(Clone)]
enum Route {
    /// Through the registry to a group's members, from this member (when
    /// known, so tests can block its traffic).
    InProcess(Registry, Option<NodeId>, GroupId),
    Tcp(Dialer),
}

impl Peers {
    /// The group these peers serve.
    pub fn group(&self) -> &GroupId {
        match &self.route {
            Route::InProcess(_, _, group) => group,
            Route::Tcp(dialer) => &dialer.group,
        }
    }

    /// Probe `target` at `addr`: its identity (the hello checks it),
    /// initialization and contract.
    pub async fn info(
        &self,
        target: NodeId,
        addr: &str,
        timeout: Duration,
    ) -> Result<PeerInfo, String> {
        match &self.route {
            Route::InProcess(registry, source, group) => {
                let service = registry_get(registry, *source, group, target)?;
                within(timeout, service.info()).await?
            }
            Route::Tcp(dialer) => match dialer
                .request(target, addr, &TcpRequest::Info, timeout)
                .await?
            {
                TcpResponse::Info(info) => info,
                _ => Err("wrong response kind".to_string()),
            },
        }
    }

    /// Ask the leader `target` for a quorum-confirmed read log ID.
    pub async fn fence(
        &self,
        target: NodeId,
        addr: &str,
        timeout: Duration,
    ) -> Result<Option<LogId<NodeId>>, String> {
        match &self.route {
            Route::InProcess(registry, source, group) => {
                let service = registry_get(registry, *source, group, target)?;
                within(timeout, service.fence()).await?
            }
            Route::Tcp(dialer) => {
                match dialer
                    .request(target, addr, &TcpRequest::Fence, timeout)
                    .await?
                {
                    TcpResponse::Fence(fence) => fence,
                    _ => Err("wrong response kind".to_string()),
                }
            }
        }
    }

    /// Ask the voter `target` to campaign for the leadership `request`
    /// hands over.
    pub async fn transfer(
        &self,
        target: NodeId,
        addr: &str,
        request: TransferRequest,
        timeout: Duration,
    ) -> Result<(), String> {
        match &self.route {
            Route::InProcess(registry, source, group) => {
                let service = registry_get(registry, *source, group, target)?;
                within(timeout, service.transfer(request)).await?
            }
            Route::Tcp(dialer) => {
                let request = TcpRequest::TransferLeader(request);
                match dialer.request(target, addr, &request, timeout).await? {
                    TcpResponse::TransferLeader(outcome) => outcome,
                    _ => Err("wrong response kind".to_string()),
                }
            }
        }
    }

    /// Send a member request ([`crate::peer`]) to `target` at `addr` and
    /// wait up to `timeout`. Sent at most once: a request that may have
    /// arrived is never sent again, since a forwarded write must not run
    /// twice. [`MemberError::Unreachable`] means nothing ran;
    /// [`MemberError::Broken`], that the outcome is unknown.
    pub async fn member(
        &self,
        target: NodeId,
        addr: &str,
        request: MemberRequest,
        timeout: Duration,
    ) -> Result<MemberResponse, MemberError> {
        self.member_unless(target, addr, request, timeout, std::future::pending())
            .await
    }

    /// [`Peers::member`], given up as [`MemberError::Unreachable`] if
    /// `abandon` resolves while the connection is still being dialed,
    /// before anything was sent: dialing a dead host can take seconds,
    /// and a forward should go to the next leader meanwhile.
    pub async fn member_unless(
        &self,
        target: NodeId,
        addr: &str,
        request: MemberRequest,
        timeout: Duration,
        abandon: impl Future<Output = ()>,
    ) -> Result<MemberResponse, MemberError> {
        let Ok(Ok(_slot)) = tokio::time::timeout(timeout, self.members.acquire()).await else {
            return Err(MemberError::Unreachable(format!(
                "no member request slot freed within {timeout:?}"
            )));
        };
        match &self.route {
            Route::InProcess(registry, source, group) => {
                let service = registry_get(registry, *source, group, target)
                    .map_err(MemberError::Unreachable)?;
                match tokio::time::timeout(timeout, service.member(request)).await {
                    Ok(outcome) => outcome,
                    Err(_) => Err(MemberError::Broken(format!(
                        "node {target} gave no answer within {timeout:?}"
                    ))),
                }
            }
            Route::Tcp(dialer) => dialer.member(target, addr, request, timeout, abandon).await,
        }
    }

    /// Hold replication back from every voter but `to`, or (`None`)
    /// release it.
    pub(crate) fn withhold(&self, to: Option<NodeId>) {
        self.withhold.set(to);
    }
}

/// A registered member `source` may reach, or why there is none.
fn registry_get(
    registry: &Registry,
    source: Option<NodeId>,
    group: &GroupId,
    target: NodeId,
) -> Result<PeerService, String> {
    if registry.is_blocked(source, target) {
        return Err(format!("traffic to node {target} is blocked"));
    }
    registry
        .get(group, target)
        .ok_or_else(|| format!("node {target} hosts no group {group}"))
}

/// Run `work` within `timeout`.
async fn within<T>(timeout: Duration, work: impl Future<Output = T>) -> Result<T, String> {
    tokio::time::timeout(timeout, work)
        .await
        .map_err(|_| format!("no answer within {timeout:?}"))
}

/// Factory for in-process connections through a [`Registry`].
#[derive(Clone)]
pub struct InProcessFactory {
    registry: Registry,
    withhold: Arc<Withhold>,
    source: Option<NodeId>,
    group: GroupId,
}

impl InProcessFactory {
    /// Route through this registry, to the root group's members.
    pub fn new(registry: Registry) -> Self {
        InProcessFactory {
            registry,
            withhold: Arc::default(),
            source: None,
            group: GroupId::root(),
        }
    }

    /// Name the member sending through this factory, so the registry
    /// can block its traffic ([`Registry::block`]).
    pub fn with_source(mut self, id: NodeId) -> Self {
        self.source = Some(id);
        self
    }

    /// Route to the members of `group` instead.
    pub fn for_group(mut self, group: GroupId) -> Self {
        self.group = group;
        self
    }
}

impl RaftNetworkFactory<TypeConfig> for InProcessFactory {
    type Network = InProcessNetwork;

    async fn new_client(&mut self, target: NodeId, _node: &BasicNode) -> InProcessNetwork {
        InProcessNetwork {
            target,
            source: self.source,
            group: self.group.clone(),
            registry: self.registry.clone(),
            withhold: self.withhold.clone(),
            observer: crate::telemetry::observer(&self.group.name),
        }
    }
}

impl PeerFactory for InProcessFactory {
    fn peers(&self) -> Peers {
        Peers {
            route: Route::InProcess(self.registry.clone(), self.source, self.group.clone()),
            withhold: self.withhold.clone(),
            members: Arc::new(tokio::sync::Semaphore::new(MEMBER_REQUESTS)),
        }
    }

    fn group(&self) -> GroupId {
        self.group.clone()
    }
}

/// One in-process connection to a member.
pub struct InProcessNetwork {
    target: NodeId,
    source: Option<NodeId>,
    group: GroupId,
    registry: Registry,
    withhold: Arc<Withhold>,
    /// The group's instruments.
    observer: Arc<Observer>,
}

impl InProcessNetwork {
    /// The target's service, or an unreachable error when missing or
    /// blocked.
    fn handle<E: std::error::Error>(&self) -> Result<PeerService, RPCError<NodeId, BasicNode, E>> {
        if self.registry.is_blocked(self.source, self.target) {
            return Err(unreachable(format!(
                "traffic to node {} is blocked",
                self.target
            )));
        }
        self.registry.get(&self.group, self.target).ok_or_else(|| {
            unreachable(format!(
                "node {} hosts no group {}",
                self.target, self.group
            ))
        })
    }

    /// Refuse replication held back for a leadership transfer.
    fn check_withheld<E: std::error::Error>(&self) -> Result<(), RPCError<NodeId, BasicNode, E>> {
        if self.withhold.withholds(self.target) {
            return Err(unreachable(format!(
                "replication to node {} is held back while leadership moves",
                self.target
            )));
        }
        Ok(())
    }
}

impl RaftNetwork<TypeConfig> for InProcessNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let operation = append_kind(&rpc);
        let call = async {
            self.check_withheld()?;
            let target = self.handle()?;
            target
                .append_entries(rpc)
                .await
                .map_err(|error| RPCError::RemoteError(RemoteError::new(self.target, error)))
        };
        self.observer
            .raft_rpc(operation, self.target, Some(option.hard_ttl()), call)
            .await
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        let call = async {
            self.check_withheld()?;
            let target = self.handle()?;
            target
                .install_snapshot(rpc)
                .await
                .map_err(|error| RPCError::RemoteError(RemoteError::new(self.target, error)))
        };
        self.observer
            .raft_rpc(Rpc::Snapshot, self.target, None, call)
            .await
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let call = async {
            let target = self.handle()?;
            target
                .vote(rpc)
                .await
                .map_err(|error| RPCError::RemoteError(RemoteError::new(self.target, error)))
        };
        self.observer
            .raft_rpc(Rpc::Vote, self.target, None, call)
            .await
    }
}

/// Whether an AppendEntries carries entries or is a heartbeat.
fn append_kind(rpc: &AppendEntriesRequest<TypeConfig>) -> Rpc {
    if rpc.entries.is_empty() {
        Rpc::Heartbeat
    } else {
        Rpc::Append
    }
}

/// Hello frame magic.
const MAGIC: &[u8; 4] = b"FLRP";
/// Hello frame protocol version: 3 names the group; 4 says the largest
/// entry the dialer may send.
const VERSION: u8 = 4;
/// Longest accepted peer token, in bytes.
const MAX_TOKEN: usize = 1024;
/// Longest accepted compatibility contract, in bytes.
const MAX_CONTRACT: usize = 256;
/// Hello acknowledgement: accepted; the listener's ID and contract follow.
const ACCEPTED: u8 = 0;
/// Hello acknowledgement: wrong token, version or framing.
const REJECTED: u8 = 1;
/// Hello acknowledgement: the listener is another node; its ID and
/// contract follow.
const WRONG_NODE: u8 = 2;
/// Hello acknowledgement: incompatible contract; the listener's ID and
/// contract follow.
const INCOMPATIBLE: u8 = 3;
/// Hello acknowledgement: the listener hosts no such group, or hosts it
/// under another uid; its ID and contract follow.
const WRONG_GROUP: u8 = 4;
/// Hello acknowledgement: the listener's frames cannot carry the largest
/// entry the dialer may send (its transaction bound, see
/// [`flower_engine::patch::frame_for_entry`]); its ID and contract
/// follow. Every member's inbound frame must fit what any leader
/// proposes, which no member's own startup check can see.
const FRAME_TOO_SMALL: u8 = 5;
/// Member requests one member sends at once ([`Peers::member`]).
const MEMBER_REQUESTS: usize = 16;
/// Initial body buffer: bodies grow past it only as bytes arrive.
const READ_CHUNK: usize = 64 * 1024;
/// Frames up to this size skip the replication budget: heartbeats,
/// votes, small appends and Flower's own requests always get through.
const SMALL_FRAME: u64 = 64 * 1024;
/// The frame length a listener writes in place of a response when it
/// closes an idle connection to make room (see [`Connections`]): no
/// response is empty, so a dialer that reads it knows that the request
/// it sent, if any, was never read, and sends it again on a fresh dial.
/// An older dialer reads it as a broken response.
const EVICTED: u64 = 0;
/// Room the replication budget keeps beyond one largest frame, for the
/// frames around it.
const BUDGET_SLACK: u64 = 1024 * 1024;
/// Idle connections kept per peer.
const POOL_PER_PEER: usize = 4;
/// The default connection cap: room for every Raft client of a few
/// dozen hosted groups (one per peer and group replicating, voting or
/// proving a read at once, and four pooled per peer and group) beside
/// as many forwarded requests. A process hosting eight groups of three
/// once had all 64 of an older default busy after a member restarted:
/// its heartbeats and votes found no connection, and a group elected
/// four times in two seconds.
pub const DEFAULT_MAX_CONNECTIONS: usize = 512;
/// The wait before a data append after a second refusal in a row; it
/// doubles with each further one.
const REFUSAL_BACKOFF: Duration = Duration::from_millis(25);
/// The first wait before dialing a peer again (see [`reconnect_backoff`]).
const RECONNECT_BACKOFF: Duration = Duration::from_millis(25);
/// The longest wait before dialing a peer again: OpenRaft's own.
const RECONNECT_BACKOFF_MAX: Duration = Duration::from_millis(500);

/// Peer transport settings shared by dialers and the listener.
#[derive(Clone, Debug)]
pub struct PeerConfig {
    /// The cluster's shared peer credential. `None` disables
    /// authentication, for loopback test clusters only.
    pub token: Option<Arc<str>>,
    /// Largest request or response frame, in bytes. Must exceed the
    /// JSON encoding of the largest single log entry (a deployment's
    /// Wasm bundle) and of one snapshot segment.
    pub max_frame: u64,
    /// Most concurrent inbound connections, every group's together. At
    /// the cap a dial closes the connection idle longest; only a cap of
    /// busy connections refuses it, and forwarded client requests hold
    /// at most half (see [`DEFAULT_MAX_CONNECTIONS`]).
    pub max_connections: usize,
    /// Deadline for a dialer's hello.
    pub handshake_timeout: Duration,
    /// Idle time between requests before the listener hangs up. Pooled
    /// connections idle half as long at most.
    pub idle_timeout: Duration,
    /// Deadline for reading the rest of a frame and for each write.
    pub io_timeout: Duration,
    /// Bytes of inbound frames over 64 KiB the listener holds at once,
    /// from their first byte until their response is written; `None`:
    /// twice [`PeerConfig::max_frame`] plus 1 MiB. At least one largest
    /// frame plus 1 MiB.
    pub max_queued_bytes: Option<u64>,
    /// The least time a data append gets, however short OpenRaft's
    /// RPC deadline.
    pub append_timeout: Duration,
    /// The slowest transfer a data append is given time for: each byte
    /// adds `1 / append_bytes_per_second` to its deadline.
    pub append_bytes_per_second: u64,
    /// Run every peer connection inside TLS with these credentials and
    /// trust roots; `None` for plaintext. Every member must agree.
    pub tls: Option<Arc<PeerTls>>,
}

impl Default for PeerConfig {
    fn default() -> Self {
        PeerConfig {
            token: None,
            max_frame: 256 * 1024 * 1024,
            max_connections: DEFAULT_MAX_CONNECTIONS,
            handshake_timeout: Duration::from_secs(5),
            idle_timeout: Duration::from_secs(120),
            io_timeout: Duration::from_secs(60),
            max_queued_bytes: None,
            append_timeout: Duration::from_secs(5),
            append_bytes_per_second: 8 * 1024 * 1024,
            tls: None,
        }
    }
}

impl PeerConfig {
    /// Default settings with a peer token.
    pub fn with_token(token: impl Into<Arc<str>>) -> Self {
        PeerConfig {
            token: Some(token.into()),
            ..PeerConfig::default()
        }
    }

    /// The replication budget in bytes.
    pub fn queued_bytes(&self) -> u64 {
        self.max_queued_bytes.unwrap_or_else(|| {
            self.max_frame
                .saturating_mul(2)
                .saturating_add(BUDGET_SLACK)
        })
    }

    /// Reject settings under which replication could never progress.
    pub fn validate(&self) -> Result<(), String> {
        if self.max_frame == 0 || self.max_connections == 0 {
            return Err("the Raft frame cap and connection cap must be positive".to_string());
        }
        // A follower must always have room for one largest append, or
        // it never catches up: every attempt would be refused.
        let least = self.max_frame.saturating_add(BUDGET_SLACK);
        if self.queued_bytes() < least {
            return Err(format!(
                "the Raft queued-input budget ({}) must hold one frame of --raft-max-frame-bytes \
                 ({}) plus 1 MiB: at least {least} bytes",
                self.queued_bytes(),
                self.max_frame
            ));
        }
        if self.append_bytes_per_second == 0 {
            return Err("the Raft append rate must be positive".to_string());
        }
        let longest = self.append_allowance(self.append_timeout, self.max_frame as usize);
        if Instant::now().checked_add(longest).is_none() {
            return Err("the Raft append timeout and rate exceed the timer range".to_string());
        }
        Ok(())
    }

    /// How long a data append of `bytes` encoded bytes may take on the
    /// wire and at the follower: at least `base` and
    /// [`PeerConfig::append_timeout`], plus its bytes at the slowest
    /// rate. A fixed deadline would fail a large append on every attempt
    /// over a slow link.
    pub fn append_allowance(&self, base: Duration, bytes: usize) -> Duration {
        let nanos = (bytes as u128)
            .saturating_mul(1_000_000_000)
            .div_ceil(u128::from(self.append_bytes_per_second.max(1)));
        base.max(self.append_timeout)
            .saturating_add(Duration::from_nanos(
                u64::try_from(nanos).unwrap_or(u64::MAX),
            ))
    }
}

/// Compare secrets in time independent of where they differ. Lengths
/// are not secret: a length mismatch answers at once.
pub fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let diff = left
        .iter()
        .zip(right)
        .fold(0u8, |diff, (a, b)| diff | (a ^ b));
    std::hint::black_box(diff) == 0
}

/// One framed TCP request.
#[derive(Serialize, Deserialize)]
enum TcpRequest {
    /// Replicate entries or heartbeat.
    AppendEntries(AppendEntriesRequest<TypeConfig>),
    /// Stream one snapshot segment.
    InstallSnapshot(InstallSnapshotRequest<TypeConfig>),
    /// Request a vote.
    Vote(VoteRequest<NodeId>),
    /// Probe identity, initialization and contract.
    Info,
    /// Ask a leader for a quorum-confirmed read log ID.
    Fence,
    /// Ask a voter to campaign for a leadership handed over.
    TransferLeader(TransferRequest),
    /// A member request ([`crate::peer`]).
    Member(MemberRequest),
}

/// One framed TCP response. Remote Raft errors arrive as message
/// strings and map to unreachable: the peer is broken or gone, and
/// retrying with backoff is the only sound answer.
#[derive(Serialize, Deserialize)]
enum TcpResponse {
    /// AppendEntries outcome or remote failure.
    AppendEntries(Result<AppendEntriesResponse<NodeId>, String>),
    /// InstallSnapshot outcome or remote failure.
    InstallSnapshot(Result<InstallSnapshotResponse<NodeId>, String>),
    /// Vote outcome or remote failure.
    Vote(Result<VoteResponse<NodeId>, String>),
    /// The member's identity, initialization and contract.
    Info(Result<PeerInfo, String>),
    /// The leader's read log ID, or why it gave none.
    Fence(Result<Option<LogId<NodeId>>, String>),
    /// Whether the voter campaigns, or why not.
    TransferLeader(Result<(), String>),
    /// A member request's answer.
    Member(Result<MemberResponse, MemberError>),
    /// The listener had no room for the request: it was read and
    /// discarded, not processed.
    Busy,
}

/// Idle connections to one peer with when each went idle, newest last.
type Idle = Vec<(Stream, Instant)>;

/// Idle authenticated connections by peer and address.
#[derive(Default)]
struct Pool {
    idle: Mutex<HashMap<(NodeId, String), Idle>>,
}

impl Pool {
    /// An idle connection to `target` at `addr` younger than `max_idle`
    /// that its peer has not closed: a request sent on a closed one is
    /// answered by nothing, and a request that must not run twice then
    /// fails with its outcome unknown rather than going on a fresh dial.
    fn take(&self, target: NodeId, addr: &str, max_idle: Duration) -> Option<Stream> {
        let mut idle = self.idle.lock().expect("pool poisoned");
        let connections = idle.get_mut(&(target, addr.to_string()))?;
        while let Some((stream, since)) = connections.pop() {
            if since.elapsed() < max_idle && !stream.closed() {
                return Some(stream);
            }
        }
        None
    }

    /// Keep an idle connection, dropping the oldest past the cap.
    fn put(&self, target: NodeId, addr: &str, stream: Stream) {
        let mut idle = self.idle.lock().expect("pool poisoned");
        let connections = idle.entry((target, addr.to_string())).or_default();
        connections.push((stream, Instant::now()));
        if connections.len() > POOL_PER_PEER {
            connections.remove(0);
        }
    }
}

/// Dials, authenticates and pools connections for one member of one
/// group.
#[derive(Clone)]
struct Dialer {
    own: NodeId,
    group: GroupId,
    config: Arc<PeerConfig>,
    pool: Arc<Pool>,
}

/// Why an exchange failed.
#[derive(Debug)]
enum Failure {
    /// Dialing or the hello failed: back off.
    Connect(String),
    /// The exchange broke mid-way or timed out: retry.
    Exchange(String),
    /// The request exceeds the frame cap.
    TooLarge(u64),
}

/// Why sending on a connection failed.
enum SendError {
    /// No byte of a response arrived: a pooled connection the listener
    /// had already closed fails this way.
    Unanswered(std::io::Error),
    /// The response broke off or did not decode.
    Broken(String),
    /// The listener closed the connection to make room before reading
    /// the request ([`EVICTED`]): it never ran.
    Evicted,
}

impl Dialer {
    /// Dial `target` at `addr`, present the hello and check the answer.
    async fn dial(&self, target: NodeId, addr: &str) -> Result<Stream, Failure> {
        let tcp = TcpStream::connect(addr)
            .await
            .map_err(|error| Failure::Connect(format!("dial {addr}: {error}")))?;
        let _ = tcp.set_nodelay(true);
        let mut socket = match &self.config.tls {
            Some(tls) => tls.connect(addr, tcp).await.map_err(|error| {
                tracing::warn!(addr, %error, "peer TLS handshake failed");
                Failure::Connect(format!("TLS with {addr}: {error}"))
            })?,
            None => Stream::Plain(tcp),
        };
        let token = self.config.token.as_deref().unwrap_or("").as_bytes();
        if token.len() > MAX_TOKEN {
            return Err(Failure::Connect(
                "peer token is longer than 1024 bytes".to_string(),
            ));
        }
        let hello = encode_hello(
            token,
            self.own,
            target,
            &self.group,
            contract().as_bytes(),
            flower_engine::patch::max_transaction_bytes() as u64,
        );
        let hello_error =
            |error: std::io::Error| Failure::Connect(format!("hello {addr}: {error}"));
        socket.write_all(&hello).await.map_err(hello_error)?;
        socket.flush().await.map_err(hello_error)?;
        let status = socket.read_u8().await.map_err(hello_error)?;
        if status == REJECTED {
            tracing::warn!(addr, "peer rejected this node's peer token");
            return Err(Failure::Connect(format!("{addr} rejected the peer token")));
        }
        let (id, theirs) = read_identity(&mut socket).await.map_err(hello_error)?;
        match status {
            ACCEPTED if id == target && theirs == contract() => Ok(socket),
            ACCEPTED | WRONG_NODE | WRONG_GROUP if id != target => {
                tracing::warn!(addr, target, id, "peer address answers as another node");
                Err(Failure::Connect(format!(
                    "{addr} is node {id}, not node {target}"
                )))
            }
            WRONG_GROUP => {
                tracing::debug!(addr, target, group = %self.group, "peer does not host the group");
                Err(Failure::Connect(format!(
                    "node {target} at {addr} hosts no group {}",
                    self.group
                )))
            }
            FRAME_TOO_SMALL => {
                let needed = flower_engine::patch::frame_for_entry(
                    flower_engine::patch::max_transaction_bytes() as u64,
                );
                tracing::error!(
                    addr,
                    target,
                    needed,
                    "peer frames cannot carry this node's largest entry; raise its --raft-max-frame-bytes or lower --max-transaction-bytes here"
                );
                Err(Failure::Connect(format!(
                    "node {target} at {addr} takes frames too small for this node's entries ({needed} bytes needed)"
                )))
            }
            _ => {
                tracing::warn!(addr, target, theirs, ours = contract(), "incompatible peer");
                Err(Failure::Connect(format!(
                    "node {target} at {addr} runs contract {theirs}, this node {}",
                    contract()
                )))
            }
        }
    }

    /// Send one encoded request on a connection (the given one, a pooled
    /// one or a fresh dial) and read its response. A pooled connection
    /// that answers nothing is retried once on a fresh dial: the
    /// listener closes idle connections. Returns the connection with the
    /// response, ready for the next exchange.
    async fn round_trip(
        &self,
        target: NodeId,
        addr: &str,
        connection: Option<Stream>,
        body: &[u8],
    ) -> Result<(TcpResponse, Stream), Failure> {
        let pooled =
            connection.or_else(|| self.pool.take(target, addr, self.config.idle_timeout / 2));
        let reused = pooled.is_some();
        let mut stream = match pooled {
            Some(stream) => stream,
            None => self.dial(target, addr).await?,
        };
        match send(&mut stream, body, self.config.max_frame).await {
            Ok(response) => return Ok((response, stream)),
            Err(SendError::Unanswered(_) | SendError::Evicted) if reused => {}
            Err(error) => return Err(exchange_failure(error)),
        }
        let mut stream = self.dial(target, addr).await?;
        match send(&mut stream, body, self.config.max_frame).await {
            Ok(response) => Ok((response, stream)),
            Err(error) => Err(exchange_failure(error)),
        }
    }

    /// A member request on a pooled connection or a fresh dial. A pooled
    /// connection that answers nothing (the listener closes idle ones)
    /// is retried on a fresh dial only for a request that may run twice:
    /// a forwarded client request may have arrived, so it is not. A dial
    /// still under way when `abandon` resolves is given up: nothing was
    /// sent yet.
    async fn member(
        &self,
        target: NodeId,
        addr: &str,
        request: MemberRequest,
        timeout: Duration,
        abandon: impl Future<Output = ()>,
    ) -> Result<MemberResponse, MemberError> {
        let once = matches!(request, MemberRequest::Forward(_));
        let body =
            encode(&TcpRequest::Member(request), self.config.max_frame).map_err(|failure| {
                match failure {
                    Failure::TooLarge(len) => MemberError::Failed {
                        code: "too_large".to_string(),
                        message: format!("a {len}-byte member request exceeds the frame cap"),
                    },
                    other => MemberError::Unreachable(format!("{other:?}")),
                }
            })?;
        let connect = |failure: Failure| match failure {
            Failure::Connect(message) => MemberError::Unreachable(message),
            other => MemberError::Broken(format!("{other:?}")),
        };
        let unknown = |error: SendError| match error {
            SendError::Unanswered(error) => MemberError::Broken(error.to_string()),
            SendError::Broken(error) => MemberError::Broken(error),
            SendError::Evicted => MemberError::Unreachable(format!(
                "node {target} closed the connection before reading the request"
            )),
        };
        // Sent again on a fresh dial: a request the listener closed the
        // connection without reading, which never ran, and one a pooled
        // connection answered nothing to if it may run twice. A forwarded
        // client request may not: nothing but the listener's notice says
        // it did not run (a process that died after running it answers
        // nothing either).
        let again = |error: &SendError, reused: bool| match error {
            SendError::Evicted => true,
            SendError::Unanswered(_) => reused && !once,
            SendError::Broken(_) => false,
        };
        let exchange = async {
            let pooled = self.pool.take(target, addr, self.config.idle_timeout / 2);
            let reused = pooled.is_some();
            let mut stream = match pooled {
                Some(stream) => stream,
                None => tokio::select! {
                    dialed = self.dial(target, addr) => dialed.map_err(connect)?,
                    () = abandon => {
                        return Err(MemberError::Unreachable(format!(
                            "dialing node {target} was given up"
                        )));
                    }
                },
            };
            match send(&mut stream, &body, self.config.max_frame).await {
                Ok(response) => Ok((response, stream)),
                Err(error) if again(&error, reused) => {
                    let mut stream = self.dial(target, addr).await.map_err(connect)?;
                    let response = send(&mut stream, &body, self.config.max_frame)
                        .await
                        .map_err(unknown)?;
                    Ok((response, stream))
                }
                Err(error) => Err(unknown(error)),
            }
        };
        match tokio::time::timeout(timeout, exchange).await {
            Ok(Ok((response, stream))) => {
                self.pool.put(target, addr, stream);
                match response {
                    TcpResponse::Member(outcome) => outcome,
                    // Read and discarded, not processed.
                    TcpResponse::Busy => Err(MemberError::Unreachable(format!(
                        "node {target} had no room for the request"
                    ))),
                    _ => Err(MemberError::Broken("wrong response kind".to_string())),
                }
            }
            Ok(Err(error)) => Err(error),
            Err(_) => Err(MemberError::Broken(format!(
                "node {target} gave no answer within {timeout:?}"
            ))),
        }
    }

    /// One of Flower's own requests on a pooled connection.
    async fn request(
        &self,
        target: NodeId,
        addr: &str,
        request: &TcpRequest,
        timeout: Duration,
    ) -> Result<TcpResponse, String> {
        let body =
            encode(request, self.config.max_frame).map_err(|failure| format!("{failure:?}"))?;
        match tokio::time::timeout(timeout, self.round_trip(target, addr, None, &body)).await {
            Ok(Ok((response, stream))) => {
                self.pool.put(target, addr, stream);
                Ok(response)
            }
            Ok(Err(failure)) => Err(match failure {
                Failure::Connect(message) | Failure::Exchange(message) => message,
                Failure::TooLarge(len) => format!("{len}-byte request exceeds the frame cap"),
            }),
            Err(_) => Err(format!("node {target} gave no answer within {timeout:?}")),
        }
    }
}

/// The hello a dialer presents.
fn encode_hello(
    token: &[u8],
    source: NodeId,
    target: NodeId,
    group: &GroupId,
    contract: &[u8],
    max_entry: u64,
) -> Vec<u8> {
    let mut hello = Vec::with_capacity(44 + token.len() + group.name.len() + contract.len());
    hello.extend_from_slice(MAGIC);
    hello.push(VERSION);
    hello.extend_from_slice(&(token.len() as u16).to_be_bytes());
    hello.extend_from_slice(token);
    hello.extend_from_slice(&source.to_be_bytes());
    hello.extend_from_slice(&target.to_be_bytes());
    // Names are at most 63 bytes (`crate::group::validate_name`).
    hello.push(group.name.len() as u8);
    hello.extend_from_slice(group.name.as_bytes());
    hello.extend_from_slice(&group.uid.to_be_bytes());
    hello.extend_from_slice(&(contract.len() as u16).to_be_bytes());
    hello.extend_from_slice(contract);
    hello.extend_from_slice(&max_entry.to_be_bytes());
    hello
}

/// A node ID and contract, as acknowledgements carry them.
fn encode_identity(id: NodeId, contract: &str) -> Vec<u8> {
    let mut identity = Vec::with_capacity(10 + contract.len());
    identity.extend_from_slice(&id.to_be_bytes());
    identity.extend_from_slice(&(contract.len() as u16).to_be_bytes());
    identity.extend_from_slice(contract.as_bytes());
    identity
}

/// Read a node ID and contract, bounding the contract first.
async fn read_identity<R: AsyncRead + Unpin>(reader: &mut R) -> std::io::Result<(NodeId, String)> {
    let id = reader.read_u64().await?;
    let len = reader.read_u16().await? as usize;
    if len > MAX_CONTRACT {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "compatibility contract is too long",
        ));
    }
    let mut contract = vec![0u8; len];
    reader.read_exact(&mut contract).await?;
    Ok((id, String::from_utf8_lossy(&contract).into_owned()))
}

/// A byte sink that stops at a cap: an oversized request stops
/// encoding there instead of allocating all of itself.
struct Capped {
    bytes: Vec<u8>,
    cap: u64,
    exceeded: bool,
}

impl std::io::Write for Capped {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if (self.bytes.len() + bytes.len()) as u64 > self.cap {
            self.exceeded = true;
            return Err(std::io::Error::other("frame cap exceeded"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Encode a request within the frame cap.
fn encode(request: &TcpRequest, cap: u64) -> Result<Vec<u8>, Failure> {
    let mut body = Capped {
        bytes: Vec::new(),
        cap,
        exceeded: false,
    };
    match serde_json::to_writer(&mut body, request) {
        Ok(()) => {
            body.flush()
                .map_err(|error| Failure::Exchange(error.to_string()))?;
            Ok(body.bytes)
        }
        Err(_) if body.exceeded => Err(Failure::TooLarge(cap.saturating_add(1))),
        Err(error) => Err(Failure::Exchange(error.to_string())),
    }
}

/// Write one request frame and read its response frame.
async fn send<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    body: &[u8],
    max: u64,
) -> Result<TcpResponse, SendError> {
    write_frame(stream, body)
        .await
        .map_err(SendError::Unanswered)?;
    let len = stream.read_u64().await.map_err(SendError::Unanswered)?;
    if len == EVICTED {
        return Err(SendError::Evicted);
    }
    let bytes = read_body(stream, len, max)
        .await
        .map_err(|error| SendError::Broken(error.to_string()))?;
    serde_json::from_slice(&bytes).map_err(|error| SendError::Broken(error.to_string()))
}

/// Factory for TCP connections to member addresses.
#[derive(Clone)]
pub struct TcpFactory {
    dialer: Dialer,
    withhold: Arc<Withhold>,
}

impl TcpFactory {
    /// Dial the root group's members from this node ID with these
    /// transport settings.
    pub fn new(own: NodeId, config: PeerConfig) -> Self {
        Self::for_group(own, config, GroupId::root())
    }

    /// Dial `group`'s members from this node ID: every connection's hello
    /// names the group, and its pool is the group's own.
    pub fn for_group(own: NodeId, config: PeerConfig, group: GroupId) -> Self {
        TcpFactory {
            dialer: Dialer {
                own,
                group,
                config: Arc::new(config),
                pool: Arc::default(),
            },
            withhold: Arc::default(),
        }
    }
}

impl RaftNetworkFactory<TypeConfig> for TcpFactory {
    type Network = TcpNetwork;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> TcpNetwork {
        TcpNetwork {
            dialer: self.dialer.clone(),
            target,
            addr: node.addr.clone(),
            withhold: self.withhold.clone(),
            connection: None,
            pending: None,
            refusals: Refusals::default(),
            observer: crate::telemetry::observer(&self.dialer.group.name),
        }
    }
}

impl PeerFactory for TcpFactory {
    fn peers(&self) -> Peers {
        Peers {
            route: Route::Tcp(self.dialer.clone()),
            withhold: self.withhold.clone(),
            members: Arc::new(tokio::sync::Semaphore::new(MEMBER_REQUESTS)),
        }
    }

    fn group(&self) -> GroupId {
        self.dialer.group.clone()
    }
}

/// One Raft client's connection to a member address.
pub struct TcpNetwork {
    dialer: Dialer,
    target: NodeId,
    addr: String,
    withhold: Arc<Withhold>,
    /// An idle authenticated connection. An exchange takes it out and
    /// puts it back only once its whole response arrived, so an
    /// exchange OpenRaft abandons mid-way closes its connection instead
    /// of leaving a response behind for the next request.
    connection: Option<Stream>,
    /// The data append in flight, resumed by OpenRaft's retry of it.
    pending: Option<PendingAppend>,
    refusals: Refusals,
    /// The group's instruments.
    observer: Arc<Observer>,
}

impl Drop for TcpNetwork {
    fn drop(&mut self) {
        if let Some(stream) = self.connection.take() {
            self.dialer.pool.put(self.target, &self.addr, stream);
        }
    }
}

/// What identifies a data append's entries: under one leader vote, the
/// same previous log ID and the same first and last log IDs name the
/// same entries (Raft's log matching).
#[derive(Clone, Debug, PartialEq, Eq)]
struct AppendKey {
    vote: Vote<NodeId>,
    prev: Option<LogId<NodeId>>,
    first: Option<LogId<NodeId>>,
    last: Option<LogId<NodeId>>,
    len: usize,
}

impl AppendKey {
    fn of(rpc: &AppendEntriesRequest<TypeConfig>) -> Self {
        AppendKey {
            vote: rpc.vote,
            prev: rpc.prev_log_id,
            first: rpc.entries.first().map(|entry| entry.log_id),
            last: rpc.entries.last().map(|entry| entry.log_id),
            len: rpc.entries.len(),
        }
    }
}

/// A data append running in its own task, so that OpenRaft dropping
/// the call at its heartbeat-period deadline doesn't restart it.
struct PendingAppend {
    key: AppendKey,
    /// The highest commit index a caller sent with these entries: a
    /// regressed one starts a new append.
    commit: Option<LogId<NodeId>>,
    entries: usize,
    task: tokio::task::JoinHandle<Result<(TcpResponse, Stream), Failure>>,
}

impl Drop for PendingAppend {
    fn drop(&mut self) {
        // Changed entries or a closed replication stream: stop sending.
        // The follower applies what it already received under Raft's
        // ordinary checks, as for any abandoned RPC.
        self.task.abort();
    }
}

/// Data appends a follower refused in a row for want of room, and when
/// the next may go. The first refusal of a batch resends at once with
/// one entry; a follower that refuses one entry too, or keeps refusing,
/// gets time for its room to free up. Heartbeats never wait for this.
#[derive(Debug, Default)]
struct Refusals {
    count: u32,
    retry_at: Option<Instant>,
}

impl Refusals {
    /// A refusal of an append of `entries` entries whose caller waits at
    /// most `deadline`: the wait before the next stays well inside it.
    fn refused(&mut self, entries: usize, deadline: Duration) {
        self.count = self.count.saturating_add(1);
        if self.count == 1 && entries > 1 {
            self.retry_at = None;
            return;
        }
        let doublings = (self.count - 1).min(10);
        let wait = REFUSAL_BACKOFF
            .saturating_mul(1 << doublings)
            .min(deadline / 2);
        self.retry_at = Some(Instant::now() + wait);
    }
}

/// An exchange that failed after its connection was had: retried
/// promptly as a network error.
fn exchange_failure(error: SendError) -> Failure {
    match error {
        SendError::Unanswered(error) => Failure::Exchange(error.to_string()),
        SendError::Broken(error) => Failure::Exchange(error),
        SendError::Evicted => {
            Failure::Exchange("the peer closed the connection to make room".to_string())
        }
    }
}

/// Map any transport failure to unreachable with backoff.
fn unreachable<E: std::error::Error>(message: String) -> RPCError<NodeId, BasicNode, E> {
    let io = std::io::Error::other(message);
    RPCError::Unreachable(Unreachable::new(&io))
}

/// Map a transient failure to a promptly retried network error.
fn network<E: std::error::Error>(message: String) -> RPCError<NodeId, BasicNode, E> {
    let io = std::io::Error::other(message);
    RPCError::Network(NetworkError::new(&io))
}

/// Read a frame body of `len` bytes, growing the buffer as bytes
/// arrive: a claimed length never allocates by itself.
async fn read_body<R: AsyncRead + Unpin>(
    reader: &mut R,
    len: u64,
    max: u64,
) -> std::io::Result<Vec<u8>> {
    if len > max {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("frame of {len} bytes exceeds the {max}-byte cap"),
        ));
    }
    let mut body = Vec::with_capacity(usize::try_from(len).unwrap_or(usize::MAX).min(READ_CHUNK));
    let read = (&mut *reader).take(len).read_to_end(&mut body).await?;
    if read as u64 != len {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "connection closed mid-frame",
        ));
    }
    Ok(body)
}

/// Write one length-prefixed frame.
async fn write_frame<W: AsyncWrite + Unpin>(writer: &mut W, body: &[u8]) -> std::io::Result<()> {
    writer.write_all(&(body.len() as u64).to_be_bytes()).await?;
    writer.write_all(body).await?;
    writer.flush().await
}

impl TcpNetwork {
    /// Send one request and read its response within `ttl`, on this
    /// client's connection, a pooled one or a fresh dial.
    async fn exchange(
        &mut self,
        request: &TcpRequest,
        ttl: Duration,
    ) -> Result<TcpResponse, Failure> {
        let body = encode(request, self.dialer.config.max_frame)?;
        let connection = self.connection.take();
        let round_trip = self
            .dialer
            .round_trip(self.target, &self.addr, connection, &body);
        match tokio::time::timeout(ttl, round_trip).await {
            Ok(Ok((response, stream))) => {
                self.keep(stream);
                Ok(response)
            }
            Ok(Err(failure)) => Err(failure),
            Err(_) => Err(Failure::Exchange(format!("no answer within {ttl:?}"))),
        }
    }

    /// Keep a connection that just completed an exchange.
    fn keep(&mut self, stream: Stream) {
        if self.connection.is_none() {
            self.connection = Some(stream);
        } else {
            self.dialer.pool.put(self.target, &self.addr, stream);
        }
    }

    /// Describe an RPC for error messages.
    fn label(&self, rpc: &str) -> String {
        format!("{rpc} {}->{}", self.dialer.own, self.target)
    }

    /// Refuse replication held back for a leadership transfer.
    fn check_withheld<E: std::error::Error>(
        &self,
        rpc: &str,
    ) -> Result<(), RPCError<NodeId, BasicNode, E>> {
        if self.withhold.withholds(self.target) {
            return Err(unreachable(format!(
                "{}: held back while leadership moves",
                self.label(rpc)
            )));
        }
        Ok(())
    }

    /// Send a data append in its own task, or resume waiting for the one
    /// already sending these entries.
    async fn append_data(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let key = AppendKey::of(&rpc);
        let commit = rpc.leader_commit;
        let resumable = self
            .pending
            .as_ref()
            .is_some_and(|pending| pending.key == key && pending.commit <= commit);
        if resumable {
            if let Some(pending) = &mut self.pending {
                pending.commit = commit;
            }
        } else {
            self.pending = None;
            // After refusals, give the follower's room time to free up.
            // A caller dropped meanwhile leaves the time in place.
            if let Some(retry_at) = self.refusals.retry_at {
                tokio::time::sleep_until(retry_at).await;
                self.refusals.retry_at = None;
            }
            let entries = rpc.entries.len();
            let body = match encode(
                &TcpRequest::AppendEntries(rpc),
                self.dialer.config.max_frame,
            ) {
                Ok(body) => body,
                // OpenRaft resends a smaller batch; a single entry over
                // the cap cannot shrink and needs a larger
                // --raft-max-frame-bytes.
                Err(Failure::TooLarge(_)) if entries > 1 => {
                    tracing::debug!(entries, "append batch over the frame cap; halving");
                    return Err(RPCError::PayloadTooLarge(
                        PayloadTooLarge::new_entries_hint((entries as u64 / 2).max(1)),
                    ));
                }
                Err(failure) => {
                    if matches!(failure, Failure::TooLarge(_)) {
                        tracing::error!(
                            target = self.target,
                            "a single log entry exceeds the peer frame cap; raise --raft-max-frame-bytes"
                        );
                    }
                    return Err(rpc_error(self.label("append_entries"), failure));
                }
            };
            let allowance = self
                .dialer
                .config
                .append_allowance(option.hard_ttl(), body.len());
            let (dialer, target, addr) = (self.dialer.clone(), self.target, self.addr.clone());
            let connection = self.connection.take();
            let task = tokio::spawn(async move {
                let round_trip = dialer.round_trip(target, &addr, connection, &body);
                match tokio::time::timeout(allowance, round_trip).await {
                    Ok(outcome) => outcome,
                    Err(_) => Err(Failure::Exchange(format!("no answer within {allowance:?}"))),
                }
            });
            self.pending = Some(PendingAppend {
                key,
                commit,
                entries,
                task,
            });
        }
        // Await by reference: if OpenRaft drops this call, the task and
        // its connection stay, and a retry of these entries resumes here.
        let pending = self.pending.as_mut().expect("pending append");
        let joined = (&mut pending.task).await;
        let entries = pending.entries;
        self.pending = None;
        let outcome = match joined {
            Ok(outcome) => outcome,
            Err(error) => Err(Failure::Exchange(error.to_string())),
        };
        match outcome {
            Ok((response, stream)) => {
                self.keep(stream);
                match response {
                    TcpResponse::AppendEntries(result) => {
                        self.refusals = Refusals::default();
                        result.map_err(unreachable)
                    }
                    TcpResponse::Busy => {
                        tracing::debug!(
                            target = self.target,
                            entries,
                            "follower out of replication room; resending fewer entries"
                        );
                        self.refusals.refused(entries, option.hard_ttl());
                        Err(RPCError::PayloadTooLarge(
                            PayloadTooLarge::new_entries_hint(1),
                        ))
                    }
                    _ => Err(unreachable("wrong response kind".to_string())),
                }
            }
            Err(failure) => Err(rpc_error(self.label("append_entries"), failure)),
        }
    }
}

/// Map an exchange failure into the RPC error OpenRaft expects.
fn rpc_error<E: std::error::Error>(
    label: String,
    failure: Failure,
) -> RPCError<NodeId, BasicNode, E> {
    match failure {
        Failure::Connect(message) => unreachable(format!("{label}: {message}")),
        Failure::Exchange(message) => network(format!("{label}: {message}")),
        Failure::TooLarge(len) => {
            unreachable(format!("{label}: {len}-byte request exceeds the frame cap"))
        }
    }
}

impl RaftNetwork<TypeConfig> for TcpNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let observer = self.observer.clone();
        let operation = append_kind(&rpc);
        let (target, timeout) = (self.target, option.hard_ttl());
        observer
            .raft_rpc(
                operation,
                target,
                Some(timeout),
                self.send_append(rpc, option),
            )
            .await
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        let observer = self.observer.clone();
        let target = self.target;
        observer
            .raft_rpc(Rpc::Snapshot, target, None, self.send_snapshot(rpc, option))
            .await
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let observer = self.observer.clone();
        let target = self.target;
        observer
            .raft_rpc(Rpc::Vote, target, None, self.send_vote(rpc, option))
            .await
    }

    /// After a dial or hello failed: retry within a heartbeat, then
    /// doubling (see [`reconnect_backoff`]).
    fn backoff(&self) -> openraft::network::Backoff {
        openraft::network::Backoff::new(reconnect_backoff())
    }
}

/// The waits before a replication stream dials a peer again after
/// failing to: 25 ms, doubling to 500 ms. OpenRaft's own is 500 ms from
/// the first failure, and a leader that sends a follower nothing for
/// longer than its lease lets it ask for a pre-vote; under CPU or disk
/// pressure a dial, or the hello after it, can miss one RPC deadline
/// without the peer being gone (each one OpenRaft abandons closes its
/// connection, so the next RPC dials). A peer that is gone is still
/// dialed at most every 500 ms once the waits have grown.
fn reconnect_backoff() -> impl Iterator<Item = Duration> + Send + 'static {
    std::iter::successors(Some(RECONNECT_BACKOFF), |wait| {
        Some((*wait * 2).min(RECONNECT_BACKOFF_MAX))
    })
}

impl TcpNetwork {
    async fn send_append(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        self.check_withheld("append_entries")?;
        if !rpc.entries.is_empty() {
            return self.append_data(rpc, option).await;
        }
        // A heartbeat of a newer vote ends any append of an older one.
        if self
            .pending
            .as_ref()
            .is_some_and(|pending| pending.key.vote != rpc.vote)
        {
            self.pending = None;
        }
        match self
            .exchange(&TcpRequest::AppendEntries(rpc), option.hard_ttl())
            .await
        {
            Ok(TcpResponse::AppendEntries(result)) => result.map_err(unreachable),
            Ok(_) => Err(unreachable("wrong response kind".to_string())),
            Err(failure) => Err(rpc_error(self.label("append_entries"), failure)),
        }
    }

    async fn send_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        self.check_withheld("install_snapshot")?;
        match self
            .exchange(&TcpRequest::InstallSnapshot(rpc), option.hard_ttl())
            .await
        {
            Ok(TcpResponse::InstallSnapshot(result)) => result.map_err(unreachable),
            Ok(TcpResponse::Busy) => Err(network(format!(
                "{}: follower out of replication room",
                self.label("install_snapshot")
            ))),
            Ok(_) => Err(unreachable("wrong response kind".to_string())),
            Err(failure) => Err(rpc_error(self.label("install_snapshot"), failure)),
        }
    }

    async fn send_vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        match self
            .exchange(&TcpRequest::Vote(rpc), option.hard_ttl())
            .await
        {
            Ok(TcpResponse::Vote(result)) => result.map_err(unreachable),
            Ok(_) => Err(unreachable("wrong response kind".to_string())),
            Err(failure) => Err(rpc_error(self.label("vote"), failure)),
        }
    }
}

/// The listener's replication budget: bytes of large inbound frames
/// held at once.
struct Room {
    limit: u64,
    used: Mutex<u64>,
    refused: AtomicU64,
}

/// Room one frame holds until its response is written.
struct Held<'a> {
    room: &'a Room,
    bytes: u64,
}

impl Room {
    fn new(limit: u64) -> Self {
        Room {
            limit,
            used: Mutex::new(0),
            refused: AtomicU64::new(0),
        }
    }

    /// Room for a frame of `bytes`, if there is some. Small frames need
    /// none.
    fn take(&self, bytes: u64) -> Option<Held<'_>> {
        if bytes <= SMALL_FRAME {
            return Some(Held {
                room: self,
                bytes: 0,
            });
        }
        let mut used = self.used.lock().expect("room poisoned");
        if used.saturating_add(bytes) > self.limit {
            self.refused.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        *used += bytes;
        Some(Held { room: self, bytes })
    }
}

impl Drop for Held<'_> {
    fn drop(&mut self) {
        if self.bytes > 0 {
            *self.room.used.lock().expect("room poisoned") -= self.bytes;
        }
    }
}

/// Why the listener answered a forwarded request it had no room for.
const FORWARDS_EXHAUSTED: &str =
    "this member serves as many forwarded requests as it may; retry later";

/// The listener's connections: at most [`PeerConfig::max_connections`]
/// still saying hello, and as many past it.
///
/// A dial first takes a place among those saying hello (TLS, then the
/// token and identities), which it holds until its hello is checked or
/// the handshake deadline passes; with every such place taken it is
/// closed at once, and nothing else is touched: a dialer without the
/// token can delay others' new dials, never close their connections.
/// A connection whose hello passed then takes a place under the cap of
/// established connections. One waiting for its next request is idle;
/// dialers pool idle connections, every group's to every peer, so idle
/// ones alone can fill the cap. An authenticated connection that finds
/// the cap reached therefore closes the connection idle longest and
/// takes its place: the dialer that pooled it dials again at the cost of
/// a hello, while a refused dial would fail a heartbeat, a vote or a
/// forwarded write. Only a cap of connections all serving requests
/// refuses one. An evicted connection is told so before it closes
/// ([`EVICTED`]), so a request already on its way is known never to have
/// been read. Forwarded client requests are served at most half the cap
/// at once ([`Connections::forwards`]): beyond it they are answered as
/// the leader's admission answers an overload, so a flood of them can
/// never hold every connection while Raft's own RPCs wait for one.
struct Connections {
    cap: usize,
    /// Forwarded requests being served.
    forwards: tokio::sync::Semaphore,
    open: Mutex<Open>,
}

#[derive(Default)]
struct Open {
    next: u64,
    /// Connections saying hello.
    greeting: usize,
    /// Every established connection: since when it is idle (`None` while
    /// it serves a request), and how to close it.
    slots: HashMap<u64, (Option<Instant>, Arc<tokio::sync::Notify>)>,
}

/// A place among the connections saying hello; dropping it frees it.
struct Greeting {
    connections: Arc<Connections>,
}

impl Drop for Greeting {
    fn drop(&mut self) {
        self.connections
            .open
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .greeting -= 1;
    }
}

/// One open connection's place under the cap; dropping it frees the
/// place unless an eviction already did.
struct Slot {
    connections: Arc<Connections>,
    id: u64,
    close: Arc<tokio::sync::Notify>,
}

impl Connections {
    fn new(cap: usize) -> Self {
        let cap = cap.max(1);
        Connections {
            cap,
            forwards: tokio::sync::Semaphore::new(cap.div_ceil(2)),
            open: Mutex::default(),
        }
    }

    /// A place for a dial to say hello in; `None` when every one is
    /// taken. It closes nothing.
    fn greet(self: &Arc<Self>) -> Option<Greeting> {
        let mut open = self.open.lock().unwrap_or_else(PoisonError::into_inner);
        if open.greeting >= self.cap {
            return None;
        }
        open.greeting += 1;
        Some(Greeting {
            connections: self.clone(),
        })
    }

    /// A place for a connection whose hello passed, closing the
    /// longest-idle one at the cap; `None` when every connection is busy.
    fn admit(self: &Arc<Self>) -> Option<Slot> {
        let mut open = self.open.lock().unwrap_or_else(PoisonError::into_inner);
        if open.slots.len() >= self.cap {
            let (_, oldest) = open
                .slots
                .iter()
                .filter_map(|(id, (idle, _))| idle.map(|since| (since, *id)))
                .min()?;
            if let Some((_, close)) = open.slots.remove(&oldest) {
                close.notify_one();
            }
        }
        let id = open.next;
        open.next += 1;
        let close = Arc::new(tokio::sync::Notify::new());
        open.slots.insert(id, (None, close.clone()));
        Some(Slot {
            connections: self.clone(),
            id,
            close,
        })
    }
}

impl Slot {
    /// Mark the connection idle (`true`) or serving (`false`); `false`
    /// once it was evicted, when it should close.
    fn set_idle(&self, idle: bool) -> bool {
        let mut open = self
            .connections
            .open
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        match open.slots.get_mut(&self.id) {
            Some((since, _)) => {
                *since = idle.then(Instant::now);
                true
            }
            None => false,
        }
    }

    /// Resolves once the connection was evicted.
    async fn evicted(&self) {
        self.close.notified().await;
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        self.connections
            .open
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .slots
            .remove(&self.id);
    }
}

/// What the leader's API answers a request admission refuses, for a
/// forwarded request this listener had no room to serve.
fn forward_overloaded() -> ForwardedResponse {
    let retry = crate::admission::RETRY_AFTER;
    ForwardedResponse {
        status: 503,
        headers: vec![
            ("content-type".to_string(), "application/json".to_string()),
            (
                "retry-after".to_string(),
                retry.as_secs().max(1).to_string(),
            ),
        ],
        body: serde_json::json!({
            "ok": false,
            "code": "overloaded",
            "message": FORWARDS_EXHAUSTED,
            "retry_after_ms": retry.as_millis() as u64,
        })
        .to_string(),
    }
}

/// Serve Raft RPCs on a bound listener until the task is dropped:
/// dropping it closes every connection it spawned. Accept errors
/// (descriptor exhaustion, aborted handshakes) pause briefly instead of
/// ending the listener. Votes route straight to `raft`: a member serves
/// its [`crate::node::Node::peer_service`] through [`serve_peer`].
pub async fn serve(listener: TcpListener, raft: Raft<TypeConfig>, config: PeerConfig) {
    let id = raft.metrics().borrow().id;
    serve_peer(listener, PeerService::bare(id, raft), config).await;
}

/// Serve a member's peer requests on a bound listener, as [`serve`].
pub async fn serve_peer(listener: TcpListener, service: PeerService, config: PeerConfig) {
    let directory = PeerDirectory::new(service.id);
    directory.insert(service);
    serve_directory(listener, directory, config).await;
}

/// The groups one process answers for on its peer listener, by name:
/// groups join as they open and leave as they close, while the listener
/// runs. Every service answers for the same node.
#[derive(Clone)]
pub struct PeerDirectory {
    id: NodeId,
    services: Arc<RwLock<HashMap<Arc<str>, PeerService>>>,
}

impl PeerDirectory {
    /// An empty directory for node `id`.
    pub fn new(id: NodeId) -> Self {
        PeerDirectory {
            id,
            services: Arc::default(),
        }
    }

    /// The node every service answers for.
    pub fn id(&self) -> NodeId {
        self.id
    }

    /// Answer for a group's member. A service for another node is a
    /// programming error.
    pub fn insert(&self, service: PeerService) {
        assert_eq!(service.id, self.id, "a directory serves one node");
        self.services
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(service.group.name.clone(), service);
    }

    /// Stop answering for a group.
    pub fn remove(&self, group: &str) {
        self.services
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(group);
    }

    /// The service answering for `name` under `uid`, if hosted.
    pub fn get(&self, name: &str, uid: u64) -> Option<PeerService> {
        self.services
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(name)
            .filter(|service| service.group.uid == uid)
            .cloned()
    }

    /// The groups answered for, by name.
    pub fn groups(&self) -> Vec<GroupId> {
        self.services
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .map(|service| service.group.clone())
            .collect()
    }
}

/// Serve every group of `directory` on one bound listener: each
/// connection's hello names its group, and its requests go to that
/// group's service, under one connection cap ([`Connections`]) and one
/// follower byte budget for the process.
pub async fn serve_directory(listener: TcpListener, directory: PeerDirectory, config: PeerConfig) {
    if config.token.is_none() {
        tracing::warn!("Raft listener accepts peers without a peer token");
    }
    let room = Arc::new(Room::new(config.queued_bytes()));
    let config = Arc::new(config);
    let connections = Arc::new(Connections::new(config.max_connections));
    let mut tasks = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let socket = match accepted {
                    Ok((socket, _)) => socket,
                    Err(error) => {
                        tracing::warn!(%error, "Raft listener accept failed");
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        continue;
                    }
                };
                let Some(greeting) = connections.greet() else {
                    tracing::warn!("Raft listener has every place for a hello taken; closing a dial");
                    continue;
                };
                let (directory, config, room, connections) =
                    (directory.clone(), config.clone(), room.clone(), connections.clone());
                tasks.spawn(async move {
                    handle(socket, &directory, &config, &room, &connections, greeting).await;
                });
            }
            Some(_) = tasks.join_next() => {}
        }
    }
}

/// A dialer's hello, read whole before any of it is trusted.
struct Hello {
    token: Vec<u8>,
    source: NodeId,
    target: NodeId,
    group: Vec<u8>,
    uid: u64,
    contract: Vec<u8>,
    /// The largest entry the dialer may send.
    max_entry: u64,
}

/// Read a dialer's hello: magic, version, then the token, the group name,
/// the contract (whose lengths are bounded before anything is allocated
/// for them) and the largest entry the dialer may send. `None` for
/// another protocol or version.
async fn read_hello<R: AsyncRead + Unpin>(socket: &mut R) -> std::io::Result<Option<Hello>> {
    let mut head = [0u8; 7];
    socket.read_exact(&mut head).await?;
    if &head[..4] != MAGIC || head[4] != VERSION {
        return Ok(None);
    }
    let len = u16::from_be_bytes([head[5], head[6]]) as usize;
    if len > MAX_TOKEN {
        return Ok(None);
    }
    let mut token = vec![0u8; len];
    socket.read_exact(&mut token).await?;
    let source = socket.read_u64().await?;
    let target = socket.read_u64().await?;
    let len = socket.read_u8().await? as usize;
    if len > MAX_NAME {
        return Ok(None);
    }
    let mut group = vec![0u8; len];
    socket.read_exact(&mut group).await?;
    let uid = socket.read_u64().await?;
    let len = socket.read_u16().await? as usize;
    if len > MAX_CONTRACT {
        return Ok(None);
    }
    let mut contract = vec![0u8; len];
    socket.read_exact(&mut contract).await?;
    let max_entry = socket.read_u64().await?;
    Ok(Some(Hello {
        token,
        source,
        target,
        group,
        uid,
        contract,
        max_entry,
    }))
}

/// Check a hello: the token first, in constant time, then the intended
/// node, the contract and the group. The acknowledgement to answer and,
/// when accepted, the dialer's node ID and the group's service.
fn check_hello(
    hello: Option<Hello>,
    directory: &PeerDirectory,
    config: &PeerConfig,
) -> (Vec<u8>, Option<(NodeId, PeerService)>) {
    let Some(hello) = hello else {
        return (vec![REJECTED], None);
    };
    let authenticated = match &config.token {
        Some(expected) => constant_time_eq(expected.as_bytes(), &hello.token),
        None => true,
    };
    if !authenticated {
        return (vec![REJECTED], None);
    }
    let id = directory.id;
    let service = std::str::from_utf8(&hello.group)
        .ok()
        .and_then(|name| directory.get(name, hello.uid));
    let status = if hello.target != id {
        WRONG_NODE
    } else if hello.contract != contract().as_bytes() {
        INCOMPATIBLE
    } else if service.is_none() {
        WRONG_GROUP
    } else if flower_engine::patch::frame_for_entry(hello.max_entry) > config.max_frame {
        tracing::error!(
            source = hello.source,
            max_entry = hello.max_entry,
            max_frame = config.max_frame,
            "refused a Raft dial: this member's frames cannot carry the dialer's largest entry; raise --raft-max-frame-bytes here or lower --max-transaction-bytes there"
        );
        FRAME_TOO_SMALL
    } else {
        ACCEPTED
    };
    let mut ack = vec![status];
    ack.extend_from_slice(&encode_identity(id, contract()));
    let accepted = (status == ACCEPTED)
        .then_some(service)
        .flatten()
        .map(|service| (hello.source, service));
    (ack, accepted)
}

/// Serve one connection: run the TLS handshake when configured, then
/// authenticate and answer requests in order until the dialer hangs
/// up, idles out or breaks a deadline. A silent or failed handshake
/// closes the connection before any hello is read.
async fn handle(
    socket: TcpStream,
    directory: &PeerDirectory,
    config: &PeerConfig,
    room: &Room,
    connections: &Arc<Connections>,
    greeting: Greeting,
) {
    let _ = socket.set_nodelay(true);
    let mut stream = match &config.tls {
        Some(tls) => {
            let peer = socket.peer_addr().ok();
            match tokio::time::timeout(config.handshake_timeout, tls.accept(socket)).await {
                Ok(Ok(stream)) => stream,
                Ok(Err(error)) => {
                    tracing::warn!(?peer, %error, "refused a Raft dial: TLS handshake failed");
                    return;
                }
                Err(_) => {
                    tracing::warn!(?peer, "refused a Raft dial: no TLS handshake in time");
                    return;
                }
            }
        }
        None => Stream::Plain(socket),
    };
    serve_connection(&mut stream, directory, config, room, connections, greeting).await;
}

/// The body of [`handle`], over any stream.
async fn serve_connection<S: AsyncRead + AsyncWrite + Unpin>(
    socket: &mut S,
    directory: &PeerDirectory,
    config: &PeerConfig,
    room: &Room,
    connections: &Arc<Connections>,
    greeting: Greeting,
) {
    // A dialer that says nothing in time is refused like a wrong one.
    let hello = match tokio::time::timeout(config.handshake_timeout, read_hello(socket)).await {
        Ok(Ok(hello)) => hello,
        _ => None,
    };
    let (ack, accepted) = check_hello(hello, directory, config);
    // An authenticated connection takes its place among the established
    // ones before it is told it was accepted; with every one busy, it is
    // closed unanswered, as a dial at the cap always was.
    let slot = match accepted {
        Some(_) => match connections.admit() {
            Some(slot) => Some(slot),
            None => {
                tracing::warn!(
                    "Raft listener at its connection cap, every connection busy; closing a dial"
                );
                return;
            }
        },
        None => None,
    };
    drop(greeting);
    let acknowledge = async {
        socket.write_all(&ack).await?;
        socket.flush().await
    };
    let _ = tokio::time::timeout(config.io_timeout, acknowledge).await;
    let Some((dialer, service)) = accepted else {
        if ack[0] == WRONG_GROUP {
            // A peer dialing a group this process closed, or has not
            // opened yet: routine while groups come and go.
            tracing::debug!(
                node = directory.id,
                "refused a Raft dial for a group not hosted"
            );
        } else {
            tracing::warn!(
                node = directory.id,
                status = ack[0],
                "refused a Raft dial: wrong token, node or compatibility contract"
            );
        }
        return;
    };
    let slot = slot.expect("an accepted connection holds a place");
    let service = &service;
    tracing::debug!(node = service.id, dialer, group = %service.group, "accepted a Raft peer connection");
    loop {
        // Between requests the connection is idle, and a dial at the cap
        // may close it; one evicted while serving closes once it answered.
        if !slot.set_idle(true) {
            return;
        }
        let len = tokio::select! {
            biased;
            read = tokio::time::timeout(config.idle_timeout, socket.read_u64()) => match read {
                Ok(Ok(len)) => len,
                _ => return,
            },
            () = slot.evicted() => {
                // Say so before closing: a request the dialer sent
                // meanwhile is never read, and it may send it again.
                // Whatever it sent is then read off unread until it hangs
                // up: closing with bytes unread resets the connection,
                // which can take the notice with it.
                let notice = async {
                    socket.write_u64(EVICTED).await?;
                    socket.shutdown().await?;
                    tokio::io::copy(&mut *socket, &mut tokio::io::sink()).await
                };
                let _ = tokio::time::timeout(config.handshake_timeout, notice).await;
                return;
            }
        };
        slot.set_idle(false);
        if len > config.max_frame {
            return;
        }
        let response = match room.take(len) {
            Some(held) => {
                let body = match tokio::time::timeout(
                    config.io_timeout,
                    read_body(socket, len, config.max_frame),
                )
                .await
                {
                    Ok(Ok(body)) => body,
                    _ => return,
                };
                let Ok(request) = serde_json::from_slice::<TcpRequest>(&body) else {
                    return;
                };
                drop(body);
                let response = if matches!(request, TcpRequest::Member(MemberRequest::Forward(_))) {
                    match connections.forwards.try_acquire() {
                        Ok(_serving) => route(service, request).await,
                        Err(_) => {
                            TcpResponse::Member(Ok(MemberResponse::Forward(forward_overloaded())))
                        }
                    }
                } else {
                    route(service, request).await
                };
                let Ok(bytes) = serde_json::to_vec(&response) else {
                    return;
                };
                let written =
                    tokio::time::timeout(config.io_timeout, write_frame(socket, &bytes)).await;
                drop(held);
                match written {
                    Ok(Ok(())) => continue,
                    _ => return,
                }
            }
            None => {
                // No room: read the frame off the wire without keeping
                // it, so the connection stays in step, and say so.
                let discarded = tokio::time::timeout(
                    config.io_timeout,
                    tokio::io::copy(&mut (&mut *socket).take(len), &mut tokio::io::sink()),
                )
                .await;
                if !matches!(discarded, Ok(Ok(read)) if read == len) {
                    return;
                }
                TcpResponse::Busy
            }
        };
        let Ok(bytes) = serde_json::to_vec(&response) else {
            return;
        };
        match tokio::time::timeout(config.io_timeout, write_frame(socket, &bytes)).await {
            Ok(Ok(())) => {}
            _ => return,
        }
    }
}

/// Route one request into the local member.
async fn route(service: &PeerService, request: TcpRequest) -> TcpResponse {
    let text = |error: &dyn std::fmt::Display| error.to_string();
    match request {
        TcpRequest::AppendEntries(rpc) => {
            TcpResponse::AppendEntries(service.append_entries(rpc).await.map_err(|e| text(&e)))
        }
        TcpRequest::InstallSnapshot(rpc) => {
            TcpResponse::InstallSnapshot(service.install_snapshot(rpc).await.map_err(|e| text(&e)))
        }
        TcpRequest::Vote(rpc) => TcpResponse::Vote(service.vote(rpc).await.map_err(|e| text(&e))),
        TcpRequest::Info => TcpResponse::Info(service.info().await),
        TcpRequest::Fence => TcpResponse::Fence(service.fence().await),
        TcpRequest::TransferLeader(request) => {
            TcpResponse::TransferLeader(service.transfer(request).await)
        }
        TcpRequest::Member(request) => TcpResponse::Member(service.member(request).await),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constant_time_eq_compares_whole_secrets() {
        assert!(constant_time_eq(b"secret", b"secret"));
        assert!(!constant_time_eq(b"secret", b"secreT"));
        assert!(!constant_time_eq(b"secret", b"secret!"));
        assert!(constant_time_eq(b"", b""));
    }

    use std::sync::atomic::AtomicUsize;

    use openraft::{CommittedLeaderId, Entry, EntryPayload};

    /// A scripted peer: node `id` speaking the hello, answering each
    /// request after `delay` (the first request only) with whatever
    /// `answer` makes of it. Counts connections and requests.
    struct FakePeer {
        addr: String,
        connections: Arc<AtomicUsize>,
        requests: Arc<AtomicUsize>,
    }

    impl FakePeer {
        async fn start(
            id: NodeId,
            delay: Duration,
            answer: fn(TcpRequest) -> TcpResponse,
        ) -> FakePeer {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap().to_string();
            let (connections, requests) =
                (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
            let (accepted, received) = (connections.clone(), requests.clone());
            tokio::spawn(async move {
                loop {
                    let (mut socket, _) = listener.accept().await.unwrap();
                    accepted.fetch_add(1, Ordering::SeqCst);
                    let received = received.clone();
                    tokio::spawn(async move {
                        let hello = read_hello(&mut socket).await.unwrap().unwrap();
                        assert_eq!(hello.target, id);
                        let mut ack = vec![ACCEPTED];
                        ack.extend_from_slice(&encode_identity(id, contract()));
                        socket.write_all(&ack).await.unwrap();
                        loop {
                            let Ok(len) = socket.read_u64().await else {
                                return;
                            };
                            let body = read_body(&mut socket, len, u64::MAX).await.unwrap();
                            let request: TcpRequest = serde_json::from_slice(&body).unwrap();
                            if received.fetch_add(1, Ordering::SeqCst) == 0 {
                                tokio::time::sleep(delay).await;
                            }
                            let response = serde_json::to_vec(&answer(request)).unwrap();
                            if write_frame(&mut socket, &response).await.is_err() {
                                return;
                            }
                        }
                    });
                }
            });
            FakePeer {
                addr,
                connections,
                requests,
            }
        }
    }

    /// Grant every vote, echoing the candidate's vote: a response names
    /// the request it answers.
    fn echo_votes(request: TcpRequest) -> TcpResponse {
        match request {
            TcpRequest::Vote(vote) => {
                TcpResponse::Vote(Ok(VoteResponse::new(vote.vote, None, true)))
            }
            TcpRequest::AppendEntries(_) => {
                TcpResponse::AppendEntries(Ok(AppendEntriesResponse::Success))
            }
            _ => TcpResponse::Busy,
        }
    }

    fn vote(term: u64) -> VoteRequest<NodeId> {
        VoteRequest::new(Vote::new(term, 1), None)
    }

    async fn client(peer: &FakePeer) -> TcpNetwork {
        let mut factory = TcpFactory::new(1, PeerConfig::default());
        factory.new_client(2, &BasicNode::new(&peer.addr)).await
    }

    /// A forwarded request the listener answers with a 200.
    fn forwarded() -> TcpResponse {
        TcpResponse::Member(Ok(MemberResponse::Forward(ForwardedResponse {
            status: 200,
            headers: Vec::new(),
            body: "{}".to_string(),
        })))
    }

    #[tokio::test]
    async fn a_forward_the_listener_evicted_unread_is_sent_again_and_runs_once() {
        // The listener answers a first forward, then closes the pooled
        // connection to make room just as the second arrives on it,
        // without reading it, as an eviction racing a request does. Told
        // so, the dialer sends it again on a fresh dial, where it runs
        // once. Without the notice it could only report the outcome
        // unknown: an unanswered request may have run.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let ran = Arc::new(AtomicUsize::new(0));
        let served = ran.clone();
        tokio::spawn(async move {
            for connection in 0..2 {
                let (mut socket, _) = listener.accept().await.unwrap();
                let served = served.clone();
                tokio::spawn(async move {
                    read_hello(&mut socket).await.unwrap().unwrap();
                    let mut ack = vec![ACCEPTED];
                    ack.extend_from_slice(&encode_identity(2, contract()));
                    socket.write_all(&ack).await.unwrap();
                    for request in 0.. {
                        if connection == 0 && request == 1 {
                            // The second request has arrived, unread.
                            socket.peek(&mut [0u8; 1]).await.unwrap();
                            socket.write_u64(EVICTED).await.unwrap();
                            socket.shutdown().await.unwrap();
                            let _ = tokio::io::copy(&mut socket, &mut tokio::io::sink()).await;
                            return;
                        }
                        let Ok(len) = socket.read_u64().await else {
                            return;
                        };
                        let body = read_body(&mut socket, len, u64::MAX).await.unwrap();
                        let _: TcpRequest = serde_json::from_slice(&body).unwrap();
                        served.fetch_add(1, Ordering::SeqCst);
                        let response = serde_json::to_vec(&forwarded()).unwrap();
                        if write_frame(&mut socket, &response).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });
        let peers = TcpFactory::new(1, PeerConfig::default()).peers();
        let forward = || {
            MemberRequest::Forward(crate::peer::ForwardedRequest {
                method: "POST".to_string(),
                path: "/v1/mutate".to_string(),
                headers: Vec::new(),
                body: "{}".to_string(),
            })
        };
        for _ in 0..2 {
            let answer = peers
                .member(2, &addr, forward(), Duration::from_secs(5))
                .await;
            assert!(
                matches!(&answer, Ok(MemberResponse::Forward(response)) if response.status == 200),
                "{answer:?}"
            );
        }
        assert_eq!(ran.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn a_failed_dial_is_retried_within_a_heartbeat_then_less_often() {
        // OpenRaft waits 500 ms after every unreachable answer; a
        // follower the leader cannot reach for 300 ms asks for a pre-vote.
        let peer = FakePeer::start(2, Duration::ZERO, echo_votes).await;
        let network = client(&peer).await;
        let ms = |ms: u64| Duration::from_millis(ms);
        assert_eq!(
            network.backoff().take(8).collect::<Vec<_>>(),
            [25, 50, 100, 200, 400, 500, 500, 500].map(ms)
        );
    }

    #[tokio::test]
    async fn an_abandoned_exchange_never_answers_the_next_request() {
        // OpenRaft drops an RPC at its own deadline. The first vote's
        // answer comes late; the next vote must get its own answer, not
        // the late one left on the connection.
        let peer = FakePeer::start(2, Duration::from_millis(300), echo_votes).await;
        let mut network = client(&peer).await;
        let option = RPCOption::new(Duration::from_secs(5));
        let abandoned = tokio::time::timeout(
            Duration::from_millis(100),
            network.vote(vote(1), option.clone()),
        )
        .await;
        assert!(abandoned.is_err(), "the first vote should still be waiting");
        let answer = network.vote(vote(2), option).await.unwrap();
        assert_eq!(answer.vote.leader_id().term, 2);
    }

    #[tokio::test]
    async fn short_lived_clients_reuse_pooled_connections() {
        let peer = FakePeer::start(2, Duration::ZERO, echo_votes).await;
        let mut factory = TcpFactory::new(1, PeerConfig::default());
        for term in 1..=5 {
            let mut network = factory.new_client(2, &BasicNode::new(&peer.addr)).await;
            let answer = network
                .vote(vote(term), RPCOption::new(Duration::from_secs(5)))
                .await
                .unwrap();
            assert_eq!(answer.vote.leader_id().term, term);
        }
        assert_eq!(peer.connections.load(Ordering::SeqCst), 1);
        assert_eq!(peer.requests.load(Ordering::SeqCst), 5);
    }

    fn append(entries: u64) -> AppendEntriesRequest<TypeConfig> {
        let log_id = |index| LogId::new(CommittedLeaderId::new(1, 1), index);
        AppendEntriesRequest {
            vote: Vote::new_committed(1, 1),
            prev_log_id: None,
            entries: (1..=entries)
                .map(|index| Entry {
                    log_id: log_id(index),
                    payload: EntryPayload::Blank,
                })
                .collect(),
            leader_commit: None,
        }
    }

    #[tokio::test]
    async fn a_data_append_outlives_openraft_dropping_its_call() {
        // A data append slower than OpenRaft's deadline keeps going, and
        // OpenRaft's retry of the same entries collects its answer rather
        // than sending them again.
        let peer = FakePeer::start(2, Duration::from_millis(300), echo_votes).await;
        let mut network = client(&peer).await;
        let option = RPCOption::new(Duration::from_millis(100));
        for _ in 0..2 {
            let dropped = tokio::time::timeout(
                Duration::from_millis(100),
                network.append_entries(append(3), option.clone()),
            )
            .await;
            assert!(dropped.is_err());
        }
        let answer = network.append_entries(append(3), option).await.unwrap();
        assert_eq!(answer, AppendEntriesResponse::Success);
        assert_eq!(peer.requests.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_busy_follower_gets_one_entry_at_a_time_then_backoff() {
        fn busy(_: TcpRequest) -> TcpResponse {
            TcpResponse::Busy
        }
        let peer = FakePeer::start(2, Duration::ZERO, busy).await;
        let mut network = client(&peer).await;
        let option = RPCOption::new(Duration::from_secs(2));
        let hint = |error| match error {
            RPCError::PayloadTooLarge(too_large) => too_large.entries_hint(),
            other => panic!("expected a smaller resend, got {other:?}"),
        };
        // The first refusal resends one entry at once.
        let error = network
            .append_entries(append(4), option.clone())
            .await
            .unwrap_err();
        assert_eq!(hint(error), 1);
        assert!(network.refusals.retry_at.is_none());
        // Refusing that too makes the next data append wait, doubling.
        let error = network
            .append_entries(append(1), option.clone())
            .await
            .unwrap_err();
        assert_eq!(hint(error), 1);
        let first = network.refusals.retry_at.unwrap() - Instant::now();
        let started = Instant::now();
        let error = network.append_entries(append(1), option).await.unwrap_err();
        assert_eq!(hint(error), 1);
        assert!(started.elapsed() >= first.saturating_sub(Duration::from_millis(5)));
        let second = network.refusals.retry_at.unwrap() - Instant::now();
        assert!(second > first, "{second:?} should exceed {first:?}");
    }

    #[test]
    fn appends_get_time_for_their_bytes() {
        let config = PeerConfig::default();
        let ttl = Duration::from_millis(500);
        assert_eq!(config.append_allowance(ttl, 0), Duration::from_secs(5));
        // 64 MiB at 8 MiB/s: eight more seconds.
        assert_eq!(
            config.append_allowance(ttl, 64 * 1024 * 1024),
            Duration::from_secs(13)
        );
    }

    #[test]
    fn the_replication_budget_holds_one_largest_frame() {
        assert!(PeerConfig::default().validate().is_ok());
        let tight = PeerConfig {
            max_frame: 4 * 1024 * 1024,
            max_queued_bytes: Some(4 * 1024 * 1024),
            ..PeerConfig::default()
        };
        assert!(tight.validate().unwrap_err().contains("at least"));
        let room = Room::new(200 * 1024);
        let held = room.take(150 * 1024).unwrap();
        assert!(room.take(100 * 1024).is_none());
        // Small frames always pass.
        assert!(room.take(SMALL_FRAME).is_some());
        drop(held);
        assert!(room.take(100 * 1024).is_some());
        assert_eq!(room.refused.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn bodies_grow_with_arriving_bytes_not_claimed_lengths() {
        // A claimed 200 MB frame followed by four bytes and EOF fails
        // without allocating the claim.
        let (mut client, mut server) = tokio::io::duplex(64);
        tokio::spawn(async move {
            client.write_all(b"tiny").await.unwrap();
        });
        let error = read_body(&mut server, 200 * 1024 * 1024, 256 * 1024 * 1024)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
        let (_client, mut server) = tokio::io::duplex(64);
        let error = read_body(&mut server, 2048, 1024).await.unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    }
}
