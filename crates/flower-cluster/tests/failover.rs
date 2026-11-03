//! Raft timing as members live it, with production timing
//! ([`timing::raft_config`] on the derived defaults): how long a group
//! goes without a leader once its leader dies, that a follower slower
//! than the heartbeat keeps its leader under sustained writes, and that
//! a snapshot transfer longer than the election timeout starts no
//! election, and that a pre-vote refused by a higher vote asks for the
//! term after it.

mod fixtures;

use fixtures::puts::{Puts, manifest};
use fixtures::{obj, s};

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use flower_cluster::logstore::LogStore;
use flower_cluster::network::{
    InProcessFactory, InProcessNetwork, PeerConfig, PeerFactory, Peers, Registry, TcpFactory,
};
use flower_cluster::node::{FileOpener, ISOLATED, Node, NodeError};
use flower_cluster::timing::{RaftTiming, SnapshotSettings, TimingSettings, raft_config};
use flower_cluster::types::{ClusterWrite, NodeId, TypeConfig};
use flower_engine::engine::MutationOptions;
use openraft::error::{InstallSnapshotError, RPCError, RaftError};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::storage::RaftLogStorage;
use openraft::{BasicNode, ServerState, Vote};
use redb::backends::FileBackend;

/// Production timing, as `cargo flower serve` derives it with no
/// timing flags, and the OpenRaft configuration built from it.
fn production(snapshots: &SnapshotSettings) -> (RaftTiming, Arc<openraft::Config>) {
    let timing = RaftTiming::derive(&TimingSettings::default()).unwrap();
    let config = raft_config(&timing, snapshots).unwrap();
    (timing, Arc::new(config))
}

async fn put(node: &Node, key: &str, value: &str) -> u64 {
    try_put(node, key, value).await.unwrap()
}

async fn try_put(node: &Node, key: &str, value: &str) -> Result<u64, NodeError> {
    let args = obj(vec![("key", s(&format!("\"{key}\""))), ("value", s(value))]);
    node.mutate(b"db", "app", "put", &args, &MutationOptions::default())
        .await
        .map(|written| written.position)
}

async fn wait_until(what: &str, within: Duration, condition: impl Fn() -> bool) {
    let deadline = Instant::now() + within;
    while !condition() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// The leader every one of `nodes` agrees on.
async fn agreed_leader(nodes: &[&Node]) -> NodeId {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let leaders: BTreeSet<Option<NodeId>> =
            nodes.iter().map(|node| node.status().leader).collect();
        if let [Some(leader)] = leaders.into_iter().collect::<Vec<_>>()[..] {
            return leader;
        }
        assert!(Instant::now() < deadline, "no agreed leader");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

// ------------------------------------------------------------ failover

/// A loopback TCP member: its node, address and serving task.
struct TcpMember {
    node: Node,
    addr: String,
    serving: tokio::task::JoinHandle<()>,
}

async fn tcp_member(
    id: NodeId,
    dir: &Path,
    config: Arc<openraft::Config>,
    peers: &PeerConfig,
) -> TcpMember {
    let node = Node::open(
        id,
        &dir.join(format!("n{id}")),
        config,
        TcpFactory::new(id, peers.clone()),
        Arc::new(Puts),
    )
    .await
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let serving = node.serve_listener(listener, peers.clone());
    TcpMember {
        node,
        addr,
        serving,
    }
}

/// Start a three-member loopback group, let it settle, kill its leader
/// and answer how long until a survivor leads.
async fn failover_once(dir: &Path, config: Arc<openraft::Config>) -> Duration {
    let peers = PeerConfig::with_token("secret");
    let mut members = Vec::new();
    for id in 1..=3 {
        members.push(tcp_member(id, dir, config.clone(), &peers).await);
    }
    members[0]
        .node
        .initialize_nodes(
            members
                .iter()
                .map(|member| (member.node.id(), BasicNode::new(&member.addr)))
                .collect::<BTreeMap<_, _>>(),
        )
        .await
        .unwrap();
    for member in &members {
        wait_until("every member to catch up", Duration::from_secs(20), || {
            !member.node.rejoining()
        })
        .await;
    }
    let nodes: Vec<&Node> = members.iter().map(|member| &member.node).collect();
    let leader = agreed_leader(&nodes).await;
    let lead = nodes.iter().find(|node| node.id() == leader).unwrap();
    lead.deploy(b"db", "app", [7u8; 32], b"wasm", &manifest())
        .await
        .unwrap();
    let written = put(lead, "before", "kill").await;
    for node in &nodes {
        wait_until("every member to apply", Duration::from_secs(20), || {
            node.status().last_applied >= Some(written)
        })
        .await;
    }
    // Steady heartbeats for a while: the followers' leases on the
    // leader are as fresh as they get.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Kill the leader: its listener closes and its Raft core stops.
    let index = members
        .iter()
        .position(|member| member.node.id() == leader)
        .unwrap();
    let dead = members.remove(index);
    let killed = Instant::now();
    dead.serving.abort();
    let survivors: Vec<&Node> = members.iter().map(|member| &member.node).collect();
    let elected = async {
        let deadline = killed + Duration::from_secs(20);
        loop {
            for node in &survivors {
                let metrics = node.raft().metrics().borrow().clone();
                if metrics.state == ServerState::Leader && metrics.current_leader == Some(node.id())
                {
                    return (node.id(), killed.elapsed());
                }
            }
            assert!(Instant::now() < deadline, "no survivor took over");
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    };
    let (stopped, (successor, failover)) = tokio::join!(dead.node.shutdown(), elected);
    stopped.unwrap();
    assert_ne!(successor, leader);
    // The successor takes writes.
    let next = survivors
        .iter()
        .find(|node| node.id() == successor)
        .unwrap();
    put(next, "after", "kill").await;
    for member in members {
        member.serving.abort();
        member.node.shutdown().await.unwrap();
    }
    failover
}

/// Election after the leader of a three-member loopback group dies. A
/// follower asks to campaign once its leader lease plus its random election
/// timeout (min..max) passed since the last heartbeat, checked on ticks 1.5
/// heartbeats apart, then runs a pre-vote and a vote round: about 0.45–0.6 s
/// by default. The registry crate's timing took 4.5–6 s and fails here.
/// `flower-client/tests/failover.rs` times what clients see.
#[tokio::test(flavor = "multi_thread")]
async fn a_killed_leader_is_replaced_within_the_derived_bound() {
    let (timing, config) = production(&SnapshotSettings::default());
    let lease = Duration::from_millis(timing.leader_lease_ms);
    let longest = timing.longest_detection();
    let tick = Duration::from_millis(config.heartbeat_interval * 3 / 2);
    // Pre-vote and vote rounds, metrics and scheduling under a parallel
    // test run.
    let slack = Duration::from_millis(300);
    let mut samples = Vec::new();
    for round in 0..3 {
        let dir = tempfile::tempdir().unwrap();
        let failover = failover_once(dir.path(), config.clone()).await;
        eprintln!("failover round {round}: {failover:?}");
        samples.push(failover);
    }
    eprintln!(
        "failover over {} kills: min {:?}, max {:?}; derived {:?}..{:?} plus a {tick:?} tick",
        samples.len(),
        samples.iter().min().unwrap(),
        samples.iter().max().unwrap(),
        lease + Duration::from_millis(timing.election_min_ms),
        longest,
    );
    // The derived bound leaves half a second for the rest of a failover.
    assert!(
        longest <= Duration::from_millis(500),
        "lease plus the longest election timeout is {longest:?}, over 0.5 s"
    );
    for failover in samples {
        // No survivor campaigned before its lease on the dead leader ran.
        assert!(failover >= lease, "{failover:?} is shorter than the lease");
        assert!(
            failover <= longest + tick + slack,
            "{failover:?} exceeds {longest:?} + {tick:?} + {slack:?}"
        );
    }
}

// --------------------------------------------------- slow member harness

/// Which members are slow, one bit per node ID: their disks sync late
/// and RPCs to them take a round trip. None at first.
#[derive(Clone, Default)]
struct Slowness {
    members: Arc<AtomicU64>,
}

impl Slowness {
    /// Make `id` the one slow member.
    fn set(&self, id: NodeId) {
        self.members.store(1 << id, Ordering::SeqCst);
    }

    /// Make `id` slow too.
    fn add(&self, id: NodeId) {
        self.members.fetch_or(1 << id, Ordering::SeqCst);
    }

    /// Make every member fast again.
    fn clear(&self) {
        self.members.store(0, Ordering::SeqCst);
    }

    fn is(&self, id: NodeId) -> bool {
        slow(&self.members, id)
    }
}

/// Whether `id`'s bit is set in `members`.
fn slow(members: &AtomicU64, id: NodeId) -> bool {
    members.load(Ordering::SeqCst) & (1 << id) != 0
}

/// A file whose syncs take `delay()` longer while its member is slow.
#[derive(Debug)]
struct SlowDisk {
    file: FileBackend,
    id: NodeId,
    slow: Arc<AtomicU64>,
    delay: fn() -> Duration,
}

impl redb::StorageBackend for SlowDisk {
    fn len(&self) -> std::io::Result<u64> {
        self.file.len()
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> std::io::Result<()> {
        self.file.read(offset, out)
    }

    fn set_len(&self, len: u64) -> std::io::Result<()> {
        self.file.set_len(len)
    }

    fn sync_data(&self) -> std::io::Result<()> {
        if slow(&self.slow, self.id) {
            std::thread::sleep((self.delay)());
        }
        self.file.sync_data()
    }

    fn write(&self, offset: u64, data: &[u8]) -> std::io::Result<()> {
        self.file.write(offset, data)
    }

    fn close(&self) -> std::io::Result<()> {
        self.file.close()
    }
}

/// Open member `id`'s files over disks that sync `delay()` late while it
/// is the slow one.
fn slow_files(id: NodeId, slowness: &Slowness, delay: fn() -> Duration) -> FileOpener {
    let slow = slowness.members.clone();
    Arc::new(move |path: &Path| {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .map_err(|error| redb::DatabaseError::Storage(redb::StorageError::Io(error)))?;
        redb::Builder::new().create_with_backend(SlowDisk {
            file: FileBackend::new(file)?,
            id,
            slow: slow.clone(),
            delay,
        })
    })
}

/// No extra fsync time.
fn no_delay() -> Duration {
    Duration::ZERO
}

/// A slow disk's extra fsync time: mostly 20–90 ms, one sync in ten
/// 100–150 ms (its 99th percentile), one in a hundred a 250 ms stall.
fn slow_fsync() -> Duration {
    let roll: f64 = rand::random();
    let ms = if roll < 0.01 {
        250
    } else if roll < 0.10 {
        rand::random_range(100..150)
    } else {
        rand::random_range(20..90)
    };
    Duration::from_millis(ms)
}

/// In-process peers where every RPC takes a round trip of up to
/// `jitter`, the slow member's `rtt` more, and each InstallSnapshot
/// segment to it `per_chunk` more.
#[derive(Clone)]
struct Lagging {
    inner: InProcessFactory,
    slowness: Slowness,
    rtt: Duration,
    jitter: Duration,
    per_chunk: Duration,
    stats: Arc<Stats>,
}

/// What the slow member's RPCs did.
#[derive(Default)]
struct Stats {
    /// Longest AppendEntries round trip to it, in milliseconds.
    slowest_append_ms: AtomicU64,
    /// AppendEntries that completed.
    appends: AtomicUsize,
    /// InstallSnapshot segments sent to it.
    chunks: AtomicUsize,
}

struct LaggingNetwork {
    inner: InProcessNetwork,
    target: NodeId,
    slowness: Slowness,
    rtt: Duration,
    jitter: Duration,
    per_chunk: Duration,
    stats: Arc<Stats>,
}

impl LaggingNetwork {
    /// Half a round trip to the target, drawn anew for each leg: before
    /// the request and after the answer.
    fn half(&self) -> Duration {
        let base = if self.slowness.is(self.target) {
            self.rtt / 2
        } else {
            Duration::ZERO
        };
        let jitter = self.jitter.as_micros() as u64 / 2;
        base + Duration::from_micros(if jitter == 0 {
            0
        } else {
            rand::random_range(0..jitter)
        })
    }
}

impl RaftNetworkFactory<TypeConfig> for Lagging {
    type Network = LaggingNetwork;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> LaggingNetwork {
        LaggingNetwork {
            inner: self.inner.new_client(target, node).await,
            target,
            slowness: self.slowness.clone(),
            rtt: self.rtt,
            jitter: self.jitter,
            per_chunk: self.per_chunk,
            stats: self.stats.clone(),
        }
    }
}

impl PeerFactory for Lagging {
    fn peers(&self) -> Peers {
        self.inner.peers()
    }

    fn group(&self) -> flower_cluster::group::GroupId {
        self.inner.group()
    }
}

impl RaftNetwork<TypeConfig> for LaggingNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let started = Instant::now();
        let half = self.half();
        tokio::time::sleep(half).await;
        let answer = self.inner.append_entries(rpc, option).await;
        tokio::time::sleep(half).await;
        if self.slowness.is(self.target) && answer.is_ok() {
            self.stats.appends.fetch_add(1, Ordering::SeqCst);
            self.stats
                .slowest_append_ms
                .fetch_max(started.elapsed().as_millis() as u64, Ordering::SeqCst);
        }
        answer
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        let half = self.half();
        if self.slowness.is(self.target) {
            self.stats.chunks.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(self.per_chunk).await;
        }
        tokio::time::sleep(half).await;
        let answer = self.inner.install_snapshot(rpc, option).await;
        tokio::time::sleep(half).await;
        answer
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let half = self.half();
        tokio::time::sleep(half).await;
        let answer = self.inner.vote(rpc, option).await;
        tokio::time::sleep(half).await;
        answer
    }
}

/// An in-process group of slowable members.
struct Group {
    dir: tempfile::TempDir,
    registry: Registry,
    config: Arc<openraft::Config>,
    slowness: Slowness,
    disk_delay: fn() -> Duration,
    network: Lagging,
}

impl Group {
    /// The slow member's disk syncs `disk_delay()` late and RPCs to it
    /// take `rtt` more; every RPC takes a round trip of up to `jitter`.
    fn new(
        config: Arc<openraft::Config>,
        disk_delay: fn() -> Duration,
        rtt: Duration,
        jitter: Duration,
    ) -> Self {
        let registry = Registry::new();
        let slowness = Slowness::default();
        Group {
            dir: tempfile::tempdir().unwrap(),
            network: Lagging {
                inner: InProcessFactory::new(registry.clone()),
                slowness: slowness.clone(),
                rtt,
                jitter,
                per_chunk: Duration::ZERO,
                stats: Arc::default(),
            },
            registry,
            config,
            slowness,
            disk_delay,
        }
    }

    /// Open (or reopen) member `id` and register it. Its traffic carries
    /// its ID, so the registry can cut its links.
    async fn open(&self, id: NodeId) -> Node {
        let mut network = self.network.clone();
        network.inner = network.inner.with_source(id);
        let node = Node::open_with(
            id,
            &self.dir.path().join(format!("n{id}")),
            self.config.clone(),
            network,
            Arc::new(Puts),
            slow_files(id, &self.slowness, self.disk_delay),
        )
        .await
        .unwrap();
        self.registry.register_peer(node.peer_service());
        node
    }

    /// `count` voters with an agreed leader and the module deployed.
    async fn start(&self, count: u64) -> Vec<Option<Node>> {
        let mut nodes = Vec::new();
        for id in 1..=count {
            nodes.push(Some(self.open(id).await));
        }
        nodes[0]
            .as_ref()
            .unwrap()
            .initialize((1..=count).collect::<BTreeSet<_>>())
            .await
            .unwrap();
        let live: Vec<&Node> = nodes.iter().flatten().collect();
        let leader = agreed_leader(&live).await;
        live.iter()
            .find(|node| node.id() == leader)
            .unwrap()
            .deploy(b"db", "app", [7u8; 32], b"wasm", &manifest())
            .await
            .unwrap();
        for node in &live {
            wait_until("every member to catch up", Duration::from_secs(20), || {
                !node.rejoining()
            })
            .await;
        }
        nodes
    }
}

/// Each member's (term, leader) and whether any member ever campaigned
/// (OpenRaft 0.9 has no pre-vote: every campaign raises a term).
fn terms(nodes: &[&Node]) -> Vec<(NodeId, u64, Option<NodeId>)> {
    nodes
        .iter()
        .map(|node| {
            let status = node.status();
            (node.id(), status.term, status.leader)
        })
        .collect()
}

// ---------------------------------------------------------- slow follower

/// Thirty seconds of sustained writes with a follower slower than the
/// 50 ms heartbeat, its log fsync taking 20–90 ms more, 100–150 ms at
/// its 99th percentile and 250 ms now and then, and every round trip
/// jittering: one leader and one term throughout, and the follower
/// keeps replicating. It hears its leader only between fsyncs, about
/// 0.3 s apart at worst here, inside its 0.4 s of lease and shortest
/// election timeout (it passes without pre-vote too); a longer stall
/// would only start a pre-vote, which the other voters, still hearing
/// the leader, refuse. Registry OpenRaft bounded every AppendEntries by
/// the heartbeat, so its appends restarted forever; that is why
/// `timing.rs` once raised the heartbeat to 500 ms.
#[tokio::test(flavor = "multi_thread")]
async fn thirty_seconds_with_a_slow_follower_keep_one_leader_and_one_term() {
    let (timing, config) = production(&SnapshotSettings::default());
    let heartbeat = Duration::from_millis(timing.heartbeat_ms);
    // The slow follower: slow fsyncs and 20 ms more round trip; every
    // link: up to 30 ms of jitter.
    let group = Group::new(
        config,
        slow_fsync,
        Duration::from_millis(20),
        Duration::from_millis(30),
    );
    let nodes = group.start(3).await;
    let live: Vec<&Node> = nodes.iter().flatten().collect();
    let leader = agreed_leader(&live).await;
    let slow = live
        .iter()
        .map(|node| node.id())
        .find(|id| *id != leader)
        .unwrap();
    group.slowness.set(slow);
    let lead = *live.iter().find(|node| node.id() == leader).unwrap();
    let follower = *live.iter().find(|node| node.id() == slow).unwrap();
    let before = terms(&live);
    let first_index = lead.raft().metrics().borrow().last_log_index;

    // Four writers for thirty seconds. A write the leader gives up
    // because it heard no quorum for a lease is retried, as clients do:
    // that takes the leader's own disk stalling past the lease (another
    // process's load on this machine), not the slow follower, and is
    // counted, not failed; an election would still fail the test.
    let run = Duration::from_secs(30);
    let started = Instant::now();
    let mut matched_midway = None;
    let mut changed = Vec::new();
    let isolated = AtomicU64::new(0);
    let writer = |writer: u32| {
        let isolated = &isolated;
        async move {
            let mut count = 0u64;
            let mut position = 0;
            while started.elapsed() < run {
                let key = format!("w{writer}-{count}");
                position = loop {
                    match try_put(lead, &key, "steady").await {
                        Ok(position) => break position,
                        Err(NodeError::Failure { code, .. }) if code == ISOLATED => {
                            isolated.fetch_add(1, Ordering::SeqCst);
                        }
                        Err(error) => panic!("write {key} failed: {error}"),
                    }
                };
                count += 1;
            }
            (count, position)
        }
    };
    let watch = async {
        while started.elapsed() < run {
            let now = terms(&live);
            if now != before {
                changed.push(now);
            }
            if matched_midway.is_none() && started.elapsed() >= run / 2 {
                matched_midway = lead
                    .raft()
                    .metrics()
                    .borrow()
                    .replication
                    .as_ref()
                    .and_then(|replication| replication.get(&slow).copied().flatten())
                    .map(|log| log.index);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    let (a, b, c, d, ()) = tokio::join!(writer(0), writer(1), writer(2), writer(3), watch);
    let written = [a, b, c, d];
    let total: u64 = written.iter().map(|(count, _)| count).sum();
    let last = written.iter().map(|(_, position)| *position).max().unwrap();
    let slowest =
        Duration::from_millis(group.network.stats.slowest_append_ms.load(Ordering::SeqCst));
    let appends = group.network.stats.appends.load(Ordering::SeqCst);
    eprintln!(
        "{total} writes in {run:?}; {appends} appends reached the slow follower, the slowest \
         in {slowest:?}; it matched {matched_midway:?} midway; {} writes retried after the \
         leader heard no quorum for a lease",
        isolated.load(Ordering::SeqCst)
    );

    // The scenario held: appends to the follower outlasted heartbeats.
    assert!(slowest > heartbeat, "slowest append {slowest:?}");
    // Nobody campaigned and the leader stayed.
    assert!(
        changed.is_empty(),
        "terms or leaders moved: {before:?} -> {changed:?}"
    );
    assert_eq!(terms(&live), before);
    // The follower kept replicating: it had matched entries written
    // during the run by its middle, and catches up once writes stop.
    assert!(
        matched_midway > first_index,
        "the slow follower matched {matched_midway:?} midway, from {first_index:?}"
    );
    wait_until(
        "the slow follower to catch up",
        Duration::from_secs(20),
        || follower.status().last_applied >= Some(last),
    )
    .await;
    assert_eq!(terms(&live), before);
}

// ------------------------------------------------------ loaded followers

/// A flush longer than the 300 ms lease but well inside the lease plus
/// the shortest 100 ms election timeout.
fn loaded_fsync() -> Duration {
    Duration::from_millis(330)
}

/// A leader whose followers both flush slowly is slow, not cut off: it
/// takes every write. The quorum's acknowledgement is dated when its
/// request was sent, so 330 ms flushes make it a little more than that
/// old before each answer, and no other leader can exist before the
/// 300 ms lease plus the shortest 100 ms election timeout. Cut off after
/// the lease alone, the leader gave up every write as `leader_isolated`
/// (none committed in four seconds), under load that changed nothing
/// about its leadership.
#[tokio::test(flavor = "multi_thread")]
async fn a_leader_slowed_by_its_followers_flushes_takes_every_write() {
    let (_, config) = production(&SnapshotSettings::default());
    let group = Group::new(config, loaded_fsync, Duration::ZERO, Duration::ZERO);
    let nodes = group.start(3).await;
    let live: Vec<&Node> = nodes.iter().flatten().collect();
    let leader = agreed_leader(&live).await;
    let lead = *live.iter().find(|node| node.id() == leader).unwrap();
    for node in &live {
        if node.id() != leader {
            group.slowness.add(node.id());
        }
    }
    let before = terms(&live);
    // A write given up is retried 50 ms later, as clients do, and the
    // time counted: a stall of the whole machine (other processes'
    // load) can still hold an acknowledgement past the window now and
    // then, but cut off after the lease alone, the leader refused
    // nearly the whole run.
    let run = Duration::from_secs(4);
    let started = Instant::now();
    let (mut written, mut refused) = (0u64, Duration::ZERO);
    while started.elapsed() < run {
        match try_put(lead, &format!("k{written}"), "loaded").await {
            Ok(_) => written += 1,
            Err(NodeError::Failure { code, .. }) if code == ISOLATED => {
                tokio::time::sleep(Duration::from_millis(50)).await;
                refused += Duration::from_millis(50);
            }
            Err(error) => panic!("write {written} failed: {error}"),
        }
    }
    group.slowness.clear();
    eprintln!(
        "{written} writes with both followers flushing for {:?}; refused for {refused:?}",
        loaded_fsync()
    );
    assert!(written > 4, "{written} writes");
    assert!(refused < run / 4, "refused for {refused:?} of {run:?}");
    assert_eq!(terms(&live), before);
}

/// A leader cut off from both followers still gives a waiting write up
/// as `leader_isolated` within its isolation window (the lease plus the
/// shortest election timeout, 400 ms by default) and the metrics'
/// reporting tick, so its clients retry where a new leader can take the
/// write. The window bounds liveness only (another leader can exist
/// from the lease on), and a longer one would keep them waiting.
#[tokio::test(flavor = "multi_thread")]
async fn a_cut_off_leader_gives_writes_up_within_its_isolation_window() {
    let (timing, config) = production(&SnapshotSettings::default());
    let window = Duration::from_millis(timing.leader_lease_ms + timing.election_min_ms);
    let group = Group::new(config, no_delay, Duration::ZERO, Duration::ZERO);
    let nodes = group.start(3).await;
    let live: Vec<&Node> = nodes.iter().flatten().collect();
    let leader = agreed_leader(&live).await;
    let lead = *live.iter().find(|node| node.id() == leader).unwrap();
    put(lead, "before", "cut").await;
    for node in &live {
        if node.id() != leader {
            group.registry.block(leader, node.id());
            group.registry.block(node.id(), leader);
        }
    }
    let started = Instant::now();
    let given_up = try_put(lead, "after", "cut").await;
    let elapsed = started.elapsed();
    group.registry.heal();
    assert!(
        matches!(&given_up, Err(NodeError::Failure { code, .. }) if code == ISOLATED),
        "{given_up:?}"
    );
    // Two ticks of 75 ms for the metrics to show it, and a little more.
    let tick = Duration::from_millis(timing.heartbeat_ms * 3 / 2);
    assert!(
        elapsed <= window + 2 * tick + Duration::from_millis(100),
        "given up after {elapsed:?}, window {window:?}"
    );
}

// ------------------------------------------------ slow beside cut off

/// A flush that outlasts the 300 ms lease plus the longest 200 ms
/// election timeout.
fn stalled_fsync() -> Duration {
    Duration::from_millis(600)
}

/// With one follower cut off from the leader, the other, flushing for
/// longer than the lease plus an election timeout, keeps the leader it
/// forms a quorum with. The leader sends a follower nothing more until
/// it answers, and the follower's Raft core handles nothing while its
/// flush runs, so its lease runs from when its leader's entries are
/// durable (Flower's OpenRaft fork, change 7). Counted from their
/// arrival instead, the timer ticks queued during each flush found the
/// leader silent the moment it ended, the follower asked for a
/// pre-vote, the cut-off follower (hearing no leader) granted it, and a
/// healthy leader with a quorum was deposed.
#[tokio::test(flavor = "multi_thread")]
async fn a_slow_follower_keeps_its_leader_while_the_other_is_cut_off() {
    // An RPC deadline past the stall, so the leader waits for each
    // answer rather than resending (a resend would renew the lease).
    let settings = TimingSettings {
        rpc_timeout_ms: Some(2_000),
        ..TimingSettings::default()
    };
    let timing = RaftTiming::derive(&settings).unwrap();
    let config = Arc::new(raft_config(&timing, &SnapshotSettings::default()).unwrap());
    let group = Group::new(config, stalled_fsync, Duration::ZERO, Duration::ZERO);
    let nodes = group.start(3).await;
    let live: Vec<&Node> = nodes.iter().flatten().collect();
    let leader = agreed_leader(&live).await;
    let lead = *live.iter().find(|node| node.id() == leader).unwrap();
    let mut followers = live.iter().map(|node| node.id()).filter(|id| *id != leader);
    let (slow, cut) = (followers.next().unwrap(), followers.next().unwrap());
    group.slowness.set(slow);
    group.registry.block(leader, cut);
    group.registry.block(cut, leader);
    let before: Vec<_> = terms(&live)
        .into_iter()
        .filter(|(id, ..)| *id != cut)
        .collect();

    // Proposals straight to Raft for six seconds, two at a time (the
    // writer would refuse client writes here: the quorum's
    // acknowledgement is a flush old, past the isolation window), and a
    // watch on the leader's and the slow follower's terms and leaders.
    let run = Duration::from_secs(6);
    let started = Instant::now();
    let mut changed = Vec::new();
    let proposer = |id: u64| async move {
        let mut written = 0u64;
        while started.elapsed() < run {
            let proposed = lead.raft().client_write(ClusterWrite::Incarnation {
                incarnation: 7,
                proposed_ms: id * 1_000_000 + written,
            });
            match tokio::time::timeout(Duration::from_secs(5), proposed).await {
                Ok(Ok(_)) => written += 1,
                Ok(Err(error)) => return (written, Some(error.to_string())),
                Err(_) => return (written, Some("a proposal took 5 s".to_string())),
            }
        }
        (written, None)
    };
    let watch = async {
        while started.elapsed() < run {
            let now: Vec<_> = terms(&live)
                .into_iter()
                .filter(|(id, ..)| *id != cut)
                .collect();
            if now != before && changed.len() < 3 {
                changed.push(now);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    let ((a, failed_a), (b, failed_b), ()) = tokio::join!(proposer(1), proposer(2), watch);
    group.slowness.clear();
    group.registry.heal();
    eprintln!(
        "{} writes in {run:?} with the quorum's follower flushing for {:?}",
        a + b,
        stalled_fsync()
    );
    assert!(
        changed.is_empty(),
        "terms or leaders moved: {before:?} -> {changed:?}"
    );
    assert_eq!((failed_a, failed_b), (None, None));
    assert!(a + b > 0, "no write committed");
}

// ---------------------------------------------------- partitioned follower

/// A follower cut off from its group for many election timeouts comes
/// back without deposing the leader the others went on following: its
/// pre-votes found no quorum, so its term never rose. Without pre-vote
/// it campaigned on every timeout, and the higher term it returned with
/// made the leader step down.
#[tokio::test(flavor = "multi_thread")]
async fn a_partitioned_follower_deposes_nobody_when_it_returns() {
    let (timing, config) = production(&SnapshotSettings::default());
    let group = Group::new(config, no_delay, Duration::ZERO, Duration::ZERO);
    let nodes = group.start(3).await;
    let live: Vec<&Node> = nodes.iter().flatten().collect();
    let leader = agreed_leader(&live).await;
    let lead = *live.iter().find(|node| node.id() == leader).unwrap();
    let cut = live
        .iter()
        .map(|node| node.id())
        .find(|id| *id != leader)
        .unwrap();
    let before = terms(&live);
    let others: Vec<NodeId> = live
        .iter()
        .map(|node| node.id())
        .filter(|id| *id != cut)
        .collect();
    for other in &others {
        group.registry.block(cut, *other);
        group.registry.block(*other, cut);
    }
    // The other two go on writing for several election timeouts.
    let away = timing.longest_detection() * 6;
    let started = Instant::now();
    let mut last = 0;
    let mut count = 0;
    while started.elapsed() < away {
        last = put(lead, &format!("away-{count}"), "x").await;
        count += 1;
    }
    group.registry.heal();
    let returned = *live.iter().find(|node| node.id() == cut).unwrap();
    wait_until(
        "the returned follower to catch up",
        Duration::from_secs(20),
        || returned.status().last_applied >= Some(last),
    )
    .await;
    tokio::time::sleep(timing.longest_detection() * 2).await;
    assert_eq!(terms(&live), before, "{count} writes while it was away");
}

// ------------------------------------------------- refused pre-vote

/// The member holding the longest log is elected even when a voter with
/// a shorter one holds a vote in the next term already. Of five voters,
/// the leader writes an entry that reaches one follower only (the
/// "ahead" member), then dies with two others; one survivor (the
/// "campaigner") had campaigned in the next term, granted pre-votes by
/// the two that died before they answered its vote. The ahead member's
/// pre-vote for its own term + 1 is refused by the campaigner, whose vote
/// in that term is greater; the campaigner's for the term after is
/// refused by the ahead member, whose log is longer; the third survivor
/// can elect neither alone. The refusal carries the campaigner's vote,
/// and the ahead member adopts it, so its next pre-vote asks for the
/// term after. Before, it asked for the same term on every timeout and no
/// leader was elected until a dead member returned.
#[tokio::test(flavor = "multi_thread")]
async fn a_pre_vote_refused_by_a_higher_vote_asks_for_the_next_term() {
    let (timing, config) = production(&SnapshotSettings::default());
    let group = Group::new(config, no_delay, Duration::ZERO, Duration::ZERO);
    let mut nodes = group.start(5).await;
    let live: Vec<&Node> = nodes.iter().flatten().collect();
    let leader = agreed_leader(&live).await;
    // The ahead member has the smallest ID and the campaigner the largest,
    // so the ahead member's vote in the campaigner's term is the smaller.
    let others: Vec<NodeId> = (1..=5).filter(|id| *id != leader).collect();
    let (ahead, third, fourth, campaigner) = (others[0], others[1], others[2], others[3]);
    let slot = |id: NodeId| (id - 1) as usize;

    // Two members die; the leader, the ahead member and the campaigner
    // still make a quorum. Then the leader loses the campaigner, and an
    // entry it writes reaches the ahead member only.
    for id in [third, fourth] {
        nodes[slot(id)].take().unwrap().shutdown().await.unwrap();
    }
    group.registry.block(leader, campaigner);
    group.registry.block(campaigner, leader);
    let lead = nodes[slot(leader)].as_ref().unwrap();
    let term = lead.status().term;
    let last_index = |id: NodeId| {
        nodes[slot(id)]
            .as_ref()
            .unwrap()
            .raft()
            .metrics()
            .borrow()
            .last_log_index
    };
    let write = tokio::time::timeout(timing.longest_detection(), try_put(lead, "ahead", "x"));
    let reached = wait_until(
        "the entry to reach the ahead member",
        Duration::from_secs(20),
        || last_index(ahead) > last_index(campaigner),
    );
    let (written, ()) = tokio::join!(write, reached);
    assert!(
        !matches!(written, Ok(Ok(_))),
        "two of five committed: {written:?}"
    );

    // The leader dies too, and the ahead member and the campaigner stop.
    // The campaigner comes back holding its vote in the next term, which
    // nobody answered; the third member returns.
    for id in [leader, ahead, campaigner] {
        nodes[slot(id)].take().unwrap().shutdown().await.unwrap();
    }
    {
        let path = group
            .dir
            .path()
            .join(format!("n{campaigner}"))
            .join("raft.redb");
        let mut logs = LogStore::open(&path).unwrap();
        logs.save_vote(&Vote::new(term + 1, campaigner))
            .await
            .unwrap();
    }
    group.registry.heal();
    let mut back = Vec::new();
    for id in [ahead, campaigner, third] {
        back.push(group.open(id).await);
    }
    let back: Vec<&Node> = back.iter().collect();
    assert_eq!(agreed_leader(&back).await, ahead);
    assert!(back[0].status().term > term + 1, "{:?}", terms(&back));
}

// ------------------------------------------------------- slow snapshot

/// Snapshot and purge a member's log through `upto`, as Raft's log
/// compaction would.
async fn compact_log(node: &Node, upto: u64) {
    wait_until("applied", Duration::from_secs(20), || {
        node.status().last_applied >= Some(upto)
    })
    .await;
    node.raft().trigger().snapshot().await.unwrap();
    wait_until("snapshot", Duration::from_secs(20), || {
        node.raft().metrics().borrow().snapshot.map(|id| id.index) >= Some(upto)
    })
    .await;
    node.raft().trigger().purge_log(upto).await.unwrap();
    wait_until("purge", Duration::from_secs(20), || {
        node.raft().metrics().borrow().purged.map(|id| id.index) >= Some(upto)
    })
    .await;
}

/// A follower that catches up by a snapshot whose transfer outlasts its
/// lease on the leader plus the longest election timeout starts no
/// election: each segment renews its leader's lease, as an append does.
/// Registry OpenRaft received segments outside the Raft core, and the
/// leader sends a follower nothing else while a snapshot is in flight,
/// so the follower campaigned mid-transfer and the new term restarted
/// the transfer. It runs without pre-vote, which would also stop that
/// campaign while the other voters hear the leader, to check the
/// renewal itself.
#[tokio::test(flavor = "multi_thread")]
async fn a_snapshot_transfer_longer_than_the_election_timeout_starts_no_election() {
    let snapshots = SnapshotSettings {
        chunk_bytes: 1024,
        ..SnapshotSettings::default()
    };
    // A lease longer than OpenRaft's 500 ms reconnect backoff, so the
    // reopened follower hears its leader before it would campaign, and
    // segments half a lease apart.
    let (timing, config) = production(&snapshots);
    let config = Arc::new(openraft::Config {
        pre_vote: false,
        leader_lease: 1_000,
        ..(*config).clone()
    });
    let longest = config.leader_lease() + Duration::from_millis(timing.election_max_ms);
    let mut group = Group::new(config, no_delay, Duration::ZERO, Duration::ZERO);
    group.network.per_chunk = Duration::from_millis(500);
    let mut nodes = group.start(3).await;
    let leader = agreed_leader(&nodes.iter().flatten().collect::<Vec<_>>()).await;
    let lead = (leader - 1) as usize;
    let behind = (1..=3).find(|id| *id != leader).unwrap();
    let slot = (behind - 1) as usize;
    let first = put(nodes[lead].as_ref().unwrap(), "first", "x").await;
    wait_until("the follower to apply", Duration::from_secs(20), || {
        nodes[slot].as_ref().unwrap().status().last_applied >= Some(first)
    })
    .await;
    let term = nodes[lead].as_ref().unwrap().status().term;

    // The follower goes down; the others write enough for a snapshot of
    // many segments and compact their logs past it.
    group.registry.unregister(behind);
    nodes[slot].take().unwrap().shutdown().await.unwrap();
    let filler = "f".repeat(400);
    let mut last = first;
    for key in 0..40 {
        last = put(nodes[lead].as_ref().unwrap(), &format!("k{key}"), &filler).await;
    }
    for node in nodes.iter().flatten() {
        compact_log(node, last).await;
    }

    // Back, it catches up by a slow snapshot.
    group.slowness.set(behind);
    let reopened = Instant::now();
    nodes[slot] = Some(group.open(behind).await);
    let live: Vec<&Node> = nodes.iter().flatten().collect();
    let caught_up = nodes[slot].as_ref().unwrap();
    let mut highest = term;
    let deadline = reopened + Duration::from_secs(60);
    while caught_up
        .raft()
        .metrics()
        .borrow()
        .snapshot
        .map(|id| id.index)
        < Some(last)
    {
        assert!(
            Instant::now() < deadline,
            "the snapshot was never installed"
        );
        highest = live
            .iter()
            .map(|node| node.status().term)
            .fold(highest, u64::max);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let transfer = reopened.elapsed();
    let chunks = group.network.stats.chunks.load(Ordering::SeqCst);
    eprintln!("snapshot of {chunks} segments installed {transfer:?} after the follower reopened");

    // The transfer outlasted the follower's lease plus its longest
    // election timeout, and nobody campaigned.
    assert!(
        transfer > longest,
        "the transfer took {transfer:?}, not over {longest:?}"
    );
    assert_eq!(highest, term, "a member campaigned during the transfer");
    let next = put(nodes[lead].as_ref().unwrap(), "next", "x").await;
    wait_until(
        "the follower to apply past the snapshot",
        Duration::from_secs(20),
        || caught_up.status().last_applied >= Some(next),
    )
    .await;
    assert!(live.iter().all(|node| node.status().term == term));
    assert!(live.iter().all(|node| node.status().leader == Some(leader)));
}
