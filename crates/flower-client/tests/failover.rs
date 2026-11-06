//! Failover as clients see it: kill the leader of a three-member group
//! (Raft over loopback TCP, an API on every member, production timing)
//! under sustained writes from clients routing by leader with the
//! default retry policy, and time how long until a client gets a write
//! acknowledged that the new leader appended, and until every client
//! whose URL names a survivor does. The same for a group other than the
//! root, among processes hosting several groups, one of which forwards
//! the group's writes from outside it.

mod fixtures;

use fixtures::{hc, s};

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use flower_client::client::{FlowerClient, MutationOptions, RequestOptions};
use flower_client::retry::Retry;
use flower_client::transport::Credentials;
use flower_cluster::host::{Host, HostSettings, Transport};
use flower_cluster::network::{InProcessFactory, PeerConfig, Registry, TcpFactory};
use flower_cluster::node::Node;
use flower_cluster::statemachine::RuntimeFactory;
use flower_cluster::timing::{RaftTiming, SnapshotSettings, TimingSettings, raft_config};
use flower_engine::catalog::{Catalog, CatalogChange, GroupStatus, PartitionStatus};
use flower_engine::runtime::{CallKind, ClosureRuntime, HostCall};
use flower_sdk::abi::HostOp;
use flower_sdk::{Value, fail};
use flower_server::api::{AppState, serve_until};
use flower_server::groups::{HostApi, StateFactory, serve_host};
use flower_server::listener::HttpOptions;
use openraft::{BasicNode, ServerState};

/// `put {key, title}`.
struct App;

impl RuntimeFactory for App {
    fn build(
        &self,
        _db: &[u8],
        _module: &str,
        _wasm: &[u8],
        _manifest: &Value,
    ) -> Result<Box<dyn flower_engine::runtime::Runtime>, String> {
        Ok(Box::new(ClosureRuntime::new(
            |kind: CallKind, _module: &str, name: &str, args: &Value, host: &mut dyn HostCall| {
                let Value::Object(map) = args else {
                    return Err(fail("TYPE_MISMATCH", "args take an object"));
                };
                if !matches!((kind, name), (CallKind::Mutation, "put")) {
                    return Err(fail("UNKNOWN_METHOD", "unknown definition"));
                }
                let key = map.get("key").cloned().unwrap_or(Value::Null);
                let title = map.get("title").cloned().unwrap_or(Value::Null);
                let mut row = BTreeMap::new();
                row.insert("title".to_string(), title);
                hc(
                    host,
                    HostOp::Set,
                    vec![s(""), s("tasks"), key, Value::Object(row)],
                )?;
                Ok(s("put-ok"))
            },
        )))
    }
}

fn manifest() -> serde_json::Value {
    serde_json::json!({
        "collections": [{
            "name": "tasks",
            "key": {"type": "string"},
            "value": {"type": "object", "fields": [
                {"name": "title", "schema": {"type": "string"}, "optional": false}
            ]},
            "indexes": [],
            "references": []
        }],
        "definitions": [{"name": "put", "kind": "mutation", "args": {"type": "json"}}],
        "triggers": [],
        "http": [{"alias": "put", "definition": "put"}]
    })
}

/// Production timing, as `cargo flower serve` derives it by default.
fn production() -> Arc<openraft::Config> {
    let timing = RaftTiming::derive(&TimingSettings::default()).unwrap();
    Arc::new(raft_config(&timing, &SnapshotSettings::default()).unwrap())
}

/// One member, in a runtime of its own: killing it drops every task
/// and socket it has at once, as a process kill does.
struct Member {
    node: Arc<Node>,
    runtime: Option<tokio::runtime::Runtime>,
}

/// How members reach each other.
#[derive(Clone)]
enum Peers {
    /// Raft over loopback TCP.
    Tcp,
    /// In process, through a registry that can cut links.
    InProcess(Registry),
}

/// Three members, each with an API.
struct Cluster {
    _dir: tempfile::TempDir,
    members: Vec<Member>,
    bases: Vec<String>,
}

impl Cluster {
    async fn start(config: Arc<openraft::Config>, network: Peers) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let peers = PeerConfig::with_token("secret");
        let mut members = Vec::new();
        let mut bases = Vec::new();
        let mut addrs = BTreeMap::new();
        for id in 1..=3u64 {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();
            let (path, config, peers) = (
                dir.path().join(format!("n{id}")),
                config.clone(),
                peers.clone(),
            );
            let network = network.clone();
            let (node, addr, base) = runtime
                .spawn(async move {
                    let (node, addr) = match network {
                        Peers::Tcp => {
                            let node = Node::open(
                                id,
                                &path,
                                config,
                                TcpFactory::new(id, peers.clone()),
                                Arc::new(App),
                            )
                            .await
                            .unwrap();
                            let listener =
                                tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                            let addr = listener.local_addr().unwrap().to_string();
                            let _raft = node.serve_listener(listener, peers);
                            (node, addr)
                        }
                        Peers::InProcess(registry) => {
                            let factory = InProcessFactory::new(registry.clone()).with_source(id);
                            let node = Node::open(id, &path, config, factory, Arc::new(App))
                                .await
                                .unwrap();
                            registry.register_peer(node.peer_service());
                            (node, String::new())
                        }
                    };
                    let node = Arc::new(node);
                    let http = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                    let base = format!("http://{}", http.local_addr().unwrap());
                    let state = AppState::new(node.clone())
                        .with_admin_token("secret".to_string())
                        .with_api_url(base.clone());
                    tokio::spawn(serve_until(
                        http,
                        state,
                        std::future::pending::<()>(),
                        Duration::from_secs(1),
                    ));
                    (node, addr, base)
                })
                .await
                .unwrap();
            addrs.insert(id, BasicNode::new(addr));
            members.push(Member {
                node,
                runtime: Some(runtime),
            });
            bases.push(base);
        }
        members[0].node.initialize_nodes(addrs).await.unwrap();
        let cluster = Cluster {
            _dir: dir,
            members,
            bases,
        };
        let leader = cluster.leader().await;
        FlowerClient::builder(&cluster.bases[leader])
            .credentials(Credentials::token("secret"))
            .build()
            .unwrap()
            .deploy(
                "db",
                "app",
                None,
                b"{}",
                &manifest(),
                &RequestOptions::default(),
            )
            .await
            .unwrap();
        cluster
    }

    /// The index of the member every live member agrees leads.
    async fn leader(&self) -> usize {
        for _ in 0..2000 {
            for (index, member) in self.members.iter().enumerate() {
                let metrics = member.node.raft().metrics().borrow().clone();
                if metrics.state == ServerState::Leader
                    && metrics.current_leader == Some(member.node.id())
                    && self
                        .members
                        .iter()
                        .filter(|other| other.node.status().leader == Some(member.node.id()))
                        .count()
                        == self.members.len()
                {
                    return index;
                }
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("no agreed leader");
    }

    /// Kill member `index` as a crash would: every task and socket of
    /// its runtime goes at once, telling nobody.
    fn kill(&mut self, index: usize) {
        if let Some(runtime) = self.members[index].runtime.take() {
            runtime.shutdown_background();
        }
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for index in 0..self.members.len() {
            self.kill(index);
        }
    }
}

/// Failovers are timed: the tests take turns, so their members (up to
/// four processes of two threads each per round) do not starve each
/// other of the CPU their timers need.
static TURNS: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// When each part of one failover happened, from the kill.
#[derive(Debug)]
struct Failover {
    /// A survivor leads.
    elected: Duration,
    /// The new leader applied the first entry of its term.
    committed: Duration,
    /// A client got a write acknowledged that the new leader appended.
    acknowledged: Duration,
    /// Every client writing before the strike did.
    every: Duration,
}

/// Writes acknowledged so far: when, and at which log position.
type Acks = Arc<Mutex<Vec<(Instant, u64, String, Instant)>>>;

/// Loop writes to `db` through a client of `base` routing by leader,
/// with the default retry policy, until `stop`.
fn writer(
    base: String,
    db: &'static str,
    name: String,
    acks: Acks,
    stop: Arc<AtomicBool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let client = FlowerClient::builder(&base)
            .credentials(Credentials::token("secret"))
            .leader_routing(true)
            .retry(Retry::Default)
            .build()
            .unwrap();
        let mut count = 0u64;
        while !stop.load(Ordering::Relaxed) {
            let started_call = Instant::now();
            let args = serde_json::json!({"key": format!("\"{name}-{count}\""), "title": "x"});
            if let Ok(written) = client
                .mutate_value(
                    db,
                    "app",
                    "put",
                    &args,
                    &MutationOptions::new().request_id(format!("{name}-{count}")),
                )
                .await
            {
                acks.lock().unwrap().push((
                    Instant::now(),
                    written.position,
                    name.clone(),
                    started_call,
                ));
            }
            count += 1;
        }
    })
}

/// What happens to the leader.
#[derive(Clone, Copy, Debug)]
enum Fault {
    /// Its process dies.
    Crash,
    /// It keeps running and serving its API, but no Raft or member
    /// traffic passes between it and the others.
    Partition,
}

/// Start a group, write for a while, strike its leader with `fault`
/// and time the failover.
async fn failover_once(config: Arc<openraft::Config>, fault: Fault) -> Failover {
    let registry = Registry::new();
    let network = match fault {
        Fault::Crash => Peers::Tcp,
        Fault::Partition => Peers::InProcess(registry.clone()),
    };
    let mut cluster = Cluster::start(config, network).await;
    let acks: Acks = Arc::default();
    let stop = Arc::new(AtomicBool::new(false));
    let writers: Vec<_> = (0..6)
        .map(|index| {
            writer(
                cluster.bases[index % 3].clone(),
                "db",
                format!("w{index}"),
                acks.clone(),
                stop.clone(),
            )
        })
        .collect();
    // Sustained writes before the kill.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let leader = cluster.leader().await;
    assert!(
        !acks.lock().unwrap().is_empty(),
        "no write was acknowledged before the kill"
    );
    let old = cluster.members[leader]
        .node
        .raft()
        .metrics()
        .borrow()
        .clone();
    let appended = old.last_log_index.unwrap_or(0);
    let survivors: Vec<Arc<Node>> = cluster
        .members
        .iter()
        .enumerate()
        .filter(|(index, _)| *index != leader)
        .map(|(_, member)| member.node.clone())
        .collect();
    let killed = Instant::now();
    match fault {
        Fault::Crash => cluster.kill(leader),
        Fault::Partition => {
            let cut = cluster.members[leader].node.id();
            for node in &survivors {
                registry.block(cut, node.id());
                registry.block(node.id(), cut);
            }
        }
    }
    // Writers whose only URL is the struck member cannot reach another.
    let reachable = (0..6)
        .filter(|index| index % 3 != leader)
        .map(|index| format!("w{index}"))
        .collect();
    let failover = measure(killed, &survivors, &old, appended, &acks, &reachable).await;
    stop.store(true, Ordering::Relaxed);
    for writer in writers {
        writer.abort();
    }
    failover
}

/// Watch `survivors` and `acks` from `killed` until a survivor leads in
/// a later term than `old`, has applied the first entry of its term,
/// and a client, then each of `writers`, has had a write acknowledged
/// past `appended`, the old leader's last log index.
async fn measure(
    killed: Instant,
    survivors: &[Arc<Node>],
    old: &openraft::RaftMetrics<u64, BasicNode>,
    appended: u64,
    acks: &Acks,
    writers: &BTreeSet<String>,
) -> Failover {
    let deadline = killed + Duration::from_secs(20);
    let mut elected = None;
    let mut committed = None;
    let mut acknowledged = None;
    let mut first = None;
    let mut every = None;
    while acknowledged.is_none() || committed.is_none() || every.is_none() {
        assert!(
            Instant::now() < deadline,
            "no failover within 20 s: elected {elected:?}, committed {committed:?}, first ack \n             {acknowledged:?}"
        );
        for node in survivors {
            let metrics = node.raft().metrics().borrow().clone();
            if metrics.state == ServerState::Leader
                && metrics.current_leader == Some(node.id())
                && metrics.current_term > old.current_term
            {
                elected.get_or_insert_with(|| killed.elapsed());
                let start = *first.get_or_insert(metrics.last_log_index);
                if committed.is_none() && metrics.last_applied.map(|log| log.index) >= start {
                    committed = Some(killed.elapsed());
                }
            }
        }
        if acknowledged.is_none() {
            acknowledged = acks
                .lock()
                .unwrap()
                .iter()
                .filter(|(at, position, _, _)| *at > killed && *position > appended)
                .map(|(at, _, _, _)| at.duration_since(killed))
                .min();
        }
        if every.is_none() {
            let acks = acks.lock().unwrap();
            let firsts: Vec<Duration> = writers
                .iter()
                .filter_map(|writer| {
                    acks.iter()
                        .filter(|(at, position, name, _)| {
                            *at > killed && *position > appended && name == writer
                        })
                        .map(|(at, _, _, _)| at.duration_since(killed))
                        .min()
                })
                .collect();
            if firsts.len() == writers.len() {
                every = firsts.into_iter().max();
            }
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    if acknowledged.is_some_and(|at| at > Duration::from_secs(1))
        || every.is_some_and(|at| at > Duration::from_secs(1))
    {
        tokio::time::sleep(Duration::from_millis(500)).await;
        eprintln!("DIAG slow failover; appended {appended}");
        for (at, position, name, started) in acks.lock().unwrap().iter() {
            if *at > killed - Duration::from_millis(200) {
                eprintln!(
                    "DIAG {name} acked +{:?} pos {position} call started {:?} before the kill",
                    at.saturating_duration_since(killed),
                    killed.saturating_duration_since(*started),
                );
            }
        }
    }
    Failover {
        elected: elected.unwrap(),
        committed: committed.unwrap(),
        acknowledged: acknowledged.unwrap(),
        every: every.unwrap(),
    }
}

/// Strike `rounds` leaders with `fault`, each in a fresh group (so every
/// member draws its randomized election timeout anew), and check that
/// the worst failover stays under a second.
async fn worst_under_a_second(fault: Fault, rounds: usize) {
    let _turn = TURNS.lock().await;
    let config = production();
    let mut samples = Vec::new();
    for round in 0..rounds {
        let failover = failover_once(config.clone(), fault).await;
        eprintln!("{fault:?} {round}: {failover:?}");
        samples.push(failover);
    }
    check_worst(&format!("{fault:?}"), samples);
}

/// Report `samples` and check that the worst failover stays under a
/// second to the first acknowledged write, and the median under a
/// second to every client's. A client whose call was already retrying
/// before the strike carries its backoff past it (a retry after 1 s or
/// more), so one client's tail measures that rather than the failover;
/// a client routed to a dead leader for seconds shows in every round.
fn check_worst(label: &str, samples: Vec<Failover>) {
    let mut acknowledged: Vec<Duration> = samples.iter().map(|f| f.acknowledged).collect();
    acknowledged.sort();
    let worst = *acknowledged.last().unwrap();
    let median = acknowledged[acknowledged.len() / 2];
    let elected: Vec<Duration> = samples.iter().map(|f| f.elected).collect();
    let committed: Vec<Duration> = samples.iter().map(|f| f.committed).collect();
    let mut every: Vec<Duration> = samples.iter().map(|f| f.every).collect();
    every.sort();
    let every_median = every[every.len() / 2];
    eprintln!(
        "{label}: acknowledged after median {median:?}, worst {worst:?}; elected {:?}..{:?}, \
         its first entry applied {:?}..{:?}; every client after median {every_median:?}, \
         worst {:?}",
        elected.iter().min().unwrap(),
        elected.iter().max().unwrap(),
        committed.iter().min().unwrap(),
        committed.iter().max().unwrap(),
        every.last().unwrap(),
    );
    assert!(
        worst < Duration::from_secs(1),
        "worst failover {worst:?}: {samples:#?}"
    );
    assert!(
        every_median < Duration::from_secs(1),
        "clients wrote again after a median of {every_median:?}: {samples:#?}"
    );
}

/// Ten leader crashes: the worst must stay under a second from the
/// kill to a client's acknowledged write. v1's timing (election max as
/// the lease, 0.55–1.1 s elections) took 1.65–2.2 s to elect alone,
/// and the registry crate's about 5 s.
#[tokio::test(flavor = "multi_thread")]
async fn clients_write_again_within_a_second_of_a_leader_crash() {
    worst_under_a_second(Fault::Crash, 10).await;
}

/// Five leader partitions: the cut-off leader keeps serving its API,
/// takes writes it cannot commit, and must give them up (and stop
/// naming itself leader) so clients routed to it find the new one.
#[tokio::test(flavor = "multi_thread")]
async fn clients_write_again_within_a_second_of_a_leader_partition() {
    worst_under_a_second(Fault::Partition, 5).await;
}

/// One process hosting groups, in a runtime of its own: killing it
/// drops every group's tasks and sockets at once.
struct Process {
    host: Arc<Host>,
    runtime: Option<tokio::runtime::Runtime>,
}

/// The members of group `g`, which owns partition `p`.
const GROUP: [u64; 3] = [2, 3, 4];

/// Four processes, each a root member serving every group it hosts on
/// one API, as `cargo flower serve` does, with Raft over loopback TCP or
/// in process. Process 1 leads the root and does not host `g`, so its
/// clients' writes to `p` cross groups to `g`'s leader.
struct Hosts {
    _dir: tempfile::TempDir,
    processes: Vec<Process>,
    bases: Vec<String>,
}

impl Hosts {
    async fn start(network: Peers) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let peers = PeerConfig::with_token("secret");
        let (mut processes, mut bases, mut addrs) = (Vec::new(), Vec::new(), BTreeMap::new());
        for id in 1..=4u64 {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();
            let (path, peers) = (dir.path().join(format!("n{id}")), peers.clone());
            let network = network.clone();
            let (host, addr, base) = runtime
                .spawn(async move {
                    let (transport, raft) = match network {
                        Peers::Tcp => {
                            let raft = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                            (Transport::Tcp(peers), Some(raft))
                        }
                        Peers::InProcess(registry) => (Transport::InProcess(registry), None),
                    };
                    let addr = raft
                        .as_ref()
                        .map_or_else(String::new, |raft| raft.local_addr().unwrap().to_string());
                    let http = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                    let base = format!("http://{}", http.local_addr().unwrap());
                    let mut settings = HostSettings::new(production());
                    let url = base.clone();
                    settings.configure = Some(Arc::new(move |_, node: Node| {
                        node.set_api_url(Some(url.clone()));
                        node
                    }));
                    let host = Arc::new(
                        Host::open(id, &path, Arc::new(App), transport, settings)
                            .await
                            .unwrap(),
                    );
                    if let Some(raft) = raft {
                        host.serve_peers(raft).unwrap();
                    }
                    let factory: StateFactory = {
                        let host = host.clone();
                        Arc::new(move |node| {
                            AppState::new(node)
                                .with_admin_token("secret".to_string())
                                .with_host(host.clone())
                        })
                    };
                    let api = HostApi::new(host.clone(), factory);
                    tokio::spawn(async move {
                        serve_host(
                            http,
                            api,
                            std::future::pending::<()>(),
                            Duration::from_secs(1),
                            &HttpOptions::default(),
                        )
                        .await
                    });
                    (host, addr, base)
                })
                .await
                .unwrap();
            addrs.insert(id, BasicNode::new(addr));
            processes.push(Process {
                host,
                runtime: Some(runtime),
            });
            bases.push(base);
        }
        let hosts = Hosts {
            _dir: dir,
            processes,
            bases,
        };
        let first = hosts.host(1);
        hosts
            .on(1, async move { first.root().initialize_nodes(addrs).await })
            .await
            .unwrap();
        for id in 1..=4 {
            let host = hosts.host(id);
            hosts.on(id, async move { host.start() }).await;
        }
        // Process 1 leads the root, so killing the leader of `g` leaves
        // the root's leader (and the catalog driver) standing.
        let root = hosts.leader("root", &[1, 2, 3, 4]).await;
        if root != 1 {
            let node = hosts.host(root).root();
            hosts
                .on(root, async move { node.transfer_leadership(1).await })
                .await
                .unwrap();
            assert_eq!(hosts.leader("root", &[1, 2, 3, 4]).await, 1);
        }
        let host = hosts.host(1);
        hosts
            .on(1, async move {
                host.create_group("g", GROUP.to_vec(), "create-g")
                    .await
                    .unwrap()
                    .unwrap();
            })
            .await;
        hosts
            .until("g active", |catalog| {
                catalog
                    .groups
                    .get("g")
                    .is_some_and(|entry| entry.status == GroupStatus::Active)
            })
            .await;
        let host = hosts.host(1);
        hosts
            .on(1, async move {
                host.change_catalog(CatalogChange::CreatePartition {
                    partition: "p".into(),
                    group: "g".into(),
                    op: "create-p".into(),
                })
                .await
                .unwrap()
                .unwrap();
            })
            .await;
        // Every process routes `p` to `g`, and `g` has a leader.
        hosts
            .until("p active", |catalog| {
                catalog
                    .partitions
                    .get("p")
                    .is_some_and(|entry| entry.status == PartitionStatus::Active)
            })
            .await;
        hosts.leader("g", &GROUP).await;
        let client = FlowerClient::builder(&hosts.bases[0])
            .credentials(Credentials::token("secret"))
            .retry(Retry::Default)
            .build()
            .unwrap();
        client
            .deploy(
                "p",
                "app",
                None,
                b"{}",
                &manifest(),
                &RequestOptions::default(),
            )
            .await
            .unwrap();
        hosts
    }

    /// Wait until every process has applied a catalog satisfying `done`.
    async fn until(&self, what: &str, done: impl Fn(&Catalog) -> bool) {
        for _ in 0..4000 {
            let applied = self.processes.iter().all(|process| {
                process
                    .host
                    .applied_catalog()
                    .is_ok_and(|catalog| done(&catalog))
            });
            if applied {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("the catalog never showed {what} everywhere");
    }

    /// Process `id`'s host.
    fn host(&self, id: u64) -> Arc<Host> {
        self.processes[id as usize - 1].host.clone()
    }

    /// Run `work` on process `id`'s runtime, as that process would.
    async fn on<T: Send + 'static>(
        &self,
        id: u64,
        work: impl std::future::Future<Output = T> + Send + 'static,
    ) -> T {
        self.processes[id as usize - 1]
            .runtime
            .as_ref()
            .expect("the process is alive")
            .spawn(work)
            .await
            .unwrap()
    }

    /// The node leading `group` whom every one of `members` follows.
    async fn leader(&self, group: &str, members: &[u64]) -> u64 {
        for _ in 0..4000 {
            for &id in members {
                let Some(node) = self.host(id).group(group) else {
                    continue;
                };
                let metrics = node.raft().metrics().borrow().clone();
                if metrics.state == ServerState::Leader
                    && metrics.current_leader == Some(id)
                    && members.iter().all(|&other| {
                        self.host(other)
                            .group(group)
                            .is_some_and(|node| node.status().leader == Some(id))
                    })
                {
                    return id;
                }
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("group {group} has no agreed leader");
    }

    /// Kill process `id` as a crash would.
    fn kill(&mut self, id: u64) {
        if let Some(runtime) = self.processes[id as usize - 1].runtime.take() {
            runtime.shutdown_background();
        }
    }
}

impl Drop for Hosts {
    fn drop(&mut self) {
        for id in 1..=self.processes.len() as u64 {
            self.kill(id);
        }
    }
}

/// Start four processes, write to `p` for a while through clients of
/// every process, strike the process leading `g` with `fault`, and time
/// the failover.
async fn group_failover_once(fault: Fault) -> Failover {
    let registry = Registry::new();
    let network = match fault {
        Fault::Crash => Peers::Tcp,
        Fault::Partition => Peers::InProcess(registry.clone()),
    };
    let mut hosts = Hosts::start(network).await;
    let acks: Acks = Arc::default();
    let stop = Arc::new(AtomicBool::new(false));
    let writers: Vec<_> = (0..8)
        .map(|index| {
            writer(
                hosts.bases[index % 4].clone(),
                "p",
                format!("w{index}"),
                acks.clone(),
                stop.clone(),
            )
        })
        .collect();
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let leader = hosts.leader("g", &GROUP).await;
    assert!(
        !acks.lock().unwrap().is_empty(),
        "no write was acknowledged before the kill"
    );
    let old = hosts
        .host(leader)
        .group("g")
        .unwrap()
        .raft()
        .metrics()
        .borrow()
        .clone();
    let appended = old.last_log_index.unwrap_or(0);
    let survivors: Vec<Arc<Node>> = GROUP
        .iter()
        .filter(|&&id| id != leader)
        .map(|&id| hosts.host(id).group("g").unwrap())
        .collect();
    let killed = Instant::now();
    match fault {
        Fault::Crash => hosts.kill(leader),
        Fault::Partition => {
            // Every group's traffic, the root's included, between it and
            // every other process.
            for other in (1..=4).filter(|&other| other != leader) {
                registry.block(leader, other);
                registry.block(other, leader);
            }
        }
    }
    // Writers whose only URL is the struck process cannot reach another.
    let reachable = (0..8)
        .filter(|index| index % 4 != (leader - 1) as usize)
        .map(|index| format!("w{index}"))
        .collect();
    let failover = measure(killed, &survivors, &old, appended, &acks, &reachable).await;
    stop.store(true, Ordering::Relaxed);
    for writer in writers {
        writer.abort();
    }
    failover
}

/// Five crashes of the process leading a non-root group while clients
/// of every process (one of which does not host the group) write to its
/// partition: the worst must stay under a second from the kill to an
/// acknowledged write, and the median to every client's, as for the
/// root. Before cross-group forwards passed over members that failed to
/// answer, the clients of the process outside the group took 2.5–3.4 s
/// in every round: it went on dialing the dead leader, which other
/// members kept naming.
#[tokio::test(flavor = "multi_thread")]
async fn clients_write_again_within_a_second_of_a_group_leader_crash() {
    group_worst_under_a_second(Fault::Crash).await;
}

/// Five partitions of the process leading a non-root group: its API
/// still serves, and the writes it takes for `g` must be given up once
/// it cannot reach the group, as the root's are.
#[tokio::test(flavor = "multi_thread")]
async fn clients_write_again_within_a_second_of_a_group_leader_partition() {
    group_worst_under_a_second(Fault::Partition).await;
}

/// Strike the leader of `g` five times with `fault`, each among fresh
/// processes, and check the worst failover.
async fn group_worst_under_a_second(fault: Fault) {
    let _turn = TURNS.lock().await;
    let mut samples = Vec::new();
    for round in 0..5 {
        let failover = group_failover_once(fault).await;
        eprintln!("group {fault:?} {round}: {failover:?}");
        samples.push(failover);
    }
    check_worst(&format!("group {fault:?}"), samples);
}
