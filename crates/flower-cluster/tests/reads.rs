//! Read consistency and routing across members: leader leases, fresh
//! reads on followers through the leader's read fence, replica-local
//! reads with bounded staleness, and requests forwarded to the leader
//! over the member channel.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use flower_cluster::network::{InProcessFactory, PeerConfig, Registry, TcpFactory};
use flower_cluster::node::{Node, NodeError};
use flower_cluster::peer::{
    BoxFuture, ForwardedRequest, ForwardedResponse, Forwarder, MemberError,
};
use flower_cluster::read::{QUORUM, ReadOptions, ReadSettings, STALE_REPLICA, Served};
use flower_cluster::statemachine::RuntimeFactory;
use flower_engine::engine::{CallOptions, MutationOptions};
use flower_engine::runtime::{CallKind, ClosureRuntime, HostCall};
use flower_sdk::abi::{HostOp, decode_envelope};
use flower_sdk::{Failure, Value, fail};
use openraft::{BasicNode, ServerState};

fn s(text: &str) -> Value {
    Value::String(text.to_string())
}

fn obj(pairs: Vec<(&str, Value)>) -> Value {
    Value::Object(
        pairs
            .into_iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect(),
    )
}

fn arr(items: Vec<Value>) -> Value {
    Value::Array(items)
}

fn str_schema() -> Value {
    obj(vec![("type", s("string"))])
}

/// A definition, with a query's declared consistency when given.
fn definition(name: &str, kind: &str, consistency: Option<&str>) -> Value {
    let mut pairs = vec![
        ("name", s(name)),
        ("kind", s(kind)),
        ("args", obj(vec![("type", s("json"))])),
    ];
    if let Some(consistency) = consistency {
        pairs.push(("consistency", s(consistency)));
    }
    obj(pairs)
}

/// A replica-local query declaring its own staleness bound.
fn bounded_definition(name: &str, max_staleness_ms: u32) -> Value {
    let Value::Object(mut map) = definition(name, "query", Some("replica_local")) else {
        unreachable!("a definition is an object");
    };
    map.insert(
        "max_staleness_ms".to_string(),
        Value::Number(f64::from(max_staleness_ms)),
    );
    Value::Object(map)
}

/// One `tasks` collection; `put` writes, `get` reads linearizably,
/// `local` reads replica-locally and `bounded` replica-locally within
/// 100 ms.
fn manifest() -> Value {
    let tasks = obj(vec![
        ("name", s("tasks")),
        ("key", str_schema()),
        (
            "value",
            obj(vec![
                ("type", s("object")),
                (
                    "fields",
                    arr(vec![obj(vec![
                        ("name", s("title")),
                        ("schema", str_schema()),
                        ("optional", Value::Bool(false)),
                    ])]),
                ),
            ]),
        ),
        ("indexes", arr(vec![])),
        ("references", arr(vec![])),
    ]);
    obj(vec![
        ("collections", arr(vec![tasks])),
        (
            "definitions",
            arr(vec![
                definition("put", "mutation", None),
                definition("get", "query", Some("linearizable")),
                definition("local", "query", Some("replica_local")),
                bounded_definition("bounded", 100),
            ]),
        ),
        ("triggers", arr(vec![])),
        ("http", arr(vec![])),
    ])
}

fn hc(host: &mut dyn HostCall, op: HostOp, args: Vec<Value>) -> Result<Value, Failure> {
    let input = Value::Array(args).encode_binary().unwrap();
    let output = host.op(op as u32 as u8, &input);
    decode_envelope(Value::decode_binary(&output).unwrap())
}

/// The test application: `put {key, title}`, `get {key}`, `local {key}`.
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
                let key = map.get("key").cloned().unwrap_or(Value::Null);
                match (kind, name) {
                    (CallKind::Mutation, "put") => {
                        let title = map.get("title").cloned().unwrap_or(Value::Null);
                        hc(
                            host,
                            HostOp::Set,
                            vec![s(""), s("tasks"), key, obj(vec![("title", title)])],
                        )?;
                        Ok(s("put-ok"))
                    }
                    (CallKind::Query, "get" | "local" | "bounded") => {
                        hc(host, HostOp::Get, vec![s(""), s("tasks"), key])
                    }
                    _ => Err(fail("UNKNOWN_METHOD", "unknown definition")),
                }
            },
        )))
    }
}

fn key(name: &str) -> Value {
    s(&format!("\"{name}\""))
}

fn put_args(name: &str, title: &str) -> Value {
    obj(vec![("key", key(name)), ("title", s(title))])
}

fn get_args(name: &str) -> Value {
    obj(vec![("key", key(name))])
}

fn titled(title: &str) -> Value {
    obj(vec![("title", s(title))])
}

/// OpenRaft's fast defaults (50 ms heartbeats, 150-300 ms elections):
/// a 270 ms read lease.
fn config() -> Arc<openraft::Config> {
    Arc::new(openraft::Config::default())
}

/// A three-member in-process cluster whose traffic tests can block.
struct Cluster {
    _dir: tempfile::TempDir,
    registry: Registry,
    nodes: Vec<Node>,
}

impl Cluster {
    async fn start(config: Arc<openraft::Config>) -> Self {
        Self::start_with(config, None).await
    }

    async fn start_with(config: Arc<openraft::Config>, reads: Option<ReadSettings>) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let registry = Registry::new();
        let mut nodes = Vec::new();
        for id in 1..=3u64 {
            let mut node = Node::open(
                id,
                &dir.path().join(format!("n{id}")),
                config.clone(),
                InProcessFactory::new(registry.clone()).with_source(id),
                Arc::new(App),
            )
            .await
            .unwrap();
            if let Some(reads) = reads {
                node = node.with_reads(reads);
            }
            registry.register_peer(node.peer_service());
            nodes.push(node);
        }
        nodes[0]
            .initialize(BTreeSet::from([1, 2, 3]))
            .await
            .unwrap();
        let cluster = Cluster {
            _dir: dir,
            registry,
            nodes,
        };
        let leader = cluster.leader().await;
        leader
            .deploy(b"db", "app", [7u8; 32], b"wasm", &manifest())
            .await
            .unwrap();
        cluster
    }

    fn node(&self, id: u64) -> &Node {
        &self.nodes[(id - 1) as usize]
    }

    /// The member every member agrees leads.
    async fn leader(&self) -> &Node {
        for _ in 0..400 {
            let leaders: BTreeSet<_> = self.nodes.iter().map(|node| node.status().leader).collect();
            if let [Some(leader)] = leaders.into_iter().collect::<Vec<_>>()[..] {
                let node = self.node(leader);
                if leads(node) {
                    return node;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("no agreed leader");
    }

    fn followers(&self, leader: u64) -> Vec<&Node> {
        self.nodes
            .iter()
            .filter(|node| node.id() != leader)
            .collect()
    }

    /// Block traffic between two members both ways.
    fn cut(&self, a: u64, b: u64) {
        self.registry.block(a, b);
        self.registry.block(b, a);
    }
}

/// Whether a member's core says it leads.
fn leads(node: &Node) -> bool {
    let metrics = node.raft().metrics();
    let metrics = metrics.borrow();
    metrics.state == ServerState::Leader && metrics.current_leader == Some(node.id())
}

async fn put(node: &Node, name: &str, title: &str) -> u64 {
    node.mutate(
        b"db",
        "app",
        "put",
        &put_args(name, title),
        &MutationOptions::default(),
    )
    .await
    .unwrap()
    .position
}

async fn read(node: &Node, query: &str, name: &str, bound: Option<Duration>) -> ReadAnswer {
    node.query_read(
        b"db",
        "app",
        query,
        &get_args(name),
        &CallOptions::default(),
        &ReadOptions {
            max_staleness: bound,
            ..ReadOptions::default()
        },
    )
    .await
    .map(|(value, _, served)| (value, served))
}

type ReadAnswer = Result<(Value, Served), NodeError>;

fn failure_code(answer: &ReadAnswer) -> Option<&str> {
    match answer {
        Err(NodeError::Failure { code, .. }) => Some(code),
        _ => None,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn followers_answer_linearizable_reads_through_the_leaders_fence() {
    let cluster = Cluster::start(config()).await;
    let leader = cluster.leader().await;
    put(leader, "a", "first").await;
    // Read right after the write commits, without waiting for followers
    // to apply it: the fence makes them wait.
    for follower in cluster.followers(leader.id()) {
        let (value, served) = read(follower, "get", "a", None).await.unwrap();
        assert_eq!(value, titled("first"), "follower {}", follower.id());
        assert_eq!(served, Served::Fresh);
        assert!(follower.read_metrics().remote_fences >= 1);
    }
    assert!(leader.read_metrics().fences_served >= 2);
    // Linearizable queries read fresh with or without a bound.
    let follower = cluster.followers(leader.id())[0];
    put(leader, "a", "second").await;
    let (value, _) = read(follower, "get", "a", Some(Duration::from_secs(60)))
        .await
        .unwrap();
    assert_eq!(value, titled("second"));
}

#[tokio::test(flavor = "multi_thread")]
async fn the_leaders_lease_answers_fresh_reads_without_quorum_rounds() {
    let cluster = Cluster::start(config()).await;
    let leader = cluster.leader().await;
    put(leader, "a", "first").await;
    read(leader, "get", "a", None).await.unwrap();
    let before = leader.read_metrics();
    for _ in 0..20 {
        let (value, served) = read(leader, "get", "a", None).await.unwrap();
        assert_eq!((value, served), (titled("first"), Served::Fresh));
    }
    let after = leader.read_metrics();
    assert!(
        after.lease_reads >= before.lease_reads + 18,
        "{before:?} -> {after:?}"
    );
    // Background renewal may run a round or two; reads don't wait.
    assert!(
        after.quorum_rounds <= before.quorum_rounds + 2,
        "{before:?} -> {after:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn without_a_lease_each_fresh_read_runs_a_quorum_round() {
    let unleased = ReadSettings {
        lease: None,
        ..ReadSettings::for_config(&config())
    };
    let cluster = Cluster::start_with(config(), Some(unleased)).await;
    let leader = cluster.leader().await;
    assert_eq!(leader.read_settings().lease, None);
    put(leader, "a", "first").await;
    let before = leader.read_metrics();
    for _ in 0..5 {
        read(leader, "get", "a", None).await.unwrap();
    }
    let after = leader.read_metrics();
    assert_eq!(after.lease_reads, before.lease_reads);
    assert_eq!(after.quorum_rounds, before.quorum_rounds + 5);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_lease_as_long_as_election_max_is_cut_to_the_default() {
    let config = config();
    let safe = ReadSettings::for_config(&config);
    assert_eq!(safe.lease, Some(Duration::from_millis(270)));
    let dir = tempfile::tempdir().unwrap();
    let node = Node::open(
        1,
        dir.path(),
        config.clone(),
        InProcessFactory::new(Registry::new()),
        Arc::new(App),
    )
    .await
    .unwrap();
    assert_eq!(node.read_settings(), safe);
    // Voters refuse other candidates for election max: a lease that
    // long could outlive their refusal.
    let node = node.with_reads(ReadSettings {
        lease: Some(Duration::from_millis(config.election_timeout_max)),
        timeout: Duration::from_secs(3),
    });
    assert_eq!(node.read_settings().lease, safe.lease);
    assert_eq!(node.read_settings().timeout, Duration::from_secs(3));
    let node = node.with_reads(ReadSettings {
        lease: Some(Duration::from_millis(100)),
        timeout: Duration::from_secs(3),
    });
    assert_eq!(node.read_settings().lease, Some(Duration::from_millis(100)));
    node.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn an_isolated_leader_stops_lease_reads_before_another_leader_exists() {
    let cluster = Cluster::start(config()).await;
    let old = cluster.leader().await;
    put(old, "a", "first").await;
    read(old, "get", "a", None).await.unwrap();
    for follower in cluster.followers(old.id()) {
        cluster.cut(old.id(), follower.id());
    }
    let isolated = Instant::now();
    // Keep reading at the isolated leader: its lease answers for a
    // while, then nothing does.
    let mut last_lease_read = None;
    let mut new_leader = None;
    let deadline = isolated + Duration::from_secs(10);
    while new_leader.is_none() && Instant::now() < deadline {
        // A read is fresh as of when it began.
        let began = Instant::now();
        match read(old, "get", "a", None).await {
            Ok((value, _)) => {
                assert_eq!(value, titled("first"));
                last_lease_read = Some(began);
            }
            Err(error) => assert!(
                matches!(&error, NodeError::Failure { code, .. } if code == QUORUM)
                    || matches!(error, NodeError::Forward { .. }),
                "{error:?}"
            ),
        }
        new_leader = cluster
            .followers(old.id())
            .into_iter()
            .find(|node| leads(node))
            .map(|node| (node.id(), Instant::now()));
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    let (new_id, elected) = new_leader.expect("the majority elected a leader");
    let last_lease_read = last_lease_read.expect("the lease answered while isolated");
    assert!(
        last_lease_read < elected,
        "lease read {:?} after the new leader was seen {:?}",
        last_lease_read - isolated,
        elected - isolated
    );
    // The majority moves on; the old leader never answers with its
    // state again.
    put(cluster.node(new_id), "a", "second").await;
    for _ in 0..20 {
        let answer = read(old, "get", "a", None).await;
        assert!(answer.is_err(), "{answer:?}");
    }
    // Healed, it learns the new term and reads fresh through its fence.
    cluster.registry.heal();
    let deadline = Instant::now() + Duration::from_secs(10);
    let value = loop {
        match read(old, "get", "a", None).await {
            Ok((value, _)) => break value,
            Err(error) => assert!(Instant::now() < deadline, "{error:?}"),
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    assert_eq!(value, titled("second"));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_leader_that_grants_a_vote_stops_lease_reads_at_once() {
    // A longer lease leaves the old lease's timer running well after
    // the vote it grants.
    let config = Arc::new(openraft::Config {
        heartbeat_interval: 50,
        election_timeout_min: 300,
        election_timeout_max: 600,
        ..openraft::Config::default()
    });
    let cluster = Cluster::start(config).await;
    let old = cluster.leader().await;
    let last = put(old, "a", "first").await;
    let followers = cluster.followers(old.id());
    let (steady, deaf) = (followers[0], followers[1]);
    for node in &cluster.nodes {
        while node.status().last_applied.unwrap_or(0) < last {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
    // The leader's own vote is older than election max by now, so it
    // grants a candidate with its log; `steady` still hears it and
    // refuses.
    tokio::time::sleep(Duration::from_millis(700)).await;
    // `deaf` stops hearing the leader and campaigns; reads keep the
    // leader's lease fresh meanwhile.
    cluster.registry.block(old.id(), deaf.id());
    let lease_reads = old.read_metrics().lease_reads;
    let deadline = Instant::now() + Duration::from_secs(10);
    while !leads(deaf) {
        assert!(Instant::now() < deadline, "the deaf follower never led");
        let _ = read(old, "get", "a", None).await;
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(
        old.read_metrics().lease_reads > lease_reads,
        "the old leader served lease reads up to the vote"
    );
    // The old leader stepped down granting that vote. Keep the new
    // leader's writes from reaching it, then write.
    cluster.registry.block(deaf.id(), old.id());
    put(deaf, "a", "second").await;
    assert_ne!(steady.status().leader, Some(old.id()));
    // Its lease's timer still runs, but the vote it granted ended it:
    // the core, not the timer or the metrics, says whether it leads.
    let confirmed = old.ensure_leader().await;
    assert!(
        matches!(confirmed, Err(NodeError::Forward { .. })),
        "a leader that granted a vote confirmed its leadership: {confirmed:?}"
    );
    for _ in 0..10 {
        let answer = read(old, "get", "a", None).await;
        if let Ok((value, _)) = &answer {
            assert_eq!(value, &titled("second"), "stale read after the vote");
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn replica_local_reads_serve_a_cut_off_follower_within_their_bound() {
    let cluster = Cluster::start(config()).await;
    let leader = cluster.leader().await;
    let last = put(leader, "a", "first").await;
    let follower = cluster.followers(leader.id())[0];
    while follower.status().last_applied.unwrap_or(0) < last {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    // A healthy follower knows how stale it is: about a heartbeat.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let (value, served) = read(follower, "local", "a", None).await.unwrap();
    assert_eq!(value, titled("first"));
    match served {
        Served::Local(Some(staleness)) => {
            assert!(staleness < Duration::from_secs(1), "{staleness:?}")
        }
        other => panic!("{other:?}"),
    }
    // Within its declared bound, a bounded query answers locally too.
    let (value, served) = read(follower, "bounded", "a", None).await.unwrap();
    assert_eq!(value, titled("first"));
    assert!(matches!(served, Served::Local(Some(_))), "{served:?}");

    for other in &cluster.nodes {
        if other.id() != follower.id() {
            cluster.cut(follower.id(), other.id());
        }
    }
    put(leader, "a", "second").await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    // Unbounded, the cut-off follower answers from its own state.
    let (value, served) = read(follower, "local", "a", None).await.unwrap();
    assert_eq!(value, titled("first"));
    let Served::Local(Some(staleness)) = served else {
        panic!("{served:?}");
    };
    assert!(staleness >= Duration::from_millis(250), "{staleness:?}");
    // A loose bound still lets it answer; a tight one makes it prove a
    // fresh read, which it cannot.
    let (value, _) = read(follower, "local", "a", Some(Duration::from_secs(60)))
        .await
        .unwrap();
    assert_eq!(value, titled("first"));
    let answer = read(follower, "local", "a", Some(Duration::from_millis(100))).await;
    assert_eq!(failure_code(&answer), Some(STALE_REPLICA), "{answer:?}");
    // A query's declared bound holds without the caller's, and a
    // looser caller bound cannot loosen it.
    for asked in [None, Some(Duration::from_secs(60))] {
        let answer = read(follower, "bounded", "a", asked).await;
        assert_eq!(failure_code(&answer), Some(STALE_REPLICA), "{answer:?}");
    }
    // Linearizable reads cannot be served there at all.
    let answer = read(follower, "get", "a", None).await;
    assert!(answer.is_err(), "{answer:?}");

    // Reconnected, a zero bound proves a fresh read and reports zero.
    cluster.registry.heal();
    let (value, served) = read(follower, "local", "a", Some(Duration::ZERO))
        .await
        .unwrap();
    assert_eq!(value, titled("second"));
    assert_eq!(served, Served::Local(Some(Duration::ZERO)));
}

/// A forwarder standing in for the leader's API.
struct Echo {
    id: u64,
}

impl Forwarder for Echo {
    fn forward(
        &self,
        request: ForwardedRequest,
    ) -> BoxFuture<Result<ForwardedResponse, MemberError>> {
        let id = self.id;
        Box::pin(async move {
            Ok(ForwardedResponse {
                status: 200,
                headers: vec![("content-type".to_string(), "application/json".to_string())],
                body: format!("{{\"served_by\":{id},\"path\":\"{}\"}}", request.path),
            })
        })
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn tcp_followers_read_fresh_and_forward_to_the_leader() {
    let dir = tempfile::tempdir().unwrap();
    let peers = PeerConfig::with_token("peer-secret");
    let mut nodes = Vec::new();
    let mut members = BTreeMap::new();
    let mut servers = Vec::new();
    for id in 1..=2u64 {
        let node = Node::open(
            id,
            &dir.path().join(format!("n{id}")),
            config(),
            TcpFactory::new(id, peers.clone()),
            Arc::new(App),
        )
        .await
        .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        members.insert(
            id,
            BasicNode {
                addr: listener.local_addr().unwrap().to_string(),
            },
        );
        servers.push(node.serve_listener(listener, peers.clone()));
        nodes.push(node);
    }
    nodes[0].initialize_nodes(members).await.unwrap();
    let mut leader = None;
    for _ in 0..400 {
        leader = nodes.iter().find(|node| leads(node));
        if leader.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let leader = leader.expect("a leader");
    let follower = nodes.iter().find(|node| node.id() != leader.id()).unwrap();
    leader
        .deploy(b"db", "app", [7u8; 32], b"wasm", &manifest())
        .await
        .unwrap();
    put(leader, "a", "first").await;
    let (value, served) = read(follower, "get", "a", None).await.unwrap();
    assert_eq!((value, served), (titled("first"), Served::Fresh));
    assert!(leader.read_metrics().fences_served >= 1);
    // Appends heard over TCP keep the follower's staleness known.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let staleness = follower.staleness().expect("known staleness");
    assert!(staleness < Duration::from_secs(1), "{staleness:?}");

    // Without an API at the leader, a forward reports it as such.
    let request = ForwardedRequest {
        method: "POST".to_string(),
        path: "/v1/mutate".to_string(),
        headers: Vec::new(),
        body: "{}".to_string(),
    };
    assert!(matches!(
        follower.forward(request.clone()).await,
        Err(NodeError::Forward { .. })
    ));
    let echo: Arc<dyn Forwarder> = Arc::new(Echo { id: leader.id() });
    leader.member_desk().set_forwarder(Arc::downgrade(&echo));
    let answered = follower.forward(request).await.unwrap();
    assert_eq!(answered.status, 200);
    assert_eq!(
        answered.body,
        format!("{{\"served_by\":{},\"path\":\"/v1/mutate\"}}", leader.id())
    );
    // The leader describes where its API serves.
    leader.set_api_url(Some("http://leader.example".to_string()));
    let mut described = None;
    for _ in 0..200 {
        described = follower.leader_api_url();
        if described.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(described.as_deref(), Some("http://leader.example"));
    for server in servers {
        server.abort();
    }
}

/// A forwarder standing in for a leader's API that holds every request
/// until let through.
struct Gate {
    arrived: Arc<std::sync::atomic::AtomicUsize>,
    open: Arc<tokio::sync::Semaphore>,
}

impl Forwarder for Gate {
    fn forward(
        &self,
        _request: ForwardedRequest,
    ) -> BoxFuture<Result<ForwardedResponse, MemberError>> {
        let (arrived, open) = (self.arrived.clone(), self.open.clone());
        Box::pin(async move {
            arrived.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let _through = open.acquire().await;
            Ok(ForwardedResponse {
                status: 200,
                headers: Vec::new(),
                body: "{}".to_string(),
            })
        })
    }
}

/// Forwarded client requests hold at most half a member's peer
/// connections: past that, the leader's listener answers at once as
/// its admission answers an overload (503 `overloaded`, `Retry-After`),
/// instead of serving them on connections its Raft traffic needs. A
/// flood of forwards filled every connection, and heartbeats and votes
/// found none.
#[tokio::test(flavor = "multi_thread")]
async fn forwards_past_half_the_connection_cap_are_answered_overloaded_at_once() {
    use std::sync::atomic::Ordering;
    let dir = tempfile::tempdir().unwrap();
    let peers = PeerConfig::with_token("peer-secret");
    // Two connections: one forward at a time.
    let listening = PeerConfig {
        max_connections: 2,
        ..peers.clone()
    };
    let mut nodes = Vec::new();
    let mut members = BTreeMap::new();
    let mut servers = Vec::new();
    for id in 1..=2u64 {
        let node = Node::open(
            id,
            &dir.path().join(format!("n{id}")),
            config(),
            TcpFactory::new(id, peers.clone()),
            Arc::new(App),
        )
        .await
        .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        members.insert(
            id,
            BasicNode {
                addr: listener.local_addr().unwrap().to_string(),
            },
        );
        servers.push(node.serve_listener(listener, listening.clone()));
        nodes.push(node);
    }
    nodes[0].initialize_nodes(members).await.unwrap();
    let mut leader = None;
    for _ in 0..400 {
        leader = nodes.iter().find(|node| leads(node));
        if leader.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let leader = leader.expect("a leader");
    let follower = nodes.iter().find(|node| node.id() != leader.id()).unwrap();
    leader
        .deploy(b"db", "app", [7u8; 32], b"wasm", &manifest())
        .await
        .unwrap();
    put(leader, "a", "first").await;
    for _ in 0..400 {
        if follower.status().leader == Some(leader.id()) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let gate = Gate {
        arrived: Arc::default(),
        open: Arc::new(tokio::sync::Semaphore::new(0)),
    };
    let (arrived, open) = (gate.arrived.clone(), gate.open.clone());
    let gate: Arc<dyn Forwarder> = Arc::new(gate);
    leader.member_desk().set_forwarder(Arc::downgrade(&gate));
    let request = ForwardedRequest {
        method: "POST".to_string(),
        path: "/v1/mutate".to_string(),
        headers: Vec::new(),
        body: "{}".to_string(),
    };

    // The first forward reaches the leader's API and waits there.
    let first = follower.forward(request.clone());
    tokio::pin!(first);
    while arrived.load(Ordering::SeqCst) == 0 {
        tokio::select! {
            answered = &mut first => panic!("the first forward was answered: {answered:?}"),
            () = tokio::time::sleep(Duration::from_millis(10)) => {}
        }
    }
    // The second finds no room for forwards: it is answered at once,
    // never reaching the API.
    let second = tokio::time::timeout(Duration::from_secs(2), follower.forward(request))
        .await
        .expect("the second forward waited")
        .unwrap();
    assert_eq!(second.status, 503);
    let body: serde_json::Value = serde_json::from_str(&second.body).unwrap();
    assert_eq!(body["code"], "overloaded");
    assert!(
        second
            .headers
            .iter()
            .any(|(name, value)| name == "retry-after" && value == "1"),
        "{:?}",
        second.headers
    );
    assert_eq!(arrived.load(Ordering::SeqCst), 1);
    // Raft goes on meanwhile, and the first forward is answered once
    // let through.
    put(leader, "b", "second").await;
    open.add_permits(1);
    assert_eq!(first.await.unwrap().status, 200);
    for server in servers {
        server.abort();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_released_lease_stays_off_for_the_rest_of_the_term() {
    let cluster = Cluster::start(config()).await;
    let leader = cluster.leader().await;
    put(leader, "a", "first").await;
    read(leader, "get", "a", None).await.unwrap();
    read(leader, "get", "a", None).await.unwrap();
    assert!(leader.read_metrics().lease_reads >= 1);
    // A leadership transfer releases the lease before the target
    // campaigns; rounds still answer, but confirm no lease this term.
    leader.release_read_lease();
    let before = leader.read_metrics();
    for _ in 0..5 {
        let (value, served) = read(leader, "get", "a", None).await.unwrap();
        assert_eq!((value, served), (titled("first"), Served::Fresh));
    }
    let after = leader.read_metrics();
    assert_eq!(
        after.lease_reads, before.lease_reads,
        "{before:?} -> {after:?}"
    );
    assert_eq!(after.quorum_rounds, before.quorum_rounds + 5);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_leadership_transfer_releases_the_old_leaders_lease_before_the_target_campaigns() {
    // A longer lease, and a leader too new for its own vote to lapse at
    // once: the transfer waits, its lease's timer running meanwhile.
    let config = Arc::new(openraft::Config {
        heartbeat_interval: 50,
        election_timeout_min: 300,
        election_timeout_max: 600,
        ..openraft::Config::default()
    });
    let cluster = Cluster::start(config).await;
    let old = cluster.leader().await;
    let to = cluster.followers(old.id())[0].id();
    put(old, "a", "first").await;
    read(old, "get", "a", None).await.unwrap();
    read(old, "get", "a", None).await.unwrap();
    assert!(old.read_metrics().lease_reads >= 1);
    let transfer = old.transfer_leadership(to);
    let during = async {
        // Writes answer `leadership_transfer` once the transfer began.
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            assert!(Instant::now() < deadline, "the transfer never began");
            let written = old
                .mutate(
                    b"db",
                    "app",
                    "put",
                    &put_args("b", "during"),
                    &MutationOptions::default(),
                )
                .await;
            if let Err(NodeError::Failure { code, .. }) = &written
                && code == flower_cluster::node::TRANSFERRING
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        // The target campaigns at once, ignoring its own refusal: from
        // here no read may lean on the old lease.
        let before = old.read_metrics();
        for _ in 0..5 {
            let _ = read(old, "get", "a", None).await;
        }
        (before, old.read_metrics())
    };
    let (moved, (before, after)) = tokio::join!(transfer, during);
    assert_eq!(moved.unwrap().to, to);
    assert_eq!(
        after.lease_reads, before.lease_reads,
        "the old leader served lease reads during the transfer: {before:?} -> {after:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rejoining_member_answers_no_replica_local_read() {
    let mut cluster = Cluster::start(config()).await;
    let leader = cluster.leader().await.id();
    let written = put(cluster.node(leader), "a", "first").await;
    let wiped = cluster.followers(leader)[0].id();
    // Wipe a follower and bring it back under its ID. Its leader
    // replicates to it, but it cannot reach anyone itself, so it stays
    // rejoining (see `flower_cluster::rejoin`).
    let index = (wiped - 1) as usize;
    cluster.registry.unregister(wiped);
    cluster.nodes.remove(index).shutdown().await.unwrap();
    let path = cluster._dir.path().join(format!("n{wiped}"));
    std::fs::remove_dir_all(&path).unwrap();
    let back = Node::open(
        wiped,
        &path,
        config(),
        InProcessFactory::new(Registry::new()).with_source(wiped),
        Arc::new(App),
    )
    .await
    .unwrap();
    assert!(back.rejoining());
    cluster.registry.register_peer(back.peer_service());
    cluster.nodes.insert(index, back);
    let back = cluster.node(wiped);
    let deadline = Instant::now() + Duration::from_secs(10);
    while back.status().leader != Some(leader) || back.status().last_applied < Some(written) {
        assert!(Instant::now() < deadline, "the wiped member never followed");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // It hears its leader, so its staleness clock reads recent, but its
    // state is whatever it caught up to so far: it answers none of it.
    assert!(back.rejoining());
    assert_eq!(back.staleness(), None);
    for (query, bound) in [
        ("local", None),
        ("local", Some(Duration::from_secs(60))),
        ("bounded", None),
    ] {
        let answer = read(back, query, "a", bound).await;
        assert_eq!(
            failure_code(&answer),
            Some(flower_cluster::rejoin::REJOINING),
            "{query} {bound:?}: {answer:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_recovering_member_answers_no_replica_local_read() {
    use flower_cluster::logstore::LogStore;
    use openraft::storage::{RaftLogStorage, RaftLogStorageExt};
    use openraft::{Entry, EntryPayload, LogId, Membership};

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("n1");
    let open = || async {
        let registry = Registry::new();
        let node = Node::open(
            1,
            &path,
            config(),
            InProcessFactory::new(registry.clone()).with_source(1),
            Arc::new(App),
        )
        .await
        .unwrap();
        registry.register_peer(node.peer_service());
        (registry, node)
    };
    {
        let (_registry, node) = open().await;
        node.initialize(BTreeSet::from([1])).await.unwrap();
        while !leads(&node) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        node.deploy(b"db", "app", [7u8; 32], b"wasm", &manifest())
            .await
            .unwrap();
        put(&node, "a", "first").await;
        let (value, _) = read(&node, "local", "a", None).await.unwrap();
        assert_eq!(value, titled("first"));
        node.shutdown().await.unwrap();
    }
    // Its log now runs past its state: a membership naming two members
    // that never come back and an entry, both committed, then one more
    // its group never confirmed (as `tests/recovery.rs` builds it).
    let mut logs = LogStore::open(&path.join("raft.redb")).unwrap();
    let last = logs.get_log_state().await.unwrap().last_log_id.unwrap();
    let next = |by: u64| LogId::new(last.leader_id, last.index + by);
    let voters = BTreeMap::from([1, 2, 3].map(|id| (id, BasicNode::default())));
    let membership = Entry {
        log_id: next(1),
        payload: EntryPayload::Membership(Membership::new(
            vec![voters.keys().copied().collect()],
            voters,
        )),
    };
    let blank = |id| Entry {
        log_id: id,
        payload: EntryPayload::Blank,
    };
    logs.blocking_append([membership, blank(next(2))])
        .await
        .unwrap();
    logs.save_committed(Some(next(2))).await.unwrap();
    logs.blocking_append([blank(next(3))]).await.unwrap();
    logs.stop_purging().await;
    drop(logs);

    let (_registry, node) = open().await;
    assert!(node.recovering().is_some());
    assert_eq!(node.staleness(), None);
    for (query, bound) in [("local", None), ("bounded", None)] {
        let answer = read(&node, query, "a", bound).await;
        assert_eq!(
            failure_code(&answer),
            Some(flower_cluster::recovery::RECOVERING),
            "{query}: {answer:?}"
        );
    }
    node.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_leadership_restored_on_reopening_confirms_no_lease() {
    // Before a restart the leader may have asked voters to release their
    // leases on it for a transfer; they remember, nothing persists it.
    // So the leadership OpenRaft restores from the files (a sole voter's
    // here, in the same term) proves every fresh read with a round.
    let dir = tempfile::tempdir().unwrap();
    let open = async || {
        Node::open(
            1,
            dir.path(),
            config(),
            InProcessFactory::new(Registry::new()),
            Arc::new(App),
        )
        .await
        .unwrap()
    };
    let node = open().await;
    node.initialize(BTreeSet::from([1])).await.unwrap();
    while !leads(&node) {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    node.deploy(b"db", "app", [7u8; 32], b"wasm", &manifest())
        .await
        .unwrap();
    put(&node, "a", "first").await;
    for _ in 0..3 {
        read(&node, "get", "a", None).await.unwrap();
    }
    assert!(node.read_metrics().lease_reads >= 1);
    let term = node.status().term;
    node.shutdown().await.unwrap();

    let node = open().await;
    while !leads(&node) {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(node.status().term, term, "restored, not re-elected");
    for _ in 0..5 {
        let (value, served) = read(&node, "get", "a", None).await.unwrap();
        assert_eq!((value, served), (titled("first"), Served::Fresh));
    }
    let metrics = node.read_metrics();
    assert_eq!(metrics.lease_reads, 0, "{metrics:?}");
    assert_eq!(metrics.quorum_rounds, 5, "{metrics:?}");
    node.shutdown().await.unwrap();
}
