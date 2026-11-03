//! Membership and peer protocol: validated membership changes,
//! leadership transfer, rejoin after a wiped directory, peer identity
//! and compatibility on the wire, and the follower replication budget.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use flower_cluster::compat::contract;
use flower_cluster::logstore::LogStore;
use flower_cluster::network::{InProcessFactory, PeerConfig, PeerFactory, Registry, TcpFactory};
use flower_cluster::node::{
    MEMBERSHIP, MembershipChange, Node, NodeError, TRANSFER_FAILED, TRANSFERRING,
};
use flower_cluster::rejoin::REJOINING;
use flower_cluster::statemachine::RuntimeFactory;
use flower_cluster::tls::PeerTls;
use flower_engine::engine::MutationOptions;
use flower_engine::runtime::{CallKind, ClosureRuntime, HostCall};
use flower_sdk::abi::{HostOp, decode_envelope};
use flower_sdk::{Value, fail};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::VoteRequest;
use openraft::storage::RaftLogStorage;
use openraft::{BasicNode, CommittedLeaderId, LogId, Vote};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

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

/// A module with one collection and a `put` mutation.
fn manifest() -> Value {
    let string = obj(vec![("type", s("string"))]);
    let tasks = obj(vec![
        ("name", s("tasks")),
        ("key", string.clone()),
        ("value", obj(vec![("type", s("json"))])),
        ("indexes", Value::Array(vec![])),
        ("references", Value::Array(vec![])),
    ]);
    let put = obj(vec![
        ("name", s("put")),
        ("kind", s("mutation")),
        ("args", obj(vec![("type", s("json"))])),
    ]);
    obj(vec![
        ("collections", Value::Array(vec![tasks])),
        ("definitions", Value::Array(vec![put])),
        ("triggers", Value::Array(vec![])),
        ("http", Value::Array(vec![])),
    ])
}

/// Runtimes whose `put` stores `{key, value}` in `tasks`.
struct Puts;

impl RuntimeFactory for Puts {
    fn build(
        &self,
        _db: &[u8],
        _module: &str,
        _wasm: &[u8],
        _manifest: &Value,
    ) -> Result<Box<dyn flower_engine::runtime::Runtime>, String> {
        Ok(Box::new(ClosureRuntime::new(
            |kind: CallKind, _module: &str, name: &str, args: &Value, host: &mut dyn HostCall| {
                if !matches!((kind, name), (CallKind::Mutation, "put")) {
                    return Err(fail("UNKNOWN_METHOD", "unknown definition"));
                }
                let Value::Object(map) = args else {
                    return Err(fail("TYPE_MISMATCH", "args take an object"));
                };
                let key = map.get("key").cloned().unwrap_or(Value::Null);
                let value = map.get("value").cloned().unwrap_or(Value::Null);
                let input = Value::Array(vec![s(""), s("tasks"), key, value])
                    .encode_binary()
                    .unwrap();
                let output = host.op(HostOp::Set as u32 as u8, &input);
                decode_envelope(Value::decode_binary(&output).unwrap())?;
                Ok(Value::Null)
            },
        )))
    }
}

fn runtimes() -> Arc<dyn RuntimeFactory> {
    Arc::new(Puts)
}

/// Fast OpenRaft timing: 50 ms heartbeats, 150–300 ms elections.
fn config() -> Arc<openraft::Config> {
    Arc::new(openraft::Config::default())
}

/// The lease a voter holds on its leader: the longest election timeout.
fn lease() -> Duration {
    Duration::from_millis(config().election_timeout_max)
}

/// Timing with as wide an election window as production's: 50 ms
/// heartbeats, 400–800 ms elections. A voter whose lease ran out waits
/// at least 400 ms more before it campaigns.
fn wide() -> Arc<openraft::Config> {
    Arc::new(openraft::Config {
        election_timeout_min: 400,
        election_timeout_max: 800,
        ..openraft::Config::default()
    })
}

async fn open(id: u64, dir: &Path, registry: &Registry) -> Node {
    open_with(id, dir, registry, config()).await
}

/// Open (or reopen, right after its shutdown) member `id` and register it.
async fn open_with(
    id: u64,
    dir: &Path,
    registry: &Registry,
    config: Arc<openraft::Config>,
) -> Node {
    let node = Node::open(
        id,
        &dir.join(format!("n{id}")),
        config,
        InProcessFactory::new(registry.clone()),
        runtimes(),
    )
    .await
    .unwrap();
    registry.register_peer(node.peer_service());
    node
}

/// Remove a member's directory: its shutdown released the files.
async fn wipe(dir: &Path, id: u64) {
    std::fs::remove_dir_all(dir.join(format!("n{id}"))).unwrap();
}

/// `count` in-process members, all voters, with a leader elected and
/// the `app` module deployed.
async fn cluster(dir: &Path, count: u64) -> (Registry, Vec<Node>) {
    cluster_with(dir, count, config()).await
}

/// [`cluster`] with this timing.
async fn cluster_with(
    dir: &Path,
    count: u64,
    config: Arc<openraft::Config>,
) -> (Registry, Vec<Node>) {
    let registry = Registry::new();
    let mut nodes = Vec::new();
    for id in 1..=count {
        nodes.push(open_with(id, dir, &registry, config.clone()).await);
    }
    nodes[0]
        .initialize((1..=count).collect::<BTreeSet<_>>())
        .await
        .unwrap();
    let leader = wait_leader(&nodes).await;
    leader_of(&nodes, leader)
        .deploy(b"db", "app", [7u8; 32], b"wasm", &manifest())
        .await
        .unwrap();
    // Every member caught up: none is rejoining after its empty start.
    for node in &nodes {
        wait_until(|| !node.rejoining(), "every member to catch up").await;
    }
    (registry, nodes)
}

fn leader_of(nodes: &[Node], id: u64) -> &Node {
    nodes.iter().find(|node| node.id() == id).unwrap()
}

async fn wait_until(condition: impl Fn() -> bool, what: &str) {
    for _ in 0..200 {
        if condition() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for {what}");
}

/// The leader every live member agrees on.
async fn wait_leader(nodes: &[Node]) -> u64 {
    for _ in 0..200 {
        let leaders: BTreeSet<Option<u64>> =
            nodes.iter().map(|node| node.status().leader).collect();
        if let [Some(leader)] = leaders.into_iter().collect::<Vec<_>>()[..] {
            return leader;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("no agreed leader");
}

async fn put(node: &Node, key: &str) -> Result<u64, NodeError> {
    let args = obj(vec![("key", s(&format!("\"{key}\""))), ("value", s(key))]);
    let response = node
        .mutate(b"db", "app", "put", &args, &MutationOptions::default())
        .await?;
    Ok(response.position)
}

fn code(error: &NodeError) -> &str {
    match error {
        NodeError::Failure { code, .. } => code,
        _ => panic!("expected a coded failure, got {error:?}"),
    }
}

// ---------------------------------------------------------------- membership

#[tokio::test(flavor = "multi_thread")]
async fn membership_changes_check_the_expected_log_id_and_new_members() {
    let dir = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let one = open(1, dir.path(), &registry).await;
    one.initialize(BTreeSet::from([1])).await.unwrap();
    wait_until(|| one.status().leader == Some(1), "node 1 to lead").await;
    let two = open(2, dir.path(), &registry).await;
    let before = one.membership();
    assert_eq!(before.voter_configs, vec![BTreeSet::from([1])]);

    // Planned against the current membership: committed, and node 2
    // caught up before it counted toward the quorum.
    let change = MembershipChange {
        members: BTreeMap::from([(1, String::new()), (2, String::new())]),
        expected_log_id: before.log_id,
        retain: false,
    };
    let after = one.reconfigure(change.clone()).await.unwrap();
    assert_eq!(after.voter_configs, vec![BTreeSet::from([1, 2])]);
    assert!(after.log_id > before.log_id);
    assert!(two.status().last_log >= before.log_id.map(|log| log.index));

    // The same plan again is stale: refused, and nothing changes.
    let error = one.reconfigure(change).await.unwrap_err();
    assert_eq!(code(&error), MEMBERSHIP);
    assert!(error.to_string().contains("membership changed"), "{error}");
    assert_eq!(one.membership().log_id, after.log_id);

    // A member of another cluster cannot join: it holds Raft state.
    let other = open(3, dir.path(), &Registry::new()).await;
    registry.register_peer(other.peer_service());
    other.initialize(BTreeSet::from([3])).await.unwrap();
    let error = one
        .reconfigure(MembershipChange {
            members: BTreeMap::from([(1, String::new()), (2, String::new()), (3, String::new())]),
            ..MembershipChange::default()
        })
        .await
        .unwrap_err();
    assert_eq!(code(&error), MEMBERSHIP);
    assert!(
        error.to_string().contains("already holds Raft state"),
        "{error}"
    );
    assert_eq!(one.membership().voter_configs, vec![BTreeSet::from([1, 2])]);

    // Followers forward.
    let error = two
        .reconfigure(MembershipChange {
            members: BTreeMap::from([(1, String::new())]),
            ..MembershipChange::default()
        })
        .await
        .unwrap_err();
    assert!(
        matches!(error, NodeError::Forward { leader: Some(1) }),
        "{error:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn members_keep_their_address_and_addresses_their_member() {
    let dir = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let one = open(1, dir.path(), &registry).await;
    one.initialize_nodes(BTreeMap::from([(1, BasicNode::new("n1:7001"))]))
        .await
        .unwrap();
    wait_until(|| one.status().leader == Some(1), "node 1 to lead").await;
    let _two = open(2, dir.path(), &registry).await;
    let _three = open(3, dir.path(), &registry).await;
    let members = |pairs: &[(u64, &str)]| MembershipChange {
        members: pairs
            .iter()
            .map(|(id, addr)| (*id, addr.to_string()))
            .collect(),
        ..MembershipChange::default()
    };
    one.reconfigure(members(&[(1, "n1:7001"), (2, "n2:7002")]))
        .await
        .unwrap();
    // An existing ID cannot move: the member is replaced instead.
    let error = one
        .reconfigure(members(&[(1, "n1:7001"), (2, "n2:9999")]))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("cannot move"), "{error}");
    // Nor can a new ID take an existing member's address.
    let error = one
        .reconfigure(members(&[(1, "n1:7001"), (3, "n2:7002")]))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("belongs to node 2"), "{error}");
    let error = one
        .reconfigure(members(&[(1, "n1:7001"), (2, "n2:7002"), (3, "n2:7002")]))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("two nodes"), "{error}");
    assert_eq!(one.membership().voter_configs, vec![BTreeSet::from([1, 2])]);
}

// ---------------------------------------------------------- peer identity

/// Bind a TCP member: its node, listener address and serving task.
async fn tcp_node(
    id: u64,
    dir: &Path,
    config: PeerConfig,
) -> (Node, String, tokio::task::JoinHandle<()>) {
    let node = Node::open(
        id,
        &dir.join(format!("n{id}")),
        self::config(),
        TcpFactory::new(id, config.clone()),
        runtimes(),
    )
    .await
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let serving = node.serve_listener(listener, config);
    (node, addr, serving)
}

#[tokio::test(flavor = "multi_thread")]
async fn an_address_answering_as_another_node_never_counts_as_a_voter() {
    // Node 1's membership says node 2 is at node 3's address. Without
    // identities on the wire, node 3 (empty, so willing) would vote for
    // node 1 and take its entries as node 2: one process counted as a
    // voter it is not.
    let dir = tempfile::tempdir().unwrap();
    let peers = PeerConfig::with_token("secret");
    let (one, addr1, _serve1) = tcp_node(1, dir.path(), peers.clone()).await;
    let (three, addr3, _serve3) = tcp_node(3, dir.path(), peers.clone()).await;
    one.initialize_nodes(BTreeMap::from([
        (1, BasicNode::new(&addr1)),
        (2, BasicNode::new(&addr3)),
    ]))
    .await
    .unwrap();
    tokio::time::sleep(lease() * 6).await;
    assert_eq!(one.status().leader, None);
    assert_eq!(three.status().last_log, None);
    assert_eq!(three.status().term, 0);

    // The probe names the mismatch; the right ID answers.
    let probe = TcpFactory::new(9, peers).peers();
    let error = probe
        .info(2, &addr3, Duration::from_secs(5))
        .await
        .unwrap_err();
    assert!(error.contains("is node 3, not node 2"), "{error}");
    let info = probe.info(3, &addr3, Duration::from_secs(5)).await.unwrap();
    assert_eq!(info.id, 3);
    assert!(!info.initialized);
    assert!(info.rejoining);
    assert_eq!(info.compatibility.contract(), contract());
}

/// A listener that answers every hello as `id` running `contract`.
async fn fake_member(id: u64, contract: &'static str) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            // Magic, version, token length, token, IDs, group, uid,
            // contract.
            let mut head = [0u8; 7];
            socket.read_exact(&mut head).await.unwrap();
            let mut rest = vec![0u8; u16::from_be_bytes([head[5], head[6]]) as usize + 16];
            socket.read_exact(&mut rest).await.unwrap();
            let group = socket.read_u8().await.unwrap() as usize;
            let mut group = vec![0u8; group + 8];
            socket.read_exact(&mut group).await.unwrap();
            let len = socket.read_u16().await.unwrap() as usize;
            let mut theirs = vec![0u8; len];
            socket.read_exact(&mut theirs).await.unwrap();
            // The dialer's largest entry.
            socket.read_u64().await.unwrap();
            let mut ack = vec![3u8];
            ack.extend_from_slice(&id.to_be_bytes());
            ack.extend_from_slice(&(contract.len() as u16).to_be_bytes());
            ack.extend_from_slice(contract.as_bytes());
            socket.write_all(&ack).await.unwrap();
        }
    });
    addr
}

#[tokio::test(flavor = "multi_thread")]
async fn incompatible_builds_neither_replicate_nor_join() {
    let dir = tempfile::tempdir().unwrap();
    let peers = PeerConfig::with_token("secret");
    let (one, addr1, _serve) = tcp_node(1, dir.path(), peers.clone()).await;
    one.initialize_nodes(BTreeMap::from([(1, BasicNode::new(&addr1))]))
        .await
        .unwrap();
    wait_until(|| one.status().leader == Some(1), "node 1 to lead").await;
    let other = fake_member(2, "wire1-state1-snapshot1-value1-wasmtime-1.0.0").await;
    let error = one
        .reconfigure(MembershipChange {
            members: BTreeMap::from([(1, addr1.clone()), (2, other.clone())]),
            ..MembershipChange::default()
        })
        .await
        .unwrap_err();
    assert_eq!(code(&error), MEMBERSHIP);
    assert!(error.to_string().contains("wasmtime-1.0.0"), "{error}");
    assert_eq!(one.membership().nodes.len(), 1);

    // Our own listener refuses a dialer of another contract, and one
    // that dialed the wrong node, before reading any request.
    for (target, contract, status) in [(1u64, "wire0-other", 3u8), (5, contract(), 2)] {
        let mut socket = tokio::net::TcpStream::connect(&addr1).await.unwrap();
        let mut hello = b"FLRP\x04".to_vec();
        hello.extend_from_slice(&6u16.to_be_bytes());
        hello.extend_from_slice(b"secret");
        hello.extend_from_slice(&9u64.to_be_bytes());
        hello.extend_from_slice(&target.to_be_bytes());
        hello.extend_from_slice(b"\x04root");
        hello.extend_from_slice(&0u64.to_be_bytes());
        hello.extend_from_slice(&(contract.len() as u16).to_be_bytes());
        hello.extend_from_slice(contract.as_bytes());
        hello.extend_from_slice(&(1024u64 * 1024).to_be_bytes());
        socket.write_all(&hello).await.unwrap();
        assert_eq!(socket.read_u8().await.unwrap(), status);
        assert_eq!(socket.read_u64().await.unwrap(), 1);
        let len = socket.read_u16().await.unwrap() as usize;
        let mut theirs = vec![0u8; len];
        socket.read_exact(&mut theirs).await.unwrap();
        // Then the connection closes, whatever is sent.
        let _ = socket.write_all(&8u64.to_be_bytes()).await;
        let mut rest = Vec::new();
        let _ = socket.read_to_end(&mut rest).await;
        assert!(rest.is_empty());
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_follower_out_of_room_refuses_large_frames_and_stays_in_step() {
    let dir = tempfile::tempdir().unwrap();
    let peers = PeerConfig {
        max_frame: 4 * 1024 * 1024,
        max_queued_bytes: Some(512 * 1024),
        ..PeerConfig::with_token("secret")
    };
    let (_node, addr, _serve) = tcp_node(1, dir.path(), peers).await;
    let mut socket = tokio::net::TcpStream::connect(&addr).await.unwrap();
    let mut hello = b"FLRP\x04".to_vec();
    hello.extend_from_slice(&6u16.to_be_bytes());
    hello.extend_from_slice(b"secret");
    hello.extend_from_slice(&9u64.to_be_bytes());
    hello.extend_from_slice(&1u64.to_be_bytes());
    hello.extend_from_slice(b"\x04root");
    hello.extend_from_slice(&0u64.to_be_bytes());
    hello.extend_from_slice(&(contract().len() as u16).to_be_bytes());
    hello.extend_from_slice(contract().as_bytes());
    // Entries of up to 512 KiB: 3 MiB frames carry them.
    hello.extend_from_slice(&(512u64 * 1024).to_be_bytes());
    socket.write_all(&hello).await.unwrap();
    assert_eq!(socket.read_u8().await.unwrap(), 0);
    let mut identity = vec![0u8; 10 + contract().len()];
    socket.read_exact(&mut identity).await.unwrap();
    async fn exchange(socket: &mut tokio::net::TcpStream, body: &[u8]) -> serde_json::Value {
        socket
            .write_all(&(body.len() as u64).to_be_bytes())
            .await
            .unwrap();
        socket.write_all(body).await.unwrap();
        let len = socket.read_u64().await.unwrap();
        let mut response = vec![0u8; len as usize];
        socket.read_exact(&mut response).await.unwrap();
        serde_json::from_slice(&response).unwrap()
    }
    // A frame larger than the room left is read off and refused, unread.
    let large = vec![b' '; 1024 * 1024];
    assert_eq!(
        exchange(&mut socket, &large).await,
        serde_json::json!("Busy")
    );
    // The connection stays in step: the next request gets its answer.
    let info = exchange(&mut socket, b"\"Info\"").await;
    assert_eq!(info["Info"]["Ok"]["id"], 1);
}

// ---------------------------------------------------------------- peer TLS

/// TLS presenting `leaf` (with `key`) and trusting `tests/fixtures/tls/ca.crt`.
fn tls(leaf: &str, key: &str) -> Option<Arc<PeerTls>> {
    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tls");
    let tls = PeerTls::from_files(
        &fixtures.join(leaf),
        &fixtures.join(key),
        &fixtures.join("ca.crt"),
    )
    .unwrap();
    Some(Arc::new(tls))
}

fn tls_peers() -> PeerConfig {
    PeerConfig {
        tls: tls("node.crt", "node.key"),
        ..PeerConfig::with_token("secret")
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn members_replicate_and_move_leadership_over_tls() {
    let dir = tempfile::tempdir().unwrap();
    let mut members = Vec::new();
    for id in 1..=3 {
        members.push(tcp_node(id, dir.path(), tls_peers()).await);
    }
    members[0]
        .0
        .initialize_nodes(
            members
                .iter()
                .map(|(node, addr, _)| (node.id(), BasicNode::new(addr)))
                .collect(),
        )
        .await
        .unwrap();
    // Catching up takes a Fence from the leader, over TLS.
    for (node, _, _) in &members {
        wait_until(|| !node.rejoining(), "every member to catch up over TLS").await;
    }
    let nodes: Vec<&Node> = members.iter().map(|(node, _, _)| node).collect();
    let leader = nodes[0].status().leader.unwrap();
    let old = nodes.iter().find(|node| node.id() == leader).unwrap();
    old.deploy(b"db", "app", [7u8; 32], b"wasm", &manifest())
        .await
        .unwrap();
    let written = put(old, "secret-data").await.unwrap();
    for node in &nodes {
        wait_until(
            || node.status().last_applied >= Some(written),
            "every member to apply over TLS",
        )
        .await;
    }
    tokio::time::sleep(lease()).await;
    let to = nodes
        .iter()
        .map(|node| node.id())
        .find(|id| *id != leader)
        .unwrap();
    assert_eq!(old.transfer_leadership(to).await.unwrap().to, to);
}

#[tokio::test(flavor = "multi_thread")]
async fn peer_tls_refuses_untrusted_and_misnamed_certificates_and_plaintext() {
    let dir = tempfile::tempdir().unwrap();
    let (_trusted, trusted, _a) = tcp_node(1, dir.path(), tls_peers()).await;
    let untrusted_peers = PeerConfig {
        tls: tls("untrusted.crt", "node.key"),
        ..PeerConfig::with_token("secret")
    };
    let (_untrusted, untrusted, _b) = tcp_node(2, dir.path(), untrusted_peers).await;
    let misnamed_peers = PeerConfig {
        tls: tls("wrong.crt", "wrong.key"),
        ..PeerConfig::with_token("secret")
    };
    let (_misnamed, misnamed, _c) = tcp_node(3, dir.path(), misnamed_peers).await;
    let (_plain, plain, _d) = tcp_node(4, dir.path(), PeerConfig::with_token("secret")).await;
    let timeout = Duration::from_secs(5);
    let tls_dialer = TcpFactory::new(9, tls_peers()).peers();
    let plain_dialer = TcpFactory::new(9, PeerConfig::with_token("secret")).peers();

    assert_eq!(tls_dialer.info(1, &trusted, timeout).await.unwrap().id, 1);
    for (id, addr) in [(2, &untrusted), (3, &misnamed)] {
        let error = tls_dialer.info(id, addr, timeout).await.unwrap_err();
        assert!(error.contains("certificate"), "node {id}: {error}");
    }
    // Members must agree on TLS: neither side falls back to plaintext.
    assert!(plain_dialer.info(1, &trusted, timeout).await.is_err());
    assert!(tls_dialer.info(4, &plain, timeout).await.is_err());
    assert_eq!(plain_dialer.info(4, &plain, timeout).await.unwrap().id, 4);

    // A dialer that never handshakes is dropped at the deadline.
    let quick = PeerConfig {
        handshake_timeout: Duration::from_millis(300),
        ..tls_peers()
    };
    let (_quick, quick, _e) = tcp_node(5, dir.path(), quick).await;
    let mut silent = tokio::net::TcpStream::connect(&quick).await.unwrap();
    let started = std::time::Instant::now();
    let mut rest = Vec::new();
    let closed = tokio::time::timeout(Duration::from_secs(5), silent.read_to_end(&mut rest)).await;
    assert!(closed.is_ok() && rest.is_empty());
    assert!(started.elapsed() < Duration::from_secs(3));
}

// ----------------------------------------------------- leadership transfer

#[tokio::test(flavor = "multi_thread")]
async fn leadership_moves_to_a_caught_up_voter() {
    let dir = tempfile::tempdir().unwrap();
    let (_registry, nodes) = cluster(dir.path(), 3).await;
    let from = wait_leader(&nodes).await;
    let old = leader_of(&nodes, from);
    let written = put(old, "before").await.unwrap();
    let to = nodes.iter().map(Node::id).find(|id| *id != from).unwrap();
    let term = old.status().term;

    let outcome = old.transfer_leadership(to).await.unwrap();
    assert_eq!(
        (outcome.from, outcome.to, outcome.unchanged),
        (from, to, false)
    );
    // The target won its first campaign: the group was never without a
    // leader for an election timeout.
    assert_eq!(outcome.term, term + 1);
    assert_eq!(wait_leader(&nodes).await, to);
    let new = leader_of(&nodes, to);
    assert!(new.status().last_applied >= Some(written));
    // The new leader takes writes; the old one forwards to it.
    put(new, "after").await.unwrap();
    assert!(matches!(
        put(old, "stale").await,
        Err(NodeError::Forward { leader: Some(leader) }) if leader == to
    ));
    // Asking the leader to lead changes nothing; a follower forwards.
    let again = new.transfer_leadership(to).await.unwrap();
    assert!(again.unchanged);
    assert!(matches!(
        old.transfer_leadership(from).await,
        Err(NodeError::Forward { leader: Some(leader) }) if leader == to
    ));
    // Straight back: the new leader's own lease on itself has not run,
    // so it releases it rather than ask for a vote it would refuse.
    let back = new.transfer_leadership(from).await.unwrap();
    assert_eq!((back.to, back.term), (from, outcome.term + 1));
    assert_eq!(wait_leader(&nodes).await, from);
}

#[tokio::test(flavor = "multi_thread")]
async fn tcp_members_rejoin_move_leadership_and_reconfigure() {
    // Fence, TransferLeader and Info over the wire.
    let dir = tempfile::tempdir().unwrap();
    let peers = PeerConfig::with_token("secret");
    let mut members = Vec::new();
    for id in 1..=3 {
        members.push(tcp_node(id, dir.path(), peers.clone()).await);
    }
    members[0]
        .0
        .initialize_nodes(
            members
                .iter()
                .map(|(node, addr, _)| (node.id(), BasicNode::new(addr)))
                .collect(),
        )
        .await
        .unwrap();
    for (node, _, _) in &members {
        wait_until(|| !node.rejoining(), "every member to catch up over TCP").await;
    }
    let nodes: Vec<&Node> = members.iter().map(|(node, _, _)| node).collect();
    let leader = loop {
        if let Some(leader) = nodes[0].status().leader {
            break leader;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    let old = nodes.iter().find(|node| node.id() == leader).unwrap();
    let to = nodes
        .iter()
        .map(|node| node.id())
        .find(|id| *id != leader)
        .unwrap();
    tokio::time::sleep(lease()).await;
    let outcome = old.transfer_leadership(to).await.unwrap();
    assert_eq!(outcome.to, to);
    let new = nodes.iter().find(|node| node.id() == to).unwrap();
    wait_until(|| new.status().leader == Some(to), "the new leader").await;
    // Remove the old leader: every remaining member is probed by ID.
    let view = new.membership();
    let remaining: BTreeMap<u64, String> = view
        .nodes
        .iter()
        .filter(|(id, _)| **id != leader)
        .map(|(id, addr)| (*id, addr.clone()))
        .collect();
    let after = new
        .reconfigure(MembershipChange {
            members: remaining.clone(),
            expected_log_id: view.log_id,
            retain: false,
        })
        .await
        .unwrap();
    assert_eq!(
        after.voter_configs,
        vec![remaining.keys().copied().collect::<BTreeSet<_>>()]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn leadership_moves_past_other_voters_leases() {
    // With five voters the old leader's grant is not enough: the target
    // needs two more voters, which release their leases on the old
    // leader when it asks. Registry OpenRaft has no release, so the
    // transfer used to wait those leases out: an election timeout.
    let dir = tempfile::tempdir().unwrap();
    let (_registry, nodes) = cluster_with(dir.path(), 5, wide()).await;
    let from = wait_leader(&nodes).await;
    let old = leader_of(&nodes, from);
    let to = nodes.iter().map(Node::id).find(|id| *id != from).unwrap();
    let term = old.status().term;
    let outcome = old.transfer_leadership(to).await.unwrap();
    assert_eq!(outcome.to, to);
    assert!(
        Duration::from_millis(outcome.elapsed_ms)
            < Duration::from_millis(wide().election_timeout_max),
        "{outcome:?}"
    );
    // The target won its first campaign: the voters had released their
    // leases, so the old leader never stepped down to a candidate that
    // could not win yet, leaving the group leaderless.
    assert_eq!(outcome.term, term + 1);
    assert_eq!(wait_leader(&nodes).await, to);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_stopping_leader_hands_off_so_no_election_follows() {
    // `serve` does this on a shutdown signal: a planned restart costs a
    // transfer, and the group never waits out a lease.
    let dir = tempfile::tempdir().unwrap();
    let (registry, mut nodes) = cluster(dir.path(), 3).await;
    let from = wait_leader(&nodes).await;
    let term = leader_of(&nodes, from).status().term;
    put(leader_of(&nodes, from), "before").await.unwrap();
    // A follower has nothing to hand over.
    let follower = nodes.iter().find(|node| node.id() != from).unwrap();
    assert_eq!(follower.hand_off_leadership().await.unwrap(), None);

    let outcome = leader_of(&nodes, from)
        .hand_off_leadership()
        .await
        .unwrap()
        .unwrap();
    assert_eq!((outcome.from, outcome.term), (from, term + 1));
    assert_ne!(outcome.to, from);
    assert!(
        Duration::from_millis(outcome.elapsed_ms) < lease(),
        "{outcome:?}"
    );
    // The old leader goes; the new one goes on in the same term.
    let index = nodes.iter().position(|node| node.id() == from).unwrap();
    registry.unregister(from);
    nodes.remove(index).shutdown().await.unwrap();
    put(leader_of(&nodes, outcome.to), "after").await.unwrap();
    tokio::time::sleep(lease() * 2).await;
    assert_eq!(wait_leader(&nodes).await, outcome.to);
    assert!(nodes.iter().all(|node| node.status().term == outcome.term));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_transfer_refuses_writes_meanwhile_then_resumes() {
    let dir = tempfile::tempdir().unwrap();
    let (registry, nodes) = cluster(dir.path(), 3).await;
    let from = wait_leader(&nodes).await;
    let old = leader_of(&nodes, from);
    // Learners and unknown nodes are refused at once.
    let error = old.transfer_leadership(42).await.unwrap_err();
    assert_eq!(code(&error), TRANSFER_FAILED);
    // A target that can't be reached never catches up: the transfer
    // gives up after twice the election timeout. Meanwhile writes are
    // refused, retryably; afterwards they go through again.
    let to = nodes.iter().map(Node::id).find(|id| *id != from).unwrap();
    registry.unregister(to);
    put(old, "fill").await.unwrap();
    let transfer = old.transfer_leadership(to);
    let probe = async {
        tokio::time::sleep(lease() / 2).await;
        put(old, "during").await
    };
    let (transferred, during) = tokio::join!(transfer, probe);
    let error = transferred.unwrap_err();
    assert_eq!(code(&error), TRANSFER_FAILED);
    assert_eq!(code(&during.unwrap_err()), TRANSFERRING);
    assert_eq!(old.status().leader, Some(from));
    put(old, "after").await.unwrap();
}

// ------------------------------------------------------------------ rejoin

#[tokio::test(flavor = "multi_thread")]
async fn a_wiped_member_votes_and_serves_only_after_catching_up() {
    let dir = tempfile::tempdir().unwrap();
    let (registry, mut nodes) = cluster(dir.path(), 3).await;
    let leader = wait_leader(&nodes).await;
    let wiped = nodes.iter().map(Node::id).find(|id| *id != leader).unwrap();
    let written = put(leader_of(&nodes, leader), "kept").await.unwrap();

    // Wipe a follower and bring it back under its ID. Its leader
    // replicates to it, but it cannot ask that leader for a fence yet
    // (its own requests go through `outbound`, which lacks the leader).
    let index = nodes.iter().position(|node| node.id() == wiped).unwrap();
    registry.unregister(wiped);
    nodes.remove(index).shutdown().await.unwrap();
    wipe(dir.path(), wiped).await;
    let outbound = Registry::new();
    let back = Node::open(
        wiped,
        &dir.path().join(format!("n{wiped}")),
        config(),
        InProcessFactory::new(outbound.clone()),
        runtimes(),
    )
    .await
    .unwrap();
    assert!(back.rejoining());
    registry.register_peer(back.peer_service());
    wait_until(
        || back.status().leader == Some(leader) && back.status().last_applied >= Some(written),
        "the wiped member to follow its leader",
    )
    .await;
    // Following and applied, it still neither serves nor votes.
    let readiness = back.readiness().await;
    assert!(readiness.rejoining && !readiness.ready, "{readiness:?}");
    assert_eq!(code(&back.sync_since(0, 10).unwrap_err()), REJOINING);
    let candidate = VoteRequest::new(
        Vote::new(99, leader),
        Some(LogId::new(CommittedLeaderId::new(98, leader), written + 10)),
    );
    let reach = Registry::new();
    reach.register_peer(back.peer_service());
    let mut network = InProcessFactory::new(reach)
        .new_client(wiped, &BasicNode::default())
        .await;
    let answer = network
        .vote(candidate.clone(), RPCOption::new(Duration::from_secs(1)))
        .await
        .unwrap();
    assert!(!answer.vote_granted);
    assert!(back.rejoining());
    // Nor does it take leadership, though it holds the whole log.
    let error = leader_of(&nodes, leader)
        .transfer_leadership(wiped)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("not caught up"), "{error}");
    assert_eq!(wait_leader(&nodes).await, leader);

    // Once its fence reaches the leader, it caught up: it serves.
    for node in &nodes {
        outbound.register_peer(node.peer_service());
    }
    wait_until(|| !back.rejoining(), "the wiped member to catch up").await;
    assert!(back.readiness().await.ready);
    back.sync_since(0, 10).unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_wiped_member_never_elects_a_candidate_missing_a_committed_entry() {
    // Leader A commits an entry with C alone while B is down. C loses
    // its disk and A goes down. B, lacking the entry, campaigns: C's
    // vote would elect it and lose the entry. C must refuse it.
    let dir = tempfile::tempdir().unwrap();
    let (registry, nodes) = cluster(dir.path(), 3).await;
    let a = wait_leader(&nodes).await;
    let mut ids = nodes.iter().map(Node::id).filter(|id| *id != a);
    let (b, c) = (ids.next().unwrap(), ids.next().unwrap());
    let mut nodes: BTreeMap<u64, Node> = nodes.into_iter().map(|node| (node.id(), node)).collect();

    registry.unregister(b);
    nodes.remove(&b).unwrap().shutdown().await.unwrap();
    let committed = put(&nodes[&a], "only-on-a-and-c").await.unwrap();
    for id in [a, c] {
        registry.unregister(id);
        nodes.remove(&id).unwrap().shutdown().await.unwrap();
    }
    wipe(dir.path(), c).await;
    let c_back = open(c, dir.path(), &registry).await;
    assert!(c_back.rejoining());
    let b_back = open(b, dir.path(), &registry).await;
    assert!(!b_back.rejoining());
    assert!(b_back.status().last_log < Some(committed));
    tokio::time::sleep(lease() * 8).await;
    assert_eq!(
        b_back.status().leader,
        None,
        "B was elected without the entry"
    );
    assert_eq!(c_back.status().leader, None);

    // A returns: it leads with the entry, and C catches up from it.
    let a_back = open(a, dir.path(), &registry).await;
    let alive = [a_back, b_back, c_back];
    let leader = wait_leader(&alive).await;
    assert_eq!(leader, a);
    wait_until(|| !alive[2].rejoining(), "the wiped member to catch up").await;
    for node in &alive {
        wait_until(
            || node.status().last_applied >= Some(committed),
            "every member to apply the entry",
        )
        .await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_restart_before_catching_up_keeps_a_member_rejoining() {
    let dir = tempfile::tempdir().unwrap();
    // A member that opened empty, took its leader's vote and then
    // restarted: it holds Raft state now, but the mark stays.
    {
        let path = dir.path().join("n1");
        std::fs::create_dir_all(&path).unwrap();
        let mut logs = LogStore::open(&path.join("raft.redb")).unwrap();
        // A member's own file: claimed when it was created.
        logs.claim_contract(flower_cluster::compat::storage_contract())
            .unwrap()
            .unwrap();
        logs.set_rejoining(true).unwrap();
        logs.save_vote(&Vote::new_committed(3, 2)).await.unwrap();
        assert!(logs.has_raft_state().unwrap());
    }
    let node = open(1, dir.path(), &Registry::new()).await;
    assert!(node.rejoining());
    node.shutdown().await.unwrap();
    // Founding a cluster ends it, durably.
    let fresh = open(2, dir.path(), &Registry::new()).await;
    assert!(fresh.rejoining());
    fresh.initialize(BTreeSet::from([2])).await.unwrap();
    assert!(!fresh.rejoining());
    fresh.shutdown().await.unwrap();
    let fresh = open(2, dir.path(), &Registry::new()).await;
    assert!(!fresh.rejoining());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rejoining_voter_never_campaigns() {
    // A wiped voter took its group's membership (a snapshot, say) and
    // restarted before catching up; its leader is gone. Holding the
    // membership, it would campaign on its own election timer: it must
    // wait for a leader instead.
    let dir = tempfile::tempdir().unwrap();
    let (registry, nodes) = cluster(dir.path(), 2).await;
    let follower = nodes
        .iter()
        .map(Node::id)
        .find(|id| Some(*id) != nodes[0].status().leader)
        .unwrap();
    for node in nodes {
        registry.unregister(node.id());
        node.shutdown().await.unwrap();
    }
    let path = dir.path().join(format!("n{follower}")).join("raft.redb");
    LogStore::open(&path).unwrap().set_rejoining(true).unwrap();
    let back = open(follower, dir.path(), &registry).await;
    assert!(back.rejoining());
    // Metrics report the stored term once the core published them.
    wait_until(|| back.status().term > 0, "the stored term").await;
    let term = back.status().term;
    tokio::time::sleep(lease() * 6).await;
    assert_eq!(back.status().term, term, "a rejoining voter campaigned");
    assert!(back.rejoining());
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_returns_once_the_files_are_closed() {
    // OpenRaft lets its state machine worker and snapshot builders go
    // after the core stops; shutdown waits for them, so the files reopen
    // at once, every time, even with a snapshot building.
    let dir = tempfile::tempdir().unwrap();
    let node = open(1, dir.path(), &Registry::new()).await;
    node.initialize(BTreeSet::from([1])).await.unwrap();
    wait_until(|| node.status().leader == Some(1), "node 1 to lead").await;
    let mut node = node;
    for _ in 0..10 {
        node.raft().trigger().snapshot().await.unwrap();
        node.shutdown().await.unwrap();
        node = open(1, dir.path(), &Registry::new()).await;
        wait_until(|| node.status().leader == Some(1), "node 1 to lead again").await;
    }
    node.shutdown().await.unwrap();
    std::fs::remove_dir_all(dir.path().join("n1")).unwrap();
}
