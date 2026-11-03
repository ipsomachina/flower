//! Readiness under load: applying trails committing by an entry or a
//! batch on a busy member, which must not take it out of rotation, while
//! a member that really cannot apply what it knows committed stays not
//! ready.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use flower_cluster::network::{InProcessFactory, Registry};
use flower_cluster::node::{Node, READY_APPLY_WAIT};
use flower_cluster::statemachine::RuntimeFactory;
use flower_engine::engine::MutationOptions;
use flower_engine::runtime::{CallKind, ClosureRuntime, HostCall};
use flower_sdk::abi::{HostOp, decode_envelope};
use flower_sdk::{Failure, Value, fail};
use openraft::ServerState;

/// Why a member that has not applied what it knows committed is not
/// ready.
const APPLYING: &str = "applying committed entries";

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

/// One `tasks` collection and a `put` mutation.
fn manifest() -> Value {
    let string = obj(vec![("type", s("string"))]);
    let tasks = obj(vec![
        ("name", s("tasks")),
        ("key", string.clone()),
        (
            "value",
            obj(vec![
                ("type", s("object")),
                (
                    "fields",
                    arr(vec![obj(vec![
                        ("name", s("title")),
                        ("schema", string),
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
            arr(vec![obj(vec![
                ("name", s("put")),
                ("kind", s("mutation")),
                ("args", obj(vec![("type", s("json"))])),
            ])]),
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
                match (kind, name) {
                    (CallKind::Mutation, "put") => {
                        let key = map.get("key").cloned().unwrap_or(Value::Null);
                        let title = map.get("title").cloned().unwrap_or(Value::Null);
                        hc(
                            host,
                            HostOp::Set,
                            vec![s(""), s("tasks"), key, obj(vec![("title", title)])],
                        )?;
                        Ok(s("put-ok"))
                    }
                    _ => Err(fail("UNKNOWN_METHOD", "unknown definition")),
                }
            },
        )))
    }
}

/// Three in-process members with the app deployed.
struct Cluster {
    _dir: tempfile::TempDir,
    nodes: Vec<Arc<Node>>,
}

impl Cluster {
    async fn start() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let registry = Registry::new();
        let mut nodes = Vec::new();
        for id in 1..=3u64 {
            let node = Node::open(
                id,
                &dir.path().join(format!("n{id}")),
                Arc::new(openraft::Config::default()),
                InProcessFactory::new(registry.clone()).with_source(id),
                Arc::new(App),
            )
            .await
            .unwrap();
            registry.register_peer(node.peer_service());
            nodes.push(Arc::new(node));
        }
        nodes[0]
            .initialize(BTreeSet::from([1, 2, 3]))
            .await
            .unwrap();
        let cluster = Cluster { _dir: dir, nodes };
        cluster
            .leader()
            .await
            .deploy(b"db", "app", [7u8; 32], b"wasm", &manifest())
            .await
            .unwrap();
        cluster
    }

    /// The member every member agrees leads.
    async fn leader(&self) -> Arc<Node> {
        for _ in 0..400 {
            let leaders: BTreeSet<_> = self.nodes.iter().map(|node| node.status().leader).collect();
            if let [Some(leader)] = leaders.into_iter().collect::<Vec<_>>()[..] {
                let node = &self.nodes[(leader - 1) as usize];
                if node.raft().metrics().borrow().state == ServerState::Leader {
                    return node.clone();
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("no agreed leader");
    }
}

/// Write `put` as fast as the leader commits until `stop`.
async fn write_until(leader: Arc<Node>, stop: Arc<AtomicBool>, writer: u32) -> u64 {
    let mut written = 0u64;
    while !stop.load(Ordering::Relaxed) {
        let args = obj(vec![
            ("key", s(&format!("\"w{writer}-{}\"", written % 50))),
            ("title", s(&format!("update {written}"))),
        ]);
        leader
            .mutate(b"db", "app", "put", &args, &MutationOptions::default())
            .await
            .unwrap();
        written += 1;
    }
    written
}

/// The regression: readiness compared the applied index with the
/// committed index at one instant, so a member applying one entry behind
/// commit under steady writes answered "applying committed entries" now
/// and then, and a probe took a busy, healthy member out of rotation.
#[tokio::test(flavor = "multi_thread")]
async fn members_keeping_up_under_sustained_writes_stay_ready() {
    let cluster = Cluster::start().await;
    let leader = cluster.leader().await;
    let stop = Arc::new(AtomicBool::new(false));
    let writers: Vec<_> = (0..4)
        .map(|writer| tokio::spawn(write_until(leader.clone(), stop.clone(), writer)))
        .collect();
    let mut probes = 0;
    let mut unready = Vec::new();
    let started = Instant::now();
    // Probes far more often than an orchestrator's (every second or so),
    // to meet the in-flight gap often.
    while started.elapsed() < Duration::from_secs(3) {
        for node in &cluster.nodes {
            let readiness = node.readiness().await;
            probes += 1;
            if !readiness.ready {
                unready.push((
                    node.id(),
                    readiness.reason,
                    readiness.applied,
                    readiness.committed,
                ));
            }
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    stop.store(true, Ordering::Relaxed);
    let mut written = 0;
    for writer in writers {
        written += writer.await.unwrap();
    }
    assert!(
        written > 100,
        "only {written} writes: no load to test under"
    );
    assert!(probes > 100, "only {probes} probes");
    assert!(
        unready.is_empty(),
        "{} of {probes} probes during {written} writes found a member keeping up not ready: {:?}",
        unready.len(),
        &unready[..unready.len().min(5)]
    );
}

/// A member that holds committed entries it cannot apply is behind, not
/// in flight: still not ready once the probe's wait runs out, and never
/// past the probe's client deadline. Ready again once it applies them.
#[tokio::test(flavor = "multi_thread")]
async fn a_member_that_cannot_apply_what_it_knows_committed_is_not_ready() {
    let cluster = Cluster::start().await;
    let leader = cluster.leader().await;
    let follower = cluster
        .nodes
        .iter()
        .find(|node| node.id() != leader.id())
        .unwrap()
        .clone();
    // Its state machine applies under the engine lock: holding it stalls
    // applying, while the follower still appends and learns the leader's
    // commit index (the other follower makes the quorum).
    let stalled = follower.engine().clone().lock_owned().await;
    for index in 0..5 {
        let args = obj(vec![
            ("key", s(&format!("\"k{index}\""))),
            ("title", s("t")),
        ]);
        leader
            .mutate(b"db", "app", "put", &args, &MutationOptions::default())
            .await
            .unwrap();
    }
    let mut behind = None;
    for _ in 0..100 {
        let started = Instant::now();
        let readiness = follower.readiness().await;
        if readiness.reason.as_deref() == Some(APPLYING) {
            behind = Some((readiness, started.elapsed()));
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let (readiness, waited) = behind.expect("the stalled follower never reported its backlog");
    assert!(!readiness.ready);
    assert!(readiness.applied < readiness.committed, "{readiness:?}");
    // It gave applying its chance before answering, and no more.
    assert!(waited >= READY_APPLY_WAIT, "answered after {waited:?}");
    assert!(waited < READY_APPLY_WAIT * 4, "answered after {waited:?}");
    // A probe with a shorter client deadline answers by it.
    let deadline = tokio::time::Instant::now() + Duration::from_millis(30);
    let started = Instant::now();
    let readiness = flower_cluster::deadline::scoped(Some(deadline), follower.readiness()).await;
    assert_eq!(readiness.reason.as_deref(), Some(APPLYING));
    assert!(
        started.elapsed() < READY_APPLY_WAIT,
        "{:?}",
        started.elapsed()
    );
    // The leader, applying as it commits, stays ready meanwhile.
    assert!(leader.readiness().await.ready);
    drop(stalled);
    let mut ready = false;
    for _ in 0..200 {
        if follower.readiness().await.ready {
            ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(ready, "{:?}", follower.readiness().await);
}
