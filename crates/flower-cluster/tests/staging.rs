//! Replicated staged deployments through members: a staging survives a
//! change of leader and the new leader continues it, the graph phase
//! rebuilds derived roots through the writer before activation, and the
//! leader's maintenance collects what deployments retired.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use flower_cluster::network::{InProcessFactory, Registry};
use flower_cluster::node::Node;
use flower_cluster::statemachine::RuntimeFactory;
use flower_engine::engine::{CallOptions, MutationOptions};
use flower_engine::runtime::{CallKind, ClosureRuntime, HostCall};
use flower_engine::staging::StagingPhase;
use flower_sdk::abi::{HostOp, decode_envelope};
use flower_sdk::{Failure, Value, fail};

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

/// Canonical key text for a simple string key.
fn k(name: &str) -> String {
    format!("\"{name}\"")
}

fn str_schema() -> Value {
    obj(vec![("type", s("string"))])
}

fn json_schema() -> Value {
    obj(vec![("type", s("json"))])
}

fn index(name: &str, fields: Vec<&str>) -> Value {
    obj(vec![
        ("name", s(name)),
        ("fields", arr(fields.into_iter().map(s).collect())),
    ])
}

/// `tasks {title}` with the given indexes.
fn tasks(indexes: Vec<Value>) -> Value {
    obj(vec![
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
        ("indexes", arr(indexes)),
        ("references", arr(vec![])),
    ])
}

fn definition(name: &str, kind: &str) -> Value {
    obj(vec![
        ("name", s(name)),
        ("kind", s(kind)),
        ("args", json_schema()),
    ])
}

/// The test module: `tasks` indexed by title, and by title again when
/// `again`; `put`, and a derived `scaled` every instance of which
/// stays materialized, read by `read`.
fn app_manifest(again: bool) -> Value {
    let mut indexes = vec![index("by_title", vec!["title"])];
    if again {
        indexes.push(index("by_title_again", vec!["title"]));
    }
    obj(vec![
        ("collections", arr(vec![tasks(indexes)])),
        (
            "definitions",
            arr(vec![
                definition("put", "mutation"),
                obj(vec![
                    ("name", s("scaled")),
                    ("kind", s("derived")),
                    ("args", json_schema()),
                    ("materialize", s("always")),
                ]),
                definition("read", "query"),
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

fn field(value: &Value, name: &str) -> Value {
    match value {
        Value::Object(map) => map.get(name).cloned().unwrap_or(Value::Null),
        _ => Value::Null,
    }
}

/// Builds the test module's runtimes. `scaled` is a title's length
/// times a factor its bundle sets: 2 for `wasm-v1`, 3 for any other.
/// Evaluations count per bundle.
#[derive(Clone, Default)]
struct Factory {
    v1: Arc<AtomicUsize>,
    v2: Arc<AtomicUsize>,
}

impl RuntimeFactory for Factory {
    fn build(
        &self,
        _db: &[u8],
        _module: &str,
        wasm: &[u8],
        _manifest: &Value,
    ) -> Result<Box<dyn flower_engine::runtime::Runtime>, String> {
        let (factor, evaluations) = if wasm == b"wasm-v1" {
            (2.0, self.v1.clone())
        } else {
            (3.0, self.v2.clone())
        };
        Ok(Box::new(ClosureRuntime::new(
            move |kind: CallKind,
                  _module: &str,
                  name: &str,
                  args: &Value,
                  host: &mut dyn HostCall| {
                let Value::String(key) = field(args, "key") else {
                    return Err(fail("TYPE_MISMATCH", "args need a key"));
                };
                match (kind, name) {
                    (CallKind::Mutation, "put") => {
                        hc(
                            host,
                            HostOp::Set,
                            vec![
                                s(""),
                                s("tasks"),
                                s(&k(&key)),
                                obj(vec![("title", field(args, "title"))]),
                            ],
                        )?;
                        Ok(Value::Null)
                    }
                    (CallKind::Derived, "scaled") => {
                        evaluations.fetch_add(1, Ordering::SeqCst);
                        let row = hc(host, HostOp::Get, vec![s(""), s("tasks"), s(&k(&key))])?;
                        let Value::String(title) = field(&row, "title") else {
                            return Err(fail("NOT_FOUND", "no such task"));
                        };
                        Ok(Value::Number(title.len() as f64 * factor))
                    }
                    (CallKind::Query, "read") => hc(
                        host,
                        HostOp::GetDerived,
                        vec![s(""), s("scaled"), args.clone()],
                    ),
                    _ => Err(fail("UNKNOWN_METHOD", "unknown definition")),
                }
            },
        )))
    }
}

fn config() -> Arc<openraft::Config> {
    Arc::new(openraft::Config::default())
}

async fn eventually<F, Fut>(what: &str, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while tokio::time::Instant::now() < deadline {
        if check().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("timed out waiting for {what}");
}

/// Open `count` initialized in-process members sharing `factory`.
async fn members(dir: &std::path::Path, count: u64, factory: &Factory) -> (Registry, Vec<Node>) {
    let registry = Registry::new();
    let mut nodes = Vec::new();
    for id in 1..=count {
        let node = Node::open(
            id,
            &dir.join(format!("n{id}")),
            config(),
            InProcessFactory::new(registry.clone()),
            Arc::new(factory.clone()),
        )
        .await
        .unwrap();
        registry.register(id, node.raft().clone());
        nodes.push(node);
    }
    nodes[0].initialize((1..=count).collect()).await.unwrap();
    eventually("a leader", || {
        let elected = nodes.iter().any(|node| node.status().leader.is_some());
        async move { elected }
    })
    .await;
    (registry, nodes)
}

/// The member that leads, once one does.
async fn leader(nodes: &[Node]) -> &Node {
    eventually("a leader", || {
        let elected = nodes
            .iter()
            .any(|node| node.status().leader == Some(node.id()));
        async move { elected }
    })
    .await;
    nodes
        .iter()
        .find(|node| node.status().leader == Some(node.id()))
        .unwrap()
}

/// Wait for a member to apply through `index`.
async fn applied(node: &Node, index: u64) {
    eventually("the member to apply", || {
        let done = node.status().last_applied.unwrap_or(0) >= index;
        async move { done }
    })
    .await;
}

fn put(key: &str, title: &str) -> Value {
    obj(vec![("key", s(key)), ("title", s(title))])
}

/// Entries of one of `tasks`' indexes on a member.
fn entries(node: &Node, index: &str) -> usize {
    let read = node.reader().store().begin_read().unwrap();
    let qualified = flower_engine::host::qualify_index("app", "tasks", index);
    let Some(id) = read.intern_id(b"db", qualified.as_bytes()).unwrap() else {
        return 0;
    };
    read.scan_index(
        b"db",
        id,
        1,
        &[],
        flower_engine::storage::ScanRange::default(),
    )
    .unwrap()
    .len()
}

fn staging(node: &Node) -> Option<flower_engine::engine::StagingStatus> {
    node.reader().staging_status(b"db", "app").unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_staging_survives_a_change_of_leader() {
    let dir = tempfile::tempdir().unwrap();
    let factory = Factory::default();
    let (registry, nodes) = members(dir.path(), 3, &factory).await;
    let first = leader(&nodes).await;
    first
        .deploy(b"db", "app", [1u8; 32], b"wasm-v1", &app_manifest(false))
        .await
        .unwrap();
    for key in ["a", "b", "c", "d"] {
        first
            .mutate(
                b"db",
                "app",
                "put",
                &put(key, key),
                &MutationOptions::default(),
            )
            .await
            .unwrap();
    }
    // Stage a second index and build half of it at this leader.
    let staged = first
        .stage_deployment(b"db", "app", [2u8; 32], b"wasm-v2", &app_manifest(true))
        .await
        .unwrap();
    assert_eq!(staged.status.unwrap().phase, StagingPhase::Building);
    let advanced = first.advance_deployment(b"db", "app", 2).await.unwrap();
    assert_eq!(advanced.scanned, 2);
    // Every member holds the same staging and the same half-built index.
    let position = first.status().last_applied.unwrap();
    for node in &nodes {
        applied(node, position).await;
        let status = staging(node).expect("the staging replicated");
        assert!(status.replicated, "node {}", node.id());
        assert_eq!(status.phase, StagingPhase::Building);
        assert_eq!(entries(node, "by_title_again"), 2, "node {}", node.id());
    }

    // The leader goes; the staging stays.
    let gone = first.id();
    registry.unregister(gone);
    let mut survivors = Vec::new();
    for node in nodes {
        if node.id() == gone {
            node.shutdown().await.unwrap();
        } else {
            survivors.push(node);
        }
    }
    let next = leader(&survivors).await;
    let status = next.deployment_status(b"db", "app").await.unwrap();
    assert_eq!(status.identity, Some([2u8; 32]));
    // The new leader continues where the old one stood: the two records
    // left, not the four again.
    let mut scanned = 0;
    let mut steps = 0;
    loop {
        let step = next.advance_deployment(b"db", "app", 2).await.unwrap();
        scanned += step.scanned;
        if step.status.unwrap().phase != StagingPhase::Building {
            break;
        }
        steps += 1;
        assert!(steps < 10, "the backfill never finished");
    }
    assert_eq!(scanned, 2);
    // The module's derived definition rebuilds next; it has no root
    // yet, so one page finishes the graph.
    let status = next.deployment_status(b"db", "app").await.unwrap();
    assert_eq!(status.status.unwrap().phase, StagingPhase::Rebuilding);
    let step = next.advance_deployment(b"db", "app", 2).await.unwrap();
    assert_eq!(step.rebuilt, 0);
    assert!(step.status.unwrap().ready);
    let activated = next.activate_deployment(b"db", "app").await.unwrap();
    for node in &survivors {
        applied(node, activated.position).await;
        assert_eq!(entries(node, "by_title_again"), 4, "node {}", node.id());
        let read = node.reader().store().begin_read().unwrap();
        let active = read
            .meta(b"db", &flower_engine::staging::code_identity_key("app"))
            .unwrap();
        assert_eq!(active.as_deref(), Some(&[2u8; 32][..]));
        drop(read);
        assert!(staging(node).is_none());
    }
    for node in survivors {
        node.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_graph_phase_rebuilds_roots_through_the_writer() {
    let dir = tempfile::tempdir().unwrap();
    let factory = Factory::default();
    let (_registry, nodes) = members(dir.path(), 1, &factory).await;
    let node = leader(&nodes).await;
    node.deploy(b"db", "app", [1u8; 32], b"wasm-v1", &app_manifest(false))
        .await
        .unwrap();
    let titles = [("a", "x"), ("b", "xx"), ("c", "xxx")];
    for (key, title) in titles {
        node.mutate(
            b"db",
            "app",
            "put",
            &put(key, title),
            &MutationOptions::default(),
        )
        .await
        .unwrap();
        // Reading materializes the root.
        let read = node
            .query(
                b"db",
                "app",
                "read",
                &obj(vec![("key", s(key))]),
                &CallOptions::default(),
            )
            .await
            .unwrap();
        assert_eq!(read, Value::Number(title.len() as f64 * 2.0));
    }
    let staged = node
        .stage_deployment(b"db", "app", [2u8; 32], b"wasm-v2", &app_manifest(false))
        .await
        .unwrap();
    let status = staged.status.unwrap();
    assert_eq!(status.phase, StagingPhase::Rebuilding);
    assert_eq!(status.graph, 1);
    match node.activate_deployment(b"db", "app").await {
        Err(flower_cluster::node::NodeError::Failure { code, .. }) => {
            assert_eq!(code, "deployment")
        }
        other => panic!("activated before the graph was built: {other:?}"),
    }
    // One root per step, each evaluated under the target once.
    let mut rebuilt = 0;
    let mut steps = 0;
    loop {
        let step = node.advance_deployment(b"db", "app", 1).await.unwrap();
        rebuilt += step.rebuilt;
        if step.status.unwrap().ready {
            break;
        }
        steps += 1;
        assert!(steps < 10, "the graph never finished");
    }
    assert_eq!(rebuilt, 3);
    assert_eq!(factory.v2.load(Ordering::SeqCst), 3);
    node.activate_deployment(b"db", "app").await.unwrap();
    // The cutover serves the rebuilt roots: no evaluation.
    for (key, title) in titles {
        let read = node
            .query(
                b"db",
                "app",
                "read",
                &obj(vec![("key", s(key))]),
                &CallOptions::default(),
            )
            .await
            .unwrap();
        assert_eq!(read, Value::Number(title.len() as f64 * 3.0));
    }
    assert_eq!(factory.v2.load(Ordering::SeqCst), 3);
    for node in nodes {
        node.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn maintenance_collects_what_a_deployment_retired() {
    let dir = tempfile::tempdir().unwrap();
    let factory = Factory::default();
    let (_registry, nodes) = members(dir.path(), 3, &factory).await;
    let first = leader(&nodes).await;
    first
        .deploy(b"db", "app", [1u8; 32], b"wasm-v1", &app_manifest(true))
        .await
        .unwrap();
    for key in ["a", "b", "c"] {
        first
            .mutate(
                b"db",
                "app",
                "put",
                &put(key, key),
                &MutationOptions::default(),
            )
            .await
            .unwrap();
    }
    // Deploying without the second index retires it on every member.
    let deployed = first
        .deploy(b"db", "app", [2u8; 32], b"wasm-v2", &app_manifest(false))
        .await
        .unwrap();
    for node in &nodes {
        applied(node, deployed.position).await;
        assert!(staging(node).is_some_and(|status| status.retired >= 1));
        assert_eq!(entries(node, "by_title_again"), 3);
    }
    // No operator asks: the leader's maintenance collects it, on every
    // member alike.
    let (stop, stopped) = tokio::sync::watch::channel(false);
    let interval = Duration::from_millis(20);
    tokio::join!(
        nodes[0].run_maintenance(interval, stopped.clone()),
        nodes[1].run_maintenance(interval, stopped.clone()),
        nodes[2].run_maintenance(interval, stopped.clone()),
        async {
            eventually("every member to collect", || {
                let collected = nodes
                    .iter()
                    .all(|node| staging(node).is_none() && entries(node, "by_title_again") == 0);
                async move { collected }
            })
            .await;
            stop.send(true).unwrap();
        },
    );
    let stats = first.collection_stats();
    assert!(stats.collections >= 1 && stats.collected >= 3, "{stats:?}");
    for node in nodes {
        node.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_one_call_deployment_replaces_a_replicated_staging_everywhere() {
    let dir = tempfile::tempdir().unwrap();
    let factory = Factory::default();
    let (_registry, nodes) = members(dir.path(), 3, &factory).await;
    let first = leader(&nodes).await;
    first
        .deploy(b"db", "app", [1u8; 32], b"wasm-v1", &app_manifest(false))
        .await
        .unwrap();
    first
        .mutate(
            b"db",
            "app",
            "put",
            &put("a", "a"),
            &MutationOptions::default(),
        )
        .await
        .unwrap();
    first
        .stage_deployment(b"db", "app", [2u8; 32], b"wasm-v2", &app_manifest(true))
        .await
        .unwrap();
    let deployed = first
        .deploy(b"db", "app", [3u8; 32], b"wasm-v3", &app_manifest(false))
        .await
        .unwrap();
    for node in &nodes {
        applied(node, deployed.position).await;
        // The abandoned staging's partial index retires everywhere; no
        // staging stays open on any member.
        assert!(
            staging(node).is_none_or(|status| status.phase == StagingPhase::Gc),
            "node {}",
            node.id()
        );
    }
    for node in nodes {
        node.shutdown().await.unwrap();
    }
}

/// Wait for every member to apply `position`, then check no staging of
/// the module stays open on any of them.
async fn no_staging_open(nodes: &[Node], position: u64) {
    for node in nodes {
        applied(node, position).await;
        assert!(
            staging(node).is_none_or(|status| status.phase == StagingPhase::Gc),
            "node {}: {:?}",
            node.id(),
            staging(node)
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deploying_the_active_code_again_leaves_no_staging() {
    let dir = tempfile::tempdir().unwrap();
    let factory = Factory::default();
    let (_registry, nodes) = members(dir.path(), 3, &factory).await;
    let first = leader(&nodes).await;
    first
        .deploy(b"db", "app", [1u8; 32], b"wasm-v1", &app_manifest(false))
        .await
        .unwrap();
    // The same code again activates nothing, and the staging the
    // deployment opened for it does not stay behind.
    let again = first
        .deploy(b"db", "app", [1u8; 32], b"wasm-v1", &app_manifest(false))
        .await
        .unwrap();
    no_staging_open(&nodes, again.position).await;
    for node in nodes {
        node.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn activating_the_active_code_closes_its_staging() {
    let dir = tempfile::tempdir().unwrap();
    let factory = Factory::default();
    let (_registry, nodes) = members(dir.path(), 3, &factory).await;
    let first = leader(&nodes).await;
    first
        .deploy(b"db", "app", [1u8; 32], b"wasm-v1", &app_manifest(false))
        .await
        .unwrap();
    // The active code, staged and activated: nothing changes, and the
    // staging closes as an activation's would.
    first
        .stage_deployment(b"db", "app", [1u8; 32], b"wasm-v1", &app_manifest(false))
        .await
        .unwrap();
    let mut steps = 0;
    while !first
        .advance_deployment(b"db", "app", 100)
        .await
        .unwrap()
        .status
        .unwrap()
        .ready
    {
        steps += 1;
        assert!(steps < 10, "the staging never got ready");
    }
    let activated = first.activate_deployment(b"db", "app").await.unwrap();
    no_staging_open(&nodes, activated.position).await;
    // A later deployment of other code goes ahead.
    let next = first
        .deploy(b"db", "app", [2u8; 32], b"wasm-v2", &app_manifest(true))
        .await
        .unwrap();
    no_staging_open(&nodes, next.position).await;
    for node in nodes {
        node.shutdown().await.unwrap();
    }
}
