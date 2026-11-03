//! Maintenance tasks in a replicated group: the leader runs a due task
//! once, followers never do, the run replicates, and both the work left
//! and a failing task's backoff carry over a change of leader.

mod fixtures;

use fixtures::{json_schema, obj, s};

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use flower_cluster::network::{InProcessFactory, Registry};
use flower_cluster::node::{Node, TaskReport, TaskSweepReport};
use flower_cluster::statemachine::RuntimeFactory;
use flower_engine::engine::{CallOptions, MutationOptions};
use flower_engine::runtime::{CallKind, ClosureRuntime, HostCall};
use flower_engine::task::TaskStatus;
use flower_sdk::abi::{HostOp, decode_envelope};
use flower_sdk::{Failure, Value, fail};

/// Runs of `tick` anywhere in this process: guest code runs only where a
/// run is prepared.
static TICKS: AtomicU64 = AtomicU64::new(0);

/// `state` rows plus `arm`, `raise` and `get`; tasks `tick` (due when
/// `next` says, counting its runs in `count` and clearing `next`) and
/// `fragile` (due while `fragile` is set, always failing).
fn manifest() -> Value {
    let definition = |name: &str, kind: &str| {
        obj(vec![
            ("name", s(name)),
            ("kind", s(kind)),
            ("args", json_schema()),
        ])
    };
    let task = |name: &str| obj(vec![("name", s(name)), ("on_error", Value::Bool(false))]);
    obj(vec![
        (
            "collections",
            Value::Array(vec![obj(vec![
                ("name", s("state")),
                ("key", obj(vec![("type", s("string"))])),
                ("value", json_schema()),
                ("indexes", Value::Array(vec![])),
                ("references", Value::Array(vec![])),
            ])]),
        ),
        (
            "definitions",
            Value::Array(vec![
                definition("arm", "mutation"),
                definition("raise", "mutation"),
                definition("fill", "mutation"),
                definition("get", "query"),
            ]),
        ),
        ("triggers", Value::Array(vec![])),
        ("http", Value::Array(vec![])),
        (
            "tasks",
            Value::Array(vec![task("tick"), task("fragile"), task("drain")]),
        ),
    ])
}

fn call(host: &mut dyn HostCall, op: HostOp, args: Vec<Value>) -> Result<Value, Failure> {
    let input = Value::Array(args).encode_binary().unwrap();
    let output = host.op(op as u32 as u8, &input);
    decode_envelope(Value::decode_binary(&output).unwrap())
}

fn read(host: &mut dyn HostCall, key: &str) -> Result<Value, Failure> {
    call(
        host,
        HostOp::Get,
        vec![s(""), s("state"), s(&format!("\"{key}\""))],
    )
}

fn write(host: &mut dyn HostCall, key: &str, value: Value) -> Result<(), Failure> {
    call(
        host,
        HostOp::Set,
        vec![s(""), s("state"), s(&format!("\"{key}\"")), value],
    )?;
    Ok(())
}

fn erase(host: &mut dyn HostCall, key: &str) -> Result<(), Failure> {
    call(
        host,
        HostOp::Delete,
        vec![s(""), s("state"), s(&format!("\"{key}\""))],
    )?;
    Ok(())
}

fn app(
    module: &str,
    kind: CallKind,
    name: &str,
    args: &Value,
    host: &mut dyn HostCall,
) -> Result<Value, Failure> {
    let arg = |name: &str| match args {
        Value::Object(map) => map.get(name).cloned().unwrap_or(Value::Null),
        _ => Value::Null,
    };
    match (kind, name) {
        (CallKind::Mutation, "arm") => write(host, "next", arg("at")).map(|()| Value::Null),
        (CallKind::Mutation, "raise") => {
            write(host, "fragile", Value::Bool(true)).map(|()| Value::Null)
        }
        (CallKind::Query, "get") => read(
            host,
            &match arg("key") {
                Value::String(key) => key,
                _ => return Err(fail("TYPE_MISMATCH", "get takes a key")),
            },
        ),
        (CallKind::Query, "tick") => read(host, "next"),
        (CallKind::Mutation, "tick") => {
            if module == "app" {
                TICKS.fetch_add(1, Ordering::SeqCst);
            }
            let count = match read(host, "count")? {
                Value::Number(count) => count,
                _ => 0.0,
            };
            write(host, "count", Value::Number(count + 1.0))?;
            erase(host, "next")?;
            Ok(Value::Null)
        }
        (CallKind::Query, "fragile") => Ok(match read(host, "fragile")? {
            Value::Null => Value::Null,
            _ => Value::Number(0.0),
        }),
        (CallKind::Mutation, "fragile") => Err(fail("BOOM", "the fragile task fails")),
        // `drain` has a unit of work per `left`, each moving one to `done`.
        (CallKind::Mutation, "fill") => write(host, "left", arg("n")).map(|()| Value::Null),
        (CallKind::Query, "drain") => Ok(match read(host, "left")? {
            Value::Number(left) if left > 0.0 => Value::Number(0.0),
            _ => Value::Null,
        }),
        (CallKind::Mutation, "drain") => {
            let number = |value: Value| match value {
                Value::Number(number) => number,
                _ => 0.0,
            };
            let left = number(read(host, "left")?);
            if left <= 0.0 {
                return Err(fail("EMPTY", "drained a unit that was not there"));
            }
            write(host, "left", Value::Number(left - 1.0))?;
            let done = number(read(host, "done")?);
            write(host, "done", Value::Number(done + 1.0))?;
            Ok(Value::Null)
        }
        _ => Err(fail("UNKNOWN_METHOD", "unknown definition")),
    }
}

struct Apps;

impl RuntimeFactory for Apps {
    fn build(
        &self,
        _db: &[u8],
        _module: &str,
        _wasm: &[u8],
        _manifest: &Value,
    ) -> Result<Box<dyn flower_engine::runtime::Runtime>, String> {
        Ok(Box::new(ClosureRuntime::new(
            |kind: CallKind, module: &str, name: &str, args: &Value, host: &mut dyn HostCall| {
                app(module, kind, name, args, host)
            },
        )))
    }
}

async fn open(id: u64, dir: &Path, registry: &Registry) -> Node {
    let node = Node::open(
        id,
        &dir.join(format!("n{id}")),
        Arc::new(openraft::Config::default()),
        InProcessFactory::new(registry.clone()),
        Arc::new(Apps),
    )
    .await
    .unwrap();
    registry.register_peer(node.peer_service());
    node
}

async fn wait_until(condition: impl Fn() -> bool, what: &str) {
    for _ in 0..400 {
        if condition() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for {what}");
}

/// The leader every member agrees on.
async fn wait_leader(nodes: &[Node]) -> u64 {
    for _ in 0..400 {
        let leaders: BTreeSet<Option<u64>> =
            nodes.iter().map(|node| node.status().leader).collect();
        if let [Some(leader)] = leaders.into_iter().collect::<Vec<_>>()[..] {
            return leader;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("no agreed leader");
}

fn member(nodes: &[Node], id: u64) -> &Node {
    nodes.iter().find(|node| node.id() == id).unwrap()
}

/// Every member applied everything the leader applied.
async fn settle(nodes: &[Node], leader: u64) {
    let applied = member(nodes, leader).status().last_applied;
    for node in nodes {
        wait_until(
            || node.status().last_applied >= applied,
            "every member to apply",
        )
        .await;
    }
}

/// `count` on one member's own state.
async fn count(node: &Node) -> Value {
    let engine = node.engine().lock().await;
    engine
        .invoke_query(
            b"db",
            "app",
            "get",
            &obj(vec![("key", s("count"))]),
            &CallOptions::default(),
        )
        .unwrap()
}

async fn arm(node: &Node, at: f64) {
    node.mutate(
        b"db",
        "app",
        "arm",
        &obj(vec![("at", Value::Number(at))]),
        &MutationOptions::default(),
    )
    .await
    .unwrap();
}

async fn statuses(node: &Node) -> Vec<TaskStatus> {
    node.with_reader(|reader| {
        reader
            .tasks(b"db", "app")
            .map_err(|failure| flower_cluster::node::NodeError::Failure {
                code: failure.code,
                message: failure.message,
            })
    })
    .await
    .unwrap()
}

fn ran(report: &TaskSweepReport) -> (u64, u64) {
    (report.ran, report.failed)
}

#[tokio::test(flavor = "multi_thread")]
async fn tasks_run_once_on_the_leader_across_a_leader_change() {
    let dir = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let mut nodes = Vec::new();
    for id in 1..=3 {
        nodes.push(open(id, dir.path(), &registry).await);
    }
    nodes[0]
        .initialize(BTreeSet::from([1, 2, 3]))
        .await
        .unwrap();
    let first = wait_leader(&nodes).await;
    member(&nodes, first)
        .deploy(b"db", "app", [5u8; 32], b"wasm", &manifest())
        .await
        .unwrap();
    for node in &nodes {
        wait_until(|| !node.rejoining(), "every member to catch up").await;
    }
    let leader = member(&nodes, first);
    let followers: Vec<&Node> = nodes.iter().filter(|node| node.id() != first).collect();

    // No work, then work not due yet: nothing runs.
    assert_eq!(ran(&leader.sweep_tasks_once().await.unwrap()), (0, 0));
    arm(leader, 9e15).await;
    assert_eq!(ran(&leader.sweep_tasks_once().await.unwrap()), (0, 0));
    assert_eq!(TICKS.load(Ordering::SeqCst), 0);

    // Due: followers never run it; the leader runs it once, and the run
    // reaches every member.
    arm(leader, 0.0).await;
    settle(&nodes, first).await;
    for follower in &followers {
        let report = follower.sweep_tasks_once().await.unwrap();
        assert!(!report.leader);
        assert_eq!(ran(&report), (0, 0));
    }
    assert_eq!(TICKS.load(Ordering::SeqCst), 0);
    let report = leader.sweep_tasks_once().await.unwrap();
    assert!(report.leader);
    assert_eq!((report.dbs, report.ran, report.failed), (1, 1, 0));
    assert_eq!(TICKS.load(Ordering::SeqCst), 1);
    assert_eq!(ran(&leader.sweep_tasks_once().await.unwrap()), (0, 0));
    assert_eq!(
        leader.run_task(b"db", "app", "tick").await.unwrap(),
        TaskReport::Skipped
    );
    assert_eq!(TICKS.load(Ordering::SeqCst), 1);
    settle(&nodes, first).await;
    for node in &nodes {
        assert_eq!(count(node).await, Value::Number(1.0));
    }

    // Work left due when the leader changes: the new leader runs it,
    // once, and the old one no longer runs anything.
    arm(leader, 0.0).await;
    settle(&nodes, first).await;
    let next = followers[0].id();
    tokio::time::sleep(Duration::from_millis(
        openraft::Config::default().election_timeout_max,
    ))
    .await;
    assert_eq!(leader.transfer_leadership(next).await.unwrap().to, next);
    assert_eq!(wait_leader(&nodes).await, next);
    let old = leader;
    let leader = member(&nodes, next);
    let report = old.sweep_tasks_once().await.unwrap();
    assert!(!report.leader);
    assert_eq!(TICKS.load(Ordering::SeqCst), 1);
    assert_eq!(ran(&leader.sweep_tasks_once().await.unwrap()), (1, 0));
    assert_eq!(ran(&leader.sweep_tasks_once().await.unwrap()), (0, 0));
    assert_eq!(TICKS.load(Ordering::SeqCst), 2);
    settle(&nodes, next).await;
    for node in &nodes {
        assert_eq!(count(node).await, Value::Number(2.0));
    }

    // A failing task backs off, its failure state replicated: every
    // member reports it, and the next leader goes on from it.
    leader
        .mutate(
            b"db",
            "app",
            "raise",
            &obj(vec![]),
            &MutationOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(ran(&leader.sweep_tasks_once().await.unwrap()), (0, 1));
    assert_eq!(ran(&leader.sweep_tasks_once().await.unwrap()), (0, 0));
    settle(&nodes, next).await;
    let fragile = |statuses: Vec<TaskStatus>| {
        statuses
            .into_iter()
            .find(|status| status.name == "fragile")
            .unwrap()
    };
    let mut retry = None;
    for node in &nodes {
        let failing = fragile(statuses(node).await);
        assert_eq!(failing.failures, 1);
        assert_eq!(failing.error.unwrap().code, "BOOM");
        assert!(retry.is_none() || retry == failing.retry_at_ms);
        retry = failing.retry_at_ms;
    }
    let retry = retry.unwrap();
    tokio::time::sleep(Duration::from_millis(
        openraft::Config::default().election_timeout_max,
    ))
    .await;
    assert_eq!(leader.transfer_leadership(first).await.unwrap().to, first);
    assert_eq!(wait_leader(&nodes).await, first);
    let leader = member(&nodes, first);
    let swept_at = now();
    let report = leader.sweep_tasks_once().await.unwrap();
    let failing = fragile(statuses(leader).await);
    match ran(&report) {
        // Still backing off under the new leader.
        (0, 0) => {
            assert!(swept_at < retry, "{swept_at} past {retry}");
            assert_eq!(failing.failures, 1);
        }
        // Past the backoff: one more failure, counted on from the
        // replicated state.
        (0, 1) => {
            assert!(now() >= retry);
            assert_eq!(failing.failures, 2);
        }
        other => panic!("unexpected pass {other:?}"),
    }
    assert_eq!(TICKS.load(Ordering::SeqCst), 2);
}

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// A task with many units of work runs them in batches of units the
/// writer prepares one on another's staged state: every unit runs, and
/// none twice.
#[tokio::test(flavor = "multi_thread")]
async fn many_units_run_together_once_each() {
    let dir = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let nodes = vec![open(1, dir.path(), &registry).await];
    nodes[0].initialize(BTreeSet::from([1])).await.unwrap();
    wait_leader(&nodes).await;
    let node = &nodes[0];
    node.deploy(b"db", "units", [7u8; 32], b"wasm", &manifest())
        .await
        .unwrap();
    node.mutate(
        b"db",
        "units",
        "fill",
        &obj(vec![("n", Value::Number(40.0))]),
        &MutationOptions::default(),
    )
    .await
    .unwrap();
    let read = |key: &str| {
        let key = key.to_string();
        async move {
            let engine = node.engine().lock().await;
            engine
                .invoke_query(
                    b"db",
                    "units",
                    "get",
                    &obj(vec![("key", s(&key))]),
                    &CallOptions::default(),
                )
                .unwrap()
        }
    };
    let (mut total, mut passes) = (0, 0);
    while read("left").await != Value::Number(0.0) {
        let report = node.sweep_tasks_once().await.unwrap();
        assert_eq!(report.failed, 0, "{report:?}");
        total += report.ran;
        passes += 1;
        assert!(passes < 100, "the drain never finished");
    }
    assert_eq!(total, 40);
    assert_eq!(read("done").await, Value::Number(40.0));
    assert_eq!(ran(&node.sweep_tasks_once().await.unwrap()), (0, 0));
}

/// The leader's task loop wakes as v1's maintenance does: a write that
/// makes a task due wakes it, and a task due later wakes it at that
/// time, long before its interval would.
#[tokio::test(flavor = "multi_thread")]
async fn the_task_loop_wakes_on_writes_and_due_times() {
    let dir = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let nodes = vec![open(1, dir.path(), &registry).await];
    nodes[0].initialize(BTreeSet::from([1])).await.unwrap();
    wait_leader(&nodes).await;
    let node = Arc::new(nodes.into_iter().next().unwrap());
    node.deploy(b"db", "later", [6u8; 32], b"wasm", &manifest())
        .await
        .unwrap();
    let (stop, stopped) = tokio::sync::watch::channel(false);
    let looping = {
        let node = node.clone();
        tokio::spawn(async move { node.run_tasks(Duration::from_secs(60), stopped).await })
    };
    let arm = |at: f64| {
        let node = node.clone();
        async move {
            node.mutate(
                b"db",
                "later",
                "arm",
                &obj(vec![("at", Value::Number(at))]),
                &MutationOptions::default(),
            )
            .await
            .unwrap();
        }
    };
    let reached = |expected: f64| {
        let node = node.clone();
        async move {
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            while std::time::Instant::now() < deadline {
                let engine = node.engine().lock().await;
                let count = engine
                    .invoke_query(
                        b"db",
                        "later",
                        "get",
                        &obj(vec![("key", s("count"))]),
                        &CallOptions::default(),
                    )
                    .unwrap();
                drop(engine);
                if count == Value::Number(expected) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            panic!("count never reached {expected}");
        }
    };
    // Let the loop's first pass go by with nothing to do.
    tokio::time::sleep(Duration::from_millis(300)).await;
    arm(0.0).await;
    reached(1.0).await;
    // Due in 600 ms: the write wakes the loop, which finds nothing due
    // yet and sleeps until then.
    let armed = now();
    arm((armed + 600) as f64).await;
    reached(2.0).await;
    assert!(now() >= armed + 600, "ran before it was due");
    stop.send(true).unwrap();
    looping.await.unwrap();
}
