//! Watch wakes, clock wakes and deltas over a live node: commits wake
//! only the watches whose results they can change, declared instants and
//! clock polls refresh without commits, and subscribers that ask get
//! JSON Patch deltas against the value they hold.

mod fixtures;

use fixtures::{field, hc, obj, s};

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use flower_cluster::network::{InProcessFactory, Registry};
use flower_cluster::node::Node;
use flower_cluster::statemachine::RuntimeFactory;
use flower_engine::engine::CallOptions;
use flower_engine::runtime::{CallKind, ClosureRuntime, HostCall};
use flower_sdk::abi::HostOp;
use flower_sdk::{Failure, Value, fail};
use flower_server::api::{AppState, serve};
use flower_server::watch::{Hub, HubEvent, HubMessage};
use tokio::sync::mpsc;

fn n(num: f64) -> Value {
    Value::Number(num)
}

/// Canonical key text of the `key` argument.
fn key_of(args: &Value) -> Result<String, Failure> {
    match field(args, "key") {
        Value::String(key) => Ok(format!("\"{key}\"")),
        _ => Err(fail("TYPE_MISMATCH", "needs a key")),
    }
}

/// A task's title, or null.
fn title(host: &mut dyn HostCall, key: &str) -> Result<Value, Failure> {
    match hc(host, HostOp::Get, vec![s(""), s("tasks"), s(key)])? {
        Value::Object(map) => Ok(map.get("title").cloned().unwrap_or(Value::Null)),
        _ => Ok(Value::Null),
    }
}

type Handler = Arc<
    dyn Fn(CallKind, &str, &str, &Value, &mut dyn HostCall) -> Result<Value, Failure> + Send + Sync,
>;

/// The test module. `slow` sleeps through its first evaluation, after
/// its snapshot is pinned.
fn handler(slept: Arc<AtomicBool>) -> Handler {
    Arc::new(
        move |kind: CallKind, _module: &str, name: &str, args: &Value, host: &mut dyn HostCall| {
            match (kind, name) {
                (CallKind::Mutation, "put") => {
                    let key = key_of(args)?;
                    hc(
                        host,
                        HostOp::Set,
                        vec![
                            s(""),
                            s("tasks"),
                            s(&key),
                            obj(vec![("title", field(args, "title"))]),
                        ],
                    )?;
                    Ok(s("ok"))
                }
                (CallKind::Query, "get") | (CallKind::Derived, "title") => {
                    title(host, &key_of(args)?)
                }
                (CallKind::Query, "slow") => {
                    if !slept.swap(true, Ordering::SeqCst) {
                        std::thread::sleep(Duration::from_millis(800));
                    }
                    title(host, &key_of(args)?)
                }
                (CallKind::Query, "via") => hc(
                    host,
                    HostOp::GetDerived,
                    vec![s("app"), s("title"), obj(vec![("key", field(args, "key"))])],
                ),
                (CallKind::Query, "list") => {
                    let page = hc(
                        host,
                        HostOp::Scan,
                        vec![s(""), s("tasks"), n(100.0), Value::Null],
                    )?;
                    let Value::Array(rows) = field(&page, "rows") else {
                        return Err(fail("TYPE_MISMATCH", "page rows"));
                    };
                    Ok(Value::Array(
                        rows.into_iter()
                            .map(|row| match row {
                                Value::Array(mut pair) if pair.len() == 2 => {
                                    field(&pair.remove(1), "title")
                                }
                                _ => Value::Null,
                            })
                            .collect(),
                    ))
                }
                (CallKind::Query, "gate") => {
                    let Value::Number(at) = field(args, "at") else {
                        return Err(fail("TYPE_MISMATCH", "gate needs at"));
                    };
                    let Value::Number(clock) = hc(host, HostOp::Clock, vec![])? else {
                        return Err(fail("TYPE_MISMATCH", "clock"));
                    };
                    if clock < at {
                        hc(host, HostOp::ChangesAt, vec![n(at)])?;
                    }
                    Ok(Value::Bool(clock >= at))
                }
                (CallKind::Query, "now") => hc(host, HostOp::Now, vec![]),
                _ => Err(fail("UNKNOWN_METHOD", "unknown definition")),
            }
        },
    )
}

#[derive(Clone)]
struct TestFactory {
    handler: Handler,
}

impl RuntimeFactory for TestFactory {
    fn build(
        &self,
        _db: &[u8],
        _module: &str,
        _wasm: &[u8],
        _manifest: &Value,
    ) -> Result<Box<dyn flower_engine::runtime::Runtime>, String> {
        let handler = self.handler.clone();
        Ok(Box::new(ClosureRuntime::new(
            move |kind, module, name, args, host| handler(kind, module, name, args, host),
        )))
    }
}

fn manifest() -> serde_json::Value {
    let definitions: Vec<serde_json::Value> = [
        ("put", "mutation"),
        ("get", "query"),
        ("slow", "query"),
        ("via", "query"),
        ("list", "query"),
        ("gate", "query"),
        ("now", "query"),
        ("title", "derived"),
    ]
    .into_iter()
    .map(|(name, kind)| serde_json::json!({"name": name, "kind": kind, "args": {"type": "json"}}))
    .collect();
    let aliases: Vec<serde_json::Value> = ["put", "get", "slow", "via", "list", "gate", "now"]
        .into_iter()
        .map(|name| serde_json::json!({"alias": name, "definition": name}))
        .collect();
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
        "definitions": definitions,
        "triggers": [],
        "http": aliases,
    })
}

struct Served {
    base: String,
    hub: Arc<Hub>,
    http: reqwest::Client,
}

/// Serve an initialized single node with the test module deployed and
/// `hub` built from it as the watch hub.
async fn serve_node(hub: impl FnOnce(Hub) -> Hub) -> (tempfile::TempDir, Served) {
    let dir = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let node = Node::open(
        1,
        &dir.path().join("n1"),
        Arc::new(openraft::Config::default()),
        InProcessFactory::new(registry.clone()),
        Arc::new(TestFactory {
            handler: handler(Arc::new(AtomicBool::new(false))),
        }),
    )
    .await
    .unwrap();
    registry.register(1, node.raft().clone());
    node.initialize(BTreeSet::from([1])).await.unwrap();
    for _ in 0..500 {
        if node.status().leader == Some(1) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let node = Arc::new(node);
    let hub = Arc::new(hub(Hub::new(node.clone())));
    let mut state = AppState::new(node.clone()).with_admin_token("secret".to_string());
    state.hub = hub.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(serve(listener, state));
    let http = reqwest::Client::new();
    for _ in 0..500 {
        if http.get(format!("{base}/v1/status")).send().await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let deployed = http
        .post(format!("{base}/v1/deploy"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({
            "db": "db",
            "module": "app",
            "wasm_b64": "e30=",
            "manifest": manifest(),
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(deployed.status(), 200);
    (dir, Served { base, hub, http })
}

impl Served {
    async fn put(&self, key: &str, title: &str) {
        let response = self
            .http
            .post(format!("{}/v1/mutate", self.base))
            .json(&serde_json::json!({
                "db": "db",
                "module": "app",
                "name": "put",
                "args": {"key": key, "title": title},
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
    }

    async fn watch(
        &self,
        name: &str,
        args: Value,
        deltas: bool,
    ) -> (HubEvent, mpsc::Receiver<HubMessage>) {
        let options = CallOptions {
            external: true,
            ..CallOptions::default()
        };
        self.hub
            .subscribe_with(b"db", "app", name, &args, &options, deltas)
            .await
            .unwrap()
    }
}

fn key(name: &str) -> Value {
    obj(vec![("key", s(name))])
}

/// The next message within `wait`, if any.
async fn next(rx: &mut mpsc::Receiver<HubMessage>, wait: Duration) -> Option<HubMessage> {
    tokio::time::timeout(wait, rx.recv()).await.ok().flatten()
}

/// The next snapshot's value as JSON, failing on anything else.
async fn snapshot(rx: &mut mpsc::Receiver<HubMessage>) -> serde_json::Value {
    match next(rx, Duration::from_secs(5)).await {
        Some(HubMessage::Snapshot(event)) => serde_json::from_slice(&event.value).unwrap(),
        other => panic!("expected a snapshot, got {other:?}"),
    }
}

#[tokio::test]
async fn commits_to_rows_a_watch_never_read_wake_nothing() {
    let (_dir, served) = serve_node(|hub| hub).await;
    served.put("a", "first").await;
    let (_, mut watch) = served.watch("get", key("a"), false).await;
    for other in ["b", "c", "d"] {
        served.put(other, "elsewhere").await;
    }
    assert_eq!(next(&mut watch, Duration::from_millis(500)).await, None);
    assert_eq!(served.hub.evaluations(), 0, "unrelated commits evaluated");
    served.put("a", "second").await;
    assert_eq!(snapshot(&mut watch).await, serde_json::json!("second"));
    assert_eq!(served.hub.evaluations(), 1);
}

#[tokio::test]
async fn derived_reads_and_scans_wake_on_what_they_cover() {
    let (_dir, served) = serve_node(|hub| hub).await;
    served.put("a", "first").await;
    let (_, mut via) = served.watch("via", key("a"), false).await;
    let (_, mut list) = served.watch("list", Value::Null, false).await;
    // A new row lands inside the scan, outside the derived value's read.
    served.put("b", "new").await;
    assert_eq!(
        snapshot(&mut list).await,
        serde_json::json!(["first", "new"])
    );
    assert_eq!(next(&mut via, Duration::from_millis(500)).await, None);
    assert_eq!(served.hub.evaluations(), 1);
    // The row the derived value read wakes it through its edges.
    served.put("a", "second").await;
    assert_eq!(snapshot(&mut via).await, serde_json::json!("second"));
    assert_eq!(
        snapshot(&mut list).await,
        serde_json::json!(["second", "new"])
    );
}

#[tokio::test]
async fn subscribers_that_ask_receive_patches_to_the_value_they_hold() {
    let (_dir, served) = serve_node(|hub| hub).await;
    let long = "a title long enough that a patch beats the whole list";
    served.put("a", long).await;
    served.put("b", long).await;
    let (first, mut patched) = served.watch("list", Value::Null, true).await;
    let (_, mut whole) = served.watch("list", Value::Null, false).await;
    served.put("c", "short").await;
    let expected = snapshot(&mut whole).await;
    assert_eq!(expected, serde_json::json!([long, long, "short"]));
    let Some(HubMessage::Delta(delta)) = next(&mut patched, Duration::from_secs(5)).await else {
        panic!("no delta");
    };
    assert_eq!(delta.base, first.id);
    let mut value: serde_json::Value = serde_json::from_slice(&first.value).unwrap();
    for operation in serde_json::from_str::<Vec<serde_json::Value>>(&delta.patch).unwrap() {
        assert_eq!(operation["op"], "add");
        let index: usize = operation["path"]
            .as_str()
            .unwrap()
            .trim_start_matches('/')
            .parse()
            .unwrap();
        value
            .as_array_mut()
            .unwrap()
            .insert(index, operation["value"].clone());
    }
    assert_eq!(value, expected);
    // The next patch names the identity the last one delivered.
    served.put("d", "more").await;
    let Some(HubMessage::Delta(next_delta)) = next(&mut patched, Duration::from_secs(5)).await
    else {
        panic!("no second delta");
    };
    assert_eq!(next_delta.base, delta.id);
}

#[tokio::test]
async fn declared_instants_refresh_without_commits() {
    let (_dir, served) = serve_node(|hub| hub).await;
    let at = flower_cluster::exec::now_ms() + 500;
    let (first, mut watch) = served
        .watch("gate", obj(vec![("at", n(at as f64))]), false)
        .await;
    assert_eq!(first.value, b"false");
    assert_eq!(snapshot(&mut watch).await, serde_json::json!(true));
    assert!(flower_cluster::exec::now_ms() >= at);
    // Open, it declares nothing more: no further refreshes.
    let evaluations = served.hub.evaluations();
    assert_eq!(next(&mut watch, Duration::from_millis(500)).await, None);
    assert_eq!(served.hub.evaluations(), evaluations);
}

#[tokio::test]
async fn clock_polls_refresh_on_the_poll_interval() {
    let (_dir, served) = serve_node(|hub| hub.with_poll_interval(Duration::from_millis(100))).await;
    let (first, mut watch) = served.watch("now", Value::Null, false).await;
    let mut last: serde_json::Value = serde_json::from_slice(&first.value).unwrap();
    for _ in 0..3 {
        let now = snapshot(&mut watch).await;
        assert!(now.as_f64().unwrap() > last.as_f64().unwrap());
        last = now;
    }
}

#[tokio::test]
async fn a_subscribers_deadline_bounds_its_first_snapshot_only() {
    // The request's deadline (`x-flower-timeout-ms`) is the subscribing
    // request's: refreshes after it, for this subscriber or any sharing
    // its entry, run without it.
    let (_dir, served) = serve_node(|hub| hub).await;
    served.put("a", "first").await;
    let options = CallOptions {
        external: true,
        deadline: Some(std::time::Instant::now() + Duration::from_millis(300)),
        ..CallOptions::default()
    };
    let (first, mut watch) = served
        .hub
        .subscribe_with(b"db", "app", "get", &key("a"), &options, false)
        .await
        .unwrap();
    assert_eq!(first.value, br#""first""#);
    let (_, mut sharing) = served.watch("get", key("a"), false).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    served.put("a", "second").await;
    assert_eq!(snapshot(&mut watch).await, serde_json::json!("second"));
    assert_eq!(snapshot(&mut sharing).await, serde_json::json!("second"));
}

#[tokio::test]
async fn watches_read_the_members_clock_not_a_pinned_one() {
    // A gateway may pin a call's clock; a watch outlives the call, so
    // it reads the member's clock, or a clock-reading query would never
    // change and a token never expire.
    let (_dir, served) = serve_node(|hub| hub.with_poll_interval(Duration::from_millis(100))).await;
    let pinned = 1_000u64;
    let options = CallOptions {
        external: true,
        timestamp_ms: Some(pinned),
        ..CallOptions::default()
    };
    let (first, mut watch) = served
        .hub
        .subscribe_with(b"db", "app", "now", &Value::Null, &options, false)
        .await
        .unwrap();
    let first: f64 = serde_json::from_slice(&first.value).unwrap();
    assert!(first > pinned as f64, "{first}");
    let later = snapshot(&mut watch).await.as_f64().unwrap();
    assert!(later > first, "{later} after {first}");
}

#[tokio::test]
async fn the_hub_bounds_what_its_watches_hold() {
    // Each watch holds its arguments and credentials (up to a request's
    // 2 MiB); the hub holds a bounded total and refuses past it, as an
    // overload to retry once watches end.
    let (_dir, served) = serve_node(|hub| hub.with_max_retained_bytes(8 * 1024)).await;
    served.put("a", "first").await;
    let heavy = CallOptions {
        external: true,
        credentials: Some(obj(vec![("token", s(&"x".repeat(3_000)))])),
        ..CallOptions::default()
    };
    let args = key("a");
    let subscribe = || {
        served
            .hub
            .subscribe_with(b"db", "app", "get", &args, &heavy, false)
    };
    let (_, one) = subscribe().await.unwrap();
    let (_, _two) = subscribe().await.unwrap();
    let refused = subscribe().await.unwrap_err();
    assert!(
        matches!(refused, flower_cluster::node::NodeError::Overloaded { .. }),
        "{refused:?}"
    );
    // A watch that ends gives its bytes back once the hub drops it, at
    // its next delivery.
    drop(one);
    served.put("a", "second").await;
    let mut admitted = false;
    for _ in 0..50 {
        if subscribe().await.is_ok() {
            admitted = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(admitted);
}

#[tokio::test]
async fn commits_handled_before_a_subscriber_registers_still_reach_it() {
    let (_dir, served) = serve_node(|hub| hub).await;
    served.put("a", "first").await;
    let hub = served.hub.clone();
    let subscribing = tokio::spawn(async move {
        let options = CallOptions {
            external: true,
            ..CallOptions::default()
        };
        hub.subscribe_with(b"db", "app", "slow", &key("a"), &options, false)
            .await
            .unwrap()
    });
    // Commit while the first evaluation sleeps on its pinned snapshot;
    // the hub handles the notice before the subscriber registers.
    tokio::time::sleep(Duration::from_millis(200)).await;
    served.put("a", "second").await;
    let (first, mut watch) = subscribing.await.unwrap();
    assert_eq!(first.value, br#""first""#);
    assert_eq!(snapshot(&mut watch).await, serde_json::json!("second"));
}

#[tokio::test]
async fn sse_streams_carry_identities_and_patches_when_asked() {
    let (_dir, served) = serve_node(|hub| hub).await;
    let long = "a title long enough that a patch beats the whole list";
    served.put("a", long).await;
    let args: serde_json::Value = serde_json::from_slice(b"null").unwrap();
    let open = |deltas: bool| {
        served
            .http
            .post(format!(
            "{}/v1/watch",
                served.base
        ))
        .json(&serde_json::json!({"db": "db", "module": "app", "name": "list", "args": args, "deltas": deltas}))
            .send()
    };
    let mut patched = open(true).await.unwrap();
    let mut whole = open(false).await.unwrap();
    // Read until the initial snapshot arrived on both.
    let mut patched_text = String::new();
    let mut whole_text = String::new();
    for (response, text) in [
        (&mut patched, &mut patched_text),
        (&mut whole, &mut whole_text),
    ] {
        while !text.contains("\n\n") {
            let chunk = tokio::time::timeout(Duration::from_secs(5), response.chunk())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            text.push_str(&String::from_utf8_lossy(&chunk));
        }
        assert!(text.contains("event: snapshot"), "{text}");
        assert!(text.contains("\"id\":"), "{text}");
    }
    served.put("b", long).await;
    for (response, text, event) in [
        (&mut patched, &mut patched_text, "event: patch"),
        (&mut whole, &mut whole_text, "event: snapshot"),
    ] {
        while text.matches("\n\n").count() < 2 {
            let chunk = tokio::time::timeout(Duration::from_secs(5), response.chunk())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            text.push_str(&String::from_utf8_lossy(&chunk));
        }
        let second = text.split("\n\n").nth(1).unwrap();
        assert!(second.contains(event), "{text}");
    }
    assert!(patched_text.contains("\"base\":"), "{patched_text}");
}
