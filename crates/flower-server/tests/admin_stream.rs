//! The operator event stream, `GET /v1/admin/stream`: a snapshot first,
//! then Raft transitions, readiness, catalog changes, metrics samples and
//! the rest as they happen, over three live members, without costing the
//! cluster reads or forwards.

mod fixtures;

use fixtures::{hc, s};

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use flower_cluster::network::{InProcessFactory, Registry};
use flower_cluster::node::Node;
use flower_cluster::statemachine::RuntimeFactory;
use flower_crypto::replicated::{ManagedKeys, WrappingKey};
use flower_engine::engine::MutationOptions;
use flower_engine::retention::RetentionCommand;
use flower_engine::runtime::{CallKind, ClosureRuntime, HostCall};
use flower_sdk::abi::HostOp;
use flower_sdk::{Value, fail};
use flower_server::admin_stream::{MAX_INTERVAL_MS, MAX_STREAMS, MIN_INTERVAL_MS};
use flower_server::api::{AppState, serve_until};
use openraft::ServerState;
use serde_json::json;

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
                        let mut row = std::collections::BTreeMap::new();
                        row.insert(
                            "title".to_string(),
                            map.get("title").cloned().unwrap_or(Value::Null),
                        );
                        hc(
                            host,
                            HostOp::Set,
                            vec![s(""), s("tasks"), key, Value::Object(row)],
                        )?;
                        Ok(s("put-ok"))
                    }
                    _ => Err(fail("UNKNOWN_METHOD", "unknown definition")),
                }
            },
        )))
    }
}

fn manifest() -> serde_json::Value {
    json!({
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

fn manifest_value() -> Value {
    flower_engine::json::parse_value(manifest().to_string().as_bytes()).unwrap()
}

/// Three members, each serving its own API until its stop fires.
struct Cluster {
    _dir: tempfile::TempDir,
    nodes: Vec<Arc<Node>>,
    bases: Vec<String>,
    stops: Vec<tokio::sync::watch::Sender<bool>>,
}

impl Cluster {
    async fn start() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let registry = Registry::new();
        let (mut nodes, mut bases, mut stops) = (Vec::new(), Vec::new(), Vec::new());
        for id in 1..=3u64 {
            let node = Node::open(
                id,
                &dir.path().join(format!("n{id}")),
                Arc::new(openraft::Config::default()),
                InProcessFactory::new(registry.clone()).with_source(id),
                Arc::new(App),
            )
            .await
            .unwrap()
            .with_key_catalog(Arc::new(ManagedKeys::new(
                Some(WrappingKey::new(&[8; 32])),
                Vec::new(),
            )));
            registry.register_peer(node.peer_service());
            let node = Arc::new(node);
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            bases.push(format!("http://{}", listener.local_addr().unwrap()));
            let (stop, mut stopped) = tokio::sync::watch::channel(false);
            let state = AppState::new(node.clone()).with_admin_token("secret".to_string());
            tokio::spawn(serve_until(
                listener,
                state,
                async move {
                    let _ = stopped.wait_for(|stop| *stop).await;
                },
                Duration::from_secs(1),
            ));
            nodes.push(node);
            stops.push(stop);
        }
        nodes[0]
            .initialize(BTreeSet::from([1, 2, 3]))
            .await
            .unwrap();
        let cluster = Cluster {
            _dir: dir,
            nodes,
            bases,
            stops,
        };
        let deployed = cluster
            .leader()
            .await
            .deploy(b"db", "app", [7u8; 32], b"wasm", &manifest_value())
            .await
            .unwrap()
            .position;
        // A follower's stream shows what it applied: every member holds
        // the deploy before a test opens one.
        let deadline = Instant::now() + Duration::from_secs(10);
        while cluster
            .nodes
            .iter()
            .any(|node| node.status().last_applied < Some(deployed))
        {
            assert!(
                Instant::now() < deadline,
                "the deploy never applied everywhere"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        cluster
    }

    /// The member every member agrees leads.
    async fn leader(&self) -> Arc<Node> {
        for _ in 0..500 {
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

    /// A member other than `id`.
    fn other(&self, id: u64) -> Arc<Node> {
        self.nodes
            .iter()
            .find(|node| node.id() != id)
            .unwrap()
            .clone()
    }

    fn base(&self, node: &Node) -> &str {
        &self.bases[(node.id() - 1) as usize]
    }
}

async fn put(node: &Node, key: &str) {
    let args = Value::Object(
        [
            ("key".to_string(), s(&format!("\"{key}\""))),
            ("title".to_string(), s("t")),
        ]
        .into_iter()
        .collect(),
    );
    node.mutate(b"db", "app", "put", &args, &MutationOptions::default())
        .await
        .unwrap();
}

/// What reading the stream gave.
#[derive(Debug)]
enum Next {
    Event(String, serde_json::Value),
    Ended,
    TimedOut,
}

/// An admin stream being read.
struct Events {
    response: reqwest::Response,
    buffer: String,
}

impl Events {
    async fn open(base: &str, query: &str) -> Self {
        let response = reqwest::Client::new()
            .get(format!("{base}/v1/admin/stream?{query}"))
            .bearer_auth("secret")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        assert_eq!(response.headers()["content-type"], "text/event-stream");
        Events {
            response,
            buffer: String::new(),
        }
    }

    /// The next event within `within`, skipping keepalive comments.
    async fn next(&mut self, within: Duration) -> Next {
        let deadline = tokio::time::Instant::now() + within;
        loop {
            while let Some(end) = self.buffer.find("\n\n") {
                let block: String = self.buffer.drain(..end + 2).collect();
                let mut name = None;
                let mut data = String::new();
                for line in block.lines() {
                    if let Some(value) = line.strip_prefix("event:") {
                        name = Some(value.trim().to_string());
                    } else if let Some(value) = line.strip_prefix("data:") {
                        data.push_str(value.strip_prefix(' ').unwrap_or(value));
                    }
                }
                if let Some(name) = name {
                    return Next::Event(name, serde_json::from_str(&data).unwrap());
                }
            }
            match tokio::time::timeout_at(deadline, self.response.chunk()).await {
                Ok(Ok(Some(chunk))) => self.buffer.push_str(std::str::from_utf8(&chunk).unwrap()),
                Ok(Ok(None)) => return Next::Ended,
                Ok(Err(error)) => panic!("stream failed: {error}"),
                Err(_) => return Next::TimedOut,
            }
        }
    }

    /// Read until an event `pick` takes, within `within`.
    async fn until(
        &mut self,
        within: Duration,
        mut pick: impl FnMut(&str, &serde_json::Value) -> bool,
    ) -> Option<serde_json::Value> {
        let deadline = Instant::now() + within;
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            match self.next(left).await {
                Next::Event(name, value) if pick(&name, &value) => return Some(value),
                Next::Event(..) => {}
                Next::Ended | Next::TimedOut => return None,
            }
        }
    }

    /// The opening snapshot.
    async fn snapshot(&mut self) -> serde_json::Value {
        match self.next(Duration::from_secs(10)).await {
            Next::Event(name, value) => {
                assert_eq!(name, "snapshot", "{value}");
                value
            }
            other => panic!("no snapshot: {other:?}"),
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_stream_opens_with_a_snapshot_of_every_section() {
    let cluster = Cluster::start().await;
    let leader = cluster.leader().await;
    let follower = cluster.other(leader.id());
    let mut events = Events::open(cluster.base(&follower), "queue=db/email").await;
    let snapshot = events.snapshot().await;
    for section in [
        "v",
        "reason",
        "at_ms",
        "interval_ms",
        "node",
        "readiness",
        "raft",
        "transitions",
        "membership",
        "metrics",
        "catalog",
        "backup",
        "work",
        "activity",
    ] {
        assert!(
            !snapshot[section].is_null(),
            "{section} missing: {snapshot}"
        );
        assert!(
            snapshot[section].get("error").is_none(),
            "{section}: {snapshot}"
        );
    }
    assert_eq!(snapshot["v"], 1);
    assert_eq!(snapshot["reason"], "connect");
    assert_eq!(snapshot["node"]["id"], follower.id());
    assert_eq!(snapshot["readiness"]["ok"], true);
    assert!(snapshot["metrics"]["at_ms"].as_u64().is_some());
    assert_eq!(snapshot["metrics"]["node"]["id"], follower.id());
    // The ring holds what this member saw: at least its current term.
    let transitions = snapshot["transitions"].as_array().unwrap();
    assert!(!transitions.is_empty());
    assert_eq!(
        transitions.last().unwrap()["leader"],
        json!(leader.id()),
        "{transitions:?}"
    );
    let catalog = &snapshot["catalog"];
    assert_eq!(catalog["leads"], false);
    let db = catalog["databases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|db| db["db"] == "db")
        .expect("the deployed database");
    assert_eq!(db["modules"][0]["name"], "app");
    assert!(db["retention"]["incarnation"].is_string());
    assert!(catalog["keys"]["databases"].is_object(), "{catalog}");
    assert_eq!(snapshot["work"]["queues"][0]["queue"], "email");
    assert_eq!(snapshot["work"]["timers"][0]["db"], "db");
    assert!(snapshot["activity"]["process_id"].is_string());
}

/// The poll this stream replaces saw status every 2 s: leadership moving
/// away and back inside that showed as a term bump at most.
#[tokio::test(flavor = "multi_thread")]
async fn a_quick_leadership_flip_delivers_both_transitions_in_order() {
    let cluster = Cluster::start().await;
    let first = cluster.leader().await;
    let second = cluster.other(first.id());
    let watcher = cluster
        .nodes
        .iter()
        .find(|node| node.id() != first.id() && node.id() != second.id())
        .unwrap()
        .clone();
    let mut events = Events::open(cluster.base(&watcher), "activity=0").await;
    let snapshot = events.snapshot().await;
    let term = snapshot["node"]["term"].as_u64().unwrap();
    // Away and straight back, each asked of the member leading then.
    let started = Instant::now();
    let away = first.transfer_leadership(second.id()).await;
    assert!(away.is_ok(), "{away:?}");
    let back = second.transfer_leadership(first.id()).await;
    assert!(back.is_ok(), "{back:?}");
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "the flip took {:?}: not inside one old poll interval",
        started.elapsed()
    );
    let mut leaders = Vec::new();
    let mut last_seq = 0;
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        let Next::Event(name, value) = events.next(Duration::from_millis(500)).await else {
            continue;
        };
        if name != "raft" {
            continue;
        }
        let seq = value["seq"].as_u64().unwrap();
        assert!(seq > last_seq, "transitions out of order");
        last_seq = seq;
        if let Some(leader) = value["leader"].as_u64()
            && leaders.last() != Some(&leader)
        {
            leaders.push(leader);
        }
        if leaders.ends_with(&[second.id(), first.id()]) {
            assert!(value["term"].as_u64().unwrap() >= term + 2, "{value}");
            return;
        }
    }
    panic!(
        "leaders seen {leaders:?}, not {} then {}",
        second.id(),
        first.id()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn catalog_changes_are_pushed_without_a_request() {
    let cluster = Cluster::start().await;
    let leader = cluster.leader().await;
    let follower = cluster.other(leader.id());
    let mut events = Events::open(cluster.base(&follower), "activity=0").await;
    events.snapshot().await;

    // A deployment.
    leader
        .deploy(b"other", "app", [9u8; 32], b"wasm", &manifest_value())
        .await
        .unwrap();
    let pushed = catalog(&mut events, |value: &serde_json::Value| {
        value["databases"]
            .as_array()
            .unwrap()
            .iter()
            .any(|db| db["db"] == "other" && db["modules"][0]["name"] == "app")
    })
    .await;
    let pushed = pushed.expect("no catalog event for a deployment");
    assert!(pushed["applied"].as_u64().is_some());

    // A key change.
    let changed = reqwest::Client::new()
        .post(format!("{}/v1/admin/keys", cluster.base(&leader)))
        .bearer_auth("secret")
        .json(&json!({"db": "db", "operation": "generate", "requestId": "g", "name": "signing", "algorithm": "Ed25519"}))
        .send()
        .await
        .unwrap();
    assert_eq!(changed.status(), 200);
    catalog(&mut events, |value: &serde_json::Value| {
        value["keys"]["databases"]["db"]["value"]["keys"]["signing"]["algorithm"] == "Ed25519"
    })
    .await
    .expect("no catalog event for a key change");

    // A retention change.
    leader
        .retention(
            b"db",
            RetentionCommand::Rotate {
                epoch_ms: Some(60_000),
                keep_epochs: 3,
            },
        )
        .await
        .unwrap()
        .unwrap();
    catalog(&mut events, |value: &serde_json::Value| {
        value["databases"]
            .as_array()
            .unwrap()
            .iter()
            .any(|db| db["db"] == "db" && db["retention"]["epoch_ms"] == 60_000)
    })
    .await
    .expect("no catalog event for a retention change");
}

#[tokio::test(flavor = "multi_thread")]
async fn readiness_transitions_are_pushed() {
    let cluster = Cluster::start().await;
    let leader = cluster.leader().await;
    let follower = cluster.other(leader.id());
    let mut events = Events::open(cluster.base(&follower), "activity=0").await;
    assert_eq!(events.snapshot().await["readiness"]["ok"], true);
    // Stall its applying (the state machine applies under the engine
    // lock) while the leader goes on committing with the other member.
    let stalled = follower.engine().clone().lock_owned().await;
    for index in 0..5 {
        put(&leader, &format!("k{index}")).await;
    }
    let behind = events
        .until(Duration::from_secs(10), |name, value| {
            name == "readiness" && value["ok"] == false
        })
        .await
        .expect("no readiness event for the stalled member");
    assert_eq!(behind["reason"], "applying committed entries");
    drop(stalled);
    events
        .until(Duration::from_secs(10), |name, value| {
            name == "readiness" && value["ok"] == true
        })
        .await
        .expect("no readiness event once it caught up");
}

#[tokio::test(flavor = "multi_thread")]
async fn metrics_arrive_at_the_interval_on_the_members_clock() {
    let cluster = Cluster::start().await;
    let leader = cluster.leader().await;
    let mut events = Events::open(cluster.base(&leader), "interval_ms=300&activity=0").await;
    assert_eq!(events.snapshot().await["interval_ms"], 300);
    let mut stamps = Vec::new();
    while stamps.len() < 5 {
        let sample = events
            .until(Duration::from_secs(5), |name, _| name == "metrics")
            .await
            .expect("no metrics sample");
        stamps.push(sample["at_ms"].as_u64().unwrap());
    }
    for pair in stamps.windows(2) {
        let gap = pair[1] - pair[0];
        assert!(
            (200..=600).contains(&gap),
            "samples {gap} ms apart: {stamps:?}"
        );
    }
    // Out-of-range intervals are clamped.
    let mut fast = Events::open(cluster.base(&leader), "interval_ms=1&activity=0").await;
    assert_eq!(fast.snapshot().await["interval_ms"], MIN_INTERVAL_MS);
    let mut slow = Events::open(cluster.base(&leader), "interval_ms=999999999&activity=0").await;
    assert_eq!(slow.snapshot().await["interval_ms"], MAX_INTERVAL_MS);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_stream_needs_the_admin_token_is_capped_and_ends_on_shutdown() {
    let cluster = Cluster::start().await;
    let leader = cluster.leader().await;
    let follower = cluster.other(leader.id());
    let base = cluster.base(&follower).to_string();
    let http = reqwest::Client::new();
    let anonymous = http
        .get(format!("{base}/v1/admin/stream"))
        .send()
        .await
        .unwrap();
    assert_eq!(anonymous.status(), 403);
    let wrong = http
        .get(format!("{base}/v1/admin/stream"))
        .bearer_auth("nope")
        .send()
        .await
        .unwrap();
    assert_eq!(wrong.status(), 403);
    let mut open = Vec::new();
    for _ in 0..MAX_STREAMS {
        let mut events = Events::open(&base, "activity=0").await;
        events.snapshot().await;
        open.push(events);
    }
    let refused = http
        .get(format!("{base}/v1/admin/stream"))
        .bearer_auth("secret")
        .send()
        .await
        .unwrap();
    assert_eq!(refused.status(), 429);
    let body: serde_json::Value = refused.json().await.unwrap();
    assert_eq!(body["code"], "resource_exhausted");
    // One closes, one more opens.
    drop(open.pop());
    let mut reopened = None;
    for _ in 0..50 {
        let response = http
            .get(format!("{base}/v1/admin/stream?activity=0"))
            .bearer_auth("secret")
            .send()
            .await
            .unwrap();
        if response.status() == 200 {
            reopened = Some(response);
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(reopened.is_some(), "a released slot was not reused");
    // Shutdown ends every stream.
    cluster.stops[(follower.id() - 1) as usize]
        .send(true)
        .unwrap();
    for events in &mut open {
        loop {
            match events.next(Duration::from_secs(5)).await {
                Next::Ended => break,
                Next::Event(..) => {}
                Next::TimedOut => panic!("a stream outlived shutdown"),
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_reader_that_never_reads_holds_nothing_back() {
    let cluster = Cluster::start().await;
    let leader = cluster.leader().await;
    let follower = cluster.other(leader.id());
    // Streams on both members, at the shortest interval, never read past
    // the response head.
    let stalled = [
        Events::open(
            cluster.base(&leader),
            &format!("interval_ms={MIN_INTERVAL_MS}"),
        )
        .await,
        Events::open(
            cluster.base(&follower),
            &format!("interval_ms={MIN_INTERVAL_MS}"),
        )
        .await,
    ];
    let started = Instant::now();
    for index in 0..300 {
        put(&leader, &format!("k{}", index % 20)).await;
    }
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_secs(30),
        "300 writes took {elapsed:?}"
    );
    // The follower applied all of them meanwhile.
    let target = leader.status().last_applied;
    let mut applied = false;
    for _ in 0..500 {
        if follower.status().last_applied >= target {
            applied = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(applied, "the follower fell behind a stalled stream");
    // Read again: what the socket held drains, then samples are fresh
    // (the server keeps no backlog of its own; see the unit tests for
    // the latest-sample and resync rules).
    tokio::time::sleep(Duration::from_secs(1)).await;
    for mut events in stalled {
        events.snapshot().await;
        let resumed = now_ms();
        let mut latest = 0;
        let deadline = Instant::now() + Duration::from_secs(3);
        while Instant::now() < deadline {
            if let Next::Event(name, value) = events.next(Duration::from_millis(100)).await
                && name == "metrics"
            {
                latest = value["at_ms"].as_u64().unwrap();
            }
        }
        assert!(
            latest + 1_000 >= resumed,
            "samples stuck at {latest}, resumed at {resumed}"
        );
    }
}

/// The member's clock, as samples stamp it.
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// The dashboard must not measure itself: a stream costs no quorum
/// round, no read fence and no forward to the leader.
#[tokio::test(flavor = "multi_thread")]
async fn streams_cost_no_reads_and_no_forwards() {
    let cluster = Cluster::start().await;
    let leader = cluster.leader().await;
    let follower = cluster.other(leader.id());
    let before: Vec<_> = cluster
        .nodes
        .iter()
        .map(|node| node.read_metrics())
        .collect();
    let mut on_follower =
        Events::open(cluster.base(&follower), "interval_ms=250&queue=db/email").await;
    let mut on_leader = Events::open(cluster.base(&leader), "interval_ms=250&queue=db/email").await;
    on_follower.snapshot().await;
    on_leader.snapshot().await;
    let deadline = Instant::now() + Duration::from_secs(3);
    while Instant::now() < deadline {
        on_follower.next(Duration::from_millis(50)).await;
        on_leader.next(Duration::from_millis(50)).await;
    }
    let after: Vec<_> = cluster
        .nodes
        .iter()
        .map(|node| node.read_metrics())
        .collect();
    for (node, (before, after)) in cluster.nodes.iter().zip(before.iter().zip(&after)) {
        assert_eq!(
            (
                after.quorum_rounds,
                after.remote_fences,
                after.fences_served,
                after.forwards
            ),
            (
                before.quorum_rounds,
                before.remote_fences,
                before.fences_served,
                before.forwards
            ),
            "member {} paid for the streams: {before:?} -> {after:?}",
            node.id()
        );
    }
}

/// The next catalog event `test` takes, within 10 s.
async fn catalog(
    events: &mut Events,
    test: impl Fn(&serde_json::Value) -> bool,
) -> Option<serde_json::Value> {
    events
        .until(Duration::from_secs(10), |name, value| {
            name == "catalog" && test(value)
        })
        .await
}
