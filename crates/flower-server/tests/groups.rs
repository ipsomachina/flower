//! Many groups behind one API per process: groups and partitions
//! created over the operator routes, and every request routed to the
//! group that owns the database it names, from any process, whether or
//! not that process hosts the group.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use flower_cluster::host::{Host, HostSettings, Transport};
use flower_cluster::network::Registry;
use flower_cluster::node::Node;
use flower_cluster::statemachine::RuntimeFactory;
use flower_cluster::txn::{CallWire, PrepareRequest, StepWire};
use flower_engine::runtime::{CallKind, ClosureRuntime, HostCall};
use flower_engine::txn::PlacedDb;
use flower_sdk::abi::{HostOp, decode_envelope};
use flower_sdk::{Failure, Value, fail};
use flower_server::api::AppState;
use flower_server::forward::LEADER_URL;
use flower_server::groups::{EPOCH_HEADER, GROUP_HEADER, HostApi, StateFactory, serve_host};
use flower_server::listener::HttpOptions;

fn s(text: &str) -> Value {
    Value::String(text.to_string())
}

fn hc(host: &mut dyn HostCall, op: HostOp, args: Vec<Value>) -> Result<Value, Failure> {
    let input = Value::Array(args).encode_binary().unwrap();
    let output = host.op(op as u32 as u8, &input);
    decode_envelope(Value::decode_binary(&output).unwrap())
}

/// `incr {key}` and linearizable `get {key}` over one counter
/// collection.
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
                let count = match hc(host, HostOp::Get, vec![s(""), s("counters"), key.clone()])? {
                    Value::Number(count) => count,
                    _ => 0.0,
                };
                match (kind, name) {
                    (CallKind::Mutation, "incr") => {
                        hc(
                            host,
                            HostOp::Set,
                            vec![s(""), s("counters"), key, Value::Number(count + 1.0)],
                        )?;
                        Ok(Value::Number(count + 1.0))
                    }
                    (CallKind::Query, "get") => Ok(Value::Number(count)),
                    _ => Err(fail("UNKNOWN_METHOD", "unknown definition")),
                }
            },
        )))
    }
}

fn manifest() -> serde_json::Value {
    serde_json::json!({
        "collections": [{
            "name": "counters",
            "key": {"type": "string"},
            "value": {"type": "json"},
            "indexes": [],
            "references": []
        }],
        "definitions": [
            {"name": "incr", "kind": "mutation", "args": {"type": "json"}},
            {"name": "get", "kind": "query", "args": {"type": "json"}, "consistency": "linearizable"}
        ],
        "triggers": [],
        "http": [
            {"alias": "incr", "definition": "incr"},
            {"alias": "get", "definition": "get"}
        ]
    })
}

/// Three processes, each a root member serving every group it hosts.
struct Cluster {
    _dir: tempfile::TempDir,
    hosts: Vec<Arc<Host>>,
    bases: Vec<String>,
    stops: Vec<tokio::sync::oneshot::Sender<()>>,
    served: Vec<tokio::task::JoinHandle<std::io::Result<()>>>,
}

impl Cluster {
    async fn start() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let registry = Registry::new();
        let (mut hosts, mut bases, mut stops, mut served) = (vec![], vec![], vec![], vec![]);
        for id in 1..=3u64 {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let base = format!("http://{}", listener.local_addr().unwrap());
            let mut settings = HostSettings::new(Arc::new(openraft::Config::default()));
            let url = base.clone();
            settings.configure = Some(Arc::new(move |_, node: Node| {
                node.set_api_url(Some(url.clone()));
                node
            }));
            let host = Arc::new(
                Host::open(
                    id,
                    &dir.path().join(format!("n{id}")),
                    Arc::new(App),
                    Transport::InProcess(registry.clone()),
                    settings,
                )
                .await
                .unwrap(),
            );
            let factory: StateFactory = {
                let host = host.clone();
                Arc::new(move |node| {
                    AppState::new(node)
                        .with_admin_token("secret".to_string())
                        .with_host(host.clone())
                })
            };
            let api = HostApi::new(host.clone(), factory);
            let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
            served.push(tokio::spawn(async move {
                serve_host(
                    listener,
                    api,
                    async move {
                        let _ = stopped.await;
                    },
                    Duration::from_secs(2),
                    &HttpOptions::default(),
                )
                .await
            }));
            hosts.push(host);
            bases.push(base);
            stops.push(stop);
        }
        hosts[0]
            .root()
            .initialize(BTreeSet::from([1, 2, 3]))
            .await
            .unwrap();
        for host in &hosts {
            host.start();
        }
        Cluster {
            _dir: dir,
            hosts,
            bases,
            stops,
            served,
        }
    }

    async fn stop(self) {
        for stop in self.stops {
            let _ = stop.send(());
        }
        for served in self.served {
            served.await.unwrap().unwrap();
        }
        for host in &self.hosts {
            host.stop_following().await;
        }
        for host in self.hosts {
            for _ in 0..200 {
                if Arc::strong_count(&host) == 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            let host = Arc::try_unwrap(host).unwrap_or_else(|_| panic!("a host is still shared"));
            host.shutdown().await.unwrap();
        }
    }
}

async fn post(http: &reqwest::Client, url: String, body: serde_json::Value) -> reqwest::Response {
    http.post(url)
        .bearer_auth("secret")
        .json(&body)
        .send()
        .await
        .unwrap()
}

/// POST until the answer is 200, as a client retrying would.
async fn post_ok(
    http: &reqwest::Client,
    url: String,
    body: serde_json::Value,
) -> (reqwest::header::HeaderMap, serde_json::Value) {
    let mut last = String::new();
    for _ in 0..200 {
        let response = post(http, url.clone(), body.clone()).await;
        let status = response.status();
        let headers = response.headers().clone();
        let text = response.text().await.unwrap();
        if status.is_success() {
            return (headers, serde_json::from_str(&text).unwrap());
        }
        last = format!("{status}: {text}");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("{url} never succeeded: {last}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn operator_plans_require_identities_and_replay_their_results() {
    let cluster = Cluster::start().await;
    let http = reqwest::Client::new();
    let base = &cluster.bases[0];
    for op in [None, Some("")] {
        for (route, body) in [
            (
                "partitions",
                serde_json::json!({"operation":"resize", "groups":["root"], "op":op}),
            ),
            (
                "groups",
                serde_json::json!({"operation":"prefer_leader", "group":"root", "node":1, "op":op}),
            ),
        ] {
            let response = post(&http, format!("{base}/v1/admin/{route}"), body).await;
            assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
        }
    }
    let url = format!("{base}/v1/admin/partitions");
    let resize = serde_json::json!({"operation":"resize", "groups":["root"], "op":"resize-one"});
    let (_, first) = post_ok(&http, url.clone(), resize.clone()).await;
    let (_, again) = post_ok(&http, url.clone(), resize).await;
    assert_eq!(first, again);
    assert_eq!(first["value"]["groups"], serde_json::json!(["root"]));
    assert_eq!(first["value"]["moves"], serde_json::json!([]));
    let reused = post(
        &http,
        url,
        serde_json::json!({"operation":"resize", "groups":["elsewhere"], "op":"resize-one"}),
    )
    .await;
    assert_eq!(reused.status(), reqwest::StatusCode::CONFLICT);
    let url = format!("{base}/v1/admin/groups");
    let (_, preference) = post_ok(&http, url.clone(), serde_json::json!({"operation":"prefer_leader", "group":"root", "node":1, "op":"prefer-one"})).await;
    assert_eq!(preference["value"]["node"], 1);
    let groups = get_json(&http, url.clone()).await;
    assert_eq!(groups["groups"][0]["preferred_leader"], 1);
    let (_, preference) = post_ok(&http, url, serde_json::json!({"operation":"prefer_leader", "group":"root", "node":null, "op":"clear-one"})).await;
    assert!(preference["value"]["node"].is_null());
    cluster.stop().await;
}

/// An admin stream being read.
struct Events {
    response: reqwest::Response,
    buffer: String,
    /// Events read but not yet taken by [`Events::until`].
    history: Vec<(String, serde_json::Value)>,
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
        Events {
            response,
            buffer: String::new(),
            history: Vec::new(),
        }
    }

    /// The next event within `within`, if any.
    async fn next(&mut self, within: Duration) -> Option<(String, serde_json::Value)> {
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
                    return Some((name, serde_json::from_str(&data).unwrap()));
                }
            }
            match tokio::time::timeout_at(deadline, self.response.chunk()).await {
                Ok(Ok(Some(chunk))) => self.buffer.push_str(std::str::from_utf8(&chunk).unwrap()),
                _ => return None,
            }
        }
    }

    /// The first event named `name` that `test` takes, within 20 s, in
    /// whatever order events of different names arrive: it and the
    /// events of that name before it are taken, the others kept.
    async fn until(
        &mut self,
        name: &str,
        test: impl Fn(&serde_json::Value) -> bool,
    ) -> serde_json::Value {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            if let Some(found) = self
                .history
                .iter()
                .position(|(event, value)| event == name && test(value))
            {
                let value = self.history.remove(found).1;
                let mut index = 0;
                self.history.retain(|(event, _)| {
                    index += 1;
                    index > found || event != name
                });
                return value;
            }
            let left = deadline.saturating_duration_since(tokio::time::Instant::now());
            match self.next(left).await {
                Some(event) => self.history.push(event),
                None => {
                    let last = self.history.iter().rev().find(|(event, _)| event == name);
                    let names: Vec<&str> = self
                        .history
                        .iter()
                        .map(|(event, _)| event.as_str())
                        .collect();
                    panic!("no {name} event as expected; the last: {last:?}; read: {names:?}");
                }
            }
        }
    }
}

/// The partition named `name` in a `partitions` section.
fn partition<'a>(section: &'a serde_json::Value, name: &str) -> Option<&'a serde_json::Value> {
    section["partitions"]
        .as_array()?
        .iter()
        .find(|entry| entry["name"] == name)
}

/// One replica's quorum rounds, remote fences, fences served and
/// forwards, by process and group.
type Paid = (u64, String, (u64, u64, u64, u64));

/// What every hosted replica has paid in fresh-read proofs and
/// forwards so far.
fn reads(cluster: &Cluster) -> Vec<Paid> {
    let mut paid = Vec::new();
    for host in &cluster.hosts {
        for node in host.groups() {
            let metrics = node.read_metrics();
            paid.push((
                host.id(),
                node.group().name.to_string(),
                (
                    metrics.quorum_rounds,
                    metrics.remote_fences,
                    metrics.fences_served,
                    metrics.forwards,
                ),
            ));
        }
    }
    paid
}

/// The root's admin stream carries every group the process hosts: the
/// groups and their Raft transitions, the partition catalog with epochs
/// and each step of a move, and transactions in doubt, pushed as they
/// change, without costing any group a read or a forward.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_root_stream_carries_every_group_partition_and_transaction() {
    let cluster = Cluster::start().await;
    let http = reqwest::Client::new();
    let one = &cluster.bases[0];
    // Process 2 will host east.
    let mut events = Events::open(&cluster.bases[1], "activity=0").await;
    let (name, snapshot) = events.next(Duration::from_secs(10)).await.unwrap();
    assert_eq!(name, "snapshot");
    assert_eq!(snapshot["groups"], serde_json::json!([]), "{snapshot}");
    assert_eq!(snapshot["partitions"]["groups"][0]["name"], "root");
    assert!(snapshot["transactions"]["groups"]["root"]["prepared"].is_array());
    post_ok(
        &http,
        format!("{one}/v1/admin/groups"),
        serde_json::json!({"operation": "create", "group": "east", "members": [2, 3]}),
    )
    .await;
    events
        .until("groups", |value| value["groups"][0]["name"] == "east")
        .await;
    let transition = events
        .until("group_raft", |value| value["group"] == "east")
        .await;
    assert_eq!(transition["uid"].as_str().unwrap().len(), 16);
    events
        .until("partitions", |value| {
            value["groups"]
                .as_array()
                .unwrap()
                .iter()
                .any(|group| group["name"] == "east" && group["status"] == "active")
        })
        .await;
    post_ok(
        &http,
        format!("{one}/v1/admin/partitions"),
        serde_json::json!({"operation": "create", "partition": "acme", "group": "east"}),
    )
    .await;
    events
        .until("partitions", |value| {
            partition(value, "acme")
                .is_some_and(|acme| acme["status"] == "active" && acme["epoch"] == 1)
        })
        .await;
    post_ok(
        &http,
        format!("{one}/v1/deploy"),
        serde_json::json!({
            "db": "acme",
            "module": "app",
            "identity": null,
            "wasm_b64": "e30=",
            "manifest": manifest(),
        }),
    )
    .await;
    // A prepare no coordinator began: in doubt until east's resolver
    // asks the root and aborts it.
    let east = cluster
        .hosts
        .iter()
        .find_map(|host| {
            host.group("east")
                .filter(|node| node.status().leader == Some(host.id()))
        })
        .unwrap();
    east.prepare_txn(PrepareRequest {
        id: "0f".into(),
        coordinator: "root".into(),
        coordinator_uid: 0,
        dbs: vec![PlacedDb {
            db: "acme".into(),
            epoch: 1,
            steps: vec![0],
        }],
        steps: vec![StepWire {
            index: 0,
            db: "acme".into(),
            module: "app".into(),
            method: "incr".into(),
            args: serde_json::json!({"key": "\"k\""}).to_string(),
        }],
        options: CallWire::default(),
    })
    .await
    .unwrap()
    .unwrap();
    drop(east);
    let in_doubt = events
        .until("transactions", |value| {
            value["groups"]["east"]["prepared"][0]["id"] == "0f"
        })
        .await;
    assert_eq!(
        in_doubt["groups"]["east"]["prepared"][0]["coordinator"],
        "root"
    );
    events
        .until("transactions", |value| {
            value["groups"]["east"]["prepared"] == serde_json::json!([])
        })
        .await;
    // A move to the root: each phase arrives, then the cutover's epoch.
    post_ok(
        &http,
        format!("{one}/v1/admin/partitions"),
        serde_json::json!({"operation": "move", "partition": "acme", "group": "root"}),
    )
    .await;
    // A phase arrives while the move runs, unless the whole move fits in
    // the stream's settle window (50 ms, as it often does here): then
    // the next sample shows it complete.
    let complete = |value: &serde_json::Value| {
        partition(value, "acme").is_some_and(|acme| acme["owner"] == "root")
            && value["moves"]
                .as_array()
                .is_some_and(|moves| moves.iter().any(|done| done["phase"] == "complete"))
    };
    let seen = events
        .until("partitions", |value| {
            partition(value, "acme").is_some_and(|acme| acme["movement"]["phase"].is_string())
                || complete(value)
        })
        .await;
    let moved = if complete(&seen) {
        seen
    } else {
        events.until("partitions", complete).await
    };
    assert_eq!(partition(&moved, "acme").unwrap()["epoch"], 2, "{moved}");
    assert_eq!(
        partition(&moved, "acme").unwrap()["movement"],
        serde_json::Value::Null
    );
    // Quiet now: streams on every process cost no group a read proof or
    // a forward.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let before = reads(&cluster);
    let mut streams = Vec::new();
    for base in &cluster.bases {
        streams.push(Events::open(base, "interval_ms=250").await);
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while tokio::time::Instant::now() < deadline {
        for stream in &mut streams {
            stream.next(Duration::from_millis(50)).await;
        }
        events.next(Duration::from_millis(10)).await;
    }
    assert_eq!(reads(&cluster), before);
    drop((events, streams));
    cluster.stop().await;
}

async fn get_json(http: &reqwest::Client, url: String) -> serde_json::Value {
    http.get(url)
        .bearer_auth("secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_process_routes_a_partition_to_its_group() {
    let cluster = Cluster::start().await;
    let http = reqwest::Client::new();
    let one = &cluster.bases[0];
    // `east` on nodes 2 and 3; node 1 hosts none of it.
    post_ok(
        &http,
        format!("{one}/v1/admin/groups"),
        serde_json::json!({"operation": "create", "group": "east", "members": [2, 3]}),
    )
    .await;
    let mut active = false;
    for _ in 0..400 {
        let groups = get_json(&http, format!("{one}/v1/admin/groups")).await;
        if groups["groups"].as_array().is_some_and(|groups| {
            groups
                .iter()
                .any(|g| g["name"] == "east" && g["status"] == "active")
        }) {
            active = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(active, "east never became active");
    post_ok(
        &http,
        format!("{one}/v1/admin/partitions"),
        serde_json::json!({"operation": "create", "partition": "acme", "group": "east"}),
    )
    .await;
    // Deploying and calling through node 1: forwarded to east's leader.
    post_ok(
        &http,
        format!("{one}/v1/deploy"),
        serde_json::json!({
            "db": "acme",
            "module": "app",
            "identity": null,
            "wasm_b64": "e30=",
            "manifest": manifest(),
        }),
    )
    .await;
    for (round, base) in cluster.bases.iter().enumerate() {
        let (headers, body) = post_ok(
            &http,
            format!("{base}/v1/mutate"),
            serde_json::json!({"db": "acme", "module": "app", "name": "incr", "args": {"key": "\"a\""}}),
        )
        .await;
        assert_eq!(body["value"].as_f64(), Some(round as f64 + 1.0), "{body}");
        assert_eq!(headers[GROUP_HEADER], "east");
        assert_eq!(headers[EPOCH_HEADER], "1");
        // The leader URL names east's leader, wherever the call came in.
        let leader = headers[LEADER_URL].to_str().unwrap().to_string();
        let east_leader = cluster
            .hosts
            .iter()
            .zip(&cluster.bases)
            .find(|(host, _)| {
                host.group("east")
                    .is_some_and(|node| node.status().leader == Some(host.id()))
            })
            .map(|(_, base)| base.clone())
            .unwrap();
        assert_eq!(leader, east_leader);
    }
    let (_, read) = post_ok(
        &http,
        format!("{one}/v1/query"),
        serde_json::json!({"db": "acme", "module": "app", "name": "get", "args": {"key": "\"a\""}}),
    )
    .await;
    assert_eq!(read["value"].as_f64(), Some(3.0));
    // An unplaced database stays in the root, as before.
    post_ok(
        &http,
        format!("{one}/v1/deploy"),
        serde_json::json!({
            "db": "plain",
            "module": "app",
            "identity": null,
            "wasm_b64": "e30=",
            "manifest": manifest(),
        }),
    )
    .await;
    let (headers, _) = post_ok(
        &http,
        format!("{}/v1/mutate", cluster.bases[2]),
        serde_json::json!({"db": "plain", "module": "app", "name": "incr", "args": {"key": "\"a\""}}),
    )
    .await;
    assert_eq!(headers[GROUP_HEADER], "root");
    // A transaction across both groups commits by two-phase commit from
    // the first step's group, whichever process it came in through.
    let (_, committed) = post_ok(
        &http,
        format!("{one}/v1/transaction"),
        serde_json::json!({"steps": [
            {"db": "acme", "module": "app", "method": "incr", "args": {"key": "\"a\""}},
            {"db": "plain", "module": "app", "method": "incr", "args": {"key": "\"a\""}},
        ]}),
    )
    .await;
    assert_eq!(committed["ok"], true, "{committed}");
    let counts: Vec<f64> = committed["value"]
        .as_array()
        .unwrap()
        .iter()
        .map(|count| count.as_f64().unwrap())
        .collect();
    assert_eq!(counts, vec![4.0, 2.0], "{committed}");
    // Each participant's revision and its group's log position.
    assert!(
        committed["revisions"]["acme"].as_u64().unwrap() > 0,
        "{committed}"
    );
    assert!(
        committed["revisions"]["plain"].as_u64().unwrap() > 0,
        "{committed}"
    );
    assert!(
        committed["positions"]["east"].as_u64().unwrap() > 0,
        "{committed}"
    );
    assert!(
        committed["positions"]["root"].as_u64().unwrap() > 0,
        "{committed}"
    );
    assert_eq!(committed["position"], committed["positions"]["east"]);
    // The coordinator group lists it, committed, until it completes.
    let mut listed = serde_json::Value::Null;
    for _ in 0..400 {
        listed = get_json(&http, format!("{}/v1/admin/transactions", cluster.bases[2])).await;
        let records = &listed["groups"]["east"]["coordinated"];
        if records[0]["complete"] == true {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let record = &listed["groups"]["east"]["coordinated"][0];
    assert_eq!(record["decision"], "commit", "{listed}");
    assert_eq!(record["complete"], true, "{listed}");
    assert_eq!(record["steps"], 2, "{listed}");
    assert_eq!(listed["groups"]["root"]["prepared"], serde_json::json!([]));
    // A decided transaction keeps its decision when an operator aborts
    // it; an unknown one is not found.
    let id = record["id"].as_str().unwrap().to_string();
    let (_, kept) = post_ok(
        &http,
        format!("{one}/v1/admin/transactions"),
        serde_json::json!({"operation": "abort", "transaction": id, "group": "east"}),
    )
    .await;
    assert_eq!(kept["value"]["decision"], "commit", "{kept}");
    let unknown = post(
        &http,
        format!("{one}/v1/admin/transactions"),
        serde_json::json!({"operation": "abort", "transaction": "00", "group": "east"}),
    )
    .await;
    assert_eq!(unknown.status(), reqwest::StatusCode::NOT_FOUND);
    // Readiness covers every group the process hosts.
    let ready = get_json(&http, format!("{}/health/ready", cluster.bases[1])).await;
    assert_eq!(ready["ok"], true, "{ready}");
    assert_eq!(ready["groups"]["east"]["ok"], true, "{ready}");
    let partitions = get_json(&http, format!("{one}/v1/admin/partitions")).await;
    assert_eq!(partitions["partitions"][0]["owner"], "east", "{partitions}");
    // Devices follow one group's history, from any process: east's log
    // holds the partition's deployment, the root's does not.
    let deployed = |sync: &serde_json::Value| {
        sync["entries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|entry| entry["write"]["Deploy"]["db"] == serde_json::json!(b"acme".to_vec()))
    };
    let east = get_json(&http, format!("{one}/v1/sync?since=0&limit=100&group=east")).await;
    assert!(deployed(&east), "{east}");
    let root = get_json(&http, format!("{one}/v1/sync?since=0&limit=100")).await;
    assert!(!deployed(&root), "{root}");
    cluster.stop().await;
}

/// A partition's modules are removed and purged in the group that owns
/// it, through any process, as the rest of the admin API routes: its
/// log carries the removal and the purge pages, its leader purges, and
/// the root group keeps running its own databases' modules.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_partition_removes_and_purges_its_modules_in_its_own_group() {
    let cluster = Cluster::start().await;
    let http = reqwest::Client::new();
    let one = &cluster.bases[0];
    // `east` on nodes 2 and 3; node 1 hosts none of it.
    post_ok(
        &http,
        format!("{one}/v1/admin/groups"),
        serde_json::json!({"operation": "create", "group": "east", "members": [2, 3]}),
    )
    .await;
    let mut active = false;
    for _ in 0..400 {
        let groups = get_json(&http, format!("{one}/v1/admin/groups")).await;
        if groups["groups"].as_array().is_some_and(|groups| {
            groups
                .iter()
                .any(|g| g["name"] == "east" && g["status"] == "active")
        }) {
            active = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(active, "east never became active");
    post_ok(
        &http,
        format!("{one}/v1/admin/partitions"),
        serde_json::json!({"operation": "create", "partition": "acme", "group": "east"}),
    )
    .await;
    for db in ["acme", "plain"] {
        post_ok(
            &http,
            format!("{one}/v1/deploy"),
            serde_json::json!({
                "db": db,
                "module": "app",
                "identity": null,
                "wasm_b64": "e30=",
                "manifest": manifest(),
            }),
        )
        .await;
        for key in ["a", "b", "c"] {
            post_ok(
                &http,
                format!("{one}/v1/mutate"),
                serde_json::json!({"db": db, "module": "app", "name": "incr", "args": {"key": format!("\"{key}\"")}}),
            )
            .await;
        }
    }

    // Through node 1, the removal reaches east and commits in its log.
    let (headers, removed) = post_ok(
        &http,
        format!("{one}/v1/admin/modules"),
        serde_json::json!({"operation": "remove", "db": "acme", "module": "app", "purge": true}),
    )
    .await;
    assert_eq!(headers[GROUP_HEADER], "east");
    assert_eq!(
        removed["value"],
        serde_json::json!({"purging": true, "removed": "app"}),
        "{removed}"
    );
    let listed = get_json(&http, format!("{one}/v1/admin/modules?db=acme")).await;
    assert_eq!(
        listed["databases"][0]["purging"][0]["name"], "app",
        "{listed}"
    );
    assert_eq!(listed["databases"][0]["modules"], serde_json::json!([]));
    let refused = post(
        &http,
        format!("{one}/v1/mutate"),
        serde_json::json!({"db": "acme", "module": "app", "name": "incr", "args": {"key": "\"a\""}}),
    )
    .await;
    assert!(!refused.status().is_success());

    // East's leader purges it; the root never sees it.
    let east = cluster
        .hosts
        .iter()
        .filter_map(|host| host.group("east"))
        .find(|node| node.status().leader == Some(node.id()))
        .expect("east has a leader");
    let mut pages = 0;
    while let Some(page) = east.purge_page(b"acme").await.unwrap() {
        let Ok(Value::Object(page)) = page else {
            panic!("a page answers an object: {page:?}");
        };
        assert_eq!(page["module"], Value::String("app".to_string()));
        pages += 1;
    }
    assert_eq!(pages, 1);
    let listed = get_json(&http, format!("{one}/v1/admin/modules?db=acme")).await;
    assert_eq!(listed["databases"][0]["purging"], serde_json::json!([]));
    let carries = |sync: &serde_json::Value, write: &str| {
        sync["entries"].as_array().unwrap().iter().any(|entry| {
            entry["write"][write]["db"] == serde_json::json!(b"acme".to_vec())
                || entry["write"]["Batch"]["items"]
                    .as_array()
                    .is_some_and(|items| {
                        items
                            .iter()
                            .any(|item| item[write]["db"] == serde_json::json!(b"acme".to_vec()))
                    })
        })
    };
    let east_log = get_json(&http, format!("{one}/v1/sync?since=0&limit=200&group=east")).await;
    assert!(carries(&east_log, "Remove"), "{east_log}");
    assert!(carries(&east_log, "Purge"), "{east_log}");
    let root_log = get_json(&http, format!("{one}/v1/sync?since=0&limit=200")).await;
    assert!(!carries(&root_log, "Remove") && !carries(&root_log, "Purge"));
    // The root's database keeps its module.
    let (headers, counted) = post_ok(
        &http,
        format!("{}/v1/mutate", cluster.bases[2]),
        serde_json::json!({"db": "plain", "module": "app", "name": "incr", "args": {"key": "\"a\""}}),
    )
    .await;
    assert_eq!(headers[GROUP_HEADER], "root");
    assert_eq!(counted["value"].as_f64(), Some(2.0));
    // Deployed again in the partition, it starts empty.
    post_ok(
        &http,
        format!("{one}/v1/deploy"),
        serde_json::json!({
            "db": "acme",
            "module": "app",
            "identity": null,
            "wasm_b64": "e30=",
            "manifest": manifest(),
        }),
    )
    .await;
    let (_, read) = post_ok(
        &http,
        format!("{one}/v1/query"),
        serde_json::json!({"db": "acme", "module": "app", "name": "get", "args": {"key": "\"a\""}}),
    )
    .await;
    assert_eq!(read["value"].as_f64(), Some(0.0));
    cluster.stop().await;
}
