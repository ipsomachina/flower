//! Request operations over HTTP: `Server-Timing` on answers, forwarded
//! ones included; client deadlines bounding waits on the member that
//! answers, in the leader's writer queue, and across a forward;
//! request bodies leased against the admission budget before they are
//! read; the activity stream.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use flower_cluster::admission::{AdmissionConfig, Class, WRITER_QUEUE};
use flower_cluster::network::{InProcessFactory, Registry};
use flower_cluster::node::Node;
use flower_cluster::statemachine::RuntimeFactory;
use flower_engine::runtime::{CallKind, ClosureRuntime, HostCall};
use flower_sdk::abi::{HostOp, decode_envelope};
use flower_sdk::{Failure, Value, fail};
use flower_server::api::{AppState, serve};
use openraft::ServerState;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn s(text: &str) -> Value {
    Value::String(text.to_string())
}

fn hc(host: &mut dyn HostCall, op: HostOp, args: Vec<Value>) -> Result<Value, Failure> {
    let input = Value::Array(args).encode_binary().unwrap();
    let output = host.op(op as u32 as u8, &input);
    decode_envelope(Value::decode_binary(&output).unwrap())
}

/// Whether the gate `parked` queries wait at is open, and how many
/// parked there.
static PARKED: std::sync::Mutex<(bool, usize)> = std::sync::Mutex::new((false, 0));
/// Signalled when the gate opens.
static OPENED: std::sync::Condvar = std::sync::Condvar::new();

/// Wait at the gate until it opens.
fn park() {
    let mut parked = PARKED.lock().unwrap();
    parked.1 += 1;
    while !parked.0 {
        parked = OPENED.wait(parked).unwrap();
    }
}

/// Open the gate for good.
fn open_gate() {
    PARKED.lock().unwrap().0 = true;
    OPENED.notify_all();
}

/// `put {key, title}`, `get {key}`, `parked {key}` (a `get` that
/// waits at the gate first) and `slow {ms}`.
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
                        let mut row = std::collections::BTreeMap::new();
                        row.insert("title".to_string(), title);
                        hc(
                            host,
                            HostOp::Set,
                            vec![s(""), s("tasks"), key, Value::Object(row)],
                        )?;
                        Ok(s("put-ok"))
                    }
                    // Puts `key` and deletes `from`.
                    (CallKind::Mutation, "move") => {
                        let title = map.get("title").cloned().unwrap_or(Value::Null);
                        let from = map.get("from").cloned().unwrap_or(Value::Null);
                        let mut row = std::collections::BTreeMap::new();
                        row.insert("title".to_string(), title);
                        hc(
                            host,
                            HostOp::Set,
                            vec![s(""), s("tasks"), key, Value::Object(row)],
                        )?;
                        hc(host, HostOp::Delete, vec![s(""), s("tasks"), from])?;
                        Ok(s("move-ok"))
                    }
                    (CallKind::Query, "get") => hc(host, HostOp::Get, vec![s(""), s("tasks"), key]),
                    // Answer their arguments, read and written alike.
                    (CallKind::Query, "echo") | (CallKind::Mutation, "echo_write") => {
                        Ok(args.clone())
                    }
                    (CallKind::Query, "parked") => {
                        let row = hc(host, HostOp::Get, vec![s(""), s("tasks"), key])?;
                        park();
                        Ok(row)
                    }
                    // Holds the writer for `ms` milliseconds.
                    (CallKind::Mutation, "slow") => {
                        let ms = match map.get("ms") {
                            Some(Value::Number(ms)) => *ms as u64,
                            _ => 0,
                        };
                        std::thread::sleep(Duration::from_millis(ms));
                        Ok(s("slow-ok"))
                    }
                    _ => Err(fail("UNKNOWN_METHOD", "unknown definition")),
                }
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
        "definitions": [
            {"name": "put", "kind": "mutation", "args": {"type": "json"}},
            {"name": "get", "kind": "query", "args": {"type": "json"}},
            {"name": "parked", "kind": "query", "args": {"type": "json"}},
            {"name": "echo", "kind": "query", "args": {"type": "json"}},
            {"name": "echo_write", "kind": "mutation", "args": {"type": "json"}},
            {"name": "slow", "kind": "mutation", "args": {"type": "json"}},
            {"name": "move", "kind": "mutation", "args": {"type": "json"}}
        ],
        "triggers": [],
        "http": [
            {"alias": "put", "definition": "put"},
            {"alias": "get", "definition": "get"},
            {"alias": "parked", "definition": "parked"},
            {"alias": "echo", "definition": "echo"},
            {"alias": "echo_write", "definition": "echo_write"},
            {"alias": "slow", "definition": "slow"},
            {"alias": "move", "definition": "move"}
        ]
    })
}

/// `size` members, each serving its own API.
struct Cluster {
    _dir: tempfile::TempDir,
    nodes: Vec<Arc<Node>>,
    bases: Vec<String>,
}

impl Cluster {
    async fn start(size: u64, admission: AdmissionConfig) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let registry = Registry::new();
        let mut nodes = Vec::new();
        let mut bases = Vec::new();
        for id in 1..=size {
            let node = Node::open(
                id,
                &dir.path().join(format!("n{id}")),
                Arc::new(openraft::Config::default()),
                InProcessFactory::new(registry.clone()),
                Arc::new(App),
            )
            .await
            .unwrap()
            .with_admission(admission.clone());
            registry.register_peer(node.peer_service());
            let node = Arc::new(node);
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let base = format!("http://{}", listener.local_addr().unwrap());
            let state = AppState::new(node.clone()).with_admin_token("secret".to_string());
            tokio::spawn(serve(listener, state));
            nodes.push(node);
            bases.push(base);
        }
        nodes[0]
            .initialize((1..=size).collect::<BTreeSet<_>>())
            .await
            .unwrap();
        let cluster = Cluster {
            _dir: dir,
            nodes,
            bases,
        };
        cluster.leader().await;
        let deployed = reqwest::Client::new()
            .post(format!("{}/v1/deploy", cluster.bases[0]))
            .header("authorization", "Bearer secret")
            .json(&serde_json::json!({
                "db": "db", "module": "app", "identity": null,
                "wasm_b64": "e30=", "manifest": manifest(),
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(deployed.status(), 200);
        cluster
    }

    /// The leader's index, once every member agrees.
    async fn leader(&self) -> usize {
        for _ in 0..500 {
            let leaders: BTreeSet<_> = self.nodes.iter().map(|node| node.status().leader).collect();
            if let [Some(leader)] = leaders.into_iter().collect::<Vec<_>>()[..] {
                let index = (leader - 1) as usize;
                if self.nodes[index].raft().metrics().borrow().state == ServerState::Leader {
                    return index;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("no agreed leader");
    }
}

/// A call body.
fn call(name: &str, key: &str) -> serde_json::Value {
    serde_json::json!({
        "db": "db", "module": "app", "name": name,
        "args": {"key": format!("\"{key}\""), "title": key},
    })
}

/// The `Server-Timing` metric names of an answer, in order.
fn timing(response: &reqwest::Response) -> Vec<String> {
    response
        .headers()
        .get("server-timing")
        .map(|value| value.to_str().unwrap())
        .unwrap_or_default()
        .split(", ")
        .filter(|metric| !metric.is_empty())
        .map(str::to_string)
        .collect()
}

fn names(metrics: &[String]) -> Vec<&str> {
    metrics
        .iter()
        .map(|metric| metric.split(';').next().unwrap())
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn answers_say_where_their_time_went() {
    let cluster = Cluster::start(2, AdmissionConfig::default()).await;
    let leader = cluster.leader().await;
    let follower = 1 - leader;
    let (leader_id, follower_id) = (leader as u64 + 1, follower as u64 + 1);
    let http = reqwest::Client::new();

    let put = http
        .post(format!("{}/v1/mutate", cluster.bases[leader]))
        .json(&call("put", "a"))
        .send()
        .await
        .unwrap();
    assert_eq!(put.status(), 200);
    // A write's wait for its turn in the leader's writer queue, its
    // preparation and its batch's commit, reported by the writer task,
    // the commit split into the leader's append, the quorum's commit and
    // the apply.
    let metrics = timing(&put);
    assert_eq!(
        names(&metrics),
        [
            "admission",
            "queue",
            "eval",
            "commit",
            "append",
            "replicate",
            "apply",
            "total",
            "node",
            "leader"
        ],
        "{metrics:?}"
    );
    assert!(metrics[8].ends_with(&format!("desc={leader_id}")));
    assert!(metrics[9].ends_with(&format!("desc={leader_id}")));
    // Durations carry one decimal.
    let ms = |metric: &str, name: &str| -> f64 {
        let value = metric.strip_prefix(&format!("{name};dur=")).unwrap();
        assert_eq!(value.split('.').nth(1).map(str::len), Some(1), "{value}");
        value.parse().unwrap()
    };
    let commit = ms(&metrics[3], "commit");
    // Each part lies within the commit (one decimal of rounding apart).
    for (metric, name) in [(&metrics[4], "append"), (&metrics[5], "replicate")] {
        assert!(ms(metric, name) <= commit + 0.1, "{metrics:?}");
    }
    assert!(ms(&metrics[5], "replicate") + ms(&metrics[6], "apply") <= commit + 0.2);

    // A query says whether the query cache answered it: first it
    // evaluates, then the cache answers the same query.
    let get = || {
        http.post(format!("{}/v1/query", cluster.bases[leader]))
            .json(&call("get", "a"))
            .send()
    };
    let first = timing(&get().await.unwrap());
    assert_eq!(
        names(&first),
        [
            "cache",
            "fence",
            "admission",
            "eval",
            "total",
            "node",
            "leader"
        ]
    );
    assert_eq!(first[0], "cache;desc=miss");
    let again = timing(&get().await.unwrap());
    assert_eq!(again[0], "cache;desc=hit", "{again:?}");

    // A follower forwards the write: its round trip comes first, then
    // the leader's phases, then its own total and identity.
    let forwarded = http
        .post(format!("{}/v1/mutate", cluster.bases[follower]))
        .json(&call("put", "b"))
        .send()
        .await
        .unwrap();
    assert_eq!(forwarded.status(), 200);
    let metrics = timing(&forwarded);
    assert_eq!(
        names(&metrics),
        [
            "forward",
            "admission",
            "queue",
            "eval",
            "commit",
            "append",
            "replicate",
            "apply",
            "total",
            "node",
            "leader"
        ],
        "{metrics:?}"
    );
    assert!(metrics[9].ends_with(&format!("desc={follower_id}")));
    assert!(metrics[10].ends_with(&format!("desc={leader_id}")));

    // Error answers carry it too; streams do not.
    let unknown = http
        .post(format!("{}/v1/query", cluster.bases[leader]))
        .json(&call("missing", "a"))
        .send()
        .await
        .unwrap();
    assert_eq!(unknown.status(), 404);
    assert!(names(&timing(&unknown)).contains(&"total"));
    let args: serde_json::Value = serde_json::from_slice(b"{\"key\":\"\\\"a\\\"\"}").unwrap();
    let watch = http
        .post(format!("{}/v1/watch", cluster.bases[leader]))
        .json(&serde_json::json!({"db": "db", "module": "app", "name": "get", "args": args}))
        .send()
        .await
        .unwrap();
    assert_eq!(watch.status(), 200);
    assert!(timing(&watch).is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn queries_and_mutations_write_a_value_with_the_same_bytes() {
    let cluster = Cluster::start(1, AdmissionConfig::default()).await;
    let base = cluster.bases[0].clone();
    let http = reqwest::Client::new();
    let args = serde_json::json!({"n": [1e21, 1e-7, 0.5, 3, -2.5e-300, 1e16]});
    let answer = |path: &str, name: &str| {
        let request = http
            .post(format!("{base}{path}"))
            .json(&serde_json::json!({"db": "db", "module": "app", "name": name, "args": args}));
        async move {
            let response = request.send().await.unwrap();
            assert_eq!(response.status(), 200);
            response.text().await.unwrap()
        }
    };
    // The value's bytes in an answer: what follows `"value":`, up to
    // the end of the object or the next field.
    let value = |body: &str| -> String {
        let rest = body
            .strip_prefix(r#"{"ok":true,"value":"#)
            .unwrap_or_else(|| panic!("{body}"));
        let end = rest.find(r#","revision""#).unwrap_or(rest.len() - 1);
        rest[..end].to_string()
    };
    let evaluated = value(&answer("/v1/query", "echo").await);
    let cached = value(&answer("/v1/query", "echo").await);
    let written = value(&answer("/v1/mutate", "echo_write").await);
    // Evaluated, cached and written alike, numbers print as
    // `serde_json` prints them: the API's contract.
    assert_eq!(evaluated, written);
    assert_eq!(cached, written);
    assert_eq!(
        written,
        r#"{"n":[1e+21,1e-7,0.5,3,-2.5e-300,10000000000000000]}"#
    );
}

/// Opens the gate when dropped, so a failing test releases its parked
/// queries instead of hanging on them.
struct Opens;

impl Drop for Opens {
    fn drop(&mut self) {
        open_gate();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn joined_queries_answer_as_cached_ones_and_say_they_waited() {
    const CALLS: usize = 4;
    let _opens = Opens;
    let cluster = Cluster::start(1, AdmissionConfig::default()).await;
    let (node, base) = (cluster.nodes[0].clone(), cluster.bases[0].clone());
    let http = reqwest::Client::new();
    let put = http
        .post(format!("{base}/v1/mutate"))
        .json(&call("put", "a"))
        .send()
        .await
        .unwrap();
    assert_eq!(put.status(), 200);
    let query = |name: &str| {
        let (http, base, body) = (http.clone(), base.clone(), call(name, "a"));
        async move {
            let response = http
                .post(format!("{base}/v1/query"))
                .json(&body)
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), 200);
            let metrics = timing(&response);
            (metrics, response.bytes().await.unwrap())
        }
    };
    // Identical calls at once: one evaluates, the others wait for it.
    let calls: Vec<_> = (0..CALLS).map(|_| tokio::spawn(query("parked"))).collect();
    let deadline = Instant::now() + Duration::from_secs(10);
    while node.flight_stats().joined < CALLS as u64 - 1 {
        assert!(Instant::now() < deadline, "{:?}", node.flight_stats());
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    assert_eq!(PARKED.lock().unwrap().1, 1);
    open_gate();
    let mut answers = Vec::new();
    for call in calls {
        answers.push(call.await.unwrap());
    }
    let expected: &[u8] = br#"{"ok":true,"value":{"title":"a"}}"#;
    let (mut led, mut joined) = (0, 0);
    for (metrics, body) in &answers {
        assert_eq!(body.as_ref(), expected);
        if metrics.iter().any(|metric| metric.starts_with("join;dur=")) {
            // A waiter answers from the cache, after its own proof.
            assert_eq!(
                names(metrics),
                [
                    "cache",
                    "fence",
                    "join",
                    "admission",
                    "eval",
                    "total",
                    "node",
                    "leader"
                ]
            );
            assert_eq!(metrics[0], "cache;desc=hit");
            joined += 1;
        } else {
            assert_eq!(metrics[0], "cache;desc=miss", "{metrics:?}");
            led += 1;
        }
    }
    assert_eq!((led, joined), (1, CALLS - 1));
    assert_eq!(PARKED.lock().unwrap().1, 1);
    // A call after them answers from the cache with the same bytes, and
    // with the cache off an evaluation renders exactly them too.
    let (metrics, body) = query("parked").await;
    assert_eq!(
        (metrics[0].as_str(), body.as_ref()),
        ("cache;desc=hit", expected)
    );
    node.reader().set_query_cache_bytes(0);
    let (metrics, body) = query("parked").await;
    assert_eq!(
        (metrics[0].as_str(), body.as_ref()),
        ("cache;desc=miss", expected)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn client_deadlines_bound_waits_here_and_across_a_forward() {
    let cluster = Cluster::start(2, AdmissionConfig::default()).await;
    let leader = cluster.leader().await;
    let follower = 1 - leader;
    let http = reqwest::Client::new();

    let malformed = http
        .post(format!("{}/v1/mutate", cluster.bases[leader]))
        .header("x-flower-timeout-ms", "soon")
        .json(&call("put", "a"))
        .send()
        .await
        .unwrap();
    assert_eq!(malformed.status(), 400);

    // Every place in the leader's writer queue is taken: a write waits
    // for one, up to ten seconds without a deadline.
    let mut held = Vec::new();
    for _ in 0..WRITER_QUEUE {
        held.push(
            cluster.nodes[leader]
                .admission()
                .acquire(Class::Writer, b"db", 0)
                .await
                .unwrap(),
        );
    }
    for (base, label) in [
        (&cluster.bases[leader], "at the leader"),
        (&cluster.bases[follower], "through a follower"),
    ] {
        let started = Instant::now();
        let refused = http
            .post(format!("{base}/v1/mutate"))
            .header("x-flower-timeout-ms", "400")
            .json(&call("put", "b"))
            .send()
            .await
            .unwrap();
        let elapsed = started.elapsed();
        assert_eq!(refused.status(), 503, "{label}");
        assert!(refused.headers().contains_key("retry-after"));
        let body: serde_json::Value = refused.json().await.unwrap();
        assert_eq!(body["code"], "overloaded", "{label}: {body}");
        assert!(
            body["message"]
                .as_str()
                .unwrap()
                .contains("client deadline"),
            "{label}: {body}"
        );
        assert!(
            elapsed >= Duration::from_millis(300) && elapsed < Duration::from_secs(5),
            "{label}: answered after {elapsed:?}"
        );
    }
    drop(held);
    let metrics = cluster.nodes[leader].admission().metrics();
    assert_eq!(metrics.shed.deadline, 2);
    // With room again, a write within its deadline commits.
    let put = http
        .post(format!("{}/v1/mutate", cluster.bases[follower]))
        .header("x-flower-timeout-ms", "10000")
        .json(&call("put", "c"))
        .send()
        .await
        .unwrap();
    assert_eq!(put.status(), 200);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_write_past_its_deadline_in_the_writer_queue_never_runs() {
    let cluster = Cluster::start(2, AdmissionConfig::default()).await;
    let leader = cluster.leader().await;
    let follower = 1 - leader;
    let http = reqwest::Client::new();
    for (base, key, label) in [
        (&cluster.bases[leader], "late-here", "at the leader"),
        (&cluster.bases[follower], "late-there", "through a follower"),
    ] {
        // A slow write holds the writer; the next one has a place but
        // waits in the writer's queue past its deadline.
        let slow = tokio::spawn({
            let (http, base) = (http.clone(), cluster.bases[leader].clone());
            async move {
                http.post(format!("{base}/v1/mutate"))
                    .json(&serde_json::json!({
                        "db": "db", "module": "app", "name": "slow", "args": {"ms": 800},
                    }))
                    .send()
                    .await
                    .unwrap()
                    .status()
            }
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        let refused = http
            .post(format!("{base}/v1/mutate"))
            .header("x-flower-timeout-ms", "300")
            .json(&call("put", key))
            .send()
            .await
            .unwrap();
        assert_eq!(refused.status(), 503, "{label}");
        let body: serde_json::Value = refused.json().await.unwrap();
        assert!(
            body["message"]
                .as_str()
                .unwrap()
                .contains("client deadline"),
            "{label}: {body}"
        );
        assert_eq!(slow.await.unwrap(), 200, "{label}");
        // Refused at its turn, before it prepared: it never ran.
        let get = http
            .post(format!("{}/v1/query", cluster.bases[leader]))
            .json(&call("get", key))
            .send()
            .await
            .unwrap();
        let body: serde_json::Value = get.json().await.unwrap();
        assert_eq!(body["ok"], true, "{label}: {body}");
        assert_eq!(body["value"], serde_json::Value::Null, "{label}: {body}");
    }
    let metrics = cluster.nodes[leader].admission().metrics();
    assert_eq!(metrics.shed.deadline, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn bodies_lease_their_bytes_before_they_are_read() {
    let admission = AdmissionConfig {
        queued_input_bytes: 4096,
        ..AdmissionConfig::default()
    };
    let cluster = Cluster::start(1, admission).await;
    let address = cluster.bases[0]
        .strip_prefix("http://")
        .unwrap()
        .to_string();
    // A client declares 3,000 bytes and stalls after the headers: the
    // member leases them at once, before reading any.
    let mut stalled = tokio::net::TcpStream::connect(&address).await.unwrap();
    stalled
        .write_all(
            format!(
                "POST /v1/mutate HTTP/1.1\r\nhost: {address}\r\ncontent-type: application/json\r\n\
                 content-length: 3000\r\n\r\n{{\"db\":"
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let node = &cluster.nodes[0];
    for _ in 0..500 {
        if node.admission().metrics().ingress_bytes > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let metrics = node.admission().metrics();
    assert_eq!(metrics.ingress_bytes, 3000 + 512);
    // Another body that would not fit beside it is refused before it is
    // read, though it alone would fit.
    let http = reqwest::Client::new();
    let mut padded = call("put", "a");
    padded["args"]["title"] = serde_json::Value::String("x".repeat(700));
    let refused = http
        .post(format!("{}/v1/mutate", cluster.bases[0]))
        .json(&padded)
        .send()
        .await
        .unwrap();
    assert_eq!(refused.status(), 503);
    let body: serde_json::Value = refused.json().await.unwrap();
    assert_eq!(body["code"], "overloaded");
    // The stalled client gives up: its lease goes, and the body fits.
    drop(stalled);
    for _ in 0..500 {
        if node.admission().metrics().ingress_bytes == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(node.admission().metrics().ingress_bytes, 0);
    let put = http
        .post(format!("{}/v1/mutate", cluster.bases[0]))
        .json(&padded)
        .send()
        .await
        .unwrap();
    assert_eq!(put.status(), 200);
    // Bodies past the call limit are refused unread.
    let mut huge = tokio::net::TcpStream::connect(&address).await.unwrap();
    huge.write_all(
        format!(
            "POST /v1/mutate HTTP/1.1\r\nhost: {address}\r\ncontent-type: application/json\r\n\
             content-length: 99999999\r\nconnection: close\r\n\r\n"
        )
        .as_bytes(),
    )
    .await
    .unwrap();
    let mut answer = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(10), huge.read_to_end(&mut answer)).await;
    let answer = String::from_utf8_lossy(&answer);
    assert!(answer.starts_with("HTTP/1.1 413"), "{answer}");
    assert_eq!(node.admission().metrics().ingress_bytes, 0);
}

/// Read one SSE dispatch (up to a blank line), skipping comments.
async fn next_event(response: &mut reqwest::Response, buffered: &mut String) -> String {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(end) = buffered.find("\n\n") {
                let event: String = buffered.drain(..end + 2).collect();
                if event.starts_with(':') {
                    continue;
                }
                return event;
            }
            let chunk = response.chunk().await.unwrap().expect("the stream ended");
            buffered.push_str(&String::from_utf8_lossy(&chunk));
        }
    })
    .await
    .expect("no event in time")
}

#[tokio::test(flavor = "multi_thread")]
async fn operators_watch_writes_and_watches_as_they_happen() {
    let cluster = Cluster::start(1, AdmissionConfig::default()).await;
    let base = &cluster.bases[0];
    let admin = |request: reqwest::RequestBuilder| request.header("authorization", "Bearer secret");
    let http = reqwest::Client::new();
    let snapshot = || async {
        let response = admin(http.get(format!("{base}/v1/admin/activity")))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        response.json::<serde_json::Value>().await.unwrap()
    };
    let before = snapshot().await;
    assert_eq!(before["version"], 1);

    let mut stream = admin(http.get(format!("{base}/v1/admin/activity/stream")))
        .send()
        .await
        .unwrap();
    assert_eq!(stream.status(), 200);
    assert_eq!(stream.headers()["cache-control"], "no-store");
    let mut buffered = String::new();
    let first = next_event(&mut stream, &mut buffered).await;
    assert!(first.starts_with("event: snapshot\n"), "{first}");

    // A write shows up, on the stream too.
    let put = http
        .post(format!("{base}/v1/mutate"))
        .json(&call("put", "a"))
        .send()
        .await
        .unwrap();
    assert_eq!(put.status(), 200);
    let revision = put.json::<serde_json::Value>().await.unwrap()["revision"].clone();
    let pushed = next_event(&mut stream, &mut buffered).await;
    assert!(pushed.contains("\"writes\""), "{pushed}");
    let after = snapshot().await;
    let total = |snapshot: &serde_json::Value| snapshot["writes"]["total"].as_u64().unwrap();
    assert_eq!(total(&after), total(&before) + 1);
    let last = |snapshot: &serde_json::Value| {
        snapshot["writes"]["entries"]
            .as_array()
            .unwrap()
            .last()
            .unwrap()
            .clone()
    };
    let put_entry = last(&after);
    assert_eq!(
        (put_entry["db"].clone(), put_entry["revision"].clone()),
        ("db".into(), revision)
    );
    // Each write says the records it put and deleted.
    let records = |entry: &serde_json::Value| {
        (
            entry["puts"].as_u64().unwrap(),
            entry["deletes"].as_u64().unwrap(),
        )
    };
    assert_eq!(records(&put_entry), (1, 0), "{put_entry}");
    let moved = http
        .post(format!("{base}/v1/mutate"))
        .json(&serde_json::json!({
            "db": "db", "module": "app", "name": "move",
            "args": {"key": "\"b\"", "title": "b", "from": "\"a\""},
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(moved.status(), 200);
    let mut moving = snapshot().await;
    for _ in 0..500 {
        if total(&moving) == total(&before) + 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
        moving = snapshot().await;
    }
    assert_eq!(records(&last(&moving)), (1, 1), "{moving}");
    let sums = |snapshot: &serde_json::Value| records(&snapshot["writes"]);
    assert_eq!(sums(&moving).0, sums(&before).0 + 2);
    assert_eq!(sums(&moving).1, sums(&before).1 + 1);

    // A live watch counts until its client leaves.
    let args: serde_json::Value = serde_json::from_slice(b"{\"key\":\"\\\"a\\\"\"}").unwrap();
    let mut watch = http
        .post(format!("{base}/v1/watch"))
        .json(&serde_json::json!({"db": "db", "module": "app", "name": "get", "args": args}))
        .send()
        .await
        .unwrap();
    let mut watch_buffer = String::new();
    next_event(&mut watch, &mut watch_buffer).await;
    let watching = snapshot().await;
    assert_eq!(watching["watches"]["active"], 1);
    let entry = &watching["watches"]["entries"][0];
    assert_eq!(
        (entry["db"].clone(), entry["name"].clone()),
        ("db".into(), "get".into())
    );
    drop(watch);
    for _ in 0..500 {
        if snapshot().await["watches"]["active"] == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let ended = snapshot().await;
    assert_eq!(
        (
            ended["watches"]["active"].clone(),
            ended["watches"]["ended"].clone()
        ),
        (0.into(), 1.into())
    );

    // Streams are bounded, and operator-only.
    let mut open = Vec::new();
    for _ in 1..16 {
        let response = admin(http.get(format!("{base}/v1/admin/activity/stream")))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        open.push(response);
    }
    let refused = admin(http.get(format!("{base}/v1/admin/activity/stream")))
        .send()
        .await
        .unwrap();
    assert_eq!(refused.status(), 429);
    let body: serde_json::Value = refused.json().await.unwrap();
    assert_eq!(body["code"], "resource_exhausted");
    let anonymous = http
        .get(format!("{base}/v1/admin/activity"))
        .send()
        .await
        .unwrap();
    assert_eq!(anonymous.status(), 403);
    // Leaving frees a stream's slot.
    drop(stream);
    let mut reopened = None;
    for _ in 0..500 {
        let response = admin(http.get(format!("{base}/v1/admin/activity/stream")))
            .send()
            .await
            .unwrap();
        if response.status() == 200 {
            reopened = Some(response);
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(reopened.is_some(), "a closed stream kept its slot");
}

#[tokio::test(flavor = "multi_thread")]
async fn metrics_say_what_each_method_cost() {
    let cluster = Cluster::start(1, AdmissionConfig::default()).await;
    let base = &cluster.bases[0];
    let http = reqwest::Client::new();
    // A database of this test's own: the table is the process's.
    let deployed = http
        .post(format!("{base}/v1/deploy"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({
            "db": "costs", "module": "app", "identity": null,
            "wasm_b64": "e30=", "manifest": manifest(),
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(deployed.status(), 200);
    let call = |name: &str, args: serde_json::Value| {
        http.post(format!(
            "{base}/v1/{}",
            if name == "get" { "query" } else { "mutate" }
        ))
        .json(&serde_json::json!({"db": "costs", "module": "app", "name": name, "args": args}))
        .send()
    };
    for key in ["a", "b"] {
        let put = call(
            "put",
            serde_json::json!({"key": format!("\"{key}\""), "title": key}),
        )
        .await
        .unwrap();
        assert_eq!(put.status(), 200);
    }
    // The same query twice: evaluated once, then answered by the cache.
    for _ in 0..2 {
        let get = call("get", serde_json::json!({"key": "\"a\""}))
            .await
            .unwrap();
        assert_eq!(get.status(), 200);
    }
    // A name no method has takes no row.
    let missing = call("nothing", serde_json::json!({})).await.unwrap();
    assert!(missing.status().is_client_error() || missing.status().is_server_error());
    let metrics: serde_json::Value = http
        .get(format!("{base}/v1/metrics"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let methods = &metrics["methods"];
    assert_eq!(methods["limit"], 512, "{methods:#}");
    let row = |name: &str| {
        methods["byWeight"]
            .as_array()
            .unwrap()
            .iter()
            .find(|row| row["db"] == "costs" && row["method"] == name)
            .cloned()
    };
    let put = row("put").expect("put has a row");
    assert_eq!(put["module"], "app");
    let mutations = &put["mutations"];
    assert_eq!(mutations["finished"], 2, "{put:#}");
    assert_eq!(mutations["ok"], 2);
    assert_eq!(mutations["evaluation"]["count"], 2);
    assert_eq!(mutations["evaluationCpu"]["count"], 2);
    assert_eq!(mutations["queueWait"]["count"], 2);
    assert_eq!(mutations["total"]["ok"]["count"], 2);
    assert!(mutations["total"]["ok"]["p99UpperMs"].as_u64().unwrap() >= 1);
    let get = row("get").expect("get has a row");
    assert_eq!(get["queries"]["evaluated"]["count"], 1, "{get:#}");
    assert_eq!(get["queries"]["cacheHits"], 1);
    assert!(row("nothing").is_none(), "{methods:#}");
    // The sum covers every method.
    assert!(methods["all"]["mutations"]["finished"].as_u64().unwrap() >= 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn activity_records_every_write_of_a_batch() {
    let cluster = Cluster::start(1, AdmissionConfig::default()).await;
    let base = cluster.bases[0].clone();
    let http = reqwest::Client::new();
    let snapshot = || async {
        http.get(format!("{base}/v1/admin/activity"))
            .header("authorization", "Bearer secret")
            .send()
            .await
            .unwrap()
            .json::<serde_json::Value>()
            .await
            .unwrap()
    };
    let total = || async { snapshot().await["writes"]["total"].as_u64().unwrap() };
    let before = total().await;
    // A slow write holds the writer while sixteen more queue behind it:
    // they commit together, as one batch entry.
    let slow = tokio::spawn({
        let (http, base) = (http.clone(), base.clone());
        async move {
            http.post(format!("{base}/v1/mutate"))
                .json(&serde_json::json!({
                    "db": "db", "module": "app", "name": "slow", "args": {"ms": 300},
                }))
                .send()
                .await
                .unwrap()
                .status()
        }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut puts = Vec::new();
    for key in 0..16 {
        let (http, base) = (http.clone(), base.clone());
        puts.push(tokio::spawn(async move {
            http.post(format!("{base}/v1/mutate"))
                .json(&call("put", &format!("k{key}")))
                .send()
                .await
                .unwrap()
                .json::<serde_json::Value>()
                .await
                .unwrap()["revision"]
                .as_u64()
                .unwrap()
        }));
    }
    assert_eq!(slow.await.unwrap(), 200);
    let mut revisions = BTreeSet::new();
    for put in puts {
        revisions.insert(put.await.unwrap());
    }
    assert_eq!(revisions.len(), 16, "one revision each: {revisions:?}");
    let writer = cluster.nodes[0].writer_stats();
    assert!(
        writer.largest >= 2,
        "the puts never shared a batch: {writer:?}"
    );
    // Every put that moved the revision is one recorded write.
    let mut recorded = 0;
    for _ in 0..500 {
        recorded = total().await - before;
        if recorded >= 16 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        (16..=17).contains(&recorded),
        "{recorded} writes recorded for sixteen puts and a slow write"
    );
    // Each put of the batch put its own record, and only it.
    let recorded = snapshot().await;
    let records: Vec<(u64, u64)> = recorded["writes"]["entries"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|entry| revisions.contains(&entry["revision"].as_u64().unwrap()))
        .map(|entry| {
            (
                entry["puts"].as_u64().unwrap(),
                entry["deletes"].as_u64().unwrap(),
            )
        })
        .collect();
    assert_eq!(records, vec![(1, 0); 16], "{recorded}");
}
