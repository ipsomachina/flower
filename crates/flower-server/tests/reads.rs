//! Reads and routing over HTTP across members: any member answers
//! queries the way they declare and forwards what only the leader can
//! serve; replies name the leader's API and replica-local staleness.

mod fixtures;

use fixtures::{hc, read_dispatch, s};

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use flower_cluster::network::{InProcessFactory, Registry};
use flower_cluster::node::Node;
use flower_cluster::statemachine::RuntimeFactory;
use flower_engine::runtime::{CallKind, ClosureRuntime, HostCall};
use flower_sdk::abi::HostOp;
use flower_sdk::{Value, fail};
use flower_server::api::{AppState, serve};
use flower_server::forward::{LEADER_URL, MAX_STALENESS, STALENESS};
use openraft::ServerState;

/// `put {key, title}`, linearizable `get {key}`, replica-local
/// `local {key}`, and `bounded {key}` within 200 ms.
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
                    (CallKind::Query, "get" | "local" | "bounded") => {
                        hc(host, HostOp::Get, vec![s(""), s("tasks"), key])
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
            {"name": "get", "kind": "query", "args": {"type": "json"}, "consistency": "linearizable"},
            {"name": "local", "kind": "query", "args": {"type": "json"}, "consistency": "replica_local"},
            {"name": "bounded", "kind": "query", "args": {"type": "json"}, "consistency": "replica_local", "max_staleness_ms": 200}
        ],
        "triggers": [],
        "http": [
            {"alias": "put", "definition": "put"},
            {"alias": "get", "definition": "get"},
            {"alias": "local", "definition": "local"},
            {"alias": "bounded", "definition": "bounded"}
        ]
    })
}

/// Three members, each with its own API advertising its URL.
struct Cluster {
    _dir: tempfile::TempDir,
    registry: Registry,
    nodes: Vec<Arc<Node>>,
    bases: Vec<String>,
}

impl Cluster {
    async fn start() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let registry = Registry::new();
        let mut nodes = Vec::new();
        let mut bases = Vec::new();
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
            let node = Arc::new(node);
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let base = format!("http://{}", listener.local_addr().unwrap());
            let state = AppState::new(node.clone())
                .with_admin_token("secret".to_string())
                .with_api_url(base.clone());
            tokio::spawn(serve(listener, state));
            nodes.push(node);
            bases.push(base);
        }
        nodes[0]
            .initialize(BTreeSet::from([1, 2, 3]))
            .await
            .unwrap();
        let cluster = Cluster {
            _dir: dir,
            registry,
            nodes,
            bases,
        };
        cluster.leader().await;
        // Every member caught up after its empty start: none is still
        // rejoining, which refuses to take leadership.
        for node in &cluster.nodes {
            for _ in 0..1000 {
                if !node.rejoining() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            assert!(!node.rejoining(), "node {} never caught up", node.id());
        }
        cluster
    }

    /// The leader's index, once every member agrees.
    async fn leader(&self) -> usize {
        for _ in 0..500 {
            let leaders: BTreeSet<_> = self.nodes.iter().map(|node| node.status().leader).collect();
            if let [Some(leader)] = leaders.into_iter().collect::<Vec<_>>()[..] {
                let index = (leader - 1) as usize;
                let metrics = self.nodes[index].raft().metrics().borrow().clone();
                if metrics.state == ServerState::Leader {
                    return index;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("no agreed leader");
    }

    async fn follower(&self) -> usize {
        (self.leader().await + 1) % self.nodes.len()
    }
}

fn key(name: &str) -> String {
    format!("\"{name}\"")
}

async fn post(
    http: &reqwest::Client,
    url: String,
    body: serde_json::Value,
    headers: &[(&str, &str)],
) -> reqwest::Response {
    let mut request = http.post(url).json(&body);
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    request.send().await.unwrap()
}

async fn deploy(http: &reqwest::Client, base: &str, token: Option<&str>) -> reqwest::Response {
    let authorization = token.map(|token| format!("Bearer {token}"));
    let headers: Vec<(&str, &str)> = authorization
        .iter()
        .map(|value| ("authorization", value.as_str()))
        .collect();
    post(
        http,
        format!("{base}/v1/deploy"),
        serde_json::json!({
            "db": "db",
            "module": "app",
            "identity": null,
            "wasm_b64": "e30=",
            "manifest": manifest(),
        }),
        &headers,
    )
    .await
}

async fn put(http: &reqwest::Client, base: &str, name: &str, title: &str) -> serde_json::Value {
    let response = post(
        http,
        format!("{base}/v1/mutate"),
        serde_json::json!({
            "db": "db", "module": "app", "name": "put",
            "args": {"key": key(name), "title": title},
            "idempotency_key": format!("{name}-{title}"),
        }),
        &[],
    )
    .await;
    assert_eq!(response.status(), 200);
    response.json().await.unwrap()
}

async fn query(
    http: &reqwest::Client,
    base: &str,
    name: &str,
    row: &str,
    headers: &[(&str, &str)],
) -> reqwest::Response {
    post(
        http,
        format!("{base}/v1/query"),
        serde_json::json!({"db": "db", "module": "app", "name": name, "args": {"key": key(row)}}),
        headers,
    )
    .await
}

#[tokio::test(flavor = "multi_thread")]
async fn followers_forward_writes_and_answer_fresh_reads() {
    let cluster = Cluster::start().await;
    let http = reqwest::Client::new();
    let follower = &cluster.bases[cluster.follower().await];
    // Deployments and mutations sent to a follower commit through the
    // leader, which answers as it would have directly.
    let response = deploy(&http, follower, Some("secret")).await;
    assert_eq!(response.status(), 200);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["ok"], true);
    let body = put(&http, follower, "a", "first").await;
    assert_eq!(body["ok"], true);
    assert_eq!(body["value"], "put-ok");
    assert!(body["position"].as_u64().unwrap() > 0);
    // A retry with the same idempotency key replays its outcome without
    // replicating, never applying twice, and answers where the
    // original committed.
    let again = put(&http, follower, "a", "first").await;
    assert_eq!(again["value"], "put-ok");
    assert_eq!(again["position"], body["position"]);
    assert_eq!(again["revision"], body["revision"]);
    // Every member answers the linearizable read with the write.
    for base in &cluster.bases {
        let response = query(&http, base, "get", "a", &[]).await;
        assert_eq!(response.status(), 200);
        assert!(response.headers().get(STALENESS).is_none());
        let body: serde_json::Value = response.json().await.unwrap();
        assert_eq!(body["value"]["title"], "first", "{base}");
    }
}

/// A replay after later writes answers what the original call was
/// answered: its value, its revision and its log position, through the
/// leader and through a follower, saying it is a duplicate, and every
/// member holds the same receipt. Before receipts recorded where they committed, a replay
/// answered with the database's current revision and position 0 (v1
/// returned the original revision).
#[tokio::test(flavor = "multi_thread")]
async fn replays_after_later_writes_answer_the_original_revision_and_position() {
    let cluster = Cluster::start().await;
    let http = reqwest::Client::new();
    let leader = cluster.leader().await;
    let follower = cluster.follower().await;
    let response = deploy(&http, &cluster.bases[leader], Some("secret")).await;
    assert_eq!(response.status(), 200);
    let original = put(&http, &cluster.bases[leader], "a", "first").await;
    assert_eq!(original["ok"], true);
    assert_eq!(original["duplicate"], false);
    let (revision, position) = (&original["revision"], &original["position"]);
    assert!(position.as_u64().unwrap() > 0);
    // Later writes move the revision and the log on.
    for name in ["b", "c", "d"] {
        put(&http, &cluster.bases[follower], name, "later").await;
    }
    let later = put(&http, &cluster.bases[leader], "e", "later").await;
    assert_eq!(later["duplicate"], false);
    assert!(later["revision"].as_u64() > revision.as_u64());
    assert!(later["position"].as_u64() > position.as_u64());
    for base in [&cluster.bases[leader], &cluster.bases[follower]] {
        let again = put(&http, base, "a", "first").await;
        assert_eq!(again["value"], "put-ok", "{base}");
        assert_eq!(&again["revision"], revision, "{base}");
        assert_eq!(&again["position"], position, "{base}");
        // Answered from the receipt, as v1 said with `duplicate: true`.
        assert_eq!(again["duplicate"], true, "{base}");
    }
    // Every member applied the same receipt, stamped the same way.
    let identity = flower_engine::retry::unscoped_identity_key(b"a-first");
    let mut stored = Vec::new();
    for _ in 0..500 {
        stored.clear();
        for node in &cluster.nodes {
            let identity = identity.clone();
            let receipt = node
                .with_engine(move |engine| {
                    let read = engine.store().begin_read().unwrap();
                    Ok(read.get_receipt(b"db", &identity).unwrap())
                })
                .await
                .unwrap();
            stored.push(receipt);
        }
        if stored.iter().all(Option::is_some) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let first = stored[0].clone().expect("every member holds the receipt");
    assert!(
        stored
            .iter()
            .all(|receipt| receipt.as_ref() == Some(&first))
    );
    let committed = flower_engine::retry::Receipt::decode(&first)
        .unwrap()
        .committed
        .expect("the receipt records where it committed");
    assert_eq!(serde_json::json!(committed.revision), *revision);
    assert_eq!(serde_json::json!(committed.position), *position);
}

#[tokio::test(flavor = "multi_thread")]
async fn forwarded_requests_keep_the_callers_credential() {
    let cluster = Cluster::start().await;
    let http = reqwest::Client::new();
    let follower = &cluster.bases[cluster.follower().await];
    // Refused at the follower, before anything is forwarded.
    assert_eq!(deploy(&http, follower, None).await.status(), 403);
    assert_eq!(deploy(&http, follower, Some("wrong")).await.status(), 403);
    // A forwarded request cannot be forwarded again: a member that does
    // not lead answers a forward to it.
    deploy(&http, follower, Some("secret")).await;
    let response = post(
        &http,
        format!("{follower}/v1/mutate"),
        serde_json::json!({
            "db": "db", "module": "app", "name": "put",
            "args": {"key": key("a"), "title": "first"},
        }),
        &[("x-flower-forwarded-by", "9")],
    )
    .await;
    assert_eq!(response.status(), 503);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["code"], "forward");
}

#[tokio::test(flavor = "multi_thread")]
async fn replica_local_queries_report_staleness_within_the_callers_bound() {
    let cluster = Cluster::start().await;
    let http = reqwest::Client::new();
    let leader = cluster.leader().await;
    let follower = &cluster.bases[cluster.follower().await];
    deploy(&http, &cluster.bases[leader], Some("secret")).await;
    put(&http, &cluster.bases[leader], "a", "first").await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let response = query(&http, follower, "local", "a", &[]).await;
    assert_eq!(response.status(), 200);
    let staleness: u64 = response.headers()[STALENESS]
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    assert!(staleness < 1000, "{staleness}");
    // A zero bound proves a fresh read: zero staleness.
    let response = query(&http, follower, "local", "a", &[(MAX_STALENESS, "0")]).await;
    assert_eq!(response.status(), 200);
    assert_eq!(response.headers()[STALENESS], "0");
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["value"]["title"], "first");
    for bad in ["-1", "1.5", "", "2147483648", "5ms"] {
        let response = query(&http, follower, "local", "a", &[(MAX_STALENESS, bad)]).await;
        assert_eq!(response.status(), 400, "{bad:?}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn replies_name_the_leaders_api() {
    let cluster = Cluster::start().await;
    let http = reqwest::Client::new();
    let leader = cluster.leader().await;
    deploy(&http, &cluster.bases[leader], Some("secret")).await;
    for base in &cluster.bases {
        let mut named = None;
        for _ in 0..200 {
            let response = query(&http, base, "local", "a", &[]).await;
            named = response
                .headers()
                .get(LEADER_URL)
                .map(|value| value.to_str().unwrap().to_string());
            if named.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            named.as_deref(),
            Some(cluster.bases[leader].as_str()),
            "{base}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn watches_on_followers_stream_fresh_snapshots() {
    let cluster = Cluster::start().await;
    let http = reqwest::Client::new();
    let leader = &cluster.bases[cluster.leader().await];
    let follower = &cluster.bases[cluster.follower().await];
    deploy(&http, leader, Some("secret")).await;
    put(&http, leader, "a", "first").await;
    let args: serde_json::Value = serde_json::from_slice(br#"{"key": "\"a\""}"#).unwrap();
    let mut response = http
        .post(format!("{follower}/v1/watch"))
        .json(&serde_json::json!({"db": "db", "module": "app", "name": "get", "args": args}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let first = read_dispatch(&mut response).await;
    assert!(first.contains("event: snapshot"), "{first}");
    assert!(first.contains("first"), "{first}");
    put(&http, leader, "a", "second").await;
    let second = read_dispatch(&mut response).await;
    assert!(second.contains("event: snapshot"), "{second}");
    assert!(second.contains("second"), "{second}");
}

#[tokio::test(flavor = "multi_thread")]
async fn bounded_watches_end_on_a_follower_cut_off_past_their_bound() {
    let cluster = Cluster::start().await;
    let http = reqwest::Client::new();
    let leader = cluster.leader().await;
    let follower = cluster.follower().await;
    deploy(&http, &cluster.bases[leader], Some("secret")).await;
    put(&http, &cluster.bases[leader], "a", "first").await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let args: serde_json::Value = serde_json::from_slice(br#"{"key": "\"a\""}"#).unwrap();
    let watch = |name: &str| {
        http.post(format!("{}/v1/watch", cluster.bases[follower]))
            .json(&serde_json::json!({"db": "db", "module": "app", "name": name, "args": args}))
            .send()
    };
    let mut bounded = watch("bounded").await.unwrap();
    let mut unbounded = watch("local").await.unwrap();
    assert!(read_dispatch(&mut bounded).await.contains("first"));
    assert!(read_dispatch(&mut unbounded).await.contains("first"));
    // Cut off, the follower hears no commits; its staleness passes the
    // bounded query's 200 ms and that watch ends.
    let cut = cluster.nodes[follower].id();
    for node in &cluster.nodes {
        if node.id() != cut {
            cluster.registry.block(cut, node.id());
            cluster.registry.block(node.id(), cut);
        }
    }
    let ended = read_dispatch(&mut bounded).await;
    assert!(ended.contains("event: error"), "{ended}");
    // The unbounded watch keeps its stream: only keepalives arrive.
    let still = tokio::time::timeout(Duration::from_millis(1500), unbounded.chunk()).await;
    assert!(
        still.is_err()
            || still
                .unwrap()
                .unwrap()
                .is_some_and(|chunk| !chunk.starts_with(b"event: error")),
        "an unbounded watch ended"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_follower_lagging_a_deployment_forwards_writes_for_it() {
    let cluster = Cluster::start().await;
    let http = reqwest::Client::new();
    let leader = cluster.leader().await;
    let follower = cluster.follower().await;
    let (leader_id, follower_id) = (cluster.nodes[leader].id(), cluster.nodes[follower].id());
    // The follower hears nothing from the leader: the deployment
    // commits without it, so its own state lacks the module.
    cluster.registry.block(leader_id, follower_id);
    let response = deploy(&http, &cluster.bases[leader], Some("secret")).await;
    assert_eq!(response.status(), 200);
    // Its writes still run where the module is: at the leader.
    let body = put(&http, &cluster.bases[follower], "a", "first").await;
    assert_eq!(body["ok"], true, "{body}");
    assert_eq!(body["value"], "put-ok");
    cluster.registry.heal();
}

#[tokio::test(flavor = "multi_thread")]
async fn admin_membership_and_leadership_requests_forward_to_the_leader() {
    let cluster = Cluster::start().await;
    let http = reqwest::Client::new();
    let leader = cluster.leader().await;
    let follower = cluster.follower().await;
    let target = cluster.nodes[follower].id();
    // A follower's own view answers locally.
    let view = http
        .get(format!("{}/v1/admin/membership", cluster.bases[follower]))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap();
    assert_eq!(view.status(), 200);
    // A member opened without Raft state takes no leadership until it
    // has caught up with the group (a transfer to it is refused): wait.
    for _ in 0..400 {
        if !cluster.nodes[follower].readiness().await.rejoining {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(!cluster.nodes[follower].readiness().await.rejoining);
    // A transfer asked of a follower runs at the leader.
    let response = post(
        &http,
        format!("{}/v1/admin/leadership", cluster.bases[follower]),
        serde_json::json!({"to": target}),
        &[("authorization", "Bearer secret")],
    )
    .await;
    let status = response.status();
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["transfer"]["from"], cluster.nodes[leader].id());
    assert_eq!(body["transfer"]["to"], target);
    assert_eq!(cluster.leader().await, follower);
}
