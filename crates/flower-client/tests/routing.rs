//! Leader-aware routing and replica-local staleness through the client,
//! against a three-member cluster with an API on every member.

mod fixtures;

use fixtures::{hc, s};

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use flower_client::client::{FlowerClient, MutationOptions, RequestOptions, Staleness};
use flower_client::error::FlowerError;
use flower_client::retry::RetryPolicy;
use flower_client::transport::Credentials;
use flower_cluster::network::{InProcessFactory, Registry};
use flower_cluster::node::Node;
use flower_cluster::statemachine::RuntimeFactory;
use flower_engine::runtime::{CallKind, ClosureRuntime, HostCall};
use flower_sdk::abi::HostOp;
use flower_sdk::{Value, fail};
use flower_server::api::{AppState, serve_until};
use openraft::ServerState;

/// `put {key, title}`, linearizable `get {key}`, replica-local
/// `local {key}`.
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
                    (CallKind::Query, "get" | "local") => {
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
            {"name": "get", "kind": "query", "args": {"type": "json"}},
            {"name": "local", "kind": "query", "args": {"type": "json"}, "consistency": "replica_local"}
        ],
        "triggers": [],
        "http": [
            {"alias": "put", "definition": "put"},
            {"alias": "get", "definition": "get"},
            {"alias": "local", "definition": "local"}
        ]
    })
}

/// One member's API and what stops it.
struct Server {
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<std::io::Result<()>>,
}

/// Three members, each serving an API that advertises its URL.
struct Cluster {
    _dir: tempfile::TempDir,
    registry: Registry,
    nodes: Vec<Arc<Node>>,
    bases: Vec<String>,
    servers: Vec<Server>,
}

impl Cluster {
    async fn start() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let registry = Registry::new();
        let (mut nodes, mut bases, mut servers) = (Vec::new(), Vec::new(), Vec::new());
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
            let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
            let shutdown = async move {
                let _ = stopped.await;
            };
            servers.push(Server {
                stop: Some(stop),
                task: tokio::spawn(serve_until(
                    listener,
                    state,
                    shutdown,
                    Duration::from_secs(1),
                )),
            });
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
            servers,
        };
        let leader = cluster.leader().await;
        FlowerClient::builder(&cluster.bases[leader])
            .credentials(Credentials::token("secret"))
            .build()
            .unwrap()
            .deploy(
                "db",
                "app",
                None,
                b"{}",
                &manifest(),
                &RequestOptions::default(),
            )
            .await
            .unwrap();
        cluster
    }

    /// The leader's index, once every live member agrees.
    async fn leader(&self) -> usize {
        for _ in 0..1000 {
            for (index, node) in self.nodes.iter().enumerate() {
                let metrics = node.raft().metrics().borrow().clone();
                if metrics.state == ServerState::Leader
                    && metrics.current_leader == Some(node.id())
                    && self
                        .nodes
                        .iter()
                        .filter(|other| other.status().leader == Some(node.id()))
                        .count()
                        >= 2
                {
                    return index;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("no agreed leader");
    }

    /// Stop one member's API, closing its connections.
    async fn stop(&mut self, index: usize) {
        let server = &mut self.servers[index];
        if let Some(stop) = server.stop.take() {
            let _ = stop.send(());
        }
        let _ = (&mut server.task).await;
    }
}

fn key(name: &str) -> serde_json::Value {
    serde_json::json!({"key": format!("\"{name}\"")})
}

fn row(name: &str, title: &str) -> serde_json::Value {
    serde_json::json!({"key": format!("\"{name}\""), "title": title})
}

/// Ask through `client` until a reply named the leader.
async fn learn_leader(client: &FlowerClient) -> String {
    for _ in 0..500 {
        let _ = client
            .query_value("db", "app", "local", &key("a"), &RequestOptions::default())
            .await;
        if let Some(leader) = client.leader() {
            return leader;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("no reply named the leader");
}

#[tokio::test(flavor = "multi_thread")]
async fn leader_routing_sends_writes_to_the_leader_a_reply_named() {
    let mut cluster = Cluster::start().await;
    let leader = cluster.leader().await;
    let follower = (leader + 1) % 3;
    let routed = FlowerClient::builder(&cluster.bases[follower])
        .leader_routing(true)
        .build()
        .unwrap();
    let plain = FlowerClient::builder(&cluster.bases[follower])
        .build()
        .unwrap();
    // Without routing a write goes through the follower, which forwards.
    let written = plain
        .mutate_value(
            "db",
            "app",
            "put",
            &row("a", "first"),
            &MutationOptions::new(),
        )
        .await
        .unwrap();
    assert!(written.position > 0);
    assert_eq!(plain.leader(), None);
    assert_eq!(learn_leader(&routed).await, cluster.bases[leader]);

    // With the follower's API gone, routed writes still reach the
    // leader; reads keep the main URL, and the plain client has no
    // other way in.
    cluster.stop(follower).await;
    let written = routed
        .mutate_value(
            "db",
            "app",
            "put",
            &row("a", "second"),
            &MutationOptions::new(),
        )
        .await
        .unwrap();
    assert!(written.position > 0);
    assert!(matches!(
        routed
            .query_value("db", "app", "get", &key("a"), &RequestOptions::default())
            .await,
        Err(FlowerError::Transport(_))
    ));
    assert!(matches!(
        plain
            .mutate_value(
                "db",
                "app",
                "put",
                &row("a", "third"),
                &MutationOptions::new()
            )
            .await,
        Err(FlowerError::Transport(_))
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn routed_writes_fall_back_to_the_main_url_when_the_leader_fails() {
    let mut cluster = Cluster::start().await;
    let old = cluster.leader().await;
    let main = (old + 1) % 3;
    let client = FlowerClient::builder(&cluster.bases[main])
        .leader_routing(true)
        .retry(RetryPolicy {
            attempts: 8,
            initial_delay: Duration::from_millis(50),
            max_delay: Duration::from_millis(200),
            attempt_timeout: Duration::from_secs(15),
            ..RetryPolicy::default()
        })
        .build()
        .unwrap();
    assert_eq!(learn_leader(&client).await, cluster.bases[old]);
    // The leader's member and API both fail: the others elect anew.
    let old_id = cluster.nodes[old].id();
    for other in &cluster.nodes {
        if other.id() != old_id {
            cluster.registry.block(old_id, other.id());
            cluster.registry.block(other.id(), old_id);
        }
    }
    cluster.stop(old).await;
    // The routed attempt fails; the next goes through the main URL,
    // whose member forwards to the new leader.
    let written = client
        .mutate_value(
            "db",
            "app",
            "put",
            &row("a", "after"),
            &MutationOptions::new().request_id("after-failover"),
        )
        .await
        .unwrap();
    assert!(written.position > 0);
    let new = cluster.leader().await;
    assert_ne!(new, old);
    assert_eq!(learn_leader(&client).await, cluster.bases[new]);
    let value = client
        .query_value("db", "app", "get", &key("a"), &RequestOptions::default())
        .await
        .unwrap();
    assert_eq!(value.value["title"], "after");
    assert_eq!(value.staleness, None);
}

#[tokio::test(flavor = "multi_thread")]
async fn replica_local_answers_report_their_staleness() {
    let cluster = Cluster::start().await;
    let leader = cluster.leader().await;
    let follower = FlowerClient::builder(&cluster.bases[(leader + 1) % 3])
        .build()
        .unwrap();
    follower
        .mutate_value(
            "db",
            "app",
            "put",
            &row("a", "first"),
            &MutationOptions::new(),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let answer = follower
        .query_value("db", "app", "local", &key("a"), &RequestOptions::default())
        .await
        .unwrap();
    match answer.staleness {
        Some(Staleness::Known(staleness)) => {
            assert!(staleness < Duration::from_secs(1), "{staleness:?}")
        }
        other => panic!("{other:?}"),
    }
    let fenced = follower
        .query_value(
            "db",
            "app",
            "local",
            &key("a"),
            &RequestOptions::new().max_staleness(Duration::ZERO),
        )
        .await
        .unwrap();
    assert_eq!(fenced.staleness, Some(Staleness::Known(Duration::ZERO)));
    assert_eq!(fenced.value["title"], "first");
    let fresh = follower
        .query_value("db", "app", "get", &key("a"), &RequestOptions::default())
        .await
        .unwrap();
    assert_eq!(fresh.staleness, None);
}
