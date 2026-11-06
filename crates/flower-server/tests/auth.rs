//! Authentication over HTTP: the alias allowlist, credentials reaching a
//! module's authorization hook, denials before replay, and watch
//! subscribers reauthorized before every delivery.
//!
//! The test module keeps sessions in a `sessions` collection: its hook
//! maps a bearer token to the session's subject and role, refusing
//! unknown, revoked and expired tokens. `grant` and `revoke` are public.

mod fixtures;

use fixtures::{hc, k, obj, read_dispatch, read_until_end, s};

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use flower_cluster::network::{InProcessFactory, Registry};
use flower_cluster::node::Node;
use flower_cluster::statemachine::RuntimeFactory;
use flower_engine::runtime::{CallKind, ClosureRuntime, HostCall};
use flower_sdk::abi::HostOp;
use flower_sdk::{Failure, Value, fail};
use flower_server::api::{AppState, serve};
use flower_server::forward::STALENESS;
use flower_server::watch::Hub;
use openraft::ServerState;

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn get(host: &mut dyn HostCall, collection: &str, key: &str) -> Result<Value, Failure> {
    hc(host, HostOp::Get, vec![s(""), s(collection), s(key)])
}

fn field<'a>(value: &'a Value, name: &str) -> &'a Value {
    value
        .as_object()
        .and_then(|map| map.get(name))
        .unwrap_or(&Value::Null)
}

/// Every credential value the hook was shown.
type Seen = Arc<Mutex<Vec<Value>>>;

type Handler = Arc<
    dyn Fn(CallKind, &str, &str, &Value, &mut dyn HostCall) -> Result<Value, Failure> + Send + Sync,
>;

fn handler(seen: Seen) -> Handler {
    Arc::new(
        move |kind: CallKind, _module: &str, name: &str, args: &Value, host: &mut dyn HostCall| {
            match (kind, name) {
                (CallKind::Query, "$flower.authorize") => {
                    seen.lock()
                        .unwrap()
                        .push(field(args, "credentials").clone());
                    let method = field(args, "method").as_str().unwrap_or_default();
                    if method == "grant" || method == "revoke" {
                        return Ok(obj(vec![("principal", Value::Null)]));
                    }
                    let Some(token) = field(field(args, "credentials"), "bearer").as_str() else {
                        return Err(fail("UNAUTHENTICATED", "no bearer token"));
                    };
                    let session = get(host, "sessions", &k(token))?;
                    if session.is_null() {
                        return Err(fail("UNAUTHENTICATED", "unknown session"));
                    }
                    let now = hc(host, HostOp::Now, vec![])?.as_f64().unwrap_or(0.0);
                    if field(&session, "expires_ms").as_f64().unwrap_or(0.0) <= now {
                        return Err(fail("UNAUTHENTICATED", "session expired"));
                    }
                    Ok(obj(vec![(
                        "principal",
                        obj(vec![
                            ("subject", field(&session, "subject").clone()),
                            (
                                "claims",
                                obj(vec![("role", field(&session, "role").clone())]),
                            ),
                        ]),
                    )]))
                }
                (CallKind::Query, "whoami" | "whoami_local") => {
                    let principal = hc(host, HostOp::Principal, vec![])?;
                    Ok(obj(vec![
                        ("subject", field(&principal, "subject").clone()),
                        ("role", field(field(&principal, "claims"), "role").clone()),
                        ("note", get(host, "notes", &k("n"))?),
                    ]))
                }
                (CallKind::Mutation, "note") => {
                    hc(
                        host,
                        HostOp::Set,
                        vec![s(""), s("notes"), s(&k("n")), field(args, "text").clone()],
                    )?;
                    Ok(s("noted"))
                }
                (CallKind::Mutation, "grant") => {
                    let token = field(args, "token")
                        .as_str()
                        .unwrap_or_default()
                        .to_string();
                    let session = obj(vec![
                        ("subject", field(args, "subject").clone()),
                        ("role", field(args, "role").clone()),
                        ("expires_ms", field(args, "expires_ms").clone()),
                    ]);
                    hc(
                        host,
                        HostOp::Set,
                        vec![s(""), s("sessions"), s(&k(&token)), session],
                    )?;
                    Ok(Value::Null)
                }
                (CallKind::Mutation, "revoke") => {
                    let token = field(args, "token")
                        .as_str()
                        .unwrap_or_default()
                        .to_string();
                    hc(
                        host,
                        HostOp::Delete,
                        vec![s(""), s("sessions"), s(&k(&token))],
                    )?;
                    Ok(Value::Null)
                }
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
    let json = serde_json::json!({"type": "json"});
    let string = serde_json::json!({"type": "string"});
    serde_json::json!({
        "collections": [
            {"name": "sessions", "key": string, "value": json, "indexes": [], "references": []},
            {"name": "notes", "key": string, "value": json, "indexes": [], "references": []}
        ],
        "definitions": [
            {"name": "whoami", "kind": "query", "args": json},
            {"name": "whoami_local", "kind": "query", "args": json, "consistency": "replica_local"},
            {"name": "note", "kind": "mutation", "args": json},
            {"name": "grant", "kind": "mutation", "args": json},
            {"name": "revoke", "kind": "mutation", "args": json}
        ],
        "triggers": [],
        "http": [
            {"alias": "me", "definition": "whoami"},
            {"alias": "near", "definition": "whoami_local"},
            {"alias": "note", "definition": "note"},
            {"alias": "grant", "definition": "grant"},
            {"alias": "revoke", "definition": "revoke"}
        ],
        "authorize": true
    })
}

struct Served {
    base: String,
    hub: Arc<Hub>,
    seen: Seen,
    http: reqwest::Client,
}

/// Serve an initialized single node with `secret` as the admin token,
/// the test module deployed, and the hub reauthorizing every
/// `reauthorize`.
async fn serve_node(reauthorize: Duration) -> (tempfile::TempDir, Served) {
    let dir = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let seen: Seen = Arc::new(Mutex::new(Vec::new()));
    let node = Node::open(
        1,
        &dir.path().join("n1"),
        Arc::new(openraft::Config::default()),
        InProcessFactory::new(registry.clone()),
        Arc::new(TestFactory {
            handler: handler(seen.clone()),
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
    let hub = Arc::new(Hub::new(node.clone()).with_reauthorize_interval(reauthorize));
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
    (
        dir,
        Served {
            base,
            hub,
            seen,
            http,
        },
    )
}

impl Served {
    /// Call an alias, presenting `bearer` if given.
    async fn call(
        &self,
        route: &str,
        name: &str,
        args: serde_json::Value,
        bearer: Option<&str>,
        extra: serde_json::Value,
    ) -> reqwest::Response {
        let mut body = serde_json::json!({"db": "db", "module": "app", "name": name, "args": args});
        if let serde_json::Value::Object(extra) = extra {
            body.as_object_mut().unwrap().extend(extra);
        }
        let mut request = self
            .http
            .post(format!("{}/v1/{route}", self.base))
            .json(&body);
        if let Some(bearer) = bearer {
            request = request.header("authorization", format!("Bearer {bearer}"));
        }
        request.send().await.unwrap()
    }

    async fn grant(&self, token: &str, subject: &str, role: &str, expires_ms: u64) {
        let response = self
            .call(
                "mutate",
                "grant",
                serde_json::json!({"token": token, "subject": subject, "role": role, "expires_ms": expires_ms}),
                None,
                serde_json::json!({}),
            )
            .await;
        assert_eq!(response.status(), 200);
        let body: serde_json::Value = response.json().await.unwrap();
        assert_eq!(body["ok"], true, "{body}");
    }

    async fn revoke(&self, token: &str) {
        let response = self
            .call(
                "mutate",
                "revoke",
                serde_json::json!({"token": token}),
                None,
                serde_json::json!({}),
            )
            .await;
        assert_eq!(response.status(), 200);
    }

    async fn note(&self, text: &str, bearer: &str) {
        let response = self
            .call(
                "mutate",
                "note",
                serde_json::json!({"text": text}),
                Some(bearer),
                serde_json::json!({}),
            )
            .await;
        assert_eq!(response.status(), 200);
    }

    async fn watch(&self, bearer: Option<&str>) -> reqwest::Response {
        let args: serde_json::Value = serde_json::from_slice(b"{}").unwrap();
        let mut request = self
            .http
            .post(format!("{}/v1/watch", self.base))
            .json(&serde_json::json!({"db": "db", "module": "app", "name": "me", "args": args}));
        if let Some(bearer) = bearer {
            request = request.header("authorization", format!("Bearer {bearer}"));
        }
        request.send().await.unwrap()
    }
}

const HOUR_MS: u64 = 3_600_000;

/// Read dispatches until one contains `needle`.
async fn read_until(response: &mut reqwest::Response, needle: &str) -> String {
    for _ in 0..20 {
        let dispatch = read_dispatch(response).await;
        if dispatch.contains(needle) {
            return dispatch;
        }
    }
    panic!("no dispatch carried {needle}");
}

#[tokio::test]
async fn aliases_are_the_only_http_surface() {
    let (_dir, served) = serve_node(Duration::from_secs(3600)).await;
    served
        .grant("t1", "ada", "reader", now_ms() + HOUR_MS)
        .await;
    // The internal definition name is not callable over HTTP, credentials
    // or not; the alias is.
    let internal = served
        .call(
            "query",
            "whoami",
            serde_json::json!({}),
            Some("t1"),
            serde_json::json!({}),
        )
        .await;
    assert_eq!(internal.status(), 404);
    let body: serde_json::Value = internal.json().await.unwrap();
    assert_eq!(body["code"], "UNKNOWN_METHOD");
    let alias = served
        .call(
            "query",
            "me",
            serde_json::json!({}),
            Some("t1"),
            serde_json::json!({}),
        )
        .await;
    assert_eq!(alias.status(), 200);
    let body: serde_json::Value = alias.json().await.unwrap();
    assert_eq!(body["value"]["subject"], "ada");
    // Watches too.
    let args: serde_json::Value = serde_json::from_slice(b"{}").unwrap();
    let watch = served
        .http
        .post(format!("{}/v1/watch", served.base))
        .json(&serde_json::json!({"db": "db", "module": "app", "name": "whoami", "args": args}))
        .header("authorization", "Bearer t1")
        .send()
        .await
        .unwrap();
    assert_eq!(watch.status(), 404);
}

#[tokio::test]
async fn the_hook_decides_from_credentials_and_denials_are_403() {
    let (_dir, served) = serve_node(Duration::from_secs(3600)).await;
    served
        .grant("t1", "ada", "reader", now_ms() + HOUR_MS)
        .await;
    for bearer in [None, Some("nope")] {
        let denied = served
            .call(
                "query",
                "me",
                serde_json::json!({}),
                bearer,
                serde_json::json!({}),
            )
            .await;
        assert_eq!(denied.status(), 403);
        let body: serde_json::Value = denied.json().await.unwrap();
        assert_eq!(body["code"], "unauthorized");
        assert!(
            body["message"]
                .as_str()
                .unwrap()
                .contains("UNAUTHENTICATED"),
            "{body}"
        );
    }
    // A body's credentials count as much as a bearer's.
    let explicit = served
        .call(
            "query",
            "me",
            serde_json::json!({}),
            None,
            serde_json::json!({"credentials": {"bearer": "t1"}}),
        )
        .await;
    assert_eq!(explicit.status(), 200);
    // The admin token makes a gateway, never a credential: the hook is
    // not shown it, and a gateway's asserted principal is the hook's to
    // judge (this one ignores it).
    let gateway = served
        .call(
            "query",
            "me",
            serde_json::json!({}),
            Some("secret"),
            serde_json::json!({"principal": {"subject": "mallory"}}),
        )
        .await;
    assert_eq!(gateway.status(), 403);
    let seen = served.seen.lock().unwrap().clone();
    assert!(!seen.is_empty());
    assert!(
        seen.iter()
            .all(|credentials| !format!("{credentials:?}").contains("secret")),
        "{seen:?}"
    );
    // Watches authorize before their first snapshot.
    assert_eq!(served.watch(None).await.status(), 403);
}

#[tokio::test]
async fn a_revoked_caller_never_replays_its_idempotent_result() {
    let (_dir, served) = serve_node(Duration::from_secs(3600)).await;
    served
        .grant("t1", "ada", "writer", now_ms() + HOUR_MS)
        .await;
    let key = serde_json::json!({"idempotency_key": "k1"});
    let first = served
        .call(
            "mutate",
            "note",
            serde_json::json!({"text": "one"}),
            Some("t1"),
            key.clone(),
        )
        .await;
    assert_eq!(first.status(), 200);
    let body: serde_json::Value = first.json().await.unwrap();
    assert_eq!(body["value"], "noted");
    // The same retry still replays while the session holds...
    let replayed = served
        .call(
            "mutate",
            "note",
            serde_json::json!({"text": "one"}),
            Some("t1"),
            key.clone(),
        )
        .await;
    assert_eq!(replayed.status(), 200);
    // ...and is refused once it is revoked, before the receipt answers.
    served.revoke("t1").await;
    let refused = served
        .call(
            "mutate",
            "note",
            serde_json::json!({"text": "one"}),
            Some("t1"),
            key,
        )
        .await;
    assert_eq!(refused.status(), 403);
    let body: serde_json::Value = refused.json().await.unwrap();
    assert_eq!(body["code"], "unauthorized");
}

#[tokio::test]
async fn a_watch_ends_when_its_credentials_are_revoked() {
    let (_dir, served) = serve_node(Duration::from_secs(3600)).await;
    served
        .grant("t1", "ada", "reader", now_ms() + HOUR_MS)
        .await;
    let mut response = served.watch(Some("t1")).await;
    assert_eq!(response.status(), 200);
    assert!(read_dispatch(&mut response).await.contains("\"ada\""));
    served.revoke("t1").await;
    let text = read_until_end(&mut response, "event: error").await;
    assert!(text.contains("unauthorized"), "{text}");
}

#[tokio::test]
async fn a_watch_ends_when_its_principal_changes() {
    let (_dir, served) = serve_node(Duration::from_secs(3600)).await;
    served
        .grant("t1", "ada", "reader", now_ms() + HOUR_MS)
        .await;
    let mut response = served.watch(Some("t1")).await;
    assert!(read_dispatch(&mut response).await.contains("\"reader\""));
    // Same token, new claims: the shared result was computed for the old
    // principal, so the subscriber reconnects instead of receiving it.
    served.grant("t1", "ada", "admin", now_ms() + HOUR_MS).await;
    let text = read_until_end(&mut response, "event: error").await;
    assert!(text.contains("watch_scope_changed"), "{text}");
    assert!(!text.contains("\"admin\""), "{text}");
}

#[tokio::test]
async fn a_watch_is_reauthorized_without_commits() {
    let (_dir, served) = serve_node(Duration::from_millis(200)).await;
    // Nothing commits after the subscription: only the reauthorization
    // tick can notice the expiry.
    served.grant("t1", "ada", "reader", now_ms() + 1_500).await;
    let mut response = served.watch(Some("t1")).await;
    assert_eq!(response.status(), 200);
    assert!(read_dispatch(&mut response).await.contains("\"ada\""));
    let text = read_until_end(&mut response, "event: error").await;
    assert!(text.contains("session expired"), "{text}");
}

#[tokio::test]
async fn subscribers_share_an_evaluation_but_not_access() {
    let (_dir, served) = serve_node(Duration::from_secs(3600)).await;
    // Two sessions, one principal: one hub entry.
    served
        .grant("t1", "ada", "reader", now_ms() + HOUR_MS)
        .await;
    served
        .grant("t2", "ada", "reader", now_ms() + HOUR_MS)
        .await;
    let mut first = served.watch(Some("t1")).await;
    let mut second = served.watch(Some("t2")).await;
    assert!(read_dispatch(&mut first).await.contains("snapshot"));
    assert!(read_dispatch(&mut second).await.contains("snapshot"));
    assert_eq!(served.hub.keys().await, 1);
    served.revoke("t2").await;
    let text = read_until_end(&mut second, "event: error").await;
    assert!(text.contains("unauthorized"), "{text}");
    // The other subscriber keeps its stream and its updates.
    served.note("hello", "t1").await;
    read_until(&mut first, "hello").await;
}

/// Three members serving the test module, each with its own API.
struct Cluster {
    _dir: tempfile::TempDir,
    registry: Registry,
    nodes: Vec<Arc<Node>>,
    bases: Vec<String>,
    http: reqwest::Client,
}

impl Cluster {
    /// Start with hubs reauthorizing every `reauthorize`.
    async fn start(reauthorize: Duration) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let registry = Registry::new();
        let seen: Seen = Arc::new(Mutex::new(Vec::new()));
        let mut nodes = Vec::new();
        let mut bases = Vec::new();
        for id in 1..=3u64 {
            let node = Node::open(
                id,
                &dir.path().join(format!("n{id}")),
                Arc::new(openraft::Config::default()),
                InProcessFactory::new(registry.clone()).with_source(id),
                Arc::new(TestFactory {
                    handler: handler(seen.clone()),
                }),
            )
            .await
            .unwrap();
            registry.register_peer(node.peer_service());
            let node = Arc::new(node);
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let base = format!("http://{}", listener.local_addr().unwrap());
            let mut state = AppState::new(node.clone())
                .with_admin_token("secret".to_string())
                .with_api_url(base.clone());
            state.hub = Arc::new(Hub::new(node.clone()).with_reauthorize_interval(reauthorize));
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
            http: reqwest::Client::new(),
        };
        let leader = cluster.leader().await;
        let deployed = cluster
            .http
            .post(format!("{}/v1/deploy", cluster.bases[leader]))
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

    /// One member's API, for the shared call helpers.
    fn member(&self, index: usize) -> Served {
        Served {
            base: self.bases[index].clone(),
            hub: Arc::new(Hub::new(self.nodes[index].clone())),
            seen: Arc::new(Mutex::new(Vec::new())),
            http: self.http.clone(),
        }
    }
}

#[tokio::test]
async fn followers_forward_writes_for_the_leader_to_decide_and_decide_reads_themselves() {
    let cluster = Cluster::start(Duration::from_secs(3600)).await;
    let leader = cluster.member(cluster.leader().await);
    let follower = cluster.member(cluster.follower().await);
    // A session granted at the leader works through the follower at
    // once: a forwarded write's access is the leader's to decide, on its
    // own state, whatever the follower has applied.
    leader
        .grant("t1", "ada", "writer", now_ms() + HOUR_MS)
        .await;
    let written = follower
        .call(
            "mutate",
            "note",
            serde_json::json!({"text": "via follower"}),
            Some("t1"),
            serde_json::json!({}),
        )
        .await;
    assert_eq!(written.status(), 200);
    let body: serde_json::Value = written.json().await.unwrap();
    assert_eq!(body["value"], "noted", "{body}");
    // Refused there as well, with the leader's denial relayed.
    let refused = follower
        .call(
            "mutate",
            "note",
            serde_json::json!({"text": "anonymous"}),
            None,
            serde_json::json!({}),
        )
        .await;
    assert_eq!(refused.status(), 403);
    let body: serde_json::Value = refused.json().await.unwrap();
    assert_eq!(body["code"], "unauthorized");
    // Body credentials travel too: the forwarded body is byte-faithful.
    let explicit = follower
        .call(
            "mutate",
            "note",
            serde_json::json!({"text": "explicit"}),
            None,
            serde_json::json!({"credentials": {"bearer": "t1"}}),
        )
        .await;
    assert_eq!(explicit.status(), 200);
    // Fresh reads are the follower's own, behind its read fence, and
    // decide access there.
    let read = follower
        .call(
            "query",
            "me",
            serde_json::json!({}),
            Some("t1"),
            serde_json::json!({}),
        )
        .await;
    assert_eq!(read.status(), 200);
    let body: serde_json::Value = read.json().await.unwrap();
    assert_eq!(body["value"]["subject"], "ada");
    assert_eq!(body["value"]["note"], "explicit");
    let denied = follower
        .call(
            "query",
            "me",
            serde_json::json!({}),
            None,
            serde_json::json!({}),
        )
        .await;
    assert_eq!(denied.status(), 403);
    // Operator routes are routes, not aliases: no 404 for them.
    let membership = cluster
        .http
        .get(format!("{}/v1/admin/membership", follower.base))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap();
    assert_eq!(membership.status(), 200);
}

#[tokio::test]
async fn replica_local_aliases_read_the_way_their_definition_declares() {
    let cluster = Cluster::start(Duration::from_secs(3600)).await;
    let leader = cluster.member(cluster.leader().await);
    let follower = cluster.member(cluster.follower().await);
    leader
        .grant("t1", "ada", "reader", now_ms() + HOUR_MS)
        .await;
    // A fresh read through the follower makes it apply the grant before
    // the local read below.
    let fresh = follower
        .call(
            "query",
            "me",
            serde_json::json!({}),
            Some("t1"),
            serde_json::json!({}),
        )
        .await;
    assert_eq!(fresh.status(), 200);
    // Proved fresh, it reports no staleness.
    assert!(!fresh.headers().contains_key(STALENESS));
    // `near` aliases a replica-local query: the follower answers from
    // its own state and says how stale that may be.
    let near = follower
        .call(
            "query",
            "near",
            serde_json::json!({}),
            Some("t1"),
            serde_json::json!({}),
        )
        .await;
    assert_eq!(near.status(), 200);
    assert!(near.headers().contains_key(STALENESS));
    let body: serde_json::Value = near.json().await.unwrap();
    assert_eq!(body["value"]["subject"], "ada");
}

#[tokio::test]
async fn a_watch_on_a_follower_ends_when_its_credentials_are_revoked() {
    let cluster = Cluster::start(Duration::from_secs(3600)).await;
    let leader = cluster.member(cluster.leader().await);
    let follower = cluster.member(cluster.follower().await);
    leader
        .grant("t1", "ada", "reader", now_ms() + HOUR_MS)
        .await;
    let mut response = follower.watch(Some("t1")).await;
    assert_eq!(response.status(), 200);
    assert!(read_dispatch(&mut response).await.contains("\"ada\""));
    // Updates reach it through the follower's fresh refreshes.
    leader.note("hello", "t1").await;
    read_until(&mut response, "hello").await;
    leader.revoke("t1").await;
    let text = read_until_end(&mut response, "event: error").await;
    assert!(text.contains("unauthorized"), "{text}");
}

#[tokio::test(flavor = "multi_thread")]
async fn reauthorization_needs_no_fresh_read_on_a_cut_off_follower() {
    let cluster = Cluster::start(Duration::from_millis(200)).await;
    let leader_index = cluster.leader().await;
    let follower_index = cluster.follower().await;
    let leader = cluster.member(leader_index);
    let follower = cluster.member(follower_index);
    leader
        .grant("t1", "ada", "reader", now_ms() + HOUR_MS)
        .await;
    let fresh = follower
        .call(
            "query",
            "me",
            serde_json::json!({}),
            Some("t1"),
            serde_json::json!({}),
        )
        .await;
    assert_eq!(fresh.status(), 200);
    // A watch of an unbounded replica-local query on the follower.
    let args: serde_json::Value = serde_json::from_slice(b"{}").unwrap();
    let mut response = cluster
        .http
        .post(format!("{}/v1/watch", follower.base))
        .json(&serde_json::json!({"db": "db", "module": "app", "name": "near", "args": args}))
        .header("authorization", "Bearer t1")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert!(read_dispatch(&mut response).await.contains("\"ada\""));
    // Cut off, the follower can prove no fresh read. Reauthorization
    // rounds keep running on its own state, which still holds the
    // session, so the watch stays open across several of them.
    let cut = cluster.nodes[follower_index].id();
    for node in &cluster.nodes {
        if node.id() != cut {
            cluster.registry.block(cut, node.id());
            cluster.registry.block(node.id(), cut);
        }
    }
    let still = tokio::time::timeout(Duration::from_millis(1500), response.chunk()).await;
    assert!(
        still.is_err()
            || still
                .unwrap()
                .unwrap()
                .is_some_and(|chunk| !chunk.starts_with(b"event: error")),
        "reauthorization ended a watch for want of a fresh read"
    );
    cluster.registry.heal();
}
