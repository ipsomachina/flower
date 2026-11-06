//! HTTP API integration: deploy, calls, forwards, admin auth,
//! membership, backup, sync, status and metrics over a live node.

mod fixtures;

use fixtures::{hc, k, obj, read_dispatch, read_until_end, s};

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use flower_cluster::network::{InProcessFactory, Registry};
use flower_cluster::node::Node;
use flower_cluster::statemachine::RuntimeFactory;
use flower_engine::runtime::{CallKind, ClosureRuntime, HostCall};
use flower_sdk::abi::HostOp;
use flower_sdk::{Failure, Value, fail};
use flower_server::api::{AppState, serve};

fn opt(value: Value) -> Option<Value> {
    if value == Value::Null {
        None
    } else {
        Some(value)
    }
}

fn get(host: &mut dyn HostCall, collection: &str, key: &str) -> Result<Option<Value>, Failure> {
    Ok(opt(hc(
        host,
        HostOp::Get,
        vec![s(""), s(collection), s(key)],
    )?))
}

fn set(host: &mut dyn HostCall, collection: &str, key: &str, value: Value) -> Result<(), Failure> {
    hc(host, HostOp::Set, vec![s(""), s(collection), s(key), value])?;
    Ok(())
}

type Handler = Arc<
    dyn Fn(CallKind, &str, &str, &Value, &mut dyn HostCall) -> Result<Value, Failure> + Send + Sync,
>;

fn app_handler() -> Handler {
    Arc::new(
        |kind: CallKind, _module: &str, name: &str, args: &Value, host: &mut dyn HostCall| {
            let map = match args {
                Value::Object(map) => map.clone(),
                _ => return Err(fail("TYPE_MISMATCH", "args take an object")),
            };
            let key = match map.get("key") {
                Some(Value::String(key)) => key.clone(),
                _ => return Err(fail("TYPE_MISMATCH", "args need a key")),
            };
            match (kind, name) {
                (CallKind::Mutation, "put") => {
                    let title = map.get("title").cloned().unwrap_or(Value::Null);
                    set(host, "tasks", &key, obj(vec![("title", title)]))?;
                    Ok(s("put-ok"))
                }
                (CallKind::Query, "get") => {
                    let row = get(host, "tasks", &key)?.unwrap_or(Value::Null);
                    // A task titled `refused` fails the read with details.
                    if row == obj(vec![("title", s("refused"))]) {
                        return Err(Failure::with_details(
                            "TASK_REFUSED",
                            "this task is refused",
                            obj(vec![("key", s(&key))]),
                        ));
                    }
                    Ok(row)
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
    serde_json::json!({
        "collections": [{
            "name": "tasks",
            "key": {"type": "string"},
            "value": {"type": "object", "fields": [
                {"name": "title", "schema": {"type": "string"}, "optional": false}
            ]},
            "indexes": [{"name": "by_title", "fields": ["title"]}],
            "references": []
        }],
        "definitions": [
            {"name": "put", "kind": "mutation", "args": {"type": "json"}},
            {"name": "get", "kind": "query", "args": {"type": "json"}}
        ],
        "triggers": [],
        "http": [
            {"alias": "put", "definition": "put"},
            {"alias": "get", "definition": "get"}
        ]
    })
}

struct Served {
    base: String,
    node: Arc<Node>,
}

/// Serve an initialized single node with `secret` as the admin token.
async fn serve_node() -> (tempfile::TempDir, Served) {
    serve_configured(|state| state).await
}

/// Serve an initialized single node with `secret` as the admin token
/// plus whatever else `configure` sets.
async fn serve_configured(
    configure: impl FnOnce(AppState) -> AppState,
) -> (tempfile::TempDir, Served) {
    let dir = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let node = Node::open(
        1,
        &dir.path().join("n1"),
        Arc::new(openraft::Config::default()),
        InProcessFactory::new(registry.clone()),
        Arc::new(TestFactory {
            handler: app_handler(),
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
    assert_eq!(node.status().leader, Some(1));
    let node = Arc::new(node);
    let state = configure(AppState::new(node.clone()).with_admin_token("secret".to_string()));
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
    (dir, Served { base, node })
}

/// Serve a node that never initializes: every call forwards.
async fn serve_uninitialized() -> (tempfile::TempDir, String) {
    let dir = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let node = Node::open(
        1,
        &dir.path().join("n1"),
        Arc::new(openraft::Config::default()),
        InProcessFactory::new(registry.clone()),
        Arc::new(TestFactory {
            handler: app_handler(),
        }),
    )
    .await
    .unwrap();
    registry.register(1, node.raft().clone());
    let state = AppState::new(Arc::new(node)).with_admin_token("secret".to_string());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(serve(listener, state));
    (dir, base)
}

async fn deploy(http: &reqwest::Client, base: &str) -> serde_json::Value {
    deploy_db(http, base, "db").await
}

async fn deploy_db(http: &reqwest::Client, base: &str, db: &str) -> serde_json::Value {
    let response = http
        .post(format!("{base}/v1/deploy"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({
            "db": db,
            "module": "app",
            "identity": null,
            "wasm_b64": "e30=",
            "manifest": manifest(),
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    response.json().await.unwrap()
}

#[tokio::test]
async fn transaction_commits_two_databases() {
    let (_dir, served) = serve_node().await;
    let http = reqwest::Client::new();
    deploy_db(&http, &served.base, "dba").await;
    deploy_db(&http, &served.base, "dbb").await;

    let response = http
        .post(format!("{}/v1/transaction", served.base))
        .json(&serde_json::json!({
            "steps": [
                {"db": "dba", "module": "app", "method": "put",
                 "args": {"key": k("a"), "title": "first"}},
                {"db": "dbb", "module": "app", "method": "put",
                 "args": {"key": k("b"), "title": "second"}},
            ],
            "idempotency_key": "txn-1",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let committed: serde_json::Value = response.json().await.unwrap();
    assert_eq!(committed["ok"], true);
    assert_eq!(committed["value"], serde_json::json!(["put-ok", "put-ok"]));
    assert_eq!(
        committed["revisions"],
        serde_json::json!({"dba": 1, "dbb": 1})
    );
    assert!(committed["position"].as_u64().unwrap() >= 1);

    for (db, key, title) in [("dba", "a", "first"), ("dbb", "b", "second")] {
        let response = http
            .post(format!("{}/v1/query", served.base))
            .json(&serde_json::json!({
                "db": db,
                "module": "app",
                "name": "get",
                "args": {"key": k(key)},
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        let read: serde_json::Value = response.json().await.unwrap();
        assert_eq!(read["value"], serde_json::json!({"title": title}));
    }
}

#[tokio::test]
async fn transaction_aborts_without_committing() {
    let (_dir, served) = serve_node().await;
    let http = reqwest::Client::new();
    deploy_db(&http, &served.base, "dba").await;
    deploy_db(&http, &served.base, "dbb").await;

    // The second step's args are not an object: the handler fails and
    // the transaction aborts before replicating.
    let response = http
        .post(format!("{}/v1/transaction", served.base))
        .json(&serde_json::json!({
            "steps": [
                {"db": "dba", "module": "app", "method": "put",
                 "args": {"key": k("a"), "title": "first"}},
                {"db": "dbb", "module": "app", "method": "put", "args": 7},
            ],
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 422);
    let aborted: serde_json::Value = response.json().await.unwrap();
    assert_eq!(aborted["ok"], false);
    assert_eq!(aborted["code"], "TYPE_MISMATCH");

    let response = http
        .post(format!("{}/v1/query", served.base))
        .json(&serde_json::json!({
            "db": "dba",
            "module": "app",
            "name": "get",
            "args": {"key": k("a")},
        }))
        .send()
        .await
        .unwrap();
    let read: serde_json::Value = response.json().await.unwrap();
    assert_eq!(read["value"], serde_json::Value::Null);
}

#[tokio::test]
async fn deploy_mutate_query_roundtrip() {
    let (_dir, served) = serve_node().await;
    let http = reqwest::Client::new();
    let deployed: serde_json::Value = deploy(&http, &served.base).await;
    assert_eq!(deployed["ok"], true);
    assert!(deployed["position"].as_u64().unwrap() >= 1);

    let response = http
        .post(format!("{}/v1/mutate", served.base))
        .json(&serde_json::json!({
            "db": "db",
            "module": "app",
            "name": "put",
            "args": {"key": k("a"), "title": "first"},
            "idempotency_key": "call-1",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let mutated: serde_json::Value = response.json().await.unwrap();
    assert_eq!(mutated["ok"], true);
    assert_eq!(mutated["value"], "put-ok");

    let response = http
        .post(format!("{}/v1/query", served.base))
        .json(&serde_json::json!({
            "db": "db",
            "module": "app",
            "name": "get",
            "args": {"key": k("a")},
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let got: serde_json::Value = response.json().await.unwrap();
    assert_eq!(got["ok"], true);
    assert_eq!(got["value"], serde_json::json!({"title": "first"}));
}

#[tokio::test]
async fn business_failure_shapes() {
    let (_dir, served) = serve_node().await;
    let http = reqwest::Client::new();
    deploy(&http, &served.base).await;

    // Guest failures replicate their receipt: 422 with ok false, as v1.
    let response = http
        .post(format!("{}/v1/mutate", served.base))
        .json(&serde_json::json!({
            "db": "db",
            "module": "app",
            "name": "put",
            "args": {"title": "no key"},
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 422);
    let failed: serde_json::Value = response.json().await.unwrap();
    assert_eq!(failed["ok"], false);
    assert_eq!(failed["code"], "TYPE_MISMATCH");

    let response = http
        .post(format!("{}/v1/mutate", served.base))
        .json(&serde_json::json!({
            "db": "db",
            "module": "app",
            "name": "nope",
            "args": {"key": k("a")},
        }))
        .send()
        .await
        .unwrap();
    // Names the module doesn't expose as aliases are not found.
    assert_eq!(response.status(), 404);

    // Unknown query aliases too: 404 with the engine code.
    let response = http
        .post(format!("{}/v1/query", served.base))
        .json(&serde_json::json!({
            "db": "db",
            "module": "app",
            "name": "nope",
            "args": {"key": k("a")},
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 404);
    let failed: serde_json::Value = response.json().await.unwrap();
    assert_eq!(failed["ok"], false);
    assert_eq!(failed["code"], "UNKNOWN_METHOD");
}

#[tokio::test]
async fn malformed_requests_answer_400_with_an_envelope() {
    let (_dir, served) = serve_node().await;
    let base = &served.base;
    let http = reqwest::Client::new();
    deploy(&http, base).await;
    // Not JSON, then JSON missing a member: the request's fault, said
    // as every other refusal is, never a 422 a client would take for
    // the call's own failure.
    for body in ["{", r#"{"module": "app", "name": "read", "args": {}}"#] {
        let response = http
            .post(format!("{base}/v1/query"))
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 400, "{body}");
        let envelope: serde_json::Value = response.json().await.unwrap();
        assert_eq!(envelope["ok"], false);
        assert_eq!(envelope["code"], "bad_request", "{envelope}");
    }
}

#[tokio::test]
async fn uninitialized_node_forwards() {
    let (_dir, base) = serve_uninitialized().await;
    let http = reqwest::Client::new();
    let response = http
        .post(format!("{base}/v1/query"))
        .json(&serde_json::json!({
            "db": "db",
            "module": "app",
            "name": "get",
            "args": {"key": k("a")},
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 503);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["ok"], false);
    assert_eq!(body["code"], "forward");
}

fn test_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or(0)
}

#[tokio::test]
async fn queue_readiness_is_its_own_read() {
    let (_dir, served) = serve_node().await;
    let http = admin_http();
    deploy(&http, &served.base).await;
    // `first` waits in the empty scope's line; then a job arrives.
    let waited: serde_json::Value = http
        .post(format!("{}/v1/queue/poll", served.base))
        .json(&serde_json::json!({"db": "db", "queue": "jobs", "scope": "s", "owner": "first", "limit": 1, "wait_ms": 60000}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(waited["items"], serde_json::json!([]), "{waited}");
    {
        let engine = served.node.engine().lock().await;
        let payload = s("x");
        let mut item = flower_engine::work_queue::NewItem::new("jobs", &payload, 0, 5);
        item.scope = "s";
        engine.enqueue_item(b"db", &item, test_now_ms()).unwrap();
    }
    let ready = |owner: &str| {
        let url = format!(
            "{}/v1/queue/ready?db=db&queue=jobs&scope=s&owner={owner}",
            served.base
        );
        let http = http.clone();
        async move {
            let response = http.get(url).send().await.unwrap();
            assert_eq!(response.status(), 200);
            response.json::<serde_json::Value>().await.unwrap()
        }
    };
    // The first in line has the new job to itself for the turn; the
    // read answers readiness alone, without counting the scope's rows.
    let first = ready("first").await;
    assert_eq!(
        (&first["ok"], &first["ready"]),
        (&true.into(), &true.into()),
        "{first}"
    );
    assert!(first.get("pending").is_none(), "{first}");
    let second = ready("second").await;
    assert_eq!(second["ready"], false, "{second}");
    assert!(second["next_ms"].as_u64().is_some(), "{second}");
    // An unknown queue name answers a business failure, like stats.
    let bad: serde_json::Value = http
        .get(format!(
            "{}/v1/queue/ready?db=db&queue=bad%20name&scope=s",
            served.base
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(bad["ok"], false, "{bad}");
}

#[tokio::test]
async fn queue_claim_lifecycle_over_http() {
    let (_dir, served) = serve_node().await;
    let http = admin_http();
    deploy(&http, &served.base).await;
    // Seed two items straight into the leader's engine.
    let now = test_now_ms();
    let first = {
        let engine = served.node.engine().lock().await;
        let first = engine.enqueue(b"db", "jobs", &s("a"), 0, 1, now).unwrap();
        engine.enqueue(b"db", "jobs", &s("b"), 0, 5, now).unwrap();
        first
    };
    // Poll claims both, due-first.
    let response = http
        .post(format!("{}/v1/queue/poll", served.base))
        .json(&serde_json::json!({"db": "db", "queue": "jobs", "limit": 10, "lease_ms": 60000}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let polled: serde_json::Value = response.json().await.unwrap();
    assert_eq!(polled["ok"], true);
    let items = polled["items"].as_array().unwrap();
    assert_eq!(items.len(), 2);
    // Same-due items order by random ID: resolve tokens by item.
    let token_of = |id: &str| {
        items.iter().find(|item| item["id"] == id).unwrap()["token"]
            .as_str()
            .unwrap()
            .to_string()
    };
    let token = token_of(&first);
    let second = items.iter().find(|item| item["id"] != first).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let second_token = token_of(&second);
    // Heartbeat renews; completion lands; double completion fails
    // as a business failure, not a 500.
    let response = http
        .post(format!("{}/v1/queue/heartbeat", served.base))
        .json(&serde_json::json!({"db": "db", "queue": "jobs", "id": first, "token": token, "lease_ms": 60000}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        response.json::<serde_json::Value>().await.unwrap()["ok"],
        true
    );
    let response = http
        .post(format!("{}/v1/queue/complete", served.base))
        .json(&serde_json::json!({"db": "db", "queue": "jobs", "id": first, "token": token}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(
        response.json::<serde_json::Value>().await.unwrap()["ok"],
        true
    );
    let response = http
        .post(format!("{}/v1/queue/complete", served.base))
        .json(&serde_json::json!({"db": "db", "queue": "jobs", "id": first, "token": token}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let done: serde_json::Value = response.json().await.unwrap();
    assert_eq!(done["ok"], false);
    assert_eq!(done["code"], "INVALID_VALUE");
    // Failing the survivor (one attempt spent of five) requeues it;
    // stats and item inspection agree.
    let response = http
        .post(format!("{}/v1/queue/fail", served.base))
        .json(
            &serde_json::json!({"db": "db", "queue": "jobs", "id": second, "token": second_token}),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(
        response.json::<serde_json::Value>().await.unwrap()["ok"],
        true
    );
    let response = http
        .get(format!("{}/v1/queue/stats?db=db&queue=jobs", served.base))
        .send()
        .await
        .unwrap();
    let stats: serde_json::Value = response.json().await.unwrap();
    assert_eq!(stats["ok"], true);
    assert_eq!(stats["pending"], 1);
    assert_eq!(stats["claimed"], 0);
    assert_eq!(stats["dead"], 0);
    let response = http
        .get(format!(
            "{}/v1/queue/item?db=db&queue=jobs&id={second}",
            served.base
        ))
        .send()
        .await
        .unwrap();
    let item: serde_json::Value = response.json().await.unwrap();
    assert_eq!(item["ok"], true);
    assert_eq!(item["item"]["state"], "pending");
    assert_eq!(item["item"]["attempts"], 1);
    assert_eq!(item["item"]["payload"], "b");
    // Cancel removes it; a second cancel reports missing.
    let response = http
        .post(format!("{}/v1/queue/cancel", served.base))
        .json(&serde_json::json!({"db": "db", "queue": "jobs", "id": second}))
        .send()
        .await
        .unwrap();
    let cancelled: serde_json::Value = response.json().await.unwrap();
    assert_eq!(cancelled["ok"], true);
    assert_eq!(cancelled["removed"], true);
    let response = http
        .post(format!("{}/v1/queue/cancel", served.base))
        .json(&serde_json::json!({"db": "db", "queue": "jobs", "id": second}))
        .send()
        .await
        .unwrap();
    let cancelled: serde_json::Value = response.json().await.unwrap();
    assert_eq!(cancelled["removed"], false);
}

#[tokio::test]
async fn queue_dead_letter_over_http() {
    let (_dir, served) = serve_node().await;
    let http = admin_http();
    deploy(&http, &served.base).await;
    let now = test_now_ms();
    {
        let engine = served.node.engine().lock().await;
        engine.enqueue(b"db", "jobs", &s("a"), 0, 1, now).unwrap();
    }
    let response = http
        .post(format!("{}/v1/queue/poll", served.base))
        .json(&serde_json::json!({"db": "db", "queue": "jobs"}))
        .send()
        .await
        .unwrap();
    let polled: serde_json::Value = response.json().await.unwrap();
    let id = polled["items"][0]["id"].as_str().unwrap().to_string();
    let token = polled["items"][0]["token"].as_str().unwrap().to_string();
    // One attempt spent of one: the item dies.
    let response = http
        .post(format!("{}/v1/queue/fail", served.base))
        .json(&serde_json::json!({"db": "db", "queue": "jobs", "id": id, "token": token}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        response.json::<serde_json::Value>().await.unwrap()["ok"],
        true
    );
    let response = http
        .get(format!(
            "{}/v1/queue/item?db=db&queue=jobs&id={id}",
            served.base
        ))
        .send()
        .await
        .unwrap();
    let item: serde_json::Value = response.json().await.unwrap();
    assert_eq!(item["item"]["state"], "dead");
    // Retry revives it; release hands it back without an attempt.
    let response = http
        .post(format!("{}/v1/queue/retry", served.base))
        .json(&serde_json::json!({"db": "db", "queue": "jobs", "id": id}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        response.json::<serde_json::Value>().await.unwrap()["ok"],
        true
    );
    let response = http
        .post(format!("{}/v1/queue/poll", served.base))
        .json(&serde_json::json!({"db": "db", "queue": "jobs"}))
        .send()
        .await
        .unwrap();
    let polled: serde_json::Value = response.json().await.unwrap();
    let token = polled["items"][0]["token"].as_str().unwrap().to_string();
    let response = http
        .post(format!("{}/v1/queue/release", served.base))
        .json(&serde_json::json!({"db": "db", "queue": "jobs", "id": id, "token": token}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        response.json::<serde_json::Value>().await.unwrap()["ok"],
        true
    );
    // Kill it again, then purge the dead letter away.
    let response = http
        .post(format!("{}/v1/queue/poll", served.base))
        .json(&serde_json::json!({"db": "db", "queue": "jobs"}))
        .send()
        .await
        .unwrap();
    let polled: serde_json::Value = response.json().await.unwrap();
    let token = polled["items"][0]["token"].as_str().unwrap().to_string();
    http.post(format!("{}/v1/queue/fail", served.base))
        .json(&serde_json::json!({"db": "db", "queue": "jobs", "id": id, "token": token}))
        .send()
        .await
        .unwrap();
    let response = http
        .post(format!("{}/v1/queue/purge", served.base))
        .json(&serde_json::json!({"db": "db", "queue": "jobs", "older_than_ms": test_now_ms() + 1000}))
        .send()
        .await
        .unwrap();
    let purged: serde_json::Value = response.json().await.unwrap();
    assert_eq!(purged["ok"], true);
    assert_eq!(purged["removed"], 1);
    let response = http
        .get(format!(
            "{}/v1/queue/item?db=db&queue=jobs&id={id}",
            served.base
        ))
        .send()
        .await
        .unwrap();
    let item: serde_json::Value = response.json().await.unwrap();
    assert_eq!(item["ok"], false);
    assert_eq!(item["code"], "UNKNOWN_ITEM");
}

#[tokio::test]
async fn queue_delays_are_bounded_over_http() {
    let (_dir, served) = serve_node().await;
    let http = admin_http();
    deploy(&http, &served.base).await;
    let now = test_now_ms();
    let (live, dying) = {
        let engine = served.node.engine().lock().await;
        (
            engine.enqueue(b"db", "jobs", &s("a"), 0, 5, now).unwrap(),
            engine.enqueue(b"db", "jobs", &s("b"), 0, 1, now).unwrap(),
        )
    };
    let post = |path: &str, body: serde_json::Value| {
        let request = http.post(format!("{}/v1/queue/{path}", served.base));
        async move {
            request
                .json(&body)
                .send()
                .await
                .unwrap()
                .json::<serde_json::Value>()
                .await
                .unwrap()
        }
    };
    let polled = post("poll", serde_json::json!({"db": "db", "queue": "jobs"})).await;
    let token = |id: &str| {
        polled["items"]
            .as_array()
            .unwrap()
            .iter()
            .find(|item| item["id"] == id)
            .unwrap()["token"]
            .clone()
    };
    let (live_token, dying_token) = (token(&live), token(&dying));
    // Past 30 days, fail and release refuse as business failures.
    let too_long = 30u64 * 24 * 60 * 60 * 1000 + 1;
    for path in ["fail", "release"] {
        let refused = post(
            path,
            serde_json::json!({"db": "db", "queue": "jobs", "id": live, "token": live_token, "delay_ms": too_long}),
        )
        .await;
        assert_eq!(refused["ok"], false, "{path}: {refused}");
        assert_eq!(refused["code"], "INVALID_VALUE", "{path}: {refused}");
    }
    // A delay a row's numbers would round leaves no item a cancel
    // cannot find: refused, so the cancel takes the claim.
    let huge = post(
        "fail",
        serde_json::json!({"db": "db", "queue": "jobs", "id": live, "token": live_token, "delay_ms": 1u64 << 60}),
    )
    .await;
    let cancelled = post(
        "cancel",
        serde_json::json!({"db": "db", "queue": "jobs", "id": live}),
    )
    .await;
    assert_eq!(cancelled["removed"], true);
    // Retries refuse too.
    post(
        "fail",
        serde_json::json!({"db": "db", "queue": "jobs", "id": dying, "token": dying_token}),
    )
    .await;
    let refused = post(
        "retry",
        serde_json::json!({"db": "db", "queue": "jobs", "id": dying, "delay_ms": too_long}),
    )
    .await;
    assert_eq!(refused["code"], "INVALID_VALUE", "{refused}");
    let stats: serde_json::Value = http
        .get(format!("{}/v1/queue/stats?db=db&queue=jobs", served.base))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        (&stats["pending"], &stats["claimed"], &stats["dead"]),
        (
            &serde_json::json!(0),
            &serde_json::json!(0),
            &serde_json::json!(1)
        ),
        "{stats}"
    );
    assert_eq!(huge["code"], "INVALID_VALUE", "{huge}");
}

#[tokio::test]
async fn timer_inspect_and_maintain_over_http() {
    let (_dir, served) = serve_node().await;
    let http = admin_http();
    // Seed two timers straight into the leader's engine. The
    // handlers name nothing deployed, so fires die on budget.
    let now = test_now_ms();
    {
        let engine = served.node.engine().lock().await;
        engine
            .schedule_timer(b"db", "t1", "app", "missing", &s("a"), 0, 1, now)
            .unwrap();
        engine
            .schedule_timer(b"db", "t2", "app", "missing", &s("b"), u64::MAX, 5, now)
            .unwrap();
    }
    let response = http
        .get(format!("{}/v1/timer/stats?db=db", served.base))
        .send()
        .await
        .unwrap();
    let stats: serde_json::Value = response.json().await.unwrap();
    assert_eq!(stats["ok"], true);
    assert_eq!(stats["pending"], 2);
    assert_eq!(stats["dead"], 0);
    // Due-first list; single read; unknown reads fail cleanly.
    let response = http
        .get(format!("{}/v1/timer/list?db=db", served.base))
        .send()
        .await
        .unwrap();
    let list: serde_json::Value = response.json().await.unwrap();
    assert_eq!(list["timers"].as_array().unwrap().len(), 2);
    assert_eq!(list["timers"][0]["id"], "t1");
    assert_eq!(list["timers"][0]["handler"], "missing");
    assert_eq!(list["timers"][0]["attempts"], 0);
    let response = http
        .get(format!("{}/v1/timer/get?db=db&id=t1", served.base))
        .send()
        .await
        .unwrap();
    let item: serde_json::Value = response.json().await.unwrap();
    assert_eq!(item["item"]["state"], "pending");
    assert_eq!(item["item"]["args"], "a");
    let response = http
        .get(format!("{}/v1/timer/get?db=db&id=nope", served.base))
        .send()
        .await
        .unwrap();
    let item: serde_json::Value = response.json().await.unwrap();
    assert_eq!(item["ok"], false);
    assert_eq!(item["code"], "UNKNOWN_TIMER");
    // Fire the one-shot through consensus: it dies, then retry
    // revives it and cancel removes it.
    served.node.fire_timer(b"db", "t1").await.unwrap();
    let response = http
        .get(format!("{}/v1/timer/list?db=db&state=dead", served.base))
        .send()
        .await
        .unwrap();
    let list: serde_json::Value = response.json().await.unwrap();
    assert_eq!(list["timers"].as_array().unwrap().len(), 1);
    let response = http
        .post(format!("{}/v1/timer/retry", served.base))
        .json(&serde_json::json!({"db": "db", "id": "t1"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        response.json::<serde_json::Value>().await.unwrap()["ok"],
        true
    );
    let response = http
        .post(format!("{}/v1/timer/cancel", served.base))
        .json(&serde_json::json!({"db": "db", "id": "t1"}))
        .send()
        .await
        .unwrap();
    let cancelled: serde_json::Value = response.json().await.unwrap();
    assert_eq!(cancelled["removed"], true);
    let response = http
        .post(format!("{}/v1/timer/purge", served.base))
        .json(&serde_json::json!({"db": "db", "older_than_ms": test_now_ms() + 1000}))
        .send()
        .await
        .unwrap();
    let purged: serde_json::Value = response.json().await.unwrap();
    assert_eq!(purged["removed"], 0);
}

#[tokio::test]
async fn timer_forwards_off_leader() {
    let (_dir, base) = serve_uninitialized().await;
    let http = admin_http();
    let response = http
        .get(format!("{base}/v1/timer/stats?db=db"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 503);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["code"], "forward");
}

#[tokio::test]
async fn queue_forwards_off_leader() {
    let (_dir, base) = serve_uninitialized().await;
    let http = admin_http();
    let response = http
        .post(format!("{base}/v1/queue/poll"))
        .json(&serde_json::json!({"db": "db", "queue": "jobs"}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 503);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["ok"], false);
    assert_eq!(body["code"], "forward");
}

#[tokio::test]
async fn admin_endpoints_reject_without_token() {
    let (_dir, served) = serve_node().await;
    let http = reqwest::Client::new();
    let body = serde_json::json!({
        "db": "db",
        "module": "app",
        "identity": null,
        "wasm_b64": "e30=",
        "manifest": manifest(),
    });
    let response = http
        .post(format!("{}/v1/deploy", served.base))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 403);
    let response = http
        .post(format!("{}/v1/deploy", served.base))
        .header("authorization", "Bearer wrong")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 403);
    let denied: serde_json::Value = response.json().await.unwrap();
    assert_eq!(denied["code"], "forbidden");
}

#[tokio::test]
async fn membership_changes_and_validates() {
    let (_dir, served) = serve_node().await;
    let http = reqwest::Client::new();
    let response = http
        .post(format!("{}/v1/admin/membership", served.base))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({"members": ["1"], "retain": false}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let response = http
        .post(format!("{}/v1/admin/membership", served.base))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({"members": ["abc"], "retain": false}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 400);
}

#[tokio::test]
async fn backup_needs_a_driver_then_reports() {
    let (dir, served) = serve_node().await;
    let http = admin_http();
    let response = http
        .post(format!("{}/v1/admin/backup", served.base))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 503);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["code"], "unavailable");
    let status: serde_json::Value = http
        .get(format!("{}/v1/admin/backup", served.base))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(status["enabled"], false);
    assert!(status["continuous"].is_null() && status["last_backup"].is_null());
    assert_eq!(status["log"]["hold"]["enabled"], false);

    // Attach a memory driver through a second server over the same node.
    let handler = app_handler();
    let driver = flower_backup::driver::Driver::memory(
        &dir.path().join("keystore.json"),
        [7u8; 32],
        served.node.engine().clone(),
        served.node.logs().clone(),
        Arc::new(
            move |_db: &[u8], _module: &str, _wasm: &[u8], _manifest: &Value| {
                let handler = handler.clone();
                Ok(Box::new(ClosureRuntime::new(
                    move |kind, module, name, args, host| handler(kind, module, name, args, host),
                ))
                    as Box<dyn flower_engine::runtime::Runtime>)
            },
        ),
    )
    .unwrap();
    let state = AppState::new(served.node.clone()).with_admin_token("secret".to_string());
    let state = state.with_backup(Arc::new(driver));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(serve(listener, state));

    deploy(&http, &served.base).await;
    let response = http
        .post(format!("{base}/v1/admin/backup"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let summary: serde_json::Value = response.json().await.unwrap();
    assert_eq!(summary["ok"], true);
    assert!(!summary["backup_id"].as_str().unwrap().is_empty());
    assert!(summary["objects"].as_u64().unwrap() >= 1);

    let response = http.get(format!("{base}/v1/metrics")).send().await.unwrap();
    assert_eq!(response.status(), 200);
    let metrics: serde_json::Value = response.json().await.unwrap();
    assert_eq!(metrics["last_backup"]["backup_id"], summary["backup_id"]);

    // The status endpoint reports the driver, idle until continuous
    // backup runs, and the backup taken through the API.
    let response = http
        .get(format!("{base}/v1/admin/backup"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let status: serde_json::Value = response.json().await.unwrap();
    assert_eq!(status["enabled"], true);
    assert_eq!(status["continuous"]["role"], "idle");
    assert_eq!(status["last_backup"]["backup_id"], summary["backup_id"]);
    assert!(status["log"]["first_retained"].as_u64().is_some());
    // Operator-only, like every admin surface.
    let response = reqwest::Client::new()
        .get(format!("{base}/v1/admin/backup"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 403);
}

#[tokio::test]
async fn watch_streams_snapshots() {
    let (_dir, served) = serve_node().await;
    let http = reqwest::Client::new();
    deploy(&http, &served.base).await;
    http.post(format!("{}/v1/mutate", served.base))
        .json(&serde_json::json!({
            "db": "db",
            "module": "app",
            "name": "put",
            "args": {"key": k("a"), "title": "first"},
        }))
        .send()
        .await
        .unwrap();

    let args: serde_json::Value = serde_json::from_slice(br#"{"key": "\"a\""}"#).unwrap();
    let mut response = http
        .post(format!("{}/v1/watch", served.base))
        .json(&serde_json::json!({"db": "db", "module": "app", "name": "get", "args": args}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let first = read_dispatch(&mut response).await;
    assert!(first.contains("event: snapshot"));
    assert!(first.contains("first"));

    http.post(format!("{}/v1/mutate", served.base))
        .json(&serde_json::json!({
            "db": "db",
            "module": "app",
            "name": "put",
            "args": {"key": k("a"), "title": "second"},
        }))
        .send()
        .await
        .unwrap();
    let second = read_dispatch(&mut response).await;
    assert!(second.contains("event: snapshot"));
    assert!(second.contains("second"));
}

/// Put the task `key` titled `title`.
async fn put_task(http: &reqwest::Client, base: &str, key: &str, title: &str) {
    let response = http
        .post(format!("{base}/v1/mutate"))
        .json(&serde_json::json!({
            "db": "db",
            "module": "app",
            "name": "put",
            "args": {"key": k(key), "title": title},
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
}

#[tokio::test]
async fn a_watch_failing_before_its_first_snapshot_keeps_the_details() {
    let (_dir, served) = serve_node().await;
    let http = reqwest::Client::new();
    deploy(&http, &served.base).await;
    put_task(&http, &served.base, "a", "refused").await;
    let body =
        serde_json::json!({"db": "db", "module": "app", "name": "get", "args": {"key": k("a")}});
    // The watch answers the failure the query does, details included.
    for path in ["/v1/query", "/v1/watch"] {
        let response = http
            .post(format!("{}{path}", served.base))
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 422, "{path}");
        let answer: serde_json::Value = response.json().await.unwrap();
        assert_eq!(answer["code"], "TASK_REFUSED", "{path}: {answer}");
        assert_eq!(
            answer["details"],
            serde_json::json!({"key": k("a")}),
            "{path}: {answer}"
        );
    }
}

#[tokio::test]
async fn a_watch_error_event_keeps_the_details() {
    let (_dir, served) = serve_node().await;
    let http = reqwest::Client::new();
    deploy(&http, &served.base).await;
    put_task(&http, &served.base, "a", "first").await;
    let mut response = http
        .post(format!("{}/v1/watch", served.base))
        .json(&serde_json::json!({"db": "db", "module": "app", "name": "get", "args": {"key": k("a")}}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert!(
        read_dispatch(&mut response)
            .await
            .contains("event: snapshot")
    );

    put_task(&http, &served.base, "a", "refused").await;
    let dispatch = read_dispatch(&mut response).await;
    assert!(dispatch.contains("event: error"), "{dispatch}");
    let data = dispatch
        .lines()
        .find_map(|line| line.strip_prefix("data: "))
        .unwrap();
    let error: serde_json::Value = serde_json::from_str(data).unwrap();
    assert_eq!(error["code"], "TASK_REFUSED", "{error}");
    assert_eq!(
        error["details"],
        serde_json::json!({"key": k("a")}),
        "{error}"
    );
}

#[tokio::test]
async fn watch_rejects_unknown_definitions() {
    let (_dir, served) = serve_node().await;
    let http = reqwest::Client::new();
    deploy(&http, &served.base).await;
    let args: serde_json::Value = serde_json::from_slice(br#"{"key": "\"a\""}"#).unwrap();
    let response = http
        .post(format!("{}/v1/watch", served.base))
        .json(&serde_json::json!({"db": "db", "module": "app", "name": "nope", "args": args}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 404);
}

#[tokio::test]
async fn sync_status_and_metrics() {
    let (_dir, served) = serve_node().await;
    let http = admin_http();
    deploy(&http, &served.base).await;
    http.post(format!("{}/v1/mutate", served.base))
        .json(&serde_json::json!({
            "db": "db",
            "module": "app",
            "name": "put",
            "args": {"key": k("a"), "title": "first"},
        }))
        .send()
        .await
        .unwrap();

    let response = http
        .get(format!("{}/v1/sync?since=0", served.base))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let batch: serde_json::Value = response.json().await.unwrap();
    assert!(batch["entries"].as_array().unwrap().len() >= 2);
    let current = batch["current"].as_u64().unwrap();
    assert!(current >= 1);
    let response = http
        .get(format!("{}/v1/sync?since={current}", served.base))
        .send()
        .await
        .unwrap();
    let batch: serde_json::Value = response.json().await.unwrap();
    assert!(batch["entries"].as_array().unwrap().is_empty());

    let response = http
        .get(format!("{}/v1/status", served.base))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let status: serde_json::Value = response.json().await.unwrap();
    assert_eq!(status["id"], 1);
    assert_eq!(status["leader"], 1);
    assert_eq!(status["state"], "Leader");

    let response = http
        .get(format!("{}/v1/metrics", served.base))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let metrics: serde_json::Value = response.json().await.unwrap();
    let dbs = metrics["dbs"].as_array().unwrap();
    assert_eq!(dbs.len(), 1);
    assert_eq!(dbs[0]["db"], "db");
    assert!(dbs[0]["revision"].as_u64().unwrap() >= 1);
    // The background log purge and the snapshot triggers report too.
    let purge = &metrics["log_purge"];
    assert_eq!(purge["transactions"], 0, "{purge}");
    assert_eq!(purge["behind"], false);
    assert_eq!(purge["hold"]["enabled"], false);
    let snapshots = &metrics["snapshots"];
    assert!(
        snapshots["unsnapshotted_bytes"].as_u64().unwrap() > 0,
        "{snapshots}"
    );
    assert_eq!(snapshots["builds"], 0);
    assert_eq!(snapshots["after_bytes"], 16 * 1024 * 1024);
    assert!(snapshots["applied"].as_u64().unwrap() >= current);
}

/// A client presenting the admin token on every request.
fn admin_http() -> reqwest::Client {
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        reqwest::header::AUTHORIZATION,
        reqwest::header::HeaderValue::from_static("Bearer secret"),
    );
    reqwest::Client::builder()
        .default_headers(headers)
        .build()
        .unwrap()
}

/// Send a request with an optional bearer token, returning the status.
async fn status_with(request: reqwest::RequestBuilder, token: Option<&str>) -> u16 {
    let request = match token {
        Some(token) => request.header("authorization", format!("Bearer {token}")),
        None => request,
    };
    request.send().await.unwrap().status().as_u16()
}

#[tokio::test]
async fn credentials_gate_operator_worker_and_sync_surfaces() {
    let (_dir, served) = serve_configured(|state| {
        state
            .with_worker_token("worker".to_string())
            .with_sync_token("device".to_string())
            .with_deploy_limit(4096)
    })
    .await;
    let http = reqwest::Client::new();
    let base = &served.base;
    let poll = || {
        http.post(format!("{base}/v1/queue/poll"))
            .json(&serde_json::json!({"db": "db", "queue": "jobs"}))
    };
    let sync = || http.get(format!("{base}/v1/sync?since=0"));
    let timer = || http.get(format!("{base}/v1/timer/stats?db=db"));
    let metrics = || http.get(format!("{base}/v1/metrics"));

    // Queues: worker or admin only.
    assert_eq!(status_with(poll(), None).await, 403);
    assert_eq!(status_with(poll(), Some("device")).await, 403);
    assert_eq!(status_with(poll(), Some("worker")).await, 200);
    assert_eq!(status_with(poll(), Some("secret")).await, 200);
    // Sync: device or admin only.
    assert_eq!(status_with(sync(), None).await, 403);
    assert_eq!(status_with(sync(), Some("worker")).await, 403);
    assert_eq!(status_with(sync(), Some("device")).await, 200);
    assert_eq!(status_with(sync(), Some("secret")).await, 200);
    // Timers and metrics: admin only.
    for token in [None, Some("worker"), Some("device"), Some("secre")] {
        assert_eq!(status_with(timer(), token).await, 403);
        assert_eq!(status_with(metrics(), token).await, 403);
    }
    assert_eq!(status_with(timer(), Some("secret")).await, 200);
    assert_eq!(status_with(metrics(), Some("secret")).await, 200);

    // The sync batch echoes the position it continues from.
    let batch: serde_json::Value = http
        .get(format!("{base}/v1/sync?since=0&limit=1000000"))
        .header("authorization", "Bearer device")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(batch["since"], 0);

    // Deploy bodies are capped, not unlimited.
    let oversized = http
        .post(format!("{base}/v1/deploy"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({
            "db": "db",
            "module": "app",
            "wasm_b64": "A".repeat(8192),
            "manifest": manifest(),
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(oversized.status(), 413);
    // A non-ASCII identity is a bad request, not a panic.
    let bad_identity = http
        .post(format!("{base}/v1/deploy"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({
            "db": "db",
            "module": "app",
            "identity": format!("a{}{}", "é", "0".repeat(61)),
            "wasm_b64": "e30=",
            "manifest": manifest(),
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(bad_identity.status(), 400);
}

#[tokio::test]
async fn callers_shorten_deadlines_but_never_buy_fuel() {
    let (_dir, served) = serve_node().await;
    let http = reqwest::Client::new();
    deploy(&http, &served.base).await;
    let call = serde_json::json!({
        "db": "db",
        "module": "app",
        "name": "put",
        "args": {"key": k("a"), "title": "first"},
        "fuel": 0,
    });
    // Nobody states fuel: the member means nothing, from an anonymous
    // caller and a trusted gateway alike.
    for gateway in [false, true] {
        let mut request = http.post(format!("{}/v1/mutate", served.base));
        if gateway {
            request = request.header("authorization", "Bearer secret");
        }
        let answer: serde_json::Value = request
            .json(&call)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(answer["ok"], true, "{answer}");
    }
    // A caller's own deadline, which has already passed, stops the call.
    let response = http
        .post(format!("{}/v1/query", served.base))
        .header("x-flower-timeout-ms", "0")
        .json(&serde_json::json!({
            "db": "db",
            "module": "app",
            "name": "get",
            "args": {"key": k("a")},
        }))
        .send()
        .await
        .unwrap();
    assert!(!response.status().is_success(), "{}", response.status());
}

#[tokio::test]
async fn health_probes_report_liveness_and_readiness() {
    let (_dir, served) = serve_node().await;
    let http = reqwest::Client::new();
    let live = http
        .get(format!("{}/health/live", served.base))
        .send()
        .await
        .unwrap();
    assert_eq!(live.status(), 200);
    let ready = http
        .get(format!("{}/health/ready", served.base))
        .send()
        .await
        .unwrap();
    assert_eq!(ready.status(), 200);
    let body: serde_json::Value = ready.json().await.unwrap();
    assert_eq!(body["ok"], true);
    assert_eq!(body["leader"], 1);

    let (_dir, base) = serve_uninitialized().await;
    let live = http
        .get(format!("{base}/health/live"))
        .send()
        .await
        .unwrap();
    assert_eq!(live.status(), 200);
    let ready = http
        .get(format!("{base}/health/ready"))
        .send()
        .await
        .unwrap();
    assert_eq!(ready.status(), 503);
    let body: serde_json::Value = ready.json().await.unwrap();
    assert_eq!(body["ok"], false);
}

#[tokio::test]
async fn watch_ends_with_an_error_when_refresh_fails() {
    let (_dir, served) = serve_node().await;
    let http = reqwest::Client::new();
    deploy(&http, &served.base).await;
    let args: serde_json::Value = serde_json::from_slice(br#"{"key": "\"a\""}"#).unwrap();
    let mut response = http
        .post(format!("{}/v1/watch", served.base))
        .json(&serde_json::json!({"db": "db", "module": "app", "name": "get", "args": args}))
        .send()
        .await
        .unwrap();
    assert!(
        read_dispatch(&mut response)
            .await
            .contains("event: snapshot")
    );
    // Redeploy without the watched query: the next refresh fails, so
    // the stream reports it and ends instead of idling on keepalives.
    let mut without_get = manifest();
    without_get["definitions"] = serde_json::json!([
        {"name": "put", "kind": "mutation", "args": {"type": "json"}}
    ]);
    without_get["http"] = serde_json::json!([{"alias": "put", "definition": "put"}]);
    let redeployed = http
        .post(format!("{}/v1/deploy", served.base))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({
            "db": "db",
            "module": "app",
            "wasm_b64": "e30=",
            "manifest": without_get,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(redeployed.status(), 200);
    read_until_end(&mut response, "event: error").await;
}

#[tokio::test]
async fn watch_keys_on_the_whole_principal() {
    let (_dir, served) = serve_node().await;
    let http = reqwest::Client::new();
    deploy(&http, &served.base).await;
    let hub = flower_server::watch::Hub::new(served.node.clone());
    let args = obj(vec![("key", s(&k("a")))]);
    let mut streams = Vec::new();
    for tenant in ["acme", "globex"] {
        let options = flower_engine::engine::CallOptions {
            principal: Some(
                flower_sdk::Principal::new("same-user".to_string(), Some(tenant.to_string()), None)
                    .unwrap(),
            ),
            ..Default::default()
        };
        streams.push(
            hub.subscribe(b"db", "app", "get", &args, &options)
                .await
                .unwrap(),
        );
    }
    // Same subject, different tenants: separate evaluations.
    assert_eq!(hub.keys().await, 2);
}

#[tokio::test]
async fn serve_until_drains_and_releases_the_node() {
    let dir = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let node = Node::open(
        1,
        &dir.path().join("n1"),
        Arc::new(openraft::Config::default()),
        InProcessFactory::new(registry.clone()),
        Arc::new(TestFactory {
            handler: app_handler(),
        }),
    )
    .await
    .unwrap();
    registry.register(1, node.raft().clone());
    node.initialize(BTreeSet::from([1])).await.unwrap();
    let node = Arc::new(node);
    for _ in 0..500 {
        if node.status().leader == Some(1) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let state = AppState::new(node.clone()).with_admin_token("secret".to_string());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(flower_server::api::serve_until(
        listener,
        state,
        async move {
            let _ = stopped.await;
        },
        Duration::from_secs(5),
    ));
    let http = reqwest::Client::new();
    deploy(&http, &base).await;
    let args: serde_json::Value = serde_json::from_slice(br#"{"key": "\"a\""}"#).unwrap();
    let mut watch = http
        .post(format!("{base}/v1/watch"))
        .json(&serde_json::json!({"db": "db", "module": "app", "name": "get", "args": args}))
        .send()
        .await
        .unwrap();
    assert!(read_dispatch(&mut watch).await.contains("event: snapshot"));

    stop.send(()).unwrap();
    // The open watch ends promptly instead of pinning the drain.
    read_until_end(&mut watch, "event: error").await;
    tokio::time::timeout(Duration::from_secs(20), server)
        .await
        .expect("serve_until never returned")
        .unwrap()
        .unwrap();
    // The hub let go of the node: this is the last handle, so the
    // node can shut down.
    let node = Arc::try_unwrap(node)
        .ok()
        .expect("a task still holds the node");
    node.shutdown().await.unwrap();
}

/// An in-memory backup store whose manifest uploads wait until the test
/// lets them go, saying when one starts waiting.
#[derive(Debug)]
struct Held {
    inner: object_store::memory::InMemory,
    reached: tokio::sync::Semaphore,
    released: tokio::sync::Semaphore,
}

impl std::fmt::Display for Held {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Held({})", self.inner)
    }
}

#[async_trait::async_trait]
impl object_store::ObjectStore for Held {
    async fn put_opts(
        &self,
        location: &object_store::path::Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        if location.as_ref().contains("/manifest/") {
            self.reached.add_permits(1);
            let _released = self.released.acquire().await.unwrap();
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &object_store::path::Path,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &object_store::path::Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: futures_util::stream::BoxStream<
            'static,
            object_store::Result<object_store::path::Path>,
        >,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>
    {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
    {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// A backup taken through the API finishes although its client stopped
/// waiting, and shows as the last one: the backup runs on its own task,
/// so a base that outlasts a client's patience is not cut off
/// mid-upload, whatever the server does with the abandoned request.
#[tokio::test(flavor = "multi_thread")]
async fn an_api_backup_finishes_after_its_client_gives_up() {
    let (dir, served) = serve_node().await;
    let http = admin_http();
    deploy(&http, &served.base).await;
    let store = Arc::new(Held {
        inner: object_store::memory::InMemory::new(),
        reached: tokio::sync::Semaphore::new(0),
        released: tokio::sync::Semaphore::new(0),
    });
    let handler = app_handler();
    let driver = flower_backup::driver::Driver::new(
        store.clone(),
        &dir.path().join("keystore.json"),
        [7u8; 32],
        served.node.engine().clone(),
        served.node.logs().clone(),
        Arc::new(
            move |_db: &[u8], _module: &str, _wasm: &[u8], _manifest: &Value| {
                let handler = handler.clone();
                Ok(Box::new(ClosureRuntime::new(
                    move |kind, module, name, args, host| handler(kind, module, name, args, host),
                ))
                    as Box<dyn flower_engine::runtime::Runtime>)
            },
        ),
    )
    .unwrap();
    let state = AppState::new(served.node.clone())
        .with_admin_token("secret".to_string())
        .with_backup(Arc::new(driver));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(serve(listener, state));

    // The client waits until the backup is uploading its manifest, then
    // gives up: its request goes away with the connection.
    let client = tokio::spawn({
        let (http, base) = (http.clone(), base.clone());
        async move {
            http.post(format!("{base}/v1/admin/backup"))
                .json(&serde_json::json!({}))
                .send()
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(30), store.reached.acquire())
        .await
        .expect("the backup never reached its manifest")
        .unwrap()
        .forget();
    client.abort();
    assert!(client.await.unwrap_err().is_cancelled());
    store.released.add_permits(1);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let status: serde_json::Value = http
            .get(format!("{base}/v1/admin/backup"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if !status["last_backup"].is_null() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the backup its client gave up on never finished"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
