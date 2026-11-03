//! Typed watches and subscriptions, declared failures, and the
//! scripted test transport.

extern crate alloc;

mod fixtures;

use fixtures::hc;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use flower_client::client::{FlowerClient, MutationOptions, RequestOptions};
use flower_client::error::FlowerError;
use flower_client::testing::{MockBody, MockReply, MockTransport};
use flower_client::transport::Credentials;
use flower_client::typed::{RemoteError, RemoteTarget};
use flower_client::watch::{SubscribeOptions, WatchOptions};
use flower_cluster::network::{InProcessFactory, Registry};
use flower_cluster::node::Node;
use flower_cluster::statemachine::RuntimeFactory;
use flower_engine::runtime::{CallKind, ClosureRuntime, HostCall};
use flower_sdk::abi::HostOp;
use flower_sdk::{Failure, FlowerError as DeclaredError, FlowerType, Value, fail};
use flower_server::api::{AppState, serve};

#[derive(FlowerType, Clone, Debug, PartialEq)]
struct Task {
    title: String,
}

#[derive(FlowerType, Clone, Debug, PartialEq)]
struct Key {
    key: String,
}

#[derive(FlowerType, Clone, Debug, PartialEq)]
struct Put {
    key: String,
    title: String,
}

#[derive(DeclaredError, Clone, Debug, PartialEq)]
enum TaskError {
    #[flower(code = "NOT_FOUND")]
    Missing,
    #[flower(code = "INVALID")]
    Invalid(String),
}

fn text(map: &std::collections::BTreeMap<String, Value>, field: &str) -> Result<String, Failure> {
    match map.get(field) {
        Some(Value::String(text)) => Ok(text.clone()),
        _ => Err(fail("TYPE_MISMATCH", "missing field")),
    }
}

/// Tasks keyed by plain strings: `put` refuses empty titles with a
/// declared `invalid` carrying the field, `get` of `missing` fails
/// `not_found`, and `fatal` fails with an undeclared code.
fn handle(
    kind: CallKind,
    name: &str,
    args: &Value,
    host: &mut dyn HostCall,
) -> Result<Value, Failure> {
    let Value::Object(map) = args else {
        return Err(fail("TYPE_MISMATCH", "args take an object"));
    };
    let key = text(map, "key")?;
    let stored = format!("\"{key}\"");
    let tasks = || Value::String("tasks".to_string());
    match (kind, name) {
        (CallKind::Mutation, "put") => {
            let title = text(map, "title")?;
            if title.is_empty() {
                return Err(Failure::with_details(
                    "INVALID",
                    "titles are not empty",
                    Value::String("title".to_string()),
                ));
            }
            let mut task = std::collections::BTreeMap::new();
            task.insert("title".to_string(), Value::String(title));
            hc(
                host,
                HostOp::Set,
                vec![
                    Value::String(String::new()),
                    tasks(),
                    Value::String(stored),
                    Value::Object(task),
                ],
            )?;
            Ok(Value::Null)
        }
        (CallKind::Mutation, "fatal") => Err(fail("SOMETHING_ELSE", "undeclared")),
        (CallKind::Query, "get") => {
            if key == "missing" {
                return Err(fail("NOT_FOUND", "no such task"));
            }
            if key.is_empty() {
                return Err(Failure::with_details(
                    "INVALID",
                    "keys are not empty",
                    Value::String("key".to_string()),
                ));
            }
            hc(
                host,
                HostOp::Get,
                vec![Value::String(String::new()), tasks(), Value::String(stored)],
            )
        }
        _ => Err(fail("UNKNOWN_METHOD", "unknown definition")),
    }
}

struct Factory;

impl RuntimeFactory for Factory {
    fn build(
        &self,
        _db: &[u8],
        _module: &str,
        _wasm: &[u8],
        _manifest: &Value,
    ) -> Result<Box<dyn flower_engine::runtime::Runtime>, String> {
        Ok(Box::new(ClosureRuntime::new(
            |kind, _module, name, args, host| handle(kind, name, args, host),
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
            {"name": "fatal", "kind": "mutation", "args": {"type": "json"}},
            {"name": "get", "kind": "query", "args": {"type": "json"}}
        ],
        "triggers": [],
        "http": [
            {"alias": "put", "definition": "put"},
            {"alias": "fatal", "definition": "fatal"},
            {"alias": "get", "definition": "get"}
        ]
    })
}

/// Serve an initialized single node with the module deployed.
async fn serve_app() -> (tempfile::TempDir, FlowerClient) {
    let dir = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let node = Node::open(
        1,
        &dir.path().join("n1"),
        Arc::new(openraft::Config::default()),
        InProcessFactory::new(registry.clone()),
        Arc::new(Factory),
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
    let state = AppState::new(Arc::new(node)).with_admin_token("secret".to_string());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(serve(listener, state));
    let client = FlowerClient::builder(&base)
        .credentials(Credentials::token("secret"))
        .build()
        .unwrap();
    for _ in 0..500 {
        if client.status(&RequestOptions::default()).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    client
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
    (dir, client)
}

fn put(key: &str, title: &str) -> Put {
    Put {
        key: key.to_string(),
        title: title.to_string(),
    }
}

fn key(key: &str) -> Key {
    Key {
        key: key.to_string(),
    }
}

#[tokio::test]
async fn declared_failures_come_back_typed() {
    let (_dir, client) = serve_app().await;
    // A write's declared variant keeps its payload.
    let error = client
        .typed_mutate_declared::<_, (), TaskError>(
            "db",
            "app",
            "put",
            &put("a", ""),
            &MutationOptions::default(),
        )
        .await
        .unwrap_err();
    assert_eq!(
        error.declared(),
        Some(&TaskError::Invalid("title".to_string()))
    );
    assert!(matches!(
        error.source(),
        FlowerError::Business { code, details: Some(details), .. }
            if code == "INVALID" && details == "title"
    ));
    // An undeclared failure stays `Other`, untouched.
    let error = client
        .typed_mutate_declared::<_, (), TaskError>(
            "db",
            "app",
            "fatal",
            &put("a", "x"),
            &MutationOptions::default(),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        RemoteError::Other(ref other)
            if matches!(**other, FlowerError::Business { ref code, .. } if code == "SOMETHING_ELSE")
    ));
    // A query's payload-free variant comes back declared too.
    let target = RemoteTarget::new("app", "get").unwrap();
    let error = client
        .typed_query_declared_at::<_, Option<Task>, TaskError>(
            "db",
            &target,
            &key("missing"),
            &RequestOptions::default(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.declared(), Some(&TaskError::Missing));
    // So does a query's variant carrying a payload, with it.
    let error = client
        .typed_query_declared::<_, Option<Task>, TaskError>(
            "db",
            "app",
            "get",
            &key(""),
            &RequestOptions::default(),
        )
        .await
        .unwrap_err();
    assert_eq!(
        error.declared(),
        Some(&TaskError::Invalid("key".to_string()))
    );
    assert!(matches!(
        error.source(),
        FlowerError::Business { code, details: Some(details), .. }
            if code == "INVALID" && details == "key"
    ));
    // Successes decode as usual.
    client
        .typed_mutate_declared::<_, (), TaskError>(
            "db",
            "app",
            "put",
            &put("a", "first"),
            &MutationOptions::default(),
        )
        .await
        .unwrap();
    let found = client
        .typed_query_declared::<_, Option<Task>, TaskError>(
            "db",
            "app",
            "get",
            &key("a"),
            &RequestOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        found.value,
        Some(Task {
            title: "first".to_string()
        })
    );
}

/// The guest's own declarations, as a guest crate exports them: the
/// client takes each one's name, kind and types from it.
fn unused<'c, 'r>(_: &'c mut flower_sdk::Ctx<'c, 'r>, _: Value) -> flower_sdk::Result<Value> {
    unreachable!("the client never runs guest code")
}
static PUT: flower_sdk::Op<Put, (), TaskError> =
    flower_sdk::Op::new(flower_sdk::Method::mutation("put", unused));
static GET: flower_sdk::Op<Key, Option<Task>, TaskError> =
    flower_sdk::Op::new(flower_sdk::Method::query("get", unused));

#[tokio::test]
async fn calls_take_name_kind_and_types_from_the_declaration() {
    let (_dir, client) = serve_app().await;
    client
        .call("db", "app", &PUT, &put("a", "first"))
        .await
        .unwrap();
    assert_eq!(
        client.call("db", "app", &GET, &key("a")).await.unwrap(),
        Some(Task {
            title: "first".to_string()
        })
    );
    // Declared failures come back typed.
    let error = client
        .call("db", "app", &GET, &key("missing"))
        .await
        .unwrap_err();
    assert_eq!(error.declared(), Some(&TaskError::Missing));
}

#[tokio::test]
async fn typed_calls_take_options() {
    // A chosen request ID and deadline, through the typed descriptor.
    let mock = MockTransport::scripted([MockReply::ok(serde_json::Value::Null)]);
    let options = MutationOptions {
        request_id: Some("order-17".to_string()),
        base: RequestOptions {
            deadline: Some(std::time::Instant::now() + Duration::from_secs(30)),
            ..RequestOptions::default()
        },
        ..MutationOptions::default()
    };
    mock.client()
        .call_with("db", "app", &PUT, &put("a", "x"), &options)
        .await
        .unwrap();
    let sent = &mock.requests()[0];
    assert_eq!(sent.path, "/v1/mutate");
    assert_eq!(
        sent.json()["idempotency_key"],
        "order-17",
        "{}",
        sent.json()
    );
    assert!(sent.header("x-flower-timeout-ms").is_some());
}

/// A declared error with a catch-all for every other failure.
#[derive(DeclaredError, Clone, Debug, PartialEq)]
enum OrderError {
    #[flower(code = "OUT_OF_STOCK")]
    OutOfStock,
    #[flower(other)]
    Host(Failure),
}

#[test]
fn remote_errors_stay_small() {
    // Returned from every typed call: callers' `Result`s must not trip
    // clippy's `result_large_err` (128 bytes).
    assert!(
        std::mem::size_of::<RemoteError<OrderError>>() <= 32,
        "{}",
        std::mem::size_of::<RemoteError<OrderError>>()
    );
}

#[tokio::test]
async fn transport_and_auth_failures_are_never_declared() {
    // Failure accepts every code, yet auth refusals, unknown aliases
    // answered by the server (its real 404), other host failures and
    // exhausted retries stay `Other`: only a 422 is the call's own.
    let mock = MockTransport::scripted([
        MockReply::error(403, "forbidden", "no"),
        MockReply::error(400, "bad_request", "malformed"),
        MockReply::error(404, "UNKNOWN_METHOD", "no alias `get`"),
        MockReply::error(403, "UNAUTHENTICATED", "authentication required"),
        // A lowercase code is the host's (a call's own codes are
        // UPPER_SNAKE_CASE), so the server answers it 500.
        MockReply::error(500, "invalid", "not the call's own"),
        MockReply::overloaded(None),
    ]);
    let client = mock.client();
    for _ in 0..6 {
        let error = client
            .typed_query_declared::<_, (), Failure>(
                "db",
                "app",
                "get",
                &key("a"),
                &RequestOptions::default(),
            )
            .await
            .unwrap_err();
        assert!(matches!(error, RemoteError::Other(_)), "{error:?}");
    }
}

#[tokio::test]
async fn a_queue_being_purged_is_retried() {
    // As the server answers `queue_purging`: another attempt, once the
    // purge ends, succeeds.
    let purging = |_: ()| MockReply {
        status: flower_server::api::status_of("queue_purging"),
        retry_after_ms: Some(10),
        headers: Vec::new(),
        body: MockBody::Full(
            br#"{"ok":false,"code":"queue_purging","message":"being purged"}"#.to_vec(),
        ),
    };
    let mock = MockTransport::scripted([
        purging(()),
        purging(()),
        MockReply::ok(serde_json::json!(3)),
    ]);
    let value = mock
        .client()
        .query::<_, serde_json::Value>(
            "db",
            "app",
            "count",
            &serde_json::json!({}),
            &RequestOptions {
                retry: Some(flower_client::retry::Retry::Policy(
                    flower_client::retry::RetryPolicy {
                        initial_delay: Duration::from_millis(5),
                        ..flower_client::retry::RetryPolicy::default()
                    },
                )),
                ..RequestOptions::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(value.value, serde_json::json!(3));
}

#[tokio::test]
async fn a_422_without_an_envelope_is_the_servers() {
    // What a proxy or a framework rejection answers: nothing to decode,
    // so a server failure, not a decode error.
    let mock = MockTransport::scripted([MockReply {
        status: 422,
        retry_after_ms: None,
        headers: Vec::new(),
        body: MockBody::Full(b"Failed to deserialize the JSON body".to_vec()),
    }]);
    let error = mock
        .client()
        .query::<_, serde_json::Value>(
            "db",
            "app",
            "get",
            &serde_json::json!({"key": "a"}),
            &RequestOptions::default(),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, FlowerError::Http(_)), "{error:?}");
}

#[tokio::test]
async fn typed_watches_and_subscriptions_decode_updates() {
    let (_dir, client) = serve_app().await;
    let mut watch = client
        .typed_watch::<_, Option<Task>>("db", "app", "get", &key("w"), &WatchOptions::default())
        .await
        .unwrap();
    assert_eq!(watch.next().await.unwrap().unwrap().value, None);
    let mut subscription = client
        .typed_subscribe::<_, Option<Task>>(
            "db",
            "app",
            "get",
            &key("w"),
            &SubscribeOptions::default(),
        )
        .unwrap();
    let first = subscription.next().await.unwrap().unwrap();
    assert!(first.reset);
    assert_eq!(first.value, None);
    client
        .typed_mutate::<_, ()>(
            "db",
            "app",
            "put",
            &put("w", "watched"),
            &MutationOptions::default(),
        )
        .await
        .unwrap();
    let watched = Some(Task {
        title: "watched".to_string(),
    });
    assert_eq!(watch.next().await.unwrap().unwrap().value, watched);
    let update = subscription.next().await.unwrap().unwrap();
    assert!(!update.reset);
    assert_eq!(update.value, watched);
    assert!(update.revision > first.revision);
}

#[tokio::test]
async fn a_value_that_does_not_decode_ends_the_typed_watch() {
    let (reply, stream) = MockReply::event_stream(
        "event: snapshot\ndata: {\"revision\":1,\"value\":{\"title\":\"a\"}}\n\n\
         event: snapshot\ndata: {\"revision\":2,\"value\":7}\n\n",
    );
    let mock = MockTransport::scripted([reply]);
    let client = mock.client();
    let mut watch = client
        .typed_watch::<_, Task>("db", "app", "get", &key("a"), &WatchOptions::default())
        .await
        .unwrap();
    assert_eq!(watch.next().await.unwrap().unwrap().value.title, "a");
    assert!(matches!(
        watch.next().await,
        Some(Err(FlowerError::Decode(_)))
    ));
    assert!(watch.next().await.is_none());
    // The connection was dropped with the watch's inner stream.
    assert!(!stream.open());
    assert_eq!(mock.requests()[0].method, "STREAM");
}

#[tokio::test]
async fn the_mock_transport_records_requests_and_hangs_when_done() {
    let mock = MockTransport::scripted([
        MockReply::ok(serde_json::json!(3)).with_header("x-flower-staleness-ms", "12"),
        MockReply::failure("INVALID", "bad", Some(serde_json::json!("title"))),
    ]);
    let client = FlowerClient::builder("http://mock")
        .transport(mock.clone())
        .credentials(Credentials::token("t0k"))
        .build()
        .unwrap();
    let value = client
        .query_value(
            "db",
            "app",
            "count",
            &serde_json::json!({"n": 1}),
            &RequestOptions::new(),
        )
        .await
        .unwrap();
    assert_eq!(value.value, 3);
    let error = client
        .mutate_value(
            "db",
            "app",
            "put",
            &serde_json::json!({}),
            &MutationOptions::new(),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        FlowerError::Business {
            details: Some(_),
            ..
        }
    ));
    // Past the script, requests hang like an unresponsive server.
    let options = RequestOptions::new().timeout(Duration::from_millis(50));
    let error = client
        .query_value("db", "app", "count", &serde_json::json!({}), &options)
        .await
        .unwrap_err();
    assert!(matches!(error, FlowerError::Timeout));
    let requests = mock.requests();
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[0].method, "POST");
    assert_eq!(requests[0].path, "/v1/query");
    assert_eq!(requests[0].json()["args"]["n"], 1);
    assert_eq!(requests[0].authorization.as_deref(), Some("Bearer t0k"));
    assert!(requests[0].header("x-flower-timeout-ms").is_some());
    assert_eq!(requests[1].path, "/v1/mutate");
    // A handler answers from the request.
    let mock =
        MockTransport::with_handler(|request| MockReply::ok(serde_json::json!(request.path.len())));
    let value = mock
        .client()
        .query_value(
            "db",
            "app",
            "x",
            &serde_json::json!({}),
            &RequestOptions::new(),
        )
        .await
        .unwrap();
    assert_eq!(value.value, "/v1/query".len());
}
