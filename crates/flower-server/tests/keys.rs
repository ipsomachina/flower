//! The operator key API: `POST /v1/admin/keys` changes and reads the
//! replicated managed-key catalog behind the admin credential.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use flower_cluster::network::{InProcessFactory, Registry};
use flower_cluster::node::Node;
use flower_cluster::statemachine::RuntimeFactory;
use flower_crypto::replicated::{ManagedKeys, WrappingKey};
use flower_engine::runtime::ClosureRuntime;
use flower_sdk::{Value, fail};
use flower_server::api::{AppState, serve};
use serde_json::json;

struct NoGuests;

impl RuntimeFactory for NoGuests {
    fn build(
        &self,
        _db: &[u8],
        _module: &str,
        _wasm: &[u8],
        _manifest: &Value,
    ) -> Result<Box<dyn flower_engine::runtime::Runtime>, String> {
        Ok(Box::new(ClosureRuntime::new(|_, _, _, _, _| {
            Err(fail("UNKNOWN_METHOD", "no guests"))
        })))
    }
}

#[tokio::test]
async fn a_member_without_the_wrapping_key_answers_keys_locked() {
    let dir = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let node = Node::open(
        1,
        &dir.path().join("n1"),
        Arc::new(openraft::Config::default()),
        InProcessFactory::new(registry.clone()),
        Arc::new(NoGuests),
    )
    .await
    .unwrap()
    .with_key_catalog(Arc::new(ManagedKeys::new(None, Vec::new())));
    registry.register(1, node.raft().clone());
    node.initialize(BTreeSet::from([1])).await.unwrap();
    for _ in 0..500 {
        if node.status().leader == Some(1) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let node = Arc::new(node);
    let state = AppState::new(node.clone()).with_admin_token("secret".to_string());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/v1/admin/keys", listener.local_addr().unwrap());
    tokio::spawn(serve(listener, state));
    let answer = reqwest::Client::new()
        .post(&url)
        .header("authorization", "Bearer secret")
        .json(&json!({"db":"db","operation":"generate","requestId":"g","name":"signing","algorithm":"P256"}))
        .send()
        .await
        .unwrap();
    // Not the request's fault: its own code and a status to retry on.
    assert_eq!(answer.status(), 503);
    let body: serde_json::Value = answer.json().await.unwrap();
    assert_eq!(body["code"], "KEYS_LOCKED", "{body}");
}

#[tokio::test]
async fn operators_manage_the_key_catalog_over_http() {
    let dir = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let node = Node::open(
        1,
        &dir.path().join("n1"),
        Arc::new(openraft::Config::default()),
        InProcessFactory::new(registry.clone()),
        Arc::new(NoGuests),
    )
    .await
    .unwrap()
    .with_key_catalog(Arc::new(ManagedKeys::new(
        Some(WrappingKey::new(&[8; 32])),
        Vec::new(),
    )));
    registry.register(1, node.raft().clone());
    node.initialize(BTreeSet::from([1])).await.unwrap();
    for _ in 0..500 {
        if node.status().leader == Some(1) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let node = Arc::new(node);
    let state = AppState::new(node.clone()).with_admin_token("secret".to_string());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/v1/admin/keys", listener.local_addr().unwrap());
    tokio::spawn(serve(listener, state));
    let http = reqwest::Client::new();
    let post = |body: serde_json::Value, token: &str| {
        http.post(&url)
            .header("authorization", format!("Bearer {token}"))
            .json(&body)
            .send()
    };

    let generate = json!({"db":"db","operation":"generate","requestId":"g","name":"signing","algorithm":"P256"});
    // The catalog is an operator surface.
    let denied = post(generate.clone(), "wrong").await.unwrap();
    assert_eq!(denied.status(), 403);

    let created = post(generate.clone(), "secret").await.unwrap();
    assert_eq!(created.status(), 200);
    let created: serde_json::Value = created.json().await.unwrap();
    assert_eq!(created["revision"], 1);
    assert_eq!(created["value"]["keys"]["signing"]["algorithm"], "P256");
    // No envelope or material leaves the server.
    assert!(!created.to_string().contains("ciphertext"));

    // Retries answer from the receipt.
    let retried: serde_json::Value = post(generate, "secret")
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(retried["duplicate"], true);

    // Another database's catalog is its own: the same request there
    // creates its first key, not a duplicate.
    let other: serde_json::Value = post(
        json!({"db":"other","operation":"generate","requestId":"g","name":"signing","algorithm":"P256"}),
        "secret",
    )
    .await
    .unwrap()
    .json()
    .await
    .unwrap();
    assert_eq!(other["duplicate"], false, "{other}");
    assert_eq!(other["revision"], 1);
    // A change names its database.
    let unnamed = post(
        json!({"operation":"generate","requestId":"u","name":"signing","algorithm":"P256"}),
        "secret",
    )
    .await
    .unwrap();
    assert_eq!(unnamed.status(), 400);
    // ...a database's name: never empty, reserved (`$cluster` is the
    // group's own) or with control characters.
    for db in ["", "$cluster", "a\u{1}b"] {
        let invalid = post(
            json!({"db":db,"operation":"generate","requestId":"i","name":"signing","algorithm":"P256"}),
            "secret",
        )
        .await
        .unwrap();
        assert_eq!(invalid.status(), 400, "{db:?}");
        let body: serde_json::Value = invalid.json().await.unwrap();
        assert_eq!(body["code"], "KEY_REQUEST", "{body}");
    }
    let reserved = http
        .get(format!("{url}?db=%24cluster"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap();
    assert_eq!(reserved.status(), 400);

    // Refused requests are client errors.
    let refused = post(
        json!({"db":"db","operation":"bind","requestId":"b","name":"s","key":"signing","usages":["encrypt"]}),
        "secret",
    )
    .await
    .unwrap();
    assert_eq!(refused.status(), 400);
    let body: serde_json::Value = refused.json().await.unwrap();
    // The refusal's own code, not `bad_request` with it in the message.
    assert_eq!(body["code"], "KEY_REQUEST", "{body}");

    let listed: serde_json::Value = post(json!({"db":"db","operation":"list"}), "secret")
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(listed["value"]["keys"]["signing"]["activeVersion"], 1);
    // Without a database, every database's catalog.
    let every: serde_json::Value = http
        .get(&url)
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(every["databases"]["db"]["revision"], 1, "{every}");
    assert_eq!(every["databases"]["other"]["revision"], 1, "{every}");
    assert_eq!(
        every["databases"].as_object().unwrap().len(),
        2,
        "no invalid name holds a catalog: {every}"
    );
    let one: serde_json::Value = http
        .get(format!("{url}?db=other"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        one["value"]["keys"]["signing"]["algorithm"], "P256",
        "{one}"
    );
    let cache: serde_json::Value = post(json!({"operation":"cache"}), "secret")
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(cache["budgetBytes"].as_u64().unwrap() > 0, "{cache}");
}
