//! Operator calls through the client against a live server: module
//! listings (maintenance tasks included), a staged deployment driven
//! step by step, the managed-key catalog, retention status, Raft and
//! node metrics, and backup status.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use flower_client::admin::{KeyOperation, TaskInfo};
use flower_client::client::{FlowerClient, MutationOptions, RequestOptions};
use flower_client::error::FlowerError;
use flower_client::transport::Credentials;
use flower_cluster::network::{InProcessFactory, Registry};
use flower_cluster::node::Node;
use flower_cluster::statemachine::RuntimeFactory;
use flower_crypto::replicated::{ManagedKeys, WrappingKey};
use flower_engine::runtime::{CallKind, ClosureRuntime, HostCall};
use flower_sdk::abi::{HostOp, decode_envelope};
use flower_sdk::{Failure, Value, fail};
use flower_server::api::{AppState, serve};

/// Stores `{key, title}` under `tasks`.
fn put(host: &mut dyn HostCall, args: &Value) -> Result<Value, Failure> {
    let Value::Object(map) = args else {
        return Err(fail("TYPE_MISMATCH", "args take an object"));
    };
    let (Some(key), Some(title)) = (map.get("key"), map.get("title")) else {
        return Err(fail("TYPE_MISMATCH", "args need a key and a title"));
    };
    let mut row = std::collections::BTreeMap::new();
    row.insert("title".to_string(), title.clone());
    let input = Value::Array(vec![
        Value::String(String::new()),
        Value::String("tasks".to_string()),
        key.clone(),
        Value::Object(row),
    ])
    .encode_binary()
    .unwrap();
    let output = host.op(HostOp::Set as u32 as u8, &input);
    decode_envelope(Value::decode_binary(&output).unwrap())?;
    Ok(Value::Null)
}

struct TestFactory;

impl RuntimeFactory for TestFactory {
    fn build(
        &self,
        _db: &[u8],
        _module: &str,
        _wasm: &[u8],
        _manifest: &Value,
    ) -> Result<Box<dyn flower_engine::runtime::Runtime>, String> {
        Ok(Box::new(ClosureRuntime::new(
            |kind: CallKind, _module: &str, name: &str, args: &Value, host: &mut dyn HostCall| {
                match (kind, name) {
                    (CallKind::Mutation, "put") => put(host, args),
                    _ => Err(fail("UNKNOWN_METHOD", "unknown definition")),
                }
            },
        )))
    }
}

/// The test module, with one more index when `indexed`.
fn manifest(indexed: bool) -> serde_json::Value {
    let mut indexes = vec![serde_json::json!({"name": "by_title", "fields": ["title"]})];
    if indexed {
        indexes.push(serde_json::json!({"name": "by_title_again", "fields": ["title"]}));
    }
    serde_json::json!({
        "collections": [{
            "name": "tasks",
            "key": {"type": "string"},
            "value": {"type": "object", "fields": [
                {"name": "title", "schema": {"type": "string"}, "optional": false}
            ]},
            "indexes": indexes,
            "references": []
        }],
        "definitions": [{"name": "put", "kind": "mutation", "args": {"type": "json"}}],
        "triggers": [],
        "http": [{"alias": "put", "definition": "put"}],
        "tasks": [{"name": "nightly", "on_error": true}]
    })
}

/// Serve an initialized single node with a mounted wrapping key.
async fn serve_node() -> (tempfile::TempDir, String) {
    let (dir, base, _) = serve_with(false).await;
    (dir, base)
}

/// Serve an initialized single node with a mounted wrapping key, running
/// its maintenance every 20 ms when `maintained` (until the returned
/// sender drops).
async fn serve_with(
    maintained: bool,
) -> (tempfile::TempDir, String, tokio::sync::watch::Sender<bool>) {
    let dir = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let node = Node::open(
        1,
        &dir.path().join("n1"),
        Arc::new(openraft::Config::default()),
        InProcessFactory::new(registry.clone()),
        Arc::new(TestFactory),
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
    let node = Arc::new(node.with_key_catalog(Arc::new(ManagedKeys::new(
        Some(WrappingKey::new(&[7; 32])),
        Vec::new(),
    ))));
    let (stop, stopped) = tokio::sync::watch::channel(false);
    if maintained {
        let node = node.clone();
        tokio::spawn(async move {
            node.run_maintenance(Duration::from_millis(20), stopped)
                .await
        });
    }
    let state = AppState::new(node).with_admin_token("secret".to_string());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(serve(listener, state));
    (dir, base, stop)
}

fn admin(base: &str) -> FlowerClient {
    FlowerClient::builder(base)
        .credentials(Credentials::Token("secret".to_string()))
        .build()
        .unwrap()
}

#[tokio::test]
async fn operators_list_modules_and_drive_a_staged_deployment() {
    let (_dir, base) = serve_node().await;
    let client = admin(&base);
    let options = RequestOptions::default();
    let first = client
        .deploy("db", "app", None, b"wasm", &manifest(false), &options)
        .await
        .unwrap();
    assert!(first.position > 0);
    for key in ["a", "b", "c"] {
        client
            .mutate_value(
                "db",
                "app",
                "put",
                &serde_json::json!({"key": format!("\"{key}\""), "title": key}),
                &MutationOptions::default(),
            )
            .await
            .unwrap();
    }

    let listed = client.modules(None, &options).await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].db, "db");
    let module = &listed[0].modules[0];
    assert_eq!(module.name, "app");
    assert_eq!(module.definitions["put"], "mutation");
    assert_eq!(module.collections, ["tasks"]);
    assert_eq!(module.http["put"], "put");
    assert!(!module.authorize);
    // Its maintenance task, healthy: no failure state.
    assert_eq!(
        module.tasks,
        [TaskInfo {
            name: "nightly".to_string(),
            on_error: true,
            failures: 0,
            retry_at_ms: None,
            error: None,
        }]
    );
    let deployed = module.identity.clone().unwrap();
    assert_eq!(deployed.len(), 64);
    assert!(
        client.modules(Some("other"), &options).await.unwrap()[0]
            .modules
            .is_empty()
    );

    // Stage a target with a new index: its backfill covers the rows.
    let staged = client
        .stage_deployment("db", "app", None, b"wasm", &manifest(true), &options)
        .await
        .unwrap();
    let staging = staged.staging.unwrap();
    assert!(!staging.ready && staging.planned >= 1, "{staged:?}");
    let target = staged.identity.clone().unwrap();
    assert_ne!(target, deployed);
    // Activation refuses until the backfill is built.
    match client.activate_deployment("db", "app", &options).await {
        Err(FlowerError::Http(problem)) => assert_eq!(problem.code, "deployment"),
        other => panic!("activated an unbuilt staging: {other:?}"),
    }
    // One record per step, until ready.
    let mut steps = 0;
    loop {
        let advanced = client
            .advance_deployment("db", "app", Some(1), &options)
            .await
            .unwrap();
        steps += 1;
        if advanced.staging.unwrap().ready {
            break;
        }
        assert!(steps < 20, "the backfill never finished");
    }
    assert!(steps >= 3, "three rows, one per step: {steps}");
    let status = client
        .deployment_status("db", "app", &options)
        .await
        .unwrap();
    assert!(status.staging.unwrap().ready);
    assert_eq!(status.identity.as_deref(), Some(target.as_str()));
    // The old code still serves until activation.
    let listed = client.modules(Some("db"), &options).await.unwrap();
    assert_eq!(
        listed[0].modules[0].identity.as_deref(),
        Some(deployed.as_str())
    );

    let activated = client
        .activate_deployment("db", "app", &options)
        .await
        .unwrap();
    assert!(activated.position > first.position);
    let listed = client.modules(Some("db"), &options).await.unwrap();
    assert_eq!(
        listed[0].modules[0].identity.as_deref(),
        Some(target.as_str())
    );
    // Nothing is staged now: steps refuse, and cancel reports none.
    assert!(
        !client
            .cancel_deployment("db", "app", &options)
            .await
            .unwrap()
    );
    match client.advance_deployment("db", "app", None, &options).await {
        Err(FlowerError::Http(problem)) => assert_eq!(problem.code, "deployment"),
        other => panic!("advanced nothing: {other:?}"),
    }

    // A staging can also be abandoned.
    client
        .stage_deployment("db", "app", None, b"other", &manifest(false), &options)
        .await
        .unwrap();
    assert!(
        client
            .cancel_deployment("db", "app", &options)
            .await
            .unwrap()
    );
    let status = client
        .deployment_status("db", "app", &options)
        .await
        .unwrap();
    assert!(status.identity.is_none());

    // Deploying without the extra index retires it; collection deletes
    // its entries on the member, a bounded step at a time, and then the
    // staging row goes.
    client
        .deploy("db", "app", None, b"wasm", &manifest(false), &options)
        .await
        .unwrap();
    let staging = client.modules(Some("db"), &options).await.unwrap()[0].modules[0]
        .staging
        .expect("the removed index retired");
    assert!(staging.retired >= 1, "{staging:?}");
    let mut collected = 0;
    for step in 0.. {
        let (keys, done) = client
            .collect_deployment("db", "app", Some(1), &options)
            .await
            .unwrap();
        assert!(keys <= 1);
        collected += keys;
        if done {
            break;
        }
        assert!(step < 50, "collection never finished");
    }
    assert!(collected >= 3, "three rows' index entries: {collected}");
    assert!(
        client.modules(Some("db"), &options).await.unwrap()[0].modules[0]
            .staging
            .is_none()
    );
}

/// The test module exporting `tasks`.
fn exporting_manifest() -> serde_json::Value {
    let mut manifest = manifest(false);
    manifest["exports"] = serde_json::json!({
        "collections": ["tasks"],
        "derived": [],
        "methods": [],
    });
    manifest
}

/// A module importing `app`'s `tasks`.
fn importer_manifest() -> serde_json::Value {
    serde_json::json!({
        "collections": [],
        "definitions": [{"name": "put", "kind": "mutation", "args": {"type": "json"}}],
        "triggers": [],
        "http": [],
        "imports": [{
            "module": "app",
            "collections": [{
                "name": "tasks",
                "key": {"type": "string"},
                "row": {"type": "json"},
            }],
            "derived": [],
            "methods": [],
        }],
    })
}

/// A raw admin request's HTTP status and body.
async fn raw_post(base: &str, path: &str, body: serde_json::Value) -> (u16, serde_json::Value) {
    let response = reqwest::Client::new()
        .post(format!("{base}{path}"))
        .bearer_auth("secret")
        .header("content-type", "application/json")
        .body(serde_json::to_vec(&body).unwrap())
        .send()
        .await
        .unwrap();
    let status = response.status().as_u16();
    let bytes = response.bytes().await.unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}

#[tokio::test]
async fn operators_remove_modules_and_purge_their_data() {
    let (_dir, base, _maintenance) = serve_with(true).await;
    let client = admin(&base);
    let options = RequestOptions::default();
    client
        .deploy("db", "app", None, b"wasm", &exporting_manifest(), &options)
        .await
        .unwrap();
    client
        .deploy(
            "db",
            "importer",
            None,
            b"wasm",
            &importer_manifest(),
            &options,
        )
        .await
        .unwrap();
    for key in ["a", "b", "c"] {
        client
            .mutate_value(
                "db",
                "app",
                "put",
                &serde_json::json!({"key": format!("\"{key}\""), "title": key}),
                &MutationOptions::default(),
            )
            .await
            .unwrap();
    }

    // A module another imports from is refused, 409, and stays.
    match client.remove_module("db", "app", &options).await {
        Err(FlowerError::Http(problem)) => {
            let (code, message) = (problem.code, problem.message);
            assert_eq!(code, "MODULE_CONFLICT");
            assert!(message.contains("`importer`"), "{message}");
        }
        other => panic!("removed an imported module: {other:?}"),
    }
    let (status, body) = raw_post(
        &base,
        "/v1/admin/modules",
        serde_json::json!({"operation": "remove", "db": "db", "module": "app", "purge": true}),
    )
    .await;
    assert_eq!(
        (status, body["code"].as_str()),
        (409, Some("MODULE_CONFLICT"))
    );
    match client.remove_module("db", "ghost", &options).await {
        Err(FlowerError::Http(problem)) => assert_eq!(problem.code, "MODULE_CONFLICT"),
        other => panic!("removed a module never deployed: {other:?}"),
    }

    // The importer goes; its declarations stay, listed as removed.
    let removed = client
        .remove_module("db", "importer", &options)
        .await
        .unwrap();
    assert!(!removed.purging && removed.position > 0, "{removed:?}");
    let listed = &client.modules(Some("db"), &options).await.unwrap()[0];
    let names: Vec<&str> = listed.modules.iter().map(|m| m.name.as_str()).collect();
    assert_eq!(names, ["app"]);
    assert_eq!(listed.removed.len(), 1);
    assert_eq!(listed.removed[0].name, "importer");
    assert_eq!(listed.removed[0].identity.len(), 64);
    assert!(listed.purging.is_empty());

    // Now the app can go, with its data: maintenance purges it while
    // its deployment waits, and the listing shows how far it went.
    let purged = client.purge_module("db", "app", &options).await.unwrap();
    assert!(purged.purging && purged.position > removed.position);
    match client
        .deploy("db", "app", None, b"wasm", &exporting_manifest(), &options)
        .await
    {
        // Purged already, or still purging: either way the listing ends
        // without it below.
        Ok(_) => {}
        Err(FlowerError::Http(problem)) => {
            let (code, message) = (problem.code, problem.message);
            assert_eq!(code, "MODULE_CONFLICT");
            assert!(message.contains("still being purged"), "{message}");
        }
        Err(other) => panic!("deploy failed otherwise: {other:?}"),
    }
    let mut purging = true;
    for _ in 0..250 {
        let listed = &client.modules(Some("db"), &options).await.unwrap()[0];
        purging = !listed.purging.is_empty();
        if !purging {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(!purging, "the purge never finished");
    client
        .deploy("db", "app", None, b"wasm", &exporting_manifest(), &options)
        .await
        .unwrap();
    // Deployed again, it starts empty: what it wrote went with the
    // purge, so the same key writes afresh (the purge left no index
    // entry behind to collide with).
    client
        .mutate_value(
            "db",
            "app",
            "put",
            &serde_json::json!({"key": "\"a\"", "title": "again"}),
            &MutationOptions::default(),
        )
        .await
        .unwrap();

    // The list operation answers as `GET` does; removal is
    // operator-only.
    let (status, body) = raw_post(
        &base,
        "/v1/admin/modules",
        serde_json::json!({"operation": "list", "db": "db"}),
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(body["databases"][0]["removed"][0]["name"], "importer");
    let anonymous = FlowerClient::builder(&base).build().unwrap();
    match anonymous.remove_module("db", "app", &options).await {
        Err(FlowerError::Http(problem))
            if problem.origin == flower_client::Origin::Authorization => {}
        other => panic!("removed without credentials: {other:?}"),
    }
}

#[tokio::test]
async fn operators_read_keys_retention_metrics_and_backup_status() {
    let (_dir, base) = serve_node().await;
    let client = admin(&base);
    let options = RequestOptions::default();
    client
        .deploy("db", "app", None, b"wasm", &manifest(false), &options)
        .await
        .unwrap();

    let retention = client.retention_status("db", &options).await.unwrap();
    assert!(
        retention["receipts"]["expiry_clock_ms"].as_u64().unwrap() > 0,
        "{retention}"
    );
    assert_eq!(retention["receipts"]["receipt_ttl_ms"], 86_400_000);
    assert_eq!(retention["initialized"], false);
    assert!(retention["stats"]["collections"].as_u64().is_some());

    let raft = client.raft_metrics(&options).await.unwrap();
    assert_eq!(raft["id"], 1);
    assert_eq!(raft["current_leader"], 1);
    assert!(
        raft["last_applied"]["index"].as_u64().unwrap() >= 1,
        "{raft}"
    );

    let activity = client.activity(&options).await.unwrap();
    assert_eq!(activity["version"], 1);
    // Deployments move no revision: not data writes.
    assert_eq!(activity["writes"]["total"], 0, "{activity}");
    let metrics = client.node_metrics(&options).await.unwrap();
    assert_eq!(metrics["node"]["id"], 1);
    assert!(metrics["admission"]["admitted"].as_u64().is_some());

    let backup = client.backup_status(&options).await.unwrap();
    assert_eq!(backup["enabled"], false);

    // Every operator call needs the admin credential.
    let anonymous = FlowerClient::builder(&base).build().unwrap();
    for refused in [
        anonymous.keys("db", &options).await.map(drop),
        anonymous.key_cache(&options).await.map(drop),
        anonymous.retention_status("db", &options).await.map(drop),
        anonymous.raft_metrics(&options).await.map(drop),
        anonymous.modules(None, &options).await.map(drop),
        anonymous
            .deployment_status("db", "app", &options)
            .await
            .map(drop),
    ] {
        match refused {
            Err(FlowerError::Http(problem))
                if problem.origin == flower_client::Origin::Authorization => {}
            other => panic!("served without credentials: {other:?}"),
        }
    }
}

#[tokio::test]
async fn operators_run_the_managed_key_lifecycle_through_the_catalog() {
    let (_dir, base) = serve_node().await;
    let client = admin(&base);
    let options = RequestOptions::default();
    let listed = client.keys("db", &options).await.unwrap();
    assert!(listed.catalog.keys.is_empty(), "{listed:?}");

    let generate = KeyOperation::Generate {
        name: "signer".to_string(),
        algorithm: "Ed25519".to_string(),
        bits: None,
    };
    let generated = client
        .change_keys("db", &generate, "g1", &options)
        .await
        .unwrap();
    assert!(!generated.duplicate);
    let key = &generated.catalog.keys["signer"];
    assert_eq!(key.algorithm, "Ed25519");
    assert_eq!(key.active_version, 1);
    assert_eq!(
        key.versions[0].wrapping_id.as_deref(),
        Some(WrappingKey::new(&[7; 32]).id())
    );
    // The same request ID answers the committed change again.
    let again = client
        .change_keys("db", &generate, "g1", &options)
        .await
        .unwrap();
    assert!(again.duplicate);
    assert_eq!(again.revision, generated.revision);

    let bind = KeyOperation::Bind {
        alias: "tokens".to_string(),
        key: "signer".to_string(),
        usages: vec!["sign".to_string(), "verify".to_string()],
    };
    let bound = client
        .change_keys("db", &bind, "b1", &options)
        .await
        .unwrap();
    assert_eq!(bound.catalog.bindings["tokens"].key, "signer");
    assert_eq!(bound.catalog.bindings["tokens"].usages, ["sign", "verify"]);
    // A usage the algorithm lacks is the caller's mistake.
    let wrong = KeyOperation::Bind {
        alias: "sealing".to_string(),
        key: "signer".to_string(),
        usages: vec!["encrypt".to_string()],
    };
    assert!(
        client
            .change_keys("db", &wrong, "b2", &options)
            .await
            .is_err()
    );

    let rotated = client
        .change_keys(
            "db",
            &KeyOperation::Rotate {
                name: "signer".to_string(),
                bits: None,
            },
            "r1",
            &options,
        )
        .await
        .unwrap();
    assert_eq!(rotated.catalog.keys["signer"].active_version, 2);
    let retired = client
        .change_keys(
            "db",
            &KeyOperation::Retire {
                name: "signer".to_string(),
                version: Some(1),
            },
            "t1",
            &options,
        )
        .await
        .unwrap();
    let versions = &retired.catalog.keys["signer"].versions;
    assert!(versions[0].retired && !versions[1].retired, "{versions:?}");
    let destroyed = client
        .change_keys(
            "db",
            &KeyOperation::Destroy {
                name: "signer".to_string(),
                version: Some(1),
            },
            "d1",
            &options,
        )
        .await
        .unwrap();
    assert!(destroyed.catalog.keys["signer"].versions[0].destroyed);
    let unbound = client
        .change_keys(
            "db",
            &KeyOperation::Unbind {
                alias: "tokens".to_string(),
            },
            "u1",
            &options,
        )
        .await
        .unwrap();
    assert!(unbound.catalog.bindings.is_empty());
    let rewrapped = client
        .change_keys("db", &KeyOperation::Rewrap { name: None }, "w1", &options)
        .await
        .unwrap();
    assert!(rewrapped.revision > unbound.revision);

    // The listing reads the catalog every member applies, alone or
    // beside every other database's.
    let listed = client.keys("db", &options).await.unwrap();
    assert_eq!(listed.catalog, rewrapped.catalog);
    let all = client.key_catalogs(&options).await.unwrap();
    assert_eq!(all.len(), 1, "{all:?}");
    assert_eq!(all["db"], rewrapped.catalog);
    // Another database's catalog is its own: the same request ID there
    // generates another key.
    let other = client
        .change_keys("other", &generate, "g1", &options)
        .await
        .unwrap();
    assert!(!other.duplicate);
    assert_eq!(other.revision, 1);
    assert_ne!(
        other.catalog.keys["signer"].id,
        rewrapped.catalog.keys["signer"].id
    );
    assert_eq!(client.key_catalogs(&options).await.unwrap().len(), 2);
    assert!(client.key_cache(&options).await.unwrap().is_object());
}

/// A module providing `people`, exported when `exported`.
fn provider(exported: bool) -> serde_json::Value {
    let exports: Vec<&str> = if exported { vec!["people"] } else { vec![] };
    serde_json::json!({
        "collections": [{
            "name": "people",
            "key": {"type": "string"},
            "value": {"type": "json"},
            "indexes": [],
            "references": []
        }],
        "definitions": [],
        "triggers": [],
        "http": [],
        "exports": {"collections": exports, "derived": [], "methods": []}
    })
}

/// A module reading `provider`'s `people`.
fn importer() -> serde_json::Value {
    serde_json::json!({
        "collections": [],
        "definitions": [],
        "triggers": [],
        "http": [],
        "imports": [{
            "module": "provider",
            "collections": [{"name": "people", "key": {"type": "string"}, "row": {"type": "json"}}],
            "derived": [],
            "methods": []
        }]
    })
}

#[tokio::test]
async fn staged_steps_check_links_as_a_deploy_does() {
    let (_dir, base) = serve_node().await;
    let client = admin(&base);
    let options = RequestOptions::default();
    let refused = |error: FlowerError, expected: &str| {
        let text = format!("{error:?}");
        assert!(text.contains(expected), "{text}");
    };
    // Nothing to import from yet.
    refused(
        client
            .stage_deployment("db", "importer", None, b"wasm", &importer(), &options)
            .await
            .unwrap_err(),
        "not deployed",
    );
    // `$` starts a slot, never a module.
    refused(
        client
            .stage_deployment("db", "$importer", None, b"wasm", &importer(), &options)
            .await
            .unwrap_err(),
        "invalid module identity",
    );
    client
        .deploy("db", "provider", None, b"wasm", &provider(true), &options)
        .await
        .unwrap();
    let staged = client
        .stage_deployment("db", "importer", None, b"wasm", &importer(), &options)
        .await
        .unwrap();
    assert!(staged.staging.unwrap().ready);
    // The provider stops exporting while the importer is staged: the
    // activation checks the links again and refuses.
    client
        .deploy("db", "provider", None, b"wasm2", &provider(false), &options)
        .await
        .unwrap();
    refused(
        client
            .activate_deployment("db", "importer", &options)
            .await
            .unwrap_err(),
        "does not export collection `people`",
    );
    assert!(
        client
            .cancel_deployment("db", "importer", &options)
            .await
            .unwrap()
    );
}
