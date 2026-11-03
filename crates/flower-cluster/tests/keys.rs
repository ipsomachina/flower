//! The managed-key catalogs replicate through the Raft log: every
//! member applies the leader's prepared changes, serves the same keys,
//! and gets the catalogs back from its own storage on restart and from
//! a snapshot when it fell behind a purged log. Every database keeps a
//! catalog of its own: databases sharing a group never share a key,
//! a binding or a request receipt.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use flower_cluster::keys::KEY_CATALOG;
use flower_cluster::network::{InProcessFactory, Registry};
use flower_cluster::node::{KEY_REQUEST, Node, NodeError};
use flower_cluster::statemachine::{RuntimeFactory, StateMachine};
use flower_cluster::types::ClusterWrite;
use flower_crypto::replicated::{Declaration, ManagedKeys, ManagedOutput, Prepared, WrappingKey};
use flower_engine::runtime::ClosureRuntime;
use flower_sdk::{Value, fail};
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

/// The database most tests provision keys in.
const DB: &[u8] = b"app";

fn keys() -> Arc<ManagedKeys> {
    Arc::new(ManagedKeys::new(
        Some(WrappingKey::new(&[4; 32])),
        Vec::new(),
    ))
}

async fn open(id: u64, dir: &std::path::Path, registry: &Registry) -> Node {
    let node = Node::open(
        id,
        &dir.join(format!("n{id}")),
        Arc::new(openraft::Config::default()),
        InProcessFactory::new(registry.clone()),
        Arc::new(NoGuests),
    )
    .await
    .unwrap()
    .with_key_catalog(keys());
    registry.register(id, node.raft().clone());
    node
}

async fn eventually(what: &str, mut check: impl FnMut() -> bool) {
    for _ in 0..400 {
        if check() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for {what}");
}

fn declaration() -> Declaration {
    Declaration {
        name: "sessions".to_string(),
        algorithm: flower_crypto::catalog::Algorithm::Ed25519,
        usages: ["sign", "verify", "publicKey"]
            .into_iter()
            .map(str::to_string)
            .collect(),
    }
}

/// Run a managed operation through a member's runtime, as a guest of
/// database `db` on it would.
fn run(
    node: &Node,
    db: &[u8],
    operation: &str,
    args: &[&[u8]],
    now_ms: u64,
) -> Result<ManagedOutput, String> {
    node.key_catalog()
        .execute(
            db,
            &declaration(),
            operation,
            None,
            args,
            &json!({}),
            now_ms,
        )
        .map_err(|error| format!("{error:#}"))
}

/// Sign through a member's runtime, as a guest of [`DB`] on it would.
fn sign(node: &Node, message: &[u8]) -> Result<Vec<u8>, String> {
    sign_in(node, DB, message)
}

/// [`sign`] for a guest of database `db`.
fn sign_in(node: &Node, db: &[u8], message: &[u8]) -> Result<Vec<u8>, String> {
    match run(node, db, "sign", &[message], 0)? {
        ManagedOutput::Bytes(signature) => Ok(signature),
        _ => Err("signing returned no bytes".to_string()),
    }
}

fn revision(node: &Node) -> u64 {
    node.key_catalog().catalog(DB).revision
}

/// [`Node::manage_keys`] in [`DB`].
async fn manage(node: &Node, request: serde_json::Value) -> Result<serde_json::Value, NodeError> {
    node.manage_keys(DB, request).await
}

#[tokio::test(flavor = "multi_thread")]
async fn the_catalog_replicates_and_every_member_signs_alike() {
    let dir = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let mut nodes = Vec::new();
    for id in 1..=3 {
        nodes.push(open(id, dir.path(), &registry).await);
    }
    nodes[0]
        .initialize(BTreeSet::from([1, 2, 3]))
        .await
        .unwrap();
    eventually("a leader", || nodes[0].status().leader.is_some()).await;
    let leader_id = nodes[0].status().leader.unwrap();
    let leader = nodes.iter().find(|node| node.id() == leader_id).unwrap();
    let follower = nodes.iter().find(|node| node.id() != leader_id).unwrap();

    // Followers refuse changes so the API layer forwards them.
    let refused = manage(
        follower,
        json!({"operation":"generate","requestId":"g","name":"signing","algorithm":"Ed25519"}),
    )
    .await
    .unwrap_err();
    assert!(matches!(refused, NodeError::Forward { .. }), "{refused}");

    let generated = manage(
        leader,
        json!({"operation":"generate","requestId":"g","name":"signing","algorithm":"Ed25519"}),
    )
    .await
    .unwrap();
    assert_eq!(generated["revision"], 1);
    let bound = manage(leader, json!({"operation":"bind","requestId":"b","name":"sessions","key":"signing","usages":["sign","verify","publicKey"]}))
        .await
        .unwrap();
    assert_eq!(bound["revision"], 2);
    // A retried request answers from its receipt instead of applying
    // twice.
    let retried = manage(leader, json!({"operation":"bind","requestId":"b","name":"sessions","key":"signing","usages":["sign","verify","publicKey"]}))
        .await
        .unwrap();
    assert_eq!(retried["duplicate"], true);
    assert_eq!(retried["revision"], 2);
    let reused = manage(leader, json!({"operation":"bind","requestId":"b","name":"other","key":"signing","usages":["sign"]}))
        .await
        .unwrap_err();
    assert!(
        reused.to_string().contains("KEY_REQUEST_REUSED"),
        "{reused}"
    );

    // Every member applied the catalog and signs alike.
    for node in &nodes {
        eventually("the catalog on every member", || revision(node) == 2).await;
    }
    let signatures: Vec<Vec<u8>> = nodes.iter().map(|node| sign(node, b"m").unwrap()).collect();
    assert!(
        signatures
            .iter()
            .all(|signature| *signature == signatures[0])
    );
    let listed = manage(leader, json!({"operation":"list"})).await.unwrap();
    assert_eq!(listed["value"]["keys"]["signing"]["activeVersion"], 1);
    let all = leader.list_keys().await.unwrap();
    assert_eq!(
        all["databases"]["app"]["value"]["keys"]["signing"]["activeVersion"],
        1
    );
    let cache = manage(follower, json!({"operation":"cache"}))
        .await
        .unwrap();
    assert_eq!(cache["loads"], 1);

    // Rotation reaches every member: all sign with the new version.
    manage(
        leader,
        json!({"operation":"rotate","requestId":"r","name":"signing"}),
    )
    .await
    .unwrap();
    for node in &nodes {
        eventually("the rotation on every member", || revision(node) == 3).await;
    }
    let rotated: Vec<Vec<u8>> = nodes.iter().map(|node| sign(node, b"m").unwrap()).collect();
    assert!(rotated.iter().all(|signature| *signature == rotated[0]));
    assert_ne!(rotated[0], signatures[0]);

    // Devices syncing down see catalog changes emptied of material:
    // they hold no wrapping key, but the entry moves their group head
    // as it moves every member's.
    let batch = leader.sync_since(0, 4096).unwrap();
    let shipped: Vec<&ClusterWrite> = batch
        .entries
        .iter()
        .map(|entry| &entry.write)
        .filter(|write| matches!(write, ClusterWrite::Keys { .. }))
        .collect();
    assert!(!shipped.is_empty(), "catalog changes never reached devices");
    assert!(
        shipped.iter().all(|write| matches!(
            write,
            ClusterWrite::Keys { db, change, .. } if change.is_empty() && db == DB
        )),
        "catalog material shipped to devices"
    );
    assert_eq!(Some(batch.current), leader.status().last_applied);
    for node in nodes {
        registry.unregister(node.id());
        node.shutdown().await.unwrap();
    }
}

/// A key request names a database: never the empty name, a reserved
/// one (`$cluster` is the group's own, and registered as a database
/// every snapshot would carry it twice) or one with control characters.
/// The leader refuses it before proposing, and apply refuses an entry
/// naming one anyway, alike on every member and in replay.
#[tokio::test(flavor = "multi_thread")]
async fn key_requests_name_a_database() {
    let dir = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let node = open(1, dir.path(), &registry).await;
    node.initialize(BTreeSet::from([1])).await.unwrap();
    eventually("node 1 to lead", || node.status().leader == Some(1)).await;
    let generate =
        json!({"operation":"generate","requestId":"g","name":"signer","algorithm":"Ed25519"});
    for db in [&b""[..], b"$cluster", b"$x", b"a\x01b", b"\xff"] {
        match node.manage_keys(db, generate.clone()).await {
            Err(NodeError::Failure { code, .. }) => assert_eq!(code, KEY_REQUEST, "{db:?}"),
            other => panic!("{db:?}: {other:?}"),
        }
    }

    let Prepared::Change(change) = keys().prepare(None, &generate).unwrap() else {
        panic!("generating prepares a change");
    };
    let store = flower_engine::storage::GroupStore::create(dir.path().join("apply.redb")).unwrap();
    let write = store.begin_write().unwrap();
    for db in [&b""[..], b"$cluster", b"a\x01b"] {
        let refused = StateMachine::apply_key_change(&write, db, &change)
            .unwrap()
            .unwrap_err();
        assert!(refused.starts_with(KEY_REQUEST), "{refused}");
        assert_eq!(write.meta(db, KEY_CATALOG).unwrap(), None);
    }
    StateMachine::apply_key_change(&write, DB, &change)
        .unwrap()
        .unwrap();
    assert!(write.meta(DB, KEY_CATALOG).unwrap().is_some());
}

#[tokio::test(flavor = "multi_thread")]
async fn databases_sharing_a_group_keep_their_own_keys() {
    // The same key name, alias and request IDs in two databases of one group
    // provision two keys, and a token one issues never authorizes the other.
    let dir = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let node = open(1, dir.path(), &registry).await;
    node.initialize(BTreeSet::from([1])).await.unwrap();
    eventually("node 1 to lead", || node.status().leader == Some(1)).await;
    let (a, b): (&[u8], &[u8]) = (b"tenant-a", b"tenant-b");
    let mut ids = Vec::new();
    for db in [a, b] {
        let generated = node
            .manage_keys(
                db,
                json!({"operation":"generate","requestId":"generate","name":"signer","algorithm":"Ed25519"}),
            )
            .await
            .unwrap();
        assert_eq!(generated["duplicate"], false, "{generated}");
        assert_eq!(generated["revision"], 1);
        ids.push(generated["value"]["keys"]["signer"]["id"].clone());
        node.manage_keys(db, json!({"operation":"bind","requestId":"bind","name":"sessions","key":"signer","usages":["sign","verify","publicKey"]}))
            .await
            .unwrap();
    }
    assert_ne!(ids[0], ids[1], "each database generated a key of its own");
    let public = |db: &[u8]| match run(&node, db, "publicKey", &[], 0).unwrap() {
        ManagedOutput::Bytes(public) => public,
        _ => panic!("public keys are bytes"),
    };
    assert_ne!(public(a), public(b), "one alias, two keys");

    let ManagedOutput::Text(token) = run(
        &node,
        a,
        "jwt.sign",
        &[br#"{"sub":"tenant","exp":4102444800}"#],
        0,
    )
    .unwrap() else {
        panic!("tokens are text");
    };
    let now = 1_700_000_000_000;
    run(&node, a, "jwt.verify", &[token.as_bytes()], now).unwrap();
    let refused = run(&node, b, "jwt.verify", &[token.as_bytes()], now)
        .err()
        .expect("the same alias in another database must not authorize the first's key");
    assert!(refused.starts_with("UNKNOWN_KEY"), "{refused}");
    assert_ne!(
        sign_in(&node, a, b"m").unwrap(),
        sign_in(&node, b, b"m").unwrap()
    );

    // A change to one database's catalog leaves the other's alone, and
    // a database without a catalog resolves nothing.
    node.manage_keys(
        a,
        json!({"operation":"rotate","requestId":"rotate","name":"signer"}),
    )
    .await
    .unwrap();
    assert_eq!(node.key_catalog().catalog(a).revision, 3);
    assert_eq!(node.key_catalog().catalog(b).revision, 2);
    let unknown = sign_in(&node, b"tenant-c", b"m").unwrap_err();
    assert!(unknown.starts_with("UNKNOWN_KEY"), "{unknown}");
    let listed = node.list_keys().await.unwrap();
    assert_eq!(listed["databases"]["tenant-a"]["revision"], 3);
    assert_eq!(listed["databases"]["tenant-b"]["revision"], 2);
    assert!(listed["databases"].get("tenant-c").is_none(), "{listed}");

    // Each catalog comes back from storage into a fresh runtime.
    registry.unregister(1);
    node.shutdown().await.unwrap();
    let node = open(1, dir.path(), &registry).await;
    eventually("both catalogs after a restart", || {
        node.key_catalog().catalog(a).revision == 3 && node.key_catalog().catalog(b).revision == 2
    })
    .await;
    assert!(run(&node, b, "jwt.verify", &[token.as_bytes()], now).is_err());
    registry.unregister(1);
    node.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn point_in_time_recovery_replays_catalog_changes() {
    use flower_backup::driver::{Driver, RuntimeBuilder};
    use flower_cluster::statemachine::StateMachine;
    let dir = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let node = open(1, dir.path(), &registry).await;
    node.initialize(BTreeSet::from([1])).await.unwrap();
    eventually("node 1 to lead", || node.status().leader == Some(1)).await;
    node.establish_incarnation().await.unwrap();
    let builder: RuntimeBuilder = Arc::new(|_: &[u8], _: &str, _: &[u8], _: &Value| {
        Ok(Box::new(ClosureRuntime::new(|_, _, _, _, _| {
            Err(fail("UNKNOWN_METHOD", "no guests"))
        })) as Box<dyn flower_engine::runtime::Runtime>)
    });
    // The base predates the catalog: only replay recreates it.
    let store = Driver::memory_store();
    let keystore = dir.path().join("keystore.json");
    let driver = Driver::new(
        store.clone(),
        &keystore,
        [3u8; 32],
        node.engine().clone(),
        node.logs().clone(),
        builder.clone(),
    )
    .unwrap();
    driver.backup_now().await.unwrap();
    manage(
        &node,
        json!({"operation":"generate","requestId":"g","name":"signing","algorithm":"Ed25519"}),
    )
    .await
    .unwrap();
    manage(&node, json!({"operation":"bind","requestId":"b","name":"sessions","key":"signing","usages":["sign"]}))
        .await
        .unwrap();
    let expected = sign(&node, b"m").unwrap();
    let target = node.status().last_applied.unwrap();
    driver.archive_now().await.unwrap();
    registry.unregister(1);
    node.shutdown().await.unwrap();

    let offline = Driver::offline(store, &keystore, [3u8; 32], builder).unwrap();
    let restored = dir.path().join("restored").join("group.redb");
    std::fs::create_dir_all(restored.parent().unwrap()).unwrap();
    offline
        .restore_to_index(&restored, target, None)
        .await
        .unwrap();
    let engine = flower_engine::engine::Engine::new(
        flower_engine::storage::GroupStore::open(&restored).unwrap(),
    );
    let catalog = StateMachine::key_catalog(&engine, DB).unwrap();
    drop(engine);
    let restored_keys = keys();
    restored_keys.install(DB, catalog.as_deref()).unwrap();
    assert_eq!(restored_keys.catalog(DB).revision, 2);
    let signed = match restored_keys
        .execute(DB, &declaration(), "sign", None, &[b"m"], &json!({}), 0)
        .unwrap()
    {
        ManagedOutput::Bytes(signature) => signature,
        _ => panic!("signing returns bytes"),
    };
    assert_eq!(signed, expected);
}

#[test]
fn the_wasm_factory_scopes_crypto_to_each_module() {
    use flower_cluster::wasm::WasmFactory;
    use flower_crypto::host::CryptoHost;
    let factory = WasmFactory::with_crypto(Arc::new(CryptoHost::new().with_catalog(keys())));
    // Conflicting declarations reject the module before its code
    // compiles: deploys validate through the factory.
    let declaration = |algorithm: &str| {
        Value::Object(
            [
                ("name".to_string(), Value::String("sessions".to_string())),
                (
                    "algorithm".to_string(),
                    Value::String(algorithm.to_string()),
                ),
                (
                    "usages".to_string(),
                    Value::Array(vec![Value::String("sign".to_string())]),
                ),
            ]
            .into_iter()
            .collect(),
        )
    };
    let manifest = Value::Object(
        [(
            "keys".to_string(),
            Value::Array(vec![declaration("Ed25519"), declaration("P256")]),
        )]
        .into_iter()
        .collect(),
    );
    let refused = factory
        .build(DB, "app", b"not wasm", &manifest)
        .err()
        .expect("conflicting declarations reject the module");
    assert!(refused.contains("declares invalid keys"), "{refused}");
}

#[tokio::test(flavor = "multi_thread")]
async fn members_recover_the_catalog_from_storage_and_snapshots() {
    let dir = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let mut nodes = Vec::new();
    for id in 1..=3 {
        nodes.push(open(id, dir.path(), &registry).await);
    }
    nodes[0]
        .initialize(BTreeSet::from([1, 2, 3]))
        .await
        .unwrap();
    eventually("a leader", || nodes[0].status().leader.is_some()).await;

    // Member 3 stops before the catalog changes.
    let third = nodes.pop().unwrap();
    registry.unregister(3);
    third.shutdown().await.unwrap();
    eventually("a leader of two", || {
        nodes
            .iter()
            .any(|node| node.status().leader == Some(node.id()))
    })
    .await;
    let leader = nodes
        .iter()
        .find(|node| node.status().leader == Some(node.id()))
        .unwrap();
    manage(
        leader,
        json!({"operation":"generate","requestId":"g","name":"signing","algorithm":"Ed25519"}),
    )
    .await
    .unwrap();
    manage(leader, json!({"operation":"bind","requestId":"b","name":"sessions","key":"signing","usages":["sign"]}))
        .await
        .unwrap();
    let expected = sign(leader, b"m").unwrap();
    for node in &nodes {
        eventually("the catalog on the running members", || revision(node) == 2).await;
    }
    // Both running members snapshot and purge the log past the change,
    // so member 3 can only catch up from a snapshot.
    for node in &nodes {
        node.raft().trigger().snapshot().await.unwrap();
    }
    for node in &nodes {
        eventually("a snapshot", || {
            node.raft().metrics().borrow().snapshot.is_some()
        })
        .await;
        let upto = node.raft().metrics().borrow().snapshot.unwrap().index;
        node.raft().trigger().purge_log(upto).await.unwrap();
    }

    // Member 3 reopens behind the purged log: the snapshot carries the
    // catalog to its runtime.
    let third = open(3, dir.path(), &registry).await;
    eventually("the catalog from a snapshot", || revision(&third) == 2).await;
    assert_eq!(sign(&third, b"m").unwrap(), expected);
    nodes.push(third);

    // A restarted member reads the catalog back from its own storage
    // into a fresh runtime, before any new entry applies.
    let restarted = nodes.remove(0);
    let id = restarted.id();
    registry.unregister(id);
    restarted.shutdown().await.unwrap();
    let back = open(id, dir.path(), &registry).await;
    eventually("the catalog after a restart", || revision(&back) == 2).await;
    assert_eq!(sign(&back, b"m").unwrap(), expected);
    nodes.push(back);
    for node in nodes {
        registry.unregister(node.id());
        node.shutdown().await.unwrap();
    }
}

/// A derived value that resolves a key, and a query reading it.
fn stamping() -> Box<dyn flower_engine::runtime::Runtime> {
    Box::new(ClosureRuntime::new(
        |kind: flower_engine::runtime::CallKind,
         _module: &str,
         name: &str,
         _args: &Value,
         host: &mut dyn flower_engine::runtime::HostCall| {
            use flower_engine::runtime::CallKind;
            match (kind, name) {
                (CallKind::Derived, "stamp") => {
                    host.read_keys();
                    Ok(Value::Number(1.0))
                }
                (CallKind::Query, "read") => {
                    let input = Value::Array(vec![
                        Value::String("app".to_string()),
                        Value::String("stamp".to_string()),
                        Value::Object(Default::default()),
                    ])
                    .encode_binary()
                    .unwrap();
                    let output = host.op(flower_sdk::abi::HostOp::GetDerived as u32 as u8, &input);
                    flower_sdk::abi::decode_envelope(Value::decode_binary(&output).unwrap())
                }
                _ => Err(fail("UNKNOWN_METHOD", "no such definition")),
            }
        },
    ))
}

struct Stamping;

impl RuntimeFactory for Stamping {
    fn build(
        &self,
        _db: &[u8],
        _module: &str,
        _wasm: &[u8],
        _manifest: &Value,
    ) -> Result<Box<dyn flower_engine::runtime::Runtime>, String> {
        Ok(stamping())
    }
}

/// Committed derived outcomes of `db` that resolved a key.
fn key_readers(engine: &flower_engine::engine::Engine) -> usize {
    engine
        .store()
        .begin_read()
        .unwrap()
        .dependents(b"db", &flower_engine::certificate::keys_dep())
        .unwrap()
        .len()
}

#[tokio::test(flavor = "multi_thread")]
async fn point_in_time_recovery_drops_derived_values_with_key_changes() {
    use flower_backup::driver::{Driver, RuntimeBuilder};
    let dir = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let node = Node::open(
        1,
        &dir.path().join("n1"),
        Arc::new(openraft::Config::default()),
        InProcessFactory::new(registry.clone()),
        Arc::new(Stamping),
    )
    .await
    .unwrap()
    .with_key_catalog(keys());
    registry.register(1, node.raft().clone());
    node.initialize(BTreeSet::from([1])).await.unwrap();
    eventually("node 1 to lead", || node.status().leader == Some(1)).await;
    node.establish_incarnation().await.unwrap();
    let manifest = flower_engine::json::parse_value(
        json!({
            "collections": [],
            "definitions": [
                {"name": "stamp", "kind": "derived", "args": {"type": "json"}},
                {"name": "read", "kind": "query", "args": {"type": "json"}},
            ],
            "triggers": [],
            "http": [],
        })
        .to_string()
        .as_bytes(),
    )
    .unwrap();
    node.deploy(b"db", "app", [6u8; 32], b"", &manifest)
        .await
        .unwrap();
    // A read computes the derived value and keeps it, edged to the keys.
    let empty = Value::Object(Default::default());
    let read = node
        .reader()
        .invoke_query(b"db", "app", "read", &empty, &Default::default())
        .unwrap();
    assert_eq!(read, Value::Number(1.0));
    assert_eq!(key_readers(&*node.engine().lock().await), 1);
    // Another database's key change leaves it: that database resolves
    // keys in a catalog of its own.
    node.manage_keys(
        b"other",
        json!({"operation":"generate","requestId":"g","name":"signing","algorithm":"Ed25519"}),
    )
    .await
    .unwrap();
    assert_eq!(key_readers(&*node.engine().lock().await), 1);
    // The base holds it; only replaying its database's key change drops
    // it.
    let builder: RuntimeBuilder = Arc::new(|_: &[u8], _: &str, _: &[u8], _: &Value| Ok(stamping()));
    let store = Driver::memory_store();
    let keystore = dir.path().join("keystore.json");
    let driver = Driver::new(
        store.clone(),
        &keystore,
        [3u8; 32],
        node.engine().clone(),
        node.logs().clone(),
        builder.clone(),
    )
    .unwrap();
    driver.backup_now().await.unwrap();
    node.manage_keys(
        b"db",
        json!({"operation":"generate","requestId":"g","name":"signing","algorithm":"Ed25519"}),
    )
    .await
    .unwrap();
    assert_eq!(key_readers(&*node.engine().lock().await), 0);
    let target = node.status().last_applied.unwrap();
    driver.archive_now().await.unwrap();
    registry.unregister(1);
    node.shutdown().await.unwrap();

    let offline = Driver::offline(store, &keystore, [3u8; 32], builder).unwrap();
    let restored = dir.path().join("restored").join("group.redb");
    std::fs::create_dir_all(restored.parent().unwrap()).unwrap();
    offline
        .restore_to_index(&restored, target, None)
        .await
        .unwrap();
    let engine = flower_engine::engine::Engine::new(
        flower_engine::storage::GroupStore::open(&restored).unwrap(),
    );
    assert_eq!(key_readers(&engine), 0);
}
