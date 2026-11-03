//! Real-guest execution: compile the guest crate to `wasm32` and
//! run it through the wasmtime backend, proving the full loop of
//! instantiation, allocation, invocation, host calls and envelopes.

mod fixtures;

use fixtures::{arr, obj, s};

use std::path::PathBuf;
use std::process::Command;

use flower_engine::engine::{CallOptions, Engine, MutationOptions};
use flower_engine::json::parse_value;
use flower_engine::runtime::wasm::WasmBackend;
use flower_engine::storage::GroupStore;
use flower_sdk::Value;

fn cargo() -> String {
    option_env!("CARGO")
        .map(str::to_string)
        .or_else(|| std::env::var("CARGO").ok())
        .unwrap_or_else(|| "cargo".to_string())
}

/// Build the guest crate for `wasm32` in an isolated target dir —
/// the outer `cargo test` owns the workspace lock — and return the
/// artifact bytes. Incremental across runs.
fn guest_wasm() -> Vec<u8> {
    let root: PathBuf = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf();
    let target = root.join("target").join("wasm-guest");
    let status = Command::new(cargo())
        .args([
            "build",
            "--offline",
            "--quiet",
            "--target",
            "wasm32-unknown-unknown",
            "-p",
            "flower-guest-test",
        ])
        .current_dir(&root)
        .env("CARGO_TARGET_DIR", &target)
        .status()
        .unwrap();
    assert!(status.success(), "guest build failed");
    let wasm = target
        .join("wasm32-unknown-unknown")
        .join("debug")
        .join("flower_guest_test.wasm");
    std::fs::read(&wasm).unwrap()
}

fn manifest() -> Value {
    let str_schema = obj(vec![("type", s("string"))]);
    let collection = obj(vec![
        ("name", s("tasks")),
        ("key", str_schema.clone()),
        (
            "value",
            obj(vec![
                ("type", s("object")),
                (
                    "fields",
                    arr(vec![obj(vec![
                        ("name", s("title")),
                        ("schema", str_schema),
                        ("optional", Value::Bool(false)),
                    ])]),
                ),
            ]),
        ),
        (
            "indexes",
            arr(vec![obj(vec![
                ("name", s("by_title")),
                ("fields", arr(vec![s("title")])),
            ])]),
        ),
        ("references", arr(vec![])),
    ]);
    let json_schema = obj(vec![("type", s("json"))]);
    let definitions = arr(vec![
        obj(vec![
            ("name", s("put")),
            ("kind", s("mutation")),
            ("args", json_schema.clone()),
        ]),
        obj(vec![
            ("name", s("get")),
            ("kind", s("query")),
            ("args", json_schema.clone()),
        ]),
        obj(vec![
            ("name", s("sign_message")),
            ("kind", s("mutation")),
            ("args", json_schema.clone()),
        ]),
        obj(vec![
            ("name", s("verify_message")),
            ("kind", s("query")),
            ("args", json_schema.clone()),
        ]),
        obj(vec![
            ("name", s("send_email")),
            ("kind", s("mutation")),
            ("args", json_schema.clone()),
        ]),
        obj(vec![
            ("name", s("remind_me")),
            ("kind", s("mutation")),
            ("args", json_schema),
        ]),
    ]);
    obj(vec![
        ("collections", arr(vec![collection])),
        ("definitions", definitions),
        ("triggers", arr(vec![])),
        ("http", arr(vec![])),
        // `send_email`'s queue, as the guest declares it.
        ("queues", arr(vec![obj(vec![("name", s("email"))])])),
    ])
}

#[test]
fn real_guest_put_get_roundtrip() {
    let wasm = guest_wasm();
    let mut path = std::env::temp_dir();
    path.push(format!("flower-wasm-{}", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let store = GroupStore::create(&path).unwrap();
    let mut engine = Engine::new(store);
    // The guest imports crypto_call, so the backend carries a manager
    // even though put/get never touch it.
    let host = std::sync::Arc::new(flower_crypto::host::CryptoHost::new());
    let mut backend = WasmBackend::new().unwrap();
    backend.set_crypto(host);
    backend.register("app", &wasm).unwrap();
    engine
        .deploy(
            b"db",
            "app",
            [7u8; 32],
            &wasm,
            &manifest(),
            Box::new(backend),
        )
        .unwrap();

    // A mutation through real Wasm: guest args decode, typed handles,
    // host Set, envelope encode.
    let args = parse_value(br#"{"key":"a","title":"first"}"#).unwrap();
    let out = engine
        .invoke_mutation(b"db", "app", "put", &args, &MutationOptions::default())
        .unwrap();
    assert_eq!(out, s("put-ok"));

    // A query reading the committed row back through the guest.
    let args = parse_value(br#"{"key":"a"}"#).unwrap();
    let out = engine
        .invoke_query(b"db", "app", "get", &args, &CallOptions::default())
        .unwrap();
    assert_eq!(out, obj(vec![("title", s("first"))]));

    // A missing row reads as null through the guest's Option return.
    let args = parse_value(br#"{"key":"missing"}"#).unwrap();
    let out = engine
        .invoke_query(b"db", "app", "get", &args, &CallOptions::default())
        .unwrap();
    assert_eq!(out, Value::Null);

    // Guest dispatch rejects malformed args with its own schema check.
    let failure = engine
        .invoke_mutation(
            b"db",
            "app",
            "put",
            &Value::Null,
            &MutationOptions::default(),
        )
        .unwrap_err();
    assert_eq!(failure.code, "INVALID_ARGUMENT");

    // The guest describes itself through flower_manifest.
    let mut backend = WasmBackend::new().unwrap();
    backend.set_crypto(std::sync::Arc::new(flower_crypto::host::CryptoHost::new()));
    backend.register("app", &wasm).unwrap();
    let described = backend.manifest("app").unwrap();
    let definitions = match &described {
        Value::Object(map) => match map.get("definitions") {
            Some(Value::Array(definitions)) => definitions
                .iter()
                .map(|definition| match definition {
                    Value::Object(def) => match def.get("name") {
                        Some(Value::String(name)) => name.clone(),
                        _ => panic!("definition without a name"),
                    },
                    _ => panic!("definition is not an object"),
                })
                .collect::<Vec<_>>(),
            _ => panic!("manifest has no definitions"),
        },
        _ => panic!("manifest is not an object"),
    };
    assert_eq!(
        definitions,
        vec![
            "get".to_string(),
            "put".to_string(),
            "sign_message".to_string(),
            "verify_message".to_string(),
            "send_email".to_string(),
            "remind_me".to_string(),
            "title_count".to_string(),
            "titled".to_string()
        ]
    );

    drop(engine);
    let _ = std::fs::remove_file(&path);
}

/// An aggregate and a maintenance task in a real `wasm32` guest, on its
/// own manifest: the reducer runs as invocation kind 5 through
/// `flower_invoke` (the count builds once, then takes inserts, moves and
/// the task's renames as deltas), and the task runs through the engine.
#[test]
fn real_guest_aggregates_by_deltas_and_runs_tasks() {
    use flower_engine::task::TaskPrepare;

    let wasm = guest_wasm();
    let mut path = std::env::temp_dir();
    path.push(format!("flower-wasm-deltas-{}", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let mut engine = Engine::new(GroupStore::create(&path).unwrap());
    let mut backend = WasmBackend::new().unwrap();
    backend.set_crypto(std::sync::Arc::new(flower_crypto::host::CryptoHost::new()));
    backend.register("app", &wasm).unwrap();
    let described = backend.manifest("app").unwrap();
    engine
        .deploy(
            b"db",
            "app",
            [8u8; 32],
            &wasm,
            &described,
            Box::new(backend),
        )
        .unwrap();
    let put = |engine: &mut Engine, key: &str, title: &str| {
        engine
            .invoke_mutation(
                b"db",
                "app",
                "put",
                &obj(vec![("key", s(key)), ("title", s(title))]),
                &MutationOptions::default(),
            )
            .unwrap();
    };
    let titled = |engine: &Engine, title: &str| {
        engine
            .invoke_query(
                b"db",
                "app",
                "titled",
                &obj(vec![("title", s(title))]),
                &CallOptions::default(),
            )
            .unwrap()
    };
    // A group rebuilt from its rows would count no removals.
    let count = |count: f64, removed: f64| {
        obj(vec![
            ("count", Value::Number(count)),
            ("removed", Value::Number(removed)),
        ])
    };
    for (key, title) in [("a", "todo"), ("b", "todo"), ("c", "x")] {
        put(&mut engine, key, title);
    }
    assert_eq!(titled(&engine, "todo"), count(2.0, 0.0));
    put(&mut engine, "d", "todo");
    assert_eq!(titled(&engine, "todo"), count(3.0, 0.0));
    put(&mut engine, "a", "x");
    assert_eq!(titled(&engine, "todo"), count(2.0, 1.0));
    assert_eq!(titled(&engine, "x"), count(2.0, 0.0));

    // The task renames the todos, one per run, then has nothing due.
    let due = engine.reader().due_tasks(b"db", u64::MAX / 2).unwrap();
    assert_eq!(due.len(), 1);
    assert_eq!(
        (due[0].module.as_str(), due[0].task.as_str()),
        ("app", "tidy")
    );
    let mut runs = 0;
    for position in 1.. {
        match engine.prepare_task_run(b"db", "app", "tidy").unwrap() {
            TaskPrepare::Ran(prepared) => {
                engine
                    .apply_patch(&prepared.patch, position)
                    .unwrap()
                    .result
                    .unwrap();
                runs += 1;
            }
            TaskPrepare::Skip => break,
            other => panic!("unexpected task outcome {other:?}"),
        }
        assert!(position < 10, "the task never finished");
    }
    assert_eq!(runs, 2);
    assert_eq!(titled(&engine, "todo"), count(0.0, 3.0));
    assert_eq!(titled(&engine, "done"), count(2.0, 0.0));

    drop(engine);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn real_guest_signs_through_managed_keys() {
    use std::sync::Arc;

    let wasm = guest_wasm();
    let mut path = std::env::temp_dir();
    path.push(format!("flower-wasm-crypto-{}", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let store = GroupStore::create(&path).unwrap();
    let mut engine = Engine::new(store);

    // Bind the alias the guest references by name in database `db`'s
    // catalog, as the leader prepares and every member applies it, and
    // declare it in the module's manifest.
    use flower_crypto::replicated::{ManagedKeys, Prepared, WrappingKey};
    let keys = Arc::new(ManagedKeys::new(
        Some(WrappingKey::new(&[9; 32])),
        Vec::new(),
    ));
    let mut stored: Option<Vec<u8>> = None;
    for request in [
        serde_json::json!({"operation":"generate","requestId":"g","name":"k","algorithm":"Ed25519"}),
        serde_json::json!({"operation":"bind","requestId":"b","name":"deploy","key":"k","usages":["sign","verify"]}),
    ] {
        let Prepared::Change(change) = keys.prepare(stored.as_deref(), &request).unwrap() else {
            panic!("changes propose");
        };
        stored = flower_crypto::catalog::apply_change(stored.as_deref(), &change)
            .unwrap()
            .catalog;
    }
    keys.install(b"db", stored.as_deref()).unwrap();
    let declared = obj(vec![(
        "keys",
        arr(vec![
            obj(vec![
                ("name", s("deploy")),
                ("algorithm", s("Ed25519")),
                ("usages", arr(vec![s("sign"), s("verify")])),
            ]),
            obj(vec![
                ("name", s("missing")),
                ("algorithm", s("Ed25519")),
                ("usages", arr(vec![s("sign")])),
            ]),
        ]),
    )]);
    let host = flower_crypto::host::CryptoHost::new()
        .with_catalog(keys)
        .for_module(b"db", "app", &declared)
        .unwrap();
    let mut backend = WasmBackend::new().unwrap();
    backend.set_crypto(Arc::new(host));
    backend.register("app", &wasm).unwrap();
    engine
        .deploy(
            b"db",
            "app",
            [7u8; 32],
            &wasm,
            &manifest(),
            Box::new(backend),
        )
        .unwrap();

    // Sign through the guest, verify through the guest.
    let args = parse_value(br#"{"key":"deploy","message":"release"}"#).unwrap();
    let out = engine
        .invoke_mutation(
            b"db",
            "app",
            "sign_message",
            &args,
            &MutationOptions::default(),
        )
        .unwrap();
    let signature = match out {
        Value::String(signature) => signature,
        _ => panic!("signature is hex"),
    };
    assert_eq!(signature.len(), 128);
    let args = parse_value(
        format!("{{\"key\":\"deploy\",\"message\":\"release\",\"signature\":\"{signature}\"}}")
            .as_bytes(),
    )
    .unwrap();
    let out = engine
        .invoke_query(
            b"db",
            "app",
            "verify_message",
            &args,
            &CallOptions::default(),
        )
        .unwrap();
    assert_eq!(out, Value::Bool(true));

    // Tampering fails verification without failing the call.
    let args = parse_value(
        format!("{{\"key\":\"deploy\",\"message\":\"tampered\",\"signature\":\"{signature}\"}}")
            .as_bytes(),
    )
    .unwrap();
    let out = engine
        .invoke_query(
            b"db",
            "app",
            "verify_message",
            &args,
            &CallOptions::default(),
        )
        .unwrap();
    assert_eq!(out, Value::Bool(false));

    // Declared aliases the catalog does not bind fail as catchable
    // business failures.
    let args = parse_value(br#"{"key":"missing","message":"release"}"#).unwrap();
    let failure = engine
        .invoke_mutation(
            b"db",
            "app",
            "sign_message",
            &args,
            &MutationOptions::default(),
        )
        .unwrap_err();
    assert_eq!(failure.code, "UNKNOWN_KEY");

    // At prepare, the same failure forms a failure patch instead of
    // poisoning: business failures replicate as receipts.
    let prepared = match engine
        .prepare_mutation(
            b"db",
            "app",
            "sign_message",
            &args,
            &MutationOptions::default(),
        )
        .unwrap()
    {
        flower_engine::engine::PrepareOutcome::Patch(prepared) => prepared,
        _ => panic!("prepare short-circuited"),
    };
    assert_eq!(prepared.outcome.unwrap_err().code, "UNKNOWN_KEY");
    let patch = flower_engine::patch::decode_patch(&prepared.patch).unwrap();
    assert!(patch.failed);

    drop(engine);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn real_guest_enqueue_pollable() {
    let wasm = guest_wasm();
    let mut path = std::env::temp_dir();
    path.push(format!("flower-wasm-queue-{}", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let store = GroupStore::create(&path).unwrap();
    let mut engine = Engine::new(store);
    let mut backend = WasmBackend::new().unwrap();
    backend.set_crypto(std::sync::Arc::new(flower_crypto::host::CryptoHost::new()));
    backend.register("app", &wasm).unwrap();
    engine
        .deploy(
            b"db",
            "app",
            [7u8; 32],
            &wasm,
            &manifest(),
            Box::new(backend),
        )
        .unwrap();

    // A real guest mutation enqueues through the SDK and returns the ID.
    let args = parse_value(br#"{"to":"ada@example.com"}"#).unwrap();
    let out = engine
        .invoke_mutation(
            b"db",
            "app",
            "send_email",
            &args,
            &MutationOptions::default(),
        )
        .unwrap();
    let id = match out {
        Value::String(id) => id,
        _ => panic!("enqueue returns the item ID"),
    };
    assert_eq!(id.len(), 32);

    // The staged enqueue committed: a native poll claims it back.
    let claimed = engine
        .poll_queue(b"db", "email", u64::MAX, 10, 60_000)
        .unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].id, id);
    assert_eq!(claimed[0].payload, s("ada@example.com"));
    engine
        .complete_queue(b"db", "email", &id, &claimed[0].token, u64::MAX)
        .unwrap();
    let stats = engine.queue_stats(b"db", "email").unwrap();
    assert_eq!((stats.pending, stats.claimed, stats.dead), (0, 0, 0));

    drop(engine);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn real_guest_timer_fires_into_queue() {
    use flower_engine::engine::FirePrepare;

    let wasm = guest_wasm();
    let mut path = std::env::temp_dir();
    path.push(format!("flower-wasm-timer-{}", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let store = GroupStore::create(&path).unwrap();
    let mut engine = Engine::new(store);
    let mut backend = WasmBackend::new().unwrap();
    backend.set_crypto(std::sync::Arc::new(flower_crypto::host::CryptoHost::new()));
    backend.register("app", &wasm).unwrap();
    engine
        .deploy(
            b"db",
            "app",
            [7u8; 32],
            &wasm,
            &manifest(),
            Box::new(backend),
        )
        .unwrap();

    // A real guest mutation schedules a reminder due immediately.
    let args = parse_value(br#"{"id":"r1","to":"ada@example.com","delay_ms":0}"#).unwrap();
    engine
        .invoke_mutation(
            b"db",
            "app",
            "remind_me",
            &args,
            &MutationOptions::default(),
        )
        .unwrap();
    assert!(engine.get_timer(b"db", "r1").unwrap().is_some());

    // Firing runs the guest handler: the email enqueues.
    match engine.prepare_timer_fire(b"db", "r1").unwrap() {
        FirePrepare::Patch(prepared) => {
            engine.apply_patch(&prepared.patch, 1).unwrap();
        }
        _ => panic!("expected a fire patch"),
    }
    assert!(engine.get_timer(b"db", "r1").unwrap().is_none());
    let claimed = engine
        .poll_queue(b"db", "email", u64::MAX, 10, 60_000)
        .unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].payload, s("ada@example.com"));

    drop(engine);
    let _ = std::fs::remove_file(&path);
}
