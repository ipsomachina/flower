//! Operator CLI tests: offline contract inspection and diffs plus
//! a serve round-trip (boot, status, deploy, membership) against a
//! real node subprocess.

mod common;

use std::process::Command;
use std::time::Duration;

use common::{Server as Booted, bin};

// Each process test compiles Wasm in its own server. Running them
// together on CI can exhaust the client's deployment timeout.
static SERVER_TEST: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn manifest_v1() -> serde_json::Value {
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
            {"name": "get", "kind": "query", "args": {"type": "json"}},
            {"name": "sign_message", "kind": "mutation", "args": {"type": "json"}},
            {"name": "verify_message", "kind": "query", "args": {"type": "json"}}
        ],
        "triggers": [],
        "http": []
    })
}

fn manifest_v2() -> serde_json::Value {
    serde_json::json!({
        "collections": [
            manifest_v1()["collections"][0].clone(),
            {
                "name": "posts",
                "key": {"type": "string"},
                "value": {"type": "object", "fields": [
                    {"name": "title", "schema": {"type": "string"}, "optional": false}
                ]},
                "indexes": [],
                "references": []
            }
        ],
        "definitions": [
            {"name": "put", "kind": "mutation", "args": {"type": "json"}}
        ],
        "triggers": [],
        "http": []
    })
}

/// Expose every callable definition under an HTTP alias of the same name:
/// served calls name aliases, never internal definitions.
fn exposed(mut manifest: serde_json::Value) -> serde_json::Value {
    let aliases: Vec<serde_json::Value> = manifest["definitions"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|definition| definition["kind"] != "derived")
        .map(|definition| serde_json::json!({"alias": definition["name"], "definition": definition["name"]}))
        .collect();
    manifest["http"] = serde_json::Value::Array(aliases);
    manifest
}

fn write(dir: &std::path::Path, name: &str, value: &serde_json::Value) -> String {
    let path = dir.join(name);
    std::fs::write(&path, serde_json::to_string(value).unwrap()).unwrap();
    path.to_str().unwrap().to_string()
}

#[test]
fn contract_inspect_prints_summary() {
    let dir = tempfile::tempdir().unwrap();
    let manifest = write(dir.path(), "v1.json", &manifest_v1());
    let output = common::authenticated_command()
        .args(["contract", "inspect", "--manifest", &manifest])
        .output()
        .unwrap();
    assert!(output.status.success());
    let text = String::from_utf8(output.stdout).unwrap();
    assert!(text.contains("tasks"));
    assert!(text.contains("by_title"));
    assert!(text.contains("put"));
    assert!(text.contains("get"));
    assert!(text.contains("sign_message"));
}

#[test]
fn contract_inspect_rejects_garbage() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bad.json");
    std::fs::write(&path, b"{\"collections\": 42}").unwrap();
    let output = common::authenticated_command()
        .args(["contract", "inspect", "--manifest", path.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(!output.status.success());
}

#[test]
fn contract_diff_flags_changes() {
    let dir = tempfile::tempdir().unwrap();
    let from = write(dir.path(), "v1.json", &manifest_v1());
    let to = write(dir.path(), "v2.json", &manifest_v2());
    let output = common::authenticated_command()
        .args(["contract", "diff", "--from", &from, "--to", &to])
        .output()
        .unwrap();
    assert!(output.status.success());
    let text = String::from_utf8(output.stdout).unwrap();
    assert!(text.contains("+ collection posts"));
    assert!(text.contains("! definition get removed"));

    let output = common::authenticated_command()
        .args(["contract", "diff", "--from", &to, "--to", &from])
        .output()
        .unwrap();
    let text = String::from_utf8(output.stdout).unwrap();
    assert!(text.contains("! collection posts removed"));
}

/// Minimal valid Wasm importing `host_call` exactly once: deploys
/// through the real wasmtime backend but cannot execute calls.
const STUB_WASM: &[u8] = &[
    0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, // magic + version
    0x01, 0x04, 0x01, 0x60, 0x00, 0x00, // one () -> () type
    0x02, 0x11, 0x01, 0x03, 0x65, 0x6e, 0x76, // import "env".
    0x09, 0x68, 0x6f, 0x73, 0x74, 0x5f, 0x63, 0x61, 0x6c, 0x6c, 0x00,
    0x00, // "host_call", func 0
];

fn cargo() -> String {
    option_env!("CARGO")
        .map(str::to_string)
        .or_else(|| std::env::var("CARGO").ok())
        .unwrap_or_else(|| "cargo".to_string())
}

/// The workspace root.
fn workspace_root() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf()
}

/// Largest guest the serve tests deploy. Every deployment compiles the
/// module at least twice (validation on the leader, then the apply),
/// and a debug guest (2.3 MB) takes more than twice as long as the
/// release one (200 KB): with an unoptimized compiler that alone came
/// near the client's 20 s attempt timeout under load.
const MAX_TEST_GUEST_BYTES: usize = 1024 * 1024;

/// Build the guest crate for `wasm32` in release, in an isolated
/// target dir, and return the artifact bytes. Incremental across runs.
fn guest_wasm() -> Vec<u8> {
    let root = workspace_root();
    let target = root.join("target").join("wasm-guest");
    let status = std::process::Command::new(cargo())
        .args([
            "build",
            "--offline",
            "--quiet",
            "--release",
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
        .join("release")
        .join("flower_guest_test.wasm");
    let bytes = std::fs::read(&wasm).unwrap();
    assert!(
        bytes.len() <= MAX_TEST_GUEST_BYTES,
        "the test guest is {} bytes: deploys of it compile slowly",
        bytes.len()
    );
    bytes
}

/// Dev builds optimize the Wasm compiler: unoptimized Cranelift made a
/// debug `serve` take 9 s to deploy the release test guest (20 s for
/// the debug one), against the client's 20 s attempt timeout, and the
/// serve tests timed out under load. Optimized, the same deploys take
/// 0.7 s and 1.7 s.
#[test]
fn dev_builds_optimize_the_wasm_compiler() {
    let manifest = std::fs::read_to_string(workspace_root().join("Cargo.toml"))
        .unwrap()
        .replace("\r\n", "\n");
    for package in [
        "cranelift-codegen",
        "cranelift-frontend",
        "regalloc2",
        "wasmparser",
        "wasmtime",
        "wasmtime-environ",
        "wasmtime-internal-cranelift",
    ] {
        let section = format!("[profile.dev.package.{package}]\nopt-level = 3");
        assert!(
            manifest.contains(&section),
            "the workspace must optimize {package} in dev builds"
        );
    }
}

fn manifest_queue() -> serde_json::Value {
    let mut value = manifest_v1();
    value["definitions"]
        .as_array_mut()
        .unwrap()
        .push(serde_json::json!({
            "name": "send_email",
            "kind": "mutation",
            "args": {"type": "json"}
        }));
    // `send_email`'s queue, as the guest declares it.
    value["queues"] = serde_json::json!([{"name": "email"}]);
    value
}

fn manifest_timer() -> serde_json::Value {
    let mut value = manifest_queue();
    value["definitions"]
        .as_array_mut()
        .unwrap()
        .push(serde_json::json!({
            "name": "remind_me",
            "kind": "mutation",
            "args": {"type": "json"}
        }));
    value
}

/// A wrapping-key file in `dir` for `serve --key-wrapping-file`: the
/// guests' crypto host, over every database's key catalog.
fn wrapping_key(dir: &std::path::Path) -> String {
    let path = dir.join("wrapping.key");
    common::write_key(&path, &[7u8; 32]);
    path.to_str().unwrap().to_string()
}

/// Boot a single node with the given timer sweep interval.
async fn boot(dir: &std::path::Path, node: &str, timer_sweep_ms: &str) -> Booted {
    serve_node(&dir.join(node), &["--timer-sweep-ms", timer_sweep_ms]).await
}

/// `cargo-flower` with the admin token, as an operator runs it.
fn admin(args: &[&str]) -> std::process::Output {
    Command::new(bin())
        .env("FLOWER_ADMIN_TOKEN", "secret")
        .args(args)
        .output()
        .unwrap()
}

/// A served process hosts groups: one is listed, founded and served; a
/// partition placed in it moves back to the root with `--wait`
/// following each phase; the transactions it coordinates are listed and
/// an unknown one is refused.
#[tokio::test]
async fn groups_partitions_and_moves_from_the_command_line() {
    let _turn = SERVER_TEST.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let booted = boot(dir.path(), "node", "3600000").await;
    let endpoint = booted.endpoint.as_str();
    let created = admin(&[
        "group",
        "--endpoint",
        endpoint,
        "create",
        "east",
        "--members",
        "1",
    ]);
    assert!(created.status.success(), "{created:?}");
    assert!(String::from_utf8_lossy(&created.stdout).contains("\"name\": \"east\""));
    let east_active = |listed: &str| {
        listed.lines().any(|line| {
            line.starts_with("group=east ") && line.contains("status=active members=1 hosted=true")
        })
    };
    let mut listed = String::new();
    for _ in 0..100 {
        let output = admin(&["group", "--endpoint", endpoint]);
        listed = String::from_utf8(output.stdout).unwrap();
        if east_active(&listed) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(east_active(&listed), "{listed}");
    assert!(
        listed.contains("group=root uid=0000000000000000"),
        "{listed}"
    );
    let placed = admin(&[
        "partition",
        "--endpoint",
        endpoint,
        "create",
        "acme",
        "--group",
        "east",
    ]);
    assert!(placed.status.success(), "{placed:?}");
    // The same operation again answers what the first did.
    let again = admin(&[
        "partition",
        "--endpoint",
        endpoint,
        "create",
        "acme",
        "--group",
        "east",
    ]);
    assert!(again.status.success(), "{again:?}");
    let moved = admin(&[
        "partition",
        "--endpoint",
        endpoint,
        "move",
        "acme",
        "--to",
        "root",
        "--wait",
    ]);
    let text = String::from_utf8_lossy(&moved.stdout).to_string();
    assert!(moved.status.success(), "{moved:?}");
    assert!(
        text.contains("complete, acme served by root at epoch 2"),
        "{text}"
    );
    let partitions = admin(&["partition", "--endpoint", endpoint]);
    let text = String::from_utf8(partitions.stdout).unwrap();
    assert!(
        text.contains("partition=acme owner=root epoch=2 status=active move=none"),
        "{text}"
    );
    assert!(
        text.contains("moved op=move-acme partition=acme from=east to=root phase=complete"),
        "{text}"
    );
    // A move past its cutover is not aborted.
    let late = admin(&[
        "partition",
        "--endpoint",
        endpoint,
        "abort",
        "acme",
        "--op",
        "move-acme",
    ]);
    assert!(!late.status.success(), "{late:?}");
    let transactions = admin(&["transactions", "--endpoint", endpoint]);
    assert!(transactions.status.success(), "{transactions:?}");
    let listed: serde_json::Value = serde_json::from_slice(&transactions.stdout).unwrap();
    assert!(
        listed["groups"]["east"]["coordinated"].is_array(),
        "{listed}"
    );
    let unknown = admin(&[
        "transactions",
        "--endpoint",
        endpoint,
        "abort",
        "00",
        "--group",
        "east",
    ]);
    assert!(!unknown.status.success(), "{unknown:?}");
    assert!(
        String::from_utf8_lossy(&unknown.stderr).contains("transaction_unknown"),
        "{unknown:?}"
    );
    drop(booted);
}

#[tokio::test]
async fn serve_roundtrip() {
    let _turn = SERVER_TEST.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let node_dir = dir.path().join("node");
    let mut declared = exposed(manifest_v1());
    declared["keys"] = serde_json::json!([{"name": "deploy", "algorithm": "Ed25519", "usages": ["sign", "verify"]}]);
    let manifest = write(dir.path(), "app.json", &declared);
    let wasm_path = dir.path().join("app.wasm");
    std::fs::write(&wasm_path, guest_wasm()).unwrap();
    let wasm = wasm_path.to_str().unwrap().to_string();
    let broken_path = dir.path().join("broken.wasm");
    std::fs::write(&broken_path, STUB_WASM).unwrap();
    let broken = broken_path.to_str().unwrap().to_string();
    let Booted {
        mut child,
        endpoint,
    } = serve_node(
        &node_dir,
        &[
            "--query-cache-bytes",
            "4194304",
            "--query-flight-bytes",
            "262144",
            "--decision-memo-bytes",
            "2097152",
        ],
    )
    .await;

    let output = common::authenticated_command()
        .args([
            "deploy",
            "--endpoint",
            &endpoint,
            "--db",
            "db",
            "--module",
            "app",
            "--wasm",
            &wasm,
            "--manifest",
            &manifest,
            "--admin-token",
            "secret",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let text = String::from_utf8(output.stdout).unwrap();
    assert!(text.contains("position="));

    // Real guest calls through the served node: mutate then query.
    let output = common::authenticated_command()
        .args([
            "call",
            "--endpoint",
            &endpoint,
            "--db",
            "db",
            "--module",
            "app",
            "--name",
            "put",
            "--mutation",
            "--args",
            "{\"key\": \"a\", \"title\": \"hi\"}",
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    assert!(String::from_utf8(output.stdout).unwrap().contains("put-ok"));

    let output = common::authenticated_command()
        .args([
            "call",
            "--endpoint",
            &endpoint,
            "--db",
            "db",
            "--module",
            "app",
            "--name",
            "get",
            "--args",
            "{\"key\": \"a\"}",
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    assert!(String::from_utf8(output.stdout).unwrap().contains("hi"));

    // The same read again answers from the query cache, sized by its
    // flag; the guest's image sits in the Wasm runtime's cache.
    let get = [
        "call",
        "--endpoint",
        &endpoint,
        "--db",
        "db",
        "--module",
        "app",
        "--name",
        "get",
        "--args",
        "{\"key\": \"a\"}",
    ];
    assert!(
        Command::new(bin())
            .args(get)
            .output()
            .unwrap()
            .status
            .success()
    );
    let output = Command::new(bin())
        .args([
            "metrics",
            "--endpoint",
            &endpoint,
            "--admin-token",
            "secret",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let metrics: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let cache = &metrics["queries"]["cache"];
    assert_eq!(cache["budget_bytes"], 4_194_304, "{cache}");
    assert!(cache["hits"].as_u64().unwrap() >= 1, "{cache}");
    assert!(cache["entries"].as_u64().unwrap() >= 1, "{cache}");
    let flights = &metrics["queries"]["flights"];
    assert_eq!(flights["budget_bytes"], 262_144, "{flights}");
    assert!(flights["led"].as_u64().unwrap() >= 1, "{flights}");
    assert!(metrics["queries"]["watch_evaluations"].is_u64());
    // The decision memo, sized by its flag.
    let decisions = &metrics["authorization"]["decisions"];
    assert_eq!(decisions["budget_bytes"], 2_097_152, "{decisions}");
    for counter in ["hits", "evaluations", "entries", "bytes"] {
        assert!(decisions[counter].is_u64(), "{decisions}");
    }
    let wasm = &metrics["wasm"];
    assert!(wasm["images"]["images"].as_u64().unwrap() >= 1, "{wasm}");
    assert!(wasm["images"]["prepared"].as_u64().unwrap() >= 1, "{wasm}");
    assert!(wasm["pooling"].is_boolean(), "{wasm}");

    // Signing through a key the database's catalog binds to the alias
    // its module declares.
    for (request_id, change) in [
        (
            "generate",
            vec!["generate", "--name", "signer", "--algorithm", "Ed25519"],
        ),
        (
            "bind",
            vec![
                "bind",
                "--alias",
                "deploy",
                "--key",
                "signer",
                "--usages",
                "sign,verify",
            ],
        ),
    ] {
        let mut args = vec![
            "keys",
            "--endpoint",
            &endpoint,
            "--db",
            "db",
            "--request-id",
            request_id,
        ];
        args.extend(change);
        let output = Command::new(bin())
            .env("FLOWER_ADMIN_TOKEN", "secret")
            .args(&args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let output = common::authenticated_command()
        .args([
            "call",
            "--endpoint",
            &endpoint,
            "--db",
            "db",
            "--module",
            "app",
            "--name",
            "sign_message",
            "--mutation",
            "--args",
            "{\"key\": \"deploy\", \"message\": \"release\"}",
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    let text = String::from_utf8(output.stdout).unwrap();
    let signature = text.lines().next().unwrap().trim_matches('"');
    assert_eq!(signature.len(), 128);

    // The stub guest cannot execute: registration validates its ABI, so
    // the deploy fails cleanly and calls find no module, rather than the
    // first call discovering the broken guest.
    let output = common::authenticated_command()
        .args([
            "deploy",
            "--endpoint",
            &endpoint,
            "--db",
            "db",
            "--module",
            "broken",
            "--wasm",
            &broken,
            "--manifest",
            &manifest,
            "--admin-token",
            "secret",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("invalid_module"));
    let output = common::authenticated_command()
        .args([
            "call",
            "--endpoint",
            &endpoint,
            "--db",
            "db",
            "--module",
            "broken",
            "--name",
            "get",
            "--args",
            "{\"key\": \"a\"}",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());

    let output = common::authenticated_command()
        .args([
            "membership",
            "--endpoint",
            &endpoint,
            "--members",
            "1",
            "--admin-token",
            "secret",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );

    // No backup driver is configured on this node.
    let output = common::authenticated_command()
        .args(["backup", "--endpoint", &endpoint, "--admin-token", "secret"])
        .output()
        .unwrap();
    assert!(!output.status.success());

    child.kill().await.unwrap();
}

#[tokio::test]
async fn queue_lifecycle() {
    let _turn = SERVER_TEST.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let node_dir = dir.path().join("node");
    let manifest = write(dir.path(), "app.json", &exposed(manifest_queue()));
    let wasm_path = dir.path().join("app.wasm");
    std::fs::write(&wasm_path, guest_wasm()).unwrap();
    let wasm = wasm_path.to_str().unwrap().to_string();
    let Booted {
        mut child,
        endpoint,
    } = serve_node(&node_dir, &[]).await;

    let output = common::authenticated_command()
        .args([
            "deploy",
            "--endpoint",
            &endpoint,
            "--db",
            "db",
            "--module",
            "app",
            "--wasm",
            &wasm,
            "--manifest",
            &manifest,
            "--admin-token",
            "secret",
        ])
        .output()
        .unwrap();
    assert!(output.status.success());

    // A guest mutation enqueues one email.
    let output = common::authenticated_command()
        .args([
            "call",
            "--endpoint",
            &endpoint,
            "--db",
            "db",
            "--module",
            "app",
            "--name",
            "send_email",
            "--mutation",
            "--args",
            "{\"to\": \"ops@example.com\"}",
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    let text = String::from_utf8(output.stdout).unwrap();
    let enqueued = text.lines().next().unwrap().trim_matches('"');
    assert_eq!(enqueued.len(), 32);

    // Stats, peek and poll see the pending item.
    let output = common::authenticated_command()
        .args([
            "queue",
            "--endpoint",
            &endpoint,
            "--db",
            "db",
            "stats",
            "--queue",
            "email",
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    let text = String::from_utf8(output.stdout).unwrap();
    assert!(text.contains("pending=1"), "{text}");

    let output = common::authenticated_command()
        .args([
            "queue",
            "--endpoint",
            &endpoint,
            "--db",
            "db",
            "peek",
            "--queue",
            "email",
            "--id",
            enqueued,
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    let text = String::from_utf8(output.stdout).unwrap();
    assert!(text.contains("ops@example.com"), "{text}");
    assert!(text.contains("\"pending\""), "{text}");

    let output = common::authenticated_command()
        .args([
            "queue",
            "--endpoint",
            &endpoint,
            "--db",
            "db",
            "poll",
            "--queue",
            "email",
            "--limit",
            "10",
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    let text = String::from_utf8(output.stdout).unwrap();
    let claim: serde_json::Value = serde_json::from_str(text.lines().next().unwrap()).unwrap();
    assert_eq!(claim["id"], enqueued);
    let token = claim["token"].as_str().unwrap().to_string();

    // Heartbeat, complete and the drained stats.
    let output = common::authenticated_command()
        .args([
            "queue",
            "--endpoint",
            &endpoint,
            "--db",
            "db",
            "heartbeat",
            "--queue",
            "email",
            "--id",
            enqueued,
            "--token",
            &token,
        ])
        .output()
        .unwrap();
    assert!(output.status.success());

    let output = common::authenticated_command()
        .args([
            "queue",
            "--endpoint",
            &endpoint,
            "--db",
            "db",
            "complete",
            "--queue",
            "email",
            "--id",
            enqueued,
            "--token",
            &token,
        ])
        .output()
        .unwrap();
    assert!(output.status.success());

    let output = common::authenticated_command()
        .args([
            "queue",
            "--endpoint",
            &endpoint,
            "--db",
            "db",
            "stats",
            "--queue",
            "email",
        ])
        .output()
        .unwrap();
    let text = String::from_utf8(output.stdout).unwrap();
    assert!(text.contains("pending=0"), "{text}");
    assert!(text.contains("claimed=0"), "{text}");

    // Double completion fails; peeking the gone item fails too.
    let output = common::authenticated_command()
        .args([
            "queue",
            "--endpoint",
            &endpoint,
            "--db",
            "db",
            "complete",
            "--queue",
            "email",
            "--id",
            enqueued,
            "--token",
            &token,
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());

    let output = common::authenticated_command()
        .args([
            "queue",
            "--endpoint",
            &endpoint,
            "--db",
            "db",
            "peek",
            "--queue",
            "email",
            "--id",
            enqueued,
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());

    child.kill().await.unwrap();
}

#[tokio::test]
async fn timer_inspect_maintain() {
    let _turn = SERVER_TEST.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let mut booted = boot(dir.path(), "node", "3600000").await;
    let endpoint = booted.endpoint.clone();
    let manifest = write(dir.path(), "app.json", &exposed(manifest_timer()));
    let wasm_path = dir.path().join("app.wasm");
    std::fs::write(&wasm_path, guest_wasm()).unwrap();
    let wasm = wasm_path.to_str().unwrap().to_string();

    let output = common::authenticated_command()
        .args([
            "deploy",
            "--endpoint",
            &endpoint,
            "--db",
            "db",
            "--module",
            "app",
            "--wasm",
            &wasm,
            "--manifest",
            &manifest,
            "--admin-token",
            "secret",
        ])
        .output()
        .unwrap();
    assert!(output.status.success());

    // A guest mutation schedules two reminders; the hourly sweep
    // never fires during the test.
    for (id, delay) in [("soon", "0"), ("later", "3600000")] {
        let output = common::authenticated_command()
            .args([
                "call",
                "--endpoint",
                &endpoint,
                "--db",
                "db",
                "--module",
                "app",
                "--name",
                "remind_me",
                "--mutation",
                "--args",
                &format!(
                    "{{\"id\": \"{id}\", \"to\": \"ops@example.com\", \"delay_ms\": {delay}}}"
                ),
            ])
            .output()
            .unwrap();
        assert!(output.status.success());
    }

    let output = common::authenticated_command()
        .args(["timer", "--endpoint", &endpoint, "--db", "db", "stats"])
        .output()
        .unwrap();
    let text = String::from_utf8(output.stdout).unwrap();
    assert!(text.contains("pending=2"), "{text}");

    let output = common::authenticated_command()
        .args([
            "timer",
            "--endpoint",
            &endpoint,
            "--db",
            "db",
            "get",
            "--id",
            "soon",
        ])
        .output()
        .unwrap();
    let text = String::from_utf8(output.stdout).unwrap();
    assert!(text.contains("\"pending\""), "{text}");
    assert!(text.contains("send_email"), "{text}");

    let output = common::authenticated_command()
        .args(["timer", "--endpoint", &endpoint, "--db", "db", "list"])
        .output()
        .unwrap();
    let text = String::from_utf8(output.stdout).unwrap();
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines.len(), 2);
    assert!(lines[0].contains("\"soon\""), "{text}");

    let output = common::authenticated_command()
        .args([
            "timer",
            "--endpoint",
            &endpoint,
            "--db",
            "db",
            "cancel",
            "--id",
            "soon",
        ])
        .output()
        .unwrap();
    let text = String::from_utf8(output.stdout).unwrap();
    assert!(text.contains("removed=true"), "{text}");

    let output = common::authenticated_command()
        .args([
            "timer",
            "--endpoint",
            &endpoint,
            "--db",
            "db",
            "get",
            "--id",
            "soon",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());

    booted.child.kill().await.unwrap();
}

#[tokio::test]
async fn transaction_runs_live() {
    let _turn = SERVER_TEST.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let mut booted = boot(dir.path(), "node", "3600000").await;
    let endpoint = booted.endpoint.clone();
    let manifest = write(dir.path(), "app.json", &exposed(manifest_v1()));
    let wasm_path = dir.path().join("app.wasm");
    std::fs::write(&wasm_path, guest_wasm()).unwrap();
    let wasm = wasm_path.to_str().unwrap().to_string();

    for db in ["dba", "dbb"] {
        let output = common::authenticated_command()
            .args([
                "deploy",
                "--endpoint",
                &endpoint,
                "--db",
                db,
                "--module",
                "app",
                "--wasm",
                &wasm,
                "--manifest",
                &manifest,
                "--admin-token",
                "secret",
            ])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let steps = serde_json::json!([
        {"db": "dba", "module": "app", "method": "put",
         "args": {"key": "\"a\"", "title": "first"}},
        {"db": "dbb", "module": "app", "method": "put",
         "args": {"key": "\"b\"", "title": "second"}},
    ]);
    let output = common::authenticated_command()
        .args([
            "transaction",
            "--endpoint",
            &endpoint,
            "--steps",
            &serde_json::to_string(&steps).unwrap(),
        ])
        .output()
        .unwrap();
    assert!(output.status.success(), "{:?}", output.status);
    let text = String::from_utf8(output.stdout).unwrap();
    assert!(text.contains("put-ok"), "{text}");
    assert!(text.contains("\"dba\":1"), "{text}");
    assert!(text.contains("\"dbb\":1"), "{text}");

    for (db, key, title) in [("dba", "a", "first"), ("dbb", "b", "second")] {
        let output = common::authenticated_command()
            .args([
                "call",
                "--endpoint",
                &endpoint,
                "--db",
                db,
                "--module",
                "app",
                "--name",
                "get",
                "--args",
                &format!("{{\"key\": \"\\\"{key}\\\"\"}}"),
            ])
            .output()
            .unwrap();
        assert!(output.status.success());
        let text = String::from_utf8(output.stdout).unwrap();
        assert!(text.contains(title), "{text}");
    }

    booted.child.kill().await.unwrap();
}

#[tokio::test]
async fn timer_sweep_fires() {
    let _turn = SERVER_TEST.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let mut booted = boot(dir.path(), "node", "100").await;
    let endpoint = booted.endpoint.clone();
    let manifest = write(dir.path(), "app.json", &exposed(manifest_timer()));
    let wasm_path = dir.path().join("app.wasm");
    std::fs::write(&wasm_path, guest_wasm()).unwrap();
    let wasm = wasm_path.to_str().unwrap().to_string();

    let output = common::authenticated_command()
        .args([
            "deploy",
            "--endpoint",
            &endpoint,
            "--db",
            "db",
            "--module",
            "app",
            "--wasm",
            &wasm,
            "--manifest",
            &manifest,
            "--admin-token",
            "secret",
        ])
        .output()
        .unwrap();
    assert!(output.status.success());

    let output = common::authenticated_command()
        .args([
            "call",
            "--endpoint",
            &endpoint,
            "--db",
            "db",
            "--module",
            "app",
            "--name",
            "remind_me",
            "--mutation",
            "--args",
            "{\"id\": \"soon\", \"to\": \"sweep@example.com\", \"delay_ms\": 0}",
        ])
        .output()
        .unwrap();
    assert!(output.status.success());

    // The 100 ms sweep fires the due timer on its own: the email
    // lands in the queue within seconds.
    let mut found = false;
    for _ in 0..300 {
        let output = common::authenticated_command()
            .args([
                "queue",
                "--endpoint",
                &endpoint,
                "--db",
                "db",
                "poll",
                "--queue",
                "email",
                "--limit",
                "1",
            ])
            .output()
            .unwrap();
        let text = String::from_utf8(output.stdout).unwrap();
        if text.contains("sweep@example.com") {
            found = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(found, "sweep never fired the due timer");

    let output = common::authenticated_command()
        .args(["timer", "--endpoint", &endpoint, "--db", "db", "stats"])
        .output()
        .unwrap();
    let text = String::from_utf8(output.stdout).unwrap();
    assert!(text.contains("pending=0"), "{text}");

    booted.child.kill().await.unwrap();
}

/// Spawn `serve` for node 1 in `node_dir` with `extra` flags and wait
/// until it elects itself.
async fn serve_node(node_dir: &std::path::Path, extra: &[&str]) -> Booted {
    serve_member(node_dir, true, extra, &[]).await
}

/// [`serve_node`] with environment variables set for the node.
async fn serve_node_with(
    node_dir: &std::path::Path,
    extra: &[&str],
    envs: &[(&str, &str)],
) -> Booted {
    serve_member(node_dir, true, extra, envs).await
}

/// [`serve_node`], mounting a wrapping key only when `keyed`, with
/// environment variables set for the node.
async fn serve_member(
    node_dir: &std::path::Path,
    keyed: bool,
    extra: &[&str],
    envs: &[(&str, &str)],
) -> Booted {
    let wrapping = wrapping_key(node_dir.parent().unwrap());
    let mut args = Vec::new();
    if keyed {
        args.extend(["--key-wrapping-file", wrapping.as_str()]);
    }
    args.extend_from_slice(extra);
    Booted::start(node_dir, &args, envs).await
}

/// A member without a wrapping key builds its guests with a crypto host
/// all the same, over a locked catalog: it refuses a module whose key
/// declarations conflict, as a member with the key does (so no member
/// halts applying a deployment another accepted), its guests that import
/// the crypto host run, and their catalog keys answer as business
/// failures rather than traps.
#[tokio::test]
async fn a_member_without_a_wrapping_key_validates_and_runs_guests_alike() {
    let _turn = SERVER_TEST.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let booted = serve_member(&dir.path().join("node"), false, &[], &[]).await;
    let endpoint = booted.endpoint.clone();
    let wasm_path = dir.path().join("app.wasm");
    std::fs::write(&wasm_path, guest_wasm()).unwrap();
    let wasm = wasm_path.to_str().unwrap().to_string();
    let deploy = |manifest: &str| {
        Command::new(bin())
            .env("FLOWER_ADMIN_TOKEN", "secret")
            .args([
                "deploy",
                "--endpoint",
                &endpoint,
                "--db",
                "db",
                "--module",
                "app",
                "--wasm",
                &wasm,
                "--manifest",
                manifest,
                "--admin-token",
                "secret",
            ])
            .output()
            .unwrap()
    };
    let call = |name: &str, args: &str| {
        Command::new(bin())
            .env("FLOWER_WORKER_TOKEN", "secret")
            .args([
                "call",
                "--endpoint",
                &endpoint,
                "--db",
                "db",
                "--module",
                "app",
                "--name",
                name,
                "--mutation",
                "--args",
                args,
            ])
            .output()
            .unwrap()
    };

    let mut conflicting = exposed(manifest_v1());
    conflicting["keys"] = serde_json::json!([
        {"name": "deploy", "algorithm": "Ed25519", "usages": ["sign"]},
        {"name": "deploy", "algorithm": "P256", "usages": ["sign"]},
    ]);
    let refused = deploy(&write(dir.path(), "conflicting.json", &conflicting));
    let stderr = String::from_utf8_lossy(&refused.stderr);
    assert!(!refused.status.success(), "{stderr}");
    assert!(stderr.contains("invalid_module"), "{stderr}");
    assert!(stderr.contains("conflicting declarations"), "{stderr}");

    let mut declared = exposed(manifest_v1());
    declared["keys"] = serde_json::json!([{"name": "deploy", "algorithm": "Ed25519", "usages": ["sign", "verify"]}]);
    let deployed = deploy(&write(dir.path(), "app.json", &declared));
    assert!(
        deployed.status.success(),
        "{}",
        String::from_utf8_lossy(&deployed.stderr)
    );
    let put = call("put", "{\"key\": \"a\", \"title\": \"hi\"}");
    assert!(
        String::from_utf8_lossy(&put.stdout).contains("put-ok"),
        "{}",
        String::from_utf8_lossy(&put.stderr)
    );
    // Nothing is provisioned (nothing can be, without the key): the
    // alias is unbound.
    let signed = call("sign_message", "{\"key\": \"deploy\", \"message\": \"m\"}");
    let stderr = String::from_utf8_lossy(&signed.stderr);
    assert!(!signed.status.success(), "{stderr}");
    assert!(stderr.contains("UNKNOWN_KEY"), "{stderr}");
    drop(booted);
}

/// Run the CLI with the admin token, asserting success; its stdout.
fn cli(args: &[&str]) -> String {
    cli_with(args, &[])
}

/// [`cli`] with environment variables set.
fn cli_with(args: &[&str], envs: &[(&str, &str)]) -> String {
    let output = Command::new(bin())
        .env("FLOWER_ADMIN_TOKEN", "secret")
        .envs(envs.iter().copied())
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

/// One `key=value` field of a CLI output line.
fn field<'a>(line: &'a str, name: &str) -> &'a str {
    line.split_whitespace()
        .find_map(|pair| pair.strip_prefix(name)?.strip_prefix('='))
        .unwrap_or_else(|| panic!("no {name} in {line}"))
}

#[tokio::test]
async fn serve_backs_up_continuously_and_restores_by_index() {
    let _turn = SERVER_TEST.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let backups = dir.path().join("backups");
    backs_up_continuously_and_restores_by_index(&["--backup-dir", backups.to_str().unwrap()], &[])
        .await;
}

/// `serve --backup-url s3://…` backs up to S3, reading its endpoint and
/// credentials from the environment, and `backup list` and `backup restore`
/// read it back from there.
#[tokio::test(flavor = "multi_thread")]
async fn serve_backs_up_continuously_to_s3_and_restores_by_index() {
    let _turn = SERVER_TEST.lock().await;
    let root = tempfile::tempdir().unwrap();
    let endpoint = serve_s3(root.path()).await;
    let envs = [
        ("FLOWER_BACKUP_S3_ENDPOINT", endpoint.as_str()),
        ("FLOWER_BACKUP_S3_REGION", S3_REGION),
        ("FLOWER_BACKUP_S3_ACCESS_KEY_ID", S3_ACCESS_KEY),
        ("FLOWER_BACKUP_S3_SECRET_ACCESS_KEY", S3_SECRET_KEY),
    ];
    let url = format!("s3://{S3_BUCKET}/clusters/one");
    backs_up_continuously_and_restores_by_index(&["--backup-url", &url], &envs).await;
    // Every object went below the URL's prefix.
    let manifests = root
        .path()
        .join(S3_BUCKET)
        .join("clusters/one/flower-backup/v1/manifest");
    assert_eq!(std::fs::read_dir(manifests).unwrap().count(), 1);
}

const S3_ACCESS_KEY: &str = "testadmin";
const S3_SECRET_KEY: &str = "testadmin-secret";
const S3_BUCKET: &str = "flowertest";
const S3_REGION: &str = "us-east-1";

/// Serve S3 in-process from a directory, with the bucket made; its
/// endpoint URL. It serves until the test's runtime drops.
async fn serve_s3(root: &std::path::Path) -> String {
    use hyper_util::rt::{TokioExecutor, TokioIo};
    use hyper_util::server::conn::auto::Builder as ConnBuilder;
    std::fs::create_dir_all(root.join(S3_BUCKET)).unwrap();
    let mut builder = s3s::service::S3ServiceBuilder::new(s3s_fs::FileSystem::new(root).unwrap());
    builder.set_auth(s3s::auth::SimpleAuth::from_single(
        S3_ACCESS_KEY,
        S3_SECRET_KEY,
    ));
    let service = builder.build();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        let http = ConnBuilder::new(TokioExecutor::new());
        while let Ok((socket, _)) = listener.accept().await {
            let conn = http
                .serve_connection(TokioIo::new(socket), service.clone())
                .into_owned();
            tokio::spawn(async move {
                let _ = conn.await;
            });
        }
    });
    endpoint
}

/// Serve one node backing up continuously to `location` (its flags,
/// with `envs` set for every command), write, then restore exactly
/// through a log index from the archive into a new node and serve it.
async fn backs_up_continuously_and_restores_by_index(location: &[&str], envs: &[(&str, &str)]) {
    let cli = |args: &[&str]| cli_with(args, envs);
    let dir = tempfile::tempdir().unwrap();
    let manifest = write(dir.path(), "app.json", &exposed(manifest_v1()));
    let wasm_path = dir.path().join("app.wasm");
    std::fs::write(&wasm_path, guest_wasm()).unwrap();
    let kek = dir.path().join("kek.bin");
    std::fs::write(&kek, [7u8; 32]).unwrap();
    let keystore = dir.path().join("keystore.json");
    let mut store = location.to_vec();
    store.extend([
        "--keystore",
        keystore.to_str().unwrap(),
        "--kek-file",
        kek.to_str().unwrap(),
    ]);
    let mut serve_flags = store.to_vec();
    serve_flags.extend(["--backup-archive-ms", "50"]);
    let booted = serve_node_with(&dir.path().join("node"), &serve_flags, envs).await;
    let endpoint = booted.endpoint.clone();

    // Continuous backup takes its first base once the node establishes
    // its history; later writes reach the archive only.
    let list = || cli(&[&["backup", "list"], &store[..]].concat());
    let mut base = String::new();
    for _ in 0..200 {
        base = list();
        if !base.trim().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(base.lines().count(), 1, "{base}");
    cli(&[
        "deploy",
        "--endpoint",
        &endpoint,
        "--db",
        "db",
        "--module",
        "app",
        "--wasm",
        wasm_path.to_str().unwrap(),
        "--manifest",
        &manifest,
    ]);
    let mut positions = Vec::new();
    for key in ["a", "b", "c"] {
        let args = format!("{{\"key\": \"{key}\", \"title\": \"title-{key}\"}}");
        let out = cli(&[
            "call",
            "--endpoint",
            &endpoint,
            "--db",
            "db",
            "--module",
            "app",
            "--name",
            "put",
            "--mutation",
            "--args",
            &args,
        ]);
        let line = out.lines().find(|line| line.contains("position=")).unwrap();
        positions.push(field(line, "position").parse::<u64>().unwrap());
    }
    let mut listed = String::new();
    for _ in 0..200 {
        listed = list();
        let through: u64 = field(listed.lines().next().unwrap(), "restorable_through")
            .parse()
            .unwrap();
        if through >= positions[2] {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let line = listed.lines().next().unwrap().to_string();
    assert_eq!(
        listed.lines().count(),
        1,
        "one base, then the archive: {listed}"
    );
    assert!(
        field(&line, "restorable_through").parse::<u64>().unwrap() >= positions[2],
        "{line}"
    );
    assert_ne!(field(&line, "restorable_through_ms"), "-");
    let mut child = booted.child;
    child.kill().await.unwrap();
    child.wait().await.unwrap();

    // Restore exactly through B into a new node directory and serve it.
    let restored = dir.path().join("restored");
    let index = positions[1].to_string();
    let out = cli(&[
        &[
            "backup",
            "restore",
            "--dir",
            restored.to_str().unwrap(),
            "--index",
            &index,
        ],
        &store[..],
    ]
    .concat());
    let line = out
        .lines()
        .find(|line| line.starts_with("restored"))
        .unwrap();
    assert_eq!(field(line, "index"), index);
    assert_eq!(field(line, "history"), field(&base, "history"));
    let booted = serve_node(&restored, &[]).await;
    let get = |key: &str| {
        cli(&[
            "call",
            "--endpoint",
            &booted.endpoint,
            "--db",
            "db",
            "--module",
            "app",
            "--name",
            "get",
            "--args",
            &format!("{{\"key\": \"{key}\"}}"),
        ])
    };
    assert!(get("a").contains("title-a"));
    assert!(get("b").contains("title-b"));
    assert!(!get("c").contains("title-c"));
    let mut child = booted.child;
    child.kill().await.unwrap();
    child.wait().await.unwrap();

    // Without a target, the newest state the backups hold: the base and every
    // archived entry after it.
    let newest = dir.path().join("newest");
    let out = cli(&[
        &["backup", "restore", "--dir", newest.to_str().unwrap()],
        &store[..],
    ]
    .concat());
    assert!(
        out.lines().any(|line| line.starts_with("restored")),
        "{out}"
    );
    let booted = serve_node(&newest, &[]).await;
    let get = |key: &str| {
        cli(&[
            "call",
            "--endpoint",
            &booted.endpoint,
            "--db",
            "db",
            "--module",
            "app",
            "--name",
            "get",
            "--args",
            &format!("{{\"key\": \"{key}\"}}"),
        ])
    };
    assert!(get("c").contains("title-c"));
    let mut child = booted.child;
    child.kill().await.unwrap();
}

/// Run the CLI with the admin token as a trusted gateway's credential;
/// its exit status, stdout and stderr.
fn retry_cli(args: &[&str]) -> (bool, String, String) {
    let output = Command::new(bin())
        .env("FLOWER_ADMIN_TOKEN", "secret")
        .env("FLOWER_TOKEN", "secret")
        .args(args)
        .output()
        .unwrap();
    (
        output.status.success(),
        String::from_utf8(output.stdout).unwrap(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

/// `serve` gives a deployed database default retention by itself (a 24-hour
/// window: epochs of three hours, nine kept), and the command line runs retry
/// sessions end to end: open one under a fresh ID, send a sequence under its
/// `f2` request ID (a retry says `duplicate=true`), acknowledge it (it never
/// runs again) and close it; a byte budget then refuses new results.
#[tokio::test]
async fn retry_sessions_from_the_command_line() {
    let _turn = SERVER_TEST.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let mut booted = serve_node(&dir.path().join("node"), &[]).await;
    let endpoint = booted.endpoint.clone();
    let manifest = write(dir.path(), "app.json", &exposed(manifest_v1()));
    let wasm_path = dir.path().join("app.wasm");
    std::fs::write(&wasm_path, guest_wasm()).unwrap();
    cli(&[
        "deploy",
        "--endpoint",
        &endpoint,
        "--db",
        "d",
        "--module",
        "app",
        "--wasm",
        wasm_path.to_str().unwrap(),
        "--manifest",
        &manifest,
    ]);
    let retry = |extra: &[&str]| {
        let mut args = vec!["retry", "--endpoint", &endpoint, "--db", "d"];
        args.extend_from_slice(extra);
        let (ok, stdout, stderr) = retry_cli(&args);
        assert!(ok, "{extra:?}: {stderr}");
        stdout
    };
    let mut identity = serde_json::Value::Null;
    for _ in 0..100 {
        identity = serde_json::from_str(&retry(&[])).unwrap();
        if identity["initialized"] == true {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(identity["initialized"], true, "{identity}");
    assert_eq!(identity["epoch_ms"], 10_800_000);
    let opened: serde_json::Value =
        serde_json::from_str(&retry(&["open", "--module", "app"])).unwrap();
    let session = opened["session"].as_str().unwrap().to_string();
    assert_eq!(session.len(), 32);
    let first = retry(&[
        "session-id",
        "--module",
        "app",
        "--session",
        &session,
        "--sequence",
        "1",
    ]);
    let first = first.trim();
    assert!(first.starts_with("f2:"), "{first}");
    let put = |request_id: &str| {
        retry_cli(&[
            "call",
            "--endpoint",
            &endpoint,
            "--db",
            "d",
            "--module",
            "app",
            "--name",
            "put",
            "--args",
            "{\"key\": \"\\\"a\\\"\", \"title\": \"first\"}",
            "--mutation",
            "--request-id",
            request_id,
        ])
    };
    let (ok, original, stderr) = put(first);
    assert!(ok, "{stderr}");
    assert!(original.contains("duplicate=false"), "{original}");
    let (ok, again, _) = put(first);
    assert!(ok);
    assert!(again.contains("duplicate=true"), "{again}");
    let acked: serde_json::Value = serde_json::from_str(&retry(&[
        "ack",
        "--module",
        "app",
        "--session",
        &session,
        "--through",
        "1",
    ]))
    .unwrap();
    assert_eq!(acked["acknowledged"], 1);
    let (ok, _, stderr) = put(first);
    assert!(!ok);
    assert!(stderr.contains("already_acknowledged"), "{stderr}");
    let closed: serde_json::Value =
        serde_json::from_str(&retry(&["close", "--module", "app", "--session", &session])).unwrap();
    assert_eq!(closed["closed"], true);
    // A budget of one byte refuses any new result.
    let budget = cli(&[
        "retention",
        "--endpoint",
        &endpoint,
        "--db",
        "d",
        "budget",
        "--max-bytes",
        "1",
    ]);
    assert!(budget.contains("\"max_receipt_bytes\": 1"), "{budget}");
    let order = retry(&["id", "--intent", "order:1"]);
    let order = order.trim();
    assert!(order.starts_with("f1:"), "{order}");
    let (ok, _, stderr) = put(order);
    assert!(!ok);
    assert!(stderr.contains("receipt_budget_exceeded"), "{stderr}");
    booted.child.kill().await.unwrap();
}
