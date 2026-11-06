//! `cargo flower build`, `plan` and preflighted `deploy` over the
//! example guests, and the crypto-ticket example end to end against a
//! served node.

mod common;

use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::Duration;

use sha2::Digest;
use tokio::io::{AsyncBufReadExt, BufReader};

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_cargo-flower")
}

/// The workspace root.
fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf()
}

/// Run the CLI from the workspace root, building guests in the shared
/// wasm target folder, offline.
fn cli(args: &[&str]) -> Output {
    Command::new(bin())
        .args(args)
        .current_dir(root())
        .env("CARGO_TARGET_DIR", root().join("target").join("wasm-guest"))
        .env("CARGO_NET_OFFLINE", "true")
        .env("FLOWER_ADMIN_TOKEN", "secret")
        .env("FLOWER_WORKER_TOKEN", "secret")
        .stdin(Stdio::null())
        .output()
        .unwrap()
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

/// Build an example into `out`, returning its Wasm and manifest paths.
fn build(package: &str, out: &Path) -> (PathBuf, PathBuf) {
    let output = cli(&[
        "build",
        "--package",
        package,
        "--out-dir",
        out.to_str().unwrap(),
    ]);
    assert!(output.status.success(), "{}", stderr(&output));
    let text = stdout(&output);
    assert!(text.contains(&format!("built {package} ")), "{text}");
    let wasm = out.join(format!("{package}.wasm"));
    let manifest = out.join(format!("{package}.manifest.json"));
    let digest: String = sha2::Sha256::digest(std::fs::read(&wasm).unwrap())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    assert!(text.contains(&format!("sha256={digest}")), "{text}");
    (wasm, manifest)
}

#[test]
fn build_writes_inspected_artifacts_and_plan_checks_them() {
    let out = tempfile::tempdir().unwrap();
    let (wasm, manifest) = build("crypto-ticket", out.path());
    let declared: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&manifest).unwrap()).unwrap();
    let names: Vec<&str> = declared["definitions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|definition| definition["name"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"ticket.issue"), "{names:?}");
    // A workspace with several guests needs --package.
    let output = cli(&["build", "--out-dir", out.path().to_str().unwrap()]);
    assert!(!output.status.success());
    assert!(
        stderr(&output).contains("select exactly one"),
        "{}",
        stderr(&output)
    );
    // Plan: the module alone, then against a compatible and an
    // incompatible deployed contract.
    let wasm = wasm.to_str().unwrap();
    let output = cli(&["plan", "--wasm", wasm]);
    assert!(output.status.success(), "{}", stderr(&output));
    let text = stdout(&output);
    assert!(text.contains("mutations=2 "), "{text}");
    assert!(text.contains("compatible=true"), "{text}");
    let output = cli(&[
        "plan",
        "--wasm",
        wasm,
        "--current",
        manifest.to_str().unwrap(),
    ]);
    assert!(output.status.success(), "{}", stderr(&output));
    let (other, _) = build("external-transactions", out.path());
    let output = cli(&["plan", "--wasm", wasm, "--current", other.to_str().unwrap()]);
    assert!(!output.status.success());
    let text = stdout(&output);
    assert!(text.contains("! collection docs removed"), "{text}");
    assert!(text.contains("compatible=false"), "{text}");
    // A manifest other than the module's own is flagged.
    let edited = out.path().join("edited.json");
    let mut changed = declared.clone();
    changed["http"] = serde_json::json!([]);
    std::fs::write(&edited, changed.to_string()).unwrap();
    let output = cli(&[
        "plan",
        "--wasm",
        wasm,
        "--manifest",
        edited.to_str().unwrap(),
    ]);
    assert!(output.status.success(), "{}", stderr(&output));
    assert!(stdout(&output).contains("differs from the one the module declares"));
    // Garbage fails inspection.
    let garbage = out.path().join("garbage.wasm");
    std::fs::write(&garbage, b"\0asm not really").unwrap();
    let output = cli(&["plan", "--wasm", garbage.to_str().unwrap()]);
    assert!(!output.status.success());
}

/// Boot a single node with a replicated key catalog (its wrapping key
/// mounted) and return it with its endpoint once it leads.
async fn serve(dir: &Path) -> (tokio::process::Child, String) {
    let wrapping = dir.join("wrapping.key");
    common::write_key(&wrapping, &[7u8; 32]);
    let mut child = tokio::process::Command::new(bin())
        .args([
            "serve",
            "--dir",
            dir.join("node").to_str().unwrap(),
            "--id",
            "1",
            "--http",
            "127.0.0.1:0",
            "--raft",
            "127.0.0.1:0",
            "--bootstrap",
            "1",
            "--admin-token",
            "secret",
            "--key-wrapping-file",
            wrapping.to_str().unwrap(),
            "--timer-sweep-ms",
            "3600000",
        ])
        .env("FLOWER_WORKER_TOKEN", "secret")
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
    let http = tokio::time::timeout(Duration::from_secs(60), async {
        while let Some(line) = lines.next_line().await.unwrap() {
            if let Some(addr) = line.strip_prefix("listening http=") {
                return addr.to_string();
            }
        }
        panic!("serve exited before listening");
    })
    .await
    .unwrap();
    let endpoint = format!("http://{http}");
    for _ in 0..200 {
        let output = cli(&["status", "--endpoint", &endpoint]);
        if output.status.success() && stdout(&output).contains("leader=Some(1)") {
            return (child, endpoint);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("the node never led");
}

/// Call the crypto example, returning its printed value.
fn call(endpoint: &str, name: &str, args: serde_json::Value, mutation: bool) -> Output {
    let args = args.to_string();
    let mut argv = vec![
        "call",
        "--endpoint",
        endpoint,
        "--db",
        "db",
        "--module",
        "booth",
        "--name",
        name,
        "--args",
        &args,
        "--token",
        "secret",
    ];
    if mutation {
        argv.push("--mutation");
    }
    cli(&argv)
}

fn value(output: &Output) -> serde_json::Value {
    assert!(output.status.success(), "{}", stderr(output));
    serde_json::from_str(stdout(output).lines().next().unwrap()).unwrap()
}

#[tokio::test]
async fn deploy_preflights_built_artifacts_and_the_ticket_booth_runs() {
    let dir = tempfile::tempdir().unwrap();
    let (wasm, _) = build("crypto-ticket", dir.path());
    let (_child, endpoint) = serve(dir.path()).await;
    // Garbage never leaves the machine.
    let garbage = dir.path().join("garbage.wasm");
    std::fs::write(&garbage, b"\0asm\x01\0\0\0junk").unwrap();
    let deploy = |wasm: &Path| {
        cli(&[
            "deploy",
            "--endpoint",
            &endpoint,
            "--db",
            "db",
            "--module",
            "booth",
            "--wasm",
            wasm.to_str().unwrap(),
        ])
    };
    let output = deploy(&garbage);
    assert!(!output.status.success());
    assert!(
        stderr(&output).contains("nothing was sent"),
        "{}",
        stderr(&output)
    );
    // The built artifact deploys with the manifest it declares.
    let output = deploy(&wasm);
    assert!(output.status.success(), "{}", stderr(&output));
    // The alias the booth declares, bound to a catalog key.
    let keys = |args: &[&str]| {
        let mut argv = vec!["keys", "--endpoint", &endpoint, "--db", "db"];
        argv.extend_from_slice(args);
        let output = cli(&argv);
        assert!(
            output.status.success(),
            "keys {args:?}: {}",
            stderr(&output)
        );
    };
    keys(&["generate", "--name", "booth", "--algorithm", "Ed25519"]);
    keys(&[
        "bind",
        "--alias",
        "tickets",
        "--key",
        "booth",
        "--usages",
        "sign,verify",
    ]);
    let issued = value(&call(
        &endpoint,
        "ticket.issue",
        serde_json::json!({"guest": "ada", "lifetimeSeconds": 60}),
        true,
    ));
    let ticket = issued["ticket"].as_str().unwrap().to_string();
    let jti = issued["claims"]["jti"].as_str().unwrap().to_string();
    assert_eq!(jti.len(), 32);
    // A compact JWS: header, claims, signature. The header names EdDSA.
    assert_eq!(ticket.split('.').count(), 3, "{ticket}");
    assert!(ticket.starts_with("eyJ"), "{ticket}");
    let claims = value(&call(
        &endpoint,
        "ticket.check",
        serde_json::json!({"ticket": ticket}),
        false,
    ));
    assert_eq!(claims["sub"], "ada");
    assert_eq!(claims["aud"], "greenhouse");
    // A forged ticket: one claim byte flipped.
    let (body, signature) = ticket.split_once('.').unwrap();
    let mut forged = body.as_bytes().to_vec();
    let last = forged.len() - 1;
    forged[last] = if forged[last] == b'0' { b'1' } else { b'0' };
    let forged = format!("{}.{signature}", String::from_utf8(forged).unwrap());
    let output = call(
        &endpoint,
        "ticket.check",
        serde_json::json!({"ticket": forged}),
        false,
    );
    assert!(!output.status.success());
    assert!(
        stderr(&output).contains("INVALID_TICKET"),
        "{}",
        stderr(&output)
    );
    // Revoked tickets stop checking.
    let revoked = value(&call(
        &endpoint,
        "ticket.revoke",
        serde_json::json!({"jti": jti}),
        true,
    ));
    assert_eq!(revoked, true);
    let output = call(
        &endpoint,
        "ticket.check",
        serde_json::json!({"ticket": ticket}),
        false,
    );
    assert!(
        stderr(&output).contains("TICKET_REVOKED"),
        "{}",
        stderr(&output)
    );
    // Rotation: tickets signed by the old version still check, until
    // that version is revoked.
    let older = value(&call(
        &endpoint,
        "ticket.issue",
        serde_json::json!({"guest": "grace", "lifetimeSeconds": 60}),
        true,
    ))["ticket"]
        .as_str()
        .unwrap()
        .to_string();
    keys(&["rotate", "--name", "booth"]);
    let check = |ticket: &str| {
        call(
            &endpoint,
            "ticket.check",
            serde_json::json!({"ticket": ticket}),
            false,
        )
    };
    assert_eq!(value(&check(&older))["sub"], "grace");
    keys(&["revoke", "--name", "booth", "--version", "1"]);
    let output = check(&older);
    assert!(
        stderr(&output).contains("INVALID_TICKET"),
        "{}",
        stderr(&output)
    );
    // Host digests match, and queries never get entropy.
    let digest = value(&call(
        &endpoint,
        "crypto.digest",
        serde_json::json!({"text": "flower"}),
        false,
    ));
    let expected: String = sha2::Sha256::digest(b"flower")
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    assert_eq!(digest, expected);
    let output = call(
        &endpoint,
        "crypto.randomQuery",
        serde_json::json!({}),
        false,
    );
    assert!(!output.status.success());
    assert!(
        stderr(&output).contains("INVALID_INVOCATION"),
        "{}",
        stderr(&output)
    );
}

/// Call a module in `db` with the client token.
fn call_in(
    endpoint: &str,
    module: &str,
    name: &str,
    args: serde_json::Value,
    mutation: bool,
) -> serde_json::Value {
    let args = args.to_string();
    let mut argv = vec![
        "call",
        "--endpoint",
        endpoint,
        "--db",
        "db",
        "--module",
        module,
        "--name",
        name,
        "--args",
        &args,
        "--token",
        "secret",
    ];
    if mutation {
        argv.push("--mutation");
    }
    value(&cli(&argv))
}

/// v1's typed-modules pair: the consumer reaches the provider only
/// through the `tasks.v1` contract and its `$tasks` slot, which
/// `deploy --bind` binds to a deployed module inside the manifest the
/// deployment's identity covers.
#[tokio::test]
async fn deploy_binds_slots_to_deployed_modules() {
    let dir = tempfile::tempdir().unwrap();
    let (provider, _) = build("typed-modules-provider", dir.path());
    let (consumer, manifest) = build("typed-modules-consumer", dir.path());
    let (provider, consumer) = (provider.to_str().unwrap(), consumer.to_str().unwrap());
    // The artifact keeps the slot unbound; bindings are per deployment.
    let declared: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&manifest).unwrap()).unwrap();
    assert_eq!(declared["imports"][0]["module"], "$tasks");
    assert!(declared.get("bindings").is_none(), "{declared}");
    let (_child, endpoint) = serve(dir.path()).await;
    let deploy = |module: &str, wasm: &str, binds: &[&str]| {
        let mut argv = vec![
            "deploy",
            "--endpoint",
            &endpoint,
            "--db",
            "db",
            "--module",
            module,
            "--wasm",
            wasm,
        ];
        for bind in binds {
            argv.extend(["--bind", bind]);
        }
        cli(&argv)
    };
    // Unbound, or bound beyond its imports: refused, nothing sent.
    for binds in [&[][..], &["tasks=board", "other=board"][..]] {
        let output = deploy("planner", consumer, binds);
        assert!(!output.status.success());
        let text = stderr(&output);
        assert!(text.contains("nothing was sent"), "{text}");
    }
    assert!(stderr(&deploy("planner", consumer, &[])).contains("slot `$tasks` is not bound"));
    let output = deploy("planner", consumer, &["tasks=board", "tasks=other"]);
    assert!(
        stderr(&output).contains("bound to both"),
        "{}",
        stderr(&output)
    );
    // The provider, then the consumer bound to it, with the identity the
    // plan predicted.
    let output = deploy("board", provider, &[]);
    assert!(output.status.success(), "{}", stderr(&output));
    let plan = cli(&["plan", "--wasm", consumer, "--bind", "tasks=board"]);
    assert!(plan.status.success(), "{}", stderr(&plan));
    let planned = stdout(&plan);
    assert!(planned.contains("bindings: tasks=board"), "{planned}");
    let identity = planned
        .lines()
        .find_map(|line| line.strip_prefix("identity="))
        .unwrap()
        .to_string();
    let unbound = cli(&["plan", "--wasm", consumer, "--bind", "tasks=elsewhere"]);
    assert!(
        !stdout(&unbound).contains(&identity),
        "bindings change identity"
    );
    // Offline link checks against the database's other modules: the
    // provider satisfies the contract; a module that doesn't export it,
    // or a binding to a module that isn't there, fails the plan.
    let link = |bind: &str, with: &str| {
        cli(&[
            "plan", "--wasm", consumer, "--module", "planner", "--bind", bind, "--with", with,
        ])
    };
    let linked = link("tasks=board", &format!("board={provider}"));
    assert!(linked.status.success(), "{}", stderr(&linked));
    assert!(
        stdout(&linked).contains("links=ok modules=1"),
        "{}",
        stdout(&linked)
    );
    for (bind, with) in [
        ("tasks=board", format!("board={consumer}")),
        ("tasks=nobody", format!("board={provider}")),
    ] {
        let refused = link(bind, &with);
        assert!(!refused.status.success());
        assert!(
            stdout(&refused).contains("! links: "),
            "{}",
            stdout(&refused)
        );
    }
    // The server checks links too: a binding to a module that isn't
    // deployed is refused there.
    let output = deploy("planner", consumer, &["tasks=nobody"]);
    assert!(!output.status.success());
    assert!(
        stderr(&output).contains("MODULE_CONFLICT"),
        "{}",
        stderr(&output)
    );
    let output = deploy("planner", consumer, &["tasks=board"]);
    assert!(output.status.success(), "{}", stderr(&output));
    let listed = cli(&["modules", "--endpoint", &endpoint, "--db", "db"]);
    assert!(listed.status.success(), "{}", stderr(&listed));
    assert!(
        stdout(&listed).contains(&format!("module=planner identity={identity}")),
        "{}",
        stdout(&listed)
    );
    // Calls cross the slot; the provider's declared refusal comes back
    // decoded.
    let task = serde_json::json!({"id": "t1", "owner": "ada", "title": "bake"});
    assert_eq!(
        call_in(&endpoint, "planner", "assign", task.clone(), true),
        "assigned"
    );
    assert_eq!(
        call_in(&endpoint, "planner", "complete", task.clone(), true),
        "assigned"
    );
    assert_eq!(call_in(&endpoint, "planner", "assign", task, true), "done");
    let shown = call_in(
        &endpoint,
        "planner",
        "show",
        serde_json::json!({"id": "t1"}),
        false,
    );
    assert_eq!(shown["done"], true);
    assert_eq!(shown["owner"], "ada");
    assert_eq!(
        call_in(&endpoint, "planner", "count", serde_json::json!({}), false),
        1
    );
    let direct = call_in(
        &endpoint,
        "board",
        "get_task",
        serde_json::json!({"id": "t1"}),
        false,
    );
    assert_eq!(direct["title"], "bake");
}
