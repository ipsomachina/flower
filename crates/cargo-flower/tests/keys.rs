//! `cargo flower serve` mounts the catalogs' wrapping key: catalog
//! changes seal under it, and a malformed key file refuses to boot.
//! `cargo flower keys` drives a database's catalog through its
//! lifecycle against it, and `cargo flower retention` reads a
//! database's retention.

use std::process::{Command, Stdio};
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

fn bin() -> String {
    env!("CARGO_BIN_EXE_cargo-flower").to_string()
}

fn serve_args<'a>(dir: &'a str, wrapping: &'a str) -> Vec<&'a str> {
    vec![
        "serve",
        "--dir",
        dir,
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
        wrapping,
    ]
}

/// POST JSON over plain HTTP/1.1, returning the status and body.
async fn post(addr: &str, path: &str, body: &str) -> (u16, String) {
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let request = format!(
        "POST {path} HTTP/1.1\r\nHost: {addr}\r\nAuthorization: Bearer secret\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    let status = response
        .split(' ')
        .nth(1)
        .and_then(|status| status.parse().ok())
        .unwrap_or(0);
    let body = response
        .split_once("\r\n\r\n")
        .map(|(_, body)| body.to_string())
        .unwrap_or_default();
    (status, body)
}

#[tokio::test]
async fn serve_seals_catalog_keys_under_the_mounted_wrapping_key() {
    let dir = tempfile::tempdir().unwrap();
    // A key file that is not 32 bytes refuses to boot.
    let short = dir.path().join("short.key");
    std::fs::write(&short, [1u8; 16]).unwrap();
    let refused = Command::new(bin())
        .args(serve_args(
            dir.path().join("refused").to_str().unwrap(),
            short.to_str().unwrap(),
        ))
        .output()
        .unwrap();
    assert!(!refused.status.success());

    let wrapping = dir.path().join("wrapping.key");
    std::fs::write(&wrapping, [6u8; 32]).unwrap();
    let node_dir = dir.path().join("node");
    let mut child = tokio::process::Command::new(bin())
        .args(serve_args(
            node_dir.to_str().unwrap(),
            wrapping.to_str().unwrap(),
        ))
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
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

    let generate = r#"{"db":"db","operation":"generate","requestId":"g","name":"signing","algorithm":"Ed25519"}"#;
    let mut response = (0, String::new());
    for _ in 0..200 {
        response = post(&http, "/v1/admin/keys", generate).await;
        if response.0 == 200 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(response.0, 200, "{}", response.1);
    let created: serde_json::Value = serde_json::from_str(&response.1).unwrap();
    let mounted = flower_crypto::replicated::WrappingKey::new(&[6u8; 32]);
    assert_eq!(
        created["value"]["keys"]["signing"]["versions"][0]["wrappingId"],
        mounted.id()
    );

    // A request that names no database is refused: every database
    // keeps a catalog of its own.
    let unnamed = post(
        &http,
        "/v1/admin/keys",
        r#"{"operation":"generate","requestId":"g","name":"signing","algorithm":"Ed25519"}"#,
    )
    .await;
    assert_eq!(unnamed.0, 400, "{}", unnamed.1);

    // The same catalog through `cargo flower keys`.
    let endpoint = format!("http://{http}");
    let keys = |args: &[&str]| {
        let mut command = vec![
            "keys",
            "--endpoint",
            &endpoint,
            "--admin-token",
            "secret",
            "--db",
            "db",
        ];
        command.extend_from_slice(args);
        let output = Command::new(bin()).args(&command).output().unwrap();
        assert!(
            output.status.success(),
            "keys {args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap()
    };
    let listed = keys(&[]);
    assert!(
        listed.contains("key=signing algorithm=Ed25519 active=1 versions=1:live"),
        "{listed}"
    );
    // Without `--db`, every database's catalog, and changes refused.
    let every = Command::new(bin())
        .args(["keys", "--endpoint", &endpoint, "--admin-token", "secret"])
        .output()
        .unwrap();
    let every = String::from_utf8(every.stdout).unwrap();
    assert!(every.contains("db=db revision=1"), "{every}");
    assert!(every.contains("key=signing algorithm=Ed25519"), "{every}");
    let unnamed = Command::new(bin())
        .args([
            "keys",
            "--endpoint",
            &endpoint,
            "--admin-token",
            "secret",
            "rotate",
            "--name",
            "signing",
        ])
        .output()
        .unwrap();
    assert!(!unnamed.status.success());
    assert!(
        String::from_utf8_lossy(&unnamed.stderr).contains("--db"),
        "{}",
        String::from_utf8_lossy(&unnamed.stderr)
    );
    let bind = [
        "--request-id",
        "b1",
        "bind",
        "--alias",
        "tokens",
        "--key",
        "signing",
        "--usages",
        "sign,verify",
    ];
    let bound = keys(&bind);
    assert!(bound.contains("request_id=b1 "), "{bound}");
    assert!(bound.contains("duplicate=false"), "{bound}");
    assert!(
        bound.contains("binding=tokens key=signing usages=sign,verify"),
        "{bound}"
    );
    assert!(keys(&bind).contains("duplicate=true"));
    let rotated = keys(&["rotate", "--name", "signing"]);
    assert!(rotated.contains("request_id=cli-"), "{rotated}");
    assert!(
        rotated.contains("active=2 versions=1:live,2:live"),
        "{rotated}"
    );
    let revoked = keys(&["revoke", "--name", "signing", "--version", "1"]);
    assert!(revoked.contains("versions=1:revoked,2:live"), "{revoked}");
    // Imported material is sealed to the wrapping key before it leaves.
    let seed = dir.path().join("seed.key");
    std::fs::write(&seed, [9u8; 32]).unwrap();
    let imported = keys(&[
        "import",
        "--name",
        "imported",
        "--algorithm",
        "Ed25519",
        "--key-file",
        seed.to_str().unwrap(),
        "--wrapping-file",
        wrapping.to_str().unwrap(),
    ]);
    assert!(
        imported.contains("key=imported algorithm=Ed25519 active=1"),
        "{imported}"
    );
    let refused = Command::new(bin())
        .args([
            "keys",
            "--endpoint",
            &endpoint,
            "--admin-token",
            "secret",
            "--db",
            "db",
            "bind",
            "--alias",
            "sealing",
            "--key",
            "signing",
            "--usages",
            "encrypt",
        ])
        .output()
        .unwrap();
    assert!(!refused.status.success());
    assert!(keys(&["cache"]).contains('{'));

    // Retention is per database, on one route.
    let output = Command::new(bin())
        .args([
            "retention",
            "--endpoint",
            &endpoint,
            "--admin-token",
            "secret",
            "--db",
            "db",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let status: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(status["initialized"], false, "{status}");
    assert_eq!(status["receipts"]["receipt_ttl_ms"], 86_400_000, "{status}");
    child.kill().await.unwrap();
}
