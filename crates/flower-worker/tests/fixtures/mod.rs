//! Build and deploy Wasm examples for worker integration tests.

use flower_client::FlowerClient;
use flower_client::client::RequestOptions;
use flower_client::transport::Credentials;
use flower_cluster::network::{InProcessFactory, Registry};
use flower_cluster::node::Node;
use flower_cluster::wasm::WasmFactory;
use flower_engine::runtime::wasm::WasmBackend;
use serde_json::Value;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

fn root() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf()
}

/// Build the example for wasm32 in release and return its bytes.
fn example_wasm(package: &str) -> Vec<u8> {
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
    let target = root().join("target").join("wasm-guest");
    let status = std::process::Command::new(cargo)
        .args([
            "build",
            "--offline",
            "--quiet",
            "--release",
            "--target",
            "wasm32-unknown-unknown",
            "-p",
            package,
        ])
        .current_dir(root())
        .env("CARGO_TARGET_DIR", &target)
        .status()
        .unwrap();
    assert!(status.success(), "example build failed");
    std::fs::read(
        target
            .join("wasm32-unknown-unknown")
            .join("release")
            .join(format!("{}.wasm", package.replace('-', "_"))),
    )
    .unwrap()
}

pub async fn deploy_example(
    package: &'static str,
    module: &str,
) -> (tempfile::TempDir, Arc<Node>, FlowerClient) {
    let wasm = tokio::task::spawn_blocking(move || example_wasm(package))
        .await
        .unwrap();
    let manifest = {
        let mut backend = WasmBackend::new().unwrap();
        backend.register(module, &wasm).unwrap();
        let manifest = backend.manifest(module).unwrap();
        serde_json::from_slice::<Value>(&flower_engine::json::render(&manifest).unwrap()).unwrap()
    };
    let dir = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let node = Node::open(
        1,
        &dir.path().join("n1"),
        Arc::new(openraft::Config::default()),
        InProcessFactory::new(registry.clone()),
        Arc::new(WasmFactory::default()),
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
    let state =
        flower_server::api::AppState::new(node.clone()).with_admin_token("secret".to_string());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(flower_server::api::serve(listener, state));
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
            module,
            None,
            &wasm,
            &manifest,
            &RequestOptions::default().timeout(Duration::from_secs(60)),
        )
        .await
        .unwrap();
    (dir, node, client)
}
