//! Storage failure over HTTP: a member whose disk fills answers
//! `/health/ready` with 503 and the failure, refuses writes with a
//! retryable 503 `storage_failed`, and reports both files' compaction in
//! its metrics.

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use flower_cluster::network::{InProcessFactory, Registry};
use flower_cluster::node::{FileOpener, Node};
use flower_cluster::statemachine::RuntimeFactory;
use flower_engine::runtime::{CallKind, ClosureRuntime, HostCall};
use flower_sdk::abi::{HostOp, decode_envelope};
use flower_sdk::{Value, fail};
use flower_server::api::{AppState, serve};
use redb::backends::FileBackend;

/// A file whose writes fail with "storage full" while `full` is set.
#[derive(Debug)]
struct FillingDisk {
    file: FileBackend,
    full: Arc<AtomicBool>,
}

impl FillingDisk {
    fn check(&self) -> std::io::Result<()> {
        if self.full.load(Ordering::SeqCst) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::StorageFull,
                "No space left on device",
            ));
        }
        Ok(())
    }
}

impl redb::StorageBackend for FillingDisk {
    fn len(&self) -> std::io::Result<u64> {
        self.file.len()
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> std::io::Result<()> {
        self.file.read(offset, out)
    }

    fn set_len(&self, len: u64) -> std::io::Result<()> {
        self.check()?;
        self.file.set_len(len)
    }

    fn sync_data(&self) -> std::io::Result<()> {
        self.check()?;
        self.file.sync_data()
    }

    fn write(&self, offset: u64, data: &[u8]) -> std::io::Result<()> {
        self.check()?;
        self.file.write(offset, data)
    }

    fn close(&self) -> std::io::Result<()> {
        self.file.close()
    }
}

/// Open every file over a disk that fills with `full`.
fn filling_files(full: Arc<AtomicBool>) -> FileOpener {
    Arc::new(move |path: &Path| {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .map_err(|error| redb::DatabaseError::Storage(redb::StorageError::Io(error)))?;
        redb::Builder::new().create_with_backend(FillingDisk {
            file: FileBackend::new(file)?,
            full: full.clone(),
        })
    })
}

fn s(text: &str) -> Value {
    Value::String(text.to_string())
}

/// `put {key, title}` stores a task.
struct App;

impl RuntimeFactory for App {
    fn build(
        &self,
        _db: &[u8],
        _module: &str,
        _wasm: &[u8],
        _manifest: &Value,
    ) -> Result<Box<dyn flower_engine::runtime::Runtime>, String> {
        Ok(Box::new(ClosureRuntime::new(
            |kind, _module: &str, name: &str, args: &Value, host: &mut dyn HostCall| {
                let Value::Object(map) = args else {
                    return Err(fail("TYPE_MISMATCH", "args take an object"));
                };
                let (Some(key), Some(title)) = (map.get("key"), map.get("title")) else {
                    return Err(fail("TYPE_MISMATCH", "args need a key and a title"));
                };
                if !matches!((kind, name), (CallKind::Mutation, "put")) {
                    return Err(fail("UNKNOWN_METHOD", "unknown definition"));
                }
                let value = Value::Object([("title".to_string(), title.clone())].into());
                let input = Value::Array(vec![s(""), s("tasks"), key.clone(), value])
                    .encode_binary()
                    .unwrap();
                let output = host.op(HostOp::Set as u32 as u8, &input);
                decode_envelope(Value::decode_binary(&output).unwrap())?;
                Ok(s("put-ok"))
            },
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
        "definitions": [{"name": "put", "kind": "mutation", "args": {"type": "json"}}],
        "triggers": [],
        "http": [{"alias": "put", "definition": "put"}]
    })
}

async fn put(http: &reqwest::Client, base: &str, key: &str) -> reqwest::Response {
    http.post(format!("{base}/v1/mutate"))
        .json(&serde_json::json!({
            "db": "db",
            "module": "app",
            "name": "put",
            "args": {"key": format!("\"{key}\""), "title": key},
        }))
        .timeout(Duration::from_secs(20))
        .send()
        .await
        .expect("a write never hangs on a failed disk")
}

#[tokio::test(flavor = "multi_thread")]
async fn a_full_disk_flips_readiness_and_refuses_writes_with_503() {
    let dir = tempfile::tempdir().unwrap();
    let full = Arc::new(AtomicBool::new(false));
    let registry = Registry::new();
    let node = Node::open_with(
        1,
        dir.path(),
        Arc::new(openraft::Config::default()),
        InProcessFactory::new(registry.clone()),
        Arc::new(App),
        filling_files(full.clone()),
    )
    .await
    .unwrap();
    registry.register(1, node.raft().clone());
    node.initialize(BTreeSet::from([1])).await.unwrap();
    let node = Arc::new(node);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let state = AppState::new(node.clone()).with_admin_token("secret".to_string());
    tokio::spawn(serve(listener, state));
    let http = reqwest::Client::new();
    let ready = || async {
        let response = http
            .get(format!("{base}/health/ready"))
            .send()
            .await
            .unwrap();
        let status = response.status().as_u16();
        (status, response.json::<serde_json::Value>().await.unwrap())
    };
    for _ in 0..1000 {
        if ready().await.0 == 200 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let (status, body) = ready().await;
    assert_eq!(status, 200, "{body}");
    assert!(body["storage"].is_null(), "{body}");
    let deployed = http
        .post(format!("{base}/v1/deploy"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({
            "db": "db",
            "module": "app",
            "identity": null,
            "wasm_b64": "e30=",
            "manifest": manifest(),
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(deployed.status(), 200);
    assert_eq!(put(&http, &base, "before").await.status(), 200);

    // The disk fills: the write fails, and readiness reports why.
    full.store(true, Ordering::SeqCst);
    assert_ne!(put(&http, &base, "during").await.status(), 200);
    let mut answer = ready().await;
    for _ in 0..1000 {
        if !answer.1["storage"].is_null() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        answer = ready().await;
    }
    let (status, body) = answer;
    assert_eq!(status, 503, "{body}");
    assert_eq!(body["ok"], false);
    assert_eq!(body["storage"]["component"], "raft", "{body}");
    assert!(
        body["storage"]["error"]
            .as_str()
            .unwrap()
            .contains("No space left"),
        "{body}"
    );
    assert!(body["storage"]["since_ms"].as_u64().unwrap() > 0);

    // Writes are refused up front, retryably: another member can serve.
    let refused = put(&http, &base, "after").await;
    assert_eq!(refused.status(), 503);
    let refused: serde_json::Value = refused.json().await.unwrap();
    assert_eq!(refused["code"], "storage_failed", "{refused}");

    // Metrics report the failure and both files' compaction.
    let metrics: serde_json::Value = http
        .get(format!("{base}/v1/metrics"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(metrics["storage_failure"]["component"], "raft", "{metrics}");
    let files: Vec<&str> = metrics["compaction"]
        .as_array()
        .unwrap()
        .iter()
        .map(|file| file["file"].as_str().unwrap())
        .collect();
    assert_eq!(files, ["group", "raft_log"], "{metrics}");
}
