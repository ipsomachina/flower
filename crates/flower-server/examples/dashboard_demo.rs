//! A three-member cluster on loopback with steady traffic, for
//! previewing and developing the admin dashboard (`GET /admin`, see
//! `flower_server::dashboard`):
//!
//! ```text
//! cargo run -p flower-server --example dashboard_demo
//! ```
//!
//! Members 1 to 3 serve their HTTP APIs on 127.0.0.1:7801-7803 and
//! Raft on 127.0.0.1:7901-7903, in one process, with [`ADMIN_TOKEN`] as
//! the admin token. The demo builds the test guest (`flower-guest-test`)
//! for `wasm32` as the execution tests do, deploys it to the `demo` and
//! `audit` databases, stages a second version on `audit` for the
//! deployment controls, generates a managed key, backs up continuously
//! into its temporary directory, and then drives mixed traffic until
//! Ctrl-C: writes through every member (followers forward them),
//! linearizable queries, two live watches, and queue items (mutations
//! and timers enqueue them, a small worker claims, completes and
//! sometimes fails them). Everything lives in a temporary directory
//! removed on exit.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use base64::Engine as _;
use flower_backup::driver::{ContinuousSettings, Driver};
use flower_cluster::network::{PeerConfig, TcpFactory};
use flower_cluster::node::Node;
use flower_cluster::read::ReadSettings;
use flower_cluster::snapshot_policy::SnapshotPolicy;
use flower_cluster::statemachine::RuntimeFactory;
use flower_cluster::timing::{RaftTiming, SnapshotSettings, TimingSettings, raft_config};
use flower_cluster::wasm::WasmFactory;
use flower_crypto::host::CryptoHost;
use flower_crypto::replicated::{ManagedKeys, WrappingKey};
use flower_engine::storage::compact::CompactionSettings;
use flower_server::api::{AppState, serve};
use openraft::BasicNode;

/// The demo's admin token: a fixed development credential, never one
/// to reuse.
const ADMIN_TOKEN: &str = "flower-demo-admin-token";
/// The peer token the members present to each other.
const PEER_TOKEN: &str = "flower-demo-peer-token";
/// Members, their HTTP and Raft ports.
const MEMBERS: [(u64, u16, u16); 3] = [(1, 7801, 7901), (2, 7802, 7902), (3, 7803, 7903)];
/// The module every database runs.
const MODULE: &str = "app";

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("dashboard_demo: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    println!("building the test guest for wasm32 ...");
    let wasm = guest_wasm()?;
    let dir = tempfile::tempdir().map_err(|error| error.to_string())?;
    let (stop, stopped) = tokio::sync::watch::channel(false);
    let mut nodes = Vec::new();
    for (id, http, raft) in MEMBERS {
        nodes.push(start_member(dir.path(), id, http, raft, stopped.clone()).await?);
    }
    let members: BTreeMap<u64, BasicNode> = MEMBERS
        .iter()
        .map(|(id, _, raft)| (*id, BasicNode::new(format!("127.0.0.1:{raft}"))))
        .collect();
    nodes[0]
        .initialize_nodes(members)
        .await
        .map_err(|error| error.to_string())?;
    wait_for_leader(&nodes).await?;

    let http = reqwest::Client::new();
    let base = url(1);
    for db in ["demo", "audit"] {
        let deployed = admin(&http, &format!("{base}/v1/deploy"))
            .json(&serde_json::json!({
                "db": db,
                "module": MODULE,
                "wasm_b64": base64::prelude::BASE64_STANDARD.encode(&wasm),
                "manifest": manifest(),
            }))
            .send()
            .await
            .map_err(|error| error.to_string())?;
        if !deployed.status().is_success() {
            let body = deployed.text().await.unwrap_or_default();
            return Err(format!("deploy to {db} failed: {body}"));
        }
    }
    // A staged deployment for the dashboard's deployment controls: the
    // same guest with one more index, staged on `audit` at the leader and
    // left for the operator to advance, activate or cancel.
    let mut staged = manifest();
    staged["collections"][0]["indexes"]
        .as_array_mut()
        .ok_or("manifest without indexes")?
        .push(serde_json::json!({"name": "by_title_v2", "fields": ["title"]}));
    let _ = admin(&http, &format!("{base}/v1/admin/deployments"))
        .json(&serde_json::json!({
            "operation": "stage",
            "db": "audit",
            "module": MODULE,
            "wasm_b64": base64::prelude::BASE64_STANDARD.encode(&wasm),
            "manifest": staged,
        }))
        .send()
        .await;
    let _ = admin(&http, &format!("{base}/v1/admin/keys"))
        .json(&serde_json::json!({
            "db": "audit",
            "operation": "generate",
            "requestId": "demo-signing",
            "name": "demo-signing",
            "algorithm": "Ed25519",
        }))
        .send()
        .await;

    let traffic = Traffic::default();
    let tasks = vec![
        tokio::spawn(traffic.clone().writes(http.clone())),
        tokio::spawn(traffic.clone().queries(http.clone())),
        tokio::spawn(traffic.clone().watch(http.clone(), 2, "k1")),
        tokio::spawn(traffic.clone().watch(http.clone(), 3, "k2")),
        tokio::spawn(traffic.clone().enqueue(http.clone())),
        tokio::spawn(traffic.clone().work(http.clone())),
    ];

    println!();
    println!("Flower dashboard demo: three members on loopback");
    for (id, _, _) in MEMBERS {
        println!("  member {id}  {}/admin?queue=demo/email", url(id));
    }
    println!("  admin token: {ADMIN_TOKEN}");
    println!("  (ADMIN_TOKEN in crates/flower-server/examples/dashboard_demo.rs)");
    println!("Ctrl-C stops the cluster.");

    tokio::signal::ctrl_c()
        .await
        .map_err(|error| error.to_string())?;
    println!("stopping ...");
    for task in tasks {
        task.abort();
    }
    let _ = stop.send(true);
    tokio::time::sleep(Duration::from_millis(500)).await;
    Ok(())
}

/// The base URL of member `id`'s API.
fn url(id: u64) -> String {
    let (_, port, _) = MEMBERS[(id - 1) as usize];
    format!("http://127.0.0.1:{port}")
}

/// A POST with the admin token.
fn admin(http: &reqwest::Client, url: &str) -> reqwest::RequestBuilder {
    http.post(url).bearer_auth(ADMIN_TOKEN)
}

/// Open, serve and maintain one member: Raft on its port, the API on
/// its own, continuous backup, snapshots, compaction and the timer
/// sweep, until `stopped`.
async fn start_member(
    dir: &Path,
    id: u64,
    http_port: u16,
    raft_port: u16,
    stopped: tokio::sync::watch::Receiver<bool>,
) -> Result<Arc<Node>, String> {
    let timing = RaftTiming::derive(&TimingSettings::default())?;
    // Small snapshot triggers, so snapshots and the purge move.
    let snapshots = SnapshotSettings {
        logs_since_last: 2_000,
        keep_logs: 500,
        ..SnapshotSettings::default()
    };
    let policy = SnapshotPolicy {
        after_bytes: 512 * 1024,
        max_age: Duration::from_secs(60),
        ..SnapshotPolicy::default()
    };
    let peers = PeerConfig::with_token(PEER_TOKEN);
    // The guest imports the crypto lane: back it with the replicated
    // catalog every member unwraps with the same key.
    let catalog = Arc::new(ManagedKeys::new(
        Some(WrappingKey::new(&[42; 32])),
        Vec::new(),
    ));
    let crypto = CryptoHost::new().with_catalog(catalog.clone());
    let runtimes: Arc<dyn RuntimeFactory> = Arc::new(WasmFactory::with_crypto(Arc::new(crypto)));
    let node = Node::open(
        id,
        &dir.join(format!("n{id}")),
        Arc::new(raft_config(&timing, &snapshots)?),
        TcpFactory::new(id, peers.clone()),
        runtimes.clone(),
    )
    .await
    .map_err(|error| error.to_string())?
    .with_reads(ReadSettings::from_timing(&timing))
    .with_key_catalog(catalog);
    let raft = tokio::net::TcpListener::bind(("127.0.0.1", raft_port))
        .await
        .map_err(|error| format!("raft port {raft_port}: {error}"))?;
    // Detached: the process exits with it.
    let _peer = node.serve_listener(raft, peers);
    let node = Arc::new(node);

    let mut state = AppState::new(node.clone())
        .with_admin_token(ADMIN_TOKEN.to_string())
        .with_snapshot_policy(policy)
        .with_api_url(url(id));
    let builder = runtimes.clone();
    let backup = Driver::local(
        &dir.join("backups"),
        // One archive and one keystore for every member, as `serve`
        // asks: whichever member leads continues the same archive.
        &dir.join("keystore.json"),
        [7; 32],
        node.engine().clone(),
        node.logs().clone(),
        Arc::new(
            move |db: &[u8], module: &str, wasm: &[u8], manifest: &flower_sdk::Value| {
                builder.build(db, module, wasm, manifest)
            },
        ),
    );
    match backup {
        Ok(driver) => {
            let raft = node.raft().clone();
            let driver = Arc::new(driver.with_leadership(Arc::new(move || {
                raft.metrics().borrow().current_leader == Some(id)
            })));
            state = state.with_backup(driver.clone());
            let stopped = stopped.clone();
            tokio::spawn(async move {
                if let Err(error) = driver
                    .run_continuous(&ContinuousSettings::default(), stopped)
                    .await
                {
                    eprintln!("member {id}: continuous backup stopped: {error}");
                }
            });
        }
        Err(error) => eprintln!("member {id}: backup off: {error}"),
    }

    let api = tokio::net::TcpListener::bind(("127.0.0.1", http_port))
        .await
        .map_err(|error| format!("http port {http_port}: {error}"))?;
    tokio::spawn(serve(api, state));
    {
        let (node, stopped) = (node.clone(), stopped.clone());
        tokio::spawn(async move { node.run_maintenance(Duration::from_secs(1), stopped).await });
    }
    {
        let (node, stopped) = (node.clone(), stopped.clone());
        tokio::spawn(async move { node.run_snapshots(policy, stopped).await });
    }
    {
        let (node, stopped) = (node.clone(), stopped);
        tokio::spawn(async move {
            node.run_compaction(CompactionSettings::default(), stopped)
                .await
        });
    }
    Ok(node)
}

/// Wait until every member names the same leader.
async fn wait_for_leader(nodes: &[Arc<Node>]) -> Result<(), String> {
    for _ in 0..1_000 {
        let leaders: Vec<_> = nodes.iter().map(|node| node.status().leader).collect();
        if leaders[0].is_some() && leaders.iter().all(|leader| *leader == leaders[0]) {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Err("no leader elected".to_string())
}

/// The test guest's contract, every definition exposed under its own
/// name.
fn manifest() -> serde_json::Value {
    let definitions = [
        ("put", "mutation"),
        ("get", "query"),
        ("send_email", "mutation"),
        ("remind_me", "mutation"),
    ];
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
        "definitions": definitions
            .iter()
            .map(|(name, kind)| serde_json::json!({"name": name, "kind": kind, "args": {"type": "json"}}))
            .collect::<Vec<_>>(),
        "triggers": [],
        "http": definitions
            .iter()
            .map(|(name, _)| serde_json::json!({"alias": name, "definition": name}))
            .collect::<Vec<_>>(),
        "queues": [{"name": "email"}],
    })
}

/// Build `flower-guest-test` for `wasm32` in its own target directory,
/// as the execution tests do, and read the artifact.
fn guest_wasm() -> Result<Vec<u8>, String> {
    let root: PathBuf = Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .ok_or("no workspace root")?
        .to_path_buf();
    let target = root.join("target").join("wasm-guest");
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
    let status = std::process::Command::new(cargo)
        .args([
            "build",
            "--quiet",
            "--target",
            "wasm32-unknown-unknown",
            "-p",
            "flower-guest-test",
        ])
        .current_dir(&root)
        .env("CARGO_TARGET_DIR", &target)
        .status()
        .map_err(|error| error.to_string())?;
    if !status.success() {
        return Err("guest build failed (rustup target add wasm32-unknown-unknown?)".to_string());
    }
    let artifact = target
        .join("wasm32-unknown-unknown")
        .join("debug")
        .join("flower_guest_test.wasm");
    std::fs::read(&artifact).map_err(|error| format!("{}: {error}", artifact.display()))
}

/// Shared traffic state: a cheap random source and a sequence.
#[derive(Clone)]
struct Traffic {
    started: Instant,
    seed: Arc<AtomicU64>,
}

impl Default for Traffic {
    fn default() -> Self {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(1, |elapsed| elapsed.as_nanos() as u64);
        Traffic {
            started: Instant::now(),
            seed: Arc::new(AtomicU64::new(nanos | 1)),
        }
    }
}

impl Traffic {
    /// A pseudo-random number below `bound` (xorshift; not for secrets).
    fn below(&self, bound: u64) -> u64 {
        let mut x = self.seed.load(Ordering::Relaxed);
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.seed.store(x, Ordering::Relaxed);
        x % bound.max(1)
    }

    /// A member's base URL, chosen at random.
    fn member(&self) -> String {
        url(1 + self.below(3))
    }

    /// The pause between operations at `per_second`, swelling and
    /// ebbing over a few minutes so the charts have a shape.
    fn pace(&self, per_second: f64) -> Duration {
        let phase = self.started.elapsed().as_secs_f64() / 90.0 * std::f64::consts::TAU;
        let rate = per_second * (1.0 + 0.6 * phase.sin());
        Duration::from_secs_f64(1.0 / rate.max(0.5))
    }

    /// Writes to random keys through random members.
    async fn writes(self, http: reqwest::Client) {
        let mut sequence = 0u64;
        loop {
            sequence += 1;
            let db = if self.below(5) == 0 { "audit" } else { "demo" };
            let key = format!("k{}", self.below(40));
            let call = serde_json::json!({
                "db": db, "module": MODULE, "name": "put",
                "args": {"key": key, "title": format!("update {sequence}")},
            });
            let target = self.member();
            let _ = http
                .post(format!("{target}/v1/mutate"))
                .json(&call)
                .send()
                .await;
            tokio::time::sleep(self.pace(12.0)).await;
        }
    }

    /// Linearizable reads through random members.
    async fn queries(self, http: reqwest::Client) {
        loop {
            let call = serde_json::json!({
                "db": "demo", "module": MODULE, "name": "get",
                "args": {"key": format!("k{}", self.below(40))},
            });
            let target = self.member();
            let _ = http
                .post(format!("{target}/v1/query"))
                .json(&call)
                .send()
                .await;
            tokio::time::sleep(self.pace(20.0)).await;
        }
    }

    /// One live watch on member `id`, reopened whenever it ends.
    async fn watch(self, http: reqwest::Client, id: u64, key: &'static str) {
        let target = format!("{}/v1/watch", url(id));
        let body = serde_json::json!({
            "db": "demo",
            "module": MODULE,
            "name": "get",
            "args": {"key": key},
        });
        loop {
            if let Ok(mut response) = http.post(&target).json(&body).send().await {
                while let Ok(Some(_)) = response.chunk().await {}
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    /// Queue items, directly and through timers that fire later.
    async fn enqueue(self, http: reqwest::Client) {
        let mut sequence = 0u64;
        loop {
            sequence += 1;
            let to = format!("ops+{sequence}@example.com");
            let call = if sequence.is_multiple_of(3) {
                serde_json::json!({
                    "db": "demo", "module": MODULE, "name": "remind_me",
                    "args": {"id": format!("reminder-{sequence}"), "to": to, "delay_ms": 4_000.0},
                })
            } else {
                serde_json::json!({
                    "db": "demo", "module": MODULE, "name": "send_email",
                    "args": {"to": to},
                })
            };
            let target = self.member();
            let _ = http
                .post(format!("{target}/v1/mutate"))
                .json(&call)
                .send()
                .await;
            tokio::time::sleep(self.pace(1.5)).await;
        }
    }

    /// A worker on the `email` queue: claims a few items at a time,
    /// completes most and fails about one in eight.
    async fn work(self, http: reqwest::Client) {
        loop {
            tokio::time::sleep(Duration::from_millis(1_500)).await;
            let target = self.member();
            let Ok(response) = admin(&http, &format!("{target}/v1/queue/poll"))
                .json(&serde_json::json!({"db": "demo", "queue": "email", "limit": 2}))
                .send()
                .await
            else {
                continue;
            };
            let Ok(polled) = response.json::<serde_json::Value>().await else {
                continue;
            };
            let items = polled["items"].as_array().cloned().unwrap_or_default();
            for item in items {
                let outcome = if self.below(8) == 0 {
                    "fail"
                } else {
                    "complete"
                };
                let _ = admin(&http, &format!("{target}/v1/queue/{outcome}"))
                    .json(&serde_json::json!({
                        "db": "demo", "queue": "email",
                        "id": item["id"], "token": item["token"], "delay_ms": 2_000,
                    }))
                    .send()
                    .await;
            }
        }
    }
}
