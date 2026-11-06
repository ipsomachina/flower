//! Write latency and throughput of a three-member group over real files,
//! and the fsyncs each write costs: the numbers `spec.md` quotes for the
//! deferred flushes (`flower_cluster::flush`). Ignored: it measures, it
//! checks nothing. Run it on a quiet machine, in release:
//!
//! ```text
//! cargo test --release -p flower-cluster --test write_latency -- --ignored --nocapture
//! ```
//!
//! The members take their flush settings from the environment
//! (`FLOWER_LEADER_FLUSH`, `FLOWER_APPLY_FLUSH_MS`, ...; see
//! `FlushSettings::from_env`). Without the `with_flush` line the file
//! runs on the build before the deferred flushes, which measured the
//! "before" numbers.

mod fixtures;

use fixtures::puts::{Puts, manifest};
use fixtures::{obj, s};

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use flower_cluster::network::{InProcessFactory, Registry};
use flower_cluster::node::{FileOpener, Node, NodeError};
use flower_cluster::timing::{RaftTiming, SnapshotSettings, TimingSettings, raft_config};
use flower_cluster::types::NodeId;
use flower_engine::engine::MutationOptions;
use redb::backends::FileBackend;

/// A real file counting its syncs.
#[derive(Debug)]
struct Counted {
    file: FileBackend,
    syncs: Arc<AtomicU64>,
}

impl redb::StorageBackend for Counted {
    fn len(&self) -> std::io::Result<u64> {
        self.file.len()
    }
    fn read(&self, offset: u64, out: &mut [u8]) -> std::io::Result<()> {
        self.file.read(offset, out)
    }
    fn set_len(&self, len: u64) -> std::io::Result<()> {
        self.file.set_len(len)
    }
    fn sync_data(&self) -> std::io::Result<()> {
        self.syncs.fetch_add(1, Ordering::Relaxed);
        self.file.sync_data()
    }
    fn write(&self, offset: u64, data: &[u8]) -> std::io::Result<()> {
        self.file.write(offset, data)
    }
    fn close(&self) -> std::io::Result<()> {
        self.file.close()
    }
}

/// One member's sync counters: group file, Raft file.
#[derive(Default, Clone)]
struct Syncs {
    group: Arc<AtomicU64>,
    raft: Arc<AtomicU64>,
}

impl Syncs {
    fn files(&self) -> FileOpener {
        let syncs = self.clone();
        Arc::new(move |path: &Path| {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(path)
                .map_err(|error| redb::DatabaseError::Storage(redb::StorageError::Io(error)))?;
            let counter = if path.ends_with("group.redb") {
                syncs.group.clone()
            } else {
                syncs.raft.clone()
            };
            redb::Builder::new().create_with_backend(Counted {
                file: FileBackend::new(file)?,
                syncs: counter,
            })
        })
    }

    fn read(&self) -> (u64, u64) {
        (
            self.raft.load(Ordering::Relaxed),
            self.group.load(Ordering::Relaxed),
        )
    }
}

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    sorted[((sorted.len() as f64 * p) as usize).min(sorted.len() - 1)]
}

fn ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

async fn put(node: &Node, key: String) {
    let args = obj(vec![("key", s(&format!("\"{key}\""))), ("value", s("v"))]);
    loop {
        match node
            .mutate(b"db", "app", "put", &args, &MutationOptions::default())
            .await
        {
            Ok(_) => return,
            // Admission sheds a burst on a busy machine: retry, as
            // clients do; the latency counts the wait.
            Err(NodeError::Overloaded { .. }) => {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            Err(error) => panic!("write {key} failed: {error}"),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "measures write latency; run on a quiet machine"]
async fn measure_write_latency_and_throughput() {
    let dir = tempfile::tempdir().unwrap();
    let timing = RaftTiming::derive(&TimingSettings::default()).unwrap();
    let config = Arc::new(raft_config(&timing, &SnapshotSettings::default()).unwrap());
    let registry = Registry::new();
    let syncs: Vec<Syncs> = (0..3).map(|_| Syncs::default()).collect();
    let mut nodes = Vec::new();
    for id in 1..=3u64 {
        let node = Node::open_with(
            id,
            &dir.path().join(format!("n{id}")),
            config.clone(),
            InProcessFactory::new(registry.clone()),
            Arc::new(Puts),
            syncs[id as usize - 1].files(),
        )
        .await
        .unwrap();
        registry.register_peer(node.peer_service());
        let node = node.with_flush(flower_cluster::flush::FlushSettings::from_env().unwrap());
        nodes.push(Arc::new(node));
    }
    nodes[0]
        .initialize((1..=3).collect::<BTreeSet<NodeId>>())
        .await
        .unwrap();
    let leader = loop {
        let leaders: BTreeSet<Option<NodeId>> =
            nodes.iter().map(|node| node.status().leader).collect();
        if let [Some(leader)] = leaders.into_iter().collect::<Vec<_>>()[..] {
            break leader;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    let lead = nodes[leader as usize - 1].clone();
    let follower = nodes.iter().position(|node| node.id() != leader).unwrap();
    lead.deploy(b"db", "app", [7u8; 32], b"wasm", &manifest())
        .await
        .unwrap();
    for i in 0..100 {
        put(&lead, format!("warm{i}")).await;
    }

    // One write at a time: each pays the whole commit path.
    let sequential = 1000;
    let before = (syncs[leader as usize - 1].read(), syncs[follower].read());
    let mut latencies = Vec::with_capacity(sequential);
    let started = Instant::now();
    for i in 0..sequential {
        let one = Instant::now();
        put(&lead, format!("seq{i}")).await;
        latencies.push(one.elapsed());
    }
    let elapsed = started.elapsed();
    let after = (syncs[leader as usize - 1].read(), syncs[follower].read());
    latencies.sort();
    let per = |a: u64, b: u64| (b - a) as f64 / sequential as f64;
    println!(
        "sequential: {sequential} writes, p50 {:.2} ms, p90 {:.2} ms, p99 {:.2} ms, mean {:.2} ms; \
         fsyncs per write: leader log {:.2}, leader group {:.2}, follower log {:.2}, follower group {:.2}",
        ms(percentile(&latencies, 0.5)),
        ms(percentile(&latencies, 0.9)),
        ms(percentile(&latencies, 0.99)),
        ms(elapsed) / sequential as f64,
        per(before.0.0, after.0.0),
        per(before.0.1, after.0.1),
        per(before.1.0, after.1.0),
        per(before.1.1, after.1.1),
    );

    // Many writers at once: the writer groups them into batch entries.
    let writers = 64;
    let each = 100;
    let before = (syncs[leader as usize - 1].read(), syncs[follower].read());
    let started = Instant::now();
    let tasks: Vec<_> = (0..writers)
        .map(|writer| {
            let lead = lead.clone();
            tokio::spawn(async move {
                let mut latencies = Vec::with_capacity(each);
                for i in 0..each {
                    let one = Instant::now();
                    put(&lead, format!("c{writer}-{i}")).await;
                    latencies.push(one.elapsed());
                }
                latencies
            })
        })
        .collect();
    let mut latencies = Vec::new();
    for task in tasks {
        latencies.extend(task.await.unwrap());
    }
    let elapsed = started.elapsed();
    let after = (syncs[leader as usize - 1].read(), syncs[follower].read());
    latencies.sort();
    let total = (writers * each) as u64;
    let per = |a: u64, b: u64| (b - a) as f64 / total as f64;
    println!(
        "concurrent: {writers} writers x {each}, {:.0} writes/s, p50 {:.2} ms, p99 {:.2} ms; \
         fsyncs per write: leader log {:.3}, leader group {:.3}, follower log {:.3}, follower group {:.3}",
        total as f64 / elapsed.as_secs_f64(),
        ms(percentile(&latencies, 0.5)),
        ms(percentile(&latencies, 0.99)),
        per(before.0.0, after.0.0),
        per(before.0.1, after.0.1),
        per(before.1.0, after.1.0),
        per(before.1.1, after.1.1),
    );
    drop(lead);
    for node in nodes {
        let node = Arc::try_unwrap(node).ok().expect("the writers are done");
        node.shutdown().await.unwrap();
    }
}
