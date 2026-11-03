//! An in-process measurement of speculative preparation: throughput and
//! latency of a CPU-bound read-modify-write mutation on one member, with
//! the writer serial and speculating, under an uncontended workload
//! (each call on its own counter) and a contended one (most calls on one
//! hot counter). Ignored by default: run it on a quiet machine, in
//! release, with
//!
//! ```text
//! cargo test --release -p flower-cluster --test speculation_bench -- --ignored --nocapture
//! ```
//!
//! `FLOWER_BENCH_SECONDS` (default 4), `FLOWER_BENCH_CLIENTS` (64) and
//! `FLOWER_BENCH_WORK` (guest work per call, xorshift rounds, default
//! 100000), `FLOWER_BENCH_PASSES` (interleaved passes, default 1) and
//! `FLOWER_BENCH_FSYNC` (1 syncs commits; default 0) tune
//! it.

use std::collections::BTreeSet;
use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use flower_cluster::fsync::Durability;
use flower_cluster::network::{InProcessFactory, Registry};
use flower_cluster::node::Node;
use flower_cluster::speculation::SpeculationSettings;
use flower_cluster::statemachine::RuntimeFactory;
use flower_engine::engine::MutationOptions;
use flower_engine::runtime::{CallKind, ClosureRuntime, HostCall};
use flower_sdk::abi::{HostOp, decode_envelope};
use flower_sdk::{Failure, Value, fail};

fn s(text: &str) -> Value {
    Value::String(text.to_string())
}

fn obj(pairs: Vec<(&str, Value)>) -> Value {
    Value::Object(
        pairs
            .into_iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect(),
    )
}

fn manifest() -> Value {
    let counters = obj(vec![
        ("name", s("counters")),
        ("key", obj(vec![("type", s("string"))])),
        ("value", obj(vec![("type", s("json"))])),
        ("indexes", Value::Array(vec![])),
        ("references", Value::Array(vec![])),
    ]);
    let definition = |name: &str, kind: &str| {
        obj(vec![
            ("name", s(name)),
            ("kind", s(kind)),
            ("args", obj(vec![("type", s("json"))])),
        ])
    };
    obj(vec![
        ("collections", Value::Array(vec![counters])),
        (
            "definitions",
            Value::Array(vec![definition("bump", "mutation")]),
        ),
        ("triggers", Value::Array(vec![])),
        ("http", Value::Array(vec![])),
    ])
}

fn hc(host: &mut dyn HostCall, op: HostOp, args: Vec<Value>) -> Result<Value, Failure> {
    let input = Value::Array(args).encode_binary().unwrap();
    let output = host.op(op as u32 as u8, &input);
    decode_envelope(Value::decode_binary(&output).unwrap())
}

/// `bump {key}`: read a counter, burn the configured work, write it
/// plus one.
struct Bench {
    work: u64,
}

impl RuntimeFactory for Bench {
    fn build(
        &self,
        _db: &[u8],
        _module: &str,
        _wasm: &[u8],
        _manifest: &Value,
    ) -> Result<Box<dyn flower_engine::runtime::Runtime>, String> {
        let work = self.work;
        Ok(Box::new(ClosureRuntime::new(
            move |kind: CallKind,
                  _module: &str,
                  name: &str,
                  args: &Value,
                  host: &mut dyn HostCall| {
                let Value::Object(map) = args else {
                    return Err(fail("TYPE_MISMATCH", "args take an object"));
                };
                let Some(Value::String(key)) = map.get("key") else {
                    return Err(fail("TYPE_MISMATCH", "args need a key"));
                };
                let key = format!("\"{key}\"");
                match (kind, name) {
                    (CallKind::Mutation, "bump") => {
                        let count =
                            match hc(host, HostOp::Get, vec![s(""), s("counters"), s(&key)])? {
                                Value::Number(count) => count,
                                _ => 0.0,
                            };
                        let mut state = 0x9e37_79b9_7f4a_7c15_u64 ^ count.to_bits();
                        for _ in 0..work {
                            state ^= state << 13;
                            state ^= state >> 7;
                            state ^= state << 17;
                            state = black_box(state);
                        }
                        black_box(state);
                        hc(
                            host,
                            HostOp::Set,
                            vec![s(""), s("counters"), s(&key), Value::Number(count + 1.0)],
                        )?;
                        Ok(Value::Number(count + 1.0))
                    }
                    _ => Err(fail("UNKNOWN_METHOD", "unknown definition")),
                }
            },
        )))
    }
}

fn setting(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|text| text.parse().ok())
        .unwrap_or(default)
}

async fn member(dir: &std::path::Path, work: u64) -> Node {
    let registry = Registry::new();
    let node = Node::open_with_durability(
        1,
        &dir.join("n1"),
        Arc::new(openraft::Config::default()),
        InProcessFactory::new(registry.clone()),
        Arc::new(Bench { work }),
        if setting("FLOWER_BENCH_FSYNC", 0) == 1 {
            Durability::on()
        } else {
            Durability::off()
        },
    )
    .await
    .unwrap();
    registry.register(1, node.raft().clone());
    node.initialize(BTreeSet::from([1])).await.unwrap();
    for _ in 0..200 {
        if node.status().leader == Some(1) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    node.deploy(b"db", "app", [9u8; 32], b"wasm", &manifest())
        .await
        .unwrap();
    node
}

/// One run: `clients` calling `bump` as fast as answered for
/// `seconds`, after a second of warm-up; calls per second and latency
/// quantiles in ms.
async fn run(
    settings: SpeculationSettings,
    contended: bool,
    clients: usize,
    seconds: u64,
    work: u64,
) -> (f64, f64, f64, flower_cluster::speculation::SpeculationStats) {
    let dir = tempfile::tempdir().unwrap();
    let node = Arc::new(member(dir.path(), work).await);
    node.set_speculation(settings);
    let warm = Instant::now() + Duration::from_secs(1);
    let end = warm + Duration::from_secs(seconds);
    let tasks: Vec<_> = (0..clients)
        .map(|client| {
            let node = node.clone();
            tokio::spawn(async move {
                let mut latencies = Vec::new();
                let mut seed = (client as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
                let mut call = 0u64;
                loop {
                    let now = Instant::now();
                    if now >= end {
                        return latencies;
                    }
                    seed ^= seed << 13;
                    seed ^= seed >> 7;
                    seed ^= seed << 17;
                    call += 1;
                    let key = if contended && seed % 10 < 9 {
                        "hot".to_string()
                    } else {
                        format!("c{client}n{call}")
                    };
                    let args = obj(vec![("key", s(&key))]);
                    node.mutate(b"db", "app", "bump", &args, &MutationOptions::default())
                        .await
                        .unwrap();
                    if now >= warm {
                        latencies.push(now.elapsed());
                    }
                }
            })
        })
        .collect();
    let stats_before = node.writer_stats().speculation;
    let mut latencies = Vec::new();
    for task in tasks {
        latencies.extend(task.await.unwrap());
    }
    let mut stats = node.writer_stats().speculation;
    stats.candidates -= stats_before.candidates;
    latencies.sort_unstable();
    let quantile = |q: f64| {
        latencies
            .get(((latencies.len() as f64) * q) as usize)
            .copied()
            .unwrap_or_default()
            .as_secs_f64()
            * 1000.0
    };
    let throughput = latencies.len() as f64 / seconds as f64;
    let result = (throughput, quantile(0.5), quantile(0.99), stats);
    Arc::into_inner(node).unwrap().shutdown().await.unwrap();
    result
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "a measurement: run in release on a quiet machine"]
async fn measure_speculation() {
    let seconds = setting("FLOWER_BENCH_SECONDS", 4);
    let clients = setting("FLOWER_BENCH_CLIENTS", 64) as usize;
    let work = setting("FLOWER_BENCH_WORK", 100_000);
    let passes = setting("FLOWER_BENCH_PASSES", 1).max(1) as usize;
    let configs = [
        ("serial", 1, true),
        ("4 workers, no prediction", 4, false),
        ("4 workers, prediction", 4, true),
        ("8 workers, prediction", 8, true),
    ];
    println!("clients {clients}, {seconds} s per run, work {work}, {passes} passes");
    let workloads = [false, true];
    // Runs interleave across passes, so drift on a shared machine
    // spreads over every configuration.
    let mut results = vec![Vec::new(); workloads.len() * configs.len()];
    for pass in 0..passes {
        for (w, contended) in workloads.into_iter().enumerate() {
            for (c, (name, workers, prediction)) in configs.into_iter().enumerate() {
                let settings = SpeculationSettings {
                    workers,
                    prediction,
                };
                let result = run(settings, contended, clients, seconds, work).await;
                println!(
                    "pass {pass} {} {name}: {:.0}/s p50 {:.2} p99 {:.2} {:?}",
                    if contended {
                        "contended"
                    } else {
                        "uncontended"
                    },
                    result.0,
                    result.1,
                    result.2,
                    result.3
                );
                results[w * configs.len() + c].push(result);
            }
        }
    }
    println!(
        "| workload | writer | calls/s median (min-max) | p50 ms | p99 ms | reused | conflicted | placed | cooldown |"
    );
    println!("|---|---|---|---|---|---|---|---|---|");
    let median = |mut values: Vec<f64>| {
        values.sort_by(f64::total_cmp);
        values[values.len() / 2]
    };
    for (w, contended) in workloads.into_iter().enumerate() {
        for (c, (name, _, _)) in configs.into_iter().enumerate() {
            let runs = &results[w * configs.len() + c];
            let throughputs: Vec<f64> = runs.iter().map(|run| run.0).collect();
            let low = throughputs.iter().copied().fold(f64::INFINITY, f64::min);
            let high = throughputs.iter().copied().fold(0.0, f64::max);
            let sum = |pick: fn(&flower_cluster::speculation::SpeculationStats) -> u64| {
                runs.iter().map(|run| pick(&run.3)).sum::<u64>()
            };
            println!(
                "| {} | {name} | {:.0} ({low:.0}-{high:.0}) | {:.1} | {:.1} | {} | {} | {} | {} |",
                if contended {
                    "contended"
                } else {
                    "uncontended"
                },
                median(throughputs.clone()),
                median(runs.iter().map(|run| run.1).collect()),
                median(runs.iter().map(|run| run.2).collect()),
                sum(|stats| stats.reused),
                sum(|stats| stats.conflicted),
                sum(|stats| stats.placed_serially),
                sum(|stats| stats.cooldown_serial_calls),
            );
        }
    }
}
