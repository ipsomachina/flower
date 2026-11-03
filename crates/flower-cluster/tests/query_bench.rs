//! An in-process measurement of query reuse on one member: identical
//! queries in flight joining one evaluation, cache hits sent as the
//! rendering they keep, and results kept across writes that leave the
//! derived values they read as they were. Ignored by default: run it on
//! a quiet machine, in release, with
//!
//! ```text
//! cargo test --release -p flower-cluster --test query_bench -- --ignored --nocapture
//! ```
//!
//! `FLOWER_BENCH_ROUNDS` (default 40), `FLOWER_BENCH_CLIENTS` (64),
//! `FLOWER_BENCH_WORK` (guest work per evaluation, xorshift rounds,
//! default 1000000), `FLOWER_BENCH_ROWS` (rows in the large result,
//! default 2000) and `FLOWER_BENCH_PASSES` (interleaved passes, default
//! 3) tune it.

use std::collections::BTreeSet;
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use flower_cluster::network::{InProcessFactory, Registry};
use flower_cluster::node::Node;
use flower_cluster::statemachine::RuntimeFactory;
use flower_engine::engine::{CallOptions, MutationOptions};
use flower_engine::json::render;
use flower_engine::runtime::{CallKind, ClosureRuntime, HostCall};
use flower_sdk::abi::{HostOp, decode_envelope};
use flower_sdk::{Failure, Value, fail};

fn s(text: &str) -> Value {
    Value::String(text.to_string())
}

fn n(number: f64) -> Value {
    Value::Number(number)
}

fn obj(pairs: Vec<(&str, Value)>) -> Value {
    Value::Object(
        pairs
            .into_iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect(),
    )
}

fn env(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn manifest() -> Value {
    let rows = obj(vec![
        ("name", s("rows")),
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
        ("collections", Value::Array(vec![rows])),
        (
            "definitions",
            Value::Array(vec![
                definition("put", "mutation"),
                definition("heavy", "query"),
                definition("big", "query"),
                definition("parity", "derived"),
                definition("top", "derived"),
                definition("report", "query"),
            ]),
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

/// Burn `rounds` of xorshift.
fn work(rounds: u64) -> u64 {
    let mut x = 0x9e37_79b9_7f4a_7c15u64;
    for _ in 0..rounds {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
    }
    black_box(x)
}

/// `put {v}` writes row `a`; `heavy` reads it and works; `big` answers
/// many rows of nothing it read; `report` reads `top`, which reads
/// `parity` of row `a`, and works.
struct Bench {
    work: u64,
    rows: u64,
    heavy: Arc<AtomicUsize>,
}

impl RuntimeFactory for Bench {
    fn build(
        &self,
        _db: &[u8],
        _module: &str,
        _wasm: &[u8],
        _manifest: &Value,
    ) -> Result<Box<dyn flower_engine::runtime::Runtime>, String> {
        let (rounds, rows, heavy) = (self.work, self.rows, self.heavy.clone());
        Ok(Box::new(ClosureRuntime::new(
            move |kind: CallKind,
                  _module: &str,
                  name: &str,
                  args: &Value,
                  host: &mut dyn HostCall| {
                let row = |host: &mut dyn HostCall| {
                    hc(host, HostOp::Get, vec![s(""), s("rows"), s("\"a\"")])
                };
                match (kind, name) {
                    (CallKind::Mutation, "put") => {
                        hc(
                            host,
                            HostOp::Set,
                            vec![s(""), s("rows"), s("\"a\""), args.clone()],
                        )?;
                        Ok(Value::Null)
                    }
                    (CallKind::Query, "heavy") => {
                        heavy.fetch_add(1, Ordering::Relaxed);
                        let value = row(host)?;
                        work(rounds);
                        Ok(value)
                    }
                    (CallKind::Query, "big") => Ok(Value::Array(
                        (0..rows)
                            .map(|i| {
                                obj(vec![
                                    ("id", n(i as f64)),
                                    ("title", s(&format!("row number {i}"))),
                                    ("done", Value::Bool(i % 3 == 0)),
                                ])
                            })
                            .collect(),
                    )),
                    (CallKind::Derived, "parity") => match row(host)? {
                        Value::Object(map) => match map.get("v") {
                            Some(Value::Number(v)) => Ok(n(v % 2.0)),
                            _ => Ok(Value::Null),
                        },
                        _ => Ok(Value::Null),
                    },
                    (CallKind::Derived, "top") => {
                        let parity = hc(
                            host,
                            HostOp::GetDerived,
                            vec![s("app"), s("parity"), Value::Null],
                        )?;
                        Ok(obj(vec![("parity", parity)]))
                    }
                    (CallKind::Query, "report") => {
                        let top = hc(
                            host,
                            HostOp::GetDerived,
                            vec![s("app"), s("top"), Value::Null],
                        )?;
                        work(rounds);
                        Ok(top)
                    }
                    _ => Err(fail("UNKNOWN_METHOD", name.to_string())),
                }
            },
        )))
    }
}

async fn member(dir: &std::path::Path, bench: Bench) -> Arc<Node> {
    let registry = Registry::new();
    let node = Node::open(
        1,
        &dir.join("n1"),
        Arc::new(openraft::Config::default()),
        InProcessFactory::new(registry.clone()),
        Arc::new(bench),
    )
    .await
    .unwrap();
    registry.register_peer(node.peer_service());
    node.initialize(BTreeSet::from([1])).await.unwrap();
    for _ in 0..200 {
        if node.status().leader == Some(1) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    node.deploy(b"db", "app", [7u8; 32], b"wasm", &manifest())
        .await
        .unwrap();
    Arc::new(node)
}

async fn put(node: &Node, v: f64) {
    node.mutate(
        b"db",
        "app",
        "put",
        &obj(vec![("v", n(v))]),
        &MutationOptions::default(),
    )
    .await
    .unwrap();
}

/// The median and 99th percentile of `samples`.
fn percentiles(samples: &mut [Duration]) -> (Duration, Duration) {
    samples.sort();
    let at = |q: f64| samples[((samples.len() - 1) as f64 * q).round() as usize];
    (at(0.5), at(0.99))
}

fn ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

/// Rounds of a write to row `a` followed by `clients` identical `heavy`
/// queries at once: each round's wall time and the evaluations it took.
async fn burst(
    node: &Arc<Node>,
    heavy: &AtomicUsize,
    rounds: u64,
    clients: u64,
) -> (Vec<Duration>, usize) {
    let mut times = Vec::new();
    let before = heavy.load(Ordering::Relaxed);
    for round in 0..rounds {
        put(node, round as f64).await;
        let started = Instant::now();
        let calls: Vec<_> = (0..clients)
            .map(|_| {
                let node = node.clone();
                tokio::spawn(async move {
                    node.query(b"db", "app", "heavy", &Value::Null, &CallOptions::default())
                        .await
                        .unwrap()
                })
            })
            .collect();
        for call in calls {
            call.await.unwrap();
        }
        times.push(started.elapsed());
    }
    (times, heavy.load(Ordering::Relaxed) - before)
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "measurement: run in release on a quiet machine"]
async fn query_reuse() {
    let rounds = env("FLOWER_BENCH_ROUNDS", 40);
    let clients = env("FLOWER_BENCH_CLIENTS", 64);
    let rounds_of_work = env("FLOWER_BENCH_WORK", 1_000_000);
    let rows = env("FLOWER_BENCH_ROWS", 2000);
    let passes = env("FLOWER_BENCH_PASSES", 3);
    let dir = tempfile::tempdir().unwrap();
    let heavy = Arc::new(AtomicUsize::new(0));
    let node = member(
        dir.path(),
        Bench {
            work: rounds_of_work,
            rows,
            heavy: heavy.clone(),
        },
    )
    .await;
    let started = Instant::now();
    work(rounds_of_work);
    eprintln!(
        "guest work per evaluation: {:.2} ms; {clients} clients, {rounds} rounds, {passes} passes",
        ms(started.elapsed())
    );

    // Identical queries in flight after each write.
    for pass in 0..passes {
        for joining in [true, false] {
            node.set_query_flight_bytes(if joining {
                flower_cluster::flight::DEFAULT_FLIGHT_BYTES
            } else {
                0
            });
            let (mut times, evaluations) = burst(&node, &heavy, rounds, clients).await;
            let (p50, p99) = percentiles(&mut times);
            eprintln!(
                "pass {pass} joining {:>3}: {:.1} evaluations per round, round p50 {:.2} ms p99 {:.2} ms",
                if joining { "on" } else { "off" },
                evaluations as f64 / rounds as f64,
                ms(p50),
                ms(p99)
            );
        }
    }
    node.set_query_flight_bytes(flower_cluster::flight::DEFAULT_FLIGHT_BYTES);

    // Hits of a large result: before, the route cloned the value,
    // rendered it, parsed the rendering and rendered that again; now it
    // sends the rendering the entry keeps.
    let reader = node.reader().clone();
    let options = CallOptions::default();
    let (value, _) = reader
        .query_at(b"db", "app", "big", &Value::Null, &options)
        .unwrap();
    let size = render(&value).unwrap().len();
    let hits = 2000;
    for pass in 0..passes {
        let started = Instant::now();
        for _ in 0..hits {
            let (value, _) = reader
                .query_at(b"db", "app", "big", &Value::Null, &options)
                .unwrap();
            let json: serde_json::Value = serde_json::from_slice(&render(&value).unwrap()).unwrap();
            black_box(serde_json::to_vec(&serde_json::json!({"ok": true, "value": json})).unwrap());
        }
        let before = started.elapsed();
        let started = Instant::now();
        for _ in 0..hits {
            let (encoded, _) = reader
                .query_encoded_at(b"db", "app", "big", &Value::Null, &options)
                .unwrap();
            let mut body = Vec::with_capacity(encoded.bytes().len() + 20);
            body.extend_from_slice(br#"{"ok":true,"value":"#);
            body.extend_from_slice(encoded.bytes());
            body.push(b'}');
            black_box(body);
        }
        let after = started.elapsed();
        eprintln!(
            "pass {pass} hit of a {size}-byte result: value path {:.1} us, encoded path {:.1} us ({:.1}x)",
            before.as_secs_f64() * 1e6 / hits as f64,
            after.as_secs_f64() * 1e6 / hits as f64,
            before.as_secs_f64() / after.as_secs_f64()
        );
    }

    // A result read through derived values, after writes that leave
    // them as they were and after writes that change them.
    let report = || {
        let reader = reader.clone();
        let options = options.clone();
        async move {
            let started = Instant::now();
            reader
                .query_at(b"db", "app", "report", &Value::Null, &options)
                .unwrap();
            started.elapsed()
        }
    };
    put(&node, 0.0).await;
    report().await;
    for pass in 0..passes {
        let before = reader.query_cache_stats();
        let mut kept = Vec::new();
        let mut changed = Vec::new();
        let mut v = 0.0;
        for _ in 0..rounds {
            // Even to even: `parity` derives again and comes out the same.
            v += 2.0;
            put(&node, v).await;
            kept.push(report().await);
        }
        for _ in 0..rounds {
            // Odd and even in turn: `parity` changes every time.
            v += 1.0;
            put(&node, v).await;
            changed.push(report().await);
        }
        let after = reader.query_cache_stats();
        let (kept50, kept99) = percentiles(&mut kept);
        let (changed50, changed99) = percentiles(&mut changed);
        eprintln!(
            "pass {pass} derived input written: output unchanged p50 {:.3} ms p99 {:.3} ms ({} kept), \
             output changed p50 {:.3} ms p99 {:.3} ms",
            ms(kept50),
            ms(kept99),
            after.rederived - before.rederived,
            ms(changed50),
            ms(changed99)
        );
    }
}
