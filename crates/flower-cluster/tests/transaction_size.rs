//! What one replicated entry may hold: the bound `serve
//! --max-transaction-bytes` sets, for a batch of writes, a business
//! failure and a cross-group decision alike, so no entry outgrows the
//! peer frame and stalls its group. The bound is the process's, so these
//! tests run in a binary of their own, at 64 KiB.

mod counters;
mod hosts;

use std::sync::Arc;

use counters::{key, obj, s, single_node};
use flower_cluster::node::NodeError;
use flower_cluster::types::{ClusterWrite, ResponseOutcome, WriteResponse};
use flower_engine::engine::{MultiStep, MutationOptions};
use flower_engine::patch::{TRANSACTION_TOO_LARGE, set_max_transaction_bytes};
use flower_sdk::Value;
use hosts::Hosts;
use openraft::EntryPayload;

const BOUND: usize = 64 * 1024;

fn sized(name: &str, bytes: usize) -> Value {
    obj(vec![
        ("key", s(name)),
        ("bytes", Value::Number(bytes as f64)),
    ])
}

/// The failure code a write answered with, either way it answers one.
fn code(answer: &Result<WriteResponse, NodeError>) -> Option<String> {
    match answer {
        Ok(response) => match &response.outcome {
            ResponseOutcome::Failure(bytes) => {
                let value = flower_engine::json::parse_value(bytes).unwrap();
                flower_sdk::abi::decode_envelope(value)
                    .err()
                    .map(|f| f.code)
            }
            _ => None,
        },
        Err(NodeError::Failure { code, .. } | NodeError::Call { code, .. }) => Some(code.clone()),
        Err(_) => None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_batch_never_outgrows_the_bound() {
    set_max_transaction_bytes(BOUND);
    let dir = tempfile::tempdir().unwrap();
    let node = Arc::new(single_node(dir.path()).await);
    let before = node.status().last_log.unwrap_or(0);
    // Writes of 40 KiB, all at once: any two together pass the bound,
    // so each rides an entry of its own.
    const CALLS: usize = 24;
    let calls: Vec<_> = (0..CALLS)
        .map(|index| {
            let node = node.clone();
            tokio::spawn(async move {
                node.mutate(
                    b"db",
                    "app",
                    "fill",
                    &sized(&format!("k{index}"), 40_000),
                    &MutationOptions::default(),
                )
                .await
            })
        })
        .collect();
    for call in calls {
        let answer = call.await.unwrap();
        assert_eq!(code(&answer), None, "{answer:?}");
        answer.unwrap();
    }
    let after = node.status().last_log.unwrap_or(0);
    let entries = node.logs().read_entries(before + 1, after + 1).unwrap();
    let mut batched = 0;
    for entry in &entries {
        if let EntryPayload::Normal(ClusterWrite::Batch { items, .. }) = &entry.payload {
            let bytes: usize = items.iter().map(|item| item.len()).sum();
            assert!(
                items.len() == 1 || bytes <= BOUND,
                "{} items, {bytes} bytes in one entry",
                items.len()
            );
            batched += items.len();
        }
    }
    assert_eq!(batched, CALLS);
    Arc::into_inner(node).unwrap().shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failure_too_large_to_keep_is_refused_whole() {
    set_max_transaction_bytes(BOUND);
    let dir = tempfile::tempdir().unwrap();
    let node = single_node(dir.path()).await;
    // A business failure replicates (with its receipt, here) like any
    // effect: one past the bound is refused, never proposed.
    for options in [
        MutationOptions::default(),
        MutationOptions {
            idempotency_key: Some(b"once".to_vec()),
            ..MutationOptions::default()
        },
    ] {
        let answer = node
            .mutate(b"db", "app", "fail_big", &sized("k", 100_000), &options)
            .await;
        assert_eq!(
            code(&answer).as_deref(),
            Some(TRANSACTION_TOO_LARGE),
            "{answer:?}"
        );
    }
    node.shutdown().await.unwrap();
}

fn step(db: &str, method: &str, bytes: usize) -> MultiStep {
    MultiStep {
        db: db.as_bytes().to_vec(),
        module: "app".into(),
        method: method.into(),
        args: sized("k", bytes),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_transaction_across_groups_stays_within_the_bound() {
    set_max_transaction_bytes(BOUND);
    // Root on nodes 1-3 holds `a`; `east` on nodes 2-3 holds `b`.
    let hosts = Hosts::start(3).await;
    hosts.group("east", vec![2, 3]).await;
    hosts.deploy("a").await;
    hosts.partition("b", "east").await;
    let host = hosts.host(1);
    // Each step fits its own group; the decision carrying both results
    // would not: the transaction aborts as too large, a 413.
    let both = [step("a", "big", 40_000), step("b", "big", 40_000)];
    let answer = host
        .distributed_transaction(&both, &MutationOptions::default())
        .await;
    assert_eq!(
        code(&answer).as_deref(),
        Some(TRANSACTION_TOO_LARGE),
        "{answer:?}"
    );
    // A step too large for its own group aborts the same way, under the
    // same code: not `transaction_aborted`, which is a 422.
    let one = [step("a", "big", 10), step("b", "big", 100_000)];
    let answer = host
        .distributed_transaction(&one, &MutationOptions::default())
        .await;
    assert_eq!(
        code(&answer).as_deref(),
        Some(TRANSACTION_TOO_LARGE),
        "{answer:?}"
    );
    // Nothing changed anywhere: a small transaction still commits.
    let small = [step("a", "incr", 0), step("b", "incr", 0)];
    let answer = host
        .distributed_transaction(&small, &MutationOptions::default())
        .await;
    assert_eq!(code(&answer), None, "{answer:?}");
    let _ = key("k");
    drop(host);
    hosts.stop().await;
}
