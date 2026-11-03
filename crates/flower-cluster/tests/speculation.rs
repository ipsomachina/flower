//! Speculative preparation in the writer: mutations prepared in
//! parallel waves on a snapshot of the staged state, each adopted in
//! queue order only while nothing written before it touches what it
//! read, prepared again in its turn otherwise; conflict prediction
//! placing calls on a hot record in their turn.

mod counters;

use std::collections::BTreeMap;
use std::sync::Arc;

use counters::{key, read, single_node, value};
use flower_cluster::node::Node;
use flower_cluster::speculation::SpeculationSettings;
use flower_engine::engine::MutationOptions;
use flower_sdk::Value;

fn speculating(prediction: bool) -> SpeculationSettings {
    SpeculationSettings {
        workers: 4,
        prediction,
    }
}

/// Fire one increment per name, all at once; answers by call, in the
/// order given.
async fn increments(node: &Arc<Node>, names: &[String]) -> Vec<(String, usize, u64)> {
    let tasks: Vec<_> = names
        .iter()
        .map(|name| {
            let (node, name) = (node.clone(), name.clone());
            tokio::spawn(async move {
                let response = node
                    .mutate(
                        b"db",
                        "app",
                        "incr",
                        &key(&name),
                        &MutationOptions::default(),
                    )
                    .await
                    .unwrap();
                let count = match value(&response.outcome) {
                    Value::Number(count) => count as usize,
                    other => panic!("incr answered {other:?}"),
                };
                (name, count, response.revision)
            })
        })
        .collect();
    let mut answers = Vec::new();
    for task in tasks {
        answers.push(task.await.unwrap());
    }
    answers
}

/// Increments on distinct counters read nothing another writes: their
/// speculative preparations hold and commit as prepared, with the
/// answers and revisions a serial run gives.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn independent_writes_commit_from_speculation() {
    let dir = tempfile::tempdir().unwrap();
    let node = Arc::new(single_node(dir.path()).await);
    node.set_speculation(speculating(false));
    let mut reused = 0;
    for round in 0..4 {
        let names: Vec<String> = (0..48).map(|index| format!("r{round}k{index}")).collect();
        let answers = increments(&node, &names).await;
        assert!(answers.iter().all(|(_, count, _)| *count == 1));
        for name in &names {
            assert_eq!(read(&node, name).await, 1.0);
        }
        reused = node.writer_stats().speculation.reused;
        if reused > 0 {
            break;
        }
    }
    let stats = node.writer_stats().speculation;
    assert!(reused > 0, "no candidate was reused: {stats:?}");
    assert!(stats.waves > 0 && stats.candidates >= stats.reused);
    assert_eq!(
        stats.conflicted, 0,
        "distinct counters never conflict: {stats:?}"
    );
    Arc::into_inner(node).unwrap().shutdown().await.unwrap();
}

/// Increments of a few counters in random interleavings: candidates
/// that read a counter an earlier write in their batch changed are
/// prepared again, and every counter ends as a serial run leaves it,
/// each call answering a distinct count, none lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn conflicting_writes_are_prepared_again_as_a_serial_run_would() {
    let dir = tempfile::tempdir().unwrap();
    let node = Arc::new(single_node(dir.path()).await);
    node.set_speculation(speculating(false));
    let mut expected: BTreeMap<String, usize> = BTreeMap::new();
    let mut seed = 0x9e37_79b9_7f4a_7c15_u64;
    let mut next = move || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };
    for _ in 0..6 {
        let names: Vec<String> = (0..64).map(|_| format!("c{}", next() % 5)).collect();
        let answers = increments(&node, &names).await;
        let mut by_name: BTreeMap<String, Vec<usize>> = BTreeMap::new();
        for (name, count, _) in &answers {
            by_name.entry(name.clone()).or_default().push(*count);
        }
        for (name, mut counts) in by_name {
            let before = *expected.get(&name).unwrap_or(&0);
            counts.sort_unstable();
            let calls = counts.len();
            assert_eq!(
                counts,
                (before + 1..=before + calls).collect::<Vec<_>>(),
                "{name} answered as no serial order would"
            );
            expected.insert(name, before + calls);
        }
        let mut revisions: Vec<u64> = answers.iter().map(|(_, _, revision)| *revision).collect();
        revisions.sort_unstable();
        revisions.dedup();
        assert_eq!(
            revisions.len(),
            answers.len(),
            "each write its own revision"
        );
    }
    for (name, count) in &expected {
        assert_eq!(read(&node, name).await, *count as f64);
    }
    let stats = node.writer_stats().speculation;
    assert!(
        stats.conflicted > 0,
        "hot counters must conflict: {stats:?}"
    );
    assert!(stats.conflicts.source > 0, "on their records: {stats:?}");
    Arc::into_inner(node).unwrap().shutdown().await.unwrap();
}

/// With prediction on, a method whose candidates keep failing on the
/// record its `key` argument names is placed in its turn when an
/// earlier call of the wave names the same record, while calls on
/// other records keep speculating; answers stay those of a serial run.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prediction_places_calls_on_a_hot_record_in_their_turn() {
    let dir = tempfile::tempdir().unwrap();
    let node = Arc::new(single_node(dir.path()).await);
    node.set_speculation(speculating(true));
    let mut hot = 0;
    for round in 0..8 {
        // Mostly the hot counter, some cold ones.
        let names: Vec<String> = (0..64)
            .map(|index| {
                if index % 4 == 3 {
                    format!("cold{round}x{index}")
                } else {
                    "hot".to_string()
                }
            })
            .collect();
        let answers = increments(&node, &names).await;
        let mut counts: Vec<usize> = answers
            .iter()
            .filter(|(name, _, _)| name == "hot")
            .map(|(_, count, _)| *count)
            .collect();
        counts.sort_unstable();
        assert_eq!(counts, (hot + 1..=hot + counts.len()).collect::<Vec<_>>());
        hot += counts.len();
        if node.writer_stats().speculation.placed_serially > 0 {
            break;
        }
    }
    assert_eq!(read(&node, "hot").await, hot as f64);
    let stats = node.writer_stats().speculation;
    assert!(
        stats.placed_serially > 0,
        "prediction placed no hot call: {stats:?}"
    );
    Arc::into_inner(node).unwrap().shutdown().await.unwrap();
}

/// Off (one worker), nothing speculates.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_worker_prepares_serially() {
    let dir = tempfile::tempdir().unwrap();
    let node = Arc::new(single_node(dir.path()).await);
    node.set_speculation(SpeculationSettings {
        workers: 1,
        prediction: true,
    });
    let names: Vec<String> = (0..32).map(|index| format!("k{index}")).collect();
    increments(&node, &names).await;
    assert_eq!(
        node.writer_stats().speculation,
        Default::default(),
        "nothing speculated"
    );
    Arc::into_inner(node).unwrap().shutdown().await.unwrap();
}
