//! Live partition moves: pre-copy while the source serves, freeze,
//! the difference, cutover at the next epoch, and every way a move can
//! be interrupted (abort, a transaction holding the partition, crashes
//! and leadership changes between any two steps). A partition's managed
//! keys move with it, and a move holds until its destination can unwrap
//! them. Every group times Raft as the root does, and a stopping process
//! hands off every group it leads.

mod counters;
mod hosts;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use counters::key;
use flower_cluster::keys::KEYS_LOCKED;
use flower_cluster::node::Node;
use flower_cluster::txn::{CallWire, PrepareRequest, StepWire};
use flower_cluster::types::ResponseOutcome;
use flower_crypto::replicated::{Declaration, ManagedKeys, ManagedOutput, WrappingKey};
use flower_engine::catalog::{MovePhase, PartitionStatus};
use flower_engine::engine::{CallOptions, MutationOptions, PrepareOutcome, RetryIdentity};
use flower_engine::placement::{PARTITION_MOVED, PARTITION_MOVING, Placement, PlacementCommand};
use flower_engine::retention::{
    Acknowledgement, RetentionCommand, request_fingerprint, session_intent,
};
use flower_engine::retry::ScopedIdentity;
use flower_engine::txn::PlacedDb;
use hosts::{Hosts, incr, read};

/// Root on nodes 1-3; `east` on 1-2 holds `acme`; `west` on 3.
async fn layout(settings: fn(&mut flower_cluster::host::HostSettings)) -> Hosts {
    let hosts = Hosts::start_with(3, settings).await;
    hosts.group("east", vec![1, 2]).await;
    hosts.group("west", vec![3]).await;
    hosts.partition("acme", "east").await;
    hosts
}

/// Increment `acme` through whichever group owns it, retrying while it
/// moves, as a routed client does: the count that committed.
async fn incr_routed(hosts: &Hosts) -> f64 {
    for _ in 0..400 {
        let owner = hosts.owner("acme").await;
        match incr(&owner, "acme").await {
            Ok(count) => return count,
            Err(error)
                if error.to_string().contains(PARTITION_MOVING)
                    || error.to_string().contains(PARTITION_MOVED)
                    || error.to_string().contains("forward") =>
            {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(error) => panic!("increment failed: {error}"),
        }
    }
    panic!("acme never took the write");
}

async fn moved(hosts: &Hosts, phase: MovePhase) {
    hosts
        .until(&format!("the move {phase:?}"), |catalog| {
            let entry = &catalog.partitions["acme"];
            match &entry.movement {
                Some(movement) => movement.phase == phase,
                None => catalog.moves.last().is_some_and(|last| last.phase == phase),
            }
        })
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_partition_moves_live_without_losing_or_repeating_a_write() {
    let hosts = Arc::new(layout(|_| {}).await);
    for _ in 0..30 {
        incr_routed(&hosts).await;
    }
    // A writer keeps writing through the whole move.
    let (stop, written) = (
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicU64::new(30)),
    );
    let writer = {
        let (hosts, stop, written) = (hosts.clone(), stop.clone(), written.clone());
        tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                let count = incr_routed(&hosts).await;
                // Every write lands exactly once: counts never skip or
                // repeat.
                assert_eq!(count, (written.fetch_add(1, Ordering::SeqCst) + 1) as f64);
            }
        })
    };
    // A reader keeps reading through it too, from whichever group owns
    // `acme`: it sees a count no writer has passed, or a refusal it
    // retries, never a method the partition lacks.
    let reader = {
        let (hosts, stop, written) = (hosts.clone(), stop.clone(), written.clone());
        tokio::spawn(async move {
            let mut reads = 0;
            while !stop.load(Ordering::Relaxed) {
                let owner = hosts.owner("acme").await;
                let answer = owner
                    .query(
                        b"acme",
                        "app",
                        "read",
                        &key("k"),
                        &flower_engine::engine::CallOptions::default(),
                    )
                    .await;
                drop(owner);
                match answer {
                    Ok(flower_sdk::Value::Number(count)) => {
                        assert!(count <= written.load(Ordering::SeqCst) as f64 + 1.0);
                        reads += 1;
                    }
                    Err(error)
                        if error.to_string().contains(PARTITION_MOVING)
                            || error.to_string().contains(PARTITION_MOVED)
                            || error.to_string().contains("forward") =>
                    {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                    other => panic!("read failed: {other:?}"),
                }
            }
            reads
        })
    };
    hosts
        .any()
        .move_partition("acme", "west", "move-1")
        .await
        .unwrap()
        .unwrap();
    moved(&hosts, MovePhase::Complete).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    stop.store(true, Ordering::Relaxed);
    writer.await.unwrap();
    assert!(reader.await.unwrap() > 0);
    let total = written.load(Ordering::SeqCst) as f64;
    // The destination holds every write, at the next epoch, and serves
    // the moved code.
    let catalog = hosts.any().catalog().await.unwrap();
    let entry = &catalog.partitions["acme"];
    assert_eq!((entry.owner.as_str(), entry.epoch), ("west", 2));
    assert_eq!(entry.status, PartitionStatus::Active);
    let west = hosts.leader_of("west").await;
    assert_eq!(read(&west, "acme").await, total);
    assert_eq!(
        west.placement(b"acme").unwrap(),
        Some(Placement::Active { epoch: 2 })
    );
    assert_eq!(incr(&west, "acme").await.unwrap(), total + 1.0);
    // The source dropped its copy and points at the new owner.
    let east = hosts.leader_of("east").await;
    assert!(matches!(
        east.placement(b"acme").unwrap(),
        Some(Placement::Retired { moved_to: Some(ref to), .. }) if to == "west"
    ));
    let refused = incr(&east, "acme").await.unwrap_err();
    assert!(refused.to_string().contains(PARTITION_MOVED), "{refused}");
    drop((west, east));
    Arc::try_unwrap(hosts)
        .unwrap_or_else(|_| panic!("hosts are still shared"))
        .stop()
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_prepared_before_the_freeze_is_refused_after_cutover() {
    let hosts = layout(|_| {}).await;
    incr_routed(&hosts).await;
    let east = hosts.leader_of("east").await;
    // Prepared at the source while it serves, at epoch 1...
    let prepared = east
        .with_engine(|engine| {
            match engine
                .prepare_mutation(
                    b"acme",
                    "app",
                    "incr",
                    &key("k"),
                    &MutationOptions::default(),
                )
                .map_err(|failure| flower_cluster::node::NodeError::Failure {
                    code: failure.code,
                    message: failure.message,
                })? {
                PrepareOutcome::Patch(prepared) => Ok(prepared),
                other => panic!("prepared nothing: {other:?}"),
            }
        })
        .await
        .unwrap();
    hosts
        .any()
        .move_partition("acme", "west", "move-2")
        .await
        .unwrap()
        .unwrap();
    moved(&hosts, MovePhase::Complete).await;
    // ...and proposed at the old owner after the cutover: refused on
    // every member, and the moved partition never sees it.
    let response = east
        .raft()
        .client_write(flower_cluster::types::ClusterWrite::Patch {
            db: b"acme".to_vec(),
            patch: prepared.patch,
            proposed_ms: 0,
        })
        .await
        .unwrap()
        .data;
    let refusal = format!("{:?}", response.outcome);
    assert!(refusal.contains(PARTITION_MOVED), "{refusal}");
    assert_eq!(read(&hosts.leader_of("west").await, "acme").await, 1.0);
    drop(east);
    hosts.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_aborted_move_serves_from_its_source_at_a_later_epoch() {
    let mut hosts = layout(|_| {}).await;
    incr_routed(&hosts).await;
    // The destination is down: the move cannot copy.
    hosts.crash(3).await;
    hosts
        .any()
        .move_partition("acme", "west", "move-3")
        .await
        .unwrap()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    hosts
        .any()
        .abort_move("acme", "move-3", "operator")
        .await
        .unwrap()
        .unwrap();
    // The source thaws first: it serves again, at the epoch after the
    // transfer's, while the destination is still down.
    moved(&hosts, MovePhase::Abandoning).await;
    let catalog = hosts.any().catalog().await.unwrap();
    assert_eq!(catalog.partitions["acme"].epoch, 3);
    assert_eq!(incr_routed(&hosts).await, 2.0);
    let east = hosts.leader_of("east").await;
    assert_eq!(
        east.placement(b"acme").unwrap(),
        Some(Placement::Active { epoch: 3 })
    );
    // Back, the destination drops what it imported and keeps a marker.
    hosts.restart(3).await;
    moved(&hosts, MovePhase::Aborted).await;
    let west = hosts.leader_of("west").await;
    assert!(matches!(
        west.placement(b"acme").unwrap(),
        Some(Placement::Retired { epoch: 2, .. })
    ));
    let catalog = hosts.any().catalog().await.unwrap();
    let last = catalog.moves.last().unwrap();
    assert_eq!(last.reason.as_deref(), Some("operator"));
    // A later move goes through.
    hosts
        .any()
        .move_partition("acme", "west", "move-4")
        .await
        .unwrap()
        .unwrap();
    moved(&hosts, MovePhase::Complete).await;
    assert_eq!(read(&hosts.owner("acme").await, "acme").await, 2.0);
    assert_eq!(
        hosts.any().catalog().await.unwrap().partitions["acme"].epoch,
        4
    );
    drop((east, west));
    hosts.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_move_waits_for_a_prepared_transaction_and_fences_it_afterwards() {
    let hosts = layout(|_| {}).await;
    incr_routed(&hosts).await;
    let east = hosts.leader_of("east").await;
    let prepare = |id: &str, epoch: u64| PrepareRequest {
        id: id.into(),
        coordinator: "root".into(),
        coordinator_uid: 0,
        dbs: vec![PlacedDb {
            db: "acme".into(),
            epoch,
            steps: vec![0],
        }],
        steps: vec![StepWire {
            index: 0,
            db: "acme".into(),
            module: "app".into(),
            method: "incr".into(),
            args: "{\"key\":\"k\"}".into(),
        }],
        options: CallWire::default(),
    };
    // A prepared transaction holds `acme` (its coordinator never began
    // it, so its participant's resolver aborts it after a while).
    east.prepare_txn(prepare("0a", 1)).await.unwrap().unwrap();
    let started = Instant::now();
    hosts
        .any()
        .move_partition("acme", "west", "move-5")
        .await
        .unwrap()
        .unwrap();
    moved(&hosts, MovePhase::Complete).await;
    // The freeze waited for the lock to go: never over a prepared
    // transaction.
    assert!(started.elapsed() >= Duration::from_secs(1));
    assert_eq!(read(&hosts.owner("acme").await, "acme").await, 1.0);
    // A transaction planned against the old placement is refused at both
    // ends: the source no longer serves it, the destination serves at
    // epoch 2.
    let refused = east
        .prepare_txn(prepare("0b", 1))
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(refused.code, PARTITION_MOVED);
    let west = hosts.leader_of("west").await;
    let refused = west
        .prepare_txn(prepare("0c", 1))
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(refused.code, flower_engine::txn::TXN_PLACEMENT);
    west.prepare_txn(prepare("0d", 2)).await.unwrap().unwrap();
    drop((east, west));
    hosts.stop().await;
}

/// Increment `acme`'s counter at `node` under scoped identity
/// `incarnation`/`epoch`/`intent` for `caller`: the count, or the code
/// of the refusal.
async fn scoped_incr(
    node: &flower_cluster::node::Node,
    incarnation: u64,
    intent: Vec<u8>,
    caller: &CallOptions,
) -> Result<f64, String> {
    let options = MutationOptions {
        call: caller.clone(),
        retry: Some(RetryIdentity {
            identity: ScopedIdentity {
                incarnation,
                epoch: 0,
                intent,
            },
            fingerprint: request_fingerprint("app", "incr", &key("k"), None).unwrap(),
        }),
        ..MutationOptions::default()
    };
    let response = node
        .mutate(b"acme", "app", "incr", &key("k"), &options)
        .await
        .unwrap();
    match &response.outcome {
        ResponseOutcome::Value(_) => match counters::value(&response.outcome) {
            flower_sdk::Value::Number(count) => Ok(count),
            other => panic!("incr answered {other:?}"),
        },
        ResponseOutcome::Failure(bytes) => {
            let envelope = flower_engine::json::parse_value(bytes).unwrap();
            Err(flower_sdk::abi::decode_envelope(envelope).unwrap_err().code)
        }
        ResponseOutcome::Error(message) => Err(message.split(": ").next().unwrap().to_string()),
    }
}

/// Retry identities and sessions from before a move keep their history
/// at the destination, whose group has another incarnation: results
/// replay, sessions acknowledge and run new sequences, and collection
/// keeps what retention still promises. (Before the move's lineage held
/// them, every one answered `wrong_history` and a walk dropped them.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retry_identities_and_sessions_keep_their_history_across_a_move() {
    let hosts = layout(|_| {}).await;
    let east = hosts.leader_of("east").await;
    let rotate = RetentionCommand::Rotate {
        epoch_ms: None,
        keep_epochs: 2,
    };
    let before = east.establish_incarnation().await.unwrap();
    assert_ne!(before, 0);
    assert_eq!(
        east.retry_status(b"acme").await.unwrap().incarnation,
        before
    );
    east.retention(b"acme", rotate).await.unwrap().unwrap();
    let ada = CallOptions {
        principal: Some(flower_sdk::Principal::new("ada".to_string(), None, None).unwrap()),
        ..CallOptions::default()
    };
    let (session, closing) = ([5u8; 16], [6u8; 16]);
    for id in [session, closing] {
        east.open_retry_session(b"acme", "app", id, &ada)
            .await
            .unwrap()
            .unwrap();
    }
    let sequence = |sequence| session_intent(&session, sequence);
    let once = vec![3u8; 32];
    assert_eq!(scoped_incr(&east, before, sequence(1), &ada).await, Ok(1.0));
    assert_eq!(scoped_incr(&east, before, sequence(2), &ada).await, Ok(2.0));
    assert_eq!(
        scoped_incr(&east, before, once.clone(), &ada).await,
        Ok(3.0)
    );
    drop(east);
    hosts
        .any()
        .move_partition("acme", "west", "move-retry")
        .await
        .unwrap()
        .unwrap();
    moved(&hosts, MovePhase::Complete).await;
    let west = hosts.leader_of("west").await;
    // `west` had no incarnation yet when `acme` arrived (its maintenance
    // does not run here): what `acme` carried in is its history.
    assert_eq!(west.retry_status(b"acme").await.unwrap().incarnation, 0);
    assert_eq!(scoped_incr(&west, before, sequence(2), &ada).await, Ok(2.0));
    // Establishing one keeps that history.
    let after = west.establish_incarnation().await.unwrap();
    assert_ne!(after, 0);
    assert_ne!(after, before, "the destination group has its own history");
    assert_eq!(west.retry_status(b"acme").await.unwrap().incarnation, after);
    // Replays answer the original outcomes.
    assert_eq!(scoped_incr(&west, before, sequence(2), &ada).await, Ok(2.0));
    assert_eq!(
        scoped_incr(&west, before, once.clone(), &ada).await,
        Ok(3.0)
    );
    // The session acknowledges, refuses what it acknowledged and runs
    // new sequences.
    west.acknowledge_retry_session(b"acme", "app", session, Acknowledgement::through(1), &ada)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        scoped_incr(&west, before, sequence(1), &ada).await,
        Err("already_acknowledged".to_string())
    );
    assert_eq!(scoped_incr(&west, before, sequence(3), &ada).await, Ok(4.0));
    // A walk (closing the other session starts one) keeps the results
    // and the session retention still promises.
    west.close_retry_session(b"acme", "app", closing, &ada)
        .await
        .unwrap()
        .unwrap();
    loop {
        let answer = west
            .retention(b"acme", RetentionCommand::Collect { limit: 1 })
            .await
            .unwrap()
            .unwrap();
        let flower_sdk::Value::Object(fields) = answer else {
            panic!("collect answered {answer:?}");
        };
        if fields.get("complete") == Some(&flower_sdk::Value::Bool(true)) {
            break;
        }
    }
    assert_eq!(scoped_incr(&west, before, sequence(2), &ada).await, Ok(2.0));
    assert_eq!(scoped_incr(&west, before, once, &ada).await, Ok(3.0));
    let status = west
        .retry_session_status(b"acme", "app", session, &ada)
        .await
        .unwrap()
        .unwrap();
    let flower_sdk::Value::Object(status) = status else {
        panic!("status answered {status:?}");
    };
    assert_eq!(
        status.get("acknowledged"),
        Some(&flower_sdk::Value::Number(1.0))
    );
    assert_eq!(read(&west, "acme").await, 4.0);
    // The source keeps `acme` registered, retired: its leader's default
    // retention passes it over rather than propose what it would refuse.
    let east = hosts.leader_of("east").await;
    east.set_retry_window(Some(Duration::from_secs(16)));
    let before = east.status().last_log;
    east.maintain_retention(Duration::from_millis(100)).await;
    assert_eq!(east.status().last_log, before);
    drop((east, west));
    hosts.stop().await;
}

/// A point-in-time restore of a group, to a point after a database
/// moved into it, ends the history that database carried in: a retry
/// session opened at the source is refused, and so is a sequence that
/// ran in the live history after the restore point, which therefore
/// never runs twice. (Before, replay recorded the move's arrival under
/// no incarnation, and the restored group's first incarnation adopted
/// that lineage, reviving the old history.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restore_after_a_move_ends_the_history_the_move_carried_in() {
    use flower_backup::driver::{Driver, RuntimeBuilder};
    use flower_cluster::statemachine::RuntimeFactory;
    let hosts = layout(|_| {}).await;
    let east = hosts.leader_of("east").await;
    let before = east.establish_incarnation().await.unwrap();
    let rotate = RetentionCommand::Rotate {
        epoch_ms: None,
        keep_epochs: 2,
    };
    east.retention(b"acme", rotate).await.unwrap().unwrap();
    let ada = CallOptions {
        principal: Some(flower_sdk::Principal::new("ada".to_string(), None, None).unwrap()),
        ..CallOptions::default()
    };
    let session = [7u8; 16];
    east.open_retry_session(b"acme", "app", session, &ada)
        .await
        .unwrap()
        .unwrap();
    let sequence = |sequence| session_intent(&session, sequence);
    assert_eq!(scoped_incr(&east, before, sequence(1), &ada).await, Ok(1.0));
    drop(east);
    // `west` backs up before `acme` arrives.
    let west = hosts.leader_of("west").await;
    let destination = west.establish_incarnation().await.unwrap();
    assert_ne!(destination, before);
    let builder: RuntimeBuilder = Arc::new(
        |db: &[u8], module: &str, wasm: &[u8], manifest: &flower_sdk::Value| {
            counters::Counters.build(db, module, wasm, manifest)
        },
    );
    let dir = tempfile::tempdir().unwrap();
    let store = Driver::memory_store();
    let keystore = dir.path().join("keystore.json");
    let driver = Driver::new(
        store.clone(),
        &keystore,
        [5u8; 32],
        west.engine().clone(),
        west.logs().clone(),
        builder.clone(),
    )
    .unwrap();
    driver.backup_now().await.unwrap();
    hosts
        .any()
        .move_partition("acme", "west", "move-restore")
        .await
        .unwrap()
        .unwrap();
    moved(&hosts, MovePhase::Complete).await;
    let now = || {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    let target = now();
    tokio::time::sleep(Duration::from_millis(100)).await;
    // Past the restore point, the live history runs sequence 2.
    assert_eq!(scoped_incr(&west, before, sequence(2), &ada).await, Ok(2.0));
    driver.archive_now().await.unwrap();
    drop((west, driver));

    let offline = Driver::offline(store, &keystore, [5u8; 32], builder).unwrap();
    let restored = dir.path().join("restored").join("group.redb");
    std::fs::create_dir_all(restored.parent().unwrap()).unwrap();
    offline.pitr(&restored, target).await.unwrap();
    // The restored file itself holds no history: no incarnation and no
    // lineage anything could adopt, whoever opens it.
    {
        let store = flower_engine::storage::GroupStore::open(&restored).unwrap();
        let read = store.begin_read().unwrap();
        let history = flower_engine::placement::History::read(&read, b"acme").unwrap();
        assert_eq!(history, flower_engine::placement::History::default());
        let lineage = read
            .meta(b"acme", flower_engine::storage::meta::HISTORY_LINEAGE)
            .unwrap();
        assert_eq!(lineage, None);
    }
    let registry = flower_cluster::network::Registry::new();
    let node = Arc::new(
        flower_cluster::node::Node::open(
            1,
            restored.parent().unwrap(),
            counters::config(),
            flower_cluster::network::InProcessFactory::new(registry.clone()),
            Arc::new(counters::Counters),
        )
        .await
        .unwrap(),
    );
    registry.register(1, node.raft().clone());
    node.initialize(std::collections::BTreeSet::from([1]))
        .await
        .unwrap();
    counters::wait_leader(&[&*node]).await;
    let restored_history = node.establish_incarnation().await.unwrap();
    assert!(![0, before, destination].contains(&restored_history));
    // The restore point: the move, without sequence 2.
    assert_eq!(read(&node, "acme").await, 1.0);
    for intent in [sequence(2), sequence(3)] {
        assert_eq!(
            scoped_incr(&node, before, intent, &ada).await,
            Err("wrong_history".to_string())
        );
    }
    let reopened = node
        .open_retry_session(b"acme", "app", session, &ada)
        .await
        .unwrap();
    assert_eq!(reopened.unwrap_err().code, "wrong_history");
    assert_eq!(read(&node, "acme").await, 1.0);
    Arc::try_unwrap(node)
        .unwrap_or_else(|_| panic!("the restored node is still shared"))
        .shutdown()
        .await
        .unwrap();
    hosts.stop().await;
}

/// Hosts that never drive moves themselves: the test steps them.
fn stepped(settings: &mut flower_cluster::host::HostSettings) {
    settings.drive = false;
}

/// [`stepped`], with a leader lease longer than the quorum loss a test
/// holds on purpose: a leader cut off from its quorum for a lease gives
/// up the writes it waits on (`leader_isolated`, see
/// `flower_cluster::writer`), and such a test needs them to wait.
fn patient(settings: &mut flower_cluster::host::HostSettings) {
    stepped(settings);
    settings.raft = Arc::new(openraft::Config {
        leader_lease: 2_000,
        ..openraft::Config::default()
    });
}

/// Hosts timing Raft as `serve` does, from the default timing flags.
fn production(settings: &mut flower_cluster::host::HostSettings) {
    use flower_cluster::timing::{RaftTiming, SnapshotSettings, TimingSettings, raft_config};
    let timing = RaftTiming::derive(&TimingSettings::default()).unwrap();
    settings.raft = Arc::new(raft_config(&timing, &SnapshotSettings::default()).unwrap());
}

/// Every group a process hosts times Raft as its root does: the same
/// heartbeat, RPC deadline, leader lease, elections, pre-vote and read
/// lease, so each fails over in the same bound and keeps the same
/// read-lease safety. A partition then moves under that timing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_group_times_raft_as_the_root_and_moves_under_it() {
    let hosts = Arc::new(layout(production).await);
    for id in 1..=3 {
        let host = hosts.host(id);
        let root = host.root().raft().config().clone();
        assert_eq!(root.heartbeat_interval, 50);
        assert_eq!(root.rpc_timeout(), Duration::from_millis(500));
        assert_eq!(root.leader_lease(), Duration::from_millis(300));
        assert_eq!(
            (root.election_timeout_min, root.election_timeout_max),
            (100, 200)
        );
        assert!(root.pre_vote);
        let root_reads = host.root().read_settings();
        assert_eq!(root_reads.lease, Some(Duration::from_millis(270)));
        let groups = host.groups();
        assert!(groups.len() >= 2, "node {id} hosts {}", groups.len());
        for node in groups {
            let config = node.raft().config();
            assert_eq!(
                (
                    config.heartbeat_interval,
                    config.rpc_timeout(),
                    config.leader_lease(),
                    config.election_timeout_min,
                    config.election_timeout_max,
                    config.pre_vote,
                    node.read_settings().lease,
                ),
                (
                    root.heartbeat_interval,
                    root.rpc_timeout(),
                    root.leader_lease(),
                    root.election_timeout_min,
                    root.election_timeout_max,
                    root.pre_vote,
                    root_reads.lease,
                ),
                "group {} on node {id}",
                node.group()
            );
        }
    }
    for _ in 0..5 {
        incr_routed(&hosts).await;
    }
    // A group's leadership moves within it, through the same listener as
    // every other group's, well inside an election timeout.
    let east = hosts.leader_of("east").await;
    let other = if east.status().id == 1 { 2 } else { 1 };
    let moved_to = east.transfer_leadership(other).await.unwrap();
    assert_eq!((moved_to.to, moved_to.unchanged), (other, false));
    assert!(moved_to.elapsed_ms < 1_100, "{}ms", moved_to.elapsed_ms);
    drop(east);
    assert_eq!(hosts.leader_of("east").await.status().id, other);
    hosts
        .any()
        .move_partition("acme", "west", "move-8")
        .await
        .unwrap()
        .unwrap();
    moved(&hosts, MovePhase::Complete).await;
    assert_eq!(incr_routed(&hosts).await, 6.0);
    Arc::try_unwrap(hosts)
        .unwrap_or_else(|_| panic!("hosts are still shared"))
        .stop()
        .await;
}

/// A stopping process hands every group it leads to a caught-up voter,
/// all at once, as `serve` does on a shutdown signal: when it goes, no
/// group waits out a lease or holds an election.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stopping_host_hands_off_every_group_it_leads() {
    let mut hosts = Hosts::start_with(3, production).await;
    hosts.group("east", vec![1, 2, 3]).await;
    hosts.group("west", vec![2, 3]).await;
    hosts.partition("acme", "east").await;
    // Node 1 leads the root and `east`, and does not host `west`.
    let mut terms = BTreeMap::new();
    for group in ["root", "east"] {
        let leader = hosts.leader_of(group).await;
        if leader.status().id != 1 {
            leader.transfer_leadership(1).await.unwrap();
        }
        drop(leader);
        let leader = hosts.leader_of(group).await;
        assert_eq!(leader.status().id, 1, "{group}");
        terms.insert(group.to_string(), leader.status().term);
    }
    let mut handed = BTreeMap::new();
    for (group, outcome) in hosts.host(1).hand_off_leadership().await {
        let outcome = outcome
            .unwrap()
            .unwrap_or_else(|| panic!("node 1 led {group}"));
        assert_eq!(
            (outcome.from, outcome.term, outcome.unchanged),
            (1, terms[&*group.name] + 1, false),
            "{group}"
        );
        assert_ne!(outcome.to, 1);
        assert!(outcome.elapsed_ms < 300, "{group}: {outcome:?}");
        handed.insert(group.name.to_string(), outcome);
    }
    assert_eq!(handed.len(), 2, "{handed:?}");
    // Node 1 goes; the groups it led go on under the members it chose,
    // in the same terms, past a lease and more.
    hosts.crash(1).await;
    incr_routed(&hosts).await;
    tokio::time::sleep(Duration::from_millis(600)).await;
    for (group, outcome) in &handed {
        let leader = hosts.leader_of(group).await;
        assert_eq!(
            (leader.status().id, leader.status().term),
            (outcome.to, outcome.term),
            "{group}"
        );
    }
    hosts.stop().await;
}

/// Take one driver step from the root's leader.
async fn step(hosts: &Hosts) {
    let leader = hosts.leader_of("root").await;
    let host = hosts.host(leader.status().id);
    drop(leader);
    flower_cluster::moves::drive_once(&host).await.unwrap();
}

/// The move's phase in the catalog, `None` once finished.
async fn phase(hosts: &Hosts) -> Option<MovePhase> {
    let catalog = hosts.any().catalog().await.unwrap();
    catalog.partitions["acme"]
        .movement
        .as_ref()
        .map(|movement| movement.phase)
}

/// At cutover the destination's activation is in flight: the writer
/// stages it, and prepares later writes on top, but the partition's code
/// loads only as it applies. A write there meanwhile is answered as
/// still moving, which a routed client retries, never as a method the
/// partition lacks; once the activation applies it is served.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_while_the_destination_activates_is_retried_never_refused() {
    // West's leader goes without its quorum for longer than the default
    // lease below, and must keep waiting on the activation meanwhile.
    let mut hosts = Hosts::start_with(3, patient).await;
    hosts.group("east", vec![1]).await;
    hosts.group("west", vec![2, 3]).await;
    hosts.partition("acme", "east").await;
    // Node 1 leads the root, so west's follower going down leaves the
    // root's leader standing rather than waiting out the longer lease.
    let root = hosts.leader_of("root").await;
    if root.status().id != 1 {
        root.transfer_leadership(1).await.unwrap();
    }
    drop(root);
    assert_eq!(hosts.leader_of("root").await.status().id, 1);
    for _ in 0..3 {
        incr_routed(&hosts).await;
    }
    hosts
        .any()
        .move_partition("acme", "west", "move-7")
        .await
        .unwrap()
        .unwrap();
    // Step to the cutover: recorded in the catalog, not yet activated.
    for attempt in 0.. {
        if phase(&hosts).await == Some(MovePhase::Activating) {
            break;
        }
        step(&hosts).await;
        assert!(attempt < 200, "the move never reached its cutover");
    }
    // West's follower goes down: its leader stages the activation but
    // cannot commit it.
    let west = hosts.leader_of("west").await;
    let follower = if west.status().id == 2 { 3 } else { 2 };
    hosts.crash(follower).await;
    let activation = {
        let west = west.clone();
        tokio::spawn(async move {
            west.place(
                b"acme",
                PlacementCommand::Activate {
                    epoch: 2,
                    op: "move-7".into(),
                },
            )
            .await
        })
    };
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let metrics = west.raft().metrics().borrow().clone();
        if metrics.last_log_index > metrics.last_applied.map(|log| log.index) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the activation was never proposed"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // A write now prepares on the staged activation; its answer waits
    // for the activation's batch, as every answer waits for its group's.
    let write = {
        let west = west.clone();
        tokio::spawn(async move { incr(&west, "acme").await })
    };
    tokio::time::sleep(Duration::from_millis(500)).await;
    // Back to a quorum, the activation applies: the write prepared
    // before it is answered as moving, and the next one is served.
    hosts.restart(follower).await;
    activation.await.unwrap().unwrap().unwrap();
    let refused = write.await.unwrap().unwrap_err();
    assert!(refused.to_string().contains(PARTITION_MOVING), "{refused}");
    assert_eq!(incr(&west, "acme").await.unwrap(), 4.0);
    for attempt in 0.. {
        if phase(&hosts).await.is_none() {
            break;
        }
        step(&hosts).await;
        assert!(attempt < 200, "the move never finished");
    }
    assert_eq!(read(&hosts.owner("acme").await, "acme").await, 4.0);
    drop(west);
    hosts.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_move_survives_a_crash_or_a_leader_change_between_any_two_steps() {
    let mut hosts = layout(stepped).await;
    for _ in 0..10 {
        incr_routed(&hosts).await;
    }
    hosts
        .any()
        .move_partition("acme", "west", "move-6")
        .await
        .unwrap()
        .unwrap();
    let mut seen = Vec::new();
    let mut round = 0;
    while let Some(current) = phase(&hosts).await {
        seen.push(current);
        // Between steps, take down in turn the source's leader process,
        // the destination's, and the root's (the driver's), or move the
        // root's leadership.
        match round % 4 {
            0 => {
                let east = hosts.leader_of("east").await.status().id;
                hosts.crash(east).await;
                hosts.restart(east).await;
            }
            1 => {
                hosts.crash(3).await;
                hosts.restart(3).await;
            }
            2 => {
                let root = hosts.leader_of("root").await.status().id;
                hosts.crash(root).await;
                hosts.restart(root).await;
            }
            _ => {
                let root = hosts.leader_of("root").await;
                let other = if root.status().id == 1 { 2 } else { 1 };
                let _ = root.transfer_leadership(other).await;
            }
        }
        round += 1;
        // A step may fail while the groups elect leaders: the next
        // attempt repeats it.
        for attempt in 0.. {
            let before = phase(&hosts).await;
            step(&hosts).await;
            if phase(&hosts).await != before {
                break;
            }
            assert!(attempt < 200, "the move is stuck in {before:?}");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    assert_eq!(
        seen,
        vec![
            MovePhase::Copying,
            MovePhase::Freezing,
            MovePhase::Importing,
            MovePhase::Activating,
            MovePhase::Retiring
        ]
    );
    let owner = hosts.owner("acme").await;
    assert_eq!(owner.group().name.as_ref(), "west");
    assert_eq!(read(&owner, "acme").await, 10.0);
    assert_eq!(incr(&owner, "acme").await.unwrap(), 11.0);
    drop(owner);
    hosts.stop().await;
}

/// [`stepped`], with a move allowed to keep its partition unserved for
/// two seconds.
fn freeze_limited(settings: &mut flower_cluster::host::HostSettings) {
    stepped(settings);
    settings.freeze_max = Duration::from_secs(2);
}

/// Step the move until its phase changes, retrying while groups elect.
async fn advance(hosts: &Hosts) -> Option<MovePhase> {
    let before = phase(hosts).await;
    for _ in 0..200 {
        step(hosts).await;
        let now = phase(hosts).await;
        if now != before {
            return now;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("the move is stuck in {before:?}");
}

/// A move whose source froze and whose destination then went away
/// gives itself up once the partition stayed unserved past the limit:
/// the source serves again without an operator, and the abort finishes
/// when the destination returns.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_move_unserved_past_its_limit_gives_itself_up() {
    let mut hosts = layout(freeze_limited).await;
    assert_eq!(incr_routed(&hosts).await, 1.0);
    hosts
        .any()
        .move_partition("acme", "west", "move-8")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(advance(&hosts).await, Some(MovePhase::Freezing));
    assert_eq!(advance(&hosts).await, Some(MovePhase::Importing));
    // The source is frozen; the destination goes before it imports.
    hosts.crash(3).await;
    step(&hosts).await;
    assert_eq!(phase(&hosts).await, Some(MovePhase::Importing));
    tokio::time::sleep(Duration::from_millis(2_100)).await;
    assert_eq!(advance(&hosts).await, Some(MovePhase::Aborting));
    assert_eq!(advance(&hosts).await, Some(MovePhase::Abandoning));
    assert_eq!(incr_routed(&hosts).await, 2.0);
    assert_eq!(
        hosts.any().catalog().await.unwrap().partitions["acme"].epoch,
        3
    );
    hosts.restart(3).await;
    assert_eq!(advance(&hosts).await, None);
    let catalog = hosts.any().catalog().await.unwrap();
    let last = catalog.moves.last().unwrap();
    assert_eq!(last.phase, MovePhase::Aborted);
    assert_eq!(
        last.reason.as_deref(),
        Some("the partition stayed unserved past 2000 ms")
    );
    assert_eq!(incr_routed(&hosts).await, 3.0);
    hosts.stop().await;
}

/// The wrapping key node 3 (`west`) mounts in
/// [`a_locked_destination_holds_the_move_while_its_source_serves`]: a
/// wrong one until it restarts with the cluster's.
static COPY_WEST_KEY: AtomicU8 = AtomicU8::new(2);
/// The same for [`keys_changed_after_the_copy_hold_the_cutover`].
static IMPORT_WEST_KEY: AtomicU8 = AtomicU8::new(2);

/// Every member's groups mount wrapping key 1, except node 3's, which
/// mount `west`'s.
fn wrapping(node: Node, west: &AtomicU8) -> Node {
    let byte = if node.id() == 3 {
        west.load(Ordering::SeqCst)
    } else {
        1
    };
    node.with_key_catalog(Arc::new(ManagedKeys::new(
        Some(WrappingKey::new(&[byte; 32])),
        Vec::new(),
    )))
}

/// [`stepped`], with [`COPY_WEST_KEY`] mounted on node 3.
fn locked_copy(settings: &mut flower_cluster::host::HostSettings) {
    stepped(settings);
    settings.configure = Some(Arc::new(|_, node| wrapping(node, &COPY_WEST_KEY)));
}

/// [`stepped`], with [`IMPORT_WEST_KEY`] mounted on node 3.
fn locked_import(settings: &mut flower_cluster::host::HostSettings) {
    stepped(settings);
    settings.configure = Some(Arc::new(|_, node| wrapping(node, &IMPORT_WEST_KEY)));
}

/// Every member mounts wrapping key 1.
static SAME_KEY: AtomicU8 = AtomicU8::new(1);

/// [`stepped`], with [`SAME_KEY`] mounted everywhere.
fn keyed(settings: &mut flower_cluster::host::HostSettings) {
    stepped(settings);
    settings.configure = Some(Arc::new(|_, node| wrapping(node, &SAME_KEY)));
}

/// Only the root holds databases no partition places: a key change in
/// another group for a database it does not hold is refused there, and
/// leaves no rows that would keep the database from moving in later.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_key_change_where_no_database_is_placed_is_refused() {
    let hosts = layout(keyed).await;
    let west = hosts.leader_of("west").await;
    match west.manage_keys(b"acme", provisioning()[0].clone()).await {
        Err(flower_cluster::node::NodeError::Failure { code, .. }) => {
            assert_eq!(code, flower_cluster::node::KEY_REQUEST)
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(west.placement(b"acme").unwrap(), None);
    drop(west);
    hosts
        .any()
        .move_partition("acme", "west", "move-p")
        .await
        .unwrap()
        .unwrap();
    while advance(&hosts).await.is_some() {}
    assert_eq!(hosts.owner("acme").await.group().name.as_ref(), "west");
    hosts.stop().await;
}

/// The alias `acme`'s module declares.
fn sessions() -> Declaration {
    Declaration {
        name: "sessions".to_string(),
        algorithm: flower_crypto::catalog::Algorithm::Ed25519,
        usages: ["sign", "verify", "publicKey"]
            .into_iter()
            .map(str::to_string)
            .collect(),
    }
}

/// Sign through a member's key runtime, as `acme`'s guests there do.
fn signed(node: &Node) -> Result<Vec<u8>, String> {
    match node
        .key_catalog()
        .execute(
            b"acme",
            &sessions(),
            "sign",
            None,
            &[b"m"],
            &serde_json::json!({}),
            0,
        )
        .map_err(|error| format!("{error:#}"))?
    {
        ManagedOutput::Bytes(signature) => Ok(signature),
        _ => Err("signing returned no bytes".to_string()),
    }
}

/// `acme`'s key provisioning, each change under its request ID.
fn provisioning() -> [serde_json::Value; 2] {
    [
        serde_json::json!({"operation":"generate","requestId":"generate","name":"signer","algorithm":"Ed25519"}),
        serde_json::json!({"operation":"bind","requestId":"bind","name":"sessions","key":"signer","usages":["sign","verify","publicKey"]}),
    ]
}

/// Provision `acme`'s key at its owner: what it signs, and the listing.
async fn provision(hosts: &Hosts) -> (Vec<u8>, serde_json::Value) {
    let owner = hosts.owner("acme").await;
    for request in provisioning() {
        owner.manage_keys(b"acme", request).await.unwrap();
    }
    let listed = owner
        .manage_keys(b"acme", serde_json::json!({"operation":"list"}))
        .await
        .unwrap();
    (signed(&owner).unwrap(), listed)
}

/// What `west`'s leader answers when asked whether it can serve
/// `acme`'s keys as it holds them.
async fn west_keys(hosts: &Hosts) -> Result<(), flower_sdk::Failure> {
    hosts
        .leader_of("west")
        .await
        .check_keys(b"acme")
        .await
        .unwrap()
        .map(drop)
}

/// After the move: `acme`'s keys answer at `west` exactly as they did at
/// `east` (identity, versions, signatures), their provisioning receipts
/// came along, and `east` serves them no more.
async fn keys_moved(hosts: &Hosts, signature: &[u8], listed: &serde_json::Value) {
    let owner = hosts.owner("acme").await;
    assert_eq!(owner.group().name.as_ref(), "west");
    let after = owner
        .manage_keys(b"acme", serde_json::json!({"operation":"list"}))
        .await
        .unwrap();
    assert_eq!(after["value"], listed["value"], "key identity survives");
    assert_eq!(signed(&owner).unwrap(), signature);
    for request in provisioning() {
        let retried = owner.manage_keys(b"acme", request).await.unwrap();
        assert_eq!(retried["duplicate"], true, "{retried}");
    }
    let east = hosts.leader_of("east").await;
    assert_eq!(east.key_catalog().catalog(b"acme").revision, 0);
    let refused = east
        .manage_keys(
            b"acme",
            serde_json::json!({"operation":"rotate","requestId":"late","name":"signer"}),
        )
        .await
        .unwrap_err();
    assert!(refused.to_string().contains(PARTITION_MOVED), "{refused}");
}

/// Managed-key preflight: a database's keys travel with its rows, and a move
/// never freezes its source for a destination that could not unwrap them.
/// With the wrong wrapping key there, the move waits in its copy while the
/// source serves; restarted with the right one, the destination takes the
/// partition with its keys, their identities, the signatures they make and
/// their provisioning receipts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_locked_destination_holds_the_move_while_its_source_serves() {
    let mut hosts = layout(locked_copy).await;
    hosts
        .deploy_with("acme", [8; 32], &counters::manifest_stamped())
        .await;
    let (signature, listed) = provision(&hosts).await;
    assert_eq!(incr_routed(&hosts).await, 1.0);
    // A value derived from the key, materialized at the source.
    let stamp = stamped(&hosts).await;
    assert_eq!(key_readers(&*hosts.owner("acme").await).await, 1);
    hosts
        .any()
        .move_partition("acme", "west", "move-k")
        .await
        .unwrap()
        .unwrap();
    for _ in 0..5 {
        hosts.drive().await;
    }
    assert_eq!(phase(&hosts).await, Some(MovePhase::Copying));
    let locked = west_keys(&hosts).await.unwrap_err();
    assert_eq!(locked.code, KEYS_LOCKED);
    assert!(locked.message.contains("wrapping key"), "{locked:?}");
    // The source never froze: it serves and owns the partition.
    assert_eq!(incr_routed(&hosts).await, 2.0);
    let catalog = hosts.any().catalog().await.unwrap();
    assert_eq!(catalog.partitions["acme"].owner, "east");
    assert_eq!(
        hosts.leader_of("east").await.placement(b"acme").unwrap(),
        Some(Placement::Active { epoch: 1 })
    );

    // Restarted with the cluster's wrapping key, the destination takes
    // the move through to the end.
    hosts.crash(3).await;
    COPY_WEST_KEY.store(1, Ordering::SeqCst);
    hosts.restart(3).await;
    while advance(&hosts).await.is_some() {}
    west_keys(&hosts).await.unwrap();
    // The materialized value moved with its key: served at the
    // destination without evaluating again.
    let evaluations = counters::STAMPS.load(Ordering::SeqCst);
    assert_eq!(key_readers(&*hosts.owner("acme").await).await, 1);
    assert_eq!(stamped(&hosts).await, stamp);
    assert_eq!(counters::STAMPS.load(Ordering::SeqCst), evaluations);
    keys_moved(&hosts, &signature, &listed).await;
    assert_eq!(incr_routed(&hosts).await, 3.0);
    hosts.stop().await;
}

/// The wrapping keys node 3 (`west`) mounts in
/// [`a_move_held_for_keys_gives_itself_up_past_its_limit`],
/// [`a_former_leader_lets_go_of_its_pinned_base`] and
/// [`an_activation_checks_the_keys_where_it_is_proposed`].
static BOUNDED_WEST_KEY: AtomicU8 = AtomicU8::new(2);
static PINNED_WEST_KEY: AtomicU8 = AtomicU8::new(2);
static ACTIVATE_WEST_KEY: AtomicU8 = AtomicU8::new(1);

/// [`freeze_limited`], with [`BOUNDED_WEST_KEY`] mounted on node 3.
fn locked_bounded(settings: &mut flower_cluster::host::HostSettings) {
    freeze_limited(settings);
    settings.configure = Some(Arc::new(|_, node| wrapping(node, &BOUNDED_WEST_KEY)));
}

/// [`stepped`], with [`PINNED_WEST_KEY`] mounted on node 3 and every
/// group's maintenance running.
fn locked_maintained(settings: &mut flower_cluster::host::HostSettings) {
    stepped(settings);
    settings.duties.maintenance = Some(Duration::from_millis(50));
    settings.configure = Some(Arc::new(|_, node| wrapping(node, &PINNED_WEST_KEY)));
}

/// [`stepped`], with [`ACTIVATE_WEST_KEY`] mounted on node 3.
fn locked_activation(settings: &mut flower_cluster::host::HostSettings) {
    stepped(settings);
    settings.configure = Some(Arc::new(|_, node| wrapping(node, &ACTIVATE_WEST_KEY)));
}

/// Step the move until it holds, copying, for `west`'s keys.
async fn held_copying(hosts: &Hosts, op: &str) {
    provision(hosts).await;
    assert_eq!(incr_routed(hosts).await, 1.0);
    hosts
        .any()
        .move_partition("acme", "west", op)
        .await
        .unwrap()
        .unwrap();
    for _ in 0..3 {
        hosts.drive().await;
    }
    assert_eq!(phase(hosts).await, Some(MovePhase::Copying));
    assert_eq!(west_keys(hosts).await.unwrap_err().code, KEYS_LOCKED);
}

/// A move held before its freeze for a destination that cannot serve the
/// partition's keys gives itself up past the freeze limit: its source serves
/// on at the next epoch, and lets go of the base read it pinned for the move.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_move_held_for_keys_gives_itself_up_past_its_limit() {
    let hosts = layout(locked_bounded).await;
    held_copying(&hosts, "move-b").await;
    assert_eq!(hosts.leader_of("east").await.pinned_exports(), 1);
    tokio::time::sleep(Duration::from_millis(2_100)).await;
    assert_eq!(advance(&hosts).await, Some(MovePhase::Aborting));
    assert_eq!(advance(&hosts).await, Some(MovePhase::Abandoning));
    assert_eq!(advance(&hosts).await, None);
    let catalog = hosts.any().catalog().await.unwrap();
    let last = catalog.moves.last().unwrap();
    assert_eq!(last.phase, MovePhase::Aborted);
    let reason = last.reason.as_deref().unwrap();
    assert!(
        reason.starts_with("the destination could not serve the partition's keys past 2000 ms"),
        "{reason}"
    );
    assert_eq!(catalog.partitions["acme"].epoch, 3);
    assert_eq!(hosts.leader_of("east").await.pinned_exports(), 0);
    assert_eq!(incr_routed(&hosts).await, 2.0);
    hosts.stop().await;
}

/// A base read a leader pinned for a move goes once it stops leading,
/// with no export to notice: maintenance lets go of it, so a move held
/// before its freeze never keeps a former leader's group file from
/// reusing pages.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_former_leader_lets_go_of_its_pinned_base() {
    let hosts = layout(locked_maintained).await;
    held_copying(&hosts, "move-q").await;
    let east = hosts.leader_of("east").await;
    let id = east.status().id;
    assert_eq!(east.pinned_exports(), 1);
    east.transfer_leadership(if id == 1 { 2 } else { 1 })
        .await
        .unwrap();
    for _ in 0..200 {
        if east.pinned_exports() == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_ne!(east.status().leader, Some(id));
    assert_eq!(east.pinned_exports(), 0);
    drop(east);
    hosts.stop().await;
}

/// The destination's activation checks the keys where it is proposed: a
/// destination leader that took over after the move's last check, without the
/// wrapping key, refuses it, and the move, past its cutover, waits activating
/// (the partition served nowhere) until a leader with the key proposes it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_activation_checks_the_keys_where_it_is_proposed() {
    let mut hosts = layout(locked_activation).await;
    let (signature, listed) = provision(&hosts).await;
    assert_eq!(incr_routed(&hosts).await, 1.0);
    hosts
        .any()
        .move_partition("acme", "west", "move-a")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(advance(&hosts).await, Some(MovePhase::Freezing));
    assert_eq!(advance(&hosts).await, Some(MovePhase::Importing));
    assert_eq!(advance(&hosts).await, Some(MovePhase::Activating));

    // West's leader comes back without the key.
    hosts.crash(3).await;
    ACTIVATE_WEST_KEY.store(2, Ordering::SeqCst);
    hosts.restart(3).await;
    for _ in 0..5 {
        hosts.drive().await;
    }
    assert_eq!(phase(&hosts).await, Some(MovePhase::Activating));
    let epoch = hosts.any().catalog().await.unwrap().partitions["acme"]
        .movement
        .as_ref()
        .unwrap()
        .epoch;
    let west = hosts.leader_of("west").await;
    assert!(
        matches!(
            west.placement(b"acme").unwrap(),
            Some(Placement::Staged { .. })
        ),
        "{:?}",
        west.placement(b"acme")
    );
    let refused = west
        .place(
            b"acme",
            PlacementCommand::Activate {
                epoch,
                op: "move-a".to_string(),
            },
        )
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(refused.code, KEYS_LOCKED, "{refused:?}");
    drop(west);

    hosts.crash(3).await;
    ACTIVATE_WEST_KEY.store(1, Ordering::SeqCst);
    hosts.restart(3).await;
    while advance(&hosts).await.is_some() {}
    keys_moved(&hosts, &signature, &listed).await;
    assert_eq!(incr_routed(&hosts).await, 2.0);
    hosts.stop().await;
}

/// `acme`'s `stamped`, read fresh at its owner.
async fn stamped(hosts: &Hosts) -> flower_sdk::Value {
    hosts
        .owner("acme")
        .await
        .query(
            b"acme",
            "app",
            "stamped",
            &key("k"),
            &CallOptions::default(),
        )
        .await
        .unwrap()
}

/// `acme`'s committed derived outcomes that resolved a managed key, at
/// `node`.
async fn key_readers(node: &Node) -> usize {
    node.engine()
        .lock()
        .await
        .store()
        .begin_read()
        .unwrap()
        .dependents(b"acme", &flower_engine::certificate::keys_dep())
        .unwrap()
        .len()
}

/// Keys the source gained after the base copy are checked again before
/// the cutover: the move waits importing, its source frozen (and
/// refusing key changes), until the destination can serve them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keys_changed_after_the_copy_hold_the_cutover() {
    let mut hosts = layout(locked_import).await;
    assert_eq!(incr_routed(&hosts).await, 1.0);
    hosts
        .any()
        .move_partition("acme", "west", "move-i")
        .await
        .unwrap()
        .unwrap();
    // No keys yet: the base copies and the destination passes.
    assert_eq!(advance(&hosts).await, Some(MovePhase::Freezing));
    let (signature, listed) = provision(&hosts).await;
    assert_eq!(advance(&hosts).await, Some(MovePhase::Importing));
    for _ in 0..5 {
        hosts.drive().await;
    }
    assert_eq!(phase(&hosts).await, Some(MovePhase::Importing));
    let locked = west_keys(&hosts).await.unwrap_err();
    assert_eq!(locked.code, KEYS_LOCKED, "{locked:?}");
    let catalog = hosts.any().catalog().await.unwrap();
    assert_eq!(catalog.partitions["acme"].owner, "east");
    // The frozen source takes no key change the import would miss.
    let east = hosts.leader_of("east").await;
    let refused = east
        .manage_keys(
            b"acme",
            serde_json::json!({"operation":"rotate","requestId":"frozen","name":"signer"}),
        )
        .await
        .unwrap_err();
    assert!(refused.to_string().contains(PARTITION_MOVING), "{refused}");
    drop(east);

    hosts.crash(3).await;
    IMPORT_WEST_KEY.store(1, Ordering::SeqCst);
    hosts.restart(3).await;
    while advance(&hosts).await.is_some() {}
    keys_moved(&hosts, &signature, &listed).await;
    assert_eq!(incr_routed(&hosts).await, 2.0);
    hosts.stop().await;
}
