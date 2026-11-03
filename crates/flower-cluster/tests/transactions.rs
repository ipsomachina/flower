//! Two-phase commit across groups: all or nothing everywhere, locks that
//! hold writes until the decision, and recovery from a coordinator or a
//! participant crashing mid-protocol.

mod counters;
mod hosts;

use std::time::{Duration, Instant};

use counters::{key, obj, s, value};
use flower_cluster::group::GroupId;
use flower_cluster::node::NodeError;
use flower_cluster::txn::{CallWire, PrepareRequest, StepWire};
use flower_engine::engine::{CallOptions, MultiStep, MutationOptions};
use flower_engine::txn::{
    CoordinatorRecord, Decision, Participant, PlacedDb, TXN_ABORTED, TXN_FINISHED, TXN_PREPARED,
    TxnCommand,
};
use flower_sdk::{Principal, Value};
use hosts::{Hosts, incr, read};

fn step(db: &str, method: &str) -> MultiStep {
    MultiStep {
        db: db.as_bytes().to_vec(),
        module: "app".into(),
        method: method.into(),
        args: key("k"),
    }
}

fn failure_code(response: &flower_cluster::types::WriteResponse) -> Option<String> {
    match &response.outcome {
        flower_cluster::types::ResponseOutcome::Failure(bytes) => {
            let value = flower_engine::json::parse_value(bytes).unwrap();
            flower_sdk::abi::decode_envelope(value)
                .err()
                .map(|f| f.code)
        }
        _ => None,
    }
}

/// Root on nodes 1-3 holds `a`; `east` on nodes 2-3 holds `b`.
async fn two_groups() -> Hosts {
    let hosts = Hosts::start(3).await;
    hosts.group("east", vec![2, 3]).await;
    hosts.deploy("a").await;
    hosts.partition("b", "east").await;
    hosts
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_transaction_across_groups_commits_everywhere_or_nowhere() {
    let hosts = two_groups().await;
    let host = hosts.host(1);
    let steps = [step("a", "incr"), step("b", "incr")];
    let response = host
        .distributed_transaction(&steps, &MutationOptions::default())
        .await
        .unwrap();
    assert_eq!(
        value(&response.outcome),
        Value::Array(vec![Value::Number(1.0), Value::Number(1.0)])
    );
    assert_eq!(read(&hosts.owner("a").await, "a").await, 1.0);
    assert_eq!(read(&hosts.owner("b").await, "b").await, 1.0);

    // A failing step aborts the whole transaction: nothing changes
    // anywhere, and no lock is left behind.
    let failing = [step("a", "incr"), step("b", "missing")];
    let response = host
        .distributed_transaction(&failing, &MutationOptions::default())
        .await
        .unwrap();
    assert_eq!(failure_code(&response).as_deref(), Some(TXN_ABORTED));
    assert_eq!(read(&hosts.owner("a").await, "a").await, 1.0);
    let started = Instant::now();
    assert_eq!(incr(&hosts.owner("a").await, "a").await.unwrap(), 2.0);
    assert_eq!(incr(&hosts.owner("b").await, "b").await.unwrap(), 2.0);
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "a lock outlived its abort"
    );

    // The coordinator keeps both records, complete.
    let (records, prepared) = hosts.owner("a").await.txn_records().unwrap();
    assert!(prepared.is_empty());
    assert_eq!(records.len(), 2);
    assert!(records.iter().all(|record| record.complete), "{records:#?}");
    // A retry with the same idempotency key replays the outcome.
    let keyed = MutationOptions {
        idempotency_key: Some(b"once".to_vec()),
        ..MutationOptions::default()
    };
    let first = host.distributed_transaction(&steps, &keyed).await.unwrap();
    // It answers where each participant committed: each database's
    // revision after the commit, and each group's own log position.
    let a_after = hosts.owner("a").await.status().last_applied;
    assert_eq!(first.revisions.len(), 2, "{first:?}");
    assert_eq!(first.revisions[0].0, b"a".to_vec());
    assert_eq!(first.revisions[1].0, b"b".to_vec());
    assert!(
        first.revisions.iter().all(|(_, revision)| *revision > 0),
        "{first:?}"
    );
    assert_eq!(first.revision, first.revisions[0].1);
    let positions: std::collections::BTreeMap<_, _> = first.positions.iter().cloned().collect();
    assert_eq!(positions.len(), 2, "{first:?}");
    assert!(
        positions.values().all(|position| *position > 0),
        "{first:?}"
    );
    assert_eq!(first.position, positions["root"]);
    assert!(first.position <= a_after.unwrap_or(0));
    // Later writes move both databases on; a replay still answers the
    // original revisions and positions on each participant.
    incr(&hosts.owner("a").await, "a").await.unwrap();
    incr(&hosts.owner("b").await, "b").await.unwrap();
    let again = host.distributed_transaction(&steps, &keyed).await.unwrap();
    assert_eq!(value(&first.outcome), value(&again.outcome));
    assert_eq!(
        (
            &again.revisions,
            &again.positions,
            again.revision,
            again.position
        ),
        (
            &first.revisions,
            &first.positions,
            first.revision,
            first.position
        )
    );
    assert_eq!(read(&hosts.owner("a").await, "a").await, 4.0);
    drop(host);
    hosts.stop().await;
}

/// Participants decide for themselves: the coordinator forwards the
/// caller's credentials untouched and every step's module runs its own
/// authorization hook on them, so no group ever vouches for a caller to
/// another (v1's delegation policies decide whether a participant trusts
/// its coordinator's word; here there is no word to trust). A caller one
/// participant's hook refuses aborts the whole transaction, whatever
/// principal it asserts, though the coordinator's own hook admits it;
/// one every hook admits commits everywhere.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_participant_authenticates_the_caller_itself() {
    let hosts = two_groups().await;
    let authorized = counters::manifest_authorized();
    for db in ["a", "b"] {
        hosts.deploy_with(db, [8; 32], &authorized).await;
    }
    // Alice may write both databases; Mallory only `a`, whose group
    // coordinates.
    for (db, grants) in [("a", &["alice", "mallory"][..]), ("b", &["alice"][..])] {
        let owner = hosts.owner(db).await;
        for who in grants {
            owner
                .mutate(
                    db.as_bytes(),
                    "app",
                    "incr",
                    &key(&format!("grant:{who}")),
                    &MutationOptions::default(),
                )
                .await
                .unwrap();
        }
    }
    let presenting = |token: &str, asserted: Option<&str>| MutationOptions {
        call: CallOptions {
            credentials: Some(obj(vec![("token", s(token))])),
            principal: asserted.map(|subject| Principal::new(subject.into(), None, None).unwrap()),
            ..CallOptions::default()
        },
        ..MutationOptions::default()
    };
    let host = hosts.host(1);
    let steps = [step("a", "incr"), step("b", "incr")];
    for asserted in [None, Some("alice")] {
        let response = host
            .distributed_transaction(&steps, &presenting("mallory", asserted))
            .await
            .unwrap();
        assert_eq!(
            failure_code(&response).as_deref(),
            Some(TXN_ABORTED),
            "{asserted:?}"
        );
    }
    assert_eq!(read(&hosts.owner("a").await, "a").await, 0.0);
    assert_eq!(read(&hosts.owner("b").await, "b").await, 0.0);
    let response = host
        .distributed_transaction(&steps, &presenting("alice", None))
        .await
        .unwrap();
    assert_eq!(
        value(&response.outcome),
        Value::Array(vec![Value::Number(1.0), Value::Number(1.0)])
    );
    drop(host);
    hosts.stop().await;
}

/// The prepare request a coordinator would send `group` for `db`.
fn prepare_request(id: &str, db: &str, index: usize) -> PrepareRequest {
    PrepareRequest {
        id: id.into(),
        coordinator: "root".into(),
        coordinator_uid: 0,
        dbs: vec![PlacedDb {
            db: db.into(),
            epoch: if db == "a" { 0 } else { 1 },
            steps: vec![index],
        }],
        steps: vec![StepWire {
            index,
            db: db.into(),
            module: "app".into(),
            method: "incr".into(),
            args: "{\"key\":\"k\"}".into(),
        }],
        options: CallWire::default(),
    }
}

fn participants(east_uid: u64) -> Vec<Participant> {
    vec![
        Participant {
            group: "root".into(),
            uid: 0,
            dbs: vec![PlacedDb {
                db: "a".into(),
                epoch: 0,
                steps: vec![0],
            }],
        },
        Participant {
            group: "east".into(),
            uid: east_uid,
            dbs: vec![PlacedDb {
                db: "b".into(),
                epoch: 1,
                steps: vec![1],
            }],
        },
    ]
}

fn begun(id: &str, east_uid: u64) -> TxnCommand {
    TxnCommand::Begin {
        record: Box::new(CoordinatorRecord {
            id: id.into(),
            key: None,
            participants: participants(east_uid),
            steps: 2,
            decision: None,
            results: Vec::new(),
            reason: None,
            code: None,
            finished: Vec::new(),
            commits: Vec::new(),
            complete: false,
            created_ms: 0,
            decided_ms: None,
            completed_ms: None,
            details: None,
        }),
    }
}

#[test]
fn a_prepare_carries_what_is_left_of_the_callers_time() {
    // The participant restarts the clock when the prepare arrives: what
    // it gets must already account for time the prepare spent getting
    // there (here a retry 300 ms later), never the whole budget again.
    let deadline = std::time::Instant::now() + Duration::from_millis(1_000);
    let options = CallOptions {
        deadline: Some(deadline),
        ..CallOptions::default()
    };
    let wire = CallWire::of(&options).unwrap();
    std::thread::sleep(Duration::from_millis(300));
    let arrived = wire.call_options().unwrap().deadline.unwrap();
    assert!(
        arrived <= deadline + Duration::from_millis(50),
        "{:?} past the caller's deadline",
        arrived.saturating_duration_since(deadline)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_prepare_never_outlives_the_callers_deadline() {
    // `east` (nodes 4 and 5) is down: its prepare cannot be delivered.
    // The caller gave the transaction half a second, so it ends then,
    // not after the delivery's own patience.
    let mut hosts = Hosts::start(5).await;
    hosts.group("east", vec![4, 5]).await;
    hosts.deploy("a").await;
    hosts.partition("b", "east").await;
    hosts.crash(4).await;
    hosts.crash(5).await;
    let host = hosts.host(1);
    let steps = [step("a", "incr"), step("b", "incr")];
    let deadline = Instant::now() + Duration::from_millis(500);
    let options = MutationOptions {
        call: CallOptions {
            deadline: Some(deadline),
            ..CallOptions::default()
        },
        ..MutationOptions::default()
    };
    let started = Instant::now();
    let outcome = flower_cluster::deadline::scoped(
        Some(tokio::time::Instant::from_std(deadline)),
        host.distributed_transaction(&steps, &options),
    )
    .await;
    assert!(
        started.elapsed() < Duration::from_secs(4),
        "took {:?}: {outcome:?}",
        started.elapsed()
    );
    if let Ok(response) = &outcome {
        assert!(failure_code(response).is_some(), "{response:?}");
    }
    drop(host);
    hosts.restart(4).await;
    hosts.restart(5).await;
    hosts.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prepared_read_footprints_allow_independent_writes_and_commit_after_them() {
    let hosts = two_groups().await;
    let east = hosts.owner("b").await;
    east.prepare_txn(prepare_request("c0", "b", 1))
        .await
        .unwrap()
        .unwrap();
    let (_, prepared) = east.txn_records().unwrap();
    assert!(prepared[0].reads.is_some());
    // A different key and its persisted retry receipt commute with the
    // prepared increment; replay must not increment it again.
    let options = MutationOptions {
        idempotency_key: Some(b"independent-retry".to_vec()),
        ..MutationOptions::default()
    };
    let result = east
        .mutate(b"b", "app", "incr", &key("independent"), &options)
        .await
        .unwrap();
    assert_eq!(value(&result.outcome), Value::Number(1.0));
    let replay = east
        .mutate(b"b", "app", "incr", &key("independent"), &options)
        .await
        .unwrap();
    assert_eq!(value(&replay.outcome), Value::Number(1.0));
    east.txn_command(TxnCommand::Finish {
        id: "c0".into(),
        decision: Decision::Commit,
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!(read(&east, "b").await, 1.0);
    let independent = east
        .query(
            b"b",
            "app",
            "read",
            &key("independent"),
            &CallOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(independent, Value::Number(1.0));
    drop(east);
    hosts.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_prepared_database_holds_writes_and_an_orphan_prepare_is_aborted() {
    let hosts = two_groups().await;
    let east = hosts.owner("b").await;
    // A prepare whose coordinator never began the transaction: what a
    // delayed duplicate leaves after its transaction was collected.
    east.prepare_txn(prepare_request("0f", "b", 1))
        .await
        .unwrap()
        .unwrap();
    // Writes to the locked database wait for the lock, then refuse.
    let started = Instant::now();
    let refused = incr(&east, "b").await.unwrap_err();
    assert!(refused.to_string().contains(TXN_PREPARED), "{refused}");
    assert!(started.elapsed() >= Duration::from_secs(1));
    // Reads still answer committed state.
    assert_eq!(read(&east, "b").await, 0.0);
    // Its coordinator has no record of it: the participant's resolver
    // aborts it, and the database takes writes again.
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let (_, prepared) = east.txn_records().unwrap();
        if prepared.is_empty() {
            break;
        }
        assert!(Instant::now() < deadline, "the orphan was never aborted");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(incr(&hosts.owner("b").await, "b").await.unwrap(), 1.0);
    // The finished transaction's late prepare is refused by its tombstone.
    let late = east
        .prepare_txn(prepare_request("0f", "b", 1))
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(late.code, TXN_FINISHED);
    hosts.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_coordinator_that_crashes_after_every_prepare_still_commits() {
    let mut hosts = two_groups().await;
    let east_uid = hosts.any().cached_catalog().groups["east"].uid;
    // Move root's leadership to node 1 (which hosts no `east`) so that
    // crashing it crashes the coordinator alone.
    let root = hosts.leader_of("root").await;
    if root.status().id != 1 {
        root.transfer_leadership(1).await.unwrap();
    }
    drop(root);
    let coordinator = hosts.leader_of("root").await;
    assert_eq!(coordinator.status().id, 1);
    // The coordinator began the transaction and both participants
    // prepared; then it crashed before deciding.
    coordinator
        .txn_command(begun("aa", east_uid))
        .await
        .unwrap()
        .unwrap();
    coordinator
        .prepare_txn(prepare_request("aa", "a", 0))
        .await
        .unwrap()
        .unwrap();
    hosts
        .owner("b")
        .await
        .prepare_txn(prepare_request("aa", "b", 1))
        .await
        .unwrap()
        .unwrap();
    drop(coordinator);
    hosts.crash(1).await;
    // The new root leader's resolver finds the undecided record, hears
    // that every participant prepared, and commits it: v1 would abort.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let leader = hosts.leader_of("root").await;
        let (records, _) = leader.txn_records().unwrap();
        if records.iter().any(|record| record.complete) {
            assert_eq!(records[0].decision, Some(Decision::Commit));
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the transaction was never resolved"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(read(&hosts.owner("a").await, "a").await, 1.0);
    assert_eq!(read(&hosts.owner("b").await, "b").await, 1.0);
    hosts.restart(1).await;
    hosts.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_participant_that_crashes_while_prepared_commits_after_it_returns() {
    let mut hosts = two_groups().await;
    let east_uid = hosts.any().cached_catalog().groups["east"].uid;
    let coordinator = hosts.leader_of("root").await;
    coordinator
        .txn_command(begun("bb", east_uid))
        .await
        .unwrap()
        .unwrap();
    coordinator
        .prepare_txn(prepare_request("bb", "a", 0))
        .await
        .unwrap()
        .unwrap();
    let east = hosts.owner("b").await;
    east.prepare_txn(prepare_request("bb", "b", 1))
        .await
        .unwrap()
        .unwrap();
    let east_id = east.status().id;
    drop(east);
    drop(coordinator);
    // `east` loses its leader's process: with one of its two members
    // left, it cannot apply anything until that one returns.
    hosts.crash(east_id).await;
    let coordinator = hosts.leader_of("root").await;
    coordinator
        .txn_command(TxnCommand::Decide {
            id: "bb".into(),
            decision: Decision::Commit,
            results: vec!["1".into(), "1".into()],
            reason: None,
            code: None,
            details: None,
        })
        .await
        .unwrap()
        .unwrap();
    drop(coordinator);
    // Decided, the reachable participant commits at once; `east` holds
    // its prepared record while it is down.
    tokio::time::sleep(Duration::from_secs(2)).await;
    // Back, it recovers its prepared record from its log, and the
    // decision reaches it: both commit.
    hosts.restart(east_id).await;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let leader = hosts.leader_of("root").await;
        let (records, _) = leader.txn_records().unwrap();
        if records.iter().any(|record| record.complete) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the decision never reached every participant"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(read(&hosts.owner("a").await, "a").await, 1.0);
    assert_eq!(read(&hosts.owner("b").await, "b").await, 1.0);
    let (_, prepared) = hosts.owner("b").await.txn_records().unwrap();
    assert!(prepared.is_empty());
    hosts.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_prepare_at_a_stale_epoch_is_refused() {
    let hosts = two_groups().await;
    let east = hosts.owner("b").await;
    let mut request = prepare_request("cc", "b", 1);
    request.dbs[0].epoch = 7;
    let refused = east.prepare_txn(request).await.unwrap().unwrap_err();
    assert_eq!(refused.code, flower_engine::txn::TXN_PLACEMENT);
    // And nothing was locked.
    assert_eq!(incr(&east, "b").await.unwrap(), 1.0);
    let _ = GroupId::root();
    let _: Option<NodeError> = None;
    hosts.stop().await;
}

/// A participant that cannot say whether it prepared, within the time a
/// request to its group may take, counts as not prepared: the resolver
/// aborts, and the reachable participants' databases take writes again
/// while it stays away, as v1's replacement coordinator aborts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unreachable_participant_aborts_an_undecided_transaction() {
    let hosts = Hosts::start(4).await;
    hosts.group("east", vec![4]).await;
    hosts.deploy("a").await;
    hosts.partition("b", "east").await;
    let east_uid = hosts.any().cached_catalog().groups["east"].uid;
    let root = hosts.leader_of("root").await;
    if root.status().id == 4 {
        root.transfer_leadership(1).await.unwrap();
    }
    drop(root);
    let coordinator = hosts.leader_of("root").await;
    assert_ne!(coordinator.status().id, 4);
    // The coordinator began the transaction and prepared its own part;
    // then every link to `east`'s only member goes.
    coordinator
        .txn_command(begun("dd", east_uid))
        .await
        .unwrap()
        .unwrap();
    coordinator
        .prepare_txn(prepare_request("dd", "a", 0))
        .await
        .unwrap()
        .unwrap();
    drop(coordinator);
    for id in 1..=3 {
        hosts.registry.block(id, 4);
        hosts.registry.block(4, id);
    }
    // The resolver cannot hear from `east` and aborts: `a` is free while
    // `east` is still away.
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let leader = hosts.leader_of("root").await;
        let (records, prepared) = leader.txn_records().unwrap();
        if records[0].decision.is_some() && prepared.is_empty() {
            assert_eq!(records[0].decision, Some(Decision::Abort));
            assert!(!records[0].complete);
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the transaction stayed undecided: {records:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert_eq!(
        incr(&hosts.leader_of("root").await, "a").await.unwrap(),
        1.0
    );
    // Back, `east` hears the abort, and the record completes.
    hosts.registry.heal();
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let (records, _) = hosts.leader_of("root").await.txn_records().unwrap();
        if records[0].complete {
            break;
        }
        assert!(Instant::now() < deadline, "the abort never completed");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert_eq!(incr(&hosts.owner("b").await, "b").await.unwrap(), 1.0);
    hosts.stop().await;
}

/// A caller that goes away mid-transaction (a client that hung up, its
/// request dropped) leaves the transaction to the resolver, which still
/// decides it on the host the caller left.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_transaction_its_caller_abandoned_is_still_resolved() {
    let hosts = Hosts::start(4).await;
    hosts.group("east", vec![4]).await;
    hosts.deploy("a").await;
    hosts.partition("b", "east").await;
    let root = hosts.leader_of("root").await;
    if root.status().id == 4 {
        root.transfer_leadership(1).await.unwrap();
    }
    drop(root);
    let leader = hosts.leader_of("root").await.status().id;
    for id in 1..=3 {
        hosts.registry.block(id, 4);
        hosts.registry.block(4, id);
    }
    // The caller gives up while `east` cannot answer its prepare.
    let steps = vec![step("a", "incr"), step("b", "incr")];
    let abandoned = tokio::time::timeout(
        Duration::from_secs(1),
        hosts
            .host(leader)
            .distributed_transaction(&steps, &MutationOptions::default()),
    )
    .await;
    assert!(abandoned.is_err(), "{abandoned:?}");
    let (records, _) = hosts.leader_of("root").await.txn_records().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].decision, None);
    // The resolver takes it up and aborts it: `east` never answers.
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let (records, prepared) = hosts.leader_of("root").await.txn_records().unwrap();
        if records[0].decision.is_some() && prepared.is_empty() {
            assert_eq!(records[0].decision, Some(Decision::Abort));
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the abandoned transaction was never resolved: {records:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert_eq!(
        incr(&hosts.leader_of("root").await, "a").await.unwrap(),
        1.0
    );
    hosts.registry.heal();
    hosts.stop().await;
}
