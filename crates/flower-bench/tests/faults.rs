//! Faults on real server processes, as v1's end-to-end scripts inject
//! them: `cargo-flower serve` members killed outright and started again
//! from their data directories, the whole cluster at once included, and
//! members frozen in place (`SIGSTOP`, `NtSuspendProcess` on Windows)
//! while the rest carry on, and members started with the wrong wrapping
//! key. Every acknowledged write must survive, on every member; a
//! request sent again under its ID after an unclean stop answers its
//! first outcome without running twice, whatever arguments come with
//! it. Each test first reaches the state it names (a cached aggregate
//! maintained by deltas, a move with part of its base at the
//! destination, a transaction prepared at its coordinator) and fails
//! if it never does.
//!
//! The members run the tasks guest (`flower-guest-test`), built for
//! `wasm32` into `target/wasm-guest` as the CLI's serve tests build it;
//! the server is built as the bench builds it, or taken from
//! `FLOWER_BENCH_SERVER_BINARY`. The tests take turns, so none's timing
//! suffers from another's cluster.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use flower_bench::artifacts;
use flower_bench::cluster::{Cluster, ClusterSpec, TlsFiles, random_hex};
use flower_client::admin::KeyOperation;
use flower_client::client::{FlowerClient, MutationOptions, MutationResult, RequestOptions};
use flower_client::error::FlowerError;
use flower_client::partitions::PartitionInfo;
use flower_client::retry::Retry;
use flower_client::transaction::{TransactionOptions, TransactionResult, TransactionStep};
use flower_client::transport::{Credentials, HttpTransport, Transport, TransportOptions};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// One cluster at a time.
static TURN: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

const DB: &str = "faults";
const MODULE: &str = "app";

/// The server, built once per test binary.
fn server() -> &'static PathBuf {
    static BUILT: OnceLock<PathBuf> = OnceLock::new();
    BUILT.get_or_init(|| match std::env::var_os("FLOWER_BENCH_SERVER_BINARY") {
        Some(path) => PathBuf::from(path),
        None => artifacts::build_server().expect("building cargo-flower"),
    })
}

/// The tasks guest and its manifest, built once per test binary.
fn guest() -> &'static (Vec<u8>, Value) {
    static BUILT: OnceLock<(Vec<u8>, Value)> = OnceLock::new();
    BUILT.get_or_init(|| {
        let root = artifacts::workspace_root().expect("the workspace");
        let target = root.join("target").join("wasm-guest");
        let status = std::process::Command::new(
            std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string()),
        )
        .current_dir(&root)
        .args([
            "build",
            "--quiet",
            "--release",
            "--target",
            "wasm32-unknown-unknown",
            "--package",
            "flower-guest-test",
        ])
        .env("CARGO_TARGET_DIR", &target)
        .status()
        .expect("running cargo");
        assert!(status.success(), "building the tasks guest failed");
        let wasm = std::fs::read(
            target
                .join("wasm32-unknown-unknown")
                .join("release")
                .join("flower_guest_test.wasm"),
        )
        .unwrap();
        // Served calls name HTTP aliases, never definitions.
        let mut manifest = artifacts::manifest(&wasm).unwrap();
        manifest["http"] = json!([
            {"alias": "put", "definition": "put"},
            {"alias": "get", "definition": "get"},
            {"alias": "titled", "definition": "titled"},
            {"alias": "send_email", "definition": "send_email"},
        ]);
        (wasm, manifest)
    })
}

/// Three members, the tasks guest deployed.
async fn start(label: &str) -> Cluster {
    start_with(label, None).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graceful_process_stop_hands_off_and_preserves_acknowledged_writes() {
    let _turn = TURN.lock().await;
    let cluster = start("graceful-stop").await;
    let expected = tasks(0..4, "before-stop");
    for (id, title) in &expected {
        put(&cluster, DB, &[0, 1, 2], id, title, Duration::from_secs(30))
            .await
            .unwrap();
    }
    let leader = agreed_leader(&cluster, &[0, 1, 2], None).await;
    let status = cluster
        .stop_gracefully(leader, Duration::from_secs(30))
        .await
        .unwrap_or_else(|error| panic!("{error}\n{}", cluster.log_tails(20)));
    assert!(status.success(), "server exited unsuccessfully: {status}");
    let remaining: Vec<_> = (0..3).filter(|index| *index != leader).collect();
    agreed_leader(&cluster, &remaining, Some(leader)).await;
    for &index in &remaining {
        holds(&cluster, index, DB, &expected).await;
    }
    cluster.restart(leader).unwrap();
    holds(&cluster, leader, DB, &expected).await;
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_data_group_keeps_serving_during_a_catalog_quorum_outage() {
    let _turn = TURN.lock().await;
    let cluster = start_groups("catalog-outage").await;
    place(&cluster, "independent", "west").await;
    put(
        &cluster,
        "independent",
        &[2],
        "before",
        "old",
        Duration::from_secs(30),
    )
    .await
    .unwrap();
    cluster.kill(0).await.unwrap();
    cluster.kill(1).await.unwrap();
    // Node 3 still owns a one-member data group but root lacks a quorum.
    assert!(
        client(&cluster, 2)
            .groups(&RequestOptions::new().timeout(Duration::from_secs(2)))
            .await
            .is_err()
    );
    let result = client(&cluster, 2)
        .mutate_value(
            "independent",
            MODULE,
            "put",
            &json!({"key":"after", "title":"new"}),
            &MutationOptions::default(),
        )
        .await;
    assert!(
        result.is_ok(),
        "data write during catalog outage: {result:?}"
    );
    assert_eq!(
        title_on(&cluster, 2, "independent", "before")
            .await
            .unwrap()
            .as_deref(),
        Some("old")
    );
    assert_eq!(
        title_on(&cluster, 2, "independent", "after")
            .await
            .unwrap()
            .as_deref(),
        Some("new")
    );
    cluster.restart(0).unwrap();
    cluster.restart(1).unwrap();
    agreed_leader(&cluster, &[0, 1, 2], None).await;
    cluster.shutdown().await;
}

/// [`start`], with TLS on every API and Raft port when `tls` is given.
async fn start_with(label: &str, tls: Option<TlsFiles>) -> Cluster {
    let binary = tokio::task::spawn_blocking(server).await.unwrap().clone();
    let cluster = Cluster::start(&ClusterSpec {
        binary,
        replicas: 3,
        data_parent: None,
        keep_data: false,
        label: format!("faults-{label}"),
        // The guest imports the crypto host (`sign_message`).
        crypto: true,
        otel: None,
        tls,
    })
    .await
    .unwrap();
    deploy(&cluster, DB).await;
    cluster
}

/// Deploy the tasks guest in database `db`, through whichever member
/// takes it.
async fn deploy(cluster: &Cluster, db: &str) {
    let (_, manifest) = tokio::task::spawn_blocking(guest).await.unwrap();
    deploy_manifest(cluster, db, manifest).await;
}

/// Deploy the tasks guest under `manifest` in database `db`.
async fn deploy_manifest(cluster: &Cluster, db: &str, manifest: &Value) {
    let (wasm, _) = tokio::task::spawn_blocking(guest).await.unwrap();
    let mut last = String::new();
    for attempt in 0..40 {
        match routed(cluster, attempt % 3)
            .deploy(
                db,
                MODULE,
                None,
                wasm,
                manifest,
                &RequestOptions::new().timeout(Duration::from_secs(120)),
            )
            .await
        {
            Ok(_) => return,
            Err(error) => last = error.to_string(),
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("deploying the tasks guest in {db} failed: {last}");
}

/// A client of member `index` presenting `token`, trusting the
/// cluster's root under TLS.
fn builder(cluster: &Cluster, index: usize, token: &str) -> flower_client::client::Builder {
    let mut builder = FlowerClient::builder(cluster.members()[index].url.clone())
        .credentials(Credentials::Token(token.to_string()))
        .timeout(Duration::from_secs(3));
    if let Some(ca) = cluster.ca_file() {
        builder = builder.ca_file(ca);
    }
    builder
}

/// An admin client of member `index` alone.
fn client(cluster: &Cluster, index: usize) -> FlowerClient {
    builder(cluster, index, cluster.admin_token())
        .build()
        .unwrap()
}

/// An admin client routing writes to the leader, starting at member
/// `index`.
fn routed(cluster: &Cluster, index: usize) -> FlowerClient {
    builder(cluster, index, cluster.admin_token())
        .leader_routing(true)
        .build()
        .unwrap()
}

/// Member `index`'s view of the leader, if it answers.
async fn leader_seen_by(cluster: &Cluster, index: usize) -> Option<u64> {
    client(cluster, index)
        .status(&RequestOptions::new().timeout(Duration::from_secs(1)))
        .await
        .ok()
        .and_then(|status| status.leader)
}

/// Wait until every member in `members` names the same leader, other
/// than `not`; returns its index.
async fn agreed_leader(cluster: &Cluster, members: &[usize], not: Option<usize>) -> usize {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let mut seen = Vec::new();
        for &index in members {
            seen.push(leader_seen_by(cluster, index).await);
        }
        if let Some(Some(leader)) = seen.first().copied()
            && seen.iter().all(|other| *other == Some(leader))
        {
            let index = leader as usize - 1;
            if Some(index) != not {
                return index;
            }
        }
        assert!(
            Instant::now() < deadline,
            "members {members:?} never agreed on a leader (saw {seen:?})\n{}",
            cluster.log_tails(30)
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Run mutation `method` in `db` under `request_id`, starting at each
/// member of `via` in turn and retrying through failovers under that
/// ID, until acknowledged or `within` runs out.
async fn write(
    cluster: &Cluster,
    db: &str,
    via: &[usize],
    method: &str,
    args: &Value,
    request_id: &str,
    within: Duration,
) -> Result<MutationResult<Value>, String> {
    let options = MutationOptions::new()
        .request_id(request_id)
        .retry(Retry::Default)
        .timeout(Duration::from_secs(3));
    let deadline = Instant::now() + within;
    let mut last = String::new();
    for &index in via.iter().cycle() {
        if Instant::now() > deadline {
            break;
        }
        match routed(cluster, index)
            .mutate_value(db, MODULE, method, args, &options)
            .await
        {
            Ok(result) => return Ok(result),
            Err(error) => last = error.to_string(),
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err(last)
}

/// Write task `key` in `db` (see [`write`]), under a request ID naming
/// the database, the task and its title.
async fn put(
    cluster: &Cluster,
    db: &str,
    via: &[usize],
    key: &str,
    title: &str,
    within: Duration,
) -> Result<MutationResult<Value>, String> {
    write(
        cluster,
        db,
        via,
        "put",
        &json!({"key": key, "title": title}),
        &format!("put-{db}-{key}-{title}"),
        within,
    )
    .await
}

/// Task `key`'s title in `db` as member `index` reads it: `get` is
/// linearizable, so the member answers only once it has applied what
/// the leader committed before the read (for a database another group
/// owns, the member forwards the read there).
async fn title_on(
    cluster: &Cluster,
    index: usize,
    db: &str,
    key: &str,
) -> Result<Option<String>, String> {
    let answer = client(cluster, index)
        .query_value(
            db,
            MODULE,
            "get",
            &json!({"key": key}),
            &RequestOptions::new().timeout(Duration::from_secs(3)),
        )
        .await
        .map_err(|error| error.to_string())?;
    Ok(answer.value["title"].as_str().map(str::to_string))
}

/// Wait until member `index` reads every task in `expected` in `db`
/// with its title.
async fn holds(cluster: &Cluster, index: usize, db: &str, expected: &BTreeMap<String, String>) {
    let deadline = Instant::now() + Duration::from_secs(30);
    for (key, title) in expected {
        loop {
            match title_on(cluster, index, db, key).await {
                Ok(Some(seen)) if &seen == title => break,
                outcome => assert!(
                    Instant::now() < deadline,
                    "member {} lost task {key} of {db} ({outcome:?})\n{}",
                    index + 1,
                    cluster.log_tails(30)
                ),
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

/// The `title_count` aggregate's value for `title` on member `index`:
/// how many tasks carry it, and how many rows left the group as deltas
/// since the member last built it from its rows (a build counts none).
async fn titled_on(cluster: &Cluster, index: usize, title: &str) -> Result<(u64, u64), String> {
    let answer = client(cluster, index)
        .query_value(
            DB,
            MODULE,
            "titled",
            &json!({"title": title}),
            &RequestOptions::new().timeout(Duration::from_secs(3)),
        )
        .await
        .map_err(|error| error.to_string())?;
    let field = |name: &str| answer.value[name].as_u64().unwrap_or(u64::MAX);
    Ok((field("count"), field("removed")))
}

/// Wait until every member in `members` counts `count` tasks titled
/// `title`, `removed` of them taken out as deltas: a member that built
/// the aggregate again from its rows counts none removed.
async fn counts(cluster: &Cluster, members: &[usize], title: &str, count: u64, removed: u64) {
    let deadline = Instant::now() + Duration::from_secs(30);
    for &index in members {
        loop {
            let seen = titled_on(cluster, index, title).await;
            if seen == Ok((count, removed)) {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "member {} counts {title}: {seen:?}, not ({count}, {removed})\n{}",
                index + 1,
                cluster.log_tails(30)
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

/// Every task in `keys` titled `title`.
fn tasks(keys: std::ops::Range<usize>, title: &str) -> BTreeMap<String, String> {
    keys.map(|key| (format!("t{key:03}"), title.to_string()))
        .collect()
}

/// Jobs waiting in the `email` queue, as the leader counts them.
async fn emails(cluster: &Cluster) -> usize {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match routed(cluster, 0)
            .queue_stats(
                DB,
                "email",
                &RequestOptions::new().timeout(Duration::from_secs(3)),
            )
            .await
        {
            Ok(stats) => return stats.pending,
            Err(error) => assert!(
                Instant::now() < deadline,
                "the email queue never answered: {error}"
            ),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

const ALL: [usize; 3] = [0, 1, 2];

#[tokio::test(flavor = "multi_thread")]
async fn acknowledged_writes_survive_killing_every_member_then_the_new_leader() {
    let _turn = TURN.lock().await;
    let cluster = start("crash-all").await;
    let mut before = tasks(0..40, "before");
    for (key, title) in &before {
        put(&cluster, DB, &ALL, key, title, Duration::from_secs(20))
            .await
            .unwrap();
    }
    // Read on every member, the aggregate is cached there; renames then
    // reach it as deltas (the leader folds them, its patches carry the
    // result), never as a build from the rows.
    counts(&cluster, &ALL, "before", 40, 0).await;
    let renamed = tasks(0..5, "renamed");
    for (key, title) in &renamed {
        put(&cluster, DB, &ALL, key, title, Duration::from_secs(20))
            .await
            .unwrap();
    }
    before.extend(renamed);
    counts(&cluster, &ALL, "before", 35, 5).await;
    for job in 0..5 {
        write(
            &cluster,
            DB,
            &ALL,
            "send_email",
            &json!({"to": format!("{job}@example.com")}),
            &format!("email-{job}"),
            Duration::from_secs(20),
        )
        .await
        .unwrap();
    }
    assert_eq!(emails(&cluster).await, 5);
    // No step down, no final flush: every process ends at once, and
    // every member opens its files after an unclean stop.
    for index in ALL {
        cluster.kill(index).await.unwrap();
    }
    for index in ALL {
        cluster.restart(index).unwrap();
    }
    let leader = agreed_leader(&cluster, &ALL, None).await;
    for index in ALL {
        holds(&cluster, index, DB, &before).await;
    }
    // Every member kept the maintained aggregate: none built it again.
    counts(&cluster, &ALL, "before", 35, 5).await;
    // Sent again under their IDs, writes answer their first outcome and
    // run no second time: the queue holds each job once.
    let again = put(
        &cluster,
        DB,
        &ALL,
        "t010",
        "before",
        Duration::from_secs(20),
    )
    .await
    .unwrap();
    assert!(again.duplicate, "{again:?}");
    for job in 0..5 {
        let again = write(
            &cluster,
            DB,
            &ALL,
            "send_email",
            &json!({"to": format!("{job}@example.com")}),
            &format!("email-{job}"),
            Duration::from_secs(20),
        )
        .await
        .unwrap();
        assert!(again.duplicate, "email-{job}: {again:?}");
    }
    assert_eq!(emails(&cluster).await, 5);
    // An unscoped ID's first outcome wins, as in v1: sent with other
    // arguments, it answers that outcome and runs none of them.
    let reused = routed(&cluster, leader)
        .mutate_value(
            DB,
            MODULE,
            "put",
            &json!({"key": "t010", "title": "other"}),
            &MutationOptions::new()
                .request_id("put-faults-t010-before")
                .timeout(Duration::from_secs(3)),
        )
        .await
        .unwrap();
    assert!(reused.duplicate, "{reused:?}");
    assert_eq!(
        title_on(&cluster, leader, DB, "t010").await.unwrap(),
        Some("before".to_string())
    );
    // Then the leader the restarted members chose dies: the other two
    // make the quorum from what they recovered, and take writes.
    cluster.kill(leader).await.unwrap();
    let survivors: Vec<usize> = ALL.into_iter().filter(|&index| index != leader).collect();
    agreed_leader(&cluster, &survivors, Some(leader)).await;
    let after = tasks(40..50, "after");
    for (key, title) in &after {
        put(
            &cluster,
            DB,
            &survivors,
            key,
            title,
            Duration::from_secs(20),
        )
        .await
        .unwrap();
    }
    cluster.restart(leader).unwrap();
    agreed_leader(&cluster, &ALL, None).await;
    for index in ALL {
        holds(&cluster, index, DB, &before).await;
        holds(&cluster, index, DB, &after).await;
    }
    counts(&cluster, &ALL, "before", 35, 5).await;
    counts(&cluster, &ALL, "after", 10, 0).await;
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_frozen_leader_is_replaced_and_never_answers_from_before() {
    let _turn = TURN.lock().await;
    let cluster = start("freeze-leader").await;
    put(
        &cluster,
        DB,
        &ALL,
        "t000",
        "before",
        Duration::from_secs(20),
    )
    .await
    .unwrap();
    // Cached on every member, the old leader included.
    counts(&cluster, &ALL, "before", 1, 0).await;
    let old = agreed_leader(&cluster, &ALL, None).await;
    let survivors: Vec<usize> = ALL.into_iter().filter(|&index| index != old).collect();
    // Frozen, not dead: its sockets stay open and never answer.
    assert!(cluster.freeze(old));
    let new = agreed_leader(&cluster, &survivors, Some(old)).await;
    let mut written = tasks(1..20, "while frozen");
    written.insert("t000".to_string(), "after".to_string());
    for (key, title) in &written {
        put(
            &cluster,
            DB,
            &survivors,
            key,
            title,
            Duration::from_secs(20),
        )
        .await
        .unwrap();
    }
    assert!(cluster.thaw(old));
    // The old leader wakes believing it leads: until it learns better,
    // a linearizable read there may fail, but never answers the title
    // the survivors replaced.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match title_on(&cluster, old, DB, "t000").await {
            Ok(Some(title)) => {
                assert_eq!(title, "after", "the thawed leader answered stale");
                break;
            }
            Ok(None) => panic!("the thawed leader lost t000"),
            Err(error) => assert!(
                Instant::now() < deadline,
                "the thawed leader never answered: {error}\n{}",
                cluster.log_tails(30)
            ),
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(agreed_leader(&cluster, &ALL, None).await, new);
    for index in ALL {
        holds(&cluster, index, DB, &written).await;
    }
    // The rename reached the old leader's cached count as the new
    // leader's delta, not as a build from its rows.
    counts(&cluster, &ALL, "before", 0, 1).await;
    counts(&cluster, &ALL, "while frozen", 19, 0).await;
    cluster.shutdown().await;
}

/// The process `west` lives on alone.
const WEST: usize = 2;
/// The processes `east` lives on, the root's majority.
const EAST: [usize; 2] = [0, 1];

fn admin_options() -> RequestOptions {
    RequestOptions::new().timeout(Duration::from_secs(5))
}

/// Three members and two more groups: `east` on members 1 and 2, `west`
/// on member 3 alone, so freezing process 3 freezes `west` whole while
/// the root and `east` keep their quorums.
async fn start_groups(label: &str) -> Cluster {
    let cluster = start(label).await;
    for (name, members) in [("east", vec![1, 2]), ("west", vec![3])] {
        let op = format!("create-{name}");
        let deadline = Instant::now() + Duration::from_secs(60);
        let mut attempt = 0;
        while let Err(error) = routed(&cluster, attempt % 3)
            .create_group(name, &members, Some(&op), &admin_options())
            .await
        {
            assert!(Instant::now() < deadline, "creating {name}: {error}");
            attempt += 1;
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        loop {
            let mut up = true;
            for &id in &members {
                let hosted = client(&cluster, id as usize - 1)
                    .groups(&admin_options())
                    .await
                    .is_ok_and(|listed| {
                        listed.groups.iter().any(|group| {
                            group.name == name
                                && group.status == "active"
                                && group.hosted
                                && group.leader.is_some()
                        })
                    });
                up &= hosted;
            }
            if up {
                break;
            }
            assert!(Instant::now() < deadline, "{name} never became active");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    cluster
}

/// The leader member `index` knows for `group`.
async fn group_leader(cluster: &Cluster, index: usize, group: &str) -> Option<usize> {
    let listed = client(cluster, index).groups(&admin_options()).await.ok()?;
    let entry = listed
        .groups
        .into_iter()
        .find(|entry| entry.name == group)?;
    Some(entry.leader? as usize - 1)
}

/// `partition` as the catalog lists it, asked of the members in `via`.
async fn listed(cluster: &Cluster, via: &[usize], partition: &str) -> Option<PartitionInfo> {
    for &index in via {
        if let Ok(listed) = routed(cluster, index).partitions(&admin_options()).await {
            return listed
                .partitions
                .into_iter()
                .find(|entry| entry.name == partition);
        }
    }
    None
}

/// Make a catalog change through the members in `via`, retrying the
/// same operation until one takes it.
async fn change<F, Fut>(cluster: &Cluster, via: &[usize], what: &str, call: F)
where
    F: Fn(FlowerClient) -> Fut,
    Fut: std::future::Future<Output = Result<Value, FlowerError>>,
{
    let deadline = Instant::now() + Duration::from_secs(60);
    for &index in via.iter().cycle() {
        match call(routed(cluster, index)).await {
            Ok(_) => return,
            Err(error) => assert!(
                Instant::now() < deadline,
                "{what}: {error}\n{}",
                cluster.log_tails(30)
            ),
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Wait until `partition` is active in `owner` with no move in flight;
/// returns its epoch.
async fn settled(cluster: &Cluster, via: &[usize], partition: &str, owner: &str) -> u64 {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let entry = listed(cluster, via, partition).await;
        if let Some(entry) = &entry
            && entry.owner == owner
            && entry.status == "active"
            && entry.movement.is_none()
        {
            return entry.epoch;
        }
        assert!(
            Instant::now() < deadline,
            "{partition} never settled in {owner}: {entry:?}\n{}",
            cluster.log_tails(30)
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Place `partition` in `group` and deploy the tasks guest there.
async fn place(cluster: &Cluster, partition: &str, group: &str) {
    let op = format!("create-{partition}");
    change(cluster, &ALL, &op, |client| {
        let op = op.clone();
        async move {
            client
                .create_partition(partition, group, Some(&op), &admin_options())
                .await
        }
    })
    .await;
    settled(cluster, &ALL, partition, group).await;
    deploy(cluster, partition).await;
}

/// Begin moving `partition` to `group` under operation `op`.
async fn begin_move(cluster: &Cluster, via: &[usize], partition: &str, group: &str, op: &str) {
    change(cluster, via, op, |client| async move {
        client
            .move_partition(partition, group, Some(op), &admin_options())
            .await
    })
    .await;
}

/// Fill `db` with `rows` tasks whose titles are 32 KiB of random hex,
/// which lz4 cannot shrink, so a move's base (rows and their `by_title`
/// entries) spans several 1 MiB pages.
async fn fill(cluster: &Cluster, db: &str, rows: usize) {
    for row in 0..rows {
        let title = random_hex(16 * 1024);
        write(
            cluster,
            db,
            &ALL,
            "put",
            &json!({"key": format!("fill{row:03}"), "title": title}),
            &format!("fill-{db}-{row}"),
            Duration::from_secs(20),
        )
        .await
        .unwrap();
    }
}

/// Member `index`'s readiness answer: per group, among others, the
/// index it applied.
async fn readiness(cluster: &Cluster, index: usize) -> Option<Value> {
    let mut options = TransportOptions::new().connect_timeout(Duration::from_millis(500));
    if let Some(ca) = cluster.ca_file() {
        options = options.ca_file(ca);
    }
    let transport =
        HttpTransport::with_options(cluster.members()[index].url.clone(), options).ok()?;
    let reply = tokio::time::timeout(Duration::from_secs(2), transport.get("/health/ready", None))
        .await
        .ok()?
        .ok()?;
    serde_json::from_slice(&reply.body).ok()
}

/// The index `west`'s only member applied: every page of a move's base
/// it imports is one entry of its log, and nothing else writes there.
async fn west_applied(cluster: &Cluster) -> u64 {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(applied) = readiness(cluster, WEST)
            .await
            .and_then(|ready| ready["groups"]["west"]["applied"].as_u64())
        {
            return applied;
        }
        assert!(Instant::now() < deadline, "west never said what it applied");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Begin moving `acme` (filled past one page) to `west` and freeze
/// `west` once part of the base reached it: the destination holds an
/// unsealed import, and the move waits in its copy. Fails unless it
/// gets there, as v1's scripts wait for `base_bytes > 0`.
async fn freeze_mid_copy(cluster: &Cluster, op: &str) {
    let applied = west_applied(cluster).await;
    begin_move(cluster, &EAST, "acme", "west", op).await;
    // The move's `begin` at the destination, then its first page.
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let now = west_applied(cluster).await;
        if now >= applied + 2 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "no page of the base reached west ({applied} to {now})\n{}",
            cluster.log_tails(30)
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(cluster.freeze(WEST));
    // Had the copy finished, the move would go on to freeze its source
    // (east alone does that) within moments: it stays in its copy.
    let until = Instant::now() + Duration::from_secs(2);
    while Instant::now() < until {
        let entry = listed(cluster, &EAST, "acme").await.unwrap();
        let phase = entry
            .movement
            .as_ref()
            .map(|movement| movement.phase.as_str());
        assert_eq!(phase, Some("copying"), "the copy finished: {entry:?}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_move_to_a_frozen_group_waits_until_its_operator_gives_up() {
    let _turn = TURN.lock().await;
    let cluster = start_groups("frozen-move").await;
    place(&cluster, "acme", "east").await;
    fill(&cluster, "acme", 160).await;
    let mut written = tasks(0..10, "before");
    for (key, title) in &written {
        put(&cluster, "acme", &ALL, key, title, Duration::from_secs(20))
            .await
            .unwrap();
    }
    let first = settled(&cluster, &ALL, "acme", "east").await;
    // Frozen with part of the base imported, the destination holds the
    // copy's connections and never answers: the source serves on.
    freeze_mid_copy(&cluster, "move-1").await;
    let during = tasks(10..20, "during the copy");
    for (key, title) in &during {
        put(&cluster, "acme", &EAST, key, title, Duration::from_secs(20))
            .await
            .unwrap();
    }
    written.extend(during);
    // The operator gives up.
    change(&cluster, &EAST, "abort move-1", |client| async move {
        client
            .abort_move(
                "acme",
                "move-1",
                Some("frozen destination"),
                &admin_options(),
            )
            .await
    })
    .await;
    // The source thaws first: it owns the partition again at a later
    // epoch while the move waits, abandoning, for the destination to
    // drop its import.
    let deadline = Instant::now() + Duration::from_secs(60);
    let aborted = loop {
        let entry = listed(&cluster, &EAST, "acme").await;
        if let Some(entry) = &entry
            && entry.owner == "east"
            && entry.epoch > first
            && entry
                .movement
                .as_ref()
                .is_some_and(|movement| movement.phase == "abandoning")
        {
            break entry.epoch;
        }
        assert!(
            Instant::now() < deadline,
            "the source never served again: {entry:?}\n{}",
            cluster.log_tails(30)
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    let after = tasks(20..30, "after the abort");
    for (key, title) in &after {
        put(&cluster, "acme", &EAST, key, title, Duration::from_secs(20))
            .await
            .unwrap();
    }
    written.extend(after);
    // Thawed, the destination drops the half import it holds, the abort
    // finishes at the source's epoch, and a second move to it completes
    // with every write.
    assert!(cluster.thaw(WEST));
    assert_eq!(settled(&cluster, &ALL, "acme", "east").await, aborted);
    begin_move(&cluster, &ALL, "acme", "west", "move-2").await;
    let moved = settled(&cluster, &ALL, "acme", "west").await;
    assert!(moved > aborted, "epoch {moved} after {aborted}");
    for index in ALL {
        holds(&cluster, index, "acme", &written).await;
    }
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn every_member_killed_mid_copy_resumes_the_move_from_disk() {
    let _turn = TURN.lock().await;
    let cluster = start_groups("crash-move").await;
    place(&cluster, "acme", "east").await;
    fill(&cluster, "acme", 160).await;
    let mut written = tasks(0..10, "before");
    for (key, title) in &written {
        put(&cluster, "acme", &ALL, key, title, Duration::from_secs(20))
            .await
            .unwrap();
    }
    freeze_mid_copy(&cluster, "move-1").await;
    let during = tasks(10..20, "during the copy");
    for (key, title) in &during {
        put(&cluster, "acme", &EAST, key, title, Duration::from_secs(20))
            .await
            .unwrap();
    }
    written.extend(during);
    // Every process ends at once mid-copy, the frozen destination with
    // its unsealed import included, and opens its files after an
    // unclean stop: the catalog's move resumes and copies the base again.
    for index in ALL {
        cluster.kill(index).await.unwrap();
    }
    for index in ALL {
        cluster.restart(index).unwrap();
    }
    agreed_leader(&cluster, &ALL, None).await;
    settled(&cluster, &ALL, "acme", "west").await;
    for index in ALL {
        holds(&cluster, index, "acme", &written).await;
    }
    let after = tasks(20..30, "after the restart");
    for (key, title) in &after {
        put(&cluster, "acme", &ALL, key, title, Duration::from_secs(20))
            .await
            .unwrap();
    }
    for index in ALL {
        holds(&cluster, index, "acme", &after).await;
    }
    cluster.shutdown().await;
}

/// Put `key` in both `acme` (first, so `east` coordinates) and `beta`.
fn both(key: &str, title: &str) -> Vec<TransactionStep> {
    ["acme", "beta"]
        .into_iter()
        .map(|db| TransactionStep {
            db: db.to_string(),
            module: MODULE.to_string(),
            method: "put".to_string(),
            args: json!({"key": key, "title": title}),
        })
        .collect()
}

/// Transaction options under request ID `id`.
fn under(id: &str, timeout: Duration) -> TransactionOptions {
    TransactionOptions {
        request_id: Some(id.to_string()),
        base: RequestOptions::new().timeout(timeout),
    }
}

/// Run a transaction through the members in `via` until it commits.
async fn transact(
    cluster: &Cluster,
    via: &[usize],
    id: &str,
    steps: &[TransactionStep],
) -> TransactionResult {
    let options = under(id, Duration::from_secs(5));
    let deadline = Instant::now() + Duration::from_secs(60);
    for &index in via.iter().cycle() {
        let error = match routed(cluster, index).transaction(steps, &options).await {
            Ok(result) => return result,
            Err(error) => error,
        };
        assert!(
            Instant::now() < deadline,
            "transaction {id}: {error}\n{}",
            cluster.log_tails(30)
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    unreachable!("the members cycle")
}

/// A failure's machine-readable code.
fn code(error: &FlowerError) -> Option<&str> {
    error.code()
}

/// Wait until no group holds a prepared transaction or coordinates an
/// unfinished one.
async fn resolved(cluster: &Cluster) {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let records = routed(cluster, 0).transactions(&admin_options()).await;
        let open = match &records {
            Ok(records) => records["groups"].as_object().is_none_or(|groups| {
                groups.values().any(|group| {
                    group.get("error").is_some()
                        || group["prepared"]
                            .as_array()
                            .is_none_or(|list| !list.is_empty())
                        || group["coordinated"]
                            .as_array()
                            .is_none_or(|list| list.iter().any(|record| record["complete"] != true))
                })
            }),
            Err(_) => true,
        };
        if !open {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "transactions stayed open: {records:?}\n{}",
            cluster.log_tails(30)
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Whether every member reads `key` in both databases, asserting that
/// none reads it in one alone.
async fn atomic(cluster: &Cluster, key: &str) -> bool {
    let mut seen = Vec::new();
    for index in ALL {
        for db in ["acme", "beta"] {
            let deadline = Instant::now() + Duration::from_secs(30);
            loop {
                match title_on(cluster, index, db, key).await {
                    Ok(title) => {
                        seen.push(title.is_some());
                        break;
                    }
                    Err(error) => assert!(
                        Instant::now() < deadline,
                        "member {} cannot read {db}: {error}",
                        index + 1
                    ),
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
    assert!(
        seen.iter().all(|present| *present == seen[0]),
        "{key} committed in part: {seen:?}"
    );
    seen[0]
}

/// Observe the participant itself: unrelated writes can succeed while
/// its transaction remains prepared.
async fn prepared(cluster: &Cluster, group: &str, expected: bool) {
    let deadline = Instant::now() + Duration::from_secs(60);
    let http = reqwest::Client::new();
    loop {
        // The stream's snapshot is local. The cluster-wide listing waits
        // on the deliberately frozen group and can outlive this prepare.
        let outcome: Result<Value, String> = async {
            let mut response = http
                .get(format!(
                    "{}/v1/admin/stream",
                    cluster.members()[EAST[0]].url
                ))
                .bearer_auth(cluster.admin_token())
                .timeout(Duration::from_secs(3))
                .send()
                .await
                .map_err(|error| error.to_string())?
                .error_for_status()
                .map_err(|error| error.to_string())?;
            let mut bytes = Vec::new();
            while let Some(chunk) = response.chunk().await.map_err(|error| error.to_string())? {
                bytes.extend_from_slice(&chunk);
                if let Some(end) = bytes.windows(2).position(|pair| pair == b"\n\n") {
                    let block =
                        std::str::from_utf8(&bytes[..end]).map_err(|error| error.to_string())?;
                    let data = block
                        .lines()
                        .find_map(|line| line.strip_prefix("data:"))
                        .ok_or("snapshot has no data")?;
                    return serde_json::from_str(data).map_err(|error| error.to_string());
                }
            }
            Err("stream ended before its snapshot".to_string())
        }
        .await;
        if let Ok(records) = &outcome
            && let Some(records) = records["transactions"]["groups"][group]["prepared"].as_array()
            && !records.is_empty() == expected
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{group} never reached prepared={expected}: {outcome:?}\n{}",
            cluster.log_tails(30)
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_transaction_across_groups_stays_whole_through_a_frozen_participant_and_a_killed_coordinator()
 {
    let _turn = TURN.lock().await;
    let cluster = start_groups("frozen-txn").await;
    place(&cluster, "acme", "east").await;
    place(&cluster, "beta", "west").await;
    transact(&cluster, &ALL, "txn-0", &both("k0", "both")).await;
    assert!(atomic(&cluster, "k0").await);
    // `west` freezes; `east` (owning the first step) coordinates from its
    // leader, which the call goes to, so it is in flight when that dies.
    assert!(cluster.freeze(WEST));
    let coordinator = group_leader(&cluster, 0, "east").await.unwrap();
    assert!(EAST.contains(&coordinator));
    let call = {
        let client = builder(&cluster, coordinator, cluster.admin_token())
            .build()
            .unwrap();
        tokio::spawn(async move {
            client
                .transaction(
                    &both("k1", "both"),
                    &under("txn-1", Duration::from_secs(60)),
                )
                .await
        })
    };
    // `east` prepares its own part and records the transaction; `west`
    // never answers. The coordinator dies before it can decide.
    prepared(&cluster, "east", true).await;
    let conflict = routed(&cluster, coordinator)
        .mutate_value(
            "acme",
            MODULE,
            "put",
            &json!({"key": "k1", "title": "conflict"}),
            &MutationOptions::new()
                .request_id("conflict")
                .timeout(Duration::from_secs(3)),
        )
        .await;
    assert!(
        matches!(&conflict, Err(error) if code(error) == Some(flower_engine::txn::TXN_PREPARED)),
        "{conflict:?}"
    );
    put(
        &cluster,
        "acme",
        &EAST,
        "independent",
        "while prepared",
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    cluster.kill(coordinator).await.unwrap();
    let interrupted = call.await.unwrap();
    assert!(interrupted.is_err(), "{interrupted:?}");
    cluster.restart(coordinator).unwrap();
    // Its successor cannot hear from `west` and aborts: `acme` takes
    // writes again, without `k1`, while `west` is still frozen.
    prepared(&cluster, "east", false).await;
    put(
        &cluster,
        "acme",
        &EAST,
        "free",
        "again",
        Duration::from_secs(60),
    )
    .await
    .unwrap();
    for index in EAST {
        assert_eq!(title_on(&cluster, index, "acme", "k1").await, Ok(None));
    }
    // Thawed, `west` hears the abort; whatever prepare of the dead
    // coordinator it reads late applies nothing.
    assert!(cluster.thaw(WEST));
    resolved(&cluster).await;
    assert!(!atomic(&cluster, "k1").await);
    // Durable: the same request ID answers the abort, and a committed
    // transaction's its commit, without running again.
    let replay = routed(&cluster, 0)
        .transaction(&both("k1", "both"), &under("txn-1", Duration::from_secs(5)))
        .await;
    assert!(
        matches!(&replay, Err(error) if code(error) == Some(flower_engine::txn::TXN_ABORTED)),
        "{replay:?}"
    );
    let replay = transact(&cluster, &ALL, "txn-0", &both("k0", "both")).await;
    assert!(replay.duplicate, "{replay:?}");
    // Nothing stays locked: the next transaction over both commits.
    transact(&cluster, &ALL, "txn-2", &both("k2", "both")).await;
    assert!(atomic(&cluster, "k2").await);
    // And every member keeps all of it through a full unclean restart.
    for index in ALL {
        cluster.kill(index).await.unwrap();
    }
    for index in ALL {
        cluster.restart(index).unwrap();
    }
    agreed_leader(&cluster, &ALL, None).await;
    resolved(&cluster).await;
    assert!(atomic(&cluster, "k0").await);
    assert!(!atomic(&cluster, "k1").await);
    assert!(atomic(&cluster, "k2").await);
    let replay = transact(&cluster, &ALL, "txn-0", &both("k0", "both")).await;
    assert!(replay.duplicate, "{replay:?}");
    cluster.shutdown().await;
}

/// The peer-protocol test certificate (`flower-cluster`'s fixtures):
/// `DNS:localhost` and `IP:127.0.0.1`, signed by its test root.
fn fixtures() -> TlsFiles {
    let dir = artifacts::workspace_root()
        .unwrap()
        .join("crates")
        .join("flower-cluster")
        .join("tests")
        .join("fixtures")
        .join("tls");
    TlsFiles {
        cert: dir.join("node.crt"),
        key: dir.join("node.key"),
        ca: dir.join("ca.crt"),
    }
}

/// Write task `key` through member `index` alone, which forwards it to
/// the leader over the Raft port.
async fn put_through(cluster: &Cluster, index: usize, key: &str, title: &str) {
    let options = MutationOptions::new()
        .request_id(format!("through-{key}-{title}"))
        .timeout(Duration::from_secs(5));
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        match client(cluster, index)
            .mutate_value(
                DB,
                MODULE,
                "put",
                &json!({"key": key, "title": title}),
                &options,
            )
            .await
        {
            Ok(_) => return,
            Err(error) => assert!(
                Instant::now() < deadline,
                "member {} never took {key}: {error}\n{}",
                index + 1,
                cluster.log_tails(30)
            ),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// A peer-protocol hello to the root group presenting `token`, naming
/// node 999 as the member it means to reach: a member checks the token
/// first, then answers that it is another node.
fn hello(token: &str) -> Vec<u8> {
    let mut hello = b"FLRP\x04".to_vec();
    hello.extend_from_slice(&(token.len() as u16).to_be_bytes());
    hello.extend_from_slice(token.as_bytes());
    hello.extend_from_slice(&999u64.to_be_bytes());
    hello.extend_from_slice(&999u64.to_be_bytes());
    hello.extend_from_slice(b"\x04root");
    hello.extend_from_slice(&0u64.to_be_bytes());
    hello.extend_from_slice(&1u16.to_be_bytes());
    hello.extend_from_slice(b"x");
    // The dialer's largest entry: 1 MiB, which every member's frames carry.
    hello.extend_from_slice(&(1024u64 * 1024).to_be_bytes());
    hello
}

/// The first byte a member's Raft port answers `hello` with, over TLS
/// trusting `ca` (or in plaintext without it); `None` when it closes
/// or stays silent.
async fn raft_answer(addr: SocketAddr, ca: Option<&Path>, hello: &[u8]) -> Option<u8> {
    use tokio_rustls::rustls::pki_types::pem::PemObject;
    use tokio_rustls::rustls::pki_types::{CertificateDer, ServerName};
    use tokio_rustls::rustls::{ClientConfig, RootCertStore, crypto};
    let socket = tokio::net::TcpStream::connect(addr).await.ok()?;
    let exchange = async {
        match ca {
            Some(ca) => {
                let mut roots = RootCertStore::empty();
                for root in CertificateDer::pem_file_iter(ca).unwrap() {
                    roots.add(root.unwrap()).unwrap();
                }
                let config = ClientConfig::builder_with_provider(Arc::new(
                    crypto::aws_lc_rs::default_provider(),
                ))
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_root_certificates(roots)
                .with_no_client_auth();
                let name = ServerName::from(addr.ip());
                let mut stream = tokio_rustls::TlsConnector::from(Arc::new(config))
                    .connect(name, socket)
                    .await
                    .ok()?;
                stream.write_all(hello).await.ok()?;
                stream.read_u8().await.ok()
            }
            None => {
                let mut socket = socket;
                socket.write_all(hello).await.ok()?;
                socket.read_u8().await.ok()
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(5), exchange)
        .await
        .ok()
        .flatten()
}

#[tokio::test(flavor = "multi_thread")]
async fn members_speak_tls_on_both_ports_through_forwarding_and_a_leader_kill() {
    let _turn = TURN.lock().await;
    let cluster = start_with("tls", Some(fixtures())).await;
    let leader = agreed_leader(&cluster, &ALL, None).await;
    let follower = ALL.into_iter().find(|&index| index != leader).unwrap();
    // A follower takes writes and forwards them over the Raft port; every
    // member's linearizable read confirms the leader over it too.
    let before = tasks(0..10, "before");
    for (key, title) in &before {
        put_through(&cluster, follower, key, title).await;
    }
    for index in ALL {
        holds(&cluster, index, DB, &before).await;
    }
    // The leader dies; the survivors elect over TLS, and the restarted
    // member rejoins over it.
    cluster.kill(leader).await.unwrap();
    let survivors: Vec<usize> = ALL.into_iter().filter(|&index| index != leader).collect();
    agreed_leader(&cluster, &survivors, Some(leader)).await;
    let after = tasks(10..20, "after");
    for (key, title) in &after {
        put(
            &cluster,
            DB,
            &survivors,
            key,
            title,
            Duration::from_secs(20),
        )
        .await
        .unwrap();
    }
    cluster.restart(leader).unwrap();
    agreed_leader(&cluster, &ALL, None).await;
    for index in ALL {
        holds(&cluster, index, DB, &before).await;
        holds(&cluster, index, DB, &after).await;
    }
    // The Raft port speaks the peer protocol inside TLS alone: with the
    // members' root and their peer token, a hello gets past the token
    // check (to "another node", as it names node 999); another token is
    // rejected; and a plaintext hello gets no answer of the protocol's.
    let raft = cluster.raft_addr(follower);
    let ca = cluster.ca_file().unwrap();
    assert_eq!(
        raft_answer(raft, Some(ca), &hello(cluster.peer_token())).await,
        Some(2)
    );
    assert_eq!(
        raft_answer(raft, Some(ca), &hello("not-the-peer-token")).await,
        Some(1)
    );
    let plain = raft_answer(raft, None, &hello(cluster.peer_token())).await;
    assert!(!matches!(plain, Some(0..=4)), "{plain:?}");
    // The API port: no plaintext, no trust in another root, and the peer
    // token, which the Raft port takes, is no operator credential.
    let url = &cluster.members()[follower].url;
    let plain = FlowerClient::builder(url.replacen("https://", "http://", 1))
        .credentials(Credentials::Token(cluster.admin_token().to_string()))
        .timeout(Duration::from_secs(3))
        .build()
        .unwrap();
    let options = RequestOptions::new().timeout(Duration::from_secs(3));
    assert!(plain.status(&options).await.is_err());
    let untrusting = FlowerClient::builder(url.clone())
        .credentials(Credentials::Token(cluster.admin_token().to_string()))
        .timeout(Duration::from_secs(3))
        .build()
        .unwrap();
    assert!(untrusting.status(&options).await.is_err());
    let (wasm, manifest) = tokio::task::spawn_blocking(guest).await.unwrap();
    let peer = builder(&cluster, follower, cluster.peer_token())
        .build()
        .unwrap()
        .deploy(DB, MODULE, None, wasm, manifest, &options)
        .await;
    assert!(matches!(&peer, Err(error) if error.is_auth()), "{peer:?}");
    cluster.shutdown().await;
}

/// The tasks guest declaring `sessions`, an Ed25519 key it signs and
/// verifies with, and serving `sign` and `verify`.
async fn keyed_manifest() -> Value {
    let (_, manifest) = tokio::task::spawn_blocking(guest).await.unwrap();
    let mut manifest = manifest.clone();
    manifest["keys"] = json!([
        {"name": "sessions", "algorithm": "Ed25519", "usages": ["sign", "verify"]}
    ]);
    let http = manifest["http"].as_array_mut().unwrap();
    http.push(json!({"alias": "sign", "definition": "sign_message"}));
    http.push(json!({"alias": "verify", "definition": "verify_message"}));
    manifest
}

/// Provision `db`'s signing key and bind `sessions` to it, under the
/// same request IDs in every database, as v1's tenants do: the key's
/// identity, and whether the provisioning answered as a duplicate.
async fn provision(cluster: &Cluster, db: &str) -> (String, bool) {
    let operations = [
        (
            "generate",
            KeyOperation::Generate {
                name: "signer".to_string(),
                algorithm: "Ed25519".to_string(),
                bits: None,
            },
        ),
        (
            "bind",
            KeyOperation::Bind {
                alias: "sessions".to_string(),
                key: "signer".to_string(),
                usages: vec!["sign".to_string(), "verify".to_string()],
            },
        ),
    ];
    let mut provisioned = (String::new(), true);
    for (request_id, operation) in &operations {
        let deadline = Instant::now() + Duration::from_secs(60);
        let mut attempt = 0;
        let change = loop {
            match routed(cluster, attempt % 3)
                .change_keys(db, operation, request_id, &admin_options())
                .await
            {
                Ok(change) => break change,
                Err(error) => assert!(
                    Instant::now() < deadline,
                    "{request_id} in {db}: {error}\n{}",
                    cluster.log_tails(30)
                ),
            }
            attempt += 1;
            tokio::time::sleep(Duration::from_millis(200)).await;
        };
        provisioned = (
            change.catalog.keys["signer"].id.clone(),
            provisioned.1 && change.duplicate,
        );
    }
    provisioned
}

/// `message` signed with `db`'s `sessions` key by its guest.
async fn signed(cluster: &Cluster, db: &str, message: &str) -> String {
    let options = MutationOptions::new()
        .request_id(format!("sign-{db}-{message}"))
        .retry(Retry::Default)
        .timeout(Duration::from_secs(3));
    let deadline = Instant::now() + Duration::from_secs(30);
    for index in ALL.iter().cycle() {
        match routed(cluster, *index)
            .mutate_value(
                db,
                MODULE,
                "sign",
                &json!({"key": "sessions", "message": message}),
                &options,
            )
            .await
        {
            Ok(result) => return result.value.as_str().unwrap().to_string(),
            Err(error) => assert!(
                Instant::now() < deadline,
                "signing in {db}: {error}\n{}",
                cluster.log_tails(30)
            ),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    unreachable!("the members cycle")
}

/// Whether `db`'s guest verifies `signature` of `message` under its
/// `sessions` key.
async fn verified(cluster: &Cluster, db: &str, message: &str, signature: &str) -> bool {
    let deadline = Instant::now() + Duration::from_secs(30);
    for index in ALL.iter().cycle() {
        match routed(cluster, *index)
            .query_value(
                db,
                MODULE,
                "verify",
                &json!({"key": "sessions", "message": message, "signature": signature}),
                &RequestOptions::new().timeout(Duration::from_secs(3)),
            )
            .await
        {
            Ok(answer) => return answer.value.as_bool().unwrap(),
            Err(error) => assert!(
                Instant::now() < deadline,
                "verifying in {db}: {error}\n{}",
                cluster.log_tails(30)
            ),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    unreachable!("the members cycle")
}

/// Place `partition` in `group` with the keyed guest and its key.
async fn place_keyed(cluster: &Cluster, partition: &str, group: &str) -> (String, bool) {
    let op = format!("create-{partition}");
    change(cluster, &ALL, &op, |client| {
        let op = op.clone();
        async move {
            client
                .create_partition(partition, group, Some(&op), &admin_options())
                .await
        }
    })
    .await;
    settled(cluster, &ALL, partition, group).await;
    deploy_manifest(cluster, partition, &keyed_manifest().await).await;
    provision(cluster, partition).await
}

/// v1's `e2e-managed-migration`: a partition's keys travel with it, and
/// a move holds before its cutover until the destination can unwrap
/// them. `west`'s only member runs with the wrong wrapping key: the move
/// waits in its copy, the source serving and owning the partition; once
/// that member restarts with the cluster's key the move completes, and
/// the key's identity, the signatures it makes and its provisioning
/// receipts are the same at the destination. Another tenant binding the
/// same alias under the same request IDs gets a key of its own, which
/// never verifies the first tenant's signatures.
#[tokio::test(flavor = "multi_thread")]
async fn a_move_waits_for_its_destination_to_mount_the_wrapping_key() {
    let _turn = TURN.lock().await;
    let cluster = start_groups("keyed-move").await;
    let wrong = cluster.data_dir().join("wrong-wrapping.bin");
    std::fs::write(&wrong, [0x5a; 32]).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&wrong, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    cluster.kill(WEST).await.unwrap();
    cluster.mount_wrapping(WEST, Some(wrong));
    cluster.restart(WEST).unwrap();
    let deadline = Instant::now() + Duration::from_secs(60);
    while group_leader(&cluster, WEST, "west").await.is_none() {
        assert!(Instant::now() < deadline, "west never came back");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let (key, _) = place_keyed(&cluster, "acme", "east").await;
    let signature = signed(&cluster, "acme", "release").await;
    assert!(verified(&cluster, "acme", "release", &signature).await);
    let mut written = tasks(0..10, "before");
    for (key, title) in &written {
        put(&cluster, "acme", &ALL, key, title, Duration::from_secs(20))
            .await
            .unwrap();
    }
    let first = settled(&cluster, &ALL, "acme", "east").await;

    // The destination cannot unwrap `acme`'s keys: the move waits in its
    // copy while the source serves on, and the driver says why.
    begin_move(&cluster, &ALL, "acme", "west", "move-k").await;
    let during = tasks(10..20, "during the hold");
    for (key, title) in &during {
        put(&cluster, "acme", &ALL, key, title, Duration::from_secs(20))
            .await
            .unwrap();
    }
    written.extend(during);
    tokio::time::sleep(Duration::from_secs(2)).await;
    let entry = listed(&cluster, &ALL, "acme").await.unwrap();
    assert_eq!(entry.owner, "east", "{entry:?}");
    assert_eq!(entry.epoch, first, "{entry:?}");
    let phase = entry
        .movement
        .as_ref()
        .map(|movement| movement.phase.as_str());
    assert_eq!(phase, Some("copying"), "{entry:?}");
    assert!(
        cluster
            .log_tails(400)
            .contains("the move waits for its destination's wrapping key"),
        "{}",
        cluster.log_tails(40)
    );

    // Restarted with the cluster's key, the destination takes `acme`.
    cluster.kill(WEST).await.unwrap();
    cluster.mount_wrapping(WEST, None);
    cluster.restart(WEST).unwrap();
    let moved = settled(&cluster, &ALL, "acme", "west").await;
    assert!(moved > first, "epoch {moved} after {first}");
    for index in ALL {
        holds(&cluster, index, "acme", &written).await;
    }
    assert!(verified(&cluster, "acme", "release", &signature).await);
    assert_eq!(signed(&cluster, "acme", "release-again").await.len(), 128);
    let (same, duplicate) = provision(&cluster, "acme").await;
    assert_eq!(same, key, "the key's identity moved");
    assert!(duplicate, "provisioning receipts moved with the partition");

    // Another tenant in `east`, the same alias and request IDs: a key of
    // its own, which never verifies `acme`'s signatures.
    let (other, duplicate) = place_keyed(&cluster, "beta", "east").await;
    assert_ne!(other, key);
    assert!(!duplicate, "another tenant's provisioning is its own");
    assert!(!verified(&cluster, "beta", "release", &signature).await);
    cluster.shutdown().await;
}
