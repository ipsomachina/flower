//! Many Raft groups per process: each group a complete member with its
//! own leader, log, files and writer admission, on one host and, over
//! TCP, one peer listener.

mod counters;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use counters::{Counters, config, key, manifest, value};
use flower_cluster::admission::{AdmissionConfig, Class};
use flower_cluster::group::GroupId;
use flower_cluster::host::{GroupEvent, Host, HostSettings, Transport};
use flower_cluster::network::{PeerConfig, Registry};
use flower_cluster::node::Node;
use flower_engine::engine::{CallOptions, MutationOptions};
use flower_sdk::Value;
use openraft::BasicNode;

fn east() -> GroupId {
    GroupId::new("east", 0xe457).unwrap()
}

async fn host(id: u64, dir: &std::path::Path, transport: Transport) -> Host {
    Host::open(
        id,
        &dir.join(format!("n{id}")),
        Arc::new(Counters),
        transport,
        HostSettings::new(config()),
    )
    .await
    .unwrap()
}

/// Wait until `node`'s group has a leader, and return it.
async fn leader_of(nodes: &[Arc<Node>]) -> u64 {
    for _ in 0..200 {
        for node in nodes {
            if let Some(leader) = node.status().leader {
                return leader;
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("no leader elected");
}

async fn incr(node: &Node, db: &[u8], name: &str) -> f64 {
    let response = node
        .mutate(db, "app", "incr", &key(name), &MutationOptions::default())
        .await
        .unwrap();
    match value(&response.outcome) {
        Value::Number(count) => count,
        other => panic!("incr answered {other:?}"),
    }
}

async fn read(node: &Node, db: &[u8], name: &str) -> Option<f64> {
    match node
        .query(db, "app", "read", &key(name), &CallOptions::default())
        .await
    {
        Ok(Value::Number(count)) => Some(count),
        Ok(other) => panic!("read answered {other:?}"),
        Err(_) => None,
    }
}

/// Wait for `node` to apply what its group committed.
async fn caught_up(node: &Node, index: u64) {
    for _ in 0..200 {
        if node
            .status()
            .last_applied
            .is_some_and(|applied| applied >= index)
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("node {} never applied {index}", node.status().id);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn groups_in_one_process_lead_write_and_apply_independently() {
    let dir = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let mut hosts = Vec::new();
    for id in 1..=3 {
        hosts.push(host(id, dir.path(), Transport::InProcess(registry.clone())).await);
    }
    let mut easts = Vec::new();
    for host in &hosts {
        easts.push(host.open_group(east()).await.unwrap());
    }
    let roots: Vec<_> = hosts.iter().map(Host::root).collect();
    let members = BTreeSet::from([1, 2, 3]);
    roots[0].initialize(members.clone()).await.unwrap();
    easts[2].initialize(members).await.unwrap();
    let (root_leader, east_leader) = (leader_of(&roots).await, leader_of(&easts).await);
    let root = roots[root_leader as usize - 1].clone();
    let east_node = easts[east_leader as usize - 1].clone();
    // Each group is its own Raft group: its own leader and term, its
    // own log and its own databases.
    assert_eq!(*east_node.group(), east());
    root.deploy(b"west-db", "app", [1; 32], b"wasm", &manifest())
        .await
        .unwrap();
    east_node
        .deploy(b"east-db", "app", [2; 32], b"wasm", &manifest())
        .await
        .unwrap();
    for _ in 0..3 {
        incr(&root, b"west-db", "a").await;
    }
    assert_eq!(incr(&east_node, b"east-db", "a").await, 1.0);
    let root_index = root.status().last_applied.unwrap();
    let east_index = east_node.status().last_applied.unwrap();
    for (root, east) in roots.iter().zip(&easts) {
        caught_up(root, root_index).await;
        caught_up(east, east_index).await;
        // Followers apply each group's entries into that group's files
        // only: the other group's database does not exist there.
        let root_dbs = root
            .with_reader(|reader| Ok(reader.databases()))
            .await
            .unwrap();
        let east_dbs = east
            .with_reader(|reader| Ok(reader.databases()))
            .await
            .unwrap();
        assert!(root_dbs.iter().any(|db| db.as_slice() == b"west-db"));
        assert!(!root_dbs.iter().any(|db| db.as_slice() == b"east-db"));
        assert!(east_dbs.iter().any(|db| db.as_slice() == b"east-db"));
        assert!(!east_dbs.iter().any(|db| db.as_slice() == b"west-db"));
    }
    assert_eq!(read(&root, b"west-db", "a").await, Some(3.0));
    assert_eq!(read(&east_node, b"east-db", "a").await, Some(1.0));
    // Each group's files live where the host put them: the root's in the
    // data directory, as a lone member's, the others' under `groups/`.
    let data = dir.path().join("n1");
    assert!(data.join("raft.redb").exists());
    assert!(data.join("groups/east/raft.redb").exists());
    assert!(data.join("groups/east/group.id").exists());
    drop((roots, easts, root, east_node));
    for host in hosts {
        host.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn groups_reopen_from_their_directories_and_refuse_another_uid() {
    let dir = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let first = host(1, dir.path(), Transport::InProcess(registry.clone())).await;
    let mut events = first.subscribe();
    let group = first.open_group(east()).await.unwrap();
    assert_eq!(events.recv().await.unwrap(), GroupEvent::Opened(east()));
    group.initialize(BTreeSet::from([1])).await.unwrap();
    leader_of(std::slice::from_ref(&group)).await;
    group
        .deploy(b"db", "app", [3; 32], b"wasm", &manifest())
        .await
        .unwrap();
    assert_eq!(incr(&group, b"db", "k").await, 1.0);
    drop(group);
    first.shutdown().await.unwrap();

    let again = host(1, dir.path(), Transport::InProcess(registry.clone())).await;
    let stored = again.stored_groups().unwrap();
    assert_eq!(stored, vec![GroupId::root(), east()]);
    // The same name under another uid is another group: its replica
    // never opens over these files.
    let impostor = GroupId::new("east", 0xbad).unwrap();
    let Err(refused) = again.open_group(impostor).await else {
        panic!("an impostor uid opened");
    };
    assert!(refused.to_string().contains("holds group"), "{refused}");
    let group = again.open_group(east()).await.unwrap();
    leader_of(std::slice::from_ref(&group)).await;
    assert_eq!(incr(&group, b"db", "k").await, 2.0);
    // Closing for good moves the files aside; the root never closes alone.
    drop(group);
    assert!(again.close_group("root", true).await.is_err());
    again.close_group("east", true).await.unwrap();
    assert!(again.group("east").is_none());
    let data = dir.path().join("n1/groups");
    assert!(!data.join("east").exists());
    assert!(
        data.join(".retired/east-000000000000e457/raft.redb")
            .exists()
    );
    again.shutdown().await.unwrap();
}

/// Every group's duties hear the stop together, so a host with many
/// groups stops as fast as its slowest duty (a leader's bounded last
/// backup archive) rather than their sum.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stopping_host_stops_every_groups_duties_at_once() {
    const LINGER: Duration = Duration::from_millis(700);
    let dir = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let mut settings = HostSettings::new(config());
    // A duty that takes a while to finish once told to stop.
    settings.extra_duties = Some(Arc::new(|_node, mut stopped| {
        vec![tokio::spawn(async move {
            while !*stopped.borrow() {
                if stopped.changed().await.is_err() {
                    break;
                }
            }
            tokio::time::sleep(LINGER).await;
        })]
    }));
    let host = Host::open(
        1,
        &dir.path().join("n1"),
        Arc::new(Counters),
        Transport::InProcess(registry),
        settings,
    )
    .await
    .unwrap();
    host.open_group(east()).await.unwrap();
    host.open_group(GroupId::new("west", 0x3e57).unwrap())
        .await
        .unwrap();
    let started = std::time::Instant::now();
    host.shutdown().await.unwrap();
    let took = started.elapsed();
    // Three groups one after another would take three lingers.
    assert!(took < LINGER * 2, "the host took {took:?} to stop");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_listener_serves_every_group_and_refuses_groups_it_does_not_host() {
    let dir = tempfile::tempdir().unwrap();
    let mut hosts = Vec::new();
    let mut addrs = BTreeMap::new();
    for id in 1..=3 {
        let host = host(id, dir.path(), Transport::Tcp(PeerConfig::default())).await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        addrs.insert(id, listener.local_addr().unwrap().to_string());
        host.serve_peers(listener).unwrap();
        hosts.push(host);
    }
    let members: BTreeMap<u64, BasicNode> = addrs
        .iter()
        .map(|(id, addr)| (*id, BasicNode::new(addr)))
        .collect();
    // `east` lives on nodes 2 and 3 only.
    let easts = vec![
        hosts[1].open_group(east()).await.unwrap(),
        hosts[2].open_group(east()).await.unwrap(),
    ];
    let roots: Vec<_> = hosts.iter().map(Host::root).collect();
    roots[0].initialize_nodes(members.clone()).await.unwrap();
    let east_members: BTreeMap<_, _> = members.into_iter().filter(|(id, _)| *id != 1).collect();
    easts[0].initialize_nodes(east_members).await.unwrap();
    let root_leader = leader_of(&roots).await;
    let east_leader = leader_of(&easts).await;
    assert!(east_leader == 2 || east_leader == 3);
    let east_node = easts[east_leader as usize - 2].clone();
    east_node
        .deploy(b"db", "app", [4; 32], b"wasm", &manifest())
        .await
        .unwrap();
    assert_eq!(incr(&east_node, b"db", "x").await, 1.0);
    let root = roots[root_leader as usize - 1].clone();
    root.deploy(b"db", "app", [5; 32], b"wasm", &manifest())
        .await
        .unwrap();
    assert_eq!(incr(&root, b"db", "x").await, 1.0);
    assert_eq!(incr(&root, b"db", "x").await, 2.0);
    // Node 1 hosts no `east`: a dial naming it is refused at the hello,
    // as is one naming `east` under another uid on a node that hosts it.
    let probe = hosts[1].peers(&east());
    let refused = probe
        .info(1, &addrs[&1], Duration::from_secs(2))
        .await
        .unwrap_err();
    assert!(refused.contains("hosts no group"), "{refused}");
    let impostor = hosts[0].peers(&GroupId::new("east", 0xbad).unwrap());
    let refused = impostor
        .info(2, &addrs[&2], Duration::from_secs(2))
        .await
        .unwrap_err();
    assert!(refused.contains("hosts no group"), "{refused}");
    let info = hosts[0]
        .peers(&east())
        .info(3, &addrs[&3], Duration::from_secs(2))
        .await
        .unwrap();
    assert!(info.initialized);
    drop((roots, easts, east_node, root));
    for host in hosts {
        host.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn each_group_admits_its_own_writes_while_reads_share_the_process() {
    let dir = tempfile::tempdir().unwrap();
    let mut settings = HostSettings::new(config());
    // Room for one write's bytes at a time per group.
    settings.writers = AdmissionConfig {
        queued_input_bytes: 4096,
        max_wait: Duration::from_millis(100),
        ..AdmissionConfig::default()
    };
    let host = Host::open(
        1,
        dir.path(),
        Arc::new(Counters),
        Transport::InProcess(Registry::new()),
        settings,
    )
    .await
    .unwrap();
    let root = host.root();
    let east = host.open_group(east()).await.unwrap();
    // The root's writer is full...
    let held = root
        .writer_admission()
        .acquire(Class::Writer, b"db", 4096)
        .await
        .unwrap();
    assert!(
        root.writer_admission()
            .acquire(Class::Writer, b"db", 1024)
            .await
            .is_err()
    );
    // ...and `east`'s writer is untouched by it.
    let other = east
        .writer_admission()
        .acquire(Class::Writer, b"db", 4096)
        .await
        .unwrap();
    drop((held, other));
    // Reads and control share one controller across the process: a
    // query admitted through one group counts in the other.
    let query = root
        .admission()
        .acquire(Class::Query, b"db", 1)
        .await
        .unwrap();
    assert_eq!(east.admission().metrics().classes[0].1.active, 1);
    drop(query);
    drop((root, east));
    host.shutdown().await.unwrap();
}
