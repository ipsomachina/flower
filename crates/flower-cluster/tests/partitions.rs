//! Named partitions: groups founded from the catalog, partitions placed
//! in them under ownership epochs, and the placement fence every write
//! and read passes in its group.

mod counters;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use counters::{Counters, config, key, manifest, value};
use flower_cluster::host::{Host, HostSettings, Transport};
use flower_cluster::network::Registry;
use flower_cluster::node::{Node, NodeError};
use flower_engine::catalog::{CatalogChange, GroupStatus, PartitionStatus};
use flower_engine::engine::{CallOptions, MutationOptions};
use flower_engine::placement::{PARTITION_MOVED, PARTITION_RETIRED, PlacementCommand};
use flower_sdk::Value;

/// Three processes, each a root member, with the catalog followed.
struct Cluster {
    _dir: tempfile::TempDir,
    hosts: Vec<Arc<Host>>,
}

impl Cluster {
    async fn start() -> Cluster {
        let dir = tempfile::tempdir().unwrap();
        let registry = Registry::new();
        let mut hosts = Vec::new();
        for id in 1..=3 {
            let host = Host::open(
                id,
                &dir.path().join(format!("n{id}")),
                Arc::new(Counters),
                Transport::InProcess(registry.clone()),
                HostSettings::new(config()),
            )
            .await
            .unwrap();
            hosts.push(Arc::new(host));
        }
        hosts[0]
            .root()
            .initialize(BTreeSet::from([1, 2, 3]))
            .await
            .unwrap();
        for host in &hosts {
            host.start();
        }
        let cluster = Cluster { _dir: dir, hosts };
        cluster.leader_of("root").await;
        cluster
    }

    /// The node leading `group`, from whichever host hosts it.
    async fn leader_of(&self, group: &str) -> Arc<Node> {
        for _ in 0..400 {
            for host in &self.hosts {
                if let Some(node) = host.group(group)
                    && node.status().leader == Some(host.id())
                {
                    return node;
                }
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("group {group} elected no leader");
    }

    /// Wait until the catalog satisfies `done`.
    async fn until(&self, what: &str, done: impl Fn(&flower_engine::catalog::Catalog) -> bool) {
        for _ in 0..400 {
            if let Ok(catalog) = self.hosts[0].catalog().await
                && done(&catalog)
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("the catalog never showed {what}");
    }

    async fn stop(self) {
        for host in &self.hosts {
            host.stop_following().await;
        }
        for host in self.hosts {
            let host = Arc::try_unwrap(host).unwrap_or_else(|_| panic!("a host is still shared"));
            host.shutdown().await.unwrap();
        }
    }
}

async fn incr(node: &Node, db: &[u8]) -> Result<f64, NodeError> {
    let response = node
        .mutate(db, "app", "incr", &key("k"), &MutationOptions::default())
        .await?;
    match value(&response.outcome) {
        Value::Number(count) => Ok(count),
        other => panic!("incr answered {other:?}"),
    }
}

fn refusal_code(response: &flower_cluster::types::WriteResponse) -> String {
    match &response.outcome {
        flower_cluster::types::ResponseOutcome::Failure(bytes) => {
            let value = flower_engine::json::parse_value(bytes).unwrap();
            match value {
                Value::Object(map) => match map.get("code") {
                    Some(Value::String(code)) => code.clone(),
                    other => panic!("no code in {other:?}"),
                },
                other => panic!("not a failure: {other:?}"),
            }
        }
        flower_cluster::types::ResponseOutcome::Error(message) => {
            message.split(':').next().unwrap().to_string()
        }
        other => panic!("not refused: {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resize_moves_surplus_and_preferred_leaders_converge() {
    let cluster = Cluster::start().await;
    let host = cluster.hosts[0].clone();
    for name in ["east", "west"] {
        host.create_group(name, vec![1, 2, 3], &format!("create-{name}"))
            .await
            .unwrap()
            .unwrap();
        cluster
            .until("group active", |catalog| {
                catalog
                    .groups
                    .get(name)
                    .is_some_and(|g| g.status == GroupStatus::Active)
            })
            .await;
    }
    for name in ["a", "b", "c", "d"] {
        host.change_catalog(CatalogChange::CreatePartition {
            partition: name.into(),
            group: "east".into(),
            op: format!("create-{name}"),
        })
        .await
        .unwrap()
        .unwrap();
    }
    cluster
        .until("partitions active", |c| {
            c.partitions.len() == 4
                && c.partitions
                    .values()
                    .all(|p| p.status == PartitionStatus::Active)
        })
        .await;
    let resize = CatalogChange::Resize {
        groups: vec!["east".into(), "west".into()],
        op: "balance".into(),
    };
    let first = host.change_catalog(resize.clone()).await.unwrap().unwrap();
    cluster
        .until("resize finished", |c| {
            c.partitions
                .values()
                .all(|p| p.status == PartitionStatus::Active)
                && c.partitions.values().filter(|p| p.owner == "west").count() == 2
        })
        .await;
    assert_eq!(host.change_catalog(resize).await.unwrap().unwrap(), first);
    host.change_catalog(CatalogChange::PreferLeader {
        group: "west".into(),
        node: Some(3),
        op: "west-3".into(),
    })
    .await
    .unwrap()
    .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        if cluster.leader_of("west").await.status().id == 3 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "preferred leader did not converge"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    host.change_catalog(CatalogChange::Resize {
        groups: vec!["east".into()],
        op: "shrink".into(),
    })
    .await
    .unwrap()
    .unwrap();
    cluster
        .until("shrink finished", |c| {
            c.partitions
                .values()
                .all(|p| p.status == PartitionStatus::Active && p.owner == "east")
        })
        .await;
    drop(host);
    cluster.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn groups_found_themselves_from_the_catalog_and_partitions_are_placed_there() {
    let cluster = Cluster::start().await;
    let host = cluster.hosts[0].clone();
    // `east` lives on nodes 2 and 3; node 2 founds it.
    host.create_group("east", vec![3, 2], "create-east")
        .await
        .unwrap()
        .unwrap();
    cluster
        .until("east active", |catalog| {
            catalog.groups["east"].status == GroupStatus::Active
        })
        .await;
    let catalog = host.catalog().await.unwrap();
    assert_eq!(catalog.groups["east"].founder, Some(2));
    assert!(cluster.hosts[0].group("east").is_none());
    assert!(cluster.hosts[1].group("east").is_some());
    assert!(cluster.hosts[2].group("east").is_some());
    let east = cluster.leader_of("east").await;
    assert_ne!(east.group().uid, 0);

    // A partition placed in `east` at epoch 1.
    host.change_catalog(CatalogChange::CreatePartition {
        partition: "acme".into(),
        group: "east".into(),
        op: "create-acme".into(),
    })
    .await
    .unwrap()
    .unwrap();
    cluster
        .until("acme active", |catalog| {
            catalog.partitions["acme"].status == PartitionStatus::Active
        })
        .await;
    let route = host.fresh_route("acme").await.unwrap().unwrap();
    assert_eq!(&*route.group.name, "east");
    assert_eq!(route.epoch, Some(1));
    assert_eq!(&*host.route("plain").unwrap().group.name, "root");
    east.deploy(b"acme", "app", [1; 32], b"wasm", &manifest())
        .await
        .unwrap();
    assert_eq!(incr(&east, b"acme").await.unwrap(), 1.0);

    // The root refuses the partition it placed elsewhere, at prepare and
    // at apply, however a stale router reaches it.
    let root = cluster.leader_of("root").await;
    let refused = incr(&root, b"acme").await.unwrap_err();
    assert!(refused.to_string().contains(PARTITION_MOVED), "{refused}");
    let read = root
        .query(b"acme", "app", "read", &key("k"), &CallOptions::default())
        .await
        .unwrap_err();
    assert!(read.to_string().contains(PARTITION_MOVED), "{read}");

    // Retiring in place: the owner drops it, and it serves nowhere.
    host.change_catalog(CatalogChange::RetirePartition {
        partition: "acme".into(),
        op: "retire-acme".into(),
    })
    .await
    .unwrap()
    .unwrap();
    cluster
        .until("acme retired", |catalog| {
            catalog.partitions["acme"].status == PartitionStatus::Retired
        })
        .await;
    let refused = incr(&east, b"acme").await.unwrap_err();
    assert!(refused.to_string().contains(PARTITION_RETIRED), "{refused}");

    // A group owning nothing can leave; its members close and set its
    // files aside.
    host.change_catalog(CatalogChange::RemoveGroup {
        group: "east".into(),
        op: "remove-east".into(),
    })
    .await
    .unwrap()
    .unwrap();
    for _ in 0..400 {
        if cluster.hosts[1].group("east").is_none() && cluster.hosts[2].group("east").is_none() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(cluster.hosts[1].group("east").is_none());
    drop((east, root, host));
    cluster.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_existing_root_database_is_adopted_in_place() {
    let cluster = Cluster::start().await;
    let host = cluster.hosts[0].clone();
    let root = cluster.leader_of("root").await;
    root.deploy(b"legacy", "app", [2; 32], b"wasm", &manifest())
        .await
        .unwrap();
    assert_eq!(incr(&root, b"legacy").await.unwrap(), 1.0);
    host.change_catalog(CatalogChange::CreatePartition {
        partition: "legacy".into(),
        group: "root".into(),
        op: "adopt".into(),
    })
    .await
    .unwrap()
    .unwrap();
    cluster
        .until("legacy placed", |catalog| {
            catalog.partitions["legacy"].status == PartitionStatus::Active
        })
        .await;
    // Its rows and code stayed; it is now a partition at epoch 1.
    assert_eq!(incr(&root, b"legacy").await.unwrap(), 2.0);
    let placement = root.placement(b"legacy").unwrap().unwrap();
    assert_eq!(placement.epoch(), 1);
    drop((root, host));
    cluster.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_prepared_before_a_retirement_is_refused_at_apply() {
    let cluster = Cluster::start().await;
    let root = cluster.leader_of("root").await;
    root.deploy(b"db", "app", [3; 32], b"wasm", &manifest())
        .await
        .unwrap();
    root.place(
        b"db",
        PlacementCommand::Create {
            epoch: 1,
            op: "c".into(),
        },
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(incr(&root, b"db").await.unwrap(), 1.0);
    // A patch prepared now, while the database serves...
    let prepared = root
        .with_engine(|engine| {
            match engine
                .prepare_mutation(b"db", "app", "incr", &key("k"), &MutationOptions::default())
                .map_err(|failure| NodeError::Failure {
                    code: failure.code,
                    message: failure.message,
                })? {
                flower_engine::engine::PrepareOutcome::Patch(prepared) => Ok(prepared),
                other => panic!("prepared nothing: {other:?}"),
            }
        })
        .await
        .unwrap();
    // ...then the database retires in its log...
    root.place(
        b"db",
        PlacementCommand::Discard {
            epoch: 1,
            op: "d".into(),
        },
    )
    .await
    .unwrap()
    .unwrap();
    // ...and the patch, ordered after it, is refused on every member
    // and writes nothing.
    let response = root
        .raft()
        .client_write(flower_cluster::types::ClusterWrite::Patch {
            db: b"db".to_vec(),
            patch: prepared.patch,
            proposed_ms: 0,
        })
        .await
        .unwrap()
        .data;
    assert_eq!(refusal_code(&response), PARTITION_RETIRED);
    let revision = root
        .with_reader(|reader| {
            reader
                .store()
                .begin_read()
                .and_then(|read| read.meta_u64(b"db", flower_engine::storage::meta::REVISION))
                .map_err(|error| NodeError::Fatal(error.to_string()))
        })
        .await
        .unwrap();
    assert_eq!(revision, 0);
    drop(root);
    cluster.stop().await;
}
