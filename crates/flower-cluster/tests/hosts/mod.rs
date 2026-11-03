//! A cluster of in-process hosts for partition, move and transaction
//! tests: every host a root member, with groups created and partitions
//! placed through the catalog, and hosts that can crash and come back.

#![allow(dead_code)]

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use flower_cluster::host::{Host, HostSettings, Transport};
use flower_cluster::network::Registry;
use flower_cluster::node::{Node, NodeError};
use flower_engine::catalog::{Catalog, CatalogChange, GroupStatus, PartitionStatus};
use flower_engine::engine::{CallOptions, MutationOptions};
use flower_sdk::Value;

use crate::counters::{Counters, config, key, manifest, value};

/// In-process hosts, each a root member, following the catalog.
pub struct Hosts {
    pub dir: tempfile::TempDir,
    pub registry: Registry,
    /// By node ID - 1; `None` while crashed.
    pub hosts: Vec<Option<Arc<Host>>>,
    /// Adjusts every host's settings, restarts included.
    pub settings: fn(&mut HostSettings),
}

impl Hosts {
    /// `count` hosts with the root initialized on every one.
    pub async fn start(count: u64) -> Hosts {
        Self::start_with(count, |_| {}).await
    }

    /// [`Hosts::start`] with every host's settings adjusted.
    pub async fn start_with(count: u64, settings: fn(&mut HostSettings)) -> Hosts {
        let dir = tempfile::tempdir().unwrap();
        let registry = Registry::new();
        let mut hosts = Vec::new();
        for id in 1..=count {
            hosts.push(Some(
                open(id, &dir.path().join(format!("n{id}")), &registry, settings).await,
            ));
        }
        hosts[0]
            .as_ref()
            .unwrap()
            .root()
            .initialize((1..=count).collect::<BTreeSet<_>>())
            .await
            .unwrap();
        for host in hosts.iter().flatten() {
            host.start();
        }
        let hosts = Hosts {
            dir,
            registry,
            hosts,
            settings,
        };
        hosts.leader_of("root").await;
        hosts
    }

    /// The host of node `id`.
    pub fn host(&self, id: u64) -> Arc<Host> {
        self.hosts[id as usize - 1]
            .clone()
            .unwrap_or_else(|| panic!("node {id} is down"))
    }

    /// Any live host.
    pub fn any(&self) -> Arc<Host> {
        self.hosts.iter().flatten().next().unwrap().clone()
    }

    /// The node leading `group`, once one does.
    pub async fn leader_of(&self, group: &str) -> Arc<Node> {
        for _ in 0..600 {
            for host in self.hosts.iter().flatten() {
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
    pub async fn until(&self, what: &str, done: impl Fn(&Catalog) -> bool) {
        for _ in 0..600 {
            if let Ok(catalog) = self.any().catalog().await
                && done(&catalog)
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("the catalog never showed {what}");
    }

    /// Create group `name` on `members` and wait until it is active.
    pub async fn group(&self, name: &str, members: Vec<u64>) {
        self.any()
            .create_group(name, members, &format!("create-{name}"))
            .await
            .unwrap()
            .unwrap();
        self.until(&format!("{name} active"), |catalog| {
            catalog
                .groups
                .get(name)
                .is_some_and(|group| group.status == GroupStatus::Active)
        })
        .await;
        self.leader_of(name).await;
    }

    /// Place partition `name` in `group`, wait until it serves, and
    /// deploy the counter app to it.
    pub async fn partition(&self, name: &str, group: &str) {
        self.any()
            .change_catalog(CatalogChange::CreatePartition {
                partition: name.into(),
                group: group.into(),
                op: format!("create-{name}"),
            })
            .await
            .unwrap()
            .unwrap();
        for _ in 0..600 {
            // Hosts that do not drive their catalog are stepped here.
            self.drive().await;
            if let Ok(catalog) = self.any().catalog().await
                && catalog
                    .partitions
                    .get(name)
                    .is_some_and(|entry| entry.status == PartitionStatus::Active)
            {
                self.deploy(name).await;
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("the catalog never showed {name} active");
    }

    /// Take one driver step from the root leader's host: what its driver
    /// does on every tick (steps repeat harmlessly).
    pub async fn drive(&self) {
        let leader = self.leader_of("root").await;
        let host = self.host(leader.status().id);
        drop(leader);
        let _ = flower_cluster::moves::drive_once(&host).await;
    }

    /// Deploy the counter app to `db` in the group that owns it.
    pub async fn deploy(&self, db: &str) {
        self.deploy_with(db, [7; 32], &manifest()).await;
    }

    /// Deploy `manifest` of the counter app, as `identity`, to `db` in
    /// the group that owns it.
    pub async fn deploy_with(&self, db: &str, identity: [u8; 32], manifest: &Value) {
        let leader = self.owner(db).await;
        leader
            .deploy(db.as_bytes(), "app", identity, b"wasm", manifest)
            .await
            .unwrap();
    }

    /// The leader of the group that owns `db`, by a fresh catalog. A
    /// healed partition may still be electing the root's next leader.
    pub async fn owner(&self, db: &str) -> Arc<Node> {
        for _ in 0..600 {
            match self.any().fresh_route(db).await {
                Ok(route) => return self.leader_of(&route.unwrap().group.name).await,
                Err(NodeError::Forward { .. }) => {}
                Err(error) => panic!("could not route {db}: {error}"),
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("the root never became ready to route {db}");
    }

    /// Crash node `id`: its host shuts down at once, its files stay.
    pub async fn crash(&mut self, id: u64) {
        let host = self.hosts[id as usize - 1].take().unwrap();
        host.stop_following().await;
        stop(host).await;
    }

    /// Bring node `id` back over its files.
    pub async fn restart(&mut self, id: u64) {
        let host = open(
            id,
            &self.dir.path().join(format!("n{id}")),
            &self.registry,
            self.settings,
        )
        .await;
        for group in host.stored_groups().unwrap() {
            if !group.is_root() {
                host.open_group(group).await.unwrap();
            }
        }
        host.start();
        self.hosts[id as usize - 1] = Some(host);
    }

    /// Stop every host.
    pub async fn stop(self) {
        for host in self.hosts.iter().flatten() {
            host.stop_following().await;
        }
        for host in self.hosts.into_iter().flatten() {
            stop(host).await;
        }
    }
}

async fn open(
    id: u64,
    dir: &std::path::Path,
    registry: &Registry,
    adjust: fn(&mut HostSettings),
) -> Arc<Host> {
    let mut settings = HostSettings::new(config());
    adjust(&mut settings);
    Arc::new(
        Host::open(
            id,
            dir,
            Arc::new(Counters),
            Transport::InProcess(registry.clone()),
            settings,
        )
        .await
        .unwrap(),
    )
}

async fn stop(host: Arc<Host>) {
    let mut host = host;
    for _ in 0..400 {
        match Arc::try_unwrap(host) {
            Ok(owned) => {
                owned.shutdown().await.unwrap();
                return;
            }
            Err(shared) => {
                host = shared;
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }
    panic!("a host is still shared");
}

/// Increment counter `k` of `db` at `node`.
pub async fn incr(node: &impl std::ops::Deref<Target = Node>, db: &str) -> Result<f64, NodeError> {
    let response = node
        .mutate(
            db.as_bytes(),
            "app",
            "incr",
            &key("k"),
            &MutationOptions::default(),
        )
        .await?;
    match &response.outcome {
        flower_cluster::types::ResponseOutcome::Value(_) => match value(&response.outcome) {
            Value::Number(count) => Ok(count),
            other => panic!("incr answered {other:?}"),
        },
        other => Err(NodeError::Failure {
            code: "refused".into(),
            message: format!("{other:?}"),
        }),
    }
}

/// Counter `k` of `db`, read fresh at `node`.
pub async fn read(node: &impl std::ops::Deref<Target = Node>, db: &str) -> f64 {
    match node
        .query(
            db.as_bytes(),
            "app",
            "read",
            &key("k"),
            &CallOptions::default(),
        )
        .await
        .unwrap()
    {
        Value::Number(count) => count,
        other => panic!("read answered {other:?}"),
    }
}

/// Data directory of node `id`.
pub fn node_dir(hosts: &Hosts, id: u64) -> PathBuf {
    hosts.dir.path().join(format!("n{id}"))
}
