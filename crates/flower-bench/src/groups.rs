//! Many groups in one set of server processes (v1's `--hosted`).
//!
//! v1's hosted mode ran one process per replica slot, host *k* serving
//! replica *k* of every group. v2's `serve` hosts any number of Raft
//! groups per process, so the bench starts the usual one, two or three
//! processes and then, through the catalog the root group keeps:
//!
//! 1. creates groups `g1` to `gN` founded on every process
//!    (`POST /v1/admin/groups`) and waits until every process hosts an
//!    active replica of each, with a leader;
//! 2. spreads the leaders as v1 did by rotating the initializing host:
//!    group *k*'s leadership goes to process *k mod replicas*
//!    (`POST /v1/admin/leadership?group=`), so each process starts out
//!    leading an equal share;
//! 3. places each tenant as a named partition `pizza-<tenant>` on its
//!    group (`POST /v1/admin/partitions`), v1's static tenant-to-group
//!    assignment, and waits until each is active there.
//!
//! Calls then name the tenant's database and every member routes them to
//! the owner group's leader; replies name that leader, which the bench's
//! per-group client follows. The audit reads the catalog again to prove
//! no partition moved or changed epoch during the run.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use flower_client::client::RequestOptions;
use serde::Serialize;
use serde_json::json;

use crate::client::BenchClient;

/// How long creating groups or partitions may take.
const PLACE_TIMEOUT: Duration = Duration::from_secs(90);

/// The database (named partition) that holds one tenant.
pub fn tenant_db(tenant: &str) -> String {
    format!("pizza-{tenant}")
}

/// Group `index`'s name: `g1`, `g2`, ...
pub fn group_name(index: usize) -> String {
    format!("g{}", index + 1)
}

/// Where the catalog says a partition is.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Placement {
    /// The partition (its database).
    pub partition: String,
    /// The group serving it.
    pub group: String,
    /// Its ownership epoch.
    pub epoch: u64,
    /// `active`, `moving`, ...
    pub status: String,
}

fn options() -> RequestOptions {
    RequestOptions::new().timeout(Duration::from_secs(10))
}

/// Create `groups` founded on `members` and wait until every member
/// hosts each one active, with a leader.
pub async fn create_groups(
    root: &BenchClient,
    groups: &[String],
    members: &[u64],
) -> Result<(), String> {
    let deadline = Instant::now() + PLACE_TIMEOUT;
    for group in groups {
        let op = format!("create-{group}");
        let mut member = 0;
        loop {
            let error = match root
                .admin_client(member)
                .create_group(group, members, Some(&op), &options())
                .await
            {
                Ok(_) => break,
                Err(error) => error.to_string(),
            };
            if Instant::now() > deadline {
                return Err(format!("creating group {group} failed: {error}"));
            }
            member = (member + 1) % root.members().len();
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
    let mut last = String::new();
    while Instant::now() < deadline {
        let mut ready = true;
        for member in 0..root.members().len() {
            match root.admin_client(member).groups(&options()).await {
                Ok(listed) => {
                    for group in groups {
                        let entry = listed.groups.iter().find(|entry| &entry.name == group);
                        let up = entry.is_some_and(|entry| {
                            entry.status == "active" && entry.hosted && entry.leader.is_some()
                        });
                        if !up {
                            ready = false;
                            last = format!(
                                "n{} sees {group} as {:?}",
                                root.members()[member].id,
                                entry.map(|entry| (&entry.status, entry.hosted, entry.leader))
                            );
                        }
                    }
                }
                Err(error) => {
                    ready = false;
                    last = error.to_string();
                }
            }
        }
        if ready {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err(format!(
        "the groups were not active on every member within {}s: {last}",
        PLACE_TIMEOUT.as_secs()
    ))
}

/// The member index that leads `group`, if one says so.
pub async fn leader_of(root: &BenchClient, group: &str) -> Option<usize> {
    for member in 0..root.members().len() {
        if let Ok(status) = root
            .status_in(member, Some(group), Duration::from_millis(500))
            .await
            && status.state == "Leader"
            && status.leader == Some(status.id)
        {
            return Some(member);
        }
    }
    None
}

/// Hand group *k*'s leadership to member *k mod members*, as v1 rotated
/// the initializing host. Best effort: returns what could not be done.
pub async fn spread_leaders(root: &BenchClient, groups: &[String]) -> Vec<String> {
    let mut notes = Vec::new();
    let count = root.members().len();
    if count < 2 {
        return notes;
    }
    for (index, group) in groups.iter().enumerate() {
        let wanted = index % count;
        let to = root.members()[wanted].id;
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut done = false;
        while Instant::now() < deadline {
            match leader_of(root, group).await {
                Some(leader) if leader == wanted => {
                    done = true;
                    break;
                }
                Some(leader) => {
                    let path = format!("/v1/admin/leadership?group={group}");
                    let _ = root
                        .admin_post(leader, &path, &json!({"to": to}), Duration::from_secs(5))
                        .await;
                }
                None => {}
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        if !done {
            notes.push(format!("{group}: leadership did not move to n{to}"));
        }
    }
    notes
}

/// Place each `(partition, group)` and wait until the catalog lists
/// every one active on its group.
pub async fn place(root: &BenchClient, partitions: &[(String, String)]) -> Result<(), String> {
    let deadline = Instant::now() + PLACE_TIMEOUT;
    for (partition, group) in partitions {
        let op = format!("create-{partition}");
        let mut member = 0;
        loop {
            let error = match root
                .admin_client(member)
                .create_partition(partition, group, Some(&op), &options())
                .await
            {
                Ok(_) => break,
                Err(error) => error.to_string(),
            };
            if Instant::now() > deadline {
                return Err(format!("placing {partition} on {group} failed: {error}"));
            }
            member = (member + 1) % root.members().len();
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
    let wanted: BTreeMap<&str, &str> = partitions
        .iter()
        .map(|(partition, group)| (partition.as_str(), group.as_str()))
        .collect();
    let mut last = String::new();
    while Instant::now() < deadline {
        match placements(root).await {
            Ok(placed) => {
                let active = wanted.iter().all(|(partition, group)| {
                    placed.iter().any(|placement| {
                        placement.partition == *partition
                            && placement.group == *group
                            && placement.status == "active"
                    })
                });
                if active {
                    return Ok(());
                }
                last = format!("{placed:?}");
            }
            Err(error) => last = error,
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err(format!(
        "the partitions were not active within {}s: {last}",
        PLACE_TIMEOUT.as_secs()
    ))
}

/// The catalog's partitions, from any member that answers.
pub async fn placements(root: &BenchClient) -> Result<Vec<Placement>, String> {
    let mut last = String::new();
    for member in 0..root.members().len() {
        match root.admin_client(member).partitions(&options()).await {
            Ok(listed) => {
                return Ok(listed
                    .partitions
                    .into_iter()
                    .map(|partition| Placement {
                        partition: partition.name,
                        group: partition.owner,
                        epoch: partition.epoch,
                        status: partition.status,
                    })
                    .collect());
            }
            Err(error) => last = error.to_string(),
        }
    }
    Err(format!("reading the catalog failed: {last}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_follow_the_plan() {
        assert_eq!(group_name(0), "g1");
        assert_eq!(group_name(7), "g8");
        assert_eq!(tenant_db("tenant-3"), "pizza-tenant-3");
    }
}
