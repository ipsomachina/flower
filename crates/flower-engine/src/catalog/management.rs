//! Durable operator plans. Resize commits all planned moves together;
//! the existing crash-resumable move driver completes each one.

use super::*;
use std::collections::BTreeSet;

pub(super) const LEADERS: &[u8] = b"leader\0";
const RESIZES: &[u8] = b"resize\0";

/// A group's desired leader; `None` leaves leadership to elections.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct LeaderPreference {
    pub node: Option<u64>,
    pub op: String,
}

/// An immutable resize receipt. The listed moves report their own progress.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ResizePlan {
    pub op: String,
    pub groups: Vec<String>,
    pub moves: Vec<Movement>,
}

pub(super) fn leaders(read: &ReadTxn) -> Result<BTreeMap<String, LeaderPreference>> {
    read.scan_meta_prefix(CATALOG_DB, LEADERS)?
        .into_iter()
        .map(|(name, value)| {
            let name = String::from_utf8(name[LEADERS.len()..].to_vec())
                .map_err(|_| Error::corrupt("invalid leader preference name".to_string()))?;
            Ok((name, decode(&value)?))
        })
        .collect()
}

pub(super) fn prefer(write: &WriteTxn, group: &str, node: Option<u64>, op: &str) -> Result<Step> {
    if group != ROOT_GROUP {
        let Some(entry) =
            read_group(write, group)?.filter(|entry| entry.status != GroupStatus::Removed)
        else {
            return unknown(format!("no group `{group}`"));
        };
        if node.is_some_and(|node| !entry.members.contains(&node)) {
            return conflict("preferred leader must be a group member");
        }
    }
    let preference = LeaderPreference {
        node,
        op: op.into(),
    };
    let row = key(LEADERS, group);
    if let Some(bytes) = write.meta(CATALOG_DB, &row)? {
        let previous: LeaderPreference = decode(&bytes)?;
        if previous.op == op {
            return if previous == preference {
                Ok(Step::Replayed(to_value(&previous)))
            } else {
                conflict("leader preference operation was reused")
            };
        }
    }
    put(write, &row, &preference)?;
    Ok(Step::Changed(to_value(&preference)))
}

pub(super) fn spread(write: &WriteTxn, group: &GroupEntry) -> Result<()> {
    let mut counts = BTreeMap::<u64, usize>::new();
    for (_, bytes) in write.scan_meta_prefix(CATALOG_DB, LEADERS)? {
        if let Some(node) = decode::<LeaderPreference>(&bytes)?.node {
            *counts.entry(node).or_default() += 1;
        }
    }
    let node = group
        .members
        .iter()
        .min_by_key(|node| (counts.get(node).copied().unwrap_or(0), **node))
        .copied();
    put(
        write,
        &key(LEADERS, &group.name),
        &LeaderPreference {
            node,
            op: group.op.clone(),
        },
    )
}

pub(super) fn resize(write: &WriteTxn, groups: &[String], op: &str, now: u64) -> Result<Step> {
    let unique: BTreeSet<_> = groups.iter().cloned().collect();
    if unique.is_empty() || unique.len() != groups.len() || op.is_empty() {
        return conflict("resize needs distinct groups and a nonempty operation identity");
    }
    let groups: Vec<_> = unique.into_iter().collect();
    let row = key(RESIZES, op);
    if let Some(bytes) = write.meta(CATALOG_DB, &row)? {
        let previous: ResizePlan = decode(&bytes)?;
        return if previous.groups == groups {
            Ok(Step::Replayed(to_value(&previous)))
        } else {
            conflict("resize operation identity was reused")
        };
    }
    for group in &groups {
        if !usable_group(write, group)? {
            return unknown(format!("no active group `{group}`"));
        }
    }
    let mut owners: Vec<PartitionEntry> = write
        .scan_meta_prefix(CATALOG_DB, PARTITION_PREFIX)?
        .into_iter()
        .map(|(_, bytes)| decode(&bytes))
        .collect::<Result<_>>()?;
    owners.retain(|entry| entry.status != PartitionStatus::Retired);
    if owners
        .iter()
        .any(|entry| entry.status != PartitionStatus::Active || entry.movement.is_some())
    {
        return conflict("finish existing partition operations before resizing");
    }
    owners.sort_by(|a, b| a.name.cmp(&b.name));
    let mut desired: BTreeMap<_, _> = groups
        .iter()
        .enumerate()
        .map(|(i, group)| {
            (
                group.clone(),
                owners.len() / groups.len() + usize::from(i < owners.len() % groups.len()),
            )
        })
        .collect();
    let mut surplus = Vec::new();
    for owner in owners {
        match desired.get_mut(&owner.owner) {
            Some(left) if *left > 0 => *left -= 1,
            _ => surplus.push(owner),
        }
    }
    if surplus.iter().any(|entry| entry.epoch >= u64::MAX - 1) {
        return conflict("a partition ran out of ownership epochs");
    }
    let destinations = desired
        .into_iter()
        .flat_map(|(group, count)| std::iter::repeat_n(group, count));
    let mut moves = Vec::new();
    // Everything that can refuse is checked before the first write.
    for (mut owner, destination) in surplus.into_iter().zip(destinations) {
        let movement = Movement {
            op: format!("resize:{}:{op}:{}", op.len(), owner.name),
            partition: owner.name.clone(),
            source: owner.owner.clone(),
            destination,
            source_epoch: owner.epoch,
            epoch: owner.epoch + 1,
            phase: MovePhase::Copying,
            reason: None,
            started_ms: now,
            updated_ms: now,
        };
        owner.op = movement.op.clone();
        owner.status = PartitionStatus::Moving;
        owner.updated_ms = now;
        owner.movement = Some(movement.clone());
        put(write, &key(PARTITION_PREFIX, &owner.name), &owner)?;
        moves.push(movement);
    }
    let plan = ResizePlan {
        op: op.into(),
        groups,
        moves,
    };
    put(write, &row, &plan)?;
    Ok(Step::Changed(to_value(&plan)))
}
