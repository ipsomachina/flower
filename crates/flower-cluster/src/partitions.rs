//! A host's side of the catalog: reaching any group's leader, keeping
//! this process's replicas in step with the groups the catalog lists,
//! routing databases to their owners, and driving partition creation,
//! retirement and moves from the root's leader.
//!
//! **Reaching a group.** [`Host::send`] delivers a member request to a
//! group's leader whether or not this process hosts the group: to the
//! leader its own replica knows, else to the leader it learned last, else
//! to the group's founding members in turn, following each `NotLeader`
//! hint, within a budget. Node addresses come from the root group's
//! membership, which every process belongs to. A member that fails to
//! answer is tried only after every other for a second, and hints naming
//! it are not followed meanwhile; a dial is given up after three
//! heartbeats at first, twice as late each round: a dead leader goes on
//! being named until its group notices, and dialing a dead host can take
//! seconds, while the others name the new leader once one is elected.
//!
//! **The keeper.** [`Host::run_keeper`] follows the catalog the root
//! applies here. It opens a replica of every group listing this node and
//! closes (and retires the files of) groups the catalog removed. The
//! lowest listed member founds a new group, but only through the
//! catalog: it claims the group (`initializing`, naming itself) and
//! initializes its Raft membership only once the claim applied and only
//! while it holds no Raft state for the group, or, when it finds its own
//! claim but no state, only after asking every other member and hearing
//! that none is initialized (it crashed before initializing; had it lost
//! its disk after, some member would be). The leader of a group the
//! catalog has not marked active marks it.
//!
//! **The driver.** [`Host::run_driver`] runs while this process leads
//! the root group. Every unfinished creation, retirement and move in the
//! catalog takes its next step there (see [`crate::moves`]); each step
//! is an idempotent control request to a group's leader, and the catalog
//! advances only after the step applied, so a driver that crashes or is
//! deposed mid-step leaves nothing the next one cannot repeat.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, PoisonError, Weak};
use std::time::Duration;

use flower_engine::catalog::{
    CATALOG_DB, Catalog, CatalogChange, GroupEntry, GroupStatus, PartitionEntry, PartitionStatus,
};
use flower_sdk::{Failure, Value};
use openraft::BasicNode;
use tokio::sync::watch;
use tokio::time::Instant;

use crate::group::{GroupId, ROOT};
use crate::host::Host;
use crate::node::{Node, NodeError, control_outcome};
use crate::peer::{ControlRequest, MemberError, MemberRequest, MemberResponse};
use crate::types::NodeId;

/// How long [`Host::send`] keeps looking for a group's leader.
pub const SEND_BUDGET: Duration = Duration::from_secs(10);
/// How long one member request to a group's leader may take.
const SEND_TIMEOUT: Duration = Duration::from_secs(10);
/// How long [`Host::send`] tries a member that failed to answer only
/// after every other: a dead process goes on being named leader until
/// its group notices, and dialing it can take seconds.
const UNANSWERED_FOR: Duration = Duration::from_secs(1);
/// How often the keeper and driver look at the catalog without a
/// notice.
const TICK: Duration = Duration::from_millis(250);
/// Failure code: no member of a group could be reached.
pub const GROUP_UNREACHABLE: &str = "group_unreachable";

/// Where a database is served from, as this process's catalog says.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Route {
    /// The group owning it: root for an unplaced database.
    pub group: GroupId,
    /// Its ownership epoch, when placed.
    pub epoch: Option<u64>,
    /// Its catalog entry, when placed.
    pub partition: Option<PartitionEntry>,
}

impl Host {
    /// Reconcile preferred leaders independently of moves and the replica
    /// keeper. Catalog outages suspend transfers; failed transfers back off.
    pub async fn run_leader_placement(host: Weak<Host>, mut stop: watch::Receiver<bool>) {
        let mut ready = BTreeSet::new();
        let mut retry = BTreeMap::new();
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = tick.tick() => {},
                changed = stop.changed() => { if changed.is_err() || *stop.borrow() { return; } }
            }
            if *stop.borrow() {
                return;
            }
            let Some(host) = host.upgrade() else { return };
            // A settled placement costs no quorum reads. The applied
            // catalog is only a hint that a transfer might be needed;
            // a fresh proof below is still required before any action.
            let hinted = host.cached_catalog();
            if !hinted.leaders.iter().any(|(name, preference)| {
                preference.node.is_some_and(|to| {
                    host.group(name)
                        .is_some_and(|node| node.preferred_ready(to))
                })
            }) {
                ready.clear();
                retry.clear();
                continue;
            }
            let catalog = tokio::select! {
                catalog = host.catalog() => catalog,
                _ = stop.changed() => return,
            };
            let Ok(catalog) = catalog else {
                ready.clear();
                continue;
            };
            let mut next = BTreeSet::new();
            for (name, preference) in &catalog.leaders {
                let Some(to) = preference.node else { continue };
                let Some(node) = host.group(name) else {
                    continue;
                };
                let identity = (node.group().clone(), to);
                if !node.preferred_ready(to) {
                    continue;
                }
                next.insert(identity.clone());
                if !ready.contains(&identity)
                    || retry
                        .get(&identity)
                        .is_some_and(|(at, _)| Instant::now() < *at)
                {
                    continue;
                }
                // Let an in-flight transfer finish its gates at shutdown.
                match node.transfer_leadership(to).await {
                    Ok(_) => {
                        retry.remove(&identity);
                    }
                    Err(error) => {
                        let delay = retry.get(&identity).map_or(
                            Duration::from_secs(5),
                            |(_, delay): &(Instant, Duration)| {
                                (*delay * 2).min(Duration::from_secs(60))
                            },
                        );
                        retry.insert(identity, (Instant::now() + delay, delay));
                        tracing::debug!(%name, %to, %error, "preferred leader transfer will retry");
                    }
                }
            }
            retry.retain(|identity, _| next.contains(identity));
            ready = next;
        }
    }

    /// The catalog as this process's root replica applied it.
    pub fn applied_catalog(&self) -> Result<Catalog, NodeError> {
        self.root().applied_catalog()
    }

    /// The catalog, linearizable through the root's leader.
    pub async fn catalog(&self) -> Result<Catalog, NodeError> {
        self.root().catalog().await
    }

    /// A group's identity, as the catalog lists it (root always).
    pub fn group_id(&self, catalog: &Catalog, name: &str) -> Option<GroupId> {
        if name == ROOT {
            return Some(GroupId::root());
        }
        let entry = catalog.groups.get(name)?;
        (entry.status != GroupStatus::Removed)
            .then(|| GroupId::new(&entry.name, entry.uid).ok())
            .flatten()
    }

    /// Where `db` is served from, by `catalog`: its owner when it is a
    /// partition, the root otherwise. `None` for a partition whose owner
    /// the catalog no longer lists.
    pub fn route_in(&self, catalog: &Catalog, db: &str) -> Option<Route> {
        match catalog.partitions.get(db) {
            None => Some(Route {
                group: GroupId::root(),
                epoch: None,
                partition: None,
            }),
            Some(entry) => Some(Route {
                group: self.group_id(catalog, &entry.owner)?,
                epoch: Some(entry.epoch),
                partition: Some(entry.clone()),
            }),
        }
    }

    /// Read the root's applied catalog into the routing cache, and
    /// answer it.
    pub fn refresh_catalog(&self) -> Result<Arc<Catalog>, NodeError> {
        let catalog = Arc::new(self.applied_catalog()?);
        let mut cached = self.catalog.write().unwrap_or_else(PoisonError::into_inner);
        // Applies only move forward: never replace a newer read.
        if catalog.revision >= cached.revision {
            *cached = catalog.clone();
        }
        Ok(cached.clone())
    }

    /// The catalog as routing sees it: the root's applied catalog as the
    /// keeper last read it. A route it gives may be stale by a change
    /// the root applied a moment ago; the owner's placement fence
    /// refuses such a request, and the caller asks again with
    /// [`Host::refresh_catalog`] or a fresh [`Host::catalog`].
    pub fn cached_catalog(&self) -> Arc<Catalog> {
        self.catalog
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Where `db` is served from, by the routing cache.
    pub fn route(&self, db: &str) -> Option<Route> {
        self.route_in(&self.cached_catalog(), db)
    }

    /// Where `db` is served from, by a catalog proven fresh through the
    /// root's leader: what routing asks after an owner refused.
    pub async fn fresh_route(&self, db: &str) -> Result<Option<Route>, NodeError> {
        let catalog = Arc::new(self.catalog().await?);
        {
            let mut cached = self.catalog.write().unwrap_or_else(PoisonError::into_inner);
            if catalog.revision >= cached.revision {
                *cached = catalog.clone();
            }
        }
        Ok(self.route_in(&catalog, db))
    }

    /// Every node's Raft address, from the root's membership.
    pub fn addresses(&self) -> BTreeMap<NodeId, String> {
        self.root().membership().nodes
    }

    /// The members to try for `group`'s leader, best first: the leader
    /// its local replica knows, the leader learned last, then the
    /// group's members (its replica's membership when hosted, else its
    /// founding members).
    fn candidates(&self, group: &GroupId, catalog: Option<&Catalog>) -> Vec<NodeId> {
        let mut order = Vec::new();
        let local = self.group(&group.name).filter(|node| node.group() == group);
        if let Some(node) = &local {
            let view = node.membership();
            order.extend(view.leader);
            order.extend(view.nodes.keys().copied());
        }
        if let Some(leader) = self.known_leader(group) {
            order.insert(0, leader);
        }
        if group.is_root() {
            order.extend(self.addresses().keys().copied());
        } else if let Some(entry) = catalog.and_then(|catalog| catalog.groups.get(&*group.name)) {
            order.extend(entry.members.iter().copied());
        }
        let mut seen = BTreeSet::new();
        order.retain(|id| seen.insert(*id));
        order
    }

    /// Deliver `request` to `group`'s leader and answer what it
    /// answered. `once` requests (forwarded client requests) are never
    /// sent twice: an exchange that broke after the request may have
    /// arrived fails with its outcome unknown. Other requests are
    /// idempotent and go again.
    pub async fn send(
        &self,
        group: &GroupId,
        request: MemberRequest,
        once: bool,
    ) -> Result<MemberResponse, NodeError> {
        let deadline = Instant::now()
            + crate::deadline::remaining().map_or(SEND_BUDGET, |left| left.min(SEND_BUDGET));
        let catalog = if group.is_root() {
            None
        } else {
            self.applied_catalog().ok()
        };
        let peers = self.peers(group);
        // Three heartbeats, the window in which a live leader's
        // followers hear from it (see `crate::read`); every group times
        // Raft as the root does.
        let dial_patience = {
            let config = self.root().raft().config().clone();
            Duration::from_millis(config.heartbeat_interval.saturating_mul(3))
                .min(config.leader_lease())
        };
        let mut rounds = 0u32;
        let mut last = String::from("no member was tried");
        let mut hint: Option<NodeId> = None;
        let mut tried = BTreeSet::new();
        loop {
            if Instant::now() >= deadline {
                return Err(NodeError::coded(
                    GROUP_UNREACHABLE,
                    format!("no leader of group {group} answered: {last}"),
                ));
            }
            // Members that just failed to answer are tried only once no
            // other is left, and hints naming them are not followed:
            // members go on naming a dead leader until they notice its
            // silence.
            let unanswered = self.unanswered();
            let mut candidates = self.candidates(group, catalog.as_ref());
            if candidates.iter().any(|id| !unanswered.contains(id)) {
                candidates.retain(|id| !unanswered.contains(id));
            }
            let next = hint
                .take()
                .filter(|id| !tried.contains(id) && !unanswered.contains(id))
                .or_else(|| candidates.iter().copied().find(|id| !tried.contains(id)));
            let Some(target) = next else {
                // Everyone answered without leading: give elections a
                // moment, then try them all again, waiting longer for
                // dials.
                tried.clear();
                rounds += 1;
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            };
            tried.insert(target);
            let outcome = if target == self.id()
                && let Some(node) = self.group(&group.name).filter(|node| node.group() == group)
            {
                node.member_desk().handle(request.clone()).await
            } else {
                let addr = self.addresses().get(&target).cloned().unwrap_or_default();
                // A dead member's host may never answer a dial (and a
                // refused one takes seconds on some systems): give it up
                // after a contact window, which a live member's dial
                // fits in where its heartbeats do, and twice as late each
                // round in case it does not. Nothing was sent yet.
                let patience = dial_patience.saturating_mul(1 << rounds.min(4));
                peers
                    .member_unless(
                        target,
                        &addr,
                        request.clone(),
                        SEND_TIMEOUT,
                        tokio::time::sleep(patience),
                    )
                    .await
            };
            match &outcome {
                Ok(_) | Err(MemberError::NotLeader { .. } | MemberError::Failed { .. }) => {
                    self.answered(target, true);
                }
                Err(MemberError::Unreachable(_) | MemberError::Broken(_)) => {
                    self.answered(target, false);
                    self.forget_leader(group, target);
                }
            }
            match outcome {
                Ok(response) => {
                    self.learn_leader(group, target);
                    return Ok(response);
                }
                Err(MemberError::NotLeader { leader }) => {
                    last = format!("node {target} does not lead");
                    if let Some(leader) = leader {
                        tried.remove(&leader);
                        hint = Some(leader);
                    }
                }
                Err(MemberError::Unreachable(message)) => last = message,
                Err(MemberError::Broken(message)) if !once => last = message,
                Err(MemberError::Broken(message)) => {
                    return Err(NodeError::coded(
                        crate::peer::FORWARD_FAILED,
                        format!(
                            "the exchange with group {group}'s leader broke ({message}); the \
                             outcome is unknown: retry with the same idempotency key"
                        ),
                    ));
                }
                Err(MemberError::Failed { code, message }) => {
                    return Err(NodeError::Failure { code, message });
                }
            }
        }
    }

    /// Apply a control request at `group`'s leader: its answer, or the
    /// group's deterministic refusal as the inner `Err`.
    pub async fn control(
        &self,
        group: &GroupId,
        request: ControlRequest,
    ) -> Result<Result<Value, Failure>, NodeError> {
        match self
            .send(group, MemberRequest::Control(request), false)
            .await?
        {
            MemberResponse::Control(answer) => control_outcome(answer),
            _ => Err(NodeError::coded(
                "control",
                "a group's leader answered a control request with another kind",
            )),
        }
    }

    /// Apply a catalog change at the root's leader.
    pub async fn change_catalog(
        &self,
        change: CatalogChange,
    ) -> Result<Result<Value, Failure>, NodeError> {
        self.control(&GroupId::root(), ControlRequest::Catalog(change))
            .await
    }

    /// List a new group with founding `members` (root members all) under
    /// operation `op`; the keepers on those members open and found it.
    pub async fn create_group(
        &self,
        name: &str,
        members: Vec<NodeId>,
        op: &str,
    ) -> Result<Result<Value, Failure>, NodeError> {
        crate::group::validate_name(name).map_err(|message| NodeError::coded("group", message))?;
        let known = self.addresses();
        if let Some(stranger) = members.iter().find(|id| !known.contains_key(id)) {
            return Err(NodeError::coded(
                "group",
                format!("node {stranger} is not a member of the root group"),
            ));
        }
        let uid = loop {
            let uid = rand::random::<u64>();
            if uid != 0 {
                break uid;
            }
        };
        self.change_catalog(CatalogChange::CreateGroup {
            group: name.to_string(),
            uid,
            members,
            op: op.to_string(),
        })
        .await
    }

    /// The leader of `group` this host learned last, if any.
    fn known_leader(&self, group: &GroupId) -> Option<NodeId> {
        self.leaders
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(group)
            .copied()
    }

    fn learn_leader(&self, group: &GroupId, leader: NodeId) {
        self.leaders
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(group.clone(), leader);
    }

    /// Stop starting with `member` for `group`, if it was the leader
    /// learned last: it failed to answer.
    fn forget_leader(&self, group: &GroupId, member: NodeId) {
        let mut leaders = self.leaders.lock().unwrap_or_else(PoisonError::into_inner);
        if leaders.get(group) == Some(&member) {
            leaders.remove(group);
        }
    }

    /// Record whether `member` answered a member request.
    fn answered(&self, member: NodeId, answered: bool) {
        let mut unanswered = self
            .unanswered
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if answered {
            unanswered.remove(&member);
        } else {
            unanswered.insert(member, Instant::now());
        }
    }

    /// The members that failed to answer within [`UNANSWERED_FOR`].
    fn unanswered(&self) -> BTreeSet<NodeId> {
        let mut unanswered = self
            .unanswered
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        unanswered.retain(|_, at| at.elapsed() < UNANSWERED_FOR);
        unanswered.keys().copied().collect()
    }

    /// Keep this process's replicas in step with the catalog until
    /// `stop` turns true: on every catalog change the root applies here,
    /// and every quarter second.
    pub async fn run_keeper(host: Weak<Host>, mut stop: watch::Receiver<bool>) {
        let mut notices = match host.upgrade() {
            Some(host) => host.root().subscribe_commits(),
            None => return,
        };
        loop {
            if *stop.borrow() {
                return;
            }
            let Some(strong) = host.upgrade() else { return };
            if let Err(error) = strong.keep_once().await {
                tracing::debug!(%error, "the group keeper will try again");
            }
            drop(strong);
            tokio::select! {
                _ = tokio::time::sleep(TICK) => {}
                notice = notices.recv() => {
                    if let Ok(notice) = notice && notice.db != CATALOG_DB {
                        continue;
                    }
                }
                changed = stop.changed() => {
                    if changed.is_err() {
                        return;
                    }
                }
            }
        }
    }

    /// One pass of the keeper over the applied catalog.
    pub async fn keep_once(&self) -> Result<(), NodeError> {
        let catalog = self.refresh_catalog()?;
        for entry in catalog.groups.values() {
            if let Err(error) = self.keep_group(entry).await {
                tracing::debug!(group = %entry.name, %error, "keeping a group failed for now");
            }
        }
        Ok(())
    }

    /// Bring one listed group's local replica in line with its entry.
    async fn keep_group(&self, entry: &GroupEntry) -> Result<(), NodeError> {
        let group = GroupId::new(&entry.name, entry.uid)
            .map_err(|message| NodeError::coded("group", message))?;
        let listed = entry.members.contains(&self.id());
        if entry.status == GroupStatus::Removed || !listed {
            if entry.status == GroupStatus::Removed
                && self
                    .group(&entry.name)
                    .is_some_and(|node| *node.group() == group)
            {
                self.close_group(&entry.name, true).await?;
            }
            return Ok(());
        }
        let node = self.open_group(group.clone()).await?;
        match entry.status {
            GroupStatus::Creating if entry.members.first() == Some(&self.id()) => {
                if !node.is_initialized().await? {
                    self.change_catalog(CatalogChange::GroupInitializing {
                        group: entry.name.clone(),
                        uid: entry.uid,
                        founder: self.id(),
                    })
                    .await?
                    .map_err(NodeError::failure)?;
                }
            }
            GroupStatus::Initializing if entry.founder == Some(self.id()) => {
                let unfounded =
                    !node.is_initialized().await? && self.nobody_initialized(&group, entry).await;
                if unfounded {
                    let addresses = self.addresses();
                    let members: BTreeMap<NodeId, BasicNode> = entry
                        .members
                        .iter()
                        .map(|id| {
                            let addr = addresses.get(id).cloned().unwrap_or_default();
                            (*id, BasicNode::new(addr))
                        })
                        .collect();
                    node.initialize_nodes(members).await?;
                }
            }
            _ => {}
        }
        if matches!(
            entry.status,
            GroupStatus::Creating | GroupStatus::Initializing
        ) && node.status().leader == Some(self.id())
        {
            self.change_catalog(CatalogChange::GroupActive {
                group: entry.name.clone(),
                uid: entry.uid,
            })
            .await?
            .map_err(NodeError::failure)?;
        }
        Ok(())
    }

    /// Whether no other founding member of `group` holds Raft state for
    /// it: every one answered uninitialized. One that cannot be asked
    /// counts as initialized, so the founder never founds twice.
    async fn nobody_initialized(&self, group: &GroupId, entry: &GroupEntry) -> bool {
        let peers = self.peers(group);
        let addresses = self.addresses();
        for member in entry.members.iter().filter(|id| **id != self.id()) {
            let Some(addr) = addresses.get(member) else {
                return false;
            };
            match peers.info(*member, addr, SEND_TIMEOUT).await {
                Ok(info) if !info.initialized => {}
                _ => return false,
            }
        }
        true
    }

    /// Drive the catalog's unfinished work while this process leads the
    /// root group, until `stop` turns true.
    pub async fn run_driver(host: Weak<Host>, mut stop: watch::Receiver<bool>) {
        let mut notices = match host.upgrade() {
            Some(host) => host.root().subscribe_commits(),
            None => return,
        };
        loop {
            if *stop.borrow() {
                return;
            }
            let Some(strong) = host.upgrade() else { return };
            let root = strong.root();
            if root.status().leader == Some(strong.id())
                && let Err(error) = crate::moves::drive_once(&strong).await
            {
                tracing::debug!(%error, "the partition driver will try again");
            }
            drop((root, strong));
            tokio::select! {
                _ = tokio::time::sleep(TICK) => {}
                notice = notices.recv() => {
                    if let Ok(notice) = notice && notice.db != CATALOG_DB {
                        continue;
                    }
                }
                changed = stop.changed() => {
                    if changed.is_err() {
                        return;
                    }
                }
            }
        }
    }
}

/// Whether a partition entry has work for the driver.
pub fn unfinished(entry: &PartitionEntry) -> bool {
    matches!(
        entry.status,
        PartitionStatus::Creating | PartitionStatus::Retiring | PartitionStatus::Moving
    )
}

/// A node's own group replica, if it is the one `group` names.
pub fn hosted(host: &Host, group: &GroupId) -> Option<Arc<Node>> {
    host.group(&group.name).filter(|node| node.group() == group)
}
