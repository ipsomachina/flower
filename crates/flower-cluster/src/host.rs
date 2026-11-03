//! One process, many groups.
//!
//! A [`Host`] owns what a process has once whatever the number of Raft
//! groups it serves: its node ID and data directory, the runtime factory
//! (and through it the process-wide Wasm engine and image cache), the
//! peer transport and the directory its one listener answers from, and
//! the admission that bounds evaluation slots, evaluation memory and
//! request bytes for every group's reads. Each group it opens is a
//! complete [`Node`], unchanged from a lone member: its own log and
//! group files, engine, writer, leader, snapshots, compaction and
//! health, plus a writer admission of its own (see
//! [`Node::with_shared_admission`]).
//!
//! The root group's files stay where a single-group member always kept
//! them, in the data directory itself; every other group lives under
//! `groups/<name>/`, with a `group.id` file naming the group and the
//! uid it was created with, written before the group's files and checked
//! at every open. A replica whose recorded uid differs is refused, so
//! the files of a removed group never join a new group that took its
//! name. Closing a group for good moves its directory under
//! `groups/.retired/<name>-<uid>/`, left for the operator.
//!
//! Each open group runs the duties `serve` ran for its one node
//! ([`GroupDuties`]): maintenance (timers, retention, the incarnation,
//! receipt purge), the soft snapshot triggers and online compaction,
//! plus any the caller adds ([`HostSettings::duties`], such as
//! continuous backup), all stopped when the group closes.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError, RwLock};
use std::time::Duration;

use flower_engine::storage::compact::CompactionSettings;
use openraft::Config;
use tokio::sync::{Mutex as AsyncMutex, broadcast, watch};
use tokio::task::JoinHandle;

use crate::admission::{Admission, AdmissionConfig};
use crate::flush::FlushSettings;
use crate::fsync::Durability;
use crate::group::{GroupId, ROOT, validate_name};
use crate::network::{
    InProcessFactory, PeerConfig, PeerDirectory, PeerFactory, Peers, Registry, TcpFactory,
    serve_directory,
};
use crate::node::{Node, NodeError, TransferOutcome};
use crate::read::ReadSettings;
use crate::snapshot_policy::SnapshotPolicy;
use crate::statemachine::RuntimeFactory;
use crate::types::NodeId;

/// Directory holding every group but the root.
pub const GROUPS_DIR: &str = "groups";
/// Directory under [`GROUPS_DIR`] holding closed-for-good groups.
pub const RETIRED_DIR: &str = ".retired";
/// File naming a group directory's group: `<name> <uid as 16 hex digits>`.
pub const GROUP_ID_FILE: &str = "group.id";
/// How long a move may keep its partition unserved by default.
pub const DEFAULT_FREEZE_MAX: Duration = Duration::from_secs(30);
/// The longest a group close waits for other holders of its node to
/// let go before shutting it down anyway.
const CLOSE_WAIT: Duration = Duration::from_secs(10);

/// How a host's groups reach their peers.
#[derive(Clone)]
pub enum Transport {
    /// Through an in-process registry, as tests and single-process
    /// clusters do.
    InProcess(Registry),
    /// Over authenticated TCP with these settings.
    Tcp(PeerConfig),
}

/// Extra per-group duties a caller runs beside the host's own, such as
/// continuous backup: started when the group opens, given its stop
/// signal, awaited when it closes.
pub type DutyStarter =
    Arc<dyn Fn(Arc<Node>, watch::Receiver<bool>) -> Vec<JoinHandle<()>> + Send + Sync>;

/// Configures each group's node after it opens: key catalogs, query
/// cache sizes, the advertised API URL.
pub type NodeConfigurer = Arc<dyn Fn(&GroupId, Node) -> Node + Send + Sync>;

/// Builds each group's runtime factory: groups keep their own managed
/// key catalogs, so each gets a crypto host of its own.
pub type RuntimesFor =
    Arc<dyn Fn(&GroupId) -> Result<Arc<dyn RuntimeFactory>, String> + Send + Sync>;

/// The background work every open group runs.
#[derive(Clone, Debug, Default)]
pub struct GroupDuties {
    /// Maintenance every interval (see [`Node::run_maintenance`]), and
    /// maintenance tasks at least that often (see [`Node::run_tasks`]).
    pub maintenance: Option<Duration>,
    /// Soft snapshot triggers (see [`Node::run_snapshots`]).
    pub snapshots: Option<SnapshotPolicy>,
    /// Online compaction of both files (see [`Node::run_compaction`]).
    pub compaction: Option<CompactionSettings>,
}

/// How a host opens its groups.
#[derive(Clone)]
pub struct HostSettings {
    /// Raft settings, the same for every group.
    pub raft: Arc<Config>,
    /// fsync on or off, for every group's files.
    pub durability: Durability,
    /// When every group's log and group file reach the disk (see
    /// [`crate::flush`]).
    pub flush: FlushSettings,
    /// Admission shared by every group's reads and control work.
    pub admission: AdmissionConfig,
    /// Each group's own writer admission.
    pub writers: AdmissionConfig,
    /// How each group proves fresh reads.
    pub reads: Option<ReadSettings>,
    /// The duties every group runs.
    pub duties: GroupDuties,
    /// Further per-group duties.
    pub extra_duties: Option<DutyStarter>,
    /// Per-group node configuration.
    pub configure: Option<NodeConfigurer>,
    /// Per-group runtime factories; the host's own for every group
    /// when unset.
    pub runtimes_for: Option<RuntimesFor>,
    /// How long a move may keep its partition unserved (or wait to freeze it)
    /// before the driver gives it up.
    pub freeze_max: Duration,
    /// Whether this host drives the catalog's unfinished work while it
    /// leads the root group (see [`crate::moves`]); tests that step a
    /// move themselves turn it off.
    pub drive: bool,
    /// Reconcile catalog leader preferences on this host's groups.
    pub leader_placement: bool,
}

impl HostSettings {
    /// Defaults around a Raft configuration: fsync on, default
    /// admission, no duties.
    pub fn new(raft: Arc<Config>) -> Self {
        HostSettings {
            raft,
            durability: Durability::on(),
            flush: FlushSettings::default(),
            admission: AdmissionConfig::default(),
            writers: AdmissionConfig::default(),
            reads: None,
            duties: GroupDuties::default(),
            extra_duties: None,
            configure: None,
            runtimes_for: None,
            freeze_max: DEFAULT_FREEZE_MAX,
            drive: true,
            leader_placement: true,
        }
    }
}

/// A group opening or closing on this host.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GroupEvent {
    /// The group opened and serves peers.
    Opened(GroupId),
    /// The group closed.
    Closed(GroupId),
}

/// The catalog-following tasks and their stop signal.
type Following = (watch::Sender<bool>, Vec<JoinHandle<()>>);

/// One open group: its node and its duties.
struct Hosted {
    node: Arc<Node>,
    stop: watch::Sender<bool>,
    duties: Vec<JoinHandle<()>>,
}

/// A process hosting any number of Raft groups.
pub struct Host {
    id: NodeId,
    dir: PathBuf,
    runtimes: Arc<dyn RuntimeFactory>,
    transport: Transport,
    directory: PeerDirectory,
    admission: Admission,
    settings: HostSettings,
    groups: RwLock<BTreeMap<Arc<str>, Hosted>>,
    /// Serializes opening and closing groups.
    changing: AsyncMutex<()>,
    events: broadcast::Sender<GroupEvent>,
    /// Peer clients for groups this host does not host, by group.
    remote: Mutex<BTreeMap<GroupId, Peers>>,
    /// The leader of each group as last learned, for reaching groups
    /// this host does not lead or host (see [`Host::send`]).
    pub(crate) leaders: Mutex<BTreeMap<GroupId, NodeId>>,
    /// Members whose last member request from here failed, and when
    /// (see [`Host::send`]).
    pub(crate) unanswered: Mutex<BTreeMap<NodeId, tokio::time::Instant>>,
    /// The catalog as the root last applied it here, for routing: the
    /// keeper refreshes it on every catalog change.
    pub(crate) catalog: RwLock<Arc<flower_engine::catalog::Catalog>>,
    /// Transactions a live call on this host coordinates: the resolver
    /// leaves them to it.
    pub(crate) driving: Mutex<std::collections::BTreeSet<String>>,
    /// Moves this host's driver holds for their destination's managed
    /// keys, by operation (see [`crate::moves`]).
    pub(crate) key_holds: Mutex<BTreeMap<String, crate::moves::KeyHold>>,
    listener: Mutex<Option<JoinHandle<()>>>,
    /// The keeper and driver, once started, and their stop signal.
    tasks: Mutex<Option<Following>>,
}

impl Host {
    /// Open the root group's replica in `dir`, as a lone member opens
    /// there, and nothing else yet: open further groups with
    /// [`Host::open_group`].
    pub async fn open(
        id: NodeId,
        dir: &Path,
        runtimes: Arc<dyn RuntimeFactory>,
        transport: Transport,
        settings: HostSettings,
    ) -> Result<Host, NodeError> {
        let (events, _) = broadcast::channel(64);
        let host = Host {
            id,
            dir: dir.to_path_buf(),
            runtimes,
            transport,
            directory: PeerDirectory::new(id),
            admission: Admission::new(settings.admission.clone()),
            settings,
            groups: RwLock::new(BTreeMap::new()),
            changing: AsyncMutex::new(()),
            events,
            remote: Mutex::new(BTreeMap::new()),
            leaders: Mutex::new(BTreeMap::new()),
            unanswered: Mutex::new(BTreeMap::new()),
            catalog: RwLock::default(),
            driving: Mutex::default(),
            key_holds: Mutex::default(),
            listener: Mutex::new(None),
            tasks: Mutex::new(None),
        };
        host.open_group(GroupId::root()).await?;
        host.refresh_catalog()?;
        Ok(host)
    }

    /// This process's node ID.
    pub fn id(&self) -> NodeId {
        self.id
    }

    /// The data directory.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// How long a move may keep its partition unserved.
    pub fn freeze_max(&self) -> Duration {
        self.settings.freeze_max
    }

    /// The admission every group's reads share.
    pub fn admission(&self) -> &Admission {
        &self.admission
    }

    /// The runtime factory every group builds guests with.
    pub fn runtimes(&self) -> &Arc<dyn RuntimeFactory> {
        &self.runtimes
    }

    /// The directory the peer listener answers from.
    pub fn directory(&self) -> &PeerDirectory {
        &self.directory
    }

    /// Serve every hosted group's peers on `listener` (TCP hosts),
    /// until the host shuts down.
    pub fn serve_peers(&self, listener: tokio::net::TcpListener) -> Result<(), NodeError> {
        let Transport::Tcp(config) = &self.transport else {
            return Err(NodeError::coded(
                "transport",
                "an in-process host serves no listener",
            ));
        };
        let task = tokio::spawn(serve_directory(
            listener,
            self.directory.clone(),
            config.clone(),
        ));
        if let Some(old) = self
            .listener
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .replace(task)
        {
            old.abort();
        }
        Ok(())
    }

    /// Follow groups opening and closing.
    pub fn subscribe(&self) -> broadcast::Receiver<GroupEvent> {
        self.events.subscribe()
    }

    /// The root group's node.
    pub fn root(&self) -> Arc<Node> {
        self.group(ROOT)
            .expect("the root group is open while the host lives")
    }

    /// An open group's node, by name.
    pub fn group(&self, name: &str) -> Option<Arc<Node>> {
        self.groups
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(name)
            .map(|hosted| hosted.node.clone())
    }

    /// Every open group's node, by name.
    pub fn groups(&self) -> Vec<Arc<Node>> {
        self.groups
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .map(|hosted| hosted.node.clone())
            .collect()
    }

    /// As a member about to stop, hand the leadership of every group it
    /// leads to another voter of that group, all at once
    /// ([`Node::hand_off_leadership`]): a planned restart then costs each
    /// group a transfer rather than a failover. One outcome per group;
    /// `None` for those this member does not lead.
    pub async fn hand_off_leadership(
        &self,
    ) -> Vec<(GroupId, Result<Option<TransferOutcome>, NodeError>)> {
        let mut handing = tokio::task::JoinSet::new();
        for node in self.groups() {
            handing.spawn(async move { (node.group().clone(), node.hand_off_leadership().await) });
        }
        let mut outcomes = Vec::new();
        while let Some(done) = handing.join_next().await {
            if let Ok(outcome) = done {
                outcomes.push(outcome);
            }
        }
        outcomes
    }

    /// Where `group`'s files live on this host.
    pub fn group_dir(&self, group: &str) -> PathBuf {
        if group == ROOT {
            self.dir.clone()
        } else {
            self.dir.join(GROUPS_DIR).join(group)
        }
    }

    /// The groups whose directories this host holds, by their `group.id`
    /// files (the root always), whether open or not: a restart reopens
    /// them.
    pub fn stored_groups(&self) -> Result<Vec<GroupId>, NodeError> {
        let mut found = vec![GroupId::root()];
        let root = self.dir.join(GROUPS_DIR);
        let entries = match std::fs::read_dir(&root) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(found),
            Err(error) => return Err(NodeError::coded("io", error)),
        };
        for entry in entries {
            let entry = entry.map_err(|error| NodeError::coded("io", error))?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if validate_name(name).is_err() || name == ROOT {
                continue;
            }
            if let Some(group) = read_group_id(&entry.path())? {
                found.push(group);
            }
        }
        Ok(found)
    }

    /// Open a replica of `group` on this host (creating its directory
    /// the first time), configure it, start its duties and answer its
    /// peers. Opening a group already open returns it, if the uid
    /// matches. A replica opening without Raft state rejoins its group
    /// like any wiped member: initialize its membership separately.
    pub async fn open_group(&self, group: GroupId) -> Result<Arc<Node>, NodeError> {
        let _changing = self.changing.lock().await;
        if let Some(node) = self.group(&group.name) {
            if node.group().uid != group.uid {
                return Err(NodeError::coded(
                    "group",
                    format!(
                        "group `{}` is open here as {}, not {group}",
                        group.name,
                        node.group()
                    ),
                ));
            }
            return Ok(node);
        }
        let dir = self.group_dir(&group.name);
        if !group.is_root() {
            claim_group_dir(&dir, &group)?;
        }
        let node = match &self.transport {
            Transport::InProcess(registry) => {
                let factory = InProcessFactory::new(registry.clone())
                    .with_source(self.id)
                    .for_group(group.clone());
                self.open_node(&group, &dir, factory).await?
            }
            Transport::Tcp(config) => {
                let factory = TcpFactory::for_group(self.id, config.clone(), group.clone());
                self.open_node(&group, &dir, factory).await?
            }
        };
        let node = Arc::new(node);
        // Its leader answers other groups' control requests.
        let weak = Arc::downgrade(&node);
        let control: std::sync::Weak<dyn crate::peer::ControlHandler> = weak;
        node.member_desk().set_control(control);
        let (stop, stopped) = watch::channel(false);
        let duties = self.start_duties(&node, stopped);
        if let Transport::InProcess(registry) = &self.transport {
            registry.register_peer(node.peer_service());
        }
        self.directory.insert(node.peer_service());
        self.groups
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(
                group.name.clone(),
                Hosted {
                    node: node.clone(),
                    stop,
                    duties,
                },
            );
        let _ = self.events.send(GroupEvent::Opened(group));
        Ok(node)
    }

    /// Open one group's node through `factory`, configured.
    async fn open_node<F: PeerFactory>(
        &self,
        group: &GroupId,
        dir: &Path,
        factory: F,
    ) -> Result<Node, NodeError> {
        let runtimes = match &self.settings.runtimes_for {
            Some(build) => build(group).map_err(|message| NodeError::coded("runtimes", message))?,
            None => self.runtimes.clone(),
        };
        let node = Node::open_with_durability(
            self.id,
            dir,
            self.settings.raft.clone(),
            factory,
            runtimes,
            self.settings.durability.clone(),
        )
        .await?
        .with_flush(self.settings.flush)
        .with_shared_admission(self.admission.clone(), self.settings.writers.clone());
        let node = match &self.settings.reads {
            Some(reads) => node.with_reads(*reads),
            None => node,
        };
        Ok(match &self.settings.configure {
            Some(configure) => configure(group, node),
            None => node,
        })
    }

    /// Start a group's duties.
    fn start_duties(
        &self,
        node: &Arc<Node>,
        stopped: watch::Receiver<bool>,
    ) -> Vec<JoinHandle<()>> {
        let duties = &self.settings.duties;
        let mut tasks = Vec::new();
        if let Some(interval) = duties.maintenance {
            // Maintenance tasks wake on their own schedule, beside it.
            let (member, halted) = (node.clone(), stopped.clone());
            tasks.push(tokio::spawn(async move {
                member.run_tasks(interval, halted).await;
            }));
            let (node, stopped) = (node.clone(), stopped.clone());
            tasks.push(tokio::spawn(async move {
                node.run_maintenance(interval, stopped).await;
            }));
        }
        if let Some(policy) = duties.snapshots {
            let (node, stopped) = (node.clone(), stopped.clone());
            tasks.push(tokio::spawn(async move {
                node.run_snapshots(policy, stopped).await;
            }));
        }
        if let Some(settings) = duties.compaction {
            let (node, stopped) = (node.clone(), stopped.clone());
            tasks.push(tokio::spawn(async move {
                node.run_compaction(settings, stopped).await;
            }));
        }
        if let Some(start) = &self.settings.extra_duties {
            tasks.extend(start(node.clone(), stopped));
        }
        tasks
    }

    /// Close a group's replica: stop answering its peers, stop its
    /// duties and shut it down. With `retire`, its directory then moves
    /// under `groups/.retired/`. The root closes only with the host.
    pub async fn close_group(&self, name: &str, retire: bool) -> Result<(), NodeError> {
        if name == ROOT {
            return Err(NodeError::coded(
                "group",
                "the root group closes only with its host",
            ));
        }
        let _changing = self.changing.lock().await;
        let Some(hosted) = self
            .groups
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(name)
        else {
            return Ok(());
        };
        let group = hosted.node.group().clone();
        self.stop_group(hosted).await?;
        if retire {
            let dir = self.group_dir(name);
            let retired = self
                .dir
                .join(GROUPS_DIR)
                .join(RETIRED_DIR)
                .join(format!("{name}-{:016x}", group.uid));
            std::fs::create_dir_all(retired.parent().expect("retired path has a parent"))
                .map_err(|error| NodeError::coded("io", error))?;
            std::fs::rename(&dir, &retired).map_err(|error| NodeError::coded("io", error))?;
        }
        let _ = self.events.send(GroupEvent::Closed(group));
        Ok(())
    }

    /// Stop one group: peers first, then duties, then the node.
    async fn stop_group(&self, hosted: Hosted) -> Result<(), NodeError> {
        let Hosted { node, stop, duties } = hosted;
        let group = node.group().clone();
        self.directory.remove(&group.name);
        if let Transport::InProcess(registry) = &self.transport {
            registry.unregister_group(&group.name, self.id);
        }
        let _ = stop.send(true);
        for duty in duties {
            let _ = duty.await;
        }
        let node = unwrap_node(node).await;
        match node {
            Ok(node) => node.shutdown().await.map_err(NodeError::Fatal),
            Err(node) => {
                tracing::warn!(group = %group, "other holders kept the node past its close");
                node.raft()
                    .shutdown()
                    .await
                    .map_err(|error| NodeError::Fatal(error.to_string()))
            }
        }
    }

    /// Peer clients for `group` from this host, whether or not it hosts
    /// the group: the member channel to its members.
    pub fn peers(&self, group: &GroupId) -> Peers {
        if let Some(node) = self.group(&group.name)
            && node.group().uid == group.uid
        {
            return node.peer_client().clone();
        }
        let mut remote = self.remote.lock().unwrap_or_else(PoisonError::into_inner);
        remote
            .entry(group.clone())
            .or_insert_with(|| match &self.transport {
                Transport::InProcess(registry) => InProcessFactory::new(registry.clone())
                    .with_source(self.id)
                    .for_group(group.clone())
                    .peers(),
                Transport::Tcp(config) => {
                    TcpFactory::for_group(self.id, config.clone(), group.clone()).peers()
                }
            })
            .clone()
    }

    /// Start following the catalog: the keeper on every process, the
    /// partition driver wherever the root is led (see
    /// [`crate::partitions`]). They stop at shutdown.
    pub fn start(self: &Arc<Self>) {
        let mut tasks = self.tasks.lock().unwrap_or_else(PoisonError::into_inner);
        if tasks.is_some() {
            return;
        }
        let (stop, stopped) = watch::channel(false);
        let keeper = tokio::spawn(Host::run_keeper(Arc::downgrade(self), stopped.clone()));
        let resolver = tokio::spawn(Host::run_resolver(Arc::downgrade(self), stopped.clone()));
        let mut running = vec![keeper, resolver];
        if self.settings.leader_placement {
            running.push(tokio::spawn(Host::run_leader_placement(
                Arc::downgrade(self),
                stopped.clone(),
            )));
        }
        if self.settings.drive {
            running.push(tokio::spawn(Host::run_driver(
                Arc::downgrade(self),
                stopped,
            )));
        }
        *tasks = Some((stop, running));
    }

    /// Stop following the catalog: the keeper and the driver end, and
    /// hold no reference to this host once this returns.
    pub async fn stop_following(&self) {
        let tasks = self
            .tasks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some((stop, tasks)) = tasks {
            let _ = stop.send(true);
            for task in tasks {
                let _ = task.await;
            }
        }
    }

    /// Stop every group, the root last, then the peer listener. Every
    /// group's duties hear the stop first, together, so the host waits
    /// for the slowest (a leader's bounded last backup archive) rather
    /// than their sum.
    pub async fn shutdown(self) -> Result<(), String> {
        self.stop_following().await;
        let _changing = self.changing.lock().await;
        let mut groups =
            std::mem::take(&mut *self.groups.write().unwrap_or_else(PoisonError::into_inner));
        for hosted in groups.values() {
            let _ = hosted.stop.send(true);
        }
        let root = groups.remove(ROOT);
        let mut outcome = Ok(());
        for (_, hosted) in groups {
            if let Err(error) = self.stop_group(hosted).await {
                outcome = Err(error.to_string());
            }
        }
        if let Some(root) = root
            && let Err(error) = self.stop_group(root).await
        {
            outcome = Err(error.to_string());
        }
        if let Some(listener) = self
            .listener
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
        {
            listener.abort();
        }
        outcome
    }
}

/// Wait for every other holder of a group's node to let go, up to
/// [`CLOSE_WAIT`].
async fn unwrap_node(mut node: Arc<Node>) -> Result<Node, Arc<Node>> {
    let deadline = tokio::time::Instant::now() + CLOSE_WAIT;
    loop {
        match Arc::try_unwrap(node) {
            Ok(node) => return Ok(node),
            Err(shared) if tokio::time::Instant::now() >= deadline => return Err(shared),
            Err(shared) => {
                node = shared;
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }
}

/// Read a group directory's `group.id`, if it has one.
fn read_group_id(dir: &Path) -> Result<Option<GroupId>, NodeError> {
    let text = match std::fs::read_to_string(dir.join(GROUP_ID_FILE)) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(NodeError::coded("io", error)),
    };
    let corrupt = || {
        NodeError::coded(
            "group",
            format!(
                "{} is not `<name> <uid>`",
                dir.join(GROUP_ID_FILE).display()
            ),
        )
    };
    let (name, uid) = text.trim().split_once(' ').ok_or_else(corrupt)?;
    let uid = u64::from_str_radix(uid, 16).map_err(|_| corrupt())?;
    GroupId::new(name, uid)
        .map(Some)
        .map_err(|message| NodeError::coded("group", message))
}

/// Make `dir` the home of `group`: record its identity in a fresh
/// directory before any file of the group exists, or check the one an
/// earlier open recorded. A directory holding group files without an
/// identity, or another group's, is refused.
fn claim_group_dir(dir: &Path, group: &GroupId) -> Result<(), NodeError> {
    let io = |error: std::io::Error| NodeError::coded("io", error);
    match read_group_id(dir)? {
        Some(recorded) if recorded == *group => return Ok(()),
        Some(recorded) => {
            return Err(NodeError::coded(
                "group",
                format!(
                    "{} holds group {recorded}, not {group}: a removed group's files never join \
                     a new one",
                    dir.display()
                ),
            ));
        }
        None => {}
    }
    if dir.exists() && std::fs::read_dir(dir).map_err(io)?.next().is_some() {
        return Err(NodeError::coded(
            "group",
            format!("{} holds files but no {GROUP_ID_FILE}", dir.display()),
        ));
    }
    std::fs::create_dir_all(dir).map_err(io)?;
    let path = dir.join(GROUP_ID_FILE);
    let part = dir.join(format!("{GROUP_ID_FILE}.part"));
    {
        use std::io::Write as _;
        let mut file = std::fs::File::create(&part).map_err(io)?;
        writeln!(file, "{} {:016x}", group.name, group.uid).map_err(io)?;
        file.sync_all().map_err(io)?;
    }
    std::fs::rename(&part, &path).map_err(io)?;
    Ok(())
}
