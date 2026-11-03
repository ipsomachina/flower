//! Deferred flushes (`flower_cluster::flush`): a leader that replicates
//! before its own log flush completes and defers that flush while its
//! followers commit, and applies made durable in batches, replayed from
//! the log after a crash.
//!
//! The disks here lose every write since their last sync when a
//! member's power is cut, so the tests crash members at each point the
//! deferral opens (the leader's unflushed tail, applies the group file
//! has not synced, a durable group commit while the leader's log is
//! unflushed, a snapshot and log purge over deferred applies) and check
//! that no acknowledged write is lost and that no applied state survives
//! ahead of its log.

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock, RwLockReadGuard};
use std::time::{Duration, Instant};

use flower_cluster::flush::{EAGER_WINDOW, FlushSettings, LeaderFlush};
use flower_cluster::logstore::LogStore;
use flower_cluster::network::{InProcessFactory, Registry};
use flower_cluster::node::{FileOpener, Node, NodeError};
use flower_cluster::statemachine::{RuntimeFactory, StateMachine};
use flower_cluster::timing::{RaftTiming, SnapshotSettings, TimingSettings, raft_config};
use flower_cluster::types::{ClusterWrite, NodeId};
use flower_engine::engine::Engine;
use flower_engine::runtime::Runtime;
use flower_engine::storage::GroupStore;
use flower_sdk::Value;
use redb::StorageBackend as _;
use redb::backends::FileBackend;

// ------------------------------------------------------------- disks

/// A file's writes since its last sync, to undo when the power goes.
#[derive(Default)]
struct Unsynced {
    /// The file's length at its last sync, once anything changed since.
    synced_len: Option<u64>,
    /// What each write since overwrote, oldest first.
    overwritten: Vec<(u64, Vec<u8>)>,
}

/// One file of a member's disk.
struct Disk {
    file: FileBackend,
    name: String,
    unsynced: Mutex<Unsynced>,
    syncs: AtomicU64,
    /// While true, syncs wait.
    held: Mutex<bool>,
    released: Condvar,
}

impl Disk {
    /// Keep what `[offset, offset + len)` holds now, before a write
    /// changes it.
    fn keep(&self, unsynced: &mut Unsynced, offset: u64, len: u64) -> std::io::Result<()> {
        let current = self.file.len()?;
        if unsynced.synced_len.is_none() {
            unsynced.synced_len = Some(current);
        }
        if offset < current {
            let mut old = vec![0; (current - offset).min(len) as usize];
            self.file.read(offset, &mut old)?;
            unsynced.overwritten.push((offset, old));
        }
        Ok(())
    }

    /// Lose every write since the last sync.
    fn lose_unsynced(&self) {
        let mut unsynced = self.unsynced.lock().unwrap();
        let Some(len) = unsynced.synced_len.take() else {
            return;
        };
        if self.file.len().unwrap() < len {
            self.file.set_len(len).unwrap();
        }
        for (offset, old) in unsynced.overwritten.drain(..).rev() {
            self.file.write(offset, &old).unwrap();
        }
        self.file.set_len(len).unwrap();
    }

    fn hold_syncs(&self, held: bool) {
        *self.held.lock().unwrap() = held;
        self.released.notify_all();
    }
}

/// A member's power: cutting it fails every later I/O of its disks at
/// once and takes each of them back to its last sync.
#[derive(Clone, Default)]
struct Power(Arc<PowerState>);

#[derive(Default)]
struct PowerState {
    cut: RwLock<bool>,
    disks: Mutex<Vec<Arc<Disk>>>,
}

impl Power {
    /// Shared while one I/O runs: a cut waits for it.
    fn on(&self) -> std::io::Result<RwLockReadGuard<'_, bool>> {
        let cut = self.0.cut.read().unwrap();
        if *cut {
            return Err(std::io::Error::other("the power is off"));
        }
        Ok(cut)
    }

    fn cut(&self) {
        let mut cut = self.0.cut.write().unwrap();
        *cut = true;
        for disk in self.0.disks.lock().unwrap().drain(..) {
            disk.lose_unsynced();
        }
    }

    /// The disk of the file named `name` (`group.redb`, `raft.redb`).
    fn disk(&self, name: &str) -> Arc<Disk> {
        self.0
            .disks
            .lock()
            .unwrap()
            .iter()
            .find(|disk| disk.name == name)
            .cloned()
            .unwrap()
    }

    /// Open member files over this power's disks.
    fn files(&self) -> FileOpener {
        let power = self.clone();
        Arc::new(move |path: &Path| {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(path)
                .map_err(|error| redb::DatabaseError::Storage(redb::StorageError::Io(error)))?;
            let disk = Arc::new(Disk {
                file: FileBackend::new(file)?,
                name: path.file_name().unwrap().to_string_lossy().into_owned(),
                unsynced: Mutex::default(),
                syncs: AtomicU64::new(0),
                held: Mutex::new(false),
                released: Condvar::new(),
            });
            power.0.disks.lock().unwrap().push(disk.clone());
            redb::Builder::new().create_with_backend(PowerLossDisk {
                disk,
                power: power.clone(),
            })
        })
    }
}

/// The redb backend over a [`Disk`] behind a [`Power`].
struct PowerLossDisk {
    disk: Arc<Disk>,
    power: Power,
}

impl std::fmt::Debug for PowerLossDisk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PowerLossDisk({})", self.disk.name)
    }
}

impl redb::StorageBackend for PowerLossDisk {
    fn len(&self) -> std::io::Result<u64> {
        let _on = self.power.on()?;
        self.disk.file.len()
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> std::io::Result<()> {
        let _on = self.power.on()?;
        self.disk.file.read(offset, out)
    }

    fn set_len(&self, len: u64) -> std::io::Result<()> {
        let _on = self.power.on()?;
        let mut unsynced = self.disk.unsynced.lock().unwrap();
        let current = self.disk.file.len()?;
        self.disk
            .keep(&mut unsynced, len, current.saturating_sub(len))?;
        self.disk.file.set_len(len)
    }

    fn sync_data(&self) -> std::io::Result<()> {
        {
            let mut held = self.disk.held.lock().unwrap();
            while *held {
                held = self.disk.released.wait(held).unwrap();
            }
        }
        let _on = self.power.on()?;
        let mut unsynced = self.disk.unsynced.lock().unwrap();
        self.disk.file.sync_data()?;
        *unsynced = Unsynced::default();
        self.disk.syncs.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn write(&self, offset: u64, data: &[u8]) -> std::io::Result<()> {
        let _on = self.power.on()?;
        let mut unsynced = self.disk.unsynced.lock().unwrap();
        self.disk.keep(&mut unsynced, offset, data.len() as u64)?;
        self.disk.file.write(offset, data)
    }

    fn close(&self) -> std::io::Result<()> {
        self.disk.file.close()
    }
}

/// What a member's files hold, read straight from them.
#[derive(Debug)]
struct OnDisk {
    /// The group file's applied position.
    applied: u64,
    /// The expiry clock: the largest stamp applied.
    clock: u64,
    /// The log's last index, its purge point when empty.
    last: u64,
    /// The log's purge point.
    purged: u64,
}

impl OnDisk {
    fn read(dir: &Path) -> Self {
        let engine = Engine::new(GroupStore::open(dir.join("group.redb")).unwrap());
        let applied = StateMachine::group_applied(&engine)
            .unwrap()
            .map_or(0, |id| id.index);
        let clock = StateMachine::expiry_clock(&engine).unwrap();
        drop(engine);
        let logs = LogStore::open(&dir.join("raft.redb")).unwrap();
        let last = logs.last_log_index().unwrap().unwrap_or(0);
        let purged = logs.last_purged().unwrap().map_or(0, |id| id.index);
        OnDisk {
            applied,
            clock,
            last,
            purged,
        }
    }

    /// No applied state ahead of its log, and none behind its purge
    /// point: OpenRaft can start from these files.
    fn consistent(&self) {
        assert!(
            self.applied <= self.last,
            "the group file applied past its log: {self:?}"
        );
        assert!(
            self.applied >= self.purged,
            "the log purged entries the group file lacks: {self:?}"
        );
    }
}

// ------------------------------------------------------------ members

/// These members run no modules: their writes are incarnation entries,
/// each advancing the expiry clock to its stamp.
struct NoRuntimes;

impl RuntimeFactory for NoRuntimes {
    fn build(
        &self,
        _db: &[u8],
        module: &str,
        _: &[u8],
        _: &Value,
    ) -> Result<Box<dyn Runtime>, String> {
        Err(format!("no runtime for `{module}` in this test"))
    }
}

fn config(snapshots: &SnapshotSettings) -> Arc<openraft::Config> {
    let timing = RaftTiming::derive(&TimingSettings::default()).unwrap();
    Arc::new(raft_config(&timing, snapshots).unwrap())
}

/// A lazy leader that never flushes on its own within a test.
fn never_flushing() -> LeaderFlush {
    LeaderFlush {
        lazy: true,
        grace: Duration::from_secs(3600),
        interval: Duration::from_secs(3600),
    }
}

/// Deferred applies the test flushes itself.
const NO_APPLY_FLUSH: Option<Duration> = Some(Duration::from_secs(3600));

/// An in-process group of members over power-loss disks.
struct Members {
    dir: tempfile::TempDir,
    registry: Registry,
    config: Arc<openraft::Config>,
    nodes: Vec<Option<Node>>,
    power: Vec<Power>,
    flush: FlushSettings,
}

impl Members {
    async fn start(count: u64, config: Arc<openraft::Config>, flush: FlushSettings) -> Self {
        let mut members = Members {
            dir: tempfile::tempdir().unwrap(),
            registry: Registry::new(),
            config,
            nodes: (0..count).map(|_| None).collect(),
            power: (0..count).map(|_| Power::default()).collect(),
            flush,
        };
        for id in 1..=count {
            members.open(id).await;
        }
        members
            .node(1)
            .initialize((1..=count).collect::<BTreeSet<_>>())
            .await
            .unwrap();
        members.leader().await;
        // A new leader flushes every append at once for its first second
        // (`LeaderFlusher::leading`): the tests watch what comes after.
        tokio::time::sleep(EAGER_WINDOW + Duration::from_millis(100)).await;
        members
    }

    fn path(&self, id: NodeId) -> std::path::PathBuf {
        self.dir.path().join(format!("n{id}"))
    }

    /// Open member `id` over fresh disks, as a restart after a cut.
    async fn open(&mut self, id: NodeId) {
        let power = Power::default();
        let node = Node::open_with(
            id,
            &self.path(id),
            self.config.clone(),
            InProcessFactory::new(self.registry.clone()).with_source(id),
            Arc::new(NoRuntimes),
            power.files(),
        )
        .await
        .unwrap()
        .with_flush(self.flush);
        self.registry.register_peer(node.peer_service());
        self.power[id as usize - 1] = power;
        self.nodes[id as usize - 1] = Some(node);
    }

    fn node(&self, id: NodeId) -> &Node {
        self.nodes[id as usize - 1].as_ref().unwrap()
    }

    fn live(&self) -> Vec<&Node> {
        self.nodes.iter().flatten().collect()
    }

    /// The leader every live member agrees on.
    async fn leader(&self) -> NodeId {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let leaders: BTreeSet<Option<NodeId>> = self
                .live()
                .iter()
                .map(|node| node.status().leader)
                .collect();
            if let [Some(leader)] = leaders.into_iter().collect::<Vec<_>>()[..]
                && self.nodes[leader as usize - 1].is_some()
            {
                return leader;
            }
            assert!(Instant::now() < deadline, "no agreed leader");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Cut member `id` off the network and its power at once: every
    /// write since its disks' last syncs is lost.
    async fn cut(&mut self, id: NodeId) {
        for other in 1..=self.nodes.len() as NodeId {
            if other != id {
                self.registry.block(id, other);
                self.registry.block(other, id);
            }
        }
        self.registry.unregister(id);
        self.power[id as usize - 1].cut();
        let node = self.nodes[id as usize - 1].take().unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(20), node.shutdown()).await;
    }

    /// Stop member `id` cleanly.
    async fn stop(&mut self, id: NodeId) {
        self.registry.unregister(id);
        let node = self.nodes[id as usize - 1].take().unwrap();
        node.shutdown().await.unwrap();
    }

    /// Restart every member that is down, over its files.
    async fn restart(&mut self) {
        self.registry.heal();
        for id in 1..=self.nodes.len() as NodeId {
            if self.nodes[id as usize - 1].is_none() {
                self.open(id).await;
            }
        }
    }

    async fn stop_all(mut self) {
        for id in 1..=self.nodes.len() as NodeId {
            if self.nodes[id as usize - 1].is_some() {
                self.stop(id).await;
            }
        }
    }
}

/// Propose an entry stamped `stamp` through `node`: its log index once
/// the group committed and `node` applied it.
async fn write(node: &Node, stamp: u64) -> Result<u64, String> {
    let written = tokio::time::timeout(
        Duration::from_secs(10),
        node.raft().client_write(ClusterWrite::Incarnation {
            incarnation: 7,
            proposed_ms: stamp,
        }),
    )
    .await
    .map_err(|_| "timed out".to_string())?
    .map_err(|error| error.to_string())?;
    Ok(written.log_id.index)
}

/// Write through whichever member leads, retrying while leadership
/// settles after a restart: its log index.
async fn write_to_leader(members: &Members, stamp: u64) -> u64 {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let leader = members.leader().await;
        match write(members.node(leader), stamp).await {
            Ok(index) => return index,
            Err(error) => assert!(
                Instant::now() < deadline,
                "no write after the restart: {error}"
            ),
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn failed(error: impl std::fmt::Display) -> NodeError {
    NodeError::Failure {
        code: "test".to_string(),
        message: error.to_string(),
    }
}

async fn clock(node: &Node) -> u64 {
    node.with_engine(|engine| {
        StateMachine::expiry_clock(engine).map_err(|failure| failed(failure.message))
    })
    .await
    .unwrap()
}

/// Wait until `node` serves: its recovery fence, if any, cleared.
async fn recovered(node: &Node) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while let Some(reason) = node.recovering() {
        assert!(Instant::now() < deadline, "still recovering: {reason}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn applied_through(node: &Node, index: u64) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while node.status().last_applied.unwrap_or(0) < index {
        assert!(
            Instant::now() < deadline,
            "member {} never applied {index}: {:?}",
            node.id(),
            node.status()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

// ----------------------------------------------- the leader's flush

/// The leader replicates an append while its own flush of it has not
/// returned, and the followers commit it: with its Raft disk's syncs
/// held, a write still commits. Upstream OpenRaft awaited the leader's
/// flush before replicating, so the write would wait for the held sync.
#[tokio::test(flavor = "multi_thread")]
async fn a_leader_replicates_before_its_own_flush_completes() {
    let flush = FlushSettings {
        leader: LeaderFlush {
            lazy: false,
            ..LeaderFlush::default()
        },
        apply_every: NO_APPLY_FLUSH,
    };
    let members = Members::start(3, config(&SnapshotSettings::default()), flush).await;
    let leader = members.leader().await;
    let lead = members.node(leader);
    write(lead, 1).await.unwrap();
    let disk = members.power[leader as usize - 1].disk("raft.redb");
    let synced = disk.syncs.load(Ordering::SeqCst);
    disk.hold_syncs(true);
    let index = write(lead, 2).await;
    disk.hold_syncs(false);
    let index = index.expect("the followers commit what the leader has not flushed");
    assert!(
        disk.syncs.load(Ordering::SeqCst) >= synced,
        "the leader flushed in parallel"
    );
    let stats = lead.logs().leader_flush_stats();
    assert!(stats.deferred >= 2, "{stats:?}");
    for node in members.live() {
        applied_through(node, index).await;
    }
    members.stop_all().await;
}

/// While its followers commit, a lazy leader flushes its log only at the
/// interval: appends far outnumber flushes. Once a follower is down the
/// followers alone no longer commit, an append outlives the grace, the
/// leader flushes and then flushes at once while it needs its own vote.
#[tokio::test(flavor = "multi_thread")]
async fn a_lazy_leader_flushes_only_when_its_vote_is_needed() {
    let flush = FlushSettings {
        leader: LeaderFlush {
            lazy: true,
            // Long enough that a busy machine doesn't run it out.
            grace: Duration::from_secs(2),
            interval: Duration::from_millis(500),
        },
        apply_every: NO_APPLY_FLUSH,
    };
    let mut members = Members::start(3, config(&SnapshotSettings::default()), flush).await;
    let leader = members.leader().await;
    let started = Instant::now();
    let before = members.node(leader).logs().leader_flush_stats();
    for stamp in 0..60 {
        write(members.node(leader), stamp).await.unwrap();
    }
    let stats = members.node(leader).logs().leader_flush_stats();
    let flushes = stats.flushes - before.flushes;
    let allowed = (started.elapsed().as_millis() / 500) as u64 + 2;
    assert!(stats.deferred - before.deferred >= 60, "{stats:?}");
    assert!(
        flushes <= allowed,
        "{flushes} flushes > {allowed}: {stats:?}"
    );
    assert_eq!(stats.grace_flushes, 0, "{stats:?}");

    // A follower goes down: each commit needs the leader's own flush.
    let follower = (1..=3).find(|id| *id != leader).unwrap();
    members.stop(follower).await;
    let lead = members.node(leader);
    lead.logs().flush().unwrap();
    lead.logs().set_leader_flush(LeaderFlush {
        grace: Duration::from_millis(30),
        ..flush.leader
    });
    let started = Instant::now();
    write(lead, 100).await.unwrap();
    let first = started.elapsed();
    let stats = lead.logs().leader_flush_stats();
    assert!(stats.grace_flushes >= 1, "{stats:?}");
    assert!(first >= Duration::from_millis(30), "{first:?}");
    // Eager now: the next writes don't wait out the grace.
    let started = Instant::now();
    for stamp in 101..111 {
        write(lead, stamp).await.unwrap();
    }
    let grace_flushes = lead.logs().leader_flush_stats().grace_flushes;
    assert!(
        grace_flushes <= stats.grace_flushes + 1,
        "eager writes waited for the grace: {grace_flushes} grace flushes in {:?}",
        started.elapsed()
    );
    members.stop_all().await;
}

/// A lazy leader defers its flush only while its followers alone form a
/// majority of every voter configuration: with two voters its own vote
/// is needed for every commit, so it flushes each append at once, in
/// parallel with replication, never waiting for a grace or an interval.
#[tokio::test(flavor = "multi_thread")]
async fn a_leader_of_two_voters_flushes_each_append_at_once() {
    let flush = FlushSettings {
        leader: never_flushing(),
        apply_every: NO_APPLY_FLUSH,
    };
    let members = Members::start(2, config(&SnapshotSettings::default()), flush).await;
    let leader = members.leader().await;
    let lead = members.node(leader);
    let before = lead.logs().leader_flush_stats();
    for stamp in 1..=10 {
        write(lead, stamp)
            .await
            .expect("two voters commit without waiting for a lazy flush");
    }
    let stats = lead.logs().leader_flush_stats();
    assert_eq!(stats.deferred, before.deferred + 10, "{stats:?}");
    assert!(stats.flushes >= before.flushes + 10, "{stats:?}");
    assert_eq!(stats.grace_flushes, 0, "{stats:?}");
    members.stop_all().await;
}

/// A leader elected after another went down may need its own vote for
/// every commit: it flushes at once for its first second, so the
/// failover pays no grace. With a lazy leader that otherwise never
/// flushes, the new leader of the two members left still commits.
#[tokio::test(flavor = "multi_thread")]
async fn a_new_leader_flushes_at_once_so_a_failover_waits_for_no_grace() {
    let flush = FlushSettings {
        leader: never_flushing(),
        apply_every: NO_APPLY_FLUSH,
    };
    let mut members = Members::start(3, config(&SnapshotSettings::default()), flush).await;
    let old = members.leader().await;
    members.cut(old).await;
    let leader = members.leader().await;
    assert_ne!(leader, old);
    let started = Instant::now();
    write(members.node(leader), 1)
        .await
        .expect("the new leader commits with its own flush");
    let took = started.elapsed();
    let stats = members.node(leader).logs().leader_flush_stats();
    assert!(stats.flushes >= 1, "{stats:?}");
    assert_eq!(stats.grace_flushes, 0, "{stats:?}");
    assert!(took < Duration::from_secs(5), "{took:?}");
    members.stop_all().await;
}

/// A leader counts toward the commit quorum only for appends it has
/// flushed: with a follower down and the leader holding its flush, a
/// write does not commit on the remaining follower alone; it commits
/// once the leader flushes.
#[tokio::test(flavor = "multi_thread")]
async fn the_leader_counts_itself_only_for_appends_it_flushed() {
    let flush = FlushSettings {
        leader: never_flushing(),
        apply_every: NO_APPLY_FLUSH,
    };
    let mut members = Members::start(3, config(&SnapshotSettings::default()), flush).await;
    let leader = members.leader().await;
    let follower = (1..=3).find(|id| *id != leader).unwrap();
    members.stop(follower).await;
    let lead = members.node(leader);
    let pending = tokio::spawn({
        let raft = lead.raft().clone();
        async move {
            raft.client_write(ClusterWrite::Incarnation {
                incarnation: 7,
                proposed_ms: 1,
            })
            .await
        }
    });
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(
        !pending.is_finished(),
        "a write committed on one follower and an unflushed leader"
    );
    lead.logs().flush().unwrap();
    let written = tokio::time::timeout(Duration::from_secs(10), pending)
        .await
        .expect("the write commits once the leader flushed")
        .unwrap()
        .unwrap();
    applied_through(lead, written.log_id.index).await;
    members.stop_all().await;
}

// ---------------------------------------------------------- crashes

/// The whole group loses power while the leader's log tail is unflushed
/// and no member's group file has synced an apply: every acknowledged
/// write is still there once the group restarts, the leader having lost
/// its tail like a member that never received it, and no file held
/// applied state past its log.
#[tokio::test(flavor = "multi_thread")]
async fn a_power_cut_of_every_member_loses_no_acknowledged_write() {
    let flush = FlushSettings {
        leader: never_flushing(),
        apply_every: NO_APPLY_FLUSH,
    };
    let mut members = Members::start(3, config(&SnapshotSettings::default()), flush).await;
    let leader = members.leader().await;
    // Every flush so far has happened: from here on, none.
    for node in members.live() {
        node.logs().flush().unwrap();
        node.flush_applies().await.unwrap();
    }
    let before = members.node(leader).logs().leader_flush_stats();
    let mut acknowledged = 0;
    for stamp in 1..=40 {
        acknowledged = write(members.node(leader), 1000 + stamp).await.unwrap();
    }
    let stats = members.node(leader).logs().leader_flush_stats();
    assert_eq!(stats.flushes, before.flushes, "{stats:?}");
    assert_eq!(stats.deferred, before.deferred + 40, "{stats:?}");
    for id in 1..=3 {
        members.cut(id).await;
    }
    let mut leader_lost_its_tail = false;
    for id in 1..=3 {
        let on_disk = OnDisk::read(&members.path(id));
        on_disk.consistent();
        assert!(
            on_disk.clock < 1040,
            "the group file synced a deferred apply: {on_disk:?}"
        );
        if id == leader {
            leader_lost_its_tail = on_disk.last < acknowledged;
        } else {
            assert!(
                on_disk.last >= acknowledged,
                "a follower lost an entry it acknowledged: {on_disk:?}"
            );
        }
    }
    assert!(
        leader_lost_its_tail,
        "the cut came after the leader's flush"
    );

    members.restart().await;
    let index = write_to_leader(&members, 2000).await;
    assert!(
        index > acknowledged,
        "{index} <= {acknowledged}: {:?}",
        members
            .live()
            .iter()
            .map(|node| node.status())
            .collect::<Vec<_>>()
    );
    for node in members.live() {
        applied_through(node, index).await;
        assert_eq!(clock(node).await, 2000);
        recovered(node).await;
    }
    members.stop_all().await;
}

/// A durable commit of the group file makes the leader's log durable
/// first: the leader's applies outran its unflushed log, then the group
/// file synced twice (a flush of the applies, then an unrelated durable
/// transaction) before the power went. The group file never holds an
/// entry its log lost.
#[tokio::test(flavor = "multi_thread")]
async fn durable_group_commits_flush_the_leaders_log_first() {
    let flush = FlushSettings {
        leader: never_flushing(),
        apply_every: NO_APPLY_FLUSH,
    };
    let mut members = Members::start(3, config(&SnapshotSettings::default()), flush).await;
    let leader = members.leader().await;
    let mut acknowledged = 0;
    for stamp in 1..=20 {
        acknowledged = write(members.node(leader), stamp).await.unwrap();
    }
    let lead = members.node(leader);
    applied_through(lead, acknowledged).await;
    lead.flush_applies().await.unwrap();
    for stamp in 21..=30 {
        acknowledged = write(lead, stamp).await.unwrap();
    }
    applied_through(lead, acknowledged).await;
    // Maintenance-like work commits durably, deferring nothing.
    lead.with_engine(|engine| {
        let write = engine.store().begin_write().map_err(failed)?;
        write.commit().map_err(failed)
    })
    .await
    .unwrap();
    members.cut(leader).await;
    let on_disk = OnDisk::read(&members.path(leader));
    on_disk.consistent();
    assert_eq!(on_disk.applied, acknowledged, "{on_disk:?}");
    assert_eq!(on_disk.clock, 30, "{on_disk:?}");

    members.restart().await;
    let index = write_to_leader(&members, 31).await;
    for node in members.live() {
        applied_through(node, index).await;
        assert_eq!(clock(node).await, 31);
    }
    members.stop_all().await;
}

/// Snapshots purge the log: a member makes its deferred applies durable
/// before it publishes one, so a crash never leaves its state behind a
/// purged log.
#[tokio::test(flavor = "multi_thread")]
async fn a_snapshot_makes_deferred_applies_durable_before_the_log_purges() {
    let snapshots = SnapshotSettings {
        logs_since_last: 20,
        keep_logs: 0,
        ..SnapshotSettings::default()
    };
    let flush = FlushSettings {
        leader: LeaderFlush::default(),
        apply_every: NO_APPLY_FLUSH,
    };
    let mut members = Members::start(1, config(&snapshots), flush).await;
    let node = members.node(1);
    let mut acknowledged = 0;
    for stamp in 1..=50 {
        acknowledged = write(node, stamp).await.unwrap();
    }
    let deadline = Instant::now() + Duration::from_secs(60);
    while node.logs().last_purged().unwrap().map_or(0, |id| id.index) < 20 {
        assert!(
            Instant::now() < deadline,
            "the log never purged: snapshot {:?}, {:?}",
            node.logs()
                .snapshot_meta()
                .unwrap()
                .map(|meta| meta.last_log_id),
            node.status()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    members.cut(1).await;
    let on_disk = OnDisk::read(&members.path(1));
    on_disk.consistent();
    assert!(on_disk.purged >= 20, "{on_disk:?}");
    assert!(on_disk.last >= acknowledged, "{on_disk:?}");

    members.restart().await;
    let node = members.node(1);
    applied_through(node, acknowledged).await;
    assert_eq!(clock(node).await, 50);
    members.stop_all().await;
}

/// Applies commit to the group file without an fsync, made durable by
/// the periodic flush: a member's writes cost its group file no sync of
/// their own. A crash loses the deferred applies, and the member
/// re-applies them from its log before it serves.
#[tokio::test(flavor = "multi_thread")]
async fn deferred_applies_sync_nothing_and_replay_from_the_log_after_a_crash() {
    let mut members = Members::start(
        1,
        config(&SnapshotSettings::default()),
        FlushSettings {
            leader: LeaderFlush::default(),
            apply_every: NO_APPLY_FLUSH,
        },
    )
    .await;
    let node = members.node(1);
    write(node, 1).await.unwrap();
    node.flush_applies().await.unwrap();
    let group = members.power[0].disk("group.redb");
    let synced = group.syncs.load(Ordering::SeqCst);
    let mut acknowledged = 0;
    for stamp in 2..=41 {
        acknowledged = write(node, stamp).await.unwrap();
    }
    assert_eq!(
        group.syncs.load(Ordering::SeqCst),
        synced,
        "forty applies synced the group file"
    );
    assert!(node.apply_durability().pending());

    // The same writes with durable applies: one sync each.
    node.apply_durability().set(None);
    for stamp in 42..=51 {
        acknowledged = write(node, stamp).await.unwrap();
    }
    assert!(group.syncs.load(Ordering::SeqCst) >= synced + 10);
    node.apply_durability().set(NO_APPLY_FLUSH);
    for stamp in 52..=61 {
        acknowledged = write(node, stamp).await.unwrap();
    }

    members.cut(1).await;
    let on_disk = OnDisk::read(&members.path(1));
    on_disk.consistent();
    assert_eq!(on_disk.clock, 51, "{on_disk:?}");
    assert!(on_disk.last >= acknowledged, "{on_disk:?}");

    members.restart().await;
    let node = members.node(1);
    applied_through(node, acknowledged).await;
    assert_eq!(clock(node).await, 61);
    members.stop_all().await;
}
