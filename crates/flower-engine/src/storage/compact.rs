//! Online compaction of a redb file under open readers.
//!
//! redb is copy-on-write and reuses a freed page only once no reader
//! holds it, and grows its file (by doubling) when no free page is
//! usable, so under load live pages end up near the end of a file that
//! is mostly free inside, and a commit gives back only free pages at the
//! end. Upstream `Database::compact` moves pages down but needs exclusive
//! access, and Flower always holds read snapshots. The vendored fork
//! (`vendor/redb/FLOWER.md`) adds bounded relocation inside an ordinary
//! write transaction, which [`Compactor::step`] drives.
//!
//! Once the file up to its last allocated page (its occupied extent) is
//! at least [`CompactionSettings::ratio`] times its allocated pages, each
//! step moves [`CompactionSettings::step_pages`] of the pages nearest the
//! end down, in its own durable transaction, and its commit gives back
//! the free tail. A run stops once the extent is within a quarter of the
//! allocated pages, no page can move lower, or [`STALL_STEPS`] steps
//! haven't lowered the extent; a final commit then gives back what
//! readers let go of meanwhile. A moved page stays pinned while
//! a reader can see it, so bounded steps never mean immediate reclamation.
//! Free pages at the end of the file, as a doubling leaves, don't count:
//! redb fills them first, and giving them back would only grow the file
//! again. A step never grows the file.
//!
//! Steps take redb's write lock like any writer, between other writes,
//! and never a lock of the store's users: callers run them on a thread of
//! their own and pace them with [`CompactionSettings::pause_after`].

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use super::{DurableHook, storage};
use crate::error::{Error, Result};

/// When and how fast a file compacts online.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CompactionSettings {
    /// Start once the occupied extent is at least this many times the
    /// allocated pages; zero disables online compaction.
    pub ratio: u64,
    /// Most pages one step moves.
    pub step_pages: usize,
    /// Files whose occupied extent is below this are left alone.
    pub min_file_bytes: u64,
    /// How often an idle compactor measures the file.
    pub check: Duration,
    /// Share of the time a running compaction spends in steps, 1-100.
    pub duty_percent: u32,
}

impl Default for CompactionSettings {
    fn default() -> Self {
        CompactionSettings {
            ratio: 2,
            step_pages: 256,
            min_file_bytes: 16 << 20,
            check: Duration::from_secs(10),
            // A fifth: between steps, the compactor waits four times as
            // long as the last one took.
            duty_percent: 20,
        }
    }
}

impl CompactionSettings {
    /// Longest idle check interval: a day.
    pub const MAX_CHECK: Duration = Duration::from_secs(24 * 60 * 60);

    /// Whether online compaction runs at all.
    pub fn enabled(&self) -> bool {
        self.ratio != 0
    }

    /// Reject settings that can't run: a ratio of one (every file is
    /// due), empty steps, a check interval outside 1 ms to a day, or a
    /// duty share outside 1-100.
    pub fn validate(&self) -> std::result::Result<(), String> {
        if self.ratio == 1 {
            return Err("the compaction ratio must be 0 (off) or at least 2".to_string());
        }
        if self.step_pages == 0 {
            return Err("compaction steps must move at least one page".to_string());
        }
        if self.check < Duration::from_millis(1) || self.check > Self::MAX_CHECK {
            return Err("the compaction check interval must be from 1 ms to a day".to_string());
        }
        if !(1..=100).contains(&self.duty_percent) {
            return Err("the compaction duty must be a percentage from 1 to 100".to_string());
        }
        Ok(())
    }

    /// How long to wait after a step that took `took`, so steps take
    /// `duty_percent` of the time; at least 10 ms.
    pub fn pause_after(&self, took: Duration) -> Duration {
        let duty = self.duty_percent.clamp(1, 100);
        (took * (100 - duty) / duty).max(Duration::from_millis(10))
    }
}

/// What one [`Compactor::step`] did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Step {
    /// Nothing was due: measure again after the check interval.
    Idle,
    /// Moved pages down, starting a run if none was active, in this
    /// long: pace the next step after it.
    Moved(Duration),
    /// Ended a run, giving back the free tail. `reached` tells whether
    /// the extent came within a quarter of the allocated pages; a run
    /// that didn't waits before starting again.
    Finished {
        /// File length when the run's last check measured it.
        before: u64,
        /// File length after the final commit.
        after: u64,
        /// Whether the run reached its goal.
        reached: bool,
    },
}

/// What a file's compactor has measured and done, for metrics.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CompactionStats {
    /// A run is in progress.
    pub active: bool,
    /// Measurements taken.
    pub checks: u64,
    /// Steps that moved pages.
    pub steps: u64,
    /// Time spent in steps, in microseconds.
    pub step_micros: u64,
    /// Longest step, in microseconds.
    pub max_step_micros: u64,
    /// File length at the last check; `None` before the first.
    pub file_bytes: Option<u64>,
    /// Bytes through the last allocated page at the last check.
    pub occupied_bytes: Option<u64>,
    /// Bytes of allocated pages at the last check.
    pub allocated_bytes: Option<u64>,
    /// How long a run that gave up still waits before starting again.
    pub waiting_ms: Option<u64>,
}

/// The first wait after a run gives up, and the longest.
pub const GIVE_UP_FIRST: Duration = Duration::from_secs(20);
/// The longest wait after runs keep giving up.
pub const GIVE_UP_MAX: Duration = Duration::from_secs(640);
/// Steps a run may take without lowering the occupied extent before it
/// gives up. A reader held across a run pins every page freed after it
/// began, so under writes each commit allocates fresh pages that the
/// next step moves again while the pinned tail never trims: without
/// this bound such a run would step until the reader let go.
pub const STALL_STEPS: u64 = 64;

/// A run that ended because nothing could move lower, with pages still
/// stranded at the end: readers held the free pages below, or there were
/// none. Starting again at the next check would flush twice for nothing,
/// so the next run waits until the file has grown past where this one
/// left it, an eighth of its pages have been freed, or `retry_at`, which
/// doubles with every run that gives up again.
#[derive(Clone, Copy, Debug)]
struct GaveUp {
    file: u64,
    allocated: u64,
    retry_at: Instant,
    backoff: Duration,
}

/// One file's run state and counters.
#[derive(Default)]
struct State {
    active: AtomicBool,
    /// The last step moved nothing: the next one only trims.
    finishing: AtomicBool,
    gave_up: Mutex<Option<GaveUp>>,
    /// The lowest occupied extent this run has reached, and the steps
    /// taken since it last went lower.
    run_lowest: AtomicU64,
    run_stalled: AtomicU64,
    checks: AtomicU64,
    steps: AtomicU64,
    step_micros: AtomicU64,
    max_step_micros: AtomicU64,
    file_bytes: AtomicU64,
    occupied_bytes: AtomicU64,
    allocated_bytes: AtomicU64,
}

impl State {
    fn gave_up(&self) -> Option<GaveUp> {
        *self.gave_up.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn set_gave_up(&self, gave_up: Option<GaveUp>) {
        *self.gave_up.lock().unwrap_or_else(PoisonError::into_inner) = gave_up;
    }
}

/// Compacts one redb file online. Cloning shares the file handle and the
/// run state.
#[derive(Clone)]
pub struct Compactor {
    inner: Arc<Inner>,
}

struct Inner {
    db: Arc<redb::Database>,
    path: PathBuf,
    state: State,
    /// Runs before each step's durable commit: a group file's
    /// [`super::BeforeDurable`] hook.
    hook: Arc<DurableHook>,
}

/// A file's length, as an I/O failure when it can't be read.
fn file_len(path: &Path) -> Result<u64> {
    std::fs::metadata(path)
        .map(|metadata| metadata.len())
        .map_err(|error| Error::Io(format!("measure {}: {error}", path.display())))
}

impl Compactor {
    /// A compactor for `db`, whose file is at `path`.
    pub fn new(db: Arc<redb::Database>, path: &Path) -> Self {
        Self::hooked(db, path, Arc::default())
    }

    /// A compactor running `hook` before each durable commit: a step's
    /// commit makes the file's deferred commits durable like any other.
    pub(crate) fn hooked(db: Arc<redb::Database>, path: &Path, hook: Arc<DurableHook>) -> Self {
        Compactor {
            inner: Arc::new(Inner {
                db,
                path: path.to_path_buf(),
                state: State::default(),
                hook,
            }),
        }
    }

    /// The file's path.
    pub fn path(&self) -> &Path {
        &self.inner.path
    }

    /// Measure the file and, when a run is due or active, take one step:
    /// move up to `step_pages` pages down, or end the run. Blocking: it
    /// waits for redb's write lock and flushes. Errors leave the file as
    /// it was; [`Error::Io`] means the disk failed.
    pub fn step(&self, settings: &CompactionSettings) -> Result<Step> {
        let state = &self.inner.state;
        let started = Instant::now();
        let mut transaction = self.inner.db.begin_write().map_err(storage)?;
        let allocated = transaction.allocated_bytes().map_err(storage)?;
        let occupied = transaction.occupied_bytes().map_err(storage)?;
        let file = file_len(&self.inner.path)?;
        state.checks.fetch_add(1, Ordering::AcqRel);
        state.file_bytes.store(file, Ordering::Release);
        state.occupied_bytes.store(occupied, Ordering::Release);
        state.allocated_bytes.store(allocated, Ordering::Release);
        let active = state.active.load(Ordering::Acquire);
        let due = settings.enabled()
            && occupied >= settings.min_file_bytes
            && occupied >= allocated.saturating_mul(settings.ratio);
        // Once started, keep going until the extent is close to the
        // allocated pages, so the file ends near its live size rather
        // than just under the ratio.
        let reached = occupied <= allocated.saturating_add(allocated / 4);
        let gave_up = state.gave_up();
        let waiting = gave_up.is_some_and(|gave_up| {
            occupied <= gave_up.file
                && allocated.saturating_add(gave_up.allocated / 8) > gave_up.allocated
                && Instant::now() < gave_up.retry_at
        });
        if !active && (!due || waiting) {
            transaction.abort().map_err(storage)?;
            if !due {
                state.set_gave_up(None);
            }
            return Ok(Step::Idle);
        }
        let stalled = active && {
            if occupied < state.run_lowest.load(Ordering::Acquire) {
                state.run_lowest.store(occupied, Ordering::Release);
                state.run_stalled.store(0, Ordering::Release);
                false
            } else {
                state.run_stalled.fetch_add(1, Ordering::AcqRel) + 1 >= STALL_STEPS
            }
        };
        if active && (reached || stalled || state.finishing.load(Ordering::Acquire)) {
            // Give back the free tail the steps left, now that readers
            // have let go of the pages the last one moved.
            transaction.trim_on_commit();
            self.inner.hook.run()?;
            transaction.commit().map_err(storage)?;
            state.active.store(false, Ordering::Release);
            state.finishing.store(false, Ordering::Release);
            let after = file_len(&self.inner.path)?;
            state.file_bytes.store(after, Ordering::Release);
            state.set_gave_up((!reached).then(|| {
                let backoff = gave_up.map_or(GIVE_UP_FIRST, |gave_up| {
                    (gave_up.backoff * 2).min(GIVE_UP_MAX)
                });
                GaveUp {
                    file: after,
                    allocated,
                    retry_at: Instant::now() + backoff,
                    backoff,
                }
            }));
            return Ok(Step::Finished {
                before: file,
                after,
                reached,
            });
        }
        if !active {
            state.active.store(true, Ordering::Release);
            state.run_lowest.store(occupied, Ordering::Release);
            state.run_stalled.store(0, Ordering::Release);
        }
        // Durable, as redb's default: it reuses pages a non-durable
        // commit frees only after a later durable one, so non-durable
        // steps could not move pages into the space the previous step
        // freed, and the file would not shrink.
        let moved = transaction
            .compact_some(settings.step_pages)
            .map_err(storage)?;
        self.inner.hook.run()?;
        transaction.commit().map_err(storage)?;
        let took = started.elapsed();
        let micros = u64::try_from(took.as_micros()).unwrap_or(u64::MAX);
        state.steps.fetch_add(1, Ordering::AcqRel);
        state.step_micros.fetch_add(micros, Ordering::AcqRel);
        state.max_step_micros.fetch_max(micros, Ordering::AcqRel);
        if !moved {
            // Whatever is left is held by readers, or can't go lower:
            // trim next time, and stop.
            state.finishing.store(true, Ordering::Release);
        }
        Ok(Step::Moved(took))
    }

    /// What this compactor has measured and done.
    pub fn stats(&self) -> CompactionStats {
        let state = &self.inner.state;
        let load = |counter: &AtomicU64| counter.load(Ordering::Acquire);
        let checks = load(&state.checks);
        let measured = |counter: &AtomicU64| (checks > 0).then(|| load(counter));
        let active = state.active.load(Ordering::Acquire);
        CompactionStats {
            active,
            checks,
            steps: load(&state.steps),
            step_micros: load(&state.step_micros),
            max_step_micros: load(&state.max_step_micros),
            file_bytes: measured(&state.file_bytes),
            occupied_bytes: measured(&state.occupied_bytes),
            allocated_bytes: measured(&state.allocated_bytes),
            waiting_ms: state
                .gave_up()
                .filter(|_| !active)
                .and_then(|gave_up| gave_up.retry_at.checked_duration_since(Instant::now()))
                .map(|left| u64::try_from(left.as_millis()).unwrap_or(u64::MAX).max(1)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::GroupStore;
    use redb::{ReadableDatabase, ReadableTableMetadata, TableDefinition};

    const LIVE: TableDefinition<u64, &[u8]> = TableDefinition::new("live");
    const GARBAGE: TableDefinition<u64, &[u8]> = TableDefinition::new("garbage");

    fn temp_path(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "flower-compact-{name}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir.join("group.redb")
    }

    fn fill(db: &redb::Database, table: TableDefinition<u64, &[u8]>, rows: u64, byte: u8) {
        let transaction = db.begin_write().unwrap();
        {
            let mut table = transaction.open_table(table).unwrap();
            for key in 0..rows {
                table.insert(key, [byte; 1000].as_slice()).unwrap();
            }
        }
        transaction.commit().unwrap();
    }

    fn check(transaction: &redb::ReadTransaction, rows: u64, byte: u8) {
        let table = transaction.open_table(LIVE).unwrap();
        assert_eq!(table.len().unwrap(), rows);
        for key in (0..rows).step_by(97) {
            assert_eq!(table.get(key).unwrap().unwrap().value(), [byte; 1000]);
        }
    }

    /// A file of about 50 MB whose live rows sit after 40 MB of deleted
    /// ones: mostly free inside, its live pages near the end.
    fn mostly_free(path: &Path) -> (Arc<redb::Database>, Compactor) {
        let db = Arc::new(redb::Database::create(path).unwrap());
        fill(&db, GARBAGE, 40_000, 1);
        fill(&db, LIVE, 8_000, 2);
        let transaction = db.begin_write().unwrap();
        transaction.delete_table(GARBAGE).unwrap();
        transaction.commit().unwrap();
        let compactor = Compactor::new(db.clone(), path);
        (db, compactor)
    }

    fn run_to_end(compactor: &Compactor, settings: &CompactionSettings) -> usize {
        let mut steps = 0;
        while compactor.stats().active {
            compactor.step(settings).unwrap();
            steps += 1;
            assert!(steps < 1000, "compaction ends");
        }
        steps
    }

    #[test]
    fn settings_default_validate_and_pace_steps() {
        let defaults = CompactionSettings::default();
        assert!(defaults.enabled());
        assert_eq!(defaults.validate(), Ok(()));
        assert_eq!(defaults.min_file_bytes, 16 << 20);
        // A fifth of the time: four times the step's own time between.
        assert_eq!(
            defaults.pause_after(Duration::from_millis(100)),
            Duration::from_millis(400)
        );
        let full = CompactionSettings {
            duty_percent: 100,
            ..defaults
        };
        assert_eq!(
            full.pause_after(Duration::from_secs(1)),
            Duration::from_millis(10),
            "back to back, with a floor"
        );
        let off = CompactionSettings {
            ratio: 0,
            ..defaults
        };
        assert!(!off.enabled());
        assert_eq!(off.validate(), Ok(()));
        for bad in [
            CompactionSettings {
                ratio: 1,
                ..defaults
            },
            CompactionSettings {
                step_pages: 0,
                ..defaults
            },
            CompactionSettings {
                check: Duration::ZERO,
                ..defaults
            },
            CompactionSettings {
                check: CompactionSettings::MAX_CHECK + Duration::from_millis(1),
                ..defaults
            },
            CompactionSettings {
                duty_percent: 0,
                ..defaults
            },
            CompactionSettings {
                duty_percent: 101,
                ..defaults
            },
        ] {
            assert!(bad.validate().is_err(), "{bad:?}");
        }
    }

    /// Step until a run ends, checking that no step grows the file.
    fn run_without_growing(
        compactor: &Compactor,
        settings: &CompactionSettings,
        path: &Path,
        mut between: impl FnMut(),
    ) -> Step {
        for _ in 0..1000 {
            let length = file_len(path).unwrap();
            let step = compactor.step(settings).unwrap();
            assert!(file_len(path).unwrap() <= length, "a step never grows");
            if !matches!(step, Step::Moved(_)) {
                return step;
            }
            between();
        }
        panic!("compaction ends");
    }

    #[test]
    fn a_run_beside_a_long_held_reader_never_grows_the_file() {
        let path = temp_path("long-reader");
        let (db, compactor) = mostly_free(&path);
        let settings = CompactionSettings::default();
        // A reader open across the whole run keeps seeing its snapshot,
        // and writes go on between steps.
        let reader = db.begin_read().unwrap();
        assert!(matches!(compactor.step(&settings).unwrap(), Step::Moved(_)));
        fill(&db, LIVE, 8_000, 3);
        let mut steps = 0u64;
        let end = run_without_growing(&compactor, &settings, &path, || {
            check(&reader, 8_000, 2);
            // Appends keep dirtying fresh pages, which the reader keeps
            // from reusing what earlier steps freed.
            let transaction = db.begin_write().unwrap();
            transaction
                .open_table(GARBAGE)
                .unwrap()
                .insert(steps, [4u8; 1000].as_slice())
                .unwrap();
            transaction.commit().unwrap();
            steps += 1;
        });
        // It pins the old copy of every page it can see, so the run ends
        // short of its goal, at the latest once it stalls, and waits
        // before trying again.
        assert!(
            matches!(end, Step::Finished { reached: false, .. }),
            "{end:?}"
        );
        assert!(steps <= 2 * STALL_STEPS, "{steps} steps");
        assert!(!compactor.stats().active);
        assert!(compactor.stats().waiting_ms.is_some());
        check(&reader, 8_000, 2);
        check(&db.begin_read().unwrap(), 8_000, 3);
    }

    #[test]
    fn the_file_shrinks_while_a_snapshot_is_always_open() {
        let path = temp_path("rolling");
        let (db, compactor) = mostly_free(&path);
        let settings = CompactionSettings::default();
        let before = file_len(&path).unwrap();
        assert!(before > 40 << 20, "{before}");
        // As the engine serves reads: some snapshot is open at every
        // moment, and each newer one replaces the oldest. Upstream
        // `Database::compact` refuses to run at all while one is open.
        let mut snapshots = std::collections::VecDeque::from([db.begin_read().unwrap()]);
        let mut runs = 0;
        let mut steps = 0;
        while file_len(&path).unwrap() * 3 >= before {
            let length = file_len(&path).unwrap();
            let step = compactor.step(&settings).unwrap();
            assert!(file_len(&path).unwrap() <= length, "a step never grows");
            match step {
                Step::Moved(_) => steps += 1,
                Step::Finished { .. } => runs += 1,
                Step::Idle => panic!("one run shrinks the file; no wait"),
            }
            snapshots.push_back(db.begin_read().unwrap());
            if snapshots.len() > 2 {
                let oldest = snapshots.pop_front().unwrap();
                check(&oldest, 8_000, 2);
            }
            assert!(steps + runs < 2000, "{before} → {length} bytes");
        }
        let after = file_len(&path).unwrap();
        assert!(after * 3 < before, "{before} → {after} bytes");
        for snapshot in &snapshots {
            check(snapshot, 8_000, 2);
        }
        let stats = compactor.stats();
        assert!(stats.steps >= 1);
        assert!(stats.checks >= stats.steps);
        drop(snapshots);
        drop(compactor);
        drop(db);
        // The file reopens with every row.
        let reopened = redb::Database::open(&path).unwrap();
        check(&reopened.begin_read().unwrap(), 8_000, 2);
    }

    #[test]
    fn small_or_dense_files_and_disabled_settings_stay_idle() {
        let path = temp_path("idle");
        let (_db, compactor) = mostly_free(&path);
        let large = CompactionSettings {
            min_file_bytes: 1 << 40,
            ..CompactionSettings::default()
        };
        assert_eq!(compactor.step(&large).unwrap(), Step::Idle);
        let off = CompactionSettings {
            ratio: 0,
            ..CompactionSettings::default()
        };
        assert_eq!(compactor.step(&off).unwrap(), Step::Idle);
        let stats = compactor.stats();
        assert!(!stats.active);
        assert_eq!(stats.checks, 2);
        assert_eq!(stats.steps, 0);
        assert!(stats.occupied_bytes.unwrap() > 40 << 20);
    }

    #[test]
    fn a_run_that_gives_up_waits_before_starting_again() {
        let path = temp_path("give-up");
        let (db, compactor) = mostly_free(&path);
        let settings = CompactionSettings::default();
        let state = &compactor.inner.state;

        // A run whose last step moved nothing, far from its goal, gives up.
        state.active.store(true, Ordering::Release);
        state.finishing.store(true, Ordering::Release);
        assert!(matches!(
            compactor.step(&settings).unwrap(),
            Step::Finished { reached: false, .. }
        ));
        let first = state.gave_up().expect("gave up");
        assert_eq!(first.backoff, GIVE_UP_FIRST);
        assert_eq!(first.file, file_len(&path).unwrap());
        let waiting = compactor.stats().waiting_ms.unwrap();
        assert!(waiting <= GIVE_UP_FIRST.as_millis() as u64, "{waiting}");

        // Still due, but it doesn't start again while nothing changed.
        for _ in 0..3 {
            assert_eq!(compactor.step(&settings).unwrap(), Step::Idle);
        }
        assert_eq!(compactor.stats().steps, 0);

        // Giving up again doubles the wait, up to its cap.
        state.active.store(true, Ordering::Release);
        state.finishing.store(true, Ordering::Release);
        compactor.step(&settings).unwrap();
        assert_eq!(state.gave_up().unwrap().backoff, GIVE_UP_FIRST * 2);
        state.set_gave_up(Some(GaveUp {
            backoff: GIVE_UP_MAX,
            ..state.gave_up().unwrap()
        }));
        state.active.store(true, Ordering::Release);
        state.finishing.store(true, Ordering::Release);
        compactor.step(&settings).unwrap();
        assert_eq!(state.gave_up().unwrap().backoff, GIVE_UP_MAX);

        // Once its wait is over it runs, and a run that reaches its goal
        // forgets the give-ups.
        state.set_gave_up(Some(GaveUp {
            retry_at: Instant::now(),
            ..state.gave_up().unwrap()
        }));
        assert!(matches!(compactor.step(&settings).unwrap(), Step::Moved(_)));
        run_to_end(&compactor, &settings);
        assert!(state.gave_up().is_none());
        assert_eq!(compactor.stats().waiting_ms, None);
        check(&db.begin_read().unwrap(), 8_000, 2);
    }

    #[test]
    fn freed_pages_end_the_wait_after_a_give_up() {
        let path = temp_path("freed");
        let (_db, compactor) = mostly_free(&path);
        let file = file_len(&path).unwrap();
        // As if a run gave up when twice as many pages were allocated: an
        // eighth of them have been freed since, so it starts at once.
        compactor.inner.state.set_gave_up(Some(GaveUp {
            file,
            allocated: file,
            retry_at: Instant::now() + GIVE_UP_MAX,
            backoff: GIVE_UP_MAX,
        }));
        assert!(matches!(
            compactor.step(&CompactionSettings::default()).unwrap(),
            Step::Moved(_)
        ));
    }

    #[test]
    fn group_stores_compact_their_file_under_open_snapshots() {
        use crate::storage::ReadTxn;
        use crate::storage::keys::ROOT_DB;
        let path = temp_path("group");
        let store = GroupStore::create(&path).unwrap();
        let compactor = store.compactor().expect("a file-backed store compacts");
        // Incompressible record values: lz4 packs repeated bytes to
        // nothing, which would leave no file to compact.
        let mut seed = 0x9e37_79b9_7f4a_7c15u64;
        let mut value = || {
            let pad: String = (0..2000)
                .map(|_| {
                    seed ^= seed << 13;
                    seed ^= seed >> 7;
                    seed ^= seed << 17;
                    char::from(b"0123456789abcdef"[(seed % 16) as usize])
                })
                .collect();
            format!("{{\"pad\":\"{pad}\"}}")
        };
        // Many records, most of them deleted: the survivors sit at the
        // end of a mostly free file.
        let txn = store.begin_write().unwrap();
        let collection = txn.intern(ROOT_DB, b"rows").unwrap();
        let mut survivors = Vec::new();
        for key in 0..20_000u32 {
            let value = value();
            txn.put_record(ROOT_DB, collection, &key.to_be_bytes(), value.as_bytes())
                .unwrap();
            if key >= 19_000 {
                survivors.push((key.to_be_bytes().to_vec(), value.into_bytes()));
            }
        }
        txn.commit().unwrap();
        let txn = store.begin_write().unwrap();
        for key in 0..19_000u32 {
            txn.delete_record(ROOT_DB, collection, &key.to_be_bytes())
                .unwrap();
        }
        txn.commit().unwrap();
        let before = file_len(&path).unwrap();
        assert!(before > 16 << 20, "{before}");
        let settings = CompactionSettings::default();
        let check = |snapshot: &ReadTxn| {
            let rows = snapshot
                .scan_records(ROOT_DB, collection, None, None, None, false)
                .unwrap();
            assert_eq!(rows.len(), survivors.len());
            assert!(rows == survivors, "a snapshot reads exactly the survivors");
        };
        // The engine always holds a snapshot; each newer one replaces it.
        let mut snapshot = store.begin_read().unwrap();
        let mut steps = 0;
        while file_len(&path).unwrap() * 3 >= before {
            assert_ne!(compactor.step(&settings).unwrap(), Step::Idle);
            let next = store.begin_read().unwrap();
            check(&snapshot);
            snapshot = next;
            steps += 1;
            assert!(steps < 2000, "{before} → {}", file_len(&path).unwrap());
        }
        check(&snapshot);
        let after = file_len(&path).unwrap();
        assert!(
            after * 3 < before,
            "{before} → {after} under open snapshots"
        );
    }

    #[test]
    fn stores_without_a_path_have_no_compactor() {
        let db = redb::Builder::new()
            .create_with_backend(redb::backends::InMemoryBackend::new())
            .unwrap();
        let store = GroupStore::from_database(db, None).unwrap();
        assert!(store.compactor().is_none());
    }
}
