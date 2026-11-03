//! Member-local write and watch activity for operators (admin), as v1's
//! `/admin/activity`:
//!
//! - `GET /v1/admin/activity`: one snapshot;
//! - `GET /v1/admin/activity/stream`: server-sent events, a `snapshot`
//!   event at once, then the latest snapshot after each change,
//!   coalesced to at most one a second, with `: flower` comments every
//!   15 seconds while idle. At most [`MAX_STREAMS`] streams at once; one
//!   more answers 429 `resource_exhausted`. Shutdown ends every stream.
//!
//! **Writes** are the applied writes this member's state machine
//! announced (leader and followers alike, replays included): database,
//! revision and log position, and the stored records the write put and
//! deleted (rows set and removed, queue and timer rows included; index
//! entries and derived values follow from them and are not counted),
//! for writes that moved their database's revision. `writes.puts` and
//! `writes.deletes` sum every write's, those past the detail window
//! included. The last [`MAX_WRITES`] stay; older ones are counted as
//! evicted. Never sum members' counts into cluster writes: each member
//! applies each write. **Watches** are this member's live `/v1/watch`
//! streams after their first snapshot: database, module, alias, when
//! each started and last updated, its updates (snapshots sent, the
//! first included) and revision. At most [`MAX_WATCHES`] are detailed;
//! the rest count as omitted. Labels are cut to [`MAX_LABEL_BYTES`].
//!
//! No arguments, results, keys, values, principals or credentials enter
//! it. Recording never waits for a reader: a busy snapshot costs a write
//! its detail (counted in `drops`), never its commit. Counters reset
//! with the process, whose random `process_id` tells restarts apart.

use std::collections::{HashMap, VecDeque};
use std::convert::Infallible;
use std::hash::{BuildHasher, Hasher};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Json, Response};
use axum::routing::get;
use flower_cluster::exec::now_ms;
use flower_cluster::types::CommitNotice;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, broadcast, watch};

use crate::api::AppState;

/// Applied writes kept in detail.
pub const MAX_WRITES: usize = 256;
/// Live watches kept in detail.
pub const MAX_WATCHES: usize = 256;
/// Longest label kept, in UTF-8 bytes.
pub const MAX_LABEL_BYTES: usize = 192;
/// Concurrent activity streams.
pub const MAX_STREAMS: usize = 16;
/// The most often a stream sends a snapshot.
const CADENCE: Duration = Duration::from_secs(1);
/// Idle stream comments.
const KEEPALIVE: Duration = Duration::from_secs(15);

/// One applied write.
#[derive(Clone, Debug)]
struct Write {
    id: u64,
    at_ms: u64,
    db: String,
    revision: u64,
    position: u64,
    puts: u64,
    deletes: u64,
}

/// One live watch.
#[derive(Debug)]
pub struct Watched {
    id: u64,
    db: String,
    module: String,
    name: String,
    started_at_ms: u64,
    updated_at_ms: AtomicU64,
    updates: AtomicU64,
    revision: AtomicU64,
}

/// The detail windows.
#[derive(Default)]
struct Window {
    writes: VecDeque<Write>,
    watches: Vec<Weak<Watched>>,
}

/// A member's activity record.
pub struct Activity {
    process_id: String,
    started_at_ms: u64,
    window: Mutex<Window>,
    sequence: AtomicU64,
    ids: AtomicU64,
    writes: AtomicU64,
    puts: AtomicU64,
    deletes: AtomicU64,
    evicted: AtomicU64,
    write_drops: AtomicU64,
    label_drops: AtomicU64,
    active: AtomicU64,
    started: AtomicU64,
    ended: AtomicU64,
    updates: AtomicU64,
    omitted: AtomicU64,
    changed: Notify,
    streams: Arc<Semaphore>,
    closed: watch::Sender<bool>,
}

impl Default for Activity {
    fn default() -> Self {
        let mut random = std::collections::hash_map::RandomState::new().build_hasher();
        random.write_u64(now_ms());
        Activity {
            process_id: format!("{:016x}", random.finish()),
            started_at_ms: now_ms(),
            window: Mutex::default(),
            sequence: AtomicU64::new(0),
            ids: AtomicU64::new(0),
            writes: AtomicU64::new(0),
            puts: AtomicU64::new(0),
            deletes: AtomicU64::new(0),
            evicted: AtomicU64::new(0),
            write_drops: AtomicU64::new(0),
            label_drops: AtomicU64::new(0),
            active: AtomicU64::new(0),
            started: AtomicU64::new(0),
            ended: AtomicU64::new(0),
            updates: AtomicU64::new(0),
            omitted: AtomicU64::new(0),
            changed: Notify::new(),
            streams: Arc::new(Semaphore::new(MAX_STREAMS)),
            closed: watch::channel(false).0,
        }
    }
}

impl Activity {
    /// Note a change: streams send the next snapshot.
    fn signal(&self) {
        self.sequence.fetch_add(1, Ordering::Release);
        self.changed.notify_waiters();
    }

    /// A label cut to [`MAX_LABEL_BYTES`] on a character boundary.
    fn label(&self, text: &str) -> String {
        if text.len() <= MAX_LABEL_BYTES {
            return text.to_string();
        }
        self.label_drops.fetch_add(1, Ordering::Relaxed);
        let mut end = MAX_LABEL_BYTES;
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        text[..end].to_string()
    }

    /// Record one applied write, without waiting for a reader.
    fn write(&self, notice: &CommitNotice) {
        self.writes.fetch_add(1, Ordering::Relaxed);
        self.puts.fetch_add(notice.puts, Ordering::Relaxed);
        self.deletes.fetch_add(notice.deletes, Ordering::Relaxed);
        let entry = Write {
            id: self.ids.fetch_add(1, Ordering::Relaxed),
            at_ms: now_ms(),
            db: self.label(&String::from_utf8_lossy(&notice.db)),
            revision: notice.revision,
            position: notice.position,
            puts: notice.puts,
            deletes: notice.deletes,
        };
        match self.window.try_lock() {
            Ok(mut window) => {
                if window.writes.len() == MAX_WRITES {
                    window.writes.pop_front();
                    self.evicted.fetch_add(1, Ordering::Relaxed);
                }
                window.writes.push_back(entry);
            }
            Err(_) => {
                self.write_drops.fetch_add(1, Ordering::Relaxed);
            }
        }
        self.signal();
    }

    /// Record applied writes from the member's commit notices until
    /// `stop` turns true: those that moved their database's revision.
    /// Key-catalog notices move none.
    pub(crate) async fn record(
        self: Arc<Self>,
        mut commits: broadcast::Receiver<CommitNotice>,
        mut stop: watch::Receiver<bool>,
    ) {
        let mut revisions: HashMap<Vec<u8>, u64> = HashMap::new();
        loop {
            tokio::select! {
                notice = commits.recv() => match notice {
                    Ok(notice) if notice.keys => {}
                    Ok(notice) => {
                        let last = revisions.entry(notice.db.clone()).or_insert(0);
                        if notice.revision > *last {
                            *last = notice.revision;
                            self.write(&notice);
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(missed)) => {
                        self.writes.fetch_add(missed, Ordering::Relaxed);
                        self.write_drops.fetch_add(missed, Ordering::Relaxed);
                        self.signal();
                    }
                    Err(broadcast::error::RecvError::Closed) => return,
                },
                changed = stop.changed() => {
                    if changed.is_err() || *stop.borrow() {
                        return;
                    }
                }
            }
        }
    }

    /// Register a live watch at `revision`, its first snapshot sent.
    /// The guard counts later snapshots and ends it on drop.
    pub(crate) fn watch(
        self: &Arc<Self>,
        db: &str,
        module: &str,
        name: &str,
        revision: u64,
    ) -> WatchGuard {
        self.active.fetch_add(1, Ordering::Relaxed);
        self.started.fetch_add(1, Ordering::Relaxed);
        self.updates.fetch_add(1, Ordering::Relaxed);
        let now = now_ms();
        let watched = Arc::new(Watched {
            id: self.ids.fetch_add(1, Ordering::Relaxed),
            db: self.label(db),
            module: self.label(module),
            name: self.label(name),
            started_at_ms: now,
            updated_at_ms: AtomicU64::new(now),
            updates: AtomicU64::new(1),
            revision: AtomicU64::new(revision),
        });
        let kept = match self.window.try_lock() {
            Ok(mut window) => {
                window.watches.retain(|watched| watched.strong_count() > 0);
                if window.watches.len() < MAX_WATCHES {
                    window.watches.push(Arc::downgrade(&watched));
                    true
                } else {
                    false
                }
            }
            Err(_) => false,
        };
        if !kept {
            self.omitted.fetch_add(1, Ordering::Relaxed);
        }
        self.signal();
        WatchGuard {
            activity: self.clone(),
            watched,
            alive: AtomicBool::new(true),
        }
    }

    /// Stop every activity stream: the member shuts down.
    pub(crate) fn close(&self) {
        let _ = self.closed.send(true);
    }

    /// The change count: it moves with every recorded change, so a
    /// reader compares it with the one its last snapshot saw.
    pub(crate) fn sequence(&self) -> u64 {
        self.sequence.load(Ordering::Acquire)
    }

    /// Resolves at the next recorded change. Enable it before reading
    /// [`Activity::sequence`], so no change slips between the two.
    pub(crate) fn next_change(&self) -> tokio::sync::futures::Notified<'_> {
        self.changed.notified()
    }

    /// The current snapshot.
    pub fn snapshot(&self) -> serde_json::Value {
        // Read before the details: a change racing the snapshot costs
        // one extra snapshot, never a missed one.
        let sequence = self.sequence.load(Ordering::Acquire);
        let (writes, watches) = {
            let window = self.window.lock().unwrap_or_else(PoisonError::into_inner);
            let writes: Vec<serde_json::Value> = window
                .writes
                .iter()
                .map(|write| {
                    serde_json::json!({
                        "id": write.id,
                        "at_ms": write.at_ms,
                        "db": write.db,
                        "revision": write.revision,
                        "position": write.position,
                        "puts": write.puts,
                        "deletes": write.deletes,
                    })
                })
                .collect();
            let watches: Vec<serde_json::Value> = window
                .watches
                .iter()
                .filter_map(Weak::upgrade)
                .map(|watched| {
                    serde_json::json!({
                        "id": watched.id,
                        "db": watched.db,
                        "module": watched.module,
                        "name": watched.name,
                        "started_at_ms": watched.started_at_ms,
                        "updated_at_ms": watched.updated_at_ms.load(Ordering::Relaxed),
                        "updates": watched.updates.load(Ordering::Relaxed),
                        "revision": watched.revision.load(Ordering::Relaxed),
                    })
                })
                .collect();
            (writes, watches)
        };
        let count = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        serde_json::json!({
            "version": 1,
            "process_id": self.process_id,
            "started_at_ms": self.started_at_ms,
            "sampled_at_ms": now_ms(),
            "sequence": sequence,
            "limits": {
                "writes": MAX_WRITES,
                "watches": MAX_WATCHES,
                "label_bytes": MAX_LABEL_BYTES,
            },
            "writes": {
                "total": count(&self.writes),
                "puts": count(&self.puts),
                "deletes": count(&self.deletes),
                "evicted": count(&self.evicted),
                "entries": writes,
            },
            "watches": {
                "active": count(&self.active),
                "started": count(&self.started),
                "ended": count(&self.ended),
                "updates": count(&self.updates),
                "omitted": count(&self.omitted),
                "entries": watches,
            },
            "drops": {
                "writes": count(&self.write_drops),
                "labels": count(&self.label_drops),
            },
        })
    }
}

/// A live watch's registration; dropping it ends the watch.
pub(crate) struct WatchGuard {
    activity: Arc<Activity>,
    watched: Arc<Watched>,
    alive: AtomicBool,
}

impl WatchGuard {
    /// The watch sent a snapshot at `revision`.
    pub(crate) fn update(&self, revision: u64) {
        self.activity.updates.fetch_add(1, Ordering::Relaxed);
        self.watched.updates.fetch_add(1, Ordering::Relaxed);
        self.watched.revision.store(revision, Ordering::Relaxed);
        self.watched
            .updated_at_ms
            .store(now_ms(), Ordering::Relaxed);
        self.activity.signal();
    }
}

impl Drop for WatchGuard {
    fn drop(&mut self) {
        if self.alive.swap(false, Ordering::AcqRel) {
            self.activity.active.fetch_sub(1, Ordering::Relaxed);
            self.activity.ended.fetch_add(1, Ordering::Relaxed);
            self.activity.signal();
        }
    }
}

/// The activity routes, admin-only.
pub(crate) fn routes() -> Router<AppState> {
    Router::new()
        .route("/v1/admin/activity", get(get_activity))
        .route("/v1/admin/activity/stream", get(get_stream))
}

/// One snapshot.
async fn get_activity(State(state): State<AppState>) -> Json<serde_json::Value> {
    Json(state.activity.snapshot())
}

/// A stream's state between events.
struct Streaming {
    activity: Arc<Activity>,
    _permit: OwnedSemaphorePermit,
    closed: watch::Receiver<bool>,
    /// The sequence the last snapshot sent saw; `None` before the first.
    sent: Option<u64>,
    /// When the next snapshot may go.
    next: tokio::time::Instant,
}

/// The snapshot stream: at once, then after changes at most once per
/// [`CADENCE`], until shutdown or the client leaves.
async fn get_stream(State(state): State<AppState>) -> Response {
    let Ok(permit) = state.activity.streams.clone().try_acquire_owned() else {
        let body = serde_json::json!({
            "ok": false,
            "code": "resource_exhausted",
            "message": "operator activity stream capacity reached",
        });
        return (StatusCode::TOO_MANY_REQUESTS, Json(body)).into_response();
    };
    let streaming = Streaming {
        closed: state.activity.closed.subscribe(),
        activity: state.activity.clone(),
        _permit: permit,
        sent: None,
        next: tokio::time::Instant::now(),
    };
    let events = futures_util::stream::unfold(streaming, |mut streaming| async move {
        loop {
            if *streaming.closed.borrow() {
                return None;
            }
            // Registered before the sequence is read: no change slips
            // between the check and the wait.
            let activity = streaming.activity.clone();
            let changed = activity.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let sequence = streaming.activity.sequence.load(Ordering::Acquire);
            if streaming.sent != Some(sequence) {
                tokio::select! {
                    () = tokio::time::sleep_until(streaming.next) => {}
                    _ = streaming.closed.changed() => return None,
                }
                let snapshot = streaming.activity.snapshot();
                streaming.sent = snapshot["sequence"].as_u64();
                streaming.next = tokio::time::Instant::now() + CADENCE;
                let event = Event::default()
                    .event("snapshot")
                    .data(snapshot.to_string());
                return Some((Ok::<_, Infallible>(event), streaming));
            }
            tokio::select! {
                () = &mut changed => {}
                _ = streaming.closed.changed() => return None,
            }
        }
    });
    let mut response = Sse::new(events)
        .keep_alive(KeepAlive::new().interval(KEEPALIVE).text("flower"))
        .into_response();
    response.headers_mut().insert(
        axum::http::header::CACHE_CONTROL,
        axum::http::HeaderValue::from_static("no-store"),
    );
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    fn commit(revision: u64, position: u64, puts: u64, deletes: u64) -> CommitNotice {
        CommitNotice::commit(b"db".to_vec(), revision, position).with_records(puts, deletes)
    }

    #[test]
    fn writes_keep_a_bounded_window_and_count_the_rest() {
        let activity = Activity::default();
        for revision in 1..=300 {
            activity.write(&commit(revision, revision + 10, 2, 1));
        }
        let snapshot = activity.snapshot();
        assert_eq!(snapshot["writes"]["total"], 300);
        assert_eq!(snapshot["writes"]["evicted"], 44);
        // Records count for every write, the evicted included.
        assert_eq!(snapshot["writes"]["puts"], 600);
        assert_eq!(snapshot["writes"]["deletes"], 300);
        let entries = snapshot["writes"]["entries"].as_array().unwrap();
        assert_eq!(entries.len(), MAX_WRITES);
        assert_eq!(entries[0]["revision"], 45);
        assert_eq!(entries[255]["position"], 310);
        assert_eq!(
            (
                entries[255]["puts"].clone(),
                entries[255]["deletes"].clone()
            ),
            (2.into(), 1.into())
        );
        // A reader holding the window costs a write its detail, never
        // its count.
        let held = activity.window.lock().unwrap();
        activity.write(&commit(301, 311, 0, 4));
        drop(held);
        let snapshot = activity.snapshot();
        assert_eq!(snapshot["writes"]["total"], 301);
        assert_eq!(snapshot["writes"]["deletes"], 304);
        assert_eq!(snapshot["drops"]["writes"], 1);
        assert_eq!(snapshot["sequence"], 301);
    }

    #[tokio::test]
    async fn key_catalog_notices_are_not_writes() {
        // A key change moves no revision, even the one naming the
        // database it changed.
        let activity = Arc::new(Activity::default());
        let (commits, receiver) = broadcast::channel(8);
        let (_stop, stopped) = watch::channel(false);
        commits.send(CommitNotice::keys(b"db".to_vec(), 4)).unwrap();
        commits
            .send(CommitNotice::commit(b"db".to_vec(), 1, 5))
            .unwrap();
        commits.send(CommitNotice::all_keys(6)).unwrap();
        drop(commits);
        activity.clone().record(receiver, stopped).await;
        let snapshot = activity.snapshot();
        assert_eq!(snapshot["writes"]["total"], 1);
        let entries = snapshot["writes"]["entries"].as_array().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(
            (entries[0]["db"].clone(), entries[0]["position"].clone()),
            ("db".into(), 5.into())
        );
    }

    #[test]
    fn watches_count_their_lifecycle_and_cut_long_labels() {
        let activity = Arc::new(Activity::default());
        let mut guards: Vec<WatchGuard> = (0..300)
            .map(|i| activity.watch("db", "app", &format!("q{i}"), 1))
            .collect();
        guards[0].update(2);
        let snapshot = activity.snapshot();
        assert_eq!(snapshot["watches"]["active"], 300);
        assert_eq!(snapshot["watches"]["omitted"], 44);
        assert_eq!(snapshot["watches"]["updates"], 301);
        let entries = snapshot["watches"]["entries"].as_array().unwrap();
        assert_eq!(entries.len(), MAX_WATCHES);
        assert_eq!(
            (
                entries[0]["updates"].clone(),
                entries[0]["revision"].clone()
            ),
            (2.into(), 2.into())
        );
        guards.truncate(10);
        let snapshot = activity.snapshot();
        assert_eq!(snapshot["watches"]["active"], 10);
        assert_eq!(snapshot["watches"]["ended"], 290);
        assert_eq!(snapshot["watches"]["entries"].as_array().unwrap().len(), 10);
        // Freed detail slots take new watches; labels stop at 192 bytes
        // on a character boundary.
        let long = format!("{}é", "x".repeat(MAX_LABEL_BYTES - 1));
        let _guard = activity.watch(&long, "app", "q", 1);
        let snapshot = activity.snapshot();
        let entries = snapshot["watches"]["entries"].as_array().unwrap();
        let last = entries.last().unwrap();
        assert_eq!(last["db"].as_str().unwrap().len(), MAX_LABEL_BYTES - 1);
        assert_eq!(snapshot["drops"]["labels"], 1);
    }
}
