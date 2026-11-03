//! The live view: a read-only page on 127.0.0.1 showing the run as it
//! happens.
//!
//! The page, its script and its style are embedded in the binary, as
//! the `/admin` dashboard's are, and served with the same headers: a
//! strict same-origin Content-Security-Policy (no inline script or
//! style, nothing from another origin), `nosniff`, `no-referrer` and
//! `Cross-Origin-Resource-Policy: same-origin`. It shows bench data
//! only, never a token: the state it streams is built from the bench's
//! own counters, the cluster's status and the Flower watches below.
//! Requests naming another host are refused, so a page elsewhere cannot
//! rebind a name to this port and read it. The script renders every
//! string through `textContent` and DOM calls, never as markup.
//!
//! **Where the numbers come from.** The shop floor is Flower's own: one
//! watch (`DeltaStream`, asking for deltas) per tenant on its
//! `pizza.leaderboard`, which reconnects to another member when its
//! member dies, so the page also shows watches working through a
//! failover. Members' roles come from polling `/v1/status`. Drones,
//! charts, the kitchen totals and the audit checklist come from the
//! bench's counters. `GET /events` streams the whole state as JSON four
//! times a second; `GET /state` answers it once.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::{Duration, Instant};

use axum::Router;
use axum::extract::{Request, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Json, Response};
use axum::routing::get;
use flower_client::CancellationToken;
use futures_util::stream::Stream;
use serde::Serialize;
use serde_json::{Value, json};

use crate::audit::{Audit, CHECKLIST};
use crate::client::BenchClient;
use crate::drones::Yard;
use crate::metrics::{Phase, Recorder};
use crate::workload::Ledger;

/// The Content-Security-Policy every response carries: the `/admin`
/// dashboard's.
pub const CSP: &str = "default-src 'none'; script-src 'self'; style-src 'self'; \
                       connect-src 'self'; img-src 'self' data:; frame-ancestors 'none'; \
                       base-uri 'none'; form-action 'none'";

/// Timeline points the page charts.
const CHART_POINTS: usize = 150;
/// How often the event stream sends the state.
const STREAM_EVERY: Duration = Duration::from_millis(250);

/// One embedded file.
struct Asset {
    path: &'static str,
    content_type: &'static str,
    body: &'static [u8],
}

const ASSETS: [Asset; 6] = [
    Asset {
        path: "/",
        content_type: "text/html; charset=utf-8",
        body: include_bytes!("live/index.html"),
    },
    Asset {
        path: "/style.css",
        content_type: "text/css; charset=utf-8",
        body: include_bytes!("live/style.css"),
    },
    Asset {
        path: "/app.js",
        content_type: "text/javascript; charset=utf-8",
        body: include_bytes!("live/app.js"),
    },
    Asset {
        path: "/charts.js",
        content_type: "text/javascript; charset=utf-8",
        body: include_bytes!("live/charts.js"),
    },
    Asset {
        path: "/dom.js",
        content_type: "text/javascript; charset=utf-8",
        body: include_bytes!("live/dom.js"),
    },
    Asset {
        path: "/favicon.svg",
        content_type: "image/svg+xml",
        body: include_bytes!("live/favicon.svg"),
    },
];

/// Every live-view path with its content type.
pub fn assets() -> impl Iterator<Item = (&'static str, &'static str, &'static [u8])> {
    ASSETS
        .iter()
        .map(|asset| (asset.path, asset.content_type, asset.body))
}

/// A member as the page draws it.
#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MemberView {
    /// Member ID.
    pub id: u64,
    /// API URL.
    pub url: String,
    /// Its own dashboard.
    pub admin_url: String,
    /// `leader`, `follower`, `candidate`, `learner`, `down` or `unknown`.
    pub role: String,
    /// Raft term.
    pub term: Option<u64>,
    /// Applied log index.
    pub applied: Option<u64>,
    /// With several groups: the groups it leads (its role and the crown
    /// are the first group's).
    pub leads: Vec<String>,
}

/// A chaos event as the timeline marks it: seconds from the start of
/// the measured window.
#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChaosMark {
    /// The killed member.
    pub killed: u64,
    /// When it was killed.
    pub kill_t: f64,
    /// When a new leader was seen.
    pub elected_t: Option<f64>,
    /// The new leader.
    pub new_leader: Option<u64>,
    /// When the first write was acknowledged after the kill.
    pub recovered_t: Option<f64>,
    /// Kill to first acknowledged write, ms.
    pub failover_ms: Option<f64>,
    /// When the killed member was started again.
    pub restarted_t: Option<f64>,
    /// When it had caught up.
    pub caught_up_t: Option<f64>,
}

/// One tenant's stores, as its watch last delivered them.
#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TenantFloor {
    /// Tenant ID.
    pub tenant: String,
    /// The member the watch is connected to.
    pub member: Option<u64>,
    /// Whether it is connected.
    pub connected: bool,
    /// Values received (snapshots and applied patches).
    pub updates: u64,
    /// Fresh snapshots after (re)connecting or a gap.
    pub resets: u64,
    /// Database revision of the last value.
    pub revision: Option<u64>,
    /// The leaderboard: store rows, best first.
    pub stores: Vec<Value>,
}

/// One checklist line.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckView {
    /// Check ID.
    pub id: &'static str,
    /// Title.
    pub title: &'static str,
    /// `pending`, `pass` or `fail`.
    pub state: &'static str,
    /// Evidence, once checked.
    pub detail: String,
}

/// The run's ending, for the page.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Finale {
    /// Whether the run passed.
    pub passed: bool,
    /// Goodput over the measured window.
    pub goodput: f64,
    /// Write p99 over it, ms.
    pub write_p99: Option<f64>,
    /// Read p99 over it, ms.
    pub read_p99: Option<f64>,
    /// The JSON report.
    pub report_json: String,
    /// The HTML report.
    pub report_html: String,
}

#[derive(Default)]
struct LiveData {
    note: String,
    members: Vec<MemberView>,
    chaos: Vec<ChaosMark>,
    floor: BTreeMap<String, TenantFloor>,
    audit: Option<Audit>,
    auditing: bool,
    finale: Option<Finale>,
}

/// What the page's numbers are read from.
pub struct Sources {
    /// The run's accounting (every group's).
    pub recorder: Arc<Recorder>,
    /// Each group's ledger and drone yard.
    pub groups: Vec<(Arc<Ledger>, Arc<Yard>)>,
}

/// Static facts about the run.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RunFacts {
    /// Run ID.
    pub id: String,
    /// Preset name.
    pub preset: Option<&'static str>,
    /// One-line description.
    pub describe: String,
    /// Measured seconds.
    pub duration_s: f64,
    /// When chaos strikes, seconds into the window, if it does.
    pub chaos_at_s: Option<f64>,
    /// Order cap.
    pub max_orders: u64,
    /// Starting stock per store.
    pub stock_per_store: u64,
    /// Oven time, ms.
    pub bake_ms: u64,
    /// Read alias customers use.
    pub read_method: &'static str,
}

/// The live view's state, shared by the run and the server.
pub struct Live {
    facts: RunFacts,
    started: Instant,
    data: Mutex<LiveData>,
    sources: OnceLock<Sources>,
}

impl Live {
    /// A live view for a run.
    pub fn new(facts: RunFacts, tenants: &[String]) -> Arc<Live> {
        let floor = tenants
            .iter()
            .map(|tenant| {
                (
                    tenant.clone(),
                    TenantFloor {
                        tenant: tenant.clone(),
                        ..TenantFloor::default()
                    },
                )
            })
            .collect();
        Arc::new(Live {
            facts,
            started: Instant::now(),
            data: Mutex::new(LiveData {
                floor,
                ..LiveData::default()
            }),
            sources: OnceLock::new(),
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, LiveData> {
        self.data.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Attach the counters; once.
    pub fn attach(&self, sources: Sources) {
        let _ = self.sources.set(sources);
    }

    /// Show a note under the phase (setup steps, chaos lines).
    pub fn note(&self, note: impl Into<String>) {
        self.lock().note = note.into();
    }

    /// Replace the members' view.
    pub fn members(&self, members: Vec<MemberView>) {
        self.lock().members = members;
    }

    /// Add or update chaos event `index`.
    pub fn chaos(&self, index: usize, mark: ChaosMark) {
        let mut data = self.lock();
        if index < data.chaos.len() {
            data.chaos[index] = mark;
        } else {
            data.chaos.push(mark);
        }
    }

    /// Update one tenant's floor.
    pub fn floor(&self, tenant: &str, update: impl FnOnce(&mut TenantFloor)) {
        if let Some(floor) = self.lock().floor.get_mut(tenant) {
            update(floor);
        }
    }

    /// The audit is running.
    pub fn auditing(&self) {
        self.lock().auditing = true;
    }

    /// The audit's result.
    pub fn audited(&self, audit: Audit) {
        let mut data = self.lock();
        data.auditing = false;
        data.audit = Some(audit);
    }

    /// The run's ending.
    pub fn finish(&self, finale: Finale) {
        self.lock().finale = Some(finale);
    }

    /// The whole state as the page reads it.
    pub fn snapshot(&self) -> Value {
        let data = self.lock();
        let sources = self.sources.get();
        let phase = sources.map(|sources| sources.recorder.phase());
        let load_t = sources
            .and_then(|sources| sources.recorder.load_started())
            .map(|start| {
                Instant::now()
                    .saturating_duration_since(start)
                    .as_secs_f64()
            });
        let timeline =
            sources.map_or_else(Vec::new, |sources| sources.recorder.recent(CHART_POINTS));
        let kitchen = sources.map(|sources| {
            let views: Vec<_> = sources
                .groups
                .iter()
                .map(|(ledger, _)| ledger.view())
                .collect();
            let issued: u64 = sources
                .groups
                .iter()
                .map(|(ledger, _)| ledger.issued())
                .sum();
            let delivered: u64 = data
                .floor
                .values()
                .flat_map(|floor| &floor.stores)
                .filter_map(|store| store["delivered"].as_u64())
                .sum();
            let coins: u64 = data
                .floor
                .values()
                .flat_map(|floor| &floor.stores)
                .filter_map(|store| store["revenue"].as_u64())
                .sum();
            json!({
                "maxOrders": self.facts.max_orders * sources.groups.len().max(1) as u64,
                "issued": issued,
                "acknowledged": views.iter().map(|view| view.orders.len()).sum::<usize>(),
                "tips": views.iter().flat_map(|view| view.tips.values()).sum::<u64>(),
                "delivered": delivered,
                "coins": coins,
                "failures": views.iter().map(|view| view.failures).sum::<u64>(),
                "replays": views.iter().map(|view| view.replay_checks).sum::<u64>(),
                "uncertain": views.iter().map(|view| view.pending).sum::<usize>(),
            })
        });
        let drones = sources.map(|sources| {
            let mut yards = sources.groups.iter().map(|(_, yard)| yard.view());
            let mut all = yards.next()?;
            for view in yards {
                all.slots.extend(view.slots);
                all.dropped.extend(view.dropped);
                let counters = &mut all.counters;
                counters.claims += view.counters.claims;
                counters.reclaimed += view.counters.reclaimed;
                counters.delivered += view.counters.delivered;
                counters.failed += view.counters.failed;
                counters.abandoned += view.counters.abandoned;
                counters.lost += view.counters.lost;
                counters.fleet += view.counters.fleet;
            }
            let from = all.dropped.len().saturating_sub(12);
            all.dropped.drain(..from);
            Some(all)
        });
        // The checklist, then any check beyond it once the audit ran
        // (`access` with --auth, the cross-group checks).
        let extra: Vec<(&'static str, &'static str)> = data
            .audit
            .iter()
            .flat_map(|audit| &audit.checks)
            .filter(|check| !CHECKLIST.iter().any(|(id, _)| *id == check.id))
            .map(|check| (check.id, check.title))
            .collect();
        let checks: Vec<CheckView> = CHECKLIST
            .iter()
            .copied()
            .chain(extra)
            .map(|(id, title)| {
                let done = data
                    .audit
                    .as_ref()
                    .and_then(|audit| audit.checks.iter().find(|check| check.id == id));
                CheckView {
                    id,
                    title,
                    state: match done {
                        Some(check) if check.passed => "pass",
                        Some(_) => "fail",
                        None => "pending",
                    },
                    detail: done.map_or(String::new(), |check| check.detail.clone()),
                }
            })
            .collect();
        json!({
            "run": {
                "facts": self.facts,
                "phase": phase.map(Phase::name),
                "note": data.note,
                "elapsedS": self.started.elapsed().as_secs_f64(),
                "loadT": load_t,
            },
            "members": data.members,
            "chaos": data.chaos,
            "kitchen": kitchen,
            "floor": data.floor.values().collect::<Vec<_>>(),
            "drones": drones.flatten(),
            "timeline": timeline,
            "audit": {
                "state": if data.audit.is_some() { "done" } else if data.auditing { "running" } else { "pending" },
                "passed": data.audit.as_ref().map(|audit| audit.passed),
                "checks": checks,
            },
            "finale": data.finale,
        })
    }
}

/// A running live server.
pub struct LiveServer {
    /// Where it serves.
    pub addr: SocketAddr,
    stop: CancellationToken,
}

impl LiveServer {
    /// The page's URL.
    pub fn url(&self) -> String {
        format!("http://{}/", self.addr)
    }

    /// Stop serving.
    pub fn stop(&self) {
        self.stop.cancel();
    }
}

/// Serve the live view on `127.0.0.1:port` (0 for any free port).
pub async fn serve(live: Arc<Live>, port: u16) -> Result<LiveServer, String> {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
        .await
        .map_err(|error| format!("live view on port {port}: {error}"))?;
    let addr = listener.local_addr().map_err(|error| error.to_string())?;
    let stop = CancellationToken::new();
    let router = router(live, addr.port());
    let shutdown = stop.clone();
    tokio::spawn(async move {
        let _ = axum::serve(listener, router)
            .with_graceful_shutdown(async move { shutdown.cancelled().await })
            .await;
    });
    Ok(LiveServer { addr, stop })
}

/// The live view's routes.
pub fn router(live: Arc<Live>, port: u16) -> Router {
    let mut router = Router::new()
        .route("/state", get(state))
        .route("/events", get(events));
    for (index, asset) in ASSETS.iter().enumerate() {
        router = router.route(asset.path, get(move || async move { serve_asset(index) }));
    }
    router
        .layer(middleware::from_fn(secure))
        .layer(middleware::from_fn(move |request, next| {
            same_host(port, request, next)
        }))
        .with_state(live)
}

/// Refuse requests naming another host: DNS rebinding cannot reach the
/// page under a name of its own.
async fn same_host(port: u16, request: Request, next: Next) -> Response {
    let allowed = [
        format!("127.0.0.1:{port}"),
        format!("localhost:{port}"),
        format!("[::1]:{port}"),
    ];
    let host = request
        .headers()
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    if allowed.iter().any(|name| name == host) {
        next.run(request).await
    } else {
        (StatusCode::MISDIRECTED_REQUEST, "unexpected Host").into_response()
    }
}

/// The `/admin` dashboard's headers on every response.
async fn secure(request: Request, next: Next) -> Response {
    let mut response = next.run(request).await;
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(CSP),
    );
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
    headers.insert(
        header::HeaderName::from_static("cross-origin-resource-policy"),
        HeaderValue::from_static("same-origin"),
    );
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

fn serve_asset(index: usize) -> Response {
    let asset = &ASSETS[index];
    (
        [(
            header::CONTENT_TYPE,
            HeaderValue::from_static(asset.content_type),
        )],
        asset.body,
    )
        .into_response()
}

async fn state(State(live): State<Arc<Live>>) -> Json<Value> {
    Json(live.snapshot())
}

async fn events(
    State(live): State<Arc<Live>>,
) -> Sse<impl Stream<Item = Result<Event, std::convert::Infallible>>> {
    let stream = futures_util::stream::unfold(
        (live, tokio::time::interval(STREAM_EVERY)),
        |(live, mut ticks)| async move {
            ticks.tick().await;
            let event = Event::default().data(live.snapshot().to_string());
            Some((Ok(event), (live, ticks)))
        },
    );
    Sse::new(stream).keep_alive(KeepAlive::default())
}

/// One tenant's floor to watch: its group's client and its database.
pub struct FloorWatch {
    /// The tenant's group's client.
    pub client: Arc<BenchClient>,
    /// The database holding the tenant.
    pub db: String,
    /// The tenant.
    pub tenant: String,
}

/// Keep each tenant's floor current from a Flower watch on its
/// leaderboard, moving to another member when a connection ends. With
/// `--auth` each watch presents its tenant's bearer token.
pub fn watch_floor(
    live: Arc<Live>,
    watches: Vec<FloorWatch>,
    stop: CancellationToken,
) -> Vec<tokio::task::JoinHandle<()>> {
    watches
        .into_iter()
        .enumerate()
        .map(|(index, watch)| {
            let (live, stop) = (live.clone(), stop.clone());
            tokio::spawn(async move {
                watch_tenant(&live, &watch, index, &stop).await;
            })
        })
        .collect()
}

async fn watch_tenant(live: &Live, watch: &FloorWatch, start: usize, stop: &CancellationToken) {
    use flower_client::transport::Credentials;
    use flower_client::watch::{WatchEvent, WatchOptions};
    let (client, tenant) = (&watch.client, watch.tenant.as_str());
    let count = client.members().len();
    let mut member = start % count;
    let options = WatchOptions {
        credentials: client
            .bearer_for(crate::client::Who::Tenant(tenant))
            .map(Credentials::token),
        timeout: Some(Duration::from_secs(2)),
        cancel: Some(stop.clone()),
    };
    let args = json!(tenant);
    while !stop.is_cancelled() {
        let id = client.members()[member].id;
        let flower = client.anonymous_client(member);
        let opened = tokio::select! {
            () = stop.cancelled() => return,
            opened = flower.watch_deltas(&watch.db, client.module(), "pizza.leaderboard", &args, &options) => opened,
        };
        if let Ok(mut stream) = opened {
            live.floor(tenant, |floor| {
                floor.member = Some(id);
                floor.connected = true;
            });
            // Every connection starts with a whole snapshot.
            let mut first = true;
            loop {
                let event = tokio::select! {
                    () = stop.cancelled() => return,
                    event = stream.next_event() => event,
                };
                let (revision, value, reset) = match event {
                    Some(Ok(WatchEvent::Snapshot { revision, value })) => (revision, value, false),
                    Some(Ok(WatchEvent::Reset { revision, value })) => (revision, value, true),
                    _ => break,
                };
                let snapshot = reset || first;
                first = false;
                live.floor(tenant, |floor| {
                    floor.updates += 1;
                    floor.resets += u64::from(snapshot);
                    floor.revision = Some(revision);
                    floor.stores = value.as_array().cloned().unwrap_or_default();
                });
            }
        }
        live.floor(tenant, |floor| floor.connected = false);
        member = (member + 1) % count;
        tokio::select! {
            () = stop.cancelled() => return,
            () = tokio::time::sleep(Duration::from_millis(200)) => {}
        }
    }
}

/// Open `url` in the default browser; best effort.
pub fn open_browser(url: &str) {
    let result = if cfg!(windows) {
        std::process::Command::new("cmd")
            .args(["/C", "start", "", url])
            .spawn()
    } else if cfg!(target_os = "macos") {
        std::process::Command::new("open").arg(url).spawn()
    } else {
        std::process::Command::new("xdg-open").arg(url).spawn()
    };
    if result.is_err() {
        eprintln!("could not open a browser; visit {url}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Whether markup carries an inline event handler (` onclick=`).
    fn has_handler(page: &str) -> bool {
        page.match_indices(" on").any(|(at, _)| {
            let rest = &page[at + 3..];
            let name: String = rest.chars().take_while(char::is_ascii_alphabetic).collect();
            !name.is_empty() && rest[name.len()..].starts_with('=')
        })
    }

    #[test]
    fn the_page_loads_nothing_inline_or_from_elsewhere() {
        let page = std::str::from_utf8(ASSETS[0].body).unwrap();
        assert!(!page.contains("<style"), "inline style");
        assert!(!page.contains(" style="), "style attribute");
        assert!(!has_handler(page), "inline handler");
        for script in page.split("<script").skip(1) {
            assert!(
                script.contains("src=\"/"),
                "inline or remote script: {script}"
            );
        }
        for asset in &ASSETS {
            // The SVG namespace is a name, not a load.
            let text =
                String::from_utf8_lossy(asset.body).replace("http://www.w3.org/2000/svg", "");
            assert!(
                !text.contains("http://") && !text.contains("https://") && !text.contains("//cdn"),
                "remote URL in {}",
                asset.path
            );
            assert!(!text.contains("innerHTML"), "innerHTML in {}", asset.path);
            assert!(
                !text.contains("insertAdjacentHTML"),
                "markup insertion in {}",
                asset.path
            );
            assert!(!text.contains("eval("), "eval in {}", asset.path);
        }
    }

    #[test]
    fn snapshots_never_carry_tokens() {
        let live = Live::new(
            RunFacts {
                id: "run".to_string(),
                preset: None,
                describe: String::new(),
                duration_s: 1.0,
                chaos_at_s: None,
                max_orders: 1,
                stock_per_store: 14,
                bake_ms: 1,
                read_method: "pizza.shop.local",
            },
            &["tenant-0".to_string()],
        );
        let snapshot = live.snapshot().to_string();
        assert!(!snapshot.to_ascii_lowercase().contains("token"));
        assert_eq!(
            live.snapshot()["audit"]["checks"].as_array().unwrap().len(),
            CHECKLIST.len()
        );
    }
}
