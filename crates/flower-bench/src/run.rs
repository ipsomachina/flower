//! One run, start to finish: binaries, servers, groups and partitions,
//! setup, warmup, the measured window with drones and chaos, the drain,
//! the audit and the report.
//!
//! Phases run in every group's kitchen at once from one synchronized start:
//! setup deploys the guest and opens the kitchens, then makes the setup
//! visible on every replica with one fresh read each (a replica-local read on
//! a lagging follower could otherwise precede it); warmup sends unmeasured
//! reads; the measured window runs customers, drones and the clumsy drone,
//! and kills a server process halfway when asked; the drain stops the clumsy
//! drone, resolves every uncertain mutation under its own request ID and
//! waits until every acknowledged order is delivered and the queues and ovens
//! are empty; the audit reads fresh and reconciles each group, then the
//! groups against each other. A first Ctrl-C ends the measured window early
//! and still drains and audits.

use std::collections::BTreeMap;
use std::io::{IsTerminal, Write};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use flower_client::CancellationToken;
use futures_util::future::{join_all, try_join_all};
use serde_json::Value;
use tokio::sync::Notify;

use crate::artifacts::{self, fingerprint};
use crate::audit::{self, AcrossInput, Audit, AuditInput, GroupSide, World};
use crate::auth::Identities;
use crate::chaos::{Chaos, ChaosEvent, ProcessKill, Target};
use crate::client::{BenchClient, ClientSpec, Member, Timing, Tokens};
use crate::cluster::{Cluster, ClusterSpec, OtelExport, random_hex};
use crate::config::{Settings, Topology};
use crate::drones::{self, Fleet, Yard};
use crate::groups::{self, Placement};
use crate::histogram::{Histogram, scheme};
use crate::kitchen::{Kitchen, MODULE};
use crate::live::{self, Finale, FloorWatch, Live, LiveServer, MemberView, RunFacts, Sources};
use crate::metrics::{self, Phase, Recorder};
use crate::report::{
    self, Binaries, ClusterInfo, Drones, Errors, GroupReport, Lifecycle, Replays, Report,
};
use crate::resources::{self, Sample};
use crate::workload::{ArrivalCounters, Arrivals, Ledger};
use crate::{compare, otel, profile};

/// A finished run.
pub struct Finished {
    /// The report, already written.
    pub report: Report,
    /// The live view, still serving.
    pub live: Option<LiveServer>,
}

/// Print a line, clearing the status line first.
fn say(text: &str) {
    let mut out = std::io::stdout().lock();
    if out.is_terminal() {
        let _ = write!(out, "\r\x1b[2K");
    }
    let _ = writeln!(out, "{text}");
}

/// UTC now as RFC 3339, seconds.
pub fn utc_now() -> String {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |time| time.as_secs()) as i64;
    let (days, rest) = (seconds.div_euclid(86_400), seconds.rem_euclid(86_400));
    // Civil date from days since 1970-01-01 (Howard Hinnant's algorithm).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        rest / 3600,
        rest % 3600 / 60,
        rest % 60
    )
}

/// Nanoseconds since the Unix epoch.
fn unix_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |time| time.as_nanos())
}

/// The servers and the kitchens on them.
struct Topo {
    clusters: Vec<Arc<Cluster>>,
    kitchens: Vec<Kitchen>,
    /// Partitions placed: (partition, group).
    planned: Vec<(String, String)>,
    /// A root client of the first cluster (catalog reads, the live
    /// view's member roles).
    root: Arc<BenchClient>,
    notes: Vec<String>,
}

/// Run the benchmark. `closing` ends the measured window early.
pub async fn run(settings: Settings, closing: CancellationToken) -> Result<Finished, String> {
    let settings = Arc::new(settings);
    let started_at = utc_now();
    let run_id = random_hex(6);
    let recorder = Arc::new(Recorder::new());
    let tenants = settings.tenant_ids();
    say(&format!("Goblin Pizza Express: {}.", settings.describe()));
    let baseline = match &settings.baseline {
        Some(path) => Some(compare::read(path)?),
        None => None,
    };

    let live = settings.live.then(|| {
        Live::new(
            RunFacts {
                id: run_id.clone(),
                preset: settings.preset,
                describe: settings.describe(),
                duration_s: settings.duration_s,
                chaos_at_s: settings.chaos.then_some(settings.duration_s / 2.0),
                max_orders: settings.max_orders,
                stock_per_store: settings.stock_per_store,
                bake_ms: settings.bake_ms,
                read_method: match settings.read_consistency {
                    crate::config::ReadConsistency::Local => "pizza.shop.local",
                    crate::config::ReadConsistency::Fresh => "pizza.shop",
                },
            },
            &tenants,
        )
    });
    let server = match &live {
        Some(live) => {
            let server = live::serve(live.clone(), settings.live_port).await?;
            say(&format!("Live view: {}", server.url()));
            if settings.open {
                live::open_browser(&server.url());
            }
            live.note("Building and starting the servers...");
            Some(server)
        }
        None => None,
    };

    // Binaries.
    let external = !settings.targets.is_empty();
    let server_binary = if external {
        None
    } else {
        Some(match &settings.server_binary {
            Some(path) => path.clone(),
            None => {
                say("Building cargo-flower...");
                tokio::task::spawn_blocking(artifacts::build_server)
                    .await
                    .map_err(|error| error.to_string())??
            }
        })
    };
    let guest = match &settings.guest_wasm {
        Some(path) => path.clone(),
        None => {
            say(if settings.auth {
                "Building the authenticated goblin-pizza guest for wasm32..."
            } else {
                "Building the goblin-pizza guest for wasm32..."
            });
            let auth = settings.auth;
            tokio::task::spawn_blocking(move || artifacts::build_guest(auth))
                .await
                .map_err(|error| error.to_string())??
        }
    };
    let wasm = std::fs::read(&guest).map_err(|error| format!("{}: {error}", guest.display()))?;
    let manifest = artifacts::manifest(&wasm)?;
    if settings.auth && manifest["authorize"] != Value::Bool(true) {
        return Err(format!(
            "--auth needs the guest built with its access rules (feature `auth`): {} has no authorization hook",
            guest.display()
        ));
    }
    let binaries = Binaries {
        server: server_binary.as_deref().map(fingerprint).transpose()?,
        guest: fingerprint(&guest)?,
        bench: std::env::current_exe()
            .ok()
            .and_then(|path| fingerprint(&path).ok()),
    };
    let identities = if settings.auth {
        Some(Arc::new(Identities::generate(&tenants)?))
    } else {
        None
    };
    let receiver = match settings.otel {
        Some(_) => Some(otel::Receiver::start(otel::Limits::from_env()?).await?),
        None => None,
    };

    let topo = start(
        &settings,
        server_binary,
        &run_id,
        identities.clone(),
        receiver.as_ref().map(otel::Receiver::endpoint),
        &recorder,
    )
    .await?;
    say("Flower's own dashboards, side by side:");
    for cluster in &topo.clusters {
        for member in cluster.members() {
            say(&format!("  n{}  {}/admin", member.id, member.url));
        }
    }
    match topo.clusters.first() {
        Some(cluster) => say(&format!(
            "  admin token: {}  (this run only; it dies with the servers)",
            cluster.admin_token()
        )),
        None => say("  admin token: your cluster's"),
    }
    for note in &topo.notes {
        say(&format!("  note: {note}"));
    }

    let context = Context {
        settings: settings.clone(),
        recorder,
        live: live.clone(),
        topo,
        closing,
        run_id,
        started_at,
        binaries,
        wasm,
        manifest,
        receiver: Mutex::new(receiver),
    };
    let result = context.drive().await;
    for cluster in &context.topo.clusters {
        cluster.shutdown().await;
    }
    let mut report = result?;
    if let Some(baseline) = &baseline {
        let candidate = serde_json::to_value(&report).map_err(|error| error.to_string())?;
        report.comparison = Some(compare::compare(
            baseline,
            &candidate,
            compare::DEFAULT_THRESHOLD,
        ));
    }
    report.write()?;
    if let Some(live) = &live {
        live.finish(Finale {
            passed: report.passed,
            goodput: report.goodput.per_second,
            write_p99: report.goodput.write_latency.percentiles.p99,
            read_p99: report.goodput.read_latency.percentiles.p99,
            report_json: report.settings.json.display().to_string(),
            report_html: report.settings.html.display().to_string(),
        });
    }
    Ok(Finished {
        report,
        live: server,
    })
}

fn timing(settings: &Settings) -> Timing {
    Timing {
        request_timeout: Duration::from_millis(settings.request_timeout_ms),
        retry_budget: Duration::from_millis(settings.retry_budget_ms),
    }
}

/// Start the servers (or address `--target`), create the groups and
/// partitions the topology needs and build each group's kitchen.
async fn start(
    settings: &Arc<Settings>,
    server_binary: Option<std::path::PathBuf>,
    run_id: &str,
    identities: Option<Arc<Identities>>,
    otel: Option<String>,
    recorder: &Arc<Recorder>,
) -> Result<Topo, String> {
    let spec = |label: String| ClusterSpec {
        binary: server_binary
            .clone()
            .expect("spawned clusters have a binary"),
        replicas: settings.replicas,
        data_parent: settings.data.clone(),
        keep_data: settings.keep_data,
        label,
        crypto: settings.auth,
        otel: otel.clone().map(|endpoint| OtelExport {
            endpoint,
            sample: settings.otel.unwrap_or(0.01),
        }),
        tls: None,
    };
    let client = |members: Vec<Member>,
                  tokens: &Tokens,
                  group: Option<String>,
                  db: String,
                  tag: &str,
                  recorder: Arc<Recorder>| {
        BenchClient::new(
            ClientSpec {
                members,
                tokens: tokens.clone(),
                timing: timing(settings),
                group,
                db,
                module: MODULE.to_string(),
                run_tag: format!("bench-{run_id}-{tag}"),
                identities: identities.clone(),
            },
            recorder,
        )
        .map(Arc::new)
    };
    let tokens_of = |cluster: &Cluster| Tokens {
        admin: Some(cluster.admin_token().to_string()),
        worker: Some(cluster.worker_token().to_string()),
    };
    let kitchen = |index: usize,
                   name: String,
                   cluster: Option<Arc<Cluster>>,
                   client: Arc<BenchClient>,
                   recorder: Arc<Recorder>,
                   dbs: BTreeMap<String, String>| {
        let tenants = settings.group_tenants(index);
        let databases = {
            let mut seen: Vec<&String> = Vec::new();
            for tenant in &tenants {
                if let Some(db) = dbs.get(tenant)
                    && !seen.contains(&db)
                {
                    seen.push(db);
                }
            }
            seen.len().max(1)
        };
        Kitchen {
            index,
            name,
            cluster,
            client,
            recorder,
            seed: settings.group_seed(index),
            dbs: Arc::new(dbs),
            tenants,
            ledger: Arc::new(Ledger::default()),
            yard: Arc::new(Yard::new(
                fleet_size(settings.workers, databases) * databases,
                Duration::from_millis(settings.lease_ms),
            )),
            arrivals: Arc::new(ArrivalCounters::default()),
        }
    };
    let every = |db: &str, index: usize| -> BTreeMap<String, String> {
        settings
            .group_tenants(index)
            .into_iter()
            .map(|tenant| (tenant, db.to_string()))
            .collect()
    };

    if !settings.targets.is_empty() {
        let members: Vec<Member> = settings
            .targets
            .iter()
            .enumerate()
            .map(|(index, url)| Member {
                id: index as u64 + 1,
                url: url.trim_end_matches('/').to_string(),
            })
            .collect();
        let tokens = Tokens {
            admin: settings.admin_token.clone(),
            worker: settings
                .worker_token
                .clone()
                .or_else(|| settings.admin_token.clone()),
        };
        let db = format!("pizza-{run_id}");
        let group_recorder = Arc::new(Recorder::child(recorder));
        let root = client(
            members.clone(),
            &tokens,
            None,
            db.clone(),
            "root",
            group_recorder.clone(),
        )?;
        return Ok(Topo {
            clusters: Vec::new(),
            kitchens: vec![kitchen(
                0,
                "root".to_string(),
                None,
                root.clone(),
                group_recorder,
                every(&db, 0),
            )],
            planned: Vec::new(),
            root,
            notes: Vec::new(),
        });
    }

    match settings.topology {
        Topology::Single => {
            say(&format!(
                "Starting {} server process(es)...",
                settings.replicas
            ));
            let cluster = Arc::new(Cluster::start(&spec(String::new())).await?);
            let tokens = tokens_of(&cluster);
            let group_recorder = Arc::new(Recorder::child(recorder));
            let root = client(
                cluster.members(),
                &tokens,
                None,
                "pizza".to_string(),
                "root",
                group_recorder.clone(),
            )?;
            Ok(Topo {
                kitchens: vec![kitchen(
                    0,
                    "root".to_string(),
                    Some(cluster.clone()),
                    root.clone(),
                    group_recorder,
                    every("pizza", 0),
                )],
                clusters: vec![cluster],
                planned: Vec::new(),
                root,
                notes: Vec::new(),
            })
        }
        Topology::Separate => {
            say(&format!(
                "Starting {} separate cluster(s) of {} process(es)...",
                settings.groups, settings.replicas
            ));
            let clusters: Vec<Arc<Cluster>> = try_join_all(
                (0..settings.groups as usize)
                    .map(|index| Cluster::start_owned(spec(groups::group_name(index)))),
            )
            .await?
            .into_iter()
            .map(Arc::new)
            .collect();
            let mut kitchens = Vec::new();
            for (index, cluster) in clusters.iter().enumerate() {
                let name = groups::group_name(index);
                let group_recorder = Arc::new(Recorder::child(recorder));
                let group_client = client(
                    cluster.members(),
                    &tokens_of(cluster),
                    None,
                    "pizza".to_string(),
                    &name,
                    group_recorder.clone(),
                )?;
                kitchens.push(kitchen(
                    index,
                    name,
                    Some(cluster.clone()),
                    group_client,
                    group_recorder,
                    every("pizza", index),
                ));
            }
            let root = kitchens[0].client.clone();
            Ok(Topo {
                clusters,
                kitchens,
                planned: Vec::new(),
                root,
                notes: Vec::new(),
            })
        }
        Topology::Hosted => {
            say(&format!(
                "Starting {} server process(es) to host {} group(s)...",
                settings.replicas, settings.groups
            ));
            let cluster = Arc::new(Cluster::start(&spec(String::new())).await?);
            let tokens = tokens_of(&cluster);
            let root = client(
                cluster.members(),
                &tokens,
                None,
                "pizza".to_string(),
                "catalog",
                Arc::new(Recorder::child(recorder)),
            )?;
            let names: Vec<String> = (0..settings.groups as usize)
                .map(groups::group_name)
                .collect();
            let ids: Vec<u64> = cluster.members().iter().map(|member| member.id).collect();
            say(&format!(
                "Creating {} on every process, spreading their leaders and placing {} tenant partitions...",
                names.join(", "),
                settings.tenant_ids().len()
            ));
            groups::create_groups(&root, &names, &ids).await?;
            let notes = groups::spread_leaders(&root, &names).await;
            let planned: Vec<(String, String)> = (0..settings.groups as usize)
                .flat_map(|index| {
                    settings
                        .group_tenants(index)
                        .into_iter()
                        .map(move |tenant| (groups::tenant_db(&tenant), groups::group_name(index)))
                })
                .collect();
            groups::place(&root, &planned).await?;
            let mut kitchens = Vec::new();
            for (index, name) in names.iter().enumerate() {
                let dbs: BTreeMap<String, String> = settings
                    .group_tenants(index)
                    .into_iter()
                    .map(|tenant| {
                        let db = groups::tenant_db(&tenant);
                        (tenant, db)
                    })
                    .collect();
                let first = dbs.values().next().cloned().unwrap_or_default();
                let group_recorder = Arc::new(Recorder::child(recorder));
                let group_client = client(
                    cluster.members(),
                    &tokens,
                    Some(name.clone()),
                    first,
                    name,
                    group_recorder.clone(),
                )?;
                kitchens.push(kitchen(
                    index,
                    name.clone(),
                    Some(cluster.clone()),
                    group_client,
                    group_recorder,
                    dbs,
                ));
            }
            Ok(Topo {
                clusters: vec![cluster],
                kitchens,
                planned,
                root,
                notes,
            })
        }
    }
}

/// Drone handlers per database: the group's `--workers` split over its
/// databases, at least one each.
fn fleet_size(workers: usize, databases: usize) -> usize {
    workers.div_ceil(databases.max(1)).max(1)
}

/// Everything a run's phases share.
struct Context {
    settings: Arc<Settings>,
    recorder: Arc<Recorder>,
    live: Option<Arc<Live>>,
    topo: Topo,
    closing: CancellationToken,
    run_id: String,
    started_at: String,
    binaries: Binaries,
    wasm: Vec<u8>,
    manifest: Value,
    receiver: Mutex<Option<otel::Receiver>>,
}

/// What a chaos task did: the kill, and each group's view of it.
type Struck = (ProcessKill, Vec<ChaosEvent>);

/// What one group's drain and audit produced.
struct GroupOutcome {
    audit: Audit,
    world: World,
    fences: Vec<drones::FenceProbe>,
    worker: BTreeMap<String, u64>,
}

impl Context {
    fn note(&self, text: &str) {
        if let Some(live) = &self.live {
            live.note(text);
        }
    }

    fn kitchens(&self) -> &[Kitchen] {
        &self.topo.kitchens
    }

    /// Enter `phase` at `now` in the run's and every group's accounting.
    fn enter(&self, phase: Phase, now: Instant) {
        self.recorder.enter(phase, now);
        for kitchen in self.kitchens() {
            kitchen.recorder.enter(phase, now);
        }
    }

    async fn drive(&self) -> Result<Report, String> {
        let settings = &self.settings;
        if let Some(live) = &self.live {
            live.attach(Sources {
                recorder: self.recorder.clone(),
                groups: self
                    .kitchens()
                    .iter()
                    .map(|kitchen| (kitchen.ledger.clone(), kitchen.yard.clone()))
                    .collect(),
            });
        }
        let background = CancellationToken::new();
        // Background tasks stop however this returns.
        let _stop_background = background.clone().drop_guard();
        let samples: Arc<Mutex<BTreeMap<String, Vec<Sample>>>> = Arc::default();
        let mut tasks = vec![self.ticker(background.clone(), samples.clone())];
        if let Some(live) = &self.live {
            tasks.push(self.status_poller(live.clone(), background.clone()));
        }

        // Setup, in every group at once.
        self.note("Deploying the Goblin Pizza guest...");
        try_join_all(
            self.kitchens()
                .iter()
                .map(|kitchen| kitchen.setup(settings, &self.wasm, &self.manifest)),
        )
        .await?;
        let access: Vec<Option<Vec<audit::AccessProbe>>> = if settings.auth {
            self.note("Checking the access rules...");
            join_all(self.kitchens().iter().map(Kitchen::probe_access))
                .await
                .into_iter()
                .map(Some)
                .collect()
        } else {
            vec![None; self.kitchens().len()]
        };
        if let Some(live) = &self.live {
            let watches = self
                .kitchens()
                .iter()
                .flat_map(|kitchen| {
                    kitchen.tenants.iter().map(|tenant| FloorWatch {
                        client: kitchen.client.clone(),
                        db: kitchen.db(tenant).to_string(),
                        tenant: tenant.clone(),
                    })
                })
                .collect();
            tasks.extend(live::watch_floor(live.clone(), watches, background.clone()));
        }
        let customers: Vec<_> = self
            .kitchens()
            .iter()
            .map(|kitchen| kitchen.customers(settings))
            .collect();

        // The CPU profile attaches now, so loading its symbols never
        // eats into the window; it samples from the window's start.
        let profiling = settings.cpu_profile.as_ref().map(|profile| {
            let target = self.profile_target();
            say(&format!(
                "Sampling the stacks of n{} (pid {}) for {} s from the start of the window...",
                target.1,
                target.0.map_or("?".to_string(), |pid| pid.to_string()),
                profile.seconds
            ));
            profile::start(target.0, target.1, profile.clone())
        });

        // Warmup.
        if settings.warmup_s > 0.0 && !self.closing.is_cancelled() {
            self.note("Warming up: unmeasured reads on every replica.");
            self.enter(Phase::Warmup, Instant::now());
            let deadline = Instant::now() + Duration::from_secs_f64(settings.warmup_s);
            join_all(
                self.kitchens()
                    .iter()
                    .zip(&customers)
                    .map(|(kitchen, customers)| {
                        customers
                            .clone()
                            .warm_up(kitchen.shops(settings.stores), deadline)
                    }),
            )
            .await;
        }

        // The measured window.
        let start = Instant::now();
        let load_started_unix = unix_nanos();
        self.enter(Phase::Load, start);
        self.note("Rush hour!");
        let deadline = start + Duration::from_secs_f64(settings.duration_s);
        let stop_fumbling = CancellationToken::new();
        let mut flying = Vec::new();
        let mut fumbling = Vec::new();
        for (index, kitchen) in self.kitchens().iter().enumerate() {
            let databases = kitchen.databases();
            for db in &databases {
                let fleet = Fleet {
                    client: kitchen.client.clone(),
                    db: db.clone(),
                    tenants: kitchen.tenants_in(db),
                    ledger: kitchen.ledger.clone(),
                    yard: kitchen.yard.clone(),
                    workers: fleet_size(settings.workers, databases.len()),
                    lease: Duration::from_millis(settings.lease_ms),
                    poll: Duration::from_millis(settings.poll_ms),
                    duplicate_rate: settings.duplicate_rate,
                    abandon_rate: settings.abandon_rate,
                    seed: format!("{}:{db}", kitchen.seed),
                };
                // One stop signal per fleet: `notify_one` wakes one.
                let stop = Arc::new(Notify::new());
                flying.push((
                    index,
                    fleet.clone(),
                    stop.clone(),
                    tokio::spawn(fleet.clone().fly(stop)),
                ));
                fumbling.push(tokio::spawn(fleet.fumble(stop_fumbling.clone())));
            }
        }
        let chaos = self.chaos(start);
        let profiling = profiling.map(|(go, task)| {
            go.go();
            task
        });
        join_all(
            customers
                .iter()
                .map(|customers| self.load(customers.clone(), start, deadline)),
        )
        .await;
        let interrupted = self.closing.is_cancelled();

        // The drain.
        self.enter(Phase::Drain, Instant::now());
        let load_ended_unix = unix_nanos();
        self.note("Closing time: the drones finish every acknowledged order.");
        stop_fumbling.cancel();
        for task in fumbling {
            let _ = task.await;
        }
        let drain = Duration::from_secs_f64(settings.drain_s);
        let settled = join_all(self.kitchens().iter().map(|kitchen| async move {
            let undecided = kitchen.resolve().await;
            let drained = kitchen.wait_drained(drain).await;
            (undecided, drained)
        }))
        .await;
        let mut events: Vec<Vec<ChaosEvent>> = vec![Vec::new(); self.kitchens().len()];
        let mut kill: Option<ProcessKill> = None;
        for handle in chaos {
            if let Ok(Some((process, measured))) = handle.await {
                for event in measured {
                    if let Some(index) = self
                        .kitchens()
                        .iter()
                        .position(|kitchen| kitchen.name == event.group)
                    {
                        events[index].push(event);
                    }
                }
                if settings.topology == Topology::Hosted && settings.groups > 1 {
                    kill = Some(process);
                }
            }
        }
        let mut workers: Vec<BTreeMap<String, u64>> = vec![BTreeMap::new(); self.kitchens().len()];
        for (index, fleet, stop, handle) in flying {
            stop.notify_one();
            match handle.await {
                Ok(Ok(summary)) => {
                    fleet.lost(summary.lost);
                    let worker = &mut workers[index];
                    for (name, value) in [
                        ("completed", summary.completed),
                        ("failed", summary.failed),
                        ("killed", summary.killed),
                        ("lost", summary.lost),
                    ] {
                        *worker.entry(name.to_string()).or_default() += value;
                    }
                }
                Ok(Err(error)) => return Err(format!("the drone fleet stopped: {error}")),
                Err(error) => return Err(format!("the drone fleet panicked: {error}")),
            }
        }

        // The audit.
        self.enter(Phase::Audit, Instant::now());
        self.note("The goblin accountants are checking the books...");
        if let Some(live) = &self.live {
            live.auditing();
        }
        let outcomes = try_join_all(
            self.kitchens()
                .iter()
                .zip(settled)
                .zip(access)
                .zip(workers)
                .map(|(((kitchen, (undecided, drained)), access), worker)| {
                    self.audit_group(kitchen, undecided, drained, access, worker)
                }),
        )
        .await?;
        let several = self.kitchens().len() > 1 || settings.topology == Topology::Hosted;
        let across = if several {
            let placements = if self.topo.planned.is_empty() {
                None
            } else {
                groups::placements(&self.topo.root).await.ok()
            };
            Some(audit::across(&AcrossInput {
                groups: self
                    .kitchens()
                    .iter()
                    .zip(&outcomes)
                    .map(|(kitchen, outcome)| GroupSide {
                        name: kitchen.name.clone(),
                        tenants: kitchen.tenants.clone(),
                        world: outcome.world.clone(),
                        passed: outcome.audit.passed,
                    })
                    .collect(),
                planned: self.topo.planned.clone(),
                placements: placements.clone(),
            }))
        } else {
            None
        };
        let placements: Vec<Placement> = if self.topo.planned.is_empty() {
            Vec::new()
        } else {
            groups::placements(&self.topo.root)
                .await
                .unwrap_or_default()
        };
        if let Some(live) = &self.live {
            let audits: Vec<&Audit> = outcomes.iter().map(|outcome| &outcome.audit).collect();
            let mut shown = audit::combine(&audits).unwrap_or_else(|| outcomes[0].audit.clone());
            if let Some(across) = &across {
                shown.checks.extend(across.checks.iter().cloned());
                shown.passed &= across.passed;
            }
            live.audited(shown.clone());
            self.note(if shown.passed {
                "The books balance. Every check passed."
            } else {
                "The books do not balance: see the failed checks."
            });
        }

        // Diagnostics: the profile, then the telemetry the servers
        // exported (given a moment for their last batches).
        let cpu_profile = match profiling {
            Some(task) => Some(task.await.map_err(|error| error.to_string())?),
            None => None,
        };
        let receiver = self
            .receiver
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        let otel = match receiver {
            Some(receiver) => {
                tokio::time::sleep(Duration::from_millis(2_500)).await;
                let capture = receiver.stop().await;
                let window = otel::Window {
                    start_unix_ns: load_started_unix,
                    end_unix_ns: load_ended_unix,
                };
                let mut summary = otel::summarize(&capture, window);
                let dir = otel::directory(&settings.json);
                summary.files = Some(dir.display().to_string());
                if let Err(error) = otel::write(&summary, &capture, &dir) {
                    say(&format!("OpenTelemetry files: {error}"));
                }
                Some(summary)
            }
            None => None,
        };
        background.cancel();
        for task in tasks {
            let _ = task.await;
        }

        // The report.
        let now = Instant::now();
        let phases = self.recorder.phases(now);
        let load = phases
            .iter()
            .find(|phase| phase.phase == Phase::Load)
            .cloned()
            .ok_or("the run never reached its measured window")?;
        let goodput = metrics::goodput(&load);
        let mut failed: Vec<String> = Vec::new();
        for (kitchen, outcome) in self.kitchens().iter().zip(&outcomes) {
            for check in outcome.audit.checks.iter().filter(|check| !check.passed) {
                failed.push(if self.kitchens().len() > 1 {
                    format!("{} {}", kitchen.name, check.id)
                } else {
                    check.id.to_string()
                });
            }
        }
        if let Some(across) = &across {
            for check in across.checks.iter().filter(|check| !check.passed) {
                failed.push(check.id.to_string());
            }
        }
        if let Some(otel) = &otel
            && !otel.passed
        {
            failed.push(format!("otel ({})", otel.problems.join("; ")));
        }
        if let Some(profile) = &cpu_profile
            && !profile.passed()
        {
            failed.push(format!(
                "cpu-profile ({})",
                profile
                    .error
                    .clone()
                    .unwrap_or_else(|| profile.status.clone())
            ));
        }
        let passed = failed.is_empty();
        let verdict = if passed {
            "every audit check held".to_string()
        } else {
            format!("failed: {}", failed.join(", "))
        };
        let samples = samples
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let views: Vec<_> = self
            .kitchens()
            .iter()
            .map(|kitchen| kitchen.ledger.view())
            .collect();
        let offered_load = (settings.offered_rate > 0.0).then(|| {
            merge_arrivals(
                &self
                    .kitchens()
                    .iter()
                    .map(|kitchen| kitchen.arrivals.snapshot())
                    .collect::<Vec<_>>(),
            )
        });
        let mut world = World::default();
        for outcome in &outcomes {
            world.shops.extend(outcome.world.shops.iter().cloned());
            world.orders.extend(outcome.world.orders.iter().cloned());
        }
        let groups = self
            .kitchens()
            .iter()
            .zip(outcomes)
            .zip(events)
            .map(|((kitchen, outcome), chaos)| {
                let phases = kitchen.recorder.phases(now);
                let goodput = phases
                    .iter()
                    .find(|phase| phase.phase == Phase::Load)
                    .map(metrics::goodput);
                GroupReport {
                    index: kitchen.index as u32,
                    name: kitchen.name.clone(),
                    tenants: kitchen.tenants.clone(),
                    goodput,
                    offered_load: (settings.offered_rate > 0.0)
                        .then(|| kitchen.arrivals.snapshot()),
                    cluster: ClusterInfo {
                        mode: match (&kitchen.cluster, settings.topology) {
                            (None, _) => "external",
                            (Some(_), Topology::Single) => "spawned",
                            (Some(_), Topology::Hosted) => "hosted",
                            (Some(_), Topology::Separate) => "separate",
                        },
                        members: kitchen.client.members().to_vec(),
                        server_args: kitchen
                            .cluster
                            .as_ref()
                            .map_or_else(Vec::new, |cluster| cluster.server_args()),
                        databases: kitchen.databases(),
                        data_dir: kitchen
                            .cluster
                            .as_ref()
                            .filter(|cluster| cluster.keeps_data())
                            .map(|cluster| cluster.data_dir().display().to_string()),
                    },
                    chaos,
                    audit: Some(outcome.audit),
                    drones: Drones {
                        counters: kitchen.yard.counters(),
                        worker: outcome.worker,
                        fences: outcome.fences,
                    },
                    routing: kitchen.client.routing(),
                }
            })
            .collect();
        Ok(Report {
            schema: report::SCHEMA,
            run_id: self.run_id.clone(),
            started_at: self.started_at.clone(),
            finished_at: utc_now(),
            passed,
            interrupted,
            verdict,
            settings: (*self.settings).clone(),
            machine: artifacts::machine(),
            binaries: self.binaries.clone(),
            goodput,
            offered_load,
            histograms: scheme(),
            errors: Errors {
                by_kind: metrics::errors_by_kind(&load),
                attempts: load.operations.values().map(|op| op.attempts).sum(),
                retries: load.operations.values().map(|op| op.retries).sum(),
                customer_failures: views.iter().map(|view| view.failures).sum(),
                samples: views
                    .iter()
                    .flat_map(|view| view.samples.iter().cloned())
                    .take(50)
                    .collect(),
            },
            replays: Replays {
                checks: views.iter().map(|view| view.replay_checks).sum(),
                mismatches: views
                    .iter()
                    .flat_map(|view| view.mismatches.iter().cloned())
                    .collect(),
            },
            phases,
            timeline: self.recorder.timeline(),
            resources: resources::summaries(&samples),
            lifecycle: Some(lifecycle(&world)),
            groups,
            across,
            kill,
            placements,
            otel,
            cpu_profile,
            comparison: None,
            notes: report::notes(&self.topo.notes, settings.diagnostic()),
        })
    }

    /// One group's customers through the measured window.
    async fn load(
        &self,
        customers: Arc<crate::workload::Customers>,
        start: Instant,
        deadline: Instant,
    ) {
        if self.settings.offered_rate > 0.0 {
            customers
                .open_loop(start, deadline, self.closing.clone())
                .await;
        } else {
            customers.closed_loop(deadline, self.closing.clone()).await;
        }
    }

    /// The chaos tasks: one per cluster, each killing the process that
    /// leads the most of the groups it hosts halfway through.
    fn chaos(&self, start: Instant) -> Vec<tokio::task::JoinHandle<Option<Struck>>> {
        if !self.settings.chaos {
            return Vec::new();
        }
        let at = start + Duration::from_secs_f64(self.settings.duration_s / 2.0);
        self.topo
            .clusters
            .iter()
            .map(|cluster| {
                let targets = self
                    .kitchens()
                    .iter()
                    .filter(|kitchen| {
                        kitchen
                            .cluster
                            .as_ref()
                            .is_some_and(|own| Arc::ptr_eq(own, cluster))
                    })
                    .map(|kitchen| Target {
                        index: kitchen.index,
                        group: kitchen.name.clone(),
                        client: kitchen.client.clone(),
                        recorder: kitchen.recorder.clone(),
                    })
                    .collect();
                let chaos = Chaos {
                    cluster: cluster.clone(),
                    targets,
                    live: self.live.clone(),
                    load_started: start,
                };
                let closing = self.closing.clone();
                tokio::spawn(async move {
                    tokio::select! {
                        () = closing.cancelled() => None,
                        () = tokio::time::sleep_until(at.into()) => Some(chaos.strike(at).await),
                    }
                })
            })
            .collect()
    }

    /// The process the CPU profile samples: the one leading the most
    /// groups at the start of the window (its pid, its member ID).
    fn profile_target(&self) -> (Option<u32>, u64) {
        let Some(cluster) = self.topo.clusters.first() else {
            return (None, 0);
        };
        let leaders: Vec<Option<usize>> = self
            .kitchens()
            .iter()
            .filter(|kitchen| {
                kitchen
                    .cluster
                    .as_ref()
                    .is_some_and(|own| Arc::ptr_eq(own, cluster))
            })
            .map(|kitchen| Some(kitchen.client.leader()))
            .collect();
        let member = crate::chaos::busiest(&leaders, cluster.members().len()).unwrap_or(0);
        (cluster.pid(member), cluster.members()[member].id)
    }

    /// One group's audit: fences, replays, fresh reads, then the checks.
    async fn audit_group(
        &self,
        kitchen: &Kitchen,
        undecided: usize,
        drained: bool,
        access: Option<Vec<audit::AccessProbe>>,
        worker: BTreeMap<String, u64>,
    ) -> Result<GroupOutcome, String> {
        let settings = &self.settings;
        let fences = drones::probe_fences(&kitchen.client, &kitchen.yard.abandoned()).await;
        kitchen.replay_receipts().await;
        let world = kitchen.fetch_world().await?;
        let leaderboards = kitchen.leaderboards().await?;
        let leftovers = kitchen.leftovers().await;
        let replicas = kitchen.replicas(&world, settings.stores).await;
        let view = kitchen.ledger.view();
        let input = AuditInput {
            tenants: kitchen.tenants.clone(),
            stores: settings.stores,
            stock_per_store: settings.stock_per_store,
            bake_ms: settings.bake_ms,
            world: world.clone(),
            leaderboards,
            acknowledged: view.orders.clone(),
            tips: view.tips.clone(),
            replay_checks: view.replay_checks,
            mismatches: view.mismatches.clone(),
            replays_expected: settings.duplicate_rate > 0.0 && !view.orders.is_empty(),
            fences: fences.clone(),
            abandon_expected: settings.abandon_rate > 0.0 && !view.orders.is_empty(),
            leftovers,
            drained,
            undecided,
            replicas,
            customer_failures: view.failures,
            access,
        };
        Ok(GroupOutcome {
            audit: audit::audit(&input),
            world,
            fences,
            worker,
        })
    }

    /// Every second: close the timeline intervals, sample processes and
    /// print the status line.
    fn ticker(
        &self,
        stop: CancellationToken,
        samples: Arc<Mutex<BTreeMap<String, Vec<Sample>>>>,
    ) -> tokio::task::JoinHandle<()> {
        let recorder = self.recorder.clone();
        let recorders: Vec<Arc<Recorder>> = self
            .kitchens()
            .iter()
            .map(|kitchen| kitchen.recorder.clone())
            .collect();
        let clusters = self.topo.clusters.clone();
        let ledgers: Vec<Arc<Ledger>> = self
            .kitchens()
            .iter()
            .map(|kitchen| kitchen.ledger.clone())
            .collect();
        let yards: Vec<Arc<Yard>> = self
            .kitchens()
            .iter()
            .map(|kitchen| kitchen.yard.clone())
            .collect();
        let first = self.kitchens()[0].client.clone();
        let quiet = self.settings.quiet;
        let max_orders = self.settings.max_orders * self.kitchens().len() as u64;
        let groups = self.kitchens().len();
        tokio::spawn(async move {
            let mut ticks = tokio::time::interval(Duration::from_secs(1));
            ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            ticks.tick().await;
            let terminal = std::io::stdout().is_terminal();
            let mut printed = Instant::now();
            loop {
                tokio::select! {
                    () = stop.cancelled() => break,
                    _ = ticks.tick() => {}
                }
                let now = Instant::now();
                let point = recorder.tick(now);
                for group in &recorders {
                    group.tick(now);
                }
                let phase = recorder.phase();
                let mut taken = Vec::new();
                for (cluster_index, cluster) in clusters.iter().enumerate() {
                    for (index, (pid, usage)) in cluster.usage() {
                        let name = if clusters.len() == 1 {
                            format!("n{}", index + 1)
                        } else {
                            format!("{}-n{}", groups::group_name(cluster_index), index + 1)
                        };
                        taken.push((name, pid, usage));
                    }
                }
                if let Some(usage) = resources::own_usage() {
                    taken.push(("bench".to_string(), std::process::id(), usage));
                }
                {
                    let mut samples = samples.lock().unwrap_or_else(PoisonError::into_inner);
                    for (name, pid, usage) in taken {
                        samples.entry(name).or_default().push(Sample {
                            t: point.t,
                            phase,
                            pid,
                            cpu_ms: usage.cpu_ms,
                            rss_bytes: usage.rss_bytes,
                        });
                    }
                }
                if quiet || (!terminal && printed.elapsed() < Duration::from_secs(5)) {
                    continue;
                }
                printed = now;
                let delivered: u64 = yards.iter().map(|yard| yard.counters().delivered).sum();
                let acknowledged: usize = ledgers
                    .iter()
                    .map(|ledger| ledger.acknowledged_orders())
                    .sum();
                let leader = if groups == 1 {
                    format!("leader n{}", first.members()[first.leader()].id)
                } else {
                    format!("{groups} groups")
                };
                let line = format!(
                    "[{} {:>5.1}s] goodput {:>8}/s · write p99 {:>8} · read p99 {:>8} · orders {}/{} · delivered {} · retries {:.0}/s · failed {:.0}/s · {leader}",
                    phase.name(),
                    point.t,
                    report::rate(point.reads_per_second + point.writes_per_second),
                    report::ms(point.write_p99),
                    report::ms(point.read_p99),
                    acknowledged,
                    max_orders,
                    delivered,
                    point.retries_per_second,
                    point.failed_attempts_per_second,
                );
                let mut out = std::io::stdout().lock();
                if terminal {
                    let _ = write!(out, "\r\x1b[2K{line}");
                    let _ = out.flush();
                } else {
                    let _ = writeln!(out, "{line}");
                }
            }
            if terminal && !quiet {
                let mut out = std::io::stdout().lock();
                let _ = write!(out, "\r\x1b[2K");
                let _ = out.flush();
            }
        })
    }

    /// Four times a second: every member's role, for the live view. Its
    /// role and the crown are the first group's; with several groups,
    /// each member also lists the groups it leads.
    fn status_poller(
        &self,
        live: Arc<Live>,
        stop: CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        let client = self.kitchens()[0].client.clone();
        let others: Vec<Arc<BenchClient>> = if self.settings.topology == Topology::Hosted {
            self.kitchens()
                .iter()
                .map(|kitchen| kitchen.client.clone())
                .collect()
        } else {
            Vec::new()
        };
        tokio::spawn(async move {
            while !stop.is_cancelled() {
                let mut views = Vec::new();
                for (index, member) in client.members().iter().enumerate() {
                    let status = client.status(index, Duration::from_millis(300)).await.ok();
                    let mut leads = Vec::new();
                    if others.len() > 1 {
                        for group in &others {
                            if let Ok(status) =
                                group.status(index, Duration::from_millis(300)).await
                                && status.state == "Leader"
                            {
                                leads.push(group.group().unwrap_or("root").to_string());
                            }
                        }
                    }
                    views.push(MemberView {
                        id: member.id,
                        url: member.url.clone(),
                        admin_url: format!("{}/admin", member.url),
                        role: status.as_ref().map_or("down".to_string(), |status| {
                            match status.state.as_str() {
                                "Leader" => "leader",
                                "Follower" => "follower",
                                "Candidate" => "candidate",
                                "Learner" => "learner",
                                _ => "unknown",
                            }
                            .to_string()
                        }),
                        term: status.as_ref().map(|status| status.term),
                        applied: status.as_ref().and_then(|status| status.last_applied),
                        leads,
                    });
                }
                live.members(views);
                tokio::select! {
                    () = stop.cancelled() => break,
                    () = tokio::time::sleep(Duration::from_millis(250)) => {}
                }
            }
        })
    }
}

/// Every group's open-loop counters, summed; lag histograms merged by
/// bucket.
fn merge_arrivals(all: &[Arrivals]) -> Arrivals {
    let mut lag = Histogram::default();
    let mut total = Arrivals::default();
    for arrivals in all {
        total.offered += arrivals.offered;
        total.dispatched += arrivals.dispatched;
        total.driver_dropped += arrivals.driver_dropped;
        total.completed += arrivals.completed;
        total.failed += arrivals.failed;
        if let Some(histogram) = Histogram::from_snapshot(&arrivals.scheduling_lag_ms) {
            lag.merge(&histogram);
        }
    }
    total.scheduling_lag_ms = lag.snapshot();
    total
}

/// Oven lateness and order-to-door times from the source rows.
fn lifecycle(world: &World) -> Lifecycle {
    let mut lateness = Histogram::default();
    let mut door = Histogram::default();
    for order in &world.orders {
        let field = |name: &str| order[name].as_f64();
        if let (Some(due), Some(ready)) = (field("dueAt"), field("readyAt")) {
            lateness.record(ready - due);
        }
        if let (Some(created), Some(delivered)) = (field("createdAt"), field("deliveredAt")) {
            door.record(delivered - created);
        }
    }
    Lifecycle {
        oven_lateness_ms: lateness.snapshot(),
        order_to_door_ms: door.snapshot(),
    }
}

/// The final scorecard, for the terminal.
pub fn scorecard(report: &Report) -> String {
    let mut lines = Vec::new();
    let goodput = &report.goodput;
    lines.push(String::new());
    lines.push(format!(
        "{}  Goblin Pizza Express: {}",
        if report.passed { "PASS" } else { "FAIL" },
        report.verdict
    ));
    lines.push(format!(
        "  goodput        {} calls/s ({} reads, {} orders and tips; {:.0}% reads)",
        report::rate(goodput.per_second),
        report::count(goodput.reads.completed),
        report::count(goodput.mutations.completed),
        goodput.reads.fraction.unwrap_or(0.0) * 100.0
    ));
    let p = &goodput.write_latency.percentiles;
    lines.push(format!(
        "  writes         p50 {} · p90 {} · p99 {} · p99.9 {} · max {}",
        report::ms(p.p50),
        report::ms(p.p90),
        report::ms(p.p99),
        report::ms(p.p999),
        report::ms(p.max)
    ));
    let p = &goodput.read_latency.percentiles;
    lines.push(format!(
        "  reads          p50 {} · p90 {} · p99 {} · p99.9 {} · max {}",
        report::ms(p.p50),
        report::ms(p.p90),
        report::ms(p.p99),
        report::ms(p.p999),
        report::ms(p.max)
    ));
    if let Some(offered) = &report.offered_load {
        lines.push(format!(
            "  arrivals       {} offered · {} dispatched · {} driver drops",
            report::count(offered.offered),
            report::count(offered.dispatched),
            report::count(offered.driver_dropped)
        ));
    }
    lines.push(format!(
        "  errors         {} failed attempts, {} retries in the window; {} failed customer calls",
        report::count(report.errors.by_kind.values().sum()),
        report::count(report.errors.retries),
        report::count(report.errors.customer_failures)
    ));
    if let Some(kill) = &report.kill {
        lines.push(format!(
            "  chaos          n{} killed at {:.1}s: it led {} of {} groups ({})",
            kill.killed,
            kill.killed_at_s,
            kill.led.len(),
            kill.groups,
            kill.led.join(", ")
        ));
    }
    let several = report.groups.len() > 1;
    for group in &report.groups {
        for event in &group.chaos {
            if several && !event.led {
                continue;
            }
            lines.push(format!(
                "  chaos{}n{} killed at {:.1}s: new leader in {}, writes back after {} (failover), caught up after {}",
                if several {
                    format!(" {:<9}", group.name)
                } else {
                    " ".repeat(10)
                },
                event.killed,
                event.killed_at_s,
                report::ms(event.election_ms),
                report::ms(event.failover_ms),
                report::ms(event.caught_up_ms)
            ));
        }
    }
    let audits: Vec<&Audit> = report
        .groups
        .iter()
        .filter_map(|group| group.audit.as_ref())
        .collect();
    if let Some(audit) = audit::combine(&audits) {
        lines.push(format!(
            "  kitchen        {} orders, {} pizzas delivered, {} coins earned, {} tipped; {} receipt replays; {} stale drone tokens refused",
            report::count(audit.totals.orders as u64),
            report::count(audit.totals.pizzas),
            report::count(audit.totals.revenue),
            report::count(audit.totals.tips),
            report::count(report.replays.checks),
            report
                .groups
                .iter()
                .flat_map(|group| &group.drones.fences)
                .filter(|probe| probe.rejected)
                .count()
        ));
        lines.push(if several {
            format!(
                "  audit          (every check across {} groups)",
                audits.len()
            )
        } else {
            "  audit".to_string()
        });
        let checks = if several {
            &audit.checks
        } else {
            &audits[0].checks
        };
        for check in checks {
            lines.push(format!(
                "    {} {:<48} {}",
                if check.passed { "ok  " } else { "FAIL" },
                check.title,
                check.detail
            ));
            for violation in check.violations.iter().take(5) {
                lines.push(format!("           {violation}"));
            }
        }
    }
    if let Some(across) = &report.across {
        for check in &across.checks {
            lines.push(format!(
                "    {} {:<48} {}",
                if check.passed { "ok  " } else { "FAIL" },
                check.title,
                check.detail
            ));
            for violation in check.violations.iter().take(5) {
                lines.push(format!("           {violation}"));
            }
        }
    }
    for (name, summary) in &report.resources {
        lines.push(format!(
            "  {name:<14} CPU {} cores over {:.0}s · peak RSS {}",
            summary
                .load_cpu_cores
                .map_or("—".to_string(), |cores| format!("{cores:.2}")),
            summary.covered_seconds,
            summary
                .peak_rss_bytes
                .map_or("—".to_string(), |bytes| format!(
                    "{:.0} MiB",
                    bytes as f64 / 1_048_576.0
                ))
        ));
    }
    if let Some(cpu) = compare::cpu_per_call(report) {
        lines.push(format!(
            "  cpu / call     servers {} µs · bench {} µs per successful customer call",
            cpu.servers
                .map_or("—".to_string(), |value| format!("{value:.1}")),
            cpu.bench
                .map_or("—".to_string(), |value| format!("{value:.1}"))
        ));
    }
    if let Some(otel) = &report.otel {
        lines.push(format!(
            "  otel           {} spans and {} metric points from {} processes{}",
            report::count(otel.spans.received),
            report::count(otel.metrics.points),
            otel.resources.len(),
            if otel.passed {
                String::new()
            } else {
                format!(" · FAILED: {}", otel.problems.join("; "))
            }
        ));
    }
    if let Some(profile) = &report.cpu_profile {
        lines.push(format!(
            "  cpu profile    {} · {} stack samples of n{} · {}",
            profile.status,
            report::count(profile.samples),
            profile.member,
            profile.raw_path
        ));
    }
    if let Some(comparison) = &report.comparison {
        lines.push(format!("  vs baseline    {}", comparison.headline()));
    }
    if let Some(dir) = report
        .groups
        .iter()
        .find_map(|group| group.cluster.data_dir.as_ref())
    {
        lines.push(format!("  data kept      {dir}"));
    }
    lines.push(format!(
        "  report         {}",
        report.settings.json.display()
    ));
    lines.push(format!(
        "                 {}",
        report.settings.html.display()
    ));
    lines.push(
        "  note           shared-machine observations, not capacity: see the report's notes"
            .to_string(),
    );
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn utc_dates_are_civil() {
        let now = utc_now();
        assert_eq!(now.len(), 20);
        assert!(now.ends_with('Z'));
        assert!(now.starts_with("20"));
    }

    #[test]
    fn workers_split_over_a_groups_databases() {
        assert_eq!(fleet_size(2, 1), 2);
        assert_eq!(fleet_size(4, 2), 2);
        assert_eq!(fleet_size(2, 3), 1);
        assert_eq!(fleet_size(1, 0), 1);
    }

    #[test]
    fn open_loop_counters_sum_across_groups() {
        let one = ArrivalCounters::default();
        one.dispatch(1.0);
        one.finish(true);
        let two = ArrivalCounters::default();
        two.dispatch(3.0);
        two.drop_arrivals(2);
        two.finish(false);
        let all = merge_arrivals(&[one.snapshot(), two.snapshot()]);
        assert_eq!(all.offered, 4);
        assert_eq!(all.dispatched, 2);
        assert_eq!(all.driver_dropped, 2);
        assert_eq!((all.completed, all.failed), (1, 1));
        assert_eq!(all.scheduling_lag_ms.samples, 2);
    }
}
