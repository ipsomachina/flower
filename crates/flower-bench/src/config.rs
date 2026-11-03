//! Command-line settings and presets.
//!
//! Every flag defaults to v1's benchmark default (`bench/config.mjs`)
//! unless a preset sets it; a flag given on the command line always
//! wins over the preset. Workload counts (customers, drones, tenants,
//! the order cap) are per Raft group, as v1's; stores are per tenant.
//!
//! Besides a run, the binary has two subcommands: `compare` (a report
//! against a baseline, failing past a regression threshold) and
//! `publish` (validate and retain reports, and render their summary).

use std::path::PathBuf;

use clap::{Parser, Subcommand, ValueEnum};
use serde::Serialize;

/// A named starting point.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Preset {
    /// Three replicas, a leader kill, the live view opened, moderate
    /// open-loop load for two minutes or until Ctrl-C.
    Demo,
    /// One replica for a few seconds: does everything work?
    Smoke,
    /// v1's stress preset: eight three-replica groups hosted by three
    /// server processes, 256 customer loops and four drones per group,
    /// 96 orders per group, 60 s, the busiest process killed halfway.
    Stress,
}

/// How the run's Raft groups map to server processes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Topology {
    /// One group, the root: one database holds every tenant.
    Single,
    /// v1's `--hosted`: each replica process hosts its replica of every
    /// group; each tenant is a named partition placed on its group.
    Hosted,
    /// v1's default multi-group topology: an independent cluster per
    /// group, each replica its own process.
    Separate,
}

/// A CPU profile of one server process during the measured window.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CpuProfileSettings {
    /// Where the raw stacks go (folded, one stack per line).
    pub path: PathBuf,
    /// How long to sample, from the start of the measured window.
    pub seconds: u64,
    /// Requested interval between samples.
    pub interval_ms: u64,
}

/// The subcommands.
#[derive(Subcommand, Debug)]
pub enum Command {
    /// Compare a report with a baseline: goodput, latency, server and
    /// bench CPU per successful call, with v1's fixed-work criteria.
    /// Exits 1 when a measure regressed past the threshold, 2 when the
    /// reports cannot be read.
    Compare {
        /// The baseline report (JSON).
        baseline: PathBuf,
        /// The candidate report (JSON).
        candidate: PathBuf,
        /// Largest regression tolerated, percent (default 10).
        #[arg(long, default_value_t = 10.0)]
        threshold: f64,
        /// Print the comparison as JSON instead of Markdown.
        #[arg(long)]
        json: bool,
    },
    /// Validate reports and retain them as the published results (and
    /// the baselines `compare` reads), with their HTML and a summary in
    /// the crate README; `--check` revalidates what is retained.
    Publish {
        /// Reports to publish (JSON), each kept under its preset's name.
        reports: Vec<PathBuf>,
        /// Validate the retained results and the README summary instead.
        #[arg(long)]
        check: bool,
        /// Where results are retained (default: this crate's `results/`).
        #[arg(long)]
        dir: Option<PathBuf>,
        /// The README whose summary is replaced (default: this crate's).
        #[arg(long)]
        readme: Option<PathBuf>,
        /// Keep the (one) report under this name instead of its
        /// preset's: for a run whose flags changed its preset.
        #[arg(long)]
        name: Option<String>,
    },
}

impl Preset {
    fn name(self) -> &'static str {
        match self {
            Preset::Demo => "demo",
            Preset::Smoke => "smoke",
            Preset::Stress => "stress",
        }
    }
}

/// Read consistency for customer store previews.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ReadConsistency {
    /// `pizza.shop.local`: the serving replica's own applied state, may
    /// lag (v1's `replica-local`).
    Local,
    /// `pizza.shop`: linearizable, proved fresh on whichever replica
    /// serves it.
    Fresh,
}

/// Goblin Pizza stress benchmark and live demo for Flower v2.
///
/// Starts real `cargo-flower serve` processes (or drives `--target`),
/// deploys the Goblin Pizza guest, runs customers and delivery drones,
/// optionally kills the leader, drains, audits every acknowledged
/// effect with fresh reads and writes JSON and HTML reports. Exits
/// nonzero when the audit fails. Ctrl-C ends the load early and still
/// drains and audits; a second Ctrl-C stops at once.
#[derive(Parser, Debug, Default)]
#[command(name = "flower-bench", version, args_conflicts_with_subcommands = true)]
pub struct Cli {
    /// `compare` or `publish` instead of a run.
    #[command(subcommand)]
    pub command: Option<Command>,
    /// Start from a preset; explicit flags override it.
    #[arg(long, value_enum)]
    pub preset: Option<Preset>,
    /// Replicas per group: 1 or 3 (default 3).
    #[arg(long)]
    pub replicas: Option<u32>,
    /// Raft groups (default 1). Customers, drones, tenants and the order
    /// cap are per group. Above one, every replica process hosts its
    /// replica of every group and each tenant is a named partition on
    /// its group (v1's --hosted), unless --separate.
    #[arg(long)]
    pub groups: Option<u32>,
    /// Tenants as named partitions on groups that every replica process
    /// hosts (v1's --hosted); the default above one group.
    #[arg(long, conflicts_with = "separate")]
    pub hosted: bool,
    /// One independent cluster per group, each replica its own process
    /// (v1's multi-group default).
    #[arg(long)]
    pub separate: bool,
    /// Authenticated kitchen (v1's --auth; also PIZZA_AUTH=1): deploy the
    /// guest built with access rules and send every call with its
    /// tenant's (or the administrator's) bearer JWT, minted under this
    /// run's own Ed25519 key.
    #[arg(long)]
    pub auth: bool,
    /// Capture the servers' OpenTelemetry traces and metrics in a local
    /// OTLP/HTTP receiver and report them (a diagnostic run).
    #[arg(long)]
    pub otel: bool,
    /// Trace sampling ratio with --otel (default 0.01, parent-based).
    #[arg(long)]
    pub otel_sample: Option<f64>,
    /// Sample the busiest server's stacks during the measured window and
    /// write them here (folded stacks; a diagnostic run).
    #[arg(long)]
    pub cpu_profile: Option<PathBuf>,
    /// Seconds to sample with --cpu-profile (default 10, at most 60).
    #[arg(long)]
    pub cpu_profile_seconds: Option<u64>,
    /// Interval between stack samples, ms (default 1).
    #[arg(long)]
    pub cpu_profile_interval_ms: Option<u64>,
    /// An earlier report to compare this run with in its report.
    #[arg(long)]
    pub baseline: Option<PathBuf>,
    /// Drive an existing cluster instead: its members' API URLs,
    /// comma-separated (needs the admin token; no chaos).
    #[arg(long, value_delimiter = ',')]
    pub target: Vec<String>,
    /// Admin token of a `--target` cluster.
    #[arg(long, env = "FLOWER_ADMIN_TOKEN", hide_env_values = true)]
    pub admin_token: Option<String>,
    /// Worker token of a `--target` cluster (default: the admin token).
    #[arg(long, env = "FLOWER_WORKER_TOKEN", hide_env_values = true)]
    pub worker_token: Option<String>,
    /// The `cargo-flower` executable to serve with (default: build it).
    #[arg(long)]
    pub server_binary: Option<PathBuf>,
    /// The Goblin Pizza guest module (default: build it for wasm32).
    #[arg(long)]
    pub guest_wasm: Option<PathBuf>,
    /// Measured load, in seconds (default 10).
    #[arg(long)]
    pub duration: Option<f64>,
    /// Unmeasured warmup reads before it, in seconds (default 1).
    #[arg(long)]
    pub warmup: Option<f64>,
    /// Longest drain after it, in seconds (default 60).
    #[arg(long)]
    pub drain: Option<f64>,
    /// Customer loops, or open-loop slots (default 8).
    #[arg(long)]
    pub concurrency: Option<usize>,
    /// Open loop: independent arrivals per second (default 0: closed
    /// loop). Arrivals finding every slot busy are dropped and counted.
    #[arg(long)]
    pub offered_rate: Option<f64>,
    /// Delivery drones (default 2).
    #[arg(long)]
    pub workers: Option<usize>,
    /// Tenants (default 2).
    #[arg(long)]
    pub tenants: Option<usize>,
    /// Stores per tenant (default 4; v1's `--shops`).
    #[arg(long)]
    pub stores: Option<usize>,
    /// Hot stores per tenant (default 1).
    #[arg(long)]
    pub hot_stores: Option<usize>,
    /// Share of store choices drawn from the hot set (default 0.8).
    #[arg(long)]
    pub hot_probability: Option<f64>,
    /// Orders admitted in all; later order choices become reads
    /// (default 32).
    #[arg(long)]
    pub max_orders: Option<u64>,
    /// Oven time per pizza, in ms (default 250).
    #[arg(long)]
    pub bake_ms: Option<u64>,
    /// Delivery lease, in ms (default 2000).
    #[arg(long)]
    pub lease_ms: Option<u64>,
    /// Share of acknowledged mutations replayed under the same request
    /// ID to check their receipt (default 0.1).
    #[arg(long)]
    pub duplicate_rate: Option<f64>,
    /// Share of first delivery claims deliberately abandoned (default
    /// 0.1; at least one when positive).
    #[arg(long)]
    pub abandon_rate: Option<f64>,
    /// Drone polling and failure pause, in ms (default 150).
    #[arg(long)]
    pub poll_ms: Option<u64>,
    /// One attempt's timeout, in ms (default 2000).
    #[arg(long)]
    pub request_timeout_ms: Option<u64>,
    /// A call's whole retry budget, in ms (default 15000).
    #[arg(long)]
    pub retry_budget_ms: Option<u64>,
    /// Workload seed (default 42).
    #[arg(long)]
    pub seed: Option<String>,
    /// Customer previews: local (default) or fresh. Audits always read
    /// fresh.
    #[arg(long, value_enum)]
    pub read_consistency: Option<ReadConsistency>,
    /// Kill the leader halfway through the load and restart it.
    #[arg(long, overrides_with = "no_chaos")]
    pub chaos: bool,
    /// No leader kill, whatever the preset says.
    #[arg(long)]
    pub no_chaos: bool,
    /// Serve the live view on 127.0.0.1.
    #[arg(long, overrides_with = "no_live")]
    pub live: bool,
    /// No live view, whatever the preset says.
    #[arg(long)]
    pub no_live: bool,
    /// Open the live view in a browser (implies --live).
    #[arg(long, overrides_with = "no_open")]
    pub open: bool,
    /// Never open a browser, whatever the preset says.
    #[arg(long)]
    pub no_open: bool,
    /// Port for the live view (default: any free port).
    #[arg(long)]
    pub live_port: Option<u16>,
    /// After the run, keep serving the live view until Ctrl-C.
    #[arg(long)]
    pub linger: bool,
    /// Keep the servers' data directories.
    #[arg(long)]
    pub keep_data: bool,
    /// Parent directory for the servers' data (default: the system
    /// temporary directory).
    #[arg(long)]
    pub data: Option<PathBuf>,
    /// JSON report path (default target/flower-bench/<preset>.json).
    #[arg(long)]
    pub json: Option<PathBuf>,
    /// HTML report path (default: the JSON path with .html).
    #[arg(long)]
    pub html: Option<PathBuf>,
    /// No live status line; the final scorecard still prints.
    #[arg(long)]
    pub quiet: bool,
}

/// Resolved settings: what a run does, as its report records it.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Settings {
    /// The preset, if any.
    pub preset: Option<&'static str>,
    /// Raft groups driven.
    pub groups: u32,
    /// How groups map to server processes.
    pub topology: Topology,
    /// Replicas per group.
    pub replicas: u32,
    /// The authenticated kitchen.
    pub auth: bool,
    /// OpenTelemetry capture, with this trace sampling ratio.
    pub otel: Option<f64>,
    /// A CPU profile, if asked.
    pub cpu_profile: Option<CpuProfileSettings>,
    /// The report compared with, if any.
    pub baseline: Option<PathBuf>,
    /// External members, when driving `--target`.
    pub targets: Vec<String>,
    /// Measured load, seconds.
    pub duration_s: f64,
    /// Warmup, seconds.
    pub warmup_s: f64,
    /// Longest drain, seconds.
    pub drain_s: f64,
    /// Customer loops or open-loop slots.
    pub concurrency: usize,
    /// Open-loop arrivals per second; 0 is closed loop.
    pub offered_rate: f64,
    /// Delivery drones.
    pub workers: usize,
    /// Tenants.
    pub tenants: usize,
    /// Stores per tenant.
    pub stores: usize,
    /// Hot stores per tenant.
    pub hot_stores: usize,
    /// Hot-set weight.
    pub hot_probability: f64,
    /// Order cap.
    pub max_orders: u64,
    /// Starting stock per store: four pizzas per admitted order plus
    /// ten, as v1, so no store runs out.
    pub stock_per_store: u64,
    /// Oven time, ms.
    pub bake_ms: u64,
    /// Delivery lease, ms.
    pub lease_ms: u64,
    /// Receipt-replay probe rate.
    pub duplicate_rate: f64,
    /// Abandoned-claim rate.
    pub abandon_rate: f64,
    /// Drone polling, ms.
    pub poll_ms: u64,
    /// Attempt timeout, ms.
    pub request_timeout_ms: u64,
    /// Retry budget, ms.
    pub retry_budget_ms: u64,
    /// Workload seed.
    pub seed: String,
    /// Customer preview consistency.
    pub read_consistency: ReadConsistency,
    /// Leader kill.
    pub chaos: bool,
    /// Live view.
    pub live: bool,
    /// Open a browser on it.
    pub open: bool,
    /// Its port; 0 picks one.
    pub live_port: u16,
    /// Keep serving it after the run.
    pub linger: bool,
    /// Keep data directories.
    pub keep_data: bool,
    /// Data parent directory.
    pub data: Option<PathBuf>,
    /// JSON report path.
    pub json: PathBuf,
    /// HTML report path.
    pub html: PathBuf,
    /// Server executable, if given.
    pub server_binary: Option<PathBuf>,
    /// Guest module, if given.
    pub guest_wasm: Option<PathBuf>,
    /// No live status line.
    pub quiet: bool,
    /// A `--target` cluster's admin token; never reported.
    #[serde(skip)]
    pub admin_token: Option<String>,
    /// A `--target` cluster's worker token; never reported.
    #[serde(skip)]
    pub worker_token: Option<String>,
}

/// The values a preset (or no preset) starts from.
struct Base {
    groups: u32,
    replicas: u32,
    duration: f64,
    warmup: f64,
    drain: f64,
    concurrency: usize,
    offered_rate: f64,
    workers: usize,
    tenants: usize,
    stores: usize,
    max_orders: u64,
    bake_ms: u64,
    lease_ms: u64,
    poll_ms: u64,
    chaos: bool,
    live: bool,
    linger: bool,
}

impl Base {
    /// v1's defaults.
    fn v1() -> Base {
        Base {
            groups: 1,
            replicas: 3,
            duration: 10.0,
            warmup: 1.0,
            drain: 60.0,
            concurrency: 8,
            offered_rate: 0.0,
            workers: 2,
            tenants: 2,
            stores: 4,
            max_orders: 32,
            bake_ms: 250,
            lease_ms: 2_000,
            poll_ms: 150,
            chaos: false,
            live: false,
            linger: false,
        }
    }

    fn of(preset: Option<Preset>) -> Base {
        let v1 = Base::v1();
        match preset {
            None => v1,
            Some(Preset::Smoke) => Base {
                replicas: 1,
                duration: 3.0,
                warmup: 0.5,
                drain: 30.0,
                concurrency: 4,
                max_orders: 8,
                ..v1
            },
            // Arrivals at a pace the eye can follow: about 36 orders a
            // second for two minutes, ovens slow enough to watch, a
            // drone fleet that keeps up, and the leader killed at one
            // minute.
            Some(Preset::Demo) => Base {
                duration: 120.0,
                concurrency: 64,
                offered_rate: 120.0,
                workers: 6,
                tenants: 3,
                stores: 2,
                max_orders: 5_000,
                bake_ms: 1_500,
                lease_ms: 3_000,
                poll_ms: 100,
                chaos: true,
                live: true,
                linger: true,
                ..v1
            },
            // v1's stress preset (bench/stress.mjs): eight groups on
            // three host processes (v1's --hosted).
            Some(Preset::Stress) => Base {
                groups: 8,
                duration: 60.0,
                concurrency: 256,
                workers: 4,
                max_orders: 96,
                chaos: true,
                ..v1
            },
        }
    }
}

impl Cli {
    /// Resolve flags over the preset and check them.
    pub fn settings(&self) -> Result<Settings, String> {
        let base = Base::of(self.preset);
        let external = !self.target.is_empty();
        let replicas = if external {
            self.target.len() as u32
        } else {
            self.replicas.unwrap_or(base.replicas)
        };
        let chaos = !self.no_chaos && (self.chaos || base.chaos);
        let live = !self.no_live && (self.live || self.open || base.live);
        let open = live
            && !self.no_open
            && (self.open || (base.live && self.preset == Some(Preset::Demo)));
        let max_orders = self.max_orders.unwrap_or(base.max_orders);
        let json = match &self.json {
            Some(path) => path.clone(),
            None => default_results_dir().join(format!(
                "{}.json",
                self.preset.map_or("custom", Preset::name)
            )),
        };
        let html = self
            .html
            .clone()
            .unwrap_or_else(|| json.with_extension("html"));
        let groups = self.groups.unwrap_or(base.groups);
        let topology = if self.separate {
            Topology::Separate
        } else if self.hosted || groups > 1 {
            Topology::Hosted
        } else {
            Topology::Single
        };
        let auth = self.auth || auth_from_env(std::env::var("PIZZA_AUTH").ok().as_deref())?;
        let otel =
            (self.otel || self.otel_sample.is_some()).then(|| self.otel_sample.unwrap_or(0.01));
        let cpu_profile = self.cpu_profile.clone().map(|path| CpuProfileSettings {
            path,
            seconds: self.cpu_profile_seconds.unwrap_or(10),
            interval_ms: self.cpu_profile_interval_ms.unwrap_or(1),
        });
        let settings = Settings {
            preset: self.preset.map(Preset::name),
            groups,
            topology,
            replicas,
            auth,
            otel,
            cpu_profile,
            baseline: self.baseline.clone(),
            targets: self.target.clone(),
            duration_s: self.duration.unwrap_or(base.duration),
            warmup_s: self.warmup.unwrap_or(base.warmup),
            drain_s: self.drain.unwrap_or(base.drain),
            concurrency: self.concurrency.unwrap_or(base.concurrency),
            offered_rate: self.offered_rate.unwrap_or(base.offered_rate),
            workers: self.workers.unwrap_or(base.workers),
            tenants: self.tenants.unwrap_or(base.tenants),
            stores: self.stores.unwrap_or(base.stores),
            hot_stores: self.hot_stores.unwrap_or(1),
            hot_probability: self.hot_probability.unwrap_or(0.8),
            max_orders,
            stock_per_store: max_orders.saturating_mul(4).saturating_add(10),
            bake_ms: self.bake_ms.unwrap_or(base.bake_ms),
            lease_ms: self.lease_ms.unwrap_or(base.lease_ms),
            duplicate_rate: self.duplicate_rate.unwrap_or(0.1),
            abandon_rate: self.abandon_rate.unwrap_or(0.1),
            poll_ms: self.poll_ms.unwrap_or(base.poll_ms),
            request_timeout_ms: self.request_timeout_ms.unwrap_or(2_000),
            retry_budget_ms: self.retry_budget_ms.unwrap_or(15_000),
            seed: self.seed.clone().unwrap_or_else(|| "42".to_string()),
            read_consistency: self.read_consistency.unwrap_or(ReadConsistency::Local),
            chaos,
            live,
            open,
            live_port: self.live_port.unwrap_or(0),
            linger: live && (self.linger || base.linger),
            keep_data: self.keep_data,
            data: self.data.clone(),
            json,
            html,
            server_binary: self.server_binary.clone(),
            guest_wasm: self.guest_wasm.clone(),
            quiet: self.quiet,
            admin_token: self.admin_token.clone(),
            worker_token: self.worker_token.clone(),
        };
        settings.validate(external, self.admin_token.is_some())?;
        Ok(settings)
    }
}

/// Where reports go by default: the workspace's `target/flower-bench`,
/// else `flower-bench-results` in the current directory.
fn default_results_dir() -> PathBuf {
    match crate::artifacts::workspace_root() {
        Some(root) => root.join("target").join("flower-bench"),
        None => PathBuf::from("flower-bench-results"),
    }
}

/// Whether `PIZZA_AUTH` asks for the authenticated kitchen, as v1 reads
/// it: 1, true or yes; 0, false, no or empty is no.
pub fn auth_from_env(value: Option<&str>) -> Result<bool, String> {
    match value
        .map(|value| value.trim().to_ascii_lowercase())
        .as_deref()
    {
        None | Some("" | "0" | "false" | "no") => Ok(false),
        Some("1" | "true" | "yes") => Ok(true),
        Some(_) => Err("PIZZA_AUTH must be 1 or 0".to_string()),
    }
}

fn check(condition: bool, message: &str) -> Result<(), String> {
    if condition {
        Ok(())
    } else {
        Err(message.to_string())
    }
}

impl Settings {
    fn validate(&self, external: bool, admin_token: bool) -> Result<(), String> {
        let fraction = |value: f64| value.is_finite() && (0.0..=1.0).contains(&value);
        check(
            self.replicas == 1 || self.replicas == 3 || external,
            "--replicas must be 1 or 3",
        )?;
        check(
            !external || admin_token,
            "--target needs the cluster's admin token (--admin-token or FLOWER_ADMIN_TOKEN)",
        )?;
        check(
            !(external && self.chaos),
            "--chaos needs servers the bench started: it cannot kill a --target member",
        )?;
        check(
            !self.chaos || self.replicas == 3,
            "--chaos needs 3 replicas: a group of one has no one to elect",
        )?;
        check((1..=64).contains(&self.groups), "--groups must be 1 to 64")?;
        check(
            !external || self.topology == Topology::Single,
            "--target drives one group: many groups, --hosted and --separate need servers the bench started",
        )?;
        check(
            self.otel
                .is_none_or(|ratio| ratio.is_finite() && (0.0..=1.0).contains(&ratio)),
            "--otel-sample must be a ratio from 0 to 1",
        )?;
        check(
            !(external && self.otel.is_some()),
            "--otel needs servers the bench started: it sets their exporter",
        )?;
        if let Some(profile) = &self.cpu_profile {
            check(!external, "--cpu-profile needs servers the bench started")?;
            check(
                (1..=60).contains(&profile.seconds),
                "--cpu-profile-seconds must be 1 to 60",
            )?;
            check(
                (1..=1000).contains(&profile.interval_ms),
                "--cpu-profile-interval-ms must be 1 to 1000",
            )?;
            check(
                profile.path != self.json
                    && profile.path != self.html
                    && Some(&profile.path) != self.baseline.as_ref(),
                "--cpu-profile must not overwrite a report or the baseline",
            )?;
        }
        if let Some(baseline) = &self.baseline {
            check(
                baseline != &self.json && baseline != &self.html,
                "reports must not overwrite --baseline",
            )?;
        }
        check(
            self.duration_s.is_finite() && (1.0..=3600.0).contains(&self.duration_s),
            "--duration must be 1 to 3600 seconds",
        )?;
        check(
            self.warmup_s.is_finite() && (0.0..=60.0).contains(&self.warmup_s),
            "--warmup must be 0 to 60 seconds",
        )?;
        check(
            self.drain_s.is_finite() && (1.0..=3600.0).contains(&self.drain_s),
            "--drain must be 1 to 3600 seconds",
        )?;
        check(self.concurrency >= 1, "--concurrency must be at least 1")?;
        check(
            self.offered_rate.is_finite() && self.offered_rate >= 0.0,
            "--offered-rate must be a non-negative number",
        )?;
        check(
            (self.offered_rate * self.duration_s) < 1e12,
            "--offered-rate times --duration is too many arrivals",
        )?;
        check(self.workers >= 1, "--workers must be at least 1")?;
        check(self.tenants >= 1, "--tenants must be at least 1")?;
        check(
            (1..=64).contains(&self.stores),
            "--stores must be 1 to 64 (the guest's limit)",
        )?;
        check(
            self.hot_stores >= 1 && self.hot_stores <= self.stores,
            "--hot-stores must be 1 to --stores",
        )?;
        check(
            fraction(self.hot_probability),
            "--hot-probability must be 0 to 1",
        )?;
        check(self.max_orders >= 1, "--max-orders must be at least 1")?;
        check(
            self.stock_per_store <= 1_000_000,
            "--max-orders is too large: stores hold at most 1,000,000 pizzas",
        )?;
        check(self.bake_ms <= 60_000, "--bake-ms must be at most 60000")?;
        check(
            (100..=60_000).contains(&self.lease_ms),
            "--lease-ms must be 100 to 60000",
        )?;
        check(
            fraction(self.duplicate_rate),
            "--duplicate-rate must be 0 to 1",
        )?;
        check(fraction(self.abandon_rate), "--abandon-rate must be 0 to 1")?;
        check(
            (10..=10_000).contains(&self.poll_ms),
            "--poll-ms must be 10 to 10000",
        )?;
        check(
            (100..=60_000).contains(&self.request_timeout_ms),
            "--request-timeout-ms must be 100 to 60000",
        )?;
        check(
            self.retry_budget_ms >= self.request_timeout_ms && self.retry_budget_ms <= 300_000,
            "--retry-budget-ms must be at least --request-timeout-ms and at most 300000",
        )?;
        check(!self.seed.is_empty(), "--seed must not be empty")?;
        check(self.html != self.json, "--html and --json must differ")?;
        Ok(())
    }

    /// Every group's tenant IDs.
    pub fn tenant_ids(&self) -> Vec<String> {
        (0..self.groups as usize)
            .flat_map(|group| self.group_tenants(group))
            .collect()
    }

    /// Group `group`'s tenants: `tenant-<group × tenants + i>`, v1's
    /// static assignment, so no tenant is in two groups.
    pub fn group_tenants(&self, group: usize) -> Vec<String> {
        (0..self.tenants)
            .map(|index| format!("tenant-{}", group * self.tenants + index))
            .collect()
    }

    /// Group `group`'s workload seed: the run's for one group, v1's
    /// `<seed>:group-<g>` for several.
    pub fn group_seed(&self, group: usize) -> String {
        if self.groups == 1 {
            self.seed.clone()
        } else {
            format!("{}:group-{group}", self.seed)
        }
    }

    /// Whether a diagnostic (OpenTelemetry or a CPU profile) perturbed
    /// the run: such a run is not a capacity measurement.
    pub fn diagnostic(&self) -> bool {
        self.otel.is_some() || self.cpu_profile.is_some()
    }

    /// One line describing the run.
    pub fn describe(&self) -> String {
        let load = if self.offered_rate > 0.0 {
            format!(
                "{}/s open-loop arrivals ({} slots)",
                self.offered_rate, self.concurrency
            )
        } else {
            format!("{} closed-loop customers", self.concurrency)
        };
        let groups = match self.topology {
            Topology::Single => String::new(),
            Topology::Hosted => format!(
                "{} group{} hosted by every process (tenants as partitions), per group: ",
                self.groups,
                if self.groups == 1 { "" } else { "s" }
            ),
            Topology::Separate => format!(
                "{} separate cluster{}, per group: ",
                self.groups,
                if self.groups == 1 { "" } else { "s" }
            ),
        };
        let chaos = match (self.chaos, self.topology) {
            (false, _) => "",
            (true, Topology::Hosted) if self.groups > 1 => ", the busiest process killed",
            (true, Topology::Separate) if self.groups > 1 => ", every group's leader killed",
            (true, _) => ", one leader kill",
        };
        format!(
            "{groups}{} replica{}, {load}, {} drones, {} tenants x {} stores, {} orders max, {}s measured{chaos}{}{}{}",
            self.replicas,
            if self.replicas == 1 { "" } else { "s" },
            self.workers,
            self.tenants,
            self.stores,
            self.max_orders,
            self.duration_s,
            if self.auth {
                ", bearer JWTs (--auth)"
            } else {
                ""
            },
            if self.otel.is_some() {
                ", OpenTelemetry captured"
            } else {
                ""
            },
            if self.cpu_profile.is_some() {
                ", CPU profiled"
            } else {
                ""
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Result<Settings, String> {
        let mut argv = vec!["flower-bench"];
        argv.extend_from_slice(args);
        Cli::try_parse_from(argv)
            .map_err(|error| error.to_string())?
            .settings()
    }

    #[test]
    fn defaults_are_v1s() {
        let settings = parse(&[]).unwrap();
        assert_eq!(settings.replicas, 3);
        assert_eq!(settings.duration_s, 10.0);
        assert_eq!(settings.concurrency, 8);
        assert_eq!(settings.workers, 2);
        assert_eq!(settings.tenants, 2);
        assert_eq!(settings.stores, 4);
        assert_eq!(settings.max_orders, 32);
        assert_eq!(settings.stock_per_store, 138);
        assert_eq!(settings.lease_ms, 2_000);
        assert_eq!(settings.read_consistency, ReadConsistency::Local);
        assert!(!settings.chaos && !settings.live);
        assert_eq!(settings.groups, 1);
        assert_eq!(settings.topology, Topology::Single);
        assert!(settings.otel.is_none() && settings.cpu_profile.is_none());
    }

    #[test]
    fn presets_set_and_flags_override() {
        let demo = parse(&["--preset", "demo"]).unwrap();
        assert!(demo.chaos && demo.live && demo.open && demo.linger);
        assert_eq!(demo.replicas, 3);
        assert!(demo.offered_rate > 0.0);
        let headless = parse(&["--preset", "demo", "--no-open"]).unwrap();
        assert!(headless.live && !headless.open);
        let quiet = parse(&["--preset", "demo", "--no-chaos", "--no-live"]).unwrap();
        assert!(!quiet.chaos && !quiet.live && !quiet.open);
        let smoke = parse(&["--preset", "smoke"]).unwrap();
        assert_eq!(smoke.replicas, 1);
        assert!(!smoke.chaos);
        let stress = parse(&["--preset", "stress", "--duration", "5"]).unwrap();
        assert_eq!(stress.concurrency, 256);
        assert_eq!(stress.max_orders, 96);
        assert_eq!(stress.duration_s, 5.0);
        assert!(stress.json.ends_with("stress.json"));
        assert!(stress.html.ends_with("stress.html"));
        // v1's stress preset: eight groups on three host processes.
        assert_eq!(stress.groups, 8);
        assert_eq!(stress.topology, Topology::Hosted);
        let separate = parse(&["--preset", "stress", "--separate"]).unwrap();
        assert_eq!(separate.topology, Topology::Separate);
        let one = parse(&["--preset", "stress", "--groups", "1"]).unwrap();
        assert_eq!(one.topology, Topology::Single);
        let hosted_one = parse(&["--hosted"]).unwrap();
        assert_eq!(
            (hosted_one.groups, hosted_one.topology),
            (1, Topology::Hosted)
        );
    }

    #[test]
    fn groups_get_disjoint_tenants_and_their_own_seeds() {
        let settings = parse(&["--groups", "3", "--tenants", "2"]).unwrap();
        assert_eq!(settings.group_tenants(0), vec!["tenant-0", "tenant-1"]);
        assert_eq!(settings.group_tenants(2), vec!["tenant-4", "tenant-5"]);
        assert_eq!(settings.tenant_ids().len(), 6);
        assert_eq!(settings.group_seed(1), "42:group-1");
        let single = parse(&[]).unwrap();
        assert_eq!(single.group_seed(0), "42");
    }

    #[test]
    fn diagnostics_and_baselines_resolve() {
        let settings = parse(&[
            "--otel",
            "--cpu-profile",
            "cpu.folded",
            "--cpu-profile-seconds",
            "5",
            "--baseline",
            "before.json",
            "--auth",
        ])
        .unwrap();
        assert_eq!(settings.otel, Some(0.01));
        let profile = settings.cpu_profile.as_ref().unwrap();
        assert_eq!((profile.seconds, profile.interval_ms), (5, 1));
        assert!(settings.diagnostic() && settings.auth);
        assert_eq!(settings.baseline, Some(PathBuf::from("before.json")));
        assert_eq!(auth_from_env(Some(" Yes ")), Ok(true));
        assert_eq!(auth_from_env(Some("0")), Ok(false));
        assert_eq!(auth_from_env(None), Ok(false));
        assert!(auth_from_env(Some("maybe")).is_err());
    }

    #[test]
    fn subcommands_parse_apart_from_run_flags() {
        let cli = Cli::try_parse_from([
            "flower-bench",
            "compare",
            "a.json",
            "b.json",
            "--threshold",
            "5",
        ])
        .unwrap();
        assert!(
            matches!(cli.command, Some(Command::Compare { threshold, .. }) if threshold == 5.0)
        );
        let cli = Cli::try_parse_from(["flower-bench", "publish", "--check"]).unwrap();
        assert!(matches!(
            cli.command,
            Some(Command::Publish { check: true, .. })
        ));
        assert!(
            Cli::try_parse_from(["flower-bench", "--duration", "5", "compare", "a", "b"]).is_err()
        );
    }

    #[test]
    fn impossible_runs_are_refused() {
        assert!(parse(&["--replicas", "2"]).is_err());
        assert!(parse(&["--replicas", "1", "--chaos"]).is_err());
        assert!(parse(&["--hot-stores", "5"]).is_err());
        assert!(parse(&["--retry-budget-ms", "100"]).is_err());
        assert!(parse(&["--abandon-rate", "2"]).is_err());
        assert!(parse(&["--target", "http://127.0.0.1:1"]).is_err());
        assert!(
            parse(&[
                "--target",
                "http://127.0.0.1:1",
                "--admin-token",
                "t",
                "--chaos"
            ])
            .is_err()
        );
        assert!(parse(&["--json", "x.json", "--html", "x.json"]).is_err());
        assert!(parse(&["--groups", "0"]).is_err());
        assert!(parse(&["--groups", "65"]).is_err());
        assert!(parse(&["--hosted", "--separate"]).is_err());
        assert!(
            parse(&[
                "--target",
                "http://127.0.0.1:1",
                "--admin-token",
                "t",
                "--groups",
                "2"
            ])
            .is_err()
        );
        assert!(parse(&["--otel-sample", "2"]).is_err());
        assert!(parse(&["--cpu-profile", "p", "--cpu-profile-seconds", "0"]).is_err());
        assert!(parse(&["--json", "x.json", "--baseline", "x.json"]).is_err());
        assert!(parse(&["--json", "x.json", "--cpu-profile", "x.json"]).is_err());
    }
}
