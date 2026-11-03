//! The run's report: JSON with a stable schema (`flower-bench/2`), and
//! a self-contained HTML page with SVG charts.
//!
//! The JSON keeps everything needed to recompute any figure: every
//! phase's per-operation histograms (sparse buckets over fixed bounds,
//! so runs merge bucket by bucket), the timeline, chaos events, process
//! samples, each group's audit checklist and goodput, the cross-group
//! audit, diagnostics and the binaries' hashes. Fields are only ever
//! added under one schema version; version 2 added many groups (each
//! group's goodput, tenants and databases, the process kill, the
//! catalog's placements, the cross-group audit), the diagnostics and the
//! baseline comparison, and named a group's databases (`databases`)
//! where version 1 had one (`db`). `compare` and `publish` read both.
//!
//! The HTML page has no script at all and loads nothing: its one style
//! sheet is inline and allowed by its own SHA-256 in the page's
//! Content-Security-Policy. Charts are inline SVG with a native tooltip
//! per time slot and a table view beside each.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::artifacts::{Fingerprint, Machine};
use crate::audit::Audit;
use crate::chaos::{ChaosEvent, ProcessKill};
use crate::client::{Member, RouteCounters};
use crate::compare::Comparison;
use crate::config::Settings;
use crate::drones::{DroneCounters, FenceProbe};
use crate::groups::Placement;
use crate::histogram::{Histogram, HistogramScheme, HistogramSnapshot};
use crate::metrics::{Goodput, PhaseSummary, TimelinePoint};
use crate::otel::OtelReport;
use crate::profile::CpuProfile;
use crate::resources::ProcessSummary;
use crate::workload::{Arrivals, FailureSample, Mismatch};

/// The schema this module writes.
pub const SCHEMA: &str = "flower-bench/2";

/// The binaries a run used.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Binaries {
    /// The server, when the bench started one.
    pub server: Option<Fingerprint>,
    /// The guest module.
    pub guest: Fingerprint,
    /// The bench itself.
    pub bench: Option<Fingerprint>,
}

/// The cluster a run drove.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterInfo {
    /// `spawned` (one group), `hosted`, `separate` or `external`.
    pub mode: &'static str,
    /// Members.
    pub members: Vec<Member>,
    /// Server arguments, tokens left out.
    pub server_args: Vec<String>,
    /// Databases the group served: one holding every tenant, or one
    /// named partition per tenant.
    pub databases: Vec<String>,
    /// Kept data directory, if kept.
    pub data_dir: Option<String>,
}

/// Errors across the measured window.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Errors {
    /// Failed attempts by kind, every class.
    pub by_kind: BTreeMap<String, u64>,
    /// Attempts sent.
    pub attempts: u64,
    /// Retry attempts.
    pub retries: u64,
    /// Failed customer calls in the whole run.
    pub customer_failures: u64,
    /// Samples of them.
    pub samples: Vec<FailureSample>,
}

/// Pizzas' journeys, from the source rows.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Lifecycle {
    /// Oven bell lateness: ready time less due time, ms.
    pub oven_lateness_ms: HistogramSnapshot,
    /// Order to door: delivery time less order time, ms.
    pub order_to_door_ms: HistogramSnapshot,
}

/// Drone results.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Drones {
    /// Counters.
    pub counters: DroneCounters,
    /// The worker's summary: completed, failed, killed, lost.
    pub worker: BTreeMap<String, u64>,
    /// The abandoned tokens presented again.
    pub fences: Vec<FenceProbe>,
}

/// Receipt replays.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Replays {
    /// Probes made, during the run and at the audit.
    pub checks: u64,
    /// Those whose value, revision or log position differed.
    pub mismatches: Vec<Mismatch>,
}

/// One Raft group's results.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupReport {
    /// Group index.
    pub index: u32,
    /// Its name: `root` for a single group, else `g1`, `g2`...
    pub name: String,
    /// Its tenants.
    pub tenants: Vec<String>,
    /// Its own goodput (the run's merges every group's by bucket).
    pub goodput: Option<Goodput>,
    /// Its open-loop accounting, in open-loop runs.
    pub offered_load: Option<Arrivals>,
    /// Its cluster.
    pub cluster: ClusterInfo,
    /// What a process kill did to it.
    pub chaos: Vec<ChaosEvent>,
    /// Its audit.
    pub audit: Option<Audit>,
    /// Its drones.
    pub drones: Drones,
    /// Its routing per member.
    pub routing: Vec<RouteCounters>,
}

/// The whole report.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Report {
    /// `flower-bench/1`.
    pub schema: &'static str,
    /// Run ID.
    pub run_id: String,
    /// Start, UTC.
    pub started_at: String,
    /// End, UTC.
    pub finished_at: String,
    /// The verdict: every audit check held.
    pub passed: bool,
    /// Ctrl-C ended the load early.
    pub interrupted: bool,
    /// One line: PASS, or FAIL and why.
    pub verdict: String,
    /// Resolved settings.
    pub settings: Settings,
    /// Machine and build.
    pub machine: Machine,
    /// Binary identities.
    pub binaries: Binaries,
    /// Goodput over the measured window.
    pub goodput: Goodput,
    /// Open-loop accounting, in open-loop runs.
    pub offered_load: Option<Arrivals>,
    /// How histograms are bucketed.
    pub histograms: HistogramScheme,
    /// Every phase's per-operation accounting.
    pub phases: Vec<PhaseSummary>,
    /// Errors.
    pub errors: Errors,
    /// Receipt replays.
    pub replays: Replays,
    /// One second at a time.
    pub timeline: Vec<TimelinePoint>,
    /// Sampled process CPU and memory, by process name.
    pub resources: BTreeMap<String, ProcessSummary>,
    /// Pizzas' journeys.
    pub lifecycle: Option<Lifecycle>,
    /// Per group: tenants, goodput, cluster, chaos, audit, drones,
    /// routing.
    pub groups: Vec<GroupReport>,
    /// The audit across groups (several groups, or hosted partitions).
    pub across: Option<Audit>,
    /// The process kill, when one process hosted many groups.
    pub kill: Option<ProcessKill>,
    /// The catalog's partitions after the run (hosted runs).
    pub placements: Vec<Placement>,
    /// The servers' OpenTelemetry, with `--otel`.
    pub otel: Option<OtelReport>,
    /// The CPU profile, with `--cpu-profile`.
    pub cpu_profile: Option<CpuProfile>,
    /// The comparison with `--baseline`.
    pub comparison: Option<Comparison>,
    /// What these numbers are and are not.
    pub notes: Vec<String>,
}

/// The notes every report carries, then the run's own.
pub fn notes(extra: &[String], diagnostic: bool) -> Vec<String> {
    let mut notes = vec![
        "Every replica, the drones and the customers share this machine's CPU, memory and disk; nothing else on it was controlled. Treat throughput and latency as this run's observations, not capacity.".to_string(),
        "Server CPU includes replication, drone traffic and background work; goodput excludes them.".to_string(),
        "For a clean run: a quiet machine (no builds or tests), a release build (cargo run --release), several repetitions alternating any compared binaries, and the same seed and settings.".to_string(),
    ];
    if diagnostic {
        notes.push("Diagnostics (OpenTelemetry capture or stack sampling) ran in this window and perturb it: this run is a diagnostic, not a capacity measurement, and `publish` refuses it.".to_string());
    }
    notes.extend(extra.iter().cloned());
    notes
}

impl Report {
    /// Write the JSON and HTML files, creating their directories.
    pub fn write(&self) -> Result<(), String> {
        for path in [&self.settings.json, &self.settings.html] {
            if let Some(parent) = path
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
            {
                std::fs::create_dir_all(parent)
                    .map_err(|error| format!("{}: {error}", parent.display()))?;
            }
        }
        let json = serde_json::to_string_pretty(self).map_err(|error| error.to_string())?;
        std::fs::write(&self.settings.json, json + "\n")
            .map_err(|error| format!("{}: {error}", self.settings.json.display()))?;
        std::fs::write(&self.settings.html, render(self))
            .map_err(|error| format!("{}: {error}", self.settings.html.display()))?;
        Ok(())
    }

    /// The load phase's summary.
    pub fn load(&self) -> Option<&PhaseSummary> {
        self.phases
            .iter()
            .find(|phase| phase.phase == crate::metrics::Phase::Load)
    }
}

/// Escape text for HTML.
pub fn esc(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for character in text.chars() {
        match character {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            other => out.push(other),
        }
    }
    out
}

/// Standard base64, for the style sheet's hash.
fn base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let triple = (u32::from(chunk[0]) << 16)
            | (u32::from(*chunk.get(1).unwrap_or(&0)) << 8)
            | u32::from(*chunk.get(2).unwrap_or(&0));
        for index in 0..4 {
            if index <= chunk.len() {
                out.push(char::from(
                    ALPHABET[(triple >> (18 - 6 * index) & 63) as usize],
                ));
            } else {
                out.push('=');
            }
        }
    }
    out
}

/// A duration in ms with three significant digits, seconds from 1 s.
pub fn ms(value: Option<f64>) -> String {
    match value {
        None => "—".to_string(),
        Some(value) if value >= 1000.0 => format!("{:.2} s", value / 1000.0),
        Some(value) if value >= 100.0 => format!("{value:.0} ms"),
        Some(value) if value >= 10.0 => format!("{value:.1} ms"),
        Some(value) => format!("{value:.2} ms"),
    }
}

/// A count with thousands separators.
pub fn count(value: u64) -> String {
    let digits = value.to_string();
    let mut out = String::new();
    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            out.push(',');
        }
        out.push(digit);
    }
    out
}

/// A rate with one decimal and separators.
pub fn rate(value: f64) -> String {
    if !value.is_finite() {
        return "—".to_string();
    }
    let whole = value.trunc() as u64;
    format!(
        "{}.{}",
        count(whole),
        ((value.fract() * 10.0).round() as u64).min(9)
    )
}

/// The page's style sheet: the reference palette's roles, light and
/// dark, with colors bound to classes so SVG marks follow the theme.
const STYLE: &str = r#"
:root{color-scheme:light;--page:#f9f9f7;--surface:#fcfcfb;--ink:#0b0b0b;--ink2:#52514e;--muted:#898781;--grid:#e1e0d9;--axis:#c3c2b7;--ring:rgba(11,11,11,.10);--s1:#2a78d6;--s2:#eb6834;--s3:#1baf7a;--s4:#eda100;--good:#0ca30c;--goodtext:#006300;--critical:#d03b3b;--warning:#fab219}
@media (prefers-color-scheme:dark){:root{color-scheme:dark;--page:#0d0d0d;--surface:#1a1a19;--ink:#f0efec;--ink2:#c3c2b7;--muted:#898781;--grid:#2c2c2a;--axis:#383835;--ring:rgba(255,255,255,.10);--s1:#3987e5;--s2:#d95926;--s3:#199e70;--s4:#c98500;--goodtext:#0ca30c}}
*{box-sizing:border-box}
body{margin:0;background:var(--page);color:var(--ink);font:15px/1.5 system-ui,-apple-system,"Segoe UI",sans-serif}
main{max-width:1100px;margin:0 auto;padding:24px 16px 48px}
h1{font-size:24px;margin:0 0 4px}h2{font-size:18px;margin:32px 0 8px}h3{font-size:15px;margin:0}
p{margin:4px 0}.muted{color:var(--ink2)}.small{font-size:13px}
.verdict{display:inline-flex;gap:8px;align-items:center;font-weight:600;padding:4px 12px;border-radius:999px;border:1px solid var(--ring)}
.verdict.pass .icon{color:var(--good)}.verdict.fail .icon{color:var(--critical)}
.tiles{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:12px;margin:16px 0}
.tile{background:var(--surface);border:1px solid var(--ring);border-radius:12px;padding:12px 16px}
.tile .label{color:var(--ink2);font-size:13px}.tile .value{font-size:26px;font-weight:600}
.tile.hero .value{font-size:48px;line-height:1.1}
.card{background:var(--surface);border:1px solid var(--ring);border-radius:12px;padding:16px;margin:12px 0}
.legend{display:flex;gap:16px;flex-wrap:wrap;list-style:none;padding:0;margin:4px 0 8px;font-size:13px;color:var(--ink2)}
.key{display:inline-block;width:14px;height:2px;margin-right:6px;vertical-align:middle;background:currentColor}
.k1{color:var(--s1)}.k2{color:var(--s2)}.k3{color:var(--s3)}.k4{color:var(--s4)}
svg.chart{width:100%;height:auto;display:block}
.gridline{stroke:var(--grid);stroke-width:1}.baseline{stroke:var(--axis);stroke-width:1}
.tick{fill:var(--muted);font-size:11px;font-variant-numeric:tabular-nums}
.line{fill:none;stroke-width:2;stroke-linejoin:round;stroke-linecap:round}
.l1{stroke:var(--s1)}.l2{stroke:var(--s2)}.l3{stroke:var(--s3)}.l4{stroke:var(--s4)}
.band{fill:var(--critical);opacity:.12}.kill{stroke:var(--critical);stroke-width:1}
.marklabel{fill:var(--ink2);font-size:11px}
.slot{fill:transparent}.slot:hover{fill:var(--ink);opacity:.05}
.bar{fill:var(--s1)}.bar.w{fill:var(--s2)}
table{border-collapse:collapse;width:100%;font-size:13px}
th,td{text-align:left;padding:6px 8px;border-bottom:1px solid var(--grid);vertical-align:top}
td.num,th.num{text-align:right;font-variant-numeric:tabular-nums}
.table-wrap{overflow-x:auto}
.check{display:grid;grid-template-columns:24px 1fr;gap:4px 8px;padding:8px 0;border-bottom:1px solid var(--grid)}
.check .icon{font-weight:700}.check.pass .icon{color:var(--good)}.check.fail .icon{color:var(--critical)}
.check ul{margin:4px 0;padding-left:18px;color:var(--ink2);font-size:13px}
details summary{cursor:pointer;color:var(--ink2);font-size:13px;margin-top:8px}
code{font-size:12px;word-break:break-all}
"#;

/// Render the HTML report.
pub fn render(report: &Report) -> String {
    let hash = base64(&Sha256::digest(STYLE.as_bytes()));
    let mut page = String::new();
    let title = format!(
        "Goblin Pizza bench {} — {}",
        report.settings.preset.unwrap_or("custom"),
        if report.passed { "PASS" } else { "FAIL" }
    );
    let _ = write!(
        page,
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">\
         <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\
         <meta http-equiv=\"Content-Security-Policy\" content=\"default-src 'none'; style-src 'sha256-{hash}'; img-src data:; base-uri 'none'; form-action 'none'\">\
         <title>{}</title><style>{STYLE}</style></head><body><main>",
        esc(&title)
    );
    header(&mut page, report);
    tiles(&mut page, report);
    checklist(&mut page, report);
    comparison(&mut page, report);
    groups_table(&mut page, report);
    charts(&mut page, report);
    latency(&mut page, report);
    chaos(&mut page, report);
    errors(&mut page, report);
    drones(&mut page, report);
    resources(&mut page, report);
    crate::otel::render(&mut page, report.otel.as_ref());
    crate::profile::render(&mut page, report.cpu_profile.as_ref());
    metadata(&mut page, report);
    page.push_str("</main></body></html>\n");
    page
}

fn header(page: &mut String, report: &Report) {
    let (class, icon, word) = if report.passed {
        ("pass", "✓", "PASS")
    } else {
        ("fail", "✕", "FAIL")
    };
    let _ = write!(
        page,
        "<p class=\"muted small\">Flower · flower-bench · run {} · {}</p>\
         <h1>Goblin Pizza Express: {}</h1>\
         <p><span class=\"verdict {class}\"><span class=\"icon\" aria-hidden=\"true\">{icon}</span>{word}</span> {}</p>\
         <p class=\"muted\">{}{}</p>",
        esc(&report.run_id),
        esc(&report.started_at),
        esc(report.settings.preset.unwrap_or("custom run")),
        esc(&report.verdict),
        esc(&report.settings.describe()),
        if report.interrupted {
            " · interrupted: Ctrl-C ended the load early"
        } else {
            ""
        }
    );
    if let Some(note) = report.notes.first() {
        let _ = write!(page, "<p class=\"small muted\">{}</p>", esc(note));
    }
}

/// Every group's audit as one checklist (each check holds where it held
/// in every group), the cross-group checks after it.
fn overall(report: &Report) -> Option<Audit> {
    let audits: Vec<&Audit> = report
        .groups
        .iter()
        .filter_map(|group| group.audit.as_ref())
        .collect();
    let mut audit = if audits.len() == 1 {
        audits[0].clone()
    } else {
        crate::audit::combine(&audits)?
    };
    if let Some(across) = &report.across {
        audit.checks.extend(across.checks.iter().cloned());
        audit.passed &= across.passed;
    }
    Some(audit)
}

fn tiles(page: &mut String, report: &Report) {
    let goodput = &report.goodput;
    // The slowest group's failover: when the run was serving again.
    let failover = report
        .groups
        .iter()
        .flat_map(|group| &group.chaos)
        .filter_map(|event| event.failover_ms)
        .reduce(f64::max);
    let audit = overall(report);
    let checks = audit
        .as_ref()
        .map(|audit| {
            format!(
                "{}/{}",
                audit.checks.iter().filter(|check| check.passed).count(),
                audit.checks.len()
            )
        })
        .unwrap_or_else(|| "—".to_string());
    page.push_str("<section class=\"tiles\" aria-label=\"Headline figures\">");
    let tile = |page: &mut String, hero: bool, label: &str, value: String| {
        let _ = write!(
            page,
            "<div class=\"tile{}\"><div class=\"label\">{}</div><div class=\"value\">{}</div></div>",
            if hero { " hero" } else { "" },
            esc(label),
            esc(&value)
        );
    };
    tile(
        page,
        true,
        "Goodput, calls per second",
        rate(goodput.per_second),
    );
    tile(
        page,
        false,
        "Write p99",
        ms(goodput.write_latency.percentiles.p99),
    );
    tile(
        page,
        false,
        "Read p99",
        ms(goodput.read_latency.percentiles.p99),
    );
    tile(
        page,
        false,
        "Failover",
        failover.map_or_else(|| "no kill".to_string(), |value| ms(Some(value))),
    );
    tile(page, false, "Audit checks", checks);
    if report.groups.len() > 1 {
        tile(page, false, "Raft groups", report.groups.len().to_string());
    }
    if let Some(audit) = &audit {
        tile(
            page,
            false,
            "Orders delivered",
            format!(
                "{} of {}",
                count(audit.totals.delivered as u64),
                count(audit.totals.orders as u64)
            ),
        );
    }
    page.push_str("</section>");
}

fn checklist(page: &mut String, report: &Report) {
    page.push_str("<h2>The audit</h2><p class=\"muted small\">Fresh reads after the drain, reconciled against every acknowledged effect. Any failed check fails the run.");
    if report.groups.len() > 1 {
        page.push_str(" Each group is audited on its own; a check holds here only where it held in every group, and the groups are then checked against each other.");
    }
    page.push_str("</p><div class=\"card\">");
    match overall(report).as_ref() {
        None => page.push_str("<p>The run ended before the audit.</p>"),
        Some(audit) => {
            for check in &audit.checks {
                let (class, icon) = if check.passed {
                    ("pass", "✓")
                } else {
                    ("fail", "✕")
                };
                let _ = write!(
                    page,
                    "<div class=\"check {class}\"><span class=\"icon\" aria-label=\"{}\">{icon}</span><div><strong>{}</strong><p class=\"small muted\">{}</p>",
                    if check.passed { "passed" } else { "failed" },
                    esc(check.title),
                    esc(&check.detail)
                );
                if !check.violations.is_empty() {
                    page.push_str("<ul>");
                    for violation in &check.violations {
                        let _ = write!(page, "<li>{}</li>", esc(violation));
                    }
                    page.push_str("</ul>");
                }
                page.push_str("</div></div>");
            }
        }
    }
    page.push_str("</div>");
}

/// The comparison with `--baseline`.
fn comparison(page: &mut String, report: &Report) {
    let Some(comparison) = &report.comparison else {
        return;
    };
    page.push_str("<h2>Against the baseline</h2>");
    let _ = write!(
        page,
        "<p class=\"muted small\">Baseline {} · {}. Two observations, not a statistical study: repeat both before drawing conclusions.</p>",
        esc(&comparison.baseline.run_id),
        esc(&comparison.baseline.started_at)
    );
    if !comparison.matched_fixed_work {
        let _ = write!(
            page,
            "<div class=\"card\"><p><strong>Exploratory: not matched fixed work.</strong></p><ul class=\"small\">{}</ul></div>",
            comparison
                .reasons
                .iter()
                .map(|reason| format!("<li>{}</li>", esc(reason)))
                .collect::<String>()
        );
    }
    page.push_str("<div class=\"card table-wrap\"><table><thead><tr><th>Measure</th><th class=\"num\">Baseline</th><th class=\"num\">This run</th><th class=\"num\">Change</th><th>Verdict</th></tr></thead><tbody>");
    for row in &comparison.rows {
        let _ = write!(
            page,
            "<tr><td>{}</td><td class=\"num\">{}</td><td class=\"num\">{}</td><td class=\"num\">{}</td><td>{}</td></tr>",
            esc(&row.measure),
            esc(&crate::compare::figure(row.baseline, &row.unit)),
            esc(&crate::compare::figure(row.candidate, &row.unit)),
            row.change_percent
                .map_or("—".to_string(), |change| format!("{change:+.1}%")),
            esc(&row.verdict)
        );
    }
    let _ = write!(
        page,
        "</tbody></table><p class=\"small muted\">Regressions beyond {:.1}% fail `flower-bench compare`; this run's own verdict rests on its audit.</p></div>",
        comparison.threshold_percent
    );
}

/// Each group's tenants, databases, goodput, latency and audit.
fn groups_table(page: &mut String, report: &Report) {
    if report.groups.len() < 2 && report.placements.is_empty() {
        return;
    }
    page.push_str("<h2>Groups</h2><p class=\"muted small\">Each group's own customers, drones and audit; the run's goodput merges every group's calls over one synchronized window, and its percentiles merge their buckets.</p>");
    if let Some(kill) = &report.kill {
        let _ = write!(
            page,
            "<p class=\"small\">n{} was killed {:.1} s into the window; it led {} of {} groups ({}).</p>",
            kill.killed,
            kill.killed_at_s,
            kill.led.len(),
            kill.groups,
            esc(&kill.led.join(", "))
        );
    }
    page.push_str("<div class=\"card table-wrap\"><table><thead><tr><th>Group</th><th>Tenants</th><th>Databases</th><th class=\"num\">Goodput /s</th><th class=\"num\">Write p99</th><th class=\"num\">Read p99</th><th class=\"num\">Orders</th><th class=\"num\">Failover</th><th>Audit</th></tr></thead><tbody>");
    for group in &report.groups {
        let goodput = group.goodput.as_ref();
        let audit = group.audit.as_ref();
        let _ = write!(
            page,
            "<tr><td>{}</td><td>{}</td><td><code>{}</code></td><td class=\"num\">{}</td><td class=\"num\">{}</td><td class=\"num\">{}</td><td class=\"num\">{}</td><td class=\"num\">{}</td><td>{}</td></tr>",
            esc(&group.name),
            esc(&group.tenants.join(", ")),
            esc(&group.cluster.databases.join(", ")),
            goodput.map_or("—".to_string(), |goodput| rate(goodput.per_second)),
            ms(goodput.and_then(|goodput| goodput.write_latency.percentiles.p99)),
            ms(goodput.and_then(|goodput| goodput.read_latency.percentiles.p99)),
            audit.map_or("—".to_string(), |audit| count(audit.totals.orders as u64)),
            ms(group
                .chaos
                .iter()
                .filter_map(|event| event.failover_ms)
                .reduce(f64::max)),
            match audit {
                Some(audit) if audit.passed => "✓ passed".to_string(),
                Some(audit) => format!(
                    "✕ {}",
                    audit
                        .checks
                        .iter()
                        .filter(|check| !check.passed)
                        .map(|check| check.id)
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
                None => "—".to_string(),
            }
        );
    }
    page.push_str("</tbody></table></div>");
    if !report.placements.is_empty() {
        page.push_str("<div class=\"card table-wrap\"><h3>The catalog after the run</h3><table><thead><tr><th>Partition</th><th>Group</th><th class=\"num\">Epoch</th><th>Status</th></tr></thead><tbody>");
        for placement in &report.placements {
            let _ = write!(
                page,
                "<tr><td><code>{}</code></td><td>{}</td><td class=\"num\">{}</td><td>{}</td></tr>",
                esc(&placement.partition),
                esc(&placement.group),
                placement.epoch,
                esc(&placement.status)
            );
        }
        page.push_str("</tbody></table></div>");
    }
}

/// One plotted series.
struct Series<'a> {
    label: &'a str,
    slot: u8,
    values: Vec<Option<f64>>,
}

/// Round a maximum up to a clean axis value with its step (the admin
/// dashboard's `niceScale`).
pub fn nice_scale(max: f64, ticks: u32) -> (f64, f64) {
    if !max.is_finite() || max <= 0.0 {
        return (1.0, 0.5);
    }
    let rough = max / f64::from(ticks);
    let power = 10f64.powf(rough.log10().floor());
    let step = [1.0, 2.0, 2.5, 5.0, 10.0]
        .iter()
        .map(|factor| factor * power)
        .find(|candidate| *candidate >= rough)
        .unwrap_or(10.0 * power);
    (step * (max / step - 1e-9).ceil(), step)
}

fn axis_value(value: f64) -> String {
    if value >= 1000.0 {
        format!("{}K", rate(value / 1000.0).trim_end_matches(".0"))
    } else if value.fract() == 0.0 {
        format!("{value:.0}")
    } else {
        format!("{value:.1}")
    }
}

/// A time-series chart: lines on one y axis, chaos marked, a tooltip
/// per second and a table view.
fn time_chart(
    page: &mut String,
    title: &str,
    subtitle: &str,
    times: &[f64],
    series: &[Series<'_>],
    unit: &str,
    marks: &[(f64, Option<f64>, String)],
) {
    const WIDTH: f64 = 760.0;
    const HEIGHT: f64 = 220.0;
    let (left, right, top, bottom) = (52.0, 12.0, 12.0, 26.0);
    let inner = (WIDTH - left - right, HEIGHT - top - bottom);
    let max = series
        .iter()
        .flat_map(|series| series.values.iter().flatten())
        .fold(0.0f64, |max, value| max.max(*value));
    let (ymax, step) = nice_scale(max, 3);
    let (from, to) = match (times.first(), times.last()) {
        (Some(first), Some(last)) if last > first => (*first, *last),
        (Some(first), _) => (*first, first + 1.0),
        _ => (0.0, 1.0),
    };
    let x = |t: f64| left + (t - from) / (to - from) * inner.0;
    let y = |value: f64| top + inner.1 - value / ymax * inner.1;
    let _ = write!(
        page,
        "<figure class=\"card\"><h3>{}</h3><p class=\"small muted\">{}</p>",
        esc(title),
        esc(subtitle)
    );
    if series.len() > 1 {
        page.push_str("<ul class=\"legend\">");
        for series in series {
            let _ = write!(
                page,
                "<li><span class=\"key k{}\" aria-hidden=\"true\"></span>{}</li>",
                series.slot,
                esc(series.label)
            );
        }
        page.push_str("</ul>");
    }
    let _ = write!(
        page,
        "<svg class=\"chart\" viewBox=\"0 0 {WIDTH} {HEIGHT}\" role=\"img\" aria-label=\"{}\">",
        esc(title)
    );
    // Gridlines and y ticks.
    let mut tick = 0.0;
    while tick <= ymax + 1e-9 {
        let at = y(tick);
        let _ = write!(
            page,
            "<line class=\"{}\" x1=\"{left}\" x2=\"{}\" y1=\"{at:.1}\" y2=\"{at:.1}\"/><text class=\"tick\" x=\"{}\" y=\"{:.1}\" text-anchor=\"end\">{}</text>",
            if tick == 0.0 { "baseline" } else { "gridline" },
            WIDTH - right,
            left - 6.0,
            at + 4.0,
            axis_value(tick)
        );
        tick += step;
    }
    // X ticks.
    let span = to - from;
    let every = [5.0, 10.0, 15.0, 30.0, 60.0, 120.0, 300.0]
        .into_iter()
        .find(|every| span / every <= 8.0)
        .unwrap_or(600.0);
    let mut t = (from / every).ceil() * every;
    while t <= to {
        let _ = write!(
            page,
            "<text class=\"tick\" x=\"{:.1}\" y=\"{}\" text-anchor=\"middle\">{t:.0}s</text>",
            x(t),
            HEIGHT - 8.0
        );
        t += every;
    }
    // Chaos: the outage band and the kill line.
    for (kill, recovered, label) in marks {
        if *kill < from || *kill > to {
            continue;
        }
        let end = recovered.unwrap_or(*kill).min(to);
        let _ = write!(
            page,
            "<rect class=\"band\" x=\"{:.1}\" y=\"{top}\" width=\"{:.1}\" height=\"{}\"/><line class=\"kill\" x1=\"{:.1}\" x2=\"{:.1}\" y1=\"{top}\" y2=\"{}\"/><text class=\"marklabel\" x=\"{:.1}\" y=\"{}\">{}</text>",
            x(*kill),
            (x(end) - x(*kill)).max(1.0),
            inner.1,
            x(*kill),
            x(*kill),
            top + inner.1,
            x(*kill) + 4.0,
            top + 12.0,
            esc(label)
        );
    }
    // Lines, broken at missing values.
    for series in series {
        let mut segments: Vec<Vec<(f64, f64)>> = vec![Vec::new()];
        for (t, value) in times.iter().zip(&series.values) {
            match value {
                Some(value) => segments
                    .last_mut()
                    .expect("never empty")
                    .push((x(*t), y(*value))),
                None if !segments.last().expect("never empty").is_empty() => {
                    segments.push(Vec::new())
                }
                None => {}
            }
        }
        for segment in segments.iter().filter(|segment| !segment.is_empty()) {
            let points: Vec<String> = segment
                .iter()
                .map(|(px, py)| format!("{px:.1},{py:.1}"))
                .collect();
            let _ = write!(
                page,
                "<polyline class=\"line l{}\" points=\"{}\"/>",
                series.slot,
                points.join(" ")
            );
        }
    }
    // One hover slot per second, naming every series.
    let slot_width = inner.0 / times.len().max(1) as f64;
    for (index, t) in times.iter().enumerate() {
        let mut tip = format!("{t:.0} s");
        for series in series {
            let value = series.values[index]
                .map_or("—".to_string(), |value| format!("{value:.1} {unit}"));
            let _ = write!(tip, "\n{}: {value}", series.label);
        }
        let _ = write!(
            page,
            "<rect class=\"slot\" x=\"{:.1}\" y=\"{top}\" width=\"{slot_width:.1}\" height=\"{}\"><title>{}</title></rect>",
            x(*t) - slot_width / 2.0,
            inner.1,
            esc(&tip)
        );
    }
    page.push_str("</svg><details><summary>Show as a table</summary><div class=\"table-wrap\"><table><thead><tr><th class=\"num\">Second</th>");
    for series in series {
        let _ = write!(
            page,
            "<th class=\"num\">{} ({})</th>",
            esc(series.label),
            esc(unit)
        );
    }
    page.push_str("</tr></thead><tbody>");
    for (index, t) in times.iter().enumerate() {
        let _ = write!(page, "<tr><td class=\"num\">{t:.0}</td>");
        for series in series {
            let _ = write!(
                page,
                "<td class=\"num\">{}</td>",
                series.values[index].map_or("—".to_string(), |value| format!("{value:.1}"))
            );
        }
        page.push_str("</tr>");
    }
    page.push_str("</tbody></table></div></details></figure>");
}

fn charts(page: &mut String, report: &Report) {
    let points: Vec<&TimelinePoint> = report
        .timeline
        .iter()
        .filter(|point| point.phase == crate::metrics::Phase::Load)
        .collect();
    if points.is_empty() {
        return;
    }
    let times: Vec<f64> = points.iter().map(|point| point.t).collect();
    let marks: Vec<(f64, Option<f64>, String)> = report
        .groups
        .iter()
        .flat_map(|group| &group.chaos)
        .map(|event| {
            (
                event.killed_at_s,
                event.failover_ms.map(|ms| event.killed_at_s + ms / 1000.0),
                format!(
                    "n{} killed{}",
                    event.killed,
                    event.failover_ms.map_or(String::new(), |ms| format!(
                        " · writes back after {}",
                        self::ms(Some(ms))
                    ))
                ),
            )
        })
        .collect();
    page.push_str("<h2>Over the measured window</h2>");
    time_chart(
        page,
        "Goodput",
        "Successful customer calls per second; the red band runs from the leader kill to the first acknowledged write.",
        &times,
        &[
            Series {
                label: "Reads",
                slot: 1,
                values: points
                    .iter()
                    .map(|point| Some(point.reads_per_second))
                    .collect(),
            },
            Series {
                label: "Orders and tips",
                slot: 2,
                values: points
                    .iter()
                    .map(|point| Some(point.writes_per_second))
                    .collect(),
            },
        ],
        "/s",
        &marks,
    );
    time_chart(
        page,
        "Latency, p99 each second",
        "Each second's own p99 of customer calls answered in it, retries included.",
        &times,
        &[
            Series {
                label: "Reads",
                slot: 1,
                values: points.iter().map(|point| point.read_p99).collect(),
            },
            Series {
                label: "Orders and tips",
                slot: 2,
                values: points.iter().map(|point| point.write_p99).collect(),
            },
        ],
        "ms",
        &marks,
    );
    time_chart(
        page,
        "Errors and retries",
        "Failed attempts and retry attempts per second, every caller.",
        &times,
        &[
            Series {
                label: "Failed attempts",
                slot: 3,
                values: points
                    .iter()
                    .map(|point| Some(point.failed_attempts_per_second))
                    .collect(),
            },
            Series {
                label: "Retries",
                slot: 4,
                values: points
                    .iter()
                    .map(|point| Some(point.retries_per_second))
                    .collect(),
            },
        ],
        "/s",
        &marks,
    );
}

/// A small histogram of one operation's latency on a shared log axis.
fn histogram_svg(snapshot: &HistogramSnapshot, low: f64, high: f64, writes: bool) -> String {
    const WIDTH: f64 = 220.0;
    const HEIGHT: f64 = 36.0;
    const BINS: usize = 44;
    let Some(histogram) = Histogram::from_snapshot(snapshot) else {
        return String::new();
    };
    let (low, high) = (low.max(0.001).ln(), high.max(0.002).ln());
    let mut bins = [0u64; BINS];
    for (bound, count) in histogram.nonzero() {
        let position = ((bound.max(0.001).ln() - low) / (high - low) * BINS as f64) as isize;
        bins[position.clamp(0, BINS as isize - 1) as usize] += count;
    }
    let most = *bins.iter().max().unwrap_or(&1).max(&1) as f64;
    let width = WIDTH / BINS as f64;
    let mut svg = format!(
        "<svg class=\"chart\" viewBox=\"0 0 {WIDTH} {HEIGHT}\" role=\"img\" aria-label=\"latency distribution\">"
    );
    for (index, count) in bins.iter().enumerate().filter(|(_, count)| **count > 0) {
        let height = (*count as f64 / most * (HEIGHT - 2.0)).max(1.0);
        let from = (low + (high - low) * index as f64 / BINS as f64).exp();
        let to = (low + (high - low) * (index + 1) as f64 / BINS as f64).exp();
        let _ = write!(
            svg,
            "<rect class=\"bar{}\" x=\"{:.1}\" y=\"{:.1}\" width=\"{:.1}\" height=\"{height:.1}\"><title>{} to {}: {} calls</title></rect>",
            if writes { " w" } else { "" },
            index as f64 * width + 1.0,
            HEIGHT - height,
            (width - 2.0).max(1.0),
            esc(&ms(Some(from))),
            esc(&ms(Some(to))),
            count
        );
    }
    svg.push_str("</svg>");
    svg
}

fn latency(page: &mut String, report: &Report) {
    let Some(load) = report.load() else {
        return;
    };
    let rows: Vec<_> = load
        .operations
        .iter()
        .filter(|(_, op)| op.calls > 0)
        .collect();
    let low = rows
        .iter()
        .filter_map(|(_, op)| op.latency.min)
        .fold(f64::INFINITY, f64::min);
    let high = rows
        .iter()
        .filter_map(|(_, op)| op.latency.percentiles.max)
        .fold(0.0f64, f64::max);
    page.push_str("<h2>Latency by operation</h2><p class=\"muted small\">Successful logical calls in the measured window, retries and leader discovery included; percentiles from merged 1% log buckets, never averaged. Distributions share one log axis.</p><div class=\"card table-wrap\"><table><thead><tr><th>Operation</th><th>Whose</th><th class=\"num\">Calls</th><th class=\"num\">Failed</th><th class=\"num\">p50</th><th class=\"num\">p90</th><th class=\"num\">p99</th><th class=\"num\">p99.9</th><th class=\"num\">Max</th><th>Distribution</th></tr></thead><tbody>");
    for (name, op) in rows {
        let p = &op.latency.percentiles;
        let _ = write!(
            page,
            "<tr><td><code>{}</code></td><td>{:?}</td><td class=\"num\">{}</td><td class=\"num\">{}</td><td class=\"num\">{}</td><td class=\"num\">{}</td><td class=\"num\">{}</td><td class=\"num\">{}</td><td class=\"num\">{}</td><td>{}</td></tr>",
            esc(name),
            op.class,
            count(op.calls),
            count(op.failed),
            ms(p.p50),
            ms(p.p90),
            ms(p.p99),
            ms(p.p999),
            ms(p.max),
            if low.is_finite() && high > 0.0 {
                histogram_svg(
                    &op.latency,
                    low,
                    high,
                    op.class == crate::metrics::Class::Mutation,
                )
            } else {
                String::new()
            }
        );
    }
    page.push_str("</tbody></table></div>");
    server_stages(page, load);
    let goodput = &report.goodput;
    let _ = write!(
        page,
        "<p class=\"small muted\">Goodput: {} — {} successful of {} customer calls over {}; {} drone, {} replay and {} control completions excluded.</p>",
        esc(goodput.definition),
        count(goodput.completed),
        count(goodput.calls),
        esc(&ms(Some(goodput.duration_ms))),
        count(goodput.excluded_worker_completions),
        count(goodput.excluded_replay_completions),
        count(goodput.excluded_control_completions)
    );
    if let Some(offered) = &report.offered_load {
        let _ = write!(
            page,
            "<div class=\"card table-wrap\"><h3>Open-loop arrivals</h3><table><tbody><tr><td>Offered</td><td class=\"num\">{}</td></tr><tr><td>Dispatched</td><td class=\"num\">{}</td></tr><tr><td>Driver drops (every slot busy)</td><td class=\"num\">{}</td></tr><tr><td>Completed</td><td class=\"num\">{}</td></tr><tr><td>Failed</td><td class=\"num\">{}</td></tr><tr><td>Scheduling lag p99</td><td class=\"num\">{}</td></tr></tbody></table></div>",
            count(offered.offered),
            count(offered.dispatched),
            count(offered.driver_dropped),
            count(offered.completed),
            count(offered.failed),
            ms(offered.scheduling_lag_ms.percentiles.p99)
        );
    }
}

/// Where answered attempts spent their time on the server that
/// answered, from every reply's `Server-Timing` (unsampled).
fn server_stages(page: &mut String, load: &PhaseSummary) {
    const STAGES: [&str; 7] = [
        "forward",
        "fence",
        "admission",
        "queue",
        "eval",
        "commit",
        "total",
    ];
    let rows: Vec<_> = load
        .operations
        .iter()
        .filter(|(_, op)| !op.stages.is_empty())
        .collect();
    if rows.is_empty() {
        return;
    }
    page.push_str("<h3>Where the server spent the time</h3><p class=\"muted small\">Every answered attempt's <code>Server-Timing</code>, p50 · p99 per stage on the member that answered (a forward is the follower's round trip to the leader; the stages after it are the leader's). Stages overlap and do not add up to the total.</p><div class=\"card table-wrap\"><table><thead><tr><th>Operation</th>");
    for stage in STAGES {
        let _ = write!(page, "<th class=\"num\">{stage}</th>");
    }
    page.push_str("</tr></thead><tbody>");
    for (name, op) in rows {
        let _ = write!(page, "<tr><td><code>{}</code></td>", esc(name));
        for stage in STAGES {
            let cell = op
                .stages
                .get(stage)
                .and_then(Histogram::from_snapshot)
                .map_or("—".to_string(), |histogram| {
                    let p = histogram.percentiles();
                    format!("{} · {}", ms(p.p50), ms(p.p99))
                });
            let _ = write!(page, "<td class=\"num\">{}</td>", esc(&cell));
        }
        page.push_str("</tr>");
    }
    page.push_str("</tbody></table></div>");
}

fn chaos(page: &mut String, report: &Report) {
    let events: Vec<&ChaosEvent> = report
        .groups
        .iter()
        .flat_map(|group| &group.chaos)
        .collect();
    if events.is_empty() {
        return;
    }
    page.push_str("<h2>Chaos</h2>");
    let several = report.groups.len() > 1;
    for event in events {
        let heading = if several {
            format!(
                "{}: n{} killed {:.1} s into the window (pid {}); {}",
                event.group,
                event.killed,
                event.killed_at_s,
                event
                    .killed_pid
                    .map_or("?".to_string(), |pid| pid.to_string()),
                if event.led {
                    "it led this group"
                } else {
                    "this group's leader survived"
                }
            )
        } else {
            format!(
                "n{} killed {:.1} s into the window (pid {})",
                event.killed,
                event.killed_at_s,
                event
                    .killed_pid
                    .map_or("?".to_string(), |pid| pid.to_string())
            )
        };
        let _ = write!(
            page,
            "<div class=\"card table-wrap\"><h3>{}</h3><table><tbody>\
             <tr><td>New leader on /v1/status</td><td class=\"num\">{}</td><td>{}</td></tr>\
             <tr><td><strong>Failover: first acknowledged write</strong></td><td class=\"num\"><strong>{}</strong></td><td>{}</td></tr>\
             <tr><td>Killed member restarted</td><td class=\"num\">{}</td><td></td></tr>\
             <tr><td>Restarted member caught up</td><td class=\"num\">{}</td><td></td></tr>\
             <tr><td>Burst: failed attempts, retries, failed calls</td><td class=\"num\">{} · {} · {}</td><td>{}</td></tr>\
             </tbody></table>{}</div>",
            esc(&heading),
            ms(event.election_ms),
            event
                .new_leader
                .map_or(String::new(), |id| format!("n{id}")),
            ms(event.failover_ms),
            event
                .first_ack_member
                .map_or(String::new(), |id| format!("acknowledged by n{id}")),
            ms(event.restarted_ms),
            ms(event.caught_up_ms),
            count(event.burst.failed_attempts),
            count(event.burst.retries),
            count(event.burst.failed_calls),
            esc(&event
                .burst
                .errors
                .iter()
                .map(|(kind, n)| format!("{kind} {n}"))
                .collect::<Vec<_>>()
                .join(", ")),
            event.error.as_ref().map_or(String::new(), |error| format!(
                "<p class=\"small\">{}</p>",
                esc(error)
            ))
        );
    }
}

fn errors(page: &mut String, report: &Report) {
    page.push_str("<h2>Errors</h2><div class=\"card table-wrap\">");
    if report.errors.by_kind.is_empty() {
        page.push_str("<p>No attempt failed in the measured window.</p>");
    } else {
        page.push_str("<table><thead><tr><th>Kind</th><th class=\"num\">Failed attempts</th></tr></thead><tbody>");
        for (kind, n) in &report.errors.by_kind {
            let _ = write!(
                page,
                "<tr><td><code>{}</code></td><td class=\"num\">{}</td></tr>",
                esc(kind),
                count(*n)
            );
        }
        page.push_str("</tbody></table>");
    }
    let _ = write!(
        page,
        "<p class=\"small muted\">{} attempts, {} retries in the window; {} failed customer calls in the run.</p>",
        count(report.errors.attempts),
        count(report.errors.retries),
        count(report.errors.customer_failures)
    );
    for sample in report.errors.samples.iter().take(10) {
        let _ = write!(
            page,
            "<p class=\"small\"><code>{}</code> {}: {}</p>",
            esc(&sample.kind),
            esc(&sample.context),
            esc(&sample.message)
        );
    }
    page.push_str("</div>");
}

fn drones(page: &mut String, report: &Report) {
    if report.groups.is_empty() {
        return;
    }
    // Every group's drones, summed.
    let mut counters = report.groups[0].drones.counters.clone();
    for group in &report.groups[1..] {
        let more = &group.drones.counters;
        counters.claims += more.claims;
        counters.reclaimed += more.reclaimed;
        counters.delivered += more.delivered;
        counters.failed += more.failed;
        counters.abandoned += more.abandoned;
        counters.lost += more.lost;
        counters.fleet += more.fleet;
    }
    let refused = report
        .groups
        .iter()
        .flat_map(|group| &group.drones.fences)
        .filter(|probe| probe.rejected)
        .count();
    let _ = write!(
        page,
        "<h2>Drones and routing</h2><div class=\"card table-wrap\"><table><tbody>\
         <tr><td>Fleet</td><td class=\"num\">{}</td></tr><tr><td>Claims run</td><td class=\"num\">{}</td></tr>\
         <tr><td>Reclaimed after a lapsed lease</td><td class=\"num\">{}</td></tr><tr><td>Delivered</td><td class=\"num\">{}</td></tr>\
         <tr><td>Abandoned on purpose</td><td class=\"num\">{}</td></tr><tr><td>Stale tokens refused</td><td class=\"num\">{}</td></tr>\
         <tr><td>Leases lost under a handler</td><td class=\"num\">{}</td></tr></tbody></table></div>",
        counters.fleet,
        count(counters.claims),
        count(counters.reclaimed),
        count(counters.delivered),
        count(counters.abandoned),
        refused,
        count(counters.lost)
    );
    page.push_str("<div class=\"card table-wrap\"><table><thead><tr><th>Group</th><th>Member</th><th class=\"num\">Attempts</th><th class=\"num\">Answered</th><th class=\"num\">Failed</th></tr></thead><tbody>");
    for group in &report.groups {
        for route in &group.routing {
            let _ = write!(
                page,
                "<tr><td>{}</td><td>n{} <code>{}</code></td><td class=\"num\">{}</td><td class=\"num\">{}</td><td class=\"num\">{}</td></tr>",
                esc(&group.name),
                route.id,
                esc(&route.url),
                count(route.attempts),
                count(route.answered),
                count(route.failures)
            );
        }
    }
    page.push_str("</tbody></table></div>");
}

fn resources(page: &mut String, report: &Report) {
    page.push_str("<h2>Processes</h2><p class=\"muted small\">Sampled once a second. CPU counts only pairs of samples of one process inside the measured window; coverage says how much of it.</p><div class=\"card table-wrap\"><table><thead><tr><th>Process</th><th class=\"num\">CPU cores</th><th class=\"num\">Coverage</th><th class=\"num\">Peak RSS</th><th class=\"num\">PIDs</th></tr></thead><tbody>");
    for (name, summary) in &report.resources {
        let _ = write!(
            page,
            "<tr><td>{}</td><td class=\"num\">{}</td><td class=\"num\">{:.0} s</td><td class=\"num\">{}</td><td class=\"num\">{}</td></tr>",
            esc(name),
            summary
                .load_cpu_cores
                .map_or("—".to_string(), |cores| format!("{cores:.2}")),
            summary.covered_seconds,
            summary
                .peak_rss_bytes
                .map_or("—".to_string(), |bytes| format!(
                    "{:.0} MiB",
                    bytes as f64 / 1_048_576.0
                )),
            summary.pids.len()
        );
    }
    page.push_str("</tbody></table></div>");
}

fn metadata(page: &mut String, report: &Report) {
    page.push_str("<h2>Run metadata</h2><div class=\"card table-wrap\"><table><tbody>");
    let mut row = |label: &str, value: String| {
        let _ = write!(
            page,
            "<tr><td>{}</td><td><code>{}</code></td></tr>",
            esc(label),
            esc(&value)
        );
    };
    row("Schema", report.schema.to_string());
    row(
        "Run",
        format!(
            "{} · {} → {}",
            report.run_id, report.started_at, report.finished_at
        ),
    );
    row("Seed", report.settings.seed.clone());
    row(
        "Machine",
        format!(
            "{} {} · {} logical CPUs · {} · bench {} build",
            report.machine.os,
            report.machine.arch,
            report.machine.logical_cpus,
            report.machine.cpu.clone().unwrap_or_default(),
            report.machine.profile
        ),
    );
    if let Some(commit) = &report.machine.commit {
        row("Commit", commit.clone());
    }
    if let Some(server) = &report.binaries.server {
        row(
            "Server",
            format!("{} sha256 {}", server.path, server.sha256),
        );
    }
    row(
        "Guest",
        format!(
            "{} sha256 {}",
            report.binaries.guest.path, report.binaries.guest.sha256
        ),
    );
    if let Some(bench) = &report.binaries.bench {
        row("Bench", format!("{} sha256 {}", bench.path, bench.sha256));
    }
    page.push_str("</tbody></table>");
    let settings = serde_json::to_string_pretty(&report.settings).unwrap_or_default();
    let _ = write!(
        page,
        "<details><summary>Every setting</summary><pre><code>{}</code></pre></details>",
        esc(&settings)
    );
    for note in &report.notes {
        let _ = write!(page, "<p class=\"small muted\">{}</p>", esc(note));
    }
    page.push_str("</div>");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64_matches_the_standard() {
        assert_eq!(base64(b""), "");
        assert_eq!(base64(b"f"), "Zg==");
        assert_eq!(base64(b"fo"), "Zm8=");
        assert_eq!(base64(b"foo"), "Zm9v");
        assert_eq!(base64(b"foobar"), "Zm9vYmFy");
    }

    #[test]
    fn formatting() {
        assert_eq!(count(1_234_567), "1,234,567");
        assert_eq!(count(12), "12");
        assert_eq!(rate(1234.56), "1,234.6");
        assert_eq!(ms(Some(1500.0)), "1.50 s");
        assert_eq!(ms(Some(12.34)), "12.3 ms");
        assert_eq!(ms(None), "—");
        assert_eq!(nice_scale(87.0, 3), (100.0, 50.0));
        assert_eq!(nice_scale(250.0, 3), (300.0, 100.0));
        assert_eq!(nice_scale(0.0, 3), (1.0, 0.5));
        assert_eq!(esc("<a href='x'>&"), "&lt;a href=&#39;x&#39;&gt;&amp;");
    }
}
