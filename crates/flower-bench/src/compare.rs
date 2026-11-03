//! Comparing a run with a baseline and its report's before-and-after panel,
//! with a regression threshold.
//!
//! A report is summarized from its JSON (schemas `flower-bench/1` and `/2`),
//! never from a re-run: goodput and latency as recorded, and CPU per
//! successful customer call estimated from sampled CPU. Each sampled
//! process's CPU counts only between consecutive samples of the same process
//! inside the measured window; its mean cores over that covered time is
//! extrapolated across the whole window (`cores × window`), the servers'
//! estimates are summed and divided by successful primary customer calls.
//! Coverage stays visible, and a server without samples makes the server
//! estimate unavailable rather than zero.
//!
//! The comparison calls itself **matched fixed work** only when the criteria
//! hold: the same workload settings, machine and guest and bench binaries; no
//! diagnostics in either run; both passed with no failed customer call; open
//! loop at the same offered rate with every arrival served and the same count
//! of successful calls. Otherwise it is exploratory and says why. Different
//! server binaries are the point of a comparison and are never a reason.
//!
//! `flower-bench compare` fails (exit 1) when the candidate failed its
//! audit or a gated measure regressed beyond the threshold (default
//! 10%): goodput down, read or write p99 up, or server CPU per call up.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::Path;

use serde::Serialize;
use serde_json::Value;

use crate::report::Report;

/// The regression threshold, in percent, unless told otherwise.
pub const DEFAULT_THRESHOLD: f64 = 10.0;

/// Settings that define the workload: a difference makes a comparison
/// exploratory.
const WORKLOAD: [&str; 25] = [
    "groups",
    "topology",
    "replicas",
    "durationS",
    "warmupS",
    "concurrency",
    "offeredRate",
    "workers",
    "tenants",
    "stores",
    "hotStores",
    "hotProbability",
    "maxOrders",
    "bakeMs",
    "leaseMs",
    "duplicateRate",
    "abandonRate",
    "pollMs",
    "requestTimeoutMs",
    "retryBudgetMs",
    "seed",
    "readConsistency",
    "chaos",
    "auth",
    "targets",
];

/// Read a report.
pub fn read(path: &Path) -> Result<Value, String> {
    let bytes = std::fs::read(path).map_err(|error| format!("{}: {error}", path.display()))?;
    let value: Value = serde_json::from_slice(&bytes)
        .map_err(|error| format!("{}: not JSON: {error}", path.display()))?;
    match value["schema"].as_str() {
        Some(schema) if schema.starts_with("flower-bench/") => Ok(value),
        _ => Err(format!(
            "{}: not a flower-bench report (no flower-bench schema)",
            path.display()
        )),
    }
}

/// One side's CPU: servers, or the bench.
#[derive(Clone, Debug, Default, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Cpu {
    /// Processes expected.
    pub processes: usize,
    /// Those with CPU samples in the window.
    pub measured: usize,
    /// CPU observed over covered intervals, ms.
    pub observed_cpu_ms: f64,
    /// Lowest and highest share of the window a process's samples cover.
    pub coverage: Option<(f64, f64)>,
    /// Estimated CPU over the whole window, ms; `None` unless every
    /// process was measured.
    pub estimated_cpu_ms: Option<f64>,
    /// That estimate per successful customer call, µs.
    pub us_per_call: Option<f64>,
}

/// What a comparison reads from one report.
#[derive(Clone, Debug, Default, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Summary {
    /// Its schema.
    pub schema: String,
    /// Run ID.
    pub run_id: String,
    /// When it started.
    pub started_at: String,
    /// Its preset.
    pub preset: Option<String>,
    /// Whether it passed its audit.
    pub passed: bool,
    /// Whether diagnostics ran in its window.
    pub diagnostic: bool,
    /// Successful primary customer calls.
    pub completed: u64,
    /// Failed ones.
    pub failed: u64,
    /// The measured window, ms.
    pub duration_ms: f64,
    /// Goodput, calls per second.
    pub goodput: f64,
    /// Customer read latency, p50 and p99, ms.
    pub read_p50: Option<f64>,
    /// Read p99.
    pub read_p99: Option<f64>,
    /// Customer write latency, p50, ms.
    pub write_p50: Option<f64>,
    /// Write p99.
    pub write_p99: Option<f64>,
    /// Slowest failover after a kill, ms.
    pub failover_ms: Option<f64>,
    /// Open loop: offered, dispatched, completed, failed, dropped.
    pub offered: Option<[u64; 5]>,
    /// Server CPU.
    pub servers: Cpu,
    /// The bench's own CPU.
    pub bench: Cpu,
    /// Binary hashes: server, guest, bench.
    pub binaries: [Option<String>; 3],
    /// The machine: OS, architecture, CPU, logical CPUs.
    pub machine: Value,
    /// The workload settings.
    pub workload: BTreeMap<String, Value>,
}

fn f64_at(value: &Value, path: &[&str]) -> Option<f64> {
    path.iter()
        .try_fold(value, |value, key| value.get(*key))?
        .as_f64()
        .filter(|number| number.is_finite())
}

/// A latency percentile of a histogram snapshot, whose percentiles sit
/// beside its buckets.
fn percentile(goodput: &Value, histogram: &str, which: &str) -> Option<f64> {
    f64_at(goodput, &[histogram, which])
}

fn str_at(value: &Value, path: &[&str]) -> Option<String> {
    path.iter()
        .try_fold(value, |value, key| value.get(*key))?
        .as_str()
        .map(str::to_string)
}

/// Whether a resource name is a server process (`n1`, `g2-n3`).
fn is_server(name: &str) -> bool {
    let member = name.rsplit('-').next().unwrap_or(name);
    member.starts_with('n') && member[1..].chars().all(|c| c.is_ascii_digit()) && member.len() > 1
}

fn cpu(resources: &Value, duration_ms: f64, completed: u64, servers: bool) -> Cpu {
    let mut cpu = Cpu::default();
    let mut estimated = 0.0;
    let (mut low, mut high) = (f64::INFINITY, 0.0f64);
    if let Some(resources) = resources.as_object() {
        for (name, summary) in resources {
            if is_server(name) != servers || (!servers && name != "bench") {
                continue;
            }
            cpu.processes += 1;
            let (Some(cores), Some(covered)) = (
                f64_at(summary, &["loadCpuCores"]),
                f64_at(summary, &["coveredSeconds"]),
            ) else {
                continue;
            };
            if covered <= 0.0 || duration_ms <= 0.0 {
                continue;
            }
            cpu.measured += 1;
            cpu.observed_cpu_ms += cores * covered * 1000.0;
            estimated += cores * duration_ms;
            let coverage = (covered * 1000.0 / duration_ms).min(1.0);
            low = low.min(coverage);
            high = high.max(coverage);
        }
    }
    if cpu.measured > 0 {
        cpu.coverage = Some((low, high));
    }
    if cpu.processes > 0 && cpu.measured == cpu.processes {
        cpu.estimated_cpu_ms = Some(estimated);
        if completed > 0 {
            cpu.us_per_call = Some(estimated * 1000.0 / completed as f64);
        }
    }
    cpu
}

/// Summarize one report.
pub fn summarize(report: &Value) -> Result<Summary, String> {
    let goodput = &report["goodput"];
    let completed = goodput["completed"]
        .as_u64()
        .ok_or("the report has no goodput accounting")?;
    let duration_ms = f64_at(goodput, &["durationMs"]).ok_or("the report has no window")?;
    let settings = &report["settings"];
    let workload = WORKLOAD
        .iter()
        .map(|key| {
            let value = match (*key, &settings[*key]) {
                // Version 1 reports ran one group on one cluster.
                ("groups", Value::Null) => Value::from(1),
                ("topology", Value::Null) => Value::from("single"),
                ("auth", Value::Null) => Value::Bool(false),
                (_, value) => value.clone(),
            };
            (key.to_string(), value)
        })
        .collect();
    let offered = report["offeredLoad"].as_object().map(|offered| {
        [
            "offered",
            "dispatched",
            "completed",
            "failed",
            "driverDropped",
        ]
        .map(|key| offered.get(key).and_then(Value::as_u64).unwrap_or(0))
    });
    let failover = report["groups"]
        .as_array()
        .into_iter()
        .flatten()
        .flat_map(|group| group["chaos"].as_array().cloned().unwrap_or_default())
        .filter_map(|event| f64_at(&event, &["failoverMs"]))
        .reduce(f64::max);
    Ok(Summary {
        schema: str_at(report, &["schema"]).unwrap_or_default(),
        run_id: str_at(report, &["runId"]).unwrap_or_default(),
        started_at: str_at(report, &["startedAt"]).unwrap_or_default(),
        preset: str_at(settings, &["preset"]),
        passed: report["passed"].as_bool() == Some(true),
        diagnostic: !settings["otel"].is_null() || !settings["cpuProfile"].is_null(),
        completed,
        failed: goodput["failed"].as_u64().unwrap_or(0),
        duration_ms,
        goodput: f64_at(goodput, &["perSecond"]).unwrap_or(0.0),
        read_p50: percentile(goodput, "readLatency", "p50"),
        read_p99: percentile(goodput, "readLatency", "p99"),
        write_p50: percentile(goodput, "writeLatency", "p50"),
        write_p99: percentile(goodput, "writeLatency", "p99"),
        failover_ms: failover,
        offered,
        servers: cpu(&report["resources"], duration_ms, completed, true),
        bench: cpu(&report["resources"], duration_ms, completed, false),
        binaries: [
            str_at(report, &["binaries", "server", "sha256"]),
            str_at(report, &["binaries", "guest", "sha256"]),
            str_at(report, &["binaries", "bench", "sha256"]),
        ],
        machine: serde_json::json!({
            "os": report["machine"]["os"],
            "arch": report["machine"]["arch"],
            "cpu": report["machine"]["cpu"],
            "logicalCpus": report["machine"]["logicalCpus"],
        }),
        workload,
    })
}

/// One measure, compared.
#[derive(Clone, Debug, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Row {
    /// What is measured.
    pub measure: String,
    /// Its unit.
    pub unit: String,
    /// The baseline's figure.
    pub baseline: Option<f64>,
    /// The candidate's.
    pub candidate: Option<f64>,
    /// Candidate against baseline, percent.
    pub change_percent: Option<f64>,
    /// Whether more is better.
    pub higher_is_better: bool,
    /// Whether a regression past the threshold fails the comparison.
    pub gated: bool,
    /// Whether it regressed past the threshold.
    pub regressed: bool,
    /// One word: better, worse, unchanged, regressed, n/a.
    pub verdict: String,
}

/// A comparison.
#[derive(Clone, Debug, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Comparison {
    /// The baseline.
    pub baseline: Summary,
    /// The candidate.
    pub candidate: Summary,
    /// The regression threshold, percent.
    pub threshold_percent: f64,
    /// Whether the matched-fixed-work criteria hold.
    pub matched_fixed_work: bool,
    /// Why not, if not.
    pub reasons: Vec<String>,
    /// The measures.
    pub rows: Vec<Row>,
    /// Gated measures that regressed past the threshold.
    pub regressions: Vec<String>,
    /// No regression past the threshold, and the candidate passed.
    pub passed: bool,
    /// What the figures are.
    pub note: String,
}

impl Comparison {
    /// One line for the terminal.
    pub fn headline(&self) -> String {
        let change = |measure: &str| {
            self.rows
                .iter()
                .find(|row| row.measure == measure)
                .and_then(|row| row.change_percent)
                .map_or("—".to_string(), |change| format!("{change:+.1}%"))
        };
        format!(
            "goodput {}, write p99 {}, server CPU/call {}; {}{}",
            change("Goodput"),
            change("Write p99"),
            change("Server CPU per call"),
            if self.matched_fixed_work {
                "matched fixed work"
            } else {
                "exploratory"
            },
            if self.regressions.is_empty() {
                String::new()
            } else {
                format!("; REGRESSED: {}", self.regressions.join(", "))
            }
        )
    }
}

/// Format a figure in its unit.
pub fn figure(value: Option<f64>, unit: &str) -> String {
    match (value, unit) {
        (None, _) => "—".to_string(),
        (Some(value), "ms") => crate::report::ms(Some(value)),
        (Some(value), "calls/s") => crate::report::rate(value),
        (Some(value), "µs") => format!("{value:.1} µs"),
        (Some(value), _) => format!("{value:.2}"),
    }
}

/// Compare a candidate report with a baseline.
pub fn compare(baseline: &Value, candidate: &Value, threshold: f64) -> Comparison {
    let empty = Summary::default();
    let (a, b, mut reasons) = match (summarize(baseline), summarize(candidate)) {
        (Ok(a), Ok(b)) => (a, b, Vec::new()),
        (a, b) => {
            let mut reasons = Vec::new();
            if let Err(error) = &a {
                reasons.push(format!("baseline: {error}"));
            }
            if let Err(error) = &b {
                reasons.push(format!("candidate: {error}"));
            }
            (
                a.unwrap_or_else(|_| empty.clone()),
                b.unwrap_or_else(|_| empty.clone()),
                reasons,
            )
        }
    };
    for (key, value) in &a.workload {
        if b.workload.get(key) != Some(value) {
            reasons.push(format!("Changed workload.{key}"));
        }
    }
    if a.machine != b.machine {
        reasons.push("Changed machine".to_string());
    }
    if a.binaries[1].is_none() || a.binaries[1] != b.binaries[1] {
        reasons.push("Changed or unavailable guest binary".to_string());
    }
    if a.binaries[2].is_none() || a.binaries[2] != b.binaries[2] {
        reasons.push("Changed or unavailable bench binary (the customer driver)".to_string());
    }
    let open = |summary: &Summary| {
        summary
            .workload
            .get("offeredRate")
            .and_then(Value::as_f64)
            .unwrap_or(0.0)
            > 0.0
    };
    if !(open(&a) && open(&b)) {
        reasons.push("Closed-loop load: arrivals change with server speed; use matched --offered-rate for fixed-work CPU comparisons".to_string());
    }
    for (label, summary) in [("Baseline", &a), ("Candidate", &b)] {
        if !summary.passed || summary.failed != 0 {
            reasons.push(format!("{label} failed its audit or customer calls"));
        }
        if summary.servers.estimated_cpu_ms.is_none() {
            reasons.push(format!("{label} has incomplete server CPU measurements"));
        }
        if summary.diagnostic {
            reasons.push(format!("{label} has diagnostics enabled"));
        }
        if open(summary) {
            let served =
                summary
                    .offered
                    .is_some_and(|[offered, dispatched, completed, failed, dropped]| {
                        failed == 0
                            && dropped == 0
                            && offered == dispatched
                            && dispatched == completed
                            && completed == summary.completed
                    });
            if !served {
                reasons.push(format!(
                    "{label} did not successfully serve every offered primary call"
                ));
            }
        }
    }
    if open(&a) && open(&b) && a.completed != b.completed {
        reasons.push("Successful primary call counts differ under fixed offered load".to_string());
    }
    reasons.dedup();
    let row = |measure: &str,
               unit: &str,
               before: Option<f64>,
               after: Option<f64>,
               higher: bool,
               gated: bool| {
        let change = match (before, after) {
            (Some(before), Some(after)) if before > 0.0 => Some((after / before - 1.0) * 100.0),
            _ => None,
        };
        let worse = change.map(|change| if higher { -change } else { change });
        let regressed = gated && worse.is_some_and(|worse| worse > threshold);
        Row {
            measure: measure.to_string(),
            unit: unit.to_string(),
            baseline: before,
            candidate: after,
            change_percent: change,
            higher_is_better: higher,
            gated,
            regressed,
            verdict: match worse {
                None => "n/a".to_string(),
                Some(_) if regressed => "regressed".to_string(),
                Some(worse) if worse.abs() < 0.05 => "unchanged".to_string(),
                Some(worse) if worse < 0.0 => "better".to_string(),
                Some(_) => "worse".to_string(),
            },
        }
    };
    let rows = vec![
        row(
            "Goodput",
            "calls/s",
            Some(a.goodput),
            Some(b.goodput),
            true,
            true,
        ),
        row("Read p50", "ms", a.read_p50, b.read_p50, false, false),
        row("Read p99", "ms", a.read_p99, b.read_p99, false, true),
        row("Write p50", "ms", a.write_p50, b.write_p50, false, false),
        row("Write p99", "ms", a.write_p99, b.write_p99, false, true),
        row(
            "Server CPU per call",
            "µs",
            a.servers.us_per_call,
            b.servers.us_per_call,
            false,
            true,
        ),
        row(
            "Bench CPU per call",
            "µs",
            a.bench.us_per_call,
            b.bench.us_per_call,
            false,
            false,
        ),
        row("Failover", "ms", a.failover_ms, b.failover_ms, false, false),
    ];
    let regressions: Vec<String> = rows
        .iter()
        .filter(|row| row.regressed)
        .map(|row| {
            format!(
                "{} {:+.1}%",
                row.measure,
                row.change_percent.unwrap_or_default()
            )
        })
        .collect();
    Comparison {
        passed: regressions.is_empty() && b.passed,
        matched_fixed_work: reasons.is_empty(),
        baseline: a,
        candidate: b,
        threshold_percent: threshold,
        reasons,
        rows,
        regressions,
        note: "CPU counters include replication, retries, drone traffic and background server work; the denominator counts successful primary customer calls only. Each process's sampled CPU/wall ratio is extrapolated across the whole window, assuming uncovered intervals resemble covered ones; coverage, kills and restarts can bias it. Matching fixed offered work improves interpretability but removes neither sampling gaps nor scheduling noise: repeat matched runs, alternating binaries.".to_string(),
    }
}

/// The comparison as Markdown.
pub fn render_markdown(comparison: &Comparison) -> String {
    let mut out = String::new();
    let (a, b) = (&comparison.baseline, &comparison.candidate);
    let _ = writeln!(
        out,
        "{}\n",
        if comparison.matched_fixed_work {
            "Matched fixed offered work"
        } else {
            "Exploratory comparison; fixed-work criteria not met"
        }
    );
    let _ = writeln!(
        out,
        "Baseline {} ({}) · candidate {} ({})\n",
        a.run_id, a.started_at, b.run_id, b.started_at
    );
    let _ = writeln!(out, "| Measurement | Baseline | Candidate | Change |");
    let _ = writeln!(out, "| --- | ---: | ---: | ---: |");
    let _ = writeln!(
        out,
        "| Successful primary calls | {} | {} | |",
        a.completed, b.completed
    );
    let _ = writeln!(
        out,
        "| Failed primary calls | {} | {} | |",
        a.failed, b.failed
    );
    for row in &comparison.rows {
        let _ = writeln!(
            out,
            "| {}{} | {} | {} | {} |",
            row.measure,
            if row.regressed { " (regressed)" } else { "" },
            figure(row.baseline, &row.unit),
            figure(row.candidate, &row.unit),
            row.change_percent
                .map_or("—".to_string(), |change| format!("{change:+.1}%"))
        );
    }
    let coverage = |cpu: &Cpu| {
        cpu.coverage
            .map_or("unavailable".to_string(), |(low, high)| {
                format!("{:.1}–{:.1}%", low * 100.0, high * 100.0)
            })
    };
    let _ = writeln!(
        out,
        "| Observed server CPU seconds | {:.2} | {:.2} | |",
        a.servers.observed_cpu_ms / 1000.0,
        b.servers.observed_cpu_ms / 1000.0
    );
    let _ = writeln!(
        out,
        "| Server coverage per process | {} | {} | |",
        coverage(&a.servers),
        coverage(&b.servers)
    );
    let _ = writeln!(
        out,
        "| Bench coverage | {} | {} | |\n",
        coverage(&a.bench),
        coverage(&b.bench)
    );
    if comparison.regressions.is_empty() {
        let _ = writeln!(
            out,
            "No gated measure regressed beyond {:.1}%.",
            comparison.threshold_percent
        );
    } else {
        let _ = writeln!(
            out,
            "REGRESSED beyond {:.1}%: {}.",
            comparison.threshold_percent,
            comparison.regressions.join(", ")
        );
    }
    if !b.passed {
        let _ = writeln!(out, "The candidate failed its audit.");
    }
    for reason in &comparison.reasons {
        let _ = writeln!(out, "- {reason}");
    }
    let _ = writeln!(out, "\n{}", comparison.note);
    out
}

/// CPU per successful customer call, servers and bench, µs.
#[derive(Clone, Copy, Debug)]
pub struct CpuPerCall {
    /// Every server.
    pub servers: Option<f64>,
    /// The bench.
    pub bench: Option<f64>,
}

/// A finished report's CPU per call.
pub fn cpu_per_call(report: &Report) -> Option<CpuPerCall> {
    let value = serde_json::to_value(report).ok()?;
    let summary = summarize(&value).ok()?;
    Some(CpuPerCall {
        servers: summary.servers.us_per_call,
        bench: summary.bench.us_per_call,
    })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    /// A minimal report: `completed` calls in 10 s, three servers at
    /// `cores` each over 9 s of the window, the bench at half a core.
    pub(crate) fn report(completed: u64, cores: f64, write_p99: f64) -> Value {
        let server = json!({"loadCpuCores": cores, "coveredSeconds": 9.0});
        json!({
            "schema": "flower-bench/2",
            "runId": "r",
            "startedAt": "2026-10-31T00:00:00Z",
            "passed": true,
            "settings": {
                "preset": "stress", "groups": 8, "topology": "hosted", "replicas": 3,
                "durationS": 10.0, "offeredRate": 1000.0, "seed": "42", "otel": null,
                "cpuProfile": null, "auth": false,
            },
            "machine": {"os": "windows", "arch": "aarch64", "cpu": "x", "logicalCpus": 12},
            "binaries": {
                "server": {"sha256": "s1"}, "guest": {"sha256": "g"}, "bench": {"sha256": "b"},
            },
            "goodput": {
                "completed": completed, "failed": 0, "durationMs": 10_000.0,
                "perSecond": completed as f64 / 10.0,
                "readLatency": {"p50": 1.0, "p99": 5.0},
                "writeLatency": {"p50": 10.0, "p99": write_p99},
            },
            "offeredLoad": {
                "offered": completed, "dispatched": completed, "completed": completed,
                "failed": 0, "driverDropped": 0,
            },
            "resources": {
                "n1": server, "n2": server, "n3": server,
                "bench": {"loadCpuCores": 0.5, "coveredSeconds": 9.0},
            },
            "groups": [{"chaos": [{"failoverMs": 600.0}]}],
        })
    }

    #[test]
    fn cpu_per_call_extrapolates_over_the_window() {
        let summary = summarize(&report(10_000, 2.0, 50.0)).unwrap();
        // Three servers at 2 cores over a 10 s window: 60,000 ms of CPU
        // for 10,000 calls is 6,000 µs per call.
        assert_eq!(summary.servers.processes, 3);
        assert_eq!(summary.servers.estimated_cpu_ms, Some(60_000.0));
        assert_eq!(summary.servers.us_per_call, Some(6_000.0));
        assert_eq!(summary.servers.coverage, Some((0.9, 0.9)));
        assert!((summary.servers.observed_cpu_ms - 54_000.0).abs() < 1e-6);
        assert_eq!(summary.bench.us_per_call, Some(500.0));
        assert_eq!(summary.failover_ms, Some(600.0));
        // A server without samples makes the estimate unavailable.
        let mut partial = report(10_000, 2.0, 50.0);
        partial["resources"]["n2"] = json!({"loadCpuCores": null, "coveredSeconds": 0.0});
        assert_eq!(summarize(&partial).unwrap().servers.us_per_call, None);
        assert!(is_server("g3-n2") && is_server("n1") && !is_server("bench") && !is_server("n"));
    }

    #[test]
    fn matched_fixed_work_requires_matching_settings_and_complete_runs() {
        let base = report(10_000, 2.0, 50.0);
        let same = compare(&base, &report(10_000, 1.8, 48.0), 10.0);
        assert!(same.matched_fixed_work, "{:?}", same.reasons);
        assert!(same.passed);
        let row = same
            .rows
            .iter()
            .find(|row| row.measure == "Server CPU per call")
            .unwrap();
        assert!((row.change_percent.unwrap() + 10.0).abs() < 1e-9);
        assert_eq!(row.verdict, "better");
        // Another seed, a diagnostic, a closed loop or a dropped arrival
        // each make it exploratory, and say so.
        let mut seed = report(10_000, 2.0, 50.0);
        seed["settings"]["seed"] = json!("7");
        let mut otel = report(10_000, 2.0, 50.0);
        otel["settings"]["otel"] = json!(0.01);
        let mut closed = report(10_000, 2.0, 50.0);
        closed["settings"]["offeredRate"] = json!(0.0);
        let mut dropped = report(10_000, 2.0, 50.0);
        dropped["offeredLoad"]["driverDropped"] = json!(3);
        for (candidate, reason) in [
            (seed, "workload.seed"),
            (otel, "diagnostics"),
            (closed, "Closed-loop"),
            (dropped, "did not successfully serve"),
        ] {
            let comparison = compare(&base, &candidate, 10.0);
            assert!(!comparison.matched_fixed_work);
            assert!(
                comparison.reasons.iter().any(|text| text.contains(reason)),
                "{reason}: {:?}",
                comparison.reasons
            );
        }
        // A different server binary is the point of a comparison.
        let mut other = report(10_000, 2.0, 50.0);
        other["binaries"]["server"]["sha256"] = json!("s2");
        assert!(compare(&base, &other, 10.0).matched_fixed_work);
    }

    #[test]
    fn regressions_beyond_the_threshold_fail() {
        let base = report(10_000, 2.0, 50.0);
        // 20% more server CPU per call and a write p99 up 30%.
        let worse = compare(&base, &report(10_000, 2.4, 65.0), 10.0);
        assert!(!worse.passed);
        assert_eq!(worse.regressions.len(), 2, "{:?}", worse.regressions);
        assert!(
            worse
                .regressions
                .iter()
                .any(|text| text.starts_with("Server CPU per call"))
        );
        assert!(
            worse
                .regressions
                .iter()
                .any(|text| text.starts_with("Write p99"))
        );
        // Within a looser threshold it passes.
        assert!(compare(&base, &report(10_000, 2.4, 65.0), 35.0).passed);
        // Goodput down 20% (closed loop: fewer calls) regresses too.
        let fewer = compare(&base, &report(8_000, 2.0, 50.0), 10.0);
        assert!(
            fewer
                .regressions
                .iter()
                .any(|text| text.starts_with("Goodput"))
        );
        // An ungated measure never fails it.
        let mut slow = report(10_000, 2.0, 50.0);
        slow["goodput"]["writeLatency"]["p50"] = json!(100.0);
        assert!(compare(&base, &slow, 10.0).passed);
        // A candidate that failed its audit fails the comparison.
        let mut failed = report(10_000, 2.0, 50.0);
        failed["passed"] = json!(false);
        assert!(!compare(&base, &failed, 10.0).passed);
        let text = render_markdown(&worse);
        assert!(text.contains("REGRESSED") && text.contains("| Server CPU per call (regressed) |"));
    }

    /// A report read back from its file holds the figures it was written
    /// with, to the bit: serde_json's default parser rounds these one ulp
    /// off, so a comparison of a report with itself could see a change.
    #[test]
    fn a_report_reads_back_every_figure_exactly() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("report.json");
        for p99 in [
            54.781751339831196,
            59.376870092717276,
            54.745479877983556,
            57.855272891060004,
            53.341468365222156,
        ] {
            std::fs::write(&path, report(1, 1.0, p99).to_string()).unwrap();
            let summary = summarize(&read(&path).unwrap()).unwrap();
            assert_eq!(summary.write_p99.map(f64::to_bits), Some(p99.to_bits()));
        }
    }

    #[test]
    fn latencies_are_read_from_goodput_as_reports_serialize_it() {
        use crate::metrics::{Class, Phase, Recorder, goodput};
        let recorder = Recorder::new();
        let start = std::time::Instant::now();
        recorder.enter(Phase::Load, start);
        recorder.call("pizza.shop.local", Class::Read, 3.0, true);
        recorder.call("pizza.order", Class::Mutation, 40.0, true);
        let load = recorder
            .phases(start + std::time::Duration::from_secs(1))
            .into_iter()
            .find(|phase| phase.phase == Phase::Load)
            .unwrap();
        let mut value = report(2, 1.0, 1.0);
        value["goodput"] = serde_json::to_value(goodput(&load)).unwrap();
        let summary = summarize(&value).unwrap();
        let near = |value: Option<f64>, want: f64| {
            value.is_some_and(|got| (got - want).abs() / want < 0.02)
        };
        assert!(near(summary.read_p99, 3.0), "{:?}", summary.read_p99);
        assert!(near(summary.write_p99, 40.0), "{:?}", summary.write_p99);
        assert!(near(summary.write_p50, 40.0));
    }

    #[test]
    fn version_one_reports_compare_as_one_group() {
        let mut old = report(10_000, 2.0, 50.0);
        old["schema"] = json!("flower-bench/1");
        old["settings"]["groups"] = json!(1);
        if let Some(settings) = old["settings"].as_object_mut() {
            settings.remove("topology");
            settings.remove("auth");
        }
        let summary = summarize(&old).unwrap();
        assert_eq!(summary.workload["topology"], "single");
        assert_eq!(summary.workload["auth"], false);
        assert!(summarize(&json!({"schema": "flower-bench/2"})).is_err());
    }
}
