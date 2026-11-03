//! `flower-bench publish`: retain measured runs.
//!
//! Publishing validates a report and keeps it under this crate's
//! `results/` as `<preset>.json` (compacted: per-second process samples
//! dropped, every summary kept) with its HTML page, then rewrites the
//! results table between the markers in the crate's README from every
//! retained report. The retained reports are the baselines
//! `flower-bench compare` reads. `publish --check` validates every
//! retained report offline and fails when the README's table no longer
//! matches them; it never reruns the workload.
//!
//! A report is publishable only when it is a measurement: it passed its
//! audit (every group's, and across groups), it was not interrupted, no
//! diagnostic (OpenTelemetry, CPU profile) ran in its window, and its
//! goodput agrees with its own accounting: completed calls are reads
//! plus orders and tips, they match the load phase's successful
//! customer calls, the rate is those calls over the window, and each
//! group's goodput adds up to the run's.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use serde_json::Value;

use crate::compare::{self, Summary};

/// Marks the results table in the README.
pub const START: &str = "<!-- flower-bench results:start -->";
/// Ends it.
pub const END: &str = "<!-- flower-bench results:end -->";

/// This crate's directory, when the workspace is still there.
fn crate_dir() -> Option<PathBuf> {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    dir.join("Cargo.toml").is_file().then_some(dir)
}

/// Where results are retained by default.
pub fn default_dir() -> Option<PathBuf> {
    crate_dir().map(|dir| dir.join("results"))
}

/// The README whose table is rewritten by default.
pub fn default_readme() -> Option<PathBuf> {
    crate_dir().map(|dir| dir.join("README.md"))
}

fn close(a: f64, b: f64) -> bool {
    (a - b).abs() <= 1e-9 * a.abs().max(b.abs()).max(1.0)
}

/// Check that a report is a publishable measurement; returns the name
/// it is kept under.
pub fn validate(report: &Value) -> Result<String, String> {
    let schema = report["schema"].as_str().unwrap_or_default();
    if schema != "flower-bench/2" {
        return Err(format!(
            "only flower-bench/2 reports are published (this is {schema:?})"
        ));
    }
    if report["passed"].as_bool() != Some(true) {
        return Err(format!(
            "the run did not pass: {}",
            report["verdict"].as_str().unwrap_or("no verdict")
        ));
    }
    if report["interrupted"].as_bool() != Some(false) {
        return Err("the run was interrupted".to_string());
    }
    let settings = &report["settings"];
    if !settings["otel"].is_null() || !settings["cpuProfile"].is_null() {
        return Err(
            "diagnostics ran in the window: a diagnostic run is not a measurement".to_string(),
        );
    }
    let goodput = &report["goodput"];
    let completed = goodput["completed"].as_u64().ok_or("no goodput")?;
    let reads = goodput["reads"]["completed"].as_u64().ok_or("no reads")?;
    let mutations = goodput["mutations"]["completed"]
        .as_u64()
        .ok_or("no mutations")?;
    if completed == 0 || reads + mutations != completed {
        return Err("completed calls are not reads plus orders and tips".to_string());
    }
    let duration = goodput["durationMs"].as_f64().ok_or("no window")?;
    let rate = goodput["perSecond"].as_f64().ok_or("no rate")?;
    if duration <= 0.0 || !close(rate, completed as f64 / (duration / 1000.0)) {
        return Err("the goodput is not the completed calls over the window".to_string());
    }
    let load = report["phases"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|phase| phase["phase"] == "load")
        .ok_or("no load phase")?;
    if !close(load["durationMs"].as_f64().unwrap_or(-1.0), duration) {
        return Err("the goodput window is not the load phase".to_string());
    }
    let customer: u64 = load["operations"]
        .as_object()
        .into_iter()
        .flatten()
        .filter(|(_, op)| op["class"] == "read" || op["class"] == "mutation")
        .filter_map(|(_, op)| op["completed"].as_u64())
        .sum();
    if customer != completed {
        return Err(format!(
            "the load phase holds {customer} successful customer calls, the goodput {completed}"
        ));
    }
    let groups = report["groups"].as_array().ok_or("no groups")?;
    if settings["groups"].as_u64() != Some(groups.len() as u64) {
        return Err("the groups do not match the settings".to_string());
    }
    let mut sum = 0;
    for group in groups {
        if group["audit"]["passed"].as_bool() != Some(true) {
            return Err(format!(
                "group {} did not pass its audit",
                group["name"].as_str().unwrap_or("?")
            ));
        }
        sum += group["goodput"]["completed"].as_u64().unwrap_or(0);
    }
    if sum != completed {
        return Err(format!(
            "the groups' goodput adds to {sum}, the run's is {completed}"
        ));
    }
    if !report["across"].is_null() && report["across"]["passed"].as_bool() != Some(true) {
        return Err("the cross-group audit failed".to_string());
    }
    for binary in ["server", "guest"] {
        if report["binaries"][binary]["sha256"]
            .as_str()
            .unwrap_or("")
            .is_empty()
        {
            return Err(format!("no {binary} binary identity"));
        }
    }
    let preset = settings["preset"].as_str().unwrap_or("custom");
    Ok(if settings["auth"].as_bool() == Some(true) {
        format!("{preset}-auth")
    } else {
        preset.to_string()
    })
}

/// A report as retained: per-second process samples dropped (their
/// summaries stay).
pub fn compact(report: &Value) -> Value {
    let mut report = report.clone();
    if let Some(resources) = report["resources"].as_object_mut() {
        for summary in resources.values_mut() {
            if let Some(summary) = summary.as_object_mut() {
                summary.remove("samples");
            }
        }
    }
    report
}

/// Whether a retained name is acceptable: lowercase letters, digits and
/// dashes, 1 to 40 of them.
fn valid_name(name: &str) -> bool {
    (1..=40).contains(&name.len())
        && name
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
}

/// The name a retained report is kept under: the one it was published
/// as (`publishedAs`), else its preset's.
fn retained_name(report: &Value) -> Result<String, String> {
    let preset = validate(report)?;
    Ok(report["publishedAs"]
        .as_str()
        .map_or(preset, str::to_string))
}

/// Validate `report` (read from `source`, whose HTML page sits beside it
/// or where its settings say) and retain it in `dir` under `name` (else
/// its preset's); returns the name.
pub fn retain(source: &Path, dir: &Path, name: Option<&str>) -> Result<String, String> {
    let report = compare::read(source)?;
    let preset = validate(&report).map_err(|error| format!("{}: {error}", source.display()))?;
    let name = match name {
        Some(name) if valid_name(name) => name.to_string(),
        Some(name) => {
            return Err(format!(
                "{name:?}: a result's name is 1 to 40 lowercase letters, digits and dashes"
            ));
        }
        None => preset,
    };
    let html_source = [
        source.with_extension("html"),
        PathBuf::from(report["settings"]["html"].as_str().unwrap_or_default()),
    ]
    .into_iter()
    .find(|path| path.is_file())
    .ok_or_else(|| format!("{}: its HTML report is missing", source.display()))?;
    let html = std::fs::read_to_string(&html_source)
        .map_err(|error| format!("{}: {error}", html_source.display()))?;
    let run = report["runId"].as_str().unwrap_or_default();
    if run.is_empty() || !html.contains(run) {
        return Err(format!(
            "{}: the HTML report is of another run",
            html_source.display()
        ));
    }
    std::fs::create_dir_all(dir).map_err(|error| format!("{}: {error}", dir.display()))?;
    let mut kept = compact(&report);
    kept["publishedAs"] = Value::String(name.clone());
    let json = serde_json::to_string_pretty(&kept).map_err(|error| error.to_string())?;
    std::fs::write(dir.join(format!("{name}.json")), json + "\n")
        .map_err(|error| format!("{}: {error}", dir.display()))?;
    std::fs::write(dir.join(format!("{name}.html")), html)
        .map_err(|error| format!("{}: {error}", dir.display()))?;
    Ok(name)
}

/// Every retained report, by name.
pub fn retained(dir: &Path) -> Result<BTreeMap<String, Value>, String> {
    let mut reports = BTreeMap::new();
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(reports),
        Err(error) => return Err(format!("{}: {error}", dir.display())),
    };
    for entry in entries {
        let path = entry.map_err(|error| error.to_string())?.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }
        let name = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .unwrap_or_default()
            .to_string();
        reports.insert(name, compare::read(&path)?);
    }
    Ok(reports)
}

/// The README's results table, from the retained reports. Links are
/// relative to the README (`results/<name>.html`).
pub fn table(reports: &BTreeMap<String, Value>, link: &str) -> String {
    let mut out = String::new();
    if reports.is_empty() {
        out.push_str("No results are retained yet.\n");
        return out;
    }
    out.push_str("| Run | Groups × replicas | Goodput (calls/s) | Writes p50 · p99 | Reads p99 | Server CPU / call | Failover | Audit | Measured |\n");
    out.push_str("|---|---|---:|---|---:|---:|---:|---|---|\n");
    for (name, report) in reports {
        let summary: Summary = match compare::summarize(report) {
            Ok(summary) => summary,
            Err(error) => {
                let _ = writeln!(out, "| {name} | unreadable: {error} | | | | | | | |");
                continue;
            }
        };
        let settings = &report["settings"];
        let ms = |value: Option<f64>| crate::report::ms(value);
        let _ = writeln!(
            out,
            "| [{name}]({link}/{name}.html) ([JSON]({link}/{name}.json)) | {} × {} ({}) | {} | {} · {} | {} | {} | {} | {} | {}, {} |",
            settings["groups"].as_u64().unwrap_or(1),
            settings["replicas"].as_u64().unwrap_or(0),
            settings["topology"].as_str().unwrap_or("single"),
            crate::report::rate(summary.goodput),
            ms(summary.write_p50),
            ms(summary.write_p99),
            ms(summary.read_p99),
            summary
                .servers
                .us_per_call
                .map_or("—".to_string(), |value| format!("{value:.0} µs")),
            ms(summary.failover_ms),
            if summary.passed { "passed" } else { "FAILED" },
            report["finishedAt"]
                .as_str()
                .unwrap_or("")
                .get(..10)
                .unwrap_or(""),
            report["machine"]["cpu"]
                .as_str()
                .unwrap_or("CPU not recorded")
                .trim(),
        );
    }
    out
}

/// Replace the text between the markers.
pub fn replace(readme: &str, table: &str) -> Result<String, String> {
    let start = readme
        .find(START)
        .ok_or("the README has no results start marker")?;
    let end = readme
        .find(END)
        .ok_or("the README has no results end marker")?;
    if end < start
        || readme[start + START.len()..].contains(START)
        || readme[end + END.len()..].contains(END)
    {
        return Err("the README needs exactly one pair of results markers".to_string());
    }
    Ok(format!(
        "{}\n{}{}",
        &readme[..start + START.len()],
        table,
        &readme[end..]
    ))
}

fn link(readme: &Path, dir: &Path) -> String {
    let base = readme.parent().unwrap_or(Path::new("."));
    dir.strip_prefix(base)
        .map(|relative| relative.to_string_lossy().replace('\\', "/"))
        .unwrap_or_else(|_| dir.display().to_string())
}

/// Publish `reports` into `dir` and rewrite `readme`'s table.
pub fn publish(
    reports: &[PathBuf],
    dir: &Path,
    readme: &Path,
    name: Option<&str>,
) -> Result<Vec<String>, String> {
    if reports.is_empty() {
        return Err("name the reports to publish (JSON), or pass --check".to_string());
    }
    if name.is_some() && reports.len() != 1 {
        return Err("--name names one report".to_string());
    }
    let mut names = Vec::new();
    for report in reports {
        names.push(retain(report, dir, name)?);
    }
    let text = std::fs::read_to_string(readme)
        .map_err(|error| format!("{}: {error}", readme.display()))?;
    let updated = replace(&text, &table(&retained(dir)?, &link(readme, dir)))?;
    std::fs::write(readme, updated).map_err(|error| format!("{}: {error}", readme.display()))?;
    Ok(names)
}

/// Validate every retained report and the README's table; returns the
/// problems found (none is a pass).
pub fn check(dir: &Path, readme: &Path) -> Result<Vec<String>, String> {
    let reports = retained(dir)?;
    let mut problems = Vec::new();
    for (name, report) in &reports {
        match retained_name(report) {
            Ok(expected) if &expected == name => {}
            Ok(expected) => problems.push(format!("{name}.json was published as {expected}")),
            Err(error) => problems.push(format!("{name}.json: {error}")),
        }
        let html = dir.join(format!("{name}.html"));
        match std::fs::read_to_string(&html) {
            Ok(text) if text.contains(report["runId"].as_str().unwrap_or("\u{0}")) => {}
            Ok(_) => problems.push(format!("{name}.html is of another run")),
            Err(error) => problems.push(format!("{}: {error}", html.display())),
        }
    }
    let text = std::fs::read_to_string(readme)
        .map_err(|error| format!("{}: {error}", readme.display()))?;
    match replace(&text, &table(&reports, &link(readme, dir))) {
        Ok(expected) if expected == text => {}
        Ok(_) => problems.push(format!(
            "{}'s results table does not match the retained results: run `flower-bench publish` again",
            readme.display()
        )),
        Err(error) => problems.push(error),
    }
    Ok(problems)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    /// A publishable report: 1,000 calls in 10 s over two groups.
    fn report() -> Value {
        let group = |name: &str, completed: u64| {
            json!({
                "name": name,
                "audit": {"passed": true},
                "goodput": {"completed": completed},
                "chaos": [],
            })
        };
        json!({
            "schema": "flower-bench/2",
            "runId": "abc123",
            "startedAt": "2026-10-31T10:00:00Z",
            "finishedAt": "2026-10-31T10:01:00Z",
            "passed": true,
            "interrupted": false,
            "verdict": "every audit check held",
            "settings": {
                "preset": "stress", "groups": 2, "replicas": 3, "topology": "hosted",
                "otel": null, "cpuProfile": null, "auth": false, "html": "",
            },
            "machine": {"os": "windows", "arch": "aarch64", "cpu": "Test CPU", "logicalCpus": 8},
            "binaries": {"server": {"sha256": "s"}, "guest": {"sha256": "g"}, "bench": {"sha256": "b"}},
            "goodput": {
                "completed": 1000, "failed": 0, "durationMs": 10_000.0, "perSecond": 100.0,
                "reads": {"completed": 700}, "mutations": {"completed": 300},
                "readLatency": {"p50": 1.0, "p99": 4.0},
                "writeLatency": {"p50": 9.0, "p99": 40.0},
            },
            "phases": [{"phase": "load", "durationMs": 10_000.0, "operations": {
                "pizza.shop.local": {"class": "read", "completed": 700},
                "pizza.order": {"class": "mutation", "completed": 100},
                "pizza.tip": {"class": "mutation", "completed": 200},
                "pizza.deliver": {"class": "worker", "completed": 99},
            }}],
            "resources": {
                "n1": {"loadCpuCores": 1.0, "coveredSeconds": 9.0, "samples": [{"t": 1}]},
                "bench": {"loadCpuCores": 0.2, "coveredSeconds": 9.0, "samples": []},
            },
            "groups": [group("g1", 600), group("g2", 400)],
            "across": {"passed": true},
        })
    }

    #[test]
    fn only_complete_measurements_publish() {
        assert_eq!(validate(&report()), Ok("stress".to_string()));
        let mut auth = report();
        auth["settings"]["auth"] = json!(true);
        assert_eq!(validate(&auth), Ok("stress-auth".to_string()));
        type Break = Box<dyn Fn(&mut Value)>;
        let broken: Vec<(&str, Break)> = vec![
            (
                "failed",
                Box::new(|r: &mut Value| r["passed"] = json!(false)),
            ),
            (
                "interrupted",
                Box::new(|r: &mut Value| r["interrupted"] = json!(true)),
            ),
            (
                "diagnostic",
                Box::new(|r: &mut Value| r["settings"]["otel"] = json!(0.01)),
            ),
            (
                "profiled",
                Box::new(|r: &mut Value| r["settings"]["cpuProfile"] = json!({})),
            ),
            (
                "rate",
                Box::new(|r: &mut Value| r["goodput"]["perSecond"] = json!(120.0)),
            ),
            (
                "mix",
                Box::new(|r: &mut Value| r["goodput"]["reads"]["completed"] = json!(600)),
            ),
            (
                "phase",
                Box::new(|r: &mut Value| {
                    r["phases"][0]["operations"]["pizza.tip"]["completed"] = json!(150)
                }),
            ),
            (
                "group",
                Box::new(|r: &mut Value| r["groups"][1]["audit"]["passed"] = json!(false)),
            ),
            (
                "sum",
                Box::new(|r: &mut Value| r["groups"][1]["goodput"]["completed"] = json!(300)),
            ),
            (
                "across",
                Box::new(|r: &mut Value| r["across"]["passed"] = json!(false)),
            ),
            (
                "binary",
                Box::new(|r: &mut Value| r["binaries"]["server"] = json!(null)),
            ),
            (
                "schema",
                Box::new(|r: &mut Value| r["schema"] = json!("flower-bench/1")),
            ),
        ];
        for (what, change) in broken {
            let mut report = report();
            change(&mut report);
            assert!(validate(&report).is_err(), "{what} should not publish");
        }
        let compacted = compact(&report());
        assert!(compacted["resources"]["n1"].get("samples").is_none());
        assert_eq!(compacted["resources"]["n1"]["loadCpuCores"], 1.0);
    }

    #[test]
    fn publishing_retains_reports_and_check_holds_the_readme_to_them() {
        let work = tempfile::tempdir().unwrap();
        let source = work.path().join("run.json");
        std::fs::write(&source, report().to_string()).unwrap();
        std::fs::write(work.path().join("run.html"), "<html>run abc123</html>").unwrap();
        let readme = work.path().join("README.md");
        std::fs::write(
            &readme,
            format!("# Bench\n\n{START}\nold\n{END}\n\nAfter.\n"),
        )
        .unwrap();
        let dir = work.path().join("results");
        assert_eq!(
            publish(std::slice::from_ref(&source), &dir, &readme, None).unwrap(),
            vec!["stress"]
        );
        let text = std::fs::read_to_string(&readme).unwrap();
        assert!(text.contains("[stress](results/stress.html)"));
        assert!(text.contains("| 2 × 3 (hosted) | 100.0 |"));
        assert!(text.contains("After."));
        assert!(!text.contains("old"));
        assert!(dir.join("stress.html").is_file());
        assert_eq!(check(&dir, &readme).unwrap(), Vec::<String>::new());

        // A retained report edited by hand fails the check.
        let retained = dir.join("stress.json");
        let mut edited: Value = serde_json::from_slice(&std::fs::read(&retained).unwrap()).unwrap();
        edited["goodput"]["perSecond"] = json!(1_000.0);
        std::fs::write(&retained, edited.to_string()).unwrap();
        let problems = check(&dir, &readme).unwrap();
        assert!(
            problems.iter().any(|problem| problem.contains("goodput")),
            "{problems:?}"
        );
        assert!(
            problems.iter().any(|problem| problem.contains("table")),
            "{problems:?}"
        );

        // So does a README table edited by hand.
        std::fs::write(&retained, compact(&report()).to_string()).unwrap();
        std::fs::write(&readme, text.replace("100.0", "999.0")).unwrap();
        let problems = check(&dir, &readme).unwrap();
        assert_eq!(problems.len(), 1, "{problems:?}");

        // A run published under a name of its own keeps it, and the
        // check holds the file to it.
        std::fs::write(&readme, &text).unwrap();
        assert_eq!(
            publish(
                std::slice::from_ref(&source),
                &dir,
                &readme,
                Some("hosted-64")
            )
            .unwrap(),
            vec!["hosted-64"]
        );
        let text = std::fs::read_to_string(&readme).unwrap();
        assert!(text.contains("[hosted-64](results/hosted-64.html)"));
        assert_eq!(check(&dir, &readme).unwrap(), Vec::<String>::new());
        std::fs::copy(dir.join("hosted-64.json"), dir.join("other.json")).unwrap();
        std::fs::copy(dir.join("hosted-64.html"), dir.join("other.html")).unwrap();
        let problems = check(&dir, &readme).unwrap();
        assert!(
            problems
                .iter()
                .any(|problem| problem.contains("published as hosted-64")),
            "{problems:?}"
        );
        std::fs::remove_file(dir.join("other.json")).unwrap();
        assert!(
            publish(
                std::slice::from_ref(&source),
                &dir,
                &readme,
                Some("Bad Name")
            )
            .is_err()
        );

        // A failed run is refused and changes nothing.
        let mut failed = report();
        failed["passed"] = json!(false);
        std::fs::write(&source, failed.to_string()).unwrap();
        assert!(publish(std::slice::from_ref(&source), &dir, &readme, None).is_err());
    }

    #[test]
    fn markers_must_be_one_pair() {
        assert!(replace("no markers", "t").is_err());
        assert!(replace(&format!("{END}{START}"), "t").is_err());
        assert!(replace(&format!("{START}{END}{START}{END}"), "t").is_err());
        assert_eq!(
            replace(&format!("a{START}x{END}b"), "T\n").unwrap(),
            format!("a{START}\nT\n{END}b")
        );
    }
}
