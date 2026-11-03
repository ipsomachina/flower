//! End to end, through the bench's own `run`: the smoke preset against
//! one real `cargo-flower serve` process; three replicas with the
//! leader killed halfway; three processes hosting two groups with each
//! tenant a partition and the busiest process killed; the authenticated
//! kitchen; and a diagnostic run capturing the servers' OpenTelemetry
//! (and, on Windows, sampling a server's stacks). Each must pass its
//! business audit; the chaos runs must have measured a failover, the
//! hosted run its partitions where they were placed, the authenticated
//! run every access rule.
//!
//! The server and the guests are built the way the binary builds them
//! (`artifacts::build_server` with this test's profile, the guests into
//! `target/wasm-guest` and `target/wasm-guest-auth`);
//! `FLOWER_BENCH_SERVER_BINARY` points at a prebuilt server instead.
//! The runs take turns so none's timing suffers from another's cluster.

use std::path::PathBuf;
use std::sync::OnceLock;

use clap::Parser;
use flower_bench::artifacts;
use flower_bench::config::{Cli, Settings};
use flower_bench::run::run;
use flower_client::CancellationToken;

/// One run at a time.
static TURN: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// The server and guest, built once per test binary.
fn binaries() -> &'static (PathBuf, PathBuf) {
    static BUILT: OnceLock<(PathBuf, PathBuf)> = OnceLock::new();
    BUILT.get_or_init(|| {
        let server = match std::env::var_os("FLOWER_BENCH_SERVER_BINARY") {
            Some(path) => PathBuf::from(path),
            None => artifacts::build_server().expect("building cargo-flower"),
        };
        let guest = artifacts::build_guest(false).expect("building the guest");
        (server, guest)
    })
}

/// The authenticated kitchen, built once.
fn auth_guest() -> &'static PathBuf {
    static BUILT: OnceLock<PathBuf> = OnceLock::new();
    BUILT.get_or_init(|| artifacts::build_guest(true).expect("building the authenticated guest"))
}

async fn settings(dir: &std::path::Path, args: &[&str]) -> Settings {
    let (server, guest) = tokio::task::spawn_blocking(binaries).await.unwrap();
    let guest = if args.contains(&"--auth") {
        tokio::task::spawn_blocking(auth_guest)
            .await
            .unwrap()
            .clone()
    } else {
        guest.clone()
    };
    let json = dir.join("report.json");
    let mut argv = vec![
        "flower-bench".to_string(),
        "--quiet".to_string(),
        "--server-binary".to_string(),
        server.display().to_string(),
        "--guest-wasm".to_string(),
        guest.display().to_string(),
        "--json".to_string(),
        json.display().to_string(),
    ];
    argv.extend(args.iter().map(|arg| arg.to_string()));
    Cli::try_parse_from(argv).unwrap().settings().unwrap()
}

fn assert_reports(dir: &std::path::Path) -> serde_json::Value {
    let json: serde_json::Value =
        serde_json::from_slice(&std::fs::read(dir.join("report.json")).unwrap()).unwrap();
    assert_eq!(json["schema"], "flower-bench/2");
    assert!(json["goodput"]["perSecond"].as_f64().unwrap() > 0.0);
    assert!(json["phases"].as_array().unwrap().len() >= 4);
    let html = std::fs::read_to_string(dir.join("report.html")).unwrap();
    assert!(html.contains("Content-Security-Policy"));
    assert!(!html.contains("<script"));
    let elsewhere = html.replace("http://www.w3.org/2000/svg", "");
    assert!(!elsewhere.contains("src=\"http") && !elsewhere.contains("href=\"http"));
    json
}

#[tokio::test(flavor = "multi_thread")]
async fn the_smoke_preset_passes_its_audit() {
    let _turn = TURN.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let settings = settings(dir.path(), &["--preset", "smoke"]).await;
    let finished = run(settings, CancellationToken::new()).await.unwrap();
    let report = &finished.report;
    let audit = report.groups[0].audit.as_ref().unwrap();
    assert!(report.passed, "{:#?}", audit.checks);
    assert!(audit.totals.orders > 0);
    assert!(report.goodput.completed > 0);
    assert!(report.replays.checks > 0);
    assert!(!report.groups[0].drones.fences.is_empty());
    assert!(report.across.is_none() && report.kill.is_none());
    // Every answer's Server-Timing: where writes spent their time.
    let order = &report.load().unwrap().operations["pizza.order"];
    for stage in ["eval", "commit", "total"] {
        assert!(
            order.stages.contains_key(stage),
            "{stage}: {:?}",
            order.stages.keys()
        );
    }
    let json = assert_reports(dir.path());
    // A report compares with itself: nothing regressed. A closed loop is
    // never matched fixed work.
    let comparison = flower_bench::compare::compare(&json, &json, 10.0);
    assert!(comparison.passed, "{:?}", comparison.regressions);
    assert!(!comparison.matched_fixed_work);
    // Every gated measure is read from a real report.
    let summary = &comparison.candidate;
    assert_eq!(
        summary.write_p99,
        report.goodput.write_latency.percentiles.p99
    );
    assert!(summary.read_p99.is_some() && summary.write_p99.is_some());
    assert!(summary.servers.us_per_call.is_some(), "{summary:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_leader_kill_keeps_the_books_balanced() {
    let _turn = TURN.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let settings = settings(
        dir.path(),
        &[
            "--replicas",
            "3",
            "--chaos",
            "--duration",
            "6",
            "--warmup",
            "0.5",
            "--max-orders",
            "24",
        ],
    )
    .await;
    let finished = run(settings, CancellationToken::new()).await.unwrap();
    let report = &finished.report;
    let group = &report.groups[0];
    let audit = group.audit.as_ref().unwrap();
    assert!(report.passed, "{:#?}", audit.checks);
    let event = group.chaos.first().expect("the leader was killed");
    assert!(event.error.is_none(), "{event:#?}");
    assert!(event.led);
    let failover = event
        .failover_ms
        .expect("a write was acknowledged after the kill");
    assert!(failover > 0.0);
    assert_ne!(event.new_leader, Some(event.killed));
    assert!(event.caught_up_ms.is_some(), "the killed member caught up");
    assert_reports(dir.path());
}

#[tokio::test(flavor = "multi_thread")]
async fn hosted_groups_keep_their_tenants_through_a_process_kill() {
    let _turn = TURN.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let settings = settings(
        dir.path(),
        &[
            "--groups",
            "2",
            "--replicas",
            "3",
            "--tenants",
            "1",
            "--stores",
            "2",
            "--concurrency",
            "4",
            "--chaos",
            "--duration",
            "6",
            "--warmup",
            "0.5",
            "--max-orders",
            "12",
        ],
    )
    .await;
    let finished = run(settings, CancellationToken::new()).await.unwrap();
    let report = &finished.report;
    let across = report.across.as_ref().expect("a cross-group audit");
    assert!(
        report.passed,
        "{}\n{:#?}",
        report.verdict,
        report
            .groups
            .iter()
            .map(|group| &group.audit)
            .collect::<Vec<_>>()
    );
    assert!(across.passed, "{:#?}", across.checks);
    assert!(across.checks.iter().any(|check| check.id == "placement"));
    assert_eq!(report.groups.len(), 2);
    for (index, group) in report.groups.iter().enumerate() {
        assert_eq!(group.name, format!("g{}", index + 1));
        assert_eq!(group.tenants, vec![format!("tenant-{index}")]);
        assert_eq!(
            group.cluster.databases,
            vec![format!("pizza-tenant-{index}")]
        );
        assert!(group.goodput.as_ref().unwrap().completed > 0);
        assert!(group.audit.as_ref().unwrap().totals.orders > 0);
    }
    // Each group's goodput is its own; the run's is their sum.
    let sum: u64 = report
        .groups
        .iter()
        .map(|group| group.goodput.as_ref().unwrap().completed)
        .sum();
    assert_eq!(sum, report.goodput.completed);
    // Both partitions stayed on their groups, epoch 1.
    let placed: Vec<(&str, &str, u64)> = report
        .placements
        .iter()
        .map(|placement| {
            (
                placement.partition.as_str(),
                placement.group.as_str(),
                placement.epoch,
            )
        })
        .collect();
    assert!(placed.contains(&("pizza-tenant-0", "g1", 1)), "{placed:?}");
    assert!(placed.contains(&("pizza-tenant-1", "g2", 1)), "{placed:?}");
    // The killed process led at least one group, which elected a new
    // leader and wrote again; every group's replica there caught up.
    let kill = report.kill.as_ref().expect("a process was killed");
    assert!(kill.error.is_none(), "{kill:#?}");
    assert!(!kill.led.is_empty(), "{kill:#?}");
    assert_eq!(kill.groups, 2);
    for group in &report.groups {
        let event = group.chaos.first().expect("every group saw the kill");
        assert!(event.error.is_none(), "{event:#?}");
        assert!(event.failover_ms.is_some(), "{event:#?}");
        assert!(event.caught_up_ms.is_some(), "{event:#?}");
        assert_eq!(event.led, kill.led.contains(&group.name));
        if event.led {
            assert!(event.election_ms.is_some());
            assert_ne!(event.new_leader, Some(event.killed));
        }
    }
    let json = assert_reports(dir.path());
    assert_eq!(json["settings"]["topology"], "hosted");
    let html = std::fs::read_to_string(dir.path().join("report.html")).unwrap();
    assert!(html.contains("<h2>Groups</h2>") && html.contains("pizza-tenant-1"));
}

#[tokio::test(flavor = "multi_thread")]
async fn the_authenticated_kitchen_enforces_its_rules() {
    let _turn = TURN.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let settings = settings(dir.path(), &["--preset", "smoke", "--auth"]).await;
    let finished = run(settings, CancellationToken::new()).await.unwrap();
    let report = &finished.report;
    let audit = report.groups[0].audit.as_ref().unwrap();
    assert!(report.passed, "{:#?}", audit.checks);
    let access = audit
        .checks
        .iter()
        .find(|check| check.id == "access")
        .expect("an authenticated run checks its access rules");
    assert!(access.passed, "{access:#?}");
    assert!(access.detail.starts_with("8 probes"), "{}", access.detail);
    assert!(audit.totals.orders > 0);
    assert!(report.settings.auth);
    // Tokens never reach the report.
    let json = std::fs::read_to_string(dir.path().join("report.json")).unwrap();
    assert!(!json.contains("eyJ"), "a JWT leaked into the report");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_diagnostic_run_captures_telemetry_and_stacks() {
    let _turn = TURN.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let profile = dir.path().join("cpu.folded");
    let profile = profile.display().to_string();
    let mut args = vec![
        "--preset",
        "smoke",
        "--duration",
        "4",
        "--otel",
        "--otel-sample",
        "1",
    ];
    // Stacks are sampled in-process on Windows; elsewhere they need perf
    // or sample, which a test machine may not have.
    if cfg!(windows) {
        args.extend([
            "--cpu-profile",
            profile.as_str(),
            "--cpu-profile-seconds",
            "2",
            "--cpu-profile-interval-ms",
            "5",
        ]);
    }
    let settings = settings(dir.path(), &args).await;
    let finished = run(settings, CancellationToken::new()).await.unwrap();
    let report = &finished.report;
    let otel = report.otel.as_ref().expect("an OpenTelemetry report");
    assert!(report.passed, "{}: {otel:#?}", report.verdict);
    assert!(otel.passed, "{otel:#?}");
    assert!(otel.spans.in_window > 0, "{otel:#?}");
    assert!(otel.metrics.intervals > 0, "{otel:#?}");
    assert!(
        otel.metrics.routes.contains_key("/v1/mutate")
            || otel.metrics.routes.contains_key("/v1/query"),
        "{otel:#?}"
    );
    assert!(
        otel.resources
            .iter()
            .any(|resource| resource.node == Some(1))
    );
    let files = dir.path().join("report-otel");
    assert!(files.join("summary.json").is_file());
    assert!(files.join("capture.ndjson").is_file());
    if cfg!(windows) {
        let cpu = report.cpu_profile.as_ref().expect("a CPU profile");
        assert!(cpu.passed(), "{cpu:#?}");
        assert!(cpu.samples > 0);
        let summary = cpu.summary.as_ref().unwrap();
        assert_eq!(summary.classification, "cpu-counter");
        assert!(
            summary
                .threads
                .iter()
                .any(|group| group.thread.contains("tokio")),
            "{summary:#?}"
        );
        assert!(
            std::fs::metadata(dir.path().join("cpu.folded"))
                .unwrap()
                .len()
                > 0
        );
    }
    // A diagnostic run is never published.
    let json = assert_reports(dir.path());
    assert!(flower_bench::publish::validate(&json).is_err());
}
