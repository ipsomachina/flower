//! `flower-bench`: see the library's docs and the crate README.
//!
//! Exit codes: 0 when every audit check held, 1 when one failed or the
//! run could not finish, 2 for bad flags, 130 when a second Ctrl-C
//! stopped it.

use std::process::ExitCode;

use clap::Parser;
use flower_bench::config::{Cli, Command};
use flower_bench::run::{run, scorecard};
use flower_bench::{compare, publish};
use flower_client::CancellationToken;

/// `compare`: 0 when nothing gated regressed and the candidate passed,
/// 1 otherwise, 2 when a report cannot be read.
fn compare_command(
    baseline: &std::path::Path,
    candidate: &std::path::Path,
    threshold: f64,
    json: bool,
) -> ExitCode {
    let (before, after) = match (compare::read(baseline), compare::read(candidate)) {
        (Ok(before), Ok(after)) => (before, after),
        (Err(error), _) | (_, Err(error)) => {
            eprintln!("flower-bench compare: {error}");
            return ExitCode::from(2);
        }
    };
    if !(threshold.is_finite() && threshold >= 0.0) {
        eprintln!("flower-bench compare: --threshold must be a non-negative percentage");
        return ExitCode::from(2);
    }
    let comparison = compare::compare(&before, &after, threshold);
    if json {
        match serde_json::to_string_pretty(&comparison) {
            Ok(text) => println!("{text}"),
            Err(error) => {
                eprintln!("flower-bench compare: {error}");
                return ExitCode::from(2);
            }
        }
    } else {
        print!("{}", compare::render_markdown(&comparison));
    }
    if comparison.passed {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

/// `publish`: retain reports and rewrite the README's table, or check
/// what is retained.
fn publish_command(
    reports: &[std::path::PathBuf],
    check: bool,
    dir: Option<std::path::PathBuf>,
    readme: Option<std::path::PathBuf>,
    name: Option<&str>,
) -> ExitCode {
    let (Some(dir), Some(readme)) = (
        dir.or_else(publish::default_dir),
        readme.or_else(publish::default_readme),
    ) else {
        eprintln!("flower-bench publish: the workspace is gone: pass --dir and --readme");
        return ExitCode::from(2);
    };
    if check {
        return match publish::check(&dir, &readme) {
            Ok(problems) if problems.is_empty() => {
                println!("The retained results in {} hold.", dir.display());
                ExitCode::SUCCESS
            }
            Ok(problems) => {
                for problem in problems {
                    eprintln!("flower-bench publish --check: {problem}");
                }
                ExitCode::FAILURE
            }
            Err(error) => {
                eprintln!("flower-bench publish --check: {error}");
                ExitCode::FAILURE
            }
        };
    }
    match publish::publish(reports, &dir, &readme, name) {
        Ok(names) => {
            println!(
                "Published {} into {} and updated {}.",
                names.join(", "),
                dir.display(),
                readme.display()
            );
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("flower-bench publish: {error}");
            ExitCode::FAILURE
        }
    }
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match &cli.command {
        Some(Command::Compare {
            baseline,
            candidate,
            threshold,
            json,
        }) => return compare_command(baseline, candidate, *threshold, *json),
        Some(Command::Publish {
            reports,
            check,
            dir,
            readme,
            name,
        }) => {
            return publish_command(
                reports,
                *check,
                dir.clone(),
                readme.clone(),
                name.as_deref(),
            );
        }
        None => {}
    }
    let settings = match cli.settings() {
        Ok(settings) => settings,
        Err(error) => {
            eprintln!("flower-bench: {error}");
            return ExitCode::from(2);
        }
    };
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("flower-bench: {error}");
            return ExitCode::FAILURE;
        }
    };
    runtime.block_on(async move {
        // The first Ctrl-C closes the kitchen: the load stops, and the
        // drain, audit and report still run. The second stops at once;
        // dropping the run kills the servers and removes their data.
        let closing = CancellationToken::new();
        let abort = CancellationToken::new();
        {
            let (closing, abort) = (closing.clone(), abort.clone());
            tokio::spawn(async move {
                while tokio::signal::ctrl_c().await.is_ok() {
                    if closing.is_cancelled() {
                        abort.cancel();
                        return;
                    }
                    eprintln!("\nClosing time: stopping the load, then draining and auditing. Ctrl-C again stops at once.");
                    closing.cancel();
                }
            });
        }
        let linger = settings.linger;
        let finished = tokio::select! {
            finished = run(settings, closing.clone()) => finished,
            () = abort.cancelled() => {
                eprintln!("flower-bench: stopped");
                return ExitCode::from(130);
            }
        };
        match finished {
            Ok(finished) => {
                println!("{}", scorecard(&finished.report));
                if let (true, Some(live)) = (linger, &finished.live) {
                    println!(
                        "\nThe live view stays up at {} with the final state. Ctrl-C to exit.",
                        live.url()
                    );
                    let wait = if closing.is_cancelled() { &abort } else { &closing };
                    wait.cancelled().await;
                    live.stop();
                }
                if finished.report.passed {
                    ExitCode::SUCCESS
                } else {
                    ExitCode::FAILURE
                }
            }
            Err(error) => {
                eprintln!("flower-bench: {error}");
                ExitCode::FAILURE
            }
        }
    })
}
