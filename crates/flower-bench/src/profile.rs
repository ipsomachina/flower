//! `--cpu-profile`: native stack samples of one server process.
//!
//! From the start of the measured window, for `--cpu-profile-seconds`,
//! the bench samples the process that leads the most groups (the
//! leader, with one group) and keeps it however leadership moves:
//!
//! - **Windows:** the bench samples the process itself, as `sample` does
//!   on macOS: every interval it suspends each of the server's threads
//!   in turn, reads its context, walks its stack with DbgHelp
//!   (`StackWalk64`, which unwinds through the images' unwind tables on
//!   x64 and ARM64) and resumes it. A thread's cycle counter
//!   (`QueryThreadCycleTime`) tells whether it ran since its previous
//!   sample, so each stack is classified on or off CPU from the
//!   scheduler's own counter rather than guessed from wait frames. No
//!   administrator rights are needed: the bench started the process.
//!   Frames resolve through the server's PDB (DbgHelp) and Rust names
//!   are demangled; a release server built without debug information
//!   (`CARGO_PROFILE_RELEASE_DEBUG=line-tables-only` adds it) resolves
//!   to module offsets only, and frames inside JIT-compiled guest code
//!   stay unresolved addresses.
//! - **Linux:** `perf record -g` at the requested rate on the PID, then
//!   `perf script`: on-CPU samples with kernel-resolved user stacks
//!   (needs `perf` and a `perf_event_paranoid` that allows it).
//! - **macOS:** `/usr/bin/sample`: all-thread wall stacks, classified waiting
//!   or active by their blocking frames.
//!
//! The raw stacks go to the `--cpu-profile` path as folded stacks
//! (`thread;outermost;...;innermost count`, what flame-graph tools
//! read); the report keeps a summary: active and waiting observations,
//! active work by subsystem (each stack assigned to its innermost
//! recognized subsystem, so the shares add up), the hottest inclusive
//! and self frames, waiting frames, and threads grouped by name. Counts
//! are stack observations, not CPU time. Sampling suspends the server's
//! threads, so it perturbs the run: a profiled run is a diagnostic and
//! a failed profile fails it.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::path::Path;
use std::time::{Duration, Instant};

use serde::Serialize;

use crate::config::CpuProfileSettings;
use crate::report::{count, esc};

/// Most frames kept per stack.
const MAX_DEPTH: usize = 256;
/// Rows kept per ranking.
const TOP: usize = 25;
/// Largest raw text summarized.
#[cfg(any(target_os = "linux", target_os = "macos"))]
const MAX_RAW: usize = 64 * 1_048_576;

/// One sampled stack.
#[derive(Clone, Debug, PartialEq)]
pub struct Stack {
    /// The thread's name (or `thread <id>`).
    pub thread: String,
    /// Frames, innermost first.
    pub frames: Vec<String>,
    /// Whether the thread was on CPU, when the sampler knows.
    pub on_cpu: Option<bool>,
    /// How many observations this stack stands for.
    pub weight: u64,
}

/// One ranked frame or category.
#[derive(Clone, Debug, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Ranked {
    /// The frame or category.
    pub name: String,
    /// Observations.
    pub samples: u64,
    /// Share of the ranking's denominator.
    pub fraction: Option<f64>,
}

/// Threads of one name.
#[derive(Clone, Debug, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ThreadGroup {
    /// The name.
    pub thread: String,
    /// Distinct threads.
    pub physical: usize,
    /// Observations.
    pub total: u64,
    /// Active ones.
    pub active: u64,
    /// Waiting ones.
    pub waiting: u64,
}

/// What the samples say.
#[derive(Clone, Debug, Default, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Summary {
    /// What a count is.
    pub unit: String,
    /// Every observation.
    pub total: u64,
    /// On CPU (or with no blocking frame).
    pub active: u64,
    /// Off CPU (or blocked).
    pub waiting: u64,
    /// How activity was decided: `cpu-counter` or `wait-frames`.
    pub classification: String,
    /// Share of active observations whose frames all failed to resolve.
    pub unresolved_active: Option<f64>,
    /// Active work by innermost recognized subsystem.
    pub categories: Vec<Ranked>,
    /// Hottest frames anywhere on active stacks (overlapping).
    pub inclusive: Vec<Ranked>,
    /// Hottest innermost frames of active stacks.
    pub self_frames: Vec<Ranked>,
    /// Where waiting stacks waited.
    pub waiting_frames: Vec<Ranked>,
    /// Threads by name.
    pub threads: Vec<ThreadGroup>,
    /// What the figures are not.
    pub limitations: Vec<String>,
}

/// The profile in a report.
#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CpuProfile {
    /// `complete`, `failed` or `unsupported`.
    pub status: String,
    /// The sampler.
    pub tool: String,
    /// The sampled member.
    pub member: u64,
    /// Its process ID.
    pub pid: Option<u32>,
    /// Requested sampling time.
    pub requested_seconds: u64,
    /// Requested interval.
    pub interval_ms: u64,
    /// Sampling rounds taken.
    pub rounds: u64,
    /// Stack observations taken.
    pub samples: u64,
    /// How long sampling ran.
    pub elapsed_ms: f64,
    /// The folded stacks.
    pub raw_path: String,
    /// Their size.
    pub raw_bytes: u64,
    /// The summary.
    pub summary: Option<Summary>,
    /// What went wrong.
    pub error: Option<String>,
    /// Always: sampling perturbs the server.
    pub perturbs_performance: bool,
}

impl CpuProfile {
    /// Whether the profile was taken and summarized.
    pub fn passed(&self) -> bool {
        self.status == "complete"
    }
}

/// Blocking primitives, by name: waits on Windows, Linux and macOS.
fn waits(frame: &str) -> bool {
    /// Windows system calls that block, without their `Nt`/`Zw` prefix. A
    /// durable flush counts as waiting.
    const SYSCALLS: [&str; 11] = [
        "WaitForSingleObject",
        "WaitForMultipleObjects",
        "WaitForAlertByThreadId",
        "WaitForWorkViaWorkerFactory",
        "WaitForKeyedEvent",
        "SignalAndWaitForSingleObject",
        "DelayExecution",
        "RemoveIoCompletion",
        "RemoveIoCompletionEx",
        "FlushBuffersFile",
        "FlushBuffersFileEx",
    ];
    /// Library waits on Windows, Linux and macOS.
    const WAITS: [&str; 22] = [
        "WaitForSingleObject",
        "WaitForSingleObjectEx",
        "WaitForMultipleObjects",
        "WaitForMultipleObjectsEx",
        "SleepConditionVariableSRW",
        "RtlSleepConditionVariableSRW",
        "GetQueuedCompletionStatus",
        "GetQueuedCompletionStatusEx",
        "WaitOnAddress",
        "RtlWaitOnAddress",
        "SleepEx",
        "futex_wait",
        "epoll_wait",
        "__poll",
        "poll",
        "nanosleep",
        "clock_nanosleep",
        "pthread_cond_wait",
        "pthread_cond_timedwait",
        "__psynch_cvwait",
        "kevent",
        "__semwait_signal",
    ];
    let name = frame.rsplit("::").next().unwrap_or(frame);
    let name = name.rsplit('!').next().unwrap_or(name);
    if let Some(base) = name.strip_prefix("Nt").or_else(|| name.strip_prefix("Zw"))
        && SYSCALLS.contains(&base)
    {
        return true;
    }
    WAITS.contains(&name)
        || name.starts_with("__ulock_wait")
        || name.starts_with("semaphore_")
        || name.starts_with("mach_msg")
}

/// Whether a suspended thread was on CPU: a thread stopped inside a
/// blocking system call (its innermost frame) was waiting, whatever its
/// counter says (it may have run earlier in the interval); otherwise it
/// was running if its cycle counter advanced since its previous sample,
/// and ready but not running if it did not. Unknown without a counter
/// reading.
pub fn on_cpu(ran: Option<bool>, frames: &[String]) -> Option<bool> {
    let blocked = frames.first().is_some_and(|frame| waits(frame));
    ran.map(|ran| ran && !blocked)
}

/// The innermost recognized subsystem of a stack.
fn category(frames: &[String]) -> &'static str {
    const RULES: [(&str, &[&str]); 16] = [
        ("JSON", &["serde_json", "flower_engine::json"]),
        ("Hashing", &["sha2::", "blake3", "sha256"]),
        ("Compression", &["lz4"]),
        ("Cryptography", &["aws_lc", "ring::", "ed25519"]),
        ("Storage (redb)", &["redb::"]),
        ("Raft (OpenRaft)", &["openraft::"]),
        ("Wasm runtime", &["wasmtime", "cranelift", "wasm["]),
        (
            "HTTP",
            &["hyper::", "h2::", "axum::", "tower::", "hyper_util::"],
        ),
        (
            "Allocation",
            &[
                "alloc::alloc",
                "__rust_alloc",
                "__rust_dealloc",
                "malloc",
                "free",
                "HeapAlloc",
                "HeapFree",
                "RtlAllocateHeap",
                "RtlFreeHeap",
                "mi_",
            ],
        ),
        ("Flower engine", &["flower_engine::"]),
        ("Flower cluster", &["flower_cluster::"]),
        ("Flower server", &["flower_server::"]),
        ("Flower backup", &["flower_backup::"]),
        ("Async runtime (Tokio)", &["tokio::"]),
        ("Standard library", &["std::", "core::"]),
        (
            "Operating system",
            &["ntdll", "kernel32", "KERNELBASE", "libc.so", "[kernel"],
        ),
    ];
    for frame in frames {
        for (name, needles) in RULES {
            if needles.iter().any(|needle| frame.contains(needle)) {
                return name;
            }
        }
    }
    "Other"
}

/// Whether a frame never resolved to a name.
fn unresolved(frame: &str) -> bool {
    frame.starts_with("0x") || frame.contains("!0x") || frame == "?"
}

fn ranked(map: BTreeMap<String, u64>, denominator: u64) -> Vec<Ranked> {
    let mut rows: Vec<(String, u64)> = map.into_iter().collect();
    rows.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    rows.into_iter()
        .take(TOP)
        .map(|(name, samples)| Ranked {
            name,
            samples,
            fraction: (denominator > 0).then(|| samples as f64 / denominator as f64),
        })
        .collect()
}

/// Summarize stacks.
pub fn summarize(stacks: &[Stack]) -> Summary {
    let counter = stacks.iter().any(|stack| stack.on_cpu.is_some());
    let mut summary = Summary {
        unit: "stack observations".to_string(),
        classification: if counter {
            "cpu-counter"
        } else {
            "wait-frames"
        }
        .to_string(),
        ..Summary::default()
    };
    let mut categories = BTreeMap::new();
    let mut inclusive = BTreeMap::new();
    let mut innermost = BTreeMap::new();
    let mut waiting = BTreeMap::new();
    let mut threads: BTreeMap<String, (BTreeSet<String>, u64, u64, u64)> = BTreeMap::new();
    let mut unresolved_active = 0;
    for stack in stacks {
        let weight = stack.weight;
        summary.total += weight;
        let group = stack
            .thread
            .trim_end_matches(|c: char| c.is_ascii_digit() || c == '-' || c == '#')
            .trim()
            .to_string();
        let group = if group.is_empty() || group == "thread" {
            "unnamed".to_string()
        } else {
            group
        };
        let entry = threads.entry(group).or_default();
        entry.0.insert(stack.thread.clone());
        entry.1 += weight;
        let blocker = stack.frames.iter().find(|frame| waits(frame));
        let active = match stack.on_cpu {
            Some(on) => on,
            None => blocker.is_none(),
        };
        if active {
            summary.active += weight;
            entry.2 += weight;
            *categories
                .entry(category(&stack.frames).to_string())
                .or_default() += weight;
            *innermost
                .entry(
                    stack
                        .frames
                        .first()
                        .cloned()
                        .unwrap_or_else(|| "[no frames]".to_string()),
                )
                .or_default() += weight;
            for frame in stack.frames.iter().collect::<BTreeSet<_>>() {
                *inclusive.entry(frame.clone()).or_default() += weight;
            }
            if stack.frames.iter().all(|frame| unresolved(frame)) {
                unresolved_active += weight;
            }
        } else {
            summary.waiting += weight;
            entry.3 += weight;
            let at = blocker
                .or(stack.frames.first())
                .cloned()
                .unwrap_or_else(|| "[no frames]".to_string());
            *waiting.entry(at).or_default() += weight;
        }
    }
    summary.unresolved_active =
        (summary.active > 0).then(|| unresolved_active as f64 / summary.active as f64);
    summary.categories = ranked(categories, summary.active);
    summary.inclusive = ranked(inclusive, summary.active);
    summary.self_frames = ranked(innermost, summary.active);
    summary.waiting_frames = ranked(waiting, summary.waiting);
    let mut groups: Vec<ThreadGroup> = threads
        .into_iter()
        .map(|(thread, (physical, total, active, waiting))| ThreadGroup {
            thread,
            physical: physical.len(),
            total,
            active,
            waiting,
        })
        .collect();
    groups.sort_by(|a, b| b.active.cmp(&a.active).then_with(|| b.total.cmp(&a.total)));
    groups.truncate(50);
    summary.threads = groups;
    summary.limitations = vec![
        "Counts are stack observations, not CPU time or utilization.".to_string(),
        if counter {
            "A stack is active when the CPU clock took it (Linux perf), or (Windows) when its thread was not stopped inside a blocking system call and its cycle counter advanced since that thread's previous sample; a thread's first sample is classified by its frames.".to_string()
        } else {
            "Active means no recognized blocking frame in the stack; unrecognized blocking can remain.".to_string()
        },
        "Categories are mutually exclusive (the innermost recognized subsystem); inclusive frame counts overlap and must not be summed.".to_string(),
        "Only the sampled process is covered: other members, and a process that replaced it after a kill, are not.".to_string(),
    ];
    summary
}

/// Stacks as folded text: `thread;outermost;...;innermost count`.
pub fn folded(stacks: &[Stack]) -> String {
    let mut merged: BTreeMap<String, u64> = BTreeMap::new();
    for stack in stacks {
        let mut line = clean(&stack.thread);
        for frame in stack.frames.iter().rev() {
            line.push(';');
            line.push_str(&clean(frame));
        }
        *merged.entry(line).or_default() += stack.weight;
    }
    let mut out = String::new();
    for (line, weight) in merged {
        let _ = writeln!(out, "{line} {weight}");
    }
    out
}

/// A frame name fit for folded text: no separators, no newlines.
fn clean(name: &str) -> String {
    name.replace([';', '\n', '\r'], " ")
}

/// Rust names demangled, the hash dropped.
pub fn demangle(name: &str) -> String {
    match rustc_demangle::try_demangle(name) {
        Ok(demangled) => format!("{demangled:#}"),
        Err(_) => name.to_string(),
    }
}

/// Parse `perf script` output (default fields): a header line per
/// sample (`comm pid/tid ...`), then one indented `address symbol
/// (module)` line per frame, innermost first, and a blank line.
pub fn parse_perf_script(text: &str) -> Vec<Stack> {
    let mut stacks = Vec::new();
    let mut current: Option<Stack> = None;
    for line in text.lines() {
        if line.trim().is_empty() {
            if let Some(stack) = current.take() {
                stacks.push(stack);
            }
            continue;
        }
        if line.starts_with(|c: char| c.is_whitespace()) {
            if let Some(stack) = current.as_mut()
                && stack.frames.len() < MAX_DEPTH
            {
                let mut parts = line.split_whitespace();
                let address = parts.next().unwrap_or("?");
                let rest: Vec<&str> = parts.collect();
                let module = rest
                    .last()
                    .filter(|part| part.starts_with('(') && part.ends_with(')'))
                    .map(|part| part.trim_matches(['(', ')']));
                let symbol_parts = if module.is_some() {
                    &rest[..rest.len() - 1]
                } else {
                    &rest[..]
                };
                let symbol = symbol_parts.join(" ");
                let symbol = symbol
                    .rsplit_once("+0x")
                    .map_or(symbol.as_str(), |(name, _)| name)
                    .to_string();
                stack
                    .frames
                    .push(if symbol.is_empty() || symbol == "[unknown]" {
                        match module {
                            Some(module) => format!("{module}!0x{address}"),
                            None => format!("0x{address}"),
                        }
                    } else {
                        demangle(&symbol)
                    });
            }
            continue;
        }
        if let Some(stack) = current.take() {
            stacks.push(stack);
        }
        // `comm pid/tid cpu time: period event:`: the command name may
        // hold spaces; the pid/tid field is the first with a slash or the
        // first number.
        let fields: Vec<&str> = line.split_whitespace().collect();
        let at = fields
            .iter()
            .position(|field| {
                field.contains('/') && field.split('/').all(|part| part.parse::<u64>().is_ok())
                    || field.parse::<u64>().is_ok()
            })
            .unwrap_or(1)
            .max(1);
        let thread = fields[..at.min(fields.len())].join(" ");
        current = Some(Stack {
            thread: if thread.is_empty() {
                "unnamed".to_string()
            } else {
                thread
            },
            frames: Vec::new(),
            on_cpu: Some(true),
            weight: 1,
        });
    }
    if let Some(stack) = current {
        stacks.push(stack);
    }
    stacks
}

/// Parse macOS `sample` output: its call graph is a tree of `count frame`
/// lines indented by depth under `Thread_` roots. Each node's own
/// observations (its count less its children's) become one stack.
pub fn parse_sample(text: &str) -> Result<Vec<Stack>, String> {
    struct Node {
        name: String,
        depth: usize,
        samples: u64,
        children: u64,
        parent: Option<usize>,
        thread: bool,
    }
    let mut nodes: Vec<Node> = Vec::new();
    let mut path: Vec<usize> = Vec::new();
    let mut in_graph = false;
    for line in text.lines() {
        if line.starts_with("Call graph:") {
            in_graph = true;
            continue;
        }
        if !in_graph {
            continue;
        }
        let trimmed = line.trim_start();
        if trimmed.starts_with("Total number in stack")
            || trimmed.starts_with("Sort by top of stack")
            || trimmed.starts_with("Binary Images:")
        {
            break;
        }
        let prefix = line
            .char_indices()
            .find(|(_, c)| !matches!(c, ' ' | '+' | '!' | ':' | '|' | '\t'))
            .map_or(line.len(), |(at, _)| at);
        let rest = &line[prefix..];
        let Some((count, name)) = rest.split_once(char::is_whitespace) else {
            continue;
        };
        let Ok(samples) = count.parse::<u64>() else {
            continue;
        };
        let name = name
            .split(" (in ")
            .next()
            .unwrap_or(name)
            .split("  [0x")
            .next()
            .unwrap_or(name);
        let name = name
            .rsplit_once(" + ")
            .filter(|(_, offset)| offset.chars().all(|c| c.is_ascii_digit()))
            .map_or(name, |(name, _)| name)
            .trim()
            .to_string();
        let thread = name.starts_with("Thread_");
        while let Some(&top) = path.last() {
            if thread || nodes[top].depth >= prefix {
                path.pop();
            } else {
                break;
            }
        }
        if !thread && path.is_empty() {
            continue;
        }
        if nodes.len() >= 100_000 || path.len() >= MAX_DEPTH {
            return Err("the sample call tree exceeds the summary bounds".to_string());
        }
        let parent = path.last().copied();
        if let Some(parent) = parent {
            nodes[parent].children += samples;
        }
        nodes.push(Node {
            name,
            depth: prefix,
            samples,
            children: 0,
            parent,
            thread,
        });
        path.push(nodes.len() - 1);
    }
    let mut stacks = Vec::new();
    for index in 0..nodes.len() {
        let node = &nodes[index];
        if node.children > node.samples {
            return Err("inconsistent sample call-tree counts".to_string());
        }
        let own = node.samples - node.children;
        if own == 0 {
            continue;
        }
        let mut frames = Vec::new();
        let mut thread = String::new();
        let mut cursor = Some(index);
        while let Some(at) = cursor {
            if nodes[at].thread {
                thread = nodes[at].name.clone();
            } else {
                frames.push(demangle(&nodes[at].name));
            }
            cursor = nodes[at].parent;
        }
        stacks.push(Stack {
            thread,
            frames,
            on_cpu: None,
            weight: own,
        });
    }
    Ok(stacks)
}

/// Sample process `pid` (member `member`) as `settings` says, from now;
/// the task always finishes with a profile, failed or not.
pub fn start(
    pid: Option<u32>,
    member: u64,
    settings: CpuProfileSettings,
) -> (Go, tokio::task::JoinHandle<CpuProfile>) {
    let (go, ready) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let mut profile = CpuProfile {
            status: "failed".to_string(),
            member,
            pid,
            requested_seconds: settings.seconds,
            interval_ms: settings.interval_ms,
            raw_path: settings.path.display().to_string(),
            perturbs_performance: true,
            ..CpuProfile::default()
        };
        let Some(pid) = pid else {
            profile.error = Some("the process to sample was not running".to_string());
            return profile;
        };
        let duration = Duration::from_secs(settings.seconds);
        let interval = Duration::from_millis(settings.interval_ms);
        let sampled = sample(pid, duration, interval, ready).await;
        match sampled {
            Ok((tool, rounds, stacks, elapsed)) => {
                profile.elapsed_ms = elapsed.as_secs_f64() * 1000.0;
                profile.tool = tool.to_string();
                profile.rounds = rounds;
                profile.samples = stacks.iter().map(|stack| stack.weight).sum();
                let text = folded(&stacks);
                profile.raw_bytes = text.len() as u64;
                if let Err(error) = write_raw(&settings.path, &text) {
                    profile.error = Some(error);
                    return profile;
                }
                let summary = summarize(&stacks);
                if summary.total == 0 {
                    profile.error = Some("no stack was sampled".to_string());
                } else {
                    profile.status = "complete".to_string();
                }
                profile.summary = Some(summary);
            }
            Err((tool, error)) => {
                profile.tool = tool.to_string();
                if tool == "unsupported" {
                    profile.status = "unsupported".to_string();
                }
                profile.error = Some(error);
            }
        }
        profile
    });
    (Go(go), task)
}

/// Starts the sampling [`start`] prepared (on Windows, the symbol
/// session and its modules load before the window, so loading them
/// never eats into it). Dropping it unsent cancels the profile.
pub struct Go(tokio::sync::oneshot::Sender<Instant>);

impl Go {
    /// Begin sampling now.
    pub fn go(self) {
        let _ = self.0.send(Instant::now());
    }
}

/// The signal a sampler waits for.
type Ready = tokio::sync::oneshot::Receiver<Instant>;

fn write_raw(path: &Path, text: &str) -> Result<(), String> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)
            .map_err(|error| format!("{}: {error}", parent.display()))?;
    }
    std::fs::write(path, text).map_err(|error| format!("{}: {error}", path.display()))
}

/// A sampler's outcome: its tool, rounds, stacks and how long it
/// sampled.
type Sampled = Result<(&'static str, u64, Vec<Stack>, Duration), (&'static str, String)>;

#[cfg(windows)]
async fn sample(pid: u32, duration: Duration, interval: Duration, ready: Ready) -> Sampled {
    tokio::task::spawn_blocking(move || windows::sample(pid, duration, interval, ready))
        .await
        .map_err(|error| ("windows-stackwalk", error.to_string()))?
        .map(|(rounds, stacks, elapsed)| ("windows-stackwalk", rounds, stacks, elapsed))
        .map_err(|error| ("windows-stackwalk", error))
}

#[cfg(target_os = "linux")]
async fn sample(pid: u32, duration: Duration, interval: Duration, ready: Ready) -> Sampled {
    let tool = "perf";
    let began = ready
        .await
        .map_err(|_| (tool, "the measured window never began".to_string()))?;
    let dir = tempfile::tempdir().map_err(|error| (tool, error.to_string()))?;
    let data = dir.path().join("perf.data");
    let hz = (1000 / interval.as_millis().max(1)).max(1);
    let recorded = tokio::process::Command::new("perf")
        .args([
            "record",
            "-q",
            "-g",
            "-F",
            &hz.to_string(),
            "-p",
            &pid.to_string(),
            "-o",
        ])
        .arg(&data)
        .args(["--", "sleep", &duration.as_secs().to_string()])
        .output()
        .await
        .map_err(|error| (tool, format!("running perf: {error} (is perf installed?)")))?;
    if !recorded.status.success() {
        return Err((
            tool,
            format!(
                "perf record failed: {}",
                String::from_utf8_lossy(&recorded.stderr)
                    .chars()
                    .take(2000)
                    .collect::<String>()
            ),
        ));
    }
    let script = tokio::process::Command::new("perf")
        .args(["script", "-i"])
        .arg(&data)
        .output()
        .await
        .map_err(|error| (tool, format!("running perf script: {error}")))?;
    if script.stdout.len() > MAX_RAW {
        return Err((
            tool,
            "perf script output exceeds the summary bound".to_string(),
        ));
    }
    let stacks = parse_perf_script(&String::from_utf8_lossy(&script.stdout));
    let rounds = stacks.len() as u64;
    Ok((tool, rounds, stacks, began.elapsed().min(duration)))
}

#[cfg(target_os = "macos")]
async fn sample(pid: u32, duration: Duration, interval: Duration, ready: Ready) -> Sampled {
    let tool = "sample";
    let began = ready
        .await
        .map_err(|_| (tool, "the measured window never began".to_string()))?;
    let dir = tempfile::tempdir().map_err(|error| (tool, error.to_string()))?;
    let out = dir.path().join("sample.txt");
    let status = tokio::process::Command::new("/usr/bin/sample")
        .arg(pid.to_string())
        .arg(duration.as_secs().to_string())
        .arg(interval.as_millis().max(1).to_string())
        .args(["-mayDie", "-fullPaths", "-file"])
        .arg(&out)
        .output()
        .await
        .map_err(|error| (tool, format!("running sample: {error}")))?;
    if !status.status.success() {
        return Err((tool, format!("sample exited with {}", status.status)));
    }
    let text = std::fs::read_to_string(&out).map_err(|error| (tool, error.to_string()))?;
    if text.len() > MAX_RAW {
        return Err((tool, "sample output exceeds the summary bound".to_string()));
    }
    let stacks = parse_sample(&text).map_err(|error| (tool, error))?;
    Ok((tool, 0, stacks, began.elapsed().min(duration)))
}

#[cfg(not(any(windows, target_os = "linux", target_os = "macos")))]
async fn sample(_: u32, _: Duration, _: Duration, _: Ready) -> Sampled {
    Err((
        "unsupported",
        "--cpu-profile samples on Windows, Linux (perf) and macOS (sample)".to_string(),
    ))
}

#[cfg(windows)]
mod windows {
    //! Suspend, walk and resume each thread of a process we own.

    use std::collections::BTreeMap;
    use std::time::{Duration, Instant};

    use windows_sys::Win32::Foundation::{CloseHandle, HANDLE, INVALID_HANDLE_VALUE, LocalFree};
    use windows_sys::Win32::System::Diagnostics::Debug::{
        AddrModeFlat, CONTEXT, GetThreadContext, IMAGEHLP_MODULEW64, STACKFRAME64, SYMBOL_INFOW,
        SYMOPT_FAIL_CRITICAL_ERRORS, SYMOPT_NO_PROMPTS, SYMOPT_UNDNAME, StackWalk64, SymCleanup,
        SymFromAddrW, SymFunctionTableAccess64, SymGetModuleBase64, SymGetModuleInfoW64,
        SymInitializeW, SymRefreshModuleList, SymSetOptions,
    };
    use windows_sys::Win32::System::Diagnostics::ToolHelp::{
        CreateToolhelp32Snapshot, TH32CS_SNAPTHREAD, THREADENTRY32, Thread32First, Thread32Next,
    };
    use windows_sys::Win32::System::Threading::{
        GetThreadDescription, OpenProcess, OpenThread, PROCESS_NAME_WIN32,
        PROCESS_QUERY_INFORMATION, PROCESS_VM_READ, QueryFullProcessImageNameW, ResumeThread,
        SuspendThread, THREAD_GET_CONTEXT, THREAD_QUERY_LIMITED_INFORMATION, THREAD_SUSPEND_RESUME,
    };
    use windows_sys::Win32::System::WindowsProgramming::QueryThreadCycleTime;

    use super::{MAX_DEPTH, Stack, demangle, on_cpu};

    #[cfg(target_arch = "aarch64")]
    const MACHINE: u32 =
        windows_sys::Win32::System::SystemInformation::IMAGE_FILE_MACHINE_ARM64 as u32;
    #[cfg(target_arch = "x86_64")]
    const MACHINE: u32 =
        windows_sys::Win32::System::SystemInformation::IMAGE_FILE_MACHINE_AMD64 as u32;
    #[cfg(target_arch = "aarch64")]
    const FULL: u32 = windows_sys::Win32::System::Diagnostics::Debug::CONTEXT_FULL_ARM64;
    #[cfg(target_arch = "x86_64")]
    const FULL: u32 = windows_sys::Win32::System::Diagnostics::Debug::CONTEXT_FULL_AMD64;

    /// How often the thread list is refreshed.
    const REFRESH: Duration = Duration::from_millis(250);

    /// An owned handle, closed on drop.
    struct Owned(HANDLE);

    impl Drop for Owned {
        fn drop(&mut self) {
            if !self.0.is_null() && self.0 != INVALID_HANDLE_VALUE {
                // SAFETY: the handle is owned and closed once.
                unsafe {
                    CloseHandle(self.0);
                }
            }
        }
    }

    struct Thread {
        handle: Owned,
        name: String,
        cycles: Option<u64>,
        /// Its last walked stack: a thread that executed nothing since
        /// is still there, so it is not suspended or walked again.
        last: Option<std::rc::Rc<Vec<u64>>>,
    }

    /// The process's threads by ID.
    fn threads(pid: u32) -> Vec<u32> {
        // SAFETY: a fresh snapshot handle, owned below.
        let snapshot = Owned(unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0) });
        if snapshot.0 == INVALID_HANDLE_VALUE {
            return Vec::new();
        }
        let mut entry = THREADENTRY32 {
            dwSize: std::mem::size_of::<THREADENTRY32>() as u32,
            ..THREADENTRY32::default()
        };
        let mut found = Vec::new();
        // SAFETY: `entry` is a valid THREADENTRY32 with its size set.
        let mut more = unsafe { Thread32First(snapshot.0, &mut entry) } != 0;
        while more {
            if entry.th32OwnerProcessID == pid {
                found.push(entry.th32ThreadID);
            }
            // SAFETY: as above.
            more = unsafe { Thread32Next(snapshot.0, &mut entry) } != 0;
        }
        found
    }

    fn describe(handle: HANDLE, id: u32) -> String {
        let mut text: windows_sys::core::PWSTR = std::ptr::null_mut();
        // SAFETY: on success the system allocates `text`, freed below.
        let ok = unsafe { GetThreadDescription(handle, &mut text) } >= 0;
        let mut name = String::new();
        if ok && !text.is_null() {
            // SAFETY: `text` is a NUL-terminated wide string.
            unsafe {
                let mut length = 0;
                while *text.add(length) != 0 && length < 256 {
                    length += 1;
                }
                name = String::from_utf16_lossy(std::slice::from_raw_parts(text, length));
                LocalFree(text.cast());
            }
        }
        if name.is_empty() {
            format!("thread {id}")
        } else {
            format!("{name} {id}")
        }
    }

    /// The program counters of one suspended thread's stack.
    ///
    /// # Safety
    ///
    /// `process` and `thread` must be live handles with the rights
    /// opened below, and the thread suspended.
    unsafe fn walk(process: HANDLE, thread: HANDLE) -> Result<Vec<u64>, std::io::Error> {
        /// `GetThreadContext` needs a 16-byte aligned record.
        #[repr(C, align(16))]
        struct Aligned(CONTEXT);
        // SAFETY: CONTEXT is plain data; zeroed is a valid start.
        let mut aligned: Aligned = unsafe { std::mem::zeroed() };
        let context = &mut aligned.0;
        context.ContextFlags = FULL;
        // SAFETY: the caller's handle; the record is aligned.
        if unsafe { GetThreadContext(thread, context) } == 0 {
            return Err(std::io::Error::last_os_error());
        }
        let mut frame = STACKFRAME64::default();
        #[cfg(target_arch = "aarch64")]
        {
            frame.AddrPC.Offset = context.Pc;
            frame.AddrStack.Offset = context.Sp;
            // SAFETY: reading the integer-register view of the union.
            frame.AddrFrame.Offset = unsafe { context.Anonymous.Anonymous.Fp };
        }
        #[cfg(target_arch = "x86_64")]
        {
            frame.AddrPC.Offset = context.Rip;
            frame.AddrStack.Offset = context.Rsp;
            frame.AddrFrame.Offset = context.Rbp;
        }
        frame.AddrPC.Mode = AddrModeFlat;
        frame.AddrStack.Mode = AddrModeFlat;
        frame.AddrFrame.Mode = AddrModeFlat;
        let mut pcs = Vec::new();
        while pcs.len() < MAX_DEPTH {
            // SAFETY: valid handles, frame and context; DbgHelp reads
            // the target's memory itself.
            let ok = unsafe {
                StackWalk64(
                    MACHINE,
                    process,
                    thread,
                    &mut frame,
                    (context as *mut CONTEXT).cast(),
                    None,
                    Some(SymFunctionTableAccess64),
                    Some(SymGetModuleBase64),
                    None,
                )
            };
            if ok == 0 || frame.AddrPC.Offset == 0 {
                break;
            }
            pcs.push(frame.AddrPC.Offset);
            if frame.AddrReturn.Offset == 0 {
                break;
            }
        }
        if pcs.is_empty() {
            return Err(std::io::Error::last_os_error());
        }
        Ok(pcs)
    }

    /// The directory of the process's executable.
    fn image_dir(process: HANDLE) -> Option<String> {
        let mut buffer = vec![0u16; 1024];
        let mut length = buffer.len() as u32;
        // SAFETY: the buffer holds `length` characters.
        let ok = unsafe {
            QueryFullProcessImageNameW(
                process,
                PROCESS_NAME_WIN32,
                buffer.as_mut_ptr(),
                &mut length,
            )
        } != 0;
        if !ok {
            return None;
        }
        let path = std::path::PathBuf::from(String::from_utf16_lossy(&buffer[..length as usize]));
        path.parent().map(|dir| dir.display().to_string())
    }

    /// A name for an address: its symbol (demangled), else its module
    /// and offset, else the address.
    fn symbol(process: HANDLE, address: u64) -> String {
        const MAX_NAME: usize = 1024;
        #[repr(C)]
        struct Buffer {
            info: SYMBOL_INFOW,
            name: [u16; MAX_NAME],
        }
        // SAFETY: plain data.
        let mut buffer: Buffer = unsafe { std::mem::zeroed() };
        buffer.info.SizeOfStruct = std::mem::size_of::<SYMBOL_INFOW>() as u32;
        buffer.info.MaxNameLen = MAX_NAME as u32;
        let mut displacement = 0u64;
        // SAFETY: the buffer holds MaxNameLen characters past the struct.
        let found =
            unsafe { SymFromAddrW(process, address, &mut displacement, &mut buffer.info) } != 0;
        if found {
            let length = (buffer.info.NameLen as usize).min(MAX_NAME);
            // SAFETY: the name starts at `info.Name` and runs `length`
            // characters into the buffer that follows it.
            let name = unsafe {
                std::slice::from_raw_parts(
                    std::ptr::addr_of!(buffer.info.Name).cast::<u16>(),
                    length,
                )
            };
            return demangle(&String::from_utf16_lossy(name));
        }
        // SAFETY: plain data, size set.
        let mut module: IMAGEHLP_MODULEW64 = unsafe { std::mem::zeroed() };
        module.SizeOfStruct = std::mem::size_of::<IMAGEHLP_MODULEW64>() as u32;
        // SAFETY: as above.
        if unsafe { SymGetModuleInfoW64(process, address, &mut module) } != 0 {
            let end = module
                .ModuleName
                .iter()
                .position(|c| *c == 0)
                .unwrap_or(module.ModuleName.len());
            return format!(
                "{}!0x{:x}",
                String::from_utf16_lossy(&module.ModuleName[..end]),
                address - module.BaseOfImage
            );
        }
        format!("0x{address:x}")
    }

    /// Attach to `pid` and load every module's symbols now, then, once
    /// `ready` fires, sample it for `duration` every `interval`: rounds
    /// taken, the stacks and how long sampling ran.
    pub fn sample(
        pid: u32,
        duration: Duration,
        interval: Duration,
        ready: super::Ready,
    ) -> Result<(u64, Vec<super::Stack>, Duration), String> {
        // SAFETY: a handle to a process the bench started, owned below.
        let process =
            Owned(unsafe { OpenProcess(PROCESS_QUERY_INFORMATION | PROCESS_VM_READ, 0, pid) });
        if process.0.is_null() {
            return Err(format!(
                "cannot open process {pid}: {}",
                std::io::Error::last_os_error()
            ));
        }
        // SAFETY: DbgHelp's global options; this thread alone uses it.
        // Loads are eager: every module's symbols load while the session
        // is set up, before the window, never during a stack walk.
        unsafe {
            SymSetOptions(SYMOPT_UNDNAME | SYMOPT_FAIL_CRITICAL_ERRORS | SYMOPT_NO_PROMPTS);
        }
        // Symbols are searched beside the executable (Rust builds name
        // the PDB without a directory), then where `_NT_SYMBOL_PATH`
        // says.
        let mut search: Vec<u16> = image_dir(process.0)
            .into_iter()
            .chain(std::env::var("_NT_SYMBOL_PATH").ok())
            .collect::<Vec<_>>()
            .join(";")
            .encode_utf16()
            .collect();
        search.push(0);
        // SAFETY: the handle is live; `search` is NUL-terminated.
        // Modules load below rather than by invading, which fails on a
        // process still starting up.
        if unsafe { SymInitializeW(process.0, search.as_ptr(), 0) } == 0 {
            return Err(format!(
                "DbgHelp cannot attach to process {pid}: {}",
                std::io::Error::last_os_error()
            ));
        }
        let symbols = Symbols(process.0);
        let mut loaded = false;
        for _ in 0..20 {
            // SAFETY: an initialized DbgHelp session for the handle.
            if unsafe { SymRefreshModuleList(symbols.0) } != 0 {
                loaded = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        if !loaded {
            return Err(format!(
                "DbgHelp cannot list the modules of process {pid}: {}",
                std::io::Error::last_os_error()
            ));
        }
        // Wait for the measured window; a run that ends first cancels.
        if ready.blocking_recv().is_err() {
            return Err("the measured window never began".to_string());
        }
        let mut known: BTreeMap<u32, Thread> = BTreeMap::new();
        let mut raw: Vec<(String, std::rc::Rc<Vec<u64>>, Option<bool>)> = Vec::new();
        let started = Instant::now();
        let mut refreshed: Option<Instant> = None;
        let mut rounds = 0u64;
        let mut failures = 0u64;
        let mut last_failure: Option<std::io::Error> = None;
        while started.elapsed() < duration {
            if refreshed.is_none_or(|at| at.elapsed() >= REFRESH) {
                let alive = threads(pid);
                known.retain(|id, _| alive.contains(id));
                for id in alive {
                    if known.contains_key(&id) {
                        continue;
                    }
                    // SAFETY: a fresh handle, owned by `Thread`.
                    let handle = unsafe {
                        OpenThread(
                            THREAD_SUSPEND_RESUME
                                | THREAD_GET_CONTEXT
                                | THREAD_QUERY_LIMITED_INFORMATION,
                            0,
                            id,
                        )
                    };
                    if handle.is_null() {
                        continue;
                    }
                    let name = describe(handle, id);
                    known.insert(
                        id,
                        Thread {
                            handle: Owned(handle),
                            name,
                            cycles: None,
                            last: None,
                        },
                    );
                }
                refreshed = Some(Instant::now());
                if known.is_empty() {
                    break;
                }
            }
            for thread in known.values_mut() {
                let mut cycles = 0u64;
                // SAFETY: a live thread handle.
                let counted = unsafe { QueryThreadCycleTime(thread.handle.0, &mut cycles) } != 0;
                let on_cpu = match (thread.cycles, counted) {
                    (Some(before), true) => Some(cycles > before),
                    _ => None,
                };
                if counted {
                    thread.cycles = Some(cycles);
                }
                // Not one cycle since its last walk: the same stack.
                if on_cpu == Some(false)
                    && let Some(last) = &thread.last
                {
                    raw.push((thread.name.clone(), last.clone(), on_cpu));
                    continue;
                }
                // SAFETY: a live thread handle with suspend rights.
                if unsafe { SuspendThread(thread.handle.0) } == u32::MAX {
                    continue;
                }
                // SAFETY: both handles live, the thread suspended.
                let walked = unsafe { walk(process.0, thread.handle.0) };
                // SAFETY: resumes the suspension above.
                unsafe {
                    ResumeThread(thread.handle.0);
                }
                // The counter after the walk, so the walk's own wait
                // for the suspension is not taken for work next round.
                let mut after = 0u64;
                // SAFETY: a live thread handle.
                if unsafe { QueryThreadCycleTime(thread.handle.0, &mut after) } != 0 {
                    thread.cycles = Some(after);
                }
                match walked {
                    Ok(pcs) => {
                        let pcs = std::rc::Rc::new(pcs);
                        thread.last = Some(pcs.clone());
                        raw.push((thread.name.clone(), pcs, on_cpu));
                    }
                    Err(error) => {
                        thread.last = None;
                        failures += 1;
                        last_failure.get_or_insert(error);
                    }
                }
            }
            rounds += 1;
            std::thread::sleep(interval);
        }
        let elapsed = started.elapsed();
        if raw.is_empty() && failures > 0 {
            return Err(format!(
                "no thread's stack could be walked ({failures} failures; last: {})",
                last_failure.map_or_else(String::new, |error| error.to_string())
            ));
        }
        let mut names: BTreeMap<u64, String> = BTreeMap::new();
        let mut stacks = Vec::with_capacity(raw.len());
        for (thread, pcs, ran) in raw {
            let frames: Vec<String> = pcs
                .iter()
                .map(|&pc| {
                    names
                        .entry(pc)
                        .or_insert_with(|| symbol(process.0, pc))
                        .clone()
                })
                .collect();
            stacks.push(Stack {
                on_cpu: on_cpu(ran, &frames),
                thread,
                frames,
                weight: 1,
            });
        }
        drop(symbols);
        Ok((rounds, stacks, elapsed))
    }

    /// A DbgHelp session, cleaned up on drop.
    struct Symbols(HANDLE);

    impl Drop for Symbols {
        fn drop(&mut self) {
            // SAFETY: matches the SymInitializeW that made the session.
            unsafe {
                SymCleanup(self.0);
            }
        }
    }
}

/// The report's CPU profile section.
pub fn render(page: &mut String, profile: Option<&CpuProfile>) {
    let Some(profile) = profile else {
        return;
    };
    let _ = write!(
        page,
        "<h2>CPU profile</h2><p class=\"muted small\">Stack samples of n{} (pid {}) from the start of the measured window, by {}. Sampling suspends the server's threads: a diagnostic, not a capacity run. Counts are stack observations, not CPU time.</p><div class=\"card\"><p>{} · {} observations over {} rounds in {:.1} s · raw folded stacks <code>{}</code> ({} bytes)</p>",
        profile.member,
        profile.pid.map_or("?".to_string(), |pid| pid.to_string()),
        esc(&profile.tool),
        esc(&profile.status),
        count(profile.samples),
        count(profile.rounds),
        profile.elapsed_ms / 1000.0,
        esc(&profile.raw_path),
        count(profile.raw_bytes)
    );
    if let Some(error) = &profile.error {
        let _ = write!(page, "<p class=\"small\">✕ {}</p>", esc(error));
    }
    page.push_str("</div>");
    let Some(summary) = &profile.summary else {
        return;
    };
    let _ = write!(
        page,
        "<div class=\"card\"><p>{} active and {} waiting observations ({}){}</p></div>",
        count(summary.active),
        count(summary.waiting),
        esc(&summary.classification),
        summary.unresolved_active.map_or(String::new(), |share| format!(
            "; {:.0}% of active stacks resolved to no name at all (build the server with debug information for symbols)",
            share * 100.0
        ))
    );
    let table = |page: &mut String, title: &str, rows: &[Ranked]| {
        if rows.is_empty() {
            return;
        }
        let _ = write!(
            page,
            "<div class=\"card table-wrap\"><h3>{}</h3><table><thead><tr><th>Frame</th><th class=\"num\">Observations</th><th class=\"num\">Share</th></tr></thead><tbody>",
            esc(title)
        );
        for row in rows {
            let _ = write!(
                page,
                "<tr><td><code>{}</code></td><td class=\"num\">{}</td><td class=\"num\">{}</td></tr>",
                esc(&row.name),
                count(row.samples),
                row.fraction
                    .map_or("—".to_string(), |share| format!("{:.1}%", share * 100.0))
            );
        }
        page.push_str("</tbody></table></div>");
    };
    table(page, "Active work by subsystem", &summary.categories);
    table(page, "Hottest active frames, inclusive", &summary.inclusive);
    table(page, "Hottest active frames, self", &summary.self_frames);
    table(
        page,
        "Where waiting threads waited",
        &summary.waiting_frames,
    );
    page.push_str("<div class=\"card table-wrap\"><h3>Threads</h3><table><thead><tr><th>Thread</th><th class=\"num\">Threads</th><th class=\"num\">Observations</th><th class=\"num\">Active</th><th class=\"num\">Waiting</th></tr></thead><tbody>");
    for group in &summary.threads {
        let _ = write!(
            page,
            "<tr><td>{}</td><td class=\"num\">{}</td><td class=\"num\">{}</td><td class=\"num\">{}</td><td class=\"num\">{}</td></tr>",
            esc(&group.thread),
            group.physical,
            count(group.total),
            count(group.active),
            count(group.waiting)
        );
    }
    page.push_str("</tbody></table>");
    for limitation in &summary.limitations {
        let _ = write!(page, "<p class=\"small muted\">{}</p>", esc(limitation));
    }
    page.push_str("</div>");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stack(thread: &str, frames: &[&str], on_cpu: Option<bool>) -> Stack {
        Stack {
            thread: thread.to_string(),
            frames: frames.iter().map(|frame| frame.to_string()).collect(),
            on_cpu,
            weight: 1,
        }
    }

    #[test]
    fn summaries_split_active_from_waiting_and_attribute_innermost_subsystems() {
        let stacks = vec![
            stack(
                "tokio-runtime-worker 12",
                &[
                    "serde_json::de::parse",
                    "flower_engine::json::render",
                    "tokio::runtime::run",
                ],
                Some(true),
            ),
            stack(
                "tokio-runtime-worker 13",
                &["redb::tree::insert", "flower_cluster::writer::commit"],
                Some(true),
            ),
            stack(
                "tokio-runtime-worker 13",
                &["redb::tree::insert", "flower_cluster::writer::commit"],
                Some(true),
            ),
            stack(
                "raft 7",
                &["ntdll!NtWaitForAlertByThreadId", "std::thread::park"],
                Some(false),
            ),
            // No counter: a wait frame decides.
            stack("io 9", &["ZwRemoveIoCompletionEx", "mio::poll"], None),
            stack("io 9", &["cargo-flower!0x1234"], None),
        ];
        let summary = summarize(&stacks);
        assert_eq!(summary.total, 6);
        assert_eq!(summary.active, 4);
        assert_eq!(summary.waiting, 2);
        assert_eq!(summary.classification, "cpu-counter");
        assert_eq!(summary.categories[0].name, "Storage (redb)");
        assert_eq!(summary.categories[0].samples, 2);
        assert_eq!(summary.categories[1].name, "JSON");
        // Shares of active observations add up.
        let shares: f64 = summary
            .categories
            .iter()
            .filter_map(|row| row.fraction)
            .sum();
        assert!((shares - 1.0).abs() < 1e-9);
        assert_eq!(summary.self_frames[0].name, "redb::tree::insert");
        assert_eq!(summary.unresolved_active, Some(0.25));
        let workers = summary
            .threads
            .iter()
            .find(|group| group.thread == "tokio-runtime-worker")
            .unwrap();
        assert_eq!((workers.physical, workers.active), (2, 3));
        assert!(
            summary
                .waiting_frames
                .iter()
                .any(|row| row.name.contains("NtWaitForAlertByThreadId"))
        );
    }

    #[test]
    fn folded_stacks_run_outermost_first_and_merge() {
        let stacks = vec![
            stack("w 1", &["leaf", "mid;dle", "root"], Some(true)),
            stack("w 1", &["leaf", "mid;dle", "root"], Some(true)),
        ];
        assert_eq!(folded(&stacks), "w 1;root;mid dle;leaf 2\n");
    }

    #[test]
    fn perf_script_parses_into_on_cpu_stacks() {
        let text = "\
tokio-runtime-w 4242/4243 [002] 1234.5: 1000000 cpu-clock:\n\
\t    55d5c2a1b2c3 _ZN4core3ptr13drop_in_place17h0123456789abcdefE+0x13 (/usr/bin/cargo-flower)\n\
\t    55d5c2a1b2d0 redb::tree::insert+0x40 (/usr/bin/cargo-flower)\n\
\t    7f0000000001 [unknown] (/usr/lib/libc.so.6)\n\
\n\
raft 4242/4250 [001] 1234.6: 1000000 cpu-clock:\n\
\t    55d5c2a1b2c3 openraft::core::run (/usr/bin/cargo-flower)\n";
        let stacks = parse_perf_script(text);
        assert_eq!(stacks.len(), 2);
        assert_eq!(stacks[0].thread, "tokio-runtime-w");
        assert_eq!(stacks[0].frames[0], "core::ptr::drop_in_place");
        assert_eq!(stacks[0].frames[1], "redb::tree::insert");
        assert_eq!(stacks[0].frames[2], "/usr/lib/libc.so.6!0x7f0000000001");
        assert_eq!(stacks[0].on_cpu, Some(true));
        assert_eq!(stacks[1].frames, vec!["openraft::core::run"]);
    }

    #[test]
    fn macos_sample_trees_become_self_stacks() {
        let text = "\
Analysis of sampling cargo-flower (pid 42) every 1 millisecond\n\
Call graph:\n\
    10 Thread_1001   DispatchQueue_1: com.apple.main-thread\n\
    + 10 start  (in dyld) + 1  [0x1]\n\
    +   6 main  (in cargo-flower) + 2  [0x2]\n\
    +   ! 6 redb::tree::insert  (in cargo-flower) + 3  [0x3]\n\
    +   4 __psynch_cvwait  (in libsystem_kernel.dylib) + 8  [0x4]\n\
\n\
Total number in stack (recursive counted multiple times):\n";
        let stacks = parse_sample(text).unwrap();
        let summary = summarize(&stacks);
        assert_eq!(summary.total, 10);
        assert_eq!(summary.active, 6);
        assert_eq!(summary.waiting, 4);
        assert_eq!(summary.self_frames[0].name, "redb::tree::insert");
        assert_eq!(summary.classification, "wait-frames");
        assert!(parse_sample("Call graph:\n    1 Thread_1\n    + 2 x\n").is_err());
    }

    #[test]
    fn a_thread_stopped_in_a_blocking_call_is_waiting_whatever_it_ran() {
        let frames =
            |names: &[&str]| -> Vec<String> { names.iter().map(|name| name.to_string()).collect() };
        let blocked = frames(&[
            "ntdll!NtWaitForAlertByThreadId",
            "RtlWaitOnAddress",
            "tokio::park",
        ]);
        let running = frames(&["redb::tree::insert", "flower_cluster::writer::commit"]);
        // It ran earlier in the interval, but is blocked now.
        assert_eq!(on_cpu(Some(true), &blocked), Some(false));
        assert_eq!(on_cpu(Some(true), &running), Some(true));
        // Ready but not scheduled since its last sample.
        assert_eq!(on_cpu(Some(false), &running), Some(false));
        // A first sample has no counter reading.
        assert_eq!(on_cpu(None, &running), None);
        assert!(waits("ZwRemoveIoCompletionEx") && waits("NtFlushBuffersFileEx"));
        assert!(!waits("Ntfs") && !waits("NtCreateFile"));
    }

    #[test]
    fn rust_names_demangle_without_their_hash() {
        assert_eq!(
            demangle("_ZN4core3ptr13drop_in_place17h0123456789abcdefE"),
            "core::ptr::drop_in_place"
        );
        assert_eq!(demangle("NtWaitForSingleObject"), "NtWaitForSingleObject");
        assert!(waits("ntdll!NtWaitForSingleObject"));
        assert!(waits("std::sys::pal::windows::c::NtWaitForAlertByThreadId"));
        assert!(!waits("redb::tree::insert"));
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn the_windows_sampler_walks_a_live_process() {
        // Sample this test process's own sibling: a child that spins.
        let mut child = std::process::Command::new("cmd")
            .args(["/C", "for /L %i in (1,1,200000000) do @rem"])
            .spawn()
            .unwrap();
        let pid = child.id();
        let dir = tempfile::tempdir().unwrap();
        let (go, task) = start(
            Some(pid),
            1,
            CpuProfileSettings {
                path: dir.path().join("cmd.folded"),
                seconds: 1,
                interval_ms: 5,
            },
        );
        go.go();
        let profile = task.await.unwrap();
        let _ = child.kill();
        let _ = child.wait();
        assert!(profile.passed(), "{profile:?}");
        assert!(profile.rounds > 10, "{profile:?}");
        let summary = profile.summary.unwrap();
        assert!(summary.total > 0);
        // The spinning interpreter is on CPU in most samples.
        assert!(summary.active > 0, "{summary:?}");
        let raw = std::fs::read_to_string(dir.path().join("cmd.folded")).unwrap();
        assert!(raw.lines().all(|line| line.rsplit_once(' ').is_some()));
    }
}
