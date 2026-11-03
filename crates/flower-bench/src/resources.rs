//! Process CPU time and resident memory, sampled once a second for every
//! server process and the bench itself.
//!
//! Windows reads `GetProcessTimes` and the working set through the
//! process handle the bench holds; Linux reads `/proc/<pid>/stat` and
//! `statm`; other Unix systems ask `ps`, as v1 did. A summary counts CPU
//! only between consecutive samples of the same process (same PID) that
//! both fall in the measured window: a restart, a missing sample or a
//! phase edge leaves a gap, and the coverage says how much of the
//! window the figure rests on. Server CPU includes replication, the
//! drones' traffic and background work, which goodput excludes.

use std::collections::BTreeMap;

use serde::Serialize;

use crate::metrics::Phase;

/// One process's cumulative counters at one instant.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Usage {
    /// User plus kernel CPU time, in ms.
    pub cpu_ms: f64,
    /// Resident set (working set on Windows), in bytes.
    pub rss_bytes: u64,
}

/// One sample.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Sample {
    /// Seconds from the start of the measured window (negative before).
    pub t: f64,
    /// Phase when sampled.
    pub phase: Phase,
    /// Process ID.
    pub pid: u32,
    /// Cumulative CPU time, in ms.
    pub cpu_ms: f64,
    /// Resident memory, in bytes.
    pub rss_bytes: u64,
}

/// One process's summary.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProcessSummary {
    /// Highest resident memory sampled, in bytes.
    pub peak_rss_bytes: Option<u64>,
    /// Average CPU cores over the covered part of the measured window.
    pub load_cpu_cores: Option<f64>,
    /// Seconds of the measured window the CPU figure covers.
    pub covered_seconds: f64,
    /// Distinct process IDs seen (restarts add one each).
    pub pids: Vec<u32>,
    /// Every sample.
    pub samples: Vec<Sample>,
}

/// Summarize one process's samples, oldest first.
pub fn summarize(samples: &[Sample]) -> ProcessSummary {
    let mut cpu = 0.0;
    let mut covered = 0.0;
    for pair in samples.windows(2) {
        let (before, after) = (&pair[0], &pair[1]);
        if before.pid == after.pid
            && before.phase == Phase::Load
            && after.phase == Phase::Load
            && after.t > before.t
            && after.cpu_ms >= before.cpu_ms
        {
            cpu += after.cpu_ms - before.cpu_ms;
            covered += after.t - before.t;
        }
    }
    let mut pids: Vec<u32> = Vec::new();
    for sample in samples {
        if !pids.contains(&sample.pid) {
            pids.push(sample.pid);
        }
    }
    ProcessSummary {
        peak_rss_bytes: samples.iter().map(|sample| sample.rss_bytes).max(),
        load_cpu_cores: (covered > 0.0).then(|| cpu / 1000.0 / covered),
        covered_seconds: covered,
        pids,
        samples: samples.to_vec(),
    }
}

/// Every sampled process's summary by name.
pub fn summaries(samples: &BTreeMap<String, Vec<Sample>>) -> BTreeMap<String, ProcessSummary> {
    samples
        .iter()
        .map(|(name, samples)| (name.clone(), summarize(samples)))
        .collect()
}

/// The bench process's own counters.
pub fn own_usage() -> Option<Usage> {
    #[cfg(windows)]
    {
        // SAFETY: `GetCurrentProcess` returns a pseudo-handle that needs
        // no closing and is valid for the life of the process.
        let handle = unsafe { windows_sys::Win32::System::Threading::GetCurrentProcess() };
        windows::usage(handle)
    }
    #[cfg(unix)]
    {
        unix::usage(std::process::id())
    }
    #[cfg(not(any(windows, unix)))]
    {
        None
    }
}

/// A child's counters, through its handle on Windows or its PID.
pub fn child_usage(child: &tokio::process::Child) -> Option<Usage> {
    #[cfg(windows)]
    {
        windows::usage(child.raw_handle()?)
    }
    #[cfg(unix)]
    {
        unix::usage(child.id()?)
    }
    #[cfg(not(any(windows, unix)))]
    {
        let _ = child;
        None
    }
}

#[cfg(windows)]
mod windows {
    use windows_sys::Win32::Foundation::FILETIME;
    use windows_sys::Win32::System::ProcessStatus::{
        K32GetProcessMemoryInfo, PROCESS_MEMORY_COUNTERS,
    };
    use windows_sys::Win32::System::Threading::GetProcessTimes;

    use super::Usage;

    fn ticks(time: FILETIME) -> u64 {
        (u64::from(time.dwHighDateTime) << 32) | u64::from(time.dwLowDateTime)
    }

    /// Counters of the process behind `handle`, which must stay open for
    /// the call.
    pub(super) fn usage(handle: std::os::windows::io::RawHandle) -> Option<Usage> {
        let handle = handle as windows_sys::Win32::Foundation::HANDLE;
        let mut creation = FILETIME::default();
        let mut exit = FILETIME::default();
        let mut kernel = FILETIME::default();
        let mut user = FILETIME::default();
        // SAFETY: `handle` is a live process handle owned by the caller
        // for the duration of the call, and every out pointer is a valid,
        // writable FILETIME on this stack frame.
        let ok =
            unsafe { GetProcessTimes(handle, &mut creation, &mut exit, &mut kernel, &mut user) };
        if ok == 0 {
            return None;
        }
        let mut counters = PROCESS_MEMORY_COUNTERS {
            cb: std::mem::size_of::<PROCESS_MEMORY_COUNTERS>() as u32,
            ..PROCESS_MEMORY_COUNTERS::default()
        };
        // SAFETY: as above; `counters` is a writable struct whose size
        // `cb` states.
        let ok = unsafe { K32GetProcessMemoryInfo(handle, &mut counters, counters.cb) };
        if ok == 0 {
            return None;
        }
        // FILETIME counts 100 ns ticks.
        Some(Usage {
            cpu_ms: (ticks(kernel) + ticks(user)) as f64 / 10_000.0,
            rss_bytes: counters.WorkingSetSize as u64,
        })
    }
}

#[cfg(unix)]
mod unix {
    use super::Usage;

    /// Counters of process `pid`.
    pub(super) fn usage(pid: u32) -> Option<Usage> {
        #[cfg(target_os = "linux")]
        {
            proc_usage(pid)
        }
        #[cfg(not(target_os = "linux"))]
        {
            ps_usage(pid)
        }
    }

    #[cfg(target_os = "linux")]
    fn proc_usage(pid: u32) -> Option<Usage> {
        let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
        // Fields after the command name, which may hold spaces and ends
        // at the last ')': state is field 3, utime 14, stime 15.
        let rest = &stat[stat.rfind(')')? + 2..];
        let fields: Vec<&str> = rest.split_whitespace().collect();
        let utime: f64 = fields.get(11)?.parse().ok()?;
        let stime: f64 = fields.get(12)?.parse().ok()?;
        let statm = std::fs::read_to_string(format!("/proc/{pid}/statm")).ok()?;
        let pages: u64 = statm.split_whitespace().nth(1)?.parse().ok()?;
        // SAFETY: `sysconf` reads configuration values and has no
        // preconditions.
        let (hz, page) = unsafe {
            (
                libc::sysconf(libc::_SC_CLK_TCK),
                libc::sysconf(libc::_SC_PAGESIZE),
            )
        };
        if hz <= 0 || page <= 0 {
            return None;
        }
        Some(Usage {
            cpu_ms: (utime + stime) * 1000.0 / hz as f64,
            rss_bytes: pages * page as u64,
        })
    }

    #[cfg(not(target_os = "linux"))]
    fn ps_usage(pid: u32) -> Option<Usage> {
        let output = std::process::Command::new("ps")
            .args(["-o", "rss=,time=", "-p", &pid.to_string()])
            .output()
            .ok()?;
        let text = String::from_utf8(output.stdout).ok()?;
        let mut fields = text.split_whitespace();
        let kib: u64 = fields.next()?.parse().ok()?;
        let cpu_ms = super::cpu_time_ms(fields.next()?)?;
        Some(Usage {
            cpu_ms,
            rss_bytes: kib * 1024,
        })
    }
}

/// Parse `ps` CPU time, `[[dd-]hh:]mm:ss[.ss]`, into milliseconds.
#[cfg_attr(not(all(unix, not(target_os = "linux"))), allow(dead_code))]
fn cpu_time_ms(text: &str) -> Option<f64> {
    let (days, clock) = match text.split_once('-') {
        Some((days, clock)) => (days.parse::<f64>().ok()?, clock),
        None => (0.0, text),
    };
    let parts: Vec<&str> = clock.split(':').collect();
    if parts.is_empty() || parts.len() > 3 {
        return None;
    }
    let mut seconds = 0.0;
    for part in &parts {
        seconds = seconds * 60.0 + part.parse::<f64>().ok()?;
    }
    Some((days * 86_400.0 + seconds) * 1000.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(t: f64, phase: Phase, pid: u32, cpu_ms: f64, rss: u64) -> Sample {
        Sample {
            t,
            phase,
            pid,
            cpu_ms,
            rss_bytes: rss,
        }
    }

    #[test]
    fn cpu_counts_only_same_process_pairs_inside_the_window() {
        let samples = vec![
            sample(-1.0, Phase::Warmup, 10, 100.0, 50),
            sample(0.0, Phase::Load, 10, 200.0, 60),
            sample(1.0, Phase::Load, 10, 700.0, 80),
            // Killed and restarted: the pair across the restart is a gap.
            sample(2.0, Phase::Load, 11, 50.0, 40),
            sample(3.0, Phase::Load, 11, 1050.0, 45),
            sample(4.0, Phase::Drain, 11, 1100.0, 45),
        ];
        let summary = summarize(&samples);
        assert_eq!(summary.peak_rss_bytes, Some(80));
        assert_eq!(summary.covered_seconds, 2.0);
        // 500 ms + 1,000 ms of CPU over two covered seconds.
        assert!((summary.load_cpu_cores.unwrap() - 0.75).abs() < 1e-9);
        assert_eq!(summary.pids, vec![10, 11]);
        assert_eq!(summarize(&[]).load_cpu_cores, None);
    }

    #[test]
    fn ps_cpu_times_parse() {
        assert_eq!(cpu_time_ms("0:01.50"), Some(1500.0));
        assert_eq!(cpu_time_ms("01:02:03"), Some(3_723_000.0));
        assert_eq!(cpu_time_ms("1-00:00:01"), Some(86_401_000.0));
        assert_eq!(cpu_time_ms("x"), None);
    }

    #[test]
    fn own_usage_is_readable() {
        let usage = own_usage().expect("this platform samples processes");
        assert!(usage.rss_bytes > 0);
        assert!(usage.cpu_ms >= 0.0);
    }
}
