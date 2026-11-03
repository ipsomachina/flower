//! Process-kill chaos and failover measurement.
//!
//! Halfway through the measured window one server process is killed outright:
//! the leader of the run's only group, or, when every process hosts many
//! groups, the process leading the most of them, which every group observes
//! at once like a machine failure. From that instant each group measures:
//!
//! - **election:** for a group the killed process led, until a
//!   surviving member reports itself that group's leader on
//!   `/v1/status?group=` (polled every 25 ms);
//! - **failover:** until the first write *acknowledged by a surviving
//!   member in an attempt sent after the kill*, whatever sent it
//!   (customers' orders and tips, drones' deliveries). Writes in flight
//!   at the kill do not count, so a reply that raced the kill cannot
//!   shorten it. A group whose leader survived measures how long its
//!   writes stalled behind a lost follower, usually not at all;
//! - **the burst:** the group's own failed attempts by kind, retries and
//!   failed calls from the kill until one second after its first
//!   acknowledgement.
//!
//! Once every group serves again, the killed process restarts from its
//! data directory, and each group records when its replica there has
//! caught up: applied at least the group leader's index at the restart,
//! and following that leader.

use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::future::join_all;
use serde::Serialize;

use crate::client::BenchClient;
use crate::cluster::Cluster;
use crate::live::{ChaosMark, Live};
use crate::metrics::{Burst, Recorder};

/// How long a recovery step may take before the event records it as
/// not reached.
const STEP_TIMEOUT: Duration = Duration::from_secs(60);
/// The burst window extends this far past the first acknowledgement.
const BURST_TAIL: Duration = Duration::from_secs(1);

/// One group's view of a process kill, in ms from the kill unless named
/// otherwise.
#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChaosEvent {
    /// The group (`root` for a single-group run).
    pub group: String,
    /// The killed member.
    pub killed: u64,
    /// Whether it led this group at the kill.
    pub led: bool,
    /// Its process ID.
    pub killed_pid: Option<u32>,
    /// Seconds into the measured window of the kill.
    pub killed_at_s: f64,
    /// The member leading the group after the kill.
    pub new_leader: Option<u64>,
    /// Kill to a new leader on `/v1/status` (groups it led).
    pub election_ms: Option<f64>,
    /// Kill to the first write acknowledged afterwards: the failover.
    pub failover_ms: Option<f64>,
    /// The member that acknowledged it.
    pub first_ack_member: Option<u64>,
    /// Kill to the killed member's restart.
    pub restarted_ms: Option<f64>,
    /// Kill to its replica of this group having caught up.
    pub caught_up_ms: Option<f64>,
    /// Errors and retries from the kill to a second after the failover.
    pub burst: Burst,
    /// What went wrong, if a step did not finish.
    pub error: Option<String>,
}

/// The kill itself, when one process hosts many groups.
#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProcessKill {
    /// The killed member.
    pub killed: u64,
    /// Its process ID.
    pub killed_pid: Option<u32>,
    /// Seconds into the measured window.
    pub killed_at_s: f64,
    /// The groups it led at the kill.
    pub led: Vec<String>,
    /// Groups it hosted.
    pub groups: usize,
    /// Kill to its restart, once every group served again.
    pub restarted_ms: Option<f64>,
    /// What went wrong, if anything.
    pub error: Option<String>,
}

/// One group a kill is measured in.
pub struct Target {
    /// Its index among the run's groups (the live view's chaos slot).
    pub index: usize,
    /// Its name.
    pub group: String,
    /// Its client, which spots the first acknowledgement.
    pub client: Arc<BenchClient>,
    /// Its accounting, for the burst.
    pub recorder: Arc<Recorder>,
}

/// What a chaos task needs.
pub struct Chaos {
    /// The servers.
    pub cluster: Arc<Cluster>,
    /// The groups those servers host.
    pub targets: Vec<Target>,
    /// The live view, if any.
    pub live: Option<Arc<Live>>,
    /// The start of the measured window.
    pub load_started: Instant,
}

/// The member that leads the most groups (the lowest on a tie).
pub fn busiest(leaders: &[Option<usize>], members: usize) -> Option<usize> {
    let mut counts = vec![0usize; members];
    for leader in leaders.iter().flatten() {
        if let Some(count) = counts.get_mut(*leader) {
            *count += 1;
        }
    }
    let most = *counts.iter().max()?;
    if most == 0 {
        return None;
    }
    counts.iter().position(|count| *count == most)
}

impl Chaos {
    /// Kill the busiest process at `at`, measure every group and restart
    /// it.
    pub async fn strike(&self, at: Instant) -> (ProcessKill, Vec<ChaosEvent>) {
        tokio::time::sleep_until(at.into()).await;
        let Some(first) = self.targets.first() else {
            return (ProcessKill::default(), Vec::new());
        };
        let members = first.client.members().to_vec();
        let leaders = join_all(self.targets.iter().map(|target| leader(&target.client))).await;
        let Some(victim) = busiest(&leaders, members.len()) else {
            let error = Some("no leader to kill".to_string());
            let events = self
                .targets
                .iter()
                .map(|target| ChaosEvent {
                    group: target.group.clone(),
                    error: error.clone(),
                    ..ChaosEvent::default()
                })
                .collect();
            return (
                ProcessKill {
                    error,
                    ..ProcessKill::default()
                },
                events,
            );
        };
        let killed = members[victim].id;
        let led: Vec<String> = self
            .targets
            .iter()
            .zip(&leaders)
            .filter(|(_, leader)| **leader == Some(victim))
            .map(|(target, _)| target.group.clone())
            .collect();
        self.say(if self.targets.len() == 1 {
            format!("A dragon ate the leader, n{killed}! Electing another goblin...")
        } else {
            format!(
                "A dragon ate n{killed}, which led {} of {} groups! Electing...",
                led.len(),
                self.targets.len()
            )
        });
        for target in &self.targets {
            target.recorder.begin_burst();
        }
        let kill = Instant::now();
        for target in &self.targets {
            target.client.watch_acks(kill, victim);
        }
        let pid = self.cluster.kill(victim).await;
        let t = |instant: Instant| {
            instant
                .saturating_duration_since(self.load_started)
                .as_secs_f64()
        };
        let since =
            |instant: Instant| instant.saturating_duration_since(kill).as_secs_f64() * 1000.0;
        let mut process = ProcessKill {
            killed,
            killed_pid: pid,
            killed_at_s: t(kill),
            led: led.clone(),
            groups: self.targets.len(),
            ..ProcessKill::default()
        };
        let mut marks: Vec<ChaosMark> = self
            .targets
            .iter()
            .map(|_| ChaosMark {
                killed,
                kill_t: t(kill),
                ..ChaosMark::default()
            })
            .collect();
        for (target, mark) in self.targets.iter().zip(&marks) {
            self.mark(target.index, mark);
        }

        // Each group elects (if it lost its leader) and writes again.
        let measured = join_all(
            self.targets
                .iter()
                .zip(&leaders)
                .map(|(target, leader)| self.recover(target, *leader, victim, kill)),
        )
        .await;
        let mut events = Vec::new();
        for ((target, mark), (event, elected, first)) in
            self.targets.iter().zip(marks.iter_mut()).zip(measured)
        {
            mark.new_leader = event.new_leader;
            mark.elected_t = elected.map(t);
            mark.recovered_t = first.map(t);
            mark.failover_ms = event.failover_ms;
            self.mark(target.index, mark);
            events.push(ChaosEvent {
                killed,
                killed_pid: pid,
                killed_at_s: t(kill),
                ..event
            });
        }
        if let Some(slowest) = events
            .iter()
            .filter_map(|event| event.failover_ms)
            .reduce(f64::max)
        {
            self.say(format!(
                "Serving again: {} acknowledged a write within {slowest:.0} ms of the kill.",
                if events.len() == 1 {
                    "the group".to_string()
                } else {
                    format!("all {} groups", events.len())
                }
            ));
        }

        // Restart the killed process and wait for each group to catch up.
        let targets_applied = join_all(self.targets.iter().map(|target| async {
            match leader(&target.client).await {
                Some(index) => target
                    .client
                    .status(index, Duration::from_secs(1))
                    .await
                    .ok()
                    .and_then(|status| status.last_applied),
                None => None,
            }
        }))
        .await;
        if let Err(error) = self.cluster.restart(victim) {
            let error = format!("restart failed: {error}");
            process.error = Some(error.clone());
            for event in &mut events {
                event.error.get_or_insert_with(|| error.clone());
            }
            return (process, events);
        }
        let restarted = Instant::now();
        process.restarted_ms = Some(since(restarted));
        for (target, mark) in self.targets.iter().zip(marks.iter_mut()) {
            mark.restarted_t = Some(t(restarted));
            self.mark(target.index, mark);
        }
        self.say(format!("n{killed} is back from the dragon, catching up..."));
        let caught = join_all(self.targets.iter().zip(&events).zip(&targets_applied).map(
            |((target, event), applied)| {
                caught_up(target, victim, event.new_leader, *applied, restarted)
            },
        ))
        .await;
        for ((target, mark), (event, caught)) in self
            .targets
            .iter()
            .zip(marks.iter_mut())
            .zip(events.iter_mut().zip(caught))
        {
            event.restarted_ms = process.restarted_ms;
            match caught {
                Some(at) => {
                    event.caught_up_ms = Some(since(at));
                    mark.caught_up_t = Some(t(at));
                    self.mark(target.index, mark);
                }
                None => {
                    event.error.get_or_insert_with(|| {
                        format!(
                            "n{killed} did not catch up in {} within {}s",
                            target.group,
                            STEP_TIMEOUT.as_secs()
                        )
                    });
                }
            }
        }
        if events.iter().all(|event| event.caught_up_ms.is_some()) {
            self.say(format!("n{killed} caught up in every group."));
        }
        (process, events)
    }

    /// One group after the kill: its election if it lost its leader,
    /// its first acknowledged write and its burst.
    async fn recover(
        &self,
        target: &Target,
        leader: Option<usize>,
        victim: usize,
        kill: Instant,
    ) -> (ChaosEvent, Option<Instant>, Option<Instant>) {
        let members = target.client.members();
        let since =
            |instant: Instant| instant.saturating_duration_since(kill).as_secs_f64() * 1000.0;
        let led = leader == Some(victim);
        let mut event = ChaosEvent {
            group: target.group.clone(),
            led,
            ..ChaosEvent::default()
        };
        let deadline = kill + STEP_TIMEOUT;
        let mut elected = None;
        if led {
            while Instant::now() < deadline {
                if let Some(index) = target.client.find_leader(Duration::from_millis(250)).await
                    && index != victim
                {
                    let now = Instant::now();
                    event.new_leader = Some(members[index].id);
                    event.election_ms = Some(since(now));
                    elected = Some(now);
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        } else {
            event.new_leader = leader.map(|index| members[index].id);
        }
        let first = loop {
            if let Some(first) = target.client.first_ack() {
                break Some(first);
            }
            if Instant::now() > deadline {
                break None;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        };
        let first_at = match first {
            Some((at, member)) => {
                event.failover_ms = Some(since(at));
                event.first_ack_member = Some(members[member].id);
                tokio::time::sleep_until((at + BURST_TAIL).into()).await;
                Some(at)
            }
            None => {
                event.error = Some("no write was acknowledged after the kill".to_string());
                None
            }
        };
        event.burst = target.recorder.end_burst();
        (event, elected, first_at)
    }

    fn say(&self, note: String) {
        if let Some(live) = &self.live {
            live.note(note);
        }
    }

    fn mark(&self, index: usize, mark: &ChaosMark) {
        if let Some(live) = &self.live {
            live.chaos(index, mark.clone());
        }
    }
}

/// When the restarted member's replica of `target`'s group follows its
/// leader and has applied at least `applied`.
async fn caught_up(
    target: &Target,
    victim: usize,
    leader: Option<u64>,
    applied: Option<u64>,
    restarted: Instant,
) -> Option<Instant> {
    let deadline = restarted + STEP_TIMEOUT;
    while Instant::now() < deadline {
        if let Ok(status) = target
            .client
            .status(victim, Duration::from_millis(500))
            .await
            && status.leader.is_some()
            && status.leader == leader
            && status.last_applied >= applied
        {
            return Some(Instant::now());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    None
}

/// The group's leader, asking for up to two seconds.
async fn leader(client: &BenchClient) -> Option<usize> {
    for _ in 0..40 {
        if let Some(index) = client.find_leader(Duration::from_millis(500)).await {
            return Some(index);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_busiest_process_leads_the_most_groups() {
        assert_eq!(busiest(&[Some(0)], 3), Some(0));
        assert_eq!(busiest(&[Some(1), Some(2), Some(1), None], 3), Some(1));
        // A tie goes to the lowest member.
        assert_eq!(busiest(&[Some(2), Some(1)], 3), Some(1));
        assert_eq!(busiest(&[None, None], 3), None);
        assert_eq!(busiest(&[], 3), None);
    }
}
