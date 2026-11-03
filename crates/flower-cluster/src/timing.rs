//! Raft timing: sub-second failover by default, scaled for slower
//! networks by one knob.
//!
//! All values are milliseconds. Use the same settings on every member.
//!
//! `FLOWER_RAFT_RTT_MS` is the round trip between the farthest two
//! replicas (0 by default: one host or one zone). The rest derive from
//! it, and each may still be set on its own, the others then deriving
//! from what was set:
//!
//! | Setting | Derived | Default (RTT ≤ 10 ms) |
//! | --- | --- | ---: |
//! | `FLOWER_RAFT_HEARTBEAT_MS` | max(50, RTT) | 50 |
//! | `FLOWER_RAFT_RPC_TIMEOUT_MS` | max(500, 3 × heartbeat, 5 × RTT + 100) | 500 |
//! | `FLOWER_RAFT_LEADER_LEASE_MS` | max(300, 6 × heartbeat) | 300 |
//! | `FLOWER_RAFT_ELECTION_MIN_MS` | max(100, 2 × heartbeat) | 100 |
//! | `FLOWER_RAFT_ELECTION_MAX_MS` | 2 × election min | 200 |
//! | `FLOWER_RAFT_READ_LEASE_MS` | 9/10 × leader lease (0: off) | 270 |
//!
//! The heartbeat says how often a leader sends; the RPC timeout says how
//! long it waits for an AppendEntries answer, data or heartbeat, and for
//! the heartbeats that confirm its leadership for a linearizable read.
//! The answer waits for the follower's log fsync, and a heartbeat waits
//! behind the appends ahead of it: every commit here fsyncs, and on a
//! ZFS mirror without a log device fsyncs take 66–110 ms at the 99th
//! percentile and above. Registry OpenRaft waits only the heartbeat
//! interval, so once a round trip plus a follower's fsync passed 50 ms,
//! reads lost their quorum and appends restarted forever. The RPC
//! timeout is therefore at least 500 ms, and five round trips plus
//! 100 ms for queued fsyncs. It reaches OpenRaft as `Config::rpc_timeout`,
//! which Flower's OpenRaft fork adds (`vendor/openraft/README.md`).
//!
//! The leader lease (`Config::leader_lease`, also the fork's) is how long
//! a voter refuses other candidates after its leader's last message, and
//! how long a follower waits before it asks to campaign; the election
//! timeout, drawn once per member between min and max, only spreads the
//! members apart past it. A follower then asks the voters whether they
//! would vote for it (a pre-vote, `Config::pre_vote`), and campaigns
//! only once a quorum would: a voter says yes only when its own lease
//! ran out and the asker's log is at least as long as its own. So a
//! follower that alone stops hearing its leader (its own fsync stalls,
//! or the leader's appends to it queue behind a slow one) raises no
//! term and deposes nobody, and elections need not wait out the RPC
//! timeout; before pre-vote they were held at the RPC timeout plus a
//! heartbeat (0.55–1.1 s, 1.5–3 s with the registry crate's timing).
//! The lease must outlast the gaps between a healthy follower's
//! messages from its leader: 1.5 heartbeats between ticks, plus a round
//! trip and the fsync of the append ahead.
//!
//! Failover: a follower asks to campaign once its lease plus its election
//! timeout passed since it last heard the leader, checked on ticks 1.5
//! heartbeats apart, then runs a pre-vote and a vote round; the new leader
//! commits an entry of its term and clients' writes, parked at the other
//! members meanwhile, go through. Detection takes 0.4–0.5 s by default and
//! 0.8–1 s at a 100 ms RTT. Killing the leader of a three-member group under
//! sustained writes, a client had a write acknowledged after a median of 0.56
//! s and at worst 0.65 s over a hundred kills
//! (`flower-client/tests/failover.rs`; partitioning the leader instead, 0.63
//! s at worst over fifty). The registry crate's timing took 4.5–6 s. Every
//! group a process hosts runs the configuration [`raft_config`] builds, so a
//! group other than the root fails over in the same bound (0.65 s at worst
//! over fifty kills of its leader's process).
//!
//! What raising the timers trades: a longer lease and election timeout
//! tolerate longer stalls (a leader whose own log fsync stalls for more
//! than the lease less 1.5 heartbeats, about 225 ms by default, loses
//! its leadership to a failover) at the cost of a slower failover, about
//! the lease plus the longest election timeout plus 0.1–0.2 s. A larger
//! heartbeat costs the same and saves little on a LAN.
//!
//! A follower's own flush is not its leader's silence: the leader sends
//! it nothing more until it answers, so its lease and election timer run
//! from when its leader's entries are durable (the OpenRaft fork's
//! change 7), and followers whose disks stall together keep their
//! leader. The leader counts as cut off from its quorum, and refuses
//! writes as `leader_isolated`, only after the lease plus the shortest
//! election timeout without a quorum's acknowledgement (400 ms by
//! default; see `node::leadership::isolation_window`). That bounds
//! liveness only: a follower that lost the leader earlier is granted a
//! pre-vote once the others' leases ran out, so another leader can exist
//! from the lease on.
//!
//! The read lease lets a leader answer fresh reads without a quorum
//! round for that long after a round that began before them succeeded
//! (see [`crate::read`]): OpenRaft's voters refuse every candidate for
//! the leader lease after an append from their leader reaches them, and
//! wait longer still before asking to campaign themselves, so no other
//! leader can exist yet. It assumes members' monotonic clocks advance at
//! nearly the same rate (the tenth left over covers rates up to 5%
//! apart; NTP slews by at most 0.05%) and never stop while a member
//! runs: a host suspended to RAM could serve a stale read on waking.
//! Set it to 0 to prove every fresh read with a quorum round instead.
//!
//! Explicit settings win, within what validation refuses: an election
//! timeout no longer than the heartbeat, a lease shorter than two
//! heartbeats, or a read lease at least as long as the leader lease.

use std::time::{Duration, Instant};

use openraft::{Config, SnapshotPolicy};

/// The shortest default RPC deadline: above a follower's fsync tail
/// with queued appends ahead of a heartbeat (module documentation).
pub const MIN_RPC_TIMEOUT_MS: u64 = 500;

/// The shortest default leader lease: a healthy follower hears from its
/// leader several times within it (module documentation).
pub const MIN_LEADER_LEASE_MS: u64 = 300;

/// Operator timing settings; `None` derives the value.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TimingSettings {
    /// `FLOWER_RAFT_RTT_MS`: farthest replica round trip.
    pub rtt_ms: Option<u64>,
    /// `FLOWER_RAFT_HEARTBEAT_MS`: how often a leader sends heartbeats.
    pub heartbeat_ms: Option<u64>,
    /// `FLOWER_RAFT_RPC_TIMEOUT_MS`: AppendEntries and quorum deadline.
    pub rpc_timeout_ms: Option<u64>,
    /// `FLOWER_RAFT_LEADER_LEASE_MS`: how long voters refuse other
    /// candidates after hearing from their leader.
    pub leader_lease_ms: Option<u64>,
    /// `FLOWER_RAFT_ELECTION_MIN_MS`.
    pub election_min_ms: Option<u64>,
    /// `FLOWER_RAFT_ELECTION_MAX_MS`.
    pub election_max_ms: Option<u64>,
    /// `FLOWER_RAFT_READ_LEASE_MS`: 0 turns lease reads off.
    pub read_lease_ms: Option<u64>,
}

/// Derived, validated Raft timing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RaftTiming {
    /// How often a leader sends heartbeats.
    pub heartbeat_ms: u64,
    /// How long a leader waits for an AppendEntries answer.
    pub rpc_timeout_ms: u64,
    /// How long voters refuse other candidates after hearing from their
    /// leader, and wait before campaigning themselves.
    pub leader_lease_ms: u64,
    /// Shortest election timeout, past the lease.
    pub election_min_ms: u64,
    /// Longest election timeout, past the lease.
    pub election_max_ms: u64,
    /// How long a confirmed leader serves fresh reads without another
    /// quorum round; 0 when lease reads are off.
    pub read_lease_ms: u64,
}

/// The default read lease for a leader lease: nine tenths of it.
pub fn default_read_lease_ms(leader_lease_ms: u64) -> u64 {
    leader_lease_ms / 10 * 9
}

/// Snapshot and log retention settings.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SnapshotSettings {
    /// Build a snapshot every this many applied entries.
    pub logs_since_last: u64,
    /// Entries kept after a snapshot: the sync-down window devices and
    /// lagging followers catch up from without a snapshot.
    pub keep_logs: u64,
    /// Deadline for sending and installing the last snapshot segment.
    pub install_timeout_ms: u64,
    /// Snapshot segment size on the wire.
    pub chunk_bytes: u64,
}

impl Default for SnapshotSettings {
    fn default() -> Self {
        // Snapshots encode every database, so they stay rare; the kept
        // window lets devices offline for a while resume from entries.
        // Installing the last segment applies the whole snapshot, far
        // longer than OpenRaft's 200 ms default.
        SnapshotSettings {
            logs_since_last: 10_000,
            keep_logs: 10_000,
            install_timeout_ms: 60_000,
            chunk_bytes: 4 * 1024 * 1024,
        }
    }
}

impl RaftTiming {
    /// Derive timing from operator settings, validating the result.
    pub fn derive(settings: &TimingSettings) -> Result<Self, String> {
        let rtt = settings.rtt_ms.unwrap_or(0);
        // Checked: an absurd RTT must fail here, not wrap.
        let scaled = |factor: u64, plus: u64| {
            rtt.checked_mul(factor)
                .and_then(|value| value.checked_add(plus))
                .ok_or_else(|| "FLOWER_RAFT_RTT_MS overflows Raft's timer arithmetic".to_string())
        };
        let heartbeat = settings.heartbeat_ms.unwrap_or(50u64.max(rtt));
        let rpc_timeout = match settings.rpc_timeout_ms {
            Some(value) => value,
            None => MIN_RPC_TIMEOUT_MS
                .max(heartbeat.saturating_mul(3))
                .max(scaled(5, 100)?),
        };
        let leader_lease = settings
            .leader_lease_ms
            .unwrap_or(MIN_LEADER_LEASE_MS.max(heartbeat.saturating_mul(6)));
        let election_min = settings
            .election_min_ms
            .unwrap_or(100u64.max(heartbeat.saturating_mul(2)));
        let election_max = settings
            .election_max_ms
            .unwrap_or(election_min.saturating_mul(2));
        let read_lease = settings
            .read_lease_ms
            .unwrap_or(default_read_lease_ms(leader_lease));
        let timing = RaftTiming {
            heartbeat_ms: heartbeat,
            rpc_timeout_ms: rpc_timeout,
            leader_lease_ms: leader_lease,
            election_min_ms: election_min,
            election_max_ms: election_max,
            read_lease_ms: read_lease,
        };
        timing.validate()?;
        Ok(timing)
    }

    /// The longest a follower waits after its leader's last heartbeat
    /// before it asks to campaign: the lease plus the longest election
    /// timeout.
    pub fn longest_detection(&self) -> Duration {
        Duration::from_millis(self.leader_lease_ms.saturating_add(self.election_max_ms))
    }

    /// Reject inconsistent or overflowing timing.
    fn validate(&self) -> Result<(), String> {
        for (name, value) in [
            ("FLOWER_RAFT_HEARTBEAT_MS", self.heartbeat_ms),
            ("FLOWER_RAFT_RPC_TIMEOUT_MS", self.rpc_timeout_ms),
            ("FLOWER_RAFT_LEADER_LEASE_MS", self.leader_lease_ms),
            ("FLOWER_RAFT_ELECTION_MIN_MS", self.election_min_ms),
            ("FLOWER_RAFT_ELECTION_MAX_MS", self.election_max_ms),
        ] {
            if value == 0 {
                return Err(format!("{name} must be positive"));
            }
        }
        if self.rpc_timeout_ms < self.heartbeat_ms {
            return Err(format!(
                "FLOWER_RAFT_RPC_TIMEOUT_MS ({}) must be at least FLOWER_RAFT_HEARTBEAT_MS ({})",
                self.rpc_timeout_ms, self.heartbeat_ms
            ));
        }
        // Heartbeats go out on ticks 1.5 heartbeats apart: a lease shorter
        // than two heartbeats would lapse between them.
        if self.leader_lease_ms < self.heartbeat_ms.saturating_mul(2) {
            return Err(format!(
                "FLOWER_RAFT_LEADER_LEASE_MS ({}) must be at least twice FLOWER_RAFT_HEARTBEAT_MS \
                 ({}): it would lapse between heartbeats",
                self.leader_lease_ms, self.heartbeat_ms
            ));
        }
        // OpenRaft: a follower must hear at least one heartbeat before
        // it may campaign, and the random range must be non-empty.
        if self.election_min_ms <= self.heartbeat_ms {
            return Err(format!(
                "FLOWER_RAFT_ELECTION_MIN_MS ({}) must exceed FLOWER_RAFT_HEARTBEAT_MS ({})",
                self.election_min_ms, self.heartbeat_ms
            ));
        }
        if self.election_max_ms <= self.election_min_ms {
            return Err(format!(
                "FLOWER_RAFT_ELECTION_MAX_MS ({}) must exceed FLOWER_RAFT_ELECTION_MIN_MS ({})",
                self.election_max_ms, self.election_min_ms
            ));
        }
        // Voters refuse other candidates for the leader lease after
        // hearing from their leader: a read lease that long or longer
        // could outlive it.
        if self.read_lease_ms >= self.leader_lease_ms {
            return Err(format!(
                "FLOWER_RAFT_READ_LEASE_MS ({}) must be shorter than FLOWER_RAFT_LEADER_LEASE_MS \
                 ({}): voters refuse other candidates only that long",
                self.read_lease_ms, self.leader_lease_ms
            ));
        }
        // OpenRaft multiplies the heartbeat by 3 before dividing by 2 (its
        // tick), waits the RPC timeout, adds the lease to an election
        // timeout and doubles election max: each must stay a
        // representable deadline.
        for (name, value, multiplier) in [
            ("FLOWER_RAFT_HEARTBEAT_MS", self.heartbeat_ms, 3),
            ("FLOWER_RAFT_RPC_TIMEOUT_MS", self.rpc_timeout_ms, 1),
            ("FLOWER_RAFT_LEADER_LEASE_MS", self.leader_lease_ms, 2),
            ("FLOWER_RAFT_ELECTION_MAX_MS", self.election_max_ms, 4),
        ] {
            let derived = value
                .checked_mul(multiplier)
                .ok_or_else(|| format!("{name} overflows OpenRaft's timer arithmetic"))?;
            if Instant::now()
                .checked_add(Duration::from_millis(derived))
                .is_none()
            {
                return Err(format!("{name} exceeds this platform's deadline range"));
            }
        }
        Ok(())
    }
}

/// Build the OpenRaft configuration from timing and snapshot settings.
pub fn raft_config(timing: &RaftTiming, snapshots: &SnapshotSettings) -> Result<Config, String> {
    if snapshots.logs_since_last == 0 || snapshots.chunk_bytes == 0 {
        return Err("snapshot interval and segment size must be positive".to_string());
    }
    let config = Config {
        heartbeat_interval: timing.heartbeat_ms,
        // Flower's OpenRaft fork: the AppendEntries and quorum deadline,
        // apart from how often heartbeats go out; the voters' refusal
        // window, apart from the election timeouts; and a pre-vote before
        // every election the timer starts.
        rpc_timeout: timing.rpc_timeout_ms,
        leader_lease: timing.leader_lease_ms,
        pre_vote: true,
        election_timeout_min: timing.election_min_ms,
        election_timeout_max: timing.election_max_ms,
        install_snapshot_timeout: snapshots.install_timeout_ms.max(timing.rpc_timeout_ms),
        snapshot_policy: SnapshotPolicy::LogsSinceLast(snapshots.logs_since_last),
        max_in_snapshot_log_to_keep: snapshots.keep_logs,
        // A follower further behind than a snapshot interval plus the
        // kept window catches up by snapshot, as OpenRaft advises.
        replication_lag_threshold: snapshots
            .logs_since_last
            .saturating_add(snapshots.keep_logs),
        snapshot_max_chunk_size: snapshots.chunk_bytes,
        ..Config::default()
    };
    config.validate().map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn timing(settings: TimingSettings) -> Result<(u64, u64, u64, u64, u64), String> {
        let timing = RaftTiming::derive(&settings)?;
        Ok((
            timing.heartbeat_ms,
            timing.rpc_timeout_ms,
            timing.leader_lease_ms,
            timing.election_min_ms,
            timing.election_max_ms,
        ))
    }

    #[test]
    fn defaults_wait_out_fsync_tails_and_campaign_soon_after_the_lease() {
        let derived = RaftTiming::derive(&TimingSettings::default()).unwrap();
        assert_eq!(derived.heartbeat_ms, 50);
        assert_eq!(derived.rpc_timeout_ms, MIN_RPC_TIMEOUT_MS);
        assert_eq!(derived.leader_lease_ms, MIN_LEADER_LEASE_MS);
        // The election timeout only spreads candidates past the lease.
        assert_eq!(derived.election_min_ms, 100);
        assert_eq!(derived.election_max_ms, 200);
        let config = raft_config(&derived, &SnapshotSettings::default()).unwrap();
        // Unlike OpenRaft's defaults (50 ms deadline, 150–300 ms elections
        // behind a 300 ms lease), a 100 ms fsync neither times out nor
        // campaigns, while heartbeats keep their 50 ms interval.
        assert_eq!(config.heartbeat_interval, 50);
        assert_eq!(config.rpc_timeout(), Duration::from_millis(500));
        assert_eq!(config.leader_lease(), Duration::from_millis(300));
        assert!(config.pre_vote);
        assert!(config.install_snapshot_timeout >= 10_000);
        assert_eq!(
            config.snapshot_policy,
            SnapshotPolicy::LogsSinceLast(10_000)
        );
        assert_eq!(config.max_in_snapshot_log_to_keep, 10_000);
        assert!(config.replication_lag_threshold > 10_000);
    }

    /// Pins the failover bound the module documents to the derived settings:
    /// a follower asks to campaign once its lease plus its randomized
    /// election timeout (min..max) passed since it last heard its leader, so
    /// failover detection takes 0.4–0.5 s by default (4.5–6 s with the
    /// registry crate's timing), and 0.8–1 s at a 100 ms RTT.
    /// `flower-client/tests/failover.rs` measures what clients see.
    #[test]
    fn failover_waits_the_leader_lease_on_top_of_an_election_timeout() {
        let bound = |settings: TimingSettings| {
            let derived = RaftTiming::derive(&settings).unwrap();
            let config = raft_config(&derived, &SnapshotSettings::default()).unwrap();
            let lease = config.leader_lease().as_millis() as u64;
            let fastest = lease + config.election_timeout_min;
            let slowest = lease + config.election_timeout_max;
            assert_eq!(derived.longest_detection(), Duration::from_millis(slowest));
            (fastest, slowest)
        };
        assert_eq!(bound(TimingSettings::default()), (400, 500));
        let wide = TimingSettings {
            rtt_ms: Some(100),
            ..TimingSettings::default()
        };
        assert_eq!(bound(wide), (800, 1_000));
    }

    #[test]
    fn one_round_trip_setting_scales_every_raft_timer() {
        for (rtt, expected) in [
            (0, (50, 500, 300, 100, 200)),
            (10, (50, 500, 300, 100, 200)),
            (100, (100, 600, 600, 200, 400)),
            (300, (300, 1600, 1800, 600, 1200)),
        ] {
            let settings = TimingSettings {
                rtt_ms: Some(rtt),
                ..TimingSettings::default()
            };
            assert_eq!(timing(settings).unwrap(), expected, "RTT {rtt}");
        }
    }

    #[test]
    fn explicit_timers_win_and_the_rest_derive_from_them() {
        let heartbeat = TimingSettings {
            heartbeat_ms: Some(200),
            ..TimingSettings::default()
        };
        assert_eq!(timing(heartbeat).unwrap(), (200, 600, 1200, 400, 800));
        let rpc = TimingSettings {
            rpc_timeout_ms: Some(1000),
            ..TimingSettings::default()
        };
        assert_eq!(timing(rpc).unwrap(), (50, 1000, 300, 100, 200));
        let lease = TimingSettings {
            leader_lease_ms: Some(1000),
            ..TimingSettings::default()
        };
        assert_eq!(timing(lease).unwrap(), (50, 500, 1000, 100, 200));
        // Operators may restore OpenRaft's stock timing explicitly.
        let stock = TimingSettings {
            rpc_timeout_ms: Some(50),
            election_min_ms: Some(150),
            election_max_ms: Some(300),
            ..TimingSettings::default()
        };
        assert_eq!(timing(stock).unwrap(), (50, 50, 300, 150, 300));
    }

    #[test]
    fn invalid_and_inconsistent_timing_fails() {
        for bad in [
            TimingSettings {
                heartbeat_ms: Some(0),
                ..TimingSettings::default()
            },
            TimingSettings {
                rpc_timeout_ms: Some(20),
                ..TimingSettings::default()
            },
            // No heartbeat could reach a follower before it campaigns.
            TimingSettings {
                election_min_ms: Some(50),
                ..TimingSettings::default()
            },
            TimingSettings {
                election_max_ms: Some(100),
                ..TimingSettings::default()
            },
            // The lease would lapse between heartbeats.
            TimingSettings {
                leader_lease_ms: Some(60),
                ..TimingSettings::default()
            },
            TimingSettings {
                rtt_ms: Some(u64::MAX),
                ..TimingSettings::default()
            },
            TimingSettings {
                election_max_ms: Some(u64::MAX),
                ..TimingSettings::default()
            },
        ] {
            assert!(timing(bad).is_err(), "{bad:?} should fail");
        }
        let error = timing(TimingSettings {
            rpc_timeout_ms: Some(20),
            ..TimingSettings::default()
        })
        .unwrap_err();
        assert!(error.contains("FLOWER_RAFT_RPC_TIMEOUT_MS"), "{error}");
        let error = timing(TimingSettings {
            leader_lease_ms: Some(60),
            ..TimingSettings::default()
        })
        .unwrap_err();
        assert!(error.contains("FLOWER_RAFT_LEADER_LEASE_MS"), "{error}");
    }

    #[test]
    fn the_read_lease_ends_before_voters_stop_refusing_and_can_be_off() {
        let lease =
            |settings: TimingSettings| RaftTiming::derive(&settings).map(|t| t.read_lease_ms);
        assert_eq!(lease(TimingSettings::default()).unwrap(), 270);
        assert_eq!(
            lease(TimingSettings {
                rtt_ms: Some(100),
                ..TimingSettings::default()
            })
            .unwrap(),
            540
        );
        assert_eq!(
            lease(TimingSettings {
                read_lease_ms: Some(0),
                ..TimingSettings::default()
            })
            .unwrap(),
            0
        );
        // Voters refuse other candidates for the leader lease after an
        // append reaches them: the read lease must end first.
        for bad in [300, 301, u64::MAX] {
            let error = lease(TimingSettings {
                read_lease_ms: Some(bad),
                ..TimingSettings::default()
            })
            .unwrap_err();
            assert!(error.contains("FLOWER_RAFT_READ_LEASE_MS"), "{error}");
        }
        let explicit = RaftTiming::derive(&TimingSettings {
            leader_lease_ms: Some(4000),
            read_lease_ms: Some(3999),
            ..TimingSettings::default()
        })
        .unwrap();
        assert!(explicit.read_lease_ms < explicit.leader_lease_ms);
    }

    #[test]
    fn snapshot_settings_validate() {
        let derived = RaftTiming::derive(&TimingSettings::default()).unwrap();
        let zero = SnapshotSettings {
            logs_since_last: 0,
            ..SnapshotSettings::default()
        };
        assert!(raft_config(&derived, &zero).is_err());
    }
}
