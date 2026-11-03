# Flower's OpenRaft 0.9.25

This is the published `openraft` 0.9.25 crate (upstream VCS record
`8815cdba2826f74e848acef361ad03f93bb1c3f8`, path `openraft`): its `src` and
manifest, without `Cargo.toml.orig`, `Cargo.lock` or upstream's README. The
first commit that added this directory holds it byte for byte (the manifest
gains only a header comment); every change since is marked `Flower:` in the
source and listed here. It is published as `flower-openraft` and used through
an explicit versioned dependency, aliased to `openraft` in Rust. This
keeps the fork when consumers install Flower from crates.io. Local workspaces
patch that same package name to this directory. The normal
workspace excludes it from its members, as it does `vendor/redb`; the release
workspace includes both forks. The license remains upstream's,
`MIT OR Apache-2.0`. The license texts are copied from the upstream commit
above (the original published crate omitted them).

Seven changes. The first three and the sixth are ported from Flower v1's fork
(`back/flower/vendor/openraft`); the fourth and fifth take failover under a
second, and the seventh keeps a leader under load. Each is covered by
tests in Flower's crates, named below with what fails without the change, and
OpenRaft's own unit tests pass on the patched crate
(`cargo test --lib --features serde,storage-v2,loosen-follower-log-revert`
from a copy of this directory: 197, seven of them Flower's pre-vote engine
tests).

## 1. An RPC deadline apart from the heartbeat

Upstream bounds every AppendEntries RPC, data or heartbeat, and the heartbeats
`Raft::ensure_linearizable` sends to confirm leadership, by
`heartbeat_interval`. A follower answers an append only once its log is
durable, and a heartbeat waits behind the appends ahead of it, so a round trip
plus a follower's fsync longer than the heartbeat failed the RPC: reads lost
their quorum and appends restarted forever. Flower therefore had to run 500 ms
heartbeats, which held elections at 1.5–3 s and failover at 4.5–6 s.

- `src/config/config.rs`: `Config::rpc_timeout`, in milliseconds (`0`, the
  default, keeps upstream's `heartbeat_interval`), and `Config::rpc_timeout()`.
- `src/replication/mod.rs`: `send_log_entries` waits `rpc_timeout()`.
- `src/core/raft_core.rs`: `handle_check_is_leader_request` waits
  `rpc_timeout()`.

Flower runs 50 ms heartbeats with a 500 ms RPC deadline. A follower whose
fsync plus round trip exceeds the heartbeat keeps its leader and keeps
replicating under sustained writes; with upstream's deadline it stays stuck at
its first entry (`flower-cluster/tests/failover.rs`,
`thirty_seconds_with_a_slow_follower_keep_one_leader_and_one_term`).

## 2. Snapshot segments renew the leader's lease

Upstream receives InstallSnapshot segments outside `RaftCore`, reading the
local vote without accepting the leader's, so no segment refreshes the
follower's leader lease; and a leader sends a follower nothing else (no
heartbeat) while a snapshot is in flight to it. A transfer longer than the
lease plus an election timeout made the follower campaign, and the new term
restarted the transfer, forever.

- `src/core/raft_msg/mod.rs`: `RaftMsg::SnapshotChunk { vote, tx }`.
- `src/engine/engine_impl.rs`: `Engine::handle_snapshot_chunk` accepts the
  vote as `handle_install_full_snapshot` does (`VoteHandler::accept_vote`,
  which touches the vote's time on equal votes), and answers the local vote.
- `src/core/raft_core.rs`: `handle_api_msg` dispatches it.
- `src/raft/mod.rs`: `Raft::install_snapshot` sends each segment's vote to the
  core as `SnapshotChunk` before receiving the segment, in place of upstream's
  read-only check (a rejected vote returns the local one, as before); while
  the final segment's install runs, it sends the same message every heartbeat
  interval without awaiting the replies, which queue behind the install's.

A segment still has to arrive within the leader lease (300 ms by default) for
the follower to stay quiet; pre-vote (change 5) keeps a follower that does
not from raising a term while the other voters hear the leader. Test, with
pre-vote off: a snapshot of nine segments 500 ms apart under a 1 s lease
starts no election
(`a_snapshot_transfer_longer_than_the_election_timeout_starts_no_election`);
with the segment's vote only read, it is never installed.

## 3. Lease release for leadership transfer

Upstream 0.9 has no leadership transfer, and its voters refuse every vote
request for the leader lease after their leader's last message (the leader
itself, for as long after it began to lead). Flower's transfer
(`crates/flower-cluster/src/node/leadership.rs`) had to wait those leases out:
in a group of four or more voters, by holding replication back from every
voter but the target for a lease.

- `src/engine/engine_impl.rs`: `Engine::lease_released`, set by
  `Engine::release_leader_lease(vote)` when `vote` is this node's committed
  vote; `handle_vote_req` skips the lease check for that vote, so Raft's term
  and log rules alone decide. A new vote makes the release moot.
- `src/core/raft_msg/mod.rs`, `src/core/raft_core.rs`:
  `RaftMsg::ReleaseLeaderLease { vote, tx }`, answered with whether the lease
  was released.
- `src/raft/mod.rs`: `Raft::release_leader_lease(vote)`.

The leader releases its own lease and asks every other voter (Flower's
`TransferLeader` peer request names its target) before the target campaigns.
A release voids the refusal the read lease rests on, so the leader surrenders
its read lease for the term first, and a leadership OpenRaft restores from the
files on reopening confirms no read lease (`crates/flower-cluster/src/read.rs`;
tests `leadership_moves_past_other_voters_leases`, which completes within an
election timeout, and `a_leadership_restored_on_reopening_confirms_no_lease`).
`cargo flower serve` hands over the leadership of every group it leads this
way, all at once, when it stops on a signal (`Host::hand_off_leadership`), so
a planned restart costs each group a transfer, not a failover
(`a_stopping_leader_hands_off_so_no_election_follows`,
`a_stopping_host_hands_off_every_group_it_leads`). v1's
fork carries the whole transfer in the core; v2 keeps the rest in
`leadership.rs` and `read.rs`.

## 4. A leader lease apart from the election timeout

Upstream sets the leader lease to `election_timeout_max`
(`EngineConfig::new`): a follower holding a committed vote refuses candidates
for election max after its leader's last message and campaigns only after
election max plus its election timeout, so failover took election max plus
election min..max. With elections that had to clear the RPC deadline (before
change 5), that was 1.65–2.2 s.

- `src/config/config.rs`: `Config::leader_lease`, in milliseconds (`0`, the
  default, keeps `election_timeout_max`), and `Config::leader_lease()`.
- `src/engine/engine_config.rs`: `EngineConfig::new` takes the lease from it.

Flower runs a 300 ms lease with election timeouts of 100–200 ms past it: a
follower asks to campaign 0.4–0.5 s after its leader's last heartbeat. Its
read lease stays nine tenths of the leader lease (`timing.rs`, `read.rs`).
Without the field, upstream's lease would be election max (200 ms), shorter
than that 270 ms read lease, which could then serve a stale read; the timing
tests pin the lease (`failover_waits_the_leader_lease_on_top_of_an_election_timeout`),
and with v1's timing instead the client failover tests fail (election alone
took 1.65–2.2 s).

## 5. Pre-vote

A follower whose election timer fires campaigns at once upstream, raising its
term. A follower that alone stopped hearing its leader (its own fsync stalls,
or the leader's appends to it queue behind a slow one) then deposes a leader
the others still follow, once its higher term reaches them; and one cut off
by a partition comes back with a term that deposes the leader. Elections
therefore had to wait out the RPC deadline plus a heartbeat. Under load, a
lagging follower that campaigned first could also force rounds of split votes
before the most up-to-date one won (3.2 s in the benchmark).

- `src/raft/message/vote.rs`: `VoteRequest::pre_vote` (`#[serde(default)]`)
  and `VoteRequest::new_pre_vote`; the request literals in OpenRaft's engine
  tests set it.
- `src/config/config.rs`: `Config::pre_vote` (off by default, as upstream).
- `src/engine/engine_impl.rs`: `Engine::handle_pre_vote_req` answers whether
  this node would grant the vote now, changing nothing: not while it leads,
  not while its leader's lease runs, not to a log behind its own, not to a
  vote that does not supersede its own. `handle_vote_req` sends pre-votes
  there. `Engine::handle_pre_vote_refused` adopts, uncommitted, a greater
  vote a refusal carries, as `handle_vote_resp` does for a refused vote and
  etcd's pre-vote does with the refusal's term: asking for its own term + 1
  again and again, a member was refused on every timeout by a voter that
  had voted in that term already, and when that voter's log was the shorter
  neither could be elected.
- `src/core/raft_core.rs`: when the election timer passes and `pre_vote` is
  on, `handle_tick_election` asks for a pre-vote (at most once per election
  timeout) instead of electing; `send_pre_vote`, run at the end of each turn
  of the core's loop, asks every other voter for the next term and reports
  `Notify::PreVoteGranted` once a quorum says yes, which runs the timer's
  checks again and elects, and `Notify::PreVoteRefused` for a refusal
  holding a greater vote, adopted unless this node's vote changed meanwhile.
  `RaftCore::pre_vote_started` and `pre_vote_due` hold that state
  (`src/raft/mod.rs` initializes them).
- `src/core/notify.rs`: `Notify::PreVoteGranted` and `Notify::PreVoteRefused`.
- `src/engine/tests/handle_pre_vote_req_test.rs` (new, beside upstream's
  `handle_vote_req_test.rs`): a pre-vote granted once the lease ran out
  changes no vote, vote time, server state or command, and the real vote
  that follows is granted; one is refused within the lease unless released,
  for a shorter log, for a smaller vote and while leading. With
  `handle_vote_req` taking pre-votes for votes, three of the five fail. A
  refusal's greater vote is adopted uncommitted, a smaller or equal one
  changes nothing; and the member with the longest log, refused by a voter
  that voted in its next term already, is granted the term after it.

Elections started with `Raft::trigger().elect()` (leadership transfer) skip
it. Tests: thirty seconds of writes with a follower whose fsync takes 20–90 ms
more (100–150 ms at its 99th percentile, 250 ms stalls) under jittering round
trips keep one leader and one term
(`thirty_seconds_with_a_slow_follower_keep_one_leader_and_one_term`; its gaps
stay inside the lease and election timeout, so it passes without pre-vote
too); a follower partitioned away and back deposes nobody
(`a_partitioned_follower_deposes_nobody_when_it_returns`; without pre-vote it
returns with term 20 and deposes the leader); of five voters, the one holding
an entry nobody else has is elected although a survivor holds a vote in its
next term, from a campaign the two members that died never answered
(`a_pre_vote_refused_by_a_higher_vote_asks_for_the_next_term`; without
adopting the refusal's vote no leader is elected); and failover after a
crash or a partition of the leader, as clients see it, stays under a second
(`flower-client/tests/failover.rs`: a median of 0.56 s and at worst 0.65 s
over a hundred crashes, 0.63 s at worst over fifty partitions, and the same
for a group other than the root: 0.65 s and 0.60 s at worst over fifty
each).

## 6. Parallel leader append

Upstream awaits the leader's own log flush inside `RaftCore::append_to_log`
before it runs the `Replicate` commands queued behind `AppendInputEntries`, so
a commit paid the leader's fsync and then a follower's, one after the other.
Raft lets a leader write its log in parallel with replication (Ongaro's
thesis, section 10.2.1), provided it counts itself toward the commit quorum
only for entries it has flushed. This is v1's patch.

- `src/storage/callback.rs`: `LogFlushed::new_leader` and
  `LogFlushed::is_leader_append()`: the current leader's own append, whose
  entries storage must make readable before `append` returns and order before
  every later log write, but whose callback it may complete later than a
  follower's.
- `src/core/raft_core.rs`: `AppendInputEntries` under the current leader's
  vote calls `submit_leader_append`, which returns once `append` did; a
  spawned task waits for the callback and sends `Notify::LocalLogFlushed`.
  Only that notification advances the leader's own matching log
  (`update_local_progress`); one from an older leadership is ignored, and a
  failed flush stops the core like any storage error. Follower appends still
  block, so their AppendEntries responses go on implying durability.
- `src/core/notify.rs`: `Notify::LocalLogFlushed { vote, log_id, result }`.

Flower's log store (`crates/flower-cluster/src/logstore.rs`, policy in
`crates/flower-cluster/src/flush.rs`) commits such an append to redb without
an fsync and completes the callback once a durable commit covers it: at once
(in parallel with replication) in groups of fewer than three voters per
configuration, and while the followers alone form a majority of every
configuration, only when the group has not committed the append within a
grace (30 ms) or at most every 500 ms (v1's lazy flush). Tests, in
`flower-cluster/tests/durability.rs`: with the leader's Raft disk holding
every sync, a write still commits on the followers
(`a_leader_replicates_before_its_own_flush_completes`; with upstream's core it
times out); a lazy leader flushes at the interval while its followers commit,
and once a follower is down flushes after the grace, then at once
(`a_lazy_leader_flushes_only_when_its_vote_is_needed`); and with a follower
down and the leader holding its flush, a write does not commit on the other
follower alone (`the_leader_counts_itself_only_for_appends_it_flushed`).

## 7. A follower's lease runs from its flush

A follower's `RaftCore` awaits its own log flush inside `append_to_log`, and
the leader sends it nothing more (heartbeats included) until it answers. Upstream
counts the follower's leader lease and election timeout from the append's
arrival, so a flush longer than the lease plus an election timeout ended in a
tick that found the leader silent. When every follower's disk stalls at once
(every replica of a busy host flushing together), each asks for a pre-vote, the
others, as slow, grant it, and a healthy leader is deposed, then its successor.

- `src/core/raft_core.rs`: once `append_to_log` returns for a follower, under
  the committed vote of another member that is still this member's vote, the
  vote's time is touched: the lease and the election timer run from the flush.

Voters refuse candidates for longer, never shorter, so the read lease, which
counts from before the append was sent, holds. A leader that really dies
during a follower's flush is detected that flush later. Test, with an RPC
deadline past the stall so the leader waits rather than resends: with one
follower cut off from the leader, the other, flushing for 600 ms (past the
300 ms lease and the longest 200 ms election timeout), keeps the leader it
forms a quorum with through six seconds of writes
(`flower-cluster/tests/failover.rs`,
`a_slow_follower_keeps_its_leader_while_the_other_is_cut_off`); counted from
the arrival, its queued ticks find the leader silent as each flush ends, the
cut-off follower grants its pre-vote, and it is elected.

## Not ported from v1's fork

- **Streaming snapshot sender** (read to the end rather than seek): Flower's
  snapshot images are complete files before they are sent.
- **Lease reads in the core** and `RaftNetwork::append_sent_at`: Flower's
  leases live in `read.rs`, confirmed by quorum rounds of their own.
- **Grouped client writes in the core**: Flower's writer batches writes before
  Raft.

## Not changed: heartbeats beside data

Heartbeats still go through each follower's replication stream, so one queues
behind a data append in flight to that follower. With pre-vote that only
quiets one follower: a voter grants nothing while it hears its leader, so
disruption needs a quorum to go without its leader's messages for a lease,
and with change 7 a follower's own flush does not count as going without.
What reaches every follower at once is a stall in the leader itself (its core
awaits its own log writes, which queue behind a flush in progress on redb's
write lock), and heartbeats sent around the core would hide a stuck leader
rather than help. A data append that takes longer than the lease
to reach every follower at once (a very large entry over a slow link) would
start an election; raise `FLOWER_RAFT_LEADER_LEASE_MS` on such links.

## Updating

Replace `src` and `Cargo.toml` with a newer published crate's, keep the
manifest's header comment, and reapply the changes marked `Flower:`
(`grep -rn "Flower:" src`), and carry over
`src/engine/tests/handle_pre_vote_req_test.rs`. Then run OpenRaft's unit
tests as above, `cargo test -p flower-cluster --test failover --test
membership --test reads --test conformance --test moves --test durability`
and `cargo test -p flower-client --test failover`.
