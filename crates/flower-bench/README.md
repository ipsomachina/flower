# flower-bench: Goblin Pizza, live and audited

`flower-bench` is the Goblin Pizza stress benchmark and live dashboard in one
Rust program. It starts real `cargo-flower serve` processes, hosts as many
Raft groups in them as asked with each tenant a named partition, deploys the
Goblin Pizza guest (with access rules and end-user tokens under `--auth`),
sends customers and delivery drones at every group, kills the busiest process
halfway if asked, drains, audits every acknowledged effect with fresh reads
(each group, then the groups against each other), and writes a JSON report and
a self-contained HTML report. It can capture the servers' OpenTelemetry and
sample a server's stacks, compare a run with a baseline (failing past a
regression threshold) and publish results. Any failed audit check makes it
exit nonzero. Throughput has no pass threshold.

## Run it

The fun one: three replicas, a leader kill one minute in, and the live
view opened in your browser.

```sh
cargo run --release -p flower-bench -- --preset demo
```

It builds `cargo-flower` (same profile) and the guest for `wasm32`, both
no-ops when up to date, then prints the live view's URL, each member's
`/admin` dashboard URL and the run's admin token, so Flower's own view
can sit beside it. The page shows:

- **the cluster:** three replica towers, a crown on the leader. When the
  dragon eats the leader its tower shakes and goes dark, the crown
  falls, and it lands on whichever replica wins the election; the banner
  says when the first write was acknowledged again, and the killed
  replica comes back as a follower. With several groups the crown is the
  first group's and each tower lists the groups its process leads;
- **the shop floor:** every tenant's stores with pizzas in the oven,
  boxes on the ready shelf, deliveries, coins, dough left and the
  leaderboard rank. This comes from Flower watches (one `DeltaStream` per
  tenant on `pizza.leaderboard`, asking for deltas), which reconnect to
  another member when theirs is killed; the page says which member each
  watch is on, its revision and how many updates and snapshots arrived;
- **the drone yard:** each drone and the order it carries, claims that
  are second attempts, and the crates the clumsy drone dropped, with
  their lease counting down until another drone picks them up;
- **charts:** goodput, p99 latency and errors and retries per second,
  with the outage marked from the kill to the first acknowledged write;
- **the audit:** the checks that tick green (or red) after the drain.

The run lasts two minutes; Ctrl-C ends the rush early and still drains
and audits. The page stays up with the final state until a second
Ctrl-C. `--no-open` skips the browser.

Other presets:

```sh
cargo run --release -p flower-bench -- --preset smoke    # 1 replica, 3 s: does it all work?
cargo run --release -p flower-bench -- --preset stress   # stress preset: 8 groups on 3 processes
cargo run --release -p flower-bench -- --help            # every flag
```

| Preset | Groups | Replicas | Load (per group) | Drones | Kitchens | Orders | Measured | Chaos | Live view |
|---|---|---|---|---|---|---|---|---|---|
| none (defaults) | 1 | 3 | 8 closed-loop customers | 2 | 2 tenants × 4 stores | 32 | 10 s | no | no |
| `smoke` | 1 | 1 | 4 closed-loop customers | 2 | 2 × 4 | 8 | 3 s | no | no |
| `demo` | 1 | 3 | 120 arrivals/s open loop, 64 slots | 6 | 3 × 2 | 5,000 | 120 s | at 60 s | opened |
| `stress` | 8, hosted | 3 | 256 closed-loop customers | 4 | 2 × 4 | 96 | 60 s | busiest process at 30 s | no |

Every flag overrides its preset: `--preset stress --duration 20`, `--preset
stress --groups 1`, `--preset demo --no-chaos`. Available flags include
(`--groups`, `--hosted`, `--auth`, `--concurrency`, `--offered-rate`,
`--workers`, `--tenants`, `--hot-stores`, `--hot-probability`, `--max-orders`,
`--bake-ms`, `--lease-ms`, `--duplicate-rate`, `--abandon-rate`, `--poll-ms`,
`--request-timeout-ms`, `--retry-budget-ms`, `--seed`, `--warmup`, `--drain`,
`--keep-data`, `--data`, `--json`, `--html`, `--baseline`, `--cpu-profile`).
`--replicas` sets the replica count and `--stores` the stores per tenant.
Customers, drones, tenants and the order cap are per group; stores are per
tenant.

`--target URL,URL,...` drives an existing cluster instead (with
`--admin-token` or `FLOWER_ADMIN_TOKEN`, and `FLOWER_WORKER_TOKEN` if
workers use their own); each run deploys into a fresh database
`pizza-<run id>`. Chaos, many groups and the diagnostics need servers
the bench started.

Reports go to `target/flower-bench/<preset>.json` and `.html` unless
`--json`/`--html` say otherwise.

### Processes, ports and cleanup

Each replica is its own `cargo-flower serve` process with its own data
directory under a fresh temporary directory (or `--data DIR`), and its
own HTTP and Raft ports. Ports are free ones from 20000-31999, below
every ephemeral range: the bench binds one, notes it and releases it,
and starts a cluster again on fresh ports if a replica loses that race,
so runs never collide with each other or with fixed ports such as the
dashboard demo's 7801-7803 and 7901-7903, and no outgoing connection
takes a killed member's port before it restarts on it. The live view
also takes any free port on 127.0.0.1 (`--live-port` fixes it). The
admin and worker tokens are random per run and reach the servers
through their environment, not their command line. The servers run with
`--timer-sweep-ms 50`, so oven bells ring within 50 ms of their time,
and otherwise with defaults.

The servers never outlive the bench: they run in their own process
group, so a terminal's Ctrl-C reaches only the bench, which stops them;
on Windows a job object, and on Linux the parent-death signal, ends them
even if the bench is killed outright. Data directories are removed at
the end unless `--keep-data`.

## Many groups

`--groups N` runs N Raft groups at once, each a complete kitchen: its own
tenants (group *g* has tenants `tenant-<g × tenants + i>`), customers, drones,
ledger and audit, all started from one synchronized window. How the groups map
to processes:

- **Hosted** (`--hosted`, the default above one group). The usual one or three
  `serve` processes each host their replica of every group. The groups share
  each process's runtime, admission and peer listener (each group keeps its
  own redb files and writer). Through the catalog the root group keeps, the
  bench creates groups `g1`..`gN` founded on every process, waits until every
  process hosts each one active, and spreads their leaders: group *k*'s
  leadership moves to process *k mod replicas*, so each starts out leading an
  equal share. Each tenant then becomes a named partition `pizza-<tenant>`
  placed on its group; calls name the tenant's database and any member routes
  them to the owner group's leader. Every tenant database has its own delivery
  queue, and the group's `--workers` drones are split over them. `--chaos`
  kills the process that leads the most groups: every group observes the kill
  at once; the groups it led elect and write again, the others measure how
  long their writes stalled behind the lost follower; once every group serves
  again the process restarts and each group records when its replica there has
  caught up. The report names the killed process and which groups it led.
- **Separate** (`--separate`): one independent cluster per group, each replica
  its own process, each group the root of its cluster holding its tenants in
  `pizza`. `--chaos` kills every group's leader halfway, each in its own
  cluster.

The run's goodput is every group's successful customer calls over the
one window, its percentiles every group's buckets merged; each group's
own goodput, latency, failover and audit are in the report too. The
audit is exact in every group, and across groups three more checks
hold: every group's own audit passed, each tenant's stores and orders
are in its own group's databases only (and no tenant is in two groups),
and every partition is still active on the group it was placed on at
epoch 1, read from the catalog after the drain: nothing moved under
the load.

```sh
cargo run --release -p flower-bench -- --groups 4 --chaos             # 4 groups on 3 processes
cargo run --release -p flower-bench -- --groups 4 --separate --chaos  # 4 clusters, 12 processes
```

## Authenticated mode (`--auth`)

By default any caller may act for any tenant. `--auth` (or `PIZZA_AUTH=1`)
deploys the authenticated kitchen instead: the same guest built with its
`auth` feature (into `target/wasm-guest-auth`), in which every method sits
behind an access rule decided by the module's authorization hook before the
call runs.

- Every method that names a tenant (`pizza.order`, `pizza.tip`,
  `pizza.deliver`, `pizza.shop`, `pizza.shop.local`,
  `pizza.leaderboard`) admits only callers whose token's `tenant` claim
  is that tenant; others, and anonymous callers, are refused
  `FORBIDDEN`.
- `pizza.setup` and `pizza.world`, which see every tenant, need
  `role: "admin"`. The administrator's token names no tenant, so it acts
  for none.
- A token that does not verify (another key, a rewritten claim, wrong
  issuer or audience, expired) is refused `UNAUTHENTICATED`.

Each run makes its own Ed25519 signing key from the OS's randomness and mints
one EdDSA token per tenant (`sub: customer@<tenant>`, `tenant`) and one
administrator token (`sub: goblin-admin`, `role: admin`), issuer and audience
`goblin-pizza`, valid two days. The hook verifies them natively (the host's
JOSE verifier, under an EdDSA allowlist) against the public key `pizza.setup`
installed. Only the deployer can install it: before a key exists the setup is
authorized as a trusted gateway, which over HTTP only the holder of the
server's admin token can be (it asserts the administrator principal), so only
the deployer decides whose tokens the kitchen accepts. The servers mount a
run-scoped key-wrapping key (`--key-wrapping-file`), which gives guests the
host crypto the hook needs. Calls carry their tokens in the request body's
`credentials`: customers their tenant's, drones the order's tenant's, setup
the gateway's assertion, audits the administrator's or each tenant's; live
watches present their tenant's bearer. Tokens never appear in argv, reports or
logs.

Before the load each group probes the rules, and the audit's `access`
check requires every answer: another tenant's store, an anonymous read,
a token under a foreign key, a tenant reading the world, the
administrator reading a tenant's store and a tenant ordering for
another are each refused with the right code; a tenant's own read and
the administrator's world read are served. Replica-local reads stay
local: the hook decides on the snapshot the read serves.

```sh
cargo run --release -p flower-bench -- --preset smoke --auth
PIZZA_AUTH=1 cargo run --release -p flower-bench -- --groups 2
```

## The workload

The workload models tenants, stores, customers and delivery drones:

- **Kitchens.** `--tenants` tenants per group with `--stores` stores
  each, every store stocked with four pizzas per admitted order plus
  ten, so none runs out. Store choices pick the hot set (`--hot-stores`,
  default one per tenant) with probability `--hot-probability` (0.8),
  else any store: the hot store takes about 85% of its tenant's calls.
- **The mix.** Each customer step draws, in order with a seeded generator
  (seeded `<seed>:group-<g>` per group with several groups), a choice, the
  store set, a tenant and a store: 30% order one to four pizzas until
  `--max-orders` orders were issued in the group, 40% read the store, 30% tip
  it one to three coins. After the cap, order choices read, so a long
  closed-loop run settles near 70% reads.
- **Closed or open loop.** By default `--concurrency` customers per
  group each wait for their answer. `--offered-rate N` schedules N
  arrivals per second per group on an independent clock with at most
  `--concurrency` in flight; an arrival that finds every slot busy is a
  *driver drop*, counted and never queued or delayed, and its latency is
  never fabricated. Open-loop latency runs from the intended arrival, so
  scheduling lag is in it.
- **Reads.** `--read-consistency local` (default) reads
  `pizza.shop.local`, the serving replica's own applied state; `fresh`
  reads `pizza.shop`, proved fresh on whichever replica serves it. Reads
  rotate over every member over pooled h2c, skipping a member for a
  moment after it fails. The setup is made visible on every replica with
  one fresh read each before any local read. Audits always read fresh.
- **Writes and retries.** Orders and tips go to the leader each reply
  names (members forward otherwise; with several groups, the leader of
  the group owning the tenant's partition). Each call's body is
  serialized once and every attempt sends those bytes, request ID
  included: transport failures, timeouts and 429/502/503/504 retry with
  backoff for up to `--retry-budget-ms`, each attempt bounded by
  `--request-timeout-ms`. A call whose budget runs out is *uncertain*:
  the drain sends it again under the same request ID until the server
  decides it, so the audit knows exactly what applied. A
  `--duplicate-rate` share of acknowledged mutations is sent again at
  once to check its receipt.
- **Drones.** `--workers` delivery drones per group are `flower-worker` queue
  workers on the `deliveries` queue the ovens fill, one worker per tenant
  scope (with tenants as partitions the workers are split over their
  databases, then their scopes): each claims under a lease the worker renews,
  flies 5 to 30 ms, calls `pizza.deliver` with its lease and uncertain retries
  (the guest renews that lease in the paying transaction, so a lapsed drone
  cannot collect coins) and completes the claim, claiming its scope's next
  deliveries in the same commit. A worker whose scope runs dry keeps its place
  in the scope's line and reads readiness instead of claiming until work
  arrives; stopping, it gives deliveries in the air ten seconds before failing
  them back. A clumsy drone claims items itself, from each tenant's scope in
  turn, under the short `--lease-ms` lease and abandons first claims,
  `--abandon-rate` of acknowledged orders (at least one); each lease lapses, a
  drone reclaims the item (its second claim) and delivers it. At the audit
  every abandoned token is presented again to complete and to renew the claim,
  and both must be refused.

## Chaos and failover

`--chaos` (three replicas) kills a server process outright halfway
through the measured window: the leader with one group, the process
leading the most groups when every process hosts many, every group's
leader with separate clusters. Each group records:

- **election:** for a group the killed process led, from the kill until
  a surviving member reports itself that group's leader on
  `/v1/status?group=` (polled every 25 ms);
- **failover:** from the kill until the first write acknowledged by a
  surviving member *in an attempt sent after the kill*, whoever sent it
  (orders, tips, deliveries). Writes in flight at the kill do not count,
  so a reply that raced the kill cannot shorten it;
- **the burst:** the group's failed attempts by kind, retries and failed
  calls from the kill until one second after its first acknowledgement;
- **recovery:** once every group writes again, the killed process
  restarts from its data directory on its ports; each group records when
  its replica there follows the group's leader and has applied what the
  leader had applied at the restart.

Every chart, live and in the report, marks the kill and shades the
outage until the first acknowledged write.

## The audit

After the drain (every acknowledged order delivered, the queues and the
ovens empty), each group's audit reads `pizza.world` page by page in
every database it serves, each tenant's `pizza.leaderboard`, the
queues' and the timers' counts, and every replica's own copy of every
store, all fresh. Derived values never serve as the oracle: stock,
revenue and the store counters are rebuilt from the source order rows
and the acknowledged tips. Every check must pass:

| Check | Verifies |
|---|---|
| The kitchens took orders | At least one order was acknowledged |
| Every acknowledged order is stored exactly once | No acknowledged order is missing or stored twice |
| No order exists that was never acknowledged | Every stored order was acknowledged (to a customer, or when the drain decided an uncertain call); no order ID appears twice |
| Orders kept their store and quantity | Each order is as it was acknowledged |
| Every order baked and was delivered | Each is delivered; due time is order time plus bake time, ready after due, delivered after ready |
| Stock is conserved | Each store's stock is its starting stock less every ordered pizza |
| Revenue is seven coins per delivered pizza | Each store earned exactly 7 coins per delivered pizza: a double charge or a lost payment fails it |
| Tips match the acknowledged tips | Each store holds exactly the tips customers were told succeeded |
| Store counters match their source orders | Each store's order, baking, ready and delivered counters match a recount of its orders |
| Leaderboards match the source | Each tenant's fresh leaderboard is its stores ranked by revenue plus tips, ties by key |
| Replays return the original receipt | Every replay under an acknowledged request ID (during the run, and up to 100 orders plus kept tips again at the audit, after any failover) returned the original answer: the same value, revision and log position |
| Expired leases are fenced | Every abandoned claim's token was refused to complete and to renew |
| No delivery or oven left behind | The drain finished; no queue item pending, claimed or dead; no oven timer pending or dead; no uncertain call undecided |
| Every replica converged on the same state | All replicas applied the same log index of the group, and each one's replica-local read of every store equals the fresh row |
| Every customer call succeeded | No customer call failed (retries must carry every call through a failover) |
| Access rules held (`--auth`) | Every access probe got its expected answer: refusals with the right code, own calls served |
| Every group's books balance (several groups) | Each group's own audit passed |
| Each tenant stayed in its own group (several groups) | Each group's databases hold only its own tenants' stores and orders, every tenant's are there, and no tenant is assigned to two groups |
| Every partition stayed where it was placed (hosted) | The catalog lists each tenant's partition active on its group at epoch 1 after the drain |

The audit is a pure function of its input; its tests feed it a settled
kitchen and then a lost order, a double charge, a replay mismatch and
other drift (a stray tenant, a moved partition, a served access probe),
and require the matching check to fail each time.

## Accounting

- **Goodput** counts successful primary customer logical calls (store reads,
  orders, tips) per second of the measured window, every group's together. It
  excludes drone traffic, replay probes, setup, warmup, drain and audit calls,
  and every failed call; a call that succeeded after retries counts once. The
  window runs from the synchronized start until the last customer call in
  flight at the deadline answered, so final tails are inside it.
- **Latency** is per logical call, from its start (open loop: its intended
  arrival) to its answer, retries and leader discovery included. Histograms
  use logarithmic buckets (0, then 0.001 ms growing by 1% to 24 hours, then
  infinity), so a percentile is at most 1% above the truth; percentiles are
  nearest-rank bucket upper bounds capped at the maximum. Histograms merge by
  adding buckets, never by averaging percentiles: each group records into its
  own accounting and the run's at once. Reports give p50, p90, p99, p99.9 and
  the maximum for every operation, and keep the buckets.
- **Server stages.** Every answered attempt's `Server-Timing` header
  says where its time went on the member that answered: `forward` (a
  follower's round trip to the leader), `fence`, `admission`, `queue`
  (the leader's writer queue), `eval`, `commit` and `total`. Each stage
  is a histogram per operation, unsampled and merged by bucket like the
  rest, shown under the latency table.
- **Attempts and errors.** Every attempt is counted, with its error kind
  when it failed (`timeout`, `connect`, `transport`,
  `http-503:<code>`, `http-403:unauthorized`, `business:<code>`, ...).
  The timeline shows failed attempts and retries per second.
- **Open loop.** Offered = dispatched + driver drops; dispatched =
  completed + failed; scheduling lag is a histogram; per group and
  summed.
- **Processes.** Every server process's and the bench's CPU time and resident
  memory are sampled each second (`GetProcessTimes` and the working set on
  Windows, `/proc` on Linux, `ps` elsewhere). CPU counts only pairs of samples
  of one process both inside the measured window, and the report says how many
  seconds that covers; a killed member's restart is a new process. A hosted
  process's CPU serves every group it hosts. The scorecard and `compare` turn
  it into CPU per successful customer call (below).
- **Metadata.** Each report records the SHA-256 of the server, the guest
  and the bench, every setting and the seed, the machine (OS, CPU,
  logical CPUs, build profile) and the workspace commit.

## Profiling

Both profilers are diagnostics: they perturb the run, its report says so,
`compare` never calls it matched work and `publish` refuses it. A requested
profile that fails fails the run, independently of the audit.

### OpenTelemetry (`--otel`)

The bench starts a bounded OTLP/HTTP JSON receiver on 127.0.0.1 (no collector
service; nothing leaves the machine) and every server exports to it
(`FLOWER_OTEL_ENABLED=1`, `http/json`, parent-based sampling at
`--otel-sample`, 1% by default, one-second metric exports and span batches);
inherited exporter endpoints, headers and resource attributes are removed from
the servers' environment for the run. Records are sanitized as they arrive:
resource identity (instance, node ID, PID), span IDs and times and only the
attributes Flower's spans carry (route, method, status, outcome, node and
leader), and the request-duration histogram and in-flight gauge points.
Bodies, headers, events and every other attribute are dropped. At most
`FLOWER_BENCH_OTEL_MAX_RECORDS` records (100,000; up to 1,000,000) and
`FLOWER_BENCH_OTEL_MAX_BYTES` (256 MiB; up to 1 GiB) are kept, requests are
limited to 4 MiB and eight are read at once.

The report's OpenTelemetry section (and `<json stem>-otel/summary.json`
with the sanitized `capture.ndjson`) gives, per route, the servers'
request count, rate and duration percentiles from the metric
(cumulative points differenced between adjacent exports of the same
process inside the window, resets skipped, merged across nodes by
bucket; percentiles are bucket upper bounds) beside the sampled spans'
durations (spans wholly inside the window), errors, the slowest sampled
spans with their trace IDs, spans joined across members by forwarding,
the peak in-flight count and every process that exported (a restarted
member is a new one). Missing spans or metrics, no metric interval
inside the window, malformed requests or exceeded bounds fail the run.
Exporter queue loss and what a killed member had buffered are invisible
to the receiver and reported as unknown.

```sh
cargo run --release -p flower-bench -- --preset stress --duration 20 --otel --json /tmp/otel.json
```

### CPU profile (`--cpu-profile PATH`)

From the start of the measured window, for `--cpu-profile-seconds` (default
10, at most 60) every `--cpu-profile-interval-ms` (default 1), the bench
samples the stacks of the process leading the most groups, and keeps that
process however leadership moves.

- **Windows:** the bench samples the process itself, as `sample` does: each
  round it suspends each of the server's threads in turn, reads its context,
  walks its stack with DbgHelp's `StackWalk64` (through the images' unwind
  tables, x64 and ARM64) and resumes it. Each thread's cycle counter
  (`QueryThreadCycleTime`) says whether it ran since its previous sample, so
  stacks are classified on or off CPU by the scheduler's own counter. No
  administrator rights are needed: the bench started the process. Names
  resolve through the server's PDB and are demangled. A release server without
  debug information resolves only to module offsets: build it with
  `CARGO_PROFILE_RELEASE_DEBUG=line-tables-only`. Frames inside JIT-compiled
  guest code stay addresses.
- **Linux:** `perf record -g` on the PID at the requested rate, then
  `perf script`: on-CPU samples (needs `perf` and a
  `perf_event_paranoid` that allows it).
- **macOS:** `/usr/bin/sample`: all-thread wall stacks, classified waiting or
  active by their blocking frames.

The raw stacks go to `PATH` as folded stacks (`thread;outermost;...;
innermost count`), which flame-graph tools read. The report summarizes
them: active and waiting observations, active work by subsystem (each
stack assigned to its innermost recognized subsystem: JSON, hashing,
compression, cryptography, redb, OpenRaft, the Wasm runtime, HTTP,
allocation, Flower's engine, cluster, server and backup, Tokio, the
standard library, the OS), the hottest inclusive and self frames, where
waiting threads waited, threads grouped by name, and how many active
stacks resolved to no name. Counts are stack observations, not CPU
time.

```sh
CARGO_PROFILE_RELEASE_DEBUG=line-tables-only cargo build --release -p cargo-flower
cargo run --release -p flower-bench -- --preset stress --duration 30 \
  --cpu-profile /tmp/flower.folded --json /tmp/flower-cpu.json
```

## Comparing runs

`--baseline PATH` puts a before-and-after panel in a run's report, and
`compare` compares any two reports:

```sh
cargo run --release -p flower-bench -- compare BASELINE.json CANDIDATE.json
cargo run --release -p flower-bench -- compare --threshold 5 --json BASELINE.json CANDIDATE.json
```

It reads each report's goodput, read and write p50 and p99, the slowest
failover, and CPU per successful customer call, estimated from sampled CPU:
each sampled process's CPU counts only between consecutive samples of the same
process inside the window, its mean cores over that covered time is
extrapolated across the whole window, the servers' (or the bench's) estimates
are summed and divided by successful primary customer calls. Coverage stays
visible, and a server with no samples makes the estimate unavailable rather
than zero. Server CPU includes replication, retries, drones and background
work, which goodput excludes.

The comparison calls itself **matched fixed work** only under these criteria:
the same workload settings, machine, guest and bench binaries; no diagnostics
in either run; both passed with no failed customer call; open loop at the same
`--offered-rate` with every arrival served and the same number of successful
calls. Otherwise it is exploratory and lists why. A different server binary is
the point of a comparison, never a reason. `compare` fails (exit 1) when the
candidate failed its audit or a gated measure regressed beyond `--threshold`
percent (default 10): goodput down, read or write p99 up, or server CPU per
call up. Exit 2 means a report could not be read.

For a CPU comparison, pick an offered rate both builds serve without
driver drops or failed calls and alternate the builds over repeated
runs:

```sh
cargo run --release -p flower-bench -- --preset stress --offered-rate 3000 --duration 40 \
  --server-binary /path/to/before --json /tmp/before.json
cargo run --release -p flower-bench -- --preset stress --offered-rate 3000 --duration 40 \
  --server-binary /path/to/after --json /tmp/after.json
cargo run --release -p flower-bench -- compare /tmp/before.json /tmp/after.json
```

## Results and publication

`publish` keeps measured runs: it validates each report, keeps it under this
crate's `results/` as `<preset>.json` (`<preset>-auth.json` with `--auth`;
per-second process samples dropped, every summary kept) with its HTML page,
and rewrites the table below from every retained report; `--name` keeps one
report under a name of its own instead (for a run whose flags changed its
preset), recorded in the retained JSON as `publishedAs`. Only measurements
publish: the run passed every audit (each group's and across groups), was not
interrupted, ran no diagnostic, and its goodput agrees with its own accounting
(reads plus orders and tips, the load phase's successful customer calls, the
rate over the window, the groups' goodput adding up to the run's). The
retained reports are the baselines `compare` reads. `publish --check`
revalidates them offline and fails when this table no longer matches them; it
never reruns the workload.

```sh
cargo run --release -p flower-bench -- --preset stress
cargo run --release -p flower-bench -- publish target/flower-bench/stress.json
cargo run --release -p flower-bench -- publish --check
cargo run --release -p flower-bench -- compare crates/flower-bench/results/stress.json target/flower-bench/stress.json
```

<!-- flower-bench results:start -->
| Run | Groups × replicas | Goodput (calls/s) | Writes p50 · p99 | Reads p99 | Server CPU / call | Failover | Audit | Measured |
|---|---|---:|---|---:|---:|---:|---|---|
| [stress-64](results/stress-64.html) ([JSON](results/stress-64.json)) | 8 × 3 (hosted) | 7,052.8 | 189 ms · 618 ms | 26.6 ms | 875 µs | 635 ms | passed | 2026-10-31, ARMv8 (64-bit) Family 8 Model 2 Revision 201, Qualcomm Technologies Inc |
<!-- flower-bench results:end -->

## Reading a report

The HTML report (no script, nothing loaded from anywhere; its style is
allowed by hash in its own Content-Security-Policy) leads with the
verdict and the headline figures, then the audit checklist with any
violations (every group's, then across groups), the comparison with a
baseline, the groups (tenants, databases, goodput, latency, failover,
audit, and the catalog after the run), the per-second charts with the
chaos band, latency by operation with each one's distribution on a
shared log axis, the chaos events per group, errors by kind, drones and
routing per group and member, processes, the OpenTelemetry and CPU
profile sections when they ran, and the binaries and settings. Every
chart has a table view.

The JSON (schema `flower-bench/2`) has `goodput`, `offeredLoad`,
`phases` (per phase and operation: calls, failures, attempts, retries,
errors by kind and a sparse histogram), `timeline`, `resources`,
`lifecycle` (oven lateness and order-to-door times from the source
rows), `replays`, `groups` (one entry per Raft group: its name,
tenants, own goodput and open-loop counters, cluster and databases,
`chaos` events, `audit`, `drones` and `routing`), `across` (the
cross-group audit), `kill` (the hosted process kill: which member, its
PID and the groups it led), `placements` (the catalog's partitions
after the run), `otel`, `cpuProfile` and `comparison`. Version 2 added
those and renamed a group's `db` to `databases`; `compare` reads both
versions.

## Taking a clean measurement

Numbers from a shared machine are observations, not capacity. For a
measurement worth comparing: build with `--release`; close builds,
tests and anything else busy; keep the seed and every setting; run each
configuration several times, alternating any binaries you compare; and
compare closed-loop goodput only alongside latency, or use the same
`--offered-rate` for both and compare latency, CPU per call and driver
drops (`compare` says whether the runs were matched fixed work). Every
replica, the drones and the customers share the machine's CPU and disk.

## Tests

`cargo test -p flower-bench` runs the unit tests (histogram, goodput, group
accounting, open-loop accounting, the audit with its injected faults per group
and across groups, routing and credentials, tokens checked by the server's own
JWT verifier, the OTLP receiver's sanitizing and bounds, metric differencing
and resets, stack summaries, perf and `sample` parsing, the Windows sampler on
a live process, `compare`'s CPU estimate, matched-work criteria and
thresholds, `publish`'s validation, retention and README check, reports, the
live page's posture) and five end-to-end runs against real server processes:
the smoke preset on one replica; three replicas with a leader kill that must
pass its audit and measure a failover; three processes hosting two groups with
a tenant partition each and the busiest process killed, every group failing
over and the partitions where they were placed; the authenticated kitchen with
every access probe answered right; and a diagnostic run whose OpenTelemetry
capture (and, on Windows, CPU profile) must be complete. `tests/faults.rs`
injects end-to-end faults into three real processes running the tasks guest:
every member killed at once, requests resent under their IDs, and then the new
leader; a frozen leader (`Cluster::freeze`, as `SIGSTOP`); a move whose
destination is frozen once part of the base reached it, aborted by its
operator; every member killed with half a base at the destination; a
transaction across groups with its participant frozen and its coordinator's
leader killed once prepared; a move held until its destination mounts the
right wrapping key; a graceful leader stop and restart preserving acknowledged
writes; an independent data group serving during a catalog quorum outage; and
TLS on the API and Raft ports. Each test fails unless it reaches the state it
names. On Windows, graceful process-group stops use Ctrl-Break; run that test
from a console/PTY shared with the server children. `cargo test -p
goblin-pizza --features auth` tests the access rules' helpers. Set
`FLOWER_BENCH_SERVER_BINARY` to test a prebuilt server.
