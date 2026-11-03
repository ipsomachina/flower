//! `flower-bench`: the Goblin Pizza stress benchmark and live demo.
//!
//! One Rust program starts real `cargo-flower serve` processes (or drives an
//! existing cluster), hosts many Raft groups in them with each tenant a named
//! partition, deploys the Goblin Pizza guest (with access rules and end-user
//! tokens under `--auth`), runs customers and delivery drones in every group,
//! kills the busiest process halfway when asked, drains, audits every
//! acknowledged effect with fresh reads (each group, then across groups), and
//! writes JSON and HTML reports. It can capture the servers' OpenTelemetry
//! and sample a server's stacks, compare a run with a baseline and publish
//! results. The run fails when any audit check fails. See the crate's README
//! for how to run it and how to read a report.
//!
//! Module map:
//!
//! - [`config`]: flags, presets and subcommands.
//! - [`artifacts`]: building and fingerprinting the server and guest.
//! - [`cluster`]: the server processes, their ports, tokens and chaos.
//! - [`groups`]: many groups per process and tenants as partitions.
//! - [`auth`]: `--auth`'s signing key and end-user tokens.
//! - [`client`]: routing, the retry loop and per-attempt accounting.
//! - [`kitchen`]: one group's tenants, databases and steps.
//! - [`workload`]: customers (closed and open loop) and the ledger.
//! - [`drones`]: the delivery fleet, the clumsy drone and fencing.
//! - [`chaos`]: the process kill and failover measurement.
//! - [`audit`]: the end-of-run business audit, per group and across.
//! - [`histogram`] and [`metrics`]: latency histograms and accounting.
//! - [`resources`]: process CPU and memory sampling.
//! - [`otel`]: the OpenTelemetry receiver and its report.
//! - [`profile`]: stack sampling of a server process.
//! - [`compare`]: a run against a baseline, with a threshold.
//! - [`publish`]: retained results and the README's table.
//! - [`report`]: the JSON report and its HTML rendering.
//! - [`live`]: the live view.
//! - [`run`]: one run, start to finish.

pub mod artifacts;
pub mod audit;
pub mod auth;
pub mod chaos;
pub mod client;
pub mod cluster;
pub mod compare;
pub mod config;
pub mod drones;
pub mod groups;
pub mod histogram;
pub mod kitchen;
pub mod live;
pub mod metrics;
pub mod otel;
pub mod profile;
pub mod publish;
pub mod report;
pub mod resources;
pub mod run;
pub mod workload;
