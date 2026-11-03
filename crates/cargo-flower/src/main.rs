//! `cargo-flower`: operator tooling for contracts, calls, membership,
//! backups and serving a node.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use clap::{Parser, Subcommand};

#[cfg(test)]
mod argument_input_tests {
    use super::*;
    #[test]
    fn call_accepts_file_or_stdin_and_refuses_conflicting_inline_args() {
        let base = [
            "cargo-flower",
            "call",
            "--db",
            "demo",
            "--module",
            "shop",
            "--name",
            "sql",
        ];
        for path in ["request.json", "-"] {
            let mut args = base.to_vec();
            args.extend(["--args-file", path]);
            assert!(Cli::try_parse_from(&args).is_ok());
            args.extend(["--args", "{}"]);
            assert!(Cli::try_parse_from(&args).is_err());
        }
    }
}
use flower_backup::driver::{ContinuousSettings, Driver, RuntimeBuilder};
use flower_client::client::{FlowerClient, MutationOptions, RequestOptions};
use flower_client::membership::{LeaderId, LogId, MembershipChange, MembershipView};
use flower_client::transaction::{TransactionOptions, TransactionStep};
use flower_client::transport::Credentials;
use flower_cluster::admission::AdmissionConfig;
use flower_cluster::flush::FlushSettings;
use flower_cluster::fsync::Durability;
use flower_cluster::group::GroupId;
use flower_cluster::host::{
    DutyStarter, GroupDuties, Host, HostSettings, NodeConfigurer, RuntimesFor, Transport,
};
use flower_cluster::network::PeerConfig;
use flower_cluster::node::Node;
use flower_cluster::read::ReadSettings;
use flower_cluster::snapshot_policy::SnapshotPolicy;
use flower_cluster::statemachine::RuntimeFactory;
use flower_cluster::timing::{RaftTiming, SnapshotSettings, TimingSettings, raft_config};
use flower_cluster::tls::PeerTls;
use flower_cluster::wasm::WasmFactory;
use flower_crypto::replicated::ManagedKeys;
use flower_engine::descriptors::{ModuleDef, load_module};
use flower_engine::json::parse_value;
use flower_engine::runtime::wasm;
use flower_engine::storage::compact::CompactionSettings;
use flower_server::api::{AppState, decode_identity_hex, parse_members};
use flower_server::groups::{HostApi, StateFactory, serve_host};
use flower_server::listener::{ApiTls, HttpOptions};

mod artifact;
mod codegen;
mod dev;

/// Flower operator tooling.
#[derive(Parser, Debug)]
#[command(name = "cargo-flower", about = "Operate a Flower cluster")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Watch local Rust sources, rebuild and deploy to a running development server.
    Dev(dev::Options),
    /// Explain a named SQL plan from an offline manifest (no database reads).
    SqlExplain {
        /// Manifest JSON, as produced by `cargo flower build`.
        #[arg(long)]
        manifest: PathBuf,
        /// Module-local plan name.
        #[arg(long)]
        plan: String,
    },
    /// Inspect or diff manifest contracts offline.
    Contract {
        #[command(subcommand)]
        command: ContractCommand,
    },
    /// Build a workspace guest for wasm32, inspect the Wasm Cargo
    /// reported through the server's backend, and write
    /// `<name>.wasm` plus `<name>.manifest.json` to the output folder.
    Build {
        /// Cargo.toml of the workspace or package.
        #[arg(long)]
        manifest_path: Option<PathBuf>,
        /// The guest package (a cdylib); needed when there are several.
        #[arg(short, long)]
        package: Option<String>,
        /// Cargo profile.
        #[arg(long, default_value = "release")]
        profile: String,
        /// Cargo features.
        #[arg(long, value_delimiter = ',')]
        features: Vec<String>,
        /// Disable default features.
        #[arg(long)]
        no_default_features: bool,
        /// Require Cargo.lock to be up to date.
        #[arg(long)]
        locked: bool,
        /// Output folder.
        #[arg(long, default_value = ".flower")]
        out_dir: PathBuf,
    },
    /// Preflight a deployment offline: inspect the Wasm, validate the
    /// manifest to deploy, and with `--current` diff it against the
    /// deployed contract; exits 1 on an incompatible change.
    Plan {
        /// Wasm module file.
        #[arg(long)]
        wasm: PathBuf,
        /// Manifest JSON to deploy; default the module's own.
        #[arg(long)]
        manifest: Option<PathBuf>,
        /// The deployed contract: manifest JSON or the deployed Wasm.
        #[arg(long)]
        current: Option<PathBuf>,
        #[command(flatten)]
        bind: BindArgs,
        /// The module name to deploy as, for the link check.
        #[arg(long, default_value = "module")]
        module: String,
        /// Another module deployed in the database, as MODULE=FILE (its
        /// manifest JSON, or its Wasm for the manifest it declares);
        /// repeatable. Given any, the plan checks every link between
        /// the module and them as the deployment will.
        #[arg(long = "with", value_name = "MODULE=FILE")]
        with: Vec<String>,
    },
    /// Deploy a module through consensus (admin), after the offline
    /// preflight `plan` runs: nothing is sent unless it passes.
    Deploy {
        /// Server base URL.
        #[arg(long, env = "FLOWER_ENDPOINT", default_value = DEFAULT_ENDPOINT)]
        endpoint: String,
        /// Database name.
        #[arg(long)]
        db: String,
        /// Module name.
        #[arg(long)]
        module: String,
        /// Wasm bundle file.
        #[arg(long)]
        wasm: PathBuf,
        /// Manifest JSON file; default the manifest the module declares.
        #[arg(long)]
        manifest: Option<PathBuf>,
        /// Identity override as 64 hex chars.
        #[arg(long)]
        identity: Option<String>,
        #[command(flatten)]
        bind: BindArgs,
        /// Admin bearer token.
        #[arg(long, env = "FLOWER_ADMIN_TOKEN")]
        admin_token: Option<String>,
    },
    /// Call a query or mutation.
    Call {
        /// Server base URL.
        #[arg(long, env = "FLOWER_ENDPOINT", default_value = DEFAULT_ENDPOINT)]
        endpoint: String,
        /// Database name.
        #[arg(long)]
        db: String,
        /// Module name.
        #[arg(long)]
        module: String,
        /// Definition name.
        #[arg(long)]
        name: String,
        /// Arguments as a JSON object.
        #[arg(long, default_value = "{}")]
        args: String,
        /// Read argument JSON from a file, or '-' for standard input.
        #[arg(long, conflicts_with = "args")]
        args_file: Option<PathBuf>,
        /// Validate the public alias, operation kind and argument shape locally.
        #[arg(long)]
        contract: Option<PathBuf>,
        /// Run a mutation instead of a query.
        #[arg(long)]
        mutation: bool,
        /// The mutation's request ID: retrying with the same one answers
        /// the original outcome (`duplicate=true`) instead of running it
        /// again. A scoped ID from `retry id` or `retry session-id` is
        /// refused rather than run again once its outcome is gone.
        #[arg(long, requires = "mutation")]
        request_id: Option<String>,
        /// Bearer token: an end user's, for the module's authorization
        /// hook, or the admin token, for a trusted gateway.
        #[arg(long, env = "FLOWER_TOKEN")]
        token: Option<String>,
    },
    /// Report node status.
    Status {
        /// Server base URL.
        #[arg(long, env = "FLOWER_ENDPOINT", default_value = DEFAULT_ENDPOINT)]
        endpoint: String,
    },
    /// Inspect or change cluster membership (admin). Without
    /// `--members`, print the addressed member's view: leader, voter
    /// sets, member addresses and the membership log ID. With them, send
    /// the leader the complete desired voter set; it checks each
    /// member's identity and compatibility, refuses a new member that
    /// holds Raft state, waits for new members to catch up and commits
    /// through joint consensus. A refused or uncertain change exits
    /// nonzero: inspect before retrying.
    Membership {
        /// Server base URL.
        #[arg(long, env = "FLOWER_ENDPOINT", default_value = DEFAULT_ENDPOINT)]
        endpoint: String,
        /// The desired voters as `id` (in-process) or `id=addr` (the
        /// member's Raft address), repeatable.
        #[arg(long)]
        members: Vec<String>,
        /// Refuse the change unless membership is still at this log ID
        /// (`term-node-index`, as printed without `--members`).
        #[arg(long)]
        expected_log_id: Option<String>,
        /// Retain removed members as learners.
        #[arg(long)]
        retain: bool,
        /// Admin bearer token.
        #[arg(long, env = "FLOWER_ADMIN_TOKEN")]
        admin_token: Option<String>,
    },
    /// Hand leadership to another voter (admin), at the leader. The
    /// leader stops proposing, waits until the voter holds its whole
    /// log and has it campaign; writes meanwhile answer a retryable
    /// 503. With four or more voters this waits out the other voters'
    /// leases, one election timeout. Exits nonzero if the transfer did
    /// not happen.
    Leadership {
        /// The leader's base URL.
        #[arg(long, env = "FLOWER_ENDPOINT", default_value = DEFAULT_ENDPOINT)]
        endpoint: String,
        /// The voter to lead.
        #[arg(long)]
        to: u64,
        /// Admin bearer token.
        #[arg(long, env = "FLOWER_ADMIN_TOKEN")]
        admin_token: Option<String>,
    },
    /// Take a full backup through a node (admin), or list and restore
    /// backups offline.
    Backup(BackupArgs),
    /// Operate durable work queues.
    Queue {
        /// Server base URL.
        #[arg(long, env = "FLOWER_ENDPOINT", default_value = DEFAULT_ENDPOINT)]
        endpoint: String,
        /// Database name.
        #[arg(long)]
        db: String,
        /// Worker (or admin) bearer token for the queue protocol.
        #[arg(long, env = "FLOWER_WORKER_TOKEN")]
        token: Option<String>,
        #[command(subcommand)]
        op: QueueCommand,
    },
    /// Inspect and maintain durable timers.
    Timer {
        /// Server base URL.
        #[arg(long, env = "FLOWER_ENDPOINT", default_value = DEFAULT_ENDPOINT)]
        endpoint: String,
        /// Database name.
        #[arg(long)]
        db: String,
        /// Admin bearer token: timer maintenance is an operator surface.
        #[arg(long, env = "FLOWER_ADMIN_TOKEN")]
        admin_token: Option<String>,
        #[command(subcommand)]
        op: TimerCommand,
    },
    /// Run a multi-database transaction atomically.
    Transaction {
        /// Server base URL.
        #[arg(long, env = "FLOWER_ENDPOINT", default_value = DEFAULT_ENDPOINT)]
        endpoint: String,
        /// Bearer token: an end user's, for the module's authorization
        /// hook, or the admin token, for a trusted gateway.
        #[arg(long, env = "FLOWER_TOKEN")]
        token: Option<String>,
        /// Steps as a JSON array of {db, module, method, args}.
        #[arg(long)]
        steps: String,
        /// Stable request identity.
        #[arg(long)]
        request_id: Option<String>,
    },
    /// List each database's active modules (admin): code identity,
    /// definitions, collections, HTTP aliases and any staging left
    /// open, after the member proves a fresh read, with the modules whose
    /// data is being purged and those removed whose data stays. `remove`
    /// removes a module through consensus.
    Modules {
        /// Server base URL.
        #[arg(long, env = "FLOWER_ENDPOINT", default_value = DEFAULT_ENDPOINT)]
        endpoint: String,
        /// Only this database (required to remove).
        #[arg(long)]
        db: Option<String>,
        /// Admin bearer token.
        #[arg(long, env = "FLOWER_ADMIN_TOKEN")]
        admin_token: Option<String>,
        #[command(subcommand)]
        op: Option<ModulesCommand>,
    },
    /// Drive a staged deployment step by step at the leader (admin):
    /// stage a module without activating it, build its backfill and
    /// rebuild its derived graph in bounded pages, inspect it, then
    /// activate or cancel it. A staging replicates: a new leader
    /// continues it, and a one-call `deploy` of the module replaces it.
    Deployment {
        /// Server base URL.
        #[arg(long, env = "FLOWER_ENDPOINT", default_value = DEFAULT_ENDPOINT)]
        endpoint: String,
        /// Database name.
        #[arg(long)]
        db: String,
        /// Module name.
        #[arg(long)]
        module: String,
        /// Admin bearer token.
        #[arg(long, env = "FLOWER_ADMIN_TOKEN")]
        admin_token: Option<String>,
        #[command(subcommand)]
        op: DeploymentCommand,
    },
    /// Inspect or change a database's replicated managed-key catalog
    /// (admin): every database keeps its own. Without a subcommand,
    /// list its keys, versions and guest bindings (every database's,
    /// without `--db`). Changes name their database and run at the
    /// leader of its group under a request ID the database scopes: pass
    /// `--request-id` to retry one safely (the same change answers as a
    /// duplicate); without it a fresh ID is made and printed.
    Keys {
        /// Server base URL.
        #[arg(long, env = "FLOWER_ENDPOINT", default_value = DEFAULT_ENDPOINT)]
        endpoint: String,
        /// Database name; changes need one. Before or after the
        /// subcommand.
        #[arg(long, global = true)]
        db: Option<String>,
        /// Admin bearer token.
        #[arg(long, env = "FLOWER_ADMIN_TOKEN")]
        admin_token: Option<String>,
        /// Request ID of a change. Before or after the subcommand.
        #[arg(long, global = true)]
        request_id: Option<String>,
        #[command(subcommand)]
        op: Option<KeysCommand>,
    },
    /// Read or change one database's retry retention (admin). Without
    /// a subcommand, print its epochs and rotation, the leader's
    /// maintenance counters and when the member's unscoped receipts
    /// expire, as JSON.
    Retention {
        /// Server base URL.
        #[arg(long, env = "FLOWER_ENDPOINT", default_value = DEFAULT_ENDPOINT)]
        endpoint: String,
        /// Database name.
        #[arg(long)]
        db: String,
        /// Admin bearer token.
        #[arg(long, env = "FLOWER_ADMIN_TOKEN")]
        admin_token: Option<String>,
        #[command(subcommand)]
        op: Option<RetentionOp>,
    },
    /// Mint scoped request IDs and run retry sessions in one database.
    /// Without a subcommand, print the identity new scoped request IDs
    /// take (history incarnation, epoch, floor) as JSON. Send a minted
    /// ID with `call --mutation --request-id`.
    Retry {
        /// Server base URL.
        #[arg(long, env = "FLOWER_ENDPOINT", default_value = DEFAULT_ENDPOINT)]
        endpoint: String,
        /// Database name.
        #[arg(long)]
        db: String,
        /// Bearer token: the session owner's, for the module's
        /// authorization hook, or the admin token, for a trusted gateway.
        #[arg(long, env = "FLOWER_TOKEN")]
        token: Option<String>,
        #[command(subcommand)]
        op: Option<RetryOp>,
    },
    /// List, create or remove groups in the partition catalog (admin).
    /// Without a subcommand, list every group with whether the addressed
    /// process hosts it and who it knows leads. Changes name an
    /// operation (`--op`): repeating one answers its outcome again.
    Group {
        /// Server base URL.
        #[arg(long, env = "FLOWER_ENDPOINT", default_value = DEFAULT_ENDPOINT)]
        endpoint: String,
        /// Admin bearer token.
        #[arg(long, env = "FLOWER_ADMIN_TOKEN")]
        admin_token: Option<String>,
        #[command(subcommand)]
        op: Option<GroupOp>,
    },
    /// List, place, retire or move named partitions (admin). Without a
    /// subcommand, list every partition with its owner group, ownership
    /// epoch and move in flight, then the last finished moves.
    Partition {
        /// Server base URL.
        #[arg(long, env = "FLOWER_ENDPOINT", default_value = DEFAULT_ENDPOINT)]
        endpoint: String,
        /// Admin bearer token.
        #[arg(long, env = "FLOWER_ADMIN_TOKEN")]
        admin_token: Option<String>,
        #[command(subcommand)]
        op: Option<PartitionOp>,
    },
    /// List or abort cross-group transactions (admin). Without a
    /// subcommand, print every group's coordinator and prepared records
    /// as JSON, in flight first.
    Transactions {
        /// Server base URL.
        #[arg(long, env = "FLOWER_ENDPOINT", default_value = DEFAULT_ENDPOINT)]
        endpoint: String,
        /// Admin bearer token.
        #[arg(long, env = "FLOWER_ADMIN_TOKEN")]
        admin_token: Option<String>,
        #[command(subcommand)]
        op: Option<TransactionsOp>,
    },
    /// Print the addressed member's OpenRaft metrics as JSON (admin).
    Raft {
        /// Server base URL.
        #[arg(long, env = "FLOWER_ENDPOINT", default_value = DEFAULT_ENDPOINT)]
        endpoint: String,
        /// Admin bearer token.
        #[arg(long, env = "FLOWER_ADMIN_TOKEN")]
        admin_token: Option<String>,
    },
    /// Print the addressed member's node metrics as JSON (admin):
    /// admission, reads, compaction, log purge, snapshots, backups.
    Metrics {
        /// Server base URL.
        #[arg(long, env = "FLOWER_ENDPOINT", default_value = DEFAULT_ENDPOINT)]
        endpoint: String,
        /// Admin bearer token.
        #[arg(long, env = "FLOWER_ADMIN_TOKEN")]
        admin_token: Option<String>,
    },
    /// Print the addressed member's write and watch activity snapshot
    /// as JSON (admin): the applied writes it recorded and its live
    /// watches.
    Activity {
        /// Server base URL.
        #[arg(long, env = "FLOWER_ENDPOINT", default_value = DEFAULT_ENDPOINT)]
        endpoint: String,
        /// Admin bearer token.
        #[arg(long, env = "FLOWER_ADMIN_TOKEN")]
        admin_token: Option<String>,
    },
    /// Serve a cluster node: Raft plus the HTTP API. OpenTelemetry
    /// export turns on with FLOWER_OTEL_ENABLED=1 and the standard
    /// OTEL_* variables (endpoint, protocol http/protobuf or
    /// http/json, service name, sampler).
    Serve(Box<ServeArgs>),
}

/// One staged-deployment step.
#[derive(Subcommand, Debug)]
enum DeploymentCommand {
    /// Validate and stage a module without activating it.
    Stage {
        /// Wasm bundle file.
        #[arg(long)]
        wasm: PathBuf,
        /// Manifest JSON file.
        #[arg(long)]
        manifest: PathBuf,
        /// Identity override as 64 hex chars.
        #[arg(long)]
        identity: Option<String>,
        #[command(flatten)]
        bind: BindArgs,
    },
    /// Build more of the staging's backfill, then of its graph.
    Advance {
        /// Most records (or graph roots) to build in this step (server
        /// default 4,096).
        #[arg(long)]
        max_records: Option<usize>,
    },
    /// Print the staging.
    Status,
    /// Activate the ready staging through consensus.
    Activate,
    /// Abandon the staging.
    Cancel,
    /// Delete structures the module's deployments retired, on every
    /// member alike (the leader's maintenance also collects them).
    Collect {
        /// Most keys to delete in this step (server default 4,096).
        #[arg(long)]
        max_keys: Option<usize>,
    },
}

/// One operation on a database's modules.
#[derive(Subcommand, Debug)]
enum ModulesCommand {
    /// List the modules (the default).
    List,
    /// Remove a module through consensus: its code, aliases, hooks and
    /// derived values go on every member and synced device, and its data
    /// stays with its declarations, so deploying it again rebuilds
    /// nothing that did not change. Refused while another module imports
    /// from it.
    Remove {
        /// Module name.
        #[arg(long)]
        module: String,
        /// Purge its data too, or what an earlier removal of it kept: the
        /// group's leader deletes it a page at a time, and the module
        /// cannot deploy until `modules` no longer lists it purging.
        #[arg(long)]
        purge: bool,
    },
}

/// One managed-key catalog request.
#[derive(Subcommand, Debug)]
enum KeysCommand {
    /// List the catalog (the default).
    List,
    /// Seal key material offline. Prints an envelope for `import --sealed-file`.
    Seal {
        #[arg(long)]
        key_file: PathBuf,
        #[arg(long, default_value = "raw", value_parser = ["raw", "pem", "der"])]
        format: String,
        #[arg(long)]
        wrapping_file: PathBuf,
        /// Write the envelope to a new file instead of stdout.
        #[arg(long)]
        out: Option<PathBuf>,
    },
    /// Generate a key.
    Generate {
        /// Key name.
        #[arg(long)]
        name: String,
        /// Ed25519, P256, RSA, HS256, A256GCM, XSalsa20Poly1305 or
        /// X25519.
        #[arg(long)]
        algorithm: String,
        /// RSA modulus size.
        #[arg(long)]
        bits: Option<u64>,
    },
    /// Import key material, sealed here to the cluster's wrapping key
    /// so it never travels in the clear.
    Import {
        /// Key name.
        #[arg(long)]
        name: String,
        /// Algorithm name, as for `generate`.
        #[arg(long)]
        algorithm: String,
        /// The key material.
        #[arg(
            long,
            required_unless_present = "sealed_file",
            conflicts_with = "sealed_file",
            requires = "wrapping_file"
        )]
        key_file: Option<PathBuf>,
        /// An envelope produced offline by `keys seal`.
        #[arg(long, conflicts_with_all = ["key_file", "wrapping_file", "format"])]
        sealed_file: Option<PathBuf>,
        /// Its encoding: raw, pem or der.
        #[arg(long, default_value = "raw")]
        format: String,
        /// The cluster's active wrapping key (serve's
        /// --key-wrapping-file).
        #[arg(long)]
        wrapping_file: Option<PathBuf>,
    },
    /// Grant a guest alias usages over a key.
    Bind {
        /// Alias guests name.
        #[arg(long)]
        alias: String,
        /// Key name.
        #[arg(long)]
        key: String,
        /// Usages, comma-separated: sign, verify, publicKey, encrypt,
        /// decrypt or derive, as the algorithm supports.
        #[arg(long, value_delimiter = ',', required = true)]
        usages: Vec<String>,
    },
    /// Remove a guest alias.
    Unbind {
        /// Alias.
        #[arg(long)]
        alias: String,
    },
    /// Make a new version of a key active.
    Rotate {
        /// Key name.
        #[arg(long)]
        name: String,
        /// New RSA modulus size; the current one when absent.
        #[arg(long)]
        bits: Option<u64>,
    },
    /// Keep a version (every version without one) for verifying and
    /// decrypting only.
    Retire {
        /// Key name.
        #[arg(long)]
        name: String,
        /// One version.
        #[arg(long)]
        version: Option<u64>,
    },
    /// Refuse a version (every version without one) for every use.
    Revoke {
        /// Key name.
        #[arg(long)]
        name: String,
        /// One version.
        #[arg(long)]
        version: Option<u64>,
    },
    /// Delete a version's material (every version's without one) for
    /// good.
    Destroy {
        /// Key name.
        #[arg(long)]
        name: String,
        /// One version.
        #[arg(long)]
        version: Option<u64>,
    },
    /// Seal a key's material (every key's without a name) under the
    /// leader's active wrapping key.
    Rewrap {
        /// Key name.
        #[arg(long)]
        name: Option<String>,
    },
    /// Print the addressed member's prepared-key cache statistics.
    Cache,
}

/// One retention transition, run through the leader's writer.
#[derive(Subcommand, Debug)]
enum RetentionOp {
    /// Advance the epoch every `--epoch-ms` of leader time, keeping
    /// `--keep-epochs` (the current one included); without
    /// `--epoch-ms`, stop rotating.
    Rotate {
        /// Leader time per epoch.
        #[arg(long)]
        epoch_ms: Option<u64>,
        /// Epochs kept.
        #[arg(long)]
        keep_epochs: u64,
    },
    /// Move the current epoch and the floor by hand, planned against
    /// the current epoch `--from-epoch`.
    Advance {
        /// The current epoch the plan read.
        #[arg(long)]
        from_epoch: u64,
        /// The new current epoch.
        #[arg(long)]
        current_epoch: u64,
        /// The new floor: identities below it are retired.
        #[arg(long)]
        min_epoch: u64,
    },
    /// Collect a page of the receipts and session results the floor
    /// retired.
    Collect {
        /// Receipts to inspect (server default 1,024).
        #[arg(long)]
        limit: Option<u64>,
    },
    /// Bound the bytes of results and sessions retention keeps: past
    /// it, new results are refused (`receipt_budget_exceeded`) until
    /// acknowledgement, close or collection make room; nothing kept is
    /// evicted. Without `--max-bytes`, remove the bound.
    Budget {
        /// Logical bytes of receipts and session rows.
        #[arg(long)]
        max_bytes: Option<u64>,
    },
}

/// Scoped request IDs and retry sessions.
#[derive(Subcommand, Debug)]
enum RetryOp {
    /// Print the `f1` request ID of a business intent in the current
    /// epoch: the same intent always gives the same ID. Persist it
    /// before sending it.
    Id {
        /// The business intent, e.g. `order:42`.
        #[arg(long)]
        intent: String,
    },
    /// Open a retry session for the principal the module's
    /// authorization decides, or recover it: opening again answers the
    /// same session. Without `--session`, open one under a fresh random
    /// ID (persist the ID it prints before using it).
    Open {
        /// The module whose authorization decides the owner.
        #[arg(long)]
        module: String,
        /// The session's 128-bit ID, 32 hex digits.
        #[arg(long)]
        session: Option<String>,
    },
    /// Print a session's state.
    Status {
        /// The module whose authorization decides the owner.
        #[arg(long)]
        module: String,
        /// The session's 128-bit ID, 32 hex digits.
        #[arg(long)]
        session: String,
    },
    /// Acknowledge every sequence through `--through` once its results
    /// are durably consumed: they go, and those sequences never run
    /// again.
    Ack {
        /// The module whose authorization decides the owner.
        #[arg(long)]
        module: String,
        /// The session's 128-bit ID, 32 hex digits.
        #[arg(long)]
        session: String,
        /// The new watermark.
        #[arg(long)]
        through: u64,
        /// Also fence sequences without a result: only to give up on
        /// them deliberately, never for a timeout.
        #[arg(long)]
        abandon: bool,
        /// The most work this acknowledgement does: sequences a strict
        /// one moves past, results an abandon deletes at once (server
        /// default 256, at most 65,536).
        #[arg(long)]
        limit: Option<u64>,
    },
    /// Close a session for good: none of its requests runs again.
    Close {
        /// The module whose authorization decides the owner.
        #[arg(long)]
        module: String,
        /// The session's 128-bit ID, 32 hex digits.
        #[arg(long)]
        session: String,
    },
    /// Print the `f2` request ID of one sequence of a session.
    SessionId {
        /// The module whose authorization decides the owner.
        #[arg(long)]
        module: String,
        /// The session's 128-bit ID, 32 hex digits.
        #[arg(long)]
        session: String,
        /// The sequence, from 1.
        #[arg(long)]
        sequence: u64,
    },
}

/// A change to the catalog's groups.
#[derive(Subcommand, Debug)]
enum GroupOp {
    /// Keep a group led by a preferred voter; omit --node to disable.
    PreferLeader {
        name: String,
        #[arg(long)]
        node: Option<u64>,
        #[arg(long)]
        op: String,
    },
    /// List a group founded on root members `--members`: the lowest one
    /// founds it, and every process hosting a member opens a replica.
    Create {
        /// The group's name.
        name: String,
        /// Founding members, by node ID (comma-separated or repeated).
        #[arg(long, value_delimiter = ',', required = true)]
        members: Vec<u64>,
        /// The operation's identity (default `create-<name>`).
        #[arg(long)]
        op: Option<String>,
    },
    /// Remove a group that owns no partition; its replicas retire.
    Remove {
        /// The group's name.
        name: String,
        /// The operation's identity (default `remove-<name>`).
        #[arg(long)]
        op: Option<String>,
    },
}

/// A change to the catalog's partitions.
#[derive(Subcommand, Debug)]
enum PartitionOp {
    /// Balance partition counts over existing groups through durable moves.
    Resize {
        #[arg(long, value_delimiter = ',', required = true)]
        groups: Vec<String>,
        /// Persist and reuse this identity when retrying an uncertain request.
        #[arg(long)]
        op: String,
    },
    /// Place partition `name` (a database) in `--group`; an existing
    /// root database is adopted in place.
    Create {
        /// The partition: its database's name.
        name: String,
        /// The group serving it.
        #[arg(long)]
        group: String,
        /// The operation's identity (default `create-<name>`).
        #[arg(long)]
        op: Option<String>,
    },
    /// Retire a partition where it is: its rows go, its name stays.
    Retire {
        /// The partition.
        name: String,
        /// The operation's identity (default `retire-<name>`).
        #[arg(long)]
        op: Option<String>,
    },
    /// Move a partition to `--to` while it serves: copy, freeze, import
    /// the difference, cut over at the next epoch. With `--wait`, follow
    /// it and exit nonzero unless it completes.
    Move {
        /// The partition.
        name: String,
        /// The destination group.
        #[arg(long)]
        to: String,
        /// The operation's identity (default `move-<name>`).
        #[arg(long)]
        op: Option<String>,
        /// Wait until the move completes or is given up.
        #[arg(long)]
        wait: bool,
        /// How long `--wait` waits.
        #[arg(long, default_value_t = 120_000)]
        timeout_ms: u64,
    },
    /// Give up move `--op` before its cutover: the source serves again
    /// at a new epoch.
    Abort {
        /// The partition.
        name: String,
        /// The move's operation.
        #[arg(long)]
        op: String,
        /// Why, recorded with the move.
        #[arg(long)]
        reason: Option<String>,
    },
}

/// An operator action on cross-group transactions.
#[derive(Subcommand, Debug)]
enum TransactionsOp {
    /// Abort an undecided transaction at its coordinator group; one
    /// already decided keeps its decision.
    Abort {
        /// The transaction ID (32 hex digits).
        transaction: String,
        /// Its coordinator group (default the root).
        #[arg(long)]
        group: Option<String>,
    },
}

/// `cargo-flower modules`.
async fn run_modules(
    endpoint: &str,
    db: Option<String>,
    admin_token: Option<String>,
    op: Option<ModulesCommand>,
) -> Result<(), String> {
    let client = client(endpoint, admin_token)?;
    let options = RequestOptions::default();
    match op.unwrap_or(ModulesCommand::List) {
        ModulesCommand::List => {
            let listed = client
                .modules(db.as_deref(), &options)
                .await
                .map_err(|error| error.to_string())?;
            for database in listed {
                for module in database.modules {
                    let staging = module.staging.map_or("none".to_string(), |staging| {
                        format!(
                            "ready={} position={}/{} retired={} graph={}/{} rebuilt={}",
                            staging.ready,
                            staging.position,
                            staging.planned,
                            staging.retired,
                            staging.graph_position,
                            staging.graph,
                            staging.rebuilt
                        )
                    });
                    println!(
                        "db={} module={} identity={} definitions={} collections={} http={} \
                         authorize={} staging={staging}",
                        database.db,
                        module.name,
                        module.identity.as_deref().unwrap_or("none"),
                        module.definitions.len(),
                        module.collections.join(","),
                        module.http.keys().cloned().collect::<Vec<_>>().join(","),
                        module.authorize,
                    );
                }
                for purging in database.purging {
                    println!(
                        "db={} purging={} deleted={}",
                        database.db, purging.name, purging.deleted
                    );
                }
                for removed in database.removed {
                    println!(
                        "db={} removed={} identity={}",
                        database.db, removed.name, removed.identity
                    );
                }
            }
            Ok(())
        }
        ModulesCommand::Remove { module, purge } => {
            let db = db.ok_or("`modules remove` needs --db")?;
            let outcome = if purge {
                client.purge_module(&db, &module, &options).await
            } else {
                client.remove_module(&db, &module, &options).await
            }
            .map_err(|error| error.to_string())?;
            println!(
                "removed={module} db={db} purging={} position={}",
                outcome.purging, outcome.position
            );
            Ok(())
        }
    }
}

/// `cargo-flower group`.
async fn run_group(
    endpoint: &str,
    admin_token: Option<String>,
    op: Option<GroupOp>,
) -> Result<(), String> {
    let client = client(endpoint, admin_token)?;
    let options = RequestOptions::default();
    let answer = match op {
        Some(GroupOp::PreferLeader { name, node, op }) => {
            client.prefer_group_leader(&name, node, &op, &options).await
        }
        None => {
            let listed = client
                .groups(&options)
                .await
                .map_err(|error| error.to_string())?;
            for group in listed.groups {
                let members: Vec<String> = group.members.iter().map(u64::to_string).collect();
                println!(
                    "group={} uid={} status={} members={} hosted={} leader={}",
                    group.name,
                    group.uid,
                    group.status,
                    members.join(","),
                    group.hosted,
                    group
                        .leader
                        .map_or("unknown".to_string(), |id| id.to_string()),
                );
            }
            println!("revision={} node={}", listed.revision, listed.node);
            return Ok(());
        }
        Some(GroupOp::Create { name, members, op }) => {
            client
                .create_group(&name, &members, op.as_deref(), &options)
                .await
        }
        Some(GroupOp::Remove { name, op }) => {
            client.remove_group(&name, op.as_deref(), &options).await
        }
    };
    let value = answer.map_err(|error| error.to_string())?;
    println!("{value:#}");
    Ok(())
}

/// One partition as `cargo-flower partition` lists it.
fn partition_line(entry: &flower_client::partitions::PartitionInfo) -> String {
    let movement = entry
        .movement
        .as_ref()
        .map_or("none".to_string(), |movement| {
            format!(
                "{}:{}->{}@{}",
                movement.phase, movement.source, movement.destination, movement.epoch
            )
        });
    format!(
        "partition={} owner={} epoch={} status={} move={movement}",
        entry.name, entry.owner, entry.epoch, entry.status
    )
}

/// `cargo-flower partition`.
async fn run_partition(
    endpoint: &str,
    admin_token: Option<String>,
    op: Option<PartitionOp>,
) -> Result<(), String> {
    let client = client(endpoint, admin_token)?;
    let options = RequestOptions::default();
    let answer = match op {
        None => {
            let listed = client
                .partitions(&options)
                .await
                .map_err(|error| error.to_string())?;
            for entry in &listed.partitions {
                println!("{}", partition_line(entry));
            }
            for done in &listed.moves {
                println!(
                    "moved op={} partition={} from={} to={} phase={} epoch={} reason={}",
                    done.op,
                    done.partition,
                    done.source,
                    done.destination,
                    done.phase,
                    done.epoch,
                    done.reason.as_deref().unwrap_or("none"),
                );
            }
            println!("revision={}", listed.revision);
            return Ok(());
        }
        Some(PartitionOp::Resize { groups, op }) => {
            client.resize_partitions(&groups, &op, &options).await
        }
        Some(PartitionOp::Create { name, group, op }) => {
            client
                .create_partition(&name, &group, op.as_deref(), &options)
                .await
        }
        Some(PartitionOp::Retire { name, op }) => {
            client
                .retire_partition(&name, op.as_deref(), &options)
                .await
        }
        Some(PartitionOp::Abort { name, op, reason }) => {
            client
                .abort_move(&name, &op, reason.as_deref(), &options)
                .await
        }
        Some(PartitionOp::Move {
            name,
            to,
            op,
            wait,
            timeout_ms,
        }) => {
            let op = op.unwrap_or_else(|| format!("move-{name}"));
            let begun = client
                .move_partition(&name, &to, Some(&op), &options)
                .await
                .map_err(|error| error.to_string())?;
            println!("{begun:#}");
            if !wait {
                return Ok(());
            }
            return wait_for_move(
                &client,
                &name,
                &op,
                std::time::Duration::from_millis(timeout_ms),
            )
            .await;
        }
    };
    let value = answer.map_err(|error| error.to_string())?;
    println!("{value:#}");
    Ok(())
}

/// Follow move `op` of `name` until it finishes: its phases as they
/// change, then its outcome. Nonzero unless it completed.
async fn wait_for_move(
    client: &FlowerClient,
    name: &str,
    op: &str,
    timeout: std::time::Duration,
) -> Result<(), String> {
    let deadline = std::time::Instant::now() + timeout;
    let mut last = String::new();
    loop {
        let listed = client
            .partitions(&RequestOptions::default())
            .await
            .map_err(|error| error.to_string())?;
        if let Some(done) = listed.moves.iter().rev().find(|done| done.op == op) {
            return if done.phase == "complete" {
                println!(
                    "move {op}: complete, {name} served by {} at epoch {}",
                    done.destination, done.epoch
                );
                Ok(())
            } else {
                Err(format!(
                    "move {op}: {} ({})",
                    done.phase,
                    done.reason.as_deref().unwrap_or("no reason")
                ))
            };
        }
        let phase = listed
            .partitions
            .iter()
            .find(|entry| entry.name == name)
            .and_then(|entry| entry.movement.as_ref())
            .filter(|movement| movement.op == op)
            .map(|movement| movement.phase.clone())
            .unwrap_or_default();
        if !phase.is_empty() && phase != last {
            println!("move {op}: {phase}");
            last = phase;
        }
        if std::time::Instant::now() >= deadline {
            return Err(format!(
                "move {op} still {last} after {}ms",
                timeout.as_millis()
            ));
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
}

/// `cargo-flower transactions`.
async fn run_transactions(
    endpoint: &str,
    admin_token: Option<String>,
    op: Option<TransactionsOp>,
) -> Result<(), String> {
    let client = client(endpoint, admin_token)?;
    let options = RequestOptions::default();
    let value = match op {
        None => client.transactions(&options).await,
        Some(TransactionsOp::Abort { transaction, group }) => {
            client
                .abort_transaction(&transaction, group.as_deref(), &options)
                .await
        }
    }
    .map_err(|error| error.to_string())?;
    println!("{value:#}");
    Ok(())
}

/// `cargo-flower backup`: with `--endpoint`, take a full backup through
/// a node now; `list` and `restore` read the backup directory offline.
#[derive(clap::Args, Debug)]
struct BackupArgs {
    #[command(subcommand)]
    op: Option<BackupCommand>,
    /// Server base URL to take a full backup through now.
    #[arg(long, env = "FLOWER_ENDPOINT")]
    endpoint: Option<String>,
    /// Admin bearer token.
    #[arg(long, env = "FLOWER_ADMIN_TOKEN")]
    admin_token: Option<String>,
}

#[derive(Subcommand, Debug)]
enum BackupCommand {
    /// Print a member's continuous-backup status as JSON (with
    /// `--endpoint`): its role, ticks, bases, archive position and
    /// failures, the last backup taken through it and its log hold.
    Status,
    /// List live backups, oldest first: each one's history, cut and how
    /// far a restore from it reaches through the archive after it.
    List {
        #[command(flatten)]
        store: BackupStoreArgs,
    },
    /// Restore a stopped node's directory offline: the newest state the
    /// backups hold (the latest base and the archive after it), one
    /// backup by manifest name, the state at a time, or exactly through
    /// a log index. The group file is replaced and the Raft log removed;
    /// start the node with `--bootstrap` afterwards. For an existing
    /// cluster, stop every member and restore the same target on each.
    Restore {
        #[command(flatten)]
        store: BackupStoreArgs,
        #[command(flatten)]
        guest: GuestArgs,
        /// Node directory to restore into.
        #[arg(long)]
        dir: PathBuf,
        /// Restore every write stamped at or before this time, in ms
        /// since the epoch.
        #[arg(long, conflicts_with_all = ["index", "manifest"])]
        at_ms: Option<u64>,
        /// Restore exactly through this log index of one history.
        #[arg(long, conflicts_with = "manifest")]
        index: Option<u64>,
        /// The history `--index` counts in, as `backup list` prints it;
        /// defaults to the latest backup's.
        #[arg(long, requires = "index")]
        history: Option<String>,
        /// Restore this backup's manifest as it was taken.
        #[arg(long)]
        manifest: Option<String>,
    },
}

/// Where backups live and the key that opens them. Every flag falls
/// back to its environment variable, as `serve`'s do.
#[derive(clap::Args, Debug)]
#[command(group(
    clap::ArgGroup::new("backup_location")
        .args(["backup_dir", "backup_url"])
        .required(true)
))]
struct BackupStoreArgs {
    /// Backup object directory.
    #[arg(long, env = "FLOWER_BACKUP_DIR", conflicts_with = "backup_url")]
    backup_dir: Option<PathBuf>,
    /// Where backups live instead, as `serve --backup-url` names it:
    /// `s3://BUCKET/PREFIX` (configured from FLOWER_BACKUP_S3_*) or
    /// `file:///PATH`.
    #[arg(long, env = "FLOWER_BACKUP_URL")]
    backup_url: Option<String>,
    /// Backup keystore file.
    #[arg(long, env = "FLOWER_BACKUP_KEYSTORE")]
    keystore: PathBuf,
    /// File holding the 32 raw KEK bytes.
    #[arg(long, env = "FLOWER_BACKUP_KEK_FILE")]
    kek_file: PathBuf,
}

/// A deployment's slot bindings, shared by `plan`, `deploy` and
/// `deployment stage`. They ride in the manifest's `bindings` member,
/// which the deployment's content identity covers.
#[derive(clap::Args, Debug, Default)]
struct BindArgs {
    /// Bind a logical import slot (`$name`) to a deployed module, as
    /// SLOT=MODULE; repeatable.
    #[arg(long = "bind", value_name = "SLOT=MODULE")]
    bind: Vec<String>,
    /// Bindings from a JSON object of slot to module.
    #[arg(long, value_name = "FILE")]
    bindings: Option<PathBuf>,
}

impl BindArgs {
    /// The bindings the flags name.
    fn bindings(&self) -> Result<artifact::Bindings, String> {
        artifact::parse_bindings(&self.bind, self.bindings.as_deref())
    }
}

/// Wasm guest limits, shared by `serve` and `backup restore`: a
/// restore registers every module again, and must do so under the
/// limits the cluster deployed it with. Every flag falls back to its
/// environment variable.
#[derive(clap::Args, Debug)]
struct GuestArgs {
    /// Linear memory one Wasm guest instance may reach, in bytes.
    #[arg(long, env = "FLOWER_GUEST_MEMORY_BYTES", default_value_t = wasm::DEFAULT_INSTANCE_MEMORY)]
    guest_memory_bytes: usize,
    /// Linear memory all live guest instances of one invocation may
    /// reach together, nested calls included, in bytes.
    #[arg(long, env = "FLOWER_GUEST_INVOCATION_MEMORY_BYTES", default_value_t = wasm::DEFAULT_INVOCATION_MEMORY)]
    guest_invocation_memory_bytes: usize,
    /// Elements one guest table may reach.
    #[arg(long, env = "FLOWER_GUEST_TABLE_ELEMENTS", default_value_t = wasm::DEFAULT_TABLE_ELEMENTS)]
    guest_table_elements: usize,
    /// Wall-clock deadline of one guest invocation whose method
    /// declares none (`timeout` on `#[query]`/`#[mutation]`), nested
    /// calls included, in ms. Wall-clock only: fuel follows the
    /// declaration and constants, the same on every member.
    #[arg(long, env = "FLOWER_GUEST_TIMEOUT_MS", default_value_t = wasm::DEFAULT_TIMEOUT.as_millis() as u64)]
    guest_timeout_ms: u64,
    /// The longest deadline a method may declare, in ms: longer
    /// declarations run to this.
    #[arg(long, env = "FLOWER_GUEST_MAX_TIMEOUT_MS", default_value_t = wasm::DEFAULT_MAX_TIMEOUT.as_millis() as u64)]
    guest_max_timeout_ms: u64,
}

impl GuestArgs {
    /// Guest runtime limits, validated.
    fn limits(&self) -> Result<wasm::Limits, String> {
        let limits = wasm::Limits {
            instance_memory: self.guest_memory_bytes,
            invocation_memory: self.guest_invocation_memory_bytes,
            table_elements: self.guest_table_elements,
            timeout: std::time::Duration::from_millis(self.guest_timeout_ms),
            max_timeout: std::time::Duration::from_millis(self.guest_max_timeout_ms),
            fuel: wasm::FUEL_PER_CALL,
        };
        if limits.instance_memory == 0 || limits.instance_memory > 1 << 32 {
            return Err("--guest-memory-bytes must be between 1 and 4 GiB".to_string());
        }
        if limits.invocation_memory < limits.instance_memory {
            return Err(
                "--guest-invocation-memory-bytes must be at least --guest-memory-bytes".to_string(),
            );
        }
        if limits.table_elements == 0 || limits.timeout.is_zero() {
            return Err(
                "--guest-table-elements and --guest-timeout-ms must be positive".to_string(),
            );
        }
        if limits.max_timeout < limits.timeout {
            return Err("--guest-max-timeout-ms must be at least --guest-timeout-ms".to_string());
        }
        Ok(limits)
    }
}

/// `cargo-flower serve` settings. Every credential and tuning flag
/// falls back to its environment variable.
#[derive(clap::Args, Debug)]
struct ServeArgs {
    /// Node directory holding the group and Raft files.
    #[arg(long)]
    dir: PathBuf,
    /// This node's ID.
    #[arg(long)]
    id: u64,
    /// HTTP listen address: HTTP/1.1 and HTTP/2 (prior-knowledge h2c,
    /// or `h2` by ALPN over TLS).
    #[arg(long, default_value = "127.0.0.1:8080")]
    http: String,
    /// Serve the API on this already-bound listening TCP socket, a
    /// descriptor (a socket handle on Windows) the supervisor passed,
    /// instead of binding --http, which must name its address. The
    /// member takes it over and closes it on exit.
    #[arg(long, env = "FLOWER_LISTEN_FD")]
    listen_fd: Option<u64>,
    /// The inherited API listener, taken before the runtime starts.
    #[arg(skip)]
    inherited: Option<std::net::TcpListener>,
    /// PEM certificate chain the HTTP API presents, leaf first. With
    /// --tls-key every API connection runs TLS 1.2 or 1.3 and clients
    /// use https URLs; no plaintext fallback.
    #[arg(long, env = "FLOWER_TLS_CERT_FILE")]
    tls_cert: Option<PathBuf>,
    /// PEM private key for --tls-cert (PKCS#8, PKCS#1 or SEC1).
    #[arg(long, env = "FLOWER_TLS_KEY_FILE")]
    tls_key: Option<PathBuf>,
    /// How long an API TLS handshake may take before the connection is
    /// dropped, in ms.
    #[arg(
        long,
        env = "FLOWER_TLS_HANDSHAKE_TIMEOUT_MS",
        default_value_t = 10_000
    )]
    tls_handshake_timeout_ms: u64,
    /// Most concurrent HTTP/2 streams per API connection; hyper's
    /// default when unset.
    #[arg(long, env = "FLOWER_HTTP2_MAX_STREAMS")]
    http2_max_streams: Option<u32>,
    /// Raft listen address.
    #[arg(long, default_value = "127.0.0.1:9000")]
    raft: String,
    /// Bootstrap a new cluster with these `id`/`id=addr` members.
    #[arg(long)]
    bootstrap: Vec<String>,
    /// Admin bearer token: operator endpoints, and the trusted-gateway
    /// credential that may assert principals and timestamps.
    #[arg(long, env = "FLOWER_ADMIN_TOKEN")]
    admin_token: Option<String>,
    /// Peer token every Raft member presents; defaults to the admin
    /// token. Required unless --raft binds a loopback address.
    #[arg(long, env = "FLOWER_PEER_TOKEN")]
    peer_token: Option<String>,
    /// Worker bearer token for the /v1/queue protocol.
    #[arg(long, env = "FLOWER_WORKER_TOKEN")]
    worker_token: Option<String>,
    /// Sync bearer token for /v1/sync replicas and devices.
    #[arg(long, env = "FLOWER_SYNC_TOKEN")]
    sync_token: Option<String>,
    #[command(flatten)]
    guest: GuestArgs,
    /// Guest instances the pooling allocator holds slots for, across
    /// the process.
    #[arg(long, env = "FLOWER_WASM_POOL_SLOTS", default_value_t = wasm::DEFAULT_POOL_SLOTS)]
    wasm_pool_slots: u32,
    /// Written guest memory a released slot restores in place and keeps
    /// resident, in bytes (Linux 6.7+; no effect elsewhere).
    #[arg(long, env = "FLOWER_WASM_KEEP_RESIDENT_BYTES", default_value_t = wasm::DEFAULT_KEEP_RESIDENT)]
    wasm_keep_resident_bytes: usize,
    /// Compiled guest images the image cache keeps, in bytes.
    #[arg(long, env = "FLOWER_IMAGE_CACHE_BYTES", default_value_t = wasm::DEFAULT_IMAGE_CACHE_BYTES)]
    image_cache_bytes: usize,
    /// Compiled guest functions kept for reuse by later compilations,
    /// in bytes; 0 keeps none.
    #[arg(long, env = "FLOWER_CODE_CACHE_BYTES", default_value_t = wasm::DEFAULT_CODE_CACHE_BYTES)]
    code_cache_bytes: usize,
    /// Query results kept for reuse across revisions their reads did
    /// not change, in bytes; 0 disables the cache.
    #[arg(long, env = "FLOWER_QUERY_CACHE_BYTES", default_value_t = flower_engine::query_cache::DEFAULT_QUERY_CACHE_BYTES)]
    query_cache_bytes: usize,
    /// Identical queries in flight a member keeps track of, so later
    /// arrivals wait for the first one's result instead of evaluating
    /// it again, in bytes; 0 turns joining off.
    #[arg(long, env = "FLOWER_QUERY_FLIGHT_BYTES", default_value_t = flower_cluster::flight::DEFAULT_FLIGHT_BYTES)]
    query_flight_bytes: usize,
    /// Authorization decisions kept for reuse while nothing they read
    /// changes, in bytes; 0 runs every module's hook on every call.
    #[arg(long, env = "FLOWER_DECISION_MEMO_BYTES", default_value_t = flower_engine::decisions::DEFAULT_DECISION_MEMO_BYTES)]
    decision_memo_bytes: usize,
    /// Threads the leader's writer prepares a wave of mutations on
    /// speculatively, its own included, and the widest a wave gets; 1
    /// prepares every write serially on the staged state. Defaults to
    /// this machine's cores, at most 8.
    #[arg(
        long,
        env = "FLOWER_WRITER_PREPARATION_WORKERS",
        default_value_t = flower_cluster::speculation::default_workers() as u16,
        value_parser = clap::value_parser!(u16).range(1..=256)
    )]
    writer_preparation_workers: u16,
    /// Conflict prediction: `1` places calls of methods (and records)
    /// whose speculative preparations keep conflicting in their turn
    /// instead of speculating them; `0` speculates every mutation.
    #[arg(
        long,
        env = "FLOWER_WRITER_CONFLICT_PREDICTION",
        default_value = "1",
        action = clap::ArgAction::Set,
        value_parser = zero_or_one
    )]
    writer_conflict_prediction: bool,
    /// Largest deploy request body, in bytes.
    #[arg(long, env = "FLOWER_DEPLOY_MAX_BYTES", default_value_t = flower_server::api::DEFAULT_DEPLOY_LIMIT)]
    deploy_max_bytes: usize,
    /// Largest encoded effects one transaction may commit (its
    /// replicated entry), in bytes; larger ones fail
    /// `transaction_too_large` (HTTP 413) before anything is proposed.
    /// Must fit the peer frame with its JSON encoding (four times it):
    /// this member's, checked at startup, and every peer's, checked when
    /// a peer refuses a dial whose entries its frames cannot carry.
    #[arg(long, env = "FLOWER_MAX_TRANSACTION_BYTES", default_value_t = flower_engine::patch::DEFAULT_MAX_TRANSACTION_BYTES)]
    max_transaction_bytes: usize,
    /// Largest Raft peer frame, in bytes: above the JSON encoding of
    /// the largest deployment entry (about 4x its Wasm bundle).
    #[arg(long, env = "FLOWER_RAFT_MAX_FRAME_BYTES", default_value_t = 256 * 1024 * 1024)]
    raft_max_frame_bytes: u64,
    /// Most concurrent inbound Raft peer connections, every hosted
    /// group's together; at the cap a dial replaces the connection idle
    /// longest, and forwarded client requests hold at most half.
    #[arg(long, env = "FLOWER_RAFT_MAX_CONNECTIONS", default_value_t = flower_cluster::network::DEFAULT_MAX_CONNECTIONS)]
    raft_max_connections: usize,
    /// Bytes of inbound Raft frames over 64 KiB this member holds at
    /// once; a leader whose append finds no room resends fewer entries,
    /// then backs off. Defaults to twice --raft-max-frame-bytes plus
    /// 1 MiB; at least one frame plus 1 MiB.
    #[arg(long, env = "FLOWER_RAFT_QUEUED_INPUT_BYTES")]
    raft_queued_input_bytes: Option<u64>,
    /// Least time a data append gets, in ms, however short the Raft
    /// RPC timeout.
    #[arg(long, env = "FLOWER_RAFT_APPEND_TIMEOUT_MS", default_value_t = 5_000)]
    raft_append_timeout_ms: u64,
    /// Slowest link a data append is given time for, in bytes per
    /// second: each byte adds to its deadline.
    #[arg(long, env = "FLOWER_RAFT_APPEND_BYTES_PER_SECOND", default_value_t = 8 * 1024 * 1024)]
    raft_append_bytes_per_second: u64,
    /// PEM certificate chain this member presents on its Raft port,
    /// leaf first. With --raft-tls-key and --raft-tls-ca every peer
    /// connection runs inside TLS, verified against the member's
    /// address; all members must agree. The peer token still applies.
    #[arg(long, env = "FLOWER_RAFT_TLS_CERT_FILE")]
    raft_tls_cert: Option<PathBuf>,
    /// PEM private key for --raft-tls-cert (PKCS#8, PKCS#1 or SEC1).
    #[arg(long, env = "FLOWER_RAFT_TLS_KEY_FILE")]
    raft_tls_key: Option<PathBuf>,
    /// PEM CA roots trusted for peers' certificates, and only them.
    #[arg(long, env = "FLOWER_RAFT_TLS_CA_FILE")]
    raft_tls_ca: Option<PathBuf>,
    /// Round trip between the farthest replicas, in ms; the other Raft
    /// timers derive from it.
    #[arg(long, env = "FLOWER_RAFT_RTT_MS")]
    raft_rtt_ms: Option<u64>,
    /// How often a Raft leader sends heartbeats, in ms.
    #[arg(long, env = "FLOWER_RAFT_HEARTBEAT_MS")]
    raft_heartbeat_ms: Option<u64>,
    /// AppendEntries and quorum-read deadline, in ms.
    #[arg(long, env = "FLOWER_RAFT_RPC_TIMEOUT_MS")]
    raft_rpc_timeout_ms: Option<u64>,
    /// How long voters refuse other candidates after hearing from their
    /// leader, and wait before campaigning themselves, in ms.
    #[arg(long, env = "FLOWER_RAFT_LEADER_LEASE_MS")]
    raft_leader_lease_ms: Option<u64>,
    /// Shortest election timeout past the leader lease, in ms.
    #[arg(long, env = "FLOWER_RAFT_ELECTION_MIN_MS")]
    raft_election_min_ms: Option<u64>,
    /// Longest election timeout past the leader lease, in ms.
    #[arg(long, env = "FLOWER_RAFT_ELECTION_MAX_MS")]
    raft_election_max_ms: Option<u64>,
    /// How long a confirmed leader answers fresh reads without another
    /// quorum round, in ms: shorter than the leader lease;
    /// 0 proves every fresh read with a round.
    #[arg(long, env = "FLOWER_RAFT_READ_LEASE_MS")]
    raft_read_lease_ms: Option<u64>,
    /// Base URL clients reach this member's HTTP API at, named to
    /// clients routing by leader while it leads; defaults to the HTTP
    /// listen address unless that binds every interface.
    #[arg(long, env = "FLOWER_ADVERTISE_HTTP")]
    advertise_http: Option<String>,
    /// Serve the admin dashboard at /admin on the HTTP port: static
    /// pages that read the admin endpoints with the admin token the
    /// operator enters. `false` (or `0`) answers 404 there.
    #[arg(
        long,
        env = "FLOWER_ADMIN_UI",
        default_value_t = true,
        action = clap::ArgAction::Set,
        value_parser = clap::builder::BoolishValueParser::new()
    )]
    admin_ui: bool,
    /// Build a snapshot every this many applied entries.
    #[arg(long, env = "FLOWER_RAFT_SNAPSHOT_LOGS", default_value_t = SnapshotSettings::default().logs_since_last)]
    raft_snapshot_logs: u64,
    /// Entries kept after a snapshot for catch-up and device sync.
    #[arg(long, env = "FLOWER_RAFT_KEEP_LOGS", default_value_t = SnapshotSettings::default().keep_logs)]
    raft_keep_logs: u64,
    /// Deadline for sending and installing a snapshot's last segment, in ms.
    #[arg(long, env = "FLOWER_RAFT_INSTALL_SNAPSHOT_TIMEOUT_MS", default_value_t = SnapshotSettings::default().install_timeout_ms)]
    raft_install_snapshot_timeout_ms: u64,
    /// Snapshot segment size on the wire, in bytes.
    #[arg(long, env = "FLOWER_RAFT_SNAPSHOT_CHUNK_BYTES", default_value_t = SnapshotSettings::default().chunk_bytes)]
    raft_snapshot_chunk_bytes: u64,
    /// Also snapshot once the entries applied since the last snapshot
    /// carry this many bytes; 0 turns the byte trigger off.
    #[arg(long, env = "FLOWER_SNAPSHOT_AFTER_BYTES", default_value_t = SnapshotPolicy::default().after_bytes)]
    snapshot_after_bytes: u64,
    /// Also snapshot once the last snapshot is this old and anything
    /// applied since, in ms; 0 turns the age trigger off.
    #[arg(long, env = "FLOWER_SNAPSHOT_MAX_AGE_MS", default_value_t = 300_000)]
    snapshot_max_age_ms: u64,
    /// `off` never syncs the data files: a crash of the process loses
    /// nothing, a crash of the machine may, and the member then refuses
    /// to open until its directory is emptied or the files trusted.
    #[arg(long, env = "FLOWER_FSYNC", default_value = "on")]
    fsync: String,
    /// `lazy` defers the leader's own log flush while its followers
    /// alone form a majority (three voters or more): commits form on
    /// their fsyncs. `immediate` flushes each leader append at once,
    /// still in parallel with replication.
    #[arg(long, env = "FLOWER_LEADER_FLUSH", default_value = "lazy")]
    leader_flush: String,
    /// How long a lazy leader waits for its followers to commit an
    /// append before it flushes the append itself, in ms.
    #[arg(long, env = "FLOWER_LEADER_FLUSH_GRACE_MS", default_value_t = 30)]
    leader_flush_grace_ms: u64,
    /// The longest a lazy leader's append waits for its flush, in ms.
    #[arg(long, env = "FLOWER_LEADER_FLUSH_INTERVAL_MS", default_value_t = 500)]
    leader_flush_interval_ms: u64,
    /// How often applied entries are made durable in the group file, in
    /// ms; a crash replays the rest from the log. 0 makes every apply
    /// durable on its own, a second fsync per entry.
    #[arg(long, env = "FLOWER_APPLY_FLUSH_MS", default_value_t = 1_000)]
    apply_flush_ms: u64,
    /// How often the byte and age triggers are checked, in ms.
    #[arg(long, env = "FLOWER_SNAPSHOT_CHECK_MS", default_value_t = 1_000)]
    snapshot_check_ms: u64,
    /// Most of the time snapshot builds may take before the byte and
    /// age triggers wait, 1-100.
    #[arg(long, env = "FLOWER_SNAPSHOT_DUTY_PERCENT", default_value_t = SnapshotPolicy::default().duty_percent)]
    snapshot_duty_percent: u8,
    /// Time in-flight HTTP requests get to finish on shutdown, in ms.
    #[arg(long, env = "FLOWER_SHUTDOWN_TIMEOUT_MS", default_value_t = 30_000)]
    shutdown_timeout_ms: u64,
    /// Backup object directory (needs keystore and KEK too); turns on
    /// continuous backup. Give every member the same one: the leader
    /// takes bases and archives the log, and every member keeps purged
    /// log entries until they are archived, so whichever member leads
    /// next continues the same archive.
    #[arg(long, env = "FLOWER_BACKUP_DIR", conflicts_with = "backup_url")]
    backup_dir: Option<PathBuf>,
    /// Where backups go instead of --backup-dir: `s3://BUCKET/PREFIX` or
    /// `file:///PATH`. S3 reads its region, endpoint, addressing and
    /// credentials from FLOWER_BACKUP_S3_REGION, FLOWER_BACKUP_S3_ENDPOINT
    /// (another S3 implementation), FLOWER_BACKUP_S3_ADDRESSING (path or
    /// virtual) and FLOWER_BACKUP_S3_ACCESS_KEY_ID,
    /// FLOWER_BACKUP_S3_SECRET_ACCESS_KEY and FLOWER_BACKUP_S3_SESSION_TOKEN,
    /// else their AWS_ names, else AWS's credential chain.
    #[arg(long, env = "FLOWER_BACKUP_URL")]
    backup_url: Option<String>,
    /// Backup keystore file.
    #[arg(long, env = "FLOWER_BACKUP_KEYSTORE")]
    keystore: Option<PathBuf>,
    /// File holding the 32 raw KEK bytes.
    #[arg(long, env = "FLOWER_BACKUP_KEK_FILE")]
    kek_file: Option<PathBuf>,
    /// How often the leader archives applied log entries and every
    /// member lets its log go of archived ones, in ms.
    #[arg(long, env = "FLOWER_BACKUP_ARCHIVE_MS", default_value_t = 1000)]
    backup_archive_ms: u64,
    /// How old the newest base backup may grow before the leader takes
    /// another, in ms.
    #[arg(long, env = "FLOWER_BACKUP_BASE_MS", default_value_t = 24 * 60 * 60 * 1000)]
    backup_base_ms: u64,
    /// How many sealed bytes the leader may archive after the newest base
    /// before it takes another, unless that base is larger (64 MiB): a base
    /// is due once the archive since it outgrows both. 0 leaves bases to
    /// --backup-base-ms.
    #[arg(long, env = "FLOWER_BACKUP_BASE_BYTES", default_value_t = flower_backup::driver::DEFAULT_BASE_BYTES)]
    backup_base_bytes: u64,
    /// Bytes per uploaded part of a backup object (16 MiB; at least 5 MiB,
    /// S3's smallest): larger objects go up as multipart uploads, and this
    /// much is buffered per upload.
    #[arg(long, env = "FLOWER_BACKUP_PART_BYTES", default_value_t = flower_backup::object::DEFAULT_PART_BYTES as u64)]
    backup_part_bytes: u64,
    /// How far back restores reach, in ms (30 days): retention keeps every
    /// point of this window restorable, and thins older bases to one per
    /// --backup-base-ms.
    #[arg(long, env = "FLOWER_BACKUP_RETENTION_MS", default_value_t = flower_backup::driver::DEFAULT_RETENTION.as_millis() as u64)]
    backup_retention_ms: u64,
    /// How often the leader retains, collects orphans and retires key
    /// versions besides after each base, in ms (an hour).
    #[arg(long, env = "FLOWER_BACKUP_RETENTION_INTERVAL_MS", default_value_t = flower_backup::driver::DEFAULT_RETENTION_INTERVAL.as_millis() as u64)]
    backup_retention_interval_ms: u64,
    /// Most purged log entries a member keeps for an archive that
    /// cannot keep up; past it the oldest go unarchived and the leader
    /// takes a base to resume. 0 keeps every one until archived.
    #[arg(long, env = "FLOWER_BACKUP_HOLD_MAX_ENTRIES", default_value_t = 0)]
    backup_hold_max_entries: u64,
    /// Most bytes of purged log entries a member keeps for an archive that
    /// cannot keep up (1 GiB); past it the oldest go unarchived and the
    /// leader takes a base to resume. 0 keeps every one until archived.
    #[arg(long, env = "FLOWER_BACKUP_HOLD_MAX_BYTES", default_value_t = flower_backup::driver::DEFAULT_MAX_HELD_BYTES)]
    backup_hold_max_bytes: u64,
    /// File holding the 32 raw bytes of the wrapping key that seals the
    /// databases' replicated key catalogs; every member of every group
    /// mounts the same one. Without it the member applies the catalogs
    /// but serves no catalog keys, and a move to a group it leads waits
    /// until it can. `FLOWER_KEYRING_FILE`, its earlier name, is read
    /// when neither the flag nor `FLOWER_KEY_WRAPPING_FILE` is given.
    #[arg(long, env = "FLOWER_KEY_WRAPPING_FILE")]
    key_wrapping_file: Option<PathBuf>,
    /// Earlier wrapping-key files still unwrapping catalog envelopes
    /// until a `rewrap`, repeatable.
    #[arg(long)]
    key_wrapping_previous: Vec<PathBuf>,
    /// Prepared-key cache budget in bytes; 0 disables retention.
    #[arg(long, env = "FLOWER_KEY_CACHE_BYTES", default_value_t = flower_crypto::replicated::DEFAULT_CACHE_BYTES)]
    key_cache_bytes: usize,
    /// Prepared-key context lifetime in milliseconds; 0 never expires.
    #[arg(long, env = "FLOWER_KEY_CACHE_TTL_MS", default_value_t = 0)]
    key_cache_ttl_ms: u64,
    /// Timer sweep interval in milliseconds.
    #[arg(long, default_value_t = 1000)]
    timer_sweep_ms: u64,
    /// How long a partition move may keep its partition unserved, or wait
    /// to freeze it, before it is given up, in milliseconds.
    #[arg(long, env = "FLOWER_PARTITION_FREEZE_MAX_MS", default_value_t = 30_000)]
    partition_freeze_max_ms: u64,
    /// Reconcile groups' preferred leaders, or leave leadership to elections.
    #[arg(long, env = "FLOWER_LEADER_PLACEMENT", default_value = "on", value_parser = ["on", "off"])]
    leader_placement: String,
    /// The retry window the leader gives every database nobody
    /// configured retention for, in milliseconds: epochs of an eighth of
    /// it, nine kept, so results stay retryable at least this long and
    /// at most an eighth longer, and are collected after. 0 initializes
    /// none (receipts with no expiry are then kept until an operator
    /// rotates or advances retention).
    #[arg(long, env = "FLOWER_RETRY_WINDOW_MS", default_value_t = flower_cluster::node::DEFAULT_RETRY_WINDOW_MS)]
    retry_window_ms: u64,
    /// Compact a file online once its extent is this many times its
    /// allocated pages; 0 turns online compaction off.
    #[arg(long, env = "FLOWER_COMPACT_RATIO", default_value_t = CompactionSettings::default().ratio)]
    compact_ratio: u64,
    /// Most pages one compaction step moves.
    #[arg(long, env = "FLOWER_COMPACT_STEP_PAGES", default_value_t = CompactionSettings::default().step_pages)]
    compact_step_pages: usize,
    /// Files whose extent is below this many bytes never compact.
    #[arg(long, env = "FLOWER_COMPACT_MIN_FILE_BYTES", default_value_t = CompactionSettings::default().min_file_bytes)]
    compact_min_file_bytes: u64,
    /// How often an idle compactor measures each file, in ms.
    #[arg(long, env = "FLOWER_COMPACT_CHECK_MS", default_value_t = 10_000)]
    compact_check_ms: u64,
    /// Share of the time a running compaction spends in steps, 1-100.
    #[arg(long, env = "FLOWER_COMPACT_DUTY_PERCENT", default_value_t = CompactionSettings::default().duty_percent)]
    compact_duty_percent: u32,
    /// Bytes the data directory's file system must have free before a
    /// member whose storage failed exits for its restart: a restart
    /// into a disk with less fails again.
    #[arg(long, env = "FLOWER_STORAGE_RECOVERY_FREE_BYTES", default_value_t = flower_cluster::health::DEFAULT_RECOVERY_FREE_BYTES)]
    storage_recovery_free_bytes: u64,
    /// Concurrent query and watch-refresh evaluations; defaults to
    /// the CPU count.
    #[arg(long, env = "FLOWER_QUERY_WORKERS")]
    query_workers: Option<usize>,
    /// Concurrent deployments.
    #[arg(long, env = "FLOWER_CONTROL_WORKERS", default_value_t = AdmissionConfig::default().control_workers)]
    control_workers: usize,
    /// Bytes queries, refreshes and writers may hold queued or running.
    #[arg(long, env = "FLOWER_QUEUED_INPUT_BYTES", default_value_t = AdmissionConfig::default().queued_input_bytes)]
    queued_input_bytes: usize,
    /// Bytes deployments may hold queued or running.
    #[arg(long, env = "FLOWER_CONTROL_QUEUED_INPUT_BYTES", default_value_t = AdmissionConfig::default().control_input_bytes)]
    control_queued_input_bytes: usize,
    /// Longest a query or writer waits for admission before a
    /// retryable overload, in ms.
    #[arg(long, env = "FLOWER_ADMISSION_MAX_WAIT_MS", default_value_t = 10_000)]
    admission_max_wait_ms: u64,
    /// Watch refreshes' minimum share of evaluation turns and slots
    /// while queries wait, 1-100.
    #[arg(long, env = "FLOWER_ADMISSION_REFRESH_SHARE_PERCENT", default_value_t = AdmissionConfig::default().refresh_share_percent)]
    admission_refresh_share_percent: usize,
    /// The queue delay admission tolerates as a standing minimum, in ms:
    /// once every query (or writer) wait in an interval exceeded it,
    /// requests carrying a client deadline (x-flower-timeout-ms) that
    /// waited past twice it are shed with a retryable overload.
    #[arg(long, env = "FLOWER_ADMISSION_TARGET_MS", default_value_t = 50)]
    admission_target_ms: u64,
    /// The interval admission decides queue overload over, in ms.
    #[arg(long, env = "FLOWER_ADMISSION_INTERVAL_MS", default_value_t = 500)]
    admission_interval_ms: u64,
    /// Memory reserved per running query or refresh evaluation: a
    /// guest instance plus the host's share.
    #[arg(long, env = "FLOWER_EVALUATION_MEMORY_BYTES", default_value_t = AdmissionConfig::default().evaluation_memory_bytes)]
    evaluation_memory_bytes: usize,
    /// Memory all running evaluations may be reserved together; the
    /// evaluation pool runs no more slots than it holds. Defaults to a
    /// reservation per query worker.
    #[arg(long, env = "FLOWER_EVALUATION_MEMORY_BUDGET_BYTES")]
    evaluation_memory_budget_bytes: Option<usize>,
}

impl ServeArgs {
    /// The wrapping-key file: the flag (or `FLOWER_KEY_WRAPPING_FILE`),
    /// else `FLOWER_KEYRING_FILE`, the variable's earlier name.
    fn wrapping_key_file(&self) -> Option<PathBuf> {
        self.key_wrapping_file
            .clone()
            .or_else(|| std::env::var_os("FLOWER_KEYRING_FILE").map(PathBuf::from))
    }

    /// How the HTTP API listener serves: TLS from both files or
    /// neither, and the HTTP/2 stream limit.
    fn http_options(&self) -> Result<HttpOptions, String> {
        let tls = match (&self.tls_cert, &self.tls_key) {
            (None, None) => None,
            (Some(cert), Some(key)) => {
                if self.tls_handshake_timeout_ms == 0 {
                    return Err("the TLS handshake timeout must be positive".to_string());
                }
                Some(ApiTls::from_files(cert, key)?.with_handshake_timeout(
                    std::time::Duration::from_millis(self.tls_handshake_timeout_ms),
                ))
            }
            _ => return Err("API TLS needs --tls-cert and --tls-key together".to_string()),
        };
        if self.http2_max_streams == Some(0) {
            return Err("--http2-max-streams must be positive".to_string());
        }
        Ok(HttpOptions {
            tls,
            http2_max_streams: self.http2_max_streams,
        })
    }

    /// Take over the listening socket --listen-fd names, which must be
    /// bound to --http. Runs before the runtime starts, so no other
    /// startup work can reuse the descriptor's number first.
    fn inherit_listener(&mut self) -> Result<(), String> {
        let Some(raw) = self.listen_fd else {
            return Ok(());
        };
        let expected: std::net::SocketAddr = self.http.parse().map_err(|_| {
            format!(
                "--listen-fd needs --http to name the socket's address as ip:port, not {:?}",
                self.http
            )
        })?;
        let listener = flower_server::listener::inherited(raw, expected)
            .map_err(|error| format!("--listen-fd {raw}: {error}"))?;
        self.inherited = Some(listener);
        Ok(())
    }

    /// Peer TLS from the three files, or none without any of them.
    fn peer_tls(&self) -> Result<Option<Arc<PeerTls>>, String> {
        match (&self.raft_tls_cert, &self.raft_tls_key, &self.raft_tls_ca) {
            (None, None, None) => Ok(None),
            (Some(cert), Some(key), Some(ca)) => {
                PeerTls::from_files(cert, key, ca).map(|tls| Some(Arc::new(tls)))
            }
            _ => Err(
                "peer TLS needs --raft-tls-cert, --raft-tls-key and --raft-tls-ca together"
                    .to_string(),
            ),
        }
    }

    /// Online compaction settings, validated.
    fn compaction(&self) -> Result<CompactionSettings, String> {
        let settings = CompactionSettings {
            ratio: self.compact_ratio,
            step_pages: self.compact_step_pages,
            min_file_bytes: self.compact_min_file_bytes,
            check: std::time::Duration::from_millis(self.compact_check_ms),
            duty_percent: self.compact_duty_percent,
        };
        settings.validate()?;
        Ok(settings)
    }

    /// Continuous-backup pacing, validated.
    fn continuous(&self) -> Result<ContinuousSettings, String> {
        if [
            self.backup_archive_ms,
            self.backup_base_ms,
            self.backup_retention_ms,
            self.backup_retention_interval_ms,
        ]
        .contains(&0)
        {
            return Err(
                "backup archive, base, retention and retention intervals must be positive"
                    .to_string(),
            );
        }
        Ok(ContinuousSettings {
            archive_interval: std::time::Duration::from_millis(self.backup_archive_ms),
            base_interval: std::time::Duration::from_millis(self.backup_base_ms),
            base_bytes: (self.backup_base_bytes > 0).then_some(self.backup_base_bytes),
            retention: std::time::Duration::from_millis(self.backup_retention_ms),
            retention_interval: std::time::Duration::from_millis(self.backup_retention_interval_ms),
            base_retry: flower_backup::driver::BASE_RETRY,
            max_held: (self.backup_hold_max_entries > 0).then_some(self.backup_hold_max_entries),
            max_held_bytes: (self.backup_hold_max_bytes > 0).then_some(self.backup_hold_max_bytes),
            // Give the last shipment a sixth of the shutdown budget.
            final_archive_within: std::time::Duration::from_millis(self.shutdown_timeout_ms / 6),
        })
    }

    /// Bytes per uploaded backup part, validated: no smaller than S3
    /// accepts.
    fn backup_part_bytes(&self) -> Result<usize, String> {
        let min = flower_backup::object::MIN_PART_BYTES;
        usize::try_from(self.backup_part_bytes)
            .ok()
            .filter(|bytes| *bytes >= min)
            .ok_or_else(|| format!("backup part bytes must be at least {min} (5 MiB)"))
    }

    /// When the log and group files reach the disk, validated.
    fn flush(&self) -> Result<FlushSettings, String> {
        FlushSettings::parse(
            Some(&self.leader_flush),
            Some(&self.leader_flush_grace_ms.to_string()),
            Some(&self.leader_flush_interval_ms.to_string()),
            Some(&self.apply_flush_ms.to_string()),
        )
    }

    /// Soft snapshot triggers, validated.
    fn snapshot_policy(&self) -> Result<SnapshotPolicy, String> {
        let policy = SnapshotPolicy {
            after_bytes: self.snapshot_after_bytes,
            max_age: std::time::Duration::from_millis(self.snapshot_max_age_ms),
            check: std::time::Duration::from_millis(self.snapshot_check_ms),
            duty_percent: self.snapshot_duty_percent,
        };
        policy.validate()?;
        Ok(policy)
    }

    /// The process-wide Wasm engine: its pooling allocator sized for
    /// the guest limits, and its image cache.
    fn wasm_engine(&self) -> Result<wasm::EngineSettings, String> {
        if self.wasm_pool_slots == 0 || self.image_cache_bytes == 0 {
            return Err("--wasm-pool-slots and --image-cache-bytes must be positive".to_string());
        }
        Ok(wasm::EngineSettings {
            pool_slots: self.wasm_pool_slots,
            table_elements: self.guest.guest_table_elements,
            keep_resident: self.wasm_keep_resident_bytes,
            image_cache_bytes: self.image_cache_bytes,
            code_cache_bytes: self.code_cache_bytes,
        })
    }

    /// Admission sizing, validated.
    fn admission(&self) -> Result<AdmissionConfig, String> {
        let defaults = AdmissionConfig::default();
        let config = AdmissionConfig {
            query_workers: self.query_workers.unwrap_or(defaults.query_workers),
            control_workers: self.control_workers,
            queued_input_bytes: self.queued_input_bytes,
            control_input_bytes: self.control_queued_input_bytes,
            max_wait: std::time::Duration::from_millis(self.admission_max_wait_ms),
            refresh_share_percent: self.admission_refresh_share_percent,
            codel_target: std::time::Duration::from_millis(self.admission_target_ms),
            codel_interval: std::time::Duration::from_millis(self.admission_interval_ms),
            evaluation_memory_bytes: self.evaluation_memory_bytes,
            evaluation_memory_budget: self.evaluation_memory_budget_bytes,
        };
        config.validate()?;
        Ok(config)
    }
}

#[derive(Subcommand, Debug)]
enum QueueCommand {
    /// Count pending, claimed and dead items.
    Stats {
        /// Queue name.
        #[arg(long)]
        queue: String,
        /// Count one scope only (`""` is the default scope); every scope
        /// without it.
        #[arg(long)]
        scope: Option<String>,
    },
    /// Print one stored item as JSON.
    Peek {
        /// Queue name.
        #[arg(long)]
        queue: String,
        /// Item ID.
        #[arg(long)]
        id: String,
    },
    /// Claim due items, one JSON object per line.
    Poll {
        /// Queue name.
        #[arg(long)]
        queue: String,
        /// Most items to claim.
        #[arg(long)]
        limit: Option<usize>,
        /// Claim lease in milliseconds.
        #[arg(long)]
        lease_ms: Option<u64>,
        /// The scope to claim from (default: the default scope).
        #[arg(long, default_value = "")]
        scope: String,
        /// The claimer, recorded on each lease.
        #[arg(long)]
        owner: Option<String>,
        /// Wait in the scope's line this long when the claim comes back short
        /// (needs --owner).
        #[arg(long)]
        wait_ms: Option<u64>,
    },
    /// Renew a claim's lease.
    Heartbeat {
        /// Queue name.
        #[arg(long)]
        queue: String,
        /// Item ID.
        #[arg(long)]
        id: String,
        /// Claim token.
        #[arg(long)]
        token: String,
        /// New lease in milliseconds.
        #[arg(long)]
        lease_ms: Option<u64>,
    },
    /// Complete a claim.
    Complete {
        /// Queue name.
        #[arg(long)]
        queue: String,
        /// Item ID.
        #[arg(long)]
        id: String,
        /// Claim token.
        #[arg(long)]
        token: String,
    },
    /// Fail a claim: requeue, or die past the maximum.
    Fail {
        /// Queue name.
        #[arg(long)]
        queue: String,
        /// Item ID.
        #[arg(long)]
        id: String,
        /// Claim token.
        #[arg(long)]
        token: String,
        /// Redelivery delay in milliseconds.
        #[arg(long)]
        delay_ms: Option<u64>,
    },
    /// Kill a claim into the dead letter file now.
    Kill {
        /// Queue name.
        #[arg(long)]
        queue: String,
        /// Item ID.
        #[arg(long)]
        id: String,
        /// Claim token.
        #[arg(long)]
        token: String,
    },
    /// Release a claim to pending without an attempt.
    Release {
        /// Queue name.
        #[arg(long)]
        queue: String,
        /// Item ID.
        #[arg(long)]
        id: String,
        /// Claim token.
        #[arg(long)]
        token: String,
        /// Redelivery delay in milliseconds.
        #[arg(long)]
        delay_ms: Option<u64>,
    },
    /// Cancel an item in any state.
    Cancel {
        /// Queue name.
        #[arg(long)]
        queue: String,
        /// Item ID.
        #[arg(long)]
        id: String,
    },
    /// Requeue a dead item with a fresh budget.
    Retry {
        /// Queue name.
        #[arg(long)]
        queue: String,
        /// Item ID.
        #[arg(long)]
        id: String,
        /// Redelivery delay in milliseconds.
        #[arg(long)]
        delay_ms: Option<u64>,
    },
    /// Delete dead items enqueued before a cutoff.
    Purge {
        /// Queue name.
        #[arg(long)]
        queue: String,
        /// Purge items enqueued before this time (default now).
        #[arg(long)]
        older_than_ms: Option<u64>,
    },
}

#[derive(Subcommand, Debug)]
enum TimerCommand {
    /// Count pending, dead and held timers.
    Stats,
    /// Print one stored timer as JSON.
    Get {
        /// Timer ID.
        #[arg(long)]
        id: String,
    },
    /// List timers due-first, one JSON object per line.
    List {
        /// `pending` (default), `dead`, `held` or `all`.
        #[arg(long)]
        state: Option<String>,
        /// Most timers to list.
        #[arg(long)]
        limit: Option<usize>,
    },
    /// Cancel a timer.
    Cancel {
        /// Timer ID.
        #[arg(long)]
        id: String,
    },
    /// Requeue a dead timer with a fresh budget.
    Retry {
        /// Timer ID.
        #[arg(long)]
        id: String,
        /// Redelivery delay in milliseconds.
        #[arg(long)]
        delay_ms: Option<u64>,
    },
    /// Delete dead timers scheduled before a cutoff.
    Purge {
        /// Purge timers scheduled before this time (default now).
        #[arg(long)]
        older_than_ms: Option<u64>,
    },
}

#[derive(Subcommand, Debug)]
enum ContractCommand {
    /// Generate TypeScript wire contracts for public queries and mutations.
    Typescript {
        #[arg(long)]
        manifest: PathBuf,
        #[arg(long)]
        out: PathBuf,
    },
    /// Print a manifest's collections, definitions, triggers and SQL plans.
    Inspect {
        /// Manifest JSON file.
        #[arg(long)]
        manifest: PathBuf,
    },
    /// Diff two manifests; incompatible lines start with `!`.
    Diff {
        /// Old manifest JSON file.
        #[arg(long)]
        from: PathBuf,
        /// New manifest JSON file.
        #[arg(long)]
        to: PathBuf,
    },
}

/// The server client commands talk to without `--endpoint` or
/// `FLOWER_ENDPOINT`: a local `cargo flower serve` with its defaults.
const DEFAULT_ENDPOINT: &str = "http://127.0.0.1:8080";

/// The stack the CLI runs on: clap builds every command's arguments at
/// once, which outgrows a 1 MiB main thread (Windows') in debug builds.
const MAIN_STACK: usize = 16 * 1024 * 1024;

fn main() {
    let main = std::thread::Builder::new()
        .name("cargo-flower".to_string())
        .stack_size(MAIN_STACK)
        .spawn(cli_main)
        .expect("main thread");
    if main.join().is_err() {
        std::process::exit(101);
    }
}

/// Parse the command line and run the command.
fn cli_main() {
    // Cargo runs a subcommand binary with a leading `flower`; direct
    // invocation works too.
    let mut args: Vec<_> = std::env::args_os().collect();
    if args.get(1).is_some_and(|arg| arg == "flower") {
        args.remove(1);
    }
    let mut cli = Cli::parse_from(args);
    if let Command::Serve(args) = &mut cli.command
        && let Err(error) = args.inherit_listener()
    {
        eprintln!("cargo-flower: {error}");
        std::process::exit(1);
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    if let Err(error) = runtime.block_on(run(cli.command)) {
        eprintln!("cargo-flower: {error}");
        std::process::exit(1);
    }
}

async fn run(command: Command) -> Result<(), String> {
    match command {
        Command::Dev(options) => dev::run(options).await,
        Command::SqlExplain { manifest, plan } => {
            let bytes = std::fs::read(&manifest).map_err(|e| e.to_string())?;
            let manifest = parse_value(&bytes).map_err(|e| e.to_string())?;
            let module = load_module(&manifest).map_err(|e| e.to_string())?;
            let plan = module
                .sql
                .get(&plan)
                .ok_or_else(|| "SQL plan is not declared".to_string())?;
            println!(
                "{}",
                plan.explain().canonical_json().map_err(|e| e.message)?
            );
            Ok(())
        }
        Command::Contract { command } => run_contract(command),
        Command::Build {
            manifest_path,
            package,
            profile,
            features,
            no_default_features,
            locked,
            out_dir,
        } => {
            let artifact = artifact::Build {
                manifest_path,
                package,
                profile,
                features,
                no_default_features,
                locked,
            }
            .run(&out_dir)?;
            println!(
                "built {} wasm={} manifest={} sha256={}",
                artifact.name,
                artifact.wasm.display(),
                artifact.manifest.display(),
                artifact.sha256
            );
            println!("{}", artifact.summary);
            Ok(())
        }
        Command::Plan {
            wasm,
            manifest,
            current,
            bind,
            module,
            with,
        } => {
            let bytes = std::fs::read(&wasm).map_err(|error| error.to_string())?;
            let manifest = manifest
                .map(|path| artifact::read_manifest(&path))
                .transpose()?;
            let checked = artifact::preflight(&module, &bytes, manifest, &bind.bindings()?)?;
            println!("{}", checked.summary);
            if !checked.matches_embedded {
                println!("~ the manifest differs from the one the module declares");
            }
            println!(
                "bindings: {}",
                artifact::describe_bindings(&checked.manifest)
            );
            println!("identity={}", artifact::hex(&checked.identity));
            let mut compatible = match current {
                None => true,
                Some(path) => {
                    let current = artifact::check_artifact(&artifact::read_manifest(&path)?)?;
                    diff_modules(&current, &checked.module)
                }
            };
            if !with.is_empty() {
                let active = artifact::deployed_modules(&with)?;
                match flower_engine::contracts::check_links(&active, &module, &checked.module) {
                    Ok(()) => println!("links=ok modules={}", active.len()),
                    Err(failure) => {
                        println!("! links: {}", failure.message);
                        compatible = false;
                    }
                }
            }
            println!("compatible={compatible}");
            if compatible {
                Ok(())
            } else {
                Err("incompatible contract change".to_string())
            }
        }
        Command::Deploy {
            endpoint,
            db,
            module,
            wasm,
            manifest,
            identity,
            bind,
            admin_token,
        } => {
            let wasm = std::fs::read(&wasm).map_err(|error| error.to_string())?;
            let manifest = manifest
                .map(|path| artifact::read_manifest(&path))
                .transpose()?;
            // Refuse a module the server would refuse before credentials
            // are used or anything is sent.
            let bindings = bind.bindings()?;
            let checked =
                artifact::preflight(&module, &wasm, manifest, &bindings).map_err(|error| {
                    format!("invalid_module (preflight; nothing was sent): {error}")
                })?;
            let identity = identity.map(|raw| decode_identity_hex(&raw)).transpose()?;
            let client = client(&endpoint, admin_token)?;
            let outcome = client
                .deploy(
                    &db,
                    &module,
                    identity,
                    &wasm,
                    &checked.manifest,
                    &RequestOptions::default(),
                )
                .await
                .map_err(|error| error.to_string())?;
            println!(
                "revision={} position={}",
                outcome.revision, outcome.position
            );
            Ok(())
        }
        Command::Call {
            endpoint,
            db,
            module,
            name,
            args,
            args_file,
            contract,
            mutation,
            request_id,
            token,
        } => {
            let args = match args_file {
                Some(path) if path == Path::new("-") => {
                    std::io::read_to_string(std::io::stdin()).map_err(|error| error.to_string())?
                }
                Some(path) => std::fs::read_to_string(path).map_err(|error| error.to_string())?,
                None => args,
            };
            let args: serde_json::Value =
                serde_json::from_str(&args).map_err(|error| error.to_string())?;
            if let Some(path) = contract {
                codegen::validate_call(&load_manifest(&path)?, &name, mutation, &args)?;
            }
            let client = client(&endpoint, token)?;
            if mutation {
                let options = MutationOptions {
                    request_id,
                    ..MutationOptions::default()
                };
                let result = client
                    .mutate_value(&db, &module, &name, &args, &options)
                    .await
                    .map_err(|error| error.to_string())?;
                println!("{}", result.value);
                println!(
                    "revision={} position={} duplicate={}",
                    result.revision, result.position, result.duplicate
                );
            } else {
                let result = client
                    .query_value(&db, &module, &name, &args, &RequestOptions::default())
                    .await
                    .map_err(|error| error.to_string())?;
                println!("{}", result.value);
            }
            Ok(())
        }
        Command::Status { endpoint } => {
            let client = client(&endpoint, None)?;
            let status = client
                .status(&RequestOptions::default())
                .await
                .map_err(|error| error.to_string())?;
            println!(
                "id={} state={} leader={:?} term={} last_log={:?} last_applied={:?}",
                status.id,
                status.state,
                status.leader,
                status.term,
                status.last_log,
                status.last_applied
            );
            Ok(())
        }
        Command::Membership {
            endpoint,
            members,
            expected_log_id,
            retain,
            admin_token,
        } => {
            let client = client(&endpoint, admin_token)?;
            let view = if members.is_empty() {
                client.membership_view(&RequestOptions::default()).await
            } else {
                let change = MembershipChange {
                    members: parse_members(&members)?
                        .into_iter()
                        .map(|(id, node)| (id, node.addr))
                        .collect(),
                    expected_log_id: expected_log_id.as_deref().map(parse_log_id).transpose()?,
                    retain,
                };
                client
                    .change_membership(&change, &RequestOptions::default())
                    .await
            }
            .map_err(|error| error.to_string())?;
            print_membership(&view);
            Ok(())
        }
        Command::Leadership {
            endpoint,
            to,
            admin_token,
        } => {
            let client = client(&endpoint, admin_token)?;
            let outcome = client
                .transfer_leadership(to, &RequestOptions::default())
                .await
                .map_err(|error| error.to_string())?;
            println!(
                "from={} to={} term={} unchanged={} elapsed_ms={}",
                outcome.from, outcome.to, outcome.term, outcome.unchanged, outcome.elapsed_ms
            );
            Ok(())
        }
        Command::Backup(args) => run_backup(args).await,
        Command::Queue {
            endpoint,
            db,
            token,
            op,
        } => run_queue(&endpoint, &db, token, op).await,
        Command::Timer {
            endpoint,
            db,
            admin_token,
            op,
        } => run_timer(&endpoint, &db, admin_token, op).await,
        Command::Transaction {
            endpoint,
            token,
            steps,
            request_id,
        } => run_transaction(&endpoint, token, &steps, request_id).await,
        Command::Modules {
            endpoint,
            db,
            admin_token,
            op,
        } => run_modules(&endpoint, db, admin_token, op).await,
        Command::Deployment {
            endpoint,
            db,
            module,
            admin_token,
            op,
        } => run_deployment(&endpoint, &db, &module, admin_token, op).await,
        Command::Keys {
            endpoint,
            db,
            admin_token,
            request_id,
            op,
        } => {
            run_keys(
                &endpoint,
                db.as_deref(),
                admin_token,
                request_id,
                op.unwrap_or(KeysCommand::List),
            )
            .await
        }
        Command::Retention {
            endpoint,
            db,
            admin_token,
            op,
        } => {
            let client = client(&endpoint, admin_token)?;
            let options = RequestOptions::default();
            let operation = match op {
                None => {
                    let status = client
                        .retention_status(&db, &options)
                        .await
                        .map_err(|error| error.to_string())?;
                    println!("{status:#}");
                    return Ok(());
                }
                Some(RetentionOp::Rotate {
                    epoch_ms,
                    keep_epochs,
                }) => serde_json::json!({
                    "operation": "rotate",
                    "epoch_ms": epoch_ms,
                    "keep_epochs": keep_epochs,
                }),
                Some(RetentionOp::Advance {
                    from_epoch,
                    current_epoch,
                    min_epoch,
                }) => serde_json::json!({
                    "operation": "advance",
                    "from_epoch": from_epoch,
                    "current_epoch": current_epoch,
                    "min_epoch": min_epoch,
                }),
                Some(RetentionOp::Collect { limit }) => {
                    serde_json::json!({"operation": "collect", "limit": limit})
                }
                Some(RetentionOp::Budget { max_bytes }) => {
                    serde_json::json!({"operation": "budget", "max_receipt_bytes": max_bytes})
                }
            };
            let answer = client
                .control_retention(&db, operation, &options)
                .await
                .map_err(|error| error.to_string())?;
            println!("{answer:#}");
            Ok(())
        }
        Command::Retry {
            endpoint,
            db,
            token,
            op,
        } => run_retry(&endpoint, &db, token, op).await,
        Command::Group {
            endpoint,
            admin_token,
            op,
        } => run_group(&endpoint, admin_token, op).await,
        Command::Partition {
            endpoint,
            admin_token,
            op,
        } => run_partition(&endpoint, admin_token, op).await,
        Command::Transactions {
            endpoint,
            admin_token,
            op,
        } => run_transactions(&endpoint, admin_token, op).await,
        Command::Raft {
            endpoint,
            admin_token,
        } => {
            let client = client(&endpoint, admin_token)?;
            let metrics = client
                .raft_metrics(&RequestOptions::default())
                .await
                .map_err(|error| error.to_string())?;
            println!("{metrics:#}");
            Ok(())
        }
        Command::Metrics {
            endpoint,
            admin_token,
        } => {
            let client = client(&endpoint, admin_token)?;
            let metrics = client
                .node_metrics(&RequestOptions::default())
                .await
                .map_err(|error| error.to_string())?;
            println!("{metrics:#}");
            Ok(())
        }
        Command::Activity {
            endpoint,
            admin_token,
        } => {
            let client = client(&endpoint, admin_token)?;
            let activity = client
                .activity(&RequestOptions::default())
                .await
                .map_err(|error| error.to_string())?;
            println!("{activity:#}");
            Ok(())
        }
        Command::Serve(args) => run_serve(*args).await,
    }
}

/// Run one staged-deployment step and print the staging after it.
/// `cargo-flower keys`: list or change a database's managed-key
/// catalog; without a database, list every database's.
async fn run_keys(
    endpoint: &str,
    db: Option<&str>,
    admin_token: Option<String>,
    request_id: Option<String>,
    op: KeysCommand,
) -> Result<(), String> {
    use flower_client::admin::KeyOperation;
    if let KeysCommand::Seal {
        key_file,
        format,
        wrapping_file,
        out,
    } = &op
    {
        let sealed = seal_key_file(key_file, wrapping_file, format)?;
        let text = serde_json::to_string_pretty(&sealed).map_err(|error| error.to_string())?;
        match out {
            Some(path) => {
                use std::io::Write;
                let mut file = std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(path)
                    .map_err(|error| format!("{}: {error}", path.display()))?;
                writeln!(file, "{text}").map_err(|error| error.to_string())?;
                file.sync_all().map_err(|error| error.to_string())?;
            }
            None => println!("{text}"),
        }
        return Ok(());
    }
    let client = client(endpoint, admin_token)?;
    let options = RequestOptions::default();
    let operation = match op {
        KeysCommand::Seal { .. } => {
            unreachable!("offline seal handled before constructing a client")
        }
        KeysCommand::List => {
            match db {
                Some(db) => {
                    let listed = client
                        .keys(db, &options)
                        .await
                        .map_err(|error| error.to_string())?;
                    print_catalog(&listed.catalog);
                }
                None => {
                    let catalogs = client
                        .key_catalogs(&options)
                        .await
                        .map_err(|error| error.to_string())?;
                    for (db, catalog) in &catalogs {
                        println!("db={db} revision={}", catalog.revision);
                        print_catalog(catalog);
                    }
                }
            }
            return Ok(());
        }
        KeysCommand::Cache => {
            let stats = client
                .key_cache(&options)
                .await
                .map_err(|error| error.to_string())?;
            println!("{stats:#}");
            return Ok(());
        }
        KeysCommand::Generate {
            name,
            algorithm,
            bits,
        } => KeyOperation::Generate {
            name,
            algorithm,
            bits,
        },
        KeysCommand::Import {
            name,
            algorithm,
            key_file,
            sealed_file,
            format,
            wrapping_file,
        } => {
            let sealed = match sealed_file {
                Some(path) => serde_json::from_slice(
                    &std::fs::read(&path)
                        .map_err(|error| format!("{}: {error}", path.display()))?,
                )
                .map_err(|error| format!("{}: {error}", path.display()))?,
                None => seal_key_file(
                    key_file.as_deref().ok_or("--key-file is required")?,
                    wrapping_file
                        .as_deref()
                        .ok_or("--wrapping-file is required")?,
                    &format,
                )?,
            };
            KeyOperation::Import {
                name,
                algorithm,
                sealed,
            }
        }
        KeysCommand::Bind { alias, key, usages } => KeyOperation::Bind { alias, key, usages },
        KeysCommand::Unbind { alias } => KeyOperation::Unbind { alias },
        KeysCommand::Rotate { name, bits } => KeyOperation::Rotate { name, bits },
        KeysCommand::Retire { name, version } => KeyOperation::Retire { name, version },
        KeysCommand::Revoke { name, version } => KeyOperation::Revoke { name, version },
        KeysCommand::Destroy { name, version } => KeyOperation::Destroy { name, version },
        KeysCommand::Rewrap { name } => KeyOperation::Rewrap { name },
    };
    let db = db.ok_or("key changes name their database (--db)")?;
    let request_id = request_id.unwrap_or_else(fresh_request_id);
    let change = client
        .change_keys(db, &operation, &request_id, &options)
        .await
        .map_err(|error| error.to_string())?;
    println!(
        "request_id={request_id} revision={} duplicate={}",
        change.revision, change.duplicate
    );
    print_catalog(&change.catalog);
    Ok(())
}

fn seal_key_file(
    key_file: &Path,
    wrapping_file: &Path,
    format: &str,
) -> Result<serde_json::Value, String> {
    let wrapping = flower_crypto::replicated::WrappingKey::read(wrapping_file)
        .map_err(|error| format!("{}: {error:#}", wrapping_file.display()))?;
    let material = zeroize::Zeroizing::new(
        std::fs::read(key_file).map_err(|error| format!("{}: {error}", key_file.display()))?,
    );
    flower_crypto::replicated::seal_import(&wrapping, &material, format)
        .map_err(|error| format!("{error:#}"))
}

/// One line per key and per binding.
fn print_catalog(catalog: &flower_client::admin::KeyCatalog) {
    for (name, key) in &catalog.keys {
        let state = |version: &flower_client::admin::KeyVersion| match (
            version.destroyed,
            version.revoked,
            version.retired,
        ) {
            (true, _, _) => "destroyed",
            (_, true, _) => "revoked",
            (_, _, true) => "retired",
            _ => "live",
        };
        let versions: Vec<String> = key
            .versions
            .iter()
            .map(|version| format!("{}:{}", version.version, state(version)))
            .collect();
        println!(
            "key={name} algorithm={} active={} versions={}",
            key.algorithm,
            key.active_version,
            versions.join(","),
        );
    }
    for (alias, binding) in &catalog.bindings {
        println!(
            "binding={alias} key={} usages={}",
            binding.key,
            binding.usages.join(",")
        );
    }
}

/// A request ID unique to this invocation: the clock and the process.
fn fresh_request_id() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_nanos());
    format!("cli-{nanos:x}-{:x}", std::process::id())
}

async fn run_deployment(
    endpoint: &str,
    db: &str,
    module: &str,
    token: Option<String>,
    op: DeploymentCommand,
) -> Result<(), String> {
    let client = client(endpoint, token)?;
    let options = RequestOptions::default();
    let staged = match op {
        DeploymentCommand::Stage {
            wasm,
            manifest,
            identity,
            bind,
        } => {
            let wasm = std::fs::read(&wasm).map_err(|error| error.to_string())?;
            let mut manifest: serde_json::Value = serde_json::from_slice(
                &std::fs::read(&manifest).map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string())?;
            artifact::bind(&mut manifest, &bind.bindings()?)?;
            let identity = identity.map(|raw| decode_identity_hex(&raw)).transpose()?;
            client
                .stage_deployment(db, module, identity, &wasm, &manifest, &options)
                .await
        }
        DeploymentCommand::Advance { max_records } => {
            client
                .advance_deployment(db, module, max_records, &options)
                .await
        }
        DeploymentCommand::Status => client.deployment_status(db, module, &options).await,
        DeploymentCommand::Activate => {
            let outcome = client
                .activate_deployment(db, module, &options)
                .await
                .map_err(|error| error.to_string())?;
            println!(
                "revision={} position={}",
                outcome.revision, outcome.position
            );
            return Ok(());
        }
        DeploymentCommand::Cancel => {
            let canceled = client
                .cancel_deployment(db, module, &options)
                .await
                .map_err(|error| error.to_string())?;
            println!("canceled={canceled}");
            return Ok(());
        }
        DeploymentCommand::Collect { max_keys } => {
            let (collected, done) = client
                .collect_deployment(db, module, max_keys, &options)
                .await
                .map_err(|error| error.to_string())?;
            println!("collected={collected} done={done}");
            return Ok(());
        }
    }
    .map_err(|error| error.to_string())?;
    match staged.staging {
        None => println!("staging=none"),
        Some(staging) => println!(
            "ready={} position={}/{} retired={} identity={} scanned={} graph={}/{} rebuilt={}",
            staging.ready,
            staging.position,
            staging.planned,
            staging.retired,
            staged.identity.as_deref().unwrap_or("none"),
            staged.scanned,
            staging.graph_position,
            staging.graph,
            staging.rebuilt
        ),
    }
    Ok(())
}

/// A switch written `0` or `1`.
fn zero_or_one(text: &str) -> Result<bool, String> {
    match text {
        "0" => Ok(false),
        "1" => Ok(true),
        _ => Err("must be 0 or 1".to_string()),
    }
}

/// A log ID as `term-node-index`.
fn parse_log_id(text: &str) -> Result<LogId, String> {
    let parts: Vec<&str> = text.split('-').collect();
    let number = |part: &str| part.parse::<u64>().ok();
    match parts[..] {
        [term, node_id, index] => match (number(term), number(node_id), number(index)) {
            (Some(term), Some(node_id), Some(index)) => Ok(LogId {
                leader_id: LeaderId { term, node_id },
                index,
            }),
            _ => Err(format!("bad log ID {text:?}: expected term-node-index")),
        },
        _ => Err(format!("bad log ID {text:?}: expected term-node-index")),
    }
}

/// A log ID as `term-node-index`, or `none`.
fn show_log_id(log_id: Option<LogId>) -> String {
    log_id.map_or("none".to_string(), |log| {
        format!(
            "{}-{}-{}",
            log.leader_id.term, log.leader_id.node_id, log.index
        )
    })
}

/// Print a membership view, one fact per line.
fn print_membership(view: &MembershipView) {
    println!("leader={:?}", view.leader);
    println!("log_id={}", show_log_id(view.log_id));
    println!("last_applied={}", show_log_id(view.last_applied));
    for (index, voters) in view.voter_configs.iter().enumerate() {
        let voters: Vec<String> = voters.iter().map(u64::to_string).collect();
        println!("voters[{index}]={}", voters.join(","));
    }
    for (id, addr) in &view.nodes {
        println!("node {id}={addr}");
    }
    let compatibility = &view.compatibility;
    println!(
        "compatibility=wire{}-state{}-snapshot{}-value{}-{} build={}",
        compatibility.raft_wire,
        compatibility.state_machine,
        compatibility.snapshot_format,
        compatibility.value_format,
        compatibility.guest_runtime,
        compatibility.build
    );
}

/// Build a client with an optional bearer token.
/// Run one scoped-ID or retry-session operation, printing its answer.
async fn run_retry(
    endpoint: &str,
    db: &str,
    token: Option<String>,
    op: Option<RetryOp>,
) -> Result<(), String> {
    use flower_client::retention::{Acknowledgement, RetrySession};
    let client = client(endpoint, token)?;
    let options = RequestOptions::default();
    let session_id = |text: &str| -> Result<[u8; 16], String> {
        flower_engine::hex::decode(&text.to_ascii_lowercase())
            .and_then(|bytes| bytes.try_into().ok())
            .ok_or_else(|| format!("`{text}` is not a session ID: 32 hex digits"))
    };
    let print = |session: &RetrySession| {
        let json = serde_json::json!({
            "session": flower_engine::hex::encode(&session.id),
            "incarnation": format!("{:016x}", session.incarnation),
            "epoch": session.epoch,
            "acknowledged": session.acknowledged,
            "closed": session.closed,
        });
        println!("{json:#}");
    };
    let failed = |error: flower_client::FlowerError| error.to_string();
    match op {
        None => {
            let identity = client.retry_identity(db, &options).await.map_err(failed)?;
            let json = serde_json::json!({
                "incarnation": format!("{:016x}", identity.incarnation),
                "epoch": identity.epoch,
                "min_epoch": identity.min_epoch,
                "epoch_ms": identity.epoch_ms,
                "initialized": identity.initialized,
            });
            println!("{json:#}");
        }
        Some(RetryOp::Id { intent }) => {
            let identity = client.retry_identity(db, &options).await.map_err(failed)?;
            println!("{}", identity.request_id(intent.as_bytes()));
        }
        Some(RetryOp::Open { module, session }) => {
            let id = match session {
                Some(text) => session_id(&text)?,
                None => rand::random(),
            };
            let session = client
                .open_retry_session(db, &module, id, &options)
                .await
                .map_err(failed)?;
            print(&session);
        }
        Some(RetryOp::Status { module, session }) => {
            let session = client
                .retry_session_status(db, &module, session_id(&session)?, &options)
                .await
                .map_err(failed)?;
            print(&session);
        }
        Some(RetryOp::Ack {
            module,
            session,
            through,
            abandon,
            limit,
        }) => {
            let mut ack = Acknowledgement::through(through);
            if abandon {
                ack = ack.abandoning();
            }
            if let Some(limit) = limit {
                ack = ack.limit(limit);
            }
            let session = client
                .acknowledge_retry_session(db, &module, session_id(&session)?, ack, &options)
                .await
                .map_err(failed)?;
            print(&session);
        }
        Some(RetryOp::Close { module, session }) => {
            let session = client
                .close_retry_session(db, &module, session_id(&session)?, &options)
                .await
                .map_err(failed)?;
            print(&session);
        }
        Some(RetryOp::SessionId {
            module,
            session,
            sequence,
        }) => {
            if sequence == 0 {
                return Err("sequences start at 1".to_string());
            }
            let session = client
                .retry_session_status(db, &module, session_id(&session)?, &options)
                .await
                .map_err(failed)?;
            println!("{}", session.request_id(sequence));
        }
    }
    Ok(())
}

fn client(endpoint: &str, token: Option<String>) -> Result<FlowerClient, String> {
    let builder = FlowerClient::builder(endpoint);
    let builder = match token {
        Some(token) => builder.credentials(Credentials::Token(token)),
        None => builder,
    };
    builder.build().map_err(|error| error.to_string())
}

/// Run one queue operation.
async fn run_queue(
    endpoint: &str,
    db: &str,
    token: Option<String>,
    op: QueueCommand,
) -> Result<(), String> {
    let client = client(endpoint, token)?;
    let options = RequestOptions::default();
    match op {
        QueueCommand::Stats { queue, scope } => {
            let stats = client
                .queue_stats_in(db, &queue, scope.as_deref(), &options)
                .await
                .map_err(|error| error.to_string())?;
            println!(
                "pending={} claimed={} dead={}",
                stats.pending, stats.claimed, stats.dead
            );
        }
        QueueCommand::Peek { queue, id } => {
            let item = client
                .get_queue_item(db, &queue, &id, &options)
                .await
                .map_err(|error| error.to_string())?
                .ok_or_else(|| format!("unknown queue item {id}"))?;
            println!(
                "{}",
                serde_json::json!({
                    "id": item.id,
                    "payload": item.payload,
                    "state": item.state,
                    "attempts": item.attempts,
                    "max_attempts": item.max_attempts,
                    "enqueued_ms": item.enqueued_ms,
                    "due_ms": item.due_ms,
                    "scope": item.scope,
                    "priority": item.priority,
                    "fence": item.fence,
                    "owner": item.owner,
                    "name": item.name,
                    "group": item.group,
                    "result": item.result,
                })
            );
        }
        QueueCommand::Poll {
            queue,
            limit,
            lease_ms,
            scope,
            owner,
            wait_ms,
        } => {
            let claimed = client
                .claim_queue(
                    db,
                    &queue,
                    &flower_client::work::QueuePoll {
                        scope: &scope,
                        owner: owner.as_deref(),
                        limit,
                        lease_ms,
                        wait_ms,
                    },
                    &options,
                )
                .await
                .map_err(|error| error.to_string())?;
            for claim in claimed {
                println!(
                    "{}",
                    serde_json::json!({
                        "id": claim.id,
                        "payload": claim.payload,
                        "token": claim.token,
                        "attempts": claim.attempts,
                        "fence": claim.fence,
                        "scope": claim.scope,
                        "priority": claim.priority,
                        "expires_ms": claim.expires_ms,
                    })
                );
            }
        }
        QueueCommand::Heartbeat {
            queue,
            id,
            token,
            lease_ms,
        } => {
            client
                .heartbeat_queue(db, &queue, &id, &token, lease_ms, &options)
                .await
                .map_err(|error| error.to_string())?;
            println!("ok");
        }
        QueueCommand::Complete { queue, id, token } => {
            client
                .complete_queue(db, &queue, &id, &token, &options)
                .await
                .map_err(|error| error.to_string())?;
            println!("ok");
        }
        QueueCommand::Fail {
            queue,
            id,
            token,
            delay_ms,
        } => {
            client
                .fail_queue(db, &queue, &id, &token, delay_ms, &options)
                .await
                .map_err(|error| error.to_string())?;
            println!("ok");
        }
        QueueCommand::Kill { queue, id, token } => {
            client
                .kill_queue(db, &queue, &id, &token, &options)
                .await
                .map_err(|error| error.to_string())?;
            println!("ok");
        }
        QueueCommand::Release {
            queue,
            id,
            token,
            delay_ms,
        } => {
            client
                .release_queue(db, &queue, &id, &token, delay_ms, &options)
                .await
                .map_err(|error| error.to_string())?;
            println!("ok");
        }
        QueueCommand::Cancel { queue, id } => {
            let removed = client
                .cancel_queue(db, &queue, &id, &options)
                .await
                .map_err(|error| error.to_string())?;
            println!("removed={removed}");
        }
        QueueCommand::Retry {
            queue,
            id,
            delay_ms,
        } => {
            client
                .retry_queue(db, &queue, &id, delay_ms, &options)
                .await
                .map_err(|error| error.to_string())?;
            println!("ok");
        }
        QueueCommand::Purge {
            queue,
            older_than_ms,
        } => {
            let removed = client
                .purge_queue(db, &queue, older_than_ms, &options)
                .await
                .map_err(|error| error.to_string())?;
            println!("removed={removed}");
        }
    }
    Ok(())
}

/// Run one timer operation.
async fn run_transaction(
    endpoint: &str,
    token: Option<String>,
    steps_json: &str,
    request_id: Option<String>,
) -> Result<(), String> {
    let client = client(endpoint, token)?;
    let steps: Vec<TransactionStep> =
        serde_json::from_str(steps_json).map_err(|error| format!("bad steps JSON: {error}"))?;
    let options = TransactionOptions {
        request_id,
        base: RequestOptions::default(),
    };
    let result = client
        .transaction(&steps, &options)
        .await
        .map_err(|error| error.to_string())?;
    println!(
        "{}",
        serde_json::json!({
            "values": result.values,
            "revisions": result.revisions,
            "position": result.position,
        })
    );
    Ok(())
}

async fn run_timer(
    endpoint: &str,
    db: &str,
    token: Option<String>,
    op: TimerCommand,
) -> Result<(), String> {
    let client = client(endpoint, token)?;
    let options = RequestOptions::default();
    match op {
        TimerCommand::Stats => {
            let stats = client
                .timer_stats(db, &options)
                .await
                .map_err(|error| error.to_string())?;
            println!(
                "pending={} dead={} held={}",
                stats.pending, stats.dead, stats.held
            );
        }
        TimerCommand::Get { id } => {
            let item = client
                .get_timer(db, &id, &options)
                .await
                .map_err(|error| error.to_string())?
                .ok_or_else(|| format!("unknown timer {id}"))?;
            println!(
                "{}",
                serde_json::json!({
                    "id": item.id,
                    "handler": item.handler,
                    "args": item.args,
                    "due_ms": item.due_ms,
                    "attempts": item.attempts,
                    "max_attempts": item.max_attempts,
                    "state": item.state,
                })
            );
        }
        TimerCommand::List { state, limit } => {
            let timers = client
                .list_timers(db, state.as_deref(), limit, &options)
                .await
                .map_err(|error| error.to_string())?;
            for timer in timers {
                println!(
                    "{}",
                    serde_json::json!({
                        "id": timer.id,
                        "handler": timer.handler,
                        "args": timer.args,
                        "due_ms": timer.due_ms,
                        "attempts": timer.attempts,
                        "max_attempts": timer.max_attempts,
                        "state": timer.state,
                    })
                );
            }
        }
        TimerCommand::Cancel { id } => {
            let removed = client
                .cancel_timer(db, &id, &options)
                .await
                .map_err(|error| error.to_string())?;
            println!("removed={removed}");
        }
        TimerCommand::Retry { id, delay_ms } => {
            client
                .retry_timer(db, &id, delay_ms, &options)
                .await
                .map_err(|error| error.to_string())?;
            println!("ok");
        }
        TimerCommand::Purge { older_than_ms } => {
            let removed = client
                .purge_timers(db, older_than_ms, &options)
                .await
                .map_err(|error| error.to_string())?;
            println!("removed={removed}");
        }
    }
    Ok(())
}

/// Load and validate a manifest file.
fn load_manifest(path: &Path) -> Result<ModuleDef, String> {
    let bytes = std::fs::read(path).map_err(|error| error.to_string())?;
    let value = parse_value(&bytes).map_err(|error| error.to_string())?;
    load_module(&value).map_err(|error| error.to_string())
}

fn run_contract(command: ContractCommand) -> Result<(), String> {
    match command {
        ContractCommand::Typescript { manifest, out } => {
            let module = load_manifest(&manifest)?;
            std::fs::write(out, codegen::typescript(&module)).map_err(|error| error.to_string())
        }
        ContractCommand::Inspect { manifest } => {
            let module = load_manifest(&manifest)?;
            println!("collections:");
            for collection in &module.collections {
                println!("  {}:", collection.name);
                println!("    key: {:?}", collection.key_schema);
                println!("    value: {:?}", collection.value_schema);
                for index in &collection.indexes {
                    println!("    index {}: {}", index.name, index.fields.join(", "));
                }
                for reference in &collection.references {
                    println!(
                        "    references {} on_delete={:?}{}",
                        reference.targets.join("|"),
                        reference.on_delete,
                        reference
                            .discriminator
                            .as_ref()
                            .map(|name| format!(" discriminator={name}"))
                            .unwrap_or_default()
                    );
                }
            }
            println!("definitions:");
            let mut defs: Vec<_> = module.def_kinds.iter().collect();
            defs.sort_by_key(|(name, _)| name.as_str());
            for (name, kind) in defs {
                println!("  {name}: {kind:?}");
            }
            println!("triggers:");
            for trigger in &module.triggers {
                println!("  {}: {}", trigger.name, trigger.source);
            }
            println!("sql plans:");
            let plans: BTreeMap<_, _> = module.sql.iter().collect();
            for (name, plan) in plans {
                println!(
                    "  {name}: {}",
                    plan.explain().canonical_json().map_err(|e| e.message)?
                );
            }
            println!("sql endpoints:");
            let endpoints: BTreeMap<_, _> = module.sql_endpoints.iter().collect();
            for (name, endpoint) in endpoints {
                println!(
                    "  {name}: collections={:?} allow_scan={} candidates={} bytes={} work={}",
                    endpoint.collections,
                    endpoint.allow_scan,
                    endpoint.budget.candidates,
                    endpoint.budget.bytes,
                    endpoint.budget.work
                );
            }
            Ok(())
        }
        ContractCommand::Diff { from, to } => {
            let old = load_manifest(&from)?;
            let current = load_manifest(&to)?;
            diff_modules(&old, &current);
            Ok(())
        }
    }
}

/// Print the changes from `old` to `current`: `+` added, `-`
/// removed, `~` changed, `!` changed incompatibly. Returns
/// whether the change is compatible (no `!` line).
fn diff_modules(old: &ModuleDef, current: &ModuleDef) -> bool {
    let mut compatible = true;
    let old_collections: BTreeMap<&str, _> = old
        .collections
        .iter()
        .map(|collection| (collection.name.as_str(), collection))
        .collect();
    let current_collections: BTreeMap<&str, _> = current
        .collections
        .iter()
        .map(|collection| (collection.name.as_str(), collection))
        .collect();
    let mut names = BTreeSet::new();
    names.extend(old_collections.keys().copied());
    names.extend(current_collections.keys().copied());
    for name in names {
        match (old_collections.get(name), current_collections.get(name)) {
            (None, Some(_)) => println!("+ collection {name}"),
            (Some(_), None) => {
                compatible = false;
                println!("! collection {name} removed")
            }
            (Some(old), Some(current)) => {
                if old.key_schema != current.key_schema {
                    compatible = false;
                    println!("! collection {name} key schema changed");
                }
                if old.value_schema != current.value_schema {
                    compatible = false;
                    println!("! collection {name} value schema changed");
                }
                let old_indexes: BTreeMap<&str, _> = old
                    .indexes
                    .iter()
                    .map(|index| (index.name.as_str(), index))
                    .collect();
                let current_indexes: BTreeMap<&str, _> = current
                    .indexes
                    .iter()
                    .map(|index| (index.name.as_str(), index))
                    .collect();
                let mut index_names = BTreeSet::new();
                index_names.extend(old_indexes.keys().copied());
                index_names.extend(current_indexes.keys().copied());
                for index in index_names {
                    match (old_indexes.get(index), current_indexes.get(index)) {
                        (None, Some(_)) => println!("+ collection {name} index {index}"),
                        (Some(_), None) => println!("- collection {name} index {index} removed"),
                        (Some(old), Some(current)) if old.fields != current.fields => {
                            println!("~ collection {name} index {index} fields changed")
                        }
                        _ => {}
                    }
                }
                if old.references != current.references {
                    println!("~ collection {name} references changed");
                }
            }
            (None, None) => {}
        }
    }
    let mut defs = BTreeSet::new();
    defs.extend(old.def_kinds.keys());
    defs.extend(current.def_kinds.keys());
    for name in defs {
        match (old.def_kinds.get(name), current.def_kinds.get(name)) {
            (None, Some(kind)) => println!("+ definition {name} ({kind:?})"),
            (Some(_), None) => {
                compatible = false;
                println!("! definition {name} removed")
            }
            (Some(old), Some(current)) if old != current => {
                compatible = false;
                println!("! definition {name} kind changed from {old:?} to {current:?}")
            }
            (Some(_), Some(_)) if old.args.get(name) != current.args.get(name) => {
                println!("~ definition {name} arguments changed");
            }
            _ => {}
        }
    }
    let old_triggers: BTreeMap<&str, _> = old
        .triggers
        .iter()
        .map(|trigger| (trigger.name.as_str(), trigger))
        .collect();
    let current_triggers: BTreeMap<&str, _> = current
        .triggers
        .iter()
        .map(|trigger| (trigger.name.as_str(), trigger))
        .collect();
    let mut triggers = BTreeSet::new();
    triggers.extend(old_triggers.keys().copied());
    triggers.extend(current_triggers.keys().copied());
    for name in triggers {
        match (old_triggers.get(name), current_triggers.get(name)) {
            (None, Some(trigger)) => println!("+ trigger {name} on {}", trigger.source),
            (Some(_), None) => {
                compatible = false;
                println!("! trigger {name} removed")
            }
            (Some(old), Some(current)) if old.source != current.source => {
                println!("~ trigger {name} source changed to {}", current.source)
            }
            _ => {}
        }
    }
    let mut plans = BTreeSet::new();
    plans.extend(old.sql.keys());
    plans.extend(current.sql.keys());
    for name in plans {
        match (old.sql.get(name), current.sql.get(name)) {
            (None, Some(_)) => println!("+ SQL plan {name}"),
            (Some(_), None) => println!("- SQL plan {name}"),
            (Some(old), Some(current)) if old != current => println!("~ SQL plan {name} changed"),
            _ => {}
        }
    }
    let mut endpoints = BTreeSet::new();
    endpoints.extend(old.sql_endpoints.keys());
    endpoints.extend(current.sql_endpoints.keys());
    for name in endpoints {
        match (old.sql_endpoints.get(name), current.sql_endpoints.get(name)) {
            (None, Some(_)) => println!("+ SQL endpoint {name}"),
            (Some(_), None) => println!("- SQL endpoint {name}"),
            (Some(old), Some(current)) if old != current => {
                println!("~ SQL endpoint {name} scope or limits changed")
            }
            _ => {}
        }
    }
    compatible
}

/// The replicated key catalog's runtime: the mounted wrapping keys and
/// the prepared-key cache. Without a wrapping key the runtime is
/// locked: the member applies and lists the catalogs, and its guests
/// hash, draw entropy and use keys they carry, but every catalog key
/// answers `KEYS_LOCKED`.
fn key_catalog(
    wrapping: Option<PathBuf>,
    previous: &[PathBuf],
    cache_bytes: usize,
    cache_ttl_ms: u64,
) -> Result<Arc<flower_crypto::replicated::ManagedKeys>, String> {
    use flower_crypto::replicated::{ManagedKeys, WrappingKey};
    let read = |path: &Path| {
        WrappingKey::read(path).map_err(|error| format!("{}: {error:#}", path.display()))
    };
    let active = match wrapping {
        Some(wrapping) => Some(read(&wrapping)?),
        None if previous.is_empty() => None,
        None => return Err("--key-wrapping-previous needs --key-wrapping-file".to_string()),
    };
    let previous = previous
        .iter()
        .map(|path| read(path))
        .collect::<Result<Vec<_>, _>>()?;
    let ttl = (cache_ttl_ms > 0).then(|| std::time::Duration::from_millis(cache_ttl_ms));
    Ok(Arc::new(
        ManagedKeys::new(active, previous).with_cache(cache_bytes, ttl),
    ))
}

/// Shut down on Ctrl-C, SIGTERM on Unix, or Ctrl-Break on Windows.
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut term =
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(term) => term,
                Err(_) => {
                    let _ = tokio::signal::ctrl_c().await;
                    return;
                }
            };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(windows)]
    {
        // CREATE_NEW_PROCESS_GROUP disables Ctrl-C delivery on Windows;
        // Ctrl-Break can still target that group without stopping its parent.
        match tokio::signal::windows::ctrl_break() {
            Ok(mut interrupt) => tokio::select! {
                _ = tokio::signal::ctrl_c() => {},
                _ = interrupt.recv() => {},
            },
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

/// Whether a listen address binds only the loopback interface.
fn is_loopback(addr: &std::net::SocketAddr) -> bool {
    addr.ip().is_loopback()
}

/// The API URL this member advertises for clients routing by leader:
/// the explicit one, else the HTTP listen address unless it binds every
/// interface (clients could not dial that).
fn advertised_api(
    explicit: Option<String>,
    http: &std::net::SocketAddr,
    tls: bool,
) -> Option<String> {
    let scheme = if tls { "https" } else { "http" };
    explicit
        .map(|url| url.trim_end_matches('/').to_string())
        .or_else(|| (!http.ip().is_unspecified()).then(|| format!("{scheme}://{http}")))
}

/// The peer credential policy: an explicit peer token, else the admin token
/// (the fallback for single-secret deployments). A Raft listener reachable
/// from other hosts without either is refused: an unauthenticated peer port
/// lets anyone who can reach it rewrite the replicated log. Loopback
/// listeners (one-host test clusters) run without one, with a loud warning.
fn peer_token(
    peer_token: Option<String>,
    admin_token: Option<&str>,
    raft: &std::net::SocketAddr,
) -> Result<Option<String>, String> {
    match peer_token.or_else(|| admin_token.map(str::to_string)) {
        Some(token) if token.trim().is_empty() => {
            Err("the peer token must be nonempty".to_string())
        }
        Some(token) => Ok(Some(token)),
        None if is_loopback(raft) => {
            eprintln!(
                "cargo-flower: WARNING: Raft on {raft} accepts unauthenticated peers; \
                 set --peer-token or FLOWER_PEER_TOKEN before exposing it"
            );
            Ok(None)
        }
        None => Err(format!(
            "refusing to serve Raft on {raft} without a peer token: set --peer-token or \
             FLOWER_PEER_TOKEN (or --admin-token), or bind a loopback address"
        )),
    }
}

async fn run_serve(args: ServeArgs) -> Result<(), String> {
    // Logs always; OpenTelemetry export when FLOWER_OTEL_ENABLED asks.
    // The exporters' blocking HTTP clients are built off the async
    // workers.
    let otel = flower_server::telemetry::OtelConfig::from_env()?;
    let id = args.id;
    let telemetry =
        tokio::task::spawn_blocking(move || flower_server::telemetry::init_with(&otel, id))
            .await
            .map_err(|error| error.to_string())??;
    let served = serve_node(args).await;
    // Flush what telemetry buffered, whatever stopped the node.
    if let Err(error) = telemetry.shutdown().await {
        eprintln!("cargo-flower: telemetry shutdown: {error}");
    }
    served
}

/// Serve a node until shutdown (see [`run_serve`]).
async fn serve_node(mut args: ServeArgs) -> Result<(), String> {
    let compaction = args.compaction()?;
    let snapshot_policy = args.snapshot_policy()?;
    let durability = Durability::parse(Some(&args.fsync))?;
    let flush = args.flush()?;
    if !durability.fsync {
        eprintln!(
            "warning: FLOWER_FSYNC=off: the data files are never synced; a crash of the machine \
             can lose acknowledged writes, and this member then refuses to open its files"
        );
    }
    let admission = args.admission()?;
    let guest_limits = args.guest.limits()?;
    let entry_room = flower_engine::patch::frame_for_entry(args.max_transaction_bytes as u64);
    if args.max_transaction_bytes == 0 || entry_room > args.raft_max_frame_bytes {
        return Err(format!(
            "--max-transaction-bytes must be positive, and four times it plus 1 MiB at most --raft-max-frame-bytes ({})",
            args.raft_max_frame_bytes
        ));
    }
    flower_engine::patch::set_max_transaction_bytes(args.max_transaction_bytes);
    // Before any backend exists: the engine starts with these settings.
    wasm::configure(args.wasm_engine()?).map_err(|failure| failure.message)?;
    let continuous = args.continuous()?;
    let part_bytes = args.backup_part_bytes()?;
    let http_options = args.http_options()?;
    let inherited = args.inherited.take();
    std::fs::create_dir_all(&args.dir).map_err(|error| error.to_string())?;
    let timing = RaftTiming::derive(&TimingSettings {
        rtt_ms: args.raft_rtt_ms,
        heartbeat_ms: args.raft_heartbeat_ms,
        rpc_timeout_ms: args.raft_rpc_timeout_ms,
        leader_lease_ms: args.raft_leader_lease_ms,
        election_min_ms: args.raft_election_min_ms,
        election_max_ms: args.raft_election_max_ms,
        read_lease_ms: args.raft_read_lease_ms,
    })?;
    let snapshots = SnapshotSettings {
        logs_since_last: args.raft_snapshot_logs,
        keep_logs: args.raft_keep_logs,
        install_timeout_ms: args.raft_install_snapshot_timeout_ms,
        chunk_bytes: args.raft_snapshot_chunk_bytes,
    };
    let config = raft_config(&timing, &snapshots)?;
    let raft_listener = tokio::net::TcpListener::bind(&args.raft)
        .await
        .map_err(|error| error.to_string())?;
    let raft_addr = raft_listener
        .local_addr()
        .map_err(|error| error.to_string())?;
    let wrapping_file = args.wrapping_key_file();
    let tls = args.peer_tls()?;
    let peers = PeerConfig {
        tls,
        token: peer_token(args.peer_token, args.admin_token.as_deref(), &raft_addr)?
            .map(Into::into),
        max_frame: args.raft_max_frame_bytes,
        max_connections: args.raft_max_connections,
        max_queued_bytes: args.raft_queued_input_bytes,
        append_timeout: std::time::Duration::from_millis(args.raft_append_timeout_ms),
        append_bytes_per_second: args.raft_append_bytes_per_second,
        ..PeerConfig::default()
    };
    peers.validate()?;
    let catalog = key_catalog(
        wrapping_file.clone(),
        &args.key_wrapping_previous,
        args.key_cache_bytes,
        args.key_cache_ttl_ms,
    )?;
    // The root's crypto host, over its databases' replicated key
    // catalogs. Every other group keeps a runtime of its own over its
    // databases' (see `runtimes_for`). Every member has one, locked
    // when no wrapping key is mounted: deployments' key declarations
    // validate alike on every member, and guests' crypto answers there
    // rather than trapping.
    let base_crypto =
        Arc::new(flower_crypto::host::CryptoHost::new().with_catalog(catalog.clone()));
    let catalogs: Arc<std::sync::Mutex<BTreeMap<String, Arc<ManagedKeys>>>> = Arc::default();
    catalogs
        .lock()
        .expect("catalogs poisoned")
        .insert("root".to_string(), catalog);
    let runtimes_for: RuntimesFor = {
        let (base, catalogs, limits) =
            (base_crypto.clone(), catalogs.clone(), guest_limits.clone());
        let (wrapping, previous) = (wrapping_file, args.key_wrapping_previous.clone());
        let (cache_bytes, cache_ttl_ms) = (args.key_cache_bytes, args.key_cache_ttl_ms);
        Arc::new(move |group: &GroupId| {
            let crypto = if group.is_root() {
                base.clone()
            } else {
                let own = key_catalog(wrapping.clone(), &previous, cache_bytes, cache_ttl_ms)?;
                catalogs
                    .lock()
                    .expect("catalogs poisoned")
                    .insert(group.name.to_string(), own.clone());
                Arc::new(flower_crypto::host::CryptoHost::new().with_catalog(own))
            };
            Ok(
                Arc::new(WasmFactory::with_crypto(crypto).with_limits(limits.clone()))
                    as Arc<dyn RuntimeFactory>,
            )
        })
    };
    let runtimes = runtimes_for(&GroupId::root())?;
    // The API listener first: every group advertises its URL.
    let http_listener = match inherited {
        Some(listener) => {
            tokio::net::TcpListener::from_std(listener).map_err(|error| error.to_string())?
        }
        None => tokio::net::TcpListener::bind(&args.http)
            .await
            .map_err(|error| error.to_string())?,
    };
    let http_addr = http_listener
        .local_addr()
        .map_err(|error| error.to_string())?;
    let api_url = advertised_api(args.advertise_http, &http_addr, http_options.tls.is_some());
    let configure: NodeConfigurer = {
        let (catalogs, api_url) = (catalogs.clone(), api_url.clone());
        let (query_cache_bytes, query_flight_bytes, decision_memo_bytes) = (
            args.query_cache_bytes,
            args.query_flight_bytes,
            args.decision_memo_bytes,
        );
        let retry_window = (args.retry_window_ms > 0)
            .then(|| std::time::Duration::from_millis(args.retry_window_ms));
        let speculation = flower_cluster::speculation::SpeculationSettings {
            workers: usize::from(args.writer_preparation_workers),
            prediction: args.writer_conflict_prediction,
        };
        Arc::new(move |group: &GroupId, node: Node| {
            node.reader().set_query_cache_bytes(query_cache_bytes);
            node.set_query_flight_bytes(query_flight_bytes);
            node.reader().set_decision_memo_bytes(decision_memo_bytes);
            node.set_speculation(speculation);
            node.set_api_url(api_url.clone());
            node.set_retry_window(retry_window);
            let own = catalogs
                .lock()
                .expect("catalogs poisoned")
                .get(&*group.name)
                .cloned();
            match own {
                Some(catalog) => {
                    // A wrong or missing wrapping key leaves catalog keys
                    // locked on this member: say so instead of failing
                    // calls one by one.
                    if let Err(error) = catalog.validate_ready() {
                        eprintln!("warning: managed keys locked in group {group}: {error:#}");
                    }
                    node.with_key_catalog(catalog)
                }
                None => node,
            }
        })
    };
    // Backups run per group, each under a directory and keystore of its
    // own: the root's where they always were, others' beside them.
    let drivers: Arc<std::sync::Mutex<BTreeMap<String, Arc<Driver>>>> = Arc::default();
    let location = BackupLocation::parse(args.backup_dir, args.backup_url)?;
    let backups = match (location, args.keystore, args.kek_file) {
        (None, None, None) => None,
        (Some(location), Some(keystore), Some(kek_file)) => {
            Some((location, keystore, read_kek(&kek_file)?))
        }
        _ => {
            return Err(
                "backup needs --backup-dir or --backup-url, --keystore and --kek-file together"
                    .to_string(),
            );
        }
    };
    let backup_duty: DutyStarter = {
        let (drivers, runtimes_for) = (drivers.clone(), runtimes_for.clone());
        Arc::new(move |node: Arc<Node>, stopped| {
            let group = node.group().clone();
            let Some((location, keystore, kek)) = &backups else {
                // Backups are off: let go of the log an earlier run held
                // for them.
                return vec![tokio::spawn(async move {
                    if node.logs().purge_hold().enabled {
                        let _ = node.logs().drop_purge_hold().await;
                    }
                })];
            };
            let (location, keystore) = if group.is_root() {
                (location.clone(), keystore.clone())
            } else {
                let mut keystore = keystore.clone().into_os_string();
                keystore.push(format!(".{}", group.name));
                (
                    location.join(&format!("groups/{}", group.name)),
                    PathBuf::from(keystore),
                )
            };
            let runtimes = match runtimes_for(&group) {
                Ok(runtimes) => runtimes,
                Err(error) => {
                    eprintln!("cargo-flower: no backups for group {group}: {error}");
                    return Vec::new();
                }
            };
            let driver = match group_backup_driver(
                &location, &keystore, *kek, part_bytes, &node, runtimes,
            ) {
                Ok(driver) => driver,
                Err(error) => {
                    eprintln!("cargo-flower: no backups for group {group}: {error}");
                    return Vec::new();
                }
            };
            drivers
                .lock()
                .expect("drivers poisoned")
                .insert(group.name.to_string(), driver.clone());
            let continuous = continuous.clone();
            vec![tokio::spawn(async move {
                if let Err(error) = driver.run_continuous(&continuous, stopped).await {
                    eprintln!("cargo-flower: continuous backup of group {group} stopped: {error}");
                }
            })]
        })
    };
    let settings = HostSettings {
        raft: Arc::new(config),
        durability,
        flush,
        admission: admission.clone(),
        writers: admission,
        reads: Some(ReadSettings::from_timing(&timing)),
        // The leader's sweep fires due timers through consensus; every
        // member purges expired receipts, compacts both files online,
        // snapshots for bytes and age, and with backup on holds its log
        // for the archive the leader writes. Every group runs them, and
        // all stop when it closes.
        duties: GroupDuties {
            maintenance: Some(std::time::Duration::from_millis(args.timer_sweep_ms.max(1))),
            snapshots: Some(snapshot_policy),
            compaction: Some(compaction),
        },
        extra_duties: Some(backup_duty),
        configure: Some(configure),
        runtimes_for: Some(runtimes_for),
        freeze_max: std::time::Duration::from_millis(args.partition_freeze_max_ms.max(1)),
        drive: true,
        leader_placement: args.leader_placement == "on",
    };
    let host = Host::open(
        args.id,
        &args.dir,
        runtimes,
        Transport::Tcp(peers.clone()),
        settings,
    )
    .await
    .map_err(|error| error.to_string())?;
    // Groups this process held when it stopped open again at once; the
    // keeper opens the ones the catalog lists later.
    for group in host.stored_groups().map_err(|error| error.to_string())? {
        if !group.is_root() {
            host.open_group(group)
                .await
                .map_err(|error| error.to_string())?;
        }
    }
    println!("listening raft={raft_addr}");
    host.serve_peers(raft_listener)
        .map_err(|error| error.to_string())?;
    let root = host.root();
    if !args.bootstrap.is_empty() {
        let members = parse_members(&args.bootstrap)?;
        // Restarts pass the same flags: only fresh nodes take
        // the members.
        if !root
            .is_initialized()
            .await
            .map_err(|error| error.to_string())?
        {
            root.initialize_nodes(members)
                .await
                .map_err(|error| error.to_string())?;
        }
    }
    drop(root);
    let host = Arc::new(host);
    host.start();
    let factory: StateFactory = {
        let (host, drivers) = (host.clone(), drivers.clone());
        let (admin_token, worker_token, sync_token) =
            (args.admin_token, args.worker_token, args.sync_token);
        let (deploy_limit, admin_ui) = (args.deploy_max_bytes, args.admin_ui);
        Arc::new(move |node: Arc<Node>| {
            let group = node.group().name.to_string();
            let mut state = AppState::new(node)
                .with_deploy_limit(deploy_limit)
                .with_snapshot_policy(snapshot_policy)
                .with_admin_ui(admin_ui)
                .with_host(host.clone());
            if let Some(token) = &admin_token {
                state = state.with_admin_token(token.clone());
            }
            if let Some(token) = &worker_token {
                state = state.with_worker_token(token.clone());
            }
            if let Some(token) = &sync_token {
                state = state.with_sync_token(token.clone());
            }
            if let Some(driver) = drivers.lock().expect("drivers poisoned").get(&group) {
                state = state.with_backup(driver.clone());
            }
            state
        })
    };
    println!("listening http={http_addr}");
    let api = HostApi::new(host.clone(), factory);
    let drain = std::time::Duration::from_millis(args.shutdown_timeout_ms);
    // A storage failure is permanent for the process: once the disk
    // accepts writes again with room to spare, shut down and exit
    // nonzero so the supervisor restarts the node, which reopens and
    // recovers its files.
    let storage_recovered = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let free_bytes = args.storage_recovery_free_bytes;
    let stop = {
        let (host, recovered) = (host.clone(), storage_recovered.clone());
        async move {
            let recovery = async {
                loop {
                    let nodes = host.groups();
                    let waits = nodes
                        .iter()
                        .map(|node| Box::pin(node.storage_recovered(free_bytes)));
                    let waits: Vec<_> = waits.collect();
                    if waits.is_empty() {
                        std::future::pending::<()>().await;
                    }
                    tokio::select! {
                        _ = select_first(waits) => return,
                        _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {}
                    }
                }
            };
            tokio::select! {
                () = shutdown_signal() => {
                    // Every group this member leads hands its leadership
                    // to a caught-up voter before the API stops, while it
                    // still answers: a planned restart then costs each
                    // group a transfer, not a failover, and writes
                    // meanwhile retry or forward.
                    for (group, outcome) in host.hand_off_leadership().await {
                        match outcome {
                            Ok(Some(outcome)) => println!(
                                "handed group {group} to node {} in {} ms",
                                outcome.to, outcome.elapsed_ms
                            ),
                            Ok(None) => {}
                            Err(error) => eprintln!(
                                "cargo-flower: group {group} leadership hand-off failed: {error}"
                            ),
                        }
                    }
                }
                () = recovery => {
                    recovered.store(true, std::sync::atomic::Ordering::Release);
                }
            }
        }
    };
    let served = serve_host(http_listener, api.clone(), stop, drain, &http_options).await;
    println!("shutting down");
    // HTTP drained and every group's hub stopped: stop following the
    // catalog, then every group (its duties first, then Raft), then the
    // peer listener. Backup holds persist, so a restart keeps holding
    // the logs from open. redb commits are durable, so a second signal
    // or a kill from here on is still crash-safe.
    drop(api);
    host.stop_following().await;
    let stopped = match Arc::try_unwrap(host) {
        Ok(host) => host.shutdown().await,
        Err(host) => {
            for node in host.groups() {
                let _ = node.raft().shutdown().await;
            }
            Ok(())
        }
    };
    served.map_err(|error| error.to_string())?;
    if storage_recovered.load(std::sync::atomic::Ordering::Acquire) {
        return Err(
            "storage failed and the data directory accepts writes again: \
             exiting so a restart reopens the files"
                .to_string(),
        );
    }
    stopped
}

/// The first of `futures` to finish.
async fn select_first<F: std::future::Future<Output = ()> + Unpin>(futures: Vec<F>) {
    let mut futures = futures;
    std::future::poll_fn(move |context| {
        for future in &mut futures {
            if std::pin::Pin::new(future).poll(context).is_ready() {
                return std::task::Poll::Ready(());
            }
        }
        std::task::Poll::Pending
    })
    .await
}

/// Open a group's backup driver over its node's live handles, uploading
/// in parts of `part_bytes`. It leads when the node leads its group.
fn group_backup_driver(
    location: &BackupLocation,
    keystore: &Path,
    kek: [u8; 32],
    part_bytes: usize,
    node: &Node,
    runtimes: Arc<dyn RuntimeFactory>,
) -> Result<Arc<Driver>, String> {
    let raft = node.raft().clone();
    let id = node.status().id;
    let driver = Driver::new(
        location.open(true)?,
        keystore,
        kek,
        node.engine().clone(),
        node.logs().clone(),
        wasm_runtimes(runtimes),
    )
    .map_err(|error| error.to_string())?
    .with_leadership(Arc::new(move || {
        raft.metrics().borrow().current_leader == Some(id)
    }))
    .with_part_bytes(part_bytes);
    Ok(Arc::new(driver))
}

/// Where backup objects live: a directory, or an S3 bucket and prefix.
#[derive(Clone, Debug, PartialEq, Eq)]
enum BackupLocation {
    Dir(PathBuf),
    /// `s3://BUCKET/PREFIX`, without a trailing slash.
    S3(String),
}

impl BackupLocation {
    /// The location `--backup-dir` or `--backup-url` names, if either: a URL
    /// is `s3://BUCKET/PREFIX` or `file:///PATH`.
    fn parse(dir: Option<PathBuf>, url: Option<String>) -> Result<Option<Self>, String> {
        let Some(url) = url else {
            return Ok(dir.map(BackupLocation::Dir));
        };
        if let Some(path) = url.strip_prefix("file://") {
            // `file:///C:/...` names a drive on Windows: the slash before
            // it is the URL's, not the path's.
            let drive = |rest: &str| {
                let bytes = rest.as_bytes();
                bytes.len() >= 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':'
            };
            let path = match path.strip_prefix('/') {
                Some(rest) if cfg!(windows) && drive(rest) => rest,
                _ => path,
            };
            let path = PathBuf::from(path);
            if !path.is_absolute() {
                return Err(format!(
                    "{url}: file:// needs an absolute path (file:///PATH)"
                ));
            }
            return Ok(Some(BackupLocation::Dir(path)));
        }
        if url.starts_with("s3://") {
            return Ok(Some(BackupLocation::S3(
                url.trim_end_matches('/').to_string(),
            )));
        }
        Err(format!("{url}: a backup URL starts with s3:// or file://"))
    }

    /// A location below this one, for a group's own backups.
    fn join(&self, below: &str) -> Self {
        match self {
            BackupLocation::Dir(dir) => BackupLocation::Dir(dir.join(below)),
            BackupLocation::S3(url) => BackupLocation::S3(format!("{url}/{below}")),
        }
    }

    /// The object store here, a directory created when `create` asks.
    /// S3 reads its settings from the environment.
    fn open(&self, create: bool) -> Result<Arc<dyn object_store::ObjectStore>, String> {
        match self {
            BackupLocation::Dir(dir) => {
                if create {
                    std::fs::create_dir_all(dir)
                        .map_err(|error| format!("{}: {error}", dir.display()))?;
                }
                Driver::local_store(dir).map_err(|error| error.to_string())
            }
            BackupLocation::S3(url) => Driver::s3_from_url(url, |name| std::env::var(name).ok())
                .map_err(|error| error.to_string()),
        }
    }
}

/// Read a 32-byte KEK file.
fn read_kek(path: &Path) -> Result<[u8; 32], String> {
    let kek = std::fs::read(path).map_err(|error| format!("{}: {error}", path.display()))?;
    kek.try_into()
        .map_err(|_| "KEK file must hold exactly 32 bytes".to_string())
}

/// Restore rebuilds guest runtimes with the serving backend.
fn wasm_runtimes(factory: Arc<dyn RuntimeFactory>) -> RuntimeBuilder {
    Arc::new(
        move |db: &[u8], module: &str, wasm: &[u8], manifest: &flower_sdk::Value| {
            factory.build(db, module, wasm, manifest)
        },
    )
}

/// The group file inside a node directory, as `Node::open` names it.
const GROUP_FILE: &str = "group.redb";

/// Run `cargo-flower backup`.
async fn run_backup(args: BackupArgs) -> Result<(), String> {
    let Some(op) = args.op else {
        let endpoint = args
            .endpoint
            .ok_or("backup needs --endpoint to back up through a node, or `list` or `restore`")?;
        let client = client(&endpoint, args.admin_token)?;
        let outcome = client
            .backup(&RequestOptions::default())
            .await
            .map_err(|error| error.to_string())?;
        println!(
            "backup_id={} objects={} covered_index={}",
            outcome.backup_id, outcome.objects, outcome.covered_index
        );
        return Ok(());
    };
    match op {
        BackupCommand::Status => {
            let endpoint = args
                .endpoint
                .ok_or("backup status needs --endpoint, the member to ask")?;
            let client = client(&endpoint, args.admin_token)?;
            let status = client
                .backup_status(&RequestOptions::default())
                .await
                .map_err(|error| error.to_string())?;
            println!("{status:#}");
            Ok(())
        }
        BackupCommand::List { store } => {
            let driver = offline_driver(&store, wasm_runtimes(Arc::new(WasmFactory::default())))?;
            driver
                .check_format()
                .await
                .map_err(|error| error.to_string())?;
            for backup in driver
                .list_backups()
                .await
                .map_err(|error| error.to_string())?
            {
                println!(
                    "backup_id={} manifest={} created_ms={} history={:016x} covered_index={} \
                     restorable_through={} restorable_through_ms={} objects={} bytes={}",
                    backup.backup_id,
                    backup.manifest,
                    backup.created_ms,
                    backup.incarnation,
                    backup.covered_index,
                    backup.restorable_through,
                    backup
                        .restorable_through_ms
                        .map_or_else(|| "-".to_string(), |ms| ms.to_string()),
                    backup.objects,
                    backup.ciphertext_bytes,
                );
            }
            Ok(())
        }
        BackupCommand::Restore {
            store,
            guest,
            dir,
            at_ms,
            index,
            history,
            manifest,
        } => {
            let driver = offline_driver(&store, restore_runtimes(&guest)?)?;
            driver
                .check_format()
                .await
                .map_err(|error| error.to_string())?;
            std::fs::create_dir_all(&dir).map_err(|error| error.to_string())?;
            let group = dir.join(GROUP_FILE);
            let failed = |error: flower_backup::BackupError| error.to_string();
            match (at_ms, index, manifest) {
                (Some(at_ms), _, _) => {
                    let summary = driver.pitr(&group, at_ms).await.map_err(failed)?;
                    println!(
                        "restored backup_id={} manifest={} replayed={} reached_ms={}",
                        summary.backup_id, summary.manifest, summary.replayed, summary.reached_ms
                    );
                }
                (None, Some(index), _) => {
                    let history = history
                        .map(|hex| {
                            u64::from_str_radix(&hex, 16)
                                .map_err(|_| format!("history `{hex}` is not hex"))
                        })
                        .transpose()?;
                    let summary = driver
                        .restore_to_index(&group, index, history)
                        .await
                        .map_err(failed)?;
                    println!(
                        "restored backup_id={} manifest={} history={:016x} index={} replayed={} \
                         reached_ms={}",
                        summary.backup_id,
                        summary.manifest,
                        summary.incarnation,
                        summary.index,
                        summary.replayed,
                        summary.reached_ms
                    );
                }
                (None, None, Some(manifest)) => {
                    let summary = driver
                        .restore_backup(&group, &manifest)
                        .await
                        .map_err(failed)?;
                    println!(
                        "restored backup_id={} manifest={} covered_index={}",
                        summary.backup_id, summary.manifest, summary.covered_index
                    );
                }
                (None, None, None) => {
                    let summary = driver.restore_newest(&group).await.map_err(failed)?;
                    println!(
                        "restored backup_id={} manifest={} replayed={} reached_ms={}",
                        summary.backup_id, summary.manifest, summary.replayed, summary.reached_ms
                    );
                }
            }
            println!("start the node with --bootstrap to serve the restored state");
            Ok(())
        }
    }
}

/// An offline driver over a backup location.
fn offline_driver(store: &BackupStoreArgs, runtimes: RuntimeBuilder) -> Result<Driver, String> {
    let location = BackupLocation::parse(store.backup_dir.clone(), store.backup_url.clone())?
        .ok_or("backup needs --backup-dir or --backup-url")?;
    let objects = location.open(false)?;
    Driver::offline(
        objects,
        &store.keystore,
        read_kek(&store.kek_file)?,
        runtimes,
    )
    .map_err(|error| error.to_string())
}

/// Restore registers every module again, running its `flower_init`:
/// under the serving limits and with a crypto host checking key
/// declarations, as live deployment did, so a module the cluster
/// accepted restores too, and one it refused does not.
fn restore_runtimes(guest: &GuestArgs) -> Result<RuntimeBuilder, String> {
    let limits = guest.limits()?;
    wasm::configure(wasm::EngineSettings {
        table_elements: limits.table_elements,
        ..wasm::EngineSettings::default()
    })
    .map_err(|failure| failure.message)?;
    Ok(wasm_runtimes(Arc::new(
        WasmFactory::with_crypto(Arc::new(flower_crypto::host::CryptoHost::new()))
            .with_limits(limits),
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn help_names_the_credential_each_command_takes() {
        use clap::CommandFactory;
        let mut cli = Cli::command();
        for name in [
            "deploy",
            "call",
            "membership",
            "leadership",
            "backup",
            "transaction",
        ] {
            let help = cli
                .find_subcommand_mut(name)
                .unwrap()
                .render_long_help()
                .to_string();
            assert!(!help.contains("REDACTED"), "{name}: {help}");
            assert!(
                help.to_lowercase().contains("bearer token"),
                "{name}: {help}"
            );
        }
    }

    #[tokio::test]
    async fn key_sealing_is_offline_and_import_accepts_only_one_source() {
        let parse = |args: &[&str]| Cli::try_parse_from([&["cargo-flower"][..], args].concat());
        let dir = tempfile::tempdir().unwrap();
        let wrapping = dir.path().join("wrapping");
        let material = dir.path().join("private");
        let out = dir.path().join("sealed.json");
        std::fs::write(&wrapping, [1; 32]).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&wrapping, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        std::fs::write(&material, [2; 32]).unwrap();
        let seal = || KeysCommand::Seal {
            key_file: material.clone(),
            wrapping_file: wrapping.clone(),
            format: "raw".into(),
            out: Some(out.clone()),
        };
        // Even an invalid endpoint is irrelevant to offline sealing.
        run_keys("not a URL", None, None, None, seal())
            .await
            .unwrap();
        assert!(
            run_keys("not a URL", None, None, None, seal())
                .await
                .is_err()
        );
        let sealed: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&out).unwrap()).unwrap();
        let keys = flower_crypto::replicated::ManagedKeys::new(
            Some(flower_crypto::replicated::WrappingKey::new(&[1; 32])),
            vec![],
        );
        assert!(keys.prepare(None, &serde_json::json!({"operation":"import","requestId":"import","name":"key","algorithm":"HS256","sealed":sealed})).is_ok());
        let args = [
            "keys",
            "import",
            "--name",
            "key",
            "--algorithm",
            "HS256",
            "--sealed-file",
            "sealed.json",
        ];
        assert!(parse(&args).is_ok());
        assert!(parse(&[args.as_slice(), &["--key-file", "private"]].concat()).is_err());
        assert!(parse(&[args.as_slice(), &["--wrapping-file", "wrapping"]].concat()).is_err());
        assert!(
            parse(&[
                "keys",
                "import",
                "--name",
                "key",
                "--algorithm",
                "HS256",
                "--key-file",
                "private"
            ])
            .is_err()
        );
    }

    #[test]
    fn keys_retention_and_the_query_cache_parse() {
        let parse = |argv: &[&str]| {
            let mut full = vec!["cargo-flower"];
            full.extend_from_slice(argv);
            Cli::try_parse_from(full).map(|cli| cli.command)
        };
        let Command::Keys { op, request_id, .. } = parse(&["keys", "--endpoint", "e"]).unwrap()
        else {
            panic!("expected keys");
        };
        assert!(op.is_none() && request_id.is_none());
        let Command::Keys {
            op: Some(KeysCommand::Bind { alias, key, usages }),
            db,
            request_id,
            ..
        } = parse(&[
            "keys",
            "--endpoint",
            "e",
            "--db",
            "acme",
            "--request-id",
            "r",
            "bind",
            "--alias",
            "a",
            "--key",
            "k",
            "--usages",
            "sign,verify",
        ])
        .unwrap()
        else {
            panic!("expected keys bind");
        };
        assert_eq!(
            (
                alias.as_str(),
                key.as_str(),
                usages,
                db.as_deref(),
                request_id.as_deref()
            ),
            (
                "a",
                "k",
                vec!["sign".to_string(), "verify".to_string()],
                Some("acme"),
                Some("r")
            )
        );
        // A binding grants something; destroying names a key.
        assert!(
            parse(&[
                "keys",
                "--endpoint",
                "e",
                "bind",
                "--alias",
                "a",
                "--key",
                "k"
            ])
            .is_err()
        );
        assert!(parse(&["keys", "--endpoint", "e", "destroy"]).is_err());
        // The database may follow the subcommand too.
        let Command::Keys { db, op, .. } = parse(&["keys", "list", "--db", "acme"]).unwrap() else {
            panic!("expected keys");
        };
        assert_eq!(db.as_deref(), Some("acme"));
        assert!(matches!(op, Some(KeysCommand::List)));
        let Command::Retention { db, op, .. } = parse(&[
            "retention",
            "--endpoint",
            "e",
            "--db",
            "d",
            "rotate",
            "--keep-epochs",
            "3",
        ])
        .unwrap() else {
            panic!("expected retention");
        };
        assert_eq!(db, "d");
        assert!(matches!(
            op,
            Some(RetentionOp::Rotate {
                epoch_ms: None,
                keep_epochs: 3
            })
        ));
        assert!(parse(&["retention", "--endpoint", "e"]).is_err());
        let serve = |extra: &[&str]| {
            let mut argv = vec!["serve", "--dir", "n", "--id", "1"];
            argv.extend_from_slice(extra);
            match parse(&argv).unwrap() {
                Command::Serve(args) => args.query_cache_bytes,
                _ => panic!("expected serve"),
            }
        };
        assert_eq!(
            serve(&[]),
            flower_engine::query_cache::DEFAULT_QUERY_CACHE_BYTES
        );
        assert_eq!(serve(&["--query-cache-bytes", "0"]), 0);
        let flights = |extra: &[&str]| {
            let mut argv = vec!["serve", "--dir", "n", "--id", "1"];
            argv.extend_from_slice(extra);
            match parse(&argv).unwrap() {
                Command::Serve(args) => args.query_flight_bytes,
                _ => panic!("expected serve"),
            }
        };
        assert_eq!(flights(&[]), flower_cluster::flight::DEFAULT_FLIGHT_BYTES);
        assert_eq!(flights(&["--query-flight-bytes", "0"]), 0);
        let memo = |extra: &[&str]| {
            let mut argv = vec!["serve", "--dir", "n", "--id", "1"];
            argv.extend_from_slice(extra);
            match parse(&argv).unwrap() {
                Command::Serve(args) => args.decision_memo_bytes,
                _ => panic!("expected serve"),
            }
        };
        assert_eq!(
            memo(&[]),
            flower_engine::decisions::DEFAULT_DECISION_MEMO_BYTES
        );
        assert_eq!(memo(&["--decision-memo-bytes", "0"]), 0);
        let speculation = |extra: &[&str]| {
            let mut argv = vec!["serve", "--dir", "n", "--id", "1"];
            argv.extend_from_slice(extra);
            parse(&argv).map(|command| match command {
                Command::Serve(args) => (
                    args.writer_preparation_workers,
                    args.writer_conflict_prediction,
                ),
                _ => panic!("expected serve"),
            })
        };
        assert_eq!(
            speculation(&[]).unwrap(),
            (flower_cluster::speculation::default_workers() as u16, true)
        );
        assert_eq!(
            speculation(&[
                "--writer-preparation-workers",
                "4",
                "--writer-conflict-prediction",
                "0"
            ])
            .unwrap(),
            (4, false)
        );
        assert!(speculation(&["--writer-preparation-workers", "0"]).is_err());
        assert!(speculation(&["--writer-conflict-prediction", "yes"]).is_err());
    }

    /// Retry sessions, scoped IDs, the receipt budget, the default retry
    /// window and request IDs on calls.
    #[test]
    fn retry_sessions_budgets_and_windows_parse() {
        let parse = |argv: &[&str]| {
            let mut full = vec!["cargo-flower"];
            full.extend_from_slice(argv);
            Cli::try_parse_from(full).map(|cli| cli.command)
        };
        let retry = |extra: &[&str]| {
            let mut argv = vec!["retry", "--endpoint", "e", "--db", "d"];
            argv.extend_from_slice(extra);
            parse(&argv).map(|command| match command {
                Command::Retry { op, .. } => op,
                _ => panic!("expected retry"),
            })
        };
        assert!(retry(&[]).unwrap().is_none());
        assert!(matches!(
            retry(&["id", "--intent", "order:1"]).unwrap(),
            Some(RetryOp::Id { intent }) if intent == "order:1"
        ));
        assert!(matches!(
            retry(&["open", "--module", "app"]).unwrap(),
            Some(RetryOp::Open { session: None, .. })
        ));
        let session = "0123456789abcdef0123456789abcdef";
        let Some(RetryOp::Ack {
            module,
            through,
            abandon,
            limit,
            ..
        }) = retry(&[
            "ack",
            "--module",
            "app",
            "--session",
            session,
            "--through",
            "9",
            "--abandon",
            "--limit",
            "2",
        ])
        .unwrap()
        else {
            panic!("expected ack");
        };
        assert_eq!(
            (module.as_str(), through, abandon, limit),
            ("app", 9, true, Some(2))
        );
        // An acknowledgement names its watermark; a session request ID
        // its sequence.
        assert!(retry(&["ack", "--module", "app", "--session", session]).is_err());
        assert!(retry(&["session-id", "--module", "app", "--session", session]).is_err());
        assert!(matches!(
            retry(&[
                "session-id",
                "--module",
                "app",
                "--session",
                session,
                "--sequence",
                "3"
            ])
            .unwrap(),
            Some(RetryOp::SessionId { sequence: 3, .. })
        ));
        let budget = |extra: &[&str]| {
            let mut argv = vec!["retention", "--endpoint", "e", "--db", "d", "budget"];
            argv.extend_from_slice(extra);
            match parse(&argv).unwrap() {
                Command::Retention {
                    op: Some(RetentionOp::Budget { max_bytes }),
                    ..
                } => max_bytes,
                _ => panic!("expected a budget"),
            }
        };
        assert_eq!(budget(&["--max-bytes", "4096"]), Some(4096));
        assert_eq!(budget(&[]), None);
        let window = |extra: &[&str]| {
            let mut argv = vec!["serve", "--dir", "n", "--id", "1"];
            argv.extend_from_slice(extra);
            match parse(&argv).unwrap() {
                Command::Serve(args) => args.retry_window_ms,
                _ => panic!("expected serve"),
            }
        };
        assert_eq!(window(&[]), flower_cluster::node::DEFAULT_RETRY_WINDOW_MS);
        assert_eq!(window(&["--retry-window-ms", "0"]), 0);
        let call = |extra: &[&str]| {
            let mut argv = vec![
                "call",
                "--endpoint",
                "e",
                "--db",
                "d",
                "--module",
                "app",
                "--name",
                "put",
            ];
            argv.extend_from_slice(extra);
            parse(&argv).map(|command| match command {
                Command::Call { request_id, .. } => request_id,
                _ => panic!("expected call"),
            })
        };
        assert_eq!(
            call(&["--mutation", "--request-id", "f1:x"]).unwrap(),
            Some("f1:x".to_string())
        );
        // A query has no request ID to retry under.
        assert!(call(&["--request-id", "f1:x"]).is_err());
    }

    #[test]
    fn modules_list_and_remove_parse() {
        let parse = |argv: &[&str]| {
            let mut full = vec!["cargo-flower"];
            full.extend_from_slice(argv);
            Cli::try_parse_from(full).map(|cli| cli.command)
        };
        let Command::Modules { db, op, .. } = parse(&["modules", "--endpoint", "e"]).unwrap()
        else {
            panic!("expected modules");
        };
        assert!(db.is_none() && op.is_none());
        let removal = |extra: &[&str]| {
            let mut argv = vec![
                "modules",
                "--endpoint",
                "e",
                "--db",
                "d",
                "remove",
                "--module",
                "m",
            ];
            argv.extend_from_slice(extra);
            match parse(&argv).unwrap() {
                Command::Modules {
                    db,
                    op: Some(ModulesCommand::Remove { module, purge }),
                    ..
                } => (db, module, purge),
                _ => panic!("expected modules remove"),
            }
        };
        assert_eq!(
            removal(&[]),
            (Some("d".to_string()), "m".to_string(), false)
        );
        assert!(removal(&["--purge"]).2);
        // A removal names its module.
        assert!(parse(&["modules", "--endpoint", "e", "--db", "d", "remove"]).is_err());
    }

    #[test]
    fn exposed_raft_listeners_need_a_peer_token() {
        let exposed: std::net::SocketAddr = "0.0.0.0:9000".parse().unwrap();
        let loopback: std::net::SocketAddr = "127.0.0.1:9000".parse().unwrap();
        let error = peer_token(None, None, &exposed).unwrap_err();
        assert!(error.contains("FLOWER_PEER_TOKEN"), "{error}");
        assert_eq!(peer_token(None, None, &loopback).unwrap(), None);
        // The admin token is the fallback; an explicit peer token wins; blank
        // tokens are refused.
        assert_eq!(
            peer_token(None, Some("admin"), &exposed)
                .unwrap()
                .as_deref(),
            Some("admin")
        );
        assert_eq!(
            peer_token(Some("peer".to_string()), Some("admin"), &exposed)
                .unwrap()
                .as_deref(),
            Some("peer")
        );
        assert!(peer_token(Some("  ".to_string()), None, &loopback).is_err());
    }

    #[test]
    fn serve_flags_parse_with_env_style_timing() {
        let cli = Cli::try_parse_from([
            "cargo-flower",
            "serve",
            "--dir",
            "d",
            "--id",
            "1",
            "--raft-rtt-ms",
            "100",
            "--raft-max-frame-bytes",
            "1048576",
        ])
        .unwrap();
        let Command::Serve(args) = cli.command else {
            panic!("expected serve");
        };
        assert_eq!(args.raft_rtt_ms, Some(100));
        assert_eq!(args.raft_max_frame_bytes, 1_048_576);
        assert_eq!(args.compaction().unwrap(), CompactionSettings::default());
        assert_eq!(args.raft_queued_input_bytes, None);
        assert_eq!(args.raft_append_timeout_ms, 5_000);
        assert_eq!(args.raft_append_bytes_per_second, 8 * 1024 * 1024);
        assert_eq!(args.raft_read_lease_ms, None);
        assert_eq!(args.advertise_http, None);
        assert_eq!(
            args.storage_recovery_free_bytes,
            flower_cluster::health::DEFAULT_RECOVERY_FREE_BYTES
        );
        let cli = Cli::try_parse_from([
            "cargo-flower",
            "serve",
            "--dir",
            "d",
            "--id",
            "1",
            "--storage-recovery-free-bytes",
            "1048576",
        ])
        .unwrap();
        let Command::Serve(args) = cli.command else {
            panic!("expected serve");
        };
        assert_eq!(args.storage_recovery_free_bytes, 1 << 20);
    }

    #[test]
    fn read_lease_and_advertised_api_flags_parse() {
        let cli = Cli::try_parse_from([
            "cargo-flower",
            "serve",
            "--dir",
            "d",
            "--id",
            "1",
            "--raft-read-lease-ms",
            "0",
            "--advertise-http",
            "https://flower-1.example/",
        ])
        .unwrap();
        let Command::Serve(args) = cli.command else {
            panic!("expected serve");
        };
        assert_eq!(args.raft_read_lease_ms, Some(0));
        let any: std::net::SocketAddr = "0.0.0.0:8080".parse().unwrap();
        let local: std::net::SocketAddr = "127.0.0.1:8080".parse().unwrap();
        assert_eq!(
            advertised_api(args.advertise_http, &any, false).as_deref(),
            Some("https://flower-1.example")
        );
        assert_eq!(advertised_api(None, &any, false), None);
        assert_eq!(
            advertised_api(None, &local, false).as_deref(),
            Some("http://127.0.0.1:8080")
        );
        // A TLS listener advertises https.
        assert_eq!(
            advertised_api(None, &local, true).as_deref(),
            Some("https://127.0.0.1:8080")
        );
    }

    #[test]
    fn peer_tls_takes_all_three_files_or_none() {
        let fixtures = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("flower-cluster")
            .join("tests")
            .join("fixtures")
            .join("tls");
        let file = |name: &str| fixtures.join(name).to_str().unwrap().to_string();
        let serve = |extra: &[String]| {
            let mut argv: Vec<String> = ["cargo-flower", "serve", "--dir", "d", "--id", "1"]
                .map(str::to_string)
                .to_vec();
            argv.extend_from_slice(extra);
            let Command::Serve(args) = Cli::try_parse_from(argv).unwrap().command else {
                panic!("expected serve");
            };
            args
        };
        assert!(serve(&[]).peer_tls().unwrap().is_none());
        let all = [
            "--raft-tls-cert".to_string(),
            file("node.crt"),
            "--raft-tls-key".to_string(),
            file("node.key"),
            "--raft-tls-ca".to_string(),
            file("ca.crt"),
        ];
        assert!(serve(&all).peer_tls().unwrap().is_some());
        let error = serve(&all[..4]).peer_tls().unwrap_err();
        assert!(error.contains("together"), "{error}");
        let mismatched = [
            "--raft-tls-cert".to_string(),
            file("node.crt"),
            "--raft-tls-key".to_string(),
            file("wrong.key"),
            "--raft-tls-ca".to_string(),
            file("ca.crt"),
        ];
        assert!(serve(&mismatched).peer_tls().is_err());
    }

    #[test]
    fn api_tls_http2_and_inherited_listener_flags() {
        let fixtures =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../flower-cluster/tests/fixtures/tls");
        let file = |name: &str| fixtures.join(name).to_str().unwrap().to_string();
        let serve = |extra: &[String]| {
            let mut argv: Vec<String> = ["cargo-flower", "serve", "--dir", "d", "--id", "1"]
                .map(str::to_string)
                .to_vec();
            argv.extend_from_slice(extra);
            let Command::Serve(args) = Cli::try_parse_from(argv).unwrap().command else {
                panic!("expected serve");
            };
            args
        };
        let plain = serve(&[]).http_options().unwrap();
        assert!(plain.tls.is_none() && plain.http2_max_streams.is_none());
        let both = [
            "--tls-cert".to_string(),
            file("node.crt"),
            "--tls-key".to_string(),
            file("node.key"),
            "--http2-max-streams".to_string(),
            "64".to_string(),
        ];
        let set = serve(&both).http_options().unwrap();
        assert!(set.tls.is_some());
        assert_eq!(set.http2_max_streams, Some(64));
        let error = serve(&both[..2]).http_options().unwrap_err();
        assert!(error.contains("together"), "{error}");
        let mismatched = [
            "--tls-cert".to_string(),
            file("node.crt"),
            "--tls-key".to_string(),
            file("wrong.key"),
        ];
        assert!(serve(&mismatched).http_options().is_err());
        assert!(
            serve(&["--http2-max-streams".to_string(), "0".to_string()])
                .http_options()
                .is_err()
        );

        // --listen-fd takes over a socket bound where --http says.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        #[cfg(windows)]
        let raw = {
            use std::os::windows::io::IntoRawSocket;
            listener.into_raw_socket()
        };
        #[cfg(unix)]
        let raw = {
            use std::os::fd::IntoRawFd;
            listener.into_raw_fd() as u64
        };
        let mut inherit = serve(&[
            "--listen-fd".to_string(),
            raw.to_string(),
            "--http".to_string(),
            address.to_string(),
        ]);
        inherit.inherit_listener().unwrap();
        assert_eq!(
            inherit.inherited.as_ref().unwrap().local_addr().unwrap(),
            address
        );
        let mut unnamed = serve(&[
            "--listen-fd".to_string(),
            "7".to_string(),
            "--http".to_string(),
            "localhost:8080".to_string(),
        ]);
        let error = unnamed.inherit_listener().unwrap_err();
        assert!(error.contains("ip:port"), "{error}");
    }

    #[test]
    fn membership_and_leadership_commands_parse() {
        let cli = Cli::try_parse_from([
            "cargo-flower",
            "membership",
            "--endpoint",
            "http://a",
            "--members",
            "1=a:1",
            "--members",
            "2=b:2",
            "--expected-log-id",
            "3-1-42",
        ])
        .unwrap();
        let Command::Membership {
            members,
            expected_log_id,
            ..
        } = cli.command
        else {
            panic!("expected membership");
        };
        assert_eq!(members.len(), 2);
        let log_id = parse_log_id(&expected_log_id.unwrap()).unwrap();
        assert_eq!(
            (
                log_id.leader_id.term,
                log_id.leader_id.node_id,
                log_id.index
            ),
            (3, 1, 42)
        );
        assert_eq!(show_log_id(Some(log_id)), "3-1-42");
        for bad in ["3-1", "a-1-2", "1-2-3-4", ""] {
            assert!(parse_log_id(bad).is_err(), "{bad}");
        }
        let cli = Cli::try_parse_from([
            "cargo-flower",
            "leadership",
            "--endpoint",
            "http://a",
            "--to",
            "2",
        ])
        .unwrap();
        assert!(matches!(cli.command, Command::Leadership { to: 2, .. }));
    }

    #[test]
    fn compaction_flags_parse_and_validate() {
        let serve = |extra: &[&str]| {
            let mut argv = vec!["cargo-flower", "serve", "--dir", "d", "--id", "1"];
            argv.extend_from_slice(extra);
            let Command::Serve(args) = Cli::try_parse_from(argv).unwrap().command else {
                panic!("expected serve");
            };
            args
        };
        let set = serve(&[
            "--compact-ratio",
            "3",
            "--compact-step-pages",
            "64",
            "--compact-min-file-bytes",
            "0",
            "--compact-check-ms",
            "2500",
            "--compact-duty-percent",
            "50",
        ])
        .compaction()
        .unwrap();
        assert_eq!(
            set,
            CompactionSettings {
                ratio: 3,
                step_pages: 64,
                min_file_bytes: 0,
                check: std::time::Duration::from_millis(2500),
                duty_percent: 50,
            }
        );
        assert!(
            !serve(&["--compact-ratio", "0"])
                .compaction()
                .unwrap()
                .enabled()
        );
        for bad in [
            ["--compact-ratio", "1"],
            ["--compact-step-pages", "0"],
            ["--compact-check-ms", "0"],
            ["--compact-duty-percent", "0"],
            ["--compact-duty-percent", "101"],
        ] {
            assert!(serve(&bad).compaction().is_err(), "{bad:?}");
        }
    }

    #[test]
    fn backup_flags_parse_and_validate() {
        let serve = |extra: &[&str]| {
            let mut argv = vec!["cargo-flower", "serve", "--dir", "d", "--id", "1"];
            argv.extend_from_slice(extra);
            let Command::Serve(args) = Cli::try_parse_from(argv).unwrap().command else {
                panic!("expected serve");
            };
            args
        };
        let defaults = serve(&[]).continuous().unwrap();
        assert_eq!(
            defaults,
            ContinuousSettings {
                archive_interval: std::time::Duration::from_secs(1),
                base_interval: std::time::Duration::from_secs(24 * 60 * 60),
                ..ContinuousSettings::default()
            }
        );
        assert_eq!(defaults.max_held, None);
        // Default cap on the bytes held for the archive.
        assert_eq!(defaults.max_held_bytes, Some(1 << 30));
        // The base-by-bytes trigger and part size, the 30-day window retained
        // hourly, and its minute between failed bases.
        assert_eq!(defaults.base_bytes, Some(64 << 20));
        assert_eq!(
            (
                defaults.retention,
                defaults.retention_interval,
                defaults.base_retry
            ),
            (
                std::time::Duration::from_secs(30 * 24 * 60 * 60),
                std::time::Duration::from_secs(60 * 60),
                std::time::Duration::from_secs(60)
            )
        );
        assert_eq!(serve(&[]).backup_part_bytes().unwrap(), 16 << 20);
        let set = serve(&[
            "--backup-archive-ms",
            "250",
            "--backup-base-ms",
            "60000",
            "--backup-base-bytes",
            "0",
            "--backup-retention-ms",
            "86400000",
            "--backup-retention-interval-ms",
            "600000",
            "--backup-hold-max-entries",
            "100000",
            "--backup-hold-max-bytes",
            "0",
            "--shutdown-timeout-ms",
            "60000",
        ])
        .continuous()
        .unwrap();
        assert_eq!(
            set,
            ContinuousSettings {
                archive_interval: std::time::Duration::from_millis(250),
                base_interval: std::time::Duration::from_secs(60),
                base_bytes: None,
                retention: std::time::Duration::from_secs(24 * 60 * 60),
                retention_interval: std::time::Duration::from_secs(600),
                base_retry: std::time::Duration::from_secs(60),
                max_held: Some(100_000),
                max_held_bytes: None,
                // A sixth of the shutdown budget.
                final_archive_within: std::time::Duration::from_secs(10),
            }
        );
        for bad in [
            ["--backup-archive-ms", "0"],
            ["--backup-base-ms", "0"],
            ["--backup-retention-ms", "0"],
            ["--backup-retention-interval-ms", "0"],
        ] {
            assert!(serve(&bad).continuous().is_err(), "{bad:?}");
        }
        assert_eq!(
            serve(&["--backup-base-bytes", "1000"])
                .continuous()
                .unwrap()
                .base_bytes,
            Some(1000)
        );
        assert_eq!(
            serve(&["--backup-part-bytes", "8388608"])
                .backup_part_bytes()
                .unwrap(),
            8 << 20
        );
        assert!(
            serve(&["--backup-part-bytes", "5242879"])
                .backup_part_bytes()
                .is_err()
        );

        // `backup` keeps backing up through a node, and lists and
        // restores offline with exactly one target.
        let backup = |argv: &[&str]| {
            let mut full = vec!["cargo-flower", "backup"];
            full.extend_from_slice(argv);
            Cli::try_parse_from(full).map(|cli| match cli.command {
                Command::Backup(args) => args,
                _ => panic!("expected backup"),
            })
        };
        let now = backup(&["--endpoint", "http://x"]).unwrap();
        assert_eq!(now.endpoint.as_deref(), Some("http://x"));
        assert!(now.op.is_none());
        let store = ["--backup-dir", "b", "--keystore", "k", "--kek-file", "f"];
        let list = backup(&[&["list"], &store[..]].concat()).unwrap();
        assert!(matches!(list.op, Some(BackupCommand::List { .. })));
        let restore =
            |target: &[&str]| backup(&[&["restore", "--dir", "n"], &store[..], target].concat());
        let Some(BackupCommand::Restore { index, history, .. }) =
            restore(&["--index", "42", "--history", "00000000000000ff"])
                .unwrap()
                .op
        else {
            panic!("expected restore");
        };
        assert_eq!(
            (index, history.as_deref()),
            (Some(42), Some("00000000000000ff"))
        );
        assert!(restore(&["--at-ms", "1", "--index", "2"]).is_err());
        assert!(restore(&["--index", "2", "--manifest", "m"]).is_err());
        assert!(restore(&["--history", "ff"]).is_err());

        // Backups go to a directory or a URL, never both, and `list` and
        // `restore` read either.
        let at_url = serve(&["--backup-url", "s3://bucket/prefix"]);
        assert_eq!(at_url.backup_url.as_deref(), Some("s3://bucket/prefix"));
        let both = ["serve", "--dir", "d", "--id", "1", "--backup-dir", "b"];
        assert!(
            Cli::try_parse_from(
                [
                    &["cargo-flower"],
                    &both[..],
                    &["--backup-url", "s3://bucket"]
                ]
                .concat()
            )
            .is_err()
        );
        let keys = ["--keystore", "k", "--kek-file", "f"];
        let list =
            backup(&[&["list", "--backup-url", "s3://bucket/p"], &keys[..]].concat()).unwrap();
        let Some(BackupCommand::List { store }) = list.op else {
            panic!("expected list");
        };
        assert_eq!(
            BackupLocation::parse(store.backup_dir, store.backup_url).unwrap(),
            Some(BackupLocation::S3("s3://bucket/p".to_string()))
        );
        assert!(backup(&[&["list"], &keys[..]].concat()).is_err());
    }

    #[test]
    fn the_wrapping_key_variable_is_named_after_its_flag() {
        use clap::CommandFactory;
        let cli = Cli::command();
        let serve = cli.find_subcommand("serve").unwrap();
        let flag = serve
            .get_arguments()
            .find(|arg| arg.get_long() == Some("key-wrapping-file"))
            .unwrap();
        assert_eq!(
            flag.get_env().and_then(|name| name.to_str()),
            Some("FLOWER_KEY_WRAPPING_FILE")
        );
    }

    #[cfg(windows)]
    #[test]
    fn standard_file_urls_name_windows_paths() {
        let parse = |url: &str| BackupLocation::parse(None, Some(url.to_string()));
        assert_eq!(
            parse("file:///C:/backups").unwrap(),
            Some(BackupLocation::Dir(PathBuf::from("C:/backups")))
        );
        assert_eq!(
            parse("file://C:/backups").unwrap(),
            Some(BackupLocation::Dir(PathBuf::from("C:/backups")))
        );
    }

    #[test]
    fn backup_locations_parse_and_nest() {
        let parse = |url: &str| BackupLocation::parse(None, Some(url.to_string()));
        assert_eq!(
            parse("s3://bucket/a/b/").unwrap(),
            Some(BackupLocation::S3("s3://bucket/a/b".to_string()))
        );
        let dir = std::env::temp_dir().join("backups");
        assert_eq!(
            parse(&format!("file://{}", dir.display())).unwrap(),
            Some(BackupLocation::Dir(dir.clone()))
        );
        assert!(parse("file://relative/backups").is_err());
        assert!(parse("https://bucket.example/backups").is_err());
        assert_eq!(
            BackupLocation::parse(Some(dir.clone()), None).unwrap(),
            Some(BackupLocation::Dir(dir.clone()))
        );
        assert_eq!(BackupLocation::parse(None, None).unwrap(), None);
        // Other groups back up below the root's location.
        assert_eq!(
            BackupLocation::S3("s3://bucket/a".to_string()).join("groups/g"),
            BackupLocation::S3("s3://bucket/a/groups/g".to_string())
        );
        assert_eq!(
            BackupLocation::Dir(dir.clone()).join("groups/g"),
            BackupLocation::Dir(dir.join("groups/g"))
        );
    }

    #[test]
    fn snapshot_trigger_flags_parse_and_validate() {
        let serve = |extra: &[&str]| {
            let mut argv = vec!["cargo-flower", "serve", "--dir", "d", "--id", "1"];
            argv.extend_from_slice(extra);
            let Command::Serve(args) = Cli::try_parse_from(argv).unwrap().command else {
                panic!("expected serve");
            };
            args
        };
        assert_eq!(
            serve(&[]).snapshot_policy().unwrap(),
            SnapshotPolicy::default()
        );
        let set = serve(&[
            "--snapshot-after-bytes",
            "0",
            "--snapshot-max-age-ms",
            "60000",
            "--snapshot-check-ms",
            "250",
            "--snapshot-duty-percent",
            "25",
        ])
        .snapshot_policy()
        .unwrap();
        assert_eq!(
            set,
            SnapshotPolicy {
                after_bytes: 0,
                max_age: std::time::Duration::from_secs(60),
                check: std::time::Duration::from_millis(250),
                duty_percent: 25,
            }
        );
        for bad in [
            ["--snapshot-check-ms", "0"],
            ["--snapshot-duty-percent", "0"],
            ["--snapshot-duty-percent", "101"],
        ] {
            assert!(serve(&bad).snapshot_policy().is_err(), "{bad:?}");
        }
        // Lazy leader flush and deferred applies by default; each knob
        // parses, and nonsense is refused.
        assert_eq!(serve(&[]).flush().unwrap(), FlushSettings::default());
        let flush = serve(&[
            "--leader-flush",
            "immediate",
            "--leader-flush-grace-ms",
            "5",
            "--apply-flush-ms",
            "0",
        ])
        .flush()
        .unwrap();
        assert!(!flush.leader.lazy);
        assert_eq!(flush.leader.grace, std::time::Duration::from_millis(5));
        assert_eq!(flush.apply_every, None);
        assert!(serve(&["--leader-flush", "later"]).flush().is_err());
        assert!(serve(&["--leader-flush-interval-ms", "0"]).flush().is_err());
        // Durability is the default; anything but on or off is refused.
        assert!(Durability::parse(Some(&serve(&[]).fsync)).unwrap().fsync);
        let off = serve(&["--fsync", "off"]).fsync;
        assert!(!Durability::parse(Some(&off)).unwrap().fsync);
        let bad = serve(&["--fsync", "sometimes"]).fsync;
        assert!(Durability::parse(Some(&bad)).is_err());
    }

    #[test]
    fn the_admin_dashboard_is_on_unless_turned_off() {
        let parse = |extra: &[&str]| {
            let mut argv = vec!["cargo-flower", "serve", "--dir", "d", "--id", "1"];
            argv.extend_from_slice(extra);
            Cli::try_parse_from(argv).map(|cli| match cli.command {
                Command::Serve(args) => args.admin_ui,
                _ => panic!("expected serve"),
            })
        };
        assert!(parse(&[]).unwrap());
        for (value, on) in [("false", false), ("0", false), ("true", true), ("1", true)] {
            assert_eq!(parse(&["--admin-ui", value]).unwrap(), on, "{value}");
        }
        assert!(parse(&["--admin-ui", "sometimes"]).is_err());
    }

    #[test]
    fn admission_flags_parse_and_validate() {
        let serve = |extra: &[&str]| {
            let mut argv = vec!["cargo-flower", "serve", "--dir", "d", "--id", "1"];
            argv.extend_from_slice(extra);
            let Command::Serve(args) = Cli::try_parse_from(argv).unwrap().command else {
                panic!("expected serve");
            };
            args
        };
        assert_eq!(serve(&[]).admission().unwrap(), AdmissionConfig::default());
        let set = serve(&[
            "--query-workers",
            "3",
            "--control-workers",
            "2",
            "--queued-input-bytes",
            "1048576",
            "--control-queued-input-bytes",
            "2097152",
            "--admission-max-wait-ms",
            "250",
            "--admission-refresh-share-percent",
            "50",
            "--admission-target-ms",
            "20",
            "--admission-interval-ms",
            "200",
            "--evaluation-memory-bytes",
            "1000",
            "--evaluation-memory-budget-bytes",
            "2500",
        ])
        .admission()
        .unwrap();
        assert_eq!(
            set,
            AdmissionConfig {
                query_workers: 3,
                control_workers: 2,
                queued_input_bytes: 1_048_576,
                control_input_bytes: 2_097_152,
                max_wait: std::time::Duration::from_millis(250),
                refresh_share_percent: 50,
                codel_target: std::time::Duration::from_millis(20),
                codel_interval: std::time::Duration::from_millis(200),
                evaluation_memory_bytes: 1000,
                evaluation_memory_budget: Some(2500),
            }
        );
        for bad in [
            ["--query-workers", "0"],
            ["--queued-input-bytes", "10"],
            ["--admission-max-wait-ms", "0"],
            ["--admission-refresh-share-percent", "0"],
            ["--admission-target-ms", "0"],
            ["--evaluation-memory-budget-bytes", "1"],
        ] {
            assert!(serve(&bad).admission().is_err(), "{bad:?}");
        }
    }

    #[test]
    fn guest_runtime_flags_parse_and_validate() {
        let serve = |extra: &[&str]| {
            let mut argv = vec!["cargo-flower", "serve", "--dir", "d", "--id", "1"];
            argv.extend_from_slice(extra);
            let Command::Serve(args) = Cli::try_parse_from(argv).unwrap().command else {
                panic!("expected serve");
            };
            args
        };
        assert_eq!(serve(&[]).guest.limits().unwrap(), wasm::Limits::default());
        assert_eq!(
            serve(&[]).wasm_engine().unwrap(),
            wasm::EngineSettings::default()
        );
        let set = serve(&[
            "--guest-memory-bytes",
            "1048576",
            "--guest-invocation-memory-bytes",
            "4194304",
            "--guest-table-elements",
            "1000",
            "--guest-timeout-ms",
            "250",
            "--guest-max-timeout-ms",
            "2000",
            "--wasm-pool-slots",
            "64",
            "--wasm-keep-resident-bytes",
            "0",
            "--image-cache-bytes",
            "2097152",
            "--code-cache-bytes",
            "0",
        ]);
        assert_eq!(
            set.guest.limits().unwrap(),
            wasm::Limits {
                instance_memory: 1_048_576,
                invocation_memory: 4_194_304,
                table_elements: 1000,
                timeout: std::time::Duration::from_millis(250),
                max_timeout: std::time::Duration::from_millis(2000),
                fuel: wasm::FUEL_PER_CALL,
            }
        );
        // The pool's tables hold what the guest limit allows.
        assert_eq!(
            set.wasm_engine().unwrap(),
            wasm::EngineSettings {
                pool_slots: 64,
                table_elements: 1000,
                keep_resident: 0,
                image_cache_bytes: 2_097_152,
                code_cache_bytes: 0,
            }
        );
        for bad in [
            ["--guest-memory-bytes", "0"],
            ["--guest-memory-bytes", "8589934592"],
            ["--guest-invocation-memory-bytes", "1024"],
            ["--guest-timeout-ms", "0"],
            ["--guest-max-timeout-ms", "1"],
        ] {
            assert!(serve(&bad).guest.limits().is_err(), "{bad:?}");
        }
        for bad in [["--wasm-pool-slots", "0"], ["--image-cache-bytes", "0"]] {
            assert!(serve(&bad).wasm_engine().is_err(), "{bad:?}");
        }
    }

    /// A guest importing `flower.host_call` and exporting the ABI over
    /// 2,064 pages (129 MiB) of memory, one page past the default
    /// instance limit.
    fn large_guest() -> Vec<u8> {
        let mut wasm = b"\0asm\x01\0\0\0".to_vec();
        // Types: 0 = (i32 i32 i32) -> i64, 1 = (i32) -> i32, 2 = () -> i64.
        wasm.extend([
            0x01, 0x11, 0x03, 0x60, 0x03, 0x7f, 0x7f, 0x7f, 0x01, 0x7e, 0x60, 0x01, 0x7f, 0x01,
            0x7f, 0x60, 0x00, 0x01, 0x7e,
        ]);
        wasm.extend([0x02, 0x14, 0x01, 0x06]);
        wasm.extend(b"flower");
        wasm.push(0x09);
        wasm.extend(b"host_call");
        wasm.extend([0x00, 0x00]);
        // Functions 1 alloc, 2 invoke, 3 manifest; 2,064 pages minimum.
        wasm.extend([0x03, 0x04, 0x03, 0x01, 0x00, 0x02]);
        wasm.extend([0x05, 0x04, 0x01, 0x00, 0x90, 0x10]);
        let mut exports = vec![4u8];
        for (name, kind, index) in [
            ("memory", 2u8, 0u8),
            ("flower_alloc", 0, 1),
            ("flower_invoke", 0, 2),
            ("flower_manifest", 0, 3),
        ] {
            exports.push(name.len() as u8);
            exports.extend(name.as_bytes());
            exports.extend([kind, index]);
        }
        wasm.extend([0x07, exports.len() as u8]);
        wasm.extend(exports);
        // Bodies: alloc returns 0; invoke and manifest return 0.
        wasm.extend([
            0x0a, 0x10, 0x03, 0x04, 0x00, 0x41, 0x00, 0x0b, 0x04, 0x00, 0x42, 0x00, 0x0b, 0x04,
            0x00, 0x42, 0x00, 0x0b,
        ]);
        wasm
    }

    #[test]
    fn restore_registers_modules_under_the_guest_limits() {
        let restore = |extra: &[&str]| {
            let mut argv = vec![
                "cargo-flower",
                "backup",
                "restore",
                "--backup-dir",
                "b",
                "--keystore",
                "k",
                "--kek-file",
                "f",
                "--dir",
                "d",
            ];
            argv.extend_from_slice(extra);
            let Command::Backup(BackupArgs {
                op: Some(BackupCommand::Restore { guest, .. }),
                ..
            }) = Cli::try_parse_from(argv).unwrap().command
            else {
                panic!("expected restore");
            };
            restore_runtimes(&guest).unwrap()
        };
        let wasm = large_guest();
        let error = restore(&[])(b"db", "app", &wasm, &flower_sdk::Value::Null)
            .err()
            .unwrap();
        assert!(error.starts_with("out_of_memory"), "{error}");
        // A cluster serving with raised limits deployed it: its restore
        // registers it under the same flags.
        let raised = restore(&["--guest-memory-bytes", "268435456"]);
        assert!(raised(b"db", "app", &wasm, &flower_sdk::Value::Null).is_ok());
    }
}
