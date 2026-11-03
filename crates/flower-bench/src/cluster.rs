//! A cluster of real `cargo-flower serve` processes on loopback.
//!
//! Each replica is its own process with its own data directory, HTTP
//! port and Raft port; member 1 bootstraps the group. Ports are free
//! ones below every ephemeral range (bound, noted, released), so runs
//! never collide with each other or with fixed demo ports, no outgoing
//! connection takes a killed member's port before it restarts on it,
//! and a replica that loses a race for its port is started again on
//! fresh ones. Every run gets random admin and worker tokens, passed
//! through the environment rather than the command line.
//!
//! Chaos is real: [`Cluster::kill`] terminates a process outright
//! (`TerminateProcess`, `SIGKILL`), [`Cluster::restart`] starts it
//! again from its data directory on the same ports, and
//! [`Cluster::freeze`] stops every thread of a live one
//! (`NtSuspendProcess`, `SIGSTOP`) until [`Cluster::thaw`].
//!
//! The servers never outlive the bench: they run in their own process
//! group, so a terminal's Ctrl-C reaches only the bench, which stops
//! them; dropping the cluster kills them; and on Windows a job object
//! (on Linux the parent-death signal) ends them even if the bench is
//! killed outright. Data directories go when the cluster drops, unless
//! kept.

use std::collections::BTreeMap;
use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};

use crate::client::Member;
use crate::resources::{Usage, child_usage};

/// How long a fresh cluster may take to elect a leader and be ready.
const READY_TIMEOUT: Duration = Duration::from_secs(90);
/// Attempts to start a cluster when a replica loses its port.
const PORT_ATTEMPTS: usize = 4;
/// Where member ports come from: below every ephemeral range (Linux from
/// 32768, Windows and macOS from 49152), so no outgoing connection takes
/// a killed member's port before it starts again on it.
const MEMBER_PORTS: std::ops::Range<u16> = 20_000..32_000;
/// Timer sweep the servers run: ovens ring within this of their bell.
pub const TIMER_SWEEP_MS: u64 = 50;

/// What the servers are started with.
#[derive(Clone, Debug)]
pub struct ClusterSpec {
    /// The `cargo-flower` executable.
    pub binary: PathBuf,
    /// Replicas: 1 or 3.
    pub replicas: u32,
    /// Parent directory for the data directory; the system's temporary
    /// directory when `None`.
    pub data_parent: Option<PathBuf>,
    /// Keep the data directory afterwards.
    pub keep_data: bool,
    /// Name in the data directory's prefix (`flower-bench-<label>-`).
    pub label: String,
    /// Mount a run-scoped key-wrapping key (`--key-wrapping-file`): the
    /// replicated key catalog, and with it the guest crypto host the
    /// authenticated kitchen's hook verifies tokens with.
    pub crypto: bool,
    /// OpenTelemetry export to this local receiver, if any.
    pub otel: Option<OtelExport>,
    /// TLS on every member's API and Raft ports, if any.
    pub tls: Option<TlsFiles>,
}

/// What every member presents on its API and Raft ports, and the root
/// it (and every client of the cluster) trusts.
#[derive(Clone, Debug)]
pub struct TlsFiles {
    /// PEM chain, leaf first, naming `127.0.0.1`.
    pub cert: PathBuf,
    /// The leaf's PEM private key.
    pub key: PathBuf,
    /// The PEM root that signed it.
    pub ca: PathBuf,
}

/// Where and how the servers export OpenTelemetry.
#[derive(Clone, Debug)]
pub struct OtelExport {
    /// The receiver's base URL (`/v1/traces` and `/v1/metrics` follow).
    pub endpoint: String,
    /// Root-span sampling ratio (parent-based).
    pub sample: f64,
}

impl OtelExport {
    /// The servers' environment: export on, OTLP/HTTP JSON to the local
    /// receiver, one-second metric and span batches.
    pub fn env(&self) -> Vec<(&'static str, String)> {
        vec![
            ("FLOWER_OTEL_ENABLED", "1".to_string()),
            ("OTEL_EXPORTER_OTLP_ENDPOINT", self.endpoint.clone()),
            ("OTEL_EXPORTER_OTLP_PROTOCOL", "http/json".to_string()),
            (
                "OTEL_TRACES_SAMPLER",
                "parentbased_traceidratio".to_string(),
            ),
            ("OTEL_TRACES_SAMPLER_ARG", self.sample.to_string()),
            ("OTEL_METRIC_EXPORT_INTERVAL", "1000".to_string()),
            ("OTEL_BSP_SCHEDULE_DELAY", "1000".to_string()),
            ("OTEL_SERVICE_NAME", "flower".to_string()),
        ]
    }

    /// Exporter variables a server must not inherit from the bench's
    /// environment: other endpoints, headers (credentials) and resource
    /// attributes.
    pub const CLEARED: [&'static str; 9] = [
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
        "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
        "OTEL_EXPORTER_OTLP_HEADERS",
        "OTEL_EXPORTER_OTLP_TRACES_HEADERS",
        "OTEL_EXPORTER_OTLP_METRICS_HEADERS",
        "OTEL_RESOURCE_ATTRIBUTES",
        "OTEL_SDK_DISABLED",
        "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL",
        "OTEL_EXPORTER_OTLP_METRICS_PROTOCOL",
    ];
}

/// One server process.
struct Server {
    id: u64,
    http: SocketAddr,
    raft: SocketAddr,
    dir: PathBuf,
    log: PathBuf,
    child: Option<tokio::process::Child>,
    restarts: u32,
    /// The wrapping key this member mounts instead of the cluster's,
    /// from its next start (see [`Cluster::mount_wrapping`]).
    wrapping: Option<PathBuf>,
}

/// The data directory: removed on drop unless kept.
enum Root {
    Temporary(tempfile::TempDir),
    Kept(PathBuf),
}

impl Root {
    fn path(&self) -> &Path {
        match self {
            Root::Temporary(dir) => dir.path(),
            Root::Kept(path) => path,
        }
    }
}

/// A running local cluster.
pub struct Cluster {
    servers: Mutex<Vec<Server>>,
    admin_token: String,
    worker_token: String,
    /// What members present to each other: never the admin token.
    peer_token: String,
    binary: PathBuf,
    root: Mutex<Option<Root>>,
    /// The key-wrapping file every server mounts, with `crypto`.
    wrapping: Option<PathBuf>,
    otel: Option<OtelExport>,
    tls: Option<TlsFiles>,
    #[cfg(windows)]
    job: Option<job::Job>,
}

/// Up to `bytes` random bytes as hex, from the OS-seeded hasher.
pub fn random_hex(bytes: usize) -> String {
    use std::hash::{BuildHasher, Hasher};
    let mut text = String::new();
    while text.len() < bytes * 2 {
        let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
        hasher.write_u128(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |time| time.as_nanos()),
        );
        hasher.write_usize(text.len());
        text.push_str(&format!("{:016x}", hasher.finish()));
    }
    text.truncate(bytes * 2);
    text
}

/// Pairs of free loopback ports from [`MEMBER_PORTS`]: bind them all at
/// once, then release.
fn free_ports(count: usize) -> std::io::Result<Vec<(SocketAddr, SocketAddr)>> {
    use std::hash::{BuildHasher, Hasher};
    let span = u64::from(MEMBER_PORTS.end - MEMBER_PORTS.start);
    let mut listeners = Vec::new();
    let mut tries = 0;
    while listeners.len() < count * 2 {
        tries += 1;
        if tries > 10_000 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AddrInUse,
                format!("no free loopback port in {MEMBER_PORTS:?}"),
            ));
        }
        let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
        hasher.write_usize(tries);
        let port = MEMBER_PORTS.start + (hasher.finish() % span) as u16;
        // Taken (by another run, or by one of ours already): pick again.
        if let Ok(listener) = TcpListener::bind(("127.0.0.1", port)) {
            listeners.push(listener);
        }
    }
    let addresses = listeners
        .iter()
        .map(TcpListener::local_addr)
        .collect::<std::io::Result<Vec<_>>>()?;
    drop(listeners);
    Ok(addresses.chunks(2).map(|pair| (pair[0], pair[1])).collect())
}

impl Cluster {
    /// [`Cluster::start`] taking its spec.
    pub async fn start_owned(spec: ClusterSpec) -> Result<Cluster, String> {
        Cluster::start(&spec).await
    }

    /// Start the replicas and wait until every one is ready and agrees
    /// on a leader.
    pub async fn start(spec: &ClusterSpec) -> Result<Cluster, String> {
        if spec.replicas != 1 && spec.replicas != 3 {
            return Err("--replicas must be 1 or 3".to_string());
        }
        let parent = match &spec.data_parent {
            Some(parent) => {
                std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
                parent.clone()
            }
            None => std::env::temp_dir(),
        };
        let prefix = if spec.label.is_empty() {
            "flower-bench-".to_string()
        } else {
            format!("flower-bench-{}-", spec.label)
        };
        let dir = tempfile::Builder::new()
            .prefix(&prefix)
            .tempdir_in(&parent)
            .map_err(|error| format!("data directory: {error}"))?;
        let wrapping = if spec.crypto {
            use std::io::Write;

            let mut key = [0u8; 32];
            getrandom::getrandom(&mut key).map_err(|error| format!("randomness: {error}"))?;
            let path = dir.path().join("key-wrapping.bin");
            let mut options = std::fs::OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            options
                .open(&path)
                .and_then(|mut file| file.write_all(&key))
                .map_err(|error| format!("{}: {error}", path.display()))?;
            Some(path)
        } else {
            None
        };
        let root = if spec.keep_data {
            Root::Kept(dir.keep())
        } else {
            Root::Temporary(dir)
        };
        let mut cluster = Cluster {
            servers: Mutex::new(Vec::new()),
            admin_token: random_hex(16),
            worker_token: random_hex(16),
            peer_token: random_hex(16),
            binary: spec.binary.clone(),
            root: Mutex::new(Some(root)),
            wrapping,
            otel: spec.otel.clone(),
            tls: spec.tls.clone(),
            #[cfg(windows)]
            job: job::Job::new(),
        };
        let mut last = String::new();
        for attempt in 0..PORT_ATTEMPTS {
            match cluster.launch(spec.replicas, attempt).await {
                Ok(()) => return Ok(cluster),
                Err(Launch::PortTaken(message)) => {
                    last = message;
                    cluster.stop_all().await;
                }
                Err(Launch::Failed(message)) => {
                    cluster.stop_all().await;
                    return Err(message);
                }
            }
        }
        Err(format!("could not bind the cluster's ports: {last}"))
    }

    async fn launch(&mut self, replicas: u32, attempt: usize) -> Result<(), Launch> {
        let ports =
            free_ports(replicas as usize).map_err(|error| Launch::Failed(error.to_string()))?;
        let root = self.root().join(format!("attempt-{attempt}"));
        let servers = ports
            .iter()
            .enumerate()
            .map(|(index, (http, raft))| {
                let id = index as u64 + 1;
                Server {
                    id,
                    http: *http,
                    raft: *raft,
                    dir: root.join(format!("n{id}")),
                    log: root.join(format!("n{id}.log")),
                    child: None,
                    restarts: 0,
                    wrapping: None,
                }
            })
            .collect::<Vec<_>>();
        let bootstrap = servers
            .iter()
            .map(|server| format!("{}={}", server.id, server.raft))
            .collect::<Vec<_>>();
        *self.lock() = servers;
        for index in 0..replicas as usize {
            self.spawn(index, &bootstrap).map_err(Launch::Failed)?;
        }
        self.wait_ready(READY_TIMEOUT).await
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Vec<Server>> {
        self.servers.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn root(&self) -> PathBuf {
        self.root
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .map_or_else(std::env::temp_dir, |root| root.path().to_path_buf())
    }

    /// Start (or restart) one server process.
    fn spawn(&self, index: usize, bootstrap: &[String]) -> Result<(), String> {
        let mut servers = self.lock();
        let server = &mut servers[index];
        std::fs::create_dir_all(&server.dir).map_err(|error| error.to_string())?;
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&server.log)
            .map_err(|error| error.to_string())?;
        let stderr = log.try_clone().map_err(|error| error.to_string())?;
        let mut command = tokio::process::Command::new(&self.binary);
        command
            .arg("serve")
            .arg("--dir")
            .arg(&server.dir)
            .args(["--id", &server.id.to_string()])
            .args(["--http", &server.http.to_string()])
            .args(["--raft", &server.raft.to_string()])
            .args(["--timer-sweep-ms", &TIMER_SWEEP_MS.to_string()])
            .env("FLOWER_ADMIN_TOKEN", &self.admin_token)
            .env("FLOWER_WORKER_TOKEN", &self.worker_token)
            .env("FLOWER_PEER_TOKEN", &self.peer_token)
            .stdin(Stdio::null())
            .stdout(Stdio::from(log))
            .stderr(Stdio::from(stderr))
            .kill_on_drop(true);
        if let Some(tls) = &self.tls {
            command
                .arg("--tls-cert")
                .arg(&tls.cert)
                .arg("--tls-key")
                .arg(&tls.key)
                .arg("--raft-tls-cert")
                .arg(&tls.cert)
                .arg("--raft-tls-key")
                .arg(&tls.key)
                .arg("--raft-tls-ca")
                .arg(&tls.ca);
        }
        if std::env::var_os("RUST_LOG").is_none() {
            command.env("RUST_LOG", "warn");
        }
        if let Some(wrapping) = server.wrapping.as_ref().or(self.wrapping.as_ref()) {
            command.arg("--key-wrapping-file").arg(wrapping);
        }
        if let Some(otel) = &self.otel {
            for name in OtelExport::CLEARED {
                command.env_remove(name);
            }
            for (name, value) in otel.env() {
                command.env(name, value);
            }
        } else {
            // Export only when the run asks: an inherited switch would
            // perturb a capacity run.
            command.env_remove("FLOWER_OTEL_ENABLED");
        }
        // Member 1 founds the group; restarts pass the same flags and
        // an initialized member ignores them.
        if server.id == 1 {
            for member in bootstrap {
                command.args(["--bootstrap", member]);
            }
        }
        isolate(&mut command);
        let child = command
            .spawn()
            .map_err(|error| format!("starting {}: {error}", self.binary.display()))?;
        #[cfg(windows)]
        if let (Some(job), Some(handle)) = (&self.job, child.raw_handle()) {
            job.assign(handle);
        }
        server.child = Some(child);
        Ok(())
    }

    /// Wait until every running server is ready and they agree on a
    /// leader.
    async fn wait_ready(&self, timeout: Duration) -> Result<(), Launch> {
        let deadline = Instant::now() + timeout;
        let http = Probe {
            ca: self.ca_file().map(Path::to_path_buf),
        };
        loop {
            if let Some(failure) = self.exited() {
                return Err(failure);
            }
            if self.ready_once(&http).await {
                return Ok(());
            }
            if Instant::now() > deadline {
                return Err(Launch::Failed(format!(
                    "the cluster was not ready within {}s\n{}",
                    timeout.as_secs(),
                    self.log_tails(20)
                )));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Whether every server answers ready with one leader, once.
    async fn ready_once(&self, http: &Probe) -> bool {
        let urls: Vec<String> = self
            .members()
            .into_iter()
            .map(|member| member.url)
            .collect();
        let mut leaders = Vec::new();
        for url in &urls {
            match http.status(url).await {
                Some(status) if status.leader.is_some() => leaders.push(status.leader),
                _ => return false,
            }
            if !http.ready(url).await {
                return false;
            }
        }
        leaders.windows(2).all(|pair| pair[0] == pair[1])
    }

    /// A server that exited by itself, with why.
    fn exited(&self) -> Option<Launch> {
        let mut servers = self.lock();
        for server in servers.iter_mut() {
            let Some(child) = server.child.as_mut() else {
                continue;
            };
            if let Ok(Some(status)) = child.try_wait() {
                let tail = tail(&server.log, 20);
                let message = format!("member {} exited ({status}):\n{tail}", server.id);
                let taken = ["10048", "address already in use", "only one usage"]
                    .iter()
                    .any(|needle| tail.to_ascii_lowercase().contains(needle));
                return Some(if taken {
                    Launch::PortTaken(message)
                } else {
                    Launch::Failed(message)
                });
            }
        }
        None
    }

    /// The members' API URLs.
    pub fn members(&self) -> Vec<Member> {
        self.lock()
            .iter()
            .map(|server| Member {
                id: server.id,
                url: format!(
                    "{}://{}",
                    if self.tls.is_some() { "https" } else { "http" },
                    server.http
                ),
            })
            .collect()
    }

    /// Member `index`'s Raft (peer protocol) address.
    pub fn raft_addr(&self, index: usize) -> SocketAddr {
        self.lock()[index].raft
    }

    /// The root the members' certificates chain to, with TLS.
    pub fn ca_file(&self) -> Option<&Path> {
        self.tls.as_ref().map(|tls| tls.ca.as_path())
    }

    /// The admin token.
    pub fn admin_token(&self) -> &str {
        &self.admin_token
    }

    /// The token members present to each other.
    pub fn peer_token(&self) -> &str {
        &self.peer_token
    }

    /// The worker token.
    pub fn worker_token(&self) -> &str {
        &self.worker_token
    }

    /// The data directory.
    pub fn data_dir(&self) -> PathBuf {
        self.root()
    }

    /// Whether the data directory outlives the cluster.
    pub fn keeps_data(&self) -> bool {
        matches!(
            *self.root.lock().unwrap_or_else(PoisonError::into_inner),
            Some(Root::Kept(_))
        )
    }

    /// The arguments every server runs with, tokens left out, and the
    /// OpenTelemetry variables set.
    pub fn server_args(&self) -> Vec<String> {
        let mut args = vec![
            "serve".to_string(),
            "--http 127.0.0.1:<port>".to_string(),
            "--raft 127.0.0.1:<port>".to_string(),
            format!("--timer-sweep-ms {TIMER_SWEEP_MS}"),
        ];
        if self.wrapping.is_some() {
            args.push("--key-wrapping-file <run key>".to_string());
        }
        if let Some(otel) = &self.otel {
            for (name, value) in otel.env() {
                args.push(format!("{name}={value}"));
            }
        }
        args
    }

    /// Member `index`'s process ID, while it runs.
    pub fn pid(&self, index: usize) -> Option<u32> {
        self.lock()
            .get(index)
            .and_then(|server| server.child.as_ref())
            .and_then(tokio::process::Child::id)
    }

    /// Kill member `index` outright; returns its process ID.
    pub async fn kill(&self, index: usize) -> Option<u32> {
        let child = self.lock()[index].child.take();
        let mut child = child?;
        let pid = child.id();
        let _ = child.start_kill();
        let _ = child.wait().await;
        pid
    }

    /// Ask one isolated server to drain and exit, then wait for its exit
    /// status. A timeout restores the child handle so cleanup can kill it.
    pub async fn stop_gracefully(
        &self,
        index: usize,
        timeout: Duration,
    ) -> Result<std::process::ExitStatus, String> {
        let mut child = self.lock()[index]
            .child
            .take()
            .ok_or("member is not running")?;
        let Some(pid) = child.id() else {
            return Err("member has no process ID".into());
        };
        if let Err(error) = signal_stop(pid) {
            self.lock()[index].child = Some(child);
            return Err(error.to_string());
        }
        match tokio::time::timeout(timeout, child.wait()).await {
            Ok(Ok(status)) => Ok(status),
            result => {
                self.lock()[index].child = Some(child);
                Err(format!("waiting for graceful stop: {result:?}"))
            }
        }
    }

    /// Have member `index` mount the wrapping key in `path` from its
    /// next start, instead of the cluster's (`None`: the cluster's
    /// again). A member with another key applies every key catalog but
    /// unwraps none of it, as a member mounted with the wrong key file
    /// does.
    pub fn mount_wrapping(&self, index: usize, path: Option<PathBuf>) {
        self.lock()[index].wrapping = path;
    }

    /// Start member `index` again from its data directory, on its ports.
    pub fn restart(&self, index: usize) -> Result<(), String> {
        let bootstrap = self
            .lock()
            .iter()
            .map(|server| format!("{}={}", server.id, server.raft))
            .collect::<Vec<_>>();
        self.spawn(index, &bootstrap)?;
        self.lock()[index].restarts += 1;
        Ok(())
    }

    /// Freeze member `index` in place: every thread stops and every socket
    /// stays open, so peers and clients reach a member that accepts
    /// connections and never answers. Returns whether a running process was
    /// frozen.
    pub fn freeze(&self, index: usize) -> bool {
        self.lock()[index]
            .child
            .as_ref()
            .is_some_and(|child| pause(child, true))
    }

    /// Let member `index` run again after [`Cluster::freeze`].
    pub fn thaw(&self, index: usize) -> bool {
        self.lock()[index]
            .child
            .as_ref()
            .is_some_and(|child| pause(child, false))
    }

    /// Whether member `index` has a running process.
    pub fn is_running(&self, index: usize) -> bool {
        let mut servers = self.lock();
        match servers[index].child.as_mut() {
            Some(child) => matches!(child.try_wait(), Ok(None)),
            None => false,
        }
    }

    /// Each running member's process ID and counters, by member index.
    pub fn usage(&self) -> BTreeMap<usize, (u32, Usage)> {
        let servers = self.lock();
        servers
            .iter()
            .enumerate()
            .filter_map(|(index, server)| {
                let child = server.child.as_ref()?;
                Some((index, (child.id()?, child_usage(child)?)))
            })
            .collect()
    }

    /// The last `lines` lines of every member's log.
    pub fn log_tails(&self, lines: usize) -> String {
        self.lock()
            .iter()
            .map(|server| format!("--- member {} ---\n{}", server.id, tail(&server.log, lines)))
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// Stop every server and wait for each to exit.
    pub async fn stop_all(&self) {
        let children: Vec<_> = self
            .lock()
            .iter_mut()
            .filter_map(|server| server.child.take())
            .collect();
        for mut child in children {
            let _ = child.start_kill();
            let _ = child.wait().await;
        }
    }

    /// Stop every server and remove the data directory unless kept,
    /// retrying while the OS releases the files.
    pub async fn shutdown(&self) {
        self.stop_all().await;
        let root = self
            .root
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(Root::Temporary(dir)) = root {
            let path = dir.keep();
            for _ in 0..20 {
                if std::fs::remove_dir_all(&path).is_ok() || !path.exists() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        // Reached without `shutdown` only when a run is abandoned (a
        // second Ctrl-C): kill what still runs and give the processes a
        // moment to exit, so the data directory's own drop can remove
        // their files.
        let mut servers = self.lock();
        for server in servers.iter_mut() {
            if let Some(child) = server.child.as_mut() {
                let _ = child.start_kill();
            }
        }
        let deadline = Instant::now() + Duration::from_secs(3);
        for server in servers.iter_mut() {
            if let Some(child) = server.child.as_mut() {
                while matches!(child.try_wait(), Ok(None)) && Instant::now() < deadline {
                    std::thread::sleep(Duration::from_millis(20));
                }
            }
        }
    }
}

/// Why a launch failed.
enum Launch {
    /// A member lost the race for its port: try fresh ports.
    PortTaken(String),
    /// Anything else.
    Failed(String),
}

/// Put a server in its own process group, so a terminal's Ctrl-C
/// reaches only the bench, and on Linux end it with the bench.
fn isolate(command: &mut tokio::process::Command) {
    #[cfg(windows)]
    {
        /// `CREATE_NEW_PROCESS_GROUP`.
        const NEW_PROCESS_GROUP: u32 = 0x0000_0200;
        command.creation_flags(NEW_PROCESS_GROUP);
    }
    #[cfg(unix)]
    {
        command.process_group(0);
        #[cfg(target_os = "linux")]
        // SAFETY: the closure runs in the child between fork and exec and
        // calls only `prctl`, which is async-signal-safe.
        unsafe {
            command.pre_exec(|| {
                if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
    }
}

fn signal_stop(pid: u32) -> std::io::Result<()> {
    #[cfg(windows)]
    {
        #[link(name = "kernel32")]
        unsafe extern "system" {
            fn GenerateConsoleCtrlEvent(event: u32, group: u32) -> i32;
        }
        // SAFETY: the server is the leader of its isolated process group;
        // Ctrl-Break (1) targets only that group in the shared console.
        if unsafe { GenerateConsoleCtrlEvent(1, pid) } == 0 {
            return Err(std::io::Error::last_os_error());
        }
    }
    #[cfg(unix)]
    {
        // SAFETY: signal the live child PID, never the caller's process group.
        if unsafe { libc::kill(pid as i32, libc::SIGTERM) } == -1 {
            return Err(std::io::Error::last_os_error());
        }
    }
    Ok(())
}

/// Stop (`frozen`) or resume every thread of a child process.
fn pause(child: &tokio::process::Child, frozen: bool) -> bool {
    #[cfg(windows)]
    {
        let Some(handle) = child.raw_handle() else {
            return false;
        };
        // SAFETY: the handle is the live child's, which `CreateProcess`
        // opened with full access, `PROCESS_SUSPEND_RESUME` included.
        let status = unsafe {
            if frozen {
                suspension::NtSuspendProcess(handle.cast())
            } else {
                suspension::NtResumeProcess(handle.cast())
            }
        };
        status >= 0
    }
    #[cfg(unix)]
    {
        let Some(pid) = child.id() else {
            return false;
        };
        let signal = if frozen { libc::SIGSTOP } else { libc::SIGCONT };
        // SAFETY: signals the child's own process ID, which stays
        // reserved while the unreaped `Child` holds it.
        unsafe { libc::kill(pid as libc::pid_t, signal) == 0 }
    }
}

/// Whole-process suspension, which Windows has only in `ntdll`: the
/// call Sysinternals' `pssuspend` makes. It suspends every thread at
/// once, so no thread the process starts meanwhile escapes.
#[cfg(windows)]
mod suspension {
    use windows_sys::Win32::Foundation::HANDLE;

    #[link(name = "ntdll")]
    unsafe extern "system" {
        pub(super) fn NtSuspendProcess(process: HANDLE) -> i32;
        pub(super) fn NtResumeProcess(process: HANDLE) -> i32;
    }
}

/// The last `lines` lines of a file.
fn tail(path: &Path, lines: usize) -> String {
    let text = std::fs::read(path).unwrap_or_default();
    let text = String::from_utf8_lossy(&text);
    let all: Vec<&str> = text.lines().collect();
    all[all.len().saturating_sub(lines)..].join("\n")
}

/// Status and readiness probes through the client crate's transport.
struct Probe {
    /// The root to trust, with TLS.
    ca: Option<PathBuf>,
}

impl Probe {
    async fn get(&self, url: &str, path: &str) -> Option<(u16, Vec<u8>)> {
        use flower_client::transport::{HttpTransport, Transport, TransportOptions};
        let mut options = TransportOptions::new().connect_timeout(Duration::from_millis(500));
        if let Some(ca) = &self.ca {
            options = options.ca_file(ca);
        }
        let transport = HttpTransport::with_options(url.to_string(), options).ok()?;
        let reply = tokio::time::timeout(Duration::from_secs(2), transport.get(path, None))
            .await
            .ok()?
            .ok()?;
        Some((reply.status, reply.body))
    }

    async fn status(&self, url: &str) -> Option<flower_client::client::ServerStatus> {
        let (status, body) = self.get(url, "/v1/status").await?;
        (status == 200)
            .then(|| serde_json::from_slice(&body).ok())
            .flatten()
    }

    async fn ready(&self, url: &str) -> bool {
        matches!(self.get(url, "/health/ready").await, Some((200, _)))
    }
}

#[cfg(windows)]
mod job {
    use windows_sys::Win32::Foundation::{CloseHandle, HANDLE};
    use windows_sys::Win32::System::JobObjects::{
        AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
        JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectExtendedLimitInformation,
        SetInformationJobObject,
    };

    /// A job object that kills its processes when its last handle
    /// closes, which happens when the bench exits however it exits.
    pub(super) struct Job(HANDLE);

    // SAFETY: a job handle is a kernel object handle, usable from any
    // thread.
    unsafe impl Send for Job {}
    // SAFETY: as above; the methods take `&self` and the kernel
    // serializes operations on the job.
    unsafe impl Sync for Job {}

    impl Job {
        pub(super) fn new() -> Option<Job> {
            // SAFETY: null attributes and name create an anonymous job
            // with default security.
            let handle = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
            if handle.is_null() {
                return None;
            }
            let job = Job(handle);
            let mut limits = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
            limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
            // SAFETY: `limits` is a valid struct of the class's type and
            // its exact size is passed.
            let ok = unsafe {
                SetInformationJobObject(
                    job.0,
                    JobObjectExtendedLimitInformation,
                    (&limits as *const JOBOBJECT_EXTENDED_LIMIT_INFORMATION).cast(),
                    std::mem::size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
                )
            };
            (ok != 0).then_some(job)
        }

        /// Put a process in the job; failures (a process already in a
        /// job that forbids it) leave it to the other cleanups.
        pub(super) fn assign(&self, process: std::os::windows::io::RawHandle) {
            // SAFETY: both handles are live: the job for `self`'s life,
            // the process for the caller's `Child`.
            unsafe {
                AssignProcessToJobObject(self.0, process as HANDLE);
            }
        }
    }

    impl Drop for Job {
        fn drop(&mut self) {
            // SAFETY: the handle is owned and closed once.
            unsafe {
                CloseHandle(self.0);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn free_ports_are_distinct_pairs() {
        let ports = free_ports(3).unwrap();
        assert_eq!(ports.len(), 3);
        let mut all: Vec<u16> = ports
            .iter()
            .flat_map(|(http, raft)| [http.port(), raft.port()])
            .collect();
        all.sort_unstable();
        all.dedup();
        assert_eq!(all.len(), 6);
    }

    #[test]
    fn free_ports_stay_below_every_ephemeral_range() {
        // An outgoing connection never takes one while its member is
        // down: Linux draws from 32768 up, Windows and macOS from 49152.
        for _ in 0..20 {
            for (http, raft) in free_ports(3).unwrap() {
                assert!(MEMBER_PORTS.contains(&http.port()), "{http}");
                assert!(MEMBER_PORTS.contains(&raft.port()), "{raft}");
                assert!(raft.port() < 32_768);
            }
        }
    }

    #[test]
    fn tokens_are_random_hex() {
        let token = random_hex(16);
        assert_eq!(token.len(), 32);
        assert!(token.bytes().all(|byte| byte.is_ascii_hexdigit()));
        assert_ne!(token, random_hex(16));
    }
}
