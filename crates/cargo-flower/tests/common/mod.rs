use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, BufReader};

pub fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_cargo-flower")
}

/// An operator command with the test cluster's credentials.
pub fn authenticated_command() -> Command {
    let mut command = Command::new(bin());
    command
        .env("FLOWER_ADMIN_TOKEN", "secret")
        .env("FLOWER_WORKER_TOKEN", "secret");
    command
}

/// The common single-node server flags, also used to test startup failures.
pub fn serve_command(node_dir: &Path) -> Command {
    let mut command = authenticated_command();
    command.args(["serve", "--dir"]).arg(node_dir).args([
        "--id",
        "1",
        "--http",
        "127.0.0.1:0",
        "--raft",
        "127.0.0.1:0",
        "--bootstrap",
        "1",
        "--admin-token",
        "secret",
    ]);
    command
}

/// A ready single-node server; dropping its child stops the process.
pub struct Server {
    pub child: tokio::process::Child,
    pub endpoint: String,
}

impl Server {
    pub async fn start(node_dir: &Path, extra: &[&str], envs: &[(&str, &str)]) -> Self {
        let mut child = tokio::process::Command::from(serve_command(node_dir))
            .args(extra)
            .envs(envs.iter().copied())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
        let http = tokio::time::timeout(Duration::from_secs(60), async {
            while let Some(line) = lines.next_line().await.unwrap() {
                if let Some(addr) = line.strip_prefix("listening http=") {
                    return addr.to_string();
                }
            }
            panic!("serve exited before listening");
        })
        .await
        .expect("server did not start listening");
        let endpoint = format!("http://{http}");
        for _ in 0..200 {
            let output = authenticated_command()
                .args(["status", "--endpoint", &endpoint])
                .output()
                .unwrap();
            if output.status.success()
                && String::from_utf8_lossy(&output.stdout).contains("leader=Some(1)")
            {
                return Self { child, endpoint };
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("single node never elected itself");
    }
}

/// Write a key fixture with the permissions required by `WrappingKey::read`.
pub fn write_key(path: &Path, bytes: &[u8]) {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path).unwrap().write_all(bytes).unwrap();
}
