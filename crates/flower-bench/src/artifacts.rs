//! The binaries a run uses: the `cargo-flower` server, the Goblin Pizza
//! guest and the bench itself, each identified by its SHA-256.
//!
//! Without `--server-binary`, the bench builds `cargo-flower` with the
//! profile it was built with itself (release under `cargo run
//! --release`), from the workspace it was compiled in, and uses that.
//! Without `--guest-wasm`, it builds the guest for `wasm32` in release
//! into `target/wasm-guest`, the directory the workspace's own tests
//! build guests into. Both builds are no-ops when up to date.

use std::path::{Path, PathBuf};
use std::process::Command;

use serde::Serialize;
use sha2::{Digest, Sha256};

/// One binary's identity.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Fingerprint {
    /// Where it was read from.
    pub path: String,
    /// SHA-256 of its bytes, hex.
    pub sha256: String,
    /// Size in bytes.
    pub bytes: u64,
}

/// Hash a file.
pub fn fingerprint(path: &Path) -> Result<Fingerprint, String> {
    let bytes = std::fs::read(path).map_err(|error| format!("{}: {error}", path.display()))?;
    Ok(Fingerprint {
        path: path.display().to_string(),
        sha256: hex(&Sha256::digest(&bytes)),
        bytes: bytes.len() as u64,
    })
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// The workspace this crate was compiled in, if it is still there.
pub fn workspace_root() -> Option<PathBuf> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent()?.parent()?;
    root.join("Cargo.toml")
        .is_file()
        .then(|| root.to_path_buf())
}

fn cargo() -> String {
    std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string())
}

/// Build `cargo-flower` with this binary's own profile and return the
/// executable Cargo reports.
pub fn build_server() -> Result<PathBuf, String> {
    let root = workspace_root()
        .ok_or("the Flower workspace this bench was built in is gone: pass --server-binary")?;
    let mut command = Command::new(cargo());
    command
        .current_dir(&root)
        .args([
            "build",
            "--package",
            "cargo-flower",
            "--bin",
            "cargo-flower",
            "--message-format",
            "json-render-diagnostics",
        ])
        .stderr(std::process::Stdio::inherit());
    if !cfg!(debug_assertions) {
        command.arg("--release");
    }
    let output = command
        .output()
        .map_err(|error| format!("running cargo: {error}"))?;
    if !output.status.success() {
        return Err("building cargo-flower failed".to_string());
    }
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .filter(|message| {
            message["reason"] == "compiler-artifact" && message["target"]["name"] == "cargo-flower"
        })
        .find_map(|message| message["executable"].as_str().map(PathBuf::from))
        .ok_or_else(|| "cargo reported no cargo-flower executable".to_string())
}

/// Build the Goblin Pizza guest for `wasm32` in release and return the
/// module's path: with `auth`, the authenticated kitchen (feature
/// `auth`), into its own target directory so the two builds never
/// replace each other's module.
pub fn build_guest(auth: bool) -> Result<PathBuf, String> {
    let root = workspace_root()
        .ok_or("the Flower workspace this bench was built in is gone: pass --guest-wasm")?;
    let target = root.join("target").join(if auth {
        "wasm-guest-auth"
    } else {
        "wasm-guest"
    });
    let mut command = Command::new(cargo());
    command.current_dir(&root).args([
        "build",
        "--quiet",
        "--release",
        "--target",
        "wasm32-unknown-unknown",
        "--package",
        "goblin-pizza",
    ]);
    if auth {
        command.args(["--features", "auth"]);
    }
    let status = command
        .env("CARGO_TARGET_DIR", &target)
        .status()
        .map_err(|error| format!("running cargo: {error}"))?;
    if !status.success() {
        return Err("building the goblin-pizza guest for wasm32 failed".to_string());
    }
    Ok(target
        .join("wasm32-unknown-unknown")
        .join("release")
        .join("goblin_pizza.wasm"))
}

/// The manifest a guest declares, as the server's backend reads it.
pub fn manifest(wasm: &[u8]) -> Result<serde_json::Value, String> {
    use flower_engine::runtime::wasm::WasmBackend;
    let mut backend = WasmBackend::new().map_err(|error| format!("{error:?}"))?;
    backend
        .register("pizza", wasm)
        .map_err(|error| format!("{error:?}"))?;
    let manifest = backend
        .manifest("pizza")
        .map_err(|error| format!("{error:?}"))?;
    let rendered = flower_engine::json::render(&manifest).map_err(|error| format!("{error:?}"))?;
    serde_json::from_slice(&rendered).map_err(|error| error.to_string())
}

/// Machine and toolchain facts a report keeps.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Machine {
    /// Operating system family.
    pub os: &'static str,
    /// CPU architecture.
    pub arch: &'static str,
    /// Logical CPUs available to the bench.
    pub logical_cpus: usize,
    /// CPU model, where the OS names it.
    pub cpu: Option<String>,
    /// Build profile of the bench (and of a server it built).
    pub profile: &'static str,
    /// The workspace's Git commit, when it is a checkout.
    pub commit: Option<String>,
}

/// Describe this machine.
pub fn machine() -> Machine {
    Machine {
        os: std::env::consts::OS,
        arch: std::env::consts::ARCH,
        logical_cpus: std::thread::available_parallelism().map_or(1, |count| count.get()),
        cpu: cpu_model(),
        profile: if cfg!(debug_assertions) {
            "dev"
        } else {
            "release"
        },
        commit: workspace_root().and_then(|root| {
            let output = Command::new("git")
                .current_dir(root)
                .args(["rev-parse", "HEAD"])
                .output()
                .ok()?;
            output
                .status
                .success()
                .then(|| String::from_utf8_lossy(&output.stdout).trim().to_string())
        }),
    }
}

fn cpu_model() -> Option<String> {
    if let Ok(name) = std::env::var("PROCESSOR_IDENTIFIER") {
        return Some(name);
    }
    let info = std::fs::read_to_string("/proc/cpuinfo").ok()?;
    info.lines()
        .find(|line| line.starts_with("model name"))
        .and_then(|line| line.split_once(':'))
        .map(|(_, name)| name.trim().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fingerprints_hash_contents() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("x");
        std::fs::write(&path, b"abc").unwrap();
        let print = fingerprint(&path).unwrap();
        assert_eq!(
            print.sha256,
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(print.bytes, 3);
        assert!(fingerprint(&dir.path().join("missing")).is_err());
    }
}
