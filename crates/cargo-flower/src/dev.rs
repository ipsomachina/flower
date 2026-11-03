//! Local source watcher. Each successful build uses the ordinary deployment preflight.
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

#[derive(clap::Args, Debug)]
pub struct Options {
    #[arg(long)]
    pub package: String,
    #[arg(long)]
    pub db: String,
    #[arg(long)]
    pub module: String,
    /// Source tree to watch; target, .git and .flower directories are excluded.
    #[arg(long, default_value = ".")]
    pub watch: PathBuf,
    #[arg(long, env = "FLOWER_ENDPOINT", default_value = "http://127.0.0.1:8080")]
    pub endpoint: String,
    /// Build and deploy once, then exit.
    #[arg(long)]
    pub once: bool,
}

fn sources(root: &Path) -> Result<BTreeMap<PathBuf, Vec<u8>>, String> {
    let mut pending = vec![root.to_path_buf()];
    let mut result = BTreeMap::new();
    while let Some(directory) = pending.pop() {
        for item in std::fs::read_dir(directory).map_err(|e| e.to_string())? {
            let item = item.map_err(|e| e.to_string())?;
            let kind = item.file_type().map_err(|e| e.to_string())?;
            if kind.is_symlink() {
                continue;
            }
            let path = item.path();
            if kind.is_dir() {
                if !matches!(
                    item.file_name().to_str(),
                    Some("target" | ".git" | ".flower")
                ) {
                    pending.push(path);
                }
            } else if matches!(
                path.extension().and_then(|s| s.to_str()),
                Some("rs" | "toml" | "lock")
            ) {
                result.insert(
                    path.clone(),
                    Sha256::digest(std::fs::read(path).map_err(|e| e.to_string())?).to_vec(),
                );
            }
        }
    }
    Ok(result)
}

async fn child(root: &Path, args: &[&str]) -> Result<(), String> {
    let mut command =
        tokio::process::Command::new(std::env::current_exe().map_err(|e| e.to_string())?);
    command.current_dir(root).args(args).kill_on_drop(true);
    let status = command.status().await.map_err(|e| e.to_string())?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("flower {} exited with {status}", args[0]))
    }
}

pub async fn run(options: Options) -> Result<(), String> {
    let root = options.watch.canonicalize().map_err(|e| e.to_string())?;
    let mut previous = sources(&root)?;
    let wasm = format!(".flower/{}.wasm", options.package);
    loop {
        let iteration = async {
            child(
                &root,
                &["build", "--package", &options.package, "--profile", "dev"],
            )
            .await?;
            child(
                &root,
                &[
                    "deploy",
                    "--db",
                    &options.db,
                    "--module",
                    &options.module,
                    "--wasm",
                    &wasm,
                    "--endpoint",
                    &options.endpoint,
                ],
            )
            .await
        };
        let outcome = tokio::select! {
            outcome = iteration => outcome,
            signal = tokio::signal::ctrl_c() => return signal.map_err(|e| e.to_string()),
        };
        if options.once {
            return outcome;
        }
        if let Err(error) = outcome {
            eprintln!("{error}; waiting for a source change");
        }
        loop {
            tokio::select! {
                _ = tokio::time::sleep(std::time::Duration::from_millis(500)) => {},
                signal = tokio::signal::ctrl_c() => return signal.map_err(|e| e.to_string()),
            }
            let current = sources(&root)?;
            if current != previous {
                previous = current;
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn watches_content_and_deletions_but_ignores_build_output() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("lib.rs"), "one").unwrap();
        let before = sources(dir.path()).unwrap();
        std::fs::create_dir(dir.path().join("target")).unwrap();
        std::fs::write(dir.path().join("target/generated.rs"), "generated").unwrap();
        assert_eq!(before, sources(dir.path()).unwrap());
        std::fs::write(dir.path().join("lib.rs"), "two").unwrap();
        assert_ne!(before, sources(dir.path()).unwrap());
        std::fs::remove_file(dir.path().join("lib.rs")).unwrap();
        assert!(sources(dir.path()).unwrap().is_empty());
    }
}
