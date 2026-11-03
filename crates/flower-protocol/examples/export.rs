//! Export the versioned protocol artifacts to the documentation site, or verify their freshness.
use std::{path::PathBuf, process::ExitCode};
fn main() -> ExitCode {
    let check = std::env::args().any(|arg| arg == "--check");
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../web/docs/assets/protocol");
    let result = (|| -> std::io::Result<()> {
        if !check {
            std::fs::create_dir_all(&root)?;
        }
        for (name, contents) in [
            ("v1.schema.json", flower_protocol::WIRE_SCHEMA),
            ("v1.fixtures.json", flower_protocol::GOLDEN_FIXTURES),
        ] {
            let path = root.join(name);
            if check {
                if std::fs::read_to_string(&path)? != contents {
                    return Err(std::io::Error::other(format!(
                        "{} is out of date",
                        path.display()
                    )));
                }
            } else {
                std::fs::write(path, contents)?;
            }
        }
        Ok(())
    })();
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}
