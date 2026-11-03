//! Guest artifacts: `cargo flower build`, the offline inspection behind
//! `plan` and `deploy`, and manifest checks.
//!
//! Cargo discovery follows metadata and compiler-artifact messages, as
//! v1: no assumption about target directories, workspace layout, target
//! names, profiles or file-name rewriting. Inspection loads the emitted
//! Wasm through the server's own backend (wasmtime, ABI and import
//! checks, `flower_init` under the default limits) and reads the
//! manifest the module declares; the guest never runs natively.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use flower_engine::descriptors::{DefKind, ModuleDef, load_module};
use flower_engine::runtime::wasm::WasmBackend;
use serde::Deserialize;
use sha2::Digest;

/// `cargo metadata` output, the parts discovery reads.
#[derive(Debug, Deserialize)]
pub struct Metadata {
    pub packages: Vec<Package>,
    pub workspace_members: Vec<String>,
}

/// A package in the metadata.
#[derive(Debug, Deserialize)]
pub struct Package {
    pub id: String,
    pub name: String,
    pub targets: Vec<Target>,
}

/// A build target of a package.
#[derive(Debug, Deserialize)]
pub struct Target {
    pub crate_types: Vec<String>,
}

impl Package {
    fn guest(&self) -> bool {
        self.targets
            .iter()
            .any(|target| target.crate_types.iter().any(|kind| kind == "cdylib"))
    }
}

/// The one workspace cdylib to build: named by `selector` (package name
/// or ID), or the only one there is.
pub fn select_package<'a>(
    metadata: &'a Metadata,
    selector: Option<&str>,
) -> Result<&'a Package, String> {
    let matches: Vec<_> = metadata
        .packages
        .iter()
        .filter(|package| {
            metadata.workspace_members.contains(&package.id)
                && package.guest()
                && selector
                    .is_none_or(|selector| selector == package.name || selector == package.id)
        })
        .collect();
    match matches.as_slice() {
        [package] => Ok(package),
        _ => Err(format!(
            "select exactly one workspace cdylib with --package (found {} candidates)",
            matches.len()
        )),
    }
}

/// A Cargo JSON message, the parts discovery reads.
#[derive(Deserialize)]
struct Message {
    reason: String,
    #[serde(default)]
    package_id: String,
    target: Option<Target>,
    #[serde(default)]
    filenames: Vec<PathBuf>,
}

/// The one Wasm cdylib Cargo reported for `package`, ignoring other
/// packages, build scripts, rlibs and diagnostics.
pub fn discover_artifact(messages: impl BufRead, package: &Package) -> Result<PathBuf, String> {
    let mut paths = BTreeSet::new();
    for line in messages.lines() {
        let line = line.map_err(|error| format!("reading cargo output: {error}"))?;
        let Ok(message) = serde_json::from_str::<Message>(&line) else {
            continue;
        };
        if message.reason != "compiler-artifact" || message.package_id != package.id {
            continue;
        }
        let Some(target) = message.target else {
            continue;
        };
        if !target.crate_types.iter().any(|kind| kind == "cdylib") {
            continue;
        }
        paths.extend(message.filenames.into_iter().filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "wasm")
        }));
    }
    let count = paths.len();
    let mut paths = paths.into_iter();
    match (paths.next(), count) {
        (Some(path), 1) => Ok(path),
        _ => Err(format!(
            "cargo must report exactly one Wasm cdylib artifact (reported {count})"
        )),
    }
}

/// What `cargo flower build` runs Cargo with.
pub struct Build {
    pub manifest_path: Option<PathBuf>,
    pub package: Option<String>,
    pub profile: String,
    pub features: Vec<String>,
    pub no_default_features: bool,
    pub locked: bool,
}

/// A built and inspected artifact.
#[derive(Debug)]
pub struct Artifact {
    /// The package (and module) name.
    pub name: String,
    /// The written Wasm.
    pub wasm: PathBuf,
    /// The written manifest JSON.
    pub manifest: PathBuf,
    /// SHA-256 of the Wasm, hex.
    pub sha256: String,
    /// The module's shape.
    pub summary: Summary,
}

fn cargo() -> Command {
    Command::new(std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into()))
}

impl Build {
    fn manifest_arg(&self, command: &mut Command) {
        if let Some(path) = &self.manifest_path {
            command.arg("--manifest-path").arg(path);
        }
    }

    /// Build the guest for wasm32, inspect what Cargo emitted, and only
    /// then write `<name>.wasm` and `<name>.manifest.json` to `out_dir`:
    /// an artifact that fails inspection is never written.
    pub fn run(&self, out_dir: &Path) -> Result<Artifact, String> {
        let mut command = cargo();
        command.args(["metadata", "--format-version=1", "--no-deps"]);
        self.manifest_arg(&mut command);
        let output = command
            .stdin(Stdio::null())
            .output()
            .map_err(|error| format!("running cargo metadata: {error}"))?;
        if !output.status.success() {
            return Err(format!(
                "cargo metadata failed: {}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }
        let metadata: Metadata = serde_json::from_slice(&output.stdout)
            .map_err(|error| format!("decoding cargo metadata: {error}"))?;
        let package = select_package(&metadata, self.package.as_deref())?;
        let mut command = cargo();
        command.args([
            "build",
            "--lib",
            "--target",
            "wasm32-unknown-unknown",
            "--message-format=json-render-diagnostics",
            "--package",
            &package.name,
            "--profile",
            &self.profile,
        ]);
        self.manifest_arg(&mut command);
        if self.locked {
            command.arg("--locked");
        }
        if self.no_default_features {
            command.arg("--no-default-features");
        }
        if !self.features.is_empty() {
            command.arg("--features").arg(self.features.join(","));
        }
        // Diagnostics stay on stderr; stdout drains while Cargo runs so a
        // long artifact log cannot block it.
        let mut child = command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(|error| format!("running cargo build: {error}"))?;
        let stdout = child.stdout.take().ok_or("cargo build has no stdout")?;
        let artifact = discover_artifact(BufReader::new(stdout), package);
        let status = child
            .wait()
            .map_err(|error| format!("waiting for cargo build: {error}"))?;
        if !status.success() {
            return Err(format!("cargo build failed ({status})"));
        }
        emit(&artifact?, out_dir, &package.name)
    }
}

/// A module's shape, for reports.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Summary {
    pub sql_endpoints: usize,
    pub sql_plans: usize,
    pub collections: usize,
    pub queries: usize,
    pub mutations: usize,
    pub derived: usize,
    pub transactions: usize,
    pub triggers: usize,
    pub aliases: usize,
}

impl std::fmt::Display for Summary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "collections={} queries={} mutations={} derived={} transactions={} triggers={} http={} sql_plans={} sql_endpoints={}",
            self.collections,
            self.queries,
            self.mutations,
            self.derived,
            self.transactions,
            self.triggers,
            self.aliases,
            self.sql_plans,
            self.sql_endpoints
        )
    }
}

fn summary(module: &ModuleDef, manifest: &serde_json::Value) -> Summary {
    let count = |kind: DefKind| module.def_kinds.values().filter(|k| **k == kind).count();
    Summary {
        sql_endpoints: module.sql_endpoints.len(),
        sql_plans: module.sql.len(),
        collections: module.collections.len(),
        queries: count(DefKind::Query),
        mutations: count(DefKind::Mutation),
        derived: count(DefKind::Derived),
        transactions: count(DefKind::Transaction),
        triggers: module.triggers.len(),
        aliases: manifest["http"].as_array().map_or(0, Vec::len),
    }
}

/// Load Wasm through the server's backend and read the manifest it
/// declares, as JSON: the checks a deployment's validation runs (ABI
/// exports, import rules, `flower_init` under the default limits).
pub fn embedded_manifest(name: &str, wasm: &[u8]) -> Result<serde_json::Value, String> {
    let mut backend = WasmBackend::new().map_err(|failure| failure.message)?;
    backend
        .register(name, wasm)
        .map_err(|failure| format!("{}: {}", failure.code, failure.message))?;
    let manifest = backend
        .manifest(name)
        .map_err(|failure| format!("{}: {}", failure.failure.code, failure.failure.message))?;
    let bytes = flower_engine::json::render(&manifest).map_err(|error| error.to_string())?;
    serde_json::from_slice(&bytes).map_err(|error| error.to_string())
}

/// Validate a manifest as a deployment would: the descriptor contract
/// (schemas, indexes, references, access rules, aliases) and triggers
/// that name mutations.
pub fn check_manifest(manifest: &serde_json::Value) -> Result<ModuleDef, String> {
    let text = serde_json::to_vec(manifest).map_err(|error| error.to_string())?;
    let value = flower_engine::json::parse_value(&text).map_err(|error| error.to_string())?;
    let module = load_module(&value).map_err(|error| format!("invalid manifest: {error}"))?;
    for trigger in &module.triggers {
        if module.def_kinds.get(&trigger.name) != Some(&DefKind::Mutation) {
            return Err(format!(
                "invalid manifest: trigger `{}` is not a mutation",
                trigger.name
            ));
        }
    }
    Ok(module)
}

/// A preflighted deployment: the manifest to send (bindings included),
/// its module, whether it is the one the module declares, and the
/// content identity the server will compute for it.
pub struct Preflight {
    pub manifest: serde_json::Value,
    pub module: ModuleDef,
    pub matches_embedded: bool,
    pub summary: Summary,
    pub identity: [u8; 32],
}

/// Inspect Wasm and validate the manifest to deploy (the module's own
/// unless one is given) with this deployment's bindings, before
/// anything is sent anywhere.
pub fn preflight(
    name: &str,
    wasm: &[u8],
    manifest: Option<serde_json::Value>,
    bindings: &Bindings,
) -> Result<Preflight, String> {
    let embedded = embedded_manifest(name, wasm)?;
    let mut manifest = manifest.unwrap_or_else(|| embedded.clone());
    let matches_embedded = manifest == embedded;
    bind(&mut manifest, bindings)?;
    let module = check_manifest(&manifest)?;
    let summary = summary(&module, &manifest);
    let identity = content_identity(wasm, &manifest)?;
    Ok(Preflight {
        matches_embedded,
        manifest,
        module,
        summary,
        identity,
    })
}

/// Deployment bindings: each logical import slot (named without `$`)
/// and the module it stands for in this deployment.
pub type Bindings = BTreeMap<String, String>;

/// Collect `--bind slot=module` arguments and a `--bindings` JSON file
/// (an object of slot to module, as v1 took it) into one set. A slot
/// may carry its `$`; one named twice with different modules is
/// refused.
pub fn parse_bindings(pairs: &[String], file: Option<&Path>) -> Result<Bindings, String> {
    let mut bindings = Bindings::new();
    let mut add = |slot: &str, module: &str| -> Result<(), String> {
        let slot = slot.strip_prefix('$').unwrap_or(slot);
        if slot.is_empty() || module.is_empty() || module.starts_with('$') {
            return Err(format!(
                "binding `{slot}={module}`: name a slot and a deployed module"
            ));
        }
        match bindings.insert(slot.to_string(), module.to_string()) {
            Some(earlier) if earlier != module => Err(format!(
                "slot `{slot}` is bound to both `{earlier}` and `{module}`"
            )),
            _ => Ok(()),
        }
    };
    if let Some(path) = file {
        let bytes =
            std::fs::read(path).map_err(|error| format!("reading {}: {error}", path.display()))?;
        let parsed: BTreeMap<String, String> = serde_json::from_slice(&bytes).map_err(|error| {
            format!(
                "{}: bindings are a JSON object of slot to module: {error}",
                path.display()
            )
        })?;
        for (slot, module) in &parsed {
            add(slot, module)?;
        }
    }
    for pair in pairs {
        let (slot, module) = pair
            .split_once('=')
            .ok_or_else(|| format!("binding `{pair}`: expected SLOT=MODULE"))?;
        add(slot, module)?;
    }
    Ok(bindings)
}

/// Write bindings into the manifest's root `bindings` member, where the
/// deployment's content identity covers them. Every slot the module
/// imports must end up bound; a slot the manifest already binds to
/// another module is refused.
pub fn bind(manifest: &mut serde_json::Value, bindings: &Bindings) -> Result<(), String> {
    let root = manifest
        .as_object_mut()
        .ok_or("invalid manifest: not an object")?;
    if !bindings.is_empty() {
        let member = root
            .entry("bindings")
            .or_insert_with(|| serde_json::json!({}))
            .as_object_mut()
            .ok_or("invalid manifest: bindings is not an object")?;
        for (slot, module) in bindings {
            match member.get(slot).and_then(serde_json::Value::as_str) {
                Some(earlier) if earlier != module => {
                    return Err(format!(
                        "slot `{slot}` is already bound to `{earlier}` in the manifest"
                    ));
                }
                _ => {
                    member.insert(slot.clone(), serde_json::json!(module));
                }
            }
        }
    }
    if let Some(slot) = unbound_slots(manifest).into_iter().next() {
        return Err(format!(
            "slot `${slot}` is not bound: deploy with --bind {slot}=<module>"
        ));
    }
    Ok(())
}

/// The slots the manifest imports that its `bindings` leave unbound.
fn unbound_slots(manifest: &serde_json::Value) -> BTreeSet<String> {
    let bound = manifest["bindings"].as_object();
    manifest["imports"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|import| import["module"].as_str()?.strip_prefix('$'))
        .filter(|slot| !bound.is_some_and(|bound| bound.contains_key(*slot)))
        .map(str::to_string)
        .collect()
}

/// [`check_manifest`] for an artifact rather than a deployment: slots
/// may still be unbound, so each is checked as if bound.
pub fn check_artifact(manifest: &serde_json::Value) -> Result<ModuleDef, String> {
    let slots = unbound_slots(manifest);
    if slots.is_empty() {
        return check_manifest(manifest);
    }
    let mut checked = manifest.clone();
    let placeholders = slots.into_iter().map(|slot| (slot.clone(), slot)).collect();
    bind(&mut checked, &placeholders)?;
    check_manifest(&checked)
}

/// The identity a deployment of `wasm` with `manifest` gets when none is
/// given: the server's hash of the Wasm and the canonical manifest.
fn content_identity(wasm: &[u8], manifest: &serde_json::Value) -> Result<[u8; 32], String> {
    let text = serde_json::to_vec(manifest).map_err(|error| error.to_string())?;
    let canonical = flower_engine::json::canonicalize(&text).map_err(|error| error.to_string())?;
    Ok(flower_crypto::hash::content_identity(wasm, &canonical))
}

/// The database's other modules for a link check, from `MODULE=FILE`
/// pairs: each file a manifest JSON (a deployed one carries its
/// bindings) or Wasm, read for the manifest it declares.
pub fn deployed_modules(pairs: &[String]) -> Result<HashMap<String, ModuleDef>, String> {
    let mut modules = HashMap::new();
    for pair in pairs {
        let (name, path) = pair
            .split_once('=')
            .filter(|(name, path)| !name.is_empty() && !path.is_empty())
            .ok_or_else(|| format!("`{pair}`: expected MODULE=FILE"))?;
        let module = check_artifact(&read_manifest(Path::new(path))?)
            .map_err(|error| format!("{name}: {error}"))?;
        if modules.insert(name.to_string(), module).is_some() {
            return Err(format!("module `{name}` is given twice"));
        }
    }
    Ok(modules)
}

/// Lowercase hex.
pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Bindings as `slot=module` pairs, or `none`.
pub fn describe_bindings(manifest: &serde_json::Value) -> String {
    let pairs: Vec<String> = manifest["bindings"]
        .as_object()
        .into_iter()
        .flatten()
        .map(|(slot, module)| format!("{slot}={}", module.as_str().unwrap_or_default()))
        .collect();
    if pairs.is_empty() {
        "none".to_string()
    } else {
        pairs.join(",")
    }
}

/// Inspect the artifact Cargo built, then write it and its manifest.
fn emit(source: &Path, out_dir: &Path, name: &str) -> Result<Artifact, String> {
    let wasm =
        std::fs::read(source).map_err(|error| format!("reading {}: {error}", source.display()))?;
    // Bindings belong to a deployment: the artifact keeps the manifest
    // the module declares, slots unbound.
    let manifest = embedded_manifest(name, &wasm)?;
    let module = check_artifact(&manifest)?;
    std::fs::create_dir_all(out_dir).map_err(|error| error.to_string())?;
    let wasm_path = out_dir.join(format!("{name}.wasm"));
    let manifest_path = out_dir.join(format!("{name}.manifest.json"));
    std::fs::write(&wasm_path, &wasm).map_err(|error| error.to_string())?;
    let pretty = serde_json::to_vec_pretty(&manifest).map_err(|error| error.to_string())?;
    std::fs::write(&manifest_path, pretty).map_err(|error| error.to_string())?;
    let sha256 = hex(&sha2::Sha256::digest(&wasm));
    Ok(Artifact {
        name: name.to_string(),
        wasm: wasm_path,
        manifest: manifest_path,
        sha256,
        summary: summary(&module, &manifest),
    })
}

/// Read a manifest from JSON, or from the module a Wasm file declares.
pub fn read_manifest(path: &Path) -> Result<serde_json::Value, String> {
    let bytes =
        std::fs::read(path).map_err(|error| format!("reading {}: {error}", path.display()))?;
    if bytes.starts_with(b"\0asm") {
        return embedded_manifest("module", &bytes);
    }
    serde_json::from_slice(&bytes).map_err(|error| format!("parsing {}: {error}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::io::Cursor;

    #[test]
    fn workspace_discovery_requires_one_cdylib() {
        let metadata: Metadata = serde_json::from_value(json!({
            "workspace_members": ["a", "b", "native"],
            "packages": [
                {"id": "a", "name": "first-guest", "targets": [{"name": "different_name", "crate_types": ["cdylib", "rlib"]}]},
                {"id": "b", "name": "other", "targets": [{"name": "other", "crate_types": ["cdylib"]}]},
                {"id": "native", "name": "native", "targets": [{"name": "native", "crate_types": ["lib"]}]},
                {"id": "dependency", "name": "dependency", "targets": [{"name": "dependency", "crate_types": ["cdylib"]}]}
            ]
        }))
        .unwrap();
        assert!(select_package(&metadata, None).is_err());
        assert_eq!(
            select_package(&metadata, Some("first-guest")).unwrap().id,
            "a"
        );
        assert_eq!(select_package(&metadata, Some("b")).unwrap().name, "other");
        assert!(select_package(&metadata, Some("native")).is_err());
        assert!(select_package(&metadata, Some("dependency")).is_err());
    }

    #[test]
    fn artifact_discovery_reads_reported_file_names() {
        let package = Package {
            id: "workspace-id".into(),
            name: "package-name".into(),
            targets: vec![],
        };
        let messages = [
            json!({"reason": "compiler-message", "message": {"rendered": "diagnostic"}}),
            json!({"reason": "compiler-artifact", "package_id": "dependency", "target": {"name": "dep", "crate_types": ["cdylib"]}, "filenames": ["/wrong/dep.wasm"]}),
            json!({"reason": "compiler-artifact", "package_id": "workspace-id", "target": {"name": "build-script-build", "crate_types": ["bin"]}, "filenames": ["/x/build.wasm"]}),
            json!({"reason": "compiler-artifact", "package_id": "workspace-id", "target": {"name": "target-name", "crate_types": ["cdylib"]}, "filenames": ["/custom target/wasm32-unknown-unknown/custom/actual.wasm", "/custom/actual.d"]}),
        ];
        let log = messages
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n");
        assert_eq!(
            discover_artifact(Cursor::new(log), &package).unwrap(),
            PathBuf::from("/custom target/wasm32-unknown-unknown/custom/actual.wasm")
        );
        assert!(discover_artifact(Cursor::new(""), &package).is_err());
    }

    #[test]
    fn manifests_with_triggers_on_non_mutations_are_refused() {
        let manifest = json!({
            "collections": [{"name": "docs", "key": {"type": "json"}, "value": {"type": "json"}, "indexes": [], "references": []}],
            "definitions": [{"name": "watch", "kind": "query", "args": {"type": "json"}}],
            "triggers": [{"name": "watch", "source": "docs"}],
            "http": []
        });
        let error = check_manifest(&manifest).unwrap_err();
        assert!(error.contains("not a mutation"), "{error}");
        assert!(check_manifest(&json!({"collections": 3})).is_err());
    }

    #[test]
    fn bindings_come_from_flags_and_files_and_land_in_the_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("bindings.json");
        std::fs::write(&file, r#"{"$tasks": "board", "audit": "log"}"#).unwrap();
        let pairs = ["tasks=board".to_string(), "$mail=post".to_string()];
        let bindings = parse_bindings(&pairs, Some(&file)).unwrap();
        let expected: Vec<(&str, &str)> =
            vec![("audit", "log"), ("mail", "post"), ("tasks", "board")];
        assert_eq!(
            bindings
                .iter()
                .map(|(slot, module)| (slot.as_str(), module.as_str()))
                .collect::<Vec<_>>(),
            expected
        );
        for refused in [&["tasks"][..], &["=board"], &["tasks="], &["tasks=$other"]] {
            let pairs: Vec<String> = refused.iter().map(ToString::to_string).collect();
            assert!(parse_bindings(&pairs, None).is_err(), "{refused:?}");
        }
        let error = parse_bindings(&["tasks=other".to_string()], Some(&file)).unwrap_err();
        assert!(error.contains("bound to both"), "{error}");
        std::fs::write(&file, "[]").unwrap();
        assert!(parse_bindings(&[], Some(&file)).is_err());
        // Into a manifest: added beside its own, the same twice is fine,
        // a different module for one of its slots is refused, and every
        // imported slot must end up bound.
        let mut manifest = json!({"imports": [{"module": "$tasks"}, {"module": "ledger"}]});
        assert!(bind(&mut manifest, &Bindings::new()).is_err());
        let one = Bindings::from([("tasks".to_string(), "board".to_string())]);
        bind(&mut manifest, &one).unwrap();
        bind(&mut manifest, &one).unwrap();
        assert_eq!(manifest["bindings"], json!({"tasks": "board"}));
        assert_eq!(describe_bindings(&manifest), "tasks=board");
        let other = Bindings::from([("tasks".to_string(), "other".to_string())]);
        assert!(
            bind(&mut manifest, &other)
                .unwrap_err()
                .contains("already bound")
        );
        assert_eq!(describe_bindings(&json!({})), "none");
    }
}
