#!/usr/bin/env bun
/** Validate or publish Flower's release workspace. Uploads require --publish. */
import { TOML } from "bun";
import { cpSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join, relative } from "node:path";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";
import { parseArgs } from "node:util";

export const ROOT = fileURLToPath(new URL("../", import.meta.url));
export const FORKS = ["vendor/redb", "vendor/openraft"];
export const manifest = path => TOML.parse(readFileSync(path, "utf8"));
export function packages(root) {
  const result = new Map();
  for (const path of [...manifest(join(root, "Cargo.toml")).workspace.members, ...FORKS]) {
    const pkg = manifest(join(root, path, "Cargo.toml")).package;
    if (pkg.publish !== false) result.set(pkg.name, [path, pkg.version]);
  }
  return result;
}
export function validate(root, tag, read = manifest) {
  const selected = packages(root), version = selected.get("cargo-flower")[1];
  if (manifest(join(root, "Cargo.toml")).workspace.package.license !== "MIT") throw new Error("workspace must declare the MIT license");
  if (tag !== undefined && tag !== `v${version}`) throw new Error(`release tag must be v${version}, got ${JSON.stringify(tag)}`);
  for (const [name, [path, current]] of selected) {
    const data = read(join(root, path, "Cargo.toml"));
    if (!FORKS.includes(path) && current !== version) throw new Error(`${name} is ${current}; expected ${version}`);
    for (const section of ["dependencies", "build-dependencies"]) {
      for (const [alias, dep] of Object.entries(data[section] ?? {})) {
        if (typeof dep !== "object") continue;
        const actual = dep.package ?? alias;
        if (!selected.has(actual)) {
          if (dep.path !== undefined) throw new Error(`${name} depends on unpublished package ${actual}`);
          continue;
        }
        if ((dep.version ?? "").replace(/^=/, "") !== selected.get(actual)[1]) throw new Error(`${name}: ${actual} needs its current registry version`);
      }
    }
    if (!FORKS.includes(path) && data.package.license?.workspace !== true) throw new Error(`${name} must inherit the MIT license`);
  }
  return selected;
}
export function stage(root, destination) {
  for (const directory of ["crates", "vendor", "examples"]) cpSync(join(root, directory), join(destination, directory), {
    recursive: true, filter: path => !["target", ".git", "__pycache__"].includes(path.split(/[\\/]/).at(-1)),
  });
  for (const file of ["Cargo.toml", "Cargo.lock", "LICENSE", "README.md"]) cpSync(join(root, file), join(destination, file));
  const workspace = manifest(join(root, "Cargo.toml"));
  const path = join(destination, "Cargo.toml");
  let text = readFileSync(path, "utf8");
  if (!/^members = \[/m.test(text)) throw new Error("cannot locate workspace members");
  text = text.replace(/^members = \[/m, 'members = [\n    "vendor/redb",\n    "vendor/openraft",');
  text = text.replace(/^exclude = \["vendor\/redb", "vendor\/openraft"\]\r?\n/m, "");
  const forks = new Map(FORKS.map(path => [manifest(join(root, path, "Cargo.toml")).package.name, path]));
  if (Object.keys(workspace.patch?.["crates-io"] ?? {}).sort().join() !== [...forks.keys()].sort().join()) throw new Error("release staging needs an explicit rule for each registry patch");
  // Local path edges let Cargo order unpublished fork uploads. Normalized
  // registry dependencies retain their package names and exact versions.
  text = text.split(/\r?\n\[patch\.crates-io\]\r?\n/)[0].trimEnd() + "\n";
  writeFileSync(path, text);
  for (const member of workspace.workspace.members) {
    const file = join(destination, member, "Cargo.toml");
    let text = readFileSync(file, "utf8");
    for (const [name, fork] of forks) text = text.replaceAll(`package = "${name}",`, `package = "${name}", path = "${relative(dirname(file), join(destination, fork)).replaceAll("\\", "/")}",`);
    writeFileSync(file, text);
    // SPDX metadata alone does not include the workspace's root LICENSE
    // in Cargo archives. Stage the canonical text beside each manifest.
    cpSync(join(root, "LICENSE"), join(dirname(file), "LICENSE"));
  }
}
export async function published(name, version, request = fetch) {
  const response = await request(`https://crates.io/api/v1/crates/${name}/${version}`, {
    headers: { "User-Agent": "flower-release (github.com/ipsomachina/flower)" }, signal: AbortSignal.timeout(30_000),
  });
  if (response.status === 404) return null;
  if (!response.ok) throw new Error(`crates.io returned HTTP ${response.status} for ${name} ${version}`);
  return (await response.json()).version;
}
export async function main(args = process.argv.slice(2), { root = ROOT, env = process.env, run = spawnSync, lookup = published } = {}) {
  const { values, positionals } = parseArgs({ args, allowPositionals: true, options: {
    check: { type: "boolean" }, "dry-run": { type: "boolean" }, publish: { type: "boolean" }, tag: { type: "string" }, help: { type: "boolean", short: "h" },
  } });
  if (values.help) { console.log("Usage: bun scripts/release.mjs (--check | --dry-run | --publish) [--tag vVERSION]"); return; }
  if (positionals.length || [values.check, values["dry-run"], values.publish].filter(Boolean).length !== 1) throw new Error("select exactly one of --check, --dry-run or --publish");
  if (values.publish && (!values.tag || !env.CARGO_REGISTRY_TOKEN)) throw new Error("--publish requires --tag and CARGO_REGISTRY_TOKEN");
  const selected = validate(root, values.tag);
  console.log("Release packages:", [...selected.keys()].join(", "));
  if (values.check) return;
  if (values.publish) {
    for (const [name, [, version]] of selected) {
      const existing = await lookup(name, version);
      if (existing !== null) {
        if (existing.yanked) throw new Error(`${name} ${version} is yanked; bump its version`);
        console.log(`Already published: ${name} ${version}`);
        selected.delete(name);
      }
    }
  }
  if (!selected.size) { console.log("All release versions are already published."); return; }
  const destination = mkdtempSync(join(tmpdir(), "flower-release-"));
  try {
    stage(root, destination);
    const command = ["publish", "--registry", "crates-io"];
    if (values["dry-run"]) command.push("--dry-run");
    for (const name of selected.keys()) command.push("--package", name);
    const result = run("cargo", command, { cwd: destination, stdio: "inherit", env: { ...env,
      CARGO_TARGET_DIR: env.CARGO_TARGET_DIR ?? join(root, "target", "release-packages"),
    } });
    if (result.error) throw result.error;
    if (result.status !== 0) throw new Error(`cargo publish exited ${result.status ?? result.signal}`);
  } finally { rmSync(destination, { recursive: true, force: true }); }
}
if (import.meta.main) {
  try { await main(); } catch (error) { console.error(error.message); process.exitCode = 1; }
}
