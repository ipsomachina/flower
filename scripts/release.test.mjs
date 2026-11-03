import { test, expect } from "bun:test";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { ROOT, FORKS, manifest, packages, validate, stage, published, main } from "./release.mjs";

test("production set includes forks and excludes examples and fixtures", () => {
  const result = validate(ROOT, `v${packages(ROOT).get("cargo-flower")[1]}`);
  for (const name of ["cargo-flower", "flower-redb", "flower-openraft"]) expect(result.has(name)).toBe(true);
  for (const name of ["flower-guest-test", "flower-guest-crypto", "flower-bench"]) expect(result.has(name)).toBe(false);
});
test("mismatched tags and missing registry versions fail before uploads", () => {
  expect(() => validate(ROOT, "v999.0.0")).toThrow("release tag");
  expect(() => validate(ROOT, undefined, path => {
    const data = manifest(path);
    if (data.package.name === "flower-sql") delete data.dependencies["flower-sdk"].version;
    return data;
  })).toThrow("registry version");
});
test("registry outages cannot masquerade as unpublished versions", async () => {
  for (const status of [403, 429, 500]) await expect(published("flower-sdk", "0.1.0", async () => new Response("", { status }))).rejects.toThrow(`HTTP ${status}`);
  expect(await published("flower-sdk", "0.1.0", async () => new Response("", { status: 404 }))).toBeNull();
});
test("uploads require an explicit matching tag and credentials", async () => {
  let ran = false;
  await expect(main(["--publish"], { env: {}, run: () => { ran = true; } })).rejects.toThrow("CARGO_REGISTRY_TOKEN");
  expect(ran).toBe(false);
});
test("staging preserves source and licenses and orders the fork dependencies", () => {
  const destination = mkdtempSync(join(tmpdir(), "flower-release-test-"));
  try {
    stage(ROOT, destination);
    const data = manifest(join(destination, "Cargo.toml"));
    expect(data.workspace.exclude).toBeUndefined();
    for (const fork of FORKS) expect(data.workspace.members).toContain(fork);
    const dep = manifest(join(destination, "crates/flower-engine/Cargo.toml")).dependencies.redb;
    expect(dep.path).toBe("../../vendor/redb");
    expect(dep.package).toBe("flower-redb");
    expect(data.workspace.package.license).toBe("MIT");
    for (const [name, [path]] of packages(ROOT)) {
      if (FORKS.includes(path)) continue;
      expect(manifest(join(destination, path, "Cargo.toml")).package.license.workspace).toBe(true);
      expect(readFileSync(join(destination, path, "LICENSE"))).toEqual(readFileSync(join(ROOT, "LICENSE")));
    }
    for (const path of ["LICENSE", "crates/cargo-flower/src/main.rs", "vendor/openraft/LICENSE-MIT", "vendor/redb/LICENSE-APACHE"]) expect(readFileSync(join(destination, path))).toEqual(readFileSync(join(ROOT, path)));
  } finally { rmSync(destination, { recursive: true, force: true }); }
}, 30_000);
test("dry run invokes Cargo without upload authorization", async () => {
  let invoked = false;
  await main(["--dry-run"], { env: {}, run: (command, args, options) => {
    expect(command).toBe("cargo");
    expect(args).toContain("--dry-run");
    expect(args).toContain("flower-redb");
    expect(options.env.CARGO_REGISTRY_TOKEN).toBeUndefined();
    expect(manifest(join(options.cwd, "crates/flower-engine/Cargo.toml")).dependencies.redb.path).toBe("../../vendor/redb");
    invoked = true;
    return { status: 0 };
  } });
  expect(invoked).toBe(true);
}, 30_000);
