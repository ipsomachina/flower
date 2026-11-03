# CI and releases

Flower is open source under the [MIT license](LICENSE). Crate archives and
container images include the license text and advertise the MIT license.
Bundled third-party components, including the two dependency forks, retain
their upstream licenses.

Each first-party crate inherits the workspace's `license = "MIT"` metadata.
Release staging copies the root `LICENSE` beside every first-party manifest
so Cargo includes the complete notice in the published archives.

## Continuous integration

`.github/workflows/ci.yml` runs on pull requests and pushes to `main`, and can
be run manually. It checks:

- Formatting, strict Clippy and the full workspace test suite on Linux and
  Windows, including replica/conformance, S3 and real-process fault tests.
- The pinned Nix shell: evaluate all four Linux/macOS architectures and test
  Wasm compilation/execution, native OpenSSL linkage and repository tools on Linux.
- Generated documentation, protocol fixtures, native documentation examples
  and Wasm examples.
- Packaging and compilation of all 14 publishable crate archives, without
  uploading them. This catches failures hidden by local path dependencies.
- A Linux amd64 container build, startup as an unprivileged user, readiness,
  graceful shutdown and restart using persistent storage.

CI uses Rust 1.99.1, with `wasm32-unknown-unknown` for execution tests. The
crate MSRV remains 1.96; this workflow tests the pinned release toolchain.
Action revisions are pinned to commit hashes.

## One-time GitHub setup

1. Create the `crates-io` environment in `ipsomachina/flower`. Add a
   `CARGO_REGISTRY_TOKEN` secret containing a crates.io token permitted to
   create and publish the production packages and the two fork packages.
   A repository secret with the same name also works. Restrict access to
   release tags using the environment settings if desired.
2. The GHCR job uses GitHub's `GITHUB_TOKEN` with `packages: write`; no Docker
   Hub account or personal access token is needed. Ensure the organization
   allows this repository to create packages. Set the GHCR package's visibility
   to public after its first publication if unauthenticated pulls are wanted.

Registry names must be available or owned by the publishing account. The
workflow does not create credentials or change package visibility.

## Publish a version

1. Set the same version on every production Flower crate and update their
   internal dependency versions. Test guests, examples and `flower-bench`
   have `publish = false`. Forks use independent versions:
   `flower-redb` is currently `4.4.0-flower.1`, and `flower-openraft` is
   `0.9.25-flower.1`. Bump a fork and its exact requirements when changing it.
2. Update the main and documentation lockfiles, then run:

   ```sh
   bun scripts/release.mjs --check --tag v0.0.20261105
   bun scripts/release.mjs --dry-run
   ```

3. Commit, then push the matching version tag:

   ```sh
   git tag v0.0.20261105
   git push origin main v0.0.20261105
   ```

The tag must point to the commit containing the matching crate versions and
lockfiles. Rerunning an older tag does not pick up fixes committed to `main`.

`.github/workflows/release.yml` validates the tag and runs the complete CI
workflow at that commit. Only after all checks pass does it publish crates
in dependency order. After the crates succeed, it publishes the image to
`ghcr.io/ipsomachina/flower:0.0.20261105` and `:latest`. Pre-release tags such as
`v0.2.0-rc.1` publish an exact version image without moving `latest`.
The image includes provenance and an SBOM. The initial image platform is
`linux/amd64`.

To resume a failed release, rerun its failed jobs or dispatch Release against
the same tag. The publisher queries crates.io and skips existing, non-yanked
versions. It stops on registry errors and on yanked versions; it does not
silently ignore failed uploads. Existing versions are immutable: any change
to an already published crate requires a version bump, even on a retry.

`scripts/release.mjs` assembles a temporary workspace including both vendored
forks and invokes Cargo's multi-package publisher. Staging turns local fork
patches into versioned path dependencies so Cargo can order their uploads;
their normalized registry identities stay the same. Cargo verifies the
normalized archives, resolves unpublished local packages together and uploads
in dependency order. Path-only development dependencies are omitted from
the archives by Cargo, avoiding test-only dependency cycles. Normal builds
keep the forks outside the workspace lint/test gates. The temporary tree
uses the checked-in sources and lockfile; no credentials are copied into it.

## Run the image

```sh
export FLOWER_ADMIN_TOKEN='replace-with-your-admin-token'
docker run --rm --name flower \
  -p 127.0.0.1:8080:8080 \
  -e FLOWER_ADMIN_TOKEN \
  -v flower-data:/data \
  ghcr.io/ipsomachina/flower:0.0.20261105
```

The default command starts a single node with ID 1. Data belongs to UID/GID
10001; prepare bind mounts with matching ownership, or use a named volume
as above. The image carries the `cargo-flower` CLI under `/usr/local/bin/flower`:
arguments after the image replace its default command (`--help`, `status`,
`serve`, etc.). It contains the server/operator CLI, not a Rust toolchain;
build guest modules outside the runtime image.

For a cluster, supply `serve --dir /data --id … --http 0.0.0.0:8080
--raft 0.0.0.0:9000 --bootstrap …`, the appropriate peer credentials and
network-addressable member endpoints. The default keeps Raft on loopback;
exposing port 9000 alone does not make a cluster. See the
[operations guide](web/docs/guides/operations.html) for TLS and topology.

Local image checks:

```sh
docker build -t flower:local .
bash scripts/test-image.sh flower:local
```

References: [Cargo publishing](https://doc.rust-lang.org/cargo/commands/cargo-publish.html),
[versioned path dependencies](https://doc.rust-lang.org/cargo/reference/specifying-dependencies.html#multiple-locations),
and [GitHub's GHCR publishing guide](https://docs.github.com/en/actions/tutorials/publish-packages/publish-docker-images).
