# Flower

A database of source records and reactive derived values, accessed through
deployed, named application methods. Isolated Rust guests compute against
tracked state; the native engine validates and atomically publishes their
consequences; Raft replicates accepted consequences across server members;
devices follow as synced replicas with offline intent queues.

See `spec.md` for the architecture and API specification.

Bounded read-only SQL is available through typed named plans and scoped SDK
endpoints accepting SQL text through ordinary module queries.
See [SQL semantics and limits](SQL.md) and the
[working example](examples/sql-queries/src/lib.rs).

Flower's code and documentation are open source under the [MIT license](LICENSE).
Bundled third-party components retain their own licenses, including the
[dependency forks](vendor/) and [Bodoni Moda font](web/fonts/OFL.txt).

## Layout

- `crates/flower-sdk` — `no_std` + `alloc` guest runtime: codecs, schemas,
  descriptors, contexts, modules and policies. No runtime dependencies.
- `crates/flower-sdk-macros` — derive/attribute macros expanding on the
  compiler host.
- `crates/flower-sql` — read-only SQL parser, schema binder, logical ordering,
  budgeted evaluator and explain output. Guests use the SDK's `SqlQuery` handle.
- `crates/flower-protocol` — shared wire contracts, version/capability
  negotiation, structured problems, ordering tokens and sync coverage validation.
- `crates/flower-engine` — engine core: wasmtime execution, evaluator,
  graph/index engine, admission, storage, staging, patch/sync codecs and
  the intent log. No consensus or networking.
- `crates/flower-cluster` — replication layer: Raft ordering, sync serving,
  quorum fences and single-cluster multi-database transactions. Servers
  only; devices never link it.
- `crates/flower-crypto` — host crypto lane: hashing, content identity,
  versioned AEAD envelopes and the deployment keystore.
- `crates/flower-backup` — encrypted backup, archive segments, retention,
  re-encryption and point-in-time recovery over object storage.
- `crates/flower-server` — HTTP API over a cluster node: calls,
  transactions, sync, status, metrics, work-queue and timer endpoints,
  admin endpoints and the embedded admin dashboard at `/admin`.
- `crates/flower-client` — native client: unary, typed and declared-error
  calls, typed watches, transactions, retry with overload budgets,
  deadlines and cancellation, sync-down replicas, the offline intent
  queue, work-queue and timer calls, admin calls, an h2c (default),
  HTTP/1.1 and TLS (h2 by ALPN, extra CA roots) transport with pooled
  connections and a watch lane that grows past server stream limits,
  and a scripted test transport (`testing::MockTransport`).
  The default remote client does not link the engine; `replica` enables
  sync-down and offline intents. Scoped typed handles, persisted commands,
  typed transactions and lease handles are described in its
  [API and migration guide](crates/flower-client/README.md).
- `crates/flower-worker` — native queue workers with fixed or adaptive
  concurrency and concurrent claimers over a `QueueClient` trait, the
  `reconcile` worker for external values, and an in-memory queue
  (feature `testing`).
- `crates/cargo-flower` — operator CLI: `build` (guest to inspected
  Wasm plus manifest), `plan` (offline deployment preflight), contract
  inspect/diff, deploy, call, transaction, status, membership, backup,
  queue and timer operations and `serve` (with the timer sweep).
- `crates/flower-bench` — the Goblin Pizza stress benchmark and live
  demo: real server processes, leader-kill chaos, an end-of-run business
  audit, JSON and HTML reports and a live view. `cargo run --release -p
  flower-bench -- --preset demo` (see its README).
- `crates/flower-guest-test` — a real guest crate compiled to `wasm32`
  by execution tests.
- `examples/` — guests ported from v1's Rust examples:
  `external-transactions` (external values kept current by `reconcile`),
  `crypto-ticket` (JWS tickets under a declared catalog key),
  `goblin-pizza` (orders counted by an aggregate, oven timers, delivery
  drones as queue workers on per-tenant scopes)
  and `typed-modules-contract`/`-provider`/`-consumer` (a consumer
  reaching its provider through the `tasks.v1` interface and a `$tasks`
  slot bound at deployment).

## Development

With Nix flakes enabled, run `nix develop`, or run `direnv allow` once if
direnv is hooked into your shell. The locked development shell supports
Linux and macOS on x86-64 and ARM64; on Windows, use it inside WSL. It supplies
Rust 1.99.1 with Clippy, rustfmt, rust-analyzer, Rust sources and the Wasm
target, plus the C/Clang build tools, OpenSSL, Bun 1.4.4 and
actionlint. Use `nix fmt flake.nix nix/bun.nix` to format the flake. Commit `flake.lock` updates
when intentionally upgrading its inputs.

GitHub Actions run tests and package checks on pull requests and `main`.
Version tags publish the production crates to crates.io and a server image
to `ghcr.io/ipsomachina/flower` after the checks pass. See
[CI and releases](RELEASING.md) for setup, versioning and Docker usage.

The [website and documentation](web/README.md) use Build Awesome. Run
`bun install --cwd web --frozen-lockfile` and `bun run --cwd web dev` to preview
them, or `bun run --cwd web build` to generate the static site in `web/_site/`.

- `cargo flower dev --package sql-queries --db demo --module shop` watches Rust,
  Cargo manifest and lockfile changes, builds the guest and deploys successful
  builds to a running development server. Set `FLOWER_ADMIN_TOKEN` for deployment.
  `--watch` selects the Cargo workspace directory, `--endpoint` selects the server,
  and `--once` performs a single build/deploy cycle. Ctrl-C stops the watcher.
  Build failures wait for the next source edit; they do not deploy an artifact.
  Symlinks and directories named `target`, `.git` and `.flower` are excluded.
- `cargo flower contract typescript --manifest .flower/sql-queries.manifest.json
  --out contracts.ts` generates deterministic TypeScript wire types for public
  query and mutation aliases. Decimal integers stay strings; optional fields and
  nullable fields remain distinct. These are static contracts; bounds, patterns
  and authorization still require server validation.
- Add `--contract .flower/sql-queries.manifest.json` to `cargo flower call` to
  check its public alias, query/mutation kind and structural argument schema
  before delivery. Host-only validations and authorization remain server checks.

- An optional `#[flower_sdk::application]` on an inline module assembles its
  `#[collection]` declarations and `#[query]`, `#[mutation]`, and `#[derived]`
  methods into `module::APP`. Mark additional component statics with
  `#[component]`; static component arrays can include components from other
  modules. Export the catalogue explicitly with `flower_sdk::export!(module::APP)`.
  The SQL example demonstrates this style. Existing `App::new` assembly remains
  available for custom initialization and composition.
- `cargo flower call --args-file request.json` reads arguments from a file;
  `--args-file -` reads standard input. It is exclusive with inline `--args`.

- Toolchain: Rust 1.96 or newer, edition 2024, plus the
  `wasm32-unknown-unknown` target (`rustup target add
  wasm32-unknown-unknown`) and a C compiler for the zstd native codec.
  Peer TLS uses rustls with AWS-LC, whose assembly needs clang: on
  Windows ARM64 install LLVM (`winget install LLVM.LLVM`) and put its
  `bin` on `PATH` before building (Git Bash: `PATH="/c/Program
  Files/LLVM/bin:$PATH" cargo ...`; PowerShell: `$env:Path =
  'C:\Program Files\LLVM\bin;' + $env:Path`).
- Full suite: `cargo test --workspace --features flower-client/replica,flower-client/conformance`. Execution tests build the guest
  crate and the examples for `wasm32` into `target/wasm-guest`
  automatically. Dev builds optimize the Wasm compiler crates (see
  `[profile.dev.package]` in `Cargo.toml`), so debug servers deploy
  modules in about a second.
- Build a guest: `cargo flower build --package goblin-pizza` writes
  `.flower/goblin-pizza.wasm` and its manifest; `cargo flower plan
  --wasm .flower/goblin-pizza.wasm --current old.manifest.json` checks a
  deployment offline; `cargo flower deploy --wasm ...` deploys it with
  the manifest it declares, and `--bind tasks=board` binds a slot the
  module imports.
- Quality gate: `cargo fmt --all --check` and `cargo clippy --workspace
  --all-targets --features flower-client/replica,flower-client/conformance -- -D warnings` must both pass. Every crate opts into the
  workspace lint policy (`[workspace.lints]` in `Cargo.toml`); every
  `unsafe` block carries a `// SAFETY:` justification.
- S3 suite: `cargo test -p flower-backup --features s3` runs backup and
  retention against an in-process s3s server; `cargo-flower`'s `cli`
  tests serve, list and restore through one. aws-lc needs clang on
  `PATH` on Windows ARM64, as every build there does.
- Serve a single node: `cargo run -p cargo-flower -- serve --dir data
  --id 1 --bootstrap 1 --admin-token secret`, then `cargo flower status
  --endpoint http://127.0.0.1:8080`. A Raft listener on a non-loopback
  address needs `--peer-token` (or `FLOWER_PEER_TOKEN`; it defaults to the
  admin token); workers authenticate with `--worker-token` and devices
  with `--sync-token`. `--raft-tls-cert`, `--raft-tls-key` and
  `--raft-tls-ca` put every peer connection inside TLS. Raft timing
  derives from `--raft-rtt-ms`; see `serve --help` for the individual
  overrides. `cargo flower membership` inspects and changes the voter
  set and `cargo flower leadership` moves leadership.
- Admin dashboard: open `/admin` on a member's HTTP port (on by default;
  `serve --admin-ui false` turns it off) and enter its admin token.
  `cargo run -p flower-server --example dashboard_demo` serves a
  three-member loopback cluster with steady traffic at
  `http://127.0.0.1:7801/admin` (and 7802, 7803); its token is
  `ADMIN_TOKEN` in `crates/flower-server/examples/dashboard_demo.rs`.
- Parity with v1: [PARITY.md](PARITY.md) tracks every non-JavaScript v1
  capability and where v2 stands.
