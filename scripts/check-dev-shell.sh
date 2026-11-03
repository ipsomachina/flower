#!/usr/bin/env bash
# Run through `nix develop --command bash scripts/check-dev-shell.sh`.
set -euo pipefail
scratch=$(mktemp -d "${TMPDIR:-/tmp}/flower-shell.XXXXXX")
trap 'rm -rf "$scratch"' EXIT

rustc --version
cargo --version
cargo clippy --version
rustfmt --version
rust-analyzer --version
clang --version
cmake --version
bun --version

# Exercise the installed Wasm target and its linker, then run the artifact.
cat > "$scratch/guest.rs" <<'RUST'
#[unsafe(no_mangle)]
pub extern "C" fn answer() -> u32 { 42 }
RUST
rustc --edition 2024 --crate-type cdylib --target wasm32-unknown-unknown \
  -O "$scratch/guest.rs" -o "$scratch/guest.wasm"
bun -e 'const fs = require("node:fs"); const m = new WebAssembly.Module(fs.readFileSync(process.argv[1])); if (new WebAssembly.Instance(m).exports.answer() !== 42) process.exit(1)' "$scratch/guest.wasm"

# Check the C compiler and headers, pkg-config lookup, linkage and runtime.
cat > "$scratch/native.c" <<'C'
#include <openssl/crypto.h>
int main(void) { return OpenSSL_version_num() == 0; }
C
read -r -a openssl_flags <<< "$(pkg-config --cflags --libs openssl)"
cc "$scratch/native.c" -o "$scratch/native" "${openssl_flags[@]}"
"$scratch/native"

bun scripts/release.mjs --check
bun test ./scripts
actionlint -shellcheck= -pyflakes=
bun web/docs/tools/sync.mjs --check
echo 'Development shell checks passed.'
