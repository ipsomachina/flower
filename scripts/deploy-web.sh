#!/usr/bin/env bash
# Build, check and deploy the public website through xmit.
set -euo pipefail

repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"

if ! command -v xmit >/dev/null 2>&1; then
  echo 'xmit is missing; run this script through nix develop.' >&2
  exit 1
fi

bun install --cwd web --frozen-lockfile
bun run --cwd web check
exec xmit flower.ipsomachina.com web/_site/
