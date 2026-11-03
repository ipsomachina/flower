#!/usr/bin/env bash
set -euo pipefail
image=${1:?usage: test-image.sh IMAGE}
name="flower-smoke-${RANDOM}-$$"
volume="${name}-data"
cleanup() {
  docker logs "$name" || true
  docker rm -f "$name" >/dev/null 2>&1 || true
  docker volume rm "$volume" >/dev/null 2>&1 || true
}
trap cleanup EXIT
docker run --rm "$image" --help >/dev/null
test "$(docker image inspect "$image" --format '{{.Config.User}}')" = '10001:10001'
docker volume create "$volume" >/dev/null
docker run -d --name "$name" -p 127.0.0.1::8080 \
  -e FLOWER_ADMIN_TOKEN=container-smoke-test \
  -v "$volume:/data" "$image" >/dev/null
ready() {
  local address
  address=$(docker port "$name" 8080/tcp)
  for _ in $(seq 1 90); do
    if curl --fail --silent "http://${address}/health/ready" >/dev/null; then
      return 0
    fi
    sleep 1
  done
  return 1
}
ready
docker stop --time 30 "$name" >/dev/null
test "$(docker inspect "$name" --format '{{.State.ExitCode}}')" = 0
docker start "$name" >/dev/null
ready
