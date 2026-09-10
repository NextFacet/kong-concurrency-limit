#!/usr/bin/env bash
# End-to-end local test: brings up a plain Kong 2.5.1 + Postgres + mock
# upstream via docker compose, deploys the concurrency-limit plugin into the
# running Kong container via deploy-plugin.sh, wires up a test service/route,
# and fires a concurrency load test to prove the plugin throttles correctly.
#
# Usage:
#   ./scripts/run-docker-tests.sh [max_concurrency] [burst_concurrency] [default_conn_delay]
#
# Requires: docker, docker compose (v2 plugin), curl, python3, bc.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${REPO_ROOT}/docker/docker-compose.yml"

MAX_CONCURRENCY="${1:-5}"
BURST_CONCURRENCY="${2:-0}"
DEFAULT_CONN_DELAY="${3:-2}"

cleanup() {
  echo "[run-docker-tests] tearing down stack..."
  docker compose -f "${COMPOSE_FILE}" down -v
}
trap cleanup EXIT

echo "[run-docker-tests] building/starting stack..."
docker compose -f "${COMPOSE_FILE}" up -d --build

echo "[run-docker-tests] waiting for Kong to become healthy..."
for _ in $(seq 1 60); do
  status=$(docker inspect --format='{{.State.Health.Status}}' "$(docker compose -f "${COMPOSE_FILE}" ps -q kong)" 2>/dev/null || echo "starting")
  if [ "${status}" = "healthy" ]; then
    break
  fi
  sleep 2
done

if [ "${status}" != "healthy" ]; then
  echo "[run-docker-tests] ERROR: kong did not become healthy in time" >&2
  docker compose -f "${COMPOSE_FILE}" logs kong
  exit 1
fi

echo "[run-docker-tests] plain Kong is up. Deploying plugin into the running container..."
# -u root: the plugin lua path isn't writable by the container's default
# "kong" user; deploy-plugin.sh itself drops back to "kong" for the actual
# `kong reload` (see comments in that script for why that split matters).
docker exec -u root "$(docker compose -f "${COMPOSE_FILE}" ps -q kong)" /opt/scripts/deploy-plugin.sh

echo "[run-docker-tests] wiring up test service/route/plugin config..."
KONG_ADMIN_URL="http://localhost:8001" \
MOCK_UPSTREAM_URL="http://mock-upstream:9000" \
  "${REPO_ROOT}/scripts/setup-test-service.sh" "${MAX_CONCURRENCY}" "${BURST_CONCURRENCY}" "${DEFAULT_CONN_DELAY}" "false"

echo ""
echo "=== Baseline: concurrency within the limit (should be all 200s) ==="
KONG_PROXY_URL="http://localhost:8000" \
  "${REPO_ROOT}/scripts/load-test.sh" "${MAX_CONCURRENCY}" "${DEFAULT_CONN_DELAY}"

echo ""
echo "=== Overload: 4x the limit fired at once (expect a mix of 200 and 429) ==="
OVERLOAD=$((MAX_CONCURRENCY * 4))
KONG_PROXY_URL="http://localhost:8000" \
  "${REPO_ROOT}/scripts/load-test.sh" "${OVERLOAD}" "${DEFAULT_CONN_DELAY}"

echo ""
echo "[run-docker-tests] done. Stack will be torn down now (trap). Re-run this"
echo "[run-docker-tests] script any time to repeat the test from a clean state."
