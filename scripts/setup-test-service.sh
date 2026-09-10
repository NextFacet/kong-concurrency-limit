#!/usr/bin/env bash
# Host-side helper: registers a Service + Route pointing at the mock
# upstream, then attaches the concurrency-limit plugin to that route via
# Kong's Admin API. Run after deploy-plugin.sh has installed the plugin.
#
# Usage:
#   ./setup-test-service.sh [max_concurrency] [burst_concurrency] [default_conn_delay] [dry_run]
#
# Example:
#   ./setup-test-service.sh 5 0 2 false

set -euo pipefail

ADMIN_URL="${KONG_ADMIN_URL:-http://localhost:8001}"
UPSTREAM_URL="${MOCK_UPSTREAM_URL:-http://mock-upstream:9000}"

MAX_CONCURRENCY="${1:-5}"
BURST_CONCURRENCY="${2:-0}"
DEFAULT_CONN_DELAY="${3:-2}"
DRY_RUN="${4:-false}"

echo "[setup] waiting for Kong Admin API at ${ADMIN_URL}..."
for _ in $(seq 1 30); do
  if curl -sf "${ADMIN_URL}/status" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

echo "[setup] creating/updating service 'mock-service' -> ${UPSTREAM_URL}"
curl -sf -X PUT "${ADMIN_URL}/services/mock-service" \
  -d "url=${UPSTREAM_URL}" > /dev/null

echo "[setup] creating/updating route 'mock-route' -> /mock"
curl -sf -X PUT "${ADMIN_URL}/services/mock-service/routes/mock-route" \
  -d "paths[]=/mock" \
  -d "strip_path=true" > /dev/null

echo "[setup] attaching concurrency-limit plugin (max_concurrency=${MAX_CONCURRENCY}, burst_concurrency=${BURST_CONCURRENCY}, default_conn_delay=${DEFAULT_CONN_DELAY}, dry_run=${DRY_RUN})"

# Remove any existing instance on this route first so re-runs are idempotent.
EXISTING_ID=$(curl -sf "${ADMIN_URL}/routes/mock-route/plugins" \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print(next((p['id'] for p in d['data'] if p['name']=='concurrency-limit'), ''))")

if [ -n "${EXISTING_ID}" ]; then
  curl -sf -X DELETE "${ADMIN_URL}/routes/mock-route/plugins/${EXISTING_ID}" > /dev/null
fi

curl -sf -X POST "${ADMIN_URL}/routes/mock-route/plugins" \
  -d "name=concurrency-limit" \
  -d "config.max_concurrency=${MAX_CONCURRENCY}" \
  -d "config.burst_concurrency=${BURST_CONCURRENCY}" \
  -d "config.default_conn_delay=${DEFAULT_CONN_DELAY}" \
  -d "config.dry_run=${DRY_RUN}" > /dev/null

echo "[setup] done. Proxy route ready at ${KONG_PROXY_URL:-http://localhost:8000}/mock/anything?delay=2"
