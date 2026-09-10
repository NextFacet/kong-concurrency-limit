#!/usr/bin/env bash
# Fires N concurrent requests at the mock route through Kong and tallies the
# HTTP status codes returned, to demonstrate the concurrency-limit plugin
# admitting up to max_concurrency (+burst) requests and rejecting the rest.
#
# Usage:
#   ./load-test.sh [concurrency] [upstream_delay_seconds]
#
# Example: 20 simultaneous requests, each taking 3s upstream -> with
# max_concurrency=5/burst=0 you should see 5x 200 and 15x 429.
set -euo pipefail

PROXY_URL="${KONG_PROXY_URL:-http://localhost:8000}"
ROUTE_PATH="${ROUTE_PATH:-/mock/anything}"
CONCURRENCY="${1:-20}"
DELAY="${2:-3}"

tmpdir=$(mktemp -d)
trap 'rm -rf "${tmpdir}"' EXIT

echo "[load-test] firing ${CONCURRENCY} concurrent requests at ${PROXY_URL}${ROUTE_PATH} (upstream delay=${DELAY}s)..."

start=$(date +%s.%N)
for i in $(seq 1 "${CONCURRENCY}"); do
  (
    code=$(curl -s -o /dev/null -w "%{http_code}" "${PROXY_URL}${ROUTE_PATH}?delay=${DELAY}")
    echo "${code}" > "${tmpdir}/${i}.code"
  ) &
done
wait
end=$(date +%s.%N)

echo "[load-test] all requests completed in $(echo "${end} - ${start}" | bc)s"
echo "[load-test] status code breakdown:"
cat "${tmpdir}"/*.code | sort | uniq -c | sort -rn
