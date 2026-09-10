#!/bin/sh
# Installs / updates the "concurrency-limit" plugin onto an ALREADY RUNNING
# Kong node, without rebuilding or recreating the container/host. This is
# the exact sequence you'd run on the real bare-metal production Kong box:
#
#   1. copy the plugin's Lua files onto Kong's Lua package path
#   2. tell Kong the plugin is enabled (KONG_PLUGINS / kong.conf `plugins`)
#   3. declare the shared-memory zone the plugin's counter lives in
#   4. `kong reload` to pick both up via a graceful (zero-downtime) reload
#
# Written in POSIX sh (no bashisms) so it runs unmodified both inside the
# Alpine-based Kong docker image (used for local testing, see docker/) and
# directly on the bare-metal production Kong host (which has no docker at
# all) -- same script, same procedure, either place.
#
# Usage:
#   docker exec -u root <kong-container> /opt/scripts/deploy-plugin.sh   # docker test env
#   sudo ./deploy-plugin.sh                                              # bare-metal prod host
#
# Env overrides (rarely needed):
#   PLUGIN_SRC_DIR    - where the plugin source is mounted (default below)
#   KONG_LUA_LIB_DIR  - Kong's Lua library root (default: /usr/local/share/lua/5.1)
#   KONG_PREFIX_DIR   - Kong's running prefix, where pids/nginx.pid lives
#                       (default: /usr/local/kong)
#   DEPLOY_AS_USER    - force the user `kong reload` runs as, instead of
#                       auto-detecting it from the running nginx master
#                       process (see below for why this matters)

set -eu

PLUGIN_NAME="concurrency-limit"
SRC_DIR="${PLUGIN_SRC_DIR:-/opt/plugin-src/kong/plugins/${PLUGIN_NAME}}"
LUA_LIB_DIR="${KONG_LUA_LIB_DIR:-/usr/local/share/lua/5.1}"
DEST_DIR="${LUA_LIB_DIR}/kong/plugins/${PLUGIN_NAME}"
KONG_PREFIX_DIR="${KONG_PREFIX_DIR:-/usr/local/kong}"
SHARED_DICT_DECL="concurrency_limit_store 10m"

echo "[deploy-plugin] source: ${SRC_DIR}"
echo "[deploy-plugin] target: ${DEST_DIR}"

if [ ! -f "${SRC_DIR}/handler.lua" ] || [ ! -f "${SRC_DIR}/schema.lua" ]; then
  echo "[deploy-plugin] ERROR: plugin source not found at ${SRC_DIR}" >&2
  exit 1
fi

mkdir -p "${DEST_DIR}"
cp "${SRC_DIR}/handler.lua" "${DEST_DIR}/handler.lua"
cp "${SRC_DIR}/schema.lua" "${DEST_DIR}/schema.lua"
# api.lua (Admin API routes for inspecting/resetting a stuck counter) is
# optional -- older checkouts of this plugin may not have it yet -- but copy
# it whenever present so `kong reload` picks up the Admin API routes too.
if [ -f "${SRC_DIR}/api.lua" ]; then
  cp "${SRC_DIR}/api.lua" "${DEST_DIR}/api.lua"
fi
echo "[deploy-plugin] plugin files copied."

EXISTING_PLUGINS="${KONG_PLUGINS:-bundled}"
case ",${EXISTING_PLUGINS}," in
  *",${PLUGIN_NAME},"*) NEW_PLUGINS="${EXISTING_PLUGINS}" ;;
  *) NEW_PLUGINS="${EXISTING_PLUGINS},${PLUGIN_NAME}" ;;
esac

echo "[deploy-plugin] reloading Kong with:"
echo "[deploy-plugin]   KONG_PLUGINS=${NEW_PLUGINS}"
echo "[deploy-plugin]   KONG_NGINX_HTTP_LUA_SHARED_DICT=${SHARED_DICT_DECL}"

# `kong reload` regenerates the prefix's .kong_env / nginx-kong.conf files as
# whichever user runs it, then sends the running nginx master a graceful
# SIGHUP -- and the master re-reads .kong_env itself on that SIGHUP, AS
# ITSELF. So reload must be issued as whatever user the nginx master is
# already running as, or the regenerated .kong_env comes out owned by the
# wrong user/mode and the master can't read its own config on reload,
# crashing it. That user varies by install (non-root "kong" in the official
# docker image; often root on a bare-metal systemd-managed install) so it's
# detected from the live process rather than assumed.
NGINX_PID=""
if [ -f "${KONG_PREFIX_DIR}/pids/nginx.pid" ]; then
  NGINX_PID=$(cat "${KONG_PREFIX_DIR}/pids/nginx.pid")
fi

RELOAD_AS_USER="${DEPLOY_AS_USER:-}"
if [ -z "${RELOAD_AS_USER}" ] && [ -n "${NGINX_PID}" ] && [ -f "/proc/${NGINX_PID}/status" ]; then
  # Read the owning uid straight from procfs and resolve it via /etc/passwd
  # rather than shelling out to `ps -p`, whose flags differ between BusyBox
  # (Alpine, used by the official Kong image) and full util-linux (typical
  # on a bare-metal RHEL/Ubuntu host).
  NGINX_UID=$(awk '/^Uid:/{print $2}' "/proc/${NGINX_PID}/status")
  RELOAD_AS_USER=$(awk -F: -v uid="${NGINX_UID}" '$3==uid{print $1; exit}' /etc/passwd)
fi

RELOAD_CMD="KONG_PLUGINS='${NEW_PLUGINS}' KONG_NGINX_HTTP_LUA_SHARED_DICT='${SHARED_DICT_DECL}' kong reload"

if [ -z "${RELOAD_AS_USER}" ] || [ "${RELOAD_AS_USER}" = "$(id -un)" ]; then
  echo "[deploy-plugin] running reload as current user ($(id -un))"
  export KONG_PLUGINS="${NEW_PLUGINS}"
  export KONG_NGINX_HTTP_LUA_SHARED_DICT="${SHARED_DICT_DECL}"
  kong reload
else
  echo "[deploy-plugin] nginx master runs as '${RELOAD_AS_USER}'; reloading as that user"
  su -s /bin/sh "${RELOAD_AS_USER}" -c "${RELOAD_CMD}"
fi

echo "[deploy-plugin] reload issued, waiting for Kong to settle..."
sleep 2

if kong health >/dev/null 2>&1; then
  echo "[deploy-plugin] kong is healthy."
else
  echo "[deploy-plugin] WARNING: 'kong health' did not report healthy after reload." >&2
  exit 1
fi

echo "[deploy-plugin] done. NOTE: this is a hot/runtime install. If the Kong"
echo "[deploy-plugin] process or container is fully restarted from its"
echo "[deploy-plugin] original startup config, re-run this script (or make"
echo "[deploy-plugin] the KONG_PLUGINS / shared-dict settings permanent in"
echo "[deploy-plugin] kong.conf) or the plugin will be gone again."
