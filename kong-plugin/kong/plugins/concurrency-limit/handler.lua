-- Kong plugin: concurrency-limit
--
-- Caps the number of requests that may be *in flight at the same time* for
-- the service/route this plugin is attached to, as opposed to Kong's stock
-- rate-limiting plugin which caps requests *per time window*. This is meant
-- for slow, resource-heavy endpoints (e.g. large file processing) where too
-- many simultaneous calls -- not too many calls per minute -- is what hurts
-- the upstream.
--
-- Built on resty.limit.conn (lua-resty-limit-traffic, bundled with the
-- OpenResty distribution Kong itself runs on -- v0.07 as shipped in Kong
-- 2.5.1's kong:2.5.1-alpine image; no extra rocks required). Its data model
-- is a single integer-per-key counter in an nginx shared dict, incremented
-- by incoming() and decremented by leaving() -- NOT a per-request lease/TTL
-- system. See README "Risk assessment / stale counter" for what that does
-- and doesn't guarantee.

local limit_conn = require "resty.limit.conn"

local kong = kong
local ngx = ngx

-- Must match the `lua_shared_dict` zone declared for Kong at startup
-- (see docker-compose / kong.conf: KONG_NGINX_HTTP_LUA_SHARED_DICT).
local SHARED_DICT_NAME = "concurrency_limit_store"

-- `key` below (e.g. "concurrency-limit:<route_id>") is the exact dict key
-- resty.limit.conn itself increments/decrements -- api.lua and this file
-- must both read that same, un-prefixed-further string for the live count.
-- The extra bookkeeping this plugin keeps alongside it (rejects, errors,
-- log debounce) is namespaced by simple suffixes on that same key so it's
-- always obvious which counter belongs to which plugin instance.
local function build_key(conf)
  -- Kong injects route_id/service_id/consumer_id into every plugin's `conf`
  -- automatically; scope the counter to whichever entity this plugin
  -- instance is bound to so separate routes/services don't share a bucket.
  return "concurrency-limit:" .. (conf.route_id or conf.service_id or "global")
end

-- Emits a WARN-level "at/near limit" diagnostic at most once per
-- config.rejection_log_interval seconds *per key* -- rejections themselves
-- always still happen immediately for every request; only this extra log
-- line is throttled, via dict:add() as an atomic debounce (add fails
-- without side effects if a debounce marker is already present and unexpired,
-- so concurrent workers/requests can't double-log a burst of rejections).
local function log_rejection_throttled(conf, key, dry_run, current, dict)
  local ok = dict:add(key .. ":log-debounce", true, conf.rejection_log_interval)
  if not ok then
    return -- logged recently for this key; skip
  end

  local verb = dry_run and "(dry-run) would reject" or "rejecting"
  kong.log.warn("[concurrency-limit] ", verb, " '", key,
                 "': ", current, "/", conf.max_concurrency, " concurrency slots in use")
end

local ConcurrencyLimitHandler = {
  -- Runs after authentication-class plugins (key-auth, jwt, etc. default to
  -- priority >= 1000) so only authenticated traffic consumes a concurrency
  -- slot, but before traffic-shaping plugins like rate-limiting (901) and
  -- acl (950), so a request that will be blocked by concurrency pressure
  -- never reaches later checks.
  PRIORITY = 1000,
  VERSION = "1.2.1",
}

function ConcurrencyLimitHandler:access(conf)
  local dict = ngx.shared[SHARED_DICT_NAME]
  if not dict then
    kong.log.err("[concurrency-limit] shared dict '", SHARED_DICT_NAME, "' not found -- ",
                 "is KONG_NGINX_HTTP_LUA_SHARED_DICT / nginx_http_lua_shared_dict configured?")
    return -- fail open: never block traffic because of a plugin-internal/deploy error
  end

  local lim, err = limit_conn.new(SHARED_DICT_NAME, conf.max_concurrency, conf.burst_concurrency, conf.default_conn_delay)
  if not lim then
    kong.log.err("[concurrency-limit] failed to instantiate limiter: ", err)
    return -- fail open
  end

  local key = build_key(conf)

  -- Retain this SAME limiter instance (and key) for the log phase instead of
  -- constructing a fresh one there, per resty.limit.conn's own documented
  -- multi-phase usage pattern (see lua-resty-limit-traffic README: stash the
  -- object from access_by_lua in ngx.ctx, reuse it in log_by_lua). Stashed
  -- immediately -- before incoming() is even called -- so log() can still
  -- find it and correctly see is_committed() == false even if something
  -- below throws before this function returns normally.
  --
  -- Reuse also matters for a second reason: leaving() feeds each request's
  -- observed latency back into lim.unit_delay (an exponential moving
  -- average used to size the burst queue delay). A fresh lim per phase
  -- would reset that from conf.default_conn_delay every time and make the
  -- self-tuning a no-op; reusing the instance is required for it to do
  -- anything even within a single request's own two phases (there is
  -- currently no persistence of the tuned average *across* requests -- see
  -- README).
  kong.ctx.plugin.lim = lim
  kong.ctx.plugin.key = key

  local delay, limit_err = lim:incoming(key, true)
  if not delay then
    if limit_err == "rejected" then
      local current = dict:get(key) or conf.max_concurrency
      dict:incr(key .. ":rejected", 1, 0)
      log_rejection_throttled(conf, key, conf.dry_run, current, dict)

      if conf.dry_run then
        return
      end
      return kong.response.exit(conf.response_code, { message = conf.response_message })
    end

    dict:incr(key .. ":incoming-errors", 1, 0)
    kong.log.err("[concurrency-limit] error limiting '", key, "': ", limit_err)
    return -- fail open
  end

  if delay > 0 then
    -- Admitted, but only within the configured burst allowance: hold the
    -- request briefly so it doesn't hammer the upstream immediately. NOTE:
    -- this slot was already committed (incremented) above, before this
    -- sleep -- with burst_concurrency > 0, true concurrent upstream load
    -- can exceed max_concurrency for as long as this delay under-estimates
    -- real service time. See README "Hard limit vs burst" before relying on
    -- burst_concurrency > 0 for a strict cap.
    ngx.sleep(delay)
  end

  kong.ctx.plugin.start_time = ngx.now()
end

function ConcurrencyLimitHandler:log(conf)
  -- Release first, as the very first thing this function does, so no
  -- unrelated code below could ever run first and prevent the slot from
  -- being freed.
  local lim = kong.ctx.plugin.lim
  if not lim or not lim:is_committed() then
    return
  end

  local key = kong.ctx.plugin.key
  local latency = ngx.now() - (kong.ctx.plugin.start_time or ngx.now())
  local conn, err = lim:leaving(key, latency)
  if not conn then
    kong.log.err("[concurrency-limit] failed to release slot for '", key, "': ", err)
    local dict = ngx.shared[SHARED_DICT_NAME]
    if dict then
      dict:incr(key .. ":leaving-errors", 1, 0)
    end
  end
end

return ConcurrencyLimitHandler
