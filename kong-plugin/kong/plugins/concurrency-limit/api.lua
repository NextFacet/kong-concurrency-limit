-- Admin API routes for concurrency-limit: read-only visibility into the
-- live in-flight counters (for spotting a suspiciously stuck one), plus a
-- deliberate, human-triggered reset for when one is confirmed stuck.
--
-- IMPORTANT: this state is per Kong NODE, not cluster-wide -- see README
-- "Multi-worker / multi-node behaviour". These routes only see/affect the
-- shared dict on whichever node answers the Admin API request. In a
-- multi-node deployment, repeat GET/DELETE against each node individually
-- (there is deliberately no cross-node broadcast here -- see README for
-- why that was left out).
--
-- Also NOTHING in this file is ever called by the plugin itself; DELETE is
-- purely an operator action, never automatic. See README "Suspected stuck
-- counter" runbook for when to use it.

local kong = kong

local SHARED_DICT_NAME = "concurrency_limit_store"

local function shared_dict()
  local dict = ngx.shared[SHARED_DICT_NAME]
  if not dict then
    return nil, "shared dict '" .. SHARED_DICT_NAME .. "' not found"
  end
  return dict
end

local function each_concurrency_limit_plugin()
  local iter = kong.db.plugins:each()
  return function()
    while true do
      local plugin, err = iter()
      if err then
        error(err) -- caught by the pcall-style loop below via kong.response.exit callers
      end
      if not plugin then
        return nil
      end
      if plugin.name == "concurrency-limit" then
        return plugin
      end
    end
  end
end

local function plugin_key(plugin)
  local route_id = plugin.route and plugin.route.id
  local service_id = plugin.service and plugin.service.id
  return "concurrency-limit:" .. (route_id or service_id or "global"), route_id, service_id
end

return {
  -- Snapshot of every concurrency-limit instance configured on this node,
  -- with its live counter next to its configured max. The primary tool for
  -- answering "is anything stuck?" without needing to know route ids
  -- ahead of time.
  ["/concurrency-limit"] = {
    GET = function()
      local dict, err = shared_dict()
      if not dict then
        return kong.response.exit(500, { message = err })
      end

      local ok, instances_or_err = pcall(function()
        local instances = {}
        for plugin in each_concurrency_limit_plugin() do
          local key, route_id, service_id = plugin_key(plugin)
          instances[#instances + 1] = {
            plugin_id = plugin.id,
            route_id = route_id,
            service_id = service_id,
            key = key,
            max_concurrency = plugin.config.max_concurrency,
            burst_concurrency = plugin.config.burst_concurrency,
            dry_run = plugin.config.dry_run,
            current_concurrency = dict:get(key) or 0,
            rejected_total = dict:get(key .. ":rejected") or 0,
            incoming_errors_total = dict:get(key .. ":incoming-errors") or 0,
            leaving_errors_total = dict:get(key .. ":leaving-errors") or 0,
          }
        end
        return instances
      end)

      if not ok then
        return kong.response.exit(500, { message = instances_or_err })
      end

      return kong.response.exit(200, {
        node_id = kong.node.get_id(),
        data = instances_or_err,
      })
    end,
  },

  -- Inspect (GET) or forcibly clear (DELETE) a single counter by the raw
  -- route_id or service_id the plugin instance is attached to (see the
  -- `key`/`route_id`/`service_id` fields from the list endpoint above).
  ["/concurrency-limit/:entity_id"] = {
    GET = function(self)
      local dict, err = shared_dict()
      if not dict then
        return kong.response.exit(500, { message = err })
      end

      local key = "concurrency-limit:" .. self.params.entity_id
      return kong.response.exit(200, {
        node_id = kong.node.get_id(),
        key = key,
        current_concurrency = dict:get(key) or 0,
        rejected_total = dict:get(key .. ":rejected") or 0,
        incoming_errors_total = dict:get(key .. ":incoming-errors") or 0,
        leaving_errors_total = dict:get(key .. ":leaving-errors") or 0,
      })
    end,

    -- Forces the live in-flight counter back to 0 on THIS node. This is a
    -- blunt instrument: only use it once you've confirmed (via real
    -- upstream/backend traffic data, not guesswork) that no genuine
    -- in-flight requests for this key remain on this node. See README
    -- "Suspected stuck counter" runbook.
    DELETE = function(self)
      local dict, err = shared_dict()
      if not dict then
        return kong.response.exit(500, { message = err })
      end

      local key = "concurrency-limit:" .. self.params.entity_id
      local previous = dict:get(key) or 0
      local ok, set_err = dict:safe_set(key, 0)
      if not ok then
        return kong.response.exit(500, { message = set_err })
      end

      kong.log.warn("[concurrency-limit] admin API reset counter '", key,
                     "' from ", previous, " to 0 on node ", kong.node.get_id())

      return kong.response.exit(200, {
        node_id = kong.node.get_id(),
        key = key,
        previous_value = previous,
        reset_to = 0,
      })
    end,
  },
}
