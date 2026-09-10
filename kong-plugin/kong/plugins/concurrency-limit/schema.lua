local typedefs = require "kong.db.schema.typedefs"

return {
  name = "concurrency-limit",
  fields = {
    { consumer = typedefs.no_consumer },
    { protocols = typedefs.protocols_http },
    { config = {
        type = "record",
        fields = {
          -- Hard ceiling on requests that may be in flight (per Kong node) for
          -- the service/route this plugin instance is attached to.
          { max_concurrency = {
              type = "integer",
              required = true,
              default = 10,
              gt = 0,
          } },
          -- Extra requests allowed beyond max_concurrency that get queued
          -- (delayed) instead of rejected outright. 0 = hard reject at the limit.
          { burst_concurrency = {
              type = "number",
              required = true,
              default = 0,
              -- Kong 2.5.1's schema DSL has no `gte`; `between` is the
              -- closest inclusive-lower-bound check available.
              between = { 0, 1000000 },
          } },
          -- Estimated average time (seconds) to process one request. Used by
          -- the underlying algorithm to compute how long a queued (burst)
          -- request should be delayed. Tune this to the real upstream latency
          -- (e.g. how long the slow backend endpoint typically takes) for
          -- accurate queueing behaviour.
          { default_conn_delay = {
              type = "number",
              required = true,
              default = 1,
              gt = 0,
          } },
          -- HTTP status returned to clients rejected for exceeding the limit.
          -- 429 (Too Many Requests) by default -- the client can retry, same
          -- family Kong's own rate-limiting plugins use for this kind of
          -- rejection; reserve burst_concurrency for cases where a brief
          -- queued wait is actually wanted instead of an immediate reject.
          { response_code = {
              type = "integer",
              required = true,
              default = 429,
              between = { 100, 599 },
          } },
          { response_message = {
              type = "string",
              required = true,
              default = "Too many concurrent requests, please retry later.",
          } },
          -- When true, never actually rejects requests -- only logs what it
          -- would have done. Useful for rolling this out against production
          -- traffic to size max_concurrency before enforcing it.
          { dry_run = {
              type = "boolean",
              required = true,
              default = false,
          } },
          -- Minimum seconds between "at/near limit" WARN log lines for the
          -- same route/service, so sustained overload produces one line
          -- every N seconds rather than one per rejected request. Does NOT
          -- throttle the rejections themselves -- every over-limit request
          -- still gets an immediate response either way.
          { rejection_log_interval = {
              type = "number",
              required = true,
              default = 10,
              gt = 0,
          } },
      },
    } },
  },
}
