# kong-concurrency-limit

A custom Kong (OSS) plugin that caps the number of requests that may be **in
flight at the same time** for a given service/route -- independent of Kong's
stock rate-limiting plugin, which caps requests *per time window*, not
concurrency. Useful for protecting slow, resource-heavy backend endpoints
(large file processing, heavy report generation, anything that takes tens of
seconds rather than milliseconds) from being overwhelmed by too many
simultaneous callers, in front of a backend that has no concurrency
protection of its own.

This repo contains the plugin itself, a full Docker-based test harness for
it, and an 8-hour soak test tool.

Developed and tested against Kong **2.5.1**, installed directly on Linux (no
containers) in the target deployment this was originally built for. The
Docker setup here is only for local development/testing; see [Deploying to
production](#deploying-to-production-bare-metal-no-docker) for the
non-Docker install path.

## How it works

The plugin is built on `resty.limit.conn` (part of `lua-resty-limit-traffic`,
which ships with the OpenResty distribution Kong itself runs on -- no extra
rocks needed). It keeps an in-flight counter per plugin instance (i.e. per
route/service it's attached to) in an nginx shared-memory dict:

- `access` phase: try to take a slot for the incoming request.
  - Under `max_concurrency`: admitted immediately.
  - Over `max_concurrency` but within `max_concurrency + burst_concurrency`:
    admitted, but delayed briefly (queued) so the upstream isn't hit all at
    once.
  - Over both: rejected immediately (no waiting) with `config.response_code`
    (default `429`) -- or, in `dry_run` mode, let through and just logged.
- `log` phase: release the slot once the response has been sent.

Config is entirely per plugin-instance, so different routes can have
different limits by just attaching the plugin separately with different
`config`.

### Config reference

| Field | Type | Default | Meaning |
|---|---|---|---|
| `max_concurrency` | integer | `10` | Hard cap on simultaneous in-flight requests for this route/service. |
| `burst_concurrency` | number | `0` | Extra requests allowed to queue (delayed, not rejected) beyond `max_concurrency`. `0` = reject immediately at the limit. |
| `default_conn_delay` | number | `1` | Estimated average seconds to process one request; used to compute how long a queued request waits. Tune to the real upstream latency. |
| `response_code` | integer | `429` | HTTP status returned to rejected clients. |
| `response_message` | string | `"Too many concurrent requests, please retry later."` | Body message for rejected clients. |
| `dry_run` | boolean | `false` | If true, never actually rejects -- only logs what it would have done. Use to size `max_concurrency` against real traffic before enforcing it. |
| `rejection_log_interval` | number (seconds) | `10` | Minimum gap between "at/near limit" WARN log lines for the same route, so sustained overload logs once every N seconds instead of once per rejected request. Rejections themselves are never throttled -- every over-limit request still gets an immediate response. |

### Important caveats

- **Immediate rejection is the default.** With `burst_concurrency = 0`
  (default), a request over `max_concurrency` is rejected right away --
  `429` immediately, no waiting. Setting `burst_concurrency > 0` is an
  opt-in trade: those extra requests get queued (delayed via `ngx.sleep`,
  up to roughly `default_conn_delay` seconds) instead of rejected, which
  smooths out brief spikes at the cost of added latency. If fail-fast is
  what you want, leave `burst_concurrency` at `0`.
- **Scope: shared within a node, independent across nodes.**
  `ngx.shared.DICT` is one memory region owned by the Kong/nginx **master**
  process and mapped into every **worker** process it forks -- so within a
  single Kong node, all workers (however many `nginx_worker_processes` you
  run) see and update the exact same counter correctly; concurrency really
  is capped node-wide, not per-worker. Across **separate Kong
  nodes/pods/containers**, each has its own independent OS process and thus
  its own independent memory -- there is no sharing between them. So with
  `max_concurrency = 10` and 3 Kong replicas, the effective ceiling on the
  shared backend is up to `3 x 10 = 30` concurrent requests, not 10. Do not
  describe or rely on this as cluster-wide -- it isn't. A true global cap
  would require centralizing the counter (e.g. Redis `INCR`/`DECR`), which
  adds latency and an external dependency and isn't implemented here.
- **Fails open.** If the plugin hits an internal error (e.g. the shared dict
  isn't declared), it logs and lets the request through rather than blocking
  traffic on a plugin bug.
- **Plugin ordering.** `PRIORITY = 1000`, so it runs after auth-class plugins
  (key-auth, jwt, etc. default to priority >= 1000) but before rate-limiting
  (901) / acl (950) -- only authenticated traffic (if auth is configured on
  the route) consumes a concurrency slot.
- **A counter can leak, in one specific residual scenario.** See
  [Lifecycle hardening](#lifecycle-hardening-leak-prevention--recovery)
  below for exactly which scenarios are safe, which one genuinely isn't, and
  how to detect/recover from it.

## Lifecycle hardening: leak prevention & recovery

`resty.limit.conn`'s entire data model is **one integer per key** in an
nginx shared dict, incremented by `incoming()` and decremented by
`leaving()`. It is not a per-request lease/TTL system -- it has no concept
of "whose" slot a given increment belongs to, so it cannot individually
expire a specific stale slot. Every design choice below follows from that
one fact.

### How `incoming()` / `leaving()` actually behave

(Read straight from the library as shipped -- `/usr/local/openresty/lualib/resty/limit/conn.lua`,
`_VERSION = '0.07'`, in the `kong:2.5.1-alpine` image -- rather than assumed.)

- `incoming(key, true)` (`commit=true`, what this plugin uses) atomically
  `dict:incr(key, 1, 0)`s the counter. If the result exceeds `max + burst`,
  it immediately decrements back and returns `nil, "rejected"` -- so a
  rejected request never holds a slot. Otherwise it's committed, and
  `self.committed` is set to `true` **on that specific Lua object instance**.
- `is_committed()` just returns that instance's `self.committed`. It only
  means anything if you call it on the *same* `lim` object `incoming()` was
  called on -- a freshly `.new()`-created object always reports
  `is_committed() == false`, regardless of what any other instance did. The
  original version of this plugin called `limit_conn.new()` again inside
  `log()`, which made `is_committed()` on that object meaningless (always
  false) and is exactly the kind of bug this hardening pass fixes.
- `leaving(key, latency)` does `dict:incr(key, -1)` and returns the new
  value (or `nil, err` if the dict-level operation itself fails -- e.g. the
  dict doesn't exist, or the key was evicted in between). It also folds
  `latency` into `self.unit_delay`, an exponential moving average used to
  size the burst queue delay -- another reason to reuse the same instance
  rather than recreate it.

### Source changes made

1. **Reuse the same `lim` object across `access()` and `log()`** via
   `kong.ctx.plugin.lim`, per `lua-resty-limit-traffic`'s own documented
   multi-phase pattern, instead of calling `limit_conn.new()` a second time.
   `log()` now gates release on that object's real `lim:is_committed()`
   rather than a hand-maintained boolean that happened to mirror it
   correctly but wasn't the library's own source of truth.
2. **`kong.ctx.plugin.lim`/`.key` are stashed immediately after
   `limit_conn.new()` succeeds**, before `incoming()` is even called -- so
   if anything *after* that point throws, `log()` can still find them and
   correctly ask `is_committed()` (which will honestly answer `false` if
   the throw happened before commit, `true` if after).
3. **`log()` calls `lim:leaving()` as its first real operation**, with
   nothing but a nil-guard ahead of it, so no unrelated code (metrics,
   logging) can run first and prevent the release.
4. **Operational counters** (`<key>:rejected`, `<key>:incoming-errors`,
   `<key>:leaving-errors`) are maintained alongside the main counter,
   exposed read-only via the new Admin API routes (below).
5. **Rejection log lines are debounced** per key via `dict:add()` (an
   atomic "only if absent" shared-dict op) so sustained overload produces
   one WARN line every `rejection_log_interval` seconds, not one per
   rejected request -- the rejections themselves are never delayed by this.

### Failure scenario matrix

Verified empirically against a live Kong 2.5.1 container (`docker exec`
into it, drive real traffic, read the live counter back via the new Admin
API) rather than assumed -- see `scripts/test-lifecycle.sh` for the
automated version of most of these.

| Scenario | Does `log()` run? | Slot released? | Notes |
|---|---|---|---|
| Normal success | Yes | Yes | |
| Backend 4xx/5xx | Yes | Yes | Same code path as normal success; the upstream status code doesn't change nginx's phase handling. |
| Upstream timeout (Kong returns 504) | Yes | Yes | **Verified**: forced a 3s upstream against a 500ms `read_timeout`; counter returned to 0 immediately after the 504. |
| Client disconnects mid-request | Yes | Yes | Documented nginx/OpenResty behaviour: the request is still finalized (and logged) server-side even though nothing more can be written to the closed client socket. Relied on rather than re-proven -- it's a core nginx guarantee independent of this plugin. |
| An earlier-priority plugin (or Kong itself) terminates the request before our `access()` runs | N/A | N/A -- nothing was ever committed | **Verified**: attached `key-auth` (priority 1003, ahead of our 1000) with no credentials; request got `401` from key-auth, our `access()` never ran, `log()`'s nil-guard on `kong.ctx.plugin.lim` no-opped cleanly. |
| Lua runtime error in **our own `access()`**, after `incoming()` already committed | Yes | Yes | **Verified**: deliberately injected `error(...)` right after a successful commit; client got `500`, but the counter net-change was exactly zero -- Kong's log phase still ran and released it. This is the scenario the "reuse + stash-early" change (source changes #1-2 above) directly targets. |
| `leaving()` itself returns an error (dict-level failure) | Yes | **No** | Genuinely not released. Counted in `<key>:leaving-errors`. In practice this needs the dict to be missing or the key to have been evicted (LRU) between `incoming()` and `leaving()` -- very unlikely for a small, actively-used dict sized per the guidance below, but not impossible. |
| nginx **worker** process crash/OOM-kill/`kill -9` mid-request | **No** | **No -- genuine leak** | The one scenario this plugin cannot protect against by itself. The worker dies before `log_by_lua` runs for its in-flight requests; the master respawns a replacement worker, but the shared dict (owned by the still-alive master) keeps the stale increment. This is the residual risk `is_committed()`/reuse/early-release cannot close -- see recovery below. |
| Full Kong process/container/pod restart | New process | Self-heals to 0 | Shared memory zones are allocated by the master process's own address space; a brand-new process gets a brand-new, empty dict. **But see the crash-loop warning in "Deploying to production" -- restarting is not a safe casual fix if the plugin is already attached to a route and not durably enabled in `kong.conf`.** |
| `kong reload` (graceful config reload) | Master reloads config, same process | **Does NOT clear the dict** | **Verified**: put a request in flight, ran `kong reload` mid-flight, counter was unchanged immediately after the reload, and released normally once the request completed. Nginx reuses an existing shared-memory zone across reload when its name+size match (well-established nginx/OpenResty behaviour, exploited on purpose here so counters survive routine config reloads) -- so `kong reload` is **not** a way to clear a stuck counter; only a full process restart is. |

### Why there's no automatic TTL/lease-based recovery

The request explicitly asked for this to be investigated rather than
assumed, and to be told plainly if it isn't safely possible: **it isn't,
not with `resty.limit.conn`'s data model, without changing that model
entirely.**

A safe per-slot expiry needs to know *which* increments are stale versus
genuinely still in flight. That requires tracking each admitted request as
its own entry (e.g. `key:<request_id> -> started_at`) and deriving "current
concurrency" by counting/expiring those individually -- a different design
(closer to a hand-rolled semaphore/lease table) than a single aggregate
integer. `ngx.shared.DICT` doesn't have an efficient way to enumerate keys
by prefix in a hot path (`get_keys()` is a full, expensive scan), so this
would add real per-request overhead and meaningfully more code for a
scenario (worker crashes) that should be rare.

The blanket alternative -- reset the whole counter to 0 on a timer -- is
exactly what the request warned against, and correctly so: with
legitimately long-running requests (this plugin exists for slow,
resource-heavy backend calls), a time-based reset while real requests are still in flight
directly reopens the over-concurrency problem the plugin exists to prevent.
**Confirmed the hard way**: `scripts/test-lifecycle.sh` step 8
deliberately resets a counter while a real request is still in flight, and
the counter goes negative once that request's own release lands on top of
the reset -- harmless in *that* specific case (it self-corrects upward,
and the direction of the error is fail-open, not fail-closed), but it's
concrete proof that resetting at the wrong moment does actively corrupt
the count, not just "waste" a slot.

Given both of those, the design here is, in the order the request asked
for: (1) correct `resty.limit.conn` usage (above) to make the *only*
realistic leak vector "an nginx worker actually crashes" rather than any
bug in this plugin; (2) detection and manual, deliberate recovery, not
silent automatic correction.

### Detection & recovery: the Admin API routes

`api.lua` adds two Admin API routes (proxied through Kong's Admin API,
`localhost:8001` in the docker setup -- **this is only as safe as your
Admin API network access already is**; it carries no auth of its own,
same as every other built-in Admin API route):

```sh
# Every concurrency-limit instance on THIS node, with its live counter next
# to its configured max -- the first thing to check if a route seems to be
# rejecting more than expected.
curl localhost:8001/concurrency-limit

# One instance by its route_id or service_id (from the list above)
curl localhost:8001/concurrency-limit/<route_id>

# Force it back to 0 on THIS node -- a deliberate, human action, never
# called by the plugin itself. See the runbook below before using it.
curl -X DELETE localhost:8001/concurrency-limit/<route_id>
```

**This state is per Kong node**, same as the counter itself -- in a
multi-node deployment you need to check/reset each node individually.
There's deliberately no cross-node broadcast (Kong's `cluster_events` could
do this, similar to how the bundled `proxy-cache` plugin broadcasts purges)
-- left out to keep this plugin small, since it isn't needed to *detect* a
problem (each node's own counters are independently meaningful), only to
make a *reset* one-call-covers-everything. Worth adding later if
multi-node manual resets become a frequent operation.

### Runbook: suspected stuck counter

Symptom: a route is rejecting (`429`/configured `response_code`) requests
that a check of the actual backend (its own logs/APM, or the mock
upstream's own `inflight_peak_seen` in this test setup) shows isn't
actually busy.

1. **Confirm, don't guess.** `curl localhost:8001/concurrency-limit` on the
   node(s) serving that route. Compare `current_concurrency` against real
   backend concurrency from an independent source. A high `rejected_total`
   alone is not proof of a leak -- it may just mean the limit is set too
   low for real traffic (consider `dry_run` + raising `max_concurrency`
   instead).
2. **Prefer restart over reset if you can afford a brief blip**, since it's
   the one option that's *guaranteed* correct: restarting the Kong
   process/pod recreates the shared dict empty. **Before doing this,
   confirm `concurrency-limit` is durably enabled** (`plugins =` in
   `kong.conf`, not just a prior hot `deploy-plugin.sh` reload) -- see the
   crash-loop warning below. In a multi-node deployment, restart nodes one
   at a time behind the load balancer, not all at once.
3. **If a restart isn't acceptable right now**, and you've confirmed via
   independent data that the count really is stuck (not just busy), use
   `DELETE /concurrency-limit/<route_id>` on the affected node. Do this
   only once you're confident no genuine in-flight request for that key
   remains -- resetting too early can drive the counter negative (see
   above); harmless in effect but a sign the reset was premature.
4. **File it.** A stuck counter means a worker actually crashed underneath
   Kong; that's worth investigating in its own right (OOM? a segfault in
   some other module? host memory pressure?), independent of this plugin.

## Repo layout

```
kong-plugin/                          Kong plugin source
  concurrency-limit-1.0.0-1.rockspec  luarocks packaging (optional, for a formal install)
  kong/plugins/concurrency-limit/
    handler.lua
    schema.lua
    api.lua                          Admin API routes: inspect/reset a counter (see "Lifecycle hardening")
docker/
  docker-compose.yml                  plain kong:2.5.1-alpine + postgres + mock upstream
mock-upstream/                        stand-in for a slow, resource-heavy backend endpoint
  Dockerfile
  server.py
scripts/
  deploy-plugin.sh                    installs/reloads the plugin onto a RUNNING Kong
                                       (works both inside the docker container and,
                                       unmodified, on the bare-metal prod host)
  setup-test-service.sh               creates a test Service/Route + plugin config via Admin API
  load-test.sh                        fires N concurrent requests, tallies status codes
  run-docker-tests.sh                 one-command basic demo: up -> deploy -> configure -> load test -> teardown
  test-lifecycle.sh                   normal + abnormal lifecycle test suite (see "Lifecycle hardening")
```

## Testing with Docker

Requires Docker + Docker Compose v2. Everything runs against a completely
**plain, unmodified** `kong:2.5.1-alpine` image -- the plugin is never baked
into a custom image. Instead, `docker-compose.yml` starts vanilla Kong, and
`scripts/deploy-plugin.sh` installs the plugin onto it afterwards, the same
way it would be installed on the real bare-metal host: copy the plugin's Lua
files onto Kong's Lua path, enable it, and `kong reload`.

### One command

```sh
./scripts/run-docker-tests.sh [max_concurrency] [burst_concurrency] [default_conn_delay]
# e.g.
./scripts/run-docker-tests.sh 5 0 2
```

This brings up the stack, deploys the plugin into the running Kong
container, wires up a test route backed by the mock upstream, runs a
within-limit load test (expect all `200`s) and an overload load test at 4x
the limit (expect a mix of `200`/`429`), then tears everything down.

Sample output from an actual run (`max_concurrency=5`, `burst_concurrency=0`):

```
=== Baseline: concurrency within the limit (should be all 200s) ===
[load-test] all requests completed in 2.252034512s
[load-test] status code breakdown:
      5 200

=== Overload: 4x the limit fired at once (expect a mix of 200 and 429) ===
[load-test] all requests completed in 2.312762871s
[load-test] status code breakdown:
     15 429
      5 200
```

Note both runs finish in ~2.3s total -- the same as the mock upstream's own
2s response delay. The 15 rejected requests above returned essentially
instantly (`429`, no waiting); only the 5 admitted ones actually waited on
the upstream. This is the `burst_concurrency=0` (default) fail-fast path --
see the caveat below for what changes if `burst_concurrency > 0`.

The mock upstream (`mock-upstream/server.py`) also independently tracks and
reports its own peak concurrent-request count, which never exceeded 5
regardless of how many requests were fired at once -- confirming the plugin,
not the backend, is what enforced the cap.

### Step by step (for exploring interactively)

```sh
# 1. Bring up a plain Kong + Postgres + mock upstream
docker compose -f docker/docker-compose.yml up -d

# 2. Install the plugin onto the already-running Kong container
docker exec -u root $(docker compose -f docker/docker-compose.yml ps -q kong) \
  /opt/scripts/deploy-plugin.sh

# 3. Create a test Service/Route and attach the plugin
#    args: max_concurrency burst_concurrency default_conn_delay dry_run
./scripts/setup-test-service.sh 5 0 2 false

# 4. Hammer it
#    args: concurrency upstream_delay_seconds
./scripts/load-test.sh 20 2

# 5. Tear down
docker compose -f docker/docker-compose.yml down -v
```

Admin API is on `localhost:8001`, proxy on `localhost:8000`. The test route
is `GET /mock/anything?delay=<seconds>`.

### Lifecycle test suite

```sh
./scripts/test-lifecycle.sh
```

Self-contained (brings up and tears down its own stack). Asserts PASS/FAIL
against the Admin API's live counters for the scenarios in the [Lifecycle
hardening](#lifecycle-hardening-leak-prevention--recovery) failure-scenario
matrix: normal success, concurrency rejection, burst admission, dry-run,
independent per-route counters, a request blocked upstream of this plugin
(missing context), a missing shared dict (fail-open), a simulated stuck
counter plus Admin API recovery (and the negative-counter risk of resetting
too early), and a deliberately-injected Lua error after a slot is committed
(does `log()` still release it). See the comment block at the top of the
script for the few scenarios (a genuine worker crash; a real client abort)
that aren't practically reproducible in a portable test script, and why.

### Why `deploy-plugin.sh` needs `-u root` and then drops privileges again

Two permission facts about the official Kong image, discovered while
building this:

1. `/usr/local/share/lua/5.1/kong/plugins/` (Kong's Lua plugin path) is not
   writable by the container's default user, so copying the plugin files in
   needs root.
2. The Kong/nginx master process itself runs as an **unprivileged** user
   (`kong`, uid 100) in the official image. `kong reload` regenerates its
   config file (`.kong_env`) as whoever runs it, and the running master
   re-reads that file on the reload signal, **as itself**. If `kong reload`
   is run as root, `.kong_env` comes out root-owned and unreadable to the
   actual (non-root) master process, and the reload crashes the Lua init
   phase.

`deploy-plugin.sh` handles this by copying files as whatever user invoked
it, then detecting the actual user the nginx master is running as (via
`/proc/<pid>/status`, not `ps -p` -- BusyBox's `ps` doesn't support `-p`)
and running `kong reload` as that user specifically. This makes the same
script correct both in this Alpine/non-root docker setup and on a bare-metal
install where Kong might run as root -- no hardcoded assumption either way.

### Soak test (long-running, randomised load)

`scripts/soak_test.py` + `scripts/soak_report.py` drive hours of continuously
varying concurrency (idle / low / normal / boundary / overload / recovery,
randomised order and duration, with every overload phase deliberately
followed by a recovery phase) against ~25-35s backend requests, and produce
a timestamped, reproducible report -- specifically to catch a leaked/stuck
concurrency counter, which needs sustained, varied, hours-long load to
surface (a single short test run cannot).

```sh
# quick sanity check (a few minutes) before committing to a long run
python3 scripts/soak_test.py --validation --route mock-route --path /mock/anything

# the real thing
python3 scripts/soak_test.py --duration-hours 8 --route mock-route --path /mock/anything
```

Output lands in `soak-test-results/<label>-<UTC timestamp>/`: per-request
CSV, a Kong-side live-counter timeline (sampled independently via the
`api.lua` Admin API route), the exact phase sequence realised, a full log,
and `report.md` -- plus a copy of the script and resolved config, so the run
is reproducible later (this directory is gitignored; each run generates its
own timestamped copy locally rather than being checked in).

An 8-hour run of this suite (`max_concurrency=10`, `burst_concurrency=0`)
completed **PASS**: 21,789 requests, 15,933 expected 429s, **0 unexpected
429s**, 0 errors, 50/50 overload->recovery cycles verified (49 succeeded
outright, 1 inconclusive due to no post-grace traffic in that particular
window -- not a failure), and Kong's own reported counter never exceeded 10
across 14,332 independent samples over the full run.

## Deploying to production (bare metal, no Docker)

The production Kong host has no containers, so the exact same
`scripts/deploy-plugin.sh` script is meant to be copied to and run directly
on that host (it's plain POSIX `sh`, no docker dependency):

```sh
# On the production Kong host, with this repo (or just kong-plugin/ and
# scripts/) copied over:
export PLUGIN_SRC_DIR=/path/to/kong-concurrency-limit/kong-plugin/kong/plugins/concurrency-limit
sudo -E ./scripts/deploy-plugin.sh
```

This performs a **hot, zero-downtime install**: it copies the plugin files
onto Kong's Lua path, enables `concurrency-limit` (added to the existing
`KONG_PLUGINS`/`plugins` list), declares the shared-memory zone the counter
lives in, and issues `kong reload` (graceful SIGHUP, no dropped connections)
as the correct user.

**This is a runtime-only change, and making it durable is not optional once
you've attached the plugin to a route.** Discovered the hard way while
testing this: once a route/service in Kong's database has `concurrency-limit`
attached to it, Kong **refuses to start at all** if it comes up without that
plugin enabled -- not a silent revert to "unprotected", a hard crash:

```
init_by_lua error: .../kong/init.lua:534: error building initial plugins:
concurrency-limit plugin is in use but not enabled
```

Reproduced this directly: attached the plugin to a test route, then did a
plain `docker restart` on the Kong container (which restarts using its
*original* startup environment, not the hot-reloaded one) -- the container
exited immediately and stayed down, because the plugin was already
referenced in the database but not in that startup config's `KONG_PLUGINS`.
There is no way to `docker exec`/SSH in and hot-fix it at that point either,
since Kong never reaches a running state.

So: **before** attaching `concurrency-limit` to any route in production,
add to `/etc/kong/kong.conf` (or wherever this Kong install's config lives,
and however its process is normally started/restarted -- systemd
`EnvironmentFile`, etc.) once verified working via `deploy-plugin.sh`:

```conf
plugins = bundled,concurrency-limit
nginx_http_lua_shared_dict = concurrency_limit_store 10m
```

(If `plugins` is already customized in that file, append
`,concurrency-limit` to the existing value rather than overwriting it.)

If this is ever hit anyway (plugin attached, config not durable, Kong now
refuses to start): either fix the durable config as above and restart again
(now it will come up clean), or, as an emergency fallback to restore
service immediately, remove the plugin's row from Kong's database (e.g.
`DELETE FROM plugins WHERE name = 'concurrency-limit';` against the
Postgres backing store) to let Kong start without it, then re-attach once
the config is durable.

Then attach the plugin to the specific service/route(s) that need it via the
Admin API (or declarative config, if that's how routes are managed in
production) -- see `scripts/setup-test-service.sh` for the equivalent
`curl` calls against `/routes/<route>/plugins`. Start with `config.dry_run =
true` and watch the logs for `(dry-run) would reject` lines to size
`max_concurrency` against real traffic before enforcing it.

**Recommended rollout:** stage this on one Kong node (or the docker test
harness against representative concurrency/latency) first, run in `dry_run`
to observe real numbers, then enable enforcement -- rather than turning it on
cluster-wide in one step, since (per the caveats above) the effective total
capacity is `max_concurrency x number of Kong nodes`.
