"""Analysis + report generation for soak_test.py.

Reads the CSVs a run produced (request-results.csv, phases.csv,
concurrency-timeline.csv) and writes summary.txt + report.md. Kept as a
separate, pure-function module (no side effects besides file writes) so it
can also be re-run standalone against an existing run directory:

    python3 soak_report.py <run_dir>
"""

import csv
import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone

PHASE_OVERLOAD = "OVERLOAD"
PHASE_RECOVERY = "RECOVERY"

# datetime.fromisoformat() doesn't exist before Python 3.7 (this runs under
# 3.6 in the docker/test environment), so timestamps written by soak_test.py
# (isoformat() of a timezone-aware UTC datetime, e.g.
# "2026-09-09T20:29:35.738880+00:00") are parsed manually instead.
_ISO_RE = re.compile(
    r"^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})"
    r"(?:\.(\d+))?([+-]\d{2}:\d{2}|Z)?$"
)


def _parse_ts(s):
    if not s:
        return None
    m = _ISO_RE.match(s)
    if not m:
        raise ValueError("unrecognized timestamp format: %r" % s)
    year, month, day, hour, minute, second, frac, tz = m.groups()
    microsecond = int((frac or "0").ljust(6, "0")[:6])
    dt = datetime(int(year), int(month), int(day), int(hour), int(minute),
                  int(second), microsecond)
    if tz and tz != "Z":
        sign = 1 if tz[0] == "+" else -1
        th, tm = tz[1:].split(":")
        offset_minutes = sign * (int(th) * 60 + int(tm))
        dt = dt.replace(tzinfo=timezone(timedelta(minutes=offset_minutes)))
    else:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _percentile(sorted_values, pct):
    if not sorted_values:
        return None
    if len(sorted_values) == 1:
        return sorted_values[0]
    k = (len(sorted_values) - 1) * (pct / 100.0)
    f = int(k)
    c = min(f + 1, len(sorted_values) - 1)
    if f == c:
        return sorted_values[f]
    d0 = sorted_values[f] * (c - k)
    d1 = sorted_values[c] * (k - f)
    return d0 + d1


def load_rows(run_dir):
    path = os.path.join(run_dir, "request-results.csv")
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    for row in rows:
        row["target_concurrency"] = int(row["target_concurrency"])
        row["configured_max_concurrency"] = int(row["configured_max_concurrency"])
        row["in_flight_at_send"] = int(row["in_flight_at_send"])
        row["phase_id"] = int(row["phase_id"])
        row["response_time_seconds"] = float(row["response_time_seconds"])
        row["http_status"] = int(row["http_status"]) if row["http_status"] else None
        row["_sent_dt"] = _parse_ts(row["timestamp_sent"])
        row["_resp_dt"] = _parse_ts(row["timestamp_response"])
    return rows


def load_phases(run_dir):
    path = os.path.join(run_dir, "phases.csv")
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        phases = list(reader)
    for p in phases:
        p["phase_id"] = int(p["phase_id"])
        p["target_concurrency"] = int(p["target_concurrency"])
        p["duration_seconds"] = float(p["duration_seconds"])
        p["_start_dt"] = _parse_ts(p["start_timestamp"])
    return phases


def load_timeline(run_dir):
    path = os.path.join(run_dir, "concurrency-timeline.csv")
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    kong_values = []
    for r in rows:
        v = r.get("kong_reported_concurrency")
        if v not in (None, "", "None"):
            try:
                kong_values.append(int(float(v)))
            except ValueError:
                pass
    return rows, kong_values


def analyze(run_dir, max_concurrency, max_delay):
    rows = load_rows(run_dir)
    phases = load_phases(run_dir)
    timeline_rows, kong_values = load_timeline(run_dir)

    total = len(rows)
    status_counts = {}
    for row in rows:
        key = row["http_status"] if row["http_status"] is not None else "ERROR"
        status_counts[key] = status_counts.get(key, 0) + 1

    expected_429 = sum(1 for r in rows if r["classification"] == "EXPECTED_429")
    unexpected_429 = [r for r in rows if r["classification"] == "UNEXPECTED_429"]
    errors = [r for r in rows if r["classification"] == "ERROR"]
    other_status = [r for r in rows if r["classification"] == "OTHER_STATUS"]

    ok_response_times = sorted(
        r["response_time_seconds"] for r in rows if r["classification"] == "OK_200"
    )
    avg_rt = sum(ok_response_times) / len(ok_response_times) if ok_response_times else None
    p95_rt = _percentile(ok_response_times, 95)
    p99_rt = _percentile(ok_response_times, 99)

    max_inflight_client = max((r["in_flight_at_send"] for r in rows), default=0)
    max_kong_reported = max(kong_values, default=None)

    # --- missing-expected-429 detection: any phase whose target exceeded
    # the configured max, but which produced zero 429 responses anywhere
    # in it -- i.e. the limiter apparently admitted more than it should
    # have without ever rejecting.
    by_phase = {}
    for r in rows:
        by_phase.setdefault(r["phase_id"], []).append(r)

    missing_expected_429 = []
    for phase in phases:
        pid = phase["phase_id"]
        if phase["target_concurrency"] <= max_concurrency:
            continue
        phase_rows = by_phase.get(pid, [])
        if not phase_rows:
            continue
        saw_429 = any(r["http_status"] == 429 for r in phase_rows)
        peak_inflight = max((r["in_flight_at_send"] for r in phase_rows), default=0)
        if not saw_429 and peak_inflight > max_concurrency:
            missing_expected_429.append({
                "phase_id": pid,
                "target_concurrency": phase["target_concurrency"],
                "start_timestamp": phase["start_timestamp"],
                "peak_inflight_observed": peak_inflight,
            })

    # --- overload -> recovery cycle detection & verification.
    phases_sorted = sorted(phases, key=lambda p: p["phase_id"])
    grace_seconds = max_delay + 10  # let leftover overload requests drain
    recovery_cycles = []
    for i, phase in enumerate(phases_sorted):
        if phase["target_concurrency"] <= max_concurrency:
            continue
        if phase["target_concurrency"] <= max_concurrency:
            continue
        # find the next phase; a RECOVERY phase is expected right after
        if i + 1 >= len(phases_sorted):
            continue
        nxt = phases_sorted[i + 1]
        if nxt["phase_label"] != PHASE_RECOVERY:
            continue

        recovery_start = nxt["_start_dt"]
        recovery_rows = by_phase.get(nxt["phase_id"], [])
        post_grace_rows = [
            r for r in recovery_rows
            if r["_sent_dt"] is not None and recovery_start is not None
            and (r["_sent_dt"] - recovery_start).total_seconds() >= grace_seconds
        ]
        bad = [r for r in post_grace_rows if r["http_status"] == 429]
        # Three-way outcome, not a boolean: a cycle whose recovery phase
        # happened to send zero NEW requests after the grace window (e.g. a
        # short phase, or low target concurrency already satisfied by
        # requests still draining from the overload phase) has produced NO
        # EVIDENCE either way -- that's "inconclusive", not "failed". Only
        # an actual 429 observed after the grace window is a real failure.
        if not post_grace_rows:
            status = "inconclusive"
        elif bad:
            status = "failed"
        else:
            status = "success"
        recovery_cycles.append({
            "overload_phase_id": phase["phase_id"],
            "overload_target": phase["target_concurrency"],
            "recovery_phase_id": nxt["phase_id"],
            "recovery_start": nxt["start_timestamp"],
            "post_grace_requests": len(post_grace_rows),
            "post_grace_429s": len(bad),
            "status": status,
            "bad_examples": [
                {"timestamp_sent": r["timestamp_sent"], "request_id": r["request_id"]}
                for r in bad[:5]
            ],
        })

    successful_cycles = [c for c in recovery_cycles if c["status"] == "success"]
    failed_cycles = [c for c in recovery_cycles if c["status"] == "failed"]
    inconclusive_cycles = [c for c in recovery_cycles if c["status"] == "inconclusive"]

    persistent_429_periods = [
        {
            "recovery_phase_id": c["recovery_phase_id"],
            "recovery_start": c["recovery_start"],
            "post_grace_429s": c["post_grace_429s"],
            "examples": c["bad_examples"],
        }
        for c in failed_cycles
    ]

    stuck_events_path = os.path.join(run_dir, "stuck-events.json")
    stuck_events = []
    if os.path.exists(stuck_events_path):
        with open(stuck_events_path) as f:
            stuck_events = json.load(f)

    limiter_breach = max_kong_reported is not None and max_kong_reported > max_concurrency

    passed = (
        len(unexpected_429) == 0
        and len(missing_expected_429) == 0
        and len(persistent_429_periods) == 0
        and len(failed_cycles) == 0
        and len(errors) == 0
        and not limiter_breach
        and not stuck_events
    )

    return {
        "total": total,
        "status_counts": status_counts,
        "expected_429": expected_429,
        "unexpected_429": unexpected_429,
        "errors": errors,
        "other_status": other_status,
        "avg_response_time": avg_rt,
        "p95_response_time": p95_rt,
        "p99_response_time": p99_rt,
        "max_inflight_client": max_inflight_client,
        "max_kong_reported": max_kong_reported,
        "missing_expected_429": missing_expected_429,
        "recovery_cycles": recovery_cycles,
        "successful_cycles": successful_cycles,
        "failed_cycles": failed_cycles,
        "inconclusive_cycles": inconclusive_cycles,
        "persistent_429_periods": persistent_429_periods,
        "stuck_events": stuck_events,
        "limiter_breach": limiter_breach,
        "passed": passed,
        "phases_run": len(phases),
    }


def _fmt(v, nd=3):
    if v is None:
        return "n/a"
    return "%.*f" % (nd, v)


def generate_report(run_dir, run_config, max_concurrency, burst_concurrency,
                     min_delay, max_delay, started_at_dt, ended_at_dt,
                     stuck_events=None):
    if stuck_events:
        with open(os.path.join(run_dir, "stuck-events.json"), "w") as f:
            json.dump(stuck_events, f, indent=2)

    a = analyze(run_dir, max_concurrency, max_delay)
    duration = ended_at_dt - started_at_dt

    with open(os.path.join(run_dir, "summary.txt"), "w") as f:
        f.write("Concurrency-limit soak test summary\n")
        f.write("====================================\n")
        f.write("start: %s\n" % started_at_dt.isoformat())
        f.write("end:   %s\n" % ended_at_dt.isoformat())
        f.write("duration: %s\n" % duration)
        f.write("configured max_concurrency: %s\n" % max_concurrency)
        f.write("configured burst_concurrency: %s\n" % burst_concurrency)
        f.write("total requests: %s\n" % a["total"])
        f.write("status counts: %s\n" % a["status_counts"])
        f.write("expected 429: %s\n" % a["expected_429"])
        f.write("unexpected 429: %s\n" % len(a["unexpected_429"]))
        f.write("missing expected 429 windows: %s\n" % len(a["missing_expected_429"]))
        f.write("network/other errors: %s\n" % len(a["errors"]))
        f.write("max observed concurrency (client): %s\n" % a["max_inflight_client"])
        f.write("max observed concurrency (Kong-reported): %s\n" % a["max_kong_reported"])
        f.write("avg response time (200s): %s\n" % _fmt(a["avg_response_time"]))
        f.write("p95 response time (200s): %s\n" % _fmt(a["p95_response_time"]))
        f.write("p99 response time (200s): %s\n" % _fmt(a["p99_response_time"]))
        f.write("recovery cycles tested: %s\n" % len(a["recovery_cycles"]))
        f.write("recovery cycles succeeded: %s\n" % len(a["successful_cycles"]))
        f.write("recovery cycles FAILED: %s\n" % len(a["failed_cycles"]))
        f.write("recovery cycles inconclusive (no post-grace traffic sent): %s\n" %
                len(a["inconclusive_cycles"]))
        f.write("stuck-counter events: %s\n" % len(a["stuck_events"]))
        f.write("limiter breach (Kong counter > max_concurrency): %s\n" % a["limiter_breach"])
        f.write("RESULT: %s\n" % ("PASS" if a["passed"] else "FAIL"))

    report_path = os.path.join(run_dir, "report.md")
    with open(report_path, "w") as f:
        w = f.write
        w("# Concurrency-limit soak test report\n\n")

        w("## 1. Test window\n\n")
        w("- Start: `%s`\n" % started_at_dt.isoformat())
        w("- End: `%s`\n" % ended_at_dt.isoformat())
        w("- Duration: `%s`\n\n" % duration)

        w("## 2. Kong / plugin configuration (as actually deployed)\n\n")
        pc = run_config["plugin_config_actual"]
        w("- Route: `%s` (id `%s`)\n" % (run_config["route"], run_config["route_id"]))
        w("- `max_concurrency`: **%s**\n" % max_concurrency)
        w("- `burst_concurrency`: **%s**\n" % burst_concurrency)
        w("- `response_code`: **%s**\n" % pc.get("response_code"))
        w("- `dry_run`: %s\n" % pc.get("dry_run"))
        w("- `default_conn_delay`: %s\n" % pc.get("default_conn_delay"))
        w("- Full config snapshot: see `run-config.json` in this directory.\n\n")

        w("## 3. Backend response-time behaviour\n\n")
        w("Mock upstream (`mock-upstream/server.py`), each request held for a "
          "uniform random delay in **[%s, %s] seconds** before returning "
          "HTTP 200 (`?delay=<seconds>` chosen client-side per request, "
          "equivalent to the backend picking its own random delay).\n\n" % (
              min_delay, max_delay))

        w("## 4. Traffic-generation strategy\n\n")
        w("Continuous, randomised phase scheduler (`soak_test.py`), phases:\n\n")
        w("| Phase | Target concurrency | Typical duration |\n")
        w("|---|---|---|\n")
        w("| IDLE | 0 | 30-90s |\n")
        w("| LOW | 1-5 | 90-240s |\n")
        w("| NORMAL | 6-9 | 90-240s |\n")
        w("| BOUNDARY | exactly %s | 90-200s |\n" % max_concurrency)
        w("| OVERLOAD | %s-%s | 60-180s |\n" % (max_concurrency + 1, max_concurrency + 10))
        w("| RECOVERY | 1-5 | 90-240s |\n\n")
        w("Phase order and durations are randomised at runtime; the one "
          "deliberate rule is that **every OVERLOAD phase is immediately "
          "followed by a RECOVERY phase** (rather than leaving that to "
          "chance), so overload->recovery is exercised repeatedly and "
          "unambiguously throughout the run. %s phase(s) were run in "
          "total. See `phases.csv` for the exact realised sequence.\n\n" % a["phases_run"])

        w("## 5. Total requests\n\n")
        w("**%s** requests sent.\n\n" % a["total"])

        w("## 6. HTTP status breakdown\n\n")
        w("| Status | Count |\n|---|---|\n")
        for k in sorted(a["status_counts"].keys(), key=lambda x: str(x)):
            w("| %s | %s |\n" % (k, a["status_counts"][k]))
        w("\n")

        w("## 7. Expected 429 count\n\n")
        w("**%s** -- 429 responses that occurred while the client-tracked "
          "in-flight count at send time (%s) exceeded the configured "
          "`max_concurrency` (%s). These are correct, intentional "
          "rejections during OVERLOAD phases, not failures.\n\n" % (
              a["expected_429"], "in_flight_at_send", max_concurrency))

        w("## 8. Unexpected 429 count\n\n")
        if a["unexpected_429"]:
            w("**%s -- SEE DETAIL BELOW, NOT HIDDEN IN AGGREGATE STATS.**\n\n" %
              len(a["unexpected_429"]))
            w("| timestamp_sent | request_id | phase | target | in_flight_at_send | max_concurrency |\n")
            w("|---|---|---|---|---|---|\n")
            for r in a["unexpected_429"][:50]:
                w("| %s | %s | %s (id=%s) | %s | %s | %s |\n" % (
                    r["timestamp_sent"], r["request_id"], r["phase_label"],
                    r["phase_id"], r["target_concurrency"], r["in_flight_at_send"],
                    r["configured_max_concurrency"]))
            if len(a["unexpected_429"]) > 50:
                w("\n... and %s more (see request-results.csv, "
                  "classification=UNEXPECTED_429).\n" % (len(a["unexpected_429"]) - 50))
        else:
            w("**0.** No 429 response was ever observed while the client-tracked "
              "in-flight count was at or below the configured `max_concurrency`.\n")
        w("\n")

        w("## 9. Other errors\n\n")
        if a["errors"] or a["other_status"]:
            w("- Network/timeout/exception errors: **%s**\n" % len(a["errors"]))
            w("- Other (non-200/429) HTTP statuses: **%s**\n\n" % len(a["other_status"]))
            for r in (a["errors"] + a["other_status"])[:20]:
                w("  - `%s` request=%s phase=%s status=%s error=%s\n" % (
                    r["timestamp_sent"], r["request_id"], r["phase_label"],
                    r["http_status"], r["error_detail"]))
        else:
            w("None.\n")
        w("\n")

        w("## 10. Maximum observed / requested concurrency\n\n")
        w("- Client-requested (in-flight at send time, our own bookkeeping): **%s**\n" %
          a["max_inflight_client"])
        w("- Kong-reported (`current_concurrency` via the Admin API, sampled "
          "every %ss, independent of the client): **%s**\n\n" % (
              run_config["poll_interval"], a["max_kong_reported"]))
        if a["limiter_breach"]:
            w("**WARNING: Kong's own reported counter exceeded the configured "
              "`max_concurrency` (%s) at least once.** With `burst_concurrency` "
              "as configured, this indicates the limiter admitted more "
              "concurrent requests than intended -- see `concurrency-timeline.csv` "
              "for exact samples.\n\n" % max_concurrency)
        else:
            w("Kong's own reported counter never exceeded the configured "
              "`max_concurrency` at any sampled point.\n\n")

        w("## 11. Response-time statistics (successful 200s only)\n\n")
        w("- Average: **%ss**\n" % _fmt(a["avg_response_time"]))
        w("- p95: **%ss**\n" % _fmt(a["p95_response_time"]))
        w("- p99: **%ss**\n\n" % _fmt(a["p99_response_time"]))

        w("## 12. Overload -> recovery cycles tested\n\n")
        w("**%s** cycles (every OVERLOAD phase, paired with the RECOVERY "
          "phase that immediately followed it).\n\n" % len(a["recovery_cycles"]))

        w("## 13. Recovery cycle results\n\n")
        w("- Succeeded (no 429 once %ss+ past the start of the recovery "
          "phase -- draining time for leftover overload requests): **%s**\n" % (
              max_delay + 10, len(a["successful_cycles"])))
        w("- **FAILED (a 429 observed after the grace window): %s**\n" % len(a["failed_cycles"]))
        w("- Inconclusive (recovery phase happened to send no *new* request "
          "after the grace window, so there's no data either way -- not "
          "counted as a failure, but also not evidence of success): **%s**\n\n" %
          len(a["inconclusive_cycles"]))
        if a["failed_cycles"]:
            w("| overload phase | recovery phase | recovery start | 429s after grace |\n")
            w("|---|---|---|---|\n")
            for c in a["failed_cycles"]:
                w("| %s (target=%s) | %s | %s | %s |\n" % (
                    c["overload_phase_id"], c["overload_target"],
                    c["recovery_phase_id"], c["recovery_start"],
                    c["post_grace_429s"]))
            w("\n")

        w("## 14. Persistent 429 after concurrency dropped below limit\n\n")
        if a["persistent_429_periods"]:
            w("**Found -- see below. This is the specific symptom the request "
              "asked to highlight as a possible stuck/leaked counter.**\n\n")
            for p in a["persistent_429_periods"]:
                w("- Recovery phase id=%s (started `%s`): %s unexpected 429(s) "
                  "after the drain grace period. Examples: %s\n" % (
                      p["recovery_phase_id"], p["recovery_start"],
                      p["post_grace_429s"], p["examples"]))
        else:
            w("None observed. Every recovery phase's post-grace traffic was "
              "429-free.\n")
        w("\n")

        w("## 15. Indication of a leaked/stuck concurrency counter\n\n")
        if a["stuck_events"] or a["missing_expected_429"] or a["limiter_breach"]:
            if a["stuck_events"]:
                w("- **Stuck-counter heuristic fired %s time(s)** (Kong reported "
                  "current_concurrency > 0 for 15s+ while the client believed "
                  "0 requests were in flight). Details:\n\n" % len(a["stuck_events"]))
                for e in a["stuck_events"][:20]:
                    w("  - `%s`: kong_reported=%s sustained=%.0fs phase=%s target=%s\n" % (
                        e["timestamp"], e["kong_reported"], e["sustained_seconds"],
                        e["phase_label"], e["target_concurrency"]))
            if a["missing_expected_429"]:
                w("\n- **%s overload window(s) produced zero 429s despite "
                  "client-observed concurrency exceeding max_concurrency** "
                  "(the limiter may have silently admitted more than "
                  "configured):\n\n" % len(a["missing_expected_429"]))
                for m in a["missing_expected_429"]:
                    w("  - phase id=%s start=%s target=%s peak_inflight_observed=%s\n" % (
                        m["phase_id"], m["start_timestamp"],
                        m["target_concurrency"], m["peak_inflight_observed"]))
            if a["limiter_breach"]:
                w("\n- Kong's own counter exceeded `max_concurrency` at least "
                  "once (see section 10).\n")
        else:
            w("No evidence found: no stuck-counter heuristic firings, no "
              "overload window failed to produce any 429, and Kong's own "
              "reported counter never exceeded the configured limit.\n")
        w("\n")

        w("## 16. Final result\n\n")
        w("# %s\n\n" % ("PASS" if a["passed"] else "FAIL"))
        w("PASS requires all of:\n\n")
        checks = [
            ("Expected 429 responses occurred during intentional overload",
             a["expected_429"] > 0),
            ("No unexpected 429 responses during below-limit periods",
             len(a["unexpected_429"]) == 0),
            ("No persistent 429 remained after overload traffic subsided",
             len(a["persistent_429_periods"]) == 0),
            ("All recovery cycles returned to normal",
             len(a["failed_cycles"]) == 0),
            ("No unexpected 5xx/network failures",
             len(a["errors"]) == 0),
            ("No evidence of a leaked/stuck concurrency counter",
             not a["stuck_events"] and not a["missing_expected_429"] and not a["limiter_breach"]),
        ]
        for desc, ok in checks:
            w("- [%s] %s\n" % ("x" if ok else " ", desc))
        w("\n")

        w("## Reproducing this run\n\n")
        w("`soak_test.py` and `run-config.json` in this directory are the "
          "exact script and resolved configuration used. Raw per-request "
          "data: `request-results.csv`; Kong-side live counter samples: "
          "`concurrency-timeline.csv`; exact phase sequence: `phases.csv`; "
          "full log: `test.log`.\n")

    return a


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("usage: soak_report.py <run_dir>", file=sys.stderr)
        sys.exit(1)
    run_dir = sys.argv[1]
    with open(os.path.join(run_dir, "run-config.json")) as f:
        run_config = json.load(f)
    pc = run_config["plugin_config_actual"]
    generate_report(
        run_dir=run_dir,
        run_config=run_config,
        max_concurrency=int(pc["max_concurrency"]),
        burst_concurrency=pc["burst_concurrency"],
        min_delay=run_config["min_delay"],
        max_delay=run_config["max_delay"],
        started_at_dt=_parse_ts(run_config["started_at_utc"]),
        ended_at_dt=datetime.now(timezone.utc),
    )
    print("Report regenerated in %s" % run_dir)
