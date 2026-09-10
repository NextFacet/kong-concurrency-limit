#!/usr/bin/env python3
"""Concurrency-limit soak test.

Drives long-lived (~25-35s) requests through Kong at a randomised,
constantly-changing target concurrency (idle / low / normal / boundary /
overload / recovery), against the concurrency-limit plugin as already
configured and deployed -- this script never touches the plugin itself.

For every request it records: timestamp, request id, traffic phase,
configured limit, client-observed in-flight count at send time, HTTP
status (or error), response time, and whether a 429 was expected given
that in-flight count. A background poller separately samples the plugin's
OWN live counter (via the Admin API added in api.lua) on a fixed interval,
independent of client-side bookkeeping, specifically to catch a stuck/leaked
counter (Kong reporting > 0, or > configured max, while the client believes
traffic is well below the limit).

Usage:
    python3 soak_test.py --duration-hours 8 --route mock-route \
        --path /mock/anything

    # short validation run
    python3 soak_test.py --duration-hours 0.05 --route mock-route \
        --path /mock/anything

Output: soak-test-results/concurrency-soak-<UTC timestamp>/
    request-results.csv   one row per request
    concurrency-timeline.csv   periodic Kong-side live counter samples
    test.log               heartbeat + event log
    summary.txt            machine-ish summary (also embedded in report.md)
    report.md              human-readable final report
    soak_test.py           copy of this script, for reproducibility
    run-config.json        resolved configuration used for this run
"""

import argparse
import csv
import json
import os
import random
import shutil
import signal
import statistics
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import requests

# ---------------------------------------------------------------------------
# Phase definitions. RECOVERY is semantically identical to LOW (1-5) but is
# used specifically for the phase immediately following an OVERLOAD phase,
# so overload->recovery cycles can be identified and verified unambiguously
# during analysis, without depending on always guessing right from labels
# alone.
# ---------------------------------------------------------------------------
PHASE_IDLE = "IDLE"
PHASE_LOW = "LOW"
PHASE_NORMAL = "NORMAL"
PHASE_BOUNDARY = "BOUNDARY"
PHASE_OVERLOAD = "OVERLOAD"
PHASE_RECOVERY = "RECOVERY"

stop_event = threading.Event()


def utc_now():
    return datetime.now(timezone.utc)


def iso(dt):
    return dt.isoformat()


class Counters(object):
    """Thread-safe running totals, kept alongside the CSV (which remains
    the source of truth for the final report -- these are only for the
    live heartbeat)."""

    def __init__(self):
        self.lock = threading.Lock()
        self.total = 0
        self.status_200 = 0
        self.status_429 = 0
        self.status_other = 0
        self.errors = 0
        self.expected_429 = 0
        self.unexpected_429 = 0
        self.max_inflight_client = 0

    def record(self, status, classification, in_flight):
        with self.lock:
            self.total += 1
            if status == 200:
                self.status_200 += 1
            elif status == 429:
                self.status_429 += 1
            elif status is None:
                self.errors += 1
            else:
                self.status_other += 1
            if classification == "EXPECTED_429":
                self.expected_429 += 1
            elif classification == "UNEXPECTED_429":
                self.unexpected_429 += 1
            if in_flight > self.max_inflight_client:
                self.max_inflight_client = in_flight

    def snapshot(self):
        with self.lock:
            return dict(
                total=self.total,
                status_200=self.status_200,
                status_429=self.status_429,
                status_other=self.status_other,
                errors=self.errors,
                expected_429=self.expected_429,
                unexpected_429=self.unexpected_429,
                max_inflight_client=self.max_inflight_client,
            )


class InFlight(object):
    """Client-side "how many requests have I sent that haven't completed
    yet" counter -- this is the "approximate active/concurrent request
    count when sent" the request asked for. Deliberately independent of
    anything Kong reports, so it can be cross-checked against Kong's own
    counter (see ConcurrencyPoller) rather than assuming they agree."""

    def __init__(self):
        self.lock = threading.Lock()
        self.count = 0

    def try_acquire(self, target):
        with self.lock:
            if self.count < target:
                self.count += 1
                return self.count
            return None

    def release(self):
        with self.lock:
            self.count -= 1
            return self.count

    def current(self):
        with self.lock:
            return self.count


class ResultWriter(object):
    def __init__(self, path):
        self.lock = threading.Lock()
        self.f = open(path, "w", newline="")
        self.w = csv.writer(self.f)
        self.w.writerow([
            "timestamp_sent", "timestamp_response", "request_id",
            "phase_id", "phase_label", "target_concurrency",
            "configured_max_concurrency", "configured_burst_concurrency",
            "in_flight_at_send", "http_status", "response_time_seconds",
            "classification", "error_detail",
        ])
        self.f.flush()

    def write(self, row):
        with self.lock:
            self.w.writerow(row)
            self.f.flush()

    def close(self):
        with self.lock:
            self.f.close()


class TimelineWriter(object):
    def __init__(self, path):
        self.lock = threading.Lock()
        self.f = open(path, "w", newline="")
        self.w = csv.writer(self.f)
        self.w.writerow([
            "timestamp", "phase_id", "phase_label", "target_concurrency",
            "client_inflight", "kong_reported_concurrency",
            "kong_rejected_total", "poll_error",
        ])
        self.f.flush()

    def write(self, row):
        with self.lock:
            self.w.writerow(row)
            self.f.flush()

    def close(self):
        with self.lock:
            self.f.close()


class PhaseWriter(object):
    """One row per phase actually run, with its precise start timestamp --
    used during analysis to define the "grace window" at the start of each
    RECOVERY phase (letting leftover overload-phase requests drain) rather
    than approximating phase boundaries from the request rows themselves."""

    def __init__(self, path):
        self.lock = threading.Lock()
        self.f = open(path, "w", newline="")
        self.w = csv.writer(self.f)
        self.w.writerow([
            "phase_id", "phase_label", "target_concurrency", "duration_seconds",
            "start_timestamp",
        ])
        self.f.flush()

    def write(self, row):
        with self.lock:
            self.w.writerow(row)
            self.f.flush()

    def close(self):
        with self.lock:
            self.f.close()


class Logger(object):
    def __init__(self, path):
        self.lock = threading.Lock()
        self.f = open(path, "w")

    def log(self, msg):
        line = "[%s] %s" % (iso(utc_now()), msg)
        with self.lock:
            self.f.write(line + "\n")
            self.f.flush()
        print(line)

    def close(self):
        with self.lock:
            self.f.close()


class ConcurrencyPoller(threading.Thread):
    """Independently samples Kong's OWN live counter (api.lua) on a fixed
    interval -- the authoritative cross-check for a stuck/leaked counter,
    since it doesn't rely on the client's own bookkeeping being correct."""

    def __init__(self, admin_url, entity_id, interval, timeline, logger,
                 phase_state, inflight, max_concurrency):
        threading.Thread.__init__(self, daemon=True)
        self.admin_url = admin_url
        self.entity_id = entity_id
        self.interval = interval
        self.timeline = timeline
        self.logger = logger
        self.phase_state = phase_state
        self.inflight = inflight
        self.max_concurrency = max_concurrency
        self.stuck_suspected_since = None
        self.stuck_events = []

    def run(self):
        url = "%s/concurrency-limit/%s" % (self.admin_url, self.entity_id)
        while not stop_event.is_set():
            ts = utc_now()
            kong_conc = None
            rejected_total = None
            poll_err = ""
            try:
                r = requests.get(url, timeout=5)
                if r.status_code == 200:
                    data = r.json()
                    kong_conc = data.get("current_concurrency")
                    rejected_total = data.get("rejected_total")
                else:
                    poll_err = "admin_http_%s" % r.status_code
            except Exception as exc:  # noqa: BLE001
                poll_err = "%s: %s" % (type(exc).__name__, exc)

            phase_id, phase_label, target = self.phase_state.get()
            client_inflight = self.inflight.current()
            self.timeline.write([
                iso(ts), phase_id, phase_label, target, client_inflight,
                kong_conc, rejected_total, poll_err,
            ])

            # Stuck-counter heuristic: Kong reports a nonzero counter while
            # the client believes NOTHING is in flight, sustained (not a
            # single sample -- that could just be in-flight requests we
            # don't yet know completed, or ordinary poll/log-phase timing
            # jitter of a second or so).
            if kong_conc is not None and client_inflight == 0 and kong_conc > 0:
                if self.stuck_suspected_since is None:
                    self.stuck_suspected_since = ts
                elapsed = (ts - self.stuck_suspected_since).total_seconds()
                if elapsed >= 15:
                    self.logger.log(
                        "STUCK COUNTER SUSPECTED: Kong reports "
                        "current_concurrency=%s while client in-flight=0 "
                        "for %.0fs (phase=%s target=%s)" % (
                            kong_conc, elapsed, phase_label, target))
                    self.stuck_events.append({
                        "timestamp": iso(ts),
                        "kong_reported": kong_conc,
                        "sustained_seconds": elapsed,
                        "phase_label": phase_label,
                        "target_concurrency": target,
                    })
            else:
                self.stuck_suspected_since = None

            # Separate, harder check: Kong's own counter exceeding the
            # configured max at all (burst_concurrency=0 makes this an
            # outright limiter failure, not just a leak).
            if kong_conc is not None and kong_conc > self.max_concurrency:
                self.logger.log(
                    "LIMITER BREACH SUSPECTED: Kong reports "
                    "current_concurrency=%s > configured max_concurrency=%s "
                    "(phase=%s)" % (kong_conc, self.max_concurrency, phase_label))

            time.sleep(self.interval)


class PhaseState(object):
    def __init__(self):
        self.lock = threading.Lock()
        self.phase_id = 0
        self.phase_label = "STARTUP"
        self.target = 0

    def set(self, phase_id, phase_label, target):
        with self.lock:
            self.phase_id = phase_id
            self.phase_label = phase_label
            self.target = target

    def get(self):
        with self.lock:
            return self.phase_id, self.phase_label, self.target


def choose_phase(max_concurrency, previous_label):
    """Returns (label, target_concurrency, duration_seconds).

    Durations are randomised within ranges wide enough to comfortably
    contain several ~30s request waves (so overlap is real, not
    coincidental), except IDLE which is intentionally short (30-90s) per
    the request.
    """
    if previous_label == PHASE_OVERLOAD:
        # Always follow an overload with an explicit, distinctly-labelled
        # recovery period at low concurrency -- this is what turns "some
        # random phases happen to alternate" into "every overload is
        # deliberately followed by a verified recovery", as asked for.
        target = random.randint(1, 5)
        duration = random.randint(90, 240)
        return PHASE_RECOVERY, target, duration

    weights = [
        (PHASE_IDLE, 0.08),
        (PHASE_LOW, 0.22),
        (PHASE_NORMAL, 0.22),
        (PHASE_BOUNDARY, 0.16),
        (PHASE_OVERLOAD, 0.32),
    ]
    r = random.random()
    acc = 0.0
    label = PHASE_LOW
    for lbl, w in weights:
        acc += w
        if r <= acc:
            label = lbl
            break

    if label == PHASE_IDLE:
        return label, 0, random.randint(30, 90)
    if label == PHASE_LOW:
        return label, random.randint(1, 5), random.randint(90, 240)
    if label == PHASE_NORMAL:
        return label, random.randint(6, 9), random.randint(90, 240)
    if label == PHASE_BOUNDARY:
        return label, max_concurrency, random.randint(90, 200)
    if label == PHASE_OVERLOAD:
        return label, random.randint(max_concurrency + 1, max_concurrency + 10), \
            random.randint(60, 180)
    raise AssertionError(label)


def send_one_request(session, base_url, path, min_delay, max_delay,
                      phase_id, phase_label, target, max_concurrency,
                      burst_concurrency, in_flight_at_send, writer,
                      counters, timeout):
    request_id = str(uuid.uuid4())
    delay = round(random.uniform(min_delay, max_delay), 2)
    url = "%s%s" % (base_url, path)
    sent_ts = utc_now()
    status = None
    error_detail = ""
    t0 = time.time()
    try:
        resp = session.get(url, params={"delay": delay}, timeout=timeout)
        status = resp.status_code
    except Exception as exc:  # noqa: BLE001
        error_detail = "%s: %s" % (type(exc).__name__, exc)
    elapsed = time.time() - t0
    resp_ts = utc_now()

    if status == 429:
        classification = (
            "EXPECTED_429" if in_flight_at_send > max_concurrency
            else "UNEXPECTED_429"
        )
    elif status == 200:
        classification = "OK_200"
    elif status is None:
        classification = "ERROR"
    else:
        classification = "OTHER_STATUS"

    writer.write([
        iso(sent_ts), iso(resp_ts), request_id, phase_id, phase_label,
        target, max_concurrency, burst_concurrency, in_flight_at_send,
        status if status is not None else "", round(elapsed, 3),
        classification, error_detail,
    ])
    counters.record(status, classification, in_flight_at_send)

    if classification == "UNEXPECTED_429":
        # Surfaced immediately, not just buried in the CSV -- exactly what
        # was asked for ("do not hide them in aggregate statistics").
        print("[%s] UNEXPECTED 429: request=%s phase=%s(id=%s) target=%s "
              "in_flight_at_send=%s max_concurrency=%s" % (
                  iso(resp_ts), request_id, phase_label, phase_id, target,
                  in_flight_at_send, max_concurrency))


def run_phase(executor, session, base_url, path, min_delay, max_delay,
              phase_id, phase_label, target, duration, max_concurrency,
              burst_concurrency, inflight, writer, counters, logger,
              phase_state, phase_writer, timeout, poll_interval=0.3):
    phase_state.set(phase_id, phase_label, target)
    phase_writer.write([phase_id, phase_label, target, duration, iso(utc_now())])
    logger.log("PHASE START id=%s label=%s target_concurrency=%s "
               "duration=%ss" % (phase_id, phase_label, target, duration))
    end_at = time.time() + duration
    futures = []
    while time.time() < end_at and not stop_event.is_set():
        n = inflight.try_acquire(target)
        if n is not None:
            fut = executor.submit(
                _run_and_release, session, base_url, path, min_delay,
                max_delay, phase_id, phase_label, target, max_concurrency,
                burst_concurrency, n, writer, counters, inflight, timeout,
            )
            futures.append(fut)
        time.sleep(poll_interval)
    return futures


def _run_and_release(session, base_url, path, min_delay, max_delay,
                      phase_id, phase_label, target, max_concurrency,
                      burst_concurrency, in_flight_at_send, writer,
                      counters, inflight, timeout):
    try:
        send_one_request(
            session, base_url, path, min_delay, max_delay, phase_id,
            phase_label, target, max_concurrency, burst_concurrency,
            in_flight_at_send, writer, counters, timeout,
        )
    finally:
        inflight.release()


def fetch_plugin_config(admin_url, route_name):
    r = requests.get("%s/routes/%s" % (admin_url, route_name), timeout=10)
    r.raise_for_status()
    route_id = r.json()["id"]

    r = requests.get("%s/routes/%s/plugins" % (admin_url, route_name), timeout=10)
    r.raise_for_status()
    plugins = [p for p in r.json()["data"] if p["name"] == "concurrency-limit"]
    if not plugins:
        raise RuntimeError(
            "route '%s' has no concurrency-limit plugin attached" % route_name)
    conf = plugins[0]["config"]
    return route_id, conf


def heartbeat_loop(counters, phase_state, logger, started_at, end_at, interval=30):
    while not stop_event.is_set():
        now = time.time()
        remaining = max(0, end_at - now)
        snap = counters.snapshot()
        phase_id, phase_label, target = phase_state.get()
        logger.log(
            "HEARTBEAT elapsed=%.0fs remaining=%.0fs phase=%s(id=%s) "
            "target=%s total=%s 200=%s 429=%s(exp=%s,unexp=%s) other=%s "
            "err=%s max_inflight_client=%s" % (
                now - started_at, remaining, phase_label, phase_id, target,
                snap["total"], snap["status_200"], snap["status_429"],
                snap["expected_429"], snap["unexpected_429"],
                snap["status_other"], snap["errors"],
                snap["max_inflight_client"],
            )
        )
        if stop_event.wait(interval):
            break


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--duration-hours", type=float, default=8.0)
    ap.add_argument("--kong-proxy", default="http://localhost:8000")
    ap.add_argument("--kong-admin", default="http://localhost:8001")
    ap.add_argument("--route", default="mock-route")
    ap.add_argument("--path", default="/mock/anything")
    ap.add_argument("--min-delay", type=float, default=25.0)
    ap.add_argument("--max-delay", type=float, default=35.0)
    ap.add_argument("--request-timeout", type=float, default=60.0)
    ap.add_argument("--poll-interval", type=float, default=2.0,
                     help="Kong live-counter poll interval, seconds")
    ap.add_argument("--output-dir", default="soak-test-results")
    ap.add_argument("--label", default="concurrency-soak")
    ap.add_argument(
        "--validation", action="store_true",
        help="Run a short, fixed LOW->OVERLOAD->RECOVERY->IDLE->LOW sequence "
             "(a few minutes, using the real --min-delay/--max-delay) instead "
             "of the randomised scheduler, to sanity-check the three required "
             "behaviours before committing to the full run: below limit -> no "
             "429, above limit -> expected 429, back below limit -> 429 "
             "disappears again.")
    args = ap.parse_args()

    started_at_dt = utc_now()
    ts_str = started_at_dt.strftime("%Y%m%d-%H%M%S")
    run_dir = os.path.join(args.output_dir, "%s-%s" % (args.label, ts_str))
    os.makedirs(run_dir, exist_ok=True)

    logger = Logger(os.path.join(run_dir, "test.log"))
    logger.log("Soak test starting. Output dir: %s" % run_dir)

    try:
        route_id, plugin_conf = fetch_plugin_config(args.kong_admin, args.route)
    except Exception as exc:  # noqa: BLE001
        logger.log("FATAL: could not fetch plugin config: %s" % exc)
        raise

    max_concurrency = int(plugin_conf["max_concurrency"])
    burst_concurrency = plugin_conf["burst_concurrency"]
    response_code = plugin_conf["response_code"]
    logger.log(
        "Using ACTUAL configured plugin values: max_concurrency=%s "
        "burst_concurrency=%s response_code=%s dry_run=%s (route_id=%s)" % (
            max_concurrency, burst_concurrency, response_code,
            plugin_conf.get("dry_run"), route_id))

    run_config = {
        "started_at_utc": iso(started_at_dt),
        "duration_hours_requested": args.duration_hours,
        "kong_proxy": args.kong_proxy,
        "kong_admin": args.kong_admin,
        "route": args.route,
        "route_id": route_id,
        "path": args.path,
        "min_delay": args.min_delay,
        "max_delay": args.max_delay,
        "request_timeout": args.request_timeout,
        "poll_interval": args.poll_interval,
        "plugin_config_actual": plugin_conf,
    }
    with open(os.path.join(run_dir, "run-config.json"), "w") as f:
        json.dump(run_config, f, indent=2)

    try:
        shutil.copy(os.path.abspath(__file__), os.path.join(run_dir, "soak_test.py"))
    except Exception as exc:  # noqa: BLE001
        logger.log("WARNING: could not copy script into run dir: %s" % exc)

    writer = ResultWriter(os.path.join(run_dir, "request-results.csv"))
    timeline = TimelineWriter(os.path.join(run_dir, "concurrency-timeline.csv"))
    phase_writer = PhaseWriter(os.path.join(run_dir, "phases.csv"))
    counters = Counters()
    inflight = InFlight()
    phase_state = PhaseState()

    def handle_signal(signum, frame):
        logger.log("Received signal %s, stopping gracefully..." % signum)
        stop_event.set()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    poller = ConcurrencyPoller(
        args.kong_admin, route_id, args.poll_interval, timeline, logger,
        phase_state, inflight, max_concurrency,
    )
    poller.start()

    started_at = time.time()
    end_at = started_at + args.duration_hours * 3600.0

    hb_thread = threading.Thread(
        target=heartbeat_loop,
        args=(counters, phase_state, logger, started_at, end_at),
        daemon=True,
    )
    hb_thread.start()

    session = requests.Session()
    executor = ThreadPoolExecutor(max_workers=max(64, max_concurrency * 4))

    all_futures = []
    previous_label = None
    phase_id = 0

    if args.validation:
        # Fixed, deliberately short sequence -- long enough (durations >
        # max_delay) that requests sent early in each phase actually
        # complete and show up in the results before the phase ends, using
        # the SAME real delay range as the full run (not artificially
        # shortened), so this genuinely validates the behaviour the full
        # run relies on rather than a different, easier condition.
        drain = args.max_delay + 15
        # The RECOVERY phase needs enough runway past the analysis grace
        # window (max_delay + 10) for at least one *new* request to
        # actually get sent after it, or the recovery-cycle check for this
        # short run comes back "inconclusive" (no data) rather than a
        # clean "success" -- not wrong, but noisier for a quick check.
        recovery_duration = args.max_delay + 10 + 30
        sequence = [
            (PHASE_LOW, min(2, max_concurrency), drain),
            (PHASE_OVERLOAD, max_concurrency + 5, drain),
            (PHASE_RECOVERY, min(3, max_concurrency), recovery_duration),
            (PHASE_IDLE, 0, 20),
            (PHASE_LOW, min(2, max_concurrency), drain),
        ]
        logger.log("VALIDATION MODE: running fixed sequence %s" % sequence)
        try:
            for label, target, duration in sequence:
                if stop_event.is_set():
                    break
                phase_id += 1
                futs = run_phase(
                    executor, session, args.kong_proxy, args.path,
                    args.min_delay, args.max_delay, phase_id, label, target,
                    duration, max_concurrency, burst_concurrency, inflight,
                    writer, counters, logger, phase_state, phase_writer,
                    args.request_timeout,
                )
                all_futures.extend(futs)
        except KeyboardInterrupt:
            logger.log("KeyboardInterrupt, stopping...")
            stop_event.set()
    else:
        try:
            while time.time() < end_at and not stop_event.is_set():
                remaining = end_at - time.time()
                if remaining <= 5:
                    break
                label, target, duration = choose_phase(max_concurrency, previous_label)
                duration = min(duration, max(5, int(remaining)))
                phase_id += 1
                futs = run_phase(
                    executor, session, args.kong_proxy, args.path,
                    args.min_delay, args.max_delay, phase_id, label, target,
                    duration, max_concurrency, burst_concurrency, inflight,
                    writer, counters, logger, phase_state, phase_writer,
                    args.request_timeout,
                )
                all_futures.extend(futs)
                previous_label = label
        except KeyboardInterrupt:
            logger.log("KeyboardInterrupt, stopping...")
            stop_event.set()

    logger.log("Traffic generation finished (or time/stop reached). "
               "Waiting for in-flight requests to complete...")
    phase_state.set(phase_id, "DRAINING", 0)
    for fut in all_futures:
        try:
            fut.result(timeout=args.max_delay + args.request_timeout + 10)
        except Exception as exc:  # noqa: BLE001
            logger.log("Error waiting for in-flight request: %s" % exc)

    stop_event.set()
    poller.join(timeout=args.poll_interval + 5)

    writer.close()
    timeline.close()
    phase_writer.close()

    logger.log("Soak test finished. Generating report...")

    try:
        from soak_report import generate_report  # local import, see below
    except ImportError:
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        from soak_report import generate_report

    generate_report(
        run_dir=run_dir,
        run_config=run_config,
        max_concurrency=max_concurrency,
        burst_concurrency=burst_concurrency,
        min_delay=args.min_delay,
        max_delay=args.max_delay,
        started_at_dt=started_at_dt,
        ended_at_dt=utc_now(),
        stuck_events=poller.stuck_events,
    )

    logger.log("Report written to %s" % run_dir)
    logger.close()


if __name__ == "__main__":
    main()
