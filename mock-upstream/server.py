#!/usr/bin/env python3
"""Minimal mock of a slow, resource-heavy backend endpoint.

Stands in for a real backend during plugin testing: it deliberately blocks
for `delay` seconds per request to simulate heavy processing work, and
handles requests concurrently (ThreadingHTTPServer) so that Kong's
concurrency-limit plugin -- not this mock -- is what throttles things.
"""
import json
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

inflight = 0
inflight_peak = 0


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        print("[mock-upstream] %s - %s" % (self.address_string(), fmt % args))

    def do_GET(self):
        global inflight, inflight_peak

        parsed = urlparse(self.path)
        if parsed.path == "/health":
            self._respond(200, {"status": "ok"})
            return

        params = parse_qs(parsed.query)
        delay = float(params.get("delay", ["2"])[0])

        inflight += 1
        inflight_peak = max(inflight_peak, inflight)
        started = datetime.now(timezone.utc).isoformat()
        try:
            time.sleep(delay)
            self._respond(200, {
                "status": "ok",
                "path": parsed.path,
                "delay_seconds": delay,
                "started_at": started,
                "inflight_at_response": inflight,
                "inflight_peak_seen": inflight_peak,
            })
        finally:
            inflight -= 1

    def _respond(self, code, payload):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


if __name__ == "__main__":
    addr = ("0.0.0.0", 9000)
    httpd = ThreadingHTTPServer(addr, Handler)
    print("[mock-upstream] listening on %s:%s" % addr)
    httpd.serve_forever()
