# Changelog

All notable changes to this project are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versioning follows
[Semantic Versioning](https://semver.org/).

## [1.0.0] - 2026-09-10

Initial public release.

### Added
- `concurrency-limit` Kong plugin: caps in-flight requests per
  service/route, built on `resty.limit.conn`. Configurable
  `max_concurrency`, `burst_concurrency`, `default_conn_delay`,
  `response_code` (default `429`), `response_message`, `dry_run`,
  `rejection_log_interval`.
- `api.lua` Admin API routes for inspecting and manually resetting a
  route's live concurrency counter.
- Docker Compose test harness that installs the plugin onto a plain,
  unmodified `kong:2.5.1-alpine` image at runtime.
- `scripts/test-lifecycle.sh`: automated normal/abnormal lifecycle test
  suite (16 assertions).
- `scripts/soak_test.py` + `scripts/soak_report.py`: long-running soak
  test tool with randomised traffic phases and a reproducible report.
  An 8-hour run passed cleanly (0 unexpected 429s, 0 stuck-counter
  evidence, 50/50 overload->recovery cycles verified).
