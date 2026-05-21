# Changelog

## 1.5.0 — 2026-05-21

### Added
- **Network instrumentation** — wraps `global.fetch` and
  `XMLHttpRequest.prototype`. Outbound HTTP becomes breadcrumbs
  (method/host/status/duration) plus a `network/request_duration_ms`
  metric. Excludes `scoo-va.info` from instrumentation. On by default;
  disable via `enableNetworkInstrumentation: false`.
- **Continuous battery + memory sampling** — periodic 60s emit of
  `battery/level` and `memory/used_bytes` via DeviceInfo's sync APIs.
  Configurable via `resourceSampleIntervalMs` (0 to disable).
- **Install date** persisted to AsyncStorage on first init. Forwarded as
  `device.installDate` (ms since epoch).
- `trackCustomMetric(name, value, unit)` public API — parity with iOS,
  Android, Flutter, and Web SDKs.

### Changed
- SDK version reported as `1.5.0` in every event payload.

## 1.4.0

Initial public release.
