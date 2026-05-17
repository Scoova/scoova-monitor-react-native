# Changelog

## 1.4.1

- Install attribution: the SDK no longer reports a fabricated
  `install_source` of `"organic"` when no MMP (AppsFlyer / Branch) is
  installed. An unmeasured install now reports nothing and buckets as
  "direct" in the dashboard.
- Added `setInstallSource(source, campaign?)` — a manual hook to report
  attribution from your own wiring (parity with the iOS/Android SDKs).

## 1.4.0

Initial public release of the Scoova Monitor React Native SDK.

- Crash reporting — uncaught errors and unhandled promise rejections
- Analytics events and screen tracking (React Navigation aware)
- Performance metrics — cold start and frame-rate sampling
- Battery monitoring (with `react-native-device-info`)
- Structured logging with tagged loggers
- Privacy: user IDs are SHA-256 hashed on-device before sending;
  no device location is collected
- GDPR / CCPA `clearLocalUserData()` helper
- Pure TypeScript — no native module to link
