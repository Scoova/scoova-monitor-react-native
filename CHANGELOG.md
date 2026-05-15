# Changelog

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
