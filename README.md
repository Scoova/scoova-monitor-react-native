# Scoova Monitor — React Native SDK

Crash reporting, analytics, performance, and battery monitoring for React
Native 0.72+. Pure TypeScript, no native module to link.

## Install

```bash
npm install @scoova/monitor-react-native
# or
yarn add @scoova/monitor-react-native
```

### Recommended peer dependencies

These are **optional** — the SDK degrades gracefully without them:

```bash
npm install @react-native-async-storage/async-storage   # disk-backed queues
npm install react-native-device-info                    # richer device fields
npm install @react-native-community/netinfo             # accurate network type
```

Without `AsyncStorage`, queues are in-memory only (events lost on app kill).
With `AsyncStorage`, events survive crashes and cold starts.

## Usage

Initialize as early as possible — typically the first thing in your
`App.tsx` or `index.js`:

```typescript
import { ScoovaMonitor } from '@scoova/monitor-react-native'

ScoovaMonitor.init('sm_your_api_key')

export default function App() { /* … */ }
```

## Configuration

```typescript
ScoovaMonitor.init('sm_your_api_key', {
  endpoint: 'https://monitor.scoo-va.info',   // self-hosted? change this
  flushIntervalMs: 300_000,                    // 5 minutes
  maxBatchSize: 50,
  enableSDKDetection: false,                   // opt-in; default false
})
```

### Privacy opt-in

`enableSDKDetection` is **off by default**. When enabled, on first launch
only the SDK probes for known React Native libraries in the bundle
(`@react-native-firebase/*`, `@sentry/react-native`, `mixpanel-react-native`,
`@stripe/stripe-react-native`, etc.) and emits a single `detected_sdks`
event.

See [the SDK documentation](../docs/)
for the full collection inventory.

## API

### Identify the user

```typescript
ScoovaMonitor.setUserId('user_123')
```

The user ID is hashed before it leaves the device. Without `setUserId`
the SDK falls back to an anonymous installation ID.

### Track events

```typescript
ScoovaMonitor.trackEvent('checkout_started', {
  plan: 'annual',
  amount: '29.99',
})
```

### Track screens

```typescript
ScoovaMonitor.trackScreen('ProductDetail')
```

Or hand off React Navigation's state directly:

```typescript
import { NavigationContainer } from '@react-navigation/native'
import { ScoovaMonitor } from '@scoova/monitor-react-native'

<NavigationContainer onStateChange={ScoovaMonitor.onNavigationStateChange}>
  {/* … */}
</NavigationContainer>
```

### Capture an error

```typescript
try {
  await riskyWork()
} catch (e) {
  ScoovaMonitor.captureError(e as Error)
}
```

Uncaught errors and unhandled promise rejections are captured automatically.

### Breadcrumbs

```typescript
ScoovaMonitor.addBreadcrumb('Started photo upload', 'media')
```

### Tagged loggers

```typescript
const log = ScoovaMonitor.logger('payment')
log.info('Started checkout', { amount: '29.99' })
log.error('Card declined', { code: 'card_declined' })
```

### Right-to-erasure (GDPR / CCPA)

```typescript
await ScoovaMonitor.clearLocalUserData()
```

Wipes queued events, the pending crash store, breadcrumbs, the anonymous
installation ID, the session counter, and the once-per-install
detected-SDKs marker. Pair with a server-side
`DELETE /v1/ingest/me/{userId}`.

### Manual flush

```typescript
await ScoovaMonitor.flush()
```

The SDK auto-flushes every 5 minutes, on `AppState` → background, and when
the batch threshold is hit. Manual flush is rarely needed.

## Symbolication

React Native release builds produce two kinds of debug symbols:

- **Hermes / Metro source maps** for the JS bundle — de-obfuscates JS
  stack traces.
- **Native debug symbols** — the Android `mapping.txt` (ProGuard / R8) and
  the iOS `.dSYM`s — for crashes below the JS layer.

Upload them with the standalone Node scripts that ship in the SDK
repositories — `scoova-upload-sourcemaps.js` in the Web SDK repo,
`scoova-upload-mapping.js` in the Android SDK repo, and
`scoova-upload-dsyms.js` in the iOS SDK repo. Each runs with Node:

```bash
node scoova-upload-sourcemaps.js \
    --api-key sm_your_api_key \
    --version 1.0.0 \
    --build 42 \
    --dir ./android/app/build/generated/sourcemaps/react/release
```

For the full release-build wiring see
[the SDK documentation](../docs/).

## License

[Apache 2.0](../LICENSE).
