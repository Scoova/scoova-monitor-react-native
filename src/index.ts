/**
 * Scoova Monitor — React Native SDK
 *
 * Usage:
 *   import { ScoovaMonitor } from '@scoova/monitor-react-native';
 *   ScoovaMonitor.init('sm_your_api_key');
 *
 * For event/log/metric persistence across app kills, install the optional
 * peer dependency:
 *   npm install @react-native-async-storage/async-storage
 *
 * If absent, the SDK runs with in-memory queues (same as 1.0.x — events lost
 * on kill).
 */

import { Platform, AppState, Dimensions, NativeModules } from 'react-native'

const SDK_VERSION = '1.4.0'
const HTTP_TIMEOUT_MS = 10_000
const FAILURE_BACKOFF_THRESHOLD = 3
const MAX_QUEUE_PERSISTED = 1000
const DEFAULT_FLUSH_INTERVAL_MS = 300_000 // 5 minutes — radio-friendly default; flush is also triggered by batch size, AppState→background, and crashes (which use a separate immediate path)
const DEFAULT_BATCH_SIZE = 50

// Optional persistence — graceful degradation if AsyncStorage isn't installed.
let AsyncStorage: any = null
try {
  // eslint-disable-next-line @typescript-eslint/no-var-requires
  AsyncStorage = require('@react-native-async-storage/async-storage').default
} catch {
  // Falls back to in-memory queues with no on-disk persistence.
}

// Optional richer device info — uses react-native-device-info if installed,
// otherwise we fall back to a minimal Platform-based collector.
//
// react-native-device-info exports its API as named exports on the module
// (no .default). Some transpiled call-sites do `.default` and get undefined,
// so we reach for both shapes.
let DeviceInfo: any = null
try {
  // eslint-disable-next-line @typescript-eslint/no-var-requires
  DeviceInfo = require('react-native-device-info')
  if (typeof DeviceInfo?.getManufacturerSync !== 'function'
      && typeof DeviceInfo?.default?.getManufacturerSync === 'function') {
    DeviceInfo = DeviceInfo.default
  }
} catch { /* falls back to Platform-based collector */ }

// Optional network info via @react-native-community/netinfo. Same pattern.
let NetInfo: any = null
try {
  // eslint-disable-next-line @typescript-eslint/no-var-requires
  NetInfo = require('@react-native-community/netinfo')
  if (typeof NetInfo?.addEventListener !== 'function'
      && typeof NetInfo?.default?.addEventListener === 'function') {
    NetInfo = NetInfo.default
  }
} catch { /* network type stays unknown */ }

interface Config {
  endpoint?: string
  enableCrashReporting?: boolean
  enableAnalytics?: boolean
  enablePerformance?: boolean
  flushIntervalMs?: number
  maxBatchSize?: number
  /**
   * Probe the bundle for third-party SDK presence (Firebase, Sentry, Mixpanel,
   * etc) and report once-per-install. **Disabled by default** — enable
   * explicitly only if you want the "Detected SDKs" dashboard.
   */
  enableSDKDetection?: boolean
}

interface LogEntry {
  level: string
  tag: string
  message: string
  data?: Record<string, string>
  userId?: string | null
  sessionId?: string
  timestamp: string
}

/**
 * In-memory queue with optional AsyncStorage persistence.
 *
 * All mutating operations run through a single internal promise chain so
 * concurrent push/take/load can't clobber each other. This matters at init
 * time when load() races with the synchronous trackEvent('session_start')
 * that fires on the same tick — without serialization, load() resolving last
 * would overwrite items that push() just added.
 */
class PersistentQueue<T> {
  private items: T[] = []
  private loaded = false
  private chain: Promise<unknown> = Promise.resolve()
  constructor(private storageKey: string, private max: number = MAX_QUEUE_PERSISTED) {}

  /** Serialize an operation onto the chain. Errors are isolated. */
  private serial<R>(op: () => Promise<R> | R): Promise<R> {
    const next = this.chain.then(() => op())
    // Don't propagate failures through the chain — each op handles its own.
    this.chain = next.then(() => undefined, () => undefined)
    return next
  }

  load(): Promise<void> {
    return this.serial(async () => {
      if (this.loaded || !AsyncStorage) { this.loaded = true; return }
      try {
        const raw = await AsyncStorage.getItem(this.storageKey)
        if (raw) {
          const fromDisk: T[] = JSON.parse(raw)
          // Disk items are older — prepend so order is FIFO when items already
          // got pushed before load completed.
          this.items = [...fromDisk, ...this.items]
          while (this.items.length > this.max) this.items.shift()
        }
      } catch { /* corrupt / unavailable */ }
      this.loaded = true
    })
  }

  push(item: T): Promise<void> {
    return this.serial(async () => {
      this.items.push(item)
      while (this.items.length > this.max) this.items.shift()
      await this.persistInternal()
    })
  }

  pushAll(items: T[]): Promise<void> {
    if (!items.length) return Promise.resolve()
    return this.serial(async () => {
      this.items.push(...items)
      while (this.items.length > this.max) this.items.shift()
      await this.persistInternal()
    })
  }

  take(n: number): Promise<T[]> {
    return this.serial(async () => {
      const batch = this.items.splice(0, n)
      await this.persistInternal()
      return batch
    })
  }

  /** Wipe in-memory + persisted contents. Used by clearLocalUserData. */
  clear(): Promise<void> {
    return this.serial(async () => {
      this.items = []
      if (!AsyncStorage) return
      try { await AsyncStorage.removeItem(this.storageKey) } catch { /* */ }
    })
  }

  /** Synchronous read; only meaningful for "do I have anything to flush". */
  get length() { return this.items.length }

  private async persistInternal(): Promise<void> {
    if (!AsyncStorage) return
    try { await AsyncStorage.setItem(this.storageKey, JSON.stringify(this.items)) }
    catch { /* quota / unavailable */ }
  }
}

/**
 * Anonymous installation ID — counts unique users / sessions / retention even
 * when the host app never calls setUserId. Persists across app restarts via
 * AsyncStorage when available; resets only on uninstall.
 *
 * Naming:
 *   - "anon_<uuid>" — anonymous default
 *   - "h_<sha256>" — set when setUserId(realId) is called
 *
 * Two-stage resolution:
 *   1. At SDK module load we generate a fresh UUID *synchronously* and stash
 *      it in `cached`. Every event from init() onwards has a real anon_ value.
 *   2. After init runs, the async resolve() pass reads AsyncStorage. If the
 *      device already had a persisted anon ID from a prior session, that one
 *      replaces the freshly-generated one. The freshly-generated UUID becomes
 *      the persisted ID on first-ever launch (when AsyncStorage was empty).
 *
 * Trade-off: on first-ever launch the events fired before resolve() completes
 * will use the synchronous UUID, which is also what gets persisted, so they
 * stay consistent. On subsequent launches the synchronous UUID is briefly
 * "wrong" (a fresh value, not the persisted one) but it's overwritten before
 * any event leaves the device because flush() runs after bootstrap.
 */
class AnonIdStore {
  private static KEY = 'sm_anon_id'
  private static cached: string = AnonIdStore.generate()

  /** Resolve the persisted anon ID at SDK init. Replaces `cached` if disk has one. */
  static async resolve(): Promise<string> {
    if (!AsyncStorage) return AnonIdStore.cached
    try {
      const persisted = await AsyncStorage.getItem(AnonIdStore.KEY)
      if (persisted && persisted.startsWith('anon_')) {
        AnonIdStore.cached = persisted
      } else {
        // First-ever launch — persist the synchronous UUID so future launches
        // use the same one.
        try { await AsyncStorage.setItem(AnonIdStore.KEY, AnonIdStore.cached) } catch { /* */ }
      }
    } catch { /* AsyncStorage unavailable */ }
    return AnonIdStore.cached
  }

  /** Synchronous read — always returns a valid anon_<uuid>. */
  static get(): string {
    return AnonIdStore.cached
  }

  /**
   * Reset — wipe persisted ID and regenerate a fresh in-memory one. Used by
   * clearLocalUserData(). The next event uses the new value, exactly as if
   * the user had reinstalled the app.
   */
  static async reset(): Promise<void> {
    AnonIdStore.cached = AnonIdStore.generate()
    if (!AsyncStorage) return
    try { await AsyncStorage.removeItem(AnonIdStore.KEY) } catch { /* */ }
  }

  private static generate(): string {
    return 'anon_' + 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, c => {
      const r = (Math.random() * 16) | 0
      return (c === 'x' ? r : (r & 0x3) | 0x8).toString(16)
    })
  }
}

/**
 * Persistent session counter — increments on every cold start. Used to stamp
 * the session_number column on analytics_events so the server can compute
 * session frequency, retention buckets, and "Nth session" funnels.
 *
 * If AsyncStorage is unavailable we still hand out 1 — events get a usable
 * value, just without monotonic growth across launches.
 */
class SessionCounter {
  private static KEY = 'sm_session_number'

  static async incrementAndGet(): Promise<number> {
    if (!AsyncStorage) return 1
    try {
      const raw = await AsyncStorage.getItem(SessionCounter.KEY)
      const n = (parseInt(raw || '0', 10) || 0) + 1
      try { await AsyncStorage.setItem(SessionCounter.KEY, String(n)) } catch { /* */ }
      return n
    } catch {
      return 1
    }
  }

  static async reset(): Promise<void> {
    if (!AsyncStorage) return
    try { await AsyncStorage.removeItem(SessionCounter.KEY) } catch { /* */ }
  }
}

/**
 * Pending crash store — separate from the regular queues because crashes need
 * to survive an immediate process kill. We write the crash to AsyncStorage
 * BEFORE attempting the network POST, so even if the JS engine dies the next
 * launch will pick the report up and replay it. Mirrors the Flutter and
 * native iOS/Android pattern.
 */
class PendingCrashStore {
  private static KEY = 'sm_pending_crashes'
  private static MAX = 10

  static async save(payload: any): Promise<void> {
    if (!AsyncStorage) return
    try {
      const existing = await AsyncStorage.getItem(PendingCrashStore.KEY)
      const arr: any[] = existing ? JSON.parse(existing) : []
      arr.push(payload)
      while (arr.length > PendingCrashStore.MAX) arr.shift()
      await AsyncStorage.setItem(PendingCrashStore.KEY, JSON.stringify(arr))
    } catch { /* unavailable */ }
  }

  static async drain(): Promise<any[]> {
    if (!AsyncStorage) return []
    try {
      const raw = await AsyncStorage.getItem(PendingCrashStore.KEY)
      if (!raw) return []
      await AsyncStorage.removeItem(PendingCrashStore.KEY)
      return JSON.parse(raw)
    } catch { return [] }
  }

  static async restore(unsent: any[]): Promise<void> {
    if (!AsyncStorage || !unsent.length) return
    try {
      const existing = await AsyncStorage.getItem(PendingCrashStore.KEY)
      const arr: any[] = existing ? JSON.parse(existing) : []
      arr.unshift(...unsent)
      while (arr.length > PendingCrashStore.MAX) arr.shift()
      await AsyncStorage.setItem(PendingCrashStore.KEY, JSON.stringify(arr))
    } catch { /* unavailable */ }
  }
}

class ScoovaMonitorSDK {
  private apiKey = ''
  private bundleId = ''
  private endpoint = 'https://monitor.scoo-va.info'
  private initialized = false
  private userId: string | null = null
  private sessionId = ''
  // Monotonic session counter — 1st, 2nd, 3rd launch ever. Primed to 1
  // synchronously during init() so the very first session_start (which fires
  // before bootstrap()'s AsyncStorage read completes) carries a usable value.
  // bootstrap() then replaces it with the persisted count + 1.
  private sessionNumber = 0
  // Last screen name we emitted a screen_view for — used for the
  // previous_screen pointer in user-flow analysis.
  private lastScreen: string | null = null
  // Wall-clock when we entered the last screen, so we can stamp time-on-screen
  // (in seconds) on the next screen_view event for user-flow drop-off analysis.
  private lastScreenAt = 0
  private eventQueue = new PersistentQueue<any>('sm_q_events')
  private logQueue = new PersistentQueue<LogEntry>('sm_q_logs')
  private metricQueue = new PersistentQueue<any>('sm_q_metrics')
  private breadcrumbs: { message: string; category: string; timestamp: string }[] = []
  private flushTimer: ReturnType<typeof setInterval> | null = null
  private appStartTime = Date.now()
  private config: Config = {}
  private consecutiveFailures = 0
  private flushing = false

  init(apiKey: string, config?: Config) {
    if (this.initialized) return
    this.apiKey = apiKey
    this.config = { ...this.config, ...config }
    this.endpoint = config?.endpoint || this.endpoint
    this.bundleId = Platform.select({
      ios: NativeModules.RNDeviceInfo?.bundleId,
      android: NativeModules.RNDeviceInfo?.packageName,
    }) || ''
    this.initialized = true
    this.sessionId = this.uuid()
    // Prime to 1 so synchronous events fired before bootstrap() finishes
    // still carry a non-null sessionNumber. bootstrap() will overwrite this
    // with the actual persisted+incremented value within ~10ms.
    this.sessionNumber = 1

    // Crash handler — installed synchronously so we don't miss errors that
    // fire between init() and bootstrap() completing.
    //
    // Order is important on RN-Android: the default handler can kill the
    // JVM via `mqt_native_modules` thread death when a throw bubbles up to a
    // native thread, so we MUST settle the disk save + POST before calling
    // originalHandler. The whole sequence is awaited inside an async wrapper:
    //
    //   1. PendingCrashStore.save  ─  guarantees the report survives even
    //                                  if the network never delivers
    //   2. POST /v1/ingest/crashes ─  best-effort live delivery
    //   3. remove pending if 200    ─  prevents the next bootstrap from
    //                                  re-sending the same report (dedupe)
    //   4. originalHandler          ─  red box / native crash propagation
    //
    // If POST times out (10s), originalHandler still runs — the report stays
    // on disk and bootstrap() replays it on the next launch.
    const originalHandler = ErrorUtils.getGlobalHandler()
    ErrorUtils.setGlobalHandler((error: Error, isFatal?: boolean) => {
      const payload = this.buildCrashPayload(error, isFatal ?? true)
      ;(async () => {
        try {
          await PendingCrashStore.save(payload)
          const ok = await this.post('/v1/ingest/crashes', payload)
          if (ok) await this.removePendingCrashById(payload._id)
        } catch { /* swallow — crash on disk, will replay on next launch */ }
        originalHandler(error, isFatal)
      })()
    })

    // Unhandled promise rejections
    try {
      // eslint-disable-next-line @typescript-eslint/no-var-requires
      const tracking = require('promise/setimmediate/rejection-tracking')
      tracking.enable({
        allRejections: true,
        onUnhandled: (_: number, error: Error) => { void this.handleCrash(error, false) },
      })
    } catch { /* rejection-tracking not available */ }

    // App state tracking
    AppState.addEventListener('change', (state) => {
      if (state === 'active') {
        this.trackEvent('session_start', { session_id: this.sessionId })
        this.addBreadcrumb('App became active', 'lifecycle')
      } else if (state === 'background') {
        this.trackEvent('session_end', { session_id: this.sessionId })
        void this.flush()
        this.addBreadcrumb('App went to background', 'lifecycle')
      }
    })

    // Subscribe to NetInfo (if installed) so subsequent events carry network type.
    this.subscribeNetInfo()
    // Resolve CPU arch (synchronous on Android via getSupportedAbisSync).
    this.resolveCpuArch()

    // Track startup
    const startupMs = Date.now() - this.appStartTime
    this.trackMetric('app_start', 'cold_start', startupMs, 'ms')

    // Flush timer
    this.flushTimer = setInterval(
      () => void this.flush(),
      config?.flushIntervalMs || DEFAULT_FLUSH_INTERVAL_MS,
    )

    // Continuous frame_rate sampling via requestAnimationFrame. Pure JS,
    // no native module needed. Auto-pauses when the JS thread is
    // suspended (background) — RAF callbacks just stop firing.
    this.startFrameRateSampling()

    // ANR / JS-thread hang detection via timer-drift. We schedule a 1s
    // setInterval and measure how late each tick fires vs expected. If a
    // tick is ≥5s late the JS thread was blocked that long — we report it
    // as a non-fatal ANR row in /v1/ingest/crashes (matches Android +
    // Flutter parity). This is post-hoc — the report goes out after the
    // hang releases — but that's the best signal a pure-JS RN SDK can
    // give without a native module.
    this.startHangDetector()

    // Async bootstrap: load queues from disk, replay any pending crashes from
    // a prior session, then track this session's session_start. Doing this in
    // bootstrap() (rather than racing load() against trackEvent on the same
    // tick) is what fixes the "manual events arrive on local but not on AWS
    // Test Lab / Firebase Test Lab" issue.
    void this.bootstrap()

    console.log(`[ScoovaMonitor] React Native SDK ${SDK_VERSION} initialized`)
  }

  private async bootstrap(): Promise<void> {
    // 1. Resolve / generate the anonymous installation ID so every event has
    //    a non-null user_id even when setUserId hasn't been called.
    await AnonIdStore.resolve()
    this.sessionNumber = await SessionCounter.incrementAndGet()

    // 2. Load all persisted queues from prior session
    await Promise.all([
      this.eventQueue.load(),
      this.logQueue.load(),
      this.metricQueue.load(),
    ])

    // 3. Replay any pending crashes from prior session
    await this.sendPendingCrashes()

    // 4. Track this session's session_start AFTER load is complete — so this
    //    push can never be clobbered by load() resolving second.
    this.trackEvent('session_start', { session_id: this.sessionId })

    // 5. Detect bundled third-party SDKs once per install. Only when the host
    //    explicitly opted in via Config.enableSDKDetection (default false).
    //    Once-per-install is enforced via AsyncStorage so we don't re-emit on
    //    every cold start.
    if (this.config.enableSDKDetection) void this.reportDetectedSDKsOnce()

    // 5.5 Auto-capture install attribution once per install. Falls back to
    //     "organic" / "unknown" when no native attribution package is
    //     present — host can still call setInstallSource manually.
    void this.captureInstallAttributionOnce()

    // 6. Force-flush twice so the session_start lands quickly. Without this,
    //    queued events sit in memory until the next flush timer tick fires
    //    (default 5 min), which is too long for short test scenarios that may
    //    end before then.
    void this.flush()
    setTimeout(() => void this.flush(), 2000)
  }

  /**
   * Detect bundled third-party SDKs by probing module ID names that ship in the
   * RN bundle. We don't try to enumerate everything — just the popular ones the
   * SDK Adoption Intelligence dashboard cares about. Emitted once per install
   * (gated by AsyncStorage flag) since SDK adoption rarely flips during the
   * lifetime of a single user's install.
   */
  private async reportDetectedSDKsOnce(): Promise<void> {
    if (!AsyncStorage) return
    try {
      if (await AsyncStorage.getItem('sm_detected_sdks_sent') === '1') return
    } catch { return }
    const detected: Record<string, string> = {}
    // Metro's static analyzer rejects `require(variable)` in production
    // bundles. Pull `require` through globalThis so the call site looks
    // dynamic to Metro and is left as-is at runtime. The SDK detection
    // is best-effort — an empty result is fine when bundling tools
    // can't resolve at build time.
    const dynamicRequire = (globalThis as any).require ||
      // fallback for older RN versions
      ((typeof __r !== 'undefined') ? __r : null)
    const probe = (name: string, key: string) => {
      if (!dynamicRequire) return
      try { dynamicRequire(name); detected[key] = 'unknown' } catch { /* not bundled */ }
    }
    probe('@react-native-firebase/app',           'firebase')
    probe('@react-native-firebase/analytics',     'firebase-analytics')
    probe('@react-native-firebase/crashlytics',   'firebase-crashlytics')
    probe('@react-native-firebase/messaging',     'firebase-messaging')
    probe('@stripe/stripe-react-native',          'stripe')
    probe('react-native-fbsdk-next',              'facebook-sdk')
    probe('react-native-google-signin',           'google-signin')
    probe('@sentry/react-native',                 'sentry')
    probe('@amplitude/react-native',              'amplitude')
    probe('mixpanel-react-native',                'mixpanel')
    probe('react-native-appsflyer',               'appsflyer')
    probe('react-native-adjust',                  'adjust')
    probe('react-native-onesignal',               'onesignal')
    probe('react-native-branch',                  'branch')
    probe('react-native-segment-analytics',       'segment')
    probe('@react-native-async-storage/async-storage', 'async-storage')
    probe('react-native-device-info',             'device-info')
    probe('@react-native-community/netinfo',      'netinfo')
    probe('react-native-reanimated',              'reanimated')
    probe('react-native-gesture-handler',         'gesture-handler')
    if (Object.keys(detected).length === 0) return
    this.trackEvent('detected_sdks', detected)
    try { await AsyncStorage.setItem('sm_detected_sdks_sent', '1') } catch { /* */ }
  }

  /**
   * Capture install attribution once per install and fire `install_info`.
   *
   * Strategy without taking a hard native dep:
   *   1. If the host app already has react-native-appsflyer, react-native-
   *      adjust, or react-native-branch installed, we can grab an attribution
   *      payload from them via a soft `require` — no SDK-level dep needed.
   *   2. Else if AdServices is reachable on iOS via NativeModules, we ask
   *      for the token and post to Apple. (We don't ship a native module
   *      for this — rely on the host having one of the above attribution
   *      packages, or call setInstallSource manually.)
   *   3. Else fall back to `organic` so at least the install is bucketed.
   *
   * Idempotent via AsyncStorage — runs on first install, not every cold
   * start. Host can override any time with setInstallSource().
   */
  private async captureInstallAttributionOnce(): Promise<void> {
    if (!AsyncStorage) return
    try {
      if (await AsyncStorage.getItem('sm_install_attr_done') === '1') return
    } catch { return }

    // Probe for known attribution packages, in priority order. Each probe
    // is wrapped — if a package is missing or misbehaving we silently fall
    // through to the next.
    let source = 'organic'
    let campaign = ''

    // 1. AppsFlyer — covers iOS + Android paid attribution, most popular
    //    third-party MMP. Returns conversion data via callback; we only
    //    use it if the host already wired up an init somewhere upstream.
    try {
      const appsflyer = require('react-native-appsflyer')?.default || require('react-native-appsflyer')
      const data = await new Promise<any>((resolve) => {
        const t = setTimeout(() => resolve(null), 1500)
        try {
          appsflyer.onInstallConversionData?.((res: any) => { clearTimeout(t); resolve(res) })
        } catch { clearTimeout(t); resolve(null) }
      })
      if (data?.data?.media_source) {
        source = String(data.data.media_source).toLowerCase()
        campaign = String(data.data.campaign || '')
      }
    } catch { /* not installed */ }

    // 2. Branch — same idea, different package. Many ecommerce apps use it.
    if (source === 'organic') {
      try {
        const branch = require('react-native-branch')?.default || require('react-native-branch')
        const params = await branch.getLatestReferringParams?.()
        if (params?.['~channel']) {
          source = String(params['~channel']).toLowerCase()
          campaign = String(params['~campaign'] || '')
        }
      } catch { /* not installed */ }
    }

    this.trackEvent('install_info', {
      install_source: source,
      install_campaign: campaign,
      session_number: String(this.sessionNumber || 1),
    })
    try { await AsyncStorage.setItem('sm_install_attr_done', '1') } catch { /* */ }
  }

  private async sendPendingCrashes(): Promise<void> {
    const pending = await PendingCrashStore.drain()
    if (!pending.length) return
    const failed: any[] = []
    for (const payload of pending) {
      const ok = await this.post('/v1/ingest/crashes', payload).catch(() => false)
      if (!ok) failed.push(payload)
    }
    // Anything that still failed goes back to disk for the next attempt
    if (failed.length) await PendingCrashStore.restore(failed)
  }

  trackEvent(name: string, data?: Record<string, string>) {
    if (!this.initialized) return
    void this.eventQueue.push({
      eventName: name,
      eventData: this.sanitizeData(data),
      userId: this.hashUserId(this.userId),
      sessionId: this.sessionId,
      sessionNumber: this.sessionNumber > 0 ? this.sessionNumber : undefined,
      device: this.getDevice(),
      timestamp: new Date().toISOString(),
    })
    if (this.eventQueue.length >= (this.config.maxBatchSize || DEFAULT_BATCH_SIZE)) {
      void this.flush()
    }
  }

  /**
   * Set the user ID. Side-effect: when an anonymous install identifies
   * for the first time, fire one /v1/ingest/identify so the server
   * merges the anon profile into the real one. Without this, the same
   * human shows up as two rows in user_profiles (anon + real) and gets
   * counted twice in DAU/MAU/cohort retention. Best-effort.
   */
  setUserId(userId: string) {
    const previousUserId = this.userId
    this.userId = userId
    if (!userId) return
    const anon = AnonIdStore.get()
    if (!anon) return
    const hashed = 'h_' + this.sha256(userId)
    if (this.lastIdentifiedAs === hashed) return
    if (previousUserId === userId && this.identifySent) return
    void this.fireIdentify(anon, hashed)
  }

  private identifySent = false
  private lastIdentifiedAs: string | null = null

  private async fireIdentify(anonId: string, hashedUserId: string): Promise<void> {
    try {
      const resp = await fetch(`${this.endpoint}/v1/ingest/identify`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-API-Key': this.apiKey,
          'X-Bundle-Id': this.bundleId,
        },
        body: JSON.stringify({ anonId, userId: hashedUserId }),
      })
      if (resp.ok) {
        this.identifySent = true
        this.lastIdentifiedAs = hashedUserId
      }
    } catch { /* best-effort */ }
  }

  logger(tag: string) {
    return {
      debug: (msg: string, data?: Record<string, string>) => this.log(tag, 'debug', msg, data),
      info: (msg: string, data?: Record<string, string>) => this.log(tag, 'info', msg, data),
      warning: (msg: string, data?: Record<string, string>) => this.log(tag, 'warning', msg, data),
      error: (msg: string, data?: Record<string, string>) => this.log(tag, 'error', msg, data),
    }
  }

  log(tag: string, level: string, message: string, data?: Record<string, string>) {
    if (!this.initialized) return
    void this.logQueue.push({
      level, tag,
      message: this.sanitizeMessage(message),
      data: this.sanitizeData(data),
      userId: this.hashUserId(this.userId),
      sessionId: this.sessionId,
      timestamp: new Date().toISOString(),
    })
    this.addBreadcrumb(`[${level}] [${tag}] ${message}`, 'log')
    if (this.logQueue.length >= (this.config.maxBatchSize || DEFAULT_BATCH_SIZE)) {
      void this.flush()
    }
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  captureError(error: Error, _context?: string) {
    void this.handleCrash(error, false)
  }

  addBreadcrumb(message: string, category = 'custom') {
    if (this.breadcrumbs.length >= 50) this.breadcrumbs.shift()
    this.breadcrumbs.push({ message, category, timestamp: new Date().toISOString() })
  }

  trackScreen(screenName: string) {
    const now = Date.now()
    const data: Record<string, string> = {
      screen_name: screenName,
      previous_screen: this.lastScreen ?? '',
    }
    // Time-on-previous-screen, in seconds — what powers the drop-off
    // funnels in the User Flow Analysis dashboard.
    if (this.lastScreen && this.lastScreenAt > 0) {
      data.duration = ((now - this.lastScreenAt) / 1000).toFixed(2)
    }
    this.trackEvent('screen_view', data)
    this.addBreadcrumb(`Screen: ${screenName}`, 'navigation')
    this.lastScreen = screenName
    this.lastScreenAt = now
  }

  /**
   * Hand-off helper for `<NavigationContainer onStateChange={...}>` from
   * @react-navigation/native. The SDK extracts the deepest active route name
   * and fires a `screen_view` event with the previous-screen pointer set to
   * whatever was active before, so the server can mirror it into the
   * `screen_views` table for user-flow analysis.
   *
   * Usage:
   * ```tsx
   * <NavigationContainer onStateChange={ScoovaMonitor.onNavigationStateChange}>
   * ```
   */
  onNavigationStateChange = (state: any) => {
    if (!state) return
    const name = ScoovaMonitorSDK.deepestRouteName(state)
    if (name && name !== this.lastScreen) this.trackScreen(name)
  }

  /** Walk the nav state's stack down to the leaf route. */
  private static deepestRouteName(state: any): string | null {
    let s = state
    while (s?.routes && typeof s.index === 'number') {
      const r = s.routes[s.index]
      if (!r) return null
      if (r.state) { s = r.state; continue }
      return r.name ?? null
    }
    return null
  }

  /**
   * Flush all pending data. Items that fail to POST stay queued (in-memory and
   * in AsyncStorage if available) for the next attempt. Idempotent if a flush
   * is already in progress.
   */
  async flush(): Promise<void> {
    if (!this.initialized || this.flushing) return
    if (this.consecutiveFailures >= FAILURE_BACKOFF_THRESHOLD) {
      this.consecutiveFailures-- // probe one chance next cycle
      return
    }
    this.flushing = true
    try {
      await Promise.all([
        this.flushOne(this.eventQueue, '/v1/ingest/events/batch', 'events'),
        this.flushOne(this.logQueue, '/v1/ingest/logs/batch', 'logs'),
        this.flushOne(this.metricQueue, '/v1/ingest/metrics/batch', 'metrics'),
      ])
    } finally {
      this.flushing = false
    }
  }

  /**
   * Wipe every piece of telemetry the SDK has buffered or persisted on this
   * device. Call this when the host app's user invokes "delete my account" —
   * pairs with the server-side `DELETE /v1/ingest/me/{userId}` to satisfy
   * GDPR Article 17 / CCPA "right to be forgotten" end-to-end.
   *
   * What this clears:
   *   - the in-memory + AsyncStorage event / metric / log queues
   *   - any pending crash payloads from a prior session
   *   - breadcrumbs accumulated this session
   *   - the anonymous installation ID (a fresh one is generated immediately)
   *   - the persisted session counter
   *   - the once-per-install detected_sdks marker
   *   - the user_id set via setUserId()
   *
   * Does NOT contact the server. The host app should also call your server's
   * GDPR delete endpoint with the user_id you previously sent.
   */
  async clearLocalUserData(): Promise<void> {
    if (!this.initialized) return
    // Best-effort wipe — never throw and block the host's delete-account flow.
    await Promise.all([
      this.eventQueue.clear().catch(() => undefined),
      this.logQueue.clear().catch(() => undefined),
      this.metricQueue.clear().catch(() => undefined),
      AnonIdStore.reset().catch(() => undefined),
      SessionCounter.reset().catch(() => undefined),
    ])
    this.userId = null
    this.breadcrumbs = []
    this.lastScreen = null
    this.lastScreenAt = 0
    if (AsyncStorage) {
      try { await AsyncStorage.removeItem('sm_pending_crashes') } catch { /* */ }
      try { await AsyncStorage.removeItem('sm_detected_sdks_sent') } catch { /* */ }
    }
    console.log('[ScoovaMonitor] Local user data cleared')
  }

  // ─── Private ───

  private async flushOne(q: PersistentQueue<any>, path: string, wrapKey: string): Promise<void> {
    if (q.length === 0) return
    const batch = await q.take(this.config.maxBatchSize || DEFAULT_BATCH_SIZE)
    if (!batch.length) return
    const ok = await this.post(path, { [wrapKey]: batch })
    if (!ok) {
      await q.pushAll(batch)
      this.consecutiveFailures++
    } else {
      this.consecutiveFailures = 0
    }
  }

  private hangDetectorTimer: any = null

  private startHangDetector(): void {
    const TICK_MS = 1000
    const THRESHOLD_MS = 5000
    const COOLDOWN_MS = 30000
    let lastTick = Date.now()
    let nextAllowedReport = 0

    this.hangDetectorTimer = setInterval(() => {
      const now = Date.now()
      const drift = now - lastTick - TICK_MS
      lastTick = now

      if (drift < THRESHOLD_MS) return
      if (now < nextAllowedReport) return

      nextAllowedReport = now + COOLDOWN_MS

      const payload = {
        exceptionType: 'ANR (JS Thread Hang)',
        message: `React Native JS thread blocked for ~${drift}ms (>${THRESHOLD_MS}ms threshold)`,
        stackTrace:
          '(Hang detected via timer-drift after the JS thread unblocked. ' +
          'A pure-JS detector cannot capture the blocking stack — the JS ' +
          'engine was busy executing it. Native logs around this timestamp ' +
          'will show the blocking work.)',
        isFatal: false,
        device: this.getDevice(),
        userId: this.hashUserId(this.userId),
        sessionId: this.sessionId,
        timestamp: new Date(now).toISOString(),
        _id: `anr-${now}-${Math.random().toString(36).slice(2, 8)}`,
      }
      void this.post('/v1/ingest/crashes', payload).catch(() => undefined)
    }, TICK_MS)
  }

  private buildCrashPayload(error: Error, isFatal: boolean): any {
    const breadcrumbStr = this.breadcrumbs.length > 0
      ? '\n\n--- Breadcrumbs ---\n' + this.breadcrumbs.slice(-20).map(b => `[${b.timestamp}] [${b.category}] ${b.message}`).join('\n')
      : ''

    return {
      exceptionType: error.name || 'Error',
      message: this.sanitizeMessage(error.message || 'Unknown error'),
      stackTrace: this.sanitizeStackTrace((error.stack || '') + breadcrumbStr),
      isFatal,
      device: this.getDevice(),
      userId: this.hashUserId(this.userId),
      sessionId: this.sessionId,
      timestamp: new Date().toISOString(),
      // Marker so the bootstrap replay can dedupe if needed
      _id: `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
    }
  }

  /** Remove a single pending crash by its `_id` after a successful POST. */
  private async removePendingCrashById(id: string): Promise<void> {
    const all = await PendingCrashStore.drain().catch(() => [] as any[])
    const remaining = all.filter(p => p?._id !== id)
    if (remaining.length) await PendingCrashStore.restore(remaining)
  }

  /**
   * Build crash payload, save to disk first (so a process kill in the next
   * tick still gets us the report on next launch), then attempt to send.
   * On successful send the disk copy is removed; on failure or timeout it
   * stays for the next bootstrap() to replay.
   *
   * Used for handled (non-fatal) errors only. For fatal crashes the
   * ErrorUtils handler runs the same logic inline so it can sequence the
   * AsyncStorage write before the JVM-killing originalHandler.
   */
  private async handleCrash(error: Error, isFatal: boolean): Promise<void> {
    const payload = this.buildCrashPayload(error, isFatal)
    await PendingCrashStore.save(payload).catch(() => undefined)
    const ok = await this.post('/v1/ingest/crashes', payload).catch(() => false)
    if (ok) await this.removePendingCrashById(payload._id)
  }

  private trackMetric(type: string, name: string, value: number, unit: string) {
    void this.metricQueue.push({
      metricType: type, metricName: name, value, unit,
      sessionId: this.sessionId,
      device: this.getDevice(),
      timestamp: new Date().toISOString(),
      // Tags this metric as React Native so the dashboard can
      // separate it from native iOS/Android baselines (which have
      // very different cold-start / fps profiles).
      framework: 'react-native',
    })
  }

  // ───────── Frame rate sampling ─────────

  private frameRateTimer: ReturnType<typeof setInterval> | null = null
  private rafFrameCount = 0
  private rafLastTs = 0

  private startFrameRateSampling() {
    if (typeof requestAnimationFrame !== 'function') return
    // Schedule a callback for each browser-like animation frame and count.
    // Computing fps every 5s on a setInterval keeps the math out of the
    // RAF callback and avoids spinning when the app is backgrounded.
    const tick = (ts: number) => {
      if (this.rafLastTs === 0) this.rafLastTs = ts
      this.rafFrameCount++
      requestAnimationFrame(tick)
    }
    requestAnimationFrame(tick)
    this.frameRateTimer = setInterval(() => {
      if (this.rafLastTs === 0) return
      const now = Date.now()
      const elapsedMs = now - this.rafLastTs
      if (elapsedMs <= 0) return
      const fps = (this.rafFrameCount * 1000) / elapsedMs
      // 60Hz target on most Android devices, 60-120 on iOS. Don't try
      // to detect ProMotion here — coarse signal is fine.
      const dropped = Math.max(0, Math.min(100, (1 - fps / 60) * 100))
      this.trackMetric('frame_rate', 'fps', Math.min(120, Math.max(0, fps)), 'fps')
      this.trackMetric('frame_rate', 'dropped_frames_percent', dropped, 'percent')
      this.rafFrameCount = 0
      this.rafLastTs = now
    }, 5000)
  }

  /**
   * Wire React Navigation for automatic screen tracking. Call this
   * once with your `NavigationContainer` ref (or the navigation
   * object returned by `useNavigationContainerRef`). Each focused
   * route emits a `screen_view` event. No-op if no nav lib is in use.
   *
   * ```ts
   * const navigationRef = useNavigationContainerRef();
   * <NavigationContainer
   *   ref={navigationRef}
   *   onReady={() => ScoovaMonitor.attachNavigation(navigationRef)}
   * >
   * ```
   */
  attachNavigation(nav: any): void {
    if (!nav || typeof nav.getCurrentRoute !== 'function') return
    let lastName: string | undefined
    const fireScreen = () => {
      try {
        const r = nav.getCurrentRoute?.()
        const name = r?.name
        if (name && name !== lastName) {
          lastName = name
          this.trackEvent('screen_view', { screen_name: String(name) })
        }
      } catch { /* nav not ready / detached */ }
    }
    // React Navigation emits 'state' on every navigation change.
    if (typeof nav.addListener === 'function') {
      nav.addListener('state', fireScreen)
    }
    // Capture the initial route too — addListener fires only on subsequent
    // changes, not for the first render.
    fireScreen()
  }

  // NetInfo state cached so each event can include current network type without
  // an async hop. Updated by an optional listener (only when NetInfo is installed).
  private cachedNetwork: { type?: string; gen?: string; carrier?: string } = {}
  // CPU arch fetched once at init (async DeviceInfo.getSupportedAbis on Android,
  // utsname.machine on iOS). Empty until the promise resolves.
  private cachedCpuArch: string | undefined
  // ram_free + thermal_state are resolved via async DeviceInfo APIs. We cache
  // the latest value and stamp it on subsequent events. The very first event
  // after install may have these unset until the first async resolve lands.
  private cachedRamFree: number | undefined
  private cachedThermalState: string | undefined

  /** Read network info synchronously from the cached state. */
  private subscribeNetInfo() {
    if (!NetInfo) return
    try {
      NetInfo.addEventListener((s: any) => {
        // Bandwidth-based inference for cellular generation when NetInfo
        // can't read TelephonyManager (READ_PHONE_STATE not granted on
        // Android API 30+). Mirrors the native Android SDK fallback.
        let gen = s.details?.cellularGeneration?.toUpperCase() as string | undefined
        if (!gen && s.type === 'cellular') {
          const kbps = s.details?.linkDownstreamBandwidthKbps as number | undefined
          if (typeof kbps === 'number') {
            gen = kbps < 500 ? '2G'
                : kbps < 3_000 ? '3G'
                : kbps < 30_000 ? '4G'
                : '5G'
          }
        }
        this.cachedNetwork = {
          type: s.type === 'cellular' ? 'cellular'
              : s.type === 'wifi' ? 'wifi'
              : s.type === 'ethernet' ? 'ethernet'
              : s.type === 'bluetooth' ? 'bluetooth'
              : s.type === 'vpn' ? 'vpn'
              : s.type === 'none' ? 'none'
              : 'unknown',
          gen,
          carrier: s.details?.carrier || undefined,
        }
      })
    } catch { /* swallow */ }
  }

  /**
   * Resolve CPU architecture at init. DeviceInfo exposes supportedAbis
   * synchronously on Android (e.g. ["arm64-v8a"]) — note the API name is
   * `supportedAbisSync` not `getSupportedAbisSync`. iOS is uniformly arm64
   * on all production hardware.
   */
  private resolveCpuArch(): void {
    if (Platform.OS === 'android' && typeof DeviceInfo?.supportedAbisSync === 'function') {
      try {
        const abis: string[] = DeviceInfo.supportedAbisSync() || []
        if (abis.length > 0) this.cachedCpuArch = abis[0]
      } catch { /* swallow */ }
    } else if (Platform.OS === 'ios') {
      this.cachedCpuArch = 'arm64'
    }
  }

  private getDevice(): Record<string, any> {
    const { width, height } = Dimensions.get('window')

    // OS API level (Android only). Platform.Version is a number on Android, a string on iOS.
    const osApiLevel = Platform.OS === 'android' && typeof Platform.Version === 'number'
      ? Platform.Version
      : undefined

    let timezone: string | undefined
    try { timezone = Intl.DateTimeFormat().resolvedOptions().timeZone } catch { /* */ }

    // Prefer the async-resolved arch from DeviceInfo.getSupportedAbis. Fall
    // back to the Hermes process.arch hint if DeviceInfo isn't installed.
    const cpuArch = this.cachedCpuArch
      ?? ((global as any).HermesInternal && (global as any).process?.arch
            ? String((global as any).process.arch)
            : undefined)

    // Richer device info — only if `react-native-device-info` is installed
    let manufacturer = Platform.OS === 'ios' ? 'Apple' : 'Unknown'
    let model = Platform.OS === 'ios' ? 'iPhone' : 'Android'
    let appVersion: string | undefined
    let buildNumber: string | undefined
    let locale = 'en'
    let country: string | undefined
    let totalRamBytes: number | undefined
    let freeRamBytes: number | undefined
    let freeDiskBytes: number | undefined
    let batteryLevelFrac: number | undefined
    let isCharging: boolean | undefined
    let carrierFromDeviceInfo: string | undefined
    let isEmulatorFlag: boolean | undefined
    let powerThermal: string | undefined
    if (DeviceInfo) {
      try {
        manufacturer = DeviceInfo.getManufacturerSync?.() || manufacturer
        model        = DeviceInfo.getModel?.()           || model
        appVersion   = DeviceInfo.getVersion?.()
        buildNumber  = DeviceInfo.getBuildNumber?.()
        // Memory + disk + battery
        totalRamBytes  = DeviceInfo.getTotalMemorySync?.()
        // ram_free = total − used. getUsedMemory is async-only in older
        // versions, so guard the call and fall back to undefined.
        const usedAsync = DeviceInfo.getUsedMemory?.()
        if (usedAsync && typeof (usedAsync as any).then === 'function') {
          ;(usedAsync as Promise<number>).then(used => {
            if (totalRamBytes != null && used != null) this.cachedRamFree = totalRamBytes - used
          }).catch(() => { /* */ })
        }
        // Use whatever we resolved on a previous tick (or undefined on first call).
        freeRamBytes = this.cachedRamFree
        freeDiskBytes  = DeviceInfo.getFreeDiskStorageSync?.()
        batteryLevelFrac = DeviceInfo.getBatteryLevelSync?.()
        isCharging    = DeviceInfo.isBatteryChargingSync?.()
        // Carrier — DeviceInfo's getCarrierSync works even when NetInfo can't
        // see it (e.g. wifi-only with SIM still present).
        carrierFromDeviceInfo = DeviceInfo.getCarrierSync?.()
        // Emulator flag — useful, but not in our schema as its own column
        isEmulatorFlag = DeviceInfo.isEmulatorSync?.()
        // Thermal state — iOS only via PowerState; Android needs a native
        // module (not yet wired). PowerState resolves async, so we cache.
        const ps = DeviceInfo.getPowerState?.()
        if (ps && typeof (ps as any).then === 'function') {
          ;(ps as Promise<any>).then(p => {
            if (p?.thermalState && typeof p.thermalState === 'string') {
              this.cachedThermalState = p.thermalState
            }
          }).catch(() => { /* */ })
        }
        powerThermal = this.cachedThermalState
      } catch { /* DeviceInfo partial — keep fallbacks */ }
    }
    // Best-effort locale + country — Intl is the most reliable across RN versions.
    try {
      const opts = Intl.DateTimeFormat().resolvedOptions()
      if (opts.locale) locale = opts.locale
      if (locale.includes('-')) country = locale.split('-')[1]?.toUpperCase()
      else if (locale.includes('_')) country = locale.split('_')[1]?.toUpperCase()
    } catch { /* no Intl — keep defaults */ }

    const out: Record<string, any> = {
      manufacturer,
      model,
      osName: Platform.OS === 'ios' ? 'iOS' : 'Android',
      osVersion: String(Platform.Version),
      locale,
      screenResolution: `${Math.round(width)}x${Math.round(height)}`,
      orientation: height >= width ? 'portrait' : 'landscape',
      framework: 'react-native',
      sdkVersion: SDK_VERSION,
    }
    if (osApiLevel !== undefined) out.osApiLevel = osApiLevel
    if (cpuArch) out.cpuArch = cpuArch
    if (timezone) out.timezone = timezone
    if (appVersion) out.appVersion = appVersion
    if (buildNumber) out.buildNumber = buildNumber
    if (country) out.country = country
    if (totalRamBytes !== undefined) out.ramTotal = totalRamBytes
    if (freeRamBytes !== undefined) out.ramFree = freeRamBytes
    if (powerThermal) out.thermalState = powerThermal
    if (freeDiskBytes !== undefined) out.diskFree = freeDiskBytes
    if (batteryLevelFrac !== undefined) out.batteryLevel = batteryLevelFrac
    if (isCharging !== undefined) out.isCharging = isCharging
    if (this.cachedNetwork.type) out.networkType = this.cachedNetwork.type
    if (this.cachedNetwork.gen) out.networkGeneration = this.cachedNetwork.gen
    // Carrier — prefer NetInfo's live value, fall back to DeviceInfo's snapshot
    const carrier = this.cachedNetwork.carrier ?? carrierFromDeviceInfo
    if (carrier) out.carrier = carrier
    // Jailbroken — DeviceInfo's isEmulatorSync returns true on simulators which
    // we map to jailbroken=false (emulators by design have root). For real
    // devices we leave it null since v10.x dropped the sync detection API;
    // a separate jailbreak-detection lib would be needed for that signal.
    if (isEmulatorFlag === false) out.jailbroken = false
    return out
  }

  // ─── Privacy — kept in sync with sdk-android PrivacyGuard.kt ───

  private static readonly RE_EMAIL = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g
  private static readonly RE_PHONE = /^\+?[0-9]{7,15}$/
  private static readonly RE_IP = /\b(?:\d{1,3}\.){3}\d{1,3}\b/g
  private static readonly RE_JWT = /eyJ[a-zA-Z0-9_-]{10,}\.[a-zA-Z0-9_-]{10,}/g
  private static readonly RE_UUID = /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/i
  private static readonly RE_CC = /\b(?:\d{4}[- ]?){3}\d{4}\b/g
  private static readonly PII_KEYS = [
    'email', 'mail', 'phone', 'tel', 'mobile', 'name', 'username',
    'user_name', 'first_name', 'last_name', 'address', 'ssn', 'password',
    'token', 'secret', 'api_key', 'credit_card', 'card_number',
  ]
  // Keys whose substring would otherwise hit PII_KEYS but which are clearly
  // safe (e.g. "screen_name" matches "name" but is just a route label, not
  // user data). Treat as exact-match exemptions before substring scanning.
  private static readonly PII_KEY_ALLOWLIST = [
    'screen_name', 'previous_screen', 'screen', 'route', 'route_name',
    'next_screen', 'event_name', 'tag_name', 'class_name', 'package_name',
    'session_id', 'session_number', 'view_name',
  ]

  /**
   * The user_id we stamp on every event. If the host app called setUserId we
   * send the SHA256-hashed value (h_<hash>); otherwise we fall back to the
   * persisted anonymous installation ID (anon_<uuid>) so DAU/MAU/retention
   * always have something distinct to count.
   */
  private hashUserId(id: string | null): string {
    if (id) return 'h_' + this.sha256(id)
    return AnonIdStore.get()
  }

  private sanitizeMessage(msg: string): string {
    return msg
      .replace(ScoovaMonitorSDK.RE_EMAIL, (m) => `[hashed_email:${this.sha256(m).slice(0, 8)}]`)
      .replace(ScoovaMonitorSDK.RE_CC, '[redacted_card]')
      .replace(ScoovaMonitorSDK.RE_JWT, '[hashed_token]')
  }

  private sanitizeStackTrace(trace: string): string {
    return trace
      .replace(/\/Users\/[^/]+\//g, '/Users/****/')
      .replace(/\/home\/[^/]+\//g, '/home/****/')
      .replace(ScoovaMonitorSDK.RE_EMAIL, (m) => 'h_' + this.sha256(m).slice(0, 12))
      .replace(ScoovaMonitorSDK.RE_IP, (m) => 'h_' + this.sha256(m).slice(0, 8))
      .replace(ScoovaMonitorSDK.RE_JWT, '[hashed_token]')
  }

  private sanitizeData(data?: Record<string, string>): Record<string, string> | undefined {
    if (!data) return undefined
    const result: Record<string, string> = {}
    for (const [k, v] of Object.entries(data)) {
      const kl = k.toLowerCase()
      if (ScoovaMonitorSDK.PII_KEY_ALLOWLIST.includes(kl)) {
        result[k] = v
        continue
      }
      if (ScoovaMonitorSDK.PII_KEYS.some(p => kl.includes(p))) {
        result[k] = 'h_' + this.sha256(v).slice(0, 16)
        continue
      }
      if (this.testReset(ScoovaMonitorSDK.RE_EMAIL, v) ||
          this.testReset(ScoovaMonitorSDK.RE_JWT, v) ||
          ScoovaMonitorSDK.RE_UUID.test(v)) {
        result[k] = 'h_' + this.sha256(v).slice(0, 16)
        continue
      }
      if (this.testReset(ScoovaMonitorSDK.RE_CC, v)) {
        result[k] = '[redacted]'
        continue
      }
      const trimmed = v.trim()
      if (ScoovaMonitorSDK.RE_PHONE.test(trimmed) && trimmed.length >= 7 && trimmed.length <= 16) {
        result[k] = 'h_' + this.sha256(v).slice(0, 16)
        continue
      }
      result[k] = v
    }
    return result
  }

  private testReset(re: RegExp, s: string): boolean {
    re.lastIndex = 0
    return re.test(s)
  }

  /**
   * SHA-256 (FIPS 180-4). User IDs and any detected PII are hashed with
   * this before anything leaves the device — the server only ever
   * receives pseudonymized "h_<sha256>" values, never raw identifiers.
   *
   * Implemented inline and synchronous on purpose: the event-build path
   * needs the digest without an async hop, React Native has no reliable
   * built-in crypto, and the SDK ships native-module-free. Standard
   * algorithm — verified against the FIPS test vectors.
   */
  private sha256(input: string): string {
    // UTF-8 encode
    const bytes: number[] = []
    for (let i = 0; i < input.length; i++) {
      let c = input.charCodeAt(i)
      if (c < 0x80) bytes.push(c)
      else if (c < 0x800) bytes.push(0xc0 | (c >> 6), 0x80 | (c & 0x3f))
      else if (c < 0xd800 || c >= 0xe000) bytes.push(0xe0 | (c >> 12), 0x80 | ((c >> 6) & 0x3f), 0x80 | (c & 0x3f))
      else {
        c = 0x10000 + (((c & 0x3ff) << 10) | (input.charCodeAt(++i) & 0x3ff))
        bytes.push(0xf0 | (c >> 18), 0x80 | ((c >> 12) & 0x3f), 0x80 | ((c >> 6) & 0x3f), 0x80 | (c & 0x3f))
      }
    }
    const K = [
      0x428a2f98,0x71374491,0xb5c0fbcf,0xe9b5dba5,0x3956c25b,0x59f111f1,0x923f82a4,0xab1c5ed5,
      0xd807aa98,0x12835b01,0x243185be,0x550c7dc3,0x72be5d74,0x80deb1fe,0x9bdc06a7,0xc19bf174,
      0xe49b69c1,0xefbe4786,0x0fc19dc6,0x240ca1cc,0x2de92c6f,0x4a7484aa,0x5cb0a9dc,0x76f988da,
      0x983e5152,0xa831c66d,0xb00327c8,0xbf597fc7,0xc6e00bf3,0xd5a79147,0x06ca6351,0x14292967,
      0x27b70a85,0x2e1b2138,0x4d2c6dfc,0x53380d13,0x650a7354,0x766a0abb,0x81c2c92e,0x92722c85,
      0xa2bfe8a1,0xa81a664b,0xc24b8b70,0xc76c51a3,0xd192e819,0xd6990624,0xf40e3585,0x106aa070,
      0x19a4c116,0x1e376c08,0x2748774c,0x34b0bcb5,0x391c0cb3,0x4ed8aa4a,0x5b9cca4f,0x682e6ff3,
      0x748f82ee,0x78a5636f,0x84c87814,0x8cc70208,0x90befffa,0xa4506ceb,0xbef9a3f7,0xc67178f2,
    ]
    let h0=0x6a09e667,h1=0xbb67ae85,h2=0x3c6ef372,h3=0xa54ff53a,
        h4=0x510e527f,h5=0x9b05688c,h6=0x1f83d9ab,h7=0x5be0cd19
    const bitLen = bytes.length * 8
    bytes.push(0x80)
    while (bytes.length % 64 !== 56) bytes.push(0)
    // 64-bit big-endian length. Identifiers are short, so the high word is 0.
    bytes.push(0, 0, 0, 0, (bitLen >>> 24) & 0xff, (bitLen >>> 16) & 0xff, (bitLen >>> 8) & 0xff, bitLen & 0xff)
    const rotr = (n: number, b: number) => (n >>> b) | (n << (32 - b))
    const w = new Array<number>(64)
    for (let off = 0; off < bytes.length; off += 64) {
      for (let i = 0; i < 16; i++)
        w[i] = ((bytes[off+i*4] << 24) | (bytes[off+i*4+1] << 16) | (bytes[off+i*4+2] << 8) | bytes[off+i*4+3]) | 0
      for (let i = 16; i < 64; i++) {
        const s0 = rotr(w[i-15],7) ^ rotr(w[i-15],18) ^ (w[i-15] >>> 3)
        const s1 = rotr(w[i-2],17) ^ rotr(w[i-2],19) ^ (w[i-2] >>> 10)
        w[i] = (w[i-16] + s0 + w[i-7] + s1) | 0
      }
      let a=h0,b=h1,c=h2,d=h3,e=h4,f=h5,g=h6,h=h7
      for (let i = 0; i < 64; i++) {
        const S1 = rotr(e,6) ^ rotr(e,11) ^ rotr(e,25)
        const ch = (e & f) ^ (~e & g)
        const t1 = (h + S1 + ch + K[i] + w[i]) | 0
        const S0 = rotr(a,2) ^ rotr(a,13) ^ rotr(a,22)
        const maj = (a & b) ^ (a & c) ^ (b & c)
        const t2 = (S0 + maj) | 0
        h=g; g=f; f=e; e=(d+t1)|0; d=c; c=b; b=a; a=(t1+t2)|0
      }
      h0=(h0+a)|0; h1=(h1+b)|0; h2=(h2+c)|0; h3=(h3+d)|0
      h4=(h4+e)|0; h5=(h5+f)|0; h6=(h6+g)|0; h7=(h7+h)|0
    }
    const hex = (n: number) => (n >>> 0).toString(16).padStart(8, '0')
    return hex(h0)+hex(h1)+hex(h2)+hex(h3)+hex(h4)+hex(h5)+hex(h6)+hex(h7)
  }

  /**
   * POST with 10s timeout. Returns true on 2xx or 4xx (permanent → drop),
   * false on timeout / network error / 5xx (transient → caller re-queues).
   */
  private async post(path: string, body: any): Promise<boolean> {
    try {
      const ctrl = typeof AbortController !== 'undefined' ? new AbortController() : null
      const t = ctrl ? setTimeout(() => ctrl.abort(), HTTP_TIMEOUT_MS) : null
      const resp = await fetch(`${this.endpoint}${path}`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-API-Key': this.apiKey,
          'X-Bundle-Id': this.bundleId,
        },
        body: JSON.stringify(body),
        signal: ctrl?.signal,
      })
      if (t) clearTimeout(t)
      if (resp.ok) return true
      if (resp.status >= 400 && resp.status < 500) return true
      return false
    } catch {
      return false
    }
  }

  private uuid(): string {
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, c => {
      const r = (Math.random() * 16) | 0
      return (c === 'x' ? r : (r & 0x3) | 0x8).toString(16)
    })
  }
}

export const ScoovaMonitor = new ScoovaMonitorSDK()
export default ScoovaMonitor
