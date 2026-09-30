import pg from 'pg'

/**
 * The live view: one Postgres LISTEN per process, fanned out to every open /api/live
 * stream.
 *
 * drain announces each beacon on `drain_beacon` once its insert has committed
 * (internal/writer/pg.go), with a small whitelisted payload. This keeps the trailing hour
 * of those in memory - a few thousand small objects - and answers everything on the page
 * from it. The database sees one read of the last hour when the first viewer arrives,
 * and nothing after that however many tabs are open.
 *
 * Nothing runs while nobody is watching: the listener connects on the first subscriber
 * and disconnects after the last. The dashboard runs a single replica (see cache.ts), so
 * module state is the whole story.
 */

const CHANNEL = 'drain_beacon'
const WINDOW_MS = 60 * 60_000
/** Newest beacons a fresh viewer opens on. */
const FEED_LIMIT = 100
/** How often the counters and chart are re-sent. Individual beacons go out as they land. */
const SUMMARY_MS = 2_000
/** drain flushes every 250ms, so a batch arrives as a burst; send it as one message. */
const COALESCE_MS = 250
const RETRY_MS = 3_000

/** One beacon as the page sees it. The install id is cut to a prefix; no IP ever. */
export interface LiveBeacon {
  key: string
  time: string
  name: string
  install: string
  version: string | null
  mode: string | null
  auth: string | null
  browser: string | null
  os: string | null
  containers: number | null
  activeMinutes: string | null
}

export interface LiveSummary {
  at: string
  /** False while the listener is down; the numbers are then as of the last good moment. */
  connected: boolean
  installsLast5m: number
  installsLastHour: number
  /** Beacons in the trailing hour, by name. */
  lastHour: Record<string, number>
  /** Whole minutes, oldest first: `counts[name][i]` is minute `start + i`. */
  minutes: { start: string; counts: Record<string, number[]> }
  /** Versions seen in the last 15 minutes, by distinct install. */
  versions: { version: string; installs: number }[]
}

export type LiveMessage =
  | { type: 'snapshot'; summary: LiveSummary; beacons: LiveBeacon[] }
  | { type: 'beacons'; beacons: LiveBeacon[] }
  | { type: 'summary'; summary: LiveSummary }

type Listener = (msg: LiveMessage) => void

/** The payload drain sends, and the shape the seed read produces to match it. */
interface Payload {
  time: string | Date
  name: string
  client: string
  version?: string | null
  mode?: string | null
  auth?: string | null
  ua?: string | null
  containers?: number | null
  activeMinutes?: string | null
}

interface Rec extends Payload {
  at: number
  /** Identity for de-duplication. Holds the full install id, so it stays server-side. */
  id: string
  /** What the browser gets instead. */
  key: string
}

const listeners = new Set<Listener>()
let conn: pg.Client | undefined
let ready = false
/** Notifications that land while the seed read is still running. */
let pending: Rec[] | null = null
/** The trailing hour, oldest first. */
let records: Rec[] = []
let ids = new Set<string>()
let outgoing: LiveBeacon[] = []
let flushTimer: ReturnType<typeof setTimeout> | undefined
let summaryTimer: ReturnType<typeof setInterval> | undefined
let retryTimer: ReturnType<typeof setTimeout> | undefined

export function subscribeLive(fn: Listener): () => void {
  listeners.add(fn)
  if (ready) deliver(fn, snapshot())
  if (!conn && !retryTimer) void connect()
  summaryTimer ??= setInterval(() => broadcast({ type: 'summary', summary: summarize() }), SUMMARY_MS)

  return () => {
    listeners.delete(fn)
    if (listeners.size > 0) return
    clearInterval(summaryTimer)
    clearTimeout(retryTimer)
    clearTimeout(flushTimer)
    summaryTimer = retryTimer = flushTimer = undefined
    if (conn) drop(conn)
    records = []
    ids = new Set()
    outgoing = []
  }
}

async function connect() {
  const client = new pg.Client({ connectionString: useRuntimeConfig().databaseUrl })
  conn = client
  // An unhandled 'error' on a pg client takes the whole process down.
  client.on('error', (err) => {
    console.error('live listener error', err)
    drop(client)
  })
  client.on('end', () => drop(client))
  client.on('notification', (n) => {
    if (conn === client && n.channel === CHANNEL && n.payload) receive(n.payload)
  })

  try {
    await client.connect()
    // Listen before reading, and hold what arrives meanwhile, so a beacon committed
    // during the read is either in it or in `pending` - never in neither.
    pending = []
    await client.query(`LISTEN ${CHANNEL}`)
    const rows = await query<Payload>(
      `SELECT time, name, client_id AS client,
              metadata ->> 'version'                 AS version,
              metadata ->> 'mode'                    AS mode,
              metadata ->> 'authProvider'            AS auth,
              left(metadata ->> 'browser', 256)      AS ua,
              drain_int(metadata, 'runningContainers') AS containers,
              metadata ->> 'activeMinutes'           AS "activeMinutes"
         FROM beacon
        -- Whole minutes, so the oldest bar is complete.
        WHERE time >= time_bucket('1 minute', now() - interval '1 hour')
        ORDER BY time`,
    )
    if (conn !== client) return

    records = []
    ids = new Set()
    for (const r of rows) add(toRec(r))
    for (const r of pending) add(r)
    pending = null
    ready = true
    broadcast(snapshot())
  } catch (err) {
    console.error('live listener failed to start', err)
    drop(client)
  }
}

/** Tear down one connection; if anyone is still watching, try again shortly. */
function drop(client: pg.Client) {
  if (conn !== client) return
  conn = undefined
  ready = false
  pending = null
  client.end().catch(() => {})
  if (listeners.size === 0) return
  broadcast({ type: 'summary', summary: summarize() })
  retryTimer = setTimeout(() => {
    retryTimer = undefined
    if (listeners.size > 0 && !conn) void connect()
  }, RETRY_MS)
}

function receive(raw: string) {
  let rec: Rec
  try {
    rec = toRec(JSON.parse(raw) as Payload)
  } catch {
    return
  }
  if (pending) {
    pending.push(rec)
    return
  }
  if (!add(rec)) return
  outgoing.push(toBeacon(rec))
  flushTimer ??= setTimeout(() => {
    flushTimer = undefined
    const beacons = outgoing.reverse()
    outgoing = []
    broadcast({ type: 'beacons', beacons })
  }, COALESCE_MS)
}

function toRec(p: Payload): Rec {
  const at = new Date(p.time).getTime()
  if (!Number.isFinite(at) || !p.name) throw new Error('bad beacon')
  const client = p.client ?? ''
  const id = `${at}|${p.name}|${client}`
  return { ...p, at, client, id, key: hashKey(id) }
}

/** Keeps `records` ordered and duplicate-free, and trims what fell out of the hour. */
function add(rec: Rec): boolean {
  if (ids.has(rec.id)) return false
  ids.add(rec.id)
  records.push(rec)
  // drain stamps time on arrival but writes from one goroutine, so order is only nearly
  // guaranteed. A stray is walked back into place; there is never more than a batch.
  for (let i = records.length - 1; i > 0 && records[i - 1]!.at > records[i]!.at; i--) {
    ;[records[i - 1], records[i]] = [records[i]!, records[i - 1]!]
  }
  const cutoff = minuteFloor(Date.now() - WINDOW_MS)
  let drop = 0
  while (drop < records.length && records[drop]!.at < cutoff) ids.delete(records[drop++]!.id)
  if (drop) records = records.slice(drop)
  return true
}

function snapshot(): LiveMessage {
  const beacons = records.slice(-FEED_LIMIT).reverse().map(toBeacon)
  return { type: 'snapshot', summary: summarize(), beacons }
}

function summarize(): LiveSummary {
  const now = Date.now()
  const start = minuteFloor(now - WINDOW_MS)
  const width = Math.floor((minuteFloor(now) - start) / 60_000) + 1
  const hourAgo = now - WINDOW_MS
  const fiveAgo = now - 5 * 60_000
  const fifteenAgo = now - 15 * 60_000

  const lastHour: Record<string, number> = {}
  const counts: Record<string, number[]> = {}
  const hour = new Set<string>()
  const five = new Set<string>()
  const versionOf = new Map<string, string>()

  for (const r of records) {
    if (r.at >= start) {
      const i = Math.floor((r.at - start) / 60_000)
      if (i < width) (counts[r.name] ??= new Array(width).fill(0))[i]++
    }
    if (r.at <= hourAgo) continue
    lastHour[r.name] = (lastHour[r.name] ?? 0) + 1
    hour.add(r.client)
    if (r.at > fiveAgo) five.add(r.client)
    // Oldest first, so an install that upgraded in the window counts once, as the newer.
    if (r.at > fifteenAgo) versionOf.set(r.client, r.version || 'unknown')
  }

  const perVersion = new Map<string, number>()
  for (const v of versionOf.values()) perVersion.set(v, (perVersion.get(v) ?? 0) + 1)
  const versions = [...perVersion]
    .map(([version, installs]) => ({ version, installs }))
    .sort((a, b) => b.installs - a.installs)
    .slice(0, 8)

  return {
    at: new Date(now).toISOString(),
    connected: ready,
    installsLast5m: five.size,
    installsLastHour: hour.size,
    lastHour,
    minutes: { start: new Date(start).toISOString(), counts },
    versions,
  }
}

function toBeacon(r: Rec): LiveBeacon {
  const ua = r.ua || null
  return {
    key: r.key,
    time: new Date(r.at).toISOString(),
    name: r.name,
    install: r.client.slice(0, 8),
    version: r.version || null,
    mode: r.mode || null,
    auth: r.auth || null,
    browser: ua && browserOf(ua),
    os: ua && osOf(ua),
    containers: r.name === 'events' ? (r.containers ?? null) : null,
    activeMinutes: r.activeMinutes || null,
  }
}

/** Same buckets, same order, as drain_browser in migrations/001. */
function browserOf(ua: string): string {
  if (ua.includes('Edg/') || ua.includes('Edge/')) return 'Edge'
  if (ua.includes('OPR/') || ua.includes('Opera')) return 'Opera'
  if (ua.includes('Firefox/')) return 'Firefox'
  if (ua.includes('Chrome/')) return 'Chrome'
  if (ua.includes('Safari/')) return 'Safari'
  return 'Other'
}

/** Same buckets, same order, as drain_os in migrations/001. */
function osOf(ua: string): string {
  if (ua.includes('Windows')) return 'Windows'
  if (ua.includes('Android')) return 'Android'
  if (/iPhone|iPad|iPod/.test(ua)) return 'iOS'
  if (ua.includes('Mac OS X') || ua.includes('Macintosh')) return 'macOS'
  if (ua.includes('Linux') || ua.includes('X11')) return 'Linux'
  return 'Other'
}

function broadcast(msg: LiveMessage) {
  for (const fn of listeners) deliver(fn, msg)
}

/** One subscriber throwing must not stop the rest, or escape into the pg client. */
function deliver(fn: Listener, msg: LiveMessage) {
  try {
    fn(msg)
  } catch (err) {
    console.error('live subscriber failed', err)
  }
}

function minuteFloor(ms: number): number {
  return Math.floor(ms / 60_000) * 60_000
}

/**
 * A short opaque row key, so the browser never sees the full install id. Two 32-bit
 * FNV-1a lanes: an hour holds ~10k beacons, where one lane would collide about 1% of
 * the time.
 */
function hashKey(s: string): string {
  let a = 2166136261
  let b = 0x811c9dc5 ^ 0x5bd1e995
  for (let i = 0; i < s.length; i++) {
    const c = s.charCodeAt(i)
    a = Math.imul(a ^ c, 16777619)
    b = Math.imul(b ^ c, 16777619) ^ (b >>> 15)
  }
  return (a >>> 0).toString(36) + (b >>> 0).toString(36)
}
