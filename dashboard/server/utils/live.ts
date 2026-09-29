/**
 * The live view: one poller per process, fanned out to every open /api/live stream.
 *
 * Everything else on the dashboard reads aggregates that refresh hourly. This reads the
 * raw `beacon` hypertable instead, but only its newest hour, which sits in the one
 * uncompressed chunk and is covered by idx_beacon_time_name_client. The poller runs only
 * while someone has the page open, and a tab more costs nothing: every subscriber gets
 * the same tick, so the database sees one set of queries every POLL_MS regardless.
 *
 * The dashboard runs a single replica (see cache.ts), so module state is the whole story.
 */

const POLL_MS = 5_000
/** Newest beacons sent per tick. A feed scrolling faster than this is unreadable anyway. */
const FEED_LIMIT = 100
/**
 * The feed re-reads this far back and drops what it already sent, rather than trusting
 * `time > last`. drain stamps `time` when the request arrives but inserts from a single
 * writer goroutine, so a beacon can commit after a later-stamped one; a strict cursor
 * would skip it for good.
 */
const LOOKBACK = '20 seconds'

/**
 * What a feed row carries. Deliberately a whitelist: the payload also holds the hashed
 * client address, and nothing on this page needs it. The install id is cut to a prefix -
 * enough to see the same install come round again, not a full identifier to copy out.
 */
export interface LiveBeacon {
  key: string
  time: string
  name: string
  install: string
  version: string | null
  mode: string | null
  browser: string | null
  os: string | null
  auth: string | null
  containers: number | null
}

export interface LiveTick {
  at: string
  stats: {
    beaconsLastMinute: number
    installsLast5m: number
    installsLastHour: number
  }
  /** Per-minute beacon counts by name for the trailing hour, oldest first. */
  minutes: { minute: string; name: string; beacons: number }[]
  /** Versions seen in the last 15 minutes, by distinct install. */
  versions: { version: string; installs: number }[]
  /** Beacons not in any earlier tick, newest first. On a fresh subscriber, the latest few. */
  beacons: LiveBeacon[]
}

type Listener = (tick: LiveTick) => void

const listeners = new Set<Listener>()
let timer: ReturnType<typeof setInterval> | undefined
let last: LiveTick | undefined
let seen = new Set<string>()
/** The newest beacons across ticks, newest first, so a late joiner opens on a full feed. */
let recent: LiveBeacon[] = []
let running = false

export function subscribeLive(fn: Listener): () => void {
  listeners.add(fn)
  // A late joiner gets the last tick straight away rather than a blank page for 5s.
  if (last) fn({ ...last, beacons: recent })
  if (!timer) {
    void poll()
    timer = setInterval(() => void poll(), POLL_MS)
  }
  return () => {
    listeners.delete(fn)
    if (listeners.size === 0 && timer) {
      clearInterval(timer)
      timer = undefined
      last = undefined
      seen = new Set()
      recent = []
    }
  }
}

async function poll() {
  // A slow tick must not stack a second one behind it.
  if (running) return
  running = true
  try {
    const tick = await readTick()
    last = tick
    recent = [...tick.beacons, ...recent].slice(0, FEED_LIMIT)
    for (const fn of listeners) fn(tick)
  } catch (err) {
    console.error('live poll failed', err)
  } finally {
    running = false
  }
}

async function readTick(): Promise<LiveTick> {
  const [stats, minutes, versions, rows] = await Promise.all([
    query<LiveTick['stats']>(
      `SELECT count(*) FILTER (WHERE time > now() - interval '1 minute')::int              AS "beaconsLastMinute",
              count(DISTINCT client_id) FILTER (WHERE time > now() - interval '5 minutes')::int AS "installsLast5m",
              count(DISTINCT client_id)::int                                                AS "installsLastHour"
         FROM beacon
        WHERE time > now() - interval '1 hour'`,
    ),
    query<LiveTick['minutes'][number]>(
      `SELECT time_bucket('1 minute', time) AS minute, name, count(*)::int AS beacons
         FROM beacon
        -- Whole minutes, or the oldest bar would shrink a little on every tick.
        WHERE time >= time_bucket('1 minute', now() - interval '1 hour')
        GROUP BY 1, 2 ORDER BY 1`,
    ),
    query<LiveTick['versions'][number]>(
      `SELECT COALESCE(NULLIF(metadata->>'version', ''), 'unknown') AS version,
              count(DISTINCT client_id)::int AS installs
         FROM beacon
        WHERE time > now() - interval '15 minutes'
        GROUP BY 1 ORDER BY 2 DESC LIMIT 8`,
    ),
    query<Omit<LiveBeacon, 'key'> & { client_id: string }>(
      `SELECT time, name, client_id,
              left(client_id, 8)                     AS install,
              NULLIF(metadata->>'version', '')       AS version,
              NULLIF(metadata->>'mode', '')          AS mode,
              drain_browser(metadata)                AS browser,
              drain_os(metadata)                     AS os,
              drain_auth_provider(metadata)          AS auth,
              drain_int(metadata, 'runningContainers') AS containers
         FROM beacon
        WHERE time > now() - $1::interval
        ORDER BY time DESC
        LIMIT $2`,
      [LOOKBACK, FEED_LIMIT],
    ),
  ])

  const fresh: LiveBeacon[] = []
  const window = new Set<string>()
  for (const { client_id, ...r } of rows) {
    const time = new Date(r.time).toISOString()
    // The full id stays server-side; it only has to make the key unique.
    const key = `${time}|${r.name}|${client_id}`
    window.add(key)
    if (!seen.has(key)) fresh.push({ ...r, time, key: hashKey(key) })
  }
  // Only the lookback window can come back, so that is all worth remembering.
  seen = window

  return {
    at: new Date().toISOString(),
    stats: stats[0] ?? { beaconsLastMinute: 0, installsLast5m: 0, installsLastHour: 0 },
    minutes: minutes.map((m) => ({ ...m, minute: new Date(m.minute).toISOString() })),
    versions,
    beacons: fresh,
  }
}

/** A short opaque row key, so the browser never sees the full install id. */
function hashKey(s: string): string {
  let h = 2166136261
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i)
    h = Math.imul(h, 16777619)
  }
  return (h >>> 0).toString(36)
}
