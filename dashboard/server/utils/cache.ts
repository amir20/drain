import type { H3Event } from 'h3'

/**
 * Response caching for the analytics endpoints.
 *
 * Every table these endpoints read has exactly one writer, `drain_refresh_analytics()`,
 * which runs hourly (migrations/003 registers it on TimescaleDB's scheduler). Between
 * two runs the answers are not merely similar, they are identical - so the cache is keyed
 * on when that procedure last finished rather than on a guessed TTL. A refresh lands and
 * every entry becomes unreachable at once, which is exactly the moment someone reloads
 * to see the new numbers.
 *
 * Safe to share between users: `require-auth.ts` is server middleware, so it runs before
 * any route handler, cached or not, and none of these responses contain anything specific
 * to the person who asked. The dashboard also runs a single replica, so an in-memory
 * store needs no coherency between instances; the cost is a cold cache after a deploy.
 */

/** `analytics_meta.last_refresh` as epoch milliseconds - a key-safe, monotonic stamp. */
const refreshStamp = defineCachedFunction(
  async (): Promise<string> => {
    const [row] = await query<{ value: string | null }>(
      `SELECT value FROM analytics_meta WHERE key = 'last_refresh'`,
    )
    const at = row?.value ? Date.parse(row.value) : Number.NaN
    return Number.isFinite(at) ? String(at) : 'none'
  },
  {
    // One indexed single-row read, held briefly so a burst of page loads does not repeat
    // it. A refresh takes ~60s, so a minute of lag on noticing one is not worth a query
    // per request.
    maxAge: 60,
    name: 'analytics',
    getKey: () => 'refresh-stamp',
  },
)

/**
 * Wraps an analytics endpoint in the shared cache.
 *
 * The key is built from the range parameters explicitly rather than from the raw query
 * string, so `?range=30d` and `?range=30d&foo=1` cannot become two entries and, more
 * importantly, a reordered query string cannot become a second copy of the same answer.
 */
export function cachedAnalytics<T>(handler: (event: H3Event) => Promise<T>) {
  return defineCachedEventHandler(handler, {
    // A backstop, not the mechanism: the stamp is what invalidates. This only bounds how
    // long a preset range - whose `to` is derived from the current date - could survive
    // if the refresh job ever stopped running.
    maxAge: 3600,
    name: 'analytics',
    swr: true,
    getKey: async (event) => {
      const q = getQuery(event)
      const path = (event.path.split('?')[0] ?? '').replace(/^\/api\//, '')
      const parts = [path, String(q.range ?? ''), String(q.from ?? ''), String(q.to ?? ''), await refreshStamp()]
      // Storage keys are paths; keep them to characters that cannot introduce a level.
      return parts.join('_').replace(/[^A-Za-z0-9_-]/g, '')
    },
  })
}
