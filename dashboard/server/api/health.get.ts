/**
 * Public - see middleware/require-auth.ts - so anyone can call it. Held briefly so that
 * calling it in a loop is not a database query per request; a probe does not need
 * freshness finer than this.
 */
export default defineCachedEventHandler(
  async () => {
    const [row] = await query<{ last_refresh: string | null; duration_ms: number | null }>(
      `SELECT value AS last_refresh, duration_ms FROM analytics_meta WHERE key = 'last_refresh'`,
    )
    return { ok: true, lastRefresh: row?.last_refresh ?? null, refreshMs: row?.duration_ms ?? null }
  },
  { maxAge: 10, name: 'health', getKey: () => 'health' },
)
