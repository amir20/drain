export default defineEventHandler(async () => {
  const [row] = await query<{ last_refresh: string | null; duration_ms: number | null }>(
    `SELECT value AS last_refresh, duration_ms FROM analytics_meta WHERE key = 'last_refresh'`,
  )
  return { ok: true, lastRefresh: row?.last_refresh ?? null, refreshMs: row?.duration_ms ?? null }
})
