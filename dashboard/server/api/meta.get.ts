/** Freshness of the derived tables, so the UI can say when the numbers last moved. */
export default defineEventHandler(async () => {
  const rows = await query<{ key: string; value: string; duration_ms: number | null }>(
    `SELECT key, value, duration_ms FROM analytics_meta ORDER BY key`,
  )
  const [span] = await query<{ first_day: string | null; last_day: string | null }>(
    `SELECT min(day) AS first_day, max(day) AS last_day FROM active_counts_daily`,
  )
  return { phases: rows, span: span ?? { first_day: null, last_day: null } }
})
