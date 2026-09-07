/**
 * How hard installs are used, as opposed to how many there are.
 *
 * `active_days` is materialised on the weekly snapshot, so intensity and depth are a
 * group-by over a column instead of a count-distinct over daily rows.
 */
export default defineEventHandler(async (event) => {
  const range = resolveRange(event)
  const wk = weeklyWindow(range)

  const [intensity, depth, clients] = await Promise.all([
    // Average distinct days per week an install reports in. 7 means it runs
    // continuously; near 1 means it is opened for a task and closed.
    query<{ week: string; avg_days: number; installs: number }>(
      `SELECT week, round(avg(active_days), 2) AS avg_days, count(*)::int AS installs
         FROM client_snapshot_weekly
        WHERE week BETWEEN $1 AND $2 AND week < date_trunc('week', CURRENT_DATE)::date
        GROUP BY 1 ORDER BY 1`,
      [wk.from, range.to],
    ),

    query<{ week: string; band: number; installs: number }>(
      `SELECT week,
              CASE WHEN active_days = 1  THEN 0
                   WHEN active_days <= 3 THEN 1
                   WHEN active_days <= 6 THEN 2
                   ELSE 3 END AS band,
              count(*)::int AS installs
         FROM client_snapshot_weekly
        WHERE week BETWEEN $1 AND $2 AND week < date_trunc('week', CURRENT_DATE)::date
        GROUP BY 1, 2 ORDER BY 1, 2`,
      [wk.from, range.to],
    ),

    // Concurrent browser sessions per install, as its weekly peak. p95 shows the
    // multi-user installs, the average shows the typical one.
    query<{ week: string; avg: number; p95: number }>(
      `SELECT week,
              round(avg(peak_clients), 2) AS avg,
              round(percentile_cont(0.95) WITHIN GROUP (ORDER BY peak_clients)::numeric, 2) AS p95
         FROM client_snapshot_weekly
        WHERE week BETWEEN $1 AND $2 AND week < date_trunc('week', CURRENT_DATE)::date
        GROUP BY 1 ORDER BY 1`,
      [wk.from, range.to],
    ),
  ])

  return { range, weekly: wk, intensity, depth, clients }
})
