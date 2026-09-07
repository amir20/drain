/**
 * Cohort retention.
 *
 * The whole grid is precomputed into `cohort_retention_weekly` (a few thousand rows),
 * so the table, the average curve and the W1/W4/W12 trend all come out of one small
 * scan instead of re-deriving every install's activation date per panel.
 *
 * Cohorts are seeded from an install's first `events` beacon, never its first `start`.
 * `start` comes from roughly four times as many installs, most of which launch once and
 * die; mixing the two inflates every denominator and fakes a retention cliff.
 */
const MAX_WEEK_INDEX = 12

export default defineEventHandler(async (event) => {
  const range = resolveRange(event)
  const wk = weeklyWindow(range)

  const [grid, curve, tenure] = await Promise.all([
    query<{ cohort_week: string; cohort_size: number; week_index: number; actives: number }>(
      `SELECT cohort_week, cohort_size, week_index, actives
         FROM cohort_retention_weekly
        WHERE cohort_week BETWEEN $1 AND $2 AND week_index <= $3
        ORDER BY cohort_week DESC, week_index`,
      [wk.from, range.to, MAX_WEEK_INDEX],
    ),

    query<{ week_index: number; pct: number; cohorts: number }>(
      `SELECT week_index,
              round(avg(100.0 * actives / cohort_size), 1) AS pct,
              count(*)::int AS cohorts
         FROM cohort_retention_weekly
        WHERE cohort_week BETWEEN $1 AND $2
          AND week_index <= $3
          -- Only count a cohort at week N once it has actually lived N weeks, or young
          -- cohorts drag the tail of the curve to zero.
          AND cohort_week <= CURRENT_DATE - ((week_index + 1) * 7)
        GROUP BY 1 ORDER BY 1`,
      [wk.from, range.to, MAX_WEEK_INDEX],
    ),

    // The active base split by how long ago each install first reported. A base that is
    // mostly under a month old is churning; a thickening 1-year band is real retention.
    // tenure_band is materialised on the snapshot, so this is a group-by with no join.
    query<{ week: string; band: number; installs: number }>(
      `SELECT week, tenure_band AS band, count(*)::int AS installs
         FROM client_snapshot_weekly
        WHERE week BETWEEN $1 AND $2
          AND week < date_trunc('week', CURRENT_DATE)::date
        GROUP BY 1, 2 ORDER BY 1, 2`,
      [wk.from, range.to],
    ),
  ])

  // Pivot the grid into one row per cohort for the table, keeping the raw counts so the
  // UI can show "312 of 1,204" rather than a bare percentage.
  const byCohort = new Map<
    string,
    { cohort: string; size: number; weeks: (number | null)[] }
  >()
  for (const r of grid) {
    let row = byCohort.get(r.cohort_week)
    if (!row) {
      row = {
        cohort: r.cohort_week,
        size: r.cohort_size,
        weeks: Array.from({ length: MAX_WEEK_INDEX + 1 }, () => null),
      }
      byCohort.set(r.cohort_week, row)
    }
    row.weeks[r.week_index] = r.cohort_size ? (100 * r.actives) / r.cohort_size : null
  }

  return {
    range,
    weekly: wk,
    maxWeekIndex: MAX_WEEK_INDEX,
    cohorts: [...byCohort.values()],
    curve,
    tenure,
    trend: grid
      .filter((r) => r.week_index === 1 || r.week_index === 4 || r.week_index === 12)
      .map((r) => ({
        week: r.cohort_week,
        index: r.week_index,
        pct: r.cohort_size ? (100 * r.actives) / r.cohort_size : null,
      }))
      .sort((a, b) => a.week.localeCompare(b.week)),
  }
})
