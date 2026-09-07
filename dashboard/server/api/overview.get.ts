/**
 * The landing page: how big is the active base, which way is it moving, and where do
 * installs come from and go.
 *
 * Every query here reads a pre-aggregated table. `active_counts_daily` is one row per
 * calendar day (a few hundred rows for a year), so the DAU/WAU/MAU series is a range
 * scan on a primary key rather than the six-CTE interval sweep it replaces.
 */
export default defineEventHandler(async (event) => {
  const range = resolveRange(event)
  const wk = weeklyWindow(range)

  const [series, hourly, current, previous, totals, lifecycle, funnel, population] =
    await Promise.all([
      query<{ day: string; dau: number; wau: number; mau: number }>(
        `SELECT day, dau, wau, mau
           FROM active_counts_daily
          WHERE day BETWEEN $1 AND $2
            -- today's bucket is still filling and would render as a cliff
            AND day < CURRENT_DATE
          ORDER BY day`,
        [range.from, range.to],
      ),

      // Only the 1-day and 2-day views get an hour axis; skip the query otherwise.
      range.bucket === 'hour'
        ? query<{ hour: string; active_installs: number; beacons: number }>(
            `SELECT hour, active_installs, beacons
               FROM active_counts_hourly
              WHERE hour >= $1::timestamptz AND hour <= $2::timestamptz
              ORDER BY hour`,
            [range.hourFrom, range.hourTo],
          )
        : Promise.resolve([]),

      query<{ day: string; dau: number; wau: number; mau: number }>(
        `SELECT day, dau, wau, mau FROM active_counts_daily
          WHERE day <= LEAST($1::date, CURRENT_DATE - 1) ORDER BY day DESC LIMIT 1`,
        [range.to],
      ),

      query<{ dau: number; wau: number; mau: number }>(
        `SELECT dau, wau, mau FROM active_counts_daily
          WHERE day <= $1::date ORDER BY day DESC LIMIT 1`,
        [range.prevTo],
      ),

      // Clamped to the last complete day at both ends: today's counts are still filling,
      // and on the 24-hour range that would otherwise report a fraction of a day as if
      // it were the whole of it.
      query<{ new_installs: number; churned: number; resurrected: number; first_launches: number }>(
        `SELECT COALESCE(sum(new_installs), 0)::int   AS new_installs,
                COALESCE(sum(churned), 0)::int        AS churned,
                COALESCE(sum(resurrected), 0)::int    AS resurrected,
                COALESCE(sum(first_launches), 0)::int AS first_launches
           FROM active_counts_daily
          WHERE day BETWEEN LEAST($1::date, CURRENT_DATE - 1)
                        AND LEAST($2::date, CURRENT_DATE - 1)`,
        [range.from, range.to],
      ),

      query<{
        week: string
        active: number
        new_installs: number
        retained: number
        resurrected: number
        churned: number
      }>(
        `SELECT week, active, new_installs, retained, resurrected, churned
           FROM weekly_lifecycle
          WHERE week BETWEEN $1 AND $2
            -- the current week is partial; showing it reads as a collapse
            AND week < date_trunc('week', CURRENT_DATE)::date
          ORDER BY week`,
        [wk.from, range.to],
      ),

      // The launch-only funnel: installs are seeded from their first `start`, and split
      // by whether they ever went on to send a periodic `events` beacon.
      query<{ week: string; became_active: number; launch_only: number }>(
        `SELECT date_trunc('week', first_start_day)::date AS week,
                count(*) FILTER (WHERE ever_active)::int     AS became_active,
                count(*) FILTER (WHERE NOT ever_active)::int AS launch_only
           FROM client_lifecycle
          WHERE first_start_day BETWEEN $1 AND $2
            -- the current week is still filling; plotting it reads as a collapse
            AND first_start_day < date_trunc('week', CURRENT_DATE)::date
          GROUP BY 1 ORDER BY 1`,
        [wk.from, range.to],
      ),

      query<{ ever_active: number; launch_only: number; active_28d: number }>(
        `SELECT count(*) FILTER (WHERE ever_active)::int     AS ever_active,
                count(*) FILTER (WHERE NOT ever_active)::int AS launch_only,
                count(*) FILTER (WHERE last_event_day > CURRENT_DATE - 28)::int AS active_28d
           FROM client_lifecycle`,
      ),
    ])

  const now = current[0] ?? { day: null, dau: 0, wau: 0, mau: 0 }
  const then = previous[0] ?? { dau: 0, wau: 0, mau: 0 }
  const pop = population[0] ?? { ever_active: 0, launch_only: 0, active_28d: 0 }
  const t = totals[0] ?? { new_installs: 0, churned: 0, resurrected: 0, first_launches: 0 }

  return {
    range,
    weekly: wk,
    kpis: {
      dau: { value: now.dau, previous: then.dau },
      wau: { value: now.wau, previous: then.wau },
      mau: { value: now.mau, previous: then.mau },
      stickiness: {
        value: now.mau ? round1((100 * now.wau) / now.mau) : null,
        previous: then.mau ? round1((100 * then.wau) / then.mau) : null,
      },
      newInstalls: t.new_installs,
      churned: t.churned,
      resurrected: t.resurrected,
      firstLaunches: t.first_launches,
      netGrowth: t.new_installs + t.resurrected - t.churned,
      everActive: pop.ever_active,
      launchOnly: pop.launch_only,
      // The single biggest number on the old dashboards: most first launches never
      // become an active install.
      activationRate:
        pop.ever_active + pop.launch_only
          ? round1((100 * pop.ever_active) / (pop.ever_active + pop.launch_only))
          : null,
      asOf: now.day,
    },
    series,
    hourly,
    lifecycle,
    funnel,
  }
})

function round1(n: number) {
  return Math.round(n * 10) / 10
}
