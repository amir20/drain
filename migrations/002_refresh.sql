-- 002_refresh.sql
--
-- drain_refresh_analytics() rebuilds the derived tables from the continuous aggregates.
-- It is the only writer to everything created in 001, and it is what the hourly
-- scheduler runs.
--
--   CALL drain_refresh_analytics();       -- incremental, seconds
--   CALL drain_refresh_analytics(true);   -- full rebuild from scratch
--
-- Incremental mode reprocesses a trailing window rather than working out exactly which
-- rows changed: beacons arrive late, and the continuous aggregates are themselves
-- refreshed on a policy, so anything near the tail can still move.
--
-- There is no JSONB parsing anywhere below. The beacon payload is copied through
-- verbatim and parsed by the dashboard on read, which is what keeps a new beacon field
-- from needing a migration here.

-- Records how long a phase took. Split out so each phase is two lines of bookkeeping
-- rather than seven.
CREATE OR REPLACE FUNCTION drain_note_phase(p_key text, p_started timestamptz)
RETURNS void LANGUAGE sql AS
$$
  INSERT INTO analytics_meta (key, value, updated_at, duration_ms)
  VALUES (p_key, now(), now(), (EXTRACT(epoch FROM clock_timestamp() - p_started) * 1000)::int)
  ON CONFLICT (key) DO UPDATE
    SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at,
        duration_ms = EXCLUDED.duration_ms;
$$;

CREATE OR REPLACE PROCEDURE drain_refresh_analytics(p_full boolean DEFAULT false)
LANGUAGE plpgsql AS $proc$
DECLARE
  v_started    timestamptz := clock_timestamp();
  v_phase      timestamptz;
  v_from       date;
  v_start_from date;
  v_week_from  date;
  v_to         date := CURRENT_DATE;
  v_hour_from  timestamptz;
BEGIN
  -- The window. 3 days of overlap on an incremental run absorbs both late beacons and
  -- the continuous aggregate's own end_offset.
  IF p_full THEN
    v_from := COALESCE((SELECT min(day) FROM client_daily), v_to);
    -- Floored by its own history, not the events aggregate's: the two aggregates cover
    -- disjoint slices of `beacon` and 'start' comes from ~4x more installs, so its
    -- history can begin earlier. Reusing v_from would drop every launch-only install
    -- whose start predates the first events beacon - the population the funnel is about.
    v_start_from := COALESCE((SELECT min(day) FROM client_starts), v_to);
  ELSE
    v_from := COALESCE((SELECT value::date FROM analytics_meta WHERE key = 'snapshot_day'),
                       (SELECT min(day) FROM client_daily),
                       v_to) - 3;
    v_start_from := v_from;
  END IF;
  v_week_from := date_trunc('week', v_from)::date;

  --------------------------------------------------------------------------
  -- 1. client_lifecycle.
  --
  -- Maintained by upsert, not rebuilt: an install's first day only ever moves earlier
  -- and its last day only ever later, so LEAST/GREATEST against the existing row is
  -- exact while reading only the window. A full run TRUNCATEs first and takes the same
  -- path over all of history.
  --------------------------------------------------------------------------
  v_phase := clock_timestamp();

  IF p_full THEN
    TRUNCATE client_lifecycle;
  END IF;

  INSERT INTO client_lifecycle (client_id, first_event_day, last_event_day,
                                first_event_week, ever_active, metadata)
  SELECT DISTINCT ON (client_id)
         client_id,
         min(day) OVER (PARTITION BY client_id),
         max(day) OVER (PARTITION BY client_id),
         date_trunc('week', min(day) OVER (PARTITION BY client_id))::date,
         true,
         metadata
  FROM client_daily
  WHERE day >= v_from
  ORDER BY client_id, day DESC
  ON CONFLICT (client_id) DO UPDATE
    SET first_event_day  = LEAST(client_lifecycle.first_event_day, EXCLUDED.first_event_day),
        last_event_day   = GREATEST(client_lifecycle.last_event_day, EXCLUDED.last_event_day),
        first_event_week = date_trunc('week',
                             LEAST(client_lifecycle.first_event_day, EXCLUDED.first_event_day))::date,
        ever_active      = true,
        metadata         = EXCLUDED.metadata;

  -- Installs that only ever announced themselves and died: ~4x the active population.
  -- They are the launch-only funnel, not cohort members, so this never sets ever_active
  -- - an install already marked active keeps the flag.
  INSERT INTO client_lifecycle (client_id, first_start_day, last_start_day, ever_active)
  SELECT client_id, min(day), max(day), false
  FROM client_starts
  WHERE day >= v_start_from
  GROUP BY client_id
  ON CONFLICT (client_id) DO UPDATE
    SET first_start_day = LEAST(COALESCE(client_lifecycle.first_start_day, EXCLUDED.first_start_day),
                                EXCLUDED.first_start_day),
        last_start_day  = GREATEST(COALESCE(client_lifecycle.last_start_day, EXCLUDED.last_start_day),
                                   EXCLUDED.last_start_day);

  ANALYZE client_lifecycle;
  PERFORM drain_note_phase('phase_lifecycle', v_phase);
  COMMIT;

  --------------------------------------------------------------------------
  -- 2. client_weekly: the install's last beacon of each week, plus how many days it
  --    reported. The one rollup, and what every long range reads.
  --------------------------------------------------------------------------
  v_phase := clock_timestamp();

  IF p_full THEN
    TRUNCATE client_weekly;
  ELSE
    DELETE FROM client_weekly WHERE week >= v_week_from;
  END IF;

  INSERT INTO client_weekly (week, client_id, metadata, active_days, peak_clients, first_event_day)
  SELECT DISTINCT ON (date_trunc('week', d.day), d.client_id)
         date_trunc('week', d.day)::date,
         d.client_id,
         d.metadata,
         count(*)             OVER w,
         max(drain_int(d.metadata, 'clients')) OVER w,
         l.first_event_day
  FROM client_daily d
  JOIN client_lifecycle l ON l.client_id = d.client_id
  WHERE d.day >= v_week_from
  WINDOW w AS (PARTITION BY date_trunc('week', d.day), d.client_id)
  ORDER BY date_trunc('week', d.day), d.client_id, d.day DESC;

  PERFORM drain_note_phase('phase_weekly', v_phase);
  COMMIT;

  --------------------------------------------------------------------------
  -- 3. active_counts_daily.
  --
  -- DAU/WAU/MAU are trailing-window distinct counts. Rather than counting distinct
  -- clients once per output day, each install's active days are merged into coverage
  -- intervals and the intervals are swept: O(n log n) for the whole range instead of
  -- O(days x rows).
  --
  -- Reading from v_from - 28 is exactly enough: a day X only counts toward the 28-day
  -- window through X + 27, so nothing older can affect the output range.
  --------------------------------------------------------------------------
  v_phase := clock_timestamp();

  IF p_full THEN
    TRUNCATE active_counts_daily;
  ELSE
    DELETE FROM active_counts_daily WHERE day >= v_from;
  END IF;

  INSERT INTO active_counts_daily (day, dau, wau, mau, new_installs, resurrected, churned, first_launches)
  WITH d AS (
    SELECT client_id, day FROM client_daily WHERE day >= v_from - 28
  ),
  w(win) AS (VALUES (1), (7), (28)),
  marked AS (
    SELECT w.win, d.client_id, d.day,
           CASE WHEN d.day - lag(d.day) OVER (PARTITION BY w.win, d.client_id ORDER BY d.day) > w.win
                THEN 1 ELSE 0 END AS gap
    FROM d CROSS JOIN w
  ),
  grouped AS (
    SELECT win, client_id, day, sum(gap) OVER (PARTITION BY win, client_id ORDER BY day) AS run
    FROM marked
  ),
  spans AS (
    SELECT win, min(day) AS opens, max(day) + win AS closes
    FROM grouped GROUP BY win, client_id, run
  ),
  edges AS (
    SELECT win, opens AS day, 1 AS delta FROM spans
    UNION ALL SELECT win, closes, -1 FROM spans
  ),
  netted AS (SELECT win, day, sum(delta) AS delta FROM edges GROUP BY 1, 2),
  -- MATERIALIZED matters: the grid below reaches into this from a correlated subquery,
  -- and a single-reference CTE would be inlined and re-run per output day.
  cum AS MATERIALIZED (
    SELECT win, day, sum(delta) OVER (PARTITION BY win ORDER BY day) AS v FROM netted
  ),
  -- Gap to the previous / next appearance. Aggregated to a row per day and joined, never
  -- probed once per output day: `d` holds a row per install per active day, so a
  -- correlated subquery here would re-scan millions of rows for every day on the grid.
  gaps AS (
    SELECT client_id, day,
           lag(day)  OVER (PARTITION BY client_id ORDER BY day) AS prev_day,
           lead(day) OVER (PARTITION BY client_id ORDER BY day) AS next_day
    FROM d
  ),
  -- Active today, seen before, but silent for the whole 28-day window before it. On an
  -- incremental run `d` only reaches back 28 days, so an install returning after a longer
  -- gap has no prev_day in the window at all; client_lifecycle settles those.
  resurrected AS (
    SELECT g.day, count(*)::int AS n
    FROM gaps g JOIN client_lifecycle l ON l.client_id = g.client_id
    WHERE (g.prev_day IS NOT NULL AND g.day - g.prev_day > 28)
       OR (g.prev_day IS NULL AND l.first_event_day < g.day - 28)
    GROUP BY 1
  ),
  -- Went quiet: last seen 28 days ago and never came back inside the window, attributed
  -- to the day the install is finally declared churned.
  churned AS (
    SELECT (day + 28) AS day, count(*)::int AS n FROM gaps
    WHERE next_day IS NULL OR next_day - day > 28
    GROUP BY 1
  ),
  activated AS (
    SELECT first_event_day AS day, count(*)::int AS n FROM client_lifecycle
    WHERE ever_active AND first_event_day IS NOT NULL GROUP BY 1
  ),
  launched AS (
    SELECT first_start_day AS day, count(*)::int AS n FROM client_lifecycle
    WHERE first_start_day IS NOT NULL GROUP BY 1
  ),
  grid AS (SELECT generate_series(v_from, v_to, INTERVAL '1 day')::date AS day)
  SELECT g.day,
         COALESCE((SELECT c.v FROM cum c WHERE c.win = 1  AND c.day <= g.day ORDER BY c.day DESC LIMIT 1), 0),
         COALESCE((SELECT c.v FROM cum c WHERE c.win = 7  AND c.day <= g.day ORDER BY c.day DESC LIMIT 1), 0),
         COALESCE((SELECT c.v FROM cum c WHERE c.win = 28 AND c.day <= g.day ORDER BY c.day DESC LIMIT 1), 0),
         COALESCE(a.n, 0), COALESCE(r.n, 0), COALESCE(ch.n, 0), COALESCE(l.n, 0)
  FROM grid g
  LEFT JOIN activated   a  ON a.day  = g.day
  LEFT JOIN resurrected r  ON r.day  = g.day
  LEFT JOIN churned     ch ON ch.day = g.day
  LEFT JOIN launched    l  ON l.day  = g.day;

  PERFORM drain_note_phase('phase_active_counts', v_phase);
  COMMIT;

  --------------------------------------------------------------------------
  -- 4. Weekly lifecycle: new / retained / resurrected / churned installs per week.
  --------------------------------------------------------------------------
  v_phase := clock_timestamp();

  TRUNCATE weekly_lifecycle;

  INSERT INTO weekly_lifecycle (week, active, new_installs, retained, resurrected, churned)
  WITH f AS (
    SELECT week, client_id,
           lag(week)  OVER (PARTITION BY client_id ORDER BY week) AS prev_week,
           lead(week) OVER (PARTITION BY client_id ORDER BY week) AS next_week
    FROM client_weekly
  ),
  present AS (
    SELECT week,
           count(*)::int                                      AS active,
           count(*) FILTER (WHERE prev_week IS NULL)::int     AS new_installs,
           count(*) FILTER (WHERE prev_week = week - 7)::int  AS retained,
           count(*) FILTER (WHERE prev_week IS NOT NULL
                              AND prev_week < week - 7)::int  AS resurrected
    FROM f GROUP BY week
  ),
  gone AS (
    SELECT (week + 7)::date AS week, count(*)::int AS churned
    FROM f WHERE next_week IS NULL OR next_week > week + 7
    GROUP BY 1
  )
  SELECT COALESCE(p.week, g.week), COALESCE(p.active, 0), COALESCE(p.new_installs, 0),
         COALESCE(p.retained, 0), COALESCE(p.resurrected, 0), COALESCE(g.churned, 0)
  FROM present p FULL OUTER JOIN gone g ON g.week = p.week;

  PERFORM drain_note_phase('phase_weekly_lifecycle', v_phase);
  COMMIT;

  --------------------------------------------------------------------------
  -- 5. Cohort grid. Rebuilt whole; it is a few thousand rows out of a hash join.
  --------------------------------------------------------------------------
  v_phase := clock_timestamp();

  TRUNCATE cohort_retention_weekly;

  INSERT INTO cohort_retention_weekly (cohort_week, week_index, cohort_size, actives)
  WITH cohorts AS (
    SELECT first_event_week AS cohort_week, count(*)::int AS cohort_size
    FROM client_lifecycle WHERE ever_active GROUP BY 1
  ),
  activity AS (
    SELECT l.first_event_week AS cohort_week,
           ((w.week - l.first_event_week) / 7)::smallint AS week_index,
           count(*)::int AS actives
    FROM client_weekly w
    JOIN client_lifecycle l ON l.client_id = w.client_id AND l.ever_active
    WHERE w.week >= l.first_event_week AND (w.week - l.first_event_week) / 7 <= 52
    GROUP BY 1, 2
  )
  SELECT a.cohort_week, a.week_index, c.cohort_size, a.actives
  FROM activity a JOIN cohorts c ON c.cohort_week = a.cohort_week;

  PERFORM drain_note_phase('phase_cohorts', v_phase);
  COMMIT;

  --------------------------------------------------------------------------
  -- 6. Hourly actives, for the 1-day and 7-day ranges.
  --
  -- A full run must NOT delete from -infinity here. This is the one phase whose source
  -- carries a retention policy: hourly_client_events keeps 15 days, while this table is
  -- the permanent record rolled up from it. So a full run rewrites exactly the window
  -- the aggregate can still supply and leaves the accumulated history alone.
  --------------------------------------------------------------------------
  v_phase := clock_timestamp();

  v_hour_from := CASE
                   WHEN p_full THEN COALESCE((SELECT min(hour) FROM hourly_client_events),
                                             date_trunc('hour', now()))
                   ELSE date_trunc('hour', now()) - INTERVAL '3 days'
                 END;

  DELETE FROM active_counts_hourly WHERE hour >= v_hour_from;

  INSERT INTO active_counts_hourly (hour, active_installs, beacons)
  SELECT hour, count(*)::int, sum(beacons)
  FROM hourly_client_events
  WHERE client_id <> '' AND hour >= v_hour_from
  GROUP BY 1
  ON CONFLICT (hour) DO UPDATE
    SET active_installs = EXCLUDED.active_installs, beacons = EXCLUDED.beacons;

  PERFORM drain_note_phase('phase_hourly', v_phase);

  INSERT INTO analytics_meta (key, value, updated_at, duration_ms)
  VALUES ('snapshot_day', v_to::timestamptz, now(), NULL)
  ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at;
  PERFORM drain_note_phase('last_refresh', v_started);
  COMMIT;
END;
$proc$;
