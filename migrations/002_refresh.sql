-- 002_refresh.sql
--
-- drain_refresh_analytics() rebuilds the derived tables from the continuous
-- aggregates. It is the only writer to everything created in 001, and it is what the
-- hourly scheduler runs.
--
--   CALL drain_refresh_analytics();            -- incremental, seconds
--   CALL drain_refresh_analytics(true);        -- full rebuild from scratch
--
-- Incremental mode reprocesses a trailing window instead of trying to be clever about
-- exactly which rows changed: beacons arrive late, and the continuous aggregates
-- themselves are refreshed on a policy, so anything near the tail can still move.

CREATE OR REPLACE PROCEDURE drain_refresh_analytics(p_full boolean DEFAULT false)
LANGUAGE plpgsql AS $proc$
DECLARE
  v_started   timestamptz := clock_timestamp();
  v_phase     timestamptz;
  v_from      date;
  v_week_from date;
  v_to        date;
  v_hour_from timestamptz;
BEGIN
  -- The snapshot window. 3 days of overlap on an incremental run absorbs both late
  -- beacons and the continuous aggregate's own end_offset.
  IF p_full THEN
    v_from := COALESCE((SELECT min(day)::date FROM daily_client_events), CURRENT_DATE);
  ELSE
    v_from := COALESCE((SELECT value::date FROM analytics_meta WHERE key = 'snapshot_day'),
                       (SELECT min(day)::date FROM daily_client_events),
                       CURRENT_DATE) - 3;
  END IF;
  v_to        := CURRENT_DATE;
  v_week_from := date_trunc('week', v_from)::date;

  --------------------------------------------------------------------------
  -- 1. Daily snapshot: JSONB parsed once, here, instead of on every page load.
  --------------------------------------------------------------------------
  v_phase := clock_timestamp();

  IF p_full THEN
    TRUNCATE client_snapshot_daily;
  ELSE
    DELETE FROM client_snapshot_daily WHERE day >= v_from;
  END IF;

  INSERT INTO client_snapshot_daily (
    day, client_id, version, auth_provider, browser, os, clients, running_containers,
    size_bucket, has_actions, has_hostname, has_custom_address, has_custom_base,
    is_swarm, has_auth, multi_client, feature_count)
  SELECT
    s.day, s.client_id, s.version, s.auth_provider, s.browser, s.os, s.clients,
    s.running_containers, drain_size_bucket(s.running_containers),
    s.has_actions, s.has_hostname, s.has_custom_address, s.has_custom_base, s.is_swarm,
    s.auth_provider <> 'none', s.clients > 1,
    (s.has_actions::int + s.has_hostname::int + s.has_custom_address::int
     + s.has_custom_base::int + s.is_swarm::int + (s.auth_provider <> 'none')::int)::smallint
  FROM (
    SELECT
      e.day::date                                                    AS day,
      e.client_id                                                    AS client_id,
      drain_minor_version(e.last_metadata ->> 'version')             AS version,
      COALESCE(NULLIF(e.last_metadata ->> 'authProvider', ''), 'none') AS auth_provider,
      drain_browser_family(e.last_metadata ->> 'browser')            AS browser,
      drain_os_family(e.last_metadata ->> 'browser')                 AS os,
      COALESCE((e.last_metadata ->> 'clients')::int, 0)              AS clients,
      (e.last_metadata ->> 'runningContainers')::int                 AS running_containers,
      COALESCE((e.last_metadata ->> 'hasActions')::boolean, false)   AS has_actions,
      COALESCE((e.last_metadata ->> 'hasHostname')::boolean, false)  AS has_hostname,
      COALESCE((e.last_metadata ->> 'hasCustomAddress')::boolean, false) AS has_custom_address,
      COALESCE((e.last_metadata ->> 'hasCustomBase')::boolean, false)    AS has_custom_base,
      COALESCE((e.last_metadata ->> 'isSwarmMode')::boolean, false)      AS is_swarm
    FROM daily_client_events e
    -- Beacons with no ServerID all land on the empty key and would roll up into a
    -- single phantom install reporting every minute.
    WHERE e.client_id <> '' AND e.day >= v_from
  ) s;

  INSERT INTO analytics_meta (key, value, updated_at, duration_ms)
  VALUES ('phase_snapshot_daily', now(), now(),
          (EXTRACT(epoch FROM clock_timestamp() - v_phase) * 1000)::int)
  ON CONFLICT (key) DO UPDATE
    SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at,
        duration_ms = EXCLUDED.duration_ms;
  COMMIT;

  --------------------------------------------------------------------------
  -- 2. client_lifecycle.
  --
  -- Collapses the `min(day) GROUP BY client_id` that every cohort query used to
  -- recompute from scratch. Maintained by upsert rather than rebuilt: an install's
  -- first day only ever moves earlier and its last day only ever moves later, so
  -- LEAST/GREATEST against the existing row is exact while only reading the window.
  -- A full run TRUNCATEs first and then takes the same path over all of history.
  --------------------------------------------------------------------------
  v_phase := clock_timestamp();

  IF p_full THEN
    TRUNCATE client_lifecycle;
  END IF;

  INSERT INTO client_lifecycle (client_id, first_event_day, last_event_day,
                                first_event_week, ever_active)
  SELECT client_id, min(day), max(day), date_trunc('week', min(day))::date, true
  FROM client_snapshot_daily
  WHERE day >= v_from
  GROUP BY client_id
  ON CONFLICT (client_id) DO UPDATE
    SET first_event_day  = LEAST(client_lifecycle.first_event_day, EXCLUDED.first_event_day),
        last_event_day   = GREATEST(client_lifecycle.last_event_day, EXCLUDED.last_event_day),
        first_event_week = date_trunc('week',
                             LEAST(client_lifecycle.first_event_day, EXCLUDED.first_event_day))::date,
        ever_active      = true;

  -- Installs that only ever announced themselves and died: ~4x the active population.
  -- They are the launch-only funnel, not cohort members, so this never sets ever_active
  -- - an install already marked active keeps that flag.
  INSERT INTO client_lifecycle (client_id, first_start_day, last_start_day, ever_active)
  SELECT client_id, min(day)::date, max(day)::date, false
  FROM daily_client_starts
  WHERE client_id <> '' AND day >= v_from
  GROUP BY client_id
  ON CONFLICT (client_id) DO UPDATE
    SET first_start_day = LEAST(COALESCE(client_lifecycle.first_start_day, EXCLUDED.first_start_day),
                                EXCLUDED.first_start_day),
        last_start_day  = GREATEST(COALESCE(client_lifecycle.last_start_day, EXCLUDED.last_start_day),
                                   EXCLUDED.last_start_day);

  ANALYZE client_lifecycle;

  INSERT INTO analytics_meta (key, value, updated_at, duration_ms)
  VALUES ('phase_lifecycle', now(), now(),
          (EXTRACT(epoch FROM clock_timestamp() - v_phase) * 1000)::int)
  ON CONFLICT (key) DO UPDATE
    SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at,
        duration_ms = EXCLUDED.duration_ms;
  COMMIT;

  --------------------------------------------------------------------------
  -- 3. Weekly snapshot: the same facts at the install's last active day of the week,
  --    plus how many days it reported. ~7x smaller, and what the long ranges read.
  --------------------------------------------------------------------------
  v_phase := clock_timestamp();

  IF p_full THEN
    TRUNCATE client_snapshot_weekly;
  ELSE
    DELETE FROM client_snapshot_weekly WHERE week >= v_week_from;
  END IF;

  INSERT INTO client_snapshot_weekly (
    week, client_id, version, auth_provider, browser, os, clients, peak_clients,
    running_containers, size_bucket, has_actions, has_hostname, has_custom_address,
    has_custom_base, is_swarm, has_auth, multi_client, feature_count, active_days,
    tenure_band)
  SELECT
    l.week, l.client_id, l.version, l.auth_provider, l.browser, l.os, l.clients,
    a.peak_clients, l.running_containers, l.size_bucket, l.has_actions, l.has_hostname,
    l.has_custom_address, l.has_custom_base, l.is_swarm, l.has_auth, l.multi_client,
    l.feature_count, a.active_days,
    CASE WHEN l.week - c.first_event_day < 7   THEN 0
         WHEN l.week - c.first_event_day < 28  THEN 1
         WHEN l.week - c.first_event_day < 90  THEN 2
         WHEN l.week - c.first_event_day < 365 THEN 3
         ELSE 4 END::smallint
  FROM (
    SELECT DISTINCT ON (date_trunc('week', day), client_id)
           date_trunc('week', day)::date AS week, d.*
    FROM client_snapshot_daily d
    WHERE d.day >= v_week_from
    ORDER BY date_trunc('week', day), client_id, day DESC
  ) l
  JOIN (
    SELECT date_trunc('week', day)::date AS week, client_id,
           count(*)::smallint AS active_days, max(clients) AS peak_clients
    FROM client_snapshot_daily
    WHERE day >= v_week_from
    GROUP BY 1, 2
  ) a ON a.week = l.week AND a.client_id = l.client_id
  JOIN client_lifecycle c ON c.client_id = l.client_id;

  INSERT INTO analytics_meta (key, value, updated_at, duration_ms)
  VALUES ('phase_snapshot_weekly', now(), now(),
          (EXTRACT(epoch FROM clock_timestamp() - v_phase) * 1000)::int)
  ON CONFLICT (key) DO UPDATE
    SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at,
        duration_ms = EXCLUDED.duration_ms;
  COMMIT;


  --------------------------------------------------------------------------
  -- 4. client_latest: the most recent state of every install that has ever been active.
  --    Only installs seen in the window can have moved, so this is an upsert over the
  --    window rather than a DISTINCT ON across all of history.
  --------------------------------------------------------------------------
  v_phase := clock_timestamp();

  IF p_full THEN
    TRUNCATE client_latest;
  END IF;

  INSERT INTO client_latest (
    client_id, last_event_day, version, auth_provider, browser, os, clients,
    running_containers, size_bucket, has_actions, has_hostname, has_custom_address,
    has_custom_base, is_swarm, has_auth, multi_client, feature_count)
  SELECT DISTINCT ON (client_id)
    client_id, day, version, auth_provider, browser, os, clients,
    running_containers, size_bucket, has_actions, has_hostname, has_custom_address,
    has_custom_base, is_swarm, has_auth, multi_client, feature_count
  FROM client_snapshot_daily
  WHERE day >= v_from
  ORDER BY client_id, day DESC
  ON CONFLICT (client_id) DO UPDATE
    SET last_event_day     = EXCLUDED.last_event_day,
        version            = EXCLUDED.version,
        auth_provider      = EXCLUDED.auth_provider,
        browser            = EXCLUDED.browser,
        os                 = EXCLUDED.os,
        clients            = EXCLUDED.clients,
        running_containers = EXCLUDED.running_containers,
        size_bucket        = EXCLUDED.size_bucket,
        has_actions        = EXCLUDED.has_actions,
        has_hostname       = EXCLUDED.has_hostname,
        has_custom_address = EXCLUDED.has_custom_address,
        has_custom_base    = EXCLUDED.has_custom_base,
        is_swarm           = EXCLUDED.is_swarm,
        has_auth           = EXCLUDED.has_auth,
        multi_client       = EXCLUDED.multi_client,
        feature_count      = EXCLUDED.feature_count
    -- Never let a reprocessed window walk an install's state backwards.
    WHERE EXCLUDED.last_event_day >= client_latest.last_event_day;

  ANALYZE client_latest;

  INSERT INTO analytics_meta (key, value, updated_at, duration_ms)
  VALUES ('phase_client_latest', now(), now(),
          (EXTRACT(epoch FROM clock_timestamp() - v_phase) * 1000)::int)
  ON CONFLICT (key) DO UPDATE
    SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at,
        duration_ms = EXCLUDED.duration_ms;
  COMMIT;

  --------------------------------------------------------------------------
  -- 5. active_counts_daily.
  --
  -- DAU/WAU/MAU are trailing-window distinct counts, the most expensive thing on the
  -- old dashboards. Instead of counting distinct clients once per output day, each
  -- install's active days are merged into coverage intervals and the intervals are
  -- swept: O(n log n) for the whole range rather than O(days x rows).
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
    SELECT client_id, day FROM client_snapshot_daily WHERE day >= v_from - 28
  ),
  w(win) AS (VALUES (1), (7), (28)),
  marked AS (
    SELECT w.win, d.client_id, d.day,
           CASE WHEN d.day - lag(d.day) OVER (PARTITION BY w.win, d.client_id ORDER BY d.day) > w.win
                THEN 1 ELSE 0 END AS gap
    FROM d CROSS JOIN w
  ),
  grouped AS (
    SELECT win, client_id, day,
           sum(gap) OVER (PARTITION BY win, client_id ORDER BY day) AS run
    FROM marked
  ),
  spans AS (
    SELECT win, min(day) AS opens, max(day) + win AS closes
    FROM grouped GROUP BY win, client_id, run
  ),
  edges AS (
    SELECT win, opens AS day, 1 AS delta FROM spans
    UNION ALL
    SELECT win, closes, -1 FROM spans
  ),
  netted AS (SELECT win, day, sum(delta) AS delta FROM edges GROUP BY 1, 2),
  -- MATERIALIZED matters: the grid below reaches into this from a correlated
  -- subquery, and a single-reference CTE would be inlined and re-run per output day.
  cum AS MATERIALIZED (
    SELECT win, day, sum(delta) OVER (PARTITION BY win ORDER BY day) AS v FROM netted
  ),
  -- Gap to the previous / next appearance, for resurrection and churn. Aggregated to a
  -- row per day and joined, never probed once per output day: `d` holds a row per
  -- install per active day, so a correlated subquery here would re-scan millions of rows
  -- for every day on the grid.
  gaps AS (
    SELECT client_id, day,
           lag(day)  OVER (PARTITION BY client_id ORDER BY day) AS prev_day,
           lead(day) OVER (PARTITION BY client_id ORDER BY day) AS next_day
    FROM d
  ),
  -- Active today, seen before, but silent for the whole 28-day window before it.
  --
  -- On an incremental run `d` only reaches back 28 days, so an install returning after a
  -- longer gap has no prev_day inside the window at all. client_lifecycle settles those:
  -- no previous day in the window and an activation older than the window means it is a
  -- resurrection, not a new install.
  resurrected AS (
    SELECT g.day, count(*)::int AS n
      FROM gaps g
      JOIN client_lifecycle l ON l.client_id = g.client_id
     WHERE (g.prev_day IS NOT NULL AND g.day - g.prev_day > 28)
        OR (g.prev_day IS NULL AND l.first_event_day < g.day - 28)
     GROUP BY 1
  ),
  -- Went quiet: last seen 28 days ago and never came back inside the window. Attributed
  -- to the day the install is finally declared churned.
  churned AS (
    SELECT (day + 28) AS day, count(*)::int AS n FROM gaps
     WHERE next_day IS NULL OR next_day - day > 28
     GROUP BY 1
  ),
  activated AS (
    SELECT first_event_day AS day, count(*)::int AS n
      FROM client_lifecycle WHERE ever_active AND first_event_day IS NOT NULL
     GROUP BY 1
  ),
  launched AS (
    SELECT first_start_day AS day, count(*)::int AS n
      FROM client_lifecycle WHERE first_start_day IS NOT NULL
     GROUP BY 1
  ),
  grid AS (SELECT generate_series(v_from, v_to, INTERVAL '1 day')::date AS day)
  SELECT g.day,
         COALESCE((SELECT c.v FROM cum c WHERE c.win = 1  AND c.day <= g.day ORDER BY c.day DESC LIMIT 1), 0),
         COALESCE((SELECT c.v FROM cum c WHERE c.win = 7  AND c.day <= g.day ORDER BY c.day DESC LIMIT 1), 0),
         COALESCE((SELECT c.v FROM cum c WHERE c.win = 28 AND c.day <= g.day ORDER BY c.day DESC LIMIT 1), 0),
         COALESCE(a.n, 0),
         COALESCE(r.n, 0),
         COALESCE(ch.n, 0),
         COALESCE(l.n, 0)
  FROM grid g
  LEFT JOIN activated   a  ON a.day  = g.day
  LEFT JOIN resurrected r  ON r.day  = g.day
  LEFT JOIN churned     ch ON ch.day = g.day
  LEFT JOIN launched    l  ON l.day  = g.day;

  INSERT INTO analytics_meta (key, value, updated_at, duration_ms)
  VALUES ('phase_active_counts', now(), now(),
          (EXTRACT(epoch FROM clock_timestamp() - v_phase) * 1000)::int)
  ON CONFLICT (key) DO UPDATE
    SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at,
        duration_ms = EXCLUDED.duration_ms;
  COMMIT;

  --------------------------------------------------------------------------
  -- 6. Weekly lifecycle: new / retained / resurrected / churned installs per week.
  --------------------------------------------------------------------------
  v_phase := clock_timestamp();

  TRUNCATE weekly_lifecycle;

  INSERT INTO weekly_lifecycle (week, active, new_installs, retained, resurrected, churned)
  WITH f AS (
    SELECT week, client_id,
           lag(week)  OVER (PARTITION BY client_id ORDER BY week) AS prev_week,
           lead(week) OVER (PARTITION BY client_id ORDER BY week) AS next_week
    FROM client_snapshot_weekly
  ),
  present AS (
    SELECT week,
           count(*)::int                                                AS active,
           count(*) FILTER (WHERE prev_week IS NULL)::int               AS new_installs,
           count(*) FILTER (WHERE prev_week = week - 7)::int            AS retained,
           count(*) FILTER (WHERE prev_week IS NOT NULL
                              AND prev_week < week - 7)::int            AS resurrected
    FROM f GROUP BY week
  ),
  gone AS (
    -- Attributed to the week they failed to show up in.
    SELECT (week + 7)::date AS week, count(*)::int AS churned
    FROM f
    WHERE next_week IS NULL OR next_week > week + 7
    GROUP BY 1
  )
  SELECT COALESCE(p.week, g.week), COALESCE(p.active, 0), COALESCE(p.new_installs, 0),
         COALESCE(p.retained, 0), COALESCE(p.resurrected, 0), COALESCE(g.churned, 0)
  FROM present p
  FULL OUTER JOIN gone g ON g.week = p.week;

  INSERT INTO analytics_meta (key, value, updated_at, duration_ms)
  VALUES ('phase_weekly_lifecycle', now(), now(),
          (EXTRACT(epoch FROM clock_timestamp() - v_phase) * 1000)::int)
  ON CONFLICT (key) DO UPDATE
    SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at,
        duration_ms = EXCLUDED.duration_ms;
  COMMIT;

  --------------------------------------------------------------------------
  -- 7. Cohort grid. Rebuilt whole; it is a few thousand rows out of a hash join.
  --------------------------------------------------------------------------
  v_phase := clock_timestamp();

  TRUNCATE cohort_retention_weekly;

  INSERT INTO cohort_retention_weekly (cohort_week, week_index, cohort_size, actives)
  WITH cohorts AS (
    SELECT first_event_week AS cohort_week, count(*)::int AS cohort_size
    FROM client_lifecycle WHERE ever_active
    GROUP BY 1
  ),
  activity AS (
    SELECT l.first_event_week AS cohort_week,
           ((w.week - l.first_event_week) / 7)::smallint AS week_index,
           count(*)::int AS actives
    FROM client_snapshot_weekly w
    JOIN client_lifecycle l ON l.client_id = w.client_id AND l.ever_active
    WHERE w.week >= l.first_event_week
      AND (w.week - l.first_event_week) / 7 <= 52
    GROUP BY 1, 2
  )
  SELECT a.cohort_week, a.week_index, c.cohort_size, a.actives
  FROM activity a
  JOIN cohorts c ON c.cohort_week = a.cohort_week;

  INSERT INTO analytics_meta (key, value, updated_at, duration_ms)
  VALUES ('phase_cohorts', now(), now(),
          (EXTRACT(epoch FROM clock_timestamp() - v_phase) * 1000)::int)
  ON CONFLICT (key) DO UPDATE
    SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at,
        duration_ms = EXCLUDED.duration_ms;
  COMMIT;

  --------------------------------------------------------------------------
  -- 8. Hourly actives, for the 1-day and 7-day ranges. The source aggregate only keeps
  --    15 days, so this rolls it up into a row per hour before it is dropped.
  --------------------------------------------------------------------------
  v_phase := clock_timestamp();

  v_hour_from := CASE WHEN p_full THEN '-infinity'::timestamptz
                      ELSE date_trunc('hour', now()) - INTERVAL '3 days' END;

  DELETE FROM active_counts_hourly WHERE hour >= v_hour_from;

  INSERT INTO active_counts_hourly (hour, active_installs, beacons)
  SELECT hour, count(*)::int, sum(beacons)
  FROM hourly_client_events
  WHERE client_id <> '' AND hour >= v_hour_from
  GROUP BY 1
  ON CONFLICT (hour) DO UPDATE
    SET active_installs = EXCLUDED.active_installs, beacons = EXCLUDED.beacons;

  --------------------------------------------------------------------------

  INSERT INTO analytics_meta (key, value, updated_at, duration_ms)
  VALUES ('snapshot_day', v_to::timestamptz, now(), NULL),
         ('last_refresh', now(), now(),
          (EXTRACT(epoch FROM clock_timestamp() - v_started) * 1000)::int),
         ('phase_hourly', now(), now(),
          (EXTRACT(epoch FROM clock_timestamp() - v_phase) * 1000)::int)
  ON CONFLICT (key) DO UPDATE
    SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at,
        duration_ms = EXCLUDED.duration_ms;
  COMMIT;
END;
$proc$;
