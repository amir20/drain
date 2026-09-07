-- 001_analytics.sql
--
-- Derived analytics layer for the Nuxt dashboard.
--
-- The dashboard never reads `beacon` or parses JSONB at request time. Everything it
-- needs lives in narrow, typed, pre-aggregated tables refreshed by
-- drain_refresh_analytics(). A page load is a handful of index scans over tables that
-- are either tiny (a row per day) or well pruned (hypertables bucketed by time).
--
-- Idempotent: safe to run on every deploy.
--
-- Statements are applied one at a time, not wrapped in a transaction: TimescaleDB
-- refuses to create a continuous aggregate inside a transaction block. Everything here
-- is guarded, so a run that dies half way is finished by the next one.

-- ---------------------------------------------------------------------------
-- Helpers. IMMUTABLE so they can be used in indexes and generated columns.
-- ---------------------------------------------------------------------------

-- Patch releases are noise: a year holds 554 distinct versions but only a couple of
-- dozen minors. Everything version-shaped is rolled up to the minor.
CREATE OR REPLACE FUNCTION drain_minor_version(v text) RETURNS text
  LANGUAGE sql IMMUTABLE PARALLEL SAFE AS
$$ SELECT coalesce(nullif(substring(v from '^v?[0-9]+[.][0-9]+'), ''), 'unknown') $$;

CREATE OR REPLACE FUNCTION drain_browser_family(ua text) RETURNS text
  LANGUAGE sql IMMUTABLE PARALLEL SAFE AS
$$ SELECT CASE
     WHEN ua IS NULL OR ua = ''                 THEN 'Unknown'
     WHEN ua LIKE '%Edg/%' OR ua LIKE '%Edge/%' THEN 'Edge'
     WHEN ua LIKE '%OPR/%' OR ua LIKE '%Opera%' THEN 'Opera'
     WHEN ua LIKE '%Firefox/%'                  THEN 'Firefox'
     WHEN ua LIKE '%Chrome/%'                   THEN 'Chrome'
     WHEN ua LIKE '%Safari/%'                   THEN 'Safari'
     ELSE 'Other' END $$;

CREATE OR REPLACE FUNCTION drain_os_family(ua text) RETURNS text
  LANGUAGE sql IMMUTABLE PARALLEL SAFE AS
$$ SELECT CASE
     WHEN ua IS NULL OR ua = ''  THEN 'Unknown'
     WHEN ua LIKE '%Windows%'    THEN 'Windows'
     WHEN ua LIKE '%Android%'    THEN 'Android'
     WHEN ua LIKE '%iPhone%' OR ua LIKE '%iPad%' OR ua LIKE '%iPod%'    THEN 'iOS'
     WHEN ua LIKE '%Mac OS X%' OR ua LIKE '%Macintosh%'                 THEN 'macOS'
     WHEN ua LIKE '%Linux%' OR ua LIKE '%X11%'                          THEN 'Linux'
     ELSE 'Other' END $$;

-- Ordinal so the UI can sort buckets without shipping label prefixes like '3 - 6 to 20'.
--
-- Five ordered bands plus an unknown. Five is a ceiling, not a preference: an ordered
-- scale has to be drawn with a one-hue ramp for the order to be visible, and a ramp
-- needs roughly a 0.06 lightness gap between steps to stay separable - which leaves five
-- steps between "barely darker than the surface" and black. A sixth band would be a
-- split the reader cannot see, so 21-50 and 51-200 are one band.
CREATE OR REPLACE FUNCTION drain_size_bucket(c int) RETURNS smallint
  LANGUAGE sql IMMUTABLE PARALLEL SAFE AS
$$ SELECT CASE
     WHEN c IS NULL THEN 5 WHEN c = 0 THEN 0 WHEN c <= 5 THEN 1
     WHEN c <= 20 THEN 2 WHEN c <= 200 THEN 3
     ELSE 4 END::smallint $$;

-- ---------------------------------------------------------------------------
-- Refresh bookkeeping
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS analytics_meta (
  key         text PRIMARY KEY,
  value       timestamptz,
  updated_at  timestamptz NOT NULL DEFAULT now(),
  duration_ms integer
);

-- ---------------------------------------------------------------------------
-- Hourly continuous aggregate.
--
-- Only exists so the 1-day and 7-day ranges can be charted at hour granularity.
-- A year of hourly per-client rows would be ~24x the daily aggregate, so it carries a
-- short retention: the rollups the dashboard actually serves live in
-- active_counts_hourly, which is a row per hour and kept forever.
-- ---------------------------------------------------------------------------

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM timescaledb_information.continuous_aggregates
                 WHERE view_name = 'hourly_client_events') THEN
    CREATE MATERIALIZED VIEW hourly_client_events
    WITH (timescaledb.continuous) AS
    SELECT time_bucket('1 hour', time) AS hour,
           client_id,
           count(*) AS beacons
    FROM beacon
    WHERE name = 'events'
    GROUP BY 1, 2
    WITH NO DATA;

    PERFORM add_continuous_aggregate_policy('hourly_client_events',
      start_offset    => INTERVAL '3 days',
      end_offset      => INTERVAL '10 minutes',
      schedule_interval => INTERVAL '10 minutes');

    PERFORM add_retention_policy('hourly_client_events', drop_after => INTERVAL '15 days');
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_hourly_client_events_hour ON hourly_client_events (hour);

-- ---------------------------------------------------------------------------
-- client_snapshot_daily
--
-- One row per active install per day, with the JSONB already parsed into typed
-- columns. This is the workhorse for ranges up to ~90 days.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS client_snapshot_daily (
  day                date    NOT NULL,
  client_id          text    NOT NULL,
  version            text    NOT NULL,
  auth_provider      text    NOT NULL,
  browser            text    NOT NULL,
  os                 text    NOT NULL,
  clients            integer NOT NULL,
  running_containers integer,
  size_bucket        smallint NOT NULL,
  has_actions        boolean NOT NULL,
  has_hostname       boolean NOT NULL,
  has_custom_address boolean NOT NULL,
  has_custom_base    boolean NOT NULL,
  is_swarm           boolean NOT NULL,
  has_auth           boolean NOT NULL,
  multi_client       boolean NOT NULL,
  feature_count      smallint NOT NULL,
  PRIMARY KEY (day, client_id)
);

SELECT create_hypertable('client_snapshot_daily', 'day',
         chunk_time_interval => INTERVAL '7 days', if_not_exists => TRUE, migrate_data => TRUE);

CREATE INDEX IF NOT EXISTS idx_snapshot_daily_client ON client_snapshot_daily (client_id, day DESC);

-- ---------------------------------------------------------------------------
-- client_snapshot_weekly
--
-- Same shape rolled up to ISO weeks (~7x smaller), plus how many days that week the
-- install actually reported. Serves the 90-day, 1-year and long custom ranges.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS client_snapshot_weekly (
  week               date    NOT NULL,
  client_id          text    NOT NULL,
  version            text    NOT NULL,
  auth_provider      text    NOT NULL,
  browser            text    NOT NULL,
  os                 text    NOT NULL,
  clients            integer NOT NULL,
  peak_clients       integer NOT NULL,
  running_containers integer,
  size_bucket        smallint NOT NULL,
  has_actions        boolean NOT NULL,
  has_hostname       boolean NOT NULL,
  has_custom_address boolean NOT NULL,
  has_custom_base    boolean NOT NULL,
  is_swarm           boolean NOT NULL,
  has_auth           boolean NOT NULL,
  multi_client       boolean NOT NULL,
  feature_count      smallint NOT NULL,
  active_days        smallint NOT NULL,
  -- How long the install had existed by that week: 0 under a week, 1 under 4 weeks,
  -- 2 under 90 days, 3 under a year, 4 beyond. Materialised here so the tenure chart is
  -- a group-by rather than a 1.6M x 1.7M join on every page load.
  tenure_band        smallint NOT NULL,
  PRIMARY KEY (week, client_id)
);

SELECT create_hypertable('client_snapshot_weekly', 'week',
         chunk_time_interval => INTERVAL '90 days', if_not_exists => TRUE, migrate_data => TRUE);

CREATE INDEX IF NOT EXISTS idx_snapshot_weekly_client ON client_snapshot_weekly (client_id, week DESC);

-- ---------------------------------------------------------------------------
-- client_lifecycle
--
-- A row per install ever seen. Collapses the `min(day) GROUP BY client_id` that every
-- cohort query in the Grafana dashboards recomputed from scratch over the whole
-- aggregate. ~2M rows, and most panels only touch the ~380k with ever_active.
--
-- activated = first `events` beacon, never the first `start`. `start` comes from ~4x
-- more installs than `events`; seeding cohorts from starts while measuring activity
-- from events inflates every denominator and fakes a retention cliff.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS client_lifecycle (
  client_id        text PRIMARY KEY,
  first_event_day  date,
  last_event_day   date,
  first_event_week date,
  first_start_day  date,
  last_start_day   date,
  ever_active      boolean NOT NULL DEFAULT false
);

CREATE INDEX IF NOT EXISTS idx_lifecycle_first_event  ON client_lifecycle (first_event_day) WHERE ever_active;
CREATE INDEX IF NOT EXISTS idx_lifecycle_last_event   ON client_lifecycle (last_event_day)  WHERE ever_active;
CREATE INDEX IF NOT EXISTS idx_lifecycle_cohort       ON client_lifecycle (first_event_week) WHERE ever_active;
CREATE INDEX IF NOT EXISTS idx_lifecycle_first_start  ON client_lifecycle (first_start_day);

-- ---------------------------------------------------------------------------
-- client_latest
--
-- One row per install that has ever been active, holding its most recent reported
-- state. Every "of everyone using Dozzle right now, how many have X" panel reads this.
--
-- Those panels used to be a DISTINCT ON (client_id) over the whole snapshot for the
-- range - a sort of millions of rows per panel per request. Because every preset range
-- ends today, "installs active since <from>" is exactly `last_event_day >= from`, and
-- their latest state in the range is their latest state full stop. So the sort is done
-- once here and the panels become one indexed scan of ~230k rows.
--
-- A custom range that ends in the past is not the same question, and the API falls back
-- to the DISTINCT ON over the snapshot for those.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS client_latest (
  client_id          text PRIMARY KEY,
  last_event_day     date    NOT NULL,
  version            text    NOT NULL,
  auth_provider      text    NOT NULL,
  browser            text    NOT NULL,
  os                 text    NOT NULL,
  clients            integer NOT NULL,
  running_containers integer,
  size_bucket        smallint NOT NULL,
  has_actions        boolean NOT NULL,
  has_hostname       boolean NOT NULL,
  has_custom_address boolean NOT NULL,
  has_custom_base    boolean NOT NULL,
  is_swarm           boolean NOT NULL,
  has_auth           boolean NOT NULL,
  multi_client       boolean NOT NULL,
  feature_count      smallint NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_client_latest_last_event ON client_latest (last_event_day);

-- ---------------------------------------------------------------------------
-- active_counts_daily
--
-- A row per calendar day. DAU/WAU/MAU are trailing-window distinct counts, which are
-- the single most expensive thing on the old dashboards (the Grafana version ran a
-- six-CTE interval-merge per page load). Computed once here, read as a range scan.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS active_counts_daily (
  day             date PRIMARY KEY,
  dau             integer NOT NULL DEFAULT 0,
  wau             integer NOT NULL DEFAULT 0,
  mau             integer NOT NULL DEFAULT 0,
  new_installs    integer NOT NULL DEFAULT 0,
  resurrected     integer NOT NULL DEFAULT 0,
  churned         integer NOT NULL DEFAULT 0,
  first_launches  integer NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS active_counts_hourly (
  hour            timestamptz PRIMARY KEY,
  active_installs integer NOT NULL DEFAULT 0,
  beacons         bigint  NOT NULL DEFAULT 0
);

-- ---------------------------------------------------------------------------
-- cohort_retention_weekly
--
-- The full cohort grid, precomputed. cohort_week x week_index, so the retention table,
-- the average retention curve and the W1/W4/W12 trend lines are all one small scan.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS cohort_retention_weekly (
  cohort_week date     NOT NULL,
  week_index  smallint NOT NULL,
  cohort_size integer  NOT NULL,
  actives     integer  NOT NULL,
  PRIMARY KEY (cohort_week, week_index)
);

-- ---------------------------------------------------------------------------
-- weekly_lifecycle
--
-- New / retained / resurrected / churned installs per week.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS weekly_lifecycle (
  week        date PRIMARY KEY,
  active      integer NOT NULL DEFAULT 0,
  new_installs integer NOT NULL DEFAULT 0,
  retained    integer NOT NULL DEFAULT 0,
  resurrected integer NOT NULL DEFAULT 0,
  churned     integer NOT NULL DEFAULT 0
);


-- ---------------------------------------------------------------------------
-- Compression. Snapshot chunks older than 90 days are never rewritten by an
-- incremental refresh, so they can compress. A full rebuild TRUNCATEs, which works on
-- compressed hypertables.
--
-- No segmentby. The obvious choice, compress_segmentby = 'client_id', makes this table
-- BIGGER: there is at most one row per install per day and the chunk interval is 7 days,
-- so segmenting by client_id yields one compressed batch per install holding <= 7 values,
-- which is far too short to compress and carries per-batch overhead instead. Measured on
-- one real chunk: 504 kB -> 1128 kB segmented by client_id (TimescaleDB itself emits
-- "poor compression ratio detected"), against 560 kB -> 112 kB with no segmentby and
-- ordering by (day, client_id). Segmenting would only pay with a chunk interval long
-- enough to give each install a few hundred rows per batch.
--
-- Run unguarded: ALTER ... SET is idempotent, and gating it on compression_settings
-- already existing would pin a database to whatever settings it was first created with.
-- Changing the settings does NOT rewrite existing chunks - to pick up a change on a
-- database that already has compressed chunks, recompress them:
--   SELECT compress_chunk(c, recompress => true)
--     FROM show_chunks('client_snapshot_daily', older_than => INTERVAL '90 days') c;
-- ---------------------------------------------------------------------------

ALTER TABLE client_snapshot_daily
  SET (timescaledb.compress,
       timescaledb.compress_segmentby = '',
       timescaledb.compress_orderby = 'day, client_id');

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM timescaledb_information.jobs
                 WHERE proc_name = 'policy_compression'
                   AND hypertable_name = 'client_snapshot_daily') THEN
    PERFORM add_compression_policy('client_snapshot_daily', INTERVAL '90 days');
  END IF;
END $$;
