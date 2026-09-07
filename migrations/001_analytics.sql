-- 001_analytics.sql
--
-- Derived analytics layer for the Nuxt dashboard.
--
-- Design rule: NOTHING here mirrors the beacon payload. The metadata JSONB is carried
-- through verbatim and parsed at query time by the helper functions below. That means
-- adding a field to the beacon - a new feature flag, a new setting - needs no migration,
-- no column, and no rebuild: it is already in the JSONB, and a new chart just reads it.
--
-- The cost is parsing JSONB on read instead of once on write, which is a few hundred
-- milliseconds on the widest ranges. That is the deliberate trade: a slower query is
-- cheap, a schema change that has to be threaded through five tables is not.
--
-- What IS materialised is only what cannot be derived cheaply on demand:
--   * a weekly rollup, because the alternative is DISTINCT ON over ~10M rows per panel
--   * one row per install, because cohorts need every install's first and last day
--   * counts whose shape never changes (DAU/WAU/MAU, cohorts, weekly lifecycle)
--
-- Idempotent: safe to run on every deploy. Statements are applied one at a time rather
-- than in a transaction, because TimescaleDB refuses to create a continuous aggregate
-- inside a transaction block; everything is guarded, so a run that dies half way is
-- finished by the next one.

-- ---------------------------------------------------------------------------
-- Reading the beacon payload.
--
-- These are the only place that knows what a metadata field is called or how it is
-- shaped. IMMUTABLE so they can be used in indexes and generated columns.
-- ---------------------------------------------------------------------------

-- Booleans arrive as real JSON booleans, but a field that predates a release is simply
-- absent - which is 'off', not 'unknown'.
CREATE OR REPLACE FUNCTION drain_flag(m jsonb, field text) RETURNS boolean
  LANGUAGE sql IMMUTABLE PARALLEL SAFE AS
$$ SELECT COALESCE((m ->> field)::boolean, false) $$;

CREATE OR REPLACE FUNCTION drain_int(m jsonb, field text) RETURNS integer
  LANGUAGE sql IMMUTABLE PARALLEL SAFE AS
$$ SELECT (m ->> field)::integer $$;

-- Is a named feature switched on for this install?
--
-- Two features are not plain flags - authentication is "a provider other than none", and
-- multiple browsers is a count - so they are named here. Everything else falls through to
-- the flag, which means adding a boolean feature to the dashboard is one entry in the
-- FEATURES list in the API and no SQL change at all.
CREATE OR REPLACE FUNCTION drain_feature(m jsonb, feature text) RETURNS boolean
  LANGUAGE sql IMMUTABLE PARALLEL SAFE AS
$$ SELECT CASE feature
     WHEN 'auth'         THEN COALESCE(NULLIF(m ->> 'authProvider', ''), 'none') <> 'none'
     WHEN 'multiClient'  THEN COALESCE((m ->> 'clients')::int, 0) > 1
     ELSE COALESCE((m ->> feature)::boolean, false)
   END $$;

CREATE OR REPLACE FUNCTION drain_auth_provider(m jsonb) RETURNS text
  LANGUAGE sql IMMUTABLE PARALLEL SAFE AS
$$ SELECT COALESCE(NULLIF(m ->> 'authProvider', ''), 'none') $$;

-- Patch releases are noise: a year holds ~554 distinct versions but only a couple of
-- dozen minors.
CREATE OR REPLACE FUNCTION drain_minor_version(m jsonb) RETURNS text
  LANGUAGE sql IMMUTABLE PARALLEL SAFE AS
$$ SELECT COALESCE(NULLIF(substring(m ->> 'version' from '^v?[0-9]+[.][0-9]+'), ''), 'unknown') $$;

CREATE OR REPLACE FUNCTION drain_browser(m jsonb) RETURNS text
  LANGUAGE sql IMMUTABLE PARALLEL SAFE AS
$$ SELECT CASE
     WHEN ua IS NULL OR ua = ''                 THEN 'Unknown'
     WHEN ua LIKE '%Edg/%' OR ua LIKE '%Edge/%' THEN 'Edge'
     WHEN ua LIKE '%OPR/%' OR ua LIKE '%Opera%' THEN 'Opera'
     WHEN ua LIKE '%Firefox/%'                  THEN 'Firefox'
     WHEN ua LIKE '%Chrome/%'                   THEN 'Chrome'
     WHEN ua LIKE '%Safari/%'                   THEN 'Safari'
     ELSE 'Other' END
   FROM (SELECT m ->> 'browser') AS x(ua) $$;

CREATE OR REPLACE FUNCTION drain_os(m jsonb) RETURNS text
  LANGUAGE sql IMMUTABLE PARALLEL SAFE AS
$$ SELECT CASE
     WHEN ua IS NULL OR ua = ''  THEN 'Unknown'
     WHEN ua LIKE '%Windows%'    THEN 'Windows'
     WHEN ua LIKE '%Android%'    THEN 'Android'
     WHEN ua LIKE '%iPhone%' OR ua LIKE '%iPad%' OR ua LIKE '%iPod%'    THEN 'iOS'
     WHEN ua LIKE '%Mac OS X%' OR ua LIKE '%Macintosh%'                 THEN 'macOS'
     WHEN ua LIKE '%Linux%' OR ua LIKE '%X11%'                          THEN 'Linux'
     ELSE 'Other' END
   FROM (SELECT m ->> 'browser') AS x(ua) $$;

-- Ordinal, so the UI can sort without label prefixes like '3 - 6 to 20'. Five bands plus
-- an unknown: an ordered scale is drawn with a one-hue ramp, and a ramp holds about five
-- steps before adjacent shades stop reading as distinct.
CREATE OR REPLACE FUNCTION drain_size_bucket(m jsonb) RETURNS smallint
  LANGUAGE sql IMMUTABLE PARALLEL SAFE AS
$$ SELECT CASE
     WHEN c IS NULL THEN 5 WHEN c = 0 THEN 0 WHEN c <= 5 THEN 1
     WHEN c <= 20 THEN 2 WHEN c <= 200 THEN 3
     ELSE 4 END::smallint
   FROM (SELECT (m ->> 'runningContainers')::int) AS x(c) $$;

-- ---------------------------------------------------------------------------
-- Views over the continuous aggregates.
--
-- Beacons with no ServerID all land on the empty key and would roll up into a single
-- phantom install reporting every minute; excluding them is not optional, so it lives
-- here rather than in every query. These also normalise the bucket to a plain date.
-- ---------------------------------------------------------------------------

CREATE OR REPLACE VIEW client_daily AS
SELECT day::date AS day, client_id, last_metadata AS metadata
FROM daily_client_events
WHERE client_id <> '';

CREATE OR REPLACE VIEW client_starts AS
SELECT day::date AS day, client_id
FROM daily_client_starts
WHERE client_id <> '';

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
-- Only exists so the 1-day and 7-day ranges can be charted at hour granularity. A year
-- of hourly per-client rows would be ~24x the daily aggregate, so it carries a short
-- retention; what the dashboard serves is active_counts_hourly, a row per hour that
-- accumulates from it before it is dropped.
-- ---------------------------------------------------------------------------

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM timescaledb_information.continuous_aggregates
                 WHERE view_name = 'hourly_client_events') THEN
    CREATE MATERIALIZED VIEW hourly_client_events
    WITH (timescaledb.continuous) AS
    SELECT time_bucket('1 hour', time) AS hour, client_id, count(*) AS beacons
    FROM beacon WHERE name = 'events'
    GROUP BY 1, 2
    WITH NO DATA;

    PERFORM add_continuous_aggregate_policy('hourly_client_events',
      start_offset      => INTERVAL '3 days',
      end_offset        => INTERVAL '10 minutes',
      schedule_interval => INTERVAL '10 minutes');

    PERFORM add_retention_policy('hourly_client_events', drop_after => INTERVAL '15 days');
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_hourly_client_events_hour ON hourly_client_events (hour);

-- ---------------------------------------------------------------------------
-- client_lifecycle: one row per install ever seen.
--
-- Collapses the `min(day) GROUP BY client_id` that every cohort query would otherwise
-- recompute over the whole aggregate, and carries the install's most recent metadata so
-- the "what is everyone running right now" panels are one indexed scan.
--
-- `activated` is the first `events` beacon, never the first `start`. `start` comes from
-- ~4x more installs; seeding cohorts from starts while measuring activity from events
-- inflates every denominator and fakes a retention cliff.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS client_lifecycle (
  client_id        text PRIMARY KEY,
  first_event_day  date,
  last_event_day   date,
  first_event_week date,
  first_start_day  date,
  last_start_day   date,
  ever_active      boolean NOT NULL DEFAULT false,
  metadata         jsonb
);

-- CREATE TABLE IF NOT EXISTS does nothing to a table that already exists, so a column
-- added to this file after it has been applied somewhere needs its own ALTER. Adding one
-- here is the exception, not the pattern: a new *beacon* field belongs in the metadata
-- JSONB and needs no column at all.
ALTER TABLE client_lifecycle ADD COLUMN IF NOT EXISTS metadata jsonb;

CREATE INDEX IF NOT EXISTS idx_lifecycle_first_event ON client_lifecycle (first_event_day) WHERE ever_active;
CREATE INDEX IF NOT EXISTS idx_lifecycle_last_event  ON client_lifecycle (last_event_day)  WHERE ever_active;
CREATE INDEX IF NOT EXISTS idx_lifecycle_cohort      ON client_lifecycle (first_event_week) WHERE ever_active;
CREATE INDEX IF NOT EXISTS idx_lifecycle_first_start ON client_lifecycle (first_start_day);

-- ---------------------------------------------------------------------------
-- client_weekly: the daily aggregate rolled up to ISO weeks, ~7x smaller.
--
-- The one materialised rollup, and the reason a one-year range is in the same latency
-- class as a 30-day one. `metadata` is the install's last beacon that week, so every
-- dimension panel reads the same shape as client_daily.
--
-- `first_event_day` is denormalised here only so the tenure chart does not have to join
-- 1.6M rows to 1.7M on every request. The tenure *bands* deliberately are not stored -
-- they live in the API, where changing them is an edit rather than a migration.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS client_weekly (
  week            date NOT NULL,
  client_id       text NOT NULL,
  metadata        jsonb,
  active_days     smallint NOT NULL,
  peak_clients    integer,
  first_event_day date,
  PRIMARY KEY (week, client_id)
);

CREATE INDEX IF NOT EXISTS idx_client_weekly_week ON client_weekly (week);

-- ---------------------------------------------------------------------------
-- Fixed-shape counts. None of these ever gain a column when the beacon does.
-- ---------------------------------------------------------------------------

-- DAU/WAU/MAU are trailing-window distinct counts, the single most expensive thing on
-- the old dashboards. Computed once per refresh; read as a range scan of ~365 rows.
CREATE TABLE IF NOT EXISTS active_counts_daily (
  day            date PRIMARY KEY,
  dau            integer NOT NULL DEFAULT 0,
  wau            integer NOT NULL DEFAULT 0,
  mau            integer NOT NULL DEFAULT 0,
  new_installs   integer NOT NULL DEFAULT 0,
  resurrected    integer NOT NULL DEFAULT 0,
  churned        integer NOT NULL DEFAULT 0,
  first_launches integer NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS active_counts_hourly (
  hour            timestamptz PRIMARY KEY,
  active_installs integer NOT NULL DEFAULT 0,
  beacons         bigint  NOT NULL DEFAULT 0
);

-- The full cohort grid, so the retention table, the average curve and the W1/W4/W12
-- trend are all one small scan.
CREATE TABLE IF NOT EXISTS cohort_retention_weekly (
  cohort_week date     NOT NULL,
  week_index  smallint NOT NULL,
  cohort_size integer  NOT NULL,
  actives     integer  NOT NULL,
  PRIMARY KEY (cohort_week, week_index)
);

CREATE TABLE IF NOT EXISTS weekly_lifecycle (
  week         date PRIMARY KEY,
  active       integer NOT NULL DEFAULT 0,
  new_installs integer NOT NULL DEFAULT 0,
  retained     integer NOT NULL DEFAULT 0,
  resurrected  integer NOT NULL DEFAULT 0,
  churned      integer NOT NULL DEFAULT 0
);

-- ---------------------------------------------------------------------------
-- Superseded by the design above: typed mirrors of the beacon payload, which had to be
-- edited in lockstep with it. Dropped rather than left behind to go stale.
-- ---------------------------------------------------------------------------

DROP TABLE IF EXISTS client_snapshot_daily;
DROP TABLE IF EXISTS client_snapshot_weekly;
DROP TABLE IF EXISTS client_latest;

DELETE FROM analytics_meta
WHERE key IN ('phase_snapshot_daily', 'phase_snapshot_weekly', 'phase_client_latest');
