-- 005_storage.sql
--
-- Disk. The database had grown to 25 GB on a 50 GB volume, with raw `beacon` retention
-- already at a year. The space was not in the retention window but in three things that
-- can be fixed without dropping data:
--
--   * the daily aggregates compressed only after 13 months, so 5.4 GB sat uncompressed;
--   * 1.5 GB of indexes that nothing reads;
--   * `beacon` compressed 4x where TimescaleDB usually manages 10x or better.
--
-- Idempotent: every change is guarded, so re-applying is a no-op.

-- ---------------------------------------------------------------------------
-- 1. Daily aggregates: refresh the last 30 days, compress after 90.
--
-- compress_after must exceed the refresh policy's start_offset, and start_offset was a
-- year - which is why compression waited 13 months. A year of refresh window bought
-- nothing: beacons arrive live, and refresh is invalidation-driven, so only late rows
-- ever cause work. It was also a hazard: a window as wide as `beacon` retention can reach
-- into ranges whose raw chunks were just dropped, and refreshing an empty range deletes
-- the aggregate rows that are supposed to outlive them.
--
-- 90 days keeps every dashboard read on uncompressed chunks: `client_daily` serves ranges
-- of up to 31 days (their previous period reaches 62), and the incremental refresh reads
-- 28 days back. Older ranges read `client_weekly`. Measured on production, a compressed
-- chunk of these aggregates is 6-10x smaller than an uncompressed one.
-- ---------------------------------------------------------------------------

DO $$
DECLARE
  v_view text;
  v_mat  text;
BEGIN
  FOREACH v_view IN ARRAY ARRAY['daily_client_events', 'daily_client_starts'] LOOP
    SELECT materialization_hypertable_name INTO v_mat
    FROM timescaledb_information.continuous_aggregates WHERE view_name = v_view;

    IF v_mat IS NULL THEN
      CONTINUE;
    END IF;

    -- Refresh first: the compression policy is checked against it.
    IF NOT EXISTS (SELECT 1 FROM timescaledb_information.jobs
                   WHERE hypertable_name = v_mat
                     AND proc_name = 'policy_refresh_continuous_aggregate'
                     AND (config ->> 'start_offset')::interval = INTERVAL '30 days') THEN
      PERFORM remove_continuous_aggregate_policy(v_view, if_exists => true);
      PERFORM add_continuous_aggregate_policy(v_view,
        start_offset      => INTERVAL '30 days',
        end_offset        => INTERVAL '1 hour',
        schedule_interval => INTERVAL '30 minutes');
    END IF;

    IF NOT EXISTS (SELECT 1 FROM timescaledb_information.jobs
                   WHERE hypertable_name = v_mat
                     AND proc_name = 'policy_compression'
                     AND (config ->> 'compress_after')::interval = INTERVAL '90 days') THEN
      PERFORM remove_compression_policy(v_view, if_exists => true);
      PERFORM add_compression_policy(v_view, compress_after => INTERVAL '90 days');
    END IF;
  END LOOP;
END $$;

-- ---------------------------------------------------------------------------
-- 2. Indexes nothing reads.
--
-- From pg_stat_user_indexes on production, summed over chunks:
--
--     idx_daily_client_starts_metadata   GIN   595 MB   0 scans
--     idx_daily_client_events_metadata   GIN   296 MB   0 scans
--     idx_metadata (beacon)              GIN   414 MB   0 scans
--     idx_beacon_client_time                   234 MB   1 scan
--
-- The metadata is only ever parsed by the drain_* helpers, never searched. And the two
-- (client_id, day) indexes duplicate the one TimescaleDB creates on every aggregate
-- (_materialized_hypertable_N_client_id_day_idx), which takes all the scans:
--
--     idx_daily_client_starts_client           317 MB
--     idx_daily_client_events_client           153 MB
-- ---------------------------------------------------------------------------

DROP INDEX IF EXISTS _timescaledb_internal.idx_daily_client_starts_metadata;
DROP INDEX IF EXISTS _timescaledb_internal.idx_daily_client_events_metadata;
DROP INDEX IF EXISTS _timescaledb_internal.idx_daily_client_starts_client;
DROP INDEX IF EXISTS _timescaledb_internal.idx_daily_client_events_client;
DROP INDEX IF EXISTS idx_metadata;
DROP INDEX IF EXISTS idx_beacon_client_time;

-- ---------------------------------------------------------------------------
-- 3. beacon: segment by name, order by client.
--
-- Segmenting by client_id split each weekly chunk into ~120k segments of ~11 rows, too
-- few for any column to compress and each paying its own per-segment overhead - 52.6 GB
-- became 12.7 GB, a third of that index. `name` has three values, so segments fill whole
-- 1000-row batches, and ordering by client keeps an install's near-identical payloads
-- adjacent. On a synthetic week shaped like production (1.3M rows, 120k installs):
--
--     segmentby client_id, orderby time DESC        332 MB
--     segmentby name,      orderby time DESC         74 MB
--     segmentby name,      orderby client_id, time   32 MB
--
-- Nothing reads compressed beacon chunks by client: the aggregates filter on name and
-- time, and time is already excluded per chunk.
--
-- This only applies to chunks compressed from now on. drain_recompress_beacon() below
-- rewrites the existing ones.
-- ---------------------------------------------------------------------------

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM timescaledb_information.compression_settings
                 WHERE hypertable_name = 'beacon' AND attname = 'name'
                   AND segmentby_column_index = 1) THEN
    ALTER TABLE beacon SET (timescaledb.compress_segmentby = 'name',
                            timescaledb.compress_orderby   = 'client_id, time DESC');
  END IF;
END $$;

-- Decompresses and recompresses every beacon chunk still on the old settings, newest
-- first (the oldest are the next to be dropped by retention anyway), committing after
-- each so it can be stopped and resumed.
--
--   CALL drain_recompress_beacon();      -- all of them
--   CALL drain_recompress_beacon(5);     -- at most five
--
-- Not run by the migration: it is hours of I/O, and a decompressed chunk briefly needs
-- ~1.5 GB of free disk. Writes to the chunk being rewritten wait for it, which for any
-- chunk but the current one means only late beacons.
CREATE OR REPLACE PROCEDURE drain_recompress_beacon(p_limit integer DEFAULT NULL)
LANGUAGE plpgsql AS $proc$
DECLARE
  v_chunk   regclass;
  v_started timestamptz;
  v_done    integer := 0;
BEGIN
  FOR v_chunk IN
    SELECT s.chunk
    FROM timescaledb_information.chunk_compression_settings s
    JOIN timescaledb_information.chunks c
      ON format('%I.%I', c.chunk_schema, c.chunk_name)::regclass = s.chunk
    WHERE s.hypertable = 'beacon'::regclass
      AND s.segmentby IS DISTINCT FROM 'name'
    ORDER BY c.range_start DESC
    LIMIT p_limit
  LOOP
    v_started := clock_timestamp();
    PERFORM decompress_chunk(v_chunk);
    PERFORM compress_chunk(v_chunk);
    COMMIT;
    v_done := v_done + 1;
    RAISE NOTICE 'recompressed % in %', v_chunk, clock_timestamp() - v_started;
  END LOOP;
  RAISE NOTICE 'recompressed % chunk(s)', v_done;
END;
$proc$;
