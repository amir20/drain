-- 004_sargable_buckets.sql
--
-- Expose each aggregate's raw timestamptz bucket so a range filter can use the index.
--
-- `client_daily` and `client_starts` present the bucket as `day::date`. A cast on the
-- indexed column is not sargable, so TimescaleDB cannot exclude chunks and cannot use
-- `idx_daily_client_events_day`: every 30-day query decompressed and discarded the whole
-- history instead. Measured on production for one 30-day GROUP BY:
--
--     via day::date   chunks excluded 0 of 13, 1,270,764 buffers, 4058 ms
--     via the raw day 1 chunk by Index Cond,      26,874 buffers,  352 ms
--
-- The date column stays exactly as it is - it is what everything groups by and returns,
-- and changing its type would rewrite every consumer. `bucket_ts` is only for the WHERE
-- clause: filter on it, keep grouping on `day`.
--
-- CREATE OR REPLACE VIEW can only append columns, never reorder or retype the existing
-- ones, which is why this is additive rather than a redefinition.

CREATE OR REPLACE VIEW client_daily AS
SELECT day::date AS day, client_id, last_metadata AS metadata, day AS bucket_ts
FROM daily_client_events
WHERE client_id <> '';

CREATE OR REPLACE VIEW client_starts AS
SELECT day::date AS day, client_id, day AS bucket_ts
FROM daily_client_starts
WHERE client_id <> '';
