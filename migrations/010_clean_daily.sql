-- client_daily without noise installs (migrations/009), for the dashboard.
--
-- Ranges up to 31 days read the daily source directly instead of client_weekly, which
-- the refresh already filters, so without this a 30-day page still counted the burst
-- installs - about half of all 30-day actives. The refresh keeps reading client_daily:
-- it has to see every install to decide which ones are noise.
--
-- A plain view, so the bucket_ts range predicate still reaches the continuous aggregate
-- and keeps its chunk exclusion (migrations/004). The anti-join made a 30-day distinct
-- count faster on production, not slower: 607ms against 773ms.
CREATE OR REPLACE VIEW client_daily_clean AS
SELECT d.day, d.client_id, d.metadata, d.bucket_ts
FROM client_daily d
WHERE NOT EXISTS (SELECT 1 FROM client_lifecycle l WHERE l.client_id = d.client_id AND l.noise);
