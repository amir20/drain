-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- Create basic schema
CREATE TABLE IF NOT EXISTS beacon (
    time TIMESTAMPTZ NOT NULL,
    name TEXT NOT NULL,
    client_id TEXT NOT NULL,
    metadata JSONB
);

-- Convert to hypertable
SELECT
    create_hypertable ('beacon', 'time');

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_beacon_client_time ON beacon (client_id, time DESC);

CREATE INDEX idx_name_client_time ON beacon (name, client_id, time);

CREATE INDEX idx_metadata ON beacon USING GIN (metadata);

CREATE INDEX idx_beacon_time_name_client ON beacon (time, name, client_id);

-- Setup compression
ALTER TABLE beacon
SET
    (
        timescaledb.compress,
        timescaledb.compress_segmentby = 'client_id'
    );

-- Add compression policy
SELECT
    add_compression_policy ('beacon', INTERVAL '7 days');

-- Materialized views
CREATE MATERIALIZED VIEW mv_client_activations AS
SELECT
    client_id,
    MIN(time) as activation_date
FROM
    beacon
WHERE
    client_id IS NOT NULL
    AND name = 'start'
GROUP BY
    1
ORDER BY
    2;

CREATE MATERIALIZED VIEW daily_client_events
WITH
    (timescaledb.continuous) AS
SELECT
    time_bucket ('1 day', time) as day,
    client_id,
    (array_agg(metadata ORDER BY time DESC))[1] as last_metadata,
    max(time) as last_seen
FROM
    beacon
WHERE
    name = 'events'
GROUP BY
    1,
    2;

SELECT add_continuous_aggregate_policy('daily_client_events',
    start_offset => INTERVAL '1 year',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '30 minutes'
);

CREATE INDEX idx_daily_client_events_day
ON daily_client_events(day);

CREATE INDEX idx_daily_client_events_client
ON daily_client_events(client_id, day);

CREATE INDEX idx_daily_client_events_metadata
ON daily_client_events USING GIN (last_metadata);

CREATE MATERIALIZED VIEW daily_client_starts
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 day', time) as day,
    client_id,
    (array_agg(metadata ORDER BY time DESC))[1] as last_metadata,
    max(time) as last_seen
FROM beacon
WHERE name = 'start'
GROUP BY 1, 2;

-- Add refresh policy
SELECT add_continuous_aggregate_policy('daily_client_starts',
    start_offset => INTERVAL '1 year',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '30 minutes'
);

CREATE INDEX idx_daily_client_starts_day
ON daily_client_starts(day);

CREATE INDEX idx_daily_client_starts_client
ON daily_client_starts(client_id, day);

CREATE INDEX idx_daily_client_starts_metadata
ON daily_client_starts USING GIN (last_metadata);
