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

CREATE INDEX idx_metadata ON beacon USING GIN (metadata);

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
