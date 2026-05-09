-- ev-analytics TimescaleDB schema
-- Run with: sbt flywayMigrate (or apply manually against the TimescaleDB instance)

-- ── Consumption hypertable ────────────────────────────────────────────────
-- One row per meter-value reading from ev-ocpp-processor.
-- Partitioned by time for efficient range queries.

CREATE TABLE IF NOT EXISTS ev_consumptions (
  tenant_id           TEXT          NOT NULL,
  time                TIMESTAMPTZ   NOT NULL,
  charging_station_id TEXT          NOT NULL,
  connector_id        INT           NOT NULL,
  site_area_id        TEXT,
  site_id             TEXT,
  user_id             TEXT,
  transaction_id      BIGINT,
  instant_watts       DOUBLE PRECISION NOT NULL DEFAULT 0,
  cumulated_kwh       DOUBLE PRECISION NOT NULL DEFAULT 0
);

-- Convert to hypertable (1-week chunks — tune based on write volume)
SELECT create_hypertable(
  'ev_consumptions', 'time',
  chunk_time_interval => INTERVAL '1 week',
  if_not_exists       => TRUE
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_ev_consumptions_tenant_station
  ON ev_consumptions (tenant_id, charging_station_id, time DESC);

CREATE INDEX IF NOT EXISTS idx_ev_consumptions_tenant_site_area
  ON ev_consumptions (tenant_id, site_area_id, time DESC);

CREATE INDEX IF NOT EXISTS idx_ev_consumptions_tenant_tx
  ON ev_consumptions (tenant_id, transaction_id, time DESC)
  WHERE transaction_id IS NOT NULL;

-- Compression policy: compress chunks older than 30 days
SELECT add_compression_policy(
  'ev_consumptions',
  compress_after => INTERVAL '30 days',
  if_not_exists  => TRUE
);

-- ── Logs hypertable ───────────────────────────────────────────────────────
-- One row per audit log event, partitioned by time.
-- Replaces the MongoDB logs + performances collections.

CREATE TABLE IF NOT EXISTS ev_logs (
  tenant_id           TEXT          NOT NULL,
  time                TIMESTAMPTZ   NOT NULL,
  level               CHAR(1)       NOT NULL,     -- 'D' | 'I' | 'W' | 'E'
  source              TEXT          NOT NULL,
  action              TEXT          NOT NULL,
  module              TEXT,
  method              TEXT,
  message             TEXT          NOT NULL,
  user_id             TEXT,
  charging_station_id TEXT,
  site_id             TEXT,
  site_area_id        TEXT,
  details             JSONB
);

SELECT create_hypertable(
  'ev_logs', 'time',
  chunk_time_interval => INTERVAL '1 day',
  if_not_exists       => TRUE
);

CREATE INDEX IF NOT EXISTS idx_ev_logs_tenant_time
  ON ev_logs (tenant_id, time DESC);

CREATE INDEX IF NOT EXISTS idx_ev_logs_level
  ON ev_logs (tenant_id, level, time DESC);

CREATE INDEX IF NOT EXISTS idx_ev_logs_action
  ON ev_logs (tenant_id, action, time DESC);

CREATE INDEX IF NOT EXISTS idx_ev_logs_station
  ON ev_logs (tenant_id, charging_station_id, time DESC)
  WHERE charging_station_id IS NOT NULL;

-- Full-text search index on message
CREATE INDEX IF NOT EXISTS idx_ev_logs_message_trgm
  ON ev_logs USING gin (message gin_trgm_ops);

-- Retention policy: drop chunks older than 90 days (configurable via cfg)
SELECT add_retention_policy(
  'ev_logs',
  drop_after    => INTERVAL '90 days',
  if_not_exists => TRUE
);
