-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- Consumptions hypertable (replaces MongoDB consumptions collection)
CREATE TABLE IF NOT EXISTS consumptions (
  id                  BIGSERIAL,
  tenant_id           TEXT NOT NULL,
  transaction_id      BIGINT,
  charging_station_id TEXT NOT NULL,
  connector_id        INT NOT NULL,
  started_at          TIMESTAMPTZ NOT NULL,
  ended_at            TIMESTAMPTZ NOT NULL,
  consumption_wh      DOUBLE PRECISION NOT NULL,
  instant_watts       DOUBLE PRECISION,
  state_of_charge     INT,
  cumulated_price     DOUBLE PRECISION,
  pricing_source      TEXT
);
SELECT create_hypertable('consumptions', 'started_at', if_not_exists => TRUE);

-- Audit log hypertable (replaces MongoDB logs collection)
CREATE TABLE IF NOT EXISTS audit_logs (
  id              BIGSERIAL,
  tenant_id       TEXT NOT NULL,
  timestamp       TIMESTAMPTZ NOT NULL,
  action          TEXT NOT NULL,
  source          TEXT,
  user_id         TEXT,
  station_id      TEXT,
  level           TEXT NOT NULL,
  message         TEXT NOT NULL,
  details         JSONB
);
SELECT create_hypertable('audit_logs', 'timestamp', if_not_exists => TRUE);

-- Retention policy: keep 2 years of consumptions, 1 year of logs
SELECT add_retention_policy('consumptions', INTERVAL '2 years', if_not_exists => TRUE);
SELECT add_retention_policy('audit_logs', INTERVAL '1 year', if_not_exists => TRUE);
