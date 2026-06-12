-- Local-dev PostgreSQL/TimescaleDB bootstrap.
-- Mirrors the canonical service schemas:
--   modules/services/analytics/src/main/resources/db/migration/V1__analytics_schema.sql
--   modules/services/billing-service/src/main/resources/db/migration/V1__billing_schema.sql
-- Keep this file in sync with those migrations.

CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- ── ev-analytics: consumptions hypertable ──────────────────────────────────

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

SELECT create_hypertable(
  'ev_consumptions', 'time',
  chunk_time_interval => INTERVAL '1 week',
  if_not_exists       => TRUE
);

CREATE INDEX IF NOT EXISTS idx_ev_consumptions_tenant_station
  ON ev_consumptions (tenant_id, charging_station_id, time DESC);

CREATE INDEX IF NOT EXISTS idx_ev_consumptions_tenant_site_area
  ON ev_consumptions (tenant_id, site_area_id, time DESC);

CREATE INDEX IF NOT EXISTS idx_ev_consumptions_tenant_tx
  ON ev_consumptions (tenant_id, transaction_id, time DESC)
  WHERE transaction_id IS NOT NULL;

SELECT add_retention_policy('ev_consumptions', INTERVAL '2 years', if_not_exists => TRUE);

-- ── ev-analytics: logs hypertable ──────────────────────────────────────────

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

CREATE INDEX IF NOT EXISTS idx_ev_logs_message_trgm
  ON ev_logs USING gin (message gin_trgm_ops);

SELECT add_retention_policy('ev_logs', INTERVAL '90 days', if_not_exists => TRUE);

-- ── ev-billing-service tables ──────────────────────────────────────────────
-- Amounts in minor currency units (cents); timestamps as epoch ms (BIGINT).

CREATE TABLE IF NOT EXISTS billing_users (
  user_id                   VARCHAR(255) NOT NULL,
  tenant_id                 VARCHAR(255) NOT NULL,
  customer_id               VARCHAR(255) NOT NULL,
  default_payment_method_id VARCHAR(255),
  created_on                BIGINT       NOT NULL,
  PRIMARY KEY (tenant_id, user_id)
);

CREATE INDEX IF NOT EXISTS idx_billing_users_customer ON billing_users(customer_id);

CREATE TABLE IF NOT EXISTS billing_invoices (
  id                VARCHAR(255) NOT NULL,
  tenant_id         VARCHAR(255) NOT NULL,
  invoice_id        VARCHAR(255) NOT NULL,
  invoice_number    VARCHAR(255),
  status            VARCHAR(32)  NOT NULL DEFAULT 'draft',
  amount_cents      INT          NOT NULL DEFAULT 0,
  amount_paid_cents INT          NOT NULL DEFAULT 0,
  currency          VARCHAR(8)   NOT NULL,
  user_id           VARCHAR(255) NOT NULL,
  customer_id       VARCHAR(255) NOT NULL,
  live_mode         BOOLEAN      NOT NULL DEFAULT FALSE,
  sessions_json     TEXT         NOT NULL DEFAULT '[]',
  download_url      TEXT,
  pay_invoice_url   TEXT,
  last_error        TEXT,
  created_on        BIGINT       NOT NULL,
  last_changed_on   BIGINT       NOT NULL,
  PRIMARY KEY (id)
);

CREATE INDEX IF NOT EXISTS idx_billing_invoices_tenant_user   ON billing_invoices(tenant_id, user_id);
CREATE INDEX IF NOT EXISTS idx_billing_invoices_tenant_status ON billing_invoices(tenant_id, status);
CREATE INDEX IF NOT EXISTS idx_billing_invoices_invoice_id    ON billing_invoices(invoice_id);

CREATE TABLE IF NOT EXISTS billing_accounts (
  id                      VARCHAR(255) NOT NULL,
  tenant_id               VARCHAR(255) NOT NULL,
  business_owner_user_id  VARCHAR(255) NOT NULL,
  company_name            VARCHAR(512) NOT NULL,
  status                  VARCHAR(32)  NOT NULL DEFAULT 'idle',
  account_external_id     VARCHAR(255),
  activation_link         TEXT,
  created_on              BIGINT       NOT NULL,
  created_by              VARCHAR(255) NOT NULL,
  PRIMARY KEY (id)
);

CREATE INDEX IF NOT EXISTS idx_billing_accounts_tenant ON billing_accounts(tenant_id);

CREATE TABLE IF NOT EXISTS billing_transfers (
  id                    VARCHAR(255) NOT NULL,
  tenant_id             VARCHAR(255) NOT NULL,
  account_id            VARCHAR(255) NOT NULL,
  account_external_id   VARCHAR(255) NOT NULL,
  status                VARCHAR(32)  NOT NULL DEFAULT 'draft',
  currency              VARCHAR(8)   NOT NULL,
  session_counter       INT          NOT NULL DEFAULT 0,
  collected_funds_cents INT          NOT NULL DEFAULT 0,
  collected_fees_cents  INT          NOT NULL DEFAULT 0,
  transfer_amount_cents INT          NOT NULL DEFAULT 0,
  transfer_external_id  VARCHAR(255),
  created_on            BIGINT       NOT NULL,
  last_changed_on       BIGINT       NOT NULL,
  PRIMARY KEY (id)
);

CREATE INDEX IF NOT EXISTS idx_billing_transfers_account ON billing_transfers(tenant_id, account_id, currency, status);
