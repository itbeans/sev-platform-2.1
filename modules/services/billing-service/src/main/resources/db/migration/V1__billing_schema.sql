-- ev-billing PostgreSQL schema
-- Run via Flyway or manually before first start.
--
-- All amounts stored in minor currency units (cents).
-- Timestamps stored as epoch milliseconds (BIGINT) for JVM Instant compatibility.

-- ── Billing users (EV user → Stripe customer mapping) ─────────────────────

CREATE TABLE IF NOT EXISTS billing_users (
  user_id                   VARCHAR(255) NOT NULL,
  tenant_id                 VARCHAR(255) NOT NULL,
  customer_id               VARCHAR(255) NOT NULL,    -- Stripe cus_...
  default_payment_method_id VARCHAR(255),
  created_on                BIGINT       NOT NULL,    -- epoch ms
  PRIMARY KEY (tenant_id, user_id)
);

CREATE INDEX IF NOT EXISTS idx_billing_users_customer ON billing_users(customer_id);

-- ── Invoices ──────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS billing_invoices (
  id                VARCHAR(255) NOT NULL,
  tenant_id         VARCHAR(255) NOT NULL,
  invoice_id        VARCHAR(255) NOT NULL,    -- Stripe inv_...
  invoice_number    VARCHAR(255),             -- Stripe human-readable (set after finalize)
  status            VARCHAR(32)  NOT NULL DEFAULT 'draft',
  amount_cents      INT          NOT NULL DEFAULT 0,
  amount_paid_cents INT          NOT NULL DEFAULT 0,
  currency          VARCHAR(8)   NOT NULL,
  user_id           VARCHAR(255) NOT NULL,
  customer_id       VARCHAR(255) NOT NULL,    -- Stripe cus_...
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

-- ── Billing accounts (CPO connected Stripe accounts) ─────────────────────

CREATE TABLE IF NOT EXISTS billing_accounts (
  id                      VARCHAR(255) NOT NULL,
  tenant_id               VARCHAR(255) NOT NULL,
  business_owner_user_id  VARCHAR(255) NOT NULL,
  company_name            VARCHAR(512) NOT NULL,
  status                  VARCHAR(32)  NOT NULL DEFAULT 'idle',
  account_external_id     VARCHAR(255),    -- Stripe acct_...
  activation_link         TEXT,
  created_on              BIGINT       NOT NULL,
  created_by              VARCHAR(255) NOT NULL,
  PRIMARY KEY (id)
);

CREATE INDEX IF NOT EXISTS idx_billing_accounts_tenant ON billing_accounts(tenant_id);

-- ── Billing transfers (fund dispatch records) ─────────────────────────────

CREATE TABLE IF NOT EXISTS billing_transfers (
  id                    VARCHAR(255) NOT NULL,
  tenant_id             VARCHAR(255) NOT NULL,
  account_id            VARCHAR(255) NOT NULL,
  account_external_id   VARCHAR(255) NOT NULL,    -- Stripe acct_...
  status                VARCHAR(32)  NOT NULL DEFAULT 'draft',
  currency              VARCHAR(8)   NOT NULL,
  session_counter       INT          NOT NULL DEFAULT 0,
  collected_funds_cents INT          NOT NULL DEFAULT 0,
  collected_fees_cents  INT          NOT NULL DEFAULT 0,
  transfer_amount_cents INT          NOT NULL DEFAULT 0,
  transfer_external_id  VARCHAR(255),             -- Stripe tr_...
  created_on            BIGINT       NOT NULL,
  last_changed_on       BIGINT       NOT NULL,
  PRIMARY KEY (id)
);

CREATE INDEX IF NOT EXISTS idx_billing_transfers_account ON billing_transfers(tenant_id, account_id, currency, status);
