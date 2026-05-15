#!/usr/bin/env bash
# migrate-billing.sh
#
# Migrates ev-server MongoDB billing collections (per-tenant) into the
# billing-service PostgreSQL database.
#
# Source  — MongoDB database "ev", collections per tenant:
#             {tenantId}.invoices
#             {tenantId}.billingaccounts
#             {tenantId}.billingtransfers
#             {tenantId}.billingusers
#
# Target  — PostgreSQL tables (schema: V1__billing_schema.sql):
#             billing_invoices
#             billing_accounts
#             billing_transfers
#             billing_users
#
# Prerequisites:
#   - mongosh  (≥ 2.0)
#   - psql     (≥ 15)
#   - jq       (≥ 1.6)
#
# Usage:
#   MONGO_URI=mongodb://localhost:27017 \
#   PG_URI=postgresql://ev:secret@localhost:5432/ev_billing \
#   ./migrate-billing.sh [--tenant TENANT_ID] [--dry-run] [--from 2024-01-01]
#
# Idempotent: all inserts use ON CONFLICT DO UPDATE / DO NOTHING, so the
# script can be re-run safely after partial failures.

set -euo pipefail

MONGO_URI="${MONGO_URI:-mongodb://localhost:27017}"
PG_URI="${PG_URI:-postgresql://ev:secret@localhost:5432/ev_billing}"
BATCH_SIZE=500
TENANT_FILTER=""
DRY_RUN=false
FROM_DATE="1970-01-01T00:00:00Z"

# ── Argument parsing ──────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --tenant)  TENANT_FILTER="$2"; shift 2 ;;
    --dry-run) DRY_RUN=true;        shift   ;;
    --from)    FROM_DATE="$2";      shift 2 ;;
    *) echo "Unknown argument: $1" >&2; exit 1 ;;
  esac
done

# ── Schema guard ──────────────────────────────────────────────────────────────
# Verify the target tables exist (created by Flyway on service startup).
# The migration does not create tables itself — run the billing-service once
# or apply V1__billing_schema.sql manually before running this script.
ensure_tables_exist() {
  local missing
  missing=$(psql "$PG_URI" -At -c "
    SELECT table_name FROM information_schema.tables
    WHERE table_schema = 'public'
      AND table_name IN ('billing_invoices','billing_accounts','billing_transfers','billing_users')
  " | sort | tr '\n' ' ')

  for t in billing_invoices billing_accounts billing_transfers billing_users; do
    if [[ "$missing" != *"$t"* ]]; then
      echo "ERROR: Table '$t' not found in target database." >&2
      echo "       Run the billing-service once (Flyway auto-applies V1__billing_schema.sql)" >&2
      echo "       or apply it manually: psql \$PG_URI -f V1__billing_schema.sql" >&2
      exit 1
    fi
  done
}

# ── Tenant discovery ──────────────────────────────────────────────────────────
get_tenants() {
  mongosh --quiet "$MONGO_URI" --eval '
    const cols = db.getSiblingDB("ev").listCollectionNames()
      .filter(n => n.endsWith(".invoices"))
      .map(n => n.replace(".invoices", ""));
    print(cols.join("\n"));
  '
}

# ── Invoice migration ─────────────────────────────────────────────────────────
# sessions (List[BillingSessionData]) serialised as JSON text in PostgreSQL.
# Timestamps (ISODate) converted to epoch-milliseconds (BIGINT) by mongosh.
migrate_invoices() {
  local tenant="$1"
  local collection="${tenant}.invoices"
  local skip=0
  local count=0

  echo "  invoices ..."
  while true; do
    local batch
    batch=$(mongosh --quiet "$MONGO_URI" --eval "
      const col = db.getSiblingDB('ev').getCollection('${collection}');
      const docs = col.find(
        { createdOn: { \\\$gte: ISODate('${FROM_DATE}') } }
      ).skip(${skip}).limit(${BATCH_SIZE}).toArray();
      // Flatten for safe CSV export: convert ISODate → epoch ms, sessions → JSON string
      const rows = docs.map(d => ({
        id:               d._id.toString(),
        tenantId:         '${tenant}',
        invoiceId:        d.invoiceId || '',
        invoiceNumber:    d.invoiceNumber || '',
        status:           d.status || 'draft',
        amountCents:      d.amountCents || 0,
        amountPaidCents:  d.amountPaidCents || 0,
        currency:         d.currency || '',
        userId:           d.userId || '',
        customerId:       d.customerId || '',
        liveMode:         d.liveMode ? true : false,
        sessionsJson:     JSON.stringify(d.sessions || []),
        downloadUrl:      d.downloadUrl || '',
        payInvoiceUrl:    d.payInvoiceUrl || '',
        lastError:        d.lastError || '',
        createdOn:        d.createdOn ? d.createdOn.getTime() : 0,
        lastChangedOn:    d.lastChangedOn ? d.lastChangedOn.getTime() : 0
      }));
      print(JSON.stringify(rows));
    ")

    local rows
    rows=$(echo "$batch" | jq -c '.[]' 2>/dev/null || true)
    [[ -z "$rows" ]] && break

    local row_count
    row_count=$(echo "$rows" | wc -l)

    if $DRY_RUN; then
      echo "    [DRY-RUN] invoices: would upsert ${row_count} rows (skip=${skip})"
    else
      echo "$rows" | jq -r '
        [
          .id,
          .tenantId,
          .invoiceId,
          (.invoiceNumber | if . == "" then null else . end),
          .status,
          (.amountCents | tostring),
          (.amountPaidCents | tostring),
          .currency,
          .userId,
          .customerId,
          (.liveMode | tostring),
          .sessionsJson,
          (.downloadUrl  | if . == "" then null else . end),
          (.payInvoiceUrl | if . == "" then null else . end),
          (.lastError    | if . == "" then null else . end),
          (.createdOn    | tostring),
          (.lastChangedOn | tostring)
        ] | @csv
      ' | psql "$PG_URI" -c "
        COPY billing_invoices (
          id, tenant_id, invoice_id, invoice_number, status,
          amount_cents, amount_paid_cents, currency,
          user_id, customer_id, live_mode, sessions_json,
          download_url, pay_invoice_url, last_error,
          created_on, last_changed_on
        )
        FROM STDIN WITH (FORMAT csv, NULL 'null')
        ON CONFLICT (id) DO UPDATE SET
          invoice_number    = EXCLUDED.invoice_number,
          status            = EXCLUDED.status,
          amount_cents      = EXCLUDED.amount_cents,
          amount_paid_cents = EXCLUDED.amount_paid_cents,
          sessions_json     = EXCLUDED.sessions_json,
          download_url      = EXCLUDED.download_url,
          pay_invoice_url   = EXCLUDED.pay_invoice_url,
          last_error        = EXCLUDED.last_error,
          last_changed_on   = EXCLUDED.last_changed_on;
      "
    fi

    count=$(( count + row_count ))
    skip=$(( skip + BATCH_SIZE ))
    [[ "$row_count" -lt "$BATCH_SIZE" ]] && break
  done
  echo "    invoices: ${count} rows"
}

# ── Billing account migration ─────────────────────────────────────────────────
migrate_accounts() {
  local tenant="$1"
  local collection="${tenant}.billingaccounts"
  local skip=0
  local count=0

  echo "  billing_accounts ..."
  while true; do
    local batch
    batch=$(mongosh --quiet "$MONGO_URI" --eval "
      const col = db.getSiblingDB('ev').getCollection('${collection}');
      const docs = col.find(
        { createdOn: { \\\$gte: ISODate('${FROM_DATE}') } }
      ).skip(${skip}).limit(${BATCH_SIZE}).toArray();
      const rows = docs.map(d => ({
        id:                   d._id.toString(),
        tenantId:             '${tenant}',
        businessOwnerUserId:  d.businessOwnerUserId || '',
        companyName:          d.companyName || '',
        status:               d.status || 'idle',
        accountExternalId:    d.accountExternalId || '',
        activationLink:       d.activationLink || '',
        createdOn:            d.createdOn ? d.createdOn.getTime() : 0,
        createdBy:            d.createdBy || ''
      }));
      print(JSON.stringify(rows));
    ")

    local rows
    rows=$(echo "$batch" | jq -c '.[]' 2>/dev/null || true)
    [[ -z "$rows" ]] && break

    local row_count
    row_count=$(echo "$rows" | wc -l)

    if $DRY_RUN; then
      echo "    [DRY-RUN] billing_accounts: would upsert ${row_count} rows (skip=${skip})"
    else
      echo "$rows" | jq -r '
        [
          .id,
          .tenantId,
          .businessOwnerUserId,
          .companyName,
          .status,
          (.accountExternalId | if . == "" then null else . end),
          (.activationLink    | if . == "" then null else . end),
          (.createdOn | tostring),
          .createdBy
        ] | @csv
      ' | psql "$PG_URI" -c "
        COPY billing_accounts (
          id, tenant_id, business_owner_user_id, company_name, status,
          account_external_id, activation_link, created_on, created_by
        )
        FROM STDIN WITH (FORMAT csv, NULL 'null')
        ON CONFLICT (id) DO UPDATE SET
          company_name        = EXCLUDED.company_name,
          status              = EXCLUDED.status,
          account_external_id = EXCLUDED.account_external_id,
          activation_link     = EXCLUDED.activation_link;
      "
    fi

    count=$(( count + row_count ))
    skip=$(( skip + BATCH_SIZE ))
    [[ "$row_count" -lt "$BATCH_SIZE" ]] && break
  done
  echo "    billing_accounts: ${count} rows"
}

# ── Billing transfer migration ────────────────────────────────────────────────
migrate_transfers() {
  local tenant="$1"
  local collection="${tenant}.billingtransfers"
  local skip=0
  local count=0

  echo "  billing_transfers ..."
  while true; do
    local batch
    batch=$(mongosh --quiet "$MONGO_URI" --eval "
      const col = db.getSiblingDB('ev').getCollection('${collection}');
      const docs = col.find(
        { createdOn: { \\\$gte: ISODate('${FROM_DATE}') } }
      ).skip(${skip}).limit(${BATCH_SIZE}).toArray();
      const rows = docs.map(d => ({
        id:                   d._id.toString(),
        tenantId:             '${tenant}',
        accountId:            d.accountId || '',
        accountExternalId:    d.accountExternalId || '',
        status:               d.status || 'draft',
        currency:             d.currency || '',
        sessionCounter:       d.sessionCounter || 0,
        collectedFundsCents:  d.collectedFundsCents || 0,
        collectedFeesCents:   d.collectedFeesCents || 0,
        transferAmountCents:  d.transferAmountCents || 0,
        transferExternalId:   d.transferExternalId || '',
        createdOn:            d.createdOn ? d.createdOn.getTime() : 0,
        lastChangedOn:        d.lastChangedOn ? d.lastChangedOn.getTime() : 0
      }));
      print(JSON.stringify(rows));
    ")

    local rows
    rows=$(echo "$batch" | jq -c '.[]' 2>/dev/null || true)
    [[ -z "$rows" ]] && break

    local row_count
    row_count=$(echo "$rows" | wc -l)

    if $DRY_RUN; then
      echo "    [DRY-RUN] billing_transfers: would upsert ${row_count} rows (skip=${skip})"
    else
      echo "$rows" | jq -r '
        [
          .id,
          .tenantId,
          .accountId,
          .accountExternalId,
          .status,
          .currency,
          (.sessionCounter      | tostring),
          (.collectedFundsCents | tostring),
          (.collectedFeesCents  | tostring),
          (.transferAmountCents | tostring),
          (.transferExternalId | if . == "" then null else . end),
          (.createdOn    | tostring),
          (.lastChangedOn | tostring)
        ] | @csv
      ' | psql "$PG_URI" -c "
        COPY billing_transfers (
          id, tenant_id, account_id, account_external_id, status, currency,
          session_counter, collected_funds_cents, collected_fees_cents,
          transfer_amount_cents, transfer_external_id,
          created_on, last_changed_on
        )
        FROM STDIN WITH (FORMAT csv, NULL 'null')
        ON CONFLICT (id) DO UPDATE SET
          status                = EXCLUDED.status,
          session_counter       = EXCLUDED.session_counter,
          collected_funds_cents = EXCLUDED.collected_funds_cents,
          collected_fees_cents  = EXCLUDED.collected_fees_cents,
          transfer_amount_cents = EXCLUDED.transfer_amount_cents,
          transfer_external_id  = EXCLUDED.transfer_external_id,
          last_changed_on       = EXCLUDED.last_changed_on;
      "
    fi

    count=$(( count + row_count ))
    skip=$(( skip + BATCH_SIZE ))
    [[ "$row_count" -lt "$BATCH_SIZE" ]] && break
  done
  echo "    billing_transfers: ${count} rows"
}

# ── Billing user migration ────────────────────────────────────────────────────
migrate_users() {
  local tenant="$1"
  local collection="${tenant}.billingusers"
  local skip=0
  local count=0

  echo "  billing_users ..."
  while true; do
    local batch
    batch=$(mongosh --quiet "$MONGO_URI" --eval "
      const col = db.getSiblingDB('ev').getCollection('${collection}');
      const docs = col.find({}).skip(${skip}).limit(${BATCH_SIZE}).toArray();
      const rows = docs.map(d => ({
        userId:                   d.userId || '',
        tenantId:                 '${tenant}',
        customerId:               d.customerId || '',
        defaultPaymentMethodId:   d.defaultPaymentMethodId || '',
        createdOn:                d.createdOn ? d.createdOn.getTime() : 0
      }));
      print(JSON.stringify(rows));
    ")

    local rows
    rows=$(echo "$batch" | jq -c '.[]' 2>/dev/null || true)
    [[ -z "$rows" ]] && break

    local row_count
    row_count=$(echo "$rows" | wc -l)

    if $DRY_RUN; then
      echo "    [DRY-RUN] billing_users: would upsert ${row_count} rows (skip=${skip})"
    else
      echo "$rows" | jq -r '
        [
          .userId,
          .tenantId,
          .customerId,
          (.defaultPaymentMethodId | if . == "" then null else . end),
          (.createdOn | tostring)
        ] | @csv
      ' | psql "$PG_URI" -c "
        COPY billing_users (
          user_id, tenant_id, customer_id, default_payment_method_id, created_on
        )
        FROM STDIN WITH (FORMAT csv, NULL 'null')
        ON CONFLICT (tenant_id, user_id) DO UPDATE SET
          customer_id               = EXCLUDED.customer_id,
          default_payment_method_id = EXCLUDED.default_payment_method_id;
      "
    fi

    count=$(( count + row_count ))
    skip=$(( skip + BATCH_SIZE ))
    [[ "$row_count" -lt "$BATCH_SIZE" ]] && break
  done
  echo "    billing_users: ${count} rows"
}

# ── Per-tenant orchestration ──────────────────────────────────────────────────
migrate_tenant() {
  local tenant="$1"
  echo "==> Migrating tenant=${tenant}"
  migrate_invoices   "$tenant"
  migrate_accounts   "$tenant"
  migrate_transfers  "$tenant"
  migrate_users      "$tenant"
  echo "==> Done tenant=${tenant}"
  echo ""
}

# ── Main ──────────────────────────────────────────────────────────────────────
main() {
  echo "MongoDB:     ${MONGO_URI}"
  echo "PostgreSQL:  ${PG_URI}"
  echo "Batch size:  ${BATCH_SIZE}  DryRun: ${DRY_RUN}  From: ${FROM_DATE}"
  echo ""

  if ! $DRY_RUN; then
    ensure_tables_exist
  fi

  local tenants
  if [[ -n "$TENANT_FILTER" ]]; then
    tenants="$TENANT_FILTER"
  else
    tenants=$(get_tenants)
  fi

  if [[ -z "$tenants" ]]; then
    echo "No billing collections found in MongoDB. Check MONGO_URI." >&2
    exit 1
  fi

  echo "Tenants: $(echo "$tenants" | tr '\n' ' ')"
  echo ""

  while IFS= read -r tenant; do
    [[ -z "$tenant" ]] && continue
    migrate_tenant "$tenant"
  done <<< "$tenants"

  echo "Billing migration complete."
}

main
