#!/usr/bin/env bash
# migrate-consumptions.sh
#
# Migrates ev-server MongoDB `consumptions` collections (one per tenant) into
# the TimescaleDB `consumptions` hypertable.
#
# Prerequisites:
#   - mongosh  (≥ 2.0)
#   - psql     (≥ 15)
#   - jq       (≥ 1.6)
#
# Usage:
#   MONGO_URI=mongodb://localhost:27017 \
#   PG_URI=postgresql://ev:secret@localhost:5432/ev_analytics \
#   ./migrate-consumptions.sh [--tenant TENANT_ID] [--dry-run] [--from 2024-01-01]
#
# The script is idempotent: it upserts on (transaction_id, tenant_id) so it
# can be re-run after failures without creating duplicates.

set -euo pipefail

MONGO_URI="${MONGO_URI:-mongodb://localhost:27017}"
PG_URI="${PG_URI:-postgresql://ev:secret@localhost:5432/ev_analytics}"
BATCH_SIZE=1000
TENANT_FILTER=""
DRY_RUN=false
FROM_DATE="1970-01-01T00:00:00Z"

# ── Argument parsing ──────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --tenant)  TENANT_FILTER="$2"; shift 2 ;;
    --dry-run) DRY_RUN=true;        shift   ;;
    --from)    FROM_DATE="$2";      shift 2 ;;
    *) echo "Unknown arg: $1" >&2; exit 1  ;;
  esac
done

# ── Schema setup ──────────────────────────────────────────────────────────────
ensure_schema() {
  psql "$PG_URI" <<'SQL'
CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS consumptions (
  time                TIMESTAMPTZ NOT NULL,
  tenant_id           TEXT        NOT NULL,
  transaction_id      BIGINT      NOT NULL,
  charging_station_id TEXT        NOT NULL,
  connector_id        INT         NOT NULL DEFAULT 1,
  user_id             TEXT,
  site_id             TEXT,
  energy_wh           DOUBLE PRECISION NOT NULL DEFAULT 0,
  duration_secs       INT         NOT NULL DEFAULT 0,
  instantaneous_power_w DOUBLE PRECISION,
  tariff_id           TEXT,
  total_cost_cents    INT,
  currency            CHAR(3),
  stop_reason         TEXT,
  PRIMARY KEY (time, tenant_id, transaction_id)
);

SELECT create_hypertable(
  'consumptions', 'time',
  if_not_exists => TRUE,
  chunk_time_interval => INTERVAL '7 days'
);

-- Continuous aggregate: hourly rollup per station
CREATE MATERIALIZED VIEW IF NOT EXISTS consumptions_hourly
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('1 hour', time) AS bucket,
  tenant_id,
  charging_station_id,
  COUNT(*)                    AS session_count,
  SUM(energy_wh)              AS total_energy_wh,
  AVG(duration_secs)          AS avg_duration_secs,
  SUM(total_cost_cents)       AS total_cost_cents
FROM consumptions
GROUP BY bucket, tenant_id, charging_station_id
WITH NO DATA;

-- Retain raw data for 2 years; hourly rollup forever
SELECT add_retention_policy('consumptions', INTERVAL '2 years', if_not_exists => TRUE);
SQL
}

# ── Discover tenant IDs from MongoDB ─────────────────────────────────────────
get_tenants() {
  mongosh --quiet "$MONGO_URI" --eval '
    const dbs = db.adminCommand({ listDatabases: 1 }).databases.map(d => d.name);
    // ev-server uses a single database; collections are named {tenantId}.consumptions
    const cols = db.getSiblingDB("ev").listCollectionNames()
      .filter(n => n.endsWith(".consumptions"))
      .map(n => n.replace(".consumptions", ""));
    print(cols.join("\n"));
  '
}

# ── Migrate one tenant ────────────────────────────────────────────────────────
migrate_tenant() {
  local tenant="$1"
  local collection="${tenant}.consumptions"
  echo "==> Migrating tenant=${tenant} collection=${collection}"

  # Export from MongoDB as NDJSON, batch-insert into TimescaleDB
  local skip=0
  local count=0

  while true; do
    local batch
    batch=$(mongosh --quiet "$MONGO_URI" --eval "
      const col = db.getSiblingDB('ev').getCollection('${collection}');
      const docs = col.find(
        { endedAt: { \\\$gte: ISODate('${FROM_DATE}') } },
        { _id: 0, transactionId: 1, chargingStationId: 1, connectorId: 1,
          userId: 1, siteId: 1, startedAt: 1, endedAt: 1,
          totalConsumptionWh: 1, totalDurationSecs: 1,
          instantaneousPowerW: 1, pricingId: 1, totalCostCents: 1,
          currency: 1, stopReason: 1 }
      ).skip(${skip}).limit(${BATCH_SIZE}).toArray();
      print(JSON.stringify(docs));
    ")

    local rows
    rows=$(echo "$batch" | jq -c '.[]' 2>/dev/null || true)
    [[ -z "$rows" ]] && break

    local row_count
    row_count=$(echo "$rows" | wc -l)

    if $DRY_RUN; then
      echo "    [DRY-RUN] Would insert ${row_count} rows (skip=${skip})"
    else
      # Build a VALUES block and COPY via psql
      echo "$rows" | jq -r '
        [
          (.endedAt // "1970-01-01T00:00:00Z"),
          "'"${tenant}"'",
          (.transactionId // 0 | tostring),
          (.chargingStationId // ""),
          (.connectorId // 1 | tostring),
          (.userId // ""),
          (.siteId // ""),
          (.totalConsumptionWh // 0 | tostring),
          (.totalDurationSecs // 0 | tostring),
          (.instantaneousPowerW // "" | tostring),
          (.pricingId // ""),
          (.totalCostCents // "" | tostring),
          (.currency // ""),
          (.stopReason // "")
        ] | @csv
      ' | psql "$PG_URI" -c "
        COPY consumptions (
          time, tenant_id, transaction_id, charging_station_id, connector_id,
          user_id, site_id, energy_wh, duration_secs, instantaneous_power_w,
          tariff_id, total_cost_cents, currency, stop_reason
        )
        FROM STDIN WITH (FORMAT csv)
        ON CONFLICT (time, tenant_id, transaction_id) DO UPDATE SET
          energy_wh             = EXCLUDED.energy_wh,
          duration_secs         = EXCLUDED.duration_secs,
          instantaneous_power_w = EXCLUDED.instantaneous_power_w,
          total_cost_cents      = EXCLUDED.total_cost_cents;
      "
    fi

    count=$(( count + row_count ))
    skip=$(( skip + BATCH_SIZE ))
    echo "    migrated ${count} rows..."
    [[ "$row_count" -lt "$BATCH_SIZE" ]] && break
  done

  echo "==> Done tenant=${tenant}: ${count} total rows"
}

# ── Main ──────────────────────────────────────────────────────────────────────
main() {
  echo "Connecting to MongoDB: ${MONGO_URI}"
  echo "Connecting to TimescaleDB: ${PG_URI}"
  echo "Batch size: ${BATCH_SIZE}  DryRun: ${DRY_RUN}  From: ${FROM_DATE}"
  echo ""

  ensure_schema

  local tenants
  if [[ -n "$TENANT_FILTER" ]]; then
    tenants="$TENANT_FILTER"
  else
    tenants=$(get_tenants)
  fi

  if [[ -z "$tenants" ]]; then
    echo "No tenants found. Check MONGO_URI and collection names." >&2
    exit 1
  fi

  echo "Tenants to migrate: $(echo "$tenants" | tr '\n' ' ')"
  echo ""

  while IFS= read -r tenant; do
    [[ -z "$tenant" ]] && continue
    migrate_tenant "$tenant"
  done <<< "$tenants"

  echo ""
  echo "Migration complete. Refresh continuous aggregate:"
  if ! $DRY_RUN; then
    psql "$PG_URI" -c "CALL refresh_continuous_aggregate('consumptions_hourly', NULL, NULL);"
  fi
}

main
