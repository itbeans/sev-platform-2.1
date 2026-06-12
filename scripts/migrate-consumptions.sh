#!/usr/bin/env bash
# migrate-consumptions.sh
#
# Migrates ev-server MongoDB `consumptions` collections (one per tenant) into
# the TimescaleDB `ev_consumptions` hypertable used by ev-analytics
# (schema: modules/services/analytics/src/main/resources/db/migration/V1__analytics_schema.sql).
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
# The script is idempotent: rows are staged via COPY and inserted with
# ON CONFLICT DO NOTHING, so it can be re-run after failures without
# creating duplicates.

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

# ── COPY + upsert helper ──────────────────────────────────────────────────────
# PostgreSQL COPY has no ON CONFLICT clause, so rows are staged in a temp
# table and merged with INSERT ... SELECT ... ON CONFLICT.
copy_upsert() {
  local table="$1" cols="$2" conflict_clause="$3"
  {
    echo "CREATE TEMP TABLE _stage (LIKE ${table} INCLUDING DEFAULTS);"
    echo "COPY _stage (${cols}) FROM STDIN WITH (FORMAT csv, NULL '');"
    cat
    echo "\\."
    echo "INSERT INTO ${table} (${cols}) SELECT ${cols} FROM _stage ${conflict_clause};"
  } | psql "$PG_URI" -q -v ON_ERROR_STOP=1
}

# ── Schema setup ──────────────────────────────────────────────────────────────
# Matches V1__analytics_schema.sql — keep in sync.
ensure_schema() {
  psql "$PG_URI" -v ON_ERROR_STOP=1 <<'SQL'
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

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

CREATE INDEX IF NOT EXISTS idx_ev_consumptions_tenant_tx
  ON ev_consumptions (tenant_id, transaction_id, time DESC)
  WHERE transaction_id IS NOT NULL;

-- Continuous aggregate: hourly energy rollup per station
CREATE MATERIALIZED VIEW IF NOT EXISTS ev_consumptions_hourly
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('1 hour', time)             AS bucket,
  tenant_id,
  charging_station_id,
  COUNT(*)                                AS reading_count,
  MAX(cumulated_kwh) - MIN(cumulated_kwh) AS energy_kwh,
  AVG(instant_watts)                      AS avg_watts
FROM ev_consumptions
GROUP BY bucket, tenant_id, charging_station_id
WITH NO DATA;

-- Retain raw data for 2 years; hourly rollup forever
SELECT add_retention_policy('ev_consumptions', INTERVAL '2 years', if_not_exists => TRUE);
SQL
}

# ── Discover tenant IDs from MongoDB ─────────────────────────────────────────
get_tenants() {
  mongosh --quiet "$MONGO_URI" --eval '
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
        { _id: 0, transactionId: 1, connectorId: 1,
          chargingStationID: 1, chargingStationId: 1,
          siteAreaID: 1, siteAreaId: 1, siteID: 1, siteId: 1,
          userID: 1, userId: 1, startedAt: 1, endedAt: 1,
          instantWatts: 1, instantaneousPowerW: 1,
          cumulatedConsumptionWh: 1, totalConsumptionWh: 1 }
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
      # Column order: tenant_id, time, charging_station_id, connector_id,
      #               site_area_id, site_id, user_id, transaction_id,
      #               instant_watts, cumulated_kwh
      # Both monolith (chargingStationID) and Scala (chargingStationId) field
      # spellings are accepted.
      # jq @csv renders null as an unquoted empty field, which COPY reads as
      # SQL NULL (quoted "" would be an empty string / invalid number).
      echo "$rows" | jq -r --arg tenant "$tenant" '
        [
          $tenant,
          (.endedAt // .startedAt // "1970-01-01T00:00:00Z"),
          (.chargingStationID // .chargingStationId // ""),
          (.connectorId // 1),
          (.siteAreaID // .siteAreaId // null),
          (.siteID // .siteId // null),
          (.userID // .userId // null),
          (.transactionId // null),
          (.instantWatts // .instantaneousPowerW // 0),
          ((.cumulatedConsumptionWh // .totalConsumptionWh // 0) / 1000)
        ] | @csv
      ' | copy_upsert "ev_consumptions" \
            "tenant_id, time, charging_station_id, connector_id, site_area_id, site_id, user_id, transaction_id, instant_watts, cumulated_kwh" \
            "ON CONFLICT DO NOTHING"
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
    psql "$PG_URI" -c "CALL refresh_continuous_aggregate('ev_consumptions_hourly', NULL, NULL);"
  fi
}

main
