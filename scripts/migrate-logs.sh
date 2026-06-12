#!/usr/bin/env bash
# migrate-logs.sh
#
# Migrates ev-server MongoDB `logs` collections (one per tenant) into the
# TimescaleDB `ev_logs` hypertable used by ev-analytics
# (schema: modules/services/analytics/src/main/resources/db/migration/V1__analytics_schema.sql).
#
# Prerequisites: mongosh, psql, jq
#
# Usage:
#   MONGO_URI=mongodb://localhost:27017 \
#   PG_URI=postgresql://ev:secret@localhost:5432/ev_analytics \
#   ./migrate-logs.sh [--tenant TENANT_ID] [--dry-run] [--from 2024-01-01] \
#                     [--retain-days 90]
#
# MongoDB document shape (ev-server LoggingStorage):
#   { _id, level, source, host, message, timestamp, action, module, method,
#     detailedMessages: [...], user: {...}, chargingStation: {...},
#     siteID, siteAreaID }

set -euo pipefail

MONGO_URI="${MONGO_URI:-mongodb://localhost:27017}"
PG_URI="${PG_URI:-postgresql://ev:secret@localhost:5432/ev_analytics}"
BATCH_SIZE=2000
TENANT_FILTER=""
DRY_RUN=false
FROM_DATE="1970-01-01T00:00:00Z"
RETAIN_DAYS=730   # 2 years

while [[ $# -gt 0 ]]; do
  case "$1" in
    --tenant)       TENANT_FILTER="$2"; shift 2 ;;
    --dry-run)      DRY_RUN=true;        shift   ;;
    --from)         FROM_DATE="$2";      shift 2 ;;
    --retain-days)  RETAIN_DAYS="$2";    shift 2 ;;
    *) echo "Unknown arg: $1" >&2; exit 1 ;;
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
  psql "$PG_URI" -v ON_ERROR_STOP=1 <<SQL
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

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

-- Retain raw logs for configured period
SELECT add_retention_policy(
  'ev_logs',
  INTERVAL '${RETAIN_DAYS} days',
  if_not_exists => TRUE
);
SQL
}

get_tenants() {
  mongosh --quiet "$MONGO_URI" --eval '
    const cols = db.getSiblingDB("ev").listCollectionNames()
      .filter(n => n.endsWith(".logs"))
      .map(n => n.replace(".logs", ""));
    print(cols.join("\n"));
  '
}

migrate_tenant() {
  local tenant="$1"
  local collection="${tenant}.logs"
  echo "==> Migrating tenant=${tenant} collection=${collection}"

  local skip=0
  local count=0

  while true; do
    local batch
    batch=$(mongosh --quiet "$MONGO_URI" --eval "
      const col = db.getSiblingDB('ev').getCollection('${collection}');
      const docs = col.find(
        { timestamp: { \\\$gte: ISODate('${FROM_DATE}') } },
        { _id: 0, level: 1, source: 1, message: 1, timestamp: 1,
          action: 1, module: 1, method: 1, detailedMessages: 1,
          siteID: 1, siteAreaID: 1,
          'user.id': 1, 'chargingStation.id': 1 }
      ).sort({ timestamp: 1 }).skip(${skip}).limit(${BATCH_SIZE}).toArray();
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
      # Column order: tenant_id, time, level, source, action, module, method,
      #               message, user_id, charging_station_id, site_id,
      #               site_area_id, details
      # level is normalised to the single-char codes used by ev-analytics
      # ('D'|'I'|'W'|'E'); monolith levels are already single chars, longer
      # names (DEBUG, INFO, ...) are truncated to their first letter.
      echo "$rows" | jq -r --arg tenant "$tenant" '
        [
          $tenant,
          (.timestamp // "1970-01-01T00:00:00Z"),
          ((.level // "I") | ascii_upcase | .[0:1]),
          (.source // "unknown"),
          (.action // "Unknown"),
          (.module // null),
          (.method // null),
          (.message // "" | gsub("\n"; " ") | gsub("\t"; " ")),
          (.user.id // null),
          (.["chargingStation"].id // null),
          (.siteID // null),
          (.siteAreaID // null),
          (if .detailedMessages then (.detailedMessages | tojson) else null end)
        ] | @csv
      ' | copy_upsert "ev_logs" \
            "tenant_id, time, level, source, action, module, method, message, user_id, charging_station_id, site_id, site_area_id, details" \
            "ON CONFLICT DO NOTHING"
    fi

    count=$(( count + row_count ))
    skip=$(( skip + BATCH_SIZE ))
    [[ $(( count % 10000 )) -eq 0 ]] && echo "    migrated ${count} rows..."
    [[ "$row_count" -lt "$BATCH_SIZE" ]] && break
  done

  echo "==> Done tenant=${tenant}: ${count} total rows"
}

main() {
  echo "MongoDB:      ${MONGO_URI}"
  echo "TimescaleDB:  ${PG_URI}"
  echo "Batch size:   ${BATCH_SIZE}  DryRun: ${DRY_RUN}  From: ${FROM_DATE}  RetainDays: ${RETAIN_DAYS}"
  echo ""

  ensure_schema

  local tenants
  if [[ -n "$TENANT_FILTER" ]]; then
    tenants="$TENANT_FILTER"
  else
    tenants=$(get_tenants)
  fi

  [[ -z "$tenants" ]] && { echo "No log collections found." >&2; exit 1; }

  echo "Tenants: $(echo "$tenants" | tr '\n' ' ')"
  echo ""

  while IFS= read -r tenant; do
    [[ -z "$tenant" ]] && continue
    migrate_tenant "$tenant"
  done <<< "$tenants"

  echo ""
  echo "Log migration complete."
}

main
