#!/usr/bin/env bash
# migrate-logs.sh
#
# Migrates ev-server MongoDB `logs` collections (one per tenant) into
# TimescaleDB `audit_logs` hypertable for long-term retention + fast
# time-range queries.
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
#   { _id, level, source, host, message, timestamp, action, hasDetailedMessages,
#     detailedMessages: [...], user: {...}, chargingStation: {...} }

set -euo pipefail

MONGO_URI="${MONGO_URI:-mongodb://localhost:27017}"
PG_URI="${PG_URI:-postgresql://ev:secret@localhost:5432/ev_analytics}"
BATCH_SIZE=2000
TENANT_FILTER=""
DRY_RUN=false
FROM_DATE="1970-01-01T00:00:00Z"
RETAIN_DAYS=730   # 2 years — matches TimescaleDB retention policy

while [[ $# -gt 0 ]]; do
  case "$1" in
    --tenant)       TENANT_FILTER="$2"; shift 2 ;;
    --dry-run)      DRY_RUN=true;        shift   ;;
    --from)         FROM_DATE="$2";      shift 2 ;;
    --retain-days)  RETAIN_DAYS="$2";    shift 2 ;;
    *) echo "Unknown arg: $1" >&2; exit 1 ;;
  esac
done

ensure_schema() {
  psql "$PG_URI" <<SQL
CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS audit_logs (
  time           TIMESTAMPTZ  NOT NULL,
  tenant_id      TEXT         NOT NULL,
  level          TEXT         NOT NULL,   -- DEBUG INFO WARNING ERROR SECURITY
  source         TEXT         NOT NULL,   -- component / service name
  action         TEXT,                    -- OCPP action or REST endpoint
  message        TEXT         NOT NULL,
  host           TEXT,
  user_id        TEXT,
  user_email     TEXT,
  station_id     TEXT,
  details        JSONB,                   -- detailedMessages blob
  PRIMARY KEY (time, tenant_id, level, source, message)
);

SELECT create_hypertable(
  'audit_logs', 'time',
  if_not_exists => TRUE,
  chunk_time_interval => INTERVAL '1 day'
);

-- Index for common query patterns
CREATE INDEX IF NOT EXISTS idx_audit_tenant_source
  ON audit_logs (tenant_id, source, time DESC);
CREATE INDEX IF NOT EXISTS idx_audit_tenant_station
  ON audit_logs (tenant_id, station_id, time DESC)
  WHERE station_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_audit_level
  ON audit_logs (level, time DESC)
  WHERE level IN ('ERROR', 'SECURITY');

-- Retain raw logs for configured period
SELECT add_retention_policy(
  'audit_logs',
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
        { _id: 0, level: 1, source: 1, host: 1, message: 1, timestamp: 1,
          action: 1, detailedMessages: 1,
          'user.id': 1, 'user.email': 1,
          'chargingStation.id': 1 }
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
      echo "$rows" | jq -r --arg tenant "$tenant" '
        [
          (.timestamp // "1970-01-01T00:00:00Z"),
          $tenant,
          (.level // "INFO"),
          (.source // "unknown"),
          (.action // ""),
          (.message // "" | gsub("\n"; " ") | gsub("\t"; " ")),
          (.host // ""),
          (.user.id // ""),
          (.user.email // ""),
          (.["chargingStation"].id // ""),
          (if .detailedMessages then (.detailedMessages | tojson) else "" end)
        ] | @csv
      ' | psql "$PG_URI" -c "
        COPY audit_logs (
          time, tenant_id, level, source, action, message,
          host, user_id, user_email, station_id, details
        )
        FROM STDIN WITH (FORMAT csv, NULL '')
        ON CONFLICT (time, tenant_id, level, source, message) DO NOTHING;
      "
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
