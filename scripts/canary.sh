#!/usr/bin/env bash
# canary.sh — manage Kong upstream weights for Strangler Fig traffic shifting
#
# Usage:
#   canary.sh status                      print current weights for all upstreams
#   canary.sh set <service> <pct>         route <pct>% to Scala, rest to TypeScript
#   canary.sh rollback <service>          route 100% back to TypeScript
#   canary.sh cutover <service>           route 100% to Scala (remove TypeScript)
#
# <service> is one of: ocpp-gateway | rest-api | auth | roaming
#
# Requires: curl, jq
# KONG_ADMIN_URL defaults to http://localhost:8001

set -euo pipefail

KONG="${KONG_ADMIN_URL:-http://localhost:8001}"

UPSTREAMS=(
  "ocpp-gateway:ocpp-gateway-upstream:typescript-monolith:3000:ocpp-gateway:8010"
  "rest-api:rest-api-upstream:typescript-monolith:3000:rest-api:8080"
  "auth:auth-upstream:typescript-monolith:3000:auth-service:8080"
  "roaming:roaming-upstream:typescript-monolith:3000:roaming:8080"
)

usage() {
  grep '^#' "$0" | sed 's/^# \?//' | head -8
  exit 1
}

find_upstream_row() {
  local svc="$1"
  for row in "${UPSTREAMS[@]}"; do
    if [[ "${row%%:*}" == "$svc" ]]; then
      echo "$row"; return
    fi
  done
  echo "Unknown service: $svc" >&2; exit 1
}

get_targets() {
  local upstream="$1"
  curl -s "${KONG}/upstreams/${upstream}/targets" | jq -r '.data[] | "\(.target) weight=\(.weight)"'
}

set_target_weight() {
  local upstream="$1" target="$2" weight="$3"
  # Kong does not support PATCH on targets — delete + recreate
  local id
  id=$(curl -s "${KONG}/upstreams/${upstream}/targets" \
    | jq -r --arg t "$target" '.data[] | select(.target==$t) | .id')
  if [[ -n "$id" ]]; then
    curl -s -X DELETE "${KONG}/upstreams/${upstream}/targets/${id}" > /dev/null
  fi
  curl -s -X POST "${KONG}/upstreams/${upstream}/targets" \
    -d "target=${target}" \
    -d "weight=${weight}" > /dev/null
}

cmd_status() {
  printf "%-20s %-35s %s\n" "SERVICE" "TARGET" "WEIGHT"
  printf "%-20s %-35s %s\n" "-------" "------" "------"
  for row in "${UPSTREAMS[@]}"; do
    IFS=: read -r svc upstream ts_host ts_port sc_host sc_port <<< "$row"
    local ts_target="${ts_host}:${ts_port}"
    local sc_target="${sc_host}:${sc_port}"
    local data
    data=$(curl -s "${KONG}/upstreams/${upstream}/targets")
    local ts_w sc_w
    ts_w=$(echo "$data" | jq -r --arg t "$ts_target" '.data[] | select(.target==$t) | .weight // 0')
    sc_w=$(echo "$data" | jq -r --arg t "$sc_target" '.data[] | select(.target==$t) | .weight // 0')
    printf "%-20s %-35s TypeScript=%s  Scala=%s\n" "$svc" "$upstream" "${ts_w:-0}" "${sc_w:-0}"
  done
}

cmd_set() {
  local svc="$1" pct="$2"
  if ! [[ "$pct" =~ ^[0-9]+$ ]] || (( pct < 0 || pct > 100 )); then
    echo "pct must be 0–100" >&2; exit 1
  fi
  local row; row=$(find_upstream_row "$svc")
  IFS=: read -r _ upstream ts_host ts_port sc_host sc_port <<< "$row"
  local ts_target="${ts_host}:${ts_port}"
  local sc_target="${sc_host}:${sc_port}"
  local ts_w=$(( 100 - pct ))
  echo "Setting ${upstream}: TypeScript=${ts_w} Scala=${pct}"
  set_target_weight "$upstream" "$ts_target" "$ts_w"
  set_target_weight "$upstream" "$sc_target" "$pct"
  echo "Done. Verify:"
  get_targets "$upstream"
}

cmd_rollback() {
  local svc="$1"
  echo "Rolling back ${svc} to 100% TypeScript..."
  cmd_set "$svc" 0
}

cmd_cutover() {
  local svc="$1"
  echo "Cutting over ${svc} to 100% Scala..."
  cmd_set "$svc" 100
}

case "${1:-}" in
  status)            cmd_status ;;
  set)               [[ $# -ge 3 ]] || usage; cmd_set "$2" "$3" ;;
  rollback)          [[ $# -ge 2 ]] || usage; cmd_rollback "$2" ;;
  cutover)           [[ $# -ge 2 ]] || usage; cmd_cutover "$2" ;;
  *)                 usage ;;
esac
