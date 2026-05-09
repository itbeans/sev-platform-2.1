#!/usr/bin/env bash
# Tear down the full local EV stack.
#
# Usage:
#   ./scripts/down.sh         # stop containers, keep volumes
#   ./scripts/down.sh -v      # stop containers AND delete volumes (clean slate)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$SCRIPT_DIR/.."

VOLUMES_FLAG=""
for arg in "$@"; do
  [[ "$arg" == "-v" ]] && VOLUMES_FLAG="-v"
done

echo "==> Stopping Scala services..."
docker compose -f "$ROOT/docker/docker-compose-services.yml" down $VOLUMES_FLAG 2>/dev/null || true

echo "==> Stopping infrastructure..."
docker compose -f "$ROOT/docker/docker-compose-local.yml" down $VOLUMES_FLAG 2>/dev/null || true

echo "==> Stack is down."
[[ -n "$VOLUMES_FLAG" ]] && echo "    Volumes deleted (clean slate)."
