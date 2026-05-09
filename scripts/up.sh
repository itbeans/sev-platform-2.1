#!/usr/bin/env bash
# Bring up the full local EV stack: infra + all 13 Scala services.
#
# Prerequisites:
#   - Docker running
#   - Java 21 + sbt on PATH
#
# Usage:
#   ./scripts/up.sh              # build images + start everything
#   ./scripts/up.sh --skip-build # start only (images already built)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$SCRIPT_DIR/.."

SKIP_BUILD=false
for arg in "$@"; do
  [[ "$arg" == "--skip-build" ]] && SKIP_BUILD=true
done

# ── 1. Build Docker images ────────────────────────────────────────────────────
if [[ "$SKIP_BUILD" == false ]]; then
  echo "==> Building Docker images for all 13 services..."
  cd "$ROOT"
  sbt docker:publishLocal
  echo "==> Images built."
fi

# ── 2. Start infrastructure ───────────────────────────────────────────────────
echo "==> Starting infrastructure (MongoDB, Kafka, Kong, Prometheus, Grafana, Jaeger)..."
docker compose -f "$ROOT/docker/docker-compose-local.yml" up -d

# ── 3. Wait for Kafka ─────────────────────────────────────────────────────────
echo -n "==> Waiting for Kafka to be healthy"
timeout 120 bash -c '
  until docker exec ev-kafka /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server localhost:9092 --list >/dev/null 2>&1; do
    echo -n "."
    sleep 3
  done
'
echo " OK"

# ── 4. Wait for MongoDB ───────────────────────────────────────────────────────
echo -n "==> Waiting for MongoDB to be healthy"
timeout 60 bash -c '
  until docker exec ev-mongo mongosh --quiet \
        --eval "db.adminCommand(\"ping\")" >/dev/null 2>&1; do
    echo -n "."
    sleep 3
  done
'
echo " OK"

# ── 5. Start Scala services ───────────────────────────────────────────────────
echo "==> Starting Scala microservices..."
docker compose -f "$ROOT/docker/docker-compose-services.yml" up -d

# ── 6. Summary ────────────────────────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║              ev-server local stack is up             ║"
echo "╠══════════════════════════════════════════════════════╣"
echo "║  OCPP WebSocket   ws://localhost:8010/ocpp/2.1/{id}  ║"
echo "║  REST API         http://localhost:8080/api/v1       ║"
echo "║  Auth HTTP        http://localhost:8084              ║"
echo "║  Kong Admin       http://localhost:8001              ║"
echo "║  Kafka UI         http://localhost:8090              ║"
echo "║  Grafana          http://localhost:3000  (admin/admin)║"
echo "║  Jaeger UI        http://localhost:16686             ║"
echo "║  Prometheus       http://localhost:9090              ║"
echo "╚══════════════════════════════════════════════════════╝"
echo ""
echo "Logs:  docker compose -f docker/docker-compose-services.yml logs -f"
echo "Down:  ./scripts/down.sh"
