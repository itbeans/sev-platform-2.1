#!/usr/bin/env bash
# 04-build-push.sh — build Docker images and push to Artifact Registry
#
# 1. Configures Docker to authenticate with Artifact Registry.
# 2. Builds all 13 service images locally via SBT.
# 3. Re-tags each image with the Artifact Registry prefix and pushes.
#
# The SBT Docker plugin publishes to the local daemon under the
# ghcr.io/itbeans/* namespace (from build.sbt).  This script then retags
# and pushes to Artifact Registry without touching build.sbt.
#
# Prerequisites:
#   - Java 21 + sbt on PATH
#   - Docker running
#   - gcloud authenticated with Artifact Registry write access
#   - 02-provision-infra.sh completed (repository must exist)
#
# Usage:
#   source scripts/gcp/config.env && ./scripts/gcp/04-build-push.sh [--skip-build]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="${SCRIPT_DIR}/../.."
source "${SCRIPT_DIR}/config.env"

SKIP_BUILD=false
for arg in "$@"; do
  [[ "$arg" == "--skip-build" ]] && SKIP_BUILD=true
done

# All 13 service image names as published by sbt docker:publishLocal
# (without registry prefix; tag comes from build.sbt's version/dockerImageTag)
SERVICES=(
  auth-service
  ocpp-gateway
  ocpp-processor
  rest-api
  billing-service
  pricing-service
  smart-charging
  notification
  roaming
  asset
  car
  scheduler
  analytics
)

LOCAL_REGISTRY="ghcr.io/itbeans"

# ── 1. Configure Docker → Artifact Registry auth ──────────────────────────────
echo "==> Configuring Docker authentication for Artifact Registry..."
gcloud auth configure-docker "${GCP_REGION}-docker.pkg.dev" --quiet
echo "    Done."

# ── 2. Build images with SBT ──────────────────────────────────────────────────
if [[ "${SKIP_BUILD}" == false ]]; then
  echo "==> Building all 13 Docker images via SBT (this takes several minutes)..."
  cd "${ROOT}"
  sbt "set every dockerImageCreationTask := (LocalProject(\"root\") / Docker / publishLocal)" \
      docker:publishLocal
  echo "==> Build complete."
else
  echo "==> --skip-build: using existing local images."
fi

# ── 3. Detect the image tag produced by SBT ──────────────────────────────────
# SBT Docker plugin tags images with the project version by default.
# Detect it from the first service image.
SBT_TAG=$(docker images "${LOCAL_REGISTRY}/ev-auth-service" \
            --format "{{.Tag}}" 2>/dev/null | head -1 || echo "${IMAGE_TAG}")

if [[ -z "${SBT_TAG}" ]]; then
  echo "ERROR: Could not detect image tag from local Docker images." >&2
  echo "       Run without --skip-build or set IMAGE_TAG manually." >&2
  exit 1
fi

echo "==> Local image tag: ${SBT_TAG}  →  pushing as: ${IMAGE_TAG}"

# ── 4. Retag and push each service image ─────────────────────────────────────
echo "==> Pushing images to ${IMAGE_PREFIX}"
echo ""

for svc in "${SERVICES[@]}"; do
  local_image="${LOCAL_REGISTRY}/ev-${svc}:${SBT_TAG}"
  remote_image="${IMAGE_PREFIX}/ev-${svc}:${IMAGE_TAG}"
  latest_image="${IMAGE_PREFIX}/ev-${svc}:latest"

  if ! docker image inspect "${local_image}" >/dev/null 2>&1; then
    echo "  [WARN] Image not found locally: ${local_image} — skipping."
    continue
  fi

  docker tag "${local_image}" "${remote_image}"
  docker tag "${local_image}" "${latest_image}"

  echo -n "  Pushing ev-${svc}... "
  docker push "${remote_image}" --quiet
  docker push "${latest_image}" --quiet
  echo "done"
done

echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║  All images pushed — proceed to 05-deploy.sh               ║"
printf "║  Registry: %-50s ║\n" "${IMAGE_PREFIX}"
printf "║  Tag:      %-50s ║\n" "${IMAGE_TAG}"
echo "╚══════════════════════════════════════════════════════════════╝"
