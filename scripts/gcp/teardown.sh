#!/usr/bin/env bash
# teardown.sh — delete all GCP resources created by the deployment scripts
#
# This script is DESTRUCTIVE and IRREVERSIBLE.
# It deletes:
#   - All 13 Cloud Run services
#   - The Managed Kafka cluster (all topics and data)
#   - The Cloud SQL instance (all billing/analytics data, if not protected)
#   - The Artifact Registry repository (all Docker images)
#   - The VPC connector
#   - The Cloud Run service account
#
# Cloud SQL deletion protection must be disabled before the instance can be
# dropped.  This script will prompt before doing so.
#
# Usage:
#   source scripts/gcp/config.env && ./scripts/gcp/teardown.sh [--yes]
#
# --yes skips the confirmation prompt (for CI use only).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/config.env"

SKIP_CONFIRM=false
for arg in "$@"; do
  [[ "$arg" == "--yes" ]] && SKIP_CONFIRM=true
done

if [[ "${SKIP_CONFIRM}" == false ]]; then
  echo "╔══════════════════════════════════════════════════════════════════════╗"
  echo "║  WARNING: This will permanently delete all EV platform GCP          ║"
  echo "║  resources including databases (Cloud SQL) and message data (Kafka). ║"
  echo "╚══════════════════════════════════════════════════════════════════════╝"
  echo ""
  read -r -p "Type the project ID to confirm deletion [${GCP_PROJECT_ID}]: " CONFIRM
  if [[ "${CONFIRM}" != "${GCP_PROJECT_ID}" ]]; then
    echo "Confirmation mismatch — aborting."
    exit 1
  fi
fi

# ── 1. Delete Cloud Run services ──────────────────────────────────────────────
echo ""
echo "==> Deleting Cloud Run services..."

CR_SERVICES=(
  ev-auth-service ev-ocpp-gateway ev-ocpp-processor ev-rest-api
  ev-billing-service ev-pricing-service ev-smart-charging ev-notification
  ev-roaming ev-asset ev-car ev-scheduler ev-analytics
)

for svc in "${CR_SERVICES[@]}"; do
  if gcloud run services describe "${svc}" \
       --region="${CR_REGION}" \
       --project="${GCP_PROJECT_ID}" >/dev/null 2>&1; then
    gcloud run services delete "${svc}" \
      --region="${CR_REGION}" \
      --project="${GCP_PROJECT_ID}" \
      --quiet
    echo "    Deleted: ${svc}"
  else
    echo "    Not found (already deleted): ${svc}"
  fi
done

# ── 2. Delete Managed Kafka cluster ──────────────────────────────────────────
echo ""
echo "==> Deleting Managed Kafka cluster: ${KAFKA_CLUSTER_ID}"

if gcloud managed-kafka clusters describe "${KAFKA_CLUSTER_ID}" \
     --location="${GCP_REGION}" \
     --project="${GCP_PROJECT_ID}" >/dev/null 2>&1; then
  gcloud managed-kafka clusters delete "${KAFKA_CLUSTER_ID}" \
    --location="${GCP_REGION}" \
    --project="${GCP_PROJECT_ID}" \
    --quiet
  echo "    Kafka cluster deletion initiated."
else
  echo "    Kafka cluster not found."
fi

# ── 3. Delete Cloud SQL instance ─────────────────────────────────────────────
echo ""
echo "==> Deleting Cloud SQL instance: ${CLOUDSQL_INSTANCE}"

if gcloud sql instances describe "${CLOUDSQL_INSTANCE}" \
     --project="${GCP_PROJECT_ID}" >/dev/null 2>&1; then
  # Disable deletion protection first
  gcloud sql instances patch "${CLOUDSQL_INSTANCE}" \
    --project="${GCP_PROJECT_ID}" \
    --no-deletion-protection \
    --quiet

  gcloud sql instances delete "${CLOUDSQL_INSTANCE}" \
    --project="${GCP_PROJECT_ID}" \
    --quiet
  echo "    Cloud SQL instance deleted."
else
  echo "    Cloud SQL instance not found."
fi

# ── 4. Delete VPC connector ───────────────────────────────────────────────────
echo ""
echo "==> Deleting VPC connector: ${VPC_CONNECTOR_NAME}"

if gcloud compute networks vpc-access connectors describe "${VPC_CONNECTOR_NAME}" \
     --region="${GCP_REGION}" \
     --project="${GCP_PROJECT_ID}" >/dev/null 2>&1; then
  gcloud compute networks vpc-access connectors delete "${VPC_CONNECTOR_NAME}" \
    --region="${GCP_REGION}" \
    --project="${GCP_PROJECT_ID}" \
    --quiet
  echo "    VPC connector deleted."
else
  echo "    VPC connector not found."
fi

# ── 5. Delete Artifact Registry repository ────────────────────────────────────
echo ""
echo "==> Deleting Artifact Registry repository: ${AR_REPO}"

if gcloud artifacts repositories describe "${AR_REPO}" \
     --location="${GCP_REGION}" \
     --project="${GCP_PROJECT_ID}" >/dev/null 2>&1; then
  gcloud artifacts repositories delete "${AR_REPO}" \
    --location="${GCP_REGION}" \
    --project="${GCP_PROJECT_ID}" \
    --quiet
  echo "    Artifact Registry repository deleted."
else
  echo "    Repository not found."
fi

# ── 6. Delete service account ─────────────────────────────────────────────────
echo ""
echo "==> Deleting service account: ${SA_EMAIL}"

if gcloud iam service-accounts describe "${SA_EMAIL}" \
     --project="${GCP_PROJECT_ID}" >/dev/null 2>&1; then
  gcloud iam service-accounts delete "${SA_EMAIL}" \
    --project="${GCP_PROJECT_ID}" \
    --quiet
  echo "    Service account deleted."
else
  echo "    Service account not found."
fi

# ── 7. Note: secrets are NOT deleted automatically ────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════════════════════════╗"
echo "║  Teardown complete.                                                  ║"
echo "║                                                                      ║"
echo "║  NOTE: Secret Manager secrets (ev/*) were NOT deleted.              ║"
echo "║  To delete them:                                                     ║"
echo "║    gcloud secrets list --filter='name~^ev/' --format='value(name)'  ║"
echo "║      | xargs -I{} gcloud secrets delete {} --quiet                  ║"
echo "╚══════════════════════════════════════════════════════════════════════╝"
