#!/usr/bin/env bash
# 02-provision-infra.sh — provision GCP infrastructure
#
# Creates:
#   - Artifact Registry repository (Docker image storage)
#   - Cloud SQL for PostgreSQL (billing + analytics databases)
#   - GCP Managed Service for Apache Kafka cluster + topics
#
# Run once per environment.  The script is idempotent: existing resources are
# detected and skipped.
#
# Prerequisites:
#   - 01-setup-project.sh completed successfully
#   - gcloud authenticated with project owner/editor role
#
# Usage:
#   source scripts/gcp/config.env && ./scripts/gcp/02-provision-infra.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/config.env"

# ── 1. Artifact Registry ──────────────────────────────────────────────────────
echo "==> Creating Artifact Registry repository: ${AR_REPO}"

if gcloud artifacts repositories describe "${AR_REPO}" \
     --location="${GCP_REGION}" --project="${GCP_PROJECT_ID}" >/dev/null 2>&1; then
  echo "    Repository already exists — skipping."
else
  gcloud artifacts repositories create "${AR_REPO}" \
    --project="${GCP_PROJECT_ID}" \
    --location="${GCP_REGION}" \
    --repository-format=docker \
    --description="EV platform container images"
fi

# Allow the Cloud Run service account to pull images
gcloud artifacts repositories add-iam-policy-binding "${AR_REPO}" \
  --project="${GCP_PROJECT_ID}" \
  --location="${GCP_REGION}" \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/artifactregistry.reader" \
  --quiet

echo "==> Artifact Registry: ${IMAGE_PREFIX}"

# ── 2. Cloud SQL for PostgreSQL ───────────────────────────────────────────────
echo "==> Provisioning Cloud SQL instance: ${CLOUDSQL_INSTANCE}"

if gcloud sql instances describe "${CLOUDSQL_INSTANCE}" \
     --project="${GCP_PROJECT_ID}" >/dev/null 2>&1; then
  echo "    Cloud SQL instance already exists — skipping creation."
else
  gcloud sql instances create "${CLOUDSQL_INSTANCE}" \
    --project="${GCP_PROJECT_ID}" \
    --region="${GCP_REGION}" \
    --database-version=POSTGRES_16 \
    --tier="${CLOUDSQL_TIER}" \
    --storage-type=SSD \
    --storage-size=20GB \
    --storage-auto-increase \
    --no-assign-ip \
    --network=default \
    --retained-backups-count=7 \
    --enable-point-in-time-recovery \
    --deletion-protection
  echo "    Cloud SQL instance created."
fi

echo "==> Creating databases and users..."

_sql_user_exists() {
  local user="$1" db="$2"
  gcloud sql users list \
    --instance="${CLOUDSQL_INSTANCE}" \
    --project="${GCP_PROJECT_ID}" \
    --filter="name=${user}" \
    --format="value(name)" 2>/dev/null | grep -q "${user}"
}

_sql_db_exists() {
  local db="$1"
  gcloud sql databases list \
    --instance="${CLOUDSQL_INSTANCE}" \
    --project="${GCP_PROJECT_ID}" \
    --filter="name=${db}" \
    --format="value(name)" 2>/dev/null | grep -q "${db}"
}

# Billing database
if ! _sql_db_exists "${BILLING_DB}"; then
  gcloud sql databases create "${BILLING_DB}" \
    --instance="${CLOUDSQL_INSTANCE}" \
    --project="${GCP_PROJECT_ID}"
fi

if ! _sql_user_exists "${BILLING_USER}" "${BILLING_DB}"; then
  # Password is set from Secret Manager in 03-setup-secrets.sh then rotated here
  BILLING_PASS=$(openssl rand -base64 24)
  gcloud sql users create "${BILLING_USER}" \
    --instance="${CLOUDSQL_INSTANCE}" \
    --project="${GCP_PROJECT_ID}" \
    --password="${BILLING_PASS}"
  # Store the generated password into Secret Manager immediately
  printf '%s' "${BILLING_PASS}" | gcloud secrets versions add "${SM_BILLING_JDBC_PASSWORD}" \
    --project="${GCP_PROJECT_ID}" \
    --data-file=- 2>/dev/null \
    || echo "    Secret ${SM_BILLING_JDBC_PASSWORD} not yet created — run 03-setup-secrets.sh first, then re-run this script."
fi

# Analytics database (standard PostgreSQL — TimescaleDB extension not available
# on Cloud SQL; see README for GCE-based TimescaleDB alternative)
if ! _sql_db_exists "${ANALYTICS_DB}"; then
  gcloud sql databases create "${ANALYTICS_DB}" \
    --instance="${CLOUDSQL_INSTANCE}" \
    --project="${GCP_PROJECT_ID}"
fi

if ! _sql_user_exists "${ANALYTICS_USER}" "${ANALYTICS_DB}"; then
  ANALYTICS_PASS=$(openssl rand -base64 24)
  gcloud sql users create "${ANALYTICS_USER}" \
    --instance="${CLOUDSQL_INSTANCE}" \
    --project="${GCP_PROJECT_ID}" \
    --password="${ANALYTICS_PASS}"
  printf '%s' "${ANALYTICS_PASS}" | gcloud secrets versions add "${SM_ANALYTICS_JDBC_PASSWORD}" \
    --project="${GCP_PROJECT_ID}" \
    --data-file=- 2>/dev/null \
    || echo "    Secret ${SM_ANALYTICS_JDBC_PASSWORD} not yet created — run 03-setup-secrets.sh first, then re-run this script."
fi

echo "==> Cloud SQL ready: ${CLOUDSQL_CONNECTION_NAME}"

# ── 3. GCP Managed Service for Apache Kafka ───────────────────────────────────
echo "==> Provisioning Managed Kafka cluster: ${KAFKA_CLUSTER_ID}"

# Dedicated /28 subnet for the Kafka cluster bootstrap brokers
if ! gcloud compute networks subnets describe "${KAFKA_CLUSTER_SUBNET}" \
       --region="${GCP_REGION}" --project="${GCP_PROJECT_ID}" >/dev/null 2>&1; then
  gcloud compute networks subnets create "${KAFKA_CLUSTER_SUBNET}" \
    --project="${GCP_PROJECT_ID}" \
    --region="${GCP_REGION}" \
    --network=default \
    --range=10.9.0.0/28
fi

if gcloud managed-kafka clusters describe "${KAFKA_CLUSTER_ID}" \
     --location="${GCP_REGION}" --project="${GCP_PROJECT_ID}" >/dev/null 2>&1; then
  echo "    Kafka cluster already exists — skipping creation."
else
  gcloud managed-kafka clusters create "${KAFKA_CLUSTER_ID}" \
    --project="${GCP_PROJECT_ID}" \
    --location="${GCP_REGION}" \
    --subnets="projects/${GCP_PROJECT_ID}/regions/${GCP_REGION}/subnetworks/${KAFKA_CLUSTER_SUBNET}" \
    --cpu=3 \
    --memory=3221225472 \
    --auto-rebalance-config-min-partition-transfer-speed-per-broker=1MB

  echo -n "    Waiting for Kafka cluster to be active"
  until gcloud managed-kafka clusters describe "${KAFKA_CLUSTER_ID}" \
          --location="${GCP_REGION}" --project="${GCP_PROJECT_ID}" \
          --format="value(state)" 2>/dev/null | grep -q "ACTIVE"; do
    echo -n "."
    sleep 10
  done
  echo " OK"
fi

# Create Kafka topics
echo "==> Creating Kafka topics..."
for topic in "${KAFKA_TOPICS[@]}"; do
  if gcloud managed-kafka topics describe "${topic}" \
       --cluster="${KAFKA_CLUSTER_ID}" \
       --location="${GCP_REGION}" \
       --project="${GCP_PROJECT_ID}" >/dev/null 2>&1; then
    echo "    Topic '${topic}' already exists."
  else
    gcloud managed-kafka topics create "${topic}" \
      --cluster="${KAFKA_CLUSTER_ID}" \
      --location="${GCP_REGION}" \
      --project="${GCP_PROJECT_ID}" \
      --partitions=6 \
      --replication-factor="${KAFKA_REPLICATION_FACTOR}"
    echo "    Created topic: ${topic}"
  fi
done

# Retrieve and store the bootstrap server address in Secret Manager
KAFKA_BOOTSTRAP_ADDR=$(gcloud managed-kafka clusters describe "${KAFKA_CLUSTER_ID}" \
  --location="${GCP_REGION}" --project="${GCP_PROJECT_ID}" \
  --format="value(bootstrapAddress)" 2>/dev/null || echo "${KAFKA_BOOTSTRAP}")

printf '%s' "${KAFKA_BOOTSTRAP_ADDR}" | gcloud secrets versions add "${SM_KAFKA_BOOTSTRAP}" \
  --project="${GCP_PROJECT_ID}" \
  --data-file=- 2>/dev/null \
  || echo "    Run 03-setup-secrets.sh first to create the secret, then re-run."

# Kafka SASL config for GCP Managed Kafka (uses GCP OAuth2 token-based auth)
KAFKA_SASL_CONFIG='org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;'
printf '%s' "${KAFKA_SASL_CONFIG}" | gcloud secrets versions add "${SM_KAFKA_SASL_CONFIG}" \
  --project="${GCP_PROJECT_ID}" \
  --data-file=- 2>/dev/null || true

echo "==> Managed Kafka cluster: ${KAFKA_CLUSTER_ID} (${KAFKA_BOOTSTRAP_ADDR})"

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
echo "╔════════════════════════════════════════════════════════════════╗"
echo "║  Infrastructure provisioned — proceed to 03-setup-secrets.sh ║"
echo "╠════════════════════════════════════════════════════════════════╣"
printf "║  Artifact Registry  %-42s ║\n" "${IMAGE_PREFIX}"
printf "║  Cloud SQL          %-42s ║\n" "${CLOUDSQL_CONNECTION_NAME}"
printf "║  Kafka cluster      %-42s ║\n" "${KAFKA_CLUSTER_ID}"
echo "╚════════════════════════════════════════════════════════════════╝"
