#!/usr/bin/env bash
# 01-setup-project.sh — one-time GCP project bootstrap
#
# Enables required APIs, creates the Cloud Run service account, grants IAM
# roles, and sets up the serverless VPC connector used by Kafka consumers.
#
# Prerequisites:
#   - gcloud CLI authenticated and pointing at the target project
#     (gcloud auth login && gcloud config set project YOUR_PROJECT_ID)
#   - Owner or Editor role on the project (needed to enable APIs and set IAM)
#
# Usage:
#   source scripts/gcp/config.env && ./scripts/gcp/01-setup-project.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/config.env"

echo "==> Project : ${GCP_PROJECT_ID}"
echo "==> Region  : ${GCP_REGION}"
echo ""

# ── 1. Confirm project ────────────────────────────────────────────────────────
gcloud config set project "${GCP_PROJECT_ID}"

# ── 2. Enable required APIs ───────────────────────────────────────────────────
echo "==> Enabling GCP APIs (this may take a few minutes)..."

APIS=(
  run.googleapis.com
  artifactregistry.googleapis.com
  sqladmin.googleapis.com
  sql-component.googleapis.com
  secretmanager.googleapis.com
  vpcaccess.googleapis.com
  managedkafka.googleapis.com
  cloudresourcemanager.googleapis.com
  iam.googleapis.com
  compute.googleapis.com
  servicenetworking.googleapis.com
  logging.googleapis.com
  monitoring.googleapis.com
  cloudtrace.googleapis.com
)

gcloud services enable "${APIS[@]}"
echo "==> APIs enabled."

# ── 3. Create the Cloud Run service account ───────────────────────────────────
echo "==> Creating service account: ${SA_EMAIL}"

if gcloud iam service-accounts describe "${SA_EMAIL}" --project="${GCP_PROJECT_ID}" \
     >/dev/null 2>&1; then
  echo "    Service account already exists — skipping creation."
else
  gcloud iam service-accounts create "${SA_NAME}" \
    --project="${GCP_PROJECT_ID}" \
    --display-name="EV Platform Cloud Run identity"
fi

# ── 4. Grant IAM roles to the service account ─────────────────────────────────
echo "==> Granting IAM roles..."

ROLES=(
  roles/secretmanager.secretAccessor   # read Secret Manager secrets
  roles/cloudsql.client                # connect to Cloud SQL via connector
  roles/run.invoker                    # call other Cloud Run services (internal auth)
  roles/managedkafka.client            # produce/consume from Managed Kafka
  roles/logging.logWriter              # structured log ingestion
  roles/cloudtrace.agent               # distributed trace upload
  roles/monitoring.metricWriter        # custom metrics (Prometheus → Cloud Monitoring)
)

for role in "${ROLES[@]}"; do
  gcloud projects add-iam-policy-binding "${GCP_PROJECT_ID}" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="${role}" \
    --condition=None \
    --quiet
done

echo "==> IAM roles granted."

# ── 5. Create serverless VPC connector ───────────────────────────────────────
# Cloud Run services that need to reach GCP Managed Kafka (which runs in the
# VPC) use this connector.
echo "==> Creating VPC connector: ${VPC_CONNECTOR_NAME} (${VPC_CONNECTOR_RANGE})"

if gcloud compute networks vpc-access connectors describe "${VPC_CONNECTOR_NAME}" \
     --region="${GCP_REGION}" --project="${GCP_PROJECT_ID}" >/dev/null 2>&1; then
  echo "    VPC connector already exists — skipping."
else
  gcloud compute networks vpc-access connectors create "${VPC_CONNECTOR_NAME}" \
    --project="${GCP_PROJECT_ID}" \
    --region="${GCP_REGION}" \
    --network="${VPC_NETWORK}" \
    --range="${VPC_CONNECTOR_RANGE}" \
    --min-instances=2 \
    --max-instances=10 \
    --machine-type=e2-micro
fi

echo "==> VPC connector ready."

# ── 6. Enable private services access for Cloud SQL ──────────────────────────
# Allows Cloud SQL to allocate a private IP in the VPC (needed for the
# analytics/billing services when using the Cloud SQL Auth Proxy approach is
# not desired).  This is idempotent.
echo "==> Configuring private services access for Cloud SQL..."
gcloud compute addresses create google-managed-services-default \
  --project="${GCP_PROJECT_ID}" \
  --global \
  --purpose=VPC_PEERING \
  --addresses=10.100.0.0 \
  --prefix-length=16 \
  --network=default 2>/dev/null || echo "    Private range already reserved."

gcloud services vpc-peerings connect \
  --project="${GCP_PROJECT_ID}" \
  --service=servicenetworking.googleapis.com \
  --ranges=google-managed-services-default \
  --network=default 2>/dev/null || echo "    VPC peering already configured."

echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║  Project setup complete — proceed to 02-provision-infra.sh  ║"
echo "╚══════════════════════════════════════════════════════════════╝"
