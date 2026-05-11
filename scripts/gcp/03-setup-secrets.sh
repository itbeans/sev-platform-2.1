#!/usr/bin/env bash
# 03-setup-secrets.sh — create GCP Secret Manager secrets
#
# Creates all Secret Manager secrets used by the Cloud Run services.
# The script creates each secret with a placeholder value.  You MUST update the
# placeholder values with real credentials before running 05-deploy.sh.
#
# After creation, update secrets with:
#   printf 'actual-value' | gcloud secrets versions add SECRET_NAME \
#     --project=PROJECT_ID --data-file=-
#
# Usage:
#   source scripts/gcp/config.env && ./scripts/gcp/03-setup-secrets.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/config.env"

# ── Helper ────────────────────────────────────────────────────────────────────
_create_secret() {
  local name="$1" placeholder="$2"

  if gcloud secrets describe "${name}" --project="${GCP_PROJECT_ID}" >/dev/null 2>&1; then
    echo "    [exists]  ${name}"
    return
  fi

  gcloud secrets create "${name}" \
    --project="${GCP_PROJECT_ID}" \
    --replication-policy=user-managed \
    --locations="${GCP_REGION}"

  printf '%s' "${placeholder}" | gcloud secrets versions add "${name}" \
    --project="${GCP_PROJECT_ID}" \
    --data-file=-

  echo "    [created] ${name}"
}

echo "==> Creating Secret Manager secrets in project: ${GCP_PROJECT_ID}"
echo ""
echo "    Secrets containing CHANGE_ME must be updated before deploying."
echo ""

# ── Shared / cross-service secrets ───────────────────────────────────────────

# MongoDB Atlas connection string.
# Format: mongodb+srv://user:password@cluster.mongodb.net/evserver?retryWrites=true
_create_secret "${SM_MONGO_URI}" \
  "CHANGE_ME: mongodb+srv://user:password@cluster.mongodb.net/${MONGO_DB_NAME}?retryWrites=true"

# JWT signing secret — must be identical across auth-service, ocpp-gateway, rest-api.
_create_secret "${SM_JWT_SECRET}" \
  "CHANGE_ME: $(openssl rand -hex 32)"

# Kafka bootstrap address (updated automatically by 02-provision-infra.sh)
_create_secret "${SM_KAFKA_BOOTSTRAP}" \
  "CHANGE_ME: managed-kafka-bootstrap-address:9092"

# Kafka SASL config for GCP Managed Kafka
# GCP Managed Kafka requires OAUTHBEARER + Google credentials.
_create_secret "${SM_KAFKA_SASL_CONFIG}" \
  "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;"

# ── billing-service ───────────────────────────────────────────────────────────
_create_secret "${SM_BILLING_JDBC_PASSWORD}" \
  "CHANGE_ME: generated-by-02-provision-infra"

_create_secret "${SM_STRIPE_SECRET_KEY}" \
  "CHANGE_ME: sk_live_xxxx"

_create_secret "${SM_STRIPE_WEBHOOK_SECRET}" \
  "CHANGE_ME: whsec_xxxx"

# ── analytics ─────────────────────────────────────────────────────────────────
_create_secret "${SM_ANALYTICS_JDBC_PASSWORD}" \
  "CHANGE_ME: generated-by-02-provision-infra"

# ── notification ──────────────────────────────────────────────────────────────
_create_secret "${SM_SMTP_PASSWORD}" \
  "CHANGE_ME: smtp-app-password"

_create_secret "${SM_FCM_SERVICE_ACCOUNT_JSON}" \
  'CHANGE_ME: paste Firebase service account JSON here'

# ── roaming ───────────────────────────────────────────────────────────────────
_create_secret "${SM_OCPI_LOCAL_TOKEN}" \
  "CHANGE_ME: $(openssl rand -hex 20)"

# ── smart-charging ────────────────────────────────────────────────────────────
_create_secret "${SM_SAP_OPTIMIZER_PASSWORD}" \
  "CHANGE_ME: sap-optimizer-password"

# ── IAM: let the Cloud Run service account read all secrets ──────────────────
echo ""
echo "==> Granting secretAccessor to ${SA_EMAIL} for all ev/* secrets..."

ALL_SECRETS=(
  "${SM_MONGO_URI}"
  "${SM_JWT_SECRET}"
  "${SM_KAFKA_BOOTSTRAP}"
  "${SM_KAFKA_SASL_CONFIG}"
  "${SM_BILLING_JDBC_PASSWORD}"
  "${SM_STRIPE_SECRET_KEY}"
  "${SM_STRIPE_WEBHOOK_SECRET}"
  "${SM_ANALYTICS_JDBC_PASSWORD}"
  "${SM_SMTP_PASSWORD}"
  "${SM_FCM_SERVICE_ACCOUNT_JSON}"
  "${SM_OCPI_LOCAL_TOKEN}"
  "${SM_SAP_OPTIMIZER_PASSWORD}"
)

for secret in "${ALL_SECRETS[@]}"; do
  gcloud secrets add-iam-policy-binding "${secret}" \
    --project="${GCP_PROJECT_ID}" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="roles/secretmanager.secretAccessor" \
    --quiet
done

echo ""
echo "╔══════════════════════════════════════════════════════════════════════╗"
echo "║  Secrets created.                                                    ║"
echo "║                                                                      ║"
echo "║  ACTION REQUIRED — update every CHANGE_ME secret before deploying:  ║"
echo "║                                                                      ║"
echo "║  printf 'VALUE' | gcloud secrets versions add SECRET_NAME \\         ║"
echo "║    --project=${GCP_PROJECT_ID} --data-file=-                         ║"
echo "║                                                                      ║"
echo "║  Critical secrets to update:                                         ║"
echo "║    ev/mongo-uri               (MongoDB Atlas connection string)      ║"
echo "║    ev/jwt-secret              (strong random value)                  ║"
echo "║    ev/stripe-secret-key       (Stripe live key)                      ║"
echo "║    ev/stripe-webhook-secret   (Stripe webhook signing secret)        ║"
echo "║    ev/smtp-password           (SMTP app password)                    ║"
echo "║    ev/fcm-service-account-json (Firebase service account JSON)       ║"
echo "╚══════════════════════════════════════════════════════════════════════╝"
