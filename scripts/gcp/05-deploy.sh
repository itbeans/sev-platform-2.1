#!/usr/bin/env bash
# 05-deploy.sh — deploy all 13 EV platform services to Cloud Run
#
# Services are deployed in dependency order so that inter-service gRPC
# endpoints are available before downstream services start.
#
#   Tier 0 (no service deps): auth-service, pricing-service, car
#   Tier 1 (needs Tier 0):    ocpp-gateway, rest-api, billing-service,
#                              notification, roaming, asset, scheduler,
#                              analytics, smart-charging
#   Tier 2 (needs Tier 1):    ocpp-processor
#
# gRPC between Cloud Run services:
#   Cloud Run exposes HTTPS on port 443 with TLS termination.  gRPC clients
#   must connect to the Cloud Run URL on port 443 (TLS).  Services are deployed
#   with --use-http2 where they primarily serve gRPC.
#
# WebSocket (ocpp-gateway):
#   Cloud Run v2 supports WebSocket upgrades natively on port 8010.
#
# Kafka consumers (ocpp-processor, smart-charging, notification, analytics):
#   Deployed with --min-instances=1 to prevent consumer group rebalances caused
#   by scale-to-zero cold starts.
#
# Cloud SQL (billing, analytics):
#   Uses the Cloud SQL Auth Proxy sidecar via --add-cloudsql-instances and
#   the /cloudsql socket path in the JDBC URL.
#
# Usage:
#   source scripts/gcp/config.env && ./scripts/gcp/05-deploy.sh [SERVICE...]
#
# Examples:
#   ./scripts/gcp/05-deploy.sh                     # deploy all 13 services
#   ./scripts/gcp/05-deploy.sh auth-service         # re-deploy single service
#   ./scripts/gcp/05-deploy.sh auth-service pricing  # re-deploy two services

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/config.env"

# Optional: deploy only the services listed as arguments
FILTER_SERVICES=("$@")

# ── Helpers ───────────────────────────────────────────────────────────────────

_service_url() {
  # Return the Cloud Run HTTPS URL for a given service name (ev-*).
  gcloud run services describe "$1" \
    --region="${CR_REGION}" \
    --project="${GCP_PROJECT_ID}" \
    --format="value(status.url)" 2>/dev/null || echo ""
}

_grpc_host() {
  # Strip the https:// prefix — gRPC clients connect to the hostname on port 443.
  local url; url=$(_service_url "$1")
  echo "${url#https://}"
}

_should_deploy() {
  local svc="$1"
  [[ ${#FILTER_SERVICES[@]} -eq 0 ]] || [[ " ${FILTER_SERVICES[*]} " == *" ${svc} "* ]]
}

# Base Cloud Run flags shared by all services
_base_flags() {
  echo \
    --project="${GCP_PROJECT_ID}" \
    --region="${CR_REGION}" \
    --service-account="${SA_EMAIL}" \
    --vpc-connector="${VPC_CONNECTOR_NAME}" \
    --vpc-egress=all-traffic \
    --execution-environment=gen2 \
    --cpu-boost \
    --no-cpu-throttling
}

# Deploy (or update) a Cloud Run service.
# Usage: _deploy SERVICE_NAME [extra gcloud run deploy flags...]
_deploy() {
  local svc="$1"; shift
  local image="${IMAGE_PREFIX}/ev-${svc}:${IMAGE_TAG}"

  echo ""
  echo "── Deploying ev-${svc} ──────────────────────────────────────"
  gcloud run deploy "ev-${svc}" \
    --image="${image}" \
    $(_base_flags) \
    "$@"

  local url; url=$(_service_url "ev-${svc}")
  echo "   URL: ${url}"
}

# ── Secret refs ───────────────────────────────────────────────────────────────
# Format expected by --set-secrets: ENV_VAR=secret-name:version
# All services receive MONGO_URI, JWT_SECRET, and KAFKA_BOOTSTRAP.

COMMON_SECRETS=(
  "MONGO_URI=${SM_MONGO_URI}:latest"
  "KAFKA_BOOTSTRAP_SERVERS=${SM_KAFKA_BOOTSTRAP}:latest"
  "KAFKA_SASL_JAAS_CONFIG=${SM_KAFKA_SASL_CONFIG}:latest"
)

JWT_SECRET_REF="AUTH_JWT_SECRET=${SM_JWT_SECRET}:latest,JWT_SECRET=${SM_JWT_SECRET}:latest"

_common_secrets_str() {
  local IFS=","
  echo "${COMMON_SECRETS[*]}"
}

# ── Tier 0: services with no upstream service dependencies ────────────────────

if _should_deploy "auth-service"; then
  _deploy "auth-service" \
    --port=8080 \
    --use-http2 \
    --min-instances="${MIN_INSTANCES_HTTP}" \
    --max-instances="${MAX_INSTANCES_DEFAULT}" \
    --memory=512Mi \
    --cpu=1 \
    --allow-unauthenticated \
    --set-env-vars="AUTH_HTTP_PORT=8080,AUTH_GRPC_PORT=8080,MONGO_DB_NAME=${MONGO_DB_NAME},LOG_LEVEL=INFO" \
    --set-secrets="$(_common_secrets_str),${JWT_SECRET_REF},AUTH_JWT_SECRET=${SM_JWT_SECRET}:latest"
fi

if _should_deploy "pricing-service"; then
  _deploy "pricing-service" \
    --port=9090 \
    --use-http2 \
    --min-instances="${MIN_INSTANCES_HTTP}" \
    --max-instances="${MAX_INSTANCES_DEFAULT}" \
    --memory=512Mi \
    --cpu=1 \
    --no-allow-unauthenticated \
    --set-env-vars="PRICING_HTTP_PORT=9090,PRICING_GRPC_PORT=9090,MONGO_DB_NAME=${MONGO_DB_NAME}" \
    --set-secrets="$(_common_secrets_str),MONGO_URI=${SM_MONGO_URI}:latest"
fi

if _should_deploy "car"; then
  _deploy "car" \
    --port=9090 \
    --use-http2 \
    --min-instances="${MIN_INSTANCES_HTTP}" \
    --max-instances="${MAX_INSTANCES_DEFAULT}" \
    --memory=512Mi \
    --cpu=1 \
    --no-allow-unauthenticated \
    --set-env-vars="CAR_GRPC_PORT=9090,MONGO_DB_NAME=${MONGO_DB_NAME}" \
    --set-secrets="MONGO_URI=${SM_MONGO_URI}:latest"
fi

# ── Collect Tier 0 URLs ───────────────────────────────────────────────────────
AUTH_HOST=$(_grpc_host "ev-auth-service")
PRICING_HOST=$(_grpc_host "ev-pricing-service")

echo ""
echo "==> Tier 0 ready"
echo "    auth-service:    ${AUTH_HOST}"
echo "    pricing-service: ${PRICING_HOST}"

# ── Tier 1 ────────────────────────────────────────────────────────────────────

if _should_deploy "ocpp-gateway"; then
  _deploy "ocpp-gateway" \
    --port=8010 \
    --use-http2 \
    --min-instances=1 \
    --max-instances="${MAX_INSTANCES_GATEWAY}" \
    --memory=2Gi \
    --cpu=2 \
    --timeout=3600 \
    --session-affinity \
    --allow-unauthenticated \
    --set-env-vars="GATEWAY_HTTP_PORT=8010,GATEWAY_GRPC_PORT=8010,AUTH_GRPC_HOST=${AUTH_HOST},AUTH_GRPC_PORT=443,GATEWAY_RESPONSE_TIMEOUT_MS=25000,MONGO_DB_NAME=${MONGO_DB_NAME}" \
    --set-secrets="$(_common_secrets_str),${JWT_SECRET_REF}"
fi

if _should_deploy "rest-api"; then
  GW_HOST=$(_grpc_host "ev-ocpp-gateway")
  BILLING_HOST=$(_grpc_host "ev-billing-service")
  ANALYTICS_URL=$(_service_url "ev-analytics")

  _deploy "rest-api" \
    --port=8080 \
    --use-http2 \
    --min-instances="${MIN_INSTANCES_HTTP}" \
    --max-instances="${MAX_INSTANCES_DEFAULT}" \
    --memory=1Gi \
    --cpu=2 \
    --allow-unauthenticated \
    --set-env-vars="REST_API_HTTP_PORT=8080,MONGODB_DBNAME=${MONGO_DB_NAME},AUTH_GRPC_HOST=${AUTH_HOST},AUTH_GRPC_PORT=443,OCPP_GATEWAY_GRPC_HOST=${GW_HOST:-localhost},OCPP_GATEWAY_GRPC_PORT=443,BILLING_GRPC_HOST=${BILLING_HOST:-localhost},BILLING_GRPC_PORT=443,ANALYTICS_BASE_URL=${ANALYTICS_URL:-http://localhost:8090}" \
    --set-secrets="MONGODB_URI=${SM_MONGO_URI}:latest,${JWT_SECRET_REF}"
fi

if _should_deploy "billing-service"; then
  BILLING_JDBC_URL="jdbc:postgresql:///${BILLING_DB}?cloudSqlInstance=${CLOUDSQL_CONNECTION_NAME}&socketFactory=com.google.cloud.sql.postgres.SocketFactory"

  _deploy "billing-service" \
    --port=8080 \
    --use-http2 \
    --min-instances="${MIN_INSTANCES_CONSUMER}" \
    --max-instances="${MAX_INSTANCES_DEFAULT}" \
    --memory=512Mi \
    --cpu=1 \
    --no-allow-unauthenticated \
    --add-cloudsql-instances="${CLOUDSQL_CONNECTION_NAME}" \
    --set-env-vars="BILLING_HTTP_PORT=8080,BILLING_GRPC_PORT=8080,BILLING_JDBC_URL=${BILLING_JDBC_URL},BILLING_JDBC_USER=${BILLING_USER},MONGO_DB_NAME=${MONGO_DB_NAME}" \
    --set-secrets="$(_common_secrets_str),BILLING_JDBC_PASSWORD=${SM_BILLING_JDBC_PASSWORD}:latest,STRIPE_SECRET_KEY=${SM_STRIPE_SECRET_KEY}:latest,STRIPE_WEBHOOK_SECRET=${SM_STRIPE_WEBHOOK_SECRET}:latest"
fi

if _should_deploy "smart-charging"; then
  _deploy "smart-charging" \
    --port=8080 \
    --min-instances="${MIN_INSTANCES_CONSUMER}" \
    --max-instances="${MAX_INSTANCES_DEFAULT}" \
    --memory=512Mi \
    --cpu=1 \
    --no-allow-unauthenticated \
    --set-env-vars="SMART_CHARGING_HTTP_PORT=8080,MONGO_DB_NAME=${MONGO_DB_NAME}" \
    --set-secrets="$(_common_secrets_str),MONGO_URI=${SM_MONGO_URI}:latest,SAP_OPTIMIZER_PASSWORD=${SM_SAP_OPTIMIZER_PASSWORD}:latest"
fi

if _should_deploy "notification"; then
  _deploy "notification" \
    --port=8080 \
    --min-instances="${MIN_INSTANCES_CONSUMER}" \
    --max-instances="${MAX_INSTANCES_DEFAULT}" \
    --memory=512Mi \
    --cpu=1 \
    --no-allow-unauthenticated \
    --set-env-vars="MONGO_DB_NAME=${MONGO_DB_NAME},FCM_ENABLED=true,FCM_PROJECT_ID=${GCP_PROJECT_ID}" \
    --set-secrets="$(_common_secrets_str),MONGO_URI=${SM_MONGO_URI}:latest,SMTP_PASSWORD=${SM_SMTP_PASSWORD}:latest,FCM_SERVICE_ACCOUNT_JSON=${SM_FCM_SERVICE_ACCOUNT_JSON}:latest"
fi

if _should_deploy "roaming"; then
  _deploy "roaming" \
    --port=8080 \
    --min-instances="${MIN_INSTANCES_HTTP}" \
    --max-instances="${MAX_INSTANCES_DEFAULT}" \
    --memory=512Mi \
    --cpu=1 \
    --allow-unauthenticated \
    --set-env-vars="ROAMING_HTTP_PORT=8080,ROAMING_EXTERNAL_URL=https://${EXTERNAL_DOMAIN},OCPI_COUNTRY_CODE=${OCPI_COUNTRY_CODE},OCPI_PARTY_ID=${OCPI_PARTY_ID},MONGO_DB_NAME=${MONGO_DB_NAME}" \
    --set-secrets="$(_common_secrets_str),MONGO_URI=${SM_MONGO_URI}:latest,OCPI_LOCAL_TOKEN=${SM_OCPI_LOCAL_TOKEN}:latest"
fi

if _should_deploy "asset"; then
  _deploy "asset" \
    --port=8080 \
    --min-instances="${MIN_INSTANCES_HTTP}" \
    --max-instances="${MAX_INSTANCES_DEFAULT}" \
    --memory=512Mi \
    --cpu=1 \
    --no-allow-unauthenticated \
    --set-env-vars="ASSET_HTTP_PORT=8080,MONGO_DB_NAME=${MONGO_DB_NAME}" \
    --set-secrets="$(_common_secrets_str),MONGO_URI=${SM_MONGO_URI}:latest"
fi

if _should_deploy "scheduler"; then
  _deploy "scheduler" \
    --port=8080 \
    --min-instances="${MIN_INSTANCES_HTTP}" \
    --max-instances=3 \
    --memory=512Mi \
    --cpu=1 \
    --no-allow-unauthenticated \
    --set-env-vars="SCHEDULER_HTTP_PORT=8080,MONGO_DB_NAME=${MONGO_DB_NAME}" \
    --set-secrets="$(_common_secrets_str),MONGO_URI=${SM_MONGO_URI}:latest"
fi

if _should_deploy "analytics"; then
  ANALYTICS_JDBC_URL="jdbc:postgresql:///${ANALYTICS_DB}?cloudSqlInstance=${CLOUDSQL_CONNECTION_NAME}&socketFactory=com.google.cloud.sql.postgres.SocketFactory"

  _deploy "analytics" \
    --port=8080 \
    --min-instances="${MIN_INSTANCES_CONSUMER}" \
    --max-instances="${MAX_INSTANCES_DEFAULT}" \
    --memory=1Gi \
    --cpu=2 \
    --no-allow-unauthenticated \
    --add-cloudsql-instances="${CLOUDSQL_CONNECTION_NAME}" \
    --set-env-vars="ANALYTICS_HTTP_PORT=8080,ANALYTICS_JDBC_URL=${ANALYTICS_JDBC_URL},ANALYTICS_JDBC_USER=${ANALYTICS_USER},MONGO_DB_NAME=${MONGO_DB_NAME}" \
    --set-secrets="$(_common_secrets_str),ANALYTICS_JDBC_PASSWORD=${SM_ANALYTICS_JDBC_PASSWORD}:latest"
fi

# ── Tier 2: ocpp-processor (depends on auth, pricing, ocpp-gateway) ───────────
if _should_deploy "ocpp-processor"; then
  GW_HOST=$(_grpc_host "ev-ocpp-gateway")

  _deploy "ocpp-processor" \
    --port=8080 \
    --min-instances="${MIN_INSTANCES_CONSUMER}" \
    --max-instances="${MAX_INSTANCES_DEFAULT}" \
    --memory=1Gi \
    --cpu=2 \
    --no-allow-unauthenticated \
    --set-env-vars="MONGODB_DBNAME=${MONGO_DB_NAME},AUTH_GRPC_HOST=${AUTH_HOST},AUTH_GRPC_PORT=443,PRICING_GRPC_HOST=${PRICING_HOST},PRICING_GRPC_PORT=443,OCPP_GATEWAY_GRPC_HOST=${GW_HOST},OCPP_GATEWAY_GRPC_PORT=443,KAFKA_GROUP_ID=ev-ocpp-processor" \
    --set-secrets="$(_common_secrets_str),MONGODB_URI=${SM_MONGO_URI}:latest"
fi

# ── Grant internal services the right to invoke each other ───────────────────
echo ""
echo "==> Granting Cloud Run invoker permissions for internal services..."

INTERNAL_SERVICES=(
  ev-pricing-service
  ev-car
  ev-billing-service
  ev-smart-charging
  ev-notification
  ev-asset
  ev-scheduler
  ev-analytics
  ev-ocpp-processor
)

for svc in "${INTERNAL_SERVICES[@]}"; do
  gcloud run services add-iam-policy-binding "${svc}" \
    --project="${GCP_PROJECT_ID}" \
    --region="${CR_REGION}" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="roles/run.invoker" \
    --quiet 2>/dev/null || true
done

# ── Print service URLs ────────────────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════════════════════════╗"
echo "║  Deployment complete                                                 ║"
echo "╠══════════════════════════════════════════════════════════════════════╣"

ALL_SERVICES=(
  ev-auth-service ev-ocpp-gateway ev-ocpp-processor ev-rest-api
  ev-billing-service ev-pricing-service ev-smart-charging ev-notification
  ev-roaming ev-asset ev-car ev-scheduler ev-analytics
)

for svc in "${ALL_SERVICES[@]}"; do
  url=$(_service_url "${svc}" 2>/dev/null || echo "(not deployed)")
  printf "║  %-22s %s\n" "${svc}" "${url}"
done

echo "╠══════════════════════════════════════════════════════════════════════╣"
echo "║  Public endpoints:                                                   ║"
printf "║    REST API:   %-57s ║\n" "$(_service_url ev-rest-api)/api/v1"
printf "║    OCPP WS:    %-57s ║\n" "$(_service_url ev-ocpp-gateway)/ocpp/2.1/{chargePointId}"
printf "║    OCPI:       %-57s ║\n" "$(_service_url ev-roaming)/ocpi/versions"
echo "╚══════════════════════════════════════════════════════════════════════╝"
