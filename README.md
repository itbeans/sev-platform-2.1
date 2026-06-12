# sev-platform-2.1

Scala 3 / ZIO 2 microservices backend for EV charging infrastructure. Strangler-fig migration from a TypeScript monolith — 13 services running in parallel with gradual Kong traffic cutover.

---

## Architecture

```
                        Kong API Gateway (ports 80/443)
                               │
          ┌────────────────────┼──────────────────────┐
          │                    │                       │
   ev-rest-api           ev-ocpp-gateway         ev-roaming
   (Tapir HTTP)         (WebSocket OCPP)        (OCPI/OICP)
          │                    │                       │
          └────────────┬───────┘                       │
                       │ Kafka                         │
              ev-ocpp-processor ──────────────────────►│
                       │
          ┌────────────┼───────────────────────────────┐
          │            │            │                   │
   ev-billing   ev-pricing   ev-smart-charging   ev-notification
   (Stripe)     (tariffs)    (SAP/DER)           (email/FCM)
          │
   ev-analytics  ev-auth-service  ev-car  ev-asset  ev-scheduler
   (TimescaleDB) (JWT/RBAC)
```

**Data stores:** MongoDB 7 (all services) · PostgreSQL 16 + TimescaleDB (billing, analytics) · Kafka 3.8 KRaft  
**Service mesh:** Istio mTLS · canary VirtualServices · per-service AuthorizationPolicy  
**GitOps:** ArgoCD App-of-Apps · 13 Application CRs · `image-tags.yaml` auto-updated by CI

---

## Services

| Service | sbt ID | Description | Port(s) |
|---|---|---|---|
| ev-ocpp-gateway | `ocppGateway` | OCPP 1.6/2.0.1/2.1 WebSocket gateway | 8010 (WS), 9090 (gRPC), 8080 (health) |
| ev-ocpp-processor | `ocppProcessor` | OCPP business logic (Kafka consumer) | — |
| ev-rest-api | `restApi` | Dashboard REST API (Tapir) | 80/443, 9090 |
| ev-auth-service | `authService` | JWT issuance + RBAC/ABAC | 8080, 9090 |
| ev-billing-service | `billingService` | Stripe integration | 8080 |
| ev-pricing-service | `pricingService` | Tariff engine (gRPC server) | 9090, 8080 |
| ev-smart-charging | `smartCharging` | SAP Smart Charging + OCPP 2.1 DER | 8080, 9090 |
| ev-notification | `notification` | Email (Jakarta Mail) + Firebase FCM | — |
| ev-roaming | `roaming` | OCPI 2.1.1 + OICP 2.3.0 | 8080 |
| ev-asset | `asset` | Asset consumption polling | 8080 |
| ev-car | `car` | Car catalog + V2X connectors | 8080, 9090 |
| ev-scheduler | `scheduler` | Cron tasks + async jobs | — |
| ev-analytics | `analytics` | Audit logs + statistics (TimescaleDB) | 8080 |

All services expose `/metrics` on port **8888** (Prometheus) and are instrumented with OpenTelemetry.

---

## OCPP Support

| Version | Transport | Status |
|---|---|---|
| OCPP 2.1 | WebSocket JSON | ✅ Full (V2G/DER, BatterySwap, AFRRSignal) |
| OCPP 2.0.1 | WebSocket JSON | ✅ Full |
| OCPP 1.6 | WebSocket JSON | ✅ Full (BootNotification, Heartbeat, Authorize, StartTransaction, StopTransaction, MeterValues, StatusNotification, DataTransfer, FirmwareStatus, DiagnosticsStatus) |
| OCPP 1.6 | SOAP/HTTP | ⚠️ TypeScript bridge only — not implemented in Scala |

Chargers connect via:
```
wss://<host>/ocpp/2.1/<stationId>
wss://<host>/ocpp/2.0.1/<stationId>
wss://<host>/ocpp/1.6/<stationId>
```

---

## Shared Modules

| Module | Description |
|---|---|
| `ev-domain` | Core entities (pure case classes, no framework deps) |
| `ev-auth-core` | Casbin RBAC/ABAC — 369 rules, 6 roles, 40+ resources |
| `ev-mongo-zio` | Tenant-aware MongoDB wrapper (`{tenantId}.{collection}`) |
| `ev-kafka-zio` | Producer/consumer with tenant-keyed topic routing |
| `ev-otel-zio` | OpenTelemetry instrumentation layer |
| `ev-proto` | ScalaPB generated gRPC stubs |

---

## Quick Start

### Prerequisites

- JDK 21
- Docker
- sbt

### Start local infrastructure

```bash
docker compose -f docker/docker-compose-local.yml up -d mongo timescaledb kafka
```

| Service | Port |
|---|---|
| MongoDB 7 | 27017 |
| TimescaleDB (PostgreSQL 16) | 5432 (user: `ev`, pass: `ev_local_secret`) |
| Kafka KRaft | 9092 |
| Kafka UI | 8090 |
| Kong (proxy / admin) | 80, 8001 |
| Jaeger UI | 16686 |
| Grafana | 3000 (admin/admin) |
| Prometheus | 9090 |

### Build and test

```bash
sbt compile                          # type-check all modules
sbt test                             # run all 20 specs
sbt coverage test coverageAggregate  # with ≥70% coverage enforcement
sbt scalafmtCheckAll                 # lint
```

### Run a single service locally

```bash
sbt ocppGateway/reStart   # hot-reload via sbt-revolver
sbt restApi/reStart
```

---

## Testing

20 ZIO Test specs. Most run with no external dependencies (in-memory stubs).

| Spec | Module | Needs infra? |
|---|---|---|
| CasbinAuthorizationServiceSpec | auth-core | No |
| RbacMatrixSpec | auth-core | No |
| EvKafkaProducerSpec | kafka-zio | Testcontainers (auto) |
| TenantCollectionSpec | mongo-zio | No |
| OcppGatewaySpec | ocpp-gateway | No |
| OcppProcessorSpec | ocpp-processor | No |
| RestApiSpec | rest-api | No |
| AuthServicePactSpec | rest-api | No |
| AuthServiceSpec | auth-service | No |
| BillingServiceSpec | billing-service | TimescaleDB |
| TransactionLifecyclePactSpec | billing-service | No |
| PricingServiceSpec | pricing-service | No |
| SmartChargingSpec | smart-charging | No |
| NotificationServiceSpec | notification | No |
| RoamingServiceSpec | roaming | No |
| AssetServiceSpec | asset | No |
| CarServiceSpec | car | No |
| SchedulerSpec | scheduler | No |
| AnalyticsSpec | analytics | TimescaleDB |
| BillingParitySpec | billing-parity | No |

Run a single spec:

```bash
sbt "testOnly *BillingParitySpec"
sbt authService/test
```

---

## CI/CD

GitHub Actions (`.github/workflows/ci.yml`) — 4 jobs on every push/PR:

1. **lint** — `sbt scalafmtCheckAll`
2. **test** — `sbt coverage test coverageAggregate` with MongoDB + TimescaleDB containers
3. **docker** — matrix build of all 13 services, pushed to GHCR as `ghcr.io/<owner>/ev-<service>:<sha>`
4. **update-image-tags** — commits updated `argocd/values/image-tags.yaml` with 8-char SHA

Docker images are tagged with the 8-character git SHA and also `latest`.

---

## Deployment

### Helm

One chart per service, all built on the `ev-common` library chart:

```bash
# Deploy a single service
helm upgrade --install ev-auth-service helm/charts/ev-auth-service \
  -f helm/charts/ev-auth-service/values.yaml \
  -n ev-server

# Deploy Istio mesh config
helm upgrade --install ev-istio helm/charts/ev-istio -n ev-server
```

### ArgoCD (GitOps)

`argocd/` contains an App-of-Apps setup:

```
argocd/
├── project.yaml          # AppProject with RBAC
├── apps/
│   ├── root.yaml         # App-of-Apps root
│   └── ev-*.yaml         # One Application CR per service
└── values/
    ├── image-tags.yaml   # Auto-updated by CI
    ├── staging.yaml      # Reduced replicas, no TLS
    └── production.yaml   # Full TLS, wildcard *.ev.example.com
```

### Canary traffic splits

```bash
scripts/canary.sh set auth 10       # 10% to Scala auth-service
scripts/canary.sh set ocpp-gateway 25
scripts/canary.sh cutover rest-api  # 100% cutover
```

---

## Data Migrations

Run once before cutting over each service. All scripts are idempotent.

```bash
# Billing: MongoDB → PostgreSQL
MONGO_URI=mongodb://... PG_URI=postgresql://... \
  scripts/migrate-billing.sh --dry-run
  scripts/migrate-billing.sh

# Consumptions: MongoDB → TimescaleDB
MONGO_URI=mongodb://... PG_URI=postgresql://... \
  scripts/migrate-consumptions.sh --dry-run
  scripts/migrate-consumptions.sh

# Audit logs: MongoDB → TimescaleDB
MONGO_URI=mongodb://... PG_URI=postgresql://... \
  scripts/migrate-logs.sh --retain-days 730
```

All scripts support `--tenant <id>` for per-tenant dry runs and `--from <date>` for incremental migration.

---

## Istio Service Mesh

The `ev-istio` Helm chart configures:

- **PeerAuthentication** — namespace-wide STRICT mTLS (port 8888 PERMISSIVE for Prometheus scraping)
- **DestinationRule** per service — ISTIO_MUTUAL TLS + outlier detection (circuit breaking)
- **VirtualService** per service — canary weight split via `canaryWeight` value (0–100)
- **AuthorizationPolicy** per service — allow-list based on the inter-service call graph
- **ServiceAccount** per service — unique SPIFFE identity for mTLS principal matching

Enable Istio injection on the namespace before deploying:

```bash
kubectl label namespace ev-server istio-injection=enabled
helm upgrade ev-istio helm/charts/ev-istio --set enabled=true -n ev-server
```

---

## Key Files

| Path | Description |
|---|---|
| `build.sbt` | sbt multi-project build (13 services + 6 shared modules) |
| `project/Dependencies.scala` | All library versions |
| `project/plugins.sbt` | sbt plugins (scoverage, scalafmt, native-packager, protoc) |
| `docker/docker-compose-local.yml` | Full local stack |
| `docker/kong/kong.yml` | Kong declarative config |
| `docker/timescaledb/init.sql` | TimescaleDB hypertable DDL |
| `helm/charts/ev-common/` | Shared Helm library chart |
| `helm/charts/ev-istio/` | Istio mesh configuration chart |
| `argocd/` | ArgoCD App-of-Apps |
| `scripts/` | Migration + canary scripts |
| `tests/billing-parity/` | Golden-dataset parity test (Scala vs TypeScript) |
| `REFACTORING_PLAN.md` | Living progress tracker (100% implementation complete) |
| `CLAUDE.md` | Claude Code session guide |
