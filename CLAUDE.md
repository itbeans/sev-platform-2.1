# ev-server-scala — Claude Code Guide

## Project overview

Scala 3 / ZIO 2 microservices platform — 13 services + 6 shared modules.
Strangler-fig migration from a TypeScript monolith. sbt multi-project build.

---

## Running tests

### Prerequisites

- JDK 21
- Docker running (for infrastructure containers)
- `sbt` on PATH
- `.jvmopts` sets `-Xmx4g` — ensure 4 GB heap available

### Start local infrastructure

```bash
docker compose -f docker/docker-compose-local.yml up -d mongo timescaledb kafka
```

| Service     | Port  | Required by                        |
|-------------|-------|------------------------------------|
| MongoDB 7   | 27017 | most services (in-memory for tests)|
| TimescaleDB | 5432  | AnalyticsSpec, BillingServiceSpec  |
| Kafka KRaft | 9092  | EvKafkaProducerSpec (testcontainers auto-spins) |

Most specs use in-memory stubs and need no running infrastructure.

### Run tests

```bash
sbt compile                        # type-check all modules
sbt test                           # all 20 specs
sbt coverage test coverageAggregate  # with coverage (≥70% enforced)
sbt scalafmtCheckAll               # lint check (mirrors CI)
```

Coverage report: `target/scala-3.3.4/scoverage-report/index.html`

### Single spec / module

```bash
sbt "testOnly *BillingParitySpec"
sbt "testOnly *AuthServiceSpec"
sbt authService/test
sbt ocppGateway/test
sbt billingParity/test
```

### Spec inventory (20 specs)

| Spec | Module | Needs infra? |
|------|--------|--------------|
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

---

## CI pipeline

`.github/workflows/ci.yml` — four jobs on every push/PR:

1. **lint** — `sbt scalafmtCheckAll`
2. **test** — `sbt coverage test coverageAggregate` (MongoDB + TimescaleDB service containers)
3. **docker** — matrix build of all 13 services → GHCR (pushes on `main` / version tags)
4. **update-image-tags** — patches `argocd/values/image-tags.yaml` with 8-char SHA

---

## Key sbt project IDs

| sbt ID | Docker image | Directory |
|--------|-------------|-----------|
| `ocppGateway` | ev-ocpp-gateway | modules/services/ocpp-gateway |
| `ocppProcessor` | ev-ocpp-processor | modules/services/ocpp-processor |
| `restApi` | ev-rest-api | modules/services/rest-api |
| `authService` | ev-auth-service | modules/services/auth-service |
| `billingService` | ev-billing-service | modules/services/billing-service |
| `pricingService` | ev-pricing-service | modules/services/pricing-service |
| `smartCharging` | ev-smart-charging | modules/services/smart-charging |
| `notification` | ev-notification | modules/services/notification |
| `roaming` | ev-roaming | modules/services/roaming |
| `asset` | ev-asset | modules/services/asset |
| `car` | ev-car | modules/services/car |
| `scheduler` | ev-scheduler | modules/services/scheduler |
| `analytics` | ev-analytics | modules/services/analytics |

---

## Useful scripts

```bash
docker compose -f docker/docker-compose-local.yml up -d    # start all infra
docker compose -f docker/docker-compose-local.yml down     # stop all infra
scripts/canary.sh set <service> <pct>                      # Kong traffic split
scripts/migrate-billing.sh --dry-run                       # billing migration dry-run
scripts/migrate-consumptions.sh --dry-run
scripts/migrate-logs.sh --retain-days 730
```

---

## Architecture notes

- **Feature branch**: `claude/codebase-review-summary-hgNgO`
- **Helm charts**: `helm/charts/` — one chart per service + `ev-common` library chart
- **Istio mesh**: `helm/charts/ev-istio/` — mTLS, canary VirtualServices, AuthorizationPolicy
- **ArgoCD**: `argocd/` — App-of-Apps with 13 Application CRs; `image-tags.yaml` auto-updated by CI
- **Data migrations**: `scripts/migrate-{billing,consumptions,logs}.sh` — idempotent, `--dry-run` safe
- **Progress**: See `REFACTORING_PLAN.md` — 100% implementation complete, ops handoff items remain
