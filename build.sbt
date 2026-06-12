import Dependencies._
import com.typesafe.sbt.packager.docker.DockerPlugin.autoImport._

// ── Global settings ──────────────────────────────────────────────────────────

ThisBuild / scalaVersion       := Versions.scala
ThisBuild / organization       := "io.itbeans"
ThisBuild / organizationName   := "itbeans"
ThisBuild / version            := "0.1.0-SNAPSHOT"
ThisBuild / scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked",
  "-Xfatal-warnings",
  "-Yretain-trees",        // Required for ZIO Schema macro derivation
  "-Xmax-inlines:64",     // Required for circe deriveEncoder on large case classes
)

ThisBuild / testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")

ThisBuild / coverageMinimumStmtTotal  := 70
ThisBuild / coverageFailOnMinimum     := true
ThisBuild / coverageExcludedPackages  := "io\\.itbeans\\.ev\\.proto\\..*;.*\\.Main;.*Config"

// Enable semantic DB for scalafix
ThisBuild / semanticdbEnabled := true
ThisBuild / semanticdbVersion := scalafixSemanticdb.revision


// ── Shared module settings ────────────────────────────────────────────────────

lazy val commonSettings = Seq(
  scalafmtOnCompile := true,
)

// ── Shared libraries ──────────────────────────────────────────────────────────

// Domain types: pure case classes, no ZIO/HTTP/DB dependencies
lazy val domain = project
  .in(file("modules/domain"))
  .settings(commonSettings)
  .settings(
    name := "ev-domain",
    libraryDependencies ++= domainDeps,
  )

// In-process RBAC/ABAC — NOT a network service
lazy val authCore = project
  .in(file("modules/auth-core"))
  .settings(commonSettings)
  .settings(
    name := "ev-auth-core",
    libraryDependencies ++= authCoreDeps,
  )
  .dependsOn(domain)

// MongoDB ZIO wrapper with tenant-aware collection routing
lazy val mongoZio = project
  .in(file("modules/mongo-zio"))
  .settings(commonSettings)
  .settings(
    name := "ev-mongo-zio",
    libraryDependencies ++= mongoZioDeps,
  )
  .dependsOn(domain)

// Kafka producers/consumers with tenant-keyed topic routing
lazy val kafkaZio = project
  .in(file("modules/kafka-zio"))
  .settings(commonSettings)
  .settings(
    name := "ev-kafka-zio",
    libraryDependencies ++= kafkaZioDeps,
  )
  .dependsOn(domain)

// OpenTelemetry instrumentation: EvTracing service, OtelLayer, OtelConfig
lazy val otelZio = project
  .in(file("modules/otel-zio"))
  .settings(commonSettings)
  .settings(
    name := "ev-otel-zio",
    libraryDependencies ++= otelZioDeps,
  )

// Proto definitions (code-generated — consumed by services that use gRPC)
lazy val proto = project
  .in(file("modules/proto"))
  .settings(commonSettings)
  .settings(
    name := "ev-proto",
    libraryDependencies ++= grpcDeps,
    Compile / PB.protoSources := Seq(baseDirectory.value / "src" / "main" / "protobuf"),
    Compile / PB.targets := Seq(
      scalapb.gen(grpc = true) -> (Compile / sourceManaged).value / "scalapb"
    ),
  )
  .dependsOn(domain)

// ── Microservices ─────────────────────────────────────────────────────────────

def serviceProject(id: String, dir: String) =
  Project(id, file(s"modules/services/$dir"))
    .settings(commonSettings)
    .settings(
      name := s"ev-$id",
      libraryDependencies ++= serviceBaseDeps,
      // Docker image settings
      Docker / packageName := s"ev-$id",
      Docker / version     := version.value,
      dockerBaseImage      := "eclipse-temurin:21-jre-alpine",
      dockerExposedPorts   := Seq(8080, 9090),
      dockerUpdateLatest   := true,
    )
    .enablePlugins(JavaAppPackaging, DockerPlugin)
    .dependsOn(domain, authCore, mongoZio, kafkaZio, otelZio)

// OCPP 2.1 WebSocket gateway — manages all station connections
lazy val ocppGateway = serviceProject("ocpp-gateway", "ocpp-gateway")
  .settings(
    libraryDependencies ++= grpcDeps,
    dockerExposedPorts := Seq(8010, 9090, 8080),  // WS, gRPC, health
  )
  .dependsOn(proto)

// OCPP 2.1 business logic processor (Kafka consumer)
lazy val ocppProcessor = serviceProject("ocpp-processor", "ocpp-processor")
  .settings(libraryDependencies ++= grpcDeps)
  .dependsOn(proto)

// Dashboard REST API (Tapir endpoints)
lazy val restApi = serviceProject("rest-api", "rest-api")
  .settings(
    libraryDependencies ++= grpcDeps ++ Seq(
      pact4sZioTest % Test,
      pact4sCirce   % Test,
    ),
    dockerExposedPorts := Seq(80, 443, 9090),
  )
  .dependsOn(proto)

// JWT issuance & token validation service
lazy val authService = serviceProject("auth-service", "auth-service")
  .settings(
    libraryDependencies ++= grpcDeps ++ Seq(jwtCirce, bcrypt),
    dockerExposedPorts := Seq(8080, 9090),
  )
  .dependsOn(proto)

// Stripe billing integration
lazy val billingService = serviceProject("billing-service", "billing-service")
  .settings(
    libraryDependencies ++= grpcDeps ++ Seq(
      doobieCore, doobiePostgres, doobieHikari,
      pact4sZioTest % Test,
      pact4sCirce   % Test,
    ),
    dockerExposedPorts := Seq(8080, 9090),
  )
  .dependsOn(proto)

// Tariff engine — gRPC server
lazy val pricingService = serviceProject("pricing-service", "pricing-service")
  .settings(
    libraryDependencies ++= grpcDeps,
    dockerExposedPorts := Seq(9090, 8080),
  )
  .dependsOn(proto)

// SAP Smart Charging + OCPP 2.1 DER Control
lazy val smartCharging = serviceProject("smart-charging", "smart-charging")
  .settings(libraryDependencies ++= grpcDeps)
  .dependsOn(proto)

// Email + Firebase push notifications (Kafka consumer only)
lazy val notification = serviceProject("notification", "notification")
  .settings(libraryDependencies ++= Seq(jakartaMail, jwtCirce))

// OCPI 2.1.1 + OICP 2.3.0 roaming
lazy val roaming = serviceProject("roaming", "roaming")

// Asset consumption polling (solar, battery, buildings)
lazy val asset = serviceProject("asset", "asset")

// Car catalog + OCPP 2.1 V2X car connector integrations
lazy val car = serviceProject("car", "car")
  .settings(libraryDependencies ++= grpcDeps)
  .dependsOn(proto)

// Cron tasks + async job orchestration
lazy val scheduler = serviceProject("scheduler", "scheduler")

// Audit logs + statistics (TimescaleDB)
lazy val analytics = serviceProject("analytics", "analytics")
  .settings(
    libraryDependencies ++= Seq(doobieCore, doobiePostgres, doobieHikari),
  )

// ── Billing parity test (standalone, no Docker required) ──────────────────────

lazy val billingParity = project
  .in(file("tests/billing-parity"))
  .settings(commonSettings)
  .settings(
    name := "ev-billing-parity",
    publish / skip := true,
    libraryDependencies ++= Seq(
      zio, circeCore, circeGeneric, circeParser,
      zioTest % Test, zioTestSbt % Test,
    ),
  )
  .dependsOn(domain)

// ── Root aggregate (build all) ─────────────────────────────────────────────────

lazy val root = project
  .in(file("."))
  .settings(
    name := "ev-server-scala",
    publish / skip := true,
  )
  .aggregate(
    domain, authCore, mongoZio, kafkaZio, otelZio, proto,
    ocppGateway, ocppProcessor, restApi, authService,
    billingService, pricingService, smartCharging,
    notification, roaming, asset, car, scheduler, analytics,
    billingParity,
  )
