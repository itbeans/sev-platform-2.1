import sbt._

object Dependencies {

  object Versions {
    val scala          = "3.3.4"
    val zio            = "2.1.9"
    val zioHttp        = "3.0.1"
    val zioKafka       = "2.8.1"
    val zioConfig      = "4.0.2"
    val zioLogging     = "2.3.0"
    val zioMetrics     = "2.3.1"
    val zioGrpc        = "0.6.1"
    val zioSchema      = "1.4.1"
    val tapir          = "1.11.7"
    val circe          = "0.14.10"
    val mongo          = "5.2.1"
    val doobie         = "1.0.0-RC5"
    val scalaPb        = "0.11.17"
    val casbin         = "1.68.0"
    val jwtScala       = "10.0.1"
    val bcrypt         = "4.3.0"
    val logback        = "1.5.12"
    val otel           = "1.43.0"
    val otelAlpha      = "1.43.0-alpha"
    val testContainers = "0.41.4"
    val scalaCheck     = "1.18.1"
    val pact           = "4.6.16"
    val pact4s         = "0.11.2"
  }

  // ── ZIO core ──────────────────────────────────────────────────────────────
  val zio             = "dev.zio" %% "zio"                   % Versions.zio
  val zioStreams      = "dev.zio" %% "zio-streams"           % Versions.zio
  val zioTest         = "dev.zio" %% "zio-test"              % Versions.zio
  val zioTestSbt      = "dev.zio" %% "zio-test-sbt"          % Versions.zio
  val zioTestMagnolia = "dev.zio" %% "zio-test-magnolia"     % Versions.zio

  // ── ZIO HTTP (Netty-based, WebSocket support) ─────────────────────────────
  val zioHttp = "dev.zio" %% "zio-http" % Versions.zioHttp

  // ── ZIO Kafka ─────────────────────────────────────────────────────────────
  val zioKafka = "dev.zio" %% "zio-kafka" % Versions.zioKafka

  // ── ZIO Config ───────────────────────────────────────────────────────────
  val zioConfig         = "dev.zio" %% "zio-config"          % Versions.zioConfig
  val zioConfigTypesafe = "dev.zio" %% "zio-config-typesafe" % Versions.zioConfig
  val zioConfigMagnolia = "dev.zio" %% "zio-config-magnolia" % Versions.zioConfig

  // ── ZIO Logging ──────────────────────────────────────────────────────────
  val zioLogging        = "dev.zio" %% "zio-logging"         % Versions.zioLogging
  val zioLoggingSlf4j   = "dev.zio" %% "zio-logging-slf4j2"  % Versions.zioLogging

  // ── ZIO Metrics (Prometheus) ──────────────────────────────────────────────
  val zioMetrics            = "dev.zio" %% "zio-metrics-connectors"            % Versions.zioMetrics
  val zioMetricsPrometheus  = "dev.zio" %% "zio-metrics-connectors-prometheus" % Versions.zioMetrics

  // ── ZIO Schema (JSON codecs + OpenAPI schemas) ────────────────────────────
  val zioSchema        = "dev.zio" %% "zio-schema"             % Versions.zioSchema
  val zioSchemaCirce   = "dev.zio" %% "zio-schema-json"        % Versions.zioSchema
  val zioSchemaDerive  = "dev.zio" %% "zio-schema-derivation"  % Versions.zioSchema

  // ── Tapir (type-safe endpoint descriptions + OpenAPI) ─────────────────────
  val tapirCore      = "com.softwaremill.sttp.tapir" %% "tapir-core"               % Versions.tapir
  val tapirZioHttp   = "com.softwaremill.sttp.tapir" %% "tapir-zio-http-server"    % Versions.tapir
  val tapirCirce     = "com.softwaremill.sttp.tapir" %% "tapir-json-circe"         % Versions.tapir
  val tapirOpenApi   = "com.softwaremill.sttp.tapir" %% "tapir-openapi-docs"       % Versions.tapir
  val tapirSwaggerUi = "com.softwaremill.sttp.tapir" %% "tapir-swagger-ui-bundle"  % Versions.tapir
  val tapirZioTest   = "com.softwaremill.sttp.tapir" %% "tapir-sttp-stub-server"   % Versions.tapir

  // ── Circe (JSON) ──────────────────────────────────────────────────────────
  val circeCore    = "io.circe" %% "circe-core"    % Versions.circe
  val circeGeneric = "io.circe" %% "circe-generic" % Versions.circe
  val circeParser  = "io.circe" %% "circe-parser"  % Versions.circe

  // ── MongoDB Scala Driver (official reactive driver) ───────────────────────
  val mongoScalaDriver = "org.mongodb.scala" %% "mongo-scala-driver" % Versions.mongo cross CrossVersion.for3Use2_13

  // ── Doobie (PostgreSQL / TimescaleDB) ─────────────────────────────────────
  val doobieCore     = "org.tpolecat" %% "doobie-core"       % Versions.doobie
  val doobiePostgres = "org.tpolecat" %% "doobie-postgres"   % Versions.doobie
  val doobieHikari   = "org.tpolecat" %% "doobie-hikari"     % Versions.doobie
  val doobieZio      = "io.github.gaelrenoux" %% "tranzactio" % "5.0.0"

  // ── gRPC / ScalaPB ────────────────────────────────────────────────────────
  val scalaPbRuntime    = "com.thesamet.scalapb" %% "scalapb-runtime"      % Versions.scalaPb
  val scalaPbRuntimeGrpc= "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % Versions.scalaPb
  val grpcNetty         = "io.grpc"               % "grpc-netty"           % scalapb.compiler.Version.grpcJavaVersion
  val zioGrpc           = "com.thesamet.scalapb.zio-grpc" %% "zio-grpc-core" % Versions.zioGrpc

  // ── Auth / Security ───────────────────────────────────────────────────────
  val jwtCirce  = "com.github.jwt-scala" %% "jwt-circe"   % Versions.jwtScala
  val bcrypt    = "com.github.t3hnar"    %% "scala-bcrypt" % Versions.bcrypt cross CrossVersion.for3Use2_13
  val casbin    = "org.casbin"            % "jcasbin"      % Versions.casbin

  // ── Logging ───────────────────────────────────────────────────────────────
  val logback = "ch.qos.logback" % "logback-classic" % Versions.logback

  // ── OpenTelemetry ─────────────────────────────────────────────────────────
  val otelApi    = "io.opentelemetry" % "opentelemetry-api"                         % Versions.otel
  val otelSdk    = "io.opentelemetry" % "opentelemetry-sdk"                         % Versions.otel
  val otelExporter = "io.opentelemetry" % "opentelemetry-exporter-otlp"             % Versions.otel

  // ── Email (Jakarta Mail / SMTP) ───────────────────────────────────────────
  val jakartaMail = "org.eclipse.angus" % "angus-mail" % "2.0.3"

  // ── Testing ───────────────────────────────────────────────────────────────
  val testContainersMongo  = "com.dimafeng" %% "testcontainers-scala-mongodb"   % Versions.testContainers
  val testContainersKafka  = "com.dimafeng" %% "testcontainers-scala-kafka"     % Versions.testContainers
  val testContainersPg     = "com.dimafeng" %% "testcontainers-scala-postgresql"% Versions.testContainers
  val scalaCheck           = "org.scalacheck" %% "scalacheck"                   % Versions.scalaCheck

  // ── Pact contract testing ─────────────────────────────────────────────────
  val pact4sZioTest = "io.github.jbwheatley" %% "pact4s-zio-test" % Versions.pact4s
  val pact4sCirce   = "io.github.jbwheatley" %% "pact4s-circe"    % Versions.pact4s

  // ── Grouped dependency sets per module ────────────────────────────────────

  val domainDeps: Seq[ModuleID] = Seq(
    zioSchema, zioSchemaDerive, circeCore, circeGeneric, circeParser,
    zioTest % Test, zioTestSbt % Test, zioTestMagnolia % Test
  )

  val authCoreDeps: Seq[ModuleID] = Seq(
    zio, casbin, jwtCirce, bcrypt,
    zioTest % Test, zioTestSbt % Test
  )

  val mongoZioDeps: Seq[ModuleID] = Seq(
    zio, zioStreams, mongoScalaDriver,
    otelApi,
    testContainersMongo % Test, zioTest % Test, zioTestSbt % Test
  )

  val kafkaZioDeps: Seq[ModuleID] = Seq(
    zioKafka,
    // Circe needed for typed JSON serialisation in EvKafkaProducer
    circeCore, circeGeneric, circeParser,
    testContainersKafka % Test, zioTest % Test, zioTestSbt % Test
  )

  // OTel shared library: config, EvTracing service, SDK lifecycle layer
  val otelZioDeps: Seq[ModuleID] = Seq(
    zio,
    otelApi, otelSdk, otelExporter,
  )

  val serviceBaseDeps: Seq[ModuleID] = Seq(
    zio, zioStreams, zioHttp,
    zioConfig, zioConfigTypesafe, zioConfigMagnolia,
    zioLogging, zioLoggingSlf4j,
    zioMetrics, zioMetricsPrometheus,
    tapirCore, tapirZioHttp, tapirCirce, tapirOpenApi, tapirSwaggerUi,
    logback,
    otelApi, otelSdk, otelExporter,
    zioTest % Test, zioTestSbt % Test, zioTestMagnolia % Test
  )

  val grpcDeps: Seq[ModuleID] = Seq(
    scalaPbRuntime, scalaPbRuntimeGrpc, grpcNetty, zioGrpc
  )
}
