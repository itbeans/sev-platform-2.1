package io.itbeans.ev.asset

import io.itbeans.ev.kafka.{KafkaConfig, LiveEvKafkaProducer}
import io.itbeans.ev.mongo.{MongoClientLayer, MongoConfig, MongoDatabaseLayer}
import io.itbeans.ev.asset.connector.AssetConnectorRegistry
import io.itbeans.ev.domain.TenantId
import io.itbeans.ev.otel.{EvTracing, OtelConfig, OtelLayer}
import zio._
import zio.config.typesafe.TypesafeConfigProvider
import zio.http._
import zio.logging.backend.SLF4J
import zio.metrics.connectors.{prometheus, MetricsConfig}
import zio.metrics.connectors.prometheus.PrometheusPublisher

// ---------------------------------------------------------------------------
// ev-asset — Asset consumption polling service.
//
// Responsibilities:
//   • Poll vendor APIs (Greencom, IOThink, Lacroix, Schneider, WIT) every
//     `pollIntervalSeconds` for all dynamic assets of each configured tenant
//   • Publish AssetConsumptionEvent to Kafka topic `asset.consumptions`
//   • REST API: /api/assets/ CRUD (Tapir → ZIO HTTP, port 8080)
//
// Config sources (in order of precedence):
//   1. Environment variables (${?ENV_VAR} substitutions in application.conf)
//   2. application.conf (bundled in resources)
// ---------------------------------------------------------------------------

object Main extends ZIOAppDefault:

  override val bootstrap: ZLayer[ZIOAppArgs, Any, Any] =
    Runtime.removeDefaultLoggers >>> SLF4J.slf4j

  // Config givens come from MongoConfig, KafkaConfig, and AssetConfig companion objects

  private val metricsServer: ZIO[PrometheusPublisher, Throwable, Any] =
    ZIO.serviceWithZIO[PrometheusPublisher] { publisher =>
      Server
        .serve(Routes(Method.GET / "metrics" ->
          Handler.fromZIO(publisher.get.map(Response.text))))
        .provide(Server.defaultWithPort(8888))
        .forkDaemon
    }

  override def run: ZIO[ZIOAppArgs & Scope, Any, Any] =
    (metricsServer *> program).provide(
      Runtime.setConfigProvider(TypesafeConfigProvider.fromResourcePath()),
      // ── Config layers ──────────────────────────────────────────────────
      ZLayer.fromZIO(ZIO.config[MongoConfig]),
      ZLayer.fromZIO(ZIO.config[KafkaConfig]),
      ZLayer.fromZIO(ZIO.config[AssetConfig]),
      ZLayer.fromZIO(ZIO.config[OtelConfig]),
      OtelLayer.live,
      EvTracing.live,
      // ── Infrastructure ─────────────────────────────────────────────────
      MongoClientLayer.live,
      MongoDatabaseLayer.live,
      LiveEvKafkaProducer.live,
      // ── Application layers ─────────────────────────────────────────────
      MongoAssetRepository.live,
      MongoAssetConnectionRepository.live,
      AssetConnectorRegistry.live,
      AssetGetConsumptionTask.live,
      // ── HTTP server (port 8080) ─────────────────────────────────────────
      Server.defaultWithPort(8080),
      ZLayer.succeed(MetricsConfig(5.seconds)),
      prometheus.publisherLayer,
      prometheus.prometheusLayer
    )

  private val program: ZIO[
    AssetRepository & AssetGetConsumptionTask & io.itbeans.ev.kafka.EvKafkaProducer & Server & AssetConfig,
    Throwable,
    Unit
  ] =
    for
      cfg   <- ZIO.service[AssetConfig]
      repo  <- ZIO.service[AssetRepository]
      task  <- ZIO.service[AssetGetConsumptionTask]
      kafka <- ZIO.service[io.itbeans.ev.kafka.EvKafkaProducer]
      _ <- ZIO.logInfo(
        s"ev-asset starting: http=:${cfg.httpPort} " +
          s"poll=${cfg.pollIntervalSeconds}s tenants=${cfg.tenantIds.mkString(",")}"
      )
      // Launch one polling fiber per tenant, each running on its own schedule.
      _ <- ZIO.foreachDiscard(cfg.tenantIds) { rawTenantId =>
        val tenantId = TenantId(rawTenantId)
        task.run(tenantId)
          .tapError(err => ZIO.logError(s"Poll error for tenant $rawTenantId: ${err.getMessage}"))
          .ignore
          .repeat(Schedule.fixed(cfg.pollIntervalSeconds.seconds))
          .forkDaemon
      }
      // Serve HTTP REST API — blocks until shutdown.
      _ <- Server.serve(AssetHttpServer.routes(repo, kafka))
    yield ()
