package io.itbeans.ev.analytics

import io.itbeans.ev.kafka.KafkaConfig

import zio._
import zio.config.typesafe.TypesafeConfigProvider
import zio.http._
import zio.logging.backend.SLF4J
import zio.metrics.connectors.{prometheus, MetricsConfig}
import zio.metrics.connectors.prometheus.PrometheusPublisher

// ---------------------------------------------------------------------------
// ev-analytics — Audit logs + consumption statistics via TimescaleDB.
//
// Responsibilities:
//   • Kafka consumer: audit.log → ev_logs hypertable (TimescaleDB)
//   • REST API: GET /api/v1/statistics/charging-stations/{metric}
//               GET /api/v1/logs (filtered, paginated)
//   • Periodic: retention sweep (delete logs older than cfg.logRetentionDays)
//
// Database: TimescaleDB (PostgreSQL extension) via Doobie.
//   ev_consumptions — time-series power readings (one per meter-value)
//   ev_logs         — audit log (level, action, source, message, context)
// ---------------------------------------------------------------------------

object Main extends ZIOAppDefault:

  override val bootstrap: ZLayer[ZIOAppArgs, Any, Any] =
    Runtime.removeDefaultLoggers >>> SLF4J.slf4j

  // AnalyticsConfig.given defined in companion

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
      ZLayer.fromZIO(ZIO.config[KafkaConfig]),
      ZLayer.fromZIO(ZIO.config[AnalyticsConfig]),
      // ── Repository layers (TimescaleDB/Doobie) ─────────────────────────
      AnalyticsRepository.consumptionRepoLive,
      AnalyticsRepository.logRepoLive,
      // ── Kafka consumer ─────────────────────────────────────────────────
      LiveAnalyticsKafkaConsumer.live,
      // ── HTTP server ────────────────────────────────────────────────────
      Server.defaultWithPort(8080),
      ZLayer.succeed(MetricsConfig(5.seconds)),
      prometheus.publisherLayer,
      prometheus.prometheusLayer
    )

  private val program: ZIO[
    AnalyticsConfig & KafkaConfig &
      ConsumptionRepository & LogRepository &
      AnalyticsKafkaConsumer & Server,
    Throwable,
    Unit
  ] =
    for
      cfg             <- ZIO.service[AnalyticsConfig]
      consumer        <- ZIO.service[AnalyticsKafkaConsumer]
      consumptionRepo <- ZIO.service[ConsumptionRepository]
      logRepo         <- ZIO.service[LogRepository]
      _ <- ZIO.logInfo(
        s"ev-analytics starting: http=:${cfg.httpPort} " +
          s"logRetentionDays=${cfg.logRetentionDays}"
      )
      // Start Kafka consumers (audit.log + consumptions.ingest → TimescaleDB)
      _ <- consumer.start
        .tapError(err => ZIO.logError(s"Analytics Kafka consumer error: ${err.getMessage}"))
        .ignore
        .forkDaemon
      // Start log retention sweep (daily)
      _ <- retentionSweep(logRepo, cfg).forkDaemon
      // Serve HTTP
      _ <- Server.serve(AnalyticsHttpServer.routes(consumptionRepo, logRepo))
    yield ()

  private def retentionSweep(logRepo: LogRepository, cfg: AnalyticsConfig): Task[Unit] =
    (for
      _ <- ZIO.sleep(24.hours)
      _ <- ZIO.logInfo("[Analytics] Running log retention sweep")
      cutoff = java.time.Instant.now().minusSeconds(cfg.logRetentionDays.toLong * 86400)
      n <- logRepo.deleteOlderThan(cfg.defaultTenantId, cutoff)
      _ <- ZIO.logInfo(s"[Analytics] Deleted $n old log entries for ${cfg.defaultTenantId}")
    yield ()).forever
