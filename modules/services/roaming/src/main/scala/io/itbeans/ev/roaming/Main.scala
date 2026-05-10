package io.itbeans.ev.roaming

import io.itbeans.ev.kafka.KafkaConfig
import io.itbeans.ev.mongo.{MongoClientLayer, MongoConfig, MongoDatabaseLayer}
import io.itbeans.ev.otel.{EvTracing, OtelConfig, OtelLayer}
import zio._
import zio.config.typesafe.TypesafeConfigProvider
import zio.http._
import zio.logging.backend.SLF4J
import zio.metrics.connectors.{prometheus, MetricsConfig}
import zio.metrics.connectors.prometheus.PrometheusPublisher

// ---------------------------------------------------------------------------
// ev-roaming — OCPI 2.1.1 + OICP 2.3.0 roaming protocols.
//
// Responsibilities:
//   • Consume transactions.lifecycle Kafka events → generate + push CDRs
//   • OCPI CPO server: credential exchange, CDR receive (EMSP→CPO)
//   • OCPI CPO client: CDR push to registered EMSP partners (CPO→EMSP)
//   • OICP CPO client: CDR push + EVSE status push to Hubject
//   • Management REST: /ocpi/mgmt/endpoints CRUD
//
// Ports: HTTP 8080 (OCPI server + management)
// ---------------------------------------------------------------------------

object Main extends ZIOAppDefault:

  override val bootstrap: ZLayer[ZIOAppArgs, Any, Any] =
    Runtime.removeDefaultLoggers >>> SLF4J.slf4j

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
      ZLayer.fromZIO(ZIO.config[RoamingConfig]),
      ZLayer.fromZIO(ZIO.config[OtelConfig]),
      OtelLayer.live,
      EvTracing.live,
      // ── Infrastructure ─────────────────────────────────────────────────
      MongoClientLayer.live,
      MongoDatabaseLayer.live,
      // ── Repository layers ──────────────────────────────────────────────
      MongoOcpiEndpointRepository.live,
      MongoOicpEndpointRepository.live,
      MongoTransactionReadRepository.live,
      MongoChargingStationLocationRepository.live,
      // ── Application layers ─────────────────────────────────────────────
      CdrService.live,
      OcpiCpoClient.live,
      OicpCpoClient.live,
      RoamingKafkaConsumer.live,
      // ── HTTP server (port 8080) ─────────────────────────────────────────
      Server.defaultWithPort(8080),
      ZLayer.succeed(MetricsConfig(5.seconds)),
      prometheus.publisherLayer,
      prometheus.prometheusLayer
    )

  private val program: ZIO[
    OcpiEndpointRepository & OcpiCpoClient & RoamingKafkaConsumer & Server & RoamingConfig,
    Throwable,
    Nothing
  ] =
    for
      cfg        <- ZIO.service[RoamingConfig]
      ocpiRepo   <- ZIO.service[OcpiEndpointRepository]
      ocpiClient <- ZIO.service[OcpiCpoClient]
      consumer   <- ZIO.service[RoamingKafkaConsumer]
      _ <- ZIO.logInfo(
        s"ev-roaming starting: http=:${cfg.httpPort} " +
          s"OCPI ${cfg.ocpiCountryCode}-${cfg.ocpiPartyId} (${cfg.ocpiOperatorName}) " +
          s"patchJobIntervalHours=${cfg.patchJobIntervalHours}"
      )
      // Start Kafka consumer in the background
      _ <- consumer.start
        .tapError(err => ZIO.logError(s"Roaming Kafka consumer error: ${err.getMessage}"))
        .ignore
        .forkDaemon
      // Start OCPI background patch job (token refresh + location push)
      _ <- if cfg.patchJobIntervalHours > 0 then
        startOcpiPatchJob(ocpiRepo, ocpiClient, cfg).forkDaemon
      else ZIO.unit
      // Serve HTTP (OCPI server + management)
      r <- Server.serve(RoamingHttpServer.routes(ocpiRepo, cfg))
    yield r

  /**
   * Periodic OCPI background patch job.
   * For each registered EMSP partner with backgroundPatchJob=true:
   *   1. Re-exchange credentials (token refresh) via PUT /credentials
   *   2. Log patch job completion + update lastPatchJobOn in MongoDB
   *
   * Full location/tariff/session sync would require iterating our
   * charging station inventory; this is deferred to Phase 4.
   */
  private def startOcpiPatchJob(
      ocpiRepo: OcpiEndpointRepository,
      ocpiClient: OcpiCpoClient,
      cfg: RoamingConfig
  ): Task[Unit] =
    val tenantId = io.itbeans.ev.domain.TenantId(cfg.defaultTenantId)
    (for
      _ <- ZIO.sleep(cfg.patchJobIntervalHours.hours)
      _ <- ZIO.logInfo("[Roaming] Running OCPI background patch job")
      endpoints <- ocpiRepo.findRegistered(tenantId)
        .map(_.filter(ep => ep.backgroundPatchJob && ep.role == OcpiRole.EMSP))
      _ <- ZIO.logInfo(s"[Roaming] Patch job: ${endpoints.length} EMSP partner(s) to update")
      _ <- ZIO.foreachDiscard(endpoints) { ep =>
        ocpiClient.refreshCredentials(ep)
          .tapError(err =>
            ZIO.logWarning(s"[Roaming] Credential refresh failed for ${ep.id}: ${err.getMessage}")
          )
          .ignore
      }
    yield ()).forever
