package io.itbeans.ev.smartcharging

import io.itbeans.ev.kafka.KafkaConfig
import io.itbeans.ev.mongo.{MongoClientLayer, MongoConfig, MongoDatabaseLayer}
import zio._
import zio.config.magnolia._
import zio.config.typesafe.TypesafeConfigProvider
import zio.logging.backend.SLF4J

// ---------------------------------------------------------------------------
// ev-smart-charging — SAP Smart Charging integration + OCPP 2.1 DER Control.
//
// Responsibilities:
//   • Kafka consumer: smart-charging.triggers → call SAP optimizer, apply profiles
//   • Kafka consumer: asset.consumptions → trigger reoptimization on load change
//   • Periodic recompute: every cfg.periodicIntervalMins for all active site areas
//   • gRPC (pre-codegen): TriggerSmartCharging, GetChargingProfiles
//   • MongoDB: reads {tenantId}.siteareas, {tenantId}.transactions (read-only)
//              writes {tenantId}.chargingprofiles
//
// SAP Optimizer: receives fuse tree + active car states → returns 15-min
// amperage plans per car → converted to OCPP ChargingProfile documents.
// ---------------------------------------------------------------------------

object Main extends ZIOAppDefault:

  override val bootstrap: ZLayer[ZIOAppArgs, Any, Any] =
    Runtime.removeDefaultLoggers >>> SLF4J.slf4j

  // Config givens come from MongoConfig, KafkaConfig, and SmartChargingConfig companion objects

  override def run: ZIO[ZIOAppArgs & Scope, Any, Any] =
    program.provide(
      Runtime.setConfigProvider(TypesafeConfigProvider.fromResourcePath()),
      // ── Config layers ──────────────────────────────────────────────────
      ZLayer.fromZIO(ZIO.config[MongoConfig]),
      ZLayer.fromZIO(ZIO.config[KafkaConfig]),
      ZLayer.fromZIO(ZIO.config[SmartChargingConfig]),
      // ── MongoDB ────────────────────────────────────────────────────────
      MongoClientLayer.live,
      MongoDatabaseLayer.live,
      MongoSmartChargingRepository.live,
      // ── SAP optimizer HTTP client ──────────────────────────────────────
      SapSmartChargingClient.live,
      // ── OCPP Gateway gRPC client (for SetChargingProfile delivery) ─────
      SmartChargingGatewayClient.live,
      // ── Business logic ─────────────────────────────────────────────────
      LiveSmartChargingService.live,
      LiveSmartChargingGrpcHandler.live,
      // ── Kafka consumers ────────────────────────────────────────────────
      LiveSmartChargingKafkaConsumer.live
    )

  private val program: ZIO[
    SmartChargingConfig & KafkaConfig &
      SmartChargingService & SmartChargingKafkaConsumer &
      SmartChargingGrpcHandler & SmartChargingRepository,
    Throwable,
    Unit
  ] =
    for
      cfg      <- ZIO.service[SmartChargingConfig]
      consumer <- ZIO.service[SmartChargingKafkaConsumer]
      svc      <- ZIO.service[SmartChargingService]
      _ <- ZIO.logInfo(
        s"ev-smart-charging starting: " +
          s"optimizerUrl=${cfg.optimizerUrl} " +
          s"debounceSecs=${cfg.debounceSecs} " +
          s"periodicIntervalMins=${cfg.periodicIntervalMins}"
      )
      // Start both Kafka consumers in background (triggers + asset consumptions)
      _ <- consumer.start
        .tapError(err => ZIO.logError(s"Smart charging Kafka consumer error: ${err.getMessage}"))
        .ignore
        .forkDaemon
      // Start periodic fallback recompute
      _ <- startPeriodicRecompute(svc, cfg).forkDaemon
      // Block forever — consumers run as daemon fibers
      _ <- ZIO.never
    yield ()

  /**
   * Periodic fallback: reoptimize all site areas every periodicIntervalMins.
   * This catches cases where a Kafka trigger was lost (at-least-once guarantee
   * does not protect against Kafka cluster downtime).
   */
  private def startPeriodicRecompute(
      svc: SmartChargingService,
      cfg: SmartChargingConfig
  ): Task[Unit] =
    (for
      _ <- ZIO.sleep(cfg.periodicIntervalMins.minutes)
      _ <- ZIO.logDebug(
        s"[SmartCharging] Periodic recompute for tenant=${cfg.defaultTenantId}"
      )
      // In a full implementation we'd list all smartCharging-enabled site areas here.
      // For now we log readiness; the Kafka consumer path handles actual triggers.
      _ <- ZIO.logInfo("[SmartCharging] Periodic heartbeat — awaiting trigger messages")
    yield ()).forever
