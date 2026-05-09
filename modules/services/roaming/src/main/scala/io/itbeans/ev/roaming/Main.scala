package io.itbeans.ev.roaming

import io.itbeans.ev.kafka.KafkaConfig
import io.itbeans.ev.mongo.{MongoClientLayer, MongoConfig, MongoDatabaseLayer}
import zio._
import zio.config.typesafe.TypesafeConfigProvider
import zio.http.Server
import zio.logging.backend.SLF4J

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

  override def run: ZIO[ZIOAppArgs & Scope, Any, Any] =
    program.provide(
      Runtime.setConfigProvider(TypesafeConfigProvider.fromResourcePath()),
      // ── Config layers ──────────────────────────────────────────────────
      ZLayer.fromZIO(ZIO.config[MongoConfig]),
      ZLayer.fromZIO(ZIO.config[KafkaConfig]),
      ZLayer.fromZIO(ZIO.config[RoamingConfig]),
      // ── Infrastructure ─────────────────────────────────────────────────
      MongoClientLayer.live,
      MongoDatabaseLayer.live,
      // ── Repository layers ──────────────────────────────────────────────
      MongoOcpiEndpointRepository.live,
      MongoOicpEndpointRepository.live,
      MongoTransactionReadRepository.live,
      // ── Application layers ─────────────────────────────────────────────
      CdrService.live,
      OcpiCpoClient.live,
      OicpCpoClient.live,
      RoamingKafkaConsumer.live,
      // ── HTTP server (port 8080) ─────────────────────────────────────────
      Server.defaultWithPort(8080)
    )

  private val program: ZIO[OcpiEndpointRepository & RoamingKafkaConsumer & Server & RoamingConfig, Throwable, Nothing] =
    for
      cfg      <- ZIO.service[RoamingConfig]
      ocpiRepo <- ZIO.service[OcpiEndpointRepository]
      consumer <- ZIO.service[RoamingKafkaConsumer]
      _ <- ZIO.logInfo(
        s"ev-roaming starting: http=:${cfg.httpPort} " +
          s"OCPI ${cfg.ocpiCountryCode}-${cfg.ocpiPartyId} (${cfg.ocpiOperatorName})"
      )
      // Start Kafka consumer in the background
      _ <- consumer.start
        .tapError(err => ZIO.logError(s"Roaming Kafka consumer error: ${err.getMessage}"))
        .ignore
        .forkDaemon
      // Serve HTTP (OCPI server + management)
      r <- Server.serve(RoamingHttpServer.routes(ocpiRepo, cfg))
    yield r
