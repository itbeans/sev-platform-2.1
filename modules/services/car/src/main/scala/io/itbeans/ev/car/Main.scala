package io.itbeans.ev.car

import io.itbeans.ev.car.connector.CarConnectorRegistry
import io.itbeans.ev.mongo.{MongoClientLayer, MongoConfig, MongoDatabaseLayer}
import zio._
import zio.config.typesafe.TypesafeConfigProvider
import zio.http.Server
import zio.logging.backend.SLF4J

// ---------------------------------------------------------------------------
// ev-car — Car catalog sync + EV connector integrations.
//
// Responsibilities:
//   • Sync EV Database car catalog on a periodic ZIO Schedule
//   • REST API: /api/cars/, /api/carcatalogs/ (Tapir → ZIO HTTP, port 8080)
//   • gRPC: GetUserDefaultCar (port 9090, called by ev-ocpp-processor)
//   • Live SoC via car connectors (Mercedes, Tronity, Targa)
//
// Config sources (in order of precedence):
//   1. Environment variables (${?ENV_VAR} substitutions in application.conf)
//   2. application.conf (bundled in resources)
// ---------------------------------------------------------------------------

object Main extends ZIOAppDefault:

  override val bootstrap: ZLayer[ZIOAppArgs, Any, Any] =
    Runtime.removeDefaultLoggers >>> SLF4J.slf4j

  override def run: ZIO[ZIOAppArgs & Scope, Any, Any] =
    program.provide(
      Runtime.setConfigProvider(TypesafeConfigProvider.fromResourcePath()),
      // ── Config layers ──────────────────────────────────────────────────
      ZLayer.fromZIO(ZIO.config[MongoConfig]),
      ZLayer.fromZIO(ZIO.config[CarConfig]),
      // ── Infrastructure ─────────────────────────────────────────────────
      MongoClientLayer.live,
      MongoDatabaseLayer.live,
      // ── Application layers ─────────────────────────────────────────────
      MongoCarCatalogRepository.live,
      MongoCarRepository.live,
      HttpEVDatabaseClient.live,
      SynchronizeCarsTask.live,
      CarConnectorRegistry.live,
      DefaultCarService.live,
      CarGrpcHandler.live,
      // ── HTTP server (port 8080) ─────────────────────────────────────────
      Server.defaultWithPort(8080)
    )

  private val program: ZIO[CarService & CarGrpcHandler & Server & CarConfig, Throwable, Unit] =
    for
      _   <- ZIO.logInfo("ev-car starting")
      cfg <- ZIO.service[CarConfig]
      svc <- ZIO.service[CarService]
      _   <- CarGrpcTransport.start
      _   <- ZIO.logInfo(s"ev-car ready — REST :8080, gRPC :${cfg.grpcPort}")
      _ <- svc.syncCatalogs
        .tapError(err => ZIO.logError(s"Catalog sync error: ${err.getMessage}"))
        .ignore
        .repeat(Schedule.fixed(parseDuration(cfg.syncInterval)))
        .forkDaemon
      _ <- Server.serve(CarHttpServer.routes(svc))
    yield ()

  private def parseDuration(s: String): Duration =
    Duration.fromJava(java.time.Duration.parse(s))
