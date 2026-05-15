package io.itbeans.ev.restapi

import io.itbeans.ev.auth.CasbinAuthorizationService
import io.itbeans.ev.otel.{EvTracing, OtelConfig, OtelLayer}
import org.mongodb.scala.MongoClient
import zio._
import zio.config.typesafe.TypesafeConfigProvider
import zio.http._
import zio.logging.backend.SLF4J
import zio.metrics.connectors.{prometheus, MetricsConfig}
import zio.metrics.connectors.prometheus.PrometheusPublisher

// ---------------------------------------------------------------------------
// ev-rest-api — Dashboard HTTP API (Tapir endpoints + OpenAPI/Swagger).
//
// Replaces: Express.js REST server + all src/server/rest/v1/service/*.ts
//
// Endpoints (~45 groups):
//   ChargingStations, Transactions, Users, Sites, SiteAreas, Companies,
//   Tags, Assets, Settings, Billing, OcppCommands, SmartChargingProfiles,
//   Notifications, Roaming (OCPI/OICP), Statistics, Logs
//
// Auth:  JWT validated by ev-auth-service (gRPC). Authorization enforced
//        in-process by ev-auth-core (CasbinAuthorizationService).
// Data:  MongoDB (same database as TypeScript monolith, read-write).
//        Statistics/Logs proxied to ev-analytics (TimescaleDB).
// ---------------------------------------------------------------------------

object Main extends ZIOAppDefault:

  override val bootstrap: ZLayer[ZIOAppArgs, Any, Any] =
    Runtime.removeDefaultLoggers >>> SLF4J.slf4j

  // ── MongoDB layer derived from RestApiConfig ──────────────────────────────

  private val mongoDatabaseLayer: ZLayer[RestApiConfig, Throwable, org.mongodb.scala.MongoDatabase] =
    ZLayer.fromZIO(
      ZIO.serviceWithZIO[RestApiConfig](cfg =>
        ZIO.attempt(MongoClient(cfg.mongoUri).getDatabase(cfg.mongoDbName))
      )
    )

  private val metricsServer: ZIO[PrometheusPublisher, Throwable, Any] =
    ZIO.serviceWithZIO[PrometheusPublisher] { publisher =>
      Server
        .serve(Routes(Method.GET / "metrics" ->
          Handler.fromZIO(publisher.get.map(Response.text))))
        .provide(Server.defaultWithPort(8888))
        .forkDaemon
    }

  // ── Full application layer graph ──────────────────────────────────────────

  override def run: ZIO[ZIOAppArgs & Scope, Any, Any] =
    (metricsServer *> program).provide(
      Runtime.setConfigProvider(TypesafeConfigProvider.fromResourcePath()),
      // Config
      ZLayer.fromZIO(ZIO.config[RestApiConfig]),
      ZLayer.fromZIO(ZIO.config[OtelConfig]),
      OtelLayer.live,
      EvTracing.live,
      // MongoDB
      mongoDatabaseLayer,
      // Repository
      RestApiRepository.live,
      // Authorization (in-process RBAC/ABAC)
      CasbinAuthorizationService.live,
      // gRPC clients
      OcppGatewayGrpcClient.live,
      BillingGrpcClient.live,
      // REST server
      RestApiServer.live,
      ZLayer.succeed(MetricsConfig(5.seconds)),
      prometheus.publisherLayer,
      prometheus.prometheusLayer
    )

  private val program: ZIO[RestApiConfig & RestApiServer, Throwable, Unit] =
    for
      cfg    <- ZIO.service[RestApiConfig]
      server <- ZIO.service[RestApiServer]
      _ <- ZIO.logInfo(
        s"ev-rest-api starting: http=:${cfg.httpPort} " +
          s"mongo=${cfg.mongoUri} analytics=${cfg.analyticsBaseUrl}"
      )
      _ <- server.start
      _ <- ZIO.logInfo("ev-rest-api ready — Swagger UI at /api/docs")
    yield ()
