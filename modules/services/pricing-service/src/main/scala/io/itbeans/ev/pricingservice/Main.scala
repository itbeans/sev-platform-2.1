package io.itbeans.ev.pricingservice

import io.itbeans.ev.mongo.{MongoClientLayer, MongoConfig, MongoDatabaseLayer}
import zio._
import zio.config.typesafe.TypesafeConfigProvider
import zio.http.Server
import zio.logging.backend.SLF4J

// ---------------------------------------------------------------------------
// ev-pricing-service entrypoint.
//
// Starts:
//   - HTTP REST API (Tapir → ZIO HTTP) on port 8080
//     Handles: /api/pricing CRUD (tariff management)
//   - gRPC server on port 9090
//     Handles: ResolvePricing, PriceConsumption, FinalisePrice
// ---------------------------------------------------------------------------

object Main extends ZIOAppDefault:

  override val bootstrap =
    Runtime.removeDefaultLoggers >>> SLF4J.slf4j

  override def run = program.provide(
    Runtime.setConfigProvider(TypesafeConfigProvider.fromResourcePath()),
    ZLayer.fromZIO(ZIO.config[MongoConfig]),
    ZLayer.fromZIO(ZIO.config[PricingConfig]),
    MongoClientLayer.live,
    MongoDatabaseLayer.live,
    MongoPricingRepository.live,
    TariffResolver.live,
    ConsumptionPricer.live,
    PricingGrpcHandler.live,
    Server.defaultWithPort(8080)
  )

  private val program: ZIO[
    PricingRepository & PricingGrpcHandler & Server & PricingConfig,
    Throwable,
    Nothing
  ] =
    for
      cfg  <- ZIO.service[PricingConfig]
      repo <- ZIO.service[PricingRepository]
      _ <- ZIO.logInfo(
        s"ev-pricing-service starting: http=:${cfg.httpPort} grpc=:${cfg.grpcPort}"
      )
      _ <- PricingGrpcTransport.start
      r <- Server.serve(PricingHttpServer.routes(repo))
    yield r
