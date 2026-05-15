package io.itbeans.ev.pricingservice

import io.itbeans.ev.mongo.{MongoClientLayer, MongoConfig, MongoDatabaseLayer}
import io.itbeans.ev.otel.{EvTracing, OtelConfig, OtelLayer}
import zio._
import zio.config.typesafe.TypesafeConfigProvider
import zio.http._
import zio.logging.backend.SLF4J
import zio.metrics.connectors.{prometheus, MetricsConfig}
import zio.metrics.connectors.prometheus.PrometheusPublisher

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

  private val metricsServer: ZIO[PrometheusPublisher, Throwable, Any] =
    ZIO.serviceWithZIO[PrometheusPublisher] { publisher =>
      Server
        .serve(Routes(Method.GET / "metrics" ->
          Handler.fromZIO(publisher.get.map(Response.text))))
        .provide(Server.defaultWithPort(8888))
        .forkDaemon
    }

  override def run =
    (metricsServer *> program).provide(
      Runtime.setConfigProvider(TypesafeConfigProvider.fromResourcePath()),
      ZLayer.fromZIO(ZIO.config[MongoConfig]),
      ZLayer.fromZIO(ZIO.config[PricingConfig]),
      ZLayer.fromZIO(ZIO.config[OtelConfig]),
      OtelLayer.live,
      EvTracing.live,
      MongoClientLayer.live,
      MongoDatabaseLayer.live,
      MongoPricingRepository.live,
      TariffResolver.live,
      ConsumptionPricer.live,
      PricingGrpcHandler.live,
      Server.defaultWithPort(8080),
      ZLayer.succeed(MetricsConfig(5.seconds)),
      prometheus.publisherLayer,
      prometheus.prometheusLayer
    )

  private val program: ZIO[
    PricingRepository & PricingGrpcHandler & Server & PricingConfig & EvTracing,
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
