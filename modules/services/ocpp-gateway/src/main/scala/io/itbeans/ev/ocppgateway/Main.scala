package io.itbeans.ev.ocppgateway

import io.itbeans.ev.auth.CasbinAuthorizationService
import io.itbeans.ev.kafka.{KafkaConfig, LiveEvKafkaProducer}
import zio._
import zio.config.typesafe.TypesafeConfigProvider
import zio.logging.backend.SLF4J
import zio.metrics.connectors.{prometheus, MetricsConfig}

// ---------------------------------------------------------------------------
// ev-ocpp-gateway — WebSocket gateway for OCPP charging stations.
//
// Responsibilities:
//   - Accept WebSocket connections from OCPP 2.1 / 2.0.1 / 1.6 stations
//   - Parse OCPP frames (types 2/3/4/5/6)
//   - Maintain per-station ZIO fiber + ZHub connection registry
//   - Publish normalised OcppCallEvent to Kafka (ocpp.events.{tenantId})
//   - Expose gRPC for SendCommand (REST API / Smart Charging → station)
//   - Sticky WebSocket routing: Kong hashes stationId to correct pod
// ---------------------------------------------------------------------------

object Main extends ZIOAppDefault:

  override val bootstrap: ZLayer[ZIOAppArgs, Any, Any] =
    Runtime.removeDefaultLoggers >>> SLF4J.slf4j

  // KafkaConfig.given and GatewayConfig.given defined in companion objects

  override def run: ZIO[ZIOAppArgs & Scope, Any, Any] =
    program.provide(
      Runtime.setConfigProvider(TypesafeConfigProvider.fromResourcePath()),
      // Config
      ZLayer.fromZIO(ZIO.config[KafkaConfig]),
      ZLayer.fromZIO(ZIO.config[GatewayConfig]),
      // Kafka producer
      LiveEvKafkaProducer.live,
      // Connection registry (ZHub + Ref)
      ConnectionRegistry.live,
      // Frame handler (JSON parse + Kafka publish)
      OcppFrameHandler.live,
      // In-process auth (Casbin RBAC/ABAC)
      CasbinAuthorizationService.live,
      // Prometheus metrics (5-second scrape interval)
      ZLayer.succeed(MetricsConfig(5.seconds)),
      prometheus.publisherLayer,
      prometheus.prometheusLayer,
      // WebSocket server
      OcppGatewayServer.live
    )

  private val program: ZIO[GatewayConfig & OcppGatewayServer & ConnectionRegistry & OcppFrameHandler, Throwable, Unit] =
    for
      cfg    <- ZIO.service[GatewayConfig]
      server <- ZIO.service[OcppGatewayServer]
      _ <- ZIO.logInfo(
        s"ev-ocpp-gateway starting: ws=:${cfg.httpPort} grpc=:${cfg.grpcPort}"
      )
      _ <- OcppGatewayGrpcTransport.start // start Netty gRPC server (non-blocking)
      _ <- server.start                   // start ZIO HTTP WebSocket server (blocks)
    yield ()
