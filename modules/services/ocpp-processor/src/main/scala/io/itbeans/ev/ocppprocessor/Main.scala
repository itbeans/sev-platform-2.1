package io.itbeans.ev.ocppprocessor

import io.itbeans.ev.kafka.{KafkaConfig, LiveEvKafkaProducer}
import io.itbeans.ev.otel.{EvTracing, OtelConfig, OtelLayer}
import org.mongodb.scala.MongoClient
import zio._
import zio.config.typesafe.TypesafeConfigProvider
import zio.http._
import zio.logging.backend.SLF4J
import zio.metrics.connectors.{prometheus, MetricsConfig}
import zio.metrics.connectors.prometheus.PrometheusPublisher

// ---------------------------------------------------------------------------
// ev-ocpp-processor — Kafka consumer for OCPP events.
//
// Consumes: ocpp.events.{tenantId}  (wildcard subscription)
// Produces: transactions.lifecycle, smart-charging.triggers,
//           notifications.outbound, audit.log, consumptions.ingest
//
// Handles all OCPP 2.1 (and 1.6 backward-compat) business logic:
//   BootNotification, Heartbeat, StatusNotification, Authorize,
//   StartTransaction/StopTransaction/MeterValues (OCPP 1.6),
//   TransactionEvent (OCPP 2.x), NotifyDERStartStop, AFRRSignal, BatterySwap (OCPP 2.1)
//
// Scales by Kafka partition count (partitioned by chargingStationId).
// ---------------------------------------------------------------------------

object Main extends ZIOAppDefault:

  override val bootstrap: ZLayer[ZIOAppArgs, Any, Any] =
    Runtime.removeDefaultLoggers >>> SLF4J.slf4j

  private val mongoDatabaseLayer: ZLayer[ProcessorConfig, Throwable, org.mongodb.scala.MongoDatabase] =
    ZLayer.fromZIO(
      ZIO.serviceWithZIO[ProcessorConfig](cfg =>
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

  override def run: ZIO[ZIOAppArgs & Scope, Any, Any] =
    (metricsServer *> program).provide(
      Runtime.setConfigProvider(TypesafeConfigProvider.fromResourcePath()),
      // Config
      ZLayer.fromZIO(ZIO.config[KafkaConfig]),
      ZLayer.fromZIO(ZIO.config[ProcessorConfig]),
      ZLayer.fromZIO(ZIO.config[OtelConfig]),
      OtelLayer.live,
      EvTracing.live,
      // MongoDB
      mongoDatabaseLayer,
      // Repository
      ProcessorRepository.live,
      // Kafka producer (for outbound events)
      LiveEvKafkaProducer.live,
      // gRPC clients
      ProcessorAuthClient.live,
      ProcessorGatewayClient.live,
      ProcessorPricingClient.live,
      // Event processor
      OcppEventProcessor.live,
      ZLayer.succeed(MetricsConfig(5.seconds)),
      prometheus.publisherLayer,
      prometheus.prometheusLayer
    )

  private val healthRoutes: Routes[Any, Nothing] =
    Routes(Method.GET / "health" -> Handler.ok)

  private val program: ZIO[ProcessorConfig & OcppEventProcessor, Throwable, Unit] =
    for
      cfg       <- ZIO.service[ProcessorConfig]
      processor <- ZIO.service[OcppEventProcessor]
      _ <- ZIO.logInfo(
        s"ev-ocpp-processor starting: mongo=${cfg.mongoUri} " +
          s"group=${cfg.consumerGroupId}"
      )
      // Health endpoint runs as a daemon fiber alongside the Kafka consumer
      _ <- Server.serve(healthRoutes)
        .provide(Server.defaultWithPort(8080))
        .forkDaemon
      _ <- processor.startConsuming
    yield ()
