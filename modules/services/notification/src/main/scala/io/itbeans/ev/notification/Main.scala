package io.itbeans.ev.notification

import io.itbeans.ev.kafka.KafkaConfig
import io.itbeans.ev.mongo.{MongoClientLayer, MongoConfig, MongoDatabaseLayer}
import io.itbeans.ev.notification.email.SmtpEmailNotificationTask

import io.itbeans.ev.notification.fcm.FcmNotificationTaskLive
import zio._
import zio.config.typesafe.TypesafeConfigProvider
import zio.http._
import zio.logging.backend.SLF4J
import zio.metrics.connectors.{prometheus, MetricsConfig}
import zio.metrics.connectors.prometheus.PrometheusPublisher

// ---------------------------------------------------------------------------
// ev-notification — Email + Firebase push notification consumer.
//
// Consumes: notifications.outbound (Kafka)
// Replaces: NotificationHandler.ts, EMailNotificationTask.ts,
//           RemotePushNotificationTask.ts
//
// Config sources (in order of precedence):
//   1. Environment variables (${?ENV_VAR} substitutions in application.conf)
//   2. application.conf (bundled in resources)
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
      ZLayer.fromZIO(ZIO.config[NotificationConfig]),
      // ── Infrastructure ─────────────────────────────────────────────────
      MongoClientLayer.live,
      MongoDatabaseLayer.live,
      // ── Application layers ─────────────────────────────────────────────
      MongoNotificationStorage.live,
      SmtpEmailNotificationTask.live,
      FcmNotificationTaskLive.live,
      DefaultNotificationService.live,
      KafkaConsumer.live,
      ZLayer.succeed(MetricsConfig(5.seconds)),
      prometheus.publisherLayer,
      prometheus.prometheusLayer
    )

  private val healthRoutes: Routes[Any, Nothing] =
    Routes(Method.GET / "health" -> Handler.ok)

  private val program: ZIO[KafkaConsumer, Throwable, Nothing] =
    for
      _        <- ZIO.logInfo("ev-notification starting")
      consumer <- ZIO.service[KafkaConsumer]
      // Health endpoint runs as a daemon fiber alongside the Kafka consumer
      _ <- Server.serve(healthRoutes)
        .provide(Server.defaultWithPort(8080))
        .forkDaemon
      _ <- ZIO.logInfo("ev-notification ready — consuming notifications.outbound")
      r <- consumer.start *> ZIO.never
    yield r
