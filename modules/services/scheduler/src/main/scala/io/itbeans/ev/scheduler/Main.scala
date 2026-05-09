package io.itbeans.ev.scheduler

import io.itbeans.ev.kafka.KafkaConfig
import io.itbeans.ev.mongo.{MongoClientLayer, MongoConfig, MongoDatabaseLayer}
import zio._
import zio.config.magnolia._
import zio.config.typesafe.TypesafeConfigProvider
import zio.logging.backend.SLF4J

// ---------------------------------------------------------------------------
// ev-scheduler — Cron-style periodic task runner.
//
// Responsibilities:
//   • CheckOfflineStations   — sweep stations silent for > N seconds
//   • CloseStaleTransactions — force-stop open transactions > N hours old
//   • CleanupLogs            — prune old logs and performance records
//   • CheckUserInactivity    — notify inactive user accounts
//
// All tasks are multi-tenant (iterate tenants collection) and use a
// MongoDB-based distributed lock to prevent concurrent pod execution.
//
// Side effects go to Kafka (notifications.outbound) — the notification
// service handles actual email/push delivery.
// ---------------------------------------------------------------------------

object Main extends ZIOAppDefault:

  override val bootstrap: ZLayer[ZIOAppArgs, Any, Any] =
    Runtime.removeDefaultLoggers >>> SLF4J.slf4j

  // Config givens come from MongoConfig, KafkaConfig, and SchedulerConfig companion objects

  override def run: ZIO[ZIOAppArgs & Scope, Any, Any] =
    program.provide(
      Runtime.setConfigProvider(TypesafeConfigProvider.fromResourcePath()),
      // ── Config layers ──────────────────────────────────────────────────
      ZLayer.fromZIO(ZIO.config[MongoConfig]),
      ZLayer.fromZIO(ZIO.config[KafkaConfig]),
      ZLayer.fromZIO(ZIO.config[SchedulerConfig]),
      // ── MongoDB ────────────────────────────────────────────────────────
      MongoClientLayer.live,
      MongoDatabaseLayer.live,
      MongoSchedulerRepository.live,
      // ── Tasks ──────────────────────────────────────────────────────────
      LiveSchedulerTasks.live
    )

  private val program: ZIO[SchedulerConfig & SchedulerTasks, Throwable, Unit] =
    for
      cfg   <- ZIO.service[SchedulerConfig]
      tasks <- ZIO.service[SchedulerTasks]
      _ <- ZIO.logInfo(
        s"ev-scheduler starting — " +
          s"offlineStations=${cfg.offlineStationsTaskEnabled} " +
          s"closeTransactions=${cfg.closeTransactionsTaskEnabled} " +
          s"cleanupLogs=${cfg.cleanupLogsTaskEnabled} " +
          s"userInactivity=${cfg.userInactivityTaskEnabled}"
      )
      // All tasks loop forever in their own fibers; startAll blocks until all fibers exit
      _ <- tasks.startAll
        .tapError(err => ZIO.logError(s"Scheduler error: ${err.getMessage}"))
        .ignore
      _ <- ZIO.never
    yield ()
