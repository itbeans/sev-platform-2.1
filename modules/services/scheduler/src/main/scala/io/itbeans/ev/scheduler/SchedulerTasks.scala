package io.itbeans.ev.scheduler

import io.circe.syntax._
import io.circe.generic.semiauto._
import io.circe.{Encoder, Json}
import io.itbeans.ev.kafka.{KafkaConfig, Topics}
import zio._
import zio.kafka.producer.{Producer, ProducerSettings}
import zio.kafka.serde.Serde

// ---------------------------------------------------------------------------
// SchedulerTasks — all periodic background tasks for ev-scheduler.
//
// Tasks:
//   1. checkOfflineStations   — every cfg.checkOfflineStationsIntervalMins
//   2. closeStaleTransactions — every cfg.closeTransactionsIntervalHours
//   3. cleanupLogs            — every cfg.cleanupLogsIntervalHours
//   4. checkUserInactivity    — every cfg.checkUserInactivityIntervalHours
//
// Each task:
//   a. Acquires a distributed MongoDB lock (prevents double-execution across pods)
//   b. Iterates all active tenants
//   c. Performs the task for each tenant
//   d. Releases the lock
//
// Side effects go to Kafka (notifications.outbound) — the scheduler never
// sends email directly; that is the notification service's responsibility.
// ---------------------------------------------------------------------------

case class SchedulerNotificationPayload(
    tenantId: String,
    notificationType: String, // "StationOffline" | "UserInactivity"
    recipientUserId: Option[String],
    recipientEmail: Option[String],
    extraData: Map[String, String]
)

object SchedulerNotificationPayload:
  given Encoder[SchedulerNotificationPayload] = deriveEncoder

// ── Task runner ────────────────────────────────────────────────────────────

trait SchedulerTasks:
  def startAll: Task[Unit]

final class LiveSchedulerTasks(
    repo: SchedulerRepository,
    cfg: SchedulerConfig,
    kafkaCfg: KafkaConfig
) extends SchedulerTasks:

  def startAll: Task[Unit] =
    val enabledTasks = List(
      Option.when(cfg.offlineStationsTaskEnabled)(
        loopTask("offline-stations", cfg.checkOfflineStationsIntervalMins.minutes)(checkOfflineStations)
      ),
      Option.when(cfg.closeTransactionsTaskEnabled)(
        loopTask("close-transactions", cfg.closeTransactionsIntervalHours.hours)(closeStaleTransactions)
      ),
      Option.when(cfg.cleanupLogsTaskEnabled)(
        loopTask("cleanup-logs", cfg.cleanupLogsIntervalHours.hours)(cleanupLogs)
      ),
      Option.when(cfg.userInactivityTaskEnabled)(
        loopTask("user-inactivity", cfg.checkUserInactivityIntervalHours.hours)(checkUserInactivity)
      )
    ).flatten
    ZIO.foreachParDiscard(enabledTasks)(identity)

  // ── Scheduling loop ───────────────────────────────────────────────────

  /** Sleep for `interval`, then run `body`, repeat forever. Errors are logged but do not stop the loop. */
  private def loopTask(name: String, interval: Duration)(body: Task[Unit]): Task[Unit] =
    (ZIO.sleep(interval) *>
      ZIO.logDebug(s"[Scheduler] Running task: $name") *>
      body
        .tapError(err => ZIO.logError(s"[Scheduler] Task $name failed: ${err.getMessage}"))
        .ignore).forever

  // ── Task 1: Check Offline Stations ────────────────────────────────────

  private def checkOfflineStations: Task[Unit] =
    withLock("scheduler.offline-stations") {
      val thresholdMs = java.lang.System.currentTimeMillis() - (cfg.stationOfflineThresholdSecs * 1000L)
      for
        tenants <- repo.listTenantIds
        _ <- ZIO.foreachDiscard(tenants) { tenantId =>
          for
            stationIds <- repo.findOfflineStationIds(tenantId, thresholdMs)
            _ <- ZIO.when(stationIds.nonEmpty)(
              ZIO.logInfo(
                s"[Scheduler] Offline stations for $tenantId: ${stationIds.size}"
              )
            )
            _ <- ZIO.foreachDiscard(stationIds) { stationId =>
              repo.markStationOffline(tenantId, stationId) *>
                publishNotification(SchedulerNotificationPayload(
                  tenantId = tenantId,
                  notificationType = "StationOffline",
                  recipientUserId = None,
                  recipientEmail = None,
                  extraData = Map("chargingStationId" -> stationId)
                ))
            }
          yield ()
        }
      yield ()
    }

  // ── Task 2: Close Stale Transactions ─────────────────────────────────

  private def closeStaleTransactions: Task[Unit] =
    withLock("scheduler.close-transactions") {
      val cutoffMs = java.lang.System.currentTimeMillis() - (cfg.staleTransactionThresholdHours.toLong * 3600 * 1000)
      val nowMs    = java.lang.System.currentTimeMillis()
      for
        tenants <- repo.listTenantIds
        _ <- ZIO.foreachDiscard(tenants) { tenantId =>
          for
            txIds <- repo.findStaleOpenTransactionIds(tenantId, cutoffMs)
            _ <- ZIO.when(txIds.nonEmpty)(
              ZIO.logInfo(
                s"[Scheduler] Force-closing ${txIds.size} stale transactions for $tenantId"
              )
            )
            _ <- ZIO.foreachDiscard(txIds) { txId =>
              repo.forceCloseTransaction(tenantId, txId, nowMs)
                .tapError(err =>
                  ZIO.logWarning(
                    s"[Scheduler] Failed to close tx $txId: ${err.getMessage}"
                  )
                )
                .ignore
            }
          yield ()
        }
      yield ()
    }

  // ── Task 3: Cleanup Logs ──────────────────────────────────────────────

  private def cleanupLogs: Task[Unit] =
    withLock("scheduler.cleanup-logs") {
      val cutoffMs = java.lang.System.currentTimeMillis() - (cfg.logRetentionDays.toLong * 86400 * 1000)
      for
        tenants <- repo.listTenantIds
        _ <- ZIO.foreachDiscard(tenants) { tenantId =>
          for
            logsDeleted  <- repo.deleteOldLogs(tenantId, cutoffMs)
            perfsDeleted <- repo.deleteOldPerformances(tenantId, cutoffMs)
            _ <- ZIO.logInfo(
              s"[Scheduler] Cleanup $tenantId: " +
                s"$logsDeleted logs, $perfsDeleted perf records deleted"
            )
          yield ()
        }
      yield ()
    }

  // ── Task 4: Check User Inactivity ─────────────────────────────────────

  private def checkUserInactivity: Task[Unit] =
    withLock("scheduler.user-inactivity") {
      val cutoffMs = java.lang.System.currentTimeMillis() -
        (cfg.userInactivityMonths.toLong * 30 * 86400 * 1000)
      for
        tenants <- repo.listTenantIds
        _ <- ZIO.foreachDiscard(tenants) { tenantId =>
          for
            users <- repo.findInactiveUsers(tenantId, cutoffMs)
            _ <- ZIO.when(users.nonEmpty)(
              ZIO.logInfo(
                s"[Scheduler] Inactive users for $tenantId: ${users.size}"
              )
            )
            _ <- ZIO.foreachDiscard(users) { case (userId, email) =>
              publishNotification(SchedulerNotificationPayload(
                tenantId = tenantId,
                notificationType = "UserInactivity",
                recipientUserId = Some(userId),
                recipientEmail = Some(email),
                extraData = Map(
                  "inactivityMonths" -> cfg.userInactivityMonths.toString
                )
              )).ignore
            }
          yield ()
        }
      yield ()
    }

  // ── Helpers ───────────────────────────────────────────────────────────

  /** Run body only if the distributed lock is acquired; always release on exit. */
  private def withLock(lockKey: String)(body: Task[Unit]): Task[Unit] =
    repo.acquireLock(lockKey, cfg.lockDurationSecs).flatMap {
      case false =>
        ZIO.logDebug(s"[Scheduler] Lock $lockKey held by another instance — skipping")
      case true =>
        body.ensuring(repo.releaseLock(lockKey).ignore)
    }

  /** Publish a notification request to the notifications.outbound Kafka topic. */
  private def publishNotification(payload: SchedulerNotificationPayload): Task[Unit] =
    ZIO.scoped {
      Producer.make(ProducerSettings(kafkaCfg.bootstrapServers)).flatMap { producer =>
        producer.produce(
          topic = Topics.notificationsOutbound,
          key = payload.tenantId,
          value = payload.asJson.noSpaces,
          keySerializer = Serde.string,
          valueSerializer = Serde.string
        ).unit
      }
    }

object LiveSchedulerTasks:

  val live: ZLayer[SchedulerRepository & SchedulerConfig & KafkaConfig, Nothing, SchedulerTasks] =
    ZLayer.fromFunction(new LiveSchedulerTasks(_, _, _))
