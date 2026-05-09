package io.itbeans.ev.notification

import io.itbeans.ev.domain.TenantId
import io.itbeans.ev.notification.email.EmailNotificationTask
import io.itbeans.ev.notification.fcm.FcmNotificationTask
import zio._

// ---------------------------------------------------------------------------
// NotificationService — deduplication + multi-channel dispatch.
//
// For each NotificationRequest:
//  1. For every requested channel, check the MongoDB dedup store.
//  2. Skip channels that already delivered within the rate-limit window.
//  3. Dispatch to the channel task (email / FCM).
//  4. On success, record the delivery in the dedup store.
//  5. Channel failures are logged and ignored — one channel failing does not
//     prevent delivery on the other.
//
// Rate-limit windows mirror TypeScript NotificationHandler.ts:
//   hasNotifiedSource(tenant, channel, sourceDescr, chargeBoxID, userId, { intervalMins: N })
// ---------------------------------------------------------------------------

trait NotificationService:
  def dispatch(req: NotificationRequest): Task[Unit]

final class DefaultNotificationService(
    email: EmailNotificationTask,
    fcm: FcmNotificationTask,
    storage: NotificationStorage
) extends NotificationService:

  override def dispatch(req: NotificationRequest): Task[Unit] =
    val tenantId = TenantId(req.tenantId)
    for
      _ <- ZIO.logInfo(
        s"Dispatching ${req.notificationType} tenant=${req.tenantId} " +
          s"user=${req.userId.getOrElse("-")} channels=${req.channels}"
      )
      _ <- ZIO.foreachDiscard(req.channels.toList)(dispatchChannel(tenantId, req, _))
    yield ()

  private def dispatchChannel(
      tenantId: TenantId,
      req: NotificationRequest,
      channel: NotificationChannel
  ): Task[Unit] =
    val interval = rateLimitMinutes(req.notificationType)
    for
      skip <- storage.hasAlreadySent(tenantId, channel, req.sourceId, interval)
      _ <-
        if skip then
          ZIO.logDebug(
            s"${req.notificationType}/$channel already sent within ${interval}m — suppressing"
          )
        else sendAndRecord(tenantId, req, channel)
    yield ()

  private def sendAndRecord(
      tenantId: TenantId,
      req: NotificationRequest,
      channel: NotificationChannel
  ): Task[Unit] =
    val send = channel match
      case NotificationChannel.Email                  => email.send(req)
      case NotificationChannel.RemotePushNotification => fcm.send(req)
    for
      // Ignore send errors — log them, mark as sent to prevent retry storms
      _ <- send
        .tapError(err =>
          ZIO.logError(
            s"Channel $channel failed for ${req.notificationType}: ${err.getMessage}"
          )
        )
        .ignore
      _ <- storage.markSent(tenantId, channel, req.sourceId, req.notificationType)
    yield ()

  /** Rate-limit windows per notification type (minutes). 0 = no limit. */
  private def rateLimitMinutes(nt: NotificationType): Int = nt match
    case NotificationType.OfflineChargingStations       => 24 * 60      // 1 day
    case NotificationType.UserAccountInactivity         => 30 * 24 * 60 // 30 days
    case NotificationType.OcpiPatchStatusError          => 60
    case NotificationType.OicpPatchStatusError          => 60
    case NotificationType.ComputeChargingProfilesFailed => 60
    case NotificationType.ChargingStationStatusError    => 10
    case NotificationType.VerificationEmail             => 10
    case NotificationType.PreparingSessionNotStarted    => 15
    case _                                              => 0

object DefaultNotificationService:

  val live: ZLayer[EmailNotificationTask & FcmNotificationTask & NotificationStorage, Nothing, NotificationService] =
    ZLayer.fromFunction(new DefaultNotificationService(_, _, _))
