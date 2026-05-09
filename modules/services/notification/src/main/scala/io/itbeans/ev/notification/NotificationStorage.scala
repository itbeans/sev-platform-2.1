package io.itbeans.ev.notification

import io.itbeans.ev.domain.TenantId
import io.itbeans.ev.mongo.TenantCollection
import org.mongodb.scala._
import org.mongodb.scala.Document
import org.mongodb.scala.model.Filters
import zio._

import java.time.Instant
import java.time.temporal.ChronoUnit

// ---------------------------------------------------------------------------
// NotificationStorage — MongoDB-backed deduplication store.
//
// Mirrors TypeScript NotificationStorage.ts behaviour:
//   - Collection: {tenantId}.notifications
//   - Document schema: { sourceId, channel, notificationType, timestamp }
//   - hasAlreadySent checks for any document with matching sourceId + channel
//     whose timestamp is within the rate-limit window
//
// No TTL index is configured here; the TypeScript cleanup task
// (LoggingDatabaseTableCleanupTask) handles collection pruning until the
// ev-scheduler service takes over that responsibility.
// ---------------------------------------------------------------------------

trait NotificationStorage:

  def hasAlreadySent(
      tenantId: TenantId,
      channel: NotificationChannel,
      sourceId: String,
      intervalMins: Int
  ): Task[Boolean]

  def markSent(
      tenantId: TenantId,
      channel: NotificationChannel,
      sourceId: String,
      notificationType: NotificationType
  ): Task[Unit]

final class MongoNotificationStorage(db: MongoDatabase) extends NotificationStorage:

  private def col(tenantId: TenantId): TenantCollection[Document] =
    TenantCollection[Document]("notifications", tenantId, db)

  override def hasAlreadySent(
      tenantId: TenantId,
      channel: NotificationChannel,
      sourceId: String,
      intervalMins: Int
  ): Task[Boolean] =
    if intervalMins <= 0 then ZIO.succeed(false)
    else
      val since = Instant.now().minus(intervalMins.toLong, ChronoUnit.MINUTES).toString
      col(tenantId)
        .findOne(
          Filters.and(
            Filters.eq("sourceId", sourceId),
            Filters.eq("channel", channel.toString),
            Filters.gte("timestamp", since)
          )
        )
        .map(_.isDefined)

  override def markSent(
      tenantId: TenantId,
      channel: NotificationChannel,
      sourceId: String,
      notificationType: NotificationType
  ): Task[Unit] =
    col(tenantId).insertOne(
      Document(
        "sourceId"         -> sourceId,
        "channel"          -> channel.toString,
        "notificationType" -> notificationType.toString,
        "timestamp"        -> Instant.now().toString
      )
    )

object MongoNotificationStorage:

  val live: ZLayer[MongoDatabase, Nothing, NotificationStorage] =
    ZLayer.fromFunction(new MongoNotificationStorage(_))
