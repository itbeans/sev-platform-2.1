package io.itbeans.ev.restapi

import io.circe.generic.auto._
import RestApiCodecs.given
import sttp.tapir.generic.auto._
import sttp.tapir.json.circe._
import sttp.tapir.ztapir._
import zio._

// ---------------------------------------------------------------------------
// NotificationEndpoints — user notification management.
//
// Mirrors: NotificationService.ts (REST portion)
// Outbound delivery (email/push) is handled by ev-notification via Kafka.
// This endpoint manages the notification records in MongoDB for the dashboard.
// ---------------------------------------------------------------------------

case class Notification(
    id: String,
    tenantId: String,
    userId: String,
    channel: String, // "Email" | "Push" | "SMS"
    sourceDescriptor: String,
    chargeBoxID: Option[String],
    timestamp: Long,
    data: io.circe.Json
)

case class NotificationsResponse(result: List[Notification], count: Int)

object NotificationEndpoints:

  private val tenantHeader = header[String]("X-Tenant-ID")
  private val base         = endpoint.errorOut(stringBody)

  private val listEp = base.get
    .in("api" / "v1" / "Notifications")
    .in(tenantHeader)
    .in(query[Option[Int]]("Limit").default(None))
    .in(query[Option[Int]]("Skip").default(None))
    .in(query[Option[String]]("UserID"))
    .in(query[Option[String]]("Channel"))
    .out(jsonBody[NotificationsResponse])

  private val deleteEp = base.delete
    .in("api" / "v1" / "Notifications" / path[String]("id"))
    .in(tenantHeader)
    .out(statusCode(sttp.model.StatusCode.NoContent))

  /** User notification preferences */
  private val getPreferencesEp = base.get
    .in("api" / "v1" / "Users" / path[String]("userId") / "Notifications")
    .in(tenantHeader)
    .out(jsonBody[io.circe.Json])

  private val updatePreferencesEp = base.put
    .in("api" / "v1" / "Users" / path[String]("userId") / "Notifications")
    .in(tenantHeader)
    .in(jsonBody[io.circe.Json])
    .out(jsonBody[io.circe.Json])

  private def docToNotification(doc: org.bson.Document): Option[Notification] =
    for
      id     <- Option(doc.getString("_id"))
      userId <- Option(doc.getString("userId"))
    yield Notification(
      id = id,
      tenantId = Option(doc.getString("tenantId")).getOrElse(""),
      userId = userId,
      channel = Option(doc.getString("channel")).getOrElse("Push"),
      sourceDescriptor = Option(doc.getString("sourceDescriptor")).getOrElse(""),
      chargeBoxID = Option(doc.getString("chargeBoxID")),
      timestamp = Option(doc.get("timestamp")).flatMap(v => v.toString.toLongOption).getOrElse(0L),
      data = io.circe.parser.parse(
        Option(doc.get("data")).map(_.toString).getOrElse("{}")
      ).getOrElse(io.circe.Json.obj())
    )

  def routes(repo: RestApiRepository): List[ZServerEndpoint[Any, Any]] = List(
    listEp.zServerLogic { case (tenantId, limit, skip, userId, channel) =>
      repo.listNotifications(tenantId, limit.getOrElse(25), skip.getOrElse(0), userId, channel)
        .map { pr =>
          NotificationsResponse(pr.result.flatMap(docToNotification), pr.count)
        }
        .mapError(_.getMessage)
    },
    deleteEp.zServerLogic { case (id, tenantId) =>
      repo.deleteNotification(tenantId, id).mapError(_.getMessage).unit
    },
    getPreferencesEp.zServerLogic { case (userId, tenantId) =>
      repo.getNotificationPreferences(tenantId, userId)
        .map(_.getOrElse(io.circe.Json.obj()))
        .mapError(_.getMessage)
    },
    updatePreferencesEp.zServerLogic { case (userId, tenantId, body) =>
      repo.upsertNotificationPreferences(tenantId, userId, body)
        .as(body)
        .mapError(_.getMessage)
    }
  )
