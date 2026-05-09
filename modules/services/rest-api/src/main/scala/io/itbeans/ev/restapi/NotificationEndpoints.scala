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

  def routes(repo: RestApiRepository): List[ZServerEndpoint[Any, Any]] = List(
    listEp.zServerLogic { case (tenantId, limit, skip, userId, channel) =>
      ZIO.succeed(NotificationsResponse(Nil, 0))
    },
    deleteEp.zServerLogic { case (id, tenantId) =>
      ZIO.unit
    },
    getPreferencesEp.zServerLogic { case (userId, tenantId) =>
      ZIO.succeed(io.circe.Json.obj())
    },
    updatePreferencesEp.zServerLogic { case (userId, tenantId, body) =>
      ZIO.succeed(body)
    }
  )
