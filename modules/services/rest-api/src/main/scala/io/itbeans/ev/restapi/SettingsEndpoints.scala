package io.itbeans.ev.restapi

import io.circe.generic.auto._
import RestApiCodecs.given
import sttp.tapir.generic.auto._
import sttp.tapir.json.circe._
import sttp.tapir.ztapir._
import zio._

// ---------------------------------------------------------------------------
// SettingsEndpoints — per-component tenant settings.
//
// Mirrors: SettingService.ts
// Each "active component" (Pricing, Billing, SmartCharging, OCPI, OICP,
// Analytics, Car, Asset, UserInactivity, ...) has its own settings document.
//
// GET  /api/v1/Settings/{component}
// PUT  /api/v1/Settings/{component}
// ---------------------------------------------------------------------------

object SettingsEndpoints:

  private val tenantHeader = header[String]("X-Tenant-ID")
  private val base         = endpoint.errorOut(stringBody)

  /** Returns the JSON settings document for a tenant component. */
  private val getSettingEp = base.get
    .in("api" / "v1" / "Settings" / path[String]("component"))
    .in(tenantHeader)
    .out(jsonBody[io.circe.Json])

  /** Replaces the settings document for a tenant component. */
  private val updateSettingEp = base.put
    .in("api" / "v1" / "Settings" / path[String]("component"))
    .in(tenantHeader)
    .in(jsonBody[io.circe.Json])
    .out(jsonBody[io.circe.Json])

  // ── Registration Tokens ───────────────────────────────────────────────────

  case class RegistrationToken(
      id: String,
      tenantId: String,
      description: String,
      expirationDate: Option[Long], // epoch millis
      revocationDate: Option[Long],
      siteAreaId: Option[String],
      createdOn: Long,
      lastChangedOn: Long
  )

  case class RegistrationTokensResponse(result: List[io.circe.Json], count: Int)

  private val listTokensEp = base.get
    .in("api" / "v1" / "RegistrationTokens")
    .in(tenantHeader)
    .in(query[Option[Int]]("Limit").default(None))
    .in(query[Option[Int]]("Skip").default(None))
    .out(jsonBody[RegistrationTokensResponse])

  private val getTokenEp = base.get
    .in("api" / "v1" / "RegistrationTokens" / path[String]("id"))
    .in(tenantHeader)
    .out(jsonBody[io.circe.Json])

  private val createTokenEp = base.post
    .in("api" / "v1" / "RegistrationTokens")
    .in(tenantHeader)
    .in(jsonBody[io.circe.Json])
    .out(statusCode(sttp.model.StatusCode.Created))

  private val deleteTokenEp = base.delete
    .in("api" / "v1" / "RegistrationTokens" / path[String]("id"))
    .in(tenantHeader)
    .out(statusCode(sttp.model.StatusCode.NoContent))

  def routes(repo: RestApiRepository): List[ZServerEndpoint[Any, Any]] = List(
    getSettingEp.zServerLogic { case (component, tenantId) =>
      repo.getSetting(tenantId, component).map {
        case Some(json) => json
        case None       => io.circe.Json.obj("identifier" -> io.circe.Json.fromString(component))
      }.mapError(_.getMessage)
    },
    updateSettingEp.zServerLogic { case (component, tenantId, body) =>
      repo.upsertSetting(tenantId, component, body).as(body).mapError(_.getMessage)
    },
    listTokensEp.zServerLogic { case (tenantId, limit, skip) =>
      repo
        .listRegistrationTokens(tenantId, limit.getOrElse(50), skip.getOrElse(0))
        .map(r => RegistrationTokensResponse(r.result, r.count))
        .mapError(_.getMessage)
    },
    getTokenEp.zServerLogic { case (id, tenantId) =>
      repo.getRegistrationToken(tenantId, id).flatMap {
        case Some(json) => ZIO.succeed(json)
        case None       => ZIO.fail(s"RegistrationToken $id not found")
      }.mapError(_.toString)
    },
    createTokenEp.zServerLogic { case (tenantId, body) =>
      val doc = org.bson.Document.parse(body.noSpaces)
      repo.createRegistrationToken(tenantId, doc).ignore
    },
    deleteTokenEp.zServerLogic { case (id, tenantId) =>
      repo.deleteRegistrationToken(tenantId, id).ignore
    }
  )
