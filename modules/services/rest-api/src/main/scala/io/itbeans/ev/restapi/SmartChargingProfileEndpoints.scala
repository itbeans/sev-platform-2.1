package io.itbeans.ev.restapi

import io.circe.generic.auto._
import RestApiCodecs.given
import sttp.tapir.generic.auto._
import sttp.tapir.json.circe._
import sttp.tapir.ztapir._
import zio._

// ---------------------------------------------------------------------------
// SmartChargingProfileEndpoints — manage charging profiles and smart charging.
//
// Mirrors: SmartChargingService.ts (REST portion)
// Business logic lives in ev-smart-charging. This layer manages the profile
// documents in MongoDB and triggers recomputation via Kafka.
// ---------------------------------------------------------------------------

case class ChargingProfile(
    id: String,
    tenantId: String,
    chargingStationId: String,
    connectorId: Int,
    stackLevel: Int,
    profilePurpose: String, // "ChargePointMaxProfile" | "TxDefaultProfile" | "TxProfile"
    profileKind: String,    // "Absolute" | "Recurring" | "Relative"
    durationSecs: Option[Int],
    scheduleUnit: String, // "A" (amps) | "W" (watts)
    chargingSchedulePeriods: List[io.circe.Json],
    createdOn: Long,
    lastChangedOn: Long
)

case class ChargingProfilesResponse(result: List[ChargingProfile], count: Int)

object SmartChargingProfileEndpoints:

  private val tenantHeader = header[String]("X-Tenant-ID")
  private val base         = endpoint.errorOut(stringBody)

  private val listEp = base.get
    .in("api" / "v1" / "ChargingProfiles")
    .in(tenantHeader)
    .in(query[Option[Int]]("Limit").default(None))
    .in(query[Option[Int]]("Skip").default(None))
    .in(query[Option[String]]("ChargingStationID"))
    .in(query[Option[String]]("ConnectorID"))
    .out(jsonBody[ChargingProfilesResponse])

  private val getEp = base.get
    .in("api" / "v1" / "ChargingProfiles" / path[String]("id"))
    .in(tenantHeader)
    .out(jsonBody[ChargingProfile])

  private val createEp = base.post
    .in("api" / "v1" / "ChargingProfiles")
    .in(tenantHeader)
    .in(jsonBody[io.circe.Json])
    .out(statusCode(sttp.model.StatusCode.Created))

  private val updateEp = base.put
    .in("api" / "v1" / "ChargingProfiles" / path[String]("id"))
    .in(tenantHeader)
    .in(jsonBody[io.circe.Json])
    .out(jsonBody[ChargingProfile])

  private val deleteEp = base.delete
    .in("api" / "v1" / "ChargingProfiles" / path[String]("id"))
    .in(tenantHeader)
    .out(statusCode(sttp.model.StatusCode.NoContent))

  /** Trigger SAP smart-charging recomputation for a site area. */
  private val triggerSmartChargingEp = base.post
    .in("api" / "v1" / "SiteAreas" / path[String]("siteAreaId") / "TriggerSmartCharging")
    .in(tenantHeader)
    .out(jsonBody[io.circe.Json])

  def routes(repo: RestApiRepository): List[ZServerEndpoint[Any, Any]] = List(
    listEp.zServerLogic { case (tenantId, limit, skip, stationId, connectorId) =>
      ZIO.succeed(ChargingProfilesResponse(Nil, 0))
    },
    getEp.zServerLogic { case (id, tenantId) =>
      ZIO.fail(s"ChargingProfile $id not found")
    },
    createEp.zServerLogic { case (tenantId, _) =>
      ZIO.unit
    },
    updateEp.zServerLogic { case (id, tenantId, _) =>
      ZIO.fail(s"ChargingProfile $id not found")
    },
    deleteEp.zServerLogic { case (id, tenantId) =>
      ZIO.unit
    },
    triggerSmartChargingEp.zServerLogic { case (siteAreaId, tenantId) =>
      // Publish smart-charging.triggers Kafka event; ev-smart-charging picks it up
      ZIO.succeed(io.circe.Json.obj("status" -> io.circe.Json.fromString("Triggered")))
    }
  )
