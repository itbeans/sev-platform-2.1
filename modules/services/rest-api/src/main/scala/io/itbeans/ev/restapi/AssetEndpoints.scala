package io.itbeans.ev.restapi

import io.circe.generic.auto._
import RestApiCodecs.given
import sttp.tapir.generic.auto._
import sttp.tapir.json.circe._
import sttp.tapir.ztapir._
import zio._

import java.time.Instant

// ---------------------------------------------------------------------------
// AssetEndpoints — CRUD for Assets (solar panels, batteries, EV fleets).
// Mirrors: AssetService.ts
//
// Asset is defined here as a REST DTO (not in ev-domain-types) because it
// is specific to the REST API layer and to keep domain types focused.
// ---------------------------------------------------------------------------

/** REST DTO for an Asset. */
case class Asset(
    id: String,
    tenantId: String,
    siteAreaId: Option[String],
    siteId: Option[String],
    name: String,
    assetType: String, // "SC" | "EV" | "PR" | "CO" | "GE" | "UN"
    currentInstantWatts: Option[Double],
    currentStateOfCharge: Option[Int], // % for batteries
    connectionStatus: Option[String],
    dynamic: Boolean,
    createdOn: Instant,
    lastChangedOn: Instant
)

case class AssetsResponse(result: List[Asset], count: Int)

object AssetEndpoints:

  private val tenantHeader = header[String]("X-Tenant-ID")
  private val base         = endpoint.errorOut(stringBody)

  private val listEp = base.get
    .in("api" / "v1" / "Assets")
    .in(tenantHeader)
    .in(query[Option[Int]]("Limit").default(None))
    .in(query[Option[Int]]("Skip").default(None))
    .in(query[Option[String]]("SiteAreaID"))
    .in(query[Option[String]]("AssetType"))
    .in(query[Option[Boolean]]("WithSiteArea"))
    .out(jsonBody[AssetsResponse])

  private val getEp = base.get
    .in("api" / "v1" / "Assets" / path[String]("id"))
    .in(tenantHeader)
    .out(jsonBody[Asset])

  private val createEp = base.post
    .in("api" / "v1" / "Assets")
    .in(tenantHeader)
    .in(jsonBody[io.circe.Json])
    .out(statusCode(sttp.model.StatusCode.Created))

  private val updateEp = base.put
    .in("api" / "v1" / "Assets" / path[String]("id"))
    .in(tenantHeader)
    .in(jsonBody[io.circe.Json])
    .out(jsonBody[Asset])

  private val deleteEp = base.delete
    .in("api" / "v1" / "Assets" / path[String]("id"))
    .in(tenantHeader)
    .out(statusCode(sttp.model.StatusCode.NoContent))

  private val checkConnectionEp = base.get
    .in("api" / "v1" / "Assets" / path[String]("id") / "CheckConnection")
    .in(tenantHeader)
    .out(jsonBody[io.circe.Json]) // returns { connectionStatus: "Connected" | "Disconnected" }

  def routes(repo: RestApiRepository): List[ZServerEndpoint[Any, Any]] = List(
    listEp.zServerLogic { case (tenantId, limit, skip, siteAreaId, assetType, _) =>
      // Asset collection lives in MongoDB under {tenantId}.assets
      ZIO.succeed(AssetsResponse(Nil, 0))
    },
    getEp.zServerLogic { case (id, tenantId) =>
      ZIO.fail(s"Asset $id not found")
    },
    createEp.zServerLogic { case (tenantId, _) =>
      ZIO.unit
    },
    updateEp.zServerLogic { case (id, tenantId, _) =>
      ZIO.fail(s"Asset $id not found")
    },
    deleteEp.zServerLogic { case (id, tenantId) =>
      ZIO.unit
    },
    checkConnectionEp.zServerLogic { case (id, tenantId) =>
      ZIO.succeed(io.circe.Json.obj("connectionStatus" -> io.circe.Json.fromString("Disconnected")))
    }
  )
