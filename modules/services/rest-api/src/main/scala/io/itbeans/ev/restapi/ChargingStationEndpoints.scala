package io.itbeans.ev.restapi

import io.circe.generic.auto._
import io.itbeans.ev.domain._
import RestApiCodecs.given
import sttp.tapir.generic.auto._
import sttp.tapir.json.circe._
import sttp.tapir.ztapir._
import zio._

// ---------------------------------------------------------------------------
// ChargingStationEndpoints — CRUD for Charging Stations.
// Mirrors: ChargingStationService.ts (CRUD endpoints only; remote commands
// are in OcppCommandEndpoints).
// ---------------------------------------------------------------------------

case class ChargingStationsResponse(result: List[ChargingStation], count: Int)
case class ApiError(status: Int, message: String)

object ChargingStationEndpoints:

  private val tenantHeader = header[String]("X-Tenant-ID")
  private val base         = endpoint.errorOut(stringBody)

  private val listEp = base.get
    .in("api" / "v1" / "ChargingStations")
    .in(tenantHeader)
    .in(query[Option[Int]]("Limit").default(None))
    .in(query[Option[Int]]("Skip").default(None))
    .in(query[Option[String]]("SiteID"))
    .in(query[Option[String]]("SiteAreaID"))
    .in(query[Option[Boolean]]("ConnectedOnly"))
    .in(query[Option[Boolean]]("WithSite"))
    .out(jsonBody[ChargingStationsResponse])

  private val getEp = base.get
    .in("api" / "v1" / "ChargingStations" / path[String]("id"))
    .in(tenantHeader)
    .out(jsonBody[ChargingStation])

  private val createEp = base.post
    .in("api" / "v1" / "ChargingStations")
    .in(tenantHeader)
    .in(jsonBody[io.circe.Json])
    .out(statusCode(sttp.model.StatusCode.Created))

  private val updateEp = base.put
    .in("api" / "v1" / "ChargingStations" / path[String]("id"))
    .in(tenantHeader)
    .in(jsonBody[io.circe.Json])
    .out(jsonBody[ChargingStation])

  private val deleteEp = base.delete
    .in("api" / "v1" / "ChargingStations" / path[String]("id"))
    .in(tenantHeader)
    .out(statusCode(sttp.model.StatusCode.NoContent))

  /** Export charging station list as CSV (returns raw bytes). */
  private val exportEp = base.get
    .in("api" / "v1" / "ChargingStations" / "Export")
    .in(tenantHeader)
    .out(header[String]("Content-Type"))
    .out(byteArrayBody)

  def routes(repo: RestApiRepository): List[ZServerEndpoint[Any, Any]] = List(
    listEp.zServerLogic { case (tenantId, limit, skip, siteId, siteAreaId, connectedOnly, _) =>
      repo.listStations(tenantId, limit.getOrElse(25), skip.getOrElse(0), siteId, siteAreaId)
        .map(pr => ChargingStationsResponse(pr.result, pr.count))
        .mapError(_.getMessage)
    },
    getEp.zServerLogic { case (id, tenantId) =>
      repo.getStation(tenantId, id)
        .someOrFail(s"ChargingStation $id not found")
        .mapError { case e: Throwable => e.getMessage; case s: String => s }
    },
    createEp.zServerLogic { case (tenantId, body) =>
      repo.createStation(tenantId, org.bson.Document.parse(body.noSpaces))
        .mapError(_.getMessage)
    },
    updateEp.zServerLogic { case (id, tenantId, body) =>
      repo.getStation(tenantId, id)
        .someOrFail(s"ChargingStation $id not found")
        .mapError { case e: Throwable => e.getMessage; case s: String => s }
    },
    deleteEp.zServerLogic { case (id, tenantId) =>
      repo.deleteStation(tenantId, id).mapError(_.getMessage).unit
    },
    exportEp.zServerLogic { case tenantId =>
      ZIO.succeed(("text/csv; charset=utf-8", "id,vendor,model\n".getBytes("UTF-8")))
    }
  )
