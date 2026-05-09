package io.itbeans.ev.restapi

import io.circe.generic.auto._
import RestApiCodecs.given
import sttp.tapir.generic.auto._
import sttp.tapir.json.circe._
import sttp.tapir.ztapir._
import zio._

// ---------------------------------------------------------------------------
// RoamingEndpoints — OCPI and OICP endpoint management.
//
// Mirrors: OCPIService.ts (management endpoints — not the OCPI protocol handler)
//          OICPService.ts
//
// Note: The actual OCPI/OICP protocol (CDR push, token sync, etc.) is handled
// by ev-roaming. These endpoints manage the endpoint configuration records
// in MongoDB and allow manual CDR synchronisation triggers.
// ---------------------------------------------------------------------------

case class OcpiEndpoint(
    id: String,
    tenantId: String,
    name: String,
    role: String, // "CPO" | "EMSP"
    countryCode: String,
    partyId: String,
    versionUrl: String,
    version: String,
    localToken: String,
    remoteToken: Option[String],
    status: String, // "New" | "Pending" | "Registered"
    patchVersion: Int,
    createdOn: Long,
    lastChangedOn: Long
)

case class OcpiEndpointsResponse(result: List[OcpiEndpoint], count: Int)

object RoamingEndpoints:

  private val tenantHeader = header[String]("X-Tenant-ID")
  private val base         = endpoint.errorOut(stringBody)

  // ── OCPI Endpoints ────────────────────────────────────────────────────────

  private val listOcpiEp = base.get
    .in("api" / "v1" / "OcpiEndpoints")
    .in(tenantHeader)
    .in(query[Option[Int]]("Limit").default(None))
    .in(query[Option[Int]]("Skip").default(None))
    .out(jsonBody[OcpiEndpointsResponse])

  private val getOcpiEp = base.get
    .in("api" / "v1" / "OcpiEndpoints" / path[String]("id"))
    .in(tenantHeader)
    .out(jsonBody[OcpiEndpoint])

  private val createOcpiEp = base.post
    .in("api" / "v1" / "OcpiEndpoints")
    .in(tenantHeader)
    .in(jsonBody[io.circe.Json])
    .out(statusCode(sttp.model.StatusCode.Created))

  private val updateOcpiEp = base.put
    .in("api" / "v1" / "OcpiEndpoints" / path[String]("id"))
    .in(tenantHeader)
    .in(jsonBody[io.circe.Json])
    .out(jsonBody[OcpiEndpoint])

  private val deleteOcpiEp = base.delete
    .in("api" / "v1" / "OcpiEndpoints" / path[String]("id"))
    .in(tenantHeader)
    .out(statusCode(sttp.model.StatusCode.NoContent))

  /** Trigger manual CDR push to a remote OCPI endpoint. */
  private val syncOcpiCdrsEp = base.post
    .in("api" / "v1" / "OcpiEndpoints" / path[String]("id") / "PushCdrs")
    .in(tenantHeader)
    .in(query[Option[Long]]("StartDateTime"))
    .in(query[Option[Long]]("EndDateTime"))
    .out(jsonBody[io.circe.Json])

  /** Trigger manual token pull from a remote EMSP. */
  private val syncOcpiTokensEp = base.post
    .in("api" / "v1" / "OcpiEndpoints" / path[String]("id") / "PullTokens")
    .in(tenantHeader)
    .out(jsonBody[io.circe.Json])

  // ── OICP Endpoints ────────────────────────────────────────────────────────

  private val listOicpEp = base.get
    .in("api" / "v1" / "OicpEndpoints")
    .in(tenantHeader)
    .in(query[Option[Int]]("Limit").default(None))
    .in(query[Option[Int]]("Skip").default(None))
    .out(jsonBody[io.circe.Json]) // OICP has different structure — use raw JSON

  private val syncOicpEVSEsEp = base.post
    .in("api" / "v1" / "OicpEndpoints" / path[String]("id") / "PushEvseData")
    .in(tenantHeader)
    .out(jsonBody[io.circe.Json])

  def routes(repo: RestApiRepository): List[ZServerEndpoint[Any, Any]] = List(
    listOcpiEp.zServerLogic { case (tenantId, limit, skip) =>
      ZIO.succeed(OcpiEndpointsResponse(Nil, 0))
    },
    getOcpiEp.zServerLogic { case (id, tenantId) =>
      ZIO.fail(s"OcpiEndpoint $id not found")
    },
    createOcpiEp.zServerLogic { case (tenantId, _) =>
      ZIO.unit
    },
    updateOcpiEp.zServerLogic { case (id, tenantId, _) =>
      ZIO.fail(s"OcpiEndpoint $id not found")
    },
    deleteOcpiEp.zServerLogic { case (id, tenantId) =>
      ZIO.unit
    },
    syncOcpiCdrsEp.zServerLogic { case (id, tenantId, from, to) =>
      ZIO.succeed(io.circe.Json.obj("pushed" -> io.circe.Json.fromInt(0)))
    },
    syncOcpiTokensEp.zServerLogic { case (id, tenantId) =>
      ZIO.succeed(io.circe.Json.obj("pulled" -> io.circe.Json.fromInt(0)))
    },
    listOicpEp.zServerLogic { case (tenantId, limit, skip) =>
      ZIO.succeed(io.circe.Json.obj("result" -> io.circe.Json.arr(), "count" -> io.circe.Json.fromInt(0)))
    },
    syncOicpEVSEsEp.zServerLogic { case (id, tenantId) =>
      ZIO.succeed(io.circe.Json.obj("pushed" -> io.circe.Json.fromInt(0)))
    }
  )
