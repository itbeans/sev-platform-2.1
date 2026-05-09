package io.itbeans.ev.restapi

import io.circe.generic.auto._
import io.itbeans.ev.domain._
import RestApiCodecs.given
import sttp.tapir.generic.auto._
import sttp.tapir.json.circe._
import sttp.tapir.ztapir._
import zio._

// ---------------------------------------------------------------------------
// CompanyEndpoints — CRUD for Companies.
// Mirrors: CompanyService.ts
// ---------------------------------------------------------------------------

object CompanyEndpoints:

  private val tenantHeader = header[String]("X-Tenant-ID")
  private val base         = endpoint.errorOut(stringBody)

  private val listEp = base.get
    .in("api" / "v1" / "Companies")
    .in(tenantHeader)
    .in(query[Option[Int]]("Limit").default(None))
    .in(query[Option[Int]]("Skip").default(None))
    .out(jsonBody[PagedResult[Company]])

  private val getEp = base.get
    .in("api" / "v1" / "Companies" / path[String]("id"))
    .in(tenantHeader)
    .out(jsonBody[Company])

  private val createEp = base.post
    .in("api" / "v1" / "Companies")
    .in(tenantHeader)
    .in(jsonBody[io.circe.Json])
    .out(statusCode(sttp.model.StatusCode.Created))

  private val updateEp = base.put
    .in("api" / "v1" / "Companies" / path[String]("id"))
    .in(tenantHeader)
    .in(jsonBody[io.circe.Json])
    .out(jsonBody[Company])

  private val deleteEp = base.delete
    .in("api" / "v1" / "Companies" / path[String]("id"))
    .in(tenantHeader)
    .out(statusCode(sttp.model.StatusCode.NoContent))

  def routes(repo: RestApiRepository): List[ZServerEndpoint[Any, Any]] = List(
    listEp.zServerLogic { case (tenantId, limit, skip) =>
      repo.listCompanies(tenantId, limit.getOrElse(25), skip.getOrElse(0))
        .mapError(_.getMessage)
    },
    getEp.zServerLogic { case (id, tenantId) =>
      repo.getCompany(tenantId, id)
        .someOrFail(s"Company $id not found")
        .mapError { case e: Throwable => e.getMessage; case s: String => s }
    },
    createEp.zServerLogic { case (tenantId, body) =>
      repo.createCompany(tenantId, org.bson.Document.parse(body.noSpaces))
        .mapError(_.getMessage)
    },
    updateEp.zServerLogic { case (id, tenantId, _) =>
      repo.getCompany(tenantId, id)
        .someOrFail(s"Company $id not found")
        .mapError { case e: Throwable => e.getMessage; case s: String => s }
    },
    deleteEp.zServerLogic { case (id, tenantId) =>
      repo.deleteCompany(tenantId, id).mapError(_.getMessage).unit
    }
  )
