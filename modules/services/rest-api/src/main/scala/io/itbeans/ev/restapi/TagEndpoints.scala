package io.itbeans.ev.restapi

import io.circe.generic.auto._
import io.itbeans.ev.domain._
import RestApiCodecs.given
import sttp.tapir.generic.auto._
import sttp.tapir.json.circe._
import sttp.tapir.ztapir._
import zio._
import io.itbeans.ev.domain.Tag

// ---------------------------------------------------------------------------
// TagEndpoints — CRUD for RFID Tags (badges used to start/stop charging).
// Mirrors: TagService.ts
// ---------------------------------------------------------------------------

object TagEndpoints:

  private val tenantHeader = header[String]("X-Tenant-ID")
  private val base         = endpoint.errorOut(stringBody)

  private val listEp = base.get
    .in("api" / "v1" / "Tags")
    .in(tenantHeader)
    .in(query[Option[Int]]("Limit").default(None))
    .in(query[Option[Int]]("Skip").default(None))
    .in(query[Option[String]]("UserID"))
    .in(query[Option[Boolean]]("Active"))
    .out(jsonBody[PagedResult[Tag]])

  private val getEp = base.get
    .in("api" / "v1" / "Tags" / path[String]("id"))
    .in(tenantHeader)
    .out(jsonBody[Tag])

  private val createEp = base.post
    .in("api" / "v1" / "Tags")
    .in(tenantHeader)
    .in(jsonBody[io.circe.Json])
    .out(statusCode(sttp.model.StatusCode.Created))

  private val updateEp = base.put
    .in("api" / "v1" / "Tags" / path[String]("id"))
    .in(tenantHeader)
    .in(jsonBody[io.circe.Json])
    .out(jsonBody[Tag])

  private val deleteEp = base.delete
    .in("api" / "v1" / "Tags" / path[String]("id"))
    .in(tenantHeader)
    .out(statusCode(sttp.model.StatusCode.NoContent))

  def routes(repo: RestApiRepository): List[ZServerEndpoint[Any, Any]] = List(
    listEp.zServerLogic { case (tenantId, limit, skip, userId, active) =>
      repo.listTags(tenantId, limit.getOrElse(25), skip.getOrElse(0), userId, active)
        .mapError(_.getMessage)
    },
    getEp.zServerLogic { case (id, tenantId) =>
      repo.getTag(tenantId, id)
        .someOrFail(s"Tag $id not found")
        .mapError { case e: Throwable => e.getMessage; case s: String => s }
    },
    createEp.zServerLogic { case (tenantId, body) =>
      repo.createTag(tenantId, org.bson.Document.parse(body.noSpaces))
        .mapError(_.getMessage)
    },
    updateEp.zServerLogic { case (id, tenantId, _) =>
      repo.getTag(tenantId, id)
        .someOrFail(s"Tag $id not found")
        .mapError { case e: Throwable => e.getMessage; case s: String => s }
    },
    deleteEp.zServerLogic { case (id, tenantId) =>
      repo.deleteTag(tenantId, id).mapError(_.getMessage).unit
    }
  )
