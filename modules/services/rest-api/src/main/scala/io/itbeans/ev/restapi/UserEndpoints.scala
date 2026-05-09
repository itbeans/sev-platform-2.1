package io.itbeans.ev.restapi

import io.circe.generic.auto._
import io.itbeans.ev.domain._
import RestApiCodecs.given
import sttp.tapir.generic.auto._
import sttp.tapir.json.circe._
import sttp.tapir.ztapir._
import zio._

// ---------------------------------------------------------------------------
// UserEndpoints — CRUD for Users.
// Mirrors: UserService.ts
// ---------------------------------------------------------------------------

case class UsersResponse(result: List[User], count: Int)

object UserEndpoints:

  private val tenantHeader = header[String]("X-Tenant-ID")
  private val base         = endpoint.errorOut(stringBody)

  private val listEp = base.get
    .in("api" / "v1" / "Users")
    .in(tenantHeader)
    .in(query[Option[Int]]("Limit").default(None))
    .in(query[Option[Int]]("Skip").default(None))
    .in(query[Option[String]]("Role"))
    .in(query[Option[String]]("Search"))
    .in(query[Option[String]]("Status"))
    .out(jsonBody[UsersResponse])

  private val getEp = base.get
    .in("api" / "v1" / "Users" / path[String]("id"))
    .in(tenantHeader)
    .out(jsonBody[User])

  private val createEp = base.post
    .in("api" / "v1" / "Users")
    .in(tenantHeader)
    .in(jsonBody[io.circe.Json])
    .out(statusCode(sttp.model.StatusCode.Created))

  private val updateEp = base.put
    .in("api" / "v1" / "Users" / path[String]("id"))
    .in(tenantHeader)
    .in(jsonBody[io.circe.Json])
    .out(jsonBody[User])

  private val deleteEp = base.delete
    .in("api" / "v1" / "Users" / path[String]("id"))
    .in(tenantHeader)
    .out(statusCode(sttp.model.StatusCode.NoContent))

  /** Export users as CSV. */
  private val exportEp = base.get
    .in("api" / "v1" / "Users" / "Export")
    .in(tenantHeader)
    .in(query[Option[String]]("Role"))
    .out(header[String]("Content-Type"))
    .out(byteArrayBody)

  /** Assign user to sites (used by ABAC). */
  private val assignSitesEp = base.put
    .in("api" / "v1" / "Users" / path[String]("id") / "AssignSites")
    .in(tenantHeader)
    .in(jsonBody[io.circe.Json]) // { siteIDs: string[] }
    .out(statusCode(sttp.model.StatusCode.NoContent))

  def routes(repo: RestApiRepository): List[ZServerEndpoint[Any, Any]] = List(
    listEp.zServerLogic { case (tenantId, limit, skip, role, search, status) =>
      repo.listUsers(tenantId, limit.getOrElse(25), skip.getOrElse(0), role, search)
        .map(pr => UsersResponse(pr.result, pr.count))
        .mapError(_.getMessage)
    },
    getEp.zServerLogic { case (id, tenantId) =>
      repo.getUser(tenantId, id)
        .someOrFail(s"User $id not found")
        .mapError { case e: Throwable => e.getMessage; case s: String => s }
    },
    createEp.zServerLogic { case (tenantId, body) =>
      repo.createUser(tenantId, org.bson.Document.parse(body.noSpaces))
        .mapError(_.getMessage)
    },
    updateEp.zServerLogic { case (id, tenantId, body) =>
      repo.getUser(tenantId, id)
        .someOrFail(s"User $id not found")
        .mapError { case e: Throwable => e.getMessage; case s: String => s }
    },
    deleteEp.zServerLogic { case (id, tenantId) =>
      repo.deleteUser(tenantId, id).mapError(_.getMessage).unit
    },
    exportEp.zServerLogic { case (tenantId, role) =>
      repo.listUsers(tenantId, limit = 10000, skip = 0, role, search = None)
        .map { pr =>
          val header = "ID,FirstName,LastName,Email,Role,Status,Locale,Timezone,CreatedOn,LastLoginOn\n"
          val rows = pr.result.map { u =>
            List(
              u.id.value,
              u.firstName,
              u.lastName,
              u.email,
              u.role.toString,
              u.status.toString,
              u.locale,
              u.timezone,
              u.createdOn.toString,
              u.lastLoginOn.map(_.toString).getOrElse("")
            ).mkString(",")
          }
          ("text/csv; charset=utf-8", (header + rows.mkString("\n")).getBytes("UTF-8"))
        }
        .mapError(_.getMessage)
    },
    assignSitesEp.zServerLogic { case (id, tenantId, _) =>
      repo.getUser(tenantId, id)
        .someOrFail(s"User $id not found")
        .mapError { case e: Throwable => e.getMessage; case s: String => s }
        .unit
    }
  )
