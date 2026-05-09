package io.itbeans.ev.restapi

import io.circe.generic.auto._
import io.itbeans.ev.domain._
import RestApiCodecs.given
import sttp.tapir.generic.auto._
import sttp.tapir.json.circe._
import sttp.tapir.ztapir._
import zio._

// ---------------------------------------------------------------------------
// SiteEndpoints — CRUD for Sites and SiteAreas.
//
// Mirrors: SiteService.ts  (list, get, create, update, delete, assignUsers)
//          SiteAreaService.ts (same pattern for SiteAreas)
// ---------------------------------------------------------------------------

object SiteEndpoints:

  private val tenantHeader = header[String]("X-Tenant-ID")
  private val base         = endpoint.errorOut(stringBody)

  // ── Sites ─────────────────────────────────────────────────────────────────

  private val listSitesEp = base.get
    .in("api" / "v1" / "Sites")
    .in(tenantHeader)
    .in(query[Option[Int]]("Limit").default(None))
    .in(query[Option[Int]]("Skip").default(None))
    .in(query[Option[String]]("Search"))
    .out(jsonBody[PagedResult[Site]])

  private val getSiteEp = base.get
    .in("api" / "v1" / "Sites" / path[String]("id"))
    .in(tenantHeader)
    .out(jsonBody[Site])

  private val createSiteEp = base.post
    .in("api" / "v1" / "Sites")
    .in(tenantHeader)
    .in(jsonBody[io.circe.Json])
    .out(statusCode(sttp.model.StatusCode.Created))

  private val updateSiteEp = base.put
    .in("api" / "v1" / "Sites" / path[String]("id"))
    .in(tenantHeader)
    .in(jsonBody[io.circe.Json])
    .out(jsonBody[Site])

  private val deleteSiteEp = base.delete
    .in("api" / "v1" / "Sites" / path[String]("id"))
    .in(tenantHeader)
    .out(statusCode(sttp.model.StatusCode.NoContent))

  def siteRoutes(repo: RestApiRepository): List[ZServerEndpoint[Any, Any]] = List(
    listSitesEp.zServerLogic { case (tenantId, limit, skip, search) =>
      repo.listSites(tenantId, limit.getOrElse(25), skip.getOrElse(0), search)
        .mapError(_.getMessage)
    },
    getSiteEp.zServerLogic { case (id, tenantId) =>
      repo.getSite(tenantId, id)
        .someOrFail(s"Site $id not found")
        .mapError { case e: Throwable => e.getMessage; case s: String => s }
    },
    createSiteEp.zServerLogic { case (tenantId, body) =>
      repo.createSite(tenantId, org.bson.Document.parse(body.noSpaces))
        .mapError(_.getMessage)
    },
    updateSiteEp.zServerLogic { case (id, tenantId, body) =>
      repo.getSite(tenantId, id)
        .someOrFail(s"Site $id not found")
        .mapError { case e: Throwable => e.getMessage; case s: String => s }
    },
    deleteSiteEp.zServerLogic { case (id, tenantId) =>
      repo.deleteSite(tenantId, id)
        .mapError(_.getMessage)
        .unit
    }
  )

  // ── SiteAreas ─────────────────────────────────────────────────────────────

  private val listAreasEp = base.get
    .in("api" / "v1" / "SiteAreas")
    .in(tenantHeader)
    .in(query[Option[Int]]("Limit").default(None))
    .in(query[Option[Int]]("Skip").default(None))
    .in(query[Option[String]]("SiteID"))
    .out(jsonBody[PagedResult[SiteArea]])

  private val getAreaEp = base.get
    .in("api" / "v1" / "SiteAreas" / path[String]("id"))
    .in(tenantHeader)
    .out(jsonBody[SiteArea])

  private val createAreaEp = base.post
    .in("api" / "v1" / "SiteAreas")
    .in(tenantHeader)
    .in(jsonBody[io.circe.Json])
    .out(statusCode(sttp.model.StatusCode.Created))

  private val updateAreaEp = base.put
    .in("api" / "v1" / "SiteAreas" / path[String]("id"))
    .in(tenantHeader)
    .in(jsonBody[io.circe.Json])
    .out(jsonBody[SiteArea])

  private val deleteAreaEp = base.delete
    .in("api" / "v1" / "SiteAreas" / path[String]("id"))
    .in(tenantHeader)
    .out(statusCode(sttp.model.StatusCode.NoContent))

  def siteAreaRoutes(repo: RestApiRepository): List[ZServerEndpoint[Any, Any]] = List(
    listAreasEp.zServerLogic { case (tenantId, limit, skip, siteId) =>
      repo.listSiteAreas(tenantId, limit.getOrElse(25), skip.getOrElse(0), siteId)
        .mapError(_.getMessage)
    },
    getAreaEp.zServerLogic { case (id, tenantId) =>
      repo.getSiteArea(tenantId, id)
        .someOrFail(s"SiteArea $id not found")
        .mapError { case e: Throwable => e.getMessage; case s: String => s }
    },
    createAreaEp.zServerLogic { case (tenantId, body) =>
      repo.createSiteArea(tenantId, org.bson.Document.parse(body.noSpaces))
        .mapError(_.getMessage)
    },
    updateAreaEp.zServerLogic { case (id, tenantId, _) =>
      repo.getSiteArea(tenantId, id)
        .someOrFail(s"SiteArea $id not found")
        .mapError { case e: Throwable => e.getMessage; case s: String => s }
    },
    deleteAreaEp.zServerLogic { case (id, tenantId) =>
      repo.deleteSiteArea(tenantId, id)
        .mapError(_.getMessage)
        .unit
    }
  )

  val routes: (RestApiRepository) => List[ZServerEndpoint[Any, Any]] =
    repo => siteRoutes(repo) ++ siteAreaRoutes(repo)
