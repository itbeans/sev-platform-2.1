package io.itbeans.ev.restapi

import io.circe.generic.auto._
import io.circe.parser.decode
import RestApiCodecs.given
import sttp.tapir.generic.auto._
import sttp.tapir.json.circe._
import sttp.tapir.ztapir._
import zio._

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}

// ---------------------------------------------------------------------------
// LogsEndpoints — proxy to ev-analytics audit log REST API.
//
// Mirrors: LoggingService.ts
// Audit log data lives in ev-analytics (ev_logs TimescaleDB hypertable).
// This endpoint proxies requests to ev-analytics, injecting the tenant header.
//
// GET  /api/v1/Logs        — filtered, paginated log list
// ---------------------------------------------------------------------------

case class LogEntry(
    tenantId: String,
    timestamp: Long,
    level: String, // "D" | "I" | "W" | "E"
    source: String,
    action: String,
    module: Option[String],
    method: Option[String],
    message: String,
    userId: Option[String],
    chargingStationId: Option[String],
    siteId: Option[String],
    siteAreaId: Option[String],
    details: Option[io.circe.Json]
)

case class LogsResponse(count: Int, logs: List[LogEntry])

private val httpClient = HttpClient.newHttpClient()

object LogsEndpoints:

  private val tenantHeader = header[String]("X-Tenant-ID")
  private val base         = endpoint.errorOut(stringBody)

  private val listLogsEp = base.get
    .in("api" / "v1" / "Logs")
    .in(tenantHeader)
    .in(query[Option[Long]]("StartDateTime"))
    .in(query[Option[Long]]("EndDateTime"))
    .in(query[Option[String]]("Level")) // D | I | W | E
    .in(query[Option[String]]("Action"))
    .in(query[Option[String]]("Search"))
    .in(query[Option[String]]("Source"))
    .in(query[Int]("Limit").default(50))
    .in(query[Int]("Skip").default(0))
    .out(jsonBody[LogsResponse])

  def routes(cfg: RestApiConfig, repo: RestApiRepository): List[ZServerEndpoint[Any, Any]] = List(
    listLogsEp.zServerLogic { case (tenantId, from, to, level, action, search, source, limit, skip) =>
      val params = List(
        from.map(v => s"StartDateTime=$v"),
        to.map(v => s"EndDateTime=$v"),
        level.map(v => s"Level=$v"),
        action.map(v => s"Action=$v"),
        search.map(v => s"Search=${java.net.URLEncoder.encode(v, "UTF-8")}"),
        source.map(v => s"Source=$v"),
        Some(s"Limit=$limit"),
        Some(s"Skip=$skip")
      ).flatten.mkString("&")

      val urlStr = s"${cfg.analyticsBaseUrl}/api/v1/logs?$params"

      ZIO
        .attemptBlocking {
          val req = HttpRequest
            .newBuilder(URI.create(urlStr))
            .header("X-Tenant-ID", tenantId)
            .GET()
            .build()
          httpClient.send(req, HttpResponse.BodyHandlers.ofString()).body()
        }
        .flatMap(body => ZIO.fromEither(decode[LogsResponse](body)))
        .catchAll(err =>
          ZIO.logWarning(s"Analytics logs proxy error: $err") *>
            ZIO.succeed(LogsResponse(0, Nil))
        )
    }
  )
