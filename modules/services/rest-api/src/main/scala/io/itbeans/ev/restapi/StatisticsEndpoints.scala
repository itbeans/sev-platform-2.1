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
// StatisticsEndpoints — proxy to ev-analytics statistics REST API.
//
// Mirrors: StatisticService.ts
// The stats computations live in ev-analytics (TimescaleDB). This service
// transparently proxies the dashboard requests to ev-analytics, adding auth
// and tenant header injection.
//
// GET /api/v1/Statistics/ChargingStations/{metric}
//   metric: "Consumption" | "Usage" | "Inactivity" | "Transactions" | "Pricing"
// ---------------------------------------------------------------------------

case class StatPoint(label: String, period: String, value: Double)

case class StatisticsResponse(
    dataType: String,
    dataScope: String,
    unit: String,
    total: Double,
    data: List[StatPoint]
)

private val statsHttpClient = HttpClient.newHttpClient()

object StatisticsEndpoints:

  private val tenantHeader = header[String]("X-Tenant-ID")
  private val base         = endpoint.errorOut(stringBody)
  private val statsBase    = base.in("api" / "v1" / "Statistics" / "ChargingStations")

  private val consumptionEp = statsBase.get
    .in("Consumption")
    .in(tenantHeader)
    .in(query[Long]("StartDateTime"))
    .in(query[Long]("EndDateTime"))
    .in(query[String]("DataScope").default("month"))
    .in(query[Option[String]]("ChargingStationID"))
    .in(query[Option[String]]("SiteAreaID"))
    .out(jsonBody[StatisticsResponse])

  private val usageEp = statsBase.get
    .in("Usage")
    .in(tenantHeader)
    .in(query[Long]("StartDateTime"))
    .in(query[Long]("EndDateTime"))
    .in(query[String]("DataScope").default("month"))
    .in(query[Option[String]]("ChargingStationID"))
    .out(jsonBody[StatisticsResponse])

  private val inactivityEp = statsBase.get
    .in("Inactivity")
    .in(tenantHeader)
    .in(query[Long]("StartDateTime"))
    .in(query[Long]("EndDateTime"))
    .in(query[String]("DataScope").default("month"))
    .in(query[Option[String]]("ChargingStationID"))
    .out(jsonBody[StatisticsResponse])

  private val transactionsEp = statsBase.get
    .in("Transactions")
    .in(tenantHeader)
    .in(query[Long]("StartDateTime"))
    .in(query[Long]("EndDateTime"))
    .in(query[String]("DataScope").default("month"))
    .in(query[Option[String]]("ChargingStationID"))
    .out(jsonBody[StatisticsResponse])

  private val pricingEp = statsBase.get
    .in("Pricing")
    .in(tenantHeader)
    .in(query[Long]("StartDateTime"))
    .in(query[Long]("EndDateTime"))
    .in(query[String]("DataScope").default("month"))
    .in(query[Option[String]]("ChargingStationID"))
    .out(jsonBody[StatisticsResponse])

  def routes(cfg: RestApiConfig, repo: RestApiRepository): List[ZServerEndpoint[Any, Any]] = List(
    consumptionEp.zServerLogic { case (tenantId, from, to, scope, stationId, siteAreaId) =>
      proxyStatistic(cfg, tenantId, "Consumption", from, to, scope, stationId, siteAreaId, "kWh")
    },
    usageEp.zServerLogic { case (tenantId, from, to, scope, stationId) =>
      proxyStatistic(cfg, tenantId, "Usage", from, to, scope, stationId, None, "h")
    },
    inactivityEp.zServerLogic { case (tenantId, from, to, scope, stationId) =>
      proxyStatistic(cfg, tenantId, "Inactivity", from, to, scope, stationId, None, "h")
    },
    transactionsEp.zServerLogic { case (tenantId, from, to, scope, stationId) =>
      proxyStatistic(cfg, tenantId, "Transactions", from, to, scope, stationId, None, "")
    },
    pricingEp.zServerLogic { case (tenantId, from, to, scope, stationId) =>
      proxyStatistic(cfg, tenantId, "Pricing", from, to, scope, stationId, None, "€")
    }
  )

  private def proxyStatistic(
      cfg: RestApiConfig,
      tenantId: String,
      metric: String,
      from: Long,
      to: Long,
      scope: String,
      stationId: Option[String],
      siteAreaId: Option[String],
      defaultUnit: String
  ): UIO[StatisticsResponse] =
    val params = List(
      Some(s"StartDateTime=$from"),
      Some(s"EndDateTime=$to"),
      Some(s"DataScope=$scope"),
      stationId.map(id => s"ChargingStationID=${java.net.URLEncoder.encode(id, "UTF-8")}"),
      siteAreaId.map(id => s"SiteAreaID=${java.net.URLEncoder.encode(id, "UTF-8")}")
    ).flatten.mkString("&")

    val urlStr = s"${cfg.analyticsBaseUrl}/api/v1/statistics/chargingstations/${metric.toLowerCase}?$params"

    ZIO
      .attemptBlocking {
        val req = HttpRequest
          .newBuilder(URI.create(urlStr))
          .header("X-Tenant-ID", tenantId)
          .GET()
          .build()
        statsHttpClient.send(req, HttpResponse.BodyHandlers.ofString()).body()
      }
      .flatMap(body => ZIO.fromEither(decode[StatisticsResponse](body)))
      .catchAll(err =>
        ZIO.logWarning(s"Analytics stats proxy error ($metric): $err") *>
          ZIO.succeed(StatisticsResponse(metric, scope, defaultUnit, 0.0, Nil))
      )
