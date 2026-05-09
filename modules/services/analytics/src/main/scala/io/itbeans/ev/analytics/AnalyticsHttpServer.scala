package io.itbeans.ev.analytics

import sttp.tapir.ztapir._
import sttp.tapir.generic.auto._
import sttp.tapir.json.circe._
import sttp.tapir.server.ziohttp.ZioHttpInterpreter
import zio._
import zio.http.{handler, Routes}

import java.time.Instant

// ---------------------------------------------------------------------------
// AnalyticsHttpServer — Tapir endpoints for statistics and log queries.
//
// Statistics endpoints (GET /api/v1/statistics/...):
//   charging-stations/consumption  — kWh by station, bucketed by scope
//   charging-stations/transactions — tx count by station
//   logs                           — audit log with filtering + pagination
// ---------------------------------------------------------------------------

object AnalyticsHttpServer:

  // ── Shared query parameter types ──────────────────────────────────────

  private val tenantIdHeader = header[String]("X-Tenant-ID")

  // ── Statistics endpoints ───────────────────────────────────────────────

  private val consumptionEndpoint = endpoint
    .get
    .in("api" / "v1" / "statistics" / "charging-stations" / "consumption")
    .in(tenantIdHeader)
    .in(query[Long]("startDateTime"))
    .in(query[Long]("endDateTime"))
    .in(query[String]("dataScope").default("month"))
    .in(query[Option[String]]("chargingStationID"))
    .in(query[Option[String]]("siteAreaID"))
    .out(jsonBody[StatisticsResponse])
    .errorOut(stringBody)

  private val transactionsEndpoint = endpoint
    .get
    .in("api" / "v1" / "statistics" / "charging-stations" / "transactions")
    .in(tenantIdHeader)
    .in(query[Long]("startDateTime"))
    .in(query[Long]("endDateTime"))
    .in(query[String]("dataScope").default("month"))
    .in(query[Option[String]]("chargingStationID"))
    .out(jsonBody[StatisticsResponse])
    .errorOut(stringBody)

  // ── Logs endpoint ──────────────────────────────────────────────────────

  private val logsEndpoint = endpoint
    .get
    .in("api" / "v1" / "logs")
    .in(tenantIdHeader)
    .in(query[Option[Long]]("startDateTime"))
    .in(query[Option[Long]]("endDateTime"))
    .in(query[Option[String]]("level"))
    .in(query[Option[String]]("action"))
    .in(query[Option[String]]("search"))
    .in(query[Int]("limit").default(50))
    .in(query[Int]("skip").default(0))
    .out(jsonBody[LogsResponse])
    .errorOut(stringBody)

  // ── Server logic ───────────────────────────────────────────────────────

  def routes(
      consumptionRepo: ConsumptionRepository,
      logRepo: LogRepository
  ): Routes[Any, zio.http.Response] =
    ZioHttpInterpreter().toHttp(List(
      consumptionEndpoint.zServerLogic {
        case (tenantId, fromMs, toMs, scope, stationId, siteAreaId) =>
          (for
            points <- consumptionRepo.sumKwhByStation(
              tenantId = tenantId,
              from = Instant.ofEpochMilli(fromMs),
              to = Instant.ofEpochMilli(toMs),
              scope = scope,
              stationId = stationId,
              siteAreaId = siteAreaId
            )
            total = points.map(_.value).sum
          yield StatisticsResponse(
            dataType = "Consumption",
            dataScope = scope,
            unit = "kWh",
            total = total,
            data = points
          )).mapError(_.getMessage)
      },
      transactionsEndpoint.zServerLogic {
        case (tenantId, fromMs, toMs, scope, stationId) =>
          (for
            points <- consumptionRepo.countTransactionsByStation(
              tenantId = tenantId,
              from = Instant.ofEpochMilli(fromMs),
              to = Instant.ofEpochMilli(toMs),
              scope = scope,
              stationId = stationId
            )
            total = points.map(_.value).sum
          yield StatisticsResponse(
            dataType = "Transactions",
            dataScope = scope,
            unit = "",
            total = total,
            data = points
          )).mapError(_.getMessage)
      },
      logsEndpoint.zServerLogic {
        case (tenantId, fromMs, toMs, level, action, search, limit, skip) =>
          logRepo.query(
            tenantId = tenantId,
            from = fromMs.map(Instant.ofEpochMilli),
            to = toMs.map(Instant.ofEpochMilli),
            level = level,
            action = action,
            search = search,
            limit = limit.min(500), // hard cap
            offset = skip
          ).mapError(_.getMessage)
      }
    ))
