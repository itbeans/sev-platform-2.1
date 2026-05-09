package io.itbeans.ev.analytics

import cats.effect.unsafe.implicits.global
import doobie._
import doobie.implicits._
import doobie.postgres.implicits._
import zio._

import java.time.Instant

// ---------------------------------------------------------------------------
// AnalyticsRepository — Doobie / TimescaleDB repositories for ev-analytics.
//
// Two hypertables:
//   ev_consumptions — one row per meter-value reading (instantWatts, kWh)
//   ev_logs         — one row per audit log event
//
// ZIO ↔ cats-effect bridge: same pattern as BillingRepository —
// cats.effect.IO.unsafeRunSync() inside ZIO.attemptBlocking.
//
// Schema: db/migration/V1__analytics_schema.sql
// ---------------------------------------------------------------------------

// ── IO → Task bridge ──────────────────────────────────────────────────────

private def runIO[A](io: cats.effect.IO[A]): Task[A] =
  ZIO.attemptBlocking(io.unsafeRunSync())

// ── Repository traits ─────────────────────────────────────────────────────

trait ConsumptionRepository:
  def insert(r: ConsumptionReading): Task[Unit]

  // Statistics aggregations
  def sumKwhByStation(
      tenantId: String,
      from: Instant,
      to: Instant,
      scope: String, // "month" | "day" | "hour"
      stationId: Option[String],
      siteAreaId: Option[String]
  ): Task[List[StatPoint]]

  def countTransactionsByStation(
      tenantId: String,
      from: Instant,
      to: Instant,
      scope: String,
      stationId: Option[String]
  ): Task[List[StatPoint]]

trait LogRepository:
  def insert(e: LogEntry): Task[Unit]

  def query(
      tenantId: String,
      from: Option[Instant],
      to: Option[Instant],
      level: Option[String],
      action: Option[String],
      search: Option[String],
      limit: Int,
      offset: Int
  ): Task[LogsResponse]
  def deleteOlderThan(tenantId: String, cutoff: Instant): Task[Long]

// ── PostgreSQL / TimescaleDB implementations ──────────────────────────────

final class TimescaleConsumptionRepository(xa: Transactor[cats.effect.IO])
    extends ConsumptionRepository:

  def insert(r: ConsumptionReading): Task[Unit] = runIO(
    sql"""INSERT INTO ev_consumptions
            (tenant_id, time, charging_station_id, connector_id,
             site_area_id, site_id, user_id, transaction_id,
             instant_watts, cumulated_kwh)
          VALUES
            (${r.tenantId}, ${r.time},
             ${r.chargingStationId}, ${r.connectorId},
             ${r.siteAreaId}, ${r.siteId}, ${r.userId}, ${r.transactionId},
             ${r.instantWatts}, ${r.cumulatedKwh})
          ON CONFLICT DO NOTHING
       """.update.run.transact(xa).map(_ => ())
  )

  def sumKwhByStation(
      tenantId: String,
      from: Instant,
      to: Instant,
      scope: String,
      stationId: Option[String],
      siteAreaId: Option[String]
  ): Task[List[StatPoint]] = runIO {
    val truncFn        = scopeToTrunc(scope)
    val stationFilter  = stationId.fold(fr"")(id => fr"AND charging_station_id = $id")
    val siteAreaFilter = siteAreaId.fold(fr"")(id => fr"AND site_area_id = $id")
    (fr"""SELECT charging_station_id,
                 to_char(time_bucket($truncFn::interval, time), 'YYYY-MM-DD HH24:MI'),
                 COALESCE(MAX(cumulated_kwh) - MIN(cumulated_kwh), 0)
          FROM ev_consumptions
          WHERE tenant_id = $tenantId
            AND time >= $from AND time < $to"""
      ++ stationFilter ++ siteAreaFilter ++
      fr"""GROUP BY charging_station_id, time_bucket($truncFn::interval, time)
           ORDER BY charging_station_id, time_bucket($truncFn::interval, time)""")
      .query[(String, String, Double)]
      .map { case (label, period, value) => StatPoint(label, period, value) }
      .to[List]
      .transact(xa)
  }

  def countTransactionsByStation(
      tenantId: String,
      from: Instant,
      to: Instant,
      scope: String,
      stationId: Option[String]
  ): Task[List[StatPoint]] = runIO {
    val truncFn       = scopeToTrunc(scope)
    val stationFilter = stationId.fold(fr"")(id => fr"AND charging_station_id = $id")
    (fr"""SELECT charging_station_id,
                 to_char(time_bucket($truncFn::interval, time), 'YYYY-MM-DD HH24:MI'),
                 COUNT(DISTINCT transaction_id)::double precision
          FROM ev_consumptions
          WHERE tenant_id = $tenantId
            AND time >= $from AND time < $to
            AND transaction_id IS NOT NULL"""
      ++ stationFilter ++
      fr"""GROUP BY charging_station_id, time_bucket($truncFn::interval, time)
           ORDER BY charging_station_id, time_bucket($truncFn::interval, time)""")
      .query[(String, String, Double)]
      .map { case (label, period, value) => StatPoint(label, period, value) }
      .to[List]
      .transact(xa)
  }

  private def scopeToTrunc(scope: String): String = scope match
    case "hour"  => "1 hour"
    case "day"   => "1 day"
    case "month" => "1 month"
    case _       => "1 day"

// ─────────────────────────────────────────────────────────────────────────

final class TimescaleLogRepository(xa: Transactor[cats.effect.IO])
    extends LogRepository:

  def insert(e: LogEntry): Task[Unit] = runIO(
    sql"""INSERT INTO ev_logs
            (tenant_id, time, level, source, action, module, method,
             message, user_id, charging_station_id, site_id, site_area_id, details)
          VALUES
            (${e.tenantId}, ${e.time},
             ${e.level}, ${e.source}, ${e.action},
             ${e.module}, ${e.method}, ${e.message},
             ${e.userId}, ${e.chargingStationId},
             ${e.siteId}, ${e.siteAreaId}, ${e.details}::jsonb)
       """.update.run.transact(xa).map(_ => ())
  )

  def query(
      tenantId: String,
      from: Option[Instant],
      to: Option[Instant],
      level: Option[String],
      action: Option[String],
      search: Option[String],
      limit: Int,
      offset: Int
  ): Task[LogsResponse] = runIO {
    val fromFilter   = from.fold(fr"")(t => fr"AND time >= $t")
    val toFilter     = to.fold(fr"")(t => fr"AND time < $t")
    val levelFilter  = level.fold(fr"")(l => fr"AND level = $l")
    val actionFilter = action.fold(fr"")(a => fr"AND action = $a")
    val searchFilter = search.fold(fr"")(s => fr"AND message ILIKE ${"%" + s + "%"}")

    val baseWhere =
      fr"WHERE tenant_id = $tenantId" ++
        fromFilter ++ toFilter ++ levelFilter ++ actionFilter ++ searchFilter

    val countQuery =
      (fr"SELECT COUNT(*)::int FROM ev_logs" ++ baseWhere)
        .query[Int].unique.transact(xa)

    val rowsQuery =
      (fr"""SELECT tenant_id, time, level, source, action, module, method,
                   message, user_id, charging_station_id, site_id, site_area_id, details::text
            FROM ev_logs""" ++
        baseWhere ++
        fr"ORDER BY time DESC LIMIT $limit OFFSET $offset")
        .query[(
            String,
            Instant,
            String,
            String,
            String,
            Option[String],
            Option[String],
            String,
            Option[String],
            Option[String],
            Option[String],
            Option[String],
            Option[String]
        )]
        .map { case (tid, time, lvl, src, act, mod, meth, msg, uid, csid, sid, said, det) =>
          LogEntry(
            tenantId = tid,
            time = time,
            level = lvl,
            source = src,
            action = act,
            module = mod,
            method = meth,
            message = msg,
            userId = uid,
            chargingStationId = csid,
            siteId = sid,
            siteAreaId = said,
            details = det
          )
        }
        .to[List]
        .transact(xa)

    for
      count <- countQuery
      rows  <- rowsQuery
    yield LogsResponse(count, rows)
  }

  def deleteOlderThan(tenantId: String, cutoff: Instant): Task[Long] = runIO(
    sql"""DELETE FROM ev_logs
          WHERE tenant_id = $tenantId AND time < $cutoff
       """.update.run.transact(xa).map(_.toLong)
  )

// ── Transactor layer ──────────────────────────────────────────────────────

object AnalyticsRepository:

  private def transactor(cfg: AnalyticsConfig): Transactor[cats.effect.IO] =
    Transactor.fromDriverManager[cats.effect.IO](
      driver = "org.postgresql.Driver",
      url = cfg.jdbcUrl,
      user = cfg.jdbcUser,
      password = cfg.jdbcPassword,
      logHandler = None
    )

  val consumptionRepoLive: ZLayer[AnalyticsConfig, Nothing, ConsumptionRepository] =
    ZLayer.fromFunction((cfg: AnalyticsConfig) =>
      new TimescaleConsumptionRepository(transactor(cfg))
    )

  val logRepoLive: ZLayer[AnalyticsConfig, Nothing, LogRepository] =
    ZLayer.fromFunction((cfg: AnalyticsConfig) =>
      new TimescaleLogRepository(transactor(cfg))
    )
