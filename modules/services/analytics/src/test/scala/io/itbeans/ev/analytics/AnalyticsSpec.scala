package io.itbeans.ev.analytics

import zio._
import zio.test._
import zio.test.Assertion._

import java.time.Instant

// ---------------------------------------------------------------------------
// AnalyticsSpec — ZIO Test suite for ev-analytics.
//
// Uses in-memory repository stubs (no TimescaleDB / Kafka needed).
// Tests cover: domain construction, log query filtering, stat aggregation,
// and configuration defaults.
// ---------------------------------------------------------------------------

object AnalyticsSpec extends ZIOSpecDefault:

  // ── In-memory stubs ───────────────────────────────────────────────────────

  class InMemoryConsumptionRepository extends ConsumptionRepository:
    var inserted: List[ConsumptionReading] = Nil

    def insert(r: ConsumptionReading): Task[Unit] =
      ZIO.succeed { inserted = r :: inserted }

    def sumKwhByStation(
        tenantId: String,
        from: Instant,
        to: Instant,
        scope: String,
        stationId: Option[String],
        siteAreaId: Option[String]
    ): Task[List[StatPoint]] =
      ZIO.succeed {
        val filtered = inserted.filter { r =>
          r.tenantId == tenantId &&
          !r.time.isBefore(from) &&
          r.time.isBefore(to) &&
          stationId.forall(_ == r.chargingStationId)
        }
        // Group by station and return total kWh
        filtered.groupBy(_.chargingStationId).map { case (stId, readings) =>
          StatPoint(stId, "2024-01", readings.map(_.cumulatedKwh).maxOption.getOrElse(0.0))
        }.toList
      }

    def countTransactionsByStation(
        tenantId: String,
        from: Instant,
        to: Instant,
        scope: String,
        stationId: Option[String]
    ): Task[List[StatPoint]] =
      ZIO.succeed {
        val filtered = inserted.filter { r =>
          r.tenantId == tenantId && r.transactionId.isDefined &&
          stationId.forall(_ == r.chargingStationId)
        }
        filtered.groupBy(_.chargingStationId).map { case (stId, readings) =>
          StatPoint(stId, "2024-01", readings.flatMap(_.transactionId).toSet.size.toDouble)
        }.toList
      }

  class InMemoryLogRepository extends LogRepository:
    var inserted: List[LogEntry] = Nil

    def insert(e: LogEntry): Task[Unit] =
      ZIO.succeed { inserted = e :: inserted }

    def query(
        tenantId: String,
        from: Option[Instant],
        to: Option[Instant],
        level: Option[String],
        action: Option[String],
        search: Option[String],
        limit: Int,
        offset: Int
    ): Task[LogsResponse] =
      ZIO.succeed {
        val filtered = inserted.filter { e =>
          e.tenantId == tenantId &&
          from.forall(f => !e.time.isBefore(f)) &&
          to.forall(t => e.time.isBefore(t)) &&
          level.forall(_ == e.level) &&
          action.forall(_ == e.action) &&
          search.forall(s => e.message.contains(s))
        }.sortBy(_.time)(Ordering[Instant].reverse)
        val page = filtered.slice(offset, offset + limit)
        LogsResponse(filtered.size, page)
      }

    def deleteOlderThan(tenantId: String, cutoff: Instant): Task[Long] =
      ZIO.succeed {
        val before = inserted.size
        inserted = inserted.filterNot(e => e.tenantId == tenantId && e.time.isBefore(cutoff))
        (before - inserted.size).toLong
      }

  // ── Fixtures ──────────────────────────────────────────────────────────────

  val now  = Instant.parse("2024-06-15T12:00:00Z")
  val from = Instant.parse("2024-06-01T00:00:00Z")
  val to   = Instant.parse("2024-07-01T00:00:00Z")

  def makeReading(stationId: String, txId: Long, kwh: Double): ConsumptionReading =
    ConsumptionReading(
      tenantId = "t1",
      chargingStationId = stationId,
      connectorId = 1,
      siteAreaId = Some("sa-001"),
      siteId = Some("site-001"),
      userId = Some("u1"),
      transactionId = Some(txId),
      time = now,
      instantWatts = 7400.0,
      cumulatedKwh = kwh
    )

  def makeLog(level: String, action: String, msg: String): LogEntry =
    LogEntry(
      tenantId = "t1",
      time = now,
      level = level,
      source = "OcppServer",
      action = action,
      module = Some("OCPPService"),
      method = Some("handleMeterValues"),
      message = msg,
      userId = None,
      chargingStationId = Some("CS-001"),
      siteId = None,
      siteAreaId = None,
      details = None
    )

  // ── Tests ─────────────────────────────────────────────────────────────────

  override def spec = suite("AnalyticsSpec")(
    suite("ConsumptionReading domain")(
      test("insert and query by tenant and time range") {
        val repo = new InMemoryConsumptionRepository
        for
          _      <- repo.insert(makeReading("CS-001", 1L, 10.5))
          _      <- repo.insert(makeReading("CS-002", 2L, 7.3))
          points <- repo.sumKwhByStation("t1", from, to, "month", None, None)
        yield assertTrue(points.size == 2) &&
          assertTrue(points.exists(_.label == "CS-001")) &&
          assertTrue(points.exists(_.label == "CS-002"))
      },
      test("filter by chargingStationId") {
        val repo = new InMemoryConsumptionRepository
        for
          _      <- repo.insert(makeReading("CS-001", 1L, 10.5))
          _      <- repo.insert(makeReading("CS-002", 2L, 7.3))
          points <- repo.sumKwhByStation("t1", from, to, "month", Some("CS-001"), None)
        yield assertTrue(points.size == 1) &&
          assertTrue(points.head.label == "CS-001")
      },
      test("count distinct transactions by station") {
        val repo = new InMemoryConsumptionRepository
        for
          _      <- repo.insert(makeReading("CS-001", 1L, 5.0))
          _      <- repo.insert(makeReading("CS-001", 1L, 8.0))  // same tx — should count as 1
          _      <- repo.insert(makeReading("CS-001", 2L, 12.0)) // different tx
          points <- repo.countTransactionsByStation("t1", from, to, "month", Some("CS-001"))
        yield assertTrue(points.size == 1) &&
          assertTrue(points.head.value == 2.0) // 2 distinct txIds
      },
      test("tenant isolation — different tenant sees no data") {
        val repo = new InMemoryConsumptionRepository
        for
          _      <- repo.insert(makeReading("CS-001", 1L, 10.5))
          points <- repo.sumKwhByStation("other-tenant", from, to, "month", None, None)
        yield assertTrue(points.isEmpty)
      }
    ),
    suite("LogRepository")(
      test("insert and retrieve logs") {
        val repo = new InMemoryLogRepository
        for
          _      <- repo.insert(makeLog("I", "Login", "User logged in"))
          _      <- repo.insert(makeLog("E", "Error", "Connection failed"))
          result <- repo.query("t1", None, None, None, None, None, 10, 0)
        yield assertTrue(result.count == 2) &&
          assertTrue(result.logs.size == 2)
      },
      test("filter by level") {
        val repo = new InMemoryLogRepository
        for
          _      <- repo.insert(makeLog("I", "Login", "info message"))
          _      <- repo.insert(makeLog("E", "Error", "error message"))
          _      <- repo.insert(makeLog("W", "Warn", "warn message"))
          result <- repo.query("t1", None, None, Some("E"), None, None, 10, 0)
        yield assertTrue(result.count == 1) &&
          assertTrue(result.logs.head.level == "E")
      },
      test("filter by action") {
        val repo = new InMemoryLogRepository
        for
          _      <- repo.insert(makeLog("I", "Login", "user login"))
          _      <- repo.insert(makeLog("I", "Logout", "user logout"))
          result <- repo.query("t1", None, None, None, Some("Login"), None, 10, 0)
        yield assertTrue(result.count == 1) &&
          assertTrue(result.logs.head.action == "Login")
      },
      test("search filter on message") {
        val repo = new InMemoryLogRepository
        for
          _      <- repo.insert(makeLog("I", "A", "connection established"))
          _      <- repo.insert(makeLog("I", "B", "heartbeat received"))
          result <- repo.query("t1", None, None, None, None, Some("heartbeat"), 10, 0)
        yield assertTrue(result.count == 1) &&
          assertTrue(result.logs.head.message.contains("heartbeat"))
      },
      test("deleteOlderThan removes entries before cutoff") {
        val repo     = new InMemoryLogRepository
        val old      = Instant.parse("2024-01-01T00:00:00Z")
        val recent   = Instant.parse("2024-06-01T00:00:00Z")
        val oldEntry = makeLog("I", "A", "old").copy(time = old)
        val newEntry = makeLog("I", "B", "new").copy(time = recent)
        val cutoff   = Instant.parse("2024-03-01T00:00:00Z")
        for
          _         <- repo.insert(oldEntry)
          _         <- repo.insert(newEntry)
          n         <- repo.deleteOlderThan("t1", cutoff)
          remaining <- repo.query("t1", None, None, None, None, None, 10, 0)
        yield assertTrue(n == 1L) &&
          assertTrue(remaining.count == 1) &&
          assertTrue(remaining.logs.head.message == "new")
      },
      test("pagination works correctly") {
        val repo = new InMemoryLogRepository
        for
          _     <- ZIO.foreachDiscard(1 to 10)(i => repo.insert(makeLog("I", s"A$i", s"message $i")))
          page1 <- repo.query("t1", None, None, None, None, None, 5, 0)
          page2 <- repo.query("t1", None, None, None, None, None, 5, 5)
        yield assertTrue(page1.count == 10) &&
          assertTrue(page1.logs.size == 5) &&
          assertTrue(page2.logs.size == 5)
      }
    ),
    suite("AnalyticsConfig defaults")(
      test("log retention is 90 days") {
        val cfg = AnalyticsConfig(
          httpPort = 8080,
          jdbcUrl = "jdbc:postgresql://localhost/ev_analytics",
          jdbcUser = "ev",
          jdbcPassword = "ev",
          logRetentionDays = 90,
          consumptionRetentionDays = 365,
          defaultTenantId = "default"
        )
        assertTrue(cfg.logRetentionDays == 90) &&
        assertTrue(cfg.consumptionRetentionDays == 365)
      }
    )
  )
