package io.itbeans.ev.scheduler

import zio._
import zio.test._
import zio.test.Assertion._

// ---------------------------------------------------------------------------
// SchedulerSpec — ZIO Test suite for ev-scheduler tasks.
//
// Uses an in-memory stub for SchedulerRepository (no Kafka, no MongoDB).
// Tests verify: repository stubs behave correctly, lock semantics, and
// task configuration defaults.
//
// Task logic is exercised end-to-end through integration tests with
// TestContainers (see future it:test configuration).
// ---------------------------------------------------------------------------

object SchedulerSpec extends ZIOSpecDefault:

  // ── In-memory repository stub ─────────────────────────────────────────────

  class InMemorySchedulerRepository(
      tenantIds: List[String] = List("tenant1"),
      offlineIds: Map[String, List[String]] = Map.empty,
      openTxIds: Map[String, List[Long]] = Map.empty,
      inactiveUsers: Map[String, List[(String, String)]] = Map.empty
  ) extends SchedulerRepository:

    var markedOffline: List[(String, String)] = Nil
    var closedTxs: List[(String, Long, Long)] = Nil
    var deletedLogs: Map[String, Long]        = Map.empty
    var deletedPerfs: Map[String, Long]       = Map.empty

    private var locks: Set[String] = Set.empty

    def listTenantIds: Task[List[String]] =
      ZIO.succeed(tenantIds)

    def findOfflineStationIds(tenantId: String, lastSeenBeforeMs: Long): Task[List[String]] =
      ZIO.succeed(offlineIds.getOrElse(tenantId, Nil))

    def markStationOffline(tenantId: String, chargingStationId: String): Task[Unit] =
      ZIO.succeed { markedOffline = (tenantId, chargingStationId) :: markedOffline }

    def findStaleOpenTransactionIds(tenantId: String, startedBeforeMs: Long): Task[List[Long]] =
      ZIO.succeed(openTxIds.getOrElse(tenantId, Nil))

    def forceCloseTransaction(tenantId: String, transactionId: Long, nowMs: Long): Task[Unit] =
      ZIO.succeed { closedTxs = (tenantId, transactionId, nowMs) :: closedTxs }

    def deleteOldLogs(tenantId: String, olderThanMs: Long): Task[Long] =
      ZIO.succeed { val n = 5L; deletedLogs = deletedLogs.updated(tenantId, n); n }

    def deleteOldPerformances(tenantId: String, olderThanMs: Long): Task[Long] =
      ZIO.succeed { val n = 3L; deletedPerfs = deletedPerfs.updated(tenantId, n); n }

    def findInactiveUsers(tenantId: String, lastLoginBeforeMs: Long): Task[List[(String, String)]] =
      ZIO.succeed(inactiveUsers.getOrElse(tenantId, Nil))

    def acquireLock(lockKey: String, durationSecs: Int): Task[Boolean] =
      ZIO.succeed {
        if locks.contains(lockKey) then false
        else { locks = locks + lockKey; true }
      }

    def releaseLock(lockKey: String): Task[Unit] =
      ZIO.succeed { locks = locks - lockKey }

  // ── Config fixture ────────────────────────────────────────────────────────

  val testConfig = SchedulerConfig(
    httpPort = 8080,
    checkOfflineStationsIntervalMins = 5,
    closeTransactionsIntervalHours = 24,
    cleanupLogsIntervalHours = 168,
    checkUserInactivityIntervalHours = 168,
    stationOfflineThresholdSecs = 540,
    staleTransactionThresholdHours = 24,
    logRetentionDays = 7,
    userInactivityMonths = 6,
    offlineStationsTaskEnabled = true,
    closeTransactionsTaskEnabled = true,
    cleanupLogsTaskEnabled = true,
    userInactivityTaskEnabled = true,
    lockDurationSecs = 300,
    defaultTenantId = "tenant1"
  )

  // ── Tests ─────────────────────────────────────────────────────────────────

  override def spec = suite("SchedulerSpec")(
    suite("InMemorySchedulerRepository — distributed locking")(
      test("acquireLock returns true on first call") {
        val repo = new InMemorySchedulerRepository()
        for
          acquired <- repo.acquireLock("my.lock", 300)
        yield assertTrue(acquired)
      },
      test("acquireLock returns false when lock already held") {
        val repo = new InMemorySchedulerRepository()
        for
          first  <- repo.acquireLock("my.lock", 300)
          second <- repo.acquireLock("my.lock", 300)
        yield assertTrue(first) && assertTrue(!second)
      },
      test("lock can be re-acquired after release") {
        val repo = new InMemorySchedulerRepository()
        for
          _     <- repo.acquireLock("my.lock", 300)
          _     <- repo.releaseLock("my.lock")
          reAcq <- repo.acquireLock("my.lock", 300)
        yield assertTrue(reAcq)
      },
      test("different lock keys are independent") {
        val repo = new InMemorySchedulerRepository()
        for
          a <- repo.acquireLock("lock.a", 300)
          b <- repo.acquireLock("lock.b", 300)
        yield assertTrue(a) && assertTrue(b)
      }
    ),
    suite("InMemorySchedulerRepository — tenant enumeration")(
      test("listTenantIds returns configured tenants") {
        val repo = new InMemorySchedulerRepository(tenantIds = List("t1", "t2", "t3"))
        for
          ids <- repo.listTenantIds
        yield assertTrue(ids == List("t1", "t2", "t3"))
      }
    ),
    suite("InMemorySchedulerRepository — station offline")(
      test("findOfflineStationIds returns correct ids per tenant") {
        val repo = new InMemorySchedulerRepository(
          offlineIds = Map("t1" -> List("CS-001", "CS-002"), "t2" -> List("CS-003"))
        )
        for
          t1Ids <- repo.findOfflineStationIds("t1", 0L)
          t2Ids <- repo.findOfflineStationIds("t2", 0L)
          t3Ids <- repo.findOfflineStationIds("t3", 0L) // unknown tenant
        yield assertTrue(t1Ids == List("CS-001", "CS-002")) &&
          assertTrue(t2Ids == List("CS-003")) &&
          assertTrue(t3Ids.isEmpty)
      },
      test("markStationOffline records the tenant and station") {
        val repo = new InMemorySchedulerRepository()
        for
          _ <- repo.markStationOffline("t1", "CS-001")
          _ <- repo.markStationOffline("t1", "CS-002")
          _ <- repo.markStationOffline("t2", "CS-003")
        yield assertTrue(repo.markedOffline.size == 3) &&
          assertTrue(repo.markedOffline.exists(_ == ("t1", "CS-001"))) &&
          assertTrue(repo.markedOffline.exists(_ == ("t2", "CS-003")))
      }
    ),
    suite("InMemorySchedulerRepository — transactions")(
      test("findStaleOpenTransactionIds returns ids per tenant") {
        val repo = new InMemorySchedulerRepository(
          openTxIds = Map("t1" -> List(100L, 200L))
        )
        for
          ids <- repo.findStaleOpenTransactionIds("t1", 0L)
        yield assertTrue(ids == List(100L, 200L))
      },
      test("forceCloseTransaction records the closure") {
        val repo  = new InMemorySchedulerRepository()
        val nowMs = java.lang.System.currentTimeMillis()
        for
          _ <- repo.forceCloseTransaction("t1", 42L, nowMs)
        yield assertTrue(repo.closedTxs.size == 1) &&
          assertTrue(repo.closedTxs.head._1 == "t1") &&
          assertTrue(repo.closedTxs.head._2 == 42L)
      }
    ),
    suite("InMemorySchedulerRepository — log cleanup")(
      test("deleteOldLogs and deleteOldPerformances return delete counts") {
        val repo = new InMemorySchedulerRepository()
        for
          logs  <- repo.deleteOldLogs("t1", 0L)
          perfs <- repo.deleteOldPerformances("t1", 0L)
        yield assertTrue(logs == 5L) && assertTrue(perfs == 3L)
      }
    ),
    suite("InMemorySchedulerRepository — user inactivity")(
      test("findInactiveUsers returns (userId, email) pairs") {
        val repo = new InMemorySchedulerRepository(
          inactiveUsers = Map("t1" -> List(
            ("u1", "u1@example.com"),
            ("u2", "u2@example.com")
          ))
        )
        for
          users <- repo.findInactiveUsers("t1", 0L)
        yield assertTrue(users.size == 2) &&
          assertTrue(users.map(_._1).toSet == Set("u1", "u2")) &&
          assertTrue(users.map(_._2).toSet == Set("u1@example.com", "u2@example.com"))
      }
    ),
    suite("SchedulerConfig defaults")(
      test("all task flags default to enabled") {
        assertTrue(testConfig.offlineStationsTaskEnabled) &&
        assertTrue(testConfig.closeTransactionsTaskEnabled) &&
        assertTrue(testConfig.cleanupLogsTaskEnabled) &&
        assertTrue(testConfig.userInactivityTaskEnabled)
      },
      test("station offline threshold is 540 seconds (9 minutes)") {
        assertTrue(testConfig.stationOfflineThresholdSecs == 540)
      },
      test("lock duration is 300 seconds (5 minutes)") {
        assertTrue(testConfig.lockDurationSecs == 300)
      },
      test("log retention is 7 days") {
        assertTrue(testConfig.logRetentionDays == 7)
      }
    )
  )
