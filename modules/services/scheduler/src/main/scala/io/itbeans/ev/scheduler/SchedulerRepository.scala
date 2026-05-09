package io.itbeans.ev.scheduler

import org.bson.Document
import org.mongodb.scala._
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.Updates._
import org.mongodb.scala.model.{FindOneAndUpdateOptions, ReturnDocument}
import zio._

import java.time.Instant

// ---------------------------------------------------------------------------
// SchedulerRepository — MongoDB access for all scheduler tasks.
//
// Collections:
//   tenants              — global: list all active tenant IDs
//   {t}.chargingstations — offline station detection + status update
//   {t}.transactions     — stale open transaction sweep
//   {t}.logs             — log retention cleanup
//   {t}.performances     — performance data cleanup
//   {t}.users            — user inactivity detection
//   schedulerlocks       — global: distributed task locking
// ---------------------------------------------------------------------------

trait SchedulerRepository:
  // ── Tenant enumeration ────────────────────────────────────────────────
  def listTenantIds: Task[List[String]]

  // ── Charging stations ─────────────────────────────────────────────────
  // Returns chargingStationId strings for stations last seen before cutoffMs
  def findOfflineStationIds(tenantId: String, lastSeenBeforeMs: Long): Task[List[String]]
  def markStationOffline(tenantId: String, chargingStationId: String): Task[Unit]

  // ── Transactions ──────────────────────────────────────────────────────
  // Returns transaction IDs (as Longs) for open transactions started before cutoffMs
  def findStaleOpenTransactionIds(tenantId: String, startedBeforeMs: Long): Task[List[Long]]
  def forceCloseTransaction(tenantId: String, transactionId: Long, nowMs: Long): Task[Unit]

  // ── Log cleanup ───────────────────────────────────────────────────────
  def deleteOldLogs(tenantId: String, olderThanMs: Long): Task[Long]
  def deleteOldPerformances(tenantId: String, olderThanMs: Long): Task[Long]

  // ── User inactivity ───────────────────────────────────────────────────
  // Returns (userId, email) pairs for users inactive since cutoffMs
  def findInactiveUsers(tenantId: String, lastLoginBeforeMs: Long): Task[List[(String, String)]]

  // ── Distributed locking ───────────────────────────────────────────────
  def acquireLock(lockKey: String, durationSecs: Int): Task[Boolean]
  def releaseLock(lockKey: String): Task[Unit]

// ── MongoDB implementation ────────────────────────────────────────────────

final class MongoSchedulerRepository(db: MongoDatabase) extends SchedulerRepository:

  // Global collections (not tenant-namespaced)
  private val tenantsCol = db.getCollection[Document]("tenants")
  private val locksCol   = db.getCollection[Document]("schedulerlocks")

  // Tenant-namespaced helpers
  private def stationsCol(t: String)     = db.getCollection[Document](s"$t.chargingstations")
  private def transactionsCol(t: String) = db.getCollection[Document](s"$t.transactions")
  private def logsCol(t: String)         = db.getCollection[Document](s"$t.logs")
  private def perfsCol(t: String)        = db.getCollection[Document](s"$t.performances")
  private def usersCol(t: String)        = db.getCollection[Document](s"$t.users")

  // ── Tenant enumeration ────────────────────────────────────────────────

  def listTenantIds: Task[List[String]] =
    ZIO.fromFuture { _ =>
      tenantsCol
        .find(and(
          equal("issuer", false), // exclude redirected tenants
          exists("subdomain")
        ))
        .toFuture()
    }.map(_.flatMap(d => Option(d.getString("subdomain"))).toList)

  // ── Charging stations ─────────────────────────────────────────────────

  def findOfflineStationIds(tenantId: String, lastSeenBeforeMs: Long): Task[List[String]] =
    ZIO.fromFuture { _ =>
      stationsCol(tenantId)
        .find(and(
          lt("lastSeen", lastSeenBeforeMs),
          nin("status", "Unavailable", "Faulted") // don't double-flag already-bad ones
        ))
        .toFuture()
    }.map(_.flatMap(d => Option(d.getString("id"))).toList)

  def markStationOffline(tenantId: String, chargingStationId: String): Task[Unit] =
    ZIO.fromFuture { _ =>
      stationsCol(tenantId)
        .updateOne(
          equal("id", chargingStationId),
          set("connectorsStatus.0.status", "Unavailable")
        )
        .toFuture()
    }.unit

  // ── Transactions ──────────────────────────────────────────────────────

  def findStaleOpenTransactionIds(tenantId: String, startedBeforeMs: Long): Task[List[Long]] =
    ZIO.fromFuture { _ =>
      transactionsCol(tenantId)
        .find(and(
          not(exists("stop")), // no stop document = still open
          lt("timestamp", startedBeforeMs)
        ))
        .toFuture()
    }.map(_.flatMap(d => Option(d.getLong("id")).map(_.longValue)).toList)

  def forceCloseTransaction(tenantId: String, transactionId: Long, nowMs: Long): Task[Unit] =
    ZIO.fromFuture { _ =>
      val stopDoc = new Document()
        .append("timestamp", nowMs)
        .append("meterStop", 0.0)
        .append("reason", "AutoStop")
      transactionsCol(tenantId)
        .updateOne(
          equal("id", transactionId),
          set("stop", stopDoc)
        )
        .toFuture()
    }.unit

  // ── Log cleanup ───────────────────────────────────────────────────────

  def deleteOldLogs(tenantId: String, olderThanMs: Long): Task[Long] =
    ZIO.fromFuture { _ =>
      logsCol(tenantId)
        .deleteMany(lt("timestamp", olderThanMs))
        .toFuture()
    }.map(_.getDeletedCount)

  def deleteOldPerformances(tenantId: String, olderThanMs: Long): Task[Long] =
    ZIO.fromFuture { _ =>
      perfsCol(tenantId)
        .deleteMany(lt("timestamp", olderThanMs))
        .toFuture()
    }.map(_.getDeletedCount)

  // ── User inactivity ───────────────────────────────────────────────────

  def findInactiveUsers(tenantId: String, lastLoginBeforeMs: Long): Task[List[(String, String)]] =
    ZIO.fromFuture { _ =>
      usersCol(tenantId)
        .find(and(
          equal("status", "A"), // Active accounts only
          lt("lastLoginOn", lastLoginBeforeMs),
          not(equal("inactivityNotificationSent", true))
        ))
        .toFuture()
    }.map { docs =>
      docs.flatMap { d =>
        for
          id    <- Option(d.getString("id"))
          email <- Option(d.getString("email"))
        yield (id, email)
      }.toList
    }

  // ── Distributed locking ───────────────────────────────────────────────

  /**
   * Acquire a lock for `lockKey` valid for `durationSecs` seconds.
   * Returns true if the lock was acquired, false if already held.
   *
   * Two-step:
   *   1. Delete any expired lock for the key
   *   2. Try to insert a new lock document (DuplicateKeyException → lock held)
   *
   * Tasks are idempotent so occasional double-execution on very short
   * expiry windows is acceptable.
   */
  def acquireLock(lockKey: String, durationSecs: Int): Task[Boolean] =
    val now         = java.lang.System.currentTimeMillis()
    val lockedUntil = now + (durationSecs * 1000L)
    // Step 1: delete expired lock
    ZIO.fromFuture { _ =>
      locksCol.deleteOne(and(
        equal("_id", lockKey),
        lt("lockedUntil", now)
      )).toFuture()
    }.flatMap { _ =>
      // Step 2: try to insert — fails with duplicate key if lock is still held
      ZIO.fromFuture { _ =>
        locksCol.insertOne(
          new Document("_id", lockKey).append("lockedUntil", lockedUntil)
        ).toFuture()
      }.as(true)
        .catchSome {
          case e: com.mongodb.MongoWriteException
              if e.getError.getCode == 11000 => ZIO.succeed(false)
          case e: com.mongodb.MongoBulkWriteException
              if e.getMessage.contains("11000") => ZIO.succeed(false)
        }
    }

  def releaseLock(lockKey: String): Task[Unit] =
    ZIO.fromFuture { _ =>
      locksCol.deleteOne(equal("_id", lockKey)).toFuture()
    }.unit

object MongoSchedulerRepository:

  val live: ZLayer[MongoDatabase, Nothing, SchedulerRepository] =
    ZLayer.fromFunction(new MongoSchedulerRepository(_))
