package io.itbeans.ev.mongo

import io.itbeans.ev.domain._
import io.itbeans.ev.mongo.DomainBsonCodecs.{*, given}
import org.bson.Document
import org.mongodb.scala.MongoDatabase
import org.mongodb.scala.model.{Filters, Sorts}
import zio._
import zio.stream.ZStream

// ---------------------------------------------------------------------------
// TransactionRepositories — Transaction and Consumption storage.
// ---------------------------------------------------------------------------

trait TransactionRepository:
  def findById(tenantId: TenantId, id: TransactionId): Task[Option[Transaction]]
  def findByStation(tenantId: TenantId, stationId: ChargingStationId): ZStream[Any, Throwable, Transaction]
  def findInProgress(tenantId: TenantId): ZStream[Any, Throwable, Transaction]
  def findByUser(tenantId: TenantId, userId: UserId): ZStream[Any, Throwable, Transaction]
  def save(tx: Transaction): Task[Unit]
  def update(tx: Transaction): Task[Unit]

final class MongoTransactionRepository(db: MongoDatabase) extends TransactionRepository:

  private def col(tenantId: TenantId) =
    TenantCollection[Document]("transactions", tenantId, db)

  def findById(tenantId: TenantId, id: TransactionId): Task[Option[Transaction]] =
    col(tenantId).findOne(Filters.eq("_id", id.value)).flatMap {
      case Some(doc) => decodeDocZIO[Transaction](doc).map(Some(_))
      case None      => ZIO.none
    }

  def findByStation(tenantId: TenantId, stationId: ChargingStationId): ZStream[Any, Throwable, Transaction] =
    col(tenantId)
      .find(Filters.eq("chargingStationId", stationId.value), Sorts.descending("startDate"), Int.MaxValue)
      .mapZIO(decodeDocZIO[Transaction])

  def findInProgress(tenantId: TenantId): ZStream[Any, Throwable, Transaction] =
    col(tenantId).find(Filters.eq("inProgress", true)).mapZIO(decodeDocZIO[Transaction])

  def findByUser(tenantId: TenantId, userId: UserId): ZStream[Any, Throwable, Transaction] =
    col(tenantId)
      .find(Filters.eq("userId", userId.value), Sorts.descending("startDate"), Int.MaxValue)
      .mapZIO(decodeDocZIO[Transaction])

  def save(tx: Transaction): Task[Unit] =
    col(tx.tenantId).insertOne(encodeDoc(tx))

  def update(tx: Transaction): Task[Unit] =
    col(tx.tenantId).replaceOne(Filters.eq("_id", tx.id.value), encodeDoc(tx))

object MongoTransactionRepository:

  val live: ZLayer[MongoDatabase, Nothing, TransactionRepository] =
    ZLayer.fromFunction(MongoTransactionRepository(_))

// ---------------------------------------------------------------------------

trait ConsumptionRepository:
  def findByTransaction(tenantId: TenantId, txId: TransactionId): ZStream[Any, Throwable, Consumption]
  def findByStation(tenantId: TenantId, stationId: ChargingStationId): ZStream[Any, Throwable, Consumption]
  def save(c: Consumption): Task[Unit]
  def saveMany(cs: Seq[Consumption]): Task[Unit]

final class MongoConsumptionRepository(db: MongoDatabase) extends ConsumptionRepository:

  private def col(tenantId: TenantId) =
    TenantCollection[Document]("consumptions", tenantId, db)

  def findByTransaction(tenantId: TenantId, txId: TransactionId): ZStream[Any, Throwable, Consumption] =
    col(tenantId).find(Filters.eq("transactionId", txId.value)).mapZIO(decodeDocZIO[Consumption])

  def findByStation(tenantId: TenantId, stationId: ChargingStationId): ZStream[Any, Throwable, Consumption] =
    col(tenantId).find(Filters.eq("chargingStationId", stationId.value)).mapZIO(decodeDocZIO[Consumption])

  def save(c: Consumption): Task[Unit] =
    col(c.tenantId).insertOne(encodeDoc(c))

  def saveMany(cs: Seq[Consumption]): Task[Unit] =
    cs.headOption match
      case None     => ZIO.unit
      case Some(hd) => col(hd.tenantId).insertMany(cs.map(encodeDoc(_)))

object MongoConsumptionRepository:

  val live: ZLayer[MongoDatabase, Nothing, ConsumptionRepository] =
    ZLayer.fromFunction(MongoConsumptionRepository(_))
