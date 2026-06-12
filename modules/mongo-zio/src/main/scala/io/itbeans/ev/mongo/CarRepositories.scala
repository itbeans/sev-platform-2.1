package io.itbeans.ev.mongo

import io.itbeans.ev.domain._
import io.itbeans.ev.mongo.DomainBsonCodecs.{*, given}
import org.bson.Document
import org.mongodb.scala.MongoDatabase
import org.mongodb.scala.model.Filters
import zio._
import zio.stream.ZStream

// ---------------------------------------------------------------------------
// CarRepositories — Car (user EVs) and CarCatalog (global, non-tenant).
// ---------------------------------------------------------------------------

trait CarRepository:
  def findById(tenantId: TenantId, id: CarId): Task[Option[Car]]
  def findByUser(tenantId: TenantId, userId: UserId): ZStream[Any, Throwable, Car]
  def findAll(tenantId: TenantId): ZStream[Any, Throwable, Car]
  def save(car: Car): Task[Unit]
  def update(car: Car): Task[Unit]
  def delete(tenantId: TenantId, id: CarId): Task[Unit]

final class MongoCarRepository(db: MongoDatabase) extends CarRepository:

  private def col(tenantId: TenantId) =
    TenantCollection[Document]("cars", tenantId, db)

  def findById(tenantId: TenantId, id: CarId): Task[Option[Car]] =
    col(tenantId).findOne(Filters.eq("_id", id.value)).flatMap {
      case Some(doc) => decodeDocZIO[Car](doc).map(Some(_))
      case None      => ZIO.none
    }

  def findByUser(tenantId: TenantId, userId: UserId): ZStream[Any, Throwable, Car] =
    col(tenantId).find(Filters.eq("userId", userId.value)).mapZIO(decodeDocZIO[Car])

  def findAll(tenantId: TenantId): ZStream[Any, Throwable, Car] =
    col(tenantId).find(Filters.empty()).mapZIO(decodeDocZIO[Car])

  def save(car: Car): Task[Unit] =
    col(car.tenantId).insertOne(encodeDoc(car))

  def update(car: Car): Task[Unit] =
    col(car.tenantId).replaceOne(Filters.eq("_id", car.id.value), encodeDoc(car))

  def delete(tenantId: TenantId, id: CarId): Task[Unit] =
    col(tenantId).deleteOne(Filters.eq("_id", id.value))

object MongoCarRepository:

  val live: ZLayer[MongoDatabase, Nothing, CarRepository] =
    ZLayer.fromFunction(MongoCarRepository(_))

// ---------------------------------------------------------------------------
// CarCatalog is stored in a global (non-tenant-prefixed) collection.
// The primary key is an Int (catalogue entry ID from EV Database).
// ---------------------------------------------------------------------------

trait CarCatalogRepository:
  def findById(id: Int): Task[Option[CarCatalog]]
  def findAll(): ZStream[Any, Throwable, CarCatalog]
  def upsert(entry: CarCatalog): Task[Unit]

final class MongoCarCatalogRepository(db: MongoDatabase) extends CarCatalogRepository:

  private val col = GlobalCollection[Document]("carcatalogs", db)

  def findById(id: Int): Task[Option[CarCatalog]] =
    col.findOne(Filters.eq("_id", id)).flatMap {
      case Some(doc) => decodeDocZIO[CarCatalog](doc).map(Some(_))
      case None      => ZIO.none
    }

  def findAll(): ZStream[Any, Throwable, CarCatalog] =
    col.find(Filters.empty()).mapZIO(decodeDocZIO[CarCatalog])

  def upsert(entry: CarCatalog): Task[Unit] =
    val doc = encodeDoc(entry)
    col.replaceOne(Filters.eq("_id", entry.id), doc, upsert = true)

object MongoCarCatalogRepository:

  val live: ZLayer[MongoDatabase, Nothing, CarCatalogRepository] =
    ZLayer.fromFunction(MongoCarCatalogRepository(_))
