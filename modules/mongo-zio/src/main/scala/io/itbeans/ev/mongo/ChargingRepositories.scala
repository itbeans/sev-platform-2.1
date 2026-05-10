package io.itbeans.ev.mongo

import io.itbeans.ev.domain._
import io.itbeans.ev.mongo.DomainBsonCodecs.given
import org.bson.Document
import org.mongodb.scala.MongoDatabase
import org.mongodb.scala.model.Filters
import zio._
import zio.stream.ZStream

// ---------------------------------------------------------------------------
// ChargingRepositories — ChargingStation and ChargingProfile storage.
// ---------------------------------------------------------------------------

trait ChargingStationRepository:
  def findById(tenantId: TenantId, id: ChargingStationId): Task[Option[ChargingStation]]
  def findBySite(tenantId: TenantId, siteId: SiteId): ZStream[Any, Throwable, ChargingStation]
  def findBySiteArea(tenantId: TenantId, siteAreaId: SiteAreaId): ZStream[Any, Throwable, ChargingStation]
  def findAll(tenantId: TenantId): ZStream[Any, Throwable, ChargingStation]
  def save(cs: ChargingStation): Task[Unit]
  def update(cs: ChargingStation): Task[Unit]
  def delete(tenantId: TenantId, id: ChargingStationId): Task[Unit]

final class MongoChargingStationRepository(db: MongoDatabase) extends ChargingStationRepository:

  private def col(tenantId: TenantId) =
    TenantCollection[Document]("chargingstations", tenantId, db)

  def findById(tenantId: TenantId, id: ChargingStationId): Task[Option[ChargingStation]] =
    col(tenantId).findOne(Filters.eq("_id", id.value)).flatMap {
      case Some(doc) => decodeDocZIO[ChargingStation](doc).map(Some(_))
      case None      => ZIO.none
    }

  def findBySite(tenantId: TenantId, siteId: SiteId): ZStream[Any, Throwable, ChargingStation] =
    col(tenantId).find(Filters.eq("siteId", siteId.value)).mapZIO(decodeDocZIO[ChargingStation])

  def findBySiteArea(tenantId: TenantId, siteAreaId: SiteAreaId): ZStream[Any, Throwable, ChargingStation] =
    col(tenantId).find(Filters.eq("siteAreaId", siteAreaId.value)).mapZIO(decodeDocZIO[ChargingStation])

  def findAll(tenantId: TenantId): ZStream[Any, Throwable, ChargingStation] =
    col(tenantId).find(Filters.empty()).mapZIO(decodeDocZIO[ChargingStation])

  def save(cs: ChargingStation): Task[Unit] =
    col(cs.tenantId).insertOne(encodeDoc(cs))

  def update(cs: ChargingStation): Task[Unit] =
    col(cs.tenantId).replaceOne(Filters.eq("_id", cs.id.value), encodeDoc(cs))

  def delete(tenantId: TenantId, id: ChargingStationId): Task[Unit] =
    col(tenantId).deleteOne(Filters.eq("_id", id.value))

object MongoChargingStationRepository:
  val live: ZLayer[MongoDatabase, Nothing, ChargingStationRepository] =
    ZLayer.fromFunction(MongoChargingStationRepository(_))

// ---------------------------------------------------------------------------

trait ChargingProfileRepository:
  def findById(tenantId: TenantId, id: ChargingProfileId): Task[Option[ChargingProfile]]
  def findByStation(tenantId: TenantId, stationId: ChargingStationId): ZStream[Any, Throwable, ChargingProfile]
  def findBySiteArea(tenantId: TenantId, siteAreaId: SiteAreaId): ZStream[Any, Throwable, ChargingProfile]
  def save(profile: ChargingProfile): Task[Unit]
  def update(profile: ChargingProfile): Task[Unit]
  def deleteByStation(tenantId: TenantId, stationId: ChargingStationId): Task[Int]
  def delete(tenantId: TenantId, id: ChargingProfileId): Task[Unit]

final class MongoChargingProfileRepository(db: MongoDatabase) extends ChargingProfileRepository:

  private def col(tenantId: TenantId) =
    TenantCollection[Document]("chargingprofiles", tenantId, db)

  def findById(tenantId: TenantId, id: ChargingProfileId): Task[Option[ChargingProfile]] =
    col(tenantId).findOne(Filters.eq("_id", id.value)).flatMap {
      case Some(doc) => decodeDocZIO[ChargingProfile](doc).map(Some(_))
      case None      => ZIO.none
    }

  def findByStation(tenantId: TenantId, stationId: ChargingStationId): ZStream[Any, Throwable, ChargingProfile] =
    col(tenantId).find(Filters.eq("chargingStationId", stationId.value)).mapZIO(decodeDocZIO[ChargingProfile])

  def findBySiteArea(tenantId: TenantId, siteAreaId: SiteAreaId): ZStream[Any, Throwable, ChargingProfile] =
    col(tenantId).find(Filters.eq("siteAreaId", siteAreaId.value)).mapZIO(decodeDocZIO[ChargingProfile])

  def save(profile: ChargingProfile): Task[Unit] =
    col(profile.tenantId).insertOne(encodeDoc(profile))

  def update(profile: ChargingProfile): Task[Unit] =
    col(profile.tenantId).replaceOne(Filters.eq("_id", profile.id.value), encodeDoc(profile))

  def deleteByStation(tenantId: TenantId, stationId: ChargingStationId): Task[Int] =
    col(tenantId).deleteMany(Filters.eq("chargingStationId", stationId.value)).map(_.toInt)

  def delete(tenantId: TenantId, id: ChargingProfileId): Task[Unit] =
    col(tenantId).deleteOne(Filters.eq("_id", id.value))

object MongoChargingProfileRepository:
  val live: ZLayer[MongoDatabase, Nothing, ChargingProfileRepository] =
    ZLayer.fromFunction(MongoChargingProfileRepository(_))
