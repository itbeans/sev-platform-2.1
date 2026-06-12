package io.itbeans.ev.mongo

import io.itbeans.ev.domain._
import io.itbeans.ev.mongo.DomainBsonCodecs.given
import org.bson.Document
import org.mongodb.scala.MongoDatabase
import org.mongodb.scala.model.Filters
import zio._
import zio.stream.ZStream

// ---------------------------------------------------------------------------
// AssetRepositories — Asset, Connection, PricingDefinition storage.
// ---------------------------------------------------------------------------

trait AssetRepository:
  def findById(tenantId: TenantId, id: AssetId): Task[Option[Asset]]
  def findBySiteArea(tenantId: TenantId, siteAreaId: SiteAreaId): ZStream[Any, Throwable, Asset]
  def findAll(tenantId: TenantId): ZStream[Any, Throwable, Asset]
  def save(asset: Asset): Task[Unit]
  def update(asset: Asset): Task[Unit]
  def delete(tenantId: TenantId, id: AssetId): Task[Unit]

final class MongoAssetRepository(db: MongoDatabase) extends AssetRepository:

  private def col(tenantId: TenantId) =
    TenantCollection[Document]("assets", tenantId, db)

  def findById(tenantId: TenantId, id: AssetId): Task[Option[Asset]] =
    col(tenantId).findOne(Filters.eq("_id", id.value)).flatMap {
      case Some(doc) => decodeDocZIO[Asset](doc).map(Some(_))
      case None      => ZIO.none
    }

  def findBySiteArea(tenantId: TenantId, siteAreaId: SiteAreaId): ZStream[Any, Throwable, Asset] =
    col(tenantId).find(Filters.eq("siteAreaId", siteAreaId.value)).mapZIO(decodeDocZIO[Asset])

  def findAll(tenantId: TenantId): ZStream[Any, Throwable, Asset] =
    col(tenantId).find(Filters.empty()).mapZIO(decodeDocZIO[Asset])

  def save(asset: Asset): Task[Unit] =
    col(asset.tenantId).insertOne(encodeDoc(asset))

  def update(asset: Asset): Task[Unit] =
    col(asset.tenantId).replaceOne(Filters.eq("_id", asset.id.value), encodeDoc(asset))

  def delete(tenantId: TenantId, id: AssetId): Task[Unit] =
    col(tenantId).deleteOne(Filters.eq("_id", id.value))

object MongoAssetRepository:

  val live: ZLayer[MongoDatabase, Nothing, AssetRepository] =
    ZLayer.fromFunction(MongoAssetRepository(_))

// ---------------------------------------------------------------------------

trait ConnectionRepository:
  def findById(tenantId: TenantId, id: ConnectionId): Task[Option[Connection]]
  def findAll(tenantId: TenantId): ZStream[Any, Throwable, Connection]
  def save(conn: Connection): Task[Unit]
  def update(conn: Connection): Task[Unit]
  def delete(tenantId: TenantId, id: ConnectionId): Task[Unit]

final class MongoConnectionRepository(db: MongoDatabase) extends ConnectionRepository:

  private def col(tenantId: TenantId) =
    TenantCollection[Document]("connections", tenantId, db)

  def findById(tenantId: TenantId, id: ConnectionId): Task[Option[Connection]] =
    col(tenantId).findOne(Filters.eq("_id", id.value)).flatMap {
      case Some(doc) => decodeDocZIO[Connection](doc).map(Some(_))
      case None      => ZIO.none
    }

  def findAll(tenantId: TenantId): ZStream[Any, Throwable, Connection] =
    col(tenantId).find(Filters.empty()).mapZIO(decodeDocZIO[Connection])

  def save(conn: Connection): Task[Unit] =
    col(conn.tenantId).insertOne(encodeDoc(conn))

  def update(conn: Connection): Task[Unit] =
    col(conn.tenantId).replaceOne(Filters.eq("_id", conn.id.value), encodeDoc(conn))

  def delete(tenantId: TenantId, id: ConnectionId): Task[Unit] =
    col(tenantId).deleteOne(Filters.eq("_id", id.value))

object MongoConnectionRepository:

  val live: ZLayer[MongoDatabase, Nothing, ConnectionRepository] =
    ZLayer.fromFunction(MongoConnectionRepository(_))

// ---------------------------------------------------------------------------

trait PricingDefinitionRepository:
  def findById(tenantId: TenantId, id: PricingId): Task[Option[PricingDefinition]]
  def findByEntity(tenantId: TenantId, entityId: String): ZStream[Any, Throwable, PricingDefinition]
  def findBySite(tenantId: TenantId, siteId: SiteId): ZStream[Any, Throwable, PricingDefinition]
  def findAll(tenantId: TenantId): ZStream[Any, Throwable, PricingDefinition]
  def save(pd: PricingDefinition): Task[Unit]
  def update(pd: PricingDefinition): Task[Unit]
  def delete(tenantId: TenantId, id: PricingId): Task[Unit]

final class MongoPricingDefinitionRepository(db: MongoDatabase) extends PricingDefinitionRepository:

  private def col(tenantId: TenantId) =
    TenantCollection[Document]("pricingdefinitions", tenantId, db)

  def findById(tenantId: TenantId, id: PricingId): Task[Option[PricingDefinition]] =
    col(tenantId).findOne(Filters.eq("_id", id.value)).flatMap {
      case Some(doc) => decodeDocZIO[PricingDefinition](doc).map(Some(_))
      case None      => ZIO.none
    }

  def findByEntity(tenantId: TenantId, entityId: String): ZStream[Any, Throwable, PricingDefinition] =
    col(tenantId).find(Filters.eq("entityId", entityId)).mapZIO(decodeDocZIO[PricingDefinition])

  def findBySite(tenantId: TenantId, siteId: SiteId): ZStream[Any, Throwable, PricingDefinition] =
    col(tenantId).find(Filters.eq("siteId", siteId.value)).mapZIO(decodeDocZIO[PricingDefinition])

  def findAll(tenantId: TenantId): ZStream[Any, Throwable, PricingDefinition] =
    col(tenantId).find(Filters.empty()).mapZIO(decodeDocZIO[PricingDefinition])

  def save(pd: PricingDefinition): Task[Unit] =
    col(pd.tenantId).insertOne(encodeDoc(pd))

  def update(pd: PricingDefinition): Task[Unit] =
    col(pd.tenantId).replaceOne(Filters.eq("_id", pd.id.value), encodeDoc(pd))

  def delete(tenantId: TenantId, id: PricingId): Task[Unit] =
    col(tenantId).deleteOne(Filters.eq("_id", id.value))

object MongoPricingDefinitionRepository:

  val live: ZLayer[MongoDatabase, Nothing, PricingDefinitionRepository] =
    ZLayer.fromFunction(MongoPricingDefinitionRepository(_))
