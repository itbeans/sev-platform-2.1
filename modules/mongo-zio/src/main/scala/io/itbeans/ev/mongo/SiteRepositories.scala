package io.itbeans.ev.mongo

import io.itbeans.ev.domain._
import io.itbeans.ev.mongo.DomainBsonCodecs.{*, given}
import org.bson.Document
import org.mongodb.scala.MongoDatabase
import org.mongodb.scala.model.Filters
import zio._
import zio.stream.ZStream

// ---------------------------------------------------------------------------
// SiteRepositories — Site, SiteArea, Company storage.
// ---------------------------------------------------------------------------

trait SiteRepository:
  def findById(tenantId: TenantId, id: SiteId): Task[Option[Site]]
  def findByCompany(tenantId: TenantId, companyId: CompanyId): ZStream[Any, Throwable, Site]
  def findAll(tenantId: TenantId): ZStream[Any, Throwable, Site]
  def save(site: Site): Task[Unit]
  def update(site: Site): Task[Unit]
  def delete(tenantId: TenantId, id: SiteId): Task[Unit]

final class MongoSiteRepository(db: MongoDatabase) extends SiteRepository:

  private def col(tenantId: TenantId) =
    TenantCollection[Document]("sites", tenantId, db)

  def findById(tenantId: TenantId, id: SiteId): Task[Option[Site]] =
    col(tenantId).findOne(Filters.eq("_id", id.value)).flatMap {
      case Some(doc) => decodeDocZIO[Site](doc).map(Some(_))
      case None      => ZIO.none
    }

  def findByCompany(tenantId: TenantId, companyId: CompanyId): ZStream[Any, Throwable, Site] =
    col(tenantId).find(Filters.eq("companyId", companyId.value)).mapZIO(decodeDocZIO[Site])

  def findAll(tenantId: TenantId): ZStream[Any, Throwable, Site] =
    col(tenantId).find(Filters.empty()).mapZIO(decodeDocZIO[Site])

  def save(site: Site): Task[Unit] =
    col(site.tenantId).insertOne(encodeDoc(site))

  def update(site: Site): Task[Unit] =
    col(site.tenantId).replaceOne(Filters.eq("_id", site.id.value), encodeDoc(site))

  def delete(tenantId: TenantId, id: SiteId): Task[Unit] =
    col(tenantId).deleteOne(Filters.eq("_id", id.value))

object MongoSiteRepository:

  val live: ZLayer[MongoDatabase, Nothing, SiteRepository] =
    ZLayer.fromFunction(MongoSiteRepository(_))

// ---------------------------------------------------------------------------

trait SiteAreaRepository:
  def findById(tenantId: TenantId, id: SiteAreaId): Task[Option[SiteArea]]
  def findBySite(tenantId: TenantId, siteId: SiteId): ZStream[Any, Throwable, SiteArea]
  def findAll(tenantId: TenantId): ZStream[Any, Throwable, SiteArea]
  def save(area: SiteArea): Task[Unit]
  def update(area: SiteArea): Task[Unit]
  def delete(tenantId: TenantId, id: SiteAreaId): Task[Unit]

final class MongoSiteAreaRepository(db: MongoDatabase) extends SiteAreaRepository:

  private def col(tenantId: TenantId) =
    TenantCollection[Document]("siteareas", tenantId, db)

  def findById(tenantId: TenantId, id: SiteAreaId): Task[Option[SiteArea]] =
    col(tenantId).findOne(Filters.eq("_id", id.value)).flatMap {
      case Some(doc) => decodeDocZIO[SiteArea](doc).map(Some(_))
      case None      => ZIO.none
    }

  def findBySite(tenantId: TenantId, siteId: SiteId): ZStream[Any, Throwable, SiteArea] =
    col(tenantId).find(Filters.eq("siteId", siteId.value)).mapZIO(decodeDocZIO[SiteArea])

  def findAll(tenantId: TenantId): ZStream[Any, Throwable, SiteArea] =
    col(tenantId).find(Filters.empty()).mapZIO(decodeDocZIO[SiteArea])

  def save(area: SiteArea): Task[Unit] =
    col(area.tenantId).insertOne(encodeDoc(area))

  def update(area: SiteArea): Task[Unit] =
    col(area.tenantId).replaceOne(Filters.eq("_id", area.id.value), encodeDoc(area))

  def delete(tenantId: TenantId, id: SiteAreaId): Task[Unit] =
    col(tenantId).deleteOne(Filters.eq("_id", id.value))

object MongoSiteAreaRepository:

  val live: ZLayer[MongoDatabase, Nothing, SiteAreaRepository] =
    ZLayer.fromFunction(MongoSiteAreaRepository(_))

// ---------------------------------------------------------------------------

trait CompanyRepository:
  def findById(tenantId: TenantId, id: CompanyId): Task[Option[Company]]
  def findAll(tenantId: TenantId): ZStream[Any, Throwable, Company]
  def save(company: Company): Task[Unit]
  def update(company: Company): Task[Unit]
  def delete(tenantId: TenantId, id: CompanyId): Task[Unit]

final class MongoCompanyRepository(db: MongoDatabase) extends CompanyRepository:

  private def col(tenantId: TenantId) =
    TenantCollection[Document]("companies", tenantId, db)

  def findById(tenantId: TenantId, id: CompanyId): Task[Option[Company]] =
    col(tenantId).findOne(Filters.eq("_id", id.value)).flatMap {
      case Some(doc) => decodeDocZIO[Company](doc).map(Some(_))
      case None      => ZIO.none
    }

  def findAll(tenantId: TenantId): ZStream[Any, Throwable, Company] =
    col(tenantId).find(Filters.empty()).mapZIO(decodeDocZIO[Company])

  def save(company: Company): Task[Unit] =
    col(company.tenantId).insertOne(encodeDoc(company))

  def update(company: Company): Task[Unit] =
    col(company.tenantId).replaceOne(Filters.eq("_id", company.id.value), encodeDoc(company))

  def delete(tenantId: TenantId, id: CompanyId): Task[Unit] =
    col(tenantId).deleteOne(Filters.eq("_id", id.value))

object MongoCompanyRepository:

  val live: ZLayer[MongoDatabase, Nothing, CompanyRepository] =
    ZLayer.fromFunction(MongoCompanyRepository(_))
