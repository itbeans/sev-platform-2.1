package io.itbeans.ev.mongo

import io.itbeans.ev.domain._
// Named import wins over the zio._ wildcard, which also exports a `Tag` type
import io.itbeans.ev.domain.Tag
import io.itbeans.ev.mongo.DomainBsonCodecs.{*, given}
import org.bson.Document
import org.mongodb.scala.MongoDatabase
import org.mongodb.scala.model.Filters
import zio._
import zio.stream.ZStream

// ---------------------------------------------------------------------------
// IdentityRepositories — User, Tag, UserSite, Tenant storage.
// Collections follow {tenantId}.{name} convention except Tenant (global).
// ---------------------------------------------------------------------------

trait UserRepository:
  def findById(tenantId: TenantId, id: UserId): Task[Option[User]]
  def findByEmail(tenantId: TenantId, email: String): Task[Option[User]]
  def findAll(tenantId: TenantId): ZStream[Any, Throwable, User]
  def save(user: User): Task[Unit]
  def update(user: User): Task[Unit]
  def delete(tenantId: TenantId, id: UserId): Task[Unit]

final class MongoUserRepository(db: MongoDatabase) extends UserRepository:

  private def col(tenantId: TenantId) =
    TenantCollection[Document]("users", tenantId, db)

  def findById(tenantId: TenantId, id: UserId): Task[Option[User]] =
    col(tenantId).findOne(Filters.eq("_id", id.value)).flatMap {
      case Some(doc) => decodeDocZIO[User](doc).map(Some(_))
      case None      => ZIO.none
    }

  def findByEmail(tenantId: TenantId, email: String): Task[Option[User]] =
    col(tenantId).findOne(Filters.eq("email", email)).flatMap {
      case Some(doc) => decodeDocZIO[User](doc).map(Some(_))
      case None      => ZIO.none
    }

  def findAll(tenantId: TenantId): ZStream[Any, Throwable, User] =
    col(tenantId).find(Filters.empty()).mapZIO(decodeDocZIO[User])

  def save(user: User): Task[Unit] =
    col(user.tenantId).insertOne(encodeDoc(user))

  def update(user: User): Task[Unit] =
    col(user.tenantId).replaceOne(Filters.eq("_id", user.id.value), encodeDoc(user))

  def delete(tenantId: TenantId, id: UserId): Task[Unit] =
    col(tenantId).deleteOne(Filters.eq("_id", id.value))

object MongoUserRepository:

  val live: ZLayer[MongoDatabase, Nothing, UserRepository] =
    ZLayer.fromFunction(MongoUserRepository(_))

// ---------------------------------------------------------------------------

trait TagRepository:
  def findById(tenantId: TenantId, id: String): Task[Option[Tag]]
  def findByUserId(tenantId: TenantId, userId: UserId): ZStream[Any, Throwable, Tag]
  def findAll(tenantId: TenantId): ZStream[Any, Throwable, Tag]
  def save(tag: Tag): Task[Unit]
  def update(tag: Tag): Task[Unit]
  def delete(tenantId: TenantId, id: String): Task[Unit]

final class MongoTagRepository(db: MongoDatabase) extends TagRepository:

  private def col(tenantId: TenantId) =
    TenantCollection[Document]("tags", tenantId, db)

  def findById(tenantId: TenantId, id: String): Task[Option[Tag]] =
    col(tenantId).findOne(Filters.eq("_id", id)).flatMap {
      case Some(doc) => decodeDocZIO[Tag](doc).map(Some(_))
      case None      => ZIO.none
    }

  def findByUserId(tenantId: TenantId, userId: UserId): ZStream[Any, Throwable, Tag] =
    col(tenantId).find(Filters.eq("userId", userId.value)).mapZIO(decodeDocZIO[Tag])

  def findAll(tenantId: TenantId): ZStream[Any, Throwable, Tag] =
    col(tenantId).find(Filters.empty()).mapZIO(decodeDocZIO[Tag])

  def save(tag: Tag): Task[Unit] =
    col(tag.tenantId).insertOne(encodeDoc(tag))

  def update(tag: Tag): Task[Unit] =
    col(tag.tenantId).replaceOne(Filters.eq("_id", tag.id), encodeDoc(tag))

  def delete(tenantId: TenantId, id: String): Task[Unit] =
    col(tenantId).deleteOne(Filters.eq("_id", id))

object MongoTagRepository:

  val live: ZLayer[MongoDatabase, Nothing, TagRepository] =
    ZLayer.fromFunction(MongoTagRepository(_))

// ---------------------------------------------------------------------------

trait UserSiteRepository:
  def findBySite(tenantId: TenantId, siteId: SiteId): ZStream[Any, Throwable, UserSite]
  def findByUser(tenantId: TenantId, userId: UserId): ZStream[Any, Throwable, UserSite]
  def findById(tenantId: TenantId, id: String): Task[Option[UserSite]]
  def save(userSite: UserSite): Task[Unit]
  def update(userSite: UserSite): Task[Unit]
  def delete(tenantId: TenantId, id: String): Task[Unit]

final class MongoUserSiteRepository(db: MongoDatabase) extends UserSiteRepository:

  private def col(tenantId: TenantId) =
    TenantCollection[Document]("userssites", tenantId, db)

  def findBySite(tenantId: TenantId, siteId: SiteId): ZStream[Any, Throwable, UserSite] =
    col(tenantId).find(Filters.eq("siteId", siteId.value)).mapZIO(decodeDocZIO[UserSite])

  def findByUser(tenantId: TenantId, userId: UserId): ZStream[Any, Throwable, UserSite] =
    col(tenantId).find(Filters.eq("userId", userId.value)).mapZIO(decodeDocZIO[UserSite])

  def findById(tenantId: TenantId, id: String): Task[Option[UserSite]] =
    col(tenantId).findOne(Filters.eq("_id", id)).flatMap {
      case Some(doc) => decodeDocZIO[UserSite](doc).map(Some(_))
      case None      => ZIO.none
    }

  def save(us: UserSite): Task[Unit] =
    col(us.tenantId).insertOne(encodeDoc(us))

  def update(us: UserSite): Task[Unit] =
    col(us.tenantId).replaceOne(Filters.eq("_id", us.id), encodeDoc(us))

  def delete(tenantId: TenantId, id: String): Task[Unit] =
    col(tenantId).deleteOne(Filters.eq("_id", id))

object MongoUserSiteRepository:

  val live: ZLayer[MongoDatabase, Nothing, UserSiteRepository] =
    ZLayer.fromFunction(MongoUserSiteRepository(_))

// ---------------------------------------------------------------------------

trait TenantRepository:
  def findById(id: TenantId): Task[Option[Tenant]]
  def findBySubdomain(subdomain: String): Task[Option[Tenant]]
  def findAll(): ZStream[Any, Throwable, Tenant]
  def save(tenant: Tenant): Task[Unit]
  def update(tenant: Tenant): Task[Unit]

final class MongoTenantRepository(db: MongoDatabase) extends TenantRepository:

  private val col = GlobalCollection[Document]("tenants", db)

  def findById(id: TenantId): Task[Option[Tenant]] =
    col.findOne(Filters.eq("_id", id.value)).flatMap {
      case Some(doc) => decodeDocZIO[Tenant](doc).map(Some(_))
      case None      => ZIO.none
    }

  def findBySubdomain(subdomain: String): Task[Option[Tenant]] =
    col.findOne(Filters.eq("subdomain", subdomain)).flatMap {
      case Some(doc) => decodeDocZIO[Tenant](doc).map(Some(_))
      case None      => ZIO.none
    }

  def findAll(): ZStream[Any, Throwable, Tenant] =
    col.find(Filters.empty()).mapZIO(decodeDocZIO[Tenant])

  def save(tenant: Tenant): Task[Unit] =
    col.insertOne(encodeDoc(tenant))

  def update(tenant: Tenant): Task[Unit] =
    col.replaceOne(Filters.eq("_id", tenant.id.value), encodeDoc(tenant))

object MongoTenantRepository:

  val live: ZLayer[MongoDatabase, Nothing, TenantRepository] =
    ZLayer.fromFunction(MongoTenantRepository(_))
