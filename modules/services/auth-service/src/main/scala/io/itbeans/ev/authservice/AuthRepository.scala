package io.itbeans.ev.authservice

import io.itbeans.ev.domain.TenantId
import io.itbeans.ev.mongo.TenantCollection
import org.bson.Document
import org.mongodb.scala._
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.{FindOneAndReplaceOptions, Updates}
import zio._

import java.time.Instant
import scala.jdk.CollectionConverters._

// ---------------------------------------------------------------------------
// AuthRepository — MongoDB repositories for User, Tenant, Tag, UserSite.
//
// Collections (all tenant-scoped except tenants):
//   {tenantId}.users         — AuthUser documents
//   {tenantId}.tags          — AuthTag documents
//   {tenantId}.userssites    — UserSite assignments
//   tenants                  — AuthTenant documents (global, no prefix)
// ---------------------------------------------------------------------------

trait UserRepository:
  def findByEmail(tenantId: TenantId, email: String): Task[Option[AuthUser]]
  def findById(tenantId: TenantId, id: String): Task[Option[AuthUser]]

  /** Persist updated lockout counters (passwordWrongNbrTrials, passwordBlockedUntil). */
  def updateLockout(tenantId: TenantId, userId: String, trials: Int, blockedUntil: Option[Instant]): Task[Unit]
  def upsert(tenantId: TenantId, user: AuthUser): Task[Unit]

trait TenantRepository:
  def findBySubdomain(subdomain: String): Task[Option[AuthTenant]]
  def findById(id: String): Task[Option[AuthTenant]]

trait TagRepository:
  def findById(tenantId: TenantId, tagId: String): Task[Option[AuthTag]]
  def findActiveByUserId(tenantId: TenantId, userId: String): Task[List[AuthTag]]

trait UserSiteRepository:
  def findByUserId(tenantId: TenantId, userId: String): Task[List[UserSite]]

// ── MongoUserRepository ───────────────────────────────────────────────────

final class MongoUserRepository(db: MongoDatabase) extends UserRepository:

  private def col(tenantId: TenantId) =
    TenantCollection[Document]("users", tenantId, db)

  override def findByEmail(tenantId: TenantId, email: String): Task[Option[AuthUser]] =
    col(tenantId).findOne(equal("email", email)).map(_.map(docToUser))

  override def findById(tenantId: TenantId, id: String): Task[Option[AuthUser]] =
    col(tenantId).findOne(equal("_id", id)).map(_.map(docToUser))

  override def updateLockout(
      tenantId: TenantId,
      userId: String,
      trials: Int,
      blockedUntil: Option[Instant]
  ): Task[Unit] =
    ZIO.fromFuture { _ =>
      val updates = blockedUntil match
        case None =>
          Updates.combine(
            Updates.set("passwordWrongNbrTrials", trials),
            Updates.unset("passwordBlockedUntil")
          )
        case Some(t) =>
          Updates.combine(
            Updates.set("passwordWrongNbrTrials", trials),
            Updates.set("passwordBlockedUntil", t.toEpochMilli)
          )
      db.getCollection[Document](s"${tenantId.value}.users")
        .updateOne(equal("_id", userId), updates)
        .toFuture()
    }.unit

  override def upsert(tenantId: TenantId, user: AuthUser): Task[Unit] =
    ZIO.fromFuture { _ =>
      val opts = FindOneAndReplaceOptions().upsert(true)
      db.getCollection[Document](s"${tenantId.value}.users")
        .findOneAndReplace(equal("_id", user.id), userToDoc(user), opts)
        .toFuture()
    }.unit

  // ── BSON conversion ────────────────────────────────────────────────────

  private def userToDoc(u: AuthUser): Document =
    val doc = new Document("_id", u.id)
      .append("email", u.email)
      .append("password", u.password)
      .append("firstName", u.firstName)
      .append("name", u.name)
      .append("locale", u.locale)
      .append("role", u.role)
      .append("status", u.status.code)
      .append("technical", u.technical)
      .append("issuer", u.issuer)
      .append("passwordWrongNbrTrials", u.passwordWrongNbrTrials)
      .append("notificationsActive", u.notificationsActive)
    u.mobile.foreach(doc.append("mobile", _))
    u.passwordBlockedUntil.foreach(t => doc.append("passwordBlockedUntil", t.toEpochMilli))
    if u.companyIDs.nonEmpty then doc.append("companyIDs", u.companyIDs.asJava)
    doc

  private def docToUser(d: Document): AuthUser =
    AuthUser(
      id = d.getString("_id"),
      email = d.getString("email"),
      password = d.getString("password"),
      firstName = Option(d.getString("firstName")).getOrElse(""),
      name = Option(d.getString("name")).getOrElse(""),
      mobile = Option(d.getString("mobile")),
      locale = Option(d.getString("locale")).getOrElse("en_US"),
      role = Option(d.getString("role")).getOrElse("B"),
      status = AuthUserStatus.fromCode(Option(d.getString("status")).getOrElse("A"))
        .getOrElse(AuthUserStatus.Active),
      technical = Option(d.getBoolean("technical")).exists(_.booleanValue),
      issuer = Option(d.getBoolean("issuer")).exists(_.booleanValue),
      passwordWrongNbrTrials = Option(d.getInteger("passwordWrongNbrTrials")).map(_.intValue).getOrElse(0),
      passwordBlockedUntil = Option(d.getLong("passwordBlockedUntil")).map(l => Instant.ofEpochMilli(l.longValue)),
      companyIDs = Option(d.getList("companyIDs", classOf[String]))
        .map(_.asScala.toList).getOrElse(Nil),
      notificationsActive = Option(d.getBoolean("notificationsActive")).exists(_.booleanValue)
    )

object MongoUserRepository:

  val live: ZLayer[MongoDatabase, Nothing, UserRepository] =
    ZLayer.fromFunction(new MongoUserRepository(_))

// ── MongoTenantRepository ─────────────────────────────────────────────────

final class MongoTenantRepository(db: MongoDatabase) extends TenantRepository:

  // Tenants are stored in a global (non-tenant-scoped) collection.
  private val col = db.getCollection[Document]("tenants")

  override def findBySubdomain(subdomain: String): Task[Option[AuthTenant]] =
    ZIO.fromFuture(_ => col.find(equal("subdomain", subdomain)).first().toFuture())
      .map(d => Option(d).map(docToTenant))

  override def findById(id: String): Task[Option[AuthTenant]] =
    ZIO.fromFuture(_ => col.find(equal("_id", id)).first().toFuture())
      .map(d => Option(d).map(docToTenant))

  private def docToTenant(d: Document): AuthTenant =
    AuthTenant(
      id = d.getString("_id"),
      subdomain = Option(d.getString("subdomain")).getOrElse(""),
      name = Option(d.getString("name")).getOrElse(""),
      activeComponents = Option(d.getList("activeComponents", classOf[String]))
        .map(_.asScala.toList).getOrElse(Nil),
      currency = Option(d.getString("currency"))
    )

object MongoTenantRepository:

  val live: ZLayer[MongoDatabase, Nothing, TenantRepository] =
    ZLayer.fromFunction(new MongoTenantRepository(_))

// ── MongoTagRepository ────────────────────────────────────────────────────

final class MongoTagRepository(db: MongoDatabase) extends TagRepository:

  private def col(tenantId: TenantId) =
    TenantCollection[Document]("tags", tenantId, db)

  override def findById(tenantId: TenantId, tagId: String): Task[Option[AuthTag]] =
    col(tenantId).findOne(equal("_id", tagId)).map(_.map(docToTag))

  override def findActiveByUserId(tenantId: TenantId, userId: String): Task[List[AuthTag]] =
    ZIO.fromFuture { _ =>
      db.getCollection[Document](s"${tenantId.value}.tags")
        .find(and(equal("userID", userId), equal("active", true)))
        .toFuture()
    }.map(_.map(docToTag).toList)

  private def docToTag(d: Document): AuthTag =
    AuthTag(
      id = d.getString("_id"),
      userID = Option(d.getString("userID")),
      active = Option(d.getBoolean("active")).exists(_.booleanValue),
      issuer = Option(d.getBoolean("issuer")).forall(_.booleanValue)
    )

object MongoTagRepository:

  val live: ZLayer[MongoDatabase, Nothing, TagRepository] =
    ZLayer.fromFunction(new MongoTagRepository(_))

// ── MongoUserSiteRepository ───────────────────────────────────────────────

final class MongoUserSiteRepository(db: MongoDatabase) extends UserSiteRepository:

  override def findByUserId(tenantId: TenantId, userId: String): Task[List[UserSite]] =
    ZIO.fromFuture { _ =>
      db.getCollection[Document](s"${tenantId.value}.userssites")
        .find(equal("userID", userId))
        .toFuture()
    }.map(_.map(docToUserSite).toList)

  private def docToUserSite(d: Document): UserSite =
    UserSite(
      userID = d.getString("userID"),
      siteID = d.getString("siteID"),
      siteAdmin = Option(d.getBoolean("siteAdmin")).exists(_.booleanValue),
      siteOwner = Option(d.getBoolean("siteOwner")).exists(_.booleanValue)
    )

object MongoUserSiteRepository:

  val live: ZLayer[MongoDatabase, Nothing, UserSiteRepository] =
    ZLayer.fromFunction(new MongoUserSiteRepository(_))
