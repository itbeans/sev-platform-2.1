package io.itbeans.ev.mongo

import io.itbeans.ev.domain._
import io.itbeans.ev.mongo.DomainBsonCodecs.given
import org.bson.Document
import org.mongodb.scala.MongoDatabase
import org.mongodb.scala.model.{Filters, Sorts}
import zio._
import zio.stream.ZStream

// ---------------------------------------------------------------------------
// NotificationRepositories — Notification, NotificationPreferences,
//                            RegistrationToken, and TenantSettings.
// ---------------------------------------------------------------------------

trait NotificationRepository:
  def findById(tenantId: TenantId, id: NotificationId): Task[Option[Notification]]
  def findByUser(tenantId: TenantId, userId: UserId, limit: Int): ZStream[Any, Throwable, Notification]
  def save(n: Notification): Task[Unit]

final class MongoNotificationRepository(db: MongoDatabase) extends NotificationRepository:

  private def col(tenantId: TenantId) =
    TenantCollection[Document]("notifications", tenantId, db)

  def findById(tenantId: TenantId, id: NotificationId): Task[Option[Notification]] =
    col(tenantId).findOne(Filters.eq("_id", id.value)).flatMap {
      case Some(doc) => decodeDocZIO[Notification](doc).map(Some(_))
      case None      => ZIO.none
    }

  def findByUser(tenantId: TenantId, userId: UserId, limit: Int): ZStream[Any, Throwable, Notification] =
    col(tenantId)
      .find(Filters.eq("userId", userId.value), Sorts.descending("timestamp"), limit)
      .mapZIO(decodeDocZIO[Notification])

  def save(n: Notification): Task[Unit] =
    col(n.tenantId).insertOne(encodeDoc(n))

object MongoNotificationRepository:

  val live: ZLayer[MongoDatabase, Nothing, NotificationRepository] =
    ZLayer.fromFunction(MongoNotificationRepository(_))

// ---------------------------------------------------------------------------

trait NotificationPreferencesRepository:
  def findByUser(tenantId: TenantId, userId: UserId): Task[Option[NotificationPreferences]]
  def upsert(prefs: NotificationPreferences): Task[Unit]

final class MongoNotificationPreferencesRepository(db: MongoDatabase) extends NotificationPreferencesRepository:

  private def col(tenantId: TenantId) =
    TenantCollection[Document]("notificationpreferences", tenantId, db)

  def findByUser(tenantId: TenantId, userId: UserId): Task[Option[NotificationPreferences]] =
    col(tenantId).findOne(Filters.eq("userId", userId.value)).flatMap {
      case Some(doc) => decodeDocZIO[NotificationPreferences](doc).map(Some(_))
      case None      => ZIO.none
    }

  def upsert(prefs: NotificationPreferences): Task[Unit] =
    col(prefs.tenantId).replaceOne(
      Filters.eq("userId", prefs.userId.value),
      encodeDoc(prefs),
      upsert = true
    )

object MongoNotificationPreferencesRepository:

  val live: ZLayer[MongoDatabase, Nothing, NotificationPreferencesRepository] =
    ZLayer.fromFunction(MongoNotificationPreferencesRepository(_))

// ---------------------------------------------------------------------------

trait RegistrationTokenRepository:
  def findById(tenantId: TenantId, id: RegistrationTokenId): Task[Option[RegistrationToken]]
  def findAll(tenantId: TenantId): ZStream[Any, Throwable, RegistrationToken]
  def save(token: RegistrationToken): Task[Unit]
  def update(token: RegistrationToken): Task[Unit]
  def delete(tenantId: TenantId, id: RegistrationTokenId): Task[Unit]

final class MongoRegistrationTokenRepository(db: MongoDatabase) extends RegistrationTokenRepository:

  private def col(tenantId: TenantId) =
    TenantCollection[Document]("registrationtokens", tenantId, db)

  def findById(tenantId: TenantId, id: RegistrationTokenId): Task[Option[RegistrationToken]] =
    col(tenantId).findOne(Filters.eq("_id", id.value)).flatMap {
      case Some(doc) => decodeDocZIO[RegistrationToken](doc).map(Some(_))
      case None      => ZIO.none
    }

  def findAll(tenantId: TenantId): ZStream[Any, Throwable, RegistrationToken] =
    col(tenantId).find(Filters.empty()).mapZIO(decodeDocZIO[RegistrationToken])

  def save(token: RegistrationToken): Task[Unit] =
    col(token.tenantId).insertOne(encodeDoc(token))

  def update(token: RegistrationToken): Task[Unit] =
    col(token.tenantId).replaceOne(Filters.eq("_id", token.id.value), encodeDoc(token))

  def delete(tenantId: TenantId, id: RegistrationTokenId): Task[Unit] =
    col(tenantId).deleteOne(Filters.eq("_id", id.value))

object MongoRegistrationTokenRepository:

  val live: ZLayer[MongoDatabase, Nothing, RegistrationTokenRepository] =
    ZLayer.fromFunction(MongoRegistrationTokenRepository(_))

// ---------------------------------------------------------------------------

trait TenantSettingsRepository:
  def findByTenant(tenantId: TenantId): Task[Option[TenantSettings]]
  def upsert(settings: TenantSettings): Task[Unit]

final class MongoTenantSettingsRepository(db: MongoDatabase) extends TenantSettingsRepository:

  private def col(tenantId: TenantId) =
    TenantCollection[Document]("settings", tenantId, db)

  def findByTenant(tenantId: TenantId): Task[Option[TenantSettings]] =
    col(tenantId).findOne(Filters.eq("tenantId", tenantId.value)).flatMap {
      case Some(doc) => decodeDocZIO[TenantSettings](doc).map(Some(_))
      case None      => ZIO.none
    }

  def upsert(settings: TenantSettings): Task[Unit] =
    col(settings.tenantId).replaceOne(
      Filters.eq("tenantId", settings.tenantId.value),
      encodeDoc(settings),
      upsert = true
    )

object MongoTenantSettingsRepository:

  val live: ZLayer[MongoDatabase, Nothing, TenantSettingsRepository] =
    ZLayer.fromFunction(MongoTenantSettingsRepository(_))
