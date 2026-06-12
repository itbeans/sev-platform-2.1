package io.itbeans.ev.mongo

import io.itbeans.ev.domain._
import io.itbeans.ev.mongo.DomainBsonCodecs.{*, given}
import org.bson.Document
import org.mongodb.scala.MongoDatabase
import org.mongodb.scala.model.{Filters, ReplaceOptions}
import zio._
import zio.stream.ZStream

// ---------------------------------------------------------------------------
// BillingRepositories — Invoice, BillingAccount, BillingTransfer, BillingUser.
// ---------------------------------------------------------------------------

trait InvoiceRepository:
  def findById(tenantId: TenantId, id: InvoiceId): Task[Option[Invoice]]
  def findByUser(tenantId: TenantId, userId: UserId): ZStream[Any, Throwable, Invoice]
  def findAll(tenantId: TenantId): ZStream[Any, Throwable, Invoice]
  def save(invoice: Invoice): Task[Unit]
  def update(invoice: Invoice): Task[Unit]
  def delete(tenantId: TenantId, id: InvoiceId): Task[Unit]

final class MongoInvoiceRepository(db: MongoDatabase) extends InvoiceRepository:

  private def col(tenantId: TenantId) =
    TenantCollection[Document]("invoices", tenantId, db)

  def findById(tenantId: TenantId, id: InvoiceId): Task[Option[Invoice]] =
    col(tenantId).findOne(Filters.eq("_id", id.value)).flatMap {
      case Some(doc) => decodeDocZIO[Invoice](doc).map(Some(_))
      case None      => ZIO.none
    }

  def findByUser(tenantId: TenantId, userId: UserId): ZStream[Any, Throwable, Invoice] =
    col(tenantId).find(Filters.eq("userId", userId.value)).mapZIO(decodeDocZIO[Invoice])

  def findAll(tenantId: TenantId): ZStream[Any, Throwable, Invoice] =
    col(tenantId).find(Filters.empty()).mapZIO(decodeDocZIO[Invoice])

  def save(invoice: Invoice): Task[Unit] =
    col(invoice.tenantId).insertOne(encodeDoc(invoice))

  def update(invoice: Invoice): Task[Unit] =
    col(invoice.tenantId).replaceOne(Filters.eq("_id", invoice.id.value), encodeDoc(invoice))

  def delete(tenantId: TenantId, id: InvoiceId): Task[Unit] =
    col(tenantId).deleteOne(Filters.eq("_id", id.value))

object MongoInvoiceRepository:

  val live: ZLayer[MongoDatabase, Nothing, InvoiceRepository] =
    ZLayer.fromFunction(MongoInvoiceRepository(_))

// ---------------------------------------------------------------------------

trait BillingAccountRepository:
  def findById(tenantId: TenantId, id: String): Task[Option[BillingAccount]]
  def findByUser(tenantId: TenantId, userId: UserId): Task[Option[BillingAccount]]
  def save(account: BillingAccount): Task[Unit]
  def update(account: BillingAccount): Task[Unit]

final class MongoBillingAccountRepository(db: MongoDatabase) extends BillingAccountRepository:

  private def col(tenantId: TenantId) =
    TenantCollection[Document]("billingaccounts", tenantId, db)

  def findById(tenantId: TenantId, id: String): Task[Option[BillingAccount]] =
    col(tenantId).findOne(Filters.eq("_id", id)).flatMap {
      case Some(doc) => decodeDocZIO[BillingAccount](doc).map(Some(_))
      case None      => ZIO.none
    }

  def findByUser(tenantId: TenantId, userId: UserId): Task[Option[BillingAccount]] =
    col(tenantId).findOne(Filters.eq("businessOwnerUserId", userId.value)).flatMap {
      case Some(doc) => decodeDocZIO[BillingAccount](doc).map(Some(_))
      case None      => ZIO.none
    }

  def save(account: BillingAccount): Task[Unit] =
    col(account.tenantId).insertOne(encodeDoc(account))

  def update(account: BillingAccount): Task[Unit] =
    col(account.tenantId).replaceOne(Filters.eq("_id", account.id), encodeDoc(account))

object MongoBillingAccountRepository:

  val live: ZLayer[MongoDatabase, Nothing, BillingAccountRepository] =
    ZLayer.fromFunction(MongoBillingAccountRepository(_))

// ---------------------------------------------------------------------------

trait BillingTransferRepository:
  def findById(tenantId: TenantId, id: String): Task[Option[BillingTransfer]]
  def findByAccount(tenantId: TenantId, accountId: String): ZStream[Any, Throwable, BillingTransfer]
  def save(transfer: BillingTransfer): Task[Unit]
  def update(transfer: BillingTransfer): Task[Unit]

final class MongoBillingTransferRepository(db: MongoDatabase) extends BillingTransferRepository:

  private def col(tenantId: TenantId) =
    TenantCollection[Document]("billingtransfers", tenantId, db)

  def findById(tenantId: TenantId, id: String): Task[Option[BillingTransfer]] =
    col(tenantId).findOne(Filters.eq("_id", id)).flatMap {
      case Some(doc) => decodeDocZIO[BillingTransfer](doc).map(Some(_))
      case None      => ZIO.none
    }

  def findByAccount(tenantId: TenantId, accountId: String): ZStream[Any, Throwable, BillingTransfer] =
    col(tenantId).find(Filters.eq("accountId", accountId)).mapZIO(decodeDocZIO[BillingTransfer])

  def save(transfer: BillingTransfer): Task[Unit] =
    col(transfer.tenantId).insertOne(encodeDoc(transfer))

  def update(transfer: BillingTransfer): Task[Unit] =
    col(transfer.tenantId).replaceOne(Filters.eq("_id", transfer.id), encodeDoc(transfer))

object MongoBillingTransferRepository:

  val live: ZLayer[MongoDatabase, Nothing, BillingTransferRepository] =
    ZLayer.fromFunction(MongoBillingTransferRepository(_))

// ---------------------------------------------------------------------------

trait BillingUserRepository:
  def findByUserId(tenantId: TenantId, userId: UserId): Task[Option[BillingUser]]
  def upsert(bu: BillingUser): Task[Unit]

final class MongoBillingUserRepository(db: MongoDatabase) extends BillingUserRepository:

  private def col(tenantId: TenantId) =
    TenantCollection[Document]("billingusers", tenantId, db)

  def findByUserId(tenantId: TenantId, userId: UserId): Task[Option[BillingUser]] =
    col(tenantId).findOne(Filters.eq("userId", userId.value)).flatMap {
      case Some(doc) => decodeDocZIO[BillingUser](doc).map(Some(_))
      case None      => ZIO.none
    }

  def upsert(bu: BillingUser): Task[Unit] =
    col(bu.tenantId).replaceOne(
      Filters.eq("userId", bu.userId.value),
      encodeDoc(bu),
      upsert = true
    )

object MongoBillingUserRepository:

  val live: ZLayer[MongoDatabase, Nothing, BillingUserRepository] =
    ZLayer.fromFunction(MongoBillingUserRepository(_))
