package io.itbeans.ev.billingservice

import cats.effect.unsafe.implicits.global // IORuntime for unsafeRunSync
import doobie._
import doobie.implicits._
import io.circe.parser.decode
import io.circe.syntax._
import zio._

import java.time.Instant

// ---------------------------------------------------------------------------
// BillingRepository — Doobie/PostgreSQL repositories for billing data.
//
// ZIO ↔ cats-effect bridge strategy:
//   We run cats.effect.IO synchronously inside ZIO.attemptBlocking, which
//   routes the work to ZIO's blocking thread pool. This avoids a runtime
//   interop dependency (zio-interop-catz) while keeping the code simple.
//
// Schema: see resources/db/migration/V1__billing_schema.sql
// ---------------------------------------------------------------------------

// ── Meta instances for custom types ──────────────────────────────────────

given Meta[InvoiceStatus] =
  Meta[String].imap(s => InvoiceStatus.fromCode(s).getOrElse(InvoiceStatus.Draft))(_.code)

given Meta[BillingAccountStatus] =
  Meta[String].imap(s => BillingAccountStatus.fromCode(s).getOrElse(BillingAccountStatus.Idle))(_.code)

given Meta[TransferStatus] =
  Meta[String].imap(s => TransferStatus.fromCode(s).getOrElse(TransferStatus.Draft))(_.code)

given Meta[Instant] =
  Meta[Long].imap(Instant.ofEpochMilli(_))(_.toEpochMilli)

given Meta[List[BillingSessionData]] =
  Meta[String].imap(s =>
    io.circe.parser.decode[List[BillingSessionData]](s).getOrElse(Nil)
  )(sessions => io.circe.syntax.EncoderOps(sessions).asJson.noSpaces)

// ── IO → Task bridge ──────────────────────────────────────────────────────

private def runIO[A](io: cats.effect.IO[A]): Task[A] =
  ZIO.attemptBlocking(io.unsafeRunSync())

// ── Repository traits ─────────────────────────────────────────────────────

trait InvoiceRepository:
  def create(invoice: Invoice): Task[Unit]
  def findById(tenantId: String, id: String): Task[Option[Invoice]]
  def findByUserId(tenantId: String, userId: String): Task[List[Invoice]]
  def findByStatus(tenantId: String, status: InvoiceStatus): Task[List[Invoice]]
  def findDraftsBefore(tenantId: String, beforeEpochMs: Long): Task[List[Invoice]]
  def update(invoice: Invoice): Task[Unit]

trait BillingAccountRepository:
  def create(account: BillingAccount): Task[Unit]
  def findById(tenantId: String, id: String): Task[Option[BillingAccount]]
  def findByOwner(tenantId: String, userId: String): Task[Option[BillingAccount]]
  def findAll(tenantId: String): Task[List[BillingAccount]]
  def update(account: BillingAccount): Task[Unit]

trait BillingTransferRepository:
  def create(transfer: BillingTransfer): Task[Unit]
  def findById(tenantId: String, id: String): Task[Option[BillingTransfer]]
  def findDraftByAccount(tenantId: String, accountId: String, currency: String): Task[Option[BillingTransfer]]
  def findAll(tenantId: String): Task[List[BillingTransfer]]
  def update(transfer: BillingTransfer): Task[Unit]

trait BillingUserRepository:
  def findByUserId(tenantId: String, userId: String): Task[Option[BillingUser]]
  def upsert(user: BillingUser): Task[Unit]

// ── PostgreSQL implementations ────────────────────────────────────────────

final class PostgresInvoiceRepository(xa: Transactor[cats.effect.IO]) extends InvoiceRepository:

  def create(inv: Invoice): Task[Unit] = runIO(
    sql"""INSERT INTO billing_invoices
            (id, tenant_id, invoice_id, invoice_number, status, amount_cents,
             amount_paid_cents, currency, user_id, customer_id, live_mode,
             sessions_json, download_url, pay_invoice_url, last_error, created_on, last_changed_on)
          VALUES
            (${inv.id}, ${inv.tenantId}, ${inv.invoiceId}, ${inv.invoiceNumber}, ${inv.status},
             ${inv.amountCents}, ${inv.amountPaidCents}, ${inv.currency}, ${inv.userId},
             ${inv.customerId}, ${inv.liveMode}, ${inv.sessions},
             ${inv.downloadUrl}, ${inv.payInvoiceUrl}, ${inv.lastError},
             ${inv.createdOn}, ${inv.lastChangedOn})
       """.update.run.transact(xa).map(_ => ())
  )

  def findById(tenantId: String, id: String): Task[Option[Invoice]] = runIO(
    sql"""SELECT id, tenant_id, invoice_id, invoice_number, status, amount_cents,
                 amount_paid_cents, currency, user_id, customer_id, live_mode,
                 sessions_json, download_url, pay_invoice_url, last_error, created_on, last_changed_on
          FROM billing_invoices
          WHERE tenant_id = $tenantId AND id = $id"""
      .query[Invoice].option.transact(xa)
  )

  def findByUserId(tenantId: String, userId: String): Task[List[Invoice]] = runIO(
    sql"""SELECT id, tenant_id, invoice_id, invoice_number, status, amount_cents,
                 amount_paid_cents, currency, user_id, customer_id, live_mode,
                 sessions_json, download_url, pay_invoice_url, last_error, created_on, last_changed_on
          FROM billing_invoices
          WHERE tenant_id = $tenantId AND user_id = $userId
          ORDER BY created_on DESC"""
      .query[Invoice].to[List].transact(xa)
  )

  def findByStatus(tenantId: String, status: InvoiceStatus): Task[List[Invoice]] = runIO(
    sql"""SELECT id, tenant_id, invoice_id, invoice_number, status, amount_cents,
                 amount_paid_cents, currency, user_id, customer_id, live_mode,
                 sessions_json, download_url, pay_invoice_url, last_error, created_on, last_changed_on
          FROM billing_invoices
          WHERE tenant_id = $tenantId AND status = ${status.code}
          ORDER BY created_on DESC"""
      .query[Invoice].to[List].transact(xa)
  )

  def findDraftsBefore(tenantId: String, beforeEpochMs: Long): Task[List[Invoice]] = runIO(
    sql"""SELECT id, tenant_id, invoice_id, invoice_number, status, amount_cents,
                 amount_paid_cents, currency, user_id, customer_id, live_mode,
                 sessions_json, download_url, pay_invoice_url, last_error, created_on, last_changed_on
          FROM billing_invoices
          WHERE tenant_id = $tenantId
            AND status = 'draft'
            AND created_on < $beforeEpochMs
          ORDER BY created_on"""
      .query[Invoice].to[List].transact(xa)
  )

  def update(inv: Invoice): Task[Unit] = runIO(
    sql"""UPDATE billing_invoices SET
            invoice_number    = ${inv.invoiceNumber},
            status            = ${inv.status},
            amount_cents      = ${inv.amountCents},
            amount_paid_cents = ${inv.amountPaidCents},
            sessions_json     = ${inv.sessions},
            download_url      = ${inv.downloadUrl},
            pay_invoice_url   = ${inv.payInvoiceUrl},
            last_error        = ${inv.lastError},
            last_changed_on   = ${inv.lastChangedOn}
          WHERE tenant_id = ${inv.tenantId} AND id = ${inv.id}
       """.update.run.transact(xa).map(_ => ())
  )

// ─────────────────────────────────────────────────────────────────────────

final class PostgresBillingAccountRepository(xa: Transactor[cats.effect.IO]) extends BillingAccountRepository:

  def create(acc: BillingAccount): Task[Unit] = runIO(
    sql"""INSERT INTO billing_accounts
            (id, tenant_id, business_owner_user_id, company_name, status,
             account_external_id, activation_link, created_on, created_by)
          VALUES
            (${acc.id}, ${acc.tenantId}, ${acc.businessOwnerUserId}, ${acc.companyName},
             ${acc.status}, ${acc.accountExternalId}, ${acc.activationLink},
             ${acc.createdOn}, ${acc.createdBy})
       """.update.run.transact(xa).map(_ => ())
  )

  def findById(tenantId: String, id: String): Task[Option[BillingAccount]] = runIO(
    sql"""SELECT id, tenant_id, business_owner_user_id, company_name, status,
                 account_external_id, activation_link, created_on, created_by
          FROM billing_accounts WHERE tenant_id = $tenantId AND id = $id"""
      .query[BillingAccount].option.transact(xa)
  )

  def findByOwner(tenantId: String, userId: String): Task[Option[BillingAccount]] = runIO(
    sql"""SELECT id, tenant_id, business_owner_user_id, company_name, status,
                 account_external_id, activation_link, created_on, created_by
          FROM billing_accounts WHERE tenant_id = $tenantId AND business_owner_user_id = $userId"""
      .query[BillingAccount].option.transact(xa)
  )

  def findAll(tenantId: String): Task[List[BillingAccount]] = runIO(
    sql"""SELECT id, tenant_id, business_owner_user_id, company_name, status,
                 account_external_id, activation_link, created_on, created_by
          FROM billing_accounts WHERE tenant_id = $tenantId ORDER BY created_on DESC"""
      .query[BillingAccount].to[List].transact(xa)
  )

  def update(acc: BillingAccount): Task[Unit] = runIO(
    sql"""UPDATE billing_accounts SET
            status              = ${acc.status},
            account_external_id = ${acc.accountExternalId},
            activation_link     = ${acc.activationLink}
          WHERE tenant_id = ${acc.tenantId} AND id = ${acc.id}
       """.update.run.transact(xa).map(_ => ())
  )

// ─────────────────────────────────────────────────────────────────────────

final class PostgresBillingTransferRepository(xa: Transactor[cats.effect.IO]) extends BillingTransferRepository:

  def create(t: BillingTransfer): Task[Unit] = runIO(
    sql"""INSERT INTO billing_transfers
            (id, tenant_id, account_id, account_external_id, status, currency,
             session_counter, collected_funds_cents, collected_fees_cents,
             transfer_amount_cents, transfer_external_id, created_on, last_changed_on)
          VALUES
            (${t.id}, ${t.tenantId}, ${t.accountId}, ${t.accountExternalId}, ${t.status},
             ${t.currency}, ${t.sessionCounter}, ${t.collectedFundsCents}, ${t.collectedFeesCents},
             ${t.transferAmountCents}, ${t.transferExternalId}, ${t.createdOn}, ${t.lastChangedOn})
       """.update.run.transact(xa).map(_ => ())
  )

  def findById(tenantId: String, id: String): Task[Option[BillingTransfer]] = runIO(
    sql"""SELECT id, tenant_id, account_id, account_external_id, status, currency,
                 session_counter, collected_funds_cents, collected_fees_cents,
                 transfer_amount_cents, transfer_external_id, created_on, last_changed_on
          FROM billing_transfers WHERE tenant_id = $tenantId AND id = $id"""
      .query[BillingTransfer].option.transact(xa)
  )

  def findDraftByAccount(tenantId: String, accountId: String, currency: String): Task[Option[BillingTransfer]] = runIO(
    sql"""SELECT id, tenant_id, account_id, account_external_id, status, currency,
                 session_counter, collected_funds_cents, collected_fees_cents,
                 transfer_amount_cents, transfer_external_id, created_on, last_changed_on
          FROM billing_transfers
          WHERE tenant_id = $tenantId AND account_id = $accountId
            AND currency = $currency AND status = 'draft'
          LIMIT 1"""
      .query[BillingTransfer].option.transact(xa)
  )

  def findAll(tenantId: String): Task[List[BillingTransfer]] = runIO(
    sql"""SELECT id, tenant_id, account_id, account_external_id, status, currency,
                 session_counter, collected_funds_cents, collected_fees_cents,
                 transfer_amount_cents, transfer_external_id, created_on, last_changed_on
          FROM billing_transfers WHERE tenant_id = $tenantId ORDER BY created_on DESC"""
      .query[BillingTransfer].to[List].transact(xa)
  )

  def update(t: BillingTransfer): Task[Unit] = runIO(
    sql"""UPDATE billing_transfers SET
            status                = ${t.status},
            session_counter       = ${t.sessionCounter},
            collected_funds_cents  = ${t.collectedFundsCents},
            collected_fees_cents   = ${t.collectedFeesCents},
            transfer_amount_cents  = ${t.transferAmountCents},
            transfer_external_id   = ${t.transferExternalId},
            last_changed_on        = ${t.lastChangedOn}
          WHERE tenant_id = ${t.tenantId} AND id = ${t.id}
       """.update.run.transact(xa).map(_ => ())
  )

// ─────────────────────────────────────────────────────────────────────────

final class PostgresBillingUserRepository(xa: Transactor[cats.effect.IO]) extends BillingUserRepository:

  def findByUserId(tenantId: String, userId: String): Task[Option[BillingUser]] = runIO(
    sql"""SELECT user_id, tenant_id, customer_id, default_payment_method_id, created_on
          FROM billing_users WHERE tenant_id = $tenantId AND user_id = $userId"""
      .query[BillingUser].option.transact(xa)
  )

  def upsert(u: BillingUser): Task[Unit] = runIO(
    sql"""INSERT INTO billing_users (user_id, tenant_id, customer_id, default_payment_method_id, created_on)
          VALUES (${u.userId}, ${u.tenantId}, ${u.customerId}, ${u.defaultPaymentMethodId}, ${u.createdOn})
          ON CONFLICT (tenant_id, user_id) DO UPDATE SET
            customer_id               = EXCLUDED.customer_id,
            default_payment_method_id = EXCLUDED.default_payment_method_id
       """.update.run.transact(xa).map(_ => ())
  )

// ── Transactor layer ──────────────────────────────────────────────────────

object BillingRepository:

  private def transactor(cfg: BillingConfig): Transactor[cats.effect.IO] =
    Transactor.fromDriverManager[cats.effect.IO](
      driver = "org.postgresql.Driver",
      url = cfg.jdbcUrl,
      user = cfg.jdbcUser,
      password = cfg.jdbcPassword,
      logHandler = None
    )

  val invoiceRepoLive: ZLayer[BillingConfig, Nothing, InvoiceRepository] =
    ZLayer.fromFunction((cfg: BillingConfig) => new PostgresInvoiceRepository(transactor(cfg)))

  val accountRepoLive: ZLayer[BillingConfig, Nothing, BillingAccountRepository] =
    ZLayer.fromFunction((cfg: BillingConfig) => new PostgresBillingAccountRepository(transactor(cfg)))

  val transferRepoLive: ZLayer[BillingConfig, Nothing, BillingTransferRepository] =
    ZLayer.fromFunction((cfg: BillingConfig) => new PostgresBillingTransferRepository(transactor(cfg)))

  val userRepoLive: ZLayer[BillingConfig, Nothing, BillingUserRepository] =
    ZLayer.fromFunction((cfg: BillingConfig) => new PostgresBillingUserRepository(transactor(cfg)))
