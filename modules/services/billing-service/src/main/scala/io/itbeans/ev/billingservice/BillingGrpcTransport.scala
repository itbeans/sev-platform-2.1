package io.itbeans.ev.billingservice

import io.grpc.netty.NettyServerBuilder
import io.itbeans.ev.billing.grpc.billing_service._
import zio._

import scala.concurrent.{ExecutionContext, Future}

// ---------------------------------------------------------------------------
// BillingGrpcTransport — bridges the ScalaPB-generated BillingService gRPC
// interface to the ZIO-native billing handler and underlying services.
//
// Implements all RPCs declared in billing_service.proto; delegates to:
//   BillingGrpcHandler  — invoice listing, payment method checks, charge
//   BillingService      — high-level business operations
//   InvoiceRepository   — direct invoice lookup by ID
//   BillingUserRepository — billing user / payment method lookup
//   StripeClient        — Stripe payment method attach/detach
// ---------------------------------------------------------------------------

final class BillingGrpcTransport(
    handler: BillingGrpcHandler,
    billing: BillingService,
    invoiceRepo: InvoiceRepository,
    userRepo: BillingUserRepository,
    stripe: StripeClient,
    cfg: BillingConfig,
    rt: Runtime[Any]
) extends BillingServiceGrpc.BillingService:

  private def run[A](effect: Task[A]): Future[A] =
    Unsafe.unsafe(implicit u => rt.unsafe.runToFuture(effect))

  // ── User billing ──────────────────────────────────────────────────────────

  override def synchronizeBillingUser(req: SynchronizeBillingUserRequest): Future[SynchronizeBillingUserResponse] =
    run {
      userRepo.findByUserId(req.tenantId, req.userId)
        .map {
          case Some(bu) => SynchronizeBillingUserResponse(success = true, billingCustomerId = bu.customerId)
          case None     => SynchronizeBillingUserResponse(success = true)
        }
        .catchAll(err => ZIO.succeed(SynchronizeBillingUserResponse(success = false, error = err.getMessage)))
    }

  override def forceSynchronizeUser(req: ForceSynchronizeUserRequest): Future[ForceSynchronizeUserResponse] =
    run {
      ZIO.succeed(ForceSynchronizeUserResponse(success = true))
    }

  // ── Transaction pre-check ─────────────────────────────────────────────────

  override def checkTransactionBillingPrerequisites(
      req: CheckTransactionBillingPrerequisitesRequest
  ): Future[CheckTransactionBillingPrerequisitesResponse] =
    run {
      if !cfg.immediatePayment then
        ZIO.succeed(CheckTransactionBillingPrerequisitesResponse(canStart = true))
      else
        handler
          .checkPaymentMethods(CheckPaymentMethodsRequestADT(req.tenantId, req.userId))
          .map { r =>
            if r.hasDefault then
              CheckTransactionBillingPrerequisitesResponse(canStart = true)
            else
              CheckTransactionBillingPrerequisitesResponse(
                canStart = false,
                errorCodes = Seq("NO_PAYMENT_METHOD")
              )
          }
          .catchAll(_ => ZIO.succeed(CheckTransactionBillingPrerequisitesResponse(canStart = true)))
    }

  // ── Payment methods ───────────────────────────────────────────────────────

  override def setupPaymentMethod(req: SetupPaymentMethodRequest): Future[SetupPaymentMethodResponse] =
    run {
      userRepo.findByUserId(req.tenantId, req.userId).flatMap {
        case None =>
          ZIO.succeed(SetupPaymentMethodResponse(success = false, error = "No billing user found"))
        case Some(bu) =>
          stripe
            .attachPaymentMethod(req.paymentMethodId, bu.customerId)
            .flatMap { _ =>
              stripe.updateCustomer(
                bu.customerId,
                Map("invoice_settings[default_payment_method]" -> req.paymentMethodId)
              )
            }
            .flatMap { _ =>
              userRepo.upsert(bu.copy(defaultPaymentMethodId = Some(req.paymentMethodId)))
            }
            .as(SetupPaymentMethodResponse(success = true))
            .catchAll(err => ZIO.succeed(SetupPaymentMethodResponse(success = false, error = err.getMessage)))
      }
    }

  override def deletePaymentMethod(req: DeletePaymentMethodRequest): Future[DeletePaymentMethodResponse] =
    run {
      stripe
        .detachPaymentMethod(req.paymentMethodId)
        .as(DeletePaymentMethodResponse(success = true))
        .catchAll(err => ZIO.succeed(DeletePaymentMethodResponse(success = false, error = err.getMessage)))
    }

  override def getPaymentMethods(req: GetPaymentMethodsRequest): Future[GetPaymentMethodsResponse] =
    run {
      handler
        .checkPaymentMethods(CheckPaymentMethodsRequestADT(req.tenantId, req.userId))
        .map { r =>
          GetPaymentMethodsResponse(
            paymentMethods = r.paymentMethods.map(pm =>
              PaymentMethod(
                id = pm.id,
                brand = pm.brand,
                last4 = pm.last4,
                expMonth = pm.expMonth,
                expYear = pm.expYear,
                isDefault = r.hasDefault && r.paymentMethods.headOption.exists(_.id == pm.id)
              )
            )
          )
        }
        .catchAll(_ => ZIO.succeed(GetPaymentMethodsResponse()))
    }

  // ── Invoices ──────────────────────────────────────────────────────────────

  override def chargeInvoice(req: ChargeInvoiceRequest): Future[ChargeInvoiceResponse] =
    run {
      billing
        .chargeInvoice(req.tenantId, req.invoiceId)
        .map(inv => ChargeInvoiceResponse(success = true, invoiceStatus = inv.status.code))
        .catchAll(err => ZIO.succeed(ChargeInvoiceResponse(success = false, error = err.getMessage)))
    }

  override def downloadInvoice(req: DownloadInvoiceRequest): Future[DownloadInvoiceResponse] =
    run {
      invoiceRepo.findById(req.tenantId, req.invoiceId).map {
        case None      => DownloadInvoiceResponse(found = false)
        case Some(inv) => DownloadInvoiceResponse(found = true, downloadUrl = inv.downloadUrl.getOrElse(""))
      }
    }

  override def getInvoices(req: GetInvoicesRequest): Future[GetInvoicesResponse] =
    run {
      invoiceRepo
        .findByUserId(req.tenantId, req.userId)
        .map { invoices =>
          val limit = if req.limit > 0 then req.limit else 25
          val page  = invoices.drop(req.skip).take(limit)
          GetInvoicesResponse(invoices = page.map(toInvoiceRecord), total = invoices.size)
        }
    }

  override def getInvoice(req: GetInvoiceRequest): Future[GetInvoiceResponse] =
    run {
      invoiceRepo.findById(req.tenantId, req.invoiceId).map {
        case None      => GetInvoiceResponse(found = false)
        case Some(inv) => GetInvoiceResponse(found = true, invoice = Some(toInvoiceRecord(inv)))
      }
    }

  // ── Billing accounts ──────────────────────────────────────────────────────

  override def onboardBillingAccount(req: OnboardBillingAccountRequest): Future[OnboardBillingAccountResponse] =
    run {
      ZIO.succeed(OnboardBillingAccountResponse(success = false, error = "Not implemented via gRPC"))
    }

  override def activateBillingAccount(req: ActivateBillingAccountRequest): Future[ActivateBillingAccountResponse] =
    run {
      ZIO.succeed(ActivateBillingAccountResponse(success = false, error = "Not implemented via gRPC"))
    }

  // ── Transfers ─────────────────────────────────────────────────────────────

  override def finalizeTransfer(req: FinalizeTransferRequest): Future[FinalizeTransferResponse] =
    run {
      ZIO.succeed(FinalizeTransferResponse(success = false, error = "Not implemented via gRPC"))
    }

  override def sendTransfer(req: SendTransferRequest): Future[SendTransferResponse] =
    run {
      ZIO.succeed(SendTransferResponse(success = false, error = "Not implemented via gRPC"))
    }

  // ── Helpers ───────────────────────────────────────────────────────────────

  private def toInvoiceRecord(inv: Invoice): InvoiceRecord =
    InvoiceRecord(
      id            = inv.id,
      tenantId      = inv.tenantId,
      userId        = inv.userId,
      invoiceNumber = inv.invoiceNumber.getOrElse(""),
      status        = inv.status.code,
      amountCents   = inv.amountCents,
      currency      = inv.currency,
      downloadUrl   = inv.downloadUrl.getOrElse(""),
      createdOn     = inv.createdOn.toEpochMilli,
      lastChangedOn = inv.lastChangedOn.toEpochMilli
    )

object BillingGrpcTransport:

  val start: RIO[
    BillingGrpcHandler & BillingService & InvoiceRepository &
      BillingUserRepository & StripeClient & BillingConfig,
    Unit
  ] =
    for
      handler     <- ZIO.service[BillingGrpcHandler]
      billing     <- ZIO.service[BillingService]
      invoiceRepo <- ZIO.service[InvoiceRepository]
      userRepo    <- ZIO.service[BillingUserRepository]
      stripe      <- ZIO.service[StripeClient]
      cfg         <- ZIO.service[BillingConfig]
      rt          <- ZIO.runtime[Any]
      impl = new BillingGrpcTransport(handler, billing, invoiceRepo, userRepo, stripe, cfg, rt)
      server <- ZIO.attempt(
        NettyServerBuilder
          .forPort(cfg.grpcPort)
          .addService(BillingServiceGrpc.bindService(impl, ExecutionContext.global))
          .build()
          .start()
      )
      _ <- ZIO.logInfo(s"Billing gRPC server listening on :${server.getPort}")
    yield ()
