package io.itbeans.ev.billingservice

import io.grpc.netty.NettyServerBuilder
import io.itbeans.ev.billing.grpc.billing_service._
import io.itbeans.ev.otel.EvTracing
import zio._

import scala.concurrent.{ExecutionContext, Future}

// ---------------------------------------------------------------------------
// BillingGrpcTransport — bridges the ScalaPB-generated BillingService gRPC
// interface to the ZIO-native billing handler and underlying services.
// ---------------------------------------------------------------------------

final class BillingGrpcTransport(
    handler: BillingGrpcHandler,
    billing: BillingService,
    invoiceRepo: InvoiceRepository,
    userRepo: BillingUserRepository,
    stripe: StripeClient,
    cfg: BillingConfig,
    tracing: EvTracing,
    rt: Runtime[Any]
) extends BillingServiceGrpc.BillingService:

  private def run[A](spanName: String)(effect: Task[A]): Future[A] =
    Unsafe.unsafe(implicit u => rt.unsafe.runToFuture(tracing.spanTask(spanName)(effect)))

  override def synchronizeBillingUser(req: SynchronizeBillingUserRequest): Future[SynchronizeBillingUserResponse] =
    run("billing.synchronizeBillingUser") {
      userRepo.findByUserId(req.tenantId, req.userId)
        .map {
          case Some(bu) => SynchronizeBillingUserResponse(success = true, billingCustomerId = bu.customerId)
          case None     => SynchronizeBillingUserResponse(success = true)
        }
        .catchAll(err => ZIO.succeed(SynchronizeBillingUserResponse(success = false, error = err.getMessage)))
    }

  override def forceSynchronizeUser(req: ForceSynchronizeUserRequest): Future[ForceSynchronizeUserResponse] =
    run("billing.forceSynchronizeUser") {
      ZIO.succeed(ForceSynchronizeUserResponse(success = true))
    }

  override def checkTransactionBillingPrerequisites(
      req: CheckTransactionBillingPrerequisitesRequest
  ): Future[CheckTransactionBillingPrerequisitesResponse] =
    run("billing.checkTransactionBillingPrerequisites") {
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

  override def setupPaymentMethod(req: SetupPaymentMethodRequest): Future[SetupPaymentMethodResponse] =
    run("billing.setupPaymentMethod") {
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
    run("billing.deletePaymentMethod") {
      stripe
        .detachPaymentMethod(req.paymentMethodId)
        .as(DeletePaymentMethodResponse(success = true))
        .catchAll(err => ZIO.succeed(DeletePaymentMethodResponse(success = false, error = err.getMessage)))
    }

  override def getPaymentMethods(req: GetPaymentMethodsRequest): Future[GetPaymentMethodsResponse] =
    run("billing.getPaymentMethods") {
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

  override def chargeInvoice(req: ChargeInvoiceRequest): Future[ChargeInvoiceResponse] =
    run("billing.chargeInvoice") {
      billing
        .chargeInvoice(req.tenantId, req.invoiceId)
        .map(inv => ChargeInvoiceResponse(success = true, invoiceStatus = inv.status.code))
        .catchAll(err => ZIO.succeed(ChargeInvoiceResponse(success = false, error = err.getMessage)))
    }

  override def downloadInvoice(req: DownloadInvoiceRequest): Future[DownloadInvoiceResponse] =
    run("billing.downloadInvoice") {
      invoiceRepo.findById(req.tenantId, req.invoiceId).map {
        case None      => DownloadInvoiceResponse(found = false)
        case Some(inv) => DownloadInvoiceResponse(found = true, downloadUrl = inv.downloadUrl.getOrElse(""))
      }
    }

  override def getInvoices(req: GetInvoicesRequest): Future[GetInvoicesResponse] =
    run("billing.getInvoices") {
      invoiceRepo
        .findByUserId(req.tenantId, req.userId)
        .map { invoices =>
          val limit = if req.limit > 0 then req.limit else 25
          val page  = invoices.drop(req.skip).take(limit)
          GetInvoicesResponse(invoices = page.map(toInvoiceRecord), total = invoices.size)
        }
    }

  override def getInvoice(req: GetInvoiceRequest): Future[GetInvoiceResponse] =
    run("billing.getInvoice") {
      invoiceRepo.findById(req.tenantId, req.invoiceId).map {
        case None      => GetInvoiceResponse(found = false)
        case Some(inv) => GetInvoiceResponse(found = true, invoice = Some(toInvoiceRecord(inv)))
      }
    }

  override def onboardBillingAccount(req: OnboardBillingAccountRequest): Future[OnboardBillingAccountResponse] =
    run("billing.onboardBillingAccount") {
      ZIO.succeed(OnboardBillingAccountResponse(success = false, error = "Not implemented via gRPC"))
    }

  override def activateBillingAccount(req: ActivateBillingAccountRequest): Future[ActivateBillingAccountResponse] =
    run("billing.activateBillingAccount") {
      ZIO.succeed(ActivateBillingAccountResponse(success = false, error = "Not implemented via gRPC"))
    }

  override def finalizeTransfer(req: FinalizeTransferRequest): Future[FinalizeTransferResponse] =
    run("billing.finalizeTransfer") {
      ZIO.succeed(FinalizeTransferResponse(success = false, error = "Not implemented via gRPC"))
    }

  override def sendTransfer(req: SendTransferRequest): Future[SendTransferResponse] =
    run("billing.sendTransfer") {
      ZIO.succeed(SendTransferResponse(success = false, error = "Not implemented via gRPC"))
    }

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
      BillingUserRepository & StripeClient & BillingConfig & EvTracing,
    Unit
  ] =
    for
      handler     <- ZIO.service[BillingGrpcHandler]
      billing     <- ZIO.service[BillingService]
      invoiceRepo <- ZIO.service[InvoiceRepository]
      userRepo    <- ZIO.service[BillingUserRepository]
      stripe      <- ZIO.service[StripeClient]
      cfg         <- ZIO.service[BillingConfig]
      tracing     <- ZIO.service[EvTracing]
      rt          <- ZIO.runtime[Any]
      impl = new BillingGrpcTransport(handler, billing, invoiceRepo, userRepo, stripe, cfg, tracing, rt)
      server <- ZIO.attempt(
        NettyServerBuilder
          .forPort(cfg.grpcPort)
          .addService(BillingServiceGrpc.bindService(impl, ExecutionContext.global))
          .build()
          .start()
      )
      _ <- ZIO.logInfo(s"Billing gRPC server listening on :${server.getPort}")
    yield ()
