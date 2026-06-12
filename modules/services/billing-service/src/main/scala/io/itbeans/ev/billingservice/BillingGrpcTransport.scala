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
    accountRepo: BillingAccountRepository,
    transferRepo: BillingTransferRepository,
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
      val effect = for
        existing <- accountRepo.findByOwner(req.tenantId, req.businessOwnerId)
        // Resolve user email from their Stripe customer record (needed for Stripe Express account)
        emailAndName <- userRepo.findByUserId(req.tenantId, req.businessOwnerId).flatMap {
          case Some(bu) =>
            stripe.retrieveCustomer(bu.customerId).map { json =>
              val email = json.hcursor.downField("email").as[String].getOrElse(s"${req.businessOwnerId}@${req.tenantId}")
              val name  = json.hcursor.downField("name").as[String].getOrElse(req.businessOwnerId)
              (email, name)
            }
          case None =>
            ZIO.succeed((s"${req.businessOwnerId}@${req.tenantId}", req.businessOwnerId))
        }
        (email, companyName) = emailAndName
        // Create Stripe Express account (or reuse existing external id)
        stripeAccountId <- existing.flatMap(_.accountExternalId) match
          case Some(id) => ZIO.succeed(id)
          case None =>
            stripe.createConnectedAccount(email, companyName).map(
              _.hcursor.downField("id").as[String].getOrElse("")
            )
        // Always generate a fresh onboarding link
        linkJson <- stripe.createAccountLink(stripeAccountId, req.returnUrl, req.returnUrl)
        activationLink = linkJson.hcursor.downField("url").as[String].getOrElse("")
        now = java.time.Instant.now()
        accountId <- existing match
          case None =>
            val acc = BillingAccount(
              id = java.util.UUID.randomUUID().toString,
              tenantId = req.tenantId,
              businessOwnerUserId = req.businessOwnerId,
              companyName = companyName,
              status = BillingAccountStatus.Pending,
              accountExternalId = Some(stripeAccountId),
              activationLink = Some(activationLink),
              createdOn = now,
              createdBy = req.businessOwnerId
            )
            accountRepo.create(acc).as(acc.id)
          case Some(acc) =>
            accountRepo.update(
              acc.copy(accountExternalId = Some(stripeAccountId), activationLink = Some(activationLink))
            ).as(acc.id)
      yield OnboardBillingAccountResponse(success = true, accountId = accountId, activationLink = activationLink)
      effect.catchAll(err => ZIO.succeed(OnboardBillingAccountResponse(success = false, error = err.getMessage)))
    }

  override def activateBillingAccount(req: ActivateBillingAccountRequest): Future[ActivateBillingAccountResponse] =
    run("billing.activateBillingAccount") {
      accountRepo.findById(req.tenantId, req.accountId).flatMap {
        case None =>
          ZIO.succeed(ActivateBillingAccountResponse(success = false, error = s"Account ${req.accountId} not found"))
        case Some(account) =>
          account.accountExternalId match
            case None =>
              ZIO.succeed(ActivateBillingAccountResponse(success = false, error = "Account not yet onboarded to Stripe"))
            case Some(stripeAccountId) =>
              for
                stripeJson <- stripe.retrieveAccount(stripeAccountId)
                chargesEnabled = stripeJson.hcursor.downField("charges_enabled").as[Boolean].getOrElse(false)
                (newStatus, newLink) <-
                  if chargesEnabled then
                    accountRepo.update(account.copy(status = BillingAccountStatus.Active, activationLink = None))
                      .as((BillingAccountStatus.Active, None: Option[String]))
                  else
                    for
                      linkJson <- stripe.createAccountLink(stripeAccountId, req.returnUrl, req.returnUrl)
                      link = linkJson.hcursor.downField("url").as[String].toOption
                      _ <- accountRepo.update(account.copy(status = BillingAccountStatus.Pending, activationLink = link))
                    yield (BillingAccountStatus.Pending, link)
              yield ActivateBillingAccountResponse(
                success = true,
                status = newStatus.code,
                activationLink = newLink.getOrElse("")
              )
      }.catchAll(err => ZIO.succeed(ActivateBillingAccountResponse(success = false, error = err.getMessage)))
    }

  override def finalizeTransfer(req: FinalizeTransferRequest): Future[FinalizeTransferResponse] =
    run("billing.finalizeTransfer") {
      transferRepo.findById(req.tenantId, req.transferId).flatMap {
        case None =>
          ZIO.succeed(FinalizeTransferResponse(success = false, error = s"Transfer ${req.transferId} not found"))
        case Some(transfer) =>
          if transfer.status == TransferStatus.Transferred then
            ZIO.succeed(FinalizeTransferResponse(success = false, error = "Transfer already disbursed to Stripe"))
          else
            transferRepo.update(transfer.copy(status = TransferStatus.Finalized, lastChangedOn = java.time.Instant.now()))
              .as(FinalizeTransferResponse(success = true, status = TransferStatus.Finalized.code))
              .catchAll(err => ZIO.succeed(FinalizeTransferResponse(success = false, error = err.getMessage)))
      }
    }

  override def sendTransfer(req: SendTransferRequest): Future[SendTransferResponse] =
    run("billing.sendTransfer") {
      transferRepo.findById(req.tenantId, req.transferId).flatMap {
        case None =>
          ZIO.succeed(SendTransferResponse(success = false, error = s"Transfer ${req.transferId} not found"))
        case Some(transfer) =>
          if transfer.status != TransferStatus.Finalized then
            ZIO.succeed(SendTransferResponse(success = false, error = s"Transfer must be finalized before sending; current status: ${transfer.status.code}"))
          else
            billing.dispatchFundsForAccount(req.tenantId, transfer.accountId, transfer.currency).map {
              case None =>
                SendTransferResponse(success = false, error = "No funds to dispatch or account not onboarded to Stripe")
              case Some(t) =>
                SendTransferResponse(
                  success = true,
                  transferExternalId = t.transferExternalId.getOrElse(""),
                  status = t.status.code
                )
            }.catchAll(err => ZIO.succeed(SendTransferResponse(success = false, error = err.getMessage)))
      }
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
      BillingUserRepository & BillingAccountRepository & BillingTransferRepository &
      StripeClient & BillingConfig & EvTracing,
    Unit
  ] =
    for
      handler      <- ZIO.service[BillingGrpcHandler]
      billing      <- ZIO.service[BillingService]
      invoiceRepo  <- ZIO.service[InvoiceRepository]
      userRepo     <- ZIO.service[BillingUserRepository]
      accountRepo  <- ZIO.service[BillingAccountRepository]
      transferRepo <- ZIO.service[BillingTransferRepository]
      stripe       <- ZIO.service[StripeClient]
      cfg          <- ZIO.service[BillingConfig]
      tracing      <- ZIO.service[EvTracing]
      rt           <- ZIO.runtime[Any]
      impl = new BillingGrpcTransport(handler, billing, invoiceRepo, userRepo, accountRepo, transferRepo, stripe, cfg, tracing, rt)
      server <- ZIO.attempt(
        NettyServerBuilder
          .forPort(cfg.grpcPort)
          .addService(BillingServiceGrpc.bindService(impl, ExecutionContext.global))
          .build()
          .start()
      )
      _ <- ZIO.logInfo(s"Billing gRPC server listening on :${server.getPort}")
    yield ()
