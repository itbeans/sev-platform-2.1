package io.itbeans.ev.billingservice

import zio._

// ---------------------------------------------------------------------------
// BillingGrpcServer — pre-codegen gRPC server for ev-billing.
//
// In production this will be generated from billing_service.proto via ScalaPB.
// Until the proto is wired into the build, we use plain ADTs and expose the
// business logic through ZIO effects that other services can call directly
// (in-process) or via gRPC once the generated code is available.
//
// Declared RPC methods (matching billing_service.proto):
//   GetInvoices(tenantId, userId) → List[InvoiceSummary]
//   CreateBillingUser(tenantId, userId, email, name) → BillingUserSummary
//   CheckPaymentMethods(tenantId, userId) → PaymentMethodsResult
//   ChargeInvoice(tenantId, invoiceId) → InvoiceSummary
// ---------------------------------------------------------------------------

// ── Request / Response ADTs (will be replaced by ScalaPB generated classes) ──

case class GetInvoicesRequestADT(tenantId: String, userId: String)

case class InvoiceSummaryADT(
    id: String,
    invoiceId: String,
    invoiceNumber: Option[String],
    status: String,
    amountCents: Int,
    currency: String,
    downloadUrl: Option[String]
)
case class GetInvoicesResponseADT(invoices: List[InvoiceSummaryADT])

case class CreateBillingUserRequestADT(tenantId: String, userId: String, email: String, name: String)
case class BillingUserSummaryADT(userId: String, customerId: String)
case class CreateBillingUserResponseADT(user: BillingUserSummaryADT)

case class CheckPaymentMethodsRequestADT(tenantId: String, userId: String)
case class PaymentMethodADT(id: String, brand: String, last4: String, expMonth: Int, expYear: Int)
case class CheckPaymentMethodsResponseADT(paymentMethods: List[PaymentMethodADT], hasDefault: Boolean)

case class ChargeInvoiceRequestADT(tenantId: String, invoiceId: String)
case class ChargeInvoiceResponseADT(invoice: InvoiceSummaryADT)

// ── Service implementation ────────────────────────────────────────────────

trait BillingGrpcHandler:
  def getInvoices(req: GetInvoicesRequestADT): Task[GetInvoicesResponseADT]
  def createBillingUser(req: CreateBillingUserRequestADT): Task[CreateBillingUserResponseADT]
  def checkPaymentMethods(req: CheckPaymentMethodsRequestADT): Task[CheckPaymentMethodsResponseADT]
  def chargeInvoice(req: ChargeInvoiceRequestADT): Task[ChargeInvoiceResponseADT]

final class LiveBillingGrpcHandler(
    billing: BillingService,
    invoiceRepo: InvoiceRepository,
    userRepo: BillingUserRepository,
    stripe: StripeClient
) extends BillingGrpcHandler:

  def getInvoices(req: GetInvoicesRequestADT): Task[GetInvoicesResponseADT] =
    invoiceRepo.findByUserId(req.tenantId, req.userId)
      .map { invoices =>
        GetInvoicesResponseADT(invoices.map(toSummary))
      }

  def createBillingUser(req: CreateBillingUserRequestADT): Task[CreateBillingUserResponseADT] =
    billing.getOrCreateCustomer(req.tenantId, req.userId, req.email, req.name)
      .map { customerId =>
        CreateBillingUserResponseADT(BillingUserSummaryADT(req.userId, customerId))
      }

  def checkPaymentMethods(req: CheckPaymentMethodsRequestADT): Task[CheckPaymentMethodsResponseADT] =
    userRepo.findByUserId(req.tenantId, req.userId).flatMap {
      case None =>
        ZIO.succeed(CheckPaymentMethodsResponseADT(Nil, hasDefault = false))
      case Some(billingUser) =>
        stripe.listPaymentMethods(billingUser.customerId)
          .map { json =>
            val methods = json.hcursor.downField("data").as[List[io.circe.Json]].getOrElse(Nil)
            val parsed = methods.map { pm =>
              val id       = pm.hcursor.downField("id").as[String].getOrElse("")
              val brand    = pm.hcursor.downField("card").downField("brand").as[String].getOrElse("")
              val last4    = pm.hcursor.downField("card").downField("last4").as[String].getOrElse("")
              val expMonth = pm.hcursor.downField("card").downField("exp_month").as[Int].getOrElse(0)
              val expYear  = pm.hcursor.downField("card").downField("exp_year").as[Int].getOrElse(0)
              PaymentMethodADT(id, brand, last4, expMonth, expYear)
            }
            CheckPaymentMethodsResponseADT(parsed, hasDefault = billingUser.defaultPaymentMethodId.isDefined)
          }
          .catchAll { err =>
            ZIO.logWarning(s"Failed to list payment methods for ${req.userId}: ${err.getMessage}") *>
              ZIO.succeed(CheckPaymentMethodsResponseADT(Nil, hasDefault = false))
          }
    }

  def chargeInvoice(req: ChargeInvoiceRequestADT): Task[ChargeInvoiceResponseADT] =
    billing.chargeInvoice(req.tenantId, req.invoiceId)
      .map(inv => ChargeInvoiceResponseADT(toSummary(inv)))

  private def toSummary(inv: Invoice): InvoiceSummaryADT =
    InvoiceSummaryADT(
      id = inv.id,
      invoiceId = inv.invoiceId,
      invoiceNumber = inv.invoiceNumber,
      status = inv.status.code,
      amountCents = inv.amountCents,
      currency = inv.currency,
      downloadUrl = inv.downloadUrl
    )

object LiveBillingGrpcHandler:

  val live
      : ZLayer[BillingService & InvoiceRepository & BillingUserRepository & StripeClient, Nothing, BillingGrpcHandler] =
    ZLayer.fromFunction(new LiveBillingGrpcHandler(_, _, _, _))
