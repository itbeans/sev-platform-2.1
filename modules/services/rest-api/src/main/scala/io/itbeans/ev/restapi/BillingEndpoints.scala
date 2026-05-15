package io.itbeans.ev.restapi

import io.circe.generic.auto._
import RestApiCodecs.given
import sttp.tapir.generic.auto._
import sttp.tapir.json.circe._
import sttp.tapir.ztapir._
import zio._

// ---------------------------------------------------------------------------
// BillingEndpoints — invoice and billing management.
//
// Mirrors: BillingService.ts
// Billing data lives in ev-billing-service (PostgreSQL). This endpoint
// delegates to ev-billing-service via gRPC (BillingGrpcClient).
// ---------------------------------------------------------------------------

case class Invoice(
    id: String,
    tenantId: String,
    userId: String,
    userEmail: String,
    number: String,
    status: String, // "draft" | "open" | "paid" | "uncollectible" | "void"
    amount: Double,
    currency: String,
    periodFrom: Long, // epoch millis
    periodTo: Long,
    downloadUrl: Option[String],
    createdOn: Long,
    lastChangedOn: Long
)

case class InvoicesResponse(result: List[Invoice], count: Int)

case class BillingUserSyncResult(
    userId: String,
    success: Boolean,
    message: Option[String],
    invoiceId: Option[String]
)

object BillingEndpoints:

  private val tenantHeader = header[String]("X-Tenant-ID")
  private val base         = endpoint.errorOut(stringBody)

  // ── Invoices ──────────────────────────────────────────────────────────────

  private val listInvoicesEp = base.get
    .in("api" / "v1" / "Invoices")
    .in(tenantHeader)
    .in(query[Option[Int]]("Limit").default(None))
    .in(query[Option[Int]]("Skip").default(None))
    .in(query[Option[String]]("UserID"))
    .in(query[Option[String]]("Status"))
    .in(query[Option[Long]]("StartDateTime"))
    .in(query[Option[Long]]("EndDateTime"))
    .out(jsonBody[InvoicesResponse])

  private val getInvoiceEp = base.get
    .in("api" / "v1" / "Invoices" / path[String]("id"))
    .in(tenantHeader)
    .out(jsonBody[Invoice])

  private val downloadInvoiceEp = base.get
    .in("api" / "v1" / "Invoices" / path[String]("id") / "Download")
    .in(tenantHeader)
    .out(header[String]("Content-Type"))
    .out(byteArrayBody)

  // ── Billing sync actions ───────────────────────────────────────────────────

  private val syncUserBillingEp = base.post
    .in("api" / "v1" / "Users" / path[String]("userId") / "BillUser")
    .in(tenantHeader)
    .out(jsonBody[BillingUserSyncResult])

  private val forceSyncBillingEp = base.post
    .in("api" / "v1" / "Billing" / "SynchronizeInvoices")
    .in(tenantHeader)
    .in(query[Option[String]]("UserID"))
    .out(jsonBody[io.circe.Json])

  // ── Payment methods ────────────────────────────────────────────────────────

  case class PaymentMethodResponse(
      id: String,
      brand: String,
      expMonth: Int,
      expYear: Int,
      last4: String,
      `type`: String,
      isDefault: Boolean,
      createdOn: Long
  )

  private val getPaymentMethodsEp = base.get
    .in("api" / "v1" / "Users" / path[String]("userId") / "PaymentMethods")
    .in(tenantHeader)
    .out(jsonBody[List[PaymentMethodResponse]])

  private val setupPaymentMethodEp = base.post
    .in("api" / "v1" / "Users" / path[String]("userId") / "PaymentMethods")
    .in(tenantHeader)
    .in(jsonBody[io.circe.Json])
    .out(jsonBody[io.circe.Json])

  private val deletePaymentMethodEp = base.delete
    .in("api" / "v1" / "Users" / path[String]("userId") / "PaymentMethods" / path[String]("pmId"))
    .in(tenantHeader)
    .out(statusCode(sttp.model.StatusCode.NoContent))

  private def recordToInvoice(rec: io.itbeans.ev.billing.grpc.billing_service.InvoiceRecord): Invoice =
    Invoice(
      id            = rec.id,
      tenantId      = rec.tenantId,
      userId        = rec.userId,
      userEmail     = "",
      number        = rec.invoiceNumber,
      status        = rec.status,
      amount        = rec.amountCents / 100.0,
      currency      = rec.currency,
      periodFrom    = rec.createdOn,
      periodTo      = rec.lastChangedOn,
      downloadUrl   = if rec.downloadUrl.nonEmpty then Some(rec.downloadUrl) else None,
      createdOn     = rec.createdOn,
      lastChangedOn = rec.lastChangedOn
    )

  def routes(billing: BillingGrpcClient): List[ZServerEndpoint[Any, Any]] = List(
    listInvoicesEp.zServerLogic { case (tenantId, limit, skip, userId, status, from, to) =>
      billing
        .getInvoices(tenantId, userId.getOrElse(""), limit.getOrElse(25), skip.getOrElse(0))
        .map(r => InvoicesResponse(r.invoices.map(recordToInvoice).toList, r.total))
        .mapError(_.getMessage)
    },
    getInvoiceEp.zServerLogic { case (id, tenantId) =>
      billing
        .getInvoice(tenantId, id)
        .flatMap { r =>
          if r.found && r.invoice.nonEmpty then ZIO.succeed(recordToInvoice(r.invoice.get))
          else ZIO.fail(s"Invoice $id not found")
        }
        .mapError(_.toString)
    },
    downloadInvoiceEp.zServerLogic { case (id, tenantId) =>
      billing.downloadInvoice(tenantId, id).flatMap { r =>
        if r.found then
          if r.downloadUrl.nonEmpty then
            ZIO.fail(s"Redirect to: ${r.downloadUrl}")
          else
            ZIO.succeed(("application/pdf", r.pdfContent.toByteArray))
        else ZIO.fail(s"Invoice $id not found")
      }.mapError(_.toString)
    },
    // Billing sync
    syncUserBillingEp.zServerLogic { case (userId, tenantId) =>
      billing
        .synchronizeBillingUser(tenantId, userId)
        .map(r => BillingUserSyncResult(userId, r.success, if r.error.nonEmpty then Some(r.error) else None, None))
        .mapError(_.getMessage)
    },
    forceSyncBillingEp.zServerLogic { case (tenantId, userId) =>
      billing
        .forceSynchronizeUser(tenantId, userId.getOrElse(""))
        .map(r => io.circe.Json.obj("success" -> io.circe.Json.fromBoolean(r.success)))
        .mapError(_.getMessage)
    },
    // Payment methods
    getPaymentMethodsEp.zServerLogic { case (userId, tenantId) =>
      billing
        .getPaymentMethods(tenantId, userId)
        .map { r =>
          r.paymentMethods.map(pm =>
            PaymentMethodResponse(
              pm.id,
              pm.brand,
              pm.expMonth,
              pm.expYear,
              pm.last4,
              pm.`type`,
              pm.isDefault,
              pm.createdOn
            )
          ).toList
        }
        .mapError(_.getMessage)
    },
    setupPaymentMethodEp.zServerLogic { case (userId, tenantId, body) =>
      val pmId = body.hcursor.downField("paymentMethodId").as[String].getOrElse("")
      billing
        .setupPaymentMethod(tenantId, userId, pmId)
        .map(r =>
          io.circe.Json.obj(
            "success"      -> io.circe.Json.fromBoolean(r.success),
            "clientSecret" -> io.circe.Json.fromString(r.clientSecret)
          )
        )
        .mapError(_.getMessage)
    },
    deletePaymentMethodEp.zServerLogic { case (userId, pmId, tenantId) =>
      billing
        .deletePaymentMethod(tenantId, userId, pmId)
        .ignore
    }
  )
