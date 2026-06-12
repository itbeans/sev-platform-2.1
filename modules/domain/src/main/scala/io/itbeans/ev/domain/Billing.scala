package io.itbeans.ev.domain

import zio.schema.{DeriveSchema, Schema, StandardType}
import java.time.Instant

opaque type InvoiceId = String

object InvoiceId:
  def apply(v: String): InvoiceId             = v
  def unapply(id: InvoiceId): Some[String]    = Some(id)
  extension (id: InvoiceId) def value: String = id
  given Schema[InvoiceId] = Schema.Primitive(StandardType.StringType, zio.Chunk.empty).transform(InvoiceId(_), _.value)

// ---------------------------------------------------------------------------
// Billing domain — invoices, accounts, transfers, per-session billing data.
// Mirrors the TypeScript billing storage model and Stripe integration.
// ---------------------------------------------------------------------------

enum InvoiceStatus(val code: String):
  case Draft         extends InvoiceStatus("draft")
  case Open          extends InvoiceStatus("open")
  case Paid          extends InvoiceStatus("paid")
  case Void          extends InvoiceStatus("void")
  case Uncollectible extends InvoiceStatus("uncollectible")
  case Deleted       extends InvoiceStatus("deleted")

enum BillingAccountStatus(val code: String):
  case Idle    extends BillingAccountStatus("idle")
  case Pending extends BillingAccountStatus("pending")
  case Active  extends BillingAccountStatus("active")

enum BillingTransferStatus(val code: String):
  case Draft       extends BillingTransferStatus("draft")
  case Pending     extends BillingTransferStatus("pending")
  case Finalized   extends BillingTransferStatus("finalized")
  case Transferred extends BillingTransferStatus("transferred")

// One line-item dimension within an invoice (energy, flat-fee, time, etc.)
case class BillingDimension(
    `type`: String,
    unitPrice: BigDecimal,
    quantity: BigDecimal,
    amountCents: Int,
    description: String
)

// Snapshot of a charging session bundled into an invoice
case class BillingSessionData(
    transactionId: Long,
    chargingStationId: ChargingStationId,
    pricingId: Option[PricingId],
    startDate: Instant,
    endDate: Instant,
    consumptionKwh: Double,
    durationSecs: Long,
    dimensions: List[BillingDimension]
)

case class Invoice(
    id: InvoiceId,
    tenantId: TenantId,
    invoiceId: String, // Stripe invoice ID
    invoiceNumber: Option[String],
    status: InvoiceStatus,
    amountCents: Int,
    amountPaidCents: Int,
    currency: String,
    userId: UserId,
    customerId: String, // Stripe customer ID
    liveMode: Boolean,
    sessions: List[BillingSessionData],
    downloadUrl: Option[String],
    payInvoiceUrl: Option[String],
    lastError: Option[String],
    createdOn: Instant,
    lastChangedOn: Instant
)

// Stripe Connect account linked to a company (for B2B billing transfers)
case class BillingAccount(
    id: String,
    tenantId: TenantId,
    businessOwnerUserId: UserId,
    companyName: String,
    status: BillingAccountStatus,
    accountExternalId: Option[String], // Stripe account ID
    activationLink: Option[String],
    createdOn: Instant,
    createdBy: UserId
)

case class BillingTransfer(
    id: String,
    tenantId: TenantId,
    accountId: String,
    accountExternalId: String,
    status: BillingTransferStatus,
    currency: String,
    sessionCounter: Int,
    collectedFundsCents: Int,
    collectedFeesCents: Int,
    transferAmountCents: Int,
    transferExternalId: Option[String],
    createdOn: Instant,
    lastChangedOn: Instant
)

// Stripe customer record linked to a platform user
case class BillingUser(
    userId: UserId,
    tenantId: TenantId,
    customerId: String, // Stripe customer ID
    defaultPaymentMethodId: Option[String],
    createdOn: Instant
)

object Billing:
  given Schema[InvoiceStatus] = DeriveSchema.gen
  given Schema[BillingAccountStatus] = DeriveSchema.gen
  given Schema[BillingTransferStatus] = DeriveSchema.gen
  given Schema[BillingDimension] = DeriveSchema.gen
  given Schema[BillingSessionData] = DeriveSchema.gen
  given Schema[Invoice] = DeriveSchema.gen
  given Schema[BillingAccount] = DeriveSchema.gen
  given Schema[BillingTransfer] = DeriveSchema.gen
  given Schema[BillingUser] = DeriveSchema.gen
