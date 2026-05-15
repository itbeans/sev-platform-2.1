package io.itbeans.ev.billingservice

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.*

import java.time.Instant

// ---------------------------------------------------------------------------
// ev-billing domain types
//
// Amounts are in the minor unit of the currency (e.g. cents for EUR/USD)
// to match the Stripe API convention.
// ---------------------------------------------------------------------------

// ── Invoice status mirrors Stripe's invoice statuses ─────────────────────

enum InvoiceStatus(val code: String):
  case Draft         extends InvoiceStatus("draft")
  case Open          extends InvoiceStatus("open")
  case Paid          extends InvoiceStatus("paid")
  case Void          extends InvoiceStatus("void")
  case Uncollectible extends InvoiceStatus("uncollectible")
  case Deleted       extends InvoiceStatus("deleted")

object InvoiceStatus:
  def fromCode(s: String): Option[InvoiceStatus] = values.find(_.code == s)
  given Encoder[InvoiceStatus] = Encoder.encodeString.contramap(_.code)

  given Decoder[InvoiceStatus] = Decoder.decodeString.emap(s =>
    fromCode(s).toRight(s"Unknown InvoiceStatus: $s")
  )

// ── Billing account status (CPO connected Stripe account) ────────────────

enum BillingAccountStatus(val code: String):
  case Idle    extends BillingAccountStatus("idle")
  case Pending extends BillingAccountStatus("pending")
  case Active  extends BillingAccountStatus("active")

object BillingAccountStatus:
  def fromCode(s: String): Option[BillingAccountStatus] = values.find(_.code == s)
  given Encoder[BillingAccountStatus] = Encoder.encodeString.contramap(_.code)

  given Decoder[BillingAccountStatus] = Decoder.decodeString.emap(s =>
    fromCode(s).toRight(s"Unknown BillingAccountStatus: $s")
  )

// ── Transfer status ───────────────────────────────────────────────────────

enum TransferStatus(val code: String):
  case Draft       extends TransferStatus("draft")
  case Pending     extends TransferStatus("pending")
  case Finalized   extends TransferStatus("finalized")
  case Transferred extends TransferStatus("transferred")

object TransferStatus:
  def fromCode(s: String): Option[TransferStatus] = values.find(_.code == s)
  given Encoder[TransferStatus] = Encoder.encodeString.contramap(_.code)

  given Decoder[TransferStatus] = Decoder.decodeString.emap(s =>
    fromCode(s).toRight(s"Unknown TransferStatus: $s")
  )

// ── One pricing dimension within an invoice session ───────────────────────

case class BillingDimension(
    `type`: String, // FLAT_FEE | ENERGY | CHARGING_TIME | PARKING_TIME
    unitPrice: BigDecimal,
    quantity: BigDecimal,
    amountCents: Int,
    description: String
)

object BillingDimension:
  given Encoder[BillingDimension] = deriveEncoder
  given Decoder[BillingDimension] = deriveDecoder

// ── Session data embedded in an invoice line ──────────────────────────────

case class BillingSessionData(
    transactionId: Long,
    chargingStationId: String,
    pricingId: Option[String],
    startDate: Instant,
    endDate: Instant,
    consumptionKwh: Double,
    durationSecs: Long,
    dimensions: List[BillingDimension]
)

object BillingSessionData:
  given Encoder[BillingSessionData] = deriveEncoder
  given Decoder[BillingSessionData] = deriveDecoder

// ── Invoice ───────────────────────────────────────────────────────────────

case class Invoice(
    id: String, // internal ID (same as Stripe invoice ID)
    tenantId: String,
    invoiceId: String,             // Stripe invoice ID (inv_...)
    invoiceNumber: Option[String], // Stripe human-readable number (set after finalize)
    status: InvoiceStatus,
    amountCents: Int,
    amountPaidCents: Int,
    currency: String,   // ISO 4217 lowercase (eur, usd)
    userId: String,     // EV server user ID
    customerId: String, // Stripe customer ID
    liveMode: Boolean,
    sessions: List[BillingSessionData],
    downloadUrl: Option[String],
    payInvoiceUrl: Option[String],
    lastError: Option[String],
    createdOn: Instant,
    lastChangedOn: Instant
)

object Invoice:
  given Encoder[Invoice] = deriveEncoder
  given Decoder[Invoice] = deriveDecoder

// ── Billing account (CPO connected Stripe account) ────────────────────────

case class BillingAccount(
    id: String,
    tenantId: String,
    businessOwnerUserId: String,
    companyName: String,
    status: BillingAccountStatus,
    accountExternalId: Option[String], // Stripe connected account ID (acct_...)
    activationLink: Option[String],    // Stripe account link URL
    createdOn: Instant,
    createdBy: String
)

object BillingAccount:
  given Encoder[BillingAccount] = deriveEncoder
  given Decoder[BillingAccount] = deriveDecoder

// ── Billing transfer (fund dispatch to CPO) ───────────────────────────────

case class BillingTransfer(
    id: String,
    tenantId: String,
    accountId: String,         // BillingAccount ID
    accountExternalId: String, // Stripe connected account ID
    status: TransferStatus,
    currency: String,
    sessionCounter: Int,
    collectedFundsCents: Int,           // gross funds collected (transaction amounts)
    collectedFeesCents: Int,            // platform fees (flat + percentage)
    transferAmountCents: Int,           // net = collectedFunds - collectedFees
    transferExternalId: Option[String], // Stripe transfer ID (tr_...)
    createdOn: Instant,
    lastChangedOn: Instant
)

object BillingTransfer:
  given Encoder[BillingTransfer] = deriveEncoder
  given Decoder[BillingTransfer] = deriveDecoder

// ── Billing user (links EV user to Stripe customer) ──────────────────────

case class BillingUser(
    userId: String,
    tenantId: String,
    customerId: String, // Stripe customer ID (cus_...)
    defaultPaymentMethodId: Option[String],
    createdOn: Instant
)

object BillingUser:
  given Encoder[BillingUser] = deriveEncoder
  given Decoder[BillingUser] = deriveDecoder

// ── Kafka event from ev-ocpp-processor ───────────────────────────────────

/**
 * Subset of what ev-ocpp-processor publishes to transactions.lifecycle.
 * Billing only acts on action == "Stop".
 */
case class TransactionLifecycleBillingPayload(
    tenantId: String,
    transactionId: Long,
    action: String, // "Start" | "Update" | "Stop"
    chargingStationId: String,
    userId: Option[String],
    meterStart: Option[Double],
    meterStop: Option[Double],
    startDate: Option[Instant],
    endDate: Option[Instant],
    totalDurationSecs: Option[Long],
    totalInactivitySecs: Option[Long],
    consumptionWh: Option[Double],
    priceUnit: Option[String],
    totalCost: Option[Double],
    pricingId: Option[String],
    // JSON-encoded dimension breakdown from ev-pricing-service FinalisePrice RPC.
    // When present, billing dimensions are derived from this; otherwise a single
    // ENERGY dimension covering the full totalCost is created.
    invoiceItemJson: Option[String]
)

object TransactionLifecycleBillingPayload:
  given Encoder[TransactionLifecycleBillingPayload] = deriveEncoder
  given Decoder[TransactionLifecycleBillingPayload] = deriveDecoder
