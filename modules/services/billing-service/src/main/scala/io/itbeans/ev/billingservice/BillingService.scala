package io.itbeans.ev.billingservice

import io.circe.syntax._
import zio._

import java.time.Instant
import java.util.UUID

// ---------------------------------------------------------------------------
// BillingService — core billing logic.
//
// Responsibilities:
//   1. getOrCreateStripeCustomer — find/create Stripe customer for a user
//   2. createInvoiceForSession — build Stripe draft invoice from transaction data
//   3. chargeInvoice — finalize + pay a draft/open invoice
//   4. chargeAllDraftsBefore — monthly periodic billing operation
//   5. dispatchFundsForAccount — aggregate sessions → transfer to CPO account
// ---------------------------------------------------------------------------

trait BillingService:
  /** Ensure a Stripe customer exists for this user; return the customer ID. */
  def getOrCreateCustomer(tenantId: String, userId: String, email: String, name: String): Task[String]

  /** Create a Stripe draft invoice for a completed transaction session. */
  def createInvoiceForSession(tenantId: String, payload: TransactionLifecycleBillingPayload): Task[Invoice]

  /** Finalize and pay a Stripe invoice. */
  def chargeInvoice(tenantId: String, invoiceId: String): Task[Invoice]

  /** Monthly billing: finalize + pay all draft invoices created before cutoffMs. */
  def chargeAllDraftsBefore(tenantId: String, cutoffEpochMs: Long): Task[Int]

  /** Dispatch collected funds to a CPO connected account. */
  def dispatchFundsForAccount(tenantId: String, accountId: String, currency: String): Task[Option[BillingTransfer]]

final class LiveBillingService(
    stripe: StripeClient,
    invoiceRepo: InvoiceRepository,
    userRepo: BillingUserRepository,
    transferRepo: BillingTransferRepository,
    accountRepo: BillingAccountRepository,
    cfg: BillingConfig
) extends BillingService:

  def getOrCreateCustomer(tenantId: String, userId: String, email: String, name: String): Task[String] =
    userRepo.findByUserId(tenantId, userId).flatMap {
      case Some(bu) => ZIO.succeed(bu.customerId)
      case None =>
        for
          json <- stripe.createCustomer(email, name, userId)
          customerId = json.hcursor.downField("id").as[String].getOrElse("")
          billingUser = BillingUser(
            userId = userId,
            tenantId = tenantId,
            customerId = customerId,
            defaultPaymentMethodId = None,
            createdOn = Instant.now()
          )
          _ <- userRepo.upsert(billingUser)
          _ <- ZIO.logInfo(s"Created Stripe customer $customerId for user $userId")
        yield customerId
    }

  def createInvoiceForSession(tenantId: String, payload: TransactionLifecycleBillingPayload): Task[Invoice] =
    for
      userId <- ZIO.fromOption(payload.userId)
        .orElseFail(new Exception(s"Transaction ${payload.transactionId} has no userId — skip billing"))
      currency = payload.priceUnit.map(_.toLowerCase).getOrElse("eur")
      // We need a billing user record; on first charge we create the Stripe customer
      billingUser <- userRepo.findByUserId(tenantId, userId)
      customerId <- billingUser match
        case Some(bu) => ZIO.succeed(bu.customerId)
        case None => ZIO.fail(new Exception(s"No Stripe customer for user $userId — call getOrCreateCustomer first"))
      desc = s"Charging session ${payload.transactionId} at ${payload.chargingStationId}"
      invoiceJson <- stripe.createInvoice(customerId, currency, desc)
      stripeId = invoiceJson.hcursor.downField("id").as[String].getOrElse("")
      // Build line items for the session dimensions
      dimensions = buildDimensions(payload)
      totalCents = dimensions.map(_.amountCents).sum
      _ <- ZIO.foreachDiscard(dimensions) { dim =>
        stripe.createInvoiceItem(customerId, stripeId, dim.amountCents, currency, dim.description)
      }
      now = Instant.now()
      session = BillingSessionData(
        transactionId = payload.transactionId,
        chargingStationId = payload.chargingStationId,
        pricingId = payload.pricingId,
        startDate = payload.startDate.getOrElse(now),
        endDate = payload.endDate.getOrElse(now),
        consumptionKwh = payload.consumptionWh.map(_ / 1000.0).getOrElse(0.0),
        durationSecs = payload.totalDurationSecs.getOrElse(0L),
        dimensions = dimensions
      )
      invoice = Invoice(
        id = UUID.randomUUID().toString,
        tenantId = tenantId,
        invoiceId = stripeId,
        invoiceNumber = None,
        status = InvoiceStatus.Draft,
        amountCents = totalCents,
        amountPaidCents = 0,
        currency = currency,
        userId = userId,
        customerId = customerId,
        liveMode = !cfg.stripeSecretKey.startsWith("sk_test_"),
        sessions = List(session),
        downloadUrl = None,
        payInvoiceUrl = None,
        lastError = None,
        createdOn = now,
        lastChangedOn = now
      )
      _ <- invoiceRepo.create(invoice)
      _ <- ZIO.logInfo(
        s"Created Stripe draft invoice $stripeId for tx ${payload.transactionId} (${totalCents}c $currency)"
      )
      // If immediatePayment is configured, finalize + charge immediately
      result <- if cfg.immediatePayment then chargeInvoice(tenantId, invoice.id) else ZIO.succeed(invoice)
    yield result

  def chargeInvoice(tenantId: String, invoiceId: String): Task[Invoice] =
    for
      inv <- invoiceRepo.findById(tenantId, invoiceId)
        .flatMap(ZIO.fromOption(_).orElseFail(new Exception(s"Invoice $invoiceId not found")))
      // Finalize if still draft
      _ <- if inv.status == InvoiceStatus.Draft then
        stripe.finalizeInvoice(inv.invoiceId).unit
      else ZIO.unit
      // Pay
      payResult <- stripe.payInvoice(inv.invoiceId).catchAll { err =>
        ZIO.logWarning(s"Payment failed for invoice ${inv.invoiceId}: ${err.getMessage}") *>
          ZIO.succeed(io.circe.Json.Null)
      }
      // Refresh from Stripe to get the definitive status
      freshJson <- stripe.retrieveInvoice(inv.invoiceId)
      newStatus = freshJson.hcursor.downField("status").as[String]
        .toOption.flatMap(InvoiceStatus.fromCode).getOrElse(InvoiceStatus.Open)
      paidCents   = freshJson.hcursor.downField("amount_paid").as[Int].getOrElse(0)
      invoiceNum  = freshJson.hcursor.downField("number").as[String].toOption
      downloadUrl = freshJson.hcursor.downField("invoice_pdf").as[String].toOption
      payUrl      = freshJson.hcursor.downField("hosted_invoice_url").as[String].toOption
      lastErrMsg = if newStatus == InvoiceStatus.Open then
        freshJson.hcursor.downField("last_payment_error").downField("message").as[String].toOption
      else None
      now = Instant.now()
      updated = inv.copy(
        status = newStatus,
        amountPaidCents = paidCents,
        invoiceNumber = invoiceNum.orElse(inv.invoiceNumber),
        downloadUrl = downloadUrl,
        payInvoiceUrl = payUrl,
        lastError = lastErrMsg,
        lastChangedOn = now
      )
      _ <- invoiceRepo.update(updated)
      _ <- ZIO.logInfo(s"Invoice ${inv.invoiceId} charged → status=${newStatus.code} paid=${paidCents}c")
    yield updated

  def chargeAllDraftsBefore(tenantId: String, cutoffEpochMs: Long): Task[Int] =
    for
      drafts <- invoiceRepo.findDraftsBefore(tenantId, cutoffEpochMs)
      _      <- ZIO.logInfo(s"Periodic billing: charging ${drafts.size} draft invoices for tenant $tenantId")
      results <- ZIO.foreach(drafts) { inv =>
        chargeInvoice(tenantId, inv.id)
          .tapError(err => ZIO.logError(s"Failed to charge invoice ${inv.id}: ${err.getMessage}"))
          .fold(_ => 0, _ => 1)
      }
    yield results.sum

  def dispatchFundsForAccount(tenantId: String, accountId: String, currency: String): Task[Option[BillingTransfer]] =
    accountRepo.findById(tenantId, accountId).flatMap {
      case None => ZIO.logWarning(s"BillingAccount $accountId not found") *> ZIO.succeed(None)
      case Some(account) =>
        account.accountExternalId match
          case None => ZIO.logWarning(s"Account $accountId not yet onboarded to Stripe") *> ZIO.succeed(None)
          case Some(externalId) =>
            for
              draft <- transferRepo.findDraftByAccount(tenantId, accountId, currency)
              transfer <- draft match
                case Some(t) => ZIO.succeed(t)
                case None =>
                  val now = Instant.now()
                  val t = BillingTransfer(
                    id = UUID.randomUUID().toString,
                    tenantId = tenantId,
                    accountId = accountId,
                    accountExternalId = externalId,
                    status = TransferStatus.Draft,
                    currency = currency,
                    sessionCounter = 0,
                    collectedFundsCents = 0,
                    collectedFeesCents = 0,
                    transferAmountCents = 0,
                    transferExternalId = None,
                    createdOn = now,
                    lastChangedOn = now
                  )
                  transferRepo.create(t) *> ZIO.succeed(t)
              // Only transfer if there are funds to send
              _ <- if transfer.transferAmountCents > 0 then
                for
                  stripeResp <- stripe.createTransfer(
                    transfer.transferAmountCents,
                    transfer.currency,
                    externalId,
                    s"EV platform payout — ${transfer.sessionCounter} sessions"
                  )
                  transferId = stripeResp.hcursor.downField("id").as[String].getOrElse("")
                  now        = Instant.now()
                  updated = transfer.copy(
                    status = TransferStatus.Transferred,
                    transferExternalId = Some(transferId),
                    lastChangedOn = now
                  )
                  _ <- transferRepo.update(updated)
                  _ <- ZIO.logInfo(
                    s"Dispatched ${transfer.transferAmountCents}c $currency to account $externalId (transfer $transferId)"
                  )
                yield ()
              else ZIO.logInfo(s"No funds to dispatch for account $accountId ($currency)")
            yield Some(transfer)
    }

  // ── Dimension builder ─────────────────────────────────────────────────────

  /**
   * Build billing dimensions from the transaction payload.
   *
   * Priority order:
   *  1. `invoiceItemJson` — JSON array of BillingDimension from ev-pricing-service
   *     FinalisePrice RPC; used directly when present and non-empty.
   *  2. Proportional fallback — splits totalCost into ENERGY + CHARGING_TIME +
   *     PARKING_TIME based on session metrics when pricing breakdown is unavailable.
   *
   * Mirrors TypeScript BillingFacade / BillingIntegration dimension logic.
   */
  private def buildDimensions(payload: TransactionLifecycleBillingPayload): List[BillingDimension] =
    val totalCost = payload.totalCost.getOrElse(0.0)
    if totalCost <= 0.0 then Nil
    else
      // 1. Use pricing-service breakdown if available
      payload.invoiceItemJson
        .flatMap { json =>
          io.circe.parser.decode[List[BillingDimension]](json).toOption
        }
        .filter(_.nonEmpty) match
        case Some(dims) => dims

        // 2. Proportional fallback based on session metrics
        case None =>
          val totalCents      = math.round(totalCost * 100).toInt
          val energyKwh       = payload.consumptionWh.map(_ / 1000.0).getOrElse(0.0)
          val durationSecs    = payload.totalDurationSecs.getOrElse(0L)
          val inactivitySecs  = payload.totalInactivitySecs.getOrElse(0L)
          val chargingSecs    = math.max(0L, durationSecs - inactivitySecs)

          // Weight: energy (80% if both present), time (15%), parking (5%)
          // Adjusted so that zero-quantity dimensions are omitted.
          val hasEnergy  = energyKwh > 0
          val hasTime    = chargingSecs > 0
          val hasParking = inactivitySecs > 0

          val weights = List(
            if hasEnergy  then Some(("energy",   0.80)) else None,
            if hasTime    then Some(("time",      0.15)) else None,
            if hasParking then Some(("parking",   0.05)) else None
          ).flatten

          val totalWeight = weights.map(_._2).sum
          val normalized  = weights.map { case (k, w) => k -> (w / totalWeight) }

          val allocated = normalized.map { case (key, fraction) =>
            val cents = math.round(totalCents * fraction).toInt
            key match
              case "energy" =>
                BillingDimension(
                  `type` = "ENERGY",
                  unitPrice = if energyKwh > 0 then
                    BigDecimal(cents.toDouble / 100 / energyKwh).setScale(4, BigDecimal.RoundingMode.HALF_UP)
                  else BigDecimal(0),
                  quantity = BigDecimal(energyKwh).setScale(3, BigDecimal.RoundingMode.HALF_UP),
                  amountCents = cents,
                  description = f"Charging energy $energyKwh%.3f kWh"
                )
              case "time" =>
                val chargingMins = chargingSecs / 60.0
                BillingDimension(
                  `type` = "CHARGING_TIME",
                  unitPrice = if chargingMins > 0 then
                    BigDecimal(cents.toDouble / 100 / chargingMins).setScale(4, BigDecimal.RoundingMode.HALF_UP)
                  else BigDecimal(0),
                  quantity = BigDecimal(chargingMins).setScale(2, BigDecimal.RoundingMode.HALF_UP),
                  amountCents = cents,
                  description = f"Charging time $chargingMins%.0f min"
                )
              case "parking" =>
                val parkingMins = inactivitySecs / 60.0
                BillingDimension(
                  `type` = "PARKING_TIME",
                  unitPrice = if parkingMins > 0 then
                    BigDecimal(cents.toDouble / 100 / parkingMins).setScale(4, BigDecimal.RoundingMode.HALF_UP)
                  else BigDecimal(0),
                  quantity = BigDecimal(parkingMins).setScale(2, BigDecimal.RoundingMode.HALF_UP),
                  amountCents = cents,
                  description = f"Parking time $parkingMins%.0f min"
                )
              case _ => BillingDimension("OTHER", BigDecimal(0), BigDecimal(0), cents, "Other charges")
          }

          // Adjust last dimension to absorb rounding errors
          if allocated.isEmpty then Nil
          else
            val sumSoFar = allocated.dropRight(1).map(_.amountCents).sum
            val last     = allocated.last
            allocated.dropRight(1) :+ last.copy(amountCents = totalCents - sumSoFar)

object LiveBillingService:

  val live: ZLayer[
    StripeClient & InvoiceRepository & BillingUserRepository & BillingTransferRepository & BillingAccountRepository & BillingConfig,
    Nothing,
    BillingService
  ] =
    ZLayer.fromFunction(new LiveBillingService(_, _, _, _, _, _))
